import type { Api, ImageContent, Model, Usage } from "@earendil-works/pi-ai";
import type { AgentMessage, ThinkingLevel } from "../../types.ts";
import {
	type AbortRequestResult,
	type AbortResult,
	type ActionInfo,
	type AgentLane,
	type CancelQueuedResult,
	type CompactionResult,
	type CurrentOperationInfo,
	type DriveOptions,
	type DriveResult,
	HarnessClosed,
	HarnessFault,
	InvalidMessage,
	LaneBusy,
	type LaneExecutionInfo,
	type LaneSnapshot,
	MissingIdentities,
	type NavigateOptions,
	type NavigationResult,
	type NextRunResult,
	type OperationAdmissionResult,
	type OperationRequest,
	type QueueResult,
	type RecordUsageResult,
	type Resources,
	type ResumeResult,
	type RunResult,
	type SuspendedOperation,
	UnknownSkill,
	UnknownTemplate,
	type WatchHandle,
} from "../agent-harness.ts";
import { hasMissingIdentities, missingIdentities } from "../config.ts";
import type { Context } from "../context.ts";
import { BreakpointBarrier } from "../execution/breakpoint.ts";
import { formatPromptTemplateInvocation } from "../prompt-templates.ts";
import { type RestoredLane, restoreLane } from "../restore.ts";
import { Result } from "../result.ts";
import { materializeCommittedEntry } from "../session/commit.ts";
import { SessionInvariantError, SessionPendingAssistantMessageError } from "../session/session.ts";
import type {
	Entry,
	JsonValue,
	LaneConfiguration,
	LaneLastResult,
	NewEntry,
	OperationMeta,
	PendingEntry,
	RunState,
	SessionReader,
	SessionTree,
} from "../session/types.ts";
import { formatSkillInvocation } from "../skills.ts";
import { deferredValue, driveLane } from "./operation-task.ts";
import { isPendingAssistant } from "./transitions.ts";
import type {
	AcceptancePublication,
	AdmissionReservation,
	LaneRuntimeContext,
	NormalizedRunRequest,
	RuntimeLane,
} from "./types.ts";
import { RuntimeSliceNotImplemented } from "./types.ts";

export function createPublicSessionView<TContext extends object | undefined>(
	runtime: LaneRuntimeContext<TContext>,
	lane: string,
): SessionTree {
	const delegate = runtime.sessionStorage.view(lane);
	return {
		getLeafId: (context) => delegate.getLeafId(context),
		getEntry: (id, context) => delegate.getEntry(id, context),
		getStats: (context) => delegate.getStats(context),
		getName: (context) => delegate.getName(context),
		setName: (name, context) => delegate.setName(name, context),
		getLabel: (targetId, context) => delegate.getLabel(targetId, context),
		setLabel: (targetId, label, context) => delegate.setLabel(targetId, label, context),
		getCustomFact: (key, context) => delegate.getCustomFact(key, context),
		setCustomFact: (key, value, context) => delegate.setCustomFact(key, value, context),
		findEntries: (query, context) => delegate.findEntries(query, context),
		findEntry: (query, context) => delegate.findEntry(query, context),
		findEntriesOnBranch: (query, context) => delegate.findEntriesOnBranch(query, context),
		findEntryOnBranch: (query, context) => delegate.findEntryOnBranch(query, context),
		appendMessage: (message, context) =>
			appendPublicEntry(runtime, lane, { type: "message", payload: message }, context),
		appendCustomEntry: (customType, data, context) =>
			appendPublicEntry(
				runtime,
				lane,
				{
					type: "custom",
					customType,
					...(data === undefined ? {} : { payload: data }),
				},
				context,
			),
	};
}

export async function acceptLane<TContext extends object | undefined>(
	runtime: LaneRuntimeContext<TContext>,
	lane: RuntimeLane,
	request: OperationRequest,
	context: Context,
): Promise<OperationAdmissionResult> {
	const closed = runtime.resultClosedError();
	if (closed !== undefined) return Result.err(closed);
	if (request.kind === "compaction" || request.kind === "navigation") {
		throw new RuntimeSliceNotImplemented(`accept(${request.kind})`);
	}

	const resources = await runtime.readSettings((settings) => settings.resources, context);
	const normalized = normalizeRunRequest(runtime, request, resources);
	if (!normalized.ok) return normalized;
	const provisional = normalized.value;
	if (provisional.messages.some(isPendingAssistant)) {
		return Result.err(
			new InvalidMessage({
				lane: lane.name,
				reason: "pending_assistant",
				message: "A pending assistant message cannot be accepted",
			}),
		);
	}
	const reserved = deferredValue<void>();
	const reservation: AdmissionReservation = {
		operationId: provisional.operationId,
		operationKind: "run",
		completion: reserved.promise,
		resolve: () => reserved.resolve(undefined),
	};

	try {
		const busy = await runtime.sessionStorage.mutate(
			lane.name,
			async (reader) => {
				const restored = await restoreLane(reader, lane.name, undefined, context);
				const existingReservation = runtime.admissionReservations.get(lane.name);
				if (existingReservation !== undefined) return createLaneBusy(lane.name, existingReservation);
				const active = runtime.activeOperations.get(lane.name);
				if (active !== undefined) return createLaneBusy(lane.name, active);
				if (restored.current !== undefined) {
					return new LaneBusy({
						lane: lane.name,
						operationId: restored.current.operation.operationId,
						operationKind: restored.current.operation.intent.kind,
						message: `Lane ${JSON.stringify(lane.name)} already has an active operation`,
					});
				}
				runtime.admissionReservations.set(lane.name, reservation);
				return undefined;
			},
			context,
		);
		if (busy !== undefined) return Result.err(busy);

		let systemPrompt = "";
		let hookMessages: AgentMessage[] = [];
		let systemPromptOverride: string | undefined;
		let resumeData: Record<string, JsonValue> | undefined;
		if (runtime.hooks.has("before_run")) {
			systemPrompt = (await resolveSystemPrompt(runtime, context)) ?? "";
			await lane.breakpoint.hit({
				kind: "hook.before_run",
				description: "Run pre-acceptance hooks",
				details: { operationId: provisional.operationId },
			});
			runtime.assertOpen();
			const aggregate = await runtime.hooks.runBeforeAcceptanceWithResumeData(
				{
					lane: lane.name,
					runId: provisional.operationId,
					prompt: provisional.messages,
					systemPrompt,
					resources: provisional.resources,
				},
				() => runtime.assertOpen(),
				context,
			);
			hookMessages = aggregate.result?.messages ?? [];
			systemPromptOverride = aggregate.result?.systemPrompt;
			if (Object.keys(aggregate.resumeData).length !== 0) resumeData = aggregate.resumeData;
		}
		const settings = await runtime.snapshotSettings(context);
		const publication = await runtime.sessionStorage.mutate(
			lane.name,
			async (mutator) => {
				if (runtime.admissionReservations.get(lane.name) !== reservation) {
					throw new SessionInvariantError(`Lane ${JSON.stringify(lane.name)} lost its admission reservation`);
				}
				const restored = await restoreLane(mutator, lane.name, undefined, context);
				if (restored.current !== undefined || runtime.activeOperations.has(lane.name)) {
					const owner = restored.current?.operation;
					return Result.err(
						owner === undefined
							? createLaneBusy(lane.name, runtime.activeOperations.get(lane.name)!)
							: new LaneBusy({
									lane: lane.name,
									operationId: owner.operationId,
									operationKind: owner.intent.kind,
									message: `Lane ${JSON.stringify(lane.name)} already has an active operation`,
								}),
					);
				}
				const missing = missingIdentities(runtime.models, restored.configuration, settings.tools);
				if (hasMissingIdentities(missing)) {
					return Result.err(
						new MissingIdentities({
							lane: lane.name,
							...missing,
							message: `Lane ${JSON.stringify(lane.name)} has unresolved model or tool identities`,
						}),
					);
				}

				const pendingIds = [...restored.laneState.pendingNextRun];
				const pendingRegisters = await Promise.all(
					pendingIds.map((id) => mutator.getRegister("pending.entry", id, context)),
				);
				for (let index = 0; index < pendingIds.length; index++) {
					if (pendingRegisters[index] === undefined) {
						throw new SessionInvariantError(`Pending next-run entry ${pendingIds[index]} is missing`);
					}
				}
				const callerIds = provisional.messages.map(() => runtime.sessionStorage.idGenerator.next());
				const hookIds = hookMessages.map(() => runtime.sessionStorage.idGenerator.next());
				const placements: Array<{ id: string; pending: NonNullable<(typeof pendingRegisters)[number]>["value"] }> =
					[];
				for (let index = 0; index < pendingIds.length; index++) {
					placements.push({ id: pendingIds[index]!, pending: pendingRegisters[index]!.value });
				}
				for (let index = 0; index < provisional.messages.length; index++) {
					placements.push({
						id: callerIds[index]!,
						pending: { type: "message", payload: provisional.messages[index]! },
					});
				}
				for (let index = 0; index < hookMessages.length; index++) {
					placements.push({ id: hookIds[index]!, pending: { type: "message", payload: hookMessages[index]! } });
				}
				if (
					placements.some(
						(placement) => placement.pending.type === "message" && isPendingAssistant(placement.pending.payload),
					)
				) {
					return Result.err(
						new InvalidMessage({
							lane: lane.name,
							reason: "pending_assistant",
							message: "A pending assistant message cannot be accepted",
						}),
					);
				}
				if (placements.length === 0) {
					return Result.err(
						new InvalidMessage({
							lane: lane.name,
							reason: "empty",
							message: "A run must place at least one message or pending entry",
						}),
					);
				}
				let parentId = restored.leafId;
				const entryWrites: Array<{ kind: "entry"; entry: NewEntry }> = [];
				for (const placement of placements) {
					const pending = placement.pending;
					const entry: NewEntry =
						pending.type === "message"
							? { id: placement.id, parentId, type: "message", message: pending.payload }
							: {
									id: placement.id,
									parentId,
									type: "custom",
									customType: pending.customType,
									...(pending.payload === undefined ? {} : { data: pending.payload }),
								};
					entryWrites.push({ kind: "entry", entry });
					parentId = placement.id;
				}
				const triggerEntryId = parentId!;
				const operation: OperationMeta = {
					operationId: provisional.operationId,
					lane: lane.name,
					sourceLeafId: restored.leafId,
					startedAt: provisional.startedAt,
					intent: {
						kind: "run",
						promptEntryIds: callerIds,
						...(systemPromptOverride === undefined ? {} : { systemPromptOverride }),
						...(resumeData === undefined ? {} : { resumeData }),
					},
				};
				const state: RunState = {
					kind: "run",
					control: { status: "running" },
					settings: {
						compaction: { ...settings.compaction },
						steeringMode: settings.steeringMode,
						followUpMode: settings.followUpMode,
						toolExecution: settings.toolExecution,
					},
					phase: {
						kind: "checkpoint",
						continuation: { kind: "need_assistant", overflowRecoveryUsed: false },
						triggerEntryId,
						skipInboxOnce: true,
					},
					inbox: { steer: [], followUp: [], writes: [] },
					latestAssistantEntryId: null,
				};
				const result = await mutator.commit(
					{
						writes: [
							...entryWrites,
							...pendingIds.map(
								(id) => ({ kind: "register", op: "delete", namespace: "pending.entry", key: id }) as const,
							),
							{ kind: "register", op: "set", namespace: "lane.leaf", key: lane.name, value: triggerEntryId },
							{
								kind: "register",
								op: "set",
								namespace: "op.meta",
								key: provisional.operationId,
								value: operation,
							},
							{ kind: "register", op: "set", namespace: "op.state", key: provisional.operationId, value: state },
							{
								kind: "register",
								op: "set",
								namespace: "lane.state",
								key: lane.name,
								value: {
									...restored.laneState,
									currentOperationId: provisional.operationId,
									pendingNextRun: [],
								},
							},
						],
					},
					context,
				);
				return Result.ok<AcceptancePublication>({
					admission: { operationId: provisional.operationId, kind: "run", startedAt: provisional.startedAt },
					entries: entryWrites.map((write, index) =>
						materializeCommittedEntry(write.entry, result.seqs[index]!, result.timestamp),
					),
					capturedNextRun: pendingIds.length !== 0,
				});
			},
			context,
		);
		if (!publication.ok) return publication;
		runtime.attachedOperationIds.add(provisional.operationId);
		await runtime.events.emit({ type: "run_start", runId: provisional.operationId, lane: lane.name }, context);
		for (const entry of publication.value.entries) {
			if (entry.type === "message") {
				await runtime.events.emit(
					{
						type: "message_start",
						runId: provisional.operationId,
						message: entry.message,
						lane: lane.name,
					},
					context,
				);
				await runtime.events.emit(
					{
						type: "message_end",
						runId: provisional.operationId,
						message: entry.message,
						entryId: entry.id,
						lane: lane.name,
					},
					context,
				);
			}
			await runtime.events.emit({ type: "entry_added", entry, lane: lane.name }, context);
		}
		if (publication.value.capturedNextRun) {
			await runtime.events.emit(
				{ type: "queue_update", steer: [], followUp: [], nextRun: [], lane: lane.name },
				context,
			);
		}
		return Result.ok(publication.value.admission);
	} catch (error) {
		if (error instanceof HarnessClosed) return Result.err(runtime.closedError());
		if (error instanceof HarnessFault) throw error;
		throw runtime.fault(error, context);
	} finally {
		if (runtime.admissionReservations.get(lane.name) === reservation) {
			runtime.admissionReservations.delete(lane.name);
		}
		reservation.resolve();
	}
}

export async function inspectLane<TContext extends object | undefined>(
	runtime: LaneRuntimeContext<TContext>,
	lane: RuntimeLane,
	context: Context,
): Promise<LaneExecutionInfo> {
	runtime.assertOpen();
	try {
		return await runtime.sessionStorage.mutate(
			lane.name,
			async (reader) => {
				const restored = await restoreLane(reader, lane.name, { includeLastResult: true }, context);
				return {
					lane: lane.name,
					leafId: restored.leafId,
					current: currentInfo(runtime, lane.name, restored),
					...(restored.lastResult === undefined ? {} : { lastResult: restored.lastResult }),
				};
			},
			context,
		);
	} catch (error) {
		throw runtime.fault(error, context);
	}
}

async function updateLaneConfiguration<TContext extends object | undefined>(
	runtime: LaneRuntimeContext<TContext>,
	lane: string,
	update: (configuration: LaneConfiguration) => LaneConfiguration,
	context: Context,
): Promise<{ previous: LaneConfiguration; value: LaneConfiguration }> {
	runtime.assertOpen();
	try {
		return await runtime.sessionStorage.mutate(
			lane,
			async (mutator) => {
				const stored = await mutator.getRegister("lane.config", lane, context);
				if (stored === undefined) {
					throw new SessionInvariantError(`Lane ${JSON.stringify(lane)} is missing lane.config`);
				}
				const value = update(stored.value);
				await mutator.commit(
					{ writes: [{ kind: "register", op: "set", namespace: "lane.config", key: lane, value }] },
					context,
				);
				return { previous: stored.value, value };
			},
			context,
		);
	} catch (error) {
		throw runtime.fault(error, context);
	}
}

async function appendPublicEntry<TContext extends object | undefined>(
	runtime: LaneRuntimeContext<TContext>,
	lane: string,
	pending: PendingEntry,
	context: Context,
): Promise<string> {
	runtime.assertOpen();
	if (pending.type === "message" && pending.payload.role === "assistant" && pending.payload.stopReason === "pending") {
		throw new SessionPendingAssistantMessageError();
	}
	const id = runtime.sessionStorage.idGenerator.next();
	while (true) {
		const disposition = await runtime.sessionStorage.mutate(
			lane,
			async (mutator) => {
				const reservation = runtime.admissionReservations.get(lane);
				if (reservation !== undefined) return { kind: "wait" as const, completion: reservation.completion };
				const [leaf, laneState] = await Promise.all([
					mutator.getRegister("lane.leaf", lane, context),
					mutator.getRegister("lane.state", lane, context),
				]);
				if (leaf === undefined || laneState === undefined) throw new SessionInvariantError(`Unknown lane: ${lane}`);
				const operationId = laneState.value.currentOperationId;
				if (operationId === null) {
					await mutator.commit(
						{
							writes: [
								{
									kind: "entry",
									entry:
										pending.type === "message"
											? { id, parentId: leaf.value, type: "message", message: pending.payload }
											: {
													id,
													parentId: leaf.value,
													type: "custom",
													customType: pending.customType,
													...(pending.payload === undefined ? {} : { data: pending.payload }),
												},
								},
								{ kind: "register", op: "set", namespace: "lane.leaf", key: lane, value: id },
							],
						},
						context,
					);
					return { kind: "done" as const };
				}
				const [operation, state] = await Promise.all([
					mutator.getRegister("op.meta", operationId, context),
					mutator.getRegister("op.state", operationId, context),
				]);
				if (operation === undefined || state === undefined || operation.value.lane !== lane) {
					throw new SessionInvariantError(`Lane ${JSON.stringify(lane)} has incomplete operation state`);
				}
				if (state.value.kind !== "run" || operation.value.intent.kind !== "run") {
					throw new RuntimeSliceNotImplemented("tree write during structural operation");
				}
				await mutator.commit(
					{
						writes: [
							{ kind: "register", op: "set", namespace: "pending.entry", key: id, value: pending },
							{
								kind: "register",
								op: "set",
								namespace: "op.state",
								key: operationId,
								value: {
									...state.value,
									inbox: { ...state.value.inbox, writes: [...state.value.inbox.writes, id] },
								},
							},
						],
					},
					context,
				);
				return { kind: "done" as const };
			},
			context,
		);
		if (disposition.kind === "done") return id;
		await disposition.completion;
		runtime.assertOpen();
	}
}

function normalizeRunRequest<TContext extends object | undefined>(
	runtime: LaneRuntimeContext<TContext>,
	request: Extract<OperationRequest, { kind: "prompt" | "skill" | "prompt_template" }>,
	resources: Resources,
) {
	const operationId = request.operationId ?? runtime.sessionStorage.idGenerator.next();
	const startedAt = Date.now();
	let messages: AgentMessage[];
	if (request.kind === "prompt") {
		if (typeof request.prompt === "string") {
			const content = [
				...(request.prompt.length === 0 ? [] : [{ type: "text" as const, text: request.prompt }]),
				...(request.images ?? []),
			];
			messages = content.length === 0 ? [] : [{ role: "user", content, timestamp: startedAt }];
		} else {
			messages = Array.isArray(request.prompt) ? [...request.prompt] : [request.prompt];
		}
	} else if (request.kind === "skill") {
		const skill = resources.skills?.find((candidate) => candidate.name === request.name);
		if (skill === undefined) {
			return Result.err(new UnknownSkill({ name: request.name, message: `Unknown skill: ${request.name}` }));
		}
		messages = [
			{
				role: "user",
				content: formatSkillInvocation(skill, request.additionalInstructions),
				timestamp: startedAt,
			},
		];
	} else {
		const template = resources.promptTemplates?.find((candidate) => candidate.name === request.name);
		if (template === undefined) {
			return Result.err(
				new UnknownTemplate({ name: request.name, message: `Unknown prompt template: ${request.name}` }),
			);
		}
		messages = [
			{ role: "user", content: formatPromptTemplateInvocation(template, request.args), timestamp: startedAt },
		];
	}
	return Result.ok<NormalizedRunRequest>({ operationId, startedAt, messages, resources });
}

function createLaneBusy(lane: string, owner: Pick<AdmissionReservation, "operationId" | "operationKind">): LaneBusy {
	return new LaneBusy({
		lane,
		operationId: owner.operationId,
		operationKind: owner.operationKind,
		message: `Lane ${JSON.stringify(lane)} already has an active operation`,
	});
}

async function resolveSystemPrompt<TContext extends object | undefined>(
	runtime: LaneRuntimeContext<TContext>,
	context: Context,
): Promise<string | undefined> {
	const source = runtime.systemPromptSource;
	if (source === undefined || typeof source === "string") return source;
	const contextSource = runtime.toolContext;
	const toolContext = typeof contextSource === "function" ? await contextSource(context) : contextSource;
	return source(toolContext as TContext, context);
}

function currentInfo<TContext extends object | undefined>(
	runtime: LaneRuntimeContext<TContext>,
	lane: string,
	restored: RestoredLane,
): CurrentOperationInfo | null {
	const current = restored.current;
	if (current === undefined) return null;
	const active = runtime.activeOperations.get(lane);
	if (active !== undefined && active.operationId !== current.operation.operationId) {
		throw new SessionInvariantError(`Lane ${JSON.stringify(lane)} has a task for another operation`);
	}
	const status =
		current.state.control.status === "cancel_requested"
			? "aborting"
			: active?.operationId === current.operation.operationId
				? "running"
				: "suspended";
	const suspended = status === "suspended" ? suspensionForInspection(runtime, restored) : undefined;
	return {
		id: current.operation.operationId,
		kind: current.state.kind,
		status,
		startedAt: current.operation.startedAt,
		...(suspended === undefined ? {} : { suspended }),
	};
}

function suspensionForInspection<TContext extends object | undefined>(
	runtime: LaneRuntimeContext<TContext>,
	restored: RestoredLane,
): SuspendedOperation | undefined {
	const remembered = runtime.restoredSuspensions.get(restored.lane);
	return remembered?.operationId === restored.current?.operation.operationId ? remembered : undefined;
}

async function createLaneSnapshot<TContext extends object | undefined>(
	runtime: LaneRuntimeContext<TContext>,
	lane: string,
	reader: SessionReader,
	context: Context,
): Promise<LaneSnapshot> {
	const restored = await restoreLane(reader, lane, undefined, context);
	const transcript: Entry[] = [];
	const visited = new Set<string>();
	let entryId = restored.leafId;
	while (entryId !== null) {
		if (visited.has(entryId)) throw new SessionInvariantError(`Lane ${JSON.stringify(lane)} branch contains a cycle`);
		visited.add(entryId);
		const entry = (await reader.getEntries([entryId], context)).get(entryId);
		if (entry === undefined) {
			throw new SessionInvariantError(`Lane ${JSON.stringify(lane)} references missing branch entry ${entryId}`);
		}
		transcript.push(entry);
		if (entry.type === "compaction") break;
		entryId = entry.parentId;
	}
	transcript.reverse();

	const state = restored.current?.state;
	const steerIds = state?.kind === "run" ? state.inbox.steer : [];
	const followUpIds = state?.kind === "run" ? state.inbox.followUp : [];
	const writeIds = state?.kind === "run" ? state.inbox.writes : [];
	const pendingIds = [...new Set([...steerIds, ...followUpIds, ...restored.laneState.pendingNextRun, ...writeIds])];
	const pendingEntries = new Map<string, PendingEntry>();
	for (const id of pendingIds) {
		const pending = await reader.getRegister("pending.entry", id, context);
		if (pending === undefined) {
			throw new SessionInvariantError(`Lane ${JSON.stringify(lane)} references missing pending entry ${id}`);
		}
		pendingEntries.set(id, pending.value);
	}
	const queue = (ids: string[]) =>
		ids.map((id) => {
			const pending = pendingEntries.get(id);
			if (pending?.type !== "message") {
				throw new SessionInvariantError(`Lane ${JSON.stringify(lane)} queue entry ${id} is not a message`);
			}
			return { entryId: id, message: pending.payload };
		});

	const current = currentInfo(runtime, lane, restored);
	let operation: LaneSnapshot["operation"] = null;
	if (current !== null && restored.current !== undefined) {
		const runningTools: NonNullable<LaneSnapshot["operation"]>["runningTools"] = [];
		let retry: NonNullable<LaneSnapshot["operation"]>["retry"];
		if (state?.kind === "run" && state.phase.kind === "tools") {
			const assistant = restored.current.entries.get(state.phase.batch.assistantEntryId);
			if (assistant?.type !== "message" || assistant.message.role !== "assistant") {
				throw new SessionInvariantError(`Lane ${JSON.stringify(lane)} tool batch assistant is missing`);
			}
			const sourceCalls = assistant.message.content.filter((content) => content.type === "toolCall");
			for (const call of state.phase.batch.calls) {
				if (call.status !== "effect_pending") continue;
				const source = sourceCalls[call.sourceIndex];
				const args = restored.current.toolArguments.get(
					`${restored.current.operation.operationId}:${state.phase.batch.turnId}:${call.sourceIndex}`,
				);
				if (source === undefined || args === undefined) {
					throw new SessionInvariantError(`Lane ${JSON.stringify(lane)} running tool state is incomplete`);
				}
				runningTools.push({ toolCallId: source.id, toolName: source.name, args });
			}
		}
		if (state?.kind === "run" && state.phase.kind === "assistant" && state.phase.generation.status === "retry_wait") {
			retry = {
				attempt: state.phase.generation.nextAttempt,
				maxAttempts: state.phase.generation.context.retryPolicy.maxAttempts,
				nextAttemptAt: state.phase.generation.notBefore,
			};
		}
		operation = { ...current, runningTools, ...(retry === undefined ? {} : { retry }) };
	}

	return {
		lane,
		transcript,
		leafId: restored.leafId,
		operation,
		queues: {
			steer: queue(steerIds),
			followUp: queue(followUpIds),
			nextRun: queue(restored.laneState.pendingNextRun),
		},
		pendingWrites: writeIds.map((id) => {
			const pending = pendingEntries.get(id)!;
			return pending.type === "message"
				? { entryId: id, type: "message", message: pending.payload }
				: {
						entryId: id,
						type: "custom",
						customType: pending.customType,
						...(pending.payload === undefined ? {} : { data: pending.payload }),
					};
		}),
		faulted: false,
	};
}

export class AgentLaneRuntime<TContext extends object | undefined> implements AgentLane {
	readonly name: string;
	readonly sessionTree: SessionTree;
	readonly breakpoint: BreakpointBarrier;
	private readonly harness: LaneRuntimeContext<TContext>;

	constructor(harness: LaneRuntimeContext<TContext>, name: string) {
		this.harness = harness;
		this.name = name;
		this.sessionTree = createPublicSessionView(harness, name);
		this.breakpoint = new BreakpointBarrier(harness.driveMode);
	}

	async getLeafId(context: Context): Promise<string | null> {
		this.harness.assertOpen();
		return this.sessionTree.getLeafId(context);
	}

	async getLastResult(context: Context): Promise<LaneLastResult | undefined> {
		this.harness.assertOpen();
		return (await this.harness.sessionStorage.getRegister("lane.lastResult", this.name, context))?.value;
	}

	accept(request: OperationRequest, context: Context): Promise<OperationAdmissionResult> {
		return acceptLane(this.harness, this, request, context);
	}

	drive(options: DriveOptions, context: Context): Promise<DriveResult> {
		return driveLane(this.harness, this, options, context);
	}

	requestAbort(_operationId: string, context: Context): Promise<AbortRequestResult> {
		return this.unimplementedResult("requestAbort", context);
	}

	inspectExecution(context: Context): Promise<LaneExecutionInfo> {
		return inspectLane(this.harness, this, context);
	}

	prompt(text: string, images: ImageContent[] | undefined, context: Context): Promise<RunResult>;
	prompt(message: AgentMessage | AgentMessage[], context: Context): Promise<RunResult>;
	prompt(
		message: string | AgentMessage | AgentMessage[],
		imagesOrContext: ImageContent[] | Context | undefined,
		context?: Context,
	): Promise<RunResult> {
		if (typeof message === "string") {
			if (context === undefined) throw new Error("Context is required");
			return this.runAccepted(
				{
					kind: "prompt",
					prompt: message,
					...(imagesOrContext === undefined ? {} : { images: imagesOrContext as ImageContent[] }),
				},
				context,
			);
		}
		return this.runAccepted({ kind: "prompt", prompt: message }, imagesOrContext as Context);
	}
	skill(name: string, additionalInstructions: string | undefined, context: Context): Promise<RunResult> {
		return this.runAccepted(
			{
				kind: "skill",
				name,
				...(additionalInstructions === undefined ? {} : { additionalInstructions }),
			},
			context,
		);
	}
	promptFromTemplate(name: string, args: string[] | undefined, context: Context): Promise<RunResult> {
		return this.runAccepted({ kind: "prompt_template", name, ...(args === undefined ? {} : { args }) }, context);
	}
	compact(_options: { customInstructions?: string } | undefined, context: Context): Promise<CompactionResult> {
		return this.unimplementedResult("compact", context);
	}
	navigateTree(
		_targetId: string | null,
		_options: NavigateOptions | undefined,
		context: Context,
	): Promise<NavigationResult> {
		return this.unimplementedResult("navigateTree", context);
	}
	resume(context: Context): Promise<ResumeResult> {
		return this.unimplementedResult("resume", context);
	}
	abort(context: Context): Promise<AbortResult> {
		return this.unimplementedResult("abort", context);
	}
	steer(_message: string | AgentMessage, _images: ImageContent[] | undefined, context: Context): Promise<QueueResult> {
		return this.unimplementedResult("steer", context);
	}
	followUp(
		_message: string | AgentMessage,
		_images: ImageContent[] | undefined,
		context: Context,
	): Promise<QueueResult> {
		return this.unimplementedResult("followUp", context);
	}
	nextRun(
		_message: string | AgentMessage,
		_images: ImageContent[] | undefined,
		context: Context,
	): Promise<NextRunResult> {
		return this.unimplementedResult("nextRun", context);
	}
	cancelQueued(_entryId: string, context: Context): Promise<CancelQueuedResult> {
		return this.unimplementedResult("cancelQueued", context);
	}
	recordUsage(
		_usage: Usage,
		_options: { entryId?: string; details?: JsonValue } | undefined,
		context: Context,
	): Promise<RecordUsageResult> {
		return this.unimplementedResult("recordUsage", context);
	}
	waitForIdle(context: Context): Promise<void> {
		return this.notImplemented("waitForIdle", context);
	}
	runWhenIdle(_callback: (context: Context) => void | Promise<void>, context: Context): Promise<void> {
		return this.notImplemented("runWhenIdle", context);
	}
	async peekAction(context: Context): Promise<ActionInfo | undefined> {
		this.harness.assertOpen();
		return this.harness.sessionStorage.mutate(this.name, () => this.breakpoint.peek(), context);
	}
	async executeAction(context: Context): Promise<ActionInfo | undefined> {
		this.harness.assertOpen();
		return this.harness.sessionStorage.mutate(this.name, () => this.breakpoint.release(), context);
	}
	async runToCompletion(_context: Context): Promise<void> {
		this.harness.assertOpen();
		while (true) {
			const active = this.harness.activeOperations.get(this.name);
			if (active === undefined) return;
			const action = this.breakpoint.peek();
			if (action !== undefined) {
				this.breakpoint.release();
				continue;
			}
			await Promise.race([
				active.completion.then(
					() => undefined,
					() => undefined,
				),
				this.breakpoint.waitForChange(),
			]);
		}
	}
	async getModel(context: Context): Promise<Model<Api> | undefined> {
		this.harness.assertOpen();
		const configuration = await this.getConfiguration(context);
		return this.harness.models.getModel(configuration.model.provider, configuration.model.modelId);
	}
	async setModel(model: Model<Api>, context: Context): Promise<void> {
		const { previous, value } = await updateLaneConfiguration(
			this.harness,
			this.name,
			(configuration) => ({
				...configuration,
				model: { provider: model.provider, modelId: model.id },
			}),
			context,
		);
		await this.harness.events.emit(
			{
				type: "config_update",
				property: "model",
				value: { ...value.model },
				previous: { ...previous.model },
				lane: this.name,
			},
			context,
		);
	}
	async getThinkingLevel(context: Context): Promise<ThinkingLevel> {
		return (await this.getConfiguration(context)).thinkingLevel;
	}
	async setThinkingLevel(thinkingLevel: ThinkingLevel, context: Context): Promise<void> {
		const { previous } = await updateLaneConfiguration(
			this.harness,
			this.name,
			(configuration) => ({
				...configuration,
				thinkingLevel,
			}),
			context,
		);
		await this.harness.events.emit(
			{
				type: "config_update",
				property: "thinkingLevel",
				value: thinkingLevel,
				previous: previous.thinkingLevel,
				lane: this.name,
			},
			context,
		);
	}
	async getActiveTools(context: Context): Promise<string[]> {
		return [...(await this.getConfiguration(context)).activeToolNames];
	}
	async setActiveTools(activeToolNames: string[], context: Context): Promise<void> {
		const names = [...activeToolNames];
		const { previous } = await updateLaneConfiguration(
			this.harness,
			this.name,
			(configuration) => ({
				...configuration,
				activeToolNames: names,
			}),
			context,
		);
		await this.harness.events.emit(
			{
				type: "config_update",
				property: "activeTools",
				value: [...names],
				previous: [...previous.activeToolNames],
				lane: this.name,
			},
			context,
		);
	}
	async watch(context: Context): Promise<WatchHandle<LaneSnapshot>> {
		this.harness.assertOpen();
		try {
			return await this.harness.events.watchFromSnapshot(
				(context) =>
					this.harness.sessionStorage.mutate(
						this.name,
						(reader) => createLaneSnapshot(this.harness, this.name, reader, context),
						context,
					),
				(event) => event.type === "usage" || !("lane" in event) || event.lane === this.name,
				context,
			);
		} catch (error) {
			throw this.harness.fault(error, context);
		}
	}

	private async runAccepted(
		request: Extract<OperationRequest, { kind: "prompt" | "skill" | "prompt_template" }>,
		context: Context,
	): Promise<RunResult> {
		const admission = await this.accept(request, context);
		if (!admission.ok) {
			if (
				admission.error._tag === "LaneBusy" ||
				admission.error._tag === "MissingIdentities" ||
				admission.error._tag === "InvalidMessage" ||
				admission.error._tag === "UnknownSkill" ||
				admission.error._tag === "UnknownTemplate" ||
				admission.error._tag === "Closed"
			) {
				return Result.err(admission.error);
			}
			throw new SessionInvariantError("Run acceptance returned a structural-operation error");
		}
		while (true) {
			const driven = await this.drive(
				{
					operationId: admission.value.operationId,
					waitForRetry: true,
					pollDeferred: true,
				},
				context,
			);
			if (!driven.ok) {
				throw new SessionInvariantError("A convenience drive lost ownership of its accepted operation");
			}
			if (driven.value.kind === "yielded") continue;
			if (driven.value.kind !== "settled") {
				if (driven.value.reason === "retry") continue;
				const leafId = await this.getLeafId(context);
				if (leafId === null) throw new SessionInvariantError("A suspended run cannot be at the root");
				if (driven.value.reason === "missing_identities") {
					return Result.ok({
						runId: admission.value.operationId,
						kind: "suspended",
						reason: "missing_identities",
						missing: driven.value.missing,
						leafId,
					});
				}
				return Result.ok({
					runId: admission.value.operationId,
					kind: "suspended",
					reason: "deferred",
					leafId,
					finalEntryId: leafId,
					deferred: driven.value.deferred,
				});
			}
			const outcome = driven.value.outcome;
			if (outcome.operation !== "run")
				throw new SessionInvariantError("Accepted run settled as another operation kind");
			const { operation: _operation, ...run } = outcome;
			return Result.ok(run);
		}
	}

	private async getConfiguration(context: Context): Promise<LaneConfiguration> {
		this.harness.assertOpen();
		try {
			const configuration = await this.harness.sessionStorage.getRegister("lane.config", this.name, context);
			if (configuration === undefined) {
				throw new SessionInvariantError(`Lane ${JSON.stringify(this.name)} is missing lane.config`);
			}
			return configuration.value;
		} catch (error) {
			throw this.harness.fault(error, context);
		}
	}

	private async unimplementedResult(operation: string, _context: Context) {
		const closed = this.harness.resultClosedError();
		if (closed !== undefined) return Result.err(closed);
		throw new RuntimeSliceNotImplemented(operation);
	}

	private async notImplemented<T>(operation: string, _context: Context): Promise<T> {
		this.harness.assertOpen();
		throw new RuntimeSliceNotImplemented(operation);
	}
}
