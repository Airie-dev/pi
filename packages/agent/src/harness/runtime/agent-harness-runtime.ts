import type { Api, ImageContent, Message, Model, RetryPolicy, Usage } from "@earendil-works/pi-ai";
import type { AgentMessage, QueueMode, ThinkingLevel } from "../../types.ts";
import {
	type AbortRequestResult,
	type AbortResult,
	type ActionInfo,
	type AgentHarness,
	type AgentHarnessOptions,
	type AgentLane,
	type CancelQueuedResult,
	Closed,
	type CompactionResult,
	type CreateLaneResult,
	type DriveOptions,
	type DriveResult,
	HarnessClosed,
	type HarnessEvent,
	HarnessFault,
	InvalidLane,
	type LaneExecutionInfo,
	LaneExists,
	type LaneInfo,
	type LaneSnapshot,
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
	type SessionSnapshot,
	type SuspendedOperation,
	UnknownTarget,
	type WatchHandle,
} from "../agent-harness.ts";
import { type CompactionSettings, DEFAULT_COMPACTION_SETTINGS } from "../compaction/compaction.ts";
import {
	DEFAULT_RETRY_POLICY,
	hasMissingIdentities,
	missingIdentities,
	missingToolIdentities,
	validateCompactionSettings,
	validateRetryPolicy,
	validateToolNames,
} from "../config.ts";
import type { Context } from "../context.ts";
import { HarnessEventBus } from "../events.ts";
import { HookRegistry } from "../hooks.ts";
import { convertToLlm } from "../messages.ts";
import { type RestoredLane, restoreLane } from "../restore.ts";
import { Result } from "../result.ts";
import { LaneMutationLine } from "../session/lane-mutations.ts";
import {
	SessionInvalidLaneError,
	SessionInvariantError,
	SessionLaneExistsError,
	SessionUnknownTargetError,
} from "../session/session.ts";
import type {
	EntryProjector,
	JsonValue,
	LaneConfiguration,
	LaneLastResult,
	Session,
	SessionTree,
} from "../session/types.ts";
import type { AgentHarnessTool, AgentHarnessToolContextSource } from "../types.ts";
import { AgentLaneRuntime, createPublicSessionView } from "./lane-runtime.ts";
import { cloneConfiguration, suspensionBase } from "./transitions.ts";
import {
	type ActiveOperation,
	type AdmissionReservation,
	type RuntimeSettings,
	RuntimeSliceNotImplemented,
} from "./types.ts";

export async function createAgentHarness<TContext extends object | undefined = object | undefined>(
	options: AgentHarnessOptions<TContext>,
	context: Context,
): Promise<{ harness: AgentHarness<TContext>; suspended: SuspendedOperation[] }> {
	const runtime = new AgentHarnessRuntime(options);
	try {
		const suspended = await runtime.initialize(context);
		return { harness: runtime, suspended };
	} catch (error) {
		throw runtime.fault(error, context);
	}
}

class AgentHarnessRuntime<TContext extends object | undefined> implements AgentHarness<TContext> {
	readonly name = "main";
	readonly sessionTree: SessionTree;
	readonly hooks: HookRegistry;
	readonly events: HarnessEventBus;
	readonly sessionStorage: Session;
	readonly models: AgentHarnessOptions<TContext>["models"];
	readonly driveMode: "automatic" | "manual";
	readonly seed: LaneConfiguration;
	readonly toolContext: AgentHarnessToolContextSource<TContext> | undefined;
	readonly systemPromptSource: AgentHarnessOptions<TContext>["systemPrompt"];
	readonly toProviderMessages: (messages: AgentMessage[], context: Context) => Message[] | Promise<Message[]>;
	readonly entryProjectors: Readonly<Record<string, EntryProjector>>;
	readonly settingsLine = new LaneMutationLine();
	readonly laneRuntimes = new Map<string, AgentLaneRuntime<TContext>>();
	readonly activeOperations = new Map<string, ActiveOperation>();
	readonly admissionReservations = new Map<string, AdmissionReservation>();
	readonly attachedOperationIds = new Set<string>();
	readonly resumedOperationIds = new Set<string>();
	readonly resumeEventOperationIds = new Set<string>();
	readonly restoredSuspensions = new Map<string, SuspendedOperation>();
	settings: RuntimeSettings<TContext>;
	settingsRevision = 0;
	state: "open" | "faulted" | "closing" | "closed" = "open";
	faultError: HarnessFault | undefined;
	closePromise: Promise<void> | undefined;

	constructor(options: AgentHarnessOptions<TContext>) {
		validateRetryPolicy(options.retry ?? DEFAULT_RETRY_POLICY);
		validateCompactionSettings(options.compaction ?? DEFAULT_COMPACTION_SETTINGS);
		this.sessionStorage = options.session;
		this.models = options.models;
		this.driveMode = options.drive ?? "automatic";
		this.toolContext = options.toolContext;
		this.systemPromptSource = options.systemPrompt;
		this.toProviderMessages = options.toProviderMessages ?? ((messages) => convertToLlm(messages));
		this.entryProjectors = options.entryProjectors ?? {};
		const tools = [...(options.tools ?? [])];
		validateToolNames(tools);
		this.seed = {
			model: { provider: options.model.provider, modelId: options.model.id },
			thinkingLevel: options.thinkingLevel ?? "off",
			activeToolNames: [...(options.activeToolNames ?? tools.map((tool) => tool.name))],
		};
		this.settings = {
			tools,
			resources: options.resources ?? {},
			streamOptions: options.streamOptions ?? {},
			retryPolicy: options.retry ?? DEFAULT_RETRY_POLICY,
			compaction: options.compaction ?? DEFAULT_COMPACTION_SETTINGS,
			steeringMode: options.steeringMode ?? "all",
			followUpMode: options.followUpMode ?? "all",
			toolExecution: options.toolExecution ?? "parallel",
		};
		this.events = new HarnessEventBus();
		this.hooks = new HookRegistry((error, hook, lane, context) =>
			this.events.emit(
				{
					type: "handler_error",
					kind: "hook",
					hook,
					error: error.message,
					...(error.stack === undefined ? {} : { stack: error.stack }),
					lane,
				},
				context,
			),
		);
		this.sessionTree = createPublicSessionView(this, "main");
	}

	async initialize(context: Context): Promise<SuspendedOperation[]> {
		await this.initializeMainConfiguration(context);
		const [leaves, configurations, states, lastResults] = await Promise.all([
			this.sessionStorage.listRegisters("lane.leaf", undefined, context),
			this.sessionStorage.listRegisters("lane.config", undefined, context),
			this.sessionStorage.listRegisters("lane.state", undefined, context),
			this.sessionStorage.listRegisters("lane.lastResult", undefined, context),
		]);
		const leafNames = new Set(leaves.map((register) => register.key));
		if (!leafNames.has("main")) throw new SessionInvariantError("Session is missing main lane");
		for (const register of [...configurations, ...states, ...lastResults]) {
			if (!leafNames.has(register.key)) {
				throw new SessionInvariantError(
					`Lane ${JSON.stringify(register.key)} has ${register.namespace} without lane.leaf`,
				);
			}
		}

		const suspended: SuspendedOperation[] = [];
		for (const { key: lane } of leaves) {
			const restored = await this.sessionStorage.mutate(
				lane,
				(reader) => restoreLane(reader, lane, undefined, context),
				context,
			);
			const runtime = new AgentLaneRuntime(this, lane);
			this.laneRuntimes.set(lane, runtime);
			if (restored.current === undefined) continue;
			const descriptor = this.describeSuspension(restored);
			this.restoredSuspensions.set(lane, descriptor);
			suspended.push(descriptor);
		}
		return suspended;
	}

	async lane(name: string, _context: Context): Promise<AgentLane | undefined> {
		this.assertOpen();
		return this.laneRuntimes.get(name);
	}

	async createLane(name: string, at: string | null, context: Context): Promise<CreateLaneResult> {
		const closed = this.resultClosedError();
		if (closed !== undefined) return Result.err(closed);
		try {
			await this.sessionStorage.createLane(name, at, cloneConfiguration(this.seed), context);
			const lane = new AgentLaneRuntime(this, name);
			this.laneRuntimes.set(name, lane);
			await this.events.emit({ type: "lane_created", lane: name, at }, context);
			return Result.ok(lane);
		} catch (error) {
			if (error instanceof SessionLaneExistsError) {
				return Result.err(new LaneExists({ lane: error.lane, message: error.message }));
			}
			if (error instanceof SessionInvalidLaneError) {
				return Result.err(new InvalidLane({ lane: error.lane, reason: error.reason, message: error.message }));
			}
			if (error instanceof SessionUnknownTargetError) {
				return Result.err(new UnknownTarget({ targetId: error.targetId, message: error.message }));
			}
			throw this.fault(error, context);
		}
	}

	async lanes(context: Context): Promise<LaneInfo[]> {
		this.assertOpen();
		return Promise.all(
			[...this.laneRuntimes.values()].map(async (lane) => {
				const execution = await lane.inspectExecution(context);
				return {
					name: lane.name,
					leafId: execution.leafId,
					operation:
						execution.current === null
							? null
							: {
									id: execution.current.id,
									kind: execution.current.kind,
									status: execution.current.status,
								},
				};
			}),
		);
	}

	getTools(context: Context): Promise<AgentHarnessTool<TContext>[]> {
		return this.readSettings((settings) => [...settings.tools], context);
	}

	setTools(tools: AgentHarnessTool<TContext>[], context: Context): Promise<void> {
		validateToolNames(tools);
		return this.writeSettings(
			(settings) => ({ ...settings, tools: [...tools] }),
			{
				type: "config_update",
				property: "tools",
			},
			context,
		);
	}

	getResources(context: Context): Promise<Resources> {
		return this.readSettings((settings) => settings.resources, context);
	}

	setResources(resources: Resources, context: Context): Promise<void> {
		return this.writeSettings(
			(settings) => ({ ...settings, resources }),
			{
				type: "config_update",
				property: "resources",
			},
			context,
		);
	}

	getStreamOptions(context: Context): Promise<NonNullable<AgentHarnessOptions<TContext>["streamOptions"]>> {
		return this.readSettings((settings) => settings.streamOptions, context);
	}

	setStreamOptions(
		options: NonNullable<AgentHarnessOptions<TContext>["streamOptions"]>,
		context: Context,
	): Promise<void> {
		return this.writeSettings(
			(settings) => ({ ...settings, streamOptions: options }),
			{
				type: "config_update",
				property: "streamOptions",
			},
			context,
		);
	}

	getRetryPolicy(context: Context): Promise<RetryPolicy> {
		return this.readSettings((settings) => settings.retryPolicy, context);
	}

	setRetryPolicy(policy: RetryPolicy, context: Context): Promise<void> {
		validateRetryPolicy(policy);
		return this.writeSettings(
			(settings) => ({ ...settings, retryPolicy: policy }),
			{
				type: "config_update",
				property: "retryPolicy",
			},
			context,
		);
	}

	getCompactionSettings(context: Context): Promise<CompactionSettings> {
		return this.readSettings((settings) => settings.compaction, context);
	}

	setCompactionSettings(compaction: CompactionSettings, context: Context): Promise<void> {
		validateCompactionSettings(compaction);
		return this.writeSettings(
			(settings) => ({ ...settings, compaction }),
			{
				type: "config_update",
				property: "compactionSettings",
			},
			context,
		);
	}

	getSteeringMode(context: Context): Promise<QueueMode> {
		return this.readSettings((settings) => settings.steeringMode, context);
	}

	setSteeringMode(steeringMode: QueueMode, context: Context): Promise<void> {
		return this.writeSettings(
			(settings) => ({ ...settings, steeringMode }),
			{
				type: "config_update",
				property: "steeringMode",
			},
			context,
		);
	}

	getFollowUpMode(context: Context): Promise<QueueMode> {
		return this.readSettings((settings) => settings.followUpMode, context);
	}

	setFollowUpMode(followUpMode: QueueMode, context: Context): Promise<void> {
		return this.writeSettings(
			(settings) => ({ ...settings, followUpMode }),
			{
				type: "config_update",
				property: "followUpMode",
			},
			context,
		);
	}

	async watchSession(_context: Context): Promise<WatchHandle<SessionSnapshot>> {
		this.assertOpen();
		throw new RuntimeSliceNotImplemented("watchSession");
	}

	close(context: Context): Promise<void> {
		if (this.closePromise !== undefined) return this.closePromise;
		this.state = "closing";
		const error = new HarnessClosed();
		this.hooks.close(error);
		this.events.close(error);
		for (const lane of this.laneRuntimes.values()) lane.breakpoint.close(error);
		for (const [lane, active] of this.activeOperations) {
			active.effectGate.close(error);
			active.reject(error);
			this.activeOperations.delete(lane);
		}
		this.closePromise = (async () => {
			await this.settingsLine.seal(error);
			await this.sessionStorage.close(context);
			this.state = "closed";
		})();
		return this.closePromise;
	}

	getLeafId(context: Context): Promise<string | null> {
		return this.mainLane().getLeafId(context);
	}
	getLastResult(context: Context): Promise<LaneLastResult | undefined> {
		return this.mainLane().getLastResult(context);
	}
	accept(request: OperationRequest, context: Context): Promise<OperationAdmissionResult> {
		return this.mainLane().accept(request, context);
	}
	drive(options: DriveOptions, context: Context): Promise<DriveResult> {
		return this.mainLane().drive(options, context);
	}
	requestAbort(operationId: string, context: Context): Promise<AbortRequestResult> {
		return this.mainLane().requestAbort(operationId, context);
	}
	inspectExecution(context: Context): Promise<LaneExecutionInfo> {
		return this.mainLane().inspectExecution(context);
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
			return this.mainLane().prompt(message, imagesOrContext as ImageContent[] | undefined, context);
		}
		return this.mainLane().prompt(message, imagesOrContext as Context);
	}
	skill(name: string, additionalInstructions: string | undefined, context: Context): Promise<RunResult> {
		return this.mainLane().skill(name, additionalInstructions, context);
	}
	promptFromTemplate(name: string, args: string[] | undefined, context: Context): Promise<RunResult> {
		return this.mainLane().promptFromTemplate(name, args, context);
	}
	compact(options: { customInstructions?: string } | undefined, context: Context): Promise<CompactionResult> {
		return this.mainLane().compact(options, context);
	}
	navigateTree(
		targetId: string | null,
		options: NavigateOptions | undefined,
		context: Context,
	): Promise<NavigationResult> {
		return this.mainLane().navigateTree(targetId, options, context);
	}
	resume(context: Context): Promise<ResumeResult> {
		return this.mainLane().resume(context);
	}
	abort(context: Context): Promise<AbortResult> {
		return this.mainLane().abort(context);
	}
	steer(message: string | AgentMessage, images: ImageContent[] | undefined, context: Context): Promise<QueueResult> {
		return this.mainLane().steer(message, images, context);
	}
	followUp(
		message: string | AgentMessage,
		images: ImageContent[] | undefined,
		context: Context,
	): Promise<QueueResult> {
		return this.mainLane().followUp(message, images, context);
	}
	nextRun(
		message: string | AgentMessage,
		images: ImageContent[] | undefined,
		context: Context,
	): Promise<NextRunResult> {
		return this.mainLane().nextRun(message, images, context);
	}
	cancelQueued(entryId: string, context: Context): Promise<CancelQueuedResult> {
		return this.mainLane().cancelQueued(entryId, context);
	}
	recordUsage(
		usage: Usage,
		options: { entryId?: string; details?: JsonValue } | undefined,
		context: Context,
	): Promise<RecordUsageResult> {
		return this.mainLane().recordUsage(usage, options, context);
	}
	waitForIdle(context: Context): Promise<void> {
		return this.mainLane().waitForIdle(context);
	}
	runWhenIdle(callback: (context: Context) => void | Promise<void>, context: Context): Promise<void> {
		return this.mainLane().runWhenIdle(callback, context);
	}
	peekAction(context: Context): Promise<ActionInfo | undefined> {
		return this.mainLane().peekAction(context);
	}
	executeAction(context: Context): Promise<ActionInfo | undefined> {
		return this.mainLane().executeAction(context);
	}
	runToCompletion(context: Context): Promise<void> {
		return this.mainLane().runToCompletion(context);
	}
	getModel(context: Context): Promise<Model<Api> | undefined> {
		return this.mainLane().getModel(context);
	}
	setModel(model: Model<Api>, context: Context): Promise<void> {
		return this.mainLane().setModel(model, context);
	}
	getThinkingLevel(context: Context): Promise<ThinkingLevel> {
		return this.mainLane().getThinkingLevel(context);
	}
	setThinkingLevel(level: ThinkingLevel, context: Context): Promise<void> {
		return this.mainLane().setThinkingLevel(level, context);
	}
	getActiveTools(context: Context): Promise<string[]> {
		return this.mainLane().getActiveTools(context);
	}
	setActiveTools(names: string[], context: Context): Promise<void> {
		return this.mainLane().setActiveTools(names, context);
	}
	watch(context: Context): Promise<WatchHandle<LaneSnapshot>> {
		return this.mainLane().watch(context);
	}

	describeSuspension(restored: RestoredLane): SuspendedOperation {
		const current = restored.current;
		if (current === undefined) throw new SessionInvariantError(`Lane ${restored.lane} is not suspended`);
		const base = suspensionBase(restored);
		if (current.state.kind === "run" && current.state.phase.kind === "deferred") {
			const entry = current.entries.get(current.state.phase.deferred.sourceEntryId);
			if (entry?.type !== "message" || entry.message.role !== "assistant" || entry.message.deferred === undefined) {
				throw new SessionInvariantError("Deferred suspension source is invalid");
			}
			return { ...base, reason: "deferred", deferred: entry.message.deferred };
		}
		if (
			current.state.kind === "run" &&
			current.state.phase.kind === "assistant" &&
			current.state.phase.generation.status === "ready"
		) {
			const missing = missingIdentities(
				this.models,
				current.state.phase.generation.context.configuration,
				this.settings.tools,
			);
			if (hasMissingIdentities(missing)) {
				return { ...base, reason: "crash", missing };
			}
		}
		if (current.state.kind === "run" && current.state.phase.kind === "tools") {
			const missing = missingToolIdentities(current.state.phase.batch.configuration, this.settings.tools);
			if (missing.length !== 0) return { ...base, reason: "crash", missing: { tools: missing } };
		}
		return { ...base, reason: "crash" };
	}

	assertOpen(): void {
		if (this.faultError !== undefined) throw this.faultError;
		if (!this.isOpen()) throw new HarnessClosed();
	}

	isOpen(): boolean {
		return this.state === "open";
	}

	closedError(): Closed {
		return new Closed({ message: "AgentHarness is closed" });
	}

	resultClosedError(): Closed | undefined {
		if (this.faultError !== undefined) throw this.faultError;
		return this.isOpen() ? undefined : this.closedError();
	}

	fault(cause: unknown, context: Context): HarnessFault | HarnessClosed {
		if (this.faultError !== undefined) return this.faultError;
		if (this.state === "closing" || this.state === "closed") return new HarnessClosed();
		const normalized = cause instanceof Error ? cause : new Error(String(cause));
		const fault = new HarnessFault("AgentHarness storage or invariant fault", normalized);
		this.faultError = fault;
		this.state = "faulted";
		this.hooks.close(fault);
		for (const lane of this.laneRuntimes.values()) lane.breakpoint.close(fault);
		for (const [lane, active] of this.activeOperations) {
			active.effectGate.close(fault);
			active.reject(fault);
			this.activeOperations.delete(lane);
		}
		void this.events.emit({ type: "fault", code: "harness_fault", message: fault.message }, context);
		this.events.close(fault);
		return fault;
	}

	async snapshotSettings(context: Context): Promise<RuntimeSettings<TContext>> {
		return this.readSettings(
			(settings) => ({
				...settings,
				tools: [...settings.tools],
				streamOptions: { ...settings.streamOptions },
				retryPolicy: { ...settings.retryPolicy },
				compaction: { ...settings.compaction },
			}),
			context,
		);
	}

	private mainLane(): AgentLaneRuntime<TContext> {
		const lane = this.laneRuntimes.get("main");
		if (lane === undefined) throw new SessionInvariantError("AgentHarness main lane is not initialized");
		return lane;
	}

	private async initializeMainConfiguration(context: Context): Promise<void> {
		await this.sessionStorage.mutate(
			"main",
			async (mutator) => {
				const [leaf, state, configuration, lastResult] = await Promise.all([
					mutator.getRegister("lane.leaf", "main", context),
					mutator.getRegister("lane.state", "main", context),
					mutator.getRegister("lane.config", "main", context),
					mutator.getRegister("lane.lastResult", "main", context),
				]);
				if (leaf === undefined || state === undefined) {
					throw new SessionInvariantError("Session main lane has incomplete durable state");
				}
				if (configuration !== undefined) return;
				if (state.value.currentOperationId !== null || lastResult !== undefined) {
					throw new SessionInvariantError("Configured or active main lane is missing lane.config");
				}
				await mutator.commit(
					{
						writes: [
							{
								kind: "register",
								op: "set",
								namespace: "lane.config",
								key: "main",
								value: cloneConfiguration(this.seed),
							},
						],
					},
					context,
				);
			},
			context,
		);
	}

	async readSettings<T>(read: (settings: RuntimeSettings<TContext>) => T, _context: Context): Promise<T> {
		this.assertOpen();
		return this.settingsLine.run("settings", () => read(this.settings));
	}

	private async writeSettings(
		update: (settings: RuntimeSettings<TContext>) => RuntimeSettings<TContext>,
		event: Extract<HarnessEvent, { type: "config_update" }>,
		context: Context,
	): Promise<void> {
		this.assertOpen();
		await this.settingsLine.run("settings", () => {
			this.settings = update(this.settings);
			this.settingsRevision++;
		});
		await this.events.emit(event, context);
	}
}
