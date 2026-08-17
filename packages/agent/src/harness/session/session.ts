import { uuidv7 } from "@earendil-works/pi-ai";
import type { AgentMessage } from "../../types.ts";
import type { Context } from "../context.ts";
import { LaneMutationLine } from "./lane-mutations.ts";
import type {
	BranchScan,
	CommitResult,
	Entry,
	EntryQuery,
	IdGenerator,
	JsonValue,
	LaneConfiguration,
	OperationMeta,
	OperationState,
	PendingEntry,
	Register,
	RegisterNamespace,
	Session,
	SessionMetadata,
	SessionMutator,
	SessionStats,
	SessionTree,
	Storage,
	Transaction,
} from "./types.ts";

interface StorageBackedSessionConcurrency {
	laneMutationLine?: LaneMutationLine;
}

/** Durable session state is internally inconsistent and cannot be safely advanced. */
export class SessionInvariantError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "SessionInvariantError";
	}
}

/** A requested session lane name is invalid. */
export class SessionInvalidLaneError extends Error {
	readonly lane: string;
	readonly reason: string;

	constructor(lane: string, reason: string) {
		super(`Invalid lane ${JSON.stringify(lane)}: ${reason}`);
		this.name = "SessionInvalidLaneError";
		this.lane = lane;
		this.reason = reason;
	}
}

/** A requested session lane already exists. */
export class SessionLaneExistsError extends Error {
	readonly lane: string;

	constructor(lane: string) {
		super(`Lane already exists: ${lane}`);
		this.name = "SessionLaneExistsError";
		this.lane = lane;
	}
}

/** A pending assistant message cannot be persisted as a session entry. */
export class SessionPendingAssistantMessageError extends Error {
	constructor() {
		super("Cannot persist a pending assistant message");
		this.name = "SessionPendingAssistantMessageError";
	}
}

/** A requested session entry target does not exist. */
export class SessionUnknownTargetError extends Error {
	readonly targetId: string;

	constructor(targetId: string) {
		super(`Unknown target: ${targetId}`);
		this.name = "SessionUnknownTargetError";
		this.targetId = targetId;
	}
}

class StorageBackedSessionMutator implements SessionMutator {
	readonly lane: string;
	private readonly storage: Storage;
	private active = true;
	private commitResult: Promise<CommitResult> | undefined;

	constructor(lane: string, storage: Storage) {
		this.lane = lane;
		this.storage = storage;
	}

	commit(transaction: Transaction, context: Context): Promise<CommitResult> {
		this.assertActive();
		if (this.commitResult !== undefined) return Promise.reject(new Error("SessionMutator commit already attempted"));
		try {
			for (const write of transaction.writes) {
				if (
					write.kind === "entry" &&
					write.entry.type === "message" &&
					write.entry.message.role === "assistant" &&
					write.entry.message.stopReason === "pending"
				) {
					throw new SessionPendingAssistantMessageError();
				}
			}
			this.commitResult = this.storage.commit(transaction, context);
		} catch (error) {
			this.commitResult = Promise.reject(error);
		}
		return this.commitResult;
	}

	getEntries(ids: string[], context: Context): Promise<Map<string, Entry>> {
		this.assertActive();
		return this.storage.getEntries(ids, context);
	}

	getRegister<TNamespace extends RegisterNamespace>(
		namespace: TNamespace,
		key: string,
		context: Context,
	): Promise<Register<TNamespace> | undefined> {
		this.assertActive();
		return this.storage.getRegister(namespace, key, context);
	}

	listRegisters<TNamespace extends RegisterNamespace>(
		namespace: TNamespace,
		keyPrefix: string | undefined,
		context: Context,
	): Promise<Register<TNamespace>[]> {
		this.assertActive();
		return this.storage.listRegisters(namespace, keyPrefix, context);
	}

	settle(): Promise<void> {
		return (
			this.commitResult?.then(
				() => undefined,
				() => undefined,
			) ?? Promise.resolve()
		);
	}

	invalidate(): void {
		this.active = false;
	}

	private assertActive(): void {
		if (!this.active) throw new Error("SessionMutator cannot be used outside its mutation callback");
	}
}

/** Package-internal typed boundary shared by concrete session repositories. */
export class StorageBackedSession<TMetadata extends SessionMetadata = SessionMetadata> implements Session<TMetadata> {
	readonly metadata: TMetadata;
	readonly idGenerator: IdGenerator = { next: uuidv7 };
	private readonly storage: Storage;
	private readonly laneMutationLine: LaneMutationLine;
	private readonly closedError = new Error("Session is closed");
	private state: "open" | "closing" | "closed" = "open";
	private closePromise: Promise<void> | undefined;

	constructor(metadata: TMetadata, storage: Storage, concurrency: StorageBackedSessionConcurrency = {}) {
		this.metadata = metadata;
		this.storage = storage;
		this.laneMutationLine = concurrency.laneMutationLine ?? new LaneMutationLine();
	}

	async mutate<T>(
		lane: string,
		mutation: (mutator: SessionMutator, context: Context) => T | Promise<T>,
		context: Context,
	): Promise<T> {
		this.assertOpen();
		return this.laneMutationLine.run(lane, async () => {
			const mutator = new StorageBackedSessionMutator(lane, this.storage);
			try {
				try {
					return await mutation(mutator, context);
				} finally {
					await mutator.settle();
				}
			} finally {
				mutator.invalidate();
			}
		});
	}

	async getEntries(ids: string[], context: Context): Promise<Map<string, Entry>> {
		this.assertOpen();
		return this.storage.getEntries(ids, context);
	}

	async getRegister<TNamespace extends RegisterNamespace>(
		namespace: TNamespace,
		key: string,
		context: Context,
	): Promise<Register<TNamespace> | undefined> {
		this.assertOpen();
		return this.storage.getRegister(namespace, key, context);
	}

	async listRegisters<TNamespace extends RegisterNamespace>(
		namespace: TNamespace,
		keyPrefix: string | undefined,
		context: Context,
	): Promise<Register<TNamespace>[]> {
		this.assertOpen();
		return this.storage.listRegisters(namespace, keyPrefix, context);
	}

	view(lane: string): SessionTree {
		return {
			getLeafId: (context) => this.getLeafIdForLane(lane, context),
			getEntry: (id, context) => this.getEntry(id, context),
			getStats: (context) => this.getStats(context),
			getName: (context) => this.getName(context),
			setName: (name, context) => this.setNameForLane(lane, name, context),
			getLabel: (targetId, context) => this.getLabel(targetId, context),
			setLabel: (targetId, label, context) => this.setLabelForLane(lane, targetId, label, context),
			getCustomFact: (key, context) => this.getCustomFact(key, context),
			setCustomFact: (key, value, context) => this.setCustomFactForLane(lane, key, value, context),
			findEntries: (query, context) => this.findEntries(query, context),
			findEntry: (query, context) => this.findEntry(query, context),
			findEntriesOnBranch: (query, context) => this.findEntriesOnBranchForLane(lane, query, context),
			findEntryOnBranch: (query, context) => this.findEntryOnBranchForLane(lane, query, context),
			appendMessage: (message, context) => this.appendMessageForLane(lane, message, context),
			appendCustomEntry: (customType, data, context) =>
				this.appendCustomEntryForLane(lane, customType, data, context),
		};
	}

	async createLane(
		name: string,
		at: string | null,
		configuration: LaneConfiguration,
		context: Context,
	): Promise<SessionTree> {
		this.assertOpen();
		if (name.length === 0) throw new SessionInvalidLaneError(name, "lane name must not be empty");
		return this.mutate(
			name,
			async (mutator) => {
				// R1 owns complete idle-lane and current-state validation. Slice 2 only
				// distinguishes valid existing lane shapes from partial durable lane state.
				const [leaf, storedConfiguration, laneState, lastResult] = await Promise.all([
					mutator.getRegister("lane.leaf", name, context),
					mutator.getRegister("lane.config", name, context),
					mutator.getRegister("lane.state", name, context),
					mutator.getRegister("lane.lastResult", name, context),
				]);
				const presentCount = [leaf, storedConfiguration, laneState, lastResult].filter(
					(register) => register !== undefined,
				).length;
				if (
					leaf !== undefined &&
					laneState !== undefined &&
					(storedConfiguration !== undefined || (name === "main" && lastResult === undefined))
				) {
					throw new SessionLaneExistsError(name);
				}
				if (presentCount !== 0) {
					throw new SessionInvariantError(`Lane ${JSON.stringify(name)} has incomplete durable state`);
				}
				if (at !== null && !(await mutator.getEntries([at], context)).has(at))
					throw new SessionUnknownTargetError(at);

				// R6 adds the harness-wide admission barrier. Until then, close may reject
				// this lane job before Storage.commit admits it; admitted commits still drain.
				await mutator.commit(
					{
						writes: [
							{ kind: "register", op: "set", namespace: "lane.config", key: name, value: configuration },
							{ kind: "register", op: "set", namespace: "lane.leaf", key: name, value: at },
							{
								kind: "register",
								op: "set",
								namespace: "lane.state",
								key: name,
								value: { currentOperationId: null, pendingNextRun: [] },
							},
						],
					},
					context,
				);
				return this.view(name);
			},
			context,
		);
	}

	getLeafId(context: Context): Promise<string | null> {
		return this.getLeafIdForLane("main", context);
	}

	async getEntry(id: string, context: Context): Promise<Entry | undefined> {
		return (await this.getEntries([id], context)).get(id);
	}

	async getStats(context: Context): Promise<SessionStats> {
		this.assertOpen();
		return this.storage.getStats(context);
	}

	async getName(context: Context): Promise<string | undefined> {
		return (await this.getRegister("fact.name", "", context))?.value;
	}

	setName(name: string | undefined, context: Context): Promise<void> {
		return this.setNameForLane("main", name, context);
	}

	async getLabel(targetId: string, context: Context): Promise<string | undefined> {
		return (await this.getRegister("fact.label", targetId, context))?.value;
	}

	setLabel(targetId: string, label: string | undefined, context: Context): Promise<void> {
		return this.setLabelForLane("main", targetId, label, context);
	}

	async getCustomFact(key: string, context: Context): Promise<JsonValue | undefined> {
		return (await this.getRegister("fact.custom", key, context))?.value;
	}

	setCustomFact(key: string, value: JsonValue | undefined, context: Context): Promise<void> {
		return this.setCustomFactForLane("main", key, value, context);
	}

	async findEntries(query: EntryQuery | undefined, context: Context): Promise<Entry[]> {
		query ??= {};
		this.assertOpen();
		const order = query.order ?? "desc";
		if (query.cursor !== undefined) {
			if (order === "asc" && query.cursor.seq === Number.MAX_SAFE_INTEGER) return [];
			if (order === "desc" && query.cursor.seq <= 1) return [];
		}
		return this.storage.scanEntries(
			{
				type: query.type,
				customType: query.customType,
				order,
				limit: query.limit,
				...(query.cursor === undefined
					? {}
					: order === "asc"
						? { fromSeq: query.cursor.seq + 1 }
						: { toSeq: query.cursor.seq - 1 }),
			},
			context,
		);
	}

	async findEntry(query: EntryQuery | undefined, context: Context): Promise<Entry | undefined> {
		query ??= {};
		const entries = await this.findEntries(
			{
				...query,
				limit: query.limit === undefined ? 1 : Math.min(query.limit, 1),
			},
			context,
		);
		return entries[0];
	}

	findEntriesOnBranch(query: BranchScan | undefined, context: Context): Promise<Entry[]> {
		return this.findEntriesOnBranchForLane("main", query, context);
	}

	findEntryOnBranch(query: BranchScan | undefined, context: Context): Promise<Entry | undefined> {
		return this.findEntryOnBranchForLane("main", query, context);
	}

	appendMessage(message: AgentMessage, context: Context): Promise<string> {
		return this.captureAppend("main", { type: "message", payload: message }, context);
	}

	appendCustomEntry(customType: string, data: JsonValue | undefined, context: Context): Promise<string> {
		return this.captureAppend(
			"main",
			{
				type: "custom",
				customType,
				...(data === undefined ? {} : { payload: data }),
			},
			context,
		);
	}

	close(context: Context): Promise<void> {
		if (this.closePromise !== undefined) return this.closePromise;
		this.state = "closing";
		this.closePromise = this.laneMutationLine
			.seal(this.closedError)
			.then(() => this.storage.close(context))
			.finally(() => {
				this.state = "closed";
			});
		return this.closePromise;
	}

	private async getLeafIdForLane(lane: string, context: Context): Promise<string | null> {
		const leaf = await this.getRegister("lane.leaf", lane, context);
		if (leaf === undefined) throw new Error(`Unknown lane: ${lane}`);
		return leaf.value;
	}

	private async findEntriesOnBranchForLane(
		lane: string,
		query: BranchScan | undefined,
		context: Context,
	): Promise<Entry[]> {
		this.assertOpen();
		query ??= {};
		const start = query.start ?? (await this.getLeafIdForLane(lane, context));
		if (start === null) return [];
		return this.storage.scanBranch({ ...query, start, order: query.order ?? "newestFirst" }, context);
	}

	private async findEntryOnBranchForLane(
		lane: string,
		query: BranchScan | undefined,
		context: Context,
	): Promise<Entry | undefined> {
		query ??= {};
		const entries = await this.findEntriesOnBranchForLane(
			lane,
			{
				...query,
				limit: query.limit === undefined ? 1 : Math.min(query.limit, 1),
			},
			context,
		);
		return entries[0];
	}

	private setNameForLane(lane: string, name: string | undefined, context: Context): Promise<void> {
		return this.mutate(
			lane,
			async (mutator) => {
				await mutator.commit(
					{
						writes: [
							name === undefined
								? { kind: "register", op: "delete", namespace: "fact.name", key: "" }
								: { kind: "register", op: "set", namespace: "fact.name", key: "", value: name },
						],
					},
					context,
				);
			},
			context,
		);
	}

	private setLabelForLane(lane: string, targetId: string, label: string | undefined, context: Context): Promise<void> {
		return this.mutate(
			lane,
			async (mutator) => {
				await mutator.commit(
					{
						writes: [
							label === undefined
								? { kind: "register", op: "delete", namespace: "fact.label", key: targetId }
								: { kind: "register", op: "set", namespace: "fact.label", key: targetId, value: label },
						],
					},
					context,
				);
			},
			context,
		);
	}

	private setCustomFactForLane(
		lane: string,
		key: string,
		value: JsonValue | undefined,
		context: Context,
	): Promise<void> {
		return this.mutate(
			lane,
			async (mutator) => {
				await mutator.commit(
					{
						writes: [
							value === undefined
								? { kind: "register", op: "delete", namespace: "fact.custom", key }
								: { kind: "register", op: "set", namespace: "fact.custom", key, value },
						],
					},
					context,
				);
			},
			context,
		);
	}

	private appendMessageForLane(lane: string, message: AgentMessage, context: Context): Promise<string> {
		return this.captureAppend(lane, { type: "message", payload: message }, context);
	}

	private appendCustomEntryForLane(
		lane: string,
		customType: string,
		data: JsonValue | undefined,
		context: Context,
	): Promise<string> {
		return this.captureAppend(
			lane,
			{
				type: "custom",
				customType,
				...(data === undefined ? {} : { payload: data }),
			},
			context,
		);
	}

	private async captureAppend(lane: string, pending: PendingEntry, context: Context): Promise<string> {
		this.assertOpen();
		if (
			pending.type === "message" &&
			pending.payload.role === "assistant" &&
			pending.payload.stopReason === "pending"
		) {
			throw new SessionPendingAssistantMessageError();
		}
		return this.appendCaptured(lane, this.idGenerator.next(), pending, context);
	}

	private async appendCaptured(lane: string, id: string, pending: PendingEntry, context: Context): Promise<string> {
		await this.mutate(lane, (mutator) => this.appendCapturedIfReady(mutator, id, pending, context), context);
		return id;
	}

	private async appendCapturedIfReady(
		mutator: SessionMutator,
		id: string,
		pending: PendingEntry,
		context: Context,
	): Promise<void> {
		const { lane } = mutator;
		const [leaf, laneState] = await Promise.all([
			mutator.getRegister("lane.leaf", lane, context),
			mutator.getRegister("lane.state", lane, context),
		]);
		if (leaf === undefined) throw new SessionInvariantError(`Unknown lane: ${lane}`);
		if (laneState === undefined)
			throw new SessionInvariantError(`Lane ${JSON.stringify(lane)} is missing lane.state`);
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
			return;
		}

		const [operation, operationState] = await Promise.all([
			mutator.getRegister("op.meta", operationId, context),
			mutator.getRegister("op.state", operationId, context),
		]);
		if (operation === undefined) {
			throw new SessionInvariantError(`Active operation ${operationId} is missing op.meta`);
		}
		if (operationState === undefined) {
			throw new SessionInvariantError(`Active operation ${operationId} is missing op.state`);
		}
		this.validateCurrentOperation(lane, operation.value, operationState.value);
		if (operationState.value.kind !== "run") {
			// TODO: Tree writes during structural operations must wait for the operation to finish,
			// then re-evaluate the lane state. That coordination is not yet implemented.
			throw new Error(`Cannot append while structural operation ${operationId} is active`);
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
							...operationState.value,
							inbox: {
								...operationState.value.inbox,
								writes: [...operationState.value.inbox.writes, id],
							},
						},
					},
				],
			},
			context,
		);
	}

	private validateCurrentOperation(lane: string, operation: OperationMeta, state: OperationState): void {
		if (operation.lane !== lane) {
			throw new SessionInvariantError(
				`Active operation ${operation.operationId} belongs to lane ${JSON.stringify(operation.lane)}, not ${JSON.stringify(lane)}`,
			);
		}
		if (operation.intent.kind !== state.kind) {
			throw new SessionInvariantError(
				`Active operation ${operation.operationId} intent ${operation.intent.kind} does not match state ${state.kind}`,
			);
		}
	}

	private assertOpen(): void {
		if (this.state !== "open") throw this.closedError;
	}
}
