import { randomUUID } from "node:crypto";
import { createConnection, type Socket } from "node:net";
import { isAbsolute, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
	AgentHarness,
	type AgentHarness as AgentHarnessInstance,
	createBashTool,
	createReadTool,
	createWriteTool,
	type JsonlSessionMetadata,
	JsonlSessionRepo,
	type Session,
} from "@earendil-works/pi-agent-core";
import { NodeExecutionEnv } from "@earendil-works/pi-agent-core/node";
import {
	createRpcCallSchema,
	createRpcDispatcher,
	createRpcResultSchema,
	defineRpc,
	LaneEventSchema,
	LaneSnapshotSchema,
	PromptArgumentsSchema,
	type RpcCall,
	type RpcResultUnion,
	RunResultSchema,
} from "@earendil-works/pi-protocol";
import lockfile from "proper-lockfile";
import Type, { type Static } from "typebox";
import { Check } from "typebox/value";
import { findInitialModel, resolveCliModel } from "../core/model-resolver.ts";
import { ModelRuntime } from "../core/model-runtime.ts";
import { SettingsManager } from "../core/settings-manager.ts";
import { BACKGROUND_CONTEXT } from "./context.ts";
import {
	toHarnessPromptArguments,
	toWireLaneEvent,
	toWireLaneSnapshot,
	toWireRunResult,
} from "./harness-wire-adapter.ts";
import { consumeInternalProcessRole, encodeControlLine, MAX_CONTROL_LINE_BYTES } from "./process.ts";

const StrictObject = <const T extends Parameters<typeof Type.Object>[0]>(properties: T) =>
	Type.Object(properties, { additionalProperties: false });

export const SESSION_WORKER_CONTROL_ADDRESS_ENV = "PI_SESSION_WORKER_CONTROL_ADDRESS";
export const SESSION_WORKER_CONTROL_TOKEN_ENV = "PI_SESSION_WORKER_CONTROL_TOKEN";
export const SESSION_WORKER_SESSION_KEY_ENV = "PI_SESSION_WORKER_SESSION_KEY_BASE64";
export const SESSION_WORKER_PEER_ID_ENV = "PI_SESSION_WORKER_PEER_ID";

export const SessionWorkerMetadataSchema = StrictObject({
	id: Type.String({ minLength: 1 }),
	createdAt: Type.Integer(),
	storageVersion: Type.Integer(),
	cwd: Type.String(),
	path: Type.String(),
	modifiedAt: Type.Number(),
	parentSessionId: Type.Optional(Type.String()),
	legacyParentSessionPath: Type.Optional(Type.String()),
});

export const SessionWorkerOptionsSchema = StrictObject({
	sessionDir: Type.String({ minLength: 1 }),
	metadata: SessionWorkerMetadataSchema,
	provider: Type.Optional(Type.String({ minLength: 1 })),
	model: Type.Optional(Type.String({ minLength: 1 })),
});
export type SessionWorkerOptions = Static<typeof SessionWorkerOptionsSchema>;

export const SessionWorkerOperations = defineRpc({
	prompt: {
		args: Type.Tuple([PromptArgumentsSchema]),
		result: RunResultSchema,
	},
	watch: {
		args: Type.Tuple([]),
		result: StrictObject({ watchId: Type.String({ minLength: 1 }), snapshot: LaneSnapshotSchema }),
	},
	startWatch: {
		args: Type.Tuple([Type.String({ minLength: 1 })]),
		result: StrictObject({ watchId: Type.String({ minLength: 1 }) }),
	},
	stopWatch: {
		args: Type.Tuple([Type.String({ minLength: 1 })]),
		result: StrictObject({ watchId: Type.String({ minLength: 1 }) }),
	},
});
export type SessionWorkerOperationCall = RpcCall<typeof SessionWorkerOperations>;
type SessionWorkerOperationResult = RpcResultUnion<typeof SessionWorkerOperations>;

const SessionWorkerOperationCallSchema = Type.Unsafe<SessionWorkerOperationCall>(
	createRpcCallSchema(SessionWorkerOperations),
);
const SessionWorkerOperationResultSchema = Type.Unsafe<SessionWorkerOperationResult>(
	createRpcResultSchema(SessionWorkerOperations),
);

export const WorkerOperationScopeSchema = StrictObject({
	serverConnectionId: Type.String(),
	attachmentId: Type.String(),
});
export type WorkerOperationScope = Static<typeof WorkerOperationScopeSchema>;

export const WorkerOperationRequestSchema = StrictObject({
	type: Type.Literal("operation"),
	requestId: Type.String({ minLength: 1 }),
	scope: WorkerOperationScopeSchema,
	call: SessionWorkerOperationCallSchema,
});
export type WorkerOperationRequest = Static<typeof WorkerOperationRequestSchema>;

export const WorkerOperationResponseSchema = Type.Union([
	StrictObject({
		type: Type.Literal("operation_result"),
		requestId: Type.String({ minLength: 1 }),
		scope: WorkerOperationScopeSchema,
		result: SessionWorkerOperationResultSchema,
	}),
	StrictObject({
		type: Type.Literal("operation_error"),
		requestId: Type.String({ minLength: 1 }),
		scope: WorkerOperationScopeSchema,
		message: Type.String(),
	}),
]);
export type WorkerOperationResponse = Static<typeof WorkerOperationResponseSchema>;

export const SessionWorkerCommandSchema = Type.Union([
	Type.Object({ type: Type.Literal("shutdown") }),
	Type.Object({ type: Type.Literal("discover_workers") }),
	Type.Object({
		type: Type.Literal("session_demand"),
		serverConnectionId: Type.String(),
		requestId: Type.String(),
		attachmentId: Type.Union([Type.String(), Type.Null()]),
	}),
	WorkerOperationRequestSchema,
]);
export type SessionWorkerCommand = Static<typeof SessionWorkerCommandSchema>;

export const SessionWorkerEventSchema = Type.Union([
	Type.Object({
		type: Type.Literal("worker_ready"),
		token: Type.String(),
		sessionKey: Type.String(),
		sessionId: Type.String(),
		pid: Type.Integer({ minimum: 1 }),
		metadata: SessionWorkerMetadataSchema,
	}),
	Type.Object({
		type: Type.Literal("worker_failed"),
		token: Type.String(),
		sessionKey: Type.String(),
		message: Type.String(),
	}),
	Type.Object({
		type: Type.Literal("demand_applied"),
		token: Type.String(),
		sessionKey: Type.String(),
		requestId: Type.String(),
		attachmentId: Type.Union([Type.String(), Type.Null()]),
	}),
	Type.Object({
		type: Type.Literal("demand_rejected"),
		token: Type.String(),
		sessionKey: Type.String(),
		requestId: Type.String(),
		message: Type.String(),
	}),
	Type.Object({
		type: Type.Literal("operation_response"),
		token: Type.String(),
		sessionKey: Type.String(),
		response: WorkerOperationResponseSchema,
	}),
	Type.Object({
		type: Type.Literal("lane_event"),
		token: Type.String(),
		sessionKey: Type.String(),
		scope: WorkerOperationScopeSchema,
		watchId: Type.String({ minLength: 1 }),
		event: LaneEventSchema,
	}),
]);
export type SessionWorkerEvent = Static<typeof SessionWorkerEventSchema>;

/** Worker-local reconciliation of server-generation demand and Harness activity. */
export class WorkerLifecycle {
	readonly #initialDemandGraceMs: number;
	readonly #orphanDemandGraceMs: number;
	readonly #onRetire: () => void;
	readonly #demands = new Map<string, { attachmentId: string; timer?: NodeJS.Timeout }>();
	readonly #activeOperations = new Set<string>();
	#currentServerConnectionId: string | undefined;
	#initialTimer: NodeJS.Timeout | undefined;
	#demandInitialized: boolean;
	#retirementHolds = 0;
	#retiring = false;

	constructor(options: {
		initialServerConnectionId?: string;
		initialDemandGraceMs: number;
		orphanDemandGraceMs: number;
		onRetire(): void;
	}) {
		this.#currentServerConnectionId = options.initialServerConnectionId;
		this.#initialDemandGraceMs = options.initialDemandGraceMs;
		this.#orphanDemandGraceMs = options.orphanDemandGraceMs;
		this.#onRetire = options.onRetire;
		this.#demandInitialized = false;
		this.#initialTimer = setTimeout(() => {
			this.#initialTimer = undefined;
			this.#demandInitialized = true;
			this.#reconcile();
		}, this.#initialDemandGraceMs);
		this.#initialTimer.unref();
	}

	serverConnected(serverConnectionId: string): void {
		this.#currentServerConnectionId = serverConnectionId;
		const demand = this.#demands.get(serverConnectionId);
		if (demand?.timer) {
			clearTimeout(demand.timer);
			delete demand.timer;
		}
	}

	serverDisconnected(serverConnectionId: string): void {
		if (this.#currentServerConnectionId === serverConnectionId) this.#currentServerConnectionId = undefined;
		const demand = this.#demands.get(serverConnectionId);
		if (!demand || demand.timer) return;
		demand.timer = setTimeout(() => {
			if (this.#demands.get(serverConnectionId) !== demand) return;
			this.#demands.delete(serverConnectionId);
			this.#reconcile();
		}, this.#orphanDemandGraceMs);
		demand.timer.unref();
	}

	beginRequest(serverConnectionId: string, attachmentId: string): () => void {
		if (this.#retiring) throw new Error("Session worker is retiring");
		if (serverConnectionId !== this.#currentServerConnectionId) {
			throw new Error("Session worker received a request from a stale server generation");
		}
		const demand = this.#demands.get(serverConnectionId);
		if (!demand || demand.timer || demand.attachmentId !== attachmentId) {
			throw new Error("Session worker request does not match the active attachment");
		}
		return this.holdRetirement();
	}

	holdRetirement(): () => void {
		this.#retirementHolds += 1;
		let released = false;
		return () => {
			if (released) return;
			released = true;
			this.#retirementHolds -= 1;
			this.#reconcile();
		};
	}

	setDemand(serverConnectionId: string, attachmentId: string | null): void {
		if (this.#retiring) throw new Error("Session worker is retiring");
		if (serverConnectionId !== this.#currentServerConnectionId) {
			throw new Error("Session worker received demand from a stale server generation");
		}
		this.#demandInitialized = true;
		if (this.#initialTimer) {
			clearTimeout(this.#initialTimer);
			this.#initialTimer = undefined;
		}
		const previous = this.#demands.get(serverConnectionId);
		if (previous?.timer) clearTimeout(previous.timer);
		if (attachmentId === null) this.#demands.delete(serverConnectionId);
		else this.#demands.set(serverConnectionId, { attachmentId });
		this.#reconcile();
	}

	operationStarted(kind: "run" | "compaction" | "navigation", lane: string, operationId: string): void {
		this.#activeOperations.add(`${kind}\0${lane}\0${operationId}`);
	}

	operationStopped(kind: "run" | "compaction" | "navigation", lane: string, operationId: string): void {
		this.#activeOperations.delete(`${kind}\0${lane}\0${operationId}`);
		this.#reconcile();
	}

	close(): void {
		if (this.#initialTimer) clearTimeout(this.#initialTimer);
		for (const demand of this.#demands.values()) {
			if (demand.timer) clearTimeout(demand.timer);
		}
		this.#demands.clear();
	}

	#reconcile(): void {
		if (
			this.#retiring ||
			!this.#demandInitialized ||
			this.#retirementHolds !== 0 ||
			this.#activeOperations.size !== 0 ||
			this.#demands.size !== 0
		) {
			return;
		}
		this.#retiring = true;
		this.#onRetire();
	}
}

const DEFAULT_INITIAL_DEMAND_GRACE_MS = 10_000;
const DEFAULT_ORPHAN_DEMAND_GRACE_MS = 30_000;
export const SESSION_WORKER_INITIAL_DEMAND_GRACE_ENV = "__PI_SESSION_WORKER_INITIAL_DEMAND_GRACE_MS";
export const SESSION_WORKER_ORPHAN_DEMAND_GRACE_ENV = "__PI_SESSION_WORKER_ORPHAN_DEMAND_GRACE_MS";

const CoordinatorInputSchema = Type.Union([
	Type.Object({
		type: Type.Literal("peer_registered"),
		peerId: Type.String(),
		serverConnectionId: Type.Optional(Type.String()),
	}),
	Type.Object({ type: Type.Literal("server_connected"), serverConnectionId: Type.String() }),
	Type.Object({ type: Type.Literal("server_disconnected"), serverConnectionId: Type.String() }),
	Type.Object({ type: Type.Literal("message"), from: Type.Literal("server"), payload: Type.Unknown() }),
]);
type CoordinatorInput = Static<typeof CoordinatorInputSchema>;

interface WorkerControl {
	readonly initialServerConnectionId?: string;
	readonly messages: AsyncIterable<unknown>;
	readonly socket: Socket;
	send(event: SessionWorkerEvent): Promise<void>;
}

let failureControl: WorkerControl | undefined;

async function connectControl(): Promise<WorkerControl> {
	const address = process.env[SESSION_WORKER_CONTROL_ADDRESS_ENV];
	const token = process.env[SESSION_WORKER_CONTROL_TOKEN_ENV];
	const encodedSessionKey = process.env[SESSION_WORKER_SESSION_KEY_ENV];
	if (!address || !token || !encodedSessionKey) throw new Error("Session worker requires a control address");
	const peerId = process.env[SESSION_WORKER_PEER_ID_ENV];
	if (!peerId) throw new Error("Session worker requires a peer ID");
	const socket = createConnection(address);
	await new Promise<void>((resolve, reject) => {
		socket.once("connect", resolve);
		socket.once("error", reject);
	});
	const messages = createJsonLineMessages(socket);
	await writeJsonLine(socket, { type: "register_peer", protocol: 1, peerId });
	const registered = await messages[Symbol.asyncIterator]().next();
	if (
		registered.done ||
		!Check(CoordinatorInputSchema, registered.value) ||
		registered.value.type !== "peer_registered"
	) {
		throw new Error("Coordinator rejected the session worker registration");
	}
	return {
		...(registered.value.serverConnectionId === undefined
			? {}
			: { initialServerConnectionId: registered.value.serverConnectionId }),
		messages,
		socket,
		send: (event) => writeJsonLine(socket, { type: "send", to: "server", payload: event }),
	};
}

async function readCommands(
	control: WorkerControl,
	handlers: {
		onShutdown(): void;
		onDiscovery(): void;
		onDemand(command: Extract<SessionWorkerCommand, { type: "session_demand" }>): Promise<void>;
		onOperation(command: WorkerOperationRequest): void;
		onServerConnected(serverConnectionId: string): void;
		onServerDisconnected(serverConnectionId: string): void;
	},
): Promise<void> {
	for await (const value of control.messages) {
		if (!Check(CoordinatorInputSchema, value)) {
			control.socket.destroy(new Error("Coordinator sent an invalid worker message"));
			return;
		}
		const message: CoordinatorInput = value;
		if (message.type === "server_connected") {
			handlers.onServerConnected(message.serverConnectionId);
			continue;
		}
		if (message.type === "server_disconnected") {
			handlers.onServerDisconnected(message.serverConnectionId);
			continue;
		}
		if (message.type !== "message" || !Check(SessionWorkerCommandSchema, message.payload)) continue;
		const command: SessionWorkerCommand = message.payload;
		if (command.type === "shutdown") handlers.onShutdown();
		else if (command.type === "discover_workers") handlers.onDiscovery();
		else if (command.type === "session_demand") await handlers.onDemand(command);
		else handlers.onOperation(command);
	}
}

function createJsonLineMessages(socket: Socket): AsyncIterable<unknown> {
	const queued: unknown[] = [];
	const waiters: ((value: unknown) => void)[] = [];
	let buffered = "";
	socket.setEncoding("utf8");
	socket.on("data", (chunk: string) => {
		buffered += chunk;
		if (Buffer.byteLength(buffered) > MAX_CONTROL_LINE_BYTES) {
			socket.destroy(new Error("Session worker control message is too large"));
			return;
		}
		while (true) {
			const newline = buffered.indexOf("\n");
			if (newline === -1) return;
			const line = buffered.slice(0, newline);
			buffered = buffered.slice(newline + 1);
			try {
				const value: unknown = JSON.parse(line);
				const waiter = waiters.shift();
				if (waiter) waiter(value);
				else queued.push(value);
			} catch {
				socket.destroy(new Error("Session worker received invalid control JSON"));
				return;
			}
		}
	});
	return {
		[Symbol.asyncIterator]() {
			return {
				next: async () => {
					const value = queued.shift() ?? (await new Promise<unknown>((resolve) => waiters.push(resolve)));
					return { done: false as const, value };
				},
			};
		},
	};
}

function writeJsonLine(socket: Socket, message: unknown): Promise<void> {
	return new Promise((resolve, reject) => {
		socket.write(encodeControlLine(message), (error) => {
			if (error) reject(error);
			else resolve();
		});
	});
}

function sameScope(left: WorkerOperationScope, right: WorkerOperationScope): boolean {
	return left.serverConnectionId === right.serverConnectionId && left.attachmentId === right.attachmentId;
}

function lifecycleDelay(name: string, fallback: number): number {
	const value = process.env[name];
	if (value === undefined) return fallback;
	const parsed = Number(value);
	if (!Number.isSafeInteger(parsed) || parsed < 0) throw new Error(`${name} must be a non-negative safe integer`);
	return parsed;
}

async function closeResources(resources: {
	harness?: AgentHarnessInstance;
	session?: Session<JsonlSessionMetadata>;
	repo: JsonlSessionRepo;
	executionEnv: NodeExecutionEnv;
	releaseOwnership: () => Promise<void>;
}): Promise<void> {
	const errors: unknown[] = [];
	try {
		if (resources.harness) await resources.harness.close(BACKGROUND_CONTEXT);
		else await resources.session?.close(BACKGROUND_CONTEXT);
	} catch (error) {
		errors.push(error);
	}
	try {
		await resources.repo.close(BACKGROUND_CONTEXT);
	} catch (error) {
		errors.push(error);
	}
	try {
		await resources.executionEnv.cleanup(BACKGROUND_CONTEXT);
	} catch (error) {
		errors.push(error);
	}
	try {
		await resources.releaseOwnership();
	} catch (error) {
		errors.push(error);
	}
	if (errors.length === 1) throw errors[0];
	if (errors.length > 1) throw new AggregateError(errors, "Session worker cleanup failed");
}

export type CreateSessionWorkerHarness = (
	session: Session<JsonlSessionMetadata>,
	options: SessionWorkerOptions,
	executionEnv: NodeExecutionEnv,
) => Promise<AgentHarnessInstance>;

async function run(options: SessionWorkerOptions, createHarness: CreateSessionWorkerHarness): Promise<void> {
	const { sessionDir, metadata } = options;
	const sessionId = metadata.id;
	const control = await connectControl();
	const token = process.env[SESSION_WORKER_CONTROL_TOKEN_ENV]!;
	const sessionKey = Buffer.from(process.env[SESSION_WORKER_SESSION_KEY_ENV]!, "base64url").toString();
	failureControl = control;
	const releaseOwnership = await lockfile.lock(metadata.path, {
		realpath: true,
		stale: 2_000,
		update: 1_000,
		retries: { retries: 320, factor: 1, minTimeout: 25, maxTimeout: 25, maxRetryTime: 8_000 },
	});
	const executionEnv = new NodeExecutionEnv({ cwd: metadata.cwd });
	const repo = new JsonlSessionRepo({ fileSystem: executionEnv, sessionsRoot: sessionDir });
	let session: Session<JsonlSessionMetadata> | undefined;
	let harness: AgentHarnessInstance | undefined;
	try {
		session = await repo.open(metadata, BACKGROUND_CONTEXT);
		harness = await createHarness(session, options, executionEnv);
	} catch (error) {
		try {
			await closeResources({ harness, session, repo, executionEnv, releaseOwnership });
		} catch (cleanupError) {
			throw new AggregateError([error, cleanupError], "Session worker startup and cleanup failed");
		}
		throw error;
	}

	const laneWatches = new Map<
		string,
		{ readonly scope: WorkerOperationScope; readonly handle: Awaited<ReturnType<AgentHarnessInstance["watch"]>> }
	>();
	const removeLaneWatches = (matches: (scope: WorkerOperationScope) => boolean): void => {
		for (const [watchId, watch] of laneWatches) {
			if (!matches(watch.scope)) continue;
			watch.handle.unsubscribe();
			laneWatches.delete(watchId);
		}
	};
	let lifecycle: WorkerLifecycle | undefined;
	let removeLifecycleListeners: (() => void)[] = [];
	let closing: Promise<void> | undefined;
	const close = (): Promise<void> => {
		if (closing) return closing;
		lifecycle?.close();
		removeLaneWatches(() => true);
		for (const remove of removeLifecycleListeners) remove();
		removeLifecycleListeners = [];
		closing = closeResources({ harness, repo, executionEnv, releaseOwnership });
		return closing;
	};
	const closeAndExit = (): void => {
		void close().then(
			() => process.exit(0),
			(error: unknown) => {
				console.error(error);
				process.exit(1);
			},
		);
	};

	lifecycle = new WorkerLifecycle({
		initialServerConnectionId: control.initialServerConnectionId,
		initialDemandGraceMs: lifecycleDelay(SESSION_WORKER_INITIAL_DEMAND_GRACE_ENV, DEFAULT_INITIAL_DEMAND_GRACE_MS),
		orphanDemandGraceMs: lifecycleDelay(SESSION_WORKER_ORPHAN_DEMAND_GRACE_ENV, DEFAULT_ORPHAN_DEMAND_GRACE_MS),
		onRetire: closeAndExit,
	});
	removeLifecycleListeners = [
		harness.events.on("run_start", (event) => lifecycle?.operationStarted("run", event.lane, event.runId)),
		harness.events.on("run_resume", (event) => lifecycle?.operationStarted("run", event.lane, event.runId)),
		harness.events.on("run_suspend", (event) => lifecycle?.operationStopped("run", event.lane, event.runId)),
		harness.events.on("run_end", (event) => lifecycle?.operationStopped("run", event.lane, event.runId)),
		harness.events.on("compaction_start", (event) =>
			lifecycle?.operationStarted("compaction", event.lane, event.runId),
		),
		harness.events.on("compaction_suspend", (event) =>
			lifecycle?.operationStopped("compaction", event.lane, event.runId),
		),
		harness.events.on("compaction_end", (event) =>
			lifecycle?.operationStopped("compaction", event.lane, event.runId),
		),
		harness.events.on("navigation_start", (event) =>
			lifecycle?.operationStarted("navigation", event.lane, event.runId),
		),
		harness.events.on("navigation_suspend", (event) =>
			lifecycle?.operationStopped("navigation", event.lane, event.runId),
		),
		harness.events.on("navigation_end", (event) =>
			lifecycle?.operationStopped("navigation", event.lane, event.runId),
		),
		harness.events.on("fault", closeAndExit),
	];

	const dispatchWorkerOperation = createRpcDispatcher(SessionWorkerOperations, {
		prompt: async (_scope: WorkerOperationScope, prompt) => {
			const args = toHarnessPromptArguments(prompt);
			const result =
				typeof args[0] === "string"
					? await harness.prompt(args[0], args[1], BACKGROUND_CONTEXT)
					: await harness.prompt(args[0], BACKGROUND_CONTEXT);
			return toWireRunResult(result);
		},
		watch: async (scope: WorkerOperationScope, ..._args: never[]) => {
			const handle = await harness.watch(BACKGROUND_CONTEXT);
			const watchId = randomUUID();
			laneWatches.set(watchId, { scope, handle });
			return { watchId, snapshot: toWireLaneSnapshot(handle.snapshot) };
		},
		startWatch: async (scope: WorkerOperationScope, watchId) => {
			const watch = laneWatches.get(watchId);
			if (!watch || !sameScope(watch.scope, scope)) throw new Error("Session worker lane watch was not found");
			watch.handle.start(async (event) => {
				const wireEvent = toWireLaneEvent(event);
				if (wireEvent === undefined) return;
				await control.send({ type: "lane_event", token, sessionKey, scope, watchId, event: wireEvent });
			});
			return { watchId };
		},
		stopWatch: async (scope: WorkerOperationScope, watchId) => {
			const watch = laneWatches.get(watchId);
			if (!watch || !sameScope(watch.scope, scope)) throw new Error("Session worker lane watch was not found");
			watch.handle.unsubscribe();
			laneWatches.delete(watchId);
			return { watchId };
		},
	});
	const handleOperation = async (request: WorkerOperationRequest): Promise<void> => {
		let releaseRequest = (): void => {};
		try {
			releaseRequest = lifecycle!.beginRequest(request.scope.serverConnectionId, request.scope.attachmentId);
			const result = await dispatchWorkerOperation(request.call, request.scope);
			await control.send({
				type: "operation_response",
				token,
				sessionKey,
				response: { type: "operation_result", requestId: request.requestId, scope: request.scope, result },
			});
		} catch (error) {
			await control.send({
				type: "operation_response",
				token,
				sessionKey,
				response: {
					type: "operation_error",
					requestId: request.requestId,
					scope: request.scope,
					message: error instanceof Error ? error.message : String(error),
				},
			});
		} finally {
			releaseRequest();
		}
	};

	let ready = false;
	const announce = (): void => {
		if (!ready) return;
		void control
			.send({
				type: "worker_ready",
				token,
				sessionKey,
				sessionId,
				pid: process.pid,
				metadata,
			})
			.catch(() => closeAndExit());
	};
	void readCommands(control, {
		onShutdown: closeAndExit,
		onDiscovery: announce,
		onDemand: async (command) => {
			const releaseRetirement = lifecycle?.holdRetirement() ?? (() => {});
			try {
				try {
					removeLaneWatches(
						(scope) =>
							scope.serverConnectionId === command.serverConnectionId &&
							scope.attachmentId !== command.attachmentId,
					);
					lifecycle?.setDemand(command.serverConnectionId, command.attachmentId);
				} catch (error) {
					await control.send({
						type: "demand_rejected",
						token,
						sessionKey,
						requestId: command.requestId,
						message: error instanceof Error ? error.message : String(error),
					});
					return;
				}
				await control.send({
					type: "demand_applied",
					token,
					sessionKey,
					requestId: command.requestId,
					attachmentId: command.attachmentId,
				});
			} finally {
				releaseRetirement();
			}
		},
		onOperation: (request) => {
			void handleOperation(request).catch(() => closeAndExit());
		},
		onServerConnected: (serverConnectionId) => lifecycle?.serverConnected(serverConnectionId),
		onServerDisconnected: (serverConnectionId) => {
			removeLaneWatches((scope) => scope.serverConnectionId === serverConnectionId);
			lifecycle?.serverDisconnected(serverConnectionId);
		},
	}).catch(() => closeAndExit());
	control.socket.once("close", closeAndExit);
	control.socket.once("error", () => closeAndExit());
	process.once("SIGTERM", closeAndExit);
	process.once("SIGINT", closeAndExit);

	try {
		ready = true;
		await control.send({ type: "worker_ready", token, sessionKey, sessionId, pid: process.pid, metadata });
	} catch (error) {
		try {
			await close();
		} catch (cleanupError) {
			throw new AggregateError([error, cleanupError], "Session worker readiness and cleanup failed");
		}
		throw error;
	}
}

export async function runSessionWorkerWithHarness(
	args: readonly string[],
	createHarness: CreateSessionWorkerHarness,
): Promise<void> {
	try {
		if (args.length !== 1) throw new Error("Session worker requires one options argument");
		let options: unknown;
		try {
			options = JSON.parse(args[0]!);
		} catch (error) {
			throw new Error("Session worker received invalid options", { cause: error });
		}
		if (
			!Check(SessionWorkerOptionsSchema, options) ||
			!isAbsolute(options.sessionDir) ||
			!isAbsolute(options.metadata.cwd) ||
			!isAbsolute(options.metadata.path) ||
			(options.provider !== undefined && options.model === undefined)
		) {
			throw new Error("Session worker received invalid options");
		}
		await run(options, createHarness);
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		const token = process.env[SESSION_WORKER_CONTROL_TOKEN_ENV];
		const encodedSessionKey = process.env[SESSION_WORKER_SESSION_KEY_ENV];
		if (token && encodedSessionKey) {
			const sessionKey = Buffer.from(encodedSessionKey, "base64url").toString();
			await failureControl?.send({ type: "worker_failed", token, sessionKey, message }).catch(() => {});
		}
		throw error;
	}
}

async function createCodingAgentHarness(
	session: Session<JsonlSessionMetadata>,
	options: SessionWorkerOptions,
	executionEnv: NodeExecutionEnv,
): Promise<AgentHarnessInstance> {
	const modelRuntime = await ModelRuntime.create();
	let resolved: Awaited<ReturnType<typeof findInitialModel>> | ReturnType<typeof resolveCliModel>;
	if (options.model === undefined) {
		const settingsManager = SettingsManager.create(session.metadata.cwd);
		resolved = await findInitialModel({
			scopedModels: [],
			isContinuing: true,
			defaultProvider: settingsManager.getDefaultProvider(),
			defaultModelId: settingsManager.getDefaultModel(),
			defaultThinkingLevel: settingsManager.getDefaultThinkingLevel(),
			modelRuntime,
		});
	} else {
		resolved = resolveCliModel({
			cliProvider: options.provider,
			cliModel: options.model,
			modelRuntime,
		});
		if (resolved.error) throw new Error(`Session worker could not resolve model: ${resolved.error}`);
	}
	if (!resolved.model) throw new Error("Session worker could not resolve a model");
	const tools = [createReadTool(), createWriteTool(), createBashTool()];
	const activeToolNames = tools.map((tool) => tool.name);
	const harness = (
		await AgentHarness.create(
			{
				session,
				models: modelRuntime,
				model: resolved.model,
				thinkingLevel: resolved.thinkingLevel,
				tools,
				activeToolNames,
				toolContext: { env: executionEnv },
				resources: {},
			},
			BACKGROUND_CONTEXT,
		)
	).harness;
	try {
		const currentActiveToolNames = await harness.getActiveTools(BACKGROUND_CONTEXT);
		if (
			currentActiveToolNames.length !== activeToolNames.length ||
			currentActiveToolNames.some((name, index) => name !== activeToolNames[index])
		) {
			await harness.setActiveTools(activeToolNames, BACKGROUND_CONTEXT);
		}
		if (options.model !== undefined) {
			const currentModel = await harness.getModel(BACKGROUND_CONTEXT);
			if (
				!currentModel ||
				currentModel.provider !== resolved.model.provider ||
				currentModel.id !== resolved.model.id
			) {
				await harness.setModel(resolved.model, BACKGROUND_CONTEXT);
			}
			if (
				resolved.thinkingLevel !== undefined &&
				(await harness.getThinkingLevel(BACKGROUND_CONTEXT)) !== resolved.thinkingLevel
			) {
				await harness.setThinkingLevel(resolved.thinkingLevel, BACKGROUND_CONTEXT);
			}
		}
		return harness;
	} catch (error) {
		try {
			await harness.close(BACKGROUND_CONTEXT);
		} catch (cleanupError) {
			throw new AggregateError([error, cleanupError], "Session worker model selection and cleanup failed");
		}
		throw error;
	}
}

export function runSessionWorkerProcess(args: readonly string[]): Promise<void> {
	return runSessionWorkerWithHarness(args, createCodingAgentHarness);
}

if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
	const role = consumeInternalProcessRole();
	if (role !== "session-worker") {
		throw new Error("Session worker entrypoint requires an internal session-worker invocation");
	}
	void runSessionWorkerProcess(process.argv.slice(2)).catch(() => process.exit(1));
}
