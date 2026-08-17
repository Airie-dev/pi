import type { Session, SessionMetadata } from "@earendil-works/pi-agent-core";
import { BACKGROUND_CONTEXT, MemorySessionRepo } from "@earendil-works/pi-agent-core";
import type { LaneEvent, LaneSnapshot, PromptArguments, RunResult } from "@earendil-works/pi-protocol";
import type { HostedHarnessHandle, HostedHarnessWatch, PiServerHost } from "../types.ts";

export class Deferred<T> {
	readonly promise: Promise<T>;
	private resolvePromise!: (value: T) => void;

	constructor() {
		this.promise = new Promise<T>((resolve) => {
			this.resolvePromise = resolve;
		});
	}

	resolve(value: T): void {
		this.resolvePromise(value);
	}
}

interface OpenGate {
	entered: Deferred<void>;
	release: Deferred<void>;
}

const emptyLaneSnapshot: LaneSnapshot = {
	lane: "main",
	transcript: [],
	leafId: null,
	operation: null,
	queues: { steer: [], followUp: [], nextRun: [] },
	pendingWrites: [],
	faulted: false,
};

class TestHarnessWatch implements HostedHarnessWatch {
	readonly snapshot: LaneSnapshot;
	private readonly buffered: LaneEvent[] = [];
	private listener: ((event: LaneEvent) => void | Promise<void>) | undefined;
	private tail: Promise<void> = Promise.resolve();
	private state: "buffering" | "started" | "unsubscribed" = "buffering";

	constructor(snapshot: LaneSnapshot) {
		this.snapshot = structuredClone(snapshot);
	}

	start(listener: (event: LaneEvent) => void | Promise<void>): void {
		if (this.state !== "buffering") throw new Error("Test Harness watch may be started only once");
		this.state = "started";
		this.listener = listener;
		for (const event of this.buffered.splice(0)) this.enqueue(event);
	}

	unsubscribe(): void {
		this.state = "unsubscribed";
		this.buffered.splice(0);
		this.listener = undefined;
	}

	push(event: LaneEvent): Promise<void> {
		if (this.state === "unsubscribed") return Promise.resolve();
		if (this.state === "buffering") {
			this.buffered.push(structuredClone(event));
			return Promise.resolve();
		}
		return this.enqueue(event);
	}

	private enqueue(event: LaneEvent): Promise<void> {
		const listener = this.listener;
		if (listener === undefined) return Promise.resolve();
		const delivery = this.tail.then(() => listener(structuredClone(event)));
		this.tail = delivery.catch(() => {});
		return delivery;
	}
}

export class TestHarness {
	readonly session: Session;
	readonly closed = new Deferred<void>();
	readonly #termination = new Deferred<Error | undefined>();
	readonly terminated = this.#termination.promise;
	attachedClients = 0;
	attachmentReleaseCount = 0;
	closeCount = 0;
	readonly promptCalls: PromptArguments[] = [];
	watchSnapshot: LaneSnapshot = structuredClone(emptyLaneSnapshot);
	private readonly watches = new Set<TestHarnessWatch>();
	failAttachmentRelease?: Error;
	failClose?: Error;
	nextPromptError?: Error;
	nextPromptResult?: RunResult;
	private nextCloseGate?: OpenGate;
	private nextPromptGate?: OpenGate;

	constructor(session: Session) {
		this.session = session;
	}

	attachClient(): { release(): void } {
		this.attachedClients += 1;
		let released = false;
		return {
			release: () => {
				if (released) return;
				this.attachmentReleaseCount += 1;
				if (this.failAttachmentRelease) throw this.failAttachmentRelease;
				released = true;
				this.attachedClients -= 1;
			},
		};
	}

	async watch(): Promise<HostedHarnessWatch> {
		const watch = new TestHarnessWatch(this.watchSnapshot);
		this.watches.add(watch);
		return {
			snapshot: watch.snapshot,
			start: (listener) => watch.start(listener),
			unsubscribe: () => {
				watch.unsubscribe();
				this.watches.delete(watch);
			},
		};
	}

	async emitEvent(event: LaneEvent): Promise<void> {
		await Promise.all([...this.watches].map((watch) => watch.push(event)));
	}

	async prompt(prompt: PromptArguments): Promise<RunResult> {
		this.promptCalls.push(prompt);
		if (this.nextPromptError) {
			const error = this.nextPromptError;
			this.nextPromptError = undefined;
			throw error;
		}
		const gate = this.nextPromptGate;
		if (gate) {
			this.nextPromptGate = undefined;
			gate.entered.resolve(undefined);
			await gate.release.promise;
		}
		const result = this.nextPromptResult ?? {
			ok: true,
			value: { kind: "completed", runId: "run-1", leafId: "leaf-1" },
		};
		this.nextPromptResult = undefined;
		return result;
	}

	async close(): Promise<void> {
		this.closeCount += 1;
		const gate = this.nextCloseGate;
		if (gate) {
			this.nextCloseGate = undefined;
			gate.entered.resolve(undefined);
			await gate.release.promise;
		}
		if (this.failClose) {
			const error = this.failClose;
			this.failClose = undefined;
			throw error;
		}
		await this.session.close(BACKGROUND_CONTEXT);
		this.closed.resolve(undefined);
		this.#termination.resolve(undefined);
	}

	async terminate(error: Error): Promise<void> {
		await this.session.close(BACKGROUND_CONTEXT);
		this.#termination.resolve(error);
	}

	gateNextClose(): OpenGate {
		const gate = { entered: new Deferred<void>(), release: new Deferred<void>() };
		this.nextCloseGate = gate;
		return gate;
	}

	gateNextPrompt(): OpenGate {
		const gate = { entered: new Deferred<void>(), release: new Deferred<void>() };
		this.nextPromptGate = gate;
		return gate;
	}
}

interface ListDelay {
	entered: Deferred<void>;
	release: Deferred<void>;
}

export class TestServerHost implements PiServerHost {
	readonly repo = new MemorySessionRepo({ now: () => 1 });
	readonly harnesses = new Map<string, TestHarness[]>();
	createHarnessCount = 0;
	nextCreateHarnessError?: Error;
	nextHarnessCloseError?: Error;
	readonly sessions: PiServerHost["sessions"] = {
		list: async () => {
			const delay = this.nextListDelay;
			if (delay) {
				this.nextListDelay = undefined;
				delay.entered.resolve(undefined);
				await delay.release.promise;
			}
			return this.repo.list(undefined, BACKGROUND_CONTEXT);
		},
		create: async ({ id }) => {
			const session = await this.repo.create({ id }, BACKGROUND_CONTEXT);
			try {
				return session.metadata;
			} finally {
				await session.close(BACKGROUND_CONTEXT);
			}
		},
	};
	private nextListDelay?: ListDelay;
	private nextCreateHarnessGate?: OpenGate;

	async createHarness(metadata: SessionMetadata): Promise<HostedHarnessHandle> {
		this.createHarnessCount += 1;
		const gate = this.nextCreateHarnessGate;
		if (gate) {
			this.nextCreateHarnessGate = undefined;
			gate.entered.resolve(undefined);
			await gate.release.promise;
		}
		const session = await this.repo.open(metadata, BACKGROUND_CONTEXT);
		try {
			if (this.nextCreateHarnessError) {
				const error = this.nextCreateHarnessError;
				this.nextCreateHarnessError = undefined;
				throw error;
			}
			const harness = new TestHarness(session);
			if (this.nextHarnessCloseError) {
				harness.failClose = this.nextHarnessCloseError;
				this.nextHarnessCloseError = undefined;
			}
			const harnesses = this.harnesses.get(metadata.id) ?? [];
			harnesses.push(harness);
			this.harnesses.set(metadata.id, harnesses);
			return harness;
		} catch (error) {
			await session.close(BACKGROUND_CONTEXT);
			throw error;
		}
	}

	async seed(id = "session-1", parentSessionId?: string): Promise<SessionMetadata> {
		const session = await this.repo.create({ id, parentSessionId }, BACKGROUND_CONTEXT);
		const metadata = session.metadata;
		await session.close(BACKGROUND_CONTEXT);
		return metadata;
	}

	delayNextList(): ListDelay {
		const delay = { entered: new Deferred<void>(), release: new Deferred<void>() };
		this.nextListDelay = delay;
		return delay;
	}

	gateNextCreateHarness(): OpenGate {
		const gate = { entered: new Deferred<void>(), release: new Deferred<void>() };
		this.nextCreateHarnessGate = gate;
		return gate;
	}

	latestHarness(id: string): TestHarness {
		const harnesses = this.harnesses.get(id);
		if (!harnesses?.length) throw new Error(`No harness for ${id}`);
		return harnesses.at(-1)!;
	}
}
