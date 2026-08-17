import type {
	AgentMessage,
	BranchScan,
	Context,
	Entry,
	EntryQuery,
	JsonValue,
	LaneConfiguration,
	Register,
	RegisterNamespace,
	Session,
	SessionMutator,
	SessionStats,
	SessionTree,
} from "@earendil-works/pi-agent-core";
import type { SqliteSessionMetadata } from "./session/session-row.ts";

export interface SqliteOpenSessionOptions {
	onClose: () => void;
	renewWriterLease: () => void;
	releaseWriterLease: () => void;
	renewIntervalMs: number;
}

/** SQLite-specific open-session lifecycle wrapper. */
export class SqliteOpenSession implements Session<SqliteSessionMetadata> {
	readonly metadata: SqliteSessionMetadata;
	readonly idGenerator: Session<SqliteSessionMetadata>["idGenerator"];
	private readonly session: Session<SqliteSessionMetadata>;
	private readonly onClose: () => void;
	private readonly renewWriterLease: () => void;
	private readonly releaseWriterLease: () => void;
	private readonly renewalTimer: ReturnType<typeof setInterval>;
	private readonly admitted = new Set<Promise<unknown>>();
	private leaseError: unknown;
	private readonly closedError = new Error("Session is closed");
	private state: "open" | "closing" | "closed" = "open";
	private closePromise: Promise<void> | undefined;

	constructor(session: Session<SqliteSessionMetadata>, options: SqliteOpenSessionOptions) {
		this.session = session;
		this.metadata = session.metadata;
		this.idGenerator = session.idGenerator;
		this.onClose = options.onClose;
		this.renewWriterLease = options.renewWriterLease;
		this.releaseWriterLease = options.releaseWriterLease;
		this.renewalTimer = setInterval(() => {
			try {
				this.renewWriterLease();
			} catch (error) {
				this.leaseError = error;
				clearInterval(this.renewalTimer);
			}
		}, options.renewIntervalMs);
		this.renewalTimer.unref?.();
	}

	mutate<T>(
		lane: string,
		mutation: (mutator: SessionMutator, context: Context) => T | Promise<T>,
		context: Context,
	): Promise<T> {
		return this.admit(() => this.session.mutate(lane, mutation, context));
	}

	getEntries(ids: string[], context: Context): Promise<Map<string, Entry>> {
		return this.admit(() => this.session.getEntries(ids, context));
	}

	getRegister<TNamespace extends RegisterNamespace>(
		namespace: TNamespace,
		key: string,
		context: Context,
	): Promise<Register<TNamespace> | undefined> {
		return this.admit(() => this.session.getRegister(namespace, key, context));
	}

	listRegisters<TNamespace extends RegisterNamespace>(
		namespace: TNamespace,
		keyPrefix: string | undefined,
		context: Context,
	): Promise<Register<TNamespace>[]> {
		return this.admit(() => this.session.listRegisters(namespace, keyPrefix, context));
	}

	view(lane: string): SessionTree {
		const view = this.session.view(lane);
		return {
			getLeafId: (context) => this.admit(() => view.getLeafId(context)),
			getEntry: (id, context) => this.admit(() => view.getEntry(id, context)),
			getStats: (context) => this.admit(() => view.getStats(context)),
			getName: (context) => this.admit(() => view.getName(context)),
			setName: (name, context) => this.admit(() => view.setName(name, context)),
			getLabel: (targetId, context) => this.admit(() => view.getLabel(targetId, context)),
			setLabel: (targetId, label, context) => this.admit(() => view.setLabel(targetId, label, context)),
			getCustomFact: (key, context) => this.admit(() => view.getCustomFact(key, context)),
			setCustomFact: (key, value, context) => this.admit(() => view.setCustomFact(key, value, context)),
			findEntries: (query, context) => this.admit(() => view.findEntries(query, context)),
			findEntry: (query, context) => this.admit(() => view.findEntry(query, context)),
			findEntriesOnBranch: (query, context) => this.admit(() => view.findEntriesOnBranch(query, context)),
			findEntryOnBranch: (query, context) => this.admit(() => view.findEntryOnBranch(query, context)),
			appendMessage: (message, context) => this.admit(() => view.appendMessage(message, context)),
			appendCustomEntry: (customType, data, context) =>
				this.admit(() => view.appendCustomEntry(customType, data, context)),
		};
	}

	createLane(
		name: string,
		at: string | null,
		configuration: LaneConfiguration,
		context: Context,
	): Promise<SessionTree> {
		return this.admit(async () => {
			await this.session.createLane(name, at, configuration, context);
			return this.view(name);
		});
	}

	getLeafId(context: Context): Promise<string | null> {
		return this.admit(() => this.session.getLeafId(context));
	}

	getEntry(id: string, context: Context): Promise<Entry | undefined> {
		return this.admit(() => this.session.getEntry(id, context));
	}

	getStats(context: Context): Promise<SessionStats> {
		return this.admit(() => this.session.getStats(context));
	}

	getName(context: Context): Promise<string | undefined> {
		return this.admit(() => this.session.getName(context));
	}

	setName(name: string | undefined, context: Context): Promise<void> {
		return this.admit(() => this.session.setName(name, context));
	}

	getLabel(targetId: string, context: Context): Promise<string | undefined> {
		return this.admit(() => this.session.getLabel(targetId, context));
	}

	setLabel(targetId: string, label: string | undefined, context: Context): Promise<void> {
		return this.admit(() => this.session.setLabel(targetId, label, context));
	}

	getCustomFact(key: string, context: Context): Promise<JsonValue | undefined> {
		return this.admit(() => this.session.getCustomFact(key, context));
	}

	setCustomFact(key: string, value: JsonValue | undefined, context: Context): Promise<void> {
		return this.admit(() => this.session.setCustomFact(key, value, context));
	}

	findEntries(query: EntryQuery | undefined, context: Context): Promise<Entry[]> {
		return this.admit(() => this.session.findEntries(query, context));
	}

	findEntry(query: EntryQuery | undefined, context: Context): Promise<Entry | undefined> {
		return this.admit(() => this.session.findEntry(query, context));
	}

	findEntriesOnBranch(query: BranchScan | undefined, context: Context): Promise<Entry[]> {
		return this.admit(() => this.session.findEntriesOnBranch(query, context));
	}

	findEntryOnBranch(query: BranchScan | undefined, context: Context): Promise<Entry | undefined> {
		return this.admit(() => this.session.findEntryOnBranch(query, context));
	}

	appendMessage(message: AgentMessage, context: Context): Promise<string> {
		return this.admit(() => this.session.appendMessage(message, context));
	}

	appendCustomEntry(customType: string, data: JsonValue | undefined, context: Context): Promise<string> {
		return this.admit(() => this.session.appendCustomEntry(customType, data, context));
	}

	close(context: Context): Promise<void> {
		if (this.closePromise !== undefined) return this.closePromise;
		this.state = "closing";
		this.closePromise = Promise.allSettled([...this.admitted])
			.then(() => this.session.close(context))
			.finally(() => {
				clearInterval(this.renewalTimer);
				try {
					this.releaseWriterLease();
				} finally {
					this.state = "closed";
					this.onClose();
				}
			});
		return this.closePromise;
	}

	private admit<T>(operation: () => Promise<T>): Promise<T> {
		if (this.state !== "open") return Promise.reject(this.closedError);
		if (this.leaseError !== undefined) return Promise.reject(this.leaseError);
		let result: Promise<T>;
		try {
			result = operation();
		} catch (error) {
			result = Promise.reject(error);
		}
		this.admitted.add(result);
		void result.then(
			() => this.admitted.delete(result),
			() => this.admitted.delete(result),
		);
		return result;
	}
}
