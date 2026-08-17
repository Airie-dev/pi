import type { Context } from "../../context.ts";
import type {
	CommitResult,
	Entry,
	EntryScan,
	EntryStructure,
	Register,
	RegisterNamespace,
	SessionStats,
	Storage,
	StorageBranchScan,
	Transaction,
	UsageRow,
	UsageScan,
} from "../types.ts";

/** Test-only forwarding base for decorators that alter one part of Storage behavior. */
export class StorageDecorator implements Storage {
	protected readonly delegate: Storage;

	constructor(delegate: Storage) {
		this.delegate = delegate;
	}

	commit(transaction: Transaction, context: Context): Promise<CommitResult> {
		return this.delegate.commit(transaction, context);
	}

	getEntries(ids: string[], context: Context): Promise<Map<string, Entry>> {
		return this.delegate.getEntries(ids, context);
	}

	getRegister<TNamespace extends RegisterNamespace>(
		namespace: TNamespace,
		key: string,
		context: Context,
	): Promise<Register<TNamespace> | undefined> {
		return this.delegate.getRegister(namespace, key, context);
	}

	listRegisters<TNamespace extends RegisterNamespace>(
		namespace: TNamespace,
		keyPrefix: string | undefined,
		context: Context,
	): Promise<Register<TNamespace>[]> {
		return this.delegate.listRegisters(namespace, keyPrefix, context);
	}

	scanBranch(query: StorageBranchScan, context: Context): Promise<Entry[]> {
		return this.delegate.scanBranch(query, context);
	}

	scanBranchStructure(query: StorageBranchScan, context: Context): Promise<EntryStructure[]> {
		return this.delegate.scanBranchStructure(query, context);
	}

	scanEntries(query: EntryScan, context: Context): Promise<Entry[]> {
		return this.delegate.scanEntries(query, context);
	}

	scanUsage(query: UsageScan, context: Context): Promise<UsageRow[]> {
		return this.delegate.scanUsage(query, context);
	}

	getStats(context: Context): Promise<SessionStats> {
		return this.delegate.getStats(context);
	}

	close(context: Context): Promise<void> {
		return this.delegate.close(context);
	}
}
