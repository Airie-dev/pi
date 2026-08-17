import { describe, expect, it } from "vitest";
import { BACKGROUND_CONTEXT } from "../../src/harness/context.ts";
import { MemoryStorage } from "../../src/harness/session/memory.ts";
import { InstrumentedStorage } from "../../src/harness/session/testing/index.ts";
import type { CommitResult, Transaction } from "../../src/harness/session/types.ts";

class ControlledCommitStorage extends MemoryStorage {
	private readonly pending: Array<{
		resolve: (result: CommitResult) => void;
		reject: (error: unknown) => void;
	}> = [];
	private latestCommit: Promise<CommitResult> | undefined;

	override commit(_transaction: Transaction): Promise<CommitResult> {
		this.latestCommit = new Promise((resolve, reject) => {
			this.pending.push({ resolve, reject });
		});
		return this.latestCommit;
	}

	get admissionCount(): number {
		return this.pending.length;
	}

	get lastCommit(): Promise<CommitResult> | undefined {
		return this.latestCommit;
	}

	resolveNextCommit(result: CommitResult): void {
		const pending = this.pending.shift();
		if (pending === undefined) throw new Error("No pending commit");
		pending.resolve(result);
	}

	rejectNextCommit(error: unknown): void {
		const pending = this.pending.shift();
		if (pending === undefined) throw new Error("No pending commit");
		pending.reject(error);
	}
}

describe("InstrumentedStorage", () => {
	it("records commit attempts synchronously in admission order before settlement", async () => {
		const delegate = new ControlledCommitStorage();
		const storage = new InstrumentedStorage(delegate);
		const firstTransaction: Transaction = {
			writes: [{ kind: "register", op: "set", namespace: "fact.name", key: "", value: "first" }],
		};
		const secondTransaction: Transaction = {
			writes: [{ kind: "register", op: "set", namespace: "fact.name", key: "", value: "second" }],
		};

		const firstCommit = storage.commit(firstTransaction, BACKGROUND_CONTEXT);
		expect(firstCommit).toBe(delegate.lastCommit);
		expect(storage.getCommitAttempts()).toEqual([firstTransaction]);
		const secondCommit = storage.commit(secondTransaction, BACKGROUND_CONTEXT);
		expect(secondCommit).toBe(delegate.lastCommit);
		expect(storage.getCommitAttempts()).toEqual([firstTransaction, secondTransaction]);

		const firstResult = { firstSeq: 1, seqs: [1], timestamp: 10 };
		delegate.resolveNextCommit(firstResult);
		expect(await firstCommit).toBe(firstResult);
		expect(storage.getCommitAttempts()).toEqual([firstTransaction, secondTransaction]);
		const secondResult = { firstSeq: 2, seqs: [2], timestamp: 20 };
		delegate.resolveNextCommit(secondResult);
		expect(await secondCommit).toBe(secondResult);
		await storage.close(BACKGROUND_CONTEXT);
	});
	it("records the transaction reference passed to the delegate", async () => {
		const delegate = new ControlledCommitStorage();
		const storage = new InstrumentedStorage(delegate);
		const transaction: Transaction = {
			writes: [{ kind: "register", op: "set", namespace: "fact.name", key: "", value: "value" }],
		};

		const commit = storage.commit(transaction, BACKGROUND_CONTEXT);
		expect(storage.getCommitAttempts()[0]).toBe(transaction);
		delegate.resolveNextCommit({ firstSeq: 1, seqs: [1], timestamp: 10 });
		await commit;
		await storage.close(BACKGROUND_CONTEXT);
	});

	it("clears recorded attempts between phases without affecting the delegate", async () => {
		const delegate = new MemoryStorage({ now: () => 100 });
		const storage = new InstrumentedStorage(delegate);
		await storage.commit(
			{
				writes: [{ kind: "register", op: "set", namespace: "fact.name", key: "", value: "first" }],
			},
			BACKGROUND_CONTEXT,
		);

		storage.clearCommitAttempts();
		expect(storage.getCommitAttempts()).toEqual([]);
		expect(await storage.getRegister("fact.name", "", BACKGROUND_CONTEXT)).toMatchObject({ value: "first" });

		const secondTransaction: Transaction = {
			writes: [{ kind: "register", op: "set", namespace: "fact.name", key: "", value: "second" }],
		};
		await storage.commit(secondTransaction, BACKGROUND_CONTEXT);
		expect(storage.getCommitAttempts()).toEqual([secondTransaction]);
		await storage.close(BACKGROUND_CONTEXT);
	});

	it("delegates every read and query without recording synthetic writes", async () => {
		const delegate = new MemoryStorage({ now: () => 100 });
		const storage = new InstrumentedStorage(delegate);
		await storage.commit(
			{
				writes: [
					{ kind: "entry", entry: { id: "root", parentId: null, type: "custom", customType: "note" } },
					{ kind: "register", op: "set", namespace: "fact.name", key: "", value: "session" },
					{
						kind: "usage",
						row: {
							id: "usage",
							adjustment: false,
							usage: {
								input: 1,
								output: 2,
								cacheRead: 0,
								cacheWrite: 0,
								totalTokens: 3,
								cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
							},
						},
					},
				],
			},
			BACKGROUND_CONTEXT,
		);

		expect(await storage.getEntries(["root"], BACKGROUND_CONTEXT)).toEqual(
			await delegate.getEntries(["root"], BACKGROUND_CONTEXT),
		);
		expect(await storage.getRegister("fact.name", "", BACKGROUND_CONTEXT)).toEqual(
			await delegate.getRegister("fact.name", "", BACKGROUND_CONTEXT),
		);
		expect(await storage.listRegisters("fact.name", undefined, BACKGROUND_CONTEXT)).toEqual(
			await delegate.listRegisters("fact.name", undefined, BACKGROUND_CONTEXT),
		);
		expect(await storage.scanBranch({ start: "root" }, BACKGROUND_CONTEXT)).toEqual(
			await delegate.scanBranch({ start: "root" }, BACKGROUND_CONTEXT),
		);
		expect(await storage.scanBranchStructure({ start: "root" }, BACKGROUND_CONTEXT)).toEqual(
			await delegate.scanBranchStructure({ start: "root" }, BACKGROUND_CONTEXT),
		);
		expect(await storage.scanEntries({ order: "asc" }, BACKGROUND_CONTEXT)).toEqual(
			await delegate.scanEntries({ order: "asc" }, BACKGROUND_CONTEXT),
		);
		expect(await storage.scanUsage({ order: "asc" }, BACKGROUND_CONTEXT)).toEqual(
			await delegate.scanUsage({ order: "asc" }, BACKGROUND_CONTEXT),
		);
		expect(await storage.getStats(BACKGROUND_CONTEXT)).toEqual(await delegate.getStats(BACKGROUND_CONTEXT));
		expect(storage.getCommitAttempts()).toHaveLength(1);
		await storage.close(BACKGROUND_CONTEXT);
	});

	it("delegates close idempotence and admitted commit draining", async () => {
		const delegate = new MemoryStorage({ now: () => 100 });
		const storage = new InstrumentedStorage(delegate);
		const admitted = storage.commit(
			{
				writes: [{ kind: "register", op: "set", namespace: "fact.name", key: "", value: "admitted" }],
			},
			BACKGROUND_CONTEXT,
		);

		const firstClose = storage.close(BACKGROUND_CONTEXT);
		const secondClose = storage.close(BACKGROUND_CONTEXT);
		await admitted;
		await Promise.all([firstClose, secondClose]);
		await expect(storage.getStats(BACKGROUND_CONTEXT)).rejects.toThrow("MemoryStorage is closed");
		expect(storage.getCommitAttempts()).toHaveLength(1);
	});
});
