import type { AssistantMessage } from "@earendil-works/pi-ai";
import { describe, expect, it } from "vitest";
import { BACKGROUND_CONTEXT } from "../../src/harness/context.ts";
import type { CustomMessage } from "../../src/harness/messages.ts";
import { MemoryStorage } from "../../src/harness/session/memory.ts";
import { StorageBackedSession } from "../../src/harness/session/session.ts";
import { InstrumentedStorage } from "../../src/harness/session/testing/index.ts";
import type {
	MessageEntry,
	NewEntry,
	Session,
	SessionMetadata,
	SessionMutator,
	Transaction,
} from "../../src/harness/session/types.ts";

const NOW = 1_700_000_000_000;
const ENTRY_ID = "00000000-0000-7000-8000-000000000001";
const metadata = {
	id: "session",
	createdAt: NOW,
	storageVersion: 1,
	cwd: "/workspace",
} satisfies SessionMetadata;

function commitSession(session: Session, transaction: Transaction) {
	return session.mutate("main", (mutator) => mutator.commit(transaction, BACKGROUND_CONTEXT), BACKGROUND_CONTEXT);
}

describe("StorageBackedSession", () => {
	it("delegates typed values directly without validation or cloning", async () => {
		const storage = new InstrumentedStorage(new MemoryStorage({ now: () => NOW }));
		const session = new StorageBackedSession(metadata, storage);
		const data = { nested: ["original"] };
		const transaction: Transaction = {
			writes: [
				{ kind: "entry", entry: { id: ENTRY_ID, parentId: null, type: "custom", customType: "note", data } },
				{ kind: "register", op: "set", namespace: "fact.custom", key: "state", value: data },
			],
		};

		const result = await commitSession(session, transaction);

		expect(storage.getCommitAttempts()[0]).toBe(transaction);
		const entry = (await session.getEntries([ENTRY_ID], BACKGROUND_CONTEXT)).get(ENTRY_ID);
		expect(entry).toMatchObject({ seq: result.seqs[0], timestamp: NOW });
		if (entry?.type !== "custom") throw new Error("Expected custom entry");
		expect(entry.data).toBe(data);
		expect((await session.getRegister("fact.custom", "state", BACKGROUND_CONTEXT))?.value).toBe(data);
		await session.close(BACKGROUND_CONTEXT);
	});

	it("rejects pending assistant entries at the durable session write boundary", async () => {
		const storage = new InstrumentedStorage(new MemoryStorage({ now: () => NOW }));
		const session = new StorageBackedSession(metadata, storage);
		const pending: AssistantMessage = {
			role: "assistant",
			content: [],
			api: "anthropic-messages",
			provider: "anthropic",
			model: "claude-sonnet-4-5",
			usage: {
				input: 0,
				output: 0,
				cacheRead: 0,
				cacheWrite: 0,
				totalTokens: 0,
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
			},
			stopReason: "pending",
			timestamp: NOW,
		};

		await expect(
			commitSession(session, {
				writes: [{ kind: "entry", entry: { id: ENTRY_ID, parentId: null, type: "message", message: pending } }],
			}),
		).rejects.toThrow("Cannot persist a pending assistant message");
		expect(storage.getCommitAttempts()).toEqual([]);
		expect(await session.getEntries([ENTRY_ID], BACKGROUND_CONTEXT)).toEqual(new Map());
		await session.close(BACKGROUND_CONTEXT);
	});

	it("trusts typed custom messages without repository schema registration", async () => {
		const storage = new MemoryStorage({ now: () => NOW });
		const session = new StorageBackedSession(metadata, storage);
		const message: CustomMessage = {
			role: "custom",
			customType: "notice",
			content: "maintenance",
			display: true,
			timestamp: NOW,
		};
		const entry: NewEntry<MessageEntry> = { id: ENTRY_ID, parentId: null, type: "message", message };

		const result = await commitSession(session, { writes: [{ kind: "entry", entry }] });

		expect((await session.getEntries([ENTRY_ID], BACKGROUND_CONTEXT)).get(ENTRY_ID)).toEqual({
			...entry,
			seq: result.firstSeq,
			timestamp: result.timestamp,
		});
		await session.close(BACKGROUND_CONTEXT);
	});

	it("serializes mutations, permits one commit attempt, and invalidates the mutator", async () => {
		const storage = new InstrumentedStorage(new MemoryStorage({ now: () => NOW }));
		const session = new StorageBackedSession(metadata, storage);
		let captured: SessionMutator | undefined;

		await session.mutate(
			"review",
			async (mutator) => {
				captured = mutator;
				expect(mutator.lane).toBe("review");
				expect(await mutator.getRegister("fact.name", "", BACKGROUND_CONTEXT)).toBeUndefined();
				await mutator.commit(
					{
						writes: [{ kind: "register", op: "set", namespace: "fact.name", key: "", value: "committed" }],
					},
					BACKGROUND_CONTEXT,
				);
				await expect(mutator.commit({ writes: [] }, BACKGROUND_CONTEXT)).rejects.toThrow(
					"commit already attempted",
				);
			},
			BACKGROUND_CONTEXT,
		);

		expect(storage.getCommitAttempts()).toHaveLength(1);
		expect(await session.getName(BACKGROUND_CONTEXT)).toBe("committed");
		const invalidated = captured;
		if (invalidated === undefined) throw new Error("Expected captured mutator");
		expect(() => invalidated.getEntries([], BACKGROUND_CONTEXT)).toThrow("outside its mutation callback");
		await session.close(BACKGROUND_CONTEXT);
	});

	it("consumes the commit guard when the first commit fails", async () => {
		const storage = new InstrumentedStorage(new MemoryStorage({ now: () => NOW }));
		const session = new StorageBackedSession(metadata, storage);
		const transaction = {
			writes: [{ kind: "entry", entry: { id: ENTRY_ID, parentId: "missing", type: "custom", customType: "note" } }],
		} satisfies Transaction;

		await expect(
			session.mutate(
				"main",
				async (mutator) => {
					await expect(mutator.commit(transaction, BACKGROUND_CONTEXT)).rejects.toThrow("Missing parent entry");
					await expect(mutator.commit({ writes: [] }, BACKGROUND_CONTEXT)).rejects.toThrow(
						"commit already attempted",
					);
				},
				BACKGROUND_CONTEXT,
			),
		).resolves.toBeUndefined();
		expect(storage.getCommitAttempts()).toHaveLength(1);
		await session.close(BACKGROUND_CONTEXT);
	});

	it("mints distinct follower ids with the leader timestamp", async () => {
		const session = new StorageBackedSession(metadata, new MemoryStorage({ now: () => NOW }));
		const leaderTimestamp = 0x0123456789ab;
		const leader = session.idGenerator.next(leaderTimestamp);
		const followers = [session.idGenerator.next(leaderTimestamp), session.idGenerator.next(leaderTimestamp)];
		const decodeTimestamp = (id: string): number => Number.parseInt(id.replaceAll("-", "").slice(0, 12), 16);

		expect([leader, ...followers].map(decodeTimestamp)).toEqual([leaderTimestamp, leaderTimestamp, leaderTimestamp]);
		expect(new Set([leader, ...followers])).toHaveLength(3);
		await session.close(BACKGROUND_CONTEXT);
	});

	it("exposes metadata directly and the shared UUIDv7 id generator", async () => {
		const sourceMetadata = { ...metadata };
		const session = new StorageBackedSession(sourceMetadata, new MemoryStorage({ now: () => NOW }));

		expect(session.metadata).toBe(sourceMetadata);
		expect(session.idGenerator.next()).toMatch(
			/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
		);
		await session.close(BACKGROUND_CONTEXT);
	});

	it("closes idempotently and rejects operations not admitted before close", async () => {
		const storage = new MemoryStorage({ now: () => NOW });
		const session = new StorageBackedSession(metadata, storage);

		await Promise.all([session.close(BACKGROUND_CONTEXT), session.close(BACKGROUND_CONTEXT)]);
		await expect(session.mutate("main", () => undefined, BACKGROUND_CONTEXT)).rejects.toThrow("Session is closed");
		await expect(session.getEntries([], BACKGROUND_CONTEXT)).rejects.toThrow("Session is closed");
		await expect(session.getRegister("fact.name", "", BACKGROUND_CONTEXT)).rejects.toThrow("Session is closed");
		await expect(session.listRegisters("fact.name", undefined, BACKGROUND_CONTEXT)).rejects.toThrow(
			"Session is closed",
		);
	});
});
