import { type Api, createModels, fauxProvider, type Model } from "@earendil-works/pi-ai";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BACKGROUND_CONTEXT } from "../../src/harness/context.ts";
import { restoreLane } from "../../src/harness/restore.ts";
import { MemoryStorage } from "../../src/harness/session/memory.ts";
import { StorageBackedSession } from "../../src/harness/session/session.ts";
import {
	AgentHarness,
	type AgentHarness as AgentHarnessInstance,
	DEFAULT_COMPACTION_SETTINGS,
	HarnessClosed,
	HarnessFault,
	type LaneConfiguration,
	MemorySessionRepo,
	type Session,
	type SessionReader,
	type Transaction,
} from "../../src/index.ts";

interface Fixture {
	harness: AgentHarnessInstance;
	session: Session;
	repo: MemorySessionRepo;
	model: Model<Api>;
}

const fixtures: Fixture[] = [];

class FailingMemoryStorage extends MemoryStorage {
	failCommits = false;

	override commit(transaction: Transaction) {
		return this.failCommits
			? Promise.reject(new Error("commit failed"))
			: super.commit(transaction, BACKGROUND_CONTEXT);
	}
}

async function createFixture(options: { drive?: "automatic" | "manual" } = {}): Promise<Fixture> {
	const repo = new MemorySessionRepo();
	const session = await repo.create({}, BACKGROUND_CONTEXT);
	const faux = fauxProvider();
	const models = createModels();
	models.setProvider(faux.provider);
	const { harness } = await AgentHarness.create(
		{
			session,
			models,
			model: faux.getModel(),
			drive: options.drive,
		},
		BACKGROUND_CONTEXT,
	);
	const fixture = { harness, session, repo, model: faux.getModel() };
	fixtures.push(fixture);
	return fixture;
}

async function installCheckpointRun(
	session: Session,
	operationId = session.idGenerator.next(),
): Promise<{ operationId: string; entryId: string }> {
	const entryId = session.idGenerator.next();
	await session.mutate(
		"main",
		async (mutator) => {
			const laneState = await mutator.getRegister("lane.state", "main", BACKGROUND_CONTEXT);
			if (laneState === undefined) throw new Error("missing lane state");
			await mutator.commit(
				{
					writes: [
						{
							kind: "entry",
							entry: {
								id: entryId,
								parentId: null,
								type: "message",
								message: { role: "user", content: "hello", timestamp: 1 },
							},
						},
						{ kind: "register", op: "set", namespace: "lane.leaf", key: "main", value: entryId },
						{
							kind: "register",
							op: "set",
							namespace: "op.meta",
							key: operationId,
							value: {
								operationId,
								lane: "main",
								sourceLeafId: null,
								startedAt: 1,
								intent: { kind: "run", promptEntryIds: [entryId] },
							},
						},
						{
							kind: "register",
							op: "set",
							namespace: "op.state",
							key: operationId,
							value: {
								kind: "run",
								control: { status: "running" },
								settings: {
									compaction: DEFAULT_COMPACTION_SETTINGS,
									steeringMode: "all",
									followUpMode: "all",
									toolExecution: "parallel",
								},
								phase: {
									kind: "checkpoint",
									continuation: { kind: "need_assistant", overflowRecoveryUsed: false },
									triggerEntryId: entryId,
								},
								inbox: { steer: [], followUp: [], writes: [] },
								latestAssistantEntryId: null,
							},
						},
						{
							kind: "register",
							op: "set",
							namespace: "lane.state",
							key: "main",
							value: { ...laneState.value, currentOperationId: operationId },
						},
					],
				},
				BACKGROUND_CONTEXT,
			);
		},
		BACKGROUND_CONTEXT,
	);
	return { operationId, entryId };
}

async function waitForAction(harness: AgentHarnessInstance): Promise<void> {
	for (let attempt = 0; attempt < 100; attempt++) {
		if ((await harness.peekAction(BACKGROUND_CONTEXT)) !== undefined) return;
		await new Promise((resolve) => setTimeout(resolve, 0));
	}
	throw new Error("action did not park");
}

afterEach(async () => {
	for (const fixture of fixtures.splice(0)) {
		await fixture.harness.close(BACKGROUND_CONTEXT);
		await fixture.repo.close(BACKGROUND_CONTEXT);
	}
});

describe("AgentHarness runtime shell", () => {
	it("seeds main, reads configuration, and creates independently seeded lanes", async () => {
		const { harness, model } = await createFixture();
		expect(await harness.getModel(BACKGROUND_CONTEXT)).toEqual(model);
		expect(await harness.inspectExecution(BACKGROUND_CONTEXT)).toMatchObject({
			lane: "main",
			leafId: null,
			current: null,
		});

		await harness.setThinkingLevel("high", BACKGROUND_CONTEXT);
		let observedResources = false;
		harness.events.on("config_update", async (event) => {
			if (event.property !== "resources") return;
			observedResources = (await harness.getResources(BACKGROUND_CONTEXT)).skills?.[0]?.name === "test";
		});
		await harness.setResources(
			{
				skills: [{ name: "test", description: "test", content: "test", filePath: "/test" }],
			},
			BACKGROUND_CONTEXT,
		);
		expect(observedResources).toBe(true);
		harness.events.on("config_update", (event) => {
			if (event.property === "activeTools") event.value.push("listener-mutation");
		});
		await harness.setActiveTools(["read"], BACKGROUND_CONTEXT);
		expect(await harness.getActiveTools(BACKGROUND_CONTEXT)).toEqual(["read"]);
		expect((harness as AgentHarnessInstance & { settingsRevision: number }).settingsRevision).toBe(1);
		const created = await harness.createLane("worker", null, BACKGROUND_CONTEXT);
		expect(created.ok).toBe(true);
		if (!created.ok) return;
		expect(await created.value.getThinkingLevel(BACKGROUND_CONTEXT)).toBe("off");
		expect((await harness.lanes(BACKGROUND_CONTEXT)).map((lane) => lane.name).sort()).toEqual(["main", "worker"]);
	});

	it("captures a lane snapshot and streams later lane events", async () => {
		const { harness, session } = await createFixture({ drive: "manual" });
		const current = await installCheckpointRun(session);
		const watch = await harness.watch(BACKGROUND_CONTEXT);
		expect(watch.snapshot).toMatchObject({
			lane: "main",
			leafId: current.entryId,
			transcript: [{ id: current.entryId, message: { role: "user", content: "hello" } }],
			operation: { id: current.operationId, kind: "run", status: "suspended", runningTools: [] },
			queues: { steer: [], followUp: [], nextRun: [] },
			pendingWrites: [],
			faulted: false,
		});
		const events: string[] = [];
		watch.start((event) => {
			events.push(event.type);
		});
		watch.unsubscribe();
		expect(events).toEqual([]);
	});

	it("buffers watch events emitted after an idle snapshot", async () => {
		const { harness } = await createFixture({ drive: "manual" });
		const watch = await harness.watch(BACKGROUND_CONTEXT);
		expect(watch.snapshot).toMatchObject({ transcript: [], operation: null });
		const accepted = await harness.accept({ kind: "prompt", prompt: "hello" }, BACKGROUND_CONTEXT);
		expect(accepted.ok).toBe(true);
		const events: string[] = [];
		watch.start((event) => {
			events.push(event.type);
		});
		await vi.waitFor(() => expect(events).toEqual(["run_start", "message_start", "message_end", "entry_added"]));
		watch.unsubscribe();
	});

	it("fences stale drives and yields before publishing a breakpoint", async () => {
		const { harness, session } = await createFixture({ drive: "manual" });
		const current = await installCheckpointRun(session);

		const staleOperationId = session.idGenerator.next();
		const stale = await harness.drive({ operationId: staleOperationId, deadline: 0 }, BACKGROUND_CONTEXT);
		expect(stale.ok).toBe(false);
		if (!stale.ok) {
			expect(stale.error).toMatchObject({
				_tag: "OperationMismatch",
				currentOperationId: current.operationId,
			});
		}
		expect(await harness.peekAction(BACKGROUND_CONTEXT)).toBeUndefined();

		const yielded = await harness.drive({ operationId: current.operationId, deadline: 0 }, BACKGROUND_CONTEXT);
		expect(yielded).toEqual({
			ok: true,
			value: { kind: "yielded", operationId: current.operationId },
		});
		expect(await harness.peekAction(BACKGROUND_CONTEXT)).toBeUndefined();
		expect((await harness.inspectExecution(BACKGROUND_CONTEXT)).current?.status).toBe("suspended");
	});
	it("installs one same-operation drive pass and joins it", async () => {
		const { harness, session } = await createFixture({ drive: "manual" });
		const current = await installCheckpointRun(session);

		const first = harness.drive({ operationId: current.operationId }, BACKGROUND_CONTEXT);
		await waitForAction(harness);
		expect(await harness.peekAction(BACKGROUND_CONTEXT)).toMatchObject({
			kind: "runtime.dispatch",
			details: { operationId: current.operationId, operationKind: "run" },
		});
		expect((await harness.inspectExecution(BACKGROUND_CONTEXT)).current?.status).toBe("running");
		const second = harness.drive({ operationId: current.operationId, deadline: 0 }, BACKGROUND_CONTEXT);
		await new Promise((resolve) => setTimeout(resolve, 0));
		await harness.close(BACKGROUND_CONTEXT);

		const [left, right] = await Promise.allSettled([first, second]);
		expect(left.status).toBe("rejected");
		expect(right.status).toBe("rejected");
		if (left.status === "rejected" && right.status === "rejected") {
			expect(left.reason).toBe(right.reason);
			expect(left.reason).toBeInstanceOf(HarnessClosed);
		}
	});

	it("faults inspection when process and durable operation ownership disagree", async () => {
		const { harness, session } = await createFixture({ drive: "manual" });
		const current = await installCheckpointRun(session);
		const drive = harness.drive({ operationId: current.operationId }, BACKGROUND_CONTEXT);
		await waitForAction(harness);
		const [operation, state, laneState] = await Promise.all([
			session.getRegister("op.meta", current.operationId, BACKGROUND_CONTEXT),
			session.getRegister("op.state", current.operationId, BACKGROUND_CONTEXT),
			session.getRegister("lane.state", "main", BACKGROUND_CONTEXT),
		]);
		if (operation === undefined || state === undefined || laneState === undefined) {
			throw new Error("missing operation state");
		}
		const replacementId = session.idGenerator.next();
		await session.mutate(
			"main",
			(mutator) =>
				mutator.commit(
					{
						writes: [
							{
								kind: "register",
								op: "set",
								namespace: "op.meta",
								key: replacementId,
								value: { ...operation.value, operationId: replacementId },
							},
							{ kind: "register", op: "set", namespace: "op.state", key: replacementId, value: state.value },
							{
								kind: "register",
								op: "set",
								namespace: "lane.state",
								key: "main",
								value: { ...laneState.value, currentOperationId: replacementId },
							},
						],
					},
					BACKGROUND_CONTEXT,
				),
			BACKGROUND_CONTEXT,
		);

		await expect(harness.inspectExecution(BACKGROUND_CONTEXT)).rejects.toBeInstanceOf(HarnessFault);
		await expect(drive).rejects.toBeInstanceOf(HarnessFault);
	});

	it("yields after a parked boundary when the deadline expires", async () => {
		const { harness, session } = await createFixture({ drive: "manual" });
		const current = await installCheckpointRun(session);
		const now = vi.spyOn(Date, "now").mockReturnValue(100);
		try {
			const drive = harness.drive({ operationId: current.operationId, deadline: 150 }, BACKGROUND_CONTEXT);
			await waitForAction(harness);
			now.mockReturnValue(200);
			await harness.executeAction(BACKGROUND_CONTEXT);
			await expect(drive).resolves.toEqual({
				ok: true,
				value: { kind: "yielded", operationId: current.operationId },
			});
		} finally {
			now.mockRestore();
		}
	});

	it("validates base operation ownership and intent kind", async () => {
		const { session } = await createFixture();
		const current = await installCheckpointRun(session);
		const stored = await session.getRegister("op.meta", current.operationId, BACKGROUND_CONTEXT);
		if (stored === undefined) throw new Error("missing operation");
		const replaceOperation = (value: typeof stored.value) =>
			session.mutate(
				"main",
				(mutator) =>
					mutator.commit(
						{
							writes: [{ kind: "register", op: "set", namespace: "op.meta", key: current.operationId, value }],
						},
						BACKGROUND_CONTEXT,
					),
				BACKGROUND_CONTEXT,
			);

		await replaceOperation({ ...stored.value, lane: "worker" });
		await expect(restoreLane(session, "main", undefined, BACKGROUND_CONTEXT)).rejects.toThrow(
			/belongs to lane worker/,
		);
		await replaceOperation({
			...stored.value,
			intent: { kind: "navigation", targetId: null, summarize: false },
		});
		await expect(restoreLane(session, "main", undefined, BACKGROUND_CONTEXT)).rejects.toThrow(
			/intent navigation does not match state run/,
		);
		await replaceOperation(stored.value);
	});

	it("restores open operations without activating them", async () => {
		const repo = new MemorySessionRepo();
		const session = await repo.create({}, BACKGROUND_CONTEXT);
		const faux = fauxProvider();
		const models = createModels();
		models.setProvider(faux.provider);
		const seed: LaneConfiguration = {
			model: { provider: faux.getModel().provider, modelId: faux.getModel().id },
			thinkingLevel: "off",
			activeToolNames: [],
		};
		await session.mutate(
			"main",
			(mutator) =>
				mutator.commit(
					{
						writes: [{ kind: "register", op: "set", namespace: "lane.config", key: "main", value: seed }],
					},
					BACKGROUND_CONTEXT,
				),
			BACKGROUND_CONTEXT,
		);
		const current = await installCheckpointRun(session);

		const { harness, suspended } = await AgentHarness.create(
			{
				session,
				models,
				model: faux.getModel(),
				drive: "manual",
			},
			BACKGROUND_CONTEXT,
		);
		fixtures.push({ harness, session, repo, model: faux.getModel() });

		expect(suspended).toEqual([
			expect.objectContaining({
				lane: "main",
				operationId: current.operationId,
				kind: "run",
				reason: "crash",
			}),
		]);
		expect((await harness.inspectExecution(BACKGROUND_CONTEXT)).current).toMatchObject({
			id: current.operationId,
			status: "suspended",
		});
		expect(await harness.peekAction(BACKGROUND_CONTEXT)).toBeUndefined();

		const registerReads: string[] = [];
		const entryReads: string[][] = [];
		const reader: SessionReader = {
			getEntries(ids) {
				entryReads.push(ids);
				return session.getEntries(ids, BACKGROUND_CONTEXT);
			},
			getRegister(namespace, key) {
				registerReads.push(`${namespace}/${key}`);
				return session.getRegister(namespace, key, BACKGROUND_CONTEXT);
			},
			listRegisters() {
				throw new Error("restore must not scan registers");
			},
		};
		await restoreLane(reader, "main", undefined, BACKGROUND_CONTEXT);
		expect(registerReads).toEqual([
			"lane.config/main",
			"lane.state/main",
			"lane.leaf/main",
			`op.meta/${current.operationId}`,
			`op.state/${current.operationId}`,
		]);
		expect(entryReads).toEqual([[current.entryId]]);
	});

	it("returns and hydrates a matching latest terminal result", async () => {
		const { harness, session } = await createFixture();
		const current = await installCheckpointRun(session);
		await session.mutate(
			"main",
			async (mutator) => {
				const laneState = await mutator.getRegister("lane.state", "main", BACKGROUND_CONTEXT);
				if (laneState === undefined) throw new Error("missing lane state");
				await mutator.commit(
					{
						writes: [
							{ kind: "register", op: "delete", namespace: "op.meta", key: current.operationId },
							{ kind: "register", op: "delete", namespace: "op.state", key: current.operationId },
							{
								kind: "register",
								op: "set",
								namespace: "lane.lastResult",
								key: "main",
								value: {
									operationId: current.operationId,
									kind: "run",
									outcome: "completed",
									leafId: current.entryId,
									runCompletion: "terminated_tools",
								},
							},
							{
								kind: "register",
								op: "set",
								namespace: "lane.state",
								key: "main",
								value: { ...laneState.value, currentOperationId: null },
							},
						],
					},
					BACKGROUND_CONTEXT,
				);
			},
			BACKGROUND_CONTEXT,
		);

		const result = await harness.drive({ operationId: current.operationId }, BACKGROUND_CONTEXT);
		expect(result).toEqual({
			ok: true,
			value: {
				kind: "settled",
				operationId: current.operationId,
				outcome: {
					operation: "run",
					runId: current.operationId,
					kind: "completed",
					leafId: current.entryId,
				},
			},
		});

		const navigationId = session.idGenerator.next();
		await session.mutate(
			"main",
			(mutator) =>
				mutator.commit(
					{
						writes: [
							{
								kind: "register",
								op: "set",
								namespace: "lane.lastResult",
								key: "main",
								value: {
									operationId: navigationId,
									kind: "navigation",
									outcome: "completed",
									oldLeafId: null,
									leafId: current.entryId,
								},
							},
						],
					},
					BACKGROUND_CONTEXT,
				),
			BACKGROUND_CONTEXT,
		);
		const navigation = await harness.drive({ operationId: navigationId }, BACKGROUND_CONTEXT);
		expect(navigation).toMatchObject({
			ok: true,
			value: {
				kind: "settled",
				outcome: {
					operation: "navigation",
					kind: "completed",
					oldLeafId: null,
					newLeafId: current.entryId,
				},
			},
		});

		const brokenRunId = session.idGenerator.next();
		await session.mutate(
			"main",
			(mutator) =>
				mutator.commit(
					{
						writes: [
							{
								kind: "register",
								op: "set",
								namespace: "lane.lastResult",
								key: "main",
								value: {
									operationId: brokenRunId,
									kind: "run",
									outcome: "completed",
									leafId: current.entryId,
									finalAssistantEntryId: session.idGenerator.next(),
									runCompletion: "assistant",
								},
							},
						],
					},
					BACKGROUND_CONTEXT,
				),
			BACKGROUND_CONTEXT,
		);
		let fault: unknown;
		try {
			await harness.drive({ operationId: brokenRunId }, BACKGROUND_CONTEXT);
		} catch (error) {
			fault = error;
		}
		expect(fault).toBeInstanceOf(HarnessFault);
		expect((fault as HarnessFault).cause).toMatchObject({
			message: expect.stringMatching(/Final assistant.*missing/),
		});
	});

	it("closes a parked drive as a controlled crash", async () => {
		const { harness, session, repo } = await createFixture({ drive: "manual" });
		const current = await installCheckpointRun(session);
		const drive = harness.drive({ operationId: current.operationId }, BACKGROUND_CONTEXT);
		await waitForAction(harness);

		await harness.close(BACKGROUND_CONTEXT);
		await expect(drive).rejects.toBeInstanceOf(HarnessClosed);
		expect(await harness.prompt("late", undefined, BACKGROUND_CONTEXT)).toMatchObject({
			ok: false,
			error: { _tag: "Closed" },
		});
		expect(() => harness.events.on("run_start", () => {})).toThrow(HarnessClosed);
		const reopened = await repo.open(session.metadata, BACKGROUND_CONTEXT);
		expect((await reopened.getRegister("op.state", current.operationId, BACKGROUND_CONTEXT))?.value.kind).toBe("run");
		await reopened.close(BACKGROUND_CONTEXT);
	});

	it("normalizes initialization commit failures to HarnessFault", async () => {
		const storage = new FailingMemoryStorage();
		const session = new StorageBackedSession({ id: "init-fault-test", createdAt: 1, storageVersion: 1 }, storage);
		await session.mutate(
			"main",
			(mutator) =>
				mutator.commit(
					{
						writes: [
							{ kind: "register", op: "set", namespace: "lane.leaf", key: "main", value: null },
							{
								kind: "register",
								op: "set",
								namespace: "lane.state",
								key: "main",
								value: { currentOperationId: null, pendingNextRun: [] },
							},
						],
					},
					BACKGROUND_CONTEXT,
				),
			BACKGROUND_CONTEXT,
		);
		const faux = fauxProvider();
		const models = createModels();
		models.setProvider(faux.provider);
		storage.failCommits = true;

		await expect(
			AgentHarness.create({ session, models, model: faux.getModel() }, BACKGROUND_CONTEXT),
		).rejects.toBeInstanceOf(HarnessFault);
		await session.close(BACKGROUND_CONTEXT);
	});

	it("faults the harness after an admitted commit fails", async () => {
		const storage = new FailingMemoryStorage();
		const session = new StorageBackedSession({ id: "fault-test", createdAt: 1, storageVersion: 1 }, storage);
		await session.mutate(
			"main",
			(mutator) =>
				mutator.commit(
					{
						writes: [
							{ kind: "register", op: "set", namespace: "lane.leaf", key: "main", value: null },
							{
								kind: "register",
								op: "set",
								namespace: "lane.state",
								key: "main",
								value: { currentOperationId: null, pendingNextRun: [] },
							},
						],
					},
					BACKGROUND_CONTEXT,
				),
			BACKGROUND_CONTEXT,
		);
		const faux = fauxProvider();
		const models = createModels();
		models.setProvider(faux.provider);
		const { harness } = await AgentHarness.create({ session, models, model: faux.getModel() }, BACKGROUND_CONTEXT);
		storage.failCommits = true;

		let fault: unknown;
		try {
			await harness.setThinkingLevel("high", BACKGROUND_CONTEXT);
		} catch (error) {
			fault = error;
		}
		expect(fault).toBeInstanceOf(HarnessFault);
		await expect(harness.getThinkingLevel(BACKGROUND_CONTEXT)).rejects.toBe(fault);
		await harness.close(BACKGROUND_CONTEXT);
	});
});
