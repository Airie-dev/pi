import { describe, expect, it } from "vitest";
import { BACKGROUND_CONTEXT } from "../../src/harness/context.ts";
import { NodeExecutionEnv } from "../../src/harness/env/nodejs.ts";
import { JSONL_STORAGE_VERSION, JsonlSessionRepo } from "../../src/harness/session/jsonl/index.ts";
import { getOrThrow } from "../../src/harness/types.ts";
import { createTempDir } from "./session-test-utils.ts";

const NOW = 1_700_000_000_000;

describe("JsonlSessionRepo cwd-scoped lifecycle", () => {
	it("persists metadata and filters discovery by cwd", async () => {
		const fileSystem = new NodeExecutionEnv({ cwd: createTempDir() });
		const repo = new JsonlSessionRepo({ fileSystem, sessionsRoot: "sessions", now: () => NOW });
		const session = await repo.create(
			{ id: "child", cwd: "/workspace", parentSessionId: "parent" },
			BACKGROUND_CONTEXT,
		);
		const metadata = session.metadata;

		expect(metadata).toMatchObject({
			id: "child",
			createdAt: NOW,
			storageVersion: JSONL_STORAGE_VERSION,
			cwd: "/workspace",
			parentSessionId: "parent",
		});
		expect(metadata.path).toContain("/sessions/--workspace--/");
		expect(metadata.path.endsWith("_child.jsonl")).toBe(true);
		expect(Number.isFinite(metadata.modifiedAt)).toBe(true);
		await session.close(BACKGROUND_CONTEXT);

		expect(await repo.list({ cwd: "/other" }, BACKGROUND_CONTEXT)).toEqual([]);
		expect(await repo.list({ cwd: "/workspace" }, BACKGROUND_CONTEXT)).toEqual([metadata]);
		const firstLine = getOrThrow(
			await fileSystem.readTextLines(metadata.path, { maxLines: 1 }, BACKGROUND_CONTEXT),
		)[0];
		expect(JSON.parse(firstLine!)).toEqual({
			v: 4,
			kind: "header",
			id: "child",
			storageVersion: JSONL_STORAGE_VERSION,
			createdAt: NOW,
			cwd: "/workspace",
			parentSessionId: "parent",
		});
		await repo.close(BACKGROUND_CONTEXT);
	});

	it("allows the same id to be active in different working directories", async () => {
		const fileSystem = new NodeExecutionEnv({ cwd: createTempDir() });
		const repo = new JsonlSessionRepo({ fileSystem, sessionsRoot: "sessions", now: () => NOW });
		const first = await repo.create({ id: "shared", cwd: "/workspace-a" }, BACKGROUND_CONTEXT);
		const second = await repo.create({ id: "shared", cwd: "/workspace-b" }, BACKGROUND_CONTEXT);

		expect(first.metadata.path).not.toBe(second.metadata.path);
		await expect(repo.create({ id: "shared", cwd: "/workspace-a" }, BACKGROUND_CONTEXT)).rejects.toThrow(
			"already exists",
		);
		expect((await repo.list(undefined, BACKGROUND_CONTEXT)).map(({ cwd, id }) => ({ cwd, id }))).toEqual([
			{ cwd: "/workspace-a", id: "shared" },
			{ cwd: "/workspace-b", id: "shared" },
		]);

		await Promise.all([first.close(BACKGROUND_CONTEXT), second.close(BACKGROUND_CONTEXT)]);
		const reopened = await Promise.all([
			repo.open(first.metadata, BACKGROUND_CONTEXT),
			repo.open(second.metadata, BACKGROUND_CONTEXT),
		]);
		await Promise.all(reopened.map((session) => session.close(BACKGROUND_CONTEXT)));
		await repo.close(BACKGROUND_CONTEXT);
	});
});
