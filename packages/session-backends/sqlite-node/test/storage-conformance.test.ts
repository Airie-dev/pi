import { BACKGROUND_CONTEXT } from "@earendil-works/pi-agent-core";
import {
	type ConformanceCase,
	createStorageConformance,
	type StorageFixture,
} from "@earendil-works/pi-agent-core/session/testing";
import { describe, it } from "vitest";
import { createNodeSqliteFactory, SQLITE_STORAGE_VERSION, SqliteStorage, sql } from "../src/index.ts";
import { applyInitialSchema } from "../src/sqlite/migrations.ts";

const NOW = 1_700_000_000_000;
const EMPTY_USAGE = {
	input: 0,
	output: 0,
	cacheRead: 0,
	cacheWrite: 0,
	totalTokens: 0,
	cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
};

// Re-enable each case as the work-in-progress backend implements the missing contract behavior.
const DISABLED_CASES = new Set([
	"branch queries/applies stops before filters and cursors before limits",
	"lifecycle/seals admission, drains admitted commits, and closes idempotently",
]);

function registerConformance(name: string, cases: readonly ConformanceCase[]): void {
	describe(name, () => {
		for (const group of new Set(cases.map((testCase) => testCase.group))) {
			describe(group, () => {
				for (const testCase of cases.filter((candidate) => candidate.group === group)) {
					const register = DISABLED_CASES.has(`${group}/${testCase.name}`) ? it.skip : it;
					register(testCase.name, () => testCase.run());
				}
			});
		}
	});
}

registerConformance(
	"SqliteStorage conformance",
	createStorageConformance(async () => {
		const db = await createNodeSqliteFactory().open(":memory:");
		try {
			await applyInitialSchema(db);
			sql`INSERT INTO session
				(created_at, parent_session_id, storage_version, metadata, message_count, usage_payload, next_seq)
				VALUES (${NOW}, ${null}, ${SQLITE_STORAGE_VERSION}, ${null}, ${0}, ${JSON.stringify(EMPTY_USAGE)}, ${1})`.run(
				db,
			);
			const storage = new SqliteStorage(db, { now: () => NOW });
			return {
				storage,
				async [Symbol.asyncDispose]() {
					try {
						await storage.close(BACKGROUND_CONTEXT);
					} finally {
						db.close();
					}
				},
			} satisfies StorageFixture;
		} catch (error) {
			db.close();
			throw error;
		}
	}),
);
