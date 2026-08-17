import { BACKGROUND_CONTEXT, withTelemetryContext } from "../../../src/harness/context.ts";
// AgentHarness R4 context-bound tool example.
// Run from packages/agent: node test/harness/scratch/r4.ts
// Uses the faux provider and makes no external requests.

import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createModels, fauxAssistantMessage, fauxProvider, fauxToolCall } from "@earendil-works/pi-ai";
import { InMemoryTelemetryContext } from "@earendil-works/pi-telemetry";
import { NodeExecutionEnv } from "../../../src/harness/env/nodejs.ts";
import { AgentHarness, createReadTool, getOrThrow, MemorySessionRepo } from "../../../src/index.ts";

const directory = await mkdtemp(join(tmpdir(), "pi-agent-r4-"));
const env = new NodeExecutionEnv({ cwd: directory });
const repo = new MemorySessionRepo();
const session = await repo.create({}, BACKGROUND_CONTEXT);
const faux = fauxProvider({ tokenSize: { min: 1, max: 1 } });
const models = createModels();
models.setProvider(faux.provider);
const telemetry = new InMemoryTelemetryContext();

function printJson(label: string, value: unknown): void {
	console.log(`${label}:\n${JSON.stringify(value, null, 2)}`);
}
await env.writeFile("example.txt", "durable tool output\n", BACKGROUND_CONTEXT);

faux.setResponses([
	fauxAssistantMessage(fauxToolCall("read", { path: "example.txt" }, { id: "read-call" }), {
		stopReason: "toolUse",
	}),
	(context) => {
		const result = context.messages.find((message) => message.role === "toolResult");
		if (result?.role !== "toolResult" || result.content[0]?.type !== "text") {
			throw new Error("read result is missing");
		}
		return fauxAssistantMessage(`Observed: ${result.content[0].text.trim()}`);
	},
]);

const { harness } = await AgentHarness.create(
	{
		session,
		models,
		model: faux.getModel(),
		tools: [createReadTool()],
		activeToolNames: ["read"],
		toolContext: { env },
	},
	withTelemetryContext(telemetry, BACKGROUND_CONTEXT),
);

for (const type of ["tool_start", "tool_end", "turn_end"] as const) {
	harness.events.on(type, (event) => {
		printJson(type, event);
	});
}
harness.hooks.on("before_tool", async ({ runId, toolCallId }) => {
	const state = await session.getRegister("op.state", runId, BACKGROUND_CONTEXT);
	if (state?.value.kind !== "run" || state.value.phase.kind !== "tools") return undefined;
	printJson("invocation", {
		toolCallId,
		invocationId: state.value.phase.batch.calls[0]?.resultEntryId,
	});
	return undefined;
});

try {
	const admission = getOrThrow(
		await harness.accept({ kind: "prompt", prompt: "Read example.txt" }, BACKGROUND_CONTEXT),
	);
	printJson("accepted", admission);
	const driven = getOrThrow(await harness.drive({ operationId: admission.operationId }, BACKGROUND_CONTEXT));
	printJson("drive", driven);
	console.log("verified:", getOrThrow(await env.readTextFile("example.txt", BACKGROUND_CONTEXT)).trim());
	printJson("telemetry spans", telemetry.getSpans());
} finally {
	await harness.close(BACKGROUND_CONTEXT);
	await env.cleanup(BACKGROUND_CONTEXT);
	await repo.close(BACKGROUND_CONTEXT);
	await rm(directory, { recursive: true, force: true });
}
