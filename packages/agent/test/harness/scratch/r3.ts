import { BACKGROUND_CONTEXT } from "../../../src/harness/context.ts";
// AgentHarness R3 durable retry and reattachment example.
// Run from packages/agent: node test/harness/scratch/r3.ts
// Uses the faux provider and makes no external requests.

import { createModels, fauxAssistantMessage, fauxProvider } from "@earendil-works/pi-ai";
import { AgentHarness, MemorySessionRepo } from "../../../src/index.ts";

const repo = new MemorySessionRepo();
const session = await repo.create({}, BACKGROUND_CONTEXT);
const metadata = session.metadata;
const faux = fauxProvider();
const models = createModels();
models.setProvider(faux.provider);
faux.setResponses([
	fauxAssistantMessage([], { stopReason: "error", errorMessage: "503 service unavailable" }),
	fauxAssistantMessage("retry succeeded"),
]);

const first = await AgentHarness.create(
	{
		session,
		models,
		model: faux.getModel(),
		activeToolNames: [],
		retry: { enabled: true, maxRetries: 1, baseDelayMs: 100 },
	},
	BACKGROUND_CONTEXT,
);

const admission = await first.harness.accept({ kind: "prompt", prompt: "run the retry example" }, BACKGROUND_CONTEXT);
if (!admission.ok) throw admission.error;
const operationId = admission.value.operationId;
const waiting = await first.harness.drive({ operationId, waitForRetry: false }, BACKGROUND_CONTEXT);
console.log("first drive:", waiting);
console.log("provider calls before detach:", faux.state.callCount);
await first.harness.close(BACKGROUND_CONTEXT);

const reopenedSession = await repo.open(metadata, BACKGROUND_CONTEXT);
const reopenedModels = createModels();
reopenedModels.setProvider(faux.provider);
const second = await AgentHarness.create(
	{
		session: reopenedSession,
		models: reopenedModels,
		model: faux.getModel(),
		activeToolNames: [],
	},
	BACKGROUND_CONTEXT,
);
second.harness.events.on("message_update", ({ event }) => {
	if (event.type === "text_delta") process.stdout.write(event.delta);
});

try {
	process.stdout.write("assistant: ");
	const result = await second.harness.drive({ operationId, waitForRetry: true }, BACKGROUND_CONTEXT);
	process.stdout.write("\n");
	console.log("reattached drive:", result);
	console.log("provider calls after completion:", faux.state.callCount);
} finally {
	await second.harness.close(BACKGROUND_CONTEXT);
	await repo.close(BACKGROUND_CONTEXT);
}
