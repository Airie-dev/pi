import { BACKGROUND_CONTEXT } from "../../../src/harness/context.ts";
// Minimal AgentHarness R1 runtime-shell example.
// Run from packages/agent: node test/harness/scratch/r1.ts
// Uses the faux provider and makes no external requests.

import { createModels, fauxProvider } from "@earendil-works/pi-ai";
import { AgentHarness, MemorySessionRepo } from "../../../src/index.ts";

const repo = new MemorySessionRepo();
const session = await repo.create({}, BACKGROUND_CONTEXT);
const faux = fauxProvider();
const models = createModels();
models.setProvider(faux.provider);

const { harness, suspended } = await AgentHarness.create(
	{
		session,
		models,
		model: faux.getModel(),
		activeToolNames: [],
	},
	BACKGROUND_CONTEXT,
);

harness.events.on("config_update", (event) => {
	console.log(`config: ${event.property}`);
});
harness.events.on("lane_created", (event) => {
	console.log(`lane:   created ${event.lane} at ${event.at ?? "root"}`);
});

try {
	console.log("restored:", suspended);
	console.log("main:    ", await harness.inspectExecution(BACKGROUND_CONTEXT));

	await harness.setThinkingLevel("high", BACKGROUND_CONTEXT);
	const worker = await harness.createLane("worker", null, BACKGROUND_CONTEXT);
	if (!worker.ok) throw worker.error;
	await worker.value.setThinkingLevel("low", BACKGROUND_CONTEXT);

	console.log("lanes:   ", await harness.lanes(BACKGROUND_CONTEXT));
	console.log("worker:  ", await worker.value.inspectExecution(BACKGROUND_CONTEXT));
} finally {
	await harness.close(BACKGROUND_CONTEXT);
	await repo.close(BACKGROUND_CONTEXT);
}
