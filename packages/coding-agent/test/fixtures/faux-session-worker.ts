import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { AgentHarness } from "@earendil-works/pi-agent-core";
import { createModels, fauxAssistantMessage, fauxProvider } from "@earendil-works/pi-ai";
import { BACKGROUND_CONTEXT } from "../../src/experimental/context.ts";
import { consumeInternalProcessRole } from "../../src/experimental/process.ts";
import { runSessionWorkerWithHarness } from "../../src/experimental/session-worker.ts";

if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
	const role = consumeInternalProcessRole();
	if (role !== "session-worker") throw new Error("Faux Session worker requires a session-worker invocation");
	void runSessionWorkerWithHarness(process.argv.slice(2), async (session, options) => {
		if (options.provider !== "anthropic" || options.model !== "claude-sonnet-4-5") {
			throw new Error(`Unexpected faux worker model: ${options.provider}/${options.model}`);
		}
		const faux = fauxProvider();
		faux.setResponses([fauxAssistantMessage("deterministic remote answer", { timestamp: 20 })]);
		const models = createModels();
		models.setProvider(faux.provider);
		return (
			await AgentHarness.create(
				{
					session,
					models,
					model: faux.getModel(),
					tools: [],
					resources: {},
				},
				BACKGROUND_CONTEXT,
			)
		).harness;
	}).catch((error: unknown) => {
		console.error(error);
		process.exit(1);
	});
}
