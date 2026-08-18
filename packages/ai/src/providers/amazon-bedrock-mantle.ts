import { amazonBedrockMantleOpenAIResponsesApi } from "../api/amazon-bedrock-mantle-openai-responses.lazy.ts";
import { createProvider, type Provider } from "../models.ts";
import { bedrockAuth } from "./amazon-bedrock.ts";
import { AMAZON_BEDROCK_MANTLE_MODELS } from "./amazon-bedrock-mantle.models.ts";

export function amazonBedrockMantleProvider(): Provider<"openai-responses"> {
	return createProvider({
		id: "amazon-bedrock-mantle",
		name: "Amazon Bedrock Mantle",
		auth: { apiKey: bedrockAuth },
		models: Object.values(AMAZON_BEDROCK_MANTLE_MODELS),
		api: amazonBedrockMantleOpenAIResponsesApi(),
	});
}
