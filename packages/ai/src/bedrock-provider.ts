import {
	stream as bedrockMantleOpenAIResponsesStream,
	streamSimple as bedrockMantleOpenAIResponsesStreamSimple,
} from "./api/amazon-bedrock-mantle-openai-responses.ts";
import {
	stream as bedrockConverseStream,
	streamSimple as bedrockConverseStreamSimple,
} from "./api/bedrock-converse-stream.ts";

export const bedrockProviderModule = {
	stream: bedrockConverseStream,
	streamSimple: bedrockConverseStreamSimple,
};

export const bedrockMantleOpenAIResponsesProviderModule = {
	stream: bedrockMantleOpenAIResponsesStream,
	streamSimple: bedrockMantleOpenAIResponsesStreamSimple,
};
