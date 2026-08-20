import type { Model, SimpleStreamOptions, StreamFunction } from "../types.ts";
import {
	type BedrockMantleAuthOptions,
	getRegionFromBedrockMantleBaseUrl,
	prepareBedrockMantleAuth,
} from "./bedrock-mantle-auth.ts";
import {
	type OpenAIResponsesOptions,
	stream as openAIResponsesStream,
	streamSimple as openAIResponsesStreamSimple,
} from "./openai-responses.ts";

export interface AmazonBedrockMantleOpenAIResponsesOptions extends OpenAIResponsesOptions, BedrockMantleAuthOptions {}

function getMantleOpenAIResponsesBaseUrl(region: string): string {
	return `https://bedrock-mantle.${region}.api.aws/openai/v1`;
}

function withMantleOpenAIResponsesOptions(
	model: Model<"openai-responses">,
	options: AmazonBedrockMantleOpenAIResponsesOptions | undefined,
): { model: Model<"openai-responses">; options: OpenAIResponsesOptions } {
	const auth = prepareBedrockMantleAuth(options, {
		modelBaseUrl: model.baseUrl,
		headers: model.headers,
		baseUrlForRegion: getMantleOpenAIResponsesBaseUrl,
		regionFromBaseUrl: getRegionFromBedrockMantleBaseUrl,
	});
	const requestModel = { ...model, baseUrl: auth.baseUrl };

	if (auth.type === "bearer") {
		return {
			model: requestModel,
			options: { ...options, apiKey: auth.token, headers: auth.headers },
		};
	}

	return {
		model: requestModel,
		options: {
			...options,
			apiKey: auth.apiKey,
			headers: auth.headers,
			fetch: auth.fetch,
		},
	};
}

export const stream: StreamFunction<"openai-responses", AmazonBedrockMantleOpenAIResponsesOptions> = (
	model,
	context,
	options,
) => {
	const prepared = withMantleOpenAIResponsesOptions(model as Model<"openai-responses">, options);
	return openAIResponsesStream(prepared.model, context, prepared.options);
};

export const streamSimple: StreamFunction<"openai-responses", SimpleStreamOptions> = (model, context, options) => {
	const prepared = withMantleOpenAIResponsesOptions(
		model as Model<"openai-responses">,
		options as AmazonBedrockMantleOpenAIResponsesOptions | undefined,
	);
	return openAIResponsesStreamSimple(prepared.model, context, prepared.options as SimpleStreamOptions);
};
