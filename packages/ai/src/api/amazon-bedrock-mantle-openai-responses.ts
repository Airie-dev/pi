import { Sha256 } from "@aws-crypto/sha256-js";
import { defaultProvider } from "@aws-sdk/credential-provider-node";
import { SignatureV4 } from "@smithy/signature-v4";
import type { AwsCredentialIdentity, HttpRequest } from "@smithy/types";
import type { Model, ProviderHeaders, SimpleStreamOptions, StreamFunction } from "../types.ts";
import { getProviderEnvValue } from "../utils/provider-env.ts";
import {
	type OpenAIResponsesOptions,
	stream as openAIResponsesStream,
	streamSimple as openAIResponsesStreamSimple,
} from "./openai-responses.ts";

export interface AmazonBedrockMantleOpenAIResponsesOptions extends OpenAIResponsesOptions {
	region?: string;
	profile?: string;
	/** Bearer token auth for Bedrock. If omitted, the AWS SDK credential chain is used and requests are SigV4 signed. */
	bearerToken?: string;
}

const FALLBACK_REGION = "us-east-1";
const SIGNING_SERVICE = "bedrock-mantle";
const DUMMY_OPENAI_API_KEY = "aws-sigv4";

function getConfiguredProfile(
	options?: Pick<AmazonBedrockMantleOpenAIResponsesOptions, "profile" | "env">,
): string | undefined {
	return options?.profile || getProviderEnvValue("AWS_PROFILE", options?.env);
}

function getConfiguredCredentials(
	options?: Pick<AmazonBedrockMantleOpenAIResponsesOptions, "profile" | "env">,
): AwsCredentialIdentity | ReturnType<typeof defaultProvider> {
	const profile = getConfiguredProfile(options);
	if (profile) return defaultProvider({ profile });

	const accessKeyId = getProviderEnvValue("AWS_ACCESS_KEY_ID", options?.env);
	const secretAccessKey = getProviderEnvValue("AWS_SECRET_ACCESS_KEY", options?.env);
	if (accessKeyId && secretAccessKey) {
		return {
			accessKeyId,
			secretAccessKey,
			sessionToken: getProviderEnvValue("AWS_SESSION_TOKEN", options?.env),
		};
	}

	return defaultProvider();
}

function getRegionFromMantleBaseUrl(baseUrl: string | undefined): string | undefined {
	if (!baseUrl) return undefined;
	const match = baseUrl.match(/^https:\/\/bedrock-mantle\.([a-z0-9-]+)\.api\.aws(?:\/|$)/);
	return match?.[1];
}

function getMantleBaseUrl(region: string): string {
	return `https://bedrock-mantle.${region}.api.aws/openai/v1`;
}

function getBearerToken(options?: AmazonBedrockMantleOpenAIResponsesOptions): string | undefined {
	return (
		options?.bearerToken ||
		options?.apiKey ||
		getProviderEnvValue("AWS_BEARER_TOKEN_BEDROCK", options?.env) ||
		undefined
	);
}

function headersToObject(headers: Headers): Record<string, string> {
	const out: Record<string, string> = {};
	for (const [key, value] of headers.entries()) out[key.toLowerCase()] = value;
	return out;
}

function objectToHeaders(headers: Record<string, string>, suppressHost = true): Headers {
	const out = new Headers();
	for (const [key, value] of Object.entries(headers)) {
		// WHATWG fetch controls Host itself; keeping it can throw in browser-like runtimes.
		if (suppressHost && key.toLowerCase() === "host") continue;
		out.set(key, value);
	}
	return out;
}

function urlQueryToSmithy(url: URL): HttpRequest["query"] {
	const query: NonNullable<HttpRequest["query"]> = {};
	for (const [key, value] of url.searchParams.entries()) {
		const current = query[key];
		if (current === undefined || current === null) query[key] = value;
		else if (Array.isArray(current)) current.push(value);
		else query[key] = [current, value];
	}
	return query;
}

async function requestBody(request: Request): Promise<ArrayBuffer | undefined> {
	if (request.method === "GET" || request.method === "HEAD") return undefined;
	const buffer = await request.clone().arrayBuffer();
	return buffer.byteLength === 0 ? undefined : buffer;
}

function createSigV4Fetch(
	options: AmazonBedrockMantleOpenAIResponsesOptions | undefined,
	region: string,
): typeof fetch {
	const baseFetch = options?.fetch ?? globalThis.fetch;
	return async (input, init) => {
		const request = new Request(input, init);
		const url = new URL(request.url);
		const body = await requestBody(request);
		const headers = headersToObject(request.headers);

		// The OpenAI SDK injects Authorization: Bearer <apiKey>. For AWS credential-chain auth,
		// replace that placeholder with SigV4 Authorization.
		delete headers.authorization;
		headers.host = url.host;
		if (body && !headers["content-length"]) headers["content-length"] = String(body.byteLength);

		const signer = new SignatureV4({
			credentials: getConfiguredCredentials(options),
			region,
			service: SIGNING_SERVICE,
			sha256: Sha256,
		});
		const signed = await signer.sign({
			method: request.method,
			protocol: url.protocol,
			hostname: url.hostname,
			port: url.port ? Number(url.port) : undefined,
			path: url.pathname,
			query: urlQueryToSmithy(url),
			headers,
			body,
		});

		return baseFetch(url, {
			method: request.method,
			headers: objectToHeaders(signed.headers),
			body,
			signal: request.signal,
		});
	};
}

function withMantleOptions(
	model: Model<"openai-responses">,
	options: AmazonBedrockMantleOpenAIResponsesOptions | undefined,
): { model: Model<"openai-responses">; options: OpenAIResponsesOptions } {
	const region =
		options?.region ||
		getProviderEnvValue("AWS_REGION", options?.env) ||
		getProviderEnvValue("AWS_DEFAULT_REGION", options?.env) ||
		getRegionFromMantleBaseUrl(model.baseUrl) ||
		FALLBACK_REGION;
	const bearerToken = getBearerToken(options);
	const requestModel = { ...model, baseUrl: getMantleBaseUrl(region) };
	const headers: ProviderHeaders = { ...model.headers, ...options?.headers };

	if (bearerToken) {
		return {
			model: requestModel,
			options: { ...options, apiKey: bearerToken, headers },
		};
	}

	return {
		model: requestModel,
		options: {
			...options,
			apiKey: DUMMY_OPENAI_API_KEY,
			headers,
			fetch: createSigV4Fetch(options, region),
		},
	};
}

export const stream: StreamFunction<"openai-responses", AmazonBedrockMantleOpenAIResponsesOptions> = (
	model,
	context,
	options,
) => {
	const prepared = withMantleOptions(model as Model<"openai-responses">, options);
	return openAIResponsesStream(prepared.model, context, prepared.options);
};

export const streamSimple: StreamFunction<"openai-responses", SimpleStreamOptions> = (model, context, options) => {
	const prepared = withMantleOptions(
		model as Model<"openai-responses">,
		options as AmazonBedrockMantleOpenAIResponsesOptions | undefined,
	);
	return openAIResponsesStreamSimple(prepared.model, context, prepared.options as SimpleStreamOptions);
};
