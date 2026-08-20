import type { AwsCredentialIdentity, AwsCredentialIdentityProvider, HttpRequest } from "@smithy/types";
import type { ProviderEnv, ProviderHeaders } from "../types.ts";
import { getProviderEnvValue } from "../utils/provider-env.ts";

export interface BedrockMantleAuthOptions {
	region?: string;
	profile?: string;
	/** Bearer token auth for Bedrock. If omitted, the AWS SDK credential chain is used and requests are SigV4 signed. */
	bearerToken?: string;
	/** For Bedrock wrappers, apiKey is treated as a Bedrock bearer token. */
	apiKey?: string;
	env?: ProviderEnv;
	headers?: ProviderHeaders;
	fetch?: typeof globalThis.fetch;
}

export type BedrockMantleAuth =
	| {
			type: "bearer";
			baseUrl: string;
			token: string;
			headers: ProviderHeaders;
	  }
	| {
			type: "sigv4";
			baseUrl: string;
			apiKey: string;
			headers: ProviderHeaders;
			fetch: typeof globalThis.fetch;
	  };

export interface PrepareBedrockMantleAuthParams {
	modelBaseUrl?: string;
	headers?: ProviderHeaders;
	baseUrlForRegion(region: string): string;
	regionFromBaseUrl?(baseUrl: string | undefined): string | undefined;
	fallbackRegion?: string;
	signingService?: string;
	dummyApiKey?: string;
}

const FALLBACK_REGION = "us-east-1";
const SIGNING_SERVICE = "bedrock-mantle";
const DUMMY_API_KEY = "aws-sigv4";

export function getRegionFromBedrockMantleBaseUrl(baseUrl: string | undefined): string | undefined {
	if (!baseUrl) return undefined;
	const match = baseUrl.match(/^https:\/\/bedrock-mantle\.([a-z0-9-]+)\.api\.aws(?:\/|$)/);
	return match?.[1];
}

function getConfiguredProfile(options?: Pick<BedrockMantleAuthOptions, "profile" | "env">): string | undefined {
	return options?.profile || getProviderEnvValue("AWS_PROFILE", options?.env);
}

async function loadSigV4Dependencies() {
	try {
		const [{ Sha256 }, { defaultProvider }, { SignatureV4 }] = await Promise.all([
			import("@aws-crypto/sha256-js"),
			import("@aws-sdk/credential-provider-node"),
			import("@smithy/signature-v4"),
		]);
		return { Sha256, defaultProvider, SignatureV4 };
	} catch (error) {
		throw new Error(
			"AWS credential-chain auth for Amazon Bedrock Mantle APIs requires optional peer dependencies. " +
				"Install @aws-crypto/sha256-js, @aws-sdk/credential-provider-node, and @smithy/signature-v4, " +
				"or set AWS_BEARER_TOKEN_BEDROCK to use bearer-token auth.",
			{ cause: error },
		);
	}
}

function getConfiguredCredentials(
	defaultProvider: typeof import("@aws-sdk/credential-provider-node").defaultProvider,
	options?: Pick<BedrockMantleAuthOptions, "profile" | "env">,
): AwsCredentialIdentity | AwsCredentialIdentityProvider {
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

function getBearerToken(options?: BedrockMantleAuthOptions): string | undefined {
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
	options: BedrockMantleAuthOptions | undefined,
	region: string,
	signingService: string,
): typeof fetch {
	const baseFetch = options?.fetch ?? globalThis.fetch;
	return async (input, init) => {
		const request = new Request(input, init);
		const url = new URL(request.url);
		const body = await requestBody(request);
		const headers = headersToObject(request.headers);

		// SDK adapters may inject protocol-specific placeholder auth headers.
		// For AWS credential-chain auth, replace them with SigV4 Authorization.
		delete headers.authorization;
		delete headers["x-api-key"];
		headers.host = url.host;
		if (body && !headers["content-length"]) headers["content-length"] = String(body.byteLength);

		const { Sha256, SignatureV4, defaultProvider } = await loadSigV4Dependencies();
		const signer = new SignatureV4({
			credentials: getConfiguredCredentials(defaultProvider, options),
			region,
			service: signingService,
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

function resolveRegion(options: BedrockMantleAuthOptions | undefined, params: PrepareBedrockMantleAuthParams): string {
	return (
		options?.region ||
		params.regionFromBaseUrl?.(params.modelBaseUrl) ||
		getProviderEnvValue("AWS_REGION", options?.env) ||
		getProviderEnvValue("AWS_DEFAULT_REGION", options?.env) ||
		params.fallbackRegion ||
		FALLBACK_REGION
	);
}

export function prepareBedrockMantleAuth(
	options: BedrockMantleAuthOptions | undefined,
	params: PrepareBedrockMantleAuthParams,
): BedrockMantleAuth {
	const region = resolveRegion(options, params);
	const baseUrl = params.baseUrlForRegion(region);
	const headers: ProviderHeaders = { ...params.headers, ...options?.headers };
	const bearerToken = getBearerToken(options);

	if (bearerToken) {
		return { type: "bearer", baseUrl, token: bearerToken, headers };
	}

	return {
		type: "sigv4",
		baseUrl,
		apiKey: params.dummyApiKey ?? DUMMY_API_KEY,
		headers,
		fetch: createSigV4Fetch(options, region, params.signingService ?? SIGNING_SERVICE),
	};
}
