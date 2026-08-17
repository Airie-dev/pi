import { type Context, type ContextKey, NOOP_TELEMETRY_CONTEXT } from "@earendil-works/pi-agent-core";

/** Root context used by process boundaries that do not yet receive one from a caller. */
export const BACKGROUND_CONTEXT: Context = {
	abortSignal: undefined,
	telemetryContext: NOOP_TELEMETRY_CONTEXT,
	value<T>(_key: ContextKey<T>): T | undefined {
		return undefined;
	},
};
