import type { Context } from "../context.ts";
import { SessionInvariantError } from "../session/session.ts";
import type { Session, SessionReader } from "../session/types.ts";
import type { LaneState } from "./types.ts";

/** Restore every configured lane in one session without starting work. */
export async function restoreSession(session: Session, context: Context): Promise<Map<string, LaneState>> {
	const lanes = await session.listRegisters("lane.leaf", undefined, context);
	if (!lanes.some((lane) => lane.key === "main")) throw new SessionInvariantError("Session is missing main lane");
	const restored = await Promise.all(
		lanes.map(async ({ key }) => ({ key, state: await restoreLane(session, key, context) })),
	);
	return new Map(restored.map(({ key, state }) => [key, state]));
}

/** Restore one configured lane without starting work or interpreting its state. */
export function restoreLane(session: Session, lane: string, context: Context): Promise<LaneState> {
	return session.mutate(lane, (reader) => restoreLaneState(reader, lane, context), context);
}

async function restoreLaneState(reader: SessionReader, lane: string, context: Context): Promise<LaneState> {
	const [leaf, configuration, laneState, lastResult] = await Promise.all([
		reader.getRegister("lane.leaf", lane, context),
		reader.getRegister("lane.config", lane, context),
		reader.getRegister("lane.state", lane, context),
		reader.getRegister("lane.lastResult", lane, context),
	]);
	if (leaf === undefined) throw new SessionInvariantError(`Lane ${JSON.stringify(lane)} is missing lane.leaf`);
	if (configuration === undefined)
		throw new SessionInvariantError(`Lane ${JSON.stringify(lane)} is missing lane.config`);
	if (laneState === undefined) throw new SessionInvariantError(`Lane ${JSON.stringify(lane)} is missing lane.state`);

	const operationId = laneState.value.currentOperationId;
	let operation: LaneState["operation"] = null;
	if (operationId !== null) {
		const [meta, state] = await Promise.all([
			reader.getRegister("op.meta", operationId, context),
			reader.getRegister("op.state", operationId, context),
		]);
		if (meta === undefined) throw new SessionInvariantError(`Operation ${operationId} is missing op.meta`);
		if (state === undefined) throw new SessionInvariantError(`Operation ${operationId} is missing op.state`);
		operation = { meta: meta.value, state: state.value };
	}

	return {
		leafId: leaf.value,
		configuration: configuration.value,
		pendingNextRun: laneState.value.pendingNextRun,
		...(lastResult === undefined ? {} : { lastResult: lastResult.value }),
		operation,
	};
}
