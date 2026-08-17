import { HarnessClosed, HarnessFault } from "../agent-harness.ts";
import type { Context } from "../context.ts";
import { type RestoredLane, restoreLane } from "../restore.ts";
import { SessionInvariantError } from "../session/session.ts";
import type { SessionMutator } from "../session/types.ts";
import type { RuntimeProcedureContext } from "./types.ts";
import { RuntimeSliceNotImplemented } from "./types.ts";

/** Load one expected operation without changing durable state. */
export async function loadExpected<TContext extends object | undefined>(
	runtime: RuntimeProcedureContext<TContext>,
	lane: string,
	operationId: string,
	includeLastResult: boolean,
	context: Context,
): Promise<RestoredLane> {
	try {
		return await runtime.sessionStorage.mutate(
			lane,
			async (reader) => {
				const restored = await restoreLane(reader, lane, { includeLastResult }, context);
				const currentId = restored.laneState.currentOperationId;
				if (currentId !== null && currentId !== operationId) {
					throw new SessionInvariantError(`Lane ${JSON.stringify(lane)} changed operation while a drive owns it`);
				}
				return restored;
			},
			context,
		);
	} catch (error) {
		if (error instanceof HarnessClosed || error instanceof HarnessFault) throw error;
		throw runtime.fault(error, context);
	}
}

/**
 * Run one checked lane mutation. Procedure-specific phase and identity checks
 * remain in the callback beside the write list.
 */
export async function mutateRun<TContext extends object | undefined, Result>(
	runtime: RuntimeProcedureContext<TContext>,
	lane: string,
	mutation: (input: { mutator: SessionMutator; restored: RestoredLane }) => Result | Promise<Result>,
	context: Context,
): Promise<Result> {
	runtime.assertOpen();
	try {
		return await runtime.sessionStorage.mutate(
			lane,
			async (mutator) => {
				const restored = await restoreLane(mutator, lane, undefined, context);
				return mutation({ mutator, restored });
			},
			context,
		);
	} catch (error) {
		if (
			error instanceof RuntimeSliceNotImplemented ||
			error instanceof HarnessClosed ||
			error instanceof HarnessFault
		) {
			throw error;
		}
		throw runtime.fault(error, context);
	}
}
