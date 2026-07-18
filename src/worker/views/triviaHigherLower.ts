import type { UpdateEvents } from "../../common/types.ts";
import { buildHigherLowerPool } from "../core/trivia/higherLower.ts";

// Higher or Lower: the worker ships every player's category values once; the
// whole streak game runs in the UI from that pool.
const updateTriviaHigherLower = async (
	inputs: unknown,
	updateEvents: UpdateEvents,
) => {
	if (updateEvents.includes("firstRun")) {
		let players: Awaited<ReturnType<typeof buildHigherLowerPool>>;
		try {
			players = await buildHigherLowerPool();
		} catch (error) {
			console.error("Higher or Lower pool failed", error);
			players = [];
		}

		return { players };
	}
};

export default updateTriviaHigherLower;
