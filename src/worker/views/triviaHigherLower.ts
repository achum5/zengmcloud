import { buildHigherLowerPool } from "../core/trivia/higherLower.ts";
import { defineView, type ViewArgs } from "../util/defineView.ts";

// Higher or Lower: the worker ships every player's category values once; the
// whole streak game runs in the UI from that pool.
const updateTriviaHigherLower = async ({ updateEvents }: ViewArgs) => {
	if (updateEvents.has("firstRun")) {
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

export default defineView({
	id: "triviaHigherLower",
	load: updateTriviaHigherLower,
});
