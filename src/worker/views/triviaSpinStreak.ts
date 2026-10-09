import { getTriviaPool } from "../core/trivia/pool.ts";
import { defineView, type ViewArgs } from "../util/defineView.ts";

// Spin Streak: questions are generated one at a time through the
// triviaSpinQuestion API call. The view only says whether the league has
// enough history to play.
const updateTriviaSpinStreak = async ({ updateEvents }: ViewArgs) => {
	if (updateEvents.has("firstRun")) {
		let numPlayers = 0;
		try {
			const pool = await getTriviaPool();
			numPlayers = pool.players.filter((p) => p.tot.gp >= 20).length;
		} catch (error) {
			console.error("Spin Streak pool failed", error);
		}
		return { numPlayers };
	}
};

export default defineView({
	id: "triviaSpinStreak",
	load: updateTriviaSpinStreak,
});
