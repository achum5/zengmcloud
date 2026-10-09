import { g } from "../util/index.ts";
import { generateTriviaGrid } from "../core/trivia/grid.ts";
import { defineView, type ViewArgs } from "../util/defineView.ts";

// The Grids trivia game (Immaculate Grid style). A grid is generated on page
// load; the UI drives the guessing entirely from the returned pools and asks
// for a fresh grid via the triviaNewGrid API call. Deliberately NOT
// regenerated on gameSim - a puzzle shouldn't change mid-solve.
const updateTriviaGrids = async ({ updateEvents }: ViewArgs) => {
	if (updateEvents.has("firstRun")) {
		let data: Awaited<ReturnType<typeof generateTriviaGrid>>;
		try {
			data = await generateTriviaGrid();
		} catch (error) {
			console.error("Trivia grid generation failed", error);
			data = undefined;
		}

		return {
			data,
			season: g.get("season"),
		};
	}
};

export default defineView({
	id: "triviaGrids",
	load: updateTriviaGrids,
});
