import { g } from "../util/index.ts";
import type { UpdateEvents } from "../../common/types.ts";
import { generateTriviaGrid } from "../core/trivia/grid.ts";

// The Grids trivia game (Immaculate Grid style). A grid is generated on page
// load; the UI drives the guessing entirely from the returned pools and asks
// for a fresh grid via the triviaNewGrid API call. Deliberately NOT
// regenerated on gameSim - a puzzle shouldn't change mid-solve.
const updateTriviaGrids = async (
	inputs: unknown,
	updateEvents: UpdateEvents,
) => {
	if (updateEvents.includes("firstRun")) {
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

export default updateTriviaGrids;
