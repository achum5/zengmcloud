import type { UpdateEvents } from "../../common/types.ts";
import {
	generateTeamTriviaRound,
	getTeamTriviaCatalog,
} from "../core/trivia/teamTrivia.ts";

// Team Trivia: one random team-season quiz per load, plus the catalog of every
// quizzable team-season so the season and team dropdowns are populated before
// the first interaction. Fresh rounds come from the triviaNewTeamRound API
// call, which takes the pickers' current values.
const updateTriviaTeam = async (
	inputs: unknown,
	updateEvents: UpdateEvents,
) => {
	if (updateEvents.includes("firstRun")) {
		let round: Awaited<ReturnType<typeof generateTeamTriviaRound>>;
		let catalog: Awaited<ReturnType<typeof getTeamTriviaCatalog>> | undefined;
		try {
			round = await generateTeamTriviaRound();
			catalog = await getTeamTriviaCatalog();
		} catch (error) {
			console.error("Team trivia round generation failed", error);
			round = undefined;
		}

		return { round, catalog };
	}
};

export default updateTriviaTeam;
