import {
	generateTeamTriviaRound,
	getTeamTriviaCatalog,
} from "../core/trivia/teamTrivia.ts";
import { defineView, type ViewArgs } from "../util/defineView.ts";

// Team Trivia: one random team-season quiz per load, plus the catalog of every
// quizzable team-season so the season and team dropdowns are populated before
// the first interaction. Fresh rounds come from the triviaNewTeamRound API
// call, which takes the pickers' current values.
const updateTriviaTeam = async ({ updateEvents }: ViewArgs) => {
	if (updateEvents.has("firstRun")) {
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

export default defineView({
	id: "triviaTeam",
	load: updateTriviaTeam,
});
