import type { UpdateEvents } from "../../common/types.ts";
import { generateTeamTriviaRound } from "../core/trivia/teamTrivia.ts";

// Team Trivia: one random team-season quiz per load; the UI runs the rounds
// and asks for a fresh one via the triviaNewTeamRound API call.
const updateTriviaTeam = async (
	inputs: unknown,
	updateEvents: UpdateEvents,
) => {
	if (updateEvents.includes("firstRun")) {
		let round: Awaited<ReturnType<typeof generateTeamTriviaRound>>;
		try {
			round = await generateTeamTriviaRound();
		} catch (error) {
			console.error("Team trivia round generation failed", error);
			round = undefined;
		}

		return { round };
	}
};

export default updateTriviaTeam;
