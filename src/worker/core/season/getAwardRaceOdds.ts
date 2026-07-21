import getAwardCandidates from "./getAwardCandidates.ts";
import { strengthProbs } from "../../../common/sportsbookOdds.ts";
import { probToAmerican } from "../../../common/sportsbook.ts";

// How sharply award odds follow the formula's score gaps. Deliberately shared
// by the Award Races page and the Sportsbook (via this one function) so the two
// can never disagree - the Sportsbook prices each race off the exact same award
// formula and scores the Award Races page ranks by.
export const AWARD_POWER = 0.9;

// Canonical live award-race odds: the game's own award formula scores turned
// into per-candidate win probabilities (strengthProbs) and then American odds.
// This is the single source of truth for both the Award Races view and the
// Sportsbook's award futures. Each returned player keeps every field
// getAwardCandidates provides, plus an `odds` (American) field.
const getAwardRaceOdds = async (season: number) => {
	const races = await getAwardCandidates(season);
	return races.map((row) => {
		const probs = strengthProbs(
			row.players.map((p: any) =>
				typeof p.awardScore === "number" ? p.awardScore : 0,
			),
			AWARD_POWER,
		);
		const players = row.players.map((p: any, i: number) => ({
			...p,
			odds: probToAmerican(probs[i] ?? 0),
		}));
		return { ...row, players };
	});
};

export default getAwardRaceOdds;
