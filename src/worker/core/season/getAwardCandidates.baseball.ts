import { AWARD_NAMES } from "../../../common/constants.baseball.ts";
import { getPlayers, getTopPlayers } from "./awards.ts";
import {
	mvpScore,
	poyScore,
	rpoyFilter,
	royFilter,
} from "./doAwards.baseball.ts";

// `playersOverride` lets the live-odds path pass PROJECTED players in, so the
// field and its ordering come from where the season is heading rather than
// from partial cumulative totals.
const getAwardCandidates = async (season: number, playersOverride?: any[]) => {
	const players = playersOverride ?? (await getPlayers(season));

	const awardCandidates = [
		{
			name: AWARD_NAMES.mvp,
			players: getTopPlayers(
				{
					amount: 10,
					score: mvpScore,
				},
				players,
			),
			stats: ["keyStats", "war"],
		},
		{
			name: AWARD_NAMES.poy,
			players: getTopPlayers(
				{
					amount: 10,
					score: poyScore,
				},
				players,
			),
			stats: ["w", "l", "era", "ip", "rpit"],
		},
		{
			name: AWARD_NAMES.rpoy,
			players: getTopPlayers(
				{
					amount: 10,
					filter: rpoyFilter,
					score: poyScore,
				},
				players,
			),
			stats: ["sv", "era", "ip", "rpit"],
		},
		{
			name: AWARD_NAMES.roy,
			players: getTopPlayers(
				{
					amount: 10,
					filter: royFilter,
					score: mvpScore,
				},
				players,
			),
			stats: ["keyStats", "war"],
		},
	];

	return awardCandidates;
};

export default getAwardCandidates;
