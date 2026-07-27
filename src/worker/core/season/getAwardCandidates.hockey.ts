import { getPlayers, getTopPlayers } from "./awards.ts";
import {
	dpoyScore,
	dfoyFilter,
	mvpScore,
	goyScore,
	royFilter,
	royScore,
} from "./doAwards.hockey.ts";

// `playersOverride` lets the live-odds path pass PROJECTED players in, so the
// field and its ordering come from where the season is heading rather than
// from partial cumulative totals.
const getAwardCandidates = async (season: number, playersOverride?: any[]) => {
	const players = playersOverride ?? (await getPlayers(season));

	const awardCandidates = [
		{
			name: "Most Valuable Player",
			players: getTopPlayers(
				{
					amount: 10,
					score: mvpScore,
				},
				players,
			),
			stats: ["keyStats", "ps"],
		},
		{
			name: "Defensive Player of the Year",
			players: getTopPlayers(
				{
					amount: 10,
					score: dpoyScore,
				},
				players,
			),
			stats: ["tk", "hit", "dps"],
		},
		{
			name: "Defensive Forward of the Year",
			players: getTopPlayers(
				{
					amount: 10,
					filter: dfoyFilter,
					score: dpoyScore,
				},
				players,
			),
			stats: ["tk", "hit", "dps"],
		},
		{
			name: "Goalie of the Year",
			players: getTopPlayers(
				{
					amount: 10,
					score: goyScore,
				},
				players,
			),
			stats: ["gpGoalie", "gaa", "svPct", "gps"],
		},
		{
			name: "Rookie of the Year",
			players: getTopPlayers(
				{
					amount: 10,
					filter: royFilter,
					score: royScore,
				},
				players,
			),
			stats: ["keyStats", "ps"],
		},
	];

	return awardCandidates;
};

export default getAwardCandidates;
