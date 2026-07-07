import { idb } from "../db/index.ts";
import { getTeamInfoBySeason } from "./getTeamInfoBySeason.ts";

// One player's box-score line, trimmed to the stats worth narrating.
export type RecapPlayer = {
	name: string;
	min: number;
	pts: number;
	reb: number;
	ast: number;
	stl: number;
	blk: number;
	tov: number;
	fg: number;
	fga: number;
	tp: number;
	tpa: number;
	ft: number;
	fta: number;
	pf: number;
	pm?: number;
};

export type RecapTeam = {
	tid: number;
	region: string;
	name: string;
	abbrev: string;
	pts: number;
	players: RecapPlayer[];
};

// Everything an AI needs to write a recap of one completed game.
export type RecapGame = {
	gid: number;
	day: number;
	overtimes: number;
	winnerTid: number;
	teams: [RecapTeam, RecapTeam];
	// Narrative highlights ZenGM already generated (game-winners, milestones, ...).
	clutchPlays: string[];
};

const playerLine = (p: any): RecapPlayer => ({
	name: String(p?.name ?? "Unknown"),
	min: Math.round(p?.min ?? 0),
	pts: p?.pts ?? 0,
	reb: (p?.orb ?? 0) + (p?.drb ?? 0),
	ast: p?.ast ?? 0,
	stl: p?.stl ?? 0,
	blk: p?.blk ?? 0,
	tov: p?.tov ?? 0,
	fg: p?.fg ?? 0,
	fga: p?.fga ?? 0,
	tp: p?.tp ?? 0,
	tpa: p?.tpa ?? 0,
	ft: p?.ft ?? 0,
	fta: p?.fta ?? 0,
	pf: p?.pf ?? 0,
	pm: typeof p?.pm === "number" ? p.pm : undefined,
});

// All completed games on a given day of a season, each with the team names,
// every box-score line, and ZenGM's own highlight plays - the raw material a
// "Copy AI Prompt" button bakes into a recap prompt.
export const getDayGamesForRecap = async ({
	season,
	day,
}: {
	season: number;
	day: number;
}): Promise<RecapGame[]> => {
	const allGames = await idb.getCopies.games({ season }, "noCopyCache");

	const games = allGames.filter(
		(game) => game.day === day && game.won && game.lost,
	);

	const result: RecapGame[] = [];
	for (const game of games) {
		const teams = [] as unknown as [RecapTeam, RecapTeam];
		for (const t of game.teams) {
			const info = await getTeamInfoBySeason(t.tid, season);
			teams.push({
				tid: t.tid,
				region: info?.region ?? "",
				name: info?.name ?? "Team",
				abbrev: info?.abbrev ?? "???",
				pts: t.pts,
				players: (t.players ?? [])
					.filter((p: any) => (p?.min ?? 0) > 0)
					.map(playerLine),
			});
		}

		result.push({
			gid: game.gid,
			day: game.day ?? day,
			overtimes: game.overtimes ?? 0,
			winnerTid: game.won.tid,
			teams,
			clutchPlays: Array.isArray(game.clutchPlays) ? game.clutchPlays : [],
		});
	}

	return result;
};
