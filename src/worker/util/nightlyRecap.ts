import type { FaceConfig } from "facesjs";
import { getBestPlayerBoxScore } from "../../common/getBestPlayerBoxScore.ts";
import type { Game } from "../../common/types.ts";
import { updateRecordForGameResult } from "../../common/updateRecordForGameResult.ts";
import { idb } from "../db/index.ts";
import { g, helpers } from "./index.ts";
import { getTeamInfoBySeason } from "./getTeamInfoBySeason.ts";
import { notability } from "./getAutoRecap.ts";
import { recapGamesForDay } from "./getDayGamesForRecap.ts";
import { formatGameDate } from "../../common/calendarDates.ts";
import { getLiveGate } from "./liveGameGate.ts";

// A NIGHT'S RECAP, as its graphics need it: the night's games that have recap
// plays (see core/game/saveRecapReels.ts), the night's biggest first - the
// same order the day's written recap leads with (notability) - each with what
// its GAME RECAP and FINAL cards show.

export type RecapLeader = {
	pid: number;
	name: string;
	jerseyNumber?: string;
	face?: FaceConfig;
	imgURL?: string;
	pts: number;
	trb: number;
	ast: number;
};

export type RecapCardTeam = {
	tid: number;
	abbrev: string;
	region: string;
	name: string;
	colors: [string, string, string];
	// The uniform its players' faces wear (a jersey style, for facesjs).
	jersey?: string;
	imgURL?: string;
	pts: number;
	// The record going into the game and after it - in the playoffs, the
	// series.
	before: string;
	after: string;
	seed?: number;
	leaders: RecapLeader[];
};

export type RecapNightGame = {
	gid: number;
	// How many plays it has to show, at most.
	numPlays: number;
	// [home, away], as in the box score.
	teams: [RecapCardTeam, RecapCardTeam];
	overtimes: number;
	playoffs: boolean;
	// The man of the game: the best on the winning side, as box scores pick
	// them.
	mvp?: number;
};

// How many of each team's players the FINAL card shows.
export const LEADERS_SHOWN = 3;

const statsOf = (p: any) => ({
	pts: p.pts ?? 0,
	trb: (p.orb ?? 0) + (p.drb ?? 0),
	ast: p.ast ?? 0,
});

const leadersOf = async (players: any[]): Promise<RecapLeader[]> => {
	const ranked = [...players]
		.filter((p) => (p.min ?? 0) > 0)
		.map((p) => ({ p, score: helpers.gameScore(p) }))
		.sort((a, b) => b.score - a.score)
		.slice(0, LEADERS_SHOWN);
	const out: RecapLeader[] = [];
	for (const { p } of ranked) {
		const row = await idb.merged.players.get(p.pid);
		out.push({
			pid: p.pid,
			name: p.name,
			...(p.jerseyNumber === undefined ? {} : { jerseyNumber: p.jerseyNumber }),
			...(row?.face ? { face: row.face } : {}),
			...(row?.imgURL ? { imgURL: row.imgURL } : {}),
			...statsOf(p),
		});
	}
	return out;
};

const record = (t: any, playoffs: boolean) =>
	playoffs
		? t.playoffs
			? `${t.playoffs.won}-${t.playoffs.lost}`
			: ""
		: helpers.formatRecord({
				won: t.won ?? 0,
				lost: t.lost ?? 0,
				tied: t.tied,
				otl: t.otl,
			});

const cardTeam = async (game: Game, i: 0 | 1): Promise<RecapCardTeam> => {
	const t: any = game.teams[i];
	const playoffs = game.playoffs === true;
	const before = helpers.deepCopy(t);
	updateRecordForGameResult(
		before,
		game as any,
		g.get("otl", game.season) ?? false,
		-1,
	);
	const info = await getTeamInfoBySeason(t.tid, game.season);
	return {
		tid: t.tid,
		abbrev: info?.abbrev ?? "",
		region: info?.region ?? "",
		name: info?.name ?? "",
		colors: info?.colors ?? ["#333", "#999", "#fff"],
		...(info?.jersey ? { jersey: info.jersey } : {}),
		...((info as any)?.imgURL ? { imgURL: (info as any).imgURL } : {}),
		pts: t.pts,
		before: record(before, playoffs),
		after: record(t, playoffs),
		...(playoffs && t.playoffs ? { seed: t.playoffs.seed } : {}),
		leaders: await leadersOf(t.players ?? []),
	};
};

export type RecapNight = {
	// The night, as its title says it: its date on a calendar schedule.
	when: string;
	games: RecapNightGame[];
};

export const nightlyRecap = async ({
	season,
	day,
}: {
	season: number;
	day: number;
}): Promise<RecapNight> => {
	const when = `Day ${day}`;
	if (__SPORT !== "basketball") {
		return { when, games: [] };
	}
	// (Not a game still being played live: no highlights till its final.)
	const hidden =
		season === g.get("season") ? (await getLiveGate()).hidden : undefined;
	const numPlays = new Map(
		(await idb.cache.recapReels.getAll())
			.filter(
				(row) =>
					row.season === season && row.day === day && !hidden?.has(row.gid),
			)
			.map((row) => [row.gid, row.reel.plays.length]),
	);
	if (numPlays.size === 0) {
		return { when, games: [] };
	}

	// (The night's biggest first, as its written recap has them.)
	const weight = new Map<number, number>();
	try {
		for (const game of await recapGamesForDay({ season, day })) {
			weight.set(game.gid, notability(game));
		}
	} catch (error) {
		console.error("Failed to order the night's recap", error);
	}

	const out: RecapNightGame[] = [];
	let date: string | undefined;
	for (const [gid, plays] of numPlays) {
		const game = await idb.merged.games.get(gid);
		if (!game) {
			continue;
		}
		date ??= game.date;
		const winner = game.teams.find((t) => t.tid === game.won.tid);
		const best =
			game.won.pts !== game.lost.pts && winner
				? getBestPlayerBoxScore(winner.players)
				: undefined;
		out.push({
			gid,
			numPlays: plays,
			teams: [await cardTeam(game, 0), await cardTeam(game, 1)],
			overtimes: game.overtimes ?? 0,
			playoffs: game.playoffs === true,
			...(best ? { mvp: (best.p as any).pid } : {}),
		});
	}
	out.sort(
		(a, b) =>
			(weight.get(b.gid) ?? 0) - (weight.get(a.gid) ?? 0) || a.gid - b.gid,
	);
	return {
		when: date === undefined ? when : formatGameDate(date, "long"),
		games: out,
	};
};

// The nights that have a recap, newest first.
export const recapNights = async (): Promise<
	{ season: number; day: number }[]
> => {
	const seen = new Map<string, { season: number; day: number }>();
	for (const row of await idb.cache.recapReels.getAll()) {
		seen.set(`${row.season}:${row.day}`, { season: row.season, day: row.day });
	}
	return [...seen.values()].sort(
		(a, b) => b.season - a.season || b.day - a.day,
	);
};
