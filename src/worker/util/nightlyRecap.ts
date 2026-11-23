import type { FaceConfig } from "facesjs";
import { getBestPlayerBoxScore } from "../../common/getBestPlayerBoxScore.ts";
import type { Game } from "../../common/types.ts";
import { updateRecordForGameResult } from "../../common/updateRecordForGameResult.ts";
import { idb } from "../db/index.ts";
import { g, helpers } from "./index.ts";
import { getTeamInfoBySeason } from "./getTeamInfoBySeason.ts";
import { notability } from "./getAutoRecap.ts";
import { formatGameDate } from "../../common/calendarDates.ts";
import { getLiveGate } from "./liveGameGate.ts";
import {
	computeStandingsAsOf,
	recapGamesForDay,
	type RecapGame,
} from "./getDayGamesForRecap.ts";
import { gameScore } from "../views/dailyScheduleHub.ts";
import {
	pickTopPlays,
	starPlayCount,
	TOP_PLAYS_MOST,
} from "../../common/recapLength.ts";
import type { RecapPlay } from "../../common/recapReel.ts";

// A NIGHT'S RECAP, as its graphics need it: the night's games that have recap
// plays (see core/game/saveRecapReels.ts), the night's biggest first - the
// same order the day's written recap leads with (notability) - each with what
// its GAME RECAP and FINAL cards show; then the Player of the Night, the
// night's top plays and the standings through the night.

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

// THE PLAYER OF THE NIGHT: the night's best game score (as the League
// Hub's top performers have it), of a man with plays of his own in his
// game's recap - with his line for the strip under his plays.
export type RecapStar = {
	gid: number;
	pid: number;
	name: string;
	tid: number;
	abbrev: string;
	colors: [string, string, string];
	imgURL?: string;
	// "47 PTS, 6 REB, 10 3PM VS. CHA"
	line: string;
};

// One of the night's top plays: its game, and its line in that game's
// reel.
export type RecapTopPlay = { gid: number; i: number };

export type RecapStandingsTeam = {
	tid: number;
	abbrev: string;
	colors: [string, string, string];
	imgURL?: string;
	rank: number;
	won: number;
	lost: number;
};

// A conference's teams in the playoff spots, the play-in's and the rest.
export type RecapStandingsConf = {
	name: string;
	groups: {
		label?: "PLAYOFFS" | "PLAY-IN";
		teams: RecapStandingsTeam[];
	}[];
};

export type RecapStandings = {
	// "NOV. 22" or "DAY 6".
	through: string;
	confs: RecapStandingsConf[];
};

export type RecapNight = {
	// The night, as its title says it: its date on a calendar schedule.
	when: string;
	games: RecapNightGame[];
	star?: RecapStar;
	// Best first.
	top: RecapTopPlay[];
	// The regular season's.
	standings?: RecapStandings;
};

// "Nov. 22", as the broadcast's standings write a date.
const MONTHS_AP = [
	"JAN.",
	"FEB.",
	"MARCH",
	"APRIL",
	"MAY",
	"JUNE",
	"JULY",
	"AUG.",
	"SEPT.",
	"OCT.",
	"NOV.",
	"DEC.",
];
const throughOf = (date: string | undefined, day: number) => {
	const m = date === undefined ? null : /^\d+-(\d+)-(\d+)/.exec(date);
	if (!m) {
		return `DAY ${day}`;
	}
	return `${MONTHS_AP[Number(m[1]) - 1] ?? ""} ${Number(m[2])}`.trim();
};

// His line: the points, then what else stood out (at least two more), in the
// box score's order, and who it was against.
const STAR_STATS: [
	key: "reb" | "ast" | "tp" | "blk" | "stl",
	label: string,
	at: number,
][] = [
	["reb", "REB", 5],
	["ast", "AST", 5],
	["tp", "3PM", 5],
	["blk", "BLK", 4],
	["stl", "STL", 4],
];
export const starLine = (
	p: {
		pts: number;
		reb: number;
		ast: number;
		tp: number;
		blk: number;
		stl: number;
	},
	opp: string,
) => {
	const big = STAR_STATS.filter(([key, , at]) => p[key] >= at)
		.sort((a, b) => p[b[0]] / b[2] - p[a[0]] / a[2])
		.slice(0, 3);
	for (const key of ["reb", "ast"] as const) {
		if (big.length < 2 && !big.some((row) => row[0] === key)) {
			big.push(STAR_STATS.find((row) => row[0] === key)!);
		}
	}
	const stats = STAR_STATS.filter((row) => big.includes(row)).map(
		([key, label]) => `${p[key]} ${label}`,
	);
	return `${[`${p.pts} PTS`, ...stats].join(", ")} VS. ${opp}`;
};

// How many men down the night's best game scores are looked at for one with
// plays of his own to show.
const STAR_LOOK = 8;
// The fewest of his own a Player of the Night has.
const STAR_PLAYS_MIN = 2;

const starOf = async (
	recapGames: RecapGame[],
	plays: Map<number, RecapPlay[]>,
	season: number,
): Promise<RecapStar | undefined> => {
	const ranked: {
		game: RecapGame;
		i: 0 | 1;
		p: RecapGame["teams"][0]["players"][number];
		score: number;
	}[] = [];
	for (const game of recapGames) {
		if (game.allStar || !plays.has(game.gid)) {
			continue;
		}
		for (const i of [0, 1] as const) {
			for (const p of game.teams[i].players) {
				if (p.min > 0) {
					ranked.push({ game, i, p, score: gameScore(p) });
				}
			}
		}
	}
	ranked.sort((a, b) => b.score - a.score);
	for (const { game, i, p } of ranked.slice(0, STAR_LOOK)) {
		if (starPlayCount(plays.get(game.gid)!, p.pid) < STAR_PLAYS_MIN) {
			continue;
		}
		const t = game.teams[i];
		const opp = game.teams[i === 0 ? 1 : 0];
		const info = await getTeamInfoBySeason(t.tid, season);
		return {
			gid: game.gid,
			pid: p.pid,
			name: p.name,
			tid: t.tid,
			abbrev: info?.abbrev ?? t.abbrev,
			colors: info?.colors ?? ["#333", "#999", "#fff"],
			...((info as any)?.imgURL ? { imgURL: (info as any).imgURL } : {}),
			line: starLine(p, opp.abbrev),
		};
	}
};

// The standings through the night, as the League Hub has them: each
// conference's in order, split where this league's playoffs split them.
const standingsOf = async (
	season: number,
	day: number,
	date: string | undefined,
	hidden: ReadonlySet<number> | undefined,
): Promise<RecapStandings | undefined> => {
	const allGames = (await idb.merged.games.getAll({ season })).filter(
		(game) => !hidden?.has(game.gid),
	);
	const st = await computeStandingsAsOf(season, day, allGames);
	const spots = st.playoffSpots ?? 0;
	const playIn = g.get("playIn", season) && spots >= 3;
	const direct = playIn ? spots - 2 : spots;
	const looks = new Map<number, RecapStandingsTeam>();
	const confs: RecapStandingsConf[] = [];
	for (const conf of st.confs) {
		const teams: RecapStandingsTeam[] = [];
		for (const t of conf.teams) {
			if (t.tid === undefined) {
				continue;
			}
			let row = looks.get(t.tid);
			if (!row) {
				const info = await getTeamInfoBySeason(t.tid, season);
				row = {
					tid: t.tid,
					abbrev: info?.abbrev ?? t.abbrev,
					colors: info?.colors ?? ["#333", "#999", "#fff"],
					...((info as any)?.imgURL ? { imgURL: (info as any).imgURL } : {}),
					rank: 0,
					won: 0,
					lost: 0,
				};
				looks.set(t.tid, row);
			}
			teams.push({ ...row, rank: t.rank, won: t.won, lost: t.lost });
		}
		if (teams.length === 0) {
			continue;
		}
		const groups: RecapStandingsConf["groups"] = [
			{ label: "PLAYOFFS" as const, teams: teams.slice(0, direct) },
			...(playIn
				? [{ label: "PLAY-IN" as const, teams: teams.slice(direct, spots + 2) }]
				: []),
			{ teams: teams.slice(playIn ? spots + 2 : spots) },
		].filter((group) => group.teams.length > 0);
		confs.push({ name: conf.name, groups });
	}
	return confs.length > 0
		? { through: throughOf(date, day), confs }
		: undefined;
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
		return { when, games: [], top: [] };
	}
	// (Not a game still being played live: no highlights till its final.)
	const hidden =
		season === g.get("season") ? (await getLiveGate()).hidden : undefined;
	const reels = new Map(
		(await idb.cache.recapReels.getAll())
			.filter(
				(row) =>
					row.season === season && row.day === day && !hidden?.has(row.gid),
			)
			.map((row) => [row.gid, row.reel.plays]),
	);
	if (reels.size === 0) {
		return { when, games: [], top: [] };
	}

	// (The night's biggest first, as its written recap has them.)
	const weight = new Map<number, number>();
	let recapGames: RecapGame[] = [];
	try {
		recapGames = await recapGamesForDay({ season, day });
		for (const game of recapGames) {
			weight.set(game.gid, notability(game));
		}
	} catch (error) {
		console.error("Failed to order the night's recap", error);
	}

	const out: RecapNightGame[] = [];
	let date: string | undefined;
	for (const [gid, reelPlays] of reels) {
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
			numPlays: reelPlays.length,
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

	const shownGames = new Map(
		out.map((game) => [game.gid, reels.get(game.gid)!]),
	);
	let star: RecapStar | undefined;
	let standings: RecapStandings | undefined;
	try {
		star = await starOf(recapGames, shownGames, season);
		if (!out.some((game) => game.playoffs)) {
			standings = await standingsOf(season, day, date, hidden);
		}
	} catch (error) {
		console.error("Failed to make the night's recap segments", error);
	}
	const top = pickTopPlays(
		[...shownGames].map(([gid, plays]) => ({ gid, plays })),
		TOP_PLAYS_MOST,
	).map(({ gid, play }) => ({ gid, i: play.i }));

	return {
		when: date === undefined ? when : formatGameDate(date, "long"),
		games: out,
		...(star ? { star } : {}),
		top,
		...(standings ? { standings } : {}),
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
