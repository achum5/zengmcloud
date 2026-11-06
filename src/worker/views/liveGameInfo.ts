// WHAT THE GAMECAST SAYS ABOUT A GAME BEYOND THE GAME ITSELF: this season's
// meetings between the two teams, and their division standings - both as
// they stood at the tip.
//
// A live game is watched after it has been played: its result is already in
// the games store, the two teams' records and their streaks. None of that
// can show, or the standings would give the ending away. So this game, and
// any meeting after it, is listed without a score, and the two teams'
// records and streaks are taken back to before it.

import { idb } from "../db/index.ts";
import { g, helpers } from "../util/index.ts";
import type { Game } from "../../common/types.ts";

export type SeriesGame = {
	gid: number;
	day?: number;
	// Box score order, as the game itself: 0 home, 1 away.
	tids: [number, number];
	// The final score, home first - only for a meeting before this one.
	pts?: [number, number];
	// The game being watched.
	now?: boolean;
};

export type StandingsTeam = {
	tid: number;
	// Where it stands in its conference.
	rank: number;
	abbrev: string;
	region: string;
	name: string;
	imgURL?: string;
	imgURLSmall?: string;
	won: number;
	lost: number;
	tied: number;
	otl: number;
	gb: number;
	// Games won (+) or lost (-) in a row.
	streak: number;
};

// One of a team's games: who against, home or away, the score (theirs
// first) - and whether it is the game on the page.
export type RecentGame = {
	gid: number;
	day?: number;
	opp: number;
	oppAbbrev?: string;
	home: boolean;
	pts: [number, number];
	now?: boolean;
};

export type GamecastInfo = {
	series: SeriesGame[];
	// The two teams' conferences (one, when they share it), as they stood.
	standings: { name: string; teams: StandingsTeam[] }[];
	// Each team's last few games, latest first.
	recent: { tid: number; games: RecentGame[] }[];
};

type Played = Pick<Game, "gid" | "day" | "playoffs" | "teams">;
type Scheduled = {
	gid: number;
	day?: number;
	homeTid: number;
	awayTid: number;
};

// Before or after the game being watched: by day, then by gid (games are
// numbered as they're scheduled).
const before = (
	a: { day?: number; gid: number },
	b: { day?: number; gid: number },
) =>
	a.day !== undefined && b.day !== undefined && a.day !== b.day
		? a.day < b.day
		: a.gid < b.gid;

export const seriesOf = (
	game: Played,
	played: readonly Played[],
	scheduled: readonly Scheduled[],
	// A finished game's page (the box score): every meeting played so far
	// with its score, this one too - nothing to hold back.
	final = false,
): SeriesGame[] => {
	const tids = [game.teams[0].tid, game.teams[1].tid];
	const between = (a: number, b: number) =>
		(a === tids[0] && b === tids[1]) || (a === tids[1] && b === tids[0]);
	const out: SeriesGame[] = [];
	for (const p of played) {
		if (
			!!p.playoffs !== !!game.playoffs ||
			!between(p.teams[0].tid, p.teams[1].tid)
		) {
			continue;
		}
		const now = p.gid === game.gid;
		out.push({
			gid: p.gid,
			day: p.day,
			tids: [p.teams[0].tid, p.teams[1].tid],
			...(now ? { now: true } : {}),
			...((now && final) || (!now && (final || before(p, game)))
				? { pts: [p.teams[0].pts, p.teams[1].pts] as [number, number] }
				: {}),
		});
	}
	if (!game.playoffs) {
		for (const s of scheduled) {
			if (between(s.homeTid, s.awayTid) && !out.some((x) => x.gid === s.gid)) {
				out.push({ gid: s.gid, day: s.day, tids: [s.homeTid, s.awayTid] });
			}
		}
	}
	// (The game being watched is in the games store already; a follower's
	// device may not have it yet.)
	if (!out.some((x) => x.now)) {
		out.push({
			gid: game.gid,
			day: game.day,
			tids: [game.teams[0].tid, game.teams[1].tid],
			now: true,
		});
	}
	return out.sort((a, b) => (before(a, b) ? -1 : before(b, a) ? 1 : 0));
};

// A team's streak going into a game: its results before it, latest first.
export const streakBefore = (
	tid: number,
	game: Played,
	played: readonly Played[],
): number => {
	const results = played
		.filter(
			(p) =>
				!p.playoffs &&
				before(p, game) &&
				(p.teams[0].tid === tid || p.teams[1].tid === tid),
		)
		.sort((a, b) => (before(a, b) ? 1 : -1))
		.map((p) => {
			const us = p.teams[0].tid === tid ? 0 : 1;
			const diff = p.teams[us].pts - p.teams[us === 0 ? 1 : 0].pts;
			return Math.sign(diff);
		});
	let streak = 0;
	for (const r of results) {
		if (r === 0 || (streak !== 0 && Math.sign(streak) !== r)) {
			break;
		}
		streak += r;
	}
	return streak;
};

// The standings as they stood: as of the tip (the live game - the day's
// games not yet played) or that night (its box score - the day's games
// all in), worked out from the season's games, so an old game's page shows
// the table as it was then, not as it is now. Ties counted half.
export const standingsAsOf = (
	game: Played,
	played: readonly Played[],
	final: boolean,
): Map<number, { won: number; lost: number; tied: number; streak: number }> => {
	const sameDay = (p: Played) =>
		p.day !== undefined && game.day !== undefined && p.day === game.day;
	const counts = (p: Played) =>
		!p.playoffs &&
		(game.playoffs
			? true
			: final
				? p.gid === game.gid || before(p, game) || sameDay(p)
				: before(p, game) && !sameDay(p));
	const games = played
		.filter(counts)
		.sort((a, b) => (before(a, b) ? -1 : before(b, a) ? 1 : 0));
	const out = new Map<
		number,
		{ won: number; lost: number; tied: number; streak: number }
	>();
	const row = (tid: number) => {
		let r = out.get(tid);
		if (!r) {
			r = { won: 0, lost: 0, tied: 0, streak: 0 };
			out.set(tid, r);
		}
		return r;
	};
	for (const p of games) {
		for (const i of [0, 1] as const) {
			const r = row(p.teams[i].tid);
			const diff = p.teams[i].pts - p.teams[i === 0 ? 1 : 0].pts;
			if (diff > 0) {
				r.won += 1;
				r.streak = r.streak > 0 ? r.streak + 1 : 1;
			} else if (diff < 0) {
				r.lost += 1;
				r.streak = r.streak < 0 ? r.streak - 1 : -1;
			} else {
				r.tied += 1;
				r.streak = 0;
			}
		}
	}
	return out;
};

// A team's last few games, latest first: up to and including this one on
// its box score, before it at the tip.
export const recentOf = (
	tid: number,
	game: Played,
	played: readonly Played[],
	final: boolean,
	n = 5,
): RecentGame[] =>
	played
		.filter(
			(p) =>
				(p.teams[0].tid === tid || p.teams[1].tid === tid) &&
				(before(p, game) || (final && p.gid === game.gid)),
		)
		.sort((a, b) => (before(a, b) ? 1 : before(b, a) ? -1 : 0))
		.slice(0, n)
		.map((p) => {
			const us = p.teams[0].tid === tid ? 0 : 1;
			const them = us === 0 ? 1 : 0;
			return {
				gid: p.gid,
				day: p.day,
				opp: p.teams[them].tid,
				home: us === 0,
				pts: [p.teams[us].pts, p.teams[them].pts] as [number, number],
				...(p.gid === game.gid ? { now: true } : {}),
			};
		});

export const gamecastInfo = async (
	game: Game,
	// The box score's: the meetings with their scores, and the standings as
	// they stood that night - not as they stood at the tip.
	final = false,
): Promise<GamecastInfo | undefined> => {
	const tids = [game.teams[0].tid, game.teams[1].tid] as const;
	if (tids[0] < 0 || tids[1] < 0 || (game as any).exhibition) {
		return undefined;
	}
	const season = game.season;
	const played = (await idb.merged.games.getAll({ season })) as Played[];
	const scheduled =
		season === g.get("season") ? await idb.cache.schedule.getAll() : [];
	const series = seriesOf(game, played, scheduled, final);

	const teams = await idb.cache.teams.getAll();
	const teamSeasons = await idb.merged.teamSeasons.getAll({ season });
	const confs = g.get("confs", season);
	const records = standingsAsOf(game, played, final);
	// The two teams' conference(s), home's first - once, if they share it.
	const cids: number[] = [];
	for (const tid of tids) {
		const cid = teamSeasons.find((ts) => ts.tid === tid)?.cid;
		if (cid !== undefined && !cids.includes(cid)) {
			cids.push(cid);
		}
	}
	const standings: GamecastInfo["standings"] = [];
	for (const cid of cids) {
		const rows: StandingsTeam[] = [];
		for (const ts of teamSeasons) {
			if (ts.cid !== cid) {
				continue;
			}
			const t = teams.find((t2) => t2.tid === ts.tid);
			const r = records.get(ts.tid) ?? { won: 0, lost: 0, tied: 0, streak: 0 };
			rows.push({
				tid: ts.tid,
				rank: 0,
				abbrev: ts.abbrev ?? t?.abbrev ?? "",
				region: ts.region ?? t?.region ?? "",
				name: ts.name ?? t?.name ?? "",
				imgURL: ts.imgURL ?? t?.imgURL,
				imgURLSmall: ts.imgURLSmall ?? t?.imgURLSmall,
				won: r.won,
				lost: r.lost,
				tied: r.tied,
				otl: 0,
				gb: 0,
				streak: r.streak,
			});
		}
		const pct = (r: StandingsTeam) => {
			const gp = r.won + r.lost + r.tied;
			return gp > 0 ? (r.won + 0.5 * r.tied) / gp : 0;
		};
		rows.sort((a, b) => pct(b) - pct(a) || b.won - a.won || a.lost - b.lost);
		rows.forEach((r, k) => {
			r.rank = k + 1;
			r.gb = helpers.gb(rows[0]!, r);
		});
		standings.push({
			name: confs.find((c) => c.cid === cid)?.name ?? "Conference",
			teams: rows,
		});
	}

	const abbrevOf = (tid: number) =>
		teamSeasons.find((ts) => ts.tid === tid)?.abbrev ??
		teams.find((t) => t.tid === tid)?.abbrev;
	const recent = tids.map((tid) => ({
		tid,
		games: recentOf(tid, game, played, final).map((gm) => ({
			...gm,
			oppAbbrev: abbrevOf(gm.opp),
		})),
	}));

	return { series, standings, recent };
};
