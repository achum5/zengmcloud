// WHO THESE MEN ARE, AND WHAT THE NIGHT SETTLED.
//
// The wire story knows a man's career, not just his season: that 34 was a
// career high, that it was his first triple-double, that he was the No. 3
// pick, that this was his first game since the trade - and against the team
// that made it. It knows a club is on the fourth stop of a five-game trip,
// and that tonight it clinched, or was eliminated, or became the first team
// in the league to 20 wins.
//
// Pure functions over plain rows, like recapContext.ts, so a test can hand
// build a career or a standings table in a few lines. Every fact is AS OF the
// game: a recap read a month later says what was true that night.

import type {
	ContextGameRow,
	ContextScheduleRow,
	CountingTotals,
} from "./recapContext.ts";

// ---------------------------------------------------------------- HIS CAREER

// A player's regular-season career BEFORE this game.
export type PlayerPast = {
	gp: number;
	// His best single game in each column.
	high: CountingTotals;
	dd: number;
	td: number;
};

// A game a man played this season, as the recap builder keeps it.
export type CareerLine = {
	day: number;
	gid: number;
	playoffs: boolean;
	tid?: number;
	oppTid?: number;
	row: Record<string, number>;
};

const MAX_KEYS: Record<keyof CountingTotals, string> = {
	pts: "ptsMax",
	reb: "trbMax",
	ast: "astMax",
	tp: "tpMax",
	stl: "stlMax",
	blk: "blkMax",
};

const totalsOfLine = (row: Record<string, number>): CountingTotals => ({
	pts: row.pts ?? 0,
	reb: (row.orb ?? 0) + (row.drb ?? 0) + (row.reb ?? 0),
	ast: row.ast ?? 0,
	tp: row.tp ?? 0,
	stl: row.stl ?? 0,
	blk: row.blk ?? 0,
});

// Columns in double figures, of the five a double-double counts.
export const doublesOf = (t: CountingTotals) =>
	[t.pts, t.reb, t.ast, t.stl, t.blk].filter((v) => v >= 10).length;

const before = (l: { day: number; gid: number }, gid: number, day: number) =>
	l.day < day || (l.day === day && l.gid < gid);

// His career before this game, or nothing when the league does not hold it
// whole. A man who came into a new league as a ten-year veteran has no
// seasons on file, and "a career-high 25" from him would be false; so would
// one off a season imported without its single-game bests. The career counts
// only when it starts inside the league's history (drafted the summer before
// its first season or later) or every season of it is on file, each with its
// bests and its double-doubles.
// Whether a man's whole career is on file: it started inside the league's
// history, or its first season is here. A veteran who came into a new league
// has none of his years before it, and "went past 1,000 career points" from
// him would be false.
export const careerOnFile = (
	player: { stats?: any[]; draft?: { year?: number } },
	season: number,
	startingSeason: number,
): boolean => {
	const draftYear = player.draft?.year;
	if (typeof draftYear !== "number") {
		return false;
	}
	if (draftYear >= startingSeason - 1) {
		return true;
	}
	const rows = (player.stats ?? []).filter(
		(r) => r && !r.playoffs && r.season < season && (r.gp ?? 0) > 0,
	);
	return (
		rows.length > 0 &&
		Math.min(...rows.map((r) => r.season as number)) <= draftYear + 1
	);
};

export const playerPast = (
	player: {
		stats?: any[];
		draft?: { year?: number };
	},
	season: number,
	startingSeason: number,
	lines: readonly CareerLine[],
	gid: number,
	day: number,
): PlayerPast | undefined => {
	const rows = (player.stats ?? []).filter(
		(r) => r && !r.playoffs && r.season < season && (r.gp ?? 0) > 0,
	);
	const draftYear = player.draft?.year;
	if (typeof draftYear !== "number") {
		return undefined;
	}
	const inLeague = draftYear >= startingSeason - 1;
	const firstOnFile = Math.min(...rows.map((r) => r.season as number));
	if (!inLeague && !(rows.length > 0 && firstOnFile <= draftYear + 1)) {
		return undefined;
	}
	const high: CountingTotals = {
		pts: 0,
		reb: 0,
		ast: 0,
		tp: 0,
		stl: 0,
		blk: 0,
	};
	let gp = 0;
	let dd = 0;
	let td = 0;
	for (const r of rows) {
		if (typeof r.dd !== "number" || typeof r.td !== "number") {
			return undefined;
		}
		for (const key of Object.keys(MAX_KEYS) as (keyof CountingTotals)[]) {
			const max = r[MAX_KEYS[key]];
			if (!Array.isArray(max) || typeof max[0] !== "number") {
				return undefined;
			}
			high[key] = Math.max(high[key], max[0]);
		}
		gp += r.gp;
		dd += r.dd;
		td += r.td;
	}
	for (const l of lines) {
		if (l.playoffs || !before(l, gid, day)) {
			continue;
		}
		const t = totalsOfLine(l.row);
		for (const key of Object.keys(high) as (keyof CountingTotals)[]) {
			high[key] = Math.max(high[key], t[key]);
		}
		const doubles = doublesOf(t);
		gp += 1;
		dd += doubles >= 2 ? 1 : 0;
		td += doubles >= 3 ? 1 : 0;
	}
	return { gp, high, dd, td };
};

// Where he came into the league, for his first two seasons: "the No. 3
// pick". Round 0 is a man nobody drafted.
export type DraftSlot = { year: number; round: number; pick: number };

export const draftSlotOf = (
	player: { draft?: { year?: number; round?: number; pick?: number } },
	season: number,
): DraftSlot | undefined => {
	const d = player.draft;
	if (
		!d ||
		typeof d.year !== "number" ||
		typeof d.round !== "number" ||
		typeof d.pick !== "number" ||
		d.year < season - 2 ||
		d.year >= season
	) {
		return undefined;
	}
	return { year: d.year, round: d.round, pick: d.pick };
};

// ---------------------------------------------------------------- HIS MOVES

export type TeamMove = {
	// The club he played for last.
	fromTid: number;
	// How he got here, when the record says: in a trade, or signed.
	via?: "trade" | "signing";
	// He played for the other club this season; otherwise it was last season.
	midSeason: boolean;
};

const viaOf = (
	player: { transactions?: any[] },
	tid: number,
	fromTid: number,
	season: number,
): TeamMove["via"] => {
	const moves = (player.transactions ?? []).filter(
		(t) => t && t.tid === tid && t.season >= season - 1,
	);
	const last = moves.at(-1);
	if (!last) {
		return undefined;
	}
	if (last.type === "trade") {
		return last.fromTid === fromTid ? "trade" : undefined;
	}
	return last.type === "freeAgent" ? "signing" : undefined;
};

// The club he played for before this one, when this is his first game for
// this one: this season's earlier games first, then last season's last row.
const previousTeam = (
	player: { stats?: any[] },
	tid: number,
	season: number,
	prior: readonly CareerLine[],
): { tid: number; midSeason: boolean } | undefined => {
	const last = prior.at(-1);
	if (last) {
		return typeof last.tid === "number" && last.tid !== tid
			? { tid: last.tid, midSeason: true }
			: undefined;
	}
	const rows = (player.stats ?? []).filter(
		(r) => r && !r.playoffs && r.season === season - 1 && (r.gp ?? 0) > 0,
	);
	const row = rows.at(-1);
	return row && typeof row.tid === "number" && row.tid >= 0 && row.tid !== tid
		? { tid: row.tid, midSeason: false }
		: undefined;
};

// His first game for this club, having played for another before it.
export const newTeamOf = (
	player: { stats?: any[]; transactions?: any[] },
	tid: number,
	season: number,
	lines: readonly CareerLine[],
	gid: number,
	day: number,
): TeamMove | undefined => {
	const prior = lines
		.filter((l) => !l.playoffs && before(l, gid, day))
		.sort((a, b) => a.day - b.day || a.gid - b.gid);
	if (prior.some((l) => l.tid === tid)) {
		return undefined;
	}
	const prev = previousTeam(player, tid, season, prior);
	if (!prev) {
		return undefined;
	}
	const via = viaOf(player, tid, prev.tid, season);
	return {
		fromTid: prev.tid,
		midSeason: prev.midSeason,
		...(via ? { via } : {}),
	};
};

// His first game against the club he left, since leaving it: he played for
// it this season or last, and has not faced it since.
export const formerTeamOf = (
	player: { stats?: any[]; transactions?: any[] },
	tid: number,
	oppTid: number,
	season: number,
	lines: readonly CareerLine[],
	gid: number,
	day: number,
): TeamMove | undefined => {
	const prior = lines
		.filter((l) => !l.playoffs && before(l, gid, day))
		.sort((a, b) => a.day - b.day || a.gid - b.gid);
	if (prior.some((l) => l.tid === tid && l.oppTid === oppTid)) {
		return undefined;
	}
	const playedFor = prior.some((l) => l.tid === oppTid);
	const lastSeason = (player.stats ?? []).filter(
		(r) => r && !r.playoffs && r.season === season - 1 && (r.gp ?? 0) > 0,
	);
	const leftLastSeason =
		!playedFor &&
		lastSeason.at(-1)?.tid === oppTid &&
		!prior.some((l) => l.tid === oppTid);
	if (!playedFor && !leftLastSeason) {
		return undefined;
	}
	// Still with them this season, then a stop somewhere else, then here:
	// "the team that traded him" is only so when the trade was from them.
	const via = viaOf(player, tid, oppTid, season);
	return {
		fromTid: oppTid,
		midSeason: playedFor,
		...(via ? { via } : {}),
	};
};

// ---------------------------------------------------------------- THE TRIP

// The run of road games (or home games) this one is part of: which game of
// it this was, how long it is, and the record on it through tonight.
export type Stretch = {
	home: boolean;
	game: number;
	games: number;
	won: number;
	lost: number;
};

type Stop = { gid: number; day: number; home?: boolean; won?: boolean };

export const stretchOf = (
	tid: number,
	gid: number,
	games: readonly (ContextGameRow & { neutralSite?: boolean })[],
	// Games not yet played. Leave out once the regular season is over.
	schedule: readonly ContextScheduleRow[] = [],
): Stretch | undefined => {
	const stops: Stop[] = [];
	for (const g of games) {
		if (
			g.playoffs ||
			!g.won ||
			!g.lost ||
			(g.teams[0]?.tid !== tid && g.teams[1]?.tid !== tid)
		) {
			continue;
		}
		stops.push({
			gid: g.gid,
			day: g.day ?? 0,
			home: g.neutralSite ? undefined : g.teams[0]?.tid === tid,
			won: g.won.tid === tid,
		});
	}
	for (const s of schedule) {
		if (s.homeTid === tid || s.awayTid === tid) {
			stops.push({ gid: s.gid, day: s.day, home: s.homeTid === tid });
		}
	}
	stops.sort((a, b) => a.day - b.day || a.gid - b.gid);
	const at = stops.findIndex((s) => s.gid === gid);
	const home = stops[at]?.home;
	if (at < 0 || home === undefined) {
		return undefined;
	}
	let first = at;
	while (first > 0 && stops[first - 1]!.home === home) {
		first -= 1;
	}
	let last = at;
	while (last < stops.length - 1 && stops[last + 1]!.home === home) {
		last += 1;
	}
	let won = 0;
	let lost = 0;
	for (let i = first; i <= at; i++) {
		if (stops[i]!.won) {
			won += 1;
		} else {
			lost += 1;
		}
	}
	return { home, game: at - first + 1, games: last - first + 1, won, lost };
};

// ---------------------------------------------------------------- THE RACE

export type RaceRow = { tid: number; won: number; lost: number };

// What a night settled for a club in the standings.
export type StandingsNews = {
	// The best record in its group (its conference when the playoffs are by
	// conference), a playoff spot, or at least a play-in spot.
	clinched?: "top" | "playoffs" | "playIn";
	// No longer able to reach even the play-in (or the playoffs, without one).
	eliminated?: true;
	// The first team in the league to this many wins.
	firstTo?: number;
	// The first team in the league to clinch a playoff spot.
	firstToClinch?: true;
	// Its conference, when the playoffs are seeded by conference.
	conf?: string;
};

export type RaceRules = {
	numGames: number;
	// Spots straight into the playoffs, and the last spot that still plays
	// on (the play-in's), per group.
	direct: number;
	reach: number;
};

const left = (r: RaceRow, numGames: number) =>
	Math.max(0, numGames - r.won - r.lost);

// Clinched a top-k place whatever happens: fewer than k others can still
// finish level with it or ahead. (Level counts against it - a tiebreaker it
// might lose is no clinch.)
export const clinchedTop = (
	group: readonly RaceRow[],
	tid: number,
	k: number,
	numGames: number,
): boolean => {
	const me = group.find((r) => r.tid === tid);
	if (!me || k <= 0) {
		return false;
	}
	const threats = group.filter(
		(r) => r.tid !== tid && r.won + left(r, numGames) >= me.won,
	).length;
	return threats < k;
};

// Out of the top k whatever happens: k others already have more wins than
// it can reach.
export const eliminatedFrom = (
	group: readonly RaceRow[],
	tid: number,
	k: number,
	numGames: number,
): boolean => {
	const me = group.find((r) => r.tid === tid);
	if (!me) {
		return false;
	}
	const most = me.won + left(me, numGames);
	return group.filter((r) => r.tid !== tid && r.won > most).length >= k;
};

const FIRST_TO = [10, 20, 30, 40, 50, 60, 70];

export const standingsNewsOf = (
	tid: number,
	// The groups seeded separately (conferences, or the whole league), the
	// night before and the night after.
	before: readonly (readonly RaceRow[])[],
	after: readonly (readonly RaceRow[])[],
	rules: RaceRules,
): StandingsNews | undefined => {
	const groupOf = (groups: readonly (readonly RaceRow[])[]) =>
		groups.find((g) => g.some((r) => r.tid === tid));
	const was = groupOf(before);
	const now = groupOf(after);
	if (!was || !now) {
		return undefined;
	}
	const { numGames, direct, reach } = rules;
	const out: StandingsNews = {};
	const newly = (k: number) =>
		k > 0 &&
		clinchedTop(now, tid, k, numGames) &&
		!clinchedTop(was, tid, k, numGames);
	if (newly(1) && now.length > 1) {
		out.clinched = "top";
	} else if (newly(direct)) {
		out.clinched = "playoffs";
	} else if (reach > direct && newly(reach)) {
		out.clinched = "playIn";
	}
	if (
		eliminatedFrom(now, tid, reach, numGames) &&
		!eliminatedFrom(was, tid, reach, numGames)
	) {
		out.eliminated = true;
	}
	const me = now.find((r) => r.tid === tid)!;
	const everyoneBefore = before.flat();
	const everyoneAfter = after.flat();
	for (const n of FIRST_TO) {
		if (
			me.won === n &&
			everyoneBefore.every((r) => r.won < n) &&
			everyoneAfter.filter((r) => r.won >= n).length === 1
		) {
			out.firstTo = n;
		}
	}
	if (out.clinched === "top" || out.clinched === "playoffs") {
		const clinchedIn = (groups: readonly (readonly RaceRow[])[]) =>
			groups.flatMap((g) =>
				g.filter((r) => clinchedTop(g, r.tid, direct, numGames)),
			);
		if (clinchedIn(before).length === 0 && clinchedIn(after).length === 1) {
			out.firstToClinch = true;
		}
	}
	return Object.keys(out).length > 0 ? out : undefined;
};
