import { formatPlayerAwardName } from "../../../common/awards.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import type { Player } from "../../../common/types.ts";

// The shared data layer for the three trivia games (Grids, Team Trivia,
// Higher or Lower), ported from ZenGM Grids (github.com/achum5/ZenGMGrids).
// The original app parsed an uploaded league export into exactly this shape;
// here it's derived straight from the live league DB in one pass over every
// player who ever appeared in the league, then cached until the season/phase
// changes (career data only moves when games are played).

// One regular-season stint: a (season, tid) stat row. A player traded
// mid-season has multiple rows for the same season; per-season achievement
// checks merge them, team attachment uses them individually.
export type TriviaSeasonRow = {
	season: number;
	tid: number;
	gp: number;
	min: number;
	pts: number;
	trb: number;
	ast: number;
	stl: number;
	blk: number;
	tp: number;
	tpa: number;
	fg: number;
	fga: number;
	ft: number;
	fta: number;
	pos: string;
	jerseyNumber: string | undefined;
};

export type TriviaPlayer = {
	pid: number;
	name: string;
	firstSeason: number;
	lastSeason: number;
	bornYear: number;
	hof: boolean;
	draft: { round: number; pick: number; year: number };
	awards: { season: number; type: string }[];
	// Unique tids the player logged a regular-season game for (or is currently
	// rostered on) - the "played for team X" predicate.
	teamsPlayed: number[];
	rows: TriviaSeasonRow[];
	// Career regular-season totals.
	tot: {
		gp: number;
		min: number;
		pts: number;
		trb: number;
		ast: number;
		stl: number;
		blk: number;
		tp: number;
		tpa: number;
		fg: number;
		fga: number;
		ft: number;
		fta: number;
		seasons: number; // distinct seasons with gp > 0
	};
	// Single-game career highs.
	gameHigh: { pts: number; trb: number; ast: number };
	// How famous/common a player is, for rarity scoring: guessing an obscure
	// qualifier is worth more than the obvious star everyone thinks of first.
	popularity: number;
};

export type TriviaPool = {
	players: TriviaPlayer[];
	byPid: Map<number, TriviaPlayer>;
	minSeason: number;
	maxSeason: number;
};

const num = (x: unknown): number => (typeof x === "number" ? x : 0);

// *Max career-high stats are stored as [value, gid] tuples.
const maxVal = (x: unknown): number =>
	Array.isArray(x) && typeof x[0] === "number" ? x[0] : 0;

const buildPlayer = (
	p: Player,
	currentSeason: number,
): TriviaPlayer | undefined => {
	const rows: TriviaSeasonRow[] = [];
	const teamsPlayed = new Set<number>();
	const seasonsPlayed = new Set<number>();
	const tot = {
		gp: 0,
		min: 0,
		pts: 0,
		trb: 0,
		ast: 0,
		stl: 0,
		blk: 0,
		tp: 0,
		tpa: 0,
		fg: 0,
		fga: 0,
		ft: 0,
		fta: 0,
		seasons: 0,
	};
	const gameHigh = { pts: 0, trb: 0, ast: 0 };

	// Position by season, from the ratings history.
	const posBySeason = new Map<number, string>();
	for (const r of p.ratings) {
		posBySeason.set((r as any).season, (r as any).pos ?? "");
	}

	let firstSeason = Infinity;
	let lastSeason = -Infinity;

	for (const ps of p.stats) {
		if (ps.playoffs || ps.tid < 0) {
			continue;
		}
		const gp = num(ps.gp);
		// Rebounds: live BBGM stores orb/drb, no trb.
		const trb = num(ps.trb) || num(ps.orb) + num(ps.drb);
		const row: TriviaSeasonRow = {
			season: ps.season,
			tid: ps.tid,
			gp,
			min: num(ps.min),
			pts: num(ps.pts),
			trb,
			ast: num(ps.ast),
			stl: num(ps.stl),
			blk: num(ps.blk),
			tp: num(ps.tp) || num((ps as any).tpm),
			tpa: num(ps.tpa),
			fg: num(ps.fg),
			fga: num(ps.fga),
			ft: num(ps.ft),
			fta: num(ps.fta),
			pos: posBySeason.get(ps.season) ?? p.ratings.at(-1)!.pos,
			jerseyNumber: (ps as any).jerseyNumber,
		};
		rows.push(row);

		if (gp > 0 || ps.season === currentSeason) {
			teamsPlayed.add(ps.tid);
		}
		if (gp > 0) {
			seasonsPlayed.add(ps.season);
			firstSeason = Math.min(firstSeason, ps.season);
			lastSeason = Math.max(lastSeason, ps.season);
		}

		tot.gp += gp;
		tot.min += row.min;
		tot.pts += row.pts;
		tot.trb += row.trb;
		tot.ast += row.ast;
		tot.stl += row.stl;
		tot.blk += row.blk;
		tot.tp += row.tp;
		tot.tpa += row.tpa;
		tot.fg += row.fg;
		tot.fga += row.fga;
		tot.ft += row.ft;
		tot.fta += row.fta;

		gameHigh.pts = Math.max(gameHigh.pts, maxVal((ps as any).ptsMax));
		gameHigh.trb = Math.max(gameHigh.trb, maxVal((ps as any).trbMax));
		gameHigh.ast = Math.max(gameHigh.ast, maxVal((ps as any).astMax));
	}

	// Current-season roster counts as "played for" even with 0 games so far.
	if (p.tid >= 0) {
		teamsPlayed.add(p.tid);
	}

	if (teamsPlayed.size === 0) {
		return undefined; // never appeared - not part of any trivia
	}

	tot.seasons = seasonsPlayed.size;

	// Custom awards carry a name and rank rather than a fixed type string;
	// formatPlayerAwardName renders either the way the player page does.
	const awards = p.awards.map((a) => ({
		season: a.season,
		type: formatPlayerAwardName(a),
	}));
	const hof =
		!!p.hof || awards.some((a) => a.type === "Inducted into the Hall of Fame");

	// Fame proxy for rarity scoring, following the original's weighting: honors
	// dominate, log-damped volume breaks ties among role players.
	let honors = 0;
	for (const a of awards) {
		if (a.type === "Most Valuable Player") {
			honors += 4;
		} else if (a.type === "Finals MVP") {
			honors += 3;
		} else if (a.type.includes("All-League")) {
			honors += 2;
		} else if (a.type === "All-Star") {
			honors += 1;
		}
	}
	if (hof) {
		honors += 6;
	}
	const popularity =
		5 * honors +
		8 * Math.log10(1 + tot.min) +
		6 * Math.log10(1 + tot.pts) +
		4 * Math.log10(1 + tot.gp);

	return {
		pid: p.pid!,
		name: `${p.firstName} ${p.lastName}`,
		firstSeason: Number.isFinite(firstSeason) ? firstSeason : currentSeason,
		lastSeason: Number.isFinite(lastSeason) ? lastSeason : currentSeason,
		bornYear: p.born.year,
		hof,
		draft: { round: p.draft.round, pick: p.draft.pick, year: p.draft.year },
		awards,
		teamsPlayed: [...teamsPlayed],
		rows,
		tot,
		gameHigh,
		popularity,
	};
};

// Season+phase keyed cache: career/trivia data only changes as games are
// played, and a stale-by-a-few-days pool is fine for a trivia game - but a
// pool from a different league or season is not.
let cached: { key: string; pool: TriviaPool } | undefined;

export const getTriviaPool = async (): Promise<TriviaPool> => {
	const key = `${g.get("lid")}-${g.get("season")}-${g.get("phase")}`;
	if (cached?.key === key) {
		return cached.pool;
	}

	const currentSeason = g.get("season");
	const playersAll = await idb.getCopies.players(
		{ activeAndRetired: true },
		"noCopyCache",
	);

	const players: TriviaPlayer[] = [];
	for (const p of playersAll) {
		const tp = buildPlayer(p, currentSeason);
		if (tp) {
			players.push(tp);
		}
	}

	let minSeason = Infinity;
	let maxSeason = -Infinity;
	for (const p of players) {
		minSeason = Math.min(minSeason, p.firstSeason);
		maxSeason = Math.max(maxSeason, p.lastSeason);
	}
	if (!Number.isFinite(minSeason)) {
		minSeason = currentSeason;
		maxSeason = currentSeason;
	}

	const pool: TriviaPool = {
		players,
		byPid: new Map(players.map((p) => [p.pid, p])),
		minSeason,
		maxSeason,
	};
	cached = { key, pool };
	return pool;
};

// The autocomplete list every game's guess input searches over. With an
// abbrevs map, each entry also carries position + franchise abbrevs so the
// dropdown can distinguish same-named players at a glance.
export const getSearchList = (
	pool: TriviaPool,
	abbrevs?: Map<number, string>,
) =>
	pool.players.map((p) => ({
		pid: p.pid,
		name: p.name,
		years: `${p.firstSeason}-${p.lastSeason}`,
		pos: p.rows.at(-1)?.pos ?? "",
		// Fame, for hint mode: distractors are picked to be roughly as prominent
		// as the correct answer, so the right one doesn't stand out as "the only
		// name I recognise".
		pop: p.popularity,
		teams: abbrevs
			? p.teamsPlayed
					.map((tid) => abbrevs.get(tid))
					.filter((a): a is string => a !== undefined)
					.join(", ")
			: "",
	}));
