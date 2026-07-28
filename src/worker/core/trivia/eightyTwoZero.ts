import { g } from "../../util/index.ts";
import { getTriviaPool, type TriviaPlayer } from "./pool.ts";

// 82-0: build a five-man all-time lineup out of YOUR league's history and see
// how many games it wins.
//
// A round works like a slot machine. It rolls a franchise and an era, and then
// you take the best player you can find who suited up for that franchise in
// that era AND fits the position the round is asking for. Five rounds, one per
// position, and then the lineup plays a full season (see eightyTwoZeroSim.ts).
//
// The constraint is the whole game: the fun is being handed the 1990s of a
// terrible expansion franchise in the round where you still need a center. So
// the matchup list is built to only ever roll a combination that HAS someone
// eligible - being handed an impossible round isn't a hard choice, it's a dead
// end.

export const EIGHTY_TWO_ZERO_POSITIONS = ["PG", "SG", "SF", "PF", "C"] as const;
export type EightyTwoZeroPosition = (typeof EIGHTY_TWO_ZERO_POSITIONS)[number];

// Which listed positions can fill each slot. BBGM lists hybrids (G, GF, F, FC)
// and those players really can play either side of the hybrid, so they qualify
// for both - otherwise a league whose big men are mostly listed FC would have
// rounds with nobody eligible at center.
const ELIGIBLE_LISTED: Record<EightyTwoZeroPosition, ReadonlySet<string>> = {
	PG: new Set(["PG", "G"]),
	SG: new Set(["SG", "G", "GF"]),
	SF: new Set(["SF", "GF", "F"]),
	PF: new Set(["PF", "F", "FC"]),
	C: new Set(["C", "FC"]),
};

// A season only counts toward a franchise-era if the player actually played in
// it. One game in a uniform is enough to be "from" that team, but a player who
// was on the roster and never dressed is not someone anyone would draft.
const MIN_GAMES = 1;

export type EightyTwoZeroEra = {
	// Stable id, also the first season of the bucket.
	start: number;
	end: number;
	label: string;
};

// Eras are decades where the league has enough history for decades to mean
// anything, and single seasons where it doesn't - a five-year-old league
// bucketed into one decade would roll the same matchup every round.
const MIN_SEASONS_FOR_DECADES = 12;

export const buildEras = (
	minSeason: number,
	maxSeason: number,
): EightyTwoZeroEra[] => {
	const span = maxSeason - minSeason + 1;
	if (span < MIN_SEASONS_FOR_DECADES) {
		const out: EightyTwoZeroEra[] = [];
		for (let season = minSeason; season <= maxSeason; season++) {
			out.push({ start: season, end: season, label: String(season) });
		}
		return out;
	}

	const out: EightyTwoZeroEra[] = [];
	const firstDecade = Math.floor(minSeason / 10) * 10;
	for (let start = firstDecade; start <= maxSeason; start += 10) {
		out.push({
			start,
			end: start + 9,
			label: `${start}s`,
		});
	}
	return out;
};

export const inEra = (season: number, era: EightyTwoZeroEra) =>
	season >= era.start && season <= era.end;

// One draftable option: a player, and the single season of his that the round
// is really offering. A player who spent a decade with a franchise is drafted
// as his BEST year there, which is what "the 1990s Bulls" means to anyone
// picking.
export type EightyTwoZeroOption = {
	pid: number;
	name: string;
	season: number;
	pos: string;
	gp: number;
	min: number;
	pts: number;
	trb: number;
	ast: number;
	stl: number;
	blk: number;
	tp: number;
	// Only for ordering the list; never shown.
	score: number;
};

// How good a season was, for ranking the options a round offers. Deliberately
// crude and box-score-only: it decides list order, and the player decides the
// pick.
const seasonScore = (row: {
	gp: number;
	min: number;
	pts: number;
	trb: number;
	ast: number;
	stl: number;
	blk: number;
}) => {
	if (row.gp <= 0) {
		return 0;
	}
	const per = (v: number) => v / row.gp;
	return (
		per(row.pts) +
		1.2 * per(row.trb) +
		1.5 * per(row.ast) +
		2 * per(row.stl) +
		2 * per(row.blk) +
		// A great rate over ten games isn't a season anyone remembers.
		0.05 * row.gp
	);
};

// Every season a player logged for one franchise inside one era, collapsed to
// his best. Traded mid-season, he has two rows for the same year with the same
// team only in odd cases, so rows are taken as they come.
const bestSeasonFor = (
	p: TriviaPlayer,
	tid: number,
	era: EightyTwoZeroEra,
	position: EightyTwoZeroPosition,
): EightyTwoZeroOption | undefined => {
	let best: EightyTwoZeroOption | undefined;
	for (const row of p.rows) {
		if (
			row.tid !== tid ||
			!inEra(row.season, era) ||
			row.gp < MIN_GAMES ||
			!ELIGIBLE_LISTED[position].has(row.pos)
		) {
			continue;
		}
		const score = seasonScore(row);
		if (best === undefined || score > best.score) {
			best = {
				pid: p.pid,
				name: p.name,
				season: row.season,
				pos: row.pos,
				gp: row.gp,
				min: row.min,
				pts: row.pts,
				trb: row.trb,
				ast: row.ast,
				stl: row.stl,
				blk: row.blk,
				tp: row.tp,
				score,
			};
		}
	}
	return best;
};

// Cap the list a round shows. Beyond this it stops being a decision and starts
// being a search, and the tail is players nobody would take anyway.
const MAX_OPTIONS = 40;

export const getOptions = (
	pool: { players: TriviaPlayer[] },
	tid: number,
	era: EightyTwoZeroEra,
	position: EightyTwoZeroPosition,
	excludePids: ReadonlySet<number>,
): EightyTwoZeroOption[] => {
	const out: EightyTwoZeroOption[] = [];
	for (const p of pool.players) {
		if (excludePids.has(p.pid)) {
			continue;
		}
		const option = bestSeasonFor(p, tid, era, position);
		if (option) {
			out.push(option);
		}
	}
	return out.sort((a, b) => b.score - a.score).slice(0, MAX_OPTIONS);
};

export type EightyTwoZeroMatchup = {
	tid: number;
	eraStart: number;
};

// Every franchise-era that can fill each position, so the slot machine only
// ever rolls a round that has an answer.
//
// Built by walking each player's seasons ONCE and marking the buckets he
// qualifies for, rather than by asking "is there anyone?" of every
// team-era-position combination - that is thirty teams times a handful of eras
// times five positions times every player who ever played, and it is the
// difference between a page that opens and one that hangs.
export const buildMatchups = (
	pool: { players: TriviaPlayer[] },
	tids: number[],
	eras: EightyTwoZeroEra[],
): Record<EightyTwoZeroPosition, EightyTwoZeroMatchup[]> => {
	const allowedTids = new Set(tids);
	const eraFor = new Map<number, number>();
	for (const era of eras) {
		for (let season = era.start; season <= era.end; season++) {
			eraFor.set(season, era.start);
		}
	}

	const seen: Record<EightyTwoZeroPosition, Set<string>> = {
		PG: new Set(),
		SG: new Set(),
		SF: new Set(),
		PF: new Set(),
		C: new Set(),
	};

	for (const p of pool.players) {
		for (const row of p.rows) {
			if (row.gp < MIN_GAMES || !allowedTids.has(row.tid)) {
				continue;
			}
			const eraStart = eraFor.get(row.season);
			if (eraStart === undefined) {
				continue;
			}
			for (const position of EIGHTY_TWO_ZERO_POSITIONS) {
				if (ELIGIBLE_LISTED[position].has(row.pos)) {
					seen[position].add(`${row.tid}|${eraStart}`);
				}
			}
		}
	}

	const out = {} as Record<EightyTwoZeroPosition, EightyTwoZeroMatchup[]>;
	for (const position of EIGHTY_TWO_ZERO_POSITIONS) {
		out[position] = [...seen[position]]
			.map((key) => {
				const [tid, eraStart] = key.split("|");
				return { tid: Number(tid), eraStart: Number(eraStart) };
			})
			// Stable order, so a seeded roll always lands on the same matchup.
			.sort((a, b) => a.tid - b.tid || a.eraStart - b.eraStart);
	}
	return out;
};

// Deterministic per (league, day, round) so the Daily Challenge is the same
// five matchups however many times it's opened, and reloading the page can't
// reroll a bad one.
export const dailySeed = (season: number, day: number, round: number) =>
	`${g.get("lid")}|${season}|${day}|${round}`;

export const getPoolAndTeams = async () => {
	const pool = await getTriviaPool();
	const teamInfoCache = g.get("teamInfoCache");
	const tids: number[] = [];
	for (const [tid, info] of teamInfoCache.entries()) {
		if (!info.disabled) {
			tids.push(tid);
		}
	}
	return { pool, tids, eras: buildEras(pool.minSeason, pool.maxSeason) };
};
