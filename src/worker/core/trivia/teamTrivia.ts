import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import { PHASE } from "../../../common/constants.ts";
import { getSearchList, getTriviaPool } from "./pool.ts";

// Team Trivia, ported from ZenGM Grids' team-trivia page: a random
// team-season is drawn and the player works through rounds - name the
// roster, then with hints, then pick each stat leader, guess the win total
// within a window, and pick the playoff finish. All round/scoring flow lives
// in the UI; this builds one round's data bundle.

export type TeamTriviaRoster = {
	pid: number;
	name: string;
	pos: string;
	age: number;
	gp: number;
	jerseyNumber: string | undefined;
	// Minutes per game is the one rate stat the stat-leader round never asks
	// about, so it's the only one that can be shown while the quiz is running.
	mpg: number;
	ppg: number;
	rpg: number;
	apg: number;
	spg: number;
	bpg: number;
	// Season totals, for leader determination display.
	pts: number;
	trb: number;
	ast: number;
	stl: number;
	blk: number;
};

// Roster display order, for a grid whose whole point is that you don't know
// who these people are yet.
const POS_ORDER = ["PG", "SG", "G", "GF", "SF", "F", "PF", "FC", "C"];
const posRank = (pos: string) => {
	const i = POS_ORDER.indexOf(pos);
	return i === -1 ? POS_ORDER.length : i;
};
const jerseyRank = (jerseyNumber: string | undefined) => {
	const n = Number.parseInt(jerseyNumber ?? "");
	return Number.isNaN(n) ? Infinity : n;
};

// The order the card grid is rendered in. Deliberately says NOTHING about how
// good anyone was: the grid is on screen while the stat-leader round asks who
// led the team in points, so a best-first roster would print the answer in the
// top-left corner.
export const byRosterDisplayOrder = (
	a: Pick<TeamTriviaRoster, "pos" | "jerseyNumber" | "name">,
	b: Pick<TeamTriviaRoster, "pos" | "jerseyNumber" | "name">,
) =>
	posRank(a.pos) - posRank(b.pos) ||
	jerseyRank(a.jerseyNumber) - jerseyRank(b.jerseyNumber) ||
	a.name.localeCompare(b.name);

// Which team-season to quiz on. Nothing set = a random one from the whole of
// league history; a year range narrows the draw; an explicit season/tid picks
// one outright, which is how the season and team dropdowns work.
export type TeamTriviaOptions = {
	season?: number;
	tid?: number;
	minSeason?: number;
	maxSeason?: number;
};

// Every team-season with enough of a roster to be quizzable, so the UI can
// populate its season and team dropdowns without guessing (a team that didn't
// exist in 2044 must not be offered for 2044). Fetched once and cached in the
// UI rather than resent with every round.
export type TeamTriviaCatalog = {
	candidates: { season: number; tid: number }[];
	minSeason: number;
	maxSeason: number;
};

export type TeamTriviaRound = {
	season: number;
	team: {
		tid: number;
		label: string;
		abbrev: string;
		colors?: [string, string, string];
		jersey?: string;
	};
	roster: TeamTriviaRoster[];
	// pid of the team leader in each stat (by season total).
	leaders: { pts: number; trb: number; ast: number; stl: number; blk: number };
	wins: { actual: number; games: number; window: number };
	// Absent when the season's playoffs haven't happened/finished yet.
	playoffs?: { options: string[]; answerIndex: number };
	searchList: { pid: number; name: string; years: string }[];
};

const round1 = (x: number) => Math.round(x * 10) / 10;

// Fewer than this and there's nothing to name.
const MIN_ROSTER = 5;

// Every quizzable team-season. Shared by the catalog (which the dropdowns read)
// and the generator (which draws from it), so the two can never disagree about
// what's playable.
const getCandidates = (
	pool: Awaited<ReturnType<typeof getTriviaPool>>,
): { season: number; tid: number }[] => {
	const currentSeason = g.get("season");
	const playoffsDone = g.get("phase") > PHASE.PLAYOFFS;

	// Roster sizes per (season, tid), so only real team-seasons are drawn.
	const rosterCount = new Map<string, number>();
	for (const p of pool.players) {
		for (const r of p.rows) {
			if (r.gp > 0) {
				const key = `${r.season}-${r.tid}`;
				rosterCount.set(key, (rosterCount.get(key) ?? 0) + 1);
			}
		}
	}

	const candidates: { season: number; tid: number }[] = [];
	for (const [key, count] of rosterCount) {
		if (count < MIN_ROSTER) {
			continue;
		}
		const [seasonStr, tidStr] = key.split("-");
		const season = Number(seasonStr);
		const tid = Number(tidStr);
		// The current season is only quizzable once its story is finished.
		if (season === currentSeason && !playoffsDone) {
			continue;
		}
		candidates.push({ season, tid });
	}
	candidates.sort((a, b) => a.season - b.season || a.tid - b.tid);
	return candidates;
};

export const getTeamTriviaCatalog = async (): Promise<TeamTriviaCatalog> => {
	const pool = await getTriviaPool();
	const candidates = getCandidates(pool);
	let minSeason = Infinity;
	let maxSeason = -Infinity;
	for (const c of candidates) {
		minSeason = Math.min(minSeason, c.season);
		maxSeason = Math.max(maxSeason, c.season);
	}
	if (!Number.isFinite(minSeason)) {
		const season = g.get("season");
		minSeason = season;
		maxSeason = season;
	}
	return { candidates, minSeason, maxSeason };
};

// Narrow the draw to what the player asked for. An exact season+tid pick wins
// outright; a range or a team filter narrows; and a filter that rules
// everything out falls back to the unfiltered list rather than returning
// nothing, because a dropdown that silently does nothing is worse than one
// that quietly ignores an impossible combination.
export const narrowCandidates = (
	candidates: { season: number; tid: number }[],
	options: TeamTriviaOptions,
): { season: number; tid: number }[] => {
	const { season, tid, minSeason, maxSeason } = options;
	const matches = candidates.filter((c) => {
		if (season !== undefined && c.season !== season) {
			return false;
		}
		if (tid !== undefined && c.tid !== tid) {
			return false;
		}
		if (minSeason !== undefined && c.season < minSeason) {
			return false;
		}
		if (maxSeason !== undefined && c.season > maxSeason) {
			return false;
		}
		return true;
	});
	return matches.length > 0 ? matches : candidates;
};

export const generateTeamTriviaRound = async (
	options: TeamTriviaOptions = {},
): Promise<TeamTriviaRound | undefined> => {
	const pool = await getTriviaPool();
	const currentSeason = g.get("season");
	const playoffsDone = g.get("phase") > PHASE.PLAYOFFS;

	const all = getCandidates(pool);
	if (all.length === 0) {
		return undefined;
	}
	const candidates = narrowCandidates(all, options);

	// Up to a few draws in case a candidate's team-season row is missing.
	for (let attempt = 0; attempt < 10; attempt++) {
		const { season, tid } =
			candidates[Math.floor(Math.random() * candidates.length)]!;

		const teamSeasons = await idb.getCopies.teamSeasons(
			{ season },
			"noCopyCache",
		);
		const ts = teamSeasons.find((row) => row.tid === tid);
		if (!ts) {
			continue;
		}

		const games = ts.won + ts.lost + (ts.tied ?? 0) + ((ts as any).otl ?? 0);
		if (games <= 0) {
			continue;
		}

		const team = await idb.cache.teams.get(tid);
		const region = ts.region || team?.region || "";
		const name = ts.name || team?.name || "";
		const abbrev = ts.abbrev || team?.abbrev || "???";

		const roster: TeamTriviaRoster[] = [];
		for (const p of pool.players) {
			for (const r of p.rows) {
				if (r.season === season && r.tid === tid && r.gp > 0) {
					roster.push({
						pid: p.pid,
						name: p.name,
						pos: r.pos,
						age: season - p.bornYear,
						gp: r.gp,
						jerseyNumber: r.jerseyNumber,
						mpg: round1(r.min / r.gp),
						ppg: round1(r.pts / r.gp),
						rpg: round1(r.trb / r.gp),
						apg: round1(r.ast / r.gp),
						spg: round1(r.stl / r.gp),
						bpg: round1(r.blk / r.gp),
						pts: r.pts,
						trb: r.trb,
						ast: r.ast,
						stl: r.stl,
						blk: r.blk,
					});
				}
			}
		}
		if (roster.length < 5) {
			continue;
		}
		// Display order. NOT by scoring: the grid is rendered in this order, so
		// best-first would put the answer to "who led the team in points?" in the
		// top-left corner of the board before the question is even asked. Position
		// then jersey reads like a real roster page and gives nothing away.
		roster.sort(byRosterDisplayOrder);

		const leaderBy = (key: "pts" | "trb" | "ast" | "stl" | "blk") =>
			roster.reduce((best, p) => (p[key] > best[key] ? p : best)).pid;
		const leaders = {
			pts: leaderBy("pts"),
			trb: leaderBy("trb"),
			ast: leaderBy("ast"),
			stl: leaderBy("stl"),
			blk: leaderBy("blk"),
		};

		// Win-total guess: a window 12.5% of the season wide counts as correct.
		const wins = {
			actual: ts.won,
			games,
			window: Math.max(1, Math.round(games * 0.125)),
		};

		// Playoff finish, from playoffRoundsWon (-1 = missed; numRounds = title).
		let playoffs: TeamTriviaRound["playoffs"];
		const roundsWon = ts.playoffRoundsWon;
		if (roundsWon !== undefined && (season < currentSeason || playoffsDone)) {
			const series = await idb.getCopy.playoffSeries({ season }, "noCopyCache");
			const numRounds =
				series?.series.length ?? g.get("numGamesPlayoffSeries").length;
			if (numRounds > 0) {
				const options = ["Missed the playoffs"];
				for (let i = 0; i < numRounds - 1; i++) {
					options.push(`Lost in ${helpers.ordinal(i + 1)} round`);
				}
				options.push("Lost in the Finals", "Won the championship");
				// options index: 0 = missed; 1..numRounds = lost in round i; last = champ
				const answerIndex =
					roundsWon < 0 ? 0 : Math.min(roundsWon + 1, options.length - 1);
				playoffs = { options, answerIndex };
			}
		}

		return {
			season,
			team: {
				tid,
				label: `${region} ${name}`,
				abbrev,
				// One set of colors for the whole round: every player on the card
				// grid wore this team's jersey, so the faces can be drawn without a
				// per-player team lookup.
				colors: team?.colors,
				jersey: team?.jersey,
			},
			roster,
			leaders,
			wins,
			playoffs,
			searchList: getSearchList(pool),
		};
	}

	return undefined;
};
