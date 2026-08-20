import GameSim from "../GameSim.ts";
import { processTeam } from "../game/loadTeams.ts";
import { healedForward } from "./healInjuriesForward.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import { mulberry32 } from "../../../common/sportsbookOdds.ts";

// Prop pricing by simulating the actual game, over and over.
//
// The old prop model was a normal curve per stat, centered on a blend of the
// player's season average and last season's, with a hand-tuned coefficient of
// variation for the spread. Every number in it was a guess about a player in
// the abstract, and it could not see the game it was pricing: not that the
// opponent is the best defensive team in the league, not that the starting
// point guard is out and someone else has his minutes, not that a slow team
// drags the possession count down, not that a player's points and rebounds and
// assists all rise and fall together because they come from the same minutes.
// It also had to invent the joint distribution for double-doubles by assuming
// the categories were independent, which they emphatically are not.
//
// This runs the LEAGUE'S OWN game engine on the actual matchup a few hundred
// times and reads the answer off the results. Every one of those problems goes
// away at once, because the sim already models all of it - it is the same code
// that will decide the real game. A player's projected line is the mean of what
// he actually did across those games; the odds on going over it are how often
// he actually went over.
//
// Two things make this safe to price real bets against:
//
//   1. It is DETERMINISTIC. The engine draws from Math.random, so the batch
//      swaps in a seeded generator keyed to the exact league state being
//      priced, and restores the real one afterward. Same league state always
//      produces the same board, which is what lets the server re-derive it to
//      validate a bet (see bets.ts) without the board having drifted underneath
//      an honest client.
//   2. It is CACHED against a fingerprint of everything the sim reads, so the
//      board is computed once per league state rather than once per page load
//      and once more per bet placed.

export const SIM_PLAYER_STATS = [
	"min",
	"pts",
	"trb",
	"ast",
	"stl",
	"blk",
	"tp",
	"tov",
] as const;
export type SimPlayerStat = (typeof SIM_PLAYER_STATS)[number];

export const SIM_TEAM_STATS = ["pts", "trb", "ast", "tp"] as const;
export type SimTeamStat = (typeof SIM_TEAM_STATS)[number];

export type SimmedPlayer = {
	pid: number;
	name: string;
	tid: number;
	// One entry per simulated game.
	samples: Record<SimPlayerStat, number[]>;
	// How many of those games he recorded a double-double / triple-double in.
	// Counted from the joint result of a single simulated game, so the
	// correlation between categories is real rather than assumed away.
	dd: number;
	td: number;
};

export type SimmedTeam = {
	tid: number;
	samples: Record<SimTeamStat, number[]>;
	players: SimmedPlayer[];
};

export type SimmedGame = {
	numSims: number;
	// [home, away]
	teams: [SimmedTeam, SimmedTeam];
	overtimes: number;
};

// Enough runs that a coin-flip prop lands within a couple of points of its true
// probability - invisible once the vig is applied - while keeping the one-time
// cost of opening a game's prop board in the low seconds. The board is cached
// afterward, so this is paid once per league state, not once per bet.
const BASE_SIMS = 500;
const MIN_SIMS = 150;

// A sim costs roughly what a game's minutes cost, so a league playing 4x24
// minute quarters would pay double for the same number of runs. Scale the count
// back so the wait doesn't blow up with the setting. Depends only on settings,
// so it stays deterministic.
const BASELINE_REGULATION_MINUTES = 48;

// Every game attribute the sim reads that could change the distribution. Any of
// these moving has to invalidate the cached board.
const SIM_SETTINGS = [
	"numPeriods",
	"quarterLength",
	"pace",
	"homeCourtAdvantage",
	"foulRateFactor",
	"threePointers",
	"threePointTendencyFactor",
	"threePointAccuracyFactor",
	"twoPointAccuracyFactor",
	"ftAccuracyFactor",
	"blockFactor",
	"stealFactor",
	"turnoverFactor",
	"orbFactor",
	"assistFactor",
	"numPlayersOnCourt",
	"elam",
	"elamASG",
	"elamMinutes",
	"elamPoints",
	"elamOvertime",
	"injuryRate",
	"gender",
	"phase",
	"season",
	"lid",
] as const;

// A stable 32-bit hash of the fingerprint, used both as the cache key and as the
// seed - so the same league state always simulates the same games.
const hashString = (value: string) => {
	let h = 2166136261;
	for (let i = 0; i < value.length; i++) {
		h ^= value.charCodeAt(i);
		h = Math.imul(h, 16777619);
	}
	return h >>> 0;
};

// Everything the sim's output depends on: the settings above, plus each side's
// rotation exactly as the engine will see it (who is on the roster, in what
// order, how hurt, and how good).
const fingerprint = (
	gid: number,
	neutralSite: boolean,
	teams: [any, any],
): string => {
	const parts: (string | number)[] = [
		gid,
		neutralSite ? 1 : 0,
		...SIM_SETTINGS.map((key) => String(g.get(key as any))),
	];
	for (const t of teams) {
		parts.push("t", t.id);
		for (const p of t.player) {
			parts.push(
				p.pid,
				p.injured ? 1 : 0,
				p.injury?.gamesRemaining ?? 0,
				Math.round((p.valueNoPot ?? 0) * 100),
				p.ptModifier ?? 1,
			);
		}
	}
	return parts.join(",");
};

const emptyPlayerSamples = (): Record<SimPlayerStat, number[]> => ({
	min: [],
	pts: [],
	trb: [],
	ast: [],
	stl: [],
	blk: [],
	tp: [],
	tov: [],
});

const emptyTeamSamples = (): Record<SimTeamStat, number[]> => ({
	pts: [],
	trb: [],
	ast: [],
	tp: [],
});

// Same definition the game itself uses when it writes dd/td to a box score (see
// game/writePlayerStats.ts) - counted here off the simulated line rather than
// modeled, so it inherits the real correlation between categories.
const DOUBLE_STATS = ["pts", "ast", "stl", "blk", "trb"] as const;

const countDoubles = (line: Record<string, number>) => {
	let numDoubles = 0;
	for (const stat of DOUBLE_STATS) {
		if ((line[stat] ?? 0) >= 10) {
			numDoubles += 1;
		}
	}
	return numDoubles;
};

// A few games' worth, so clicking between two games' boards and then placing a
// bet on the first one doesn't pay for the whole batch again. Entries are
// keyed by the exact league state they were simulated from, so a stale one can
// never be served - it just stops matching.
const CACHE_SIZE = 4;
const cache = new Map<string, SimmedGame>();

const cacheGet = (key: string) => {
	const value = cache.get(key);
	if (value !== undefined) {
		// Refresh recency.
		cache.delete(key);
		cache.set(key, value);
	}
	return value;
};

const cacheSet = (key: string, value: SimmedGame) => {
	cache.set(key, value);
	while (cache.size > CACHE_SIZE) {
		cache.delete(cache.keys().next().value!);
	}
};

// The roster the engine will see, as of the day the game is actually played.
// Injuries are healed forward the same way the game's spread is computed (see
// gameLines.ts), so a board pulled up three days early doesn't sim a player as
// out when he'll be back by tipoff.
const loadSide = async (tid: number, daysInFuture: number) => {
	const [t, teamSeason, players] = await Promise.all([
		idb.cache.teams.get(tid),
		idb.cache.teamSeasons.indexGet("teamSeasonsByTidSeason", [
			tid,
			g.get("season"),
		]),
		// NOT copies - "noCopyCache" is this caller promising not to mutate what
		// it gets back, which is what lets the db hand over the live cache rows.
		// Anything hypothetical has to be built alongside them, never onto them.
		idb.getCopies.players({ tid }, "noCopyCache"),
	]);
	if (!t || !teamSeason) {
		return undefined;
	}

	// Healed forward as COPIES - see healInjuriesForward.ts. These rows are the
	// live cache records, and writing a hypothetical onto them put it in the
	// league and then published it to the whole room.
	return processTeam(t, teamSeason, healedForward(players, daysInFuture));
};

export const simGameOutcomes = async ({
	gid,
	homeTid,
	awayTid,
	neutralSite,
	daysInFuture = 0,
}: {
	gid: number;
	homeTid: number;
	awayTid: number;
	neutralSite: boolean;
	// How many days out the game is, so rosters can be healed forward to it.
	daysInFuture?: number;
}): Promise<SimmedGame | undefined> => {
	const [home, away] = await Promise.all([
		loadSide(homeTid, daysInFuture),
		loadSide(awayTid, daysInFuture),
	]);
	if (!home || !away) {
		return undefined;
	}
	const base = [home, away] as [any, any];

	const key = fingerprint(gid, neutralSite, base);
	const cached = cacheGet(key);
	if (cached) {
		return cached;
	}

	const regulationMinutes = g.get("numPeriods") * g.get("quarterLength");
	const numSims = Math.max(
		MIN_SIMS,
		Math.min(
			BASE_SIMS,
			Math.round(
				(BASE_SIMS * BASELINE_REGULATION_MINUTES) /
					Math.max(1, regulationMinutes),
			),
		),
	);

	const teams: [SimmedTeam, SimmedTeam] = [
		{ tid: home.id, samples: emptyTeamSamples(), players: [] },
		{ tid: away.id, samples: emptyTeamSamples(), players: [] },
	];
	const playersByTeam: Map<number, SimmedPlayer>[] = [new Map(), new Map()];
	let overtimes = 0;

	// The engine draws from Math.random. Swapping it for a seeded generator is
	// what makes the board reproducible; the batch below is entirely synchronous,
	// so nothing else in the worker can run - and see the swapped generator -
	// while it's in place. Restored unconditionally.
	const realRandom = Math.random;
	const rng = mulberry32(hashString(key));
	try {
		Math.random = rng;

		for (let i = 0; i < numSims; i++) {
			const copy = helpers.deepCopy(base);
			const result = new GameSim({
				gid,
				day: -1,
				teams: copy,
				doPlayByPlay: false,
				homeCourtFactor: 1,
				neutralSite,
				allStarGame: false,
				baseInjuryRate: g.get("injuryRate"),
			} as any).run() as any;

			if (result.overtimes > 0) {
				overtimes += 1;
			}

			for (const t of [0, 1] as const) {
				const simTeam = result.team[t];
				const teamStat = simTeam.stat;
				teams[t].samples.pts.push(teamStat.pts ?? 0);
				teams[t].samples.trb.push((teamStat.orb ?? 0) + (teamStat.drb ?? 0));
				teams[t].samples.ast.push(teamStat.ast ?? 0);
				teams[t].samples.tp.push(teamStat.tp ?? 0);

				for (const p of simTeam.player) {
					let entry = playersByTeam[t]!.get(p.id);
					if (!entry) {
						entry = {
							pid: p.id,
							name: p.name,
							tid: simTeam.id,
							samples: emptyPlayerSamples(),
							dd: 0,
							td: 0,
						};
						playersByTeam[t]!.set(p.id, entry);
					}

					const line: Record<string, number> = {
						min: p.stat.min ?? 0,
						pts: p.stat.pts ?? 0,
						trb: (p.stat.orb ?? 0) + (p.stat.drb ?? 0),
						ast: p.stat.ast ?? 0,
						stl: p.stat.stl ?? 0,
						blk: p.stat.blk ?? 0,
						tp: p.stat.tp ?? 0,
						tov: p.stat.tov ?? 0,
					};
					for (const stat of SIM_PLAYER_STATS) {
						entry.samples[stat].push(line[stat]!);
					}

					const numDoubles = countDoubles(line);
					if (numDoubles >= 2) {
						entry.dd += 1;
						if (numDoubles >= 3) {
							entry.td += 1;
						}
					}
				}
			}
		}
	} finally {
		Math.random = realRandom;
	}

	for (const t of [0, 1] as const) {
		teams[t].players = [...playersByTeam[t]!.values()];
	}

	const value: SimmedGame = { numSims, teams, overtimes };
	cacheSet(key, value);
	return value;
};
