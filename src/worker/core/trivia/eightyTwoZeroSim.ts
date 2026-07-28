import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import { mulberry32 } from "../../../common/sportsbookOdds.ts";
import GameSim from "../GameSim.ts";
import loadTeams, { processTeam } from "../game/loadTeams.ts";
import { player } from "../index.ts";
import type { Player } from "../../../common/types.ts";
import { PLAYER } from "../../../common/constants.ts";

// Does your five actually go 82-0?
//
// Rather than score the lineup with a formula, this plays the season: BBGM's
// own engine, eighty-two times, against the real teams in your league. The
// point of building the game inside the game is that the answer comes from the
// same code that decides every other result in your file.
//
// Three things have to be arranged for that to work.
//
// A drafted player is a SEASON, not a career - the 1996 version of someone, not
// whatever he is now. So each pick is rebuilt as a player frozen at that year:
// that season's ratings row, and his age in that season.
//
// Five men cannot play a season. With nobody to sub in, the engine runs all
// five for forty-eight minutes a night and fatigue quietly wrecks them, which
// would say more about the roster limit than about the picks. They get a bench
// of replacement-level players from the league - the same bench whatever you
// drafted, so it never flatters or punishes a particular five.
//
// And it has to be reproducible. The engine draws from Math.random, so the run
// is seeded from the lineup itself: the same five players always play the same
// season, however many times the page is opened.

const NUM_GAMES = 82;

// A synthetic tid for the drafted team. Anything that isn't a real team works;
// -1 and -2 are the All-Star sentinels, so those are avoided.
const DRAFT_TID = -99;

// How many bench players to add, and roughly how good they are. Deep enough for
// the engine to rotate normally, weak enough that the five are the story.
const BENCH_SIZE = 5;
const BENCH_TARGET_OVR = 42;

export type EightyTwoZeroPick = {
	pid: number;
	season: number;
};

export type EightyTwoZeroPlayerLine = {
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
	tov: number;
	fg: number;
	fga: number;
	tp: number;
	tpa: number;
	ft: number;
	fta: number;
};

export type EightyTwoZeroResult = {
	won: number;
	lost: number;
	ptsFor: number;
	ptsAgainst: number;
	best: { pts: number; opponent: string } | undefined;
	worst: { pts: number; opponent: string } | undefined;
	players: EightyTwoZeroPlayerLine[];
};

const hashString = (value: string) => {
	let h = 2166136261;
	for (let i = 0; i < value.length; i++) {
		h ^= value.charCodeAt(i);
		h = Math.imul(h, 16777619);
	}
	return h >>> 0;
};

// A stored player rebuilt as the season he was drafted from: that year's
// ratings, and that year's age.
//
// His VALUE is recomputed from that ratings row rather than left at whatever it
// is today, because the engine ranks its rotation by value - a legend drafted
// from his prime carries the value of the 38-year-old he retired as, and would
// sit on the bench of his own team.
const freezeAtSeason = async (
	p: Player,
	season: number,
	rosterOrder: number,
): Promise<Player | undefined> => {
	const ratings =
		p.ratings.find((r) => r.season === season) ??
		p.ratings.filter((r) => r.season <= season).at(-1);
	if (!ratings) {
		return undefined;
	}

	const ageThen = season - p.born.year;
	const frozen = {
		...helpers.deepCopy(p),
		ratings: [helpers.deepCopy(ratings)],
		// The engine reads age off the CURRENT season, so shift the birth year to
		// land on how old he was in the season being played.
		born: { ...p.born, year: g.get("season") - ageThen },
		stats: [],
		injury: { type: "Healthy", gamesRemaining: 0 },
		injuries: [],
		ptModifier: 1,
		rosterOrder,
	} as Player;
	await player.updateValues(frozen);
	return frozen;
};

// Real players from the league, near replacement level. Constant across
// drafts, so two lineups are compared on their five and nothing else.
const getBench = async (excludePids: ReadonlySet<number>) => {
	// Active players only, straight from the cache - a replacement-level bench is
	// whoever is scraping by in the league right now, and there is no reason to
	// walk every player who ever lived to find five of them.
	const all = await idb.cache.players.indexGetAll("playersByTid", [
		PLAYER.FREE_AGENT,
		Infinity,
	]);

	const candidates: { p: Player; season: number; ovr: number }[] = [];
	for (const p of all) {
		if (excludePids.has(p.pid)) {
			continue;
		}
		// Their newest ratings, whatever season that is. Requiring a row for the
		// current season looks right and isn't: in the offseason, or in a league
		// loaded from a file mid-progression, plenty of active players don't have
		// one yet - and an empty bench puts the drafted five back on the floor for
		// forty-eight minutes without saying so.
		const ratings = p.ratings.at(-1);
		if (ratings) {
			candidates.push({ p, season: ratings.season, ovr: ratings.ovr ?? 0 });
		}
	}

	// Closest to the target, ties broken by pid so the bench never shuffles.
	candidates.sort(
		(a, b) =>
			Math.abs(a.ovr - BENCH_TARGET_OVR) - Math.abs(b.ovr - BENCH_TARGET_OVR) ||
			a.p.pid - b.p.pid,
	);

	const out: Player[] = [];
	for (const row of candidates.slice(0, BENCH_SIZE)) {
		const frozen = await freezeAtSeason(row.p, row.season, out.length + 5);
		if (frozen) {
			out.push(frozen);
		}
	}
	return out;
};

export const simulateEightyTwoZeroSeason = async (
	picks: EightyTwoZeroPick[],
): Promise<EightyTwoZeroResult | undefined> => {
	if (picks.length === 0) {
		return undefined;
	}

	const drafted: Player[] = [];
	for (const [i, pick] of picks.entries()) {
		const p = await idb.getCopy.players({ pid: pick.pid }, "noCopyCache");
		if (!p) {
			return undefined;
		}
		const frozen = await freezeAtSeason(p, pick.season, i);
		if (!frozen) {
			return undefined;
		}
		drafted.push(frozen);
	}

	const draftedPids = new Set(drafted.map((p) => p.pid));
	const bench = await getBench(draftedPids);

	const lineup = await processTeam(
		{ tid: DRAFT_TID, playThroughInjuries: [0, 0] },
		{ won: 0, lost: 0, tied: 0, otl: 0, cid: -1, did: -1 },
		[...drafted, ...bench],
	);

	// Opponents: the real league, as it stands right now.
	const teamInfoCache = g.get("teamInfoCache");
	const opponentTids: number[] = [];
	for (const [tid, info] of teamInfoCache.entries()) {
		if (!info.disabled) {
			opponentTids.push(tid);
		}
	}
	if (opponentTids.length === 0) {
		return undefined;
	}
	const loaded = await loadTeams(opponentTids, {});
	const opponents = opponentTids
		.map((tid) => ({ tid, team: loaded[tid] }))
		.filter((row) => row.team !== undefined);
	if (opponents.length === 0) {
		return undefined;
	}

	const totals = new Map<number, EightyTwoZeroPlayerLine>();
	for (const p of drafted) {
		const ratings = p.ratings[0]!;
		totals.set(p.pid, {
			pid: p.pid,
			name: `${p.firstName} ${p.lastName}`,
			season: picks.find((pick) => pick.pid === p.pid)?.season ?? 0,
			pos: ratings.pos ?? "",
			gp: 0,
			min: 0,
			pts: 0,
			trb: 0,
			ast: 0,
			stl: 0,
			blk: 0,
			tov: 0,
			fg: 0,
			fga: 0,
			tp: 0,
			tpa: 0,
			ft: 0,
			fta: 0,
		});
	}

	let won = 0;
	let lost = 0;
	let ptsFor = 0;
	let ptsAgainst = 0;
	let best: EightyTwoZeroResult["best"];
	let worst: EightyTwoZeroResult["worst"];

	const seed = hashString(
		picks.map((pick) => `${pick.pid}:${pick.season}`).join("|"),
	);
	const realRandom = Math.random;
	try {
		Math.random = mulberry32(seed);

		for (let i = 0; i < NUM_GAMES; i++) {
			const opponent = opponents[i % opponents.length]!;
			// Alternate home and away, as a real schedule would.
			const atHome = i % 2 === 0;
			const mine = helpers.deepCopy(lineup) as any;
			const theirs = helpers.deepCopy(opponent.team) as any;
			const teams = (atHome ? [mine, theirs] : [theirs, mine]) as [any, any];

			const result = new GameSim({
				gid: i,
				day: -1,
				teams,
				doPlayByPlay: false,
				homeCourtFactor: 1,
				neutralSite: false,
				allStarGame: false,
				baseInjuryRate: 0,
			} as any).run() as any;

			const meIndex = atHome ? 0 : 1;
			const myScore = result.team[meIndex].stat.pts;
			const theirScore = result.team[1 - meIndex].stat.pts;
			ptsFor += myScore;
			ptsAgainst += theirScore;
			if (myScore > theirScore) {
				won += 1;
			} else {
				lost += 1;
			}

			const abbrev = teamInfoCache[opponent.tid]?.abbrev ?? "";
			const margin = myScore - theirScore;
			if (best === undefined || margin > best.pts) {
				best = { pts: margin, opponent: abbrev };
			}
			if (worst === undefined || margin < worst.pts) {
				worst = { pts: margin, opponent: abbrev };
			}

			for (const p of result.team[meIndex].player) {
				const line = totals.get(p.id);
				if (!line || p.stat.min <= 0) {
					continue;
				}
				line.gp += 1;
				line.min += p.stat.min;
				line.pts += p.stat.pts;
				line.trb += p.stat.orb + p.stat.drb;
				line.ast += p.stat.ast;
				line.stl += p.stat.stl;
				line.blk += p.stat.blk;
				line.tov += p.stat.tov;
				line.fg += p.stat.fg;
				line.fga += p.stat.fga;
				line.tp += p.stat.tp;
				line.tpa += p.stat.tpa;
				line.ft += p.stat.ft;
				line.fta += p.stat.fta;
			}
		}
	} finally {
		Math.random = realRandom;
	}

	return {
		won,
		lost,
		ptsFor: ptsFor / NUM_GAMES,
		ptsAgainst: ptsAgainst / NUM_GAMES,
		best,
		worst,
		// Back in the order they were drafted.
		players: picks
			.map((pick) => totals.get(pick.pid))
			.filter((line): line is EightyTwoZeroPlayerLine => line !== undefined),
	};
};
