import GameSim from "../GameSim.ts";
import { processTeam } from "../game/loadTeams.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import { mulberry32 } from "../../../common/sportsbookOdds.ts";
import toUI from "../../util/toUI.ts";

// The point spread, taken from the league's own engine instead of a formula.
//
// The shipped closed-form line is `0.3 * (ovrHome - ovrAway) + 3.35 * HCA`
// (see common/getGameSpread.ts). Measured against the engine, the points-per-ovr
// it treats as a constant is not one: across matchups the implied value ranged
// from 0.22 to 0.56, and the implied home edge came in nearer 2.3 than 3.35.
// That is not a miscalibrated coefficient - refitting it league-wide doesn't fix
// it - it's that a team overall is a lossy summary of a roster, so two teams the
// same ovr apart can be genuinely different distances apart. The only thing that
// sees that is playing the matchup.
//
// So: play it, a few dozen times, and average the margin. Three things keep that
// from being either slow or exploitable.
//
//   1. NOTHING SIMS ON THE PAGE'S CRITICAL PATH. Pricing only ever READS this
//      cache (peekSimMargin). A game with no entry is priced off the formula,
//      exactly as before, and gets queued. The board renders at its old speed
//      whatever the cache holds.
//   2. The queue is drained in the BACKGROUND, one game at a time, yielding
//      between games so the worker keeps answering everything else. When it
//      finishes it asks the sportsbook to re-render, and the refined lines
//      appear. 50 games' worth is about a fifth of a second of work per game.
//   3. It is DETERMINISTIC and CACHED against everything the sim reads, so the
//      same league state always produces the same number. That's what lets
//      bets.ts re-derive the board to validate a bet without it having drifted,
//      and what makes two devices in a shared league converge on identical
//      lines rather than merely similar ones.
//
// The sample mean is BLENDED with the formula rather than replacing it, because
// 50 runs carry real noise (~1.75 points of standard error against a 12.4-point
// game-to-game spread). Precision-weighted, the blend beats both inputs at any
// sample size - the formula's error is a bias and the sim's is noise, so there
// is no reason to pick one.

// Runs per game. 50 puts the standard error of the mean margin at about 1.75
// points and costs roughly 220ms - small enough to sit between two yields
// without the worker feeling stuck, big enough that the blend leans on it.
export const SIMS_PER_GAME = 50;

// How wrong the formula's spread is assumed to be, in points, before any sim
// evidence. This is the ONE tuning knob: it sets how far the blend moves off the
// formula. Deliberately a constant rather than fitted from the board, so the
// main board and a game's prop page can never compute it differently and bounce
// an honest bet as "that line has moved".
//
// Measured error was higher than this, but on a test league with a talent
// gradient far wider than a real one, so the measurement is an overestimate.
// 2.5 is robust across the plausible range: if the formula is really good
// (1.5 off) the blend still beats it, and if it's really poor (3.5) the blend
// captures most of what's available.
const PRIOR_SD = 2.5;

// Don't let a freak sample drag a line somewhere the formula would call absurd.
// At 50 runs a 6-point move is about 3.4 standard errors - past that it's much
// more likely the sample than the matchup.
const MAX_SHIFT = 6;

export type SimMargin = {
	// Mean home margin across the runs.
	mean: number;
	// Standard error of that mean.
	se: number;
	n: number;
};

export type SimMarginJob = {
	key: string;
	homeTid: number;
	awayTid: number;
	neutralSite: boolean;
	daysInFuture: number;
};

// Precision-weighted blend of the formula's margin and the simulated one. The
// weight on the sim is priorVar / (priorVar + simVar): with no sim evidence it
// is 0 and the formula stands; as the sample tightens it goes to 1.
export const blendMargin = (
	priorMargin: number,
	sim: SimMargin,
	priorSd = PRIOR_SD,
): number => {
	const priorVar = priorSd ** 2;
	const simVar = sim.se ** 2;
	if (!Number.isFinite(simVar) || simVar <= 0) {
		return sim.mean;
	}
	const w = priorVar / (priorVar + simVar);
	const blended = w * sim.mean + (1 - w) * priorMargin;
	return helpers.bound(
		blended,
		priorMargin - MAX_SHIFT,
		priorMargin + MAX_SHIFT,
	);
};

// Every game attribute the engine reads that could move the margin. Any of these
// changing has to invalidate every cached line.
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
	"elamMinutes",
	"elamPoints",
	"elamOvertime",
	"injuryRate",
	"gender",
	"phase",
	"season",
	"lid",
] as const;

const hashString = (value: string) => {
	let h = 2166136261;
	for (let i = 0; i < value.length; i++) {
		h ^= value.charCodeAt(i);
		h = Math.imul(h, 16777619);
	}
	return h >>> 0;
};

export const settingsFingerprint = (): string =>
	SIM_SETTINGS.map((key) => String(g.get(key as any))).join(",");

// A roster, exactly as the engine will see it - EVERYTHING processTeam reads
// when it builds a side (see core/game/loadTeams.ts):
//
//   rosterOrder     who starts
//   ptModifier      a player held out, or given extra minutes
//   injury          who is hurt, and by how much
//   value           how good everyone is
//
// plus the team's play-through-injuries setting, which decides whether a hurt
// player suits up at all. Miss one of these and a user benches his star, comes
// back to the board, and is quoted the line from before he did.
//
// Built from the player list the pricer already loaded, so asking costs nothing.
export const rosterFingerprint = (
	players: {
		pid: number;
		value?: number;
		ptModifier?: number | string;
		rosterOrder?: number;
		injury?: { gamesRemaining?: number };
	}[],
	playThroughInjuries: readonly [number, number] = [0, 0],
): string => {
	const parts: (string | number)[] = [
		playThroughInjuries[0],
		playThroughInjuries[1],
	];
	for (const p of players) {
		parts.push(
			p.pid,
			Math.round((p.value ?? 0) * 100),
			p.injury?.gamesRemaining ?? 0,
			// The roster view hands this back as a string, so normalize.
			Number(p.ptModifier ?? 1),
			p.rosterOrder ?? -1,
		);
	}
	return parts.join(",");
};

export const simMarginKey = ({
	settings,
	homeRoster,
	awayRoster,
	neutralSite,
	daysInFuture,
}: {
	settings: string;
	homeRoster: string;
	awayRoster: string;
	neutralSite: boolean;
	daysInFuture: number;
}): string =>
	`${hashString(settings)}|${hashString(homeRoster)}|${hashString(awayRoster)}|${
		neutralSite ? 1 : 0
	}|${daysInFuture}`;

// A board's worth, several times over, so flipping between the sportsbook and a
// game's prop page doesn't throw work away. Entries are keyed by the exact league
// state they came from, so a stale one can never be served - it stops matching.
const CACHE_SIZE = 80;
const cache = new Map<string, SimMargin | null>();

const cacheGet = (key: string) => {
	const value = cache.get(key);
	if (value !== undefined) {
		cache.delete(key);
		cache.set(key, value);
	}
	return value;
};

const cacheSet = (key: string, value: SimMargin | null) => {
	cache.set(key, value);
	while (cache.size > CACHE_SIZE) {
		cache.delete(cache.keys().next().value!);
	}
};

// Read-only. Never sims - this is what pricing calls, so pricing can never be
// the thing that makes the page wait. `null` is a remembered failure, so a game
// the engine can't load isn't retried on a loop.
export const peekSimMargin = (key: string): SimMargin | undefined => {
	const value = cacheGet(key);
	return value ?? undefined;
};

// Test seam.
export const __setSimMargin = (key: string, value: SimMargin | null) => {
	cacheSet(key, value);
};
export const __clearSimMargins = () => {
	cache.clear();
};

// The roster the engine will see on the day the game is played, injuries healed
// forward the same way the formula's spread heals them.
const loadSide = async (tid: number, daysInFuture: number) => {
	const [t, teamSeason, players] = await Promise.all([
		idb.cache.teams.get(tid),
		idb.cache.teamSeasons.indexGet("teamSeasonsByTidSeason", [
			tid,
			g.get("season"),
		]),
		idb.getCopies.players({ tid }, "noCopyCache"),
	]);
	if (!t || !teamSeason) {
		return undefined;
	}

	if (daysInFuture > 0) {
		for (const p of players) {
			if (p.injury.gamesRemaining > 0) {
				p.injury = {
					...p.injury,
					gamesRemaining: Math.max(0, p.injury.gamesRemaining - daysInFuture),
				};
			}
		}
	}

	return processTeam(t, teamSeason, players);
};

const simOne = async (job: SimMarginJob): Promise<SimMargin | null> => {
	const [home, away] = await Promise.all([
		loadSide(job.homeTid, job.daysInFuture),
		loadSide(job.awayTid, job.daysInFuture),
	]);
	if (!home || !away) {
		return null;
	}
	const base = [home, away] as [any, any];

	const margins: number[] = [];

	// The engine draws from Math.random, so a seeded generator keyed to the exact
	// state being priced is what makes the line reproducible. The loop below is
	// entirely synchronous - nothing else in the worker can run, and see the
	// swapped generator, while it is in place. Do NOT introduce an await here.
	const realRandom = Math.random;
	try {
		Math.random = mulberry32(hashString(job.key));
		for (let i = 0; i < SIMS_PER_GAME; i++) {
			const result: any = new GameSim({
				gid: -1,
				day: -1,
				teams: helpers.deepCopy(base),
				doPlayByPlay: false,
				homeCourtFactor: 1,
				neutralSite: job.neutralSite,
				allStarGame: false,
				baseInjuryRate: g.get("injuryRate"),
			} as any).run();
			margins.push(result.team[0].stat.pts - result.team[1].stat.pts);
		}
	} finally {
		Math.random = realRandom;
	}

	const n = margins.length;
	const mean = margins.reduce((s, x) => s + x, 0) / n;
	const variance =
		margins.reduce((s, x) => s + (x - mean) ** 2, 0) / Math.max(1, n - 1);
	return { mean, se: Math.sqrt(variance / n), n };
};

// Let the worker breathe between games. Not inside one - see the RNG swap above.
const yieldToWorker = () =>
	new Promise<void>((resolve) => {
		setTimeout(resolve, 0);
	});

let warming = false;

// Drain the queue in the background and, if anything new landed, ask the
// sportsbook to re-render with the refined lines. Fire-and-forget: callers must
// not await this, or the page waits for exactly the work this exists to avoid.
export const warmSimMargins = async (jobs: SimMarginJob[]) => {
	if (warming) {
		return false;
	}
	warming = true;
	let landed = false;
	try {
		for (const job of jobs) {
			if (cache.has(job.key)) {
				continue;
			}
			let value: SimMargin | null = null;
			try {
				value = await simOne(job);
			} catch (error) {
				console.error("Sportsbook spread sim failed", error);
				value = null;
			}
			cacheSet(job.key, value);
			if (value) {
				landed = true;
			}
			await yieldToWorker();
		}
	} finally {
		warming = false;
	}

	if (landed) {
		await toUI("realtimeUpdate", [["sportsbookLines"]]);
	}
	return landed;
};
