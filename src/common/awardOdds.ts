import { bySport } from "./sportFunctions.ts";

// Live award odds.
//
// The award formulas the game ranks candidates by are built for a FINISHED
// season: they add up cumulative production (EWA, VORP, win shares, WAR, point
// shares) and compare the totals. Applied at game 10 that is a rate stat with
// ten games of noise in it, and applied to a player who missed four of those
// games it is a rate stat scaled down by 40% for no reason that will still be
// true in April.
//
// That is exactly how the best rookie in a league ends up fourth at +1000: he
// missed a few games in November, so his EWA is 60% of a peer who played every
// night, and the odds treat a November absence as a settled fact about the
// season. Nobody reasons that way.
//
// So odds are computed from a PROJECTED final season, not from the standings so
// far:
//
//   1. Scale each candidate's cumulative production to the games he is on pace
//      to actually play, so missing time costs only the games he will really
//      miss (current injury included) rather than a proportional share of the
//      whole year.
//   2. Early on, pull the ranking toward talent. Ten games of box score is a
//      worse predictor of an end-of-season award than the fact that one player
//      is far better than the others, and the weight on talent falls to zero as
//      the season fills in.
//   3. Sample. The remaining season is uncertain, so the win probability is the
//      share of simulated finishes a candidate leads - and the spread of those
//      finishes narrows as fewer games are left. In October nobody is a lock;
//      in April the leader nearly is. A single deterministic ranking cannot
//      express that, which is the other half of why the old numbers felt wrong.

// Stats that ACCUMULATE across a season, and so have to be scaled up when
// projecting a partial season forward.
//
// Basketball divides box score stats by games played, so only the advanced
// totals accumulate. The other sports keep raw season totals for everything, so
// their counting stats accumulate too. Anything not listed here is a rate (per
// game, per 100, a percentage, a rating) and is already the right scale.
export const cumulativeAwardStats = (): ReadonlySet<string> =>
	bySport({
		basketball: BASKETBALL_CUMULATIVE,
		football: FOOTBALL_CUMULATIVE,
		hockey: HOCKEY_CUMULATIVE,
		baseball: BASEBALL_CUMULATIVE,
	});

const BASKETBALL_CUMULATIVE: ReadonlySet<string> = new Set([
	"ewa",
	"vorp",
	"ws",
	"ows",
	"dws",
	"fracWS",
	"dd",
	"td",
	"qd",
	"fxf",
]);

const FOOTBALL_CUMULATIVE: ReadonlySet<string> = new Set([
	"av",
	"krTD",
	"krYds",
	"prTD",
	"prYds",
	"pntYds",
	"defSk",
	"defInt",
	"defTck",
]);

const HOCKEY_CUMULATIVE: ReadonlySet<string> = new Set([
	"ps",
	"ops",
	"dps",
	"gps",
	"g",
	"a",
	"pts",
	"hit",
	"tk",
	"blk",
]);

const BASEBALL_CUMULATIVE: ReadonlySet<string> = new Set([
	"war",
	"rbat",
	"rbr",
	"rfld",
	"rpit",
	"rpos",
	"raa",
]);

// How many games a candidate finishes the season having played, given what he
// has played, what his team has left, and how long he is currently hurt for.
// The point is that missed games only cost what they actually cost: four games
// out in November is four games, not 40% of a season.
export const projectedGamesPlayed = ({
	gp,
	teamGp,
	numGames,
	injuryGamesRemaining = 0,
}: {
	gp: number;
	teamGp: number;
	numGames: number;
	injuryGamesRemaining?: number;
}): number => {
	const teamGamesLeft = Math.max(0, numGames - teamGp);
	const willMiss = Math.min(teamGamesLeft, Math.max(0, injuryGamesRemaining));

	// Availability so far, so a player who keeps sitting out is not projected to
	// suddenly play every remaining night. Shrunk toward full health, because
	// four missed games out of ten is not evidence of a 60%-available player -
	// it is a small sample, and treating it as a rate is the same mistake the
	// cumulative stats make. By midseason the observed rate dominates.
	const AVAILABILITY_PRIOR_GAMES = 20;
	const availability = Math.min(
		1,
		(gp + AVAILABILITY_PRIOR_GAMES) / (teamGp + AVAILABILITY_PRIOR_GAMES),
	);

	return gp + (teamGamesLeft - willMiss) * availability;
};

// Deterministic RNG, so odds don't jump every time the page re-renders. Same
// season and award always sample the same finishes.
const makeRng = (seed: string) => {
	let h = 2166136261;
	for (let i = 0; i < seed.length; i++) {
		h ^= seed.charCodeAt(i);
		h = Math.imul(h, 16777619);
	}
	let a = h >>> 0;
	return () => {
		a |= 0;
		a = (a + 0x6d2b79f5) | 0;
		let t = Math.imul(a ^ (a >>> 15), 1 | a);
		t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
};

// Box-Muller, for the noise on each simulated finish.
const gauss = (rng: () => number) => {
	const u = Math.max(1e-12, rng());
	return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * rng());
};

const stdDev = (values: number[]) => {
	if (values.length < 2) {
		return 0;
	}
	const mean = values.reduce((sum, x) => sum + x, 0) / values.length;
	return Math.sqrt(
		values.reduce((sum, x) => sum + (x - mean) ** 2, 0) / values.length,
	);
};

// How wide the remaining season is, as a multiple of the spread between
// candidates. At a full season's remove the field is nearly a coin flip between
// the plausible contenders; with a handful of games left the order is close to
// settled.
const UNCERTAINTY = 1.1;

// How much of the ranking talent carries when nothing has been played. Falls
// off with the square of the season elapsed, so it is gone well before the
// halfway mark rather than propping a name up all year.
const TALENT_WEIGHT = 0.75;

export const talentWeight = (fractionComplete: number): number =>
	TALENT_WEIGHT * (1 - Math.min(1, Math.max(0, fractionComplete))) ** 2;

// Rank-match talent onto the score scale: the most talented candidate is given
// the best projected score in the field, the next the second best, and so on.
// This sidesteps having to convert an overall rating into award-score units,
// which has no principled answer, while still saying the useful thing - early
// on, expect the finish to look more like the talent order than like ten games
// of box score.
const talentScores = (scores: number[], talent: number[]): number[] => {
	const sortedScores = [...scores].sort((a, b) => b - a);
	const order = talent
		.map((value, i) => ({ value, i }))
		.sort((a, b) => b.value - a.value);
	const out = Array<number>(scores.length).fill(0);
	for (const [rank, entry] of order.entries()) {
		out[entry.i] = sortedScores[rank]!;
	}
	return out;
};

export const awardWinProbs = (
	candidates: { score: number; talent: number }[],
	{
		fractionComplete,
		seed = "",
		samples = 2000,
	}: { fractionComplete: number; seed?: string; samples?: number },
): number[] => {
	const n = candidates.length;
	if (n === 0) {
		return [];
	}
	if (n === 1) {
		return [1];
	}

	const f = Math.min(1, Math.max(0, fractionComplete));
	const scores = candidates.map((c) => c.score);
	const talent = talentScores(
		scores,
		candidates.map((c) => c.talent),
	);

	const w = talentWeight(f);
	const blended = scores.map((score, i) => (1 - w) * score + w * talent[i]!);

	// Scale the noise to how far apart the candidates are, so this works for any
	// award formula's units. A field that is all bunched up stays bunched up.
	const spread = stdDev(blended) || Math.abs(blended[0] ?? 1) || 1;
	const sigma = UNCERTAINTY * Math.sqrt(1 - f) * spread;

	if (sigma <= 0) {
		// Season is over: the leader has it.
		let best = 0;
		for (let i = 1; i < n; i++) {
			if (blended[i]! > blended[best]!) {
				best = i;
			}
		}
		return blended.map((_, i) => (i === best ? 1 : 0));
	}

	const rng = makeRng(`${seed}|${n}`);
	const wins = Array<number>(n).fill(0);
	for (let s = 0; s < samples; s++) {
		let best = 0;
		let bestValue = -Infinity;
		for (let i = 0; i < n; i++) {
			const value = blended[i]! + sigma * gauss(rng);
			if (value > bestValue) {
				bestValue = value;
				best = i;
			}
		}
		wins[best] = (wins[best] ?? 0) + 1;
	}

	return wins.map((count) => count / samples);
};
