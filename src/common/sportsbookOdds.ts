import { bySport } from "./sportFunctions.ts";

// The pure probability models behind the sportsbook's live lines. Kept
// data-free and deterministic so they can be unit-tested and reused by the
// worker odds engine. Turning these probabilities into displayed American
// prices (with the house vig) is done in common/sportsbook.ts.

// Small deterministic PRNG (mulberry32) and a standard-normal sampler built on
// it (Box-Muller). Shared by every Monte Carlo model in the sportsbook
// (season/playoff futures in sportsbookFutures.ts, tier-membership odds
// below) so they all get the same reproducible-per-seed behavior from one
// place.
export const mulberry32 = (seed: number) => {
	let s = seed | 0;
	return () => {
		s = (s + 0x6d2b79f5) | 0;
		let t = Math.imul(s ^ (s >>> 15), 1 | s);
		t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
};

export const normalSample = (rand: () => number): number => {
	const u = Math.max(1e-12, 1 - rand());
	const v = rand();
	return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
};

// Standard normal CDF via a numerical erf approximation (Abramowitz & Stegun
// 7.1.26). Good to ~1e-7, plenty for odds.
export const normalCdf = (z: number): number => {
	const sign = z < 0 ? -1 : 1;
	const x = Math.abs(z) / Math.SQRT2;
	const t = 1 / (1 + 0.3275911 * x);
	const y =
		1 -
		((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t - 0.284496736) *
			t +
			0.254829592) *
			t *
			Math.exp(-x * x);
	return 0.5 * (1 + sign * y);
};

// Typical standard deviation of a game's final margin, per sport. Converts an
// expected point margin (from getGameSpread) into a win probability: a team
// favored by `margin` wins with probability Phi(margin / sigma). Exported so
// other margin-distribution models (e.g. the "does this game go to overtime"
// prop in getGameProps.ts) use the exact same spread rather than a
// second, potentially-inconsistent guess.
export const MARGIN_SIGMA = bySport({
	basketball: 13,
	football: 14.5,
	baseball: 4.4,
	hockey: 3.1,
});

// Win probability for a team expected to win by `expectedMargin` points
// (negative if the underdog). Clamped away from 0/1.
export const marginToWinProb = (expectedMargin: number): number => {
	const p = normalCdf(expectedMargin / MARGIN_SIGMA);
	return Math.min(0.995, Math.max(0.005, p));
};

// Expected total points for a game from each team's GENUINE season scoring
// (points scored and allowed per game). Additive matchup model - the standard
// bookmaker approach: a side's expected score is its own offense plus how much
// the opponent's defense gives up relative to league average, so a great
// offense meeting a bad defense projects high instead of being averaged down.
// Falls back to the league-average total when a team has no data yet.
export const expectedGameTotal = ({
	homeFor,
	homeAgainst,
	awayFor,
	awayAgainst,
	leagueAvgTotal,
}: {
	homeFor?: number;
	homeAgainst?: number;
	awayFor?: number;
	awayAgainst?: number;
	leagueAvgTotal: number;
}): number => {
	const halfLeague = leagueAvgTotal / 2;
	const homePts =
		(homeFor ?? halfLeague) + (awayAgainst ?? halfLeague) - halfLeague;
	const awayPts =
		(awayFor ?? halfLeague) + (homeAgainst ?? halfLeague) - halfLeague;
	return homePts + awayPts;
};

// Probability a Normal(mean, sigma) draw lands over `line`. The general form
// behind every over/under in the sportsbook - overProb below is just this
// with a whole-game-total-specific sigma baked in; player/team prop lines
// (getGameProps.ts) need their OWN calibrated sigma per stat (a player's
// rebounds and a whole game's combined point total do not share one spread
// model), so they call this directly instead.
export const overProbFromSigma = (
	mean: number,
	line: number,
	sigma: number,
): number => 1 - normalCdf((line - mean) / Math.max(1e-6, sigma));

// Probability the actual total lands OVER a line, given the expected total.
// Uses a normal model whose spread scales with the total (more points → more
// variance). Symmetric around expected === line.
export const overProb = (expectedTotal: number, line: number): number =>
	overProbFromSigma(expectedTotal, line, Math.max(1, expectedTotal * 0.09));

// A half-point line placed right at a projection, so an over/under sits near a
// coin flip (the vig is what makes the house edge). e.g. 47.3 → 47.5.
export const toHalfPointLine = (projection: number): number =>
	Math.round(projection - 0.5) + 0.5;

// n-choose-k, for the series-win formula below (small n, no overflow concern).
const choose = (n: number, k: number): number => {
	if (k < 0 || k > n) {
		return 0;
	}
	let r = 1;
	for (let i = 0; i < k; i++) {
		r = (r * (n - i)) / (i + 1);
	}
	return r;
};

// Probability of winning a best-of-`bestOf` series given a per-game win
// probability. A series amplifies the favorite (0.6/game → ~0.71 in a best-of-7),
// which is why a strong team's title odds shouldn't be a naive per-game number.
export const seriesWinProb = (pGame: number, bestOf = 7): number => {
	const p = Math.min(0.999, Math.max(0.001, pGame));
	const winsNeeded = Math.ceil(bestOf / 2);
	let prob = 0;
	// Win the deciding game after conceding `losses` of the prior games.
	for (let losses = 0; losses < winsNeeded; losses++) {
		prob +=
			choose(winsNeeded - 1 + losses, losses) *
			p ** winsNeeded *
			(1 - p) ** losses;
	}
	return prob;
};

// Convert a set of team "strengths" into probabilities via a tempered softmax.
// Higher `power` concentrates probability on the strongest; used for
// championship / conference / division futures and (with a season-progress-
// scaled power) the single-winner award races. Returns probs in input order,
// summing to 1.
export const strengthProbs = (strengths: number[], power: number): number[] => {
	if (strengths.length === 0) {
		return [];
	}
	const mean = strengths.reduce((a, b) => a + b, 0) / strengths.length;
	const sd =
		Math.sqrt(
			strengths.reduce((a, b) => a + (b - mean) ** 2, 0) / strengths.length,
		) || 1;
	const weights = strengths.map((s) => Math.exp((power * (s - mean)) / sd));
	const sum = weights.reduce((a, b) => a + b, 0);
	return weights.map((w) => w / sum);
};

// Probability a team's season win total lands OVER a line, from its projected
// final wins and per-game win probability (season-long normal approximation).
export const winTotalOverProb = ({
	projectedWins,
	line,
	gamesTotal,
	winProb,
}: {
	projectedWins: number;
	line: number;
	gamesTotal: number;
	winProb: number;
}): number => {
	// Wins ~ Normal(projectedWins, sqrt(n p (1-p))) by the CLT over the season.
	const variance = Math.max(1, gamesTotal * winProb * (1 - winProb));
	const sigma = Math.sqrt(variance);
	// Continuity correction: a half-point line sits between integers.
	return 1 - normalCdf((line - projectedWins) / sigma);
};

// Award (MVP, etc.) probabilities from an ORDERED list of candidate "scores"
// (best first). A softened, normalized power of each score, with a floor so the
// tail still has non-zero (long-shot) odds. When scores are unavailable, pass a
// descending synthetic series (e.g. n, n-1, …) to price purely by rank.
export const awardProbsFromScores = (
	scoresBestFirst: number[],
	power = 3,
): number[] => {
	if (scoresBestFirst.length === 0) {
		return [];
	}
	const min = Math.min(...scoresBestFirst);
	const max = Math.max(...scoresBestFirst);
	const range = max - min || 1;
	// Normalize to 0..1, raise to a power to reward the leaders, add a small
	// floor so nobody is exactly 0.
	const weights = scoresBestFirst.map((s) => {
		const norm = (s - min) / range;
		return 0.02 + norm ** power;
	});
	const sum = weights.reduce((a, b) => a + b, 0);
	return weights.map((w) => w / sum);
};

// Probability each candidate ends up in each size-bounded TIER of a ranking,
// e.g. All-League 1st/2nd/3rd Team (5 players apiece) or an All-Rookie Team
// (one tier of 5) - a "will player X make team Y" market, as opposed to the
// single-winner races above. A seeded Monte Carlo: each simulated world jitters
// every candidate's real-formula score with normal noise (scaled to the
// field's own spread, so a blowout-strong field still occasionally upsets and
// a bunched field is genuinely competitive), re-ranks, and tallies which tier
// (if any) each candidate's rank lands in. `scores` need not be pre-sorted -
// ranking happens internally - and the result is returned in the SAME order as
// `scores`, one probability per tier per candidate.
export const tierMembershipProbs = (
	scores: number[],
	tierSizes: number[],
	{
		iterations = 4000,
		seed = 1,
		noiseFactor = 0.6,
	}: { iterations?: number; seed?: number; noiseFactor?: number } = {},
): number[][] => {
	const n = scores.length;
	const numTiers = tierSizes.length;
	if (n === 0 || numTiers === 0) {
		return scores.map(() => tierSizes.map(() => 0));
	}

	const mean = scores.reduce((a, b) => a + b, 0) / n;
	const sd =
		Math.sqrt(scores.reduce((a, b) => a + (b - mean) ** 2, 0) / n) || 1;
	// A bigger factor jitters scores more, so the field is more competitive (used
	// to widen odds early in a season, when scores mean less). Defaults to the
	// long-standing 0.6.
	const noiseScale = sd * noiseFactor;

	const cutoffs: number[] = [];
	{
		let acc = 0;
		for (const size of tierSizes) {
			acc += size;
			cutoffs.push(acc);
		}
	}

	const rand = mulberry32(seed);
	const counts: number[][] = scores.map(() => tierSizes.map(() => 0));
	const indices = scores.map((_, i) => i);

	for (let iter = 0; iter < iterations; iter++) {
		const noised = scores.map((s) => s + normalSample(rand) * noiseScale);
		const order = [...indices].sort((a, b) => noised[b]! - noised[a]!);
		for (const [rank, idx] of order.entries()) {
			for (let tier = 0; tier < numTiers; tier++) {
				const lo = tier === 0 ? 0 : cutoffs[tier - 1]!;
				const hi = cutoffs[tier]!;
				if (rank >= lo && rank < hi) {
					counts[idx]![tier]! += 1;
					break;
				}
				if (rank < lo) {
					break;
				}
			}
		}
	}

	return counts.map((row) => row.map((c) => c / iterations));
};

// --- Player/team prop pricing primitives -----------------------------------
// The formulas behind per-game player and team props (points/rebounds/assists
// over-unders, double-double/triple-double, team totals, overtime). Every one
// of these is projection (mean + variance) → probability via the SAME normal
// model already used for the whole-game total (overProb) - nothing here
// invents a new pricing philosophy, just applies it to more stats.

// Combine independent stat variances into one (e.g. a PRA prop's variance from
// its points/rebounds/assists variances). Assumes independence, the standard
// and defensible approximation absent a real covariance model - summing raw
// sigmas instead would systematically overstate a combo prop's true spread.
export const combineIndependentSigmas = (sigmas: number[]): number =>
	Math.sqrt(sigmas.reduce((sum, s) => sum + s * s, 0));

// P(a normal(mean, sigma) draw lands within `band` of zero) - the formula
// behind the "does this game go to overtime" prop: a team's final margin is
// modeled as Normal(expectedMargin, sigma) (see marginToWinProb), and a game
// that ends up within a make-or-two of tied at the buzzer is the one that can
// go to OT. A lopsided expectedMargin correctly pushes this toward 0; an
// even matchup correctly pushes it toward its peak.
export const probNear = (mean: number, sigma: number, band: number): number =>
	normalCdf((band - mean) / sigma) - normalCdf((-band - mean) / sigma);

// P(at least `need` of the given categories individually clear `threshold`),
// e.g. double-double = at least 2 of {pts, reb, ast, stl, blk} hitting 10 -
// the exact rule the game engine itself uses to flag dd/td (see
// worker/core/game/writePlayerStats.ts). A seeded Monte Carlo: each trial
// draws every category from its OWN Normal(mean, sigma) (clamped at 0 - a
// stat can't go negative) and tallies how many trials clear the bar in at
// least `need` categories. Assumes categories are independent, which slightly
// UNDERSTATES real correlation (a big offensive night nudges assists up too),
// so this is a conservative (not inflated) estimate - never the more
// exploitable direction for a "no freebies" line.
export const milestoneProb = (
	categories: { mean: number; sigma: number }[],
	threshold: number,
	need: number,
	{ iterations = 4000, seed = 1 }: { iterations?: number; seed?: number } = {},
): number => {
	if (categories.length === 0 || need > categories.length) {
		return 0;
	}
	const rand = mulberry32(seed);
	let hits = 0;
	for (let iter = 0; iter < iterations; iter++) {
		let cleared = 0;
		for (const c of categories) {
			const draw = Math.max(0, c.mean + normalSample(rand) * c.sigma);
			if (draw >= threshold) {
				cleared += 1;
			}
		}
		if (cleared >= need) {
			hits += 1;
		}
	}
	return hits / iterations;
};
