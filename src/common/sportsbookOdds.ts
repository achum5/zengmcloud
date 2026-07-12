import { bySport } from "./sportFunctions.ts";

// The pure probability models behind the sportsbook's live lines. Kept
// data-free and deterministic so they can be unit-tested and reused by the
// worker odds engine. Turning these probabilities into displayed American
// prices (with the house vig) is done in common/sportsbook.ts.

// Standard normal CDF via a numerical erf approximation (Abramowitz & Stegun
// 7.1.26). Good to ~1e-7, plenty for odds.
export const normalCdf = (z: number): number => {
	const sign = z < 0 ? -1 : 1;
	const x = Math.abs(z) / Math.SQRT2;
	const t = 1 / (1 + 0.3275911 * x);
	const y =
		1 -
		((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t -
			0.284496736) *
			t +
			0.254829592) *
			t *
			Math.exp(-x * x);
	return 0.5 * (1 + sign * y);
};

// Typical standard deviation of a game's final margin, per sport. Converts an
// expected point margin (from getGameSpread) into a win probability: a team
// favored by `margin` wins with probability Phi(margin / sigma).
const MARGIN_SIGMA = bySport({
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

// Expected total points for a game from each team's season scoring: blend how
// many each side usually scores with how many each usually allows. Falls back
// to the league-average total when a team has no scoring data yet.
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
	const homePts = ((homeFor ?? halfLeague) + (awayAgainst ?? halfLeague)) / 2;
	const awayPts = ((awayFor ?? halfLeague) + (homeAgainst ?? halfLeague)) / 2;
	return homePts + awayPts;
};

// Probability the actual total lands OVER a line, given the expected total.
// Uses a normal model whose spread scales with the total (more points → more
// variance). Symmetric around expected === line.
export const overProb = (expectedTotal: number, line: number): number => {
	const sigma = Math.max(1, expectedTotal * 0.09);
	return 1 - normalCdf((line - expectedTotal) / sigma);
};

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
// championship / conference / division futures. Returns probs in input order,
// summing to 1.
export const strengthProbs = (
	strengths: number[],
	power: number,
): number[] => {
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
