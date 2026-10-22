import { developSeasonSync } from "../player/developSeason.ts";
import ovr from "../player/ovr.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import { RATINGS } from "../../../common/constants.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import type { MinimalPlayerRatings } from "../../../common/types.ts";

export type RatingsProjection = {
	// Expected ratings for each season, this one first.
	ratings: MinimalPlayerRatings[];
	ovr: number[];
	// Spread of his overall that season across the simulated careers.
	ovrSd: number[];
	// measure averaged over the simulated careers, each season - not measure of
	// the average ratings, which undersells anyone whose future is uncertain
	// when value is convex. Empty without measure.
	measures: number[][];
	// Each season, the worse and the better half of the simulated careers by
	// his overall then: their average ratings, overall and measure. A roster
	// gives a player who turns out well the minutes of one who did, which an
	// average of the two undersells. Empty without measure.
	halves: {
		ratings: [MinimalPlayerRatings, MinimalPlayerRatings];
		ovr: [number, number];
		measures: [number[], number[]];
	}[];
};

// A player's ratings in the seasons ahead, played out with the game's own
// development code many times over and averaged. Seeded from the caller (a
// player and a season, say), so every device in a shared league projects the
// same player the same way, and asking again cannot change the answer.
//
// developSeasonSync draws from Math.random, so for the length of this
// synchronous block Math.random is the seeded stream; nothing else can run in
// between.
export const projectRatings = ({
	ratings,
	age,
	years,
	samples,
	seed,
	coachingLevel = DEFAULT_LEVEL,
	measure,
}: {
	ratings: MinimalPlayerRatings;
	age: number;
	years: number;
	samples: number;
	seed: number;
	coachingLevel?: number;
	measure?: (ratings: MinimalPlayerRatings) => number[];
}): RatingsProjection => {
	const sums = Array.from({ length: years + 1 }, () =>
		Object.fromEntries(RATINGS.map((key) => [key, 0])),
	);
	const ovrSums = new Array<number>(years + 1).fill(0);
	const ovrSumSqs = new Array<number>(years + 1).fill(0);
	const measureSums: number[][] = [];
	// Every career's ratings, overall and measure each season, for the halves.
	const careers: { ratings: number[]; ovr: number; measure: number[] }[][] =
		Array.from({ length: years + 1 }, () => []);

	const realRandom = Math.random;
	Math.random = makeSeededRandom(seed);
	try {
		for (let s = 0; s < samples; s++) {
			const copy = { ...ratings };
			for (let y = 0; y <= years; y++) {
				if (y > 0) {
					developSeasonSync(copy, age + y, coachingLevel, undefined);
				}
				for (const key of RATINGS) {
					sums[y]![key]! += (copy as any)[key];
				}
				const o = ovr(copy as any);
				ovrSums[y]! += o;
				ovrSumSqs[y]! += o * o;
				if (measure) {
					const m = measure({ ...copy, ovr: o });
					const sum = (measureSums[y] ??= new Array<number>(m.length).fill(0));
					for (let i = 0; i < m.length; i++) {
						sum[i]! += m[i]!;
					}
					careers[y]!.push({
						ratings: RATINGS.map((key) => (copy as any)[key] as number),
						ovr: o,
						measure: m,
					});
				}
			}
		}
	} finally {
		Math.random = realRandom;
	}

	return {
		ratings: sums.map(
			(row) =>
				({
					...ratings,
					...Object.fromEntries(
						RATINGS.map((key) => [key, row[key]! / samples]),
					),
				}) as MinimalPlayerRatings,
		),
		ovr: ovrSums.map((x) => x / samples),
		ovrSd: ovrSumSqs.map((x, y) =>
			Math.sqrt(Math.max(0, x / samples - (ovrSums[y]! / samples) ** 2)),
		),
		measures: measureSums.map((sum) => sum.map((x) => x / samples)),
		halves: measure
			? careers.map((season) => {
					const sorted = [...season].sort((a, b) => a.ovr - b.ovr);
					const mid = Math.floor(sorted.length / 2);
					const half = (list: typeof sorted) => {
						const n = Math.max(1, list.length);
						return {
							ratings: {
								...ratings,
								...Object.fromEntries(
									RATINGS.map((key, i) => [
										key,
										list.reduce((a, c) => a + c.ratings[i]!, 0) / n,
									]),
								),
							} as MinimalPlayerRatings,
							ovr: list.reduce((a, c) => a + c.ovr, 0) / n,
							measure: (list[0]?.measure ?? []).map(
								(_, i) => list.reduce((a, c) => a + c.measure[i]!, 0) / n,
							),
						};
					};
					const lo = half(sorted.slice(0, mid));
					const hi = half(sorted.slice(mid));
					return {
						ratings: [lo.ratings, hi.ratings],
						ovr: [lo.ovr, hi.ovr],
						measures: [lo.measure, hi.measure],
					};
				})
			: [],
	};
};
