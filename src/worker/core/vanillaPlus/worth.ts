import { PHASE, PLAYER } from "../../../common/constants.ts";
import type { Player } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g, local } from "../../util/index.ts";
import getPickValues from "../trade/getPickValues.ts";
import { projectRatings } from "../realisticFrontOffice/projection.ts";
import type { Lane } from "./lanes.ts";

// WORTH (docs/vanilla-plus.md, "Worth, season by season"): what a player is
// worth in a trade, on vanilla's scale, counted one season at a time.
//
// Vanilla prices a player with one number - his rating and potential blended
// by age - raised to the 7th power, so one star is worth many good players.
// But it blends before it raises: a star who is great now and fading later
// is priced as his average, and a teenager by a potential rating that sits
// about five points above how careers actually turn out. Here each season is
// priced on its own (the player he is that season, by vanilla's 7th power)
// and the seasons are added up, each counting less than the one before.
//
// His seasons are played out with the game's own development code
// (realisticFrontOffice/projection.ts), many careers each, so a chance at
// stardom counts for what it's worth.
//
// How much each season counts is the team's horizon. The market's is a
// steady decline, set so that players in their prime cost what vanilla says.
// A lane's is its window: an all-in team counts this season and next (and a
// little of the one after) and nothing past it - a 17-year-old's chance of
// stardom in year eight is no help to it, however big - while a rebuilding
// team counts the seasons it means to win in above this one.
const geometric = (d: number, n: number) =>
	Array.from({ length: n }, (_, i) => d ** i);
export const worthSettings = {
	// Seasons counted, the coming one first.
	years: 10,
	// Simulated careers per player.
	samples: 40,
	// Each lane's weight on each season, the coming one first.
	weights: {
		allIn: [1, 0.6, 0.25],
		playoff: [1, 0.75, 0.5, 0.3, 0.15],
		retool: geometric(0.75, 10),
		rebuild: [0.5, 0.7, 0.9, 1, 1, 1, 0.9, 0.8, 0.7, 0.6],
	} as Record<Lane, number[]>,
	market: geometric(0.65, 10),
};

// Vanilla's (team/ValueChangeCalculator.ts): a value's z-score, and its trade
// value - the z-score raised to the 7th power above 1.
const zscore = (value: number) =>
	(value - local.playerOvrMean) / local.playerOvrStd;
const tradeValue = (z: number) => (z > 1 ? z ** 7 : z);
// Back from a trade value to the z-score that gives it.
export const zOf = (tv: number) => (tv > 1 ? tv ** (1 / 7) : tv);

// The season "now" is: in the offseason, the coming one.
const nowIndex = () => (g.get("phase") > PHASE.PLAYOFFS ? 1 : 0);

// Gauss-Hermite nodes and weights for an expectation over a standard normal
// (10 points, each used either side of zero).
const HERMITE = [
	[0.342901327223705, 0.610862633735326],
	[1.036610829789514, 0.240138611082315],
	[1.756683649299882, 0.033874394455481],
	[2.53273167423279, 0.001343645746781],
	[3.436159118837738, 0.000007640432855],
].map(([x, w]) => [x! * Math.SQRT2, w! / Math.sqrt(Math.PI)] as const);

const cache = new Map<string, number[]>();
let cacheFor = "";

// Each season's expected trade value from this one on, by the game's own
// development; cached for the season.
export const seasonValues = (p: Player, years: number) => {
	const season = g.get("season");
	const key = `${g.get("lid")}:${season}`;
	if (key !== cacheFor) {
		cache.clear();
		cacheFor = key;
	}
	const r = p.ratings.at(-1)!;
	const id = `${p.pid}:${r.season}:${r.ovr}:${r.pot}:${years}`;
	let out = cache.get(id);
	if (!out) {
		const proj = projectRatings({
			ratings: r as any,
			age: season - p.born.year,
			years: years - 1,
			samples: worthSettings.samples,
			// The same player the same season projects the same on every device.
			seed: p.pid * 7919 + season,
		});
		// valueNoPot is what he's worth now (ratings and box score, on vanilla's
		// value scale); each season moves it by how his rating moves from the
		// projection's own start. Below average stays below zero, as vanilla's
		// does, so a fringe player is still worth less than a better one.
		//
		// Each season's spread of careers, not the careers themselves: a few
		// dozen careers put a long shot's value at the mercy of one or two
		// superstar draws, which the 7th power magnifies and sorting a draft
		// class by it would select for. So the careers give each season's
		// average and spread, and the worth is the exact expectation over that
		// spread (Gauss-Hermite).
		const scale = 10 / local.playerOvrStd;
		const start = proj.ovr[0]!;
		const at = (ovr: number) =>
			tradeValue(zscore(p.valueNoPot + (ovr - start) * scale));
		out = proj.ovr.map((mean, t) => {
			const sd = proj.ovrSd[t]!;
			let sum = 0;
			for (const [x, w] of HERMITE) {
				sum += w * (at(mean + sd * x) + at(mean - sd * x));
			}
			return sum;
		});
		cache.set(id, out);
	}
	return out;
};

// His worth with these weights, as a trade value: a weighted average of his
// seasons from the coming one, counting only those from `first` on (a draft
// prospect plays only once drafted) - so seasons he can't play count as
// nothing.
export const worth = (p: Player, weights: number[], first = 0) => {
	const now = nowIndex();
	const seasons = seasonValues(p, now + worthSettings.years);
	let sum = 0;
	let weight = 0;
	for (const [i, w] of weights.entries()) {
		const t = now + i;
		weight += w;
		if (t >= first) {
			sum += w * (seasons[t] ?? 0);
		}
	}
	return sum / weight;
};

// His worth to a team in this lane, as a trade value.
export const worthFor = (p: Player, lane: Lane) =>
	worth(p, worthSettings.weights[lane]);

// What a team in this lane would pay for him, and what the market would, as
// vanilla z-scores (team/ValueChangeCalculator.ts playerWorth).
export const playerWorth = (p: Player, lane: Lane | undefined) => {
	const market = zOf(worth(p, worthSettings.market));
	const own = lane ? zOf(worth(p, worthSettings.weights[lane])) : market;
	return { own, market };
};

// A draft prospect's market worth, on vanilla's value scale (for the pick
// table, trade/getPickValues.ts): he plays from the season after his draft.
export const prospectValue = (p: Player, draftYear = p.draft.year) => {
	const first = draftYear + 1 - g.get("season");
	const z = zOf(worth(p, worthSettings.market, first));
	return z * local.playerOvrStd + local.playerOvrMean;
};

// The pick table on the same scale (trade/getPickValues.ts, which values the
// prospects already in the league). A draft further off than any class yet
// generated gets the farthest class's prospects, as if drafted then: stock
// would give it the average of the nearer classes, which counts seasons a
// pick that far off can't help in.
export const pickValues = async () => {
	const table = await getPickValues(prospectValue);
	const classes = Object.keys(table)
		.filter((key) => key !== "default")
		.map(Number)
		.sort((a, b) => a - b);
	const farthest = classes.at(-1);
	if (farthest === undefined) {
		return table;
	}
	const prospects = (
		await idb.cache.players.indexGetAll("playersByTid", PLAYER.UNDRAFTED)
	).filter((p) => p.draft.year === farthest);
	const seasons = new Set(
		(await idb.cache.draftPicks.getAll())
			.map((dp) => dp.season)
			.filter((season): season is number => typeof season === "number"),
	);
	for (const season of seasons) {
		if (season > farthest && table[season] === undefined) {
			table[season] = prospects
				.map((p) => prospectValue(p, season))
				.sort((a, b) => b - a);
		}
	}
	return table;
};
