import type { Player } from "../../../common/types.ts";
import { g, local } from "../../util/index.ts";
import { projectRatings } from "../realisticFrontOffice/projection.ts";
import type { Lane } from "./lanes.ts";

// TIMING (docs/vanilla-plus.md, "When a player helps"): vanilla's value is one
// number for a player's whole future, the same for every team. A team going
// for the title now cares about this season and next; a rebuilding one about
// three to six years out. So each lane weighs a player's seasons its own way,
// and his vanilla value is scaled by how much more or less that weighting
// gives than a typical team's.
//
// A player's seasons are played out with the game's own development code
// (realisticFrontOffice/projection.ts), and each season is worth vanilla's
// trade value of the player he is then (stars worth many good players). Only
// the ratio is used: the price level stays vanilla's, and how much a long
// shot at stardom is worth mostly cancels between the two weightings.
export const timingSettings = {
	// Seasons ahead counted, this one included.
	years: 8,
	// Simulated careers per player.
	samples: 32,
	// Each season counts this much less than the one before it, by lane.
	discount: { allIn: 0.5, playoff: 0.65, retool: 0.8, rebuild: 0.9 } as Record<
		Lane,
		number
	>,
	// A typical team's - the market's.
	marketDiscount: 0.75,
	// The ratio is kept within these.
	min: 0.05,
	max: 3,
};

// vanilla's trade value of a player's value (team/ValueChangeCalculator.ts):
// a z-score, raised to the 7th power above 1.
const tradeValue = (value: number) => {
	const z = (value - local.playerOvrMean) / local.playerOvrStd;
	return z > 1 ? z ** 7 : z;
};

// Each season's expected trade value, this one first; cached for the season.
const seasonsCache = new Map<string, number[]>();
let cacheFor = "";

export const seasonValues = (p: Player) => {
	const season = g.get("season");
	const key = `${g.get("lid")}:${season}`;
	if (key !== cacheFor) {
		seasonsCache.clear();
		cacheFor = key;
	}
	const r = p.ratings.at(-1)!;
	const id = `${p.pid}:${r.season}:${r.ovr}:${r.pot}`;
	let out = seasonsCache.get(id);
	if (!out) {
		const s = timingSettings;
		const proj = projectRatings({
			ratings: r as any,
			age: season - p.born.year,
			years: s.years - 1,
			samples: s.samples,
			// The same player the same season projects the same on every device.
			seed: p.pid * 7919 + season,
			keepCareers: true,
			measure: () => [],
		});
		// valueNoPot is what he's worth now (ratings and box score); each season
		// moves it by how his rating moves.
		out = proj.careers!.map(
			(careers) =>
				careers.reduce(
					(sum, c) =>
						sum + Math.max(0, tradeValue(p.valueNoPot + c.ovr - r.ovr)),
					0,
				) / careers.length,
		);
		seasonsCache.set(id, out);
	}
	return out;
};

const weighted = (seasons: number[], discount: number) => {
	let sum = 0;
	let weight = 0;
	for (const [t, x] of seasons.entries()) {
		sum += discount ** t * x;
		weight += discount ** t;
	}
	return sum / weight;
};

// How much more (or less) a team in this lane values him than the market,
// as a factor on his vanilla trade value.
export const timingFactor = (p: Player, lane: Lane | undefined) => {
	const s = timingSettings;
	if (!lane) {
		return 1;
	}
	const seasons = seasonValues(p);
	const market = weighted(seasons, s.marketDiscount);
	// Too little to weigh either way (a fringe player now and later).
	if (market < 0.05) {
		return 1;
	}
	const own = weighted(seasons, s.discount[lane]);
	return Math.min(s.max, Math.max(s.min, own / market));
};
