import {
	curveAt,
	OUTLOOK_YEARS,
	PATIENCE,
	type LeagueOutlook,
} from "./outlook.ts";
import { SLOT_MINUTES } from "./strength.ts";

// THE MARGIN OF ERROR on a deal (owner's call: a deal is only made if each
// side gains by more than this). Not the luck the model already prices in - a
// young player's spread of careers, a coin-flip re-signing - but how wrong its
// own reading can be:
//
//   - On-court value: what a player's ratings are worth, against fresh swap
//     tests in the sim (src/test/impact): 0.37 points a game at the sixth
//     man's minutes, about 0.8 per full game on the floor.
//   - Projections: how much further real careers land from the projection
//     than its own spread allows for, by seasons ahead, measured on every
//     player the front offices traded in arena leagues.
//
// Each moved player's error carries through every season he is expected to
// be there, weighted by his minutes and by how much a point of strength moves
// that team's title chances that season.

// Points a game, per full game on the floor.
const VALUE_ERROR = 0.8;

// Extra points a game per full game on the floor, by seasons ahead: real
// careers land 1.0, 1.8 and 2.2 points of overall further from the projection
// than its own spread allows, one to three seasons out (4,240 to 2,393
// traded players), growing about 0.4 a season after that, at about 0.3 points
// a game per full game for each point of overall.
const PROJECTION_ERROR: readonly number[] = [
	0, 0.3, 0.53, 0.66, 0.78, 0.9, 1.02, 1.14, 1.26, 1.38, 1.5,
];

// How many errors a gain must clear.
const MARGIN_Z = 1;

export type MovedPlayer = {
	// His place in the pecking order of the team he is priced for.
	place: number;
	// His chance of being there, each season of the outlook.
	availability: readonly number[];
};

// One side's margin, in the same units as franchiseValue. `path` is the team's
// strength each season, as it stands.
export const sideMargin = (
	outlook: LeagueOutlook,
	path: readonly number[],
	moved: readonly MovedPlayer[],
) => {
	let variance = 0;
	for (const p of moved) {
		const minutes = SLOT_MINUTES[Math.min(p.place, SLOT_MINUTES.length - 1)]!;
		let sd = 0;
		for (let y = 0; y <= OUTLOOK_YEARS; y++) {
			const curve = outlook.titleCurves[y]!;
			const q = curveAt(curve, path[y]!);
			const slope = curve.b * q * (1 - q);
			const projection = PROJECTION_ERROR[y] ?? PROJECTION_ERROR.at(-1)!;
			const error = Math.sqrt(VALUE_ERROR ** 2 + projection ** 2);
			sd += PATIENCE ** y * slope * minutes * (p.availability[y] ?? 0) * error;
		}
		variance += sd * sd;
	}
	return MARGIN_Z * Math.sqrt(variance);
};
