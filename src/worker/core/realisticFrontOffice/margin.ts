import {
	curveAt,
	OUTLOOK_YEARS,
	outlookSeason,
	PATIENCE,
	strengthUncertainty,
	type LeagueOutlook,
	type PickLike,
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
//
// A pick is the player it is expected to become, but his on-court value and
// his career are averaged over every player who could be taken there, and
// over everywhere the pick may land, so the errors in those wash out. What
// does not is a misread of how good its team will be.

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
	// His place in the pecking order of the team he is priced for - each
	// season of the outlook, for a pick's player.
	place: number | readonly number[];
	// His chance of being there, each season of the outlook.
	availability: readonly number[];
	// For a pick: how far off its value can be each season, in points per
	// full game, in place of a player's errors.
	error?: readonly number[];
};

// For a pick: how many places in its round it may land from where the outlook
// expects because our read of how good its team will be is off. The outlook
// already prices how far a pick may land from luck (draftPlaceSpread); this is
// only the misread, by the outlook's own estimate of how far off its read of a
// team can be (strengthUncertainty), turned into places: the teams within that
// many points of it.
export const draftPlaceMisread = (outlook: LeagueOutlook, dp: PickLike) => {
	if (dp.pick > 0 || typeof dp.season !== "number") {
		return 0;
	}
	const y = Math.min(
		OUTLOOK_YEARS,
		Math.max(0, dp.season - outlookSeason(outlook, 0)),
	);
	const mine = outlook.teams.get(dp.originalTid)?.strength[y];
	if (mine === undefined) {
		return 0;
	}
	// This season's draft goes by the record too, and the games already played
	// are no misread.
	const thisSeason = outlook.offset === 0 && y === 0;
	const misread =
		strengthUncertainty(y, thisSeason ? outlook.progress : 0) *
		(thisSeason ? 1 - outlook.progress : 1);
	let near = 0;
	for (const t of outlook.teams.values()) {
		if (t.tid !== dp.originalTid && Math.abs(t.strength[y]! - mine) < misread) {
			near += 1;
		}
	}
	return near / 2;
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
		let sd = 0;
		for (let y = 0; y <= OUTLOOK_YEARS; y++) {
			const place = typeof p.place === "number" ? p.place : (p.place[y] ?? 0);
			const minutes = SLOT_MINUTES[Math.min(place, SLOT_MINUTES.length - 1)]!;
			const curve = outlook.titleCurves[y]!;
			const q = curveAt(curve, path[y]!);
			const slope = curve.b * q * (1 - q);
			const projection = PROJECTION_ERROR[y] ?? PROJECTION_ERROR.at(-1)!;
			const error =
				p.error?.[y] ?? Math.sqrt(VALUE_ERROR ** 2 + projection ** 2);
			sd += PATIENCE ** y * slope * minutes * (p.availability[y] ?? 0) * error;
		}
		variance += sd * sd;
	}
	return MARGIN_Z * Math.sqrt(variance);
};
