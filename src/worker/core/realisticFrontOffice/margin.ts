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
//   - On-court value: what a player's ratings are worth, against the swap
//     tests in the sim the model was fit on (src/test/impact), out of
//     sample, less the sim's own noise. The better the player, the further
//     off it can be.
//   - Projections: how much further real careers land from the projection
//     than its own spread allows for, by seasons ahead, measured on every
//     player the front offices traded in arena leagues - in points of
//     overall, worth more to the better player.
//
// Each moved player's error carries through every season he is expected to
// be there, weighted by his minutes and by how much a point of strength moves
// that team's title chances that season.
//
// A pick is the player it is expected to become, but his on-court value and
// his career are averaged over every player who could be taken there, and
// over everywhere the pick may land, so the errors in those wash out. What
// does not is a misread of how good its team will be.

// Points a game per full game on the floor, by overall: about 0.4 up to 52,
// then 0.037 more for each point (3,252 swaps at the sixth man's minutes:
// 0.4 at 50-55, 0.85 at 60-65, 1.3 at 70 and up).
const valueError = (ovr: number) => 0.4 + 0.037 * Math.max(0, ovr - 52);

// Points of overall, by seasons ahead: real careers land 1.0, 1.8 and 2.2
// points further from the projection than its own spread allows, one to
// three seasons out (4,240 to 2,393 traded players), growing about 0.4 a
// season after that.
const PROJECTION_ERROR: readonly number[] = [
	0, 1, 1.8, 2.2, 2.6, 3, 3.4, 3.8, 4.2, 4.6, 5,
];

// Points a game per full game on the floor that a point of overall is worth,
// by overall: the slope of what the same swaps were worth (0.23 at 50, 0.39 at
// 60, 0.63 at 70).
const perOvr = (ovr: number) =>
	3 * 1.4946e-4 * ovr ** 2 - 2 * 1.67468e-2 * ovr + 0.781991;

// How many errors a gain must clear. Settable for the arena.
export const marginSettings = { z: 1 };

export type MovedPlayer = {
	// His place in the pecking order of the team he is priced for, 0 for the
	// first - each season of the outlook (placeBySeason), or one place for
	// every season.
	place: number | readonly number[];
	// His chance of being there, each season of the outlook.
	availability: readonly number[];
	// His expected overall, each season of the outlook.
	ovr?: readonly number[];
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

// Where a player stands in the pecking order each season of the outlook: how
// many of today's roster are better than he is expected to be then (today's
// roster standing in for whoever holds those places by then, as for a pick).
// A prospect's error is in the minutes he is expected to play once he is
// good, not in the bench minutes he plays now - priced at today's place, a
// prospect who will start in three seasons carried almost no doubt, so
// trading him looked surer than trading the veteran he went for.
export const placeBySeason = (
	others: readonly number[],
	ovr: readonly number[],
) => ovr.map((mine) => others.filter((x) => x > mine).length);

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
			const ovr = p.ovr?.[y] ?? 52;
			const projection =
				(PROJECTION_ERROR[y] ?? PROJECTION_ERROR.at(-1)!) * perOvr(ovr);
			const error =
				p.error?.[y] ?? Math.sqrt(valueError(ovr) ** 2 + projection ** 2);
			sd += PATIENCE ** y * slope * minutes * (p.availability[y] ?? 0) * error;
		}
		variance += sd * sd;
	}
	return marginSettings.z * Math.sqrt(variance);
};
