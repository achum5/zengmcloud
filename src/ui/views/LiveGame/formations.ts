import { courtRandom } from "./courtRng.ts";
import type { Slot } from "./fieldSpots.ts";

// WHO IS ON THE FIELD, AND WHERE THEY STAND.
//
// A football play is decided twice before anybody moves: by the eleven each
// side sends out, and by where those eleven line up. Third and one is a
// different game from third and eleven, and it LOOKS different - a heavy set
// with two tight ends against a goal-line front, or empty against a dime. The
// field was drawing one offensive set and one defensive front for all of it.
//
// Every offensive scrimmage formation below is written in the SAME slot order,
// so a pass concept can name a job by its slot and reach the right man whatever
// the formation is:
//
//   0-4  the line, centre first, then guards, then tackles
//   5    the tight end
//   6    the quarterback
//   7    the back
//   8    the slot receiver
//   9    the receiver split to one side
//   10   the receiver split to the other
//
// In empty there is no back, so slot 7 is a fourth receiver; in a heavy set the
// receivers come in tight and slot 8 is a second tight end. The ORDER never
// changes, which is what keeps the playbook honest.

export type OffenseFormation = {
	name: string;
	slots: Slot[];
	// Nobody is left in to protect: every eligible man releases, so a concept
	// that asks the back to block has to be given something else to do.
	empty?: boolean;
	// Extra bodies instead of receivers. The quick game is off and the run is
	// downhill.
	heavy?: boolean;
};

// ELEVEN PERSONNEL, SHOTGUN: three receivers, a tight end, a back beside the
// quarterback. The set most football is played from.
const SHOTGUN: OffenseFormation = {
	name: "Shotgun",
	slots: [
		{ depth: 0, across: 0, pos: "OL" },
		{ depth: 0, across: -3, pos: "OL" },
		{ depth: 0, across: 3, pos: "OL" },
		{ depth: 0.3, across: -6.1, pos: "OL" },
		{ depth: 0.3, across: 6.1, pos: "OL" },
		{ depth: 0.6, across: 9.4, pos: "TE" },
		{ depth: 6, across: 0.4, pos: "QB" },
		{ depth: 6.8, across: -4.2, pos: "RB" },
		{ depth: 1.6, across: -11.5, pos: "WR" },
		{ depth: 0.6, across: -21.5, pos: "WR" },
		{ depth: 0.6, across: 21.5, pos: "WR" },
	],
};

// TRIPS: three receivers to one side, which is how an offense makes a defense
// declare what it is playing.
const TRIPS: OffenseFormation = {
	name: "Trips",
	slots: [
		{ depth: 0, across: 0, pos: "OL" },
		{ depth: 0, across: -3, pos: "OL" },
		{ depth: 0, across: 3, pos: "OL" },
		{ depth: 0.3, across: -6.1, pos: "OL" },
		{ depth: 0.3, across: 6.1, pos: "OL" },
		{ depth: 1.8, across: 12.5, pos: "TE" },
		{ depth: 6, across: 0.4, pos: "QB" },
		{ depth: 6.8, across: -3.6, pos: "RB" },
		{ depth: 1.4, across: 17.5, pos: "WR" },
		{ depth: 0.6, across: -21.5, pos: "WR" },
		{ depth: 0.6, across: 23, pos: "WR" },
	],
};

// BUNCH: three receivers stacked on top of each other, so the defenders
// covering them cannot get to them cleanly.
const BUNCH: OffenseFormation = {
	name: "Bunch",
	slots: [
		{ depth: 0, across: 0, pos: "OL" },
		{ depth: 0, across: -3, pos: "OL" },
		{ depth: 0, across: 3, pos: "OL" },
		{ depth: 0.3, across: -6.1, pos: "OL" },
		{ depth: 0.3, across: 6.1, pos: "OL" },
		{ depth: 1, across: 13, pos: "TE" },
		{ depth: 6, across: 0.4, pos: "QB" },
		{ depth: 6.8, across: -4, pos: "RB" },
		{ depth: 2.6, across: 15.5, pos: "WR" },
		{ depth: 0.6, across: -21.5, pos: "WR" },
		{ depth: 2.6, across: 10.5, pos: "WR" },
	],
};

// EMPTY: five out, nobody home. Everything the quarterback has is in front of
// him and none of it is protection.
const EMPTY: OffenseFormation = {
	name: "Empty",
	empty: true,
	slots: [
		{ depth: 0, across: 0, pos: "OL" },
		{ depth: 0, across: -3, pos: "OL" },
		{ depth: 0, across: 3, pos: "OL" },
		{ depth: 0.3, across: -6.1, pos: "OL" },
		{ depth: 0.3, across: 6.1, pos: "OL" },
		{ depth: 1.2, across: 12, pos: "TE" },
		{ depth: 6, across: 0, pos: "QB" },
		{ depth: 1.4, across: -12.5, pos: "RB" },
		{ depth: 1.4, across: 17, pos: "WR" },
		{ depth: 0.6, across: -22, pos: "WR" },
		{ depth: 0.6, across: 22.5, pos: "WR" },
	],
};

// SINGLEBACK, under centre: the standard running set with three receivers still
// on the field.
const SINGLEBACK: OffenseFormation = {
	name: "Singleback",
	slots: [
		{ depth: 0, across: 0, pos: "OL" },
		{ depth: 0, across: -3, pos: "OL" },
		{ depth: 0, across: 3, pos: "OL" },
		{ depth: 0.3, across: -6.1, pos: "OL" },
		{ depth: 0.3, across: 6.1, pos: "OL" },
		{ depth: 0.6, across: 9.4, pos: "TE" },
		{ depth: 2, across: 0, pos: "QB" },
		{ depth: 7.4, across: -0.6, pos: "RB" },
		{ depth: 3.2, across: -9.5, pos: "WR" },
		{ depth: 0.6, across: -20.5, pos: "WR" },
		{ depth: 0.6, across: 21, pos: "WR" },
	],
};

// I-FORMATION: a fullback in front of the tailback, both behind the
// quarterback. Downhill football.
const I_FORM: OffenseFormation = {
	name: "I-Form",
	slots: [
		{ depth: 0, across: 0, pos: "OL" },
		{ depth: 0, across: -3, pos: "OL" },
		{ depth: 0, across: 3, pos: "OL" },
		{ depth: 0.3, across: -6.1, pos: "OL" },
		{ depth: 0.3, across: 6.1, pos: "OL" },
		{ depth: 0.6, across: 9.4, pos: "TE" },
		{ depth: 2, across: 0, pos: "QB" },
		{ depth: 8.5, across: 0, pos: "RB" },
		{ depth: 5, across: 0, pos: "RB" },
		{ depth: 0.6, across: -20.5, pos: "WR" },
		{ depth: 0.6, across: 21, pos: "WR" },
	],
};

// HEAVY: two tight ends, a fullback, and one receiver who is mostly there to
// keep a corner honest. Short yardage and the goal line.
const HEAVY: OffenseFormation = {
	name: "Heavy",
	heavy: true,
	slots: [
		{ depth: 0, across: 0, pos: "OL" },
		{ depth: 0, across: -3, pos: "OL" },
		{ depth: 0, across: 3, pos: "OL" },
		{ depth: 0.3, across: -6.1, pos: "OL" },
		{ depth: 0.3, across: 6.1, pos: "OL" },
		{ depth: 0.6, across: 9.2, pos: "TE" },
		{ depth: 2, across: 0, pos: "QB" },
		{ depth: 7.5, across: 0, pos: "RB" },
		{ depth: 0.6, across: -9.2, pos: "TE" },
		{ depth: 0.8, across: -15.5, pos: "WR" },
		{ depth: 4.6, across: 3.4, pos: "RB" },
	],
};

export const OFFENSE_FORMATIONS = {
	shotgun: SHOTGUN,
	trips: TRIPS,
	bunch: BUNCH,
	empty: EMPTY,
	singleback: SINGLEBACK,
	iForm: I_FORM,
	heavy: HEAVY,
} satisfies Record<string, OffenseFormation>;

export type OffenseFormationName = keyof typeof OFFENSE_FORMATIONS;

// WHAT THE OFFENSE LINES UP IN. The situation decides most of it - nobody runs
// empty on the goal line and nobody runs a heavy set on third and twelve - and
// the rest is variety, so a drive does not look like the same snap eleven
// times.
export const chooseOffenseFormation = ({
	running,
	down,
	toGo,
	scrimmage,
}: {
	running: boolean;
	down: number;
	toGo: number;
	// Yards from the offense's own goal line, so the goal line can be
	// recognised.
	scrimmage: number;
}): OffenseFormation => {
	const goalLine = scrimmage >= 96;
	const shortYardage = toGo <= 2 || goalLine;
	if (shortYardage) {
		return courtRandom() < 0.65 ? HEAVY : I_FORM;
	}
	if (running) {
		const r = courtRandom();
		return r < 0.45 ? SINGLEBACK : r < 0.7 ? SHOTGUN : I_FORM;
	}
	// Obvious passing downs spread the field out.
	if (down >= 3 && toGo >= 8) {
		const r = courtRandom();
		return r < 0.35 ? EMPTY : r < 0.7 ? TRIPS : SHOTGUN;
	}
	const r = courtRandom();
	return r < 0.5 ? SHOTGUN : r < 0.72 ? TRIPS : r < 0.88 ? BUNCH : SINGLEBACK;
};

// ============================================================================
// THE OTHER SIDE OF THE BALL.
//
// A front is the eleven the defense has on the field and where they align.
// Which of them rush and what the rest do about the pass is the COVERAGE, and
// that lives in coverages.ts - a front and a coverage are chosen separately,
// which is exactly how it works in football: nickel can play any of them.
//
// Defensive slots are identified by their POSITION rather than their index,
// because the groupings differ - a nickel has five defensive backs and two
// linebackers where the base has four and three. Everything downstream reads
// `pos`, so no front can quietly break a coverage.
// ============================================================================

export type DefenseFront = {
	name: string;
	slots: Slot[];
};

const BASE_43: DefenseFront = {
	name: "4-3",
	slots: [
		{ depth: -1.9, across: -7.8, pos: "DL" },
		{ depth: -1.9, across: -2.9, pos: "DL" },
		{ depth: -1.9, across: 2.9, pos: "DL" },
		{ depth: -1.9, across: 8.2, pos: "DL" },
		{ depth: -6, across: -9.5, pos: "LB" },
		{ depth: -6.6, across: 0.6, pos: "LB" },
		{ depth: -6, across: 10.5, pos: "LB" },
		{ depth: -7.5, across: -21, pos: "CB" },
		{ depth: -7.5, across: 21.5, pos: "CB" },
		{ depth: -15, across: -11, pos: "S" },
		{ depth: -16.5, across: 12, pos: "S" },
	],
};

// NICKEL: a linebacker comes off for a fifth defensive back, who walks out over
// the slot. The answer to three receivers, and most of modern football.
const NICKEL: DefenseFront = {
	name: "Nickel",
	slots: [
		{ depth: -1.9, across: -7.8, pos: "DL" },
		{ depth: -1.9, across: -2.9, pos: "DL" },
		{ depth: -1.9, across: 2.9, pos: "DL" },
		{ depth: -1.9, across: 8.2, pos: "DL" },
		{ depth: -6, across: -5.5, pos: "LB" },
		{ depth: -6.2, across: 5.5, pos: "LB" },
		{ depth: -5.5, across: -12.5, pos: "NB" },
		{ depth: -7.5, across: -21, pos: "CB" },
		{ depth: -7.5, across: 21.5, pos: "CB" },
		{ depth: -15, across: -11, pos: "S" },
		{ depth: -16.5, across: 12, pos: "S" },
	],
};

// DIME: a sixth defensive back. Third and long, two minutes, and nothing else.
const DIME: DefenseFront = {
	name: "Dime",
	slots: [
		{ depth: -1.9, across: -7.4, pos: "DL" },
		{ depth: -1.9, across: -2.6, pos: "DL" },
		{ depth: -1.9, across: 2.6, pos: "DL" },
		{ depth: -1.9, across: 7.8, pos: "DL" },
		{ depth: -6.4, across: 0, pos: "LB" },
		{ depth: -5.5, across: -12.5, pos: "NB" },
		{ depth: -5.5, across: 13, pos: "NB" },
		{ depth: -8, across: -21.5, pos: "CB" },
		{ depth: -8, across: 22, pos: "CB" },
		{ depth: -16, across: -11, pos: "S" },
		{ depth: -17, across: 12, pos: "S" },
	],
};

// GOAL LINE: everybody in the box, nobody deep, because there is no deep left
// to defend.
const GOAL_LINE: DefenseFront = {
	name: "Goal Line",
	slots: [
		{ depth: -1.4, across: -9.5, pos: "DL" },
		{ depth: -1.4, across: -5.4, pos: "DL" },
		{ depth: -1.4, across: -1.8, pos: "DL" },
		{ depth: -1.4, across: 1.8, pos: "DL" },
		{ depth: -1.4, across: 5.4, pos: "DL" },
		{ depth: -1.4, across: 9.5, pos: "DL" },
		{ depth: -4.4, across: -4, pos: "LB" },
		{ depth: -4.4, across: 4, pos: "LB" },
		{ depth: -4.6, across: -13, pos: "LB" },
		{ depth: -6.5, across: -19.5, pos: "CB" },
		{ depth: -6.5, across: 20, pos: "CB" },
	],
};

export const DEFENSE_FRONTS = {
	base: BASE_43,
	nickel: NICKEL,
	dime: DIME,
	goalLine: GOAL_LINE,
} satisfies Record<string, DefenseFront>;

export type DefenseFrontName = keyof typeof DEFENSE_FRONTS;

// The defense answers the situation, and mostly it answers the personnel: three
// receivers get nickel, four get dime, the goal line gets everybody in the box.
export const chooseDefenseFront = ({
	offense,
	down,
	toGo,
	scrimmage,
}: {
	offense: OffenseFormation;
	down: number;
	toGo: number;
	scrimmage: number;
}): DefenseFront => {
	if (scrimmage >= 96 || (toGo <= 1 && offense.heavy)) {
		return GOAL_LINE;
	}
	if (offense.heavy) {
		return BASE_43;
	}
	if (offense.empty || (down >= 3 && toGo >= 10)) {
		return DIME;
	}
	if (offense.name === "I-Form" || offense.name === "Singleback") {
		return toGo <= 4 ? BASE_43 : NICKEL;
	}
	return NICKEL;
};

// ============================================================================
// SPECIAL TEAMS. Different units entirely, so they are written as their own
// alignments rather than as variations on a scrimmage formation.
// ============================================================================

export const PUNT_UNIT: Slot[] = [
	{ depth: 0, across: 0, pos: "OL" },
	{ depth: 0, across: -2.3, pos: "OL" },
	{ depth: 0, across: 2.3, pos: "OL" },
	{ depth: 0, across: -4.6, pos: "OL" },
	{ depth: 0, across: 4.6, pos: "OL" },
	{ depth: 0, across: -7, pos: "TE" },
	{ depth: 0, across: 7, pos: "TE" },
	{ depth: 0.3, across: -23, pos: "WR" },
	{ depth: 0.3, across: 23.5, pos: "WR" },
	{ depth: 5.5, across: -4.5, pos: "RB" },
	{ depth: 14, across: 0, pos: "P" },
];

export const PUNT_RETURN_UNIT: Slot[] = [
	{ depth: -1.6, across: -5.2, pos: "DL" },
	{ depth: -1.6, across: -1.4, pos: "DL" },
	{ depth: -1.6, across: 3, pos: "DL" },
	{ depth: -2.2, across: 7.4, pos: "DL" },
	{ depth: -5.5, across: -10.5, pos: "LB" },
	{ depth: -5.5, across: 11, pos: "LB" },
	{ depth: -9, across: -2.5, pos: "LB" },
	{ depth: -2.5, across: -22.5, pos: "CB" },
	{ depth: -2.5, across: 23, pos: "CB" },
	{ depth: -20, across: -9, pos: "S" },
	{ depth: -42, across: 0, pos: "S" },
];

export const KICK_UNIT: Slot[] = [
	{ depth: 0, across: 0, pos: "OL" },
	{ depth: 0, across: -2.2, pos: "OL" },
	{ depth: 0, across: 2.2, pos: "OL" },
	{ depth: 0, across: -4.4, pos: "OL" },
	{ depth: 0, across: 4.4, pos: "OL" },
	{ depth: 0, across: -6.6, pos: "TE" },
	{ depth: 0, across: 6.6, pos: "TE" },
	{ depth: 0, across: -9.2, pos: "WR" },
	{ depth: 0, across: 9.2, pos: "WR" },
	{ depth: 7.5, across: 0, pos: "RB" },
	{ depth: 10, across: -3.2, pos: "K" },
];

export const KICK_BLOCK_UNIT: Slot[] = [
	{ depth: -1.6, across: -6.6, pos: "DL" },
	{ depth: -1.6, across: -2.2, pos: "DL" },
	{ depth: -1.6, across: 2.2, pos: "DL" },
	{ depth: -1.6, across: 6.6, pos: "DL" },
	{ depth: -2.2, across: -10.6, pos: "DL" },
	{ depth: -2.2, across: 10.6, pos: "DL" },
	{ depth: -5, across: -15, pos: "LB" },
	{ depth: -5, across: 15, pos: "LB" },
	{ depth: -8, across: 0, pos: "LB" },
	{ depth: -12, across: -13, pos: "S" },
	{ depth: -12, across: 13, pos: "S" },
];

// A kickoff: ten men strung right across the width with the kicker behind them.
const KICK_COVER_ACROSS = [-24, -19, -13.5, -8, -3, 3, 8, 13.5, 19, 24];

export const kickoffCoverSlots = (): Slot[] => [
	...KICK_COVER_ACROSS.map((across) => ({ depth: 0, across, pos: "LB" })),
	{ depth: 7, across: 0, pos: "K" },
];

// The receiving team before the kick: a front wall, a second wall, and a
// returner deep.
export const kickoffReturnSlots = (returnerDepth: number): Slot[] =>
	[
		{ depth: -9, across: -22 },
		{ depth: -9, across: -11 },
		{ depth: -9, across: 0 },
		{ depth: -9, across: 11 },
		{ depth: -9, across: 22 },
		{ depth: -20, across: -16 },
		{ depth: -20, across: -5.5 },
		{ depth: -20, across: 5.5 },
		{ depth: -20, across: 16 },
		{ depth: -(returnerDepth - 6), across: -7 },
		{ depth: -returnerDepth, across: 0.5 },
	].map((slot) => ({ ...slot, pos: "RB" }));

// COVERING A RETURN. Once the ball is caught the kicking team is not a line any
// more - it is eleven men strung out down the field between the returner and
// where the kick came from, which is the shape that makes a return read as a
// return rather than as two teams standing on the same yard line.
export const kickChaseSlots = (): Slot[] =>
	[
		{ depth: -6, across: -13 },
		{ depth: -8, across: 4 },
		{ depth: -11, across: -3 },
		{ depth: -13, across: 15 },
		{ depth: -16, across: -19 },
		{ depth: -18, across: 8 },
		{ depth: -22, across: -8 },
		{ depth: -25, across: 20 },
		{ depth: -28, across: -22 },
		{ depth: -32, across: 2 },
		{ depth: -36, across: 12 },
	].map((slot) => ({ ...slot, pos: "LB" }));

// BLOCKING FOR A RETURN. The returner is placed by the play, so his ten
// teammates are the wedge ahead of him - offset from the chasers above so the
// two teams interleave rather than standing on each other.
export const returnBlockSlots = (): Slot[] =>
	[
		{ depth: -4, across: 9 },
		{ depth: -7, across: -8 },
		{ depth: -10, across: 17 },
		{ depth: -12, across: -16 },
		{ depth: -15, across: 3 },
		{ depth: -19, across: -12 },
		{ depth: -21, across: 13 },
		{ depth: -24, across: -2 },
		{ depth: -27, across: 22 },
		{ depth: -31, across: -20 },
		{ depth: -34, across: 7 },
	].map((slot) => ({ ...slot, pos: "RB" }));

// The kind of unit a scene needs, which is decided by what the play IS rather
// than by the situation.
export type FormationKind =
	| "pass"
	| "run"
	| "punt"
	| "kick"
	| "kickoff"
	| "kickoffReturn";

export const specialTeamsOffense = (kind: FormationKind): Slot[] | undefined => {
	switch (kind) {
		case "punt":
			return PUNT_UNIT;
		case "kick":
			return KICK_UNIT;
		case "kickoff":
			return kickoffCoverSlots();
		case "kickoffReturn":
			return returnBlockSlots();
		default:
			return undefined;
	}
};

export const specialTeamsDefense = (kind: FormationKind): Slot[] | undefined => {
	switch (kind) {
		case "punt":
			return PUNT_RETURN_UNIT;
		case "kick":
			return KICK_BLOCK_UNIT;
		case "kickoff":
			return kickoffReturnSlots(60);
		case "kickoffReturn":
			return kickChaseSlots();
		default:
			return undefined;
	}
};

// The default alignment for a kind of play, with no situation to read. The
// scene builder uses the situational choosers above for a play from scrimmage;
// this is what everything else (and every test that only cares about geometry)
// gets.
export const offenseSlots = (kind: FormationKind): Slot[] =>
	specialTeamsOffense(kind) ??
	(kind === "run" ? SINGLEBACK.slots : SHOTGUN.slots);

export const defenseSlots = (kind: FormationKind): Slot[] =>
	specialTeamsDefense(kind) ?? BASE_43.slots;
