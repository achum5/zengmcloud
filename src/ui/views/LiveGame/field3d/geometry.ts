import type { Pt, Pt3 } from "../court3d/geometry.ts";

export type { Pt, Pt3 };

// THE 3D FIELD'S WORLD: feet, like the 3D court's - x along the 120 yards from
// the back of the left end zone, y across the 53 1/3 from the FAR sideline
// (0) to the near one, the side the camera is on (FIELD_W), z up off the
// turf. Display team 0 is the visitors, 1 the home team, as on the court.
//
// Measurements are the NFL's (Rule 1): hash marks 70 ft 9 in in from each
// sideline, numbers 6 ft tall with their bottoms 12 yards in, goalposts
// 18 ft 6 in wide with the crossbar 10 ft up on the end lines.

export type Side = 0 | 1;

export const YD = 3;
export const FIELD_L = 120 * YD;
export const FIELD_W = 160;
export const END_ZONE = 10 * YD;
// The goal lines.
export const GOAL_L = END_ZONE;
export const GOAL_R = FIELD_L - END_ZONE;
export const MID_X = FIELD_L / 2;
export const MID_Y = FIELD_W / 2;

// Every line is 4 inches wide.
export const LINE_W = 4 / 12;
export const HASH_IN = 70.75;
export const HASH_FAR_Y = HASH_IN;
export const HASH_NEAR_Y = FIELD_W - HASH_IN;
export const HASH_LEN = 2;
export const NUMBER_H = 6;
export const NUMBER_W = 4;
// From each sideline in to the bottom of the numbers.
export const NUMBER_IN = 12 * YD;
// The white border round the outside of the lines, and the team areas and
// the stretch of grass beyond it to the wall.
export const BORDER = 6;
export const SIDE_ROOM = 42;
export const END_ROOM = 33;
// Each team's area on its sideline, between the 25-yard lines: the coaches'
// yellow line six feet behind the border, the benches at least thirty feet
// back - and elsewhere, the broken yellow line nine feet four inches behind
// the border (six feet behind it, past the end lines).
export const TEAM_AREA: [number, number] = [GOAL_L + 25 * YD, GOAL_R - 25 * YD];
export const COACH_LINE = BORDER + 6;
export const LIMIT_LINE = BORDER + 9 + 4 / 12;
export const LIMIT_END = BORDER + 6;
export const BENCH_OFF = 30.5;

export const POST_W = 18.5;
export const CROSSBAR_Z = 10;
export const UPRIGHT_Z = CROSSBAR_Z + 35;
// The gooseneck: the post comes up behind the end line and reaches forward.
export const POST_BACK = 6;

export const other = (t: Side): Side => (t === 0 ? 1 : 0);

export const dist = (a: Pt, b: Pt): number => Math.hypot(a.x - b.x, a.y - b.y);

// The way from one man to another he is meeting (a chest bump, a high
// five), turned if need be to no more than this far (radians) off straight
// along the field - so the broadcast, looking across it, sees the two side
// on rather than one behind the other.
const SIDE_ON = 0.6;
export const sideOn = (way: number): number => {
	const along = Math.cos(way) >= 0 ? 0 : Math.PI;
	const off = Math.atan2(Math.sin(way - along), Math.cos(way - along));
	return along + Math.min(SIDE_ON, Math.max(-SIDE_ON, off));
};

// Which way a team attacks: +1 toward the right end zone. The home team takes
// the right end in the first and third quarters and the left in the second
// and fourth (and odd overtimes like the first), as teams change ends at the
// quarter.
export type Dir = 1 | -1;
export const dirFor = (t: Side, quarter: number): Dir => {
	const homeRight = quarter % 2 === 1;
	return (t === 1) === homeRight ? 1 : -1;
};

// The sim's spot is yards from the offense's own goal line (0 to 100): where
// that is along the field, for an offense going `dir`.
export const yardLineX = (yards: number, dir: Dir): number =>
	dir === 1 ? GOAL_L + yards * YD : GOAL_R - yards * YD;

// A point given the way a coach gives it, from the ball: `depth` feet back
// from the line of scrimmage (negative: across it, into the defense) and
// `across` feet toward the near sideline from the ball.
export const fromBall = (
	losX: number,
	dir: Dir,
	ballY: number,
	depth: number,
	across: number,
): Pt => ({ x: losX - dir * depth, y: ballY + across });

// The 2D field counts yards with y = 0 at the NEAR sideline; the 3D field, in
// feet, from the far one.
export const fromFieldYards = (p: { x: number; y: number }): Pt => ({
	x: p.x * YD,
	y: FIELD_W - p.y * YD,
});

// Keep a man on the field (or just off it, for a sideline route or a man
// pushed out of bounds).
export const onField = (p: Pt, margin = 1): Pt => ({
	x: Math.min(FIELD_L - margin, Math.max(margin, p.x)),
	y: Math.min(FIELD_W - margin, Math.max(margin, p.y)),
});
