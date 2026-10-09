import { COURT_H, COURT_W, RIM_INSET } from "../courtSpots.ts";

// The 2.5D court lives in the same world as the 2D court - feet, x along the
// 94ft length, y across the 50ft width, display team 0 (away) attacking the
// LEFT rim and 1 (home) the RIGHT - plus z, the height off the floor.
export { COURT_H, COURT_W };

export type Side = 0 | 1;
export type Pt = { x: number; y: number };
export type Pt3 = { x: number; y: number; z: number };

export const RIM_Z = 10;
export const RIM_R = 0.75;

export const other = (t: Side): Side => (t === 0 ? 1 : 0);

// The rim a display team attacks, and which way along x that is.
export const rimX = (t: Side): number =>
	t === 0 ? RIM_INSET : COURT_W - RIM_INSET;
export const attackDir = (t: Side): 1 | -1 => (t === 1 ? 1 : -1);
export const rimPt = (t: Side, dz = 0): Pt3 => ({
	x: rimX(t),
	y: COURT_H / 2,
	z: RIM_Z + dz,
});

// A point given as depth from the attacked baseline and position across.
export const spot = (t: Side, depth: number, across: number): Pt => ({
	x: t === 0 ? depth : COURT_W - depth,
	y: across,
});

export const dist = (a: Pt, b: Pt): number => Math.hypot(a.x - b.x, a.y - b.y);

// Keep a body on the hardwood (or just off it, for an inbounder).
export const clampPt = (p: Pt, margin = 1): Pt => ({
	x: Math.min(COURT_W + 2 - margin, Math.max(-2 + margin, p.x)),
	y: Math.min(COURT_H + 1.5 - margin, Math.max(-1.5 + margin, p.y)),
});

// Keep a man in play inside the lines, feet and all: along the sideline as
// deep in the corner as a shooter stands, under the rim a step in from the
// baseline.
export const SIDELINE_GAP = 1.6;
export const inPlay = (p: Pt): Pt => ({
	x: Math.min(COURT_W - 1, Math.max(1, p.x)),
	y: Math.min(COURT_H - SIDELINE_GAP, Math.max(SIDELINE_GAP, p.y)),
});

// A defender meant for `p` against his man at `man`: where that would stand
// him squarely in front of his man from the camera (which looks straight
// across the floor from the near side), he plays him from the side instead -
// a step across toward `endX` (the end he defends), half as far in front -
// so both stay in sight.
export const sideOn = (man: Pt, p: Pt, endX: number, across = 1.6): Pt =>
	p.y > man.y && Math.abs(p.x - man.x) < across
		? {
				x: man.x + (endX >= man.x ? 1 : -1) * across,
				y: man.y + (p.y - man.y) * 0.5,
			}
		: p;

// Where a defender stands against his man: a step toward the rim he protects,
// pinched toward the middle of the floor.
export const guardSpot = (offense: Side, man: Pt, gap = 0.3): Pt =>
	sideOn(
		man,
		{
			x: man.x + (rimX(offense) - man.x) * gap,
			y: man.y + (COURT_H / 2 - man.y) * 0.25,
		},
		rimX(offense),
	);

// The scorer's table and the two benches sit along the far sideline, so a
// substitution or a huddle walks toward the camera's back wall.
export const TABLE: Pt = { x: COURT_W / 2, y: -2.2 };

// Each team's bench runs from beside the scorer's table toward its end of the
// floor, and every player keeps his own chair for the whole game - in roster
// order - sitting in it while he is not on the floor.
export const BENCH_SEATS = 15;
export const SEAT_GAP = 2.05;
export const BENCH_Y = -8.6;
export const benchStart = (t: Side): number => (t === 0 ? 5.5 : 58);
export const seatSpot = (t: Side, i: number): Pt => ({
	x: benchStart(t) + 0.78 + Math.min(i, BENCH_SEATS - 1) * SEAT_GAP,
	y: BENCH_Y + 0.9,
});
export const benchX = (t: Side): number => (t === 0 ? 31 : 63);
export const HUDDLE_Y = 1.6;
export const huddleSpots = (t: Side): Pt[] => {
	const cx = benchX(t);
	return [0, 1, 2, 3, 4].map((i) => {
		const a = (i / 5) * Math.PI * 2 + 0.4;
		return { x: cx + Math.cos(a) * 2.6, y: HUDDLE_Y + Math.sin(a) * 1.7 };
	});
};

// Free throw alignment, by depth from the shooting team's baseline: the
// defense takes the blocks nearest the rim, the offense the spots between.
export const FT_LINE_DEPTH = 19;
// The shooter toes the line from behind it.
export const FT_SHOOTER_DEPTH = FT_LINE_DEPTH + 1.15;
export const FT_DEFENSE: [number, number][] = [
	[7, 16.6],
	[7, 33.4],
	[14.5, 16.6],
];
export const FT_OFFENSE: [number, number][] = [
	[11, 33.4],
	[11, 16.6],
];
// About how long the officials can take to get to where the game next
// wants them (ms) - they run there (see crew.ts) - for whatever waits on
// one: the ball handed to the shooter at the line.
export const OFFICIALS_SETTLE = 4000;

// The official who hands the shooter the ball - the lead, under the basket
// - stands in the lane just in front of the rim, and holds it out toward
// the line.
export const FT_OFFICIAL: [number, number] = [5.2, 25.4];
export const ftOfficialBall = (t: Side): Pt3 => ({
	...spot(t, FT_OFFICIAL[0] + 0.85, FT_OFFICIAL[1]),
	z: 3.6,
});
export const FT_BACK: [number, number][] = [
	[27, 12],
	[27, 38],
	[30, 20],
	[30, 30],
];

// Where the j-th man not on the lane stands. FT_BACK covers a five-man game;
// a league that puts more on the floor (Number of Players on Court is a
// league setting) gets the extras spread out behind them, alternating sides,
// rather than nowhere at all - which is what crashed the free throw.
export const ftBackSpot = (k: number): [number, number] => {
	const fixed = FT_BACK[k];
	if (fixed) {
		return fixed;
	}
	const extra = k - FT_BACK.length;
	return [33 + 3 * Math.floor(extra / 2), extra % 2 === 0 ? 8 : 42];
};

// The j-th tallest defender and the j-th tallest of the shooter's teammates:
// the lane spots first, then back behind the arc.
export const ftDefenseSpot = (j: number): [number, number] =>
	FT_DEFENSE[j] ?? ftBackSpot(j - FT_DEFENSE.length);
export const ftOffenseSpot = (j: number): [number, number] =>
	FT_OFFENSE[j] ?? ftBackSpot(j);
