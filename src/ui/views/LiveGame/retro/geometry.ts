import { COURT_H, COURT_W, RIM_INSET } from "../courtSpots.ts";

// The retro court lives in the same world as the 2D court - feet, x along the
// 94ft length, y across the 50ft width, display team 0 (away) attacking the
// LEFT rim and 1 (home) the RIGHT - plus z, the height off the floor.
export { COURT_H, COURT_W };

export type Side = 0 | 1;
export type Pt = { x: number; y: number };
export type Pt3 = { x: number; y: number; z: number };

export const RIM_Z = 10;
export const RIM_R = 0.75;
export const BACKBOARD_INSET = 4;

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

export const lerpPt = (a: Pt, b: Pt, f: number): Pt => ({
	x: a.x + (b.x - a.x) * f,
	y: a.y + (b.y - a.y) * f,
});

// Keep a body on the hardwood (or just off it, for an inbounder).
export const clampPt = (p: Pt, margin = 1): Pt => ({
	x: Math.min(COURT_W + 2 - margin, Math.max(-2 + margin, p.x)),
	y: Math.min(COURT_H + 1.5 - margin, Math.max(-1.5 + margin, p.y)),
});

// Where a defender stands against his man: a step toward the rim he protects,
// pinched toward the middle of the floor.
export const guardSpot = (offense: Side, man: Pt, gap = 0.3): Pt => ({
	x: man.x + (rimX(offense) - man.x) * gap,
	y: man.y + (COURT_H / 2 - man.y) * 0.25,
});

// The scorer's table and the two benches sit along the far sideline, so a
// substitution or a huddle walks toward the camera's back wall.
export const TABLE: Pt = { x: COURT_W / 2, y: -2.2 };
export const benchX = (t: Side): number => (t === 0 ? 31 : 63);
export const huddleSpots = (t: Side): Pt[] => {
	const cx = benchX(t);
	return [0, 1, 2, 3, 4].map((i) => {
		const a = (i / 5) * Math.PI * 2 + 0.4;
		return { x: cx + Math.cos(a) * 2.4, y: 1.6 + Math.sin(a) * 1.4 };
	});
};

// Free throw alignment, by depth from the shooting team's baseline: the
// defense takes the blocks nearest the rim, the offense the spots between.
export const FT_LINE_DEPTH = 19;
export const FT_DEFENSE: [number, number][] = [
	[7, 16.6],
	[7, 33.4],
	[14.5, 16.6],
];
export const FT_OFFENSE: [number, number][] = [
	[11, 33.4],
	[11, 16.6],
];
export const FT_BACK: [number, number][] = [
	[27, 12],
	[27, 38],
	[30, 20],
	[30, 30],
];

// The camera. A raised sideline camera, Hoop Land style: the floor is squashed
// in depth, heights mostly survive, and the far side of the floor is a little
// narrower than the near side. Units are screen pixels of the low-res buffer.
export const VIEW_H = 216;
export const K = 6.4; // px per foot along the court at the near sideline
export const DEPTH = 0.4; // vertical squash of court depth
export const ZF = 0.92; // vertical scale of height
export const FAR_Y = 72; // buffer row of the far sideline
export const PX_PER_FT = K * ZF; // sprite pixels per foot of height
export const persp = (y: number): number => 0.84 + 0.16 * (y / COURT_H);

// Narrower screens get a tighter camera, so the players are not specks on a
// phone. Height is fixed; the width (how much floor is visible) changes.
export const viewWidthFor = (cssWidth: number): number =>
	cssWidth < 420 ? 288 : cssWidth < 700 ? 320 : 384;

export const project = (
	viewW: number,
	camX: number,
	x: number,
	y: number,
	z: number,
): { x: number; y: number; s: number } => {
	const s = persp(y);
	return {
		x: viewW / 2 + (x - camX) * K * s,
		y: FAR_Y + y * K * DEPTH - z * PX_PER_FT,
		s,
	};
};

// How far the camera may pan before it would show past the stanchions.
export const camLimits = (viewW: number): [number, number] => {
	const half = viewW / 2 / (K * persp(COURT_H / 2));
	return [half - 6, COURT_W - half + 6];
};
