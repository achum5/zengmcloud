import { COURT_H, COURT_W, type Pt3 } from "./geometry.ts";

// THE CAMERA.
//
// A broadcast camera high above the near sideline, panning and zooming to
// follow the play - a real perspective camera, so the far sideline is narrower
// than the near one, the rims rise above their spots on the floor, and a
// player running toward the camera grows.
//
// World units are feet: x along the court (0 at the left baseline), y across
// it (0 at the far sideline, 50 at the near one), z up off the floor.

export type Camera = {
	pos: Pt3;
	right: Pt3;
	up: Pt3;
	fwd: Pt3;
	// Focal length, in screen px.
	f: number;
	cx: number;
	cy: number;
	viewW: number;
	viewH: number;
	// Upright: the floor is seen in perspective, but everything standing on it
	// rises straight up the screen at its full height (see project).
	upright: boolean;
};

const sub = (a: Pt3, b: Pt3): Pt3 => ({
	x: a.x - b.x,
	y: a.y - b.y,
	z: a.z - b.z,
});
const dot = (a: Pt3, b: Pt3) => a.x * b.x + a.y * b.y + a.z * b.z;
const norm = (a: Pt3): Pt3 => {
	const l = Math.hypot(a.x, a.y, a.z) || 1;
	return { x: a.x / l, y: a.y / l, z: a.z / l };
};

// Where the camera stands: the main camera high above the near sideline,
// looking down on the half court the way an arcade game does, or the replay
// camera courtside - up in the first rows, high enough to see the play over
// the men on the near side of the floor (lower, they stood in front of it).
// `slide` is how far it tracks along the sideline with the play (the rest
// it pans). The main camera slides all the way, never turning - straight
// across the floor, like an arcade game's - which also lets the floor and
// the stands be drawn a row at a time (see planes.ts). It is upright: from that high, true perspective would squash
// every player into the floor, so the floor alone keeps it and the players,
// the ball and the baskets stand up on it at their full height - the way a
// cartoon court is drawn.
export type Rig = {
	back: number;
	high: number;
	slide: number;
	upright: boolean;
};
export const MAIN_RIG: Rig = { back: 70, high: 62, slide: 1, upright: true };
export const REPLAY_RIG: Rig = {
	back: 24,
	high: 14,
	slide: 0.92,
	upright: false,
};
// The replay's second angle, a dunk's slam: from the near corner past the
// baseline, low in the first rows behind it - looking up the lane at the
// rim, the backboard square over him. (It slides past the rim it looks at:
// round the end, not along the side.)
export const BASELINE_RIG: Rig = {
	back: 4,
	high: 11,
	slide: 1.5,
	upright: false,
};

// Where the camera looks: a point on the court (x), how wide a slice of the
// floor fits across the screen there (feet), how far across the court the
// middle of the picture is (y), and how high (z, the floor if unset).
export type Shot = { x: number; width: number; y: number; z?: number };

export const makeCamera = (
	shot: Shot,
	viewW: number,
	viewH: number,
	rig: Rig = MAIN_RIG,
): Camera => {
	// It slides along the sideline as well as panning, so the far end of the
	// floor is never seen at too steep an angle.
	const pos = {
		x: COURT_W / 2 + (shot.x - COURT_W / 2) * rig.slide,
		y: COURT_H + rig.back,
		z: rig.high,
	};
	// Upright, the camera aims at the floor under the point it looks at, and
	// the picture is moved down to bring the point itself to the middle.
	const lift = shot.z ?? 2;
	const target = { x: shot.x, y: shot.y, z: rig.upright ? 0 : lift };
	const fwd = norm(sub(target, pos));
	// Level: right is horizontal, up is square to both.
	const right = norm({ x: -fwd.y, y: fwd.x, z: 0 });
	let up = {
		x: fwd.y * right.z - fwd.z * right.y,
		y: fwd.z * right.x - fwd.x * right.z,
		z: fwd.x * right.y - fwd.y * right.x,
	};
	if (up.z < 0) {
		up = { x: -up.x, y: -up.y, z: -up.z };
	}
	const depth = dot(sub(target, pos), fwd);
	const f = (viewW * depth) / shot.width;
	return {
		pos,
		right,
		up: norm(up),
		fwd,
		f,
		cx: viewW / 2,
		// The point it looks at sits a little below the middle of the picture,
		// leaving room above for the rims and the stands - less so on a squarer
		// (phone) picture, which would otherwise be mostly crowd.
		cy:
			viewH * (viewW / viewH < 1.5 ? 0.47 : 0.56) +
			(rig.upright ? (lift * f) / depth : 0),
		viewW,
		viewH,
		upright: rig.upright,
	};
};

export type Projected = {
	x: number;
	y: number;
	depth: number;
	// Screen px per foot, at that depth.
	k: number;
};

export const project = (cam: Camera, p: Pt3): Projected => {
	if (cam.upright) {
		// The spot on the floor beneath it, in perspective; then straight up
		// the screen by its height, at the floor's scale there.
		const d = { x: p.x - cam.pos.x, y: p.y - cam.pos.y, z: -cam.pos.z };
		const depth = Math.max(0.5, dot(d, cam.fwd));
		const k = cam.f / depth;
		return {
			x: cam.cx + dot(d, cam.right) * k,
			y: cam.cy - (dot(d, cam.up) + p.z) * k,
			depth,
			k,
		};
	}
	const d = sub(p, cam.pos);
	const depth = Math.max(0.5, dot(d, cam.fwd));
	const k = cam.f / depth;
	return {
		x: cam.cx + dot(d, cam.right) * k,
		y: cam.cy - dot(d, cam.up) * k,
		depth,
		k,
	};
};

// How far in front of the camera a point is (negative: behind it) - upright,
// the spot on the floor beneath it, so what stands nearer is drawn over.
export const depthOf = (cam: Camera, p: Pt3): number =>
	cam.upright
		? dot({ x: p.x - cam.pos.x, y: p.y - cam.pos.y, z: -cam.pos.z }, cam.fwd)
		: dot(sub(p, cam.pos), cam.fwd);

// THE WHOLE FLOOR IN THE PICTURE.
//
// However the main camera follows the play, it never goes so tight that a
// sideline leaves the picture: the near sideline sits just above the bottom
// edge, and the far one, with the heads of the players on it, below the top.
// For a picture of a given shape (width over height) that sets the narrowest
// shot across the floor there can be, and - for any shot at least that wide -
// the line across the floor to aim at. (In tighter than that - a free throw,
// when nothing is going on out by the sidelines - it zooms in on the middle
// of the floor: the line down the middle of it kept where the whole floor
// has it.)
const FIT_TOP = 0.05;
const FIT_BOTTOM = 0.985;

const edgesAt = (aspect: number, width: number, y: number) => {
	const cam = makeCamera({ x: COURT_W / 2, width, y }, aspect * 100, 100);
	return {
		top: project(cam, { x: COURT_W / 2, y: -0.5, z: 7 }).y / 100,
		bottom: project(cam, { x: COURT_W / 2, y: COURT_H + 0.5, z: 0 }).y / 100,
	};
};

// Aiming nearer the camera moves the floor up the picture: the aim that
// puts the near sideline right at the bottom.
const aimY = (aspect: number, width: number): number => {
	let lo = -10;
	let hi = COURT_H + 10;
	for (let i = 0; i < 28; i++) {
		const mid = (lo + hi) / 2;
		if (edgesAt(aspect, width, mid).bottom > FIT_BOTTOM) {
			lo = mid;
		} else {
			hi = mid;
		}
	}
	return (lo + hi) / 2;
};

// In tighter than the whole floor: the aim that keeps the line down the
// middle of the floor `at` (a share of the way down the picture) - where
// the whole floor has it, and up to TIGHT_LOWER of the picture lower by a
// shot TIGHT_OVER of the whole floor's width tighter.
const TIGHT_LOWER = 0.085;
const TIGHT_OVER = 0.33;
const tightY = (aspect: number, width: number, at: number): number => {
	let lo = -10;
	let hi = COURT_H + 10;
	for (let i = 0; i < 28; i++) {
		const mid = (lo + hi) / 2;
		const cam = makeCamera(
			{ x: COURT_W / 2, width, y: mid },
			aspect * 100,
			100,
		);
		if (project(cam, { x: COURT_W / 2, y: COURT_H / 2, z: 0 }).y / 100 > at) {
			lo = mid;
		} else {
			hi = mid;
		}
	}
	return (lo + hi) / 2;
};

export type CourtFit = { min: number; y: (width: number) => number };
const fits = new Map<number, CourtFit>();
export const courtFit = (aspect: number): CourtFit => {
	const key = Math.round(aspect * 100);
	let fit = fits.get(key);
	if (!fit) {
		const a = key / 100;
		// Wider shows the far side's heads lower in the picture.
		let lo = 10;
		let hi = 200;
		for (let i = 0; i < 28; i++) {
			const mid = (lo + hi) / 2;
			if (edgesAt(a, mid, aimY(a, mid)).top < FIT_TOP) {
				lo = mid;
			} else {
				hi = mid;
			}
		}
		const min = hi;
		const ys = new Map<number, number>();
		const tight = new Map<number, number>();
		let middle: number | undefined;
		fit = {
			min,
			y: (width) => {
				if (width >= min) {
					const w = Math.round(width * 4) / 4;
					let y = ys.get(w);
					if (y === undefined) {
						y = aimY(a, Math.max(min, w));
						ys.set(w, y);
					}
					return y;
				}
				if (middle === undefined) {
					const cam = makeCamera(
						{ x: COURT_W / 2, width: min, y: aimY(a, min) },
						a * 100,
						100,
					);
					middle =
						project(cam, { x: COURT_W / 2, y: COURT_H / 2, z: 0 }).y / 100;
				}
				const w = Math.round(width * 4) / 4;
				let y = tight.get(w);
				if (y === undefined) {
					// (In closer, the middle of the floor a little lower in the
					// picture, for room over the rim.)
					const k = Math.min(1, Math.max(0, (min - w) / (min * TIGHT_OVER)));
					y = tightY(a, Math.min(min, w), middle + TIGHT_LOWER * k);
					tight.set(w, y);
				}
				return y;
			},
		};
		fits.set(key, fit);
	}
	return fit;
};

// THE OPERATOR: where the camera is looking, moving on toward where it is
// asked to look (a Shot) a frame at a time - an operator's pan and zoom,
// eased into and out of, never a lurch when what it is following jumps -
// or, at a cut, straight there.
//
// How quickly it pans and zooms after what it follows (per second): as
// quick as a first look's lag would be, but getting going and pulling up
// smoothly.
export const CAM_PAN = 5.6;
export const CAM_ZOOM = 3.2;
// Critically damped: from where it is and how fast it is going (`v`), on
// toward `to` over `secs` - exact, however long the step.
export const follow = (
	x: number,
	v: number,
	to: number,
	w: number,
	secs: number,
): [number, number] => {
	const y = x - to;
	const e = Math.exp(-w * secs);
	const k = (v + w * y) * secs;
	return [to + (y + k) * e, (v - w * k) * e];
};

// Where it looks along the floor, how wide, across the floor and how high,
// and how fast each is changing (a second).
export type Operator = {
	camX: number;
	camW: number;
	camY: number;
	camZ: number;
	camVX: number;
	camVW: number;
	camVY: number;
	camVZ: number;
};

// On toward `aim` over `secs` (straight there on a cut). Following the play
// (`fit` given), the whole floor stays in the picture - never tighter than
// it but for a shot that asks to be (a free throw), never further past
// either end than the stands behind it, and aimed across the floor as the
// whole floor needs. A look round the building and a replay go where they
// ask, up and down and across the floor too.
export const operate = (
	op: Operator,
	aim: Shot,
	fit: CourtFit | undefined,
	secs: number,
	cut: boolean,
) => {
	if (cut) {
		op.camX = aim.x;
		op.camW = aim.width;
		op.camY = aim.y;
		op.camZ = aim.z ?? 2;
		op.camVX = 0;
		op.camVW = 0;
		op.camVY = 0;
		op.camVZ = 0;
	} else {
		[op.camX, op.camVX] = follow(op.camX, op.camVX, aim.x, CAM_PAN, secs);
		[op.camW, op.camVW] = follow(op.camW, op.camVW, aim.width, CAM_ZOOM, secs);
		[op.camY, op.camVY] = follow(op.camY, op.camVY, aim.y, CAM_ZOOM, secs);
		[op.camZ, op.camVZ] = follow(op.camZ, op.camVZ, aim.z ?? 2, CAM_ZOOM, secs);
	}
	if (fit) {
		op.camW = Math.max(op.camW, Math.min(fit.min, aim.width));
		op.camX = Math.min(
			COURT_W + 12 - op.camW / 2,
			Math.max(op.camW / 2 - 12, op.camX),
		);
		// (Where the whole floor has it look, for whatever comes next
		// without a cut to start from.)
		op.camY = fit.y(op.camW);
		op.camZ = aim.z ?? 2;
		op.camVY = 0;
		op.camVZ = 0;
	}
};

// THE SCORE BUG IN THE WAY: its box as shares of the picture (its left and
// right edges, and its top; it goes on down to the foot of the picture),
// and how far (feet, + to the right) the camera pans for the man with the
// ball to stand clear of it - never so far that the basket at his end
// leaves the picture, and the way it went last (`side`: -1 left, 1 right, 0
// neither) kept to unless the other way is much better.
export type Cover = { x0: number; x1: number; y0: number };
const DODGE_MAX = 20;
export const dodgeFor = (
	cam: Camera,
	at: { x: number; y: number },
	box: Cover,
	side: number,
): [number, number] => {
	const p = project(cam, { x: at.x, y: at.y, z: 3 });
	// Eased in as he goes down behind its top edge.
	const under = Math.min(1, Math.max(0, (p.y / cam.viewH - box.y0) / 0.07 + 1));
	const m = 0.02 + (1.5 * p.k) / cam.viewW;
	const fx = p.x / cam.viewW;
	// How far across the picture he would go to come out its left side, or
	// its right.
	const left = fx - (box.x0 - m);
	const right = box.x1 + m - fx;
	if (under <= 0 || left <= 0 || right <= 0) {
		return [0, 0];
	}
	// The camera moved d feet takes everything d * k across the other way.
	const rim = project(cam, {
		x: at.x < COURT_W / 2 ? 5.25 : COURT_W - 5.25,
		y: COURT_H / 2,
		z: 10,
	});
	const rimX = rim.x / cam.viewW;
	const per = cam.viewW / rim.k;
	const lo = Math.min(0, Math.max(-DODGE_MAX, (rimX - 0.92) * per));
	const hi = Math.max(0, Math.min(DODGE_MAX, (rimX - 0.08) * per));
	let best = 0;
	let bestScore = Infinity;
	for (const go of [-1, 1]) {
		if (go < 0 ? box.x0 - m < 0.06 : box.x1 + m > 0.94) {
			continue;
		}
		const need = (((go < 0 ? left : right) * cam.viewW) / p.k) * under;
		const d = go < 0 ? Math.min(hi, need) : Math.max(lo, -need);
		// Mostly how much of him is left behind it; then how far it pans.
		const score =
			3 * (need - Math.abs(d)) + Math.abs(d) - (go === side ? 6 : 0);
		if (score < bestScore) {
			bestScore = score;
			best = go;
		}
	}
	if (best === 0) {
		return [0, 0];
	}
	const need = (((best < 0 ? left : right) * cam.viewW) / p.k) * under;
	return [best < 0 ? Math.min(hi, need) : Math.max(lo, -need), best];
};
