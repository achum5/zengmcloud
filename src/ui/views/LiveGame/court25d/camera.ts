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
// looking down on the half court the way an arcade game does, or the low
// replay camera courtside. `slide` is how far it tracks along the sideline
// with the play (the rest it pans). The main camera slides all the way,
// never turning - straight across the floor, like an arcade game's - which
// also lets the floor and the stands be drawn a row at a time (see
// planes.ts). It is upright: from that high, true perspective would squash
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
	high: 8.5,
	slide: 0.92,
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
// the line across the floor to aim at.
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
		const ys = new Map<number, number>();
		fit = {
			min: hi,
			y: (width) => {
				const w = Math.round(Math.max(hi, width) * 4) / 4;
				let y = ys.get(w);
				if (y === undefined) {
					y = aimY(a, w);
					ys.set(w, y);
				}
				return y;
			},
		};
		fits.set(key, fit);
	}
	return fit;
};
