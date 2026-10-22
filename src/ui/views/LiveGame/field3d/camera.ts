import type { Camera } from "../court3d/camera.ts";
import { FIELD_W, MID_X, type Pt3 } from "./geometry.ts";

// THE BROADCAST CAMERA: high in the press box on the near side, sliding along
// with the ball the way the 3D court's camera does (see court3d/camera.ts,
// whose projection this shares) - framed tight on the line before the snap,
// opening up with the play.
//
// From that high a true perspective would flatten everybody into the turf,
// so like the court's main camera it is upright: the field is seen in
// perspective and everything on it stands straight up the screen at its full
// height.

export type FieldRig = {
	// How far back from the near sideline (feet), how high, how much it
	// slides along with the play (the rest it pans), and whether upright.
	back: number;
	high: number;
	slide: number;
	upright: boolean;
};
export const MAIN_RIG: FieldRig = {
	back: 230,
	high: 120,
	slide: 1,
	upright: true,
};
// The replay camera: down low on the near side, in true perspective - back
// past the edge of the turf, so the turf is all in front of it.
export const REPLAY_RIG: FieldRig = {
	back: 60,
	high: 22,
	slide: 1,
	upright: false,
};

// Where it looks: a point on the field (x, y), how wide a slice of the field
// fits across the picture there (feet), and how high (z, the turf if unset).
export type Shot = { x: number; width: number; y: number; z?: number };

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

export const makeFieldCamera = (
	shot: Shot,
	viewW: number,
	viewH: number,
	rig: FieldRig = MAIN_RIG,
): Camera => {
	const pos = {
		x: MID_X + (shot.x - MID_X) * rig.slide,
		y: FIELD_W + rig.back,
		z: rig.high,
	};
	const lift = shot.z ?? 2;
	const target = { x: shot.x, y: shot.y, z: rig.upright ? 0 : lift };
	const fwd = norm(sub(target, pos));
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
		// The point it looks at a little below the middle, leaving room above
		// for the far side's heads and a strip of the stands.
		cy:
			viewH * (viewW / viewH < 1.5 ? 0.5 : 0.55) +
			(rig.upright ? (lift * f) / depth : 0),
		viewW,
		viewH,
		upright: rig.upright,
	};
};
