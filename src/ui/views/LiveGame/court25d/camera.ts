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

// How far back from the near sideline and how high the camera sits.
export const CAM_BACK = 64;
export const CAM_HIGH = 33;

// Where the camera looks: a point on the court (x), how wide a slice of the
// floor fits across the screen there (feet), and how far across the court the
// middle of the picture is (y).
export type Shot = { x: number; width: number; y: number };

export const makeCamera = (
	shot: Shot,
	viewW: number,
	viewH: number,
): Camera => {
	// It slides along the sideline as well as panning, so the far end of the
	// floor is never seen at too steep an angle.
	const pos = {
		x: COURT_W / 2 + (shot.x - COURT_W / 2) * 0.55,
		y: COURT_H + CAM_BACK,
		z: CAM_HIGH,
	};
	const target = { x: shot.x, y: shot.y, z: 2 };
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
	return {
		pos,
		right,
		up: norm(up),
		fwd,
		f: (viewW * depth) / shot.width,
		cx: viewW / 2,
		// The point it looks at sits a little below the middle of the picture,
		// leaving room above for the rims and the stands - less so on a squarer
		// (phone) picture, which would otherwise be mostly crowd.
		cy: viewH * (viewW / viewH < 1.5 ? 0.47 : 0.56),
		viewW,
		viewH,
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

// How far in front of the camera a point is (negative: behind it).
export const depthOf = (cam: Camera, p: Pt3): number =>
	dot(sub(p, cam.pos), cam.fwd);

// The CSS transform that lays a flat element (its top-left at `origin`, each
// px along its width moving `alongX` in the world and each px down its height
// moving `alongY`) where the camera sees that patch of the world. Undefined
// when part of it would be behind the camera.
export const planeTransform = (
	cam: Camera,
	origin: Pt3,
	alongX: Pt3,
	alongY: Pt3,
	w: number,
	h: number,
): string | undefined => {
	const o = sub(origin, cam.pos);
	const row = (axis: Pt3) => [
		dot(alongX, axis),
		dot(alongY, axis),
		dot(o, axis),
	];
	const [rx, ry, r1] = row(cam.right);
	const [ux, uy, u1] = row(cam.up);
	const [fx, fy, f1] = row(cam.fwd);
	// Every corner must be well in front of the camera.
	for (const [a, b] of [
		[0, 0],
		[w, 0],
		[0, h],
		[w, h],
	] as const) {
		if (fx! * a + fy! * b + f1! < 1) {
			return undefined;
		}
	}
	const { f, cx, cy } = cam;
	const X = [f * rx! + cx * fx!, f * ry! + cx * fy!, f * r1! + cx * f1!];
	const Y = [-f * ux! + cy * fx!, -f * uy! + cy * fy!, -f * u1! + cy * f1!];
	const W = [fx!, fy!, f1!];
	// Scaled so the numbers stay friendly to the compositor.
	const s = 1 / W[2]!;
	const m = [
		X[0]! * s,
		Y[0]! * s,
		0,
		W[0]! * s,
		X[1]! * s,
		Y[1]! * s,
		0,
		W[1]! * s,
		0,
		0,
		1,
		0,
		X[2]! * s,
		Y[2]! * s,
		0,
		W[2]! * s,
	];
	return `matrix3d(${m.map((v) => (Math.abs(v) < 1e-12 ? 0 : v).toPrecision(9)).join(",")})`;
};
