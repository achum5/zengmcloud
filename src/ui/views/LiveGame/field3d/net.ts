import { project, type Camera } from "../court3d/camera.ts";
import type { Play } from "./director.ts";
import { FIELD_L, MID_Y, NET_BACK, type Side } from "./geometry.ts";

// THE NET BEHIND THE POSTS: up out of the ground behind the end line the
// kick goes to while the kicking team comes out for it, catching the ball
// (see action.ts fieldGoal), and back down once it is over.

export type NetUp = { side: Side; up: number };

// This far across the field each way from the middle, from this high to
// this high once it is all the way up (feet; the ball is drawn higher than
// it flies - see body.ts FIGURE_SCALE); the cords this far apart, and no
// nearer together in the picture than this (pixels).
export const NET_HALF = 30;
const NET_LOW = 5;
export const NET_TOP = 46;
const MESH = 1;
const MESH_LEAST_PX = 4;
// Up over this long from the kicking team out of its huddle (ms); down
// from this long after the whistle, over this long.
const RISE_MS = 1800;
const HOLD_MS = 1600;
const FALL_MS = 1800;
const KICKS = new Set(["fieldGoal", "extraPoint"]);

const ease = (u: number) => u * u * (3 - 2 * u);

export const netAt = (play: Play | undefined, t: number): NetUp | undefined => {
	if (!play || !KICKS.has(play.kind)) {
		return undefined;
	}
	const u = Math.min(
		1,
		(t - play.t0) / RISE_MS,
		(play.whistle + HOLD_MS + FALL_MS - t) / FALL_MS,
	);
	return u > 0
		? { side: play.dir === 1 ? 1 : 0, up: ease(Math.min(1, u)) }
		: undefined;
};

const CORD = "rgba(24, 26, 30, 0.38)";
const FILL = "rgba(24, 26, 30, 0.16)";
const FRAME = "#24272c";

export const drawNet = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	{ side, up }: NetUp,
) => {
	const s = side === 0 ? -1 : 1;
	const x = side === 0 ? -NET_BACK : FIELD_L + NET_BACK;
	// (Nothing of it from behind it, or right up against it.)
	if ((x - cam.pos.x) * s < 2) {
		return;
	}
	// Coming up out of the ground whole, the cords with it.
	const top = NET_TOP * up;
	const low = Math.max(0, top - (NET_TOP - NET_LOW));
	if (top < 0.5) {
		return;
	}
	const y0 = MID_Y - NET_HALF;
	const y1 = MID_Y + NET_HALF;
	const P = (y: number, z: number) => project(cam, { x, y, z });
	const k = P(MID_Y, top / 2).k;
	const step = MESH * Math.max(1, Math.ceil(MESH_LEAST_PX / (MESH * k)));
	ctx.save();
	const corners = [P(y0, low), P(y1, low), P(y1, top), P(y0, top)];
	ctx.fillStyle = FILL;
	ctx.beginPath();
	corners.forEach((p, n) =>
		n === 0 ? ctx.moveTo(p.x, p.y) : ctx.lineTo(p.x, p.y),
	);
	ctx.closePath();
	ctx.fill();
	ctx.strokeStyle = CORD;
	ctx.lineWidth = Math.min(1.5, Math.max(0.6, k * 0.08));
	ctx.beginPath();
	for (let y = y0 + step; y < y1 - 0.01; y += step) {
		const a = P(y, low);
		const b = P(y, top);
		ctx.moveTo(a.x, a.y);
		ctx.lineTo(b.x, b.y);
	}
	for (let z = top - step; z > low + 0.01; z -= step) {
		const a = P(y0, z);
		const b = P(y1, z);
		ctx.moveTo(a.x, a.y);
		ctx.lineTo(b.x, b.y);
	}
	ctx.stroke();
	// Its frame: the cable along the top, a pole up each side.
	ctx.strokeStyle = FRAME;
	ctx.lineCap = "round";
	ctx.lineWidth = Math.max(1, k * 0.25);
	ctx.beginPath();
	ctx.moveTo(corners[3]!.x, corners[3]!.y);
	ctx.lineTo(corners[2]!.x, corners[2]!.y);
	ctx.moveTo(corners[0]!.x, corners[0]!.y);
	ctx.lineTo(corners[1]!.x, corners[1]!.y);
	ctx.stroke();
	ctx.lineWidth = Math.max(1.5, k * 0.5);
	for (const y of [y0, y1]) {
		const a = P(y, 0);
		const b = P(y, top + 1);
		ctx.beginPath();
		ctx.moveTo(a.x, a.y);
		ctx.lineTo(b.x, b.y);
		ctx.stroke();
	}
	ctx.restore();
};
