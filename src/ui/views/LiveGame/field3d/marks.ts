import { depthOf, project, type Camera } from "../court3d/camera.ts";
import type { Play } from "./director.ts";
import { FIELD_W } from "./geometry.ts";

// THE BROADCAST'S LINES ON THE FIELD: blue across it at the line of
// scrimmage, yellow at the line to gain - painted on the turf under the men
// standing on it, there from when the offense comes to the line until just
// after the whistle.

export type Mark = { x: number; color: string; alpha: number };

const LOS = "#2f6fe0";
const GAIN = "#f5dc1a";
// Up over this long once the offense is coming to the line (ms), and down
// over this long after the whistle.
const IN_MS = 400;
const SHOW_BEFORE = 1600;
const OUT_MS = 500;
const SHOW_AFTER = 900;
// How wide each is (feet).
const LOS_W = 0.9;
const GAIN_W = 1.2;

export const marksAt = (play: Play | undefined, t: number): Mark[] => {
	if (play?.gainX === undefined) {
		return [];
	}
	const on = play.set - SHOW_BEFORE;
	const off = play.whistle + SHOW_AFTER;
	const a = Math.min(
		1,
		Math.max(0, (t - on) / IN_MS),
		Math.max(0, (off + OUT_MS - t) / OUT_MS),
	);
	if (a <= 0) {
		return [];
	}
	return [
		{ x: play.losX, color: LOS, alpha: 0.78 * a },
		{ x: play.gainX, color: GAIN, alpha: 0.88 * a },
	];
};

export const drawMarks = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	marks: Mark[],
) => {
	ctx.save();
	for (const m of marks) {
		const w = (m.color === GAIN ? GAIN_W : LOS_W) / 2;
		const c = [
			{ x: m.x - w, y: 0, z: 0 },
			{ x: m.x + w, y: 0, z: 0 },
			{ x: m.x + w, y: FIELD_W, z: 0 },
			{ x: m.x - w, y: FIELD_W, z: 0 },
		];
		if (c.some((p) => depthOf(cam, p) <= 1)) {
			continue;
		}
		const q = c.map((p) => project(cam, p));
		ctx.globalAlpha = m.alpha;
		ctx.fillStyle = m.color;
		ctx.beginPath();
		ctx.moveTo(q[0]!.x, q[0]!.y);
		for (const p of q.slice(1)) {
			ctx.lineTo(p.x, p.y);
		}
		ctx.closePath();
		ctx.fill();
		// (Never thinner than a hairline, far off.)
		const a = project(cam, { x: m.x, y: 0, z: 0 });
		const b = project(cam, { x: m.x, y: FIELD_W, z: 0 });
		ctx.strokeStyle = m.color;
		ctx.lineWidth = 1;
		ctx.beginPath();
		ctx.moveTo(a.x, a.y);
		ctx.lineTo(b.x, b.y);
		ctx.stroke();
	}
	ctx.restore();
};
