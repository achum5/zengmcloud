import { depthOf, project, type Camera } from "../court3d/camera.ts";
import {
	BORDER,
	FIELD_L,
	FIELD_W,
	GOAL_L,
	GOAL_R,
	HASH_FAR_Y,
	HASH_LEN,
	HASH_NEAR_Y,
	LINE_W,
	MID_Y,
	YD,
} from "./geometry.ts";

// THE LINES ON THE FIELD, drawn as the camera sees them: every one a strip of
// paint on the ground, at its true width up close and never thinner than a
// hairline far off (as the 3D court draws its own - see courtLines.ts).
//
// All of them run straight along or straight across the field, so each is a
// box: the six-foot white border round the outside, the goal lines (eight
// inches), the yard lines every five yards, and the one-yard ticks along each
// sideline and down both rows of hash marks.

type Box = { x0: number; y0: number; x1: number; y1: number; long: boolean };

const box = (
	x0: number,
	y0: number,
	x1: number,
	y1: number,
	long = false,
): Box => ({ x0, y0, x1, y1, long });

const BOXES: Box[] = (() => {
	const goal = 8 / 12;
	const out: Box[] = [
		// The border, its inside edge the boundary.
		box(-BORDER, -BORDER, FIELD_L + BORDER, 0, true),
		box(-BORDER, FIELD_W, FIELD_L + BORDER, FIELD_W + BORDER, true),
		box(-BORDER, 0, 0, FIELD_W),
		box(FIELD_L, 0, FIELD_L + BORDER, FIELD_W),
		// The goal lines, in the end zone.
		box(GOAL_L - goal, 0, GOAL_L, FIELD_W, true),
		box(GOAL_R, 0, GOAL_R + goal, FIELD_W, true),
	];
	// The yard lines, stopping eight inches short of the border.
	const gap = 8 / 12;
	for (let yl = 5; yl <= 95; yl += 5) {
		const x = GOAL_L + yl * YD;
		out.push(box(x - LINE_W / 2, gap, x + LINE_W / 2, FIELD_W - gap, true));
	}
	// Every other yard: a tick in from each sideline and the hash marks.
	for (let yl = 1; yl <= 99; yl++) {
		if (yl % 5 === 0) {
			continue;
		}
		const x = GOAL_L + yl * YD;
		const x0 = x - LINE_W / 2;
		const x1 = x + LINE_W / 2;
		out.push(
			box(x0, gap, x1, gap + HASH_LEN),
			box(x0, FIELD_W - gap - HASH_LEN, x1, FIELD_W - gap),
			box(x0, HASH_FAR_Y - HASH_LEN, x1, HASH_FAR_Y),
			box(x0, HASH_NEAR_Y, x1, HASH_NEAR_Y + HASH_LEN),
		);
	}
	// The hash marks carry on through the five-yard lines too (on the line
	// itself they are part of it), and the try is snapped from the two:
	// a yard-long mark at the middle of each.
	for (const x of [GOAL_L + 2 * YD, GOAL_R - 2 * YD]) {
		out.push(
			box(x - LINE_W / 2, MID_Y - YD / 2, x + LINE_W / 2, MID_Y + YD / 2),
		);
	}
	return out;
})();

export const fieldLineBoxes = (): readonly Box[] => BOXES;

export const drawFieldLines = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	color: string,
) => {
	const W = cam.viewW;
	const H = cam.viewH;
	ctx.save();
	ctx.fillStyle = color;
	ctx.strokeStyle = color;
	ctx.globalAlpha = 0.94;
	ctx.lineWidth = 1;
	for (const b of BOXES) {
		const c = [
			{ x: b.x0, y: b.y0, z: 0 },
			{ x: b.x1, y: b.y0, z: 0 },
			{ x: b.x1, y: b.y1, z: 0 },
			{ x: b.x0, y: b.y1, z: 0 },
		];
		if (c.some((p) => depthOf(cam, p) <= 1)) {
			continue;
		}
		const q = c.map((p) => project(cam, p));
		if (
			Math.max(q[0]!.x, q[1]!.x, q[2]!.x, q[3]!.x) < 0 ||
			Math.min(q[0]!.x, q[1]!.x, q[2]!.x, q[3]!.x) > W ||
			Math.max(q[0]!.y, q[1]!.y, q[2]!.y, q[3]!.y) < 0 ||
			Math.min(q[0]!.y, q[1]!.y, q[2]!.y, q[3]!.y) > H
		) {
			continue;
		}
		ctx.beginPath();
		ctx.moveTo(q[0]!.x, q[0]!.y);
		ctx.lineTo(q[1]!.x, q[1]!.y);
		ctx.lineTo(q[2]!.x, q[2]!.y);
		ctx.lineTo(q[3]!.x, q[3]!.y);
		ctx.closePath();
		ctx.fill();
		if (b.long) {
			// A hairline down the middle, so a line far off and nearly
			// edge-on is still there.
			const along = b.x1 - b.x0 > b.y1 - b.y0;
			const m0 = along
				? { x: b.x0, y: (b.y0 + b.y1) / 2, z: 0 }
				: { x: (b.x0 + b.x1) / 2, y: b.y0, z: 0 };
			const m1 = along
				? { x: b.x1, y: (b.y0 + b.y1) / 2, z: 0 }
				: { x: (b.x0 + b.x1) / 2, y: b.y1, z: 0 };
			const a = project(cam, m0);
			const z = project(cam, m1);
			ctx.beginPath();
			ctx.moveTo(a.x, a.y);
			ctx.lineTo(z.x, z.y);
			ctx.stroke();
		}
	}
	ctx.restore();
};
