import { depthOf, project, type Camera } from "../court3d/camera.ts";
import {
	BORDER,
	COACH_LINE,
	FIELD_L,
	FIELD_W,
	GOAL_L,
	GOAL_R,
	HASH_FAR_Y,
	HASH_LEN,
	HASH_NEAR_Y,
	LIMIT_END,
	LIMIT_LINE,
	LINE_W,
	MID_Y,
	TEAM_AREA,
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

// And off the field, in yellow: the coaches' line along each team area, and
// the broken line everywhere else round the field - back behind the benches
// at the 25s, round behind the end lines.
const YELLOW = "#f3cd1f";
const SIDELINE_BOXES: Box[] = (() => {
	const coach = 8 / 12;
	const w = 4 / 12;
	const out: Box[] = [];
	const [a, b] = TEAM_AREA;
	const dash = 3;
	const dashes = (x0: number, x1: number, y: number) => {
		for (let x = x0; x < x1; x += dash * 2) {
			out.push(box(x, y, Math.min(x1, x + dash), y + w));
		}
	};
	const across = (x: number, y0: number, y1: number) => {
		for (let y = y0; y < y1; y += dash * 2) {
			out.push(box(x, y, x + w, Math.min(y1, y + dash)));
		}
	};
	const end = LIMIT_END;
	for (const far of [true, false]) {
		const yOf = (off: number) => (far ? -off : FIELD_W + off);
		const y = (off: number, width: number) =>
			far ? yOf(off) - width : yOf(off);
		out.push(
			box(a, y(COACH_LINE, coach), b, y(COACH_LINE, coach) + coach, true),
		);
		dashes(-end, a, y(LIMIT_LINE, w));
		dashes(b, FIELD_L + end, y(LIMIT_LINE, w));
		const [o0, o1] = far
			? [yOf(LIMIT_LINE + 18), yOf(LIMIT_LINE)]
			: [yOf(LIMIT_LINE), yOf(LIMIT_LINE + 18)];
		across(a, o0, o1);
		across(b - w, o0, o1);
		dashes(a, b, y(LIMIT_LINE + 18, w));
	}
	for (const x of [-end - w, FIELD_L + end]) {
		across(x, -LIMIT_LINE, FIELD_W + LIMIT_LINE);
	}
	return out;
})();

export const fieldLineBoxes = (): readonly Box[] => BOXES;

export const drawFieldLines = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	color: string,
	// (Not the field's own, when a picture of the field has them: see
	// StadiumLook's fieldLines.)
	field = true,
) => {
	if (field) {
		drawBoxes(ctx, cam, BOXES, color);
	}
	drawBoxes(ctx, cam, SIDELINE_BOXES, YELLOW);
};

const drawBoxes = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	boxes: Box[],
	color: string,
) => {
	const W = cam.viewW;
	const H = cam.viewH;
	ctx.save();
	ctx.fillStyle = color;
	ctx.strokeStyle = color;
	ctx.globalAlpha = 0.94;
	ctx.lineWidth = 1;
	for (const b of boxes) {
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
