import type { Camera } from "./camera.ts";
import { COURT_H, COURT_W, type Pt } from "./geometry.ts";
import { RIM_INSET } from "../courtSpots.ts";

// THE LINES ON THE FLOOR, drawn as the camera sees them.
//
// The floor picture comes from the 2D court and is laid on the floor in 3D,
// but its lines are only a few pixels wide in that picture, and how much of
// them survives being tilted away from the camera depends on the browser:
// zoom in on a half court and the three-point arc could come out half there,
// or not at all. So the picture's own lines are hidden (with the flat 2D
// hoop drawn among them) and every line is drawn here instead, as a strip of
// paint on the floor - true width up close, never thinner than a hairline far
// away.

// Two inches of paint.
const LINE_W = 2 / 12;
const THREE_R = 23.75;
const CORNER_DIST = 22;
const LANE_LEN = 19;
const LANE_HALF = 8;
const MID = COURT_H / 2;

type Strip = { pts: Pt[]; closed: boolean };

const arc = (
	cx: number,
	cy: number,
	r: number,
	from: number,
	to: number,
	stepDeg = 2,
): Pt[] => {
	const n = Math.max(
		2,
		Math.ceil(Math.abs(to - from) / ((stepDeg * Math.PI) / 180)),
	);
	const out: Pt[] = [];
	for (let i = 0; i <= n; i++) {
		const a = from + ((to - from) * i) / n;
		out.push({ x: cx + Math.cos(a) * r, y: cy + Math.sin(a) * r });
	}
	return out;
};

// One end's markings, for the end whose baseline is at x = 0.
const halfStrips = (): Strip[] => {
	const out: Strip[] = [];
	const top = MID - LANE_HALF;
	const bottom = MID + LANE_HALF;
	out.push(
		// The lane.
		{
			pts: [
				{ x: 0, y: top },
				{ x: LANE_LEN, y: top },
				{ x: LANE_LEN, y: bottom },
				{ x: 0, y: bottom },
			],
			closed: false,
		},
		// The free-throw circle: solid outside the lane, dashed inside it.
		{
			pts: arc(LANE_LEN, MID, 6, -Math.PI / 2, Math.PI / 2),
			closed: false,
		},
	);
	const dashes = 9;
	for (let i = 0; i < dashes; i++) {
		const a0 = Math.PI / 2 + (Math.PI * (i + 0.2)) / dashes;
		const a1 = Math.PI / 2 + (Math.PI * (i + 0.8)) / dashes;
		out.push({ pts: arc(LANE_LEN, MID, 6, a0, a1, 4), closed: false });
	}
	// The restricted area: four feet round the basket, straight back to the
	// face of the backboard.
	out.push({
		pts: [
			{ x: 4, y: MID - 4 },
			...arc(RIM_INSET, MID, 4, -Math.PI / 2, Math.PI / 2),
			{ x: 4, y: MID + 4 },
		],
		closed: false,
	});
	// The three-point line: the corners, then the arc.
	const cornerAngle = Math.asin(CORNER_DIST / THREE_R);
	out.push({
		pts: [
			{ x: 0, y: MID - CORNER_DIST },
			...arc(RIM_INSET, MID, THREE_R, -cornerAngle, cornerAngle, 1.5),
			{ x: 0, y: MID + CORNER_DIST },
		],
		closed: false,
	});
	// Lane space marks: the block, then a tick every three feet up the lane.
	for (const [y, out1] of [
		[top, -1],
		[bottom, 1],
	] as const) {
		for (const d of [7, 8, 11, 14, 17]) {
			out.push({
				pts: [
					{ x: d, y },
					{ x: d, y: y + out1 * 0.6 },
				],
				closed: false,
			});
		}
	}
	// The hash marks on each sideline, 28 feet up the floor.
	for (const [y0, y1] of [
		[0, 3],
		[COURT_H, COURT_H - 3],
	] as const) {
		out.push({
			pts: [
				{ x: 28, y: y0 },
				{ x: 28, y: y1 },
			],
			closed: false,
		});
	}
	return out;
};

const mirror = (s: Strip): Strip => ({
	closed: s.closed,
	pts: s.pts.map((p) => ({ x: COURT_W - p.x, y: p.y })),
});

const STRIPS: Strip[] = (() => {
	const half = halfStrips();
	return [
		// The boundary and the half-court line.
		{
			pts: [
				{ x: 0, y: 0 },
				{ x: COURT_W, y: 0 },
				{ x: COURT_W, y: COURT_H },
				{ x: 0, y: COURT_H },
			],
			closed: true,
		},
		{
			pts: [
				{ x: COURT_W / 2, y: 0 },
				{ x: COURT_W / 2, y: COURT_H },
			],
			closed: false,
		},
		// The center circle.
		{
			pts: arc(COURT_W / 2, MID, 6, 0, Math.PI * 2).slice(0, -1),
			closed: true,
		},
		...half,
		...half.map(mirror),
	];
})();

// The two edges of a strip of paint along a line, offset half a line width
// either side with mitered corners.
const outline = (s: Strip, w: number): { left: Pt[]; right: Pt[] } => {
	const { pts, closed } = s;
	const n = pts.length;
	const normal = (a: Pt, b: Pt): Pt => {
		const dx = b.x - a.x;
		const dy = b.y - a.y;
		const l = Math.hypot(dx, dy) || 1;
		return { x: -dy / l, y: dx / l };
	};
	const left: Pt[] = [];
	const right: Pt[] = [];
	for (let i = 0; i < n; i++) {
		const p = pts[i]!;
		const prev = closed ? pts[(i - 1 + n) % n] : pts[i - 1];
		const next = closed ? pts[(i + 1) % n] : pts[i + 1];
		const n0 = prev ? normal(prev, p) : undefined;
		const n1 = next ? normal(p, next) : undefined;
		let nx: number;
		let ny: number;
		let scale = 1;
		if (n0 && n1) {
			nx = n0.x + n1.x;
			ny = n0.y + n1.y;
			const l = Math.hypot(nx, ny) || 1;
			nx /= l;
			ny /= l;
			// Keep the full width through a corner.
			scale = 1 / Math.max(0.35, nx * n1.x + ny * n1.y);
		} else {
			const only = (n0 ?? n1)!;
			nx = only.x;
			ny = only.y;
		}
		const o = (w / 2) * scale;
		left.push({ x: p.x + nx * o, y: p.y + ny * o });
		right.push({ x: p.x - nx * o, y: p.y - ny * o });
	}
	return { left, right };
};

const OUTLINES = STRIPS.map((s) => ({ strip: s, ...outline(s, LINE_W) }));

// THE FLOOR'S POINTS ONTO THE PICTURE, a thousand-odd a frame: worked out
// as project does it for a point on the floor, into a buffer kept for it -
// not an object made for every point every frame, for the page to clear
// away after (a quarter of a megabyte a frame, here alone). Nothing if a
// point is behind the camera, or right up against it (`front`): a strip
// reaching back behind the camera can't be drawn.
const buffers = [0, 1, 2].map(() => new Float64Array(0));
const onPicture = (
	cam: Camera,
	pts: readonly Pt[],
	which: number,
	front: boolean,
): Float64Array | undefined => {
	let out = buffers[which]!;
	if (out.length < pts.length * 2) {
		out = new Float64Array(pts.length * 4);
		buffers[which] = out;
	}
	const { pos, right, up, fwd, f, cx, cy } = cam;
	const dz = -pos.z;
	for (let i = 0; i < pts.length; i++) {
		const p = pts[i]!;
		const dx = p.x - pos.x;
		const dy = p.y - pos.y;
		const depth = dx * fwd.x + dy * fwd.y + dz * fwd.z;
		if (front && !(depth > 1)) {
			return undefined;
		}
		const k = f / Math.max(0.5, depth);
		out[i * 2] = cx + (dx * right.x + dy * right.y + dz * right.z) * k;
		out[i * 2 + 1] = cy - (dx * up.x + dy * up.y + dz * up.z) * k;
	}
	return out;
};
// Along the first `n` points in a buffer (or back along them).
const trace = (
	ctx: CanvasRenderingContext2D,
	b: Float64Array,
	n: number,
	reverse = false,
) => {
	for (let j = 0; j < n; j++) {
		const i = reverse ? n - 1 - j : j;
		if (j === 0 && !reverse) {
			ctx.moveTo(b[i * 2]!, b[i * 2 + 1]!);
		} else {
			ctx.lineTo(b[i * 2]!, b[i * 2 + 1]!);
		}
	}
};

export const drawCourtLines = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	color: string,
) => {
	ctx.save();
	ctx.fillStyle = color;
	ctx.strokeStyle = color;
	ctx.globalAlpha = 0.92;
	ctx.lineJoin = "round";
	ctx.lineCap = "round";
	for (const { strip, left, right } of OUTLINES) {
		const C = onPicture(cam, strip.pts, 0, true);
		if (!C) {
			continue;
		}
		const L = onPicture(cam, left, 1, false)!;
		const R = onPicture(cam, right, 2, false)!;
		// The paint itself, at its true width.
		ctx.beginPath();
		if (strip.closed) {
			trace(ctx, L, left.length);
			ctx.closePath();
			trace(ctx, R, right.length);
			ctx.closePath();
			ctx.fill("evenodd");
		} else {
			trace(ctx, L, left.length);
			trace(ctx, R, right.length, true);
			ctx.closePath();
			ctx.fill();
		}
		// A hairline down the middle, so a line far off and nearly edge-on is
		// still there.
		ctx.lineWidth = 1;
		ctx.beginPath();
		trace(ctx, C, strip.pts.length);
		if (strip.closed) {
			ctx.closePath();
		}
		ctx.stroke();
	}
	ctx.restore();
};

// For tests: every line's centerline, in feet.
export const courtLineStrips = (): readonly Strip[] => STRIPS;
