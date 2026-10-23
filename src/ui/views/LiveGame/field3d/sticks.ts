import { depthOf, project, type Camera } from "../court3d/camera.ts";
import { skeleton, type Body } from "../court3d/poses.ts";
import { FIGURE_SCALE } from "./body.ts";
import { chainManOf, type ChainMan } from "./chains.ts";
import type { FieldMoment } from "./evaluate.ts";
import { MID_Y, type Pt3 } from "./geometry.ts";
import { poseOfMove } from "./poses.ts";
import { bodyPoint } from "./sculpt.ts";
import type { FieldPlayerState } from "./sprite.ts";

// THE CHAIN CREW'S GEAR, as the camera sees it: each man's pole up in his
// hands - an orange marker on top of a rod, the down on the box - or laid on
// the turf beside him; and the chain along the ground between the rods.

const ORANGE = "#ff6b17";
const ORANGE_DARK = "#b9440b";
const NUMERAL = "#141416";
const CHAIN_GRAY = "#c9c9c1";
// How tall a rod is, and its marker on top (feet - drawn to the size of the
// men, see body.ts).
const ROD_H = 8 * FIGURE_SCALE;
const MARKER_W = 1.1 * FIGURE_SCALE;
const MARKER_H = 0.9 * FIGURE_SCALE;
// The box on its pole: its top, and how wide, tall and deep it is.
const BOX_TOP = 7.6 * FIGURE_SCALE;
const BOX_W = 1.4 * FIGURE_SCALE;
const BOX_T = 1.1 * FIGURE_SCALE;
const BOX_D = 0.5 * FIGURE_SCALE;
const POLE_FT = 0.15;
// Never so thin, far off, it can't be made out.
const MIN_PX = 1;

type Item = { depth: number; draw: () => void };

// Where his hands have the pole, in his frame.
const grips = new WeakMap<Body, { f: number; s: number }>();
const gripOf = (b: Body) => {
	let g = grips.get(b);
	if (!g) {
		const sk = skeleton(b, poseOfMove("holdPole", 0));
		g = {
			f: (sk.armR.end.f + sk.armL.end.f) / 2,
			s: (sk.armR.end.s + sk.armL.end.s) / 2,
		};
		grips.set(b, g);
	}
	return g;
};

const upright = (st: FieldPlayerState) =>
	st.move === "holdPole" || st.arms?.move === "holdPole";

export const chainGear = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	m: FieldMoment,
	bodyOf: (pid: number) => Body,
): Item[] => {
	const items: Item[] = [];
	const down = m.play?.down ?? 1;
	const bases = new Map<ChainMan, Pt3>();
	for (const st of m.players) {
		const c = chainManOf(st.id);
		if (!c) {
			continue;
		}
		const box = c === "box" || c === "aux";
		if (upright(st)) {
			const g = gripOf(bodyOf(st.id));
			const at = bodyPoint(
				{ x: st.x, y: st.y, z: 0, yaw: st.yaw },
				{ f: g.f, s: g.s, u: 0 },
			);
			bases.set(c, at);
			items.push({
				depth: depthOf(cam, at),
				draw: () => {
					if (box) {
						drawBox(ctx, cam, at, down);
					} else {
						drawRod(ctx, cam, at);
					}
				},
			});
		} else {
			// (On the turf at his feet, toward the field.)
			const at = { x: st.x - 1, y: st.y + (st.y < MID_Y ? 1.4 : -1.4), z: 0 };
			items.push({
				depth: depthOf(cam, at) + 0.5,
				draw: () => {
					drawLaid(ctx, cam, at, box);
				},
			});
		}
	}
	const rear = bases.get("rear");
	const front = bases.get("front");
	if (rear && front) {
		items.push({
			depth: Math.max(depthOf(cam, rear), depthOf(cam, front)) + 1,
			draw: () => {
				drawChain(ctx, cam, rear, front);
			},
		});
	}
	return items;
};

const pole = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	at: Pt3,
	top: number,
) => {
	const a = project(cam, at);
	const b = project(cam, { ...at, z: top });
	ctx.strokeStyle = ORANGE;
	ctx.lineWidth = Math.max(MIN_PX, POLE_FT * a.k);
	ctx.beginPath();
	ctx.moveTo(a.x, a.y);
	ctx.lineTo(b.x, b.y);
	ctx.stroke();
};

// A face square to the field, `w` wide from z0 up to z1.
const face = (cam: Camera, at: Pt3, w: number, z0: number, z1: number) => [
	project(cam, { x: at.x - w / 2, y: at.y, z: z1 }),
	project(cam, { x: at.x + w / 2, y: at.y, z: z1 }),
	project(cam, { x: at.x + w / 2, y: at.y, z: z0 }),
	project(cam, { x: at.x - w / 2, y: at.y, z: z0 }),
];

const fillQuad = (
	ctx: CanvasRenderingContext2D,
	q: { x: number; y: number }[],
	color: string,
) => {
	ctx.fillStyle = color;
	ctx.beginPath();
	ctx.moveTo(q[0]!.x, q[0]!.y);
	for (const p of q.slice(1)) {
		ctx.lineTo(p.x, p.y);
	}
	ctx.closePath();
	ctx.fill();
};

const drawRod = (ctx: CanvasRenderingContext2D, cam: Camera, at: Pt3) => {
	ctx.save();
	pole(ctx, cam, at, ROD_H);
	const q = face(cam, at, MARKER_W, ROD_H - MARKER_H, ROD_H);
	fillQuad(ctx, q, ORANGE);
	ctx.strokeStyle = ORANGE_DARK;
	ctx.lineWidth = Math.max(0.5, q[0]!.k * 0.05);
	ctx.stroke();
	ctx.restore();
};

const drawBox = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	at: Pt3,
	down: number,
) => {
	ctx.save();
	pole(ctx, cam, at, BOX_TOP - BOX_T);
	// Its top, a shade darker, then its face with the down on it.
	const lid = [
		project(cam, { x: at.x - BOX_W / 2, y: at.y - BOX_D / 2, z: BOX_TOP }),
		project(cam, { x: at.x + BOX_W / 2, y: at.y - BOX_D / 2, z: BOX_TOP }),
		project(cam, { x: at.x + BOX_W / 2, y: at.y + BOX_D / 2, z: BOX_TOP }),
		project(cam, { x: at.x - BOX_W / 2, y: at.y + BOX_D / 2, z: BOX_TOP }),
	];
	fillQuad(ctx, lid, ORANGE_DARK);
	const q = face(cam, at, BOX_W, BOX_TOP - BOX_T, BOX_TOP);
	fillQuad(ctx, q, ORANGE);
	const k = q[0]!.k;
	const cx = (q[0]!.x + q[2]!.x) / 2;
	const cy = (q[0]!.y + q[2]!.y) / 2;
	const tall = Math.abs(q[3]!.y - q[0]!.y);
	ctx.fillStyle = NUMERAL;
	if (tall >= 6) {
		ctx.font = `900 ${Math.round(tall * 0.86)}px "Arial Black", Arial, sans-serif`;
		ctx.textAlign = "center";
		ctx.textBaseline = "middle";
		ctx.fillText(String(down), cx, cy + tall * 0.04);
	} else {
		// (Too small to read: a mark for it.)
		const w = Math.max(1, BOX_W * 0.3 * k);
		const h = Math.max(1, tall * 0.6);
		ctx.fillRect(cx - w / 2, cy - h / 2, w, h);
	}
	ctx.restore();
};

const drawLaid = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	at: Pt3,
	box: boolean,
) => {
	const a = project(cam, at);
	const end = { ...at, x: at.x + (box ? BOX_TOP - BOX_T : ROD_H) };
	const b = project(cam, end);
	ctx.save();
	ctx.strokeStyle = ORANGE;
	ctx.lineWidth = Math.max(MIN_PX, POLE_FT * a.k);
	ctx.beginPath();
	ctx.moveTo(a.x, a.y);
	ctx.lineTo(b.x, b.y);
	ctx.stroke();
	// Its marker, or the box, flat on the turf at the end.
	const w = box ? BOX_W : MARKER_W;
	const l = box ? BOX_T : MARKER_H;
	fillQuad(
		ctx,
		[
			project(cam, { x: end.x, y: end.y - w / 2, z: 0.2 }),
			project(cam, { x: end.x + l, y: end.y - w / 2, z: 0.2 }),
			project(cam, { x: end.x + l, y: end.y + w / 2, z: 0.2 }),
			project(cam, { x: end.x, y: end.y + w / 2, z: 0.2 }),
		],
		ORANGE,
	);
	ctx.restore();
};

const drawChain = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	a: Pt3,
	b: Pt3,
) => {
	const p = project(cam, { ...a, z: 0.1 });
	const q = project(cam, { ...b, z: 0.1 });
	ctx.save();
	ctx.strokeStyle = CHAIN_GRAY;
	ctx.globalAlpha = 0.85;
	ctx.lineWidth = Math.max(0.6, 0.08 * Math.min(p.k, q.k));
	ctx.beginPath();
	ctx.moveTo(p.x, p.y);
	ctx.lineTo(q.x, q.y);
	ctx.stroke();
	ctx.restore();
};
