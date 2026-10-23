import { depthOf, project, type Camera } from "../court3d/camera.ts";
import { skeleton, type Body, type V3 } from "../court3d/poses.ts";
import { FIGURE_SCALE } from "./body.ts";
import type { Pt3 } from "./geometry.ts";
import { poseOfMove, withArms } from "./poses.ts";
import { bodyPoint } from "./sculpt.ts";
import { drawnLift, type FieldPlayerState } from "./sprite.ts";

// THE WATER AT A TIMEOUT, as the camera sees it: a bottle in the hand of a
// man taking a drink, up over his face mask; and a trainer's carrier of
// them hung from his hand.

const BOTTLE = "#2fa84f";
const BOTTLE_DARK = "#1d7a37";
const CAP = "#f2f2ee";
const RACK = "#2b2b30";
// A bottle (feet - drawn to the size of the men, see body.ts): how long,
// how big round, its cap - and how far through his hand it is held, from
// its bottom.
const BOTTLE_FT = 0.8 * FIGURE_SCALE;
const BOTTLE_D = 0.24 * FIGURE_SCALE;
const CAP_FT = 0.16 * FIGURE_SCALE;
const HELD_AT = 0.6;
// The carrier: how far below his hand its bottles hang, and how far apart
// they sit, in a row along his side.
const HANG_FT = 0.22 * FIGURE_SCALE;
const ROW_FT = 0.27 * FIGURE_SCALE;
const ROW = 4;
// Never so thin, far off, it can't be made out.
const MIN_PX = 1;

type Item = { depth: number; draw: () => void };

// His right hand, and his mouth behind his face mask, in his frame - in a
// move at its phase (one in every fortieth of it worked out once).
const STEPS = 40;
const hands = new WeakMap<Body, Map<string, { hand: V3; mouth: V3 }>>();
const handOf = (b: Body, st: FieldPlayerState) => {
	let byMove = hands.get(b);
	if (!byMove) {
		byMove = new Map();
		hands.set(b, byMove);
	}
	const step = Math.round(st.phase * STEPS);
	const arms = st.arms;
	const key = `${st.move}|${step}|${arms ? `${arms.move}|${Math.round(arms.phase * STEPS)}` : ""}`;
	let h = byMove.get(key);
	if (!h) {
		const q = poseOfMove(st.move, step / STEPS);
		const sk = skeleton(
			b,
			arms ? withArms(q, poseOfMove(arms.move, arms.phase)) : q,
		);
		h = {
			hand: sk.armR.end,
			mouth: { f: sk.head.f + 0.3, s: sk.head.s, u: sk.head.u - 0.22 },
		};
		byMove.set(key, h);
	}
	return h;
};

const carrying = (st: FieldPlayerState) =>
	st.move === "carryWater" || st.arms?.move === "carryWater";

export const waterGear = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	states: FieldPlayerState[],
	bodyOf: (pid: number) => Body,
): Item[] => {
	const items: Item[] = [];
	for (const st of states) {
		const drinking = st.move === "drink";
		if (!drinking && !carrying(st)) {
			continue;
		}
		const { hand, mouth } = handOf(bodyOf(st.id), st);
		const placed = { x: st.x, y: st.y, z: drawnLift(st), yaw: st.yaw };
		if (drinking) {
			// (Its spout to his mouth.)
			const d = {
				f: mouth.f - hand.f,
				s: mouth.s - hand.s,
				u: mouth.u - hand.u,
			};
			const l = Math.hypot(d.f, d.s, d.u) || 1;
			const along = (k: number) =>
				bodyPoint(placed, {
					f: hand.f + (d.f / l) * k,
					s: hand.s + (d.s / l) * k,
					u: hand.u + (d.u / l) * k,
				});
			const bottom = along(-BOTTLE_FT * HELD_AT);
			const top = along(BOTTLE_FT * (1 - HELD_AT));
			const spout = along(BOTTLE_FT * (1 - HELD_AT) + CAP_FT);
			items.push({
				depth: depthOf(cam, along(0)),
				draw: () => {
					drawBottle(ctx, cam, bottom, top, spout);
				},
			});
			continue;
		}
		// The carrier: a row of them along his side, hung from the handle in
		// his hand.
		const bottles: Pt3[][] = [];
		for (let k = 0; k < ROW; k++) {
			const f = hand.f + (k - (ROW - 1) / 2) * ROW_FT;
			const at = (u: number) => bodyPoint(placed, { f, s: hand.s, u });
			bottles.push([
				at(hand.u - HANG_FT - BOTTLE_FT),
				at(hand.u - HANG_FT),
				at(hand.u - HANG_FT + CAP_FT),
			]);
		}
		const grip = bodyPoint(placed, hand);
		items.push({
			depth: depthOf(cam, grip),
			draw: () => {
				drawRack(ctx, cam, grip, bottles);
			},
		});
	}
	return items;
};

const segment = (
	ctx: CanvasRenderingContext2D,
	a: { x: number; y: number },
	b: { x: number; y: number },
	color: string,
	width: number,
) => {
	ctx.strokeStyle = color;
	ctx.lineWidth = width;
	ctx.beginPath();
	ctx.moveTo(a.x, a.y);
	ctx.lineTo(b.x, b.y);
	ctx.stroke();
};

const drawBottle = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	bottom: Pt3,
	top: Pt3,
	spout: Pt3,
) => {
	const a = project(cam, bottom);
	const b = project(cam, top);
	const c = project(cam, spout);
	const w = Math.max(MIN_PX + 1, BOTTLE_D * a.k);
	ctx.save();
	ctx.lineCap = "round";
	segment(ctx, a, b, BOTTLE_DARK, w + 1);
	segment(ctx, a, b, BOTTLE, w);
	ctx.lineCap = "butt";
	segment(ctx, b, c, CAP, Math.max(MIN_PX, w * 0.55));
	ctx.restore();
};

const drawRack = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	grip: Pt3,
	bottles: Pt3[][],
) => {
	const g = project(cam, grip);
	const ends = bottles.map((b) => b.map((p) => project(cam, p)));
	const w = Math.max(MIN_PX + 1, BOTTLE_D * g.k);
	ctx.save();
	// (The handle, down from his hand to the middle of the row.)
	const first = ends[0]!;
	const last = ends.at(-1)!;
	const mid = {
		x: (first[1]!.x + last[1]!.x) / 2,
		y: (first[1]!.y + last[1]!.y) / 2,
	};
	segment(ctx, g, mid, RACK, Math.max(MIN_PX, w * 0.3));
	ctx.lineCap = "round";
	for (const [a, b, c] of ends) {
		segment(ctx, a!, b!, BOTTLE_DARK, w + 1);
		segment(ctx, a!, b!, BOTTLE, w);
		ctx.lineCap = "butt";
		segment(ctx, b!, c!, CAP, Math.max(MIN_PX, w * 0.55));
		ctx.lineCap = "round";
	}
	// (The frame round them, low down.)
	const low = (p: { x: number; y: number }[], u: number) => ({
		x: p[0]!.x + (p[1]!.x - p[0]!.x) * u,
		y: p[0]!.y + (p[1]!.y - p[0]!.y) * u,
	});
	segment(
		ctx,
		low(first, 0.35),
		low(last, 0.35),
		RACK,
		Math.max(MIN_PX, w * 0.45),
	);
	ctx.restore();
};
