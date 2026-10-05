import { project, type Camera } from "./camera.ts";
import type { PlayerState } from "./evaluate.ts";
import {
	drawFigure,
	drawHeadAt,
	type FigureAnchors,
	type Look,
} from "./figure.ts";
import { ANIMS, type AnimName, type Body } from "./poses.ts";

// THE PLAYERS, DRAWN.
//
// Each player is drawn on a canvas of his own at the picture's resolution -
// shaded round, edges smooth - with a soft dark rim round the outside so he
// reads against the floor. His face is his BBGM face, shrunk to his size.
// Then he is stamped into the picture, and kept for the next time the same
// pose comes round.

type RGB = [number, number, number];

const OUTLINE: RGB = [22, 15, 13];

export type Scratch = {
	canvas: HTMLCanvasElement;
	ctx: CanvasRenderingContext2D;
	// A second sheet, for what goes over his head.
	top?: Scratch;
};
export const makeScratch = (): Scratch => {
	const canvas = document.createElement("canvas");
	return {
		canvas,
		ctx: canvas.getContext("2d", { willReadFrequently: true })!,
	};
};
// A dark rim, soft and a pixel wide, round the outside of what is there -
// laid under the edge's own soft pixels, so the edge stays smooth. Only
// round what `isNew` says was just drawn, when asked.
const RIM_ALPHA = 1;
const SOLID = 140;
let solid = new Uint8Array(0);
const rim = (
	d: Uint8ClampedArray,
	w: number,
	h: number,
	isNew?: (i: number) => boolean,
) => {
	if (solid.length < w * h) {
		solid = new Uint8Array(w * h * 2);
	}
	for (let i = 0; i < w * h; i++) {
		solid[i] = d[i * 4 + 3]! >= SOLID && (!isNew || isNew(i)) ? 1 : 0;
	}
	for (let y = 0; y < h; y++) {
		for (let x = 0; x < w; x++) {
			const i = y * w + x;
			const a = d[i * 4 + 3]! / 255;
			if (a * 255 >= SOLID) {
				continue;
			}
			if (
				(x > 0 && solid[i - 1]) ||
				(x < w - 1 && solid[i + 1]) ||
				(y > 0 && solid[i - w]) ||
				(y < h - 1 && solid[i + w])
			) {
				// What is there, over the rim.
				const b = RIM_ALPHA * (1 - a);
				const out = a + b;
				for (let c = 0; c < 3; c++) {
					d[i * 4 + c] = (d[i * 4 + c]! * a + OUTLINE[c]! * b) / out;
				}
				d[i * 4 + 3] = out * 255;
			}
		}
	}
};

// SPRITES ARE DRAWN ONCE AND KEPT.
//
// Like a sprite game's, his animation runs in frames - eight to a stride or
// a loop, a dozen across a jump shot - and he turns in sixteen directions, so
// the same few pictures come round again and again. Each is made the first
// time it is needed and kept; after that a player costs one image copy. He
// still glides across the floor smoothly - only his pose steps, the way
// pixel art does.
type Kept = { canvas: HTMLCanvasElement; dx: number; dy: number };
export type SpriteCache = {
	kept: Map<string, Kept>;
	ids: WeakMap<Look, number>;
	next: number;
};
export const makeSpriteCache = (): SpriteCache => ({
	kept: new Map(),
	ids: new WeakMap(),
	next: 0,
});
const KEEP = 700;
const CYCLE_FRAMES = 8;
// Steps through a bounce for the dribbling hand.
const DRIBBLE_FRAMES = 8;
const ACT_FRAMES = 12;
const TURNS = 16;

// A move's frame: an act stepped through its frames (a long one - a dunk -
// gets more, so its quickest part, the slam, still shows), a cycle or a loop
// through its eight.
const frameOf = (anim: AnimName, phase: number): number => {
	const a = ANIMS[anim];
	const frames = Math.max(ACT_FRAMES, a.n * 2);
	return a.kind === "act"
		? Math.round(Math.min(1, Math.max(0, phase)) * (frames - 1)) / (frames - 1)
		: Math.floor((((phase % 1) + 1) % 1) * CYCLE_FRAMES) / CYCLE_FRAMES;
};
const dribbleFrame = (d: number | undefined) =>
	d === undefined ? undefined : Math.floor(d * DRIBBLE_FRAMES) / DRIBBLE_FRAMES;
// Hands coming up for a pass, in a few steps.
const targetFrame = (t: number | undefined) =>
	t ? Math.ceil(t * 3) / 3 : undefined;

// The pose he is drawn in: his own, stepped to the sprite's frames and turns
// - and, easing out of his last move, two steps of that.
const stepped = (st: PlayerState) => {
	const turn = Math.round(st.yaw / ((Math.PI * 2) / TURNS));
	const f = st.from;
	const w = f ? (f.w > 0.5 ? 2 / 3 : f.w > 0.12 ? 1 / 3 : 0) : 0;
	return {
		phase: frameOf(st.anim, st.phase),
		turn: ((turn % TURNS) + TURNS) % TURNS,
		yaw: (turn * Math.PI * 2) / TURNS,
		dribble: dribbleFrame(st.dribble),
		target: targetFrame(st.target),
		from:
			f && w > 0
				? {
						anim: f.anim,
						phase: frameOf(f.anim, f.phase),
						dribble: dribbleFrame(f.dribble),
						dribbleHand: f.dribbleHand,
						target: targetFrame(f.target),
						w,
					}
				: undefined,
	};
};

export const drawSprite = (
	ctx: CanvasRenderingContext2D,
	scratch: Scratch,
	cam: Camera,
	st: PlayerState,
	body: Body,
	look: Look,
	px: number,
	cache?: SpriteCache,
) => {
	const base = project(cam, { x: st.x, y: st.y, z: 0 });
	const k = base.k;
	// Room for him wherever his arms and feet go: a box round where he stands
	// (and, up in the air, that much higher).
	const half = (body.H * 0.62 + 0.6) * k;
	const lift = Math.max(0, st.z) * k;
	const left = base.x - half;
	const right = base.x + half;
	// Arms thrown up over his head reach a third again over it.
	const upper = base.y - (body.H * 1.4 + 0.5) * k - lift;
	const lower = base.y + 0.9 * k - lift;
	// Nothing of him on the picture: nothing to draw.
	if (right < 0 || left > cam.viewW || lower < 0 || upper > cam.viewH) {
		return;
	}
	const pose = stepped(st);
	let key: string | undefined;
	if (cache) {
		let id = cache.ids.get(look);
		if (id === undefined) {
			id = cache.next++;
			cache.ids.set(look, id);
		}
		const f = pose.from;
		key = `${id}|${st.anim}|${pose.phase}|${pose.turn}|${Math.round(
			Math.log(k) / Math.log(1.04),
		)}|${px}|${st.holding ? 1 : 0}|${pose.dribble ?? ""}${st.dribbleHand ?? ""}|${pose.target ?? ""}${
			f
				? `|${f.anim}${f.phase}${f.dribble ?? ""}${f.dribbleHand ?? ""}${f.target ?? ""}~${f.w}`
				: ""
		}`;
		const kept = cache.kept.get(key);
		if (kept) {
			const smoothing = ctx.imageSmoothingEnabled;
			ctx.imageSmoothingEnabled = false;
			ctx.drawImage(
				kept.canvas,
				Math.round(base.x + kept.dx),
				Math.round(base.y + kept.dy - lift),
				kept.canvas.width * px,
				kept.canvas.height * px,
			);
			ctx.imageSmoothingEnabled = smoothing;
			return;
		}
	}
	// Drawn standing on the floor; a jump only moves the picture up.
	const posed: PlayerState = {
		...st,
		z: 0,
		phase: pose.phase,
		yaw: pose.yaw,
		dribble: pose.dribble,
		target: pose.target,
		from: pose.from,
	};
	const w = Math.max(4, Math.ceil((right - left) / px) + 2);
	const h = Math.max(4, Math.ceil((lower - upper) / px) + 2);
	const { canvas, ctx: s } = scratch;
	if (canvas.width < w || canvas.height < h) {
		canvas.width = Math.max(canvas.width, w);
		canvas.height = Math.max(canvas.height, h);
	}
	s.setTransform(1, 0, 0, 1, 0, 0);
	s.clearRect(0, 0, w, h);
	// Screen coordinates, shrunk to sprite pixels, with a pixel of margin.
	const ox = left - px;
	const oy = upper + lift - px;
	s.setTransform(1 / px, 0, 0, 1 / px, -ox / px, -oy / px);
	const anchors: FigureAnchors = drawFigure(s, cam, posed, body, look, px);
	s.setTransform(1, 0, 0, 1, 0, 0);
	const img = s.getImageData(0, 0, w, h);
	const d = img.data;
	rim(d, w, h);
	s.putImageData(img, 0, 0);
	// His face, shrunk to the sprite's scale - and the rim round it, just
	// where it is new: the rest of him is done.
	s.setTransform(1 / px, 0, 0, 1 / px, -ox / px, -oy / px);
	drawHeadAt(s, cam, posed, body, look);
	s.setTransform(1, 0, 0, 1, 0, 0);
	const hr = anchors.head.r / px;
	const hx0 = Math.max(
		0,
		Math.floor((anchors.head.x - ox) / px - hr * 1.9) - 2,
	);
	const hy0 = Math.max(0, Math.floor((anchors.head.y - oy) / px - hr * 2) - 2);
	const hx1 = Math.min(w, Math.ceil((anchors.head.x - ox) / px + hr * 1.9) + 2);
	const hy1 = Math.min(h, Math.ceil((anchors.head.y - oy) / px + hr * 1.6) + 2);
	if (hx1 > hx0 && hy1 > hy0) {
		const fw = hx1 - hx0;
		const face = s.getImageData(hx0, hy0, fw, hy1 - hy0);
		rim(
			face.data,
			fw,
			hy1 - hy0,
			(i) =>
				d[((hy0 + Math.floor(i / fw)) * w + hx0 + (i % fw)) * 4 + 3]! < SOLID,
		);
		s.putImageData(face, hx0, hy0);
	}
	if (anchors.over) {
		// An arm up in front of his face goes over it: drawn on its own, in his
		// palette, outlined so it reads against his face.
		scratch.top ??= makeScratch();
		const t = scratch.top;
		if (t.canvas.width < w || t.canvas.height < h) {
			t.canvas.width = Math.max(t.canvas.width, w);
			t.canvas.height = Math.max(t.canvas.height, h);
		}
		t.ctx.setTransform(1, 0, 0, 1, 0, 0);
		t.ctx.clearRect(0, 0, w, h);
		t.ctx.setTransform(1 / px, 0, 0, 1 / px, -ox / px, -oy / px);
		anchors.over(t.ctx);
		t.ctx.setTransform(1, 0, 0, 1, 0, 0);
		const top = t.ctx.getImageData(0, 0, w, h);
		rim(top.data, w, h);
		t.ctx.putImageData(top, 0, 0);
		s.drawImage(t.canvas, 0, 0, w, h, 0, 0, w, h);
	}

	// Kept for next time, as its own little canvas.
	let src: HTMLCanvasElement = canvas;
	if (cache && key) {
		const keep = document.createElement("canvas");
		keep.width = w;
		keep.height = h;
		keep.getContext("2d")!.drawImage(canvas, 0, 0, w, h, 0, 0, w, h);
		cache.kept.set(key, { canvas: keep, dx: ox - base.x, dy: oy - base.y });
		if (cache.kept.size > KEEP) {
			// The oldest go first.
			let n = cache.kept.size - KEEP + 100;
			for (const old of cache.kept.keys()) {
				cache.kept.delete(old);
				if (--n <= 0) {
					break;
				}
			}
		}
		src = keep;
	}
	const smoothing = ctx.imageSmoothingEnabled;
	ctx.imageSmoothingEnabled = false;
	ctx.drawImage(
		src,
		0,
		0,
		w,
		h,
		Math.round(ox),
		Math.round(oy - lift),
		w * px,
		h * px,
	);
	ctx.imageSmoothingEnabled = smoothing;
};
