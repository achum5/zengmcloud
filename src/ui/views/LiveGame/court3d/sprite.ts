import { project, type Camera } from "./camera.ts";
import type { PlayerState } from "./evaluate.ts";
import { drawHeadAt, type Look } from "./figure.ts";
import { ANIMS, type AnimName, type Body } from "./poses.ts";
import { rim, SOLID } from "./rim.ts";
import { sculpt, type Sculpted } from "./sculpt.ts";
import { sculptAside } from "./sculptPool.ts";

// THE PLAYERS, DRAWN.
//
// Each player is drawn on a canvas of his own at the picture's resolution -
// shaded round, edges smooth - with a soft dark rim round the outside so he
// reads against the floor. His face is his BBGM face, shrunk to his size.
// Then he is stamped into the picture, and kept for the next time the same
// pose comes round.

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
// SPRITES ARE DRAWN ONCE AND KEPT.
//
// Like a sprite game's, his animation runs in frames - a dozen to a stride
// or a loop, twenty across a jump shot - and he turns in 24 directions, so
// the same few pictures come round again and again. Each is made the first
// time it is needed and kept; after that a player costs one image copy. He
// still glides across the floor smoothly - only his pose steps, the way
// pixel art does.
type Kept = { canvas: HTMLCanvasElement; dx: number; dy: number };
export type SpriteCache = {
	kept: Map<string, Kept>;
	// What the kept pictures hold, in bytes.
	bytes: number;
	ids: WeakMap<Look, number>;
	next: number;
	// The last picture of each man drawn, and the size and look it was drawn
	// for: it stands in while his next pose is being sculpted aside (see
	// sculptPool.ts).
	last: Map<number, { kept: Kept; k: number; look: number }>;
	// The poses out being sculpted.
	pending: Set<string>;
};
export const makeSpriteCache = (): SpriteCache => ({
	kept: new Map(),
	bytes: 0,
	ids: new WeakMap(),
	next: 0,
	last: new Map(),
	pending: new Set(),
});
// At most this many kept, holding at most this much - the finer the picture,
// the bigger each one, so the fewer.
const KEEP = 2400;
const KEEP_BYTES = 160 * 1024 * 1024;
// His last picture stands in for a new pose only at about the size he is
// drawn now - not after a cut to another camera.
const STAND_IN_SCALE = 1.12;
const CYCLE_FRAMES = 12;
// Steps through a bounce for the dribbling hand.
const DRIBBLE_FRAMES = 8;
const ACT_FRAMES = 20;
const TURNS = 24;

// A move's frame: an act stepped through its frames (a long one - a dunk -
// gets more, so its quickest part, the slam, still shows), a cycle or a loop
// through its dozen.
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

// An arm saying something, in steps: its angles to a few degrees, how far
// into it in thirds.
const armSteps = (a: PlayerState["arm"]): PlayerState["arm"] => {
	if (!a) {
		return undefined;
	}
	const w = a.w > 0.8 ? 1 : a.w > 0.45 ? 2 / 3 : a.w > 0.12 ? 1 / 3 : 0;
	const by = (v: number, k: number) => Math.round(v / k) * k;
	return w > 0
		? {
				hand: a.hand,
				sh: by(a.sh, 6),
				el: by(a.el, 8),
				ab: by(a.ab, 10),
				wr: by(a.wr, 10),
				w,
				...(a.point ? { point: true } : {}),
				...(a.tuck ? { tuck: Math.round(a.tuck * 4) / 4 } : {}),
			}
		: undefined;
};

// The pose he is drawn in: his own, stepped to the sprite's frames and turns
// - and, easing out of his last move, three steps of that.
const stepped = (st: PlayerState) => {
	const turn = Math.round(st.yaw / ((Math.PI * 2) / TURNS));
	const f = st.from;
	const step = (v: number) =>
		v > 0.12 ? Math.min(3, Math.max(1, Math.round(v * 4))) / 4 : 0;
	const w = f ? step(f.w) : 0;
	return {
		arm: armSteps(st.arm),
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
						...(f.mirror ? { mirror: true } : {}),
						w,
						...(f.arms === undefined ? {} : { arms: step(f.arms) }),
						...(f.from && step(f.from.w) > 0
							? {
									from: {
										anim: f.from.anim,
										phase: frameOf(f.from.anim, f.from.phase),
										dribble: dribbleFrame(f.from.dribble),
										dribbleHand: f.from.dribbleHand,
										target: targetFrame(f.from.target),
										...(f.from.mirror ? { mirror: true } : {}),
										w: step(f.from.w),
									},
								}
							: {}),
					}
				: undefined,
	};
};

// How each sprite drawn came to be: kept from before, standing in while his
// new pose is sculpted aside, or sculpted here and now - counted for
// measuring.
export const spriteStats = { kept: 0, standIn: 0, here: 0 };

// Kept for next time, as its own little canvas - the oldest let go once
// there are too many, or they hold too much.
const keepSprite = (cache: SpriteCache, key: string, kept: Kept) => {
	const old = cache.kept.get(key);
	if (old) {
		cache.bytes -= old.canvas.width * old.canvas.height * 4;
		cache.kept.delete(key);
	}
	cache.kept.set(key, kept);
	cache.bytes += kept.canvas.width * kept.canvas.height * 4;
	if (cache.kept.size > KEEP || cache.bytes > KEEP_BYTES) {
		for (const [k, v] of cache.kept) {
			cache.kept.delete(k);
			cache.bytes -= v.canvas.width * v.canvas.height * 4;
			if (cache.kept.size <= KEEP - 100 && cache.bytes <= KEEP_BYTES * 0.9) {
				break;
			}
		}
	}
};

// From his body as sculpted (rim and all) to the finished sprite on the
// scratch canvas: his face on it - shrunk to the sprite's scale, and the rim
// round it just where it is new: the rest of him is done - then an arm raised
// in front of it over that.
const finish = (
	scratch: Scratch,
	cam: Camera,
	posed: PlayerState,
	body: Body,
	look: Look,
	made: Sculpted,
	px: number,
	ox: number,
	oy: number,
	w: number,
	h: number,
) => {
	const { canvas, ctx: s } = scratch;
	if (canvas.width < w || canvas.height < h) {
		canvas.width = Math.max(canvas.width, w);
		canvas.height = Math.max(canvas.height, h);
	}
	s.setTransform(1, 0, 0, 1, 0, 0);
	s.clearRect(0, 0, w, h);
	const img = made.img;
	const d = img.data;
	s.putImageData(img, 0, 0);
	s.setTransform(1 / px, 0, 0, 1 / px, -ox / px, -oy / px);
	drawHeadAt(s, cam, posed, body, look);
	s.setTransform(1, 0, 0, 1, 0, 0);
	const head = made.head;
	const hr = head.r;
	const hx0 = Math.max(0, Math.floor(head.x - hr * 1.9) - 2);
	const hy0 = Math.max(0, Math.floor(head.y - hr * 2) - 2);
	const hx1 = Math.min(w, Math.ceil(head.x + hr * 1.9) + 2);
	const hy1 = Math.min(h, Math.ceil(head.y + hr * 1.6) + 2);
	if (hx1 > hx0 && hy1 > hy0) {
		const fw = hx1 - hx0;
		// (Never let a picture we may not read stop the game: no rim then.)
		let face: ImageData | undefined;
		try {
			face = s.getImageData(hx0, hy0, fw, hy1 - hy0);
		} catch {
			face = undefined;
		}
		if (face) {
			rim(
				face.data,
				fw,
				hy1 - hy0,
				(i) =>
					d[((hy0 + Math.floor(i / fw)) * w + hx0 + (i % fw)) * 4 + 3]! < SOLID,
			);
			s.putImageData(face, hx0, hy0);
		}
	}
	if (made.over) {
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
		t.ctx.putImageData(made.over, 0, 0);
		s.drawImage(t.canvas, 0, 0, w, h, 0, 0, w, h);
	}
};

// The finished sprite on the scratch canvas, copied out to keep.
const copyOut = (scratch: Scratch, w: number, h: number): HTMLCanvasElement => {
	const keep = document.createElement("canvas");
	keep.width = w;
	keep.height = h;
	keep.getContext("2d")!.drawImage(scratch.canvas, 0, 0, w, h, 0, 0, w, h);
	return keep;
};

const stamp = (
	ctx: CanvasRenderingContext2D,
	kept: Kept,
	x: number,
	y: number,
	px: number,
) => {
	const smoothing = ctx.imageSmoothingEnabled;
	ctx.imageSmoothingEnabled = false;
	ctx.drawImage(
		kept.canvas,
		Math.round(x + kept.dx),
		Math.round(y + kept.dy),
		kept.canvas.width * px,
		kept.canvas.height * px,
	);
	ctx.imageSmoothingEnabled = smoothing;
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
	// Arms thrown up over his head reach well over it.
	const upper = base.y - (body.H * 1.4 + 0.5) * k - lift;
	const lower = base.y + 0.9 * k - lift;
	// Nothing of him on the picture: nothing to draw.
	if (right < 0 || left > cam.viewW || lower < 0 || upper > cam.viewH) {
		return;
	}
	const pose = stepped(st);
	let key: string | undefined;
	let lookId = -1;
	if (cache) {
		let id = cache.ids.get(look);
		if (id === undefined) {
			id = cache.next++;
			cache.ids.set(look, id);
		}
		lookId = id;
		const f = pose.from;
		const a = pose.arm;
		key = `${id}|${st.anim}${st.mirror ? "m" : ""}|${pose.phase}|${pose.turn}|${Math.round(
			Math.log(k) / Math.log(1.04),
		)}|${px}|${st.holding ? 1 : 0}|${pose.dribble ?? ""}${st.dribbleHand ?? ""}|${pose.target ?? ""}${
			f
				? `|${f.anim}${f.mirror ? "m" : ""}${f.phase}${f.dribble ?? ""}${f.dribbleHand ?? ""}${f.target ?? ""}~${f.w}~${f.arms ?? ""}${
						f.from
							? `<${f.from.anim}${f.from.mirror ? "m" : ""}${f.from.phase}${f.from.dribble ?? ""}${f.from.dribbleHand ?? ""}${f.from.target ?? ""}~${f.from.w}`
							: ""
					}`
				: ""
		}${a ? `|${a.hand}${a.point ? "p" : ""}${a.tuck ? `t${a.tuck}` : ""}${a.sh},${a.el},${a.ab},${a.wr}~${a.w}` : ""}`;
		const kept = cache.kept.get(key);
		if (kept) {
			// Kept longest are the ones still in use: to the back of the line.
			cache.kept.delete(key);
			cache.kept.set(key, kept);
			cache.last.set(st.pid, { kept, k, look: id });
			stamp(ctx, kept, base.x, base.y - lift, px);
			spriteStats.kept += 1;
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
		arm: pose.arm,
	};
	const w = Math.max(4, Math.ceil((right - left) / px) + 2);
	const h = Math.max(4, Math.ceil((lower - upper) / px) + 2);
	// Screen coordinates, shrunk to sprite pixels, with a pixel of margin.
	const ox = left - px;
	const oy = upper + lift - px;

	// A new pose: sculpted aside, if it can be, while his last picture - at
	// about this size - stands in for the frame or two it takes.
	if (cache && key) {
		const last = cache.last.get(st.pid);
		const standIn =
			last &&
			last.look === lookId &&
			Math.max(k / last.k, last.k / k) < STAND_IN_SCALE
				? last.kept
				: undefined;
		if (standIn) {
			const want = key;
			if (
				cache.pending.has(want) ||
				sculptAside(
					look,
					{ cam, st: posed, body, px, ox, oy, w, h },
					(made) => {
						cache.pending.delete(want);
						if (!made) {
							return;
						}
						finish(scratch, cam, posed, body, look, made, px, ox, oy, w, h);
						keepSprite(cache, want, {
							canvas: copyOut(scratch, w, h),
							dx: ox - base.x,
							dy: oy - base.y,
						});
					},
				)
			) {
				cache.pending.add(want);
				stamp(ctx, standIn, base.x, base.y - lift, px);
				spriteStats.standIn += 1;
				return;
			}
		}
	}

	// Sculpted here and now.
	spriteStats.here += 1;
	const made = sculpt(cam, posed, body, look, px, ox, oy, w, h);
	rim(made.img.data, w, h);
	if (made.over) {
		rim(made.over.data, w, h);
	}
	finish(scratch, cam, posed, body, look, made, px, ox, oy, w, h);
	if (cache && key) {
		const kept = {
			canvas: copyOut(scratch, w, h),
			dx: ox - base.x,
			dy: oy - base.y,
		};
		keepSprite(cache, key, kept);
		cache.last.set(st.pid, { kept, k, look: lookId });
		stamp(ctx, kept, base.x, base.y - lift, px);
		return;
	}
	const smoothing = ctx.imageSmoothingEnabled;
	ctx.imageSmoothingEnabled = false;
	ctx.drawImage(
		scratch.canvas,
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
