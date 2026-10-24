import { project, type Camera, type Projected } from "./camera.ts";
import {
	legTurnFrom,
	type Blend,
	type PlayerState,
	type Stride,
} from "./evaluate.ts";
import { drawHeadAt, headOf, type Look } from "./figure.ts";
import { ANIMS, type AnimName, type Body, type V3 } from "./poses.ts";
import { sculpt } from "./sculpt.ts";

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
export const rim = (
	d: Uint8ClampedArray,
	w: number,
	h: number,
	isNew?: (i: number) => boolean,
	// (Only round what is in this box: nothing is drawn outside it.)
	box?: Box,
) => {
	if (solid.length < w * h) {
		solid = new Uint8Array(w * h * 2);
	}
	const bx0 = box ? Math.max(0, box.x0 - 1) : 0;
	const by0 = box ? Math.max(0, box.y0 - 1) : 0;
	const bx1 = box ? Math.min(w - 1, box.x1 + 1) : w - 1;
	const by1 = box ? Math.min(h - 1, box.y1 + 1) : h - 1;
	// (What is solid a pixel further out too: the rim's edge asks.)
	for (let y = Math.max(0, by0 - 1); y <= Math.min(h - 1, by1 + 1); y++) {
		for (let x = Math.max(0, bx0 - 1); x <= Math.min(w - 1, bx1 + 1); x++) {
			const i = y * w + x;
			solid[i] = d[i * 4 + 3]! >= SOLID && (!isNew || isNew(i)) ? 1 : 0;
		}
	}
	for (let y = by0; y <= by1; y++) {
		for (let x = bx0; x <= bx1; x++) {
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
// Like a sprite game's, his animation runs in frames - a dozen to a stride
// or a loop, twenty across a jump shot - and he turns in 24 directions, so
// the same few pictures come round again and again. Each is made the first
// time it is needed and kept; after that a player costs one image copy. He
// still glides across the floor smoothly - only his pose steps, the way
// pixel art does.
//
// Each is kept trimmed to what is drawn of him, not the whole box he might
// reach into - and only so many of them, let go of the moment they are done
// with: they are pictures on the graphics card. (Thousands there, each its
// own picture - and thrown away by the thousand each time the picture
// changed size - took the whole browser down: every window black. Kept in
// plain memory instead, every one drawn has to be copied over to the card
// again and again, and the game slows to a crawl.)
type Box = { x0: number; y0: number; x1: number; y1: number };
export type Kept = {
	canvas: HTMLCanvasElement;
	w: number;
	h: number;
	// From where he stands, on the picture.
	dx: number;
	dy: number;
};
export type SpriteCache = {
	kept: Map<string, Kept>;
	// All of them together, in pixels.
	area: number;
	// Each look's number in the keys (a court player's, or a football
	// player's - see field3d/sprite.ts).
	ids: WeakMap<object, number>;
	next: number;
	// With time to spare on the device, his poses in finer steps - twice as
	// many frames, turns and steps of a blend - so he moves the more smoothly
	// (Court3D decides, by how long a frame takes to draw).
	fine?: boolean;
	// SO MUCH A FRAME. The frame being drawn (see startFrame), and the time
	// spent on new pictures in it (ms). Once that is more than `budget`, a
	// man with no picture yet for his new pose is drawn one more frame in
	// the one he had - a few frames behind at most - and gets his new one
	// as soon as there is time: a frame that would have taken too long,
	// taken in several.
	frame: number;
	spent: number;
	budget?: number;
	// The picture as good as black (dipped between clips): more time a
	// frame, and once that is spent a man with no picture yet at all is
	// left out of the frame rather than all of them made in one - and how
	// many were (see Court3D, which holds the black till none are). All
	// black, nothing of the frame is drawn (see drawFrame): all its time
	// goes on new pictures, more still.
	dark?: boolean;
	black?: boolean;
	skipped: number;
	// How fast the moment drawn moves on, to the wall clock's (1 at real
	// speed; a slow-motion replay less): poses come round that much more
	// slowly, and a man may be drawn as he was for that many more frames.
	pace?: number;
	// Each man's picture last drawn (by who he is - see Plan.who): the
	// frame, how many frames behind his pose it was then, his size in it and
	// whether the ball was in his hands in it.
	last: Map<
		number,
		{ kept: Kept; frame: number; behind: number; scale: number; held?: boolean }
	>;
	// This frame's plan for each man made ready (see prepareSprites).
	plans: Map<PlayerState, Plan>;
	// Hands off the page's own thread to make new pictures on (see farm.ts),
	// if there are any to be had - and whether the cache is done with (let
	// go of: nothing more to be kept in it).
	farm?: Farm;
	done?: boolean;
};

// HOW HE IS MADE, in plain numbers - the camera, his pose stepped to the
// sprite's frames, his build, the box on the picture he is drawn in and
// where he stands - so a worker can sculpt him as readily as the page can.
export type Job = {
	cam: Camera;
	st: PlayerState;
	body: Body;
	px: number;
	left: number;
	right: number;
	upper: number;
	lower: number;
	lift: number;
	baseX: number;
	baseY: number;
};
// A man sculpted off the page's thread (see shape), for the page to
// finish: the job and look it was for, and whose it is (see Plan.who), the
// frame it was asked for in and his size then.
export type Made = {
	key: string;
	shaped: Shaped;
	job: Job;
	look: Look;
	id: number;
	frame: number;
	scale: number;
};
// Workers that sculpt men for the page (see farm.ts).
export type Farm = {
	// Takes on sculpting this picture for the cache, if it can now (false:
	// it can't - it is made here or not at all).
	ask: (
		cache: SpriteCache,
		key: string,
		job: Job,
		look: Look,
		whose: { id: number; frame: number; scale: number },
	) => boolean;
	// Whether it is still being made.
	busy: (cache: SpriteCache, key: string) => boolean;
	// The pictures made for the cache since last asked.
	take: (cache: SpriteCache) => Made[];
};
// How many frames in a row a man may be drawn as he was; and how much
// more time a frame may take on new pictures while the picture is black.
const MAX_BEHIND = 3;
const DARK_BUDGET = 8;
const BLACK_BUDGET = 16;
export const makeSpriteCache = (): SpriteCache => ({
	kept: new Map(),
	area: 0,
	ids: new WeakMap(),
	next: 0,
	frame: 0,
	spent: 0,
	skipped: 0,
	last: new Map(),
	plans: new Map(),
});
// A new frame: none of its time spent yet.
export const startFrame = (cache: SpriteCache) => {
	cache.frame += 1;
	cache.spent = 0;
	cache.skipped = 0;
	cache.plans.clear();
};
const KEEP = 2400;
// And no more than this many pixels of them (four bytes each): beyond about
// this, more kept hardly saves drawing any.
const KEEP_AREA = 8_000_000;
// Done with: its memory let go of now, not whenever the page next tidies up
// - and nothing still to be drawn from it as the last picture of a man.
const drop = (cache: SpriteCache, k: Kept) => {
	cache.area -= k.w * k.h;
	for (const [id, was] of cache.last) {
		if (was.kept === k) {
			cache.last.delete(id);
		}
	}
	k.canvas.width = 0;
	k.canvas.height = 0;
};
// Every one of them let go of: the cache is done with (a new size, the game
// off the screen).
export const releaseSprites = (cache: SpriteCache) => {
	for (const k of cache.kept.values()) {
		k.canvas.width = 0;
		k.canvas.height = 0;
	}
	cache.kept.clear();
	cache.last.clear();
	cache.area = 0;
	cache.done = true;
};
export const keep = (
	cache: SpriteCache,
	key: string,
	from: HTMLCanvasElement,
	fx: number,
	fy: number,
	w: number,
	h: number,
	dx: number,
	dy: number,
): Kept => {
	// Copied over as pixels, not drawn: a sheet drawn from is held on to as
	// it was until the picture it was drawn into is next painted, so the
	// next man sculpted on it waited while all of it was copied - and it is
	// as big as the biggest man ever drawn on it (a close-up replay's: a
	// couple of million pixels, every new pose after it). (A sheet with a
	// picture on it we may not read - a face from another site - can only
	// be drawn from.)
	const canvas = document.createElement("canvas");
	canvas.width = w;
	canvas.height = h;
	const into = canvas.getContext("2d")!;
	let pixels: ImageData | undefined;
	try {
		pixels = from.getContext("2d")!.getImageData(fx, fy, w, h);
	} catch {
		pixels = undefined;
	}
	if (pixels) {
		into.putImageData(pixels, 0, 0);
	} else {
		into.drawImage(from, fx, fy, w, h, 0, 0, w, h);
	}
	return remember(cache, key, { canvas, w, h, dx, dy });
};
// A picture kept as the pixels it was finished in (see finishPixels - and
// field3d/sprite.ts).
export const keepPixels = (
	cache: SpriteCache,
	key: string,
	pixels: ImageData,
	dx: number,
	dy: number,
): Kept => {
	const canvas = document.createElement("canvas");
	canvas.width = pixels.width;
	canvas.height = pixels.height;
	canvas.getContext("2d")!.putImageData(pixels, 0, 0);
	return remember(cache, key, {
		canvas,
		w: pixels.width,
		h: pixels.height,
		dx,
		dy,
	});
};
const remember = (cache: SpriteCache, key: string, k: Kept): Kept => {
	cache.kept.set(key, k);
	cache.area += k.w * k.h;
	// The oldest go first.
	for (const [old, o] of cache.kept) {
		if (o === k || (cache.kept.size <= KEEP && cache.area <= KEEP_AREA)) {
			break;
		}
		cache.kept.delete(old);
		drop(cache, o);
	}
	return k;
};
// A reflection's bands, from the floor up: how much of his height each
// takes, and how strongly it shows (the rest, too faint to be worth
// drawing).
const SHINE_BANDS: [number, number][] = [
	[0.3, 1],
	[0.35, 0.45],
];
export const stamp = (
	ctx: CanvasRenderingContext2D,
	k: Kept,
	x: number,
	y: number,
	px: number,
	// His reflection in the floor first: how strong, and where the floor
	// under him is on the picture.
	shine?: { alpha: number; floorY: number },
) => {
	if (k.canvas.width === 0) {
		return;
	}
	const smoothing = ctx.imageSmoothingEnabled;
	ctx.imageSmoothingEnabled = false;
	if (shine) {
		// Upside down from the floor under him - as far under it as he is
		// over it - and fading away from it in steps: his feet plain in it,
		// his head hardly.
		const alpha = ctx.globalAlpha;
		const m = ctx.getTransform();
		ctx.transform(1, 0, 0, -1, 0, 2 * Math.round(shine.floorY));
		const X = Math.round(x + k.dx);
		const Y = Math.round(y + k.dy);
		let below = k.h;
		for (const [share, fade] of SHINE_BANDS) {
			const rows = Math.min(below, Math.ceil(k.h * share));
			if (rows <= 0) {
				break;
			}
			below -= rows;
			ctx.globalAlpha = alpha * shine.alpha * fade;
			ctx.drawImage(
				k.canvas,
				0,
				below,
				k.w,
				rows,
				X,
				Y + below * px,
				k.w * px,
				rows * px,
			);
		}
		ctx.setTransform(m);
		ctx.globalAlpha = alpha;
	}
	ctx.drawImage(
		k.canvas,
		Math.round(x + k.dx),
		Math.round(y + k.dy),
		k.w * px,
		k.h * px,
	);
	ctx.imageSmoothingEnabled = smoothing;
};
const CYCLE_FRAMES = 12;
// Steps through a bounce for the dribbling hand.
const DRIBBLE_FRAMES = 8;
const ACT_FRAMES = 20;
const TURNS = 24;
// How many times finer, with time to spare.
const FINE = 2;

type Steps = {
	cycle: number;
	dribble: number;
	act: number;
	turns: number;
	// A blend (or the hands closing on the ball) in so many steps, an arm
	// saying something in so many, and his hips' turn to so many degrees.
	blend: number;
	arm: number;
	legs: number;
};
const COARSE_STEPS: Steps = {
	cycle: CYCLE_FRAMES,
	dribble: DRIBBLE_FRAMES,
	act: ACT_FRAMES,
	turns: TURNS,
	blend: 4,
	arm: 3,
	legs: 8,
};
const FINE_STEPS: Steps = {
	cycle: CYCLE_FRAMES * FINE,
	dribble: DRIBBLE_FRAMES * FINE,
	act: ACT_FRAMES * FINE,
	turns: TURNS * FINE,
	blend: 4 * FINE,
	arm: 3 * FINE,
	legs: 8 / FINE,
};

// A move's frame: an act stepped through its frames (a long one - a dunk -
// gets more, so its quickest part, the slam, still shows), a cycle or a loop
// through its dozen.
const frameOf = (anim: AnimName, phase: number, k: Steps): number => {
	const a = ANIMS[anim];
	const frames = Math.max(k.act, a.n * 2 * (k.act / ACT_FRAMES));
	return a.kind === "act"
		? Math.round(Math.min(1, Math.max(0, phase)) * (frames - 1)) / (frames - 1)
		: Math.floor((((phase % 1) + 1) % 1) * k.cycle) / k.cycle;
};
const dribbleFrame = (d: number | undefined, k: Steps) =>
	d === undefined ? undefined : Math.floor(d * k.dribble) / k.dribble;
// Hands coming up for a pass, in a few steps.
const targetFrame = (t: number | undefined, k: Steps) =>
	t ? Math.ceil(t * k.arm) / k.arm : undefined;

// An arm saying something, in steps: its angles to a few degrees, how far
// into it in thirds (or finer).
const armSteps = (a: PlayerState["arm"], k: Steps): PlayerState["arm"] => {
	if (!a) {
		return undefined;
	}
	const w = a.w > 0.12 ? Math.max(1, Math.round(a.w * k.arm)) / k.arm : 0;
	const f = k.arm / 3;
	const by = (v: number, step: number) =>
		Math.round(v / (step / f)) * (step / f);
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

// Shuffling, the way he goes, in steps.
const wayStep = (w: [number, number], k: Steps): [number, number] => {
	const n = 40 / k.legs;
	return [Math.round(w[0] * n) / n, Math.round(w[1] * n) / n];
};
// A move he is easing out of, in steps (and the one it was easing out of).
const blendStep = (v: number, k: Steps) =>
	v > 0.12
		? Math.min(k.blend - 1, Math.max(1, Math.round(v * k.blend))) / k.blend
		: 0;
// Short steps, getting going or pulling up (see shortSteps): each leg's
// stride in sixths of a full one.
const LEN_STEPS = 4;
const lenStep = (
	len: [number, number] | undefined,
): [number, number] | undefined => {
	if (!len) {
		return undefined;
	}
	const by = (x: number) => Math.max(1, Math.round(x * LEN_STEPS)) / LEN_STEPS;
	const r = by(len[0]);
	const l = by(len[1]);
	return r >= 1 && l >= 1 ? undefined : [r, l];
};
// His legs on a stride, in the stride's frames and the blend's steps.
const stepStride = (s: Stride, k: Steps): Stride => {
	const len = lenStep(s.len);
	return {
		anim: s.anim,
		phase: frameOf(s.anim, s.phase, k),
		w: Math.round(s.w * k.blend) / k.blend,
		...(len ? { len } : {}),
	};
};
const strideKey = (s: Stride | undefined): string =>
	s ? `s${s.anim}${s.phase}~${s.w}${s.len ? `~${s.len}` : ""}` : "";
const stepBlend = (f: Blend, w: number, k: Steps): Blend => {
	const g = f.from;
	const gw = g ? blendStep(g.w, k) : 0;
	const len = lenStep(f.stepLen);
	return {
		anim: f.anim,
		phase: frameOf(f.anim, f.phase, k),
		...(len ? { stepLen: len } : {}),
		dribble: dribbleFrame(f.dribble, k),
		dribbleHand: f.dribbleHand,
		...(f.dribbleFrom ? { dribbleFrom: f.dribbleFrom } : {}),
		...(f.tight ? { tight: Math.round(f.tight * k.blend) / k.blend } : {}),
		...(f.stride ? { stride: stepStride(f.stride, k) } : {}),
		...(f.held ? { held: true } : {}),
		target: targetFrame(f.target, k),
		...(f.mirror ? { mirror: true } : {}),
		w,
		...(f.arms === undefined ? {} : { arms: blendStep(f.arms, k) }),
		...(f.down ? { down: Math.round(f.down * k.blend) / k.blend } : {}),
		...(f.way === undefined ? {} : { way: wayStep(f.way, k) }),
		...(f.pivot === undefined
			? {}
			: {
					pivot: [
						Math.round(f.pivot[0] / k.legs) * k.legs,
						Math.round(f.pivot[1] / k.legs) * k.legs,
						Math.round(f.pivot[2] * k.blend) / k.blend,
						Math.round(f.pivot[3] * k.blend) / k.blend,
					] as [number, number, number, number],
				}),
		...(g && gw > 0 ? { from: stepBlend(g, gw, k) } : {}),
	};
};
const blendKey = (f: Blend): string =>
	`${f.anim}${f.mirror ? "m" : ""}${f.phase}${f.stepLen ? `l${f.stepLen}` : ""}${f.dribble ?? ""}${f.dribbleHand ?? ""}${f.dribbleFrom ?? ""}${f.tight ? `t${f.tight}` : ""}${strideKey(f.stride)}${f.held ? "h" : ""}${f.target ?? ""}~${f.w}~${f.arms ?? ""}~${f.turns ?? ""}~${f.pivot ?? ""}~${f.down ?? ""}~${f.way ?? ""}${f.from ? `<${blendKey(f.from)}` : ""}`;

// The pose he is drawn in: his own, stepped to the sprite's frames and turns
// - and, easing out of his last move, a few steps of that.
const stepped = (st: PlayerState, k: Steps) => {
	const turn = Math.round(st.yaw / ((Math.PI * 2) / k.turns));
	const f = st.from;
	const w = f ? blendStep(f.w, k) : 0;
	return {
		arm: armSteps(st.arm, k),
		// His hips' turn to a few degrees; his hands closing on the ball in
		// steps.
		legs:
			st.legs === undefined ? undefined : Math.round(st.legs / k.legs) * k.legs,
		// Shuffling, the way he goes, the same.
		way: st.way === undefined ? undefined : wayStep(st.way, k),
		// Each leg turned on its own, and a foot up stepping round, the same.
		pivot:
			st.pivot === undefined
				? undefined
				: ([
						Math.round(st.pivot[0] / k.legs) * k.legs,
						Math.round(st.pivot[1] / k.legs) * k.legs,
						Math.round(st.pivot[2] * k.blend) / k.blend,
						Math.round(st.pivot[3] * k.blend) / k.blend,
					] as [number, number, number, number]),
		grip:
			st.grip === undefined
				? undefined
				: Math.floor(st.grip * k.blend) / k.blend,
		phase: frameOf(st.anim, st.phase, k),
		stepLen: lenStep(st.stepLen),
		// How hard he is going at it, in steps.
		drive: st.drive
			? Math.round(st.drive * k.arm) / k.arm || undefined
			: undefined,
		turn: ((turn % k.turns) + k.turns) % k.turns,
		yaw: (turn * Math.PI * 2) / k.turns,
		dribble: dribbleFrame(st.dribble, k),
		// His dribble kept in by his hip, in steps.
		tight: st.tight ? Math.round(st.tight * k.blend) / k.blend : undefined,
		stride: st.stride ? stepStride(st.stride, k) : undefined,
		target: targetFrame(st.target, k),
		down: st.down ? Math.round(st.down * k.blend) / k.blend : undefined,
		from:
			f && (w > 0 || (f.arms ?? 0) > 0)
				? {
						...stepBlend(f, w, k),
						// Each leg's turn as the move he is easing out of has it,
						// the way he faces now, to a few degrees.
						turns: ([0, 1] as const).map(
							(i) =>
								Math.round(legTurnFrom(f, i, st.yaw, st.anim) / k.legs) *
								k.legs,
						) as [number, number],
					}
				: undefined,
	};
};

// Where he is drawn this frame and in which picture: worked out once a
// frame (see prepareSprites), used again when he is drawn.
type Plan = {
	base: Projected;
	lift: number;
	left: number;
	right: number;
	upper: number;
	lower: number;
	pose: ReturnType<typeof stepped>;
	// His size on the picture, in steps of 4%.
	scale: number;
	// The ball in his hands (drawn with him, not on its own - see scene.ts).
	held: boolean;
	// His look's number in the cache (in his pictures' keys), and who he is
	// (his pid): the picture he was last drawn in is his whatever he looked
	// like in it - so a new look (his face come in, the warm-up top off) is
	// made as any new pose of his is, not all at once on the page.
	id: number;
	who: number;
	key?: string;
	// Left out of this frame: the picture black, and no time to make his.
	leftOut?: boolean;
	// His new picture being made off the page's thread: drawn as he was
	// meanwhile.
	farmed?: boolean;
};

const planOf = (
	cam: Camera,
	st: PlayerState,
	body: Body,
	look: Look,
	px: number,
	cache?: SpriteCache,
): Plan | undefined => {
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
		return undefined;
	}
	const fine = cache?.fine === true;
	const pose = stepped(st, fine ? FINE_STEPS : COARSE_STEPS);
	const scale = Math.round(Math.log(k) / Math.log(1.04));
	const plan: Plan = {
		base,
		lift,
		left,
		right,
		upper,
		lower,
		pose,
		scale,
		held: st.holding === true,
		id: 0,
		who: st.pid,
	};
	if (cache) {
		const known = cache.ids.get(look);
		const id = known ?? cache.next++;
		if (known === undefined) {
			cache.ids.set(look, id);
		}
		const f = pose.from;
		const a = pose.arm;
		plan.id = id;
		plan.key = `${fine ? "f" : ""}${id}|${st.anim}${st.mirror ? "m" : ""}|${pose.phase}${pose.stepLen ? `l${pose.stepLen}` : ""}${pose.drive ? `v${pose.drive}` : ""}|${pose.turn}|${scale}|${px}|${st.holding ? 1 : 0}${pose.grip ?? ""}|${pose.legs ?? ""}${pose.way === undefined ? "" : `w${pose.way}`}${pose.pivot ? `p${pose.pivot}` : ""}${pose.down ? `d${pose.down}` : ""}|${pose.dribble ?? ""}${st.dribbleHand ?? ""}${st.dribbleFrom ?? ""}${pose.tight ? `t${pose.tight}` : ""}${strideKey(pose.stride)}|${pose.target ?? ""}${
			f ? `|${blendKey(f)}` : ""
		}${a ? `|${a.hand}${a.point ? "p" : ""}${a.tuck ? `t${a.tuck}` : ""}${a.sh},${a.el},${a.ab},${a.wr}~${a.w}` : ""}`;
	}
	return plan;
};

// Whether he may be drawn this frame in the picture he had last frame (out
// of time for a new one): if he had one a frame ago, not for too many frames
// already (more in slow motion), and not much bigger or smaller since - no
// lurch at a cut. (Black, a man much bigger or smaller since is left out
// till his picture is made: the picture is held black for him - see
// Court3D - so it comes up with everybody the size he is.) And never with
// the ball in his hands in it when he has let it go, or without it once he
// has it: the ball would be on the picture twice, or not at all.
const asWas = (cache: SpriteCache, plan: Plan) => {
	const was = cache.last.get(plan.who);
	return was &&
		was.behind <
			Math.round(MAX_BEHIND / Math.min(1, Math.max(0.35, cache.pace ?? 1))) &&
		was.frame === cache.frame - 1 &&
		Math.abs(was.scale - plan.scale) <= 1 &&
		(was.held ?? false) === plan.held
		? was
		: undefined;
};
const overBudget = (cache: SpriteCache) =>
	cache.budget !== undefined &&
	cache.spent >=
		cache.budget * (cache.black ? BLACK_BUDGET : cache.dark ? DARK_BUDGET : 1);

// What making his picture for the plan takes (see Job).
const jobOf = (
	plan: Plan,
	cam: Camera,
	st: PlayerState,
	body: Body,
	px: number,
): Job => {
	const { base, lift, left, right, upper, lower, pose } = plan;
	return {
		cam,
		// Drawn standing on the floor; a jump only moves the picture up.
		st: {
			...st,
			z: 0,
			phase: pose.phase,
			stepLen: pose.stepLen,
			drive: pose.drive,
			yaw: pose.yaw,
			dribble: pose.dribble,
			tight: pose.tight,
			stride: pose.stride,
			target: pose.target,
			from: pose.from,
			arm: pose.arm,
			legs: pose.legs,
			way: pose.way,
			pivot: pose.pivot,
			grip: pose.grip,
			down: pose.down,
		},
		body,
		px,
		left,
		right,
		upper,
		lower,
		lift,
		baseX: base.x,
		baseY: base.y,
	};
};

// HIS BODY, SCULPTED (see sculpt.ts) and rimmed: all of him but his head,
// and nothing but arithmetic - most of the work of a new picture, and done
// as readily in a worker as on the page (see farm.ts). The sheet's box he
// takes (w x h, at ox, oy on the picture) and the part of it with anything
// in it (rx0, ry0, rw x rh); his body's pixels (and an arm in front of his
// face, if one is) - all of the box, or `cut` down to that part, at `at`.
export type Shaped = {
	w: number;
	h: number;
	ox: number;
	oy: number;
	rx0: number;
	ry0: number;
	rw: number;
	rh: number;
	box: Box;
	head: { x: number; y: number; r: number };
	// Where his head is on his body, for the page to draw it there (see
	// finish).
	headAt: V3;
	at: [number, number];
	img: ImageData;
	over?: ImageData;
};
const cutTo = (
	img: ImageData,
	x0: number,
	y0: number,
	w: number,
	h: number,
) => {
	const out = new ImageData(Math.max(1, w), Math.max(1, h));
	for (let y = 0; y < h; y++) {
		const from = ((y0 + y) * img.width + x0) * 4;
		out.data.set(img.data.subarray(from, from + w * 4), y * w * 4);
	}
	return out;
};
export const shape = (job: Job, look: Look, cut = false): Shaped => {
	const { cam, st: posed, body, px, left, right, upper, lower, lift } = job;
	const w = Math.max(4, Math.ceil((right - left) / px) + 2);
	const h = Math.max(4, Math.ceil((lower - upper) / px) + 2);
	// Screen coordinates, shrunk to sprite pixels, with a pixel of margin.
	const ox = left - px;
	const oy = upper + lift - px;
	const made = sculpt(cam, posed, body, look, px, ox, oy, w, h);
	const box = made.box;
	rim(made.img.data, w, h, undefined, box);
	if (made.over) {
		rim(made.over.data, w, h, undefined, box);
	}
	// Only what is drawn of him (and the rim round it) goes on the sheet: the
	// rest of his box is clear, and most of it.
	const rx0 = Math.max(0, box.x0 - 1);
	const ry0 = Math.max(0, box.y0 - 1);
	const rw = Math.max(0, Math.min(w, box.x1 + 2) - rx0);
	const rh = Math.max(0, Math.min(h, box.y1 + 2) - ry0);
	return {
		w,
		h,
		ox,
		oy,
		rx0,
		ry0,
		rw,
		rh,
		box,
		head: made.head,
		headAt: headOf(posed, body),
		at: cut ? [rx0, ry0] : [0, 0],
		img: cut ? cutTo(made.img, rx0, ry0, rw, rh) : made.img,
		...(made.over
			? { over: cut ? cutTo(made.over, rx0, ry0, rw, rh) : made.over }
			: {}),
	};
};

// Where his head is drawn on the sheet: round it, his face and hair are
// drawn and rimmed (see finish).
const headBox = (sh: Shaped): Box => {
	const { w, h, head } = sh;
	const hr = head.r;
	return {
		x0: Math.max(0, Math.floor(head.x - hr * 1.9) - 2),
		y0: Math.max(0, Math.floor(head.y - hr * 2) - 2),
		x1: Math.min(w, Math.ceil(head.x + hr * 1.9) + 2),
		y1: Math.min(h, Math.ceil(head.y + hr * 1.6) + 2),
	};
};
// What is kept of the sheet: just what is drawn of him (the rim round it,
// his head and all) - x0, y0 to x1, y1 (nothing, if x1 <= x0) - and where
// that sits from where he stands (dx, dy).
const keptBox = (sh: Shaped, hb: Box, job: Job) => {
	const { w, h, ox, oy, box } = sh;
	const x0 = Math.max(0, Math.min(box.x0 - 1, hb.x0));
	const y0 = Math.max(0, Math.min(box.y0 - 1, hb.y0));
	return {
		w,
		h,
		ox,
		oy,
		x0,
		y0,
		x1: Math.min(w, Math.max(box.x1 + 2, hb.x1)),
		y1: Math.min(h, Math.max(box.y1 + 2, hb.y1)),
		dx: ox + x0 * job.px - job.baseX,
		dy: oy + y0 * job.px - job.baseY,
	};
};
// His face, shrunk to the sprite's scale, drawn on the sheet over what is
// there - its top left corner at (fx, fy) on the sheet.
// (Whatever drawing his head leaves set on the sheet - the joins of its
// ink, its caps - is undone after: the next man's head is drawn the same
// whoever was drawn on the sheet before him.)
const drawHeadOn = (
	s: CanvasRenderingContext2D,
	sh: Shaped,
	job: Job,
	look: Look,
	fx: number,
	fy: number,
) => {
	const { cam, st: posed, body, px } = job;
	s.save();
	s.setTransform(1 / px, 0, 0, 1 / px, -sh.ox / px - fx, -sh.oy / px - fy);
	drawHeadAt(s, cam, posed, body, look, sh.headAt);
	s.restore();
	s.setTransform(1, 0, 0, 1, 0, 0);
};
// The rim round his face (the head box's pixels, read back), just where it
// is new: the rest of him is done. (His body's own pixels there: none
// outside what it has.)
const rimFace = (face: ImageData, sh: Shaped, hb: Box) => {
	const { img } = sh;
	const [ax, ay] = sh.at;
	const fw = hb.x1 - hb.x0;
	const d = img.data;
	const iw = img.width;
	const ih = img.height;
	rim(face.data, fw, hb.y1 - hb.y0, (i) => {
		const x = hb.x0 + (i % fw) - ax;
		const y = hb.y0 + Math.floor(i / fw) - ay;
		return (
			x < 0 || y < 0 || x >= iw || y >= ih || d[(y * iw + x) * 4 + 3]! < SOLID
		);
	});
};
const roomFor = (scratch: Scratch, w: number, h: number) => {
	const { canvas } = scratch;
	if (canvas.width < w || canvas.height < h) {
		canvas.width = Math.max(canvas.width, w);
		canvas.height = Math.max(canvas.height, h);
	}
};

// HIS PICTURE, FINISHED AS PIXELS, to be kept: his body's pixels as they
// came (from a hand, or sculpted here), and over them his head - the only
// part of him drawn on a canvas, so the only part put on the scratch sheet
// and read back, with just the bit of his body under it. (Finished on the
// sheet, all of him was put on it and read back off it: two copies of every
// pixel of him, the most of the page's share of a new picture - see
// farm.ts.) Nothing, if he can only be finished on the sheet (see finish):
// an arm in front of his face, or a head that may not be read.
export const finishPixels = (
	sh: Shaped,
	job: Job,
	look: Look,
	scratch: Scratch,
): (ReturnType<typeof keptBox> & { pixels: ImageData }) | undefined => {
	if (sh.over) {
		return undefined;
	}
	const { rx0, ry0, rw, rh, img } = sh;
	const [ax, ay] = sh.at;
	const hb = headBox(sh);
	const k = keptBox(sh, hb, job);
	const ow = k.x1 - k.x0;
	const oh = k.y1 - k.y0;
	if (ow <= 0 || oh <= 0) {
		return undefined;
	}
	const pixels = new ImageData(ow, oh);
	const out = pixels.data;
	const iw = img.width;
	for (let y = 0; y < rh; y++) {
		const from = ((ry0 - ay + y) * iw + rx0 - ax) * 4;
		out.set(
			img.data.subarray(from, from + rw * 4),
			((ry0 - k.y0 + y) * ow + rx0 - k.x0) * 4,
		);
	}
	const fw = hb.x1 - hb.x0;
	const fh = hb.y1 - hb.y0;
	if (fw > 0 && fh > 0) {
		roomFor(scratch, fw, fh);
		const s = scratch.ctx;
		s.setTransform(1, 0, 0, 1, 0, 0);
		s.clearRect(0, 0, fw, fh);
		const bx0 = Math.max(rx0, hb.x0);
		const by0 = Math.max(ry0, hb.y0);
		const bx1 = Math.min(rx0 + rw, hb.x1);
		const by1 = Math.min(ry0 + rh, hb.y1);
		if (bx1 > bx0 && by1 > by0) {
			s.putImageData(
				img,
				ax - hb.x0,
				ay - hb.y0,
				bx0 - ax,
				by0 - ay,
				bx1 - bx0,
				by1 - by0,
			);
		}
		drawHeadOn(s, sh, job, look, hb.x0, hb.y0);
		let face: ImageData;
		try {
			face = s.getImageData(0, 0, fw, fh);
		} catch {
			return undefined;
		}
		rimFace(face, sh, hb);
		const fd = face.data;
		for (let y = 0; y < fh; y++) {
			out.set(
				fd.subarray(y * fw * 4, (y + 1) * fw * 4),
				((hb.y0 - k.y0 + y) * ow + hb.x0 - k.x0) * 4,
			);
		}
	}
	return { ...k, pixels };
};

// His picture, finished on the scratch sheet: his body put on it, his head
// drawn over (always on the page - see farm.ts), and the rim round that.
// The part of the sheet with anything drawn in it, and where that sits
// from where he stands (see keptBox).
export const finish = (sh: Shaped, job: Job, look: Look, scratch: Scratch) => {
	const { w, h, rx0, ry0, rw, rh, img } = sh;
	const [ax, ay] = sh.at;
	const { ctx: s } = scratch;
	roomFor(scratch, w, h);
	s.setTransform(1, 0, 0, 1, 0, 0);
	s.clearRect(0, 0, w, h);
	if (rw > 0 && rh > 0) {
		s.putImageData(img, ax, ay, rx0 - ax, ry0 - ay, rw, rh);
	}
	drawHeadOn(s, sh, job, look, 0, 0);
	const hb = headBox(sh);
	if (hb.x1 > hb.x0 && hb.y1 > hb.y0) {
		// (Never let a picture we may not read stop the game: no rim then.)
		let face: ImageData | undefined;
		try {
			face = s.getImageData(hb.x0, hb.y0, hb.x1 - hb.x0, hb.y1 - hb.y0);
		} catch {
			face = undefined;
		}
		if (face) {
			rimFace(face, sh, hb);
			s.putImageData(face, hb.x0, hb.y0);
		}
	}
	if (sh.over) {
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
		if (rw > 0 && rh > 0) {
			t.ctx.putImageData(sh.over, ax, ay, rx0 - ax, ry0 - ay, rw, rh);
			s.drawImage(t.canvas, rx0, ry0, rw, rh, rx0, ry0, rw, rh);
		}
	}
	return keptBox(sh, hb, job);
};

// His picture, finished and kept: a new picture for the cache (nothing, if
// there is nothing of him to keep - then left on the sheet).
const keepMade = (
	cache: SpriteCache,
	key: string,
	scratch: Scratch,
	sh: Shaped,
	job: Job,
	look: Look,
): Kept | ReturnType<typeof finish> => {
	const f = finishPixels(sh, job, look, scratch);
	if (f) {
		return keepPixels(cache, key, f.pixels, f.dx, f.dy);
	}
	const p = finish(sh, job, look, scratch);
	return p.x1 > p.x0 && p.y1 > p.y0
		? keep(
				cache,
				key,
				scratch.canvas,
				p.x0,
				p.y0,
				p.x1 - p.x0,
				p.y1 - p.y0,
				p.dx,
				p.dy,
			)
		: p;
};

// His picture for the plan, made: kept, with a cache - or, without, left on
// the scratch sheet to be drawn from.
const make = (
	plan: Plan,
	scratch: Scratch,
	cam: Camera,
	st: PlayerState,
	body: Body,
	look: Look,
	px: number,
	cache?: SpriteCache,
): Kept | { w: number; h: number; ox: number; oy: number } => {
	const began = cache ? performance.now() : 0;
	const job = jobOf(plan, cam, st, body, px);
	const sh = shape(job, look);
	if (!cache || !plan.key) {
		return finish(sh, job, look, scratch);
	}
	// Kept for next time.
	const made = keepMade(cache, plan.key, scratch, sh, job, look);
	cache.spent += performance.now() - began;
	return made;
};

// THE NEW PICTURES A FRAME NEEDS, MADE NEAREST FIRST. The frame is drawn
// from the back of the picture to the front, so left to that, a frame out
// of time for new pictures ran out on the men nearest the camera - the
// biggest on the picture - and drew them behind their pose. Made here first
// instead, before anything is drawn, nearest first while there is time (and
// whoever has no picture to be drawn in meanwhile, regardless) - so the men
// left a frame behind are the furthest and smallest.
// Which picture he is drawn in this frame - his own for this pose, kept;
// the one he had, if he may be drawn as he was; or none yet (to be made
// now - or, black and out of time, left out) - and the books kept on it as
// if he were drawn.
const pick = (cache: SpriteCache, plan: Plan): Kept | "make" | "out" => {
	const kept = plan.key === undefined ? undefined : cache.kept.get(plan.key);
	if (kept && plan.key !== undefined) {
		// Kept longest are the ones still in use: to the back of the line.
		cache.kept.delete(plan.key);
		cache.kept.set(plan.key, kept);
		cache.last.set(plan.who, {
			kept,
			frame: cache.frame,
			behind: 0,
			scale: plan.scale,
			held: plan.held,
		});
		return kept;
	}
	if (plan.leftOut) {
		return "out";
	}
	// Out of time this frame - or his new picture being made off the page's
	// thread - and drawn a moment ago: as he was.
	if (plan.farmed || overBudget(cache)) {
		const was = asWas(cache, plan);
		if (was) {
			was.frame = cache.frame;
			was.behind += 1;
			return was.kept;
		}
		if (cache.dark) {
			plan.leftOut = true;
			cache.skipped += 1;
			return "out";
		}
	}
	return "make";
};

// The men sculpted off the page's thread since the last frame, finished
// and kept - and each the picture a man is drawn in meanwhile, if it is of
// a later pose of his than the one he has: he is then that much less
// behind.
const took = (cache: SpriteCache, farm: Farm, scratch: Scratch) => {
	const began = performance.now();
	for (const m of farm.take(cache)) {
		if (cache.kept.has(m.key)) {
			continue;
		}
		const kept = keepMade(cache, m.key, scratch, m.shaped, m.job, m.look);
		if (!("canvas" in kept)) {
			continue;
		}
		const was = cache.last.get(m.id);
		if (
			m.frame < cache.frame &&
			(!was ||
				(was.frame === cache.frame - 1 && was.frame - was.behind < m.frame))
		) {
			cache.last.set(m.id, {
				kept,
				frame: cache.frame - 1,
				behind: cache.frame - 1 - m.frame,
				scale: m.scale,
				held: m.job.st.holding === true,
			});
		}
	}
	cache.spent += performance.now() - began;
};

export const prepareSprites = (
	scratch: Scratch,
	cam: Camera,
	men: { st: PlayerState; body: Body; look: Look }[],
	px: number,
	cache: SpriteCache,
) => {
	const farm = cache.farm;
	if (farm) {
		took(cache, farm, scratch);
	}
	const all: { man: (typeof men)[number]; plan: Plan }[] = [];
	const todo: { man: (typeof men)[number]; plan: Plan; wait: boolean }[] = [];
	for (const man of men) {
		const plan = planOf(cam, man.st, man.body, man.look, px, cache);
		if (plan?.key === undefined) {
			continue;
		}
		cache.plans.set(man.st, plan);
		all.push({ man, plan });
		if (!cache.kept.has(plan.key)) {
			todo.push({ man, plan, wait: asWas(cache, plan) !== undefined });
		}
	}
	todo.sort((a, b) => b.plan.base.k - a.plan.base.k);
	for (const { man, plan, wait } of todo) {
		const key = plan.key!;
		// Made off the page's thread, if it can be, and drawn as he was till
		// it comes.
		const farmed = () =>
			farm !== undefined &&
			(farm.busy(cache, key) ||
				farm.ask(cache, key, jobOf(plan, cam, man.st, man.body, px), man.look, {
					id: plan.who,
					frame: cache.frame,
					scale: plan.scale,
				}));
		if (wait && farmed()) {
			plan.farmed = true;
			continue;
		}
		// Out of time: drawn as he was - or, the picture black, left out of
		// the frame (and the picture held black for him: see Court3D) till
		// it is made, here or off the page's thread.
		if (overBudget(cache) && (wait || cache.dark)) {
			if (!wait) {
				farmed();
			}
			continue;
		}
		make(plan, scratch, cam, man.st, man.body, man.look, px, cache);
	}
	// All black, nobody is drawn - but the books are kept as if they were,
	// so a man made a frame ago may be drawn as he was the next, as in any
	// other frame, rather than made again for each new pose while it stays
	// black.
	if (cache.black) {
		for (const { plan } of all) {
			pick(cache, plan);
		}
	}
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
	// How strongly he shows in the polished floor under him (0: not at all -
	// in a chair, behind the table).
	reflect = 0,
) => {
	const plan = cache?.plans.get(st) ?? planOf(cam, st, body, look, px, cache);
	if (!plan) {
		return;
	}
	const { base, lift } = plan;
	const shine = reflect > 0 ? { alpha: reflect, floorY: base.y } : undefined;
	if (cache && plan.key !== undefined) {
		const was = pick(cache, plan);
		if (was === "out") {
			return;
		}
		if (was !== "make") {
			stamp(ctx, was, base.x, base.y - lift, px, shine);
			return;
		}
	}
	const made = make(plan, scratch, cam, st, body, look, px, cache);
	if ("canvas" in made) {
		cache?.last.set(plan.who, {
			kept: made,
			frame: cache.frame,
			behind: 0,
			scale: plan.scale,
			held: plan.held,
		});
		stamp(ctx, made, base.x, base.y - lift, px, shine);
		return;
	}
	const smoothing = ctx.imageSmoothingEnabled;
	ctx.imageSmoothingEnabled = false;
	ctx.drawImage(
		scratch.canvas,
		0,
		0,
		made.w,
		made.h,
		Math.round(made.ox),
		Math.round(made.oy - lift),
		made.w * px,
		made.h * px,
	);
	ctx.imageSmoothingEnabled = smoothing;
};
