import { project, type Camera, type Projected } from "./camera.ts";
import {
	legTurnFrom,
	type Blend,
	type PlayerState,
	type Stride,
} from "./evaluate.ts";
import { drawHeadAt, type Look } from "./figure.ts";
import { ANIMS, type AnimName, type Body } from "./poses.ts";
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
	// many were (see Court3D, which holds the black till none are).
	dark?: boolean;
	skipped: number;
	// How fast the moment drawn moves on, to the wall clock's (1 at real
	// speed; a slow-motion replay less): poses come round that much more
	// slowly, and a man may be drawn as he was for that many more frames.
	pace?: number;
	last: Map<
		number,
		{ kept: Kept; frame: number; behind: number; scale: number }
	>;
	// This frame's plan for each man made ready (see prepareSprites).
	plans: Map<PlayerState, Plan>;
};
// How many frames in a row a man may be drawn as he was; and how much
// more time a frame may take on new pictures while the picture is black.
const MAX_BEHIND = 3;
const DARK_BUDGET = 8;
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
	const canvas = document.createElement("canvas");
	canvas.width = w;
	canvas.height = h;
	// Copied over as pixels, not drawn: a sheet drawn from is held on to as
	// it was until the picture it was drawn into is next painted, so the
	// next man sculpted on it waited while all of it was copied - and it is
	// as big as the biggest man ever drawn on it (a close-up replay's: a
	// couple of million pixels, every new pose after it).
	canvas
		.getContext("2d")!
		.putImageData(from.getContext("2d")!.getImageData(fx, fy, w, h), 0, 0);
	const k: Kept = { canvas, w, h, dx, dy };
	cache.kept.set(key, k);
	cache.area += w * h;
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
export const stamp = (
	ctx: CanvasRenderingContext2D,
	k: Kept,
	x: number,
	y: number,
	px: number,
) => {
	if (k.canvas.width === 0) {
		return;
	}
	const smoothing = ctx.imageSmoothingEnabled;
	ctx.imageSmoothingEnabled = false;
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
// His legs on a stride, in the stride's frames and the blend's steps.
const stepStride = (s: Stride, k: Steps): Stride => ({
	anim: s.anim,
	phase: frameOf(s.anim, s.phase, k),
	w: Math.round(s.w * k.blend) / k.blend,
});
const strideKey = (s: Stride | undefined): string =>
	s ? `s${s.anim}${s.phase}~${s.w}` : "";
const stepBlend = (f: Blend, w: number, k: Steps): Blend => {
	const g = f.from;
	const gw = g ? blendStep(g.w, k) : 0;
	return {
		anim: f.anim,
		phase: frameOf(f.anim, f.phase, k),
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
	`${f.anim}${f.mirror ? "m" : ""}${f.phase}${f.dribble ?? ""}${f.dribbleHand ?? ""}${f.dribbleFrom ?? ""}${f.tight ? `t${f.tight}` : ""}${strideKey(f.stride)}${f.held ? "h" : ""}${f.target ?? ""}~${f.w}~${f.arms ?? ""}~${f.turns ?? ""}~${f.pivot ?? ""}~${f.down ?? ""}~${f.way ?? ""}${f.from ? `<${blendKey(f.from)}` : ""}`;

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
	id: number;
	key?: string;
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
		id: 0,
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
		plan.key = `${fine ? "f" : ""}${id}|${st.anim}${st.mirror ? "m" : ""}|${pose.phase}|${pose.turn}|${scale}|${px}|${st.holding ? 1 : 0}${pose.grip ?? ""}|${pose.legs ?? ""}${pose.way === undefined ? "" : `w${pose.way}`}${pose.pivot ? `p${pose.pivot}` : ""}${pose.down ? `d${pose.down}` : ""}|${pose.dribble ?? ""}${st.dribbleHand ?? ""}${st.dribbleFrom ?? ""}${pose.tight ? `t${pose.tight}` : ""}${strideKey(pose.stride)}|${pose.target ?? ""}${
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
// Court3D - so it comes up with everybody the size he is.)
const asWas = (cache: SpriteCache, plan: Plan) => {
	const was = cache.last.get(plan.id);
	return was &&
		was.behind <
			Math.round(MAX_BEHIND / Math.min(1, Math.max(0.35, cache.pace ?? 1))) &&
		was.frame === cache.frame - 1 &&
		Math.abs(was.scale - plan.scale) <= 1
		? was
		: undefined;
};
const overBudget = (cache: SpriteCache) =>
	cache.budget !== undefined &&
	cache.spent >= cache.budget * (cache.dark ? DARK_BUDGET : 1);

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
	const { base, lift, left, right, upper, lower, pose } = plan;
	const began = cache ? performance.now() : 0;
	// Drawn standing on the floor; a jump only moves the picture up.
	const posed: PlayerState = {
		...st,
		z: 0,
		phase: pose.phase,
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
	const made = sculpt(cam, posed, body, look, px, ox, oy, w, h);
	const img = made.img;
	const d = img.data;
	rim(d, w, h, undefined, made.box);
	// Only what is drawn of him (and the rim round it) is put on the sheet:
	// the rest of his box is clear already, and most of it.
	const box = made.box;
	const rx0 = Math.max(0, box.x0 - 1);
	const ry0 = Math.max(0, box.y0 - 1);
	const rw = Math.min(w, box.x1 + 2) - rx0;
	const rh = Math.min(h, box.y1 + 2) - ry0;
	if (rw > 0 && rh > 0) {
		s.putImageData(img, 0, 0, rx0, ry0, rw, rh);
	}
	// His face, shrunk to the sprite's scale - and the rim round it, just
	// where it is new: the rest of him is done.
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
		const top = made.over;
		rim(top.data, w, h, undefined, made.box);
		if (rw > 0 && rh > 0) {
			t.ctx.putImageData(top, 0, 0, rx0, ry0, rw, rh);
			s.drawImage(t.canvas, rx0, ry0, rw, rh, rx0, ry0, rw, rh);
		}
	}

	// Kept for next time: just what is drawn of him (the rim round it, his
	// head and all).
	if (cache && plan.key) {
		const x0 = Math.max(0, Math.min(box.x0 - 1, hx0));
		const y0 = Math.max(0, Math.min(box.y0 - 1, hy0));
		const x1 = Math.min(w, Math.max(box.x1 + 2, hx1));
		const y1 = Math.min(h, Math.max(box.y1 + 2, hy1));
		const kept =
			x1 > x0 && y1 > y0
				? keep(
						cache,
						plan.key,
						canvas,
						x0,
						y0,
						x1 - x0,
						y1 - y0,
						ox + x0 * px - base.x,
						oy + y0 * px - base.y,
					)
				: undefined;
		cache.spent += performance.now() - began;
		if (kept) {
			return kept;
		}
	}
	return { w, h, ox, oy };
};

// THE NEW PICTURES A FRAME NEEDS, MADE NEAREST FIRST. The frame is drawn
// from the back of the picture to the front, so left to that, a frame out
// of time for new pictures ran out on the men nearest the camera - the
// biggest on the picture - and drew them behind their pose. Made here first
// instead, before anything is drawn, nearest first while there is time (and
// whoever has no picture to be drawn in meanwhile, regardless) - so the men
// left a frame behind are the furthest and smallest.
export const prepareSprites = (
	scratch: Scratch,
	cam: Camera,
	men: { st: PlayerState; body: Body; look: Look }[],
	px: number,
	cache: SpriteCache,
) => {
	const todo: { man: (typeof men)[number]; plan: Plan; wait: boolean }[] = [];
	for (const man of men) {
		const plan = planOf(cam, man.st, man.body, man.look, px, cache);
		if (plan?.key === undefined) {
			continue;
		}
		cache.plans.set(man.st, plan);
		if (!cache.kept.has(plan.key)) {
			todo.push({ man, plan, wait: asWas(cache, plan) !== undefined });
		}
	}
	todo.sort((a, b) => b.plan.base.k - a.plan.base.k);
	for (const { man, plan, wait } of todo) {
		// Out of time: drawn as he was - or, the picture black, left out of
		// the frame (see drawSprite).
		if (overBudget(cache) && (wait || cache.dark)) {
			continue;
		}
		make(plan, scratch, cam, man.st, man.body, man.look, px, cache);
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
) => {
	const plan = cache?.plans.get(st) ?? planOf(cam, st, body, look, px, cache);
	if (!plan) {
		return;
	}
	const { base, lift } = plan;
	if (cache && plan.key !== undefined) {
		const kept = cache.kept.get(plan.key);
		if (kept) {
			// Kept longest are the ones still in use: to the back of the line.
			cache.kept.delete(plan.key);
			cache.kept.set(plan.key, kept);
			stamp(ctx, kept, base.x, base.y - lift, px);
			cache.last.set(plan.id, {
				kept,
				frame: cache.frame,
				behind: 0,
				scale: plan.scale,
			});
			return;
		}
		// Out of time this frame, and drawn a moment ago: as he was.
		if (overBudget(cache)) {
			const was = asWas(cache, plan);
			if (was) {
				stamp(ctx, was.kept, base.x, base.y - lift, px);
				was.frame = cache.frame;
				was.behind += 1;
				return;
			}
			if (cache.dark) {
				cache.skipped += 1;
				return;
			}
		}
	}
	const made = make(plan, scratch, cam, st, body, look, px, cache);
	if ("canvas" in made) {
		cache?.last.set(plan.id, {
			kept: made,
			frame: cache.frame,
			behind: 0,
			scale: plan.scale,
		});
		stamp(ctx, made, base.x, base.y - lift, px);
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
