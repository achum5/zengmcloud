import { project, type Camera } from "../court3d/camera.ts";
import { lerpPose, type Body, type Pose, type V3 } from "../court3d/poses.ts";
import {
	keep,
	rim,
	stamp,
	type Scratch,
	type SpriteCache,
} from "../court3d/sprite.ts";
import { FIGURE_SCALE } from "./body.ts";
import type { Side } from "./geometry.ts";
import type { FieldLook } from "./kit.ts";
import type { BallHold } from "./director.ts";
import {
	actFrames,
	gripsOf,
	heldBall,
	holding,
	moveKind,
	poseOfMove,
	strideLift,
	withArms,
	type Move,
} from "./poses.ts";
import { sculptFigure, type Figure, type HeldBall } from "./sculpt.ts";

// THE PLAYERS ON THE FIELD, DRAWN - the way the court draws its own (see
// court3d/sprite.ts, whose cache this shares): each sculpted on a sheet of
// his own at the picture's resolution, a soft dark rim round him, stamped
// into the picture and kept for the next time the same pose comes round.
// His animation runs in frames and he turns in 36 directions (more of each
// with time to spare, the way the court does it), so the same few
// pictures come round again and again, and a whole formation set in its
// stances costs a picture apiece.

export type FieldPlayerState = {
	// Who: his pid (or one of his own for anybody else on the field), and
	// whose side he is on.
	id: number;
	team: Side;
	x: number;
	y: number;
	z: number;
	// Which way he faces, radians (0 = toward the right end zone).
	yaw: number;
	move: Move;
	// How far through it (0 to 1).
	phase: number;
	// Done the other way round: his left doing what his right does.
	mirrored?: boolean;
	// Short steps: his strides (and the ones he was taking, just after a
	// change) this share of full ones.
	len?: number;
	// Turning where he stands, a foot at a time: which way each foot points
	// on the turf, right and left, and how far up off it each is (a full
	// step 1 - see evaluate.ts pivotOf).
	pivot?: [number, number, number, number];
	// Just after a change - of move, of what his arms are doing, of how he
	// has the ball: the last as it was when it changed, and how much of his
	// pose is still that (1 all, 0 none).
	from?: Blend;
	// The ball with him, and how he has it.
	ball?: BallHold;
	// His arms in an act of their own over the rest of him, and how far
	// through it.
	arms?: { move: Move; phase: number };
	// His helmet off, in his hand: on the sideline.
	bare?: true;
	// The ball he has on its way to another man's hands, or coming to his:
	// drawn on its own, not in his (see evaluate.ts).
	unseen?: true;
};

type Blend = {
	move: Move;
	phase: number;
	mirrored?: boolean;
	w: number;
	arms?: { move: Move; phase: number };
	ball?: BallHold;
	// How fast, a ms: his move through its phase, the one he was through
	// its own, and the blend out of it (for it to keep step with their
	// frames - see weightOf).
	rate?: { move: number; from: number; w: number };
	// That one itself still partly what he was before it, when it ended.
	under?: Under;
	// Into or out of standing turning a foot at a time: where his feet were
	// as it changed (as pivot).
	feet?: [number, number, number, number];
};
type Under = {
	move: Move;
	phase: number;
	mirrored?: boolean;
	w: number;
	arms?: { move: Move; phase: number };
};

// In so many steps: a stride's frames, an act's, the ways he faces, and a
// blend from one move into the next - or, with time to spare on the device
// (see Field3D), more of each, so he moves the more smoothly.
// (An act's, no fewer than `act` over it, and more where he moves fast
// through it: no joint more than `actDeg` degrees from one to the next -
// see actFrames.)
type Steps = {
	cycle: number;
	act: number;
	actDeg: number;
	turns: number;
	blend: number;
};
const COARSE: Steps = { cycle: 12, act: 20, actDeg: 10, turns: 36, blend: 8 };
const FINE: Steps = { cycle: 24, act: 40, actDeg: 4, turns: 48, blend: 16 };

// (A stride slow to go round - a walk - in this many times more.)
const SLOW_STRIDES = new Map<Move, number>([["walk", 2]]);

// The frame of an act he is on, and where it starts (each the nearest to
// the phases about it).
const actFrameAt = (
	m: Move,
	phase: number,
	k: Steps,
): { at: number; from: number } => {
	const f = actFrames(m, k.act, k.actDeg);
	const u = Math.min(1, Math.max(0, phase));
	let lo = 0;
	let hi = f.length - 1;
	while (hi - lo > 1) {
		const mid = (lo + hi) >> 1;
		if (f[mid]! <= u) {
			lo = mid;
		} else {
			hi = mid;
		}
	}
	const mid = (f[lo]! + f[hi]!) / 2;
	return u < mid || lo === hi
		? { at: f[lo]!, from: lo > 0 ? (f[lo - 1]! + f[lo]!) / 2 : -Infinity }
		: { at: f[hi]!, from: mid };
};

const frameOf = (m: Move, phase: number, k: Steps): number => {
	const kind = moveKind(m);
	if (kind === "still") {
		return 0;
	}
	if (kind === "act") {
		return actFrameAt(m, phase, k).at;
	}
	const n = k.cycle * (SLOW_STRIDES.get(m) ?? 1);
	return Math.floor((((phase % 1) + 1) % 1) * n) / n;
};

// How long since he came to the frame he is on (ms), going through his move
// at this rate.
const sinceFrame = (m: Move, phase: number, rate: number, k: Steps) => {
	const kind = moveKind(m);
	if (kind === "still" || rate <= 0) {
		return Infinity;
	}
	const into =
		kind === "act"
			? phase - actFrameAt(m, phase, k).from
			: (((phase % 1) + 1) % 1) - frameOf(m, phase, k);
	return Math.max(0, into) / rate;
};

// How much of his pose is still what he was, in steps: as much as there was
// when his move (or the one he was) last came to a new frame - a sprite of
// its own then anyway, so the blend costs none of its own - or, if neither
// did, on the nearest of so many steps.
const WEIGHTS = 32;
const weightOf = (st: FieldPlayerState, f: Blend, k: Steps): number => {
	let w = Math.ceil(f.w * k.blend) / k.blend;
	const r = f.rate;
	if (r && r.w > 0) {
		const since = Math.min(
			sinceFrame(st.move, st.phase, r.move, k),
			sinceFrame(f.move, f.phase, r.from, k),
		);
		w = Math.min(w, f.w + since * r.w);
	}
	return Math.round(w * WEIGHTS) / WEIGHTS;
};

// His pose, stepped to the sprite's frames: what he is doing, blended with
// what he just was.
type Stepped = {
	phase: number;
	len: number | undefined;
	turn: number;
	yaw: number;
	from: Blend | undefined;
	arms: { move: Move; phase: number } | undefined;
	feet: Feet | undefined;
};
// Each foot turned under him (degrees, his right then his left) and how far
// up off the turf.
type Feet = [number, number, number, number];
const armsIn = (a: { move: Move; phase: number }, k: Steps) => ({
	move: a.move,
	phase: frameOf(a.move, a.phase, k),
});
const underIn = (u: Under, k: Steps): Under | undefined => {
	const w = Math.round(u.w * k.blend) / k.blend;
	return w > 0
		? {
				...u,
				phase: frameOf(u.move, u.phase, k),
				w,
				...(u.arms ? { arms: armsIn(u.arms, k) } : {}),
			}
		: undefined;
};
// (Short steps, to the nearest of so many.)
const LENS = 8;
const lenIn = (len: number | undefined): number | undefined => {
	if (len === undefined) {
		return undefined;
	}
	const q = Math.max(1, Math.round(len * LENS)) / LENS;
	return q < 1 ? q : undefined;
};
// A foot pointing `to` on the turf, turned under him from the way he is
// drawn facing (in his turns, so it stays put on the turf as he turns
// round over it): as far as hips turn - and further round than that, back
// with him by the time he is facing the other way. (His left is the
// turf's clockwise.)
const LEGS_TURN = 75;
const hipsTurn = (to: number, turn: number, k: Steps): number => {
	const n = k.turns;
	const d =
		((((Math.round((to * n) / (Math.PI * 2)) - turn) % n) + n) % n) * (360 / n);
	const w = d > 180 ? d - 360 : d;
	const a = Math.abs(w);
	return (
		-Math.sign(w) *
		(a <= LEGS_TURN ? a : (LEGS_TURN * (180 - a)) / (180 - LEGS_TURN))
	);
};
// (In steps of so many degrees, and so much of a step up.)
const FEET_DEG = 2;
const FEET_UP = 6;
const feetIn = (
	st: FieldPlayerState,
	turn: number,
	fw: number,
	k: Steps,
): Feet | undefined => {
	const p = st.pivot;
	const was = fw > 0 ? st.from?.feet : undefined;
	if (!p && !was) {
		return undefined;
	}
	const at = (i: 0 | 1) => {
		const to = p ? hipsTurn(p[i], turn, k) : 0;
		return was ? to + (hipsTurn(was[i], turn, k) - to) * fw : to;
	};
	const up = (i: 2 | 3) => {
		const to = p?.[i] ?? 0;
		return was ? to + (was[i] - to) * fw : to;
	};
	const feet: Feet = [
		Math.round(at(0) / FEET_DEG) * FEET_DEG,
		Math.round(at(1) / FEET_DEG) * FEET_DEG,
		Math.round(up(2) * FEET_UP) / FEET_UP,
		Math.round(up(3) * FEET_UP) / FEET_UP,
	];
	return feet.some((v) => v !== 0) ? feet : undefined;
};
const stepped = (st: FieldPlayerState, k: Steps): Stepped => {
	const turn = Math.round(st.yaw / ((Math.PI * 2) / k.turns));
	const f = st.from;
	const fw = f ? weightOf(st, f, k) : 0;
	return {
		feet: feetIn(st, turn, fw, k),
		phase: frameOf(st.move, st.phase, k),
		len: lenIn(st.len),
		turn,
		yaw: (turn * Math.PI * 2) / k.turns,
		from:
			f && fw > 0
				? {
						...f,
						phase: frameOf(f.move, f.phase, k),
						w: fw,
						...(f.arms ? { arms: armsIn(f.arms, k) } : {}),
						under: f.under && underIn(f.under, k),
					}
				: undefined,
		arms: st.arms ? armsIn(st.arms, k) : undefined,
	};
};

// A whole pose: what he is doing, his arms in what they are doing, and the
// ball the way he has it.
const wholePose = (
	move: Move,
	phase: number,
	mirrored: boolean,
	arms: { move: Move; phase: number } | undefined,
	ball: BallHold | undefined,
	len = 1,
): Pose => {
	let pose = poseOfMove(move, phase, mirrored, len);
	if (arms) {
		pose = withArms(pose, poseOfMove(arms.move, arms.phase, mirrored));
	}
	return holding(pose, ball);
};

// A step's lift: degrees at the hip and knee (see court3d's PIVOT_HIP).
const STEP_HIP = 16;
const STEP_KNEE = 34;
const poseFor = (st: FieldPlayerState, s: Stepped): Pose => {
	const p = blendedPose(st, s);
	const f = s.feet;
	return f
		? {
				...p,
				spinN: p.spinN + f[0],
				spinF: p.spinF + f[1],
				hipN: p.hipN + STEP_HIP * f[2],
				kneeN: p.kneeN + STEP_KNEE * f[2],
				hipF: p.hipF + STEP_HIP * f[3],
				kneeF: p.kneeF + STEP_KNEE * f[3],
			}
		: p;
};
const blendedPose = (st: FieldPlayerState, s: Stepped): Pose => {
	const { phase, from, arms, len } = s;
	const pose = wholePose(
		st.move,
		phase,
		st.mirrored === true,
		arms,
		st.ball,
		len,
	);
	if (!from || from.w <= 0) {
		return pose;
	}
	let was = wholePose(
		from.move,
		from.phase,
		from.mirrored === true,
		from.arms,
		from.ball,
		len,
	);
	const u = from.under;
	if (u) {
		was = lerpPose(
			was,
			wholePose(u.move, u.phase, u.mirrored === true, u.arms, from.ball),
			u.w,
		);
	}
	return lerpPose(pose, was, from.w);
};

// How he is drawn, the way the sprite has him: his pose and the way he
// faces (to the nearest of its turns) - in the finer steps, if he is drawn
// in them.
export const drawnPose = (
	st: FieldPlayerState,
	fine = false,
): { pose: Pose; yaw: number } => {
	const s = stepped(st, fine ? FINE : COARSE);
	return { pose: poseFor(st, s), yaw: s.yaw };
};

// How high he is drawn off the turf (feet): up as the play has him (in a
// leap, on a pile), and off it between strides as his frame has him - his
// lower foot on the turf, landing.
const liftFor = (st: FieldPlayerState, s: Stepped): number => {
	let up = strideLift(st.move, s.phase, s.len);
	const f = s.from;
	if (f) {
		up += (strideLift(f.move, f.phase, s.len) - up) * f.w;
	}
	return (Math.max(0, st.z) + up) * FIGURE_SCALE;
};
export const drawnLift = (st: FieldPlayerState, fine = false): number =>
	liftFor(st, stepped(st, fine ? FINE : COARSE));

// The ball with him, the way he has it - just after he changed how, still
// partway from the way he had it.
const ballFor = (
	st: FieldPlayerState,
	body: Body,
	pose: Pose,
	s: Stepped,
	hold: BallHold,
): HeldBall => {
	const now = heldBall(body, pose, hold);
	const was = s.from?.ball;
	const w = s.from?.w ?? 0;
	const ball =
		was && was !== hold && w > 0
			? mixBall(heldBall(body, pose, was), now, w)
			: now;
	return st.unseen ? { ...ball, unseen: true } : ball;
};
const mixV = (a: V3, b: V3, w: number): V3 => ({
	f: a.f * w + b.f * (1 - w),
	s: a.s * w + b.s * (1 - w),
	u: a.u * w + b.u * (1 - w),
});
const unit = (v: V3): V3 => {
	const l = Math.hypot(v.f, v.s, v.u) || 1;
	return { f: v.f / l, s: v.s / l, u: v.u / l };
};
const mixBall = (a: HeldBall, b: HeldBall, w: number): HeldBall => ({
	at: mixV(a.at, b.at, w),
	axis: unit(mixV(a.axis, b.axis, w)),
	laces: unit(mixV(a.laces, b.laces, w)),
});

// Where the ball he has is drawn on him (his frame), and the way he faces
// as drawn.
export const drawnBall = (
	st: FieldPlayerState,
	body: Body,
	fine = false,
): { ball: HeldBall; yaw: number } | undefined => {
	if (!st.ball) {
		return undefined;
	}
	const s = stepped(st, fine ? FINE : COARSE);
	return { ball: ballFor(st, body, poseFor(st, s), s, st.ball), yaw: s.yaw };
};

const figureOf = (st: FieldPlayerState, body: Body, s: Stepped): Figure => {
	const pose = poseFor(st, s);
	return {
		x: st.x,
		y: st.y,
		z: 0,
		yaw: s.yaw,
		pose,
		grips: gripsOf(st.move, st.ball),
		...(st.ball ? { ball: ballFor(st, body, pose, s, st.ball) } : {}),
	};
};

// How much of the light is left in the shade (a touch more of the blue).
const SHADED = 0.66;

export const drawFieldPlayer = (
	ctx: CanvasRenderingContext2D,
	scratch: Scratch,
	cam: Camera,
	st: FieldPlayerState,
	body: Body,
	look: FieldLook,
	px: number,
	cache?: SpriteCache,
	// Standing in the shadow of the stands (a day game's).
	shaded = false,
) => {
	const base = project(cam, { x: st.x, y: st.y, z: 0 });
	const k = base.k;
	const fine = cache?.fine === true;
	const steps = fine ? FINE : COARSE;
	const step = stepped(st, steps);
	const half = (body.H * 0.62 + 0.8) * k;
	const lift = liftFor(st, step) * k;
	const left = base.x - half;
	const right = base.x + half;
	const upper = base.y - (body.H * 1.4 + 0.5) * k - lift;
	const lower = base.y + 0.9 * k - lift;
	if (right < 0 || left > cam.viewW || lower < 0 || upper > cam.viewH) {
		return;
	}
	const { phase, turn, from, arms, len, feet } = step;
	const scale = Math.round(Math.log(k) / Math.log(1.04));
	let key: string | undefined;
	let id = 0;
	if (cache) {
		const known = cache.ids.get(look);
		id = known ?? cache.next++;
		if (known === undefined) {
			cache.ids.set(look, id);
		}
		key = `${fine ? "f" : ""}fb${id}|${st.move}${st.mirrored ? "m" : ""}|${phase}${len ? `s${len}` : ""}${feet ? `v${feet.join(",")}` : ""}|${((turn % steps.turns) + steps.turns) % steps.turns}|${scale}|${px}|${st.ball ?? ""}${st.unseen ? "~" : ""}${arms ? `|${arms.move}${arms.phase}` : ""}${
			from
				? `|${from.move}${from.mirrored ? "m" : ""}${from.phase}~${from.w}${from.arms ? `+${from.arms.move}${from.arms.phase}` : ""}${from.ball ? `@${from.ball}` : ""}${
						from.under
							? `^${from.under.move}${from.under.mirrored ? "m" : ""}${from.under.phase}~${from.under.w}${from.under.arms ? `+${from.under.arms.move}${from.under.arms.phase}` : ""}`
							: ""
					}`
				: ""
		}${shaded ? "|d" : ""}`;
		const kept = cache.kept.get(key);
		if (kept) {
			cache.kept.delete(key);
			cache.kept.set(key, kept);
			stamp(ctx, kept, base.x, base.y - lift, px);
			cache.last.set(id, { kept, frame: cache.frame, behind: 0, scale });
			return;
		}
		const was = cache.last.get(id);
		const over = cache.budget !== undefined && cache.spent >= cache.budget;
		if (
			over &&
			was &&
			was.behind < 3 &&
			was.frame === cache.frame - 1 &&
			Math.abs(was.scale - scale) <= 1
		) {
			stamp(ctx, was.kept, base.x, base.y - lift, px);
			was.frame = cache.frame;
			was.behind += 1;
			return;
		}
	}
	const began = cache ? performance.now() : 0;
	const fig = figureOf(st, body, step);
	const w = Math.max(4, Math.ceil((right - left) / px) + 2);
	const h = Math.max(4, Math.ceil((lower - upper) / px) + 2);
	const { canvas, ctx: s } = scratch;
	if (canvas.width < w || canvas.height < h) {
		canvas.width = Math.max(canvas.width, w);
		canvas.height = Math.max(canvas.height, h);
	}
	s.setTransform(1, 0, 0, 1, 0, 0);
	s.clearRect(0, 0, w, h);
	const ox = left - px;
	const oy = upper + lift - px;
	const made = sculptFigure(cam, fig, body, look, px, ox, oy, w, h);
	const box = made.box;
	if (shaded) {
		const d = made.img.data;
		for (let i = 0; i < d.length; i += 4) {
			d[i] = d[i]! * SHADED;
			d[i + 1] = d[i + 1]! * SHADED;
			d[i + 2] = d[i + 2]! * (SHADED + 0.06);
		}
	}
	rim(made.img.data, w, h, undefined, box);
	const rx0 = Math.max(0, box.x0 - 1);
	const ry0 = Math.max(0, box.y0 - 1);
	const rw = Math.min(w, box.x1 + 2) - rx0;
	const rh = Math.min(h, box.y1 + 2) - ry0;
	if (rw <= 0 || rh <= 0) {
		return;
	}
	s.putImageData(made.img, 0, 0, rx0, ry0, rw, rh);
	if (cache && key) {
		const kept = keep(
			cache,
			key,
			canvas,
			rx0,
			ry0,
			rw,
			rh,
			ox + rx0 * px - base.x,
			oy + ry0 * px - base.y,
		);
		cache.spent += performance.now() - began;
		cache.last.set(id, { kept, frame: cache.frame, behind: 0, scale });
		stamp(ctx, kept, base.x, base.y - lift, px);
		return;
	}
	const smoothing = ctx.imageSmoothingEnabled;
	ctx.imageSmoothingEnabled = false;
	ctx.drawImage(
		canvas,
		rx0,
		ry0,
		rw,
		rh,
		Math.round(ox + rx0 * px),
		Math.round(oy + ry0 * px - lift),
		rw * px,
		rh * px,
	);
	ctx.imageSmoothingEnabled = smoothing;
};
