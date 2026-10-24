import { project, type Camera } from "../court3d/camera.ts";
import { lerpPose, type Body, type Pose, type V3 } from "../court3d/poses.ts";
import {
	keepPixels,
	rim,
	stamp,
	type Kept,
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
	settleStep,
	strideLift,
	withArms,
	type Move,
} from "./poses.ts";
import type { FieldFarm } from "./farm.ts";
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
	// From one way of standing into another, not turning: stepped over a
	// foot at a time (see poses.ts settleStep).
	steps?: true;
};
type Under = {
	move: Move;
	phase: number;
	mirrored?: boolean;
	w: number;
	arms?: { move: Move; phase: number };
	// (And that one too, still partly what he was before it.)
	under?: Under;
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
// did, on the nearest of so many steps. (Down into a stance or up out of
// it, twice as many: he goes a long way down, or up, in a moment.)
const WEIGHTS = 32;
const STANCES = new Set<Move>([
	"threePoint",
	"dlThree",
	"center",
	"backerStance",
	"backStance",
	"receiverStance",
	"cornerStance",
]);
const weightOf = (st: FieldPlayerState, f: Blend, k: Steps): number => {
	const n = STANCES.has(f.move) || STANCES.has(st.move) ? k.blend * 2 : k.blend;
	let w = Math.ceil(f.w * n) / n;
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
				under: u.under && underIn(u.under, k),
			}
		: undefined;
};
const underKey = (u: Under | undefined): string =>
	u
		? `^${u.move}${u.mirrored ? "m" : ""}${u.phase}~${u.w}${u.arms ? `+${u.arms.move}${u.arms.phase}` : ""}${underKey(u.under)}`
		: "";
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
const underPose = (u: Under, ball: BallHold | undefined): Pose => {
	const pose = wholePose(u.move, u.phase, u.mirrored === true, u.arms, ball);
	return u.under ? lerpPose(pose, underPose(u.under, ball), u.under.w) : pose;
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
		was = lerpPose(was, underPose(u, from.ball), u.w);
	}
	const eased = lerpPose(pose, was, from.w);
	return from.steps && !u ? settleStep(eased, pose, was, 1 - from.w) : eased;
};

// A man facing all but midway between two of the sprite's turns, and
// hardly turning: held to the turn he was drawn in last frame till he is
// this much of a turn past halfway round to the next - not flicking
// between the two frame to frame. (Turning any quicker than this much of a
// turn a frame, to the nearest as he goes: held, he would only be the
// further round all at once after.) (`held`: the turn each man was last
// drawn in, of how many, and the way he faced then - kept by whoever draws
// him.)
const STEADY_PAST = 0.2;
const STEADY_RATE = 0.25;
export const steadied = (
	st: FieldPlayerState,
	held: Map<number, { n: number; turn: number; at: number }>,
	fine = false,
): FieldPlayerState => {
	const n = (fine ? FINE : COARSE).turns;
	const step = (Math.PI * 2) / n;
	const t = st.yaw / step;
	const near = (x: number) => x + n * Math.round((t - x) / n);
	const was = held.get(st.id);
	if (!was || was.n !== n) {
		held.set(st.id, { n, turn: Math.round(t), at: t });
		return st;
	}
	const turn = near(was.turn);
	const slow = Math.abs(t - near(was.at)) < STEADY_RATE;
	was.at = t;
	if (slow && Math.abs(t - turn) < 0.5 + STEADY_PAST) {
		was.turn = turn;
		return { ...st, yaw: turn * step };
	}
	was.turn = Math.round(t);
	return st;
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
// (What he was is still partly what he was before that, as in his pose.)
const wasLift = (u: Under, len: number | undefined): number => {
	const up = strideLift(u.move, u.phase, len);
	return u.under ? up + (wasLift(u.under, len) - up) * u.under.w : up;
};
const liftFor = (st: FieldPlayerState, s: Stepped): number => {
	let up = strideLift(st.move, s.phase, s.len);
	const f = s.from;
	if (f) {
		up += (wasLift(f, s.len) - up) * f.w;
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

// What drawing him takes this frame: where he stands on the picture and
// the box round him there, his pose in the sprite's steps, his size in
// steps of 4%, whether the ball is drawn with him, and his picture's key in
// the cache (with one).
type Plan = {
	base: ReturnType<typeof project>;
	lift: number;
	left: number;
	right: number;
	upper: number;
	lower: number;
	step: Stepped;
	scale: number;
	held: boolean;
	shaded: boolean;
	key?: string;
	// His new picture being made off the page's thread (see farm.ts): drawn
	// as he was meanwhile.
	farmed?: boolean;
	// Off the field - on the sideline, at the back of the picture.
	off?: boolean;
};
const planOf = (
	cam: Camera,
	st: FieldPlayerState,
	body: Body,
	look: FieldLook,
	px: number,
	cache: SpriteCache | undefined,
	shaded: boolean,
): Plan | undefined => {
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
		return undefined;
	}
	const scale = Math.round(Math.log(k) / Math.log(1.04));
	const plan: Plan = {
		base,
		lift,
		left,
		right,
		upper,
		lower,
		step,
		scale,
		held: st.ball !== undefined && !st.unseen,
		shaded,
	};
	if (cache) {
		const { phase, turn, from, arms, len, feet } = step;
		const known = cache.ids.get(look);
		const id = known ?? cache.next++;
		if (known === undefined) {
			cache.ids.set(look, id);
		}
		plan.key = `${fine ? "f" : ""}fb${id}|${st.move}${st.mirrored ? "m" : ""}|${phase}${len ? `s${len}` : ""}${feet ? `v${feet.join(",")}` : ""}|${((turn % steps.turns) + steps.turns) % steps.turns}|${scale}|${px}|${st.ball ?? ""}${st.unseen ? "~" : ""}${arms ? `|${arms.move}${arms.phase}` : ""}${
			from
				? `|${from.move}${from.mirrored ? "m" : ""}${from.steps ? "!" : ""}${from.phase}~${from.w}${from.arms ? `+${from.arms.move}${from.arms.phase}` : ""}${from.ball ? `@${from.ball}` : ""}${underKey(from.under)}`
				: ""
		}${shaded ? "|d" : ""}`;
	}
	return plan;
};

// This frame's plan for each man made ready (see prepareFieldSprites).
const planned = new WeakMap<
	SpriteCache,
	{ frame: number; plans: Map<FieldPlayerState, Plan> }
>();
const plansOf = (cache: SpriteCache) => {
	let p = planned.get(cache);
	if (!p || p.frame !== cache.frame) {
		p = { frame: cache.frame, plans: new Map() };
		planned.set(cache, p);
	}
	return p.plans;
};

// Whether he may be drawn this frame in the picture he had last frame (out
// of time for a new one): if he had one a frame ago, not for too many frames
// already, not much bigger or smaller since, and never with the ball in it
// when he has let it go, or without it once he has it (see court3d's
// asWas). Who he is, by his id: two men may look alike. (Off the field, a
// few frames more: small, at the back, and seldom on the move - his new
// picture made in his turn after the men in the play, not on the page's
// thread ahead of them.)
const MAX_BEHIND = 3;
const MAX_BEHIND_OFF = 6;
const asWas = (cache: SpriteCache, plan: Plan, who: number) => {
	const was = cache.last.get(who);
	return was &&
		was.behind < (plan.off ? MAX_BEHIND_OFF : MAX_BEHIND) &&
		was.frame === cache.frame - 1 &&
		Math.abs(was.scale - plan.scale) <= 1 &&
		(was.held ?? false) === plan.held
		? was
		: undefined;
};
const overBudget = (cache: SpriteCache) =>
	cache.budget !== undefined && cache.spent >= cache.budget;

// HOW HE IS MADE, in plain numbers - the camera, him and his pose in the
// sprite's steps, his build, the box on the picture he is drawn in and where
// he stands in it - so a worker can sculpt him as readily as the page can
// (see farm.ts).
export type FieldJob = {
	cam: Camera;
	st: FieldPlayerState;
	step: Stepped;
	body: Body;
	px: number;
	left: number;
	right: number;
	upper: number;
	lower: number;
	lift: number;
	baseX: number;
	baseY: number;
	shaded: boolean;
};
const jobOf = (
	plan: Plan,
	cam: Camera,
	st: FieldPlayerState,
	body: Body,
	px: number,
): FieldJob => ({
	cam,
	st,
	step: plan.step,
	body,
	px,
	left: plan.left,
	right: plan.right,
	upper: plan.upper,
	lower: plan.lower,
	lift: plan.lift,
	baseX: plan.base.x,
	baseY: plan.base.y,
	shaded: plan.shaded,
});

// HIM, SCULPTED (see sculpt.ts), shaded and rimmed: nothing but arithmetic,
// and done as readily in a worker as on the page. Just what is drawn of him
// and the rim round it, and where that sits from where he stands - nothing,
// if nothing of him shows.
export type FieldShaped = { pixels: ImageData; dx: number; dy: number };
export const shapeField = (
	job: FieldJob,
	look: FieldLook,
): FieldShaped | undefined => {
	const { cam, st, step, body, px, left, right, upper, lower, lift } = job;
	const fig = figureOf(st, body, step);
	const w = Math.max(4, Math.ceil((right - left) / px) + 2);
	const h = Math.max(4, Math.ceil((lower - upper) / px) + 2);
	const ox = left - px;
	const oy = upper + lift - px;
	const made = sculptFigure(cam, fig, body, look, px, ox, oy, w, h);
	const box = made.box;
	const d = made.img.data;
	if (job.shaded) {
		for (let i = 0; i < d.length; i += 4) {
			d[i] = d[i]! * SHADED;
			d[i + 1] = d[i + 1]! * SHADED;
			d[i + 2] = d[i + 2]! * (SHADED + 0.06);
		}
	}
	rim(d, w, h, undefined, box);
	const rx0 = Math.max(0, box.x0 - 1);
	const ry0 = Math.max(0, box.y0 - 1);
	const rw = Math.min(w, box.x1 + 2) - rx0;
	const rh = Math.min(h, box.y1 + 2) - ry0;
	if (rw <= 0 || rh <= 0) {
		return undefined;
	}
	const pixels = new ImageData(rw, rh);
	for (let y = 0; y < rh; y++) {
		const from = ((ry0 + y) * w + rx0) * 4;
		pixels.data.set(d.subarray(from, from + rw * 4), y * rw * 4);
	}
	return {
		pixels,
		dx: ox + rx0 * px - job.baseX,
		dy: oy + ry0 * px - job.baseY,
	};
};

// His picture for the plan, sculpted: kept, with a cache - or, without,
// left on the scratch sheet to be drawn from (its size there, and where
// that goes on the picture). Nothing, if nothing of him shows.
const make = (
	plan: Plan,
	scratch: Scratch,
	cam: Camera,
	st: FieldPlayerState,
	body: Body,
	look: FieldLook,
	px: number,
	cache: SpriteCache | undefined,
): Kept | { w: number; h: number; x: number; y: number } | undefined => {
	const began = cache ? performance.now() : 0;
	const job = jobOf(plan, cam, st, body, px);
	const sh = shapeField(job, look);
	if (!sh) {
		return undefined;
	}
	if (cache && plan.key) {
		const kept = keepPixels(cache, plan.key, sh.pixels, sh.dx, sh.dy);
		cache.spent += performance.now() - began;
		return kept;
	}
	const { canvas, ctx: s } = scratch;
	const { width: w, height: h } = sh.pixels;
	if (canvas.width < w || canvas.height < h) {
		canvas.width = Math.max(canvas.width, w);
		canvas.height = Math.max(canvas.height, h);
	}
	s.setTransform(1, 0, 0, 1, 0, 0);
	s.putImageData(sh.pixels, 0, 0);
	return { w, h, x: plan.base.x + sh.dx, y: plan.base.y + sh.dy - plan.lift };
};

// Which picture he is drawn in this frame: his own for this pose, kept;
// the one he had, if he may be drawn as he was, out of time for a new one;
// or none yet, to be made now - and the books kept on it.
const pick = (cache: SpriteCache, plan: Plan, who: number): Kept | "make" => {
	const kept = plan.key === undefined ? undefined : cache.kept.get(plan.key);
	if (kept && plan.key !== undefined) {
		// Kept longest are the ones still in use: to the back of the line.
		cache.kept.delete(plan.key);
		cache.kept.set(plan.key, kept);
		cache.last.set(who, {
			kept,
			frame: cache.frame,
			behind: 0,
			scale: plan.scale,
			held: plan.held,
		});
		return kept;
	}
	if (plan.farmed || overBudget(cache)) {
		const was = asWas(cache, plan, who);
		if (was) {
			was.frame = cache.frame;
			was.behind += 1;
			return was.kept;
		}
	}
	return "make";
};

// The men sculpted off the page's thread since the last frame, kept - and
// each the picture a man is drawn in meanwhile, if it is of a later pose of
// his than the one he has: he is then that much less behind.
const took = (cache: SpriteCache, farm: FieldFarm) => {
	const began = performance.now();
	for (const m of farm.take(cache)) {
		if (cache.kept.has(m.key)) {
			continue;
		}
		const { pixels, dx, dy } = m.shaped;
		const kept = keepPixels(cache, m.key, pixels, dx, dy);
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
				held: m.held,
			});
		}
	}
	cache.spent += performance.now() - began;
};

// THE NEW PICTURES A FRAME NEEDS, MADE FIRST - the men on the field before
// anyone off it, and of them the nearest the camera first: drawn back to
// front, a frame out of time ran out on the men in the play and nearest,
// the biggest on the picture, and drew them behind their pose. Made here
// first instead, while there is time (and whoever has no picture to be
// drawn in meanwhile, regardless), the men left a frame behind are the
// furthest and smallest, and those on the sidelines (see court3d's
// prepareSprites). Whoever may be drawn as he was is made off the page's
// thread, where there are hands for it (see farm.ts).
export const prepareFieldSprites = (
	scratch: Scratch,
	cam: Camera,
	men: {
		st: FieldPlayerState;
		body: Body;
		look: FieldLook;
		shaded: boolean;
		off?: boolean;
	}[],
	px: number,
	cache: SpriteCache,
	farm?: FieldFarm,
) => {
	if (farm) {
		took(cache, farm);
	}
	const plans = plansOf(cache);
	const todo: { man: (typeof men)[number]; plan: Plan; wait: boolean }[] = [];
	for (const man of men) {
		const plan = planOf(cam, man.st, man.body, man.look, px, cache, man.shaded);
		if (plan?.key === undefined) {
			continue;
		}
		plan.off = man.off;
		plans.set(man.st, plan);
		if (!cache.kept.has(plan.key)) {
			todo.push({
				man,
				plan,
				wait: asWas(cache, plan, man.st.id) !== undefined,
			});
		}
	}
	todo.sort(
		(a, b) =>
			(a.man.off ? 1 : 0) - (b.man.off ? 1 : 0) ||
			b.plan.base.k - a.plan.base.k,
	);
	for (const { man, plan, wait } of todo) {
		const key = plan.key!;
		if (
			wait &&
			farm &&
			(farm.busy(cache, key) ||
				farm.ask(cache, key, jobOf(plan, cam, man.st, man.body, px), man.look, {
					id: man.st.id,
					frame: cache.frame,
					scale: plan.scale,
					held: plan.held,
				}))
		) {
			plan.farmed = true;
			continue;
		}
		if (wait && overBudget(cache)) {
			continue;
		}
		make(plan, scratch, cam, man.st, man.body, man.look, px, cache);
	}
};

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
	const plan =
		(cache && plansOf(cache).get(st)) ??
		planOf(cam, st, body, look, px, cache, shaded);
	if (!plan) {
		return;
	}
	const { base, lift } = plan;
	if (cache && plan.key !== undefined) {
		const was = pick(cache, plan, st.id);
		if (was !== "make") {
			stamp(ctx, was, base.x, base.y - lift, px);
			return;
		}
	}
	const made = make(plan, scratch, cam, st, body, look, px, cache);
	if (!made) {
		return;
	}
	if ("canvas" in made) {
		cache?.last.set(st.id, {
			kept: made,
			frame: cache.frame,
			behind: 0,
			scale: plan.scale,
			held: plan.held,
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
		Math.round(made.x),
		Math.round(made.y),
		made.w * px,
		made.h * px,
	);
	ctx.imageSmoothingEnabled = smoothing;
};
