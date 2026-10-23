import { project, type Camera } from "../court3d/camera.ts";
import { lerpPose, type Body, type Pose, type V3 } from "../court3d/poses.ts";
import {
	keep,
	rim,
	stamp,
	type Scratch,
	type SpriteCache,
} from "../court3d/sprite.ts";
import type { Side } from "./geometry.ts";
import type { FieldLook } from "./kit.ts";
import type { BallHold } from "./director.ts";
import {
	gripsOf,
	heldBall,
	holding,
	moveKind,
	poseOfMove,
	withArms,
	type Move,
} from "./poses.ts";
import { sculptFigure, type Figure, type HeldBall } from "./sculpt.ts";

// THE PLAYERS ON THE FIELD, DRAWN - the way the court draws its own (see
// court3d/sprite.ts, whose cache this shares): each sculpted on a sheet of
// his own at the picture's resolution, a soft dark rim round him, stamped
// into the picture and kept for the next time the same pose comes round.
// His animation runs in frames and he turns in 24 directions (twice as many
// of each with time to spare, the way the court does it), so the same few
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
};

// In so many steps: a stride's frames, an act's, the ways he faces, and a
// blend from one move into the next - or, with time to spare on the device
// (see Field3D), twice as many of each, so he moves the more smoothly.
type Steps = { cycle: number; act: number; turns: number; blend: number };
const COARSE: Steps = { cycle: 12, act: 20, turns: 24, blend: 8 };
const FINE: Steps = { cycle: 24, act: 40, turns: 48, blend: 16 };

const frameOf = (m: Move, phase: number, k: Steps): number => {
	const kind = moveKind(m);
	if (kind === "still") {
		return 0;
	}
	if (kind === "act") {
		return (
			Math.round(Math.min(1, Math.max(0, phase)) * (k.act - 1)) / (k.act - 1)
		);
	}
	return Math.floor((((phase % 1) + 1) % 1) * k.cycle) / k.cycle;
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
			? phase - frameOf(m, phase, k) + 0.5 / (k.act - 1)
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
	turn: number;
	yaw: number;
	from: Blend | undefined;
	arms: { move: Move; phase: number } | undefined;
};
const stepped = (st: FieldPlayerState, k: Steps): Stepped => {
	const turn = Math.round(st.yaw / ((Math.PI * 2) / k.turns));
	const f = st.from;
	const fw = f ? weightOf(st, f, k) : 0;
	return {
		phase: frameOf(st.move, st.phase, k),
		turn,
		yaw: (turn * Math.PI * 2) / k.turns,
		from:
			f && fw > 0
				? {
						...f,
						phase: frameOf(f.move, f.phase, k),
						w: fw,
						...(f.arms
							? {
									arms: {
										move: f.arms.move,
										phase: frameOf(f.arms.move, f.arms.phase, k),
									},
								}
							: {}),
					}
				: undefined,
		arms: st.arms
			? { move: st.arms.move, phase: frameOf(st.arms.move, st.arms.phase, k) }
			: undefined,
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
): Pose => {
	let pose = poseOfMove(move, phase, mirrored);
	if (arms) {
		pose = withArms(pose, poseOfMove(arms.move, arms.phase, mirrored));
	}
	return holding(pose, ball);
};

const poseFor = (st: FieldPlayerState, s: Stepped): Pose => {
	const { phase, from, arms } = s;
	const pose = wholePose(st.move, phase, st.mirrored === true, arms, st.ball);
	return from && from.w > 0
		? lerpPose(
				pose,
				wholePose(
					from.move,
					from.phase,
					from.mirrored === true,
					from.arms,
					from.ball,
				),
				from.w,
			)
		: pose;
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
	const half = (body.H * 0.62 + 0.8) * k;
	const lift = Math.max(0, st.z) * k;
	const left = base.x - half;
	const right = base.x + half;
	const upper = base.y - (body.H * 1.4 + 0.5) * k - lift;
	const lower = base.y + 0.9 * k - lift;
	if (right < 0 || left > cam.viewW || lower < 0 || upper > cam.viewH) {
		return;
	}
	const fine = cache?.fine === true;
	const steps = fine ? FINE : COARSE;
	const step = stepped(st, steps);
	const { phase, turn, from, arms } = step;
	const scale = Math.round(Math.log(k) / Math.log(1.04));
	let key: string | undefined;
	let id = 0;
	if (cache) {
		const known = cache.ids.get(look);
		id = known ?? cache.next++;
		if (known === undefined) {
			cache.ids.set(look, id);
		}
		key = `${fine ? "f" : ""}fb${id}|${st.move}${st.mirrored ? "m" : ""}|${phase}|${((turn % steps.turns) + steps.turns) % steps.turns}|${scale}|${px}|${st.ball ?? ""}${st.unseen ? "~" : ""}${arms ? `|${arms.move}${arms.phase}` : ""}${
			from
				? `|${from.move}${from.mirrored ? "m" : ""}${from.phase}~${from.w}${from.arms ? `+${from.arms.move}${from.arms.phase}` : ""}${from.ball ? `@${from.ball}` : ""}`
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
