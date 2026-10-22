import { project, type Camera } from "../court3d/camera.ts";
import { lerpPose, type Body } from "../court3d/poses.ts";
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
import { sculptFigure, type Figure } from "./sculpt.ts";

// THE PLAYERS ON THE FIELD, DRAWN - the way the court draws its own (see
// court3d/sprite.ts, whose cache this shares): each sculpted on a sheet of
// his own at the picture's resolution, a soft dark rim round him, stamped
// into the picture and kept for the next time the same pose comes round.
// His animation runs in frames and he turns in 24 directions, so the same
// few pictures come round again and again, and a whole formation set in its
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
	// Just after a change of move: the last one as it was when it changed,
	// and how much of his pose is still that (1 all, 0 none).
	from?: { move: Move; phase: number; mirrored?: boolean; w: number };
	// The ball with him, and how he has it.
	ball?: BallHold;
	// His arms in an act of their own over the rest of him, and how far
	// through it.
	arms?: { move: Move; phase: number };
};

const CYCLE_FRAMES = 12;
const ACT_FRAMES = 20;
const TURNS = 24;
const BLEND_STEPS = 4;

const frameOf = (m: Move, phase: number): number => {
	const kind = moveKind(m);
	if (kind === "still") {
		return 0;
	}
	if (kind === "act") {
		return (
			Math.round(Math.min(1, Math.max(0, phase)) * (ACT_FRAMES - 1)) /
			(ACT_FRAMES - 1)
		);
	}
	return Math.floor((((phase % 1) + 1) % 1) * CYCLE_FRAMES) / CYCLE_FRAMES;
};

// His pose, stepped to the sprite's frames: what he is doing, blended with
// what he just was.
const figureOf = (
	st: FieldPlayerState,
	body: Body,
	phase: number,
	yaw: number,
	from:
		| { move: Move; phase: number; mirrored?: boolean; w: number }
		| undefined,
	arms: { move: Move; phase: number } | undefined,
): Figure => {
	let pose = poseOfMove(st.move, phase, st.mirrored === true);
	if (from && from.w > 0) {
		pose = lerpPose(
			pose,
			poseOfMove(from.move, from.phase, from.mirrored === true),
			from.w,
		);
	}
	if (arms) {
		pose = withArms(
			pose,
			poseOfMove(arms.move, arms.phase, st.mirrored === true),
		);
	}
	pose = holding(pose, st.ball);
	return {
		x: st.x,
		y: st.y,
		z: 0,
		yaw,
		pose,
		grips: gripsOf(st.move, st.ball),
		...(st.ball ? { ball: heldBall(body, pose, st.ball) } : {}),
	};
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
	const phase = frameOf(st.move, st.phase);
	const turn = Math.round(st.yaw / ((Math.PI * 2) / TURNS));
	const yaw = (turn * Math.PI * 2) / TURNS;
	const f = st.from;
	const fw =
		f && f.w > 0.12
			? Math.min(BLEND_STEPS - 1, Math.max(1, Math.round(f.w * BLEND_STEPS))) /
				BLEND_STEPS
			: 0;
	const from =
		f && fw > 0 ? { ...f, phase: frameOf(f.move, f.phase), w: fw } : undefined;
	const arms = st.arms
		? { move: st.arms.move, phase: frameOf(st.arms.move, st.arms.phase) }
		: undefined;
	const scale = Math.round(Math.log(k) / Math.log(1.04));
	let key: string | undefined;
	let id = 0;
	if (cache) {
		const known = cache.ids.get(look);
		id = known ?? cache.next++;
		if (known === undefined) {
			cache.ids.set(look, id);
		}
		key = `fb${id}|${st.move}${st.mirrored ? "m" : ""}|${phase}|${((turn % TURNS) + TURNS) % TURNS}|${scale}|${px}|${st.ball ?? ""}${arms ? `|${arms.move}${arms.phase}` : ""}${
			from
				? `|${from.move}${from.mirrored ? "m" : ""}${from.phase}~${from.w}`
				: ""
		}`;
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
	const fig = figureOf(st, body, phase, yaw, from, arms);
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
