import { depthOf, project, type Camera } from "../court3d/camera.ts";
import { skeleton, type Body, type V3 } from "../court3d/poses.ts";
import { makeCourtRng } from "../courtRng.ts";
import { FIGURE_SCALE } from "./body.ts";
import type { FieldMoment } from "./evaluate.ts";
import {
	FIELD_L,
	FIELD_W,
	GOAL_L,
	GOAL_R,
	LIMIT_LINE,
	MID_Y,
	TEAM_AREA,
	type Pt,
	type Pt3,
} from "./geometry.ts";
import { poseOfMove, withArms, type Move } from "./poses.ts";
import { bodyPoint } from "./sculpt.ts";
import type { FieldPlayerState } from "./sprite.ts";

// THE PHOTOGRAPHERS: along each sideline past the team areas, back behind
// the limit line, and in a row behind each end line - down on a knee or on
// their feet, each in his place all game, a long white lens up to the eye
// while the ball is live anywhere near him, lowered in his hand between
// plays.

export const PHOTOGRAPHERS = 64;
export const photographerId = (k: number) => -200 - k;

export type Photographer = {
	id: number;
	at: Pt;
	move: Move;
	// The way he faces, and how far round he turns after the ball.
	facing: number;
	turn: number;
	// How far off the ball he shoots it (feet), and how long after the snap
	// the camera comes up (ms).
	reach: number;
	delay: number;
};

// Along each sideline: this far past the team area at most, this far back
// off the field, this far apart at least. Behind each end line: this far
// back, off the goalposts by this much, this far apart.
const PAST_GOAL = 6;
const OFF_AREA = 8;
const SIDE_BACK: [number, number] = [LIMIT_LINE + 1, LIMIT_LINE + 6];
const END_BACK: [number, number] = [7, 13];
const POSTS_CLEAR = 9;
const APART = 4.5;
// How many to a stretch of sideline, and behind an end line.
const ON_SIDE: [number, number] = [3, 6];
const ON_END: [number, number] = [7, 11];
const KNEELING = 0.6;
// Up from just before the snap to this long after the whistle.
const UP_BEFORE = 400;
const UP_AFTER = 2200;

export const photographersFor = (gid: number): Photographer[] => {
	const r = makeCourtRng(`photographers|${gid}`);
	const out: Photographer[] = [];
	const count = ([a, b]: [number, number]) => a + Math.floor(r() * (b - a + 1));
	const near = (a: Pt, b: Pt) => Math.hypot(a.x - b.x, a.y - b.y) < APART;
	const add = (at: Pt, facing: number, turn: number) => {
		if (out.length >= PHOTOGRAPHERS || out.some((o) => near(o.at, at))) {
			return;
		}
		out.push({
			id: photographerId(out.length),
			at,
			move: r() < KNEELING ? "takeKnee" : "stand",
			facing,
			turn,
			reach: 120 + r() * 120,
			delay: r() * 700,
		});
	};
	const between = ([a, b]: [number, number]) => a + r() * (b - a);
	// (Down each sideline, from near the goal line to near the team area.)
	for (const line of [0, 1] as const) {
		const facing = line === 0 ? Math.PI / 2 : -Math.PI / 2;
		for (const span of [
			[GOAL_L - PAST_GOAL, TEAM_AREA[0] - OFF_AREA],
			[TEAM_AREA[1] + OFF_AREA, GOAL_R + PAST_GOAL],
		] as [number, number][]) {
			for (let n = count(ON_SIDE); n > 0; n--) {
				const back = between(SIDE_BACK);
				add(
					{ x: between(span), y: line === 0 ? -back : FIELD_W + back },
					facing,
					1.1,
				);
			}
		}
	}
	// (Behind each end line, clear of the posts.)
	for (const end of [0, 1] as const) {
		const facing = end === 0 ? 0 : Math.PI;
		for (let n = count(ON_END); n > 0; n--) {
			const back = between(END_BACK);
			let y = between([6, FIELD_W - 6]);
			if (Math.abs(y - MID_Y) < POSTS_CLEAR) {
				y = MID_Y + Math.sign(y - MID_Y || 1) * POSTS_CLEAR;
			}
			add({ x: end === 0 ? -back : FIELD_L + back, y }, facing, 0.9);
		}
	}
	return out;
};

// Whether he has the camera up: the ball live, or just dead, and not too
// far off.
const shooting = (s: Photographer, m: FieldMoment) => {
	const play = m.play;
	const ball = m.focus;
	if (!play || !ball) {
		return false;
	}
	return (
		m.t >= play.snap - UP_BEFORE + s.delay &&
		m.t <= play.whistle + UP_AFTER + s.delay &&
		Math.hypot(ball.x - s.at.x, ball.y - s.at.y) < s.reach
	);
};

export const photographersAt = (
	men: Photographer[],
	m: FieldMoment,
): FieldPlayerState[] => {
	const ball = m.focus;
	return men.map((s) => {
		let yaw = s.facing;
		if (ball) {
			let d = Math.atan2(ball.y - s.at.y, ball.x - s.at.x) - s.facing;
			d = Math.atan2(Math.sin(d), Math.cos(d));
			yaw = s.facing + Math.max(-s.turn, Math.min(s.turn, d));
		}
		return {
			id: s.id,
			team: 0,
			x: s.at.x,
			y: s.at.y,
			z: 0,
			yaw,
			move: s.move,
			phase: 0,
			...(shooting(s, m) ? { arms: { move: "shoot" as Move, phase: 0 } } : {}),
		};
	});
};

// ---- the cameras, drawn ------------------------------------------------------

const BODY = "#1c1c20";
const LENS = "#e8e6df";
const LENS_EDGE = "#9a9890";
const RING = "#2a2a2e";
// The camera body (feet across), the lens (long, and thick) - to the size
// of the men (see body.ts).
const BODY_FT = 0.42 * FIGURE_SCALE;
const LENS_FT = 1.35 * FIGURE_SCALE;
const LENS_D = 0.42 * FIGURE_SCALE;
const MIN_PX = 1;

type Item = { depth: number; draw: () => void };

// Where the camera is in his frame, up or down: in front of his eyes, or
// in his right hand.
const holds = new WeakMap<Body, Map<string, { at: V3; up: boolean }>>();
const holdOf = (b: Body, move: Move, up: boolean) => {
	let byMove = holds.get(b);
	if (!byMove) {
		byMove = new Map();
		holds.set(b, byMove);
	}
	const key = `${move}|${up ? 1 : 0}`;
	let h = byMove.get(key);
	if (!h) {
		const q = poseOfMove(move, 0);
		const sk = skeleton(b, up ? withArms(q, poseOfMove("shoot", 0)) : q);
		h = up
			? {
					at: { f: sk.head.f + 0.42, s: sk.head.s - 0.08, u: sk.head.u - 0.1 },
					up,
				}
			: { at: sk.armR.end, up };
		byMove.set(key, h);
	}
	return h;
};

export const cameraGear = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	states: FieldPlayerState[],
	bodyOf: (pid: number) => Body,
): Item[] =>
	states.map((st) => {
		const up = st.arms?.move === "shoot";
		const h = holdOf(bodyOf(st.id), st.move, up);
		const placed = { x: st.x, y: st.y, z: st.z, yaw: st.yaw };
		const at = bodyPoint(placed, h.at);
		// (Out ahead of him, a touch down; or hung straight down.)
		const end = bodyPoint(
			placed,
			up
				? { f: h.at.f + LENS_FT, s: h.at.s, u: h.at.u - 0.12 }
				: { f: h.at.f + 0.15, s: h.at.s, u: h.at.u - LENS_FT * 0.8 },
		);
		return {
			depth: Math.min(depthOf(cam, at), depthOf(cam, end)),
			draw: () => {
				drawCamera(ctx, cam, at, end);
			},
		};
	});

const drawCamera = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	at: Pt3,
	end: Pt3,
) => {
	const a = project(cam, at);
	const b = project(cam, end);
	ctx.lineCap = "butt";
	ctx.strokeStyle = LENS_EDGE;
	ctx.lineWidth = Math.max(MIN_PX + 1, (LENS_D + 0.08) * a.k);
	ctx.beginPath();
	ctx.moveTo(a.x, a.y);
	ctx.lineTo(b.x, b.y);
	ctx.stroke();
	ctx.strokeStyle = LENS;
	ctx.lineWidth = Math.max(MIN_PX, LENS_D * a.k);
	ctx.stroke();
	// (The hood on the end of it, dark.)
	const u = 0.85;
	ctx.strokeStyle = RING;
	ctx.beginPath();
	ctx.moveTo(a.x + (b.x - a.x) * u, a.y + (b.y - a.y) * u);
	ctx.lineTo(b.x, b.y);
	ctx.stroke();
	const w = Math.max(MIN_PX + 1, BODY_FT * a.k);
	ctx.fillStyle = BODY;
	ctx.fillRect(a.x - w / 2, a.y - w / 2, w, w);
};

// ---- the TV cameras, drawn --------------------------------------------------

const TV_BODY = "#686b72";
const TV_TOP = "#8d9199";
const TV_LENS = "#111114";
// How long the camera is, how far back of his shoulder it sits, how big
// round, and the lens on the front of it.
const TV_FT = 1.9;
const TV_BACK = 0.8;
const TV_D = 0.55;
const TV_LENS_FT = 0.45;

const shoulders = new WeakMap<Body, V3>();
const shoulderOf = (b: Body) => {
	let at = shoulders.get(b);
	if (!at) {
		const sk = skeleton(b, poseOfMove("stand", 0));
		// (Sat on top of his right shoulder.)
		at = { f: sk.armR.root.f, s: sk.armR.root.s, u: sk.armR.root.u + 0.45 };
		shoulders.set(b, at);
	}
	return at;
};

export const tvCameraGear = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	states: FieldPlayerState[],
	bodyOf: (pid: number) => Body,
): Item[] => {
	const items: Item[] = [];
	for (const st of states) {
		if (st.arms?.move !== "shoulderCam") {
			continue;
		}
		const s = shoulderOf(bodyOf(st.id));
		const placed = { x: st.x, y: st.y, z: st.z, yaw: st.yaw };
		const back = bodyPoint(placed, { ...s, f: s.f - TV_BACK });
		const front = bodyPoint(placed, { ...s, f: s.f - TV_BACK + TV_FT });
		const lens = bodyPoint(placed, {
			...s,
			f: s.f - TV_BACK + TV_FT + TV_LENS_FT,
		});
		items.push({
			depth: Math.min(depthOf(cam, back), depthOf(cam, lens)),
			draw: () => {
				const a = project(cam, back);
				const b = project(cam, front);
				const c = project(cam, lens);
				ctx.lineCap = "butt";
				ctx.strokeStyle = TV_BODY;
				ctx.lineWidth = Math.max(MIN_PX + 1, TV_D * a.k);
				ctx.beginPath();
				ctx.moveTo(a.x, a.y);
				ctx.lineTo(b.x, b.y);
				ctx.stroke();
				// (Its top, catching the light.)
				ctx.strokeStyle = TV_TOP;
				ctx.lineWidth = Math.max(MIN_PX, TV_D * 0.3 * a.k);
				ctx.beginPath();
				ctx.moveTo(a.x, a.y - TV_D * 0.3 * a.k);
				ctx.lineTo(b.x, b.y - TV_D * 0.3 * a.k);
				ctx.stroke();
				ctx.strokeStyle = TV_LENS;
				ctx.lineWidth = Math.max(MIN_PX, TV_D * 0.8 * a.k);
				ctx.beginPath();
				ctx.moveTo(b.x, b.y);
				ctx.lineTo(c.x, c.y);
				ctx.stroke();
			},
		});
	}
	return items;
};
