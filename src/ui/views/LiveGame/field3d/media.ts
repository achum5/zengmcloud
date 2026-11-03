import { depthOf, project, type Camera } from "../court3d/camera.ts";
import { skeleton, type Body, type V3 } from "../court3d/poses.ts";
import { makeCourtRng } from "../courtRng.ts";
import { FIGURE_SCALE } from "./body.ts";
import { ROWS_X } from "./cheerleaders.ts";
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
import {
	drawnLift,
	drawnPose,
	drawnShift,
	type FieldPlayerState,
} from "./sprite.ts";

// THE PHOTOGRAPHERS: along each sideline past the team areas, back behind
// the limit line (none right in front of the cheerleaders' rows - see
// cheerleaders.ts), and in a row behind each end line - down on a knee or on
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
const ROWS_CLEAR = 13;
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
		if (
			out.length >= PHOTOGRAPHERS ||
			out.some((o) => near(o.at, at)) ||
			(at.y < 0 && Math.abs(at.x - ROWS_X) < ROWS_CLEAR)
		) {
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

// Which way he faces: his own way, turned after the ball as far as he
// turns.
const yawOf = (s: Photographer, ball: Pt | undefined): number => {
	if (!ball) {
		return s.facing;
	}
	let d = Math.atan2(ball.y - s.at.y, ball.x - s.at.x) - s.facing;
	d = Math.atan2(Math.sin(d), Math.cos(d));
	return s.facing + Math.max(-s.turn, Math.min(s.turn, d));
};

export const photographersAt = (
	men: Photographer[],
	m: FieldMoment,
): FieldPlayerState[] => {
	const ball = m.focus;
	return men.map((s) => {
		const yaw = yawOf(s, ball);
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

// A camera going off after a big play near him - a score, a stop, the ball
// taken away (the 3D court's: see court3d/crew.ts flashOf): his camera up,
// a few frames each, at moments of his own - for so long after it.
const FLASH_NEAR = 90;
const FLASH_FROM = 80;
const FLASH_SPREAD = 1400;
const FLASH_FRAME = 90;
const FLASH_SHOTS = 3;
// Where it goes off: this far out in front of him, this high (his eye),
// stood or down on a knee.
const FLASH_OUT = 1.3 * FIGURE_SCALE;
const FLASH_UP = 5.6 * FIGURE_SCALE;
const FLASH_UP_KNEEL = 3.5 * FIGURE_SCALE;

const unit = (a: number, b: number): number => {
	let h =
		Math.imul((a | 0) ^ 0x9e3779b9, 0x85ebca6b) ^ Math.imul(b + 1, 0xc2b2ae35);
	h ^= h >>> 15;
	h = Math.imul(h, 0x2c1b3c6d);
	h ^= h >>> 12;
	return (h >>> 0) / 2 ** 32;
};

export const flashesAt = (men: Photographer[], m: FieldMoment): Pt3[] => {
	// (The moment the crowd is up for - where whoever made it is, or the
	// ball.)
	const big = m.play?.cheer?.big ? m.play.cheer : undefined;
	const at = m.play?.party?.at ?? m.focus;
	const since = big ? m.t - big.t : -1;
	if (
		!big ||
		!at ||
		since < 0 ||
		since > FLASH_FROM + FLASH_SPREAD + FLASH_FRAME
	) {
		return [];
	}
	const out: Pt3[] = [];
	for (const s of men) {
		if (
			!shooting(s, m) ||
			Math.hypot(s.at.x - at.x, s.at.y - at.y) > FLASH_NEAR
		) {
			continue;
		}
		for (let k = 0; k < FLASH_SHOTS; k++) {
			const age =
				since -
				FLASH_FROM -
				unit(Math.round(big.t) + k * 7, -s.id) * FLASH_SPREAD;
			if (age >= 0 && age < FLASH_FRAME) {
				const yaw = yawOf(s, m.focus);
				out.push({
					x: s.at.x + Math.cos(yaw) * FLASH_OUT,
					y: s.at.y + Math.sin(yaw) * FLASH_OUT,
					z: s.move === "takeKnee" ? FLASH_UP_KNEEL : FLASH_UP,
				});
				break;
			}
		}
	}
	return out;
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
		const placed = { x: st.x, y: st.y, z: drawnLift(st), yaw: st.yaw };
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

// ---- the sideline reporter's microphone, drawn -------------------------------

// Its handle (long, and thick), up out of her right hand and a little
// forward - its head, round, and the flag under it with the network's
// colors.
const MIC_HANDLE = "#3a3b40";
const MIC_HEAD = "#141416";
const MIC_FLAG = "#b8202c";
const MIC_FT = 0.6 * FIGURE_SCALE;
const MIC_D = 0.1 * FIGURE_SCALE;
const MIC_HEAD_R = 0.12 * FIGURE_SCALE;
const MIC_FLAG_FT = 0.2 * FIGURE_SCALE;
const MIC_LEAN = 0.3;
export const micGear = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	states: FieldPlayerState[],
	bodyOf: (pid: number) => Body,
	pid: number,
	fine: boolean,
): Item[] =>
	states.flatMap((st) => {
		if (st.id !== pid) {
			return [];
		}
		// (In her hand as she is drawn: out of what she was doing, too.)
		const body = bodyOf(st.id);
		const { pose, yaw } = drawnPose(st, fine);
		const sk = skeleton(body, pose);
		const g = sk.armR.tip ?? sk.armR.end;
		const on = drawnShift(st, body, fine);
		const placed = {
			x: st.x + on.x,
			y: st.y + on.y,
			z: drawnLift(st, fine),
			yaw,
		};
		const at = bodyPoint(placed, g);
		const top = bodyPoint(placed, {
			f: g.f + MIC_FT * MIC_LEAN,
			s: g.s,
			u: g.u + MIC_FT,
		});
		return [
			{
				depth: Math.min(depthOf(cam, at), depthOf(cam, top)),
				draw: () => {
					drawMic(ctx, cam, at, top);
				},
			},
		];
	});

const drawMic = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	at: Pt3,
	top: Pt3,
) => {
	const a = project(cam, at);
	const b = project(cam, top);
	ctx.lineCap = "round";
	ctx.strokeStyle = MIC_HANDLE;
	ctx.lineWidth = Math.max(MIN_PX, MIC_D * a.k);
	ctx.beginPath();
	ctx.moveTo(a.x, a.y);
	ctx.lineTo(b.x, b.y);
	ctx.stroke();
	// (The flag, square round the handle just under the head.)
	const u = 1 - MIC_FLAG_FT / MIC_FT;
	const fx = a.x + (b.x - a.x) * u;
	const fy = a.y + (b.y - a.y) * u;
	const w = Math.max(MIN_PX + 1, MIC_FLAG_FT * a.k);
	ctx.fillStyle = MIC_FLAG;
	ctx.fillRect(fx - w / 2, fy - w / 2, w, w);
	ctx.fillStyle = MIC_HEAD;
	ctx.beginPath();
	ctx.arc(b.x, b.y, Math.max(MIN_PX, MIC_HEAD_R * b.k), 0, Math.PI * 2);
	ctx.fill();
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
		const placed = { x: st.x, y: st.y, z: drawnLift(st), yaw: st.yaw };
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

// A COACH'S PLAY SHEET: laminated, a grid of plays printed on it, a band of
// color along its top - in his hand going over it with a man (at his chest)
// or calling a play in (up over his mouth: see poses.ts callIn). As big as
// it is (feet), its face out the way he faces, its top edge this far up
// and out from his fingers; faded in and out with the move it is held in.
const SHEET_W = 0.72 * FIGURE_SCALE;
const SHEET_H = 0.9 * FIGURE_SCALE;
const SHEET_TOP = 0.1 * FIGURE_SCALE;
const SHEET_OUT = 0.08 * FIGURE_SCALE;
const SHEET_FACE = "#efede4";
const SHEET_EDGE = "#8d8a80";
const SHEET_BAND = "#b8202c";
const SHEET_LINE = "rgba(70, 70, 80, 0.55)";
const SHEET_MOVES = new Set<Move>(["callIn", "coachTalk"]);
const SHEET_ROWS = 4;

export const sheetGear = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	states: FieldPlayerState[],
	bodyOf: (pid: number) => Body,
	fine: boolean,
): Item[] =>
	states.flatMap((st) => {
		const now = SHEET_MOVES.has(st.move);
		const was = st.from !== undefined && SHEET_MOVES.has(st.from.move);
		const alpha = now
			? st.from && !was
				? 1 - st.from.w
				: 1
			: was
				? st.from!.w
				: 0;
		if (alpha <= 0.02) {
			return [];
		}
		const body = bodyOf(st.id);
		const { pose, yaw } = drawnPose(st, fine);
		const sk = skeleton(body, pose);
		// (In whichever hand he has up nearer his face.)
		const near = (l: { end: V3; tip?: V3 }) => l.tip ?? l.end;
		const gap = (v: V3) =>
			Math.hypot(v.f - sk.head.f, v.s - sk.head.s, v.u - sk.head.u);
		const g =
			gap(near(sk.armL)) <= gap(near(sk.armR)) ? near(sk.armL) : near(sk.armR);
		const on = drawnShift(st, body, fine);
		const placed = {
			x: st.x + on.x,
			y: st.y + on.y,
			z: drawnLift(st, fine),
			yaw,
		};
		// (Its middle in front of him, off the fingers holding it at its
		// edge.)
		const c = {
			f: g.f + SHEET_OUT,
			s: g.s + (sk.chest.s - g.s) * 0.7,
			u: g.u + SHEET_TOP - SHEET_H / 2,
		};
		const at = (a: number, b: number) =>
			bodyPoint(placed, { f: c.f, s: c.s + a * SHEET_W, u: c.u + b * SHEET_H });
		const corners = [
			at(-0.5, 0.5),
			at(0.5, 0.5),
			at(0.5, -0.5),
			at(-0.5, -0.5),
		];
		return [
			{
				depth: Math.min(...corners.map((p) => depthOf(cam, p))),
				draw: () => {
					drawSheet(ctx, cam, at, alpha);
				},
			},
		];
	});

const drawSheet = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	at: (a: number, b: number) => { x: number; y: number; z: number },
	alpha: number,
) => {
	const P = (a: number, b: number) => project(cam, at(a, b));
	const quad = (a0: number, b0: number, a1: number, b1: number) => {
		const ps = [P(a0, b0), P(a1, b0), P(a1, b1), P(a0, b1)];
		ctx.beginPath();
		ps.forEach((p, n) =>
			n === 0 ? ctx.moveTo(p.x, p.y) : ctx.lineTo(p.x, p.y),
		);
		ctx.closePath();
	};
	ctx.save();
	ctx.globalAlpha *= alpha;
	quad(-0.5, 0.5, 0.5, -0.5);
	ctx.fillStyle = SHEET_FACE;
	ctx.fill();
	const k = P(0, 0).k;
	ctx.lineWidth = Math.max(0.5, 0.02 * k);
	ctx.strokeStyle = SHEET_EDGE;
	ctx.stroke();
	// (Too small to make out: the sheet and its edge, no more.)
	if (k * SHEET_W >= 10) {
		quad(-0.5, 0.5, 0.5, 0.36);
		ctx.fillStyle = SHEET_BAND;
		ctx.fill();
		ctx.strokeStyle = SHEET_LINE;
		ctx.beginPath();
		for (let n = 1; n < SHEET_ROWS; n++) {
			const b = 0.36 - (0.86 * n) / SHEET_ROWS;
			const p = P(-0.44, b);
			const q = P(0.44, b);
			ctx.moveTo(p.x, p.y);
			ctx.lineTo(q.x, q.y);
		}
		const m0 = P(0, 0.36);
		const m1 = P(0, -0.46);
		ctx.moveTo(m0.x, m0.y);
		ctx.lineTo(m1.x, m1.y);
		ctx.stroke();
	}
	ctx.restore();
};

// THE TABLET: the quarterback over it with his coordinator on the sideline
// (see director.ts TABLET_SHARE) - held in both hands in front of him, its
// screen tipped up to his face: this wide and tall, out from his hands this
// far, tipped back this far (degrees from upright); a dark case round a
// lit screen, a play drawn up on it.
const TABLET_W = 0.82 * FIGURE_SCALE;
const TABLET_H = 0.6 * FIGURE_SCALE;
const TABLET_OUT = 0.06 * FIGURE_SCALE;
const TABLET_TIP = 50;
const TABLET_CASE = "#17191e";
const TABLET_SCREEN = "#d9e4ef";
const TABLET_INK = "rgba(30, 60, 120, 0.8)";
const TABLET_RED = "rgba(190, 40, 40, 0.85)";
const TABLET_RIM = 0.07;

export const tabletGear = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	states: FieldPlayerState[],
	bodyOf: (pid: number) => Body,
	fine: boolean,
): Item[] =>
	states.flatMap((st) => {
		const now = st.move === "tablet";
		const was = st.from?.move === "tablet";
		const alpha = now
			? st.from && !was
				? 1 - st.from.w
				: 1
			: was
				? st.from!.w
				: 0;
		if (alpha <= 0.02) {
			return [];
		}
		const body = bodyOf(st.id);
		const { pose, yaw } = drawnPose(st, fine);
		const sk = skeleton(body, pose);
		const tip = (l: { end: V3; tip?: V3 }) => l.tip ?? l.end;
		const l = tip(sk.armL);
		const r = tip(sk.armR);
		const on = drawnShift(st, body, fine);
		const placed = {
			x: st.x + on.x,
			y: st.y + on.y,
			z: drawnLift(st, fine),
			yaw,
		};
		// (Between his hands - the lower of them, a finger to it, has it -
		// and out from them a little.)
		const c = {
			f: Math.max(l.f, r.f) + TABLET_OUT,
			s: (l.s + r.s) / 2,
			u: Math.min(l.u, r.u),
		};
		const a = (TABLET_TIP * Math.PI) / 180;
		const at = (x: number, y: number) =>
			bodyPoint(placed, {
				f: c.f + y * TABLET_H * Math.sin(a),
				s: c.s + x * TABLET_W,
				u: c.u + y * TABLET_H * Math.cos(a),
			});
		const corners = [
			at(-0.5, 0.5),
			at(0.5, 0.5),
			at(0.5, -0.5),
			at(-0.5, -0.5),
		];
		// (Its screen seen, or the back of it: which side of it the camera
		// is on.)
		const o = at(0, 0);
		const across = sub3(at(0.5, 0), o);
		const upIt = sub3(at(0, 0.5), o);
		const face = {
			x: across.y * upIt.z - across.z * upIt.y,
			y: across.z * upIt.x - across.x * upIt.z,
			z: across.x * upIt.y - across.y * upIt.x,
		};
		const eye = sub3(cam.pos, o);
		const screen = face.x * eye.x + face.y * eye.y + face.z * eye.z > 0;
		return [
			{
				depth: Math.min(...corners.map((p) => depthOf(cam, p))),
				draw: () => {
					drawTablet(ctx, cam, at, alpha, screen);
				},
			},
		];
	});

const sub3 = (p: Pt3, q: Pt3): Pt3 => ({
	x: p.x - q.x,
	y: p.y - q.y,
	z: p.z - q.z,
});

const drawTablet = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	at: (x: number, y: number) => Pt3,
	alpha: number,
	screen: boolean,
) => {
	const P = (x: number, y: number) => project(cam, at(x, y));
	const quad = (x0: number, y0: number, x1: number, y1: number) => {
		const ps = [P(x0, y0), P(x1, y0), P(x1, y1), P(x0, y1)];
		ctx.beginPath();
		ps.forEach((p, n) =>
			n === 0 ? ctx.moveTo(p.x, p.y) : ctx.lineTo(p.x, p.y),
		);
		ctx.closePath();
	};
	ctx.save();
	ctx.globalAlpha *= alpha;
	quad(-0.5, 0.5, 0.5, -0.5);
	ctx.fillStyle = TABLET_CASE;
	ctx.fill();
	if (screen) {
		const e = 0.5 - TABLET_RIM;
		quad(-e, e, e, -e);
		ctx.fillStyle = TABLET_SCREEN;
		ctx.fill();
		// (Too small to make out: the screen lit, no more.)
		const k = P(0, 0).k;
		if (k * TABLET_W >= 14) {
			const line = (pts: [number, number][], color: string) => {
				ctx.beginPath();
				pts.forEach(([x, y], n) => {
					const p = P(x, y);
					if (n === 0) {
						ctx.moveTo(p.x, p.y);
					} else {
						ctx.lineTo(p.x, p.y);
					}
				});
				ctx.strokeStyle = color;
				ctx.stroke();
			};
			ctx.lineWidth = Math.max(0.5, 0.012 * k);
			// (The line of scrimmage across it, three routes off it.)
			line(
				[
					[-0.36, -0.12],
					[0.36, -0.12],
				],
				TABLET_INK,
			);
			line(
				[
					[-0.3, -0.18],
					[-0.3, 0.2],
					[-0.16, 0.3],
				],
				TABLET_RED,
			);
			line(
				[
					[0, -0.18],
					[0, 0.06],
					[0.18, 0.24],
				],
				TABLET_RED,
			);
			line(
				[
					[0.3, -0.18],
					[0.3, 0.32],
				],
				TABLET_RED,
			);
		}
	}
	ctx.restore();
};
