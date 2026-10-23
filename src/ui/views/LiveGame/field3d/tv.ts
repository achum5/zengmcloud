import { makeCourtRng } from "../courtRng.ts";
import { anyBall, type CrewPart } from "./crew.ts";
import type { BallSeg, Leg, Part } from "./director.ts";
import { FIELD_L, FIELD_W, YD, type Dir, type Pt } from "./geometry.ts";
import type { Move } from "./poses.ts";

// THE TV CAMERAS ON THE SIDELINES: a man on each, a camera on his shoulder,
// out on the white border a few yards back of the line before the snap, and
// with the play once it goes - up and down the sideline after the ball as
// fast as he can go, a shuffle sideways while it is close, a run when it
// gets away - stopped once it is dead, the camera on it.

export const tvId = (side: 0 | 1) => -60 - side;
export const tvManOf = (id: number): 0 | 1 | undefined =>
	id === -60 ? 0 : id === -61 ? 1 : undefined;

// The men working the cameras: each one's build, the same every time the
// game is watched.
export const tvCrewFor = (gid: number) => {
	const r = makeCourtRng(`tv|${gid}`);
	return ([0, 1] as const).map((side) => ({
		side,
		id: tvId(side),
		hgt: 68 + Math.floor(r() * 7),
		weight: 175 + Math.floor(r() * 60),
	}));
};

// Off the sideline (feet), on the white border; back of the line before
// the snap (feet), and after the ball, behind it by this much.
const OFF = 3.5;
const BACK_OF_LINE = 6 * YD;
const TRAIL = 3 * YD;
// How quickly he closes on where he wants to be (per second), as fast as
// he goes, and close enough not to bother; and how often he looks again.
const GAIN = 1.4;
const FASTEST = 21;
const SETTLE = 4;
const STEP_MS = 200;
// Under this pace he shuffles sideways, facing the play; over it he runs.
const SHUFFLE_FPS = 7;
// Stopped this long after the whistle.
const AFTER_WHISTLE = 1500;
const ARMS: Move = "shoulderCam";

export type TvInput = {
	dir: Dir;
	losX: number;
	t0: number;
	snap: number;
	whistle: number;
	t1: number;
	parts: Part[];
	ball: BallSeg[];
	r: () => number;
};

const clampX = (x: number) => Math.min(FIELD_L + 2, Math.max(-2, x));

export const stageTv = (input: TvInput): CrewPart[] => {
	const { dir, losX, t0, snap, whistle, t1, parts, ball } = input;
	return ([0, 1] as const).map((side) => {
		const y = side === 0 ? -OFF : FIELD_W + OFF;
		// (Each his own way of going after it.)
		const trail = TRAIL * (0.6 + input.r() * 0.9);
		const gain = GAIN * (0.8 + input.r() * 0.4);
		const ballAt = (t: number): Pt => anyBall(ball, parts, t) ?? { x: losX, y };
		const facing = (at: Pt, t: number) => {
			const b = ballAt(t);
			return Math.atan2(b.y - at.y, b.x - at.x);
		};
		const legs: Leg[] = [];
		const push = (l: Leg) => {
			const last = legs.at(-1);
			if (
				last &&
				last.move === l.move &&
				last.move === "stand" &&
				Math.abs((last.yaw ?? 0) - (l.yaw ?? 0)) < 0.05
			) {
				last.t1 = l.t1;
				return;
			}
			legs.push(l);
		};
		// (Before the snap: back of the line, a step or two either way.)
		let x = clampX(losX - dir * (BACK_OF_LINE + (input.r() - 0.5) * 2 * YD));
		const start = { x, y };
		push({
			t0,
			t1: snap,
			from: start,
			to: start,
			move: "stand",
			yaw: facing(start, snap),
			arms: ARMS,
		});
		// (With the play, after the ball.)
		const stop = whistle + AFTER_WHISTLE;
		for (let t = snap; t < stop; t += STEP_MS) {
			const dt = Math.min(STEP_MS, stop - t);
			const want = clampX(ballAt(t + dt).x - dir * trail);
			const off = want - x;
			const fps =
				Math.abs(off) < SETTLE
					? 0
					: Math.min(FASTEST, (Math.abs(off) - SETTLE) * gain + 3);
			const from = { x, y };
			x = clampX(
				x + Math.sign(off) * Math.min(Math.abs(off), (fps * dt) / 1000),
			);
			const to = { x, y };
			if (fps === 0 || to.x === from.x) {
				push({
					t0: t,
					t1: t + dt,
					from,
					to: from,
					move: "stand",
					yaw: facing(from, t),
					arms: ARMS,
				});
			} else if (fps < SHUFFLE_FPS) {
				push({
					t0: t,
					t1: t + dt,
					from,
					to,
					move: "slide",
					yaw: side === 0 ? Math.PI / 2 : -Math.PI / 2,
					arms: ARMS,
				});
			} else {
				push({ t0: t, t1: t + dt, from, to, move: "run", arms: ARMS });
			}
		}
		// (And stopped, the camera on where it was blown dead.)
		const end = { x, y };
		const last = legs.at(-1)!;
		if (last.t1 < t1) {
			push({
				t0: last.t1,
				t1,
				from: end,
				to: end,
				move: "stand",
				yaw: facing(end, whistle),
				arms: ARMS,
			});
		}
		return { pid: tvId(side), team: side === 0 ? 1 : 0, legs };
	});
};
