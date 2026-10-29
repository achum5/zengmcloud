import { assert, describe, test } from "vitest";
import {
	bodyPoint,
	evalBall,
	evalPlayer,
	gripped,
	holdOf,
	onRim,
	poseOf,
	withBody,
	BALL_R,
	type PlayerState,
} from "./evaluate.ts";
import { bodyOf, skeleton } from "./poses.ts";
import { compile } from "./testGame.ts";
import type { CourtTimeline } from "./director.ts";

const body = bodyOf();

// The ball as it is drawn: in his picture, held the way his move holds it,
// while he holds it (see sculpt.ts) - on its own, where it is, otherwise.
const drawn = (tl: CourtTimeline, t: number) => {
	const b = evalBall(tl, t, () => body);
	if (b.holder === undefined) {
		return { at: b, holder: undefined };
	}
	const st = withBody(evalPlayer(tl, b.holder, t), body);
	return {
		at: st.holding ? bodyPoint(st, holdOf(st, body, poseOf(st)).ball) : b,
		holder: b.holder,
	};
};

// His hands as they are drawn (see sculpt.ts): the middle of each palm.
const palms = (st0: PlayerState) => {
	const st = withBody(st0, body);
	const q = poseOf(st);
	const held =
		st.holding || st.grip !== undefined ? holdOf(st, body, q) : undefined;
	const sk = held
		? gripped(held.sk, st, body, q)
		: onRim(skeleton(body, q), st, body);
	return [sk.armR, sk.armL].map((arm) => {
		const tip = arm.tip ?? arm.end;
		return bodyPoint(st, {
			f: (arm.end.f + tip.f) / 2,
			s: (arm.end.s + tip.s) / 2,
			u: (arm.end.u + tip.u) / 2,
		});
	});
};

const gap = (a: { x: number; y: number; z: number }, b: typeof a) =>
	Math.hypot(a.x - b.x, a.y - b.y, a.z - b.z);

describe("the ball in his hands", () => {
	// Taken into his hands - up off his dribble, off the floor - it comes up
	// into them, not into his picture at his chest in a frame: the ball as
	// drawn never jumps from one frame to the next while one man has it.
	test("never jumps into his hands", () => {
		const { tl } = compile("hands", 70);
		const fast = (t: number) =>
			tl.fast.some(([a, b]) => t >= a - 80 && t < b + 80);
		const DT = 16;
		const end = Math.min(tl.end, 600_000);
		const snaps: string[] = [];
		let a = drawn(tl, 0);
		let b = drawn(tl, DT);
		let c = drawn(tl, 2 * DT);
		for (let t = 3 * DT; t < end; t += DT) {
			const d = drawn(tl, t);
			const one =
				b.holder !== undefined &&
				a.holder === b.holder &&
				b.holder === c.holder &&
				c.holder === d.holder;
			if (one && !fast(t)) {
				const s0 = gap(a.at, b.at);
				const s1 = gap(b.at, c.at);
				const s2 = gap(c.at, d.at);
				// (Far more in the one frame than in those either side.)
				if (s1 > 0.35 && s1 > 2.5 * Math.max(s0, s2, 0.05)) {
					snaps.push(`${t - DT} ${b.holder} ${s1.toFixed(2)}`);
				}
			}
			a = b;
			b = c;
			c = d;
		}
		assert.isAtMost(snaps.length, 2, snaps.join(", "));
	}, 900_000);

	// A pass goes out of his hands - not up off the floor out of a bounce
	// of his dribble, as if it had passed itself.
	test("a pass leaves from the hands", () => {
		const { tl } = compile("hands", 70);
		let passes = 0;
		const bad: string[] = [];
		for (const s of tl.ball) {
			// (A pass: to a man of his own. Picked off or stripped, it goes
			// from wherever it was.)
			if (
				s.kind !== "fly" ||
				!("pid" in s.from) ||
				!("pid" in s.to) ||
				tl.tracks.get(s.from.pid)?.team !== tl.tracks.get(s.to.pid)?.team
			) {
				continue;
			}
			passes += 1;
			const t = s.t0 + 1;
			const ball = evalBall(tl, t, () => body);
			const off =
				Math.min(
					...palms(evalPlayer(tl, s.from.pid, t)).map((p) => gap(p, ball)),
				) - BALL_R;
			if (off > 0.3) {
				bad.push(`${Math.round(s.t0)} ${s.from.pid} ${off.toFixed(2)}`);
			}
		}
		assert.isAbove(passes, 100);
		assert.isAtMost(bad.length, 1, bad.join(", "));
	}, 900_000);
});
