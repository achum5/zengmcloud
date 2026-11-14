import { assert, describe, test } from "vitest";
import { evalPlayer, fastAt } from "./evaluate.ts";
import { rimX } from "./geometry.ts";
import { stepBeat, type AnimName } from "./poses.ts";
import { strideOf } from "./stepping.ts";
import { compile } from "./testGame.ts";

// AT REAL SPEED, REAL SPEED: a man's legs turn over as a player's do, and
// the men off the ball move with the play.

// Steps a second at pace v (feet a second): two a stride, at the gait's
// beat or as fast as its full stride takes him there.
const cadence = (anim: AnimName, v: number) =>
	2 * Math.max(stepBeat(anim), v / strideOf(anim));

describe("3D: tempo", () => {
	test("legs turn over at a player's rate at any pace, not wading through it", () => {
		// (Real players: about 2.7 steps a second on the run however easy,
		// 2.6 backpedaling, 2.8 sliding, 1.8 walking.)
		const at: [AnimName, number, number][] = [
			["run", 8, 2.6],
			["run", 11, 2.6],
			["jog", 6, 2.6],
			["jog", 8, 2.6],
			["dribble", 8, 2.6],
			["dribble", 10, 2.6],
			["back", 3, 2.5],
			["slide", 4, 2.7],
			["walk", 3, 1.75],
			["dribbleWalk", 4, 1.8],
		];
		for (const [anim, v, least] of at) {
			assert.isAtLeast(cadence(anim, v), least, `${anim} at ${v} ft/s`);
			assert.isAtMost(cadence(anim, v), 3.6, `${anim} at ${v} ft/s`);
		}
	});

	test("the men spotted up round the arc move with a drive, not stand and watch it", () => {
		const STEP = 100;
		let men = 0;
		let moved = 0;
		for (const seed of ["a", "b"]) {
			const { tl } = compile(seed, 120);
			const tracks = [...tl.tracks.values()].filter((tr) => tr.pid >= 0);
			let k = 0;
			let was: { pid: number; x: number; y: number } | undefined;
			let quiet = 0;
			for (let t = 0; t < tl.end; t += STEP) {
				while (k + 1 < tl.ball.length && tl.ball[k + 1]!.t0 <= t) {
					k++;
				}
				const s = tl.ball[k]!;
				const h = s.kind === "hold" && s.style !== "hold" ? s.pid : undefined;
				const me = h === undefined ? undefined : tl.tracks.get(h);
				if (!me || fastAt(tl, t) > 1.001 || t < quiet) {
					was = undefined;
					continue;
				}
				const P = evalPlayer(tl, me.pid, t);
				const rim = { x: rimX(me.team), y: 25 };
				const prev = was?.pid === me.pid ? was : undefined;
				was = { pid: me.pid, x: P.x, y: P.y };
				if (!prev) {
					continue;
				}
				const v = (Math.hypot(P.x - prev.x, P.y - prev.y) * 1000) / STEP;
				const r0 = Math.hypot(prev.x - rim.x, prev.y - rim.y);
				const r1 = Math.hypot(P.x - rim.x, P.y - rim.y);
				if (
					v < 9 ||
					((r0 - r1) * 1000) / STEP < 0.6 * v ||
					r1 > 30 ||
					fastAt(tl, t + 1000) > 1.001
				) {
					continue;
				}
				// A drive: where each teammate stood out on the floor is, a
				// second on.
				for (const tr of tracks) {
					if (tr.team !== me.team || tr.pid === me.pid) {
						continue;
					}
					const A = evalPlayer(tl, tr.pid, t);
					if (!A.shown || Math.hypot(A.x - rim.x, A.y - rim.y) < 16) {
						continue;
					}
					const B = evalPlayer(tl, tr.pid, t + 1000);
					men += 1;
					if (Math.hypot(B.x - A.x, B.y - A.y) >= 2) {
						moved += 1;
					}
				}
				quiet = t + 1500;
			}
		}
		assert.isAbove(men, 60);
		assert.isAbove(moved / men, 0.55, `${moved} of ${men}`);
	}, 600_000);
});
