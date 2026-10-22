import { assert, describe, test } from "vitest";
import { CROSS_RATE, evalBall } from "./evaluate.ts";
import { bodyOf } from "./poses.ts";
import { compile } from "./testGame.ts";

const body = bodyOf();
const bodyFor = () => body;

describe("dribble moves", () => {
	// Holding it in both hands and going straight into a crossover, the ball
	// goes down from where he had it into the first bounce - not over into
	// the one hand in a frame.
	test("a move out of both hands starts from where he held it", () => {
		const STEP = 16;
		let moves = 0;
		for (const seed of ["a", "b", "ball"]) {
			const { tl } = compile(seed, 140);
			for (let i = 1; i < tl.ball.length; i++) {
				const seg = tl.ball[i]!;
				const before = tl.ball[i - 1]!;
				if (
					seg.kind !== "hold" ||
					seg.style !== "cross" ||
					before.kind !== "hold" ||
					before.pid !== seg.pid ||
					before.style !== "hold" ||
					tl.cuts.some((c) => c > seg.t0 - 3 * STEP && c <= seg.t0 + 1000)
				) {
					continue;
				}
				moves += 1;
				let prev = evalBall(tl, seg.t0 - 2 * STEP, bodyFor);
				for (let t = seg.t0 - STEP; t < seg.t0 + 1000 / CROSS_RATE; t += STEP) {
					const b = evalBall(tl, t, bodyFor);
					const d = Math.hypot(b.x - prev.x, b.y - prev.y, b.z - prev.z);
					assert.isBelow(
						d,
						1,
						`${seed}: ball jumped ${d.toFixed(2)} ft at ${t} (pid ${seg.pid})`,
					);
					prev = b;
				}
			}
		}
		assert.isAbove(moves, 0);
	}, 120_000);
});
