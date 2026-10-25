import { assert, test } from "vitest";
import {
	benchMoodAt,
	benchPhase,
	benchShare,
	type BenchMood,
} from "./scene.ts";
import { compile } from "./testGame.ts";
import { ANIMS } from "./poses.ts";

// The bench idles at the pace each pose was written for, on the wall clock -
// not at a flat 1.5 cycles a second of game time, which rocked every seated
// player all night and made the bench twitch through fast-forwards.

test("a seated player shifts in his seat slowly", () => {
	const sit = ANIMS.sit;
	assert.strictEqual(sit.kind, "loop");
	const perSecond = benchPhase("sit", 1000, 3) - benchPhase("sit", 0, 3);
	assert.closeTo(perSecond, sit.kind === "loop" ? sit.fps / sit.n : 0, 1e-9);
	assert.isBelow(perSecond, 0.5);
});

test("the bench isn't all in step", () => {
	const phases = [1, 2, 3, 4, 5].map((pid) => benchPhase("sit", 0, pid) % 1);
	assert.isAbove(new Set(phases.map((x) => x.toFixed(3))).size, 3);
});

// Up off the bench for a big play and sitting back down, the whole bench got
// there in a frame - popped up and dropped back each time.
test("the bench gets up and sits back down over a moment, not in a frame", () => {
	const { tl } = compile("bench rises", 160);
	const HIGH: Record<BenchMood, number> = { sat: 0, stood: 0.6, up: 1 };
	const FRAME = 1000 / 30;
	for (const team of [0, 1] as const) {
		// (Frame by frame round each change.)
		let changes = 0;
		for (let t = 250; t < tl.end; t += 250) {
			if (benchMoodAt(tl, t, team).now === benchMoodAt(tl, t - 250, team).now) {
				continue;
			}
			changes++;
			let last: number | undefined;
			for (let u = t - 250; u < t + 750; u += FRAME) {
				const high = benchShare(benchMoodAt(tl, u, team), (x) => HIGH[x]);
				if (last !== undefined) {
					assert.isBelow(Math.abs(high - last), 0.15, `team ${team} at ${u}`);
				}
				last = high;
			}
		}
		assert.isAbove(changes, 4);
	}
}, 60_000);
