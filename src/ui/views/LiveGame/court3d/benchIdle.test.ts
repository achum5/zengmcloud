import { assert, test } from "vitest";
import { benchPhase } from "./scene.ts";
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
