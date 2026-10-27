import { assert, test } from "vitest";
import { buildClocks, gameClockAt } from "./clock.ts";
import { compileCourt } from "./director.ts";
import { fakeGame, gidOf } from "./testGame.ts";

// An Elam Ending: the game clock goes off (the sim's reads Infinity from there)
// and the game is played to a target score. The bug shows the target where the
// clock was - never "NaN:NaN" from running a clock that has no reading.
test("an Elam Ending puts the target where the clock was", () => {
	const { events, players } = fakeGame("elam", 150);
	const q4 = events.findIndex((e) => e.type === "period" && e.period === 4);
	assert.isAbove(q4, 0);
	const at = q4 + 40;
	assert.isBelow(at, events.length);
	events.splice(at, 0, { type: "elamActive", target: 131 });
	for (let i = at + 1; i < events.length; i++) {
		if (typeof events[i]!.clock === "number") {
			events[i]!.clock = Infinity;
		}
	}
	const tl = compileCourt({ events, players, gid: gidOf("elam") });
	const clocks = buildClocks(tl, events);
	assert.strictEqual(clocks.elam?.target, 131);
	const from = clocks.elam!.t;
	assert.isAbove(from, 0);
	assert.isBelow(from, tl.end);
	for (const m of clocks.marks) {
		assert.isTrue(Number.isFinite(m.clock), `mark at ${m.t}`);
	}
	for (let t = 0; t < tl.end; t += 500) {
		const c = gameClockAt(clocks, t);
		assert.isTrue(c === undefined || Number.isFinite(c), `${t}`);
	}
});
