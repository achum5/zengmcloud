import { assert, test } from "vitest";
import {
	BENCH_END,
	BENCH_LEN,
	BENCH_SEATS,
	benchStart,
	benchX,
	COURT_W,
	seatSpot,
	TABLE_X0,
	TABLE_X1,
} from "./geometry.ts";

// Along the far side: a bench, a few courtside seats, the table, a few
// more, the other bench - each the other's mirror.
test("the benches and the table along the far side, in that order", () => {
	assert.strictEqual(benchStart(0) + BENCH_LEN, BENCH_END);
	assert.closeTo(COURT_W - benchStart(1), BENCH_END, 1e-9);
	assert.closeTo(TABLE_X0 + TABLE_X1, COURT_W, 1e-9);
	// (Room for a couple of courtside seats between each bench and the
	// table.)
	assert.isAtLeast(TABLE_X0 - BENCH_END, 4.5);
	// (The coach's box in front of his own bench.)
	for (const t of [0, 1] as const) {
		const x0 = benchStart(t);
		assert.isAbove(benchX(t), x0);
		assert.isBelow(benchX(t), x0 + BENCH_LEN);
	}
});

// The men are stepped no further off the floor than this (see stepper's
// finish): every chair on the bench is one a man can get to.
test("every chair is somewhere a man can walk to", () => {
	for (const t of [0, 1] as const) {
		for (let i = 0; i < BENCH_SEATS; i++) {
			const s = seatSpot(t, i);
			assert.isAtLeast(s.x, -3.9 + 0.3);
			assert.isAtMost(s.x, COURT_W + 3.9 - 0.3);
			assert.isAtLeast(s.y, -8.4);
		}
	}
});
