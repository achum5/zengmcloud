import { assert, test } from "vitest";
import { courtsideFor } from "./courtside.ts";
import { BENCH_SEATS, benchStart, COURT_W, SEAT_GAP } from "./geometry.ts";

// The courtside seats: behind each baseline, clear of the basket's
// stanchion, and along the far side but not right behind a bench - nobody on
// the floor, the same people every time the game is shown.

test("the courtside seats are off the floor, clear of the stanchion", () => {
	const folk = courtsideFor(77, undefined, undefined);
	assert.deepEqual(courtsideFor(77, undefined, undefined), folk);
	assert.isAbove(folk.length, 70);
	for (const p of folk) {
		if (p.facing === "north") {
			assert.isBelow(p.y, -10);
			// Nobody right behind a bench.
			for (const t of [0, 1] as const) {
				const x0 = benchStart(t);
				assert.isFalse(p.x > x0 && p.x < x0 + BENCH_SEATS * SEAT_GAP);
			}
			continue;
		}
		const back = p.x < 0 ? -p.x : p.x - COURT_W;
		assert.isAbove(back, 6.5);
		assert.strictEqual(p.facing, p.x < 0 ? "east" : "west");
		// Not on top of the stanchion's padded base.
		assert.isFalse(back < 9.6 && p.y > 22 && p.y < 28);
	}
});
