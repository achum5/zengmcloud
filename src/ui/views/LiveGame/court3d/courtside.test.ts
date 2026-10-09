import { assert, test } from "vitest";
import { courtsideFor } from "./courtside.ts";
import { COURT_W } from "./geometry.ts";

// The courtside seats: behind each baseline, clear of the basket's
// stanchion, and along the far side - nobody on the floor, the same people
// every time the game is shown.

test("the courtside seats are off the floor, clear of the stanchion", () => {
	const folk = courtsideFor(77, undefined, undefined);
	assert.deepEqual(courtsideFor(77, undefined, undefined), folk);
	assert.isAbove(folk.length, 80);
	for (const p of folk) {
		if (p.facing === "north") {
			assert.isBelow(p.y, -10);
			continue;
		}
		const back = p.x < 0 ? -p.x : p.x - COURT_W;
		assert.isAbove(back, 6.5);
		assert.strictEqual(p.facing, p.x < 0 ? "east" : "west");
		// Not on top of the stanchion's padded base.
		assert.isFalse(back < 9.6 && p.y > 22 && p.y < 28);
	}
});
