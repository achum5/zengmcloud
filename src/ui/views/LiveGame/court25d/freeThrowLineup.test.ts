import { assert, test } from "vitest";
import { compileCourt } from "./director.ts";
import { ftDefenseSpot, ftOffenseSpot } from "./geometry.ts";
import { fakeGame } from "./testGame.ts";

// THE FREE THROW LINEUP NEVER RUNS OUT OF SPOTS.
//
// The lane and the arc have places for a five-man game. A league that puts
// more on the floor, or a shooter the court didn't have on the floor (events
// from another game reaching a page still holding the previous one), used to
// ask for a spot past the end of the table and crash the whole live game
// page: "... is not iterable at beatFreeThrow".

test("there is a spot for every man, however many are on the floor", () => {
	const seen = new Set<string>();
	for (let j = 0; j < 12; j++) {
		for (const spot of [ftDefenseSpot(j), ftOffenseSpot(j)]) {
			assert.isArray(spot);
			assert.lengthOf(spot, 2);
			assert.isTrue(spot.every(Number.isFinite));
		}
		seen.add(ftDefenseSpot(j).join(","));
	}
	// And they don't all pile onto one spot.
	assert.isAbove(seen.size, 8);
});

test("a free throw by a man the court has on the bench doesn't crash", () => {
	const { events, players } = fakeGame("ft-bench", 30);
	// Player 8 is on the home bench in the fake game; send him to the line
	// without a substitution bringing him on.
	const at = events.findIndex((e) => e.type === "jumpBall") + 1;
	const clock = events[at - 1]!.clock - 1;
	events.splice(
		at,
		0,
		{ type: "pf", t: 1, pid: 11, clock },
		{ type: "ft", t: 0, pid: 8, clock },
		{ type: "missFt", t: 0, pid: 8, clock },
		{ type: "drb", t: 1, pid: 12, clock: clock - 1 },
	);
	assert.doesNotThrow(() => compileCourt({ events, players, gid: 1 }));
});
