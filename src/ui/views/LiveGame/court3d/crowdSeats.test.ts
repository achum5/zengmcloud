import { assert, test } from "vitest";
import { seatsAt } from "./evaluate.ts";
import { compile } from "./testGame.ts";

// The building empties and fills: the seats full through the first half,
// half of them empty as the third quarter starts and filled again a few
// minutes in.

test("the crowd comes back from halftime", () => {
	for (const seed of ["seats-a", "seats-b"]) {
		const { tl } = compile(seed, 80);
		const periods = tl.beats.filter((b) => b.type === "period");
		assert.isAtLeast(periods.length, 2, seed);
		const third = periods[1]!;
		const fourth = periods[2];
		for (let t = 0; t < periods[0]!.preStart; t += 5000) {
			assert.strictEqual(seatsAt(tl, t), 1, `${seed} ${t}`);
		}
		assert.isAtMost(seatsAt(tl, third.preStart + 1), 0.6, seed);
		const end = fourth?.preStart ?? tl.end;
		assert.isAbove(seatsAt(tl, end - 1), 0.95, seed);
		// Filling, never emptying again, through the quarter.
		let last = 0;
		for (let t = third.preStart + 1; t < end; t += 2000) {
			const s = seatsAt(tl, t);
			assert.isAtLeast(s, last, `${seed} ${t}`);
			last = s;
		}
	}
}, 60_000);
