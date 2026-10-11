import { assert, describe, test } from "vitest";
import { floorSpotOf } from "./evaluate.ts";
import { compile } from "./testGame.ts";

// Wherever two men would end up in each other at real speed, one is kept off
// the other (see keepOff in director.ts): bodies meet, they don't pass
// through.

describe("keeping bodies apart", () => {
	test("hardly ever is one man drawn inside another", () => {
		const { tl } = compile("apart", 80);
		const tracks = [...tl.tracks.values()];
		const fast = (t: number) => tl.fast.some(([a, b]) => t >= a && t < b);
		let live = 0;
		let inside = 0;
		for (let t = 0; t < tl.end; t += 50) {
			if (fast(t)) {
				continue;
			}
			live += 1;
			const at = tracks
				.map((tr) => floorSpotOf(tr, t))
				.filter((p) => p !== undefined);
			for (let i = 0; i < at.length; i++) {
				for (let j = i + 1; j < at.length; j++) {
					const d = Math.hypot(at[i]!.x - at[j]!.x, at[i]!.y - at[j]!.y);
					if (d < 0.9) {
						inside += 1;
					}
				}
			}
		}
		// (Counted per pair of men, per moment of real speed: left to how
		// the runs fell, about one in twenty.)
		assert.isAbove(live, 1000);
		assert.isBelow(inside / live, 0.01);
	});

	test("kept off a man, he eases over and back - no jump", () => {
		const { tl } = compile("apart", 80);
		let seen = 0;
		for (const tr of tl.tracks.values()) {
			for (const s of tr.sep ?? []) {
				seen += 1;
				assert.strictEqual(s.dx.at(-1), 0);
				assert.strictEqual(s.dy.at(-1), 0);
				assert.isBelow(Math.hypot(s.dx[0]!, s.dy[0]!), 0.05);
				for (let i = 1; i < s.dx.length; i++) {
					const step = Math.hypot(
						s.dx[i]! - s.dx[i - 1]!,
						s.dy[i]! - s.dy[i - 1]!,
					);
					// (No faster than a quick step aside: feet per 50 ms.)
					assert.isBelow(step, 0.75);
				}
			}
		}
		assert.isAbove(seen, 0);
	});
});
