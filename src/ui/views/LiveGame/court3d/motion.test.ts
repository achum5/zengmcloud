import { assert, describe, test } from "vitest";
import {
	BURST,
	alongShape,
	keepThrough,
	paceFor,
	runMs,
	runShape,
} from "./motion.ts";

// A run of every length and time a game asks for, from a standstill or on
// the move, pulled up or going straight on.
const cases = (() => {
	const out: [number, number, number, number][] = [];
	for (const L of [0.4, 2, 5, 12, 30, 60]) {
		for (const T of [0.25, 0.5, 1, 2, 4]) {
			for (const v0 of [0, 6, 18]) {
				for (const v1 of [0, 8]) {
					out.push([L, T, v0, v1]);
				}
			}
		}
	}
	return out;
})();

describe("motion", () => {
	test("a run gets him exactly there, on time, never going back", () => {
		for (const [L, T, v0, v1] of cases) {
			const r = runShape(L, T, v0, v1);
			assert.closeTo(alongShape(r, T), L, 1e-6, `${L} ${T} ${v0} ${v1}`);
			assert.strictEqual(alongShape(r, 0), 0);
			let last = 0;
			for (let k = 1; k <= 200; k++) {
				const d = alongShape(r, (T * k) / 200);
				assert.isAtLeast(d, last - 1e-9, `${L} ${T} ${v0} ${v1} at ${k}`);
				last = d;
			}
		}
	});

	// No step in how fast he goes: what he has in hand coming into it, he
	// goes on at, and ends going what he is meant to.
	test("how fast he goes changes smoothly, start to finish", () => {
		for (const [L, T, v0, v1] of cases) {
			const r = runShape(L, T, v0, v1);
			if (r.vc === r.v0 && r.ta === 0 && r.td === 0) {
				// (Nothing fits: an even pace there.)
				continue;
			}
			const h = 1e-5;
			const pace = (s: number) =>
				(alongShape(r, s + h) - alongShape(r, s - h)) / (2 * h);
			assert.closeTo(pace(2 * h), v0, 0.05, `${L} ${T} ${v0} ${v1}`);
			assert.closeTo(pace(T - 2 * h), v1, 0.05, `${L} ${T} ${v0} ${v1}`);
			// From one look to the next, no more of a change than the hardest
			// push or pull of his ramps makes in the time - none at all
			// between them.
			const hardest =
				1.5 *
				Math.max(
					r.ta > 0 ? Math.abs(r.vc - v0) / r.ta : 0,
					r.td > 0 ? Math.abs(r.vc - v1) / r.td : 0,
				);
			const n = 2000;
			let prev = pace(2 * h);
			for (let k = 1; k < n; k++) {
				const v = pace((T * k) / n);
				assert.isAtMost(
					Math.abs(v - prev),
					hardest * (T / n) * 1.02 + 1e-4,
					`${L} ${T} ${v0} ${v1} at ${k}`,
				);
				prev = v;
			}
		}
	});

	test("a run timed by runMs is one he can make at his usual push", () => {
		for (const L of [3, 8, 15, 30, 50]) {
			for (const speed of [8, 14, 21]) {
				const ms = runMs(L, speed);
				const r = runShape(L, ms / 1000);
				assert.isAtMost(r.vc, speed + 0.05, `${L} ${speed}`);
				// Going flat out, he is no later there than easing along.
				assert.isBelow(runMs(L, 21), runMs(L, 8) + 2);
				// All out, quicker still.
				assert.isBelow(runMs(L, speed, 0, BURST), ms);
			}
		}
		// Already going, quicker there than from a standstill.
		assert.isBelow(runMs(20, 21, 15), runMs(20, 21));
	});

	test("the pace for a time gets him there in it", () => {
		for (const L of [4, 10, 25, 45]) {
			for (const ms of [600, 1200, 2500, 5000]) {
				const v = paceFor(L, ms, 0, 23);
				if (v < 23) {
					assert.closeTo(runMs(L, v), ms, 2, `${L} ${ms}`);
				} else {
					assert.isAtLeast(runMs(L, 23), ms - 2, `${L} ${ms}`);
				}
			}
		}
	});

	test("pace through a join: all of it straight on, none turning back", () => {
		assert.strictEqual(keepThrough(1), 1);
		assert.strictEqual(keepThrough(-1), 0);
		assert.isAbove(keepThrough(0), 0.3);
		assert.isBelow(keepThrough(0), 0.4);
	});
});
