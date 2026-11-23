import { assert, describe, test } from "vitest";
import type { Sample, Spans } from "../stepper.ts";
import { bodyOf } from "./body.ts";
import { followWith } from "./follow.ts";

const spans = (list: [number, number][]): Spans => ({
	hit: (a, b) => list.some(([s, e]) => s < b && e > a),
	next: (t) =>
		Math.min(...list.filter(([s]) => s >= t).map(([s]) => s), Infinity),
	last: (t) =>
		Math.max(...list.filter(([, e]) => e <= t).map(([, e]) => e), -Infinity),
});
const GUARD = bodyOf({ spd: 80, hgt: 30 });
const AVERAGE = bodyOf({ spd: 50, hgt: 50 });
const BIG = bodyOf({ spd: 40, hgt: 85 });
const pace = (q: Sample) => Math.hypot(q.vx, q.vy);
const at = (out: Sample[], t: number) => out.find((q) => q.t === t)!;
const off = (out: Sample[], play: (t: number) => { x: number; y: number }) =>
	Math.max(...out.map((q) => Math.hypot(q.x - play(q.t).x, q.y - play(q.t).y)));

// Stood to 2 s, then - as today's play has it - flat out at once, 18 feet
// a second, to the spot where he catches it at 5 s.
const sprint = (t: number) => ({
	x: t < 2000 ? 20 : t < 5000 ? 20 + (18 * (t - 2000)) / 1000 : 74,
	y: 25,
});
const CATCH = spans([[5000, 5500]]);

// Eased into a jog round the arc - 10 feet a second at most - and out of
// it again: what a body can do.
const jog = (t: number) => {
	const s = Math.min(1, Math.max(0, (t - 1000) / 10000));
	const a = Math.PI * (s * s * (3 - 2 * s));
	return { x: 5.25 + 22 * Math.sin(a), y: 25 - 22 * Math.cos(a) };
};

describe("3D possession sim: a man following today's play with his own body", () => {
	test("sent off flat out at once, he is into it before the play is - and never far off it", () => {
		for (const b of [GUARD, AVERAGE, BIG]) {
			const out = followWith(b)(sprint, 0, 6000, CATCH);
			assert.isAbove(pace(at(out, 2000)), 3);
			assert.isAtMost(off(out, sprint), 4.5);
			// (Kept up with, if more than his body has: no faster than
			// that, or the play, by more than he is pulled along - until
			// he is put on it for the catch.)
			for (const q of out.filter((x) => x.t <= 4500)) {
				assert.isAtMost(pace(q), Math.max(b.top, 18) + 5 + 1e-9);
			}
		}
	});

	test("on the play when something of his starts", () => {
		for (const b of [GUARD, BIG]) {
			const q = at(followWith(b)(sprint, 0, 6000, CATCH), 5000);
			assert.closeTo(q.x, 74, 1e-9);
			assert.closeTo(q.y, 25, 1e-9);
		}
	});

	test("a play his body can follow, he follows on his own, close by", () => {
		for (const b of [GUARD, BIG]) {
			const out = followWith(b)(jog, 0, 12000, spans([]));
			assert.isAtMost(off(out, jog), 1.5);
			for (const q of out) {
				assert.isAtMost(pace(q), b.top + 1e-9);
			}
		}
	});

	test("the same play followed the same way, every time", () => {
		const a = followWith(BIG)(sprint, 0, 6000, CATCH);
		const b = followWith(BIG)(sprint, 0, 6000, CATCH);
		assert.deepEqual(a, b);
	});
});
