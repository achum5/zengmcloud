import { assert, describe, test } from "vitest";
import { buildFouls, foulsAt } from "./scoreBug.ts";
import { compile } from "./testGame.ts";

// Lines at one-second steps, in order: a beat for each.
const run = (events: any[], limits?: number[]) => {
	const beats = events.map((_, i) => ({
		i,
		type: events[i].type,
		preStart: i * 1000,
		actionStart: i * 1000,
		end: i * 1000 + 500,
	}));
	return buildFouls({ beats }, events, limits);
};

describe("team fouls on the score bug", () => {
	test("counted per team, starting over each period", () => {
		const marks = run([
			{ type: "period", period: 1 },
			{ type: "pfNonShooting", t: 0, clock: 600 },
			{ type: "pfFG", t: 1, clock: 500 },
			{ type: "pfTP", t: 0, clock: 400 },
			{ type: "period", period: 2 },
			{ type: "pfBonus", t: 1, clock: 700 },
		]);
		assert.deepStrictEqual(foulsAt(marks, 3500).fouls, [2, 1]);
		assert.deepStrictEqual(foulsAt(marks, 4000).fouls, [0, 0]);
		assert.deepStrictEqual(foulsAt(marks, 5000).fouls, [0, 1]);
		// Before anything at all.
		assert.deepStrictEqual(foulsAt(marks, -1).fouls, [0, 0]);
	});

	test("a team is in the bonus once the other team reaches the limit", () => {
		const events: any[] = [{ type: "period", period: 1 }];
		for (let k = 0; k < 5; k++) {
			events.push({ type: "pfNonShooting", t: 1, clock: 600 - k * 10 });
		}
		const marks = run(events);
		assert.deepStrictEqual(foulsAt(marks, 4000).bonus, [false, false]);
		// The visitors' fifth: the home team shoots.
		assert.deepStrictEqual(foulsAt(marks, 5000).bonus, [true, false]);
	});

	test("the lower limit in the last two minutes, and the overtime limit", () => {
		const late = run([
			{ type: "period", period: 1 },
			{ type: "pfNonShooting", t: 0, clock: 100 },
			{ type: "pfNonShooting", t: 0, clock: 50 },
		]);
		assert.deepStrictEqual(foulsAt(late, 2000).bonus, [false, true]);
		const ot = run([
			{ type: "overtime", period: 5 },
			...Array.from({ length: 4 }, (_, k) => ({
				type: "pfNonShooting",
				t: 0,
				clock: 290 - k * 10,
			})),
		]);
		assert.deepStrictEqual(foulsAt(ot, 4000).bonus, [false, true]);
	});

	test("a whole game adds up to the box score's fouls", () => {
		const { events, tl } = compile("a", 80);
		const marks = buildFouls(tl, events);
		// Every foul line lands in the running count of its period.
		const pf = events.filter(
			(e) => /^pf/.test(e.type) && (e.t === 0 || e.t === 1),
		).length;
		assert.isAbove(pf, 0);
		let counted = 0;
		let prev: [number, number] = [0, 0];
		for (const m of marks) {
			const d = m.fouls[0] + m.fouls[1] - (prev[0] + prev[1]);
			if (d > 0) {
				counted += d;
			}
			prev = m.fouls;
		}
		assert.strictEqual(counted, pf);
	});
});
