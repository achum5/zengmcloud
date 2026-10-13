import { assert, describe, test } from "vitest";
import { compile } from "./testGame.ts";
import {
	KEY_PLAY_DEFAULTS,
	parseKeyPlays,
	planKeyPlays,
	segmentAt,
} from "./keyPlays.ts";
import { fastAt } from "./evaluate.ts";

const watchMs = (tl: any, segs: { t0: number; t1: number }[]) => {
	let ms = 0;
	for (const s of segs) {
		for (let t = s.t0; t < s.t1; t += 50) {
			ms += Math.min(50, s.t1 - t) / fastAt(tl, t);
		}
	}
	return ms;
};

describe("3D key plays", () => {
	const { tl, events } = compile("keys", 140);
	const on = { ...KEY_PLAY_DEFAULTS, on: true };

	test("a reel in order: the tip, the plays, the finish, to about the length asked", () => {
		for (const minutes of [2, 5]) {
			const plan = planKeyPlays(
				tl,
				events,
				{ ...on, minutes },
				{
					rate: 1,
					numPeriods: 4,
				},
			);
			assert.strictEqual(plan[0]!.t0, 0);
			assert.strictEqual(plan.at(-1)!.t1, tl.end);
			plan.forEach((s, k) => {
				assert.isBelow(s.t0, s.t1);
				if (k > 0) {
					assert.isAbove(s.t0, plan[k - 1]!.t1);
					assert.isAtLeast(s.cursor, plan[k - 1]!.cursor);
				}
			});
			const ms = watchMs(tl, plan);
			assert.isBelow(ms, minutes * 60_000 * 1.15, `${minutes} min`);
			assert.isAbove(ms, minutes * 60_000 * 0.5, `${minutes} min`);
		}
	}, 60_000);

	test("a longer game shows more of it; faster, more again", () => {
		const n = (minutes: number, rate: number) =>
			watchMs(
				tl,
				planKeyPlays(tl, events, { ...on, minutes }, { rate, numPeriods: 4 }),
			);
		assert.isAbove(n(6, 1), n(2, 1));
		assert.isAbove(n(2, 4), n(2, 1));
	}, 60_000);

	test("a close finish is every play of it", () => {
		const plan = planKeyPlays(
			tl,
			events,
			{ ...on, minutes: 1, closePoints: 30, closeMinutes: 12 },
			{ rate: 1, numPeriods: 4 },
		);
		// Every line of the fourth quarter happens inside what is shown.
		let period = 1;
		events.forEach((e: any, i: number) => {
			if (e.type === "period") {
				period += 1;
			}
			const b = tl.beats.find((x) => x.i === i);
			if (period >= 4 && b) {
				assert.isDefined(segmentAt(plan, b.actionStart).at, `line ${i}`);
			}
		});
	}, 60_000);

	test("settings read back whatever was saved, within bounds", () => {
		const s = parseKeyPlays({
			on: true,
			minutes: 900,
			closePoints: -4,
			dunks: false,
		});
		assert.isTrue(s.on);
		assert.strictEqual(s.minutes, 30);
		assert.strictEqual(s.closePoints, 0);
		assert.isFalse(s.dunks);
		assert.isTrue(s.threes);
		assert.deepEqual(parseKeyPlays("junk"), KEY_PLAY_DEFAULTS);
	});
});
