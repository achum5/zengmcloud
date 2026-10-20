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

	test("a basket is seen from the play that gets it up until it is down; no cut with the ball in the air", () => {
		const plan = planKeyPlays(
			tl,
			events,
			{ ...on, minutes: 4 },
			{ rate: 1, numPeriods: 4 },
		);
		const ballAt = (t: number) => tl.ball.findLast((s) => s.t0 <= t);
		const up = (t: number) => {
			const s = ballAt(t);
			return (
				s?.kind === "path" ||
				s?.kind === "bounce" ||
				(s?.kind === "fly" && !("pid" in s.to))
			);
		};
		let baskets = 0;
		for (const s of plan.slice(0, -1)) {
			// (Settled by the cut - or as good as: only a ball that went on
			// bouncing after a basket, long after it went in.)
			const k = tl.beats.findLastIndex((b) => b.actionStart < s.t1);
			const made =
				/^(fg|tp)/.test(tl.beats[k]?.type ?? "") &&
				!/^fga/.test(tl.beats[k]?.type ?? "");
			if (up(s.t1 - 1)) {
				assert.strictEqual(ballAt(s.t1 - 1)?.kind, "bounce", `cut at ${s.t1}`);
				assert.isTrue(made, `cut at ${s.t1}`);
			}
			// Every basket in a clip: its attempt, the shot up, in it too.
			for (const [j, b] of tl.beats.entries()) {
				if (
					b.actionStart >= s.t0 &&
					b.actionStart < s.t1 &&
					/^(fg|tp)/.test(b.type) &&
					!/^fga/.test(b.type) &&
					/^fga/.test(tl.beats[j - 1]?.type ?? "")
				) {
					baskets += 1;
					assert.isAtMost(s.t0, tl.beats[j - 1]!.actionStart);
				}
			}
		}
		assert.isAbove(baskets, 5);
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

	test("a close finish on top of the length: the rest of the game still gets all of it", () => {
		const minutes = 2;
		const rest = (closeOnTop: boolean, closePoints: number) => {
			const plan = planKeyPlays(
				tl,
				events,
				{ ...on, minutes, closePoints, closeMinutes: 12, closeOnTop },
				{ rate: 1, numPeriods: 4 },
			);
			return {
				rest: watchMs(
					tl,
					plan.filter((s) => !s.close),
				),
				close: watchMs(
					tl,
					plan.filter((s) => s.close),
				),
			};
		};
		const none = rest(true, 0);
		const onTop = rest(true, 30);
		const outOf = rest(false, 30);
		assert.isAbove(onTop.close, 0);
		// Out of the length, the finish leaves the rest of the game little;
		// on top of it, the rest gets what it gets with no close finish.
		assert.isBelow(outOf.rest, none.rest * 0.5);
		assert.isAbove(onTop.rest, none.rest * 0.8);
		assert.isAbove(onTop.rest, minutes * 60_000 * 0.6);
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
		assert.isFalse(s.closeOnTop);
		assert.isTrue(parseKeyPlays({ closeOnTop: true }).closeOnTop);
		assert.deepEqual(parseKeyPlays("junk"), KEY_PLAY_DEFAULTS);
	});
});
