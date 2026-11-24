import { assert, describe, test } from "vitest";
import { simRealGame } from "../worker/core/GameSim.basketball/realGameForTests.ts";
import { makeRecapReel, type RecapPlay } from "./recapReel.ts";
import { pickRecapPlays, RECAP_LENGTHS } from "./recapLength.ts";

const play = (i: number, w: number, period: number, more = {}): RecapPlay => ({
	i,
	from: i,
	to: i + 1,
	w,
	kind: "basket",
	pid: i,
	t: 0,
	period,
	clock: 100,
	...more,
});

describe("pickRecapPlays", () => {
	test("one play: the game-winner, else the MVP's best", () => {
		const plays = [
			play(1, 9, 1),
			play(2, 3, 2, { pid: 77 }),
			play(3, 5, 4, { winner: "won" }),
		];
		assert.deepEqual(
			pickRecapPlays(plays, 1, 77).map((p) => p.i),
			[3],
		);
		const noWinner = plays.slice(0, 2);
		assert.deepEqual(
			pickRecapPlays(noWinner, 1).map((p) => p.i),
			[1],
		);
		// The MVP's, unless another was twice as big.
		assert.deepEqual(
			pickRecapPlays(noWinner, 1, 77).map((p) => p.i),
			[1],
		);
		const close = [play(1, 9, 1), play(2, 5, 2, { pid: 77 })];
		assert.deepEqual(
			pickRecapPlays(close, 1, 77).map((p) => p.i),
			[2],
		);
	});

	test("one play: else, in a tight finish, the play that swung it most", () => {
		const plays = [
			play(1, 9, 1, { pid: 77, kind: "dunk", lev: 0.02 }),
			play(2, 8, 4, { kind: "three", lev: 0.22 }),
			play(3, 7, 4, { kind: "steal", lev: 0.25 }),
		];
		// (The steal swung it more, but not half as much again.)
		assert.deepEqual(
			pickRecapPlays(plays, 1, 77).map((p) => p.i),
			[2],
		);
		// Nothing swung it that much: the MVP's.
		const loose = plays.map((p) => ({ ...p, lev: (p.lev ?? 0) / 4 }));
		assert.deepEqual(
			pickRecapPlays(loose, 1, 77).map((p) => p.i),
			[1],
		);
	});

	test("mostly the offense: a steal only when it turned the game, no garbage time", () => {
		const plays = [
			play(1, 9, 1, { kind: "dunk" }),
			play(2, 8, 2, { kind: "steal", lev: 0.05 }),
			play(3, 8, 2, { kind: "block", lev: 0.03 }),
			play(4, 7, 3, { kind: "block", lev: 0.02 }),
			play(5, 6, 3, { kind: "three" }),
			play(6, 9, 4, { kind: "dunk", garbage: true }),
			play(7, 5, 4, { kind: "steal", lev: 0.2 }),
			play(8, 4, 1, { kind: "three" }),
		];
		const six = pickRecapPlays(plays, 6).map((p) => p.i);
		assert.notInclude(six, 2);
		assert.notInclude(six, 6);
		assert.include(six, 7);
		// (No more than one stop in six that did not turn it.)
		assert.isAtMost(six.filter((i) => i === 3 || i === 4).length, 1);
		for (const i of [1, 5, 8]) {
			assert.include(six, i);
		}
	});

	test("in order, through the whole game, shorter inside longer", async () => {
		for (const seed of [5, 9, 22]) {
			const { events } = await simRealGame(seed);
			const reel = makeRecapReel(events, { numPeriods: 4 });
			// (Every period with a play worth it outside garbage time.)
			const periods = new Set(
				reel.plays.filter((p) => !p.garbage && p.w >= 4).map((p) => p.period),
			);
			assert.isAtLeast(periods.size, 3, `seed ${seed}`);
			let longer: Set<number> | undefined;
			for (const { plays: n } of RECAP_LENGTHS) {
				const picked = pickRecapPlays(reel.plays, n);
				assert.strictEqual(picked.length, n);
				const is = picked.map((p) => p.i);
				assert.deepEqual(
					is,
					[...is].sort((a, b) => a - b),
				);
				if (longer) {
					for (const i of is) {
						assert.isTrue(longer.has(i), `seed ${seed}, ${n} plays`);
					}
				}
				longer = new Set(is);
				if (n >= 6) {
					const shown = new Set(picked.map((p) => p.period));
					for (const period of periods) {
						assert.isTrue(
							shown.has(period),
							`seed ${seed}: period ${period} in ${n} plays`,
						);
					}
				}
			}
		}
	}, 120_000);
});
