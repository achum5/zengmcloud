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
			pickRecapPlays(noWinner, 1, 77).map((p) => p.i),
			[2],
		);
		assert.deepEqual(
			pickRecapPlays(noWinner, 1).map((p) => p.i),
			[1],
		);
	});

	test("in order, through the whole game, shorter inside longer", async () => {
		for (const seed of [5, 9, 22]) {
			const { events } = await simRealGame(seed);
			const reel = makeRecapReel(events, { numPeriods: 4 });
			const periods = new Set(reel.plays.map((p) => p.period));
			assert.isAtLeast(periods.size, 4, `seed ${seed}`);
			let longer: Set<number> | undefined;
			for (const { plays: n } of RECAP_LENGTHS) {
				const picked = pickRecapPlays(reel.plays, n);
				assert.strictEqual(picked.length, Math.min(n, reel.plays.length));
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
					assert.strictEqual(
						new Set(picked.map((p) => p.period)).size,
						periods.size,
						`seed ${seed}: every period in ${n} plays`,
					);
				}
			}
		}
	}, 120_000);
});
