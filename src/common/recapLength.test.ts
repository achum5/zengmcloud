import { assert, describe, test } from "vitest";
import { simRealGame } from "../worker/core/GameSim.basketball/realGameForTests.ts";
import { makeRecapReel, type RecapPlay } from "./recapReel.ts";
import {
	pickRecapPlays,
	pickTopPlays,
	RECAP_LENGTHS,
	starPlays,
} from "./recapLength.ts";

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

describe("pickTopPlays", () => {
	// Game 1: a game-winning three and five dunks by one man. Game 2: a
	// layup that swung a tight finish, a poster, a block, a steal that turned
	// nothing. Game 3: a garbage-time dunk and a three.
	const games = [
		{
			gid: 1,
			plays: [
				play(1, 30, 4, { kind: "three", pid: 10, lev: 0.4, winner: "won" }),
				...[2, 3, 4, 5, 6].map((i) =>
					play(i, 6 + i / 10, 2, { kind: "dunk", pid: 11 }),
				),
			],
		},
		{
			gid: 2,
			plays: [
				play(1, 19, 4, { kind: "rim", pid: 20, lev: 0.3 }),
				play(2, 7.5, 1, { kind: "dunk", pid: 21 }),
				play(3, 4, 3, { kind: "block", pid: 22 }),
				play(4, 5, 3, { kind: "steal", pid: 23, lev: 0.02 }),
			],
		},
		{
			gid: 3,
			plays: [
				play(1, 1.5, 4, { kind: "dunk", pid: 30, garbage: true }),
				play(2, 4, 1, { kind: "three", pid: 31 }),
			],
		},
	];

	test("the game-winner first, then the plays that look best", () => {
		const top = pickTopPlays(games, 10);
		const at = (gid: number, i: number) =>
			top.findIndex((x) => x.gid === gid && x.play.i === i);
		assert.strictEqual(at(1, 1), 0);
		// The poster over the tight finish's layup: a swing counts for little.
		assert.isBelow(at(2, 2), at(2, 1));
		// No steal that turned nothing, no garbage time.
		assert.strictEqual(at(2, 4), -1);
		assert.strictEqual(at(3, 1), -1);
		// No game more than three, no man more than two.
		for (const gid of [1, 2, 3]) {
			assert.isAtMost(top.filter((x) => x.gid === gid).length, 3);
		}
		const byMan = new Map<number, number>();
		for (const x of top) {
			byMan.set(x.play.pid, (byMan.get(x.play.pid) ?? 0) + 1);
		}
		assert.isAtMost(Math.max(...byMan.values()), 2);
	});

	test("a shorter list is the top of the longer", () => {
		const ten = pickTopPlays(games, 10).map((x) => `${x.gid}:${x.play.i}`);
		for (const n of [5, 3, 1]) {
			assert.deepEqual(
				pickTopPlays(games, n).map((x) => `${x.gid}:${x.play.i}`),
				ten.slice(0, n),
			);
		}
	});
});

describe("starPlays", () => {
	test("his own, in order, a shorter list inside the longer", () => {
		const plays = [
			play(1, 9, 1, { pid: 5, kind: "dunk" }),
			play(2, 12, 1, { pid: 6 }),
			play(3, 4, 2, { pid: 5, kind: "three" }),
			play(4, 8, 3, { pid: 5 }),
			play(5, 6, 4, { pid: 5, kind: "three" }),
			play(6, 3, 4, { pid: 5, kind: "steal" }),
		];
		const six = starPlays(plays, 5, 6).map((p) => p.i);
		assert.deepEqual(six, [1, 3, 4, 5]);
		const three = starPlays(plays, 5, 3).map((p) => p.i);
		assert.strictEqual(three.length, 3);
		for (const i of three) {
			assert.include(six, i);
		}
		assert.deepEqual(
			three,
			[...three].sort((a, b) => a - b),
		);
	});
});
