import { assert, describe, test } from "vitest";
import {
	computeStrengthOfSchedule,
	shrunkWinp,
} from "./getStrengthOfSchedule.ts";

const strengths = (entries: [number, number][]) => new Map(entries);

describe("shrunkWinp", () => {
	// Twelve games in, a 3-0 team is not a 1.000 opponent. Without this the
	// column swings wildly in October, which is exactly when people look at it.
	test("early records are pulled toward .500", () => {
		assert.ok(shrunkWinp(1, 3) < 0.7, `${shrunkWinp(1, 3)}`);
		assert.ok(shrunkWinp(0, 3) > 0.3);
		assert.strictEqual(shrunkWinp(0.5, 3), 0.5);
	});

	// And the prior has to get out of the way, or by March it would be reporting
	// its own assumption rather than the league.
	test("it fades as the season fills in", () => {
		assert.ok(Math.abs(shrunkWinp(0.75, 70) - 0.75) < 0.04);
		assert.ok(Math.abs(shrunkWinp(0.75, 3) - 0.75) > 0.15);
	});

	test("nobody has played yet, so everyone is average", () => {
		assert.strictEqual(shrunkWinp(0, 0), 0.5);
	});
});

describe("computeStrengthOfSchedule", () => {
	test("it averages the opponents still to play", () => {
		const out = computeStrengthOfSchedule(
			[
				{ homeTid: 0, awayTid: 1 },
				{ homeTid: 0, awayTid: 2 },
			],
			strengths([
				[0, 0.5],
				[1, 0.8],
				[2, 0.2],
			]),
		);
		assert.strictEqual(out.get(0)?.sos, 0.5);
		assert.strictEqual(out.get(0)?.gamesRemaining, 2);
	});

	// The whole point of the column: two teams with the same number of games
	// left can have very different roads, and playing the best team three more
	// times has to weigh three times as much as playing them once.
	test("a repeat opponent counts every time", () => {
		const out = computeStrengthOfSchedule(
			[
				{ homeTid: 0, awayTid: 1 },
				{ homeTid: 1, awayTid: 0 },
				{ homeTid: 0, awayTid: 1 },
				{ homeTid: 0, awayTid: 2 },
			],
			strengths([
				[1, 0.9],
				[2, 0.1],
			]),
		);
		assert.strictEqual(out.get(0)?.gamesRemaining, 4);
		assert.ok(Math.abs(out.get(0)!.sos - 0.7) < 1e-9);
	});

	test("both sides of a game get the other team", () => {
		const out = computeStrengthOfSchedule(
			[{ homeTid: 0, awayTid: 1 }],
			strengths([
				[0, 0.25],
				[1, 0.75],
			]),
		);
		assert.strictEqual(out.get(0)?.sos, 0.75);
		assert.strictEqual(out.get(1)?.sos, 0.25);
	});

	// The All-Star game and the trade deadline sit in the schedule as sentinels
	// with negative tids. Counting them would put a phantom opponent in
	// everyone's average.
	test("sentinels are not opponents", () => {
		const out = computeStrengthOfSchedule(
			[
				{ homeTid: -1, awayTid: -2 },
				{ homeTid: -3, awayTid: -3 },
				{ homeTid: 0, awayTid: 1 },
			],
			strengths([
				[0, 0.4],
				[1, 0.6],
			]),
		);
		assert.strictEqual(out.get(0)?.gamesRemaining, 1);
		assert.strictEqual(out.get(0)?.sos, 0.6);
		assert.strictEqual(out.size, 2);
	});

	// A finished season has nothing left to play, and the column has nothing to
	// say rather than something wrong.
	test("no games left, no number", () => {
		const out = computeStrengthOfSchedule([], strengths([[0, 0.5]]));
		assert.strictEqual(out.size, 0);
	});

	test("an unknown opponent is skipped rather than counted as average", () => {
		const out = computeStrengthOfSchedule(
			[
				{ homeTid: 0, awayTid: 1 },
				{ homeTid: 0, awayTid: 99 },
			],
			strengths([[1, 0.8]]),
		);
		assert.strictEqual(out.get(0)?.gamesRemaining, 1);
		assert.strictEqual(out.get(0)?.sos, 0.8);
	});
});
