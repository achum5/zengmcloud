import { assert, describe, test } from "vitest";
import { FlowLog, clockLeft } from "./gameFlow.ts";

// A game told as a list of scores, and what the summary should say about it.
const play = (events: [0 | 1, number, number, number][], numPeriods = 4) => {
	const log = new FlowLog();
	for (const [side, pts, period, clock] of events) {
		log.addPoints(side, pts, period, clock, side * 100 + period);
	}
	return log.summary(numPeriods);
};

describe("FlowLog", () => {
	test("lead changes, ties and the biggest lead", () => {
		const f = play([
			[0, 2, 1, 700], // 2-0
			[1, 3, 1, 680], // 2-3 change
			[0, 1, 1, 660], // 3-3 tie
			[0, 2, 1, 640], // 5-3 change (through a tie)
			[1, 2, 1, 600], // 5-5 tie
			[0, 2, 1, 580], // 7-5 - same leader as before the tie, not a change
			[0, 3, 2, 500], // 10-5
		]);
		assert.strictEqual(f.leadChanges, 2);
		assert.strictEqual(f.ties, 2);
		assert.deepStrictEqual(f.maxLead, [5, 1]);
		assert.deepStrictEqual(f.lastTie, { period: 1, clock: 600, pts: 5 });
		// The lead last changed hands at 5-3; 7-5 kept it.
		assert.strictEqual(f.lastLead?.clock, 640);
		assert.deepStrictEqual(f.lastLead?.pts, [5, 3]);
	});

	test("a three that erases a two-point deficit is a lead change, not a tie", () => {
		const log = new FlowLog();
		log.addPoints(0, 2, 1, 700, 1);
		// The sim records a three as two points and then one, same instant.
		log.addPoints(1, 2, 1, 650, 7);
		log.addPoints(1, 1, 1, 650, 7);
		const f = log.summary(4);
		assert.strictEqual(f.ties, 0);
		assert.strictEqual(f.leadChanges, 1);
		assert.strictEqual(f.lastLead?.pid, 7);
	});

	test("the longest run and where it started", () => {
		const f = play([
			[0, 2, 1, 700],
			[1, 2, 1, 680],
			[0, 3, 3, 400],
			[0, 2, 3, 380],
			[0, 2, 3, 350],
			[0, 2, 3, 320], // 9 straight from 3:400
			[1, 2, 3, 300],
		]);
		assert.deepStrictEqual(f.run, { side: 0, pts: 9, period: 3, clock: 400 });
	});

	test("the score at five and two minutes left", () => {
		const f = play([
			[0, 2, 1, 700],
			[1, 2, 4, 400], // 2-2
			[0, 2, 4, 310], // 4-2 - the last score before 5:00
			[1, 3, 4, 200], // 4-5 - the last before 2:00
			[0, 2, 4, 50], // 6-5
		]);
		assert.deepStrictEqual(f.late, [
			{ clock: 300, pts: [4, 2] },
			{ clock: 120, pts: [4, 5] },
		]);
	});

	test("a quiet final period still has its marks, from the score entering it", () => {
		const f = play([
			[0, 2, 3, 100],
			[1, 1, 4, 30],
		]);
		assert.deepStrictEqual(f.late, [
			{ clock: 300, pts: [2, 0] },
			{ clock: 120, pts: [2, 0] },
		]);
	});

	test("an Elam ending has no clock and no marks", () => {
		const f = play([
			[0, 2, 1, 700],
			[1, 2, 4, Infinity],
			[0, 2, 4, Infinity],
		]);
		assert.isUndefined(f.late);
		assert.strictEqual(f.leadChanges, 0);
	});

	test("a game nobody caught", () => {
		const f = play([
			[0, 2, 1, 700],
			[0, 2, 2, 500],
			[1, 2, 4, 100],
		]);
		assert.strictEqual(f.leadChanges, 0);
		assert.strictEqual(f.ties, 0);
		assert.strictEqual(f.lastLead?.period, 1);
		assert.isUndefined(f.lastTie);
	});
});

describe("clockLeft", () => {
	test("minutes, seconds, and nothing", () => {
		assert.strictEqual(clockLeft(221), "3:41");
		assert.strictEqual(clockLeft(65), "1:05");
		assert.strictEqual(clockLeft(9.94), "9.9 seconds");
		assert.strictEqual(clockLeft(1), "1 second");
		assert.isUndefined(clockLeft(Infinity));
	});
});
