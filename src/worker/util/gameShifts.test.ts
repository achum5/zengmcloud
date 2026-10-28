import { assert, describe, test } from "vitest";
import {
	decodeOnFloor,
	decodeShifts,
	encodeOnFloor,
	encodeShifts,
} from "./gameShifts.ts";
import {
	ON_FLOOR_STATS,
	ShiftLog,
} from "../core/GameSim.basketball/shiftLog.ts";

const shift = (
	zero: number[],
	one: number[],
	poss: [number, number],
	pts: [number, number],
) => ({ lineups: [zero, one] as [number[], number[]], poss, pts });

describe("encodeShifts / decodeShifts", () => {
	test("a matchup survives the round trip", () => {
		const shifts = [
			shift([1, 2, 3, 4, 5], [11, 12, 13, 14, 15], [20, 21], [24, 19]),
			shift([1, 2, 3, 6, 7], [11, 12, 16, 17, 18], [8, 8], [10, 11]),
		];
		const encoded = encodeShifts(shifts, 5);
		assert.strictEqual(encoded.length, 2 * (2 * 5 + 4));
		assert.deepStrictEqual(
			decodeShifts({ shifts: encoded, numPlayersOnCourt: 5 }),
			shifts,
		);
	});

	test("a different lineup size round trips too", () => {
		const shifts = [shift([1, 2, 3], [7, 8, 9], [30, 31], [33, 30])];
		assert.deepStrictEqual(
			decodeShifts({
				shifts: encodeShifts(shifts, 3),
				numPlayersOnCourt: 3,
			}),
			shifts,
		);
	});

	// The stride is fixed, so a row that is short a man cannot be written
	// without throwing off everything after it.
	test("a shorthanded lineup is dropped rather than misaligned", () => {
		const shifts = [
			shift([1, 2, 3, 4], [11, 12, 13, 14, 15], [5, 5], [6, 4]),
			shift([1, 2, 3, 4, 5], [11, 12, 13, 14, 15], [7, 7], [9, 8]),
		];
		const decoded = decodeShifts({
			shifts: encodeShifts(shifts, 5),
			numPlayersOnCourt: 5,
		});
		assert.strictEqual(decoded.length, 1);
		assert.deepStrictEqual(decoded[0], shifts[1]);
	});

	test("nothing to decode", () => {
		assert.deepStrictEqual(decodeShifts({ numPlayersOnCourt: 5 }), []);
		assert.deepStrictEqual(
			decodeShifts({ shifts: [], numPlayersOnCourt: 5 }),
			[],
		);
		assert.deepStrictEqual(decodeShifts({ shifts: [1, 2, 3] }), []);
	});

	// Whatever damaged the row, the rest of it still reads.
	test("a truncated tail is ignored", () => {
		const encoded = encodeShifts(
			[shift([1, 2, 3, 4, 5], [11, 12, 13, 14, 15], [4, 4], [5, 6])],
			5,
		);
		const decoded = decodeShifts({
			shifts: [...encoded, 1, 2, 3],
			numPlayersOnCourt: 5,
		});
		assert.strictEqual(decoded.length, 1);
	});
});

describe("ShiftLog", () => {
	test("the same ten coming back are the same matchup", () => {
		const log = new ShiftLog();

		log.setLineups([1, 2, 3, 4, 5], [11, 12, 13, 14, 15]);
		log.addPossession(0);
		log.addPoints(0, 2);

		log.setLineups([1, 2, 3, 4, 6], [11, 12, 13, 14, 15]);
		log.addPossession(1);
		log.addPoints(1, 3);

		// The starters return, in a different order.
		log.setLineups([5, 4, 3, 2, 1], [15, 14, 13, 12, 11]);
		log.addPossession(0);
		log.addPoints(0, 3);

		const shifts = log.getShifts();
		assert.strictEqual(shifts.length, 2);
		assert.deepStrictEqual(shifts[0]!.lineups[0], [1, 2, 3, 4, 5]);
		assert.deepStrictEqual(shifts[0]!.poss, [2, 0]);
		assert.deepStrictEqual(shifts[0]!.pts, [5, 0]);
		assert.deepStrictEqual(shifts[1]!.poss, [0, 1]);
		assert.deepStrictEqual(shifts[1]!.pts, [0, 3]);
	});

	// A lineup that came and went inside one dead ball has nothing to say.
	test("a matchup that never had the ball is dropped", () => {
		const log = new ShiftLog();
		log.setLineups([1, 2, 3, 4, 5], [11, 12, 13, 14, 15]);
		log.setLineups([1, 2, 3, 4, 6], [11, 12, 13, 14, 15]);
		log.addPossession(0);
		assert.strictEqual(log.getShifts().length, 1);
	});

	test("nothing is recorded before anybody is on the floor", () => {
		const log = new ShiftLog();
		log.addPossession(0);
		log.addPoints(0, 2);
		assert.deepStrictEqual(log.getShifts(), []);
	});
});

describe("on the floor", () => {
	const K = ON_FLOOR_STATS.length;
	const at = (stat: string) => ON_FLOOR_STATS.indexOf(stat as any);

	test("a stat goes to the five who were out there, as theirs and as the other side's", () => {
		const log = new ShiftLog();
		log.setLineups([1, 2, 3, 4, 5], [11, 12, 13, 14, 15]);
		log.addStat(0, "fga", 1);
		log.addStat(1, "drb", 1);
		log.setLineups([1, 2, 3, 4, 6], [11, 12, 13, 14, 15]);
		log.addStat(0, "tp", 1);
		// Not one of them.
		log.addStat(0, "pf", 1);

		const byPid = new Map(log.getOnFloor());
		assert.strictEqual(byPid.get(1)![at("fga")], 1);
		assert.strictEqual(byPid.get(1)![K + at("drb")], 1);
		assert.strictEqual(byPid.get(1)![at("tp")], 1);
		// On the bench for the three.
		assert.strictEqual(byPid.get(5)![at("tp")], 0);
		assert.strictEqual(byPid.get(6)![at("tp")], 1);
		assert.strictEqual(byPid.get(6)![at("fga")], 0);
		// The other side saw it as theirs against them.
		assert.strictEqual(byPid.get(11)![K + at("fga")], 1);
		assert.strictEqual(byPid.get(11)![at("drb")], 1);
		assert.strictEqual(byPid.get(11)![K + at("tp")], 1);
		assert.strictEqual(
			byPid.get(1)!.reduce((a, b) => a + b, 0),
			3,
		);
	});

	test("nothing is credited before anybody is on the floor", () => {
		const log = new ShiftLog();
		log.addStat(0, "fga", 1);
		assert.deepStrictEqual(log.getOnFloor(), []);
	});

	test("the boxes survive the round trip", () => {
		const log = new ShiftLog();
		log.setLineups([1, 2, 3, 4, 5], [11, 12, 13, 14, 15]);
		log.addStat(0, "fg", 1);
		log.addStat(0, "fga", 2);
		log.addStat(1, "stl", 1);
		log.addStat(0, "tov", 1);
		const decoded = decodeOnFloor(encodeOnFloor(log.getOnFloor()));
		assert.strictEqual(decoded.size, 10);
		const [us, them] = decoded.get(3)!;
		assert.strictEqual(us.fg, 1);
		assert.strictEqual(us.fga, 2);
		assert.strictEqual(us.tov, 1);
		assert.strictEqual(them.stl, 1);
		assert.strictEqual(them.fga, 0);
		assert.strictEqual(decoded.get(12)![0].stl, 1);
	});

	// A list of a different length is some other version's; not misread.
	test("a run kept with another stat list reads as nothing", () => {
		assert.strictEqual(decodeOnFloor([K + 1, 1, 2, 3]).size, 0);
		assert.strictEqual(decodeOnFloor(undefined).size, 0);
		assert.strictEqual(decodeOnFloor([]).size, 0);
	});
});
