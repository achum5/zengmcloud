import { assert, describe, test } from "vitest";
import {
	findStarters,
	getRosterOrderByPid,
} from "./rosterAutoSort.basketball.ts";

describe("findStarters", () => {
	test("handle easy roster sorts", () => {
		let starters = findStarters([
			"PG",
			"SG",
			"SF",
			"PF",
			"C",
			"G",
			"F",
			"FC",
			"PF",
			"PG",
		]);
		assert.deepStrictEqual(starters, [0, 1, 2, 3, 4]);
		starters = findStarters([
			"PG",
			"SG",
			"G",
			"PF",
			"C",
			"G",
			"F",
			"FC",
			"PF",
			"PG",
		]);
		assert.deepStrictEqual(starters, [0, 1, 2, 3, 4]);
		starters = findStarters([
			"F",
			"SG",
			"SF",
			"PG",
			"C",
			"G",
			"F",
			"FC",
			"PF",
			"PG",
		]);
		assert.deepStrictEqual(starters, [0, 1, 2, 3, 4]);
		starters = findStarters([
			"F",
			"SG",
			"SF",
			"PF",
			"G",
			"G",
			"F",
			"FC",
			"PF",
			"PG",
		]);
		assert.deepStrictEqual(starters, [0, 1, 2, 3, 4]);
	});

	test("put two Gs in starting lineup", () => {
		let starters = findStarters([
			"PG",
			"F",
			"SF",
			"PF",
			"C",
			"G",
			"F",
			"FC",
			"PF",
			"PG",
		]);
		assert.deepStrictEqual(starters, [0, 1, 2, 3, 5]);
		starters = findStarters([
			"F",
			"PF",
			"G",
			"PF",
			"C",
			"G",
			"F",
			"FC",
			"PF",
			"PG",
		]);
		assert.deepStrictEqual(starters, [0, 1, 2, 3, 5]);
		starters = findStarters([
			"F",
			"PF",
			"SF",
			"GF",
			"C",
			"F",
			"FC",
			"PF",
			"PG",
		]);
		assert.deepStrictEqual(starters, [0, 1, 2, 3, 8]);
		starters = findStarters([
			"F",
			"PF",
			"SF",
			"C",
			"C",
			"F",
			"FC",
			"PF",
			"PG",
			"G",
		]);
		assert.deepStrictEqual(starters, [0, 1, 2, 8, 9]);
	});

	test("put two Fs (or one F and one C) in starting lineup", () => {
		let starters = findStarters([
			"PG",
			"SG",
			"G",
			"PF",
			"G",
			"G",
			"F",
			"FC",
			"PF",
			"PG",
		]);
		assert.deepStrictEqual(starters, [0, 1, 2, 3, 6]);
		starters = findStarters([
			"PG",
			"SG",
			"SG",
			"PG",
			"G",
			"G",
			"F",
			"FC",
			"PF",
			"PG",
		]);
		assert.deepStrictEqual(starters, [0, 1, 2, 6, 7]);
		starters = findStarters([
			"PG",
			"SG",
			"SG",
			"PG",
			"C",
			"G",
			"F",
			"FC",
			"PF",
			"PG",
		]);
		assert.deepStrictEqual(starters, [0, 1, 2, 4, 6]);
	});

	test("never put two pure Cs in starting lineup", () => {
		let starters = findStarters([
			"PG",
			"SG",
			"G",
			"C",
			"C",
			"G",
			"F",
			"FC",
			"PF",
			"PG",
		]);
		assert.deepStrictEqual(starters, [0, 1, 2, 3, 6]);
		starters = findStarters([
			"PG",
			"SG",
			"G",
			"C",
			"FC",
			"G",
			"F",
			"FC",
			"PF",
			"PG",
		]);
		assert.deepStrictEqual(starters, [0, 1, 2, 3, 4]);
	});
});

describe("a tanking team plays its youth", () => {
	const roster = [
		// The veteran holding the fort: best today, no upside.
		{
			pid: 1,
			value: 52,
			valueNoPot: 56,
			valueNoPotFuzz: 56,
			ratings: { pos: "G" },
		},
		// The prospect: worse today, the future.
		{
			pid: 2,
			value: 60,
			valueNoPot: 48,
			valueNoPotFuzz: 48,
			ratings: { pos: "F" },
		},
		{
			pid: 3,
			value: 50,
			valueNoPot: 50,
			valueNoPotFuzz: 50,
			ratings: { pos: "C" },
		},
	];

	test("by default the best player today starts", () => {
		const order = getRosterOrderByPid(
			roster.map((p) => ({ ...p })),
			5,
			false,
		);
		assert.isBelow(order.get(1)!, order.get(2)!);
	});

	test("in a teardown the prospect starts over him", () => {
		const order = getRosterOrderByPid(
			roster.map((p) => ({ ...p })),
			5,
			false,
			true,
		);
		assert.isBelow(order.get(2)!, order.get(1)!);
		// The order is still a full roster order, every man placed once.
		assert.deepStrictEqual(
			[...order.values()].sort((a, b) => a - b),
			[0, 1, 2],
		);
	});

	test("without a value to go on, youth falls back to today's value", () => {
		const order = getRosterOrderByPid(
			roster.map(({ value, ...p }) => ({ ...p })),
			5,
			false,
			true,
		);
		assert.isBelow(order.get(1)!, order.get(2)!);
	});
});
