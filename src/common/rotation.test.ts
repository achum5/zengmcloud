import { assert, describe, test } from "vitest";
import {
	gridToRotation,
	lineupAt,
	plannedMinutes,
	playersPerMinute,
	rotationToGrid,
	sanitizeRotation,
	type RotationStint,
} from "./rotation.ts";

const stint = (
	pid: number,
	period: number,
	start: number,
	end: number,
): RotationStint => ({ pid, period, start, end });

describe("lineupAt", () => {
	const stints = [
		stint(1, 0, 0, 0.5),
		stint(2, 0, 0.5, 1),
		stint(3, 0, 0, 1),
		stint(1, 1, 0, 1),
	];

	test("who is planned at a moment", () => {
		assert.deepStrictEqual([...lineupAt(stints, 0, 0.25)].sort(), [1, 3]);
		assert.deepStrictEqual([...lineupAt(stints, 0, 0.75)].sort(), [2, 3]);
		assert.deepStrictEqual([...lineupAt(stints, 1, 0.1)], [1]);
	});

	// A swap planned for the six minute mark happens AT the six minute mark.
	test("a boundary belongs to the stint that starts there", () => {
		assert.deepStrictEqual([...lineupAt(stints, 0, 0.5)].sort(), [2, 3]);
		assert.deepStrictEqual([...lineupAt(stints, 0, 0)].sort(), [1, 3]);
	});

	test("overtime has no plan", () => {
		assert.strictEqual(lineupAt(stints, 4, 0.5).size, 0);
	});
});

describe("plannedMinutes", () => {
	test("adds a player's stints up in minutes", () => {
		const stints = [stint(1, 0, 0, 0.5), stint(1, 2, 0.25, 1)];
		assert.closeTo(plannedMinutes(stints, 1, 12), 6 + 9, 1e-9);
		assert.strictEqual(plannedMinutes(stints, 2, 12), 0);
	});
});

describe("sanitizeRotation", () => {
	const roster = new Set([1, 2, 3]);

	test("nothing in, nothing out", () => {
		assert.isUndefined(sanitizeRotation(undefined, roster, 4));
	});

	test("a player who left the team is dropped", () => {
		const out = sanitizeRotation(
			{ auto: false, stints: [stint(1, 0, 0, 1), stint(9, 0, 0, 1)] },
			roster,
			4,
		)!;
		assert.deepStrictEqual(
			out.stints.map((s) => s.pid),
			[1],
		);
	});

	test("periods the league does not play are dropped", () => {
		const out = sanitizeRotation(
			{ auto: false, stints: [stint(1, 3, 0, 1), stint(1, 4, 0, 1)] },
			roster,
			4,
		)!;
		assert.strictEqual(out.stints.length, 1);
	});

	test("empty, inverted and out-of-range stints are dropped or clipped", () => {
		const out = sanitizeRotation(
			{
				auto: false,
				stints: [
					stint(1, 0, 0.5, 0.5),
					stint(1, 0, 0.8, 0.2),
					stint(2, 0, -1, 2),
				],
			},
			roster,
			4,
		)!;
		assert.deepStrictEqual(out.stints, [stint(2, 0, 0, 1)]);
	});

	// A man in two overlapping stints is still one man.
	test("a player's overlapping stints merge", () => {
		const out = sanitizeRotation(
			{
				auto: false,
				stints: [
					stint(1, 0, 0, 0.5),
					stint(1, 0, 0.25, 0.75),
					stint(1, 0, 0.75, 1),
				],
			},
			roster,
			4,
		)!;
		assert.deepStrictEqual(out.stints, [stint(1, 0, 0, 1)]);
	});

	test("auto defaults to on when unspecified", () => {
		const out = sanitizeRotation({ stints: [] } as any, roster, 4)!;
		assert.strictEqual(out.auto, true);
	});
});

describe("grid round trip", () => {
	test("stints become minutes and back", () => {
		const stints = [
			stint(1, 0, 0, 0.5),
			stint(1, 0, 0.75, 1),
			stint(2, 1, 1 / 12, 11 / 12),
		];
		const grid = rotationToGrid(stints, [1, 2], 2, 12);
		assert.deepStrictEqual(grid.get(1)![0], [
			true,
			true,
			true,
			true,
			true,
			true,
			false,
			false,
			false,
			true,
			true,
			true,
		]);
		assert.deepStrictEqual(gridToRotation(grid, 12), stints);
	});

	// A plan drawn for twelve minute quarters still reads under ten.
	test("a different period length keeps the shape", () => {
		const grid = rotationToGrid([stint(1, 0, 0, 0.5)], [1], 1, 10);
		assert.deepStrictEqual(grid.get(1)![0], [
			true,
			true,
			true,
			true,
			true,
			false,
			false,
			false,
			false,
			false,
		]);
	});

	test("players per minute", () => {
		const grid = rotationToGrid(
			[stint(1, 0, 0, 1), stint(2, 0, 0, 0.5)],
			[1, 2],
			1,
			4,
		);
		assert.deepStrictEqual(playersPerMinute(grid, 1, 4), [[2, 2, 1, 1]]);
	});
});
