import { assert, describe, test } from "vitest";
import {
	decodeGridCode,
	encodeGridCode,
	type GridCodeRef,
} from "./triviaGridCode.ts";

const rows: GridCodeRef[] = [
	{ kind: "team", tid: 3 },
	{ kind: "career", id: "isHallOfFamer" },
	{ kind: "decade", mode: "played", decade: 2020 },
];
const cols: GridCodeRef[] = [
	{ kind: "season", id: "AllStar" },
	{ kind: "stat", spec: "career-pts", op: "gte", value: 20_000 },
	{ kind: "stat", spec: "season-ppg", op: "lte", value: 8.5 },
];

describe("grid codes", () => {
	test("a grid survives a round trip exactly", () => {
		const decoded = decodeGridCode(encodeGridCode(rows, cols));
		assert.deepStrictEqual(decoded, { rows, cols });
	});

	test("the code is short enough to paste in a chat message", () => {
		assert.isBelow(encodeGridCode(rows, cols).length, 120);
	});

	// It has to survive being copied out of a chat app, which is where these
	// actually travel.
	test("surrounding whitespace and quotes are tolerated", () => {
		const code = encodeGridCode(rows, cols);
		assert.deepStrictEqual(decodeGridCode(`  "${code}" \n`), { rows, cols });
	});

	test("url-unsafe base64 characters never appear", () => {
		// A tid range wide enough to shake out every base64 alignment.
		for (let tid = 0; tid < 40; tid++) {
			const code = encodeGridCode(
				[{ kind: "team", tid }, ...rows.slice(1)],
				cols,
			);
			assert.notMatch(code, /[+/=]/, `tid ${tid} produced ${code}`);
		}
	});

	test("two different grids never encode the same", () => {
		const a = encodeGridCode(rows, cols);
		const b = encodeGridCode(rows, [
			{ kind: "season", id: "MVP" },
			...cols.slice(1),
		]);
		assert.notStrictEqual(a, b);
	});

	test("op and mode round trip both ways", () => {
		const refs: GridCodeRef[] = [
			{ kind: "stat", spec: "career-pts", op: "lte", value: 1 },
			{ kind: "decade", mode: "debut", decade: 1990 },
			{ kind: "team", tid: 0 },
		];
		assert.deepStrictEqual(
			decodeGridCode(encodeGridCode(refs, refs))?.rows,
			refs,
		);
	});

	test("fractional thresholds are not rounded away", () => {
		const decoded = decodeGridCode(encodeGridCode(rows, cols));
		assert.strictEqual((decoded!.cols[2] as any).value, 8.5);
	});

	describe("garbage in", () => {
		const bad = [
			"",
			"   ",
			"not a code",
			"!!!!",
			// Valid base64, wrong contents.
			btoa("1|t3|t4"),
			btoa("2|t3|t4|t5|t6|t7|t8"),
			btoa("1|t3|t4|t5|t6|t7|zzz"),
		];
		for (const code of bad) {
			test(`rejects ${JSON.stringify(code)}`, () => {
				assert.strictEqual(decodeGridCode(code), undefined);
			});
		}
	});
});
