import { assert, describe, test } from "vitest";
import {
	gradeAgainst,
	gradeTeamRatings,
	summarizeTeamRatings,
} from "./teamRatingGrade.ts";

describe("gradeTeamRatings", () => {
	test("orders grades with the league - best team never below worst", () => {
		const values = Array.from({ length: 30 }, (_, i) => 40 + i);
		const grades = gradeTeamRatings(values);
		assert.strictEqual(grades[29], "A");
		assert.strictEqual(grades[0], "F");
		// Monotonic: a higher rating can never earn a worse grade.
		const rank = { A: 4, B: 3, C: 2, D: 1, F: 0 } as const;
		for (let i = 1; i < grades.length; i++) {
			assert.ok(
				rank[grades[i]!] >= rank[grades[i - 1]!],
				`${values[i]} graded worse than ${values[i - 1]}`,
			);
		}
	});

	test("uses all five grades across a normal-looking league", () => {
		// Roughly bell-shaped, which is what a real league's category ratings look
		// like. Every grade should appear - a curve that only ever emits C is
		// useless.
		const values = [
			38, 42, 44, 45, 46, 47, 48, 48, 49, 50, 50, 50, 51, 51, 52, 52, 52, 53,
			53, 54, 54, 55, 56, 57, 58, 59, 61, 63, 66, 71,
		];
		const grades = new Set(gradeTeamRatings(values));
		for (const grade of ["A", "B", "C", "D", "F"]) {
			assert.ok(grades.has(grade as any), `no ${grade} in ${[...grades]}`);
		}
	});

	test("a league where everyone is identical is all Cs", () => {
		// Not all As and not all Fs - nobody is better than anybody.
		assert.deepStrictEqual(gradeTeamRatings([50, 50, 50, 50]), [
			"C",
			"C",
			"C",
			"C",
		]);
	});

	test("real category spread is unaffected by the noise floor", () => {
		// A normal league's spread is far above the floor, so the floor must not
		// quietly flatten a legitimate curve.
		// Mean 51.5, spread ~6.3, so 48 lands at -0.56 sigma (a D) and 54 at +0.4
		// (a B) - the curve, not the floor, is doing the work.
		const values = [42, 46, 48, 50, 52, 54, 58, 62];
		assert.deepStrictEqual(gradeTeamRatings(values), [
			"F",
			"D",
			"D",
			"C",
			"C",
			"B",
			"A",
			"A",
		]);
	});

	test("a bunched league doesn't manufacture separation it doesn't have", () => {
		// Fixed quintiles would hand out six As here. The curve reflects that these
		// teams are nearly the same, which is the honest read.
		const values = Array.from({ length: 30 }, (_, i) => 50 + (i % 3) * 0.1);
		const grades = gradeTeamRatings(values);
		assert.strictEqual(grades.filter((g) => g === "A").length, 0);
		assert.strictEqual(grades.filter((g) => g === "F").length, 0);
	});

	test("one runaway team gets the only A", () => {
		const values = [90, ...Array.from({ length: 29 }, () => 50)];
		const grades = gradeTeamRatings(values);
		assert.strictEqual(grades[0], "A");
		assert.strictEqual(grades.filter((g) => g === "A").length, 1);
	});

	test("an empty league grades nothing", () => {
		assert.deepStrictEqual(gradeTeamRatings([]), []);
	});

	test("a single team is a C, not an A", () => {
		assert.deepStrictEqual(gradeTeamRatings([73]), ["C"]);
	});
});

describe("gradeAgainst", () => {
	test("grades one value against a pre-computed league", () => {
		// The table summarizes once and grades row by row, so this has to agree
		// with grading the whole list at once.
		const values = [40, 45, 50, 55, 60, 65, 70];
		const summary = summarizeTeamRatings(values);
		assert.deepStrictEqual(
			values.map((v) => gradeAgainst(v, summary)),
			gradeTeamRatings(values),
		);
	});

	test("a degenerate league is a C rather than a crash", () => {
		assert.strictEqual(gradeAgainst(50, { mean: 50, stdDev: 0 }), "C");
	});
});
