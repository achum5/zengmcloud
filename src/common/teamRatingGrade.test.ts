import { assert, describe, test } from "vitest";
import { gradeFromRank } from "./teamRatingGrade.ts";

describe("gradeFromRank", () => {
	test("a 30-team league splits into even sixes", () => {
		const grades = Array.from({ length: 30 }, (_, i) =>
			gradeFromRank(i + 1, 30),
		);
		assert.deepStrictEqual(grades, [
			...Array.from({ length: 6 }, () => "A"),
			...Array.from({ length: 6 }, () => "B"),
			...Array.from({ length: 6 }, () => "C"),
			...Array.from({ length: 6 }, () => "D"),
			...Array.from({ length: 6 }, () => "F"),
		]);
	});

	test("the best team is an A and the worst is an F", () => {
		// The bug this replaced had it exactly backwards: the input is a RANK, so
		// 1 is the best team, and treating it as a rating gave the best teams Fs.
		assert.strictEqual(gradeFromRank(1, 30), "A");
		assert.strictEqual(gradeFromRank(30, 30), "F");
	});

	test("grades never improve as the rank gets worse", () => {
		const order = { A: 0, B: 1, C: 2, D: 3, F: 4 } as const;
		for (const numTeams of [4, 12, 22, 30, 41]) {
			let prev = -1;
			for (let rank = 1; rank <= numTeams; rank++) {
				const value = order[gradeFromRank(rank, numTeams)];
				assert.ok(
					value >= prev,
					`rank ${rank} of ${numTeams} improved on the rank before it`,
				);
				prev = value;
			}
		}
	});

	test("every grade appears in any league with at least five teams", () => {
		for (const numTeams of [5, 8, 16, 29, 30, 32]) {
			const grades = new Set(
				Array.from({ length: numTeams }, (_, i) =>
					gradeFromRank(i + 1, numTeams),
				),
			);
			assert.strictEqual(
				grades.size,
				5,
				`${numTeams} teams produced ${[...grades]}`,
			);
		}
	});

	test("the best and worst always sit at the ends, at any league size", () => {
		for (const numTeams of [2, 3, 5, 12, 23, 30, 41]) {
			assert.strictEqual(gradeFromRank(1, numTeams), "A", `${numTeams} teams`);
			assert.strictEqual(
				gradeFromRank(numTeams, numTeams),
				"F",
				`${numTeams} teams`,
			);
		}
	});

	test("a league with nothing to rank against doesn't crash", () => {
		assert.strictEqual(gradeFromRank(1, 1), "C");
		assert.strictEqual(gradeFromRank(1, 0), "C");
	});

	test("a missing rank grades as average rather than as the worst", () => {
		// The old curve returned F for anything it couldn't compute, which is how
		// a whole table of Fs got shipped.
		assert.strictEqual(gradeFromRank(Number.NaN, 30), "C");
		assert.strictEqual(gradeFromRank(undefined as any, 30), "C");
	});

	test("an odd league size still puts the top team in A and the last in F", () => {
		assert.strictEqual(gradeFromRank(1, 23), "A");
		assert.strictEqual(gradeFromRank(23, 23), "F");
	});
});
