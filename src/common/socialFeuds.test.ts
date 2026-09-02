import { assert, describe, test } from "vitest";
import { feudHeat, NO_RIVALRY, rivalryFrom } from "./socialFeuds.ts";

const heat = (
	overrides: Partial<Parameters<typeof feudHeat>[0]> = {},
): number =>
	feudHeat({
		firstTid: 0,
		secondTid: 1,
		firstOptimism: 0,
		secondOptimism: 0,
		rivalry: NO_RIVALRY,
		...overrides,
	});

describe("feudHeat", () => {
	test("two accounts with no side have nothing to fight about", () => {
		// Whatever friction they have is temperament, which replyAppetite
		// already handles. This is specifically about history.
		assert.strictEqual(heat({ firstTid: undefined }), 0);
		assert.strictEqual(heat({ secondTid: undefined }), 0);
	});

	test("strangers who have never met are not rivals", () => {
		assert.strictEqual(heat(), 0);
	});

	test("a declared rival starts hot without any history", () => {
		assert.strictEqual(
			heat({ rivalry: { ...NO_RIVALRY, declared: true } }) > 0.5,
			true,
		);
	});

	test("familiarity builds, with diminishing returns", () => {
		const once = heat({ rivalry: { ...NO_RIVALRY, meetings: 1 } });
		const four = heat({
			rivalry: { ...NO_RIVALRY, meetings: 4, firstWins: 2 },
		});
		const twenty = heat({
			rivalry: { ...NO_RIVALRY, meetings: 20, firstWins: 10 },
		});
		assert.strictEqual(four > once, true);
		// The fourth meeting makes a nemesis; the twentieth adds nothing new.
		assert.strictEqual(twenty - four < four - once, true);
	});

	test("a contested series burns hotter than a sweep", () => {
		const split = heat({
			rivalry: { ...NO_RIVALRY, meetings: 4, firstWins: 2 },
		});
		const sweep = heat({
			rivalry: { ...NO_RIVALRY, meetings: 4, firstWins: 4 },
		});
		assert.strictEqual(split > sweep, true);
	});

	test("a swapped player adds bad blood", () => {
		assert.strictEqual(
			heat({ rivalry: { ...NO_RIVALRY, meetings: 2, swapped: true } }) >
				heat({ rivalry: { ...NO_RIVALRY, meetings: 2 } }),
			true,
		);
	});

	test("two fans of the same team who agree are not enemies", () => {
		assert.strictEqual(
			heat({
				firstTid: 3,
				secondTid: 3,
				firstOptimism: 0.6,
				secondOptimism: 0.5,
			}),
			0,
		);
	});

	test("the homer and the doomer of one team are, by construction", () => {
		// No shared history needed: they disagree about the same evidence,
		// which is the most reliable argument in any fanbase.
		assert.strictEqual(
			heat({
				firstTid: 3,
				secondTid: 3,
				firstOptimism: 0.8,
				secondOptimism: -0.85,
			}) > 0.4,
			true,
		);
	});

	test("a real rivalry still outranks a civil war", () => {
		const civilWar = heat({
			firstTid: 3,
			secondTid: 3,
			firstOptimism: 1,
			secondOptimism: -1,
		});
		const rivals = heat({
			rivalry: {
				meetings: 4,
				firstWins: 2,
				declared: true,
				swapped: true,
			},
		});
		assert.strictEqual(rivals > civilWar, true);
	});

	test("heat is the same whichever account asks", () => {
		// A feud only one side feels is not a feud.
		const forward = heat({
			firstTid: 0,
			secondTid: 1,
			firstOptimism: 0.5,
			secondOptimism: -0.5,
			rivalry: { meetings: 4, firstWins: 1, declared: false, swapped: true },
		});
		const backward = heat({
			firstTid: 1,
			secondTid: 0,
			firstOptimism: -0.5,
			secondOptimism: 0.5,
			rivalry: { meetings: 4, firstWins: 3, declared: false, swapped: true },
		});
		assert.strictEqual(forward, backward);
	});

	test("heat never leaves 0 to 1", () => {
		const max = heat({
			rivalry: { meetings: 40, firstWins: 20, declared: true, swapped: true },
		});
		assert.strictEqual(max <= 1, true);
		assert.strictEqual(max > 0.9, true);
	});
});

describe("rivalryFrom", () => {
	const games = [
		{ tids: [0, 1], winnerTid: 0 },
		{ tids: [0, 1], winnerTid: 1 },
		{ tids: [0, 2], winnerTid: 0 },
		{ tids: [1, 2], winnerTid: 2 },
		{ tids: [0, 1], winnerTid: 0 },
	];

	test("counts only the games these two played each other", () => {
		const ctx = rivalryFrom({
			firstTid: 0,
			secondTid: 1,
			games,
			swappedPairs: [],
			declaredRivals: [],
		});
		assert.strictEqual(ctx.meetings, 3);
		assert.strictEqual(ctx.firstWins, 2);
	});

	test("a swap counts in either direction", () => {
		const asWritten = rivalryFrom({
			firstTid: 0,
			secondTid: 1,
			games,
			swappedPairs: [[1, 0]],
			declaredRivals: [],
		});
		assert.strictEqual(asWritten.swapped, true);
	});

	test("teams that never met produce an empty context", () => {
		const ctx = rivalryFrom({
			firstTid: 5,
			secondTid: 6,
			games,
			swappedPairs: [],
			declaredRivals: [],
		});
		assert.deepStrictEqual(ctx, {
			meetings: 0,
			firstWins: 0,
			declared: false,
			swapped: false,
		});
	});

	test("a declared rival is carried through", () => {
		assert.strictEqual(
			rivalryFrom({
				firstTid: 0,
				secondTid: 1,
				games: [],
				swappedPairs: [],
				declaredRivals: [1, 4],
			}).declared,
			true,
		);
	});

	test("the two sides count the same series the same way", () => {
		const a = rivalryFrom({
			firstTid: 0,
			secondTid: 1,
			games,
			swappedPairs: [],
			declaredRivals: [],
		});
		const b = rivalryFrom({
			firstTid: 1,
			secondTid: 0,
			games,
			swappedPairs: [],
			declaredRivals: [],
		});
		assert.strictEqual(a.meetings, b.meetings);
		assert.strictEqual(a.firstWins + b.firstWins, a.meetings);
	});
});
