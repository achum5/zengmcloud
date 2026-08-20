import { assert, describe, test } from "vitest";
import { capMoves, labelMove } from "./getSeasonRecapData.ts";
import { PHASE } from "../../common/constants.ts";

describe("labelMove", () => {
	// Without the tag, "traded X" and "signed Y" are two facts side by side. With
	// it, they're two steps of the same free agency - which is the difference
	// between a recap that reads the offseason and one that guesses at it.
	test("says which part of the calendar a move happened in", () => {
		assert.strictEqual(
			labelMove(PHASE.FREE_AGENCY, "The Lakers signed Star Guy."),
			"[Free agency] The Lakers signed Star Guy.",
		);
		assert.strictEqual(
			labelMove(PHASE.DRAFT, "The Lakers drafted Rookie."),
			"[Draft] The Lakers drafted Rookie.",
		);
		assert.strictEqual(
			labelMove(PHASE.AFTER_TRADE_DEADLINE, "The Lakers cut Guy."),
			"[After trade deadline] The Lakers cut Guy.",
		);
	});

	test("every phase a move can be logged in has a label", () => {
		for (const phase of Object.values(PHASE)) {
			const text = labelMove(phase, "Something happened.");
			assert.ok(text.startsWith("["), `phase ${phase}: ${text}`);
		}
	});

	test("an event with no phase is left alone rather than mislabelled", () => {
		assert.strictEqual(labelMove(undefined, "Something."), "Something.");
		assert.strictEqual(labelMove(99, "Something."), "Something.");
	});
});

describe("capMoves", () => {
	const moves = ["a", "b", "c", "d", "e"];

	// The recap is told to read the moves as a sequence, so the cap must not be
	// what scrambles them.
	test("keeps the order they happened in", () => {
		assert.deepStrictEqual(capMoves(moves, 3).lines, ["c", "d", "e"]);
		assert.deepStrictEqual(capMoves(moves, 10).lines, moves);
	});

	test("reports what it dropped, so the prompt can admit the list is partial", () => {
		assert.strictEqual(capMoves(moves, 3).omitted, 2);
		assert.strictEqual(capMoves(moves, 5).omitted, 0);
		assert.strictEqual(capMoves(moves, 10).omitted, 0);
		assert.strictEqual(capMoves([], 10).omitted, 0);
	});
});
