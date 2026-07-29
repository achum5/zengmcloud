import { assert, describe, test } from "vitest";
import { buildGridShareText, tierEmoji, TIER_EMOJI } from "./triviaShare.ts";

const points = (values: (number | undefined)[]) => values;

describe("tierEmoji", () => {
	test("an unsolved cell is a blank", () => {
		assert.strictEqual(tierEmoji(undefined), TIER_EMOJI.empty);
	});

	test("each tier gets its own color", () => {
		assert.strictEqual(tierEmoji(95), TIER_EMOJI.mythic);
		assert.strictEqual(tierEmoji(80), TIER_EMOJI.legendary);
		assert.strictEqual(tierEmoji(65), TIER_EMOJI.epic);
		assert.strictEqual(tierEmoji(45), TIER_EMOJI.rare);
		assert.strictEqual(tierEmoji(25), TIER_EMOJI.uncommon);
		assert.strictEqual(tierEmoji(5), TIER_EMOJI.common);
	});

	// The board's own tier boundaries, so the shared square and the badge on
	// the cell can never disagree about what a pick was worth.
	test("the boundaries match the board's", () => {
		assert.strictEqual(tierEmoji(90), TIER_EMOJI.mythic);
		assert.strictEqual(tierEmoji(89), TIER_EMOJI.legendary);
		assert.strictEqual(tierEmoji(20), TIER_EMOJI.uncommon);
		assert.strictEqual(tierEmoji(19), TIER_EMOJI.common);
	});
});

describe("buildGridShareText", () => {
	test("three rows of three, in reading order", () => {
		const text = buildGridShareText({
			points: points([95, undefined, 5, 45, 45, 45, undefined, undefined, 25]),
			score: 260,
			hintedCount: 0,
		});
		const lines = text.split("\n");
		assert.strictEqual(lines[1], "🟥⬛⬜");
		assert.strictEqual(lines[2], "🟩🟩🟩");
		assert.strictEqual(lines[3], "⬛⬛🟦");
	});

	// The whole point of the block: it says how you did without naming a single
	// player, so it can be posted where other people are about to play it.
	test("no player name reaches the share text", () => {
		const text = buildGridShareText({
			points: points(Array.from({ length: 9 }, () => 50)),
			score: 450,
			hintedCount: 0,
		});
		assert.match(text, /^[^a-z]*Immaculate/);
		assert.notMatch(text, /Jordan|Bird/);
	});

	test("a full board says so", () => {
		const text = buildGridShareText({
			points: points(Array.from({ length: 9 }, () => 30)),
			score: 270,
			hintedCount: 0,
		});
		assert.match(text, /Immaculate/);
		assert.match(text, /9\/9/);
	});

	test("an unfinished board does not", () => {
		const text = buildGridShareText({
			points: points([30, 30, undefined, 30, 30, 30, 30, 30, 30]),
			score: 240,
			hintedCount: 0,
		});
		assert.notMatch(text, /Immaculate/);
		assert.match(text, /8\/9/);
	});

	// Leaving hints out would make the score read as an unaided one.
	test("hints are disclosed when any were used", () => {
		const text = buildGridShareText({
			points: points(Array.from({ length: 9 }, () => 30)),
			score: 200,
			hintedCount: 4,
		});
		assert.match(text, /4 hinted/);
	});

	test("and not mentioned when none were", () => {
		const text = buildGridShareText({
			points: points(Array.from({ length: 9 }, () => 30)),
			score: 270,
			hintedCount: 0,
		});
		assert.notMatch(text, /hinted/);
	});
});
