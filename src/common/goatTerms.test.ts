import { assert, describe, test } from "vitest";
import {
	buildGoatTerms,
	splitAdditive,
	stripOuterParens,
	variablesUsed,
} from "./goatTerms.ts";

describe("splitting a formula into terms", () => {
	test("splits on top-level plus and minus", () => {
		assert.deepStrictEqual(splitAdditive("a + b - c"), [
			{ text: "a", negated: false },
			{ text: "b", negated: false },
			{ text: "c", negated: true },
		]);
	});

	test("does not split inside parens", () => {
		assert.deepStrictEqual(splitAdditive("(a + b) * 2 - c"), [
			{ text: "(a + b) * 2", negated: false },
			{ text: "c", negated: true },
		]);
	});

	test("a leading minus is a sign, not a separator", () => {
		assert.deepStrictEqual(splitAdditive("-a + b"), [
			{ text: "-a", negated: false },
			{ text: "b", negated: false },
		]);
	});

	test("a minus after an operator is a sign", () => {
		assert.deepStrictEqual(splitAdditive("a * -b + c"), [
			{ text: "a * -b", negated: false },
			{ text: "c", negated: false },
		]);
	});

	test("commas inside a function call are not terms", () => {
		assert.deepStrictEqual(splitAdditive("max(0, 35 - age) + b"), [
			{ text: "max(0, 35 - age)", negated: false },
			{ text: "b", negated: false },
		]);
	});
});

describe("redundant parens", () => {
	test("strips wrapping parens", () => {
		assert.strictEqual(stripOuterParens("((a + b))"), "a + b");
	});

	test("keeps parens that do not wrap the whole expression", () => {
		assert.strictEqual(stripOuterParens("(a) * (b)"), "(a) * (b)");
	});

	test("keeps a function call intact", () => {
		assert.strictEqual(stripOuterParens("min(a, b)"), "min(a, b)");
	});
});

describe("the term tree", () => {
	test("nests, and remembers which branches are subtracted", () => {
		const terms = buildGoatTerms("(a + b) - (c + d)");

		assert.strictEqual(terms.length, 2);
		assert.strictEqual(terms[0]!.negated, false);
		assert.strictEqual(terms[1]!.negated, true);
		assert.deepStrictEqual(
			terms[1]!.children.map((child) => child.text),
			["c", "d"],
		);
	});

	test("a term with no top-level operator is a leaf", () => {
		const terms = buildGoatTerms("mvp * 20");

		assert.strictEqual(terms.length, 1);
		assert.deepStrictEqual(terms[0]!.children, []);
	});

	test("stops at the depth limit", () => {
		const terms = buildGoatTerms("((a + b) + (c + d)) + e", 1);

		assert.strictEqual(terms.length, 2);
		assert.deepStrictEqual(terms[0]!.children, []);
	});
});

describe("the variables a formula reads", () => {
	test("finds plain and dotted names", () => {
		assert.deepStrictEqual(variablesUsed("awards.NBA1 *0.8 + champ /10"), [
			"awards.NBA1",
			"champ",
		]);
	});

	test("skips functions but keeps min the stat", () => {
		assert.deepStrictEqual(variablesUsed("min - max(0, 24 - min)"), ["min"]);
	});
});

describe("a parenthesised sum over a divisor", () => {
	test("splits, carrying the divisor into each child", () => {
		const terms = buildGoatTerms("(a + b) / 10");

		assert.deepStrictEqual(
			terms[0]!.children.map((child) => child.text),
			["(a) / 10", "(b) / 10"],
		);
	});

	test("splits a leading multiplier too", () => {
		const terms = buildGoatTerms("2 * (a + b)");

		assert.deepStrictEqual(
			terms[0]!.children.map((child) => child.text),
			["2 * (a)", "2 * (b)"],
		);
	});

	test("keeps subtraction inside the sum", () => {
		const terms = buildGoatTerms("(a - b) / 10");

		assert.deepStrictEqual(
			terms[0]!.children.map((child) => ({
				text: child.text,
				negated: child.negated,
			})),
			[
				{ text: "(a) / 10", negated: false },
				{ text: "(b) / 10", negated: true },
			],
		);
	});

	test("refuses a product of two sums, which does not distribute this way", () => {
		const terms = buildGoatTerms("(a + b) * (c + d)");

		assert.deepStrictEqual(terms[0]!.children, []);
	});

	test("refuses an exponent, which does not distribute at all", () => {
		const terms = buildGoatTerms("(a + b) ^ 2");

		assert.deepStrictEqual(terms[0]!.children, []);
	});
});
