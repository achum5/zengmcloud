import { assert, describe, test } from "vitest";
import {
	buildGoatTerms,
	goatLeaves,
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

	test("leaves a difference alone - that is one net quantity", () => {
		const terms = buildGoatTerms("(a - b) / 10");

		assert.deepStrictEqual(terms[0]!.children, []);
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

describe("reading a formula as a flat list", () => {
	test("returns only the leaves", () => {
		assert.deepStrictEqual(
			goatLeaves("(a + b) + c").map((leaf) => leaf.text),
			["a", "b", "c"],
		);
	});

	test("carries a negation down through the nesting", () => {
		assert.deepStrictEqual(goatLeaves("a - (b + c)"), [
			{ text: "a", sign: 1 },
			{ text: "b", sign: -1 },
			{ text: "c", sign: -1 },
		]);
	});

	test("a subtracted difference stays one leaf", () => {
		assert.deepStrictEqual(goatLeaves("a - (b - c)"), [
			{ text: "a", sign: 1 },
			{ text: "b - c", sign: -1 },
		]);
	});

	test("opens up a scaled sum", () => {
		assert.deepStrictEqual(
			goatLeaves("(a + b) / 10").map((leaf) => leaf.text),
			["(a) / 10", "(b) / 10"],
		);
	});
});

describe("net quantities stay whole", () => {
	test("does not split a difference inside a term", () => {
		assert.deepStrictEqual(
			goatLeaves("x + (ortg - drtg) / 10").map((leaf) => leaf.text),
			["x", "(ortg - drtg) / 10"],
		);
	});

	test("still splits a difference written at the top level", () => {
		assert.deepStrictEqual(
			goatLeaves("a - b").map((leaf) => leaf.text),
			["a", "b"],
		);
	});

	test("splits a nested block that only adds", () => {
		assert.deepStrictEqual(
			goatLeaves("x + (obpm + dbpm) / 10").map((leaf) => leaf.text),
			["x", "(obpm) / 10", "(dbpm) / 10"],
		);
	});

	test("a penalty block subtracted at the top level still opens up", () => {
		assert.deepStrictEqual(
			goatLeaves("a - ((tpa - tp) / 10 + (fga - fg) / 10)"),
			[
				{ text: "a", sign: 1 },
				{ text: "(tpa - tp) / 10", sign: -1 },
				{ text: "(fga - fg) / 10", sign: -1 },
			],
		);
	});
});
