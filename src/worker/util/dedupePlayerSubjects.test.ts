import { assert, describe, test } from "vitest";
import { dedupePlayerSubjects, joinShortPairs } from "./getAutoRecap.ts";

const names = ["Yuta Dunn", "Amen Dunn"];

describe("dedupePlayerSubjects", () => {
	test("the second of two sentences opening on one man gets a pronoun", () => {
		assert.deepStrictEqual(
			dedupePlayerSubjects(
				[
					"Yuta Dunn shot 64.3% on the night.",
					"Yuta Dunn had not scored more than 47 this season.",
					"Yuta Dunn's streak reached 16 games.",
				],
				names,
			),
			[
				"Yuta Dunn shot 64.3% on the night.",
				"He had not scored more than 47 this season.",
				"His streak reached 16 games.",
			],
		);
	});

	test("a different man in between keeps the name", () => {
		assert.deepStrictEqual(
			dedupePlayerSubjects(
				[
					"Yuta Dunn shot 64.3% on the night.",
					"Amen Dunn added 20 points.",
					"Yuta Dunn had not scored more than 47 this season.",
				],
				names,
			),
			[
				"Yuta Dunn shot 64.3% on the night.",
				"Amen Dunn added 20 points.",
				"Yuta Dunn had not scored more than 47 this season.",
			],
		);
	});

	test("a sentence that merely mentions him is not an opening on him", () => {
		const sentences = [
			"It was the 16th game in a row Yuta Dunn has reached 20.",
			"Yuta Dunn shot 64.3% on the night.",
		];
		assert.deepStrictEqual(dedupePlayerSubjects(sentences, names), sentences);
	});
});

describe("joinShortPairs", () => {
	test("two short sentences on one subject become one", () => {
		assert.deepStrictEqual(
			joinShortPairs([
				"The Bucks never trailed.",
				"They got double figures out of 7 men.",
				"They put together a 12-0 run in the first.",
			]),
			[
				"The Bucks never trailed and got double figures out of 7 men.",
				"They put together a 12-0 run in the first.",
			],
		);
	});

	test("a participial tail or an existing 'and' is left alone", () => {
		const kept = [
			"The Clippers cruised past the Wizards, led by Tyrese Green's 12 points.",
			"They led from start to finish.",
		];
		assert.deepStrictEqual(joinShortPairs(kept), kept);
		const withAnd = [
			"Trey Green put up 27 points and 12 rebounds.",
			"He shot 10-of-16.",
		];
		assert.deepStrictEqual(joinShortPairs(withAnd), withAnd);
	});
});
