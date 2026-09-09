import { assert, describe, test } from "vitest";
import { dedupePlayerSubjects } from "./getAutoRecap.ts";

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
