import { assert, describe, test } from "vitest";
import {
	opponentNameFromText,
	customVideoPromptSeed,
} from "./getPlayerVideoMoments.ts";

describe("opponentNameFromText", () => {
	test("pulls the opponent out of a feat/clutch line", () => {
		assert.strictEqual(
			opponentNameFromText(
				"Player had a triple-double in a 110-104 win over the Cavaliers.",
			),
			"Cavaliers",
		);
		assert.strictEqual(
			opponentNameFromText(
				"Player made a basket with 4.8 seconds left to force overtime in a 105-106 loss to the Lakers.",
			),
			"Lakers",
		);
		assert.strictEqual(
			opponentNameFromText("... in a 99-99 tie with the Nets."),
			"Nets",
		);
		assert.strictEqual(
			opponentNameFromText("... against the Trail Blazers"),
			"Trail Blazers",
		);
	});

	test("returns undefined when there's no opponent clause", () => {
		assert.strictEqual(
			opponentNameFromText("Player had a monster game."),
			undefined,
		);
	});
});

describe("customVideoPromptSeed", () => {
	test("carries the player's physical details and a blank scene to fill in", () => {
		const p: any = {
			firstName: "Test",
			lastName: "Guy",
			hgt: 79, // 6'7"
			weight: 220,
			jerseyNumber: "7",
			stats: [],
		};
		const seed = customVideoPromptSeed(p, "SF", "Boston Celtics", 2001);
		assert.ok(seed.includes("Test Guy"), seed);
		assert.ok(seed.includes(`6'7"`), seed);
		assert.ok(seed.includes("220 lbs"), seed);
		assert.ok(seed.includes("#7"), seed);
		assert.ok(seed.includes("Boston Celtics"), seed);
		// Cartoon/no-photorealism art direction is present.
		assert.ok(/NOT photorealistic/i.test(seed), seed);
	});
});
