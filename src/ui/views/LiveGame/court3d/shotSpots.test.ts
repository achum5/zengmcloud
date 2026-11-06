import { assert, describe, test } from "vitest";
import { COURT_H, rimX } from "./geometry.ts";
import { shotSpots } from "./shotSpots.ts";
import { compile } from "./testGame.ts";

describe("the shot chart's spots", () => {
	test("every shot where the court had it let go from, at his own end", () => {
		const { tl } = compile("a", 80);
		const spots = shotSpots(tl);
		const attempts = tl.beats.filter((b) => b.type.startsWith("fga")).length;
		assert.isAbove(spots.length, attempts * 0.9);
		assert.isAtMost(spots.length, attempts);
		const near = spots.filter(
			(s) => Math.hypot(s.x - rimX(s.side), s.y - COURT_H / 2) < 30,
		);
		// (Bar a heave from his own end at the buzzer.)
		assert.isAbove(near.length, spots.length * 0.97);
		assert.isTrue(spots.some((s) => s.made));
		assert.isTrue(spots.some((s) => s.three));
	}, 300_000);
});
