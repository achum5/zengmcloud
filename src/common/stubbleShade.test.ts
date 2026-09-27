import { assert, test } from "vitest";
import { stubbleShave } from "./stubbleShade.ts";

test("dark hair keeps black stubble", () => {
	assert.strictEqual(
		stubbleShave("rgba(0,0,0,0.15)", "#272421"),
		"rgba(0,0,0,0.15)",
	);
});

test("fair hair tints the stubble toward the hair, same strength", () => {
	const shade = stubbleShave("rgba(0,0,0,0.15)", "#e9c67b")!;
	assert.match(shade, /^rgba\((?:\d+,){3}0\.15\)$/);
	assert.notStrictEqual(shade, "rgba(0,0,0,0.15)");
	const [r, g, b] = shade.slice(5, -1).split(",").map(Number);
	assert.isAbove(r!, b!, "warm, like the hair");
	assert.isAbove(g!, 0);
});

test("running it twice changes nothing", () => {
	const once = stubbleShave("rgba(0,0,0,0.12)", "#cc9966");
	assert.strictEqual(stubbleShave(once, "#cc9966"), once);
});

test("no shadow, or nothing to read, is left alone", () => {
	assert.strictEqual(stubbleShave("rgba(0,0,0,0)", "#e9c67b"), "rgba(0,0,0,0)");
	assert.strictEqual(stubbleShave(undefined, "#e9c67b"), undefined);
	assert.strictEqual(
		stubbleShave("rgba(0,0,0,0.1)", undefined),
		"rgba(0,0,0,0.1)",
	);
});
