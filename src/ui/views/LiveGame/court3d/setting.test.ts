import { assert, test } from "vitest";
import { parseLiveGameView } from "./setting.ts";

// The 3D court was "retro" and then "2.5d" before it was "3d": a browser that
// picked it under either name, or a device on an older version broadcasting
// its view, still gets the 3D court.
test("every name the 3D court has had reads as 3D", () => {
	for (const value of ["3d", "2.5d", "retro"]) {
		assert.strictEqual(parseLiveGameView(value), "3d");
	}
});

test("anything else is the classic court", () => {
	for (const value of [undefined, null, "", "classic", "4d"]) {
		assert.strictEqual(parseLiveGameView(value), "classic");
	}
});
