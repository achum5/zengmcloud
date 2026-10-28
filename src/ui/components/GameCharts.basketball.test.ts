import { assert, test } from "vitest";
import { sideColors } from "./GameCharts.basketball.tsx";

test("each side in its own team's colors, told apart", () => {
	// Main colors far apart: those.
	assert.deepEqual(
		sideColors([
			{ colors: ["#002b5c", "#e31837", "#c4ced4"] },
			{ colors: ["#ce1141", "#000000", "#a1a1a4"] },
		]),
		["#002b5c", "#ce1141"],
	);
	// Dark purple and teal: told apart well enough - not the teal team in
	// its light gray.
	assert.deepEqual(
		sideColors([
			{ colors: ["#291091", "#ae7c00", "#00d1df"] },
			{ colors: ["#007681", "#d6d6d6", "#ffa400"] },
		]),
		["#291091", "#007681"],
	);
	// Both red: one of them in his other color - a team color still.
	const [a, b] = sideColors([
		{ colors: ["#ce1141", "#000000", "#ffffff"] },
		{ colors: ["#c8102e", "#1d428a", "#ffffff"] },
	]);
	assert.include(["#ce1141", "#000000", "#ffffff"], a);
	assert.include(["#c8102e", "#1d428a", "#ffffff"], b);
	assert.notStrictEqual(`${a}${b}`, "#ce1141#c8102e");
	// A near-white main color, and three-digit colors: his other one.
	assert.deepEqual(
		sideColors([
			{ colors: ["#fff", "#0a0", "#000"] },
			{ colors: ["#552583", "#fdb927", "#000000"] },
		]),
		["#0a0", "#552583"],
	);
});
