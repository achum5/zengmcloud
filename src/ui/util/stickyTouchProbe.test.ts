import { assert, describe, test } from "vitest";
import {
	projectToScreen,
	screenVerdict,
	solveTouchMapping,
	type TouchSample,
} from "./stickyTouchProbe.ts";

const sample = (
	clientY: number,
	screenY: number,
	offsetTop = 0,
): TouchSample => ({ clientY, screenY, offsetTop });

describe("solveTouchMapping", () => {
	test("two taps recover an unzoomed screen sitting below a status bar", () => {
		// The ordinary case: client 0 paints 59pt down the glass, 1:1.
		const mapping = solveTouchMapping([sample(100, 159), sample(500, 559)])!;
		assert.strictEqual(mapping.originY, 59);
		assert.strictEqual(mapping.scale, 1);
		assert.strictEqual(mapping.spread, 400);
		assert.strictEqual(mapping.samples, 2);
	});

	test("the scale is measured, not taken from the viewport", () => {
		// The field device claims scale 0.85 while reporting a viewport whose
		// axes disagree. Taps answer without asking it.
		const mapping = solveTouchMapping([sample(0, 0), sample(400, 340)])!;
		assert.strictEqual(mapping.scale, 0.85);
		assert.strictEqual(mapping.originY, 0);
	});

	test("a bar that measures at the top of the viewport can be off the glass", () => {
		// THE WHOLE POINT. rect.top === 0 with the mapping saying client 0 sits
		// 406pt above the screen is a header that is provably not visible, which
		// no viewport number has been able to establish.
		const mapping = solveTouchMapping([sample(500, 94), sample(900, 494)])!;
		assert.strictEqual(mapping.originY, -406);
		assert.strictEqual(projectToScreen(mapping, 0), -406);
	});

	test("noisy taps average out instead of trusting the last one", () => {
		const mapping = solveTouchMapping([
			sample(100, 101),
			sample(300, 299),
			sample(500, 501),
			sample(700, 699),
		])!;
		assert.closeTo(mapping.scale, 1, 0.01);
		assert.closeTo(mapping.originY, 0, 2);
		assert.strictEqual(mapping.samples, 4);
	});

	test("samples from a different pan position are not mixed in", () => {
		// Panning a zoomed page slides the mapping. The newest offset is what
		// the user is looking at; older ones describe a screen that has moved.
		const mapping = solveTouchMapping([
			sample(100, 900, 0),
			sample(500, 100, 0),
			sample(100, 100, 40),
			sample(500, 500, 40),
		])!;
		assert.strictEqual(mapping.samples, 2);
		assert.strictEqual(mapping.scale, 1);
		assert.strictEqual(mapping.originY, 0);
	});

	test("one tap answers nothing, and says so", () => {
		assert.strictEqual(solveTouchMapping([sample(100, 100)]), undefined);
		assert.strictEqual(solveTouchMapping([]), undefined);
	});

	test("taps too close together cannot separate origin from scale", () => {
		// Two taps 4px apart would fit a scale from whatever rounding error is
		// in them - and the scale multiplies through every conclusion.
		assert.strictEqual(
			solveTouchMapping([sample(300, 300), sample(304, 305)]),
			undefined,
		);
	});

	test("repeated taps in one spot are refused rather than divided by zero", () => {
		assert.strictEqual(
			solveTouchMapping([sample(300, 300), sample(300, 300), sample(300, 300)]),
			undefined,
		);
	});

	test("only the newest pan position counts, even when it is the smaller group", () => {
		const mapping = solveTouchMapping([
			sample(0, 0, 0),
			sample(400, 400, 0),
			sample(800, 800, 0),
			sample(100, 200, 12),
			sample(500, 600, 12),
		])!;
		assert.strictEqual(mapping.samples, 2);
		assert.strictEqual(mapping.originY, 100);
	});
});

describe("screenVerdict", () => {
	const H = 956;

	test("a bar within the glass is visible", () => {
		assert.strictEqual(
			screenVerdict({ top: 59, bottom: 111, screenHeight: H }),
			"visible",
		);
	});

	test("a header pushed off the top reads as above", () => {
		assert.strictEqual(
			screenVerdict({ top: -406, bottom: -354, screenHeight: H }),
			"above",
		);
	});

	test("a ticker pushed past the foot reads as below", () => {
		// The exact failure: the ticker measures its bottom at the layout
		// viewport foot and lands 345pt under the glass.
		assert.strictEqual(
			screenVerdict({ top: 1000, bottom: 1052, screenHeight: H }),
			"below",
		);
	});

	test("a bar straddling an edge is clipped, not visible", () => {
		assert.strictEqual(
			screenVerdict({ top: -10, bottom: 42, screenHeight: H }),
			"clipped",
		);
		assert.strictEqual(
			screenVerdict({ top: 930, bottom: 982, screenHeight: H }),
			"clipped",
		);
	});

	test("touching an edge exactly is still off the glass", () => {
		assert.strictEqual(
			screenVerdict({ top: -52, bottom: 0, screenHeight: H }),
			"above",
		);
		assert.strictEqual(
			screenVerdict({ top: H, bottom: H + 52, screenHeight: H }),
			"below",
		);
	});
});
