import { assert, describe, test } from "vitest";
import {
	headerVisualShift,
	tickerVisualShift,
} from "./visualViewportHeader.ts";

describe("headerVisualShift", () => {
	test("pushes the header down by however far the visible area starts", () => {
		// The field case: a 646-tall visual viewport sitting 406px down a 1052
		// layout viewport, with the header parked above everything visible.
		assert.strictEqual(headerVisualShift(406), 406);
	});

	test("does nothing when the two viewports agree", () => {
		// Which is every non-zoomed page, so the common path stays untouched.
		assert.strictEqual(headerVisualShift(0), 0);
	});

	test("never shifts upward", () => {
		assert.strictEqual(headerVisualShift(-40), 0);
	});

	test("copes with no visual viewport at all", () => {
		assert.strictEqual(headerVisualShift(undefined), 0);
		assert.strictEqual(headerVisualShift(Number.NaN), 0);
	});

	test("rounds to whole pixels", () => {
		assert.strictEqual(headerVisualShift(405.6), 406);
	});
});

describe("tickerVisualShift", () => {
	// The bug this closes, from a field report: offsetTop 300 on a 1052-tall
	// layout viewport. Sticky pinned the bar to the foot of the LAYOUT viewport,
	// which the report measured at client y 752 - exactly 1052 - 300 - and the
	// screenshot had it about 70% down the screen with box score below it.
	//
	// The visible region is the same SIZE as the layout viewport, just slid down
	// by offsetTop, so the bar needs the same downward correction the header
	// gets. It was being pushed the other way for a long time.
	test("moves down by the offset, exactly like the header", () => {
		assert.strictEqual(tickerVisualShift(300), 300);
		assert.strictEqual(tickerVisualShift(300), headerVisualShift(300));
		assert.strictEqual(tickerVisualShift(406), headerVisualShift(406));
	});

	test("an unpanned page is untouched", () => {
		// Which is every ordinary page on every device - the common path does
		// nothing at all.
		assert.strictEqual(tickerVisualShift(0), 0);
		assert.strictEqual(tickerVisualShift(undefined), 0);
	});

	test("the keyboard is still safe", () => {
		// It shortens the visible area without moving it, so offsetTop stays 0
		// and the bar stays put rather than hoisting over what is being typed.
		assert.strictEqual(tickerVisualShift(0), 0);
	});

	test("never moves upward", () => {
		assert.strictEqual(tickerVisualShift(-50), 0);
		assert.strictEqual(tickerVisualShift(Number.NaN), 0);
	});
});
