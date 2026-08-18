import { assert, describe, test } from "vitest";
import {
	headerVisualShift,
	tickerVisualShift,
	visualViewportPlausible,
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

describe("visualViewportPlausible", () => {
	// A field log caught the same device, on the same page, alternating within
	// seconds between these two readings with nothing changing:
	//
	//   vv=1083/1083@0x0.75   and   vv=636/1083@0x0.75
	//
	// Only one can be true. Zooming OUT shows MORE of the page, so at 0.75 the
	// visible height cannot be 59% of the layout viewport.
	test("the impossible reading is rejected", () => {
		assert.isFalse(
			visualViewportPlausible({
				scale: 0.75,
				visualHeight: 636,
				layoutHeight: 1083,
			}),
		);
	});

	test("the self-consistent reading is accepted", () => {
		assert.isTrue(
			visualViewportPlausible({
				scale: 0.75,
				visualHeight: 1083,
				layoutHeight: 1083,
			}),
		);
	});

	test("the earlier report had the same impossible signature", () => {
		// Which is why the mode built on it never worked.
		assert.isFalse(
			visualViewportPlausible({
				scale: 0.85,
				visualHeight: 646,
				layoutHeight: 1052,
			}),
		);
	});

	test("zoomed IN is allowed to show less - that is what zooming in is", () => {
		assert.isTrue(
			visualViewportPlausible({
				scale: 2,
				visualHeight: 500,
				layoutHeight: 1052,
			}),
		);
		assert.isTrue(
			visualViewportPlausible({
				scale: 1,
				visualHeight: 640,
				layoutHeight: 1052,
			}),
		);
	});

	test("nothing to check against means believe it", () => {
		assert.isTrue(
			visualViewportPlausible({
				scale: undefined,
				visualHeight: undefined,
				layoutHeight: 1052,
			}),
		);
	});
});

describe("tickerVisualShift stands down on an impossible viewport", () => {
	test("no shift from numbers that cannot be true", () => {
		// Acting on the bogus gap is what pulled the bar off the bottom of the
		// screen and into the middle of the page.
		assert.strictEqual(
			tickerVisualShift({
				offsetTop: 137,
				visualHeight: 636,
				layoutHeight: 1083,
				scale: 0.75,
			}),
			0,
		);
	});

	test("a genuine zoomed-in pan still gets its correction", () => {
		assert.strictEqual(
			tickerVisualShift({
				offsetTop: 100,
				visualHeight: 500,
				layoutHeight: 1000,
				scale: 2,
			}),
			-400,
		);
	});

	test("an unzoomed page is untouched, as always", () => {
		assert.strictEqual(
			tickerVisualShift({
				offsetTop: 0,
				visualHeight: 1000,
				layoutHeight: 1000,
				scale: 1,
			}),
			0,
		);
	});
});
