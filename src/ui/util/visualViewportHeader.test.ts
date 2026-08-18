import { assert, describe, test } from "vitest";
import {
	headerVisualShift,
	layoutViewportOversized,
	tickerSelfPlacementShift,
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

describe("layoutViewportOversized", () => {
	test("the field case: a restored PWA claiming a 1052px viewport over 646 visible at 0.85", () => {
		assert.isTrue(
			layoutViewportOversized({
				scale: 0.85,
				visualHeight: 646,
				layoutHeight: 1052,
			}),
		);
	});

	test("the keyboard is not oversized - it shrinks the visual viewport at scale 1", () => {
		// Engaging here would hoist the bar on top of whatever is being typed.
		assert.isFalse(
			layoutViewportOversized({
				scale: 1,
				visualHeight: 646,
				layoutHeight: 1052,
			}),
		);
	});

	test("pinch-zoom in is not oversized - the standing correction's job", () => {
		assert.isFalse(
			layoutViewportOversized({
				scale: 2,
				visualHeight: 500,
				layoutHeight: 1052,
			}),
		);
	});

	test("a toolbar transition's worth of disagreement does not qualify", () => {
		assert.isFalse(
			layoutViewportOversized({
				scale: 0.85,
				visualHeight: 960,
				layoutHeight: 1052,
			}),
		);
	});

	test("the second field case: 636 visible over a 1083 layout at 0.75", () => {
		// A different device and zoom from the first report, same disease.
		assert.isTrue(
			layoutViewportOversized({
				scale: 0.75,
				visualHeight: 636,
				layoutHeight: 1083,
			}),
		);
	});

	test("no visual viewport, no verdict", () => {
		assert.isFalse(
			layoutViewportOversized({
				scale: undefined,
				visualHeight: undefined,
				layoutHeight: 1052,
			}),
		);
		assert.isFalse(
			layoutViewportOversized({
				scale: Number.NaN,
				visualHeight: 646,
				layoutHeight: 1052,
			}),
		);
	});
});

describe("tickerSelfPlacementShift", () => {
	test("parks the bar's bottom on the visible bottom", () => {
		// The field case at resume (offset 0) and panned down 324. Either way the
		// bar's bottom edge (shift + its height) must land exactly on the bottom
		// of what the user can see (offsetTop + visual height).
		for (const offsetTop of [0, 324]) {
			const shift = tickerSelfPlacementShift({
				offsetTop,
				visualHeight: 646,
				barHeight: 37,
			});
			assert.strictEqual(shift + 37, offsetTop + 646);
		}
	});

	test("the second field case lands the bar on the visible bottom", () => {
		// Reported state: 636 visible starting 137 down, 36-tall bar. The style
		// written from these was correct; what failed was that a compositor kept
		// painting an older transform, which is why the offset now goes to `top`.
		assert.strictEqual(
			tickerSelfPlacementShift({
				offsetTop: 137,
				visualHeight: 636,
				barHeight: 36,
			}),
			737,
		);
	});

	test("a missing offset means the visual viewport starts at the top", () => {
		assert.strictEqual(
			tickerSelfPlacementShift({
				offsetTop: undefined,
				visualHeight: 646,
				barHeight: 37,
			}),
			609,
		);
	});

	test("never places the bar above the viewport", () => {
		assert.strictEqual(
			tickerSelfPlacementShift({
				offsetTop: 0,
				visualHeight: 20,
				barHeight: 40,
			}),
			0,
		);
	});
});
