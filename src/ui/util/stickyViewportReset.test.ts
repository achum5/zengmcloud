import { assert, describe, test } from "vitest";
import {
	viewportOversized,
	visualViewportStale,
	type ViewportReading,
} from "./stickyViewportReset.ts";

// The field device, as reported: zoomed out to 0.85 and with a visual viewport
// a keyboard's height short of the layout viewport, with nothing focused.
const FIELD: ViewportReading = {
	innerWidth: 518,
	innerHeight: 1052,
	screenWidth: 440,
	screenHeight: 956,
	reportedScale: 0.85,
	vvHeight: 646,
	vvOffsetTop: 15,
	editableFocused: false,
};

describe("the stale visual viewport", () => {
	test("the field device has a stale keyboard inset", () => {
		assert.strictEqual(visualViewportStale(FIELD), true);
	});

	test("a keyboard that is actually up is not a fault", () => {
		// Same numbers, but a textarea has focus: the inset is the keyboard.
		assert.strictEqual(
			visualViewportStale({ ...FIELD, editableFocused: true }),
			false,
		);
	});

	test("browser chrome does not count", () => {
		// Safari's toolbars take a hundred-odd points; that is not a keyboard.
		assert.strictEqual(
			visualViewportStale({ ...FIELD, innerHeight: 760, vvHeight: 646 }),
			false,
		);
	});

	test("a healthy page is not stale", () => {
		assert.strictEqual(
			visualViewportStale({
				...FIELD,
				innerWidth: 440,
				innerHeight: 894,
				reportedScale: 1,
				vvHeight: 894,
				vvOffsetTop: 0,
			}),
			false,
		);
	});

	test("no visual viewport API, no verdict", () => {
		assert.strictEqual(
			visualViewportStale({ ...FIELD, vvHeight: undefined }),
			false,
		);
	});
});

describe("the oversized layout viewport", () => {
	test("the field device is zoomed out", () => {
		assert.strictEqual(viewportOversized(FIELD), true);
	});

	test("a page that fits its screen is not, whatever it reports", () => {
		assert.strictEqual(
			viewportOversized({ ...FIELD, innerWidth: 440, innerHeight: 894 }),
			false,
		);
	});

	test("scale one is never oversized", () => {
		assert.strictEqual(
			viewportOversized({ ...FIELD, reportedScale: 1 }),
			false,
		);
	});
});
