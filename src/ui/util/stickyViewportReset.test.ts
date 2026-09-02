import { assert, describe, test } from "vitest";
import {
	isGhostViewport,
	viewportOversized,
	type ViewportReading,
} from "./stickyViewportReset.ts";

// The field device, as the touch probe measured it.
const FIELD: ViewportReading = {
	innerWidth: 518,
	innerHeight: 1052,
	screenWidth: 440,
	screenHeight: 956,
	reportedScale: 0.85,
	touchScale: 1.021,
};

describe("the ghost viewport", () => {
	test("the field device is a ghost", () => {
		assert.strictEqual(viewportOversized(FIELD), true);
		assert.strictEqual(isGhostViewport(FIELD), true);
	});

	test("a page the user really pinched out is not", () => {
		// Reported and measured agree: the page IS drawn at 0.85. Resetting
		// it would undo the user's own zoom.
		const pinched = { ...FIELD, touchScale: 0.85 };
		assert.strictEqual(viewportOversized(pinched), true);
		assert.strictEqual(isGhostViewport(pinched), false);
	});

	test("a healthy page is neither", () => {
		const healthy: ViewportReading = {
			innerWidth: 440,
			innerHeight: 894,
			screenWidth: 440,
			screenHeight: 956,
			reportedScale: 1,
			touchScale: 1,
		};
		assert.strictEqual(viewportOversized(healthy), false);
		assert.strictEqual(isGhostViewport(healthy), false);
	});

	test("without enough taps the automatic path cannot convict", () => {
		// Oversized says "plausible"; the ghost verdict needs the taps.
		const untapped = { ...FIELD, touchScale: undefined };
		assert.strictEqual(viewportOversized(untapped), true);
		assert.strictEqual(isGhostViewport(untapped), false);
	});

	test("a page that fits its screen is never oversized, whatever it reports", () => {
		const fits: ViewportReading = {
			...FIELD,
			innerWidth: 440,
			innerHeight: 894,
		};
		assert.strictEqual(viewportOversized(fits), false);
	});
});
