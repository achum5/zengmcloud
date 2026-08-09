import { assert, describe, test } from "vitest";
import {
	detachmentConfirmed,
	headerIsDetached,
	scrollDecision,
} from "./stickyHeaderWatchdog.ts";

const deps = ({
	scrollY = 500,
	headerTop = 0,
	stickyTop = 0,
	pinnedByModal = false,
	visualOffsetTop = 0,
}: {
	scrollY?: number;
	headerTop?: number;
	stickyTop?: number;
	pinnedByModal?: boolean;
	visualOffsetTop?: number;
}) => ({
	scrollY: () => scrollY,
	headerTop: () => headerTop,
	stickyTop: () => stickyTop,
	pinnedByModal: () => pinnedByModal,
	visualOffsetTop: () => visualOffsetTop,
});

describe("headerIsDetached", () => {
	// The failure this exists for: the page is scrolled, and the header has gone
	// up the screen with it instead of staying at the top.
	test("a header that scrolled away with the page is detached", () => {
		assert.strictEqual(
			headerIsDetached(deps({ scrollY: 500, headerTop: -500 })),
			true,
		);
	});

	test("a header holding the top is not", () => {
		assert.strictEqual(
			headerIsDetached(deps({ scrollY: 500, headerTop: 0 })),
			false,
		);
	});

	// At the top of the document a stuck header and an unstuck one sit in exactly
	// the same place, so there is nothing to compare. Guessing here would mean
	// rebuilding the sticky node on every single resume, working or not.
	test("nothing is claimed while the page is at the top", () => {
		assert.strictEqual(
			headerIsDetached(deps({ scrollY: 0, headerTop: 0 })),
			false,
		);
		assert.strictEqual(
			headerIsDetached(deps({ scrollY: 0, headerTop: -80 })),
			false,
		);
	});

	// Sub-pixel rounding and zoom mean the rect is never exactly on the line, and
	// a fraction of a pixel is not a broken header.
	test("sub-pixel drift is not a fault", () => {
		assert.strictEqual(
			headerIsDetached(deps({ scrollY: 500, headerTop: -1 })),
			false,
		);
		assert.strictEqual(
			headerIsDetached(deps({ scrollY: 500, headerTop: -1.5 })),
			false,
		);
	});

	test("a genuine break is well past the tolerance", () => {
		assert.strictEqual(
			headerIsDetached(deps({ scrollY: 500, headerTop: -3 })),
			true,
		);
	});

	// A header that sticks below something else (any future offset) is judged
	// against its own top, not against zero.
	test("it respects the header's own sticky offset", () => {
		assert.strictEqual(
			headerIsDetached(deps({ scrollY: 500, headerTop: 40, stickyTop: 40 })),
			false,
		);
		assert.strictEqual(
			headerIsDetached(deps({ scrollY: 500, headerTop: 0, stickyTop: 40 })),
			true,
		);
	});

	// While a modal pins the body the header is deliberately position:fixed. That
	// is not a fault, and rebuilding a sticky node underneath it would fight the
	// modal.
	test("a modal's deliberate pin is left alone", () => {
		assert.strictEqual(
			headerIsDetached(
				deps({ scrollY: 500, headerTop: -500, pinnedByModal: true }),
			),
			false,
		);
	});

	// Some browsers report a NaN rect for an element that isn't laid out yet.
	// Acting on that would rebuild the node for no reason.
	test("unmeasurable geometry claims nothing", () => {
		assert.strictEqual(
			headerIsDetached(deps({ scrollY: Number.NaN, headerTop: -500 })),
			false,
		);
		assert.strictEqual(
			headerIsDetached(deps({ scrollY: 500, headerTop: Number.NaN })),
			false,
		);
	});

	// A header BELOW where it sticks is a page mid-scroll-into-place, not a
	// broken one - never repair that.
	test("a header below its sticky point is not detached", () => {
		assert.strictEqual(
			headerIsDetached(deps({ scrollY: 500, headerTop: 30 })),
			false,
		);
	});
});

// The header is undetectable at the top of the page, so scrolling is the only
// event that can ever catch it - but NOT while the scroll is still running. On
// iOS the main thread's geometry lags the compositor during a flick, so a
// healthy sticky header reads exactly like a broken one. A field log showed
// every single detection taken mid-scroll, so the watch now waits for the page
// to settle and every fault is confirmed against a second reading.
// getBoundingClientRect() measures against the VISUAL viewport while sticky
// anchors to the LAYOUT one, so zooming or panning the two apart moves a
// perfectly healthy header to -offsetTop. A field log caught exactly this and
// the watchdog spent it running an impossible repair four times a second.
describe("headerIsDetached with the viewports panned apart", () => {
	test("a header at exactly -offsetTop is doing its job, not broken", () => {
		// The field numbers: 646-tall visual viewport 406px down a 1052 layout
		// viewport, header measured at -406.
		assert.strictEqual(
			headerIsDetached(
				deps({ scrollY: 864, headerTop: -406, visualOffsetTop: 406 }),
			),
			false,
		);
	});

	test("a genuinely detached header is still caught while zoomed", () => {
		// Gone up with the page rather than parked at the offset.
		assert.strictEqual(
			headerIsDetached(
				deps({ scrollY: 864, headerTop: -864, visualOffsetTop: 406 }),
			),
			true,
		);
	});

	test("without an offset the old comparison is unchanged", () => {
		assert.strictEqual(
			headerIsDetached(
				deps({ scrollY: 500, headerTop: -500, visualOffsetTop: 0 }),
			),
			true,
		);
	});
});

describe("scrollDecision", () => {
	test("a scroll far enough down the page is worth measuring", () => {
		assert.strictEqual(scrollDecision({ scrollY: 500 }), "judge");
	});

	test("says nothing at the top, where the question is unanswerable", () => {
		assert.strictEqual(scrollDecision({ scrollY: 0 }), "at-top");
		assert.strictEqual(
			scrollDecision({ scrollY: 1 }),
			"at-top",
			"within tolerance is still the top",
		);
	});

	test("a nonsense scroll position claims nothing", () => {
		assert.strictEqual(scrollDecision({ scrollY: Number.NaN }), "at-top");
	});
});

describe("detachmentConfirmed", () => {
	test("two readings at the same offset that agree is a real fault", () => {
		assert.strictEqual(
			detachmentConfirmed({
				before: { scrollY: 400, headerTop: -400 },
				after: { scrollY: 400, headerTop: -400 },
			}),
			true,
		);
	});

	test("a page that moved between readings proves nothing", () => {
		// The exact shape of the phantom faults in the field log: detected at
		// scrollY 69, re-measured at 149. Mid-flick, so unanswerable.
		assert.strictEqual(
			detachmentConfirmed({
				before: { scrollY: 69, headerTop: -69 },
				after: { scrollY: 149, headerTop: -149 },
			}),
			false,
		);
	});

	test("a header still moving at a settled offset is not confirmed", () => {
		// Same scroll offset but the rect is still catching up - the compositor
		// had not finished, so wait rather than tear the header apart.
		assert.strictEqual(
			detachmentConfirmed({
				before: { scrollY: 400, headerTop: -400 },
				after: { scrollY: 400, headerTop: -120 },
			}),
			false,
		);
	});

	test("sub-pixel disagreement is still a confirmation", () => {
		assert.strictEqual(
			detachmentConfirmed({
				before: { scrollY: 400, headerTop: -400 },
				after: { scrollY: 400, headerTop: -401 },
			}),
			true,
		);
	});
});
