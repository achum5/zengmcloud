import { assert, describe, test } from "vitest";
import {
	bottomBarIsDetached,
	detachmentConfirmed,
	headerIsDetached,
	scrollDecision,
} from "./stickyHeaderWatchdog.ts";
import { tickerVisualShift } from "./visualViewportHeader.ts";

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
				before: { scrollY: 400, edge: -400 },
				after: { scrollY: 400, edge: -400 },
			}),
			true,
		);
	});

	test("a page that moved between readings proves nothing", () => {
		// The exact shape of the phantom faults in the field log: detected at
		// scrollY 69, re-measured at 149. Mid-flick, so unanswerable.
		assert.strictEqual(
			detachmentConfirmed({
				before: { scrollY: 69, edge: -69 },
				after: { scrollY: 149, edge: -149 },
			}),
			false,
		);
	});

	test("a header still moving at a settled offset is not confirmed", () => {
		// Same scroll offset but the rect is still catching up - the compositor
		// had not finished, so wait rather than tear the header apart.
		assert.strictEqual(
			detachmentConfirmed({
				before: { scrollY: 400, edge: -400 },
				after: { scrollY: 400, edge: -120 },
			}),
			false,
		);
	});

	test("sub-pixel disagreement is still a confirmation", () => {
		assert.strictEqual(
			detachmentConfirmed({
				before: { scrollY: 400, edge: -400 },
				after: { scrollY: 400, edge: -401 },
			}),
			true,
		);
	});
});

// THE BOTTOM TICKER. Same disease as the header, different edge: what has to
// hold still is its bottom against the foot of the viewport.
const bottomDeps = ({
	barBottom = 800,
	layoutHeight = 800,
	pinnedByModal = false,
	visualOffsetTop = 0,
}: {
	barBottom?: number;
	layoutHeight?: number;
	pinnedByModal?: boolean;
	visualOffsetTop?: number;
}) => ({
	barBottom: () => barBottom,
	layoutHeight: () => layoutHeight,
	pinnedByModal: () => pinnedByModal,
	visualOffsetTop: () => visualOffsetTop,
});

describe("bottomBarIsDetached", () => {
	test("a bar sitting on the foot of the viewport is fine", () => {
		assert.strictEqual(bottomBarIsDetached(bottomDeps({})), false);
	});

	// The reported failure: the ticker scrolled up the page with the document and
	// stayed floating in the middle of the screen.
	test("a bar that floated up the page is detached", () => {
		assert.strictEqual(
			bottomBarIsDetached(bottomDeps({ barBottom: 430, layoutHeight: 800 })),
			true,
		);
	});

	// A stale compositor rule pins the bar where it last was, and the page can
	// then scroll either way underneath it.
	test("a bar left below the viewport is detached too", () => {
		assert.strictEqual(
			bottomBarIsDetached(bottomDeps({ barBottom: 1100, layoutHeight: 800 })),
			true,
		);
	});

	// iOS moves the viewport height by a few pixels while the URL bar collapses.
	// That is the browser working, not the bar breaking.
	test("a few pixels of viewport churn is not a fault", () => {
		assert.strictEqual(
			bottomBarIsDetached(bottomDeps({ barBottom: 795, layoutHeight: 800 })),
			false,
		);
	});

	// Unlike the header, a bottom bar is just as measurable at the top of the
	// document as anywhere else - there is no ambiguous position.
	test("it answers at the top of the document, where the header cannot", () => {
		assert.strictEqual(
			bottomBarIsDetached(bottomDeps({ barBottom: 200, layoutHeight: 800 })),
			true,
		);
	});

	// getBoundingClientRect reports against the visual viewport while fixed
	// positions against the layout one, so a healthy bar on a panned page reads
	// short by exactly the offset between them.
	test("a bar short by exactly the visual offset is doing its job", () => {
		assert.strictEqual(
			bottomBarIsDetached(
				bottomDeps({
					barBottom: 646,
					layoutHeight: 1052,
					visualOffsetTop: 406,
				}),
			),
			false,
		);
	});

	test("a genuinely detached bar is still caught while panned", () => {
		assert.strictEqual(
			bottomBarIsDetached(
				bottomDeps({
					barBottom: 300,
					layoutHeight: 1052,
					visualOffsetTop: 406,
				}),
			),
			true,
		);
	});

	// The repair ladder nudges the scroll position, which is the last thing a
	// modal-pinned page needs. The unpin check picks it up afterwards.
	test("a modal's pin is left alone", () => {
		assert.strictEqual(
			bottomBarIsDetached(
				bottomDeps({ barBottom: 200, layoutHeight: 800, pinnedByModal: true }),
			),
			false,
		);
	});

	test("unmeasurable geometry claims nothing", () => {
		assert.strictEqual(
			bottomBarIsDetached(bottomDeps({ barBottom: Number.NaN })),
			false,
		);
		assert.strictEqual(
			bottomBarIsDetached(bottomDeps({ layoutHeight: 0 })),
			false,
		);
	});
});

// THE STANDING CORRECTION, as opposed to the repair. Sticky anchors to the
// layout viewport, so when the visual viewport sits inside it both bars are
// parked outside what the user can see, behaving perfectly correctly.
describe("tickerVisualShift", () => {
	// The field log that diagnosed the header, read at the other end: a 646-tall
	// visual viewport 406px down a 1052-tall layout viewport leaves no gap below
	// it (406 + 646 = 1052), so the ticker is already where it can be seen.
	test("no gap below the visible area means no shift", () => {
		assert.strictEqual(
			tickerVisualShift({
				offsetTop: 406,
				visualHeight: 646,
				layoutHeight: 1052,
			}),
			0,
		);
	});

	test("a bar parked below the visible area is pulled up by the gap", () => {
		assert.strictEqual(
			tickerVisualShift({
				offsetTop: 100,
				visualHeight: 600,
				layoutHeight: 800,
			}),
			-100,
		);
	});

	// The keyboard shortens the visual viewport without moving it. Hoisting the
	// ticker above the keyboard would put it over whatever is being typed into.
	test("the keyboard is left alone", () => {
		assert.strictEqual(
			tickerVisualShift({ offsetTop: 0, visualHeight: 400, layoutHeight: 800 }),
			0,
		);
	});

	test("agreeing viewports change nothing", () => {
		assert.strictEqual(
			tickerVisualShift({ offsetTop: 0, visualHeight: 800, layoutHeight: 800 }),
			0,
		);
	});

	test("unmeasurable geometry changes nothing", () => {
		assert.strictEqual(
			tickerVisualShift({
				offsetTop: undefined,
				visualHeight: undefined,
				layoutHeight: 800,
			}),
			0,
		);
	});
});
