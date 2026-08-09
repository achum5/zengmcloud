import { assert, describe, test } from "vitest";
import { headerIsDetached, scrollDecision } from "./stickyHeaderWatchdog.ts";

const deps = ({
	scrollY = 500,
	headerTop = 0,
	stickyTop = 0,
	pinnedByModal = false,
}: {
	scrollY?: number;
	headerTop?: number;
	stickyTop?: number;
	pinnedByModal?: boolean;
}) => ({
	scrollY: () => scrollY,
	headerTop: () => headerTop,
	stickyTop: () => stickyTop,
	pinnedByModal: () => pinnedByModal,
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
// event that can ever catch it. This watch has been narrowed twice and the bug
// came back both times: first it was torn down on a timer, then it disarmed
// after a single answer - which assumed only a resume can break a header, and
// that a repair reporting success has actually held. Neither is true. It is now
// permanent, with throttling rather than disarming keeping the cost down.
describe("scrollDecision", () => {
	test("a scroll far enough down the page is worth measuring", () => {
		assert.strictEqual(
			scrollDecision({ scrollY: 500, now: 1000, lastCheck: undefined }),
			"judge",
		);
	});

	test("keeps judging on later scrolls, however many have come before", () => {
		// The regression that brought this bug back: one answer per resume left a
		// header broken by anything else with nobody watching.
		assert.strictEqual(
			scrollDecision({ scrollY: 500, now: 9_999_999, lastCheck: 1000 }),
			"judge",
		);
	});

	test("throttles bursts of scroll events", () => {
		assert.strictEqual(
			scrollDecision({ scrollY: 500, now: 1100, lastCheck: 1000 }),
			"too-soon",
		);
	});

	test("the throttle interval is the boundary", () => {
		assert.strictEqual(
			scrollDecision({
				scrollY: 500,
				now: 1000,
				lastCheck: 900,
				minIntervalMs: 100,
			}),
			"judge",
		);
		assert.strictEqual(
			scrollDecision({
				scrollY: 500,
				now: 999,
				lastCheck: 900,
				minIntervalMs: 100,
			}),
			"too-soon",
		);
	});

	test("says nothing at the top, where the question is unanswerable", () => {
		assert.strictEqual(
			scrollDecision({ scrollY: 0, now: 1000, lastCheck: undefined }),
			"at-top",
		);
		assert.strictEqual(
			scrollDecision({ scrollY: 1, now: 1000, lastCheck: undefined }),
			"at-top",
			"within tolerance is still the top",
		);
	});

	test("being at the top does not burn the throttle", () => {
		// "at-top" must not count as a check, or scrolling up and back down could
		// silently skip the one measurement that would have caught the fault.
		assert.strictEqual(
			scrollDecision({ scrollY: 0, now: 1000, lastCheck: 999 }),
			"at-top",
		);
	});

	test("a nonsense scroll position claims nothing", () => {
		assert.strictEqual(
			scrollDecision({ scrollY: Number.NaN, now: 1000, lastCheck: undefined }),
			"at-top",
		);
	});
});
