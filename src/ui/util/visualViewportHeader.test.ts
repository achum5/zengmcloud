import { assert, describe, test } from "vitest";
import {
	headerStickyFallbackShift,
	keyboardLikelyOpen,
	tickerMeasuredShift,
} from "./visualViewportHeader.ts";

// THE HEADER IS STILL NOT PLACED FROM THE VIEWPORT.
//
// It used to be pushed down by visualViewport.offsetTop. Five builds of that
// idea failed in the field, and the log that settled it has vv.height reporting
// 1052, 1052, then 646 on the same idle page with innerHeight 1052 throughout -
// so the readings being trusted were not true.
//
// What it has now reads no viewport at all: how far the header sits inside its
// own parent, against how far the page is scrolled. Those agree whenever sticky
// is working, so the correction is zero and nothing is written - which is the
// whole reason it is allowed to exist after six failures. See
// headerStickyFallbackShift.
describe("headerStickyFallbackShift", () => {
	const shift = (o: Partial<Parameters<typeof headerStickyFallbackShift>[0]>) =>
		headerStickyFallbackShift({
			headerTop: 0,
			parentTop: 0,
			currentShift: 0,
			scrollY: 0,
			layoutHeight: 1000,
			...o,
		});

	// The case that matters most: a working header must never be touched. This
	// is the objection that got the sixth build removed - it rode down the page
	// on every scroll and snapped back at every stop.
	test("a header that is sticking correctly is left alone", () => {
		for (const scrollY of [1, 18, 200, 950, 4000]) {
			// Pinned: the header sits scrollY below the top of its parent, because
			// the parent has scrolled up and the header has not.
			assert.equal(
				shift({ headerTop: 0, parentTop: -scrollY, scrollY }),
				0,
				`scrolled ${scrollY}`,
			);
		}
	});

	test("at the top of the document nothing is written either", () => {
		assert.equal(shift({ headerTop: 0, parentTop: 0, scrollY: 0 }), 0);
	});

	// The field report: lift 0 against a scroll of 18, and a fresh sticky probe
	// adrift by the same 18.
	test("a header that is not sticking is pushed back to the top", () => {
		assert.equal(shift({ headerTop: -18, parentTop: -18, scrollY: 18 }), 18);
		assert.equal(
			shift({ headerTop: -640, parentTop: -640, scrollY: 640 }),
			640,
		);
	});

	test("it converges in one step rather than chasing its own answer", () => {
		// Same broken header, re-measured with the correction already on it: the
		// rect has moved down by 18, and the answer must still be 18.
		assert.equal(
			shift({ headerTop: 0, parentTop: -18, currentShift: 18, scrollY: 18 }),
			18,
		);
	});

	test("sub-pixel differences are rounding, not misplacement", () => {
		assert.equal(shift({ headerTop: -1, parentTop: -1, scrollY: 1 }), 0);
	});

	test("it never pulls the header upward", () => {
		// A header sitting LOWER than the scroll can explain is a different
		// fault - a stale node - and belongs to the watchdog's ladder.
		assert.equal(shift({ headerTop: 60, parentTop: -18, scrollY: 18 }), 0);
	});

	test("a correction taller than the viewport is refused", () => {
		assert.equal(
			shift({
				headerTop: -5000,
				parentTop: -5000,
				scrollY: 5000,
				layoutHeight: 1000,
			}),
			0,
		);
	});

	test("missing or nonsense readings write nothing", () => {
		assert.equal(shift({ headerTop: undefined }), 0);
		assert.equal(shift({ parentTop: undefined }), 0);
		assert.equal(shift({ scrollY: undefined }), 0);
		assert.equal(shift({ headerTop: Number.NaN, scrollY: 18 }), 0);
		assert.equal(shift({ scrollY: Number.POSITIVE_INFINITY }), 0);
		assert.equal(
			shift({
				headerTop: -18,
				parentTop: -18,
				scrollY: 18,
				currentShift: Number.NaN,
			}),
			0,
		);
	});
});

// THE TICKER, MEASURED RATHER THAN PREDICTED.
//
// Four builds computed this from visualViewport and all four were wrong in the
// field. The last two put the bar's bottom at exactly vv.height - 646 and 636,
// precisely what the formula asked for - and the user reported it sitting in
// the MIDDLE of the screen both times. So vv.height is not the bottom of the
// screen here, and the target is documentElement.clientHeight, which is where
// the bar sat in every report that drew no complaint.
describe("tickerMeasuredShift", () => {
	// The common case, and the state of every quiet report: rects are
	// layout-relative, sticky already has the bar on the foot of the viewport,
	// and nothing should be written at all.
	test("a bar already at the foot is left alone", () => {
		assert.strictEqual(
			tickerMeasuredShift({
				measuredBottom: 1083,
				currentShift: 0,
				layoutHeight: 1083,
			}),
			0,
		);
	});

	// Rects relative to a visual viewport that starts 300 down: sticky's foot
	// measures short by exactly that, and the correction makes it up - without
	// reading offsetTop, which is never consulted.
	test("a bar short by a viewport offset is made up exactly", () => {
		assert.strictEqual(
			tickerMeasuredShift({
				measuredBottom: 752,
				currentShift: 0,
				layoutHeight: 1052,
			}),
			300,
		);
	});

	// THE REGRESSION THIS CLOSES. Both field reports had the bar lifted onto
	// vv.height and the user said mid-screen. Given that state, the correction
	// must undo its own lift and put the bar back on the foot.
	test("it undoes a previous build's lift", () => {
		assert.strictEqual(
			tickerMeasuredShift({
				measuredBottom: 636,
				currentShift: -447,
				layoutHeight: 1083,
			}),
			0,
		);
		assert.strictEqual(
			tickerMeasuredShift({
				measuredBottom: 646,
				currentShift: -85,
				layoutHeight: 1052,
			}),
			321,
		);
	});

	// The state in the report that ended the header shift: a viewport claiming
	// to be 646 tall inside a 1052 layout viewport, with the bar measured on the
	// foot of the layout viewport. The claim is a ghost, and the ticker must not
	// move for it - it never reads vv.height, so it does not.
	test("a lying viewport does not move a bar that is already right", () => {
		assert.strictEqual(
			tickerMeasuredShift({
				measuredBottom: 1052,
				currentShift: 229,
				layoutHeight: 1052,
			}),
			229,
		);
	});

	// Converges in one step rather than chasing its own tail: apply the shift
	// it returns, measure again, and it asks for nothing more.
	test("it settles instead of oscillating", () => {
		const layoutHeight = 1052;
		let shift = 0;
		let bottom = 752;
		for (let i = 0; i < 3; i++) {
			const next = tickerMeasuredShift({
				measuredBottom: bottom,
				currentShift: shift,
				layoutHeight,
			});
			// Applying a shift moves the measured bottom by the delta.
			bottom += next - shift;
			shift = next;
		}
		assert.strictEqual(shift, 300);
		assert.strictEqual(bottom, 1052);
	});

	test("sub-pixel drift is not worth a transform", () => {
		assert.strictEqual(
			tickerMeasuredShift({
				measuredBottom: 1082.4,
				currentShift: 0,
				layoutHeight: 1083,
			}),
			0,
		);
	});

	// A bar adrift in the document is the watchdog's to rebuild; papering over
	// it here would hide a real fault behind a correct-looking position.
	test("a correction bigger than the viewport is refused", () => {
		assert.strictEqual(
			tickerMeasuredShift({
				measuredBottom: -4000,
				currentShift: 0,
				layoutHeight: 1052,
			}),
			0,
		);
	});

	test("unmeasurable geometry changes nothing", () => {
		assert.strictEqual(
			tickerMeasuredShift({
				measuredBottom: undefined,
				currentShift: 0,
				layoutHeight: 1052,
			}),
			0,
		);
		assert.strictEqual(
			tickerMeasuredShift({
				measuredBottom: Number.NaN,
				currentShift: 0,
				layoutHeight: 1052,
			}),
			0,
		);
		assert.strictEqual(
			tickerMeasuredShift({
				measuredBottom: 700,
				currentShift: 0,
				layoutHeight: undefined,
			}),
			0,
		);
	});

	// The keyboard needs no special case: it does not change clientHeight, so a
	// bar already at the foot stays there rather than hoisting over the typing.
	test("the keyboard needs no special case", () => {
		assert.strictEqual(
			tickerMeasuredShift({
				measuredBottom: 1083,
				currentShift: 0,
				layoutHeight: 1083,
			}),
			0,
		);
	});
});

// The one thing the viewport numbers cannot tell apart, asked of the page
// instead of guessed from geometry.
describe("keyboardLikelyOpen", () => {
	const el = (tagName: string, extra: Record<string, unknown> = {}) =>
		({ tagName, ...extra }) as unknown as Element;

	test("a text field means a keyboard", () => {
		assert.strictEqual(keyboardLikelyOpen(el("INPUT", { type: "text" })), true);
		assert.strictEqual(keyboardLikelyOpen(el("TEXTAREA")), true);
		assert.strictEqual(
			keyboardLikelyOpen(el("INPUT", { type: "number" })),
			true,
		);
	});

	test("an input nobody types into does not", () => {
		assert.strictEqual(
			keyboardLikelyOpen(el("INPUT", { type: "checkbox" })),
			false,
		);
		assert.strictEqual(
			keyboardLikelyOpen(el("INPUT", { type: "range" })),
			false,
		);
	});

	test("a focused contenteditable counts", () => {
		assert.strictEqual(
			keyboardLikelyOpen(el("DIV", { isContentEditable: true })),
			true,
		);
	});

	test("the ordinary case is nothing focused at all", () => {
		assert.strictEqual(keyboardLikelyOpen(null), false);
		assert.strictEqual(keyboardLikelyOpen(undefined), false);
		assert.strictEqual(keyboardLikelyOpen(el("BODY")), false);
		assert.strictEqual(keyboardLikelyOpen(el("BUTTON")), false);
	});
});
