import { assert, describe, test } from "vitest";
import {
	headerLooksUnstuck,
	headerMeasuredShift,
	keyboardLikelyOpen,
	stickyIsBroken,
	tickerMeasuredShift,
} from "./visualViewportHeader.ts";

// THE HEADER IS STILL NOT PLACED FROM THE VIEWPORT.
//
// It used to be pushed down by visualViewport.offsetTop. Five builds of that
// idea failed in the field, and the log that settled it has vv.height reporting
// 1052, 1052, then 646 on the same idle page with innerHeight 1052 throughout -
// so the readings being trusted were not true.
//
// What it has now reads no viewport at all. It asks one question - is
// position:sticky working in this page - and if the answer is no, takes the
// header out of flow at the top of the viewport, which is where sticky would
// have held it.
//
// THE PREVIOUS BUILD OF THIS ANSWERED THE SAME QUESTION WITH A TRANSFORM AND
// BROKE SOMETHING ELSE. Translating an in-flow element down adds to scrollable
// overflow in WebKit, so the page grew by however far the header was pushed,
// which allowed more scroll, which pushed it further. Out of flow cannot do
// that, which is the whole reason for position:fixed here.
describe("stickyIsBroken", () => {
	const broken = (probe: { lift: number; possible: number } | undefined) =>
		stickyIsBroken({ probe });

	test("a probe lifted the whole way means sticky works", () => {
		assert.equal(broken({ lift: 100, possible: 100 }), false);
		assert.equal(broken({ lift: 99, possible: 100 }), false);
	});

	test("a probe that did not move means sticky is not engaging", () => {
		// The field reading: a probe that never left its static position while
		// the page was scrolled.
		assert.equal(broken({ lift: 0, possible: 18 }), true);
		assert.equal(broken({ lift: 0, possible: 640 }), true);
	});

	test("at the top of the document the two cases are the same place", () => {
		assert.equal(broken({ lift: 0, possible: 0 }), false);
		assert.equal(broken({ lift: 0, possible: 2 }), false);
	});

	// THE FLICKER THIS SIGNATURE EXISTS TO END. The probe is prepended to
	// #content, and the fixed fallback puts a header's height of padding there -
	// so with the fallback engaged and the page scrolled 30, a probe that is
	// not sticking at all reads +22, which the old absolute test read as
	// "sticky works". The fallback disengaged, the padding went, the probe read
	// -30, and it engaged again: every scroll frame, near the top of every page.
	// A lift is immune - the reference probe carries the same padding.
	test("REGRESSION: the fallback's own padding cannot fake a working probe", () => {
		// scrollY 30, 52px of fallback padding, sticky doing nothing.
		assert.equal(broken({ lift: 0, possible: 30 }), true);
	});

	test("a missing or nonsense reading is not evidence of a fault", () => {
		assert.equal(broken(undefined), false);
		assert.equal(broken({ lift: Number.NaN, possible: 100 }), false);
		assert.equal(broken({ lift: 0, possible: Number.NaN }), false);
	});
});

// WHAT PUTS THE HEADER BACK ON THE SCREEN once it is out of flow, on a device
// where position:fixed does not land where position:fixed should.
describe("headerMeasuredShift", () => {
	const shift = (o: Partial<Parameters<typeof headerMeasuredShift>[0]>) =>
		headerMeasuredShift({
			measuredTop: 0,
			currentShift: 0,
			pinned: true,
			layoutHeight: 1052,
			...o,
		});

	test("a pinned header already at the top is left alone", () => {
		assert.equal(shift({ measuredTop: 0 }), 0);
	});

	// The field report: fallback engaged, header position:fixed top:0, and it
	// measured -192 with the page scrolled 192 - the device places fixed
	// against a viewport it reports as panned by exactly the scroll offset.
	test("a header displaced by a phantom viewport offset is made up exactly", () => {
		assert.equal(shift({ measuredTop: -192 }), 192);
	});

	test("it converges in one step instead of chasing its own tail", () => {
		// Already corrected by 192 and now measuring 0: the correction is
		// working, so it is KEPT, not cancelled because the header looks right
		// while wearing it.
		assert.equal(shift({ measuredTop: 0, currentShift: 192 }), 192);
		// Still 40 short with 152 already applied: top up to 192, not 40 more.
		assert.equal(shift({ measuredTop: -40, currentShift: 152 }), 192);
	});

	// A transform on an IN-FLOW element counts toward scrollable overflow in
	// WebKit: an earlier build pushed the in-flow header down, which grew the
	// document, which allowed more scroll, which enlarged the push.
	test("an in-flow header is never transformed, however adrift it looks", () => {
		assert.equal(shift({ measuredTop: -192, pinned: false }), 0);
	});

	test("sub-pixel drift is not worth a transform", () => {
		assert.equal(shift({ measuredTop: -1 }), 0);
		assert.equal(shift({ measuredTop: 1 }), 0);
	});

	test("a correction bigger than the viewport is refused", () => {
		assert.equal(shift({ measuredTop: -2000, layoutHeight: 1052 }), 0);
	});

	test("unmeasurable geometry changes nothing", () => {
		assert.equal(shift({ measuredTop: undefined }), 0);
		assert.equal(shift({ layoutHeight: undefined }), 0);
		assert.equal(shift({ measuredTop: Number.NaN }), 0);
		assert.equal(shift({ layoutHeight: 0 }), 0);
	});
});

// The cheap pre-check, so a healthy device never pays for a probe at all.
describe("headerLooksUnstuck", () => {
	const looks = (o: Partial<Parameters<typeof headerLooksUnstuck>[0]>) =>
		headerLooksUnstuck({ headerTop: 0, parentTop: 0, scrollY: 100, ...o });

	test("a pinned header sits scrollY below the top of its parent", () => {
		for (const scrollY of [18, 100, 900]) {
			assert.equal(
				looks({ headerTop: 0, parentTop: -scrollY, scrollY }),
				false,
				`scrolled ${scrollY}`,
			);
		}
	});

	test("a header that is not sticking sits flush with its parent", () => {
		assert.equal(looks({ headerTop: -18, parentTop: -18, scrollY: 18 }), true);
	});

	// A header that has not been scrolled far enough to pin yet is sitting at
	// its own place in the page, not failing to stick.
	test("a header not yet scrolled past is not called unstuck", () => {
		// Static 90px down its parent, page only scrolled 50 - it pins at 90.
		assert.equal(looks({ headerTop: 40, parentTop: -50, scrollY: 50 }), false);
	});

	// And the same header once the page HAS scrolled past it: a pinned one would
	// read lift 100, so reading 90 means it stayed where it was.
	test("a header the page has scrolled past, still at its own place, is", () => {
		assert.equal(
			looks({ headerTop: -10, parentTop: -100, scrollY: 100 }),
			true,
		);
	});

	test("nothing to tell apart at the top of the document", () => {
		assert.equal(looks({ headerTop: 0, parentTop: 0, scrollY: 0 }), false);
	});

	test("a missing reading is not evidence of a fault", () => {
		assert.equal(looks({ headerTop: undefined }), false);
		assert.equal(looks({ parentTop: undefined }), false);
		assert.equal(looks({ scrollY: undefined }), false);
		assert.equal(looks({ headerTop: Number.NaN }), false);
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
