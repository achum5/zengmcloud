import { assert, describe, test } from "vitest";
import {
	keyboardLikelyOpen,
	tickerMeasuredShift,
} from "./visualViewportHeader.ts";

// THE HEADER HAS NO TEST HERE BECAUSE IT HAS NO CODE HERE.
//
// It used to be pushed down by visualViewport.offsetTop. Five builds of that
// idea failed in the field, and the log that settled it has vv.height reporting
// 1052, 1052, then 646 on the same idle page with innerHeight 1052 throughout -
// so the readings being trusted were not true. The header is left alone now,
// and the only thing written to it is the empty transform that strips whatever
// an older build left behind. A genuinely detached header belongs to the
// watchdog, which confirms across two frames before it believes a reading.

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
