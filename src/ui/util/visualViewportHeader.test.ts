import { assert, describe, test } from "vitest";
import {
	headerVisualShift,
	keyboardLikelyOpen,
	tickerMeasuredShift,
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
