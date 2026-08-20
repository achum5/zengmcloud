import { assert, describe, test } from "vitest";
import {
	headerConfirmsOffset,
	headerVisualShift,
	keyboardLikelyOpen,
	tickerVisualShift,
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

describe("tickerVisualShift", () => {
	// THE GHOST, from two field reports with identical geometry and the second
	// one carrying the proof: vv 646/1052 on a page whose content visibly
	// rendered far below layout y 646, once at offsetTop 0 and once at
	// offsetTop 69 - and in both, vv width 518 = the full layout width = the
	// full screen at that scale, with nothing focused. A keyboard-sized height
	// on a full-width viewport with no keyboard is a resume artifact, and the
	// bar must be left where sticky put it.
	test("the full-width ghost is not believed, panned or not", () => {
		for (const offsetTop of [0, 69]) {
			assert.strictEqual(
				tickerVisualShift({
					visualHeight: 646,
					layoutHeight: 1052,
					offsetTop,
					visualWidth: 518,
					layoutWidth: 518,
				}),
				0,
			);
		}
	});

	// Genuine pinch-zoom narrows BOTH axes - that is what tells it apart from
	// the ghost, which is a keyboard's shadow and shrinks height alone. Here
	// the lift applies: sticky holds the layout viewport's foot, which is
	// below what the user can see.
	test("a genuinely pinched viewport gets the lift", () => {
		assert.strictEqual(
			tickerVisualShift({
				visualHeight: 646,
				layoutHeight: 1052,
				offsetTop: 57,
				visualWidth: 400,
				layoutWidth: 518,
			}),
			// 646 - (1052 - 57): up to the visible foot.
			-349,
		);
	});

	test("pinched but sitting at the exact top stands down, like the header", () => {
		assert.strictEqual(
			tickerVisualShift({
				visualHeight: 646,
				layoutHeight: 1052,
				offsetTop: 0,
				visualWidth: 400,
				layoutWidth: 518,
			}),
			0,
		);
	});

	test("an unzoomed page is untouched", () => {
		// Which is every ordinary page on every device - the common path does
		// nothing at all.
		assert.strictEqual(
			tickerVisualShift({
				visualHeight: 1052,
				layoutHeight: 1052,
				offsetTop: 0,
				visualWidth: 518,
				layoutWidth: 518,
			}),
			0,
		);
	});

	// Sticky already holds the foot of the layout viewport; pushing past it can
	// only take the bar somewhere worse.
	test("never moves the bar downward", () => {
		assert.strictEqual(
			tickerVisualShift({
				visualHeight: 1200,
				layoutHeight: 1052,
				offsetTop: 10,
				visualWidth: 400,
				layoutWidth: 518,
			}),
			0,
		);
	});

	test("the keyboard is left alone", () => {
		// Hoisting the bar over it would cover whatever is being typed into.
		assert.strictEqual(
			tickerVisualShift({
				visualHeight: 500,
				layoutHeight: 1052,
				offsetTop: 100,
				visualWidth: 400,
				layoutWidth: 518,
				keyboardOpen: true,
			}),
			0,
		);
	});

	test("unreadable geometry changes nothing", () => {
		const pinched = {
			visualHeight: 646,
			layoutHeight: 1052,
			offsetTop: 57,
			visualWidth: 400,
			layoutWidth: 518,
		};
		assert.strictEqual(
			tickerVisualShift({ ...pinched, visualHeight: undefined }),
			0,
		);
		assert.strictEqual(
			tickerVisualShift({ ...pinched, visualHeight: Number.NaN }),
			0,
		);
		assert.strictEqual(
			tickerVisualShift({ ...pinched, layoutHeight: undefined }),
			0,
		);
		assert.strictEqual(
			tickerVisualShift({ ...pinched, visualWidth: undefined }),
			0,
		);
		assert.strictEqual(
			tickerVisualShift({ ...pinched, layoutWidth: Number.NaN }),
			0,
		);
		assert.strictEqual(tickerVisualShift({ ...pinched, visualHeight: 0 }), 0);
	});
});

// THE HEADER AS WITNESS. It is sticky at top 0, so on a real pan its rect
// reads -offsetTop before its own correction; when offsetTop is a ghost the
// rects know nothing about it and the same reading is 0.
describe("headerConfirmsOffset", () => {
	// The /draft report: offsetTop 406, header carrying translateY(406px) and
	// measuring 0 - so sticky alone had it at -406, and the pan is real.
	test("a real pan is confirmed", () => {
		assert.strictEqual(headerConfirmsOffset(0, 406, 406), true);
	});

	// The ghost: offsetTop 240 with the header untransformed and reading 0, so
	// sticky put it at 0, not -240.
	test("a phantom offset is not confirmed", () => {
		assert.strictEqual(headerConfirmsOffset(0, 0, 240), false);
	});

	test("subpixel rounding still confirms", () => {
		assert.strictEqual(headerConfirmsOffset(1.4, 406, 406), true);
		assert.strictEqual(headerConfirmsOffset(-1.4, 406, 406), true);
	});

	test("a detached header confirms nothing", () => {
		// The modal-unpin case: scrolled away with the page, offsetTop 0.
		assert.strictEqual(headerConfirmsOffset(-82, 0, 0), false);
	});

	test("no measurement is not a confirmation", () => {
		assert.strictEqual(headerConfirmsOffset(undefined, 0, 406), false);
		assert.strictEqual(headerConfirmsOffset(Number.NaN, 0, 406), false);
	});

	test("an unpanned page confirms trivially, and asks for no lift anyway", () => {
		assert.strictEqual(headerConfirmsOffset(0, 0, 0), true);
	});
});

// EITHER WITNESS, and the ghost passes neither. Pinned against all four field
// states so a future change cannot quietly re-admit the ghost.
describe("tickerVisualShift trusts a confirmed offset without a pinch", () => {
	// /draft: offsetTop 406 on a 1052 layout viewport, FULL width - so the
	// width test alone refuses - but the header proves the pan. Here the
	// visual viewport is flush to the foot (406 + 646 = 1052), so the honest
	// answer is still zero; the point is that it is computed, not refused.
	test("the flush-to-bottom field case wants no lift either way", () => {
		assert.strictEqual(
			tickerVisualShift({
				visualHeight: 646,
				layoutHeight: 1052,
				offsetTop: 406,
				visualWidth: 518,
				layoutWidth: 518,
				headerConfirms: true,
			}),
			0,
		);
	});

	// Panned to the MIDDLE of that same page, which the old width-only gate
	// would have refused - leaving the bar hundreds of pixels off screen.
	test("a confirmed mid-pan gets its lift despite a full width", () => {
		assert.strictEqual(
			tickerVisualShift({
				visualHeight: 646,
				layoutHeight: 1052,
				offsetTop: 200,
				visualWidth: 518,
				layoutWidth: 518,
				headerConfirms: true,
			}),
			// 646 - (1052 - 200): up to the visible foot.
			-206,
		);
	});

	test("the ghost is still refused, confirmed by neither witness", () => {
		for (const offsetTop of [0, 69, 240]) {
			assert.strictEqual(
				tickerVisualShift({
					visualHeight: 646,
					layoutHeight: 1052,
					offsetTop,
					visualWidth: 518,
					layoutWidth: 518,
					headerConfirms: false,
				}),
				0,
			);
		}
	});

	test("a pinch still works on its own, with no header to ask", () => {
		assert.strictEqual(
			tickerVisualShift({
				visualHeight: 646,
				layoutHeight: 1052,
				offsetTop: 57,
				visualWidth: 400,
				layoutWidth: 518,
			}),
			-349,
		);
	});

	test("the keyboard still overrides both witnesses", () => {
		assert.strictEqual(
			tickerVisualShift({
				visualHeight: 500,
				layoutHeight: 1052,
				offsetTop: 200,
				visualWidth: 518,
				layoutWidth: 518,
				headerConfirms: true,
				keyboardOpen: true,
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
