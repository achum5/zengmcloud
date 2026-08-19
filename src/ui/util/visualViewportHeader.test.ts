import { assert, describe, test } from "vitest";
import {
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
