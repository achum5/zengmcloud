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
	// The field report that settled the direction, from an installed PWA on an
	// iPhone: offsetTop 57 on a 1052-tall layout viewport with a 646-tall visual
	// viewport. The bar had been pushed DOWN by 57, putting its bottom at 1053 -
	// and it was not on screen. A bar at 1053 on a viewport ending at 1052 would
	// be sitting on the bottom edge in full view, so the viewport does not end
	// at 1052. It ends at 646, and the bar was 407px below the screen.
	test("lifts the bar to the foot of what the user can see", () => {
		assert.strictEqual(
			tickerVisualShift({
				visualHeight: 646,
				layoutHeight: 1052,
				offsetTop: 57,
			}),
			// 646 - (1052 - 57): up, not down.
			-349,
		);
	});

	test("the earlier report lands on the same rule", () => {
		assert.strictEqual(
			tickerVisualShift({
				visualHeight: 646,
				layoutHeight: 1052,
				offsetTop: 300,
			}),
			-106,
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
				offsetTop: 0,
			}),
			0,
		);
	});

	test("the keyboard is left alone", () => {
		// It shrinks the visual viewport exactly like a zoom, but hoisting the
		// bar would put it over whatever is being typed into.
		assert.strictEqual(
			tickerVisualShift({
				visualHeight: 500,
				layoutHeight: 1052,
				offsetTop: 0,
				keyboardOpen: true,
			}),
			0,
		);
	});

	test("unreadable geometry changes nothing", () => {
		assert.strictEqual(
			tickerVisualShift({
				visualHeight: undefined,
				layoutHeight: 1052,
				offsetTop: 0,
			}),
			0,
		);
		assert.strictEqual(
			tickerVisualShift({
				visualHeight: Number.NaN,
				layoutHeight: 1052,
				offsetTop: 0,
			}),
			0,
		);
		assert.strictEqual(
			tickerVisualShift({
				visualHeight: 646,
				layoutHeight: undefined,
				offsetTop: 0,
			}),
			0,
		);
		assert.strictEqual(
			tickerVisualShift({ visualHeight: 0, layoutHeight: 1052, offsetTop: 0 }),
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
