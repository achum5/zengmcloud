// Keep the sticky bars inside the part of the page the user can actually see.
//
// `position: sticky` sticks to the LAYOUT viewport. Pinch-zoom, or anything else
// that pans the VISUAL viewport within it, moves what you can see without moving
// where sticky sticks - so the header parks itself off the top of the screen and
// stays there, behaving perfectly correctly the whole time.
//
// A field log from an installed PWA caught it exactly: scale 0.85, a 646-tall
// visual viewport sitting 406px down a 1052-tall layout viewport, and the header
// measured at -406. That is not a broken header and no amount of rebuilding the
// sticky node can help; the fix is to put the header back where it can be seen.
//
// THE BOTTOM TICKER HAS THE SAME PROBLEM AT THE OTHER END, and it does need the
// mirrored answer: it holds the foot of the LAYOUT viewport, which on a zoomed
// page is BELOW the foot of what the user can see, so it comes up.
//
// Nothing happens at all in the normal case, where the two viewports agree. Note
// that this runs on every scroll, not only on resume - a standing correction
// rather than a repair, which is why it is separate from the watchdog.

const HEADER_SELECTOR = ".navbar-border.sticky-top";
const TICKER_SELECTOR = ".league-ticker";

// How far to push the header down so it sits at the top of the visible area.
// Never negative: if the visual viewport is somehow above the layout viewport,
// leaving the header alone is the safe answer.
export const headerVisualShift = (offsetTop: number | undefined): number => {
	if (
		offsetTop === undefined ||
		!Number.isFinite(offsetTop) ||
		offsetTop <= 0
	) {
		return 0;
	}
	return Math.round(offsetTop);
};

// WHERE THE TICKER GOES, MEASURED INSTEAD OF PREDICTED.
//
// Four builds of this correction computed the answer from visualViewport, and
// all four were wrong in the field. The reason is now beyond argument: every
// one of them treated `vv.height` as the bottom of the screen, and the last
// two reports put the bar's bottom at exactly 646 and exactly 636 - precisely
// what that formula asked for - with the user reporting the bar sitting in the
// MIDDLE of the screen both times. vv.height is not the bottom of the screen
// on this device, and no arithmetic over a number that is not true can produce
// a true answer.
//
// What IS true, in every report where the bar looked right, is that its bottom
// sat at documentElement.clientHeight. So that is the target, and the way to
// hit it is to measure where the bar actually is and move it the difference -
// no viewport model at all:
//
//     shift = clientHeight - (measured bottom with our own shift removed)
//
// This is correct in both coordinate systems without having to know which one
// is in force, which is the thing that defeated every previous attempt. When
// rects are layout-relative and sticky is healthy the measurement already
// equals clientHeight, so the shift is zero and nothing is touched - the
// common case, and the state of every report that drew no complaint. When
// rects are visual-viewport-relative the measurement comes up short by the
// offset and the shift makes it up exactly, without ever reading offsetTop.
// When the viewport is lying about its height, the lie is simply not consulted.
//
// The keyboard needs no special case either: it does not change clientHeight,
// so the target does not move and the bar stays where it is instead of
// hoisting itself over what is being typed.
export const tickerMeasuredShift = ({
	measuredBottom,
	currentShift,
	layoutHeight,
}: {
	// getBoundingClientRect().bottom of the ticker, as it renders right now.
	measuredBottom: number | undefined;
	// The correction currently on it, subtracted back out so this converges in
	// one step instead of chasing its own tail.
	currentShift: number;
	// documentElement.clientHeight - the foot of the layout viewport, which is
	// where a correctly placed bar has sat in every report that looked right.
	layoutHeight: number | undefined;
}): number => {
	if (
		measuredBottom === undefined ||
		layoutHeight === undefined ||
		!Number.isFinite(measuredBottom) ||
		!Number.isFinite(layoutHeight) ||
		layoutHeight <= 0
	) {
		return 0;
	}
	const shift = layoutHeight - (measuredBottom - currentShift);
	// Sub-pixel differences are rounding, not misplacement, and writing a
	// transform for them only churns the compositor.
	if (Math.abs(shift) < 2) {
		return 0;
	}
	// A correction larger than the viewport is not a viewport offset, it is a
	// bar that has come adrift - which is the watchdog's job to rebuild, not
	// this one's to paper over.
	if (Math.abs(shift) > layoutHeight) {
		return 0;
	}
	return Math.round(shift);
};

// Is the software keyboard (probably) up?
//
// The ticker no longer needs to ask - it measures its own position, and a
// keyboard does not move the target (see tickerMeasuredShift). The chat drawer
// still does: its lift over the keyboard is the one correction that genuinely
// depends on the keyboard being there, and geometry alone cannot tell a
// keyboard from a zoom. The page knows something the viewport does not - a
// keyboard only appears when something focusable by text is focused.
export const keyboardLikelyOpen = (
	active: Element | null | undefined,
): boolean => {
	if (!active) {
		return false;
	}
	const tag = active.tagName;
	if (tag === "TEXTAREA") {
		return true;
	}
	if (tag === "INPUT") {
		// A checkbox or a button is an <input> that no keyboard ever opens for.
		const type = (active as HTMLInputElement).type;
		return (
			type !== "checkbox" &&
			type !== "radio" &&
			type !== "button" &&
			type !== "submit" &&
			type !== "reset" &&
			type !== "range" &&
			type !== "color" &&
			type !== "file"
		);
	}
	return (active as HTMLElement).isContentEditable === true;
};

// The translateY currently written on an element by applyHeaderShift, in px.
// Parsed from the inline style we ourselves wrote rather than from a computed
// matrix, so it is exactly the number to subtract back out and nothing else.
export const currentShiftOf = (element: HTMLElement): number => {
	const match = /translateY\((-?[\d.]+)px\)/.exec(element.style.transform);
	const value = match ? Number.parseFloat(match[1]!) : 0;
	return Number.isFinite(value) ? value : 0;
};

export const applyHeaderShift = (
	element: HTMLElement | null,
	shift: number,
) => {
	if (!element) {
		return;
	}
	// Clear rather than write "translateY(0px)": an identity transform still
	// makes the element a containing block for fixed descendants, which would
	// quietly change how anything positioned inside it behaves.
	element.style.transform = shift === 0 ? "" : `translateY(${shift}px)`;
};

// Recompute both shifts against the viewports as they are RIGHT NOW and write
// them (or clear them) synchronously. This is what every event handler below
// does on a frame; exported bare because a resume needs it on demand.
//
// The stale-shift trap it closes: suspend the app while a shift is applied, and
// resume can restore the exact same viewport numbers - so no visualViewport
// event ever fires, the pre-suspend translateY stays on the bar, and the
// watchdog's position-toggling ladder cannot remove it (it repairs `position`,
// not `transform`). The ticker then sits mid-page, provably "detached", and
// unrepairable forever. The header got away with the same hole only because its
// shift pushes it DOWN into view, where being stale is much less visible.
export const resyncStickyBarShifts = () => {
	const vv = window.visualViewport;
	applyHeaderShift(
		document.querySelector<HTMLElement>(HEADER_SELECTOR),
		headerVisualShift(vv?.offsetTop),
	);

	const ticker = document.querySelector<HTMLElement>(TICKER_SELECTOR);
	if (!ticker) {
		return;
	}
	// The self-placement mode used to live here and is gone; clear anything a
	// previous build of it left behind so a bar cannot stay pinned mid-page.
	if (ticker.style.position === "fixed") {
		ticker.style.position = "";
		ticker.style.top = "";
		ticker.style.bottom = "";
		ticker.style.left = "";
		ticker.style.right = "";
	}
	applyHeaderShift(
		ticker,
		tickerMeasuredShift({
			measuredBottom: ticker.getBoundingClientRect().bottom,
			currentShift: currentShiftOf(ticker),
			layoutHeight:
				document.documentElement?.clientHeight || window.innerHeight,
		}),
	);
};

export const initVisualViewportHeader = () => {
	const vv = window.visualViewport;
	if (!vv) {
		return;
	}

	let raf: number | undefined;
	const sync = () => {
		if (raf !== undefined) {
			return;
		}
		raf = requestAnimationFrame(() => {
			raf = undefined;
			resyncStickyBarShifts();
		});
	};

	// scroll fires while panning a zoomed page; resize covers the zoom itself and
	// the keyboard.
	vv.addEventListener("scroll", sync, { passive: true });
	vv.addEventListener("resize", sync, { passive: true });
	// A page scroll can change the offset too, on browsers that clamp the visual
	// viewport against the document.
	window.addEventListener("scroll", sync, { passive: true });
	sync();
};
