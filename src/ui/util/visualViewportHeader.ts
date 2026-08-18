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
// THE BOTTOM TICKER HAS THE SAME PROBLEM AT THE OTHER END, and this is the last
// part of the header's treatment it was missing. It is sticky, so it is anchored
// to the bottom of the LAYOUT viewport; whenever the visual viewport sits inside
// the layout one, the bar is parked that far below what the user can see, and it
// appears to drift as the gap changes during a scroll. Same correction, opposite
// direction.
//
// Nothing happens at all in the normal case, where the two viewports agree. Note
// that this runs on every scroll, not only on resume - a standing correction
// rather than a repair, which is why it is separate from the watchdog.

import { recordHeaderEvent } from "./stickyHeaderDiagnostics.ts";

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

// And how far to pull the ticker up, which is the gap between the bottom of the
// visible area and the bottom of the layout viewport.
//
// GATED ON THE VIEWPORTS BEING PANNED APART, not merely on the heights
// differing, and that distinction is the software keyboard. Opening the keyboard
// shortens the visual viewport without moving it (offsetTop stays 0), and
// hoisting the ticker above the keyboard would put it on top of whatever the
// user is typing into. A parked ticker behind the keyboard is the better wrong
// answer, so this stands down there and only acts when the page is genuinely
// zoomed or panned.
export const tickerVisualShift = ({
	offsetTop,
	visualHeight,
	layoutHeight,
}: {
	offsetTop: number | undefined;
	visualHeight: number | undefined;
	layoutHeight: number;
}): number => {
	if (
		offsetTop === undefined ||
		visualHeight === undefined ||
		!Number.isFinite(offsetTop) ||
		!Number.isFinite(visualHeight) ||
		!Number.isFinite(layoutHeight) ||
		offsetTop <= 0
	) {
		return 0;
	}
	const gap = layoutHeight - (offsetTop + visualHeight);
	return gap > 0 ? -Math.round(gap) : 0;
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

// WHEN THE LAYOUT VIEWPORT ITSELF IS THE LIE, sticky cannot be corrected -
// it has to be abandoned.
//
// A field report finally caught the state whole: a resumed PWA claiming a
// 1052px layout viewport over a 646px visual one at scale 0.85, ticker style
// and geometry all reading exactly right - translateY correct to the pixel,
// measured bottom on the visible bottom, "detached=false" - while the glass
// showed the bar parked mid-screen. The compositor was applying sticky
// against the bottom edge of a viewport that does not exist, and the full
// repair ladder (position toggle, renderer rebuild, scroll nudge) ran
// end-to-end without moving the rendered pixels. Nothing that writes styles
// against that layout can ever look wrong, and nothing on the ladder makes
// the compositor believe in a different viewport.
//
// So when the layout viewport is provably oversized, the ticker stops using
// its bottom edge at all: position:fixed, anchored to the viewport's TOP -
// the one edge layout and compositor still agree on - and pushed down to the
// bottom of the VISIBLE area by a transform computed here from visual
// viewport numbers alone. The stale quantity (the phantom height) exits the
// formula entirely. This is the same mechanism as the header's shift, which
// the same report shows rendering correctly on the same screen.
//
// The stylesheet's "sticky, not fixed" rule still holds everywhere else:
// this engages only while the viewports disagree by more than any toolbar
// transition is worth AND the page is below 1:1 scale - the shrink-to-fit
// artifact of a bad restore. The scale gate keeps the keyboard case out
// (it shrinks the visual viewport at scale 1, and hoisting the bar above
// the keyboard would cover the input), and pinch-zoom out (scale > 1 shows
// LESS layout, never more... a zoomed-IN page has scale > 1 and a smaller
// visual viewport, which is the standing correction's job, not this one).
const OVERSIZED_MIN_GAP_PX = 150;
const OVERSIZED_MAX_SCALE = 0.99;

export const layoutViewportOversized = ({
	scale,
	visualHeight,
	layoutHeight,
}: {
	scale: number | undefined;
	visualHeight: number | undefined;
	layoutHeight: number;
}): boolean =>
	scale !== undefined &&
	visualHeight !== undefined &&
	Number.isFinite(scale) &&
	Number.isFinite(visualHeight) &&
	Number.isFinite(layoutHeight) &&
	scale < OVERSIZED_MAX_SCALE &&
	layoutHeight - visualHeight > OVERSIZED_MIN_GAP_PX;

// Where the top of the bar goes so its bottom sits on the visible bottom.
export const tickerSelfPlacementShift = ({
	offsetTop,
	visualHeight,
	barHeight,
}: {
	offsetTop: number | undefined;
	visualHeight: number;
	barHeight: number;
}): number => {
	const offset =
		offsetTop !== undefined && Number.isFinite(offsetTop) ? offsetTop : 0;
	return Math.max(0, Math.round(offset + visualHeight - barHeight));
};

// Whether the last placement pass used the fixed-top mode, so the transition
// gets one log line each way instead of one per frame.
let selfPlacing = false;

const applyTickerPlacement = (
	element: HTMLElement | null,
	vv: VisualViewport | null | undefined,
	layoutHeight: number,
) => {
	if (!element) {
		return;
	}
	const oversized =
		vv != null &&
		layoutViewportOversized({
			scale: vv.scale,
			visualHeight: vv.height,
			layoutHeight,
		});

	if (oversized) {
		const shift = tickerSelfPlacementShift({
			offsetTop: vv.offsetTop,
			visualHeight: vv.height,
			barHeight: element.offsetHeight,
		});
		element.style.position = "fixed";
		element.style.top = "0px";
		element.style.bottom = "auto";
		element.style.left = "0";
		element.style.right = "0";
		// Always written, never cleared: with top anchoring the transform IS the
		// placement.
		element.style.transform = `translateY(${shift}px)`;
	} else {
		// Hand the bar back to the stylesheet's sticky and the standing gap
		// correction.
		if (element.style.position === "fixed") {
			element.style.position = "";
			element.style.top = "";
			element.style.bottom = "";
			element.style.left = "";
			element.style.right = "";
		}
		applyHeaderShift(
			element,
			tickerVisualShift({
				offsetTop: vv?.offsetTop,
				visualHeight: vv?.height,
				layoutHeight,
			}),
		);
	}

	if (oversized !== selfPlacing) {
		selfPlacing = oversized;
		recordHeaderEvent({
			kind: "ticker:self-place",
			scrollY: Math.round(window.scrollY),
			headerTop: Math.round(element.getBoundingClientRect().bottom),
			detail: oversized
				? `on gap=${Math.round(layoutHeight - (vv?.height ?? 0))} scale=${vv?.scale.toFixed(2)}`
				: "off",
		});
	}
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
	applyTickerPlacement(
		document.querySelector<HTMLElement>(TICKER_SELECTOR),
		vv,
		document.documentElement?.clientHeight || window.innerHeight,
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
