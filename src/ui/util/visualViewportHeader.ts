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
	scale,
}: {
	offsetTop: number | undefined;
	visualHeight: number | undefined;
	layoutHeight: number;
	scale?: number;
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
	// A gap computed from an impossible height is an impossible gap, and acting
	// on it is what put the bar mid-page. See visualViewportPlausible.
	if (!visualViewportPlausible({ scale, visualHeight, layoutHeight })) {
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

// IS THE VISUAL VIEWPORT EVEN TELLING THE TRUTH?
//
// Everything below reads visualViewport.height, and a field log finally caught
// it lying. On one device, on one page, with nothing changing between them,
// the reported height alternated within seconds:
//
//   vv=1083/1083@0x0.75   ...then...   vv=636/1083@0x0.75
//
// Both at scale 0.75, and only one can be right. Zooming OUT shows MORE of the
// page, so at 0.75 the visible height must be at least the layout viewport's,
// never 59% of it. The 636 is impossible; the 1083, which agrees with the
// layout viewport, is the real one.
//
// This mattered because a whole mode was built on the impossible number. The
// ticker used to abandon sticky and place itself, computing "the bottom of the
// visible area" from that 636 - which put the bar 600px down a screen that is
// really 1083 tall, i.e. mid-page. The log shows it flipping on and off every
// few seconds as the reading alternated, and every time it was on the bar left
// the bottom of the screen. That mode is gone, and the standing correction
// below now stands down whenever the numbers cannot be true, which leaves
// plain sticky doing exactly what it should: holding the foot of the layout
// viewport, which is where the screen actually ends.
//
// Only the HEIGHT is judged here. The header's correction uses offsetTop,
// a different quantity that this evidence says nothing about, and it works -
// so it is left alone.
const IMPLAUSIBLE_SHRINK = 0.9;

export const visualViewportPlausible = ({
	scale,
	visualHeight,
	layoutHeight,
}: {
	scale: number | undefined;
	visualHeight: number | undefined;
	layoutHeight: number;
}): boolean => {
	if (
		scale === undefined ||
		visualHeight === undefined ||
		!Number.isFinite(scale) ||
		!Number.isFinite(visualHeight) ||
		!Number.isFinite(layoutHeight) ||
		layoutHeight <= 0
	) {
		// Nothing to check against: believe it, same as before any of this.
		return true;
	}
	if (scale >= 1) {
		// Zoomed in (or not zoomed): a smaller visible area is exactly right.
		return true;
	}
	// Zoomed out, so the visible area must not be dramatically SMALLER than the
	// layout viewport - that combination cannot happen.
	return visualHeight >= layoutHeight * IMPLAUSIBLE_SHRINK;
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
	const layoutHeight =
		document.documentElement?.clientHeight || window.innerHeight;
	// The self-placement mode used to live here and is gone; clear anything a
	// previous build of it left behind so a bar cannot stay pinned mid-page.
	if (ticker && ticker.style.position === "fixed") {
		ticker.style.position = "";
		ticker.style.top = "";
		ticker.style.bottom = "";
		ticker.style.left = "";
		ticker.style.right = "";
	}
	applyHeaderShift(
		ticker,
		tickerVisualShift({
			offsetTop: vv?.offsetTop,
			visualHeight: vv?.height,
			layoutHeight,
			scale: vv?.scale,
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
