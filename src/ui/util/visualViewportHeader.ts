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
			applyHeaderShift(
				document.querySelector<HTMLElement>(HEADER_SELECTOR),
				headerVisualShift(vv.offsetTop),
			);
			applyHeaderShift(
				document.querySelector<HTMLElement>(TICKER_SELECTOR),
				tickerVisualShift({
					offsetTop: vv.offsetTop,
					visualHeight: vv.height,
					layoutHeight:
						document.documentElement?.clientHeight || window.innerHeight,
				}),
			);
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
