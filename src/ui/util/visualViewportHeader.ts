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
// THE BOTTOM TICKER HAS THE SAME PROBLEM, and - after a long time spent
// assuming otherwise - the same answer, not a mirrored one. It is sticky, so it
// holds the foot of the LAYOUT viewport, which on a panned page is offsetTop
// pixels above the foot of what the user can see. Same correction, same
// direction: down.
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

// AND THE TICKER GETS THE SAME SHIFT, DOWN, FOR THE SAME REASON.
//
// This was wrong for a long time and the fix is smaller than every attempt at
// it. The ticker was treated as the header's opposite - if the header is above
// the visible area, surely the bar is below it - so it was pushed UP by the
// gap between the visible bottom and the layout viewport's bottom. It is not
// the opposite. The visible region is the SAME SIZE as the layout viewport,
// just slid DOWN by offsetTop, so everything in it needs the same downward
// correction.
//
// A field report settles it. offsetTop 300, layout viewport 1052, and:
//
//   - the header, shifted +300, renders at the top of the screen. Correct, and
//     it has always worked, which is the evidence that offsetTop is real.
//   - the ticker, sticky, measured its bottom at client y 752 - which is
//     exactly 1052 - 300, the layout viewport's bottom seen from a visual
//     viewport that starts 300 down.
//   - 752 of 1052 is 71% down the screen, and the screenshot has the bar at
//     about 70%, sitting on the box score with page content below it.
//
// So sticky put the bar at the foot of the LAYOUT viewport, which is 300px
// above the foot of what the user can see. It needs to come down by exactly
// the amount the header comes down by.
//
// visualViewport.height is not used at all any more, which also retires every
// problem with it: it is the number that alternated between two values seconds
// apart, and the number the old gap was computed from.
//
// The keyboard is still safe. It shortens the visible area without moving it,
// so offsetTop stays 0 and nothing shifts - a bar hoisted above the keyboard
// would cover whatever is being typed into.
export const tickerVisualShift = (offsetTop: number | undefined): number =>
	headerVisualShift(offsetTop);

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
	// The self-placement mode used to live here and is gone; clear anything a
	// previous build of it left behind so a bar cannot stay pinned mid-page.
	if (ticker && ticker.style.position === "fixed") {
		ticker.style.position = "";
		ticker.style.top = "";
		ticker.style.bottom = "";
		ticker.style.left = "";
		ticker.style.right = "";
	}
	applyHeaderShift(ticker, tickerVisualShift(vv?.offsetTop));
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
