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

// How far to lift the ticker so it sits on the foot of the visible area.
//
// THIS IS THE MIRROR OF THE HEADER, and the previous build of it was not - it
// pushed the bar DOWN by offsetTop, the same way the header goes, on the theory
// that the visible region was the same size as the layout viewport and merely
// slid down within it. A field report killed that theory outright:
//
//   offsetTop 57, layout viewport 1052, visual viewport 646 tall
//   ticker shifted +57, its bottom measured at 1053 - and NOT ON SCREEN
//
// A bar whose bottom edge is at 1053 on a viewport that ends at 1052 would be
// sitting exactly on the bottom edge, in full view. The user could not see it.
// So the visible area does not end at 1052, and visualViewport.height - the one
// number that says where it does end - is telling the truth: it ends at 646,
// and the bar was 407px below the screen.
//
// The arithmetic, all in the coordinates getBoundingClientRect reports (which
// are relative to the VISUAL viewport here - that is exactly why the header
// reads -offsetTop when it is doing its job):
//
//   sticky puts the bar's bottom at   layoutHeight - offsetTop   = 995
//   the user can see down to          visualHeight               = 646
//   so it needs to come up by         646 - 995                  = -349
//
// Never downward. A positive correction can only push the bar past the foot of
// the layout viewport, which is the one place sticky already guarantees is no
// worse than the edge of the screen.
export const tickerVisualShift = ({
	visualHeight,
	layoutHeight,
	offsetTop,
	keyboardOpen,
}: {
	visualHeight: number | undefined;
	layoutHeight: number | undefined;
	offsetTop: number | undefined;
	// The software keyboard shrinks the visual viewport exactly like a zoom
	// does, and no viewport number distinguishes them - so ask the page instead
	// of guessing from geometry. See keyboardLikelyOpen.
	keyboardOpen?: boolean;
}): number => {
	// Hoisting the bar above the keyboard would park it on top of whatever is
	// being typed into, which is worse than leaving it behind the keyboard.
	if (keyboardOpen) {
		return 0;
	}
	if (
		!Number.isFinite(visualHeight) ||
		!Number.isFinite(layoutHeight) ||
		!Number.isFinite(offsetTop) ||
		visualHeight === undefined ||
		layoutHeight === undefined ||
		offsetTop === undefined ||
		visualHeight <= 0 ||
		layoutHeight <= 0
	) {
		return 0;
	}
	const shift = visualHeight - (layoutHeight - offsetTop);
	return shift < 0 ? Math.round(shift) : 0;
};

// Is the software keyboard (probably) up?
//
// This is the only thing the geometry cannot tell us apart: a keyboard and a
// pinch-zoom both shrink visualViewport.height while the layout viewport stays
// put. The page knows something the viewport does not, though - a keyboard only
// appears when something is focused that can take text. Asking that directly
// beats every heuristic that tried to read it out of the numbers.
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
	applyHeaderShift(
		ticker,
		tickerVisualShift({
			visualHeight: vv?.height,
			layoutHeight:
				document.documentElement?.clientHeight || window.innerHeight,
			offsetTop: vv?.offsetTop,
			keyboardOpen: keyboardLikelyOpen(document.activeElement),
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
