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
//
// AND ONLY WHEN offsetTop CORROBORATES THE HEIGHT. The very next field report
// after the formula above was restored had the identical geometry - 646-tall
// visual viewport on a 1052 layout viewport, scale 0.85 - and this time it was
// FALSE: the lifted bar sat mid-screen with page visibly rendering far below
// layout y 646, and the log caught the lie being born, vv=1052/1052 before the
// app was backgrounded and vv=646/1052 on resume, same page, nothing changed.
// A resume can hand back a stale, keyboard-sized height with no keyboard
// present, so the same numbers are sometimes the truth and sometimes a ghost,
// and no reading of vv.height alone can tell which.
//
// offsetTop looked like a witness - you cannot pan a thing inside a thing it
// completely fills, so a nonzero offsetTop seemed to prove the height real.
// The very next report broke that too: vv 646/1052 AT offsetTop 69, bar lifted
// mid-screen again, and the new report fields show why the gate was fooled.
// The ghost is not one bad number, it is the whole VisualViewport object
// living in a self-consistent phantom-keyboard world: its height stays
// keyboard-sized and its offsetTop tracks the page scroll (69 = scrollY = 69)
// exactly as if the keyboard were still up. Nothing read from inside that
// world can vouch for anything else inside it.
//
// The witness has to come from outside, and width is it. A visual viewport
// that is genuinely smaller than the layout viewport only exists under
// pinch-zoom, and pinch narrows BOTH axes - while the ghost, being a
// keyboard's shadow, shrinks height alone and keeps the width spanning the
// screen (the report proves it: vv width 518 = the full layout width = the
// full 440pt screen at 0.85, with activeElement <body>, so nothing focusable
// could be covering the missing 400pt). So the lift now requires the width to
// be genuinely narrowed. On a full-width viewport there is nothing this
// correction can truthfully know about the bottom of the screen, and every
// recorded full-width case wants 0 anyway: healthy wants nothing, the ghost
// wants nothing, and a real keyboard wants nothing because hoisting the bar
// over it would cover what is being typed.
export const tickerVisualShift = ({
	visualHeight,
	layoutHeight,
	offsetTop,
	visualWidth,
	layoutWidth,
	keyboardOpen,
}: {
	visualHeight: number | undefined;
	layoutHeight: number | undefined;
	offsetTop: number | undefined;
	// The pinch corroboration - see above. Only a width genuinely narrower
	// than the layout viewport proves the viewport is really smaller at all.
	visualWidth: number | undefined;
	layoutWidth: number | undefined;
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
	if (
		!Number.isFinite(visualWidth) ||
		!Number.isFinite(layoutWidth) ||
		visualWidth === undefined ||
		layoutWidth === undefined ||
		layoutWidth <= 0
	) {
		// Unreadable widths cannot corroborate anything, and claiming less is
		// the safe direction.
		return 0;
	}
	if (offsetTop <= 0) {
		// Nowhere panned means sticky's own spot is the right one.
		return 0;
	}
	// The pinch test. The half pixel forgives rounding; a ghost is not half a
	// pixel narrow, it is exactly as wide as the layout viewport.
	if (visualWidth + 0.5 >= layoutWidth) {
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
			visualWidth: vv?.width,
			layoutWidth: document.documentElement?.clientWidth || window.innerWidth,
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
