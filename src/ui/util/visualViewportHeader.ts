// Keep the bottom ticker inside the part of the page the user can actually see,
// and hold the header up when the browser stops holding it up itself.
//
// THE HEADER IS NEVER PLACED FROM THE VIEWPORT, AND THE STORY OF WHY FOLLOWS.
// It does now get one correction, but not from any viewport number - see
// headerStickyFallbackShift below, and read this first for what it must not do.
//
// Five builds of a viewport-derived header correction failed in the field,
// because `visualViewport` lies on the device that needed them: its height
// flipped 1052 -> 646 on an idle page, and a correction derived from those
// numbers once pushed a healthy header a quarter of the way down the screen
// (translateY(229) on a page scrolled to 229). A sixth build measured instead
// of derived, corroborated offsetTop against scrollY, and gated itself to
// rest - and it WORKED, and the owner called the result ugly and asked for it
// gone, because a header that rides during every scroll and snaps back at
// every stop is worse to live with than the defect.
//
// The next theory was that the defect was not the header's at all: the device
// sat at a 518px layout viewport on a 440pt screen, and position:sticky
// anchors to the layout viewport, so both bars would sit partly outside the
// screen. minimum-scale=1 went into the viewport meta to stop the expansion.
// THAT THEORY IS DEAD TOO - the report from the build carrying it came back at
// 518 and 0.85, and the expansion cannot be what strands the bars anyway: a
// wider layout viewport scales the page down, it does not move a stuck header
// off the top of it.
//
// What the same report DID show, once the right number was looked at, is a
// header sitting at exactly its own static position while the page was
// scrolled - sticky not engaging at all - and a visualViewport.offsetTop equal
// to the scroll offset, which is what the watchdog had been subtracting before
// deciding the header was fine. See viewportOffsetIsInformative in
// stickyHeaderWatchdog.ts: the detector was cancelling the fault out, which is
// why the repair button reported nothing wrong and did nothing.
//
// THE TICKER KEEPS ITS CORRECTION, because it is MEASURED rather than derived
// from the viewport - see tickerMeasuredShift below - so a lying viewport
// cannot move it.
//
// AND THE HEADER NOW HAS ONE ON THE SAME TERMS. The report that reopened this
// added the reading the earlier rounds never had: a position:sticky element
// created on the spot, in the page, measured -18 while the page was scrolled 18
// - so sticky was not engaging for ANYTHING, not just for a stale header node.
// That is a browser fault the app cannot prevent, and the watchdog was right
// that no repair of an element could fix it, but "correctly diagnosed" left the
// header scrolling off the screen until the app was force-quit. The correction
// below reads no viewport, only how far the header sits inside its own parent
// against how far the page is scrolled, and computes to zero whenever sticky is
// working - which is the condition the sixth build failed and the reason this
// one is allowed to exist.

// WHEN STICKY IS NOT STICKING AT ALL, PUT THE HEADER WHERE STICKY WOULD.
//
// This is not a seventh viewport-derived correction - it reads no visualViewport
// number at all. It uses the one measurement the field reports have been
// carrying all along, and which the snapshot already prints as headerLift: how
// far the header sits inside its own parent.
//
//     pinned by a working sticky  ->  lift == scrollY
//     not sticking at all         ->  lift == 0
//
// The last report was lift=0 against a scroll of 18, and a freshly inserted
// position:sticky probe read -18 too, so sticky was not engaging for anything
// in the page - a WebKit fault the app cannot prevent and, until now, did
// nothing about: the watchdog correctly concluded no ELEMENT repair could help
// and stopped there, leaving the header to scroll away.
//
// scrollY is safe to use where visualViewport was not. It is corroborated by
// documentElement.scrollTop in every report, and the failure mode that killed
// the earlier builds - a viewport lying about its height - cannot reach it.
//
// Self-cancelling by construction: a healthy header measures lift == scrollY,
// so the shift is zero and nothing is written. The ugliness the owner rejected
// was a correction that ran on every scroll of a WORKING header; this one is
// inert unless sticky is actually broken.
export const headerStickyFallbackShift = ({
	headerTop,
	parentTop,
	currentShift,
	scrollY,
	layoutHeight,
}: {
	// getBoundingClientRect().top of the header and of its parent.
	headerTop: number | undefined;
	parentTop: number | undefined;
	// The correction already written on the header, subtracted back out so this
	// converges in one step instead of measuring its own last answer.
	currentShift: number;
	// How far the document is scrolled - where a pinned header's lift should be.
	scrollY: number | undefined;
	// documentElement.clientHeight, only to reject a nonsense correction.
	layoutHeight: number | undefined;
}): number => {
	if (
		headerTop === undefined ||
		parentTop === undefined ||
		scrollY === undefined ||
		!Number.isFinite(headerTop) ||
		!Number.isFinite(parentTop) ||
		!Number.isFinite(scrollY) ||
		!Number.isFinite(currentShift) ||
		scrollY <= 0
	) {
		return 0;
	}
	const lift = headerTop - currentShift - parentTop;
	const shift = scrollY - lift;
	// Rounding, not misplacement.
	if (!Number.isFinite(shift) || Math.abs(shift) < 2) {
		return 0;
	}
	// Only ever DOWNWARD. A negative shift would be the header sitting lower
	// than the scroll can explain, which is a different fault (a stale node the
	// watchdog rebuilds) and not something to paper over here.
	if (shift < 0) {
		return 0;
	}
	// A correction taller than the viewport is not a header that failed to
	// stick, it is one that has come adrift entirely - again the watchdog's job.
	if (
		layoutHeight !== undefined &&
		Number.isFinite(layoutHeight) &&
		layoutHeight > 0 &&
		shift > layoutHeight
	) {
		return 0;
	}
	return Math.round(shift);
};

const HEADER_SELECTOR = ".navbar-border.sticky-top";
const TICKER_SELECTOR = ".league-ticker";

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
// unrepairable forever.
export const resyncStickyBarShifts = () => {
	// Not placed from the viewport (see the top of this file). The only
	// correction it ever gets is the measured one above, and only when sticky
	// has stopped working altogether - which computes to 0 on a healthy page,
	// so this still strips a translateY an older build left behind.
	const header = document.querySelector<HTMLElement>(HEADER_SELECTOR);
	applyHeaderShift(
		header,
		headerStickyFallbackShift({
			headerTop: header?.getBoundingClientRect().top,
			parentTop: header?.parentElement?.getBoundingClientRect().top,
			currentShift: header ? currentShiftOf(header) : 0,
			scrollY: window.scrollY,
			layoutHeight:
				document.documentElement?.clientHeight || window.innerHeight,
		}),
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
