// Keep both sticky bars inside the part of the page the user can actually see.
//
// NOTHING HERE TRUSTS A `visualViewport` NUMBER ON ITS OWN. The ticker's
// correction is measured against clientHeight and reads no viewport at all;
// the header's (headerMeasuredShift below) acts only on an offsetTop that
// scrollY independently corroborates, only at rest, and only by the measured
// difference. The history of why - five failed viewport-derived builds, a
// ghost `vv.height`, and then a field report proving the stranded-header state
// real after all - is told at each function.
//
// The idea was that `position: sticky` sticks to the LAYOUT viewport, so a
// visual-viewport pan moves what you can see without moving where sticky
// sticks, parking the header off the top of the screen. One field log seemed to
// catch exactly that - scale 0.85, a 646-tall visual viewport sitting 406px
// down a 1052-tall layout viewport, the header measured at -406 - and the
// header was given `translateY(offsetTop)` to put it back.
//
// A later log from the same device, same page, same scale, idle, no keyboard,
// nothing focused, reports:
//
//   281301 header:resume scrollY=0 headerTop=0 vv=1052/1052@0x0.85
//   300349 header:resume scrollY=0 headerTop=0 vv=1052/1052@0x0.85
//   325275 header:resume scrollY=0 headerTop=0 vv=646/1052@0x0.85
//
// `vv.height` reports 1052 twice and then 646, with innerHeight 1052 throughout
// and nothing in between. The window did not shrink by 406px on a roster page
// with nothing focused. Those readings cannot both be true, and 646 is the one
// that is false - it is a stale keyboard-sized ghost, and the `offsetTop: 406`
// that comes with it is the same ghost's self-consistent other half
// (406 + 646 = 1052).
//
// So the -406 that justified the header shift was never proven a pan - it was
// equally this ghost - and the same log shows the header carrying
// `translateY(229px)` on a page scrolled to 229, a header pushed a quarter of
// the way down the screen by a number that was never corroborated. Five builds
// of a viewport-derived correction failed in the field this way, and for a
// while the header was left alone entirely, on the claim that the stranded
// state had never actually been reported.
//
// It has been now, unambiguously: same device, at rest, headerTop -16 at
// scrollY 16 and then -283 at 283, the user watching the header ride out of
// the window while the diagnostics called it healthy and the manual repair
// button reported success. Both facts stand - the viewport lies sometimes, AND
// the stranded state is real - so the header shift is back with the discipline
// the old one lacked: measured like the ticker's, gated to rest, and acting
// only when scrollY corroborates the offset (the one state the ghost cannot
// fake). See headerMeasuredShift.
//
// THE TICKER KEEPS ITS CORRECTION, because it is MEASURED rather than derived
// from the viewport - see tickerMeasuredShift below - so a lying viewport
// cannot move it.

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

// WHERE THE HEADER GOES - MEASURED, AT REST, AND ONLY WITH CORROBORATION.
//
// The correction the top of this file removes was killed for trusting numbers
// that lie, and the incident that killed it stands: `vv.height` flipped
// 1052 -> 646 on an idle page, and the offset that came with that ghost pushed
// a healthy header a quarter of the way down the screen. What the removal got
// wrong is the claim that the stranded-header symptom had never been reported.
// It since has been, exactly and repeatedly, from the same standalone iPhone
// PWA: at rest, headerTop -16 at scrollY 16, then -283 at 283, the header
// visibly riding up out of the window, the repair ladder measuring it
// "healthy" and the manual button doing nothing. The state is real; the old
// correction was merely wrong about WHEN.
//
// The tell separating that real state from the ghost is in the reports
// themselves. In the real one, `visualViewport.offsetTop` EQUALS `scrollY` -
// 16/16, 283/283 - which is the geometry of a layout viewport pinned to the
// document top while the visual viewport pans inside it (on iOS, scrollY
// tracks the visual viewport, so pinned layout means the two offsets are one
// number). The ghost never had that: its offset was 406 with the page at 0,
// two numbers with nothing to corroborate them. So the header correction
// returns, gated three ways the old one was not:
//
//   - CORROBORATED: only when offsetTop and scrollY agree, the one state the
//     ghost cannot fake and pinch-zoom (offset independent of scrollY) never
//     enters. A number is trusted here only when two independent sources say
//     the same thing.
//   - AT REST: never within the settle window of a scroll or viewport event.
//     Mid-flick a healthy header reads -scrollY on the main thread, which is
//     how the old correction wrote translateY(229) onto a healthy page.
//   - MEASURED, like the ticker: the shift is whatever moves the header's
//     MEASURED top back to 0, its own shift subtracted out, bounded by
//     scrollY (the displacement a ride can actually produce). In every
//     healthy state the measurement already reads 0 and nothing is written,
//     and a stale shift self-heals to 0 at the next settle.
export const headerMeasuredShift = ({
	measuredTop,
	currentShift,
	scrollY,
	visualOffsetTop,
	settled,
}: {
	// getBoundingClientRect().top of the header, as it renders right now.
	measuredTop: number | undefined;
	// The correction currently on it, subtracted back out so this converges in
	// one step instead of chasing its own tail.
	currentShift: number;
	scrollY: number | undefined;
	visualOffsetTop: number | undefined;
	// Has the page been still long enough for a reading to mean anything?
	settled: boolean;
}): number => {
	if (
		!settled ||
		measuredTop === undefined ||
		scrollY === undefined ||
		visualOffsetTop === undefined ||
		!Number.isFinite(measuredTop) ||
		!Number.isFinite(scrollY) ||
		!Number.isFinite(visualOffsetTop)
	) {
		return 0;
	}
	// The corroboration: a real pinned-layout pan is the only state where the
	// two agree while scrolled. Everything else - ghost offsets, pinch-zoom,
	// ordinary pages - fails one of these and gets no shift.
	if (scrollY <= 2 || Math.abs(scrollY - visualOffsetTop) > 2) {
		return 0;
	}
	const raw = 0 - (measuredTop - currentShift);
	// Sub-pixel differences are rounding; a shift beyond scrollY is not a
	// displacement a ride can produce, so it is a measurement not worth acting
	// on.
	if (raw < 2 || raw > scrollY + 2) {
		return 0;
	}
	return Math.round(Math.min(raw, scrollY));
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
// The last moment a scroll or viewport event fired, for the settle gate on the
// header shift. 0 means none seen since load, which counts as settled.
let lastViewportChurnAt = 0;
export const SETTLE_QUIET_MS = 250;

export const resyncStickyBarShifts = () => {
	// The header's shift is measured, corroborated and settle-gated - see
	// headerMeasuredShift. In every state but the pinned-layout pan it computes
	// 0, and passing 0 is what strips a translateY an older build left on it -
	// including one that survived a suspend, which is the only way a stale
	// shift could persist unnoticed.
	const header = document.querySelector<HTMLElement>(HEADER_SELECTOR);
	applyHeaderShift(
		header,
		header
			? headerMeasuredShift({
					measuredTop: header.getBoundingClientRect().top,
					currentShift: currentShiftOf(header),
					scrollY: window.scrollY,
					visualOffsetTop: window.visualViewport?.offsetTop,
					settled:
						lastViewportChurnAt === 0 ||
						Date.now() - lastViewportChurnAt >= SETTLE_QUIET_MS,
				})
			: 0,
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
	let settleTimer: ReturnType<typeof setTimeout> | undefined;
	const sync = () => {
		lastViewportChurnAt = Date.now();
		// One more pass once the churn stops, because that is the only moment
		// the header shift is allowed to act (see headerMeasuredShift) - the
		// per-event pass below always lands inside its own settle window.
		if (settleTimer !== undefined) {
			clearTimeout(settleTimer);
		}
		settleTimer = setTimeout(() => {
			settleTimer = undefined;
			resyncStickyBarShifts();
		}, SETTLE_QUIET_MS + 50);
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
