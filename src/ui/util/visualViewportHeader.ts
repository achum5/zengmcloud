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
//
// AND TAKING IT OUT OF FLOW WAS STILL NOT ENOUGH, which is the finding that
// produced the correction in headerMeasuredShift. The next report has the
// fallback ENGAGED - the probe read -140 on a page scrolled 192, which is the
// fallback's own 52px of padding showing through, so the header was already
// `position:fixed; top:0` - and the header measuring -192 anyway. A fixed
// element cannot sit 192px above the box it is fixed to, so the box is not
// where iOS says it is: that device reports a visual viewport 646 tall inside a
// 1052 layout viewport with offsetTop equal to the scroll offset, and places
// fixed and sticky alike against the phantom.
//
// So there is no position value that lands the header on the screen, because
// every one of them anchors to the same wrong box. What is left is the thing
// the ticker has been doing successfully all along: measure where the bar
// actually is, and move it the difference. That is now what both bars do.

// WHEN STICKY IS NOT STICKING AT ALL, PUT THE HEADER WHERE STICKY WOULD.
//
// NOT WITH A TRANSFORM. The first build of this did translate the header down
// by the scroll offset, and it pinned the header correctly - and grew the page.
// A transform on an in-flow element counts toward scrollable overflow in
// WebKit, so pushing the header down extended the document, which allowed more
// scroll, which enlarged the push. The user could scroll a long way past the
// end of the content and it stayed there. (Chromium does not do this, which is
// why it took a device to find.)
//
// position:fixed is out of flow and contributes nothing to scrollable overflow,
// so it cannot feed back into the scroll at all. It anchors to the same layout
// viewport sticky does, which is exactly where the header belongs.
//
// Taking the header out of flow costs its height, so its parent gets that back
// as padding for as long as the fallback is engaged.

// Is sticky provably not working - from a reading that does NOT depend on what
// we have already done to the header?
//
// It has to be probe-based for that reason. Once the header is fixed, its own
// position looks correct whether sticky works or not, so measuring the header
// to decide whether to keep correcting the header is circular, and flickers.
//
// At the top of the document a stuck bar and a loose one are in the same place,
// so there is nothing to tell apart and nothing to correct either - which is
// what `possible` being zero means here.
//
// The reading is a LIFT rather than an absolute top, because the absolute one
// was measured against a static position that our own fallback moves. See
// probeSticky for the flicker that cost.
export const stickyIsBroken = ({
	probe,
}: {
	// How far a position:sticky element created just now was lifted off its
	// static position, and how far it could have been lifted. See probeSticky.
	probe: { lift: number; possible: number } | undefined;
}): boolean => {
	if (
		probe === undefined ||
		!Number.isFinite(probe.lift) ||
		!Number.isFinite(probe.possible) ||
		probe.possible <= 2
	) {
		return false;
	}
	// Working sticky lifts it the whole way and broken sticky not at all, so
	// anything short of halfway is the broken case with room for the rounding
	// a zoomed page puts on every rect.
	return probe.lift < probe.possible / 2;
};

// The cheap pre-check, so a healthy device never pays for a probe. A header
// that is sticking sits scrollY below the top of its parent; one that is not
// sits flush with it. Only when this says something is wrong is the probe worth
// inserting to find out whether it is sticky itself or just this element.
export const headerLooksUnstuck = ({
	headerTop,
	parentTop,
	scrollY,
}: {
	headerTop: number | undefined;
	parentTop: number | undefined;
	scrollY: number | undefined;
}): boolean => {
	if (
		headerTop === undefined ||
		parentTop === undefined ||
		scrollY === undefined ||
		!Number.isFinite(headerTop) ||
		!Number.isFinite(parentTop) ||
		!Number.isFinite(scrollY) ||
		scrollY <= 2
	) {
		return false;
	}
	return Math.abs(headerTop - parentTop) < scrollY - 2;
};

import { probeSticky } from "./stickyHeaderDiagnostics.ts";
import { readViewport, visualViewportStale } from "./stickyViewportReset.ts";
import {
	STICKY_FALLBACK_CLASS,
	STICKY_FALLBACK_HEIGHT_VAR,
} from "./stickyHeaderPin.ts";

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

// WHERE THE PINNED HEADER GOES, MEASURED INSTEAD OF ASSUMED.
//
// The fixed fallback was supposed to be the end of this: position:fixed anchors
// to the layout viewport, which is where a stuck header belongs. The field
// report that reopened it shows the fallback ENGAGED and the header still off
// the screen - the probe read -140 with the page scrolled 192, which is exactly
// the fallback's own 52px of padding showing through, so it was on - and the
// header, `position:fixed; top:0`, measured -192.
//
// A fixed element cannot be 192px above the top of the box it is fixed to. So
// the box is not where iOS says it is: the device reports a visual viewport
// 646 tall inside a 1052 layout viewport with offsetTop tracking the scroll
// exactly, and the compositor places fixed and sticky alike against that
// phantom. Everything anchored to the top of the viewport is displaced up the
// page by the scroll offset, which is precisely "the header is not sticking"
// and precisely why no repair of any element has ever fixed it.
//
// The ticker has been immune to all of this for one reason - it measures where
// it actually is and moves by the difference, consulting no viewport number.
// This is that, for the top bar: drive the header's measured top to zero.
//
//     shift = 0 - (measured top with our own shift removed)
//
// On a healthy device a pinned header already measures 0, the shift is 0, and
// nothing is written - so this cannot regress a browser that works.
//
// ONLY EVER WHILE PINNED. A transform on an in-flow element counts toward
// scrollable overflow in WebKit, so an earlier build that pushed the in-flow
// header down grew the document, which allowed more scroll, which enlarged the
// push. Out of flow it contributes nothing to overflow and the feedback loop
// cannot exist.
export const headerMeasuredShift = ({
	measuredTop,
	currentShift,
	pinned,
	layoutHeight,
}: {
	// getBoundingClientRect().top of the header, as it renders right now.
	measuredTop: number | undefined;
	// The correction currently on it, subtracted back out so this converges in
	// one step instead of chasing its own tail.
	currentShift: number;
	// Is the header out of flow (the fixed fallback engaged)? Never correct one
	// that is still in flow - see above.
	pinned: boolean;
	// documentElement.clientHeight, only as a bound on what counts as a
	// plausible correction.
	layoutHeight: number | undefined;
}): number => {
	if (
		!pinned ||
		measuredTop === undefined ||
		layoutHeight === undefined ||
		!Number.isFinite(measuredTop) ||
		!Number.isFinite(layoutHeight) ||
		layoutHeight <= 0
	) {
		return 0;
	}
	const shift = -(measuredTop - currentShift);
	// Sub-pixel differences are rounding, not misplacement, and writing a
	// transform for them only churns the compositor.
	if (Math.abs(shift) < 2) {
		return 0;
	}
	// A correction larger than the viewport is not a viewport offset, it is a
	// bar that has come adrift - which is the watchdog's job, not this one's.
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
// Put the header out of flow at the top of the viewport, or take it back.
//
// A CLASS on the header's parent, not inline styles on the header. The
// watchdog's repair ladder clears inline `position` on the bars, so an inline
// fallback and the ladder take it in turns and the header flickers - which is
// exactly what a browser run showed. The class survives the ladder, and the
// rules it carries are the ones the iOS modal pin has been using for the same
// job (see .sticky-fallback-pinned in light.scss).
//
// Both directions are idempotent: this runs on every scroll frame and must not
// touch the DOM when nothing has changed.
export const applyHeaderFixedFallback = (
	header: HTMLElement | null,
	engaged: boolean,
) => {
	const parent = header?.parentElement;
	if (!header || !parent) {
		return;
	}
	if (engaged === parent.classList.contains(STICKY_FALLBACK_CLASS)) {
		return;
	}
	if (engaged) {
		// Measured BEFORE going out of flow, while it still has a height. The
		// stylesheet's own fallback covers a header that could not be measured.
		const height = Math.round(header.getBoundingClientRect().height);
		if (height > 0) {
			parent.style.setProperty(STICKY_FALLBACK_HEIGHT_VAR, `${height}px`);
		}
		parent.classList.add(STICKY_FALLBACK_CLASS);
	} else {
		parent.classList.remove(STICKY_FALLBACK_CLASS);
		parent.style.removeProperty(STICKY_FALLBACK_HEIGHT_VAR);
	}
};

// Probing inserts a node and reads a rect, which forces layout, so it is not
// something to do on every scroll frame of a healthy page. A device that is
// broken stays broken, so re-asking is only about noticing that it stopped.
const PROBE_INTERVAL_MS = 1000;
let lastProbeAt = 0;
let lastProbeSaidBroken = false;

const stickyBrokenNow = (header: HTMLElement): boolean => {
	const scrollY = window.scrollY;
	const engaged =
		header.parentElement?.classList.contains(STICKY_FALLBACK_CLASS) === true;

	// A STALE KEYBOARD INSET. The visual viewport is a keyboard's height
	// shorter than the layout viewport with no keyboard up, so iOS lets it pan
	// inside the layout viewport - and sticky, pinned to the layout viewport,
	// slides off the glass while the probe swears it is healthy (it is: 734
	// of 749 in the field, the missing 15 being the pan). Nothing corrects an
	// in-flow header, by design, so pin it: out of flow the measured shift is
	// allowed, and it drives the header's top to the top of what the user
	// sees. Held for as long as the inset is stale, not per pan, so the header
	// does not flip in and out of flow on every scroll.
	if (visualViewportStale(readViewport())) {
		return true;
	}

	// While engaged the header's own position proves nothing - it is fixed, so
	// it looks right either way - and only the probe can say whether to stop.
	if (
		!engaged &&
		!headerLooksUnstuck({
			headerTop: header.getBoundingClientRect().top,
			parentTop: header.parentElement?.getBoundingClientRect().top,
			scrollY,
		})
	) {
		lastProbeSaidBroken = false;
		return false;
	}

	const now = Date.now();
	if (now - lastProbeAt < PROBE_INTERVAL_MS) {
		return lastProbeSaidBroken;
	}
	lastProbeAt = now;
	lastProbeSaidBroken = stickyIsBroken({ probe: probeSticky() });
	return lastProbeSaidBroken;
};

export const resyncStickyBarShifts = () => {
	const header = document.querySelector<HTMLElement>(HEADER_SELECTOR);
	if (header) {
		const pinned = stickyBrokenNow(header);
		// Out of flow FIRST: the correction below is only allowed on a header
		// that is, and it has to measure the header as it renders once pinned.
		applyHeaderFixedFallback(header, pinned);
		// A transform on the header, which every build before this one was
		// forbidden - correctly, while the header was in flow, because one
		// grew the document. Pinned it cannot: see headerMeasuredShift. When
		// the header is in flow this computes 0, which also strips whatever an
		// older build left behind.
		applyHeaderShift(
			header,
			headerMeasuredShift({
				measuredTop: header.getBoundingClientRect().top,
				currentShift: currentShiftOf(header),
				pinned,
				layoutHeight:
					document.documentElement?.clientHeight || window.innerHeight,
			}),
		);
	}

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
