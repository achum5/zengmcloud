// The top header comes unstuck on iOS after the app is backgrounded, and stays
// broken - scrolling away with the page - until the app is force-quit.
//
// History, so nobody re-treads it: the header was position:fixed (drifted), then
// fixed with a translateZ compositor promotion (still drifted), then converted to
// position:sticky (still comes unstuck after backgrounding), then moved inside an
// app-shell inner scroller - which DID fix it, and was reverted because a tall
// iOS overflow scroller tile-paints unreliably and left whole pages rendered
// black. So the header is sticky against the document, and the document is the
// scroller, and that is not negotiable.
//
// What actually goes wrong: WebKit records a sticky element's constraints - the
// rect it may move within, and its offset - at LAYOUT time, and hands them to the
// scrolling tree, which applies them on the compositor. Resuming an installed PWA
// can restore a scrolling tree holding constraints computed against a layout that
// no longer matches (the web view is resized for the app switcher snapshot and
// back). The compositor then keeps faithfully applying a stale rule, which is why
// it never recovers on its own and why no amount of repainting helps.
//
// The recovery is to make WebKit throw the stale node away and build a new one.
// Constraints are only recomputed when the element's `position` changes, so that
// is what this does. Nothing else - a style nudge, a forced paint, a scroll event
// - causes a rebuild.
//
// This DETECTS before it acts. The header's own `top` says where a stuck header
// belongs; if it's above that while the page is scrolled, it is provably unstuck.
// That means the watchdog is inert on every platform where this doesn't happen,
// and self-verifying where it does: if a repair doesn't take, the next check
// tries again rather than quietly giving up.

import { PINNED_SELECTOR } from "./stickyHeaderPin.ts";
import { recordHeaderEvent } from "./stickyHeaderDiagnostics.ts";
import {
	applyHeaderShift,
	headerVisualShift,
	initVisualViewportHeader,
} from "./visualViewportHeader.ts";

const HEADER_SELECTOR = ".navbar-border.sticky-top";

// Sub-pixel rounding and zoom mean the rect is never exactly on the line.
const TOLERANCE_PX = 2;

// How long after a resume to keep running the TIMED checks. Layout settles late
// on a resume - the first frame back can be measured against the pre-suspend
// viewport - so a single check right away misses it.
//
// This bounds only the timed checks. The scroll watch is permanent - see
// scrollDecision.
const WATCH_MS = 6000;

// Extra checks after a resume, in ms, on top of the animation-frame one. Cheap
// (a rect read each), and they cover the late-settling case without polling.
const CHECK_DELAYS_MS = [150, 500, 1200, 3000];

type Deps = {
	scrollY: () => number;
	// Viewport-relative top edge of the header.
	headerTop: () => number;
	// The `top` the header is supposed to stick at, from its computed style.
	stickyTop: () => number;
	// A modal deliberately switches the header to position:fixed while it pins
	// the app wrapper (see .ios-modal-pinned) - not a fault, and not repairable.
	pinnedByModal: () => boolean;
	// How far the visible (visual) viewport sits below the top of the layout
	// viewport. Zero unless the page is zoomed or otherwise panned.
	visualOffsetTop: () => number;
};

// Is the header provably not stuck where it should be?
//
// Only answerable while the page is scrolled past the header's resting place: at
// the top of the document a stuck header and an unstuck one are in exactly the
// same position, so there is nothing to compare and the honest answer is "no".
export const headerIsDetached = ({
	scrollY,
	headerTop,
	stickyTop,
	pinnedByModal,
	visualOffsetTop,
}: Deps): boolean => {
	if (pinnedByModal()) {
		return false;
	}
	const scrolled = scrollY();
	if (!Number.isFinite(scrolled) || scrolled <= TOLERANCE_PX) {
		return false;
	}
	const top = headerTop();
	if (!Number.isFinite(top)) {
		return false;
	}
	// WHERE A STUCK HEADER ACTUALLY BELONGS WHEN THE PAGE IS ZOOMED.
	//
	// getBoundingClientRect() reports against the VISUAL viewport, but sticky
	// sticks to the LAYOUT viewport. Pinch-zoom or pan the two apart and a header
	// doing exactly the right thing reads at -visualOffsetTop, not 0.
	//
	// A field log caught this precisely: scale 0.85, visual viewport 646 tall
	// sitting 406px down a 1052 layout viewport, and the header reading -406 -
	// the offset, to the pixel. The old comparison called that a fault and ran
	// the repair ladder four times in a second, which could never work, because
	// nothing about the element was wrong. Subtracting the offset asks the only
	// meaningful question: is the header where sticky would put it?
	const offset = visualOffsetTop();
	const expected = stickyTop() - (Number.isFinite(offset) ? offset : 0);
	return top < expected - TOLERANCE_PX;
};

const getHeader = () => document.querySelector<HTMLElement>(HEADER_SELECTOR);

const computedStickyTop = (element: HTMLElement): number => {
	const top = Number.parseFloat(window.getComputedStyle(element).top);
	return Number.isFinite(top) ? top : 0;
};

// Element-agnostic on purpose: the pin marker moved from <body> to the app
// wrapper, and this only ever asks "is a modal currently pinning something?".
const detached = (element: HTMLElement) =>
	headerIsDetached({
		scrollY: () => window.scrollY,
		headerTop: () => element.getBoundingClientRect().top,
		stickyTop: () => computedStickyTop(element),
		pinnedByModal: () => document.querySelector(PINNED_SELECTOR) !== null,
		visualOffsetTop: () => window.visualViewport?.offsetTop ?? 0,
	});

// WHY EACH STEP SPANS A FRAME.
//
// These used to run start-to-finish inside one task, deliberately: layout is not
// paint, so the header never rendered in the intermediate state and nothing
// flashed. That is also, almost certainly, why the repair did not take.
//
// The thing being rebuilt lives in the SCROLLING TREE, on the compositor - that
// is the whole diagnosis at the top of this file. The compositor is handed a new
// tree when the browser commits a frame, not when script forces a layout. A
// position that goes sticky → relative → sticky inside a single task has the
// same computed value at both ends of that task, so the commit can carry no
// change at all and the stale node survives untouched. Reading `offsetTop`
// flushes layout, which is not the same thing and never was.
//
// So each step now holds its intermediate state across a real frame, which
// forces a commit the compositor has to act on. The flash this was avoiding
// cannot happen: the ladder only ever runs on a header that is ALREADY detached
// and already scrolled off the top of the screen, so there is no correctly
// placed header to disturb - only a broken one, briefly broken differently.
//
// Every step restores `position` to "" rather than to whatever was there
// before. The header's position must come from the stylesheet (.sticky-top, or
// the modal's fixed override); putting back a stale inline value would reinstate
// exactly the broken state we are trying to clear.

const nextFrame = () =>
	new Promise<void>((resolve) => {
		requestAnimationFrame(() => {
			resolve();
		});
	});

// Step 1: rebuild the sticky node. WebKit only recomputes a sticky element's
// constraints when its position changes, so this is the cheapest thing that can
// possibly work.
const rebuildStickyNode = async (element: HTMLElement) => {
	element.style.position = "relative";
	void element.offsetTop;
	await nextFrame();
	element.style.position = "";
	void element.offsetTop;
};

// Step 2: tear the element out of the box tree entirely and put it back. This
// destroys its renderer, its compositing layer and its scrolling-tree node,
// rather than asking for them to be recalculated - the difference matters when
// the stale state is in the compositor rather than in layout.
const rebuildRenderer = async (element: HTMLElement) => {
	element.style.display = "none";
	void element.offsetTop;
	await nextFrame();
	element.style.display = "";
	element.style.position = "";
	void element.offsetTop;
};

// Step 3: the scrolling tree itself is stale, not the element. Scrolling by a
// pixel and back makes WebKit reconcile it against the real scroll position.
// Visually a no-op, and it only ever runs on a header still measurably broken
// after both rebuilds.
const nudgeScroller = async () => {
	const y = window.scrollY;
	window.scrollTo(window.scrollX, y + 1);
	window.scrollTo(window.scrollX, y);
	// Let the reconciliation land before anything measures it.
	await nextFrame();
};

// Escalate only as far as it takes, re-measuring after each step. Cheap when the
// first step works, and it never runs at all on a healthy header.
export const repairSteps = [
	rebuildStickyNode,
	rebuildRenderer,
	nudgeScroller,
] as const;

let stop: (() => void) | undefined;

// One ladder at a time. The steps now await frames, so a scroll arriving
// mid-repair could otherwise start a second pass that fights the first.
let repairing = false;

// The layout viewport against the visual one. A field report showed innerHeight
// 1052 with a visual viewport of 646 - a 406px gap, and the header's offset
// clamping at exactly -406 - so whether these disagree, and by how much, is the
// difference between "the compositor lagged" and "sticky is anchored to a
// viewport the user cannot see".
const viewportNote = () => {
	const vv = window.visualViewport;
	if (!vv) {
		return "vv=none";
	}
	return `vv=${Math.round(vv.height)}/${window.innerHeight}@${Math.round(
		vv.offsetTop,
	)}x${vv.scale.toFixed(2)}`;
};

// Only called when something noteworthy happened, so the log stays short enough
// to paste and every line means something.
const note = (element: HTMLElement | null, kind: string, detail?: string) => {
	recordHeaderEvent({
		kind,
		scrollY: Math.round(window.scrollY),
		headerTop: element
			? Math.round(element.getBoundingClientRect().top)
			: Number.NaN,
		detail,
	});
};

// MEASURE ONLY WHEN THE PAGE IS STILL.
//
// On iOS a sticky element is repositioned by the compositor, and the main
// thread's getBoundingClientRect() does NOT keep up during a flick or its
// momentum: it keeps reporting the element's layout position, which scrolls with
// the document. So mid-scroll a perfectly healthy header reads headerTop ===
// -scrollY - identical to a genuinely broken one.
//
// A field log made that unmistakable. Every detection was followed, tens of
// milliseconds later, by a "gave-up" at a DIFFERENT scroll offset, every reading
// exactly -scrollY, and the single "success" landed at scrollY = -3, i.e. the
// rubber band at the top, where detection is disabled anyway. Not one
// measurement was taken with the page at rest.
//
// That made the watchdog worse than useless: it ran the ladder constantly on a
// header that may well have been fine, and the ladder is not free - it takes the
// header out of flow for a frame and nudges the scroll position. Doing that
// repeatedly during a scroll is itself capable of producing the symptom being
// chased.
//
// So a fault is only ever declared from two readings, a frame apart, that agree
// with each other AND sit at the same scroll offset. Anything else means the
// page was moving and the question cannot be answered yet.
export const detachmentConfirmed = ({
	before,
	after,
}: {
	before: { scrollY: number; headerTop: number };
	after: { scrollY: number; headerTop: number };
}): boolean =>
	before.scrollY === after.scrollY &&
	Math.abs(before.headerTop - after.headerTop) <= TOLERANCE_PX;

const reading = (element: HTMLElement) => ({
	scrollY: Math.round(window.scrollY),
	headerTop: Math.round(element.getBoundingClientRect().top),
});

const check = async (trigger = "scroll") => {
	if (repairing) {
		return;
	}
	const element = getHeader();
	if (!element) {
		return;
	}
	if (!detached(element)) {
		return;
	}

	// Confirm against a second reading a frame later before believing it.
	const before = reading(element);
	await nextFrame();
	const after = reading(element);
	if (!detached(element) || !detachmentConfirmed({ before, after })) {
		note(
			element,
			"unconfirmed",
			`via=${trigger} moved=${after.scrollY - before.scrollY}`,
		);
		return;
	}

	note(element, "detached", `via=${trigger} ${viewportNote()}`);

	repairing = true;
	try {
		for (const [i, step] of repairSteps.entries()) {
			await step(element);
			if (!detached(element)) {
				note(element, "repaired", `step=${i + 1}`);
				return;
			}
		}

		// Still broken after the whole ladder. One more frame, in case the
		// compositor simply had not caught up when we measured, then a final pass
		// before giving up on this attempt - the scroll watch keeps looking either
		// way, so a header that survives this will be tried again on the next
		// scroll rather than staying broken until the app is force-quit.
		await nextFrame();
		if (detached(element)) {
			await rebuildRenderer(element);
			await nudgeScroller();
		}
		note(element, detached(element) ? "gave-up" : "repaired", "step=late");
	} finally {
		repairing = false;
	}
};

// The manual button. Runs the ladder whether or not the header LOOKS broken,
// because the one place the user can reach the button - the top of the page - is
// the one place a broken header is indistinguishable from a healthy one.
export const forceHeaderRepair = async () => {
	const element = getHeader();
	if (!element) {
		return;
	}
	// Put the header back inside the visible viewport first. When the two
	// viewports have come apart this IS the reset - the ladder below cannot help,
	// because nothing about the element is wrong.
	applyHeaderShift(
		element,
		headerVisualShift(window.visualViewport?.offsetTop),
	);
	note(element, "forced", `detached=${detached(element)} ${viewportNote()}`);
	if (repairing) {
		return;
	}
	repairing = true;
	try {
		for (const step of repairSteps) {
			await step(element);
		}
	} finally {
		repairing = false;
	}
	note(element, "forced-done");
};

// Closing a modal hands the header back from position:fixed to position:sticky
// while the wrapper is unpinned and the page is scrolled back to where it was -
// the same kind of position change, against a viewport that just moved, that
// leaves WebKit holding stale sticky constraints after a resume.
//
// Nothing used to look at the header afterwards. The timed checks only arm on
// resume events, the scroll watch disarms as soon as it has judged once, and
// headerIsDetached deliberately stands down for the whole time a modal has
// something pinned. So a header broken by opening and closing the ratings
// popover stayed broken with nobody watching. Hence an explicit check, once
// layout has settled - the same ladder, on the one transition that was exempt
// from it.
const UNPIN_CHECK_DELAYS_MS = [50, 250];

export const scheduleModalUnpinCheck = () => {
	requestAnimationFrame(() => {
		void check("modal-unpin");
	});
	for (const delay of UNPIN_CHECK_DELAYS_MS) {
		setTimeout(() => {
			void check("modal-unpin");
		}, delay);
	}
};

// WHY THE SCROLL WATCH NEVER STANDS DOWN.
//
// A detached header is UNDETECTABLE at the top of the document - stuck and
// unstuck sit in exactly the same place there, and headerIsDetached says so.
// Scrolling is therefore the only event that can ever answer the question.
//
// This watch has been narrowed twice and both times the bug came back. First it
// was torn down a few seconds after a resume, so looking at the screen for
// longer than that and then scrolling met a broken header with nothing left
// watching. Then it was made to survive until a scroll produced an answer - but
// it still disarmed after that one answer, which assumed the only thing that can
// break a header is a resume, and that a repair which reports success has
// actually worked. Neither holds: a modal, an orientation change, a keyboard, or
// any other layout churn can break it just as well, and a repair that measures
// clean can still be undone by the next commit.
//
// So the watch is now permanent and its cost is bounded by throttling instead of
// by disarming. That trades a rect read every so often while scrolling for a
// header that heals itself whenever it breaks, from whatever cause, instead of
// staying broken until the app is force-quit.
export type ScrollDecision = "judge" | "at-top";

// How long the page must be still before a reading means anything. Momentum
// scrolling on iOS keeps the main thread's geometry stale well past the last
// scroll event, and measuring inside that window is what produced a log full of
// phantom faults.
const SETTLE_MS = 250;

export const scrollDecision = ({
	scrollY,
}: {
	scrollY: number;
}): ScrollDecision =>
	// At the top of the document a stuck header and an unstuck one are in the same
	// place, so there is nothing a reading here could establish.
	!Number.isFinite(scrollY) || scrollY <= TOLERANCE_PX ? "at-top" : "judge";

const RESUME_DEDUPE_MS = 1000;
let lastResumeNote: number | undefined;

let settleTimer: ReturnType<typeof setTimeout> | undefined;
let onScroll: (() => void) | undefined;

// Debounced, NOT throttled. A throttle fires during the scroll, which is exactly
// when the answer is unavailable; this waits for the scrolling to stop.
const ensureScrollWatch = () => {
	if (onScroll) {
		return;
	}
	onScroll = () => {
		if (settleTimer !== undefined) {
			clearTimeout(settleTimer);
		}
		settleTimer = setTimeout(() => {
			settleTimer = undefined;
			if (scrollDecision({ scrollY: window.scrollY }) === "judge") {
				void check("scroll-settled");
			}
		}, SETTLE_MS);
	};
	window.addEventListener("scroll", onScroll, { passive: true });
};

const watch = () => {
	stop?.();
	ensureScrollWatch();

	// A resume gets the extra timed checks on top of the standing scroll watch:
	// layout settles late coming back, and we would rather not wait for the user
	// to scroll before trying.
	// visibilitychange, pageshow and focus all fire for one resume, so collapse
	// them rather than writing the same line three times into a 60-entry log.
	const now = Date.now();
	if (lastResumeNote === undefined || now - lastResumeNote > RESUME_DEDUPE_MS) {
		lastResumeNote = now;
		note(getHeader(), "resume", viewportNote());
	}

	const timers = CHECK_DELAYS_MS.map((delay) =>
		setTimeout(() => {
			void check("resume");
		}, delay),
	);
	const frame = requestAnimationFrame(() => {
		void check("resume");
	});

	const end = setTimeout(() => {
		stop?.();
	}, WATCH_MS);

	stop = () => {
		stop = undefined;
		cancelAnimationFrame(frame);
		clearTimeout(end);
		for (const timer of timers) {
			clearTimeout(timer);
		}
	};
};

export const initStickyHeaderWatchdog = () => {
	// Sticky anchors to the layout viewport, so zoom or pan can leave the header
	// off the top of the visible area while behaving perfectly correctly. Started
	// from here so this stays the one lazily-loaded module in the pair.
	initVisualViewportHeader();

	// The standing watch, independent of any resume. Installed here rather than
	// only inside watch() so it is running from the first paint, whatever breaks
	// the header and whether or not a resume event ever fires.
	ensureScrollWatch();

	document.addEventListener("visibilitychange", () => {
		if (document.visibilityState === "visible") {
			watch();
		}
	});

	// pageshow covers a restore from the back/forward cache, which is how iOS
	// often brings a suspended PWA back; focus covers the cases where neither
	// fires (returning from a share sheet, a system prompt, split view).
	window.addEventListener("pageshow", watch);
	window.addEventListener("focus", watch);

	// The stale constraints come from the web view being resized (for the app
	// switcher snapshot) and back, so a resize is the most direct signal there
	// is that what the compositor recorded no longer matches the layout.
	// Re-arming is cheap - it schedules a few rect reads - so the keyboard
	// showing and hiding costing one extra check is a fine trade.
	window.addEventListener("resize", watch);
	window.addEventListener("orientationchange", watch);
};
