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
	initVisualViewportHeader,
	resyncStickyBarShifts,
	tickerVisualShift,
} from "./visualViewportHeader.ts";

const HEADER_SELECTOR = ".navbar-border.sticky-top";

// THE BOTTOM TICKER HAS THE SAME DISEASE, and by now the same treatment: it is
// position:sticky against the document, exactly like the header, for exactly the
// reason in the history above. So it inherits the same failure - WebKit hands
// the compositor constraints computed against a layout that no longer exists,
// and the bar rides up the page with the content and stays there.
//
// Same detection shape, same repair ladder, same watch, and the same standing
// visual-viewport correction (visualViewportHeader.ts). Only the measurement
// differs, because what a bottom bar must hold still is its bottom edge against
// the foot of the viewport rather than its top edge against the top.
const TICKER_SELECTOR = ".league-ticker";

// Sub-pixel rounding and zoom mean the rect is never exactly on the line.
const TOLERANCE_PX = 2;

// The bottom bar gets a looser one. Its expected position is derived from the
// viewport height, and on iOS that number moves while the URL bar collapses and
// expands - a few pixels of disagreement mid-transition is the browser working,
// not the bar breaking. A real detachment is tens or hundreds of pixels.
const BOTTOM_TOLERANCE_PX = 6;

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

type BottomDeps = {
	// Viewport-relative bottom edge of the bar.
	barBottom: () => number;
	// EVERY height the foot of the viewport can legitimately be at. Plural, and
	// that is the entire lesson of the second field failure (below).
	anchorHeights: () => number[];
	pinnedByModal: () => boolean;
	visualOffsetTop: () => number;
};

// Is a bar stuck to the bottom of the window provably not there?
//
// Unlike the header this is answerable at any scroll position: a bottom bar
// belongs at the foot of the viewport always, so there is no equivalent of "at
// the top of the document the two cases look the same".
//
// Both directions count. A stale compositor rule leaves the bar wherever it was
// last composited, and the page can then scroll either way underneath it.
//
// WHY THE ANCHOR IS A SET OF HEIGHTS AND NOT ONE NUMBER. The first version
// measured against documentElement.clientHeight alone. On iOS that number
// stays at the SMALL viewport - the one with the URL bar showing - while the
// layout viewport that sticky is actually anchored to GROWS by the toolbar's
// height (60-100px) the moment the user scrolls and the toolbar collapses. So
// scrolling put every healthy ticker 60-100px past the 6px tolerance, the
// watchdog declared it detached, and the repair ladder went to work on a bar
// with nothing wrong: position:relative for a frame (the bar visibly parked
// mid-content - photographed by a user), display:none for another, a scroll
// nudge mid-flick, "gave-up", and again on the next scroll event. The watchdog
// WAS the detachment.
//
// So the question is now "is the bar at ANY position a healthy bar can be at?"
// - the static document height, the dynamic innerHeight, and the visible
// bottom. A truly detached bar rides the content hundreds of pixels from all
// three; a healthy one is always within tolerance of one of them, whatever the
// toolbar is doing.
export const bottomBarIsDetached = ({
	barBottom,
	anchorHeights,
	pinnedByModal,
	visualOffsetTop,
}: BottomDeps): boolean => {
	// A modal pins the page and can legitimately move things; and the repair
	// ladder nudges the scroll position, which is the last thing a pinned page
	// needs. The unpin check picks it up afterwards.
	if (pinnedByModal()) {
		return false;
	}
	const bottom = barBottom();
	if (!Number.isFinite(bottom)) {
		return false;
	}
	// Same correction as the header, for the same reason: getBoundingClientRect
	// reports against the VISUAL viewport while sticky is anchored to the LAYOUT
	// one, so a bar doing exactly the right thing on a panned or zoomed
	// page reads short by the offset between them.
	const rawOffset = visualOffsetTop();
	const offset = Number.isFinite(rawOffset) ? rawOffset : 0;
	const anchors = anchorHeights().filter(
		(height) => Number.isFinite(height) && height > 0,
	);
	if (anchors.length === 0) {
		return false;
	}
	return anchors.every(
		(height) => Math.abs(bottom - (height - offset)) > BOTTOM_TOLERANCE_PX,
	);
};

// Where sticky ACTUALLY put the bar, with our own standing correction taken
// back out of the reading.
//
// The two mechanisms have to agree or they fight. The standing correction
// pushes the bar down by the visual offset so it lands at the foot of what the
// user can see; the rule above asks whether the bar is at the foot of the
// LAYOUT viewport, which is where sticky puts it. A healthy corrected bar
// therefore reads a full offset past what the rule expects, and the rule would
// call the correction a detachment and tear the bar down to repair it - the
// false-detachment incident, rebuilt. Subtracting the shift we applied
// ourselves keeps the rule asking the only honest question. Pure, so the
// relationship is a test.
export const stickyBottomAsPlaced = (
	renderedBottom: number,
	offsetTop: number | undefined,
): number => renderedBottom - tickerVisualShift(offsetTop);

// Every height the foot of the viewport can legitimately be at, minus one that
// cannot be trusted: a resume can hand back a documentElement.clientHeight
// still sized for the app-switcher snapshot. When it reads SMALLER than
// innerHeight by more than any toolbar transition is worth, it is not a place a
// healthy bar can honestly be - and left in the set, it vouches for a bar
// parked mid-screen and detection never fires. Only the small side is
// distrusted: a keyboard can legitimately shrink innerHeight below
// clientHeight, never the reverse. Pure, so the rule is a test.
export const tickerAnchorHeights = ({
	client,
	innerHeight,
	vvBottom,
}: {
	client: number;
	innerHeight: number;
	vvBottom: number;
}): number[] => {
	const clientStale = innerHeight - client > 150;
	return [clientStale ? Number.NaN : client, innerHeight, vvBottom];
};

const computedStickyTop = (element: HTMLElement): number => {
	const top = Number.parseFloat(window.getComputedStyle(element).top);
	return Number.isFinite(top) ? top : 0;
};

// Element-agnostic on purpose: the pin marker moved from <body> to the app
// wrapper, and this only ever asks "is a modal currently pinning something?".
const modalPinning = () => document.querySelector(PINNED_SELECTOR) !== null;

const layoutViewportHeight = () => {
	const clientHeight = document.documentElement?.clientHeight;
	return Number.isFinite(clientHeight) && clientHeight > 0
		? clientHeight
		: window.innerHeight;
};

// ONE WATCHDOG, TWO BARS.
//
// The header and the ticker break the same way and are repaired by the same
// ladder; all that differs is which edge has to hold still and where a reading
// means anything. A bar is those two answers plus how to find the element.
type Bar = {
	name: string;
	get: () => HTMLElement | null;
	// The viewport-relative edge that must not move. Compared across two frames
	// to tell a real fault from a mid-scroll reading.
	edge: (element: HTMLElement) => number;
	detached: (element: HTMLElement) => boolean;
	// Is a reading at this scroll position capable of establishing anything?
	answerable: (scrollY: number) => boolean;
};

const HEADER_BAR: Bar = {
	name: "header",
	get: () => document.querySelector<HTMLElement>(HEADER_SELECTOR),
	edge: (element) => element.getBoundingClientRect().top,
	detached: (element) =>
		headerIsDetached({
			scrollY: () => window.scrollY,
			headerTop: () => element.getBoundingClientRect().top,
			stickyTop: () => computedStickyTop(element),
			pinnedByModal: modalPinning,
			visualOffsetTop: () => window.visualViewport?.offsetTop ?? 0,
		}),
	answerable: (scrollY) => scrollDecision({ scrollY }) === "judge",
};

const TICKER_BAR: Bar = {
	name: "ticker",
	get: () => document.querySelector<HTMLElement>(TICKER_SELECTOR),
	edge: (element) => element.getBoundingClientRect().bottom,
	detached: (element) =>
		bottomBarIsDetached({
			// Measured as sticky left it, not as it renders - see above.
			barBottom: () =>
				stickyBottomAsPlaced(
					element.getBoundingClientRect().bottom,
					window.visualViewport?.offsetTop,
				),
			// Every place a healthy bottom edge can be: the static document
			// height, the dynamic innerHeight (iOS grows it when the toolbar
			// collapses - the false-detachment incident), and the visible bottom
			// (where the standing visual-viewport correction parks the bar).
			anchorHeights: () => {
				const vv = window.visualViewport;
				return tickerAnchorHeights({
					client: layoutViewportHeight(),
					innerHeight: window.innerHeight,
					vvBottom: vv ? vv.offsetTop + vv.height : Number.NaN,
				});
			},
			pinnedByModal: modalPinning,
			visualOffsetTop: () => window.visualViewport?.offsetTop ?? 0,
		}),
	// A bottom bar is out of place at the top of the document just as visibly as
	// anywhere else, so there is no position where the question is unanswerable.
	answerable: () => true,
};

const BARS: readonly Bar[] = [HEADER_BAR, TICKER_BAR];

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

// One ladder at a time PER BAR. The steps await frames, so a scroll arriving
// mid-repair could otherwise start a second pass that fights the first - but the
// header and the ticker are independent elements and must not block each other.
const repairing = new Set<string>();

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
// to paste and every line means something. The bar's name goes in the kind, so
// one log covers both without two sets of entries that look alike.
const note = (
	bar: Bar,
	element: HTMLElement | null,
	kind: string,
	detail?: string,
) => {
	recordHeaderEvent({
		kind: `${bar.name}:${kind}`,
		scrollY: Math.round(window.scrollY),
		headerTop: element ? Math.round(bar.edge(element)) : Number.NaN,
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
	before: { scrollY: number; edge: number };
	after: { scrollY: number; edge: number };
}): boolean =>
	before.scrollY === after.scrollY &&
	Math.abs(before.edge - after.edge) <= TOLERANCE_PX;

const reading = (bar: Bar, element: HTMLElement) => ({
	scrollY: Math.round(window.scrollY),
	edge: Math.round(bar.edge(element)),
});

const checkBar = async (bar: Bar, trigger: string) => {
	if (repairing.has(bar.name)) {
		return;
	}
	const element = bar.get();
	if (!element) {
		return;
	}
	if (!bar.answerable(window.scrollY) || !bar.detached(element)) {
		return;
	}

	// Before believing the geometry, make sure the standing visual-viewport
	// correction is not the geometry. A stale translateY left over from before a
	// suspend displaces the bar exactly like a detachment, and the position
	// ladder below can never remove it - the watchdog would detect, fail to
	// repair, and give up on every check forever. Re-deriving the shift from the
	// viewports as they are now either clears it (and the re-measure comes back
	// healthy) or changes nothing and the real ladder proceeds.
	resyncStickyBarShifts();
	if (!bar.detached(element)) {
		note(bar, element, "repaired", "shift-resync");
		return;
	}

	// Confirm against a second reading a frame later before believing it.
	const before = reading(bar, element);
	await nextFrame();
	const after = reading(bar, element);
	if (!bar.detached(element) || !detachmentConfirmed({ before, after })) {
		note(
			bar,
			element,
			"unconfirmed",
			`via=${trigger} moved=${after.scrollY - before.scrollY}`,
		);
		return;
	}

	note(bar, element, "detached", `via=${trigger} ${viewportNote()}`);

	repairing.add(bar.name);
	try {
		for (const [i, step] of repairSteps.entries()) {
			await step(element);
			if (!bar.detached(element)) {
				note(bar, element, "repaired", `step=${i + 1}`);
				return;
			}
		}

		// Still broken after the whole ladder. One more frame, in case the
		// compositor simply had not caught up when we measured, then a final pass
		// before giving up on this attempt - the scroll watch keeps looking either
		// way, so a bar that survives this will be tried again on the next scroll
		// rather than staying broken until the app is force-quit.
		await nextFrame();
		if (bar.detached(element)) {
			await rebuildRenderer(element);
			await nudgeScroller();
		}
		note(bar, element, bar.detached(element) ? "gave-up" : "repaired", "late");
	} finally {
		repairing.delete(bar.name);
		// The ladder clears inline `position` so the stylesheet rules; when the
		// oversized-viewport self-placement has the ticker positioned inline (see
		// visualViewportHeader.ts), that clearing strips it. Re-derive placement
		// from the viewports as they are now, whatever the ladder did.
		resyncStickyBarShifts();
	}
};

// Both bars, every time. They are checked in sequence rather than concurrently
// so that at most one repair ladder is disturbing the scroll position at once.
const check = async (trigger = "scroll") => {
	for (const bar of BARS) {
		await checkBar(bar, trigger);
	}
};

const forceBarRepair = async (bar: Bar) => {
	const element = bar.get();
	if (!element) {
		return;
	}
	note(
		bar,
		element,
		"forced",
		`detached=${bar.detached(element)} ${viewportNote()}`,
	);
	if (repairing.has(bar.name)) {
		return;
	}
	repairing.add(bar.name);
	try {
		for (const step of repairSteps) {
			await step(element);
		}
	} finally {
		repairing.delete(bar.name);
	}
	// Same reason as checkBar: the ladder cleared inline `position`, and the
	// self-placement mode lives in inline styles.
	resyncStickyBarShifts();
	note(bar, element, "forced-done");
};

// The manual button. Runs the ladder whether or not the bars LOOK broken,
// because the one place the user can reach the button - the top of the page - is
// the one place a broken header is indistinguishable from a healthy one.
export const forceHeaderRepair = async () => {
	// Put the bars back inside the visible viewport first. When the two
	// viewports have come apart this IS the reset - the ladder below cannot help,
	// because nothing about the element is wrong.
	resyncStickyBarShifts();
	for (const bar of BARS) {
		await forceBarRepair(bar);
	}
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
			// No scroll gate here any more: it was the header's rule, and the ticker
			// is measurable at the top of the document where the header is not. Each
			// bar's own `answerable` now decides whether its reading means anything.
			void check("scroll-settled");
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
		note(HEADER_BAR, HEADER_BAR.get(), "resume", viewportNote());
	}

	// First thing on any resume: rebuild both bars' visual-viewport shifts from
	// the world as it is now. Suspension is exactly when the standing correction
	// goes stale with no event left to refresh it - see resyncStickyBarShifts.
	resyncStickyBarShifts();

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

// On a REAL resume (app switcher, back/forward cache), rebuild the ticker's
// renderer without asking whether it looks broken. Two reasons this is exempt
// from the detect-before-act rule everything else here follows: a resume is
// exactly when detection can be blind (a stale snapshot-sized viewport makes a
// parked bar measure as healthy - the anchor note above), and it is exactly
// when acting is free, because the whole screen is repainting from the app
// switcher's snapshot and a one-frame rebuild of a bottom bar cannot be seen.
// Runs once immediately and once after layout has settled - constraints
// rebuilt against the first frame back can be recorded against the
// pre-suspend viewport and go stale again the moment the real one lands.
//
// The ticker only. The header has never needed it, and its detect-first watch
// demonstrably works there; unconditional toggling of an element that is
// visible at the top of every resumed screen is not worth trading that for.
const RESUME_REBUILD_SETTLE_MS = 800;
let lastTickerResumeRebuild = 0;

const rebuildTickerOnResume = () => {
	const now = Date.now();
	if (now - lastTickerResumeRebuild < RESUME_DEDUPE_MS) {
		return;
	}
	lastTickerResumeRebuild = now;

	const pass = async () => {
		const element = TICKER_BAR.get();
		if (!element || modalPinning() || repairing.has(TICKER_BAR.name)) {
			return;
		}
		repairing.add(TICKER_BAR.name);
		try {
			await rebuildRenderer(element);
		} finally {
			repairing.delete(TICKER_BAR.name);
		}
		// The rebuild cleared inline `position`; put the self-placement (or the
		// ordinary shift) straight back rather than waiting for a viewport event.
		resyncStickyBarShifts();
	};

	note(TICKER_BAR, TICKER_BAR.get(), "resume-rebuild", viewportNote());
	void pass();
	setTimeout(() => {
		void pass();
	}, RESUME_REBUILD_SETTLE_MS);
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
			rebuildTickerOnResume();
			watch();
		}
	});

	// pageshow covers a restore from the back/forward cache, which is how iOS
	// often brings a suspended PWA back; focus covers the cases where neither
	// fires (returning from a share sheet, a system prompt, split view).
	window.addEventListener("pageshow", () => {
		rebuildTickerOnResume();
		watch();
	});
	window.addEventListener("focus", watch);

	// The stale constraints come from the web view being resized (for the app
	// switcher snapshot) and back, so a resize is the most direct signal there
	// is that what the compositor recorded no longer matches the layout.
	// Re-arming is cheap - it schedules a few rect reads - so the keyboard
	// showing and hiding costing one extra check is a fine trade.
	window.addEventListener("resize", watch);
	window.addEventListener("orientationchange", watch);
};
