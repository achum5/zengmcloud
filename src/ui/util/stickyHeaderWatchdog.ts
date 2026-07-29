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

const HEADER_SELECTOR = ".navbar-border.sticky-top";

// Sub-pixel rounding and zoom mean the rect is never exactly on the line.
const TOLERANCE_PX = 2;

// How long after a resume to keep watching. Layout settles late on a resume -
// the first frame back can be measured against the pre-suspend viewport - so a
// single check right away misses it.
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
	// the body (see .ios-modal-pinned) - not a fault, and not repairable.
	pinnedByModal: () => boolean;
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
	// Above where it should be sticking = it scrolled away with the page.
	return top < stickyTop() - TOLERANCE_PX;
};

const getHeader = () => document.querySelector<HTMLElement>(HEADER_SELECTOR);

const computedStickyTop = (element: HTMLElement): number => {
	const top = Number.parseFloat(window.getComputedStyle(element).top);
	return Number.isFinite(top) ? top : 0;
};

const detached = (element: HTMLElement) =>
	headerIsDetached({
		scrollY: () => window.scrollY,
		headerTop: () => element.getBoundingClientRect().top,
		stickyTop: () => computedStickyTop(element),
		pinnedByModal: () => document.body.classList.contains("ios-modal-pinned"),
	});

// Each step below flushes layout between mutations by reading a geometry
// property. Layout is not paint - the browser paints once at the end of the
// task - so the header never renders in the intermediate state and nothing
// flashes.
//
// Every step restores `position` to "" rather than to whatever was there
// before. The header's position must come from the stylesheet (.sticky-top, or
// the modal's fixed override); putting back a stale inline value would reinstate
// exactly the broken state we are trying to clear.

// Step 1: rebuild the sticky node. WebKit only recomputes a sticky element's
// constraints when its position changes, so this is the cheapest thing that can
// possibly work.
const rebuildStickyNode = (element: HTMLElement) => {
	element.style.position = "relative";
	void element.offsetTop;
	element.style.position = "";
	void element.offsetTop;
};

// Step 2: tear the element out of the box tree entirely and put it back. This
// destroys its renderer, its compositing layer and its scrolling-tree node,
// rather than asking for them to be recalculated - the difference matters when
// the stale state is in the compositor rather than in layout.
const rebuildRenderer = (element: HTMLElement) => {
	element.style.display = "none";
	void element.offsetTop;
	element.style.display = "";
	element.style.position = "";
	void element.offsetTop;
};

// Step 3: the scrolling tree itself is stale, not the element. Scrolling by a
// pixel and back makes WebKit reconcile it against the real scroll position.
// Visually a no-op, and it only ever runs on a header still measurably broken
// after both rebuilds.
const nudgeScroller = () => {
	const y = window.scrollY;
	window.scrollTo(window.scrollX, y + 1);
	window.scrollTo(window.scrollX, y);
};

// Escalate only as far as it takes, re-measuring after each step. Cheap when the
// first step works, and it never runs at all on a healthy header.
export const repairSteps = [
	rebuildStickyNode,
	rebuildRenderer,
	nudgeScroller,
] as const;

let stop: (() => void) | undefined;

const check = () => {
	const element = getHeader();
	if (!element || !detached(element)) {
		return;
	}

	// Synchronous, so the whole ladder resolves inside one task and the user
	// never sees an intermediate state.
	rebuildStickyNode(element);
	if (!detached(element)) {
		return;
	}

	rebuildRenderer(element);
	if (!detached(element)) {
		return;
	}

	nudgeScroller();
	if (!detached(element)) {
		return;
	}

	// Measuring inside the same task can read constraints the compositor has not
	// applied yet, so give it a frame and run the ladder again before giving up
	// on this pass. Whatever happens, the scroll listener keeps watching.
	requestAnimationFrame(() => {
		const again = getHeader();
		if (again && detached(again)) {
			rebuildRenderer(again);
			nudgeScroller();
		}
	});
};

// Watch for a while after a resume: on the next frame, at a few fixed delays,
// and on any scroll (the only way to catch a resume that happened at the top of
// the page, where a broken header is indistinguishable from a working one until
// you move).
const watch = () => {
	stop?.();

	const timers = CHECK_DELAYS_MS.map((delay) => setTimeout(check, delay));
	const frame = requestAnimationFrame(check);
	window.addEventListener("scroll", check, { passive: true });

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
		window.removeEventListener("scroll", check);
	};
};

export const initStickyHeaderWatchdog = () => {
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
};
