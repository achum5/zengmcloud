// THE VIEWPORT THAT IS LYING, AND WHICH NUMBER IS THE LIE.
//
// Every broken sticky-bar report from the field shares one shape: a 518x1052
// layout viewport on a 440x956pt screen at scale 0.85, and a visual viewport
// 646pt tall inside it. Two things are wrong there and they are different
// faults.
//
// THE ZOOM IS REAL. 518 = 440/0.85 and 1052 = (956 - 62pt status bar)/0.85:
// the page is uniformly zoomed out, the whole layout viewport is on the glass,
// and it looks right because 15% is not much. iOS does this when something
// once rendered wider than the screen (shrink-to-fit) and keeps it until the
// app is actually relaunched, which a home-screen app almost never is. The
// touch probe's claim that the page was drawn at scale 1.02 was an artefact:
// WebKit's touch.screenY does not track pinch zoom, so it can only ever
// measure about 1. That reading is no longer used to decide anything.
//
// THE HEIGHT IS THE LIE. 1052 - 646 = 406 layout px = 345pt, which is the
// height of the keyboard on this phone. The visual viewport shrank for a
// keyboard and never grew back - a known WebKit fault in standalone apps - and
// iOS then lets the visual viewport PAN 406px inside the layout viewport
// before the document scrolls. position:sticky pins to the layout viewport,
// so during that pan the header slides up off the glass and stays there.
// Two field builds that treated vv.height as the bottom of the screen put the
// ticker in the middle of it, which is how it is known the glass shows the
// full 1052 and the 646 is stale.
//
// So there are two repairs, and the stale height is the one that matters:
// with it restored the visual viewport cannot pan, and sticky is simply
// right. The manual button attempts both; the automatic path attempts the
// height restore on the user's next touch, which is the gesture WebKit needs.

export type ViewportReading = {
	innerWidth: number;
	innerHeight: number;
	screenWidth: number;
	screenHeight: number;
	reportedScale: number | undefined;
	vvHeight: number | undefined;
	vvOffsetTop: number | undefined;
	// Whether something that summons a keyboard has focus. If it does, a short
	// visual viewport is the keyboard, not a fault.
	editableFocused: boolean;
};

// A layout viewport that does not fit the screen, with a reported scale that
// would make it fit: the zoom. Prevented by shrink-to-fit=no after a relaunch;
// the meta rewrite below is the only in-page action that could clear it, and
// on the field device it did nothing - kept because it is logged and cheap.
export const viewportOversized = (v: ViewportReading): boolean =>
	v.reportedScale !== undefined &&
	v.reportedScale < 0.95 &&
	(v.innerHeight > v.screenHeight + 8 || v.innerWidth > v.screenWidth + 8);

// The visual viewport is shorter than the layout viewport by more than any
// browser chrome, and nothing has a keyboard up: the stale inset.
const STALE_DEFICIT_PX = 150;
export const visualViewportStale = (v: ViewportReading): boolean =>
	v.vvHeight !== undefined &&
	Number.isFinite(v.vvHeight) &&
	v.innerHeight - v.vvHeight > STALE_DEFICIT_PX &&
	!v.editableFocused;

const EDITABLE = new Set(["INPUT", "TEXTAREA", "SELECT"]);
export const editableIsFocused = (): boolean => {
	const el = document.activeElement;
	return (
		el instanceof HTMLElement &&
		(EDITABLE.has(el.tagName) || el.isContentEditable)
	);
};

export const readViewport = (): ViewportReading => ({
	innerWidth: window.innerWidth,
	innerHeight: window.innerHeight,
	screenWidth: window.screen?.width ?? window.innerWidth,
	screenHeight: window.screen?.height ?? window.innerHeight,
	reportedScale: window.visualViewport?.scale,
	vvHeight: window.visualViewport?.height,
	vvOffsetTop: window.visualViewport?.offsetTop,
	editableFocused: editableIsFocused(),
});

const nextFrame = () =>
	new Promise<void>((resolve) => {
		requestAnimationFrame(() => resolve());
	});

const VIEWPORT_CONTENT =
	"width=device-width, initial-scale=1, minimum-scale=1, shrink-to-fit=no";

// Ask WebKit to recompute the layout viewport by rewriting the meta tag. On
// the field device this was a no-op (518x1052 before and after); it stays
// because it is the only such lever, it is logged, and it costs three frames.
export const resetLayoutViewport = async (): Promise<{
	before: {
		innerWidth: number;
		innerHeight: number;
		scale: number | undefined;
	};
	after: { innerWidth: number; innerHeight: number; scale: number | undefined };
	applied: boolean;
}> => {
	const read = () => ({
		innerWidth: window.innerWidth,
		innerHeight: window.innerHeight,
		scale: window.visualViewport?.scale,
	});
	const before = read();
	const meta = document.querySelector<HTMLMetaElement>('meta[name="viewport"]');
	if (!meta) {
		return { before, after: before, applied: false };
	}
	meta.setAttribute(
		"content",
		"width=device-width, initial-scale=1, minimum-scale=1, maximum-scale=1, shrink-to-fit=no",
	);
	await nextFrame();
	await nextFrame();
	meta.setAttribute("content", VIEWPORT_CONTENT);
	await nextFrame();
	return { before, after: read(), applied: true };
};

// Make WebKit recompute the visual viewport's height. What un-sticks a
// keyboard inset in the field is the keyboard's own bookkeeping: focusing and
// blurring an editable element, inside a user gesture. The element is read-
// only with inputmode none so no keyboard is actually summoned, and a no-op
// scroll is issued as well because that alone has cleared it on some builds.
// Returns the height before and after so the report can say whether it took.
export const restoreVisualViewport = async (): Promise<{
	before: number | undefined;
	after: number | undefined;
}> => {
	const before = window.visualViewport?.height;
	window.scrollTo(window.scrollX, window.scrollY);

	const input = document.createElement("input");
	input.readOnly = true;
	input.setAttribute("inputmode", "none");
	input.setAttribute("aria-hidden", "true");
	input.tabIndex = -1;
	input.style.cssText =
		"position:fixed;top:0;left:0;width:1px;height:1px;opacity:0;pointer-events:none;";
	document.body.append(input);
	try {
		input.focus({ preventScroll: true });
		await nextFrame();
		input.blur();
	} finally {
		input.remove();
	}
	await nextFrame();
	await nextFrame();
	return { before, after: window.visualViewport?.height };
};

// THE TRIGGER, CAUGHT IN THE ACT. The zoom is set by something that once
// rendered wider than the screen, and probing every reported page afterwards
// has never found it. Being there when innerWidth grows past the screen names
// the page it happened on.
export const watchViewportExpansion = (
	onExpand: (detail: string) => void,
): (() => void) => {
	let lastWidth = window.innerWidth;
	const check = () => {
		const width = window.innerWidth;
		const screenWidth = window.screen?.width ?? width;
		if (width > lastWidth && width > screenWidth + 8) {
			onExpand(
				`innerWidth ${lastWidth}->${width} screen=${screenWidth} scale=${window.visualViewport?.scale} url=${location.pathname}`,
			);
		}
		lastWidth = width;
	};
	window.addEventListener("resize", check);
	window.visualViewport?.addEventListener("resize", check);
	return () => {
		window.removeEventListener("resize", check);
		window.visualViewport?.removeEventListener("resize", check);
	};
};
