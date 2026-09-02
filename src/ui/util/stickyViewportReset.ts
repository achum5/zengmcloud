// THE GHOST VIEWPORT, NAMED AND REPAIRED.
//
// Every broken sticky-header report from the field shares one shape: a layout
// viewport bigger than the screen (518x1052 on a 440x956 glass) with
// visualViewport.scale reporting 0.85, the number that would make 518 fit 440.
// For a long time that read as "the page is zoomed out". The touch probe
// (stickyTouchProbe.ts) finally measured the truth from ordinary taps: the
// page is drawn at scale 1.02, one-to-one, inside that oversized layout
// viewport. The reported scale is stale. The visual viewport is a 646pt window
// panning inside a 1052pt layout viewport, and position:sticky - which pins to
// the LAYOUT viewport - never moves while the glass pans over it.
//
// Nothing about the bars is wrong, which is why every element-level repair has
// failed and why the watchdog's "unrepairable" verdict is correct. The only
// thing that can act on the state is something that makes WebKit recompute
// the layout viewport, and the one such action available from inside a page
// is rewriting the viewport meta tag.
//
// The dangerous case is a page the user REALLY pinch-zoomed: its reported and
// measured scales agree, and resetting it would undo something the user did.
// So the automatic path requires the probe's confirmation - taps that say the
// glass disagrees with the report - and the manual button, being the user
// saying "this is broken", accepts the structural evidence alone.

export type ViewportReading = {
	innerWidth: number;
	innerHeight: number;
	screenWidth: number;
	screenHeight: number;
	reportedScale: number | undefined;
	// From the touch probe. Undefined when there have not been enough taps.
	touchScale: number | undefined;
};

// A layout viewport that does not fit the screen, with a reported scale that
// would make it fit. This is the shape of both a real pinch-out and the ghost;
// on its own it says only that a reset is PLAUSIBLE.
export const viewportOversized = (v: ViewportReading): boolean =>
	v.reportedScale !== undefined &&
	v.reportedScale < 0.95 &&
	(v.innerHeight > v.screenHeight + 8 || v.innerWidth > v.screenWidth + 8);

// The ghost, confirmed: oversized as above AND the taps say the page is
// actually drawn near one-to-one. A real pinch-out measures the same scale it
// reports; only the ghost disagrees with itself.
export const isGhostViewport = (v: ViewportReading): boolean =>
	viewportOversized(v) &&
	v.touchScale !== undefined &&
	Number.isFinite(v.touchScale) &&
	v.touchScale > 0.92 &&
	v.touchScale < 1.12;

export const readViewport = (
	touchScale: number | undefined,
): ViewportReading => ({
	innerWidth: window.innerWidth,
	innerHeight: window.innerHeight,
	screenWidth: window.screen?.width ?? window.innerWidth,
	screenHeight: window.screen?.height ?? window.innerHeight,
	reportedScale: window.visualViewport?.scale,
	touchScale,
});

const VIEWPORT_CONTENT =
	"width=device-width, initial-scale=1, minimum-scale=1, shrink-to-fit=no";

// Make WebKit recompute the layout viewport. Setting the meta content to a
// pinned scale and then restoring it is the documented way to force a
// re-evaluation; a no-op write does not trigger one. Returns what changed so
// the report can say whether it worked, which the field has taught is the
// least a repair can do.
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

const nextFrame = () =>
	new Promise<void>((resolve) => {
		requestAnimationFrame(() => resolve());
	});

// THE TRIGGER, CAUGHT IN THE ACT. The expansion is transient and data-driven:
// probing every reported page afterwards found no standing overflow, so the
// only way to name the culprit is to be listening when innerWidth grows past
// the screen. Fires the callback once per expansion with the page it happened
// on, which is what the next report needs to say.
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
