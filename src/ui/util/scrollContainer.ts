// The app scrolls inside the #content element (an app-shell scroll container),
// NOT the window/document. This is what keeps the fixed header rock-solid on
// iOS: a plain element scroll container can't desync from the visual viewport
// the way document scrolling does after the PWA is backgrounded and resumed.
//
// Anywhere that used to read/write window scroll or listen for a window
// "scroll" event must go through here instead. getBoundingClientRect() and
// IntersectionObserver(root: null) stay correct as-is - they're viewport
// relative and fire on any ancestor scroll - so ONLY the scroll listeners and
// the scrollTo/scrollY/scrollBy calls needed to move.

export const SCROLL_CONTAINER_ID = "content";

// The scroll container, with a defensive fallback to the document scroller for
// any early call before #content exists (e.g. during initial mount).
export const getScrollEl = (): HTMLElement =>
	document.getElementById(SCROLL_CONTAINER_ID) ??
	(document.scrollingElement as HTMLElement | null) ??
	document.documentElement;

export const getScrollTop = (): number => getScrollEl().scrollTop;

export const scrollAppTo = (options: ScrollToOptions): void => {
	getScrollEl().scrollTo(options);
};

export const scrollAppBy = (options: ScrollToOptions): void => {
	getScrollEl().scrollBy(options);
};

// Subscribe to the app's scroll; returns an unsubscribe. Mirrors
// addEventListener so call sites read naturally.
export const onAppScroll = (
	handler: () => void,
	options?: AddEventListenerOptions,
): (() => void) => {
	const el = getScrollEl();
	el.addEventListener("scroll", handler, options);
	return () => el.removeEventListener("scroll", handler);
};
