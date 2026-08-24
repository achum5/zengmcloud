// Is a realtimeUpdate a REFRESH of the page already on screen, or a move to a
// different one?
//
// A leaf module with no imports, so the answer can be tested without dragging
// in the view manager, the router and the worker bridge behind it.
//
// The distinction is load-bearing rather than cosmetic. useBlocker reads it to
// tell a data refresh from someone walking away from unsaved work, and when it
// reads "navigation" it puts up a confirm dialog and may refuse - and a refused
// navigation returns from the router silently, without updating the view or
// reporting anything.
//
// The version this replaces compared `pathname + search` against `pathname`,
// which is equal only when there is no search. So every page carrying query
// parameters - a table's filter, a season, a team abbrev - had every one of its
// data refreshes classified as a navigation, contradicting realtimeUpdate's own
// documented contract that an absent url means a refresh.
export const isRefreshNavigation = ({
	url,
	pathname,
}: {
	// The url realtimeUpdate was called with, if any.
	url: string | undefined;
	// window.location.pathname.
	pathname: string;
}): boolean => url === undefined || url === pathname;
