import { assert, describe, test } from "vitest";
import { isRefreshNavigation } from "./refreshNavigation.ts";

describe("isRefreshNavigation", () => {
	test("no url at all is always a refresh of what is on screen", () => {
		assert.isTrue(
			isRefreshNavigation({ url: undefined, pathname: "/l/15/draft" }),
		);
	});

	test("a page carrying query parameters is still the same page", () => {
		// The case that was wrong: realtimeUpdate passes no url, the page has a
		// filter in its query string, and this used to come back false - so a
		// sync-driven data refresh was treated as walking away from the page.
		assert.isTrue(
			isRefreshNavigation({
				url: undefined,
				pathname: "/l/15/draft",
			}),
			"a refresh does not stop being one because the URL has a search",
		);
	});

	test("the same path, given explicitly, is a refresh", () => {
		assert.isTrue(
			isRefreshNavigation({ url: "/l/15/draft", pathname: "/l/15/draft" }),
		);
	});

	test("a different page is a navigation", () => {
		assert.isFalse(
			isRefreshNavigation({ url: "/l/15/roster", pathname: "/l/15/draft" }),
		);
	});
});
