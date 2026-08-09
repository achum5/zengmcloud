import { assert, describe, test } from "vitest";
import {
	isNotificationClickMessage,
	NOTIFICATION_CLICK_MESSAGE,
	resolveDeepLink,
} from "./notificationDeepLink.ts";

const msg = (path?: string, url?: string) => ({
	type: NOTIFICATION_CLICK_MESSAGE as typeof NOTIFICATION_CLICK_MESSAGE,
	path,
	url,
});

describe("resolveDeepLink", () => {
	test("prepends the league this window is actually in", () => {
		// The point of sending a league-RELATIVE path: lids differ per device, so
		// the window's own lid wins over anything the worker guessed.
		assert.strictEqual(
			resolveDeepLink(msg("game_log/ATL/2007/42"), "/l/7/standings"),
			"/l/7/game_log/ATL/2007/42",
		);
	});

	test("ignores the worker's lid when this window has one of its own", () => {
		assert.strictEqual(
			resolveDeepLink(msg("standings", "/l/99/standings"), "/l/7/roster"),
			"/l/7/standings",
		);
	});

	test("falls back to the worker's URL outside a league", () => {
		assert.strictEqual(
			resolveDeepLink(msg("standings", "/l/3/standings"), "/dashboard"),
			"/l/3/standings",
		);
	});

	test("a bare root URL is not worth navigating to", () => {
		assert.strictEqual(resolveDeepLink(msg("", "/"), "/l/7/roster"), undefined);
	});

	test("no path and no URL means stay put", () => {
		assert.strictEqual(resolveDeepLink(msg(), "/l/7/roster"), undefined);
	});

	test("a leading slash on the path doesn't produce a double slash", () => {
		assert.strictEqual(
			resolveDeepLink(msg("/trade_summary/12"), "/l/7/roster"),
			"/l/7/trade_summary/12",
		);
	});

	test("only matches a league id at the start of the path", () => {
		// "/l/5" appearing later in the URL must not be read as the league.
		assert.strictEqual(
			resolveDeepLink(msg("standings", "/l/3/standings"), "/dashboard/l/5"),
			"/l/3/standings",
		);
	});
});

describe("isNotificationClickMessage", () => {
	test("accepts our own message", () => {
		assert.strictEqual(isNotificationClickMessage(msg("standings")), true);
	});

	test("rejects anything else on the service worker message channel", () => {
		// Workbox and the sync engine post here too.
		assert.strictEqual(
			isNotificationClickMessage({ type: "SKIP_WAITING" }),
			false,
		);
		assert.strictEqual(isNotificationClickMessage(null), false);
		assert.strictEqual(
			isNotificationClickMessage("zengm-notification-click"),
			false,
		);
	});
});
