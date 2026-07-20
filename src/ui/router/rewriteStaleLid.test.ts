import { assert, describe, test } from "vitest";
import { rewriteStaleLid } from "./rewriteStaleLid.ts";

// After export + re-import a league gets a new lid; content links (game recaps,
// events, feats) still carry the OLD lid. rewriteStaleLid retargets them to the
// league currently being viewed so they don't 404 with "League not found".
describe("rewriteStaleLid", () => {
	test("retargets a stale in-league link to the current league", () => {
		assert.strictEqual(
			rewriteStaleLid("/l/5/player/123", "/l/8/game_log/BOS/2001/42"),
			"/l/8/player/123",
		);
	});

	test("leaves a same-league link untouched", () => {
		assert.strictEqual(
			rewriteStaleLid("/l/8/player/123", "/l/8/roster"),
			"/l/8/player/123",
		);
	});

	test("does not touch links when not inside a league (dashboard cross-links)", () => {
		// From the non-league dashboard, /l/2 legitimately opens league 2.
		assert.strictEqual(rewriteStaleLid("/l/2", "/"), "/l/2");
		assert.strictEqual(rewriteStaleLid("/l/2/roster", "/account"), "/l/2/roster");
	});

	test("ignores non-league target paths", () => {
		assert.strictEqual(rewriteStaleLid("/account", "/l/8/roster"), "/account");
		assert.strictEqual(rewriteStaleLid("/new_league", "/l/8/roster"), "/new_league");
	});

	test("preserves query string and hash while rewriting the lid", () => {
		assert.strictEqual(
			rewriteStaleLid("/l/5/player/9?foo=1#bar", "/l/8/roster"),
			"/l/8/player/9?foo=1#bar",
		);
	});

	test("handles the bare league-dashboard path", () => {
		assert.strictEqual(rewriteStaleLid("/l/5", "/l/8/roster"), "/l/8");
	});

	test("does not mistake a longer lid prefix (l/50 vs l/5)", () => {
		assert.strictEqual(
			rewriteStaleLid("/l/50/player/1", "/l/5/roster"),
			"/l/5/player/1",
		);
	});
});
