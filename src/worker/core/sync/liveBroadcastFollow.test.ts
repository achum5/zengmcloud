import { assert, describe, test } from "vitest";
import {
	decideFollowAction,
	shouldServeFollowedPayload,
} from "./liveBroadcastFollow.ts";

const live = { startedAt: 1000, gameOver: false };
const finished = { startedAt: 1000, gameOver: true };

describe("decideFollowAction", () => {
	test("a broadcast this device has never seen pulls it in", () => {
		assert.strictEqual(decideFollowAction(live, undefined), "join");
	});

	test("who ran the sim makes no difference - there is only one kind now", () => {
		// The whole point of the normalization: the decision cannot see, and does
		// not ask, whether the simmer was the device in charge of simming.
		assert.strictEqual(decideFollowAction(live, undefined), "join");
		assert.strictEqual(decideFollowAction(finished, undefined), "join");
	});

	test("the next broadcast pulls it in again, even after leaving the last", () => {
		assert.strictEqual(
			decideFollowAction(
				{ startedAt: 2000, gameOver: false },
				{ startedAt: 1000, left: true },
			),
			"join",
		);
	});

	test("while watching, heartbeats only move the cursor - never re-navigate", () => {
		assert.strictEqual(decideFollowAction(live, { startedAt: 1000 }), "cursor");
		assert.strictEqual(
			decideFollowAction(finished, { startedAt: 1000 }),
			"cursor",
		);
	});

	test("after leaving, heartbeats offer the way back instead of taking it", () => {
		// This is what makes Leave stick: the broadcaster is still heartbeating
		// several times a second, and every one of those used to be a chance to
		// drag the viewer back onto the live game page.
		assert.strictEqual(
			decideFollowAction(live, { startedAt: 1000, left: true }),
			"pill",
		);
	});

	test("no invitation back into a game that has already gone final", () => {
		assert.strictEqual(
			decideFollowAction(finished, { startedAt: 1000, left: true }),
			"ignore",
		);
	});

	test("a device mid-sim of its OWN game is pilled, never pulled", () => {
		// Two live sims can run at once now (ownGameSimGate). The room's
		// broadcast must not yank this device out of its own playback - and
		// every heartbeat is such a chance, same as the Leave case above.
		assert.strictEqual(decideFollowAction(live, undefined, true), "pill");
	});

	test("once its own sim ends, the next heartbeat joins as normal", () => {
		// Nothing was recorded against the broadcast while the local sim played,
		// so the ordinary never-seen-it rule takes over.
		assert.strictEqual(decideFollowAction(live, undefined, false), "join");
	});

	test("the page is only served the broadcast while actually inside it", () => {
		// The field bug: leave a broadcast, live-sim your OWN game, and the live
		// game page - re-running without its one-shot payload - was handed the
		// broadcast's cached game instead of yours. The payload must only be
		// served while genuinely following: not after leaving, and never while
		// this device's own live sim owns the page.
		assert.isTrue(shouldServeFollowedPayload({ startedAt: 1000 }, false));
		assert.isFalse(
			shouldServeFollowedPayload({ startedAt: 1000, left: true }, false),
		);
		assert.isFalse(shouldServeFollowedPayload({ startedAt: 1000 }, true));
		assert.isFalse(shouldServeFollowedPayload(undefined, false));
	});

	test("its own sim does not disturb an earlier Leave decision", () => {
		// Left the broadcast, then started an own-game sim: still just the pill,
		// and still no invitation back into a finished game.
		assert.strictEqual(
			decideFollowAction(live, { startedAt: 1000, left: true }, true),
			"pill",
		);
		assert.strictEqual(
			decideFollowAction(finished, { startedAt: 1000, left: true }, true),
			"ignore",
		);
	});
});
