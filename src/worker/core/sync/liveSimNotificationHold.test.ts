import { assert, beforeEach, describe, test } from "vitest";
import {
	beginLiveSimNotificationHold,
	holdLiveSimNotifications,
	isLiveSimNotificationHoldActive,
	releaseLiveSimNotifications,
} from "./liveSimNotificationHold.ts";
import type { SyncNotification } from "./notifications.ts";

// THE INCIDENT: a playoff run watched game by game announced nothing to the
// room. The live-sim window is force-silent so a watcher's own device doesn't
// broadcast the score while they are still on Q1 - but "silent" was implemented
// as DROPPING the pushes, and the window closes seconds in, when the sim
// finishes computing, long before the playback ends. Nothing ever re-fired
// them. The results synced; the room was simply never told.

const notif = (title: string, body = "b"): SyncNotification => ({
	title,
	body,
	targetTids: null,
});

describe("live sim notification hold", () => {
	beforeEach(() => {
		// Drain anything a previous test left behind.
		releaseLiveSimNotifications();
	});

	test("nothing is held until a live sim arms it", () => {
		assert.strictEqual(isLiveSimNotificationHoldActive(), false);
	});

	test("what is held during the playback comes back when it ends", () => {
		beginLiveSimNotificationHold();
		assert.strictEqual(isLiveSimNotificationHoldActive(), true);

		holdLiveSimNotifications([notif("Celtics 102, Heat 98")]);
		holdLiveSimNotifications([notif("Celtics lead the series 3-2")]);

		const released = releaseLiveSimNotifications();
		assert.deepStrictEqual(
			released.map((n) => n.title),
			["Celtics 102, Heat 98", "Celtics lead the series 3-2"],
		);
		// And the hold is over, so the next ordinary sim notifies live.
		assert.strictEqual(isLiveSimNotificationHoldActive(), false);
	});

	test("releasing twice does not resend", () => {
		beginLiveSimNotificationHold();
		holdLiveSimNotifications([notif("Celtics 102, Heat 98")]);
		assert.strictEqual(releaseLiveSimNotifications().length, 1);
		assert.strictEqual(releaseLiveSimNotifications().length, 0);
	});

	test("duplicates from repeated drains collapse to one push", () => {
		// A playback's own navigation spawns worker calls that drain the tracker,
		// so the same game summary can be built more than once inside one window.
		beginLiveSimNotificationHold();
		holdLiveSimNotifications([notif("Celtics 102, Heat 98")]);
		holdLiveSimNotifications([notif("Celtics 102, Heat 98")]);
		holdLiveSimNotifications([notif("Celtics 102, Heat 98", "different body")]);

		const released = releaseLiveSimNotifications();
		assert.strictEqual(released.length, 2, "same title+body collapses, ...");
		assert.strictEqual(
			released[1]!.body,
			"different body",
			"...but a different body is a different push",
		);
	});

	test("a new live sim does not inherit the previous game's pushes", () => {
		beginLiveSimNotificationHold();
		holdLiveSimNotifications([notif("Game 5")]);
		beginLiveSimNotificationHold();
		holdLiveSimNotifications([notif("Game 6")]);

		assert.deepStrictEqual(
			releaseLiveSimNotifications().map((n) => n.title),
			["Game 6"],
		);
	});

	test("an expired hold stops swallowing new pushes but never drops old ones", () => {
		// The cap exists so a device that dies mid-playback can't silence itself
		// forever. It must bound HOLDING, not RELEASING - an expired window that
		// threw its batch away would be the original bug with extra steps.
		beginLiveSimNotificationHold();
		holdLiveSimNotifications([notif("Celtics 102, Heat 98")]);

		const realNow = Date.now;
		Date.now = () => realNow() + 31 * 60 * 1000;
		try {
			assert.strictEqual(
				isLiveSimNotificationHoldActive(),
				false,
				"expired, so afterAction stops holding new pushes",
			);
			assert.deepStrictEqual(
				releaseLiveSimNotifications().map((n) => n.title),
				["Celtics 102, Heat 98"],
				"but the batch already held still goes out",
			);
		} finally {
			Date.now = realNow;
		}
	});
});
