import { assert, describe, test } from "vitest";
import hold from "../../../../functions/hold.js";

const at = (ms: number) => ({ toMillis: () => ms });

describe("the push hold, the Cloud Function's half", () => {
	test("held only while the device says so, and said so lately", () => {
		const now = 10_000_000;
		const since = at(now - 60_000);
		assert.isTrue(
			hold.isHeld(
				{ gameHold: true, gameHoldAt: at(now - 1000), gameHoldSince: since },
				now,
			),
		);
		assert.isFalse(
			hold.isHeld(
				{ gameHold: false, gameHoldAt: at(now - 1000), gameHoldSince: since },
				now,
			),
		);
		// Gone dark mid game: its pushes go out again.
		assert.isFalse(
			hold.isHeld(
				{
					gameHold: true,
					gameHoldAt: at(now - hold.LIVE_HOLD_LEASE_MS),
					gameHoldSince: since,
				},
				now,
			),
		);
		// Held on and on - kept up, but no game lasts that long: not held.
		assert.isFalse(
			hold.isHeld(
				{
					gameHold: true,
					gameHoldAt: at(now - 1000),
					gameHoldSince: at(now - hold.HOLD_MOST_MS),
				},
				now,
			),
		);
		// (An old build, never saying when it began: holds nothing.)
		assert.isFalse(
			hold.isHeld({ gameHold: true, gameHoldAt: at(now - 1000) }, now),
		);
		// (Stamp not written by the server yet.)
		assert.isFalse(hold.isHeld({ gameHold: true, gameHoldAt: null }, now));
		assert.isFalse(hold.isHeld({ fcmToken: "x" }, now));
		assert.isFalse(hold.isHeld(undefined, now));
	});

	test("keeps the latest few", () => {
		let list: unknown[] = [];
		for (let i = 0; i < hold.MAX_HELD + 3; i++) {
			list = hold.withHeld(list, hold.toHeld({ title: "T", body: `${i}` }, i));
		}
		assert.lengthOf(list, hold.MAX_HELD);
		assert.strictEqual(
			(list.at(-1) as { body: string }).body,
			`${hold.MAX_HELD + 2}`,
		);
		assert.strictEqual((list[0] as { body: string }).body, "3");
		// (Junk in the doc is left out.)
		assert.lengthOf(hold.heldList([null, 3, { title: "a" }, "x"]), 0);
		assert.lengthOf(hold.heldList(undefined), 0);
	});

	test("sent as one push when the game is over", () => {
		const now = 50_000_000;
		const one = hold.combine(
			[
				hold.toHeld(
					{ title: "Playoff scores", body: "CLE 3-1 IND", path: "/a" },
					now,
				),
			],
			now,
		);
		assert.deepEqual(one, {
			title: "Playoff scores",
			body: "CLE 3-1 IND",
			path: "/a",
		});

		const several = hold.combine(
			[
				hold.toHeld(
					{ title: "Playoff scores", body: "CLE 3-1 IND", path: "/a" },
					now - 3000,
				),
				hold.toHeld(
					{ title: "Trade", body: "BOS traded", path: "/b" },
					now - 2000,
				),
				hold.toHeld(
					{ title: "Playoff scores", body: "CLE 4-1 IND", path: "/c" },
					now - 1000,
				),
			],
			now,
		);
		assert.deepEqual(several, {
			title: "Playoff scores · Trade",
			body: "CLE 3-1 IND\nBOS traded\nCLE 4-1 IND",
			path: "/c",
		});
	});

	test("old news is dropped, not sent", () => {
		const now = 100_000_000;
		const stale = hold.toHeld(
			{ title: "Playoff scores", body: "old" },
			now - hold.HELD_MAX_AGE_MS - 1,
		);
		assert.isUndefined(hold.combine([stale], now));
		assert.strictEqual(
			hold.combine(
				[stale, hold.toHeld({ title: "Trade", body: "new" }, now)],
				now,
			)?.body,
			"new",
		);
		assert.isUndefined(hold.combine([], now));
	});

	test("never too long for a push", () => {
		const now = 1;
		const list = Array.from({ length: hold.MAX_HELD }, (_, i) =>
			hold.toHeld({ title: `Title number ${i}`, body: "x".repeat(400) }, now),
		);
		const out = hold.combine(list, now);
		assert.isAtMost(out!.title.length, 80);
		assert.isAtMost(out!.body.length, 1500);
	});
	test("the devices with pushes held are noted by room and device", () => {
		const key = hold.holdKey("room-1", "abc|def");
		assert.deepEqual(hold.fromKey(key), { code: "room-1", uid: "abc|def" });
		assert.isUndefined(hold.fromKey("nothing"));
	});
});
