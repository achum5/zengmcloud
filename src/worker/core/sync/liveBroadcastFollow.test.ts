import { assert, describe, test } from "vitest";
import {
	createFollowerHold,
	decideFollowAction,
	type FollowerHoldPatch,
	MAX_JOIN_ATTEMPTS,
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

	test("a broadcast whose payload cannot be read stops being joined", () => {
		// THE FLICKER. A join that misses the payload leaves no follow record, so
		// every one of the broadcaster's heartbeats read as a broadcast never seen
		// before and started the join over - and each attempt freezes the header
		// before it fetches, so the whole room's header alternated between the
		// phase and "Live game in progress" two or three times a second, forever.
		// Retries are for a torn write, so they are finite.
		for (let failures = 0; failures < MAX_JOIN_ATTEMPTS; failures++) {
			assert.strictEqual(
				decideFollowAction(live, undefined, false, failures),
				"join",
				`still worth a try after ${failures} failures`,
			);
		}
		assert.strictEqual(
			decideFollowAction(live, undefined, false, MAX_JOIN_ATTEMPTS),
			"ignore",
		);
		// And it stays given up on, however many more heartbeats arrive.
		assert.strictEqual(
			decideFollowAction(live, undefined, false, MAX_JOIN_ATTEMPTS + 40),
			"ignore",
		);
	});

	test("giving up does not offer the pill either", () => {
		// "ignore" rather than "pill" on purpose: a button that reopens the same
		// unreadable payload is a broken button. This is also the one case where
		// a device mid-sim of its own game is NOT pilled.
		assert.strictEqual(
			decideFollowAction(live, undefined, true, MAX_JOIN_ATTEMPTS),
			"ignore",
		);
	});

	test("a device already watching is unaffected by an old failure count", () => {
		// Failures are cleared on a successful join, so this combination should
		// not arise - but if it ever did, the rule must not eject somebody from a
		// playback that is running fine.
		assert.strictEqual(
			decideFollowAction(live, { startedAt: 1000 }, false, 0),
			"cursor",
		);
	});

	test("failures are counted per broadcast, not for the room", () => {
		// The next live sim gets its own startedAt and its own fresh chances -
		// one unreadable payload must not make the room stop watching games.
		assert.strictEqual(
			decideFollowAction(
				{ startedAt: 2000, gameOver: false },
				undefined,
				false,
				0,
			),
			"join",
		);
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

describe("createFollowerHold", () => {
	const setup = (ownLiveSimUnderway: () => boolean = () => false) => {
		const pushes: FollowerHoldPatch[] = [];
		let painted = 0;
		const hold = createFollowerHold({
			push: (patch) => {
				pushes.push(patch);
			},
			ownLiveSimUnderway,
			afterRelease: () => {
				painted += 1;
			},
		});
		return { hold, pushes, painted: () => painted };
	};

	const RELEASED = { mpLiveBroadcast: undefined, liveGameInProgress: false };

	test("joining takes the hold once, however many times the join retries", () => {
		const { hold, pushes } = setup();
		hold.take();
		hold.take();
		hold.take();
		assert.strictEqual(hold.isHeld(), true);
		assert.deepStrictEqual(pushes, [{ liveGameInProgress: true }]);
	});

	test("a follow ending releases the hold and paints what was held back", () => {
		const { hold, pushes, painted } = setup();
		hold.take();
		assert.strictEqual(hold.release(), "released");
		assert.strictEqual(hold.isHeld(), false);
		assert.deepStrictEqual(pushes.at(-1), RELEASED);
		assert.strictEqual(painted(), 1);
	});

	test("a release with nothing held still clears the flag - a hold must never stick", () => {
		// The broadcast ended after this device walked out (leaving released the
		// hold, the record stayed so Leave would stick). Releasing again costs
		// nothing and guarantees no path can strand the header on.
		const { hold, pushes, painted } = setup();
		assert.strictEqual(hold.release(), "released");
		assert.deepStrictEqual(pushes, [RELEASED]);
		assert.strictEqual(painted(), 1);
	});

	test("the followed game going final leaves the release to the page's own report", () => {
		const { hold, pushes } = setup();
		hold.take();
		hold.markOver();
		assert.strictEqual(hold.isHeld(), false);
		// Nothing pushed here: onLiveSimOver from the live game page does it.
		assert.deepStrictEqual(pushes, [{ liveGameInProgress: true }]);
	});

	test("a stale follow never releases the hold this device's own live sim owns", () => {
		// The field failure: pulled into a league-mate's live sim, left it to
		// watch your own game, and their broadcast ended while yours was on Q3.
		// The record of their broadcast was still there, and its ending used to
		// drop liveGameInProgress - final score in the header, mid-game.
		let ownSim = false;
		const { hold, pushes, painted } = setup(() => ownSim);
		hold.take();
		hold.release(); // walked out: released, as it should be
		assert.deepStrictEqual(pushes, [{ liveGameInProgress: true }, RELEASED]);

		ownSim = true; // "Watch my game"
		assert.strictEqual(hold.release(), "kept-for-own-sim");
		assert.strictEqual(pushes.length, 2, "nothing pushed to the UI");
		assert.strictEqual(painted(), 1, "nothing painted behind the playback");
		assert.strictEqual(hold.isHeld(), false);
	});

	test("a hold still held when the own sim starts drops its broadcast state, not the flag", () => {
		let ownSim = false;
		const { hold, pushes, painted } = setup(() => ownSim);
		hold.take();
		ownSim = true;
		assert.strictEqual(hold.release(), "kept-for-own-sim");
		assert.deepStrictEqual(pushes.at(-1), { mpLiveBroadcast: undefined });
		assert.strictEqual(painted(), 0);
		assert.strictEqual(hold.isHeld(), false);
	});

	test("once the own sim is over, the next follow ending releases as normal", () => {
		let ownSim = true;
		const { hold, pushes, painted } = setup(() => ownSim);
		assert.strictEqual(hold.release(), "kept-for-own-sim");
		ownSim = false;
		assert.strictEqual(hold.release(), "released");
		assert.deepStrictEqual(pushes, [RELEASED]);
		assert.strictEqual(painted(), 1);
	});

	test("a session boundary forgets the hold without touching the UI", () => {
		const { hold, pushes } = setup();
		hold.take();
		hold.reset();
		assert.strictEqual(hold.isHeld(), false);
		assert.deepStrictEqual(pushes, [{ liveGameInProgress: true }]);
	});
});
