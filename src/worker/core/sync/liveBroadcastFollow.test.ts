import { assert, describe, test } from "vitest";
import {
	broadcastStillLiveAfterReconnect,
	chooseFocusGame,
	createFollowerHold,
	createRoomLiveCheck,
	createWakeClock,
	decideFollowAction,
	followGivenUp,
	LEFTOVER_OWN_MS,
	LEFTOVER_ROOM_MS,
	liveGameHold,
	MAX_JOIN_ATTEMPTS,
	mateLiveCall,
	QUIET_WATCH_MS,
	quietGameToCheck,
	quietGameVerdict,
	roomGameStillLive,
	shouldServeFollowedPayload,
	type FollowerHoldPatch,
} from "./liveBroadcastFollow.ts";

const live = { startedAt: 1000, gameOver: false };
const finished = { startedAt: 1000, gameOver: true };

describe("decideFollowAction", () => {
	test("a broadcast this device has never seen is not asked about or joined", () => {
		// It is in the header's live scores; a tap there is the way in.
		assert.strictEqual(decideFollowAction(live, undefined), "pill");
	});

	test("who ran the sim makes no difference - there is only one kind now", () => {
		// The whole point of the normalization: the decision cannot see, and does
		// not ask, whether the simmer was the device in charge of simming.
		assert.strictEqual(decideFollowAction(live, undefined), "pill");
		assert.strictEqual(decideFollowAction(finished, undefined), "ignore");
	});

	test("the next broadcast is watchable again, even after leaving the last", () => {
		assert.strictEqual(
			decideFollowAction(
				{ startedAt: 2000, gameOver: false },
				{ startedAt: 1000, left: true },
			),
			"pill",
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

	test("once its own sim ends, the next heartbeat treats it as normal", () => {
		// Nothing was recorded against the broadcast while the local sim played,
		// so the ordinary never-seen-it rule takes over.
		assert.strictEqual(decideFollowAction(live, undefined, false), "pill");
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
		// Asked, not answered: the page is not theirs yet either.
		assert.isFalse(
			shouldServeFollowedPayload({ startedAt: 1000, offered: true }, false),
		);
	});

	test("an offer waiting on an answer stays one - heartbeats never take the device in", () => {
		// The complaint this replaced: a league-mate's live sim pulling every
		// other device off what it was doing. Until "Watch" is clicked, every
		// heartbeat - the game going final included - only keeps the question up.
		const offered = { startedAt: 1000, offered: true };
		assert.strictEqual(decideFollowAction(live, offered), "offer");
		assert.strictEqual(decideFollowAction(finished, offered), "offer");
	});

	test("an own sim starting while asked turns the question into the pill", () => {
		const offered = { startedAt: 1000, offered: true };
		assert.strictEqual(decideFollowAction(live, offered, true), "pill");
		assert.strictEqual(decideFollowAction(finished, offered, true), "ignore");
	});

	test("a broadcast whose payload cannot be read stops being offered", () => {
		// THE FLICKER. A join that misses the payload leaves no follow record, so
		// every one of the broadcaster's heartbeats read as a broadcast never seen
		// before and started the join over - and each attempt freezes the header
		// before it fetches, so the whole room's header alternated between the
		// phase and "Live game in progress" two or three times a second, forever.
		// Retries are for a torn write, so they are finite.
		for (let failures = 0; failures < MAX_JOIN_ATTEMPTS; failures++) {
			assert.strictEqual(
				decideFollowAction(live, undefined, false, failures),
				"pill",
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
			"pill",
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

	test("watching theirs when its own sim starts: out to the pill, not stepped on through theirs", () => {
		// The field report: watching a league-mate's game, clicked to watch
		// their own - and the page froze, the league-mate's cursor still driving
		// it with their own game on it.
		assert.strictEqual(
			decideFollowAction(live, { startedAt: 1000 }, true),
			"pill",
		);
		assert.strictEqual(
			decideFollowAction(finished, { startedAt: 1000 }, true),
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

// Switching apps in the middle of a league-mate's game parks this one: no
// heartbeats can arrive. Coming back used to end the watch on the spot - the
// game jumped to its final and the score bar showed it.
describe("a quiet broadcast", () => {
	const LEASE = 12_000;
	const base = {
		expiresAt: 100_000,
		heardAt: 88_000,
		wokeAt: 0,
		leaseMs: LEASE,
	};

	test("a game being watched waits out silence instead of ending at the lease", () => {
		assert.isFalse(
			followGivenUp({ ...base, now: 100_000 + 60_000, watching: true }),
		);
		assert.isTrue(
			followGivenUp({
				...base,
				now: 88_000 + QUIET_WATCH_MS + 1,
				watching: true,
			}),
		);
	});

	test("time asleep is not silence: just back, the clock starts over", () => {
		// Ten minutes in another app, then back.
		const back = 88_000 + 10 * 60_000;
		assert.isFalse(
			followGivenUp({
				...base,
				now: back + 1_000,
				wokeAt: back,
				watching: true,
			}),
		);
		assert.isTrue(
			followGivenUp({
				...base,
				now: back + QUIET_WATCH_MS + 1,
				wokeAt: back,
				watching: true,
			}),
		);
	});

	test("an offer or a finished watch keeps the plain lease, but not across a nap", () => {
		assert.isTrue(followGivenUp({ ...base, now: 100_001, watching: false }));
		const back = 400_000;
		assert.isFalse(
			followGivenUp({
				...base,
				now: back + 5_000,
				wokeAt: back,
				watching: false,
			}),
		);
		assert.isTrue(
			followGivenUp({
				...base,
				now: back + LEASE + 1,
				wokeAt: back,
				watching: false,
			}),
		);
	});

	test("the wake clock notices a long gap between ticks", () => {
		let now = 1_000;
		const clock = createWakeClock(5_000, () => now);
		clock.tick();
		now += 5_000;
		clock.tick();
		assert.strictEqual(clock.wokeAt(), 0);
		// Parked for a minute.
		now += 60_000;
		// A listener delivery can beat the first tick back.
		assert.strictEqual(clock.wokeAt(), now);
		clock.tick();
		const woke = now;
		now += 5_000;
		clock.tick();
		assert.strictEqual(clock.wokeAt(), woke);
	});

	test("the app coming back to the front counts as waking", () => {
		let now = 1_000;
		const clock = createWakeClock(5_000, () => now);
		clock.tick();
		now += 2_000;
		clock.woke();
		assert.strictEqual(clock.wokeAt(), 3_000);
	});
});

// Two of the room's teams playing each other: one simmed it and broadcast it,
// the other tapped Watch on the same game on their schedule - and got their
// own playback of it at their own pace, a quarter ahead.
describe("mateLiveCall", () => {
	const now = 1_000_000;
	const on = { active: true, gameOver: false, gid: 7, expiresAt: now + 5000 };

	test("Watch on the game a league-mate is simming joins theirs; Sim is refused", () => {
		assert.strictEqual(mateLiveCall("liveGame", 7, on, now), "join");
		assert.strictEqual(mateLiveCall("simGame", 7, on, now), "refuse");
	});

	test("any other game, or no broadcast, is left alone", () => {
		assert.isUndefined(mateLiveCall("liveGame", 8, on, now));
		assert.isUndefined(mateLiveCall("liveGame", 7, undefined, now));
		assert.isUndefined(mateLiveCall("simToGame", 7, on, now));
	});

	test("nor once it is over, stopped, or gone quiet for good", () => {
		assert.isUndefined(
			mateLiveCall("liveGame", 7, { ...on, gameOver: true }, now),
		);
		assert.isUndefined(
			mateLiveCall("liveGame", 7, { ...on, active: false }, now),
		);
		assert.isUndefined(
			mateLiveCall(
				"liveGame",
				7,
				{ ...on, expiresAt: now - QUIET_WATCH_MS - 1 },
				now,
			),
		);
		// (Only a little quiet - their phone in the background: still theirs.)
		assert.strictEqual(
			mateLiveCall("liveGame", 7, { ...on, expiresAt: now - 30_000 }, now),
			"join",
		);
	});
});

describe("several live games at once", () => {
	const now = 100_000;
	const game = (startedAt: number, extra: object = {}) => ({
		startedAt,
		gameOver: false,
		expiresAt: now + 10_000,
		...extra,
	});

	test("the newest game not yet asked about is the one to ask about", () => {
		const games = [game(1), game(3), game(2)];
		assert.strictEqual(
			chooseFocusGame(games, undefined, new Set(), now)?.startedAt,
			3,
		);
		assert.strictEqual(
			chooseFocusGame(games, undefined, new Set([3]), now)?.startedAt,
			2,
		);
	});

	test("one being watched, or asked about, stays the one", () => {
		const games = [game(1), game(5)];
		assert.strictEqual(
			chooseFocusGame(games, { startedAt: 1 }, new Set([1]), now)?.startedAt,
			1,
		);
		assert.strictEqual(
			chooseFocusGame(games, { startedAt: 1, offered: true }, new Set([1]), now)
				?.startedAt,
			1,
		);
	});

	test("walking out of one moves on to a new one, else stays with it", () => {
		const left = { startedAt: 1, left: true };
		assert.strictEqual(
			chooseFocusGame([game(1), game(5)], left, new Set([1]), now)?.startedAt,
			5,
		);
		assert.strictEqual(
			chooseFocusGame([game(1)], left, new Set([1]), now)?.startedAt,
			1,
		);
	});

	test("over or gone quiet: not asked about", () => {
		assert.isUndefined(
			chooseFocusGame(
				[game(1, { gameOver: true }), game(2, { expiresAt: now - 1 })],
				undefined,
				new Set(),
				now,
			),
		);
	});

	test("a game is live for the room till its final, or a long silence", () => {
		assert.isTrue(roomGameStillLive({ gameOver: false, expiresAt: now }, now));
		assert.isFalse(roomGameStillLive({ gameOver: true, expiresAt: now }, now));
		assert.isTrue(
			roomGameStillLive(
				{ gameOver: false, expiresAt: now },
				now + QUIET_WATCH_MS - 1,
			),
		);
		assert.isFalse(
			roomGameStillLive(
				{ gameOver: false, expiresAt: now },
				now + QUIET_WATCH_MS,
			),
		);
		// (Time this device spent asleep doesn't count as the game's silence.)
		assert.isTrue(
			roomGameStillLive(
				{ gameOver: false, expiresAt: now },
				now + QUIET_WATCH_MS,
				now + 10,
			),
		);
	});
});

describe("a room game that went quiet", () => {
	const game = (expiresAt: number, gameOver = false) => ({
		gid: 5,
		startedAt: 1000,
		gameOver,
		expiresAt,
	});

	test("stays over when its screen comes back", () => {
		const check = createRoomLiveCheck(() => true);
		assert.isTrue(check.isLive(game(10_000), 10_000));
		const quiet = 10_000 + QUIET_WATCH_MS + 1;
		assert.isFalse(check.isLive(game(10_000), quiet));
		assert.isTrue(check.givenUp(1000));
		// (Heartbeating again, a fresh lease.)
		assert.isFalse(check.isLive(game(quiet + 12_000), quiet + 1));
	});

	test("is not given up on while this device can't hear the room", () => {
		let hearing = false;
		const check = createRoomLiveCheck(() => hearing);
		const quiet = 10_000 + QUIET_WATCH_MS + 1;
		assert.isFalse(check.isLive(game(10_000), quiet));
		assert.isFalse(check.givenUp(1000));
		hearing = true;
		assert.isTrue(check.isLive(game(quiet + 12_000), quiet + 1));
	});

	test("is not given up on for this device's own sleep", () => {
		const check = createRoomLiveCheck(() => true);
		const woke = 10_000 + QUIET_WATCH_MS + 1;
		assert.isTrue(check.isLive(game(10_000), woke + 1, woke));
		assert.isFalse(check.givenUp(1000));
	});

	test("a final is just over, not given up on", () => {
		const check = createRoomLiveCheck(() => true);
		assert.isFalse(check.isLive(game(10_000, true), 10_000));
		assert.isFalse(check.givenUp(1000));
	});
});

describe("a live game left behind", () => {
	const me = "me";
	const mate = "mate";
	const doc = (over: Partial<Parameters<typeof quietGameToCheck>[0]> = {}) => ({
		holderId: mate,
		gid: 5,
		startedAt: 1000,
		expiresAt: 100_000,
		gameOver: false,
		...over,
	});
	const nothingHere = () => false;

	test("a league mate's game is looked into once it has been quiet past the wait", () => {
		const check = (now: number) =>
			quietGameToCheck(doc(), { clientId: me, playingHere: nothingHere, now });
		assert.isFalse(check(100_000 + QUIET_WATCH_MS - 1));
		assert.isTrue(check(100_000 + QUIET_WATCH_MS));
	});

	test("this device's own leftover is looked into as soon as its lease is well gone", () => {
		const own = doc({ holderId: me });
		const check = (now: number) =>
			quietGameToCheck(own, { clientId: me, playingHere: nothingHere, now });
		assert.isFalse(check(100_000 + LEFTOVER_OWN_MS - 1));
		assert.isTrue(check(100_000 + LEFTOVER_OWN_MS));
	});

	test("never the game this device is playing", () => {
		assert.isFalse(
			quietGameToCheck(doc({ holderId: me }), {
				clientId: me,
				playingHere: (gid) => gid === 5,
				now: 100_000 + LEFTOVER_ROOM_MS * 2,
			}),
		);
	});

	test("quiet on the server past the wait: over here, whenever this device woke", () => {
		const heard = doc();
		const now = 100_000 + QUIET_WATCH_MS;
		assert.deepEqual(
			quietGameVerdict({
				heard,
				server: heard,
				serverUpdatedAt: 90_000,
				clientId: me,
				now,
			}),
			{ over: true, clear: false },
		);
		// And the room's to clear once it has been quiet a long while.
		assert.deepEqual(
			quietGameVerdict({
				heard,
				server: heard,
				serverUpdatedAt: 90_000,
				clientId: me,
				now: 100_000 + LEFTOVER_ROOM_MS,
			}),
			{ over: true, clear: true },
		);
	});

	test("nothing, when the server has heard from it since this device did", () => {
		const heard = doc();
		const now = 100_000 + LEFTOVER_ROOM_MS;
		assert.deepEqual(
			quietGameVerdict({
				heard,
				server: doc({ expiresAt: now + 12_000 }),
				serverUpdatedAt: now,
				clientId: me,
				now,
			}),
			{ over: false, clear: false },
		);
		// Gone already, or another broadcast of the game.
		assert.deepEqual(
			quietGameVerdict({
				heard,
				server: undefined,
				serverUpdatedAt: undefined,
				clientId: me,
				now,
			}),
			{ over: false, clear: false },
		);
		assert.deepEqual(
			quietGameVerdict({
				heard,
				server: doc({ startedAt: 2000 }),
				serverUpdatedAt: 90_000,
				clientId: me,
				now,
			}),
			{ over: false, clear: false },
		);
	});

	test("a simmer's slow clock can't get a game cleared early", () => {
		// The lease says long gone (the simmer's clock is 20 minutes behind);
		// the server took its last write a minute ago.
		const now = 10_000_000;
		const heard = doc({ expiresAt: now - 20 * 60 * 1000 });
		const verdict = quietGameVerdict({
			heard,
			server: heard,
			serverUpdatedAt: now - 60_000,
			clientId: me,
			now,
		});
		assert.isFalse(verdict.over);
		assert.isFalse(verdict.clear);
	});

	test("this device's own leftover is cleared, not just called over", () => {
		const heard = doc({ holderId: me });
		assert.deepEqual(
			quietGameVerdict({
				heard,
				server: heard,
				serverUpdatedAt: 90_000,
				clientId: me,
				now: 100_000 + LEFTOVER_OWN_MS,
			}),
			{ over: false, clear: true },
		);
	});

	test("a final left behind is cleared, never called over", () => {
		const heard = doc({ gameOver: true });
		assert.deepEqual(
			quietGameVerdict({
				heard,
				server: heard,
				serverUpdatedAt: 90_000,
				clientId: me,
				now: 100_000 + LEFTOVER_ROOM_MS,
			}),
			{ over: false, clear: true },
		);
	});

	test("called over by the server, a game stays over here", () => {
		const check = createRoomLiveCheck(() => true);
		const game = { gid: 5, startedAt: 1000, gameOver: false, expiresAt: 0 };
		// (Just woke: the wait would start over.)
		const now = QUIET_WATCH_MS * 10;
		assert.isTrue(check.isLive(game, now, now));
		check.giveUp(game);
		assert.isFalse(check.isLive(game, now, now));
		assert.isTrue(check.givenUp(1000));
	});
});

describe("the simmer's own game after a reconnect", () => {
	test("goes on if the room hasn't called it over", () => {
		assert.isTrue(broadcastStillLiveAfterReconnect(100_000, 100_000 + 60_000));
	});

	test("ends if it has been quiet about as long as the room waits", () => {
		assert.isFalse(
			broadcastStillLiveAfterReconnect(100_000, 100_000 + QUIET_WATCH_MS),
		);
		assert.isFalse(
			broadcastStillLiveAfterReconnect(
				100_000,
				100_000 + QUIET_WATCH_MS - 10_000,
			),
		);
	});

	test("never heard from at all: over", () => {
		assert.isFalse(broadcastStillLiveAfterReconnect(0, Date.now()));
	});
});

describe("what holds the league for a live game", () => {
	const now = 1_000_000;
	const hold = (
		over: Partial<Parameters<typeof liveGameHold>[0]> & {
			games?: { label?: string; playoffs?: boolean; live: boolean }[];
		},
	) =>
		liveGameHold({
			inPlayoffs: false,
			own: undefined,
			liveSimGid: undefined,
			now,
			isLive: (game: { live: boolean }) => game.live,
			...over,
			games: over.games ?? [],
		});

	test("a playoff game holds, even once its tip-off moved the phase on", () => {
		assert.strictEqual(
			hold({
				inPlayoffs: true,
				games: [{ label: "BOS @ NYK", playoffs: true, live: true }],
			}),
			"BOS @ NYK (live)",
		);
		// (The Finals' last game: the phase is the draft lottery already.)
		assert.strictEqual(
			hold({ games: [{ playoffs: true, live: true }] }),
			"A live game",
		);
	});

	test("a regular season game outside the playoffs holds nothing", () => {
		assert.isUndefined(hold({ games: [{ playoffs: false, live: true }] }));
	});

	test("a game that is over or given up on holds nothing", () => {
		assert.isUndefined(
			hold({ inPlayoffs: true, games: [{ playoffs: true, live: false }] }),
		);
	});

	test("this device's own playoff game holds only while its page beats", () => {
		const own = { gid: 7, playoffs: true, beatAt: now - 5000 };
		assert.strictEqual(hold({ own, liveSimGid: 7 }), "Your live game");
		// A flag left up by a page that never reported back.
		assert.isUndefined(
			hold({
				own: { ...own, beatAt: now - QUIET_WATCH_MS - 1 },
				liveSimGid: 7,
			}),
		);
		assert.isUndefined(hold({ own, liveSimGid: undefined }));
		assert.isUndefined(hold({ own, liveSimGid: 8 }));
		assert.isUndefined(
			hold({ own: { ...own, playoffs: false }, liveSimGid: 7 }),
		);
	});
});
