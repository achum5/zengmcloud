// WHO GETS PULLED INTO A LIVE SIM, AND WHEN.
//
// Every live sim in the room is offered to the whole room: "Watch PHO @ DAL
// live?" on every other screen, its result held back there till that is
// answered - watched, or turned down. The device in charge of simming working
// through the schedule and a league-mate playing out their own game produce
// the same experience on every other screen, because they are the same event:
// a game happening right now. (Nobody is navigated into one any more without
// saying so: being taken off whatever you were doing to watch somebody else's
// game was the complaint.)
//
// And anyone may walk out of any of them.
// Walking out has to STICK: the broadcaster heartbeats its cursor two or three
// times a second, and a rule that only asked "is this device on the live game
// page" would fling them straight back in before they reached whatever they
// left to go and do. So the decision is made against a record of what this
// device has already done with THIS broadcast, identified by startedAt (the
// simmer's clock, new for every sim).

export type FollowRecord =
	| {
			startedAt: number;
			left?: boolean;
			// Offered, not answered yet (the popup is up).
			offered?: boolean;
	  }
	| undefined;

export type FollowAction =
	// A broadcast this device has not seen before: ask whether to watch it,
	// holding its result back till that is answered - or, asked already, go on
	// waiting for the answer.
	| "offer"
	// Already inside it: step playback to the simmer's cursor.
	| "cursor"
	// Walked out of this one. Offer the way back rather than taking it for
	// them - the header pill.
	| "pill"
	// Walked out, and it has gone final since. There is nothing left to go back
	// to, only a score to be spoiled by, so no invitation.
	| "ignore";

// Should a load of the live game page be served the followed broadcast's
// payload? The payload outlives the follow on purpose - it is what the header
// pill rejoins with - so "the payload exists" is not the question. The page
// belongs to the broadcast only while this device is actually INSIDE it:
// after walking out (left), or while this device's OWN live sim is playing,
// the page is somebody else's, and serving the broadcast there is how
// clicking "watch my game" opened a league-mate's game instead. (The pill
// rejoin clears `left` before it navigates, so it still gets the payload.)
export const shouldServeFollowedPayload = (
	followed: FollowRecord,
	localSimActive: boolean,
): boolean =>
	followed !== undefined &&
	!followed.left &&
	!followed.offered &&
	!localSimActive;

// How many times a device will try to fetch one broadcast's payload before it
// gives up on that broadcast.
//
// A join that cannot read the payload leaves NO follow record - there is
// nothing to follow - so before this existed every heartbeat looked like a
// broadcast this device had never seen and started the join over. That is fine
// for a payload that is merely late, and an unbounded loop for one that is
// GONE: the chunk docs live in fixed per-room slots, so a second broadcast
// overwrites them and either device ending one deletes them, while the meta doc
// keeps saying active for as long as someone is still heartbeating it. And
// because a join freezes the header BEFORE it fetches, each retry flashed "Live
// game in progress" over the phase - two or three times a second, on every
// device in the room at once, for as long as the heartbeat lasted.
//
// The payload chunks are written before the meta doc flips active, so a
// complete payload is always readable for a broadcast that really is live.
// These attempts only cover a torn write; past them, the broadcast is treated
// as not being there at all.
export const MAX_JOIN_ATTEMPTS = 3;

// A Watch or Sim of one game on this device, while a league-mate's broadcast
// is live: of that very game - both teams the room's, so this device's own
// game too - Watch joins their broadcast and Sim is refused. Playing it a
// second time here ran on at this device's own pace, out of step with them
// (the room's two players a quarter apart in the same game). Anything else:
// undefined, carry on.
export const mateLiveCall = (
	name: string,
	gid: unknown,
	broadcast:
		| {
				active: boolean;
				gameOver: boolean;
				gid: number;
				expiresAt: number;
		  }
		| undefined,
	now: number,
): "join" | "refuse" | undefined => {
	if (
		(name !== "liveGame" && name !== "simGame") ||
		!broadcast ||
		!broadcast.active ||
		broadcast.gameOver ||
		now - broadcast.expiresAt >= QUIET_WATCH_MS ||
		gid !== broadcast.gid
	) {
		return undefined;
	}
	return name === "liveGame" ? "join" : "refuse";
};

export const decideFollowAction = (
	broadcast: { startedAt: number; gameOver: boolean },
	followed: FollowRecord,
	// A live sim of this device's OWN game is playing right now (two can run at
	// once - see ownGameSimGate). Never navigated out from under: the pill is
	// offered instead, and since nothing is recorded against the broadcast, the
	// first heartbeat after the local sim ends offers it as normal.
	localSimActive = false,
	// Joins already attempted and failed against THIS broadcast, by startedAt.
	// Counted outside the follow record because a failed join is precisely the
	// case where there is no record to count on - see connect.ts.
	joinFailures = 0,
): FollowAction => {
	if (joinFailures >= MAX_JOIN_ATTEMPTS) {
		return "ignore";
	}
	if (!followed || followed.startedAt !== broadcast.startedAt) {
		return localSimActive ? "pill" : "offer";
	}
	if (followed.left) {
		return broadcast.gameOver ? "ignore" : "pill";
	}
	// Inside somebody else's game when a live sim of this device's own started:
	// its page is about to play THAT game, so stepping it on through theirs is
	// what froze it - their cursor driving a page with no game of theirs on it.
	// The way back is the pill, the same as having walked out.
	if (localSimActive) {
		return broadcast.gameOver ? "ignore" : "pill";
	}
	if (followed.offered) {
		return "offer";
	}
	return "cursor";
};

// A QUIET BROADCAST IS NOT AN ENDED ONE.
//
// A phone parks an app it is not showing: no timers, no listener deliveries.
// Switch away to answer a text in the middle of a league-mate's game, come
// back, and the last heartbeat this device heard is long past its lease - not
// because the broadcast is gone, but because nothing could be heard. Treating
// that as the end dropped the watch: the page went straight to the final and
// the score bar showed it. The broadcaster's own phone going into the
// background is the same silence from the other side.
//
// So a game being watched only ends when the broadcast says so (it goes
// inactive or away), or after this long with nothing heard WHILE this device
// was awake to hear it. Until then the page holds where it was, the way the
// broadcaster's own screen does, and picks up at their cursor when they are
// heard from again. A crashed broadcaster still lets go - just not in seconds.
export const QUIET_WATCH_MS = 3 * 60 * 1000;

// When this device last came back from being parked. Ticked by something
// that runs only while awake (the health tick): a gap far longer than its
// period is time spent asleep. The UI saying the app is in front again
// counts too, and a gap noticed between ticks counts at once - a listener
// delivery can beat the first tick back.
export const createWakeClock = (
	tickMs: number,
	now: () => number = Date.now,
) => {
	const SLEPT_MS = 3 * tickMs;
	let lastTick: number | undefined;
	let wokeAt = 0;
	const check = () => {
		const t = now();
		if (lastTick !== undefined && t - lastTick > SLEPT_MS) {
			wokeAt = t;
		}
		return t;
	};
	return {
		tick: () => {
			lastTick = check();
		},
		woke: () => {
			wokeAt = now();
		},
		wokeAt: () => {
			check();
			return wokeAt;
		},
	};
};

// Whether a follow record's broadcast has gone quiet for good on this device.
// A game being watched gets QUIET_WATCH_MS of awake silence; anything else (a
// popup still asking, a game already over or walked out of here) the plain
// lease - but neither runs while this device was asleep, nor in the first
// lease after it woke, when the listener has not had its chance yet.
export const followGivenUp = ({
	now,
	expiresAt,
	heardAt,
	wokeAt,
	watching,
	leaseMs,
}: {
	now: number;
	// The broadcaster's lease on its last heartbeat heard here.
	expiresAt: number;
	// When this device last heard the broadcast live (its own clock).
	heardAt: number;
	wokeAt: number;
	// Inside the game, part way through it.
	watching: boolean;
	leaseMs: number;
}): boolean => {
	if (watching) {
		return now - Math.max(heardAt, wokeAt) > QUIET_WATCH_MS;
	}
	return now > Math.max(expiresAt, wokeAt + leaseMs);
};

// THE FOLLOWER'S HALF OF THE SPOILER HOLD.
//
// liveGameInProgress - the flag that freezes the score bar and the ticker and
// hides everything a result could leak through - has two owners. A live sim of
// this device's OWN game takes it before the game is written (play.ts) and
// releases it when the playback ends (onLiveSimOver). A broadcast this device
// is watching takes it on join and releases it when the follow ends - here.
//
// The two overlap, because a follow record outlives its hold on purpose:
// walking out of a broadcast has to stick against heartbeats arriving three
// times a second, so the record stays until that broadcast is gone - and by
// then this device may be mid-way through a live sim of its own. Every way a
// follow ended used to release the flag on the strength of the record alone,
// which is how a league-mate's broadcast ending dropped the hold in the middle
// of YOUR game: pulled into their sim, left it to watch your own, and the
// moment theirs went final your score was in the header at Q3 - on every
// device that had done the same, at once.
//
// So a follow ending releases the flag only while no local live sim owns it.
// Otherwise the follow is forgotten and the flag is left exactly as it is; the
// local sim's own end releases it.
export type FollowerHoldPatch =
	| { liveGameInProgress: true }
	| { mpLiveBroadcast: undefined }
	| { mpLiveBroadcast: undefined; liveGameInProgress: false };

export const createFollowerHold = ({
	push,
	ownLiveSimUnderway,
	afterRelease,
}: {
	// Push a patch to the UI's local state.
	push: (patch: FollowerHoldPatch) => void;
	// A live sim of this device's own game is underway: requested, being
	// simmed, or playing back.
	ownLiveSimUnderway: () => boolean;
	// Paint whatever remote applies held back while the hold was up.
	afterRelease: () => void;
}) => {
	let held = false;
	return {
		isHeld: () => held,
		// Take the hold for a broadcast this device is joining. Idempotent: a
		// retried join neither re-pushes nor flashes the header a second time.
		take: () => {
			if (held) {
				return;
			}
			held = true;
			push({ liveGameInProgress: true });
		},
		// The followed game went final on this screen. The page reports that
		// itself (onLiveSimOver), and that report releases the flag; only the
		// bookkeeping happens here.
		markOver: () => {
			held = false;
		},
		// The follow is over: the broadcast ended or expired, or this device
		// walked out of it. Pushed whether or not this follow still held the
		// flag - a hold that somehow outlived its follow must never stick -
		// EXCEPT while a local live sim owns it.
		release: (): "released" | "kept-for-own-sim" => {
			const wasHeld = held;
			held = false;
			if (ownLiveSimUnderway()) {
				if (wasHeld) {
					// The broadcast state this follow put on screen must not outlive
					// it; the flag, though, is the local sim's now.
					push({ mpLiveBroadcast: undefined });
				}
				return "kept-for-own-sim";
			}
			push({ mpLiveBroadcast: undefined, liveGameInProgress: false });
			afterRelease();
			return "released";
		},
		// A session boundary (connect, teardown): the UI is reset separately.
		reset: () => {
			held = false;
		},
	};
};

// A LEAGUE-MATE'S LIVE GAME, ITS RESULT HERE BEFORE ITS BROADCAST.
//
// The result of a live-simmed game syncs in on its own, and usually lands
// well ahead of the broadcast that offers it to this device - which
// goes live only once the whole play-by-play has uploaded. Nothing was held in
// that gap, so the score bar, the ticker and the status line repainted with
// the final before the game had started on this screen. The apply layer now
// says so the moment one arrives (noticeLiveResults), and this holds the paint
// just as joining its broadcast would, and waits for that broadcast. The hold
// ends the way any watch does - the game going final on this screen, or
// walking out of it (settle) - or, if no broadcast ever gets this device
// watching it (another was already live in the room, the join failed, its own
// sim was playing), when waiting for one runs out: there is nothing here to
// watch it on.
//
// Kept per game, not as one switch: a result arriving while another game is
// being watched must not be released by THAT game's final buzzer.
//
// And only while there is a broadcast to wait for (see worthWaiting): a result
// that arrives with its broadcast already gone - a catch-up after time away,
// every game the room live-simmed meanwhile coming in at once - has nothing
// to hold for. Held anyway, each re-armed the wait, and the score bar, the
// ticker and the header sat frozen on "Live game in progress" long after the
// data had landed.
export const LIVE_RESULT_WAIT_MS = 90_000;
const MAX_REMEMBERED_LIVE_RESULTS = 16;

export const createLiveResultWait = ({
	take,
	release,
	isFollowing,
	isWatching,
	worthWaiting = () => true,
	log,
	waitMs = LIVE_RESULT_WAIT_MS,
	schedule = (fn: () => void, ms: number) => setTimeout(fn, ms),
	cancel = (timer: unknown) => {
		clearTimeout(timer as ReturnType<typeof setTimeout>);
	},
}: {
	// Hold / let go the paint (the follower hold).
	take: () => void;
	release: () => void;
	// This device is already inside (or has walked out of) the broadcast of
	// this game.
	isFollowing: (gid: number) => boolean;
	// This device is watching some broadcast right now.
	isWatching: () => boolean;
	// Whether a broadcast of this game may yet be watched here: live in the
	// room now, or the result so new its broadcast may still be on the way.
	worthWaiting?: (gid: number, liveAt: number) => boolean;
	log?: (event: string, data: Record<string, unknown>) => void;
	waitMs?: number;
	schedule?: (fn: () => void, ms: number) => unknown;
	cancel?: (timer: unknown) => void;
}) => {
	const awaited = new Map<number, { timer: unknown; liveAt: number }>();
	// The ones already held for (gid -> liveAt), so the same result arriving
	// twice - a re-delivered changeset - never holds it a second time.
	const seen = new Map<number, number>();
	const settle = (gid: number) => {
		const w = awaited.get(gid);
		if (w) {
			cancel(w.timer);
			awaited.delete(gid);
		}
	};
	return {
		// Holding a result for a game not yet watched here.
		pending: () => awaited.size > 0,
		// Which games those are.
		awaitedGids: () => [...awaited.keys()],
		// The room's broadcast changed (ended, or moved on to another game):
		// stop waiting on any result no broadcast can show here any more - and
		// with none left waiting, let the paint go now rather than when the
		// wait would have run out.
		dropUnwatchable: () => {
			let dropped = false;
			for (const [gid, w] of awaited) {
				if (!worthWaiting(gid, w.liveAt)) {
					cancel(w.timer);
					awaited.delete(gid);
					dropped = true;
					log?.("live:result-hold-dropped", { gid });
				}
			}
			if (dropped && awaited.size === 0 && !isWatching()) {
				release();
			}
		},
		notice: (gid: number, liveAt: number) => {
			if (seen.get(gid) === liveAt) {
				return;
			}
			seen.delete(gid);
			seen.set(gid, liveAt);
			while (seen.size > MAX_REMEMBERED_LIVE_RESULTS) {
				const oldest = seen.keys().next().value;
				if (oldest === undefined) {
					break;
				}
				seen.delete(oldest);
			}
			if (isFollowing(gid)) {
				return;
			}
			if (!worthWaiting(gid, liveAt)) {
				log?.("live:result-not-held", { gid });
				return;
			}
			log?.("live:result-held", { gid });
			take();
			settle(gid);
			awaited.set(gid, {
				liveAt,
				timer: schedule(() => {
					awaited.delete(gid);
					if (awaited.size > 0 || isWatching()) {
						return;
					}
					log?.("live:result-hold-expired", { gid });
					release();
				}, waitMs),
			});
		},
		// This game has been watched to the end here, or walked out of: its
		// result may show.
		settle,
	};
};

// SEVERAL LIVE GAMES AT ONCE, ONE FOLLOW.
//
// Anybody can play their own game live while somebody else's is on, so the
// room can have several. This device still follows one at a time - the game
// it is in, or the one it is being asked about - and everything above decides
// against that one, its focus. The rest are on the header's live scores.
//
// The focus: the game this device is in or being asked about, for as long as
// that lasts (gone from the room means its broadcast ended, which the follow
// handles); otherwise the newest game it has not been asked about yet, so it
// gets asked; otherwise the one it last answered or walked out of, while that
// is still on (its record is what keeps it from being asked again).
export type FocusRecord =
	| {
			startedAt: number;
			offered?: boolean;
			over?: boolean;
			left?: boolean;
	  }
	| undefined;

export const chooseFocusGame = <
	M extends { startedAt: number; gameOver: boolean; expiresAt: number },
>(
	games: readonly M[],
	followed: FocusRecord,
	// Games already offered, joined or walked out of here (by startedAt).
	answered: ReadonlySet<number>,
	now: number,
): M | undefined => {
	if (followed && (followed.offered || (!followed.over && !followed.left))) {
		return games.find((game) => game.startedAt === followed.startedAt);
	}
	let fresh: M | undefined;
	for (const game of games) {
		if (
			answered.has(game.startedAt) ||
			game.gameOver ||
			game.expiresAt <= now
		) {
			continue;
		}
		if (!fresh || game.startedAt > fresh.startedAt) {
			fresh = game;
		}
	}
	if (fresh) {
		return fresh;
	}
	return followed
		? games.find((game) => game.startedAt === followed.startedAt)
		: undefined;
};

// Is this live game still going on, for everyone not watching it? Over once
// the screen playing it reaches the final; and a game whose broadcast has
// gone quiet longer than a watch waits (QUIET_WATCH_MS) is given up on.
export const roomGameStillLive = (
	game: { gameOver: boolean; expiresAt: number },
	now: number,
	// When this device last came back from being parked (see
	// createWakeClock): its own sleep is not the broadcast's silence.
	wokeAt = 0,
): boolean =>
	!game.gameOver && now - Math.max(game.expiresAt, wokeAt) < QUIET_WATCH_MS;

// A room game given up on stays over: once it has been quiet past
// QUIET_WATCH_MS - its final, for everyone not watching - a screen that comes
// back and heartbeats again does not put it back in progress (its final, and
// in the playoffs whatever was simmed meanwhile, would go back into hiding).
// Only while this device is hearing the room itself: its own dropped
// connection is not the game's silence. Remembered by startedAt, bounded.
export const createRoomLiveCheck = (
	hearingRoom: (now: number) => boolean,
	onGiveUp?: (game: { gid?: number; startedAt: number }) => void,
) => {
	const givenUp = new Set<number>();
	return {
		isLive: (
			game: {
				gid?: number;
				startedAt: number;
				gameOver: boolean;
				expiresAt: number;
			},
			now: number,
			wokeAt = 0,
		): boolean => {
			if (givenUp.has(game.startedAt)) {
				return false;
			}
			if (roomGameStillLive(game, now, wokeAt)) {
				return true;
			}
			if (!game.gameOver && hearingRoom(now)) {
				givenUp.add(game.startedAt);
				onGiveUp?.(game);
				if (givenUp.size > 32) {
					const oldest = givenUp.values().next().value;
					if (oldest !== undefined) {
						givenUp.delete(oldest);
					}
				}
			}
			return false;
		},
		givenUp: (startedAt: number) => givenUp.has(startedAt),
	};
};

// What holds the league's next step (a playoff day, a phase change) while a
// game is being played live: undefined for nothing. A playoff game still on
// in the room, even once its tip-off moved the phase on (the title won at the
// tip-off of the Finals' last game); and this device's own playoff game while
// its page is still playing it - a flag left up by a page that never reported
// back holds nothing up.
export const liveGameHold = <M extends { label?: string; playoffs?: boolean }>({
	inPlayoffs,
	own,
	liveSimGid,
	now,
	games,
	isLive,
}: {
	inPlayoffs: boolean;
	own: { gid: number; playoffs: boolean; beatAt: number } | undefined;
	liveSimGid: number | undefined;
	now: number;
	games: Iterable<M>;
	isLive: (game: M) => boolean;
}): string | undefined => {
	if (
		own &&
		liveSimGid === own.gid &&
		(inPlayoffs || own.playoffs) &&
		now - own.beatAt < QUIET_WATCH_MS
	) {
		return "Your live game";
	}
	for (const game of games) {
		if ((inPlayoffs || game.playoffs === true) && isLive(game)) {
			return game.label ? `${game.label} (live)` : "A live game";
		}
	}
	return undefined;
};
