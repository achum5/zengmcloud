// WHO GETS PULLED INTO A LIVE SIM, AND WHEN.
//
// Every live sim in the room is watched by the whole room. The device in charge
// of simming working through the schedule and a league-mate playing out their
// own game produce the same experience on every other screen, because they are
// the same event: a game happening right now. There used to be two kinds - the
// simmer's, which navigated everyone in, and an opt-in one that only put a pill
// in the header - and the same thing felt like two different features depending
// on which device happened to be in charge that night.
//
// What is left of the difference is that anyone may walk out of any of them.
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
	  }
	| undefined;

export type FollowAction =
	// Fetch the payload, navigate in, start playback. A broadcast this device
	// has not seen before.
	| "join"
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
): boolean => followed !== undefined && !followed.left && !localSimActive;

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

export const decideFollowAction = (
	broadcast: { startedAt: number; gameOver: boolean },
	followed: FollowRecord,
	// A live sim of this device's OWN game is playing right now (two can run at
	// once - see ownGameSimGate). Never navigated out from under: the pill is
	// offered instead, and since nothing is recorded against the broadcast, the
	// first heartbeat after the local sim ends joins it as normal.
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
		return localSimActive ? "pill" : "join";
	}
	if (followed.left) {
		return broadcast.gameOver ? "ignore" : "pill";
	}
	return "cursor";
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
