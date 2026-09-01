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
