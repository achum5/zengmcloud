import type { MpLiveBroadcast } from "../../../common/types.ts";

// WHAT PART THIS PAGE PLAYS IN A LIVE BROADCAST: following a league-mate's
// live sim, broadcasting this device's own, or neither.
//
// A saved replay is this device's own to watch, whatever is being broadcast
// in the league at the time. Taken for a follower, it never started - a
// follower waits on the simmer for every line, and the simmer is playing a
// different game - while the page reloaded itself every second and a half
// trying to bring the broadcast game in, which bogged everything else down.
// Nor is a replay ever reported to the room as the game being broadcast.
export const broadcastRole = (
	mp: MpLiveBroadcast | undefined,
	replay: boolean,
): { isFollower: boolean; isBroadcaster: boolean } => ({
	isFollower: !replay && !!mp?.active && !mp.isBroadcaster,
	isBroadcaster: !replay && !!mp?.active && mp.isBroadcaster,
});
