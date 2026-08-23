import g from "./g.ts";
import local from "./local.ts";

// A team the AI is allowed to act for.
//
// Not simply "not the user's team". Under auto play and in spectator mode the
// user's own team is run by the AI too, and a feature that skipped it there
// would quietly stop working in exactly the modes where every team is supposed
// to be AI run. In multiplayer userTids syncs and holds every friend's team, so
// this excludes all of them on whichever device is in charge of simming.
//
// The trade AI's getAITids is this rule plus a challengeNoTrades clause, which
// belongs to trades and nothing else.
export const isAiControlled = (t: {
	tid: number;
	disabled?: boolean;
}): boolean => {
	if (t.disabled) {
		return false;
	}
	if (local.autoPlayUntil || g.get("spectator")) {
		return true;
	}
	return !g.get("userTids").includes(t.tid);
};
