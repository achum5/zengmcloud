// Reduce a game's stored live play-by-play to a single player's POSITIVE plays,
// for a "player highlights" reel driven by the normal live-game viewer.
//
// The viewer replays an array of events, silently applying the housekeeping ones
// (init, per-stat box updates) and pausing only on a descriptive play event (see
// processLiveGameEvents.basketball). So to show just one player's highlights we
// keep every housekeeping event - the running box score stays correct and fast-
// forwards between highlights - plus the quarter markers and the final, and drop
// every descriptive play except that player's positive ones. The viewer then
// rolls silently through the gaps and pauses on each highlight, untouched.
//
// Basketball only (the event types below are basketball's). Callers gate on sport.

// Made shots - `pid` is the scorer (and `pidAst`, if present, the assister).
const MADE_SHOT_TYPES = new Set([
	"fgAtRim",
	"fgAtRimAndOne",
	"fgLowPost",
	"fgLowPostAndOne",
	"fgMidRange",
	"fgMidRangeAndOne",
	"tp",
	"tpAndOne",
	"fgTipIn",
	"fgTipInAndOne",
	"fgPutBack",
	"fgPutBackAndOne",
	"ft",
]);

// Blocks - `pid` is the blocker.
const BLOCK_TYPES = new Set([
	"blkAtRim",
	"blkLowPost",
	"blkMidRange",
	"blkTp",
	"blkTipIn",
	"blkPutBack",
]);

// Steals and rebounds - `pid` is the stealer / rebounder.
const STEAL_REBOUND_TYPES = new Set(["stl", "orb", "drb"]);

// Housekeeping + context the viewer needs (it applies these silently to rebuild
// the box score / clock), plus quarter markers and the final - always kept so the
// reel stays accurate and reads with a bit of temporal context.
const ALWAYS_KEEP_TYPES = new Set([
	"init",
	"stat",
	"timeouts",
	"period",
	"overtime",
	"gameOver",
]);

// Is this descriptive event one of `pid`'s positive plays (a make, an assist he
// set up, a block, a steal, or a rebound)?
export const isPositivePlayForPid = (event: any, pid: number): boolean => {
	const type = event?.type;
	if (MADE_SHOT_TYPES.has(type)) {
		return event.pid === pid || event.pidAst === pid;
	}
	if (BLOCK_TYPES.has(type) || STEAL_REBOUND_TYPES.has(type)) {
		return event.pid === pid;
	}
	return false;
};

// How many actual highlight plays (not housekeeping) the reel would contain, so a
// caller can avoid launching an empty reel.
export const countPlayerHighlights = (playByPlay: any[], pid: number): number =>
	playByPlay.filter((event) => isPositivePlayForPid(event, pid)).length;

// The filtered event array to hand to the live viewer. Order is preserved.
export const filterPlayerHighlights = (playByPlay: any[], pid: number): any[] =>
	playByPlay.filter((event) => {
		if (ALWAYS_KEEP_TYPES.has(event?.type)) {
			return true;
		}
		return isPositivePlayForPid(event, pid);
	});
