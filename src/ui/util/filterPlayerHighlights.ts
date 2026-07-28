// Reduce a game's stored live play-by-play to a single player's POSITIVE plays,
// for a "player highlights" reel driven by the normal live-game viewer.
//
// The viewer replays an array of events, silently applying the housekeeping ones
// (init, per-stat box updates) and pausing only on a descriptive play event (see
// processLiveGameEvents.basketball). So to show just one player's highlights we
// keep every housekeeping event - the running box score stays correct and fast-
// forwards between highlights - plus the quarter markers and the final, plus each
// of the player's positive plays AND the plays that lead up to them within the
// same possession (the drive before a make, the miss before a rebound).
// Substitutions are kept too, but marked `silent` so they're applied without
// pausing (see below). Every other descriptive play is dropped. The viewer then rolls silently through the
// gaps and plays each highlight through its build-up, untouched.
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

// Event types that END a possession, so the next play belongs to a new one. Used
// to group plays into possessions - a highlight's lead-in is kept only from its
// OWN possession, so build-up never bleeds in from an unrelated sequence. A made
// shot ends a possession (an and-one's bonus free throw is then its own short
// possession, which is fine); a defensive rebound / steal / turnover flips it;
// and quarter/stoppage markers reset it.
const POSSESSION_ENDERS = new Set([
	...MADE_SHOT_TYPES,
	"drb",
	"stl",
	"tov",
	"jumpBall",
	"endOfPeriod",
	"timeout",
	"gameOver",
	"period",
	"overtime",
]);

// Descriptive events that are NOT plays, so they're never kept as a lead-in
// (an injury notice isn't part of a highlight's build-up). Substitutions are
// handled separately below - they're always kept, but silently.
const NON_PLAY_DESCRIPTIVE = new Set([
	"injury",
	"foulOut",
	"timeout",
	"elamActive",
	"shootoutStart",
	"shootoutTie",
	"shootoutTeam",
]);

// The filtered event array to hand to the live viewer. Keeps housekeeping (so the
// running box score stays correct), every one of `pid`'s positive plays, AND the
// plays that lead up to each of those within the same possession (the shot
// attempt before a make, the miss before a rebound, the drive before a block) -
// so each highlight plays through its build-up instead of just the literal line.
// Order is preserved.
export const filterPlayerHighlights = (
	playByPlay: any[],
	pid: number,
): any[] => {
	const n = playByPlay.length;

	// Pass 1: assign each event a possession index and record, per possession, the
	// index of the LAST highlight in it. Lead-in events (before that highlight) are
	// kept; trailing events after the last highlight in a possession are dropped.
	const possessionOf = new Array<number>(n);
	const lastHighlightIndex = new Map<number, number>();
	let possession = 0;
	for (let i = 0; i < n; i++) {
		const event = playByPlay[i];
		possessionOf[i] = possession;
		if (isPositivePlayForPid(event, pid)) {
			lastHighlightIndex.set(possession, i);
		}
		if (POSSESSION_ENDERS.has(event?.type)) {
			possession += 1;
		}
	}

	// Pass 2: keep housekeeping, every highlight, and the lead-in plays.
	const out: any[] = [];
	for (let i = 0; i < n; i++) {
		const event = playByPlay[i];
		const type = event?.type;
		if (ALWAYS_KEEP_TYPES.has(type)) {
			out.push(event);
			continue;
		}
		if (type === "sub") {
			// Who is on the floor is housekeeping too: plus/minus is credited to the
			// five in the game, and the live box score only redraws a player's row
			// while he's on the court. Dropping substitutions therefore froze the
			// whole bench at an empty stat line even though the numbers underneath
			// were right. Marked silent so the viewer applies it without stopping to
			// read out a substitution - nobody wants a highlight reel paused on
			// "On: ... Off: ...".
			out.push({ ...event, silent: true });
			continue;
		}
		if (isPositivePlayForPid(event, pid)) {
			out.push(event);
			continue;
		}
		// A lead-in: a play (not a sub/injury/stoppage) that comes before a
		// highlight within the same possession.
		if (typeof type === "string" && !NON_PLAY_DESCRIPTIVE.has(type)) {
			const lastHi = lastHighlightIndex.get(possessionOf[i]!);
			if (lastHi !== undefined && lastHi > i) {
				out.push(event);
			}
		}
	}
	return out;
};
