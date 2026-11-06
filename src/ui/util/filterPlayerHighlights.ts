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

	// Pass 2: keep housekeeping, every highlight, and the lead-in plays. The
	// first play kept after any that were dropped starts a new clip, and says
	// so (`clipStart`): the 3D court cuts to it rather than playing out how
	// everybody got from the last clip to this one.
	const out: any[] = [];
	let skipped = false;
	const keepPlay = (event: any) => {
		out.push(skipped ? { ...event, clipStart: true } : event);
		skipped = false;
	};
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
			keepPlay(event);
			continue;
		}
		// A lead-in: a play (not a sub/injury/stoppage) that comes before a
		// highlight within the same possession.
		if (typeof type === "string" && !NON_PLAY_DESCRIPTIVE.has(type)) {
			const lastHi = lastHighlightIndex.get(possessionOf[i]!);
			if (lastHi !== undefined && lastHi > i) {
				keepPlay(event);
				continue;
			}
		}
		if (typeof type === "string") {
			skipped = true;
		}
	}
	return out;
};

// Does the live viewer stop on this event - a line of the play-by-play - or
// apply it on its way (see processLiveGameEvents.basketball)?
export const isLine = (event: any): boolean =>
	typeof event?.type === "string" &&
	event.type !== "init" &&
	event.type !== "stat" &&
	event.type !== "timeouts" &&
	!(event.type === "sub" && event.silent);

// The most lines of its own possession a single play is shown after.
const CLIP_LEAD_MOST = 3;
// A play shown with the steal it came off: no more than this long after it
// (game seconds) - a break the other way.
const CLIP_BREAK_MOST = 8;
// A line of the ball in play: a shot, its miss, a block, a board off it.
const inPlay = (type: string) =>
	type === "orb" || (/^(fga|miss|blk)/.test(type) && type !== "missFt");

// ONE PLAY, TO BE SHOWN ON ITS OWN (a highlight opened on its box score):
// the game cut down the way a reel is (housekeeping all kept, the plays
// before it dropped) to the play's own lines - from `first` to `last`,
// events consumed - after a few lines of its own possession that led up to
// it, and the one line after it, for how it ends (never past `upTo`, what
// the page has shown). `start` and `end` are where it begins and ends, in
// events of the clip consumed.
export const clipOfPlay = (
	playByPlay: any[],
	first: number,
	last: number,
	upTo = playByPlay.length,
): { events: any[]; start: number; end: number } => {
	let own = first;
	while (own < last && !isLine(playByPlay[own])) {
		own += 1;
	}
	// The live play of its possession before it - the misses and the boards
	// it came off - back to a dead ball (a whistle, a free throw) or what
	// ended the last possession; or the steal it came straight off.
	const lead = new Set<number>();
	const clock = playByPlay[own]?.clock;
	for (let i = own - 1; i >= 0 && lead.size < CLIP_LEAD_MOST; i--) {
		const event = playByPlay[i];
		if (!isLine(event) || event.type === "sub") {
			continue;
		}
		if (
			event.type === "stl" &&
			typeof clock === "number" &&
			typeof event.clock === "number" &&
			event.clock - clock <= CLIP_BREAK_MOST
		) {
			lead.add(i);
			break;
		}
		if (POSSESSION_ENDERS.has(event.type) || !inPlay(event.type)) {
			break;
		}
		lead.add(i);
	}
	const from = lead.size > 0 ? Math.min(...lead) : own;
	const events: any[] = [];
	// (Where it begins: past the last line kept before it.)
	let start = 0;
	let end = -1;
	let skipped = false;
	let cut = false;
	for (let i = 0; i < Math.min(playByPlay.length, upTo); i++) {
		const event = playByPlay[i];
		const type = event?.type;
		const line = isLine(event);
		if (i === last) {
			end = events.length;
		}
		if (i >= last) {
			// On to the next line, and it.
			events.push(event);
			if (line) {
				break;
			}
		} else if (i >= from) {
			if (!line || i >= own || lead.has(i)) {
				// (Cut to: the game before it is not there.)
				if (line && skipped && !cut) {
					cut = true;
					events.push({ ...event, clipStart: true });
				} else {
					events.push(event);
				}
			} else if (type === "sub") {
				events.push({ ...event, silent: true });
			}
		} else if (ALWAYS_KEEP_TYPES.has(type)) {
			events.push(event);
			if (line) {
				start = events.length;
			}
		} else if (type === "sub") {
			events.push({ ...event, silent: true });
		} else if (typeof type === "string") {
			skipped = true;
		}
	}
	if (end < 0) {
		end = events.length;
	}
	return { events, start, end };
};
