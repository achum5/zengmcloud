// The league ticker: the ESPN bar pinned to the bottom of the screen.
//
// EVERYTHING IN IT IS LEAGUE-WIDE. The first version reused `games` out of UI
// state because it was already there and already fresh - but that array exists
// to feed the TOP score bar, which is a your-team widget, so every score in it
// had the user's own abbrev in it and the ticker was just their game log
// crawling past. The feed is built in the worker now (see
// worker/util/updateTickerItems.ts) and covers the whole league: every score
// from the day, the rest of today's slate with its point spread, the day's best
// individual performances, where the award races stand, and the news.
//
// The worker assembles each item's display text, so this side stays a renderer.

import { categories } from "./transactionInfo.ts";

export type TickerTeam = {
	tid: number;
	abbrev: string;
	pts?: number;
};

export type TickerRaceEntry = {
	pid: number;
	name: string;
	odds?: string;
};

export type TickerItem =
	// A final score from the most recent day of games.
	| {
			type: "score";
			key: string;
			gid: number;
			season: number;
			// Which team's game log the box score lives under, or "special" for the
			// All-Star game. The box score URL needs abbrev_tid/season/gid - a bare
			// gid lands on "No games found for this season".
			boxScoreTeam: string;
			away: TickerTeam;
			home: TickerTeam;
			overtimes?: number;
	  }
	// A game still to be played, with the same point spread every other page
	// shows for it.
	| {
			type: "upcoming";
			key: string;
			away: TickerTeam;
			home: TickerTeam;
			line?: string;
	  }
	// A standout stat line from the day. Split rather than pre-joined, because
	// the three parts are typeset differently: the name carries the item, the
	// stat line supports it, the score is a footnote.
	| {
			type: "performance";
			key: string;
			gid: number;
			season: number;
			boxScoreTeam: string;
			pid: number;
			name: string;
			stat: string;
			game: string;
	  }
	// Where an award race stands, quoting the same model the Award Races page
	// and the sportsbook use.
	| { type: "race"; key: string; label: string; entries: TickerRaceEntry[] }
	// Transactions, injuries, milestones - the events log.
	| { type: "news"; key: string; eid: number; text: string; category?: string };

// How much of each kind gets in. Caps are per section so one busy category
// cannot crowd the others out: an offseason day logs hundreds of events, and a
// ticker made entirely of minimum-contract signings is not a ticker.
export const TICKER_LIMITS = {
	score: 16,
	upcoming: 8,
	// Three lines each from the six best games - these are grouped into per-game
	// segments, so the budget is games x lines rather than a flat top six.
	performance: 18,
	race: 4,
	news: 18,
} as const;

// Scores first, then what is still to come, then the day's best games, then the
// races, then the news. That is roughly the order a sports bar cycles in, and
// keeping it fixed means a glance at the same spot always finds the same kind
// of thing.
const ORDER = ["score", "upcoming", "performance", "race", "news"] as const;

export const buildTickerStream = (items: TickerItem[]): TickerItem[] => {
	const out: TickerItem[] = [];
	const seen = new Set<string>();

	for (const type of ORDER) {
		let taken = 0;
		for (const item of items) {
			if (item.type !== type || taken >= TICKER_LIMITS[type]) {
				continue;
			}
			if (seen.has(item.key)) {
				continue;
			}
			seen.add(item.key);
			out.push(item);
			taken += 1;
		}
	}

	return out;
};

// WHEN THE TICKER MAY CHANGE.
//
// This is the whole spoiler question in one function. While a live game is
// playing out on screen - your own sim, or a league-mate's broadcast you are
// following - the ticker must FREEZE on whatever it was showing. A bar that
// scrolls "Final: 112-108" past the bottom of the screen while you are watching
// the first quarter of that game ruins it completely, and it would do it for
// every follower in the room at once.
//
// The top score bar solves the same problem the same way (LeagueTopBar holds
// prevGames while liveGameInProgress), and on the receiving side the sync layer
// already banks UI refreshes for the duration - so nothing even arrives until
// the playback ends. This is the last of the three doors.
export const tickerMayUpdate = (state: {
	liveGameInProgress: boolean;
	watchingBroadcast: boolean;
}): boolean => !state.liveGameInProgress && !state.watchingBroadcast;

// ---------------------------------------------------------------- SEGMENTS
//
// WHAT THE LEFT PANE IS FOR.
//
// A broadcast ticker is not one undifferentiated crawl. It runs in blocks: the
// pane names a block - SCORES, TRANSACTIONS, MVP - and that block's contents go
// past beside it, and when the block is done the pane changes to the next one.
// The best of them go further: for a single game the pane holds the actual
// score while that game's stat lines scroll past it. That is what this builds.
//
// An earlier version tried to derive the pane from wherever the marquee happened
// to be, by measuring it every frame. That was wrong twice over. It made the
// pane a guess about a continuous scroll rather than a structure, and the
// measurement itself is unreliable on iOS, where the marquee is animated on the
// compositor and the main thread's rect reads do not keep up - the same lag
// documented in stickyHeaderWatchdog.ts. The pane simply sat there.
//
// So the segments are the model, not an observation. The player advances
// through them and always knows which one it is showing.

export type TickerHeader =
	// A named block: the section, the award, the news category.
	| { kind: "label"; text: string }
	// One game's final score, held in the pane while its lines scroll.
	| { kind: "final"; away: TickerTeam; home: TickerTeam };

export type TickerSegment = {
	key: string;
	header: TickerHeader;
	items: TickerItem[];
};

// Every distinct thing gets its own block; nothing is mixed. Order is the same
// running order the flat stream used, so a glance at the pane finds the same
// kinds of thing in the same sequence each time round.
export const buildTickerSegments = (items: TickerItem[]): TickerSegment[] => {
	const segments: TickerSegment[] = [];

	const scores = items.filter((item) => item.type === "score");
	if (scores.length > 0) {
		segments.push({
			key: "scores",
			header: { kind: "label", text: "Scores" },
			items: scores,
		});
	}

	// THE GAME BLOCKS. The pane holds the score, the lines from that game go past
	// beside it. Grouped by game rather than shown as a flat "top performers"
	// list, because a stat line without its game is half a fact.
	const scoreByGid = new Map(
		scores.map((score) => [score.gid, score] as const),
	);
	const byGame = new Map<number, TickerItem[]>();
	for (const item of items) {
		if (item.type === "performance") {
			const lines = byGame.get(item.gid);
			if (lines) {
				lines.push(item);
			} else {
				byGame.set(item.gid, [item]);
			}
		}
	}
	for (const [gid, lines] of byGame) {
		const score = scoreByGid.get(gid);
		segments.push({
			key: `game-${gid}`,
			header: score
				? { kind: "final", away: score.away, home: score.home }
				: { kind: "label", text: "Top Performers" },
			items: lines,
		});
	}

	const upcoming = items.filter((item) => item.type === "upcoming");
	if (upcoming.length > 0) {
		segments.push({
			key: "odds",
			header: { kind: "label", text: "Odds" },
			items: upcoming,
		});
	}

	// One block per award, so the pane says MVP while the MVP field goes past.
	for (const item of items) {
		if (item.type === "race") {
			segments.push({
				key: item.key,
				header: { kind: "label", text: item.label },
				items: [item],
			});
		}
	}

	// News in blocks by category - TRANSACTIONS, then INJURIES - in the order the
	// categories first appear, so the busiest kind leads.
	const byCategory = new Map<string, TickerItem[]>();
	for (const item of items) {
		if (item.type === "news") {
			const key = item.category ?? "other";
			const group = byCategory.get(key);
			if (group) {
				group.push(item);
			} else {
				byCategory.set(key, [item]);
			}
		}
	}
	for (const [category, group] of byCategory) {
		segments.push({
			key: `news-${category}`,
			header: { kind: "label", text: newsHeading(category) },
			items: group,
		});
	}

	return segments;
};

// The category's display name, or a sane fallback for an event type that has
// not been given one.
const newsHeading = (category: string): string =>
	category in categories
		? categories[category as keyof typeof categories].text
		: "News";

// HOW FAR A BLOCK HAS TO TRAVEL: exactly enough to bring its far end to the
// left edge, and no further.
//
// The obvious thing - run the block in from off-screen right and out past the
// left - leaves the bar empty for seconds at both ends of every block, which on
// a wide screen was most of the time. Travelling only the overflow keeps the bar
// full from the first frame of a block to the last. A block that already fits
// travels nothing and simply sits there for the minimum dwell.
export const segmentTravelPx = (
	contentWidth: number,
	viewportWidth: number,
): number => {
	if (!Number.isFinite(contentWidth) || !Number.isFinite(viewportWidth)) {
		return 0;
	}
	return Math.max(0, contentWidth - viewportWidth);
};

// How long that takes. Constant speed, so a block of two scores does not sit on
// screen as long as a block of sixteen, with a floor and a ceiling so neither
// extreme becomes unreadable or interminable. The floor is also what gives a
// block that fits its dwell time before the next one.
export const SEGMENT_PIXELS_PER_SECOND = 150;
export const SEGMENT_MIN_SECONDS = 7;
export const SEGMENT_MAX_SECONDS = 45;

export const segmentDurationSeconds = (
	travelPx: number,
	pixelsPerSecond = SEGMENT_PIXELS_PER_SECOND,
): number => {
	if (!Number.isFinite(travelPx) || travelPx <= 0) {
		return SEGMENT_MIN_SECONDS;
	}
	return Math.min(
		SEGMENT_MAX_SECONDS,
		Math.max(SEGMENT_MIN_SECONDS, travelPx / pixelsPerSecond),
	);
};
