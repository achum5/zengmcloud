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

export type TickerTeam = {
	tid: number;
	abbrev: string;
	pts?: number;
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
	// A standout stat line from the day.
	| {
			type: "performance";
			key: string;
			gid: number;
			season: number;
			boxScoreTeam: string;
			text: string;
	  }
	// Where an award race stands, quoting the same model the Award Races page
	// and the sportsbook use.
	| { type: "race"; key: string; label: string; text: string }
	// Transactions, injuries, milestones - the events log.
	| { type: "news"; key: string; eid: number; text: string; category?: string };

// How much of each kind gets in. Caps are per section so one busy category
// cannot crowd the others out: an offseason day logs hundreds of events, and a
// ticker made entirely of minimum-contract signings is not a ticker.
export const TICKER_LIMITS = {
	score: 16,
	upcoming: 8,
	performance: 6,
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

// How long one full pass takes, so a long slate does not crawl and a short one
// does not blur past. Roughly constant pixels per second.
export const tickerDurationSeconds = (itemCount: number): number =>
	Math.max(20, Math.round(itemCount * 4.5));
