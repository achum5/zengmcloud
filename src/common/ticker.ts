// The league ticker: the ESPN bar pinned to the bottom of the screen, showing
// what is happening around the league as one continuous scroll.
//
// Two feeds go into it and they arrive by completely different routes. SCORES
// are already in UI state - the top score bar reads the same `games` array, and
// the worker keeps it fresh through mergeGames - so the ticker costs nothing
// extra for those. NEWS is pushed separately as a short tail of recent events
// (see worker/util/updateTickerNews.ts), because nothing was pushing events to
// the UI at all before this.
//
// This module is the part with rules in it: what order things run in, how much
// is kept, and - the one that matters - when the ticker is allowed to change at
// all.

export type TickerGame = {
	gid: number;
	// Set once the game has been played.
	final: boolean;
};

export type TickerNews = {
	eid: number;
	// Already-formatted HTML from the worker, links and all.
	text: string;
	// Drives the little category chip, from common/transactionInfo.ts.
	category?: string;
};

export type TickerItem =
	| ({ type: "game" } & TickerGame)
	| ({ type: "news" } & TickerNews);

// Today's slate first, then the news, which is the order ESPN's own bar runs
// in: scores are the thing you look at, headlines are the thing you read while
// waiting for the scores to come back around.
export const buildTickerStream = ({
	games,
	news,
	maxNews = 25,
}: {
	games: TickerGame[];
	news: TickerNews[];
	maxNews?: number;
}): TickerItem[] => {
	const items: TickerItem[] = games.map((game) => ({
		type: "game" as const,
		...game,
	}));

	// Newest first, and bounded: an offseason day can log hundreds of events, and
	// a scroll long enough to hold them all would take minutes to come around.
	const seen = new Set<number>();
	for (const item of news) {
		if (seen.has(item.eid)) {
			continue;
		}
		seen.add(item.eid);
		items.push({ type: "news", ...item });
		if (seen.size >= maxNews) {
			break;
		}
	}

	return items;
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
// already banks UI refreshes for the duration - so news does not even arrive
// until the playback ends. This is the last of the three doors.
export const tickerMayUpdate = (state: {
	liveGameInProgress: boolean;
	watchingBroadcast: boolean;
}): boolean => !state.liveGameInProgress && !state.watchingBroadcast;

// How long one full pass takes, so a long slate does not crawl and a short one
// does not blur past. Roughly constant pixels per second.
export const tickerDurationSeconds = (itemCount: number): number =>
	Math.max(20, Math.round(itemCount * 4.5));
