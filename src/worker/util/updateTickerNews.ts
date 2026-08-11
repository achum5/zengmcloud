import { idb } from "../db/index.ts";
import g from "./g.ts";
import toUI from "./toUI.ts";
import { formatEventText } from "./formatEventText.ts";
import { types } from "../../common/transactionInfo.ts";
import type { EventBBGM, LocalStateUI } from "../../common/types.ts";

// The news half of the bottom ticker.
//
// Scores were already being pushed to UI state for the top score bar, so the
// ticker gets those for free. Events were not being pushed anywhere - the News
// page pulls them itself when you open it - so this is the one new channel.
//
// The threshold is the News page's own: `score` is a 0-20+ importance already
// tuned on every event type, where 10 is "worth showing in the feed by default"
// and 20 is "big". Reusing it means the ticker agrees with the News page about
// what matters instead of inventing a second opinion.
const MIN_SCORE = 10;

// Enough to fill a scroll without making one pass take minutes.
const NUM_ITEMS = 25;

// How far back to look before giving up. An offseason day can log hundreds of
// low-scoring events, and without a stop the cursor would walk the entire
// season on every sim day to find 25 that qualify.
const MAX_SCANNED = 400;

const IGNORE_TYPES = new Set(["retiredList", "newTeam"]);

const qualifies = (event: EventBBGM): boolean =>
	!IGNORE_TYPES.has(event.type) &&
	event.score !== undefined &&
	event.score >= MIN_SCORE;

// Newest first, bounded.
//
// `eid` is an autoincrement, so walking the primary key backwards IS newest
// first and can stop early - no need to read the season and sort it. The
// session's own events live in the cache until it flushes, so they come off the
// top separately and get merged in.
const recentEvents = async (): Promise<EventBBGM[]> => {
	const out: EventBBGM[] = [];
	const seen = new Set<number>();

	const cached = await idb.cache.events.getAll();
	for (const event of [...cached].reverse()) {
		if (qualifies(event) && !seen.has(event.eid)) {
			seen.add(event.eid);
			out.push(event);
		}
		if (out.length >= NUM_ITEMS) {
			return out;
		}
	}

	let scanned = 0;
	let cursor = await idb.league
		.transaction("events")
		.store.openCursor(null, "prev");
	while (cursor && out.length < NUM_ITEMS && scanned < MAX_SCANNED) {
		scanned += 1;
		const event = cursor.value as EventBBGM;
		if (qualifies(event) && !seen.has(event.eid)) {
			seen.add(event.eid);
			out.push(event);
		}
		cursor = await cursor.continue();
	}

	return out;
};

export const updateTickerNews = async () => {
	let news: LocalStateUI["tickerNews"] = [];

	try {
		const events = await recentEvents();
		news = await Promise.all(
			events.map(async (event) => ({
				eid: event.eid,
				text: await formatEventText(event),
				category: types[event.type]?.category,
				season: event.season ?? g.get("season"),
			})),
		);
	} catch (error) {
		// A ticker is decoration. It must never be the reason a sim or a league
		// load fails, so a bad read just leaves it as it was.
		console.error("Failed to build ticker news", error);
		return;
	}

	await toUI("updateLocal", [{ tickerNews: news }]);
};
