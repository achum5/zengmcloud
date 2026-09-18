import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";
import { formatEventText } from "../util/formatEventText.ts";
import { feedAboutLeagueEvents } from "../util/socialFeed.ts";
import type { UpdateEvents, ViewInput } from "../../common/types.ts";

const updateEventLog = async (
	inputs: ViewInput<"transactions">,
	updateEvents: UpdateEvents,
	state: any,
) => {
	if (
		updateEvents.length >= 0 ||
		inputs.season !== state.season ||
		inputs.abbrev !== state.abbrev ||
		inputs.eventType !== state.eventType
	) {
		let events;

		if (inputs.season === "all") {
			events = await idb.getCopies.events(undefined, "noCopyCache");
		} else {
			events = await idb.getCopies.events(
				{
					season: inputs.season,
				},
				"noCopyCache",
			);
		}

		events.reverse(); // Newest first

		if (inputs.abbrev !== "all") {
			events = events.filter(
				(event) => event.tids !== undefined && event.tids.includes(inputs.tid),
			);
		}

		if (inputs.eventType === "all") {
			events = events.filter(
				(event) =>
					event.type === "reSigned" ||
					event.type === "release" ||
					event.type === "trade" ||
					event.type === "freeAgent" ||
					event.type === "draft",
			);
		} else {
			events = events.filter((event) => event.type === inputs.eventType);
		}

		const events2 = [];
		for (const event of events) {
			events2.push({
				eid: event.eid,
				type: event.type,
				text: await formatEventText(event),
				pids: event.pids,
				tids: event.tids,
				season: event.season,
				score: event.score,
			});
		}

		// The reaction under each move, when the feed is on and the season is
		// one the feed can rebuild.
		let social;
		if (g.get("socialFeed") && typeof inputs.season === "number") {
			social = await feedAboutLeagueEvents({
				textByEid: new Map(
					events2.map((event) => [
						event.eid,
						event.text.replaceAll(/<[^>]*>/g, ""),
					]),
				),
				season: inputs.season,
				eids: events2.map((event) => event.eid),
			});
		}

		return {
			abbrev: inputs.abbrev,
			events: events2,
			season: inputs.season,
			eventType: inputs.eventType,
			tid: inputs.tid,
			social,
		};
	}
};

export default updateEventLog;
