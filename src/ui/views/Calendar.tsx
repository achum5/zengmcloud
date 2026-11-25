import { useEffect } from "react";
import type { View } from "../../common/types.ts";
import { helpers } from "../util/helpers.ts";
import { useLocal } from "../util/local.ts";
import { realtimeUpdate } from "../util/realtimeUpdate.ts";

// NO CALENDAR PAGE ANY MORE: a team's Schedule has the same month calendar.
// The address is kept so an old link or bookmark lands on that team's
// Schedule (the whole league's, on the user's). The worker's calendar view
// still serves the team page's Schedule section.
const Calendar = ({ tid }: View<"calendar">) => {
	const { teamInfoCache } = useLocal(["teamInfoCache"]);
	const t = typeof tid === "number" ? teamInfoCache[tid] : undefined;
	const url = helpers.leagueUrl(
		t ? ["schedule", `${t.abbrev}_${tid}`] : ["schedule"],
	);

	useEffect(() => {
		realtimeUpdate([], url, undefined, true);
	}, [url]);

	return null;
};

export default Calendar;
