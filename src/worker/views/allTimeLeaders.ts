import type { RouteParams } from "../../ui/router/types.ts";
import { defineView } from "../util/defineView.ts";
import {
	WATCH_STATS,
	getAllTimeLeaders,
	type WatchStat,
} from "../util/milestoneWatch.ts";

// ONE ALL-TIME LIST (#/all_time_leaders/<stat>/<pid>): every player in the
// league's history by his career regular season total, from a player's
// all-time ranks on his page.

const processInputs = (params: RouteParams<"allTimeLeaders">) => {
	const stat = (WATCH_STATS as readonly string[]).includes(params.stat ?? "")
		? (params.stat as WatchStat)
		: "pts";
	const pid =
		params.pid === undefined ? undefined : Number.parseInt(params.pid);
	return {
		stat,
		pid: pid === undefined || Number.isNaN(pid) ? undefined : pid,
	};
};

export default defineView({
	id: "allTimeLeaders",
	processInputs,
	load: async ({ inputs, updateEvents, prevInputs }) => {
		if (
			updateEvents.has("firstRun") ||
			inputs.stat !== prevInputs?.stat ||
			inputs.pid !== prevInputs?.pid ||
			updateEvents.has("gameSim") ||
			updateEvents.has("playerMovement") ||
			updateEvents.has("newPhase") ||
			updateEvents.has("liveGames")
		) {
			return getAllTimeLeaders(inputs.stat, inputs.pid);
		}
	},
});
