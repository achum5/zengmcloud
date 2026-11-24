import { defineView } from "../util/defineView.ts";
import { getMilestoneWatch } from "../util/milestoneWatch.ts";

export default defineView({
	id: "milestoneWatch",
	load: async ({ updateEvents }) => {
		if (
			updateEvents.has("firstRun") ||
			updateEvents.has("gameSim") ||
			updateEvents.has("playerMovement") ||
			updateEvents.has("newPhase") ||
			updateEvents.has("liveGames")
		) {
			return getMilestoneWatch();
		}
	},
});
