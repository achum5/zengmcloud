import { startAutoPlay } from "./autoPlay.ts";
import { toUI, g, logEvent } from "../../util/index.ts";
import type { Conditions, Phase } from "../../../common/types.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { runAfterActionHook } from "../sync/afterActionHook.ts";

const initAutoPlay = async (conditions: Conditions) => {
	if (g.get("gameOver")) {
		logEvent(
			{
				type: "error",
				text: "You can't auto play while you're fired!",
				showNotification: true,
				persistent: true,
				saveToDb: false,
			},
			conditions,
		);
		return false;
	}

	const result = await toUI(
		"autoPlayDialog",
		[
			g.get("season"),
			g.get("forceHistoricalRosters"),
			g.get("repeatSeason")?.type,
		],
		conditions,
	);

	if (!result) {
		return false;
	}

	const season = Number.parseInt(result.season);
	const phase = Number.parseInt(result.phase);

	if (
		season > g.get("season") ||
		(season === g.get("season") && phase > g.get("phase"))
	) {
		// Deliberately not awaited (the run can span many seasons), so the
		// dispatching action resolves immediately and no capture window is open
		// while the run executes. Bracket the whole detached chain as a capture
		// window and publish whatever remains when it settles - without this, a
		// run that starts AND ends in the offseason (no game.play leg, whose own
		// hook would otherwise publish) never reached the cloud. startAutoPlay's
		// promise settles when cleanupAutoPlay runs - the target phase reached,
		// or the run cancelled - which is exactly when the window should close.
		changeTracker.beginSim();
		void (async () => {
			try {
				await startAutoPlay(season, phase as Phase, conditions);
			} catch (error) {
				console.error("autoPlay failed", error);
			} finally {
				changeTracker.endSim();
			}
			await runAfterActionHook("playMenu", "day").catch(() => {});
		})();
	} else {
		return false;
	}
};

export default initAutoPlay;
