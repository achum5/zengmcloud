import { startAutoPlay } from "./autoPlay.ts";
import { g, logEvent } from "../../util/index.ts";
import type { Conditions, Phase } from "../../../common/types.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { runAfterActionHook } from "../sync/afterActionHook.ts";
import { beginRoomAdvance, endRoomAdvance } from "../sync/advanceLease.ts";

// If auto play can't start, tells the user why and returns false
const checkCanAutoPlay = (conditions: Conditions) => {
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

	return true;
};

// What the UI's auto play dialog needs, or undefined if auto play can't start
export const getAutoPlayDialogInfo = (conditions: Conditions) => {
	if (!checkCanAutoPlay(conditions)) {
		return;
	}

	return {
		season: g.get("season"),
		forceHistoricalRosters: g.get("forceHistoricalRosters"),
		repeatSeason: g.get("repeatSeason")?.type,
	};
};

// Starts auto play until the season and phase picked in the UI's auto play dialog. Checks again whether auto play can start, since the league might have changed while the dialog was open.
export const initAutoPlay = (
	choice: { season: string; phase: string },
	conditions: Conditions,
) => {
	if (!checkCanAutoPlay(conditions)) {
		return false;
	}

	const season = Number.parseInt(choice.season);
	const phase = Number.parseInt(choice.phase);

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
		changeTracker.beginSim("playMenu.day");
		// The room held busy for the whole run, its phase changes, draft and
		// free agency included, until it is published (sync/advanceLease.ts).
		beginRoomAdvance();
		void (async () => {
			try {
				try {
					await startAutoPlay(season, phase as Phase, conditions);
				} catch (error) {
					console.error("autoPlay failed", error);
				} finally {
					changeTracker.endSim("playMenu.day");
				}
				await runAfterActionHook("playMenu", "day").catch(() => {});
			} finally {
				endRoomAdvance();
			}
		})();
	} else {
		return false;
	}
};
