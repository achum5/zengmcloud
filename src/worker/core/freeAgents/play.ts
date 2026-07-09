import { PHASE } from "../../../common/constants.ts";
import { league, phase, trade } from "../index.ts";
import autoSign from "./autoSign.ts";
import decreaseDemands from "./decreaseDemands.ts";
import {
	g,
	lock,
	updatePlayMenu,
	updateStatus,
	toUI,
	helpers,
} from "../../util/index.ts";
import type { Conditions } from "../../../common/types.ts";
import { recomputeLocalUITeamOvrs } from "../../util/recomputeLocalUITeamOvrs.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { runAfterActionHook } from "../sync/afterActionHook.ts";

/**
 * Simulates one or more days of free agency.
 *
 * @memberOf core.freeAgents
 * @param {number} numDays An integer representing the number of days to be simulated. If numDays is larger than the number of days remaining, then all of free agency will be simulated up until the preseason starts.
 * @param {boolean} start Is this a new request from the user to simulate days (true) or a recursive callback to simulate another day (false)? If true, then there is a check to make sure simulating games is allowed. Default true.
 */
async function play(
	numDays: number,
	conditions: Conditions,
	start: boolean = true,
) {
	// This is called when there are no more days to play, either due to the user's request (e.g. 1 week) elapsing or at the end of free agency.
	const cbNoDays = async () => {
		await lock.set("gameSim", false);
		await updatePlayMenu(); // Check to see if free agency is over

		if (g.get("daysLeft") <= 0) {
			await updateStatus("Idle");
			await phase.newPhase(PHASE.PRESEASON, conditions);
		}
	};

	// This simulates a day, including game simulation and any other bookkeeping that needs to be done
	const cbRunDay = async () => {
		// This is called if there are remaining days to simulate
		const cbYetAnother = async () => {
			await decreaseDemands();
			await autoSign();
			await league.setGameAttributes({
				daysLeft: g.get("daysLeft") - 1,
			});

			if (g.get("daysLeft") > 0 && numDays > 0) {
				await toUI("realtimeUpdate", [["playerMovement"]]);
				await recomputeLocalUITeamOvrs();
				await updateStatus(helpers.daysLeft(true));
				await trade.betweenAiTeams();
				await play(numDays - 1, conditions, false);
			} else {
				await cbNoDays();
			}
		};

		// If we didn't just stop games, let's play
		// Or, if we are starting games (and already passed the lock), continue even if stopGameSim was just seen
		const stopGameSim = lock.get("stopGameSim");

		if (numDays > 0 && (start || !stopGameSim)) {
			if (stopGameSim) {
				await lock.set("stopGameSim", false);
			}

			await cbYetAnother();
		} else {
			// If this is the last day, update play menu
			await cbNoDays();
		}
	};

	// If this is a request to start a new simulation... are we allowed to do
	// that? If so, set the lock and update the play menu
	if (start) {
		const canStartGames = lock.canStartGames();

		if (canStartGames) {
			await updatePlayMenu();
			// Bracket the whole run (all days + any phase change it chains into) as
			// a capture window, and publish at the end. This matters most for
			// autoplay, which dispatches freeAgents.play WITHOUT awaiting it (to
			// break the promise chain): the originating action has long since
			// resolved, so without this the free-agency days and the season
			// rollover they end in - the biggest changeset of the year - would sit
			// unpublished until some later unrelated action.
			changeTracker.beginSim();
			try {
				await cbRunDay();
			} finally {
				changeTracker.endSim();
			}
			await runAfterActionHook("playMenu", "day");
		}
	} else {
		await cbRunDay();
	}
}

export default play;
