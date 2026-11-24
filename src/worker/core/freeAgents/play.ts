import { PHASE } from "../../../common/constants.ts";
import { league, phase, trade } from "../index.ts";
import autoSign from "./autoSign.ts";
import decreaseDemands from "./decreaseDemands.ts";
import {
	g,
	sim,
	updatePlayMenu,
	updateStatus,
	toUI,
	helpers,
} from "../../util/index.ts";
import type { SimRun } from "../../util/sim.ts";
import type { Conditions } from "../../../common/types.ts";
import { collegeRecruitingDay } from "../college/recruiting.ts";
import { recomputeLocalUITeamOvrs } from "../../util/recomputeLocalUITeamOvrs.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { runAfterActionHook } from "../sync/afterActionHook.ts";
import {
	beginRoomAdvance,
	endRoomAdvance,
	holdRoomForAdvance,
} from "../sync/advanceLease.ts";

// Simulates one day, then calls itself for the next day, until run is finished
async function playDays(run: SimRun, numDays: number, conditions: Conditions) {
	// This is called when there are no more days to play, either due to the user's request (e.g. 1 week) elapsing or at the end of free agency.
	const cbNoDays = async () => {
		await run.finish();
		await updatePlayMenu();

		// Check to see if free agency is over
		if (g.get("daysLeft") <= 0) {
			// The room still held busy for the phase change (its front offices
			// may hold the worker past the lease stamped earlier).
			await holdRoomForAdvance();
			await phase.newPhase(PHASE.PRESEASON, conditions);
		}
	};

	// This simulates a day, including game simulation and any other bookkeeping that needs to be done
	const cbRunDay = async () => {
		// This is called if there are remaining days to simulate
		const cbYetAnother = async () => {
			// A day holds the room busy, renewed before each day: the smart CPU
			// may hold the worker for minutes on one (advanceLease.ts).
			await holdRoomForAdvance();
			if (g.get("college")) {
				await collegeRecruitingDay();
			} else {
				await decreaseDemands();
				await autoSign();
			}
			await league.setGameAttributes({
				daysLeft: g.get("daysLeft") - 1,
			});

			if (g.get("daysLeft") > 0 && numDays > 0) {
				await toUI("realtimeUpdate", [["playerMovement"]]);
				await recomputeLocalUITeamOvrs();
				await updateStatus(helpers.daysLeft(true));
				if (!g.get("college")) {
					await trade.betweenAiTeams();
				}
				await playDays(run, numDays - 1, conditions);
			} else {
				await cbNoDays();
			}
		};

		// Keep playing, unless a stop was requested
		if (numDays > 0 && run.shouldContinue()) {
			await cbYetAnother();
		} else {
			// If this is the last day, update play menu
			await cbNoDays();
		}
	};

	await cbRunDay();
}

/**
 * Simulates one or more days of free agency, if no other sim is running.
 *
 * @memberOf core.freeAgents
 * @param {number} numDays An integer representing the number of days to be simulated. If numDays is larger than the number of days remaining, then all of free agency will be simulated up until the preseason starts.
 */
const play = async (numDays: number, conditions: Conditions) => {
	let started = false;
	await sim.run(async (run) => {
		started = true;
		await updatePlayMenu();
		// Bracket the whole run (all days + any phase change it chains into) as
		// a capture window, and publish at the end. This matters most for
		// autoplay, which dispatches freeAgents.play WITHOUT awaiting it (to
		// break the promise chain): the originating action has long since
		// resolved, so without this the free-agency days and the season
		// rollover they end in - the biggest changeset of the year - would sit
		// unpublished until some later unrelated action.
		changeTracker.beginSim("playMenu.day");
		// The room held busy from the first day to the last, however long
		// after the call that started it - as game days are (game/play.ts).
		beginRoomAdvance();
		try {
			await playDays(run, numDays, conditions);
		} finally {
			changeTracker.endSim("playMenu.day");
			endRoomAdvance();
		}
	});
	if (started) {
		await runAfterActionHook("playMenu", "day");
	}
};

export default play;
