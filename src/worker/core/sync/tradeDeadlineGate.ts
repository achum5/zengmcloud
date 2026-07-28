import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { getSyncEngine } from "./engineHolder.ts";
import type { ScheduleGame } from "../../../common/types.ts";

// The trade deadline, as a place the sim actually stops.
//
// The deadline sits in the schedule as a sentinel "game" with both tids set to
// -3. Every other phase change ends a sim; this one did not - play.ts deleted
// the sentinel, flipped the phase and carried straight on into the next day. So
// a league simming a week could pass its deadline without anyone being given a
// chance to do anything about it, which for a room of humans is the single
// worst moment to blow through.
//
// Now it stops:
//
//   - Alone, the sim stops the first time it REACHES the deadline. Pressing
//     play again crosses it, so it costs one click and can never dead-end.
//   - In a shared league it becomes a ready-up gate like the draft or free
//     agency: nobody crosses until every team has said they're done trading,
//     and the crossing is performed by the ready-up evaluator rather than by
//     whoever happens to be simming (see draftReady.ts).
//
// The distinction matters for safety. Gating means the ordinary sim path
// REFUSES to cross, so if the gate were ever active without something able to
// open it the league would be stuck - which is why the gate is armed by
// draftReady itself, from the same lifecycle that runs the evaluator, rather
// than inferred from "is sync connected".

const DEADLINE_TID = -3;

export const isTradeDeadlineGame = (game: ScheduleGame | undefined) =>
	game !== undefined &&
	game.homeTid === DEADLINE_TID &&
	game.awayTid === DEADLINE_TID;

// The deadline sentinel, if it is the very next thing on the schedule.
// getSchedule guarantees the sentinel is alone on its day, so checking the head
// of the day is enough.
export const getTradeDeadlineGame = async (): Promise<
	ScheduleGame | undefined
> => {
	const schedule = await idb.cache.schedule.getAll();
	const first = schedule[0];
	return isTradeDeadlineGame(first) ? first : undefined;
};

// Armed by setupDraftReady and disarmed by teardownDraftReady, so it is true
// exactly when there is an evaluator able to cross the deadline. Anything less
// direct - "sync is connected", say - risks a state where the sim refuses to
// cross and nothing else can either.
let gateActive = false;

export const setTradeDeadlineGateActive = (active: boolean) => {
	gateActive = active;
	if (!active) {
		lastNotifiedKey = undefined;
	}
};

export const isTradeDeadlineGateActive = () => gateActive;

// Should the sim stop rather than cross?
//
// `start` is true only when a play-menu action began this sim, false for the
// recursive day-after-day continuation. Alone, that is the whole rule: stop on
// arrival, cross on the next press. Gated, the answer is always stop - the
// evaluator crosses, and pressing play harder must not be a way around the
// room.
export const shouldStopAtTradeDeadline = (start: boolean) =>
	gateActive || !start;

// Fires once per season, from the device that reached the deadline - which is
// the one simming, so there is exactly one of it and no need for the
// smallest-client-id election the holdout nudge uses. Cleared when the gate is
// disarmed so a reconnect in a later season announces again.
let lastNotifiedKey: string | undefined;

export const notifyTradeDeadlineArrived = async () => {
	const engine = getSyncEngine();
	if (!engine) {
		return;
	}

	const key = `${g.get("lid")}|${g.get("season")}`;
	if (key === lastNotifiedKey) {
		return;
	}
	lastNotifiedKey = key;

	try {
		await engine.publishNotification({
			title: "Trade deadline",
			body: "The deadline is here and the league is paused. Make your moves, then ready up — it sims on once everyone has.",
			targetTids: null,
			path: "trade",
		});
	} catch {
		// A missed announcement is harmless, and the ready-up control shows the
		// same thing. Clear the key so a later attempt can retry.
		lastNotifiedKey = undefined;
	}
};
