import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { getSyncEngine } from "./engineHolder.ts";
import { parseSimStopDays, stopsOnDay } from "../../../common/simStopDays.ts";
import type { ScheduleGame } from "../../../common/types.ts";

// The places the sim stops on purpose.
//
// There are two kinds and they behave identically. The TRADE DEADLINE sits in
// the schedule as a sentinel "game" with both tids set to -3. A DAY STOP is
// just a regular-season day number the league has asked to pause before - day
// 15, say, because that is when the summer's signings become tradeable. Both
// are configured in one League Setting (see common/simStopDays.ts), and an
// empty setting means the sim never stops on its own.
//
// The deadline used to stop the league whether it wanted to or not, and it was
// the only stop that existed. Making it an entry in a list is what lets a room
// pause on the days that actually matter to it, and lets a room that does not
// want the interruption turn it off.
//
// When a stop is configured, it stops:
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

// What the sim is standing in front of, if anything.
//
// A day stop needs no bookkeeping to say whether it has been crossed: crossing
// it means PLAYING that day, after which the next day on the schedule is a
// different number and the stop is simply no longer pending. That is the whole
// reason a stop is defined as "before day N" rather than "after" - the schedule
// itself remembers, so there is no cleared-list to sync, to migrate, or to get
// out of step between devices.
// `day` is carried on BOTH kinds because the ready-up gate needs a step number
// that increases through the season - see stopStep in draftReady.ts. Without
// it every stop in a season shared one step, and readying up for the first one
// counted as readying up for all of them.
export type SimStopPoint =
	| { kind: "deadline"; gid: number; day: number | undefined }
	| { kind: "day"; day: number };

export const getPendingSimStop = async (): Promise<
	SimStopPoint | undefined
> => {
	const stops = parseSimStopDays(g.get("simStopDays"));
	if (!stops.deadline && stops.days.length === 0) {
		return undefined;
	}

	const schedule = await idb.cache.schedule.getAll();
	const first = schedule[0];
	if (!first) {
		return undefined;
	}

	if (isTradeDeadlineGame(first)) {
		return stops.deadline
			? { kind: "deadline", gid: first.gid, day: first.day }
			: undefined;
	}
	return stopsOnDay(stops, first.day)
		? { kind: "day", day: first.day! }
		: undefined;
};

// A one-shot permission to cross the next stop, held on THIS DEVICE only and
// consumed by the first sim that meets one.
//
// Two things need it and both are a deliberate act by a person or by the room:
// the ready-up evaluator, whose way of crossing a day stop is to play that day
// through the ordinary sim path; and a user who was shown exactly who has not
// readied up and chose "Advance anyway" (see ui/util/confirmPlayMenuAdvance).
// Nothing else can set it, and it does not survive the sim it was granted for.
let crossNextStop = false;

export const allowCrossingNextSimStop = () => {
	crossNextStop = true;
};

export const clearCrossingNextSimStop = () => {
	crossNextStop = false;
};

const consumeCrossingNextSimStop = () => {
	const allowed = crossNextStop;
	crossNextStop = false;
	return allowed;
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
// arrival, cross on the next press. Gated, the answer is always stop unless
// this device has been given the one-shot permission above - so the room
// crosses by readying up, and a person crosses by being told who they are
// stepping over and saying yes anyway. Simming harder, on its own, is still not
// a way around anybody.
export const shouldStopAtSimStop = (start: boolean) => {
	if (consumeCrossingNextSimStop()) {
		return false;
	}
	return gateActive || !start;
};

// The same question asked by a SINGLE-GAME sim (Sim one game / Watch game).
//
// A single game is never the act that crosses a stop - crossing is a decision
// about the DAY, made by the room readying up or a person choosing "Advance
// anyway", and the one-shot permission above belongs to the full-day sim that
// decision launched. This deliberately does not consult (and so can never
// CONSUME) that permission: a "Sim my game" press racing the ready-up advance
// used to eat the advance's one-shot, leaving the advance to stop at the very
// gate it was sent to cross - and then falsely complete its claim, sealing the
// step while the whole room showed ready. So: gated, a single game simply
// waits with everyone else; ungated (solo league), it plays - same as before,
// when a fresh press never stopped.
export const singleGameWaitsAtSimStop = () => gateActive;

// Fires once per stop, from the device that reached it - which is the one
// simming, so there is exactly one of it and no need for the
// smallest-client-id election the holdout nudge uses. Cleared when the gate is
// disarmed so a reconnect in a later season announces again.
let lastNotifiedKey: string | undefined;

export const notifySimStopArrived = async (what: string) => {
	const engine = getSyncEngine();
	if (!engine) {
		return;
	}

	const key = `${g.get("lid")}|${g.get("season")}|${what}`;
	if (key === lastNotifiedKey) {
		return;
	}
	lastNotifiedKey = key;

	try {
		await engine.publishNotification({
			title: what,
			body: `${what} — the league is paused. Make your moves, then ready up; it sims on once everyone has.`,
			targetTids: null,
			path: "trade",
		});
	} catch {
		// A missed announcement is harmless, and the ready-up control shows the
		// same thing. Clear the key so a later attempt can retry.
		lastNotifiedKey = undefined;
	}
};
