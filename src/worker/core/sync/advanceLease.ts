import { g } from "../../util/index.ts";
import { getSyncEngine } from "./engineHolder.ts";

// THE SMART CPU HOLDS THE WORKER FOR MINUTES AT A TIME. Its front offices
// think a day's trades, a free agency day or a phase's market through without
// a pause the worker could renew a lease in - no timer runs, no write goes
// out - and on a phone one such stretch can run a few minutes. Every lease
// that guards a step of the league's advance (the sim-day fence, the ready-up
// claim, the room's busy mark) runs at least this long in a league with it on,
// so a step still running is never taken for a crashed one and run again by
// another device, and league mates' edits stay held until the sim is
// published. The cost: a device that really did crash mid-step holds the room
// this long before another may run the step.
export const SMART_CPU_LEASE_MS = 10 * 60_000;

export const advanceLeaseMs = (normal: number) =>
	g.get("smartCpu") ? Math.max(normal, SMART_CPU_LEASE_MS) : normal;

// An advance of the league (a sim of a day or more, a ready-up step) holds the
// room busy from its first step to its last - a sim of several days runs on
// after the call that started it has returned (game/play.ts), and a lease
// stamped once at the call ran out under a long one. A no-op outside a shared
// league, and on a device not in charge of simming.
export const beginRoomAdvance = () => {
	getSyncEngine()?.beginAdvance();
};

// Before each step: the busy mark renewed, the write confirmed - the step may
// hold the worker longer than any timer could renew it.
export const holdRoomForAdvance = async () => {
	await getSyncEngine()?.holdRoomBusy();
};

export const endRoomAdvance = () => {
	getSyncEngine()?.endAdvance();
};
