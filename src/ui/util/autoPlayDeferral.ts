// A scheduled fire that could not run when its moment came, and what becomes
// of it.
//
// THE BUG THIS EXISTS FOR. A blocked fire used to be dropped on the floor. The
// scheduler simply re-armed, and armTimer asks nextFireForRule for the next
// occurrence STRICTLY after now - so a "sim at 20:00 every day" rule that was
// blocked at 20:00 did not sim at 20:01, it simmed TOMORROW. On an
// every-N-minutes rule that healed itself within N minutes and nobody noticed;
// on a fixed-time rule the league just never simmed that night, with no trace
// beyond a breadcrumb.
//
// So a blocked fire is now HELD and retried instead of dropped. It is held only
// until the next scheduled fire comes round: past that moment the schedule has
// something newer to say, and running the stale one first would sim the room's
// days late, or two in a row to catch up. Whichever comes first wins - it runs,
// or the schedule takes over.

export type DeferredFire<Fire> = {
	fire: Fire;
	// The next scheduled fire, as computed at the moment we first held this one.
	// undefined means nothing else is scheduled, so the held fire keeps its claim
	// on the timer indefinitely.
	supersededAt: number | undefined;
};

// The tolerance the scheduler already uses to decide a moment has "arrived"
// (see onTimer). Shared so a held fire hands the timer over exactly when the
// normal arming path is willing to pick the scheduled fire up - a handover one
// millisecond late would find nextFireForRule looking for something strictly
// AFTER the slot, and skip that fire too. Which is the original bug again.
export const ARRIVED_MS = 250;

// Has the schedule caught up with, and taken over from, a held fire?
export const deferralSuperseded = (
	deferred: DeferredFire<unknown>,
	now: number,
): boolean =>
	deferred.supersededAt !== undefined &&
	now >= deferred.supersededAt - ARRIVED_MS;

// How long to wait before looking at a held fire again: `retryMs`, but never
// past the handover moment. Only meaningful while the fire is still held, so
// callers must check deferralSuperseded first - which also guarantees the
// result is positive.
export const deferralRetryDelay = (
	deferred: DeferredFire<unknown>,
	now: number,
	retryMs: number,
): number =>
	deferred.supersededAt === undefined
		? retryMs
		: Math.min(retryMs, deferred.supersededAt - ARRIVED_MS - now);
