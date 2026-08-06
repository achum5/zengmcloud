import type { SyncNotification } from "./notifications.ts";

// Push notifications for a game you are WATCHING.
//
// A live sim computes the whole result up front and then animates it, so the
// entire window is force-silent (see afterActionHook): pushing "Celtics 102,
// Heat 98" to the room while the person is still on Q1 spoils the game for
// everyone, including the one watching it.
//
// But silence was implemented as DROPPING those pushes, and the bill comes due
// in the playoffs, where nearly every game is watched rather than simmed: the
// day advanced, the results synced to everyone, and the room was simply never
// told. Nothing ever re-fired them, because the silent window closes when the
// sim finishes COMPUTING - which is seconds in, long before the playback ends.
//
// So hold them instead of dropping them, and send them when the playback
// actually ends (onLiveSimOver, the same signal that unfreezes the score
// ticker). Only live sims arm this. "Sim one game" has no playback and no
// end-of-playback signal, and stays silent as before - you deliberately simmed
// one game out of a day you left unplayed.

// Safety cap on holding NEW notifications, so a device that never reports its
// playback over (crashed, killed mid-game) cannot silence itself forever. It
// deliberately does not bound the RELEASE: whatever was held still goes out
// whenever the signal finally arrives.
const HOLD_MAX_MS = 30 * 60 * 1000;

let holdUntil = 0;
let held: SyncNotification[] = [];

// Called when a live sim starts, before anything it changed can be drained.
export const beginLiveSimNotificationHold = () => {
	holdUntil = Date.now() + HOLD_MAX_MS;
	held = [];
};

export const isLiveSimNotificationHoldActive = (): boolean =>
	Date.now() < holdUntil;

export const holdLiveSimNotifications = (notifications: SyncNotification[]) => {
	held.push(...notifications);
};

// Stop holding and hand back what was held, deduped. Several drains can land
// inside one playback window (the playback's own navigation spawns worker calls
// that drain the tracker), and each builds its notifications from whatever it
// caught - so the same game summary can be produced more than once.
//
// Returns the held batch regardless of whether the cap has expired: an expired
// hold means "stop holding new ones", never "throw away the ones you have".
export const releaseLiveSimNotifications = (): SyncNotification[] => {
	holdUntil = 0;
	const batch = held;
	held = [];
	const seen = new Set<string>();
	return batch.filter((notification) => {
		const key = `${notification.title} ${notification.body}`;
		if (seen.has(key)) {
			return false;
		}
		seen.add(key);
		return true;
	});
};
