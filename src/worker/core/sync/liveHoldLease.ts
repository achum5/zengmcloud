// THE PUSH HOLD: this device in the middle of a live game - its own, or a
// league mate's it is watching - says so on its member doc in the room, and
// the Cloud Function that sends pushes holds this device's until it stops
// saying so (see functions/hold.js). A phone has to show every push it gets,
// so the only place a "Playoff scores" push can wait for the game to end is
// before it is sent.
//
// A lease, not a flag: while held it is written again every so often, and
// the function only believes one written lately. A phone that went dark mid
// game - closed, out of battery - stops holding its pushes on its own. Not
// so soon that a phone locked for a few minutes mid game, which picks the
// game back up where it was when it wakes, gets the final pushed to it.

// How often a hold is written again while it lasts. The function believes
// one for ten of these (LIVE_HOLD_LEASE_MS in functions/hold.js).
export const LIVE_HOLD_REFRESH_MS = 2 * 60 * 1000;

export const createLiveHoldLease = ({
	write,
	now = Date.now,
	refreshMs = LIVE_HOLD_REFRESH_MS,
}: {
	write: (held: boolean) => Promise<void>;
	now?: () => number;
	refreshMs?: number;
}) => {
	// What the member doc says, as far as this device knows (undefined: not
	// written on this connection yet - whatever a last session left there is
	// put right by the first write).
	let written: boolean | undefined;
	let writtenAt = 0;
	let writing = false;
	let wanted = false;

	const sync = (held: boolean) => {
		wanted = held;
		if (writing) {
			return;
		}
		if (held === written && (!held || now() - writtenAt < refreshMs)) {
			return;
		}
		writing = true;
		const at = now();
		write(held).then(
			() => {
				writing = false;
				written = held;
				writtenAt = at;
				// (Changed while that was on its way.)
				if (wanted !== held) {
					sync(wanted);
				}
			},
			() => {
				// Tried again on the next sync, not straight away.
				writing = false;
			},
		);
	};

	return {
		sync,
		held: () => written === true,
	};
};
