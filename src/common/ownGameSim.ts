// May this device sim its OWN team's game while someone else is in charge of
// simming?
//
// Normally every timeline advance requires sim authority, because two devices
// simming the same games double-apply every incremental aggregate (team
// records, head-to-heads, player stats) and diverge from the game log
// permanently. Your own single game is the one safe exception, because a sim of
// one gid is a disjoint slice: the server-side fence in simDayClaimPolicy.ts
// already tracks which gids of a day have been claimed and refuses an
// overlapping ask, and that is what actually makes this safe.
//
// This function is the POLICY, not the safety. The fence is the safety. What
// this adds is avoiding the collision in the first place, because the fence's
// refusal is blunt: an auto-play device asking for a whole day that includes a
// gid you already simmed has its ENTIRE claim refused, so the room loses that
// tick. Hence the cutoff below.
//
// Lives in `common` because the worker enforces it and the UI greys the buttons
// with it, and the build blocks UI code from importing worker modules.

// Seconds before the scheduled auto sim during which a non-simming device may
// not start one of its own.
//
// The window only has to cover UPLOAD LATENCY, not the length of a live sim:
// the game is simulated and published immediately, and the live sim is just the
// animation playing afterwards. So the race is "I pressed sim and the record
// has not reached the simmer yet", which is seconds.
export const DEFAULT_OWN_GAME_SIM_CUTOFF_SECONDS = 45;

export type OwnGameSimDecision =
	| { allow: true }
	| { allow: false; reason: string };

export const decideOwnGameSim = ({
	isOwnGame,
	isAuthority,
	connectedAndReady,
	simInFlight,
	msUntilAutoSim,
	cutoffSeconds,
}: {
	// Does the game involve the team this device currently controls? In
	// multi-team mode that is the SELECTED team, not every team owned.
	isOwnGame: boolean;
	// The device in charge of simming plays by the ordinary rules; this
	// exception is not for it.
	isAuthority: boolean;
	connectedAndReady: boolean;
	// A sim already running anywhere in the room - local, or a league-mate's
	// broadcast. Two at once is exactly what the fence would refuse.
	simInFlight: boolean;
	// Until the room's scheduled auto sim. Undefined when nobody is auto-playing,
	// which removes the race entirely.
	msUntilAutoSim: number | undefined;
	cutoffSeconds: number;
}): OwnGameSimDecision => {
	if (isAuthority) {
		return { allow: true };
	}
	if (!connectedAndReady) {
		return { allow: false, reason: "Not connected to the league yet." };
	}
	if (!isOwnGame) {
		return {
			allow: false,
			reason: "You can only sim your own team's game.",
		};
	}
	if (simInFlight) {
		return {
			allow: false,
			reason: "A game is already being simmed. Try again in a moment.",
		};
	}
	if (
		msUntilAutoSim !== undefined &&
		Number.isFinite(msUntilAutoSim) &&
		msUntilAutoSim <= cutoffSeconds * 1000
	) {
		return {
			allow: false,
			reason: `Too close to the scheduled sim — wait for it to play this day.`,
		};
	}
	return { allow: true };
};
