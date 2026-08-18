// WHOSE BOX SCORE COMES FIRST DURING A LIVE GAME, AND WHETHER THE OTHER ONE
// SHOWS AT ALL.
//
// The live game page stacks both teams' box scores under the court. The order
// was always [away, home], which means the team you actually manage is below
// the fold half the time - you sim your own game and end up scrolling past the
// opponent's stat line to reach your own, on every play. A league-mate's
// request, verbatim: "be able to hide other teams box score during live game
// so I can see the Cavs at the top with the game action".
//
// So the box scores order by whose device this is, and the other team can be
// collapsed to a single line. The SCORE HEADER above is untouched - it stays
// away-over-home, because that is a scoreboard and reordering a scoreboard by
// who is watching would be wrong.
//
// The ordering lives here rather than inline in the box score because the live
// game's jump-to-team buttons have to agree with it: those buttons scroll to
// `scroll-team-1` / `scroll-team-2`, which are assigned by POSITION. Order the
// teams in one place and both the anchors and the buttons that target them
// come from the same list; order it in two places and the buttons eventually
// scroll to the wrong team.

import { safeLocalStorage } from "./safeLocalStorage.ts";

// Put the device's own team first, keeping the other in place behind it.
//
// Returns the array unchanged when this device has no team in the game - a
// neutral game, or watching a league-mate's broadcast - because there is no
// "your team" to hoist and away-over-home is the honest default.
export const orderBoxScoreTeams = <T extends { tid?: number }>(
	teams: readonly T[],
	userTid: number | undefined,
): readonly T[] => {
	if (userTid === undefined || teams.length !== 2) {
		return teams;
	}
	const userIndex = teams.findIndex((t) => t?.tid === userTid);
	if (userIndex <= 0) {
		// -1: not this device's game. 0: already on top.
		return teams;
	}
	return [teams[userIndex]!, ...teams.filter((_, i) => i !== userIndex)];
};

// Per device, not per league: it describes how this screen is being watched.
const HIDE_KEY = "bbgmLiveHideOtherBoxScore";

export const getHideOtherBoxScore = (): boolean =>
	safeLocalStorage.getItem(HIDE_KEY) === "true";

export const setHideOtherBoxScore = (value: boolean): void => {
	safeLocalStorage.setItem(HIDE_KEY, String(value));
};

// Which of the ordered teams may be collapsed: only the one that is NOT this
// device's, and only while a game is actually being played. Hiding your own
// team makes no sense, and a finished box score is a record to read rather
// than a live screen to keep tidy.
export const canHideBoxScoreTeam = ({
	tid,
	userTid,
	liveGameInProgress,
}: {
	tid: number | undefined;
	userTid: number | undefined;
	liveGameInProgress: boolean;
}): boolean =>
	liveGameInProgress &&
	userTid !== undefined &&
	tid !== undefined &&
	tid !== userTid;
