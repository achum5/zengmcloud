import {
	courtRosterOf,
	courtTeamsOf,
	introKindOf,
	stakesOf,
} from "./courtInput.ts";
import { dropStagedCourt, stageCourt } from "./stageCourt.ts";

// A LEAGUE-MATE'S LIVE GAME, STAGED WHILE THIS DEVICE IS ASKED WHETHER TO
// WATCH IT. A phone takes the better part of a minute to stage a whole game,
// and it used to start only once "Watch" was tapped and the page was up -
// so whoever watched sat on the warm-ups while the game they had said yes to
// went on without them. Staged from the same box score and play-by-play the
// page will start it from (see the worker's liveFollowPreview), read the same
// way the court reads them (courtInput.ts), it is the very staging the court
// then asks for (see stageCourt's keeping) - ready, or that much nearer.
export const prestageCourt = (
	events: any[],
	boxScore: any,
	gender: "female" | "male" | undefined,
) => {
	const raw = courtTeamsOf(boxScore?.teams, boxScore?.replayLooks);
	void stageCourt({
		events,
		players: courtRosterOf(raw[1], raw[0]),
		gid: boxScore?.gid,
		gender,
		intro: introKindOf(boxScore?.playoffs),
		stakes: stakesOf({
			playoffs: boxScore?.playoffs,
			finals: boxScore?.finals,
			numGamesToWinSeries: boxScore?.numGamesToWinSeries,
			gameOver: boxScore?.gameOver,
			won: boxScore?.won,
			teams: boxScore?.teams,
		}),
	}).catch(() => {
		// The court stages it itself, if it is watched.
	});
};

// Turned down after all: not kept, and not staged on any further.
export const dropPrestaged = (gid: number) => {
	dropStagedCourt(gid);
};
