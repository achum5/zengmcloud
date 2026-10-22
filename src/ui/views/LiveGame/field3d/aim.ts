import type { Shot } from "./camera.ts";
import type { Play } from "./director.ts";
import { MID_Y, type Pt3 } from "./geometry.ts";

// WHERE THE CAMERA LOOKS: the way a game is shot from the press box - on the
// line before the snap, the offense's backfield on one side of the picture
// and the defense's deep men on the other; wide for a kick, to take in both
// teams.

// How wide (feet across the field at the ball) each kind of snap is framed,
// and how far ahead of the line (toward the defense) the middle of the
// picture is.
const FRAMING: Record<Play["kind"], { width: number; ahead: number }> = {
	pass: { width: 118, ahead: 12 },
	run: { width: 112, ahead: 10 },
	kneel: { width: 100, ahead: 6 },
	none: { width: 112, ahead: 10 },
	fieldGoal: { width: 112, ahead: -4 },
	extraPoint: { width: 112, ahead: -4 },
	punt: { width: 170, ahead: 10 },
	kickoff: { width: 250, ahead: 90 },
	onside: { width: 150, ahead: 25 },
};

// From the snap on, it goes with the ball: held on the line while the
// ball is back behind it (the pocket, the backfield), then after it - a
// little ahead of it, the way the play is going - opening up as the play
// goes downfield.
const FOLLOWS = new Set<Play["kind"]>(["pass", "run", "kneel"]);
const FOLLOW_AHEAD = 12;
const OPEN_UP = 0.3;
const WIDEST = 190;

export const aimFor = (
	play: Play | undefined,
	t: number,
	focus: Pt3 | undefined,
): Shot => {
	if (!play) {
		return { x: 180, width: 260, y: MID_Y };
	}
	const f = FRAMING[play.kind];
	const set = {
		x: play.losX + play.dir * f.ahead,
		width: f.width,
		// (Across the field, most of the way to the middle.)
		y: MID_Y + (play.ballY - MID_Y) * 0.35,
	};
	if (t < play.snap || !focus || !FOLLOWS.has(play.kind)) {
		return set;
	}
	const down = (focus.x - play.losX) * play.dir;
	const lead = Math.max(down + FOLLOW_AHEAD, f.ahead * 0.6);
	return {
		x: play.losX + play.dir * lead,
		width: Math.min(WIDEST, f.width + Math.max(0, down) * OPEN_UP),
		y: MID_Y + (focus.y - MID_Y) * 0.45,
	};
};
