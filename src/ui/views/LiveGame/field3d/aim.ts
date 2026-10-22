import type { Shot } from "./camera.ts";
import type { Play } from "./director.ts";
import { MID_Y } from "./geometry.ts";

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

export const aimFor = (play: Play | undefined): Shot => {
	if (!play) {
		return { x: 180, width: 260, y: MID_Y };
	}
	const f = FRAMING[play.kind];
	return {
		x: play.losX + play.dir * f.ahead,
		width: f.width,
		// (Across the field, most of the way to the middle.)
		y: MID_Y + (play.ballY - MID_Y) * 0.35,
	};
};
