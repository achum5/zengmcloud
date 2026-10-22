import type { Shot } from "./camera.ts";
import type { Play } from "./director.ts";
import type { FieldMoment } from "./evaluate.ts";
import { FIELD_L, GOAL_L, GOAL_R, MID_Y } from "./geometry.ts";

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
// goes downfield. Taken back the other way (a pick, a fumble), ahead of
// the man with it.
const FOLLOWS = new Set<Play["kind"]>(["pass", "run", "kneel"]);
const FOLLOW_AHEAD = 12;
const OPEN_UP = 0.3;
const WIDEST = 190;

// A kick, from when it is away: wide with the ball in the air - tilting up
// after it as it goes high, never so far as to look past the end zone into
// the stands - then on the returner, ahead of him.
const IN_AIR: Partial<Record<Play["kind"], number>> = {
	kickoff: 215,
	onside: 150,
	punt: 195,
};
const RETURN_WIDTH = 160;
const TILT_FROM = 36;
const TILT = 0.5;
// A field goal, from the snap: opened up to take in the holder and the posts
// both, and up a little for the uprights.
const HOLD_BACK = 21;
const KICK_ROOM = 50;
const KICK_LIFT = 14;

const across = (y: number) => MID_Y + (y - MID_Y) * 0.45;

export const aimFor = (m: FieldMoment): Shot => {
	const { play, t, focus } = m;
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
	if (play.kind === "fieldGoal" || play.kind === "extraPoint") {
		if (t < play.snap) {
			return set;
		}
		const posts = play.dir === 1 ? FIELD_L : 0;
		const spot = play.losX - play.dir * HOLD_BACK;
		return {
			x: (posts + spot) / 2,
			width: Math.min(230, Math.max(130, Math.abs(posts - spot) + KICK_ROOM)),
			y: set.y,
			z: KICK_LIFT,
		};
	}
	if (t < play.follow || !focus) {
		return set;
	}
	const air = IN_AIR[play.kind];
	if (air !== undefined) {
		if (m.carrier === undefined || m.carrier === play.offense) {
			return {
				x: Math.min(GOAL_R - 12, Math.max(GOAL_L + 12, focus.x)),
				width: air,
				y: across(focus.y),
				z: 2 + Math.max(0, focus.z - TILT_FROM) * TILT,
			};
		}
		return {
			x: focus.x - play.dir * FOLLOW_AHEAD,
			width: RETURN_WIDTH,
			y: across(focus.y),
		};
	}
	if (!FOLLOWS.has(play.kind)) {
		return set;
	}
	const down = (focus.x - play.losX) * play.dir;
	const back = m.carrier !== undefined && m.carrier !== play.offense;
	const lead = back
		? down - FOLLOW_AHEAD
		: Math.max(down + FOLLOW_AHEAD, f.ahead * 0.6);
	return {
		x: play.losX + play.dir * lead,
		width: Math.min(WIDEST, f.width + Math.max(0, down) * OPEN_UP),
		y: across(focus.y),
	};
};
