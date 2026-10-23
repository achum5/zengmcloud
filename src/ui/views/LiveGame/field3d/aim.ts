import type { Shot } from "./camera.ts";
import type { Play } from "./director.ts";
import type { FieldMoment } from "./evaluate.ts";
import { FIELD_L, GOAL_L, GOAL_R, MID_Y, type Pt } from "./geometry.ts";

// WHERE THE CAMERA LOOKS: the way a game is shot from the press box - on the
// line before the snap, the offense's backfield on one side of the picture
// and the defense's deep men on the other; wide for a kick, to take in both
// teams.

// How wide (feet across the field at the ball) each kind of snap is framed,
// and how far ahead of the line (toward the defense) the middle of the
// picture is.
const FRAMING: Record<Play["kind"], { width: number; ahead: number }> = {
	pass: { width: 104, ahead: 12 },
	run: { width: 98, ahead: 10 },
	kneel: { width: 88, ahead: 6 },
	none: { width: 98, ahead: 10 },
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
// Once a man of the offense has it past the line - a catch, a back through
// the hole - and after the whistle, in on him: narrowing the further he
// gets, the men round him and not the whole field.
const CARRY_WIDTH = 84;
const NARROW = 0.5;
// However it goes, no further toward an end line than to take in the end
// zone and a strip past it (feet), not the stands behind.
const PAST_END = 12;

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

// Across the field, on the near side of the middle: the score bug and the
// caption sit over the foot of the picture, so a man split wide on the near
// side, or the ball carried down the near sideline, is kept up clear of
// them. Before the snap, on the middle of the twenty-two as they will stand
// at it - that far on past it, the far side drawn smaller.
const SET_UP = 29;
const SET_MOST = 40;
const NEAR = 18;
const across = (y: number) => MID_Y + NEAR + (y - MID_Y) * 0.7;
const HURT_WIDTH = 96;
// The game over: wide on them all, and in on the head coaches from this long
// before they shake hands, over this long.
const POST_WIDE = 124;
const POST_TIGHT = 66;
const POST_IN = 5500;
const POST_IN_MS = 4000;

export const aimFor = (m: FieldMoment): Shot => {
	const { play, t, focus } = m;
	if (!play) {
		return { x: 180, width: 260, y: MID_Y };
	}
	const f = FRAMING[play.kind];
	const set = {
		x: play.losX + play.dir * f.ahead,
		width: f.width,
		// (A kick across the field, most of the way to the middle.)
		y:
			FOLLOWS.has(play.kind) || play.kind === "none"
				? setY(play)
				: MID_Y + (play.ballY - MID_Y) * 0.35,
	};
	// A man hurt: on him, and the trainers round him, and with him off.
	const hurt = play.hurt;
	if (hurt && t >= hurt.from) {
		const him = m.players.find((p) => p.id === hurt.pid);
		if (him) {
			return { x: him.x, width: HURT_WIDTH, y: across(him.y) };
		}
	}
	// The game over: on them all as they make for the middle - and in on the
	// head coaches as they come together to shake hands.
	const post = play.postgame;
	if (post && t >= post.from) {
		const crowd = middleOf(play, m) ?? post.at;
		const u = Math.min(1, Math.max(0, (t - post.shake + POST_IN) / POST_IN_MS));
		const e = u * u * (3 - 2 * u);
		return {
			x: crowd.x + (post.at.x - crowd.x) * e,
			width: POST_WIDE + (POST_TIGHT - POST_WIDE) * e,
			y: across(crowd.y + (post.at.y - crowd.y) * e),
		};
	}
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
		return FOLLOWS.has(play.kind)
			? { ...set, x: inBounds(set.x, set.width) }
			: set;
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
	const carried = m.carrier === play.offense || t >= play.whistle;
	const width = back
		? f.width
		: carried
			? Math.max(CARRY_WIDTH, f.width - Math.max(0, down) * NARROW)
			: Math.min(WIDEST, f.width + Math.max(0, down) * OPEN_UP);
	return {
		x: inBounds(play.losX + play.dir * lead, width),
		width,
		y: across(focus.y),
	};
};

const setYs = new WeakMap<Play, number>();
const setY = (play: Play) => {
	let y = setYs.get(play);
	if (y === undefined) {
		let near = -Infinity;
		let far = Infinity;
		for (const part of play.parts) {
			const leg =
				part.legs.find((l) => l.t0 <= play.snap && l.t1 >= play.snap) ??
				part.legs[0];
			if (!leg) {
				continue;
			}
			const u =
				leg.t1 > leg.t0
					? Math.min(1, Math.max(0, (play.snap - leg.t0) / (leg.t1 - leg.t0)))
					: 1;
			const at = leg.from.y + (leg.to.y - leg.from.y) * u;
			near = Math.max(near, at);
			far = Math.min(far, at);
		}
		y =
			near < far
				? MID_Y + SET_UP
				: Math.min(
						MID_Y + SET_MOST,
						Math.max(MID_Y, (near + far) / 2 + SET_UP),
					);
		setYs.set(play, y);
	}
	return y;
};

const inBounds = (x: number, width: number) =>
	Math.min(FIELD_L - width / 2 + PAST_END, Math.max(width / 2 - PAST_END, x));

// The middle of the men on the field for a play, at the moment.
const inPlay = new WeakMap<Play, Set<number>>();
const middleOf = (play: Play, m: FieldMoment): Pt | undefined => {
	let ids = inPlay.get(play);
	if (!ids) {
		ids = new Set(play.parts.map((p) => p.pid));
		inPlay.set(play, ids);
	}
	let x = 0;
	let y = 0;
	let n = 0;
	for (const p of m.players) {
		if (ids.has(p.id)) {
			x += p.x;
			y += p.y;
			n++;
		}
	}
	return n > 0 ? { x: x / n, y: y / n } : undefined;
};
