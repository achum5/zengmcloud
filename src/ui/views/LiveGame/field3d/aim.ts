import { HERO_CAM_FT } from "./action.ts";
import type { FieldRig, Shot } from "./camera.ts";
import { type Play, QB_CAM_FT, TABLET_CAM_FT } from "./director.ts";
import type { FieldMoment } from "./evaluate.ts";
import {
	FIELD_L,
	FIELD_W,
	GOAL_L,
	GOAL_R,
	MID_X,
	MID_Y,
	type Pt,
} from "./geometry.ts";
import { officialId, REF_CAM_FT } from "./crew.ts";
import { INTERVIEW_CAM_FT } from "./postgame.ts";
import { GREET_CAM_FT } from "./greet.ts";

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
// (Eased onto it from the shot on the line over this long, ms.)
const FOLLOW_EASE_MS = 400;
// (And in on the man with it over this long, ms; and onto it, the ball
// dead, over this long.)
const CARRY_EASE_MS = 500;
const SETTLE_MS = 1400;
const OPEN_UP = 0.3;
const WIDEST = 190;
// Once a man of the offense has it past the line - a catch, a back through
// the hole - and after the whistle, in on him: narrowing the further he
// gets, the men round him and not the whole field.
const CARRY_WIDTH = 84;
const NARROW = 0.5;
// And whatever that makes of it, the ball itself no nearer the edge of the
// picture than this (feet): loose a long way back of the line - a fumble
// batted about behind it - not left out of the picture, the camera held on
// the line.
const KEEP_IN = 18;
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
// The last look, the hands all shaken (see postgame.ts): the whole stadium
// from the press box - the field end to end, the stands across it and the
// lights over them - pulled back out to it over this long, and held there.
const LOOK_FROM = { width: 300, y: 66, z: 2 };
const LOOK = { width: 440, y: 29, z: 21 };
const LOOK_MS = 3000;
// The interview after it (see postgame.ts): in close on the two of them,
// this wide, this high - from down on the field in front of them at about
// their heads' height (see rigFor).
const INTERVIEW_WIDTH = 14;
const INTERVIEW_Z = 6;
const INTERVIEW_CAM_Z = 6.2;
// In close on the kicker through his routine before the snap (see the
// director's kickerRoutine): this wide, at about his chest - from down on
// the field this far in front of where he sets, this high (see rigFor).
const KICK_CLOSE = { width: 24, z: 3.4, camFt: 26, camZ: 4.4 };
// And on the quarterback at the line before the snap (see the director's
// QB_CLOSE): from his waist up, the camera up over the linemen down in
// front of him and a way along toward the defense, on his face.
const QB_CLOSE = { width: 16, z: 5, camFt: QB_CAM_FT, camZ: 6.8 };
// And on the referee giving a foul after the play (see the director's
// refCall), from the knees up - his arms over his head in it.
const REF_CLOSE = { width: 13, z: 5.3, camFt: REF_CAM_FT, camZ: 6.2 };
// And picked off, on the quarterback taking it (see the director's qbRue):
// all of him, from the press box.
const RUE = { width: 15, z: 4 };
// And on the man a big play's moment is about (see the director's
// heroClose): all of him, and room over him for the ball held up, a leap
// (the men drawn bigger than they are - see body.ts FIGURE_SCALE).
const HERO_CLOSE = { width: 20, z: 5, camFt: HERO_CAM_FT, camZ: 5.6 };
// Won on the last play: on them all over the man who won it.
const MOB_WIDTH = 96;
// A big play's moment after it - a score, a stop, a takeaway: in on it,
// this wide, until this long after it.
const PARTY_WIDTH = 62;
const PARTY_AFTER = 600;
// A timeout: in close on the side that called it, at its sideline (the 3D
// court's huddle shot: see court3d/scene.ts arenaAim) - from down low on
// the near side, pushing slowly in from this wide to this, this high.
const TIMEOUT_WIDE = 56;
const TIMEOUT_TIGHT = 44;
const TIMEOUT_Z = 4;
// Called by the head coach across the field: first in on him calling it,
// from down low on the near side - this wide, this high.
const CALL_WIDTH = 18;
const CALL_Z = 4.5;
// The home side's quarterback over the tablet with his coordinator (see
// the director's TABLET_SHARE): from down on the field in front of them,
// this wide on the two of them, this high - the camera this high.
const TABLET = { width: 13, z: 5.2, camZ: 5.8 };
// The home side's man who scored, greeted on its sideline (see greet.ts):
// from down on the field in front of them, this wide on the three of them,
// this high - the camera this high.
const GREET = { width: 16, z: 5.2, camZ: 5.8 };
// The home side's head coach calling in its third down (see director.ts
// callIn): from down low on the near side - this wide, this high.
const CALL_IN_WIDTH = 15;
const CALL_IN_Z = 5;
// And taking a big play (see director.ts COACH_REACT_GLAD): the same, room
// over him for his arms.
const REACT_WIDTH = 17;
const REACT_Z = 5.2;
// Over a timeout, the crowd (see crowd.ts): from down low on the near side,
// up into the stands across the field, panning this far along them.
const CROWD_SHOT = { width: 150, y: -80, z: 22, pan: 30 };
// Between quarters: the whole stadium from the press box, panning slowly
// along it from this far one side of the middle to as far the other.
const BETWEEN = { width: 360, y: 44, z: 11, pan: 30 };

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
	// On the man who scored, greeted on his sideline.
	const greet = greetOf(m);
	if (greet) {
		return { x: greet.at.x, width: GREET.width, y: greet.at.y, z: GREET.z };
	}
	// On the quarterback over the tablet.
	const tablet = tabletOf(m);
	if (tablet) {
		return {
			x: (tablet.at.x + tablet.oc.x) / 2,
			width: TABLET.width,
			y: (tablet.at.y + tablet.oc.y) / 2,
			z: TABLET.z,
		};
	}
	// In close on the kicker through his routine, the referee giving a foul.
	const close = closeUpOf(m);
	if (close) {
		const him = m.players.find((p) => p.id === close.pid);
		if (him) {
			return { x: him.x, width: close.width, y: him.y, z: close.z };
		}
	}
	// Picked off: tight on the quarterback taking it, from the press box.
	const rue = play.qbRue;
	if (rue && t >= rue.t0 && t < rue.t1) {
		const him = m.players.find((p) => p.id === rue.pid) ?? rue.at;
		return { x: him.x, width: RUE.width, y: him.y, z: RUE.z };
	}
	const react = play.coachReact;
	if (react && t >= react.t0 && t < react.t1) {
		return { x: react.at.x, width: REACT_WIDTH, y: react.at.y, z: REACT_Z };
	}
	const callIn = play.callIn;
	if (callIn && t >= callIn.t0) {
		return {
			x: callIn.at.x,
			width: CALL_IN_WIDTH,
			y: callIn.at.y,
			z: CALL_IN_Z,
		};
	}
	// A timeout: on the side that called it, at its sideline - or on the
	// cheerleaders.
	const timeout = play.timeout;
	if (timeout?.cheer && t >= timeout.cheer.cut) {
		const { x, width, y } = timeout.cheer;
		return { x, width, y };
	}
	const crowd = timeout?.crowd;
	if (crowd && t >= crowd.cut) {
		const u = Math.min(1, (t - crowd.cut) / Math.max(1, play.t1 - crowd.cut));
		const { width, y, z, pan } = CROWD_SHOT;
		return { x: crowd.x + pan * u, width, y, z };
	}
	const call = timeout?.call;
	if (call && t >= call.from && t < timeout.cut) {
		const him = m.players.find((p) => p.id === call.pid);
		if (him) {
			return { x: him.x, width: CALL_WIDTH, y: him.y, z: CALL_Z };
		}
	}
	if (timeout && t >= timeout.cut) {
		const end = timeout.cheer?.cut ?? crowd?.cut ?? play.t1;
		const u = Math.min(1, (t - timeout.cut) / Math.max(1, end - timeout.cut));
		const e = u * u * (3 - 2 * u);
		return {
			x: timeout.at.x,
			width: TIMEOUT_WIDE + (TIMEOUT_TIGHT - TIMEOUT_WIDE) * e,
			y: timeout.at.y,
			z: TIMEOUT_Z,
		};
	}
	if (play.between !== undefined && t >= play.between) {
		// (To the huddles after it, if the picture goes to them.)
		const end = play.timeout?.cut ?? play.t1;
		const u = Math.min(1, (t - play.between) / Math.max(1, end - play.between));
		const { width, y, z, pan } = BETWEEN;
		return { x: MID_X + pan * (2 * u - 1), width, y, z };
	}
	// A man hurt: on him, and the trainers round him, and with him off.
	const hurt = play.hurt;
	if (hurt && t >= hurt.from) {
		const him = m.players.find((p) => p.id === hurt.pid);
		if (him) {
			return { x: him.x, width: HURT_WIDTH, y: across(him.y) };
		}
	}
	// The game over: on them all as they make for the middle - and in on the
	// head coaches as they come together to shake hands; then the last look.
	const post = play.postgame;
	if (post && t >= post.look) {
		const u = Math.min(1, (t - post.look) / LOOK_MS);
		const e = u * u * (3 - 2 * u);
		return {
			x: MID_X,
			width: LOOK_FROM.width + (LOOK.width - LOOK_FROM.width) * e,
			y: LOOK_FROM.y + (LOOK.y - LOOK_FROM.y) * e,
			z: LOOK_FROM.z + (LOOK.z - LOOK_FROM.z) * e,
		};
	}
	const talk = post?.interview;
	if (talk && t >= talk.t0) {
		return {
			x: (talk.at.x + talk.her.x) / 2,
			width: INTERVIEW_WIDTH,
			y: (talk.at.y + talk.her.y) / 2,
			z: INTERVIEW_Z,
		};
	}
	if (post?.mob && t >= post.from && t < post.mob.until) {
		return {
			x: inBounds(post.mob.at.x, MOB_WIDTH),
			width: MOB_WIDTH,
			y: across(post.mob.at.y),
		};
	}
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
	const party = play.party;
	if (party && t >= party.from && t < party.until + PARTY_AFTER) {
		return {
			x: inBounds(party.at.x, PARTY_WIDTH),
			width: PARTY_WIDTH,
			y: across(party.at.y),
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
	const going = back
		? down - FOLLOW_AHEAD
		: Math.max(down + FOLLOW_AHEAD, f.ahead * 0.6);
	// (The ball dead, the picture settles onto it - the tackle, the pass
	// on the turf - not still looking on ahead of it at nobody.)
	const dead = Math.min(1, Math.max(0, (t - play.whistle) / SETTLE_MS));
	const lead = going + (down - going) * dead * dead * (3 - 2 * dead);
	const carried = m.carrier === play.offense || t >= play.whistle;
	const open = Math.min(WIDEST, f.width + Math.max(0, down) * OPEN_UP);
	const tight = Math.max(CARRY_WIDTH, f.width - Math.max(0, down) * NARROW);
	// (In on him from the open shot over a moment once he has it - a catch,
	// the whistle - not in a frame.)
	const had = carried ? t - carriedFrom(play, t, m.carrier) : 0;
	const u = Math.min(1, Math.max(0, had / CARRY_EASE_MS));
	const width = back
		? f.width
		: carried
			? open + (tight - open) * u * u * (3 - 2 * u)
			: open;
	const keep = width / 2 - KEEP_IN;
	const x = Math.min(
		focus.x + keep,
		Math.max(focus.x - keep, play.losX + play.dir * lead),
	);
	const shot = { x: inBounds(x, width), width, y: across(focus.y) };
	// (Off the line onto the ball as it goes, not a jump to it at the snap.)
	const since = t - play.follow;
	if (since < FOLLOW_EASE_MS) {
		const u = Math.max(0, since / FOLLOW_EASE_MS);
		const e = u * u * (3 - 2 * u);
		const from = inBounds(set.x, set.width);
		return {
			x: from + (shot.x - from) * e,
			width: set.width + (shot.width - set.width) * e,
			y: set.y + (shot.y - set.y) * e,
		};
	}
	return shot;
};

// Since when the offense has had it in hands, from one of its men to the
// next (a catch, not a handoff) - or, not, since the whistle.
const carriedFrom = (
	play: Play,
	t: number,
	carrier: FieldMoment["carrier"],
): number => {
	if (carrier !== play.offense) {
		return play.whistle;
	}
	let from: number | undefined;
	for (const b of play.ball) {
		if (b.t0 > t) {
			break;
		}
		const ours =
			b.kind === "held" &&
			play.parts.find((p) => p.pid === b.pid)?.team === play.offense;
		from = ours ? (from ?? b.t0) : undefined;
	}
	return from ?? t;
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

// What the camera goes after never jumping from one place to another: a
// sudden change of shot (the ball kicked, a pass away, the whistle) glided
// over AIM_GLIDE_MS, eased in and out of - no lurch at the start of a pan.
// (Its middle along and across the field, how wide, how high: more than so
// much in FRAME_MS, a jump.)
const AIM_GLIDE_MS = 600;
const AIM_JUMPS = [2, 2, 3, 1];
const FRAME_MS = 16;
export type Glide = { was: number[]; off: number[]; ms: number };
export const glideAim = (g: Glide, to: number[], ms: number): number[] => {
	const left = () => {
		const u = Math.min(1, g.ms / AIM_GLIDE_MS);
		return 1 - u * u * (3 - 2 * u);
	};
	const most = Math.max(1, ms / FRAME_MS);
	if (to.some((v, i) => Math.abs(v - (g.was[i] ?? v)) > AIM_JUMPS[i]! * most)) {
		const l = left();
		g.off = to.map((v, i) => (g.off[i] ?? 0) * l + (g.was[i] ?? v) - v);
		g.ms = 0;
	}
	g.was = to;
	g.ms += ms;
	const l = left();
	return to.map((v, i) => v + (g.off[i] ?? 0) * l);
};

// And the camera going after it on along the field leads it, the way a man
// on a camera leads a runner: by as much as it would otherwise trail behind
// him (the picture is followed easing in - see Field3D's follow - and so
// lags a man at a sprint by half a second, the room left ahead of him in
// the picture eaten up). How fast what it looks at is going along (a jump
// left out), eased over LEAD_EASE_MS; looked on ahead of it this long, and
// no further than LEAD_MOST feet. Only a man with it (`held`), not the ball
// in the air - a kick comes down to a man going back the way it came.
const LEAD_S = 0.45;
const LEAD_EASE_MS = 350;
const LEAD_MOST = 14;
export type Lead = { was?: number; v: number };
export const leadAim = (
	l: Lead,
	x: number,
	ms: number,
	held: boolean,
): number => {
	if (l.was !== undefined && ms > 0) {
		const d = x - l.was;
		const v = !held
			? 0
			: Math.abs(d) > AIM_JUMPS[0]! * Math.max(1, ms / FRAME_MS)
				? l.v
				: d / (ms / 1000);
		l.v += (v - l.v) * (1 - Math.exp(-ms / LEAD_EASE_MS));
	}
	l.was = x;
	return Math.min(LEAD_MOST, Math.max(-LEAD_MOST, l.v * LEAD_S));
};

// In close on a man - the head coach calling a timeout, the interview after
// the game: the stadium behind out of focus (see scene.ts).
export const closeIn = (m: FieldMoment): boolean => {
	const call = m.play?.timeout?.call;
	return (
		rigFor(m) !== undefined ||
		(!!call && m.t >= call.from && m.t < m.play!.timeout!.cut) ||
		callingIn(m)
	);
};
const callingIn = (m: FieldMoment): boolean => {
	const react = m.play?.coachReact;
	return (
		(!!m.play?.callIn && m.t >= m.play.callIn.t0) ||
		(!!react && m.t >= react.t0 && m.t < react.t1)
	);
};

// Whom the picture is in close on now, if anyone - the kicker through his
// routine, the quarterback at the line, the referee giving a foul, a big
// play's man - where he stands for it, and how.
export const closeUpOf = (m: FieldMoment) => {
	const kick = m.play?.closeUp;
	if (kick && m.t >= kick.t0 && m.t < kick.t1) {
		return { pid: kick.pid, at: kick.at, ...KICK_CLOSE };
	}
	const qb = m.play?.qbClose;
	if (qb && m.t >= qb.t0 && m.t < qb.t1) {
		return { pid: qb.pid, at: qb.at, shift: qb.shift, ...QB_CLOSE };
	}
	const ref = m.play?.refCall;
	if (ref && m.t >= ref.t0 && m.t < ref.t1) {
		return { pid: officialId("R"), at: ref.at, ...REF_CLOSE };
	}
	const hero = m.play?.heroClose;
	if (hero && m.t >= hero.t0 && m.t < hero.t1) {
		return { pid: hero.pid, at: hero.at, shift: hero.shift, ...HERO_CLOSE };
	}
	return undefined;
};

// The quarterback over the tablet on the sideline now, if he is.
// The man who scored being greeted on his sideline now, if he is.
export const greetOf = (m: FieldMoment) => {
	const greet = m.play?.greet;
	return greet && m.t >= greet.t0 && m.t < greet.t1 ? greet : undefined;
};

export const tabletOf = (m: FieldMoment) => {
	const tablet = m.play?.tablet;
	return tablet && m.t >= tablet.t0 && m.t < tablet.t1 ? tablet : undefined;
};

// Where the picture is from, if not from the press box or down low on the
// near side (see fromLow): over the interview after the game, down on the
// field in front of the two of them; on the quarterback over the tablet
// on the sideline, the same, and on the man who scored greeted there; in
// close on the kicker or the referee, in front of him.
export const rigFor = (m: FieldMoment): FieldRig | undefined => {
	const greet = greetOf(m);
	if (greet) {
		return {
			back: greet.at.y + GREET_CAM_FT - FIELD_W,
			high: GREET.camZ,
			slide: 1,
			upright: false,
		};
	}
	const tablet = tabletOf(m);
	if (tablet) {
		const y = (tablet.at.y + tablet.oc.y) / 2;
		return {
			back: y + TABLET_CAM_FT - FIELD_W,
			high: TABLET.camZ,
			slide: 1,
			upright: false,
		};
	}
	const close = closeUpOf(m);
	if (close) {
		return {
			back: close.at.y + close.camFt - FIELD_W,
			high: close.camZ,
			slide: 1,
			upright: false,
			...("shift" in close ? { shift: close.shift } : {}),
		};
	}
	const post = m.play?.postgame;
	const talk = post?.interview;
	if (!post || !talk || m.t < talk.t0 || m.t >= post.look) {
		return undefined;
	}
	return {
		back: (talk.at.y + talk.her.y) / 2 + INTERVIEW_CAM_FT - FIELD_W,
		high: INTERVIEW_CAM_Z,
		slide: 1,
		upright: false,
	};
};

// Whether the picture is from down low on the near side (see camera.ts
// REPLAY_RIG): over a timeout, on the head coach calling it, the huddle or
// the crowd - not on the cheerleaders; on the head coach calling a play in,
// or taking a big play.
export const fromLow = (m: FieldMoment): boolean => {
	const timeout = m.play?.timeout;
	return (
		(!!timeout &&
			m.t >= (timeout.call?.from ?? timeout.cut) &&
			!(timeout.cheer && m.t >= timeout.cheer.cut)) ||
		callingIn(m)
	);
};
