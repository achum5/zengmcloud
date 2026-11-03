import { flairOf, speedsOf, type Flair, type FieldRatings } from "./ratings.ts";
import { makeCourtRng } from "../courtRng.ts";
import {
	alignDefense,
	chooseDefenseCall,
	chooseOffenseSet,
	KICK_BLOCK,
	KICK_TEAM,
	KICKOFF_COVER,
	KICKOFF_RECEIVE,
	OFFENSE_SETS,
	ONSIDE_RECEIVE,
	PUNT_RETURN,
	PUNT_TEAM,
	type Align,
	type OffenseSet,
	type OffenseSetName,
	type Offset,
	type Role,
} from "./formations.ts";
import {
	dirFor,
	dist,
	FIELD_L,
	FIELD_W,
	HASH_FAR_Y,
	HASH_NEAR_Y,
	MID_X,
	MID_Y,
	YD,
	yardLineX,
	type Dir,
	type Pt,
	type Pt3,
	type Side,
} from "./geometry.ts";
import {
	depthCharts,
	fillSlots,
	type DepthChart,
	type Group,
	type RosterMan,
} from "./personnel.ts";
import { moveKind, type Move } from "./poses.ts";
import { actOut, HERO_CAM_FT, type Acted } from "./action.ts";
import {
	anyBall,
	CALL_FOR_MS,
	officialId,
	posOf,
	REF_CAM_FT,
	stageCrew,
	type CrewInput,
	type CrewPart,
	type FlagFlight,
} from "./crew.ts";
import { actFouls, foulsOf } from "./fouls.ts";
import { stageChains, type ChainSpots } from "./chains.ts";
import { holderAt, replayFor, type FieldReplay } from "./replays.ts";
import { crewClear, stageTimeout } from "./timeout.ts";
import { cheerShotFor, type CheerShot } from "./cheerleaders.ts";
import { crowdShotFor, type CrowdShot } from "./crowd.ts";
import type { Shout } from "./stadium.ts";
import { measureTimes, stageMeasure } from "./measure.ts";
import { HURT_MOST, MEDICS, stageHurt } from "./hurt.ts";
import {
	crewOff,
	stageChangeEnds,
	stageHalftime,
	stagePostgame,
	type Interview,
	type Clinch,
	type Stakes,
} from "./postgame.ts";
import { SHOWER, SHOWER_MARGIN, type Shower } from "./shower.ts";
import { stageTv } from "./tv.ts";
import { stageToss, tossCrew } from "./toss.ts";
import { runOutCrew, stageRunOut, type RunOut } from "./tunnel.ts";
import { coordinatorAt, headCoachAt, sidelineOf } from "./sideline.ts";

// THE GAME STAGED: every snap of the play-by-play, in feet and milliseconds -
// who is on the field for it, where each man lines up, the ball, and the
// moment each of its lines happens - for the 3D field to act out (see
// evaluate.ts), the way the 3D court's director stages a basketball game (see
// court3d/director.ts).
//
// The sim plays football a snap at a time and says what each one came to:
// the down and the spot, the play called, how many yards, who carried it and
// who brought him down. Everything it leaves out is made up here, the same
// way every time for the same game (seeded by it, see courtRng.ts): which
// formation, which hash, where each man stands. A play runs from one "clock"
// line (the down, the distance, the spot) to the next: the offense up out of
// its huddle and set, the defense aligned on it, the snap, the play, its
// result.
//
// Times are milliseconds at 1x.

export type RawEvent = { type: string; [key: string]: any };

// One thing a man did in a play, as the sim counts it: the stat, the man,
// his team (raw: 0 the home team), and where in the events it is - just
// before the line it goes with (read off a two-point try's line, at it).
export type StatLine = { s: string; pid: number; t: number; i: number };

export type PlayKind =
	| "kickoff"
	| "onside"
	| "punt"
	| "fieldGoal"
	| "extraPoint"
	| "pass"
	| "run"
	| "kneel"
	// No snap: a foul before it, the clock run out.
	| "none";

// A stretch of a man's play: from `from` to `to` over t0..t1 doing `move` -
// standing in it, if he goes nowhere - facing `yaw`, or (unset) the way he
// goes. A man's legs follow one after another, each starting where the last
// left him.
export type Leg = {
	t0: number;
	t1: number;
	from: Pt;
	to: Pt;
	move: Move;
	yaw?: number;
	// Turning as he goes, this far round over the leg (radians, to his left
	// positive): a spin move.
	turn?: number;
	mirrored?: boolean;
	// The ball with him, and how he has it (see BallHold).
	ball?: BallHold;
	// An act cut in two: how far through it this leg starts and ends (all
	// of it, unset).
	span?: [number, number];
	// His arms in an act of their own over whatever the rest of him is
	// doing - reaching for a ball he catches in stride - and how far through
	// it this leg takes them.
	arms?: Move;
	armsSpan?: [number, number];
	// His helmet off, in his hand.
	bare?: true;
	// Talking as he does it (whatever it is): the referee giving a call over
	// his microphone.
	talk?: true;
};

// How a man has the ball: under his hand on the turf over it (a center or
// a long snapper), tucked away under his right arm (or his left) - his
// other hand over it, too, braced for a hit - in both hands, cocked back in
// his throwing hand.
export type BallHold =
	| "snap"
	| "carry"
	| "carryL"
	| "cover"
	| "coverL"
	| "hands"
	| "throw";

// One man in one play: his slot in it and what he does.
export type Part = {
	pid: number;
	team: Side;
	role: Role;
	legs: Leg[];
};

// Where the ball is, from t0 on: spotted (or stood on end: on its tee, held
// for a kick), with a man, or in
// the air - from one point to another by t1, `peak` feet over the straight
// line between them halfway, spiraling (thrown) or end over end.
export type BallSeg =
	| { kind: "spot"; t0: number; at: Pt3; stood?: "tee" | "hold" }
	| { kind: "held"; t0: number; pid: number }
	| {
			kind: "fly";
			t0: number;
			t1: number;
			from: Pt3;
			to: Pt3;
			peak: number;
			spiral?: boolean;
	  };

export type Play = {
	// The play's "clock" line.
	i: number;
	kind: PlayKind;
	quarter: number;
	// The side with the ball - kicking it off, for a kickoff - and the way
	// it goes.
	offense: Side;
	dir: Dir;
	// The line of scrimmage (the kick's spot, for a kickoff), and where the
	// ball sits across the field.
	losX: number;
	ballY: number;
	// The down and distance, as the score bug has it ("3rd & 7", "PAT").
	situation: string;
	// Up out of the huddle from t0; everyone set by `set`; the snap (or the
	// kicker away on his run-up); on to the next play at t1. And from when
	// the camera goes with the ball: the snap, or the kick.
	t0: number;
	set: number;
	snap: number;
	t1: number;
	follow: number;
	// The formation the offense is in, for a play from scrimmage.
	formation?: OffenseSetName;
	// The two-minute drill: no huddle.
	hurry?: boolean;
	parts: Part[];
	// The officials (see crew.ts), and the flags they throw.
	crew: CrewPart[];
	flags: FlagFlight[];
	// The bean bags the officials toss down, to mark a spot (see crew.ts).
	bags?: FlagFlight[];
	// The referee's calls of the fouls on it that stand: when, and against
	// which side.
	calls?: { t: number; side: Side }[];
	// The chain crew (see chains.ts), and the down on their boxes.
	chains: CrewPart[];
	down: number;
	// The TV cameramen on the sidelines (see tv.ts).
	tv: CrewPart[];
	// The trainers out to a man hurt on it (see hurt.ts) - and the man, the
	// camera on him from when he is down.
	medics?: CrewPart[];
	hurt?: { pid: number; from: number };
	// The coin toss (see toss.ts): the captains out to the middle and back;
	// where, when the coin goes up, and when the picture cuts to the kick.
	toss?: { men: CrewPart[]; at: Pt; flip: number; cut: number };
	// The home team out of its tunnel before the opening kickoff, in place of
	// the coin toss (see tunnel.ts).
	runOut?: RunOut;
	// The game over (see postgame.ts): the head coaches out to shake hands;
	// from when it is, when they do, and where. (A timeout: the head coach
	// of the side that called it out at the sideline.)
	coaches?: CrewPart[];
	// A timeout (see timeout.ts): when the picture cuts to the side that
	// called it, and where it looks - and, now and then, from when on to the
	// cheerleaders at their routine (see cheerleaders.ts), or the crowd (see
	// crowd.ts).
	// (And first, from when, on the head coach calling it - see aim.ts.)
	timeout?: {
		cut: number;
		at: Pt;
		call?: { from: number; pid: number };
		cheer?: CheerShot;
		crowd?: CrowdShot;
	};
	// A measurement (see measure.ts): when the picture cuts to the chain out
	// on the field, and where it looks.
	measure?: { cut: number; at: Pt };
	// In close on the kicker through his routine before the snap (see
	// kickerRoutine): from when, until when, and where he sets.
	closeUp?: { t0: number; t1: number; pid: number; at: Pt };
	// The home side's quarterback off the field over the tablet with his
	// coordinator, as the visitors' next drive starts (see TABLET_SHARE):
	// from when, until when; whose side, him, where he stands and which way
	// he faces; and the coordinator the same.
	tablet?: {
		t0: number;
		t1: number;
		side: Side;
		pid: number;
		at: Pt;
		yaw: number;
		oc: Pt;
		ocYaw: number;
	};
	// In close on the quarterback at the line before the snap (see
	// QB_CLOSE): from when, until when, where he sets, and how far along the
	// field from him, the way he faces, the camera is.
	qbClose?: { t0: number; t1: number; pid: number; at: Pt; shift: number };
	// In close on the referee as he gives the fouls on it (see crew.ts): from
	// when, until when, and where he stands for it.
	refCall?: { t0: number; t1: number; at: Pt };
	// A big play's moment over, the picture on the home side's head coach
	// across the field taking it (see COACH_REACT): from when, until when,
	// where he stands, and whether it went his side's way.
	coachReact?: { t0: number; t1: number; side: Side; at: Pt; glad: boolean };
	// Third down for the home side next: the picture on its head coach
	// across the field, calling it in, from when to the next play - and
	// where he stands for it (see sideline.ts, aim.ts).
	callIn?: { t0: number; t1: number; side: Side; at: Pt };
	// In close on the man a big play's moment after it is about - the man
	// who made the sack, the scorer, the man pointing out a first down after
	// a long gain - on his own in it, before the rest get to him (see
	// heroClose): from when, until when, and where he stands.
	// (And the camera this far along the field from him - see aim.ts.)
	heroClose?: { t0: number; t1: number; pid: number; at: Pt; shift: number };
	// Between quarters, and at the half: when the picture cuts to the whole
	// stadium (see aim.ts).
	between?: number;
	postgame?: {
		from: number;
		shake: number;
		at: Pt;
		winner?: Side;
		// Won on it: all over the man who did it, there, until then.
		mob?: { at: Pt; until: number };
		// The winners' head coach under the cooler (see shower.ts).
		shower?: Shower;
		// The last look, the hands all shaken: when the picture cuts to the
		// whole stadium (see aim.ts).
		look: number;
		// A playoff round won by it - the championship, it may be.
		clinch?: Clinch;
		// The winners' star of the game talking to the sideline reporter
		// (see postgame.ts).
		interview?: Interview;
	};
	ball: BallSeg[];
	// A big play's moment after it (see action.ts celebrate): from when, till
	// when, and where - the camera in on it.
	party?: { from: number; until: number; at: Pt };
	// A big play, shown again after it (see replays.ts).
	replay?: FieldReplay;
	// The crowd's moment in it: when, for which side, and how big (see
	// crowd.ts).
	cheer?: Cheer;
	// From this play on: the game in crunch time (half, or all of it), and
	// how full the stands are (all of them, unset) - see crowd.ts.
	tense?: number;
	seats?: number;
	// The ball dead; and, for a down from scrimmage, the line to gain (the
	// broadcast's lines on the field: see marks.ts).
	whistle: number;
	gainX?: number;
};

// (Held up this long, if not as long as a play's - and what for, if the
// boards make a shout of it: see crowd.ts boardAt.)
export type Cheer = {
	t: number;
	side: Side;
	big: boolean;
	hold?: number;
	what?: Shout;
};

export type Beat = {
	i: number;
	type: string;
	preStart: number;
	actionStart: number;
	end: number;
};

export type FieldTimeline = {
	plays: Play[];
	beats: Beat[];
	// Moments the picture cuts: everyone may be somewhere else just after.
	cuts: number[];
	end: number;
};

export type FieldInput = {
	events: RawEvent[];
	players: RosterMan[];
	gid: number | undefined;
	// Quarters in regulation, for which end each team defends in overtime.
	numPeriods?: number;
	// A playoff game's stakes, as they stood going into it (the 3D court's:
	// see court3d/director.ts) - celebrated accordingly at the end (see
	// postgame.ts).
	stakes?: Stakes;
};

// ---- the lines -------------------------------------------------------------

// The events that are not a line of the play-by-play (see
// processLiveGameEvents.football): stats, the timeouts left, the clock
// event that only sets up a try after a touchdown, a quarterback keeping it
// himself (no handoff to speak of).
const NOT_A_LINE = new Set(["stat", "timeouts", "init", "removeLastScore"]);
export const isLineItem = (e: RawEvent | undefined): boolean =>
	!!e &&
	typeof e.type === "string" &&
	!NOT_A_LINE.has(e.type) &&
	!(e.type === "clock" && e.awaitingAfterTouchdown) &&
	!(e.type === "handoff" && !(Array.isArray(e.names) && e.names.length > 1));

// What starts the action of a play, by the event that calls it.
const PLAY_KIND: Record<string, PlayKind> = {
	kickoff: "kickoff",
	onsideKick: "onside",
	puntTeam: "punt",
	punt: "punt",
	fieldGoalAttempt: "fieldGoal",
	extraPointAttempt: "extraPoint",
	dropback: "pass",
	handoff: "run",
	run: "run",
	kneel: "kneel",
	// (A field goal shootout's kicks.)
	shootoutShot: "fieldGoal",
};

// Said before the snap: the unit coming on, the decision made.
const BEFORE_SNAP = new Set([
	"goingForItOn4th",
	"puntTeam",
	"fieldGoalAttempt",
	"extraPointAttempt",
	"twoPointConversion",
	"onsideKick",
]);
// Said as the ball is snapped (or kicked), and how long after.
const AT_SNAP: Record<string, number> = {
	dropback: 350,
	handoff: 600,
	kneel: 900,
};
// Besides his getting out of bounds, what stops the clock after a run or a
// catch. (The ball over on downs, too: a change of hands.)
const STOPS_CLOCK = new Set<string | undefined>([
	"timeout",
	"twoMinuteWarning",
	"quarter",
	"fumble",
	"interception",
	"injury",
	"turnoverOnDowns",
]);
// What happened, said when it does.
const RESULTS = new Set([
	"passComplete",
	"passIncomplete",
	"run",
	"sack",
	"interception",
	"interceptionReturn",
	"fumble",
	"fumbleRecovery",
	"kickoff",
	"kickoffReturn",
	"punt",
	"puntReturn",
	"fieldGoal",
	"extraPoint",
	"onsideKickRecovery",
	"twoPointConversionFailed",
	"turnoverOnDowns",
	"shootoutShot",
]);
// The star of the game, for the interview after it (see starOf): what each
// stat of his counts for - so much a yard, a touchdown, a takeaway (a field
// goal made, STAR_FG) - and how much it takes all told.
const STAR_WORTH: Record<string, number> = {
	pssYds: 1 / 25,
	pssTD: 4,
	pssInt: -2,
	rusYds: 1 / 10,
	rusTD: 6,
	recYds: 1 / 10,
	recTD: 6,
	krYds: 1 / 25,
	prYds: 1 / 25,
	fmbLost: -2,
	defInt: 5,
	defSk: 3,
	defSft: 4,
	defFmbRec: 3,
	defFmbFrc: 2,
	defTckLoss: 1,
	defTckSolo: 0.5,
	defTckAst: 0.25,
};
const STAR_FG = 3;
const STAR_LEAST = 10;
// A timeout: the picture on to the side that called it no sooner than this
// long after the ball is dead (and the moment after), after the referee's
// signal, and after a replay is cued.
const TIMEOUT_AFTER = 1800;
const TIMEOUT_SAID = 1700;
const TIMEOUT_AFTER_REPLAY = 400;
// Called by the head coach across the field: the picture on him calling it
// no sooner than this long after the ball is dead (see timeout.ts).
const CALL_AFTER = 1100;
// Between quarters (the 3D court's look round the building over a break:
// see court3d/director.ts shots), as the teams change ends: the picture on
// the whole stadium this long after the play is over, for this long. At
// the half, this long after it - once the teams are on their way off (see
// postgame.ts stageHalftime).
const BETWEEN_AFTER = 1500;
const HALF_BETWEEN_AFTER = 3400;
const BETWEEN_MS = 4200;
// A field goal shown again (see replays.ts) if it ties the game, or puts
// them ahead, in the last this many minutes of it.
const LATE_KICK = 5;
// Third down for the home side next (its head coach across the field from
// the camera, the visitors' under it), this share of the time: the picture
// on him calling it in, from this long after the play is over - if that
// leaves this long of it - to the next play. Not in the two-minute drill
// (no huddle), nor with anything else to show after the play.
const CALL_IN_SHARE = 0.5;
// A big play's moment over (its party: see action.ts), the picture on the
// home side's head coach taking it - a fist pumped and clapping it, his
// side's; hands on his head, the other side's - this share of the time,
// this long after the moment (any close-up in it done), for this long; the
// replay, if there is one, straight after - and the play over no sooner
// than this long after him.
const COACH_REACT_GLAD = 0.45;
const COACH_REACT_SAD = 0.3;
const COACH_REACT_AFTER = 500;
const COACH_REACT_MS = 1800;
const COACH_REACT_TAIL = 900;

// Each man stays as his last leg left him until t1.
const holdTill = (parts: { legs: Leg[] }[], t1: number) => {
	for (const part of parts) {
		const l = part.legs.at(-1);
		if (!l || l.t1 >= t1) {
			continue;
		}
		if (
			l.from.x === l.to.x &&
			l.from.y === l.to.y &&
			moveKind(l.move) === "still" &&
			!l.arms
		) {
			l.t1 = t1;
		} else {
			part.legs.push({
				t0: l.t1,
				t1,
				from: l.to,
				to: l.to,
				move: moveKind(l.move) === "still" ? l.move : "stand",
				...(l.yaw !== undefined ? { yaw: l.yaw } : {}),
				...(l.ball ? { ball: l.ball } : {}),
			});
		}
	}
};
const CALL_IN_AFTER = 1300;
const CALL_IN_LEAST = 1500;
const HURRY_MINUTES = 2;
// After the play: how long after the line before each is said.
const AFTER_PLAY: Record<string, number> = {
	penalty: 1600,
	penaltyCount: 1600,
	injury: 1500,
	timeout: 1400,
	twoMinuteWarning: 1400,
	quarter: 1800,
	overtime: 1800,
	gameOver: 1800,
	turnoverOnDowns: 1000,
	twoPointConversionFailed: 900,
};
// Lines that come one after another, no closer than this.
const LINE_GAP = 450;
// On to the next play this long after the last line of one.
const TAIL = 1500;

// ---- timing a snap ---------------------------------------------------------

// The clock line, as the offense breaks the huddle.
const CLOCK_AT = 200;
// Up out of the huddle (or on from the sideline) this long in - the men
// going one after another over a moment, the clap just before.
const BREAK_AT = 640;
const BREAK_SPREAD = 160;
const DEF_BREAK_LATE = 150;
// The clap as it breaks, this long in (and up and away from it).
const CLAP_AT = 160;
// How far back to the huddle (feet) a man is when the picture takes up the
// play - from no farther than this off his spot, at an easy jog, or at a
// walk for a short way.
const HUDDLE_BACK = 27;
const DEF_HUDDLE_BACK = 30;
const WALK_UP_MAX = 26;
const JOG_FPS = 9;
const WALK_FPS = 4.6;
const WALK_ONLY = 10;
// Out there a long drive: from how many snaps in a row the defense has been
// on the field (since the last break) it starts to tell, and over how many
// more it tells all the way (see action.ts regroup).
const WINDED_FROM = 5;
const WINDED_SPAN = 6;
// No huddle: the men out wide (these) back up from this far downfield
// (feet, at the least, and as much as this more), the rest up from this far
// behind their spots, the defense from about this far off theirs - each as
// much as this far across - going this long after the picture takes up the
// play at most, at these paces (feet a second).
const HURRY_DOWNFIELD = [10, 25] as const;
const HURRY_BEHIND = [6, 14] as const;
const HURRY_DEFENSE = 14;
const HURRY_ACROSS = 6;
const HURRY_SPREAD = 350;
const HURRY_FPS = [10, 4] as const;
// (No quicker than gets him going and pulled up again over the way he
// has: as if at this many ft/s a second, both ways - half the pace he
// would be up to halfway.)
const HURRY_EASE = 16;
const HURRY_OUT = new Set<Role>(["X", "Z", "SL", "SL2", "TE", "TE2"]);
// Into his stance at the end of it: the last SET_IN_FT of the way at a
// walk (SET_IN_FPS), the rest of it that much quicker (no more than
// SET_IN_MOST times) - not pulled up dead out of a jog.
const SET_IN_FT = 3;
const SET_IN_FPS = 4;
const SET_IN_MOST = 1.35;
const SET_IN_LEAST = 1;
// Along a leg off `v0` ft/s and into the end of it at `w`: up to the pace
// it takes at GOING_ACCEL ft/s/s, on at it and down to `w` at the last, in
// steps GOING_STEP_MS long - there when he would have been, that much
// quicker between; not off at it, nor pulled up out of it, in a stride.
// (Just the leg, if it cannot be done.)
const GOING_ACCEL = 14;
const GOING_STEP_MS = 100;
const GOING_LEAST_FPS = 3;
// (What that costs him, ms, at `fps` the rest of it: from still, and down
// to a walk into his stance - see intoStance.)
const goingMs = (fps: number) =>
	((fps + Math.max(0, fps - SET_IN_FPS) ** 2 / fps) / (2 * GOING_ACCEL)) * 1000;
const paced = (l: Leg, v0: number, w: number): Leg[] => {
	const A = GOING_ACCEL;
	const T = (l.t1 - l.t0) / 1000;
	const D = Math.hypot(l.to.x - l.from.x, l.to.y - l.from.y);
	const b = T + (v0 + w) / A;
	const disc = b * b - (4 / A) * (D + (v0 * v0 + w * w) / (2 * A));
	if (!(T > 0 && D > 0 && disc >= 0)) {
		return [l];
	}
	const vc = (A * (b - Math.sqrt(disc))) / 2;
	const tu = (vc - v0) / A;
	const td = (vc - w) / A;
	const tc = T - tu - td;
	if (vc < Math.max(v0, w, GOING_LEAST_FPS) || tc < -1e-6) {
		return [l];
	}
	const sAt = (t: number) => {
		if (t <= tu) {
			return v0 * t + (A * t * t) / 2;
		}
		const su = v0 * tu + (A * tu * tu) / 2;
		if (t <= tu + tc) {
			return su + vc * (t - tu);
		}
		const u = Math.min(td, t - tu - tc);
		return su + vc * tc + vc * u - (A * u * u) / 2;
	};
	const marks: number[] = [];
	const nu = Math.max(1, Math.round((tu * 1000) / GOING_STEP_MS));
	for (let i = 1; i <= nu; i++) {
		marks.push((tu * i) / nu);
	}
	if (tc * 1000 >= 1) {
		marks.push(tu + tc);
	}
	const nd =
		td * 1000 >= 1 ? Math.max(1, Math.round((td * 1000) / GOING_STEP_MS)) : 0;
	for (let i = 1; i <= nd; i++) {
		marks.push(tu + tc + (td * i) / nd);
	}
	const legs: Leg[] = [];
	let from = l.from;
	let prev = 0;
	marks.forEach((t, k) => {
		const last = k === marks.length - 1;
		const u = sAt(t) / D;
		const to = last
			? l.to
			: {
					x: l.from.x + (l.to.x - l.from.x) * u,
					y: l.from.y + (l.to.y - l.from.y) * u,
				};
		legs.push({
			...l,
			t0: l.t0 + prev * 1000,
			t1: last ? l.t1 : l.t0 + t * 1000,
			from,
			to,
		});
		from = to;
		prev = t;
	});
	return legs;
};

// (Each man's way up to his spot - see intoStance - paced once the rest of
// what he does before the snap is settled: see paceWalkUps.)
const walkingUp = new WeakSet<Leg>();
const paceWalkUps = (parts: Part[]) => {
	for (const p of parts) {
		if (!p.legs.some((l) => walkingUp.has(l))) {
			continue;
		}
		p.legs = p.legs.flatMap((l, k) => {
			const before = p.legs[k - 1];
			const next = p.legs[k + 1];
			if (
				!walkingUp.has(l) ||
				!before ||
				before.from.x !== before.to.x ||
				before.from.y !== before.to.y
			) {
				return [l];
			}
			const paceOf = (x: Leg) =>
				Math.hypot(x.to.x - x.from.x, x.to.y - x.from.y) /
				Math.max(0.001, (x.t1 - x.t0) / 1000);
			const into =
				next &&
				next.move === "walk" &&
				next.from.x === l.to.x &&
				next.from.y === l.to.y
					? next
					: l;
			return paced(l, 0, paceOf(into));
		});
	}
};

const intoStance = (
	from: Pt,
	to: Pt,
	t0: number,
	t1: number,
	move: Move,
	slow: Move = "walk",
): Leg[] => {
	const d = Math.hypot(to.x - from.x, to.y - from.y);
	const fps = d / ((t1 - t0) / 1000);
	// (As far at a walk as leaves the rest no more than SET_IN_MOST times
	// as quick.)
	const k = (SET_IN_MOST * fps) / SET_IN_FPS - 1;
	const s = Math.min(SET_IN_FT, d / 3, k > 0 ? (d * (SET_IN_MOST - 1)) / k : 0);
	const main = t1 - t0 - (s / SET_IN_FPS) * 1000;
	if (fps < SET_IN_FPS * 1.5 || s < SET_IN_LEAST || main <= 0) {
		const l: Leg = { t0, t1, from, to, move };
		walkingUp.add(l);
		return [l];
	}
	const u = (d - s) / d;
	const mid = {
		x: from.x + (to.x - from.x) * u,
		y: from.y + (to.y - from.y) * u,
	};
	// (The rest of it in a gait as quick as it goes: no jog at a run's
	// pace.)
	const pace = (d - s) / (main / 1000);
	const gait =
		move === "walk" && pace >= 6.5
			? "jog"
			: move === "jog" && pace >= 12.5
				? "run"
				: move;
	const l: Leg = { t0, t1: t0 + main, from, to: mid, move: gait };
	walkingUp.add(l);
	return [l, { t0: t0 + main, t1, from: mid, to, move: slow }];
};
// Out of a ring to their spots, all at much the same time and pace: each
// man in the place round it - swapped about - that, squared, has the least
// way to go for all of them, so no two ways out of it cross at the same
// time (not, round it in order, the back of it through the front).
const ringOut = (slots: Pt[], to: Pt[]) => {
	const far = (s: Pt, p: Pt) => (s.x - p.x) ** 2 + (s.y - p.y) ** 2;
	for (let pass = 0; pass < slots.length; pass++) {
		let swapped = false;
		for (let i = 0; i < slots.length; i++) {
			for (let j = i + 1; j < slots.length; j++) {
				const a = slots[i]!;
				const b = slots[j]!;
				if (
					far(b, to[i]!) + far(a, to[j]!) <
					far(a, to[i]!) + far(b, to[j]!) - 1e-6
				) {
					slots[i] = b;
					slots[j] = a;
					swapped = true;
				}
			}
		}
		if (!swapped) {
			break;
		}
	}
};
// The home side's quarterback, off the field after its drive, over the
// tablet with his offensive coordinator (coach 1) on the sideline: the
// picture on the two of them across the field as the visitors' drive
// starts, this share of the time - from the first of it until this long
// into it, if that leaves this long of it before the snap. Him this far
// along from the coordinator, away from midfield, turned this share of the
// way to the field from facing him (it in his hands), the coordinator this
// share from facing him; the camera on
// the field this far in front of them (see aim.ts) - not with anybody
// within this much of the way to them, or stood in among them.
const TABLET_SHARE = 0.6;
const TABLET_MS = 3200;
const TABLET_WIDE = 1800;
const TABLET_APART = 2.6;
const TABLET_OPEN = 0.6;
const TABLET_OC_OPEN = 0.25;
export const TABLET_CAM_FT = 28;
const TABLET_CLEAR = 4.5;
// In close on the quarterback in the gun before the snap, from t0 to t1:
// where he is set, and whether anybody is ever between him and the camera
// on him (see QB_CAM_FT) over it, or just behind him in the picture - not
// if so.
const qbCloseFor = (
	parts: Part[],
	offense: Side,
	dir: Dir,
	t0: number,
	t1: number,
): Play["qbClose"] => {
	const qb = parts.find((p) => p.team === offense && p.role === "QB");
	const set = qb?.legs.at(-1);
	if (!qb || !set || set.move !== "shotgun") {
		return undefined;
	}
	const at = set.to;
	const between = (q: Pt) => {
		const d = q.y - at.y;
		const x = at.x + (dir * QB_CAM_ALONG * d) / QB_CAM_FT;
		return (
			Math.abs(d) > 1.5 &&
			d > -QB_BEHIND &&
			d < QB_CAM_FT &&
			Math.abs(q.x - x) < QB_CLEAR
		);
	};
	const blocked = parts.some(
		(p) =>
			p !== qb &&
			p.legs.some((l, j) => {
				if (l.t0 > t1 || (l.t1 < t0 && j < p.legs.length - 1)) {
					return false;
				}
				const n = Math.ceil(dist(l.from, l.to) / 1.5);
				for (let k = 0; k <= n; k++) {
					const u = n === 0 ? 0 : k / n;
					if (
						between({
							x: l.from.x + (l.to.x - l.from.x) * u,
							y: l.from.y + (l.to.y - l.from.y) * u,
						})
					) {
						return true;
					}
				}
				return false;
			}),
	);
	return blocked
		? undefined
		: { t0, t1, pid: qb.pid, at, shift: dir * QB_CAM_ALONG };
};
// Set this long after the last man is, before the snap; and the cadence
// from there (shortest, longest).
const SET_HOLD = 450;
const CADENCE = [700, 1500] as const;
// The ball his once the center is down over it and square to it, his hand
// on it (see evaluate.ts blendOf, TURN_MS) - not as he gets there.
const HAND_ON_BALL_MS = 500;
// The quarterback's read of the defense (see cadence): under center, down
// under him this long before the snap, getting there over GET_UNDER_MS,
// stood reading it for at least SURVEY_MIN first; in the gun, from this
// long before it (at least GUN_CALL_MIN). Pointing out a man this often -
// or, where the game has ratings, from POINT_LOW to POINT_LOW + POINT_SPAN
// by how well he reads it (Flair.vision).
const UNDER_AT = 650;
const GET_UNDER_MS = 320;
const SURVEY_BACK = 1.6;
const SURVEY_MIN = 600;
const GUN_CALL_MS = 1500;
const GUN_CALL_MIN = 900;
const POINT = 0.4;
const POINT_LOW = 0.15;
const POINT_SPAN = 0.6;
// The punter's hands out for the snap this long before it, out over this
// long (see punterSet).
const PUNT_HANDS_MS = 1300;
const PUNT_HANDS_IN = 300;
// The kicker's routine (see kickerRoutine): up to just behind the spot the
// ball goes down on (feet), eyes on the posts this long (ms, and up to this
// much more); his steps back off it and over, at this pace (feet a second),
// a breath between; set this long before the snap. In close on him through
// it (see aim.ts) on this share of field goals - of extra points - from
// just before he gets to the spot until just after he is set.
const ROUTINE_BEHIND = 2;
const ROUTINE_LOOK = 700;
const ROUTINE_LOOK_MORE = 500;
const ROUTINE_FPS = 3.2;
const ROUTINE_PAUSE = 250;
const ROUTINE_SET_MS = 1100;
const KICK_CLOSE = 0.75;
const KICK_CLOSE_XP = 0.25;
const KICK_CLOSE_BEFORE = 400;
const KICK_CLOSE_AFTER = 350;
// In close on the quarterback in the gun, reading the defense, on this
// share of third and fourth downs and plays in a tight finish - not one his head coach was just shown
// calling in, not in a hurry: from this long before the offense is set,
// until this long before the snap - the snap put back for it to be at
// least this long, and no longer than this. From this far toward the near side of him (see aim.ts),
// this far along toward the defense - nobody stood within this much of
// the way between, or of it on past him this far.
const QB_CLOSE = 0.8;
const QB_CLOSE_BEFORE = 1200;
const QB_CLOSE_LEAST = 2600;
const QB_CLOSE_MOST = 3600;
const QB_WIDE_BEFORE = 1400;
export const QB_CAM_FT = 34;
const QB_CAM_ALONG = 7;
const QB_CLEAR = 2.5;
const QB_BEHIND = 8;
// Pre-snap motion: on this share of pass plays, a man off the line (the
// flanker, a slot) this far across (feet) at this pace - never in closer
// to the ball than this, nor nearer a sideline. In man coverage, his man
// goes with him.
const MOTION = 0.38;
const MOTION_ROLES = new Set<Role>(["Z", "SL", "SL2"]);
const MOTION_FT = [12, 24] as const;
const MOTION_FPS = [9, 13] as const;
const MOTION_INSIDE = 19;
const MOTION_EDGE = 8;
const MAN_FOLLOWS = 0.5;
// (Shuffled across with him no quicker than this - ft/s.)
const FOLLOW_SLIDE_FPS = 8;
// Still this far (ms) from his spot when the ball is snapped - on his way
// out into his route without a stop.
const IN_MOTION = 200;
// The line (see lineDown): up over it, hands on his thighs, till it goes
// down together this long before the snap (soonest, latest) - each man
// within LINE_EACH_MS of the rest, the men across from them down after
// them by so much (least, most). A man up there less than LINE_UP_LEAST:
// down in it as he gets there.
const LINE_DOWN_MS = [950, 1300] as const;
const LINE_EACH_MS = 160;
const LINE_DL_MS = [80, 380] as const;
const LINE_UP_LEAST = 350;
// (The backs and the men split out down into theirs with it, up to this
// much after - stood up, or hands on hips, till then; the corners after
// it, as the line across from them is, up on their toes till then.)
const LINE_SKILL_MS = 300;
const UP_FIRST = new Map<Move, Move[]>([
	["threePoint", ["lineReady"]],
	["dlThree", ["lineReady"]],
	["backStance", ["stand", "onHips"]],
	["receiverStance", ["stand", "onHips"]],
	["cornerStance", ["safetyReady"]],
]);
// The defense waiting on the snap (see defenseSet): the call, this often,
// over this long (at least DEF_CALL_MIN) and done this long before the
// snap; a linebacker over a step this often, this far (feet); one up into
// a gap this often, this deep off the ball (feet), back out of it from this
// long before the snap, up at this pace (feet a second); of two safeties
// deep, one down late this often, to this deep (feet), over this long.
const DEF_CALL = 0.75;
const DEF_CALL_MS = 1500;
const DEF_CALL_MIN = 800;
const DEF_CALL_BEFORE = 350;
const SHUFFLE = 0.35;
const SHUFFLE_FT = [1.5, 4] as const;
const MUG = 0.14;
const MUG_D = 6.5;
const MUG_BACK = 500;
const MUG_FPS = 7;
// (Backing out of it at no more than this, feet a second, set again this
// long before the snap - as a safety down late is.)
const MUG_BACK_FPS = 9;
const MUG_SET = 250;
const CREEP = 0.3;
const CREEP_D = 21;
const CREEP_MS = 1600;
// (At no more than this, feet a second.)
const CREEP_FPS = 9;
const LBS = new Set<Role>(["SAM", "MIKE", "WILL", "BUCK"]);
const DL_ROLES = new Set<Role>(["DE", "DT", "NT"]);
// A kickoff: everyone lined up, the kicker off on his run-up this long in -
// his hand up from so long before it, and down again so long before.
const KICK_AT = 2600;
const KICK_HAND_UP = 1900;
const KICK_HAND_DOWN = 350;
// The coin toss: this many captains a side, from these groups in turn. The
// men for the kick waiting for it on their sidelines in two rows - this
// far off the field, rows this far apart, men this far apart along them -
// this far along from midfield.
const SHOWER_GROUPS: Group[] = ["OL", "DL", "LB", "TE", "RB", "S", "WR", "CB"];
const CAPTAINS = 3;
const CAPTAIN_GROUPS: Group[] = ["QB", "OL", "DL", "LB", "S", "WR", "RB"];
const WAIT_OFF = 2;
const WAIT_ROW = 2.6;
const WAIT_COL = 2.9;
const WAIT_ALONG = 32;
const waitingAt = (team: Side, k: number, r: () => number): Pt => {
	const line = sidelineOf(team);
	const lean = line === 0 ? 1 : -1;
	const off = WAIT_OFF + (k % 2) * WAIT_ROW + r() * 0.6;
	return {
		x:
			MID_X -
			lean * WAIT_ALONG +
			(Math.floor(k / 2) - 2.5 + (k % 2) * 0.5) * WAIT_COL +
			(r() - 0.5) * 0.8,
		y: line === 0 ? -off : FIELD_W + off,
	};
};
// The plays acted out from the snap (see action.ts), and how long after
// the whistle the picture stays on one - men getting up, walking off.
const ACTED = new Set<PlayKind>([
	"pass",
	"run",
	"kneel",
	"kickoff",
	"punt",
	"fieldGoal",
	"extraPoint",
	"onside",
]);
const AFTER_WHISTLE = 3200;
// (Longer after a score: the scorer's moment, his teammates round him.)
const AFTER_SCORE = 4800;
// After the moment after a big play (see action.ts celebrate), this long
// on; and at the most this long after the whistle.
const CELEBRATE_AFTER = 1300;
const CELEBRATE_MOST = 7500;
// The referee's call of a foul, from its line on (see crew.ts); a first
// down signaled this long after the whistle - after the clock stopped is,
// for a man out of bounds.
const CALL_MS = 2900;
// The picture in close on the referee as he gives it (see aim.ts): from
// this long before his signal - stopped and faced to the press box by then
// - to this long after he is done (or on to the next play, if that is
// within this long of it); not for less than this long (cut short by the
// picture going to a timeout or between quarters), nor with the camera in
// front of him off the field.
const REF_CLOSE_BEFORE = 300;
const REF_CLOSE_AFTER = 250;
const REF_CLOSE_ON = 700;
const REF_CLOSE_LEAST = 1500;
// (Not over the moment after a big play - until this long after it, as
// aim.ts has it.)
const REF_AFTER_PARTY = 600;
// (In the way of it: anyone between that camera and him - nearer it than
// this past him, inside the picture, this far either side of him at his
// distance - looked for this often, ms.)
const REF_WAY_NEAR = 0.5;
const REF_SEEN = 7;
const REF_WAY_STEP = 50;
// In close on a big play's man (see aim.ts): this often, from this long
// into his moment, for this long at most and no less than this - and no
// longer than he stays this near where he started it (feet), nobody
// between the camera and him (this near the line to him, feet) - put off
// this long at most for somebody who is.
const HERO_CLOSE_SHARE = 0.7;
const HERO_CLOSE_AFTER = 150;
const HERO_CLOSE_MOST = 2600;
const HERO_CLOSE_LEAST = 1300;
const HERO_DRIFT = 6;
const HERO_BLOCK = 2.5;
const HERO_LATE = 800;
// (The camera straight in front of him, or this far along the field to
// one side.)
const HERO_SHIFTS = [0, -7, 7, -12, 12];
// (Nor his back to it - faced no further round from it than this, as he
// is this long into it.)
const HERO_BACK_ON = -0.2;
const HERO_FACED_AT = 400;
const FIRST_DOWN_AT = 1100;
const OUT_SIGNAL_AT = 150;
const OUT_FIRST_AT = 1700;
// A first down on third or fourth down made by no more than this (feet
// past the line to gain): this often the chain out to measure it (see
// measure.ts), the picture on it this long after the play is over.
const MEASURE_NEAR = 1.5;
const MEASURE = 0.5;
const MEASURE_AFTER = 2400;
// (Not with any of these on the play: the scene is the measurement's.)
const NO_MEASURE = new Set([
	"injury",
	"timeout",
	"twoMinuteWarning",
	"quarter",
	"overtime",
	"gameOver",
]);

// The first thing said of a play after the snap: when it happened, by what
// it was. (For a play that is not acted out - a kick, so far - a fair
// guess at how long each takes.)
const resultAfterSnap = (e: RawEvent): number => {
	const yds = typeof e.yds === "number" ? e.yds : 0;
	switch (e.type) {
		case "passComplete":
			return 2600 + Math.min(2400, 45 * Math.max(0, yds));
		case "passIncomplete":
			return 2500 + 25 * Math.max(0, yds);
		case "sack":
			return 2800;
		case "interception":
			return 2900;
		case "run":
			return 1600 + Math.min(3000, 75 * Math.max(0, yds));
		case "kickoff":
		case "punt":
			return 1800;
		case "kickoffReturn":
		case "puntReturn":
			return 4000 + Math.min(4000, 70 * Math.max(0, yds));
		case "fieldGoal":
		case "extraPoint":
		case "shootoutShot":
			return 2400;
		case "onsideKickRecovery":
			return 1800;
		default:
			return 1800;
	}
};
// And each thing after that, how long after the last.
const RESULT_AFTER: Record<string, number> = {
	interceptionReturn: 1500,
	fumbleRecovery: 1400,
	kickoffReturn: 2600,
	puntReturn: 2400,
};

// ---- who and where ---------------------------------------------------------

// The groups a man the stats name could have been playing in, by the stat:
// a passer at quarterback, a man thrown to at receiver, a blocker on the
// line or in the backfield. A tackle (or a penalty) could have come from
// anybody on the field: unsaid, wherever he is highest on the depth chart.
const STAT_GROUPS: Record<string, Group[] | undefined> = {
	pss: ["QB"],
	pssCmp: ["QB"],
	pssSk: ["QB"],
	pssInt: ["QB"],
	rus: ["RB", "QB", "WR", "TE"],
	tgt: ["WR", "TE", "RB"],
	rec: ["WR", "TE", "RB"],
	pba: ["OL", "TE", "RB"],
	pbw: ["OL", "TE", "RB"],
	rba: ["OL", "TE", "RB"],
	rbw: ["OL", "TE", "RB"],
	ko: ["K"],
	ok: ["K"],
	okRec: ["K"],
	kr: ["KR"],
	pnt: ["P"],
	pr: ["PR"],
	xpa: ["K"],
	xp: ["K"],
};
const KICK_STAT = /^fga?\d+$/;
const RETURN_STATS = new Set(["kr", "krYds", "krLng"]);
// The order they are named in, most certain first: whoever had the ball.
const NAMED_ORDER = [
	"pss",
	"rus",
	"tgt",
	"rec",
	"ko",
	"ok",
	"pnt",
	"kr",
	"pr",
	"xpa",
	"pba",
	"rba",
];
const namedRank = (s: string) => {
	const i = NAMED_ORDER.indexOf(s);
	return i < 0 ? (KICK_STAT.test(s) ? 5 : NAMED_ORDER.length) : i;
};

// A play's eleven-a-side, as aligned: their spots, stances and facings.
type Placed = {
	pid: number;
	team: Side;
	role: Role;
	at: Pt;
	yaw: number;
	move: Move;
	mirrored?: boolean;
	ball?: "snap";
};

// The sideline on one side of the offense (-1 its left, 1 its right).
const sidelineY = (side: -1 | 1, dir: Dir) => (side * dir > 0 ? FIELD_W : 0);

// A slot's spot across the field, as feet to the offense's right of the
// ball - turned the other way round if `flip`.
const acrossOf = (a: Align, dir: Dir, ballY: number, flip: boolean): number => {
	if (a.wide) {
		const side = (flip ? -a.wide.side : a.wide.side) as -1 | 1;
		const line = sidelineY(side, dir);
		const y = line === 0 ? a.wide.ft : line - a.wide.ft;
		return (y - ballY) * dir;
	}
	return (flip ? -1 : 1) * (a.across ?? 0);
};

const facing = (dir: Dir): number => (dir === 1 ? 0 : Math.PI);

const clamp01 = (x: number) => Math.min(1, Math.max(0, x));
const clampY = (y: number, lo = 4, hi = FIELD_W - 4) =>
	Math.min(hi, Math.max(lo, y));

// Never back past the end line: a punter on his own 2 stands at the back of
// the end zone, not out of it.
const clampX = (x: number, margin = 3) =>
	Math.min(FIELD_L - margin, Math.max(margin, x));

const HASHES = [HASH_FAR_Y, HASH_NEAR_Y] as const;

// Just walked up to his spot and set there (see walkUp), nothing since.
const walkedUp = (p: Part): boolean => {
	const set = p.legs.at(-1);
	const up = p.legs.at(-2);
	return (
		!!set &&
		!!up &&
		set.t1 === set.t0 &&
		(up.move === "walk" || up.move === "jog")
	);
};

const ORDINALS = ["1st", "2nd", "3rd", "4th"];
// A down from scrimmage, with a line to gain.
const FROM_SCRIMMAGE = new Set<PlayKind>([
	"pass",
	"run",
	"kneel",
	"punt",
	"fieldGoal",
	"none",
]);

const situationOf = (kind: PlayKind, clock: RawEvent): string => {
	if (kind === "kickoff" || kind === "onside") {
		return "Kickoff";
	}
	if (kind === "extraPoint") {
		return "PAT";
	}
	if (clock.awaitingAfterTouchdown) {
		return "2-Pt Try";
	}
	const down = typeof clock.down === "number" ? clock.down : 1;
	const toGo = typeof clock.toGo === "number" ? clock.toGo : 10;
	const scrimmage = typeof clock.scrimmage === "number" ? clock.scrimmage : 0;
	return `${ORDINALS[down - 1] ?? `${down}th`} & ${
		scrimmage + toGo >= 100 ? "Goal" : toGo
	}`;
};

// On a two-point try the sim keeps no stats: who did what read off the
// names in its lines instead - for each name in turn, the stats it stands
// for, and whether he is with the offense (or the defense; either, unset).
const TRY_STATS: Record<string, [string[], boolean | undefined][]> = {
	dropback: [[["pss"], true]],
	passComplete: [
		[["pss"], true],
		[["tgt", "rec"], true],
	],
	passIncomplete: [
		[["pss"], true],
		[["tgt"], true],
	],
	handoff: [
		[[], true],
		[["rus"], true],
	],
	run: [[["rus"], true]],
	sack: [
		[["pss"], true],
		[["defSk"], false],
	],
	interception: [[["defInt"], false]],
	fumble: [
		[["fmb"], true],
		[["defFmbFrc"], false],
	],
	fumbleRecovery: [[["defFmbRec"], undefined]],
};

// ---- the director ----------------------------------------------------------

class Director {
	readonly events: RawEvent[];
	readonly gid: number;
	readonly numPeriods: number;
	readonly depth: [DepthChart, DepthChart];
	// Each side's men, by display side - and by name.
	readonly roster: [number[], number[]];
	readonly byName: [Map<string, number>, Map<string, number>] = [
		new Map(),
		new Map(),
	];
	// Who is out: hurt, and gone for the day.
	readonly out = new Set<number>();
	readonly plays: Play[] = [];
	readonly beats: Beat[] = [];
	readonly cuts: number[] = [];
	T = 0;
	quarter = 1;
	// Where the ball sits across the field for the next snap.
	ballY = MID_Y;
	lastLine = -Infinity;
	// Where the chain crew left off.
	chainSpots: ChainSpots | undefined;
	// The home team out of its tunnel before the game's first kickoff; a coin
	// toss before overtime's.
	runOutNext = true;
	tossNext = false;
	// The snaps in a row the defense has been out there for against this
	// offense, since the last break - a timeout, a quarter over.
	drive: { offense: Side; snaps: number } | undefined;
	// Which side had the last snap from scrimmage, and each side's
	// quarterback for its last one.
	lastScrimmage: Side | undefined;
	lastQb: [number | undefined, number | undefined] = [undefined, undefined];
	// Each man's speed against the others at his position, and his ratings,
	// where the game has them (see ratings.ts).
	readonly speeds: Map<number, number>;
	readonly flair: Map<number, Flair>;
	readonly rated = new Map<number, FieldRatings>();
	readonly stakes: Stakes | undefined;
	// What the clock read as each quarter started.
	readonly opens = new Map<number, number>();

	constructor({ events, players, gid, numPeriods = 4, stakes }: FieldInput) {
		this.events = events;
		this.gid = gid ?? 0;
		this.numPeriods = numPeriods;
		this.stakes = stakes;
		const init = events.find((e) => e?.type === "init");
		this.depth = depthCharts(init, players);
		this.roster = [
			players.filter((p) => p.team === 0).map((p) => p.pid),
			players.filter((p) => p.team === 1).map((p) => p.pid),
		];
		this.speeds = speedsOf(players);
		this.flair = flairOf(players);
		for (const p of players) {
			if (p.ratings) {
				this.rated.set(p.pid, p.ratings);
			}
		}
		for (const p of players) {
			if (p.name !== undefined) {
				this.byName[p.team].set(p.name, p.pid);
			}
		}
	}

	rng(i: number, what: string) {
		return makeCourtRng(`field|${this.gid}|${i}|${what}`);
	}

	// A line said at `want` - or, if the last one was said later than that
	// or only just before, a moment after it.
	line(i: number, type: string, want: number, end?: number): number {
		const a = Math.max(want, this.lastLine + LINE_GAP);
		this.beats.push({
			i,
			type,
			preStart: Math.min(this.T, a),
			actionStart: a,
			end: Math.max(a, end ?? a + 300),
		});
		this.lastLine = a;
		return a;
	}

	run() {
		const { events } = this;
		// The plays: from each "clock" event to the next.
		const starts: number[] = [];
		for (const [i, e] of events.entries()) {
			if (e?.type === "clock") {
				starts.push(i);
			}
		}
		// Anything said before the first (there should be nothing).
		const first = starts[0] ?? events.length;
		for (let i = 0; i < first; i++) {
			if (isLineItem(events[i])) {
				this.line(i, events[i]!.type, this.T);
				this.T = this.lastLine + TAIL;
			}
		}
		for (const [k, i0] of starts.entries()) {
			const i1 = starts[k + 1] ?? events.length;
			this.play(i0, i1);
		}
		return this.finish();
	}

	finish(): FieldTimeline {
		const end = Math.max(this.T, this.lastLine + TAIL);
		return {
			plays: this.plays,
			beats: this.beats,
			cuts: this.cuts,
			end,
		};
	}

	// The man who won the game on its last play - the side behind or level
	// before it, ahead after it: the kicker of a field goal (or an extra
	// point), the man with the ball as it went in.
	wonIt(
		kind: PlayKind,
		i0: number,
		i1: number,
		parts: Part[],
		ball: BallSeg[],
		whistle: number,
	): number | undefined {
		const before = this.pointsTo(i0);
		const after = this.pointsTo(i1);
		const winner =
			after[0] > after[1] ? 0 : after[1] > after[0] ? 1 : undefined;
		if (winner === undefined || before[winner] > before[1 - winner]!) {
			return undefined;
		}
		if (kind === "fieldGoal" || kind === "extraPoint") {
			return parts.find((p) => p.role === "K" && p.team === winner)?.pid;
		}
		const held = ball.findLast((b) => b.kind === "held" && b.t0 <= whistle);
		const pid = held?.kind === "held" ? held.pid : undefined;
		return parts.find((p) => p.pid === pid)?.team === winner ? pid : undefined;
	}

	// The score, each side's, by the lines before i1.
	pointsTo(i1: number): [number, number] {
		const pts: [number, number] = [0, 0];
		for (let i = 0; i < i1; i++) {
			const e = this.events[i];
			if (e?.type === "stat" && e.s === "pts" && typeof e.amt === "number") {
				// (Raw team t is display side 1 - t.)
				pts[e.t === 1 ? 0 : 1] += e.amt;
			}
		}
		return pts;
	}

	// The star of the game, for the interview after it (see postgame.ts):
	// of the winners on the field for the last play, the one whose game by
	// the lines before i1 counts for the most (see STAR_WORTH) - if it counts
	// for enough.
	starOf(parts: Part[], pts: [number, number], i1: number): number | undefined {
		const winner = pts[0] > pts[1] ? 0 : pts[1] > pts[0] ? 1 : undefined;
		if (winner === undefined) {
			return undefined;
		}
		const on = new Set(
			parts.filter((p) => p.team === winner).map((p) => p.pid),
		);
		const worth = new Map<number, number>();
		for (let i = 0; i < i1; i++) {
			const e = this.events[i];
			// (Raw team t is display side 1 - t.)
			if (
				e?.type !== "stat" ||
				typeof e.pid !== "number" ||
				typeof e.amt !== "number" ||
				(e.t === 1 ? 0 : 1) !== winner ||
				!on.has(e.pid)
			) {
				continue;
			}
			const s = String(e.s);
			const w = STAR_WORTH[s] ?? (/^fg\d/.test(s) ? STAR_FG : 0);
			worth.set(e.pid, (worth.get(e.pid) ?? 0) + w * e.amt);
		}
		let best: number | undefined;
		for (const [pid, w] of worth) {
			if (w >= STAR_LEAST && (best === undefined || w > worth.get(best)!)) {
				best = pid;
			}
		}
		return best;
	}

	// Who did what in a play, by its stats - on a two-point try, by the
	// names in its lines.
	statsOf(i0: number, i1: number): StatLine[] {
		const out: StatLine[] = [];
		let isTry = false;
		let onside: RawEvent | undefined;
		let onsideAt = i0;
		for (let i = i0; i < i1; i++) {
			const e = this.events[i];
			if (e?.type === "stat" && typeof e.pid === "number") {
				out.push({ s: e.s, pid: e.pid, t: e.t, i });
			} else if (e?.type === "twoPointConversion") {
				isTry = true;
			} else if (e?.type === "onsideKickRecovery") {
				onside = e;
				onsideAt = i;
			}
		}
		if (onside) {
			return this.onsideStats(out, onside, onsideAt);
		}
		if (!isTry) {
			return out;
		}
		const o = this.events[i0]?.t === 1 ? 1 : 0;
		for (let i = i0; i < i1; i++) {
			const e = this.events[i];
			const spec = e ? TRY_STATS[e.type] : undefined;
			if (!spec || !Array.isArray(e!.names)) {
				continue;
			}
			spec.forEach(([stats, own], k) => {
				const name = e!.names[k];
				if (typeof name !== "string") {
					return;
				}
				// (Raw team t is display side 1 - t.)
				const first = own === false ? 1 - o : o;
				for (const t of own === undefined ? [first, 1 - first] : [first]) {
					const pid = this.byName[(1 - t) as Side].get(name);
					if (pid !== undefined) {
						for (const s of stats) {
							out.push({ s, pid, t, i });
						}
						return;
					}
				}
			});
		}
		return out;
	}

	// An onside kick: the man who came up with it - the return the sim
	// calls it, if it was the hands team's; if the kicking team's, named in
	// its line only (the kicker credited with it) - "onsRec", wherever he
	// lines up, not back deep as a returner.
	onsideStats(stats: StatLine[], e: RawEvent, i: number): StatLine[] {
		const t = e.t === 1 ? 1 : 0;
		const name = Array.isArray(e.names) ? e.names[0] : undefined;
		const pid =
			stats.find((x) => x.s === "kr")?.pid ??
			(typeof name === "string"
				? this.byName[(1 - t) as Side].get(name)
				: undefined);
		const out = stats.filter((x) => !RETURN_STATS.has(x.s));
		if (pid !== undefined) {
			out.push({ s: "onsRec", pid, t, i });
		}
		return out;
	}

	// The men a play's stats name, for each side: who they are and what
	// they could have been playing.
	named(stats: StatLine[]): [Named[], Named[]] {
		const out: [Named[], Named[]] = [[], []];
		const seen = new Map<number, Named>();
		for (const e of stats) {
			const side = (1 - e.t) as Side;
			const groups =
				STAT_GROUPS[e.s] ??
				(KICK_STAT.test(e.s) ? (["K"] as Group[]) : undefined);
			const rank = namedRank(e.s);
			const had = seen.get(e.pid);
			if (had) {
				// (Said more than once: what he did that says most.)
				if (rank < had.rank) {
					had.rank = rank;
					had.groups = groups;
				}
				continue;
			}
			const n = { pid: e.pid, side, groups, rank };
			seen.set(e.pid, n);
			out[side].push(n);
		}
		for (const side of [0, 1] as const) {
			out[side].sort((a, b) => a.rank - b.rank);
		}
		return out;
	}

	// The spots the officials mark with a bean bag: where a punt was
	// fielded to be run back (not a fair catch, nor let go), where the ball
	// came loose.
	bagsOf(
		kind: PlayKind,
		offense: Side,
		i0: number,
		i1: number,
		acted: Acted | undefined,
		ball: BallSeg[],
		parts: Part[],
	): { t: number; at: Pt }[] {
		if (!acted) {
			return [];
		}
		const { events } = this;
		const out: { t: number; at: Pt }[] = [];
		const seg = events.slice(i0, i1);
		if (kind === "punt" && seg.some((e) => e?.type === "puntReturn")) {
			const fielded = ball.find(
				(b): b is Extract<BallSeg, { kind: "held" }> =>
					b.kind === "held" &&
					b.t0 > (acted.follow ?? 0) &&
					parts.find((p) => p.pid === b.pid)?.team !== offense,
			);
			const at = fielded && anyBall(ball, parts, fielded.t0);
			if (fielded && at) {
				out.push({ t: fielded.t0, at });
			}
		}
		for (let i = i0; i < i1; i++) {
			const t = acted.at.get(i);
			if (events[i]?.type === "fumble" && t !== undefined) {
				const at = anyBall(ball, parts, t);
				if (at) {
					out.push({ t, at });
				}
			}
		}
		return out;
	}

	// The next play a punt or a field goal (its unit coming on said first,
	// after the clock line at i1) - not a go for it, nor past the end of a
	// quarter.
	kicksNext(i1: number) {
		const { events } = this;
		for (let i = i1 + 1; i < events.length; i++) {
			const type = events[i]?.type;
			if (type === "puntTeam" || type === "fieldGoalAttempt") {
				return true;
			}
			if (
				type === "clock" ||
				type === "quarter" ||
				type === "overtime" ||
				type === "gameOver" ||
				(type !== undefined && PLAY_KIND[type] !== undefined) ||
				type === "goingForItOn4th"
			) {
				return false;
			}
		}
		return false;
	}

	play(i0: number, i1: number) {
		const { events } = this;
		const clock = events[i0]!;
		const offense = (1 - (clock.t === 1 ? 1 : 0)) as Side;
		let kind: PlayKind = "none";
		let kindAt = -1;
		for (let i = i0 + 1; i < i1; i++) {
			const type = events[i]?.type;
			const k = type === undefined ? undefined : PLAY_KIND[type];
			if (k) {
				kind = k;
				kindAt = i;
				break;
			}
		}
		const quarter = this.quarter;
		const dir = dirFor(offense, quarter);
		// (Out there a long drive, worn: see WINDED_FROM.)
		if (kind === "run" || kind === "pass" || kind === "kneel") {
			this.drive = {
				offense,
				snaps: this.drive?.offense === offense ? this.drive.snaps + 1 : 1,
			};
		} else if (kind !== "none") {
			this.drive = undefined;
		}
		const winded =
			this.drive && kind !== "none"
				? clamp01((this.drive.snaps - WINDED_FROM) / WINDED_SPAN)
				: 0;
		// (Crunch time: the big moments in it let out more - see action.ts.)
		const tensed = this.tensionOf(
			quarter,
			typeof clock.clock === "number" ? clock.clock : 15,
			i0,
		);
		const scrimmage: number =
			typeof clock.scrimmage === "number" ? clock.scrimmage : 25;
		const losX = yardLineX(scrimmage, dir);
		const toGo: number = typeof clock.toGo === "number" ? clock.toGo : 10;
		const gainX =
			FROM_SCRIMMAGE.has(kind) &&
			!clock.awaitingAfterTouchdown &&
			scrimmage + toGo < 100
				? losX + dir * toGo * YD
				: undefined;
		const kick = kind === "kickoff" || kind === "onside";
		const ballY =
			kick || kind === "extraPoint"
				? MID_Y
				: clampY(this.ballY, HASH_FAR_Y, HASH_NEAR_Y);
		const t0 = this.T;
		this.cuts.push(t0);
		const r = this.rng(i0, "play");
		const tossed = kick && this.tossNext;
		this.tossNext = false;
		const runningOut = kick && this.runOutNext;
		this.runOutNext = false;

		// Who is on, and where each lines up.
		const stats = this.statsOf(i0, i1);
		const named = this.named(stats);
		const { placed, formation } = this.lineUp(
			kind,
			events,
			i0,
			i1,
			offense,
			dir,
			losX,
			ballY,
			clock,
			named,
			r,
		);

		// The coin toss first, at the start of overtime - the men for the kick
		// waiting on their sidelines until the picture cuts to it (see
		// toss.ts).
		const toss = tossed
			? stageToss({
					captains: [0, 1].map((team) =>
						this.captainsOf(team as Side, new Set(placed.map((p) => p.pid))),
					) as [number[], number[]],
					t0,
					r: this.rng(i0, "toss"),
				})
			: undefined;
		// Or, before the game's first, the home team out of its tunnel - the
		// visitors out already (see tunnel.ts).
		const runOut = runningOut
			? stageRunOut({
					team: 1,
					pids: this.roster[1].filter((pid) => !this.out.has(pid)),
					t0,
					r: this.rng(i0, "runOut"),
				})
			: undefined;
		const base = toss ? toss.cut : runOut ? runOut.cut : t0;
		if (toss || runOut) {
			this.cuts.push(base);
		}

		// Up to the line, set, and the snap.
		const parts: Part[] = [];
		let set = base + KICK_AT;
		let hurried = false;
		if (kick) {
			const waiting = [0, 0];
			for (const p of placed) {
				const legs: Leg[] = [];
				if (runOut?.team === p.team) {
					// (Out of the tunnel with the rest of them, until the cut.)
					legs.push({
						t0,
						t1: base,
						from: p.at,
						to: p.at,
						move: p.move,
						yaw: p.yaw,
						...(p.mirrored ? { mirrored: true } : {}),
					});
				} else if (toss || runOut) {
					const at = waitingAt(p.team, waiting[p.team]!++, r);
					legs.push({
						t0,
						t1: base,
						from: at,
						to: at,
						move: "stand",
						yaw: Math.atan2(MID_Y - at.y, MID_X - at.x),
					});
				}
				legs.push({
					t0: base,
					t1: base,
					from: p.at,
					to: p.at,
					move: p.move,
					yaw: p.yaw,
					...(p.mirrored ? { mirrored: true } : {}),
				});
				// (The kicker's hand up, ready - and down again as he goes.)
				if (p.role === "K" && p.move === "stand") {
					const up = base + KICK_AT - KICK_HAND_UP;
					const down = base + KICK_AT - KICK_HAND_DOWN;
					const still = legs.at(-1)!;
					still.t1 = up;
					legs.push(
						{ ...still, t0: up, t1: down, move: "handUp" },
						{ ...still, t0: down, t1: down },
					);
				}
				parts.push({ pid: p.pid, team: p.team, role: p.role, legs });
			}
		} else {
			// The two-minute drill, as the sim has it (see GameSim's hurryUp):
			// no huddle.
			const left = typeof clock.clock === "number" ? clock.clock : 15;
			const pts = this.pointsTo(i0);
			const hurry =
				(kind === "pass" || kind === "run" || kind === "none") &&
				left <= 2 &&
				(((quarter * 2 === this.numPeriods || quarter >= this.numPeriods) &&
					scrimmage >= 50) ||
					(quarter === this.numPeriods && pts[offense] <= pts[1 - offense]!));
			hurried = hurry;
			set = hurry
				? this.hurryUp(placed, parts, offense, dir, t0, r)
				: this.walkUp(placed, parts, offense, dir, losX, ballY, t0, r);
		}
		let snap = kick
			? base + KICK_AT
			: kind === "none"
				? set + 400
				: set + CADENCE[0] + (CADENCE[1] - CADENCE[0]) * r();
		const qbClose =
			!hurried &&
			(kind === "pass" || kind === "run") &&
			(clock.down === 3 || clock.down === 4 || tensed > 0) &&
			!this.plays.at(-1)?.callIn &&
			parts.find((p) => p.team === offense && p.role === "QB")?.legs.at(-1)
				?.move === "shotgun" &&
			this.rng(i0, "qbClose")() < QB_CLOSE;
		if (qbClose) {
			snap = Math.max(
				snap,
				set - QB_CLOSE_BEFORE + QB_CLOSE_LEAST + QB_WIDE_BEFORE,
			);
		}
		const flagged = events.slice(i0, i1).some((e) => e?.type === "flag");
		if (kind === "pass" && !flagged) {
			snap = this.motion(parts, offense, dir, losX, ballY, set, snap, i0);
		}
		this.cadence(parts, offense, snap, i0, qbClose);
		const routine =
			kind === "fieldGoal" || kind === "extraPoint"
				? this.kickerRoutine(parts, offense, dir, snap, i0, kind)
				: undefined;
		if (routine) {
			snap = routine.snap;
		}
		if (kind === "punt") {
			this.punterSet(parts, offense, snap);
		}
		if (kind === "pass" || kind === "run") {
			this.defenseSet(parts, offense, dir, losX, ballY, snap, i0);
		}
		if (!kick && kind !== "none") {
			this.lineDown(parts, offense, snap, i0);
		}
		paceWalkUps(parts);
		const qbCloseUp = qbClose
			? qbCloseFor(
					parts,
					offense,
					dir,
					set - QB_CLOSE_BEFORE,
					Math.min(
						snap - QB_WIDE_BEFORE,
						set - QB_CLOSE_BEFORE + QB_CLOSE_MOST,
					),
				)
			: undefined;

		// The ball: spotted (on its tee, for a kickoff), then under the
		// center's hand once he is over it.
		const ball: BallSeg[] = [
			{
				kind: "spot",
				t0,
				at: { x: losX, y: ballY, z: 0 },
				...(kick ? { stood: "tee" as const } : {}),
			},
		];
		const center = parts.find((p) => p.role === "C" || p.role === "LS");
		const over = center?.legs.find((l) => l.ball === "snap");
		if (center && over) {
			ball.push({
				kind: "held",
				t0: Math.min(over.t0 + HAND_ON_BALL_MS, snap - 1),
				pid: center.pid,
			});
		}

		// (A new set of downs for the same side, by what it did - not a flag.)
		const after = events[i1];
		const moved =
			(kind === "run" || kind === "pass") &&
			!flagged &&
			after?.type === "clock" &&
			after.down === 1 &&
			after.t === clock.t &&
			!events
				.slice(i0, i1)
				.some(
					(e) =>
						e?.type === "interception" ||
						e?.lost ||
						e?.td === true ||
						e?.safety === true,
				);
		// (Stopped short on third down - or on fourth, the ball over on
		// downs.)
		const stopped =
			(kind === "run" || kind === "pass") && !flagged
				? events.slice(i0, i1).some((e) => e?.type === "turnoverOnDowns")
					? ("fourth" as const)
					: clock.down === 3 &&
						  after?.type === "clock" &&
						  after.down === 4 &&
						  after.t === clock.t
						? ("third" as const)
						: undefined
				: undefined;
		// (Stopped on third, and the punt or the kick on next: both sides
		// off for the units coming on - not back to their huddles.)
		const kicksNext = stopped === "third" && this.kicksNext(i1);
		// Out of bounds: a run or a catch the sim stopped the clock after
		// with nothing else to stop it - no score, no change of hands, no
		// flag, no timeout, no two-minute warning - no more time gone by the
		// next snap than the play itself took (see GameSim's doRun, doPass).
		const result = events
			.slice(i0, i1)
			.find((e) => e?.type === "run" || e?.type === "passComplete");
		const took =
			typeof clock.clock === "number" && typeof after?.clock === "number"
				? (clock.clock - after.clock) * 60
				: Infinity;
		const yards = Math.abs(typeof result?.yds === "number" ? result.yds : 0);
		const out =
			result !== undefined &&
			!flagged &&
			after?.type === "clock" &&
			took > 0 &&
			took <=
				(result.type === "run" ? 4 + yards / 10 : 6 + yards / 20) + 0.01 &&
			!events
				.slice(i0, i1)
				.some(
					(e) =>
						STOPS_CLOCK.has(e?.type) ||
						e?.lost ||
						e?.td === true ||
						e?.safety === true,
				);
		// The play itself, from the snap - or, a flag before it, the foul.
		const acted =
			ACTED.has(kind) || flagged
				? actOut({
						kind,
						offense,
						dir,
						losX,
						ballY,
						snap,
						set: formation ? OFFENSE_SETS[formation] : undefined,
						parts,
						events,
						stats,
						i0,
						i1,
						...(moved ? { firstDown: true } : {}),
						...(stopped ? { stopped } : {}),
						...(kicksNext ? { kicksNext: true } : {}),
						...(out ? { out: true } : {}),
						speeds: this.speeds,
						rated: this.rated,
						flair: this.flair,
						...(winded > 0 ? { winded } : {}),
						...(tensed > 0 ? { tense: tensed } : {}),
						r: this.rng(i0, "action"),
					})
				: undefined;
		if (acted) {
			ball.push(...acted.ball);
		}
		const whistle = acted ? acted.whistle : snap + 1200;
		// The fouls in it: when each happened, and where.
		const fouls = flagged
			? foulsOf({
					kind,
					offense,
					dir,
					losX,
					ballY,
					set,
					snap,
					whistle,
					events,
					i0,
					i1,
					parts,
					ball,
					pidOf: (side, name) => this.byName[side].get(name),
					at: acted?.at ?? new Map(),
					r: this.rng(i0, "fouls"),
				})
			: [];
		// The fouls acted out, where the field has the men who did them - and
		// the man flagged, after the whistle, now and then not having it.
		actFouls(fouls, parts, whistle);
		// When the play has each line happen.
		const timed = new Map(acted?.at ?? []);
		for (const f of fouls) {
			timed.set(f.flagI, f.t);
		}

		// The lines.
		this.lastLine = Math.max(this.lastLine, base - LINE_GAP);
		let afterSnap = false;
		let results = 0;
		let lastAt = base;
		let pre = 0;
		for (let i = i0; i < i1; i++) {
			const e = events[i];
			if (!e) {
				continue;
			}
			if (e.type === "quarter") {
				this.quarter =
					typeof e.quarter === "number" ? e.quarter : this.quarter + 1;
				this.drive = undefined;
			} else if (e.type === "overtime") {
				this.quarter =
					this.numPeriods + (typeof e.overtimes === "number" ? e.overtimes : 1);
				this.tossNext = true;
			} else if (e.type === "injury" && typeof e.injuredPID === "number") {
				this.out.add(e.injuredPID);
			}
			if (!isLineItem(e)) {
				continue;
			}
			let want: number;
			if (i === i0) {
				want = base + CLOCK_AT;
			} else if (BEFORE_SNAP.has(e.type) && !afterSnap) {
				// (The kickoff team's onside call too, though it is the kick.)
				want = base + 900 + pre * 900;
				pre += 1;
				if (e.type === "onsideKick") {
					afterSnap = i >= kindAt;
				}
			} else if (e.type === "flag" && !afterSnap) {
				// Before the snap: a false start, offside.
				want =
					timed.get(i) ?? (kindAt < 0 || i < kindAt ? set + 600 : snap + 1300);
				afterSnap = kindAt >= 0 && i > kindAt;
			} else if (timed.has(i)) {
				want = timed.get(i)!;
				afterSnap = true;
				if (RESULTS.has(e.type)) {
					results += 1;
				}
			} else if (AT_SNAP[e.type] !== undefined) {
				want = snap + AT_SNAP[e.type]!;
				afterSnap = true;
			} else if (RESULTS.has(e.type)) {
				want =
					results === 0
						? snap + resultAfterSnap(e)
						: lastAt + (RESULT_AFTER[e.type] ?? 1200);
				results += 1;
				afterSnap = true;
			} else if (e.type === "flag") {
				want = snap + 1300;
			} else {
				want = lastAt + (AFTER_PLAY[e.type] ?? 1200);
			}
			lastAt = this.line(i, e.type, want);
		}
		// What the officials signal: the referee's word on each foul as its
		// line is said; a score, an incompletion, a kick good or not, a first
		// down, a timeout (the two-minute warning one too).
		const calls: CrewInput["calls"] = [];
		const signals: CrewInput["signals"] = [];
		const spotAt = (t: number) =>
			anyBall(ball, parts, t) ?? { x: losX, y: ballY };
		let scored = false;
		for (const b of this.beats) {
			if (b.i < i0 || b.i >= i1) {
				continue;
			}
			const e = events[b.i]!;
			const t = b.actionStart;
			if (e.type === "penalty") {
				const foul = fouls.find((f) => f.penI === b.i);
				if (foul) {
					calls.push({ t, foul });
				}
			} else if (e.type === "fieldGoal" || e.type === "extraPoint") {
				signals.push({ what: e.made ? "good" : "noGood", t, at: spotAt(t) });
			} else if (e.td === true && RESULTS.has(e.type)) {
				// (When he gets in, not when he got the ball.)
				scored = true;
				const at = Math.max(t, whistle);
				signals.push({ what: "td", t: at, at: spotAt(at) });
			} else if (e.safety === true && RESULTS.has(e.type)) {
				scored = true;
				const at = Math.max(t, whistle);
				signals.push({ what: "safety", t: at, at: spotAt(at) });
			} else if (e.type === "passIncomplete") {
				signals.push({ what: "incomplete", t, at: spotAt(t) });
			} else if (e.type === "timeout" || e.type === "twoMinuteWarning") {
				signals.push({ what: "timeout", t, at: spotAt(t) });
			}
		}
		// The clock stopped: out of bounds.
		if (acted?.out) {
			signals.push({ what: "out", t: whistle + OUT_SIGNAL_AT, at: acted.out });
		}
		// (A new set of downs for the same side, by what it did - not a flag.)
		const next = events[i1];
		const made =
			(kind === "run" || kind === "pass") &&
			!scored &&
			fouls.length === 0 &&
			next?.type === "clock" &&
			next.down === 1 &&
			next.t === clock.t &&
			!events.slice(i0, i1).some((e) => e?.type === "interception" || e?.lost);
		// (Made by the nose of it, late in the downs: measured, some of the
		// time - the signal the referee's, then.)
		const measuring =
			made &&
			gainX !== undefined &&
			typeof clock.down === "number" &&
			clock.down >= 3 &&
			!acted?.out &&
			Math.abs(spotAt(whistle).x - gainX) <= MEASURE_NEAR &&
			!events.slice(i0, i1).some((e) => NO_MEASURE.has(e?.type)) &&
			this.rng(i0, "measure")() < MEASURE;
		if (made && !measuring) {
			signals.push({
				what: "first",
				t: whistle + (acted?.out ? OUT_FIRST_AT : FIRST_DOWN_AT),
				at: spotAt(whistle),
			});
		}
		let cheer = this.cheerOf(i0, i1, offense, whistle, ball, parts, stopped);
		let t1 = Math.max(
			this.lastLine + TAIL,
			snap + 1500,
			acted ? acted.whistle + (scored ? AFTER_SCORE : AFTER_WHISTLE) : 0,
			// (The moment after a big play seen out.)
			acted?.until
				? Math.min(
						acted.whistle + CELEBRATE_MOST,
						acted.until + CELEBRATE_AFTER,
					)
				: 0,
			...calls.map((c) => c.t + CALL_MS),
		);
		// A man hurt on it, as the sim says: down, the trainers out to him, and
		// off (see hurt.ts) - the play over only once he is.
		const medics: CrewPart[] = [];
		const busy = new Set<number>();
		let hurt: { pid: number; down: number; tending: number } | undefined;
		for (let i = i0; i < i1 && medics.length < HURT_MOST * MEDICS; i++) {
			const e = events[i];
			if (e?.type !== "injury" || typeof e.injuredPID !== "number") {
				continue;
			}
			const h = stageHurt({
				pid: e.injuredPID,
				n: medics.length / MEDICS,
				parts,
				whistle,
				t0,
				t1,
				ball,
				busy,
				r: this.rng(i, "hurt"),
			});
			if (h) {
				medics.push(...h.medics);
				t1 = h.t1;
				hurt ??= { pid: e.injuredPID, down: h.down, tending: h.tending };
			}
		}
		// The game over: the winners' moment, both sides out to the middle to
		// shake hands, the head coaches too (see postgame.ts).
		const ended =
			!hurt && events.slice(i0, i1).some((e) => e?.type === "gameOver");
		const hero = ended
			? this.wonIt(kind, i0, i1, parts, ball, whistle)
			: undefined;
		const pts = this.pointsTo(i1);
		const showerMen =
			ended && hero === undefined
				? this.showerOf(kind, pts, parts, i0)
				: undefined;
		const star = ended ? this.starOf(parts, pts, i1) : undefined;
		const postgame = ended
			? stagePostgame({
					parts,
					pts,
					...(star !== undefined ? { star } : {}),
					// (Won on it: from the moment it was - not after the scorer's
					// moment.)
					whistle:
						hero === undefined ? Math.max(whistle, acted?.until ?? 0) : whistle,
					...(hero !== undefined ? { hero } : {}),
					t0,
					t1,
					playI: i0,
					ball,
					...(showerMen
						? { shower: { men: showerMen, r: this.rng(i0, "showered") } }
						: {}),
					...(this.stakes ? { stakes: this.stakes } : {}),
					r: this.rng(i0, "postgame"),
				})
			: undefined;
		if (postgame) {
			t1 = postgame.t1;
			if (postgame.interview) {
				this.cuts.push(postgame.interview.t0);
			}
			this.cuts.push(postgame.look);
			if (postgame.winner !== undefined) {
				cheer = {
					t: postgame.from,
					side: postgame.winner,
					big: true,
					// (The round won: the building up for it all the way.)
					...(postgame.clinch ? { hold: t1 - postgame.from } : {}),
				};
			}
		}
		// The half over (or regulation, before overtime): everybody off for
		// the break.
		const half =
			!ended &&
			!hurt &&
			events
				.slice(i0, i1)
				.some(
					(e) =>
						(e?.type === "quarter" && e.startsWithKickoff === true) ||
						e?.type === "overtime",
				);
		const offAt = Math.max(whistle, acted?.until ?? 0);
		// (A field goal that ties it, or puts them ahead, late.)
		const clutch =
			kind === "fieldGoal" &&
			quarter >= this.numPeriods &&
			(typeof clock.clock === "number" ? clock.clock : 15) <= LATE_KICK &&
			(() => {
				const before = this.pointsTo(i0);
				const after = this.pointsTo(i1);
				return (
					before[offense] <= before[1 - offense]! &&
					after[offense] >= after[1 - offense]!
				);
			})();
		// The play shown again, if it is, with the picture on it until t1 - not
		// cued before `until`, less 400 ms.
		const replayTill = (till: number, until = acted?.until) =>
			replayFor({
				kind,
				dir,
				events,
				beats: this.beats.filter((b) => b.i >= i0 && b.i < i1),
				t0,
				snap,
				whistle,
				...(until ? { until } : {}),
				t1: till,
				ball,
				fouled: fouls.some((f) => f.accepted),
				clutch,
			});
		if (half) {
			t1 = stageHalftime({
				parts,
				whistle: offAt,
				t1,
				ball,
				r: this.rng(i0, "half"),
			});
		}
		// A timeout (the two-minute warning too): the picture on to the side
		// that called it, after the referee's signal - and any replay.
		let calledI = -1;
		if (!ended && !hurt && !half) {
			for (let i = i0; i < i1; i++) {
				const e = events[i];
				if (e?.type === "timeout" || e?.type === "twoMinuteWarning") {
					calledI = i;
				}
			}
		}
		const timeout =
			calledI >= 0
				? (() => {
						const e = events[calledI]!;
						const said =
							this.beats.find((b) => b.i === calledI)?.actionStart ?? whistle;
						const again = replayTill(whistle + 60000);
						const team: Side =
							e.type === "timeout" && typeof e.t === "number"
								? ((1 - e.t) as Side)
								: offense;
						return stageTimeout({
							parts,
							ball,
							cut: Math.max(
								offAt + TIMEOUT_AFTER,
								said + TIMEOUT_SAID,
								again ? again.cue + TIMEOUT_AFTER_REPLAY : 0,
							),
							team,
							spot: anyBall(ball, parts, offAt) ?? { x: losX, y: ballY },
							t0,
							playI: i0,
							r: this.rng(i0, "timeout"),
							// (Called by his head coach - the picture on him, across
							// the field, if no replay comes between.)
							...(e.type === "timeout"
								? {
										call: {
											at: said,
											...(!again && sidelineOf(team) === 0
												? { shot: offAt + CALL_AFTER }
												: {}),
										},
									}
								: {}),
						});
					})()
				: undefined;
		if (timeout) {
			t1 = Math.max(t1, timeout.t1);
			if (timeout.call) {
				this.cuts.push(timeout.call.from);
			}
			this.cuts.push(timeout.cut);
			this.drive = undefined;
		}
		const cheerShot = timeout
			? cheerShotFor(timeout.cut, t1, timeout.at, this.rng(i0, "cheer"))
			: undefined;
		const crowdShot =
			timeout && !cheerShot
				? crowdShotFor(timeout.cut, t1, this.rng(i0, "crowdShot"))
				: undefined;
		if (cheerShot || crowdShot) {
			// (And from them to the next play.)
			this.cuts.push((cheerShot ?? crowdShot)!.cut, t1);
		}
		// The measurement: the picture on it once the play is over.
		const measureAt =
			measuring && !timeout && !hurt && !half && !ended
				? measureTimes(offAt + MEASURE_AFTER)
				: undefined;
		if (measureAt) {
			t1 = Math.max(t1, measureAt.t1);
			this.cuts.push(measureAt.cut);
			cheer = { t: measureAt.call + 450, side: offense, big: false };
		}
		// (After any replay.)
		const between =
			!ended &&
			!hurt &&
			!timeout &&
			!measureAt &&
			events.slice(i0, i1).some((e) => e?.type === "quarter")
				? Math.max(
						offAt + (half ? HALF_BETWEEN_AFTER : BETWEEN_AFTER),
						(replayTill(whistle + 60000)?.cue ?? 0) + TIMEOUT_AFTER_REPLAY,
					)
				: undefined;
		if (between !== undefined) {
			if (!half) {
				stageChangeEnds({
					parts,
					from: between,
					ball,
					r: this.rng(i0, "ends"),
				});
			}
			t1 = Math.max(t1, between + BETWEEN_MS);
			// (And from it to the next play.)
			this.cuts.push(between, t1);
		}
		// Each line of the play ends where its own action does; the last one,
		// with the play.
		const last = this.beats.at(-1);
		if (last && last.i >= i0) {
			last.end = t1;
		}
		// And everybody stays as the play left him until the next one.
		holdTill(parts, t1);
		// The officials, with the play.
		const minutes = typeof clock.clock === "number" ? clock.clock : 15;
		const crew = stageCrew({
			kind,
			offense,
			dir,
			losX,
			ballY,
			t0: base,
			set,
			snap,
			follow: acted?.follow ?? snap,
			whistle,
			t1,
			parts,
			ball,
			backToLine: events
				.slice(i0, i1)
				.some((e) => e?.type === "passIncomplete"),
			umpireDeep:
				scrimmage >= 95 ||
				(quarter === 2 && minutes <= 2) ||
				(quarter >= this.numPeriods && minutes <= 5),
			fouls,
			calls,
			signals,
			bags: this.bagsOf(kind, offense, i0, i1, acted, ball, parts),
			r: this.rng(i0, "crew"),
		});
		if (postgame) {
			crewOff(crew.parts, postgame.from, t1, this.rng(i0, "crewOff"));
		}
		if (toss) {
			tossCrew(toss, crew.parts);
		}
		if (runOut) {
			runOutCrew(runOut, crew.parts);
		}
		if (half) {
			crewOff(crew.parts, offAt, t1, this.rng(i0, "crewOff"));
		}
		if (timeout) {
			crewClear(crew.parts, timeout, t1);
		}
		const refCall = (() => {
			const ref = crew.parts.find((p) => p.pid === officialId("R"));
			if (!ref || calls.length === 0 || postgame || half || measureAt) {
				return undefined;
			}
			const first = Math.min(...calls.map((c) => c.t));
			const at = posOf(ref.legs, first);
			const done =
				Math.max(...calls.map((c) => c.t)) + CALL_FOR_MS + REF_CLOSE_AFTER;
			let from = first - REF_CLOSE_BEFORE;
			let until = Math.min(
				t1 - done < REF_CLOSE_ON ? t1 : done,
				timeout?.call?.from ?? Infinity,
				timeout?.cut ?? Infinity,
				between ?? Infinity,
				t1,
			);
			// (Not over the moment after a big play.)
			if (acted?.party && acted.until && acted.party.from < until) {
				from = Math.max(from, acted.until + REF_AFTER_PARTY);
			}
			// (Nobody between the camera and him: in on him once nobody is,
			// and back out if somebody comes.)
			const others = [...parts, ...medics, ...crew.parts].filter(
				(q) => q !== ref && q.legs.length > 0,
			);
			const inWay = (t: number) => {
				const him = posOf(ref.legs, t);
				return others.some((q) => {
					const p = posOf(q.legs, t);
					const d = at.y + REF_CAM_FT - p.y;
					return (
						p.y > him.y + REF_WAY_NEAR &&
						d > 0 &&
						Math.abs(p.x - him.x) < (REF_SEEN * d) / REF_CAM_FT + 1
					);
				});
			};
			while (from < until && inWay(from)) {
				from += REF_WAY_STEP;
			}
			for (let t = from; t < until; t += REF_WAY_STEP) {
				if (inWay(t)) {
					until = t;
					break;
				}
			}
			return until - from >= REF_CLOSE_LEAST && at.y + REF_CAM_FT < FIELD_W
				? { t0: from, t1: until, at }
				: undefined;
		})();
		const coachReact = (() => {
			const party = acted?.party;
			const until = acted?.until;
			const hero = parts.find((p) => p.pid === party?.hero);
			if (
				!party ||
				!until ||
				!hero ||
				ended ||
				half ||
				hurt ||
				timeout ||
				measureAt ||
				between !== undefined ||
				refCall
			) {
				return undefined;
			}
			const glad = hero.team === 1;
			const from = until + COACH_REACT_AFTER;
			if (
				this.rng(i0, "coachReact")() >=
				(glad ? COACH_REACT_GLAD : COACH_REACT_SAD)
			) {
				return undefined;
			}
			return {
				t0: from,
				t1: from + COACH_REACT_MS,
				side: 1 as Side,
				at: headCoachAt(1, i0),
				glad,
			};
		})();
		if (coachReact) {
			this.cuts.push(coachReact.t0, coachReact.t1);
			t1 = Math.max(t1, coachReact.t1 + COACH_REACT_TAIL);
			// (Everybody, officials too, as they were over the longer play.)
			holdTill([...parts, ...crew.parts], t1);
		}
		// (The replay, if any, straight after the head coach.)
		const shown = replayTill(t1, coachReact ? coachReact.t1 - 400 : undefined);
		// (A man hurt: the play shown again while they see to him.)
		const replay =
			shown ??
			(hurt && !fouls.some((f) => f.accepted) && hurt.tending > whistle
				? {
						cue: hurt.tending,
						from: Math.max(t0, snap - 300, whistle - 5600),
						to: whistle + 400,
						pid: hurt.pid,
					}
				: undefined);
		const heroClose = (() => {
			const party = acted?.party;
			const pid = party?.hero;
			const hero = parts.find((p) => p.pid === pid);
			if (
				!party ||
				pid === undefined ||
				!hero ||
				!acted?.until ||
				postgame ||
				half ||
				measureAt ||
				this.rng(i0, "heroClose")() >= HERO_CLOSE_SHARE
			) {
				return undefined;
			}
			const from = party.from + HERO_CLOSE_AFTER;
			const at = posOf(hero.legs, from);
			const camY = at.y + HERO_CAM_FT;
			if (camY >= FIELD_W) {
				return undefined;
			}
			const others = [...parts, ...medics, ...crew.parts].filter(
				(q) => q !== hero && q.legs.length > 0,
			);
			// (Anyone on the line from the camera - this far along the field
			// from him - to him, between the two.)
			const inWay = (t: number, shift: number) => {
				const him = posOf(hero.legs, t);
				return others.some((q) => {
					const p = posOf(q.legs, t);
					if (p.y <= him.y + REF_WAY_NEAR || p.y >= camY) {
						return false;
					}
					const u = (camY - p.y) / (camY - him.y);
					return Math.abs(p.x - (him.x + shift * (1 - u))) < HERO_BLOCK;
				});
			};
			const end = Math.min(
				from + HERO_CLOSE_MOST,
				acted.until + 400,
				t1 - 400,
				(replay?.cue ?? Infinity) - 300,
				refCall?.t0 ?? Infinity,
				timeout?.call?.from ?? Infinity,
				timeout?.cut ?? Infinity,
				between ?? Infinity,
			);
			// (Which way he is faced, a while into it.)
			const leg = hero.legs.findLast((l) => l.t0 <= from + HERO_FACED_AT);
			const yaw =
				leg?.yaw ??
				(leg && dist(leg.from, leg.to) > 0.5
					? Math.atan2(leg.to.y - leg.from.y, leg.to.x - leg.from.x)
					: undefined);
			// (From whichever of the camera's spots sees him longest - and not
			// his back.)
			let best: { t0: number; t1: number; shift: number } | undefined;
			for (const shift of HERO_SHIFTS) {
				if (
					yaw !== undefined &&
					Math.cos(yaw - Math.atan2(HERO_CAM_FT, shift)) < HERO_BACK_ON
				) {
					continue;
				}
				let t0 = from;
				while (t0 < from + HERO_LATE && inWay(t0, shift)) {
					t0 += REF_WAY_STEP;
				}
				let until = t0;
				while (
					until < end &&
					!inWay(until, shift) &&
					dist(posOf(hero.legs, until), at) <= HERO_DRIFT
				) {
					until += REF_WAY_STEP;
				}
				until = Math.min(until, end);
				if (!best || until - t0 > best.t1 - best.t0) {
					best = { t0, t1: until, shift };
				}
			}
			return best && best.t1 - best.t0 >= HERO_CLOSE_LEAST
				? { ...best, pid, at }
				: undefined;
		})();
		const callIn = (() => {
			const next = events[i1];
			if (
				ended ||
				half ||
				hurt ||
				timeout ||
				measureAt ||
				between !== undefined ||
				replay ||
				refCall ||
				heroClose ||
				coachReact ||
				next?.type !== "clock" ||
				next.t !== 0 ||
				next.down !== 3 ||
				next.awaitingAfterTouchdown
			) {
				return undefined;
			}
			const left = typeof next.clock === "number" ? next.clock : 15;
			if (
				left <= HURRY_MINUTES &&
				(quarter * 2 === this.numPeriods || quarter >= this.numPeriods)
			) {
				return undefined;
			}
			const t0 = offAt + CALL_IN_AFTER;
			return this.rng(i0, "callIn")() < CALL_IN_SHARE &&
				t1 - t0 >= CALL_IN_LEAST
				? { t0, t1, side: 1 as Side, at: headCoachAt(1, i0) }
				: undefined;
		})();
		if (callIn) {
			this.cuts.push(callIn.t0);
		}
		const chains = stageChains({
			quarter,
			dir,
			losX,
			scrimmage: FROM_SCRIMMAGE.has(kind) && !clock.awaitingAfterTouchdown,
			down: typeof clock.down === "number" ? clock.down : 1,
			gainX,
			t0,
			snap,
			whistle,
			t1,
			parts,
			before: this.chainSpots,
		});
		this.chainSpots = chains.spots;
		const measure =
			measureAt && gainX !== undefined
				? stageMeasure({
						parts,
						crew: crew.parts,
						chains: chains.parts,
						ball,
						cut: measureAt.cut,
						spot: spotAt(whistle),
						gainX,
						dir,
						offense,
						side: chains.spots.side,
						r: this.rng(i0, "measured"),
					})
				: undefined;
		// The home side's quarterback over the tablet as the visitors' drive
		// starts - the camera on him clear of everybody else.
		const tablet = (() => {
			const pid = this.lastQb[1];
			if (
				offense !== 0 ||
				this.lastScrimmage !== 1 ||
				pid === undefined ||
				hurried ||
				qbCloseUp ||
				(kind !== "pass" && kind !== "run") ||
				snap - t0 < TABLET_MS + TABLET_WIDE ||
				parts.some((p) => p.pid === pid) ||
				this.rng(i0, "tablet")() >= TABLET_SHARE
			) {
				return undefined;
			}
			const oc = coordinatorAt(1, 1, i0);
			const away = oc.x < MID_X ? -1 : 1;
			const at = { x: oc.x + away * TABLET_APART, y: oc.y };
			const mid = { x: (at.x + oc.x) / 2, y: at.y };
			const line = sidelineOf(1);
			const toField = line === 0 ? 1 : -1;
			const others = [...parts, ...medics, ...crew.parts, ...chains.parts];
			for (let t = t0; t <= t0 + TABLET_MS; t += 100) {
				for (const q of others) {
					if (q.legs.length === 0) {
						continue;
					}
					const p = posOf(q.legs, t);
					const d = (p.y - mid.y) * toField;
					if (
						Math.abs(p.x - mid.x) < TABLET_CLEAR &&
						d > -1.5 &&
						d < TABLET_CAM_FT
					) {
						return undefined;
					}
				}
			}
			// (Turned from facing one another a way toward the field.)
			const field = (toField * Math.PI) / 2;
			const turned = (yaw: number, share: number) =>
				yaw + Math.atan2(Math.sin(field - yaw), Math.cos(field - yaw)) * share;
			return {
				t0,
				t1: t0 + TABLET_MS,
				side: 1 as Side,
				pid,
				at,
				yaw: turned(away > 0 ? Math.PI : 0, TABLET_OPEN),
				oc,
				ocYaw: turned(away > 0 ? 0 : Math.PI, TABLET_OC_OPEN),
			};
		})();
		if (tablet) {
			this.cuts.push(tablet.t1);
		}
		if (kind === "pass" || kind === "run" || kind === "kneel") {
			this.lastScrimmage = offense;
			const qbPid = parts.find(
				(p) => p.team === offense && p.role === "QB",
			)?.pid;
			if (qbPid !== undefined) {
				this.lastQb[offense] = qbPid;
			}
		}
		if (!this.opens.has(quarter)) {
			this.opens.set(quarter, minutes);
		}
		const tense = this.tensionOf(quarter, minutes, i0);
		const seats = this.seatsOf(quarter, minutes, i0);
		this.plays.push({
			i: i0,
			kind,
			quarter,
			offense,
			dir,
			losX,
			ballY,
			situation: situationOf(kind, clock),
			t0,
			set,
			snap,
			t1,
			follow: acted?.follow ?? snap,
			...(formation ? { formation } : {}),
			...(hurried ? { hurry: true } : {}),
			parts,
			crew: crew.parts,
			flags: crew.flags,
			...(crew.bags.length > 0 ? { bags: crew.bags } : {}),
			...(calls.some((c) => c.foul.accepted)
				? {
						calls: calls
							.filter((c) => c.foul.accepted)
							.map((c) => ({ t: c.t, side: c.foul.side })),
					}
				: {}),
			chains: chains.parts,
			down: chains.spots.down,
			tv: stageTv({
				dir,
				losX,
				t0,
				snap,
				whistle,
				t1,
				parts,
				ball,
				r: this.rng(i0, "tv"),
			}),
			...(medics.length > 0 ? { medics } : {}),
			...(hurt ? { hurt: { pid: hurt.pid, from: hurt.down } } : {}),
			...(runOut ? { runOut } : {}),
			...(toss
				? {
						toss: {
							men: toss.men,
							at: toss.at,
							flip: toss.flip,
							cut: toss.cut,
						},
					}
				: {}),
			...(timeout
				? {
						coaches: timeout.coaches,
						timeout: {
							cut: timeout.cut,
							at: timeout.at,
							...(timeout.call ? { call: timeout.call } : {}),
							...(cheerShot ? { cheer: cheerShot } : {}),
							...(crowdShot ? { crowd: crowdShot } : {}),
						},
					}
				: {}),
			...(measure ? { measure: { cut: measure.cut, at: measure.at } } : {}),
			...(routine?.closeUp ? { closeUp: routine.closeUp } : {}),
			...(qbCloseUp ? { qbClose: qbCloseUp } : {}),
			...(tablet ? { tablet } : {}),
			...(refCall ? { refCall } : {}),
			...(heroClose ? { heroClose } : {}),
			...(callIn ? { callIn } : {}),
			...(coachReact ? { coachReact } : {}),
			...(between !== undefined ? { between } : {}),
			...(acted?.party && acted.until
				? { party: { ...acted.party, until: acted.until } }
				: {}),
			...(postgame
				? {
						coaches: postgame.coaches,
						postgame: {
							from: postgame.from,
							shake: postgame.shake,
							at: postgame.at,
							...(postgame.winner !== undefined
								? { winner: postgame.winner }
								: {}),
							...(postgame.mob ? { mob: postgame.mob } : {}),
							...(postgame.shower ? { shower: postgame.shower } : {}),
							look: postgame.look,
							...(postgame.clinch ? { clinch: postgame.clinch } : {}),
							...(postgame.interview ? { interview: postgame.interview } : {}),
						},
					}
				: {}),
			ball,
			...(replay ? { replay } : {}),
			...(cheer ? { cheer } : {}),
			...(tense > 0 ? { tense } : {}),
			...(seats < 1 ? { seats } : {}),
			whistle,
			...(gainX !== undefined ? { gainX } : {}),
		});
		this.T = t1;
		this.ballY = this.nextSpot(kind, i0, i1, ballY, spotAt(whistle).y);
	}

	// Crunch time (the 3D court's: see court3d/director.ts tensionNow): a
	// one-score game in the last minutes of the fourth quarter, or in
	// overtime, has the stadium on its feet - more so in the last two.
	tensionOf(quarter: number, left: number, i0: number): number {
		if (quarter < this.numPeriods) {
			return 0;
		}
		const pts = this.pointsTo(i0);
		if (Math.abs(pts[0] - pts[1]) > 8) {
			return 0;
		}
		return left <= 2 ? 1 : left <= 5 || quarter > this.numPeriods ? 0.5 : 0;
	}

	// The seats (the 3D court's: see court3d/director.ts seatsNow): half of
	// them still empty as the second half starts, filling up over its first
	// few minutes; and with the home team down by three scores in the
	// fourth, emptying as the clock runs.
	seatsOf(quarter: number, left: number, i0: number): number {
		const len = this.opens.get(quarter) ?? 15;
		const gone = Math.max(0, len - left);
		let full = 1;
		if (this.numPeriods >= 2 && quarter === this.numPeriods / 2 + 1) {
			full = 0.55 + 0.45 * Math.min(1, gone / (len / 3));
		}
		const pts = this.pointsTo(i0);
		const down = pts[0] - pts[1];
		if (quarter === this.numPeriods && down >= 17) {
			full =
				1 -
				0.45 * Math.min(1, gone / (len * 0.6)) * Math.min(1, (down - 14) / 14);
		}
		return Math.round(full * 20) / 20;
	}

	// The two men off the field with the cooler for the winners' head coach
	// (see shower.ts), if they have it ready - the game won by enough, or
	// knelt out, and not on its last play: big men first.
	showerOf(
		kind: PlayKind,
		pts: [number, number],
		parts: Part[],
		i0: number,
	): [number, number] | undefined {
		const winner: Side | undefined =
			pts[0] > pts[1] ? 0 : pts[1] > pts[0] ? 1 : undefined;
		if (winner === undefined) {
			return undefined;
		}
		const r = this.rng(i0, "shower");
		const won = Math.abs(pts[0] - pts[1]) >= SHOWER_MARGIN || kind === "kneel";
		if (!won || r() >= SHOWER) {
			return undefined;
		}
		const busy = new Set(parts.map((p) => p.pid));
		const roster = new Set(this.roster[winner]);
		const pool: number[] = [];
		for (const g of SHOWER_GROUPS) {
			for (const pid of this.depth[winner][g] ?? []) {
				if (
					roster.has(pid) &&
					!busy.has(pid) &&
					!this.out.has(pid) &&
					!pool.includes(pid)
				) {
					pool.push(pid);
				}
			}
		}
		if (pool.length < 2) {
			return undefined;
		}
		const first = pool.splice(
			Math.floor(r() * Math.min(5, pool.length)),
			1,
		)[0]!;
		const second = pool[Math.floor(r() * Math.min(5, pool.length))]!;
		return [first, second];
	}

	// A side's captains for the coin toss: its quarterback, and a lineman
	// each side of the ball (or the next best) - none out for the kick.
	captainsOf(team: Side, busy: Set<number>): number[] {
		const depth = this.depth[team];
		const out: number[] = [];
		for (const g of CAPTAIN_GROUPS) {
			const pid = (depth[g] ?? []).find(
				(p) => !busy.has(p) && !this.out.has(p) && !out.includes(p),
			);
			if (pid !== undefined) {
				out.push(pid);
			}
			if (out.length === CAPTAINS) {
				break;
			}
		}
		return out;
	}

	// What brings the crowd up in a play: a score, the ball taken away, a
	// kick good, a sack, a long gain or return.
	cheerOf(
		i0: number,
		i1: number,
		offense: Side,
		whistle: number,
		ball: BallSeg[],
		parts: Part[],
		stopped?: "third" | "fourth",
	): Cheer | undefined {
		const { events } = this;
		const other = (1 - offense) as Side;
		const sideAt = (t: number): Side | undefined => {
			const pid = holderAt(ball, t);
			return parts.find((p) => p.pid === pid)?.team;
		};
		let cheer: Cheer | undefined;
		for (const b of this.beats) {
			if (b.i < i0 || b.i >= i1) {
				continue;
			}
			const e = events[b.i]!;
			const t = b.actionStart;
			const yds = typeof e.yds === "number" ? e.yds : 0;
			if (e.td === true && RESULTS.has(e.type)) {
				const at = Math.max(t, whistle);
				return {
					t: at,
					side: sideAt(at) ?? offense,
					big: true,
					what: "touchdown",
				};
			}
			if (e.safety === true && RESULTS.has(e.type)) {
				return {
					t: Math.max(t, whistle),
					side: other,
					big: true,
					what: "safety",
				};
			}
			if ((e.type === "fieldGoal" || e.type === "extraPoint") && e.made) {
				cheer ??=
					e.type === "fieldGoal"
						? { t, side: offense, big: true, what: "fieldGoal" }
						: { t, side: offense, big: false };
			} else if (e.type === "interception") {
				cheer ??= { t, side: other, big: true, what: "turnover" };
			} else if (e.type === "fumbleRecovery" && e.lost === true) {
				cheer ??= {
					t,
					side: sideAt(t + 1) ?? other,
					big: true,
					what: "turnover",
				};
			} else if (e.type === "sack") {
				cheer ??= { t: whistle, side: other, big: false, what: "sack" };
			} else if ((e.type === "run" || e.type === "passComplete") && yds >= 20) {
				cheer ??= { t: whistle, side: offense, big: false };
			} else if (
				(e.type === "kickoffReturn" || e.type === "puntReturn") &&
				yds >= 35
			) {
				cheer ??= { t: whistle, side: other, big: false };
			}
		}
		// (Stopped short: the defense's.)
		if (stopped) {
			cheer ??= {
				t: whistle,
				side: other,
				big: stopped === "fourth",
				what: "stop",
			};
		}
		return cheer;
	}

	// Across the field, where the next snap is spotted: where the ball was
	// dead (`endY`), brought in to the nearer hash from outside them.
	nextSpot(
		kind: PlayKind,
		i0: number,
		i1: number,
		ballY: number,
		endY: number,
	): number {
		let moved = false;
		for (let i = i0; i < i1; i++) {
			const e = this.events[i];
			if (
				e?.type === "passComplete" ||
				e?.type === "run" ||
				e?.type === "sack" ||
				e?.type === "interceptionReturn" ||
				e?.type === "fumbleRecovery" ||
				e?.type === "kickoffReturn" ||
				e?.type === "puntReturn" ||
				e?.type === "onsideKickRecovery"
			) {
				moved = !e.touchback;
			}
			if (e?.type === "penalty" && e.decision === "accept" && !e.tackOn) {
				// (Wiped out: from where it was.)
				moved = false;
			}
		}
		if (kind === "kickoff" || kind === "punt") {
			// A touchback: out to the middle.
			return moved ? clampY(endY, HASH_FAR_Y, HASH_NEAR_Y) : MID_Y;
		}
		if (kind === "extraPoint" || kind === "fieldGoal") {
			return MID_Y;
		}
		return moved ? clampY(endY, HASH_FAR_Y, HASH_NEAR_Y) : ballY;
	}

	// The eleven a side for a play, aligned.
	lineUp(
		kind: PlayKind,
		events: RawEvent[],
		i0: number,
		i1: number,
		offense: Side,
		dir: Dir,
		losX: number,
		ballY: number,
		clock: RawEvent,
		named: [Named[], Named[]],
		r: () => number,
	): { placed: Placed[]; formation?: OffenseSetName } {
		const defense = (1 - offense) as Side;
		const down: number = typeof clock.down === "number" ? clock.down : 1;
		const toGo: number = typeof clock.toGo === "number" ? clock.toGo : 10;
		const scrimmage: number =
			typeof clock.scrimmage === "number" ? clock.scrimmage : 25;
		let off: Align[];
		let def: Align[] | undefined;
		let formation: OffenseSetName | undefined;
		let flip = false;
		const event = (type: string) => {
			for (let i = i0; i < i1; i++) {
				if (events[i]?.type === type) {
					return events[i];
				}
			}
			return undefined;
		};
		if (kind === "kickoff" || kind === "onside") {
			const onside = kind === "onside";
			off = KICKOFF_COVER(onside ? -3 : 75);
			def = onside ? ONSIDE_RECEIVE : KICKOFF_RECEIVE(90, 180);
		} else if (kind === "punt") {
			const punt = event("punt");
			const room = 100 - scrimmage;
			const yds = typeof punt?.yds === "number" ? punt.yds : 42;
			const back =
				Math.max(Math.min(30, room - 1), Math.min(yds, room - 1)) * 3;
			off = PUNT_TEAM;
			def = PUNT_RETURN(back);
		} else if (kind === "fieldGoal" || kind === "extraPoint") {
			off = KICK_TEAM;
			def = KICK_BLOCK;
		} else {
			const running =
				kind === "run" ||
				(kind === "none" && r() < 0.45) ||
				event("handoff") !== undefined;
			formation =
				kind === "kneel"
					? "victory"
					: this.chooseSet(
							{ running, down, toGo, scrimmage },
							named[offense],
							offense,
							r(),
						);
			const set: OffenseSet = OFFENSE_SETS[formation];
			off = set.slots;
			// The strength to the wide side of the field, mostly.
			const roomRight = dir === 1 ? FIELD_W - ballY : ballY;
			const fieldRight = roomRight >= FIELD_W - roomRight;
			const boundary = r() < 0.28;
			flip = fieldRight ? boundary : !boundary;
		}

		const placed: Placed[] = [];
		const offsets: Offset[] = [];
		const offPids = this.fill(off, offense, named[offense]);
		for (const [k, a] of off.entries()) {
			const pid = offPids[k];
			const across = acrossOf(a, dir, ballY, flip);
			offsets.push({ role: a.role, pos: a.pos, across, depth: a.depth });
			if (pid === undefined) {
				continue;
			}
			// (The center's hand on the ball the same whichever way the
			// formation is flipped.)
			const snaps = a.role === "C" || a.role === "LS";
			const mirrored = !snaps && (a.mirrored === true) !== flip;
			placed.push({
				pid,
				team: offense,
				role: a.role,
				at: {
					x: clampX(losX - dir * a.depth),
					y: clampY(ballY + across * dir),
				},
				yaw: facing(dir),
				move: a.move,
				...(mirrored ? { mirrored: true } : {}),
				...(snaps ? { ball: "snap" as const } : {}),
			});
		}
		if (!def) {
			const set = OFFENSE_SETS[formation!];
			const call = chooseDefenseCall(set, { down, toGo, scrimmage }, r());
			const hashes = HASHES.map((y) => (y - ballY) * dir).sort(
				(a, b) => a - b,
			) as [number, number];
			def = alignDefense(offsets, call, hashes);
		}
		const defPids = this.fill(def, defense, named[defense]);
		for (const [k, a] of def.entries()) {
			const pid = defPids[k];
			if (pid === undefined) {
				continue;
			}
			const across = acrossOf(a, dir, ballY, false);
			placed.push({
				pid,
				team: defense,
				role: a.role,
				at: {
					x: clampX(losX + dir * a.depth),
					y: clampY(ballY + across * dir),
				},
				yaw: facing(-dir as Dir),
				move: a.move,
				...(a.mirrored ? { mirrored: true } : {}),
			});
		}
		return { placed, ...(formation ? { formation } : {}) };
	}

	// The offense's set for a snap: the situation's call - unless the men
	// the play names could not all be on the field in it (two backs, two
	// tight ends), when it is one they could.
	chooseSet(
		situation: {
			running: boolean;
			down: number;
			toGo: number;
			scrimmage: number;
		},
		named: Named[],
		side: Side,
		u: number,
	): OffenseSetName {
		const pick = chooseOffenseSet(situation, u);
		const depth = this.depth[side];
		const need: Partial<Record<Group, number>> = {};
		for (const n of named) {
			const gs = n.groups?.filter(
				(g) => g === "RB" || g === "TE" || g === "WR",
			);
			if (!gs || gs.length === 0) {
				continue;
			}
			// (Where he is highest on the chart is where he played.)
			let best: Group | undefined;
			let bestRank = Infinity;
			for (const g of n.groups!) {
				const rk = depth[g]?.indexOf(n.pid) ?? -1;
				if (rk >= 0 && rk < bestRank) {
					best = g;
					bestRank = rk;
				}
			}
			if (best === "RB" || best === "TE" || best === "WR") {
				need[best] = (need[best] ?? 0) + 1;
			}
		}
		const fits = (name: OffenseSetName) => {
			const slots = OFFENSE_SETS[name].slots;
			return (["RB", "TE", "WR"] as const).every(
				(g) => slots.filter((s) => s.pos === g).length >= (need[g] ?? 0),
			);
		};
		if (fits(pick)) {
			return pick;
		}
		const other = (Object.keys(OFFENSE_SETS) as OffenseSetName[]).find(
			(name) => name !== "victory" && fits(name),
		);
		return other ?? pick;
	}

	fill(slots: Align[], side: Side, named: Named[]): (number | undefined)[] {
		return fillSlots(
			slots.map((s) => ({ group: s.pos })),
			this.depth[side],
			named.map((n) => ({ pid: n.pid, groups: n.groups })),
			this.out,
			this.roster[side],
		);
	}

	// Up out of the huddles and set: each man from his place in his
	// huddle - or, a long way out, from partway out to his spot already -
	// at a walk or an easy jog, and down into his stance when he gets there.
	// When everybody on the offense is.
	walkUp(
		placed: Placed[],
		parts: Part[],
		offense: Side,
		dir: Dir,
		losX: number,
		ballY: number,
		t0: number,
		r: () => number,
	): number {
		let set = t0;
		for (const team of [offense, 1 - offense] as Side[]) {
			const men = placed.filter((p) => p.team === team);
			const isOffense = team === offense;
			const center: Pt = {
				x: clampX(
					losX - dir * (isOffense ? HUDDLE_BACK : -DEF_HUDDLE_BACK),
					12,
				),
				y: clampY(ballY, 50, FIELD_W - 50),
			};
			const radius = isOffense ? 5.5 : 7.5;
			// Round the huddle in the order of where each is going, so nobody
			// crosses anybody on the way out.
			const angleTo = (p: Pt) => Math.atan2(p.y - center.y, p.x - center.x);
			const order = [...men].sort((a, b) => angleTo(a.at) - angleTo(b.at));
			const slots = order.map((_, k) => {
				const a =
					angleTo(order[0]!.at) + (k / Math.max(1, order.length)) * Math.PI * 2;
				return {
					x: center.x + Math.cos(a) * radius,
					y: center.y + Math.sin(a) * radius,
				};
			});
			ringOut(
				slots,
				order.map((p) => p.at),
			);
			for (const [k, p] of order.entries()) {
				const ring = slots[k]!;
				const far = Math.hypot(p.at.x - ring.x, p.at.y - ring.y);
				const keep = far > WALK_UP_MAX ? 1 - WALK_UP_MAX / far : 0;
				const from = {
					x: ring.x + (p.at.x - ring.x) * keep,
					y: ring.y + (p.at.y - ring.y) * keep,
				};
				const d = Math.hypot(p.at.x - from.x, p.at.y - from.y);
				const jog = d > WALK_ONLY;
				const late = isOffense ? 0 : DEF_BREAK_LATE;
				const leave = t0 + BREAK_AT + BREAK_SPREAD * r() + late;
				const fps = jog ? JOG_FPS : WALK_FPS;
				const arrive = leave + (d / fps) * 1000 + goingMs(fps);
				const look = Math.atan2(center.y - from.y, center.x - from.x);
				// (In the huddle: the clap, all at once, and out of it.)
				const clap = t0 + CLAP_AT + late;
				parts.push({
					pid: p.pid,
					team: p.team,
					role: p.role,
					legs: [
						...(keep > 0
							? [
									{
										t0,
										t1: leave,
										from,
										to: from,
										move: "stand" as const,
										yaw: p.yaw,
									},
								]
							: [
									{
										t0,
										t1: clap,
										from,
										to: from,
										move: "hands" as const,
										yaw: look,
									},
									{
										t0: clap,
										t1: leave,
										from,
										to: from,
										move: "huddleBreak" as const,
										yaw: look,
									},
								]),
						...intoStance(from, p.at, leave, arrive, jog ? "jog" : "walk"),
						{
							t0: arrive,
							t1: arrive,
							from: p.at,
							to: p.at,
							move: p.move,
							yaw: p.yaw,
							...(p.mirrored ? { mirrored: true } : {}),
							...(p.ball ? { ball: p.ball } : {}),
						},
					],
				});
				if (isOffense) {
					set = Math.max(set, arrive);
				}
			}
		}
		return set + SET_HOLD;
	}

	// No huddle: each man from about where the last play left him - the
	// offense back up from downfield, the line and the backs up from behind
	// it, the defense close by - hurried to his spot and down into his
	// stance. When everybody on the offense is.
	hurryUp(
		placed: Placed[],
		parts: Part[],
		offense: Side,
		dir: Dir,
		t0: number,
		r: () => number,
	): number {
		let set = t0;
		for (const p of placed) {
			const isOffense = p.team === offense;
			const out = isOffense && HURRY_OUT.has(p.role);
			const back = isOffense
				? out
					? -(HURRY_DOWNFIELD[0] + r() * HURRY_DOWNFIELD[1])
					: HURRY_BEHIND[0] + r() * HURRY_BEHIND[1]
				: -(HURRY_DEFENSE * (r() - 0.3));
			const from = {
				x: clampX(p.at.x - dir * back, 4),
				y: clampY(p.at.y + (r() - 0.5) * 2 * HURRY_ACROSS, 4, FIELD_W - 4),
			};
			const d = Math.hypot(p.at.x - from.x, p.at.y - from.y);
			const leave = t0 + r() * HURRY_SPREAD;
			const fps = Math.min(
				HURRY_FPS[0] + r() * HURRY_FPS[1],
				Math.max(SET_IN_FPS, Math.sqrt(HURRY_EASE * d) / 2),
			);
			const arrive = leave + (d / fps) * 1000 + goingMs(fps);
			parts.push({
				pid: p.pid,
				team: p.team,
				role: p.role,
				legs: [
					{
						t0,
						t1: leave,
						from,
						to: from,
						move: "stand",
						yaw: Math.atan2(p.at.y - from.y, p.at.x - from.x),
					},
					...intoStance(
						from,
						p.at,
						leave,
						arrive,
						fps < 6 ? "walk" : fps < 12.5 ? "jog" : "run",
					),
					{
						t0: arrive,
						t1: arrive,
						from: p.at,
						to: p.at,
						move: p.move,
						yaw: p.yaw,
						...(p.mirrored ? { mirrored: true } : {}),
						...(p.ball ? { ball: p.ball } : {}),
					},
				],
			});
			if (isOffense) {
				set = Math.max(set, arrive);
			}
		}
		return set + SET_HOLD;
	}

	// THE QUARTERBACK AT THE LINE: under center, stood up behind him first
	// to read the defense - his head round one way and the other, now and
	// then an arm out at the man he picks out - and down under him for the
	// ball; in the gun, the same look round and a clap for it - from as
	// soon as he is set, with the picture in close on him for it.
	cadence(
		parts: Part[],
		offense: Side,
		snap: number,
		i0: number,
		close: boolean,
	) {
		const qb = parts.find((p) => p.team === offense && p.role === "QB");
		const set = qb?.legs.at(-1);
		if (!qb || !set || set.t1 > set.t0) {
			return;
		}
		const r = this.rng(i0, "cadence");
		const vision = this.flair.get(qb.pid)?.vision;
		const point =
			vision === undefined
				? POINT
				: POINT_LOW + POINT_SPAN * ((vision + 1) / 2);
		const at = set.to;
		const still = (t: number, move: Move): Leg => ({
			t0: t,
			t1: t,
			from: at,
			to: at,
			move,
			yaw: set.yaw,
		});
		const act = (t0: number, t1: number, move: Move): Leg => ({
			...still(t0, move),
			t1,
		});
		if (set.move === "underCenter") {
			const down = snap - UNDER_AT;
			const up = qb.legs.at(-2);
			if (down - set.t0 < SURVEY_MIN || !up) {
				return;
			}
			// (A step back off him while he stands.)
			const yaw = set.yaw ?? 0;
			const back = {
				x: at.x - Math.cos(yaw) * SURVEY_BACK,
				y: at.y - Math.sin(yaw) * SURVEY_BACK,
			};
			// (Walking in to it, all of that a step back too.)
			const jog = qb.legs.at(-3);
			if (
				up.move === "walk" &&
				jog &&
				jog.to.x === up.from.x &&
				jog.to.y === up.from.y
			) {
				up.from = {
					x: up.from.x + back.x - at.x,
					y: up.from.y + back.y - at.y,
				};
				jog.to = up.from;
			}
			up.to = back;
			qb.legs.pop();
			qb.legs.push(
				{
					...act(set.t0, down, r() < point ? "qbPoint" : "qbSurvey"),
					from: back,
					to: back,
				},
				{ ...act(down, down + GET_UNDER_MS, "getUnder"), from: back },
				still(down + GET_UNDER_MS, "underCenter"),
			);
		} else if (set.move === "shotgun") {
			const from = close
				? set.t0 + 150
				: Math.max(set.t0 + 150, snap - GUN_CALL_MS);
			if (snap - from < GUN_CALL_MIN) {
				return;
			}
			set.t1 = from;
			qb.legs.push(
				act(
					from,
					snap - 40,
					vision === undefined || r() < point ? "gunCall" : "gunLook",
				),
				still(snap - 40, "shotgun"),
			);
		}
	}

	// THE KICKER'S ROUTINE, before a kick at the posts: out of the huddle up
	// to just behind the spot the ball goes down on, a look at the posts,
	// his steps back off it and over to the side, and set - the snap no
	// sooner than ROUTINE_SET_MS after. When the snap is, then; and, on most
	// field goals, the picture in close on him through it (see aim.ts).
	kickerRoutine(
		parts: Part[],
		offense: Side,
		dir: Dir,
		snap: number,
		i0: number,
		kind: PlayKind,
	):
		| {
				snap: number;
				closeUp?: { t0: number; t1: number; pid: number; at: Pt };
		  }
		| undefined {
		const k = parts.find((p) => p.team === offense && p.role === "K");
		const h = parts.find((p) => p.team === offense && p.role === "H");
		const set = k?.legs.at(-1);
		const held = h?.legs.at(-1)?.to;
		// (Out of the huddle - stood, clapped and broken - and up to his spot.)
		const j = k
			? k.legs.findLastIndex(
					(l) => l !== set && l.from.x === l.to.x && l.from.y === l.to.y,
				)
			: -1;
		const left = k?.legs[j];
		if (
			!k ||
			!set ||
			!held ||
			!left ||
			set.t1 > set.t0 ||
			set.move !== "kickReady"
		) {
			return undefined;
		}
		const r = this.rng(i0, "routine");
		const yaw = facing(dir);
		// (Where the holder puts it down: see action.ts fieldGoal.)
		const spot = { x: held.x + dir * 0.7, y: held.y - dir * 1.7 };
		const at = set.to;
		const A = { x: spot.x - dir * ROUTINE_BEHIND, y: spot.y };
		const B = { x: at.x, y: spot.y };
		const from = left.to;
		const leave = left.t1;
		const arrive =
			leave + (dist(from, A) / WALK_FPS) * 1000 + goingMs(WALK_FPS);
		const looked = arrive + ROUTINE_LOOK + r() * ROUTINE_LOOK_MORE;
		const backed = looked + (dist(A, B) / ROUTINE_FPS) * 1000;
		const over = backed + ROUTINE_PAUSE;
		const sets = over + (dist(B, at) / ROUTINE_FPS) * 1000;
		const still = (t0: number, t1: number, p: Pt, move: Move): Leg => ({
			t0,
			t1,
			from: p,
			to: p,
			move,
			yaw,
		});
		k.legs = [
			...k.legs.slice(0, j + 1),
			...intoStance(from, A, leave, arrive, "walk"),
			still(arrive, looked, A, "stand"),
			{ t0: looked, t1: backed, from: A, to: B, move: "kickStep", yaw },
			still(backed, over, B, "stand"),
			{ t0: over, t1: sets, from: B, to: at, move: "kickStep", yaw },
			{ ...set, t0: sets, t1: sets },
		];
		const after = Math.max(snap, sets + ROUTINE_SET_MS);
		const close =
			this.rng(i0, "kickClose")() <
			(kind === "fieldGoal" ? KICK_CLOSE : KICK_CLOSE_XP);
		return {
			snap: after,
			...(close
				? {
						closeUp: {
							t0: arrive - KICK_CLOSE_BEFORE,
							t1: sets + KICK_CLOSE_AFTER,
							pid: k.pid,
							at,
						},
					}
				: {}),
		};
	}

	// THE PUNTER, back for the snap: stood up waiting, and his hands out in
	// front of him for it - the snapper's target - for the last of it.
	punterSet(parts: Part[], offense: Side, snap: number) {
		const p = parts.find((q) => q.team === offense && q.role === "P");
		const set = p?.legs.at(-1);
		const out = snap - PUNT_HANDS_MS;
		if (
			!p ||
			!set ||
			set.t1 > set.t0 ||
			set.move !== "shotgun" ||
			out - set.t0 < PUNT_HANDS_MS / 2
		) {
			return;
		}
		set.t1 = out;
		set.move = "stand";
		p.legs.push(
			{ ...set, t0: out, t1: out + PUNT_HANDS_IN, move: "puntHands" },
			{
				...set,
				t0: out + PUNT_HANDS_IN,
				t1: out + PUNT_HANDS_IN,
				move: "puntReady",
			},
		);
	}

	// PRE-SNAP MOTION, on a pass play now and then: a receiver off the line
	// set first somewhere else - wider out, or in tighter - and on his way
	// across to his spot as the ball is snapped; and, in man coverage, the
	// man over him going along with him. When the snap is, then.
	motion(
		parts: Part[],
		offense: Side,
		dir: Dir,
		losX: number,
		ballY: number,
		set: number,
		snap: number,
		i0: number,
	): number {
		const r = this.rng(i0, "motion");
		if (r() >= MOTION) {
			return snap;
		}
		const depthOf = (p: Pt) => (losX - p.x) * dir;
		const acrossOf = (p: Pt) => (p.y - ballY) * dir;
		const spot = (part: Part) => part.legs.at(-1)!.to;
		const offenseParts = parts.filter((p) => p.team === offense);
		const movers = offenseParts.filter(
			(p) => MOTION_ROLES.has(p.role) && walkedUp(p) && depthOf(spot(p)) > 3,
		);
		const him = movers[Math.floor(r() * movers.length)];
		if (!him) {
			return snap;
		}
		const b = spot(him);
		const aB = acrossOf(b);
		const out = Math.sign(aB) || 1;
		const way = r() < 0.5 ? 1 : -1;
		const shift =
			out * way * (MOTION_FT[0] + r() * (MOTION_FT[1] - MOTION_FT[0]));
		const a: Pt = { x: b.x, y: b.y + shift * dir };
		// (Not in among the line, not off the field, nobody else in the way.)
		const aA = acrossOf(a);
		if (
			Math.abs(aA) < MOTION_INSIDE ||
			Math.sign(aA) !== out ||
			a.y < MOTION_EDGE ||
			a.y > FIELD_W - MOTION_EDGE ||
			offenseParts.some((p) => {
				if (p === him) {
					return false;
				}
				const q = spot(p);
				const lo = Math.min(aA, aB) - 2.5;
				const hi = Math.max(aA, aB) + 2.5;
				return (
					Math.abs(depthOf(q) - depthOf(b)) < 2.5 &&
					acrossOf(q) > lo &&
					acrossOf(q) < hi
				);
			})
		) {
			return snap;
		}
		const fps = MOTION_FPS[0] + r() * (MOTION_FPS[1] - MOTION_FPS[0]);
		const takes = (Math.abs(shift) / fps) * 1000;
		// Set over there first, the way he would be at his spot.
		const n = him.legs.length;
		const upTo = him.legs[n - 2]!;
		const stance = him.legs[n - 1]!;
		const there =
			upTo.t0 + (dist(upTo.from, a) / (upTo.move === "jog" ? 9 : 4.6)) * 1000;
		const go = Math.max(set + 300, there + 400, snap - takes);
		const end = go + takes;
		upTo.to = a;
		upTo.t1 = there;
		him.legs[n - 1] = { ...stance, t0: there, t1: go, from: a, to: a };
		him.legs.push({
			t0: go,
			t1: end,
			from: a,
			to: b,
			move: fps < 12 ? "jog" : "run",
		});
		// The man over him, in man, with him.
		const over = parts
			.filter(
				(p) =>
					p.team !== offense &&
					walkedUp(p) &&
					Math.abs(acrossOf(spot(p)) - aB) < 9 &&
					depthOf(spot(p)) < -2 &&
					depthOf(spot(p)) > -30,
			)
			.sort(
				(p, q) =>
					Math.abs(acrossOf(spot(p)) - aB) - Math.abs(acrossOf(spot(q)) - aB),
			)[0];
		if (over && r() < MAN_FOLLOWS) {
			const k = over.legs.length;
			const up = over.legs[k - 2]!;
			const st = over.legs[k - 1]!;
			const d0 = spot(over);
			const d1 = { x: d0.x, y: d0.y + shift * dir };
			if (d1.y > MOTION_EDGE && d1.y < FIELD_W - MOTION_EDGE) {
				const thereD =
					up.t0 + (dist(up.from, d1) / (up.move === "jog" ? 9 : 4.6)) * 1000;
				const goD = Math.max(go + 120, thereD + 200);
				up.to = d1;
				up.t1 = thereD;
				over.legs[k - 1] = { ...st, t0: thereD, t1: goD, from: d1, to: d1 };
				// (Shuffled across with him - or, a man going across quicker
				// than that, turned and run with him.)
				const fpsD = dist(d1, d0) / Math.max(0.001, (end + 100 - goD) / 1000);
				over.legs.push(
					fpsD > FOLLOW_SLIDE_FPS
						? {
								t0: goD,
								t1: end + 100,
								from: d1,
								to: d0,
								move: fpsD < 12 ? "jog" : "run",
							}
						: {
								t0: goD,
								t1: end + 100,
								from: d1,
								to: d0,
								move: "slide",
								...(st.yaw !== undefined ? { yaw: st.yaw } : {}),
							},
				);
			}
		}
		return Math.max(snap, end - IN_MOTION);
	}

	// THE LINE DOWN TOGETHER: up to the line and over it, hands on his
	// thighs, as the quarterback looks the defense over - and down into his
	// stance with the rest of them a beat before the snap, the men across
	// from them down after them; not down in it from the moment he gets
	// there, stock still for two seconds and more. The backs and the men
	// split out the same, stood up till then; the corners up on their toes.
	lineDown(parts: Part[], offense: Side, snap: number, i0: number) {
		const r = this.rng(i0, "line");
		const down =
			snap - LINE_DOWN_MS[0] - r() * (LINE_DOWN_MS[1] - LINE_DOWN_MS[0]);
		for (const p of parts) {
			const st = p.legs.at(-1);
			const first = st && UP_FIRST.get(st.move);
			if (!st || !first || !walkedUp(p)) {
				continue;
			}
			const line = st.move === "threePoint" || st.move === "dlThree";
			const t =
				p.team === offense
					? down + r() * (line ? LINE_EACH_MS : LINE_SKILL_MS)
					: down + LINE_DL_MS[0] + r() * (LINE_DL_MS[1] - LINE_DL_MS[0]);
			const move = first[Math.floor(r() * first.length)]!;
			if (t - st.t0 < LINE_UP_LEAST || t >= snap) {
				continue;
			}
			// (Square over it either way: one picture of it, not two.)
			const up: Leg = { ...st, t1: t, move };
			delete up.mirrored;
			p.legs.splice(-1, 0, up);
			st.t0 = t;
			st.t1 = t;
		}
	}

	// THE DEFENSE WAITING ON THE SNAP: not stood stock still - the middle
	// linebacker turned round one way and the other making the call, an arm
	// out at the strength; a linebacker now and then over a step, or up
	// into a gap showing blitz and back out of it as the ball is snapped;
	// and now and then, of two safeties deep, one creeping down late.
	defenseSet(
		parts: Part[],
		offense: Side,
		dir: Dir,
		losX: number,
		ballY: number,
		snap: number,
		i0: number,
	) {
		const r = this.rng(i0, "defense");
		const depthOf = (p: Pt) => (losX - p.x) * dir;
		const acrossOf = (p: Pt) => (p.y - ballY) * dir;
		const defense = parts.filter((p) => p.team !== offense && walkedUp(p));
		// (Stood in his stance from when he got there till t, then over to
		// `to` by t1 doing `move` - and set there.)
		// (Into it at the last at `slow`, if he is going at all quick - see
		// intoStance.)
		const over = (
			p: Part,
			t: number,
			t1: number,
			to: Pt,
			move: Move,
			slow?: Move,
		) => {
			const st = p.legs.at(-1)!;
			st.t1 = t;
			const legs = slow
				? intoStance(st.to, to, t, t1, move, slow)
				: [{ t0: t, t1, from: st.to, to, move }];
			p.legs.push(
				...legs.map((l) => {
					const c = { ...st, ...l };
					if (walkingUp.has(l)) {
						walkingUp.add(c);
					}
					return c;
				}),
				{
					...st,
					t0: t1,
					t1,
					from: to,
					to,
				},
			);
		};
		const lbs = defense
			.filter((p) => LBS.has(p.role))
			.sort(
				(p, q) =>
					Math.abs(acrossOf(p.legs.at(-1)!.to)) -
					Math.abs(acrossOf(q.legs.at(-1)!.to)),
			);
		const mike = lbs.find((p) => p.role === "MIKE") ?? lbs[0];
		let called = false;
		if (mike && r() < DEF_CALL) {
			const st = mike.legs.at(-1)!;
			const t1 = snap - DEF_CALL_BEFORE;
			const t0 = Math.max(st.t0 + 150, t1 - DEF_CALL_MS);
			if (
				t1 - t0 >= DEF_CALL_MIN &&
				(st.move === "backerStance" || st.move === "backerReady")
			) {
				st.t1 = t0;
				mike.legs.push(
					{ ...st, t0, t1, move: "backerCall", mirrored: r() < 0.5 },
					{ ...st, t0: t1, t1, mirrored: false },
				);
				called = true;
			}
		}
		for (const lb of lbs) {
			const st = lb.legs.at(-1)!;
			const room = snap - DEF_CALL_BEFORE - st.t0;
			if (lb === mike && called) {
				continue;
			}
			const p = st.to;
			// (Up into the gap nearest him, just off the men down in it.)
			const a = acrossOf(p);
			const line = defense
				.filter((q) => DL_ROLES.has(q.role))
				.map((q) => acrossOf(q.legs.at(-1)!.to))
				.sort((x, y) => x - y);
			const gaps = line.slice(1).map((x, k) => (x + line[k]!) / 2);
			const gapA =
				gaps.sort((x, y) => Math.abs(x - a) - Math.abs(y - a))[0] ?? a / 2;
			const gap = { x: losX + dir * MUG_D, y: ballY + dir * gapA };
			const far = Math.hypot(gap.x - p.x, gap.y - p.y);
			const ms = (far / MUG_FPS) * 1000;
			const set = snap - MUG_SET;
			const back = set - Math.max(MUG_BACK, (far / MUG_BACK_FPS) * 1000);
			const latest = back - 300 - ms;
			if (r() < MUG && latest > st.t0 + 200) {
				// Showing blitz - and back out of it as the ball goes.
				const t = st.t0 + 200 + r() * (latest - st.t0 - 200);
				over(lb, t, t + ms, gap, "walk", "walk");
				over(lb, back, set, p, "back", "back");
			} else if (r() < SHUFFLE && room > 900) {
				const ft = SHUFFLE_FT[0] + r() * (SHUFFLE_FT[1] - SHUFFLE_FT[0]);
				const to = { x: p.x, y: p.y + (r() < 0.5 ? 1 : -1) * ft };
				const t = st.t0 + 200 + r() * (room - 800);
				over(lb, t, t + 160 + ft * 110, to, "slide");
			}
		}
		// A safety down late.
		const deep = defense.filter(
			(p) =>
				(p.role === "FS" || p.role === "SS") &&
				depthOf(p.legs.at(-1)!.to) < -CREEP_D - 6,
		);
		const down = deep.find((p) => p.role === "SS");
		if (deep.length === 2 && down && r() < CREEP) {
			const st = down.legs.at(-1)!;
			const p = st.to;
			const to = { x: losX - dir * -CREEP_D, y: p.y + (ballY - p.y) * 0.25 };
			const far = Math.hypot(to.x - p.x, to.y - p.y);
			const set = snap - MUG_SET;
			const t = Math.max(
				st.t0 + 200,
				set - Math.max(CREEP_MS, (far / CREEP_FPS) * 1000),
			);
			const fps = far / ((set - t) / 1000);
			if (set - t > 550) {
				over(down, t, set, to, fps < 6.5 ? "walk" : "jog", "walk");
			}
		}
	}
}

// A man a play's stats name: the groups he could have been playing in (any,
// unsaid), and how sure that says he had a hand in it (0 the surest).
type Named = {
	pid: number;
	side: Side;
	groups: Group[] | undefined;
	rank: number;
};

export const compileField = (input: FieldInput): FieldTimeline =>
	new Director(input).run();

// ---- playback --------------------------------------------------------------

// Where the animation should stand while the playback cursor (events
// consumed) is at `cursor`: the moment the next unshown line happens. Past
// the last line, the end of the game.
export const targetForCursor = (tl: FieldTimeline, cursor: number): number => {
	const beat = tl.beats.find((b) => b.i >= cursor);
	return beat ? beat.actionStart : tl.end;
};

// Where to cut to when playback jumps (a rewind, a fast-forward, joining a
// broadcast late): the end of the last line already shown.
export const snapForCursor = (tl: FieldTimeline, cursor: number): number => {
	let t = 0;
	for (const b of tl.beats) {
		if (b.i >= cursor) {
			break;
		}
		t = b.end;
	}
	return t;
};

// How many play-by-play lines the page showed going from one cursor to
// another.
export const linesBetween = (
	tl: FieldTimeline,
	from: number,
	to: number,
): number => {
	let n = 0;
	for (const b of tl.beats) {
		if (b.i >= to) {
			break;
		}
		if (b.i >= from) {
			n += 1;
		}
	}
	return n;
};
