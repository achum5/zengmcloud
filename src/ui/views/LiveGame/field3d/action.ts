import { elusiveness, type FieldRatings, type Flair } from "./ratings.ts";
import type {
	BallHold,
	BallSeg,
	Leg,
	Part,
	PlayKind,
	RawEvent,
	StatLine,
} from "./director.ts";
import type { OffenseSet, Role } from "./formations.ts";
import {
	CROSSBAR_Z,
	dist,
	END_ZONE,
	FIELD_L,
	FIELD_W,
	GOAL_L,
	GOAL_R,
	MID_X,
	MID_Y,
	NET_BACK,
	POST_W,
	sideOn,
	TEAM_AREA,
	YD,
	type Dir,
	type Pt,
	type Pt3,
	type Side,
} from "./geometry.ts";
import { drawnBody, footballBody } from "./body.ts";
import { touching } from "./apart.ts";
import { fallHurt, HURT_MOST, offHurt } from "./hurt.ts";
import {
	CLASP_HELP_AT,
	CLASP_UP_AT,
	HIGH_FIVE_AT,
	PAT_AT,
	REACH_BACK_AT,
	handOf,
	handWith,
	handsMeet,
	heldBall,
	liftOf,
	moveKind,
	poseOfMove,
	tucked,
	type Move,
} from "./poses.ts";
import { carriedIn, carriedTo, SET } from "./pace.ts";
import { sidelineOf } from "./sideline.ts";
import {
	alongRoute,
	decoyRoute,
	layRoute,
	targetRoute,
	toTheLine,
	type Bounds,
	type OPt,
	type Route,
	type RouteName,
	type Spot,
} from "./routes.ts";

// THE PLAY ITSELF: from the snap to the whistle, everything the sim says
// happened, acted out - the snap back to the quarterback, the line's blocks
// and the rush, the handoff and the back to his hole, the routes and the
// coverage on them, the throw, the catch, the run after it, the tackle -
// with the ball where the sim says it ended up, carried by the men it names,
// brought down by the ones it says made the tackle.
//
// Everything is staged in the offense's frame (see routes.ts): `d` feet
// downfield of the line of scrimmage, `a` feet across to its right.

export type ActInput = {
	kind: PlayKind;
	offense: Side;
	dir: Dir;
	losX: number;
	ballY: number;
	snap: number;
	set: OffenseSet | undefined;
	parts: Part[];
	events: RawEvent[];
	// Who did what in it (see Director.statsOf).
	stats: StatLine[];
	// The play's events: from its "clock" line up to the next one.
	i0: number;
	i1: number;
	// A new set of downs got by it (not by a flag) - or the offense stopped
	// short on third down, or on fourth and the ball over on downs.
	firstDown?: boolean;
	stopped?: "third" | "fourth";
	// ...stopped on third, and a punt or a field goal next.
	kicksNext?: boolean;
	// The man with it got out of bounds (see the director).
	out?: boolean;
	// The down, and the yards to go for a first down (where the sim says).
	down?: number;
	toGo?: number;
	// Each man's speed against the others at his position, and his ratings,
	// where the game has them (see ratings.ts).
	speeds?: Map<number, number>;
	rated?: Map<number, FieldRatings>;
	flair?: Map<number, Flair>;
	// How worn the defense is, out there a long drive (0 to 1: see the
	// director's WINDED_FROM).
	winded?: number;
	// The game in crunch time (1 the last two minutes of a close one, see
	// the director's tensionOf): the big moments let out more.
	tense?: number;
	r: () => number;
	// The play's own number, 0 to 1, for each man's counts on it (see
	// countOf).
	key: number;
	// Who a name in a line is, on a side (a flag the sim gives no stat line
	// for - declined - names the man).
	pidOf?: (side: Side, name: string) => number | undefined;
};

export type Acted = {
	// When the lines the play itself times happen (by event index).
	at: Map<number, number>;
	// Where the ball goes from the snap on.
	ball: BallSeg[];
	// The ball dead: the tackle, the incompletion, the score.
	whistle: number;
	// A kick: when it is away.
	follow?: number;
	// A big play's moment after it (see celebrate): when it is over - and
	// when it starts, where, and the man it is about (if one).
	until?: number;
	party?: { from: number; at: Pt; hero?: number };
	// Picked off: the quarterback's look after it (see regroup) - who, from
	// when, until when.
	qbRue?: { pid: number; t0: number; t1: number };
	// Where the man with it went out of bounds.
	out?: Pt;
};

// Where a carry ends: the spot, how far downfield, a score - or over a
// sideline (+1 the one on the offense's +a side).
type Sign = 1 | -1;
type End = { pt: Pt; td: boolean; d: number; out?: Sign };
// A pass knocked down at the line (see batAt): who, where it comes off his
// hands and when, and how high the throw is arched on its way there.
type Bat = { m: Actor; at: Pt3; t: number; peak: number };

// ---- who is who ------------------------------------------------------------

const OL = new Set<Role>(["C", "LG", "RG", "LT", "RT"]);
const DL = new Set<Role>(["DE", "DT", "NT"]);
const LB = new Set<Role>(["SAM", "MIKE", "WILL", "BUCK"]);
const DB = new Set<Role>(["CB", "NB", "FS", "SS"]);
const RECEIVERS = new Set<Role>([
	"TE",
	"TE2",
	"RB",
	"FB",
	"X",
	"Z",
	"SL",
	"SL2",
]);

const spotOf = (role: Role): Spot =>
	role === "X" || role === "Z"
		? "wide"
		: role === "SL" || role === "SL2"
			? "slot"
			: role === "TE" || role === "TE2"
				? "te"
				: "back";

// Flat out (feet a second): a receiver or a corner fastest, a lineman
// slowest.
const TOP: Partial<Record<Role, number>> = {
	X: 30,
	Z: 30,
	SL: 29.5,
	SL2: 29.5,
	RB: 29,
	FB: 25.5,
	TE: 26.5,
	TE2: 26.5,
	QB: 24.5,
	CB: 30,
	NB: 29.5,
	FS: 29,
	SS: 28.5,
	SAM: 26.5,
	MIKE: 26.5,
	WILL: 27,
	BUCK: 26.5,
	DE: 25,
	DT: 22.5,
	NT: 21.5,
	// The kicking game: the cover men and the returners as fast as anybody,
	// the men up front on it not so.
	KR: 29.5,
	PR: 29.5,
	GUNNER: 30,
	JAMMER: 29.5,
	COVER: 28,
	FRONT: 26.5,
	WALL: 27,
	RUSH: 24,
	UP: 23,
	WING: 21,
	K: 22,
	P: 22,
	H: 22,
};
const topOf = (role: Role) => TOP[role] ?? 20;

// How a man goes, by how fast.
const gait = (fps: number): Move =>
	fps < 6.5 ? "walk" : fps < 12.5 ? "jog" : fps < 21 ? "run" : "sprint";
// A gait asked for that he is nowhere near the pace of, for longer than
// GAIT_LEAST_MS: the one he would be going at - not sprinting on the spot.
const GAIT_LEAST: Partial<Record<Move, number>> = {
	jog: 3.25,
	run: 6.25,
	sprint: 10.5,
};
const GAIT_LEAST_MS = 300;
// (Pulled up into it - his step before it slower than TREAD_FROM_FPS: this
// long will do - see noTreading.)
const TREAD_FROM_FPS = 5;
const TREAD_PULLED_MS = 150;
// A gait gone a good deal quicker than it goes (ft/s): the next one up.
const GAIT_MOST: Partial<Record<Move, number>> = {
	walk: 9,
	jog: 15,
	run: 24,
};
// Up to speed from the pace he was going at (as much of it as was the way
// he goes now): at no more than RAMP_ACCEL ft/s/s, in steps RAMP_STEP_MS
// long, and the rest of the way that much quicker, to be there on time -
// never more than RAMP_OVER times quicker than he would have been (off
// the quicker instead). Not for less of a pick-up than RAMP_LEAST ft/s,
// or on a leg shorter than RAMP_LEG_MS.
const RAMP_ACCEL = 18;
const RAMP_STEP_MS = 100;
const RAMP_OVER = 1.12;
// (Nor quicker than a man goes, ft/s, for it.)
const RAMP_TOP = 31;
const RAMP_LEAST = 4;
const RAMP_LEG_MS = 250;
// (Short of time for it: up to speed as much as this much the harder
// first - on up past RAMP_OVER, if it takes it, to his top speed at most
// - before off the quicker all at once, by as little as does it.)
const RAMP_HARD = 1.6;
// (Nor, on up past RAMP_OVER, more than this many times quicker than he
// would have been.)
const RAMP_FAST = 1.5;
const RAMPS = new Set<Move>(["jog", "run", "sprint"]);
// Pulling up: down from his pace at STOP_ACCEL ft/s/s (harder, to stop
// as near as he has to), in steps STOP_STEP_MS long (see slower).
const STOP_ACCEL = 20;
const STOP_STEP_MS = 100;
// (A leg shorter than this, ms, eased off from: a sliver - see easeOff.)
const EASE_SLIVER_MS = 25;
// (Off a backpedal: see plantBack.)
const BACK_PLANT_ACCEL = 40;
// (From no slower than this, ft/s.)
const STOP_LEAST = 8;
// (Sitting down in a route, cut off at speed: eased off over this share of
// the time there is - turned to the quarterback for the rest.)
const SETTLE_EASE = 0.6;
// Sinking into a cut: down at this (ft/s/s).
const CUT_ACCEL = 28;
// (Cut on the move at this pace (ft/s) or quicker, and turned no more
// than PLANT_TURN: round it at the pace he cuts at, in an arc as tight as
// CUT_LAT ft/s/s across him takes - see Actor.roundCut.)
const CUT_ROUND_PACE = 7;
const CUT_LAT = 50;
// (Easing down to steps this much slower, ft/s, or more - or for a turn
// sharper than this, radians, that he plants for: see slowFor.)
const SLOW_DROP = 6;
// (A chase slowed down to the pace it goes on at - see chaseOn - and less
// of it left than this share of that pace would cover: not gone on with.)
const SLIVER_SHARE = 0.5;
const PLANT_TURN = 1.75;
// (Planted on past the turn, no time for it before: if it leaves him to go
// on no more than this many times as quick as he would have.)
const PLANT_ON = 1.35;
// Turned on the move, BEND_PACE ft/s or quicker, more than BEND_LEAST
// radians round and no more than PLANT_TURN: bent round it in an arc, at no
// more than BEND_LAT ft/s/s across him at the pace he takes it at - in off
// no more than BEND_IN of the stretch he is on, out over no more than
// BEND_OUT of the way on, BEND_STEP radians to a leg (not at all, for less
// than BEND_LEAST_FT of it).
const BEND_PACE = 10;
const BEND_LEAST = 0.3;
const BEND_LAT = 50;
const BEND_IN = 0.8;
const BEND_OUT = 0.8;
const BEND_STEP = 0.2;
const BEND_LEAST_FT = 0.5;
// (Slowed into it, if he cannot take it at as much as this of his pace.)
const BEND_SLOW = 0.9;
// (Not bent round at all, if even slowed into it that leaves him taking it
// at more than this many times as quick as he can: too tight a turn for
// his pace - a hairpin at a sprint - planted for instead.)
const BEND_TOO_QUICK = 1.4;
// (Pinned where he turns, or no more than this far (feet) short of it -
// see Actor.bendOut: round out of it from there, as much as this of a turn
// (radians), no nearer the way on than this many times as far as he goes
// round in.)
const BEND_OUT_BACK_FT = 3;
const BEND_OUT_MOST = 1.2;
const BEND_OUT_ROOM = 1.5;
// (Too near to round out to at his pace: as much again as tight, a try at a
// time - see Actor.bendOut.)
const BEND_OUT_TIGHTER = 0.85;
// (No more than this many ms later for where he is going, for bending round
// into it: see Actor.bendFor.)
const BEND_LATE_MS = 100;
// One stretch of his, pulling up over it (see slowInto): legs no further
// round from each other than this (radians), none quicker than the next by
// more than this (ft/s), over no longer than this (ms) - and still there
// this long (ms) before he goes on, if he gets there later for it.
const STRETCH_TURN = 0.12;
const STRETCH_SLACK = 1.5;
const STRETCH_MS = 3000;
const HOLD_LEFT_MS = 150;
// A way in slowed along (see slowAlong): bent no further round than this
// (radians) from a stride to the next, over the last this long (ms).
const SLOW_ALONG_TURN = 0.6;
const SLOW_ALONG_MS = 1500;
// (Laid through each turn of more than SLOW_ALONG_BEND radians on the way,
// in steps of no less than SLOW_ALONG_LEAST_S seconds.)
const SLOW_ALONG_BEND = 0.05;
const SLOW_ALONG_LEAST_S = 0.01;
// (Pressed up toward somewhere he can't get to in time: short of it by as
// much as he can't, less this much, feet - see pressUp.)
const PRESS_SLACK_FT = 0.5;
// (Short of it by so much he gets less far than this, feet: stood for it.)
const PRESS_LEAST_FT = 0.5;
// (Down at as much as this many times as hard, if he has to be.)
const STOP_HARD = 1.5;
// (Harder still by as much as this, to stay short of where he is going in
// a moment of a chase - see chaseOn.)
const CHASE_SLOW_MOST = 1.5;
// (Run in to a spot near a man to set there, coming in at this much of the
// pace he runs it at or more: eased off over as much of the way as he needs
// - see closeOn.)
const RUN_IN_ON = 0.8;
// (Locked up this long, ms, or more - the two of them no further apart
// than this, feet - a surge or a give and a lean every so long, ms, this
// far either way, feet - not into a man this near either of them, feet:
// see Action.blockBattles.)
const BATTLE_LEAST_MS = 1300;
const BATTLE_PAIR_FT = 3.5;
const BATTLE_MS = [450, 800] as const;
const BATTLE_LEAN_FT = 0.7;
const BATTLE_PUSH_FT = 0.5;
const BATTLE_CLEAR_FT = 2.5;
// (Coming in a good deal quicker than that, no further off the way to him
// than this, radians: on at the pace he has, eased off it.)
const RUN_IN_QUICK_TURN = 0.35;
const RUN_IN_NEAR_FT = 1.5;
// (Short of where that hard a stop takes him by more than this, feet: on
// past it - see Actor.pullsPast.)
const PAST_SLACK_FT = 0.5;
// (A plant left this much quicker, ft/s, than he can take round, for want
// of room: planted harder for it - see plantFor - settling, as he gets on
// round, over this many looks at it.)
const PLANT_SHORT_FPS = 1;
const PLANT_HARD_TRIES = 4;
// (See plantedFor.)
const PLANT_PAST = 0.3;
// Run at somewhere over an end line (see onField), or no more than
// EDGE_NEAR_FT short of it, no more than EDGE_ALONG of the way along it:
// pulled up into it, EDGE_SHORT_FT short of it, at least EDGE_LEAST_FT on
// from where he is - see Actor.edgeOf.
const EDGE_NEAR_FT = 1;
const EDGE_ALONG = 0.5;
const EDGE_SHORT_FT = 0.5;
const EDGE_LEAST_FT = 1;
// (Pulled up as much as this much later than he would have got there, ms,
// if that is what it takes.)
const EDGE_SPARE_MS = 450;
// (Cut short no more than this long into a leg, ms: none of it kept - or,
// in a gait, this long: kept as the one before it.)
const CUT_SLIVER_MS = 1;
// A man off his feet from on the run (see Actor.lunge), going at least
// LUNGE_LEAST_FPS: off from as far back as LUNGE_AIR_MS at that pace and
// LUNGE_SLIDE_FT more takes him (LUNGE_MOST_FT at most) - no more than
// LUNGE_BACK_MS sooner, nor less than LUNGE_ON_MS into the run he was on
// (looked back along in LUNGE_STEP_MS).
const LUNGE_LEAST_FPS = 10;
const LUNGE_AIR_MS = 250;
const LUNGE_SLIDE_FT = 1.5;
const LUNGE_MOST_FT = 8;
const LUNGE_BACK_MS = 250;
const LUNGE_ON_MS = 150;
const LUNGE_STEP_MS = 10;
const LUNGE_MOST_FPS = 28;
const LUNGE_SLIDE_MOST_MS = 700;
// (Launched more than this far round from the way he faces in it (radians),
// this far or more (feet): faced the way he goes.)
const LUNGE_ROUND = 1.75;
const LUNGE_ROUND_FT = 0.5;
// (Diving in on a man down (see Actor.diveIn): from no further off than
// DIVE_STILL_FT and as far again as LUNGE_AIR_MS at his pace takes him
// (LUNGE_MOST_FT at most) - run in to there first, and in the air no less
// than DIVE_LEAST_MS.)
const DIVE_STILL_FT = 4;
const DIVE_LEAST_MS = 380;
// (Turned round for a dive from near a stand - see diveIn: past this much
// round, at this rate, radians a second - evaluate.ts STAND_TURN.)
const DIVE_ROUND_LEAST = 1.5;
const DIVE_ROUND_RATE = 5;
const CUT_MERGE_MS = 60;
const GAITS = new Set<Move>(["walk", "jog", "run", "sprint"]);
const GAIT_ORDER: Move[] = ["walk", "jog", "run", "sprint"];
const STEPS = new Set<Move>([...GAITS, "back", "slide", "shuffle"]);
// (Stood in it, or sat in it on the spot, alive under him - a receiver
// sat down for the ball, a man lined up for a kickoff.)
const ALIVE = new Set<Move>([
	"settle",
	"receiverReady",
	"backerReady",
	"safetyReady",
	"gunReady",
]);
const sits = (m: Move) => moveKind(m) === "still" || ALIVE.has(m);
// (Slower than this, ft/s, he is not going anywhere.)
const CRAWL_FPS = 1;
// Going well off the way he faces (more than CRAB_OFF radians), in a gait
// that goes the way it faces (or low steps up, or a backpedal): at no more
// than SHUFFLE_MOST ft/s, push steps across, square to the way he goes -
// or, further round than BACK_OFF, at no more than BACK_MOST (told to
// backpedal, BACK_TOP), a backpedal square to it - in its place; quicker
// than that, a gait turned the way he goes (no man runs sideways, nor
// backpedals flat out).
const CRAB_OFF = 1.05;
const BACK_OFF = 2.1;
const SHUFFLE_MOST = 7;
const BACK_MOST = 11;
const BACK_TOP = 13;
// (Going no further than this, feet: no way to speak of.)
const FACED_LEAST = 0.25;
// (Low steps no further than this, feet: a step, the way he was told to
// face - not turned square to it for the one step.)
const SLIDE_STEP_FT = 1;
// (Kept on in push steps or a backpedal he was in, to HOLD_ON times as
// quick; turned and running on the same way already, back in them only
// under TURNED times as quick - not round and back each step.)
const HOLD_ON = 1.2;
const TURNED = 0.65;
// (Square to the way he goes - but no further round from the way he was
// told to face than he has to be, to as near it as FACED_KEEP radians: not
// swung one way and back the other each step as he drifts a little across.)
const FACED_KEEP = 0.3;
const keptTo = (square: number, yaw: number): number => {
	const d =
		yaw - square - Math.round((yaw - square) / (2 * Math.PI)) * 2 * Math.PI;
	return square + Math.max(-FACED_KEEP, Math.min(FACED_KEEP, d));
};
const faced = (
	move: Move,
	yaw: number | undefined,
	from: Pt,
	to: Pt,
	fps: number,
	was?: Leg,
): { move: Move; yaw: number | undefined } => {
	const gaited = GAITS.has(move);
	const backed = move === "back";
	if (yaw === undefined || (!gaited && !backed && move !== "slide")) {
		return { move, yaw };
	}
	if (move === "slide" && dist(from, to) < SLIDE_STEP_FT) {
		return { move, yaw };
	}
	if (dist(from, to) < FACED_LEAST) {
		// (In push steps or a backpedal, hardly going anywhere: on in them,
		// slow - not a step of a walk going backwards.)
		return gaited && (was?.move === "shuffle" || was?.move === "back")
			? { move: was.move, yaw: was.yaw ?? yaw }
			: { move, yaw };
	}
	const way = Math.atan2(to.y - from.y, to.x - from.x);
	const d = yaw - way - Math.round((yaw - way) / (2 * Math.PI)) * 2 * Math.PI;
	const off = Math.abs(d);
	if (off <= CRAB_OFF) {
		return { move: backed ? gait(fps) : move, yaw };
	}
	// (Backpedaling, square to the way he goes.)
	if (backed && off > BACK_OFF && fps <= BACK_TOP) {
		return { move, yaw: keptTo(way + Math.PI, yaw) };
	}
	const k =
		was?.move === "shuffle" || was?.move === "back"
			? HOLD_ON
			: was &&
				  GAITS.has(was.move) &&
				  was.yaw === undefined &&
				  dist(was.from, was.to) >= FACED_LEAST &&
				  angleAt(was.from, from, to) < Math.PI / 3
				? TURNED
				: 1;
	if (off <= BACK_OFF && fps <= SHUFFLE_MOST * k) {
		return {
			move: "shuffle",
			yaw: keptTo(way + (d > 0 ? 1 : -1) * (Math.PI / 2), yaw),
		};
	}
	if (off > BACK_OFF && fps <= BACK_MOST * k) {
		return { move: "back", yaw: keptTo(way + Math.PI, yaw) };
	}
	return gaited
		? { move, yaw: undefined }
		: backed
			? { move: gait(fps), yaw: undefined }
			: { move, yaw };
};
// (In the gait he was in, a run or a sprint down to a jog once he is
// that slow, with this long (ms) at least still to go - not a gait to
// each step, nor one for a moment at the last.)
const SLOW_LEAST_MS = 250;
const slower = (
	move: Move,
	n: number,
	dt: number,
	paceOf: (i: number) => number,
): ((i: number) => Move) => {
	let k = 0;
	while (k < n && paceOf(k) >= 12.5) {
		k++;
	}
	const down =
		(move === "run" || move === "sprint") &&
		(n - k) * dt * 1000 >= SLOW_LEAST_MS;
	return (i) => (down && i >= k ? "jog" : move);
};
const paced = (move: Move | undefined, fps: number, ms: number): Move => {
	const least = move === undefined ? undefined : GAIT_LEAST[move];
	return move === undefined ||
		(least !== undefined && fps < least && ms >= GAIT_LEAST_MS)
		? gait(fps)
		: move;
};

const clamp = (x: number, lo: number, hi: number) =>
	Math.min(hi, Math.max(lo, x));
// An angle (radians) the short way round: -PI to PI.
const angleOff = (a: number) => Math.atan2(Math.sin(a), Math.cos(a));
// Along the sideline from x, the nearest no nearer than OFF_APART_FT to
// any of `taken` (and taken too, now).
const offSpot = (taken: number[], x: number): number => {
	let best = x;
	for (let k = 1; taken.some((t) => Math.abs(t - best) < OFF_APART_FT); k++) {
		best = x + (k % 2 === 1 ? 1 : -1) * Math.ceil(k / 2) * OFF_APART_FT * 0.5;
	}
	taken.push(best);
	return best;
};
// Whether the line from a to b and the one from c to e cross.
const linesCross = (a: Pt, b: Pt, c: Pt, e: Pt): boolean => {
	const side = (p: Pt, q: Pt, o: Pt) =>
		Math.sign((q.x - p.x) * (o.y - p.y) - (q.y - p.y) * (o.x - p.x));
	return side(a, b, c) * side(a, b, e) < 0 && side(c, e, a) * side(c, e, b) < 0;
};
// How far round (radians) going a to b and on to c turns him at b.
const angleAt = (a: Pt, b: Pt, c: Pt): number => {
	const t = Math.atan2(c.y - b.y, c.x - b.x) - Math.atan2(b.y - a.y, b.x - a.x);
	return Math.abs(Math.atan2(Math.sin(t), Math.cos(t)));
};

// Planted from v (ft/s) down to ve on the way he is going, where he is
// going `ahead` feet on along it: no further on than that (see CUT_ACCEL)
// - not past it and back. (The pace he is down to.)
const plantedTo = (
	v: number,
	ve: number,
	ahead: number,
	accel = CUT_ACCEL,
): number =>
	Math.min(
		v,
		ahead > 0
			? Math.max(ve, Math.sqrt(Math.max(0, v * v - 2 * accel * ahead)))
			: ve,
	);

// Planted from v (ft/s) at `at`, going (cos, sin), for `to` (see
// plantFor): the pace he is down to, and the most of his pace he could
// take round (`down`, if given) - as far round as it is from where he gets
// to planting, not from where he began it: on past it planting, it is
// further round, and nearer.
const plantedFor = (
	v: number,
	at: Pt,
	cos: number,
	sin: number,
	to: Pt,
	down?: number,
	most = Infinity,
	accel = CUT_ACCEL,
	tries = 2,
	past = PLANT_PAST,
): { ve: number; want: number } => {
	const on = (to.x - at.x) * cos + (to.y - at.y) * sin;
	// (Well off to the side of it, near square to the way he goes: on past
	// it as far as PLANT_PAST of that, planting - round a little more after
	// - not hardly slowed for it at all, and round at a crawl. The nearer
	// square, the further: all the way down for it, square to it, as he is
	// for one behind him - not planted a step or two, and round from most
	// of his pace to a crawl in a stride.)
	const off = Math.abs((to.y - at.y) * cos - (to.x - at.x) * sin);
	const ahead =
		on > 0
			? Math.max(on, off > 0 ? ((off * Math.hypot(on, off)) / on) * past : 0)
			: on;
	const round = (p: Pt) => angleAt({ x: p.x - cos, y: p.y - sin }, p, to);
	let want = Math.min(most, down ?? v * cutKeep(round(at)));
	let ve = plantedTo(v, want, ahead, accel);
	for (let k = 0; k < tries && down === undefined; k++) {
		const d = (v * v - ve * ve) / (2 * accel);
		if (!(d > 0)) {
			break;
		}
		const less = v * cutKeep(round({ x: at.x + cos * d, y: at.y + sin * d }));
		if (!(less < want)) {
			break;
		}
		want = less;
		ve = plantedTo(v, want, ahead, accel);
	}
	return { ve, want };
};

// Planting for `to` off leg l, where it has him at `at` (as plantFor
// would): how long it takes him (s), how far on along it he goes the
// while (feet), and the pace he is down to - none of it, not going quick
// enough or far enough round to have to.
const plantOn = (
	l: Leg,
	at: Pt,
	to: Pt,
): { T: number; on: number; ve: number } => {
	const v = legPace(l);
	const far = dist(l.from, l.to);
	const turn = angleAt(l.from, at, to);
	if (
		!RAMPS.has(l.move) ||
		v < STOP_LEAST ||
		l.arms ||
		l.turn ||
		far <= 0 ||
		!(turn > CUT_LEAST)
	) {
		return { T: 0, on: 0, ve: v };
	}
	const { ve } = plantedFor(
		v,
		at,
		(l.to.x - l.from.x) / far,
		(l.to.y - l.from.y) / far,
		to,
	);
	const T = (v - ve) / CUT_ACCEL;
	return { T, on: ((v + ve) / 2) * T, ve };
};

// The pace (ft/s) he has to get up to off `v0`, at `A` ft/s/s, and go on
// at to go `far` feet in T seconds - Infinity, if even all the way up does
// not do it.
const paceFor = (far: number, T: number, v0: number, A: number): number => {
	const disc = T * T - (2 * (far - v0 * T)) / A;
	return disc >= 0 ? v0 + A * (T - Math.sqrt(disc)) : Infinity;
};

// How long (ms) he takes to go `far` feet from a stand to a stand: up at
// `up` ft/s/s, at no more than `fps`, and down at `down`.
const stepMs = (far: number, fps: number, up: number, down: number): number => {
	const v = Math.min(fps, Math.sqrt(far / (1 / (2 * up) + 1 / (2 * down))));
	return v > 0 ? (far / v + v / (2 * up) + v / (2 * down)) * 1000 : 0;
};

// How long (ms) he takes to go `far` feet at `fps`, off `v0` ft/s - up to
// it at RAMP_ACCEL (or `A` ft/s/s).
const rampedMs = (
	far: number,
	fps: number,
	v0: number,
	A = RAMP_ACCEL,
): number => {
	if (v0 >= fps) {
		return (far / fps) * 1000;
	}
	const up = (fps * fps - v0 * v0) / (2 * A);
	return (
		(far <= up
			? (Math.sqrt(v0 * v0 + 2 * A * far) - v0) / A
			: (fps - v0) / A + (far - up) / fps) * 1000
	);
};

// How quick he goes on leg l (ft/s).
const legPace = (l: Leg): number =>
	l.t1 > l.t0 ? dist(l.from, l.to) / ((l.t1 - l.t0) / 1000) : 0;

// How quick he can go on from `from` toward `to` off leg l (ft/s): all of
// his pace on it the way he was going, half of it square to that, none of
// it back the other way - nothing, stood or going no way at all.
const paceOn = (l: Leg, from: Pt, to: Pt): number => {
	const ms = l.t1 - l.t0;
	const lx = l.to.x - l.from.x;
	const ly = l.to.y - l.from.y;
	const dx = to.x - from.x;
	const dy = to.y - from.y;
	const was = Math.hypot(lx, ly);
	const way = Math.hypot(dx, dy);
	if (ms <= 0 || was <= 0 || way <= 0) {
		return 0;
	}
	const cos = (lx * dx + ly * dy) / (was * way);
	return ((was / (ms / 1000)) * (1 + cos)) / 2;
};

// The way from `a` to `b` no nearer `p` than PILE_PAST - or from right by
// it, away from it.
const wide = (p: Pt, a: Pt, b: Pt): boolean => {
	const vx = b.x - a.x;
	const vy = b.y - a.y;
	const l = vx * vx + vy * vy;
	const along = (p.x - a.x) * vx + (p.y - a.y) * vy;
	const u = l > 0 ? clamp(along / l, 0, 1) : 0;
	return (
		along <= 0 ||
		Math.hypot(p.x - a.x - vx * u, p.y - a.y - vy * u) >= PILE_PAST
	);
};

// Ways out from a man spread round him, no two nearer than `gap` (each as
// near as it can be to the way it was).
const spreadRound = (angles: number[], gap: number): number[] => {
	const TAU = Math.PI * 2;
	const order = angles
		.map((a, k) => ({ a: ((a % TAU) + TAU) % TAU, k }))
		.sort((x, y) => x.a - y.a);
	const n = order.length;
	for (let pass = 0; n > 1 && pass < 8; pass++) {
		for (let j = 0; j < n; j++) {
			const a = order[j]!;
			const b = order[(j + 1) % n]!;
			const d = b.a - a.a + (j === n - 1 ? TAU : 0);
			if (d < gap) {
				a.a -= (gap - d) / 2;
				b.a += (gap - d) / 2;
			}
		}
	}
	const out: number[] = [];
	for (const o of order) {
		out[o.k] = o.a;
	}
	return out;
};

// (Or as far as `roam` past a sideline, `endRoam` past an end line.)
const onField = (p: Pt, roam = 0, endRoam = 0): Pt => ({
	x: clamp(p.x, 1 - endRoam, FIELD_L - 1 + endRoam),
	y: clamp(p.y, 1 - roam, FIELD_W - 1 + roam),
});

// Of `men`, the one nearest `p` - where each is at t, or by default at the
// end of what he has so far.
const closest = (men: Actor[], p: Pt, t?: number): Actor | undefined => {
	let best: Actor | undefined;
	let far = Infinity;
	for (const m of men) {
		const d = dist(t === undefined ? m.at : m.posAt(t), p);
		if (d < far) {
			far = d;
			best = m;
		}
	}
	return best;
};

// The men up front with nobody over them each up onto a linebacker who
// `fits` him, as many of them as can be: the pairs that between them go
// the least way to where the backers are at `t` - so no two of them
// crossing each other's way up.
const climbPairs = (
	men: Actor[],
	backers: Actor[],
	t: number,
	fits: (b: Actor, m: Actor) => boolean,
): Map<Actor, Actor> => {
	const at = new Map(backers.map((m) => [m, m.posAt(t)]));
	const ok = new Map(
		men.map((b) => [b, new Set(backers.filter((m) => fits(b, m)))]),
	);
	let best: { n: number; cost: number; pairs: [Actor, Actor][] } = {
		n: 0,
		cost: 0,
		pairs: [],
	};
	const cur: [Actor, Actor][] = [];
	const used = new Set<Actor>();
	const go = (i: number, cost: number) => {
		if (i >= men.length) {
			if (cur.length > best.n || (cur.length === best.n && cost < best.cost)) {
				best = { n: cur.length, cost, pairs: [...cur] };
			}
			return;
		}
		const b = men[i]!;
		for (const m of backers) {
			if (used.has(m) || !ok.get(b)!.has(m)) {
				continue;
			}
			used.add(m);
			cur.push([b, m]);
			go(i + 1, cost + dist(b.at, at.get(m)!));
			cur.pop();
			used.delete(m);
		}
		go(i + 1, cost);
	};
	go(0, 0);
	return new Map(best.pairs);
};

// ---- one man's part, a stretch at a time ------------------------------------

class Actor {
	readonly part: Part;
	readonly top: number;
	// His ratings, where the game has them, and how he stands out by them
	// (see ratings.ts).
	readonly rated: FieldRatings | undefined;
	readonly flair: Flair;
	// The way he faces at the end of what he has been given so far.
	yaw: number;
	// How far past a sideline he may go: out of bounds.
	roam = 0;
	// And past an end line: eased off out the back of the end zone (see
	// easeOff).
	endRoam = 0;
	// Till when where he has been is fixed - where he takes the ball, say:
	// no turn after it is bent into short of where he was then (see
	// bendInto).
	pinned = -Infinity;

	constructor(part: Part, speed = 1, rated?: FieldRatings, flair: Flair = {}) {
		this.part = part;
		this.top = topOf(part.role) * speed;
		this.rated = rated;
		this.flair = flair;
		let yaw = part.legs[0]?.yaw ?? 0;
		for (const l of part.legs) {
			yaw = yawOf(l, yaw);
		}
		this.yaw = yaw;
	}

	get pid() {
		return this.part.pid;
	}
	get role() {
		return this.part.role;
	}
	get team() {
		return this.part.team;
	}
	get legs() {
		return this.part.legs;
	}
	get last(): Leg {
		return this.part.legs.at(-1)!;
	}
	get at(): Pt {
		return this.last.to;
	}
	get t(): number {
		return this.last.t1;
	}

	// Where he is at t, by what he has been given (past the end of it,
	// where it leaves him).
	posAt(t: number): Pt {
		const legs = this.part.legs;
		const k = legIndexAt(legs, t);
		if (k < 0) {
			return legs[0]!.from;
		}
		const l = legs[k]!;
		const u = l.t1 > l.t0 ? clamp((t - l.t0) / (l.t1 - l.t0), 0, 1) : 1;
		return {
			x: l.from.x + (l.to.x - l.from.x) * u,
			y: l.from.y + (l.to.y - l.from.y) * u,
		};
	}

	private push(l: Leg) {
		this.part.legs.push(l);
		this.yaw = yawOf(l, this.yaw);
	}

	// (See pinned.)
	pin(t: number) {
		this.pinned = Math.max(this.pinned, t);
	}

	// Where he is until t, in `move` (by default what he was doing, if it
	// was standing in something) facing `yaw` (the way he was).
	hold(t: number, move?: Move, yaw?: number, extra?: Partial<Leg>) {
		const l = this.last;
		if (t <= l.t1) {
			return;
		}
		const m = move ?? (sits(l.move) ? l.move : "stand");
		const still = l.from.x === l.to.x && l.from.y === l.to.y;
		// (Only a moment to wait, on his way there: there that much later,
		// not stopped dead for it and off again.)
		if (
			!still &&
			move === undefined &&
			yaw === undefined &&
			extra === undefined &&
			moveKind(l.move) === "cycle" &&
			t - l.t1 < WAIT_LEAST_MS
		) {
			l.t1 = t;
			return;
		}
		if (
			still &&
			l.move === m &&
			(yaw === undefined || yaw === l.yaw) &&
			extra === undefined &&
			moveKind(m) !== "act"
		) {
			l.t1 = t;
			return;
		}
		if (!still && sits(m)) {
			this.slowInto(0, STOP_ACCEL, t - l.t1);
			// (No pulling up on the way there - cut off at speed, say: on past
			// it, eased off in the time there is, not stopped dead.)
			const p = this.last;
			if (
				(m === "stand" || m === "settle") &&
				RAMPS.has(p.move) &&
				legPace(p) >= STOP_LEAST
			) {
				const share = m === "settle" ? SETTLE_EASE : 1;
				this.easeOff(
					Math.min(EASE_OFF_FT, (legPace(p) * (t - p.t1) * share) / 2000),
				);
				if (this.t >= t) {
					return;
				}
			}
		}
		this.push({
			t0: this.t,
			t1: t,
			from: this.at,
			to: this.at,
			move: m,
			yaw: yaw ?? this.yaw,
			...(l.mirrored && m === l.move ? { mirrored: true } : {}),
			...extra,
		});
	}

	// Pulled up at the end of his last leg (see slowInto) into `move`, a
	// stance he works about in, there until t.
	setInto(t: number, move: Move, yaw?: number) {
		const l = this.last;
		if (t > l.t1 && (l.from.x !== l.to.x || l.from.y !== l.to.y)) {
			this.slowInto(0, STOP_ACCEL, t - l.t1);
		}
		this.hold(t, move, yaw);
	}

	// To `to`, getting there at t1, in `move` (by default by how fast that
	// is), facing `yaw` (by default the way he goes).
	go(to: Pt, t1: number, move?: Move, yaw?: number, extra?: Partial<Leg>) {
		let leg = this.legTo(to, t1, move, yaw, extra);
		if (extra === undefined) {
			const t = this.t;
			const ve = this.slowFor(leg);
			if (this.t > t) {
				// (Planted on past where he turns: on from there.)
				leg = this.legTo(to, t1, move, yaw, extra);
			}
			this.rampInto(leg, ve);
		}
		this.push(leg);
	}

	// As go, in a move that is no stride (fighting his way off the line,
	// say): up to its pace off what he was doing as rampInto has him - out
	// of his stance, up from nothing - not off at it in a blink.
	goOff(to: Pt, t1: number, move: Move, yaw?: number) {
		const leg = this.legTo(to, t1, move, yaw);
		this.rampInto(leg, this.paceInto(leg.to), true);
		this.push(leg);
	}

	// Slowing for `leg`: steps a good deal slower (SLOW_DROP ft/s or more)
	// than the stride he is in, or a turn sharper than PLANT_TURN he can
	// carry only so much of his pace round (as on a route - see breakOf):
	// down to it over the last of the one he is on (see slowInto), planted
	// for a turn (see CUT_ACCEL), not braked to it in a stride - or, going
	// too fast to have slowed for it in time, planted on past it (see
	// plantFor). (Turning, the pace he is down to.)
	private slowFor(leg: Leg): number | undefined {
		const l = this.last;
		if (!STEPS.has(leg.move) || l.t1 <= l.t0 || leg.t1 <= leg.t0) {
			return undefined;
		}
		const was = legPace(l);
		const v = legPace(leg);
		const turn =
			dist(l.from, l.to) > 0 && dist(leg.from, leg.to) > 0
				? angleAt(l.from, l.to, leg.to)
				: 0;
		const round = turn > PLANT_TURN ? cutKeep(turn) : 1;
		const keep = Math.min(v / was, round);
		if (!(was * (1 - keep) >= SLOW_DROP)) {
			return undefined;
		}
		const ve = this.slowInto(keep, round < 1 ? CUT_ACCEL : STOP_ACCEL);
		if (
			round < 1 &&
			(ve === undefined || ve > was * keep + SLOW_DROP) &&
			this.plantFits(leg, was * keep)
		) {
			// (Not planted - out of a backpedal, say - none of his pace the
			// new way but what he carries round: see paceInto.)
			const before = this.last;
			this.plantFor(leg.to, was * keep);
			return this.last === before ? undefined : legPace(this.last);
		}
		return round < 1 ? ve : undefined;
	}

	// Turning on the move toward `to`, on at `w` ft/s (see BEND_PACE):
	// bent round into it in an arc as wide as he takes at the pace he comes
	// into it at, off the last of the stretch he is on (see stretchFrom) -
	// not turned in a stride at full tilt - slowed into it, if it is too
	// tight to take at that pace. (Whether he was.)
	private bendInto(to: Pt, w: number, easeTo = w): boolean {
		const legs = this.part.legs;
		const l = this.last;
		const v = legPace(l);
		const dOut = dist(l.to, to);
		if (
			!RAMPS.has(l.move) ||
			l.arms ||
			l.turn ||
			l.span ||
			v < BEND_PACE ||
			w < BEND_PACE ||
			dOut <= 0
		) {
			return false;
		}
		const turn = angleAt(l.from, l.to, to);
		if (!(turn > BEND_LEAST && turn <= PLANT_TURN)) {
			return false;
		}
		const j = this.stretchFrom();
		// (No further back than where he was when he was pinned.)
		let since = 0;
		for (let k = legs.length - 1; k >= j && legs[k]!.t1 > this.pinned; k--) {
			const lk = legs[k]!;
			const u =
				lk.t0 >= this.pinned
					? 1
					: (lk.t1 - this.pinned) / Math.max(lk.t1 - lk.t0, 1e-6);
			since += dist(lk.from, lk.to) * u;
		}
		const back = Math.min(
			((v * v) / BEND_LAT) * Math.tan(turn / 2),
			dist(legs[j]!.from, l.to) * BEND_IN,
			dOut * BEND_OUT,
			since,
		);
		if (back < BEND_LEAST_FT) {
			return false;
		}
		// (Off the stretch that far short of the turn.)
		let k = legs.length - 1;
		let left = back;
		while (k > j && dist(legs[k]!.from, legs[k]!.to) <= left) {
			left -= dist(legs[k]!.from, legs[k]!.to);
			k--;
		}
		const lk = legs[k]!;
		const f = 1 - left / Math.max(dist(lk.from, lk.to), 1e-6);
		const corner = l.to;
		// (Facing the way he went, round with it - or, turned some other way
		// to it, still that way.)
		const { yaw, ...was } = l;
		const way = Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x);
		const keep =
			yaw !== undefined &&
			Math.abs(Math.atan2(Math.sin(yaw - way), Math.cos(yaw - way))) >
				BEND_LEAST;
		// (Not round over the edge of where he can go - see onField: cut
		// back in to it, that would be, in a blink.)
		let tight = false;
		{
			const p0 = {
				x: lk.from.x + (lk.to.x - lk.from.x) * f,
				y: lk.from.y + (lk.to.y - lk.from.y) * f,
			};
			const tIn = Math.max(dist(p0, corner), 1e-6);
			const u = { x: (corner.x - p0.x) / tIn, y: (corner.y - p0.y) / tIn };
			const o = { x: (to.x - corner.x) / dOut, y: (to.y - corner.y) / dOut };
			const round = Math.acos(clamp(u.x * o.x + u.y * o.y, -1, 1));
			const R = tIn / Math.tan(round / 2);
			const s = Math.sign(u.x * o.y - u.y * o.x) || 1;
			const c = { x: p0.x - s * u.y * R, y: p0.y + s * u.x * R };
			const r0 = Math.atan2(p0.y - c.y, p0.x - c.x);
			const n = Math.max(2, Math.ceil(round / BEND_STEP));
			for (let i = 1; i <= n; i++) {
				const q = r0 + (s * round * i) / n;
				const p = { x: c.x + Math.cos(q) * R, y: c.y + Math.sin(q) * R };
				if (dist(onField(p, this.roam, this.endRoam), p) > 1e-6) {
					return false;
				}
			}
			// (Pinned part of the way along it - the ball put in his hands
			// there, say - and no room after that to slow to a pace he can
			// take it round at, however hard: not round it at that pace off
			// the pace he came in at in a stride - planted for it, on past
			// it, instead: see plantFor.)
			const most = Math.sqrt(BEND_LAT * R);
			tight = most < v * BEND_SLOW;
			let all = 0;
			for (let q = j; q < legs.length; q++) {
				all += dist(legs[q]!.from, legs[q]!.to);
			}
			if (
				most < v * BEND_SLOW &&
				since < all - 1e-6 &&
				since - back < (v * v - most * most) / (2 * CUT_ACCEL * STOP_HARD)
			) {
				return false;
			}
		}
		// (As it was, to go back to - see BEND_TOO_QUICK. The legs themselves
		// put back as they were, not copies of them: a hold on his last one
		// - see plantFor - is on it as it was, not on it cut short for this.)
		const before = tight ? legs.map((x) => [x, { ...x }] as const) : undefined;
		const was0 = { yaw: this.yaw, pinned: this.pinned };
		legs.splice(k + 1);
		lk.to = {
			x: lk.from.x + (lk.to.x - lk.from.x) * f,
			y: lk.from.y + (lk.to.y - lk.from.y) * f,
		};
		lk.t1 = lk.t0 + (lk.t1 - lk.t0) * f;
		// (Round from there, the way in to the way on.)
		const start = lk.to;
		const tIn = Math.max(dist(start, corner), 1e-6);
		const u = { x: (corner.x - start.x) / tIn, y: (corner.y - start.y) / tIn };
		const o = { x: (to.x - corner.x) / dOut, y: (to.y - corner.y) / dOut };
		const round = Math.acos(clamp(u.x * o.x + u.y * o.y, -1, 1));
		const R = tIn / Math.tan(round / 2);
		// (Too tight a turn to take at his pace: slowed into it, off the
		// stretch he is on, to one he can take it at.)
		let pace = v;
		// (Still getting up to speed where he starts round it: from the pace
		// he is at there, up as he goes round - not round it at the pace he
		// was to be up to by the turn, off a slower one in a stride.)
		let vStart = Math.min(v, legPace(lk));
		const most = Math.sqrt(BEND_LAT * R);
		if (most < v * BEND_SLOW) {
			pace = this.slowInto(most / legPace(lk), CUT_ACCEL) ?? v;
			if (pace > most) {
				// (Or, still getting up to speed into it - no time on the
				// stretch to slow for it as well: no quicker than he can take
				// it at to the last of it, and round it at that - there that
				// much later. Or planting into it, off quicker than that: down
				// no harder than he can be - see CUT_ACCEL, STOP_HARD - and round
				// it the quicker for it, not braked to it in a stride.)
				let shift = 0;
				let vIn: number | undefined;
				let s = 0;
				const A = CUT_ACCEL * STOP_HARD;
				const floor = (at: number) =>
					Math.sqrt(Math.max(0, (vIn ?? 0) ** 2 - 2 * A * at));
				for (let i = this.stretchFrom(); i < legs.length; i++) {
					const x = legs[i]!;
					const ms = x.t1 - x.t0;
					if (x.t0 + shift < this.pinned) {
						continue;
					}
					if (vIn === undefined) {
						const b = legs[i - 1];
						vIn =
							b && b.t1 === x.t0 && RAMPS.has(b.move) && !b.arms && !b.turn
								? legPace(b)
								: 0;
					}
					const d = dist(x.from, x.to);
					x.t0 += shift;
					const need = (d / Math.max(most, floor(s + d / 2))) * 1000;
					s += d;
					shift += Math.max(0, need - ms);
					x.t1 = x.t0 + Math.max(ms, need);
				}
				pace = Math.min(legPace(this.last), Math.max(most, floor(s)));
			}
			vStart = pace;
			if (before && pace > most * BEND_TOO_QUICK) {
				legs.splice(
					0,
					legs.length,
					...before.map(([x, was]) => {
						for (const key of Object.keys(x)) {
							if (!(key in was)) {
								delete (x as Record<string, unknown>)[key];
							}
						}
						return Object.assign(x, was);
					}),
				);
				this.yaw = was0.yaw;
				this.pinned = was0.pinned;
				return false;
			}
		}
		const s = Math.sign(u.x * o.y - u.y * o.x) || 1;
		const c = { x: start.x - s * u.y * R, y: start.y + s * u.x * R };
		const r0 = Math.atan2(start.y - c.y, start.x - c.x);
		const n = Math.max(2, Math.ceil(round / BEND_STEP));
		// (To go on slower than that out of it - a chase to where he is to
		// be in a moment, say: easing off round it toward `w`, at
		// STOP_ACCEL - not round it at his pace, and down to what is left of
		// the way there in a stride out of it.)
		const ve =
			easeTo < pace
				? Math.max(easeTo, pace - (STOP_ACCEL * R * round) / pace)
				: pace;
		for (let i = 1; i <= n; i++) {
			const q = r0 + (s * round * i) / n;
			const at = this.last;
			const dt =
				((R * round) / n / (vStart + ((ve - vStart) * (i - 0.5)) / n)) * 1000;
			this.push({
				...was,
				...(keep ? { yaw } : {}),
				t0: at.t1,
				t1: at.t1 + dt,
				from: at.to,
				to: { x: c.x + Math.cos(q) * R, y: c.y + Math.sin(q) * R },
			});
		}
		return true;
	}

	// Pinned where he turns toward `to`, or just short of it - the ball put
	// in his hands there, say - so no room to bend into it short of there
	// (see bendInto): round out of it from there instead, at his pace, in
	// an arc as tight as he takes at it (see BEND_LAT) - on past the turn
	// a little as he goes round, not turned in a stride at full tilt. Not
	// for more than BEND_OUT_MOST of a turn - planted for that (see
	// plantFor). (Whether he was.)
	private bendOut(to: Pt): boolean {
		const legs = this.part.legs;
		const l = this.last;
		const v = legPace(l);
		const far = dist(l.from, l.to);
		if (
			!RAMPS.has(l.move) ||
			l.arms ||
			l.turn ||
			l.span ||
			v < BEND_PACE ||
			far <= 0 ||
			this.pinned === -Infinity
		) {
			return false;
		}
		const turn = angleAt(l.from, l.to, to);
		if (!(turn > BEND_LEAST && turn <= BEND_OUT_MOST)) {
			return false;
		}
		const u = { x: (l.to.x - l.from.x) / far, y: (l.to.y - l.from.y) / far };
		// (Back to where he was pinned: the same way on from there, and no
		// further back than BEND_OUT_BACK_FT.)
		let k = legs.length - 1;
		let since = 0;
		while (k >= 0 && legs[k]!.t1 > this.pinned) {
			const lk = legs[k]!;
			const d = dist(lk.from, lk.to);
			const a =
				Math.atan2(lk.to.y - lk.from.y, lk.to.x - lk.from.x) -
				Math.atan2(u.y, u.x);
			if (
				!RAMPS.has(lk.move) ||
				lk.arms ||
				lk.turn ||
				lk.span ||
				lk.yaw !== l.yaw ||
				lk.ball !== l.ball ||
				(d > 0 && Math.abs(Math.atan2(Math.sin(a), Math.cos(a))) > STRETCH_TURN)
			) {
				return false;
			}
			since +=
				lk.t0 >= this.pinned
					? d
					: (d * (lk.t1 - this.pinned)) / Math.max(lk.t1 - lk.t0, 1e-6);
			if (since > BEND_OUT_BACK_FT) {
				return false;
			}
			if (lk.t0 < this.pinned) {
				break;
			}
			k--;
		}
		if (k < 0) {
			return false;
		}
		// (The arc out from there - round the way on, off the way he came.)
		const lk = legs[k]!;
		const f =
			lk.t1 > this.pinned && lk.t1 > lk.t0
				? clamp((this.pinned - lk.t0) / (lk.t1 - lk.t0), 0, 1)
				: 1;
		const c = {
			x: lk.from.x + (lk.to.x - lk.from.x) * f,
			y: lk.from.y + (lk.to.y - lk.from.y) * f,
		};
		const s = Math.sign(u.x * (to.y - c.y) - u.y * (to.x - c.x)) || 1;
		// (Too near it to round out to it at his pace: tighter, slowing to
		// the pace he takes that at as he goes round - no harder than he
		// plants (see CUT_ACCEL, STOP_HARD), nor below BEND_PACE - not
		// turned in a stride.)
		let R = (v * v) / BEND_LAT;
		let o = { x: c.x - s * u.y * R, y: c.y + s * u.x * R };
		let d = dist(o, to);
		while (d <= R * BEND_OUT_ROOM) {
			R *= BEND_OUT_TIGHTER;
			if (R * BEND_LAT < BEND_PACE * BEND_PACE) {
				return false;
			}
			o = { x: c.x - s * u.y * R, y: c.y + s * u.x * R };
			d = dist(o, to);
		}
		const vArc = Math.sqrt(BEND_LAT * R);
		const a = Math.atan2(to.y - o.y, to.x - o.x);
		const end = a - s * Math.acos(R / d);
		const a0 = Math.atan2(c.y - o.y, c.x - o.x);
		const sweep =
			(((s * (end - a0)) % (2 * Math.PI)) + 2 * Math.PI) % (2 * Math.PI);
		if (!(sweep > 0 && sweep <= BEND_OUT_MOST)) {
			return false;
		}
		if ((v * v - vArc * vArc) / (2 * R * sweep) > CUT_ACCEL * STOP_HARD) {
			return false;
		}
		const n = Math.max(2, Math.ceil(sweep / BEND_STEP));
		const pts: Pt[] = [];
		for (let i = 1; i <= n; i++) {
			const q = a0 + (s * sweep * i) / n;
			const p = { x: o.x + Math.cos(q) * R, y: o.y + Math.sin(q) * R };
			if (dist(onField(p, this.roam, this.endRoam), p) > 1e-6) {
				return false;
			}
			pts.push(p);
		}
		const { yaw, ...was } = l;
		legs.splice(k + 1);
		if (f < 1) {
			lk.to = c;
			lk.t1 = lk.t0 + (lk.t1 - lk.t0) * f;
			if (lk.t1 - lk.t0 < CUT_SLIVER_MS && legs.length > 1) {
				legs.pop();
				legs.at(-1)!.to = c;
			}
		}
		pts.forEach((p, i) => {
			const at = this.last;
			const dt = ((R * sweep) / n / (v + ((vArc - v) * (i + 0.5)) / n)) * 1000;
			this.push({ ...was, t0: at.t1, t1: at.t1 + dt, from: at.to, to: p });
		});
		return true;
	}

	// (Planted on past the end of the leg he is on, down to `down` ft/s
	// - see plantFor - still time to go on from there to the end of `leg`
	// by when it ends, at no more than PLANT_ON times its pace, nor more
	// than his top speed.)
	private plantFits(leg: Leg, down: number): boolean {
		const l = this.last;
		const v = legPace(l);
		const far = dist(l.from, l.to);
		if (far <= 0) {
			return false;
		}
		const cos = (l.to.x - l.from.x) / far;
		const sin = (l.to.y - l.from.y) / far;
		const ve = plantedTo(
			v,
			down,
			(leg.to.x - l.to.x) * cos + (leg.to.y - l.to.y) * sin,
		);
		if (ve >= v) {
			return false;
		}
		const T = (v - ve) / CUT_ACCEL;
		const d = ((v + ve) / 2) * T;
		const at = { x: l.to.x + cos * d, y: l.to.y + sin * d };
		const left = leg.t1 - l.t1 - T * 1000;
		const most = Math.max(
			legPace(leg),
			Math.min(legPace(leg) * PLANT_ON, this.top),
		);
		return left >= RAMP_LEG_MS && dist(at, leg.to) / (left / 1000) <= most;
	}

	// To `to` by t1, out of his stance or off `v0` ft/s: up to speed at `A`
	// ft/s/s, in steps RAMP_STEP_MS long, and on at it - as quick as he has
	// to be to be there on time (up the whole way, and the harder, if it
	// takes that); still facing the way he was over the steps in the first
	// `square` ms, getting up out of it. (The pace he gets there at.)
	getOff(
		to: Pt,
		t1: number,
		v0: number,
		A: number,
		move?: Move,
		square = 0,
		face?: number,
	): number {
		const yaw = this.yaw;
		const leg = this.legTo(to, t1, move, face);
		if (move) {
			// (Going all out from the first step, slow as it is.)
			leg.move = move;
		}
		const T = (leg.t1 - leg.t0) / 1000;
		const dx = leg.to.x - leg.from.x;
		const dy = leg.to.y - leg.from.y;
		const D = Math.hypot(dx, dy);
		if (D <= v0 * T) {
			this.push(leg);
			return D / T;
		}
		const room = T * T - (2 * (D - v0 * T)) / A;
		const a = room >= 0 ? A : (2 * (D - v0 * T)) / (T * T);
		const ta = room >= 0 ? T - Math.sqrt(room) : T;
		// (Up to it in no time: on at it from where he is - not a step of
		// nothing first, that has him stood still for it.)
		if (ta * 1000 < RAMP_STEP_MS / 2) {
			this.push(leg);
			return D / T;
		}
		const n = Math.max(1, Math.round((ta * 1000) / RAMP_STEP_MS));
		const dt = ta / n;
		// (In the gait he gets up to, if that is a quicker one than the
		// pace it averages takes - see RAMPS: not walking at a jog's pace.)
		const up = gait(v0 + a * ta);
		if (
			move === undefined &&
			GAITS.has(leg.move) &&
			GAIT_ORDER.indexOf(up) > GAIT_ORDER.indexOf(leg.move)
		) {
			leg.move = up;
		}
		// (Hardly any of it at the one pace: the last step up on to the end.)
		const steps = T - ta < RAMP_STEP_MS / 2000 ? n - 1 : n;
		let from = leg.from;
		let t = leg.t0;
		let gone = 0;
		for (let i = 0; i < steps; i++) {
			gone += (v0 + a * dt * (i + 0.5)) * dt;
			const at = {
				x: leg.from.x + (dx * gone) / D,
				y: leg.from.y + (dy * gone) / D,
			};
			const t2 = leg.t0 + (i + 1) * dt * 1000;
			this.push({
				...leg,
				t0: t,
				t1: t2,
				from,
				to: at,
				...(t2 - leg.t0 <= square ? { yaw } : {}),
			});
			from = at;
			t = t2;
		}
		leg.t0 = t;
		leg.from = from;
		this.push(leg);
		return v0 + a * ta;
	}

	// To `to` by t1 from a stand, in `move` (or the gaits his pace takes)
	// and facing `face`: up to the pace that takes at RELEASE_ACCEL, on at
	// it and eased off it to a stand there (see easeOff) - harder, if it
	// takes that (STOP_HARD) - not off at it all at once and stopped dead
	// on it. No quicker than `most` ft/s; off as hard as `off` (ft/s/s, by
	// default RELEASE_ACCEL).
	// (Whether there was the time for it so - and he was stood: if not,
	// nothing done.)
	stepTo(
		to: Pt,
		t1: number,
		most: number,
		move?: Move,
		face?: number,
		off = RELEASE_ACCEL,
	): boolean {
		const far = dist(this.at, to);
		const T = (t1 - this.t) / 1000;
		if (legPace(this.last) >= 0.5 || far < STEP_TO_LEAST_FT) {
			return false;
		}
		// (The pace v that takes far / v + v * per seconds: T.)
		const paceAt = (down: number) => {
			const per = 1 / (2 * off) + 1 / (2 * down);
			const disc = T * T - 4 * per * far;
			const v = disc >= 0 ? (T - Math.sqrt(disc)) / (2 * per) : Infinity;
			return v <= most ? { v, down } : undefined;
		};
		const at = paceAt(STOP_ACCEL) ?? paceAt(STOP_ACCEL * STOP_HARD);
		if (!at) {
			return false;
		}
		const { v, down } = at;
		const pull = (v * v) / (2 * down);
		const u = (far - pull) / far;
		this.getOff(
			{
				x: this.at.x + (to.x - this.at.x) * u,
				y: this.at.y + (to.y - this.at.y) * u,
			},
			t1 - ((2 * pull) / v) * 1000,
			0,
			off,
			move,
			0,
			face,
		);
		this.easeOff(pull);
		return true;
	}

	// Into a man at `to`, there at the end of the leg he is on: pulled up
	// over his last strides to `ve` (at `A` ft/s/s) and on in to it over `S`
	// ms, in `into`, as that takes the rest out of him - there that much
	// later than he would have been, not run into it flat out and stopped
	// dead. Along the way he was going in at it (bent round, as it may be),
	// only off a run in a gait, since `pinned` (by default, when where he
	// has been was fixed till). (When he gets there, if he does.)
	gatherTo(
		to: Pt,
		ve: number,
		A: number,
		S: number,
		into: Move,
		pinned = this.pinned,
	): number | undefined {
		const legs = this.part.legs;
		const l = this.last;
		const v = legPace(l);
		const press = (ve * S) / 2000;
		const far = (v * v - ve * ve) / (2 * A) + press;
		const fits = (x: Leg) =>
			RAMPS.has(x.move) && !x.arms && !x.turn && !x.span && x.t1 > x.t0;
		if (
			!(v > ve) ||
			l.to.x !== to.x ||
			l.to.y !== to.y ||
			!fits(l) ||
			l.t1 <= pinned
		) {
			return undefined;
		}
		// (From that far off it along his way in - or, his run in at it
		// shorter than that, from the start of it, a little harder: see
		// GATHER_HARD - leg by leg back, none turned off the next by more
		// than GATHER_STRAIGHT.)
		const wayOf = (x: Leg) => Math.atan2(x.to.y - x.from.y, x.to.x - x.from.x);
		let k = legs.length - 1;
		let gone = 0;
		let t0 = l.t0;
		for (;;) {
			const x = legs[k]!;
			const len = dist(x.from, x.to);
			// (As far as it takes from the pace he is going on this one.)
			const vx = Math.max(ve, legPace(x));
			const need = Math.max(far, (vx * vx - ve * ve) / (2 * A) + press);
			if (gone + len >= need) {
				t0 = x.t0 + (x.t1 - x.t0) * (1 - (need - gone) / Math.max(len, 1e-6));
				break;
			}
			gone += len;
			t0 = x.t0;
			const prev = legs[k - 1];
			const turn = prev ? wayOf(x) - wayOf(prev) : 0;
			if (
				!prev ||
				!fits(prev) ||
				Math.abs(prev.t1 - x.t0) > 1 ||
				dist(prev.to, x.from) > 0.01 ||
				Math.abs(Math.atan2(Math.sin(turn), Math.cos(turn))) > GATHER_STRAIGHT
			) {
				break;
			}
			k--;
		}
		t0 = Math.max(t0, pinned + 1);
		k = legIndexAt(legs, t0);
		if (k < 0 || legs[k]!.t1 <= t0) {
			return undefined;
		}
		// (The way he goes in, from there.)
		const path = [this.posAt(t0), ...legs.slice(k).map((x) => x.to)];
		const at = [0];
		for (let i = 1; i < path.length; i++) {
			at.push(at[i - 1]! + dist(path[i - 1]!, path[i]!));
		}
		const P = at.at(-1)!;
		const along = (s: number): Pt => {
			let i = 1;
			while (i < path.length - 1 && at[i]! < s) {
				i++;
			}
			const u = clamp(
				(s - at[i - 1]!) / Math.max(at[i]! - at[i - 1]!, 1e-6),
				0,
				1,
			);
			const a = path[i - 1]!;
			const b = path[i]!;
			return { x: a.x + (b.x - a.x) * u, y: a.y + (b.y - a.y) * u };
		};
		const move = legs[k]!.move;
		const v0 = legPace(legs[k]!);
		// (Slowed evenly over what is left before he is on it.)
		const Dq = P - press;
		const Ad = (v0 * v0 - ve * ve) / (2 * Math.max(0.01, Dq));
		if (!(Dq > 0) || !(v0 > ve) || Ad > A * GATHER_HARD) {
			return undefined;
		}
		this.cut(t0);
		const td = (v0 - ve) / Ad;
		const n = Math.max(1, Math.round((td * 1000) / STOP_STEP_MS));
		const dt = td / n;
		const paceOf = (i: number) => v0 - (v0 - ve) * ((i + 0.5) / n);
		const moveOf = slower(move, n, dt, paceOf);
		for (let i = 0; i < n; i++) {
			const u = (i + 1) * dt;
			this.push(
				this.legTo(
					along(i === n - 1 ? Dq : v0 * u - (Ad * u * u) / 2),
					this.t + dt * 1000,
					moveOf(i),
				),
			);
		}
		const way = wayOf(l);
		const m = Math.round(S / HIT_PRESS_STEP_MS);
		const t1 = this.t;
		for (let j = 1; j <= m; j++) {
			const u = (1 - j / m) * (1 - j / m);
			this.go(along(P - press * u), t1 + (S * j) / m, into, way);
		}
		return this.t;
	}

	// Met on the run at t (a blocker onto him, say): pulled up from the pace
	// he is going to `ve` by then, at `A` ft/s/s, on the way he is going -
	// over his last strides to it, not stopped dead in a stride at t. Only
	// off a straight run in a gait, nothing else going on. (Whether he was.)
	brakeBy(t: number, ve: number, A: number): boolean {
		const legs = this.part.legs;
		const at = legAt(this, t);
		const v = legPace(at);
		if (!(v > ve) || at.t1 < t) {
			return false;
		}
		// (From the last moment that leaves him time to get down to it by t,
		// off the pace he was going then.)
		let t0 = t;
		let v0 = v;
		let fits = false;
		while (!fits && t0 > t - BRAKE_MOST_MS) {
			t0 -= STOP_STEP_MS / 5;
			v0 = legPace(legAt(this, t0));
			fits = ((v0 - ve) / A) * 1000 <= t - t0;
		}
		const way = Math.atan2(at.to.y - at.from.y, at.to.x - at.from.x);
		const from = legIndexAt(legs, t0);
		if (!fits || !(v0 > ve) || from < 0 || t0 <= this.pinned) {
			return false;
		}
		for (let k = from; k < legs.length && legs[k]!.t0 < t; k++) {
			const l = legs[k]!;
			const a = Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x) - way;
			if (
				!RAMPS.has(l.move) ||
				l.arms ||
				l.turn ||
				l.span ||
				l.t1 <= l.t0 ||
				Math.abs(Math.atan2(Math.sin(a), Math.cos(a))) > STRETCH_TURN
			) {
				return false;
			}
		}
		const move = legs[from]!.move;
		this.cut(t0);
		const T = (t - this.t) / 1000;
		const n = Math.max(1, Math.round((T * 1000) / STOP_STEP_MS));
		const dt = T / n;
		const cos = Math.cos(way);
		const sin = Math.sin(way);
		const paceOf = (i: number) => v0 - (v0 - ve) * ((i + 0.5) / n);
		const moveOf = slower(move, n, dt, paceOf);
		for (let i = 0; i < n; i++) {
			const pace = paceOf(i);
			const p = this.at;
			this.push(
				this.legTo(
					{ x: p.x + cos * pace * dt, y: p.y + sin * pace * dt },
					i === n - 1 ? t : this.t + dt * 1000,
					moveOf(i),
				),
			);
		}
		return true;
	}

	// Off from still into a man at `to` by t1 (see hitMs): up to speed at
	// `A` ft/s/s, no quicker than `top`, and - got going quicker than `ve`
	// - gathered into him at `Ad` ft/s/s, there at `ve`: not into him at all
	// he got up to and stopped dead. Still facing the way he was over the
	// first `square` ms. (The pace he gets to him at.) (Short of time for
	// it at `top`, no quicker than `most`.)
	hitInto(
		to: Pt,
		t1: number,
		top: number,
		A: number,
		ve: number,
		Ad: number,
		move: Move,
		square: number,
		most = top,
	): number {
		const D = dist(this.at, to);
		let T = (t1 - this.t) / 1000;
		// (Short of time for it: gathered as much as he can be - harder, and
		// into him quicker - see HIT_SHORT; or got going quicker first, and
		// gathered from that.)
		let vc = Infinity;
		const tries = [top, ...(most > top ? [most] : [])].flatMap((lim) =>
			HIT_SHORT.map(([k, u]) => [k, u, lim] as const),
		);
		for (const [k, u, lim] of tries) {
			const vk = ve * k;
			const Au = Ad * u;
			const qa = 1 / (2 * A) + 1 / (2 * Au);
			const qc = (vk * vk) / (2 * Au) + D;
			// (Given just the time it takes - see hitMs - or a hair less:
			// there that much later, not harder into him - see HIT_LATE_MS.)
			const least = 2 * Math.sqrt(qa * qc) - vk / Au;
			const Tk = least > T && least - T <= HIT_LATE_MS / 1000 ? least : T;
			const qb = Tk + vk / Au;
			const disc = qb * qb - 4 * qa * qc;
			vc =
				disc >= -1e-9 * qb * qb
					? (qb - Math.sqrt(Math.max(0, disc))) / (2 * qa)
					: Infinity;
			if (vc <= lim * RAMP_OVER) {
				ve = vk;
				Ad = Au;
				T = Tk;
				t1 = this.t + T * 1000;
				top = lim;
				break;
			}
		}
		if (D <= 0 || T <= 0 || !(vc > ve) || vc > top * RAMP_OVER) {
			return this.getOff(to, t1, 0, A, move, square);
		}
		const sd = (vc * vc - ve * ve) / (2 * Ad);
		const td = (vc - ve) / Ad;
		const cos = (to.x - this.at.x) / D;
		const sin = (to.y - this.at.y) / D;
		this.getOff(
			{ x: to.x - cos * sd, y: to.y - sin * sd },
			t1 - td * 1000,
			0,
			A,
			move,
			square,
		);
		const n = Math.max(1, Math.round((td * 1000) / STOP_STEP_MS));
		const dt = td / n;
		for (let i = 0; i < n; i++) {
			const pace = vc - (vc - ve) * ((i + 0.5) / n);
			const at = this.at;
			this.push(
				this.legTo(
					i === n - 1
						? to
						: { x: at.x + cos * pace * dt, y: at.y + sin * pace * dt },
					i === n - 1 ? t1 : this.t + dt * 1000,
					move,
				),
			);
		}
		return ve;
	}

	// Pulled up to a stand by the end of the leg he is on, going at a pace
	// he could not stop dead from (see slowInto).
	pullUpOn() {
		const l = this.last;
		if (GAITS.has(l.move) && legPace(l) >= SLOW_DROP) {
			this.slowInto(0, STOP_ACCEL);
		}
	}

	// Broken down by the end of the leg he is on, into what he meets there:
	// down to `keep` of his pace over the last of it (see slowInto) - at A
	// ft/s/s, there as much as `spare` ms later for it.
	breakDown(keep: number, A = STOP_ACCEL, spare = 0) {
		const l = this.last;
		if (GAITS.has(l.move) && legPace(l) * (1 - keep) >= SLOW_DROP) {
			this.slowInto(keep, A, spare);
		}
	}

	// Cutting to `to` by t1 off the leg he is on: down to `keep` of his pace
	// over the last of it (see CUT_ACCEL), his foot planted, and off the new
	// way up from there.
	// (The pace he comes out of it at.)
	cutTo(to: Pt, t1: number, keep: number): number | undefined {
		// (There early for what is left of it - the last short way of a
		// route to where it is caught, say: down into the cut to the pace
		// he goes on out of it at, not cut at his pace and stopped dead.)
		const l = this.last;
		const v = legPace(l);
		const on = dist(l.to, to) / Math.max((t1 - l.t1) / 1000, 1e-3);
		let ve = this.slowInto(v > 0 && on < keep * v ? on / v : keep, CUT_ACCEL);
		if (ve !== undefined && this.roundCut(to, t1)) {
			ve = legPace(this.last);
		}
		const leg = this.legTo(to, t1);
		this.rampInto(leg, ve);
		this.push(leg);
		return ve;
	}

	// Out of a cut on the move toward `to` (see cutTo): round it, at the
	// pace he is down to, in an arc as tight as he takes at it (see
	// CUT_LAT) - in off the last of the way he came in to it straight, and
	// out onto the way on - not turned square in a blink. Not for less
	// than BEND_LEAST_FT of it, nor if it leaves him no time to be there
	// by t1 at no more than PLANT_ON times the pace he would have been.
	// (Whether he was.)
	private roundCut(to: Pt, t1: number): boolean {
		const legs = this.part.legs;
		const l = this.last;
		const v = legPace(l);
		const dOut = dist(l.to, to);
		if (
			!RAMPS.has(l.move) ||
			l.arms ||
			l.turn ||
			l.span ||
			v < CUT_ROUND_PACE ||
			dOut <= 0
		) {
			return false;
		}
		const turn = angleAt(l.from, l.to, to);
		if (!(turn > BEND_LEAST && turn <= PLANT_TURN)) {
			return false;
		}
		const way = Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x);
		const want = ((v * v) / CUT_LAT) * Math.tan(turn / 2);
		// (As far back as he came in straight at it, nothing else going on,
		// since he was pinned.)
		let straight = 0;
		for (let k = legs.length - 1; k >= 0 && straight < want; k--) {
			const lk = legs[k]!;
			const a = Math.atan2(lk.to.y - lk.from.y, lk.to.x - lk.from.x) - way;
			if (
				!RAMPS.has(lk.move) ||
				lk.arms ||
				lk.turn ||
				lk.span ||
				lk.t1 <= lk.t0 ||
				lk.t0 < this.pinned ||
				lk.yaw !== l.yaw ||
				lk.mirrored !== l.mirrored ||
				lk.ball !== l.ball ||
				dist(lk.from, lk.to) <= 0 ||
				Math.abs(Math.atan2(Math.sin(a), Math.cos(a))) > STRETCH_TURN
			) {
				break;
			}
			straight += dist(lk.from, lk.to);
		}
		const back = Math.min(want, straight * BEND_IN, dOut * BEND_OUT);
		if (back < BEND_LEAST_FT) {
			return false;
		}
		let k = legs.length - 1;
		let left = back;
		while (dist(legs[k]!.from, legs[k]!.to) <= left) {
			left -= dist(legs[k]!.from, legs[k]!.to);
			k--;
		}
		const lk = legs[k]!;
		const f = 1 - left / Math.max(dist(lk.from, lk.to), 1e-6);
		const corner = l.to;
		const { yaw, ...was } = l;
		const keep =
			yaw !== undefined &&
			Math.abs(Math.atan2(Math.sin(yaw - way), Math.cos(yaw - way))) >
				BEND_LEAST;
		const start = {
			x: lk.from.x + (lk.to.x - lk.from.x) * f,
			y: lk.from.y + (lk.to.y - lk.from.y) * f,
		};
		const tIn = Math.max(dist(start, corner), 1e-6);
		const u = { x: (corner.x - start.x) / tIn, y: (corner.y - start.y) / tIn };
		const o = { x: (to.x - corner.x) / dOut, y: (to.y - corner.y) / dOut };
		const round = Math.acos(clamp(u.x * o.x + u.y * o.y, -1, 1));
		if (!(round > BEND_LEAST && round <= PLANT_TURN)) {
			return false;
		}
		const R = tIn / Math.tan(round / 2);
		// (No quicker round it than it is wide enough for, cut short.)
		const pace = Math.min(v, Math.sqrt(CUT_LAT * R));
		const tStart = lk.t0 + (lk.t1 - lk.t0) * f;
		const tEnd = tStart + ((R * round) / pace) * 1000;
		const was0 = dOut / Math.max((t1 - l.t1) / 1000, 1e-3);
		// (Nor if it has him there so early he as good as stops dead out of
		// it - the last short way of a route to where it is caught, say:
		// cut, as it was timed, slowing into it and up out of it.)
		const after = (dOut - tIn) / ((t1 - tEnd) / 1000);
		if (
			!(t1 > tEnd) ||
			after > Math.max(was0, pace) * PLANT_ON ||
			after < pace * ROUND_EARLY
		) {
			return false;
		}
		legs.splice(k + 1);
		lk.to = start;
		lk.t1 = tStart;
		const s = Math.sign(u.x * o.y - u.y * o.x) || 1;
		const c = { x: start.x - s * u.y * R, y: start.y + s * u.x * R };
		const r0 = Math.atan2(start.y - c.y, start.x - c.x);
		const n = Math.max(2, Math.ceil(round / BEND_STEP));
		const dt = ((R * round) / n / pace) * 1000;
		for (let i = 1; i <= n; i++) {
			const q = r0 + (s * round * i) / n;
			const at = this.last;
			this.push({
				...was,
				...(keep ? { yaw } : {}),
				t0: at.t1,
				t1: at.t1 + dt,
				from: at.to,
				to: { x: c.x + Math.cos(q) * R, y: c.y + Math.sin(q) * R },
			});
		}
		return true;
	}

	// Sat down at the end of the stretch he is on, there when it has him
	// there (a receiver at the end of his route - see runRoute): up out of
	// the cut into it from `vs` ft/s (as he came out of it - or, unset, as
	// slowInto has it), as quick as `top`, and pulled up into where it ends
	// - not run in flat at the pace that averages and stopped dead.
	gatherInto(vs: number | undefined, top: number) {
		const l = this.last;
		if (GAITS.has(l.move) && legPace(l) >= SLOW_DROP) {
			this.slowInto(0, STOP_ACCEL, 0, { vs, top });
		}
	}

	private legTo(
		to: Pt,
		t1: number,
		move?: Move,
		yaw?: number,
		extra?: Partial<Leg>,
	): Leg {
		const l = this.last;
		const p = onField(to, this.roam, this.endRoam);
		const t0 = l.t1;
		const end = Math.max(t1, t0 + 1);
		const fps = dist(l.to, p) / ((end - t0) / 1000);
		let way = faced(paced(move, fps, end - t0), yaw, l.to, p, fps, l);
		// (Hardly going anywhere, a good while: stood there, not stepping in
		// place - and facing the way he was.)
		if (STEPS.has(way.move) && fps < CRAWL_FPS && end - t0 >= GAIT_LEAST_MS) {
			way = { move: "stand", yaw: way.yaw ?? this.yaw };
		}
		return {
			t0,
			t1: end,
			from: l.to,
			to: p,
			move: way.move,
			...(way.yaw !== undefined ? { yaw: way.yaw } : {}),
			...extra,
		};
	}

	// Pulling up to a stand at the end of his last leg (see STOP_ACCEL) - or
	// down to `keep` of his pace, at `A` ft/s/s: over the whole stretch he
	// is on (see stretchFrom), off the pace he came into it at, up to speed
	// on it and down at the last, there on time - or, no time for that and
	// `spare` ms after it to make it up in, there later (still to be there
	// HOLD_LEFT_MS before it is up). Or, none of that doing it, as slowLast.
	// (The pace he is down to.)
	// (`shape`, laid out to be run that way - see gatherInto: up from
	// `vs`, unless unset, and as quick as `top`, however slow he goes on
	// it as it is.)
	private slowInto(
		keep = 0,
		A = STOP_ACCEL,
		spare = 0,
		shape?: { vs: number | undefined; top: number },
	): number | undefined {
		const legs = this.part.legs;
		const leg = this.last;
		const ms = leg.t1 - leg.t0;
		if (!RAMPS.has(leg.move) || ms <= 0 || leg.arms || leg.turn) {
			return undefined;
		}
		const j = this.stretchFrom();
		const first = legs[j]!;
		const T = (leg.t1 - first.t0) / 1000;
		const D = dist(first.from, leg.to);
		const vl = dist(leg.from, leg.to) / (ms / 1000);
		if (T * 1000 < RAMP_LEG_MS || (vl < STOP_LEAST && !shape) || D <= 0) {
			return this.slowLast(keep, A);
		}
		const cos = (leg.to.x - first.from.x) / D;
		const sin = (leg.to.y - first.from.y) / D;
		const before = legs[j - 1];
		const vf = dist(first.from, first.to) / ((first.t1 - first.t0) / 1000);
		const vs =
			shape?.vs !== undefined
				? Math.min(shape.vs, vf)
				: !before || SET.has(before.move)
					? vf
					: Math.min(vf, paceOn(before, first.from, leg.to));
		const ve = vl * keep;
		// (Up out of his stance as he got up out of it - see rampInto.)
		const Aup = !before || SET.has(before.move) ? RELEASE_ACCEL : RAMP_ACCEL;
		const most = Math.max(
			vl,
			Math.min((D / T) * RAMP_OVER, RAMP_TOP),
			shape ? Math.min(shape.top, RAMP_TOP) : 0,
		);
		// (Up from vs to vc, on at it, and down to ve at Ad: as slow a vc as
		// gets him there on time - or, too quick, at no more than `most`, as
		// soon as that gets him there. Down at A if that does it, harder if
		// it takes it - see STOP_HARD. Or, none of that doing it, on at up
		// to RAMP_FAST times the pace he was going at - the stretch run at an
		// even pace that had the slowing at the end of it in it.)
		const fast = Math.max(most, Math.min(this.top, RAMP_TOP, vl * RAMP_FAST));
		const room = Math.max(0, spare - HOLD_LEFT_MS) / 1000;
		const cruise = (Ad: number, Au: number) => {
			const qa = 1 / (2 * Au) + 1 / (2 * Ad);
			const qb = T + vs / Au + ve / Ad;
			const qc = (vs * vs) / (2 * Au) + (ve * ve) / (2 * Ad) + D;
			const disc = qb * qb - 4 * qa * qc;
			return {
				qa,
				qc,
				vc: disc >= 0 ? (qb - Math.sqrt(disc)) / (2 * qa) : Infinity,
			};
		};
		const plan = (Ad: number, most: number, Au = Aup) => {
			const { qa, qc, vc: at } = cruise(Ad, Au);
			let vc = at;
			let Tn = T;
			if (vc > most) {
				vc = Math.min(most, Math.sqrt(qc / qa));
				Tn = (D + (vc - vs) ** 2 / (2 * Au) + (vc - ve) ** 2 / (2 * Ad)) / vc;
			}
			const ta = (vc - vs) / Au;
			const td = (vc - ve) / Ad;
			const tc = Tn - ta - td;
			return Tn <= T + room &&
				vc >= vs &&
				vc >= ve &&
				ta >= 0 &&
				tc > -1e-6 &&
				td > 0
				? { vc, Tn, ta, td, tc: Math.max(0, tc), Ad, Au }
				: undefined;
		};
		// (Or, the stretch laid up to speed harder or softer than that - see
		// rampInto, getOff - up as hard as does it, within RAMP_HARD times it
		// either way: harder for a quicker pace to be on at, softer for one
		// he would be slowing up from to what he is down to at the end.)
		const fit = (Ad: number, most: number) => {
			let lo = Aup / RAMP_HARD;
			let hi = Aup * RAMP_HARD;
			for (let i = 0; i < 12; i++) {
				const Au = (lo + hi) / 2;
				const p = plan(Ad, most, Au);
				if (p) {
					return p;
				}
				const { vc } = cruise(Ad, Au);
				if (vc <= Math.max(vs, ve)) {
					hi = Au;
				} else if (vc > most) {
					lo = Au;
				} else {
					return undefined;
				}
			}
			return undefined;
		};
		const p =
			plan(A, most) ??
			plan(A * STOP_HARD, most) ??
			plan(A, fast) ??
			plan(A * STOP_HARD, fast) ??
			fit(A, most) ??
			fit(A * STOP_HARD, most) ??
			fit(A, fast) ??
			fit(A * STOP_HARD, fast);
		if (!p) {
			return this.slowLast(keep, A);
		}
		const { vc, Tn, ta, td, tc, Ad, Au } = p;
		const sa = vs * ta + (Au * ta * ta) / 2;
		const sAt = (t: number) => {
			if (t <= ta) {
				return vs * t + (Au * t * t) / 2;
			}
			if (t <= ta + tc) {
				return sa + vc * (t - ta);
			}
			const u = Math.min(t - ta - tc, td);
			return sa + vc * tc + vc * u - (Ad * u * u) / 2;
		};
		// (In the gait he was in there, getting up to speed and on - and
		// slowing from it as he slows.)
		const old = legs.splice(j);
		const moveAt = (s: number) => {
			let gone = 0;
			for (const o of old) {
				gone += dist(o.from, o.to);
				if (gone >= s) {
					return o.move;
				}
			}
			return leg.move;
		};
		const na =
			ta > 0.001 ? Math.max(1, Math.round((ta * 1000) / RAMP_STEP_MS)) : 0;
		const nd = Math.max(1, Math.round((td * 1000) / STOP_STEP_MS));
		const marks: number[] = [];
		for (let i = 1; i <= na; i++) {
			marks.push((ta * i) / na);
		}
		if (tc > 0.001) {
			marks.push(ta + tc);
		}
		const dt = td / nd;
		const paceOf = (i: number) => vc - (vc - ve) * ((i + 0.5) / nd);
		const downOf = slower(leg.move, nd, dt, paceOf);
		const t0 = first.t0;
		let from = first.from;
		let prev = 0;
		const put = (t: number, move: Move, last: boolean) => {
			const s = sAt(t);
			const to = last
				? leg.to
				: { x: first.from.x + cos * s, y: first.from.y + sin * s };
			legs.push({
				...leg,
				t0: t0 + prev * 1000,
				t1: t0 + t * 1000,
				from,
				to,
				move,
			});
			from = to;
			prev = t;
		};
		for (const t of marks) {
			put(t, moveAt((sAt(prev) + sAt(t)) / 2), false);
		}
		for (let i = 0; i < nd; i++) {
			const last = i === nd - 1;
			put(last ? Tn : ta + tc + dt * (i + 1), downOf(i), last);
		}
		return ve;
	}

	// How far back his last legs are all one stretch: the same way (to
	// within STRETCH_TURN radians), in a gait, nothing else going on, and
	// no slower than those after them (getting up to speed, and on) - no
	// longer ago than STRETCH_MS. (The first of them.)
	private stretchFrom(): number {
		const legs = this.part.legs;
		const l = this.last;
		const way = Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x);
		let j = legs.length - 1;
		while (j > 0) {
			const p = legs[j - 1]!;
			const n = legs[j]!;
			if (
				!RAMPS.has(p.move) ||
				p.arms ||
				p.turn ||
				p.span ||
				p.t1 <= p.t0 ||
				p.yaw !== l.yaw ||
				p.mirrored !== l.mirrored ||
				p.ball !== l.ball ||
				l.t1 - p.t0 > STRETCH_MS ||
				dist(p.from, p.to) <= 0 ||
				legPace(p) > legPace(n) + STRETCH_SLACK
			) {
				break;
			}
			const a = Math.atan2(p.to.y - p.from.y, p.to.x - p.from.x) - way;
			if (Math.abs(Math.atan2(Math.sin(a), Math.cos(a))) > STRETCH_TURN) {
				break;
			}
			j--;
		}
		return j;
	}

	// Pulling up at the end of his last leg alone: down to `keep` of his
	// pace over the last of it, the rest of it that much quicker (see
	// RAMP_OVER) - or from no quicker, down as far as that takes him. (The
	// pace he is down to.)
	private slowLast(keep: number, A: number): number | undefined {
		const legs = this.part.legs;
		const leg = this.last;
		const T = (leg.t1 - leg.t0) / 1000;
		const D = dist(leg.from, leg.to);
		const v = D / T;
		if (
			!RAMPS.has(leg.move) ||
			T * 1000 < RAMP_LEG_MS ||
			v < STOP_LEAST ||
			leg.arms ||
			leg.turn
		) {
			return undefined;
		}
		let ve = v * keep;
		const room = T * T - (2 * (D - ve * T)) / A;
		let vc = room >= 0 ? ve + A * (T - Math.sqrt(room)) : Infinity;
		const most = Math.max(v, Math.min(v * RAMP_OVER, RAMP_TOP));
		if (vc > most) {
			vc = most;
			ve = Math.max(0, vc - Math.sqrt(2 * A * (vc * T - D)));
		}
		const td = (vc - ve) / A;
		if (!(td * 1000 >= CUT_SLIVER_MS && td < T)) {
			return undefined;
		}
		const cos = (leg.to.x - leg.from.x) / D;
		const sin = (leg.to.y - leg.from.y) / D;
		const end = leg.to;
		const t1 = leg.t1;
		const tc = t1 - td * 1000;
		leg.to = {
			x: leg.from.x + cos * vc * (T - td),
			y: leg.from.y + sin * vc * (T - td),
		};
		leg.t1 = tc;
		const n = Math.max(1, Math.round((td * 1000) / STOP_STEP_MS));
		const dt = td / n;
		let gone = 0;
		const paceOf = (i: number) => vc - (vc - ve) * ((i + 0.5) / n);
		const moveOf = slower(leg.move, n, dt, paceOf);
		for (let i = 0; i < n; i++) {
			const pace = paceOf(i);
			gone += pace * dt;
			const last = i === n - 1;
			const l = this.last;
			legs.push({
				...leg,
				t0: l.t1,
				t1: last ? t1 : tc + (i + 1) * dt * 1000,
				from: l.to,
				to: last ? end : { x: leg.to.x + cos * gone, y: leg.to.y + sin * gone },
				move: moveOf(i),
			});
		}
		return ve;
	}

	// On the way he was going, at the pace he was, until t1 (no more than
	// CARRY_MOST_MS of it, and eased off from there) - or, not going
	// anywhere much, stood there till then.
	carryOn(t1: number) {
		// (Already there, as good as - a hair short of it, in the sums: not
		// a step of no time at all on the end of his run, and nothing of
		// his pace left in it for the next to go on at - see legTo.)
		if (t1 - this.t < 1) {
			return;
		}
		const l = this.last;
		const v = legPace(l);
		const far = dist(l.from, l.to);
		if (!GAITS.has(l.move) || v < STOP_LEAST || t1 <= this.t) {
			// (Backpedaling, or in his low steps, quick: eased off the way he
			// was going - not stopped dead in them.)
			if (STEPS.has(l.move) && v >= STOP_LEAST && t1 > this.t) {
				this.easeOff(EASE_OFF_FT);
			}
			this.hold(t1);
			return;
		}
		const ms = Math.min(t1 - this.t, CARRY_MOST_MS);
		const on = (v * ms) / 1000 / far;
		this.go(
			{
				x: l.to.x + (l.to.x - l.from.x) * on,
				y: l.to.y + (l.to.y - l.from.y) * on,
			},
			this.t + ms,
			l.move,
			l.yaw,
		);
		if (this.t < t1) {
			this.easeOff(EASE_OFF_FT);
			this.hold(t1);
		}
	}

	// About to go off toward `to`, well round from the way he is going
	// (more than CUT_LEAST): planted first - on the way he was going, down
	// to as much of his pace as he can take round with him (as on a route,
	// see breakOf), or to `down` ft/s, at CUT_ACCEL - not turned round at
	// full speed.
	// Backpedaling, and off it another way: planted first - down off his
	// backpedal at BACK_PLANT_ACCEL, still facing the way he was, to as much
	// of his pace as goes on the new way - not turned and gone at once from
	// all his pace back.
	plantBack(to: Pt) {
		const l = this.last;
		const v = legPace(l);
		if (l.move !== "back" || v < STOP_LEAST || l.arms || l.turn) {
			return;
		}
		const turn = angleAt(l.from, l.to, to);
		if (!(turn > CUT_LEAST)) {
			return;
		}
		const far = dist(l.from, l.to);
		const cos = (l.to.x - l.from.x) / far;
		const sin = (l.to.y - l.from.y) / far;
		const ve = v * Math.max(0, Math.cos(turn));
		const T = (v - ve) / BACK_PLANT_ACCEL;
		const n = Math.max(1, Math.round((T * 1000) / STOP_STEP_MS));
		const dt = T / n;
		for (let i = 0; i < n; i++) {
			const pace = v - (v - ve) * ((i + 0.5) / n);
			this.go(
				{ x: this.at.x + cos * pace * dt, y: this.at.y + sin * pace * dt },
				this.t + dt * 1000,
				"back",
				l.yaw,
			);
		}
	}

	// How far he can go on from where he is, the way (cos, sin), before the
	// edge of where he can go (see onField) - and whether that is an end
	// line (or a sideline).
	private roomOn(cos: number, sin: number): { ft: number; end: boolean } {
		const p = this.at;
		const lo = 1 - this.roam;
		const hi = FIELD_W - 1 + this.roam;
		const xs =
			cos < -1e-9
				? (p.x - 1 + this.endRoam) / -cos
				: cos > 1e-9
					? (FIELD_L - 1 + this.endRoam - p.x) / cos
					: Infinity;
		const ys =
			sin < -1e-9
				? (p.y - lo) / -sin
				: sin > 1e-9
					? (hi - p.y) / sin
					: Infinity;
		return { ft: Math.max(0, Math.min(xs, ys)), end: xs <= ys };
	}

	// Run at `to`, over an end line (see onField) or no more than
	// EDGE_NEAR_FT short of it, head on (no more than EDGE_ALONG of the way
	// along it): where he comes up to it, EDGE_SHORT_FT short (or `to`, if
	// that is shorter) - to be pulled up into there. (None: not near it,
	// across it more than at it, or there already. Not a sideline: run up
	// to that, he is pulled up as he is stood there - see hold.)
	private edgeOf(to: Pt): Pt | undefined {
		const far = dist(this.at, to);
		if (far <= 0) {
			return undefined;
		}
		const cos = (to.x - this.at.x) / far;
		const sin = (to.y - this.at.y) / far;
		const { ft, end } = this.roomOn(cos, sin);
		const on = Math.min(far, ft - EDGE_SHORT_FT);
		if (
			ft - far > EDGE_NEAR_FT ||
			!end ||
			Math.abs(sin) > EDGE_ALONG ||
			on < EDGE_LEAST_FT
		) {
			return undefined;
		}
		return { x: this.at.x + cos * on, y: this.at.y + sin * on };
	}

	// Turned toward `to`, to go on at `w` ft/s: bent round into it, if he
	// can be (see bendInto) - or planted for it (see plantFor), down to `w`
	// if that is slower than he would take it round at: not round it
	// quicker and braked to it in a stride after.
	turnFor(to: Pt, w: number) {
		if (!(w < BEND_PACE ? this.bendSlow(to, w) : this.bendInto(to, w))) {
			this.plantFor(to, undefined, w, w);
		}
	}

	// Turning toward `to`, to go on slower than he can bend round at
	// (BEND_PACE) - pulling up on the way to it, say: bent round it all
	// the same, easing off toward `w` as he goes - see bendInto - not on
	// at full tilt and turned in a stride. If that leaves him the room
	// after it to get down to it (see slowOn) - not round it near flat
	// out and braked to it in a stride after. (Whether he was.)
	private bendSlow(to: Pt, w: number): boolean {
		const was = this.part.legs.map((l) => ({ ...l }));
		const { yaw, pinned } = this;
		if (!this.bendInto(to, BEND_PACE, w)) {
			return false;
		}
		const ve = legPace(this.last);
		const rem = Math.max(dist(this.at, to), 0.01);
		if ((ve * ve - w * w) / (2 * rem) <= STOP_ACCEL * STOP_HARD) {
			return true;
		}
		this.part.legs.splice(0, this.part.legs.length, ...was);
		this.yaw = yaw;
		this.pinned = pinned;
		return false;
	}

	// What he is doing as it stands, to go back to - see
	// Action.incomplete.
	saved(): () => void {
		const legs = this.part.legs.map((l) => ({ ...l }));
		const { yaw, pinned } = this;
		return () => {
			this.part.legs.splice(
				0,
				this.part.legs.length,
				...legs.map((l) => ({ ...l })),
			);
			this.yaw = yaw;
			this.pinned = pinned;
		};
	}

	// Toward `to` by t1: bent round into it only, if he can be (see
	// bendInto) - eased off round it toward the pace that has him there
	// then, or, that leaving him late, round it at his pace - there by
	// then at no more than a little quicker than he comes out of it, or
	// at that, no more than `late` ms late for it (BEND_LATE_MS): not the
	// rest of the way in a blink, the time gone on the way round. (When he
	// is there, if he was.)
	bendFor(to: Pt, t1: number, late = BEND_LATE_MS): number | undefined {
		const was = this.part.legs.map((l) => ({ ...l }));
		const { yaw, pinned } = this;
		const v = legPace(this.last);
		const w = dist(this.at, to) / Math.max(0.001, (t1 - this.t) / 1000);
		for (const on of w < v ? [w, v] : [w]) {
			if (!this.bendInto(to, on)) {
				return undefined;
			}
			const out = Math.max(on, legPace(this.last));
			const far = dist(this.at, to);
			if (far <= ((out + RAMP_LEAST) * Math.max(0, t1 - this.t)) / 1000) {
				return t1;
			}
			const at = this.t + (far / out) * 1000;
			if (at <= t1 + late) {
				return at;
			}
			this.part.legs.splice(
				0,
				this.part.legs.length,
				...was.map((l) => ({ ...l })),
			);
			this.yaw = yaw;
			this.pinned = pinned;
		}
		return undefined;
	}

	plantFor(to: Pt, down?: number, most?: number, on?: number) {
		const l = this.last;
		// (Off a backpedal: see plantBack.)
		if (l.move === "back") {
			this.plantBack(to);
			return;
		}
		const v = legPace(l);
		if (!RAMPS.has(l.move) || v < STOP_LEAST || l.arms || l.turn) {
			return;
		}
		const turn = angleAt(l.from, l.to, to);
		if (!(turn > CUT_LEAST)) {
			return;
		}
		const far = dist(l.from, l.to);
		const cos = (l.to.x - l.from.x) / far;
		const sin = (l.to.y - l.from.y) / far;
		let { ve, want } = plantedFor(v, l.to, cos, sin, to, down, most);
		// (No room to plant for it short of where he goes: bent round into
		// it, if he can be.)
		// (Easing off round it toward `on`, the pace he goes on at after,
		// if that is slower: see bendInto.)
		if (ve - want > (v - want) / 2 && this.bendInto(to, v, on ?? v)) {
			return;
		}
		// (Or, still no room for it: planted harder - STOP_HARD times -
		// down to what he can take round from where that has him, not left
		// going at it near flat out and round at a crawl after.)
		let hard = CUT_ACCEL;
		if (ve - want > PLANT_SHORT_FPS) {
			({ ve, want } = plantedFor(
				v,
				l.to,
				cos,
				sin,
				to,
				down,
				most,
				CUT_ACCEL * STOP_HARD,
				PLANT_HARD_TRIES,
			));
			hard = CUT_ACCEL * STOP_HARD;
		}
		// (Not even that: near it, too near to be down for it short of it
		// even so - all the way down for it, on past it as far as that takes
		// him - not round from most of his pace at a crawl in a stride, back
		// to it.)
		if (ve - want > PLANT_SHORT_FPS) {
			({ ve, want } = plantedFor(
				v,
				l.to,
				cos,
				sin,
				to,
				down,
				most,
				CUT_ACCEL * STOP_HARD,
				PLANT_HARD_TRIES,
				Infinity,
			));
		}
		if (!(ve < v)) {
			return;
		}
		// (No room for it short of the edge of where he can go - the end
		// line, say: slowed as he comes up to it, over the stretch he is on,
		// to as much of his pace as he can plant from in the room there is -
		// or, that not doing it, planted harder (see STOP_HARD) - not run
		// into it and stopped dead in a stride.)
		const room = this.roomOn(cos, sin).ft - EDGE_SHORT_FT;
		let w = v;
		let A = hard;
		if ((v * v - ve * ve) / (2 * CUT_ACCEL) > room) {
			const vr = Math.sqrt(ve * ve + 2 * CUT_ACCEL * Math.max(0, room));
			if (
				vr < v &&
				this.slowInto(vr / v, CUT_ACCEL, EDGE_SPARE_MS) !== undefined
			) {
				w = legPace(this.last);
			}
			A = clamp(
				(w * w - ve * ve) / (2 * Math.max(room, 0.1)),
				hard,
				CUT_ACCEL * STOP_HARD,
			);
		}
		if (!(ve < w)) {
			return;
		}
		const k = this.last;
		const T = (w - ve) / A;
		const n = Math.max(1, Math.round((T * 1000) / STOP_STEP_MS));
		const dt = T / n;
		const paceOf = (i: number) => w - (w - ve) * ((i + 0.5) / n);
		const moveOf = slower(k.move, n, dt, paceOf);
		for (let i = 0; i < n; i++) {
			const pace = paceOf(i);
			this.go(
				{ x: this.at.x + cos * pace * dt, y: this.at.y + sin * pace * dt },
				this.t + dt * 1000,
				moveOf(i),
				l.yaw,
			);
		}
	}

	// Into where his run ends, off it at `least` ft/s or more: the last of
	// it eased off over twice `ft` (see easeOff) - not stopped dead on it,
	// there a moment later for it.
	easeIn(ft: number, least: number) {
		const l = this.last;
		const v = legPace(l);
		// (No harder than a man pulls up hard - see STOP_HARD - in off a
		// sprint: over further than `ft`, if it takes that.)
		const want = Math.max(ft, (v * v) / (4 * STOP_ACCEL * STOP_HARD));
		const t = this.t - ((2 * want) / Math.max(1, v)) * 1000;
		// (Back over the strides he got up to speed in, if it takes that -
		// not only the last of them.)
		const legs = this.part.legs;
		let k = legs.length - 1;
		while (
			k > 0 &&
			legs[k]!.t0 > t &&
			legs[k - 1]!.t1 === legs[k]!.t0 &&
			RAMPS.has(legs[k - 1]!.move)
		) {
			k--;
		}
		// (Back no further than that run goes: over as much of it as there
		// is, if that is as far as `ft`.)
		const from = Math.max(t, legs[k]!.t0 + 1);
		const by = (v * (this.t - from)) / 2000;
		if (RAMPS.has(l.move) && v >= least && by >= ft) {
			this.cut(from);
			this.easeOff(2 * by);
		}
	}

	// No sooner than t, when he can be stood from the pace he is going,
	// pulled up as hard as he can be (see STOP_HARD) - not from a sprint
	// to a stand in a stride, for a look round at t.
	stoppedBy(t: number): number {
		const l = this.last;
		const v = RAMPS.has(l.move) && !l.arms && !l.turn ? legPace(l) : 0;
		return v < STOP_LEAST
			? t
			: Math.max(t, this.t + (v / (STOP_ACCEL * STOP_HARD)) * 1000);
	}

	// Too quick to get no further than `to` - by t1, or at all - pulling up
	// as hard as he can (see STOP_HARD): pulled up that hard on the way he
	// is going, on past it (see easeOff) - not braked to it in a stride.
	// (Whether he was.)
	pullsPast(to: Pt, t1 = Infinity): boolean {
		const l = this.last;
		const v = RAMPS.has(l.move) && !l.arms && !l.turn ? legPace(l) : 0;
		if (v < STOP_LEAST) {
			return false;
		}
		const A = STOP_ACCEL * STOP_HARD;
		const T = Math.max(0, t1 - this.t) / 1000;
		const least = v > A * T ? v * T - (A * T * T) / 2 : (v * v) / (2 * A);
		if (!(dist(this.at, to) < least - PAST_SLACK_FT)) {
			return false;
		}
		this.easeOff((v * v) / (2 * A));
		return true;
	}

	// On the way he was going, slowing to a stop no more than `most` feet
	// on (see STOP_ACCEL) - bent off to the side of `past` (the pile at the
	// ball), as little as takes him by it, if he would run on through it.
	easeOff(most: number, past?: Pt) {
		const { legs } = this.part;
		let li = legs.length - 1;
		// (Off a sliver of a leg, cut off as it began - see EASE_SLIVER_MS:
		// the way and the pace of the one it follows on from, not whatever
		// jump there is in that sliver of it.)
		const sliver = legs[li];
		const before = legs[li - 1];
		if (
			sliver &&
			before &&
			sliver.t1 - sliver.t0 < EASE_SLIVER_MS &&
			before.t1 === sliver.t0 &&
			before.t1 - before.t0 >= EASE_SLIVER_MS &&
			before.move === sliver.move
		) {
			li -= 1;
		}
		const l = legs[li] ?? this.last;
		const ms = l.t1 - l.t0;
		const far = dist(l.from, l.to);
		if (ms <= 0 || far <= 0) {
			return;
		}
		// (Off the end of an act - a swat at the ball, say - stepped down out
		// of it: not that act over again, from the start, every step. Off
		// what it leaves him going at, if he came into it quicker - see
		// pace.ts carriedTo - not its own pace.)
		const act = moveKind(l.move) === "act";
		const v = (far / (ms / 1000)) * (act ? carriedTo(carriedIn(legs, li)) : 1);
		const d = Math.min(most, (v * v) / (2 * STOP_ACCEL));
		if (d < 0.5) {
			return;
		}
		const way = Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x);
		const gaited = act || GAITS.has(l.move);
		const T = (2 * d) / v;
		const n = Math.max(1, Math.round((T * 1000) / STOP_STEP_MS));
		const dt = T / n;
		const paceOf = (i: number) => v * (1 - (i + 0.5) / n);
		const moveOf = act
			? (i: number) => gait(paceOf(i))
			: slower(l.move, n, dt, paceOf);
		const stepsOf = (bend: number, by = 1) => {
			const out: Pt[] = [];
			let at = this.at;
			for (let i = 0; i < n; i++) {
				const h = way + bend * Math.min(1, (i + 0.5) / (n * by));
				const ft = paceOf(i) * dt;
				at = { x: at.x + Math.cos(h) * ft, y: at.y + Math.sin(h) * ft };
				out.push(at);
			}
			return out;
		};
		let steps = stepsOf(0);
		if (gaited && past) {
			const clear = (pts: Pt[]) =>
				pts.every((p) => dist(p, past) >= EASE_PAST_FT);
			// (Off the side it is on.)
			const side =
				Math.cos(way) * (past.y - this.at.y) -
					Math.sin(way) * (past.x - this.at.x) >
				0
					? -1
					: 1;
			for (const bend of EASE_BENDS) {
				if (clear(steps)) {
					break;
				}
				steps = stepsOf(side * bend);
			}
		}
		// (Into the edge of where he can go - a few feet over an end line or
		// a sideline, onto the white border (see EASE_END_FT): turned along
		// it as he slows, sooner the nearer it is - not stopped dead against
		// it.)
		const endOver = Math.max(this.endRoam, EASE_END_FT);
		const sideOver = Math.max(this.roam, EASE_SIDE_FT);
		const inside = (p: Pt) =>
			p.x >= 1 - endOver &&
			p.x <= FIELD_L - 1 + endOver &&
			p.y >= 1 - sideOver &&
			p.y <= FIELD_W - 1 + sideOver;
		// (Over it already - no way back in on the way he goes.)
		if (!inside(this.at)) {
			return;
		}
		const out = steps.find((p) => !inside(p));
		if (out) {
			const end = out.x < 1 - endOver || out.x > FIELD_L - 1 + endOver;
			const offOf = (a: number) =>
				Math.atan2(Math.sin(a - way), Math.cos(a - way));
			const [a, b] = (end ? [Math.PI / 2, -Math.PI / 2] : [0, Math.PI]).map(
				offOf,
			);
			const mid = end
				? Math.sign(MID_Y - this.at.y || 1) * (Math.PI / 2)
				: Math.sign(MID_X - this.at.x || 1) > 0
					? 0
					: Math.PI;
			const turn =
				Math.abs(Math.abs(a!) - Math.abs(b!)) < EASE_EDGE_TIE
					? offOf(mid)
					: Math.abs(a!) < Math.abs(b!)
						? a!
						: b!;
			for (const by of EASE_EDGE_BY) {
				steps = stepsOf(turn, by);
				if (steps.every(inside)) {
					break;
				}
			}
		}
		for (const p of steps) {
			this.endRoam = Math.max(
				this.endRoam,
				Math.min(endOver, Math.max(1 - p.x, p.x - (FIELD_L - 1))),
			);
			this.roam = Math.max(
				this.roam,
				Math.min(sideOver, Math.max(1 - p.y, p.y - (FIELD_W - 1))),
			);
		}
		for (let i = 0; i < n; i++) {
			this.go(
				steps[i]!,
				this.t + dt * 1000,
				moveOf(i),
				gaited ? undefined : l.yaw,
			);
		}
	}

	// Up to speed for `leg`, off the end of the one before (see
	// RAMP_ACCEL): legs of his getting going put in ahead of it, and it what
	// is left of it.
	// How quick he can go on toward `to` off what he was doing (see
	// paceOn) - not out of his stance at the snap, off as quick as he can
	// be.
	private paceInto(to: Pt): number | undefined {
		const l = this.last;
		return SET.has(l.move) ? undefined : paceOn(l, l.to, to);
	}

	// (Out of his stance: up from nothing, at RELEASE_ACCEL - not left to
	// catch up with the play all at once, quicker than it has him going.)
	private rampInto(leg: Leg, into = this.paceInto(leg.to), any = false) {
		const T = (leg.t1 - leg.t0) / 1000;
		if ((!any && !RAMPS.has(leg.move)) || T * 1000 < RAMP_LEG_MS) {
			return;
		}
		const dx = leg.to.x - leg.from.x;
		const dy = leg.to.y - leg.from.y;
		const D = Math.hypot(dx, dy);
		const v = D / T;
		const A = into === undefined ? RELEASE_ACCEL : RAMP_ACCEL;
		let v0 = into ?? 0;
		if (v - v0 < RAMP_LEAST || D <= 0) {
			return;
		}
		// (Up to vc and on at it, there on time - or, if that is too quick,
		// up to it the harder, then up the harder to a quicker pace than it
		// (see RAMP_HARD), and only then off from v0 at as much more as it
		// takes.)
		const room = T * T - (2 * (D - v0 * T)) / A;
		let vc = room >= 0 ? v0 + A * (T - Math.sqrt(room)) : Infinity;
		const most = Math.max(v, Math.min(v * RAMP_OVER, RAMP_TOP));
		let a = A;
		if (vc > most) {
			vc = most;
			const over = vc * T - D;
			const hard = over > 0 ? ((vc - v0) * (vc - v0)) / (2 * over) : Infinity;
			if (hard <= A * RAMP_HARD) {
				a = Math.max(A, hard);
			} else {
				a = A * RAMP_HARD;
				const up = paceFor(D, T, v0, a);
				const fast = Math.max(
					most,
					Math.min(this.top, RAMP_TOP, v * RAMP_FAST),
				);
				if (up <= fast) {
					vc = up;
				} else {
					vc = Math.min(fast, v + (a * T) / 2);
					v0 = Math.max(v0, vc - Math.sqrt(2 * a * (vc * T - D)));
				}
			}
		}
		const ta = Math.min(T, (vc - v0) / a);
		if (!(ta > 0)) {
			return;
		}
		const n = Math.max(1, Math.round((ta * 1000) / RAMP_STEP_MS));
		const dt = ta / n;
		// (Up to it all the way, or as good as: the last step of it the leg
		// itself, not a leg of no time at all left after it.)
		const steps = (T - ta) * 1000 < RAMP_STEP_MS / 2 ? n - 1 : n;
		let from = leg.from;
		let t = leg.t0;
		let gone = 0;
		for (let i = 0; i < steps; i++) {
			gone += (v0 + a * dt * (i + 0.5)) * dt;
			const at = {
				x: leg.from.x + (dx * gone) / D,
				y: leg.from.y + (dy * gone) / D,
			};
			const t2 = leg.t0 + (i + 1) * dt * 1000;
			this.push({ ...leg, t0: t, t1: t2, from, to: at });
			from = at;
			t = t2;
		}
		leg.t0 = t;
		leg.from = from;
	}

	// (Whether he can be slowed down into `move` off the leg he is on - see
	// slowOn.)
	private slows(move?: Move): boolean {
		const l = this.last;
		return (
			RAMPS.has(l.move) &&
			!l.arms &&
			!l.turn &&
			(move === undefined || GAITS.has(move))
		);
	}

	// Toward `to`, `far` feet of the way in T seconds, off `v0` ft/s - a
	// good deal quicker than that has him going: down over his first
	// strides of it (see STOP_ACCEL), not braked to it in one. To a little
	// under it, there as soon as he would have been at it - or, too near
	// to get down to it, as hard as it takes by the time he is there,
	// STOP_HARD at most. (How far of it that takes him, and the pace he is
	// down to.) To be there no sooner than T - `due` - with no room to get
	// down to a pace to go on at: slowed over all of it as evenly as has
	// him there then (STOP_HARD at most), in at what that leaves him - not
	// down to that pace short of it and walked the rest.
	private slowOn(
		to: Pt,
		far: number,
		T: number,
		v0: number,
		move?: Move,
		yaw?: number,
		due = false,
	): { gone: number; on: number } {
		const from = this.at;
		const len = dist(from, to);
		const fps = far / T;
		let on = fps;
		let A = STOP_ACCEL;
		const room = A * T * (A * T - 2 * (v0 - fps));
		const c = room >= 0 ? v0 - A * T + Math.sqrt(room) : -1;
		if (c >= 0 && (v0 * v0 - c * c) / (2 * A) <= far) {
			on = c;
		}
		// (Not even that doing it: from no quicker than that gets him down to
		// it there, the rest of it in his first stride - not left to stop
		// dead from there.)
		let v = v0;
		const even = (2 * (v0 * T - far)) / (T * T);
		const need = (v0 * v0 - on * on) / (2 * far);
		if (
			due &&
			on === fps &&
			even > 0 &&
			even <= A * STOP_HARD &&
			v0 - even * T >= 0
		) {
			A = even;
			on = v0 - even * T;
		} else if (need > A) {
			A = Math.min(A * STOP_HARD, need);
			v = Math.min(v0, Math.sqrt(on * on + 2 * A * far));
		}
		const td = (v - on) / A;
		const n = Math.max(1, Math.round((td * 1000) / STOP_STEP_MS));
		const dt = td / n;
		const paceOf = (i: number) => v - A * dt * (i + 0.5);
		const moveOf = slower(move ?? this.last.move, n, dt, paceOf);
		const t0 = this.t;
		let gone = 0;
		for (let i = 0; i < n; i++) {
			gone = Math.min(far, gone + paceOf(i) * dt);
			const u = gone / len;
			this.go(
				{ x: from.x + (to.x - from.x) * u, y: from.y + (to.y - from.y) * u },
				t0 + (i + 1) * dt * 1000,
				moveOf(i),
				yaw,
			);
		}
		return { gone, on };
	}

	// To `to` at `fps`; when he gets there.
	// (On the move already, and that a good deal quicker: up to it first -
	// see RAMP_ACCEL - there the later for it.)
	run(to: Pt, fps: number, move?: Move, yaw?: number): number {
		const p = onField(to, this.roam, this.endRoam);
		if (move === undefined || RAMPS.has(move)) {
			if (!(fps < BEND_PACE ? this.bendSlow(p, fps) : this.bendInto(p, fps))) {
				this.bendOut(p);
			}
			// (Too sharp a turn to bend round: planted for it first - not
			// sent off the new way at the pace he had on the old one.)
			this.plantFor(p);
		}
		const from = this.at;
		const far = dist(from, p);
		// (Out of his stance: up from nothing, at RELEASE_ACCEL.)
		const into = this.paceInto(p);
		const v0 = into ?? 0;
		const g = move ?? gait(fps);
		let gone = 0;
		let on = fps;
		if (far > 0 && fps - v0 >= RAMP_LEAST && RAMPS.has(g)) {
			const A = into === undefined ? RELEASE_ACCEL : RAMP_ACCEL;
			let ta = (fps - v0) / A;
			if (v0 * ta + (A * ta * ta) / 2 > far) {
				ta = (Math.sqrt(v0 * v0 + 2 * A * far) - v0) / A;
			}
			const n = Math.max(1, Math.round((ta * 1000) / RAMP_STEP_MS));
			const dt = ta / n;
			const t0 = this.t;
			for (let i = 0; i < n; i++) {
				gone = Math.min(far, gone + (v0 + A * dt * (i + 0.5)) * dt);
				const u = gone / far;
				this.go(
					{ x: from.x + (p.x - from.x) * u, y: from.y + (p.y - from.y) * u },
					t0 + (i + 1) * dt * 1000,
					g,
					yaw,
				);
			}
		} else if (far > 0 && v0 - fps >= SLOW_DROP && this.slows(move)) {
			// (A good deal quicker than that: down to it as he goes.)
			({ gone, on } = this.slowOn(
				p,
				far,
				far / Math.max(1, fps),
				v0,
				move,
				yaw,
			));
		}
		if (far - gone > 0.01) {
			this.go(p, this.t + ((far - gone) / Math.max(1, on)) * 1000, move, yaw);
		}
		return this.t;
	}

	// As run, to stop there: pulled up over the last of it, hard (see
	// STOP_HARD) - not stopped dead off it.
	runUp(to: Pt, fps: number, move?: Move): number {
		const p = onField(to, this.roam, this.endRoam);
		const far = dist(this.at, p);
		const ease = Math.min(
			far * 0.5,
			(fps * fps) / (2 * STOP_ACCEL * STOP_HARD),
		);
		const u = far > 0 ? (far - ease) / far : 1;
		this.run(
			{
				x: this.at.x + (p.x - this.at.x) * u,
				y: this.at.y + (p.y - this.at.y) * u,
			},
			fps,
			move,
		);
		this.easeOff(ease);
		return this.t;
	}

	// Toward `to` by t1, at no more than `fps`: there, or as far as that
	// gets him - up to it off what he was doing as he goes, as hard as he
	// can (see rampInto).
	chase(to: Pt, t1: number, fps: number, move?: Move, yaw?: number) {
		// (Over an end line, or up to it, head on - after a man into the end
		// zone: up to it at the pace he would go, and pulled up into it - not
		// run into it and stopped dead in a stride.)
		// (Up to it at the pace he comes into it at, if that is quicker -
		// there sooner, and slowed from it over the stretch he is on, there
		// as much as EDGE_SPARE_MS later for it: not slowed in a stride as
		// he comes up to it, and stopped in the next.)
		const edge = this.edgeOf(to);
		if (edge && t1 > this.t) {
			const pace = Math.max(
				Math.min(fps, dist(this.at, to) / ((t1 - this.t) / 1000)),
				Math.min(this.paceInto(edge) ?? 0, this.top),
			);
			const tc = Math.min(
				t1,
				this.t + (dist(this.at, edge) / Math.max(pace, 0.001)) * 1000,
			);
			if (tc > this.t + 1) {
				this.chaseOn(edge, tc, Math.max(fps, pace), move, yaw);
				this.slowInto(0, STOP_ACCEL, t1 - this.t + EDGE_SPARE_MS);
				this.hold(t1);
				return;
			}
		}
		this.chaseOn(to, t1, fps, move, yaw);
	}

	private chaseOn(to: Pt, t1: number, fps: number, move?: Move, yaw?: number) {
		const p = onField(to, this.roam, this.endRoam);
		if (move === undefined || RAMPS.has(move)) {
			this.bendInto(
				p,
				Math.min(fps, dist(this.at, p) / Math.max(0.001, (t1 - this.t) / 1000)),
			);
		}
		const from = this.at;
		const far = dist(from, p);
		const T = Math.max(0, t1 - this.t) / 1000;
		const into = this.paceInto(p);
		// (Going a good deal quicker than he has to - than he can, or than
		// gets him there on time: down to it over his first strides - see
		// slowOn - and on at what he is down to. Not a step at a time, as
		// on his man in coverage: slowed from one to the next.)
		const pace = T > 0 ? Math.min(fps, far / T) : fps;
		const S = Math.min(far, pace * T);
		// (Only a moment of it - the last of a step he bent round into it
		// on, say: down from his pace as hard as he can be over it (see
		// STOP_HARD), on toward it as far as that takes him, for the next
		// of it to slow on from - not dropped to its pace in a stride.)
		if (
			into !== undefined &&
			into - pace >= SLOW_DROP &&
			T > 0 &&
			T * 1000 < RAMP_LEG_MS &&
			this.slows(move)
		) {
			const A = STOP_ACCEL * STOP_HARD;
			let ve = Math.max(pace, into - A * T);
			// (Not quite room for that: as much harder as it takes to be
			// there on time, if that is not much - see CHASE_SLOW_MOST - not
			// dropped to the pace it would have him at in a stride.)
			const vr = (2 * far) / T - into;
			if (
				((into + ve) / 2) * T > far &&
				vr >= 0 &&
				into - vr <= A * CHASE_SLOW_MOST * T
			) {
				ve = vr;
			}
			if (((into + ve) / 2) * T <= far + 1e-6) {
				const n = Math.max(1, Math.round((T * 1000) / STOP_STEP_MS));
				const dt = T / n;
				const paceOf = (i: number) => into - (into - ve) * ((i + 0.5) / n);
				const moveOf = slower(move ?? this.last.move, n, dt, paceOf);
				const t0 = this.t;
				let gone = 0;
				for (let i = 0; i < n; i++) {
					gone += paceOf(i) * dt;
					const u = gone / far;
					this.go(
						{ x: from.x + (p.x - from.x) * u, y: from.y + (p.y - from.y) * u },
						t0 + (i + 1) * dt * 1000,
						move ?? moveOf(i),
						yaw,
					);
				}
				return;
			}
		}
		if (
			into !== undefined &&
			into - pace >= SLOW_DROP &&
			S > 0 &&
			T * 1000 >= RAMP_LEG_MS &&
			this.slows(move)
		) {
			const { gone, on } = this.slowOn(p, S, T, into, move, yaw, true);
			const u = S / far;
			// (The rest of it at about the pace he is down to - not a sliver
			// left over, crept over at a walk, there too soon at that pace:
			// left be, on from where he is.)
			const rest = (t1 - this.t) / 1000;
			if (S - gone > 0.01 && rest > 0 && S - gone >= on * rest * SLIVER_SHARE) {
				this.go(
					{ x: from.x + (p.x - from.x) * u, y: from.y + (p.y - from.y) * u },
					t1,
					move,
					yaw,
				);
			}
			return;
		}
		const v0 = Math.min(fps, into ?? 0);
		const A = (into === undefined ? RELEASE_ACCEL : RAMP_ACCEL) * RAMP_HARD;
		const ta = Math.min(T, (fps - v0) / A);
		const room = RAMPS.has(move ?? gait(fps))
			? v0 * ta + (A * ta * ta) / 2 + fps * (T - ta)
			: fps * T;
		const u = far > room ? room / far : 1;
		// (Short of it for getting up to speed: in the gait he is up to by
		// then, not the one his pace over all of it would have him in.)
		const g =
			move ?? (far > room && room < fps * T ? gait(v0 + A * ta) : undefined);
		this.go(
			{ x: from.x + (p.x - from.x) * u, y: from.y + (p.y - from.y) * u },
			t1,
			g,
			yaw,
		);
	}

	// Toward `to` by t1, at no more than `fps` over all of it - as chase,
	// but as hard as it takes to get up to it (see rampInto), somewhere he
	// has to be: under a kick coming down, say.
	press(to: Pt, t1: number, fps: number, move?: Move, yaw?: number) {
		const p = onField(to, this.roam, this.endRoam);
		const from = this.at;
		const far = dist(from, p);
		const room = (fps * Math.max(0, t1 - this.t)) / 1000;
		const u = far > room ? room / far : 1;
		const end = {
			x: from.x + (p.x - from.x) * u,
			y: from.y + (p.y - from.y) * u,
		};
		// (In a good deal quicker than that, and no time to have slowed for
		// it on the way in - see slowFor: down to it over his first strides
		// of it - see slowOn - not dropped to it in a stride.)
		const T = Math.max(0, t1 - this.t) / 1000;
		const S = far * u;
		const into = this.paceInto(end);
		if (
			into !== undefined &&
			S > 0 &&
			T * 1000 >= RAMP_LEG_MS &&
			into - S / T >= SLOW_DROP &&
			this.slows(move) &&
			this.last.t1 - this.part.legs[this.stretchFrom()]!.t0 < RAMP_LEG_MS
		) {
			const { gone } = this.slowOn(end, S, T, into, move, yaw, true);
			if (S - gone > 0.01 && t1 > this.t) {
				this.go(end, t1, move, yaw);
			}
			this.pin(t1);
			return;
		}
		this.go(end, t1, move, yaw);
		this.pin(t1);
	}

	// As press - but up to it no harder than RAMP_HARD times as hard as he
	// gets up to speed (out of a stand, RELEASE_ACCEL): as far toward it as
	// that gets him by t1, short of it - not shot off at it from nothing to
	// full tilt in a stride.
	pressUp(to: Pt, t1: number, fps: number, move?: Move, yaw?: number) {
		const p = onField(to, this.roam, this.endRoam);
		const far = dist(this.at, p);
		const into = this.paceInto(p);
		const v0 = into ?? 0;
		const A = (into === undefined ? RELEASE_ACCEL : RAMP_ACCEL) * RAMP_HARD;
		const T = Math.max(0, t1 - this.t) / 1000;
		const top = Math.max(v0, fps);
		const ta = Math.min(T, (top - v0) / A);
		const reach = v0 * ta + (A * ta * ta) / 2 + top * (T - ta);
		if (far <= reach + PRESS_SLACK_FT) {
			this.press(p, t1, fps, move, yaw);
			return;
		}
		// (In the gait he gets up to - going all out, not walked there, nor
		// in a sprint he never gets up to.)
		const u = Math.max(0, reach - PRESS_SLACK_FT) / far;
		// (Next to nowhere: stood there for it - not running on the spot.)
		if (u * far < PRESS_LEAST_FT) {
			this.hold(t1, "stand", yaw);
			return;
		}
		this.press(
			{
				x: this.at.x + (p.x - this.at.x) * u,
				y: this.at.y + (p.y - this.at.y) * u,
			},
			t1,
			fps,
			move === undefined || RAMPS.has(move) ? gait(v0 + A * ta) : move,
			yaw,
		);
	}

	// Coming in on `to` (by t1) quicker than it takes: his way in over the
	// last SLOW_ALONG_MS - the run he is on, bent no sharper than
	// SLOW_ALONG_TURN a stride, and on to `to` - taken slowing as he goes
	// (see STOP_ACCEL, STOP_HARD) and in at what that leaves him - not on
	// at full tilt to a stride off it and walked the rest. (Whether he was.)
	// (Only if that is `least` ft/s or more slower than he comes in at.)
	slowAlong(to: Pt, t1: number, least = RAMP_LEAST): boolean {
		const legs = this.part.legs;
		let l = this.last;
		const okLeg = (x: Leg) =>
			RAMPS.has(x.move) &&
			!x.arms &&
			!x.turn &&
			!x.span &&
			x.t1 > x.t0 &&
			dist(x.from, x.to) > 0 &&
			x.yaw === l.yaw &&
			x.ball === l.ball &&
			x.mirrored === l.mirrored;
		const from = Math.max(t1 - SLOW_ALONG_MS, this.pinned);
		// (One long stride of a run since before then: from then on of it -
		// not dropped to his pace in a stride after it.)
		if (okLeg(l) && l.t0 < from && from < l.t1 && legPace(l) >= STOP_LEAST) {
			this.split(from);
			l = this.last;
		}
		if (!okLeg(l) || l.t0 < from || legPace(l) < STOP_LEAST) {
			return false;
		}
		const wayOf = (x: Leg) => Math.atan2(x.to.y - x.from.y, x.to.x - x.from.x);
		// (Back no further than he was still getting up to speed: slowed
		// from the pace he got up to - not from the slower one he began the
		// run at, too slow to have him there early at all.)
		let j = legs.length - 1;
		while (j > 0) {
			const p = legs[j - 1]!;
			const n = legs[j]!;
			if (
				p.t0 < from ||
				!okLeg(p) ||
				p.t1 !== n.t0 ||
				Math.abs(angleOff(wayOf(n) - wayOf(p))) > SLOW_ALONG_TURN ||
				legPace(p) < legPace(n) - STRETCH_SLACK
			) {
				break;
			}
			j--;
		}
		const pts = [legs[j]!.from, ...legs.slice(j).map((x) => x.to)];
		if (dist(l.to, to) > 0.01) {
			if (angleAt(l.from, l.to, to) > SLOW_ALONG_TURN) {
				return false;
			}
			pts.push(to);
		}
		const t0 = legs[j]!.t0;
		const T = (t1 - t0) / 1000;
		let L = 0;
		for (let i = 1; i < pts.length; i++) {
			L += dist(pts[i - 1]!, pts[i]!);
		}
		const vs = legPace(legs[j]!);
		if (T <= 0 || L <= 0 || !(L / T < vs - least)) {
			return false;
		}
		// (Down at Ad over td, then on at v1 - or, it taking a stop to,
		// stopped on it and stood there.)
		const easeAt = (Ad: number) => {
			const q = T * T - (2 * (vs * T - L)) / Ad;
			const v1 = q >= 0 ? vs - Ad * (T - Math.sqrt(q)) : -1;
			return v1 >= 0 ? { v1, Ad } : undefined;
		};
		// (Too quick even for that - flat out round into it, near it: pulled
		// up as hard as he plants for a cut (see CUT_ACCEL), stood there -
		// not on at full tilt to a stride off it and walked the rest.)
		const stopA = (vs * vs) / (2 * L);
		const plan =
			easeAt(STOP_ACCEL) ??
			easeAt(STOP_ACCEL * STOP_HARD) ??
			(stopA <= CUT_ACCEL * STOP_HARD ? { v1: 0, Ad: stopA } : undefined);
		if (!plan) {
			return false;
		}
		const { v1, Ad } = plan;
		const td = (vs - v1) / Ad;
		const sAt = (t: number) => {
			const u = Math.min(t, td);
			return vs * u - (Ad * u * u) / 2 + v1 * Math.max(0, t - td);
		};
		const ptAt = (s: number): Pt => {
			for (let i = 1; i < pts.length; i++) {
				const a = pts[i - 1]!;
				const b = pts[i]!;
				const d = dist(a, b);
				if (s <= d || i === pts.length - 1) {
					const u = d > 0 ? clamp(s / d, 0, 1) : 1;
					return { x: a.x + (b.x - a.x) * u, y: a.y + (b.y - a.y) * u };
				}
				s -= d;
			}
			return pts.at(-1)!;
		};
		// (When he gets `s` along it.)
		const sd = sAt(td);
		const tAt = (s: number) =>
			s <= sd
				? (vs - Math.sqrt(Math.max(0, vs * vs - 2 * Ad * s))) / Ad
				: td + (s - sd) / Math.max(v1, 1e-6);
		const yaw = l.yaw;
		legs.splice(j);
		const end = v1 > 0 ? T : Math.min(T, td);
		const n = Math.max(1, Math.round((end * 1000) / STOP_STEP_MS));
		// (A step at a time - and through every bend of it he went round,
		// where he goes round it: not cut across it a step at a time.)
		const at = Array.from({ length: n }, (_, i) => (end * (i + 1)) / n);
		let s0 = 0;
		for (let i = 1; i < pts.length - 1; i++) {
			s0 += dist(pts[i - 1]!, pts[i]!);
			if (
				s0 < sAt(end) &&
				angleAt(pts[i - 1]!, pts[i]!, pts[i + 1]!) > SLOW_ALONG_BEND
			) {
				at.push(tAt(s0));
			}
		}
		at.sort((a, b) => a - b);
		let a = 0;
		for (const b of at) {
			if (b - a < SLOW_ALONG_LEAST_S && b < end) {
				continue;
			}
			const pace = (sAt(b) - sAt(a)) / (b - a);
			this.go(ptAt(sAt(b)), t0 + b * 1000, gait(pace), yaw);
			a = b;
		}
		this.hold(t1);
		this.pin(t1);
		return true;
	}

	// Gathering for something he does where his run ends (a leap): the last
	// of the way he is on taken slowing, at A ft/s/s, to v1 at its end -
	// there a little later for it - not run into at full tilt and stopped
	// dead in a stride. (No further back than the run he is on goes, bent
	// no sharper than SLOW_ALONG_TURN a stride. Not as slowInto, which has
	// him quicker before it to be there on time.)
	gatherFor(v1: number, A: number) {
		const legs = this.part.legs;
		const l = this.last;
		const vs = legPace(l);
		if (vs <= v1 + RAMP_LEAST || l.t1 < this.pinned) {
			return;
		}
		const okLeg = (x: Leg) =>
			RAMPS.has(x.move) &&
			!x.arms &&
			!x.turn &&
			!x.span &&
			x.t1 > x.t0 &&
			dist(x.from, x.to) > 0 &&
			x.yaw === l.yaw &&
			x.ball === l.ball &&
			x.mirrored === l.mirrored;
		if (!okLeg(l)) {
			return;
		}
		const wayOf = (x: Leg) => Math.atan2(x.to.y - x.from.y, x.to.x - x.from.x);
		// (The way back to where he starts slowing: as far as that takes.)
		const need = (vs * vs - v1 * v1) / (2 * A);
		let j = legs.length - 1;
		let got = dist(l.from, l.to);
		while (got < need && j > 0) {
			const p = legs[j - 1]!;
			const n = legs[j]!;
			if (
				p.t0 < this.pinned ||
				!okLeg(p) ||
				p.t1 !== n.t0 ||
				Math.abs(angleOff(wayOf(n) - wayOf(p))) > SLOW_ALONG_TURN
			) {
				break;
			}
			j--;
			got += dist(p.from, p.to);
		}
		const D = Math.min(need, got);
		const ve = Math.sqrt(Math.max(0, vs * vs - 2 * A * D));
		// (Where on the way he starts slowing, and when he would have been
		// there.)
		const lead = legs[j]!;
		const into = got - D;
		const u = into / dist(lead.from, lead.to);
		const cut: Pt = {
			x: lead.from.x + (lead.to.x - lead.from.x) * u,
			y: lead.from.y + (lead.to.y - lead.from.y) * u,
		};
		const tc = lead.t0 + (lead.t1 - lead.t0) * u;
		const pts = [cut, ...legs.slice(j).map((x) => x.to)];
		const ptAt = (s: number): Pt => {
			for (let i = 1; i < pts.length; i++) {
				const a = pts[i - 1]!;
				const b = pts[i]!;
				const d = dist(a, b);
				if (s <= d || i === pts.length - 1) {
					const w = d > 0 ? clamp(s / d, 0, 1) : 1;
					return { x: a.x + (b.x - a.x) * w, y: a.y + (b.y - a.y) * w };
				}
				s -= d;
			}
			return pts.at(-1)!;
		};
		const { move, yaw } = lead;
		legs.splice(j);
		if (u > 0) {
			this.push({ ...lead, to: cut, t1: tc });
		}
		const td = (vs - ve) / A;
		const sAt = (t: number) => vs * t - (A * t * t) / 2;
		const n = Math.max(1, Math.round((td * 1000) / STOP_STEP_MS));
		for (let i = 1; i <= n; i++) {
			const a = (td * (i - 1)) / n;
			const b = (td * i) / n;
			const pace = (sAt(b) - sAt(a)) / (b - a);
			this.push({
				...lead,
				from: ptAt(sAt(a)),
				to: ptAt(sAt(b)),
				t0: tc + a * 1000,
				t1: tc + b * 1000,
				move: RAMPS.has(move) ? gait(pace) : move,
				yaw,
			});
		}
	}

	// Toward `to` by t1 at no more than `fps`, as chase - but getting up to
	// it from the pace he was going at (see RAMP_ACCEL) as he goes: there,
	// or as far as that gets him.
	chaseUp(to: Pt, t1: number, fps: number, move?: Move) {
		const p = onField(to, this.roam, this.endRoam);
		// (After it out over a line, as far as he may go over it (see roam),
		// going at it too quick to pull up short of that: eased off into it,
		// turned along it as he slows - see easeOff - not on at it flat out
		// and stopped dead against it.)
		if (dist(p, to) > EDGE_CLAMP_FT) {
			const l = this.last;
			const v = legPace(l);
			const stopFt = (v * v) / (2 * STOP_ACCEL * STOP_HARD);
			if (
				RAMPS.has(l.move) &&
				!l.arms &&
				!l.turn &&
				!l.span &&
				v >= STOP_LEAST &&
				dist(this.at, p) < stopFt &&
				angleAt(l.from, l.to, p) <= EDGE_EASE_TURN
			) {
				this.easeOff(stopFt);
				this.hold(t1);
				return;
			}
		}
		// (Planted first, well round from the way he is going - all the
		// time there is, maybe - and down to no quicker than he goes on at
		// after it: not planted at a run and off at a walk.)
		this.plantFor(
			p,
			undefined,
			Math.min(fps, dist(this.at, p) / Math.max(0.001, (t1 - this.t) / 1000)),
		);
		if (t1 - this.t < STOP_STEP_MS) {
			// (Only a moment left after it - planted or bent round into it,
			// there early: on at it at the pace he is going - or, there
			// already, or well round from the way he goes, on that way, past
			// it the little way that takes him, for the next of it to plant
			// for - not the last stride of the turn stretched over it, down to
			// half his pace in a stride and up again off it - see hold.)
			const l = this.last;
			const v = legPace(l);
			const far = dist(this.at, p);
			const ft = (v * (t1 - this.t)) / 1000;
			const run = dist(l.from, l.to);
			const [ux, uy] =
				ft < far && run > 0 && angleAt(l.from, l.to, p) <= CUT_LEAST
					? [(p.x - this.at.x) / far, (p.y - this.at.y) / far]
					: [(l.to.x - l.from.x) / run, (l.to.y - l.from.y) / run];
			if (
				t1 > this.t &&
				RAMPS.has(l.move) &&
				!l.arms &&
				!l.turn &&
				!l.span &&
				v >= STOP_LEAST &&
				run > 0
			) {
				this.go(
					onField(
						{ x: this.at.x + ux! * ft, y: this.at.y + uy! * ft },
						this.roam,
						this.endRoam,
					),
					t1,
					l.move,
				);
				return;
			}
			this.hold(t1);
			return;
		}
		if (move === undefined || RAMPS.has(move)) {
			this.bendInto(
				p,
				Math.min(fps, dist(this.at, p) / Math.max(0.001, (t1 - this.t) / 1000)),
			);
		}
		const from = this.at;
		const far = dist(from, p);
		const T = Math.max(0, t1 - this.t) / 1000;
		const v0 = this.paceInto(p) ?? 0;
		// (In quicker than it takes: slowing into it - at STOP_ACCEL, harder
		// if he has to, or as hard as that and on past it - not down to its
		// pace in a stride.)
		if (
			v0 >= STOP_LEAST &&
			GAITS.has(this.last.move) &&
			far > 0 &&
			T > 0 &&
			far / T < v0 - RAMP_LEAST
		) {
			const easeAt = (Ad: number) => {
				const q = T * T - (2 * (v0 * T - far)) / Ad;
				const v1 = q >= 0 ? v0 - Ad * (T - Math.sqrt(q)) : -1;
				return v1 >= 0 ? { v1, Ad } : undefined;
			};
			const { v1, Ad } = easeAt(STOP_ACCEL) ??
				easeAt(STOP_ACCEL * STOP_HARD) ?? { v1: 0, Ad: STOP_ACCEL * STOP_HARD };
			const td = (v0 - v1) / Ad;
			const n = Math.max(1, Math.round((td * 1000) / STOP_STEP_MS));
			const dt = td / n;
			const paceOf = (i: number) => v0 - (v0 - v1) * ((i + 0.5) / n);
			const moveOf = slower(this.last.move, n, dt, paceOf);
			const t0 = this.t;
			let gone = 0;
			for (let i = 0; i < n; i++) {
				gone += paceOf(i) * dt;
				this.go(
					{
						x: from.x + ((p.x - from.x) * gone) / far,
						y: from.y + ((p.y - from.y) * gone) / far,
					},
					t0 + (i + 1) * dt * 1000,
					move ?? moveOf(i),
				);
			}
			if (v1 > 0 && t1 > this.t + 1) {
				this.go(p, t1, move);
			}
			return;
		}
		const A = RAMP_ACCEL;
		const ta = Math.min(T, Math.max(0, (fps - v0) / A));
		const reach = v0 * ta + (A * ta * ta) / 2 + fps * (T - ta);
		if (fps - v0 < RAMP_LEAST || reach >= far || far <= 0) {
			// (There by t1 even so: up to as quick as gets him there just
			// then, if it is no more than fps - not off at it all at once.)
			const room = T * T - (2 * (far - v0 * T)) / A;
			const vc = room >= 0 ? v0 + A * (T - Math.sqrt(room)) : Infinity;
			const g = move ?? gait(vc);
			if (
				far > 0 &&
				T > 0 &&
				far / T - v0 >= RAMP_LEAST &&
				vc <= fps &&
				RAMPS.has(g)
			) {
				const ta = (vc - v0) / A;
				const n = Math.max(1, Math.round((ta * 1000) / RAMP_STEP_MS));
				const dt = ta / n;
				const t0 = this.t;
				for (let i = 1; i <= n; i++) {
					const s = v0 * dt * i + (A * (dt * i) ** 2) / 2;
					this.go(
						{
							x: from.x + ((p.x - from.x) * s) / far,
							y: from.y + ((p.y - from.y) * s) / far,
						},
						t0 + i * dt * 1000,
						g,
					);
				}
				if (t1 > this.t + 1) {
					this.go(p, t1, g);
				}
				return;
			}
			this.chase(to, t1, fps, move);
			return;
		}
		const g = move ?? gait(fps);
		const n = Math.max(1, Math.round((ta * 1000) / RAMP_STEP_MS));
		const dt = ta / n;
		const t0 = this.t;
		let gone = 0;
		const at = (d: number) => ({
			x: from.x + ((p.x - from.x) * d) / far,
			y: from.y + ((p.y - from.y) * d) / far,
		});
		for (let i = 0; i < n; i++) {
			gone += (v0 + A * dt * (i + 0.5)) * dt;
			this.go(at(gone), t0 + (i + 1) * dt * 1000, g);
		}
		if (t1 > this.t + 1) {
			this.go(at(reach), t1, g);
		}
	}

	// An act where he is (or going a little way), over t0..t1.
	act(move: Move, t1: number, yaw?: number, to?: Pt) {
		const l = this.last;
		this.push({
			t0: l.t1,
			t1: Math.max(t1, l.t1 + 1),
			from: l.to,
			to: to ? onField(to, this.roam, this.endRoam) : l.to,
			move,
			yaw: yaw ?? this.yaw,
		});
	}

	// Off his feet into it from on the run: launched from as far back as
	// that carries him (LUNGE_BACK_MS earlier at the most), not run on in to
	// throw himself the last yard or two - and slowed as he goes (see
	// pace.ts carriedOf).
	lunge(move: Move, t1: number, yaw: number, to: Pt, slide = false) {
		// (Only a blink of a stride out of something else - off a block, up
		// out of his set: launched from that, not a flick of a stride first.)
		const was = this.legs.at(-2);
		const step = this.last;
		if (
			was &&
			GAITS.has(step.move) &&
			!GAITS.has(was.move) &&
			was.t1 === step.t0 &&
			step.t1 - step.t0 < FLICK_MS &&
			!step.arms
		) {
			this.cut(step.t0);
		}
		const l = this.last;
		const v = GAITS.has(l.move) ? legPace(l) : 0;
		if (v >= LUNGE_LEAST_FPS) {
			const far = Math.min(
				LUNGE_MOST_FT,
				(v * LUNGE_AIR_MS) / 1000 + LUNGE_SLIDE_FT,
			);
			// (Back along the run he is on, no further than it goes.)
			const legs = this.legs;
			let k = legs.length - 1;
			while (
				k > 0 &&
				GAITS.has(legs[k - 1]!.move) &&
				legs[k - 1]!.t1 === legs[k]!.t0 &&
				legPace(legs[k - 1]!) >= LUNGE_LEAST_FPS
			) {
				k--;
			}
			const least = Math.max(legs[k]!.t0 + LUNGE_ON_MS, this.t - LUNGE_BACK_MS);
			let t0 = this.t;
			while (t0 - LUNGE_STEP_MS >= least && dist(this.posAt(t0), to) < far) {
				t0 -= LUNGE_STEP_MS;
			}
			if (t0 < this.t) {
				this.cut(t0);
			}
		}
		// (No further than he can launch himself in the time there is: short
		// of it, late into it.)
		const far = dist(this.at, to);
		const most = (LUNGE_MOST_FPS * (t1 - this.t)) / 1000;
		const u = far > most ? most / far : 1;
		// (`slide`: down off it at the pace he launched at, and slid to a
		// stop on the turf - over as long as that takes, up to
		// LUNGE_SLIDE_MOST_MS - not stopped dead as he lands.)
		const vl = GAITS.has(this.last.move) ? legPace(this.last) : 0;
		if (slide && vl > 0) {
			t1 = Math.max(
				t1,
				this.t + Math.min(LUNGE_SLIDE_MOST_MS, ((2 * far * u) / vl) * 1000),
			);
		}
		// (Back behind him, past it as he is: thrown back round at it, the
		// way he goes - not launched backward, facing away from it.)
		const way = Math.atan2(to.y - this.at.y, to.x - this.at.x);
		const round =
			far * u >= LUNGE_ROUND_FT && Math.abs(angleOff(way - yaw)) > LUNGE_ROUND;
		this.act(move, t1, round ? way : yaw, {
			x: this.at.x + (to.x - this.at.x) * u,
			y: this.at.y + (to.y - this.at.y) * u,
		});
	}

	// Diving in on `to` (a man going down there) by t1, or as soon after as
	// he gets there: from too far off to dive it, run in to where he can
	// first - not launched at it across the field from a stand. When he is
	// down.
	diveIn(to: Pt, t1: number, yaw: number): number {
		const l = this.last;
		const v = GAITS.has(l.move) ? legPace(l) : 0;
		const reach = Math.min(
			LUNGE_MOST_FT,
			DIVE_STILL_FT + (v * LUNGE_AIR_MS) / 1000,
		);
		const far = dist(this.at, to);
		if (far > reach + PAST_SLACK_FT) {
			// (Pulled up near enough to a stand, and it back the way he came
			// - more than DIVE_ROUND_LEAST round: round on his feet to it
			// first (see evaluate.ts STAND_TURN) - not spun round in a stride
			// as he sets off.)
			const way = Math.atan2(to.y - this.at.y, to.x - this.at.x);
			const round = Math.abs(angleOff(way - this.yaw));
			if (v < STOP_LEAST && round > DIVE_ROUND_LEAST) {
				this.hold(this.t + (round / DIVE_ROUND_RATE) * 1000, "stand", way);
			}
			const u = (far - reach) / far;
			this.run(
				{
					x: this.at.x + (to.x - this.at.x) * u,
					y: this.at.y + (to.y - this.at.y) * u,
				},
				Math.max(v, this.top * FLAT_OUT),
			);
			t1 = Math.max(t1, this.t + DIVE_LEAST_MS);
		}
		this.lunge("dive", t1, yaw, to, true);
		return this.t;
	}

	// Reaching for it, his hands on it at `at` (see CATCH_REACH_MS) - only
	// so much of the reach as there is still time for, if he gets to it
	// late: his hands there as it gets there, not up for it after.
	catchOn(at: number, yaw?: number, to?: Pt, move: Move = "catch") {
		this.act(move, at + CATCH_IN_MS, yaw, to);
		const l = this.last;
		const late = l.t0 - (at - CATCH_REACH_MS);
		if (late > 0 && l.t0 < at) {
			l.span = [late / (CATCH_REACH_MS + CATCH_IN_MS), 1];
		}
	}

	// Round as he goes through what he is doing now, this far (to his left
	// positive).
	turn(by: number) {
		this.last.turn = by;
		this.yaw += by;
	}

	// Set in leg k, facing his own way - locked up with a man, say - well
	// round (see TURN_INTO_LEAST) from the way his stride after it goes:
	// turned onto it over the last of it, no quicker than PICK_TURN_RATE -
	// the rest of the way round as he goes - not spun round in a blink into
	// his stride.
	turnInto(k: number) {
		const legs = this.part.legs;
		const l = legs[k];
		const next = legs[k + 1];
		if (
			!l ||
			!next ||
			l.yaw === undefined ||
			l.turn ||
			GAITS.has(l.move) ||
			l.t1 <= l.t0 ||
			legPace(l) >= STOP_LEAST ||
			next.t0 !== l.t1 ||
			!GAITS.has(next.move) ||
			(next.from.x === next.to.x && next.from.y === next.to.y)
		) {
			return;
		}
		let off = angleOff(
			Math.atan2(next.to.y - next.from.y, next.to.x - next.from.x) - l.yaw,
		);
		// (Right round from it: round the side his first strides go on round
		// to - not the long way round, and back.)
		if (Math.abs(off) > TURN_EITHER) {
			let ex = 0;
			let ey = 0;
			for (let i = k + 1; i < legs.length; i++) {
				const q = legs[i]!;
				if (i > k + 1 && (q.t0 >= l.t1 + TURN_LOOK_MS || !GAITS.has(q.move))) {
					break;
				}
				ex += q.to.x - q.from.x;
				ey += q.to.y - q.from.y;
			}
			const look = angleOff(Math.atan2(ey, ex) - l.yaw);
			if (look * off < 0) {
				off -= Math.sign(off) * 2 * Math.PI;
			}
		}
		if (Math.abs(off) <= TURN_INTO_LEAST) {
			return;
		}
		// (Over the strides of it before, too, if he is set the same in
		// them - locked up with a man a step at a time, say - but not till
		// he has been set that way TURN_SETTLE_MS: not turned off it as he
		// is still coming round into it.)
		let j = k;
		while (j > 0) {
			const p = legs[j - 1]!;
			const q = legs[j]!;
			if (
				p.t1 !== q.t0 ||
				p.move !== q.move ||
				p.yaw !== q.yaw ||
				p.turn ||
				legPace(p) >= STOP_LEAST
			) {
				break;
			}
			j--;
		}
		const t1 = l.t1;
		// (Set that way as he came - round no more than TURN_SETTLE_INTO
		// into it: round off it from the first of it, if it takes that - a
		// short lock, beaten - not a blink of it left to come round in.)
		const settle =
			Math.abs(angleOff(yawThen(this, legs[j]!.t0) - l.yaw)) > TURN_SETTLE_INTO
				? TURN_SETTLE_MS
				: 0;
		const t0 = Math.max(
			legs[j]!.t0 + settle,
			t1 - (Math.abs(off) / PICK_TURN_RATE) * 1000,
		);
		const yaw = l.yaw;
		if (!(t0 < t1)) {
			return;
		}
		while (legs[j]!.t1 <= t0) {
			j++;
		}
		if (t0 > legs[j]!.t0) {
			this.split(t0);
			j++;
			k++;
		}
		const by = clamp(
			off,
			-((t1 - t0) / 1000) * PICK_TURN_RATE,
			((t1 - t0) / 1000) * PICK_TURN_RATE,
		);
		for (let i = j; i <= k; i++) {
			const p = legs[i]!;
			const u0 = (p.t0 - t0) / (t1 - t0);
			const u1 = (p.t1 - t0) / (t1 - t0);
			p.yaw = yaw + by * u0;
			p.turn = by * (u1 - u0);
		}
	}

	// The leg he is in at t made two, there.
	split(t: number) {
		const legs = this.part.legs;
		const k = legs.findIndex((l) => l.t0 < t && t < l.t1);
		if (k < 0) {
			return;
		}
		const l = legs[k]!;
		const u = (t - l.t0) / (l.t1 - l.t0);
		const mid = {
			x: l.from.x + (l.to.x - l.from.x) * u,
			y: l.from.y + (l.to.y - l.from.y) * u,
		};
		const [p0, p1] = l.span ?? [0, 1];
		const [q0, q1] = l.armsSpan ?? [0, 1];
		const act = moveKind(l.move) === "act";
		const second: Leg = {
			...l,
			t0: t,
			from: mid,
			...(act ? { span: [p0 + (p1 - p0) * u, p1] as [number, number] } : {}),
			...(l.arms
				? { armsSpan: [q0 + (q1 - q0) * u, q1] as [number, number] }
				: {}),
			...(l.turn && l.yaw !== undefined
				? { yaw: l.yaw + l.turn * u, turn: l.turn * (1 - u) }
				: {}),
		};
		if (l.turn) {
			l.turn *= u;
		}
		l.t1 = t;
		l.to = mid;
		if (act) {
			l.span = [p0, p0 + (p1 - p0) * u];
		}
		if (l.arms) {
			l.armsSpan = [q0, q0 + (q1 - q0) * u];
		}
		legs.splice(k + 1, 0, second);
	}

	// What he does from t on (a leg starting then) made one leg, straight
	// on to where the last of it ends, in that.
	joinFrom(t: number) {
		const legs = this.part.legs;
		const k = legs.findIndex((l) => l.t0 >= t);
		if (k < 0 || k === legs.length - 1) {
			return;
		}
		const first = legs[k]!;
		const last = legs.at(-1)!;
		legs.splice(k, legs.length - k, {
			...last,
			t0: first.t0,
			from: first.from,
		});
	}

	// His arms through `move` over t0..t1, whatever the rest of him is
	// doing then.
	arms(move: Move, t0: number, t1: number) {
		this.split(t0);
		this.split(t1);
		for (const l of this.part.legs) {
			if (l.t0 >= t0 && l.t1 <= t1 && l.t1 > l.t0) {
				l.arms = move;
				l.armsSpan = [(l.t0 - t0) / (t1 - t0), (l.t1 - t0) / (t1 - t0)];
			}
		}
	}

	// Everything he does after t thrown away: what he was in the middle of
	// cut short there.
	cut(t: number) {
		const legs = this.part.legs;
		while (legs.length > 1 && legs.at(-1)!.t0 >= t) {
			legs.pop();
		}
		// (Not left a leg too short to have any pace to it, from the moment
		// it began: as if never begun.)
		if (
			legs.length > 1 &&
			legs.at(-1)!.t1 > t &&
			t - legs.at(-1)!.t0 < CUT_SLIVER_MS
		) {
			legs.pop();
		}
		// (Nor a stride or two of a stretch he was getting going over: the
		// one before it on to there instead - or, slowing, the gait he was
		// slowing from.)
		const sliver = legs.at(-1)!;
		const before = legs.at(-2);
		if (
			before &&
			sliver.t1 > t &&
			t - sliver.t0 < CUT_MERGE_MS &&
			GAITS.has(sliver.move) &&
			GAITS.has(before.move) &&
			before.t1 === sliver.t0
		) {
			if (legPace(sliver) < legPace(before)) {
				sliver.move = before.move;
			} else {
				const u = (t - sliver.t0) / (sliver.t1 - sliver.t0);
				before.to = {
					x: sliver.from.x + (sliver.to.x - sliver.from.x) * u,
					y: sliver.from.y + (sliver.to.y - sliver.from.y) * u,
				};
				before.t1 = t;
				legs.pop();
			}
		}
		const l = legs.at(-1)!;
		if (l.t1 > t && t > l.t0) {
			const u = (t - l.t0) / (l.t1 - l.t0);
			l.to = {
				x: l.from.x + (l.to.x - l.from.x) * u,
				y: l.from.y + (l.to.y - l.from.y) * u,
			};
			if (moveKind(l.move) === "act") {
				const [p0, p1] = l.span ?? [0, 1];
				l.span = [p0, p0 + (p1 - p0) * u];
			}
			if (l.arms) {
				const [q0, q1] = l.armsSpan ?? [0, 1];
				l.armsSpan = [q0, q0 + (q1 - q0) * u];
			}
			if (l.turn) {
				l.turn *= u;
			}
			l.t1 = t;
			this.stretchOver(l);
		}
		let yaw = legs[0]!.yaw ?? 0;
		for (const x of legs) {
			yaw = yawOf(x, yaw);
		}
		this.yaw = yaw;
	}

	// (Stood there, cut down to a moment between what he was doing and
	// whatever he does next: at what he was doing that moment longer
	// instead, not stopped dead for it - see hold.)
	private stretchOver(l: Leg) {
		const legs = this.part.legs;
		const before = legs.at(-2);
		if (
			!before ||
			legs.at(-1) !== l ||
			l.move !== "stand" ||
			l.arms !== undefined ||
			l.t1 - l.t0 >= WAIT_LEAST_MS ||
			l.from.x !== l.to.x ||
			l.from.y !== l.to.y ||
			before.t1 !== l.t0 ||
			moveKind(before.move) !== "cycle" ||
			(before.from.x === before.to.x && before.from.y === before.to.y)
		) {
			return;
		}
		const face = yawOf(before, before.yaw ?? 0);
		const off = (l.yaw ?? face) - face;
		if (
			Math.abs(Math.atan2(Math.sin(off), Math.cos(off))) > STRETCH_OVER_TURN
		) {
			return;
		}
		before.t1 = l.t1;
		legs.pop();
	}
}

// (The way he faces at the end of it: round by its turn.)
const yawOf = (l: Leg, before: number): number =>
	(l.yaw ??
		(l.from.x !== l.to.x || l.from.y !== l.to.y
			? Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x)
			: before)) + (l.turn ?? 0);
// (The way he faces as t comes: as the legs before it left him.)
const yawThen = (m: Actor, t: number): number => {
	let yaw = m.legs[0]?.yaw ?? 0;
	for (const l of m.legs) {
		if (l.t0 >= t) {
			break;
		}
		yaw = yawOf(l, yaw);
	}
	return yaw;
};

// ---- the ball ---------------------------------------------------------------

// How a man has the ball, by what he is doing: over it, a center; cocking
// it to throw; in both hands in the pocket, catching it, handing it off,
// taking a knee; otherwise tucked away.
const IN_HANDS = new Set<Move>([
	"qbSet",
	"pocket",
	"handOff",
	"pitch",
	"kneelDown",
	"duck",
	"catch",
	"basket",
	"leapCatch",
	"diveCatch",
	"toeTap",
	"scoop",
	"back",
	"slide",
	"shuffle",
	"walk",
	"shotgun",
	"underCenter",
	"stand",
	"punt",
	"holdKneel",
	"holdPlace",
]);
// (Up over his head in his throwing hand, his arms doing something of
// their own: spiking it, holding it up.)
const IN_HAND = new Set<Move>([
	"throwWind",
	"pump",
	"spike",
	"ballUp",
	"leap",
	"pointSky",
	"bowl",
]);
const HELD_UP = new Set<Move>(["back", "slide", "shuffle", "walk", "stand"]);
// (Kept tucked away, stood between strides with it no longer than this, ms.)
const TUCKED_MS = 250;
// (His free arm out at a man: the ball still tucked away under the other.)
const ARMS_TUCKED = new Set<Move>(["stiffArm"]);
// (Pulled up by the hand - see takeHand: the ball under the other arm.)
const TAKE_HAND_BALL = new Map<Move, BallHold>([
	["takeHand", "carryL"],
	["takeHandBack", "carryL"],
	["takeHandL", "carry"],
	["takeHandBackL", "carry"],
]);
// Which arm (see carryArm): from the middle of the field this far up it
// ahead of him (feet); over to the other arm only off a stretch of at
// least this long (feet) with them that plainly on the other side (the
// sine of the angle off his line), and not again for this long (ms).
const ARM_LOOK_FT = 30;
const ARM_LEAST_FT = 3;
const ARM_SURE = 0.35;
const ARM_KEEP_MS = 1500;
// Covered up for a hit (see coverFor): from this long (ms) before it, with
// these odds (the first, and as much again as the second by how careful
// with it he is against the others at his position).
const COVER_MS = 350;
const COVER_HITS = new Set<Move>(["tackled", "tackledBack", "drive"]);
const COVER_ODDS = [0.55, 0.3] as const;
const holdFor = (m: Move): BallHold =>
	m === "center"
		? "snap"
		: IN_HAND.has(m)
			? "throw"
			: IN_HANDS.has(m)
				? "hands"
				: "carry";

// Where a pass is let go (over his head) and caught (chest high), and how
// fast it goes along the ground: a ball thrown on a line, short, lofted as
// it goes deep - as high over the straight line as a ball thrown that long
// rises under gravity.
const RELEASE_Z = 6.9;
// A throw from the pocket (see pocketThrow): wound up and let go over this
// long, the ball back by his ear this far through it; side on to it by
// this much (radians) as he sets, coiled this much more as it goes back,
// this much still at the release, and round past it this much after;
// striding at it this far (feet) and on over his front foot this much more
// in the follow-through, over this long - then a step or two on after it
// (feet, over ms), watching it go.
const WIND_MS = 320;
const WIND_COCKED = 0.55;
const SET_OPEN = 0.55;
const COIL = 0.3;
const RELEASE_OPEN = 0.1;
const PAST_IT = -0.4;
const STRIDE = 1.3;
const WEIGHT_ON = 0.5;
const FOLLOW_MS = 450;
const WATCH_FT = 2.5;
const WATCH_MS = 1100;
// (Still up there this long (ms) after he has walked off his throw: a
// look - WATCH_PAUSE_MS, ms, from and spread - and WATCH_ON_FT more on
// over WATCH_ON_MS.)
const WATCH_LONG_MS = 1800;
const WATCH_PAUSE_MS = [300, 500] as const;
const WATCH_ON_FT = 3;
const WATCH_ON_MS = 1000;
const CATCH_Z = 4.3;
// (A pass over a man in its way: over this high (ft) where it passes him,
// no nearer him than this (ft) - looked at every so often (ms), not right
// at either end (this share of the way) - and no more than this much
// higher at the top (ft). Not over a man down, or going up for it.)
const OVER_Z = 6.2;
const OVER_NEAR = 1.3;
const OVER_STEP_MS = 25;
const OVER_FROM = 0.06;
const OVER_MOST = 4;
const OVER_NOT = new Set<Move>([
	"dive",
	"prone",
	"supine",
	"tackled",
	"tackledBack",
	"getUp",
	"getUpBack",
	"hurt",
	"slid",
	"qbSlide",
	"kneelDown",
	"swat",
	"catch",
	"basket",
	"leapCatch",
	"diveCatch",
]);
const G = 32;
// A pass: on a line at this pace (ft/s), lofted a little slower over this
// much more for each foot it goes, down to no slower than this - a deep
// one with some touch on it, up and over and down into his hands.
const PASS_FPS = 70;
const PASS_LOFT = 0.12;
const PASS_LEAST_FPS = 48;
// (A big arm, against the other quarterbacks in the game: that much harder
// at the most, a weak one that much softer.)
const ARM = 0.08;
const flightOf = (from: Pt, to: Pt, arm = 0): { ms: number; peak: number } => {
	const l = dist(from, to);
	const v =
		Math.max(PASS_LEAST_FPS, PASS_FPS - l * PASS_LOFT) * (1 + ARM * arm);
	const s = l / v;
	return { ms: s * 1000, peak: Math.max(0.4, (G * s * s) / 8) };
};

// The quarterback's drop: back no quicker than this on the way (ft/s, all
// told). One this deep (feet, from under center) taken side on to the
// field, turned this far (radians, the way he opens to throw), for this
// share of it and this share of the time - backpedalling the rest.
const QB_DROP_FPS = 11;
const DROP_TURN_FT = 18;
const DROP_TURN = 1.2;
const DROP_TURNED = 0.75;
const DROP_TURNED_MS = 0.7;

// A kick's rush off the edge (a man this far out from the ball, feet):
// round the end of the wall this far outside him, and in at the kick this
// far in front of the spot and out to the side of it - the next man out
// this much further on each.
const EDGE_A = 12;
const EDGE_ROUND = 3;
const EDGE_IN = 9;
const EDGE_WIDE = 4.5;
const EDGE_STAGGER = 2.5;
// (Bent round the end of the wall in an arc no tighter than he takes at the
// pace he gets there at, at EDGE_LAT ft/s/s across him - out along no more
// than EDGE_BEND_MOST of either side of it - in so many steps.)
const EDGE_LAT = 50;
const EDGE_BEND_MOST = 0.8;
const EDGE_ARC_STEPS = 5;
// (His leap for it carrying him on this long at the pace he came in at,
// seconds, and no further than this, feet.)
const SWAT_ON = 0.2;
const SWAT_ON_MOST = 4;
// (Breaking up a pass: carried on as far as slowing evenly through it, from
// the pace he comes in at, down to a third of that takes him - see pace.ts
// carriedTo - no further than this, feet.)
const SWAT_CARRY = 1.5;
const SWAT_CARRY_MOST = 8;
// (Swatting at it only from this near (feet) where he breaks on it from.)
const SWAT_REACH_FT = 5;
// (Off the edge at a kick, slowed to this, ft/s, to go up for it.)
const SWAT_IN_FPS = 11;
// (No quicker about it than this, ms.)
const SWAT_LEAST_MS = 450;
// (A man breaking up a pass no nearer where it was to be caught than this,
// feet, for it.)
const SWAT_CLEAR = 2;
// (Come in at more than this, ft/s, over what the swat carries him on from:
// gathered for it - see Action.incomplete - there as much as this, ms,
// later for it.)
const SWAT_GATHER_FPS = 3;
const SWAT_GATHER_SPARE_MS = 250;
// (Veered off the way he was going this much, radians, either way - for a
// carry on at least SWAT_VEER_GAIN feet longer - by the man and the ball,
// rather than stopped short of them.)
const SWAT_VEER = [0, 0.25, -0.25, 0.5, -0.5, 0.8, -0.8] as const;
const SWAT_VEER_GAIN = 0.5;
// (And knocking it away, at the receiver's shoulder: this far out in front
// of him and this far to the side, feet.)
const BREAK_IN = 1.4;
const BREAK_ASIDE = 1.6;
// (There this long before it gets there, ms - straight there as it goes
// up, if he is on his way that way already, near enough: see paceOn.)
const BREAK_UP_EARLY_MS = 150;
const BREAK_UP_ON = 0.85;
// An incomplete pass: at the receiver's hands, this far out in front of
// him (feet) - or out of his reach, over them this high - and off them or
// knocked away, coming to rest this far off (feet), dropping to the ground
// half way there over so long (ms) and bouncing the rest.
const INC_HANDS_OUT = 1.3;
const INC_OVER_Z = 6.8;
const INC_OFF_FT = 4;
const INC_KNOCKED_FT = 9;
const INC_DROP_MS = 350;
const INC_BOUNCE_MS = 420;
// Not knocked away: how it gets away from him, his own count - off his
// hands (INC_DROP of the time), out of his reach and laid out for
// (INC_DIVE), and the rest sailed on over his hands as he reaches up at it
// - or, thrown no further than INC_SHORT_FT, into the turf in front of him
// (INC_SHORT of the time). Over him: this high as it goes over (feet), on
// at this share of its pace, to the ground as far as that takes it in
// INC_DROP_MS (no less, no more than these, feet), and on this far on the
// bounce. Short: down this far in front of his hands (feet), and on round
// him on the hop, this far over so long (ms), this high.
const INC_DROP = 0.18;
const INC_DIVE = 0.17;
const INC_SHORT = 0.3;
const INC_SHORT_FT = 45;
const INC_OVER_UP_Z = 8.6;
const INC_SAIL_KEEP = 0.85;
const INC_SAIL_FT = [10, 22] as const;
const INC_SAIL_BOUNCE = 8;
const INC_SHORT_BY = 3.5;
const INC_HOP_FT = 7;
const INC_HOP_MS = 380;
const INC_HOP_PEAK = 1.3;
// Thrown behind him, running across its way: this often of those that
// would have gone on over his hands, him at this pace at least (ft/s),
// running this square to the way it comes at the least (the sine); this
// far behind where he is as it gets there and this far out to the side it
// comes from, this high (feet) - his arm back for it over this long (ms) -
// to the ground this long after (ms), coming to rest this far on.
const INC_BEHIND = 0.45;
const INC_BEHIND_FPS = 11;
const INC_BEHIND_ACROSS = 0.65;
const INC_BEHIND_FT = 1.6;
const INC_BEHIND_OUT = 2.2;
const INC_BEHIND_Z = 4.4;
const INC_BEHIND_REACH_MS = 900;
const INC_BEHIND_DROP_MS = 380;
const INC_BEHIND_ON = 9;
// After one short of him (or over his hands, this often), his palms up at
// the quarterback this often (see palmsUp): round to him over this long
// (ms), and up at him so long (ms).
const INC_PALMS = 0.45;
const INC_PALMS_OVER = 0.2;
const INC_PALMS_TURN_MS = 450;
const INC_PALMS_MS = 1100;
// Thrown away, a man of theirs this near him (feet) as he lets it go, his
// own count of the time: lofted, at this share of the pace he throws it
// at, high over the man it went to and down this far past him (feet) - or
// this far out of bounds, if the sideline comes first - and on this far on
// the bounce.
const AWAY_NEAR = 8;
const AWAY = 0.6;
const AWAY_KEEP = 0.7;
const AWAY_PAST = [20, 34] as const;
const AWAY_OUT = 5;
const AWAY_BOUNCE = 6;
// (No further than this (feet), thrown away from a man a long way off:
// not lofted half the field, hanging there for five seconds.)
const AWAY_MOST_FT = 90;
const AWAY_HANG_MS = 1800;
// (Nobody's: coming down no nearer anybody than this (feet), turned off
// the way it went this much (radians) or sent this much further or
// shorter (feet) if it would.)
const AWAY_CLEAR = 10;
const AWAY_TURNS = [0, 0.15, -0.15, 0.3, -0.3];
const AWAY_LONGER = [0, 10, -6];
// (Up at it as it goes over, his arms up from as it comes till this long
// after it gets there, ms - see reachFor.)
const REACH_UP_AFTER_MS = 400;
// (Round from the way it would go, radians, to pass nobody - no nearer
// the middle of any man than this, feet.)
const DROP_TURNS = [0, 0.7, -0.7, 1.4, -1.4, 2.1, -2.1, 2.8, -2.8];
const DROP_CLEAR = 1.6;
// A gunner set in front of the returner by this long after he would have
// got there at full tilt, ms.
const GUNNER_SET_MS = 900;
// (Waiting on a kick nobody returns at least this long, ms: set for it.)
const FAIR_SET_LEAST_MS = 400;
// On a return, a jammer within this far (feet) of a gunner on him again,
// going in at this pace (feet a second).
const JAM_AGAIN_FT = 25;
// (The wall, waiting for a cover man who would not get to him in time: up
// to meet him at no more than this much of his pace - see lockUp.)
const WALL_UP_PACE = 0.7;
const JAM_AGAIN_FPS = 8;
// (Down there with him, squared up to him at least this long, ms.)
const JAM_SET_MS = 300;
// (Running with him this far behind him and inside him, feet - over to the
// inside of him over JAM_IN_MS, back behind him over JAM_DROP_MS.)
const JAM_BEHIND_FT = 2.5;
const JAM_INSIDE_FT = 2.5;
const JAM_IN_MS = 800;
const JAM_DROP_MS = 1200;
// The cover with nobody bringing it back: down the field at this pace at
// least (feet a second).
const COVER_LEAST_FPS = 10;

// ---- the play ----------------------------------------------------------------

// The blocking on a run: zone (outside zone, to the edge), a power play
// this often, or a sneak; a blocker this far off his man (feet), in beside
// another on a double team this far over; a linebacker over to fill this
// long after the snap (ms), at this much of his speed - and a blocker up
// onto him at this much of his; a man beaten off the block this long after
// (and up to this much longer), and a man who has him off as the carrier
// goes by, as much as this much later; a receiver on a man off the ball
// this far off at most (feet), winning it this often.
type RunScheme = "zone" | "stretch" | "power" | "sneak";
const POWER = 0.35;
const BLOCK_GAP = 2.6;
// (A man blocked off the ball with the carrier coming by him while he is
// still on it - or no later than this after - driven over to no nearer his
// way than this (feet), at no more than this (ft/s) across.)
const CLEAR_LATE_MS = 300;
const BLOCK_CLEAR_FT = 3.5;
// (Blocks go on this long (ms) after he is down - see runBlock.)
const UNTIL_PAST_MS = 300;
const BLOCK_CLEAR_FPS = 4;
// (Up front, only as long as he is up and running: until this long before
// he is down, ms.)
const CLEAR_DOWN_MS = 250;
// (In the way of him before the blocker is on him: a step or two over out
// of it, there as he is no nearer than this (feet), at this pace (ft/s) -
// no more than this far over (feet), and in no less than this (ms) - and
// pulled up in no more than this far on (feet).)
const WAY_CLEAR_FT = 2.6;
// (A run into a pile: a man winning his block this near across the field
// to where the carrier goes down drives his man no further than this short
// of it, feet - the carrier stopped in front of him, not in his back.)
const PILE_FACE_A = 3;
const PILE_FACE_FT = 2.6;
const WAY_FPS = 6;
const WAY_MOST_FT = 4;
const WAY_LEAST_MS = 300;
const WAY_EASE_FT = 1;
// (Driven back, the block won, no quicker than this - ft/s. Lost, the
// ground he gives no quicker than BLOCK_GIVE_FPS: not shoved back off his
// run at him in a stride.)
const BLOCK_DRIVE_FPS = 6;
const BLOCK_GIVE_FPS = 3;
// A pancake now and then: a lineman the sim says won his block on a run,
// on his man alone and driving him at least this long (ms), puts him on
// his back - over this long (ms), this far back (feet), down this long
// (ms) - and steps on over him this far (feet); never where the man with
// the ball comes by, nor with anybody else there (no nearer than this,
// feet).
const PANCAKE = 0.03;
const PANCAKE_DRIVE_MS = 700;
const PANCAKE_FALL_MS = 450;
const PANCAKE_BACK_FT = 1.6;
const PANCAKE_DOWN_MS = 1100;
const PANCAKE_ON_FT = 0.8;
const PANCAKE_CLEAR_FT = 5;
// (Nor anybody else through where he lands while he is down, feet.)
const PANCAKE_ROOM_FT = 2;
// Let up after a pass is away on his own count too, up to this much later
// (ms) - and a block on a run once the man with the ball is down.
const PASS_END_SPREAD_MS = 450;
// A pass in the air longer than this (ms): the rush and the line on it
// let up this long after it goes (and up to PASS_END_SPREAD_MS later, each
// pair on its own count), not when it comes down - see lineAfterIt; turned
// to it over this long (ms); with this long (ms) still to go before it
// comes down, off up the field after it. Incomplete, off after it this
// long (ms) after letting up, if they have not gone already.
const RUSH_LONG_MS = 800;
const RUSH_SEES_MS = 500;
const LINE_TURN_MS = [150, 350] as const;
const LINE_GO_LEAST_MS = 900;
const LINE_AFTER_IT_MS = [100, 450] as const;
// Out on a route it did not go to, his route run with this long (ms) or
// more still to go before it comes down: off after it at a jog, as far as
// this far (feet) from where it comes down, and this much further on his
// own count - see othersAfterIt. (Stood at the end of his route as it goes:
// off after it as he sees it go - see RUSH_SEES_MS. No slower than this
// (ft/s): as slow as gets him there as it comes down - not there with time
// to stand about. On the move, and that would have him crawl: on at it at
// that, as far as that takes him as it comes down - no nearer it than this
// (feet) - not pulled up short of it with time to stand about.)
const AIR_ON_LEAST_MS = 1500;
const AIR_ON_KEEP_FT = [15, 15] as const;
const AIR_ON_FPS_LEAST = 3;
const AIR_ON_NEAR_FT = 8;
// (A rusher this near a blocker as it goes (feet): on him, the two of them
// letting go of each other at once.)
const LINE_PAIR_FT = 4;
// (Off his man or his zone for the ball, once it is thrown, up to this much
// later than the first of them, ms.)
const COVER_BREAK_SPREAD_MS = 220;
// (Further off where it goes than the men breaking on it, on his way over
// to it all the same as he reads it - see rally: at this much of his speed,
// at this much further than RALLY_FAR_FT off; aiming this far in front of
// where it comes down, toward his own goal line (feet), and on at it this
// long after it does (ms); not with less than this long to go (ms); round
// into it from no quicker than this (ft/s).)
const RALLY_PACE = 0.7;
const RALLY_FAR_PACE = 0.6;
const RALLY_FAR_FT = 100;
const RALLY_AHEAD_FT = 15;
const RALLY_ON_MS = 150;
const RALLY_LEAST_MS = 400;
const RALLY_TURN_FPS = 6;
// (Down at a blocker on a return at this much of his speed, give or take
// half of this; the blocker off at him up to this much later, ms.)
const LOCK_PACE = 0.85;
const LOCK_PACE_SPREAD = 0.1;
const LOCK_REACT_SPREAD_MS = 250;
// (A man coming down on a kick off his mark once it is down, and a block on
// him let go of once it is over, each up to this much later, ms.)
const LOCK_GO_SPREAD_MS = 200;
const LOCK_END_SPREAD_MS = 400;
// (The man with the ball this far by a block on a return (feet): it let go
// of this long after (ms) - not still at it the length of the field.)
const LOCK_PAST_FT = 15;
const LOCK_PAST_MS = 900;
// (A blocker waiting on him where he is: down in his stance for the last
// this long (and up to this much more), ms - stood up watching it before
// that, if that is LOCK_WATCH_MS at least.)
const LOCK_SET_MS = [900, 400] as const;
// (One going up to meet him slower than a walk: set where he is, alive in
// it, and up into him over his last few steps at this pace (ft/s) - not
// walked up over all the time there is, if it leaves this long (ms) set.)
const LOCK_STEP_FPS = 7.5;
const LOCK_STEP_SET_MS = 300;
// (Waiting on him, set: a step to him no longer than this, feet, not
// taken.)
const LOCK_SHIFT_FT = 1;
const LOCK_SHIFTS = new Set<Move>(["backerReady", "safetyReady"]);
// (Not to him till this close to the end of it (ms): no block at all -
// still on his way in when it is over - and the two locked up drifting
// with the return no quicker than this, ft/s. Short of that, a block
// popped just as he is brought down: on him, not stood a step off him.)
const LOCK_LEAST_MS = 300;
// (Waiting for him, not met till it is all but over: up to meet him as he
// comes, there no later than this long before it would be too late - off
// as late as that has him, at no more than the pace he is given.)
const LOCK_UP_SPARE_MS = 500;
const LOCK_DRIFT_FPS = 4;
// (How closely lockUp works out where the two meet.)
const LOCK_MEET_TRIES = 20;
// (A tackler set waiting on the man with the ball: turned with him every
// this long, ms, if he has come round this far, radians.)
const KEEP_STEP_MS = 300;
const KEEP_TURN = 0.15;
// (And, this far (feet) from where he meets him at least, and the man with
// the ball further off it than this, round it as the man comes round to
// it - up to this far round from where he set (radians), this far a step
// at most and no less (feet).)
const MIRROR_LEAST_FT = 2.5;
const MIRROR_NEAR_FT = 4;
const MIRROR_MOST = 0.5;
const MIRROR_STEP_FT = 1.2;
const MIRROR_STEP_LEAST_FT = 0.5;
// (Broken on the ball sooner, going away from it - see breakSoonest -
// looked at this often, ms.)
const BREAK_SOONER_STEP_MS = 50;
// (A carrier's run weaved to take the time it has to: cut no further round
// than this (radians) at the turn in it.)
const WEAVE_TURN = 1;
// (Nor broken on it at all, if his way takes him this near (feet) where
// he is to be for it anyway.)
const BREAK_KEEP_FT = 2;
// (Nor with less time for it than this, ms. Going away from it, no time
// to plant, stopped short over this far, feet, before he comes back.)
const BREAK_LEAST_MS = 200;
const BREAK_STOP_FT = 3;
// (Slowed sooner for it instead, if he saw it in time: at this, ft/s/s -
// see STOP_ACCEL, STOP_HARD - off the way he goes, as long as he is going
// that way still, near enough - this far round it, radians.)
const BREAK_SLOW_ACCEL = 30;
const BREAK_SLOW_TURN = 0.5;
// (The man the sim says got to the ball, on his man: no deeper than it,
// feet - see cover.)
const MARK_DEEP_FT = 3;
// The man flagged for holding in coverage, no further off his man than this
// (feet) - in his hip pocket, a hand on him.
const HOLD_CUSHION = 1.5;
// (Over toward it from this far off his man (feet), all the way this much
// further.)
const MARK_NEAR_FT = 8;
const MARK_FAR_FT = 16;
// (And all the way over to it by the time he sees it thrown, from this
// long before then, ms.)
const MARK_READ_MS = 1000;
// (And one this deep, feet, set with this long to spare at least (ms):
// read steps back first, this far (feet), over this long.)
const READ_DEEP_FT = 18;
const READ_BACK_LEAST_MS = 1500;
const READ_BACK_FT = 2.5;
const READ_BACK_MS = 450;
const LOCK_WATCH_MS = 500;
// (Up by the end of it after the whistle, each up to this much later, ms.)
const TRAIL_SPREAD_MS = 500;
// (Not walked there while it is still on: after the ball at a jog (ft/s,
// slowest, quickest) after a look at it (ms, soonest, latest), kept this
// far off it at least (feet) - if it is any further than this (feet).)
const TRAIL_JOG_FPS = [8, 10] as const;
const TRAIL_LOOK_MS = [150, 450] as const;
const TRAIL_JOG_LEAST_FT = 5;
// (There already, with longer than this (ms) to go till the whistle: with
// it all the same, not stood there through it.)
const TRAIL_IDLE_MS = 1500;
const TRAIL_KEEP_FT = 12;
// Each trailer's place round where it ends no nearer another's than this
// (feet): round the ring from where he comes, not all in on the one spot.
const TRAIL_APART_FT = 4;
// (Along with the ball, kept off it - see alongWith - no nearer it than
// this (feet), out off it no more than ALONG_OUT_FT a step.)
const ALONG_NEAREST_FT = 3.5;
// (Along with it no nearer than this (feet) to any other man up on his
// feet then - two going its way on near the one line kept off each other,
// not run together into the one spot.)
const ALONG_APART_FT = 3.5;
const ALONG_OUT_FT = 1.2;
// (The others out on routes once it is caught - see withIt: off after it
// so long after they see it (soonest, latest), ms, at a jog (ft/s), kept
// this far off it (and up to this much more), ft - not for less than this
// long (ms), nor from nearer it than that and this far, ft.)
const ROUTE_AFTER_MS = [250, 700] as const;
const ROUTE_AFTER_FPS = [8, 10.5] as const;
const ROUTE_AFTER_KEEP_FT = [14, 8] as const;
const ROUTE_AFTER_LEAST_MS = 700;
const ROUTE_AFTER_LEAST_FT = 6;
// (And the quarterback, his throw caught: kept further off it (and up to
// this much more), ft, at an easier jog, ft/s.)
const QB_AFTER_KEEP_FT = [30, 15] as const;
const QB_AFTER_FPS = [6.5, 9] as const;
// (The line once a pass is caught - see downfield: off after the play
// so long after it let up (soonest, latest), ms, at a jog (ft/s, slowest,
// quickest) - no nearer where it ends than this, nor going less than this
// far, ft.)
const DOWNFIELD_REACT_MS = [400, 1000] as const;
const DOWNFIELD_FPS = [6.5, 10] as const;
const DOWNFIELD_SHORT_FT = 15;
const DOWNFIELD_LEAST_FT = 4;
// (On after the man with the ball, pulled up as he sees it is over - each
// up to this much before the whistle, and on for at least this much of it.)
const FOLLOW_UP_SPREAD_MS = 350;
const FOLLOW_ON_LEAST = 0.6;
// (A kick wall let up once the kick is away, up to this much later, ms.)
const KICK_WALL_SPREAD_MS = 350;
const RUN_END_SPREAD_MS = 400;
// A lineman's first step on a run, off his stance: no quicker than this
// on it, ft/s (his pace over the whole of it - see runBlock).
const FIRST_STEP_FPS = 8;
// Off the snap, each on his own count: a blocker this long after it (ms),
// up to this much later; his first step this much longer or shorter (a
// share of it) - and a lineman across from him reading it, this long
// after it, up to this much later.
const GET_OFF_MS = 30;
const GET_OFF_SPREAD_MS = 80;
const GET_OFF_STEP = 0.3;
const READ_OFF_MS = 60;
const READ_OFF_SPREAD_MS = 110;
// The rest off the snap on their own counts the same way: a man on a man
// in coverage, a man dropping into a zone, a linebacker's read step on a
// run, a receiver out to block, the defensive back he blocks reading it.
const MAN_OFF_MS = 40;
const MAN_OFF_SPREAD_MS = 100;
const ZONE_OFF_MS = 100;
const ZONE_OFF_SPREAD_MS = 120;
const READ_STEP_MS = 60;
const READ_STEP_SPREAD_MS = 120;
const STALK_OFF_MS = 50;
const STALK_OFF_SPREAD_MS = 80;
const STALK_READ_MS = 70;
const STALK_READ_SPREAD_MS = 120;
// (Not so soon after what he is doing ends as to make a sliver of it, ms.)
const OFF_LEAST_MS = 40;
const SNAPPER_OFF_MS = 40;
// A lineman off the ball into the man on him: no nearer than this (feet),
// a step or two up into him, his arms out into him - not a run on the spot.
const FIRE_FT = 4;
const FIRE_HELD_MS = 250;
const FIRE_LEAST_HELD_MS = 100;
// (On him in less than this (feet): square to him all through it.)
const FIRE_SQUARE_FT = 2;
const fireOff = (m: Actor, to: Pt): Move =>
	dist(m.at, to) < FIRE_FT ? "engage" : "run";
// Locked up on a run: shoved one way and then the other, every this long
// (ms, give or take), from this long after the snap - this far across the
// field (feet) and up to this far up or back; eased into each, a step this
// long (ms) at a time.
const JOSTLE_MS = 420;
const JOSTLE_FROM = 650;
const JOSTLE_A: [number, number] = [0.5, 1.1];
const JOSTLE_D = 0.4;
const JOSTLE_STEP_MS = 90;
// (Over from one to the next no quicker than this at the height of it,
// ft/s: shoved a short way, given a short while - not flung across.)
const JOSTLE_MOST_FPS = 5;
// (A man into it after it has begun: into the shoving over this long, ms.)
const JOSTLE_IN_MS = 300;
const DOUBLE_FT = 2.2;
// (In beside a man on a double team no further off than this from where he
// is a moment after the snap, feet - and only while he is on his man.)
const DOUBLE_REACH_FT = 10;
// (Over to him up the field, driving, no quicker than this, ft/s.)
const DOUBLE_DRIVE_FPS = 8;
const FILL_MS = 1050;
const FILL_PACE = 0.7;
// (Over to fill no slower than this, ft/s - set first for it, if he is,
// no less than this long, ms.)
const FILL_LEAST_FPS = 9;
const FILL_SET_LEAST = 150;
// (Over to fill from well across, no nearer the line than this, feet: run
// over behind the men up front and the blocks on them, not through them.)
const FILL_SCRAPE_FT = 6;
// (A linebacker over to fill no nearer one already there than this, feet:
// pulled up this much of the way back short of it instead, the first that
// will do.)
const FILL_APART_FT = 4;
const FILL_SHORT = [0.3, 0.55, 0.8] as const;
// (Nor pulled up on into a block - a man in one this near (feet) where he
// would pull up, from when he is to be there to this long after (ms),
// looked at every FILL_ROOM_EVERY_MS: pulled up into the spot instead.)
const FILL_ROOM_FT = 2.4;
const FILL_ROOM_MS = 600;
const FILL_ROOM_EVERY_MS = 100;
// (Not clear even where he gets to: pulled up on his way there instead,
// from no further back than this - see shortOfBlocks.)
const FILL_BACK_STEP_MS = 50;
const FILL_BACK_MOST_MS = 800;
const CLIMB_PACE = 0.85;
// (Come up past the man he is to block on the run: looked back for a
// way into him that has him in front of him, this far (ms) at the most,
// a step at a time - see blockOn.)
const PAST_BACK_STEP_MS = 50;
// (A defensive lineman taken on by a man up front no more than this much
// further off him than the nearest - feet: see runBlock.)
const FRONT_PAIR_SLACK_FT = 3;
const PAST_BACK_MOST_MS = 600;
// (Up to a man past others in blocks - over the line to the next level, say
// - not straight through them: no nearer a man in one than CLIMB_CLEAR_FT,
// looked at every CLIMB_LOOK_MS on his way, as near as it gets him - round
// a corner bent off to one side of straight there, part of the way along it
// (CLIMB_ALONG) and so far off it (CLIMB_OFF, feet) - in no more than
// CLIMB_ROUND_MORE times the time it takes him at his pace for it. Not for
// a way shorter than CLIMB_ROUND_LEAST_FT, nor in less than
// CLIMB_ROUND_SLACK_FT of them.)
const CLIMB_CLEAR_FT = 2.4;
const CLIMB_LOOK_MS = 50;
const CLIMB_ALONG = [0.5, 0.35, 0.65] as const;
const CLIMB_OFF = [2, -2, 3, -3, 4, -4] as const;
const CLIMB_ROUND_MORE = 1.1;
const CLIMB_ROUND_LEAST_FT = 10;
const CLIMB_ROUND_SLACK_FT = 0.3;
const CLIMB_INTO_FT = 1.2;
// A back out of the backfield on a route (from at least RELEASE_BACK_FT
// behind the line to RELEASE_PAST_FT past the men on it), taken round the men in
// blocks on it if he would come nearer one than CLIMB_CLEAR_FT less
// RELEASE_INTO_FT on his way - a way round no more than RELEASE_MORE
// times as far (see releaseRound).
const RELEASE_BACK_FT = 4;
const RELEASE_PAST_FT = 1.5;
const RELEASE_SIDE_FT = 4;
// (Back onto the way he was going no more than this far off it - radians -
// as soon past them as that can be done, up to this much later, a step of
// this long at a time.)
const RELEASE_KINK = 0.4;
const RELEASE_LATER_MS = 800;
const RELEASE_LATER_STEP_MS = 100;
const RELEASE_INTO_FT = 0.6;
const RELEASE_MORE = 1.1;
// (Thrown to: all of it this long before the ball gets to him, at least.)
const RELEASE_CATCH_MS = 300;
// (A man off downfield with nobody near to block on a run: his spot moved
// over this far, feet, either way - the least first - to keep him clear of
// the men in blocks on his way there, and this long, ms, after.)
const RELEASE_CLEAR_OFF = [0, 3, -3, 6, -6, 9, -9] as const;
const RELEASE_CLEAR_MS = 300;
const SHED_MS = 550;
const SHED_SPREAD = 350;
const OFF_SPREAD = 350;
// (Nor held up on a block, a man up front, with the man with the ball this
// far (feet) off from where it is, round the edge or back behind it all -
// from this long after the snap (ms): off after him, up to OFF_SPREAD
// later.)
const AWAY_FT = 24;
const AWAY_LEAST_MS = 1500;
const STALK_FAR = 36;
const STALK_WON = 0.6;
// (Nobody to block out there: in toward where the run goes once he has
// let up, so long after (soonest, latest), ms, at a jog (ft/s, slowest,
// quickest) - no nearer where it ends than this, ft; not for less than
// DOWNFIELD_LEAST_FT.)
const WIDE_IN_MS = [250, 750] as const;
const WIDE_IN_FPS = [7, 10.5] as const;
const WIDE_IN_SHORT_FT = 18;
// (The man he goes out onto - or a linebacker over to fill - pulled up
// there no more than this far on, feet.)
const STALK_EASE_FT = 5;
// (Nobody near to block: set for one this long at the most, ms.)
const STALK_SET_MS = 1200;

// After the ball: at this much of his speed at most - this much faster than
// he needs to be, to get there in time - a leg this long at a time (ms), at
// where the carrier will be this long after it, this far off him (feet),
// closing in as he goes down - each this far at most to one side or the
// other of the way he is going, a lane of his own.
const PURSUIT_PACE = 0.85;
const PURSUIT_URGE = 1.2;
const PURSUIT_STEP = 350;
// (Off the field after a kick, along the sideline from the others off, at
// least, feet - see offSpot.)
const OFF_APART_FT = 3;
// (At most this many stretches of a lane run - see laneRun.)
const LANE_RUN_STEPS = 24;
const PURSUIT_LEAD = 300;
const PURSUIT_RING = 10;
// (In nearer him than that already, closing in this much a step, no nearer
// than so much.)
const PURSUIT_IN = 1.5;
const PURSUIT_NEAREST = 5;
// (A man after him ahead of him kept this far out to the side of his way,
// square to it, closing in - see pursue.)
const PURSUIT_SIDE_FT = 3.5;
const PURSUIT_LANE = 7;
// (Stood off the play, the ball coming to within this of him: backed off
// this far at the most, at this pace, over this long at the least - see
// giveWay.)
const GIVE_WAY_FT = 5;
const GIVE_WAY_MOST_FT = 3.5;
const GIVE_WAY_FPS = 6;
const GIVE_WAY_MS = 500;
const GIVE_WAY_EASE_FT = 2.5;
const GIVE_WAY_EASE_LEAST = 3;
// (A stretch of the chase turned this far off, either way - the least
// first - to keep him round men in blocks: see wayClear. Looked on along
// it CHASE_LOOK_MS past its end; turned no further off the way he goes
// than CHASE_TURN_MOST, if he is going - not planted for it; men going
// slower than CHASE_STILL_FPS, ft/s, kept round too (and where their legs
// leave them, as far as they are laid), and men of the other side going
// slower than CHASE_PASS_SLOWER of his pace - not run up the back of.)
const CHASE_CLEAR_TURN = [0.15, -0.15, 0.3, -0.3, 0.45, -0.45] as const;
const CHASE_LOOK_MS = 500;
const CHASE_TURN_MOST = 0.5;
const CHASE_STILL_FPS = 4;
const CHASE_PASS_SLOWER = 0.7;
const CHASE_CLEAR_SLACK_FT = 0.3;
// (Given up, or at the whistle: pulled up no more than this far on - feet;
// bent round off the pile at the ball, if it is in the way, by as little of
// these as (radians, all told) keeps him this far off it.)
const EASE_OFF_FT = 12;
// (A man slowing up after he is done: this far on, feet - see slowUp.)
const SLOW_UP_FT = 2.5;
const EASE_BENDS = [0.35, 0.7, 1.05, 1.4];
const EASE_PAST_FT = 3.5;
// (Easing off into an edge: turned along it over all of it, or if that
// still takes him over, over the first this much of it - the nearer way
// round, or toward the middle when the two are within this much, radians.)
const EASE_EDGE_BY = [1, 0.6, 0.35, 0.2, 0.1] as const;
const EASE_EDGE_TIE = 0.2;
// (An end line, though, he may ease off over, out the back of the end
// zone - this much further than he can otherwise go, feet: short of the
// posts' pads and the photographers behind it - see scene.ts, media.ts.
// And a sideline, onto the white border - short of the chains, and the
// coaches' line - see chains.ts, geometry.ts.)
export const EASE_END_FT = 4;
const EASE_SIDE_FT = 4;
// (A man chasing a spot past where he may go (see chaseUp) - further over
// than this, ft - eased off into the edge going at it within this of
// straight, radians.)
const EDGE_CLAMP_FT = 0.5;
const EDGE_EASE_TURN = 1.1;
// (Carrying on the way he was going, before he eases off, ms - see carryOn.)
const CARRY_MOST_MS = 500;
// (Broken down a few steps off a man to hit, this long at least - see
// closeOn.)
const BREAK_DOWN_MS = 450;
// (Not broken down at all with less than this long, ms, to be set in it.)
const BREAK_SET_MS = 150;
// (A back out in front of the quarterback keeping it: up to the hole at
// this pace at least (ft/s), well ahead of him - not timed to be there just
// before him at a jog, and run up the back of on the way through it.)
const KEEP_LEAD_FPS = 13;
// (A long ball still up there, more time than this to spare, ms, on the
// break for it at full tilt: at a pace that has him there this long before
// it instead (no slower than UNDER_LEAST_FPS), slowed to it off his run as
// he goes - not there flat out and stood set under it the while.)
const UNDER_SPARE_MS = 1200;
const UNDER_SET_MS = 600;
const UNDER_LEAST_FPS = 4;
// (No quicker than a walk, ft/s - see gait: going in on a man, or for the
// ball, not walked there.)
const WALK_MOST_FPS = 6.5;
// (And as slow as a jog up to it goes, ft/s - a little over that.)
const JOG_LEAST_FPS = 7.5;
// (A guard pulling down the line, no slower than this, ft/s - and a man in
// on a pile round a loose ball.)
const PULL_FPS = 12;
const PILE_RUN_FPS = 12;
// (The guard at the hole this long before the man with the ball - well
// ahead of him, for him to come off his hip, not up his back.)
const PULL_AHEAD_MS = 450;
// (Within a few steps of it already, and going no quicker than this (feet a
// second): broken down if he has this long before he goes in, this far off
// it, shuffled there at this pace.)
const BREAK_NEAR_MOST = 10;
const BREAK_NEAR_MS = 350;
const BREAK_NEAR_FT = 3;
const BREAK_NEAR_FPS = 6;
// (Going quicker than that on to it, with this long (ms) at least before he
// goes in: pulled up on the way he was going first - see closeOn.)
const BREAK_PULL_LEAST_MS = 900;
// (Off out of it into the hit up to speed (see GET_OFF_ACCEL) - this long
// before it at least, ms.)
const BREAK_FIRE_MS = 300;
// (His run slowed by more than this much over: in the gait he goes in now.)
const SLOW_RELABEL = 1.3;
// (Slowed the more the further along he is: in pieces this long, ms.)
const SLOW_PIECE_MS = 200;
// After a man gone for a score, losing ground on him this far back (feet):
// given up, now and then.
const GIVE_UP_FT = 24;

// A quarterback with it in the open, down feet first this often - a run
// that ends on a stretch this long at least (feet); sliding on this far
// (feet), this long (ms); the men on him pulled up this far off him.
const QB_SLIDE = 0.55;
const SLIDE_FROM = 15;
const SLIDE_FT = 5;
const SLIDE_MS = 650;
const PULL_UP_FT = 3.4;

// A new set of downs: the man who got it pointing the way they are going
// this often - down only this long first (ms) - for this long.
const POINT_FIRST = 0.55;
const UP_QUICK = 450;
// (Not while a man lies across him - nor in the spot he would stand in:
// looked for this often (ms), up this long after the last of it.)
const UP_OFF_EVERY_MS = 100;
const UP_OFF_MS = 350;
const POINT_MS = 1000;
// (Made by this many yards gained at least: his moment after it, the
// camera in on it from this long before he points.)
const POINT_BIG = 20;
const POINT_LEAD = 300;

// A turnover: the man who came up with it mobbed this often.
const TAKEAWAY = 0.75;
// A sack: the man who made it up and into it this often.
const SACK_PARTY = 0.7;
// The man who got the sack, up off him: a flex, a fist pump, a leap, a
// dance or a finger to the sky - up to each of these shares of his count
// (the last, the rest).
const SACK_WAYS = [0.35, 0.55, 0.7, 0.85] as const;
// The quarterback sacked, on his own count: out of the pocket this often -
// with it this far behind the line at least (feet), the room back there
// for it - this far across (feet), away over this long (ms); or else back
// past where they get him and stepping up into it this often, this far
// (feet).
const SACK_OUT = 0.2;
const SACK_OUT_LEAST = 18;
const SACK_OUT_FT = [14, 24] as const;
const SACK_OUT_MS = 1500;
// (A sack up in among his own line: moved across this far at the most, a
// step at a time, to have his way to it this clear of them - see
// pocketGap.)
const SACK_GAP_FT = 2;
const SACK_FALL_FT = 2.5;
const SACK_FALL_MS = 450;
const SACK_GAP_STEP = 0.5;
const SACK_GAP_MOST = 8;
const SACK_STEP_UP = 0.35;
const SACK_STEP_FT = [3, 6] as const;
// Sacked from his blind side (see blindLook): this often, on his own
// count, with the man who gets him this far round to his left of
// downfield (radians) this long before it (ms) - his eyes at the last out
// to his right, this far round (radians, a man out there in it if there
// is one).
const SACK_BLIND = 0.55;
const SACK_BLIND_MS = 900;
const SACK_BLIND_FROM = 0.3;
const SACK_BLIND_LOOK = [0.5, 1.2] as const;
// Another of them slipped first (see slipFirst): this often, on his own
// count, this long before the sack (ms) - him set this long before it at
// least - off his block this long before it at the least and at the most,
// from no further than this (feet), at him from no more across than this
// (the cosine off the line of the field), the man who gets him this far
// over on the side he gets away to (feet); in this far short of him (feet)
// this long after it, off his feet at him and down this far past where he
// was by this long after it, there this long (ms). Him out from under it:
// round on a planted foot this often, else a quick step aside over this
// long (ms) - either way this far (feet) - and brought down no nearer the
// line of the dive than this (feet).
const SACK_SLIP = 0.45;
const SLIP_BEFORE = [1100, 1400] as const;
const SLIP_SET_MS = 400;
const SLIP_LEAST_MS = 350;
const SLIP_EARLY_MS = 900;
const SLIP_FAR = 16;
const SLIP_ALONG = 0.6;
const SLIP_SACKER = 2;
const SLIP_HIT_FT = 3;
const SLIP_DIVE_AFTER = 60;
const SLIP_DIVE_MS = 520;
const SLIP_PAST = 0.2;
const SLIP_DOWN_MS = 700;
const SLIP_SPIN = 0.5;
const SLIP_STEP_MS = 380;
// (Ducked under him this often - the rest spun or stepped out of, half and
// half: him by on the other side this far (feet) and down this far on past
// where the quarterback was (feet); down under it from this long before
// he gets there (ms), over this long.)
const SLIP_DUCK = 0.35;
const SLIP_DUCK_BY = 2.2;
const SLIP_DUCK_PAST = 2.5;
const SLIP_DUCK_IN = 250;
const SLIP_DUCK_MS = 750;
// (The step round toward where he goes: this share of the way over, then
// this share on from there toward it.)
const SLIP_ROUND = 0.8;
const SLIP_ROUND_ON = 0.3;
const SLIP_OVER = 3;
const SLIP_CLEAR = 3.5;
const sackOut = (qb: Actor, key: number, sackD: number) =>
	sackD <= -SACK_OUT_LEAST && countOf(qb, key, 105) < SACK_OUT;
const sackStepUp = (qb: Actor, key: number, sackD: number) =>
	!sackOut(qb, key, sackD) && countOf(qb, key, 107) < SACK_STEP_UP;
// A man in his moment after a big play - a sack, a stop, a takeaway, a
// score - played out to the crowd on the press box side, more or less:
// give or take up to five times this (radians), his own way of standing
// (no draw of the play's).
const OUT_YAW = Math.PI / 2;
const OUT_OFF = 0.1;
const playedOut = (m: Actor) =>
	OUT_YAW + ((Math.abs(m.pid * 37) % 11) - 5) * OUT_OFF;
// In crunch time: a sack, a stop, a turnover let out this often; a new set
// of downs pointed this often; and this many round a man who scores.
const CRUNCH_PARTY = 0.95;
const CRUNCH_POINT = 0.85;
const CRUNCH_MOB = 6;
// The picture in close on the man a moment is about (see the director's
// heroClose, aim.ts): from down on the field this far in front of him.
export const HERO_CAM_FT = 22;

// Out of bounds: the whistle as he gets this far over the sideline (feet),
// on another stride or two - no further over than OUT_MOST - and pulled up;
// the man on him there into him with both hands this often. Not from more
// than OUT_FAR across the field from it on a run, OUT_NEAR from a catch (or
// most of the way he goes downfield, if more).
const OUT_PAST = 1.5;
const OUT_MOST = 9;
// (Pulled up over it from his pace - this fast at least, ft/s; this fast
// off a toe tap - at this many ft/s a second, over this far, feet, at the
// least and most; turned along the line over all of it, or if that takes
// him too far over, over the first this much of it.)
const OUT_LEAST_FPS = 6;
const OUT_TOE_FPS = 8;
const OUT_DECEL = 14;
const OUT_EASE_FT = [3, 11] as const;
const OUT_TURN_BY = [1, 0.7, 0.45, 0.25] as const;
// (Into a team area on the near sideline - or this near one - no further
// over than this: the men stood there between him and the camera.)
const OUT_MOST_NEAR = 4.5;
const OUT_CROWD = 10;
const SHOVE = 0.6;
const SHOVE_ON_FT = 8;
const OUT_FAR = 66;
const OUT_NEAR = 30;

// After the whistle, how long a man down stays down, and getting up.
const DOWN_MS = 900;
const GET_UP_MS = 1000;
// Each man reads the play this long after the snap before he goes to the
// ball.
const READ_MS = 350;
// A wait shorter than this, on his way somewhere: taken going there more
// slowly, not stood.
const WAIT_LEAST_MS = 150;
// (Nor, cut down to that, stood that long turned no more than this far
// round from the way he was - radians: see stretchOver.)
const STRETCH_OVER_TURN = 0.6;
// (A receiver there early, on the move, by no more than this far at the
// pace he is going, feet: on in his stride to it - see reachFor.)
const CATCH_ON_FT = 3;
// The catch (see poses.ts): reached out to it this long before his hands
// are on it, and in to his chest this long after.
const CATCH_REACH_MS = 300;
// Open for it, his hand up (see callFor): a pass going this far (feet), no
// man in coverage nearer him than this (feet) - from this long before it
// is thrown (and no sooner than this after the snap) to this long before,
// or not at all if that leaves less than this; this likely, where the
// game does not rate his hands - and where it does, from this likely with
// the worst hands in the game at his position on up by this much. (Let
// down as it is thrown, over this long after, ms - no later than this
// before he reaches for it.)
const CALL_FAR = 36;
const CALL_OPEN = 5;
const CALL_FROM_MS = 900;
const CALL_LEAST_MS = 1200;
const CALL_TO_MS = 120;
const CALL_SHORTEST_MS = 300;
const CALL_DOWN_MS = 250;
const CALL_CLEAR_MS = 50;
const CALL_ODDS = 0.45;
const CALL_LOW = 0.2;
const CALL_SPAN = 0.7;
const CATCH_IN_MS = 220;
// Picking it off going at least this fast: caught on the run, his arms
// up for it as he goes - not stopped dead to catch it.
const PICK_ON_FPS = 10;
// A pick, on the man's own count: off the receiver's hands this often (him
// no further than this, feet, from where it was going) - up this high off
// them (feet), this far over toward the man who gets it (feet), his this
// long after (ms); up off his feet for it this often, on one thrown this
// far (feet) or more; laid out for it this often, a return of no more than
// this many yards after - as far as a man going his pace goes in this long
// (s), no less and no more than these (feet): a receiver's dive.
const PICK_TIP = 0.15;
const PICK_TIP_NEAR = 5;
const PICK_TIP_UP = 3.5;
const PICK_TIP_OFF = 3.5;
const PICK_TIP_MS = 550;
const PICK_LEAP = 0.3;
const PICK_LEAP_FT = 50;
// (From no further from it than this (feet) as he goes up: no faster
// through the air than a man - see leapFor.)
const PICK_LEAP_REACH = 12;
const PICK_LEAP_STILL_FT = 4;
// (Turned round to it as he waits on it, if he is round from it more than
// this (radians) - at this many radians a second at the most.)
const PICK_TURN_LEAST = 0.6;
const PICK_TURN_RATE = 9;
// (Locked up with a man, facing him, round from the way he goes off after
// more than this, radians: turned onto it over the last of it - see
// turnInto, turnsOffBlocks.)
const TURN_INTO_LEAST = 1.2;
// (Set that way this long at least first, ms - a short lock has the rest
// of it to come round in: round all the way before he goes, not half way
// and spun the rest as he goes.)
const TURN_SETTLE_MS = 200;
// (Unless he came into it round from it no more than this, radians.)
const TURN_SETTLE_INTO = 0.5;
// (Round from it more than this, radians - near enough right round to go
// either way: round the way he goes on round to, over his first strides,
// this long, ms.)
const TURN_EITHER = 2.6;
const TURN_LOOK_MS = 400;
const SHEDS = new Set<Move>(["engage", "drive"]);
const PICK_DIVE = 0.2;
const PICK_DIVE_YDS = 6;
const PICK_DIVE_PER = 0.6;
const PICK_DIVE_LEAST = 6;
const PICK_DIVE_MOST = 13;
// (No nearer than this (feet) to the man it was meant for, stood there
// for it, as he lands: short of him - or, not room enough for the
// shortest dive short of him, taken on his feet instead.)
const PICK_DIVE_CLEAR_FT = 3;
// (Laid out for it off a run no more than this far round from the way he
// goes, radians - judged over the last this long, ms.)
const PICK_DIVE_TURN = 0.6;
const PICK_DIVE_LOOK_MS = 100;
// Picked off right at the line - no more than this many yards past it -
// on the man's own count, this often: knocked up there by one of the rush
// (see batAt) - him, if he is one of the front, or one no further than
// this from him (feet) as it goes by. Up this high off his hands (feet,
// see the fly's peak); down a step or so on from where he comes down
// (feet), or this far over toward the other man at most; his hands this
// far out in front of him to it, up for it this long before (ms).
const PICK_BAT_YDS = 3;
const PICK_BAT = 0.65;
const PICK_BAT_NEAR = 22;
const PICK_BAT_POP = [6, 11] as const;
const PICK_BAT_SELF_FT = [1.5, 3.5] as const;
const PICK_BAT_FT = 12;
const PICK_BAT_HANDS_FT = 1.2;
const PICK_BAT_REACH_MS = 650;
// A knee: a lineman and the man across from him (no further along the line
// than this, feet) up out of their stances into each other, this often on
// the lineman's own count - off the snap after this long (ms, and up to this
// much more), in this long, leaned on each other till about the whistle (up
// to this much after it) - and let go.
const KNEEL_LEAN = 0.7;
const KNEEL_PAIR_FT = 3.5;
const KNEEL_OFF_MS = 80;
const KNEEL_OFF_MORE = 150;
const KNEEL_IN_MS = 300;
const KNEEL_LEAN_MORE = 400;
// A defender sees the ball thrown and breaks on it this long after.
const BREAK_MS = 250;
// On his man in coverage: where he goes, worked out a step at a time, so
// long a step (from so long after the snap).
const COVER_STEP = 220;
// (Picking up pace on to him from one step to the next no quicker than
// this, ft/s/s.)
const COVER_ACCEL = 36;
const COVER_STEP_FIRST = 200;
// Off him by the room he lined up with: giving up COVER_GIVE of what his man
// makes down the field, and coming down on him at COVER_COME ft/s from
// COVER_READ_MS after the snap - to no closer than COVER_MIN, easing on
// to that over about COVER_SOFT (feet). And facing up the field at him
// while he is more than COVER_FRONT in front of him (see faced).
const COVER_GIVE = 0.45;
const COVER_COME = 4;
const COVER_READ_MS = 600;
const COVER_MIN = 2.5;
const COVER_SOFT = 3;
// (A cover man against the others at his position: sat this much tighter
// on his man - or looser - at the most.)
const COVER_TIGHT = 0.3;
const COVER_FRONT = 1;
// (On a man thrown to on his back shoulder: carried on up the field past
// him as he pulls up for it, this long (ms) - going this fast at least
// (ft/s) - at this share of his pace.)
const COVER_LATE_MS = 600;
const COVER_LATE_LEAST = 10;
const COVER_LATE_KEEP = 0.9;
// Zone behind it, this often - two safeties deep (the strong safety this
// deep at the snap at least, feet), or one - the rest man; never with a
// corner pressed (this near the line, feet). Two deep, the safeties each
// take a half (out toward this share of the way to the sideline) and the
// corners sit in the flats - or, man under it, the safeties are the only
// two not on a man. One deep, the free safety the middle and the corners a
// deep third each, the strong safety and the rest under them.
const ZONE_TWO = 0.7;
const ZONE_ONE = 0.55;
const TWO_HIGH_FT = 33;
const PRESSED_FT = 8;
const HALF_OUT = 0.45;
// Back into his zone at this pace (ft/s), this far back at least (feet) -
// settling into it over as much as this (feet), and turned and run there
// if it takes him any quicker than this (ft/s) over this long (ms) at least
// (out of a backpedal this long, ms).
const ZONE_FPS = 9;
const READ_STEPS_FT = 3;
const ZONE_SETTLE_FT = 3;
const BACK_MOST_FPS = 10;
const BAIL_LEAST_MS = 1000;
const BAIL_BACK_MS = 250;
// Sat in it, a step at a time this long (ms): toward the nearest man in
// it, no further off than ZONE_SEE_FT (feet), ZONE_LEAN of the way to him,
// no more than ZONE_REACH_FT from it - at no more than ZONE_DRIFT_FPS, and
// only when that is more than ZONE_STIR_FT from where he is - eased to a
// stop when it is not, with ZONE_SIT_MS sat there after (or over all the
// time there is, if no less than ZONE_EASE_LEAST_MS).
const ZONE_STEP_MS = 300;
const ZONE_SEE_FT = 50;
const ZONE_LEAN = 0.3;
const ZONE_REACH_FT = 8;
// (A cover man against the others at his position: that much more of the
// way to him, and further, at the most - or less.)
const ZONE_TIGHT = 0.5;
const ZONE_DRIFT_FPS = 6;
const ZONE_STIR_FT = 1.5;
const ZONE_SIT_MS = 300;
const ZONE_EASE_LEAST_MS = 200;
// At the mesh, the quarterback this far to the side of the back's way
// through it (feet): the two of them drawn the size they are (see body.ts),
// his hands out to the ball, and not into each other. (Turned to it as he
// goes there, side on, so he is facing it when the back gets there.)
const MESH_APART = 3;
// (In the gun, a back coming from the far side of him: the mesh this far
// (feet) up in front of the quarterback and this far across to the side
// it goes - the back's way across in front of him, not through him.)
const GUN_CROSS_UP = 3.2;
const GUN_CROSS_ALONG = 1;
// Under center, the mesh this far (feet) back of where the quarterback
// takes the snap, this far across to the side it goes - or, for a back
// lined up nearer than MESH_RUN_FT short of that (a fullback), out in
// front of him on his way: that far on from where he stands, no nearer
// the quarterback than MESH_QB_FT, and no further across from him than
// MESH_SIDE_FT - not stepped back out of the way for it, or sent across
// behind the quarterback to it.
const MESH_DEEP_FT = 9;
const MESH_SIDE_FT = 3;
const MESH_RUN_FT = 4;
const MESH_QB_FT = 3;
const underMesh = (qb: OPt, back: OPt, side: number): OPt => {
	const d = qb.d - MESH_DEEP_FT;
	const a = side * MESH_SIDE_FT;
	if (back.d + MESH_RUN_FT <= d) {
		return { d, a };
	}
	return {
		d: Math.min(back.d + MESH_RUN_FT, qb.d - MESH_QB_FT),
		a: Math.max(back.a - MESH_SIDE_FT, Math.min(back.a + MESH_SIDE_FT, a)),
	};
};
// (The quarterback's arms out with the ball from this long before the
// back gets to it, into him this far ahead of the middle of him.)
const HAND_OUT_MS = 200;
// (There with it, his arms out, this long before the back has it, ms.)
const HAND_SET_MS = 60;
// (The back's arms in to take it this long before it gets to him, and on
// round it so long after - see poses.ts TAKE_KEYS.)
const TAKE_BEFORE_MS = 450;
const TAKE_AFTER_MS = 250;
const TAKE_LEAD_MS = 60;
const MESH_AHEAD = 0.8;
// (Into his pass set, over no less than this long - off a fake on the
// run, a moment to pull up into it.)
const SET_LEAST_MS = 300;
// A lineman kick-sliding back into it, at this pace (ft/s).
const SET_FPS = 7;
// Out there to hand it off, or fake it: at no more than this pace (feet a
// second), and over no less than this long (ms).
const QB_STEP_FPS = 11;
const QB_STEP_LEAST_MS = 150;
const qbStepMs = (d: number) =>
	Math.max(QB_STEP_LEAST_MS, (d / QB_STEP_FPS) * 1000);
// A quarterback keeping it on a run (not a sneak): off a ride this often -
// held out in the back's belly this long (ms), the back on past it this far
// (feet) - and out round the edge, at least this far across (feet).
const QB_RIDE = 0.8;
const RIDE_MS = 450;
const RIDE_PAST = 12;
// (Short of the line of scrimmage by this much at least, feet.)
const RIDE_SHORT_FT = 2;
const RIDE_EDGE = 13;
// A run to the edge under center tossed to the back this often (a back
// lined up this deep at least, feet): taken on the move this far out to
// the side of where he lined up and this far up from it (feet), going at
// this much of his top speed. The quarterback opened up to him, the toss
// over this long (ms), let go halfway through - out of his hands this high
// (feet) and this far out in front of him - and in the air at this pace
// (ft/s), this far over the line between at the top.
const PITCH = 0.4;
const PITCH_DEEP = 15;
const PITCH_OUT = 8;
const PITCH_UP = 4;
const PITCH_PACE = 0.8;
const PITCH_MS = 440;
// (Opened up to him, a step back his way, no quicker than this, ft/s -
// pulled up into the toss, not slid there at a run and stopped dead.)
const PITCH_OPEN_FPS = 8;
const PITCH_FROM_Z = 3.6;
const PITCH_FROM_FT = 1.6;
const PITCH_FPS = 30;
const PITCH_PEAK = 1.3;
// (The quarterback off the other way after it, this far back and across,
// feet.)
const PITCH_BOOT = [5, 9] as const;
// The option out of the gun, this often on a run to the edge with the back
// lined up beside the quarterback on that side (no further off him than
// this, feet, either way): the quarterback down the line with it this far
// across and this far up (feet) at no more than this much of his top
// speed, the back out wide of him, this far further across and this far
// behind him (feet) as it is pitched - on the run, the quarterback's arms
// through it as he goes, and him eased up after it in no more than this
// (feet).
const OPTION = 0.25;
const OPTION_BESIDE = [8, 4] as const;
const OPTION_ALONG = 9;
const OPTION_UP = 4;
const OPTION_PACE = 0.6;
const OPTION_OUT = 9;
const OPTION_BEHIND = 2.5;
const OPTION_EASE_FT = 8;
// The end man on it at the quarterback this often (see optionEnd): one on
// the line - no further up the field than this (feet) - into him this long
// after he lets it go (ms, on his own count); off the snap a step up the
// field and in (feet) over this long (ms), there reading it no less than
// this (ms) - or straight at him -, and on after the ball this long after
// the hit (ms). The quarterback down this much longer (ms, on his count).
const OPTION_HIT = 0.6;
const OPTION_HIT_LINE = 4;
const OPTION_HIT_MS = [140, 300] as const;
const OPTION_SQUEEZE = [2.5, 1] as const;
const OPTION_SQUEEZE_MS = 450;
const OPTION_READ_LEAST_MS = 150;
const OPTION_HIT_SEE_MS = 700;
// (Down this much of a second on along the way he was going, at his pace -
// no further than this, feet.)
const OPTION_HIT_CARRY_S = 0.2;
const OPTION_HIT_CARRY_FT = 3;
const OPTION_DOWN_SPREAD_MS = 600;
// An inside zone run made to go one way and cut back the other this often;
// the back pressing it to this far behind the line and this far over that
// way first (feet).
const CUTBACK = 0.3;
const CUTBACK_PRESS = [5, 4] as const;
// A draw: out of the gun, inside, the back lined up beside the quarterback
// (no further up or back from him than this, feet) - this often on second
// or third and this far or more to go, and this often else. Made to look
// like a pass first: the line back into its sets this far (feet), the back
// up into his set beside the quarterback, the quarterback back this far
// (feet) and looking it over this long (ms, on his own count) - then
// handed off; the linebackers dropping back this far (feet) over this long
// (ms) before they come up to fill; the quarterback back on with his fake
// after it this far (feet); the rush into the line's sets at this share of
// the pace it fires into a run block at.
const DRAW_LONG = 0.5;
const DRAW = 0.2;
const DRAW_BESIDE = 2;
const DRAW_TO_GO = 7;
const DRAW_SET_FT = 2.5;
const DRAW_DROP_FT = 2;
const DRAW_LOOK_MS = [250, 450] as const;
const DRAW_LB_FT = 5;
const DRAW_LB_MS = 700;
// (No quicker than this (ms) over the least time his drop takes him.)
const DRAW_LB_SPARE_MS = 60;
const DRAW_FAKE_FT = 4;
const DRAW_HIT = 0.6;
// (The handoff this far up the field of where the quarterback lined up,
// and this far in from where the back did - feet.)
const DRAW_MESH_UP = 1;
const DRAW_MESH_IN = 1.8;
// A counter, this often of the power runs to a back lined up deep (this
// far behind the line at least, feet): his first step a jab the other way,
// this far (feet) over this long (ms), before he comes back to it - and the
// backside tackle pulled round behind the guard too, up through the hole
// onto the next man in it, at the hole this much after the guard (ms).
const COUNTER = 0.4;
const COUNTER_DEEP = 15;
const COUNTER_JAB_FT = 1.6;
const COUNTER_JAB_MS = 280;
const COUNTER_AFTER_MS = 200;
// (A man up front with nobody left this near to block him, feet: up the
// field this far after it, feet.)
const COUNTER_FREE_FT = 7;
const COUNTER_FREE_UP = 3.5;
// The line gives no ground nearer than this to where the quarterback sets
// (feet).
const POCKET_ROOM = 7;
// A back kept in to block sets up this far in front of where the
// quarterback sets and this far to his side of him - no nearer the ball
// than this (feet).
const BACK_SET_UP = 4.5;
const BACK_SET_OUT = 4;
// (A back set in the way of the man he picks up - see passRush - no nearer
// straight behind the quarterback than this; set up in front of him this
// much or more, as near as that.)
const BACK_LANE_LEAST = 2;
// (Or in the gap in the line the man he picks up comes through, no further
// out than this (feet) from the quarterback - out past the last man on the
// line this far - his way in crossing it this deep (feet).)
const BACK_GAP_MOST = 9;
const BACK_GAP_OUT = 2.5;
const BACK_GAP_LINE = 4;
const BACK_LANE_AHEAD = 2;
const BACK_OFF_LINE = 9;
// (Further off it than this (feet): up into it at a run, at no more than
// this much of his top speed, and set there this long before he takes
// anybody on.)
const BACK_STEP_FT = 5;
const BACK_UP_PACE = 0.6;
const BACK_SET_MS = 150;
// A blocker with nobody to block (see passRush): looking off to one side
// and the other, a step that way (this far out, feet), every so long (ms),
// the first this long after he is set - each step taking this long (ms),
// turned this far round (radians) - if he stands set no further than this
// (feet) all of it.
const SCAN_OUT_FT: [number, number] = [0.4, 0.9];
// A back kept in with nobody to block (see checkRelease): this share of
// them out into the flat, this long after the snap (ms), with this long
// at least till the line lets up (ms); round this far up and this far
// behind the last man on the line (or this far), where he is this long
// (ms) after it, this far outside him, out to this far
// past the line and this far outside him (feet), at this much of his top
// speed; sat there this long at least (ms), pulled up over this long (ms),
// nobody nearer the way than this (feet) - a man of his own set in the
// line, this.
const CHECK_OUT = 0.7;
const CHECK_MS = [1300, 1900] as const;
const CHECK_LEAST_MS = 900;
const CHECK_UP_FT = 2;
const CHECK_BEHIND_FT = [3, 6] as const;
const CHECK_NEAR_MS = 500;
const CHECK_WIDE_FT = 4;
const CHECK_FLAT_D = 9;
const CHECK_FLAT_OUT = 10;
const CHECK_PACE = 0.6;
const CHECK_SIT_MS = 500;
const CHECK_EASE_MS = 300;
const CHECK_CLEAR_FT = 2.5;
const CHECK_BY_OWN_FT = 1.8;
const SCAN_STEP_MS: [number, number] = [750, 1150];
const SCAN_FIRST_MS = 350;
const SCAN_GO_MS = 280;
const SCAN_LOOK = 0.4;
const SCAN_STILL_FT = 2;
// A play-action pass: this often, when it goes this far in the air (yards)
// and a back is there to fake it to; the fake this long (the ball out to
// him half way through), the back on past it this far (feet); and the
// linebackers drawn up this far by it (no quicker than this, ft/s: a
// step or two, not a lunge).
const PA_SHARE = 0.4;
const PA_AIR = 8;
const PA_MS = 400;
const PA_PAST = 5;
// (In the gun, across the quarterback's face to the fake and on past it
// downhill - on across as much as this for each foot up the field - not
// turned square up the field off it: if he goes out into a route after
// it, not if he stays in to block.)
const PA_ACROSS = 0.75;
// (Staying in to block, round off the fake from the way he came in to it:
// on into the line as far as he pulls up in at STOP_ACCEL * STOP_HARD - no
// less than PA_PAST - the curve pulled this share of that on past the
// mesh the way he came, and on into the line from there, in so many
// steps.)
const PA_ROUND = 0.8;
// (No further up than this (feet) off the ball, though: round along
// behind the line instead - as a back sets, see BACK_OFF_LINE.)
const PA_LINE_OFF = 9;
const PA_PACE_MS = 40;
const PA_ROUND_STEPS = 8;
const PA_BITE = 4;
const PA_BITE_FPS = 8;
// Off this many of the fakes under center, a bootleg: the quarterback away
// from the fake round the edge, this far out (feet), at this pace (feet a
// second), and the throw on the run - as soon as he is most of the way.
const BOOT = 0.4;
const BOOT_OUT = 30;
const BOOT_FPS = 16;
// (No time for the walk on after a throw on the run, under this long (ms):
// stood watching it - not a step of it in a blink.)
const ONRUN_WALK_LEAST_MS = 200;
// A pass he got a hand on: this often, the man who did waves it away -
// no catch - as he slows up. One off a receiver's hands: this often, his
// hands to his helmet.
const WAVE_OFF = 0.5;
const WAVE_MS = 1200;
const RUE = 0.45;
const RUE_MS = 1500;
// Anyone still on his way in once it is down: pulled up this long (ms,
// give or take) after he sees it hit the turf.
const DEAD_SEEN_MS = [250, 300] as const;
// A tackler gets to the carrier this long before he has him down, coming
// at him at no more than most of his speed - all but flat out, if he has
// to.
const HIT_MS = 280;
// Where he gets to: this far off him (feet) - or no further off than he
// is already, right there (not stepping off him to come back in at him),
// this near at the least.
const HIT_FT = 2.2;
const HIT_LEAST_FT = 1.2;
// (No nearer it than this to go: round to it where he stands.)
const TURN_IN_FT = 0.5;
// (In the carrier's way: no further off the line he runs than this, feet;
// given this much ground at most to meet him head on - see tackle - or up
// to GIVE_FAR_FT of it, if he has the time to backpedal it at GIVE_FPS.)
const IN_WAY_FT = 2.5;
const GIVE_MOST_FT = 8;
const GIVE_FAR_FT = 16;
const GIVE_FPS = 6;
const GIVE_COS = -0.5;
// (Off to his side, this much or less of the way he is faced: a slide.)
const SIDE_COS = 0.3;
// (Given ground with the time: pulled up this far (feet) short of where he
// meets him, set there this long (ms) at the least, and up into him off it.)
const GIVE_STEP_FT = 1.5;
const GIVE_SET_MS = 300;
// A tackler met him head on if he came at him from this near straight in
// front (the cosine of it); and then, this often, drove him back.
const HEAD_ON = 0.5;
const DRIVEN_BACK = 0.55;
// (How far back, feet, stood when he is met; and the pace, ft/s, at which
// he is met and goes down where he is, back no further.)
const BACK_FT = 1.4;
const BACK_EVEN_FPS = 10;
// (Stiff-armed, on into it no more than this, feet, going at the carrier -
// this much of the way he goes, or more: the arm keeps him off.)
const STIFF_ARM_CARRY_FT = 1;
const STIFF_ARM_AT = 0.7;
// Met head on and driven back, onto his back: how far on (feet) the way he
// was going - as far back as he was slow, `ft` stood, none at BACK_EVEN_FPS;
// quicker than that, carried on through it by as much as he was over it.
// Not stopped dead off a sprint and thrown back.
const drivenBy = (pace: number, ft: number): number =>
	pace < BACK_EVEN_FPS
		? -ft * (1 - pace / BACK_EVEN_FPS)
		: (pace - BACK_EVEN_FPS) * TACKLED_CARRY_S;
// Brought down off the run, carried on down by it: as far as this long at
// the pace he was going takes him (feet - TACKLED_LEAST at least, and no
// more than TACKLED_MOST) - see pace.ts carriedOf - not stopped in a
// stride and dropped where he is.
const TACKLED_CARRY_S = 0.15;
const TACKLED_LEAST = 2.5;
const TACKLED_MOST = 4.5;
// (Off a sprint: brought down from it no harder than this, ft/s/s - carried
// that much further on down, as much as TACKLED_FAST_MOST feet - not
// stopped short in a stride or two off full tilt. Not out over a sideline
// for it.)
const TACKLED_DECEL = 75;
const TACKLED_FAST_MOST = 7;
// A safety: brought down this far (feet) into his own end zone, and as
// much as this more - the ball still in it as he goes down forward with
// it, carried on (see TACKLED_MOST).
const SAFETY_IN = [6, 4] as const;
// Come at from the side (the sine of it at least this): this often swung
// round as he goes down, about this far (radians) - away from him, down
// the way he is thrown.
const SWING_SIDE = 0.7;
// (A tackler from behind him: the cosine of it under this.)
const BEHIND = -0.3;
const SWING = 0.4;
const SWING_TURN = 1.6;
// A run stopped behind the line: when (ms after the snap) the hole is
// filled, its blocks up into it, all the same.
const NO_HOLE_MS = 1600;
// (The man who swung him round down beside him, this far (feet) back
// toward his feet from his hips.)
const SWUNG_BACK = 1;
// (Beside a man lying the way `yaw` goes: +1 or -1 to his left, whichever
// is the far side of him from the cameras across the field.)
const farSide = (yaw: number) => (Math.cos(yaw) > 0 ? -1 : 1);
// One man in the secondary bringing him down from the side or behind: this
// often on his feet still after it, stood over him.
const UPRIGHT = 0.3;
// (Carried on through him that way off the pace he hits him at, over this
// long (ms) - no further than this, feet: as far as the man he hits, see
// TACKLED_MOST.)
const UPRIGHT_THROUGH_MS = 400;
const UPRIGHT_THROUGH_MOST = 4.5;
// (Eased down that way from as far off where he hits him as this, feet - a
// man further off still on his way in.)
const UPRIGHT_NEAR_FT = 2;
// Driving or fighting off a man with nobody of the other side this near:
// nobody there; on at a walk, under this pace (feet a second), a jog over.
const AIR_BLOCK = 5;
const WALK_UNDER = 6;
// (Or a pile this near, of either side, to shove in on - not a man flat
// on the turf.)
const AIR_PILE = 5.5;
const PILED = new Set<Move>(["pileIn", "overPile", "pileDive", "pileTop"]);
const FIRST_STEP = 150;
// (A long one with a man there for only some of it: on in it this long
// either side of him, ms, where at least this much of it is left over; a
// man there under this long, not at the start or the end of it, only going
// by.)
const AIR_AFTER_MS = 150;
const AIR_LEAST_MS = 500;
const AIR_PASS_MS = 400;
// A man on the line with the man he had off him this long before the end
// of the blocking: up out of his block this long after.
const FREED_MS = 600;
// (A man of the defense stood waiting this long at least, from this long
// after the snap and done this long before the whistle, ms: in his stance
// for it - see readyUp.)
const READY_LEAST_MS = 400;
const READY_FROM_MS = 300;
const READY_BEFORE_MS = 150;
const FREED_REACT = 200;
// (A man up front on a run let up off nobody (see letUp) no sooner than
// this after the snap: his first step in.)
const LET_UP_FROM_MS = 550;
// (And then after the ball at a jog (ft/s, slowest, quickest), kept this
// far off it (and up to this much more), ft - with this long (ms) at least
// to go.)
const FREED_FPS = [7, 10] as const;
const FREED_KEEP_FT = [9, 6] as const;
const FREED_AFTER_LEAST_MS = 700;
// (Without that long - his man off him a beat before the man with the
// ball is down, say - up out of it all the same, turned to the ball and a
// step or two toward it (ft, fewest, most), no nearer it than
// LOOK_AFTER_KEEP_FT, at no more than LOOK_AFTER_FPS, pulled up there
// LOOK_AFTER_SPARE_MS before the whistle: if there is LOOK_AFTER_LEAST_MS
// for it and nobody in his way (LOOK_AFTER_CLEAR_FT). If not, turned to
// it where he is, over LOOK_TURN_MS.)
const LOOK_AFTER_LEAST_MS = 500;
const LOOK_AFTER_FT = [2.5, 5] as const;
const LOOK_AFTER_KEEP_FT = 9;
const LOOK_AFTER_FPS = 7;
const LOOK_AFTER_SPARE_MS = 100;
const LOOK_AFTER_CLEAR_FT = 2.5;
const LOOK_TURN_MS = 350;
// The men who bring him down down no nearer one another than this.
const LANDED_APART = 2.4;
// Met head on, another man in on it by then (as late as this after, ms):
// this often stood up instead - held there and driven back this far (feet)
// over this long, the whistle on forward progress, him still on his feet.
const STOOD = 0.22;
// A sure tackler against the man he hits (his form, less the man's
// strength - see Flair; -1 to 1): this much likelier to drive him back,
// to stand him up, or - a man in the secondary - to stay on his feet.
const SURE_BY = { back: 0.25, stood: 0.12, upright: 0.15 };
const hitEdge = (tackler: Actor, carrier: Actor) =>
	clamp((tackler.flair.tackle ?? 0) - (carrier.flair.power ?? 0), -1, 1);
const STOOD_LATE = 350;
const STOOD_BACK = 2.6;
const STOOD_MS = 1100;
const STOOD_APART = 1;
// (Into them braced, at this, ft/s - pulled up to it at this, ft/s/s, and
// on into them over this long, ms.)
const STOOD_FPS = 10;
const STOOD_GATHER = 50;
const STOOD_PRESS_MS = 150;
// (In on him off a run this quick or more, ft/s: the last of it eased off
// over twice this far, feet.)
const STOOD_IN_LEAST = 8;
const STOOD_IN_FT = 1.2;
// (Further off where the drive takes him than this, feet, when he gets
// there: late to it, in at a run - see standUp.)
const STOOD_GAP_FT = 5;
// Not stood up and driven back where a man not in it is stood (or still
// going) by then, nearer than this (feet) to where it ends up - the man
// with the ball, or one of those driving him: on down instead.
const STOOD_CLEAR_FT = 2;
// A cover man into the blocker he meets (or a lead blocker into his man):
// down to this much of his pace as they meet (see breakDown).
const LOCK_KEEP = 0.5;
// (A blocker on his way to the man he locks up with, this near the spot
// (feet) and this near the time (ms): there.)
const LOCK_THERE_FT = 0.6;
// (Each this far off where they meet, feet, squared up.)
const LOCK_APART = 1.3;
const LOCK_THERE_MS = 60;
// (Gathered into a man off a run in too short to be pulled up at the pace
// asked for: up to this many times as hard - see gatherTo.)
const GATHER_HARD = 2;
// (And no stride of his way in at it turned off the next by more than
// this, radians.)
const GATHER_STRAIGHT = 0.35;
// (A blocker gathered into his man only on a block this long at least, ms
// - see blockOn.)
const GATHER_BLOCK_LEAST_MS = 500;
// More men in on a tackle: the first, and the second, there in time to (as
// late as this after it), this often.
// The ball out of his hands this long after the snap at the soonest (ms),
// and this much longer for each yard it goes in the air, give or take.
const TO_THROW = 1500;
const TO_THROW_YD = 45;
const TO_THROW_SPREAD = 500;
// To a man who sits down at the end of his route, as he turns for it: there
// no later than this after he is sat (but for the quarterback not set yet),
// give or take this much more.
const SETTLE_MOST_MS = 350;
const SETTLE_HOLD_MS = 250;
// To a man running on through it: no later than gets it to him with him
// slowed to this much of his pace for it (see runRoute).
const ROUTE_SLOW_MOST = 0.9;
// A screen: this often a short completion is one, caught behind the line
// (one that gains no more than this many yards), thrown this soon at the
// soonest; the receivers on his side out onto the men coming up at him,
// on them this long after the catch, give or take.
const SCREEN = 0.14;
const SCREEN_MOST = 12;
const SCREEN_THROW = 750;
const SCREEN_HOLD = 900;
// A back's screen: he waits this long (ms) as if to block, the quarterback
// drifting back this far (feet; in the gun), then out to the flat; the
// line, set a moment, lets the rush through and three of them go out in
// front of him; thrown this soon at the soonest.
const SLIP_WAIT = 750;
const SLIP_DROP = 18;
const SLIP_DROP_GUN = 9;
const SLIP_THROW = 1500;
// (The rush let through up this far (feet) short of him as he throws, there
// this long (ms) before it, and up at it then till this long after - carried
// on no more than this far with it.)
const SLIP_UP_FT = 5.5;
const SLIP_SWAT_LEAD = 200;
const SLIP_SWAT_MS = 300;
const SLIP_SWAT_ON = 2;
// (Out of his set to a man to block, with time to spare: at no less than
// this much of his pace.)
const LEAD_OUT = 0.5;
// Up for a ball at its highest, this often, thrown this deep at least
// (feet past the quarterback) with no more than this far to go after it,
// and a man on him (this near) or the end zone; how long he is up (ms), and
// how high it is caught (feet).
const LEAP = 0.6;
// (Sure hands, against the other receivers in the game: up for it, or laid
// out for it, that much more often at the most.)
const HANDS = 0.5;
const LEAP_DEEP = 36;
const LEAP_YAC = 18;
const CONTESTED = 6;
const LEAP_MS = 760;
const LEAP_Z = 8.4;
// (Down from it to go back the other way: slowing over this many legs.)
const LEAP_LAND_STEPS = 4;
// (Off from where he comes down further round than this - its cosine -
// from the way he went up: down slowing for it, to LEAP_TURN_FPS - and,
// back the other way, to a stop.)
const LEAP_ON_COS = 0.7;
const LEAP_TURN_FPS = 7;
// (A leap going further round than this from the quarterback - cos, ~100
// deg - faced the way he goes; see leapFor.)
const LEAP_AWAY_COS = -0.17;
// The man on him as it is caught - this near where it is caught (feet), no
// nearer, in his stride and with it no further behind him than this (the
// cosine) - a hand in at it, his own count of the time: from this long
// before it gets there till this long after (ms).
const CONTEST_FT = 5.5;
const CONTEST_LEAST_FT = 2;
const CONTEST_BEHIND = -0.2;
const CONTEST = 0.7;
const CONTEST_FROM_MS = 300;
const CONTEST_TO_MS = 300;
// Up for it with him (a ball he goes up for, see LEAP): off both feet over
// this long (ms), a beat (ms, give or take) either side of him; carried on
// toward it as far as this share of his pace takes him, this far at the
// most (feet), and no nearer the man going up for it than this (feet).
const CONTEST_UP_MS = 800;
const CONTEST_UP_SPREAD_MS = 60;
const CONTEST_UP_CARRY = 0.35;
const CONTEST_UP_MOST_FT = 4;
const CONTEST_UP_APART = 2.2;
// (On his feet: in his stride, or sat in his stance reading it.)
const ON_FEET = new Set<Move>([
	...GAITS,
	"backerStance",
	"backerReady",
	"safetyReady",
]);
// Carried on across the field after the catch as far as he goes in this
// long (s) at the pace he caught it at - the whole of it from this pace
// (ft/s) on.
const CATCH_DRIFT_S = 0.6;
const CATCH_DRIFT_FPS = 18;
// (So far back from that toward where he would have been brought down
// otherwise, each in turn, till the man who does it is no more than this
// late (ms) to him - or the least late.)
const CATCH_BACK = [0.25, 0.5, 0.75, 1];
// (Or first, going across quick enough - see CATCH_DRIFT_FPS - this much
// further on (feet).)
const CATCH_FURTHER_KEEP = 0.25;
const CATCH_FURTHER_FT = [8, 16, 24, 32];
const CATCH_LATE_MS = 350;
// Laid out for it, this often, thrown this deep at least - and where the
// sim has him down no further from where he would land than this (he goes
// this far laid out, feet); off his feet this long before it gets there,
// down this long after he went (ms) - and his hands, out ahead of him, as
// it does (his frame, feet).
const DIVE_CATCH = 0.3;
const DIVE_DEEP = 15;
const DIVE_FT = 5;
const DIVE_SHORT = 4;
const DIVE_BEFORE = 330;
const DIVE_CATCH_MS = 700;
const DIVE_AT = DIVE_BEFORE / DIVE_CATCH_MS;
const DIVE_HANDS = heldBall(
	footballBody(),
	poseOfMove("diveCatch", DIVE_AT),
	"hands",
).at;
// Fallen on a loose ball: down with his hand on it, out ahead of him (his
// frame, feet), from a run this far short of it.
const PRONE_HANDS = heldBall(
	footballBody(),
	poseOfMove("prone", 0),
	"carry",
).at;
const FALL_ON_FT = 4;
// (And no nearer him than he can go down on it from, plus this - feet.)
const FALL_ON_ROOM = 0.5;
// (Where he comes down, facing `yaw`, for it to be in that hand.)
const fallOn = (at: Pt, yaw: number): Pt => {
	const h = PRONE_HANDS;
	const c = Math.cos(yaw);
	const s = Math.sin(yaw);
	return { x: at.x - h.f * c - h.s * s, y: at.y - h.f * s + h.s * c };
};
// Out of bounds with next to nothing after the catch (feet downfield), on
// an out or a comeback, this often: thrown to him at the sideline, caught
// TOE_IN inside it with his toes dragged in - from this long before it gets
// there, over this long (ms).
const TOE_TAP = 0.6;
const TOE_YAC = 6;
const TOE_IN = 1.8;
const TOE_BEFORE = 250;
const TOE_MS = 900;
const TO_LINE = new Set<Route["name"]>(["out", "comeback", "backShoulder"]);
// Thrown to a man split out this far downfield at least (yards) - caught
// with this far to go after it at least (yards), or a touchdown, or over
// him - this often a slant-and-go (see routes.ts):
// the man on him in man coverage up on the jab in - from this long before
// the top of it, to this long after (ms) - at a spot this far down (feet)
// from where he was, and in past the jab this far; then turned and after
// him, up to speed at this (ft/s/s), this far behind him at best (feet). The
// quarterback's pump at the jab, the middle of it this long (ms) before the
// top of it - or as much as this long, to have this long (ms) at least
// after it before he winds up to throw.
const SLUGGO = 0.5;
const SLUGGO_YDS = 15;
const SLUGGO_YAC = 4;
const SLUGGO_BITE_BEFORE_MS = 300;
const SLUGGO_BITE_MS = 250;
const SLUGGO_BITE_DOWN = 4;
const SLUGGO_BITE_IN = 5;
const SLUGGO_TURN_ACCEL = 24;
const SLUGGO_TRAIL_FT = 3;
const SLUGGO_PUMP_LEAD_MS = 250;
const SLUGGO_PUMP_EARLY_MS = 500;
const SLUGGO_PUMP_GAP_MS = 150;
// The quarterback in the pocket: looking off another man first, this often
// with this long to spare at least; up a step (feet, ms), this often; a pump
// this often, and how long it takes.
const LOOK_OFF = 0.55;
const LOOK_OFF_MS = 900;
const STEP_UP = 0.4;
const STEP_UP_FT = 2.5;
const STEP_UP_MS = 420;
const PUMP = 0.12;
const PUMP_MS = 520;
// And with long enough still before he goes to the man it goes to: up to
// SCAN_MOST reads first, each this long (and up to this much more), at a
// man at least SCAN_APART (radians) off him and off the last, no more than
// SCAN_WIDE off straight downfield - with SCAN_LAST_MS left on him at
// least.
const SCAN_MOST = 2;
const SCAN_MS = [450, 250] as const;
const SCAN_APART = 0.4;
const SCAN_WIDE = 1.6;
const SCAN_LAST_MS = 650;
// The rush: working a blocker's shoulder, a step over (feet) this often
// (ms) - eased over and back, a step this long (ms) at a time, not swung
// like a pendulum; off the edge, on a tackle's outside shoulder (feet out,
// and in front of him), and round him by the end (feet behind).
const HAND_MS = 420;
const HAND_FT = 0.9;
const HAND_STEP_MS = 90;
// (Each step over on its own count - see countOf, from HAND_SALT: this
// much as long as HAND_MS, or as wide as his, and one in HAND_MID back
// to the middle of him - and no quicker across than HAND_MOST_FPS at the
// height of it: not swung over and back the one way like a metronome.)
const HAND_PER = [0.7, 1.35] as const;
const HAND_WIDE = [0.8, 1.2] as const;
const HAND_MID = 0.12;
const HAND_MOST_FPS = 5.5;
const HAND_SALT = 1000;
// (The blocker on him over with him this much of each step, his feet
// working with him - not stood in his set while the rusher works.)
const HAND_WITH = 0.75;
// (A rusher better at it than the blocker on him is at his job - each
// against the others at their positions: working him this much wider and
// quicker, and the blocker giving this much more ground, at the most. Or
// less, the other way round.)
const RUSH_WIDER = 0.45;
const RUSH_QUICKER = 0.2;
const RUSH_GIVE = 0.4;
// (A second rusher on a blocker already locked up with one: this far over
// on his shoulder (feet, across the field), working it this share as much.)
const DOUBLE_RUSH_FT = 2.8;
const DOUBLE_RUSH_SWAY = 0.3;
const EDGE_OUT = 1.7;
const EDGE_ON = 1.6;
const EDGE_PAST = 1.8;
// (The blocker giving ground with him, as he rides him past: this much
// (feet) at the most, no quicker than this, ft/s.)
const EDGE_GIVE = 2.5;
const EDGE_GIVE_FPS = 1.2;
// (Up the arc onto him bent through so many points.)
const EDGE_ARC_PTS = 6;
// (A rusher from off the ball onto a back kept in, straight at him through
// the men on the line: in through the gap this far (feet) to one side or
// the other of the worst of it, the nearest that has him clear - see
// Action.rushGap.)
const RUSH_GAP_OFF = [2, -2, 3.5, -3.5, 5, -5] as const;
// A twist this often on a pass play with this long to rush (ms): an end and
// the man inside him next on the line (no further apart than this, feet -
// and their blockers no further apart than this, side by side) swapping
// blockers - the inside man slanting out onto the end's, the end
// a hard step up the field (feet) and round under him onto the man he
// left: stepping across (feet) for at least this long (ms), off once the
// other is this long short of getting onto his - and round behind him by
// this far (feet).
const TWIST = 0.18;
const TWIST_LEAST_MS = 2400;
const TWIST_APART = 11;
const TWIST_BLOCKERS_APART = 6;
const TWIST_JAB_FT = 2;
const TWIST_ACROSS_MS = 120;
const TWIST_SLIDE_FT = 1.4;
const TWIST_LEAD_MS = 300;
const TWIST_CLEAR_FT = 3.5;
// (And never nearer him than this on the way round, feet: further round
// behind him by as much more as it takes of these, if he would be.)
const TWIST_ROOM_FT = 3;
const TWIST_WIDER_FT = [0, 1.5, 3];
// Off the ball into the man across from him: up to speed at GET_OFF_ACCEL
// ft/s/s, no quicker than RUSH_HIT_PACE of his top speed by the time he
// gets there - not at full tilt from his first step.
const GET_OFF_ACCEL = 28;
const RUSH_HIT_PACE = 0.7;
// (Into the man on him no quicker than this, ft/s - slowed into him at
// this, ft/s/s, over the last of his run at him: see Actor.hitInto.)
const HIT_FPS = 7;
const HIT_ACCEL = 40;
// (A man on the run a blocker gets to - see blockOn: pulled up to this,
// ft/s, by then, at HIT_ACCEL.)
const BRAKE_FPS = 7;
const BRAKE_MOST_MS = 600;
// (Short of time for that: into him this many times quicker, slowed this
// many times harder - the first of them that does it.)
const HIT_SHORT: [number, number][] = [
	[1, 1],
	[1.3, 1.5],
	[1.6, 1.5],
];
// (Or there as much as this later (ms), as he would be at it - not into him
// harder for the want of a moment: that blocker on his way from him, say.)
const HIT_LATE_MS = 20;
// (His hands on him at that, and the rest of the way in to him over this
// long, ms, the pace going out of him - in steps this long.)
const HIT_PRESS_MS = 150;
const HIT_PRESS_STEP_MS = 50;
// (How long it takes him from still, that far (feet): see Actor.hitInto.)
const hitMs = (far: number, top: number): number => {
	const A = GET_OFF_ACCEL;
	const Ad = HIT_ACCEL;
	const ve = HIT_FPS;
	if (Math.sqrt(2 * A * far) <= ve) {
		return Math.sqrt((2 * far) / A) * 1000;
	}
	const qa = 1 / (2 * A) + 1 / (2 * Ad);
	const vc = Math.min(top, Math.sqrt((far + (ve * ve) / (2 * Ad)) / qa));
	if (vc <= ve) {
		return rampedMs(far, top, 0, A);
	}
	const up = (vc * vc) / (2 * A);
	const down = (vc * vc - ve * ve) / (2 * Ad);
	return (vc / A + (vc - ve) / Ad + Math.max(0, far - up - down) / vc) * 1000;
};
// (Square to the line over his first steps, coming up out of his stance -
// turned the way he goes once he is up.)
const GET_OFF_SQUARE_MS = 200;
// (The blocker knocked back by him, feet for each ft/s he hits him at - no
// more than JOLT_MOST - over JOLT_MS.)
const JOLT_PER_FPS = 0.06;
// (The blocker turned to face him, no further than this off straight
// ahead, radians.)
const BLOCK_TURN = 1;
// A lineman with nobody to block helping on a man no further along the
// line than this (feet): in a moment after the rusher gets there (ms, and
// up to this much more, man by man), no quicker than this (ft/s) - at the
// shoulder of his own man, this far out and back.
const HELP_A = 7;
const HELP_REACT_MS = 250;
const HELP_REACT_SPREAD = 200;
const HELP_ON_FPS = 6;
const HELP_OUT = 2.4;
const HELP_BACK = 0.5;
// (No further than this off his spot by the man he helps, feet, as he goes
// - not back across the pocket, round or through the rusher, to a back
// picking one up deep in it.)
const HELP_FAR_FT = 6;
const JOLT_MOST = 1.1;
const JOLT_MS = 250;
// (A rusher met by a blocker still on the move - a back through a fake:
// the two of them at the pace between them this long after, ms.)
const CONTACT_MS = 80;
const GANG = [0.5, 0.35];
const GANG_LATE = 250;
// (Down on it no later than this after he is off for it - ms; DIVE_LEAST_MS
// and a short run in take ~450-700.)
const GANG_DIVE_MOST = 1000;
// (Down round a loose ball off the others down on it: this far apart at
// least (feet), as little way round from where he comes in as does it.)
const PILE_ON_APART = 1.6;
// (A man down on it already this near it, feet.)
const PILE_ON_NEAR = 3;
const PILE_ON_ROUND = [0, 0.5, -0.5, 1, -1, 1.5, -1.5, 2, -2];
// (Round the pile from a man down there already by no more than this,
// radians, tried in turn.)
const GANG_ROUND = [0, 0.4, -0.4, 0.8, -0.8, 1.2, -1.2];
// A sneak into the A gap, on one side of the center or the other, this
// far over (feet) - not straight through him - give or take half this
// much (feet).
const SNEAK_GAP_A = 2.3;
const SNEAK_SPREAD = 1;
// (Driving on into it no quicker than this over all of it, ft/s.)
const SNEAK_FPS = 9;
// (In it by the time he is up level with the ball, feet.)
const SNEAK_GAP_D = -0.5;
// A short run up the middle, this often a pile on him (one or less yards,
// or a sneak, this often, and the most men in it; up to this many yards,
// and this many men).
const PILE_SHORT = 0.85;
const PILE = 0.5;
const PILE_YDS = 2;
const PILE_SMALL = 3;
// Who is in it: the big men in the scrum this near where he goes down -
// each this likely, nearest first - in this long after he is hit (and each
// next one this much later), going down over this long; on their knees
// this far off the middle of it (and up to this much more), each with
// this much room (feet) - in from where he comes, or the nearest way round
// from it (radians) that has it.
const PILERS = new Set<Role>([
	"C",
	"LG",
	"RG",
	"LT",
	"RT",
	"TE",
	"TE2",
	"FB",
	"DE",
	"DT",
	"NT",
	"SAM",
	"MIKE",
	"WILL",
	"BUCK",
]);
const PILE_NEAR = 11;
const PILE_IN = [0.95, 0.85, 0.7, 0.55, 0.4];
const PILE_FIRST = 120;
const PILE_GO = 130;
const PILE_FALL_MS = 550;
// (In past his place on it by more than this (feet): back out to it first,
// to this far short of it, over this long at the least (ms), at no more
// than this (ft/s).)
const PILE_BACK_LEAST = 0.4;
const PILE_BACK_IN = 0.5;
const PILE_BACK_MS = 300;
const PILE_BACK_FPS = 5;
const PILE_OFF = 2.5;
const PILE_OFF_MORE = 1.1;
const PILE_ROOM = 1.8;
const PILE_TURNS = [
	0,
	0.3,
	-0.3,
	0.6,
	-0.6,
	0.9,
	-0.9,
	1.2,
	-1.2,
	1.6,
	-1.6,
	2,
	-2,
	2.5,
	-2.5,
	Math.PI,
];
// (From further off than this, a run in first.) Over the top of it, the
// most of them, from the third man in on, each this likely - his middle
// this far off the middle of it, and PILE_ROOM off the other man over it,
// his way in no nearer than PILE_PAST to the spot of any other in on it.
const PILE_REACH = 6;
const PILE_TOPS = 2;
const PILE_OVER = 0.6;
const PILE_TOP_OFF = 0.7;
const PILE_PAST = 1.4;
const PILE_TOP_WAIT = 500;
// (Off the top of it back the way he came over it, this much further out,
// feet - not stood up on the man under it - or round from that, radians.)
const OFF_TOP_FT = 2.4;
const OFF_TOP_TURNS = [0, 0.6, -0.6, 1.2, -1.2, 1.8, -1.8];
// In from right there: over the top this often - off his feet this far
// short of the goal line (once he has had it this long), laid out this far
// over it (and up to this much more), at this pace, over this long; on top
// of it this long.
const OVER_TOP = 0.7;
const TOP_FROM = 6;
const TOP_SET = 350;
const TOP_PAST = 1.5;
const TOP_PAST_MORE = 1.5;
const TOP_FPS = 16;
const TOP_MS = [450, 800] as const;
const TOP_STAY = 700;
// The rest near it then (this long after the hit), up to this many: in on
// it this long after, this far off the middle (and up to this much more),
// got there over this long; off it up to this long before the first man is
// up off it, this far back.
const PUSH_SEEN = 600;
const PUSH_NEAR = 9;
const PUSH_MOST = 3;
const PUSH_GO = 350;
const PUSH_AT = 4.6;
const PUSH_AT_MORE = 0.8;
const PUSH_IN_MS = 600;
const PUSH_LEAVE = 500;
const PUSH_BACK = 2.5;
// Off it: the first this long after the hit, each next one this much
// later, each this long getting up - the men under it then, him last.
const PILE_HOLD = 1500;
const PILE_PEEL = 330;
const OFF_PILE_MS = 900;
const PILE_UNDER = 350;
// A carrier making a man miss, this often on a stretch this long (feet, and
// ms) at least; a stiff arm, this often when the man comes at his free arm.
// The cut: this long, this far on and over (feet).
const EVADE = 0.5;
const EVADE_FAR = 30;
const EVADE_MS = 1200;
// (Or, a quick one, this often on a stretch this long at least.)
const EVADE_SHORT = 0.35;
const EVADE_SHORT_FAR = 20;
const EVADE_SHORT_MS = 800;
// A kick return reversed (see returnRun) this often, one of this many feet
// or more up the field: off up the field this far (feet) and this far
// across (feet; kept this far in from the sideline) at this share of his
// speed - away from where it ends, this far or more across from it - then
// round up the field this far (feet) and back in this far (feet), and back
// across, up the field this much more (feet), to this share of the way back
// from where it ends to there.
const REVERSE = 0.12;
const REVERSE_RUN = 60;
const REVERSE_UP = 15;
const REVERSE_OUT = 28;
const REVERSE_EDGE = 14;
const REVERSE_PACE = 0.75;
const REVERSE_ROUND = [8, 5] as const;
const REVERSE_ACROSS = 26;
const REVERSE_BACK_UP = 8;
const REVERSE_BACK_SHORT = 0.3;
// (A return still getting up to speed: its steps up taken as one stretch,
// none turned more than this off the last of them, radians.)
const EVADE_RAMP_TURN = 0.2;
// (And a man at his side, as far round as this - the cosine off straight
// ahead of him - grabbing at him: spun out of.)
const EVADE_RAMP_BEHIND = -0.75;
// (Met no sooner than this much of the way along it, and with this much of
// it after, feet.)
const EVADE_FIRST = 0.25;
const EVADE_AFTER_FT = 12;
const STIFF_ARM = 0.5;
// (Strong, against the others at his position: an arm out that much more
// often at the most.)
const STRONG = 0.5;
const JUKE_MS = 300;
const JUKE_ON = 2;
const JUKE_OVER = 5;
// (All of it going into it at JUKE_FULL_FPS or more - his pace over the
// last JUKE_INTO_MS - and less slower, down to JUKE_LEAST of it.)
const JUKE_FULL_FPS = 14;
const JUKE_INTO_MS = 150;
const JUKE_LEAST = 0.45;
// Instead, now and then, a hurdle (him coming from no wider than this off
// straight ahead: the cosine), this long; or a spin, this long, this far on
// and over.
const HURDLE = 0.12;
const HURDLE_AHEAD = 0.6;
const HURDLE_MS = 600;
const SPIN = 0.22;
const SPIN_MS = 480;
const SPIN_ON = 2.5;
const SPIN_OVER = 2.5;
// (Into it no quicker than this, ft/s: slowed to it over his last strides,
// at CUT_ACCEL.)
const SPIN_INTO_FPS = 12;
// By his ratings (see ratings.ts), where the game has them: as often as
// this times as everybody else to make a man miss, the least elusive, and
// this much more often the most - and over or round a man that much more,
// from this times as often.
const EVADE_LEAST = 0.4;
const EVADE_MORE = 1.2;
const FANCY_LEAST = 0.5;
const evasive = (m: Actor) => {
	const lvl = elusiveness(m.rated);
	return lvl === undefined ? 1 : EVADE_LEAST + EVADE_MORE * lvl;
};
// ...and by the man he goes past: a sure tackler (see Flair) this much less
// often made to miss, a poor one this much more - as often as ever, over
// them all. (A game without the ratings, as it always was.)
const MISSED_BY = 0.5;
const MISSED_MOST = 1 + MISSED_BY;
const missable = (m: Actor) => 1 - MISSED_BY * (m.flair.tackle ?? 0);
// How much later than he would have been a carrier can be brought down, his
// run slowed, for the man the sim says made the tackle to get there.
const SLACK_MS = 700;
// After a flag before the snap, the men who walk off a few steps: no
// quicker than this, ft/s.
const DEAD_WALK_FPS = 4;
// How far (feet) the man who jumps early before the snap goes, for the
// foul: out of his stance, or over the line.
const JUMP: Record<string, number> = {
	"False start": 1.5,
	"Illegal motion": 1.5,
	"Illegal shift": 1,
	"Neutral zone infraction": 3,
	Encroachment: 3.5,
};
// Offsides, the play run (see offside): the man flagged out of his stance
// this long before the snap (ms), up to this much sooner, a step this far
// (ft) across toward the ball - no further off it than this (ft), set there
// this long at least - the step let go of as he goes on this far (ft), no
// later than this long after he would have gone (ms), or near enough (this
// share of it left, let go of in his next stride).
const OFFSIDE_MS = 170;
const OFFSIDE_SPREAD_MS = 130;
const OFFSIDE_FT = 1;
const OFFSIDE_NEAR = 4;
const OFFSIDE_SET_MS = 300;
const OFFSIDE_FADE_FT = 2;
const OFFSIDE_FADE_MS = 900;
const OFFSIDE_LEFT = 0.1;
// Unsportsmanlike before the snap (see jaw): at a man of the other side no
// further off than this (ft), over to him at this pace (ft/s; no quicker
// than this, ms; off no sooner than this before the snap), up in his face this far from him (where he is, if no
// further off than this more), there this long before the shove, the shove
// this long before the flag - his arms in it this long either side of it;
// the other man up out of his stance over this long, shoved back this far
// (ft) over this long. Nobody over the ball, or at the kick.
const JAW_NOT = new Set(["C", "QB", "LS", "H", "K", "P"]);
const JAW_FAR = 30;
const JAW_FPS = 7;
const JAW_LEAST_MS = 500;
const JAW_EARLY_MS = 4000;
const JAW_FACE_FT = 2.3;
const JAW_STAY_FT = 1;
const JAW_SET_MS = 300;
const JAW_SHOVE_MS = 350;
const JAW_SHOVE_ARMS_MS = 200;
const JAW_UP_MS = 350;
const JAW_BACK_FT = 1.3;
const JAW_BACK_MS = 350;
// Picked off: the rest of his side after it with him, off once they are
// through with what they were doing (with this long to go at least, ms),
// at this much of their top speed - to round where it ends up, this far
// off (feet, and up to this much more: see countOf).
const WITH_IT_LEAST_MS = 400;
const WITH_IT_PACE = 0.7;
const WITH_IT_FT = [9, 8] as const;
const WITH_IT_APART = 4;
// The ball out of his hands this long after the hit that knocks it loose;
// and what a man is doing on the turf, down.
const POP_MS = 120;
// Everybody else after a loose ball (see gather): a beat after it is out
// (ms, and up to this much more), at this much of his top speed (and up to
// this much more), round it this far off (feet, and up to this much more),
// a little off the straight line in (radians across) - or, nearer than
// that already, in to this much as near (and no nearer than this, feet);
// not with less than this long left to go (ms). Fallen on, stood round the
// pile this long.
const GATHER_REACT_MS = 300;
const GATHER_REACT_SPREAD = 300;
const GATHER_PACE = 0.6;
const GATHER_PACE_SPREAD = 0.2;
const GATHER_RING = 7;
const GATHER_SPREAD = 10;
const GATHER_TURN = 0.7;
const GATHER_IN = 0.6;
const GATHER_NEAREST = 3.5;
// (No nearer another's place than this, feet: round the ring from his own
// way in to it this far, feet, as far as it takes.)
const GATHER_APART = 3;
const GATHER_ROUND = [0, 2, -2, 4, -4, 6, -6, 8, -8];
// (And off a man down, this far from the length of him, feet - from when
// he gets there to this much after, ms.)
const GATHER_DOWN_CLEAR = 2.5;
const GATHER_DOWN_STAY_MS = 800;
const GATHER_LEAST_MS = 400;
const GATHER_FALLEN_MS = 1400;
// Up off the turf with the play still going (see upAgain): back in after it
// a beat after he is up (ms, soonest and latest - see countOf), at this
// much of his top speed, to round where it ends up this far off (feet, and
// up to this much more). Not with less than this long to go (ms).
const UP_AGAIN_REACT_MS = [250, 550] as const;
const UP_AGAIN_PACE = 0.6;
const UP_AGAIN_FT = [8, 7] as const;
const UP_AGAIN_LEAST_MS = 1200;
// (Further out than that by this much, feet, if there is no room there.)
const UP_AGAIN_OUT_FT = [0, 4, 8] as const;
// (Up again with the ball still going (see upAgain): with it, not off to
// its last spot, if he would be there this long (ms) before it is - in it
// once it is no further from there than SETTLED_FT.)
const UP_AGAIN_EARLY_MS = 1000;
const SETTLED_FT = 4;
// A loose ball batted about: no nearer than this to the spot he dives from
// (feet), he is laid out for it from where he is, not stepping in first.
const BAT_RUN_LEAST_FT = 1.5;
// Batted on by the first of this many men nearest it to get there (see
// BAT_IN) - his hands onto it at the end of his dive (his frame, feet); no
// nearer it than his hands are out ahead of him and a step; at it no later
// than this after it came loose, and this long before it stops (ms).
const BAT_FROM = 4;
const BAT_HANDS = heldBall(footballBody(), poseOfMove("dive", 1), "hands").at;
const BAT_ROOM = BAT_HANDS.f + 1;
const BAT_LATEST_MS = 1500;
const BAT_ON_MS = 400;
// And down after it this long before he is getting up again (ms) - where
// nobody else comes this near him (feet) till he is half up.
const BAT_DOWN_MS = 450;
const BAT_LIE_CLEAR = 1.4;
// Where it is batted: out from where it came loose a third of the way to
// where it ends up, or turned off that this far (radians) - or, none of
// those, so far out (feet) from where it came loose, round from the way to
// where it ends up so far - the first way to it and on from it that keeps
// this far (feet) from everybody it goes by (the length of a man down on
// the turf, LIE_BACK behind his hips to LIE_HEAD ahead - see apart.ts),
// every BAT_EVERY_MS of the way; or the clearest. And the men coming at it
// to bat it: not from behind it, through them to their hands, nor it
// knocked on back into them (cosines).
const BAT_TURNS = [0, 0.4, -0.4, 0.8, -0.8, 1.2, -1.2];
const BAT_RING = [3, 5];
const BAT_RING_TURNS = [
	0,
	0.5,
	-0.5,
	1,
	-1,
	1.5,
	-1.5,
	2,
	-2,
	2.5,
	-2.5,
	Math.PI,
];
const BAT_CLEAR = 1.6;
const BAT_EVERY_MS = 60;
const LIE_BACK = 0.6;
const LIE_HEAD = 3;
const BAT_IN = 0.5;
const BAT_BACK = -0.5;
// Batted on again, out of one man's hands to the next of them, up to this
// many times in all - while it has this long (ms) still to go before it
// stops: the next man off after it this long after it squirts out (ms),
// his hands on it no sooner than this after the last man's (nor before
// its time still to go is shared out among those still to come), nor
// later than this; it squirts out at this pace (ft/s). And on from the
// last of them to where it stops: over and over in a roll this long (ms,
// and this much longer a foot it goes, up to this long), and lying there
// then.
const BAT_MOST = 4;
const BAT_AGAIN_MS = 1400;
const BAT_REACT_MS = 250;
const BAT_GAP_MS = 650;
const BAT_GAP_MOST_MS = 1500;
const BAT_SQUIRT_FPS = 9;
const BAT_ROLL_MS = [600, 90, 1200] as const;
// Down on the turf, the length of him along the way he faces - or, on his
// back, the other way.
const LIE_FACING = new Set<Move>(["prone", "dive", "tackled", "hurtProne"]);
const LIE_BACKWARD = new Set<Move>([
	"supine",
	"tackledBack",
	"hurt",
	"getUpBack",
]);
// Getting up off the turf.
const RISING = new Set<Move>(["getUp", "getUpBack", "sitUp"]);
const DOWN = new Set<Move>([
	"tackled",
	"tackledBack",
	"dive",
	"prone",
	"supine",
	"getUp",
	"getUpBack",
	"qbSlide",
	"slid",
	"sitUp",
	"pileIn",
	"overPile",
	"offPile",
	"pileDive",
	"pileTop",
	"offTop",
]);
const CLOSE = 0.85;
// A man this near a receiver as he catches it (feet), who is not the one to
// bring him down, made to miss there: with this far (feet) after the catch
// to go, this long (ms) after he is off with it - or caught in stride - and
// this long of it left after.
const CATCH_BEAT_FT = 7.5;
const CATCH_BEAT_YAC = 10;
const CATCH_BEAT_MS = 250;
const CATCH_BEAT_STRIDE_MS = 200;
const CATCH_BEAT_LEFT_MS = 500;
const CLOSE_SLIVER_MS = 150;
// (A gait no longer than this between two others, ms: see noFlicks.)
const FLICK_MS = 150;
// (One that size from stood to stood: a step, this long - his stand either
// side of it left no shorter than this.)
const STEP_MS = 320;
const STEP_LEFT_MS = 60;
// A man on his feet on his way over a man down - nearer the length of him
// than DOWN_CLEAR (feet; see LIE_BACK) - bent out round him (see
// offMenDown): off to the side by the least of DOWN_OFFS (feet) that
// clears him, eased out over DOWN_TAPER_FT of his way (and no less than
// DOWN_RAMP_MS), across no quicker than DOWN_ACROSS of his pace or
// DOWN_ACROSS_FPS - at a pinch, half as quick again. Down: on the turf, or
// on his way down from DOWN_FROM through it.
const DOWN_CLEAR = 1.6;
const DOWN_OFFS = [1, 1.5, 2, 2.5, 3, 3.5, 4.5, 5.5, 7, 8.5];
const DOWN_TAPER_FT = 5;
const DOWN_RAMP_MS = 300;
const DOWN_ACROSS = 0.8;
const DOWN_ACROSS_FPS = 10;
const DOWN_PINCH = 1.5;
// (And no quicker than this on his way for it, all told, ft/s.)
const DOWN_TOP_FPS = 31;
const DOWN_ON = new Set<Move>(["prone", "hurtProne", "supine", "hurt"]);
// (Down there on the turf, the length of his legs behind his hips too,
// feet - not stood on - till he is getting up.)
const DOWN_LEGS = 3.2;
const DOWN_GOING = new Set<Move>(["tackled", "dive", "tackledBack"]);
const DOWN_FROM = 0.6;
// (Or getting up off it, until this far through.)
const DOWN_RISING = new Set<Move>(["getUp", "getUpBack"]);
const DOWN_RISING_TILL = 0.5;
// (In on a pile, down on his knees over it, or laid over the top of it -
// from DOWN_FROM through going in on it, till half way up off it - the
// length of his legs this far behind his hips, feet.)
const PILE_GOING = new Set<Move>(["pileIn", "pileDive"]);
const PILE_DOWN = new Set<Move>(["overPile", "pileTop"]);
const PILE_RISING = new Set<Move>(["offPile", "offTop"]);
const PILE_LEGS = 1.5;
// (Looked at this often, ms; bent in steps this long; two spells of it
// this near each other gone round as one. Slower than DOWN_PACE_LEAST
// ft/s, he is hardly going anywhere - and nowhere clear, round him only
// if it is this much (feet) the clearer.)
const DOWN_EVERY_MS = 50;
// (Stood this long at least (ms) on his way past a man down: a stop.)
const DOWN_PAUSE_MS = 150;
const DOWN_STEP_MS = 100;
const DOWN_SLIVER_MS = 40;
const DOWN_JOIN_MS = 500;
const DOWN_PACE_LEAST = 1.5;
const DOWN_GAIN = 0.5;
// (Clear enough, feet - if clear of him takes more than this much further
// out, feet. Nowhere that clear, no nearer than DOWN_SOME.)
const DOWN_FAIR = 1.2;
const DOWN_FAIR_MORE = 0.5;
const DOWN_SOME = 1;
// (The way he is going there: from where he is this long before it (ms, on
// the move) to this long after - so far at least, feet.)
const DOWN_WAY_MS = 150;
const DOWN_WAY_LEAST = 0.5;
const ROUNDS = new Set<Move>([...GAITS, "shuffle", "back"]);
// Two of a side blocking, or taking on a block, in the one spot - nearer
// than TWIN_APART (feet), two men drawn in each other - spread shoulder to
// shoulder (see noTwins), no more than TWIN_MOST each (feet): eased apart
// over TWIN_EASE_MS either side of it where he is free to be (no less than
// TWIN_EASE_LEAST, in it); looked at every TWIN_EVERY_MS, spells of it
// this near each other (ms) as one.
const TWIN_APART = 1.9;
const TWIN_MOST = 1.2;
const TWIN_EASE_MS = 250;
const TWIN_EASE_LEAST = 150;
// (Back over no quicker than this, ft/s, as he comes to a stand.)
const TWIN_BACK_FPS = 5;
// (Eased over going flat out: no quicker than this, all told, ft/s - or
// no more than TWIN_EASE_FPS over, if he is that quick already.)
const TWIN_TOP_FPS = 30;
const TWIN_EASE_FPS = 2;
// (On the move, this quick or more (ft/s): eased over no harder than this,
// ft/s/s, at the most - see noTwins.)
const TWIN_RUN_FPS = 3;
const TWIN_RUN_ACCEL = 25;
const TWIN_EVERY_MS = 50;
const TWIN_JOIN_MS = 300;
// (Stood, and moved this quick or more by it, ft/s: stepped.)
const TWIN_STEP_FPS = 2;
// (Shorter than this (ms), a piece of one of those steps goes as the step
// next to it does.)
const TWIN_SLIVER_MS = 150;
// One of his own side nearer the man with the ball than this (feet): eased
// off his way, no more than this (feet) - not this long (ms) after he has
// it, nor before it leaves him (or he goes down).
const CARRY_APART = 1.8;
const CARRY_CLEAR_MOST = 1.5;
const CARRY_CLEAR_SKIP_MS = 350;
// (Of two of different sides, one facing the other - more than this, the
// cosine of the angle off his facing - as good as in a block with him.)
const TWIN_FACING = 0.7;
// (Two in a block with each other, nearer than this, feet: in each other -
// eased back off each other, no more than TWIN_MOST each.)
const CLASH_APART = 1.6;
const LINE_PLAY = new Set<Move>(["engage", "drive"]);
// (Up against a man, driving him, feet.)
const DRIVE_ON = 3.5;
// (Nor bent round him into a man on his feet: nearer than this, feet, and
// this much nearer than he was to come.)
const PASS_APART = 1.8;
const PASS_SLACK = 0.1;
// Stood on a man down to begin with: off him over this long (ms) of it, if
// there is this much more of it (ms) left after - or all of it, this long
// at least - no quicker than this (ft/s), away from the middle of him or
// round from that.
const STEP_OFF_MS = 500;
const STEP_OFF_LEFT_MS = 400;
const STEP_OFF_LEAST_MS = 300;
const STEP_OFF_FPS = 5;
const STEP_OFF_TURNS = [
	0,
	Math.PI / 4,
	-Math.PI / 4,
	Math.PI / 2,
	-Math.PI / 2,
];
// (How far out of his way he is at t, 0 to 1: out by ta from wa, round
// till tb, back in by wb.)
const bumpOf = (t: number, wa: number, ta: number, tb: number, wb: number) => {
	const u = clamp(
		t < ta
			? (t - wa) / Math.max(1, ta - wa)
			: t > tb && wb !== Infinity
				? (wb - t) / Math.max(1, wb - tb)
				: 1,
		0,
		1,
	);
	return u * u * (3 - 2 * u);
};
const FLAT_OUT = 0.97;
// A receiver down his route, at this much of his top speed - sat down at
// the end of it for the ball, up for it this long before it comes.
const ROUTE_PACE = 0.76;
const CATCH_LEAD_MS = 380;
// (Sat down waiting on it, this often a step or two up into it as it
// comes, this far, feet, over the last this long, ms, of its way to him -
// sat this long first, ms, and the step no shorter than this, ms (shorter
// in proportion): not with a man this near him on the way there, though,
// feet - on him, or breaking on it in front of him.)
const STEP_IN = 0.5;
const STEP_IN_FT = [1.2, 2.4] as const;
const STEP_IN_MS = 450;
const STEP_IN_SAT_MS = 120;
const STEP_IN_LEAST_MS = 300;
const STEP_IN_CLEAR_FT = 3.5;
// Thrown to, slowed to no less than this much of that pace to come open on
// time (see runRoute).
const ROUTE_PACE_LEAST = 0.6;
// A route laid on past the back of the end zone (a decoy's, deep in the
// red zone): run no deeper than this short of the end line (feet), and
// sat down there - not into the line and slid along it. Nor run on
// nearer a sideline than ROUTE_SIDE_FT: sat down short of it, not run into
// it flat out and turned along it in a stride.
const ROUTE_BACK_FT = 4;
const ROUTE_SIDE_FT = 3;
// (With no more than this left of it to the catch (feet), no break.)
const ROUTE_LEFT = 0.4;
// A break in it sharper than CUT_LEAST (radians), cut: into it at as much
// of his pace as goes on the new way (half of it square to it), no less
// than CUT_KEEP of it - or, turned back past CUT_BACK, less of it the
// further back, none of it straight back (a curl, a comeback: planted, not
// sent back the way he came at the pace he had on).
const CUT_LEAST = 0.5;
// (A cut not rounded if it leaves him going on out of it at less than this
// share of the pace he takes it round at - see roundCut.)
const ROUND_EARLY = 0.5;
// (A man in motion at the snap: his motion ended no further off it than
// this, ms - see runRoute.)
const MOTION_SNAP_MS = 50;
const CUT_KEEP = 0.25;
const CUT_BACK = (2 * Math.PI) / 3;
const cutKeep = (turn: number): number =>
	turn > CUT_BACK
		? (CUT_KEEP * (Math.PI - turn)) / (Math.PI - CUT_BACK)
		: Math.max(CUT_KEEP, (1 + Math.cos(turn)) / 2);
// (Out of his stance, the first stretch of it, a little slower.)
const routeFps = (speed: number, k: number) => speed * (k === 1 ? 0.8 : 1);
// A break in a route: how much of his pace he cuts at (see CUT_KEEP), what
// that costs him (ms) over going on at his pace, down into it and up out of
// it, and how fast he is going at it (ft/s), slowed into it at `accel`.
type Break = {
	keep: number;
	down: number;
	up: number;
	v: number;
	accel: number;
	// (Up out of it at this, ft/s/s - RAMP_ACCEL, unset.)
	upAccel?: number;
};
// A break at b in a route from a on to c, into it at vIn ft/s and out of
// it at vOut: cut at CUT_ACCEL, up out of it at RAMP_ACCEL.
const breakOf = (
	a: Pt,
	b: Pt,
	c: Pt,
	vIn: number,
	vOut: number,
	plant = false,
): Break => {
	const turn = angleAt(a, b, c);
	if (turn <= CUT_LEAST && !plant) {
		return { keep: 1, down: 0, up: 0, v: vIn, accel: CUT_ACCEL };
	}
	const keep = plant ? Math.min(CUT_KEEP, cutKeep(turn)) : cutKeep(turn);
	const lost = (v: number, A: number) =>
		v > 0 ? (Math.max(0, v - vIn * keep) ** 2 / (2 * A * v)) * 1000 : 0;
	return {
		keep,
		down: lost(vIn, CUT_ACCEL),
		up: lost(vOut, RAMP_ACCEL),
		v: vIn * keep,
		accel: CUT_ACCEL,
	};
};
// (Off the line: from a stand, up to his pace at RELEASE_ACCEL - not at it
// from the first step - see routeFps. Or, on the move already as he goes
// out into it - off a fake - up from the `v0` ft/s he has of it at
// RAMP_ACCEL.)
const RELEASE_ACCEL = 24;
// In motion across behind the line on a sweep: at this pace (ft/s) - no
// quicker than this, if he has less time for it.
const JET_FPS = 19;
const JET_MOST_FPS = 22;
const offLine = (fps: number, v0 = 0): Break =>
	v0 > 0
		? {
				keep: 1,
				down: 0,
				up: v0 < fps ? ((fps - v0) ** 2 / (2 * RAMP_ACCEL * fps)) * 1000 : 0,
				v: v0,
				accel: CUT_ACCEL,
			}
		: {
				keep: 1,
				down: 0,
				up: (fps / (2 * RELEASE_ACCEL)) * 1000,
				v: 0,
				accel: CUT_ACCEL,
				upAccel: RELEASE_ACCEL,
			};
// A route no deeper than `deep` feet downfield: as far as it goes before
// it gets there, and ended there.
const backIn = (path: OPt[], deep: number): OPt[] => {
	const k = path.findIndex((p) => p.d > deep);
	if (k < 1) {
		return path;
	}
	const p0 = path[k - 1]!;
	const p1 = path[k]!;
	const u = (deep - p0.d) / (p1.d - p0.d);
	const end = { d: deep, a: p0.a + (p1.a - p0.a) * u };
	const out = path.slice(0, k);
	if (Math.hypot(end.d - p0.d, end.a - p0.a) > 0.5) {
		out.push(end);
	}
	return out.length >= 2 ? out : path;
};
// How long (ms) a stretch of a route `ft` long takes him at `fps`, out of
// the break before it and into the one at its end: if it is too short for
// him to get up to his pace in between (a hitch's step back down to the
// ball), up as far as he gets and straight back down.
const stretchMs = (ft: number, fps: number, from: Break, to: Break) => {
	const v0 = from.up > 0 ? from.v : fps;
	const v1 = to.down > 0 ? to.v : fps;
	const a0 = from.upAccel ?? RAMP_ACCEL;
	const a1 = to.accel;
	if (
		(fps * fps - v0 * v0) / (2 * a0) + (fps * fps - v1 * v1) / (2 * a1) <=
		ft
	) {
		return (ft / fps) * 1000 + from.up + to.down;
	}
	// (Too short to pull up in, to sit down at the end of it - the last short
	// way of a checkdown, off a bend: pulled up over the last of the way in
	// to it, there that much later - see runRoute.)
	if (to.down > 0 && to.v === 0 && (v0 * v0) / (2 * a1) > ft) {
		return (ft / v0 + v0 / (2 * a1)) * 1000;
	}
	const top = Math.sqrt(
		(2 * ft * a0 * a1 + v0 * v0 * a1 + v1 * v1 * a0) / (a0 + a1),
	);
	return top > Math.max(v0, v1)
		? ((top - v0) / a0 + (top - v1) / a1) * 1000
		: ((2 * ft) / Math.max(1, v0 + v1)) * 1000;
};
// Sat down at the end of his route with longer than this to wait (ms) - the
// ball not coming yet, or not to him - he works his way across it to where
// he sits down again: after a moment sat (and up to this much more), at this
// pace (feet a second), no further than this nor less than this worth the
// going (feet), sat down again this long before it comes (or he is done).
// Mostly across toward the middle of the field, a little back toward the
// ball - more back down toward it off a route out to the sideline - and no
// nearer a sideline than this (feet).
const WORK_AFTER_MS = 600;
const WORK_SIT_MS = 250;
const WORK_SIT_SPREAD = 200;
const WORK_FPS = 5.5;
const WORK_MOST_FT = 12;
const WORK_LEAST_FT = 2;
const WORK_RESET_MS = 200;
const WORK_BACK = 0.35;
const WORK_BACK_OUT = 1.5;
const WORK_MID_FT = 8;
const WORK_IN_FT = 4;

// The kicking game. A place kick: the plant, and the foot into the ball this
// far through it. A punt: from the punter's first step, the ball dropped and
// the foot into it. A kickoff's run-up, from the kicker's first step to his
// plant. How fast a long snap comes back. The kicker after his kick, at
// no more than this (feet a second).
const KICK_MS = 800;
const KICK_CONTACT = 0.4;
// A loose ball picked up on the run: his hands this far out in front of
// him at the bottom of the scoop (feet), that far through it; down to it
// over so long, and the whole of it up off the turf with it so long.
const SCOOP_REACH = 2.4;
const SCOOP_LOW = 0.4;
const SCOOP_DOWN_MS = 400;
const SCOOP_UP_MS = 380;
// (Picked up to be taken off a sharper turn than this (radians) from the
// way he comes at it: slowed into it to this pace (feet a second), and
// round with it as he comes up no quicker than this (radians a second),
// on no further than this (feet) as he does.)
const SCOOP_TURN = 1.05;
const SCOOP_TURN_FPS = 5;
const SCOOP_TURN_RATE = 6;
const SCOOP_TURN_ON_FT = 1;
// The holder: the snap in his hands this long before he brings it down to
// its spot, and so long bringing it.
const HOLD_CATCH_MS = 60;
const HOLD_PLACE_MS = 300;
const PUNT_MS = 1400;
const PUNT_DROP = 0.42;
const PUNT_CONTACT = 0.55;
const KICKOFF_RUN = 1250;
const LONG_SNAP_FPS = 60;
const KICKER_JOG = 12;
const KICKER_JOG_LEAST = 7;
// On an onside kick, a man going to the ball set where he is till he has
// to go: this long, and at this share of his top speed, before he would
// have to be there.
const ONSIDE_GO_MS = 250;
const ONSIDE_GO_PACE = 0.6;
// (Late for it - this quick (ft/s) or more to be there: at it flat out,
// and fielded in his stride, his body this far (feet) short of it as his
// hands take it - see handsOn.)
const ONSIDE_STRIDE_FPS = 15;
const ONSIDE_STRIDE_FT = 1.5;
// The ball on its tee (its middle, off the turf).
const TEE_Z = 0.8;
// How high over the straight line a kick gets that is in the air `ms`.
const peakFor = (ms: number) => (G * (ms / 1000) ** 2) / 8;

const num = (x: unknown, otherwise: number): number =>
	typeof x === "number" ? x : otherwise;

// A blocker and the cover man he is on, locked up: the legs that took the
// cover man to him and kept him there, and when he would have got off.
type Lock = {
	b: Actor;
	c: Actor;
	go: number;
	run: Leg;
	lock: Leg;
};

class Action {
	readonly input: ActInput;
	readonly r: () => number;
	readonly men: Actor[];
	readonly o: Actor[];
	readonly d: Actor[];
	readonly ball: BallSeg[] = [];
	readonly at = new Map<number, number>();
	readonly bounds: Bounds;
	// The goal lines, in the offense's frame: the one it goes for, its own.
	readonly goalD: number;
	readonly ownGoalD: number;
	// Who has his part settled to the whistle: down in the tackle, the
	// quarterback rid of the ball, a man who made a play on it. Everybody
	// else has a part of his own - a route, a block, his coverage - that a
	// play on the ball takes him off.
	readonly done = new Set<number>();
	// Men stood over a man down until then (see PANCAKE): not idle, on
	// after the ball, while he is.
	readonly over = new Map<Actor, number>();
	// Arms of their own over what the rest of a man does (see Leg.arms),
	// put on once everybody has the rest.
	// (`onFeet`: only if he is on his feet all through it - see ON_FEET -
	// with nothing else in his arms.)
	readonly reaches: {
		m: Actor;
		move: Move;
		t0: number;
		t1: number;
		onFeet?: boolean;
	}[] = [];
	// When the defense (the ball picked off, the offense) sees where the
	// play is going, and can leave what it was doing to go after it.
	seen: number;
	whistle = 0;
	// A kick: when it is away, for the camera to go with it from then.
	follow?: number;
	// A kick returned: the returner with room to run (see slow).
	weave = false;
	// When the moment after a big play is over (see celebrate), if it had
	// one.
	until = 0;
	outAt?: Pt;
	// The stats said of what is being acted out: those of its lines from
	// just after `lo` through `hi` - the play up to the first time the ball
	// is out, then each recovery in turn.
	lo: number;
	hi: number;

	// The men in a moment of their own after it (see celebrate), and where
	// they crowd round the man who made it.
	readonly mob = new Set<number>();
	// Where the men trailing the play are each going round where it ends
	// (see trail).
	readonly trailing: Pt[] = [];
	mobAt?: Pt;
	// Men kept where they are after it till then (see taunt): not off with
	// the rest before.
	readonly busy = new Map<number, number>();
	// Where the scorer came out from to meet them (see meetUp).
	went?: Pt;
	// A touchdown: when he got there, and who (see tdArms).
	scored?: { t: number; scorer: Actor };
	party?: { from: number; at: Pt; hero?: number };
	qbRue?: { pid: number; t0: number; t1: number };
	// The man who brought the carrier down, the last time one did.
	downer?: Actor;
	// A pile on the carrier when he goes down (see pileUp): the most men in
	// it.
	heap = 0;

	constructor(input: ActInput) {
		this.input = input;
		this.r = input.r;
		this.men = input.parts.map(
			(p) =>
				new Actor(
					p,
					input.speeds?.get(p.pid),
					input.rated?.get(p.pid),
					input.flair?.get(p.pid),
				),
		);
		this.o = this.men.filter((m) => m.team === input.offense);
		this.d = this.men.filter((m) => m.team !== input.offense);
		const { dir, ballY, losX } = input;
		const a0 = (0 - ballY) * dir;
		const a1 = (FIELD_W - ballY) * dir;
		this.bounds = { left: Math.min(a0, a1), right: Math.max(a0, a1) };
		this.goalD = ((dir === 1 ? GOAL_R : GOAL_L) - losX) * dir;
		this.ownGoalD = ((dir === 1 ? GOAL_L : GOAL_R) - losX) * dir;
		this.seen = input.snap + READ_MS;
		this.lo = input.i0 - 1;
		const fum = this.find("fumble");
		this.hi = fum >= 0 ? fum : input.i1;
	}

	// The offense's frame to the field, and back.
	w(p: OPt): Pt {
		const { dir, losX, ballY } = this.input;
		return { x: losX + dir * p.d, y: ballY + dir * p.a };
	}
	of(p: Pt): OPt {
		const { dir, losX, ballY } = this.input;
		return { d: (p.x - losX) * dir, a: (p.y - ballY) * dir };
	}
	// Facing downfield (the offense's way), back up it, and toward a point.
	get ahead() {
		return this.input.dir === 1 ? 0 : Math.PI;
	}
	get behind() {
		return this.input.dir === 1 ? Math.PI : 0;
	}
	// The last two minutes of a close game.
	get crunch() {
		return (this.input.tense ?? 0) >= 1;
	}
	toward(from: Pt, to: Pt) {
		return Math.atan2(to.y - from.y, to.x - from.x);
	}
	// A man down at `down` facing `yaw` (on his back, or his front) pulled
	// up by the hand by a man come to about `by` (see takeHand): where his
	// hand held up meets the other's, out to his left or his right; and
	// where the other stands to meet it there, out from it the way `by` is
	// from his chest, and the way he faces.
	private handUp(down: Pt, yaw: number, back: boolean, by: Pt, left: boolean) {
		const u = { x: Math.cos(yaw), y: Math.sin(yaw) };
		// (His right this way; the hand's `s` to his left.)
		const n = { x: -u.y, y: u.x };
		const h = back ? TAKE_HAND_BACK : TAKE_HAND;
		const out = left ? h.s : -h.s;
		const meet = {
			x: down.x + u.x * h.f + n.x * out,
			y: down.y + u.y * h.f + n.y * out,
		};
		const head = back ? -HELP_CHEST : HELP_CHEST;
		const chest = { x: down.x + u.x * head, y: down.y + u.y * head };
		const from =
			dist(by, chest) > HAND_UP_LEAST_FT
				? this.toward(chest, by)
				: Math.atan2(n.y * Math.sign(out), n.x * Math.sign(out));
		return {
			at: {
				x: meet.x + Math.cos(from) * HELP_HAND_FT,
				y: meet.y + Math.sin(from) * HELP_HAND_FT,
			},
			face: from + Math.PI - HELP_HAND_TURN,
			left,
			from,
		};
	}
	// How near a man comes to `q` at t: to where he stands, or, down on the
	// turf, to the length of him (see LIE_BACK - or `legs` behind his hips,
	// his legs and all).
	private nearOf(m: Actor, t: number, q: Pt, legs = LIE_BACK): number {
		const at = m.posAt(t);
		const k = legIndexAt(m.legs, t);
		const move = k < 0 ? undefined : m.legs[k]!.move;
		const back = move !== undefined && LIE_BACKWARD.has(move);
		if (move === undefined || (!back && !LIE_FACING.has(move))) {
			return dist(at, q);
		}
		let yaw = m.yaw;
		for (let i = 0; i <= k; i++) {
			yaw = yawOf(m.legs[i]!, yaw);
		}
		const a = back ? yaw + Math.PI : yaw;
		const ux = Math.cos(a);
		const uy = Math.sin(a);
		const along = clamp((q.x - at.x) * ux + (q.y - at.y) * uy, -legs, LIE_HEAD);
		return Math.hypot(q.x - at.x - ux * along, q.y - at.y - uy * along);
	}

	// Where the man with the ball at the whistle tosses it down by his feet
	// at t, from his hands held out in front of him the way he faces: out
	// that way, or the nearest way round from it where it passes nobody by
	// him - and with nowhere clear, the way it is soonest clearest of them.
	private tossWay(m: Actor, t: number, yaw: number): { from: Pt; to: Pt } {
		const at = m.posAt(t);
		const hands = {
			x: at.x + Math.cos(yaw) * TOSS_HANDS,
			y: at.y + Math.sin(yaw) * TOSS_HANDS,
		};
		const others = this.men
			.filter((o) => o !== m)
			.flatMap((o) => [0, 0.5, 1].map((k) => o.posAt(t + k * TOSS_MS)));
		const later = this.men.flatMap((o) =>
			TOSS_LATER.map((ms) => o.posAt(t + TOSS_MS + ms)),
		);
		// (How near anybody comes to its way out from his hands to `to` -
		// past the first of it, out of anybody it starts in - or to it
		// there on the turf a while after, him too.)
		const nearest = (to: Pt) => {
			const vx = to.x - hands.x;
			const vy = to.y - hands.y;
			const l2 = vx * vx + vy * vy;
			let near = Infinity;
			for (const p of others) {
				const u =
					l2 > 0
						? clamp(
								((p.x - hands.x) * vx + (p.y - hands.y) * vy) / l2,
								TOSS_PAST,
								1,
							)
						: 1;
				near = Math.min(
					near,
					Math.hypot(p.x - hands.x - vx * u, p.y - hands.y - vy * u),
				);
			}
			for (const p of later) {
				near = Math.min(near, Math.hypot(p.x - to.x, p.y - to.y));
			}
			return near;
		};
		let best = hands;
		let widest = -1;
		for (const turn of TOSS_TURNS) {
			const a = yaw + turn;
			const to = {
				x: at.x + Math.cos(a) * TOSS_OUT,
				y: at.y + Math.sin(a) * TOSS_OUT,
			};
			const near = nearest(to);
			if (near >= TOSS_CLEAR) {
				return { from: hands, to };
			}
			if (near > widest) {
				widest = near;
				best = to;
			}
		}
		return { from: hands, to: best };
	}
	// Where a pass let go of at `from` at t (off a man's hands, or knocked
	// away) comes to rest `far` feet off: the way `prefer` if it drops and
	// bounces past nobody, else the nearest way round from it that does -
	// or with none, the way it passes furthest from anybody.
	private dropWay(from: Pt, t: number, far: number, prefer: number): Pt {
		// (On its way: a share of the way out at so long after - half of
		// it to the ground, the rest on the bounce.)
		const on = [
			[INC_DROP_MS / 3, 1 / 6],
			[(INC_DROP_MS * 2) / 3, 1 / 3],
			[INC_DROP_MS, 1 / 2],
			[INC_DROP_MS + INC_BOUNCE_MS / 2, 3 / 4],
			[INC_DROP_MS + INC_BOUNCE_MS, 1],
		] as const;
		const seen = on.map(([ms]) => this.men.map((m) => m.posAt(t + ms)));
		let best = prefer;
		let widest = -1;
		for (const turn of DROP_TURNS) {
			const a = prefer + turn;
			const ux = Math.cos(a) * far;
			const uy = Math.sin(a) * far;
			let near = Infinity;
			on.forEach(([, k], i) => {
				for (const p of seen[i]!) {
					near = Math.min(
						near,
						Math.hypot(p.x - from.x - ux * k, p.y - from.y - uy * k),
					);
				}
			});
			if (near >= DROP_CLEAR) {
				best = a;
				break;
			}
			if (near > widest) {
				widest = near;
				best = a;
			}
		}
		return {
			x: from.x + Math.cos(best) * far,
			y: from.y + Math.sin(best) * far,
		};
	}
	// A blocker turned to his man - no further than BLOCK_TURN off straight
	// ahead.
	facing(from: Pt, him: Pt) {
		const off = this.toward(from, him) - this.ahead;
		return (
			this.ahead +
			clamp(Math.atan2(Math.sin(off), Math.cos(off)), -BLOCK_TURN, BLOCK_TURN)
		);
	}
	// From `from` round `corner` toward `to`: the points of an arc bent as
	// wide as he takes it at the pace he gets there at (off from still at
	// `a`, no quicker than `top`), the corner itself if there is no turn.
	bend(from: Pt, corner: Pt, to: Pt, a: number, top: number): Pt[] {
		const dIn = dist(from, corner);
		const dOut = dist(corner, to);
		if (dIn < 1e-6 || dOut < 1e-6) {
			return [corner];
		}
		const u = { x: (corner.x - from.x) / dIn, y: (corner.y - from.y) / dIn };
		const v = { x: (to.x - corner.x) / dOut, y: (to.y - corner.y) / dOut };
		const turn = Math.acos(clamp(u.x * v.x + u.y * v.y, -1, 1));
		if (turn < 0.05) {
			return [corner];
		}
		const pace = Math.min(top, Math.sqrt(2 * a * dIn));
		const tan = Math.tan(turn / 2);
		const t = Math.min(
			((pace * pace) / EDGE_LAT) * tan,
			dIn * EDGE_BEND_MOST,
			dOut * EDGE_BEND_MOST,
		);
		const R = t / tan;
		const s = Math.sign(u.x * v.y - u.y * v.x) || 1;
		const c = {
			x: corner.x - u.x * t - s * u.y * R,
			y: corner.y - u.y * t + s * u.x * R,
		};
		const r0 = Math.atan2(corner.y - u.y * t - c.y, corner.x - u.x * t - c.x);
		const pts: Pt[] = [];
		for (let i = 0; i <= EDGE_ARC_STEPS; i++) {
			const q = r0 + (s * turn * i) / EDGE_ARC_STEPS;
			pts.push({ x: c.x + Math.cos(q) * R, y: c.y + Math.sin(q) * R });
		}
		return pts;
	}

	// Of `coverage`, the man the sim flags for pass interference.
	interferer(coverage: Actor[]): Actor | undefined {
		return this.flaggedFor("Pass interference", coverage);
	}

	// Of `men`, the man the sim flags for `name`: off the stat line that
	// goes with the flag - or, none, by his name in it.
	flaggedFor(name: string, men: Actor[]): Actor | undefined {
		const { events, i0, i1, pidOf } = this.input;
		for (let i = i0; i < i1; i++) {
			const e = events[i];
			if (e?.type !== "penalty" || e.penaltyName !== name) {
				continue;
			}
			for (let j = i + 1; j < i1; j++) {
				const x = events[j];
				if (x?.type === "penalty") {
					break;
				}
				if (x?.type === "stat" && x.s === "pen" && typeof x.pid === "number") {
					const pid = x.pid;
					return men.find((m) => m.pid === pid);
				}
			}
			const named = Array.isArray(e.names) ? e.names[0] : undefined;
			const pid =
				typeof named === "string"
					? pidOf?.((1 - (e.t === 1 ? 1 : 0)) as Side, named)
					: undefined;
			return pid === undefined ? undefined : men.find((m) => m.pid === pid);
		}
		return undefined;
	}

	// What the sim says: the play's events of a kind, its stats.
	find(type: string, from = this.input.i0): number {
		const { events, i1 } = this.input;
		for (let i = from; i < i1; i++) {
			if (events[i]?.type === type) {
				return i;
			}
		}
		return -1;
	}
	ev(i: number): RawEvent {
		return this.input.events[i]!;
	}
	statPids(...names: string[]): number[] {
		return this.statPidsIn(this.lo, this.hi, ...names);
	}
	statPidsIn(lo: number, hi: number, ...names: string[]): number[] {
		const out: number[] = [];
		for (const x of this.input.stats) {
			if (
				x.i > lo &&
				x.i <= hi &&
				names.includes(x.s) &&
				!out.includes(x.pid)
			) {
				out.push(x.pid);
			}
		}
		return out;
	}
	// The ball out again right after line i: the "fumble" line just after
	// it, or -1.
	fumbleAfter(i: number): number {
		const { events, i1 } = this.input;
		for (let k = i + 1; k < i1; k++) {
			const type = events[k]?.type;
			if (type !== "stat") {
				return type === "fumble" ? k : -1;
			}
		}
		return -1;
	}
	// A stat's amount over the play (the yards of a return the sim gives no
	// line of its own).
	amount(s: string): number {
		const { events, i0, i1 } = this.input;
		let n = 0;
		for (let i = i0; i < i1; i++) {
			const e = events[i];
			if (e?.type === "stat" && e.s === s && typeof e.amt === "number") {
				n += e.amt;
			}
		}
		return n;
	}
	man(pid: number | undefined): Actor | undefined {
		return pid === undefined ? undefined : this.men.find((m) => m.pid === pid);
	}
	role(role: Role, side = this.input.offense): Actor | undefined {
		return this.men.find((m) => m.role === role && m.team === side);
	}

	// Everybody in his stance until the snap.
	run(): Acted | undefined {
		const { kind, snap } = this.input;
		for (const m of this.men) {
			m.hold(snap);
		}
		const drop = this.find("dropback");
		const kneel = this.find("kneel");
		const handoff = this.find("handoff");
		const runI = this.find("run");
		// (A kick: what the sim says came of it, or - a flag before it -
		// nothing to act out.)
		const kicked = this.find(
			kind === "kickoff" || kind === "punt"
				? kind
				: kind === "onside"
					? "onsideKickRecovery"
					: kind,
		);
		let snapped = false;
		if (
			kind === "kickoff" ||
			kind === "punt" ||
			kind === "fieldGoal" ||
			kind === "extraPoint" ||
			kind === "onside"
		) {
			if (kicked >= 0) {
				snapped = true;
				if (kind === "kickoff") {
					this.kickoff(kicked);
				} else if (kind === "punt") {
					this.punt(kicked);
				} else if (kind === "onside") {
					this.onside(kicked);
				} else if (!this.fieldGoal(kicked)) {
					return undefined;
				}
			}
		} else if (kind === "kneel" && kneel >= 0) {
			snapped = true;
			this.kneel(kneel);
		} else if (kind === "pass" && drop >= 0) {
			snapped = true;
			this.pass(drop);
		} else if ((kind === "run" && runI >= 0) || handoff >= 0) {
			if (runI < 0) {
				return undefined;
			}
			snapped = true;
			this.runPlay(handoff, runI);
		}
		if (!snapped) {
			// (Nothing but a flag: a foul before the snap.)
			const flagI = this.find("flag");
			if (flagI < 0) {
				return undefined;
			}
			this.deadBall(flagI);
		}
		if (snapped) {
			this.offside();
		}
		this.upAgain();
		this.afterWhistle();
		this.watch();
		this.celebrate();
		this.taunt();
		this.regroup();
		this.hurtDown();
		this.noAirBlocks();
		this.blockBattles();
		this.noTwins();
		this.offMenDown();
		this.readyUp();
		this.noTreading();
		this.noIdleSets();
		this.noSlowGaits();
		this.turnsOffBlocks();
		this.noFlicks();
		this.overHeads();
		this.tdArms();
		for (const x of this.reaches) {
			if (
				x.onFeet &&
				x.m.legs.some(
					(l) =>
						l.t1 > x.t0 &&
						l.t0 < x.t1 &&
						(!ON_FEET.has(l.move) || l.arms !== undefined || l.ball),
				)
			) {
				continue;
			}
			x.m.arms(x.move, x.t0, x.t1);
		}
		this.handBall();
		return {
			at: this.at,
			ball: this.ball,
			whistle: this.whistle,
			...(this.follow !== undefined ? { follow: this.follow } : {}),
			...(this.until > 0 ? { until: this.until } : {}),
			...(this.party && this.until > 0 ? { party: this.party } : {}),
			...(this.qbRue ? { qbRue: this.qbRue } : {}),
			...(this.outAt ? { out: this.outAt } : {}),
		};
	}

	// A man of the defense stood waiting in the middle of the play - on a
	// block coming at him, on the ball carrier coming his way: down in his
	// stance and alive in it, not stood up straight like a man watching it
	// from the sideline.
	readyUp() {
		const { kind, snap } = this.input;
		if (kind !== "pass" && kind !== "run") {
			return;
		}
		// (Off at something after it - not walking off, done with it.)
		const goes = (l: Leg | undefined) =>
			l !== undefined &&
			l.move !== "walk" &&
			(LINE_PLAY.has(l.move) ||
				moveKind(l.move) === "act" ||
				l.from.x !== l.to.x ||
				l.from.y !== l.to.y);
		// (Stood there a while - his stand split in pieces or not - come
		// there on his feet, or pulled up there: see easeOff.)
		const stood = (l: Leg) =>
			l.move === "stand" &&
			l.ball === undefined &&
			l.arms === undefined &&
			l.from.x === l.to.x &&
			l.from.y === l.to.y;
		for (const m of this.d) {
			const legs = m.legs;
			for (let k = 0; k < legs.length; k++) {
				if (!stood(legs[k]!)) {
					continue;
				}
				let j = k;
				while (
					j + 1 < legs.length &&
					stood(legs[j + 1]!) &&
					legs[j + 1]!.from.x === legs[k]!.from.x &&
					legs[j + 1]!.from.y === legs[k]!.from.y
				) {
					j += 1;
				}
				const prev = legs[k - 1];
				if (
					legs[k]!.t0 >= snap + READY_FROM_MS &&
					legs[j]!.t1 <= this.whistle - READY_BEFORE_MS &&
					legs[j]!.t1 - legs[k]!.t0 >= READY_LEAST_MS &&
					prev &&
					(STEPS.has(prev.move) ||
						LINE_PLAY.has(prev.move) ||
						(prev.move === "stand" &&
							(prev.from.x !== prev.to.x || prev.from.y !== prev.to.y))) &&
					goes(legs[j + 1])
				) {
					for (let i = k; i <= j; i++) {
						legs[i]!.move = "backerReady";
					}
				}
				k = j;
			}
		}
	}

	// A man the sim says was hurt on it down on the turf after the whistle
	// as hurt.ts will have him (see fallHurt) - and there for good, as far
	// as anybody going about him knows (see offMenDown): the trainers out
	// to him and the rest of it are hurt.ts's. Or, a man with no part in it
	// the sim names, going all out away from the ball late on: pulled up
	// there, lame, and down - on the turf when the whistle goes; or, in a
	// block late on, down there in it.
	hurtDown() {
		const { events, i0, i1, snap, stats } = this.input;
		let n = 0;
		// (Where those hurt on it so far are down - see offHurt.)
		const downs: Pt[] = [];
		for (let i = i0; i < i1 && n < HURT_MOST; i++) {
			const e = events[i];
			if (e?.type !== "injury" || typeof e.injuredPID !== "number") {
				continue;
			}
			const m = this.man(e.injuredPID);
			if (!m || m.legs.length === 0) {
				continue;
			}
			n++;
			const named =
				m.role === "QB" ||
				stats.some((x) => x.pid === m.pid && x.i >= i0 && x.i < i1) ||
				this.ball.some((b) => b.kind === "held" && b.pid === m.pid);
			let lame: number | undefined;
			for (
				let t = this.whistle - HURT_LAME_LEAD_MS;
				!named && t >= snap + HURT_LAME_FROM_MS;
				t -= 50
			) {
				const l = legAt(m, t);
				const ball = this.ballAt(t);
				if (
					GAITS.has(l.move) &&
					!l.arms &&
					!l.ball &&
					legPace(l) >= HURT_LAME_FPS &&
					ball &&
					dist(m.posAt(t), ball) >= HURT_LAME_OFF_FT
				) {
					lame = t;
					break;
				}
			}
			// (Or, in the line, a man of the other side up against him: down
			// there in it, on his back.)
			let blocked: number | undefined;
			for (
				let t = this.whistle - HURT_BLOCK_LEAD_MS;
				!named && lame === undefined && t >= snap + HURT_BLOCK_FROM_MS;
				t -= 50
			) {
				const l = legAt(m, t);
				const at = m.posAt(t);
				if (
					HURT_BLOCKS.has(l.move) &&
					!l.arms &&
					this.men.some(
						(o) =>
							o.team !== m.team &&
							!DOWN.has(legAt(o, t).move) &&
							dist(o.posAt(t), at) <= HURT_BLOCK_NEAR,
					)
				) {
					blocked = t;
					break;
				}
			}
			if (blocked !== undefined) {
				m.cut(blocked);
				const yaw = m.yaw;
				m.act(
					"tackledBack",
					m.t + HURT_LAME_FALL_MS,
					yaw,
					offHurt(
						{
							x: m.at.x - Math.cos(yaw) * HURT_BLOCK_BACK_FT,
							y: m.at.y - Math.sin(yaw) * HURT_BLOCK_BACK_FT,
						},
						downs,
					),
				);
				m.hold(Math.max(m.t, this.whistle) + HURT_LAME_PAST_MS, "supine");
			} else if (lame === undefined) {
				fallHurt(m.legs, this.whistle, downs);
			} else {
				m.cut(lame);
				m.easeOff(HURT_LAME_PULL_FT);
				const back = countOf(m, this.input.key, 41) < HURT_LAME_BACK;
				m.act(
					back ? "tackledBack" : "tackled",
					m.t + HURT_LAME_FALL_MS,
					undefined,
					offHurt(m.at, downs),
				);
				m.hold(
					Math.max(m.t, this.whistle) + HURT_LAME_PAST_MS,
					back ? "supine" : "prone",
				);
			}
			downs.push(m.legs.at(-1)!.to);
		}
	}

	// A pass with a man on his feet in its way - on the line of it, below the
	// top of his head - lofted over him instead: as much higher at the top as
	// that takes (no more than OVER_MOST). Not the man it left or the man it
	// comes to, nor a man going up for it.
	overHeads() {
		this.ball.forEach((b, k) => {
			if (b.kind !== "fly" || !b.spiral || b.t0 >= this.whistle) {
				return;
			}
			const prev = this.ball[k - 1];
			const next = this.ball[k + 1];
			const ends = [prev, next].flatMap((x) =>
				x?.kind === "held" ? [x.pid] : [],
			);
			let peak = b.peak;
			for (let t = b.t0; t < b.t1; t += OVER_STEP_MS) {
				const u = (t - b.t0) / (b.t1 - b.t0);
				if (u < OVER_FROM || u > 1 - OVER_FROM) {
					continue;
				}
				const arc = 4 * u * (1 - u);
				const lin = b.from.z + (b.to.z - b.from.z) * u;
				if (lin + arc * peak >= OVER_Z) {
					continue;
				}
				const at = {
					x: b.from.x + (b.to.x - b.from.x) * u,
					y: b.from.y + (b.to.y - b.from.y) * u,
				};
				for (const m of this.men) {
					if (ends.includes(m.pid)) {
						continue;
					}
					const l = m.legs.findLast((x) => x.t0 <= t);
					if (
						!l ||
						OVER_NOT.has(l.move) ||
						(l.arms !== undefined && OVER_NOT.has(l.arms)) ||
						dist(m.posAt(t), at) >= OVER_NEAR
					) {
						continue;
					}
					peak = Math.max(peak, (OVER_Z - lin) / arc);
					break;
				}
			}
			b.peak = Math.min(peak, b.peak + OVER_MOST);
		});
	}

	// No stride or two of another gait between one and the next (a run
	// slowed into a block, its last step a jog - cut short by it): on in
	// the gait he was in. Nor a step of one between two of the same other
	// stride (a blocker's drive, a step on to the man): on in that, facing
	// the way he was.
	// Stepping on the spot - a man run up against the sideline, say, his
	// legs cut short at it but his gait carried on into nothing: a stretch
	// of steps that go next to nowhere (under CRAWL_FPS each) for at least
	// GAIT_LEAST_MS, stood there instead, facing the way he was. (See legTo,
	// which does as much for one leg; these come a step at a time.)
	noTreading() {
		const still = (l: Leg) =>
			STEPS.has(l.move) &&
			dist(l.from, l.to) < (CRAWL_FPS * (l.t1 - l.t0)) / 1000;
		for (const m of this.men) {
			const legs = m.legs;
			let yaw = legs[0]?.yaw ?? 0;
			const faced = legs.map((l) => (yaw = yawOf(l, yaw)));
			for (let i = 0; i < legs.length;) {
				if (!still(legs[i]!)) {
					i++;
					continue;
				}
				let j = i + 1;
				while (
					j < legs.length &&
					still(legs[j]!) &&
					legs[j]!.t0 === legs[j - 1]!.t1
				) {
					j++;
				}
				// (Or not so long, pulled up into it - his last steps slow
				// already: stood, not stalled in a stride.)
				const before = legs[i - 1];
				const pulled =
					before !== undefined &&
					before.t1 === legs[i]!.t0 &&
					STEPS.has(before.move) &&
					legPace(before) < TREAD_FROM_FPS;
				if (
					legs[j - 1]!.t1 - legs[i]!.t0 >=
					(pulled ? TREAD_PULLED_MS : GAIT_LEAST_MS)
				) {
					const face = faced[i - 1] ?? faced[i]!;
					for (let k = i; k < j; k++) {
						const l = legs[k]!;
						l.move = "stand";
						l.yaw ??= face;
					}
				}
				i = j;
			}
		}
	}

	// A man in to block with nobody come at him - a back, mostly - set the
	// whole time: eyes up for the blitz, a look one way and a step that way,
	// then the other, and back square where he was to be - not stood set
	// like a statue. (Last of all, his way laid out: nothing else goes by
	// where he is meanwhile - see countOf.)
	noIdleSets() {
		const snap = this.input.snap;
		for (const b of this.o) {
			const legs = b.legs;
			for (let i = 0; i < legs.length; i++) {
				const l = legs[i]!;
				if (
					l.move !== "passSet" ||
					l.arms ||
					l.t0 < snap ||
					l.t1 - l.t0 < 2 * SCAN_STEP_MS[1] ||
					dist(l.from, l.to) > SCAN_STILL_FT
				) {
					continue;
				}
				const { t0, t1 } = l;
				const face = l.yaw ?? this.ahead;
				const baseAt = (t: number) => {
					const u = (t - t0) / (t1 - t0);
					return this.of({
						x: l.from.x + (l.to.x - l.from.x) * u,
						y: l.from.y + (l.to.y - l.from.y) * u,
					});
				};
				const out: Leg[] = [];
				let at = l.from;
				let yaw = face;
				let tw = t0;
				const step = (to: Pt, ta: number, tb: number, y: number) => {
					if (ta > tw) {
						out.push({ ...l, t0: tw, t1: ta, from: at, to: at, yaw });
					}
					out.push({ ...l, t0: ta, t1: tb, from: at, to, yaw: y });
					at = to;
					yaw = y;
					tw = tb;
				};
				let side = countOf(b, this.input.key, 74) < 0.5 ? -1 : 1;
				let t = t0 + SCAN_FIRST_MS;
				for (let k = 0; t + SCAN_STEP_MS[0] <= t1; k++) {
					const h = countOf(b, this.input.key, 75 + (k % 4));
					const o = baseAt(t + SCAN_GO_MS);
					step(
						this.w({
							d: o.d,
							a:
								o.a +
								side * (SCAN_OUT_FT[0] + (SCAN_OUT_FT[1] - SCAN_OUT_FT[0]) * h),
						}),
						t,
						t + SCAN_GO_MS,
						face + side * SCAN_LOOK,
					);
					side = -side;
					t += SCAN_STEP_MS[0] + (SCAN_STEP_MS[1] - SCAN_STEP_MS[0]) * h;
				}
				if (out.length === 0) {
					continue;
				}
				// (Back square where he was to be, by then.)
				step(l.to, tw, tw + SCAN_GO_MS, face);
				out.push({ ...l, t0: tw, t1, from: at, to: at, yaw: face });
				legs.splice(i, 1, ...out);
				i += out.length - 1;
			}
		}
	}

	// A leg run, or sprinted, slower than the gait goes for GAIT_LEAST_MS
	// or more - slowed after it was laid, to meet a ball or another man:
	// in the quickest gait that goes that slow (a run at a jog's pace a
	// jog), not run on the spot. And one walked or jogged a good deal
	// quicker than that gait goes (see GAIT_MOST) - bent out round a man
	// down, or sent off in a gait asked for at a pace beyond it: in the
	// gait that goes that quick, not his legs a blur under him.
	noSlowGaits() {
		const DOWN: Partial<Record<Move, Move>> = {
			sprint: "run",
			run: "jog",
			jog: "walk",
		};
		const UP: Partial<Record<Move, Move>> = {
			walk: "jog",
			jog: "run",
			run: "sprint",
		};
		for (const m of this.men) {
			for (const l of m.legs) {
				const fps = legPace(l);
				let move = l.move;
				while (fps > (GAIT_MOST[move] ?? Infinity) && UP[move]) {
					move = UP[move]!;
				}
				if (l.t1 - l.t0 >= GAIT_LEAST_MS) {
					while (fps < (GAIT_LEAST[move] ?? -1) && DOWN[move]) {
						move = DOWN[move]!;
					}
				}
				l.move = move;
			}
		}
	}

	// Off a block into his stride, well round from the way he faced the man
	// - after the ball, say: turned off him as he sheds him (see turnInto),
	// not spun round in a blink as he goes.
	turnsOffBlocks() {
		for (const m of this.men) {
			const legs = m.legs;
			for (let k = legs.length - 2; k >= 0; k--) {
				if (SHEDS.has(legs[k]!.move)) {
					m.turnInto(k);
				}
			}
		}
	}

	noFlicks() {
		for (const m of this.men) {
			const legs = m.legs;
			for (let i = 1; i + 1 < legs.length; i++) {
				const l = legs[i]!;
				const was = legs[i - 1]!;
				const next = legs[i + 1]!;
				if (l.t1 - l.t0 >= FLICK_MS || was.t1 !== l.t0) {
					continue;
				}
				if (
					GAITS.has(l.move) &&
					GAITS.has(was.move) &&
					l.move !== was.move &&
					l.move !== next.move
				) {
					l.move = was.move;
				} else if (
					GAITS.has(l.move) &&
					!GAITS.has(was.move) &&
					moveKind(was.move) === "cycle" &&
					next.move === was.move &&
					next.yaw === was.yaw &&
					next.t0 === l.t1 &&
					!l.arms &&
					!was.arms &&
					!next.arms
				) {
					l.move = was.move;
					if (was.yaw === undefined) {
						delete l.yaw;
					} else {
						l.yaw = was.yaw;
					}
				} else if (
					GAITS.has(l.move) &&
					GAITS.has(next.move) &&
					!GAITS.has(was.move) &&
					l.move !== next.move &&
					next.t0 === l.t1 &&
					l.yaw === next.yaw &&
					!l.arms
				) {
					// (Off out of something else into one gait, a blink of
					// another first: in the one from the first of it.)
					l.move = next.move;
				} else if (
					GAITS.has(l.move) &&
					sits(was.move) &&
					(next.move === "engage" || next.move === "drive") &&
					next.t0 === l.t1 &&
					legPace(l) < BLOCK_DRIVE_FPS &&
					!l.arms
				) {
					// (Set, and a blink of a step off it before a block is on
					// him: in the block from the first of it.)
					l.move = next.move;
					if (next.yaw === undefined) {
						delete l.yaw;
					} else {
						l.yaw = next.yaw;
					}
				} else if (
					GAITS.has(l.move) &&
					next.t0 === l.t1 &&
					[was, next].every(
						(n) =>
							(moveKind(n.move) === "still" || ALIVE.has(n.move)) &&
							n.from.x === n.to.x &&
							n.from.y === n.to.y,
					)
				) {
					// (A step from stood to stood: a step, taken - not his legs
					// flicked through a stride.)
					const grow = STEP_MS - (l.t1 - l.t0);
					const back = clamp(grow / 2, 0, was.t1 - was.t0 - STEP_LEFT_MS);
					const on = clamp(grow - back, 0, next.t1 - next.t0 - STEP_LEFT_MS);
					l.t0 -= back;
					was.t1 = l.t0;
					l.t1 += on;
					next.t0 = l.t1;
					l.move = "walk";
				} else if (
					sits(l.move) &&
					sits(was.move) &&
					l.move !== next.move &&
					!l.arms &&
					!l.ball &&
					!was.arms &&
					l.from.x === l.to.x &&
					l.from.y === l.to.y &&
					was.to.x === l.from.x &&
					was.to.y === l.from.y &&
					was.from.x === was.to.x &&
					was.from.y === was.to.y
				) {
					// (Stood, set a moment before he goes - his set cut short
					// by it: stood as he was till then, not a blink of it.)
					l.move = was.move;
					if (was.yaw === undefined) {
						delete l.yaw;
					} else {
						l.yaw = was.yaw;
					}
				} else if (
					l.move === "stand" &&
					!l.arms &&
					!l.ball &&
					next.t0 === l.t1 &&
					l.from.x === l.to.x &&
					l.from.y === l.to.y &&
					next.from.x === l.to.x &&
					next.from.y === l.to.y
				) {
					// (Stood a moment where he is between one thing and the
					// next - off on his way again: on into the next, not
					// flicked through standing.)
					next.t0 = l.t0;
					legs.splice(i, 1);
					i--;
				} else if (
					l.move === "settle" &&
					!l.arms &&
					!l.ball &&
					!GAITS.has(next.move) &&
					STEPS.has(was.move) &&
					was.to.x === l.from.x &&
					was.to.y === l.from.y &&
					l.from.x === l.to.x &&
					l.from.y === l.to.y
				) {
					// (Sat down for a catch a moment before it: the last of
					// his way there slowed into it instead - the reach for it
					// timed to the ball as it was.)
					was.t1 = l.t1;
					legs.splice(i, 1);
					i--;
				}
			}
		}
	}

	// Nobody there to block, or to fight off, all the while: a man still
	// driving into a man who has gone - the one he was on away after the
	// carrier, or gone down to the turf; the man he went out to never there
	// - eases up instead, on at a walk or stood watching it.
	// LOCKED UP A LONG WHILE (BATTLE_LEAST_MS or more), the blocker driving
	// and the man on him: not slid along together stock steady the whole
	// of it, one piece, but every BATTLE_MS or so a surge or a give and a
	// lean off to one side or the other - the two of them together, the one
	// still on the other - and back on their way by the end of it. Not into
	// anybody else, though (see BATTLE_CLEAR_FT): steady there instead.
	blockBattles() {
		const key = this.input.key;
		for (const b of this.men) {
			for (let i = 0; i < b.legs.length; i++) {
				const l = b.legs[i]!;
				if (
					l.move !== "drive" ||
					l.arms ||
					l.turn ||
					l.span ||
					l.t1 - l.t0 < BATTLE_LEAST_MS
				) {
					continue;
				}
				const mid = (l.t0 + l.t1) / 2;
				const m = this.men.find(
					(x) =>
						x.team !== b.team &&
						legAt(x, mid).move === "engage" &&
						dist(x.posAt(mid), b.posAt(mid)) <= BATTLE_PAIR_FT,
				);
				if (!m) {
					continue;
				}
				const k = legIndexAt(m.legs, mid);
				const ml = m.legs[k]!;
				const t0 = Math.max(l.t0, ml.t0);
				const t1 = Math.min(l.t1, ml.t1);
				if (
					ml.arms ||
					ml.turn ||
					ml.span ||
					t1 - t0 < BATTLE_LEAST_MS ||
					dist(m.posAt(t0), b.posAt(t0)) > BATTLE_PAIR_FT ||
					dist(m.posAt(t1), b.posAt(t1)) > BATTLE_PAIR_FT
				) {
					continue;
				}
				// (Which way he drives him, and across it.)
				const bp = b.posAt(mid);
				const mp = m.posAt(mid);
				const far = Math.max(dist(bp, mp), 0.1);
				const ux = (mp.x - bp.x) / far;
				const uy = (mp.y - bp.y) / far;
				const knots: { t: number; ox: number; oy: number }[] = [
					{ t: t0, ox: 0, oy: 0 },
				];
				let j = 0;
				let t = t0;
				for (;;) {
					t +=
						BATTLE_MS[0] +
						(BATTLE_MS[1] - BATTLE_MS[0]) * countOf(b, key, 1000 + j);
					if (t > t1 - BATTLE_MS[0] / 2) {
						break;
					}
					const lean = BATTLE_LEAN_FT * (2 * countOf(b, key, 1100 + j) - 1);
					const push = BATTLE_PUSH_FT * (2 * countOf(b, key, 1200 + j) - 1);
					let ox = ux * push - uy * lean;
					let oy = uy * push + ux * lean;
					const nb = b.posAt(t);
					const nm = m.posAt(t);
					const crowded = this.men.some(
						(x) =>
							x !== b &&
							x !== m &&
							Math.min(
								dist(x.posAt(t), { x: nb.x + ox, y: nb.y + oy }),
								dist(x.posAt(t), { x: nm.x + ox, y: nm.y + oy }),
							) < BATTLE_CLEAR_FT,
					);
					if (crowded) {
						ox = 0;
						oy = 0;
					}
					knots.push({ t, ox, oy });
					j++;
				}
				knots.push({ t: t1, ox: 0, oy: 0 });
				if (knots.length < 3) {
					continue;
				}
				const pieces = (x: Leg) => {
					const along = (c: number) => {
						const u = (c - x.t0) / Math.max(1e-6, x.t1 - x.t0);
						return {
							x: x.from.x + (x.to.x - x.from.x) * u,
							y: x.from.y + (x.to.y - x.from.y) * u,
						};
					};
					const out: Leg[] = [];
					if (x.t0 < t0) {
						out.push({ ...x, t1: t0, to: along(t0) });
					}
					for (let q = 1; q < knots.length; q++) {
						const a = knots[q - 1]!;
						const c = knots[q]!;
						const pa = along(a.t);
						const pc = along(c.t);
						out.push({
							...x,
							t0: a.t,
							t1: c.t,
							from: { x: pa.x + a.ox, y: pa.y + a.oy },
							to: { x: pc.x + c.ox, y: pc.y + c.oy },
						});
					}
					if (x.t1 > t1) {
						out.push({ ...x, t0: t1, from: along(t1) });
					}
					return out;
				};
				const mine = pieces(l);
				b.legs.splice(i, 1, ...mine);
				m.legs.splice(k, 1, ...pieces(ml));
				i += mine.length - 1;
			}
		}
	}

	noAirBlocks() {
		for (const m of this.men) {
			const them = m.team === this.input.offense ? this.d : this.o;
			const by = (t: number) => {
				const at = m.posAt(t);
				return (
					them.some(
						(o) =>
							dist(o.posAt(t), at) < AIR_BLOCK && !DOWN.has(legAt(o, t).move),
					) ||
					this.men.some(
						(o) =>
							o !== m &&
							PILED.has(legAt(o, t).move) &&
							dist(o.posAt(t), at) < AIR_PILE,
					)
				);
			};
			// (A push he had a man on him for only some of - the man gone off
			// after the ball, say, or one come back at him late: made pieces
			// where he went, the ones between his own, not on driving into
			// nobody. A man only running by him a moment, or come up to him
			// just as it ends: no block.)
			// (Nor his first step out of his stance cut to a blink of it -
			// see FLICK_MS - before he is walked on from there.)
			const step = m.legs.find(
				(l) =>
					(l.move === "drive" || l.move === "engage") &&
					l.t0 < this.input.snap + FIRST_STEP,
			);
			const from = Math.max(
				this.input.snap + FIRST_STEP,
				step ? step.t0 + FLICK_MS : 0,
			);
			const pushing = (l?: Leg) =>
				l !== undefined &&
				(l.move === "drive" || l.move === "engage") &&
				l.t0 >= from;
			const free: [number, number][] = [];
			const runs: [number, number][] = [];
			for (const [i, l] of m.legs.entries()) {
				const was = runs.at(-1);
				if (!pushing(l)) {
					continue;
				}
				if (was && pushing(m.legs[i - 1]) && was[1] === l.t0) {
					was[1] = l.t1;
				} else {
					runs.push([l.t0, l.t1]);
				}
			}
			for (const [t0, t1] of runs) {
				const steps = Math.max(1, Math.ceil((t1 - t0) / 50));
				const on: [number, number][] = [];
				for (let k = 0; k <= steps; k++) {
					const t = t0 + ((t1 - t0) * k) / steps;
					if (!by(t)) {
						continue;
					}
					const was = on.at(-1);
					if (was && t - was[1] <= 100) {
						was[1] = t;
					} else {
						on.push([t, t]);
					}
				}
				const held = on.filter(([a, b]) => a === t0 || b - a >= AIR_PASS_MS);
				if (held.length === 0) {
					// (Not the last moment of his first step on its own - see
					// FIRST_STEP: a blink of something else between it and what
					// he does next.)
					const before = m.legs.find((l) => l.t1 === t0);
					if (
						t1 - t0 < AIR_LEAST_MS &&
						(before?.move === "drive" || before?.move === "engage")
					) {
						continue;
					}
					free.push([t0, t1]);
					continue;
				}
				const gaps: [number, number][] = [
					[t0, held[0]![0]],
					...held.slice(1).map((h, i): [number, number] => [held[i]![1], h[0]]),
					[held.at(-1)![1], t1],
				];
				for (const [a, b] of gaps) {
					const lo = a === t0 ? a : a + AIR_AFTER_MS;
					const hi = b === t1 ? b : b - AIR_AFTER_MS;
					if (hi - lo < AIR_LEAST_MS) {
						continue;
					}
					m.split(lo);
					m.split(hi);
					free.push([lo, hi]);
				}
			}
			const air = m.legs.map(
				(l) =>
					(l.move === "drive" || l.move === "engage") &&
					l.t0 >= from &&
					free.some(([a, b]) => l.t0 >= a && l.t1 <= b),
			);
			const eases: {
				t0: number;
				t1: number;
				a: Pt;
				b: Pt;
				f: (u: number) => number;
				regait?: boolean;
			}[] = [];
			// (A run of them together the one way, at the one pace: not
			// swaying on with the push he had into nobody, a step a walk and
			// the next a jog.)
			for (let i = 0; i < m.legs.length;) {
				let j = i + 1;
				while (
					air[i] &&
					j < m.legs.length &&
					air[j] &&
					m.legs[j]!.t0 === m.legs[j - 1]!.t1
				) {
					j++;
				}
				// (Not on into a pile, low as he is: down into it from there.)
				const next = m.legs[j];
				if (
					air[i] &&
					!(next && next.t0 === m.legs[j - 1]!.t1 && DOWN.has(next.move))
				) {
					const run = m.legs.slice(i, j);
					const a = run[0]!.from;
					const b = run.at(-1)!.to;
					const t0 = run[0]!.t0;
					const span = Math.max(1, run.at(-1)!.t1 - t0);
					const fps = dist(a, b) / (span / 1000);
					// (A blocker stays in his set, hands up, looking for
					// somebody to block - not stood, nor on at a crawl of a
					// walk.)
					const move =
						fps >= WALK_UNDER
							? "jog"
							: m.team === this.input.offense
								? "passSet"
								: fps < 1
									? "stand"
									: "walk";
					const on = (t: number) => ({
						x: a.x + ((b.x - a.x) * (t - t0)) / span,
						y: a.y + ((b.y - a.y) * (t - t0)) / span,
					});
					for (const l of run) {
						l.from = on(l.t0);
						l.to = on(l.t1);
						l.move = move;
					}
					// (At a jog, and stood after it: slowed into that over all
					// of it - up to it from a stand too, if stood before - not
					// stopped dead off it.)
					const prev = m.legs[i - 1];
					const still = (l?: Leg) =>
						l !== undefined &&
						moveKind(l.move) === "still" &&
						l.from.x === l.to.x &&
						l.from.y === l.to.y;
					// (In off a run quicker than that eases him down from -
					// through a man and up, say: down from the pace he came in
					// at, stood when that has him stopped - not dropped to a
					// jog, or a walk, in a stride. In off one slower than that:
					// on from the pace he came in at, not set off again from a
					// stand - up from a stand only if stood before.)
					const v0 =
						prev && prev.t1 === t0 && !still(prev) && prev.t1 > prev.t0
							? paceOn(prev, prev.to, b)
							: 0;
					if (
						(move === "jog" || (move === "walk" && v0 >= STOP_LEAST)) &&
						next?.t0 === run.at(-1)!.t1 &&
						still(next)
					) {
						const D = dist(a, b);
						const S = span / 1000;
						const ts = v0 > 0 && D > 0 ? (2 * D) / v0 : Infinity;
						const m0 = D > 0 ? (v0 * S) / D : 0;
						eases.push({
							t0,
							t1: t0 + span,
							a,
							b,
							f:
								ts < S
									? (u) => {
											const t = Math.min(u * S, ts);
											return (v0 * t - (v0 * t * t) / (2 * ts)) / D;
										}
									: (u) => u * u * (3 - 2 * u) + m0 * u * (1 - u) * (1 - u),
							regait: ts < S || v0 >= WALK_UNDER,
						});
					}
				}
				i = j;
			}
			for (const e of eases) {
				for (
					let t = e.t0 + STOP_STEP_MS;
					t < e.t1 - STOP_STEP_MS / 2;
					t += STOP_STEP_MS
				) {
					m.split(t);
				}
				const at = (t: number) => {
					const u = e.f(clamp((t - e.t0) / (e.t1 - e.t0), 0, 1));
					return {
						x: e.a.x + (e.b.x - e.a.x) * u,
						y: e.a.y + (e.b.y - e.a.y) * u,
					};
				};
				for (const l of m.legs) {
					if (l.t0 >= e.t0 && l.t1 <= e.t1) {
						l.from = at(l.t0);
						l.to = at(l.t1);
						if (e.regait && l.t1 > l.t0) {
							const v = legPace(l);
							l.move = v < 1 ? "stand" : gait(v);
						}
					}
				}
			}
		}
	}

	// Two of a side in a block in the one spot - two men on one blocker,
	// two blockers on one man, or two blocks run into one another - spread
	// shoulder to shoulder: each eased half the way out (TWIN_APART) to his
	// side, square to the way they face (not on into the man in front of
	// them), while they are at it. Two of different sides the same, in two
	// blocks run into one another - a man up onto a linebacker through
	// another block, say - neither facing the other (see TWIN_FACING): not
	// two men in a block with one another.
	noTwins() {
		const { snap } = this.input;
		const men = this.men.filter((m) => m.legs.length > 0);
		const end = Math.max(...men.map((m) => m.t));
		// (Where he is free to be somewhere else: blocking, on his way, or
		// stood about - not with the ball, not before the snap.)
		const movable = (l: Leg) =>
			l.t0 >= snap &&
			!l.ball &&
			!l.turn &&
			(LINE_PLAY.has(l.move) ||
				ROUNDS.has(l.move) ||
				(!touching(l) &&
					l.from.x === l.to.x &&
					l.from.y === l.to.y &&
					moveKind(l.move) !== "act" &&
					moveKind(l.move) !== "cycle"));
		const stretch = (m: Actor, t: number): [number, number] => {
			const legs = m.legs;
			let k = Math.max(0, legIndexAt(legs, t));
			let j = k;
			while (
				k > 0 &&
				movable(legs[k - 1]!) &&
				legs[k - 1]!.t1 === legs[k]!.t0
			) {
				k--;
			}
			while (
				j + 1 < legs.length &&
				movable(legs[j + 1]!) &&
				legs[j + 1]!.t0 === legs[j]!.t1
			) {
				j++;
			}
			return [legs[k]!.t0, legs[j]!.t1];
		};
		type Ease = { w0: number; a: number; b: number; w1: number; off: Pt };
		const eases = new Map<Actor, Ease[]>();
		const ease = (m: Actor, a: number, b0: number, off: Pt) => {
			const [s0, s1] = stretch(m, a);
			// (Stood once it is over, before it is eased off: back over as
			// he comes to a stand, if there is time for it then; if not, stood
			// where he is eased over to, and back as he next goes - not
			// stepped over and back stood there.)
			const still = (l: Leg) => l.from.x === l.to.x && l.from.y === l.to.y;
			const goes = m.legs.find(
				(l) =>
					l.t0 >= b0 + TWIN_EVERY_MS &&
					l.t0 < s1 - TWIN_EASE_LEAST &&
					!still(l),
			);
			const until = goes?.t0 ?? s1;
			const stands =
				m.legs.findLast((l) => l.t1 > b0 && l.t0 < until && !still(l))?.t1 ??
				b0;
			// (On the move as it comes on, or goes off: eased over the
			// longer it takes - not put on a spurt, or pulled up, by it.)
			const paceOver = (t0: number, t1: number) =>
				Math.max(
					0,
					...m.legs
						.filter((l) => l.t1 > t0 && l.t0 < t1 && l.t1 - l.t0 > 0)
						.map((l) => legPace(l)),
				);
			const runFor = (t0: number, t1: number) =>
				paceOver(t0, t1) >= TWIN_RUN_FPS
					? Math.max(
							TWIN_EASE_MS,
							Math.sqrt((6 * Math.hypot(off.x, off.y)) / TWIN_RUN_ACCEL) * 1000,
						)
					: TWIN_EASE_MS;
			let b = b0;
			let w1 = Math.min(s1, b0 + runFor(b0, b0 + TWIN_EASE_MS));
			if (stands < until && w1 > stands) {
				if (
					stands - b0 >=
					Math.max(
						TWIN_EASE_LEAST,
						(Math.hypot(off.x, off.y) / TWIN_BACK_FPS) * 1000,
					)
				) {
					w1 = stands;
				} else if (goes) {
					b = goes.t0;
					w1 = Math.min(s1, b + TWIN_EASE_MS);
				}
			}
			const w0 = Math.max(s0, a - runFor(a - TWIN_EASE_MS, a));
			const ea = Math.min(Math.max(a, w0 + TWIN_EASE_LEAST), (a + b) / 2);
			const eb = Math.max(Math.min(b, w1 - TWIN_EASE_LEAST), (a + b) / 2);
			// (Going flat out then: eased over by no more than takes him
			// past TWIN_TOP_FPS as he goes - not shot over and back.)
			const fps = Math.max(
				0,
				...m.legs
					.filter((l) => l.t1 > w0 && l.t0 < w1 && l.t1 - l.t0 > 0)
					.map((l) => dist(l.from, l.to) / ((l.t1 - l.t0) / 1000)),
			);
			const room =
				(Math.max(TWIN_EASE_FPS, TWIN_TOP_FPS - fps) *
					Math.min(ea - w0, w1 - eb)) /
				1000;
			const size = Math.hypot(off.x, off.y);
			const k = size > room ? room / size : 1;
			const e = {
				w0,
				a: ea,
				b: eb,
				w1,
				off: { x: off.x * k, y: off.y * k },
			};
			if (e.a > e.w0 && e.b < e.w1) {
				eases.set(m, [...(eases.get(m) ?? []), e]);
			}
		};
		// (Where each is every TWIN_EVERY_MS from the snap, if blocking then
		// without the ball.)
		const times: number[] = [];
		for (let t = snap; t <= end; t += TWIN_EVERY_MS) {
			times.push(t);
		}
		const grids = men.map((m) => {
			let k = 0;
			return times.map((t) => {
				while (k + 1 < m.legs.length && m.legs[k + 1]!.t0 <= t) {
					k++;
				}
				const l = m.legs[k]!;
				if (!LINE_PLAY.has(l.move) || l.ball) {
					return undefined;
				}
				const yaw =
					l.yaw ??
					(l.from.x !== l.to.x || l.from.y !== l.to.y
						? Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x)
						: 0);
				return { at: m.posAt(t), yaw };
			});
		});
		// (Each man's first and last time in it at all: two can only be at it
		// together between the later of the firsts and the earlier of the
		// lasts.)
		const spans = grids.map((g) => [
			g.findIndex((x) => x !== undefined),
			g.findLastIndex((x) => x !== undefined),
		]);
		for (let i = 0; i < men.length; i++) {
			for (let j = i + 1; j < men.length; j++) {
				const k0 = Math.max(spans[i]![0]!, spans[j]![0]!);
				const k1 = Math.min(spans[i]![1]!, spans[j]![1]!);
				if (spans[i]![0]! < 0 || spans[j]![0]! < 0 || k0 > k1) {
					continue;
				}
				const p = men[i]!;
				const q = men[j]!;
				const cross = p.team !== q.team;
				const gp = grids[i]!;
				const gq = grids[j]!;
				// (Spells of it: when, how q is off p each time, and the way
				// square to p's facing, all told.)
				let spell: { a: number; b: number; seps: Pt[]; side: Pt } | undefined;
				// (Of two sides in a block with each other, in each other: when,
				// and how q is off p each time.)
				let clash: { a: number; b: number; seps: Pt[] } | undefined;
				const clashed = () => {
					if (!clash) {
						return;
					}
					const { a, b, seps } = clash;
					clash = undefined;
					const mx = seps.reduce((sum, d) => sum + d.x, 0);
					const my = seps.reduce((sum, d) => sum + d.y, 0);
					const len = Math.hypot(mx, my);
					if (b <= a || len === 0) {
						return;
					}
					// (Back off each other along the way between them, as far
					// as it takes.)
					const ux = mx / len;
					const uy = my / len;
					const need = Math.max(
						...seps.map((d) => CLASH_APART - (d.x * ux + d.y * uy)),
					);
					const half = Math.min(TWIN_MOST, need / 2);
					if (half <= 0) {
						return;
					}
					ease(p, a, b, { x: -ux * half, y: -uy * half });
					ease(q, a, b, { x: ux * half, y: uy * half });
				};
				const done = () => {
					if (!spell) {
						return;
					}
					const { a, b, seps, side } = spell;
					spell = undefined;
					const len = Math.hypot(side.x, side.y);
					if (b <= a || len === 0) {
						return;
					}
					// (Out to the sides they are on, as far as it takes each
					// time: q on p's left if more often there than not.)
					const nx = side.x / len;
					const ny = side.y / len;
					const on = seps.reduce((sum, d) => sum + d.x * nx + d.y * ny, 0);
					const sx = on >= 0 ? nx : -nx;
					const sy = on >= 0 ? ny : -ny;
					let need = 0;
					for (const d of seps) {
						const f = d.x * sy - d.y * sx;
						const l = d.x * sx + d.y * sy;
						need = Math.max(
							need,
							Math.sqrt(Math.max(0, TWIN_APART ** 2 - f ** 2)) - l,
						);
					}
					const half = Math.min(TWIN_MOST, need / 2);
					if (half <= 0) {
						return;
					}
					ease(p, a, b, { x: -sx * half, y: -sy * half });
					ease(q, a, b, { x: sx * half, y: sy * half });
				};
				for (let k = k0; k <= k1; k++) {
					const t = times[k]!;
					const gpk = gp[k];
					const gqk = gq[k];
					if (!gpk || !gqk) {
						continue;
					}
					const d = dist(gpk.at, gqk.at);
					if (d >= TWIN_APART) {
						continue;
					}
					if (cross && d > 0) {
						const ux = (gqk.at.x - gpk.at.x) / d;
						const uy = (gqk.at.y - gpk.at.y) / d;
						if (
							Math.cos(gpk.yaw) * ux + Math.sin(gpk.yaw) * uy > TWIN_FACING ||
							-(Math.cos(gqk.yaw) * ux + Math.sin(gqk.yaw) * uy) > TWIN_FACING
						) {
							if (d < CLASH_APART) {
								if (clash && t - clash.b > TWIN_JOIN_MS) {
									clashed();
								}
								const sep = { x: d * ux, y: d * uy };
								if (clash) {
									clash.b = t;
									clash.seps.push(sep);
								} else {
									clash = { a: t, b: t, seps: [sep] };
								}
							}
							continue;
						}
					}
					if (spell && t - spell.b > TWIN_JOIN_MS) {
						done();
					}
					const sep = { x: gqk.at.x - gpk.at.x, y: gqk.at.y - gpk.at.y };
					const sx = -Math.sin(gpk.yaw);
					const sy = Math.cos(gpk.yaw);
					if (spell) {
						spell.b = t;
						spell.seps.push(sep);
						spell.side.x += sx;
						spell.side.y += sy;
					} else {
						spell = { a: t, b: t, seps: [sep], side: { x: sx, y: sy } };
					}
				}
				done();
				clashed();
			}
		}
		// The man with the ball: none of his own side in him as he comes by
		// - eased off his way, to the side they are on, all of it theirs (not
		// as he gets it, nor as he goes down, nor off their feet).
		this.ball.forEach((seg, k) => {
			const c = seg.kind === "held" ? this.man(seg.pid) : undefined;
			if (!c || seg.kind !== "held") {
				return;
			}
			const t0 = Math.max(seg.t0 + CARRY_CLEAR_SKIP_MS, snap);
			const t1 =
				Math.min(this.ball[k + 1]?.t0 ?? end, this.whistle, end) -
				CARRY_CLEAR_SKIP_MS;
			for (const m of men) {
				if (m === c || m.team !== c.team) {
					continue;
				}
				// (To the one side - the one he is on as the man with the ball
				// comes nearest - if the man with the ball comes through him:
				// not eased on along his way, the sides he is on as he comes and
				// as he goes on taken out by each other.)
				let spell:
					| {
							a: number;
							b: number;
							nx: number;
							ny: number;
							need: number;
							side: number;
					  }
					| undefined;
				const eased = () => {
					if (spell && spell.b > spell.a) {
						const len = Math.hypot(spell.nx, spell.ny);
						if (len > 0) {
							const h = Math.min(CARRY_CLEAR_MOST, spell.need) * spell.side;
							ease(m, spell.a, spell.b, {
								x: (spell.nx / len) * h,
								y: (spell.ny / len) * h,
							});
						}
					}
					spell = undefined;
				};
				// (Nor the back in on the fake of a handoff: meant to be in on
				// him, as he would be for the real one.)
				const fake = m.legs.filter((l) => l.arms === "fakeCarry");
				const fakeFrom = (fake[0]?.t0 ?? Infinity) - CARRY_CLEAR_SKIP_MS;
				const fakeTo = (fake.at(-1)?.t1 ?? -Infinity) + CARRY_CLEAR_SKIP_MS;
				for (let t = t0; t <= t1; t += TWIN_EVERY_MS) {
					const l = legAt(m, t);
					const p = c.posAt(t);
					const q = m.posAt(t);
					const d = dist(p, q);
					if (
						d >= CARRY_APART ||
						!movable(l) ||
						DOWN.has(l.move) ||
						(t >= fakeFrom && t <= fakeTo)
					) {
						continue;
					}
					if (spell && t - spell.b > TWIN_JOIN_MS) {
						eased();
					}
					const a = c.posAt(t - 50);
					const b = c.posAt(t + 50);
					const run = dist(a, b);
					const ux =
						run > 0.05 ? (b.x - a.x) / run : (q.x - p.x) / Math.max(d, 1e-6);
					const uy =
						run > 0.05 ? (b.y - a.y) / run : (q.y - p.y) / Math.max(d, 1e-6);
					const along = (q.x - p.x) * ux + (q.y - p.y) * uy;
					const across = (q.x - p.x) * -uy + (q.y - p.y) * ux;
					const sign = across >= 0 ? 1 : -1;
					const need =
						Math.sqrt(Math.max(0, CARRY_APART ** 2 - along ** 2)) -
						Math.abs(across);
					if (need <= 0) {
						continue;
					}
					if (spell) {
						spell.b = t;
						spell.nx += -uy;
						spell.ny += ux;
						if (need > spell.need) {
							spell.need = need;
							spell.side = sign;
						}
					} else {
						spell = { a: t, b: t, nx: -uy, ny: ux, need, side: sign };
					}
				}
				eased();
			}
		});
		for (const [m, es] of eases) {
			const at = (t: number): Pt => {
				let x = 0;
				let y = 0;
				for (const e of es) {
					const w = bumpOf(t, e.w0, e.a, e.b, e.w1);
					x += e.off.x * w;
					y += e.off.y * w;
				}
				return { x, y };
			};
			for (const e of es) {
				m.split(e.w0);
				m.split(e.w1);
				for (let t = e.w0 + DOWN_STEP_MS; t < e.w1; t += DOWN_STEP_MS) {
					const l = legAt(m, t);
					if (
						l.t1 - l.t0 > DOWN_STEP_MS * 1.5 &&
						t - l.t0 >= DOWN_SLIVER_MS &&
						l.t1 - t >= DOWN_SLIVER_MS
					) {
						m.split(t);
					}
				}
			}
			const lo = Math.min(...es.map((e) => e.w0));
			const hi = Math.max(...es.map((e) => e.w1));
			const legs = m.legs.filter((l) => l.t1 > lo && l.t0 < hi);
			const moved = legs.map((l) => {
				const d0 = at(l.t0);
				const d1 = at(l.t1);
				return {
					from: { x: l.from.x + d0.x, y: l.from.y + d0.y },
					to: { x: l.to.x + d1.x, y: l.to.y + d1.y },
				};
			});
			legs.forEach((l, k) => {
				l.from = moved[k]!.from;
				l.to = moved[k]!.to;
			});
			// (Stood about, and moved off where he stood by it: stepped over,
			// not slid there still stood.)
			const stepped = new Set<Leg>();
			for (const l of legs) {
				const fps =
					l.t1 > l.t0 ? dist(l.from, l.to) / ((l.t1 - l.t0) / 1000) : 0;
				if (moveKind(l.move) !== "still" || fps < TWIN_STEP_FPS) {
					continue;
				}
				const was = m.legs[m.legs.indexOf(l) - 1];
				const yaw = l.yaw ?? (was ? yawOf(was, m.yaw) : m.yaw);
				const way = faced(gait(fps), yaw, l.from, l.to, fps, was);
				l.move = way.move;
				if (way.yaw === undefined) {
					delete l.yaw;
				} else {
					l.yaw = way.yaw;
				}
				stepped.add(l);
			}
			// (A sliver of the first or last of it, easing into it or out of
			// it, in the step it is part of: not a walk for a moment into a
			// shuffle.)
			for (const [k, l] of legs.entries()) {
				if (!stepped.has(l) || l.t1 - l.t0 >= TWIN_SLIVER_MS) {
					continue;
				}
				const next = legs[k + 1];
				const prev = legs[k - 1];
				const along = (o: Leg) =>
					(o.to.x - o.from.x) * (l.to.x - l.from.x) +
						(o.to.y - o.from.y) * (l.to.y - l.from.y) >
					0;
				// (Or, the last of it as he comes to a stand out of a block,
				// say, in what he came out of: not a walk for a moment between
				// the two.)
				const like =
					[next, prev].find(
						(o) =>
							o !== undefined &&
							stepped.has(o) &&
							o.t1 - o.t0 >= TWIN_SLIVER_MS &&
							(o === next ? o.t0 === l.t1 : o.t1 === l.t0) &&
							along(o),
					) ??
					(prev !== undefined &&
					!stepped.has(prev) &&
					prev.t1 === l.t0 &&
					moveKind(prev.move) !== "still" &&
					moveKind(prev.move) !== "act" &&
					along(prev)
						? prev
						: undefined);
				if (like) {
					l.move = like.move;
					if (like.yaw === undefined) {
						delete l.yaw;
					} else {
						l.yaw = like.yaw;
					}
				}
			}
		}
	}

	// A man on his feet with his way over a man down on the turf - across
	// him, or close by the length of him - bent out round him instead (see
	// DOWN_CLEAR): off to the side, round the nearer end of him first, or
	// pulled up short of him, by as little as clears him and anybody else
	// down there, eased out to it and back in over a few strides, his way
	// either side of it as it was - or, stood there for the rest of it,
	// stood that much off him. Stood on him to begin with, a step off him.
	// Not with the ball, nor at the moment it comes to him or leaves him;
	// nor where he has to be by him (going down on him, up against
	// somebody).
	offMenDown() {
		const { snap } = this.input;
		const spells: {
			m: Actor;
			t0: number;
			t1: number;
			yaw: number;
			back: number;
		}[] = [];
		for (const m of this.men) {
			let yaw = m.legs[0]?.yaw ?? 0;
			m.legs.forEach((l, k) => {
				const piled =
					PILE_GOING.has(l.move) ||
					PILE_DOWN.has(l.move) ||
					PILE_RISING.has(l.move);
				const rising = DOWN_RISING.has(l.move) || PILE_RISING.has(l.move);
				const t0 =
					DOWN_ON.has(l.move) || PILE_DOWN.has(l.move) || rising
						? l.t0
						: DOWN_GOING.has(l.move) || PILE_GOING.has(l.move)
							? l.t0 + (l.t1 - l.t0) * DOWN_FROM
							: undefined;
				if (t0 !== undefined && l.t1 > snap) {
					const a = l.yaw ?? yaw;
					spells.push({
						m,
						t0,
						t1: rising
							? l.t0 + (l.t1 - l.t0) * DOWN_RISING_TILL
							: k === m.legs.length - 1
								? Infinity
								: l.t1,
						yaw: LIE_BACKWARD.has(l.move) ? a + Math.PI : a,
						back: rising ? LIE_BACK : piled ? PILE_LEGS : DOWN_LEGS,
					});
				}
				yaw = yawOf(l, yaw);
			});
		}
		if (spells.length === 0) {
			return;
		}
		// (How near `q` comes at t to anybody but m - or `but` - down then:
		// and the nearest of them, the middle of him, and the way straight
		// out off him to it, radians: square off his length, on it.)
		const nearDown = (m: Actor, t: number, q: Pt, but?: Actor) => {
			let near = Infinity;
			let mid: Pt | undefined;
			let out: number | undefined;
			let who: Actor | undefined;
			for (const s of spells) {
				if (s.m === m || s.m === but || t < s.t0 || t > s.t1) {
					continue;
				}
				const at = s.m.posAt(t);
				const ux = Math.cos(s.yaw);
				const uy = Math.sin(s.yaw);
				const along = clamp(
					(q.x - at.x) * ux + (q.y - at.y) * uy,
					-s.back,
					LIE_HEAD,
				);
				const ex = q.x - at.x - ux * along;
				const ey = q.y - at.y - uy * along;
				const d = Math.hypot(ex, ey);
				if (d < near) {
					near = d;
					const half = (LIE_HEAD - s.back) / 2;
					mid = { x: at.x + ux * half, y: at.y + uy * half };
					out =
						d > 0.05
							? Math.atan2(ey, ex)
							: s.yaw + (ex * -uy + ey * ux >= 0 ? 1 : -1) * (Math.PI / 2);
					who = s.m;
				}
			}
			return { near, mid, out, who };
		};
		// (Whether `q` at t takes m nearer anybody on his feet than
		// PASS_APART, and nearer him than `p` - where he was to be - did.)
		// (Where the others on their feet are at t, kept while it is m's
		// way that is bent - nobody else's changes meanwhile.)
		let upFor: Actor | undefined;
		const up = new Map<number, Pt[]>();
		const crowds = (m: Actor, t: number, q: Pt, p: Pt) => {
			if (upFor !== m) {
				upFor = m;
				up.clear();
			}
			let them = up.get(t);
			if (!them) {
				them = this.men
					.filter(
						(o) =>
							o !== m &&
							!spells.some((s) => s.m === o && t >= s.t0 && t <= s.t1),
					)
					.map((o) => o.posAt(t));
				up.set(t, them);
			}
			return them.some((at) => {
				const d = dist(q, at);
				return d < PASS_APART && d < dist(p, at) - PASS_SLACK;
			});
		};
		// (When, from a to b, a man comes down on m where he stands - clear of
		// anybody down as he stood there, with STEP_OFF_LEAST_MS of it left.)
		const landsOn = (m: Actor, a: number, b: number) => {
			const { legs } = m;
			for (let i = 0; i < legs.length; i++) {
				const l = legs[i]!;
				if (l.t1 <= a || l.t0 >= b) {
					continue;
				}
				if (l.from.x !== l.to.x || l.from.y !== l.to.y) {
					continue;
				}
				// (Stood there over more than one leg - easy, getting his
				// breath, say: as one.)
				const same = (n: Leg) =>
					n.from.x === l.from.x &&
					n.from.y === l.from.y &&
					n.to.x === l.from.x &&
					n.to.y === l.from.y;
				while (
					i + 1 < legs.length &&
					legs[i + 1]!.t0 === legs[i]!.t1 &&
					same(legs[i + 1]!)
				) {
					i++;
				}
				const s0 = Math.max(l.t0, a);
				const till = Math.min(legs[i]!.t1, b) - STEP_OFF_LEAST_MS;
				if (till <= s0 || nearDown(m, s0, l.from).near < DOWN_CLEAR) {
					continue;
				}
				for (let t = s0 + DOWN_EVERY_MS; t <= till; t += DOWN_EVERY_MS) {
					if (nearDown(m, t, l.from).near < DOWN_CLEAR) {
						return t;
					}
				}
			}
			return undefined;
		};
		// (Free to go another way, or to stand somewhere else: on his way,
		// or stood about - at something of his own where he stands, a fist
		// pumped, say - nobody's to mind - not with the ball, not on
		// anybody; or driving on at nobody on his feet there, into a man
		// gone down.)
		const free = (m: Actor, l: Leg) =>
			l.t0 >= snap &&
			!l.ball &&
			!l.turn &&
			(l.move === "drive" && !l.arms
				? l.t1 > l.t0 && !this.onSomebody(m, l)
				: !touching(l) &&
					(l.from.x === l.to.x && l.from.y === l.to.y
						? moveKind(l.move) !== "cycle"
						: ROUNDS.has(l.move) && l.t1 > l.t0));
		for (const m of this.men) {
			// (Where he is when the ball comes to him or leaves him, left be.)
			const fixed: number[] = [];
			this.ball.forEach((b, k) => {
				if (b.kind === "held" && b.pid === m.pid) {
					fixed.push(b.t0, this.ball[k + 1]?.t0 ?? Infinity);
				}
			});
			for (let i = 0; i < m.legs.length;) {
				if (!free(m, m.legs[i]!)) {
					i++;
					continue;
				}
				let j = i + 1;
				while (
					j < m.legs.length &&
					free(m, m.legs[j]!) &&
					m.legs[j]!.t0 === m.legs[j - 1]!.t1
				) {
					j++;
				}
				const c0 = m.legs[i]!.t0;
				const c1 = m.legs[j - 1]!.t1;
				const open = j === m.legs.length;
				const cuts = [
					c0,
					...fixed.filter((t) => t > c0 && t < c1).sort((a, b) => a - b),
					c1,
				];
				for (let k = 0; k + 1 < cuts.length; k++) {
					const a = cuts[k]!;
					const b = cuts[k + 1]!;
					const last = open && k + 2 === cuts.length;
					// (Stood there clear, and a man brought down at his feet,
					// onto them: from then on as stood on him to begin with -
					// a step off him.)
					const lands = landsOn(m, a, b);
					if (lands === undefined) {
						this.roundDown(m, a, b, last, nearDown, crowds);
					} else {
						m.split(lands);
						this.roundDown(m, a, lands, false, nearDown, crowds);
						this.roundDown(m, lands, b, last, nearDown, crowds);
					}
				}
				const next = m.legs.findIndex((l) => l.t0 >= c1);
				if (next < 0) {
					break;
				}
				i = next;
			}
		}
	}

	// (A man on his way in `l` up against one of them on his feet: within
	// DRIVE_ON of him at some time in it.)
	private onSomebody(m: Actor, l: Leg): boolean {
		const them = m.team === this.input.offense ? this.d : this.o;
		const steps = Math.max(1, Math.ceil((l.t1 - l.t0) / DOWN_STEP_MS));
		for (let k = 0; k <= steps; k++) {
			const t = l.t0 + ((l.t1 - l.t0) * k) / steps;
			const at = m.posAt(t);
			if (
				them.some(
					(o) => dist(o.posAt(t), at) < DRIVE_ON && !DOWN.has(legAt(o, t).move),
				)
			) {
				return true;
			}
		}
		return false;
	}

	// (See offMenDown: where he goes and stands from c0 to c1, out of the
	// way of anybody down by it - and left out of it at the end, `open`,
	// there for the rest of it.) Out to the side and back while he is on
	// his way, not while he stands: by his clock on the move. Or, stood on
	// a man to begin with (up against him as he went down), a step or two
	// off him first.
	private roundDown(
		m: Actor,
		c0: number,
		c1: number,
		open: boolean,
		nearDown: (
			m: Actor,
			t: number,
			q: Pt,
			but?: Actor,
		) => { near: number; mid?: Pt; out?: number; who?: Actor },
		crowds: (m: Actor, t: number, q: Pt, p: Pt) => boolean,
	) {
		// (Stood there over more than one leg alike - split up along the
		// way: as the one.)
		const k0 = m.legs.findIndex((l) => l.t0 === c0);
		const l0 = m.legs[k0];
		if (l0 && l0.from.x === l0.to.x && l0.from.y === l0.to.y) {
			const like = (l: Leg) => JSON.stringify({ ...l, t0: 0, t1: 0 });
			const was = like(l0);
			let k = k0 + 1;
			while (
				k < m.legs.length &&
				m.legs[k]!.t0 === m.legs[k - 1]!.t1 &&
				m.legs[k]!.t1 <= c1 &&
				like(m.legs[k]!) === was
			) {
				k++;
			}
			if (k > k0 + 1) {
				l0.t1 = m.legs[k - 1]!.t1;
				m.legs.splice(k0 + 1, k - k0 - 1);
			}
		}
		const lead = m.legs.find((l) => l.t0 === c0);
		const stood = lead ? Math.min(lead.t1, c1) - c0 : 0;
		const stepTo =
			lead &&
			lead.from.x === lead.to.x &&
			lead.from.y === lead.to.y &&
			stood >= STEP_OFF_LEAST_MS &&
			nearDown(m, c0, lead.from).near < DOWN_CLEAR
				? c0 + (stood >= STEP_OFF_MS + STEP_OFF_LEFT_MS ? STEP_OFF_MS : stood)
				: undefined;
		const stepMs = stepTo === undefined ? 0 : stepTo - c0;
		const stoodTill = lead ? Math.min(lead.t1, c1) : c0;
		const runs: { t0: number; t1: number; at: number }[] = [];
		let T = 0;
		if (stepTo !== undefined) {
			runs.push({ t0: c0, t1: stepTo, at: 0 });
			T = stepMs;
		}
		for (const l of m.legs) {
			if (l.t1 <= c0 || l.t0 >= c1) {
				continue;
			}
			if (l.from.x !== l.to.x || l.from.y !== l.to.y) {
				const t0 = Math.max(l.t0, c0);
				const t1 = Math.min(l.t1, c1);
				runs.push({ t0, t1, at: T });
				T += t1 - t0;
			}
		}
		if (T <= 0) {
			return;
		}
		// (Where on his clock he stops a while, and goes on again.)
		const pauses = runs
			.filter((r, k) => k > 0 && r.t0 - runs[k - 1]!.t1 >= DOWN_PAUSE_MS)
			.map((r) => r.at);
		const clock = (t: number) => {
			for (let k = runs.length - 1; k >= 0; k--) {
				const r = runs[k]!;
				if (t >= r.t0) {
					return r.at + Math.min(t, r.t1) - r.t0;
				}
			}
			return 0;
		};
		const timeAt = (u: number) => {
			for (const r of runs) {
				if (u <= r.at + (r.t1 - r.t0)) {
					return r.t0 + Math.max(0, u - r.at);
				}
			}
			return c1;
		};
		const spans: [number, number][] = [];
		for (let t = c0; t <= c1; t += DOWN_EVERY_MS) {
			if (nearDown(m, t, m.posAt(t)).near < DOWN_CLEAR) {
				const last = spans.at(-1);
				if (last && t - last[1] <= DOWN_JOIN_MS) {
					last[1] = t;
				} else {
					spans.push([t, t]);
				}
			}
		}
		for (const [first, last] of spans) {
			const stepping = stepTo !== undefined && first === c0;
			const ta = stepping ? stepTo : Math.max(c0, first - DOWN_EVERY_MS);
			const tb = Math.max(ta, Math.min(c1, last + DOWN_EVERY_MS));
			const ua = clock(ta);
			const ub = clock(tb);
			const u0 = stepping ? ua : Math.max(0, ua - DOWN_RAMP_MS);
			const u1 = Math.min(T, ub + DOWN_RAMP_MS);
			const pace =
				u1 > u0
					? dist(m.posAt(timeAt(u0)), m.posAt(timeAt(u1))) / ((u1 - u0) / 1000)
					: 0;
			if (pace < DOWN_PACE_LEAST && !(stepping && open)) {
				continue;
			}
			const across = Math.max(
				DOWN_PACE_LEAST,
				Math.min(DOWN_ACROSS_FPS, pace * DOWN_ACROSS),
			);
			let was = Infinity;
			for (let t = ta; t <= tb; t += DOWN_EVERY_MS) {
				was = Math.min(was, nearDown(m, t, m.posAt(t)).near);
			}
			// (Which way he is going there - and out to the side of it away
			// from the middle of the man there first, the other side, or
			// back short of him, square or slant. Stepping off a man, straight
			// out off him, or slant, or square.)
			const tm = (ta + tb) / 2;
			const pm = m.posAt(tm);
			const mid = nearDown(m, tm, pm).mid;
			let ways: Pt[];
			// (Stepping off him, the man he is on: kept clear of anybody else
			// down by him as he does.)
			const on = stepping ? nearDown(m, c0, m.posAt(c0)) : undefined;
			if (stepping) {
				const a = on?.out ?? m.yaw;
				ways = STEP_OFF_TURNS.map((r) => ({
					x: Math.cos(a + r),
					y: Math.sin(a + r),
				}));
			} else {
				const pa = m.posAt(timeAt(Math.max(0, ua - DOWN_WAY_MS)));
				const pb = m.posAt(timeAt(Math.min(T, ub + DOWN_WAY_MS)));
				const len = dist(pa, pb);
				if (len < DOWN_WAY_LEAST) {
					continue;
				}
				const fx = (pb.x - pa.x) / len;
				const fy = (pb.y - pa.y) / len;
				const away =
					mid && (pm.x - mid.x) * -fy + (pm.y - mid.y) * fx < 0 ? -1 : 1;
				ways = [
					[0, away],
					[0, -away],
					[-1, 0],
					[-Math.SQRT1_2, away * Math.SQRT1_2],
					[-Math.SQRT1_2, -away * Math.SQRT1_2],
				].map(([f, n]) => ({ x: f! * fx - n! * fy, y: f! * fy + n! * fx }));
			}
			type Pick = {
				near: number;
				wa: number;
				wb: number;
				ui: number;
				a1: number;
				b1: number;
				uo: number;
				off: Pt;
			};
			// (Clear of anybody on his feet as he goes, if there is a way
			// - and if not, any way: better in among men on their feet, for
			// a moment, than stood on a man down.)
			// (No quicker on his way for it than a man goes - DOWN_TOP_FPS -
			// back short of him and in again, say, flat out already.)
			const tooQuick = (
				off: Pt,
				wa: number,
				wb: number,
				ui: number,
				a1: number,
				b1: number,
				uo: number,
			) => {
				for (let t = wa; t + DOWN_SLIVER_MS < wb; t += DOWN_STEP_MS) {
					const t2 = Math.min(wb, t + DOWN_STEP_MS);
					const s = (t2 - t) / 1000;
					const w0 = bumpOf(clock(t), ui, a1, b1, uo);
					const w1 = bumpOf(clock(t2), ui, a1, b1, uo);
					const p0 = m.posAt(t);
					const p1 = m.posAt(t2);
					const v = Math.hypot(
						p1.x - p0.x + off.x * (w1 - w0),
						p1.y - p0.y + off.y * (w1 - w0),
					);
					if (v / s > Math.max(DOWN_TOP_FPS, dist(p0, p1) / s + 0.5)) {
						return true;
					}
				}
				return false;
			};
			const search = (careful: boolean): Pick | undefined => {
				let best: Pick | undefined;
				let fair: { h: number; pick: Pick } | undefined;
				for (const h of DOWN_OFFS) {
					if (
						(fair && h > fair.h + DOWN_FAIR_MORE) ||
						(stepping && h > (STEP_OFF_FPS * stepMs) / 1000)
					) {
						break;
					}
					// (Out to it no quicker across than he can go, over as much of
					// his way as there is - or, no way left after it to come back
					// in over, and nowhere he has to be, out there for good.)
					const need = Math.max(
						DOWN_RAMP_MS,
						(1500 * h) / across,
						(1000 * DOWN_TAPER_FT) / Math.max(pace, DOWN_PACE_LEAST),
					);
					const least = Math.max(
						DOWN_RAMP_MS * 0.6,
						(1500 * h) / (across * DOWN_PINCH),
					);
					// (On him from the first of it, or to the last, with no way
					// to ease out over before: out from the first, round him
					// part of the way only - into it at the last.)
					let a1 = ua;
					let b1 = ub;
					let rin = stepping ? ua : Math.min(need, ua);
					if (!stepping && rin < least) {
						a1 = Math.min(ub, least);
						rin = a1;
					}
					let rout = pace < DOWN_PACE_LEAST ? 0 : Math.min(need, T - ub);
					const stays = rout < least && open;
					if (rout < least && !open) {
						b1 = Math.max(a1, T - least);
						rout = T - b1;
					}
					if ((!stepping && rin < least) || (!stays && rout < least)) {
						continue;
					}
					// (Not back in over the last of his way into a stop, and on
					// after it: all of it after - not hurried into his stop.)
					const stop = stays
						? undefined
						: pauses.find((u) => u > b1 && u < b1 + rout);
					// (Or, with too little of it after, back in before his stop and
					// into it: not still coming across as he pulls up.)
					if (stop !== undefined && stop + rout <= T) {
						b1 = stop;
					} else if (stop !== undefined) {
						// (Quicker across than he would be, if that is all the time
						// there is before it - DOWN_RAMP_MS at least: not at his
						// quickest across as he stops.)
						const back = Math.max(a1, stop - rout);
						if (stop - back >= Math.min(least, DOWN_RAMP_MS)) {
							b1 = back;
							rout = stop - back;
						}
					}
					const ui = a1 - rin;
					const uo = stays ? Infinity : b1 + rout;
					const wa = timeAt(ui);
					const wb = stays ? c1 : timeAt(uo);
					for (const way of ways) {
						const off = { x: way.x * h, y: way.y * h };
						if (tooQuick(off, stepping ? c0 : wa, wb, ui, a1, b1, uo)) {
							continue;
						}
						let near = Infinity;
						for (
							let t = stepping ? c0 : wa;
							t <= wb && near >= (best?.near ?? 0);
							t += DOWN_EVERY_MS
						) {
							const w = bumpOf(clock(t), ui, a1, b1, uo);
							const p = m.posAt(t);
							const q = { x: p.x + off.x * w, y: p.y + off.y * w };
							// (Stepped off him, and back onto his way where it goes
							// by him anyway after he stood - over to help him up,
							// say: no nearer him there than he was to be. Not on
							// over him, his way across him: DOWN_SOME off him at
							// least.)
							const n = nearDown(m, t, q, t < ta ? on?.who : undefined).near;
							const by =
								stepping && t > stoodTill
									? n +
										DOWN_CLEAR -
										Math.min(
											DOWN_CLEAR,
											Math.max(DOWN_SOME, nearDown(m, t, p).near),
										)
									: n;
							// (Not into somebody else instead.)
							near =
								careful && w > 0 && crowds(m, t, q, p)
									? -1
									: Math.min(near, by);
						}
						if (!best || near > best.near) {
							best = { near, wa, wb, ui, a1, b1, uo, off };
						}
						if (near >= DOWN_CLEAR) {
							break;
						}
						if (!fair && near >= DOWN_FAIR) {
							fair = { h, pick: { near, wa, wb, ui, a1, b1, uo, off } };
						}
					}
					if (best && best.near >= DOWN_CLEAR) {
						break;
					}
				}
				if (fair && (!best || best.near < DOWN_CLEAR)) {
					best = fair.pick;
				}
				return best;
			};
			const ok = (p: Pick | undefined): p is Pick =>
				p !== undefined &&
				(p.near >= DOWN_CLEAR ||
					(p.near >= was + DOWN_GAIN && p.near >= DOWN_SOME));
			const careful = search(true);
			const best = ok(careful) ? careful : search(false);
			if (!ok(best)) {
				// (No stepping off him and back in time: on his way now, if it
				// takes him on after he stood - stood where it gets him for
				// the rest of it - not stood on him till then.)
				if (stepping && lead && this.onSooner(m, lead, c1)) {
					// (And round anybody down on his way, as he goes now.)
					const now = m.legs.find((l) => l.t0 === c0);
					if (now && (now.from.x !== now.to.x || now.from.y !== now.to.y)) {
						this.roundDown(m, c0, c1, open, nearDown, crowds);
					}
					return;
				}
				continue;
			}
			const { wa, wb, ui, a1, b1, uo, off } = best;
			// (Bent in steps: his legs there cut every DOWN_STEP_MS or so -
			// not into slivers.)
			m.split(wa);
			m.split(wb);
			for (const r of runs) {
				for (
					let t = Math.max(r.t0, wa) + DOWN_STEP_MS;
					t < Math.min(r.t1, wb);
					t += DOWN_STEP_MS
				) {
					const l = legAt(m, t);
					if (
						l.t1 - l.t0 > DOWN_STEP_MS * 1.5 &&
						t - l.t0 >= DOWN_SLIVER_MS &&
						l.t1 - t >= DOWN_SLIVER_MS
					) {
						m.split(t);
					}
				}
			}
			const legs = m.legs.filter((l) => l.t0 >= wa && l.t1 <= wb);
			const moved = legs.map((l) => {
				const w0 = bumpOf(clock(l.t0), ui, a1, b1, uo);
				const w1 = bumpOf(clock(l.t1), ui, a1, b1, uo);
				return {
					from: { x: l.from.x + off.x * w0, y: l.from.y + off.y * w0 },
					to: { x: l.to.x + off.x * w1, y: l.to.y + off.y * w1 },
				};
			});
			legs.forEach((l, k) => {
				l.from = moved[k]!.from;
				l.to = moved[k]!.to;
			});
			// (Off him in steps, facing the way he stood.)
			const steps = stepping
				? m.legs.filter((l) => l.t0 >= c0 && l.t1 <= stepTo)
				: [];
			if (steps.length > 0) {
				const a = steps[0]!;
				const b = steps.at(-1)!;
				const was = m.legs[m.legs.indexOf(a) - 1];
				const fps = dist(a.from, b.to) / (stepMs / 1000);
				const yaw = a.yaw ?? (was ? yawOf(was, m.yaw) : m.yaw);
				const way = faced(gait(fps), yaw, a.from, b.to, fps, was);
				for (const l of steps) {
					l.move = way.move;
					if (way.yaw === undefined) {
						delete l.yaw;
					} else {
						l.yaw = way.yaw;
					}
				}
			}
		}
	}

	// (See roundDown: the legs he goes on after he stands in `lead`, up to
	// c1, gone on at once - and that stand, as long, at the end of them.
	// Whether there were any.)
	private onSooner(m: Actor, lead: Leg, c1: number): boolean {
		const legs = m.legs;
		const i = legs.indexOf(lead);
		const moves: Leg[] = [];
		let t = lead.t1;
		for (let k = i + 1; k < legs.length && legs[k]!.t0 < c1; k++) {
			const l = legs[k]!;
			if (l.t0 !== t || l.t1 > c1) {
				return false;
			}
			moves.push(l);
			t = l.t1;
		}
		if (i < 0 || moves.length === 0 || lead.t1 > c1) {
			return false;
		}
		const sooner = lead.t1 - lead.t0;
		for (const l of moves) {
			l.t0 -= sooner;
			l.t1 -= sooner;
		}
		const end = moves.at(-1)!;
		legs.splice(i, 1);
		legs.splice(i + moves.length, 0, {
			...lead,
			t0: end.t1,
			t1: end.t1 + sooner,
			from: end.to,
			to: end.to,
			yaw: yawOf(end, lead.yaw ?? m.yaw),
		});
		return true;
	}

	// A foul before the snap: the man who jumped early - over the line, or
	// out of his stance - the flag, the whistle, and everybody up out of his
	// stance, a few walking off a step or two.
	deadBall(flagI: number) {
		const { snap, offense } = this.input;
		const r = this.r;
		const pen = this.find("penalty", flagI);
		const name = pen >= 0 ? this.ev(pen).penaltyName : undefined;
		const t = name === "Delay of game" ? snap + 300 : snap - 150 - r() * 100;
		const jumped = typeof name === "string" ? JUMP[name] : undefined;
		const who = this.man(this.statPids("pen")[0]);
		this.whistle = t + 400;
		for (const m of this.men) {
			m.cut(this.whistle);
			m.hold(this.whistle);
		}
		if (who && jumped !== undefined) {
			const way = who.team === offense ? 1 : -1;
			const s = this.of(who.posAt(t));
			who.cut(t);
			who.go(
				this.w({ d: s.d + way * jumped, a: s.a }),
				t + 250,
				"drive",
				way === 1 ? this.ahead : this.behind,
			);
			who.hold(this.whistle, "drive");
		}
		if (who && name === "Unsportsmanlike conduct") {
			this.jaw(who, t);
		}
		this.at.set(flagI, t);
		const spot = this.w({ d: 0, a: 0 });
		this.ball.push({ kind: "spot", t0: this.whistle, at: { ...spot, z: 0 } });
		for (const m of this.men) {
			const up = this.whistle + 150 + r() * 450;
			m.hold(up);
			if (r() < 0.5) {
				const s = this.of(m.at);
				const back = m.team === offense ? -1 : 1;
				const to = this.w({
					d: s.d + back * (2 + r() * 5),
					a: s.a + (r() - 0.5) * 4,
				});
				// (At a walk: no quicker than DEAD_WALK_FPS.)
				m.go(
					to,
					up +
						Math.max(900 + r() * 700, (dist(m.at, to) / DEAD_WALK_FPS) * 1000),
					"walk",
				);
			}
			m.hold(m.t + 400, "stand");
			this.done.add(m.pid);
		}
	}

	// OFFSIDES, THE PLAY RUN: the man flagged off a moment before the snap -
	// up out of his stance and a step across into the neutral zone, the ball
	// not yet gone - and on into the play from there when he would have gone
	// anyway, the step he has on it let go of as he goes on. Not a man off
	// the ball (a linebacker back off it), or on the move at the snap, or not
	// on forward off it soon (dropping off it, say). (No dice: see countOf.)
	offside() {
		const { snap, offense, events, i0, i1 } = this.input;
		let pen = -1;
		for (let i = i0; i < i1 && pen < 0; i++) {
			const e = events[i];
			if (e?.type === "penalty" && e.penaltyName === "Offsides") {
				pen = i;
			}
		}
		if (pen < 0) {
			return;
		}
		const who = this.man(this.statPidsIn(pen, i1, "pen")[0]);
		if (!who || who.team === offense) {
			return;
		}
		const legs = who.legs;
		const k = legIndexAt(legs, snap);
		const set = legs[k];
		const off = legs[k + 1];
		if (
			!set ||
			!off ||
			off.t0 !== set.t1 ||
			set.t1 <= snap ||
			moveKind(set.move) === "cycle" ||
			set.from.x !== set.to.x ||
			set.from.y !== set.to.y
		) {
			return;
		}
		const s = this.of(set.to);
		const t0 =
			snap - OFFSIDE_MS - OFFSIDE_SPREAD_MS * countOf(who, this.input.key, 44);
		if (s.d > OFFSIDE_NEAR || t0 - set.t0 < OFFSIDE_SET_MS) {
			return;
		}
		// (How far on forward he would have been off where he set - the
		// furthest yet, pushed back after: the step he has on that let go of
		// as he goes on, all of it (near enough) by the time he is
		// OFFSIDE_FADE_FT on, which is soon enough, on the move all the way.)
		const on = (p: Pt) => s.d - this.of(p).d;
		const left = (most: number) => clamp(1 - most / OFFSIDE_FADE_FT, 0, 1);
		const most: number[] = [];
		let n = k + 1;
		let far = 0;
		while (n < legs.length && left(far) > 0) {
			const l = legs[n]!;
			if (moveKind(l.move) !== "cycle" || l.t1 > off.t0 + OFFSIDE_FADE_MS) {
				if (left(far) > OFFSIDE_LEFT) {
					return;
				}
				break;
			}
			most.push(far);
			far = Math.max(far, on(l.to));
			n++;
		}
		const jump = this.w({ d: s.d - OFFSIDE_FT, a: s.a });
		const vx = jump.x - set.to.x;
		const vy = jump.y - set.to.y;
		for (let j = k + 1; j < n; j++) {
			const l = legs[j]!;
			const a = left(most[j - k - 1]!);
			const b = j + 1 < n ? left(most[j - k]!) : 0;
			l.from = { x: l.from.x + vx * a, y: l.from.y + vy * a };
			l.to = { x: l.to.x + vx * b, y: l.to.y + vy * b };
		}
		set.t1 = t0;
		legs.splice(k + 1, 0, {
			t0,
			t1: off.t0,
			from: set.to,
			to: jump,
			move: "drive",
			yaw: set.yaw ?? this.behind,
		});
	}

	// UNSPORTSMANLIKE BEFORE THE SNAP: the man flagged up out of where he is,
	// into the face of the nearest of the other side, and a shove - him up
	// out of his stance, back a step from it, round to him - the flag on it.
	// Not a man over the ball or at the kick; nobody near enough, nothing.
	// (No dice: see deadBall.)
	jaw(who: Actor, t: number) {
		if (JAW_NOT.has(who.role)) {
			return;
		}
		const shove = t - JAW_SHOVE_MS;
		const seen = who.posAt(shove);
		const foe = this.men
			.filter((o) => o.team !== who.team && !JAW_NOT.has(o.role))
			.sort(
				(a, b) => dist(a.posAt(shove), seen) - dist(b.posAt(shove), seen),
			)[0];
		if (!foe || dist(foe.posAt(shove), seen) > JAW_FAR) {
			return;
		}
		// (Off over to him as long before it as it takes - from where he is
		// then, to where the other man is when it comes.)
		const at = foe.posAt(shove - JAW_SET_MS - JAW_UP_MS);
		const spotFrom = (me: Pt): Pt => {
			const d = Math.max(0.01, dist(me, at));
			return d <= JAW_FACE_FT + JAW_STAY_FT
				? me
				: {
						x: at.x + ((me.x - at.x) / d) * JAW_FACE_FT,
						y: at.y + ((me.y - at.y) / d) * JAW_FACE_FT,
					};
		};
		let t0 = shove - JAW_SET_MS - JAW_LEAST_MS;
		for (let k = 0; k < 3; k++) {
			const me = who.posAt(t0);
			t0 =
				shove -
				JAW_SET_MS -
				Math.max(JAW_LEAST_MS, (dist(me, spotFrom(me)) / JAW_FPS) * 1000);
		}
		const me = who.posAt(t0);
		const spot = spotFrom(me);
		if (dist(me, spot) > JAW_FAR || t0 < this.input.snap - JAW_EARLY_MS) {
			return;
		}
		who.cut(t0);
		who.hold(t0);
		if (spot !== me) {
			who.go(spot, shove - JAW_SET_MS);
		}
		who.hold(shove - JAW_SET_MS / 2, "stand", this.toward(spot, at));
		who.hold(this.whistle, "stand", this.toward(spot, at));
		who.arms("shove", shove - JAW_SHOVE_ARMS_MS, shove + JAW_SHOVE_ARMS_MS);
		// (Him: up to it as he comes, and shoved back a step.)
		const face = this.toward(at, spot);
		const back = {
			x: at.x - Math.cos(face) * JAW_BACK_FT,
			y: at.y - Math.sin(face) * JAW_BACK_FT,
		};
		foe.cut(Math.max(t0, shove - JAW_SET_MS - JAW_UP_MS));
		foe.hold(shove - JAW_SET_MS, "stand", face);
		foe.hold(shove);
		foe.go(back, shove + JAW_BACK_MS, undefined, face);
		foe.hold(this.whistle, "stand", face);
	}

	// The ball drawn with whoever has it, the way what he is doing holds it
	// (in both hands, reaching for it): each man's legs split where he gets
	// it and where it leaves him.
	handBall() {
		const segs = this.ball;
		// (Nobody on with it the way he had it before the snap - the center
		// still in his stance on a knee - once the play has it elsewhere.)
		const first = segs[0];
		if (first) {
			for (const m of this.men) {
				if (!m.legs.some((l) => l.ball && l.t1 > first.t0)) {
					continue;
				}
				m.split(first.t0);
				for (const l of m.legs) {
					if (l.t0 >= first.t0) {
						delete l.ball;
					}
				}
			}
		}
		const up = new Set<Leg>();
		const held = new Set<Actor>();
		for (const [k, s] of segs.entries()) {
			if (s.kind !== "held") {
				continue;
			}
			const m = this.man(s.pid);
			if (!m) {
				continue;
			}
			held.add(m);
			const until = segs[k + 1]?.t0 ?? Infinity;
			m.split(s.t0);
			if (until < Infinity) {
				m.split(until);
			}
			for (const l of m.legs) {
				if (l.t0 >= s.t0 && l.t0 < until) {
					l.ball = l.arms
						? (TAKE_HAND_BALL.get(l.arms) ??
							(IN_HAND.has(l.arms)
								? "throw"
								: ARMS_TUCKED.has(l.arms)
									? "carry"
									: "hands"))
						: holdFor(l.move);
					// (On his feet with it in both hands: up at his chest,
					// not swinging.)
					if (!l.arms && l.ball === "hands" && HELD_UP.has(l.move)) {
						l.arms = "qbSet";
						up.add(l);
					}
				}
			}
		}
		// (Stood a moment between strides with it tucked away - pulled up
		// to point, say: kept tucked, not out in both hands for it and back.)
		for (const m of held) {
			const legs = m.legs;
			for (let k = 1; k + 1 < legs.length; k++) {
				const l = legs[k]!;
				const before = legs[k - 1]!;
				const after = legs[k + 1]!;
				if (
					up.has(l) &&
					l.t1 - l.t0 < TUCKED_MS &&
					before.ball === "carry" &&
					after.ball === "carry" &&
					before.t1 === l.t0 &&
					after.t0 === l.t1
				) {
					l.ball = "carry";
					delete l.arms;
				}
			}
		}
		for (const [k, s] of segs.entries()) {
			const m = s.kind === "held" ? this.man(s.pid) : undefined;
			if (m) {
				this.carryArm(m, s.t0, segs[k + 1]?.t0 ?? Infinity);
			}
		}
	}

	// Which arm he has it tucked under, from t0 till t1: the one away from
	// where they come at him from - in off the middle of the field, and up
	// it the way he is going - switched over to the other as he cuts across
	// only when that is plain, and not again for a while. (Under the other
	// one from his free arm, as he puts it out at a man; and, the play
	// over, kept where it is.)
	carryArm(m: Actor, t0: number, t1: number) {
		const { dir, offense } = this.input;
		const up = m.team === offense ? dir : -dir;
		const cover = this.coverFor(m, t0, t1);
		if (cover) {
			m.split(cover[0]);
		}
		const legs = m.legs.filter(
			(l) => l.t0 >= t0 && l.t0 < t1 && tucked(l.ball),
		);
		const stiff = legs.filter((l) => l.arms === "stiffArm").map((l) => l.t0);
		// (Each stretch he runs with it: the arm he would have it under.)
		const wants = legs.map((l) => {
			const far = dist(l.from, l.to);
			if (l.t0 >= this.whistle || !GAITS.has(l.move) || far < ARM_LEAST_FT) {
				return undefined;
			}
			if (stiff.some((t) => t >= l.t0 && t - l.t0 < ARM_KEEP_MS)) {
				return { left: false, sure: true, now: true };
			}
			const hx = (l.to.x - l.from.x) / far;
			const hy = (l.to.y - l.from.y) / far;
			const vx = up * ARM_LOOK_FT;
			const vy = FIELD_W / 2 - (l.from.y + l.to.y) / 2;
			// (Where they come from on his right, or his left - see sculpt.ts
			// bodyPoint - and how plainly.)
			const side = (hx * vy - hy * vx) / Math.hypot(vx, vy);
			return { left: side > 0, sure: Math.abs(side) >= ARM_SURE, now: false };
		});
		// (Under the one he first runs with it under from the first - down
		// on it before that, under his right.)
		const first = wants.findIndex((w) => w !== undefined);
		const down = legs.findLastIndex((l, k) => k < first && DOWN.has(l.move));
		let left = first >= 0 && wants[first]!.left;
		let since = -Infinity;
		for (const [k, l] of legs.entries()) {
			const w = wants[k];
			// (The ball under the arm he is not holding up to be pulled up
			// by - see takeHand.)
			if (l.arms !== undefined && TAKE_HAND_BALL.has(l.arms)) {
				continue;
			}
			if (
				w &&
				w.left !== left &&
				w.sure &&
				(l.t0 - since >= ARM_KEEP_MS || w.now)
			) {
				left = w.left;
				since = l.t0;
			}
			const over = cover && l.t0 >= cover[0] && l.t0 < cover[1];
			l.ball =
				left && k > down
					? over
						? "coverL"
						: "carryL"
					: over
						? "cover"
						: "carry";
		}
	}

	// Braced for the hit, his hand over it: from just before he is hit -
	// brought down, or stood up and driven back - till he is up again (or
	// done being driven). As a rule; a back careful with it (see Flair)
	// all but always, a careless one less often. (Not if it comes out.)
	coverFor(m: Actor, t0: number, t1: number): [number, number] | undefined {
		const legs = m.legs;
		const k = legs.findIndex(
			(l) =>
				l.t0 >= t0 && l.t0 < t1 && tucked(l.ball) && COVER_HITS.has(l.move),
		);
		const hit = legs[k];
		if (
			!hit ||
			hit.t1 > t1 ||
			countOf(m, this.input.key, 22) >=
				COVER_ODDS[0] + COVER_ODDS[1] * (m.flair.secure ?? 0)
		) {
			return undefined;
		}
		const c0 = hit.t0 - COVER_MS;
		// (On his feet with it tucked away into it.)
		if (
			c0 < t0 ||
			!legs.every(
				(l) =>
					l.t1 <= c0 ||
					l.t0 >= hit.t0 ||
					(tucked(l.ball) && moveKind(l.move) !== "still"),
			)
		) {
			return undefined;
		}
		let j = k + 1;
		while (
			hit.move !== "drive" &&
			legs[j] &&
			DOWN.has(legs[j]!.move) &&
			!RISING.has(legs[j]!.move)
		) {
			j++;
		}
		return [c0, Math.min(t1, legs[j]?.t0 ?? hit.t1)];
	}

	// ---- the snap -------------------------------------------------------------

	// The ball back to the quarterback: hand to hand under center, in the
	// air back to him in the gun. When he has it.
	snapTo(qb: Actor): number {
		const { snap } = this.input;
		const center = this.role("C");
		const qbAt = qb.at;
		const under = !this.input.set?.gun;
		if (under || !center) {
			const t = snap + 70;
			this.ball.push({ kind: "held", t0: t, pid: qb.pid });
			qb.hold(t);
			return t;
		}
		const from: Pt3 = { ...this.w({ d: -0.6, a: 0 }), z: 0.6 };
		const to: Pt3 = {
			...this.w({ ...this.of(qbAt), d: this.of(qbAt).d + 1 }),
			z: 3.4,
		};
		const t0 = snap + 30;
		const t1 = t0 + (dist(from, to) / 46) * 1000;
		this.ball.push(
			{ kind: "fly", t0, t1, from, to, peak: 0.6 },
			{ kind: "held", t0: t1, pid: qb.pid },
		);
		qb.hold(t1);
		return t1;
	}

	qb(): Actor {
		return (
			this.role("QB") ?? this.man(this.statPids("pss", "rus")[0]) ?? this.o[0]!
		);
	}

	// ---- a knee ------------------------------------------------------------------

	kneel(i: number) {
		const { snap } = this.input;
		const qb = this.qb();
		const has = this.snapTo(qb);
		const down = has + 500;
		qb.go(
			this.w({ d: this.of(qb.at).d - 2.5, a: 0 }),
			has + 350,
			"walk",
			this.ahead,
		);
		qb.act("kneelDown", down, this.ahead);
		qb.hold(down + 1400, "kneelDown");
		this.at.set(i, down);
		this.whistle = down + 300;
		// The lines into each other a moment, most of them (see KNEEL_LEAN),
		// and let go.
		const pairs = this.pairUp(
			this.o.filter((m) => OL.has(m.role)),
			this.d.filter((m) => DL.has(m.role)),
		).filter(
			([b, c]) =>
				Math.abs(this.of(b.at).a - this.of(c.at).a) < KNEEL_PAIR_FT &&
				countOf(b, this.input.key, 112) < KNEEL_LEAN,
		);
		for (const [b, c] of pairs) {
			const go =
				snap + KNEEL_OFF_MS + countOf(c, this.input.key, 112) * KNEEL_OFF_MORE;
			const p = this.of(b.at);
			const q = this.of(c.at);
			const mid = (p.d + q.d) / 2;
			const a = (p.a + q.a) / 2;
			const till =
				this.whistle - 100 + countOf(b, this.input.key, 113) * KNEEL_LEAN_MORE;
			b.hold(go);
			c.hold(go);
			b.go(
				this.w({ d: mid - BLOCK_GAP / 2, a }),
				go + KNEEL_IN_MS,
				"engage",
				this.ahead,
			);
			c.go(
				this.w({ d: mid + BLOCK_GAP / 2, a }),
				go + KNEEL_IN_MS,
				"engage",
				this.behind,
			);
			b.hold(till, "engage", this.ahead);
			c.hold(till, "engage", this.behind);
			b.hold(Math.max(till, this.whistle) + 300, "stand", this.ahead);
			c.hold(Math.max(till, this.whistle) + 300, "stand", this.behind);
		}
		// Nobody wants any of it: the rest up out of their stances and
		// standing.
		for (const m of this.men) {
			if (m === qb) {
				continue;
			}
			if (!pairs.some(([b, c]) => b === m || c === m)) {
				m.hold(snap + 300 + this.r() * 300);
				m.hold(this.whistle, "stand");
			}
			this.done.add(m.pid);
		}
		this.done.add(qb.pid);
	}

	// ---- a pass play --------------------------------------------------------------

	pass(drop: number) {
		const { snap, events } = this.input;
		const r = this.r;
		const qb = this.qb();
		const has = this.snapTo(qb);
		const gun = !!this.input.set?.gun;

		// What came of it.
		const sackI = this.find("sack", drop);
		const cmpI = this.find("passComplete", drop);
		const incI = this.find("passIncomplete", drop);
		const intI = this.find("interception", drop);
		const scrI = this.find("run", drop);
		const fumI = this.find("fumble", drop);
		const firsts = [sackI, cmpI, incI, intI, scrI, fumI].filter((i) => i >= 0);
		const first = firsts.length > 0 ? Math.min(...firsts) : -1;
		const thrown =
			first >= 0 && (first === cmpI || first === incI || first === intI);
		const sacked = first >= 0 && first === sackI;
		const scrambled = first >= 0 && first === scrI;
		const stripped = first >= 0 && first === fumI;

		// Who stays in to block, who goes out.
		const kept = new Set(this.statPids("pba", "pbw"));
		const named = this.man(this.statPids("tgt")[0]);
		// (A two-point try's man read off his name - one the passer shares,
		// say: thrown to a man out on a route all the same.)
		const target =
			named && named !== qb && named.team === this.input.offense
				? named
				: thrown
					? this.o.find(
							(m) =>
								RECEIVERS.has(m.role) &&
								!kept.has(m.pid) &&
								!this.done.has(m.pid),
						)
					: undefined;
		const runners = this.o.filter(
			(m) =>
				RECEIVERS.has(m.role) && (!kept.has(m.pid) || m === target) && m !== qb,
		);
		const blockers = this.o.filter(
			(m) => OL.has(m.role) || (RECEIVERS.has(m.role) && !runners.includes(m)),
		);
		// The rush: the front, and a blitzer if one got home - but not the man
		// the sim says picks it off, dropped off into coverage.
		const sacker = sacked ? this.man(this.statPids("defSk")[0]) : undefined;
		const picker =
			first >= 0 && first === intI
				? this.man(this.statPids("defInt")[0])
				: undefined;
		// (Picked off right at the line, on his own count: knocked up there
		// by one of the rush - him, if he is one of the front - see PICK_BAT.)
		const pickYds = first === intI ? events[first]?.yds : undefined;
		const pickBat =
			!!picker &&
			typeof pickYds === "number" &&
			pickYds <= PICK_BAT_YDS &&
			countOf(picker, this.input.key, 137) < PICK_BAT;
		const rushers = this.d.filter(
			(m) =>
				(DL.has(m.role) || m === sacker) &&
				(m !== picker || (pickBat && DL.has(m.role))),
		);
		if (!sacker && r() < 0.22) {
			const lb = this.d.find(
				(m) => LB.has(m.role) && !rushers.includes(m) && m !== picker,
			);
			if (lb) {
				rushers.push(lb);
			}
		}
		const coverage = this.d.filter((m) => !rushers.includes(m));

		// Where the ball is caught (or would have been), if it is thrown.
		let airYds = 0;
		let gain = 0;
		if (thrown) {
			const e = events[first]!;
			const yds = typeof e.yds === "number" ? e.yds : 0;
			gain = yds;
			// (Picked off where the sim says: the next snap is from there.)
			airYds =
				first === intI
					? yds
					: this.airYards(
							first === cmpI ? "complete" : "other",
							yds,
							e.td === true,
						);
		}
		const catchD = Math.min(airYds * YD, this.goalD + 25);
		// Sacked, where they get him.
		const sackYds = sacked ? events[sackI]!.yds : undefined;
		// (For a safety, down in his own end zone - not on the goal line,
		// to fall out over it.)
		const sackD = Math.min(
			-2,
			(typeof sackYds === "number" ? sackYds : -6) * YD,
			sacked && events[sackI]!.safety === true
				? this.ownGoalD - SAFETY_IN[0] - r() * SAFETY_IN[1]
				: Infinity,
		);

		// The route the ball goes to him on.
		const routed =
			thrown && target
				? targetRoute(
						spotOf(target.role),
						airYds,
						r(),
						first === cmpI && !!this.input.out,
					)
				: undefined;
		// (Deep, now and then a double move off a jab in - see SLUGGO: the
		// man on him beaten by it, so caught with room to run after it, or
		// thrown over him, not where somebody got a hand to it.)
		const targetName: RouteName | undefined =
			routed !== undefined &&
			airYds >= SLUGGO_YDS &&
			target &&
			(spotOf(target.role) === "wide" || spotOf(target.role) === "slot") &&
			(first === cmpI
				? gain - airYds >= SLUGGO_YAC || events[first]!.td === true
				: first !== intI && this.statPids("defPssDef").length === 0) &&
			countOf(target, this.input.key, 146) < SLUGGO
				? "sluggo"
				: routed;
		const slip = targetName === "slip";
		// (Toward the middle of the field from where he lines up.)
		const inSignOf = (start: OPt): 1 | -1 =>
			start.a > 0 ? -1 : start.a < 0 ? 1 : r() < 0.5 ? 1 : -1;
		const layTarget = (m: Actor, name: RouteName): Route => {
			const start = this.of(m.at);
			const inSign = inSignOf(start);
			const u = r();
			// A back out of the backfield to the side of the man who brings
			// him down.
			const ta = first === cmpI ? this.tacklerA(m, snap) : undefined;
			const v =
				start.a === 0 && ta !== undefined
					? (ta < 0 ? 0 : 0.5) + r() * 0.5
					: r();
			const route = layRoute(
				name,
				start,
				inSign,
				catchD,
				this.bounds,
				u,
				v,
				true,
			);
			return first === cmpI &&
				this.input.out &&
				TO_LINE.has(name) &&
				gain * YD - catchD <= TOE_YAC &&
				r() < TOE_TAP
				? toTheLine(route, this.bounds, TOE_IN)
				: route;
		};
		// (Laid now - but for a back's screen, who goes from beside the
		// quarterback - so the quarterback can roll out to where it goes.)
		const targetLaid =
			target && targetName && !slip ? layTarget(target, targetName) : undefined;

		// The quarterback's drop: none for a screen, three steps for the
		// short game, five or seven going deep (in the gun, a step or two) -
		// sacked, to a few steps short of where they get him; drifting back
		// deep for a back's screen.
		const qbStart = this.of(qb.at);
		const dropD = slip
			? gun
				? SLIP_DROP_GUN
				: SLIP_DROP
			: sacked
				? sackStepUp(qb, this.input.key, sackD)
					? clamp(
							qbStart.d -
								sackD +
								SACK_STEP_FT[0] +
								(SACK_STEP_FT[1] - SACK_STEP_FT[0]) *
									countOf(qb, this.input.key, 108),
							0,
							24,
						)
					: clamp(qbStart.d - sackD - 2 - r() * 4, 0, 24)
				: !thrown
					? gun
						? 4
						: 11
					: gun
						? airYds <= 0
							? 0
							: airYds <= 8
								? 4
								: 8
						: airYds <= 0
							? 3
							: airYds <= 8
								? 11
								: airYds <= 18
									? 18
									: 24;
		let setAt = this.w({ d: qbStart.d - dropD, a: qbStart.a });
		const dropMs =
			dropD === 0
				? 0
				: Math.max(300 + dropD * 55, (dropD / QB_DROP_FPS) * 1000 + 150);
		// Now and then a fake to a back first - play action: out to him as
		// on a run, the ball into his belly and back out, the back on
		// through the line with his arms shut round nothing; then the drop.
		const backs =
			thrown && airYds >= PA_AIR && r() < PA_SHARE
				? this.o.filter(
						(m) =>
							(m.role === "RB" || m.role === "FB") &&
							m !== target &&
							m.t <= snap + 1 &&
							this.of(m.at).d < -8,
					)
				: [];
		const faker = backs.find((m) => m.role === "RB") ?? backs[0];
		let fakeT: number | undefined;
		let bootReady = 0;
		// (Rolled out to the side it goes to, if it goes out of the middle.)
		const bootSide =
			faker &&
			targetLaid &&
			!gun &&
			Math.abs(targetLaid.catchAt.a) >= 8 &&
			r() < BOOT
				? Math.sign(targetLaid.catchAt.a)
				: 0;
		if (faker) {
			const fs = this.of(faker.at);
			const side = bootSide
				? -bootSide
				: fs.a === 0
					? r() < 0.5
						? 1
						: -1
					: (gun ? -1 : 1) * Math.sign(fs.a);
			// (Where and when a handoff would be: see runPlay.)
			const lane = this.w(
				gun
					? { d: qbStart.d + 0.5, a: side * 2.5 }
					: underMesh(qbStart, fs, side),
			);
			// (In the gun, from the far side of him: across in front of him
			// to it - see GUN_CROSS_UP - the quarterback stood where he is,
			// not through him to it out the other side; when, and the way on
			// from it, as from beside him.)
			const cross = gun && (fs.a - qbStart.a) * side < 0;
			const meshPt = cross
				? this.w({
						d: qbStart.d + GUN_CROSS_UP,
						a: qbStart.a + side * GUN_CROSS_ALONG,
					})
				: lane;
			const beside = this.w({
				d: this.of(lane).d,
				a: this.of(lane).a - side * MESH_APART,
			});
			const qbMesh = cross ? qb.at : beside;
			// (Out of his stance to it, up to speed - see the rider in runPlay
			// - not there flat out off his first step.)
			const reachOf = (p: Pt) =>
				rampedMs(dist(faker.at, p), faker.top * 0.75, 0, RELEASE_ACCEL);
			const reach = reachOf(meshPt);
			fakeT = Math.max(
				Math.max(has, qb.t) +
					PA_MS / 2 +
					qbStepMs(dist(qb.at, beside)) +
					r() * 150,
				snap + 150 + reachOf(lane),
			);
			qb.go(qbMesh, fakeT - PA_MS / 2, "slide", this.toward(qbMesh, meshPt));
			qb.act("handOff", fakeT + PA_MS / 2, this.toward(qbMesh, meshPt));
			if (bootSide) {
				setAt = this.w({
					d: this.of(qbMesh).d - 6,
					a: qbStart.a + bootSide * BOOT_OUT,
				});
				const ms = (dist(qb.at, setAt) / BOOT_FPS) * 1000;
				qb.go(setAt, qb.t + ms, "run", this.toward(qb.at, setAt));
				qb.arms("qbSet", fakeT + PA_MS / 2, qb.t);
				bootReady = qb.t - ms * 0.4;
			} else {
				setAt = this.w({
					d: Math.min(qbStart.d - dropD, this.of(beside).d - (gun ? 4 : 7)),
					a: qbStart.a,
				});
				// (Back off the fake no quicker than on any drop - see
				// QB_DROP_FPS - and pulled up into his set: not flat out off it
				// and stopped dead in his set.)
				const far = dist(qb.at, setAt);
				qb.go(
					setAt,
					qb.t + Math.max(300 + far * 55, (far / QB_DROP_FPS) * 1000 + 150),
					"back",
					this.ahead,
				);
			}
			const fakeFrom = faker.at;
			faker.hold(fakeT - reach);
			faker.getOff(
				meshPt,
				fakeT,
				0,
				RELEASE_ACCEL,
				"run",
				0,
				this.toward(faker.at, meshPt),
			);
			const on = gun && runners.includes(faker) ? PA_ACROSS : 0;
			// (Going at the pace of the last of it that took any time - not a
			// sliver of a step.)
			const into =
				faker.legs.findLast((l) => l.t1 - l.t0 >= PA_PACE_MS) ?? faker.last;
			const v = Math.min(legPace(into), faker.top);
			const far = Math.max(PA_PAST, (v * v) / (2 * STOP_ACCEL * STOP_HARD));
			const past = (on > 0 ? PA_PAST : far) / Math.hypot(1, on);
			const onTo = this.w({
				d: this.of(lane).d + past,
				a: this.of(lane).a + side * on * past,
			});
			const inFt = dist(into.from, into.to);
			if (on > 0) {
				// (Bent round off the way he came in on across - not turned
				// in a stride at full tilt.)
				faker.chase(onTo, fakeT + 300, faker.top, "run");
			} else if (inFt <= 0 || v <= 0) {
				faker.go(onTo, fakeT + 300, "run", this.ahead);
			} else {
				// (Staying in to block: on off it round from the way he came
				// in, pulled up as he goes - not turned square in a stride at
				// full tilt and stopped dead in his set. Stepped off at even
				// times along it, as far each time as his pace then takes
				// him.)
				// (Across in front of the quarterback: on the way he would
				// have gone through it beside him.)
				const laneFt = dist(fakeFrom, lane);
				const ux = cross
					? (lane.x - fakeFrom.x) / Math.max(laneFt, 0.01)
					: (into.to.x - into.from.x) / inFt;
				const uy = cross
					? (lane.y - fakeFrom.y) / Math.max(laneFt, 0.01)
					: (into.to.y - into.from.y) / inFt;
				let via = {
					x: meshPt.x + ux * far * PA_ROUND,
					y: meshPt.y + uy * far * PA_ROUND,
				};
				// (Round a quarter turn into the line off it, as wide as he
				// takes at his pace - not hooked back the way he came.)
				let end = {
					x: onTo.x + ux * far * PA_ROUND,
					y: onTo.y + uy * far * PA_ROUND,
				};
				// (But not on into the line itself, in among the men on it:
				// round along behind it instead, no further up than
				// PA_LINE_OFF off the ball - as far round as he pulls up in.)
				const m0 = this.of(lane);
				const up = -PA_LINE_OFF - m0.d;
				if (this.of(end).d > -PA_LINE_OFF && up > 0) {
					const sa =
						Math.sign(this.of(into.to).a - this.of(into.from).a) || side;
					const fwd = Math.min(up, far);
					const across = Math.sqrt(Math.max(0, far * far - fwd * fwd));
					end = this.w({ d: m0.d + fwd, a: m0.a + sa * across });
					const v0 = this.of(via);
					via = this.w({
						d: Math.min(v0.d, m0.d + fwd),
						a: m0.a + clamp(v0.a - m0.a, -across, across),
					});
				}
				const fine = [meshPt, ...this.curve(meshPt, via, end, 32)];
				const at = [0];
				for (let k = 1; k < fine.length; k++) {
					at.push(at[k - 1]! + dist(fine[k - 1]!, fine[k]!));
				}
				const len = at.at(-1)!;
				let j = 1;
				for (let k = 1; k <= PA_ROUND_STEPS; k++) {
					const u = k / PA_ROUND_STEPS;
					const want = len * (2 * u - u * u);
					while (j < fine.length - 1 && at[j]! < want) {
						j++;
					}
					const f = clamp(
						(want - at[j - 1]!) / Math.max(at[j]! - at[j - 1]!, 1e-6),
						0,
						1,
					);
					const a = fine[j - 1]!;
					const b = fine[j]!;
					faker.go(
						{ x: a.x + (b.x - a.x) * f, y: a.y + (b.y - a.y) * f },
						fakeT + ((2 * len * 1000) / v) * u,
						"run",
					);
				}
			}
			faker.arms("fakeCarry", fakeT - 200, fakeT + 300);
		} else if (dropD >= DROP_TURN_FT && !gun) {
			// (Five or seven steps: turned and away most of it, side on, then
			// round to the field for the last of it.)
			const turned = this.w({
				d: qbStart.d - dropD * DROP_TURNED,
				a: qbStart.a,
			});
			qb.go(
				turned,
				has + dropMs * DROP_TURNED_MS,
				"slide",
				this.ahead + DROP_TURN,
			);
			qb.go(setAt, has + dropMs, "back", this.ahead);
		} else if (dropD > 0) {
			qb.go(setAt, has + dropMs, "back", this.ahead);
		}
		const ready = bootSide ? bootReady : qb.t + 150;
		if (!bootSide) {
			qb.hold(ready, "pocket", this.ahead);
		}

		// A back's screen: up beside the quarterback as if to block, a beat.
		const slipSide = slip
			? Math.sign(this.of(target!.at).a) || (r() < 0.5 ? -1 : 1)
			: 0;
		if (slip) {
			target!.hold(snap + 100);
			// (As far as a set step or two gets him: short of it, if it is
			// farther - not crabbed across there flat out.)
			const aim = this.w({
				d: this.of(setAt).d + 3,
				a: qbStart.a + slipSide * 3,
			});
			const far = dist(target!.at, aim);
			const most = (SHUFFLE_MOST * Math.max(0, snap + 550 - target!.t)) / 1000;
			const u = far > most ? most / far : 1;
			target!.go(
				{
					x: target!.at.x + (aim.x - target!.at.x) * u,
					y: target!.at.y + (aim.y - target!.at.y) * u,
				},
				snap + 550,
				"passSet",
				this.ahead,
			);
			target!.hold(snap + SLIP_WAIT, "passSet", this.ahead);
		}

		// The routes, each man off the line a moment after the snap.
		const routes = new Map<number, Route>();
		for (const m of runners) {
			const start = this.of(m.at);
			const inSign = inSignOf(start);
			if (m === target && thrown && targetName) {
				routes.set(m.pid, targetLaid ?? layTarget(m, targetName));
			} else {
				const { name, depth } = decoyRoute(spotOf(m.role), r(), r());
				routes.set(
					m.pid,
					layRoute(name, start, inSign, depth, this.bounds, r(), r()),
				);
			}
		}
		const releases = new Map(
			runners.map((m) => [m.pid, Math.max(m.t, snap + 120 + r() * 100)]),
		);

		// When it is thrown, and caught: as the man it goes to comes open -
		// or, open before the quarterback is set, as soon as he can.
		let throwAt = ready + 300 + r() * 900;
		let catchAt = throwAt;
		let catchPt: Pt | undefined;
		// (A slant-and-go: when he is at the top of the jab in, there, and
		// where he broke it in from.)
		let jab: { at: number; top: Pt; stem: Pt } | undefined;
		const tr = target ? routes.get(target.pid) : undefined;
		if (thrown && target && tr) {
			catchPt = this.w(tr.catchAt);
			const go = releases.get(target.pid)!;
			const onTime =
				go +
				this.routeMs(
					tr,
					target.top * ROUTE_PACE,
					tr.catchAlong,
					this.releasePace(target, tr, go),
				);
			const fl = flightOf(
				this.w({ d: this.of(setAt).d + 1, a: this.of(setAt).a }),
				catchPt,
				qb.flair.arm,
			);
			// (Thrown to his back shoulder: there as he turns to it.)
			const shoulder = tr.name === "backShoulder";
			const hold =
				tr.settles && tr.name !== "screen" && !shoulder
					? r() * SETTLE_HOLD_MS
					: 0;
			// (Not before he has had his time back there - longer, the deeper
			// it goes.)
			const least =
				tr.name === "screen"
					? snap + SCREEN_THROW + r() * 300 + fl.ms
					: slip
						? snap + SLIP_THROW + r() * 400 + fl.ms
						: snap +
							TO_THROW +
							Math.max(0, airYds) * TO_THROW_YD +
							r() * TO_THROW_SPREAD +
							fl.ms;
			// (Nor, to a man running on through it, held on to while he
			// jogs his route out to wait on it: out to him in his stride as
			// he comes open - see ROUTE_SLOW_MOST.)
			const due = shoulder
				? onTime
				: tr.settles && tr.name !== "screen" && !slip
					? Math.min(least, onTime + SETTLE_MOST_MS)
					: tr.settles
						? least
						: Math.min(least, go + (onTime - go) / ROUTE_SLOW_MOST);
			catchAt = Math.max(onTime, ready + 300 + fl.ms, due) + hold;
			throwAt = catchAt - fl.ms;
			catchPt = this.runRoute(target, tr, go, catchAt) ?? catchPt;
			this.at.set(first, catchAt);
			if (tr.name === "sluggo") {
				const top = this.w(tr.path[2]!);
				let near = Infinity;
				let at = go;
				for (let t = go; t <= catchAt; t += 20) {
					const d = dist(target.posAt(t), top);
					if (d < near) {
						near = d;
						at = t;
					}
				}
				jab = { at, top, stem: this.w(tr.path[1]!) };
			}
		}

		// Everyone else's routes, run on through the throw.
		const rest = Math.max(throwAt + 1200, snap + 2600);
		for (const m of runners) {
			if (m === target && thrown) {
				continue;
			}
			this.runRoute(
				m,
				routes.get(m.pid)!,
				releases.get(m.pid)!,
				undefined,
				rest,
			);
		}

		if (thrown && target && catchPt) {
			this.othersAfterIt(
				runners.filter((m) => m !== target),
				throwAt,
				catchPt,
				catchAt,
			);
		}

		// The quarterback: set, through his progressions, and the throw.
		if (thrown && catchPt) {
			if (bootSide) {
				this.onTheRun(qb, bootSide, catchPt, throwAt, catchAt);
			} else {
				this.reads(
					qb,
					target!,
					catchPt,
					throwAt - WIND_MS,
					runners,
					jab && {
						at: jab.at - PUMP_MS / 2 - SLUGGO_PUMP_LEAD_MS,
						least: jab.at - PUMP_MS / 2 - SLUGGO_PUMP_EARLY_MS,
						to: {
							x: 2 * jab.top.x - jab.stem.x,
							y: 2 * jab.top.y - jab.stem.y,
						},
					},
				);
				this.pocketThrow(qb, catchPt, throwAt, catchAt);
			}
			const at = this.of(qb.posAt(throwAt));
			const release: Pt3 = {
				...this.w({ d: at.d + 0.8, a: at.a }),
				z: RELEASE_Z,
			};
			const fl = flightOf(release, catchPt, qb.flair.arm);
			this.ball.push({
				kind: "fly",
				t0: throwAt,
				t1: catchAt,
				from: release,
				to: { ...catchPt, z: CATCH_Z },
				peak: fl.peak,
				spiral: true,
			});
		}

		// Never thrown: when they get to him, he takes off, or it is knocked
		// out of his hands.
		const outT = sacked
			? Math.max(ready + 600, snap + 2300 + r() * 1300)
			: scrambled
				? Math.max(ready + 500, snap + 2200 + r() * 900)
				: stripped
					? Math.max(ready + 400, snap + 2100 + r() * 900)
					: ready + 1500;
		this.seen = thrown
			? throwAt + BREAK_MS
			: scrambled
				? outT + BREAK_MS
				: snap + 200;

		// Everybody's part until somebody makes a play: the coverage on the
		// routes (the man the sim says got to the ball on the man it went
		// to), the rush on the quarterback, the line holding it off (the man
		// the sim says gave up the sack on the man who got there).
		// (Or, nobody's, the man flagged for interfering with the man it went
		// to: on him at the catch, his hands on him - see fouls.ts actFouls.)
		const onBall = thrown
			? (this.man(this.statPids(first === intI ? "defInt" : "defPssDef")[0]) ??
				(first === intI ? undefined : this.interferer(coverage)))
			: undefined;
		// (Zone or man behind it, by the look the defense showed - see
		// ZONE_ONE: man on a back-shoulder throw or a double move, which are
		// thrown at the man on him.)
		const ss = coverage.find((m) => m.role === "SS");
		const twoHigh =
			ss !== undefined && this.of(ss.posAt(snap)).d >= TWO_HIGH_FT;
		const pressed = coverage.some(
			(m) => m.role === "CB" && this.of(m.posAt(snap)).d <= PRESSED_FT,
		);
		const zone =
			!pressed &&
			tr?.name !== "backShoulder" &&
			!jab &&
			countOf(qb, this.input.key, 147) < (twoHigh ? ZONE_TWO : ZONE_ONE);
		this.cover(
			coverage,
			runners,
			thrown ? throwAt + BREAK_MS : outT,
			catchPt,
			catchAt,
			onBall && target ? [onBall, target] : undefined,
			fakeT,
			first === cmpI ? Math.max(0, (gain - airYds) * YD) : 0,
			tr?.name === "backShoulder" ? target : undefined,
			jab && target ? { m: target, ...jab } : undefined,
			{ zone, twoHigh },
			this.flaggedFor("Holding", coverage),
		);
		const beaten = sacker ? this.man(this.statPids("skAlw")[0]) : undefined;
		this.passRush(
			rushers,
			blockers,
			qb,
			(thrown ? catchAt : outT) + 300,
			sacker && beaten ? [sacker, beaten] : undefined,
		);
		// (The backs out of the backfield on their routes round the line,
		// not through it - see releaseRound.)
		for (const m of runners) {
			if (spotOf(m.role) === "back" && !(m === target && slip)) {
				this.releaseRound(
					m,
					releases.get(m.pid)!,
					m === target && thrown ? catchAt - RELEASE_CATCH_MS : Infinity,
				);
			}
		}

		if (thrown && target && tr?.name === "screen") {
			this.screenBlocks(target, runners, catchAt);
		} else if (thrown && target && catchPt && slip) {
			this.slipBlocks(
				blockers,
				rushers,
				qb,
				slipSide,
				catchPt,
				throwAt,
				catchAt,
			);
		}

		if (thrown && target && catchPt && tr?.name !== "screen" && !slip) {
			this.callFor(target, qb, coverage, catchPt, throwAt, catchAt);
		}
		const bat =
			first === incI &&
			thrown &&
			target &&
			catchPt &&
			!bootSide &&
			tr?.name !== "screen" &&
			!slip
				? this.batAt(rushers, blockers, qb, catchPt, throwAt)
				: pickBat &&
					  picker &&
					  thrown &&
					  target &&
					  catchPt &&
					  !bootSide &&
					  tr?.name !== "screen" &&
					  !slip
					? this.batAt(rushers, blockers, qb, catchPt, throwAt, picker)
					: undefined;
		if (
			thrown &&
			catchPt &&
			!bootSide &&
			tr?.name !== "screen" &&
			!slip &&
			(first === cmpI || first === incI)
		) {
			this.qbHit(
				rushers.filter((m) => m !== bat?.m),
				blockers,
				qb,
				throwAt,
			);
		}

		if (thrown && catchPt && catchAt - throwAt > RUSH_LONG_MS) {
			this.lineAfterIt(rushers, blockers, throwAt, catchPt, catchAt);
		}

		// And what came of it.
		if (first === cmpI && thrown && target && catchPt) {
			this.complete(cmpI, qb, target, catchPt, catchAt, gain);
			if (tr?.name !== "screen" && !slip) {
				this.downfield(blockers);
				this.withIt(
					runners.filter((m) => m !== target),
					catchAt,
				);
			}
			this.withIt([qb], catchAt, QB_AFTER_KEEP_FT, QB_AFTER_FPS, false);
		} else if (first === incI && thrown && target && catchPt) {
			// (The line and the rush, let up as it went: off up the field
			// after it as it comes down, not stood where they let go of each
			// other - thrown away, nobody going for it.)
			if (!this.incomplete(incI, qb, target, catchPt, catchAt, coverage, bat)) {
				this.downfield(blockers, LINE_AFTER_IT_MS);
				this.downfield(rushers, LINE_AFTER_IT_MS);
			}
		} else if (first === intI && thrown && target && catchPt) {
			this.intercepted(intI, qb, target, catchPt, catchAt, coverage, bat);
		} else if (sacked) {
			this.sacked(sackI, qb, outT, sackD, sacker, runners);
		} else if (scrambled) {
			this.scramble(scrI, qb, outT, runners);
			this.withIt(runners, outT);
		} else if (stripped) {
			this.strip(fumI, qb, outT, runners);
		} else {
			this.whistle = Math.max(ready + 1500, qb.t);
		}
		for (const m of [qb, ...runners]) {
			this.done.add(m.pid);
		}
	}

	// THE LINE ONCE IT IS CAUGHT: off up the field after the play at a jog,
	// each on his own count and at his own pace - not stood where he set
	// till the whistle. (The punt rush the same, once it is kicked and
	// nobody is bringing it back.)
	downfield(
		blockers: Actor[],
		react: readonly [number, number] = DOWNFIELD_REACT_MS,
	) {
		const end = this.ballAt(this.whistle);
		const men = blockers.filter((b) => !this.done.has(b.pid));
		if (!end || men.length === 0) {
			return;
		}
		// (All of them the same way, keeping their places - not crowding in
		// on the one spot.)
		const mid = {
			x: men.reduce((a, b) => a + b.at.x, 0) / men.length,
			y: men.reduce((a, b) => a + b.at.y, 0) / men.length,
		};
		const far = dist(mid, end);
		if (far <= DOWNFIELD_SHORT_FT) {
			return;
		}
		const ux = (end.x - mid.x) / far;
		const uy = (end.y - mid.y) / far;
		for (const b of men) {
			const from =
				b.t + react[0] + (react[1] - react[0]) * countOf(b, this.input.key, 23);
			const fps =
				DOWNFIELD_FPS[0] +
				(DOWNFIELD_FPS[1] - DOWNFIELD_FPS[0]) * countOf(b, this.input.key, 24);
			const go = Math.min(
				far - DOWNFIELD_SHORT_FT,
				(fps * (this.whistle - from)) / 1000,
			);
			if (go < DOWNFIELD_LEAST_FT) {
				continue;
			}
			// (Turned to it the while, watching it.)
			b.hold(from, undefined, this.toward(b.at, end));
			// (Round any man down on the way - not over him.)
			const to = { x: b.at.x + ux * go, y: b.at.y + uy * go };
			for (const p of this.wayRound(b.at, to, b.t).slice(0, -1)) {
				b.run(p, fps);
			}
			b.runUp(to, fps);
		}
	}

	// THE OTHERS ONCE IT IS AWAY: thrown a long way, the men out on routes
	// it did not go to, their routes run, turned to it and off after it at
	// a jog as it comes down, each at his own pace, as far as his own count
	// keeps him off where it comes down - not stood where their routes left
	// them till it does.
	private othersAfterIt(
		men: Actor[],
		throwAt: number,
		catchPt: Pt,
		catchAt: number,
	) {
		const still = (l: Leg) =>
			l.from.x === l.to.x && l.from.y === l.to.y && !GAITS.has(l.move);
		for (const m of men) {
			if (this.done.has(m.pid) || DOWN.has(m.last.move)) {
				continue;
			}
			const seen =
				throwAt +
				RUSH_SEES_MS +
				PASS_END_SPREAD_MS * countOf(m, this.input.key, 164);
			if (
				m.t > seen &&
				m.legs.every((l) => l.t1 <= seen || still(l)) &&
				catchAt - seen >= AIR_ON_LEAST_MS
			) {
				m.cut(seen);
			}
			const far = dist(m.at, catchPt);
			const keep =
				AIR_ON_KEEP_FT[0] + AIR_ON_KEEP_FT[1] * countOf(m, this.input.key, 162);
			const fps =
				DOWNFIELD_FPS[0] +
				(DOWNFIELD_FPS[1] - DOWNFIELD_FPS[0]) * countOf(m, this.input.key, 163);
			if (catchAt - m.t < AIR_ON_LEAST_MS || far - keep < DOWNFIELD_LEAST_FT) {
				continue;
			}
			const toward = (go: number) => ({
				x:
					m.at.x +
					((catchPt.x - m.at.x) / Math.max(dist(m.at, catchPt), 1e-6)) * go,
				y:
					m.at.y +
					((catchPt.y - m.at.y) / Math.max(dist(m.at, catchPt), 1e-6)) * go,
			});
			// (On the move: round to it as he slows - see rally - and on at
			// it at the pace that has him there as it comes down.)
			const l = m.last;
			if (STEPS.has(l.move) && legPace(l) >= STOP_LEAST) {
				m.plantFor(toward(far - keep), RALLY_TURN_FPS);
				const T = (catchAt - m.t) / 1000;
				const near = dist(m.at, catchPt);
				if (T * 1000 < LINE_GO_LEAST_MS || near - keep < DOWNFIELD_LEAST_FT) {
					m.easeOff(EASE_OFF_FT);
					continue;
				}
				// (As far as slowing to it from the pace he is going and on
				// at it takes him - not slowed to a crawl short of it.)
				const v = legPace(m.last);
				const pace = clamp((near - keep) / T, AIR_ON_FPS_LEAST, fps);
				const slow = v > pace ? (v - pace) / STOP_ACCEL : 0;
				const on =
					((v * v - pace * pace) / (2 * STOP_ACCEL)) * (slow > 0 ? 1 : 0) +
					pace * Math.max(0, T - slow);
				m.chase(
					toward(Math.min(near - AIR_ON_NEAR_FT, Math.max(near - keep, on))),
					catchAt,
					fps,
				);
				continue;
			}
			// (Stood: turned to it first, watching it, and off at the pace
			// that has him there as it comes down.)
			m.hold(
				m.t +
					LINE_TURN_MS[0] +
					(LINE_TURN_MS[1] - LINE_TURN_MS[0]) * countOf(m, this.input.key, 148),
				undefined,
				this.toward(m.at, catchPt),
			);
			const ms = catchAt - m.t;
			const pace = clamp(
				((far - keep) / Math.max(ms, 1)) * 1000,
				AIR_ON_FPS_LEAST,
				fps,
			);
			const go = Math.min(far - keep, (pace * ms) / 1000);
			if (ms < LINE_GO_LEAST_MS || go < DOWNFIELD_LEAST_FT) {
				continue;
			}
			// (Round any man down on the way - not over him.)
			const to = toward(go);
			for (const q of this.wayRound(m.at, to, m.t).slice(0, -1)) {
				m.run(q, pace);
			}
			m.runUp(to, pace);
		}
	}

	// THE LINE ONCE IT IS AWAY: thrown a long way, the rush and the men on
	// them let go of each other as they see it go - not still hand fighting
	// with it long gone - each pair on its own count (see RUSH_SEES_MS); a
	// man not on anybody the same. Each turned to watch it, and with time
	// enough before it comes down, off up the field after it at a jog, all
	// the same way keeping their places (as downfield) - not stood where
	// they let go.
	private lineAfterIt(
		rushers: Actor[],
		blockers: Actor[],
		throwAt: number,
		catchPt: Pt,
		catchAt: number,
	) {
		const off = (b: Actor) =>
			throwAt +
			RUSH_SEES_MS +
			PASS_END_SPREAD_MS * countOf(b, this.input.key, 3);
		const seen = throwAt + RUSH_SEES_MS;
		const live = (m: Actor) => {
			const l = legAt(m, seen);
			return (
				!this.done.has(m.pid) &&
				(l.move === "engage" || l.move === "passSet") &&
				m.t > seen + LINE_TURN_MS[1]
			);
		};
		const line = blockers.filter(live);
		const rush = rushers.filter(live);
		const men: Actor[] = [];
		for (const m of [...line, ...rush]) {
			// (A rusher with the man on him: let go at once, the two of them.)
			const at = m.posAt(seen);
			const mate = line.includes(m)
				? m
				: (line
						.filter((b) => dist(b.posAt(seen), at) < LINE_PAIR_FT)
						.sort(
							(x, y) => dist(x.posAt(seen), at) - dist(y.posAt(seen), at),
						)[0] ?? m);
			const t = off(mate);
			if (t > catchAt - LINE_TURN_MS[1]) {
				continue;
			}
			m.cut(t);
			men.push(m);
		}
		if (men.length === 0) {
			return;
		}
		const mid = {
			x: men.reduce((a, b) => a + b.at.x, 0) / men.length,
			y: men.reduce((a, b) => a + b.at.y, 0) / men.length,
		};
		const far = dist(mid, catchPt);
		const ux = far > 0 ? (catchPt.x - mid.x) / far : 0;
		const uy = far > 0 ? (catchPt.y - mid.y) / far : 0;
		for (const m of men) {
			m.hold(
				m.t +
					LINE_TURN_MS[0] +
					(LINE_TURN_MS[1] - LINE_TURN_MS[0]) * countOf(m, this.input.key, 148),
				undefined,
				this.toward(m.at, catchPt),
			);
			const left = catchAt - m.t;
			if (left < LINE_GO_LEAST_MS) {
				continue;
			}
			const fps =
				DOWNFIELD_FPS[0] +
				(DOWNFIELD_FPS[1] - DOWNFIELD_FPS[0]) * countOf(m, this.input.key, 24);
			const go = Math.min(far - DOWNFIELD_SHORT_FT, (fps * left) / 1000);
			if (go < DOWNFIELD_LEAST_FT) {
				continue;
			}
			// (Round any man down on the way - not over him.)
			const to = { x: m.at.x + ux * go, y: m.at.y + uy * go };
			for (const p of this.wayRound(m.at, to, m.t).slice(0, -1)) {
				m.run(p, fps);
			}
			m.runUp(to, fps);
		}
	}

	// How far downfield (yards) a pass that gained `yds` was caught: the
	// rest is run after the catch. Most of a short one is in the air; a
	// long one is mostly the throw, now and then a short one taken a long
	// way.
	// Set in the pocket until `windAt`: his eyes on another man out on a
	// route first, now and then, and on round to the man it goes to; a step
	// up into the pocket before it goes, some of the time; and a pump at
	// him, once in a while.
	reads(
		qb: Actor,
		target: Actor,
		catchPt: Pt,
		windAt: number,
		runners: Actor[],
		pump?: { at: number; least: number; to: Pt },
	) {
		const r = this.r;
		const spare = windAt - qb.t;
		const to = this.toward(qb.at, catchPt);
		// (A double move: a pump at the jab, as he makes it - as soon as he
		// is set for it, or early enough to have time after it to go back
		// to him, not a pump straight into the throw - and his eyes back up
		// the field with him after it. See SLUGGO.)
		const pumpAt = pump
			? Math.min(Math.max(pump.at, qb.t), windAt - PUMP_MS - SLUGGO_PUMP_GAP_MS)
			: -Infinity;
		if (pump && pumpAt >= Math.max(qb.t, pump.least)) {
			const way = this.toward(qb.at, pump.to);
			this.scan(qb, target, way, pumpAt, runners);
			// (Only a moment to it: straight into it - see FLICK_MS.)
			if (pumpAt - qb.t >= FLICK_MS) {
				qb.hold(pumpAt, "pocket", way);
			}
			qb.act("pump", qb.t + PUMP_MS, way);
			this.scan(qb, target, to + SET_OPEN, windAt, runners);
			return;
		}
		if (spare > LOOK_OFF_MS && r() < LOOK_OFF) {
			const at = qb.t + spare * 0.3;
			const others = runners
				.filter((m) => m !== target)
				.map((m) => this.toward(qb.at, m.posAt(at)))
				.filter(
					(y) => Math.abs(Math.atan2(Math.sin(y - to), Math.cos(y - to))) > 0.5,
				);
			const other = others[Math.floor(r() * others.length)];
			qb.hold(
				qb.t + spare * (0.35 + r() * 0.2),
				"pocket",
				other === undefined ? this.ahead : other + SET_OPEN,
			);
		}
		if (windAt - qb.t > STEP_UP_MS + 250 && r() < STEP_UP) {
			const t0 = qb.t;
			const s = this.of(qb.at);
			qb.go(
				this.w({ d: s.d + STEP_UP_FT, a: s.a }),
				t0 + STEP_UP_MS,
				"walk",
				to,
			);
			qb.arms("qbSet", t0, qb.t);
		}
		if (windAt - qb.t > PUMP_MS + 400 && r() < PUMP) {
			const at = windAt - PUMP_MS - 250 - r() * 150;
			this.scan(qb, target, to, at, runners);
			qb.hold(at, "pocket", to);
			qb.act("pump", qb.t + PUMP_MS, to);
		} else {
			this.scan(qb, target, to + SET_OPEN, windAt, runners);
		}
	}

	// Long in the pocket yet before he goes to him (at `until`): his eyes
	// through a read or two first - another man out on his route, downfield
	// of him, a moment each, side on to him as he will be to the man it goes
	// to - and round to the way he faces for that (`end`) with time to see
	// him. (No r() draws.)
	scan(
		qb: Actor,
		target: Actor | undefined,
		end: number,
		until: number,
		runners: Actor[],
	) {
		const off = (a: number, b: number) =>
			Math.abs(Math.atan2(Math.sin(a - b), Math.cos(a - b)));
		for (let k = 0; k < SCAN_MOST; k++) {
			const hold =
				SCAN_MS[0] + countOf(qb, this.input.key, 16 + k) * SCAN_MS[1];
			if (until - qb.t < hold + SCAN_LAST_MS) {
				return;
			}
			const at = qb.t + hold / 2;
			const was = qb.last.yaw ?? end;
			const ways = runners
				.filter((m) => m !== target)
				.map((m) => this.toward(qb.at, m.posAt(at)))
				.filter(
					(y) =>
						off(y + SET_OPEN, end) > SCAN_APART &&
						off(y + SET_OPEN, was) > SCAN_APART &&
						off(y, this.ahead) < SCAN_WIDE,
				);
			if (ways.length === 0) {
				return;
			}
			const y =
				ways[Math.floor(countOf(qb, this.input.key, 18 + k) * ways.length)]!;
			qb.hold(qb.t + hold, "pocket", y + SET_OPEN);
		}
	}

	airYards(how: "complete" | "other", yds: number, td: boolean): number {
		const r = this.r;
		const toGoal = this.goalD / YD;
		if (how === "other") {
			return yds > 5 ? Math.round(yds * (0.75 + 0.25 * r())) : yds;
		}
		if (td && toGoal <= 30 && r() < 0.6) {
			// In the end zone.
			return Math.round(toGoal + 1 + r() * 7);
		}
		if (yds <= 0) {
			return Math.max(-6, yds - Math.round(r() * 3));
		}
		if (yds <= SCREEN_MOST && r() < SCREEN) {
			return -1 - Math.round(r() * 2);
		}
		if (yds <= 4) {
			return yds - Math.round(r() * Math.min(yds, 3));
		}
		if (yds > 15 && r() < 0.3) {
			return Math.round(5 + r() * 7);
		}
		return Math.max(1, Math.round(yds * (0.45 + 0.5 * r())));
	}

	// From the pocket: side on to it, his front shoulder at it; a step at it
	// as the ball goes back by his ear, his hips and shoulders coming round
	// through it as it goes, and his weight on over his front foot after it,
	// turned on past it.
	pocketThrow(qb: Actor, catchPt: Pt, throwAt: number, catchAt = throwAt) {
		const way = this.toward(qb.at, catchPt);
		const ahead = (ft: number) => ({
			x: qb.at.x + Math.cos(way) * ft,
			y: qb.at.y + Math.sin(way) * ft,
		});
		const w0 = throwAt - WIND_MS;
		const cocked = w0 + WIND_MS * WIND_COCKED;
		qb.hold(w0, "pocket", way + SET_OPEN);
		qb.act("throwWind", cocked, way + SET_OPEN, ahead(STRIDE * 0.75));
		qb.last.span = [0, WIND_COCKED];
		qb.turn(COIL);
		qb.act("throwWind", throwAt, way + SET_OPEN + COIL, ahead(STRIDE * 0.25));
		qb.last.span = [WIND_COCKED, 1];
		qb.turn(-(SET_OPEN + COIL - RELEASE_OPEN));
		qb.act(
			"throwFollow",
			throwAt + FOLLOW_MS,
			way + RELEASE_OPEN,
			ahead(WEIGHT_ON),
		);
		qb.turn(PAST_IT - RELEASE_OPEN);
		qb.go(ahead(WEIGHT_ON + WATCH_FT), qb.t + WATCH_MS, "walk", way);
		this.watchOn(qb, way, catchAt);
	}

	// A long one still up there once he has let it go: a look, and a few
	// more steps on after it as it comes down - not stood like a statue the
	// whole way.
	private watchOn(qb: Actor, way: number, catchAt: number) {
		if (catchAt - qb.t < WATCH_LONG_MS) {
			return;
		}
		qb.hold(
			qb.t +
				WATCH_PAUSE_MS[0] +
				WATCH_PAUSE_MS[1] * countOf(qb, this.input.key, 160),
			"stand",
			way,
		);
		const on = WATCH_ON_FT * (0.6 + 0.4 * countOf(qb, this.input.key, 161));
		qb.go(
			{ x: qb.at.x + Math.cos(way) * on, y: qb.at.y + Math.sin(way) * on },
			qb.t + WATCH_ON_MS,
			"walk",
			way,
		);
	}

	// Rolled out round the edge, on at an easy run - the ball up in both
	// hands, then cocked and let go, still going - and slowing up after.
	onTheRun(
		qb: Actor,
		side: number,
		catchPt: Pt,
		throwAt: number,
		catchAt = throwAt,
	) {
		const end = throwAt + 500;
		// (On at a quicker run than that already: on at it - not dropped
		// to it in a stride.)
		const was = qb.last;
		const v = Math.max(BOOT_FPS * 0.75, GAITS.has(was.move) ? legPace(was) : 0);
		if (qb.t > end) {
			qb.cut(end);
		} else {
			const s = this.of(qb.at);
			const room = Math.max(
				0,
				(side > 0 ? this.bounds.right : -this.bounds.left) - side * s.a - 12,
			);
			const far = Math.min(room, (v * (end - qb.t)) / 1000);
			const to = this.w({ d: s.d + far * 0.35, a: s.a + side * far * 0.94 });
			qb.go(to, end, undefined, this.toward(qb.at, to));
		}
		qb.arms("throwWind", throwAt - 320, throwAt);
		qb.arms("throwFollow", throwAt, end);
		// (Pulled up after it over a few strides, as a man slows from a
		// run - see STOP_ACCEL - not dropped to a walk in a stride; and
		// turned to watch it, stood.)
		const l = qb.last;
		if (GAITS.has(l.move) && legPace(l) >= STOP_LEAST) {
			qb.easeOff(Infinity);
			qb.hold(Math.max(qb.t, end) + 450, "stand", this.toward(qb.at, catchPt));
		} else if (end + 450 - qb.t < ONRUN_WALK_LEAST_MS) {
			qb.hold(end + 450, "stand", this.toward(qb.at, catchPt));
		} else {
			const q = this.of(qb.at);
			qb.go(
				this.w({ d: q.d + 1, a: q.a + side * 2 }),
				end + 450,
				"walk",
				this.toward(qb.at, catchPt),
			);
		}
		this.watchOn(qb, this.toward(qb.at, catchPt), catchAt);
	}

	// A screen out wide: the receivers on his side off their routes and out
	// in front of him.
	screenBlocks(target: Actor, runners: Actor[], catchAt: number) {
		const { snap } = this.input;
		const at = target.posAt(catchAt);
		const side = Math.sign(this.of(at).a) || 1;
		const mates = runners.filter(
			(m) =>
				m !== target &&
				spotOf(m.role) !== "back" &&
				Math.sign(this.of(m.posAt(snap)).a) === side,
		);
		this.leadBlocks(mates.slice(0, 2), snap + 200, at, catchAt);
	}

	// A back's screen: the line set a moment, then the rush let through on
	// to the quarterback - and the three on the side of it out in front of
	// the back.
	slipBlocks(
		blockers: Actor[],
		rushers: Actor[],
		qb: Actor,
		side: number,
		catchPt: Pt,
		throwAt: number,
		catchAt: number,
	) {
		const { snap } = this.input;
		const from = snap + SLIP_WAIT + 100;
		const line = blockers
			.filter((b) => OL.has(b.role) && !this.done.has(b.pid))
			.sort((a, b) => this.of(b.at).a * side - this.of(a.at).a * side)
			.slice(0, 3);
		const through = rushers.filter(
			(m) =>
				!this.done.has(m.pid) &&
				line.some((b) => dist(b.posAt(from), m.posAt(from)) < 5),
		);
		const thrower = qb.posAt(throwAt);
		for (const m of through) {
			m.cut(from);
			const q = this.of(thrower);
			m.chase(
				this.w({
					d: q.d + SLIP_UP_FT,
					a: q.a + (this.of(m.at).a - q.a) * 0.3,
				}),
				throwAt - SLIP_SWAT_LEAD,
				m.top * 0.9,
			);
			// (Up at it with his hands as it goes over him - carried on a
			// step off his last stride, not on into the quarterback.)
			const l = m.last;
			const far = dist(l.from, l.to);
			const on = far > 0 ? Math.min(SLIP_SWAT_ON, legPace(l) * SWAT_ON) : 0;
			m.act("swat", throwAt + SLIP_SWAT_MS, this.toward(m.at, thrower), {
				x: l.to.x + ((l.to.x - l.from.x) / Math.max(far, 1e-6)) * on,
				y: l.to.y + ((l.to.y - l.from.y) / Math.max(far, 1e-6)) * on,
			});
		}
		this.leadBlocks(line, from, catchPt, catchAt, new Set(through));
	}

	// Blockers out in front of a man catching it behind the line: each off
	// what he was doing at `from` and out onto the man nearest where he
	// catches it (each a man of his own, none of `not`) - there as it gets
	// to him, or as soon after as he can be - and on him a moment.
	// (Where a blocker meets him at c - BLOCK_GAP in front of him.)
	private inFront(m: Actor, c: number): Pt {
		const p = this.of(m.posAt(c));
		return this.w({ d: p.d - BLOCK_GAP, a: p.a });
	}

	leadBlocks(
		men: Actor[],
		from: number,
		catchPt: Pt,
		catchAt: number,
		not = new Set<Actor>(),
	) {
		const r = this.r;
		const near = this.d
			.filter((m) => !this.done.has(m.pid) && !not.has(m))
			.sort(
				(a, b) =>
					dist(a.posAt(catchAt), catchPt) - dist(b.posAt(catchAt), catchPt),
			);
		// (Not a man one of his own is on already - met where that one
		// stands: see CLIMB_CLEAR_FT.)
		const taken = (b: Actor, at: Pt, c: number) =>
			this.o.some(
				(o) =>
					o !== b &&
					dist(o.posAt(c), at) < CLIMB_CLEAR_FT &&
					(legAt(o, c).move === "passSet" || LINE_PLAY.has(legAt(o, c).move)),
			);
		for (const b of men) {
			const m = near.find((x) => !taken(b, this.inFront(x, catchAt), catchAt));
			if (!m) {
				return;
			}
			near.splice(near.indexOf(m), 1);
			b.cut(from);
			const fps = b.top * 0.9;
			// (Up to speed for it off what he was doing - out of his stance,
			// from nothing - and time to break down into him at the last: see
			// LOCK_KEEP. Or, no catching him like that, as soon as he could
			// be there up to speed as hard as he can be (see RAMP_HARD), flat
			// out to the last - not off at full speed from a stand.)
			const l = b.last;
			const off = SET.has(l.move);
			const late = ((fps * (1 - LOCK_KEEP) ** 2) / (2 * STOP_ACCEL)) * 1000;
			let meet: { t: number; at: Pt } | undefined;
			for (const up of [true, false]) {
				for (
					let c = Math.max(catchAt - 150, b.t + 300);
					c <= catchAt + 1500 && !meet;
					c += 50
				) {
					const p = this.of(m.posAt(c));
					const to = this.w({ d: p.d - BLOCK_GAP, a: p.a });
					const far = dist(b.at, to);
					const ms =
						rampedMs(
							far,
							fps,
							off ? 0 : paceOn(l, b.at, to),
							(off ? RELEASE_ACCEL : RAMP_ACCEL) * (up ? 1 : RAMP_HARD),
						) + (up ? late : 0);
					if (ms <= c - b.t) {
						meet = { t: c, at: to };
					}
				}
			}
			if (!meet) {
				continue;
			}
			// (Time to spare, a lineman: on in his set a while longer, as if
			// to block, and out to him at a pace - not walked out there.)
			if (l.move === "passSet") {
				const leave =
					meet.t - (dist(b.at, meet.at) / (b.top * LEAD_OUT)) * 1000;
				if (leave > b.t) {
					b.hold(leave, "passSet", this.ahead);
				}
			}
			b.go(meet.at, meet.t);
			// (Broken down into him and his hands pressed in, not run into
			// him flat out - see gatherTo.)
			if (
				b.gatherTo(b.at, BRAKE_FPS, HIT_ACCEL, HIT_PRESS_MS, "drive") ===
				undefined
			) {
				b.breakDown(LOCK_KEEP);
			}
			m.cut(meet.t);
			// (Pulled up into him as he comes - not stopped dead by him.)
			m.gatherTo(m.at, BRAKE_FPS, HIT_ACCEL, 0, "engage");
			const p = this.of(m.at);
			const end = Math.max(meet.t, catchAt) + SCREEN_HOLD + r() * 500;
			const give = 1 + r() * 2.5;
			m.go(this.w({ d: p.d + give, a: p.a }), end, "engage", this.behind);
			b.go(
				this.w({ d: p.d + give - BLOCK_GAP, a: p.a }),
				end,
				"drive",
				this.ahead,
			);
			b.hold(end + 400, "stand", this.ahead);
		}
	}

	// How long a route takes him, out of his stance to `upto` feet along it
	// at `speed`: a little slower off the line, down into each cut and up
	// out of it.
	// (Pulled up into the end of it, `settles`, as if he sat down there.)
	routeMs(
		route: Route,
		speed: number,
		upto: number,
		v0 = 0,
		settles = route.settles,
	): number {
		const pts = route.path.map((p) => this.w(p));
		let along = 0;
		let ms = 0;
		let cut = offLine(routeFps(speed, 1), v0);
		for (let k = 1; k < pts.length && along < upto - ROUTE_LEFT; k++) {
			const seg = dist(pts[k - 1]!, pts[k]!);
			const go = Math.min(seg, upto - along);
			const next = this.breakAt(
				pts,
				k,
				speed,
				go < seg || upto - along - go <= ROUTE_LEFT,
				settles,
				route.plant === k,
			);
			ms += stretchMs(go, routeFps(speed, k), cut, next);
			cut = next;
			along += go;
		}
		return ms;
	}

	// The break at the end of the kth stretch of a route (see breakOf) - none,
	// if it ends there (`ends`) or goes on no further: but for the time it
	// takes him to pull up there, if he sits down in it (`settles`). (Planted
	// hard there: see Route's plant.)
	private breakAt(
		pts: Pt[],
		k: number,
		speed: number,
		ends: boolean,
		settles: boolean,
		plant = false,
	): Break {
		return ends || k + 1 >= pts.length
			? {
					keep: 1,
					down: settles ? (routeFps(speed, k) / (2 * STOP_ACCEL)) * 1000 : 0,
					up: 0,
					v: settles ? 0 : routeFps(speed, k),
					accel: STOP_ACCEL,
				}
			: breakOf(
					pts[k - 1]!,
					pts[k]!,
					pts[k + 1]!,
					routeFps(speed, k),
					routeFps(speed, k + 1),
					plant,
				);
	}

	// A receiver down his route, off the line at `release`: at his route
	// speed - or, thrown to, timed to be where the ball is when it gets
	// there (slowed a little to come open on time, or sat down in it
	// waiting for it). Running on, he goes until `until`. (Where he sits
	// down for it at the last, if he works his way across to there first
	// - see workTo.)
	runRoute(
		m: Actor,
		route: Route,
		release: number,
		catchAt?: number,
		until?: number,
	): Pt | undefined {
		const speed = m.top * ROUTE_PACE;
		// (In motion at the snap, and off on his route well round from it:
		// slowed into the snap over the last of his motion, down to as much
		// of his pace as he can take round - not turned at his pace, or
		// stopped dead in it and off again from nothing.)
		{
			const l = m.last;
			const first = route.path[1];
			const turn = first ? angleAt(l.from, l.to, this.w(first)) : 0;
			if (
				Math.abs(l.t1 - release) < MOTION_SNAP_MS &&
				GAITS.has(l.move) &&
				turn > CUT_LEAST
			) {
				m.breakDown(cutKeep(turn));
			}
		}
		const v0 = this.releasePace(m, route, release);
		m.hold(release);
		const target = catchAt !== undefined;
		const endAlong = target ? route.catchAlong : Infinity;
		// (Thrown to late - the quarterback not set yet, say - with time to
		// wait for it even jogged there: pulled up into where he takes it,
		// not run into it at his pace and stopped dead in a stride.)
		const late =
			catchAt !== undefined &&
			!route.settles &&
			this.routeMs(route, speed, route.catchAlong, v0) <
				ROUTE_PACE_LEAST * (catchAt - release);
		const settles = route.settles || late;
		const pace =
			catchAt !== undefined && !route.settles
				? clamp(
						this.routeMs(route, speed, route.catchAlong, v0, settles) /
							Math.max(1, catchAt - release),
						ROUTE_PACE_LEAST,
						1,
					)
				: 1;
		const deep = this.endLineD() - ROUTE_BACK_FT;
		const path = target ? route.path : backIn(route.path, deep);
		const pts = path.map((p) => this.w(p));
		let along = 0;
		let cut = offLine(routeFps(speed, 1), v0);
		for (let k = 1; k < pts.length; k++) {
			const seg = dist(pts[k - 1]!, pts[k]!);
			const room = endAlong - along;
			// (No more than a step left of it: there already, not stood
			// marking time over a cut he does not make.)
			if (room <= ROUTE_LEFT) {
				break;
			}
			const go = Math.min(seg, room);
			const to = go < seg ? this.w(alongRoute(route, along + go)) : pts[k]!;
			const next = this.breakAt(
				pts,
				k,
				speed,
				go < seg || room - go <= ROUTE_LEFT,
				settles,
				route.plant === k,
			);
			const t1 = m.t + stretchMs(go, routeFps(speed, k), cut, next) / pace;
			if (until !== undefined && t1 > until) {
				// (Still cut into the last of it, as far as he gets: not
				// turned square at his pace - and only as far as he gets up
				// out of the cut by then, not as far as the stretch's pace
				// would have taken him from the first of it.)
				const fps = routeFps(speed, k);
				const v0 = cut.up > 0 ? cut.v : fps;
				const a0 = cut.upAccel ?? RAMP_ACCEL;
				const ts = ((until - m.t) / 1000) * pace;
				const tUp = Math.max(0, (fps - v0) / a0);
				const gone =
					ts <= tUp
						? v0 * ts + (a0 * ts * ts) / 2
						: ((v0 + fps) / 2) * tUp + fps * (ts - tUp);
				const u = go > 0 ? clamp(gone / go, 0, 1) : 1;
				const p = m.at;
				const part = { x: p.x + (to.x - p.x) * u, y: p.y + (to.y - p.y) * u };
				if (k > 1 && cut.keep < 1) {
					m.cutTo(part, until, cut.keep);
				} else {
					m.go(part, until);
				}
				return;
			}
			let out: number | undefined;
			const sits = next.v === 0 && next.down > 0;
			const vIn = legPace(m.last);
			if (
				sits &&
				k > 1 &&
				cut.keep === 1 &&
				GAITS.has(m.last.move) &&
				(vIn * vIn) / (2 * next.accel) > go
			) {
				// (See stretchMs: on at his pace, and pulled up over the last
				// of it and the way in to it.)
				m.go(to, m.t + (go / vIn) * 1000);
				m.gatherFor(0, next.accel);
				cut = next;
				along += go;
				continue;
			}
			if (k === 1) {
				m.getOff(
					to,
					t1,
					v0,
					v0 > 0 ? RAMP_ACCEL : RELEASE_ACCEL,
					gait(routeFps(speed, 1)),
				);
			} else if (cut.keep < 1) {
				out = m.cutTo(to, t1, cut.keep);
			} else {
				m.go(to, t1);
			}
			// (Sat down at the end of it: up out of the last cut and pulled
			// up into it, as he was timed to be - see breakAt.)
			if (sits) {
				m.gatherInto(out, routeFps(speed, k));
			}
			cut = next;
			along += go;
		}
		if (!target && !route.settles && until !== undefined && m.t < until) {
			// Past the end of his route: on the way it goes - as far as the
			// back of the end zone, or a sideline (see ROUTE_BACK_FT), and sat
			// down there, turned to the quarterback.
			const pa = path.at(-2)!;
			const pb = path.at(-1)!;
			const a = this.w(pa);
			const b = this.w(pb);
			const len = dist(a, b) || 1;
			const left = ((until - m.t) / 1000) * speed;
			const down = (pb.d - pa.d) / len;
			const room = down > 0 ? Math.max(0, (deep - pb.d) / down) : Infinity;
			const across = (b.y - a.y) / len;
			const side =
				across > 0
					? (FIELD_W - ROUTE_SIDE_FT - b.y) / across
					: across < 0
						? (b.y - ROUTE_SIDE_FT) / -across
						: Infinity;
			const go = Math.min(left, room, Math.max(0, side));
			const to = {
				x: b.x + ((b.x - a.x) / len) * go,
				y: b.y + ((b.y - a.y) / len) * go,
			};
			if (go >= left) {
				m.go(to, until);
			} else {
				if (go > ROUTE_LEFT) {
					m.go(to, m.t + (go / speed) * 1000);
				}
				// (Pulled up into it over the last of the way there - there a
				// little later for it - not run into it and stopped dead in a
				// stride.)
				m.gatherFor(0, STOP_ACCEL);
				const qb = this.role("QB");
				m.hold(until, "settle", qb ? this.toward(m.at, qb.at) : this.behind);
			}
		}
		if (route.settles || target) {
			// Sat down in it, turned to the quarterback.
			const qb = this.role("QB");
			const faceAt = (p: Pt) => (qb ? this.toward(p, qb.at) : this.behind);
			const t =
				catchAt !== undefined ? catchAt - CATCH_LEAD_MS : (until ?? m.t);
			const work = route.settles ? this.workTo(m, route, t) : undefined;
			if (work) {
				m.hold(m.t + work.sit, "settle", faceAt(m.at));
				m.go(work.at, t - WORK_RESET_MS, "slide", faceAt(m.at));
			}
			const end = m.at;
			m.hold(Math.max(m.t, t), "settle", faceAt(m.at));
			// (Where he sits down for it - pulled up past the end of it, if he
			// came into it at a run.)
			return work || dist(m.at, end) > 0 ? m.at : undefined;
		}
		return undefined;
	}

	// Off on `route` at `release` still on the move from what he was doing
	// (out of a fake, say), how much of his pace he has on the way it goes
	// (see paceOn) - none, out of his stance or stood there first.
	releasePace(m: Actor, route: Route, release: number): number {
		const l = m.last;
		const first = route.path[1];
		return first && release <= l.t1 && GAITS.has(l.move)
			? paceOn(l, l.to, this.w(first))
			: 0;
	}

	// Where a receiver sat down at the end of his route works his way to
	// by `t`, still facing the quarterback, and how long he sits first - if
	// he has long enough to wait there for it to be worth it (see
	// WORK_AFTER_MS). (Not a screen or a slip: the blocks are set up where
	// he sits.)
	workTo(m: Actor, route: Route, t: number) {
		const spare = t - m.t;
		if (
			spare < WORK_AFTER_MS ||
			route.name === "screen" ||
			route.name === "slip"
		) {
			return undefined;
		}
		// (Which way, and how long sat first, the same each time for him
		// there - not of the play's own draws.)
		const u = (m.pid * 0.618034 + m.t / 997) % 1;
		const sit = WORK_SIT_MS + WORK_SIT_SPREAD * u;
		const ft = Math.min(
			WORK_MOST_FT,
			(WORK_FPS * (spare - sit - WORK_RESET_MS)) / 1000,
		);
		if (ft < WORK_LEAST_FT) {
			return undefined;
		}
		const s = this.of(m.at);
		const side =
			Math.abs(s.a) < WORK_MID_FT ? (u < 0.5 ? -1 : 1) : -Math.sign(s.a);
		const back =
			route.name === "out" || route.name === "comeback"
				? WORK_BACK_OUT
				: WORK_BACK;
		const n = Math.hypot(1, back);
		const a = clamp(
			s.a + (side * ft) / n,
			this.bounds.left + WORK_IN_FT,
			this.bounds.right - WORK_IN_FT,
		);
		return { at: this.w({ d: s.d - (back * ft) / n, a }), sit };
	}

	// ---- the catch, and after it ---------------------------------------------------

	complete(
		i: number,
		qb: Actor,
		rec: Actor,
		catchPt: Pt,
		catchAt: number,
		gain: number,
	) {
		const e = this.ev(i);
		const r = this.r;
		// After the catch: up the field to where the sim says it ended -
		// carried on across the way he was going as he turns up it, and
		// toward the man who brings him down (less, the more that is back
		// against where he was going).
		const l = rec.last;
		const heading = Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x);
		const c = this.of(catchPt);
		const ha =
			this.of({
				x: catchPt.x + Math.cos(heading),
				y: catchPt.y + Math.sin(heading),
			}).a - c.a;
		const fps = Math.min(rec.top, legPace(l));
		const keep = Math.abs(ha) * Math.min(1, fps / CATCH_DRIFT_FPS);
		const ta = this.tacklerA(rec, catchAt);
		const toward = (a: number, w: number) =>
			ta === undefined ? a : a + (ta - a) * 0.5 * ((ta - a) * ha < 0 ? w : 1);
		const jit = (r() - 0.5) * 24;
		const goes = toward(
			c.a + ha * fps * CATCH_DRIFT_S + jit * (1 - keep),
			1 - keep,
		);
		const was = toward(c.a + jit, 1);
		const out = this.outSide(c.a, Math.max(OUT_NEAR, (gain * YD - c.d) * 0.8));
		const endAt = (a: number) =>
			this.endOf(gain, a, e.td === true, e.safety === true, out);
		// (Where the man who brings him down can get to him in time: further
		// on that way, if he can be there then - or else as little back
		// toward where he would have gone as lets him.)
		let across = goes;
		const by = this.statPids("defTckSolo", "defTckAst")
			.map((pid) => this.man(pid))
			.filter(
				(m): m is Actor => !!m && m.team !== rec.team && !this.done.has(m.pid),
			);
		if (by.length > 0 && !out && e.td !== true && e.safety !== true) {
			const on = keep > CATCH_FURTHER_KEEP ? CATCH_FURTHER_FT : [];
			let least = Infinity;
			for (const a of [
				goes,
				...on.map((ft) => goes + Math.sign(ha) * ft),
				...CATCH_BACK.map((u) => goes + (was - goes) * u),
			]) {
				const pt = endAt(a).pt;
				const far = dist(catchPt, pt);
				const plant = plantOn(l, catchPt, pt);
				const t1 =
					rec.t +
					plant.T * 1000 +
					rampedMs(
						Math.max(0, far - plant.on),
						rec.top * (far > 30 ? 0.95 : 0.8),
						plant.ve,
					);
				const late =
					Math.min(...by.map((m) => this.canReach(m, pt))) - (t1 - HIT_MS);
				if (late < least - 1) {
					least = late;
					across = a;
				}
				if (late <= CATCH_LATE_MS) {
					across = a;
					break;
				}
			}
		}
		let end = endAt(across);
		const yac = dist(catchPt, end.pt);
		const v = rec.top * (yac > 30 ? 0.95 : 0.8);
		// Up for it at its highest, now and then - thrown deep and covered,
		// or into the end zone.
		const deep = this.of(catchPt).d - this.of(qb.posAt(catchAt)).d;
		const covered = this.d.some(
			(m) => dist(m.posAt(catchAt), catchPt) < CONTESTED,
		);
		const leap =
			!end.out &&
			deep > LEAP_DEEP &&
			yac < LEAP_YAC &&
			(covered || e.td === true) &&
			r() < LEAP * (1 + HANDS * (rec.flair.hands ?? 0));
		// Or, on the run and down about where he would land laid out for it,
		// off his feet for it, some of the time - down there, then.
		const lands = this.of({
			x: catchPt.x + Math.cos(heading) * DIVE_FT,
			y: catchPt.y + Math.sin(heading) * DIVE_FT,
		});
		const dive =
			!leap &&
			!end.td &&
			!end.out &&
			e.safety !== true &&
			deep > DIVE_DEEP &&
			Math.abs(rec.t - catchAt) < 5 &&
			(l.from.x !== l.to.x || l.from.y !== l.to.y) &&
			Math.abs(lands.d - end.d) < DIVE_SHORT &&
			r() < DIVE_CATCH * (1 + HANDS * (rec.flair.hands ?? 0));
		if (dive) {
			end = this.endOf(gain, lands.a, false, false);
		}
		// Or caught at the sideline (see TOE_TAP).
		const edge = end.out && end.out > 0 ? this.bounds.right : this.bounds.left;
		const toe = !!end.out && Math.abs(this.of(catchPt).a - edge) < TOE_IN + 0.5;
		if (end.out) {
			rec.roam = OUT_MOST + 1;
		}
		rec.pin(catchAt);
		const inStride =
			!leap && !dive && !toe && this.reachFor(rec, catchAt, qb, false, true);
		// (Met in his stride, on past where it was thrown to: thrown to where
		// he is as it gets there - not in behind him.)
		const fly = this.ball.findLast((b) => b.kind === "fly");
		if (inStride && fly?.kind === "fly" && Math.abs(fly.t1 - catchAt) < 1) {
			const at = rec.posAt(catchAt);
			fly.to = { ...fly.to, x: at.x, y: at.y };
		}
		this.playBall(rec, catchAt, leap ? catchPt : undefined);
		if (leap) {
			this.leapFor(rec, catchPt, catchAt, qb, yac > 2 ? end.pt : undefined);
		} else if (dive) {
			this.diveFor(rec, end.pt, catchAt);
		} else if (toe) {
			this.toeTapFor(rec, end.pt, catchAt, qb);
		}
		// (Still on their way to where it was going, in their strides: off
		// it once they see it caught, after him - not on through him to
		// there.)
		const caught = catchAt + BREAK_MS;
		for (const m of this.d) {
			if (
				!this.done.has(m.pid) &&
				m.t > caught + FLICK_MS &&
				m.legs.every((l) => l.t1 <= caught || (GAITS.has(l.move) && !l.arms))
			) {
				m.cut(caught);
			}
		}
		let t = Math.max(rec.t, catchAt + 260);
		if (dive || toe) {
			t = rec.t;
		} else if (yac > 2 || inStride) {
			const off = rec.t;
			t = rec.run(end.pt, v, "sprint");
			// (The man on him as it came, not one the sim has bring him down:
			// gone for him there, and left on the turf - not along with him
			// all the way.)
			const on = yac > CATCH_BEAT_YAC ? this.onHim(rec, catchAt) : undefined;
			// (Once he is round onto the way he goes off on, if he planted to
			// turn up it - not jinked off the plant while still going the
			// other way, sent back the way he came.)
			const { from: lf, to: lt } = rec.last;
			const up = rec.legs.find(
				(l) =>
					l.t0 >= off &&
					dist(l.from, l.to) > 0 &&
					angleAt(lf, lt, {
						x: lt.x + l.to.x - l.from.x,
						y: lt.y + l.to.y - l.from.y,
					}) < CUT_LEAST,
			);
			const tc = Math.max(
				off + (inStride ? CATCH_BEAT_STRIDE_MS : CATCH_BEAT_MS),
				(up?.t0 ?? off) + JUKE_INTO_MS,
			);
			if (on && t - tc > CATCH_BEAT_LEFT_MS) {
				t = this.makeMiss(
					rec,
					on,
					on.posAt(catchAt),
					tc,
					rec.last,
					t,
					inStride ? "juke" : "spin",
				);
			}
		}
		this.ball.push({ kind: "held", t0: catchAt, pid: rec.pid });
		this.endCarry(i, rec, end, t, this.d);
	}

	// The man on him as it gets there, in his stride after him or sat
	// reading it: a hand in at it, the one on its side, too late. (His own
	// count, no draw of the play's.)
	// (Up for it: the man on him up off his feet with him - see
	// CONTEST_UP.)
	playBall(rec: Actor, catchAt: number, upAt?: Pt) {
		// (Where it gets to him: at him, in his stride - see inStride.)
		const catchPt = upAt ?? rec.posAt(catchAt);
		const near = this.d
			.filter((m) => !this.done.has(m.pid))
			.map((m) => ({ m, far: dist(m.posAt(catchAt), catchPt) }))
			.filter((x) => x.far < CONTEST_FT && x.far >= CONTEST_LEAST_FT)
			.sort((a, b) => a.far - b.far)[0];
		if (!near || countOf(near.m, this.input.key, 82) >= CONTEST) {
			return;
		}
		const { m, far } = near;
		const l = legAt(m, catchAt);
		const run = dist(l.from, l.to);
		const face =
			l.yaw ??
			(run > 0 ? Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x) : undefined);
		if (!ON_FEET.has(l.move) || face === undefined) {
			return;
		}
		const at = m.posAt(catchAt);
		const hx = Math.cos(face);
		const hy = Math.sin(face);
		const bx = (catchPt.x - at.x) / far;
		const by = (catchPt.y - at.y) / far;
		if (hx * bx + hy * by < CONTEST_BEHIND) {
			return;
		}
		if (upAt && this.upWith(m, rec, upAt, catchAt)) {
			return;
		}
		// (On his right, or his left - see carryArm.)
		const right = hx * by - hy * bx > 0;
		this.reaches.push({
			m,
			move: right ? "playBall" : "playBallL",
			t0: catchAt - CONTEST_FROM_MS,
			t1: catchAt + CONTEST_TO_MS,
			onFeet: true,
		});
	}

	// A jump ball fought for: the man on him up off both feet for it with
	// him, arms up at it, on his own count a beat either side of him - from
	// his stride on toward it as far as it carries him, but never into the
	// man going up for it (see leapFor: where he goes up from, and on
	// through it to). False, if there is no way up for him clear of him.
	upWith(m: Actor, rec: Actor, catchPt: Pt, catchAt: number): boolean {
		const t0 =
			catchAt -
			CONTEST_UP_MS / 2 +
			(countOf(m, this.input.key, 123) - 0.5) * 2 * CONTEST_UP_SPREAD_MS;
		const r0 = catchAt - LEAP_MS / 2;
		const rFrom = rec.posAt(r0);
		const rFar = dist(rFrom, catchPt);
		const rTo =
			rFar > 1
				? {
						x: catchPt.x + (catchPt.x - rFrom.x) * 0.9,
						y: catchPt.y + (catchPt.y - rFrom.y) * 0.9,
					}
				: rFrom;
		const from = m.posAt(t0);
		const was = m.posAt(t0 - 100);
		const v = dist(from, was) * 10;
		const way = this.toward(from, catchPt);
		const recAt = (t: number) => {
			const u = clamp((t - r0) / LEAP_MS, 0, 1);
			return {
				x: rFrom.x + (rTo.x - rFrom.x) * u,
				y: rFrom.y + (rTo.y - rFrom.y) * u,
			};
		};
		for (
			let go = Math.min(
				CONTEST_UP_MOST_FT,
				v * CONTEST_UP_CARRY,
				Math.max(0, dist(from, catchPt) - CONTEST_UP_APART),
			);
			go >= 0;
			go -= 0.5
		) {
			const to = {
				x: from.x + Math.cos(way) * go,
				y: from.y + Math.sin(way) * go,
			};
			let clear = true;
			for (let k = 0; k <= 8 && clear; k++) {
				const t = t0 + (CONTEST_UP_MS * k) / 8;
				const u = k / 8;
				const at = {
					x: from.x + (to.x - from.x) * u,
					y: from.y + (to.y - from.y) * u,
				};
				clear = dist(at, recAt(t)) >= CONTEST_UP_APART;
			}
			if (!clear) {
				continue;
			}
			m.cut(t0);
			m.act("leap", t0 + CONTEST_UP_MS, way, to);
			return true;
		}
		return false;
	}

	// The man of the other side on a receiver as he catches it, if any: near
	// enough to go for him there, on his feet, and not one the sim has bring
	// him down.
	onHim(rec: Actor, catchAt: number): Actor | undefined {
		const tacklers = this.statPids("defTckSolo", "defTckAst");
		const at = rec.posAt(catchAt);
		const near = (m: Actor) => dist(m.posAt(catchAt), at);
		return this.men
			.filter(
				(m) =>
					m.team !== rec.team &&
					!this.done.has(m.pid) &&
					!tacklers.includes(m.pid) &&
					moveKind(legAt(m, catchAt).move) !== "act" &&
					near(m) < CATCH_BEAT_FT,
			)
			.sort((a, b) => near(a) - near(b))[0];
	}

	// Laid out for it: off his feet from a stride or two short, his arms out
	// for it as it comes in low, and down with it on the turf at `at`.
	diveFor(rec: Actor, at: Pt, catchAt: number, yaw?: number) {
		const t0 = catchAt - DIVE_BEFORE;
		rec.cut(t0);
		rec.hold(t0);
		rec.act("diveCatch", t0 + DIVE_CATCH_MS, yaw ?? rec.yaw, at);
		rec.hold(rec.t + 200, "prone");
		// (The ball thrown to his hands, out there.)
		const fly = this.ball.findLast((b) => b.kind === "fly");
		if (fly?.kind === "fly" && Math.abs(fly.t1 - catchAt) < 1) {
			const p = rec.posAt(catchAt);
			const c = Math.cos(rec.yaw);
			const s = Math.sin(rec.yaw);
			const h = DIVE_HANDS;
			fly.to = {
				x: p.x + h.f * c + h.s * s,
				y: p.y + h.f * s - h.s * c,
				z: h.u + liftOf("diveCatch", DIVE_AT),
			};
		}
	}

	// Caught at the sideline: turned to the quarterback, his hands up to it
	// as it comes, his toes dragged in as he goes on over the line - tipped
	// toward it - to `over`; and the ball thrown to his hands there.
	toeTapFor(rec: Actor, over: Pt, catchAt: number, qb: Actor) {
		const t0 = catchAt - TOE_BEFORE;
		rec.cut(t0);
		rec.hold(t0);
		const from = rec.at;
		const face = this.toward(from, qb.posAt(catchAt));
		rec.act("toeTap", t0 + TOE_MS, face, over);
		// (Tipped to his right as it is drawn: the line on his left, the
		// other way round.)
		const goes = Math.atan2(over.y - from.y, over.x - from.x);
		if (Math.sin(goes - face) < 0) {
			rec.last.mirrored = true;
		}
		const fly = this.ball.findLast((b) => b.kind === "fly");
		if (fly?.kind === "fly" && Math.abs(fly.t1 - catchAt) < 1) {
			const p = rec.posAt(catchAt);
			fly.to = {
				...fly.to,
				x: p.x + Math.cos(face) * 1.2,
				y: p.y + Math.sin(face) * 1.2,
			};
		}
	}

	// Up off both feet for it at its highest: from wherever he is as it
	// comes, on through the spot it is caught at (or, stood there, straight
	// up), and the ball thrown up there to his hands.
	// (`next`: where he goes once he is down with it, if anywhere.)
	leapFor(rec: Actor, catchPt: Pt, catchAt: number, qb: Actor, next?: Pt) {
		const t0 = catchAt - LEAP_MS / 2;
		rec.cut(t0);
		rec.hold(t0);
		const from = rec.at;
		const far = dist(from, catchPt);
		const ang = Math.atan2(catchPt.y - from.y, catchPt.x - from.x);
		// (To go back the other way once he is down: come down slowing to a
		// stop - not carried on at the pace he went up at and turned round
		// on the spot; off well round from it, see LEAP_ON_COS, slowing to
		// LEAP_TURN_FPS.)
		const down = {
			x: catchPt.x + Math.cos(ang) * far * 0.9,
			y: catchPt.y + Math.sin(ang) * far * 0.9,
		};
		const offCos =
			far > 1 && next !== undefined && dist(down, next) > 1
				? Math.cos(Math.atan2(next.y - down.y, next.x - down.x) - ang)
				: 1;
		const back = offCos < LEAP_ON_COS;
		const T = LEAP_MS / 2;
		const v = far / T;
		const vEnd = offCos < 0 ? 0 : Math.min(v, LEAP_TURN_FPS / 1000);
		const on = back ? 0.5 * (1 + vEnd / v) : 0.9;
		const to =
			far > 1
				? {
						x: catchPt.x + Math.cos(ang) * far * on,
						y: catchPt.y + Math.sin(ang) * far * on,
					}
				: from;
		// (Faced the way he goes, going at it; or round to the quarterback
		// - but not, going away from him at all, round in the air to him
		// and carried on backward: it over his shoulder.)
		const toQb = this.toward(from, qb.posAt(catchAt));
		rec.act(
			"leapCatch",
			t0 + LEAP_MS,
			far > 4 || (far > 1 && Math.cos(ang - toQb) < LEAP_AWAY_COS) ? ang : toQb,
			to,
		);
		if (back) {
			// (Up to it at his pace, and down from it in LEAP_LAND_STEPS
			// legs slowing evenly from that to vEnd.)
			const along = (ms: number) => v * ms - ((v - vEnd) * ms * ms) / (2 * T);
			const ts = [catchAt];
			for (let k = 1; k < LEAP_LAND_STEPS; k++) {
				ts.push(catchAt + (T * k) / LEAP_LAND_STEPS);
			}
			for (const t of ts) {
				rec.split(t);
			}
			const legs = rec.legs.slice(-1 - LEAP_LAND_STEPS);
			legs[0]!.to = catchPt;
			for (let k = 1; k < legs.length; k++) {
				const l = legs[k]!;
				const a = along(l.t0 - catchAt);
				const b = along(l.t1 - catchAt);
				l.from = {
					x: catchPt.x + Math.cos(ang) * a,
					y: catchPt.y + Math.sin(ang) * a,
				};
				l.to =
					k === legs.length - 1
						? to
						: {
								x: catchPt.x + Math.cos(ang) * b,
								y: catchPt.y + Math.sin(ang) * b,
							};
			}
		}
		const fly = this.ball.findLast((b) => b.kind === "fly");
		if (fly?.kind === "fly" && Math.abs(fly.t1 - catchAt) < 1) {
			fly.to = { ...fly.to, z: LEAP_Z };
		}
	}

	// The man it goes to, open for it a long way off: his hand up for it as
	// the quarterback gets set to throw it - the surer the better his hands
	// are, where the game rates them (the 3D court's shooter calling for it:
	// see its director.ts). No draw of the play's: his own count.
	callFor(
		rec: Actor,
		qb: Actor,
		coverage: Actor[],
		catchPt: Pt,
		throwAt: number,
		catchAt: number,
	) {
		const { snap } = this.input;
		if (dist(qb.posAt(throwAt), catchPt) < CALL_FAR) {
			return;
		}
		const t0 = Math.max(throwAt - CALL_FROM_MS, snap + CALL_LEAST_MS);
		const up = throwAt - CALL_TO_MS;
		if (up - t0 < CALL_SHORTEST_MS) {
			return;
		}
		const t1 = Math.max(
			up,
			Math.min(
				throwAt + CALL_DOWN_MS,
				catchAt - CATCH_REACH_MS - CALL_CLEAR_MS,
			),
		);
		const open = (t: number) =>
			coverage.every((m) => dist(m.posAt(t), rec.posAt(t)) >= CALL_OPEN);
		const hands = rec.flair.hands;
		const odds =
			hands === undefined
				? CALL_ODDS
				: CALL_LOW + CALL_SPAN * ((hands + 1) / 2);
		if (!open(t0) || !open(up) || countOf(rec, this.input.key, 12) >= odds) {
			return;
		}
		this.reaches.push({ m: rec, move: "callFor", t0, t1 });
	}

	// Sat down in his route waiting on it, on his own count now and then
	// (see STEP_IN): a step or two up into it as it comes, toward the
	// quarterback, his hands out for it on the way - caught on the move, not
	// stood stock still for it to get to him. Not into a man on him, nor
	// off a run he is still pulling up out of. (Whether he did.)
	private stepIn(rec: Actor, catchAt: number, face: number): boolean {
		const l = rec.last;
		const t0 = Math.max(catchAt - STEP_IN_MS, l.t0 + STEP_IN_SAT_MS);
		if (
			!sits(l.move) ||
			l.arms ||
			l.turn ||
			l.from.x !== l.to.x ||
			l.from.y !== l.to.y ||
			catchAt - t0 < STEP_IN_LEAST_MS ||
			rec.t > catchAt ||
			countOf(rec, this.input.key, 152) >= STEP_IN
		) {
			return false;
		}
		const ft =
			((STEP_IN_FT[0] +
				(STEP_IN_FT[1] - STEP_IN_FT[0]) * countOf(rec, this.input.key, 153)) *
				(catchAt - t0)) /
			STEP_IN_MS;
		const to = {
			x: rec.at.x + Math.cos(face) * ft,
			y: rec.at.y + Math.sin(face) * ft,
		};
		if (
			this.men.some(
				(x) =>
					x !== rec &&
					[0.5, 1, 1.4].some(
						(u) =>
							dist(x.posAt(t0 + (catchAt - t0) * u), {
								x: rec.at.x + (to.x - rec.at.x) * Math.min(1, u),
								y: rec.at.y + (to.y - rec.at.y) * Math.min(1, u),
							}) < STEP_IN_CLEAR_FT,
					),
			)
		) {
			return false;
		}
		if (rec.t > t0) {
			rec.cut(t0);
		}
		rec.hold(t0, undefined, face);
		rec.go(to, catchAt, "walk", face);
		this.reaches.push({
			m: rec,
			move: "catch",
			t0: catchAt - CATCH_REACH_MS,
			t1: catchAt + CATCH_IN_MS,
		});
		return true;
	}

	// Reaching for it as it comes - in stride, his arms up for it as he
	// runs on, or sat down in his route waiting on it, and in to his chest.
	// Whether in stride.
	// (`up`: going on over his hands - up at it, and not in to him.)
	// (`step`: sat down waiting on it, now and then stepping up into it as
	// it comes - see STEP_IN.)
	reachFor(
		rec: Actor,
		catchAt: number,
		qb: Actor | undefined,
		up = false,
		step = false,
	): boolean {
		const reach = catchAt - CATCH_REACH_MS;
		// (On past it already, on the move - it taken in front of him,
		// picked off, say: reaching as it goes by, in his stride - not run on
		// past it and stopped dead for it after.)
		if (rec.t > catchAt) {
			const at = legAt(rec, catchAt);
			if (GAITS.has(at.move) && legPace(at) >= STOP_LEAST) {
				rec.cut(catchAt);
			}
		}
		rec.pin(catchAt);
		// (There a moment early, on the move: on the way he was going to
		// meet it, in his stride - not stopped dead for it.)
		const was = rec.last;
		if (
			GAITS.has(was.move) &&
			rec.t < catchAt &&
			((catchAt - rec.t) / 1000) * legPace(was) <= CATCH_ON_FT
		) {
			rec.carryOn(catchAt);
		}
		const l = rec.last;
		if (
			Math.abs(rec.t - catchAt) < 5 &&
			(l.from.x !== l.to.x || l.from.y !== l.to.y)
		) {
			this.reaches.push({
				m: rec,
				move: up ? "reachUp" : "catch",
				t0: reach,
				t1: catchAt + (up ? REACH_UP_AFTER_MS : CATCH_IN_MS),
			});
			return true;
		}
		const face = qb ? this.toward(rec.at, qb.at) : rec.yaw;
		if (step && qb && !up && this.stepIn(rec, catchAt, face)) {
			return true;
		}
		// (Getting there as he reaches for it, or no more than a moment
		// before - see hold: pulled up into it, not stopped dead. Sat down
		// waiting on it, reaching as it comes - not once it is there.)
		rec.hold(reach);
		rec.pullUpOn();
		const w = rec.last;
		if (
			w.t0 < catchAt &&
			rec.t > reach &&
			w.from.x === w.to.x &&
			w.from.y === w.to.y &&
			moveKind(w.move) !== "act"
		) {
			rec.cut(Math.max(reach, w.t0));
			rec.hold(reach);
		}
		if (up) {
			rec.hold(catchAt + REACH_UP_AFTER_MS, undefined, face);
			this.reaches.push({
				m: rec,
				move: "reachUp",
				t0: reach,
				t1: catchAt + REACH_UP_AFTER_MS,
			});
			return false;
		}
		rec.catchOn(catchAt, face);
		return false;
	}

	// The man it was for, on his own count this often (see countOf): pulled
	// up, round to the quarterback, and his palms up at him over it.
	palmsUp(rec: Actor, qb: Actor, share: number) {
		if (countOf(rec, this.input.key, 129) >= share) {
			return;
		}
		this.slowUp(rec);
		const at = rec.t + INC_PALMS_TURN_MS;
		rec.hold(at, "stand", this.toward(rec.at, qb.posAt(at)));
		rec.act(
			"protest",
			at + INC_PALMS_MS,
			this.toward(rec.at, qb.posAt(at + INC_PALMS_MS)),
		);
	}

	// On a few steps the way he was going, slowing up.
	// From what he is doing to a stop over a step or two, a walk the way he
	// was going.
	slowUp(m: Actor) {
		const l = m.last;
		// (Out of an act he came into quicker, off what it leaves him going
		// at - see easeOff.)
		if (
			moveKind(l.move) === "act" &&
			carriedIn(m.legs, m.legs.length - 1) > 1
		) {
			m.easeOff(SLOW_UP_FT);
			return;
		}
		const ang =
			l.from.x === l.to.x && l.from.y === l.to.y
				? m.yaw
				: Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x);
		m.go(
			{
				x: m.at.x + Math.cos(ang) * SLOW_UP_FT,
				y: m.at.y + Math.sin(ang) * SLOW_UP_FT,
			},
			m.t + 450,
			"walk",
		);
	}

	runOn(m: Actor, ms: number) {
		const l = m.last;
		const v = dist(l.from, l.to) / Math.max(0.001, (l.t1 - l.t0) / 1000);
		const ang = Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x);
		const far = v * 0.6 * (ms / 1000);
		m.go(
			{ x: m.at.x + Math.cos(ang) * far, y: m.at.y + Math.sin(ang) * far },
			m.t + ms,
		);
	}

	// Where a carry ends, given its yards from the line: in the field of
	// play, short of the sidelines - or a score, in the end zone; or a
	// safety, down in his own.
	endOf(
		yds: number,
		across: number,
		td: boolean,
		safety: boolean,
		out?: Sign,
	): End {
		const r = this.r;
		let d = yds * YD;
		if (td) {
			d = this.goalD + 4 + r() * 14;
		} else if (safety) {
			d = this.ownGoalD - SAFETY_IN[0] - r() * SAFETY_IN[1];
		}
		d = clamp(d, this.ownGoalD - 27, this.goalD + 27);
		if (out && !td && !safety) {
			const a =
				out > 0 ? this.bounds.right + OUT_PAST : this.bounds.left - OUT_PAST;
			return { pt: this.w({ d, a }), td, d, out };
		}
		const a = clamp(across, this.bounds.left + 6, this.bounds.right - 6);
		return { pt: this.w({ d, a }), td, d };
	}

	// Out of bounds, if the sim says he got there - over the sideline
	// nearer `a` (in the offense's frame), if it is no more than `reach`
	// across the field.
	outSide(a: number, reach: number): Sign | undefined {
		if (!this.input.out) {
			return undefined;
		}
		const { left, right } = this.bounds;
		const side: Sign = right - a < a - left ? 1 : -1;
		return Math.min(right - a, a - left) <= reach ? side : undefined;
	}

	// Where across the field (in the offense's frame) the man the sim says
	// made the tackle is at t - if it names one on the other side.
	tacklerA(carrier: Actor, t: number): number | undefined {
		const m = this.man(this.statPids("defTckSolo", "defTckAst")[0]);
		return m && m.team !== carrier.team ? this.of(m.posAt(t)).a : undefined;
	}

	// The end of a carry: brought down by `by` (by default the men the sim
	// says made the tackle) - or, a score, by nobody - and the whistle.
	endCarry(
		i: number,
		carrier: Actor,
		end: { pt: Pt; td: boolean; out?: Sign },
		t: number,
		chasers: Actor[],
		by?: Actor[],
		ramped = false,
	) {
		if (end.out) {
			this.runOut(i, carrier, end.pt, end.out, t, chasers, by);
			return;
		}
		if (end.td) {
			if (this.heap > 0) {
				this.overTop(i, carrier, chasers);
				return;
			}
			const late = this.evade(carrier, [], chasers, t);
			// Into the end zone, slowing up, the ball held up.
			const goal = this.goalLineX();
			const crossT = this.crossing(carrier, goal, late);
			this.at.set(i, crossT);
			this.whistle = crossT;
			const p = carrier.at;
			const ahead = {
				x: p.x + Math.cos(carrier.yaw) * 10,
				y: p.y + Math.sin(carrier.yaw) * 10,
			};
			carrier.go(ahead, late + 900, "jog");
			carrier.hold(late + 2600, "stand");
			this.done.add(carrier.pid);
			this.pullUp(chasers, carrier, crossT);
			return;
		}
		const down = this.downBy(
			carrier,
			end.pt,
			this.evade(carrier, by ?? [], chasers, t, ramped),
			chasers,
			by,
		);
		this.at.set(i, down.t);
		this.whistle = down.t;
		const fumI = this.fumbleAfter(i);
		if (fumI >= 0) {
			this.fumbled(fumI, carrier, end.pt, down.t, down.by, chasers);
			return;
		}
		// (A quarterback out in the open, as often as not, down feet first
		// before they get to him.)
		const l = carrier.last;
		if (
			carrier.role === "QB" &&
			moveKind(l.move) === "cycle" &&
			dist(l.from, l.to) > SLIDE_FROM &&
			this.r() < QB_SLIDE
		) {
			this.slideDown(carrier, down.by, end.pt, down.t);
			this.pursue(chasers, end.pt, down.t, down.by, carrier);
			return;
		}
		if (
			this.heap === 0 &&
			this.standUp(carrier, down.by, chasers, end.pt, down.t)
		) {
			this.pursue(chasers, end.pt, down.t, down.by, carrier);
			return;
		}
		const pile = this.tackle(carrier, down.by, end.pt, down.t);
		this.gangUp(chasers, pile, down.t, down.by, carrier);
		this.pursue(chasers, end.pt, down.t, down.by, carrier);
		if (this.heap > 0) {
			this.pileUp(carrier, pile, down.t, this.heap);
		}
	}

	// Out of bounds at `pt`, over the sideline on his `side`: the whistle
	// as he gets there, the men after him there when they can be (his run
	// slowed to it); on over it, slowing, and pulled up - the first of them
	// into him with both hands as he goes, or pulled up at the line.
	runOut(
		i: number,
		carrier: Actor,
		pt: Pt,
		side: Sign,
		t: number,
		chasers: Actor[],
		by?: Actor[],
	) {
		const r = this.r;
		const down = this.downBy(carrier, pt, t, chasers, by);
		this.at.set(i, down.t);
		this.whistle = down.t;
		this.outAt = pt;
		// On the way he was going, turned along the line.
		const l = carrier.last;
		const from = this.of(l.from);
		const to = this.of(pt);
		const len = Math.hypot(to.d - from.d, to.a - from.a) || 1;
		const dd = (to.d - from.d) / len;
		const da = Math.max(0.35, ((to.a - from.a) / len) * side);
		const edge = side > 0 ? this.bounds.right : this.bounds.left;
		// (Not so far into the team crowd on the near sideline that the
		// camera loses him behind it.)
		const most =
			pt.y > MID_Y &&
			pt.x > TEAM_AREA[0] - OUT_CROWD &&
			pt.x < TEAM_AREA[1] + OUT_CROWD
				? OUT_MOST_NEAR
				: OUT_MOST;
		const over = (a: number) =>
			side > 0 ? Math.min(edge + most, a) : Math.max(edge - most, a);
		// (Off a catch with his toes in, a stride or two on.)
		const toe = l.move === "toeTap";
		// Slowed from his pace to a stop, turned along the line as he goes -
		// sooner, the less room there is over it: not run into the crowd,
		// nor stopped dead against it.
		const v = toe
			? OUT_TOE_FPS
			: Math.max(
					OUT_LEAST_FPS,
					(dist(l.from, l.to) / Math.max(1, l.t1 - l.t0)) * 1000,
				);
		const far = clamp(
			(v * v) / (2 * OUT_DECEL),
			OUT_EASE_FT[0],
			OUT_EASE_FT[1],
		);
		const T = (2 * far) / v;
		const n = Math.max(2, Math.round((T * 1000) / STOP_STEP_MS));
		const dt = T / n;
		const h0 = Math.atan2(side * da, dd);
		const h1 = dd < 0 ? Math.PI : 0;
		const turn = Math.atan2(Math.sin(h1 - h0), Math.cos(h1 - h0));
		const room = Math.abs(edge + side * most - to.a);
		const pathOf = (by: number) => {
			const out: OPt[] = [];
			let p = to;
			for (let k = 0; k < n; k++) {
				const h = h0 + turn * Math.min(1, (k + 0.5) / (n * by));
				const ft = v * (1 - (k + 0.5) / n) * dt;
				p = { d: p.d + Math.cos(h) * ft, a: p.a + Math.sin(h) * ft };
				out.push(p);
			}
			return out;
		};
		let path = pathOf(OUT_TURN_BY[0]);
		for (const by of OUT_TURN_BY) {
			path = pathOf(by);
			if (path.every((p) => side * (p.a - to.a) <= room)) {
				break;
			}
		}
		const t0 = carrier.t;
		path.forEach((p, k) => {
			carrier.go(
				this.w({ d: p.d, a: over(p.a) }),
				t0 + dt * 1000 * (k + 1),
				gait(v * (1 - (k + 0.5) / n)),
			);
		});
		const p2 = path.at(-1)!;
		carrier.hold(
			down.t + 2200,
			"stand",
			this.toward(this.w(p2), this.w({ d: p2.d, a: 0 })),
		);
		this.done.add(carrier.pid);
		const [first, ...rest] = down.by;
		const inside = (m: Actor, k: number) => {
			const at = carrier.posAt(down.t - 60);
			const o = this.of(at);
			const hit = this.w({ d: o.d - 1.5 - k * 2.5, a: o.a - side * (2.2 + k) });
			this.closeOn(m, hit, down.t - 60, this.toward(hit, at), carrier);
		};
		if (first) {
			inside(first, 0);
			if (r() < SHOVE) {
				// Both hands into him, and through it: on over the line with
				// him the way he came, slowing (see STOP_ACCEL) - not stopped
				// dead on it, nor pulled back over it after.
				first.roam = Math.max(first.roam, most);
				first.easeOff(SHOVE_ON_FT);
				first.arms("shove", down.t - 260, down.t + 240);
			}
			first.hold(down.t + 1500, "stand");
			this.done.add(first.pid);
		}
		for (const [k, m] of rest.entries()) {
			inside(m, k + 1);
			m.hold(down.t + 1300, "stand");
			this.done.add(m.pid);
		}
		const near = this.w({ d: to.d, a: to.a - side * 8 });
		// (After him toward the line, on over it as far as it takes them to
		// pull up - as far as he may go - not stopped dead on it.)
		for (const m of chasers) {
			m.roam = Math.max(m.roam, most);
		}
		this.pursue(chasers, near, down.t, down.by, carrier);
	}

	// Making a man miss on the way, now and then on a long enough carry: one
	// of `chasers` (not the men who bring him down) there to meet him
	// partway along his last stretch - and a cut away from him, a plant and
	// off the other way, the man left on the turf behind him; or, coming at
	// his free arm, put on his back with a stiff arm; or a spin out of his
	// hands, or over the top of him gone low. Gives when the carry (to have
	// ended at t) ends now: a cut or a spin costs him a beat.
	evade(
		carrier: Actor,
		by: Actor[],
		chasers: Actor[],
		t: number,
		ramped = false,
	): number {
		const r = this.r;
		let l = carrier.last;
		// (`ramped`: a return still getting up to speed to the end of it -
		// its steps up, one stretch the one way, as one.)
		if (ramped && dist(l.from, l.to) < EVADE_SHORT_FAR) {
			const legs = carrier.legs;
			const way = Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x);
			let k = legs.length - 1;
			while (k > 0) {
				const p = legs[k - 1]!;
				const q = legs[k]!;
				const pw = Math.atan2(p.to.y - p.from.y, p.to.x - p.from.x);
				if (
					!GAITS.has(p.move) ||
					p.arms !== undefined ||
					p.ball !== l.ball ||
					Math.abs(p.t1 - q.t0) > 1 ||
					dist(p.from, p.to) < 0.05 ||
					Math.abs(Math.atan2(Math.sin(pw - way), Math.cos(pw - way))) >
						EVADE_RAMP_TURN
				) {
					break;
				}
				k--;
			}
			l = { ...l, from: legs[k]!.from, t0: legs[k]!.t0 };
		}
		const { t1 } = l;
		const ms = t1 - l.t0;
		// (On a shorter stretch, less often, and only a quick one: a cut or
		// an arm out - no room to go round or over him.)
		const far = dist(l.from, l.to);
		const short = far < EVADE_FAR || ms < EVADE_MS;
		// (A game with the tacklers rated: see missable.)
		const rated = chasers.some((c) => c.flair.tackle !== undefined);
		if (
			moveKind(l.move) !== "cycle" ||
			far < EVADE_SHORT_FAR ||
			ms < EVADE_SHORT_MS ||
			r() >
				(short ? EVADE_SHORT : EVADE) *
					evasive(carrier) *
					(rated ? MISSED_MOST : 1)
		) {
			return t;
		}
		// (Not the men the sim has bring him down.)
		const tacklers = this.statPids("defTckSolo", "defTckAst");
		const ang = Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x);
		// (With room after it to get back on his way - not a cut, and
		// straight back across at the man ahead.)
		const most = 1 - EVADE_AFTER_FT / far;
		const u = Math.min(0.3 + r() * 0.35, most);
		if (u < EVADE_FIRST) {
			return t;
		}
		const tc = l.t0 + ms * u;
		const at = carrier.posAt(tc);
		const m = chasers
			.filter(
				(c) =>
					c.team !== carrier.team &&
					!this.done.has(c.pid) &&
					!by.includes(c) &&
					!tacklers.includes(c.pid) &&
					c.t < tc - 400,
			)
			.map((c) => ({
				c,
				t: this.canReach(c, at),
				from: c.posAt(Math.max(this.seen, Math.min(c.t, tc - 700))),
			}))
			// (Coming at him, not from behind - on a return, from no further
			// round than his side, a hand on him: spun out of - and able to
			// get to him in time, off what he is doing: see closeOn.)
			.filter(
				(x) =>
					x.t < tc - 250 &&
					Math.cos(Math.atan2(x.from.y - at.y, x.from.x - at.x) - ang) >
						(ramped ? EVADE_RAMP_BEHIND : -0.3) &&
					this.closeFrom(x.c, at, tc - 300).need <= x.c.top * CLOSE,
			)
			.sort((a, b) => a.t - b.t)[0];
		if (
			!m ||
			(rated && countOf(m.c, this.input.key, 142) * MISSED_MOST > missable(m.c))
		) {
			return t;
		}
		const behind =
			Math.cos(Math.atan2(m.from.y - at.y, m.from.x - at.x) - ang) <= -0.3;
		return this.makeMiss(
			carrier,
			m.c,
			m.from,
			tc,
			l,
			t,
			behind ? "spin" : short ? "quick" : undefined,
		);
	}

	// `m`, coming from `from`, made to miss the carrier at tc on his stretch
	// `l` (see evade) - by the move `how`, if it has to be that one ("quick":
	// a stiff arm or a juke). Gives when the carry (to have ended at t) ends
	// now.
	makeMiss(
		carrier: Actor,
		m: Actor,
		from: Pt,
		tc: number,
		l: Leg,
		t: number,
		how?: "spin" | "juke" | "quick",
	): number {
		const r = this.r;
		const { t1 } = l;
		const ms = t1 - l.t0;
		const ang = Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x);
		const at = carrier.posAt(tc);
		const toHim = Math.atan2(from.y - at.y, from.x - at.x);
		// (Which side he comes from: +1 the side the ball is carried on, his
		// right.)
		const side = Math.sin(toHim - ang) > 0 ? 1 : -1;
		const beside = (p: Pt, a: number, by: number) => ({
			x: p.x + Math.cos(a) * by,
			y: p.y + Math.sin(a) * by,
		});
		// (Where he leaves his feet, this far out - no further than he is.)
		const out = (ft: number) =>
			Math.max(HIT_LEAST_FT, Math.min(ft, dist(from, at)));
		// (Down by `t1` - or, late there, off his feet no quicker than a man
		// gets down (see DIVE_LEAST_MS): late into it, by him, not thrown at
		// it in a blink.)
		const diveBy = (t1: number) => Math.max(t1, m.t + DIVE_LEAST_MS);
		// (And no further than he can throw himself off the pace he is at -
		// see DIVE_STILL_FT, LUNGE_AIR_MS - pulled up first, not launched as
		// far from a stand as off a sprint.)
		const reach = (p: Pt) => {
			const v = GAITS.has(m.last.move) ? legPace(m.last) : 0;
			const most = DIVE_STILL_FT + (v * LUNGE_AIR_MS) / 1000;
			const d = dist(m.at, p);
			return v >= LUNGE_LEAST_FPS || d <= most
				? p
				: {
						x: m.at.x + ((p.x - m.at.x) * most) / d,
						y: m.at.y + ((p.y - m.at.y) * most) / d,
					};
		};
		if (
			(!how || how === "quick") &&
			side < 0 &&
			r() < STIFF_ARM * (1 + STRONG * (carrier.flair.power ?? 0))
		) {
			const hit = beside(at, ang - Math.PI / 2, 2.6);
			const face = this.toward(hit, at);
			this.closeOn(m, hit, tc - 80, face, carrier);
			// (Back off it as he was slow - or quicker, on the way he was
			// going as much as he was over BACK_EVEN_FPS, as a man met head on
			// is (see drivenBy): not stopped dead off a sprint. On at the
			// carrier, though, no more than STIFF_ARM_CARRY_FT: the arm keeps
			// him off.)
			const ml = m.last;
			const v = legPace(ml);
			const mf = dist(ml.from, ml.to);
			const u =
				mf > 0
					? { x: (ml.to.x - ml.from.x) / mf, y: (ml.to.y - ml.from.y) / mf }
					: { x: Math.cos(face), y: Math.sin(face) };
			const into = u.x * Math.cos(face) + u.y * Math.sin(face);
			const on = Math.min(
				into > STIFF_ARM_AT ? STIFF_ARM_CARRY_FT : TACKLED_MOST,
				Math.max(0, drivenBy(v, 0)),
			);
			const back = Math.min(0, drivenBy(v, 1.6));
			m.act("tackledBack", tc + 520, face, {
				x: hit.x + Math.cos(face) * back + u.x * on,
				y: hit.y + Math.sin(face) * back + u.y * on,
			});
			m.hold(tc + 520 + 600, "supine");
			m.act("getUpBack", tc + 520 + 600 + GET_UP_MS);
			carrier.arms("stiffArm", tc - 280, tc + 300);
		} else {
			const { to, move } = l;
			// (Off it as fast as he went into it - in the gait he goes at, if
			// that is less.)
			const v = Math.min(carrier.top, dist(l.from, to) / (ms / 1000));
			// (Back up to it off what the move left him going at - see
			// rampInto - not shot out of it at full tilt.)
			const on = () => {
				const far = dist(carrier.at, to);
				const v0 = Math.min(v, paceOn(carrier.last, carrier.at, to));
				const at = Math.max(t1, carrier.t + rampedMs(far, v, v0));
				const fps = far / Math.max(0.001, (at - carrier.t) / 1000);
				carrier.go(to, at, GAITS.has(move) ? gait(fps) : move);
			};
			// (The slipperier he is, the more he goes over a man or round
			// him rather than only cutting away.)
			const lvl = elusiveness(carrier.rated);
			const fancy = lvl === undefined ? 1 : FANCY_LEAST + lvl;
			const hurdle = HURDLE * fancy;
			const spin = SPIN * fancy;
			const u =
				how === "spin" ? hurdle : how === "juke" || how === "quick" ? 1 : r();
			const half = HURDLE_MS / 2;
			if (
				u < hurdle &&
				Math.cos(toHim - ang) > HURDLE_AHEAD &&
				tc - half > l.t0 &&
				tc + half < t1
			) {
				// Coming at his legs, low: over the top of him, on his line.
				carrier.cut(tc - half);
				carrier.act(
					"hurdle",
					tc + half,
					ang,
					beside(at, ang, (v * half) / 1000),
				);
				on();
				const hit = beside(at, toHim, out(2.6));
				const face = this.toward(hit, at);
				this.closeOn(m, hit, tc - 300, face, carrier, true);
				const land = diveBy(tc + 250);
				m.lunge("dive", land, face, reach(beside(at, face, 1.2)), true);
				m.hold(land + 700, "prone");
				m.act("getUp", land + 700 + GET_UP_MS);
				this.done.add(m.pid);
				return t + carrier.t - t1;
			}
			if (u < hurdle + spin) {
				// Got a hand on him: round on a planted foot, out of it, and on.
				// (Gathered into it over his last strides - see SPIN_INTO_FPS -
				// not into it flat out and round at half the pace in a blink.)
				carrier.brakeBy(tc, SPIN_INTO_FPS, CUT_ACCEL);
				const there = carrier.posAt(tc);
				const off = beside(
					beside(there, ang, SPIN_ON),
					ang - (side * Math.PI) / 2,
					SPIN_OVER,
				);
				carrier.cut(tc);
				carrier.act("spin", tc + SPIN_MS, ang, off);
				carrier.turn(side * Math.PI * 2);
				on();
				const hit = beside(there, toHim, out(2.2));
				const face = this.toward(hit, there);
				this.closeOn(m, hit, tc + 60, face, carrier, true);
				const land = diveBy(tc + 520);
				m.lunge("dive", land, face, reach(beside(there, face, 0.2)), true);
				m.hold(land + 600, "prone");
				m.act("getUp", land + 600 + GET_UP_MS);
				this.done.add(m.pid);
				return t + carrier.t - t1;
			}
			// (Only as far over as the pace he comes into it at carries him -
			// a step aside, off next to nothing, not thrown five feet over.)
			const into = Math.min(
				1,
				Math.max(
					JUKE_LEAST,
					(dist(carrier.posAt(tc - JUKE_INTO_MS), at) * 1000) /
						JUKE_INTO_MS /
						JUKE_FULL_FPS,
				),
			);
			const off = beside(
				beside(at, ang, JUKE_ON * into),
				ang - (side * Math.PI) / 2,
				JUKE_OVER * into,
			);
			carrier.cut(tc);
			carrier.act("juke", tc + JUKE_MS, ang, off);
			if (side < 0) {
				// (To his right: the cut the other way round.)
				carrier.last.mirrored = true;
			}
			on();
			// (Off his feet at where he was - short of it on his own side, by
			// as much as the juke is short of a full one: by him, not through
			// him.)
			const hit = beside(at, toHim, out(3.5));
			const face = this.toward(hit, at);
			this.closeOn(m, hit, tc - 100, face, carrier, true);
			const land = diveBy(tc + 380);
			m.lunge(
				"dive",
				land,
				face,
				reach(
					beside(
						beside(at, face, 0.8),
						ang + (side * Math.PI) / 2,
						JUKE_OVER * (1 - into),
					),
				),
				true,
			);
			m.hold(land + 700, "prone");
			m.act("getUp", land + 700 + GET_UP_MS);
		}
		this.done.add(m.pid);
		return t + carrier.t - t1;
	}

	// Who brings the carrier down at `pt` - `who`, or the men the sim says
	// made the tackle, or whichever of `chasers` can get there soonest - and
	// when: when he gets there, or, if they cannot be there by then, when
	// they can (his run slowed to it).
	downBy(
		carrier: Actor,
		pt: Pt,
		t: number,
		chasers: Actor[],
		who?: Actor[],
	): { by: Actor[]; t: number } {
		const credited = this.statPids("defTckSolo", "defTckAst")
			.map((pid) => this.man(pid))
			.filter(
				(m): m is Actor =>
					!!m && m.team !== carrier.team && !this.done.has(m.pid),
			);
		// (A man the sim credits from too far off to get there - across the
		// field from it - only comes after it: the hit is made by whoever
		// can.)
		const able = (who ?? credited).filter(
			(m) => this.canReach(m, pt) <= t - HIT_MS + SLACK_MS,
		);
		const by = able.length > 0 ? able : this.nearest(chasers, pt, 1);
		let lead = t - HIT_MS;
		for (const m of by) {
			lead = Math.max(lead, this.canReach(m, pt));
		}
		const late = lead - (t - HIT_MS);
		if (late > 1) {
			this.slow(carrier, late);
			return { by, t: t + late };
		}
		return { by, t };
	}

	// The soonest he could be at `pt`: leaving what he is doing whenever he
	// likes once he has seen the play, at most of his speed.
	canReach(m: Actor, pt: Pt): number {
		const fps = m.top * 0.9;
		const last = Math.max(this.seen, m.t);
		let best = Infinity;
		for (let c = this.seen; ; c += 100) {
			const at = Math.min(c, last);
			best = Math.min(best, at + (dist(m.posAt(at), pt) / fps) * 1000);
			if (at >= last) {
				return best;
			}
		}
	}

	// Held up `ms` longer getting where he is going: the last of his run
	// slowed to it (or, standing, held there) - or, a returner with the
	// field to find his way in, the time made up going side to side.
	slow(m: Actor, ms: number, weave = true, ramp = false) {
		const l = m.last;
		const far = dist(l.from, l.to);
		if (
			far > 1 &&
			moveKind(l.move) !== "act" &&
			weave &&
			this.weave &&
			ms > 300
		) {
			const v = Math.min(m.top * 0.9, far / ((l.t1 - l.t0) / 1000));
			const half = (v * (l.t1 - l.t0 + ms)) / 2000;
			// (No sharper a cut at the far end of it than WEAVE_TURN - not
			// turned back the way he came at full tilt.)
			const h = Math.min(
				24,
				far / 2 / Math.tan((Math.PI - WEAVE_TURN) / 2),
				Math.sqrt(Math.max(0, half ** 2 - (far / 2) ** 2)),
			);
			if (h > 3) {
				const ang = Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x);
				const mid = {
					x: (l.from.x + l.to.x) / 2,
					y: (l.from.y + l.to.y) / 2,
				};
				// (Away from the nearer sideline.)
				const side = Math.sign(Math.cos(ang) * (MID_Y - mid.y)) || 1;
				const end = l.to;
				const w = onField({
					x: mid.x - Math.sin(ang) * h * side,
					y: mid.y + Math.cos(ang) * h * side,
				});
				const a = dist(l.from, w);
				const all = a + dist(w, end);
				// (Round it at his pace - and, that not taking up all the time
				// there is, the rest of it his whole run slowed - not run round
				// it at half the pace he came into it at.)
				const t1 = Math.min(l.t1 + ms, l.t0 + (all / v) * 1000);
				const rest = l.t1 + ms - t1;
				const tw = l.t0 + ((t1 - l.t0) * a) / all;
				l.to = w;
				l.t1 = tw;
				// (Round the far end of it at his pace, if he can be, there no
				// later for it - see Actor.bendFor - not cut round it in a
				// stride at full tilt.)
				m.bendFor(end, t1, 0);
				m.go(end, t1, l.move);
				if (rest > 1) {
					this.slow(m, rest, false, true);
				}
				return;
			}
		}
		// (The whole of his run on to it slowed - not the last stride of it
		// stretched, nor stood waiting at the end of it: since he has had the
		// ball, whatever else is timed to him before that left be.)
		const since =
			this.ball.findLast((b) => b.kind === "held" && b.pid === m.pid)?.t0 ??
			Infinity;
		const run: Leg[] = [];
		for (let k = m.legs.length - 1; k >= 0; k--) {
			const x = m.legs[k]!;
			if (
				x.t0 < since ||
				!GAITS.has(x.move) ||
				x.arms ||
				x.turn ||
				(x.from.x === x.to.x && x.from.y === x.to.y) ||
				(run.length > 0 && x.t1 !== run[0]!.t0)
			) {
				break;
			}
			run.unshift(x);
		}
		const span = run.reduce((d, x) => d + dist(x.from, x.to), 0);
		// (Going a good deal quicker than all of it slowed would have him
		// going, as it starts - the ball taken on the run, say: slowed the
		// more the further along it, as below - not down to its pace in a
		// stride as he takes it.)
		const before = run[0] && m.legs[m.legs.indexOf(run[0]) - 1];
		const eased =
			ramp ||
			(span > 1 &&
				before !== undefined &&
				before.t1 === run[0]!.t0 &&
				GAITS.has(before.move) &&
				legPace(before) - span / ((l.t1 - run[0]!.t0 + ms) / 1000) >=
					SLOW_DROP);
		if (span > 1 && eased) {
			// (Or slowed the more the further along it he is - off quick from
			// where he had it, down by the end of it into the men who bring him
			// down: not slowed the whole way, nor all of it in the last stretch.
			// A stride at a time - see SLOW_PIECE_MS.)
			const t0 = run[0]!.t0;
			for (const x of run) {
				for (
					let t = x.t0 + SLOW_PIECE_MS;
					t < x.t1 - SLOW_PIECE_MS / 2;
					t += SLOW_PIECE_MS
				) {
					m.split(t);
				}
			}
			const pieces = m.legs.filter((x) => x.t0 >= t0);
			let along = 0;
			let sum = 0;
			const us = pieces.map((x) => {
				const d = dist(x.from, x.to);
				const u = (along + d / 2) / span;
				along += d;
				sum += (x.t1 - x.t0) * u;
				return u;
			});
			const c = ms / Math.max(sum, 1e-6);
			let t = t0;
			pieces.forEach((x, i) => {
				const was = legPace(x);
				const dt = (x.t1 - x.t0) * (1 + c * us[i]!);
				x.t0 = t;
				x.t1 = t + dt;
				t += dt;
				if (was > legPace(x) * SLOW_RELABEL) {
					x.move = gait(legPace(x));
				}
			});
			return;
		}
		if (span > 1) {
			const t0 = run[0]!.t0;
			const k = (l.t1 - t0 + ms) / (l.t1 - t0);
			const v = span / ((l.t1 - t0 + ms) / 1000);
			for (const x of run) {
				x.t0 = t0 + (x.t0 - t0) * k;
				x.t1 = t0 + (x.t1 - t0) * k;
				if (k > SLOW_RELABEL) {
					x.move = gait(v);
				}
			}
			return;
		}
		if (far > 1 && moveKind(l.move) !== "act") {
			l.t1 += ms;
			// (A shuffle or a backpedal kept one, slower: not walked
			// sideways.)
			if (GAITS.has(l.move)) {
				l.move = gait(far / ((l.t1 - l.t0) / 1000));
			}
		} else {
			m.hold(m.t + ms);
		}
	}

	goalLineX() {
		return this.input.dir === 1 ? GOAL_R : GOAL_L;
	}

	// How far downfield of the line the end line is (feet).
	endLineD() {
		const { dir, losX } = this.input;
		return dir === 1 ? FIELD_L - losX : losX;
	}

	// When a man's legs take him over the line x = gx (or `fallback`).
	crossing(m: Actor, gx: number, fallback: number): number {
		return this.inWith(m, gx, this.input.dir, fallback);
	}

	// When `m`, with the ball, gets past the line at gx going `way` - not
	// when he ran past it on his route before he had it; and had it past
	// there (caught in the end zone), as soon as he had it.
	inWith(m: Actor, gx: number, way: number, fallback: number): number {
		const since =
			this.ball.findLast((b) => b.kind === "held" && b.pid === m.pid)?.t0 ??
			-Infinity;
		if (since > -Infinity && (m.posAt(since).x - gx) * way >= 0) {
			return since;
		}
		for (const l of m.legs) {
			const a = (l.from.x - gx) * way;
			const b = (l.to.x - gx) * way;
			if (l.t1 >= since && a < 0 && b >= 0) {
				return Math.max(since, l.t0 + ((l.t1 - l.t0) * -a) / (b - a));
			}
		}
		return fallback;
	}

	// The `n` of `men` who could be at `p` soonest.
	nearest(men: Actor[], p: Pt, n: number): Actor[] {
		return men
			.filter((m) => !this.done.has(m.pid))
			.map((m) => ({ m, t: this.canReach(m, p) }))
			.sort((a, b) => a.t - b.t)
			.slice(0, n)
			.map((x) => x.m);
	}

	// Down at `pt` at t: the carrier going down with the first tackler
	// round him, the second in on it - met head on, stood up and driven back
	// onto his back, some of the time - or, `keptUp`, the two of them off
	// their feet at him as he spins out of it.
	tackle(carrier: Actor, by: Actor[], pt: Pt, t: number, keptUp = false): Pt {
		const r = this.r;
		this.downer = by[0] ?? this.downer;
		const fall = carrier.yaw;
		const lead = t - HIT_MS;
		// (Where each comes at him from. Stood in his way short of it, on
		// the line he runs: in front of him there instead - given ground
		// ahead of him and met head on, not run through and come at from
		// behind - if that is no long way back.)
		const froms = by.map((m) => {
			const tf = Math.max(this.seen, Math.min(m.t, lead - 600));
			const f = m.posAt(tf);
			const c0 = carrier.posAt(tf);
			const run = dist(c0, pt);
			if (run < 1) {
				return f;
			}
			const ux = (pt.x - c0.x) / run;
			const uy = (pt.y - c0.y) / run;
			const on = (f.x - c0.x) * ux + (f.y - c0.y) * uy;
			const off = Math.abs((f.x - c0.x) * uy - (f.y - c0.y) * ux);
			// (Further back, with the time to backpedal it at his own pace -
			// not stood in his way to be run through and come at from behind.)
			const give = run - on + HIT_FT;
			return on > 0 &&
				on < run &&
				off < IN_WAY_FT &&
				(give < GIVE_MOST_FT ||
					(give < GIVE_FAR_FT && give <= (GIVE_FPS * (lead - tf)) / 1000))
				? { x: pt.x + ux * HIT_FT, y: pt.y + uy * HIT_FT }
				: f;
		});
		const first = froms[0];
		// (Where the first comes from, round from the way he goes.)
		const toFirst = first
			? Math.atan2(first.y - pt.y, first.x - pt.x) - fall
			: 0;
		// (Down already - off his feet for the catch - and touched there.)
		const lying = carrier.last.move === "prone";
		const back =
			!keptUp &&
			!lying &&
			first !== undefined &&
			Math.cos(toFirst) > HEAD_ON &&
			r() < DRIVEN_BACK + SURE_BY.back * hitEdge(by[0]!, carrier);
		// (The side the first comes at him from: +1 his left.)
		const from1 = Math.sin(toFirst) > 0 ? 1 : -1;
		const swing =
			!keptUp &&
			!lying &&
			!back &&
			first !== undefined &&
			Math.abs(Math.sin(toFirst)) > SWING_SIDE &&
			r() < SWING;
		const upright =
			!keptUp &&
			!lying &&
			!back &&
			by.length === 1 &&
			DB.has(by[0]!.role) &&
			Math.cos(toFirst) < HEAD_ON &&
			r() < UPRIGHT + SURE_BY.upright * (by[0]!.flair.tackle ?? 0);
		const along = (p: Pt, f: number, s: number) => ({
			x: p.x + Math.cos(fall) * f - Math.sin(fall) * s,
			y: p.y + Math.sin(fall) * f + Math.cos(fall) * s,
		});
		const l = carrier.last;
		const pace = GAITS.has(l.move) ? legPace(l) : 0;
		let carried = clamp(pace * TACKLED_CARRY_S, TACKLED_LEAST, TACKLED_MOST);
		const fast = Math.min(
			TACKLED_FAST_MOST,
			(pace * pace) / (2 * TACKLED_DECEL),
		);
		if (fast > carried) {
			const a = this.of(along(pt, fast, 0)).a;
			if (a > this.bounds.left && a < this.bounds.right) {
				carried = fast;
			}
		}
		const downAt = lying
			? carrier.at
			: swing
				? along(pt, (1.4 * carried) / TACKLED_LEAST, -from1 * 2)
				: along(pt, back ? drivenBy(pace, BACK_FT) : carried, 0);
		// (The men who meet him head on down round where he would be, stood,
		// driven back: in front of him, not carried on with him.)
		const metAt = along(pt, -BACK_FT, 0);
		// (Swung round, the way he lies after it.)
		let swungBy = 0;
		if (keptUp) {
			this.runOn(carrier, 450);
		} else if (lying) {
			carrier.hold(t + 450 + DOWN_MS, "prone");
			carrier.act("getUp", t + 450 + DOWN_MS + GET_UP_MS);
			this.done.add(carrier.pid);
		} else {
			carrier.act(back ? "tackledBack" : "tackled", t + 450, fall, downAt);
			if (swing) {
				swungBy = -from1 * SWING_TURN * (0.8 + r() * 0.4);
				carrier.turn(swungBy);
			}
			carrier.hold(t + 450 + DOWN_MS, back ? "supine" : "prone");
			carrier.act(back ? "getUpBack" : "getUp", t + 450 + DOWN_MS + GET_UP_MS);
			this.done.add(carrier.pid);
		}
		const landed: Pt[] = [];
		by.forEach((m, k) => {
			// From the way he comes at him: there at the hit. (The next of
			// them round to one side of that - his own side of the way the
			// carrier goes, not across in front of him to the other.)
			const from = froms[k]!;
			const th = Math.atan2(from.y - pt.y, from.x - pt.x) - fall;
			const keeps = (s: number) => Math.sin(th + s * 1.2) * Math.sin(th) >= 0;
			const flip = k === 0 ? 0 : r() < 0.5 ? -1 : 1;
			const side = (keeps(flip) || !keeps(-flip) ? flip : -flip) * 1.2;
			const ang = Math.atan2(from.y - pt.y, from.x - pt.x) + side;
			const off = Math.max(HIT_LEAST_FT, Math.min(HIT_FT, dist(from, pt)));
			const hit = {
				x: pt.x + Math.cos(ang) * off,
				y: pt.y + Math.sin(ang) * off,
			};
			const face = this.toward(hit, pt);
			// (Run down from behind: on his heels as he gets there, not
			// waiting there for him.)
			this.closeOn(
				m,
				hit,
				Math.cos(ang - fall) < BEHIND ? t - 60 : lead,
				face,
				carrier,
			);
			// (Only from right there on him: late in from further off, down
			// into him the way the others are - not shot across to him.)
			// (On through him off the pace he gets to him at, as far as that
			// takes him - see UPRIGHT_THROUGH_MS - not stopped dead on him.)
			const thru = Math.min(
				UPRIGHT_THROUGH_MOST,
				Math.max(1.6, (legPace(m.last) * UPRIGHT_THROUGH_MS) / 2000),
			);
			const over = {
				x: hit.x + Math.cos(face) * thru,
				y: hit.y + Math.sin(face) * thru,
			};
			// (Further off than that: on through him off the pace he comes
			// in at, down to a stand over him - see noAirBlocks - if that is
			// no quicker than a man can go; if it is, down into him with the
			// rest.)
			const T = (t + 450 - m.t) / 1000;
			const D = dist(m.at, over);
			const l0 = m.last;
			const v0 =
				l0.t1 > l0.t0 && dist(l0.from, l0.to) > 0 ? paceOn(l0, m.at, over) : 0;
			const m0 = D > 0 && T > 0 ? (v0 * T) / D : 0;
			let peak = v0;
			if (m0 < 2 && T > 0) {
				for (let u = 0; u <= 1; u += 0.05) {
					peak = Math.max(
						peak,
						((6 * u * (1 - u) + m0 * (1 - u) * (1 - 3 * u)) * D) / T,
					);
				}
			}
			if (
				upright &&
				D <= LUNGE_MOST_FPS * T &&
				(dist(m.at, hit) < UPRIGHT_NEAR_FT ||
					peak <= Math.max(v0, LUNGE_MOST_FPS))
			) {
				// Through him and up, over him. (Eased down off the pace he
				// gets there at, if he is there.)
				const from = m.at;
				const v = legPace(m.last);
				const S =
					dist(from, hit) < UPRIGHT_NEAR_FT && v > 0
						? Math.min(t + 450 - m.t, ((dist(from, over) * 2) / v) * 1000)
						: 0;
				const n = S > 0 ? Math.max(1, Math.round(S / HIT_PRESS_STEP_MS)) : 0;
				const t0 = m.t;
				for (let k = 1; k <= n; k++) {
					const u = 1 - (1 - k / n) * (1 - k / n);
					m.go(
						{
							x: from.x + (over.x - from.x) * u,
							y: from.y + (over.y - from.y) * u,
						},
						t0 + (S * k) / n,
						"drive",
						face,
					);
				}
				if (n === 0 && D > 0) {
					// (Off the pace he comes in at: down from it to a stand, if
					// that is soon enough - or on from it, and then down.)
					const ts = m0 >= 2 ? (2 * D) / v0 : T;
					const at = (s: number) => {
						const u =
							m0 >= 2
								? (v0 * s - (v0 * s * s) / (2 * ts)) / D
								: (s / T) * (s / T) * (3 - (2 * s) / T) +
									m0 * (s / T) * (1 - s / T) * (1 - s / T);
						return {
							x: from.x + (over.x - from.x) * u,
							y: from.y + (over.y - from.y) * u,
						};
					};
					const k1 = Math.max(1, Math.round((ts * 1000) / HIT_PRESS_STEP_MS));
					for (let k = 1; k <= k1; k++) {
						m.go(at((ts * k) / k1), t0 + (ts * 1000 * k) / k1, "drive", face);
					}
				}
				if (m.t < t + 450) {
					m.go(over, t + 450, "drive", face);
				}
				m.hold(t + 450 + DOWN_MS + 300, "stand", face);
				this.done.add(m.pid);
				return;
			}
			// Down alongside him on the side he came in from, not on top of
			// him - the man who drove him back down across his legs; the man
			// who swung him round, down beside him as he lies, the far side
			// of him from the cameras, not along him on top of him (in from
			// his side, swung round, he is at his feet now: either side of
			// him as near).
			// (Two in from the same side not down in the one place: the next
			// over the other side of him, or further down his legs.)
			const towards = Math.sin(ang - fall) > 0 ? 1 : -1;
			const lies = fall + swungBy;
			const landing = (by: number) =>
				swing
					? {
							x:
								downAt.x -
								Math.cos(lies) * SWUNG_BACK -
								Math.sin(lies) * by * towards * farSide(lies),
							y:
								downAt.y -
								Math.sin(lies) * SWUNG_BACK +
								Math.cos(lies) * by * towards * farSide(lies),
						}
					: !back
						? along(downAt, -1.2, by)
						: k === 0
							? along(metAt, 2.6, 2)
							: along(metAt, 0.6, by);
			const by = towards * 2.1;
			const free = (p: Pt) => landed.every((q) => dist(p, q) >= LANDED_APART);
			const lands =
				[landing(by), landing(-by), along(landing(by), -LANDED_APART, 0)].find(
					free,
				) ?? along(landing(by), -LANDED_APART * 2, 0);
			landed.push(lands);
			const down = m.diveIn(lands, t + 450, face);
			m.hold(down + DOWN_MS + 150 * k, "prone");
			m.act("getUp", down + DOWN_MS + GET_UP_MS + 150 * k);
			this.done.add(m.pid);
		});
		return downAt;
	}

	// Stood up instead of brought down, some of the time: met head on at
	// `pt` at t by the first of `by`, the next of them (or of `chasers`) in
	// on it by then, and held there - the lot of them driving him back a
	// couple of feet, and the whistle, forward progress, him still up. Whether
	// he was (and the whistle, then).
	standUp(carrier: Actor, by: Actor[], chasers: Actor[], pt: Pt, t: number) {
		const r = this.r;
		const l = carrier.last;
		if (
			carrier.role === "QB" ||
			moveKind(l.move) !== "cycle" ||
			l.to.x !== pt.x ||
			l.to.y !== pt.y ||
			by.length === 0
		) {
			return false;
		}
		const fall = Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x);
		const lead = t - HIT_MS;
		const fromOf = (m: Actor, c: number) =>
			m.posAt(Math.max(this.seen, Math.min(m.t, c - 600)));
		const first = fromOf(by[0]!, lead);
		if (
			Math.cos(Math.atan2(first.y - pt.y, first.x - pt.x) - fall) <= HEAD_ON
		) {
			return false;
		}
		const more = [
			...by.slice(1),
			...this.nearest(
				chasers.filter(
					(m) => m.team !== carrier.team && !by.includes(m) && m.t < t,
				),
				pt,
				2,
			),
		]
			.map((m) => ({ m, at: this.canReach(m, pt) }))
			.filter((x) => x.at < t + STOOD_LATE)
			.slice(0, 2);
		if (
			more.length === 0 ||
			r() > STOOD + SURE_BY.stood * hitEdge(by[0]!, carrier)
		) {
			return false;
		}
		const end = t + STOOD_MS;
		const back = {
			x: -Math.cos(fall) * STOOD_BACK,
			y: -Math.sin(fall) * STOOD_BACK,
		};
		const men = [{ m: by[0]!, at: lead }, ...more];
		// (Each in from the way he comes - no two in the same place.)
		const angs: number[] = [];
		for (const [k, { m, at }] of men.entries()) {
			const from = fromOf(m, Math.max(at, lead));
			let ang = Math.atan2(from.y - pt.y, from.x - pt.x);
			for (let n = 0; n < 4; n++) {
				const near = angs
					.map((a) => Math.atan2(Math.sin(ang - a), Math.cos(ang - a)))
					.filter((d) => Math.abs(d) < STOOD_APART)[0];
				if (near === undefined) {
					break;
				}
				ang +=
					(near === 0 ? (k % 2 === 0 ? 1 : -1) : Math.sign(near)) *
						STOOD_APART -
					near;
			}
			angs.push(ang);
		}
		// (Nobody else stood where it all ends up.)
		const scrum = new Set<Actor>([carrier, ...men.map((x) => x.m)]);
		const ends = [
			{ x: pt.x + back.x, y: pt.y + back.y },
			...angs.map((a) => ({
				x: pt.x + Math.cos(a) * 2.3 + back.x,
				y: pt.y + Math.sin(a) * 2.3 + back.y,
			})),
		];
		if (
			this.men.some(
				(o) =>
					!scrum.has(o) &&
					ends.some(
						(p) => dist(o.posAt(Math.min(o.t, end)), p) < STOOD_CLEAR_FT,
					),
			)
		) {
			return false;
		}
		this.downer = by[0]!;
		// (Braced for it - see STOOD_FPS - not into them flat out.)
		carrier.gatherTo(pt, STOOD_FPS, STOOD_GATHER, STOOD_PRESS_MS, "drive");
		carrier.go({ x: pt.x + back.x, y: pt.y + back.y }, end, "drive", fall);
		carrier.hold(end + 500, "stand", fall);
		this.done.add(carrier.pid);
		men.forEach(({ m, at }, k) => {
			const ang = angs[k]!;
			const hit = {
				x: pt.x + Math.cos(ang) * 2.3,
				y: pt.y + Math.sin(ang) * 2.3,
			};
			const face = this.toward(hit, pt);
			// (In on it where it has got to by then. Coming from behind him,
			// not there before he is by it - not stood in his way for him to
			// run into from behind.)
			const there = Math.max(
				lead,
				at + 100,
				Math.cos(ang - fall) < 0 ? t + 50 : -Infinity,
			);
			const u = clamp((there - t) / STOOD_MS, 0, 1);
			// (Where he has been fixed till before he went in on it - his
			// run in at it his own to brace at the last of.)
			const pinned = m.pinned;
			this.closeOn(
				m,
				{ x: hit.x + back.x * u, y: hit.y + back.y * u },
				there,
				face,
				carrier,
			);
			const into = { x: hit.x + back.x, y: hit.y + back.y };
			if (dist(m.at, into) > STOOD_GAP_FT) {
				// (Still a way off it, late to it: in at a run, braced into it
				// as the man with the ball was - see gatherTo - not driven in
				// across the gap at a run's pace; or pulled up on it.)
				const fps = Math.max(legPace(m.last), m.top * 0.7);
				const was = m.legs.map((x) => ({ ...x }));
				const { yaw, pinned: pin } = m;
				m.run(into, fps);
				if (
					m.gatherTo(
						m.at,
						STOOD_FPS,
						STOOD_GATHER,
						STOOD_PRESS_MS,
						"drive",
						pinned,
					) === undefined
				) {
					m.legs.splice(0, m.legs.length, ...was);
					m.yaw = yaw;
					m.pinned = pin;
					m.runUp(into, fps);
				}
				if (end - m.t >= WAIT_LEAST_MS) {
					m.hold(end, "engage", face);
				}
			} else {
				if (
					m.gatherTo(
						m.at,
						STOOD_FPS,
						STOOD_GATHER,
						STOOD_PRESS_MS,
						"drive",
						pinned,
					) === undefined
				) {
					m.easeIn(STOOD_IN_FT, STOOD_IN_LEAST);
				}
				// (Not into it for a moment and straight on to the drive.)
				if (t - m.t >= WAIT_LEAST_MS) {
					m.hold(t, "engage", face);
				}
				m.go(into, end, "drive", face);
			}
			m.hold(end + 300 + r() * 300, "stand", face);
			this.done.add(m.pid);
		});
		this.whistle = end;
		return true;
	}

	// A quarterback giving himself up: feet first at `pt` - the ball spotted
	// there - sliding on a few feet, and up; the men on him there as he goes
	// down, pulled up over him, no hit.
	slideDown(qb: Actor, by: Actor[], pt: Pt, t: number) {
		const r = this.r;
		const fall = qb.yaw;
		const at = {
			x: pt.x + Math.cos(fall) * SLIDE_FT,
			y: pt.y + Math.sin(fall) * SLIDE_FT,
		};
		qb.act("qbSlide", t + SLIDE_MS, fall, at);
		qb.hold(t + SLIDE_MS + DOWN_MS, "slid");
		qb.act("sitUp", t + SLIDE_MS + DOWN_MS + GET_UP_MS * 0.8);
		this.done.add(qb.pid);
		const lead = t - HIT_MS;
		by.forEach((m, k) => {
			const from = m.posAt(Math.max(this.seen, Math.min(m.t, lead - 600)));
			const ang =
				Math.atan2(from.y - at.y, from.x - at.x) +
				(k === 0 ? 0 : (r() < 0.5 ? -1 : 1) * 0.9);
			const over = {
				x: at.x + Math.cos(ang) * PULL_UP_FT,
				y: at.y + Math.sin(ang) * PULL_UP_FT,
			};
			const face = this.toward(over, at);
			this.closeOn(m, over, t + 250 + k * 150, face, qb);
			m.hold(t + SLIDE_MS + DOWN_MS + 400, "stand", face);
			this.done.add(m.pid);
		});
	}

	// More of them in on it - the first one or two of `men` there as he
	// goes down, off their feet onto the pile at `pile`, each in from the
	// way he comes (eyes on the man with the ball till then).
	gangUp(men: Actor[], pile: Pt, t: number, by: Actor[], carrier: Actor) {
		const r = this.r;
		const near = men
			.filter((m) => !this.done.has(m.pid) && !by.includes(m) && m.t < t)
			.map((m) => ({ m, at: this.canReach(m, pile) }))
			.filter((x) => x.at < t + GANG_LATE)
			.sort((a, b) => a.at - b.at)
			.slice(0, GANG.length);
		near.forEach(({ m }, k) => {
			if (r() > GANG[k]!) {
				return;
			}
			const from = m.posAt(Math.max(this.seen, Math.min(m.t, t - 600)));
			const ang =
				Math.atan2(from.y - pile.y, from.x - pile.x) + (r() - 0.5) * 0.6;
			const off = Math.max(HIT_FT, Math.min(4.5, dist(from, pile)));
			const hit = {
				x: pile.x + Math.cos(ang) * off,
				y: pile.y + Math.sin(ang) * off,
			};
			const go = t + 150 + k * 200 + r() * 150;
			if (this.canReach(m, hit) > go) {
				return;
			}
			// (Down off the men down there already - the one who brought him
			// down, say: round the pile from them, a little way, or not in on
			// it - not down on top of one, the two of them up out of each
			// other.)
			const lies = (a: number) => ({
				x: pile.x + Math.cos(a) * 1.6,
				y: pile.y + Math.sin(a) * 1.6,
			});
			const down = this.men
				.filter(
					(o) => o !== m && o !== carrier && DOWN.has(legAt(o, go + 450).move),
				)
				.map((o) => o.posAt(go + 450));
			const round = GANG_ROUND.find((d) =>
				down.every((p) => dist(p, lies(ang + d)) >= LANDED_APART),
			);
			if (round === undefined) {
				return;
			}
			const face = this.toward(hit, pile);
			// (Not one that only gets there well after the rest - off the
			// way he went, say, and only round to it after: not in on it.)
			const was = m.legs.map((l) => ({ ...l }));
			const { yaw, pinned } = m;
			this.closeOn(m, hit, go, face, carrier);
			const downT = m.diveIn(lies(ang + round), go + 450, face);
			if (downT - go > GANG_DIVE_MOST) {
				m.legs.splice(0, m.legs.length, ...was);
				m.yaw = yaw;
				m.pinned = pinned;
				return;
			}
			m.hold(downT + DOWN_MS, "prone");
			m.act("getUp", downT + DOWN_MS + GET_UP_MS);
			this.done.add(m.pid);
		});
	}

	// In from right there, up the middle: over the top - off his feet short
	// of the line, laid out over the pile on it as the ball goes over; up
	// off the top of it into his moment, the pile under him.
	overTop(i: number, carrier: Actor, chasers: Actor[]) {
		const r = this.r;
		const { dir } = this.input;
		const goal = this.goalLineX();
		const had =
			this.ball.findLast((b) => b.kind === "held" && b.pid === carrier.pid)
				?.t0 ?? this.input.snap;
		const go = Math.max(
			had + TOP_SET,
			this.crossing(carrier, goal - dir * TOP_FROM, carrier.t),
		);
		carrier.cut(go);
		carrier.hold(go);
		const from = carrier.at;
		const land = {
			x: goal + dir * (TOP_PAST + r() * TOP_PAST_MORE),
			y: from.y + (r() - 0.5) * 2,
		};
		const ms = clamp((dist(from, land) / TOP_FPS) * 1000, TOP_MS[0], TOP_MS[1]);
		const face = dir > 0 ? 0 : Math.PI;
		carrier.act("pileDive", go + ms, face, land);
		const crossT =
			go + ms * clamp((goal - from.x) / (land.x - from.x || dir), 0, 1);
		this.at.set(i, crossT);
		this.whistle = crossT;
		carrier.hold(go + ms + TOP_STAY + r() * 300, "pileTop", face);
		carrier.act("offTop", carrier.t + GET_UP_MS, face);
		this.done.add(carrier.pid);
		this.pullUp(chasers, carrier, crossT);
		this.pileUp(carrier, { x: goal, y: land.y }, crossT, this.heap);
	}

	// Brought down in the thick of it, short and up the middle: a pile - the
	// big men in the scrum round him falling in on top of him and the men
	// who put him down, one after another, onto their knees over the heap -
	// and off it again from the top, the last in the first up, the men under
	// it then, him the last of them.
	pileUp(carrier: Actor, at: Pt, t: number, most: number) {
		const r = this.r;
		const near = this.men
			.filter(
				(m) => m !== carrier && !this.done.has(m.pid) && PILERS.has(m.role),
			)
			.map((m) => ({ m, from: m.posAt(t) }))
			.filter((x) => dist(x.from, at) < PILE_NEAR)
			.sort((a, b) => dist(a.from, at) - dist(b.from, at))
			.slice(0, most);
		const on: { m: Actor; at: Pt; from: Pt; way: number; go: number }[] = [];
		// (Each in where he comes from, or the nearest way round from it
		// with room for him - PILE_ROOM off the rest, and off him under it,
		// laid out over the top of it past the middle of it, say - not on
		// his way there through another of them, or across another's way in.
		// None: not in on it.)
		const lies = carrier.posAt(t + PILE_FIRST + PILE_FALL_MS);
		const roomy = (from: Pt, p: Pt) =>
			dist(lies, p) >= PILE_ROOM &&
			wide(lies, from, p) &&
			on.every(
				(x) =>
					dist(x.at, p) >= PILE_ROOM &&
					wide(x.at, from, p) &&
					wide(p, x.from, x.at),
			);
		near.forEach(({ m }, k) => {
			if (r() > PILE_IN[k]!) {
				return;
			}
			// (In from where he is when he goes in - not where he was as it
			// went down, and through it from past it to that side of it.)
			const from = m.posAt(t + PILE_FIRST + on.length * PILE_GO);
			const was = Math.atan2(from.y - at.y, from.x - at.x) + (r() - 0.5) * 0.5;
			const far = PILE_OFF + r() * PILE_OFF_MORE;
			const spot = (way: number) => ({
				x: at.x + Math.cos(way) * far,
				y: at.y + Math.sin(way) * far,
			});
			const side = k % 2 === 0 ? 1 : -1;
			const way = PILE_TURNS.map((d) => was + d * side).find((w) =>
				roomy(from, spot(w)),
			);
			const late = r() * 100;
			if (way === undefined) {
				return;
			}
			on.push({
				m,
				at: spot(way),
				from,
				way,
				go: t + PILE_FIRST + on.length * PILE_GO + late,
			});
		});
		if (on.length === 0) {
			return;
		}
		const n = on.length;
		let tops = 0;
		const overs: Pt[] = [];
		on.forEach((x, k) => {
			// (One of the last in, now and then, over the top of it - laid out
			// across it.)
			// (Not onto the man over it already.)
			// (Straight in from where he is - not over one of the others in on
			// it on the way as he goes in himself: after him, if not long.)
			let top = k >= 2 && tops < PILE_TOPS && r() < PILE_OVER;
			let dive = x.go;
			let out: Pt | undefined;
			if (top) {
				const far = PILE_TOP_OFF + r() * 0.5;
				const from = x.m.posAt(x.go);
				const way =
					dist(from, at) > 0.1
						? Math.atan2(from.y - at.y, from.x - at.x)
						: x.way;
				const over = {
					x: at.x + Math.cos(way) * far,
					y: at.y + Math.sin(way) * far,
				};
				dive = Math.max(
					x.go,
					...on
						.filter((o) => o !== x && !wide(o.at, from, over))
						.map((o) =>
							dist(o.from, o.at) > PILE_REACH ? Infinity : o.go + PILE_FALL_MS,
						),
				);
				top =
					(dist(lies, at) < PILE_ROOM || dist(lies, over) >= PILE_ROOM) &&
					overs.every((p) => dist(p, over) >= PILE_ROOM) &&
					dive - x.go <= PILE_TOP_WAIT;
				if (top) {
					tops++;
					overs.push(over);
					x.at = over;
					// (Off it clear of the rest in on it - the way he came, or
					// as near it as that is.)
					for (const turn of OFF_TOP_TURNS) {
						const p = {
							x: at.x + Math.cos(way + turn) * (far + OFF_TOP_FT),
							y: at.y + Math.sin(way + turn) * (far + OFF_TOP_FT),
						};
						if (
							on.every(
								(o) =>
									o === x ||
									(dist(o.at, p) >= PILE_ROOM && wide(o.at, over, p)),
							)
						) {
							out = p;
							break;
						}
					}
				}
			}
			const face = this.toward(x.at, at);
			x.m.cut(x.go);
			x.m.hold(top ? dive : x.go);
			// (From further off, in at a run first.)
			const far = dist(x.m.at, x.at);
			if (far > PILE_REACH) {
				const u = (far - PILE_REACH) / far;
				x.m.run(
					{
						x: x.m.at.x + (x.at.x - x.m.at.x) * u,
						y: x.m.at.y + (x.at.y - x.m.at.y) * u,
					},
					x.m.top * 0.8,
				);
			}
			// (In past his place on it already - right on the man down, say:
			// a step or two back out to it first, facing in, and in from
			// there - not driven back onto it as he pitches in.)
			const fx = Math.cos(face);
			const fy = Math.sin(face);
			if (
				(x.at.x - x.m.at.x) * fx + (x.at.y - x.m.at.y) * fy <
				-PILE_BACK_LEAST
			) {
				const set = {
					x: x.at.x - fx * PILE_BACK_IN,
					y: x.at.y - fy * PILE_BACK_IN,
				};
				x.m.go(
					set,
					x.m.t +
						Math.max(PILE_BACK_MS, (dist(x.m.at, set) / PILE_BACK_FPS) * 1000),
					undefined,
					face,
				);
			}
			x.m.act(top ? "pileDive" : "pileIn", x.m.t + PILE_FALL_MS, face, x.at);
			const off = Math.max(
				x.m.t + 400,
				t + PILE_HOLD + (n - 1 - k) * PILE_PEEL + r() * 120,
			);
			x.m.hold(off, top ? "pileTop" : "overPile", face);
			x.m.act(top ? "offTop" : "offPile", off + OFF_PILE_MS, face, out);
			this.done.add(x.m.pid);
		});
		// The men stood round it: in on it, shoving, a moment - and back off.
		const ring = this.men
			.filter((m) => m !== carrier && !this.done.has(m.pid))
			.map((m) => ({ m, from: m.posAt(t + PUSH_SEEN) }))
			.filter((x) => dist(x.from, at) < PUSH_NEAR)
			.slice(0, PUSH_MOST);
		for (const { m, from } of ring) {
			const go = t + PUSH_GO + r() * 250;
			const way = Math.atan2(from.y - at.y, from.x - at.x) + (r() - 0.5) * 0.4;
			const far = PUSH_AT + r() * PUSH_AT_MORE;
			const to = {
				x: at.x + Math.cos(way) * far,
				y: at.y + Math.sin(way) * far,
			};
			const face = this.toward(to, at);
			m.cut(go);
			m.hold(go);
			m.chase(to, go + PUSH_IN_MS, m.top * 0.6, "drive", face);
			const leave = t + PILE_HOLD - r() * PUSH_LEAVE;
			m.go(
				{
					x: m.at.x + (at.x - m.at.x) * 0.15,
					y: m.at.y + (at.y - m.at.y) * 0.15,
				},
				Math.max(m.t + 300, leave),
				"drive",
				face,
			);
			m.go(
				{
					x: m.at.x + Math.cos(way) * PUSH_BACK,
					y: m.at.y + Math.sin(way) * PUSH_BACK,
				},
				m.t + 700,
				"walk",
				face,
			);
			m.hold(m.t + 400, "stand", face);
			this.done.add(m.pid);
		}
		// (Under it: the men down on him there, then him.)
		const up = t + PILE_HOLD + (n - 1) * PILE_PEEL + PILE_UNDER;
		const seen = t + DOWN_MS;
		const under = this.men.filter(
			(m) =>
				m !== carrier &&
				(legAt(m, seen).move === "prone" || legAt(m, seen).move === "supine") &&
				dist(m.posAt(seen), at) < PILE_NEAR,
		);
		under.forEach((m, k) => stayDown(m, up + k * 200 + r() * 100));
		stayDown(carrier, up + under.length * 200 + 250);
		// (The picture on it till he is up.)
		this.until = Math.max(this.until, carrier.t);
	}

	// When `m` leaves what he is doing to get to `hit` by `lead` (see
	// closeOn) - and the pace he has to get up to for it.
	closeFrom(m: Actor, hit: Pt, lead: number): { c: number; need: number } {
		// (Where and when he is off his new way, leaving what he was doing
		// at c: planted on past it first, going at all quick and well round
		// from it - see plantFor.)
		// (And the pace he is off it at.)
		const offAt = (c: number): { at: Pt; t: number; v: number } => {
			const at = m.posAt(c);
			const l = m.legs.findLast((x) => x.t0 < c && x.t1 > x.t0);
			const v = l ? legPace(l) : 0;
			const far = l ? dist(l.from, l.to) : 0;
			if (
				!l ||
				!RAMPS.has(l.move) ||
				v < STOP_LEAST ||
				l.arms ||
				l.turn ||
				far <= 0
			) {
				return { at, t: c, v: l && RAMPS.has(l.move) ? paceOn(l, at, hit) : 0 };
			}
			const cos = (l.to.x - l.from.x) / far;
			const sin = (l.to.y - l.from.y) / far;
			const turn = angleAt(
				{ x: at.x - cos, y: at.y - sin },
				at,
				dist(at, hit) > 0 ? hit : { x: at.x + cos, y: at.y + sin },
			);
			if (!(turn > CUT_LEAST)) {
				return { at, t: c, v: paceOn(l, at, hit) };
			}
			const { ve } = plantedFor(v, at, cos, sin, hit);
			const T = (v - ve) / CUT_ACCEL;
			const d = ((v + ve) / 2) * T;
			const off = { x: at.x + cos * d, y: at.y + sin * d };
			const round = angleAt({ x: off.x - cos, y: off.y - sin }, off, hit);
			return { at: off, t: c + T * 1000, v: (ve * (1 + Math.cos(round))) / 2 };
		};
		// (The pace he has to get up to - see RAMP_ACCEL - to be there by
		// `lead`, off what he is doing at c.)
		const need = (c: number) => {
			const o = offAt(c);
			const T = Math.max(0.001, (lead - o.t) / 1000);
			const D = dist(o.at, hit);
			if (o.v * T >= D) {
				return D / T;
			}
			const room = T * T - (2 * (D - o.v * T)) / RAMP_ACCEL;
			return room >= 0
				? o.v + RAMP_ACCEL * (T - Math.sqrt(room))
				: o.v + (2 * (D - o.v * T)) / T;
		};
		let c = Math.max(this.seen, Math.min(m.t, lead - 100));
		let best = c;
		while (c > this.seen && need(c) > m.top * CLOSE) {
			c = Math.max(this.seen, c - 50);
			if (need(c) < need(best)) {
				best = c;
			}
		}
		if (need(c) > m.top * CLOSE) {
			c = best;
		}
		return { c, need: need(c) };
	}

	// A man to `hit` by `lead`, facing `face` there: leaving what he was
	// doing as late as he can and still get there at most of his speed (or
	// whenever gets him there easiest) - with time to spare, up to a few
	// steps off to break down, and in from there.
	closeOn(
		m: Actor,
		hit: Pt,
		lead: number,
		face: number,
		watch?: Actor,
		overrun = false,
	) {
		let { c } = this.closeFrom(m, hit, lead);
		// (Not a step or two into something new first - a backpedal just
		// begun: off from before it. Or, begun before he could know to,
		// still in the stance he was in until then.)
		const k = m.legs.findLastIndex((x) => x.t0 < c && x.t1 > x.t0);
		const began = m.legs[k];
		if (
			began &&
			k > 0 &&
			c - began.t0 < CLOSE_SLIVER_MS &&
			m.legs[k - 1]!.move !== began.move
		) {
			const was = m.legs[k - 1]!;
			if (began.t0 >= this.seen) {
				c = began.t0;
			} else if (sits(was.move)) {
				m.cut(began.t0);
			} else if (
				sits(began.move) &&
				began.from.x === began.to.x &&
				began.from.y === began.to.y &&
				was.t1 === began.t0 &&
				was.t1 > was.t0 &&
				(was.from.x !== was.to.x || was.from.y !== was.to.y)
			) {
				// (Or pulled up into a set a moment before he knows: on as he
				// was going till then instead - not a blink of it.)
				const u = (c - was.t1) / (was.t1 - was.t0);
				m.cut(began.t0);
				m.go(
					{
						x: was.to.x + (was.to.x - was.from.x) * u,
						y: was.to.y + (was.to.y - was.from.y) * u,
					},
					c,
					was.move,
					was.yaw,
				);
			}
		}
		m.cut(c);
		// (Not off to it till he knows to be: on the way he was going till
		// then, at the pace he was - see carryOn - not pulled up dead short
		// of it in a stride.)
		m.carryOn(c);
		// (Made to miss, and on his way at it too quick to pull up short of
		// it: on past it, pulled up as hard as a man can - see STOP_HARD -
		// and back at him from there - not stopped dead on the spot in a
		// stride. Off to the side of it, he bends round to it instead - see
		// turnFor.)
		if (overrun) {
			const l = m.last;
			const v = RAMPS.has(l.move) && !l.arms && !l.turn ? legPace(l) : 0;
			const stopFt = (v * v) / (2 * STOP_ACCEL * STOP_HARD);
			if (
				v >= STOP_LEAST &&
				stopFt > dist(m.at, hit) - RUN_IN_NEAR_FT &&
				angleAt(l.from, l.to, hit) <= CUT_LEAST
			) {
				m.easeOff(stopFt);
			}
		}
		// (Deep, set, and a long while before it gets to him: a read step or
		// two back first, his depth kept till he knows - not stood stock
		// still where he lined up till it comes.)
		const at = this.of(m.at);
		if (
			at.d >= READ_DEEP_FT &&
			sits(m.last.move) &&
			lead -
				m.t -
				rampedMs(
					dist(m.at, hit) + READ_BACK_FT,
					m.top * FLAT_OUT,
					0,
					GET_OFF_ACCEL,
				) >=
				READ_BACK_LEAST_MS
		) {
			m.go(
				this.w({ d: at.d + READ_BACK_FT, a: at.a }),
				m.t + READ_BACK_MS,
				"back",
				this.behind,
			);
		}
		// (Round to it at his pace, if the turn is one he can bend round -
		// not planted for it and turned in a stride: see Actor.turnFor.)
		m.turnFor(
			hit,
			Math.min(m.top, dist(m.at, hit) / Math.max(0.001, (lead - m.t) / 1000)),
		);
		// (Still slowing for a turn when he knows - off his own plant, say -
		// too slow by then to plant again, with it well round behind him:
		// the last of his pace eased off first, not turned round on it from
		// a jog in a stride.)
		const slowing = m.last;
		const slowPace = legPace(slowing);
		if (
			RAMPS.has(slowing.move) &&
			!slowing.arms &&
			!slowing.turn &&
			slowPace < STOP_LEAST &&
			angleAt(slowing.from, slowing.to, hit) > CUT_BACK
		) {
			m.easeOff((slowPace * slowPace) / (2 * CUT_ACCEL));
		}
		// (Right on it already, going - at a run, say, or full tilt - and a
		// long while before it comes: pulled up hard on the way he was
		// going, and set from there - not stopped dead on it and stood
		// there.)
		const pace = legPace(m.last);
		if (
			dist(m.at, hit) <= 8 &&
			RAMPS.has(m.last.move) &&
			pace >= STOP_LEAST &&
			lead - m.t >= BREAK_PULL_LEAST_MS
		) {
			m.easeOff((pace * pace) / (2 * STOP_ACCEL * STOP_HARD));
		}
		const far = dist(m.at, hit);
		const secs = (lead - m.t) / 1000;
		// (When he goes in on it from this far off, set: as long before it as
		// getting up to speed into it takes - not off at it all at once.)
		const fireAt = (d: number) =>
			lead -
			Math.max(BREAK_FIRE_MS, rampedMs(d, m.top * FLAT_OUT, 0, GET_OFF_ACCEL));
		const fire = fireAt(5);
		const fireNear = fireAt(far > BREAK_NEAR_FT + 1 ? BREAK_NEAR_FT : far);
		// (Up to it no quicker than gets him there with BREAK_DOWN_MS to
		// settle into it - chopping his feet down into it, not stopped
		// dead: if there is the time for that - nor walked up there.)
		const room = (fire - BREAK_DOWN_MS - m.t) / 1000;
		const v = clamp(
			(far - 5) / Math.max(0.1, room),
			JOG_LEAST_FPS,
			m.top * 0.7,
		);
		const ease = Math.min(
			(far - 5) * 0.5,
			(v * v) / (2 * STOP_ACCEL * STOP_HARD),
		);
		const off = {
			x: hit.x + ((m.at.x - hit.x) / far) * 5,
			y: hit.y + ((m.at.y - hit.y) / far) * 5,
		};
		const nearMs = (Math.max(0, far - BREAK_NEAR_FT) / BREAK_NEAR_FPS) * 1000;
		// (Broken down, square to the man he is to hit, where he is by then -
		// not to where he will have him.)
		const look = () => {
			const him = watch?.posAt((m.t + fire) / 2);
			return him && dist(him, m.at) > 2 ? this.toward(m.at, him) : face;
		};
		// (Set there a while: turned with him as he comes - square to where
		// he is, a turn at a time - not set one way all of it; and, a way
		// off where he meets him, round it a shuffle at a time as he comes
		// round to it, to keep in front of him as he was - see MIRROR_MOST -
		// not stood still as he cuts.)
		const setTill = (t1: number) => {
			// (Only a blink of it to be set for: in at him from the first.)
			if (t1 - m.t < FLICK_MS) {
				return;
			}
			let yaw = look();
			if (watch && t1 - m.t >= 2 * KEEP_STEP_MS) {
				const ahead = watch.posAt(m.t + KEEP_STEP_MS / 2);
				if (dist(ahead, m.at) > 2) {
					yaw = this.toward(m.at, ahead);
				}
				const r0 = dist(m.at, hit);
				const a0 = this.toward(hit, m.at);
				const him0 = watch.posAt(m.t);
				const rel =
					r0 >= MIRROR_LEAST_FT && dist(him0, hit) > MIRROR_NEAR_FT
						? a0 - this.toward(him0, hit)
						: undefined;
				let a = a0;
				while (t1 - m.t >= 1.5 * KEEP_STEP_MS) {
					const him = watch.posAt(m.t + KEEP_STEP_MS / 2);
					const off =
						rel === undefined ||
						t1 - m.t < 2.5 * KEEP_STEP_MS ||
						dist(him, hit) <= MIRROR_NEAR_FT
							? 0
							: angleOff(
									a0 +
										clamp(
											angleOff(this.toward(him, hit) + rel - a0),
											-MIRROR_MOST,
											MIRROR_MOST,
										) -
										a,
								);
					if (Math.abs(off) * r0 >= MIRROR_STEP_LEAST_FT) {
						a += clamp(off, -MIRROR_STEP_FT / r0, MIRROR_STEP_FT / r0);
						const to = {
							x: hit.x + Math.cos(a) * r0,
							y: hit.y + Math.sin(a) * r0,
						};
						if (dist(him, to) > 2) {
							yaw = this.toward(to, him);
						}
						m.go(to, m.t + KEEP_STEP_MS, "slide", yaw);
						continue;
					}
					m.hold(m.t + KEEP_STEP_MS, "backerReady", yaw);
					const next = watch.posAt(m.t + KEEP_STEP_MS / 2);
					const to = this.toward(m.at, next);
					if (
						dist(next, m.at) > 2 &&
						Math.abs(angleOff(to - yaw)) > KEEP_TURN
					) {
						yaw = to;
					}
				}
			}
			m.hold(t1, "backerReady", yaw);
		};
		const downBy =
			m.t +
			rampedMs(far - 5 - ease, v, paceOn(m.last, m.at, off)) +
			((2 * ease) / v) * 1000;
		// (In at him off it, square to him: backpedaled into the spot if it
		// is behind him, a slide if off to his side - not run there backward
		// or crabbed across.)
		// (Quicker at the last of it than a man goes in push steps or a
		// backpedal, though - SHUFFLE_MOST, BACK_MOST - turned and run at
		// him: not crabbed in sideways flat out.)
		const inTo = () => {
			const c = Math.cos(this.toward(m.at, hit) - face);
			const move = c < GIVE_COS ? "back" : c < SIDE_COS ? "slide" : "run";
			const D = dist(m.at, hit);
			const T = Math.max(0.001, (lead - m.t) / 1000);
			const room = T * T - (2 * D) / GET_OFF_ACCEL;
			const top =
				room >= 0 ? GET_OFF_ACCEL * (T - Math.sqrt(room)) : (2 * D) / T;
			const quick =
				move !== "run" &&
				top > (move === "back" ? BACK_MOST : SHUFFLE_MOST) * HOLD_ON;
			m.getOff(
				hit,
				lead,
				0,
				GET_OFF_ACCEL,
				quick ? gait(top) : move,
				0,
				quick ? undefined : face,
			);
		};
		// (Giving ground in front of him: backpedaling, square to him.)
		const gives =
			far > 0 &&
			Math.cos(Math.atan2(hit.y - m.at.y, hit.x - m.at.x) - face) < GIVE_COS;
		// (With the time for it: back at a pace he can hold - no quicker than
		// he has to be - pulled up a step short of where he meets him and
		// set, and up into him off that: not backpedaled into the hit, nor
		// turned and run there, his back to him.)
		const setAt = {
			x: hit.x - Math.cos(face) * GIVE_STEP_FT,
			y: hit.y - Math.sin(face) * GIVE_STEP_FT,
		};
		const upMs = rampedMs(GIVE_STEP_FT, m.top * FLAT_OUT, 0, GET_OFF_ACCEL);
		const backFar = dist(m.at, setAt);
		const backBy = lead - upMs - GIVE_SET_MS;
		const down = STOP_ACCEL * STOP_HARD;
		// (Up and down again: the pace v that takes backFar / v + v * per
		// seconds - backBy, if it is quicker than GIVE_FPS.)
		const per = 1 / (2 * RELEASE_ACCEL) + 1 / (2 * down);
		const backT = (backBy - m.t) / 1000;
		const disc = backT * backT - 4 * per * backFar;
		const backV =
			stepMs(backFar, GIVE_FPS, RELEASE_ACCEL, down) <= backT * 1000
				? GIVE_FPS
				: disc >= 0
					? (backT - Math.sqrt(disc)) / (2 * per)
					: Infinity;
		// (Not backpedaled through the men in blocks behind him, though -
		// see blocksIn: round them, if he can be, instead.)
		const backBlocked =
			gives &&
			backFar > GIVE_STEP_FT &&
			backV > 0 &&
			Number.isFinite(backV) &&
			this.blocksIn(
				m,
				watch ?? m,
				m.at,
				[setAt],
				backBy -
					stepMs(
						backFar,
						Math.min(backV, Math.sqrt(backFar / per)),
						RELEASE_ACCEL,
						down,
					),
				backBy,
			).worst > CLIMB_INTO_FT;
		const back =
			gives &&
			!backBlocked &&
			backFar > GIVE_STEP_FT &&
			backV > 0 &&
			backV <= BACK_MOST &&
			legPace(m.last) < STOP_LEAST;
		// (On a way to five feet off him, eased into it, and set there till
		// he goes in - if that has him there in time for it: round on his
		// way there first, say, quicker about it - not there only as he has
		// to be in on him already.)
		const runIn = (fps: number, whole = true): boolean => {
			const was = m.legs.map((l) => ({ ...l }));
			const { yaw, pinned } = m;
			// (Coming in near that quick already: eased off over as much of
			// the way there as that takes - not on at it and stopped in a
			// stride or two at the last.)
			const v0 = GAITS.has(m.last.move) ? legPace(m.last) : 0;
			// (A good deal quicker than that, though, on his way at him: on at
			// the pace he has, eased off it over the last of the way - no
			// harder than STOP_HARD - and set a step nearer him for it, if
			// that is where it leaves him (RUN_IN_NEAR_FT at the nearest): not
			// braked down to the pace of the run in a stride first.)
			const hard = STOP_ACCEL * STOP_HARD;
			const stopFt = (v0 * v0) / (2 * hard);
			const l = m.last;
			if (
				v0 - fps >= SLOW_DROP &&
				!l.arms &&
				!l.turn &&
				stopFt <= far - RUN_IN_NEAR_FT &&
				angleAt(l.from, l.to, hit) <= RUN_IN_QUICK_TURN
			) {
				const runFt = far - 5 - stopFt;
				if (runFt > 0.5) {
					const u = runFt / (far - 5);
					m.run(
						{
							x: m.at.x + (off.x - m.at.x) * u,
							y: m.at.y + (off.y - m.at.y) * u,
						},
						v0,
					);
				}
				m.easeOff(stopFt);
			} else {
				const easeAt = Math.min(
					(far - 5) * (whole && v0 >= fps * RUN_IN_ON ? 1 : 0.5),
					(fps * fps) / (2 * hard),
				);
				const u = (far - 5 - easeAt) / (far - 5);
				m.run(
					{
						x: m.at.x + (off.x - m.at.x) * u,
						y: m.at.y + (off.y - m.at.y) * u,
					},
					fps,
				);
				m.easeOff(easeAt);
			}
			if (m.t <= fire) {
				return true;
			}
			m.legs.splice(0, m.legs.length, ...was);
			m.yaw = yaw;
			m.pinned = pinned;
			return false;
		};
		const canRunIn =
			!back &&
			far > 8 &&
			far < m.top * 0.5 * secs &&
			downBy <= fire - BREAK_SET_MS;
		let ranIn =
			canRunIn && (runIn(v) || (v < m.top * 0.7 && runIn(m.top * 0.7)));
		// (No time to ease off over the whole way and set: slowed along in
		// to him instead, if he can be - or, not that either, eased off
		// over the last half of it as he can.)
		const slowedIn = canRunIn && !ranIn && !gives && m.slowAlong(hit, lead);
		if (canRunIn && !ranIn && !slowedIn) {
			ranIn = runIn(v, false) || (v < m.top * 0.7 && runIn(m.top * 0.7, false));
		}
		if (slowedIn) {
			// (See slowAlong.)
		} else if (back) {
			const bv = Math.min(backV, Math.sqrt(backFar / per));
			const pull = Math.min(backFar / 2, (bv * bv) / (2 * down));
			m.hold(backBy - stepMs(backFar, bv, RELEASE_ACCEL, down));
			const u = (backFar - pull) / backFar;
			m.getOff(
				{
					x: m.at.x + (setAt.x - m.at.x) * u,
					y: m.at.y + (setAt.y - m.at.y) * u,
				},
				backBy - ((2 * pull) / bv) * 1000,
				0,
				RELEASE_ACCEL,
				"back",
				0,
				face,
			);
			m.easeOff(pull);
			setTill(lead - upMs);
			inTo();
		} else if (ranIn) {
			setTill(fire);
			inTo();
		} else if (
			far <= 8 &&
			legPace(m.last) < BREAK_NEAR_MOST &&
			fireNear - m.t >= nearMs + BREAK_NEAR_MS
		) {
			// (Right there already, and with time: squared up to him a step
			// or so off, his feet chopping, till he comes - not walked in
			// on him. Eyes on him on the way there, not on the spot.)
			if (far > BREAK_NEAR_FT + 1) {
				const at = {
					x: hit.x + ((m.at.x - hit.x) / far) * BREAK_NEAR_FT,
					y: hit.y + ((m.at.y - hit.y) / far) * BREAK_NEAR_FT,
				};
				m.go(at, m.t + nearMs, "slide", look());
			}
			setTill(fireNear);
			inTo();
		} else {
			// (A step or two off him, time over to walk it: set where he is,
			// and in at him off it - not walked in on him.)
			const fireMs = rampedMs(far, m.top * FLAT_OUT, 0, GET_OFF_ACCEL);
			const set = lead - fireMs - m.t >= BREAK_SET_MS;
			if (
				!gives &&
				far >= 1 &&
				far / Math.max(0.001, secs) < WALK_MOST_FPS &&
				lead - m.t >= fireMs &&
				(sits(m.last.move) || (set && legPace(m.last) < STOP_LEAST))
			) {
				if (set) {
					setTill(lead - fireMs);
				}
				inTo();
			} else if (
				gives
					? !(backBlocked && this.pressRound(m, hit, lead, watch))
					: !m.slowAlong(hit, lead, STRETCH_SLACK) &&
						!this.pressRound(m, hit, lead, watch)
			) {
				// (Backpedaling, if he gives ground and can go that quick so.)
				m.pressUp(
					hit,
					lead,
					m.top * FLAT_OUT,
					undefined,
					gives ? face : undefined,
				);
			}
		}
	}

	// In at `hit` by `lead` all out (see pressUp), round the men in blocks
	// in his way, if there is a way round them that gets him there in time
	// (see roundBlocks) - through the gap, not on through them. (Whether
	// he was.)
	private pressRound(m: Actor, hit: Pt, lead: number, watch?: Actor): boolean {
		const round = this.roundBlocks(m, hit, lead, watch ?? m, FLAT_OUT);
		if (!round) {
			return false;
		}
		const t0 = m.t;
		const v0 = paceOn(m.last, m.at, round[0]!);
		let all = 0;
		let was = m.at;
		const ms = round.map((p) => {
			all += dist(was, p);
			was = p;
			return rampedMs(all, m.top * FLAT_OUT, v0);
		});
		const most = ms.at(-1)!;
		if (!(most > 0) || t0 + most > lead) {
			return false;
		}
		const scale = (lead - t0) / most;
		round.forEach((p, i) => m.go(p, t0 + ms[i]! * scale));
		m.pin(lead);
		return true;
	}

	// The rest of the defense after the ball - whoever is not busy with
	// something of his own through it: each to where the play ends up,
	// near it, as fast as he can get there - those too far off never do,
	// and pull up at the whistle. After a man with the ball, on an angle
	// to him as he goes, closing in on him from his own side of him.
	pursue(men: Actor[], pt: Pt, t: number, tacklers: Actor[], carrier?: Actor) {
		const r = this.r;
		for (const m of men) {
			if (this.done.has(m.pid) || tacklers.includes(m) || m.t >= t) {
				continue;
			}
			m.carryOn(Math.max(m.t, this.seen));
			m.plantFor(carrier ? carrier.posAt(m.t + PURSUIT_LEAD) : pt);
			const turn = (r() - 0.5) * 0.8;
			const ring = 5 + r() * 9;
			const arrive = t + 200 + r() * 500;
			const lane = (r() < 0.5 ? -1 : 1) * PURSUIT_LANE * (0.2 + r() * 0.8);
			const end = () => {
				const ang = Math.atan2(m.at.y - pt.y, m.at.x - pt.x) + turn;
				return {
					x: pt.x + Math.cos(ang) * ring,
					y: pt.y + Math.sin(ang) * ring,
				};
			};
			// (No sooner than he needs to be, or near enough: not stood off
			// him, going along with him - in his lane, not all in single file
			// on his heels.)
			while (carrier && m.t + PURSUIT_STEP < arrive - 300) {
				const c = m.t + PURSUIT_STEP;
				const him = carrier.posAt(c + PURSUIT_LEAD);
				const was = carrier.posAt(c + PURSUIT_LEAD - 100);
				const goes = dist(him, was);
				const off =
					ring + (PURSUIT_RING - ring) * clamp((arrive - c) / 1500, 0, 1);
				const a = Math.atan2(m.at.y - him.y, m.at.x - him.x);
				const side = goes > 0.2 ? (lane * off) / PURSUIT_RING / goes : 0;
				const need = (dist(m.at, end()) / (arrive - m.t)) * 1000;
				// (In nearer him than that already: on along with him, closing
				// in a little - not back out to it.)
				const near = dist(m.at, him);
				if (near < off) {
					const now = carrier.posAt(m.t + PURSUIT_LEAD);
					const by = Math.min(PURSUIT_IN, Math.max(0, near - PURSUIT_NEAREST));
					// (Ahead of him, on his way or near it - see
					// PURSUIT_SIDE_FT: out to the side of it he is on, not
					// run down from behind in it.)
					let ox = 0;
					let oy = 0;
					if (goes > 0.2) {
						const dx = (him.x - was.x) / goes;
						const dy = (him.y - was.y) / goes;
						const from = carrier.posAt(m.t);
						const rx = m.at.x - from.x;
						const ry = m.at.y - from.y;
						const s = ry * dx - rx * dy;
						const out = PURSUIT_SIDE_FT - Math.abs(s);
						if (rx * dx + ry * dy > 0 && out > 0) {
							const k = (s === 0 ? Math.sign(lane) : Math.sign(s)) * out;
							ox = -dy * k;
							oy = dx * k;
						}
					}
					m.chaseUp(
						this.wayClear(
							m,
							{
								x: m.at.x + him.x - now.x - Math.cos(a) * by + ox,
								y: m.at.y + him.y - now.y - Math.sin(a) * by + oy,
							},
							c,
							m.top * PURSUIT_PACE,
						),
						c,
						m.top * PURSUIT_PACE,
					);
					continue;
				}
				const fps = clamp(
					need * PURSUIT_URGE,
					m.top * 0.45,
					m.top * PURSUIT_PACE,
				);
				m.chaseUp(
					this.wayClear(
						m,
						{
							x: him.x + Math.cos(a) * off - (him.y - was.y) * side,
							y: him.y + Math.sin(a) * off + (him.x - was.x) * side,
						},
						c,
						fps,
					),
					c,
					fps,
				);
			}
			const went = m.t;
			m.chaseUp(
				this.wayClear(m, end(), arrive, m.top * 0.95),
				arrive,
				m.top * 0.95,
			);
			// (Pulled up as he gets there - not stopped dead in a stride,
			// off to the side of the pile if it is in his way - and from short
			// of the men about the ball, if that has him pulled up in them.)
			this.easeShort(m, went, EASE_OFF_FT);
			m.easeOff(EASE_OFF_FT, pt);
		}
	}

	// Through `pts` from where he is at `fps` - up to it first off v0, at A,
	// a stride at a time (as rampInto has it, but up to it all the way,
	// however short the first of them).
	private alongAt(m: Actor, pts: Pt[], fps: number, v0: number, A: number) {
		const t0 = m.t;
		const all = [m.at, ...pts];
		const lens = [0];
		for (let i = 1; i < all.length; i++) {
			lens.push(lens[i - 1]! + dist(all[i - 1]!, all[i]!));
		}
		const L = lens.at(-1)!;
		const on = (d: number): Pt => {
			const k = Math.max(
				0,
				lens.findIndex((g) => g >= d),
			);
			if (k === 0) {
				return all[0]!;
			}
			const g0 = lens[k - 1]!;
			const u = lens[k]! > g0 ? (d - g0) / (lens[k]! - g0) : 1;
			return {
				x: all[k - 1]!.x + (all[k]!.x - all[k - 1]!.x) * u,
				y: all[k - 1]!.y + (all[k]!.y - all[k - 1]!.y) * u,
			};
		};
		const ta = v0 < fps ? (fps - v0) / A : 0;
		const n = Math.max(1, Math.round((ta * 1000) / RAMP_STEP_MS));
		let up = 0;
		for (let i = 1; i <= n && ta > 0; i++) {
			const t = (ta * i) / n;
			const d = v0 * t + (A * t * t) / 2;
			if (d >= L) {
				break;
			}
			m.go(on(d), t0 + t * 1000);
			up = d;
		}
		lens.forEach((g, i) => {
			if (i > 0 && g > up) {
				m.go(all[i]!, t0 + rampedMs(g, fps, v0, A));
			}
		});
	}

	// A score: after him till he is in, and then given up - on a few
	// strides, slowing, and stood looking at him (not all in the end zone
	// round him).
	pullUp(men: Actor[], carrier: Actor, t: number) {
		const r = this.r;
		for (const m of men) {
			if (this.done.has(m.pid) || m.t >= t) {
				continue;
			}
			m.carryOn(Math.max(m.t, this.seen));
			m.plantFor(carrier.posAt(m.t + PURSUIT_LEAD));
			// (Each on a line of his own to him, and as far off him as he
			// can keep up - a man losing ground on him well back giving it up
			// early: not all in single file on his heels.)
			const ring = PURSUIT_RING * (0.8 + r() * 1.4);
			const turn = (r() - 0.5) * 0.9;
			const pace = m.top * PURSUIT_PACE * (0.88 + r() * 0.12);
			let was = dist(m.at, carrier.posAt(m.t));
			while (m.t + PURSUIT_STEP < t) {
				const c = m.t + PURSUIT_STEP;
				const him = carrier.posAt(c + PURSUIT_LEAD);
				const a = Math.atan2(m.at.y - him.y, m.at.x - him.x) + turn;
				m.chaseUp(
					this.wayClear(
						m,
						{ x: him.x + Math.cos(a) * ring, y: him.y + Math.sin(a) * ring },
						c,
						pace,
					),
					c,
					pace,
				);
				const now = dist(m.at, carrier.posAt(m.t));
				if (now > GIVE_UP_FT && now > was && r() < 0.35) {
					break;
				}
				was = now;
			}
			m.easeOff(EASE_OFF_FT);
			const at = carrier.posAt(m.t);
			m.hold(m.t + 500 + r() * 900, "stand", this.toward(m.at, at));
		}
	}

	// ---- a ball that is not caught -------------------------------------------------

	// (Whether it was thrown away.)
	incomplete(
		i: number,
		qb: Actor,
		rec: Actor,
		catchPt: Pt,
		catchAt: number,
		coverage: Actor[],
		bat?: Bat,
	): boolean {
		const r = this.r;
		const breakup = this.man(this.statPids("defPssDef")[0]);
		const ground = catchAt + INC_DROP_MS;
		// (Where it gets to: out in front of him the way it came, at his
		// hands - not on in to his chest.)
		const thrown =
			this.ball.at(-1)?.kind === "fly" ? this.ball.at(-1) : undefined;
		const came = thrown?.kind === "fly" ? this.toward(catchPt, thrown.from) : 0;
		const hands = {
			x: catchPt.x + Math.cos(came) * INC_HANDS_OUT,
			y: catchPt.y + Math.sin(came) * INC_HANDS_OUT,
		};
		let lands: Pt;
		let from = hands;
		// (Where it is let go of, if not at his hands or over them, and when;
		// the share of the way on to where it comes to rest that it drops to
		// the ground in, and how high it goes on the way and on the bounce.)
		let p0: Pt3 | undefined;
		let at0 = catchAt;
		let groundAt = ground;
		let k = 0.5;
		let dropPeak = 0.8;
		let hopPeak = 1.6;
		let away = false;
		const u = countOf(rec, this.input.key, 83);
		const throwFt = thrown?.kind === "fly" ? dist(thrown.from, catchPt) : 0;
		const pace =
			thrown?.kind === "fly"
				? dist(thrown.from, thrown.to) /
					Math.max(0.001, (thrown.t1 - thrown.t0) / 1000)
				: PASS_FPS;
		const went = came + Math.PI;
		const throwAt = thrown?.kind === "fly" ? thrown.t0 : catchAt;
		const pressed = this.d.some(
			(m) => dist(m.posAt(throwAt), qb.posAt(throwAt)) < AWAY_NEAR,
		);
		// (Thrown behind him: on his own count, one that would have gone on
		// over his hands - him running across its way at a good clip there,
		// see INC_BEHIND.)
		const behind = (() => {
			const shortOne = throwFt <= INC_SHORT_FT && u >= 1 - INC_SHORT;
			if (
				u < INC_DROP + INC_DIVE ||
				shortOne ||
				thrown?.kind !== "fly" ||
				countOf(rec, this.input.key, 136) >= INC_BEHIND
			) {
				return undefined;
			}
			const l = rec.legs.findLast((x) => x.t0 < catchAt);
			if (
				!l ||
				!GAITS.has(l.move) ||
				l.t1 < catchAt - 50 ||
				legPace(l) < INC_BEHIND_FPS
			) {
				return undefined;
			}
			const far = dist(l.from, l.to);
			const hx = (l.to.x - l.from.x) / far;
			const hy = (l.to.y - l.from.y) / far;
			// (Across it: square enough to the way it comes.)
			const across = hx * Math.sin(came) - hy * Math.cos(came);
			if (Math.abs(across) < INC_BEHIND_ACROSS) {
				return undefined;
			}
			// (Which side of him it comes from: +1 his left.)
			const side = (
				Math.cos(came) * Math.sin(Math.atan2(hy, hx)) -
					Math.sin(came) * Math.cos(Math.atan2(hy, hx)) >=
				0
					? 1
					: -1
			) as Sign;
			return { hx, hy, side };
		})();
		// (The man the sim says got a hand on it - if he gets near enough to
		// by then: not a deep man a long way off swatting at the air yards
		// from it, but on with what he was doing, and it falls incomplete
		// some other way.)
		const swatter = (() => {
			if (
				(bat && thrown?.kind === "fly") ||
				!breakup ||
				!coverage.includes(breakup)
			) {
				return undefined;
			}
			const back = breakup.saved();
			this.breakOn(breakup, catchPt, catchAt, BREAK_UP_EARLY_MS, true);
			if (
				dist(breakup.at, this.breakSpot(breakup, catchPt, true)) <=
				SWAT_REACH_FT
			) {
				return breakup;
			}
			back();
			return undefined;
		})();
		// (Knocked down at the line: when it went up off his hands, give or
		// take, for all of them - see LINE_BAT_SEEN_MS.)
		let batSeen: number | undefined;
		if (bat && thrown?.kind === "fly") {
			// Knocked down at the line: off his hands up in the air and down
			// round the line, on the hop - the man it was for pulled up once
			// he sees it go up, the man who got it round to watch it down.
			thrown.peak = bat.peak;
			p0 = bat.at;
			at0 = bat.t;
			const pop =
				LINE_BAT_POP[0] +
				(LINE_BAT_POP[1] - LINE_BAT_POP[0]) *
					countOf(bat.m, this.input.key, 118);
			// (Down under G - see the fly's 4 * peak * u * (1 - u).)
			groundAt = at0 + Math.sqrt((8 * pop) / G) * 1000;
			const far =
				LINE_BAT_FT[0] +
				(LINE_BAT_FT[1] - LINE_BAT_FT[0]) * countOf(bat.m, this.input.key, 119);
			lands = this.dropWay(
				bat.at,
				at0,
				far,
				this.toward(bat.at, thrown.from) +
					(countOf(bat.m, this.input.key, 120) - 0.5) * 2 * LINE_BAT_TURN,
			);
			k = LINE_BAT_AIR;
			dropPeak = pop;
			hopPeak = 1;
			batSeen = at0 + LINE_BAT_SEEN_MS[0];
			const seen =
				batSeen + LINE_BAT_SEEN_MS[1] * countOf(rec, this.input.key, 121);
			if (rec.t > seen) {
				rec.cut(seen);
				rec.easeOff(EASE_OFF_FT);
			}
			const m = bat.m;
			m.hold(m.t + 150, "stand", this.toward(m.at, lands));
			if (countOf(m, this.input.key, 122) < WAVE_OFF) {
				m.act("sigWave", m.t + WAVE_MS, this.toward(m.at, qb.at));
			}
		} else if (swatter) {
			// Got a hand on it: knocked away. (In at his shoulder, the side he
			// came from - not stood in the way of it.)
			const breakup = swatter;
			// (Carried on a step or so with it, off his last stride - by the
			// ball, not through the man it was for, nor on through it as he
			// gets a hand to it.)
			const swatEnd = catchAt + 250;
			const carried = () => {
				const l = breakup.last;
				const far = dist(l.from, l.to);
				const way = Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x);
				const atBall = clamp(
					(catchAt - breakup.t) / Math.max(1, swatEnd - breakup.t),
					0,
					1,
				);
				const most =
					far > 0
						? Math.min(
								SWAT_CARRY_MOST,
								(legPace(l) * Math.max(0, swatEnd - breakup.t)) /
									1000 /
									SWAT_CARRY,
							)
						: 0;
				// (On the way he was going - or, that taking him through the
				// man or the ball, veered off it a little, by them: not
				// stopped dead in his tracks for it.)
				let best: { on: number; onTo: (k: number) => Pt } | undefined;
				for (const by of SWAT_VEER) {
					const ux = Math.cos(way + by);
					const uy = Math.sin(way + by);
					const onTo = (k: number) => ({
						x: l.to.x + ux * k,
						y: l.to.y + uy * k,
					});
					let on = most;
					while (
						on > 0.25 &&
						(dist(onTo(on), catchPt) < SWAT_CLEAR ||
							dist(onTo(on * atBall), hands) < BREAK_ASIDE)
					) {
						on -= 0.25;
					}
					if (!best || on > best.on + SWAT_VEER_GAIN) {
						best = { on, onTo };
					}
					if (on >= most) {
						break;
					}
				}
				return { v: legPace(l), ...best! };
			};
			let swat = carried();
			// (Coming in quicker than the swat carries him on from - slowing
			// to a stand over it, see pace.ts carriedOf: twice its own pace -
			// gathered for it over the last of his way in, not cut down to
			// it in a stride.)
			const vIn = (2 * swat.on) / Math.max(0.05, (swatEnd - breakup.t) / 1000);
			if (swat.v > vIn + SWAT_GATHER_FPS) {
				breakup.breakDown(
					vIn / swat.v,
					STOP_ACCEL * STOP_HARD,
					SWAT_GATHER_SPARE_MS,
				);
				swat = carried();
			}
			breakup.act(
				"swat",
				swatEnd,
				this.toward(breakup.at, catchPt),
				swat.onTo(swat.on),
			);
			this.done.add(breakup.pid);
			if (this.reachFor(rec, catchAt, qb)) {
				this.runOn(rec, 600);
			}
			lands = this.dropWay(hands, catchAt, INC_KNOCKED_FT, r() * Math.PI * 2);
			if (r() < WAVE_OFF) {
				this.slowUp(breakup);
				breakup.act(
					"sigWave",
					breakup.t + WAVE_MS,
					this.toward(breakup.at, qb.at),
				);
			}
		} else if (
			pressed &&
			thrown?.kind === "fly" &&
			countOf(rec, this.input.key, 84) < AWAY
		) {
			// Thrown away: nobody's ball, high over the man it went to and
			// down well past him - out of bounds, if the sideline comes
			// first. Him on a step or two and pulled up, watching it go; the
			// men on him pulled up too, nobody going for it (laid first, so
			// it comes down clear of where they are).
			away = true;
			this.runOn(rec, 700);
			this.slowUp(rec);
			for (const m of coverage) {
				if (this.done.has(m.pid)) {
					continue;
				}
				const seen = catchAt + DEAD_SEEN_MS[0];
				if (m.t > seen + 300) {
					m.cut(seen);
				}
				m.easeOff(EASE_OFF_FT);
			}
			const c = this.of(catchPt);
			const q = this.of(thrown.from);
			const way = Math.atan2(c.a - q.a, c.d - q.d);
			// (Harder, the harder he throws - see flightOf - and no longer in
			// the air than AWAY_HANG_MS.)
			const fps = (far: number) =>
				Math.max(pace * AWAY_KEEP, far / (AWAY_HANG_MS / 1000));
			const past0 =
				AWAY_PAST[0] +
				(AWAY_PAST[1] - AWAY_PAST[0]) * countOf(rec, this.input.key, 85);
			const spotFor = (turn: number, longer: number) => {
				const ud = Math.cos(way + turn);
				const ua = Math.sin(way + turn);
				// (From where it was thrown, as far as it would have gone and
				// on past - or to just out of bounds, or the back of the end
				// zone, if one of them comes first.)
				let far =
					Math.min(Math.hypot(c.d - q.d, c.a - q.a) + past0, AWAY_MOST_FT) +
					longer;
				if (ua !== 0) {
					const edge =
						ua > 0 ? this.bounds.right + AWAY_OUT : this.bounds.left - AWAY_OUT;
					far = Math.min(far, Math.max(0, (edge - q.a) / ua));
				}
				if (ud > 0) {
					far = Math.min(far, Math.max(0, (this.goalD + 27 - q.d) / ud));
				}
				const pt = this.w({ d: q.d + ud * far, a: q.a + ua * far });
				const t =
					thrown.t0 +
					(dist(thrown.from, pt) / fps(dist(thrown.from, pt))) * 1000;
				const clear = Math.min(...this.men.map((m) => dist(m.posAt(t), pt)));
				return { pt, clear };
			};
			let best = spotFor(0, 0);
			for (const turn of AWAY_TURNS) {
				for (const longer of AWAY_LONGER) {
					const x = spotFor(turn, longer);
					if (x.clear > best.clear) {
						best = x;
					}
					if (best.clear >= AWAY_CLEAR) {
						break;
					}
				}
				if (best.clear >= AWAY_CLEAR) {
					break;
				}
			}
			const end = best.pt;
			const fl = dist(thrown.from, end) / fps(dist(thrown.from, end));
			at0 = thrown.t0 + fl * 1000;
			p0 = { ...end, z: 0 };
			groundAt = at0;
			thrown.peak = (G * fl * fl) / 8;
			lands = {
				x: end.x + Math.cos(this.toward(thrown.from, end)) * AWAY_BOUNCE,
				y: end.y + Math.sin(this.toward(thrown.from, end)) * AWAY_BOUNCE,
			};
		} else if (behind) {
			// Thrown behind him running across: an arm back and out for it as
			// it goes by behind him - the arm on the side it comes from - off
			// his hand, and down on past him the way it was going. Him on
			// with his run, pulled up after it.
			const { hx, hy, side } = behind;
			rec.pin(catchAt);
			const l = rec.last;
			if (GAITS.has(l.move) && rec.t < catchAt) {
				rec.carryOn(catchAt);
			}
			this.reaches.push({
				m: rec,
				move: side > 0 ? "reachBackL" : "reachBack",
				t0: catchAt - INC_BEHIND_REACH_MS * REACH_BACK_AT,
				t1: catchAt + INC_BEHIND_REACH_MS * (1 - REACH_BACK_AT),
			});
			this.runOn(rec, 700);
			this.slowUp(rec);
			// (His left: see bodyPoint.)
			const lx = Math.sin(Math.atan2(hy, hx));
			const ly = -Math.cos(Math.atan2(hy, hx));
			p0 = {
				x: catchPt.x - hx * INC_BEHIND_FT + lx * side * INC_BEHIND_OUT,
				y: catchPt.y - hy * INC_BEHIND_FT + ly * side * INC_BEHIND_OUT,
				z: INC_BEHIND_Z,
			};
			groundAt = catchAt + INC_BEHIND_DROP_MS;
			lands = this.dropWay(p0, catchAt, INC_BEHIND_ON, went);
			k = 0.55;
			dropPeak = 0.3;
			hopPeak = 1;
		} else if (u < INC_DROP) {
			// Off his hands.
			if (this.reachFor(rec, catchAt, qb)) {
				this.runOn(rec, 600);
				if (r() < RUE) {
					this.slowUp(rec);
					rec.act("handsOnHelmet", rec.t + RUE_MS);
				}
			}
			lands = this.dropWay(hands, catchAt, INC_OFF_FT, r() * Math.PI * 2);
		} else if (
			u >= INC_DROP + INC_DIVE &&
			throwFt <= INC_SHORT_FT &&
			u >= 1 - INC_SHORT
		) {
			// Short: into the turf in front of him, and on round him on the
			// hop - him pulled up for it, or there waiting on it.
			p0 = {
				x: hands.x + Math.cos(came) * INC_SHORT_BY,
				y: hands.y + Math.sin(came) * INC_SHORT_BY,
				z: 0,
			};
			at0 = catchAt - ((INC_SHORT_BY + INC_HANDS_OUT) / pace) * 1000;
			groundAt = at0;
			lands = this.dropWay(p0, at0, INC_HOP_FT, went);
			const l = rec.last;
			if (GAITS.has(l.move) && legPace(l) >= STOP_LEAST) {
				this.runOn(rec, 600);
			} else {
				rec.hold(catchAt + 600);
			}
			this.palmsUp(rec, qb, INC_PALMS);
		} else if (u >= INC_DROP + INC_DIVE) {
			// Over his hands: up at it as it goes on over them, on past him
			// as far as it goes in the time it takes to come down, and on
			// the bounce.
			if (this.reachFor(rec, catchAt, qb, true)) {
				this.runOn(rec, 700);
				this.palmsUp(rec, qb, INC_PALMS_OVER);
			}
			const sail = clamp(
				(pace * INC_SAIL_KEEP * INC_DROP_MS) / 1000,
				INC_SAIL_FT[0],
				INC_SAIL_FT[1],
			);
			p0 = { ...catchPt, z: INC_OVER_UP_Z };
			lands = this.dropWay(catchPt, catchAt, sail + INC_SAIL_BOUNCE, went);
			k = sail / (sail + INC_SAIL_BOUNCE);
			dropPeak = 0.3;
			hopPeak = 1.2;
		} else {
			// Out of his reach: on past him, and a dive for it.
			const dx =
				catchPt.x -
				(this.ball.at(-1)?.kind === "fly"
					? (this.ball.at(-1) as { from: Pt3 }).from.x
					: catchPt.x);
			const over = 10 + r() * 14;
			lands = {
				x: catchPt.x + Math.sign(dx || this.input.dir) * over,
				y: catchPt.y + (r() - 0.5) * 10,
			};
			rec.cut(Math.min(rec.t, catchAt - 250));
			rec.hold(catchAt - 250);
			const toward = this.toward(rec.at, lands);
			rec.lunge("dive", catchAt + 450, toward, {
				x: rec.at.x + Math.cos(toward) * 5,
				y: rec.at.y + Math.sin(toward) * 5,
			});
			rec.hold(catchAt + 450 + 500, "prone");
			rec.act("getUp", catchAt + 450 + 500 + GET_UP_MS);
			from = catchPt;
		}
		this.done.add(rec.pid);
		// It drops, and bounces. (Over his hands, out of his reach.)
		const at: Pt3 = p0 ?? {
			...from,
			z: from === catchPt ? INC_OVER_Z : CATCH_Z,
		};
		if (thrown?.kind === "fly") {
			thrown.t1 = at0;
			thrown.to = at;
		}
		if (at.z <= 0) {
			// (Down already: on the hop.)
			this.ball.push(
				{
					kind: "fly",
					t0: at0,
					t1: at0 + INC_HOP_MS,
					from: at,
					to: { ...lands, z: 0 },
					peak: INC_HOP_PEAK,
				},
				{ kind: "spot", t0: at0 + INC_HOP_MS, at: { ...lands, z: 0 } },
			);
		} else {
			const mid = {
				x: at.x + (lands.x - at.x) * k,
				y: at.y + (lands.y - at.y) * k,
			};
			this.ball.push(
				{
					kind: "fly",
					t0: at0,
					t1: groundAt,
					from: at,
					to: { ...mid, z: 0 },
					peak: dropPeak,
				},
				{
					kind: "fly",
					t0: groundAt,
					t1: groundAt + INC_BOUNCE_MS,
					from: { ...mid, z: 0 },
					to: { ...lands, z: 0 },
					peak: hopPeak,
				},
				{
					kind: "spot",
					t0: groundAt + INC_BOUNCE_MS,
					at: { ...lands, z: 0 },
				},
			);
		}
		this.at.set(i, groundAt);
		this.whistle = groundAt;
		if (away) {
			return true;
		}
		if (batSeen === undefined) {
			this.pursue(coverage, catchPt, catchAt + 200, []);
		}
		// (Not on in at it after, all of them round the man it was for.)
		for (const m of coverage) {
			const seen =
				batSeen !== undefined
					? batSeen + LINE_BAT_SEEN_MS[1] * countOf(m, this.input.key, 121)
					: groundAt + DEAD_SEEN_MS[0] + r() * DEAD_SEEN_MS[1];
			const l = m.legs.findLast((x) => x.t0 < seen);
			if (
				this.done.has(m.pid) ||
				m.t <= seen + 300 ||
				!l ||
				!GAITS.has(l.move) ||
				legPace(l) < STOP_LEAST
			) {
				continue;
			}
			m.cut(seen);
			m.easeOff(EASE_OFF_FT, lands);
		}
		return false;
	}

	// A defender breaking on the ball: from his coverage for where it comes
	// down, there `early` ms before it (after it, negative) - just to the
	// quarterback's side of the receiver (and to knock it away, at his
	// shoulder, the side he came from) - or as near as he gets.
	breakOn(
		m: Actor,
		catchPt: Pt,
		catchAt: number,
		early: number,
		aside = false,
	) {
		const spot = this.breakSpot(m, catchPt, aside);
		const by = catchAt - early;
		const latest = Math.max(this.seen, catchAt - 700);
		// (There or near enough by then on his own - or no time to be
		// anywhere else: left be.)
		if (
			dist(m.posAt(by), spot) <= BREAK_KEEP_FT ||
			by - latest < BREAK_LEAST_MS
		) {
			m.cut(by);
			return;
		}
		const go = this.breakSoonest(m, spot, by, latest);
		const slow =
			go === undefined ? this.breakSlowing(m, spot, by, latest) : undefined;
		const t0 = go ?? slow?.t ?? latest;
		m.cut(t0);
		// (His coverage run out short of then, on his way to it already: on
		// into it from there - not eased off to a stand and off again.)
		const l0 = m.last;
		if (
			!(
				m.t < t0 &&
				GAITS.has(l0.move) &&
				legPace(l0) >= STOP_LEAST &&
				dist(l0.from, l0.to) > 0 &&
				angleAt(l0.from, l0.to, spot) <= CUT_LEAST
			)
		) {
			m.hold(t0);
		}
		// (Planted first, going the other way - not turned round on the
		// spot at full tilt - if there is the time for it; if not, slowed
		// as he comes up to it, to a stop by it, if he saw it in time; if
		// not, stopped short, hard, before he comes back for it.)
		if (go !== undefined) {
			m.plantFor(spot);
		} else if (slow) {
			m.easeOff(slow.ft);
		} else {
			const l = m.last;
			const away =
				(l.to.x - l.from.x) * (spot.x - l.to.x) +
					(l.to.y - l.from.y) * (spot.y - l.to.y) <
				0;
			const v = legPace(l);
			if (away && v >= STOP_LEAST) {
				// (Going to knock it away, flat out: pulled up no harder than a
				// man can - see STOP_HARD - not stopped dead in a stride; too
				// far on for it, maybe, then - see Action.incomplete.)
				m.easeOff(
					aside
						? Math.max(BREAK_STOP_FT, (v * v) / (2 * STOP_ACCEL * STOP_HARD))
						: BREAK_STOP_FT,
				);
			}
		}
		// (Back up to speed for it no harder than a man can - short of it, if
		// that is all he gets, not shot back at it from a stand. On his way
		// to it already, with time to spare: eased into it as he comes - not
		// on at full tilt to a stride off it and jogged the rest.)
		if (!m.slowAlong(spot, by)) {
			m.pressUp(spot, by, m.top * FLAT_OUT, "sprint");
		}
	}

	// Going on past where he is to be for it at `latest`, with no time to
	// plant and come back: slowed sooner, as he comes up to it - as hard as
	// a man pulls up (see BREAK_SLOW_ACCEL), from where that stops him
	// nearest it - on the way he was going still, once he has seen it.
	// (When he starts, and how far on he goes; none, if that has him no
	// nearer it, or there after `by`.)
	private breakSlowing(
		m: Actor,
		spot: Pt,
		by: number,
		latest: number,
	): { t: number; ft: number } | undefined {
		const k = 1000 / BREAK_SOONER_STEP_MS;
		const velAt = (t: number) => {
			const at = m.posAt(t);
			const was = m.posAt(t - BREAK_SOONER_STEP_MS);
			return { at, vx: (at.x - was.x) * k, vy: (at.y - was.y) * k };
		};
		const end = velAt(latest);
		const v1 = Math.hypot(end.vx, end.vy);
		if (
			v1 < STOP_LEAST ||
			end.vx * (spot.x - end.at.x) + end.vy * (spot.y - end.at.y) >= 0
		) {
			return undefined;
		}
		const way = Math.atan2(end.vy, end.vx);
		// (Where the hard stop off `latest` would leave him.)
		const was = dist(
			{
				x: end.at.x + (end.vx / v1) * BREAK_STOP_FT,
				y: end.at.y + (end.vy / v1) * BREAK_STOP_FT,
			},
			spot,
		);
		let best: { t: number; ft: number; off: number } | undefined;
		for (let t = latest; t >= this.seen; t -= BREAK_SOONER_STEP_MS) {
			const { at, vx, vy } = velAt(t);
			const v = Math.hypot(vx, vy);
			if (
				v < STOP_LEAST ||
				Math.abs(angleOff(Math.atan2(vy, vx) - way)) > BREAK_SLOW_TURN
			) {
				break;
			}
			const ft = (v * v) / (2 * BREAK_SLOW_ACCEL);
			const stop = { x: at.x + (vx / v) * ft, y: at.y + (vy / v) * ft };
			const off = dist(stop, spot);
			if (t + ((2 * ft) / v) * 1000 <= by && (!best || off < best.off)) {
				best = { t, ft, off };
			}
		}
		return best && best.off < was ? { t: best.t, ft: best.ft } : undefined;
	}

	// Where he gets to it from: in front of it, toward the quarterback - or,
	// knocking it away, `aside`, in at the shoulder of the man it was for,
	// the side he comes from.
	private breakSpot(m: Actor, catchPt: Pt, aside: boolean): Pt {
		const qb = this.role("QB");
		const toQb = qb ? this.toward(catchPt, qb.at) : this.behind;
		const off = aside
			? Math.sign(
					Math.cos(toQb) * (m.at.y - catchPt.y) -
						Math.sin(toQb) * (m.at.x - catchPt.x),
				) || 1
			: 0;
		return {
			x:
				catchPt.x +
				Math.cos(toQb) * (aside ? BREAK_IN : 2.2) -
				Math.sin(toQb) * off * BREAK_ASIDE,
			y:
				catchPt.y +
				Math.sin(toQb) * (aside ? BREAK_IN : 2.2) +
				Math.cos(toQb) * off * BREAK_ASIDE,
		};
	}

	// When to break on it, `latest` at the latest: sooner - as soon as he
	// sees it, at most - if he is going away from it then, to plant and
	// be back to it by `by` (see plantFor). (None, if he can't be.)
	private breakSoonest(
		m: Actor,
		spot: Pt,
		by: number,
		latest: number,
	): number | undefined {
		const fps = m.top * FLAT_OUT;
		for (let go = latest; go >= this.seen; go -= BREAK_SOONER_STEP_MS) {
			const at = m.posAt(go);
			const was = m.posAt(go - BREAK_SOONER_STEP_MS);
			const k = 1000 / BREAK_SOONER_STEP_MS;
			const vx = (at.x - was.x) * k;
			const vy = (at.y - was.y) * k;
			const v = Math.hypot(vx, vy);
			const far = dist(at, spot);
			const away =
				far > 0 && v > 0
					? -(vx * (spot.x - at.x) + vy * (spot.y - at.y)) / far
					: 0;
			// (Back over the ground he plants in, as much of it as was away.)
			const plant = away > 0 ? (v / CUT_ACCEL) * 1000 : 0;
			const past = away > 0 ? ((v * v) / (2 * CUT_ACCEL)) * (away / v) : 0;
			if (go + plant + rampedMs(far + past, fps, 0) <= by) {
				return go;
			}
		}
		return undefined;
	}

	intercepted(
		i: number,
		qb: Actor,
		rec: Actor,
		catchPt: Pt,
		catchAt: number,
		coverage: Actor[],
		bat?: Bat,
	) {
		const r = this.r;
		const picker =
			this.man(this.statPids("defInt")[0]) ??
			this.nearest(coverage, catchPt, 1)[0];
		this.at.set(i, catchAt);
		if (bat) {
			// (Knocked up at the line: the man it was meant for pulled up once
			// he sees it go up - see LINE_BAT_SEEN_MS.)
			const seen =
				bat.t +
				LINE_BAT_SEEN_MS[0] +
				LINE_BAT_SEEN_MS[1] * countOf(rec, this.input.key, 121);
			if (rec.t > seen) {
				rec.cut(seen);
				rec.easeOff(EASE_OFF_FT);
			}
		} else if (this.reachFor(rec, catchAt, qb)) {
			this.runOn(rec, 500);
		}
		if (!picker) {
			this.done.add(rec.pid);
			this.whistle = catchAt + 600;
			return;
		}
		const retI = this.find("interceptionReturn", i);
		const ret = retI >= 0 ? this.ev(retI) : undefined;
		const yds = typeof ret?.yds === "number" ? ret.yds : 0;
		const td = ret?.td === true;
		// How he gets it, on his own count: now and then off the hands of
		// the man it was meant for, popped up (see PICK_TIP); up off his
		// feet for one thrown a long way (PICK_LEAP); laid out for it, with
		// little or nothing to come of it after (PICK_DIVE) - or in front of
		// him and away.
		const thrown = this.ball.at(-1);
		const long = thrown?.kind === "fly" ? dist(thrown.from, thrown.to) : 0;
		const u = countOf(picker, this.input.key, 111);
		const recAt = rec.posAt(catchAt);
		const tip =
			!bat &&
			thrown?.kind === "fly" &&
			u < PICK_TIP &&
			dist(recAt, catchPt) < PICK_TIP_NEAR;
		let leap = !bat && !tip && long >= PICK_LEAP_FT && u < PICK_TIP + PICK_LEAP;
		let dive =
			!bat &&
			!tip &&
			!leap &&
			!td &&
			yds <= PICK_DIVE_YDS &&
			u < PICK_TIP + PICK_LEAP + PICK_DIVE;
		// (Off the receiver's hands: up off them a little way the side the
		// man picking it comes from, and his then.)
		let gotAt = catchAt;
		let gotPt = catchPt;
		if (tip && thrown?.kind === "fly") {
			gotAt = catchAt + PICK_TIP_MS;
			const from = picker.posAt(catchAt - 500);
			const far = dist(from, catchPt) || 1;
			gotPt = {
				x: catchPt.x + ((from.x - catchPt.x) / far) * PICK_TIP_OFF,
				y: catchPt.y + ((from.y - catchPt.y) / far) * PICK_TIP_OFF,
			};
			const face = this.toward(recAt, qb.posAt(catchAt));
			thrown.to = {
				x: recAt.x + Math.cos(face),
				y: recAt.y + Math.sin(face),
				z: CATCH_Z,
			};
			this.ball.push({
				kind: "fly",
				t0: catchAt,
				t1: gotAt,
				from: thrown.to,
				to: { ...gotPt, z: CATCH_Z },
				peak: PICK_TIP_UP,
			});
		}
		// Knocked up at the line (see batAt): the throw cut short at the man
		// who got a hand to it, up off his hands and down to the man who
		// picks it off - him, a step on from where he comes down, or one
		// near enough, over to it under it.
		const self = bat?.m === picker;
		if (bat && thrown?.kind === "fly") {
			thrown.peak = bat.peak;
			thrown.t1 = bat.t;
			thrown.to = bat.at;
			const pop =
				PICK_BAT_POP[0] +
				(PICK_BAT_POP[1] - PICK_BAT_POP[0]) *
					countOf(picker, this.input.key, 138);
			// (Down under G - see the fly's 4 * peak * u * (1 - u).)
			gotAt = bat.t + Math.sqrt((8 * pop) / G) * 1000;
			if (self) {
				const on = this.toward(thrown.from, bat.at);
				const ft =
					PICK_BAT_SELF_FT[0] +
					(PICK_BAT_SELF_FT[1] - PICK_BAT_SELF_FT[0]) *
						countOf(picker, this.input.key, 139);
				gotPt = {
					x: picker.at.x + Math.cos(on) * ft,
					y: picker.at.y + Math.sin(on) * ft,
				};
			} else {
				const p = picker.posAt(bat.t);
				const far = dist(bat.at, p) || 1;
				const ft = Math.min(far, PICK_BAT_FT);
				gotPt = {
					x: bat.at.x + ((p.x - bat.at.x) / far) * ft,
					y: bat.at.y + ((p.y - bat.at.y) / far) * ft,
				};
			}
			this.ball.push({
				kind: "fly",
				t0: bat.t,
				t1: gotAt,
				from: bat.at,
				to: { ...gotPt, z: CATCH_Z },
				peak: pop,
			});
			this.at.set(i, gotAt);
			// (The one who got a hand to it round to watch it down, and with
			// the rest of them after.)
			if (!self) {
				const m = bat.m;
				m.hold(m.t + 150, "stand", this.toward(m.at, gotPt));
				this.done.delete(m.pid);
			}
		}
		// He jumps the route and takes it the other way. (Off his own hands,
		// over under it from where he comes down, his hands up to it.)
		if (self) {
			const face = this.toward(picker.at, gotPt);
			const under = {
				x: gotPt.x - Math.cos(face) * PICK_BAT_HANDS_FT,
				y: gotPt.y - Math.sin(face) * PICK_BAT_HANDS_FT,
			};
			const by = gotAt - CATCH_REACH_MS;
			if (dist(picker.at, under) > 0.5 && by - picker.t > 200) {
				picker.go(under, by, "walk", face);
			} else {
				picker.hold(Math.max(picker.t, by), "stand", face);
			}
		} else {
			this.breakOn(picker, gotPt, gotAt, 60);
		}
		// (Up off his feet for it only from near enough it to get there in
		// the leap - see PICK_LEAP_REACH - off the pace he goes up at, on
		// toward it: from a stand, or going away from it, no further than
		// PICK_LEAP_STILL_FT, not flown back at it.)
		if (leap) {
			const t0 = gotAt - LEAP_MS / 2;
			const from = picker.posAt(t0);
			const l = legAt(picker, t0);
			const v = GAITS.has(l.move) ? paceOn(l, from, gotPt) : 0;
			const reach = Math.min(
				PICK_LEAP_REACH,
				PICK_LEAP_STILL_FT + (v * LEAP_MS) / 2000,
			);
			if (dist(from, gotPt) > reach) {
				leap = false;
			}
		}
		const ball = this.ball.at(-1);
		const fly = ball?.kind === "fly" ? ball : undefined;
		// (Laid out for it: from where he is as he leaves his feet, toward
		// it, no further than he could - the ball to his hands out there.
		// Going at a run: the way he goes, no more than PICK_DIVE_TURN round
		// from it - not launched off sideways from a sprint, or back the
		// way he came. Short of the man it was meant for, if he is in the
		// way of it - see PICK_DIVE_CLEAR_FT.)
		let laid: { face: number; to: Pt } | undefined;
		if (dive) {
			const from = picker.posAt(gotAt - DIVE_BEFORE);
			let face = this.toward(from, gotPt);
			const was = picker.posAt(gotAt - DIVE_BEFORE - PICK_DIVE_LOOK_MS);
			if ((dist(was, from) * 1000) / PICK_DIVE_LOOK_MS >= LUNGE_LEAST_FPS) {
				const runs = Math.atan2(from.y - was.y, from.x - was.x);
				face =
					runs + clamp(angleOff(face - runs), -PICK_DIVE_TURN, PICK_DIVE_TURN);
			}
			let far = clamp(
				legPace(legAt(picker, gotAt - DIVE_BEFORE)) * PICK_DIVE_PER,
				PICK_DIVE_LEAST,
				PICK_DIVE_MOST,
			);
			const him = rec.posAt(gotAt);
			const along =
				(him.x - from.x) * Math.cos(face) + (him.y - from.y) * Math.sin(face);
			const across = Math.abs(
				(him.x - from.x) * Math.sin(face) - (him.y - from.y) * Math.cos(face),
			);
			if (across < PICK_DIVE_CLEAR_FT && along > 0) {
				far = Math.min(
					far,
					along - Math.sqrt(PICK_DIVE_CLEAR_FT ** 2 - across ** 2),
				);
			}
			if (far >= PICK_DIVE_LEAST) {
				laid = {
					face,
					to: {
						x: from.x + Math.cos(face) * far,
						y: from.y + Math.sin(face) * far,
					},
				};
			} else {
				dive = false;
			}
		}
		const onRun =
			!leap &&
			!dive &&
			GAITS.has(picker.last.move) &&
			legPace(picker.last) >= PICK_ON_FPS;
		if (leap) {
			// (Taken back the way the offense came.)
			const got = this.of(gotPt);
			this.leapFor(
				picker,
				gotPt,
				gotAt,
				qb,
				this.w({ d: got.d - 10, a: got.a }),
			);
		} else if (laid) {
			this.diveFor(picker, laid.to, gotAt, laid.face);
			picker.act("getUp", picker.t + GET_UP_MS);
		} else if ((onRun || tip || bat) && !self) {
			// (Off a tip: his hands up for it as it comes off the other
			// man's, on it all the way down - knocked up at the line, from
			// once he is under it.)
			this.reaches.push({
				m: picker,
				move: "catch",
				t0: tip
					? catchAt
					: bat
						? Math.max(gotAt - PICK_BAT_REACH_MS, bat.t + 300)
						: gotAt - CATCH_REACH_MS,
				t1: gotAt + CATCH_IN_MS,
			});
		} else {
			// (Stood waiting on it the wrong way round - pulled up going
			// away from it: turned round to it as he waits, not spun round
			// to it in a blink as it comes.)
			const face = this.toward(picker.at, gotPt);
			const l = picker.last;
			const off = Math.atan2(
				Math.sin(face - picker.yaw),
				Math.cos(face - picker.yaw),
			);
			if (
				l.move === "stand" &&
				l.yaw !== undefined &&
				!l.turn &&
				l.from.x === l.to.x &&
				l.from.y === l.to.y &&
				l.t1 > l.t0 &&
				Math.abs(off) > PICK_TURN_LEAST
			) {
				// (No quicker round than PICK_TURN_RATE: the rest of the way
				// round as he takes it in.)
				const most = ((l.t1 - l.t0) / 1000) * PICK_TURN_RATE;
				const by = clamp(off, -most, most);
				picker.turn(by);
				picker.catchOn(gotAt, picker.yaw);
				if (Math.abs(off - by) > 0.01) {
					picker.turn(off - by);
				}
			} else {
				picker.catchOn(gotAt, face);
			}
		}
		if (fly && !leap && !dive) {
			fly.to = { ...picker.at, z: CATCH_Z };
		}
		this.ball.push({ kind: "held", t0: gotAt, pid: picker.pid });
		// (Taken back from where the sim says it was picked off.)
		const at = this.ev(i).yds;
		const pickD =
			typeof at === "number"
				? Math.min(at * YD, this.goalD + 25)
				: this.of(picker.at).d;
		let d = pickD - yds * YD;
		if (td) {
			d = this.ownGoalD - 4 - r() * 12;
		} else if (ret?.touchback) {
			d = Math.max(pickD, this.goalD + 3);
		}
		const pt = this.w({
			d: clamp(d, this.ownGoalD - 27, this.goalD + 27),
			a: clamp(
				this.of(picker.at).a + (r() - 0.5) * (dive ? 8 : 30),
				this.bounds.left + 6,
				this.bounds.right - 6,
			),
		});
		let t = picker.t;
		if (dist(picker.at, pt) > 2) {
			t = picker.run(pt, picker.top * 0.9, "sprint");
		}
		// (Caught on the run, or off a tip: where he was as it got to him.)
		if ((onRun || tip || (bat && !self)) && fly) {
			fly.to = { ...picker.posAt(gotAt), z: CATCH_Z };
		}
		// The offense sees it picked off, and comes after him.
		this.seen = (bat ? gotAt : catchAt) + BREAK_MS;
		// (And his own side, through with what they were doing, after it
		// with him at an easy run - not stood where the pass left them.)
		// (Each to a spot of his own by it - see WITH_IT_APART - not two
		// from the one place on into each other there.)
		const taken: Pt[] = [];
		for (const m of this.men) {
			if (
				m.team === picker.team &&
				m !== picker &&
				!this.done.has(m.pid) &&
				m.t < t - WITH_IT_LEAST_MS
			) {
				const ring =
					WITH_IT_FT[0] + WITH_IT_FT[1] * countOf(m, this.input.key, 21);
				const far = dist(m.at, pt);
				const by = Math.max(m.t + WITH_IT_LEAST_MS, t);
				if (
					far > ring + 2 &&
					((far - ring) / Math.max(0.001, by - m.t)) * 1000 < WALK_MOST_FPS
				) {
					// (Time enough to walk it: a look, and after it at a jog,
					// kept off it - not walked there while it is still going.
					// Pulled up for the look off his pace, not stopped from it
					// in a stride.)
					m.easeOff(EASE_OFF_FT);
					m.hold(
						m.t +
							TRAIL_LOOK_MS[0] +
							(TRAIL_LOOK_MS[1] - TRAIL_LOOK_MS[0]) *
								countOf(m, this.input.key, 29),
					);
					this.alongWith(
						m,
						TRAIL_JOG_FPS[0] +
							(TRAIL_JOG_FPS[1] - TRAIL_JOG_FPS[0]) *
								countOf(m, this.input.key, 30),
						Math.max(ring, TRAIL_KEEP_FT),
						t - 300,
					);
				} else if (far > ring + 2) {
					const a0 = Math.atan2(m.at.y - pt.y, m.at.x - pt.x);
					const spot = (a: number) => ({
						x: pt.x + Math.cos(a) * ring,
						y: pt.y + Math.sin(a) * ring,
					});
					const step = WITH_IT_APART / ring;
					let to = spot(a0);
					for (let k = 1; k <= 6; k++) {
						if (taken.every((q) => dist(q, to) >= WITH_IT_APART)) {
							break;
						}
						to = spot(a0 + (k % 2 ? 1 : -1) * Math.ceil(k / 2) * step);
					}
					taken.push(to);
					// (Round to it easing off toward the pace that has him there,
					// if he can be - see Actor.turnFor - not round it flat out
					// and pulled up hard after.)
					m.turnFor(
						to,
						Math.min(
							m.top * WITH_IT_PACE,
							dist(m.at, to) / Math.max(0.001, (by - m.t) / 1000),
						),
					);
					m.chase(to, by, m.top * WITH_IT_PACE);
				} else if (t - 300 - m.t >= ROUTE_AFTER_LEAST_MS) {
					// (Where it ends up already: out to meet it, and back with
					// it as it comes - kept off it - not stood waiting for it.)
					m.easeOff(EASE_OFF_FT);
					m.hold(
						m.t +
							TRAIL_LOOK_MS[0] +
							(TRAIL_LOOK_MS[1] - TRAIL_LOOK_MS[0]) *
								countOf(m, this.input.key, 29),
					);
					this.alongWith(m, m.top * WITH_IT_PACE, ring, t - 300);
				}
			}
		}
		// (The man it was meant for, as often as not right there, among them.)
		const others = this.o.filter((m) => !this.done.has(m.pid));
		if (retI < 0) {
			this.whistle = t;
		} else {
			this.endReturn(retI, picker, pt, t, td, others);
		}
	}

	// The end of a carry back the other way (a pick, a kick returned):
	// brought down - or into the end zone, by nobody.
	endReturn(
		i: number,
		carrier: Actor,
		pt: Pt,
		t: number,
		td: boolean,
		chasers: Actor[],
	) {
		if (!td) {
			this.endCarry(i, carrier, { pt, td: false }, t, chasers, undefined, true);
			return;
		}
		const crossT = this.crossingBack(carrier, t);
		this.at.set(i, crossT);
		this.whistle = crossT;
		carrier.hold(t + 2200, "stand");
		this.done.add(carrier.pid);
		// (After him till he is in, then given up: not all of them on to
		// the end zone round him - see endCarry.)
		this.pullUp(chasers, carrier, crossT);
	}

	crossingBack(m: Actor, fallback: number): number {
		const gx = this.input.dir === 1 ? GOAL_L : GOAL_R;
		return this.inWith(m, gx, -this.input.dir, fallback);
	}

	// ---- the quarterback brought down, or away with it -------------------------------

	sacked(
		i: number,
		qb: Actor,
		t: number,
		sackD: number,
		sacker: Actor | undefined,
		runners: Actor[] = [],
	) {
		const r = this.r;
		const { snap } = this.input;
		const from = this.of(qb.at);
		const near = (r() - 0.5) * 14;
		// (Now and then, on his own count - and with the room behind the
		// line for it - out of the pocket away from the man coming, and run
		// down out there: see SACK_OUT.)
		const side = sacker
			? Math.sign(from.a - this.of(sacker.posAt(snap)).a) || 1
			: from.a > 0
				? -1
				: 1;
		const out =
			sackOut(qb, this.input.key, sackD) && qb.t <= t - SACK_OUT_MS - 300;
		let at = this.w({
			d: sackD,
			a: clamp(
				out
					? from.a +
							side *
								(SACK_OUT_FT[0] +
									(SACK_OUT_FT[1] - SACK_OUT_FT[0]) *
										countOf(qb, this.input.key, 106))
					: from.a + near,
				this.bounds.left + 6,
				this.bounds.right - 6,
			),
		});
		if (out) {
			// Out of it: the last of his look downfield, a moment in the
			// pocket feeling it, and away - running, his eyes still out
			// there.
			this.scan(qb, undefined, this.ahead, t - SACK_OUT_MS - 300, runners);
			qb.hold(t - SACK_OUT_MS, "pocket", this.ahead);
			qb.go(at, t);
		} else {
			// (Brought down no deeper than his line is set: stepped up into
			// it through a gap - see pocketGap - not run up his center's back.)
			at = this.pocketGap(qb, at, Math.max(qb.t, t - 800), t);
			// (Now and then out from under another of them first: see
			// slipFirst.)
			const slipped = sacker && this.slipFirst(qb, t, sacker, at, runners);
			if (slipped) {
				at = slipped;
			} else {
				// Feeling it, he tries to get away from it - eyes still
				// downfield, shuffling, slow as it is (see faced): not walked
				// into it. (Back far enough before it, stepping up into it.)
				// Or - now and then, the man coming round from his left - his
				// eyes out to his right, never seeing him: see SACK_BLIND.
				const look = this.blindLook(qb, sacker, t, runners) ?? this.ahead;
				this.scan(qb, undefined, look, t - 800, runners);
				qb.hold(t - 800, "pocket", look);
				qb.go(at, t, undefined, look);
			}
		}
		this.endCarry(
			i,
			qb,
			{ pt: at, td: false },
			t,
			this.d,
			sacker ? [sacker] : undefined,
		);
		if (sacker) {
			this.rushMove(sacker, qb, t);
		}
	}

	// Where he goes down, `at`, moved across - the least way - to where his
	// way there from where he is (from t0, by t1) keeps SACK_GAP_FT clear
	// of his own blockers, if it does not already: up through a gap in the
	// line. Or the clearest of them, none clear.
	private pocketGap(qb: Actor, at: Pt, t0: number, t1: number): Pt {
		const line = this.o.filter(
			(m) =>
				m !== qb &&
				!this.done.has(m.pid) &&
				(OL.has(m.role) || m.role === "TE" || m.role === "TE2"),
		);
		const from = qb.at;
		const s = this.of(at);
		// (On through it as he goes down, too - see SACK_FALL_FT.)
		const room = (p: Pt) => {
			const D = Math.max(0.001, dist(from, p));
			const end = {
				x: p.x + ((p.x - from.x) / D) * SACK_FALL_FT,
				y: p.y + ((p.y - from.y) / D) * SACK_FALL_FT,
			};
			const t2 = t1 + SACK_FALL_MS;
			let near = Infinity;
			for (let c = t0; c <= t2; c += 50) {
				const u = c <= t1 ? (c - t0) / Math.max(1, t1 - t0) : 1;
				const w = c <= t1 ? 0 : (c - t1) / SACK_FALL_MS;
				const q =
					c <= t1
						? { x: from.x + (p.x - from.x) * u, y: from.y + (p.y - from.y) * u }
						: { x: p.x + (end.x - p.x) * w, y: p.y + (end.y - p.y) * w };
				for (const m of line) {
					near = Math.min(near, dist(m.posAt(c), q));
				}
			}
			return near;
		};
		let best = at;
		let most = room(at);
		if (most >= SACK_GAP_FT) {
			return at;
		}
		for (let o = SACK_GAP_STEP; o <= SACK_GAP_MOST; o += SACK_GAP_STEP) {
			for (const side of [1, -1]) {
				const a = s.a + side * o;
				if (a < this.bounds.left + 6 || a > this.bounds.right - 6) {
					continue;
				}
				const p = this.w({ d: s.d, a });
				const r = room(p);
				if (r >= SACK_GAP_FT) {
					return p;
				}
				if (r > most + 0.5) {
					best = p;
					most = r;
				}
			}
		}
		return best;
	}

	// SACKED FROM HIS BLIND SIDE: on his own count, the man who gets him
	// coming from his left (his back to it, set to throw), the way he faces
	// at the last - his eyes out to his right, on a man out there if there
	// is one, side on to him the way he would be to throw it. (None, if
	// not.)
	blindLook(
		qb: Actor,
		sacker: Actor | undefined,
		t: number,
		runners: Actor[],
	): number | undefined {
		if (!sacker || countOf(qb, this.input.key, 140) >= SACK_BLIND) {
			return undefined;
		}
		const off = (y: number) =>
			Math.atan2(Math.sin(y - this.ahead), Math.cos(y - this.ahead));
		const from = off(this.toward(qb.at, sacker.posAt(t - SACK_BLIND_MS)));
		if (from > -SACK_BLIND_FROM) {
			return undefined;
		}
		const ways = runners
			.map((m) => off(this.toward(qb.at, m.posAt(t - SACK_BLIND_MS))))
			.filter((y) => y >= SACK_BLIND_LOOK[0] && y <= SACK_BLIND_LOOK[1]);
		const y =
			ways.length > 0
				? ways[Math.floor(countOf(qb, this.input.key, 141) * ways.length)]!
				: (SACK_BLIND_LOOK[0] + SACK_BLIND_LOOK[1]) / 2;
		return this.ahead + y + SET_OPEN;
	}

	// The way from `from` to `hit` round the shoulder of a man at B, if he is
	// in it (see QB_HIT_ROUND_FT) - the side of him he is on already: its
	// bend (see curve) and how far it is. (Straight, round undefined, if he
	// is not in it; far Infinity if there is no room round him.)
	private roundShoulder(
		from: Pt,
		hit: Pt,
		B: Pt,
	): { far: number; round: Pt | undefined } {
		const D = dist(from, hit);
		if (D <= 0) {
			return { far: D, round: undefined };
		}
		const ux = (hit.x - from.x) / D;
		const uy = (hit.y - from.y) / D;
		const along = (B.x - from.x) * ux + (B.y - from.y) * uy;
		const across = (B.y - from.y) * ux - (B.x - from.x) * uy;
		if (!(along > 0 && along < D && Math.abs(across) < QB_HIT_ROUND_FT)) {
			return { far: D, round: undefined };
		}
		// (No further out than has him come in onto the quarterback near
		// enough the way he would have straight - see QB_HIT_ROUND_END -
		// not swung back in across him at the last.)
		const side = across >= 0 ? -1 : 1;
		const out = Math.min(
			QB_HIT_ROUND_FT - Math.abs(across),
			(Math.tan(QB_HIT_ROUND_END) * (1.5 * D - 2 * along)) / 2,
		);
		// (No room round him: no way in - not through him.)
		if (!(out >= QB_HIT_ROUND_LEAST)) {
			return { far: Infinity, round: undefined };
		}
		const via = {
			x: from.x + ux * along - uy * side * out,
			y: from.y + uy * along + ux * side * out,
		};
		const round = {
			x: 2 * via.x - (from.x + hit.x) / 2,
			y: 2 * via.y - (from.y + hit.y) / 2,
		};
		let far = 0;
		let was = from;
		for (const p of this.curve(from, round, hit, QB_HIT_ROUND_PTS)) {
			far += dist(was, p);
			was = p;
		}
		return { far, round };
	}

	// Off his block round `round` (see curve) to `hit` by t1, as getOff
	// would take him straight: up from QB_HIT_FROM_FPS at GET_OFF_ACCEL, as
	// quick as gets him there then - along the way round.
	private getOffRound(
		m: Actor,
		from: Pt,
		round: Pt,
		hit: Pt,
		t0: number,
		t1: number,
		move?: Move,
	) {
		const path = [from, ...this.curve(from, round, hit, QB_HIT_ROUND_PTS)];
		const lens = [0];
		for (let i = 1; i < path.length; i++) {
			lens.push(lens[i - 1]! + dist(path[i - 1]!, path[i]!));
		}
		const L = lens.at(-1)!;
		const T = (t1 - t0) / 1000;
		const v0 = QB_HIT_FROM_FPS;
		const room = T * T - (2 * (L - v0 * T)) / GET_OFF_ACCEL;
		const a =
			room >= 0 ? GET_OFF_ACCEL : (2 * (L - v0 * T)) / Math.max(T * T, 1e-6);
		const ta = room >= 0 ? T - Math.sqrt(room) : T;
		const gone = (t: number) =>
			L <= v0 * T
				? (L * t) / T
				: t <= ta
					? v0 * t + (a * t * t) / 2
					: v0 * ta + (a * ta * ta) / 2 + (v0 + a * ta) * (t - ta);
		const along = (g: number): Pt => {
			const k = Math.max(
				1,
				lens.findIndex((x) => x >= g),
			);
			const p = path[k - 1]!;
			const q = path[k] ?? p;
			const g0 = lens[k - 1]!;
			const u = lens[k]! > g0 ? clamp((g - g0) / (lens[k]! - g0), 0, 1) : 1;
			return { x: p.x + (q.x - p.x) * u, y: p.y + (q.y - p.y) * u };
		};
		const n = Math.max(2, Math.round((T * 1000) / RAMP_STEP_MS));
		for (let i = 1; i <= n; i++) {
			const t = (T * i) / n;
			m.go(i === n ? hit : along(gone(t)), t0 + t * 1000, move);
		}
	}

	// Whether the way in from `from` (at t0) to `hit` (at t1) - round
	// `round`, if there is one (see curve) - at an even pace, keeps
	// QB_HIT_CLEAR_FT off each of the quarterback's men as he gets to each
	// spot on it, past the first QB_HIT_CLEAR_FROM of it, off the man he
	// was on.
	private clearIn(
		qb: Actor,
		from: Pt,
		t0: number,
		hit: Pt,
		t1: number,
		round: Pt | undefined,
	): boolean {
		const men = this.men.filter((x) => x.team === qb.team && x !== qb);
		const pts = round
			? this.curve(from, round, hit, QB_HIT_ROUND_PTS)
			: Array.from({ length: QB_HIT_ROUND_PTS }, (_, k) => {
					const u = (k + 1) / QB_HIT_ROUND_PTS;
					return {
						x: from.x + (hit.x - from.x) * u,
						y: from.y + (hit.y - from.y) * u,
					};
				});
		let L = 0;
		let was = from;
		const lens = pts.map((p) => {
			L += dist(was, p);
			was = p;
			return L;
		});
		return pts.every((p, k) => {
			if (lens[k]! < QB_HIT_CLEAR_FROM || !(L > 0)) {
				return true;
			}
			const t = t0 + ((t1 - t0) * lens[k]!) / L;
			return !men.some((x) => dist(x.posAt(t), p) < QB_HIT_CLEAR_FT);
		});
	}

	// THE SACK, ANOTHER OF THEM SLIPPED FIRST: now and then (on his own
	// count, with the time for it) one more of the rush off his block and at
	// the quarterback a moment before the man the sim has get him - him out
	// from under it, round on a planted foot or a quick step aside, eyes
	// still downfield, the man left on the turf where he was. Only one
	// coming at him from in front or behind (not straight across), with the
	// man who gets him on the side he gets away to - not back across the
	// man down to him. Gives where he is brought down now (out on that side,
	// clear of the man down), if it is so.
	slipFirst(
		qb: Actor,
		t: number,
		sacker: Actor,
		at: Pt,
		runners: Actor[],
	): Pt | undefined {
		const { snap, events, i0, i1, stats } = this.input;
		const tm =
			t -
			SLIP_BEFORE[0] -
			(SLIP_BEFORE[1] - SLIP_BEFORE[0]) * countOf(qb, this.input.key, 115);
		if (
			countOf(qb, this.input.key, 114) >= SACK_SLIP ||
			qb.t > tm - SLIP_SET_MS
		) {
			return undefined;
		}
		const lame = new Set<number>();
		for (let i = i0; i < i1; i++) {
			const e = events[i];
			if (e?.type === "injury") {
				lame.add(e.injuredPID);
			}
		}
		const named = new Set(
			stats.filter((x) => x.i > this.lo && x.i <= this.hi).map((x) => x.pid),
		);
		const p0 = qb.at;
		const beside = (p: Pt, a: number, by: number) => ({
			x: p.x + Math.cos(a) * by,
			y: p.y + Math.sin(a) * by,
		});
		const pool = this.men.filter(
			(m) =>
				m.team !== qb.team &&
				m !== sacker &&
				!this.done.has(m.pid) &&
				!named.has(m.pid) &&
				!lame.has(m.pid),
		);
		for (const m of pool) {
			// (Off it as late as gets him there in time.)
			let shed: number | undefined;
			let from: Pt | undefined;
			for (
				let x = tm - SLIP_LEAST_MS;
				x >= tm - SLIP_EARLY_MS && x >= snap + RUSH_MOVE_LEAST;
				x -= 50
			) {
				const f = m.posAt(x);
				const far = dist(f, p0) - SLIP_HIT_FT;
				if (legAt(m, x).move !== "engage" || far > SLIP_FAR) {
					break;
				}
				if (
					rampedMs(
						Math.max(0, far),
						m.top * QB_HIT_PACE,
						QB_HIT_FROM_FPS,
						GET_OFF_ACCEL,
					) <=
					tm - x
				) {
					shed = x;
					from = f;
					break;
				}
			}
			if (shed === undefined || !from) {
				continue;
			}
			const face = this.toward(from, p0);
			if (Math.abs(Math.cos(face)) < SLIP_ALONG) {
				continue;
			}
			// (How far to one side of the line of his dive: + its left.)
			const off = (p: Pt) =>
				Math.cos(face) * (p.y - p0.y) - Math.sin(face) * (p.x - p0.x);
			const side = off(sacker.posAt(tm)) >= 0 ? 1 : -1;
			if (off(sacker.posAt(tm)) * side < SLIP_SACKER) {
				continue;
			}
			const away = face + (side * Math.PI) / 2;
			const p1 = beside(p0, away, SLIP_OVER);
			// (Or ducked under him, on his own count: him in at the
			// quarterback high, by him on the other side - over the top of
			// him as he goes down under it.)
			const way = countOf(qb, this.input.key, 116);
			const duck = way < SLIP_DUCK;
			const by = (p: Pt) => (duck ? beside(p, away, -SLIP_DUCK_BY) : p);
			// (Brought down over on that side - moved across the field to
			// it, if it takes that: as far back as the sim has it.)
			const short = SLIP_CLEAR - off(at) * side;
			const down =
				short > 0 ? { x: at.x, y: at.y + (side * short) / Math.cos(face) } : at;
			const a = this.of(down).a;
			if (a < this.bounds.left + 6 || a > this.bounds.right - 6) {
				continue;
			}
			// Him: off his block, in, and off his feet at where the
			// quarterback was.
			const hit = by(beside(p0, face, -SLIP_HIT_FT));
			// (In round the shoulder of the man he was on, if he is in the
			// way - not through him, nor any other of them: another of the
			// rush, if one has a way in clear.)
			const mine = this.men
				.filter(
					(x) =>
						x.team === qb.team && x !== qb && legAt(x, shed).move !== "stand",
				)
				.sort(
					(x, y) => dist(x.posAt(shed), from) - dist(y.posAt(shed), from),
				)[0];
			const inWay =
				mine && dist(mine.posAt(shed), from) <= QB_HIT_BLOCKER_FT
					? this.roundShoulder(from, hit, mine.posAt(shed))
					: { far: dist(from, hit), round: undefined };
			if (
				!Number.isFinite(inWay.far) ||
				!this.clearIn(qb, from, shed, hit, tm + SLIP_DIVE_AFTER, inWay.round)
			) {
				continue;
			}
			const fast = inWay.far / ((tm - shed) / 1000) >= QB_HIT_RUN_FPS;
			m.cut(shed);
			if (inWay.round) {
				this.getOffRound(
					m,
					from,
					inWay.round,
					hit,
					shed,
					tm + SLIP_DIVE_AFTER,
					fast ? "run" : undefined,
				);
			} else {
				m.getOff(
					hit,
					tm + SLIP_DIVE_AFTER,
					QB_HIT_FROM_FPS,
					GET_OFF_ACCEL,
					fast ? "run" : undefined,
				);
			}
			m.lunge(
				"dive",
				tm + SLIP_DIVE_MS,
				face,
				by(beside(p0, face, duck ? SLIP_DUCK_PAST : SLIP_PAST)),
			);
			m.hold(m.t + SLIP_DOWN_MS, "prone");
			m.act("getUp", m.t + GET_UP_MS);
			if (tm - shed >= RUSH_MOVE_AFTER) {
				this.rushMove(m, qb, tm);
			}
			this.done.add(m.pid);
			// (The man he was on: stood up out of his set, round to it.)
			const b = this.men
				.filter(
					(x) =>
						x.team === qb.team &&
						x !== qb &&
						!this.done.has(x.pid) &&
						legAt(x, shed).move === "engage",
				)
				.sort(
					(x, y) => dist(x.posAt(shed), from) - dist(y.posAt(shed), from),
				)[0];
			if (
				b &&
				dist(b.posAt(shed), from) <= QB_HIT_BLOCKER_FT &&
				!pool.some(
					(x) =>
						x !== m && dist(x.posAt(shed), b.posAt(shed)) <= QB_HIT_BLOCKER_FT,
				)
			) {
				b.cut(shed + QB_HIT_SEE_MS);
				b.hold(tm + SLIP_DIVE_MS, "passSet", this.toward(b.at, p0));
				b.hold(t + QB_HIT_STAND_MS, "stand", this.toward(b.at, p0));
			}
			// The quarterback: his look downfield till it comes, and out
			// from under it - then on as before.
			this.scan(qb, undefined, this.ahead, tm - SLIP_SET_MS, runners);
			if (duck) {
				qb.hold(tm - SLIP_DUCK_IN, "pocket", this.ahead);
				qb.act("duck", tm + SLIP_DUCK_MS - SLIP_DUCK_IN, this.ahead);
				qb.go(down, t, undefined, this.ahead);
				return down;
			}
			qb.hold(tm, "pocket", this.ahead);
			if (way < SLIP_DUCK + (1 - SLIP_DUCK) * SLIP_SPIN) {
				qb.act("spin", tm + SPIN_MS, this.ahead, p1);
				qb.turn(-side * Math.PI * 2);
				qb.go(down, t, undefined, this.ahead);
			} else {
				// (Most of the way over, then on round toward where he goes -
				// not stepped square across and turned sharp off it.)
				const v = (SLIP_OVER / SLIP_STEP_MS) * 1000;
				const q1 = beside(p0, away, SLIP_OVER * SLIP_ROUND);
				const q2 = {
					x: p1.x + (down.x - p1.x) * SLIP_ROUND_ON,
					y: p1.y + (down.y - p1.y) * SLIP_ROUND_ON,
				};
				const t1 = tm + SLIP_STEP_MS * SLIP_ROUND;
				const t2 = Math.min(t - 200, t1 + (dist(q1, q2) / v) * 1000);
				qb.go(q1, t1, undefined, this.ahead);
				qb.go(q2, t2, undefined, this.ahead);
				qb.go(down, t, undefined, this.ahead);
			}
			return down;
		}
		return undefined;
	}

	// The man who got home on the quarterback off his block on the way:
	// with the arm on the side of him the blocker is as he goes by, a swim
	// over the top of him or a rip up under his arm (see RUSH_SWIM).
	rushMove(m: Actor, qb: Actor, t: number) {
		const legs = m.legs;
		const k = legs.findLastIndex((l) => l.move === "engage" && l.t1 <= t);
		const next = legs[k + 1];
		if (k < 0 || !next || !GAITS.has(next.move)) {
			return;
		}
		const off = legs[k]!.t1;
		if (off < this.input.snap + RUSH_MOVE_LEAST) {
			return;
		}
		// (No longer than his run on to him lasts, nor over anything else
		// his arms are at.)
		const stop = legs.find((l) => l.t0 >= off && !GAITS.has(l.move))?.t0 ?? t;
		const t0 = off - RUSH_MOVE_BEFORE;
		const t1 = Math.min(off + RUSH_MOVE_AFTER, stop);
		if (
			t1 - t0 < RUSH_MOVE_LEAST_MS ||
			legs.some((l) => l.arms && l.t1 > t0 && l.t0 < t1)
		) {
			return;
		}
		// (The man he beats: the nearest of the other side to him then.)
		const at = m.posAt(off);
		const foe = this.men
			.filter((o) => o.team !== m.team && o !== qb)
			.map((o) => ({ o, p: o.posAt(off) }))
			.sort((a, b) => dist(a.p, at) - dist(b.p, at))[0];
		if (!foe || dist(foe.p, at) > RUSH_MOVE_NEAR) {
			return;
		}
		const way = Math.atan2(next.to.y - next.from.y, next.to.x - next.from.x);
		const left = Math.sin(Math.atan2(foe.p.y - at.y, foe.p.x - at.x) - way) > 0;
		// (Now and then a spin: round on a planted foot off him, his back to
		// him, as far on as he would have gone - with the room to run on
		// after it.)
		const end = off + SPIN_MS;
		const on = m.posAt(end);
		if (
			countOf(m, this.input.key, 45) < RUSH_SPIN &&
			stop - end >= RUSH_SPIN_AFTER &&
			dist(at, on) >= RUSH_SPIN_LEAST_FT
		) {
			m.split(end);
			const i = legs.findIndex((l) => l.t0 >= off);
			const j = legs.findIndex((l) => l.t0 >= end);
			legs.splice(i, j - i, {
				t0: off,
				t1: end,
				from: at,
				to: on,
				move: "spin",
				yaw: way,
				turn: (left ? 1 : -1) * Math.PI * 2,
			});
			return;
		}
		const swim = countOf(m, this.input.key, 38) < RUSH_SWIM;
		m.arms(swim ? (left ? "swimL" : "swimR") : left ? "ripL" : "ripR", t0, t1);
	}

	// (How long it takes him to pull up hard - see STOP_HARD - off the run
	// he is on at t: none off a block, or at no more than a jog.)
	private brakeMs(m: Actor, t: number): number {
		const l = legAt(m, t);
		const v = RAMPS.has(l.move) && !l.arms && !l.turn ? legPace(l) : 0;
		return v < STOP_LEAST ? 0 : (v / (STOP_ACCEL * STOP_HARD)) * 1000;
	}

	// KNOCKED DOWN AT THE LINE: the man in the rush the sim says got to the
	// pass (or, now and then with nobody named on it, one in the way of it
	// anyway) off his block as the quarterback winds up - a step across
	// into the lane, if he needs it - and up off both feet, arms up, as it
	// goes by: off his hands, see incomplete. Only where it goes by him in
	// front of the quarterback, near enough the lane, and no higher than he
	// gets his hands - and with room to go up. (Not a man hurt on it, nor
	// one of them the sim names on anything else.) Or picked off there:
	// `pick`, the man the sim says picks it off - knocked up by him, if he
	// is in the rush, or by one of the others near enough him for it to
	// come down to him (see PICK_BAT_NEAR).
	batAt(
		rushers: Actor[],
		blockers: Actor[],
		qb: Actor,
		catchPt: Pt,
		throwAt: number,
		pick?: Actor,
	): Bat | undefined {
		const { snap, events, i0, i1, stats } = this.input;
		const fly = this.ball.at(-1);
		if (fly?.kind !== "fly" || fly.t0 !== throwAt) {
			return undefined;
		}
		const named = pick
			? rushers.includes(pick)
				? pick
				: undefined
			: this.man(this.statPids("defPssDef")[0]);
		if (named && !rushers.includes(named)) {
			return undefined;
		}
		const lame = new Set<number>();
		for (let i = i0; i < i1; i++) {
			const e = events[i];
			if (e?.type === "injury") {
				lame.add(e.injuredPID);
			}
		}
		const others = new Set(
			stats
				.filter((x) => x.i > this.lo && x.i <= this.hi && x.s !== "defPssDef")
				.map((x) => x.pid),
		);
		if (pick) {
			others.delete(pick.pid);
		}
		const len = dist(fly.from, catchPt);
		if (len <= 0) {
			return undefined;
		}
		const ux = (catchPt.x - fly.from.x) / len;
		const uy = (catchPt.y - fly.from.y) / len;
		// (Where along it he is at t, how far to the side - and the ball as it
		// goes by there: how high, when.)
		const lane = (m: Actor, t: number) => {
			const p = m.posAt(t);
			const dx = p.x - fly.from.x;
			const dy = p.y - fly.from.y;
			const along = dx * ux + dy * uy;
			const side = dx * uy - dy * ux;
			const u = along / len;
			const z =
				fly.from.z + (fly.to.z - fly.from.z) * u + 4 * fly.peak * u * (1 - u);
			const t1 = fly.t0 + u * (fly.t1 - fly.t0);
			// (Up off his feet so his hands are there as it gets there - the
			// rise to them read off the leap's lift, sin-shaped.)
			const need = z - LINE_BAT_HANDS_Z;
			const ph =
				LEAP_UP_FROM +
				(LEAP_UP_SPAN * Math.asin(clamp(need / LEAP_UP, 0.05, 1))) / Math.PI;
			const leap = t1 - ph * LINE_BAT_LEAP_MS;
			const stepFt = Math.max(0, Math.abs(side) - LINE_BAT_SIDE_FT);
			const stepMs =
				stepFt > 0
					? Math.max(LINE_BAT_STEP_MS, (stepFt / LINE_BAT_STEP_FPS) * 1000)
					: 0;
			return { p, along, side, u, z, t1, leap, stepFt, stepMs };
		};
		const most = named ? LINE_BAT_STEP_FT : LINE_BAT_LANE_FT;
		let best:
			| {
					m: Actor;
					shed: number;
					x: ReturnType<typeof lane>;
					brakeFt: number;
			  }
			| undefined;
		for (const m of named ? [named] : rushers.filter((o) => o !== pick)) {
			if (
				this.done.has(m.pid) ||
				lame.has(m.pid) ||
				(!named && others.has(m.pid))
			) {
				continue;
			}
			// (Read where he is as he comes off it - a second look from there.)
			const guess = lane(m, throwAt);
			let x = lane(m, guess.leap - guess.stepMs);
			let shed = x.leap - x.stepMs;
			let brakeFt = 0;
			// (Coming in at a run - free on a blitz, say: pulled up hard on
			// the way he goes first (see brakeMs), read where that stops him
			// - where the run on at that pace has him half the time it takes
			// - and back a look at a time (see LINE_BAT_SEEK_MS) till he is
			// stopped in time to step across and go up: not cut from flat out
			// to a step across in a stride, nor stopped past it and stepped
			// back to it. No further from it than the stop takes him. Slowing
			// into it already - too much so for a stop back along it to have
			// him there in time: as he was, off it as he gets there.)
			if (this.brakeMs(m, shed) > 0) {
				for (let t = shed; t >= snap + RUSH_MOVE_LEAST; t -= LINE_BAT_SEEK_MS) {
					const brake = this.brakeMs(m, t);
					const y = lane(m, t + brake / 2);
					if (t + brake + y.stepMs <= y.leap) {
						x = y;
						shed = t;
						brakeFt = (legPace(legAt(m, t)) * brake) / 2000;
						break;
					}
				}
			}
			if (
				x.along < LINE_BAT_ALONG[0] ||
				x.along > LINE_BAT_ALONG[1] + brakeFt ||
				Math.abs(x.side) > most ||
				x.z > LINE_BAT_TOP_Z ||
				shed < snap + RUSH_MOVE_LEAST ||
				!["engage", ...GAITS].includes(legAt(m, shed).move) ||
				(best && Math.abs(x.side) >= Math.abs(best.x.side))
			) {
				continue;
			}
			best = { m, shed, x, brakeFt };
		}
		if (!best) {
			return undefined;
		}
		// (With nobody named on it, only now and then.)
		if (!named && !pick && countOf(qb, this.input.key, 117) >= LINE_BAT) {
			return undefined;
		}
		const { m, shed, x, brakeFt } = best;
		// (Stood in the lane, his hands just to his side of it.)
		const off =
			Math.sign(x.side) * Math.min(Math.abs(x.side), LINE_BAT_SIDE_FT);
		const lanePt = {
			x: fly.from.x + ux * x.along,
			y: fly.from.y + uy * x.along,
		};
		if (pick && pick !== m && dist(pick.posAt(x.t1), lanePt) > PICK_BAT_NEAR) {
			return undefined;
		}
		const roomAt = (p: Pt) =>
			!this.men.some((o) => o !== m && dist(o.posAt(x.t1), p) < LINE_BAT_ROOM);
		let spot =
			x.stepFt > 0 ? { x: lanePt.x + uy * off, y: lanePt.y - ux * off } : x.p;
		// (Picked off: the far side of it, if there is no room this side -
		// off his block that much sooner for the step further.)
		let off0 = shed;
		if (pick && x.stepFt > 0 && !roomAt(spot)) {
			const far = { x: lanePt.x - uy * off, y: lanePt.y + ux * off };
			const at = shed - ((2 * Math.abs(off)) / LINE_BAT_STEP_FPS) * 1000;
			if (
				at >= snap + RUSH_MOVE_LEAST &&
				["engage", ...GAITS].includes(legAt(m, at).move)
			) {
				spot = far;
				off0 = at;
			}
		}
		if (!roomAt(spot)) {
			return undefined;
		}
		const face = this.toward(spot, fly.from);
		const braking = brakeFt > 0;
		m.cut(off0);
		// (No further than takes him to a stop in the time there is before
		// he goes up.)
		const v = braking ? legPace(m.last) : 0;
		if (v > 0) {
			m.easeOff(
				Math.min(
					(v * v) / (2 * STOP_ACCEL * STOP_HARD),
					(v * Math.max(0, x.leap - m.t)) / 2000,
				),
			);
		}
		if (x.stepFt > 0 && m.t < x.leap - LINE_BAT_STEP_LEAST_MS) {
			m.go(spot, x.leap, "shuffle", face);
		} else {
			m.hold(x.leap, "stand", face);
		}
		m.act("leap", x.leap + LINE_BAT_LEAP_MS, face);
		this.done.add(m.pid);
		// (The man he was on: stood up out of his set where he was, round to
		// it - not if he has another of them on him.)
		const b = blockers
			.filter((o) => !this.done.has(o.pid))
			.sort((o, q) => dist(o.posAt(shed), x.p) - dist(q.posAt(shed), x.p))[0];
		if (
			b &&
			dist(b.posAt(shed), x.p) <= QB_HIT_BLOCKER_FT &&
			!rushers.some(
				(o) =>
					o !== m && dist(o.posAt(shed), b.posAt(shed)) <= QB_HIT_BLOCKER_FT,
			)
		) {
			b.cut(shed + LINE_BAT_SEE_MS);
			b.hold(x.t1, "passSet", this.toward(b.at, spot));
		}
		// (The throw on its way there arched as it was, cut short at him -
		// see the fly's peak.)
		return {
			m,
			at: { ...lanePt, z: x.z },
			t: x.t1,
			peak: fly.peak * x.u * x.u,
		};
	}

	// THE PASSER HIT AS IT LEAVES HIS HAND: now and then (and the man the
	// sim flags for roughing him, always) one of the rush off his block and
	// into the quarterback just after he lets it go - both hands into him and
	// on through it - him knocked off his feet, and up again after; the man
	// he got off turned to watch it. Only off a block he can get off in time
	// - not a man the sim names on anything else in it. (The quarterback the
	// sim says was hurt on it: always, and he stays down.)
	qbHit(rushers: Actor[], blockers: Actor[], qb: Actor, throwAt: number) {
		const { snap, events, i0, i1, stats } = this.input;
		let rough = -1;
		let hurt = false;
		// (Nor a man hurt on it himself: he goes down where it happens to
		// him instead - see hurtDown.)
		const lame = new Set<number>();
		for (let i = i0; i < i1; i++) {
			const e = events[i];
			if (e?.type === "injury") {
				lame.add(e.injuredPID);
			}
			if (e?.type === "injury" && e.injuredPID === qb.pid) {
				hurt = true;
			}
			if (e?.type === "penalty" && e.penaltyName === "Roughing the passer") {
				rough = i;
			}
		}
		const flagged =
			rough >= 0
				? this.man(this.statPidsIn(rough, this.hi, "pen")[0])
				: undefined;
		const named = new Set(
			stats.filter((x) => x.i > this.lo && x.i <= this.hi).map((x) => x.pid),
		);
		const pool =
			flagged && rushers.includes(flagged)
				? [flagged]
				: hurt || countOf(qb, this.input.key, 39) < QB_HIT
					? rushers.filter((m) => !named.has(m.pid))
					: [];
		// (Flagged for it: late, the ball well gone - see fouls.ts.)
		const hitT =
			throwAt +
			(pool[0] === flagged
				? QB_HIT_LATE_MS
				: QB_HIT_AFTER_MS[0] +
					(QB_HIT_AFTER_MS[1] - QB_HIT_AFTER_MS[0]) *
						countOf(qb, this.input.key, 40));
		const q = qb.posAt(hitT);
		// (His way in off his man at `shed`: round the shoulder of the man he
		// was on, if that man is in it - the side he is on of him already
		// (see QB_HIT_ROUND_FT) - and how far it is.)
		const wayIn = (from: Pt, shed: number, rounds: boolean) => {
			const into = this.toward(from, q);
			const hit = {
				x: q.x - Math.cos(into) * QB_HIT_ON,
				y: q.y - Math.sin(into) * QB_HIT_ON,
			};
			const D = dist(from, hit);
			const b = blockers
				.filter((x) => !this.done.has(x.pid))
				.sort(
					(x, y) => dist(x.posAt(shed), from) - dist(y.posAt(shed), from),
				)[0];
			if (
				!rounds ||
				!b ||
				D <= 0 ||
				dist(b.posAt(shed), from) > QB_HIT_BLOCKER_FT
			) {
				return { hit, far: D, round: undefined };
			}
			return { hit, ...this.roundShoulder(from, hit, b.posAt(shed)) };
		};
		// (Nobody else blocking on the way in, as he gets to each spot on it
		// - past the first of it, off his own man: not in through the
		// center beside him, say. See QB_HIT_CLEAR_FT.)
		const clearOf = (
			from: Pt,
			shed: number,
			way: { hit: Pt; far: number; round: Pt | undefined },
		): boolean => {
			if (!Number.isFinite(way.far) || way.far <= 0) {
				return Number.isFinite(way.far);
			}
			const pts = way.round
				? this.curve(from, way.round, way.hit, QB_HIT_ROUND_PTS)
				: Array.from({ length: QB_HIT_ROUND_PTS }, (_, k) => {
						const u = (k + 1) / QB_HIT_ROUND_PTS;
						return {
							x: from.x + (way.hit.x - from.x) * u,
							y: from.y + (way.hit.y - from.y) * u,
						};
					});
			let gone = 0;
			let was = from;
			for (const p of pts) {
				gone += dist(was, p);
				was = p;
				if (gone < QB_HIT_CLEAR_FROM) {
					continue;
				}
				const t = shed + (hitT - shed) * Math.min(1, gone / way.far);
				if (blockers.some((x) => dist(x.posAt(t), p) < QB_HIT_CLEAR_FT)) {
					return false;
				}
			}
			return true;
		};
		// (Straight in through him only if nobody can get round him in time
		// - the man the sim flags for roughing the passer, say, or the hit
		// that hurts him. A hit of our own, nobody - not through the man in
		// the way.)
		let best: { m: Actor; shed: number; from: Pt; rounds: boolean } | undefined;
		for (const rounds of (flagged && pool[0] === flagged) || hurt
			? [true, false]
			: [true]) {
			for (const m of pool) {
				if (this.done.has(m.pid) || lame.has(m.pid)) {
					continue;
				}
				// (Off it as late as gets him there in time.)
				for (
					let shed = hitT - QB_HIT_LEAST_MS;
					shed >= throwAt - QB_HIT_EARLY_MS &&
					shed >= snap + RUSH_MOVE_LEAST &&
					(!best || shed > best.shed);
					shed -= 50
				) {
					const from = m.posAt(shed);
					if (
						legAt(m, shed).move !== "engage" ||
						dist(from, q) - QB_HIT_ON > QB_HIT_FAR
					) {
						break;
					}
					const way = wayIn(from, shed, rounds);
					const { far } = way;
					if (rounds && !clearOf(from, shed, way)) {
						continue;
					}
					if (
						rampedMs(
							Math.max(0, far),
							m.top * QB_HIT_PACE,
							QB_HIT_FROM_FPS,
							GET_OFF_ACCEL,
						) <=
						hitT - shed
					) {
						best = { m, shed, from, rounds };
						break;
					}
				}
			}
			if (best) {
				break;
			}
		}
		if (!best) {
			return;
		}
		const { m, shed, from } = best;
		const into = this.toward(from, q);
		const at = (p: Pt, by: number) => ({
			x: p.x + Math.cos(into) * by,
			y: p.y + Math.sin(into) * by,
		});
		const way = wayIn(from, shed, best.rounds);
		const { hit, round } = way;
		const down = at(q, QB_HIT_BACK_FT);
		const fast = way.far / ((hitT - shed) / 1000) >= QB_HIT_RUN_FPS;
		m.cut(shed);
		if (round) {
			this.getOffRound(
				m,
				from,
				round,
				hit,
				shed,
				hitT,
				fast ? "run" : undefined,
			);
		} else {
			m.getOff(
				hit,
				hitT,
				QB_HIT_FROM_FPS,
				GET_OFF_ACCEL,
				fast ? "run" : undefined,
			);
		}
		// (On through him the way he came in on him - round the man he was
		// on, that is the way the last of it had him going.)
		const l = m.last;
		const came =
			round && dist(l.from, l.to) > 0
				? Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x)
				: into;
		const peel =
			came +
			QB_HIT_PEEL *
				(Math.sin(came + QB_HIT_PEEL) <= Math.sin(came - QB_HIT_PEEL) ? 1 : -1);
		this.hitThrough(m, came, peel, hitT);
		m.hold(hitT + QB_HIT_STAND_MS, "stand", this.toward(m.at, down));
		if (hitT - shed >= RUSH_MOVE_AFTER + QB_HIT_SHOVE_MS[0]) {
			this.rushMove(m, qb, hitT);
		}
		m.arms("shove", hitT - QB_HIT_SHOVE_MS[0], hitT + QB_HIT_SHOVE_MS[1]);
		this.done.add(m.pid);
		// (The man he was on: stood up out of his set where he was, round to
		// it - not if he has another of them on him.)
		const b = blockers
			.filter((x) => !this.done.has(x.pid))
			.sort((x, y) => dist(x.posAt(shed), from) - dist(y.posAt(shed), from))[0];
		const beaten =
			b &&
			dist(b.posAt(shed), from) <= QB_HIT_BLOCKER_FT &&
			!rushers.some(
				(x) =>
					x !== m && dist(x.posAt(shed), b.posAt(shed)) <= QB_HIT_BLOCKER_FT,
			)
				? b
				: undefined;
		if (beaten) {
			beaten.cut(shed + QB_HIT_SEE_MS);
			beaten.hold(hitT, "passSet", this.toward(beaten.at, q));
			beaten.hold(
				hitT + QB_HIT_STAND_MS,
				"stand",
				this.toward(beaten.at, down),
			);
		}
		// (Him: put down there a moment, and up - on his back, facing the
		// man who hit him, or on his front, hit from behind: whichever has
		// him turned the less from the way he faced - on his back, hit from
		// the side.)
		qb.cut(hitT);
		const turned = (a: number) =>
			Math.abs(Math.atan2(Math.sin(a - qb.yaw), Math.cos(a - qb.yaw)));
		const back = turned(into + Math.PI) <= turned(into) + QB_HIT_BACK_BIAS;
		qb.act(
			back ? "tackledBack" : "tackled",
			hitT + QB_HIT_FALL_MS,
			back ? into + Math.PI : into,
			down,
		);
		const lie = back ? "supine" : "prone";
		// (Hurt on it: down there for good - see hurtDown.)
		if (hurt) {
			qb.hold(qb.t + QB_HURT_DOWN_MS, lie);
			return;
		}
		const downAt = qb.t;
		// (A hand up, now and then: from the man who let him through, or
		// another of his line free near him, or the man who put him down -
		// over beside his chest, from round the side he is on, not over him;
		// him down till he gets there.)
		const leave = hitT + QB_HIT_THROUGH_MS + QB_HAND_SEE_MS;
		const ux = Math.cos(into);
		const uy = Math.sin(into);
		const chest = at(down, HELP_CHEST);
		const clear = (x: Actor) => {
			for (let t = leave; t <= leave + QB_HAND_WAIT_MS; t += 250) {
				const xp = x.posAt(t);
				if (
					rushers.some(
						(o) => o !== m && dist(o.posAt(t), xp) <= QB_HIT_BLOCKER_FT,
					)
				) {
					return false;
				}
			}
			return !DOWN.has(legAt(x, leave).move);
		};
		const mate =
			beaten ??
			blockers
				.filter(
					(x) =>
						!this.done.has(x.pid) &&
						dist(x.posAt(leave), chest) <= QB_HAND_MATE_FT &&
						clear(x),
				)
				.sort(
					(x, y) => dist(x.posAt(leave), chest) - dist(y.posAt(leave), chest),
				)[0];
		const helper =
			countOf(qb, this.input.key, 42) >= QB_HAND_UP
				? undefined
				: countOf(qb, this.input.key, 43) < HELP_FOE || !mate
					? m
					: mate;
		const was = helper?.posAt(leave) ?? chest;
		const near = (side: number) => ({
			x: chest.x - uy * side * HELP_BESIDE,
			y: chest.y + ux * side * HELP_BESIDE,
		});
		const roomAt = (q: Pt) =>
			Math.min(
				...this.men
					.filter((x) => x !== qb && x !== helper)
					.map((x) => dist(x.posAt(leave + QB_HAND_WAIT_MS / 2), q)),
			);
		const own = (was.x - chest.x) * -uy + (was.y - chest.y) * ux >= 0 ? 1 : -1;
		// (Where his hand held up meets the other's - see handUp - from the
		// side he is on of him, or the other, a man stood there; no room
		// for that, just over beside his chest, his hand down to him: the
		// side he is on, or, a man stood there, the other, if that is
		// clearer - not over onto him to do it.)
		const lies = back ? into + Math.PI : into;
		const handAt = (sd: number) => {
			const b = near(sd);
			const left =
				(b.x - down.x) * -Math.sin(lies) + (b.y - down.y) * Math.cos(lies) < 0;
			return { ...this.handUp(down, lies, back, b, left), side: sd };
		};
		const hand = [handAt(own), handAt(-own)].find(
			(h) => roomAt(h.at) >= HELP_ROOM,
		);
		const side =
			hand?.side ??
			(roomAt(near(own)) < HELP_ROOM && roomAt(near(-own)) > roomAt(near(own))
				? -own
				: own);
		const by = (p: Pt) => ({
			x: p.x - uy * side * HELP_BESIDE,
			y: p.y + ux * side * HELP_BESIDE,
		});
		const spot = hand?.at ?? by(chest);
		const handFace = hand?.face ?? this.toward(spot, chest);
		// (Out round his feet first, if he comes from past them - out past
		// where he stands for it, to come in to it facing the hand.)
		const route =
			(was.x - down.x) * ux + (was.y - down.y) * uy < 0
				? [
						hand
							? {
									x: spot.x + Math.cos(hand.from) * HELP_CHEST,
									y: spot.y + Math.sin(hand.from) * HELP_CHEST,
								}
							: by(down),
						spot,
					]
				: [spot];
		let far = 0;
		let p = was;
		for (const x of route) {
			far += dist(p, x);
			p = x;
		}
		// (Turned round first where he stands, him behind where he faces -
		// not walked off backward at him as he comes round.)
		const l0 = helper && legAt(helper, leave);
		const y0 = l0
			? (l0.yaw ?? Math.atan2(l0.to.y - l0.from.y, l0.to.x - l0.from.x))
			: 0;
		const w0 = Math.atan2(route[0]!.y - was.y, route[0]!.x - was.x);
		const pivot =
			l0 && far >= HELP_TURN_FT && Math.cos(w0 - y0) < 0 ? HELP_PIVOT_MS : 0;
		const arrive =
			leave + pivot + Math.max(HELP_STEP_MS, (far / HELP_FPS) * 1000);
		const upAt = Math.max(downAt + QB_HIT_DOWN_MS, arrive + 80);
		const helped = helper && upAt - downAt <= QB_HAND_WAIT_MS;
		qb.hold(helped ? upAt : downAt + QB_HIT_DOWN_MS, lie);
		qb.act(back ? "getUpBack" : "getUp", qb.t + GET_UP_MS);
		if (!helped) {
			return;
		}
		helper.cut(leave);
		helper.hold(leave);
		if (pivot > 0) {
			helper.hold(leave + pivot, "stand", w0);
		}
		let t = leave + pivot;
		p = helper.at;
		for (const x of route) {
			t += ((arrive - leave - pivot) * dist(p, x)) / Math.max(0.01, far);
			// (Turned to him on his way over - or, him behind the way he
			// goes, the way he goes: not walked over backward.)
			const face = x === spot ? handFace : this.toward(x, chest);
			const way = Math.atan2(x.y - p.y, x.x - p.x);
			helper.go(
				x,
				t,
				undefined,
				Math.cos(face - way) >= 0 || dist(p, x) < HELP_TURN_FT
					? face
					: undefined,
			);
			p = x;
		}
		helper.hold(upAt - 80);
		helper.act("helpUp", upAt + GET_UP_MS, handFace);
		if (hand) {
			qb.arms(handUpOf(back, hand.left), upAt, upAt + GET_UP_MS);
		}
	}

	scramble(i: number, qb: Actor, go: number, runners: Actor[] = []) {
		const r = this.r;
		const e = this.ev(i);
		const yds = typeof e.yds === "number" ? e.yds : 0;
		this.scan(qb, undefined, this.ahead, go, runners);
		qb.hold(go, "pocket", this.ahead);
		const ta = this.tacklerA(qb, go);
		const side =
			ta !== undefined && r() < 0.75 ? Math.sign(ta) || 1 : r() < 0.5 ? -1 : 1;
		// Out of the pocket to one side, then up the field.
		const outPt = this.w({
			d: this.of(qb.at).d + 2,
			a: side * (14 + r() * 10),
		});
		qb.run(outPt, 20);
		const end = this.endOf(
			yds,
			side * (18 + r() * 20),
			e.td === true,
			e.safety === true,
		);
		const t = qb.run(end.pt, qb.top * 0.92, "sprint");
		this.endCarry(i, qb, end, t, this.d);
	}

	// Stripped in the pocket: the ball out of his hands as he is hit, where
	// the sim says, and loose behind the line.
	strip(i: number, qb: Actor, t: number, runners: Actor[] = []) {
		const r = this.r;
		const recI = this.find("fumbleRecovery", i);
		const before = recI >= 0 ? this.ev(recI).ydsBefore : undefined;
		const at =
			typeof before === "number"
				? this.w({
						d: clamp(before * YD, this.ownGoalD - 20, -3),
						a: clamp(
							this.of(qb.at).a + (r() - 0.5) * 10,
							this.bounds.left + 6,
							this.bounds.right - 6,
						),
					})
				: qb.at;
		const forcer = this.man(this.statPids("defFmbFrc")[0]);
		const step = (dist(qb.at, at) / (qb.top * 0.6)) * 1000;
		const hold = Math.max(qb.t, t - Math.max(800, step));
		this.scan(qb, undefined, this.ahead, hold, runners);
		qb.hold(hold, "pocket", this.ahead);
		// (Eyes still downfield, as on a sack.)
		qb.go(at, t, undefined, this.ahead);
		const down = this.downBy(qb, at, t, this.d, forcer ? [forcer] : undefined);
		this.at.set(i, down.t);
		this.whistle = down.t;
		this.fumbled(i, qb, at, down.t, down.by, this.d);
		if (down.by[0]) {
			this.rushMove(down.by[0], qb, down.t);
		}
	}

	// Hit at `pt` at t, and the ball out: the men on him (the one the sim
	// says knocked it loose in on it first) taking him down - or, if he is
	// the one the sim says comes up with it, only knocking it out - and
	// everybody after it (see loose).
	fumbled(
		fumI: number,
		carrier: Actor,
		pt: Pt,
		t: number,
		by: Actor[],
		chasers: Actor[],
	) {
		const { events } = this.input;
		const recI = this.find("fumbleRecovery", fumI);
		const rec =
			recI >= 0
				? this.man(this.statPidsIn(fumI, recI, "defFmbRec")[0])
				: undefined;
		let before = fumI - 1;
		while (before > this.input.i0 && events[before]?.type === "stat") {
			before -= 1;
		}
		const forcer = this.man(this.statPidsIn(before, fumI, "defFmbFrc")[0]);
		let hitters = by.filter((m) => m !== rec);
		if (
			forcer &&
			forcer !== rec &&
			forcer.team !== carrier.team &&
			!this.done.has(forcer.pid) &&
			(hitters.includes(forcer) ||
				this.canReach(forcer, pt) <= t - HIT_MS + SLACK_MS)
		) {
			hitters = [forcer, ...hitters.filter((m) => m !== forcer)].slice(0, 2);
		}
		this.tackle(
			carrier,
			hitters,
			pt,
			t,
			rec === carrier || hitters.length === 0,
		);
		this.pursue(
			chasers.filter((m) => m !== rec),
			pt,
			t,
			hitters,
			carrier,
		);
		this.loose(fumI, recI, rec, carrier, t + POP_MS);
	}

	// The ball loose from the man who had it, at t: off the turf, and the
	// recovery the sim says - by `rec`, going for it from wherever he is
	// (up off the turf first, if he is down): fallen on where its yards
	// leave it, the men near piling in, or picked up on the run and taken
	// there, brought down (or into the end zone) - and, if the sim says so,
	// out again. On the way, a ball he has a long way to go for is batted
	// about by the men nearer it.
	loose(
		fumI: number,
		recI: number,
		rec: Actor | undefined,
		from: Actor,
		t: number,
	) {
		const r = this.r;
		const pt = from.posAt(t);
		this.at.set(fumI, t);
		this.seen = Math.max(this.seen, t + BREAK_MS);
		if (recI < 0) {
			this.ball.push({ kind: "spot", t0: t, at: { ...pt, z: 0 } });
			this.whistle = Math.max(this.whistle, t + 1500);
			return;
		}
		const e = this.ev(recI);
		const yds = typeof e.yds === "number" ? e.yds : 0;
		// Whose it is, and which way (in the offense's frame) they go with
		// it.
		const side = (1 - (e.t === 1 ? 1 : 0)) as Side;
		const way = side === this.input.offense ? 1 : -1;
		const spot = this.of(pt);
		// Where it ends up: its yards on from the spot - a score, in the end
		// zone it goes for; a touchback or a safety, down in its own.
		const own = way === 1 ? this.ownGoalD : this.goalD;
		const theirs = way === 1 ? this.goalD : this.ownGoalD;
		let endD = spot.d + way * yds * YD;
		if (e.td === true) {
			endD = theirs + way * (2 + r() * 9);
		} else if (e.safety === true || e.touchback === true) {
			endD = own - way * (2 + r() * 6);
		}
		endD = clamp(endD, this.ownGoalD - 27, this.goalD + 27);
		const gain = (endD - spot.d) * way;
		const again = this.fumbleAfter(recI);
		// Picked up and run with (to be hit and lose it again, too) - or
		// fallen on.
		const scoop = gain > 9 || again >= 0;
		// Where it gets to: up the field as far as it goes, or, picked up,
		// a little way - and across, his way.
		let a = spot.a + (r() - 0.5) * 10;
		if (rec) {
			a += clamp((this.of(rec.posAt(t)).a - a) * 0.5, -15, 15);
		}
		a = clamp(a, this.bounds.left + 3, this.bounds.right - 3);
		const restD = !scoop
			? endD
			: gain > 9
				? spot.d + way * clamp(gain * 0.3, 3, 9)
				: endD - way * 3;
		// (Fallen on by a man all but on it already: rolled off to his side
		// far enough for him to go down on it - not under his feet, for him
		// to dive back off it.)
		if (rec && !scoop) {
			const near = this.of(rec.posAt(t + 150));
			const room = Math.hypot(PRONE_HANDS.f, PRONE_HANDS.s) + FALL_ON_ROOM;
			const dd = Math.abs(restD - near.d);
			if (dd < room && Math.hypot(restD - near.d, a - near.a) < room) {
				const s = a >= near.a ? 1 : -1;
				a = clamp(
					near.a + s * Math.sqrt(room * room - dd * dd),
					this.bounds.left + 3,
					this.bounds.right - 3,
				);
			}
		}
		const restPt = this.w({ d: restD, a });
		// (Loose long enough to be called loose before anyone has it - the
		// line before it said just before.)
		const restT = t + clamp((dist(pt, restPt) / 15) * 1000, 600, 1500);
		const others = this.men.filter((m) => m !== rec);
		if (!rec) {
			this.hops({ ...pt, z: 3 }, { ...restPt, z: 0 }, t, restT);
			this.ball.push({ kind: "spot", t0: restT, at: { ...restPt, z: 0 } });
			this.whistle = restT + 1200;
			this.at.set(recI, this.whistle);
			return;
		}

		// After it.
		let go = t + 150;
		for (const l of rec.legs) {
			if (l.t1 > go && DOWN.has(l.move)) {
				go = l.t1;
			}
		}
		rec.cut(go);
		rec.hold(go);
		// (Going the other way: planted for it first - see plantFor. Up to
		// speed for it off what he was doing - out of his stance, from
		// nothing.)
		rec.plantFor(restPt);
		const into = SET.has(rec.last.move)
			? undefined
			: paceOn(rec.last, rec.at, restPt);
		// (Fallen on: there once he is where he dives on it from - FALL_ON_FT
		// short of where he comes down - or at once, nearer it than that.)
		const runFt = scoop
			? dist(rec.at, restPt)
			: Math.max(
					0,
					dist(rec.at, fallOn(restPt, this.toward(rec.at, restPt))) -
						FALL_ON_FT,
				);
		const reach =
			rec.t +
			rampedMs(
				runFt,
				rec.top * 0.92,
				into ?? 0,
				into === undefined ? RELEASE_ACCEL : RAMP_ACCEL,
			);
		// (Picked up: where he goes with it - and, that a sharp turn off the
		// way he comes at it, slowed into it as he gets to it, down to
		// SCOOP_TURN_FPS - the time that takes.)
		const endPt = scoop
			? this.w({
					d: endD,
					a: clamp(
						a + (r() - 0.5) * 16,
						this.bounds.left + 6,
						this.bounds.right - 6,
					),
				})
			: restPt;
		const turning =
			scoop &&
			dist(rec.at, restPt) > SCOOP_REACH &&
			Math.abs(
				angleOff(this.toward(restPt, endPt) - this.toward(rec.at, restPt)),
			) > SCOOP_TURN;
		const takeT = scoop
			? Math.max(
					restT,
					reach +
						(turning
							? ((rec.top * 0.92 - SCOOP_TURN_FPS) /
									(2 * STOP_ACCEL * STOP_HARD)) *
								1000
							: 0),
				)
			: Math.max(restT + 100, reach + 350);
		this.gather(
			others.filter((m) => m !== from),
			restPt,
			t,
			scoop ? takeT : takeT + GATHER_FALLEN_MS,
		);
		// It stops (or is in his hands) - batted on by the first men at it,
		// if he has a long way to go for it, one after another.
		const upTo = scoop ? takeT : takeT - 300;
		const batted = upTo - restT > 700;
		const stopT = batted ? upTo : restT;
		const stopZ = scoop && takeT - stopT < 50 ? 1.2 : 0;
		// (Fallen on: the men in on the pile - see batIt.)
		let piled: Actor[] = [];
		// How near it comes to anybody on its way from `p0` (at t0) to `q`
		// (at tq), and on from there to where it stops - not the man it
		// comes off (`skip`), on its way out of his hands, nor the man it
		// goes to.
		const clearOf = (
			p0: Pt,
			t0: number,
			q: Pt,
			tq: number,
			skip: Actor | undefined,
		) => {
			let near = Infinity;
			for (let at = t0; at <= stopT; at += BAT_EVERY_MS) {
				const first = at < tq;
				const u = first
					? (at - t0) / Math.max(1, tq - t0)
					: (at - tq) / Math.max(1, stopT - tq);
				const [a0, a1] = first ? [p0, q] : [q, restPt];
				const ball = {
					x: a0.x + (a1.x - a0.x) * u,
					y: a0.y + (a1.y - a0.y) * u,
				};
				for (const m of this.men) {
					if (m !== rec && !(first && m === skip)) {
						near = Math.min(near, this.nearOf(m, at, ball));
					}
				}
			}
			return near;
		};
		// How `m` would go for it at `q`, from `since`: off his feet at it
		// from where he is, his hands onto it as he comes down - no sooner
		// than `soonest`, no later than `latest`; not from behind it (`inWay`,
		// the way it comes), nor knocking it on back into him (`on`).
		const planBat = (
			m: Actor,
			q: Pt,
			since: number,
			soonest: number,
			latest: number,
			inWay: number,
			on: number,
		) => {
			// (Going the other way: planted for it first - see plantFor -
			// and off for it from there.)
			const was = m.posAt(since);
			const l = m.legs.findLast((x) => x.t0 < since && x.t1 > x.t0);
			const plant = l ? plantOn(l, was, q) : { T: 0, on: 0, ve: 0 };
			const along = l ? dist(l.from, l.to) : 0;
			const set =
				l && along > 0
					? {
							x: was.x + ((l.to.x - l.from.x) / along) * plant.on,
							y: was.y + ((l.to.y - l.from.y) / along) * plant.on,
						}
					: was;
			const yaw = this.toward(set, q);
			if (
				dist(set, q) < BAT_ROOM ||
				Math.cos(yaw - inWay) > BAT_IN ||
				Math.cos(yaw - on) < BAT_BACK
			) {
				return undefined;
			}
			const lands = {
				x: q.x - Math.cos(yaw) * BAT_HANDS.f,
				y: q.y - Math.sin(yaw) * BAT_HANDS.f,
			};
			const off = Math.min(3.5, dist(set, lands));
			const short = {
				x: lands.x - Math.cos(yaw) * off,
				y: lands.y - Math.sin(yaw) * off,
			};
			// (Up to speed from what he was doing, a few strides of it - or,
			// hardly any way to go, laid out from where he is.)
			const run = dist(set, short);
			const v = l ? legPace(l) : 0;
			const v0 =
				l && RAMPS.has(l.move) && v > 0
					? (paceOn(l, set, short) * plant.ve) / v
					: 0;
			const A = v0 > 0 ? RAMP_ACCEL : RELEASE_ACCEL;
			const at = Math.max(
				soonest,
				since +
					plant.T * 1000 +
					(run > BAT_RUN_LEAST_FT
						? rampedMs(run, m.top * FLAT_OUT, v0, A)
						: 0) +
					300,
			);
			if (at > latest) {
				return undefined;
			}
			// (Down where nobody else is about to be, the length of him - the
			// man it goes to, on his way in to it, too.)
			const ux = Math.cos(yaw);
			const uy = Math.sin(yaw);
			const up = at + BAT_DOWN_MS + GET_UP_MS / 2;
			for (let tt = at - 200; tt <= up; tt += BAT_EVERY_MS) {
				for (const o of this.men) {
					if (o === m || (o === rec && tt >= takeT - BAT_ON_MS)) {
						continue;
					}
					const p = o.posAt(tt);
					const k = clamp(
						(p.x - lands.x) * ux + (p.y - lands.y) * uy,
						-LIE_BACK,
						LIE_HEAD,
					);
					if (
						Math.hypot(p.x - lands.x - ux * k, p.y - lands.y - uy * k) <
						BAT_LIE_CLEAR
					) {
						return undefined;
					}
				}
			}
			return { m, at, yaw, off, run, short, lands, A, since };
		};
		// (The soonest of them at it.)
		const soonestOf = <P extends { at: number }>(plans: P[]) =>
			plans.reduce<P | undefined>(
				(best, x) => (!best || x.at < best.at ? x : best),
				undefined,
			);
		// Off his feet at it, down after it, and up again.
		const doBat = (p: NonNullable<ReturnType<typeof planBat>>, q: Pt) => {
			const { m, at, yaw, off, run, short, lands, A, since } = p;
			m.cut(since);
			m.hold(since);
			m.plantFor(q);
			if (run > BAT_RUN_LEAST_FT && at - 300 > m.t) {
				const l = m.last;
				m.getOff(
					short,
					at - 300,
					RAMPS.has(l.move) ? paceOn(l, m.at, short) : 0,
					A,
					"sprint",
				);
			} else if (off > TURN_IN_FT) {
				m.hold(at - 300);
			} else {
				m.hold(at - 300, undefined, yaw);
			}
			m.lunge("dive", at, yaw, lands);
			m.hold(at + BAT_DOWN_MS, "prone");
			m.act("getUp", at + BAT_DOWN_MS + GET_UP_MS);
			this.done.add(m.pid);
		};
		// (Once everybody else going for it is set off: see batIt.)
		const batIt = () => {
			// (Its way: out of his hands, off each man who gets a hand to it,
			// on to where it stops.)
			const way: { at: Pt3; t: number }[] = [{ at: { ...pt, z: 3 }, t }];
			if (batted) {
				const third = this.w({
					d: spot.d + (restD - spot.d) * 0.35 + (r() - 0.5) * 4,
					a: spot.a + (a - spot.a) * 0.35 + (r() - 0.5) * 6,
				});
				const timeTo = (q: Pt) =>
					t + clamp((dist(pt, q) / 15) * 1000, 350, 900);
				const toRest = this.toward(pt, restPt);
				const tries = [
					...BAT_TURNS.map((turn) => {
						const c = Math.cos(turn);
						const sn = Math.sin(turn);
						const dx = third.x - pt.x;
						const dy = third.y - pt.y;
						return { x: pt.x + dx * c - dy * sn, y: pt.y + dx * sn + dy * c };
					}),
					...BAT_RING.flatMap((far) =>
						BAT_RING_TURNS.map((turn) => ({
							x: pt.x + Math.cos(toRest + turn) * far,
							y: pt.y + Math.sin(toRest + turn) * far,
						})),
					),
				];
				let bat = third;
				let widest = -1;
				for (const p of tries) {
					const q = { x: p.x, y: clamp(p.y, 2, FIELD_W - 2) };
					const near = clearOf(pt, t, q, timeTo(q), from);
					if (near > widest) {
						widest = near;
						bat = q;
					}
					if (near >= BAT_CLEAR) {
						break;
					}
				}
				let batT = timeTo(bat);
				// How each of the men nearest it would go for it.
				const inWay = this.toward(pt, bat);
				const on = this.toward(bat, restPt);
				const first = soonestOf(
					this.nearest(
						others.filter((m) => m !== from),
						bat,
						BAT_FROM,
					).flatMap(
						(m) =>
							planBat(
								m,
								bat,
								this.seen,
								batT,
								Math.min(t + BAT_LATEST_MS, stopT - BAT_ON_MS),
								inWay,
								on,
							) ?? [],
					),
				);
				if (first) {
					batT = first.at;
					doBat(first, bat);
				}
				way.push({ at: { ...bat, z: 0.5 }, t: batT });
				// And, a long way still for him to come for it, out of that
				// man's hands and batted on again by the next of them (the men
				// stood round where it ends up, as well).
				const used = new Set<Actor>([from, rec]);
				let by = first?.m;
				let prev = bat;
				let prevT = batT;
				for (let k = 1; k < BAT_MOST && stopT - prevT >= BAT_AGAIN_MS; k++) {
					if (by) {
						used.add(by);
					}
					const since = prevT + BAT_REACT_MS;
					const men = this.men.filter((m) => {
						if (used.has(m) || (this.done.has(m.pid) && !piled.includes(m))) {
							return false;
						}
						const l = legAt(m, since);
						return !DOWN.has(l.move) && moveKind(l.move) !== "act";
					});
					const toIt = this.toward(prev, restPt);
					let best:
						| { q: Pt; p: NonNullable<ReturnType<typeof planBat>> }
						| undefined;
					let clearest = -1;
					for (const far of BAT_RING) {
						for (const turn of BAT_RING_TURNS) {
							const q = {
								x: prev.x + Math.cos(toIt + turn) * far,
								y: clamp(prev.y + Math.sin(toIt + turn) * far, 2, FIELD_W - 2),
							};
							const soonest =
								prevT +
								Math.max(
									BAT_GAP_MS,
									(dist(prev, q) / BAT_SQUIRT_FPS) * 1000,
									(stopT - prevT) / (BAT_MOST - k + 1),
								);
							const p = soonestOf(
								men
									.map((m) => ({ m, d: dist(m.posAt(since), q) }))
									.sort((x, y) => x.d - y.d)
									.slice(0, BAT_FROM)
									.flatMap(
										({ m }) =>
											planBat(
												m,
												q,
												since,
												soonest,
												Math.min(prevT + BAT_GAP_MOST_MS, stopT - BAT_ON_MS),
												this.toward(prev, q),
												this.toward(q, restPt),
											) ?? [],
									),
							);
							if (!p) {
								continue;
							}
							const near = clearOf(prev, prevT, q, p.at, by);
							if (near > clearest) {
								clearest = near;
								best = { q, p };
							}
							if (near >= BAT_CLEAR) {
								break;
							}
						}
						if (clearest >= BAT_CLEAR) {
							break;
						}
					}
					if (!best) {
						break;
					}
					doBat(best.p, best.q);
					by = best.p.m;
					prev = best.q;
					prevT = best.p.at;
					way.push({ at: { ...prev, z: 0.5 }, t: prevT });
				}
			}
			// (On to where it stops - over and over in a roll, then lying
			// there, if he is a while yet.)
			const last = way.at(-1)!;
			const end =
				stopZ > 0 || !batted
					? stopT
					: Math.min(
							stopT,
							last.t +
								clamp(
									BAT_ROLL_MS[0] + dist(last.at, restPt) * BAT_ROLL_MS[1],
									BAT_ROLL_MS[0],
									BAT_ROLL_MS[2],
								),
						);
			way.push({ at: { ...restPt, z: stopZ }, t: end });
			for (let k = 1; k < way.length; k++) {
				this.hops(way[k - 1]!.at, way[k]!.at, way[k - 1]!.t, way[k]!.t);
			}
			if (takeT > end + 1) {
				this.ball.push({ kind: "spot", t0: end, at: { ...restPt, z: 0 } });
			}
			this.ball.push({ kind: "held", t0: takeT, pid: rec.pid });
		};
		const yaw = this.toward(rec.at, restPt);

		if (!scoop) {
			// Fallen on, and everybody near in on top.
			const lands = fallOn(restPt, yaw);
			if (runFt > 0) {
				const short = {
					x: lands.x - Math.cos(yaw) * FALL_ON_FT,
					y: lands.y - Math.sin(yaw) * FALL_ON_FT,
				};
				rec.press(short, takeT - 350, rec.top * FLAT_OUT, "sprint");
			} else if (takeT - 350 > rec.t) {
				rec.hold(takeT - 350, undefined, yaw);
			}
			rec.lunge("dive", takeT, yaw, lands);
			rec.hold(takeT + 1400, "prone");
			rec.act("getUp", takeT + 1400 + GET_UP_MS);
			this.done.add(rec.pid);
			this.whistle = takeT + 500;
			this.at.set(recI, takeT + 200);
			const touched = this.statPidsIn(fumI, recI, "defTckSolo", "defTckAst")
				.map((pid) => this.man(pid))
				.filter(
					(m): m is Actor =>
						!!m && m.team !== rec.team && !this.done.has(m.pid),
				);
			const pile = [
				...touched,
				...this.nearest(
					others.filter((m) => !touched.includes(m)),
					restPt,
					6 - touched.length,
				),
			];
			for (const m of pile) {
				m.cut(this.seen);
				m.hold(this.seen);
			}
			this.pileOn(pile, restPt, takeT);
			piled = pile;
			batIt();
			return;
		}

		// Picked up on the run, and away with it: down to it as he gets there,
		// his hands on it (SCOOP_REACH out in front of him, SCOOP_LOW through
		// the scoop), and up and off with it.
		const far = dist(rec.at, restPt);
		const low =
			far > SCOOP_REACH
				? {
						x: restPt.x - ((restPt.x - rec.at.x) / far) * SCOOP_REACH,
						y: restPt.y - ((restPt.y - rec.at.y) / far) * SCOOP_REACH,
					}
				: rec.at;
		const off = rec.t;
		rec.press(low, takeT, rec.top * FLAT_OUT, "sprint");
		if (turning) {
			const v = legPace(rec.last);
			if (v > SCOOP_TURN_FPS) {
				rec.breakDown(SCOOP_TURN_FPS / v, STOP_ACCEL * STOP_HARD);
			}
		}
		const down = takeT - SCOOP_DOWN_MS;
		if (off < down && rec.t === takeT) {
			rec.split(down);
			// (Slowed into it: slowing as he goes down to it, too.)
			if (!turning) {
				rec.joinFrom(down);
			}
			for (const l of rec.legs) {
				if (l.t0 >= down) {
					Object.assign(l, {
						move: "scoop",
						span: [
							((l.t0 - down) / SCOOP_DOWN_MS) * SCOOP_LOW,
							((l.t1 - down) / SCOOP_DOWN_MS) * SCOOP_LOW,
						],
						yaw,
					});
				}
			}
		}
		// Up with it: round to the way he goes with it as he comes up, if it
		// is the other way - or on at the pace he came in at, getting up to
		// speed (see RAMP_ACCEL), not slowed to a set pace, nor shot off from
		// nothing.
		const on = Math.atan2(endPt.y - rec.at.y, endPt.x - rec.at.x);
		const up = rec.last.move === "scoop" ? SCOOP_LOW : 0;
		const vIn = rec.last.move === "scoop" ? legPace(rec.last) : 0;
		if (turning) {
			const turn = angleOff(on - yaw);
			const ms = Math.max(
				SCOOP_UP_MS * (1 - up),
				(Math.abs(turn) / SCOOP_TURN_RATE) * 1000,
			);
			const ft = Math.min(SCOOP_TURN_ON_FT, (vIn * ms) / 2000);
			rec.act("scoop", takeT + ms, yaw, {
				x: rec.at.x + Math.cos(yaw) * ft,
				y: rec.at.y + Math.sin(yaw) * ft,
			});
			rec.last.turn = turn;
		} else {
			const T = (SCOOP_UP_MS * (1 - up)) / 1000;
			const v0 = vIn * Math.max(0, Math.cos(angleOff(on - yaw)));
			const v1 = Math.min(Math.max(v0, rec.top * 0.88), v0 + RAMP_ACCEL * T);
			const ft = ((v0 + v1) / 2) * T;
			rec.act("scoop", takeT + T * 1000, on, {
				x: rec.at.x + Math.cos(on) * ft,
				y: rec.at.y + Math.sin(on) * ft,
			});
		}
		rec.last.span = [up, 1];
		const tEnd =
			dist(rec.at, endPt) > 2
				? rec.run(endPt, rec.top * 0.88, "sprint")
				: rec.t;
		batIt();
		// (Who brings him down: whoever the sim says did, for this carry.)
		this.lo = fumI;
		this.hi = recI;
		const chasers = this.men.filter((m) => m.team !== rec.team);
		if (way === 1) {
			this.endCarry(recI, rec, { pt: endPt, td: e.td === true }, tEnd, chasers);
		} else {
			this.endReturn(recI, rec, endPt, tEnd, e.td === true, chasers);
		}
		for (const m of this.men) {
			if (
				m.team === rec.team &&
				!this.done.has(m.pid) &&
				m.t < this.whistle - 300
			) {
				this.trail(m, endPt);
			}
		}
	}

	// Everybody about nothing else as it comes loose (at t): in toward it, a
	// beat after it is out, each from his own way - to stand off round where
	// it ends up, the man going for it in among them - until `until` (after
	// whoever comes up with it, then: see pursue). Not stood about where they
	// were while it is loose about them.
	gather(men: Actor[], at: Pt, t: number, until: number) {
		const r = this.r;
		// (Each to a place of his own round it, not where another is or is
		// going - the men coming in from the same side of it.)
		const taken: Pt[] = [];
		for (const m of men) {
			if (this.done.has(m.pid) || m.t >= until - GATHER_LEAST_MS) {
				continue;
			}
			const far = dist(m.at, at);
			// (In nearer than that already: in closer still.)
			const ring = Math.min(
				GATHER_RING + r() * GATHER_SPREAD,
				Math.max(GATHER_NEAREST, far * GATHER_IN),
			);
			const ang =
				Math.atan2(m.at.y - at.y, m.at.x - at.x) + (r() - 0.5) * GATHER_TURN;
			const pace = m.top * (GATHER_PACE + r() * GATHER_PACE_SPREAD);
			m.carryOn(Math.max(m.t, t + GATHER_REACT_MS + r() * GATHER_REACT_SPREAD));
			if (far <= ring + 1) {
				taken.push(m.at);
				continue;
			}
			const round = (turn: number) => ({
				x: at.x + Math.cos(ang + turn / ring) * ring,
				y: at.y + Math.sin(ang + turn / ring) * ring,
			});
			// (Nor on a man down there - in a heap round it, say: off the
			// length of him, as he gets there and a while after.)
			const there = Math.min(until, m.t + (dist(m.at, round(0)) / pace) * 1000);
			const offDown = (q: Pt) =>
				this.men.every(
					(o) =>
						o === m ||
						[there, there + GATHER_DOWN_STAY_MS].every(
							(u) =>
								!DOWN.has(legAt(o, u).move) ||
								this.nearOf(o, u, q, DOWN_LEGS) >= GATHER_DOWN_CLEAR,
						),
				);
			const apart = (q: Pt) => taken.every((p) => dist(p, q) >= GATHER_APART);
			const spots = GATHER_ROUND.map(round);
			const to =
				spots.find((q) => apart(q) && offDown(q)) ??
				spots.find(offDown) ??
				spots.find(apart) ??
				round(0);
			taken.push(to);
			m.plantFor(to);
			if (m.t + (dist(m.at, to) / pace) * 1000 <= until) {
				m.run(to, pace);
			} else {
				m.chaseUp(to, until, pace);
			}
		}
	}

	// ---- the pass rush and the coverage --------------------------------------------

	// How far the rusher on blocker `b` has the better of him (-1 to 1): the
	// one at rushing the passer against the others at his position, the
	// other at blocking for him against his.
	rushEdge(b: Actor, engaged: Map<Actor, Actor>): number {
		const m = [...engaged].find(([, x]) => x === b)?.[0];
		return m ? clamp((m.flair.rush ?? 0) - (b.flair.block ?? 0), -1, 1) : 0;
	}

	// When a rusher straight off the ball gets to `on` (where he takes his
	// man on, at t) - see passRush.
	private rushHitT(
		m: Actor,
		least: number,
		offMs: (far: number) => number,
		on: (t: number, lat: number) => Pt,
	): number {
		let t1 = least;
		for (let k = 0; k < 2; k++) {
			t1 = Math.max(least, m.t + offMs(dist(m.at, on(t1, 0))));
		}
		return t1;
	}

	// A rusher's way from where he is to `hit` by t1, through the men on
	// the line - in blocks, or set for them (not `b`, the man he is on):
	// the point beside the worst of it, RUSH_GAP_OFF over, to bend his way
	// through (see curve), that has him clear of them, or the most clear -
	// none, clear already, or nothing clearer.
	private rushGap(m: Actor, b: Actor, hit: Pt, t1: number): Pt | undefined {
		const D = dist(m.at, hit);
		if (D < 1 || t1 <= m.t) {
			return undefined;
		}
		const into = (pts: Pt[], T1: number) => {
			const all = [m.at, ...pts];
			const lens = [0];
			for (let i = 1; i < all.length; i++) {
				lens.push(lens[i - 1]! + dist(all[i - 1]!, all[i]!));
			}
			const L = lens.at(-1)!;
			let worst = 0;
			let at = hit;
			for (let c = m.t + CLIMB_LOOK_MS; c < T1; c += CLIMB_LOOK_MS) {
				const g = (L * (c - m.t)) / (T1 - m.t);
				const k = Math.max(
					1,
					lens.findIndex((x) => x >= g),
				);
				const p0 = all[k - 1]!;
				const p1 = all[k] ?? p0;
				const g0 = lens[k - 1]!;
				const u = lens[k]! > g0 ? clamp((g - g0) / (lens[k]! - g0), 0, 1) : 1;
				const p = { x: p0.x + (p1.x - p0.x) * u, y: p0.y + (p1.y - p0.y) * u };
				for (const x of this.men) {
					if (x === m || x === b) {
						continue;
					}
					const l = legAt(x, c);
					if (!LINE_PLAY.has(l.move) && l.move !== "passSet") {
						continue;
					}
					const w = CLIMB_CLEAR_FT - dist(x.posAt(c), p);
					if (w > worst) {
						worst = w;
						at = p;
					}
				}
			}
			return { worst, at, L };
		};
		const straight = into([hit], t1);
		if (straight.worst <= CHASE_CLEAR_SLACK_FT) {
			return undefined;
		}
		const ux = (hit.x - m.at.x) / D;
		const uy = (hit.y - m.at.y) / D;
		let best: Pt | undefined;
		let worst = straight.worst;
		for (const k of RUSH_GAP_OFF) {
			const via = {
				x: straight.at.x - uy * k,
				y: straight.at.y + ux * k,
			};
			const pts = this.curve(
				m.at,
				{
					x: 2 * via.x - (m.at.x + hit.x) / 2,
					y: 2 * via.y - (m.at.y + hit.y) / 2,
				},
				hit,
				EDGE_ARC_PTS,
			);
			// (Never out past where he hits and back, nor in on him turned
			// further off his way than QB_HIT_ROUND_END.)
			const last = pts.at(-2) ?? m.at;
			const ax = hit.x - last.x;
			const ay = hit.y - last.y;
			if (
				pts.some((p) => (p.x - m.at.x) * ux + (p.y - m.at.y) * uy > D) ||
				ax * ux + ay * uy < Math.hypot(ax, ay) * Math.cos(QB_HIT_ROUND_END)
			) {
				continue;
			}
			let L = 0;
			let was = m.at;
			for (const p of pts) {
				L += dist(was, p);
				was = p;
			}
			const w = into(pts, m.t + ((t1 - m.t) * L) / D).worst;
			if (w < worst - CHASE_CLEAR_SLACK_FT) {
				best = via;
				worst = w;
			}
			if (worst <= CHASE_CLEAR_SLACK_FT) {
				break;
			}
		}
		return best;
	}

	// Each rusher on a blocker (`pair`, the one the sim says got home on the
	// one who gave it up): off the ball, into him, the two of them giving
	// ground a step at a time back toward the quarterback - the pocket -
	// until `end`.
	passRush(
		rushers: Actor[],
		blockers: Actor[],
		qb: Actor,
		end: number,
		pair?: [Actor, Actor],
	) {
		const { snap } = this.input;
		const r = this.r;
		const pocket = this.of(qb.posAt(snap + 1500));
		const engaged = new Map<Actor, Actor>();
		if (pair && rushers.includes(pair[0]) && blockers.includes(pair[1])) {
			engaged.set(pair[0], pair[1]);
		}
		// (Each blocker, and the man on him, let up on his own count after
		// `end` - see countOf - up to PASS_END_SPREAD later; the pair the
		// sim says gave up the sack at it.)
		const endFor = (b: Actor) =>
			pair && b === pair[1]
				? end
				: end + PASS_END_SPREAD_MS * countOf(b, this.input.key, 3);
		// (The men on the line first, each onto a man on the line across from
		// him if there is one free - a back kept in left for a man coming
		// from off the ball, not a tackle run on past his guard to the back
		// behind him.)
		const lineFirst = [...rushers].sort(
			(x, y) => Number(DL.has(y.role)) - Number(DL.has(x.role)),
		);
		// (The men on the line in the pairs that between them go the least
		// way - see climbPairs - so not two of them across each other's way
		// to theirs; any left over on a back, below.)
		const taken = new Set(engaged.values());
		const lineUp = climbPairs(
			lineFirst.filter(
				(m) => DL.has(m.role) && !this.done.has(m.pid) && !engaged.has(m),
			),
			blockers.filter(
				(b) => (OL.has(b.role) || b.role === "TE") && !taken.has(b),
			),
			snap,
			() => true,
		);
		for (const [m, b] of lineUp) {
			engaged.set(m, b);
		}
		for (const m of lineFirst) {
			if (this.done.has(m.pid) || engaged.has(m)) {
				continue;
			}
			const a = this.of(m.at).a;
			const free = blockers.filter((b) => ![...engaged.values()].includes(b));
			const line = DL.has(m.role)
				? free.filter((b) => OL.has(b.role) || b.role === "TE")
				: [];
			const pool = line.length > 0 ? line : free.length > 0 ? free : blockers;
			const b = pool.sort(
				(x, y) => Math.abs(this.of(x.at).a - a) - Math.abs(this.of(y.at).a - a),
			)[0];
			if (b) {
				engaged.set(m, b);
			}
		}
		// A twist now and then (see TWIST): an end and the man inside him
		// trading blockers - not the pair the sim says got home.
		const loops = new Map<Actor, Actor>();
		if (
			end - snap >= TWIST_LEAST_MS &&
			countOf(qb, this.input.key, 52) < TWIST
		) {
			const live = (x: Actor) =>
				DL.has(x.role) && x !== pair?.[0] && !this.done.has(x.pid);
			const aOf = (x: Actor) => this.of(x.at).a;
			const can: [Actor, Actor][] = [];
			for (const [m, b] of engaged) {
				if (m.role !== "DE" || !live(m) || b === pair?.[1]) {
					continue;
				}
				const side = Math.sign(aOf(m)) || 1;
				const inside = [...engaged.keys()]
					.filter(
						(x) =>
							x !== m &&
							live(x) &&
							Math.sign(aOf(x)) === side &&
							Math.abs(aOf(x)) < Math.abs(aOf(m)),
					)
					.sort((x, y) => Math.abs(aOf(y)) - Math.abs(aOf(x)))[0];
				const bi = inside && engaged.get(inside);
				if (
					inside &&
					bi &&
					bi !== b &&
					bi !== pair?.[1] &&
					Math.abs(aOf(m) - aOf(inside)) <= TWIST_APART &&
					Math.abs(aOf(bi)) < Math.abs(aOf(b)) &&
					Math.abs(aOf(b) - aOf(bi)) <= TWIST_BLOCKERS_APART
				) {
					can.push([m, inside]);
				}
			}
			const pick =
				can[Math.floor(countOf(qb, this.input.key, 53) * can.length)];
			if (pick) {
				const [m, inside] = pick;
				const b = engaged.get(m)!;
				engaged.set(m, engaged.get(inside)!);
				engaged.set(inside, b);
				loops.set(m, inside);
			}
		}
		// The blockers' sets: back off the line, the tackles out wide; a
		// back kept in, up beside the quarterback (two of them, one either
		// side of him) - but never up in among the line.
		const backSides = new Set<number>();
		// (The gaps in the line as it sets - halfway between each man on it
		// and the next, and out past the last either side: see BACK_GAP_OUT.)
		const lineAs = blockers
			.filter(
				(b) => !this.done.has(b.pid) && (OL.has(b.role) || b.role === "TE"),
			)
			.map((b) => {
				const s = this.of(b.at);
				const tackle = b.role === "LT" || b.role === "RT" || b.role === "TE";
				return s.a + (tackle ? Math.sign(s.a) * 2 : 0);
			})
			.sort((x, y) => x - y);
		const gaps =
			lineAs.length > 0
				? [
						lineAs[0]! - BACK_GAP_OUT,
						...lineAs.slice(1).map((a, i) => (a + lineAs[i]!) / 2),
						lineAs.at(-1)! + BACK_GAP_OUT,
					]
				: [];
		for (const b of blockers) {
			if (this.done.has(b.pid)) {
				continue;
			}
			const s = this.of(b.at);
			const tackle =
				b.role === "LT" ||
				b.role === "RT" ||
				b.role === "TE" ||
				b.role === "TE2";
			const back = tackle ? 7 : 4;
			const out = tackle ? Math.sign(s.a) * 2 : 0;
			const inBack = b.role === "RB" || b.role === "FB";
			let side = Math.sign(s.a - pocket.a) || 1;
			// (A back with a man to pick up: set in the way he comes - the
			// gap he comes through - not out behind the man on the line
			// beside it, for him to come through that man to get to him.)
			const his = inBack
				? [...engaged].find(([, x]) => x === b)?.[0]
				: undefined;
			// (But not stood right behind the quarterback for it - off to
			// the side of him, BACK_LANE_LEAST at least - unless set up in
			// front of him, BACK_LANE_AHEAD or more.)
			const off = his ? this.of(his.at).a - pocket.a : 0;
			const least =
				Math.min(pocket.d + BACK_SET_UP, -BACK_OFF_LINE) - pocket.d >=
				BACK_LANE_AHEAD
					? 0
					: BACK_LANE_LEAST;
			if (inBack) {
				side =
					his && Math.abs(off) >= Math.max(least, 0.5)
						? Math.sign(off)
						: backSides.has(side)
							? -side
							: side;
				backSides.add(side);
			}
			// (In the gap he comes through - where his way in to the pocket
			// crosses the line as it sets - not stood behind a man on it, for
			// him to be met in among the line: no further out than
			// BACK_GAP_MOST.)
			let lane = his
				? side * clamp(Math.abs(off), least, BACK_SET_OUT)
				: undefined;
			if (his && lane !== undefined && gaps.length > 0) {
				const h = this.of(his.at);
				const lineD = Math.max(pocket.d + 4, -BACK_GAP_LINE);
				const u =
					h.d > pocket.d ? clamp((h.d - lineD) / (h.d - pocket.d), 0, 1) : 1;
				const cross = h.a + (pocket.a - h.a) * u;
				const gap = gaps.reduce((x, y) =>
					Math.abs(y - cross) < Math.abs(x - cross) ? y : x,
				);
				const g = gap - pocket.a;
				if (Math.abs(g) >= least && Math.abs(g) <= BACK_GAP_MOST) {
					lane = g;
					side = Math.sign(g) || side;
				}
			}
			this.offSnap(b, GET_OFF_MS, GET_OFF_SPREAD_MS);
			// (A back through a fake already: set where it left him.)
			const setPt =
				b.t > snap + 300
					? { d: s.d - 1, a: s.a }
					: inBack
						? {
								d: Math.min(pocket.d + BACK_SET_UP, -BACK_OFF_LINE),
								a: pocket.a + (lane ?? side * BACK_SET_OUT),
							}
						: { d: Math.max(pocket.d + 4, s.d - back), a: s.a + out };
			const far = dist(b.at, this.w(setPt));
			if (inBack && far > BACK_STEP_FT) {
				// (Up out of his stance and pulled up into it - see
				// RELEASE_ACCEL, STOP_HARD.)
				const t1 = Math.max(
					snap + 650,
					b.t +
						stepMs(
							far,
							b.top * BACK_UP_PACE,
							RELEASE_ACCEL,
							STOP_ACCEL * STOP_HARD,
						),
				);
				// (In strides that go with his pace at the most of it - not
				// walked there at a run's pace.)
				b.go(
					this.w(setPt),
					t1,
					gait(Math.min(b.top * BACK_UP_PACE, (2 * far * 1000) / (t1 - b.t))),
				);
				b.setInto(t1 + BACK_SET_MS, "passSet", this.ahead);
			} else {
				b.go(
					this.w(setPt),
					Math.max(
						snap + 650,
						b.t +
							Math.max(
								SET_LEAST_MS,
								(far / (inBack ? b.top * 0.75 : SET_FPS)) * 1000,
							),
					),
					"passSet",
					this.ahead,
				);
			}
			// Giving ground as the rush comes - never back into the pocket (a
			// back hardly at all: he is the last man between them and the
			// quarterback).
			const give = inBack
				? r() * 1.2
				: Math.min(
						(2 + r() * 4) * (1 + RUSH_GIVE * this.rushEdge(b, engaged)),
						Math.max(0, setPt.d - pocket.d - POCKET_ROOM),
					);
			b.go(
				this.w({ d: setPt.d - give, a: setPt.a + (pocket.a - setPt.a) * 0.1 }),
				endFor(b),
				"passSet",
				this.ahead,
			);
		}
		// The last man on the line on each side, for the rush round the edge.
		const { dir } = this.input;
		const edgeOf = (side: number) =>
			blockers
				.filter(
					(b) => !this.done.has(b.pid) && Math.sign(this.of(b.at).a) === side,
				)
				.sort(
					(x, y) => Math.abs(this.of(y.at).a) - Math.abs(this.of(x.at).a),
				)[0];
		const moved = new Set<Actor>();
		const locks: { m: Actor; b: Actor; hitT: number; ts: number[] }[] = [];
		// (On a twist, the man going first laid out first.)
		const hits = new Map<Actor, number>();
		const firstOn = new Map<Actor, Actor>();
		const order = [...engaged].sort(
			([x], [y]) => Number(loops.has(x)) - Number(loops.has(y)),
		);
		for (const [m, b] of order) {
			const s = this.of(m.at);
			const side = Math.sign(s.a) || 1;
			// Off the edge - lined up outside the last man on the line: up the
			// arc, onto his outside shoulder, and on round him, ridden past
			// the quarterback. Or in over the man across from him, working a
			// shoulder one way and then the other, the blocker with him.
			const edge =
				b === edgeOf(side) && Math.abs(s.a) > Math.abs(this.of(b.at).a) + 0.5;
			const end = endFor(b);
			m.hold(snap + 40 + r() * 60);
			// Into him as soon as he can get there out of his stance (see
			// GET_OFF_ACCEL).
			let least = snap + 380 + r() * 150;
			const first = loops.get(m);
			// (Straight at him, gathered into him - see hitMs. On a twist too,
			// the end's jab off timed flat out.)
			// (Off the edge too, at the end of the arc - see getOffAlong.)
			let gathers = true;
			const gather = gathers && !edge;
			const offMs = (far: number) =>
				gathers
					? hitMs(far, m.top * RUSH_HIT_PACE)
					: rampedMs(far, m.top * RUSH_HIT_PACE, 0, GET_OFF_ACCEL);
			let hitT = least;
			let under: Pt | undefined;
			let pen: { d: number; a: number } | undefined;
			if (first) {
				// The end on a twist: a hard step up the field, sold as a rush
				// round the edge - a step across, and off under the man going
				// first as he gets onto his.
				const jab = this.w({ d: s.d - TWIST_JAB_FT, a: s.a });
				const jabT =
					m.t + rampedMs(TWIST_JAB_FT, m.top * RUSH_HIT_PACE, 0, GET_OFF_ACCEL);
				m.getOff(jab, jabT, 0, GET_OFF_ACCEL, "run", GET_OFF_SQUARE_MS);
				const go = Math.min(
					Math.max(
						jabT + TWIST_ACROSS_MS,
						(hits.get(first) ?? jabT) - TWIST_LEAD_MS,
					),
					end - HAND_MS * 2,
				);
				m.go(
					this.w({ d: s.d - TWIST_JAB_FT, a: s.a - side * TWIST_SLIDE_FT }),
					go,
					"slide",
					this.behind,
				);
				least = m.t;
				hitT = least;
				// (Round behind him where he has got to, if he is in the way:
				// straight across, no further his side of him than that.)
				pen = this.of(first.posAt(hits.get(first) ?? m.t));
				const now = this.of(m.at);
				const to = this.of(b.posAt(m.t));
				to.d += 2.6;
				const u = (pen.a - now.a) / (to.a - now.a);
				if (
					u > 0 &&
					u < 1 &&
					now.d + (to.d - now.d) * u < pen.d + TWIST_CLEAR_FT
				) {
					under = this.w({ d: pen.d + TWIST_CLEAR_FT, a: pen.a });
				}
			}
			// (Where he is on the blocker at t - `lat` feet over, across the
			// field.)
			// (Met on the move - a back still going through a fake: pulled up
			// there, not the two of them carried on together at his pace.)
			let stop: ((t: number) => Pt) | undefined;
			const bAt = (t: number) =>
				stop !== undefined && t > hitT ? stop(t) : b.posAt(t);
			const on = (t: number, lat: number) => {
				const p = bAt(t);
				const u = clamp((t - hitT) / Math.max(1, end - hitT), 0, 1);
				const ahead = edge ? EDGE_ON - u * (EDGE_ON + EDGE_PAST) : 2.6;
				return {
					x: p.x + dir * ahead,
					y: p.y + dir * (edge ? side * EDGE_OUT : lat),
				};
			};
			const mark = m.t;
			// (A second man on a blocker already locked up with one: on his
			// shoulder the side he comes in on, leaning on him - not stood in
			// the first man's spot, working him across through him as the
			// blocker sways with the first.)
			const lockedOn = firstOn.get(b);
			const mid = lockedOn
				? dir *
					(Math.sign(m.at.y - lockedOn.posAt(hits.get(lockedOn) ?? m.t).y) ||
						1) *
					DOUBLE_RUSH_FT
				: 0;
			const hitLat = mid + (r() - 0.5) * 0.6;
			let vHit = 0;
			// (A man from off the ball onto a back kept in: straight at him
			// through the men on the line - in blocks, or set for them - in
			// through the gap beside them instead (see RUSH_GAP_OFF), bent
			// round them, not run through them.)
			if (!first && !edge && (b.role === "RB" || b.role === "FB")) {
				under = this.rushGap(
					m,
					b,
					on(this.rushHitT(m, least, offMs, on), hitLat),
					this.rushHitT(m, least, offMs, on),
				);
			}
			// (On a twist, round behind the man going first by more if he
			// has got into the way of it since - see TWIST_ROOM_FT.)
			for (const wider of TWIST_WIDER_FT) {
				if (wider > 0) {
					m.cut(mark);
					hitT = least;
					under = this.w({ d: pen!.d + TWIST_CLEAR_FT + wider, a: pen!.a });
				}
				// (Up the arc first, off the edge, and in onto him.)
				const arc =
					under ?? (edge ? this.w({ d: s.d - 2, a: s.a + side * 1.5 }) : m.at);
				const a = dist(m.at, arc);
				const reachBy = (t: number) =>
					Math.max(least, m.t + offMs(a + dist(arc, on(t, 0))));
				for (let k = 0; k < 2; k++) {
					hitT = reachBy(hitT);
				}
				// (Not where the blocker is at a time he cannot be there by - a
				// back coming up at him through a fake, say: halfway to when he
				// could, a few times.)
				for (let k = 0; k < 4 && reachBy(hitT) > hitT + 1; k++) {
					hitT = (hitT + reachBy(hitT)) / 2;
				}
				// (Off the edge, and no time to be gathered into him by then: up
				// the arc flat out, as he can be there.)
				if (edge && gathers && hitT > end - HAND_MS) {
					gathers = false;
					hitT = least;
					for (let k = 0; k < 2; k++) {
						hitT = reachBy(hitT);
					}
					for (let k = 0; k < 4 && reachBy(hitT) > hitT + 1; k++) {
						hitT = (hitT + reachBy(hitT)) / 2;
					}
				}
				// (Gathered into him, if not on him till late: on him as late as
				// that, for a moment - not sent flat out into him to be on him
				// a while longer.)
				hitT = Math.min(
					hitT,
					Math.max(least, end - (gather ? HAND_STEP_MS * 2 : HAND_MS)),
				);
				// (But no sooner than he could get there flat out - a backer
				// from deep on a quick throw, on him late; and not into him at
				// all if not before he lets up: on at the quarterback, below.)
				if (!first && m !== pair?.[0]) {
					const flat = (t: number) =>
						m.t +
						rampedMs(
							a + dist(arc, on(t, 0)),
							m.top * FLAT_OUT,
							0,
							GET_OFF_ACCEL,
						);
					for (let k = 0; k < 3 && flat(hitT) > hitT + 1; k++) {
						hitT = flat(hitT);
					}
					if (hitT > end - HAND_STEP_MS * 2) {
						engaged.delete(m);
						break;
					}
				}
				const hit = on(hitT, hitLat);
				// (Bent up round it onto him, not off one way and turned the
				// other - or, on a twist, through the point behind the man going
				// first.)
				const via = under
					? {
							x: 2 * under.x - (m.at.x + hit.x) / 2,
							y: 2 * under.y - (m.at.y + hit.y) / 2,
						}
					: arc;
				vHit =
					edge || under
						? this.getOffAlong(
								m,
								this.curve(m.at, via, hit, EDGE_ARC_PTS),
								hitT,
								GET_OFF_ACCEL,
								m.top * RUSH_HIT_PACE,
								"run",
								GET_OFF_SQUARE_MS,
								gathers ? HIT_FPS : undefined,
							)
						: m.hitInto(
								hit,
								hitT,
								m.top * RUSH_HIT_PACE,
								GET_OFF_ACCEL,
								HIT_FPS,
								HIT_ACCEL,
								"run",
								GET_OFF_SQUARE_MS,
								m.top * FLAT_OUT,
							);
				if (!first || !pen) {
					break;
				}
				let near = Infinity;
				for (let t = mark; t <= hitT; t += 50) {
					near = Math.min(near, dist(m.posAt(t), first.posAt(t)));
				}
				if (near >= TWIST_ROOM_FT) {
					break;
				}
			}
			if (!engaged.has(m)) {
				continue;
			}
			hits.set(m, hitT);
			if (!firstOn.has(b)) {
				firstOn.set(b, m);
			}
			const moves = !moved.has(b) && !this.done.has(b.pid);
			{
				const p1 = b.posAt(hitT);
				const p0 = b.posAt(hitT - 50);
				const v = dist(p0, p1) / 0.05;
				if (moves && v > SHUFFLE_MOST * HOLD_ON) {
					const A = STOP_ACCEL * STOP_HARD;
					// (Into each other: the two of them on at the pace between
					// them - the rusher's into him and his own, CONTACT_MS
					// getting to it - and pulled up from that. Not the rusher
					// stopped dead and carried off backward at all the pace of
					// a back still going through his fake.)
					const q1 = m.posAt(hitT);
					const q0 = m.posAt(hitT - 50);
					const vb = { x: (p1.x - p0.x) / 0.05, y: (p1.y - p0.y) / 0.05 };
					const vp = {
						x: (vb.x + (q1.x - q0.x) / 0.05) / 2,
						y: (vb.y + (q1.y - q0.y) / 0.05) / 2,
					};
					const w = Math.hypot(vp.x, vp.y);
					const Tb = CONTACT_MS / 1000;
					const pb = {
						x: p1.x + ((vb.x + vp.x) / 2) * Tb,
						y: p1.y + ((vb.y + vp.y) / 2) * Tb,
					};
					stop = (t: number) => {
						const s = Math.max(0, (t - hitT) / 1000);
						if (s <= Tb) {
							const k = (s * s) / (2 * Tb);
							return {
								x: p1.x + vb.x * s + (vp.x - vb.x) * k,
								y: p1.y + vb.y * s + (vp.y - vb.y) * k,
							};
						}
						const sd = Math.min(s - Tb, w / A);
						const far = w > 0 ? (w * sd - (A * sd * sd) / 2) / w : 0;
						return { x: pb.x + vp.x * far, y: pb.y + vp.y * far };
					};
				}
			}
			const over = this.rushEdge(b, engaged);
			const per = HAND_MS * (0.8 + r() * 0.5) * (1 - RUSH_QUICKER * over);
			const amp =
				(edge ? 0 : HAND_FT * (0.5 + r()) * (1 + RUSH_WIDER * over)) *
				(lockedOn ? DOUBLE_RUSH_SWAY : 1);
			let way = r() < 0.5 ? 1 : -1;
			const turns: { t: number; lat: number }[] = [];
			{
				let t0 = hitT;
				let lat0 = hitLat;
				for (let k = 0; ; k++) {
					const c = (j: number) =>
						countOf(m, this.input.key, HAND_SALT + 3 * k + j);
					const t =
						t0 + per * (HAND_PER[0] + (HAND_PER[1] - HAND_PER[0]) * c(0));
					if (t >= end - per / 2) {
						break;
					}
					const want =
						c(2) < HAND_MID && k > 0
							? mid
							: mid +
								way *
									amp *
									(HAND_WIDE[0] + (HAND_WIDE[1] - HAND_WIDE[0]) * c(1));
					// (Eased over and back - see below: at its quickest PI / 2
					// times as quick as over all of it. Quicker about it, as
					// much wider, the better he is than the man on him.)
					const most =
						(HAND_MOST_FPS * (1 + RUSH_WIDER * over) * 2 * (t - t0)) /
						Math.PI /
						1000;
					const lat = lat0 + clamp(want - lat0, -most, most);
					turns.push({ t, lat });
					way = -way;
					t0 = t;
					lat0 = lat;
				}
			}
			turns.push({ t: end, lat: mid });
			// (Over and back easing in and out of each way he works him.)
			const steps: { t: number; lat: number }[] = [];
			let t0 = hitT;
			let lat0 = hitLat;
			for (const x of turns) {
				const n = Math.max(1, Math.round((x.t - t0) / HAND_STEP_MS));
				for (let k = 1; k <= n; k++) {
					const u = (1 - Math.cos((Math.PI * k) / n)) / 2;
					steps.push({
						t: t0 + ((x.t - t0) * k) / n,
						lat: lat0 + (x.lat - lat0) * u,
					});
				}
				t0 = x.t;
				lat0 = x.lat;
			}
			const held = steps.map((x) => bAt(x.t));
			// (Knocked back a step as he gets into him, the two of them - see
			// JOLT_PER_FPS - and that ground made up as he gives the rest.)
			const jolt = moves ? Math.min(JOLT_MOST, vHit * JOLT_PER_FPS) : 0;
			// (Off the edge, ridden on past: the blocker giving ground with
			// him as he goes, not stood where he set the while.)
			const give = edge
				? Math.min(EDGE_GIVE, (EDGE_GIVE_FPS * Math.max(0, end - hitT)) / 1000)
				: 0;
			const shove = (t: number) => {
				const u = clamp((t - hitT) / JOLT_MS, 0, 1);
				const w = clamp((t - hitT) / Math.max(1, end - hitT), 0, 1);
				return (
					jolt * (1 - (1 - u) * (1 - u)) * (1 - w) + give * w * w * (3 - 2 * w)
				);
			};
			const him: Pt[] = [];
			for (const x of steps) {
				const p = on(x.t, x.lat);
				p.x -= dir * shove(x.t);
				him.push(p);
				// (On to him a moment later than he would have been - round the
				// edge, say: on to the step after, not this one quicker than he
				// can go.)
				if (
					x !== steps.at(-1) &&
					dist(p, m.at) > (m.top * Math.max(0, x.t - m.t)) / 1000
				) {
					continue;
				}
				m.go(
					p,
					x.t,
					"engage",
					edge ? this.toward(p, qb.posAt(x.t)) : this.behind,
				);
			}
			// (A man doubled moves with the first of them - turned to him,
			// opened up to a man round his outside, as far as BLOCK_TURN.)
			if (moves) {
				moved.add(b);
				b.cut(hitT);
				steps.forEach((x, k) => {
					const at = {
						x: held[k]!.x - dir * shove(x.t),
						y: held[k]!.y + dir * x.lat * HAND_WITH,
					};
					// (In strides while he is still quicker than a set.)
					const fps = (dist(b.at, at) / Math.max(1, x.t - b.t)) * 1000;
					if (stop !== undefined && fps > SHUFFLE_MOST * HOLD_ON) {
						b.go(at, x.t);
					} else {
						b.go(at, x.t, "passSet", this.facing(at, him[k]!));
					}
				});
			}
			locks.push({ m, b, hitT, ts: steps.map((x) => x.t) });
		}
		// A lineman with nobody to block: in to help on the nearest of his
		// own locked up with a rusher - at his shoulder, turned to the
		// rusher, giving ground with them.
		const busy = new Set(engaged.values());
		const helped = new Set<Actor>();
		const aAt = (m: Actor) => this.of(m.posAt(snap)).a;
		for (const f of blockers) {
			if (
				busy.has(f) ||
				this.done.has(f.pid) ||
				f.role === "RB" ||
				f.role === "FB"
			) {
				continue;
			}
			const fa = aAt(f);
			// (Where the man he would help is as the rush gets to him - a
			// back picking a man up well off from where he lined up, say -
			// and nobody else on the line between them there: not across
			// behind the man beside him to it.)
			const offAt = (k: (typeof locks)[number]) => {
				const ka = this.of(k.b.posAt(k.hitT)).a;
				const own = this.of(f.posAt(k.hitT)).a;
				const lo = Math.min(ka, own);
				const hi = Math.max(ka, own);
				const between = blockers.some((x) => {
					if (x === f || x === k.b || x.role === "RB" || x.role === "FB") {
						return false;
					}
					const xa = this.of(x.posAt(k.hitT)).a;
					return xa > lo + 0.5 && xa < hi - 0.5;
				});
				return between ? Infinity : Math.abs(ka - own);
			};
			const lock = locks
				.filter(
					(k) =>
						!helped.has(k.m) &&
						k.m !== pair?.[0] &&
						Math.abs(aAt(k.b) - fa) <= HELP_A &&
						offAt(k) <= HELP_A,
				)
				.sort((x, y) => Math.abs(aAt(x.b) - fa) - Math.abs(aAt(y.b) - fa))[0];
			if (!lock) {
				continue;
			}
			const go = Math.max(
				f.legs[0]!.t1,
				lock.hitT +
					HELP_REACT_MS +
					((f.pid * 7) % 10) * HELP_REACT_SPREAD * 0.1,
			);
			const ts = lock.ts.filter((t) => t > go + HAND_STEP_MS);
			if (ts.length === 0) {
				continue;
			}
			const side = Math.sign(fa - aAt(lock.b)) || 1;
			const spot = (t: number) => {
				const p = lock.b.posAt(t);
				return { x: p.x - dir * HELP_BACK, y: p.y + dir * side * HELP_OUT };
			};
			// (Over there no quicker than HELP_ON_FPS - or not at all.)
			const from = f.posAt(go);
			if (dist(from, spot(go)) > HELP_FAR_FT) {
				continue;
			}
			let k = ts.findIndex(
				(t) => dist(from, spot(t)) <= (HELP_ON_FPS * (t - go)) / 1000,
			);
			if (k < 0) {
				continue;
			}
			// (Not into the way of a back picking up a man from off the ball
			// - where they meet - if he is clearer of them where he is.)
			const backs = locks.filter((x) => x.b.role === "RB" || x.b.role === "FB");
			const nearest = (at: (t: number) => Pt) => {
				let w = Infinity;
				for (const t of ts.slice(k)) {
					for (const x of backs) {
						w = Math.min(
							w,
							dist(at(t), x.m.posAt(t)),
							dist(at(t), x.b.posAt(t)),
						);
					}
				}
				return w;
			};
			const there = nearest(spot);
			if (there < CLIMB_CLEAR_FT && there < nearest((t) => f.posAt(t))) {
				continue;
			}
			helped.add(lock.m);
			f.cut(go);
			for (; k < ts.length; k++) {
				const p = spot(ts[k]!);
				f.go(p, ts[k]!, "passSet", this.facing(p, lock.m.posAt(ts[k]!)));
			}
		}
		for (const b of blockers) {
			if (!busy.has(b)) {
				this.checkRelease(b, blockers, qb, endFor(b));
			}
		}
		for (const m of rushers) {
			if (!engaged.has(m) && !this.done.has(m.pid)) {
				m.hold(snap + 60);
				m.run(qb.posAt(snap + 1800), m.top * 0.7);
			}
		}
	}

	// CHECK AND RELEASE. A back kept in to block with nobody coming at him
	// (most of them): a look for a man coming, and then out into the flat
	// on his side of the pocket - round outside the last man on the line,
	// up past it - and sat there turned to the quarterback, a man to dump
	// it to, not stood in the pocket the while. Only with the time to get
	// there and be sat there a while, and nobody in his way.
	private checkRelease(b: Actor, blockers: Actor[], qb: Actor, end: number) {
		const { snap } = this.input;
		if (
			(b.role !== "RB" && b.role !== "FB") ||
			this.done.has(b.pid) ||
			countOf(b, this.input.key, 149) >= CHECK_OUT
		) {
			return;
		}
		const go =
			snap +
			CHECK_MS[0] +
			(CHECK_MS[1] - CHECK_MS[0]) * countOf(b, this.input.key, 150);
		if (
			end - go < CHECK_LEAST_MS ||
			b.t < end - 1 ||
			legAt(b, go).move !== "passSet"
		) {
			return;
		}
		const at = b.posAt(go);
		const s = this.of(at);
		const q = this.of(qb.posAt(go));
		const own =
			Math.sign(s.a - q.a) || (countOf(b, this.input.key, 151) < 0.5 ? -1 : 1);
		const fps = b.top * CHECK_PACE;
		// (Outside the last man on the line on his side - round behind him,
		// as deep as he is set as he gets there, or deeper - or the other
		// side, if that is the way that is clear.)
		const tryWay = (side: number, behind: number) => {
			const near = go + CHECK_NEAR_MS;
			let edge = 0;
			let deep = 0;
			for (const x of blockers) {
				if (x !== b && (OL.has(x.role) || x.role === "TE")) {
					const e = this.of(x.posAt(near));
					if (side * e.a > edge) {
						edge = side * e.a;
						deep = e.d;
					}
				}
			}
			const round = this.w({
				d: Math.min(s.d + CHECK_UP_FT, deep - behind),
				a: side * Math.max(edge + CHECK_WIDE_FT, side * s.a),
			});
			const flat = this.w({
				d: CHECK_FLAT_D,
				a: side * (edge + CHECK_FLAT_OUT),
			});
			const first = dist(at, round);
			const L = first + dist(round, flat);
			const there = go + rampedMs(L, fps, 0, RELEASE_ACCEL) + CHECK_EASE_MS;
			// (Into nobody on the way, nor anybody where he sits.)
			for (let t = go; t <= there + CHECK_SIT_MS; t += 50) {
				const u = clamp((t - go) / (there - go), 0, 1) * L;
				const [a, z, g] =
					u <= first ? [at, round, u] : [round, flat, u - first];
				const k = dist(a, z) > 0 ? g / dist(a, z) : 0;
				const p = { x: a.x + (z.x - a.x) * k, y: a.y + (z.y - a.y) * k };
				for (const x of this.men) {
					// (A man of his own set in the line: behind him at a step.)
					const room =
						x.team === b.team && legAt(x, t).move === "passSet"
							? CHECK_BY_OWN_FT
							: CHECK_CLEAR_FT;
					if (x !== b && dist(x.posAt(t), p) < room) {
						return undefined;
					}
				}
			}
			return { round, flat };
		};
		let way: { round: Pt; flat: Pt } | undefined;
		for (const side of [own, -own]) {
			for (const behind of CHECK_BEHIND_FT) {
				way ??= tryWay(side, behind);
			}
		}
		if (!way) {
			return;
		}
		const { round, flat } = way;
		b.cut(go);
		b.run(round, fps);
		b.runUp(flat, fps);
		b.hold(
			Math.max(end, b.t + CHECK_SIT_MS),
			"settle",
			this.toward(b.at, qb.posAt(b.t)),
		);
	}

	// The coverage until `until`: each man on a receiver going out - a
	// corner on the outside man, the nickel on the slot, a linebacker or a
	// safety on a tight end or a back (and `marked`, the man the sim says
	// made a play on the ball, on the man it went to) - trailing him a
	// step; the others back into their zones. When the ball is thrown,
	// they break on it.
	cover(
		coverage: Actor[],
		runners: Actor[],
		upTo: number,
		catchPt: Pt | undefined,
		catchAt: number,
		marked?: [Actor, Actor],
		fake?: number,
		yac = 0,
		late?: Actor,
		fooled?: { m: Actor; at: number; top: Pt; stem: Pt },
		call: { zone: boolean; twoHigh: boolean } = { zone: false, twoHigh: false },
		holding?: Actor,
	) {
		const { snap } = this.input;
		const r = this.r;
		// (Round where it is caught - further off, the further he goes with
		// it after: they were beaten to it.)
		const closeIn = 4.5 + clamp(yac / 5, 0, 12);
		// Everybody where he lined up.
		const set = (m: Actor) => this.of(m.posAt(snap));
		const free = coverage.filter((m) => !this.done.has(m.pid));
		const pairs = new Map<Actor, Actor>();
		if (marked && free.includes(marked[0]) && runners.includes(marked[1])) {
			pairs.set(marked[0], marked[1]);
		}
		// (The man the sim flags for holding: on the man nearest where he
		// lined up, all over him - see HOLD_CUSHION - whatever the rest are
		// in.)
		const held = (rec: Actor) => [...pairs.values()].includes(rec);
		if (holding && free.includes(holding) && !pairs.has(holding)) {
			const h = set(holding);
			const near = (rs: Actor[]) =>
				rs
					.map((rec) => {
						const ra = set(rec);
						return {
							rec,
							far: Math.abs(h.a - ra.a) + Math.abs(h.d - ra.d) * 0.3,
						};
					})
					.sort((a, b) => a.far - b.far)[0]?.rec;
			const rec = near(runners.filter((m) => !held(m))) ?? near(runners);
			if (rec) {
				pairs.set(holding, rec);
			}
		}
		const order = call.zone
			? []
			: runners
					.filter((m) => !held(m))
					.sort((a, b) => Math.abs(set(b).a) - Math.abs(set(a).a));
		// (The safeties deep, two of them, the free safety alone: not on a
		// man.)
		const deep = (m: Actor) =>
			m.role === "FS" || (call.twoHigh && m.role === "SS");
		for (const rec of order) {
			const ra = set(rec);
			let best: Actor | undefined;
			let bestScore = Infinity;
			for (const m of free) {
				if (pairs.has(m) || deep(m)) {
					continue;
				}
				const ma = set(m);
				const fit =
					(rec.role === "X" || rec.role === "Z") && m.role !== "CB" ? 25 : 0;
				const score = Math.abs(ma.a - ra.a) + Math.abs(ma.d - ra.d) * 0.3 + fit;
				if (score < bestScore) {
					bestScore = score;
					best = m;
				}
			}
			if (best) {
				pairs.set(best, rec);
			}
		}
		const closing: Pt[] = [];
		for (const m of free) {
			const rec = pairs.get(m);
			const start = set(m);
			// (Each sees it on his own count - see countOf; the man the sim
			// says got to the ball, at once.)
			const until =
				marked && m === marked[0]
					? upTo
					: upTo + COVER_BREAK_SPREAD_MS * countOf(m, this.input.key, 5);
			if (rec) {
				// On his man: a step behind him and to the inside, until the
				// ball is in the air - backpedaling out of his stance, then
				// turned and running with him.
				this.offSnap(m, MAN_OFF_MS, MAN_OFF_SPREAD_MS);
				const rs = set(rec);
				const inside = -Math.sign(rs.a || 1);
				// (The man the sim says got to the ball: never deeper than
				// where it goes, by more than MARK_DEEP_FT - sat on the route
				// as it comes back, not run on past it to turn round flat out
				// for it.)
				const marks = marked && m === marked[0] && catchPt;
				const deepest = marks ? this.of(catchPt).d + MARK_DEEP_FT : Infinity;
				const mark = marks
					? this.w({ d: deepest, a: this.of(catchPt).a })
					: undefined;
				// (On a slant-and-go - see SLUGGO - up on the jab in, and
				// after him once he is by.)
				const bit = fooled && rec === fooled.m && !marks ? fooled : undefined;
				let biteTo: Pt | undefined;
				// (A step at a time, the last of them on to the break - not
				// short of it, stood waiting for it.)
				const steps: number[] = [];
				for (let t = snap + COVER_STEP_FIRST; t <= until; t += COVER_STEP) {
					steps.push(t);
				}
				const last = steps.at(-1);
				if (last !== undefined && until > last) {
					if (until - last < COVER_STEP / 2) {
						steps[steps.length - 1] = until;
					} else {
						steps.push(until);
					}
				}
				for (const t of steps) {
					// (Not one he has no time for, still on his way out of what
					// he was doing at the snap.)
					if (t - m.t < (t === until ? 1 : COVER_STEP)) {
						continue;
					}
					const p = this.of(rec.posAt(t - 180));
					const room =
						start.d -
						rs.d -
						(1 - COVER_GIVE) * Math.max(0, p.d - rs.d) -
						(COVER_COME * Math.max(0, t - snap - COVER_READ_MS)) / 1000;
					const cushion = Math.min(
						m === holding ? HOLD_CUSHION : Infinity,
						(COVER_MIN +
							COVER_SOFT *
								Math.log1p(Math.exp((room - COVER_MIN) / COVER_SOFT))) *
							(1 - COVER_TIGHT * (m.flair.cover ?? 0)),
					);
					const biting =
						bit &&
						t >= bit.at - SLUGGO_BITE_BEFORE_MS &&
						t <= bit.at + SLUGGO_BITE_MS;
					if (biting && !biteTo) {
						const top = this.of(bit.top);
						const jabIn = Math.sign(top.a - this.of(bit.stem).a) || -inside;
						biteTo = this.w({
							d: Math.max(top.d + 2, this.of(m.at).d - SLUGGO_BITE_DOWN),
							a: top.a + jabIn * SLUGGO_BITE_IN,
						});
					}
					if (bit && !biting && t > bit.at) {
						// (Beaten: turned and after him flat out, to just behind
						// him where he will be when it is in the air - or as
						// far as he gets toward there by then.)
						const e = this.of(rec.posAt(until));
						const aim = this.w({
							d: e.d - SLUGGO_TRAIL_FT,
							a: e.a + inside * 1.8,
						});
						m.plantFor(aim);
						const far = dist(m.at, aim);
						const T = (until - m.t) / 1000;
						if (T * 1000 > FLICK_MS && far > ROUTE_LEFT) {
							const fps = m.top * 0.95;
							const v0 = Math.min(fps, paceOn(m.last, m.at, aim));
							const ta = Math.min(T, (fps - v0) / SLUGGO_TURN_ACCEL);
							const gets =
								v0 * ta + (SLUGGO_TURN_ACCEL * ta * ta) / 2 + fps * (T - ta);
							const u = Math.min(1, gets / far);
							m.getOff(
								{
									x: m.at.x + (aim.x - m.at.x) * u,
									y: m.at.y + (aim.y - m.at.y) * u,
								},
								until,
								v0,
								SLUGGO_TURN_ACCEL,
							);
						}
						break;
					}
					const on = biting
						? biteTo!
						: this.w({
								d: Math.min(p.d + cushion, deepest),
								a: p.a + inside * 1.8,
							});
					// (And a way off him, or over the last MARK_READ_MS before
					// it is thrown, over toward where it goes rather than on
					// after him - reading it, not run to where he was and on
					// past it as he cuts back.)
					const k = mark
						? Math.max(
								clamp(
									(dist(m.at, rec.posAt(t)) - MARK_NEAR_FT) / MARK_FAR_FT,
									0,
									1,
								),
								clamp(1 - (until - t) / MARK_READ_MS, 0, 1),
							)
						: 0;
					const to = mark
						? {
								x: on.x + (mark.x - on.x) * k,
								y: on.y + (mark.y - on.y) * k,
							}
						: on;
					const front = this.of(m.at).d > p.d + COVER_FRONT;
					// (His man sat down, and him there with him already: set,
					// square to him - not stood up straight in his tracks.)
					if (
						(sits(m.last.move) || legPace(m.last) < CRAWL_FPS) &&
						dist(m.at, to) < (CRAWL_FPS * (t - m.t)) / 1000
					) {
						m.hold(t, "backerReady", this.toward(m.at, rec.posAt(t)));
						continue;
					}
					// (His man broken off well round from the way he is going:
					// planted for it first - see plantFor - not snapped round
					// on to him at full speed. On after him from the next step,
					// if that takes him up to this one.)
					m.plantFor(to);
					// (Coming on to it too quick to stop there: on past it, pulled
					// up hard, back for him from there.)
					if (t - m.t < FLICK_MS || m.pullsPast(to, t)) {
						continue;
					}
					// (No further a step than he can get up to going there, off
					// what he was doing - out of his stance, from nothing.)
					const l = m.last;
					const off = SET.has(l.move);
					const v0 = off ? 0 : paceOn(l, l.to, to);
					const up = ((off ? RELEASE_ACCEL : COVER_ACCEL) * (t - m.t)) / 2000;
					m.chase(
						to,
						t,
						Math.min(m.top * 0.95, v0 + up),
						undefined,
						front ? this.behind : undefined,
					);
				}
			} else {
				// Back into his zone: deep middle, a deep half or third, a
				// flat, under the routes over the middle - see ZONE_TWO.
				const mid = (this.bounds.left + this.bounds.right) / 2;
				const outward = Math.sign(start.a - mid) || 1;
				const zone =
					call.twoHigh && deep(m)
						? {
								d: Math.max(start.d, 38 + r() * 10),
								a:
									mid +
									outward *
										(this.bounds.right - this.bounds.left) *
										HALF_OUT *
										0.5,
							}
						: m.role === "FS"
							? { d: Math.max(start.d, 42 + r() * 12), a: start.a * 0.4 }
							: m.role === "CB" && call.zone && call.twoHigh
								? { d: 16 + r() * 6, a: start.a + outward * 3 }
								: (m.role === "SS" && !call.zone) || m.role === "CB"
									? { d: Math.max(start.d, 36 + r() * 10), a: start.a * 0.9 }
									: m.role === "SS"
										? { d: 15 + r() * 8, a: start.a * 1.2 }
										: { d: 18 + r() * 10, a: start.a * 0.8 };
				this.offSnap(m, ZONE_OFF_MS, ZONE_OFF_SPREAD_MS);
				// (Up a step or two to the fake first, a linebacker, and stood
				// reading it.)
				let drop = snap + 1500;
				if (fake !== undefined && LB.has(m.role)) {
					// (Stood reading it only if for long enough to be: on with
					// the step up to then, if not.)
					const biteT = Math.max(
						Math.min(snap + 600, fake),
						m.t +
							stepMs(
								PA_BITE,
								PA_BITE_FPS,
								RELEASE_ACCEL,
								STOP_ACCEL * STOP_HARD,
							),
					);
					const readT = fake + 100;
					m.go(
						this.w({ d: start.d - PA_BITE, a: start.a }),
						readT - biteT < FLICK_MS ? Math.max(biteT, readT) : biteT,
						"slide",
						this.behind,
					);
					m.hold(readT, "backerStance", this.behind);
					drop = Math.max(drop, fake + 1300);
				}
				// (A few read steps back at least, lined up deep already - and
				// there at a backpedal's pace, not stepping in place. No further
				// back than the end line: not timed to get past it, turned and
				// walked the last of the way there.)
				const spot = onField(
					this.w({
						d: Math.max(zone.d, start.d + READ_STEPS_FT),
						a: zone.a,
					}),
					m.roam,
				);
				// (Settling into it at the last - or, too far to get there
				// backpedaling, turned and run there, round to the ball once
				// he is. Not when the ball or the run calls him off it first.)
				const far = dist(m.at, spot);
				const due = Math.min(drop, m.t + (far / ZONE_FPS) * 1000);
				let by = Math.min(until, due);
				let T = Math.max(0.1, (by - m.t) / 1000);
				const room = until < due ? 0 : Math.min(ZONE_SETTLE_FT, far / 2);
				const bail =
					room > 0 &&
					(far + room) / T > BACK_MOST_FPS &&
					T * 1000 >= BAIL_LEAST_MS;
				const fit = bail ? room : Math.min(room, BACK_MOST_FPS * T - far);
				let ease = fit < 0.5 ? 0 : fit;
				// (Backpedaling the whole way, no quicker than a backpedal goes
				// - see BACK_TOP: not run backward at a sprint's pace to beat the
				// ball there. As far as that gets him.)
				let fps = Math.min(
					m.top * 0.6,
					bail ? Infinity : BACK_TOP,
					(far + ease) / T,
				);
				let reach = far;
				if (bail) {
					// (Up to speed out of his stance as he goes - see
					// RELEASE_ACCEL - and pulled up into it over as far as that
					// takes (see STOP_HARD): as quick as gets him there on time,
					// or there later, as quick as he goes - or, the ball out
					// first, as far as that gets him.)
					const most = m.top * 0.6;
					const off = m.t + BAIL_BACK_MS;
					const v0 = (RELEASE_ACCEL * BAIL_BACK_MS) / 2000;
					const s0 = (v0 * BAIL_BACK_MS) / 1000;
					for (let k = 0; k < 2; k++) {
						ease = Math.max(
							fit,
							Math.min(far / 2, (fps * fps) / (2 * STOP_ACCEL * STOP_HARD)),
						);
						const go = far + ease - s0;
						by = Math.max(
							by,
							Math.min(until - ZONE_SIT_MS, off + rampedMs(go, most, v0)),
						);
						T = (by - off) / 1000;
						fps = Math.min(most, paceFor(go, T, v0, RAMP_ACCEL));
					}
					const ta = Math.min(T, Math.max(0, fps - v0) / RAMP_ACCEL);
					reach = Math.min(
						far,
						s0 + v0 * ta + (RAMP_ACCEL * ta * ta) / 2 + fps * (T - ta) - ease,
					);
				}
				const u = far > 0 ? Math.max(0, reach - ease) / far : 0;
				const settleAt = {
					x: m.at.x + (spot.x - m.at.x) * u,
					y: m.at.y + (spot.y - m.at.y) * u,
				};
				// (Turned and run there out of a backpedal step or two.)
				if (bail) {
					m.chase(
						settleAt,
						m.t + BAIL_BACK_MS,
						Math.min(BACK_TOP, (RELEASE_ACCEL * BAIL_BACK_MS) / 2000),
						"back",
						this.behind,
					);
				}
				// (Up into his backpedal out of his stance - see RELEASE_ACCEL -
				// not off it at full pace from still: short of where that would
				// have him by as much, on time.)
				const easeMs = ((2 * ease) / Math.max(1, fps)) * 1000;
				const T1 = (by - easeMs - m.t) / 1000;
				const go = dist(m.at, settleAt);
				const gets = Math.min(go, fps * T1) - (fps * fps) / (2 * RELEASE_ACCEL);
				if (!bail && legPace(m.last) < 0.5 && T1 > 0 && gets > 0.5) {
					m.getOff(
						{
							x: m.at.x + ((settleAt.x - m.at.x) * gets) / go,
							y: m.at.y + ((settleAt.y - m.at.y) * gets) / go,
						},
						by - easeMs,
						0,
						RELEASE_ACCEL,
						"back",
						0,
						this.behind,
					);
				} else {
					m.chase(
						settleAt,
						by - easeMs,
						fps,
						bail ? gait(fps) : "back",
						bail ? undefined : this.behind,
					);
				}
				if (ease > 0) {
					m.easeOff(ease);
				}
				// Sat in it, reading it: drifting with the nearest man coming
				// into it, a step or two at a time. (Pulled up into it off his
				// drop or a drift, not stopped dead - see easeOff - with time
				// to sit in it after.)
				const sit = (t: number) => {
					// (Only a moment left: on in what he was doing, not set in
					// his stance for it - see hold.)
					if (t - m.t < FLICK_MS) {
						m.hold(t);
						return;
					}
					const v = legPace(m.last);
					const ft = Math.min(ZONE_SETTLE_FT, (v * v) / (2 * STOP_ACCEL));
					const spare = t - m.t;
					const easing = v > 0 ? ((2 * ft) / v) * 1000 : Infinity;
					if (spare >= easing + ZONE_SIT_MS) {
						m.easeOff(ZONE_SETTLE_FT);
					} else if (spare >= ZONE_EASE_LEAST_MS && spare <= easing) {
						// (No time to sit after: eased down over all of it.)
						m.easeOff((v * spare) / 2000);
						if (t - m.t < ZONE_EASE_LEAST_MS / 2) {
							return;
						}
					}
					// (Alive in it, his weight going foot to foot - not stood
					// stock still in the middle of the play.)
					m.hold(t, "backerReady", this.behind);
				};
				for (
					let t = m.t + ZONE_STEP_MS;
					t < until - ZONE_STEP_MS / 2;
					t += ZONE_STEP_MS
				) {
					let near: Pt | undefined;
					let nd = Infinity;
					for (const rec of runners) {
						const p = rec.posAt(t);
						const d = dist(p, spot);
						if (d < nd) {
							nd = d;
							near = p;
						}
					}
					const tight = 1 + ZONE_TIGHT * (m.flair.cover ?? 0);
					const u =
						near && nd < ZONE_SEE_FT
							? Math.min(ZONE_REACH_FT * tight, nd * ZONE_LEAN * tight) / nd
							: 0;
					const to = near
						? {
								x: spot.x + (near.x - spot.x) * u,
								y: spot.y + (near.y - spot.y) * u,
							}
						: spot;
					if (dist(to, m.at) < ZONE_STIR_FT) {
						sit(t);
					} else {
						// (In his low steps, his eyes on the ball - across or
						// back, shuffling or backpedaling: see faced - not
						// walking it.)
						m.chase(to, t, ZONE_DRIFT_FPS, "slide", this.behind);
					}
				}
				sit(until);
			}
			// (On the man thrown to on his back shoulder: his back to it,
			// carried on up the field past him a moment as he pulls up for
			// it, before he sees it - see COVER_LATE_MS.)
			const l = m.last;
			if (
				rec !== undefined &&
				rec === late &&
				GAITS.has(l.move) &&
				legPace(l) >= COVER_LATE_LEAST
			) {
				const far = dist(l.from, l.to);
				const ft = legPace(l) * COVER_LATE_KEEP * (COVER_LATE_MS / 1000);
				m.go(
					{
						x: m.at.x + ((l.to.x - l.from.x) / far) * ft,
						y: m.at.y + ((l.to.y - l.from.y) / far) * ft,
					},
					m.t + COVER_LATE_MS,
				);
			}
			// The ball in the air: everyone near breaks for it, each to a
			// spot of his own round it.
			if (catchPt) {
				const to = catchPt;
				const far = dist(m.at, to);
				if (far < 60) {
					const ang = Math.atan2(m.at.y - to.y, m.at.x - to.x);
					// (Not back off it, though, a man already near.)
					const off = Math.min(closeIn, Math.max(4.5, far - 2));
					// (On his own side of it: further out, rather than round the
					// far side of the man it goes to, through him.)
					const spots = [off, off + 3.5].flatMap((ring) =>
						[0, 1, 2, 3, 4].map((k) => {
							const a = ang + Math.ceil(k / 2) * 0.7 * (k % 2 === 0 ? 1 : -1);
							return {
								x: to.x + Math.cos(a) * ring,
								y: to.y + Math.sin(a) * ring,
							};
						}),
					);
					// (The man the sim says knocked it away, with the time to
					// get there and be set for it: straight to where he does
					// it from - see breakOn - not to a place round it first and
					// then on over to it. On to it too quick to pull up there,
					// or late for it: to a place round it, and broken on it from
					// there - see breakOn.)
					const swat =
						m === marked?.[0] && this.statPids("defPssDef")[0] === m.pid
							? this.breakSpot(m, to, true)
							: undefined;
					const goes = legPace(m.last);
					const swatFar = swat ? dist(m.at, swat) : 0;
					const swats =
						swat !== undefined &&
						(goes < STOP_LEAST ||
							paceOn(m.last, m.at, swat) >= goes * BREAK_UP_ON) &&
						swatFar >=
							(goes * goes) / (2 * STOP_ACCEL * STOP_HARD) + PAST_SLACK_FT &&
						catchAt - BREAK_UP_EARLY_MS - m.t >=
							((2 * swatFar) / Math.max(goes, JOG_LEAST_FPS)) * 1000 +
								BREAK_SET_MS;
					const spot =
						swat && swats
							? swat
							: (spots.find((p) => closing.every((q) => dist(q, p) > 3.5)) ??
								spots.at(-1)!);
					closing.push(spot);
					// (Planted first, going the other way - see plantFor, and
					// plantBack off a backpedal - and up to speed out of it.)
					m.plantFor(spot);
					const v0 = paceOn(m.last, m.at, spot);
					const due = swats ? catchAt - BREAK_UP_EARLY_MS : catchAt + 150;
					// (Near it, time to spare: there on the break all the same -
					// pulled up as he gets there - and broken down square to it,
					// not walked there the while.)
					const D = dist(m.at, spot);
					const fps = Math.min(
						m.top * CLOSE,
						Math.sqrt(STOP_ACCEL * STOP_HARD * D),
					);
					const ease = Math.min(
						D * 0.5,
						(fps * fps) / (2 * STOP_ACCEL * STOP_HARD),
					);
					const quick =
						m.t + rampedMs(D - ease, fps, v0) + ((2 * ease) / fps) * 1000;
					if (m.pullsPast(spot)) {
						// (On to it too quick to pull up there: on past it, pulled
						// up hard - and broken down square to it, time to spare.)
						if (due - m.t >= BREAK_SET_MS) {
							m.hold(due, "backerStance", this.toward(m.at, to));
						}
					} else if (
						D < ZONE_STIR_FT &&
						due - m.t >= BREAK_SET_MS &&
						legPace(m.last) < STOP_LEAST
					) {
						// (There already: broken down where he is.)
						m.hold(due, "backerStance", this.toward(m.at, to));
					} else if (fps > 0 && due >= quick) {
						// (Up to it on the break, pulled up there, and set square
						// to it the while - or, only a moment over, there that
						// much later, on his feet: see hold. A long time to spare:
						// in at the pace that has him there UNDER_SET_MS before it
						// - see UNDER_SPARE_MS.)
						const pace =
							due - quick > UNDER_SPARE_MS
								? this.underPace(D, (due - UNDER_SET_MS - m.t) / 1000, fps)
								: fps;
						m.runUp(spot, pace);
						const set =
							pace < fps
								? due - m.t >= BREAK_SET_MS
								: due - quick >= BREAK_SET_MS;
						m.hold(
							due,
							set ? "backerStance" : undefined,
							set ? this.toward(spot, to) : undefined,
						);
					} else {
						// (A step or two, no time to spare: quick steps, up to
						// it and on - not a stroll.)
						const t1 = Math.max(due, m.t + rampedMs(D, m.top, v0));
						m.go(
							spot,
							t1,
							D / Math.max(0.001, (t1 - m.t) / 1000) < WALK_MOST_FPS
								? "jog"
								: undefined,
						);
					}
				} else {
					this.rally(m, to, catchAt);
				}
			}
		}
	}

	// RUNNING TO THE BALL. Thrown a long way from him, he isn't stood
	// watching it in the air from where his part of the coverage left him:
	// on his way over to it as he reads it, at a run, to cut it off in front
	// of where it comes down - as far as that gets him by then (see
	// RALLY_PACE).
	private rally(m: Actor, to: Pt, catchAt: number) {
		const due = catchAt + RALLY_ON_MS;
		if (due - m.t < RALLY_LEAST_MS || DOWN.has(m.last.move)) {
			return;
		}
		const c = this.of(to);
		const aim = this.w({ d: c.d + RALLY_AHEAD_FT, a: c.a });
		const fps =
			m.top * (dist(m.at, aim) > RALLY_FAR_FT ? RALLY_FAR_PACE : RALLY_PACE);
		// (Planted down to RALLY_TURN_FPS for it, at the most, and round -
		// not cut at speed: there is no hurry, it is a long way off.)
		m.plantFor(aim, RALLY_TURN_FPS);
		// (Pulled up for it as it comes down, no time to be off to it after
		// all: eased off there - not left going at it to stop dead.)
		if (due - m.t < RALLY_LEAST_MS / 2) {
			m.easeOff(EASE_OFF_FT);
			return;
		}
		m.chase(aim, due, fps);
	}

	// ---- a run ------------------------------------------------------------------

	runPlay(handoffI: number, runI: number) {
		const { snap } = this.input;
		const r = this.r;
		const e = this.ev(runI);
		const qb = this.qb();
		const carrier = this.man(this.statPids("rus")[0]) ?? qb;
		const has = this.snapTo(qb);
		const yds = typeof e.yds === "number" ? e.yds : 0;
		// As a rule the play goes the way of the man the sim says made the
		// tackle: he is the one at the hole.
		const ta = this.tacklerA(carrier, snap);
		const strong =
			ta !== undefined && Math.abs(ta) > 3 && r() < 0.8
				? Math.sign(ta)
				: r() < 0.5
					? -1
					: 1;
		const gun = !!this.input.set?.gun;
		// The hole: inside zone between the guards and tackles, outside
		// zone or a toss to the edge (a sweep, to the side it comes from),
		// the sneak right over the center.
		const sneak = carrier === qb && yds <= 2 && !gun;
		// (Out of bounds at the end of it: as a rule a run to the edge.)
		const toEdge = !sneak && !!this.input.out && yds < 15;
		const outside = !sneak && (toEdge || r() < 0.35);
		const cs = this.of(carrier.at);
		const sweep = spotOf(carrier.role) !== "back" && carrier !== qb;
		// A toss to the edge, now and then, under center - to a back lined up
		// deep enough to take it out in front of him on his way there.
		const pitch =
			outside &&
			!gun &&
			!sweep &&
			carrier !== qb &&
			cs.d <= -PITCH_DEEP &&
			countOf(carrier, this.input.key, 80) < PITCH;
		// Or the option out of the gun: down the line with it, the back out
		// wide of him, and pitched to him on the run.
		const q0 = this.of(qb.at);
		const option =
			outside &&
			gun &&
			!sweep &&
			carrier !== qb &&
			spotOf(carrier.role) === "back" &&
			Math.sign(cs.a - q0.a) === strong &&
			Math.abs(cs.a - q0.a) <= OPTION_BESIDE[0] &&
			Math.abs(cs.d - q0.d) <= OPTION_BESIDE[1] &&
			countOf(carrier, this.input.key, 124) < OPTION;
		const optionAt = this.w({
			d: q0.d + OPTION_UP,
			a: q0.a + strong * OPTION_ALONG,
		});
		// The quarterback keeping it (not a sneak), as a rule off a ride: the
		// read option in the gun, a fake to the back under center - the back
		// one way, and he the other, round the edge. (Under center, always:
		// the back too far behind him to get out in front of him.)
		const rider =
			carrier === qb && !sneak && (r() < QB_RIDE || !gun)
				? this.o.find((m) => m.role === "RB" && !this.done.has(m.pid))
				: undefined;
		const holeA = sneak
			? (r() < 0.5 ? -1 : 1) * SNEAK_GAP_A
			: sweep
				? -Math.sign(cs.a || 1) * (18 + r() * 6)
				: rider
					? strong * (RIDE_EDGE + r() * 6)
					: strong * (outside ? 15 + r() * 6 : 2.5 + r() * 7);
		const blockersBack = this.statPids("rba", "rbw");
		// Up front: a sneak, outside zone on a run to the edge - or inside,
		// zone, or now and then power.
		const picked: RunScheme = sneak
			? "sneak"
			: outside || sweep
				? "stretch"
				: r() < POWER
					? "power"
					: "zone";
		// Or now and then, inside, a draw (see DRAW): handed off on his side
		// of the quarterback once it looks like a pass - zone up front.
		const passingDown =
			(this.input.down ?? 1) >= 2 && (this.input.toGo ?? 10) >= DRAW_TO_GO;
		const draw =
			gun &&
			(picked === "zone" || picked === "power") &&
			!rider &&
			carrier !== qb &&
			spotOf(carrier.role) === "back" &&
			Math.abs(cs.d - q0.d) <= DRAW_BESIDE &&
			Math.abs(cs.a - q0.a) >= MESH_APART &&
			countOf(carrier, this.input.key, 143) < (passingDown ? DRAW_LONG : DRAW);
		const scheme: RunScheme = draw ? "zone" : picked;
		// On inside zone, now and then, the play made to go the other way -
		// the line stepping that way, the back handed it that side and
		// pressing it - and him cutting back behind it to the hole.
		const cutback =
			scheme === "zone" &&
			!draw &&
			carrier !== qb &&
			countOf(carrier, this.input.key, 81) < CUTBACK;
		const flow = cutback ? -strong : strong;
		// Or, on power, now and then a counter (see COUNTER).
		const counter =
			scheme === "power" &&
			carrier !== qb &&
			!sweep &&
			spotOf(carrier.role) === "back" &&
			cs.d <= -COUNTER_DEEP &&
			countOf(carrier, this.input.key, 145) < COUNTER;

		// The quarterback: a reverse pivot under center to hand it off
		// deep, or a ride in the gun - and on with his fake. A receiver
		// coming round for it on a sweep takes it behind him.
		const qs = this.of(qb.at);
		const side = sweep
			? cs.a > 0
				? 1
				: -1
			: draw
				? Math.sign(cs.a - qs.a) || flow
				: flow;
		// (In the gun, a back from the far side of him: across in front of
		// him to it, the quarterback stood where he is - not through him
		// to a mesh out the other side of him: see GUN_CROSS_UP.)
		const crosses =
			gun &&
			carrier !== qb &&
			!pitch &&
			!option &&
			!sweep &&
			!draw &&
			(cs.a - qs.a) * side < 0;
		const meshPt =
			carrier === qb
				? qb.at
				: pitch
					? this.w({ d: cs.d + PITCH_UP, a: cs.a + strong * PITCH_OUT })
					: option
						? this.w({
								d: q0.d + OPTION_UP - OPTION_BEHIND,
								a: q0.a + strong * (OPTION_ALONG + OPTION_OUT),
							})
						: this.w(
								sweep
									? { d: gun ? qs.d + 2 : qs.d - 2.5, a: side * 1.5 }
									: draw
										? // (Up and in a step from his set beside him.)
											{ d: qs.d + DRAW_MESH_UP, a: cs.a - side * DRAW_MESH_IN }
										: crosses
											? {
													d: qs.d + GUN_CROSS_UP,
													a: qs.a + side * GUN_CROSS_ALONG,
												}
											: gun
												? { d: qs.d + 0.5, a: side * 2.5 }
												: underMesh(qs, cs, side),
							);
		const qbMesh = crosses
			? qb.at
			: this.w({
					...this.of(meshPt),
					a: this.of(meshPt).a - side * MESH_APART,
				});
		const lead = cs.a === 0 && cs.d < -10 ? 0 : 150;
		let mesh =
			carrier === qb
				? has
				: sweep
					? // (No sooner than he can step over to it with it - not
						// thrown across to it in a blink.)
						has +
						Math.max(
							(gun ? 150 : 250) + r() * 100,
							HAND_SET_MS + qbStepMs(dist(qb.at, qbMesh)),
						)
					: Math.max(
							has + 180 + qbStepMs(dist(qb.at, qbMesh)) + r() * 150,
							// (Out of his stance to it, up to speed: see the rider.)
							snap +
								lead +
								rampedMs(
									dist(carrier.at, meshPt),
									carrier.top * 0.75,
									0,
									RELEASE_ACCEL,
								),
						);
		// (A draw: the quarterback back into his drop, the ball up, a look
		// down the field - and then out to the back with it.)
		const dropPt = this.w({ d: qs.d - DRAW_DROP_FT, a: qs.a });
		const dropMs = Math.max(
			300 + DRAW_DROP_FT * 55,
			(DRAW_DROP_FT / QB_DROP_FPS) * 1000 + 150,
		);
		const looked =
			has +
			dropMs +
			DRAW_LOOK_MS[0] +
			(DRAW_LOOK_MS[1] - DRAW_LOOK_MS[0]) * countOf(qb, this.input.key, 144);
		if (draw) {
			mesh = Math.max(
				mesh,
				looked + qbStepMs(dist(dropPt, qbMesh)) + HAND_OUT_MS,
			);
		}
		// (A counter: the back's jab step the other way first.)
		const jabPt = this.w({ d: cs.d + 0.3, a: cs.a - side * COUNTER_JAB_FT });
		const jabbed = snap + lead + COUNTER_JAB_MS;
		if (counter) {
			mesh = Math.max(
				mesh,
				jabbed +
					rampedMs(dist(jabPt, meshPt), carrier.top * 0.75, 0, RELEASE_ACCEL),
			);
		}
		if (sweep) {
			// In motion before the snap, across behind the line, to take it
			// on the run just as the quarterback has it - going as soon as he
			// has been set a moment, if it is a long way.
			const set = carrier.legs.findLast((l) => l.t0 < snap) ?? carrier.last;
			// (Set from when he got there - stood up first, if he was, and
			// down in his stance after: see director.ts lineDown.)
			let k = carrier.legs.indexOf(set);
			while (
				k > 0 &&
				moveKind(carrier.legs[k - 1]!.move) === "still" &&
				carrier.legs[k - 1]!.to.x === set.from.x &&
				carrier.legs[k - 1]!.to.y === set.from.y
			) {
				k -= 1;
			}
			const since = carrier.legs[k]?.t0 ?? set.t0;
			const far = dist(set.from, meshPt);
			// (Up to speed out of his stance - and, set only just in time,
			// no quicker across than a man goes in motion, the handoff
			// later for it: see JET_FPS.)
			const go = Math.max(
				since + 300,
				mesh - rampedMs(far, JET_FPS, 0, RELEASE_ACCEL),
			);
			mesh = Math.max(
				mesh,
				go +
					rampedMs(
						far,
						Math.min(carrier.top * 0.85, JET_MOST_FPS),
						0,
						RELEASE_ACCEL,
					),
			);
			carrier.cut(go);
			carrier.go(meshPt, mesh, undefined, this.toward(carrier.at, meshPt));
		}
		if (handoffI >= 0 && carrier !== qb) {
			this.at.set(handoffI, mesh);
		}
		let optionGo = 0;
		if (option) {
			// Down the line with it, the back out wide of him: pitched to him
			// as he gets there, in his stride - let go as soon as he is
			// there, or later, the back too far off it to be there to meet
			// it - and eased up after it.
			const from: Pt3 = { ...optionAt, z: PITCH_FROM_Z };
			const to: Pt3 = { ...meshPt, z: CATCH_Z };
			const flight = (dist(from, to) / PITCH_FPS) * 1000;
			const reach = rampedMs(
				dist(carrier.at, meshPt),
				carrier.top * PITCH_PACE,
				0,
				RELEASE_ACCEL,
			);
			const hasIt = Math.max(has, qb.t);
			const letGo = Math.max(
				hasIt +
					rampedMs(
						dist(qb.at, optionAt),
						qb.top * OPTION_PACE,
						0,
						RELEASE_ACCEL,
					),
				snap + lead + reach - flight,
			);
			mesh = letGo + flight;
			optionGo = letGo;
			if (handoffI >= 0) {
				this.at.set(handoffI, letGo);
			}
			qb.getOff(optionAt, letGo, 0, RELEASE_ACCEL, "run");
			const v = legPace(qb.last);
			const way = this.toward(qb.last.from, optionAt);
			const on = (v * PITCH_MS) / 2000;
			qb.go(
				{
					x: optionAt.x + Math.cos(way) * on,
					y: optionAt.y + Math.sin(way) * on,
				},
				letGo + PITCH_MS / 2,
			);
			qb.arms("pitch", letGo - PITCH_MS / 2, letGo + PITCH_MS / 2);
			qb.easeOff(OPTION_EASE_FT);
			qb.hold(mesh + 2000, "stand");
			this.done.add(qb.pid);
			this.ball.push(
				{ kind: "fly", t0: letGo, t1: mesh, from, to, peak: PITCH_PEAK },
				{ kind: "held", t0: mesh, pid: carrier.pid },
			);
			// The back off with the snap, out wide of him and there as it
			// comes, his hands up for it.
			carrier.hold(snap + lead);
			carrier.getOff(meshPt, mesh, 0, RELEASE_ACCEL, "run");
			carrier.pin(mesh);
			this.reaches.push({
				m: carrier,
				move: "catch",
				t0: mesh - CATCH_REACH_MS,
				t1: mesh + CATCH_IN_MS,
			});
		} else if (pitch) {
			// The quarterback opened up to him, a step back his way, and the
			// ball tossed out in front of him underhand, to take on the move:
			// let go as soon as he is there - or later, the back too far off
			// it to be there to meet it.
			const open = this.w({ d: qs.d - 2.5, a: qs.a + strong * 1.5 });
			const face = this.toward(open, meshPt);
			const from: Pt3 = {
				x: open.x + Math.cos(face) * PITCH_FROM_FT,
				y: open.y + Math.sin(face) * PITCH_FROM_FT,
				z: PITCH_FROM_Z,
			};
			const to: Pt3 = { ...meshPt, z: CATCH_Z };
			const flight = (dist(from, to) / PITCH_FPS) * 1000;
			const reach = rampedMs(
				dist(carrier.at, meshPt),
				carrier.top * PITCH_PACE,
				0,
				RELEASE_ACCEL,
			);
			const letGo = Math.max(
				Math.max(has, qb.t) +
					Math.max(
						QB_STEP_LEAST_MS,
						(dist(qb.at, open) / PITCH_OPEN_FPS) * 1000,
					) +
					PITCH_MS / 2,
				snap + lead + reach - flight,
			);
			mesh = letGo + flight;
			if (handoffI >= 0) {
				this.at.set(handoffI, letGo);
			}
			qb.go(open, letGo - PITCH_MS / 2, "slide", face);
			qb.act("pitch", letGo + PITCH_MS / 2, face);
			// (And off the other way after it, out of the back's way - a
			// bootleg sold.)
			qb.run(
				this.w({ d: qs.d - PITCH_BOOT[0], a: qs.a - strong * PITCH_BOOT[1] }),
				QB_STEP_FPS * 0.8,
			);
			qb.hold(mesh + 2000, "stand");
			this.done.add(qb.pid);
			this.ball.push(
				{ kind: "fly", t0: letGo, t1: mesh, from, to, peak: PITCH_PEAK },
				{ kind: "held", t0: mesh, pid: carrier.pid },
			);
			// The back off as late as has him there at his pace, his hands
			// up for it as it comes.
			carrier.hold(Math.max(snap + lead, mesh - reach));
			carrier.getOff(meshPt, mesh, 0, RELEASE_ACCEL, "run");
			carrier.pin(mesh);
			this.reaches.push({
				m: carrier,
				move: "catch",
				t0: mesh - CATCH_REACH_MS,
				t1: mesh + CATCH_IN_MS,
			});
		} else if (carrier !== qb) {
			// (His arms out with it as he comes the last of the way there: out
			// in front of him as the back gets to it - into his belly, a
			// little ahead of where he is then.)
			const from = sweep ? carrier.last.from : carrier.at;
			const far = dist(from, meshPt);
			const face = this.toward(
				qbMesh,
				far > 0.5
					? {
							x: meshPt.x + ((meshPt.x - from.x) / far) * MESH_AHEAD,
							y: meshPt.y + ((meshPt.y - from.y) / far) * MESH_AHEAD,
						}
					: meshPt,
			);
			const set = mesh - HAND_SET_MS;
			if (draw) {
				qb.go(dropPt, Math.max(has, qb.t) + dropMs, "back", this.ahead);
				qb.hold(
					mesh - HAND_OUT_MS - qbStepMs(dist(dropPt, qbMesh)),
					"pocket",
					this.ahead,
				);
			}
			// (Only a blink of a step before his arms go out with it: out
			// from the first of it, not a sliver of a step between.)
			const reach = Math.min(set, Math.max(qb.t + 1, mesh - HAND_OUT_MS));
			const out = reach - qb.t < FLICK_MS ? qb.t + 1 : reach;
			const most = set - qb.t;
			const u = most > 0 ? (out - qb.t) / most : 1;
			if (out > qb.t + 1) {
				qb.go(
					{
						x: qb.at.x + (qbMesh.x - qb.at.x) * u,
						y: qb.at.y + (qbMesh.y - qb.at.y) * u,
					},
					out,
					"slide",
					face,
				);
			}
			const there = (set - out) / (mesh + 160 - out);
			qb.act("handOff", set, face, qbMesh);
			qb.last.span = [0, there];
			qb.act("handOff", mesh + 160, face);
			qb.last.span = [there, 1];
			if (draw) {
				// (Back on with his look, as if he still had it to throw.)
				qb.go(
					this.w({
						d: this.of(qbMesh).d - DRAW_FAKE_FT,
						a: this.of(qbMesh).a,
					}),
					mesh + 900,
					"back",
					this.ahead,
				);
				qb.hold(mesh + 1600, "pocket", this.ahead);
			} else {
				qb.go(
					this.w({
						d: this.of(qbMesh).d - 6,
						a: this.of(qbMesh).a - flow * 9,
					}),
					mesh + 1400,
					"run",
				);
			}
			qb.hold(mesh + 2400, "stand");
			this.done.add(qb.pid);
			let drawGo = 0;
			if (draw) {
				// The back up into his set beside the quarterback as if to
				// block - and across to the mesh as it comes.
				const c0 = this.of(carrier.at);
				const setPt = this.w({ d: c0.d + 0.5, a: c0.a });
				carrier.hold(snap + lead);
				carrier.go(setPt, snap + 550, "passSet", this.ahead);
				drawGo =
					mesh -
					rampedMs(dist(setPt, meshPt), carrier.top * 0.75, 0, RELEASE_ACCEL);
				carrier.hold(drawGo, "passSet", this.ahead);
				carrier.getOff(meshPt, mesh, 0, RELEASE_ACCEL, "run");
			} else if (counter) {
				// The back's jab step away, planted, and back to the mesh.
				carrier.hold(snap + lead);
				carrier.go(jabPt, jabbed, "shuffle", this.ahead);
				carrier.getOff(meshPt, mesh, 0, RELEASE_ACCEL, "run");
			} else if (!sweep) {
				// The back to the mesh.
				carrier.hold(snap + lead);
				carrier.getOff(meshPt, mesh, 0, RELEASE_ACCEL, "run");
			}
			// (Into his hands as they come to the quarterback's: just short of
			// the mesh, out in front of him - at it, going by flat out on a
			// sweep: short of it, he is still well off him.)
			const take = mesh - (sweep ? 0 : TAKE_LEAD_MS);
			this.ball.push({ kind: "held", t0: take, pid: carrier.pid });
			carrier.pin(take);
			// (The back's arms open round his belly for it as he comes to it,
			// the top one on the quarterback's side - and shut round it.)
			const ox = qbMesh.x - meshPt.x;
			const oy = qbMesh.y - meshPt.y;
			const left = ox * (meshPt.y - from.y) - oy * (meshPt.x - from.x) > 0;
			this.reaches.push({
				m: carrier,
				move: left ? "takeL" : "takeR",
				t0: Math.max(
					mesh - TAKE_BEFORE_MS,
					sweep ? carrier.last.t0 : draw ? drawGo : snap + lead + 100,
				),
				t1: mesh + TAKE_AFTER_MS,
			});
		}

		if (rider) {
			const way = -strong;
			const lane = this.w(
				gun ? { d: qs.d + 0.5, a: way * 2.5 } : { d: qs.d - 7, a: way * 3 },
			);
			// (In the gun, from the far side of him: across in front of him
			// to it, the quarterback stood where he is - see GUN_CROSS_UP -
			// not through him to a mesh out the other side of him; and on
			// from there to where he would have gone.)
			const rideCross = gun && (this.of(rider.at).a - qs.a) * way < 0;
			const riderMesh = rideCross
				? this.w({ d: qs.d + GUN_CROSS_UP, a: qs.a + way * GUN_CROSS_ALONG })
				: lane;
			const besideRide = this.w({
				...this.of(lane),
				a: this.of(lane).a - way * MESH_APART,
			});
			const qbRide = rideCross ? qb.at : besideRide;
			// (Out of his stance to it, up to speed: see the back below. When
			// as if beside him - across in front of him, the same.)
			const reachOf = (p: Pt) =>
				rampedMs(dist(rider.at, p), rider.top * 0.75, 0, RELEASE_ACCEL);
			const reach = reachOf(riderMesh);
			const rideT = Math.max(
				Math.max(has, qb.t) +
					RIDE_MS / 2 +
					qbStepMs(dist(qb.at, besideRide)) +
					r() * 150,
				snap + 150 + reachOf(lane),
			);
			qb.go(
				qbRide,
				rideT - RIDE_MS / 2,
				"slide",
				this.toward(qbRide, riderMesh),
			);
			qb.act("handOff", rideT + RIDE_MS / 2, this.toward(qbRide, riderMesh));
			// (The back on with his fake into the line, and pulled up: off
			// out of his stance as late as gets him to it up to speed - not
			// walked there and away at a run - and up to his pace on past it.)
			rider.hold(rideT - reach);
			rider.getOff(riderMesh, rideT, 0, RELEASE_ACCEL, "run");
			// (Pulled up behind his line, though - not on into the backs of
			// the men blocking for it.)
			const rm = this.of(lane);
			rider.runUp(
				this.w({
					d: Math.min(rm.d + RIDE_PAST, -RIDE_SHORT_FT),
					a: rm.a + way * 3,
				}),
				rider.top * 0.7,
			);
			rider.hold(rider.t + 600, "stand");
			rider.arms("fakeCarry", rideT - 200, rideT + 500);
			this.done.add(rider.pid);
		}

		// Through the hole and on to where the sim says he was brought
		// down.
		let across =
			holeA + (r() - 0.5) * (yds > 8 ? 30 : sneak ? SNEAK_SPREAD : 8);
		if (ta !== undefined && !sneak) {
			across += (ta - across) * (yds > 8 ? 0.5 : 0.25);
		}
		const end = this.endOf(
			yds,
			across,
			e.td === true,
			e.safety === true,
			sneak
				? undefined
				: this.outSide(
						toEdge
							? holeA > 0
								? this.bounds.right
								: this.bounds.left
							: across,
						Math.max(OUT_FAR, yds * YD * 0.8),
					),
		);
		if (end.out) {
			carrier.roam = OUT_MOST + 1;
		}
		const holePt = this.w({ d: sneak ? 1 : 0.5, a: holeA });
		let t = carrier.t;
		const endD = this.of(end.pt).d;
		if (sneak) {
			// Behind the center, into the pile - over into the gap beside
			// him first, and on up it: not straight through him.
			carrier.hold(has + 150, "qbSet", this.ahead);
			const gap = this.w({ d: SNEAK_GAP_D, a: this.of(end.pt).a });
			const all = dist(carrier.at, gap) + dist(gap, end.pt);
			// (No quicker than SNEAK_FPS over it, and up to it off his set -
			// see goOff - not off it at a sprint's pace in a blink.)
			const by = Math.max(
				snap + 1300,
				carrier.t + 600,
				carrier.t + (all / SNEAK_FPS) * 1000,
			);
			if (endD > 0 && all > 0) {
				carrier.goOff(
					gap,
					carrier.t + ((by - carrier.t) * dist(carrier.at, gap)) / all,
					"drive",
					this.ahead,
				);
			}
			carrier.go(end.pt, by, "drive", this.ahead);
			t = carrier.t;
		} else if (
			endD < 0.5 &&
			dist(carrier.at, end.pt) < dist(carrier.at, holePt)
		) {
			// Met in the backfield.
			t = carrier.run(end.pt, carrier.top * 0.6);
		} else {
			// Pressing the hole, patient - then through it (on a sweep or a
			// keeper, flat out for the edge; on outside zone, hard for it).
			// (On a cutback, the way the play goes first, and planted and
			// back across.)
			if (cutback) {
				carrier.run(
					this.w({ d: -CUTBACK_PRESS[0], a: -strong * CUTBACK_PRESS[1] }),
					carrier.top * 0.6,
				);
			}
			carrier.run(
				holePt,
				carrier.top *
					(sweep || rider || pitch || option ? 0.88 : outside ? 0.76 : 0.64),
			);
			t = carrier.run(
				end.pt,
				carrier.top * (yds > 3 ? 0.88 : 0.55),
				yds > 4 ? "sprint" : "run",
			);
		}

		// Who brings him down there, and when they can; the blocking until
		// then - the line firing out into the men over them, the fullback
		// leading through the hole.
		const down = end.td
			? { by: [], t }
			: this.downBy(carrier, end.pt, t, this.d);
		// (The option: the end man left to the quarterback - see optionEnd.)
		const ender = option
			? this.optionEnd(qb, strong, optionGo, down.by)
			: undefined;
		// (Short and up the middle, like as not a pile on him - or, in from
		// right there, over the top of it.)
		const short = sneak || yds <= 1 || end.td;
		const piled =
			!end.out &&
			scheme !== "stretch" &&
			yds <= PILE_YDS &&
			r() < (end.td ? OVER_TOP : short ? PILE_SHORT : PILE);
		const blocks = this.runBlock(
			carrier,
			holeA,
			end.pt,
			down.t,
			blockersBack,
			scheme,
			down.by,
			!piled,
			flow,
			draw,
			counter,
		);
		if (piled) {
			this.heap = short ? PILE_IN.length : PILE_SMALL;
		}
		this.endCarry(
			runI,
			carrier,
			end,
			down.t,
			this.d,
			end.td ? undefined : down.by,
		);
		if (ender) {
			this.optionHit(ender, qb, optionGo, end.pt);
		}
		this.letUp(blocks.men, blocks.until);
		this.heap = 0;
	}

	// The end man on the option, on its own count (OPTION_HIT): the last
	// man on the line on the side it goes, left to the quarterback - not
	// blocked - if he is not one to bring the back down, nor named for
	// anything else on it, nor hurt on it; nor the quarterback.
	optionEnd(
		qb: Actor,
		strong: number,
		letGo: number,
		tacklers: Actor[],
	): Actor | undefined {
		const { snap, events, i0, i1, stats } = this.input;
		if (countOf(qb, this.input.key, 126) >= OPTION_HIT || letGo <= snap) {
			return undefined;
		}
		const named = new Set(
			stats.filter((x) => x.i > this.lo && x.i <= this.hi).map((x) => x.pid),
		);
		for (let i = i0; i < i1; i++) {
			const e = events[i];
			if (e?.type === "injury") {
				named.add(e.injuredPID);
			}
		}
		if (named.has(qb.pid)) {
			return undefined;
		}
		const m = this.d
			.filter((x) => {
				const p = this.of(x.at);
				return (
					(DL.has(x.role) || LB.has(x.role)) &&
					!this.done.has(x.pid) &&
					x.t <= snap + 50 &&
					p.d <= OPTION_HIT_LINE &&
					Math.sign(p.a) === strong
				);
			})
			.sort((x, y) => Math.abs(this.of(y.at).a) - Math.abs(this.of(x.at).a))[0];
		if (!m || tacklers.includes(m) || named.has(m.pid)) {
			return undefined;
		}
		this.done.add(m.pid);
		return m;
	}

	// On through the quarterback he hits at `hitT`, `way` - round off him
	// to the side, `peel`, as he goes: QB_HIT_THROUGH_FT over
	// QB_HIT_THROUGH_MS, or - hitting him at a run - eased off the pace he
	// hits him at to a stand, no harder than QB_HIT_STOP_ACCEL, over the
	// ground that takes: not stopped in a stride by him.
	private hitThrough(m: Actor, way: number, peel: number, hitT: number) {
		const v = legPace(m.last);
		const ms = Math.max(QB_HIT_THROUGH_MS, (v / QB_HIT_STOP_ACCEL) * 1000);
		const ft = Math.max(QB_HIT_THROUGH_FT, (v * ms) / 2000);
		const face = (way + peel) / 2;
		const n = ft > QB_HIT_THROUGH_FT ? QB_HIT_THROUGH_STEPS : 1;
		let at = m.at;
		for (let k = 1; k <= n; k++) {
			const u0 = (k - 1) / n;
			const u1 = k / n;
			const s = ft * (u1 * (2 - u1) - u0 * (2 - u0));
			const dir =
				n === 1 ? peel : way + (peel - way) * Math.min(1, (2 * k - 1) / n);
			at = { x: at.x + Math.cos(dir) * s, y: at.y + Math.sin(dir) * s };
			m.go(at, hitT + ms * u1, undefined, face);
		}
	}

	// Him at the quarterback: off the snap a step up the field and in -
	// squeezed down, reading it - then at him as it goes, into him just
	// after it is gone, and on through; the quarterback put down and up
	// again. Then after the ball (see trail).
	optionHit(m: Actor, qb: Actor, letGo: number, endPt: Pt) {
		const hitT =
			letGo +
			OPTION_HIT_MS[0] +
			(OPTION_HIT_MS[1] - OPTION_HIT_MS[0]) * countOf(m, this.input.key, 127);
		const q = qb.posAt(hitT);
		this.offSnap(m, READ_OFF_MS, READ_OFF_SPREAD_MS);
		const s = this.of(m.at);
		const sq = this.w({
			d: s.d - OPTION_SQUEEZE[0],
			a: s.a - Math.sign(s.a) * OPTION_SQUEEZE[1],
		});
		const into = this.toward(sq, q);
		const hit = {
			x: q.x - Math.cos(into) * QB_HIT_ON,
			y: q.y - Math.sin(into) * QB_HIT_ON,
		};
		const need = rampedMs(dist(sq, hit), m.top * QB_HIT_PACE, 0, GET_OFF_ACCEL);
		const sqBy = m.t + OPTION_SQUEEZE_MS;
		if (hitT - need >= sqBy + OPTION_READ_LEAST_MS) {
			m.go(sq, sqBy, undefined, this.toward(sq, qb.posAt(sqBy)));
			m.hold(
				hitT - need,
				"backerReady",
				this.toward(sq, qb.posAt(hitT - need)),
			);
		}
		const from = m.at;
		const fast = dist(from, hit) / ((hitT - m.t) / 1000) >= QB_HIT_RUN_FPS;
		m.getOff(hit, hitT, 0, GET_OFF_ACCEL, fast ? "run" : undefined);
		const way = this.toward(from, hit);
		const peel =
			way +
			QB_HIT_PEEL *
				(Math.sin(way + QB_HIT_PEEL) <= Math.sin(way - QB_HIT_PEEL) ? 1 : -1);
		this.hitThrough(m, way, peel, hitT);
		m.arms("shove", hitT - QB_HIT_SHOVE_MS[0], hitT + QB_HIT_SHOVE_MS[1]);
		// (Down on from where he was going, too - not stopped dead in it.)
		const ql = legAt(qb, hitT);
		const qv = Math.min(legPace(ql) * OPTION_HIT_CARRY_S, OPTION_HIT_CARRY_FT);
		const qw = Math.atan2(ql.to.y - ql.from.y, ql.to.x - ql.from.x);
		const down = {
			x: q.x + Math.cos(way) * QB_HIT_BACK_FT + Math.cos(qw) * qv,
			y: q.y + Math.sin(way) * QB_HIT_BACK_FT + Math.sin(qw) * qv,
		};
		m.hold(hitT + OPTION_HIT_SEE_MS, "stand", this.toward(m.at, down));
		this.trail(m, endPt);
		// (Him: down on his back, hit from in front - on his front from
		// behind - a moment, and up.)
		qb.cut(hitT);
		const turned = (a: number) =>
			Math.abs(Math.atan2(Math.sin(a - qb.yaw), Math.cos(a - qb.yaw)));
		const back = turned(way + Math.PI) <= turned(way) + QB_HIT_BACK_BIAS;
		qb.act(
			back ? "tackledBack" : "tackled",
			hitT + QB_HIT_FALL_MS,
			back ? way + Math.PI : way,
			down,
		);
		qb.hold(
			qb.t +
				QB_HIT_DOWN_MS +
				OPTION_DOWN_SPREAD_MS * countOf(qb, this.input.key, 128),
			back ? "supine" : "prone",
		);
		qb.act(back ? "getUpBack" : "getUp", qb.t + GET_UP_MS);
	}

	// Up front on a run, the sim's blocks: each man on the line on whoever
	// is over him - driving him back if the sim has him win it, pushed back
	// and beaten if not, the man he had off him in a moment to go after the
	// ball; one with nobody over him up to the next level, onto a
	// linebacker (or, none left, in on the double team beside him); on a
	// power play the backside guard pulled round to the hole and up it, as
	// a lead back goes up it, on whoever fills it; the receivers out onto
	// the corners and safeties. On a run `through` them (not into a pile),
	// out of the carrier's way as he comes by.
	runBlock(
		carrier: Actor,
		holeA: number,
		endPt: Pt,
		t: number,
		backBlockers: number[],
		scheme: RunScheme,
		tacklers: Actor[],
		through: boolean,
		flow?: number,
		draw = false,
		counter = false,
	) {
		const { snap } = this.input;
		const r = this.r;
		const won = new Set(this.statPids("rbw"));
		const beat = (b: Actor) => backBlockers.length === 0 || won.has(b.pid);
		const blockers = this.o.filter(
			(m) =>
				m !== carrier &&
				!this.done.has(m.pid) &&
				(OL.has(m.role) ||
					backBlockers.includes(m.pid) ||
					(backBlockers.length === 0 &&
						(m.role === "TE" || m.role === "TE2")) ||
					// (The quarterback keeping it, a back out in front of him.)
					(carrier.role === "QB" &&
						scheme !== "sneak" &&
						(m.role === "RB" || m.role === "FB"))),
		);
		const free = (m: Actor) => !this.done.has(m.pid) && !tacklers.includes(m);
		const drive = this.of(endPt).d > 3 ? 4 : this.of(endPt).d < 0 ? -2 : 1;
		const side = Math.sign(holeA) || 1;
		const until = t + UNTIL_PAST_MS;
		// (Stopped behind the line, never up to it: the hole filled when he
		// would have been there - not the blocks up through it put off
		// till he is down, walked there.)
		const reach = this.reachesAt(carrier, 0);
		const atHole = Math.min(
			reach,
			t,
			reach === Infinity ? snap + NO_HOLE_MS : Infinity,
		);
		// (Off a block to go after him as he goes by a man `d` deep, well
		// before he is down.)
		// (No such time: `late`, when it is over for him.)
		const offAt = (d: number, late = until) => {
			const by = this.reachesAt(carrier, d - 3);
			return by < t - 400
				? Math.min(t - 250, by + 150 + r() * OFF_SPREAD)
				: late;
		};
		const awayAt = (m: Actor, late: number) => {
			for (let c = snap + AWAY_LEAST_MS; c < Math.min(late, t - 400); c += 50) {
				if (dist(carrier.posAt(c), m.at) > AWAY_FT) {
					return c + 150 + OFF_SPREAD * countOf(m, this.input.key, 37);
				}
			}
			return late;
		};

		// The first step, all as one: across to the hole on zone (or the
		// way the play is made to go, `flow`, on a cutback), flat down the
		// line on outside zone, down away from the hole on power - and
		// straight ahead into the pile on a sneak.
		const pull =
			scheme === "power"
				? blockers.find(
						(b) =>
							(b.role === "LG" || b.role === "RG") &&
							Math.sign(this.of(b.at).a) === -side,
					)
				: undefined;
		const step =
			scheme === "stretch"
				? { d: 1.5, a: 4.5 * side }
				: scheme === "power"
					? { d: 3, a: -2 * side }
					: scheme === "sneak"
						? { d: 2.5, a: 0 }
						: { d: 3, a: 2 * (flow ?? side) };
		// (On a counter, the backside tackle after him.)
		const pull2 = counter
			? blockers.find(
					(b) =>
						(b.role === "LT" || b.role === "RT") &&
						Math.sign(this.of(b.at).a) === -side,
				)
			: undefined;
		const pulled = (b: Actor) => b === pull || b === pull2;
		const backs = blockers.filter((b) => !pulled(b) && this.of(b.at).d < -8);
		const front = blockers.filter((b) => !pulled(b) && !backs.includes(b));
		// (A wide one given the time it takes him - no quicker on it than
		// FIRST_STEP_FPS - not lunged across in half a second; and the man
		// over him met as he gets there.)
		const fired = new Map<Actor, number>();
		for (const b of front) {
			const s = this.of(b.at);
			const own = 1 + GET_OFF_STEP * (countOf(b, this.input.key, 1) - 0.5);
			this.offSnap(b, GET_OFF_MS, GET_OFF_SPREAD_MS);
			if (draw) {
				// (On a draw, back into his set as on a pass first.)
				const by = Math.max(
					snap + 650,
					b.t + Math.max(SET_LEAST_MS, (DRAW_SET_FT / SET_FPS) * 1000),
				);
				b.go(
					this.w({ d: s.d - DRAW_SET_FT * own, a: s.a }),
					by,
					"passSet",
					this.ahead,
				);
				fired.set(b, by);
				continue;
			}
			const to = this.w({
				d: s.d + (step.d * (drive > 1 ? 1 : 0.5) + drive * 0.3) * own,
				a: s.a + step.a * own,
			});
			const by = Math.max(
				snap + 550,
				b.t + (dist(b.at, to) / FIRST_STEP_FPS) * 1000,
			);
			b.go(to, by, "drive", this.ahead);
			fired.set(b, by);
		}

		// Who is on whom up front.
		const on = new Map<Actor, Actor[]>();
		const flats: { b: Actor; m: Actor; end: number; lands: Pt }[] = [];
		// (On a counter, the man the tackle has left behind him - nobody
		// else near enough to him to block him: up the field after it from
		// the back side, as the rest go after it - see pursue.)
		const dl = this.d.filter((m) => DL.has(m.role) && !this.done.has(m.pid));
		const left = (m: Actor) => {
			const near = closest(front, m.at);
			return (
				counter &&
				!!near &&
				!tacklers.includes(m) &&
				dist(near.at, m.at) > COUNTER_FREE_FT
			);
		};
		// (The rest each on a man up front: the pairs that between them go
		// the least way - see climbPairs - none much further off his man
		// than the nearest; not two of them across each other's way, each
		// to the man nearer the other after his first step. More of them
		// than men up front: the rest on the nearest.)
		const stepped = Math.max(snap, ...front.map((b) => b.t));
		const mates = climbPairs(
			dl.filter((m) => !left(m)),
			front,
			stepped,
			(m, b) => {
				const near = closest(front, m.at);
				return (
					!!near &&
					dist(m.at, b.at) <= dist(m.at, near.at) + FRONT_PAIR_SLACK_FT
				);
			},
		);
		for (const m of dl) {
			const b = mates.get(m) ?? closest(front, m.at);
			if (left(m)) {
				const p = this.of(m.at);
				this.offSnap(m, READ_OFF_MS, READ_OFF_SPREAD_MS);
				m.getOff(
					this.w({ d: p.d - COUNTER_FREE_UP, a: p.a + side * 1.5 }),
					snap + 750,
					0,
					GET_OFF_ACCEL,
					"run",
					GET_OFF_SQUARE_MS,
				);
				continue;
			}
			if (b) {
				on.set(b, [...(on.get(b) ?? []), m]);
			}
		}
		const offLater: Actor[] = [];
		for (const [b, men] of on) {
			const s = { ...this.of(b.at), t: b.t };
			const wins = beat(b);
			// (Let up on his own count once it is over - see countOf.)
			const till = until + RUN_END_SPREAD_MS * countOf(b, this.input.key, 4);
			const endO = this.of(endPt);
			const face =
				!through && Math.abs(s.a + step.a * 0.4 - endO.a) < PILE_FACE_A
					? Math.max(s.d, endO.d - PILE_FACE_FT)
					: Infinity;
			const path = wins
				? [
						{
							d: Math.min(s.d + drive + 1, face),
							a: s.a + step.a * 0.4,
							t: till,
						},
					]
				: [
						{ d: s.d - 1.6, a: s.a, t: snap + 1150 },
						{ d: s.d - 2.4, a: s.a, t: till },
					];
			// (Turned off the way the man with the ball comes through - over
			// to the side of it they are on by the time he is by, not stood in
			// it - on a run that gets past them, not one into a pile there.)
			const lay = (over: number, by: number) => {
				let was = s;
				for (const p of path) {
					if (was.t < by && by < p.t) {
						const u = (by - was.t) / (p.t - was.t);
						b.go(
							this.w({
								d: was.d + (p.d - was.d) * u,
								a: was.a + (p.a - was.a) * u + over,
							}),
							by,
							"drive",
							this.ahead,
						);
					}
					const u = clamp((p.t - s.t) / Math.max(1, by - s.t), 0, 1);
					b.go(this.w({ d: p.d, a: p.a + over * u }), p.t, "drive", this.ahead);
					was = p;
				}
			};
			lay(0, s.t);
			// (Into a pile too: out of his way as he comes up to it, not run
			// through by him on the way in.)
			const way = this.wayThrough(
				b,
				carrier,
				s.t,
				Math.min(until, t - CLEAR_DOWN_MS),
				[...on].flatMap(([o, them]) => (o === b ? [] : [o, ...them])),
			);
			if (way.over !== 0) {
				b.cut(s.t);
				lay(way.over, way.by);
			}
			let locked = till;
			let freed = snap;
			const hit = (fired.get(b) ?? snap + 550) - 100;
			for (const m of men) {
				const at = (time: number, lat: number) => {
					const p = this.of(b.posAt(time));
					return this.w({ d: p.d + BLOCK_GAP, a: p.a + lat });
				};
				// (Beaten, off him - if he is not the one to make the tackle,
				// who goes from wherever he is when he goes.)
				const shed = snap + SHED_MS * 1.6 + r() * SHED_SPREAD;
				const sheds = !wins && free(m) && shed <= t - 300;
				let end = !sheds
					? wins && free(m)
						? Math.min(offAt(this.of(m.at).d, till), awayAt(m, till))
						: till
					: shed;
				// (Off in time to be on him while he is - see fireInto. On a
				// draw, up the field after the quarterback, into a man set for
				// him - not as hard into him as on a run.)
				const pace = RUSH_HIT_PACE * (draw ? DRAW_HIT : 1);
				this.offSnap(
					m,
					READ_OFF_MS,
					READ_OFF_SPREAD_MS,
					end -
						FIRE_HELD_MS -
						rampedMs(dist(m.at, at(hit, 0)), m.top * pace, 0, GET_OFF_ACCEL),
				);
				// (Shed, but not before he has got to him and been on him a
				// while: a man well off him not sent flat out into him to be
				// off him again at once.)
				const lat = (r() - 0.5) * 0.6;
				if (sheds) {
					const aim = this.fireAim(m, (time) => at(time, lat), hit, pace);
					end = Math.min(till, Math.max(end, aim.t + aim.S + FIRE_HELD_MS));
				}
				this.fireInto(m, (time) => at(time, lat), hit, end, pace);
				// (Over with him, by when they are out of the way.)
				if (way.over !== 0 && m.t < way.by && way.by < end) {
					m.go(at(way.by, lat), way.by, "engage", this.behind);
				}
				m.go(at(end, (r() - 0.5) * 0.6), end, "engage", this.behind);
				locked = Math.min(locked, end);
				freed = Math.max(freed, end);
				// (Driven off his feet: see PANCAKE.)
				const p = this.of(m.at);
				const lands = this.w({ d: p.d + PANCAKE_BACK_FT, a: p.a });
				let clear = true;
				for (let c = end; c <= Math.max(end, t) && clear; c += 50) {
					clear = dist(carrier.posAt(c), lands) >= PANCAKE_CLEAR_FT;
				}
				// (Nor with another man there, to go on through it.)
				clear &&= this.men.every(
					(x) =>
						x === b || x === m || dist(x.posAt(end), lands) >= PANCAKE_CLEAR_FT,
				);
				if (
					wins &&
					clear &&
					won.has(b.pid) &&
					men.length === 1 &&
					OL.has(b.role) &&
					free(m) &&
					end - hit >= PANCAKE_DRIVE_MS &&
					end < till - FREED_MS &&
					countOf(b, this.input.key, 61) < PANCAKE
				) {
					flats.push({ b, m, end, lands });
				}
			}
			this.jostle([b, ...men], snap + JOSTLE_FROM, locked);
			// (Nobody left on him well before it is over: up out of it, and
			// after the ball at a jog, kept off it, on his own count - not
			// stood where he lost him till the whistle.)
			// (Laid once all the blocks are, below - for him to go round those
			// still going on: see alongWith.)
			if (freed < till - FREED_MS) {
				b.cut(freed + FREED_REACT);
				offLater.push(b);
			}
		}

		// The linebackers: a read step, then over to fill - but for one to
		// make the tackle, who goes from where he is.
		const backers = this.d.filter((m) => LB.has(m.role) && free(m));
		// (On a draw, back in their drops first, as on a pass.)
		const filled = snap + FILL_MS + (draw ? DRAW_LB_MS : 0);
		const fills: Pt[] = [];
		for (const m of backers) {
			const s = this.of(m.at);
			const fill = {
				d: Math.max(3, s.d - (drive > 1 ? 4 : 7)),
				a: holeA + (s.a - holeA) * 0.4,
			};
			const across = Math.abs(fill.a - s.a);
			// (Over to it behind the line - see FILL_SCRAPE_FT - not along it:
			// nor his read step up into it, if he is in nearer than that.)
			if (across > 5) {
				fill.d = Math.max(fill.d, FILL_SCRAPE_FT);
			}
			const read = across > 5 ? Math.min(s.d, FILL_SCRAPE_FT) : 0;
			this.offSnap(m, READ_STEP_MS, READ_STEP_SPREAD_MS);
			// (Back in his drop that much later than DRAW_LB_MS has it: over to
			// fill that much later too.)
			let late = 0;
			if (draw) {
				// (Back in it up to his pace and pulled up there - see stepTo
				// - over as long as that takes him, if it is longer: not off
				// it at a backpedal and stopped dead.)
				const drop = this.w({ d: s.d + DRAW_LB_FT, a: s.a });
				const per = 1 / (2 * RELEASE_ACCEL) + 1 / (2 * STOP_ACCEL);
				const least =
					m.t + 2 * Math.sqrt(per * DRAW_LB_FT) * 1000 + DRAW_LB_SPARE_MS;
				if (
					m.stepTo(
						drop,
						Math.max(snap + DRAW_LB_MS, least),
						BACK_MOST,
						"back",
						this.behind,
					)
				) {
					late = Math.max(0, least - (snap + DRAW_LB_MS));
				} else {
					m.go(drop, snap + DRAW_LB_MS, "back", this.behind);
				}
			} else {
				m.go(
					this.w({ d: Math.max(s.d - 1.5, read), a: s.a + side }),
					snap + 450,
					"slide",
					this.behind,
				);
			}
			const by = filled + late + r() * 250;
			const n0 = m.legs.length;
			const from = m.at;
			const lay = (to: Pt) => {
				// (Not far to go, across: set reading it a moment longer, and
				// over at a pace - not walked into the hole. Set for more than
				// a blink of it, between his read step and going.)
				const far = dist(m.at, to);
				const set = by - (far / FILL_LEAST_FPS) * 1000;
				if (across > 5 && set > m.t) {
					m.hold(
						Math.max(set, m.t + FILL_SET_LEAST),
						"backerReady",
						this.behind,
					);
				}
				const went = m.t;
				m.chase(
					to,
					by,
					m.top * FILL_PACE,
					across > 5 ? "run" : "slide",
					across > 5 ? undefined : this.behind,
				);
				// (Pulled up there, nobody on him yet - see STALK_EASE_FT. In
				// his low steps too, if he comes up quick in them.)
				if (across > 5) {
					this.shortOfBlocks(m, went);
				}
				if (across > 5 || legPace(m.last) >= STOP_LEAST) {
					m.easeOff(STALK_EASE_FT);
				}
			};
			// (Not filled where another of them already has: pulled up short
			// of him on his own side of it - see FILL_APART_FT - not run into
			// him and shoved off him. Nor in among the men in blocks there -
			// see FILL_ROOM_FT.)
			const crowded = () => {
				if (fills.some((p) => dist(p, m.at) < FILL_APART_FT)) {
					return true;
				}
				for (let t = m.t; t <= m.t + FILL_ROOM_MS; t += FILL_ROOM_EVERY_MS) {
					for (const x of this.men) {
						if (
							x !== m &&
							LINE_PLAY.has(legAt(x, t).move) &&
							dist(x.posAt(t), m.at) < FILL_ROOM_FT
						) {
							return true;
						}
					}
				}
				return false;
			};
			const to = this.w(fill);
			lay(to);
			for (const k of FILL_SHORT) {
				if (!crowded()) {
					break;
				}
				m.legs.splice(n0);
				lay({ x: to.x + (from.x - to.x) * k, y: to.y + (from.y - to.y) * k });
			}
			fills.push(m.at);
		}
		const taken = new Set<Actor>();
		const pick = (from: Pt) => {
			const m = closest(
				backers.filter((x) => !taken.has(x)),
				from,
				filled,
			);
			if (m) {
				taken.add(m);
			}
			return m;
		};

		// The puller and the lead back up through the hole, on whoever fills
		// it.
		const holePt = this.w({ d: 1, a: holeA });
		if (pull) {
			const s = this.of(pull.at);
			pull.hold(snap + 40);
			pull.go(this.w({ d: s.d - 1.8, a: s.a + side * 1.5 }), snap + 330, "run");
			const pre = this.w({ d: -1.5, a: holeA - side * 2.5 });
			const by = Math.max(atHole - PULL_AHEAD_MS, snap + 700);
			if (
				(dist(pull.at, pre) / Math.max(1, by - pull.t)) * 1000 <
				WALK_MOST_FPS
			) {
				// (Flat down the line at a run all the same - not walked there -
				// and set at the hole for the back.)
				// (Only a blink early: on through it, not set for a moment.)
				pull.run(pre, PULL_FPS, "run");
				if (by - pull.t >= FLICK_MS) {
					pull.setInto(by, "passSet", this.ahead);
				}
			} else {
				pull.chase(pre, by, pull.top * 0.9, "run");
			}
			pull.chase(
				this.w({ d: 2, a: holeA - side }),
				pull.t + 250,
				pull.top * 0.9,
				"run",
			);
			this.blockOn(
				pull,
				pick(holePt),
				beat(pull),
				offAt,
				until,
				carrier,
				through,
			);
		}
		if (pull2) {
			// Round behind the guard, a step deeper, and up through it after
			// him.
			const s = this.of(pull2.at);
			pull2.hold(snap + 90);
			pull2.go(
				this.w({ d: s.d - 1.8, a: s.a + side * 2.5 }),
				snap + 400,
				"run",
			);
			const pre = this.w({ d: -3, a: holeA - side * 4 });
			const by = Math.max(
				atHole - PULL_AHEAD_MS + COUNTER_AFTER_MS,
				snap + 900,
			);
			if (
				(dist(pull2.at, pre) / Math.max(1, by - pull2.t)) * 1000 <
				WALK_MOST_FPS
			) {
				pull2.run(pre, PULL_FPS, "run");
			} else {
				pull2.chase(pre, by, pull2.top * 0.9, "run");
			}
			pull2.chase(
				this.w({ d: 2, a: holeA - side * 0.5 }),
				pull2.t + 300,
				pull2.top * 0.9,
				"run",
			);
			this.blockOn(
				pull2,
				pick(holePt),
				beat(pull2),
				offAt,
				until,
				carrier,
				through,
			);
		}
		for (const b of backs) {
			b.hold(snap + 40);
			const lead = this.w({ d: -1, a: holeA - side });
			const fps = Math.min(b.top * 0.85, KEEP_LEAD_FPS);
			b.chase(
				lead,
				Math.max(
					carrier.role === "QB"
						? Math.min(
								atHole - 300,
								b.t +
									rampedMs(dist(b.at, lead), fps, paceOn(b.last, b.at, lead)),
							)
						: atHole - 300,
					snap + 500,
				),
				b.top * 0.85,
				"run",
			);
			this.blockOn(b, pick(holePt), beat(b), offAt, until, carrier, through);
		}
		// Nobody over him: up onto a linebacker - or, none left, in beside
		// the man on the nearest one, on him too. (The pairs that go the
		// least way between them - see climbPairs - not each in turn onto
		// the nearest left, a guard sent across behind the center to the
		// far one through the blocks there.)
		const ups = climbPairs(
			front.filter((b) => !on.has(b)),
			backers.filter((x) => !taken.has(x)),
			filled,
			(b, m) => this.climbClear(b, m, until),
		);
		for (const m of ups.values()) {
			taken.add(m);
		}
		for (const b of front) {
			if (on.has(b)) {
				continue;
			}
			const m = ups.get(b);
			if (m) {
				this.blockOn(b, m, beat(b), offAt, until, carrier, through);
				continue;
			}
			// (A backer left, but no way to him clear of the blocks between
			// them: up to the next level on his own side all the same, if
			// that is clear - not across through the blocks to him; or, not,
			// onto the nearest of them after all - not in beside a man where
			// the ball comes through.)
			if (backers.some((x) => !taken.has(x))) {
				const up = this.w({ ...this.of(b.at), d: this.of(b.at).d + 9 });
				if (
					this.blocksIn(b, b, b.at, [up], b.t, b.t + 900).worst <= CLIMB_INTO_FT
				) {
					this.blockOn(b, undefined, beat(b), offAt, until, carrier, through);
				} else {
					this.blockOn(b, pick(b.at), beat(b), offAt, until, carrier, through);
				}
				continue;
			}
			const near = (x: Actor) => dist(x.posAt(snap + 800), b.at);
			const mate = [...on.keys()]
				.filter((x) => near(x) <= DOUBLE_REACH_FT)
				.sort((x, y) => near(x) - near(y))[0];
			if (!mate) {
				b.go(
					this.w({ ...this.of(b.at), d: this.of(b.at).d + drive }),
					until,
					"drive",
					this.ahead,
				);
				continue;
			}
			const by = Math.sign(this.of(b.at).a - this.of(mate.at).a) || 1;
			const beside = (time: number) => {
				const p = this.of(mate.posAt(time));
				return this.w({ d: p.d - 0.4, a: p.a + by * DOUBLE_FT });
			};
			// (Not in beside him where the ball comes through, for the man
			// with it to run into: up to the next level instead, off its way
			// - see blockOn.)
			let lane = false;
			for (let c = snap + 800; c <= until && !lane; c += 50) {
				lane = dist(carrier.posAt(c), beside(c)) < BLOCK_CLEAR_FT;
			}
			if (lane) {
				this.blockOn(b, undefined, beat(b), offAt, until, carrier, through);
				continue;
			}
			// (Off it as he is - not on beside where he goes after it, the
			// man on him gone.)
			const upTo = Math.min(
				until,
				mate.legs.findLast((l) => l.move === "drive" || l.move === "engage")
					?.t1 ?? until,
			);
			// (Over to him square to the line: across in a slide, not run
			// there crabwise.)
			// (In push steps no quicker than a man takes them - not across
			// in a blink, back the way his first step took him. Nor that step
			// taken at all, if it takes him across past him, or away: straight
			// over instead.)
			const first = b.last;
			const over = this.of(beside(snap + 800));
			const was = this.of(first.from).a;
			const stepTo = this.of(first.to).a;
			if (first.t1 === fired.get(b) && (over.a - stepTo) * (stepTo - was) < 0) {
				b.cut(first.t0);
			}
			const at = this.of(b.at);
			const across = Math.abs(over.a - at.a) > Math.abs(over.d - at.d);
			const there = Math.max(
				snap + 800,
				b.t +
					(Math.hypot(over.a - at.a, over.d - at.d) /
						(across ? SHUFFLE_MOST : DOUBLE_DRIVE_FPS)) *
						1000,
			);
			b.chase(
				beside(there),
				there,
				b.top * 0.8,
				across ? "slide" : "drive",
				this.ahead,
			);
			if (upTo > b.t) {
				b.go(beside(upTo), upTo, "drive", this.ahead);
			}
		}
		// (Put on his back - see PANCAKE - once everybody's part round him is
		// laid out: nobody else on through where he goes down while he is
		// down, a man up onto a linebacker, say.)
		for (const { b, m, end, lands } of flats) {
			const upT = end + PANCAKE_FALL_MS + PANCAKE_DOWN_MS;
			const room = this.men.every((x) => {
				if (x === b || x === m) {
					return true;
				}
				for (let t = end; t <= upT; t += 50) {
					if (dist(x.posAt(t), lands) < PANCAKE_ROOM_FT) {
						return false;
					}
				}
				return true;
			});
			if (!room) {
				continue;
			}
			m.cut(end);
			m.act("tackledBack", end + PANCAKE_FALL_MS, this.behind, lands);
			m.hold(m.t + PANCAKE_DOWN_MS, "supine");
			m.act("getUpBack", m.t + GET_UP_MS);
			this.done.add(m.pid);
			b.cut(end);
			const q = this.of(b.at);
			b.go(
				this.w({ d: q.d + PANCAKE_ON_FT, a: q.a }),
				end + PANCAKE_FALL_MS,
				"drive",
				this.ahead,
			);
			// (Stood over him till he is up again - not on after the ball over
			// the top of him.)
			b.hold(Math.max(b.t, m.t), "stand", this.ahead);
			this.over.set(b, m.t);
			this.offBlock(b, until, 0);
		}

		// The receivers: out onto the nearest man off the ball, and on him -
		// or, nobody near, on downfield.
		const dbs = this.d.filter((m) => DB.has(m.role) && free(m));
		const stalks: Actor[] = [];
		for (const m of this.o) {
			if (this.done.has(m.pid) || m === carrier || blockers.includes(m)) {
				continue;
			}
			const s = this.of(m.at);
			this.offSnap(m, STALK_OFF_MS, STALK_OFF_SPREAD_MS);
			const db = closest(dbs, m.at);
			// (Off his line no quicker than he can get going out of his
			// stance - not shot off it from nothing to full tilt in a stride.)
			const release = (to: Pt, by: number) =>
				m.go(
					to,
					Math.max(
						by,
						m.t +
							rampedMs(dist(m.at, to), m.top * CLIMB_PACE, 0, RELEASE_ACCEL),
					),
					"run",
				);
			if (!db || dist(db.at, m.at) > STALK_FAR) {
				const went = m.t;
				// (Off to a spot clear of the men in blocks on his way there -
				// see RELEASE_CLEAR_OFF - not on through them.)
				const d = s.d + 9 + r() * 8;
				const by = snap + 1300;
				const tries = RELEASE_CLEAR_OFF.map((off) =>
					this.w({ d, a: s.a * 0.92 + off }),
				);
				const into = (to: Pt) => {
					const t1 = Math.max(
						by,
						m.t +
							rampedMs(dist(m.at, to), m.top * CLIMB_PACE, 0, RELEASE_ACCEL),
					);
					return this.blocksIn(m, m, m.at, [to], m.t, t1 + RELEASE_CLEAR_MS)
						.worst;
				};
				let to = tries[0]!;
				let worst = into(to);
				for (const p of tries.slice(1)) {
					if (worst <= CHASE_CLEAR_SLACK_FT) {
						break;
					}
					const w = into(p);
					if (w < worst - CHASE_CLEAR_SLACK_FT) {
						to = p;
						worst = w;
					}
				}
				release(to, by);
				// (Pulled up there, a step or two on - short of the men in blocks,
				// if that has him in them - and in toward it at a jog, not stood
				// out there till the whistle.)
				this.shortOfBlocks(m, went);
				m.easeOff(STALK_EASE_FT);
				const far = dist(m.at, endPt);
				const from =
					m.t +
					WIDE_IN_MS[0] +
					(WIDE_IN_MS[1] - WIDE_IN_MS[0]) * countOf(m, this.input.key, 27);
				const fps =
					WIDE_IN_FPS[0] +
					(WIDE_IN_FPS[1] - WIDE_IN_FPS[0]) * countOf(m, this.input.key, 28);
				const go = Math.min(
					far - WIDE_IN_SHORT_FT,
					(fps * (until - from)) / 1000,
				);
				if (go >= DOWNFIELD_LEAST_FT) {
					m.hold(from);
					m.runUp(
						{
							x: m.at.x + ((endPt.x - m.at.x) * go) / far,
							y: m.at.y + ((endPt.y - m.at.y) * go) / far,
						},
						fps,
					);
					m.hold(until);
				}
				// (Squared up for a man to block a moment, then stood watching
				// it - see watch - not set in it stock still till it is over.)
				m.hold(Math.min(until, m.t + STALK_SET_MS), "drive", this.ahead);
				m.hold(until, "stand", this.ahead);
				continue;
			}
			dbs.splice(dbs.indexOf(db), 1);
			// (He backs off at the snap, reading it, then up.)
			const c = this.of(db.at);
			this.offSnap(db, STALK_READ_MS, STALK_READ_SPREAD_MS);
			db.go(this.w({ d: c.d + 4, a: c.a }), snap + 750, "back", this.behind);
			// (Up in his low steps, as slow as that is - not walked up.)
			const up = this.w({ d: c.d + 1, a: c.a + (holeA - c.a) * 0.2 });
			db.chase(
				up,
				snap + 1600,
				db.top * 0.5,
				(dist(db.at, up) / Math.max(0.001, snap + 1600 - db.t)) * 1000 <
					WALK_MOST_FPS
					? "slide"
					: undefined,
				this.behind,
			);
			// (Pulled up there, a step or two on.)
			db.easeOff(STALK_EASE_FT);
			release(this.w({ d: s.d + 4, a: s.a + (c.a - s.a) * 0.3 }), snap + 650);
			this.blockOn(m, db, r() < STALK_WON, offAt, until, carrier, through);
			stalks.push(m);
		}
		// (The men up front whose man got off them, after it - see above.)
		for (const b of offLater) {
			this.offBlock(b, until, 0);
		}
		return { men: [...blockers, ...stalks], until };
	}

	// Men blocking on a run left pushing at nobody well before it is
	// over - the man on them gone off to make the tackle, say: up out of
	// it a beat after he is, and after the ball (see offBlock) - not stood
	// there till the whistle. (Not one since put to something else.)
	letUp(men: Actor[], until: number) {
		const { snap } = this.input;
		const pushing = (l: Leg) => l.move === "drive" || l.move === "engage";
		for (const b of men) {
			const end = b.legs.findLast(pushing)?.t1;
			if (
				end === undefined ||
				this.done.has(b.pid) ||
				b.legs.some((l) => l.t0 >= end && l.move !== "stand")
			) {
				continue;
			}
			const near = (c: number) =>
				this.d.some(
					(o) =>
						dist(o.posAt(c), b.posAt(c)) < AIR_BLOCK &&
						!DOWN.has(legAt(o, c).move),
				);
			let last = snap + LET_UP_FROM_MS;
			for (let c = end; c > last; c -= 50) {
				if (near(c)) {
					last = c;
					break;
				}
			}
			const off = last + FREED_REACT;
			if (
				off > end - FREED_MS ||
				b.legs.some((l) => l.t1 > off && !pushing(l) && l.move !== "stand")
			) {
				continue;
			}
			b.cut(off);
			this.offBlock(b, until, 0);
		}
		this.lookAfter(men);
	}

	// Men let up out of a block with no time to go after the ball before
	// the whistle (see offBlock - and so left in their block, then stood):
	// up out of it a beat after, turned to it and a step or two toward it -
	// not stood frozen where the block left them till the whistle.
	private lookAfter(men: Actor[]) {
		const upTo = this.whistle - LOOK_AFTER_SPARE_MS;
		for (const b of men) {
			const l = b.legs.findLast((x) => x.move !== "stand");
			if (
				!l ||
				this.done.has(b.pid) ||
				(l.move !== "drive" && l.move !== "engage") ||
				l.arms ||
				l.turn ||
				upTo - (l.t1 + FREED_REACT) < LOOK_AFTER_LEAST_MS
			) {
				continue;
			}
			const ball = this.ballAt(upTo);
			if (!ball) {
				continue;
			}
			b.cut(l.t1);
			b.hold(l.t1 + FREED_REACT);
			const t0 = b.t;
			const at = b.at;
			const far = dist(at, ball);
			const face = this.toward(at, ball);
			const ft = Math.min(
				LOOK_AFTER_FT[0] +
					(LOOK_AFTER_FT[1] - LOOK_AFTER_FT[0]) *
						countOf(b, this.input.key, 154),
				far - LOOK_AFTER_KEEP_FT,
			);
			const to = {
				x: at.x + ((ball.x - at.x) / Math.max(0.1, far)) * ft,
				y: at.y + ((ball.y - at.y) / Math.max(0.1, far)) * ft,
			};
			const clear =
				ft >= STEP_TO_LEAST_FT &&
				[0.5, 1, 1.4].every((u) => {
					const c = t0 + (upTo - t0) * u;
					const p = {
						x: at.x + (to.x - at.x) * Math.min(1, u),
						y: at.y + (to.y - at.y) * Math.min(1, u),
					};
					return this.men.every(
						(x) => x === b || dist(x.posAt(c), p) >= LOOK_AFTER_CLEAR_FT,
					);
				});
			if (!clear || !b.stepTo(to, upTo, LOOK_AFTER_FPS, undefined, face)) {
				b.hold(Math.min(upTo, t0 + LOOK_TURN_MS), "stand", face);
			}
		}
	}

	// (Off the snap on his own count, `ms` after it and up to `spread`
	// later - see countOf; the snapper as the ball leaves his hands. Not
	// after `latest`, if he has somewhere to be.)
	private offSnap(m: Actor, ms: number, spread: number, latest = Infinity) {
		const { snap } = this.input;
		const t = Math.min(
			latest,
			m.role === "C"
				? snap + SNAPPER_OFF_MS
				: snap + ms + spread * countOf(m, this.input.key),
		);
		if (t - m.t >= OFF_LEAST_MS) {
			m.hold(t);
		}
	}

	// A blocker onto a man off the line: as soon as he can be in front of
	// him, then on him - driving him back if he wins it; if not, the man
	// off him in a moment. Till `until` - or the man off it as the carrier
	// goes by, after him (and, `through` him, out of his way before then).
	// Pulled up into a block, as he would be on from where he is (see
	// easeOff): from far enough short of it to be pulled up where he is
	// instead, if that is clear of them (see FILL_ROOM_FT) - not on into
	// the men in it, and shoved out of them. Back no further than `from`.
	private shortOfBlocks(m: Actor, from: number) {
		const l = m.last;
		const far = dist(l.from, l.to);
		const v = legPace(l);
		const ease = Math.min(STALK_EASE_FT, (v * v) / (2 * STOP_ACCEL));
		if (far <= 0 || ease < 0.5) {
			return;
		}
		const at = m.at;
		const on = {
			x: at.x + ((l.to.x - l.from.x) / far) * ease,
			y: at.y + ((l.to.y - l.from.y) / far) * ease,
		};
		const blocked = (p: Pt, t0 = m.t) =>
			this.men.some((x) => {
				for (
					let t = t0;
					x !== m && t <= t0 + FILL_ROOM_MS;
					t += FILL_ROOM_EVERY_MS
				) {
					if (
						LINE_PLAY.has(legAt(x, t).move) &&
						dist(x.posAt(t), p) < FILL_ROOM_FT
					) {
						return true;
					}
				}
				return false;
			});
		if (!blocked(on)) {
			return;
		}
		if (blocked(at)) {
			// (Not clear where he gets to, even - a block drifted over onto
			// it, say: pulled up on his way there instead, where he comes to
			// a stop clear of them - not stood in the middle of it.)
			for (
				let tc = m.t - FILL_BACK_STEP_MS;
				tc >= Math.max(from, m.t - FILL_BACK_MOST_MS);
				tc -= FILL_BACK_STEP_MS
			) {
				const q = legAt(m, tc);
				const f = dist(q.from, q.to);
				if (!GAITS.has(q.move)) {
					return;
				}
				if (f <= 0) {
					continue;
				}
				const vq = legPace(q);
				const eq = Math.min(STALK_EASE_FT, (vq * vq) / (2 * STOP_ACCEL));
				const p = m.posAt(tc);
				const stop = {
					x: p.x + ((q.to.x - q.from.x) / f) * eq,
					y: p.y + ((q.to.y - q.from.y) / f) * eq,
				};
				if (!blocked(stop, tc)) {
					m.cut(tc);
					return;
				}
			}
			return;
		}
		// (From as far short of it as he pulls up in, at his pace there.)
		let left = 0;
		for (let k = m.legs.length - 1; k >= 0; k--) {
			const q = m.legs[k]!;
			const f = dist(q.from, q.to);
			if (q.t0 < from || !GAITS.has(q.move)) {
				return;
			}
			const vq = legPace(q);
			const eq = Math.min(STALK_EASE_FT, (vq * vq) / (2 * STOP_ACCEL));
			if (left + f >= eq) {
				m.cut(q.t1 - (q.t1 - q.t0) * (Math.max(0, eq - left) / f));
				return;
			}
			left += f;
		}
	}

	// Pulled up from where he is, `most` feet at the most (see easeOff):
	// cut back to as late as has him pulled up clear of the men in blocks
	// and tackles (see FILL_ROOM_FT) as he gets there, if it does not -
	// not on into them, and shoved out of them. Back no further than
	// `from`, nor than FILL_BACK_MOST_MS.
	private easeShort(m: Actor, from: number, most: number) {
		const stopOf = (tc: number) => {
			const q = legAt(m, tc);
			const f = dist(q.from, q.to);
			const v = legPace(q);
			if (!GAITS.has(q.move) || f <= 0 || v <= 0) {
				return undefined;
			}
			const e = Math.min(most, (v * v) / (2 * STOP_ACCEL));
			const p = m.posAt(tc);
			return {
				at: {
					x: p.x + ((q.to.x - q.from.x) / f) * e,
					y: p.y + ((q.to.y - q.from.y) / f) * e,
				},
				t: tc + ((2 * e) / v) * 1000,
			};
		};
		const blocked = (s: { at: Pt; t: number }) =>
			this.men.some((x) => {
				for (
					let c = s.t - FILL_ROOM_EVERY_MS;
					x !== m && c <= s.t + FILL_ROOM_MS;
					c += FILL_ROOM_EVERY_MS
				) {
					if (
						LINE_PLAY.has(legAt(x, c).move) &&
						dist(x.posAt(c), s.at) < FILL_ROOM_FT
					) {
						return true;
					}
				}
				return false;
			});
		const s = stopOf(m.t - 1);
		if (!s || !blocked(s)) {
			return;
		}
		for (
			let tc = m.t - FILL_BACK_STEP_MS;
			tc >= Math.max(from, m.t - FILL_BACK_MOST_MS);
			tc -= FILL_BACK_STEP_MS
		) {
			const q = stopOf(tc);
			if (!q) {
				return;
			}
			if (!blocked(q)) {
				m.cut(tc);
				return;
			}
		}
	}

	// How far into the men in blocks (see CLIMB_CLEAR_FT) a man - not
	// `him` - goes at the worst on his way from `from` through `pts`, at an
	// even pace from t0 to t1; and how far that is.
	private blocksIn(
		b: Actor,
		him: Actor,
		from: Pt,
		pts: Pt[],
		t0: number,
		t1: number,
	): { worst: number; L: number } {
		const all = [from, ...pts];
		const lens: number[] = [];
		let L = 0;
		for (let i = 1; i < all.length; i++) {
			L += dist(all[i - 1]!, all[i]!);
			lens.push(L);
		}
		let worst = 0;
		for (let c = t0 + CLIMB_LOOK_MS; c < t1; c += CLIMB_LOOK_MS) {
			const d = (L * (c - t0)) / (t1 - t0);
			const k = Math.max(
				0,
				lens.findIndex((g) => g >= d),
			);
			const g0 = k > 0 ? lens[k - 1]! : 0;
			const p0 = all[k]!;
			const p1 = all[k + 1] ?? p0;
			const u = lens[k]! > g0 ? (d - g0) / (lens[k]! - g0) : 1;
			const p = {
				x: p0.x + (p1.x - p0.x) * u,
				y: p0.y + (p1.y - p0.y) * u,
			};
			for (const x of this.men) {
				if (x !== b && x !== him && LINE_PLAY.has(legAt(x, c).move)) {
					worst = Math.max(worst, CLIMB_CLEAR_FT - dist(x.posAt(c), p));
				}
			}
		}
		return { worst, L };
	}

	// The pace (no more than `fps`, nor less than UNDER_LEAST_FPS) that has
	// him `D` feet on in T seconds and pulled up there - see runUp: slowed
	// down off a quicker one as he goes to be there as soon as at that pace
	// all the way (see run), and eased off over the last of it. (`fps`, if
	// even that gets him there no sooner.)
	private underPace(D: number, T: number, fps: number): number {
		if (!(T > 0)) {
			return fps;
		}
		let p = fps;
		for (let i = 0; i < 4; i++) {
			const ease = Math.min(D * 0.5, (p * p) / (2 * STOP_ACCEL * STOP_HARD));
			p = clamp((D + ease) / T, Math.min(UNDER_LEAST_FPS, fps), fps);
		}
		return p;
	}

	// The next stretch of a chase, to `to` by t1 at `fps`: turned a little
	// off it, if that has him clear of the men in blocks ahead of him, and
	// any stood about (see CLIMB_CLEAR_FT) - over as far as he gets toward
	// it by then, and on the same way CHASE_LOOK_MS more: round them in good
	// time, not on into them (and planted for the next of it in them).
	private wayClear(m: Actor, to: Pt, t1: number, fps: number): Pt {
		const D = dist(m.at, to);
		if (t1 <= m.t || D < 1 || !(fps > 0)) {
			return to;
		}
		const ms = t1 + CHASE_LOOK_MS - m.t;
		const v = Math.min(fps, (D * 1000) / (t1 - m.t));
		const ux = (to.x - m.at.x) / D;
		const uy = (to.y - m.at.y) / D;
		// (The men in blocks, and those stood about - or near enough - by
		// then: where he is as he goes, up to his pace from what he carries
		// that way - see RAMP_ACCEL.)
		const into = (turn: number) => {
			const vx = ux * Math.cos(turn) - uy * Math.sin(turn);
			const vy = uy * Math.cos(turn) + ux * Math.sin(turn);
			const v0 = Math.min(
				v,
				paceOn(m.last, m.at, { x: m.at.x + vx, y: m.at.y + vy }),
			);
			const up = (v - v0) / RAMP_ACCEL;
			let worst = 0;
			for (let c = m.t + CLIMB_LOOK_MS; c < m.t + ms; c += CLIMB_LOOK_MS) {
				const s = Math.min((c - m.t) / 1000, up);
				const d =
					v0 * s + (RAMP_ACCEL * s * s) / 2 + v * ((c - m.t) / 1000 - s);
				const p = { x: m.at.x + vx * d, y: m.at.y + vy * d };
				for (const x of this.men) {
					const l = legAt(x, c);
					if (
						x !== m &&
						(LINE_PLAY.has(l.move) ||
							legPace(l) < CHASE_STILL_FPS ||
							(c <= x.t &&
								x.team !== m.team &&
								legPace(l) < v * CHASE_PASS_SLOWER))
					) {
						worst = Math.max(worst, CLIMB_CLEAR_FT - dist(x.posAt(c), p));
					}
				}
			}
			return worst;
		};
		let worst = into(0);
		if (worst <= CHASE_CLEAR_SLACK_FT) {
			return to;
		}
		// (No further round from the way he is going than CHASE_TURN_MOST,
		// or than it was - not planted or cut hard for it.)
		const l = m.last;
		const goes = Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x);
		const way = Math.atan2(uy, ux);
		const most =
			dist(l.from, l.to) > 0 && legPace(l) >= STOP_LEAST
				? Math.max(CHASE_TURN_MOST, Math.abs(angleOff(way - goes)))
				: Infinity;
		let best = 0;
		for (const turn of CHASE_CLEAR_TURN) {
			if (Math.abs(angleOff(way + turn - goes)) > most) {
				continue;
			}
			const w = into(turn);
			if (w < worst - CHASE_CLEAR_SLACK_FT) {
				best = turn;
				worst = w;
			}
			if (worst <= CHASE_CLEAR_SLACK_FT) {
				break;
			}
		}
		if (best === 0) {
			return to;
		}
		const c = Math.cos(best);
		const s = Math.sin(best);
		return {
			x: m.at.x + (ux * c - uy * s) * D,
			y: m.at.y + (uy * c + ux * s) * D,
		};
	}

	// Down his lane to `to` at `fps`, in stretches round the blocks going
	// on in it and the men stood in it (see wayClear) - not on through them
	// in the one straight run.
	private laneRun(m: Actor, to: Pt, fps: number, move?: Move) {
		for (
			let k = 0;
			k < LANE_RUN_STEPS && dist(m.at, to) > (fps * PURSUIT_STEP) / 1000;
			k++
		) {
			const c = m.t + PURSUIT_STEP;
			m.chaseUp(this.wayClear(m, to, c, fps), c, fps, move);
		}
		m.run(to, fps, move);
	}

	// As chase, to `to` by `by` - but in stretches round the blocks going
	// on in his way and the men stood in it (see wayClear), at the pace that
	// has him there by then: not on through them in the one straight run.
	private chaseClear(m: Actor, to: Pt, by: number, fps: number, move?: Move) {
		const pace = Math.min(fps, (dist(m.at, to) * 1000) / Math.max(1, by - m.t));
		for (
			let k = 0;
			k < LANE_RUN_STEPS &&
			by - m.t > PURSUIT_STEP * 2 &&
			dist(m.at, to) > (pace * PURSUIT_STEP * 2) / 1000;
			k++
		) {
			const c = m.t + PURSUIT_STEP;
			// (Clear ahead, and going: on at it as chase has him - bent round
			// to it, not planted for it. From a stand, turned to it first.)
			const way = this.wayClear(m, to, c, pace);
			const l = m.last;
			if (way === to && RAMPS.has(l.move) && legPace(l) >= STOP_LEAST) {
				m.chase(to, c, pace, move);
			} else {
				m.chaseUp(way, c, pace, move);
			}
		}
		m.chase(to, by, fps, move);
	}

	// Up to `m` straight from where he is, at a climb's pace (met as in
	// blockOn), no deeper into the men in blocks on the way than
	// CLIMB_INTO_FT (see CLIMB_CLEAR_FT).
	private climbClear(b: Actor, m: Actor, until: number): boolean {
		const from = b.at;
		const fps = b.top * CLIMB_PACE;
		let to: Pt | undefined;
		let ms = 0;
		for (let c = b.t + 100; c <= until; c += 50) {
			const p = this.of(m.posAt(c));
			const at = this.w({ d: p.d - BLOCK_GAP, a: p.a });
			const need = rampedMs(dist(from, at), fps, paceOn(b.last, from, at));
			if (need <= c - b.t) {
				to = at;
				ms = c - b.t;
				break;
			}
		}
		if (!to) {
			return true;
		}
		// (Or round them - see roundBlocks - if there is a way.)
		const t1 = b.t + ms;
		if (this.blocksIn(b, m, from, [to], b.t, t1).worst <= CLIMB_INTO_FT) {
			return true;
		}
		const round = this.roundBlocks(b, to, t1, m);
		return (
			round !== undefined &&
			this.blocksIn(b, m, from, round, b.t, t1).worst <= CLIMB_INTO_FT
		);
	}

	// The way round the men in blocks between him and `to` (see
	// CLIMB_CLEAR_FT), there by t1: the points to go through, on to `to` -
	// or none, straight there clear of them already (or no way round any
	// clearer).
	private roundBlocks(
		b: Actor,
		to: Pt,
		t1: number,
		him: Actor,
		pace = CLIMB_PACE,
	): Pt[] | undefined {
		const from = b.at;
		const t0 = b.t;
		const D = dist(from, to);
		if (t1 <= t0 || D < CLIMB_ROUND_LEAST_FT) {
			return undefined;
		}
		const into = (pts: Pt[]) => this.blocksIn(b, him, from, pts, t0, t1);
		const straight = into([to]).worst;
		if (straight <= CLIMB_ROUND_SLACK_FT) {
			return undefined;
		}
		const ux = (to.x - from.x) / D;
		const uy = (to.y - from.y) / D;
		const fps = b.top * pace;
		const most = (t1 - t0) * CLIMB_ROUND_MORE;
		let best: { pts: Pt[]; worst: number } | undefined;
		for (const along of CLIMB_ALONG) {
			for (const off of CLIMB_OFF) {
				const corner = {
					x: from.x + ux * D * along - uy * off,
					y: from.y + uy * D * along + ux * off,
				};
				const pts = [...this.bend(from, corner, to, RELEASE_ACCEL, fps), to];
				const { worst, L } = into(pts);
				if (
					rampedMs(L, fps, paceOn(b.last, from, pts[0]!)) <= most &&
					worst < (best?.worst ?? straight - CLIMB_ROUND_SLACK_FT)
				) {
					best = { pts, worst };
				}
				if (best && best.worst <= CLIMB_ROUND_SLACK_FT) {
					return best.pts;
				}
			}
		}
		return best?.pts;
	}

	// A back out of the backfield on his route from t0: round the men on the
	// line - in blocks, or set for them - on his way out past it, if he
	// would go through them (see RELEASE_INTO_FT): bent out round them as
	// quick as he went (see bend), back onto his way a little past them
	// (as soon after it as that can be done - see RELEASE_LATER_MS), and on
	// with his route from there as it was. No later than `by`.
	private releaseRound(m: Actor, t0: number, by: number) {
		if (t0 >= m.t || this.of(m.posAt(t0)).d > -RELEASE_BACK_FT) {
			return;
		}
		const inLine = (x: Actor, c: number) => {
			const l = legAt(x, c);
			return x !== m && (LINE_PLAY.has(l.move) || l.move === "passSet");
		};
		const into = (p: Pt, c: number) => {
			let w = 0;
			for (const x of this.men) {
				if (inLine(x, c)) {
					w = Math.max(w, CLIMB_CLEAR_FT - dist(x.posAt(c), p));
				}
			}
			return w;
		};
		// (Past them: RELEASE_PAST_FT past the furthest up of them either side
		// of him - see RELEASE_SIDE_FT.)
		const last = Math.min(m.t, by);
		let past: number | undefined;
		for (let c = t0 + 100; c <= last; c += 20) {
			const s = this.of(m.posAt(c));
			let front = -Infinity;
			for (const x of this.men) {
				if (inLine(x, c)) {
					const q = this.of(x.posAt(c));
					if (Math.abs(q.a - s.a) <= RELEASE_SIDE_FT) {
						front = Math.max(front, q.d);
					}
				}
			}
			if (s.d >= front + RELEASE_PAST_FT) {
				past = c;
				break;
			}
		}
		if (past === undefined) {
			return;
		}
		let was = 0;
		for (let c = t0 + CLIMB_LOOK_MS; c < past; c += CLIMB_LOOK_MS) {
			was = Math.max(was, into(m.posAt(c), c));
		}
		if (was <= RELEASE_INTO_FT) {
			return;
		}
		const legs = m.legs.map((l) => ({ ...l }));
		const at = (l: Leg, t: number): Pt => {
			const u = l.t1 > l.t0 ? clamp((t - l.t0) / (l.t1 - l.t0), 0, 1) : 1;
			return {
				x: l.from.x + (l.to.x - l.from.x) * u,
				y: l.from.y + (l.to.y - l.from.y) * u,
			};
		};
		// (How far along his own way he has gone from t0 by t.)
		const S = (t: number) => {
			let s = 0;
			for (const l of legs) {
				const a = Math.max(l.t0, t0);
				const b = Math.min(l.t1, t);
				if (b > a) {
					s += dist(at(l, a), at(l, b));
				}
			}
			return s;
		};
		const lensOf = (all: Pt[]) => {
			const lens = [0];
			for (let i = 1; i < all.length; i++) {
				lens.push(lens[i - 1]! + dist(all[i - 1]!, all[i]!));
			}
			return lens;
		};
		const along = (all: Pt[], lens: number[], g: number): Pt => {
			const k = Math.max(
				1,
				lens.findIndex((x) => x >= g),
			);
			const a = all[k - 1]!;
			const b = all[k] ?? a;
			const g0 = lens[k - 1]!;
			const u = lens[k]! > g0 ? clamp((g - g0) / (lens[k]! - g0), 0, 1) : 1;
			return { x: a.x + (b.x - a.x) * u, y: a.y + (b.y - a.y) * u };
		};
		const P0 = m.posAt(t0);
		const fps = m.top * ROUTE_PACE;
		for (
			let end = past;
			end <= Math.min(last, past + RELEASE_LATER_MS);
			end += RELEASE_LATER_STEP_MS
		) {
			const L = S(end);
			const P1 = m.posAt(end);
			const D = dist(P0, P1);
			if (L < CLIMB_ROUND_LEAST_FT || D < 1) {
				continue;
			}
			const ts: number[] = [];
			for (let c = t0 + CLIMB_LOOK_MS; c < end; c += CLIMB_LOOK_MS) {
				ts.push(c);
			}
			const ss = ts.map(S);
			// (Back onto his way where it had him then, going near enough
			// the way he was - see RELEASE_KINK.)
			const on = legs.find((l) => l.t0 <= end && l.t1 > end);
			const way =
				on && dist(on.from, on.to) > 0
					? Math.atan2(on.to.y - on.from.y, on.to.x - on.from.x)
					: undefined;
			const ux = (P1.x - P0.x) / D;
			const uy = (P1.y - P0.y) / D;
			let best: { all: Pt[]; lens: number[]; w: number } | undefined;
			for (const u of CLIMB_ALONG) {
				for (const off of CLIMB_OFF) {
					const corner = {
						x: P0.x + ux * D * u - uy * off,
						y: P0.y + uy * D * u + ux * off,
					};
					const all = [
						P0,
						...this.bend(P0, corner, P1, RELEASE_ACCEL, fps),
						P1,
					];
					const q = all.at(-2)!;
					if (
						way !== undefined &&
						Math.cos(Math.atan2(P1.y - q.y, P1.x - q.x) - way) <
							Math.cos(RELEASE_KINK)
					) {
						continue;
					}
					const lens = lensOf(all);
					const Lp = lens.at(-1)!;
					if (Lp > L * RELEASE_MORE) {
						continue;
					}
					let w = 0;
					ts.forEach((c, i) => {
						w = Math.max(w, into(along(all, lens, (ss[i]! * Lp) / L), c));
					});
					if (w < (best?.w ?? was - RELEASE_INTO_FT)) {
						best = { all, lens, w };
					}
				}
			}
			if (!best) {
				continue;
			}
			// (Laid at the times his own legs had him going from one to the
			// next, and at the points of the way round - S run backward for
			// when he is at each.)
			const Lp = best.lens.at(-1)!;
			const when = (g: number) => {
				const s = (g * L) / Lp;
				let lo = t0;
				let hi = end;
				for (let k = 0; k < 30; k++) {
					const mid = (lo + hi) / 2;
					if (S(mid) < s) {
						lo = mid;
					} else {
						hi = mid;
					}
				}
				return Math.round((lo + hi) / 2);
			};
			const times = new Set<number>([end]);
			for (const l of legs) {
				if (l.t1 > t0 && l.t1 < end) {
					times.add(l.t1);
				}
			}
			for (const g of best.lens.slice(1, -1)) {
				times.add(when(g));
			}
			const keep: Leg[] = [];
			for (const l of legs) {
				if (l.t1 <= t0) {
					keep.push(l);
				} else if (l.t0 < t0) {
					keep.push({ ...l, t1: t0, to: at(l, t0) });
				}
			}
			let from = P0;
			let t = t0;
			for (const c of [...times].sort((a, b) => a - b)) {
				if (c <= t) {
					continue;
				}
				const l = legs.find((x) => x.t0 < c && x.t1 >= c) ?? legs.at(-1)!;
				const to = c === end ? P1 : along(best.all, best.lens, (S(c) * Lp) / L);
				keep.push({ ...l, t0: t, t1: c, from, to });
				from = to;
				t = c;
			}
			for (const l of legs) {
				if (l.t1 > end) {
					keep.push(l.t0 < end ? { ...l, t0: end, from: P1 } : l);
				}
			}
			const yaw = m.yaw;
			m.legs.splice(0, m.legs.length, ...keep);
			m.yaw = yaw;
			return;
		}
	}

	blockOn(
		b: Actor,
		m: Actor | undefined,
		won: boolean,
		offAt: (d: number, late?: number) => number,
		until: number,
		clear?: Actor,
		through = false,
	) {
		const r = this.r;
		// (Not up the back of the man with the ball on the way to him - come
		// up behind him and through him to a man past him, as he goes down:
		// see BLOCK_CLEAR_FT, UNTIL_PAST_MS.)
		const upHisBack = (at: Pt, c: number) => {
			if (!clear || c <= b.t) {
				return false;
			}
			const far = dist(b.at, at);
			if (far < 1) {
				return false;
			}
			const ux = (at.x - b.at.x) / far;
			const uy = (at.y - b.at.y) / far;
			for (
				let k = Math.max(b.t + 50, until - UNTIL_PAST_MS - CLEAR_DOWN_MS);
				k < c;
				k += 50
			) {
				const u = (k - b.t) / (c - b.t);
				const p = { x: b.at.x + ux * far * u, y: b.at.y + uy * far * u };
				const q = clear.posAt(k);
				if (
					dist(p, q) < BLOCK_CLEAR_FT &&
					(q.x - p.x) * ux + (q.y - p.y) * uy > 0
				) {
					return true;
				}
			}
			return false;
		};
		let late = false;
		const meetOf = () => {
			if (!m) {
				return undefined;
			}
			// (Up to speed for it off what he is doing - see rampedMs.)
			const fps = b.top * CLIMB_PACE;
			for (let c = b.t + 100; c <= until; c += 50) {
				const p = this.of(m.posAt(c));
				const at = this.w({ d: p.d - BLOCK_GAP, a: p.a });
				const far = dist(b.at, at);
				if (rampedMs(far, fps, paceOn(b.last, b.at, at)) <= c - b.t) {
					if (upHisBack(at, c)) {
						late = true;
						return undefined;
					}
					return { t: c, at };
				}
			}
			return undefined;
		};
		let meet = meetOf();
		if (m && meet && clear && through) {
			const was = m.legs.map((l) => ({ ...l }));
			const from = this.outOfWay(m, clear, meet.t);
			if (from !== undefined) {
				const again = meetOf();
				// (Not cut off a blink into his step over by the blocker on
				// him: stood his ground for him instead.)
				if (again && again.t < from + FLICK_MS) {
					m.legs.splice(0, m.legs.length, ...was);
				} else {
					meet = again;
				}
			}
		}
		// (Too late to get to him but through the man with the ball going
		// down: pulled up off it instead, out of the way of it.)
		if (m && !meet && late) {
			const went = b.t;
			if (RAMPS.has(b.last.move) && legPace(b.last) >= STOP_LEAST) {
				b.easeOff(EASE_OFF_FT, clear?.posAt(until));
				this.easeShort(b, went, EASE_OFF_FT);
			}
			this.giveWay(b, until);
			return;
		}
		if (!m || !meet) {
			// (Up to the next level, and nobody there - off to the side of
			// the way the man with the ball comes up behind him, not up the
			// middle of it ahead of him for him to run up his back.)
			const s = this.of(b.at);
			const from = b.at;
			const t0 = b.t;
			const t1 = b.t + 900;
			const to = (o: number) => this.w({ d: s.d + 9, a: s.a + o });
			// (Nor through men in blocks on his way there, or stood where
			// they get to - see blocksIn.)
			const blocked = (o: number) => {
				const p = to(o);
				let worst = this.blocksIn(b, b, from, [p], t0, t1).worst;
				for (let c = t1; c <= until; c += 100) {
					for (const x of this.men) {
						if (x !== b && LINE_PLAY.has(legAt(x, c).move)) {
							worst = Math.max(worst, CLIMB_CLEAR_FT - dist(x.posAt(c), p));
						}
					}
				}
				return Math.max(0, worst - CLIMB_INTO_FT);
			};
			let over = 0;
			{
				const nearest = (o: number) => {
					if (!clear) {
						return Infinity;
					}
					const p = to(o);
					let near = Infinity;
					// (Through to the end of it: not stood where he gets
					// to for the man with the ball to come by him there.)
					for (let c = t0; c <= Math.max(until, t1 + 600); c += 50) {
						const u = Math.min(1, (c - t0) / (t1 - t0));
						near = Math.min(
							near,
							dist(clear.posAt(c), {
								x: from.x + (p.x - from.x) * u,
								y: from.y + (p.y - from.y) * u,
							}),
						);
					}
					return near;
				};
				const bad = (o: number) =>
					blocked(o) + Math.max(0, BLOCK_CLEAR_FT - nearest(o));
				let best = bad(0);
				for (const o of [2, -2, 3, -3, 4, -4, 5, -5]) {
					if (best <= 0) {
						break;
					}
					const now = bad(o);
					if (now < best - 0.5) {
						best = now;
						over = o;
					}
				}
			}
			this.chaseClear(b, to(over), t1, b.top * 0.6, "run");
			this.offBlock(b, until);
			this.giveWay(b, until);
			return;
		}
		// (Met on the run: pulled up into it over his last strides - not
		// stopped dead in his stride as the blocker gets to him. Met where
		// that has him.)
		if (m.brakeBy(meet.t, BRAKE_FPS, HIT_ACCEL)) {
			const q = this.of(m.posAt(meet.t));
			meet = { t: meet.t, at: this.w({ d: q.d - BLOCK_GAP, a: q.a }) };
		}
		// (Come up past him on the run, with only a step or so back into
		// him: from as far back on his way as has him still in front of him
		// - not run on past him and stepped back into him off it in a
		// stride.)
		const nearOf = (to: { at: Pt; t: number }) =>
			(dist(b.at, to.at) / Math.max(0.001, to.t - b.t)) * 1000 < WALK_MOST_FPS;
		const ran = b.last;
		if (
			RAMPS.has(ran.move) &&
			legPace(ran) >= STOP_LEAST &&
			nearOf(meet) &&
			angleAt(ran.from, ran.to, meet.at) > PLANT_TURN
		) {
			const was = b.legs.map((l) => ({ ...l }));
			const { yaw, pinned } = b;
			let found = false;
			for (
				let tc = b.t - PAST_BACK_STEP_MS;
				tc >= b.t - PAST_BACK_MOST_MS && tc > b.pinned;
				tc -= PAST_BACK_STEP_MS
			) {
				if (!RAMPS.has(legAt(b, tc).move)) {
					break;
				}
				b.legs.splice(0, b.legs.length, ...was.map((l) => ({ ...l })));
				b.yaw = yaw;
				b.pinned = pinned;
				b.cut(tc);
				const again = meetOf();
				const l = b.last;
				if (
					again &&
					dist(l.from, l.to) > 0 &&
					angleAt(l.from, l.to, again.at) <= PLANT_TURN
				) {
					meet = again;
					found = true;
					break;
				}
			}
			if (!found) {
				b.legs.splice(0, b.legs.length, ...was);
				b.yaw = yaw;
				b.pinned = pinned;
			}
		}
		// (Only a step or so to square up to him: in his drive steps, facing
		// up the field - not turned round and walked back there.)
		const near = nearOf(meet);
		let round = near ? undefined : this.roundBlocks(b, meet.at, meet.t, m);
		// (Turned off the way he was going for it - round them from a stand,
		// or back for him, past bending round to him: planted for it first
		// - see Actor.plantFor - not turned in a stride at full tilt - and
		// on him where that has him meet him. Not if it has him not get to
		// him at all.)
		const bl = b.last;
		const first = round?.[0] ?? meet.at;
		if (
			!near &&
			GAITS.has(bl.move) &&
			dist(bl.from, bl.to) > 0 &&
			angleAt(bl.from, bl.to, first) > (round ? CUT_LEAST : PLANT_TURN)
		) {
			const was = b.legs.map((l) => ({ ...l }));
			const t0 = b.t;
			b.plantFor(first);
			const again = b.t > t0 ? meetOf() : undefined;
			if (again) {
				meet = again;
				round = this.roundBlocks(b, meet.at, meet.t, m);
			} else {
				b.legs.splice(0, b.legs.length, ...was);
			}
		}
		if (round) {
			// (Up to his pace off what he was doing, as straight there - see
			// meetOf - and on round at it.)
			const t0 = b.t;
			const fps = b.top * CLIMB_PACE;
			const v0 = paceOn(b.last, b.at, round[0]!);
			let all = 0;
			let was = b.at;
			const ms = round.map((p) => {
				all += dist(was, p);
				was = p;
				return rampedMs(all, fps, v0);
			});
			const scale = (meet.t - t0) / Math.max(ms.at(-1)!, 1e-6);
			round.forEach((p, i) => b.go(p, t0 + ms[i]! * scale));
		} else {
			// (Round on to him off the way he was going, if he can be - not
			// turned in a stride at full tilt: see Actor.turnFor.)
			const by = near ? undefined : b.bendFor(meet.at, meet.t);
			b.go(
				meet.at,
				by ?? meet.t,
				near ? "drive" : undefined,
				near ? this.ahead : undefined,
			);
		}
		m.cut(meet.t);
		m.hold(meet.t);
		const p = this.of(m.at);
		// (Let up on his own count once it is over - see countOf.)
		const till = until + RUN_END_SPREAD_MS * countOf(b, this.input.key, 4);
		const off = Math.max(meet.t + 200, offAt(p.d, till));
		const end = won ? off : Math.min(off, meet.t + SHED_MS + r() * SHED_SPREAD);
		// (Broken down over his last strides and his hands pressed into him
		// - not run into him at full tilt and stopped dead: see gatherTo -
		// pressed in only with the time on him for it, not glanced off him.)
		if (!near) {
			b.gatherTo(
				b.at,
				BRAKE_FPS,
				HIT_ACCEL,
				end - meet.t >= GATHER_BLOCK_LEAST_MS ? HIT_PRESS_MS : 0,
				"drive",
			);
		}
		// (On him as late as gathering into him had him there, and on with
		// him only as far as the time left on him takes them: not driven a
		// step over in a blink.)
		const hitT = Math.min(end, Math.max(meet.t, b.t));
		const give = won
			? Math.min(2.5 + r() * 2, (BLOCK_DRIVE_FPS * (end - hitT)) / 1000)
			: -Math.min(1, (BLOCK_GIVE_FPS * Math.max(0, end - b.t)) / 1000);
		// (Turned off the way the man with the ball comes by - driven over
		// to the side of it he is on, not stood in it as he comes through.)
		// (As little as does it, or as far off him as it gets them.)
		let over = 0;
		if (clear) {
			const most = (BLOCK_CLEAR_FPS * (end - hitT)) / 1000;
			const nearest = (o: number) => {
				let near = Infinity;
				for (let c = hitT; c <= end + CLEAR_LATE_MS; c += 50) {
					const u = Math.min(1, (c - hitT) / Math.max(1, end - hitT));
					const q = this.of(clear.posAt(c));
					const d = q.d - (p.d + give * u);
					const a = q.a - (p.a + o * u);
					near = Math.min(near, Math.hypot(d, a), Math.hypot(d + BLOCK_GAP, a));
				}
				return near;
			};
			let best = nearest(0);
			for (const o of [1, -1, 2, -2, 3, -3, 4, -4]) {
				if (best >= BLOCK_CLEAR_FT || Math.abs(o) > most) {
					break;
				}
				const near = nearest(o);
				if (near > best + 0.5) {
					best = near;
					over = o;
				}
			}
		}
		m.go(this.w({ d: p.d + give, a: p.a + over }), end, "engage", this.behind);
		b.go(
			this.w({ d: p.d + give - BLOCK_GAP, a: p.a + over }),
			Math.max(end, b.t),
			"drive",
			this.ahead,
		);
		this.offBlock(b, until);
		b.hold(Math.max(b.t, till), "stand", this.ahead);
	}

	// Done with his block (or with nobody to block) well before the
	// blocking is over (`until`, a beat after the man with the ball is
	// down): a look, and after the ball at a jog, kept off it, on his own
	// count - not stood where it left him.
	offBlock(b: Actor, until: number, react = FREED_REACT) {
		const by = until - 600;
		if (by - b.t - react < FREED_AFTER_LEAST_MS) {
			return;
		}
		b.hold(b.t + react);
		const went = b.t;
		this.alongWith(
			b,
			FREED_FPS[0] +
				(FREED_FPS[1] - FREED_FPS[0]) * countOf(b, this.input.key, 34),
			FREED_KEEP_FT[0] + FREED_KEEP_FT[1] * countOf(b, this.input.key, 35),
			by,
		);
		// (Pulled up clear of the blocks still going on when he stops - see
		// easeShort - not on into them.)
		this.easeShort(b, went, EASE_OFF_FT);
	}

	// A blocker locked up with the man in front of him (BLOCK_GAP on) from
	// t0, with the man with the ball through them before t1: how far over
	// (feet across the field) to have gone by when (`by`) to be out of his
	// way - none if they are not in it, or if it would have them into the
	// men around them (`crowd`) any more than they are.
	wayThrough(
		b: Actor,
		clear: Actor,
		t0: number,
		t1: number,
		crowd: Actor[],
	): { over: number; by: number } {
		const nearest = (over: number, by: number, of: Actor[]) => {
			let near = Infinity;
			let when = t0;
			for (let c = t0; c <= t1; c += 50) {
				const p = this.of(b.posAt(c));
				const a = p.a + over * clamp((c - t0) / Math.max(1, by - t0), 0, 1);
				for (const m of of) {
					const q = this.of(m.posAt(c));
					const d = Math.min(
						Math.hypot(q.d - p.d, q.a - a),
						Math.hypot(q.d - p.d - BLOCK_GAP, q.a - a),
					);
					if (d < near) {
						near = d;
						when = c;
					}
				}
			}
			return { near, when };
		};
		const first = nearest(0, t0, [clear]);
		const by = first.when;
		const most = (BLOCK_CLEAR_FPS * (by - t0)) / 1000;
		const room = Math.min(WAY_CLEAR_FT, nearest(0, by, crowd).near);
		let best = first.near;
		let over = 0;
		for (const o of [1, -1, 2, -2, 3, -3, 4, -4]) {
			if (best >= BLOCK_CLEAR_FT || Math.abs(o) > most) {
				break;
			}
			const near = nearest(o, by, [clear]).near;
			if (near > best + 0.5 && nearest(o, by, crowd).near >= room) {
				best = near;
				over = o;
			}
		}
		return { over, by };
	}

	// A man waiting on a blocker, with the man with the ball by him before
	// then (`by`): a step or two over out of his way in his low steps,
	// there as he comes - not run through stood there. When he goes, if he
	// does.
	outOfWay(m: Actor, clear: Actor, by: number): number | undefined {
		const at = m.at;
		const nearest = (to: Pt) => {
			let near = Infinity;
			let when = m.t;
			for (let c = m.t; c <= by; c += 33) {
				const d = dist(clear.posAt(c), to);
				if (d < near) {
					near = d;
					when = c;
				}
			}
			return { near, when };
		};
		const { near, when } = nearest(at);
		const a = clear.posAt(when - 100);
		const z = clear.posAt(when + 100);
		const run = dist(a, z);
		if (near >= WAY_CLEAR_FT || run < 0.5) {
			return undefined;
		}
		const n = { x: -(z.y - a.y) / run, y: (z.x - a.x) / run };
		const q = clear.posAt(when);
		const off = (at.x - q.x) * n.x + (at.y - q.y) * n.y;
		const side = off < 0 ? -1 : 1;
		// (The near side if that does it, the far one if it does not.)
		let to = at;
		let best = near;
		for (const way of [side, -side]) {
			for (let s = WAY_CLEAR_FT + 0.3 - way * off; s <= WAY_MOST_FT; s += 0.5) {
				const p = { x: at.x + n.x * way * s, y: at.y + n.y * way * s };
				const d = nearest(p).near;
				if (d > best) {
					best = d;
					to = p;
				}
				if (d >= WAY_CLEAR_FT) {
					break;
				}
			}
			if (best >= WAY_CLEAR_FT) {
				break;
			}
		}
		if (to === at) {
			return undefined;
		}
		const pace = (run / 200) * 1000;
		const there = Math.max(
			m.t + WAY_LEAST_MS,
			when - (WAY_CLEAR_FT / pace) * 1000 - 100,
		);
		const from = Math.max(
			m.t,
			there - Math.max(WAY_LEAST_MS, (dist(at, to) / WAY_FPS) * 1000),
		);
		m.hold(from);
		m.go(to, there, "slide", this.behind);
		m.easeOff(WAY_EASE_FT);
		return from;
	}

	// Men locked up, from t0 to t1: shoved this way and that together - a
	// little way across the field and back, a half step up and back - each
	// fighting the other.
	// When he can be into the man across from him (`at`, where that is at
	// t), off from where he is, no sooner than `by` - see fireInto - and
	// how he goes into him.
	fireAim(m: Actor, at: (t: number) => Pt, by: number, pace = RUSH_HIT_PACE) {
		const top = m.top * pace;
		const move = fireOff(m, at(by));
		// (Off at a run: his hands on him a little way off, and on in to
		// him as that takes the pace out of him - see HIT_PRESS_MS.)
		const S = move === "run" ? HIT_PRESS_MS : 0;
		const press = (HIT_FPS * S) / 2000;
		const way = (t: number) => {
			const p = at(t);
			const D = dist(m.at, p);
			return D > press * 2
				? { x: (p.x - m.at.x) / D, y: (p.y - m.at.y) / D }
				: { x: 0, y: 0 };
		};
		const hands = (t: number) => {
			const p = at(t);
			const u = way(t);
			return { x: p.x - u.x * press, y: p.y - u.y * press };
		};
		// (Him getting away from him as he comes: as many goes at it as
		// it takes.)
		let t = by;
		for (let k = 0; k < 6; k++) {
			const was = t;
			t = Math.max(by, m.t + hitMs(dist(m.at, hands(t)), top));
			if (Math.abs(t - was) < 0.5) {
				break;
			}
		}
		return { top, move, S, press, way, hands, t };
	}

	// Off the ball into the man across from him (`at`, where that is at t)
	// by `by`, or as soon after as he can be: up out of his stance and into
	// him (see GET_OFF_ACCEL) - a step or two, his arms out into him, or a
	// run (see fireOff); not off from still at full pace, nor into him at
	// all he got up to - gathered into him (see hitInto). (On him
	// FIRE_HELD_MS before `until` at the latest, when he is off him.)
	fireInto(
		m: Actor,
		at: (t: number) => Pt,
		by: number,
		until: number,
		pace = RUSH_HIT_PACE,
	) {
		const {
			top,
			move,
			S,
			press,
			way,
			hands,
			t: soonest,
		} = this.fireAim(m, at, by, pace);
		let t = Math.min(soonest, Math.max(by, until - FIRE_HELD_MS - S));
		// (On him a while before the end of it, but no sooner than he can be
		// gathered into him at all, at the most he has - see hitMs - nor
		// later than FIRE_LEAST_HELD_MS before it: not run into him flat
		// out to be there in time.)
		if (t < soonest) {
			const quick = m.t + hitMs(dist(m.at, hands(t)), m.top * FLAT_OUT);
			t = Math.min(
				soonest,
				Math.max(t, Math.min(quick, until - FIRE_LEAST_HELD_MS - S)),
			);
		}
		const u = way(t);
		const k0 = m.legs.length;
		const from = m.at;
		m.hitInto(
			hands(t),
			t,
			top,
			GET_OFF_ACCEL,
			HIT_FPS,
			HIT_ACCEL,
			move,
			GET_OFF_SQUARE_MS,
			m.top * FLAT_OUT,
		);
		// (Right on him from the first - a step at the most: square to him
		// through it, not turned whichever way each inch of it goes.)
		if (dist(from, m.at) < FIRE_SQUARE_FT) {
			for (let k = k0; k < m.legs.length; k++) {
				m.legs[k]!.yaw ??= this.behind;
			}
		}
		if (S === 0 || (u.x === 0 && u.y === 0)) {
			return;
		}
		const n = Math.max(1, Math.round(S / HIT_PRESS_STEP_MS));
		for (let k = 1; k <= n; k++) {
			const tk = t + (S * k) / n;
			const s = (1 - k / n) * (1 - k / n);
			const p = at(tk);
			m.go(
				{ x: p.x - u.x * press * s, y: p.y - u.y * press * s },
				tk,
				"engage",
				this.behind,
			);
		}
	}

	// Off from still along `path` (on from where he is), up to speed at `a`
	// - no quicker than `top` - and on at it: at the end of it by t1, or as
	// soon after as he can be; still facing the way he was over the first
	// `square` ms. (The pace he ends at.) Or, given `ve` and the time for
	// it, gathered into the end of it at that (see hitInto): there just as
	// soon, quicker before it.
	getOffAlong(
		m: Actor,
		path: Pt[],
		t1: number,
		a: number,
		top: number,
		move: Move,
		square = 0,
		ve?: number,
	): number {
		const go = m.t;
		const gone: number[] = [];
		let all = 0;
		let was = m.at;
		for (const p of path) {
			all += dist(was, p);
			gone.push(all);
			was = p;
		}
		if (ve !== undefined && all > 0 && path.length > 0) {
			const Ad = HIT_ACCEL;
			const T = (t1 - go) / 1000;
			const qa = 1 / (2 * a) + 1 / (2 * Ad);
			const qb = T + ve / Ad;
			const qc = (ve * ve) / (2 * Ad) + all;
			const disc = qb * qb - 4 * qa * qc;
			const vc = disc >= 0 ? (qb - Math.sqrt(disc)) / (2 * qa) : Infinity;
			const sd = (vc * vc - ve * ve) / (2 * Ad);
			if (vc > ve && vc <= top && all - sd >= (vc * vc) / (2 * a)) {
				// (Where he starts slowing, and the rest of the way from it.)
				const td = (vc - ve) / Ad;
				const from = all - sd;
				const k = gone.findIndex((g) => g >= from);
				const a0 = k > 0 ? path[k - 1]! : m.at;
				const g0 = k > 0 ? gone[k - 1]! : 0;
				const w = gone[k]! > g0 ? (from - g0) / (gone[k]! - g0) : 1;
				const cut = {
					x: a0.x + (path[k]!.x - a0.x) * w,
					y: a0.y + (path[k]!.y - a0.y) * w,
				};
				const rest = [cut, ...path.slice(k)];
				const along = (d: number): Pt => {
					let left = d;
					for (let i = 1; i < rest.length; i++) {
						const p = rest[i - 1]!;
						const q = rest[i]!;
						const len = dist(p, q);
						if (left <= len || i === rest.length - 1) {
							const u = len > 0 ? clamp(left / len, 0, 1) : 1;
							return { x: p.x + (q.x - p.x) * u, y: p.y + (q.y - p.y) * u };
						}
						left -= len;
					}
					return rest.at(-1)!;
				};
				const tc = t1 - td * 1000;
				this.getOffAlong(
					m,
					[...path.slice(0, k), cut],
					tc,
					a,
					top,
					move,
					square,
				);
				const n = Math.max(1, Math.round((td * 1000) / STOP_STEP_MS));
				for (let i = 1; i <= n; i++) {
					const u = (td * i) / n;
					m.go(
						i === n ? path.at(-1)! : along(vc * u - (Ad * u * u) / 2),
						tc + u * 1000,
						move,
						undefined,
						{},
					);
				}
				return ve;
			}
		}
		const up = (top * top) / (2 * a);
		const soonest =
			all <= up ? Math.sqrt((2 * all) / a) : top / a + (all - up) / top;
		const T = Math.max(soonest, (t1 - go) / 1000);
		const vc = a * (T - Math.sqrt(Math.max(0, T * T - (2 * all) / a)));
		const upTo = (vc * vc) / (2 * a);
		let v = 0;
		path.forEach((p, k) => {
			const d = gone[k]!;
			const at =
				k === path.length - 1
					? T
					: d <= upTo
						? Math.sqrt((2 * d) / a)
						: vc / a + (d - upTo) / vc;
			v = m.getOff(
				p,
				go + at * 1000,
				v,
				a,
				move,
				Math.max(0, go + square - m.t),
			);
		});
		return v;
	}
	// From `from` to `to` bent toward `via` on the way (a curve pulled
	// toward it, not through it): the points along it, so many.
	curve(from: Pt, via: Pt, to: Pt, n: number): Pt[] {
		const pts: Pt[] = [];
		for (let k = 1; k <= n; k++) {
			const s = k / n;
			const a = (1 - s) * (1 - s);
			const b = 2 * (1 - s) * s;
			const c = s * s;
			pts.push({
				x: a * from.x + b * via.x + c * to.x,
				y: a * from.y + b * via.y + c * to.y,
			});
		}
		return pts;
	}

	jostle(men: Actor[], t0: number, t1: number) {
		const r = this.r;
		const times: number[] = [];
		for (
			let t = t0 + JOSTLE_MS * (0.6 + r() * 0.6);
			t < t1 - JOSTLE_MS * 0.6;
			t += JOSTLE_MS * (0.7 + r() * 0.6)
		) {
			times.push(t);
		}
		let side = r() < 0.5 ? 1 : -1;
		let was: OPt = { d: 0, a: 0 };
		const shifts = times.map((t, k) => {
			side = -side;
			const want = {
				d: (r() - 0.5) * 2 * JOSTLE_D,
				a: side * (JOSTLE_A[0] + r() * (JOSTLE_A[1] - JOSTLE_A[0])),
			};
			// (Eased into it - see below: at its quickest PI / 2 times as
			// quick as over all of it.)
			const most =
				(JOSTLE_MOST_FPS * 2 * (t - (times[k - 1] ?? t0))) / Math.PI / 1000;
			const far = Math.hypot(want.d - was.d, want.a - was.a);
			const u = far > most ? most / far : 1;
			was = {
				d: was.d + (want.d - was.d) * u,
				a: was.a + (want.a - was.a) * u,
			};
			return was;
		});
		const over = (p: Pt, o: OPt) => {
			const q = this.of(p);
			return this.w({ d: q.d + o.d, a: q.a + o.a });
		};
		// (Over from one shove to the next easing in and out of it, not
		// flung from one to the other.)
		const keys = [
			{ t: t0, o: { d: 0, a: 0 } },
			...times.map((t, k) => ({ t, o: shifts[k]! })),
			{ t: t1, o: { d: 0, a: 0 } },
		];
		const steps: number[] = [];
		for (let k = 0; k + 1 < keys.length && times.length > 0; k++) {
			const a = keys[k]!;
			const b = keys[k + 1]!;
			const n = Math.max(1, Math.round((b.t - a.t) / JOSTLE_STEP_MS));
			for (let j = 1; j < (k + 2 < keys.length ? n + 1 : n); j++) {
				steps.push(a.t + ((b.t - a.t) * j) / n);
			}
		}
		if (steps.length === 0) {
			return;
		}
		// (And each end of every leg in it as far over as he is by then -
		// a man still on his way into it too, not just at each shove.)
		const offAt = (t: number): OPt => {
			const k = keys.findIndex((x, j) => j > 0 && t <= x.t);
			const a = keys[k - 1];
			const b = keys[k];
			if (!a || !b || b.t <= a.t) {
				return { d: 0, a: 0 };
			}
			const u = (1 - Math.cos((Math.PI * (t - a.t)) / (b.t - a.t))) / 2;
			return { d: a.o.d + (b.o.d - a.o.d) * u, a: a.o.a + (b.o.a - a.o.a) * u };
		};
		for (const m of men) {
			// (Not a man still on his way into it, though: from when he gets
			// to him, eased into it - see JOSTLE_IN_MS - not his run in at
			// him bent this way and that.)
			const hit = Math.max(
				t0,
				m.legs.find((l) => l.move === "engage" && l.t1 > t0)?.t0 ?? t0,
			);
			const into = (t: number, o: OPt): OPt => {
				const u = clamp((t - hit) / JOSTLE_IN_MS, 0, 1);
				const w = hit > t0 ? (1 - Math.cos(Math.PI * u)) / 2 : 1;
				return { d: o.d * w, a: o.a * w };
			};
			for (const t of steps) {
				if (t > hit) {
					m.split(t);
				}
			}
			for (const l of m.legs) {
				if (l.t0 > hit && l.t0 < t1) {
					l.from = over(l.from, into(l.t0, offAt(l.t0)));
				}
				if (l.t1 > hit && l.t1 < t1) {
					l.to = over(l.to, into(l.t1, offAt(l.t1)));
				}
			}
		}
	}

	// When he first gets `d` downfield, from the snap - or never, before he
	// is down.
	reachesAt(m: Actor, d: number): number {
		for (let c = this.input.snap; c <= m.t; c += 50) {
			if (this.of(m.posAt(c)).d >= d) {
				return c;
			}
		}
		return Infinity;
	}

	// ---- the kicking game ------------------------------------------------------------

	get defense(): Side {
		return (1 - this.input.offense) as Side;
	}

	// The long snap: back between the snapper's legs to `to`'s hands, `z`
	// feet up at `at` - the punter's, the holder's. When it gets there.
	longSnap(to: Actor, at: OPt, z: number): number {
		const { snap } = this.input;
		const from: Pt3 = { ...this.w({ d: -0.9, a: 0 }), z: 1 };
		const end: Pt3 = { ...this.w(at), z };
		const t0 = snap + 40;
		const t1 =
			t0 + (Math.hypot(dist(from, end), end.z - from.z) / LONG_SNAP_FPS) * 1000;
		this.ball.push(
			{ kind: "fly", t0, t1, from, to: end, peak: 0.8, spiral: true },
			{ kind: "held", t0: t1, pid: to.pid },
		);
		this.role("LS")?.act("longSnap", snap + 450, this.ahead);
		return t1;
	}

	// The kicker up to the ball - his plant foot down beside it at
	// `plantAt` - and through it. When his foot meets it.
	placeKick(k: Actor, spot: OPt, plantAt: number): number {
		k.go(this.w({ d: spot.d - 1.7, a: spot.a - 1.5 }), plantAt);
		k.act(
			"kick",
			plantAt + KICK_MS,
			this.ahead,
			this.w({ d: spot.d + 1.4, a: spot.a - 1.2 }),
		);
		return plantAt + KICK_MS * KICK_CONTACT;
	}

	// Of `men`, the one lined up nearest across the field to `a`.
	acrossFrom(men: Actor[], a: number): Actor | undefined {
		let best: Actor | undefined;
		for (const m of men) {
			if (
				!best ||
				Math.abs(this.of(m.at).a - a) < Math.abs(this.of(best.at).a - a)
			) {
				best = m;
			}
		}
		return best;
	}

	// Each blocker on one of `men`, the nearest across the field first.
	pairUp(blockers: Actor[], men: Actor[]): [Actor, Actor][] {
		const all = blockers
			.flatMap((b) =>
				men.map((c) => {
					const p = this.of(b.at);
					const q = this.of(c.at);
					return { b, c, far: Math.abs(p.a - q.a) + 0.3 * Math.abs(p.d - q.d) };
				}),
			)
			.sort((x, y) => x.far - y.far);
		const used = new Set<Actor>();
		const out: [Actor, Actor][] = [];
		for (const { b, c } of all) {
			if (!used.has(b) && !used.has(c)) {
				used.add(b);
				used.add(c);
				out.push([b, c]);
			}
		}
		// (No two of them across each other's way to their men - swapped,
		// each on the other's: not run through each other to get to them.)
		for (let n = 0; n < out.length * out.length; n++) {
			let swapped = false;
			for (let i = 0; i < out.length; i++) {
				for (let j = i + 1; j < out.length; j++) {
					const [b1, c1] = out[i]!;
					const [b2, c2] = out[j]!;
					if (linesCross(b1.at, c1.at, b2.at, c2.at)) {
						out[i] = [b1, c2];
						out[j] = [b2, c1];
						swapped = true;
					}
				}
			}
			if (!swapped) {
				break;
			}
		}
		return out;
	}

	// Where and when b, waiting for a man coming from `cAt` (off at `cGo`,
	// at `vc` up from `cV0` at `cA`) who would not get to him in time, meets
	// him instead - gone up to him, off as late as has them met by
	// LOCK_UP_SPARE_MS before it is too late (or as soon as they can be),
	// at no more than `fps`. (Not in time even so: undefined.)
	private upToMeet(
		b: Actor,
		cAt: Pt,
		cGo: number,
		vc: number,
		cV0: number,
		cA: number,
		fps: number,
		until: number,
	): { meet: Pt; tm: number } | undefined {
		const bAt = b.at;
		const D = dist(cAt, bAt);
		const last = until - LOCK_LEAST_MS;
		const by = last - LOCK_UP_SPARE_MS;
		const cTo = (u: number) => cGo + rampedMs(u * D, vc, cV0, cA);
		const bTo = (u: number) =>
			b.t + rampedMs((1 - u) * D, fps, 0, RELEASE_ACCEL);
		const split = (sooner: (u: number) => boolean) => {
			let lo = 0;
			let hi = 1;
			for (let i = 0; i < LOCK_MEET_TRIES; i++) {
				const mid = (lo + hi) / 2;
				if (sooner(mid)) {
					lo = mid;
				} else {
					hi = mid;
				}
			}
			return (lo + hi) / 2;
		};
		let u = split((x) => cTo(x) < by);
		let tm = by;
		if (bTo(u) > by) {
			u = split((x) => cTo(x) < bTo(x));
			tm = Math.max(cTo(u), bTo(u));
		}
		if (tm > last || D <= LOCK_APART * 2) {
			return undefined;
		}
		return {
			meet: { x: cAt.x + (bAt.x - cAt.x) * u, y: cAt.y + (bAt.y - cAt.y) * u },
			tm,
		};
	}

	// A blocker and a cover man at each other from `go` - the cover man all
	// but flat out, the blocker at `bv` feet a second (or, 0, waiting for
	// him where he is) - squared up where they meet, and locked up there,
	// the two of them drifting along with the return, until `until`.
	lockUp(
		b: Actor,
		c: Actor,
		go: number,
		bv: number,
		endPt: Pt,
		until: number,
		carrier?: Actor,
		upTo?: number,
	): Lock {
		// (Each at his own pace, and off on his own count - see countOf -
		// not two blocks met on the same step.)
		const vc =
			c.top *
			(LOCK_PACE + LOCK_PACE_SPREAD * (countOf(c, this.input.key, 7) - 0.5));
		c.hold(go + LOCK_GO_SPREAD_MS * countOf(c, this.input.key, 10));
		const cGo = c.t;
		const cAt = c.at;
		// (Up to speed off what he was doing - out of his stance, from
		// nothing - no sooner there than that gets him there.)
		const cInto = SET.has(c.last.move) ? undefined : paceOn(c.last, cAt, b.at);
		const cV0 = cInto ?? 0;
		const cA = cInto === undefined ? RELEASE_ACCEL : RAMP_ACCEL;
		let meet: Pt;
		let tm: number;
		let moved = false;
		if (bv <= 0) {
			// (On his way to where he waits still, at a run: pulled up there
			// over a few strides - see STOP_ACCEL - not stopped dead on it
			// as the man gets to him.)
			const bl = b.last;
			if (RAMPS.has(bl.move) && legPace(bl) >= STOP_LEAST) {
				b.easeOff(EASE_OFF_FT);
			}
			// (Squared up to him where he is - not a step back from it to
			// square up, in a blink, if he is on the move still.)
			const far = dist(cAt, b.at);
			meet =
				far > LOCK_APART * 2
					? {
							x: b.at.x + ((cAt.x - b.at.x) * LOCK_APART) / far,
							y: b.at.y + ((cAt.y - b.at.y) * LOCK_APART) / far,
						}
					: b.at;
			tm = Math.max(b.t, cGo + rampedMs(dist(cAt, meet), vc, cV0, cA));
			if (upTo !== undefined && tm > until - LOCK_LEAST_MS) {
				const up = this.upToMeet(b, cAt, cGo, vc, cV0, cA, upTo, until);
				if (up) {
					meet = up.meet;
					tm = up.tm;
					moved = true;
				}
			}
		} else {
			b.hold(go + LOCK_REACT_SPREAD_MS * countOf(b, this.input.key, 7));
			const bAt = b.at;
			const D = dist(bAt, cAt);
			// (Met where the two of them get to at the same time, each up to
			// speed off what he was doing - the blocker as much as the man
			// coming, not off at his pace from a standing start.)
			const bInto = SET.has(b.last.move) ? undefined : paceOn(b.last, bAt, cAt);
			const bV0 = bInto ?? 0;
			const bA = bInto === undefined ? RELEASE_ACCEL : RAMP_ACCEL;
			const cTo = (u: number) => cGo + rampedMs(u * D, vc, cV0, cA);
			const bTo = (u: number) => b.t + rampedMs((1 - u) * D, bv, bV0, bA);
			let lo = 0;
			let hi = 1;
			for (let i = 0; i < LOCK_MEET_TRIES; i++) {
				const mid = (lo + hi) / 2;
				if (cTo(mid) < bTo(mid)) {
					lo = mid;
				} else {
					hi = mid;
				}
			}
			const u = (lo + hi) / 2;
			meet = {
				x: cAt.x + (bAt.x - cAt.x) * u,
				y: cAt.y + (bAt.y - cAt.y) * u,
			};
			tm = Math.max(
				Math.max(b.t, cGo) + 150 + (D / (vc + bv)) * 1000,
				cTo(u),
				bTo(u),
			);
		}
		// Squared up to each other, a step apart.
		const ang = Math.atan2(cAt.y - meet.y, cAt.x - meet.x);
		const cHit = {
			x: meet.x + Math.cos(ang) * LOCK_APART,
			y: meet.y + Math.sin(ang) * LOCK_APART,
		};
		const bHit = {
			x: meet.x - Math.cos(ang) * LOCK_APART,
			y: meet.y - Math.sin(ang) * LOCK_APART,
		};
		if (tm > until - LOCK_LEAST_MS) {
			// (Not to him before it is over: on his way in still, pulled up
			// with the rest - see afterWhistle - and the other not set in
			// his stance for a man who never comes.)
			c.go(
				cHit,
				tm,
				gait(
					Math.min(vc, paceFor(dist(cAt, cHit), (tm - c.t) / 1000, cV0, cA)),
				),
			);
			const run = c.last;
			return { b, c, go: cGo, run, lock: run };
		}
		if (bv <= 0 && !moved) {
			// (A long wait: up watching it first, and down into his stance
			// as the man comes - not crouched there the whole time; alive in
			// it either way, not stood like a statue.)
			const set =
				tm -
				200 -
				LOCK_SET_MS[0] -
				LOCK_SET_MS[1] * countOf(b, this.input.key, 20);
			if (set - b.t >= LOCK_WATCH_MS) {
				b.hold(set, "safetyReady", ang);
			}
			b.hold(tm - 200, "backerReady", ang);
		} else {
			const far = dist(b.at, bHit);
			const step = (far / LOCK_STEP_FPS) * 1000;
			if (
				(far / Math.max(0.001, tm - b.t)) * 1000 < WALK_MOST_FPS &&
				tm - step - b.t >= LOCK_STEP_SET_MS
			) {
				b.hold(tm - step, "backerReady", ang);
			}
		}
		// (Set waiting, and a step to him no more than this: set on till he
		// gets there, and taken up in the fight - not a stride back or
		// forward for a blink of it.)
		if (dist(b.at, bHit) < LOCK_SHIFT_FT && LOCK_SHIFTS.has(b.last.move)) {
			b.hold(tm, b.last.move, ang);
		} else {
			// (There already, near enough: not a blink of a step on to it.)
			if (dist(b.at, bHit) > LOCK_THERE_FT || tm - b.t > LOCK_THERE_MS) {
				b.go(bHit, tm, undefined, ang);
			}
			// (Into him gathered, not run into him flat out - see gatherTo.)
			b.gatherTo(b.at, BRAKE_FPS, HIT_ACCEL, 0, "drive");
		}
		c.go(
			cHit,
			tm,
			gait(Math.min(vc, paceFor(dist(cAt, cHit), (tm - c.t) / 1000, cV0, cA))),
			ang + Math.PI,
		);
		if (c.gatherTo(c.at, BRAKE_FPS, HIT_ACCEL, 0, "engage") === undefined) {
			c.breakDown(LOCK_KEEP);
		}
		const run = c.last;
		// Along with the return a little, each fighting the other.
		const end = this.of(endPt);
		const m = this.of(meet);
		const by = carrier ? this.goneBy(carrier, meet, tm, until) : until;
		const t1 = Math.max(
			Math.min(until, by + LOCK_PAST_MS) +
				LOCK_END_SPREAD_MS * countOf(b, this.input.key, 11),
			tm + 300,
		);
		let dd = clamp(end.d - m.d, -5, 5) * 0.4;
		let da = clamp(end.a - m.a, -10, 10) * 0.45;
		const most = (LOCK_DRIFT_FPS * (t1 - tm)) / 1000;
		const drift = Math.hypot(dd, da);
		if (drift > most) {
			dd *= most / drift;
			da *= most / drift;
		}
		const shift = (p: Pt) => {
			const o = this.of(p);
			return this.w({ d: o.d + dd, a: o.a + da });
		};
		c.go(shift(cHit), t1, "engage", ang + Math.PI);
		const lock = c.last;
		b.go(shift(bHit), t1, "drive", ang);
		return { b, c, go: cGo, run, lock };
	}

	// After the play from wherever he is: up near where it ends by the
	// whistle, at an easy run.
	trail(m: Actor, pt: Pt) {
		const r = this.r;
		const ang = Math.atan2(m.at.y - pt.y, m.at.x - pt.x) + (r() - 0.5) * 0.6;
		const ring = 9 + r() * 8;
		const spotAt = (a: number) => ({
			x: pt.x + Math.cos(a) * ring,
			y: pt.y + Math.sin(a) * ring,
		});
		// (Not where another trailer is going: on round the ring a little,
		// one way or the other, from where he comes.)
		let to = spotAt(ang);
		const step = TRAIL_APART_FT / ring;
		for (
			let k = 1;
			k <= 6 && this.trailing.some((q) => dist(q, to) < TRAIL_APART_FT);
			k++
		) {
			to = spotAt(ang + (k % 2 === 1 ? 1 : -1) * Math.ceil(k / 2) * step);
		}
		this.trailing.push(to);
		// (Well round from the way he was going: planted first - see
		// plantFor - not turned at full speed; and down in it to the pace
		// he goes on at, not out of it at full tilt and into a jog.)
		const soon = Math.max(
			m.t + 400,
			this.whistle + 300 + TRAIL_SPREAD_MS * countOf(m, this.input.key, 8),
		);
		m.plantFor(
			to,
			undefined,
			Math.max(
				TRAIL_JOG_FPS[0],
				dist(m.at, to) / Math.max(0.001, (soon - m.t) / 1000),
			),
		);
		// (Brought by that nearer it than he can pull up in from the pace
		// he is going: eased off on past it - not braked from a sprint to a
		// walk in a stride on it.)
		const l = m.last;
		const v = RAMPS.has(l.move) && !l.arms && !l.turn ? legPace(l) : 0;
		if (
			v >= STOP_LEAST &&
			dist(m.at, to) < (v * v) / (2 * STOP_ACCEL * STOP_HARD)
		) {
			m.easeOff((v * v) / (2 * STOP_ACCEL));
			return;
		}
		const by = Math.max(
			m.t + 400,
			this.whistle + 300 + TRAIL_SPREAD_MS * countOf(m, this.input.key, 8),
		);
		const far = dist(m.at, to);
		if (
			(far > TRAIL_JOG_LEAST_FT || by - m.t > TRAIL_IDLE_MS) &&
			(far / Math.max(0.001, by - m.t)) * 1000 < WALK_MOST_FPS
		) {
			// (Slower than a walk gets him there by the whistle - or, a little
			// after it, as he would: a look at it,
			// and along after the ball at a jog, kept off it - with it as it
			// comes back his way - and in to where it ends, not walked in
			// while it is still going on.)
			const look = m.stoppedBy(
				m.t +
					TRAIL_LOOK_MS[0] +
					(TRAIL_LOOK_MS[1] - TRAIL_LOOK_MS[0]) *
						countOf(m, this.input.key, 29),
			);
			const fps =
				TRAIL_JOG_FPS[0] +
				(TRAIL_JOG_FPS[1] - TRAIL_JOG_FPS[0]) * countOf(m, this.input.key, 30);
			m.hold(look);
			this.alongWith(m, fps, Math.max(ring, TRAIL_KEEP_FT), this.whistle - 300);
			if (dist(m.at, to) > 1) {
				m.runUp(to, fps);
			}
			return;
		}
		this.chaseClear(m, to, by, m.top * 0.7);
	}

	// Stood where he is till `until` - but back off a step or two first if
	// the ball comes to within GIVE_WAY_FT of him there, the men about it
	// with it: not stood for them to run into.
	giveWay(m: Actor, until: number) {
		// (Backed off as far as the nearest it comes, away from it there -
		// and before it does.)
		let hit: number | undefined;
		let near = Infinity;
		let at: Pt | undefined;
		let nearT = until;
		const look = () => {
			hit = undefined;
			near = Infinity;
			for (let c = m.t; c <= until; c += 50) {
				const p = this.ballAt(c);
				if (!p) {
					continue;
				}
				const d = dist(p, m.at);
				if (d < GIVE_WAY_FT && hit === undefined) {
					hit = c;
				}
				if (d < near) {
					near = d;
					at = p;
					nearT = c;
				}
			}
		};
		look();
		if (hit === undefined || !at) {
			m.hold(until, "stand", this.ahead);
			return;
		}
		// (Pulled up first, if on the move still - not turned back off it
		// in a stride.)
		if (RAMPS.has(m.last.move) && legPace(m.last) >= GIVE_WAY_EASE_LEAST) {
			m.easeOff(GIVE_WAY_EASE_FT, at);
			look();
			if (hit === undefined || !at) {
				m.hold(until, "stand", this.ahead);
				return;
			}
		}
		const d = Math.max(0.1, near);
		const far = Math.min(GIVE_WAY_MOST_FT, GIVE_WAY_FT + 0.5 - near);
		const face = this.toward(m.at, at);
		const ms = Math.max(GIVE_WAY_MS, (far / GIVE_WAY_FPS) * 1000);
		const end = Math.max(m.t + ms, Math.min(nearT, hit + ms));
		m.hold(end - ms, "stand", this.ahead);
		m.go(
			{
				x: m.at.x + ((m.at.x - at.x) / d) * far,
				y: m.at.y + ((m.at.y - at.y) / d) * far,
			},
			end,
			"back",
			face,
		);
		m.hold(until, "stand", face);
	}

	// After the ball at `fps` till `until`, a leg at a time (as pursue) -
	// kept `keep` off it, and with it as it comes his way, not in on it.
	alongWith(m: Actor, fps: number, keep: number, until: number) {
		// (Off the others there then, the man with it aside - see
		// ALONG_APART_FT.)
		const apart = (p: Pt, c: number): Pt => {
			let q = p;
			for (const x of this.men) {
				if (x === m || x.t < c) {
					continue;
				}
				const l = legAt(x, c);
				if (l.ball || DOWN.has(l.move)) {
					continue;
				}
				const o = x.posAt(c);
				const d = dist(o, q);
				if (d >= ALONG_APART_FT) {
					continue;
				}
				// (Right on him: out to the side of the way he goes.)
				const wx =
					d > 0.01
						? (q.x - o.x) / d
						: -(q.y - m.at.y) / Math.max(0.01, dist(m.at, q));
				const wy =
					d > 0.01
						? (q.y - o.y) / d
						: (q.x - m.at.x) / Math.max(0.01, dist(m.at, q));
				q = { x: o.x + wx * ALONG_APART_FT, y: o.y + wy * ALONG_APART_FT };
			}
			return q;
		};
		while (m.t + PURSUIT_STEP < until) {
			const c = m.t + PURSUIT_STEP;
			const ball = this.ballAt(c + PURSUIT_LEAD);
			const now = this.ballAt(m.t + PURSUIT_LEAD);
			if (!ball || !now) {
				break;
			}
			const near = dist(m.at, ball);
			const a = Math.atan2(m.at.y - ball.y, m.at.x - ball.x);
			if (near < keep) {
				// (Out off it a little as he goes, if he is right on it - not
				// along with it in the man with it.)
				const out = clamp(ALONG_NEAREST_FT - near, 0, ALONG_OUT_FT);
				m.chaseUp(
					this.wayClear(
						m,
						apart(
							{
								x: m.at.x + ball.x - now.x + Math.cos(a) * out,
								y: m.at.y + ball.y - now.y + Math.sin(a) * out,
							},
							c,
						),
						c,
						fps,
					),
					c,
					fps,
				);
				continue;
			}
			// (Round the men still in blocks, not on through them - see
			// wayClear.)
			m.chaseUp(
				this.wayClear(
					m,
					apart(
						{ x: ball.x + Math.cos(a) * keep, y: ball.y + Math.sin(a) * keep },
						c,
					),
					c,
					fps,
				),
				c,
				fps,
			);
		}
	}

	// THE OTHERS OUT ON ROUTES ONCE IT IS CAUGHT (or the quarterback takes
	// off with it): their routes run out (not cut short under the man on
	// them, who has been set going off them), a look at it, and after it at
	// a jog, each on his own count - kept well off it, and with it as it
	// comes his way - not stood where his route left him till the whistle.
	// (Not `near`: left be if it is near enough him already.)
	withIt(
		men: Actor[],
		catchAt: number,
		keepFt: readonly [number, number] = ROUTE_AFTER_KEEP_FT,
		fpsOf: readonly [number, number] = ROUTE_AFTER_FPS,
		near = true,
	) {
		const caught = catchAt + BREAK_MS;
		for (const m of men) {
			if (this.done.has(m.pid)) {
				continue;
			}
			const from = m.stoppedBy(
				Math.max(m.t, caught) +
					ROUTE_AFTER_MS[0] +
					(ROUTE_AFTER_MS[1] - ROUTE_AFTER_MS[0]) *
						countOf(m, this.input.key, 31),
			);
			const keep = keepFt[0] + keepFt[1] * countOf(m, this.input.key, 33);
			const ball = this.ballAt(from);
			if (
				!ball ||
				this.whistle - 300 - from < ROUTE_AFTER_LEAST_MS ||
				(!near && dist(m.at, ball) < keep + ROUTE_AFTER_LEAST_FT)
			) {
				continue;
			}
			m.hold(from);
			this.alongWith(
				m,
				fpsOf[0] + (fpsOf[1] - fpsOf[0]) * countOf(m, this.input.key, 32),
				keep,
				this.whistle - 300,
			);
		}
	}

	// Blockers whose men got off them - to make the tackle, or away from
	// the play to go after it: off after it themselves.
	release(locks: Lock[], endPt: Pt) {
		for (const x of locks) {
			const { b, c } = x;
			if (this.done.has(b.pid)) {
				continue;
			}
			const off = c.legs.includes(x.lock)
				? x.lock.t1
				: c.legs.includes(x.run)
					? x.run.t1
					: x.go;
			if (off < b.t - 50) {
				b.cut(off);
			}
			if (b.t < this.whistle - 300) {
				this.trail(b, endPt);
			}
		}
	}

	// A return: up the field from the catch, then off into his lane - toward
	// the man the sim says brings him down, or one side or the other of
	// `aim` - to `endD`. When he gets there.
	returnRun(m: Actor, endD: number, seenAt: number, aim: number): number {
		const r = this.r;
		const from = this.of(m.at);
		const ta = this.tacklerA(m, seenAt);
		const lane =
			ta !== undefined ? ta + (r() - 0.5) * 16 : aim + (r() - 0.5) * 60;
		const endA = clamp(lane, this.bounds.left + 6, this.bounds.right - 6);
		const run = from.d - endD;
		// Now and then on a long one, the field reversed: off hard the other
		// way first, up the field and toward the far side from where it
		// ends, then planted (see run) and back across the field - see
		// REVERSE.
		const away =
			Math.sign(from.a - endA) ||
			(countOf(m, this.input.key, 132) < 0.5 ? 1 : -1);
		const awayA = clamp(
			from.a + away * REVERSE_OUT,
			this.bounds.left + REVERSE_EDGE,
			this.bounds.right - REVERSE_EDGE,
		);
		if (
			run >= REVERSE_RUN &&
			Math.abs(awayA - endA) >= REVERSE_ACROSS &&
			countOf(m, this.input.key, 131) < REVERSE
		) {
			// (Round up the field at the far end of it, not stopped dead and
			// sent back the way he came.)
			m.run(
				this.w({ d: from.d - REVERSE_UP, a: awayA }),
				m.top * REVERSE_PACE,
				"sprint",
			);
			m.run(
				this.w({
					d: from.d - REVERSE_UP - REVERSE_ROUND[0],
					a: awayA - away * REVERSE_ROUND[1],
				}),
				m.top * REVERSE_PACE,
				"sprint",
			);
			m.run(
				this.w({
					d: from.d - REVERSE_UP - REVERSE_ROUND[0] - REVERSE_BACK_UP,
					a: endA + (awayA - endA) * REVERSE_BACK_SHORT,
				}),
				m.top * 0.9,
				"sprint",
			);
		} else if (run > 24) {
			m.run(
				this.w({
					d: from.d - Math.min(36, run * 0.35),
					a: from.a + (endA - from.a) * 0.25,
				}),
				m.top * 0.82,
				"sprint",
			);
		}
		const end = this.w({ d: endD, a: endA });
		if (dist(m.at, end) > 2) {
			m.run(end, m.top * 0.93, "sprint");
		} else {
			m.hold(m.t + 150);
		}
		return m.t;
	}

	// Under a kick coming down at `land`, a step behind it, and up into it
	// as it comes - caught at `catchAt`, from `from` (when he can go). Where
	// his hands have it.
	// (`how`: set under it, the default; "up", set back off it and up into
	// it at a run, caught in his stride - see FIELD_UP_RUN; "back", short
	// of it and caught going back into it.)
	getUnder(
		m: Actor,
		land: OPt,
		from: number,
		catchAt: number,
		how: "set" | "up" | "back" = "set",
		onRun = true,
	): Pt3 {
		m.hold(from);
		if (how !== "set") {
			const caught = this.fieldOnMove(m, land, catchAt, how);
			if (caught) {
				return caught;
			}
		}
		// (Not there in time - a low one well short of him, say: as far
		// toward it as he can get, and it to him there - not him shot the
		// rest of the way to it as he catches it.)
		let to = this.w({ d: land.d + 3.5, a: land.a });
		const by = catchAt - 700;
		// (Stood there by then at the latest, a beat before he reaches up
		// for it.)
		const late = catchAt - CATCH_REACH_MS - UNDER_SETTLE_MS;
		const most = setReach(late - m.t, m.top * 0.95, UNDER_OFF_ACCEL);
		const far = dist(m.at, to);
		if (far > most && onRun && catchAt > m.t) {
			// (No time to be stood under it - one well across from him, say:
			// on to it at a run, and caught in his stride as he gets there -
			// not run there flat out and stopped dead under it as it comes.)
			m.chase(this.w({ d: land.d + 1, a: land.a }), catchAt, m.top * 0.95);
			// (Where he has it, his way on from there not bent round short of
			// it - the ball to his hands.)
			m.pin(catchAt);
			this.reaches.push({
				m,
				move: "catch",
				t0: catchAt - CATCH_REACH_MS,
				t1: catchAt + CATCH_IN_MS,
			});
			const now = this.of(m.posAt(catchAt));
			return { ...this.w({ d: now.d - 1, a: now.a }), z: CATCH_Z };
		}
		if (far > most) {
			const u = most / far;
			to = {
				x: m.at.x + (to.x - m.at.x) * u,
				y: m.at.y + (to.y - m.at.y) * u,
			};
		}
		const spot = this.of(to);
		this.setBack(m, to, by, late);
		m.hold(catchAt - CATCH_REACH_MS, "stand", this.behind);
		// (Now and then in a basket, his elbows in - not his hands up at it:
		// in at his belly, then.)
		const basket = countOf(m, this.input.key, 125) < BASKET;
		m.catchOn(
			catchAt,
			this.behind,
			this.w({ d: spot.d - 2.7, a: spot.a }),
			basket ? "basket" : "catch",
		);
		const at = this.of(m.posAt(catchAt));
		return {
			...this.w({ d: at.d - (basket ? BASKET_IN : 1), a: at.a }),
			z: basket ? BASKET_Z : CATCH_Z,
		};
	}

	// Fielded on the move (see getUnder): there in time, or nothing (and
	// nothing done) if he can't be.
	fieldOnMove(
		m: Actor,
		land: OPt,
		catchAt: number,
		how: "up" | "back",
	): Pt3 | undefined {
		const r = this.r;
		const at = { d: land.d + 1, a: land.a };
		const run =
			how === "up"
				? FIELD_UP_RUN[0] +
					(FIELD_UP_RUN[1] - FIELD_UP_RUN[0]) * countOf(m, this.input.key, 96)
				: FIELD_BACK_RUN;
		const fps =
			how === "up"
				? m.top *
					(FIELD_UP_PACE[0] +
						(FIELD_UP_PACE[1] - FIELD_UP_PACE[0]) *
							countOf(m, this.input.key, 97))
				: FIELD_BACK_FPS;
		// (Off a little to one side of where it comes down, coming up - not
		// straight up the line of it every time.)
		const off = how === "up" ? (r() - 0.5) * FIELD_UP_SLANT : 0;
		const start = this.w({
			d: at.d + (how === "up" ? run : -run),
			a: at.a + off,
		});
		// (Up to speed from a stand - see run - and on at it: how long that
		// takes.)
		const ta = Math.min(
			fps / RELEASE_ACCEL,
			Math.sqrt((2 * run) / RELEASE_ACCEL),
		);
		const ms =
			how === "up"
				? (ta + Math.max(0, run - (RELEASE_ACCEL * ta * ta) / 2) / fps) * 1000
				: (run / fps) * 1000;
		const go = catchAt - ms;
		// (Set there a moment before he comes up; going back, on into it in
		// the one drift.)
		const by = go - (how === "up" ? FIELD_SET_MS : 0);
		// (Stood there by then - up to speed and eased off it, not stopped
		// dead off a sprint: see setReach.)
		if (
			by <= m.t ||
			dist(m.at, start) > setReach(by - m.t, m.top * 0.95, UNDER_OFF_ACCEL)
		) {
			// (No time to set back off it: straight up into it from where he
			// is, if it is in front of him and he can be there - in his
			// stride as it gets there.)
			const to = this.w(at);
			const T = catchAt - m.t;
			const fast = (dist(m.at, to) / Math.max(1, T)) * 1000;
			const front = Math.cos(this.toward(m.at, to) - this.behind);
			if (
				how !== "up" ||
				front < FIELD_UP_FRONT ||
				fast < FIELD_UP_LEAST_FPS ||
				fast > m.top * 0.95
			) {
				return undefined;
			}
			m.chase(to, catchAt, m.top * 0.95);
		} else {
			this.setBack(m, start, by);
			m.hold(go, "stand", this.behind);
			const to = this.w(at);
			if (how === "up") {
				m.run(to, fps);
			} else {
				m.go(to, catchAt, "back", this.behind);
			}
		}
		this.reaches.push({
			m,
			move: "catch",
			t0: catchAt - CATCH_REACH_MS,
			t1: catchAt + CATCH_IN_MS,
		});
		const now = this.of(m.posAt(catchAt));
		return { ...this.w({ d: now.d - 1, a: now.a }), z: CATCH_Z };
	}

	// To `to` by `by`, where a kick is coming down (see getUnder) - or, a
	// long way off, stood there by `late` at the latest.
	setBack(m: Actor, to: Pt, by: number, late = by) {
		const fps = (dist(m.at, to) / Math.max(1, by - m.t)) * 1000;
		const rel = this.toward(m.at, to) - this.behind;
		const c = Math.cos(rel);
		const across = Math.abs(c) < DRIFT_ACROSS;
		if (fps < (across ? SHUFFLE_MOST : DRIFT_FPS)) {
			// (Not far to go: under it with his eyes on it all the way, facing
			// the kick - back, across, or in toward it. Up to it out of his
			// stance and eased down under it, if there is the time - not off
			// at it and stopped dead there.)
			const move = across ? "slide" : c < 0 ? "back" : undefined;
			if (
				!m.stepTo(
					to,
					by,
					across ? SHUFFLE_MOST : c < 0 ? BACK_MOST : m.top * 0.85,
					move,
					this.behind,
				)
			) {
				m.press(to, by, m.top * 0.85, move ?? gait(fps), this.behind);
			}
		} else if (fps * UNDER_EARLY <= m.top * 0.85) {
			// (Further, with time: turned and run there, a little quicker
			// than he has to, and pulled up under it - not across in his low
			// steps at a run, nor stopped dead there.)
			m.runUp(to, fps * UNDER_EARLY);
		} else if (
			!m.stepTo(
				to,
				Math.max(by, late),
				m.top * 0.95,
				undefined,
				undefined,
				UNDER_OFF_ACCEL,
			)
		) {
			// (Further, and no more time than it takes: off to it out of his
			// stance hard, and eased down under it - see stepTo.)
			m.press(to, by, m.top * 0.85);
		}
	}

	// No return: the cover men on down the field a way and pulling up, the
	// rest up out of their stances.
	coverDown(cover: Actor[], others: Actor[]) {
		const r = this.r;
		for (const m of cover) {
			m.hold(this.seen + LOCK_GO_SPREAD_MS * countOf(m, this.input.key, 10));
			const s = this.of(m.at);
			m.chase(
				this.w({ d: s.d + 25 + r() * 25, a: s.a * 0.85 }),
				this.whistle + 900,
				m.top * 0.75,
			);
		}
		for (const m of others) {
			m.hold(this.seen + r() * 300);
			m.hold(this.whistle + 600, "stand");
		}
	}

	// ---- a kickoff -------------------------------------------------------------------

	kickoff(i: number) {
		const { snap } = this.input;
		const r = this.r;
		const e = this.ev(i);
		// Where it comes down: yards out from the goal line (behind it, less
		// than nothing).
		const kickTo = num(e.yds, 0);
		const k = this.role("K") ?? this.o[0]!;
		const kickT = this.placeKick(k, { d: 0, a: 0 }, snap + KICKOFF_RUN);
		this.at.set(i, kickT);
		this.follow = kickT;

		// Off to one side of the landing zone or the other - now and then,
		// on the kicker's own count, aimed out toward the numbers - or, out
		// the back, deep in the end zone.
		const deep = kickTo <= -10;
		const d0 = deep
			? this.goalD + 15 + r() * 12
			: Math.min(this.goalD - kickTo * YD, this.goalD + 27);
		const a0 = (r() - 0.5) * 80;
		const aimed =
			(countOf(k, this.input.key, 100) < 0.5 ? -1 : 1) *
			(KICK_OUT_FT[0] +
				(KICK_OUT_FT[1] - KICK_OUT_FT[0]) * countOf(k, this.input.key, 101));
		const land: OPt = {
			d: d0,
			a: clamp(
				countOf(k, this.input.key, 99) < KICK_AIM_OUT ? aimed : a0,
				this.bounds.left + 14,
				this.bounds.right - 14,
			),
		};
		const landPt = this.w(land);
		const tee: Pt3 = { ...this.w({ d: 0, a: 0 }), z: TEE_Z };
		const hang = clamp(2900 + dist(tee, landPt) * 5 + r() * 400, 2900, 4400);
		const landT = kickT + hang;
		// Nobody but the kicker and the returners moves till it is down.
		this.seen = landT + 150;

		const cover = this.o.filter((m) => m.role === "COVER");
		const front = this.d.filter((m) => m.role === "FRONT");
		const returners = this.d.filter((m) => m.role === "KR");
		const kr =
			this.man(this.statPids("kr")[0]) ??
			[...returners].sort((a, b) => dist(a.at, landPt) - dist(b.at, landPt))[0];
		const others = returners.filter((m) => m !== kr);
		const retI = this.find("kickoffReturn", i);
		const ret = retI >= 0 ? this.ev(retI) : undefined;
		k.go(this.w({ d: 30 + r() * 15, a: land.a * 0.3 }), landT + 600, "jog");

		if (deep || !kr) {
			// Over everybody and out the back: the returners back to the goal
			// line, watching it go.
			const out: Pt3 = {
				...this.w({
					d: this.goalD + END_ZONE + 6,
					a: land.a + (r() - 0.5) * 8,
				}),
				z: 0,
			};
			this.ball.push(
				{
					kind: "fly",
					t0: kickT,
					t1: landT,
					from: tee,
					to: { ...landPt, z: 0 },
					peak: peakFor(hang),
				},
				{
					kind: "fly",
					t0: landT,
					t1: landT + 800,
					from: { ...landPt, z: 0 },
					to: out,
					peak: 3,
				},
				{ kind: "spot", t0: landT + 800, at: out },
			);
			this.whistle = landT + 400;
			for (const m of returners) {
				m.hold(kickT + 300);
				m.chase(
					this.w({ d: this.goalD - 4, a: this.of(m.at).a }),
					landT,
					m.top * 0.6,
				);
				m.hold(landT + 300, "stand", this.ahead);
				this.done.add(m.pid);
			}
			this.coverDown(cover, front);
			return;
		}

		const noRet = !ret || e.touchback === true;
		if (
			noRet &&
			land.d > this.goalD + KICK_LET_IN &&
			countOf(kr, this.input.key, 103) < KICK_LET
		) {
			// Let go into the end zone, nobody's to get to it: him off to the
			// side of where it comes down, watching it in and on toward the
			// back of it - a touchback.
			const side = Math.sign(this.of(kr.at).a - land.a) || 1;
			const h1 = this.w({
				d: land.d + KICK_LET_HOP[0] + r() * KICK_LET_HOP[1],
				a: land.a + (r() - 0.5) * 4,
			});
			const on = this.of(h1);
			const h2 = this.w({
				d: Math.min(
					on.d + KICK_LET_ROLL[0] + r() * KICK_LET_ROLL[1],
					this.goalD + END_ZONE + 6,
				),
				a: on.a + (on.a - land.a),
			});
			const t1 = landT + KICK_LET_MS[0];
			const t2 = t1 + KICK_LET_MS[1];
			this.ball.push(
				{
					kind: "fly",
					t0: kickT,
					t1: landT,
					from: tee,
					to: { ...landPt, z: 0 },
					peak: peakFor(hang),
				},
				{
					kind: "fly",
					t0: landT,
					t1,
					from: { ...landPt, z: 0 },
					to: { ...h1, z: 0 },
					peak: KICK_LET_UP,
				},
				{
					kind: "fly",
					t0: t1,
					t1: t2,
					from: { ...h1, z: 0 },
					to: { ...h2, z: 0 },
					peak: 0.6,
				},
				{ kind: "spot", t0: t2, at: { ...h2, z: 0 } },
			);
			this.whistle = landT + 400;
			if (retI >= 0) {
				this.at.set(retI, this.whistle);
			}
			kr.hold(kickT + 300);
			kr.chase(
				this.w({ d: land.d - 3, a: land.a + side * KICK_LET_ASIDE }),
				landT - 500,
				kr.top * 0.6,
			);
			kr.hold(landT - 150, "stand", this.toward(kr.at, landPt));
			kr.hold(t2 + 1000, "stand", this.toward(kr.at, h2));
			this.done.add(kr.pid);
			for (const m of others) {
				m.hold(kickT + 300);
				m.chase(
					this.w({ d: this.goalD - 4, a: this.of(m.at).a }),
					landT,
					m.top * 0.6,
				);
				m.hold(landT + 300, "stand", this.ahead);
				this.done.add(m.pid);
			}
			this.coverDown(cover, front);
			return;
		}

		// The catch - on the fly, or a line drive fielded on the hop. On the
		// fly, on his own count: set under it, or back under one over where
		// he lined up, caught still going back; or up into one short of him
		// at a run, caught in his stride (not one he is to down in the end
		// zone).
		const hop = kickTo >= 0 && r() < 0.3;
		const catchAt = landT + (hop ? 650 : 0);
		const delta = land.d - this.of(kr.at).d;
		const how: "set" | "up" | "back" = hop
			? "set"
			: delta > FIELD_BACK_LEAST
				? countOf(kr, this.input.key, 94) < KICK_BACK
					? "back"
					: "set"
				: !noRet &&
					  countOf(kr, this.input.key, 94) <
							(delta < -FIELD_UP_LEAST ? KICK_UP : FIELD_UP_ANY)
					? "up"
					: "set";
		const room = cover.every(
			(m) => dist(m.posAt(landT), landPt) >= FIELD_UP_ROOM,
		);
		const hands = this.getUnder(
			kr,
			land,
			kickT + 250,
			catchAt,
			how === "up" && !room ? "set" : how,
			!hop && !noRet,
		);
		if (hop) {
			const bounce: Pt3 = {
				...this.w({ d: land.d - 14 - r() * 8, a: land.a + (r() - 0.5) * 4 }),
				z: 0,
			};
			this.ball.push(
				{
					kind: "fly",
					t0: kickT,
					t1: landT,
					from: tee,
					to: bounce,
					peak: peakFor(hang),
				},
				{
					kind: "fly",
					t0: landT,
					t1: catchAt,
					from: bounce,
					to: { ...hands, z: 3.2 },
					peak: 3.5,
				},
			);
		} else {
			this.ball.push({
				kind: "fly",
				t0: kickT,
				t1: catchAt,
				from: tee,
				to: hands,
				peak: peakFor(hang),
			});
		}
		// (Now and then one caught set under it on a return juggled - see
		// JUGGLE.)
		let gotT = catchAt;
		if (
			!hop &&
			!noRet &&
			(kr.last.move === "catch" || kr.last.move === "basket") &&
			countOf(kr, this.input.key, 95) <
				JUGGLE * (1 - JUGGLE_HANDS * (kr.flair.hands ?? 0))
		) {
			gotT = catchAt + JUGGLE_MS;
			const at = this.of(kr.posAt(catchAt));
			kr.catchOn(gotT, this.behind, this.w({ d: at.d - JUGGLE_ON, a: at.a }));
			const again = this.of(kr.posAt(gotT));
			this.ball.push({
				kind: "fly",
				t0: catchAt,
				t1: gotT,
				from: hands,
				to: { ...this.w({ d: again.d - 1, a: again.a }), z: hands.z },
				peak: JUGGLE_UP,
			});
		}
		this.ball.push({ kind: "held", t0: gotT, pid: kr.pid });
		// The other returner up in front of him, to lead him.
		for (const m of others) {
			m.hold(kickT + 300);
			m.chase(
				this.w({
					d: land.d - 15,
					a: land.a + (this.of(m.at).a > land.a ? 9 : -9),
				}),
				landT,
				m.top * 0.7,
			);
		}

		if (noRet) {
			// Downed in the end zone: a knee - at once, or a beat after, on
			// his own count.
			const knee =
				catchAt +
				KICK_KNEE_MS[0] +
				(KICK_KNEE_MS[1] - KICK_KNEE_MS[0]) * countOf(kr, this.input.key, 102);
			kr.hold(knee);
			kr.hold(knee + 1150, "kneelDown", this.behind);
			this.whistle = knee + 50;
			if (retI >= 0) {
				this.at.set(retI, this.whistle);
			}
			this.done.add(kr.pid);
			this.coverDown(cover, [...front, ...others]);
			return;
		}

		// The return, and the blocks for it: the front nine and the other
		// returner each on a man coming down.
		const td = ret.td === true;
		const endD = clamp(
			td
				? this.ownGoalD - 4 - r() * 12
				: this.goalD - (kickTo + num(ret.yds, 0)) * YD,
			this.ownGoalD - 27,
			this.goalD + 27,
		);
		const t = this.returnRun(kr, endD, landT, land.a);
		this.weave = true;
		const endPt = kr.at;
		const locks = this.pairUp([...front, ...others], cover).map(([b, c]) =>
			this.lockUp(
				b,
				c,
				this.seen,
				b.role === "KR" ? b.top * 0.75 : 5,
				endPt,
				Math.abs(this.of(b.at).a - this.of(endPt).a) > 45
					? this.seen + 1600 + r() * 800
					: t,
				kr,
			),
		);
		this.endReturn(retI, kr, endPt, t, td, this.o);
		this.release(locks, endPt);
	}

	// ---- an onside kick ------------------------------------------------------------

	// Kicked short, to be got back: driven down off the tee into the turf
	// and up off it in a high hop - or skipping along the ground - past the
	// ten yards it has to go. The kicking team after it from the kick, the
	// hands team up to meet it, and the man the sim says came up with it on
	// it: everybody near piling in, or, the hands team's, a few yards back
	// up the field with it.
	onside(i: number) {
		const { snap, events, i1, offense } = this.input;
		const r = this.r;
		const e = this.ev(i);
		const success = e.success === true;
		const td = e.td === true;
		const k = this.role("K") ?? this.o[0]!;
		const kickT = this.placeKick(k, { d: 0, a: 0 }, snap + KICKOFF_RUN);
		this.follow = kickT;
		this.seen = kickT + 150;
		const tee: Pt3 = { ...this.w({ d: 0, a: 0 }), z: TEE_Z };

		// Where it is got: the next snap's spot, less what the hands team got
		// back with it - past the ten yards, and not so far past.
		const next = events[i1];
		const spot =
			!td && next?.type === "clock" && typeof next.scrimmage === "number"
				? next.scrimmage
				: undefined;
		const back = success ? 0 : this.amount("krYds");
		const recD = clamp(
			spot === undefined
				? (10.5 + r() * 4) * YD
				: success
					? this.ownGoalD + spot * YD
					: this.goalD - (spot - back) * YD,
			10.5 * YD,
			25 * YD,
		);
		// Into the lane of the man who gets it, more or less.
		const side = success ? this.o : this.d;
		const named = this.man(this.statPids("onsRec")[0]);
		const known = named && side.includes(named) ? named : undefined;
		const aim = known
			? this.of(known.at).a * (0.75 + r() * 0.25)
			: (r() - 0.5) * 80;
		const at: OPt = {
			d: recD,
			a: clamp(
				aim + (r() - 0.5) * 10,
				this.bounds.left + 12,
				this.bounds.right - 12,
			),
		};
		const restPt = this.w(at);
		const rec =
			known ??
			[...side]
				.filter((m) => m !== k)
				.sort((a, b) => dist(a.at, restPt) - dist(b.at, restPt))[0] ??
			k;
		// The kicking team off with the kick, the hands team a beat after it.
		const goOf = (m: Actor) => (m.team === offense ? kickT - 250 : kickT + 200);
		const reach = (m: Actor, p: Pt) =>
			Math.max(goOf(m), m.t) + 250 + (dist(m.at, p) / (m.top * 0.9)) * 1000;
		const ahead = rec.team === offense;
		const face = ahead ? this.ahead : this.behind;

		// The kick: into the turf and up, or skipping along the ground, each
		// hop as long in the air as it is high - caught on the fly, or down
		// short and on (the longer, the longer it is before anybody has it)
		// to whoever gets a hand on it first. A good one for the kicking team
		// the hands team should have had: off the hands of the man who got
		// to it first, loose, and fallen on.
		const pop = recD < 16 * YD && r() < 0.6;
		const airMs = (peak: number) => Math.sqrt((8 * peak) / G) * 1000;
		const legs: [number, number][] = pop
			? [
					[(6 + r() * 5) / recD, 0.2],
					[1, 13 + r() * 7],
				]
			: [
					[0.18, 0.4],
					[0.42, 2.5 + r() * 3],
					[0.25, 1.1],
					[0.15, 0.35],
				];
		const landT = legs.reduce((t, [, peak]) => t + airMs(peak), kickT);
		const reachRest = reach(rec, restPt);
		const caught = pop && reachRest <= landT - 150;
		const returned = !success && (back > 0 || td);
		const first =
			success && !caught ? this.nearest(this.d, restPt, 1)[0] : undefined;
		const muff =
			first && reach(first, restPt) + 300 < reachRest ? first : undefined;
		let touch = restPt;
		if (muff) {
			const far = dist(muff.at, restPt) || 1;
			const off = 6 + r() * 4;
			touch = {
				x: restPt.x + ((muff.at.x - restPt.x) / far) * off,
				y: restPt.y + ((muff.at.y - restPt.y) / far) * off,
			};
		}
		const touchT = caught
			? landT
			: returned
				? Math.max(landT + 450, reachRest + 100)
				: muff
					? Math.max(landT + 450, reach(muff, touch) + 100)
					: clamp(reachRest - 300, landT + 450, landT + 1600);
		const tail = caught
			? 0
			: 5 + 8 * Math.min(1.15, (touchT - landT - 450) / 1000);
		const along = dist(tee, touch) || 1;
		const down: Pt3 = {
			x: touch.x - ((touch.x - tee.x) / along) * tail,
			y: touch.y - ((touch.y - tee.y) / along) * tail,
			z: 0,
		};
		const hands: Pt3 = { ...restPt, z: CATCH_Z + 0.8 };
		{
			let t = kickT;
			let from = tee;
			let u = 0;
			for (const [k, [share, peak]] of legs.entries()) {
				const last = k === legs.length - 1;
				u = last ? 1 : u + share;
				const to: Pt3 =
					caught && last
						? hands
						: {
								x: tee.x + (down.x - tee.x) * u,
								y: tee.y + (down.y - tee.y) * u,
								z: 0,
							};
				const t1 = t + airMs(peak);
				this.ball.push({ kind: "fly", t0: t, t1, from, to, peak });
				t = t1;
				from = to;
			}
		}

		// (Set where he is till he has to go to get there by `t` - not crept
		// up on it at a walk, nor shot off at it from a stand: up to speed
		// for it - see rampedMs.)
		const offFor = (m: Actor, to: Pt, t: number) =>
			m.hold(
				Math.max(
					goOf(m),
					t -
						ONSIDE_GO_MS -
						rampedMs(dist(m.at, to), m.top * ONSIDE_GO_PACE, 0, RELEASE_ACCEL),
				),
			);
		// Up for it as it comes, facing it, his hands on it at `t`.
		const handsOn = (m: Actor, at: Pt, t: number) => {
			const yaw = this.toward(at, down);
			const to = {
				x: at.x + Math.cos(yaw) * 1.5,
				y: at.y + Math.sin(yaw) * 1.5,
			};
			offFor(m, to, t - 250);
			// (Late for it, a long way off: at it flat out, and fielded on
			// the run, his hands down for it as he comes, on through it - not
			// stopped dead on it from a sprint.)
			const far = dist(m.at, at);
			if (
				far > ONSIDE_STRIDE_FT &&
				t > m.t &&
				(far / (t - m.t)) * 1000 >= ONSIDE_STRIDE_FPS
			) {
				const way = this.toward(m.at, at);
				const was = m.saved();
				m.press(
					{
						x: at.x - Math.cos(way) * ONSIDE_STRIDE_FT,
						y: at.y - Math.sin(way) * ONSIDE_STRIDE_FT,
					},
					t,
					m.top * 0.9,
				);
				if (
					Math.abs(m.t - t) < 5 &&
					GAITS.has(m.last.move) &&
					legPace(m.last) >= STOP_LEAST
				) {
					this.reaches.push({
						m,
						move: "catch",
						t0: t - CATCH_REACH_MS,
						t1: t + CATCH_IN_MS,
					});
					return;
				}
				was();
			}
			m.press(to, t - 250, m.top * 0.9);
			m.hold(t - CATCH_REACH_MS, "stand", yaw);
			m.catchOn(t, yaw, at);
		};
		let takeT: number;
		if (caught) {
			// Caught coming down, the hands team's man facing the kick, the
			// kicking team's going with it.
			const near = this.w({ d: at.d + (ahead ? -1.5 : 1.5), a: at.a });
			offFor(rec, near, landT - 250);
			rec.press(near, landT - 250, rec.top * 0.9);
			rec.hold(landT - CATCH_REACH_MS, "stand", face);
			rec.catchOn(landT, face, restPt);
			takeT = landT;
		} else if (returned) {
			// Fielded on a hop, on the run.
			takeT = touchT;
			this.hops(down, { ...restPt, z: 3.2 }, landT, takeT);
			handsOn(rec, restPt, takeT);
		} else {
			let loose = touchT;
			if (muff) {
				// Off his hands and away.
				this.hops(down, { ...touch, z: 2.6 }, landT, touchT);
				handsOn(muff, touch, touchT);
				loose = clamp(reachRest - 300, touchT + 500, touchT + 1400);
				this.hops({ ...touch, z: 2.6 }, { ...restPt, z: 0 }, touchT, loose);
			} else {
				this.hops(down, { ...restPt, z: 0 }, landT, touchT);
			}
			// Where it stops, and fallen on.
			this.ball.push({ kind: "spot", t0: loose, at: { ...restPt, z: 0 } });
			takeT = Math.max(loose + 100, reachRest + 350);
			rec.hold(goOf(rec));
			const yaw = this.toward(rec.at, restPt);
			const lands = fallOn(restPt, yaw);
			rec.press(
				{
					x: lands.x - Math.cos(yaw) * FALL_ON_FT,
					y: lands.y - Math.sin(yaw) * FALL_ON_FT,
				},
				takeT - 350,
				rec.top * FLAT_OUT,
				"sprint",
			);
			rec.lunge("dive", takeT, yaw, lands);
			rec.hold(takeT + 1400, "prone");
			rec.act("getUp", takeT + 1400 + GET_UP_MS);
			if (muff) {
				// And after it again, too late.
				const yaw = this.toward(muff.at, restPt);
				const by = {
					x: restPt.x - Math.cos(yaw) * 1.6,
					y: restPt.y - Math.sin(yaw) * 1.6,
				};
				muff.pressUp(by, takeT - 200, muff.top * 0.8);
				muff.lunge("dive", takeT + 250, yaw, by);
				muff.hold(takeT + 1500, "prone");
				muff.act("getUp", takeT + 1500 + GET_UP_MS);
				this.done.add(muff.pid);
			}
		}
		this.ball.push({ kind: "held", t0: takeT, pid: rec.pid });
		this.done.add(rec.pid);
		const others = this.men.filter((m) => m !== rec && m !== k);

		if (returned) {
			// Back up the field with it: the kicking team's nearest at him,
			// the hands team's on them.
			const endD = clamp(
				td
					? this.ownGoalD - 4 - r() * 12
					: spot !== undefined
						? this.goalD - spot * YD
						: recD - back * YD,
				this.ownGoalD - 27,
				this.goalD + 27,
			);
			let t: number;
			if (td) {
				this.weave = true;
				t = this.returnRun(rec, endD, takeT, at.a);
			} else {
				const end = this.w({
					d: endD,
					a: clamp(
						at.a + (r() - 0.5) * 12,
						this.bounds.left + 6,
						this.bounds.right - 6,
					),
				});
				t = dist(rec.at, end) > 2 ? rec.run(end, rec.top * 0.8) : rec.t;
			}
			const endPt = rec.at;
			const locks = this.lockOn(others, kickT, endPt, t);
			this.endReturn(i, rec, endPt, t, td, this.o);
			this.release(locks, endPt);
			for (const m of this.d) {
				if (!this.done.has(m.pid) && m.t < this.whistle - 300) {
					m.hold(goOf(m));
					this.trail(m, endPt);
				}
			}
			this.kickerAfter(k, at, this.whistle);
			return;
		}

		// Everybody near piling in on him, one after another: the first few
		// off their feet into it, the rest at each other round it.
		const hitT = caught ? takeT + 450 : takeT;
		if (caught) {
			rec.hold(hitT);
			const by = this.nearest(
				others.filter((m) => m.team !== rec.team),
				restPt,
				2,
			);
			this.tackle(rec, by, restPt, hitT);
		}
		this.whistle = hitT + 500;
		this.at.set(i, this.whistle);
		const pile = this.nearest(others, restPt, 7);
		for (const m of pile) {
			m.hold(goOf(m));
		}
		this.pileOn(pile, restPt, hitT);
		const rest = others.filter((m) => !this.done.has(m.pid));
		const locks = this.lockOn(rest, kickT, restPt, this.whistle);
		this.release(locks, restPt);
		for (const m of rest) {
			if (this.done.has(m.pid) || locks.some((x) => x.b === m || x.c === m)) {
				continue;
			}
			// After it from wherever he is, pulling up as it is had.
			const s = this.of(m.at);
			m.hold(goOf(m));
			const ang =
				Math.atan2(m.at.y - restPt.y, m.at.x - restPt.x) + (r() - 0.5) * 0.8;
			const ring = 8 + r() * 12;
			m.chase(
				m.team === offense
					? {
							x: restPt.x + Math.cos(ang) * ring,
							y: restPt.y + Math.sin(ang) * ring,
						}
					: this.w({
							d: (s.d + recD) / 2,
							a: s.a * 0.7 + at.a * 0.3,
						}),
				this.whistle + 300,
				m.top * (m.team === offense ? 0.9 : 0.6),
			);
		}
		this.kickerAfter(k, at, this.whistle);
	}

	// Everybody near piling in on the ball at `pt`, one after another from
	// `hitT`: the first few there off their feet into it, the rest at each
	// other round it - each from where what he has been given so far leaves
	// him.
	pileOn(pile: Actor[], pt: Pt, hitT: number) {
		const r = this.r;
		// (Each of those off his feet into it down round it from the others
		// in on it before him, and the man down on it already - not on top of
		// one, in the one spot.)
		const landed: Pt[] = this.men
			.filter(
				(o) =>
					!pile.includes(o) &&
					DOWN.has(legAt(o, hitT + 600).move) &&
					dist(o.posAt(hitT + 600), pt) < PILE_ON_NEAR,
			)
			.map((o) => o.posAt(hitT + 600));
		const lands = (a: number) => ({
			x: pt.x + Math.cos(a) * 1.2,
			y: pt.y + Math.sin(a) * 1.2,
		});
		// (Those off their feet into it, the way each lies: his legs out
		// from where he lands the way he came in from - and the rest, on
		// their feet round it, stood off the length of them, not on them.)
		const lain: number[] = [];
		const offLain = (q: Pt) =>
			Math.min(
				Infinity,
				...lain.map((a) => {
					const at = lands(a);
					const ux = Math.cos(a);
					const uy = Math.sin(a);
					const along = clamp(
						(q.x - at.x) * ux + (q.y - at.y) * uy,
						-LIE_HEAD,
						DOWN_LEGS,
					);
					return Math.hypot(q.x - at.x - ux * along, q.y - at.y - uy * along);
				}),
			);
		pile.forEach((m, n) => {
			const t1 = hitT + 150 + n * 170 + r() * 200;
			const came = Math.atan2(m.at.y - pt.y, m.at.x - pt.x) + (r() - 0.5) * 0.9;
			const room = (a: number) =>
				Math.min(Infinity, ...landed.map((p) => dist(p, lands(a))));
			// (No further out than he is: not stepping off it to come back.)
			const ring = Math.max(
				HIT_LEAST_FT,
				Math.min(2.4 + n * 0.5 + r() * 1.5, dist(m.at, pt)),
			);
			const spot = (a: number) => ({
				x: pt.x + Math.cos(a) * ring,
				y: pt.y + Math.sin(a) * ring,
			});
			const ang =
				n < 3
					? (PILE_ON_ROUND.map((d) => came + d).find(
							(a) => room(a) >= PILE_ON_APART,
						) ??
						PILE_ON_ROUND.map((d) => came + d).reduce((x, y) =>
							room(y) > room(x) ? y : x,
						))
					: (PILE_ON_ROUND.map((d) => came + d).find(
							(a) => offLain(spot(a)) >= PILE_ON_APART,
						) ??
						PILE_ON_ROUND.map((d) => came + d).reduce((x, y) =>
							offLain(spot(y)) > offLain(spot(x)) ? y : x,
						));
			const hit = {
				x: pt.x + Math.cos(ang) * ring,
				y: pt.y + Math.sin(ang) * ring,
			};
			const inward = this.toward(hit, pt);
			// (Round to it as he gets there, if he is all but there - or, with
			// the time to walk it, set near it and in at it off that: see
			// closeOn.)
			const lead = t1 - 400;
			if (
				n < 3 &&
				dist(m.at, hit) > TURN_IN_FT &&
				dist(m.at, hit) / Math.max(0.001, (lead - m.t) / 1000) < WALK_MOST_FPS
			) {
				this.closeOn(m, hit, lead, inward);
			} else if (n < 3 && dist(m.at, hit) > TURN_IN_FT) {
				m.chase(
					hit,
					lead,
					m.top * FLAT_OUT,
					"sprint",
					dist(m.at, hit) < 3 ? inward : undefined,
				);
			} else if (n < 3) {
				m.hold(lead, undefined, inward);
			}
			if (n < 3 && dist(m.at, hit) < 3) {
				landed.push(lands(ang));
				lain.push(ang);
				m.lunge("dive", t1, inward, lands(ang));
				m.hold(t1 + 1100 + n * 150, "prone");
				m.act("getUp", t1 + 1100 + n * 150 + GET_UP_MS);
			} else {
				// (Time to walk it: in at a run all the same, there sooner, at
				// the others round it.)
				const far = dist(m.at, hit);
				if (
					far > 3 &&
					far / Math.max(0.001, (t1 - m.t) / 1000) < WALK_MOST_FPS
				) {
					m.runUp(hit, PILE_RUN_FPS);
				} else {
					m.chase(hit, t1, m.top * 0.92);
					m.easeIn(STOOD_IN_FT, STOOD_IN_LEAST);
				}
				const held = this.whistle + 700 + r() * 400;
				// (Not in on it a moment only, there so late.)
				if (held - m.t >= WAIT_LEAST_MS) {
					m.hold(held, "engage", inward);
				}
			}
			this.done.add(m.pid);
		});
	}

	// The hands team's men left each on a kicking team man coming at them,
	// from the kick: tied up near the ball until `until`.
	lockOn(men: Actor[], go: number, pt: Pt, until: number): Lock[] {
		const { offense } = this.input;
		const free = men.filter((m) => !this.done.has(m.pid));
		return this.pairUp(
			free.filter((m) => m.team !== offense && m.role === "FRONT"),
			free.filter((m) => m.team === offense),
		)
			.filter(([b, c]) => dist(b.at, c.at) < 55)
			.map(([b, c]) => this.lockUp(b, c, go, 6, pt, until));
	}

	// When a man coming back up the field with the ball is LOCK_PAST_FT by
	// `at`, between t0 and t1 (t1, if he never is).
	goneBy(m: Actor, at: Pt, t0: number, t1: number): number {
		const d = this.of(at).d - LOCK_PAST_FT;
		for (let c = t0; c < t1; c += 50) {
			if (this.of(m.posAt(c)).d < d) {
				return c;
			}
		}
		return t1;
	}

	// The kicker, his kick away, on after it at a jog.
	kickerAfter(k: Actor, at: OPt, until: number) {
		if (this.done.has(k.pid)) {
			return;
		}
		// (At a jog at most, from wherever he has got to - and a jog at
		// least, not walked out there while it is still going on; then along
		// with it, kept off it.)
		const to = this.w({ d: at.d * 0.45 + this.r() * 8, a: at.a * 0.4 });
		const far = dist(k.at, to);
		k.go(
			to,
			Math.max(
				k.t + 600,
				k.t + (far / KICKER_JOG) * 1000,
				Math.min(until, k.t + (far / KICKER_JOG_LEAST) * 1000),
			),
		);
		if (k.t < until - 300) {
			this.alongWith(k, KICKER_JOG_LEAST, TRAIL_KEEP_FT, until - 300);
		}
	}

	// A ball off the turf from `from` at t0 to `to` at t1, in hops - each a
	// little shorter than the last, as high as that long in the air goes.
	hops(from: Pt3, to: Pt3, t0: number, t1: number) {
		const n = Math.max(1, Math.ceil((t1 - t0) / 550));
		const w = Array.from({ length: n }, (_, k) => 0.75 ** k);
		const sum = w.reduce((a, b) => a + b, 0);
		let t = t0;
		let p = from;
		let u = 0;
		for (const [k, x] of w.entries()) {
			const ms = ((t1 - t0) * x) / sum;
			u += x / sum;
			const q: Pt3 =
				k === n - 1
					? to
					: {
							x: from.x + (to.x - from.x) * u,
							y: from.y + (to.y - from.y) * u,
							z: 0,
						};
			this.ball.push({
				kind: "fly",
				t0: t,
				t1: t + ms,
				from: p,
				to: q,
				peak: peakFor(ms),
			});
			t += ms;
			p = q;
		}
	}

	// ---- a punt ----------------------------------------------------------------------

	punt(i: number) {
		const { snap, ballY, dir } = this.input;
		const r = this.r;
		const e = this.ev(i);
		const yds = num(e.yds, 42);
		const touchback = e.touchback === true;
		const p = this.role("P") ?? this.o[0]!;
		const pr =
			this.man(this.statPids("pr")[0]) ?? this.role("PR", this.defense);
		const retI = this.find("puntReturn", i);
		const ret = retI >= 0 ? this.ev(retI) : undefined;

		// The snap back to him, his two steps, the ball dropped onto his foot.
		const p0 = this.of(p.at);
		const has = this.longSnap(p, { d: p0.d + 1, a: p0.a }, 3.4);
		const step = has + 380;
		p.hold(step);
		p.act("punt", step + PUNT_MS, this.ahead, this.w({ d: p0.d + 6, a: p0.a }));
		const dropT = step + PUNT_MS * PUNT_DROP;
		const kickT = step + PUNT_MS * PUNT_CONTACT;
		this.at.set(i, kickT);
		this.follow = kickT;
		const foot = (t: number, ahead: number, z: number): Pt3 => {
			const at = this.of(p.posAt(t));
			return { ...this.w({ d: at.d + ahead, a: at.a - 0.6 }), z };
		};
		const off = foot(kickT, 2.3, 1.7);
		this.ball.push({
			kind: "fly",
			t0: dropT,
			t1: kickT,
			from: foot(dropT, 1.7, 3.2),
			to: off,
			peak: 0,
		});

		// Where it comes down: near the returner, or - a touchback - in the
		// end zone or bounding into it. Across the field: round where he
		// stands, or - now and then, on the punter's own count (see countOf)
		// - out toward the numbers on one side or the other, away from the
		// room in the middle.
		// (A short one now and then off the side of his foot - a shank:
		// low, end over end, and off toward a sideline. One kept short of
		// the end zone from near midfield, as often as not a pooch: up, end
		// over end, to come down short of the goal line.)
		const near = r() - 0.5;
		const kept = yds * YD > this.goalD - POOCH_IN * YD;
		const sure = !touchback && yds < SHANK_SURE_YDS;
		const pooch =
			!touchback &&
			!sure &&
			kept &&
			yds <= POOCH_YDS &&
			countOf(p, this.input.key, 89) < POOCH;
		const shank =
			sure ||
			(!touchback &&
				!kept &&
				yds < SHANK_YDS &&
				countOf(p, this.input.key, 88) < SHANK);
		const midA = (MID_Y - ballY) * dir;
		const out = shank || countOf(p, this.input.key, 90) < AIM_OUT;
		const outA =
			midA +
			(countOf(p, this.input.key, 91) < 0.5 ? -1 : 1) *
				(AIM_OUT_FT[0] +
					(AIM_OUT_FT[1] - AIM_OUT_FT[0]) * countOf(p, this.input.key, 92));
		const land: OPt = {
			d: touchback
				? clamp(yds * YD, this.goalD - 12, this.goalD + 22)
				: Math.min(yds * YD, this.goalD - 1),
			a: clamp(
				out ? outA : (pr ? this.of(pr.at).a : 0) + near * 36,
				this.bounds.left + 12,
				this.bounds.right - 12,
			),
		};
		const landPt = this.w(land);
		const hang0 = clamp(2800 + yds * 30 + r() * 400, 3300, 4900);
		const hang = shank
			? SHANK_HANG[0] +
				(SHANK_HANG[1] - SHANK_HANG[0]) * countOf(p, this.input.key, 93)
			: pooch
				? Math.max(
						hang0,
						POOCH_HANG[0] +
							(POOCH_HANG[1] - POOCH_HANG[0]) * countOf(p, this.input.key, 93),
					)
				: hang0;
		const spiral = !shank && !pooch;
		const landT = kickT + hang;
		this.seen = kickT + 120;
		p.go(this.w({ d: p0.d + 30, a: land.a * 0.2 }), landT + 600, "jog");

		// The protection, until the ball is away: the line and the shield in
		// their sets, the middle of the rush into them.
		const line = this.o.filter((m) => m !== p && m.role !== "GUNNER");
		// (Each in his set until the kick is seen, and on his own count after
		// it - see countOf.)
		const free = this.seen;
		const freeOf = (m: Actor) =>
			free + KICK_WALL_SPREAD_MS * countOf(m, this.input.key, 6);
		for (const m of line) {
			const s = this.of(m.at);
			if (m.role !== "LS") {
				this.offSnap(m, GET_OFF_MS, GET_OFF_SPREAD_MS);
			}
			m.go(
				this.w({ d: s.d + (m.role === "UP" ? 1 : -1.5), a: s.a }),
				freeOf(m),
				"passSet",
				this.ahead,
			);
		}
		const rush = this.d.filter((m) => m.role === "RUSH");
		const inside = rush.filter((m) => Math.abs(this.of(m.at).a) < 10);
		const up = line.filter((m) => m.role !== "UP");
		for (const m of inside) {
			const b = this.acrossFrom(up, this.of(m.at).a);
			this.offSnap(m, READ_OFF_MS, READ_OFF_SPREAD_MS);
			if (!b) {
				continue;
			}
			const at = (time: number) => {
				const q = this.of(b.posAt(time));
				return this.w({ d: q.d + 2.6, a: q.a });
			};
			const till = freeOf(b);
			this.fireInto(m, at, snap + 450, till);
			m.go(at(till), till, "engage", this.behind);
		}

		// The gunners off the line through the men on them and flat out for
		// where it is coming down, the jammers running with them.
		const gunners = this.o.filter((m) => m.role === "GUNNER");
		const jammers = this.d.filter((m) => m.role === "JAMMER");
		const goalPull = this.goalD - 4;
		// (On a return that goes somewhere, a gunner held up at the line by
		// the man on him a while longer - on his own count - and down there
		// late, still coming as it comes down: the returner's room.)
		const retYds = num(ret?.yds, 0);
		const lateG = new Set<Actor>();
		for (const g of gunners) {
			const s = this.of(g.at);
			const side = Math.sign(s.a) || 1;
			const j = this.acrossFrom(jammers, s.a);
			const held =
				!touchback && ret && retYds >= JAM_HELD_YDS
					? JAM_HELD_MS[0] +
						(JAM_HELD_MS[1] - JAM_HELD_MS[0]) * countOf(g, this.input.key, 98)
					: 0;
			this.offSnap(g, GET_OFF_MS, GET_OFF_SPREAD_MS);
			g.goOff(
				this.w({ d: s.d + JAM_FIGHT_FT, a: s.a - side * 0.5 }),
				snap + JAM_FIGHT_MS,
				"engage",
				this.ahead,
			);
			if (held > 0) {
				g.go(
					this.w({ d: s.d + 9 + JAM_HELD_FT, a: s.a - side * 2.5 }),
					snap + 900 + held,
					"engage",
					this.ahead,
				);
			}
			g.run(
				this.w({
					d: Math.min(land.d - 10 - r() * 6, goalPull),
					a: land.a + side * (6 + r() * 6),
				}),
				g.top * 0.95,
				"sprint",
			);
			const late = held > 0 && g.t > landT;
			if (late) {
				lateG.add(g);
				// (Still on his way as he has it: after him from there - see
				// trail - not on to where he would have broken down.)
				g.cut(landT + 150);
			} else {
				// (Broken down there, slowing into it over his last strides.)
				g.hold(g.t + GUNNER_SET_MS, "backerReady", this.toward(g.at, landPt));
			}
			if (j) {
				jammers.splice(jammers.indexOf(j), 1);
				this.offSnap(j, READ_OFF_MS, READ_OFF_SPREAD_MS);
				// (Squaring up on him as he comes: over toward in front of him,
				// not all the way across with him in a blink.)
				j.goOff(
					this.w({
						d: s.d + JAM_FIGHT_FT + 2.4,
						a: (this.of(j.at).a + s.a - side * 0.5) / 2,
					}),
					snap + JAM_FIGHT_MS,
					"engage",
					this.behind,
				);
				if (held > 0) {
					j.go(
						this.w({
							d: s.d + 11.6 + JAM_HELD_FT,
							a: s.a - side * 2.5,
						}),
						snap + 900 + held,
						"engage",
						this.behind,
					);
				}
				// A step behind him and inside, all the way down - and squared
				// up to him there, waiting on it with him (not stood).
				// (Round to the inside of him first, and only then dropped in
				// behind him, over JAM_DROP_MS - not through him from in front
				// of him to behind, nor stood still while he goes.)
				const jam = this.of(j.at);
				const was = this.of(g.posAt(j.t));
				const ahead = jam.d - was.d;
				const beside = jam.a - was.a;
				const jamT = j.t;
				for (let time = jamT + 400; time < g.t; time += 400) {
					const q = this.of(g.posAt(time));
					const u = Math.min(1, (time - jamT) / JAM_DROP_MS);
					const w = Math.min(1, (time - jamT) / JAM_IN_MS);
					j.go(
						this.w({
							d: q.d + ahead + (-JAM_BEHIND_FT - ahead) * u,
							a: q.a + beside * (1 - w) - side * JAM_INSIDE_FT * w,
						}),
						time,
					);
				}
				if (!late) {
					j.hold(
						Math.max(j.t + JAM_SET_MS, g.t),
						"backerReady",
						this.toward(j.at, g.at),
					);
				}
			}
		}

		// The outside of the rush and the two held up behind it peeling back,
		// to set up a wall on the side the return will go.
		const ta = pr ? this.tacklerA(pr, landT) : undefined;
		const wallSide =
			ta !== undefined ? Math.sign(ta - land.a) || 1 : r() < 0.5 ? -1 : 1;
		const peel = [
			...rush.filter((m) => !inside.includes(m)),
			...this.d.filter((m) => m.role === "WALL"),
		];
		peel.forEach((m, k) => {
			m.hold(snap + 200 + r() * 150);
			const to = this.w({
				d: Math.min(land.d - 26 - k * 7 - r() * 8, goalPull - 20),
				a: land.a + wallSide * (8 + k * 7 + r() * 5),
			});
			// (Turned round first, over a step or two, and then flat out.)
			const far = dist(m.at, to);
			const u = far > 0 ? Math.min(0.2, PEEL_TURN_FT / far) : 0;
			m.go(
				{ x: m.at.x + (to.x - m.at.x) * u, y: m.at.y + (to.y - m.at.y) * u },
				m.t + PEEL_TURN_MS,
				"jog",
			);
			// (Pulled up as he gets there, over a few strides - see
			// STOP_ACCEL - not on at full tilt till he is on it.)
			const fps = m.top * 0.85;
			const ease = Math.min(EASE_OFF_FT, (fps * fps) / (2 * STOP_ACCEL));
			const left = dist(m.at, to);
			if (left > ease * 2) {
				const u = (left - ease) / left;
				m.run(
					{ x: m.at.x + (to.x - m.at.x) * u, y: m.at.y + (to.y - m.at.y) * u },
					fps,
					"sprint",
				);
				m.easeOff(ease);
			} else {
				m.run(to, fps, "sprint");
			}
		});

		// (No return coming - a fair catch, or into the end zone: the wall
		// set up for it, the jammers down with the gunners, the gunners
		// broken down in front of him, watching it come down, set - not
		// stood there like statues till the whistle.)
		const setFor = () => {
			const jams = this.d.filter((m) => m.role === "JAMMER");
			for (const m of [...peel, ...jams, ...gunners, ...lanes]) {
				if (!this.done.has(m.pid) && this.whistle - m.t >= FAIR_SET_LEAST_MS) {
					m.hold(
						this.whistle,
						m.team === this.o[0]!.team ? "backerReady" : "safetyReady",
						this.toward(m.at, landPt),
					);
				}
			}
		};

		// The rest of the cover in their lanes, spread across the field.
		const lanes = [...line].sort((x, y) => this.of(x.at).a - this.of(y.at).a);
		// (Down there by just after the whistle - or, if that is so near it
		// is no more than a walk, at COVER_LEAST_FPS: down the field after
		// it, and pulled up there.)
		const coverBy = (m: Actor, to: Pt) =>
			Math.min(
				this.whistle + 900,
				Math.max(m.t, free) + (dist(m.at, to) / COVER_LEAST_FPS) * 1000,
			);
		// (Funneling in on him, the men outside staying wider and a little
		// deeper to keep him in.)
		const laneEnd = (k: number): OPt => {
			const off = k - (lanes.length - 1) / 2;
			return {
				d: Math.min(land.d - 22 - r() * 12 + Math.abs(off) * 3, goalPull),
				a: land.a + off * (7 + r() * 3),
			};
		};

		if (touchback || !pr) {
			// Into the end zone, or bounding into it: the returner away from
			// it, the cover pulling up at the goal line.
			const bounce: Pt3 = {
				...this.w({
					d: Math.min(land.d + 12 + r() * 8, this.goalD + END_ZONE + 4),
					a: land.a + (r() - 0.5) * 8,
				}),
				z: 0,
			};
			this.ball.push(
				{
					kind: "fly",
					t0: kickT,
					t1: landT,
					from: off,
					to: { ...landPt, z: 0 },
					peak: peakFor(hang),
					spiral,
				},
				{
					kind: "fly",
					t0: landT,
					t1: landT + 700,
					from: { ...landPt, z: 0 },
					to: bounce,
					peak: 3.5,
				},
				{ kind: "spot", t0: landT + 700, at: bounce },
			);
			this.whistle = landT + 900;
			if (pr) {
				pr.hold(kickT + 300);
				pr.chase(
					this.w({ d: this.goalD - 9, a: (this.of(pr.at).a + land.a) / 2 }),
					landT - 400,
					16,
				);
				pr.hold(landT + 900, "stand", this.toward(pr.at, landPt));
				this.done.add(pr.pid);
			}
			// (Easing up short of the goal line once it is going in, scattered
			// - not all of them stood along it.)
			lanes.forEach((m, k) => {
				const end = laneEnd(k);
				const off = Math.abs(k - (lanes.length - 1) / 2);
				const to = this.w({
					d: Math.min(end.d, goalPull - TB_SHORT - r() * TB_SPREAD - off * 1.5),
					a: end.a,
				});
				m.chase(to, coverBy(m, to), m.top * 0.85);
			});
			this.downfield(inside);
			setFor();
			for (const m of this.d) {
				if (!this.done.has(m.pid)) {
					m.hold(Math.max(m.t, free) + 300);
					m.hold(this.whistle + 600, "stand");
				}
			}
			return;
		}

		const tacklers = this.statPids("defTckSolo", "defTckAst");
		const noReturn = !ret || (retYds === 0 && tacklers.length === 0);
		if (noReturn && countOf(pr, this.input.key, 86) < LET_GO) {
			// Let go: down short of him and bounding on to where it stops,
			// him off to the side of it waving his men away from it - the
			// first of the cover there over it, bent to it, downed.
			const short =
				LET_SHORT[0] +
				(LET_SHORT[1] - LET_SHORT[0]) * countOf(pr, this.input.key, 87);
			const first = this.w({
				d: Math.max(land.d - short, 10),
				a: land.a + (r() - 0.5) * 6,
			});
			const hop1 = {
				x: first.x + (landPt.x - first.x) * 0.55 + (r() - 0.5) * 3,
				y: first.y + (landPt.y - first.y) * 0.55 + (r() - 0.5) * 3,
				z: 0,
			};
			const hop2 = {
				x: first.x + (landPt.x - first.x) * 0.85,
				y: first.y + (landPt.y - first.y) * 0.85,
				z: 0,
			};
			const t1 = landT + LET_HOPS_MS[0];
			const t2 = t1 + LET_HOPS_MS[1];
			const stopT = t2 + LET_HOPS_MS[2];
			this.ball.push(
				{
					kind: "fly",
					t0: kickT,
					t1: landT,
					from: off,
					to: { ...first, z: 0 },
					peak: peakFor(hang),
					spiral,
				},
				{
					kind: "fly",
					t0: landT,
					t1,
					from: { ...first, z: 0 },
					to: hop1,
					peak: 3.2,
				},
				{ kind: "fly", t0: t1, t1: t2, from: hop1, to: hop2, peak: 1 },
				{
					kind: "fly",
					t0: t2,
					t1: stopT,
					from: hop2,
					to: { ...landPt, z: 0 },
					peak: 0,
				},
				{ kind: "spot", t0: stopT, at: { ...landPt, z: 0 } },
			);
			// (Him off to the side of where it comes down, the side he is
			// on, waving them off it, and watching it go.)
			const side = Math.sign(this.of(pr.at).a - land.a) || 1;
			pr.hold(kickT + 300);
			pr.chase(
				this.w({ d: land.d + 4, a: land.a + side * LET_ASIDE }),
				landT - 600,
				14,
			);
			pr.hold(stopT + 1200, "stand", this.toward(pr.at, first));
			this.reaches.push({
				m: pr,
				move: "waveOver",
				t0: landT - LET_WAVE_FROM,
				t1: landT - LET_WAVE_TO,
			});
			this.done.add(pr.pid);
			// (The first of the cover to it: on over it and bent to it.)
			const by = [...gunners, ...lanes]
				.filter((m) => !this.done.has(m.pid))
				.sort(
					(x, y) => dist(x.posAt(stopT), landPt) - dist(y.posAt(stopT), landPt),
				)[0];
			let downT = stopT + 400;
			if (by) {
				const over = this.w({ d: land.d - 1.6, a: land.a });
				by.chase(over, Math.max(by.t, stopT) + 200, by.top * 0.85);
				// (Pulled up into it as he gets there - there a moment later for
				// it, if he must be - not stopped dead over it off a sprint.)
				by.breakDown(0, STOP_ACCEL * STOP_HARD, LET_DOWN_SPARE_MS);
				by.act("spotBall", by.t + LET_DOWN_MS, this.toward(by.at, landPt));
				downT = by.t - LET_DOWN_MS / 2;
				this.done.add(by.pid);
			}
			this.whistle = Math.max(stopT + 200, downT);
			if (retI >= 0) {
				this.at.set(retI, this.whistle);
			}
			lanes.forEach((m, k) => {
				if (!this.done.has(m.pid)) {
					const to = this.w(laneEnd(k));
					this.chaseClear(m, to, coverBy(m, to), m.top * 0.85);
				}
			});
			this.downfield(inside);
			setFor();
			return;
		}

		// The catch: set under it, or - on his own count - back under one
		// going over where he lined up, caught still going back; or up into
		// one short of him (or, with room, one he could have stood under)
		// at a run, caught in his stride.
		const delta = land.d - this.of(pr.at).d;
		const how: "set" | "up" | "back" = noReturn
			? "set"
			: delta > FIELD_BACK_LEAST
				? countOf(pr, this.input.key, 94) < FIELD_BACK
					? "back"
					: "set"
				: countOf(pr, this.input.key, 94) <
					  (delta < -FIELD_UP_LEAST ? FIELD_UP : FIELD_UP_ANY)
					? "up"
					: "set";
		const room = gunners.every(
			(g) => dist(g.posAt(landT), landPt) >= FIELD_UP_ROOM,
		);
		const hands = this.getUnder(
			pr,
			land,
			kickT + 200,
			landT,
			how === "up" && !room ? "set" : how,
			!noReturn,
		);
		this.ball.push({
			kind: "fly",
			t0: kickT,
			t1: landT,
			from: off,
			to: { ...hands, z: hands.z + 0.6 },
			peak: peakFor(hang),
			spiral,
		});
		// (Now and then juggled - off his hands and up, and his hands on it
		// again a beat later - the surer his hands, the less often.)
		let gotT = landT;
		const set = pr.last.move === "catch" || pr.last.move === "basket";
		if (
			!noReturn &&
			set &&
			countOf(pr, this.input.key, 95) <
				JUGGLE * (1 - JUGGLE_HANDS * (pr.flair.hands ?? 0))
		) {
			gotT = landT + JUGGLE_MS;
			const at = this.of(pr.posAt(landT));
			pr.catchOn(gotT, this.behind, this.w({ d: at.d - JUGGLE_ON, a: at.a }));
			const again = this.of(pr.posAt(gotT));
			this.ball.push({
				kind: "fly",
				t0: landT,
				t1: gotT,
				from: { ...hands, z: hands.z + 0.6 },
				to: { ...this.w({ d: again.d - 1, a: again.a }), z: CATCH_Z + 0.6 },
				peak: JUGGLE_UP,
			});
		}
		this.ball.push({ kind: "held", t0: gotT, pid: pr.pid });
		if (noReturn) {
			// A fair catch: his arm up and waved as it comes, and nobody
			// touches him.
			this.reaches.push({
				m: pr,
				move: "fairCatch",
				t0: landT - FAIR_FROM,
				t1: landT - FAIR_TO,
			});
			this.whistle = landT + 400;
			if (retI >= 0) {
				this.at.set(retI, this.whistle);
			}
			this.done.add(pr.pid);
			lanes.forEach((m, k) => {
				const to = this.w(laneEnd(k));
				this.chaseClear(m, to, coverBy(m, to), m.top * 0.85);
			});
			// (The rush held up at the line off down the field after it at a
			// jog - not stood where it was kicked from.)
			this.downfield(inside);
			setFor();
			return;
		}

		// The return, the wall each on a cover man as he comes - the rest of
		// the cover in their lanes.
		const td = ret.td === true;
		const endD = clamp(
			td ? this.ownGoalD - 4 - r() * 12 : land.d - retYds * YD,
			this.ownGoalD - 27,
			this.goalD + 27,
		);
		const t = this.returnRun(pr, endD, gotT, land.a);
		this.weave = true;
		const endPt = pr.at;
		const pairs = this.pairUp(peel, lanes);
		const locks = pairs.map(([b, c]) =>
			this.lockUp(
				b,
				c,
				free,
				0,
				endPt,
				Math.abs(this.of(b.at).a - this.of(endPt).a) > 40
					? b.t + 1200 + r() * 600
					: t,
				pr,
				b.top * WALL_UP_PACE,
			),
		);
		// (The jammers, down the field with the gunners, on them again as
		// they go in after him - not stood watching it go by.)
		const jams = this.d.filter(
			(m) => m.role === "JAMMER" && !this.done.has(m.pid),
		);
		// (Not a gunner held up and still coming as he has it: past the man
		// on him, after the ball.)
		const loose = gunners.filter(
			(g) =>
				!this.done.has(g.pid) &&
				!lateG.has(g) &&
				!pairs.some(([, c]) => c === g),
		);
		for (const [b, c] of this.pairUp(jams, loose)) {
			if (dist(b.at, c.at) < JAM_AGAIN_FT) {
				locks.push(this.lockUp(b, c, landT, JAM_AGAIN_FPS, endPt, t, pr));
			}
		}
		lanes.forEach((m, k) => {
			if (!pairs.some(([, c]) => c === m)) {
				this.laneRun(
					m,
					this.w(laneEnd(k)),
					m.top * (0.8 + r() * 0.12),
					"sprint",
				);
			}
		});
		this.endReturn(retI, pr, endPt, t, td, this.o);
		this.release(locks, endPt);
		// (The rush held up at the line turned and after it at a jog, and
		// with it as it comes back - not stood at the line through it all.)
		for (const m of inside) {
			if (!this.done.has(m.pid) && m.t < this.whistle - TRAIL_LOOK_MS[1]) {
				m.hold(
					m.t +
						TRAIL_LOOK_MS[0] +
						(TRAIL_LOOK_MS[1] - TRAIL_LOOK_MS[0]) *
							countOf(m, this.input.key, 29),
				);
				this.alongWith(
					m,
					TRAIL_JOG_FPS[0] +
						(TRAIL_JOG_FPS[1] - TRAIL_JOG_FPS[0]) *
							countOf(m, this.input.key, 30),
					TRAIL_KEEP_FT,
					this.whistle - 300,
				);
			}
		}
		for (const m of [...this.d, ...this.o]) {
			if (!this.done.has(m.pid) && m.t < this.whistle - 300) {
				this.trail(m, endPt);
			}
		}
	}

	// ---- a field goal, a try ----------------------------------------------------------

	// Whether it could be acted out (a holder and a kicker on the field).
	fieldGoal(i: number): boolean {
		const { snap, ballY, dir } = this.input;
		const r = this.r;
		const e = this.ev(i);
		const made = e.made === true;
		const yds = num(e.yds, 35);
		const h = this.role("H");
		const k = this.role("K");
		if (!h || !k) {
			return false;
		}

		// The snap back to the holder, the ball down on its spot beside him,
		// and the kick.
		const hs = this.of(h.at);
		const spot: OPt = { d: hs.d + 0.7, a: hs.a - 1.7 };
		const has = this.longSnap(h, { d: spot.d + 0.3, a: spot.a + 0.9 }, 2.4);
		// (Brought down to it in both hands from the catch, and let go onto
		// it as they get there - a finger of his left on top of it.)
		const brings = has + HOLD_CATCH_MS;
		const down = brings + HOLD_PLACE_MS;
		const kickT = snap + 1280 + r() * 100;
		h.hold(brings, "holdKneel");
		h.hold(down, "holdPlace");
		h.hold(kickT + 250, "holdDown");
		this.ball.push({
			kind: "spot",
			t0: down,
			at: { ...this.w(spot), z: 0 },
			stood: "hold",
		});
		k.hold(snap + 300);
		this.placeKick(k, spot, kickT - KICK_MS * KICK_CONTACT);
		this.follow = kickT;

		// Through the posts on the end line (or wide of them, or short) and on
		// into the net behind them.
		const from: Pt3 = { ...this.w(spot), z: 0.5 };
		const lineD = this.goalD + END_ZONE;
		const postA = (MID_Y - ballY) * dir;
		const short = !made && yds >= 45 && r() < 0.35;
		const wide = r() < 0.5 ? -1 : 1;
		// (Or off the post - see DOINK: the outside of it, back off it. On
		// the kicker's own count, as how it comes back off it is - see
		// countOf: no new draws.)
		const doink = !made && !short && countOf(k, this.input.key, 70) < DOINK;
		const crossA =
			postA +
			(made
				? (r() - 0.5) * 11
				: short
					? (r() - 0.5) * 8
					: doink
						? wide * (POST_W / 2 + r() * DOINK_OUT)
						: wide * (POST_W / 2 + 1 + r() * 6));
		const vh = 64 + yds * 0.22 + r() * 5;
		const tc = Math.hypot(lineD - spot.d, crossA - spot.a) / vh;
		const z1 = short
			? 3 + r() * 5
			: CROSSBAR_Z + 4 + Math.max(0, 50 - yds) * 0.5 + r() * 6;
		const vz = (z1 - from.z + (G / 2) * tc * tc) / tc;
		const along = (s: number): Pt3 => {
			const u = s / tc;
			return {
				...this.w({
					d: spot.d + (lineD - spot.d) * u,
					a: spot.a + (crossA - spot.a) * u,
				}),
				z: Math.max(0, from.z + vz * s - (G / 2) * s * s),
			};
		};
		const tLand = (vz + Math.sqrt(vz * vz + 2 * G * from.z)) / G;
		const tNet = (tc * (lineD + NET_BACK - spot.d)) / (lineD - spot.d);
		const tEnd = doink ? tc : Math.min(tLand, tNet);
		let end = along(tEnd);
		const endT = kickT + tEnd * 1000;
		// Bent on its way - each kicker's own way, a hook or a fade, more the
		// longer it is (see KICK_BEND_FT) - and through where it would have
		// gone straight: the end of it moved over to take it there.
		const uc = tc / tEnd;
		const bend =
			uc <= 1
				? (countOf(k, 0, 109) < 0.5 ? -1 : 1) *
					(KICK_BEND_FT[0] +
						(KICK_BEND_FT[1] - KICK_BEND_FT[0]) *
							countOf(k, this.input.key, 110)) *
					Math.min(KICK_BEND_MOST, yds / KICK_BEND_YDS)
				: 0;
		if (bend !== 0) {
			// (Across only: still on to the net, or the post.)
			const dx = end.x - from.x;
			const len = Math.hypot(dx, end.y - from.y) || 1;
			const off = (4 * bend * uc * (1 - uc)) / uc;
			end = { ...end, y: end.y - (dx / len) * off };
		}
		this.ball.push({
			kind: "fly",
			t0: kickT,
			t1: endT,
			from,
			to: end,
			peak: peakFor(tEnd * 1000),
			...(bend !== 0 ? { bend } : {}),
		});
		if (doink) {
			// (Back off the post and out from the middle, falling from where
			// it hit - onto the turf, a hop, and lying there.)
			const fps =
				DOINK_FPS[0] +
				countOf(k, this.input.key, 71) * (DOINK_FPS[1] - DOINK_FPS[0]);
			const turn =
				DOINK_TURN[0] +
				countOf(k, this.input.key, 72) * (DOINK_TURN[1] - DOINK_TURN[0]);
			const vz = (countOf(k, this.input.key, 73) - 0.6) * 10;
			const fall = (vz + Math.sqrt(vz * vz + 2 * G * end.z)) / G;
			const hit = this.of(end);
			const off = (by: number): OPt => ({
				d: hit.d - Math.cos(turn) * fps * by,
				a: hit.a + wide * Math.sin(turn) * fps * by,
			});
			const lands: Pt3 = { ...this.w(off(fall)), z: 0 };
			const lies: Pt3 = { ...this.w(off(fall * (1 + DOINK_HOP))), z: 0 };
			const downT = endT + fall * 1000;
			this.ball.push(
				{
					kind: "fly",
					t0: endT,
					t1: downT,
					from: end,
					to: lands,
					peak: peakFor(fall * 1000),
				},
				{
					kind: "fly",
					t0: downT,
					t1: downT + DOINK_HOP_MS,
					from: lands,
					to: lies,
					peak: peakFor(DOINK_HOP_MS),
				},
				{ kind: "spot", t0: downT + DOINK_HOP_MS, at: lies },
			);
		} else if (tNet < tLand) {
			const fall = Math.sqrt((2 * end.z) / G) * 1000;
			const below: Pt3 = { ...end, z: 0 };
			this.ball.push(
				{
					kind: "fly",
					t0: endT,
					t1: endT + fall,
					from: end,
					to: below,
					peak: 0,
				},
				{ kind: "spot", t0: endT + fall, at: below },
			);
		} else {
			this.ball.push({ kind: "spot", t0: endT, at: end });
		}
		const crossT = kickT + Math.min(tc, tLand) * 1000;
		this.at.set(i, crossT);
		this.whistle = crossT + 300;

		// The wall holding its ground, the rush into it - off the edges and
		// up at the kick as it goes.
		const wall = this.o.filter((m) => m !== h && m !== k);
		// (Each holding it until the kick is away, and on his own count
		// after it - see countOf.)
		const heldOf = (m: Actor) =>
			kickT + 250 + KICK_WALL_SPREAD_MS * countOf(m, this.input.key, 6);
		for (const m of wall) {
			const s = this.of(m.at);
			if (m.role !== "LS") {
				this.offSnap(m, GET_OFF_MS, GET_OFF_SPREAD_MS);
			}
			m.go(
				this.w({ d: s.d - 0.8, a: s.a * 0.96 }),
				heldOf(m),
				"passSet",
				this.ahead,
			);
		}
		const edgers = this.d
			.filter(
				(m) =>
					(m.role === "RUSH" || m.role === "CB") &&
					Math.abs(this.of(m.at).a) > EDGE_A,
			)
			.sort((a, b) => Math.abs(this.of(a.at).a) - Math.abs(this.of(b.at).a));
		for (const m of this.d) {
			const s = this.of(m.at);
			const edge = Math.abs(s.a) > EDGE_A;
			m.hold(snap + 40 + r() * 40);
			if (m.role === "RUSH" && !edge) {
				const b = this.acrossFrom(wall, s.a);
				if (!b) {
					continue;
				}
				const at = (time: number) => {
					const q = this.of(b.posAt(time));
					return this.w({ d: q.d + 2.6, a: q.a + (s.a > q.a ? 0.8 : -0.8) });
				};
				const till = heldOf(b);
				this.fireInto(m, at, snap + 400, till);
				m.go(at(till), till, "engage", this.behind);
			} else if (m.role === "RUSH" || m.role === "CB") {
				// Round the end of the wall and in at the kick: the man outside
				// another wider round him, and in further back.
				const side = Math.sign(s.a) || 1;
				const k = edgers
					.filter((x) => Math.sign(this.of(x.at).a) === side)
					.indexOf(m);
				const end = this.of(
					wall.reduce((a, b) =>
						side * this.of(b.at).a > side * this.of(a.at).a ? b : a,
					).at,
				);
				const round = this.w({
					d: end.d - 0.5,
					a: end.a + side * (EDGE_ROUND + k * EDGE_STAGGER),
				});
				const block = this.w({
					d: spot.d + EDGE_IN + k * EDGE_STAGGER + r() * 2,
					a: spot.a + side * (EDGE_WIDE + k * EDGE_STAGGER + r() * 1.5),
				});
				// (Up to speed off the ball the whole way round, at
				// GET_OFF_ACCEL - not off it flat out from his first step - and
				// no quicker than he goes: there late, if need be, and laid out
				// for it after it has gone. Bent round the corner, not turned
				// square at full tilt.)
				const A = GET_OFF_ACCEL;
				const top = m.top * FLAT_OUT;
				this.getOffAlong(
					m,
					[...this.bend(m.at, round, block, A, top), block],
					kickT - 150,
					A,
					top,
					"sprint",
				);
				// (Gathered for it over his last strides - not leapt from full
				// tilt and stopped dead in the air - and up for it off the
				// last: carried on a little way with it.)
				m.gatherFor(SWAT_IN_FPS, STOP_ACCEL * STOP_HARD);
				const l = m.last;
				const far = dist(l.from, l.to);
				const on = far > 0 ? Math.min(SWAT_ON_MOST, legPace(l) * SWAT_ON) : 0;
				m.act(
					"swat",
					Math.max(kickT + 350, m.t + SWAT_LEAST_MS),
					this.toward(m.at, this.w(spot)),
					{
						x: l.to.x + ((l.to.x - l.from.x) / Math.max(far, 1e-6)) * on,
						y: l.to.y + ((l.to.y - l.from.y) / Math.max(far, 1e-6)) * on,
					},
				);
			} else {
				m.hold(kickT - 250);
				m.act("swat", kickT + 350, this.behind);
			}
		}
		// Everybody turned to watch it go - and as it goes over or by, this
		// often a man of the side it went for glad of it (a clap, a fist), and
		// of the other side, missed, rueing it: hands on his helmet.
		const posts = this.w({ d: lineD, a: postA });
		const fg = this.input.kind === "fieldGoal";
		for (const m of this.men) {
			if (m !== k) {
				m.hold(Math.max(m.t, kickT + 350) + r() * 200);
				const look = this.toward(m.at, posts);
				const ours = m.team === this.input.offense;
				const u = r();
				const glad =
					m !== h &&
					ours === made &&
					u < (made ? (fg ? KICK_GLAD : TRY_GLAD) : MISS_GLAD);
				const rue = !made && ours && m !== h && u < KICK_RUE;
				if (glad || rue) {
					const seen = Math.max(m.t, crossT + KICK_SEEN_MS * (0.4 + r()));
					m.hold(seen, "stand", look);
					if (rue) {
						m.act("handsOnHelmet", seen + RUE_HELMET_MS + r() * 400, look);
					} else {
						m.act(
							r() < 0.6 ? "clap" : "fistPump",
							seen + 1000 + r() * 300,
							look,
						);
					}
				} else {
					m.hold(crossT + 300, "stand", look);
				}
			}
		}
		// (The kicker, wide or short, as a rule - late in a close game,
		// always: his moment, the camera in on it - his own way with it, see
		// KICKER_LOOK: hands on his helmet, or on his hips with his head
		// down, or down on a knee a moment.)
		const blown = !made && fg && this.crunch;
		if (!made && (r() < KICKER_RUE || blown)) {
			const seen = Math.max(k.t, crossT + KICK_SEEN_MS);
			const look = this.toward(k.at, posts);
			k.cut(seen);
			k.hold(seen, "stand", look);
			const u = countOf(k, this.input.key, 130);
			if (u < KICKER_LOOK[0]) {
				k.act("handsOnHelmet", seen + RUE_HELMET_MS + r() * 400);
			} else {
				// (Pulled up first, off his last steps.)
				k.hold(seen + KICKER_STOP_MS, "stand", look);
				const hips = u < KICKER_LOOK[1];
				k.hold(
					k.t + (hips ? KICKER_HIPS_MS : KICKER_KNEE_MS) * (0.8 + 0.4 * u),
					hips ? "onHips" : "takeKnee",
					look,
				);
				k.hold(k.t + KICKER_UP_MS, "stand", look);
			}
			// (The holder, this often, over to him while he stands there with
			// it - not down on a knee - a hand on his shoulder, a pat or two:
			// see holderPat.)
			if (u < KICKER_LOOK[1] && countOf(h, this.input.key, 133) < HOLDER_PAT) {
				const go = seen + HOLDER_PAT_MS[0] + r() * HOLDER_PAT_MS[1];
				this.holderPat(h, k, Math.max(go, h.t), look);
			}
			if (blown) {
				this.party = { from: seen, at: k.posAt(seen), hero: k.pid };
				this.until = Math.max(this.until, k.t);
			}
		}
		// Good: this often the holder over to the kicker, a high five - the
		// kicker's fist pumped first, now and then. A long one, or one late
		// in a close game: always, his fist pumped out to the crowd first -
		// his moment, the camera in on it.
		const big = made && fg && (yds >= KICK_BIG || this.crunch);
		if (
			made &&
			(r() < (this.input.kind === "fieldGoal" ? KICK_FIVE : TRY_FIVE) || big)
		) {
			const go = crossT + r() * 150;
			k.cut(go);
			k.hold(go, "stand", this.toward(k.at, posts));
			if (r() < KICK_PUMP || big) {
				k.act("fistPump", go + 900, big ? playedOut(k) : undefined);
			}
			this.highFive(h, k, Math.max(go + 100, k.t));
			if (big) {
				this.party = { from: go, at: k.posAt(go), hero: k.pid };
				this.until = Math.max(this.until, k.t, h.t);
			}
		}
		return true;
	}

	// ---- after the whistle --------------------------------------------------------

	// A big play's moment after it: a score - the ball spiked, or held up,
	// or a leap, his teammates running to him to leap with him, the other
	// side's men nearest it hands on hips - or a sack, the man who made it
	// flexing over the quarterback.
	celebrate() {
		const r = this.r;
		const { events, i0, i1 } = this.input;
		let tdI = -1;
		for (let i = i0; i < i1; i++) {
			const e = events[i];
			if (e?.td === true && this.at.has(i) && e.type !== "fumble") {
				tdI = i;
			}
		}
		if (tdI < 0) {
			const sacker =
				this.find("sack") >= 0
					? this.man(this.statPids("defSk")[0])
					: undefined;
			if (sacker && r() < (this.crunch ? CRUNCH_PARTY : SACK_PARTY)) {
				// (Up off the quarterback, and straight into it.)
				const up = sacker.legs.findLast((l) => DOWN.has(l.move));
				if (up) {
					sacker.cut(up.t1);
				}
				this.party = { from: sacker.t, at: sacker.at, hero: sacker.pid };
				// (His own way of it, on his own count - see SACK_WAYS.)
				const u = countOf(sacker, this.input.key, 104);
				const way = playedOut(sacker);
				if (u < SACK_WAYS[0]) {
					sacker.act("flex", sacker.t + 1100, way);
				} else if (u < SACK_WAYS[1]) {
					sacker.act("fistPump", sacker.t + 1200, way);
				} else if (u < SACK_WAYS[2]) {
					sacker.act("leap", sacker.t + 800, way);
				} else if (u < SACK_WAYS[3]) {
					sacker.act("griddy", sacker.t + GRIDDY_MS, way);
				} else {
					sacker.hold(sacker.t + 1500, "pointSky", way);
				}
				this.until = sacker.t;
				sacker.hold(sacker.t + 400, "stand");
				this.mob.add(sacker.pid);
				return;
			}
			if (this.input.firstDown) {
				this.pointFirst();
			}
			if (
				this.input.stopped &&
				r() < (this.crunch ? CRUNCH_PARTY : STOP_PARTY)
			) {
				this.stopParty();
			}
			this.takeaway();
			return;
		}
		const tTD = this.at.get(tdI)!;
		const held = this.ball.findLast(
			(b): b is Extract<BallSeg, { kind: "held" }> =>
				b.kind === "held" && b.t0 <= tTD + 1,
		);
		const scorer = this.man(held?.pid);
		if (!scorer || this.ball.at(-1) !== held) {
			return;
		}
		// Slowed up in the end zone: from there - or, down on it, once he is
		// up.
		let c0 = tTD;
		let fell = false;
		for (const l of scorer.legs) {
			if (l.t1 > tTD - 200 && DOWN.has(l.move)) {
				fell = true;
			}
			if (l.t0 >= tTD && (l.from.x !== l.to.x || l.from.y !== l.to.y)) {
				c0 = l.t1;
			}
		}
		if (fell) {
			c0 = scorer.t;
		} else {
			c0 = Math.max(c0, tTD + 300);
			scorer.cut(c0);
		}
		this.scored = { t: tTD, scorer };
		const at = scorer.at;
		const u = r();
		if (u < BOWL && this.bowl(scorer, c0)) {
			return;
		}
		// (Caught it for the score, now and then first a finger back up the
		// field at the man who threw it - as the 3D court's scorer off an
		// assist. His own count: no draw of the play's.)
		const mobFrom = c0;
		const k = held ? this.ball.indexOf(held) : -1;
		const pass = this.ball[k - 1];
		const qb = this.role("QB");
		if (
			pass?.kind === "fly" &&
			pass.spiral &&
			qb &&
			qb !== scorer &&
			scorer.team === this.input.offense &&
			countOf(scorer, this.input.key, 36) < THANKS
		) {
			scorer.act("thanks", c0 + THANKS_MS, this.toward(at, qb.posAt(c0)));
			c0 = scorer.t;
		}
		if (u < 0.32) {
			scorer.act("spike", c0 + SPIKE_MS);
			const out = c0 + SPIKE_MS * SPIKE_AT;
			const ang = scorer.yaw + (r() - 0.5) * 0.6;
			const down = {
				x: at.x + Math.cos(ang) * 2.4,
				y: at.y + Math.sin(ang) * 2.4,
				z: 0.3,
			};
			const off = ang + (r() - 0.5) * 1.4;
			const far = 8 + r() * 6;
			const rest = onField({
				x: down.x + Math.cos(off) * far,
				y: down.y + Math.sin(off) * far,
			});
			this.ball.push(
				{
					kind: "fly",
					t0: out,
					t1: out + 110,
					from: {
						x: at.x + Math.cos(ang) * 1.2,
						y: at.y + Math.sin(ang) * 1.2,
						z: 4.6,
					},
					to: down,
					peak: 0,
				},
				{
					kind: "fly",
					t0: out + 110,
					t1: out + 110 + BOUNCE_MS,
					from: down,
					to: { ...rest, z: 0.3 },
					peak: 3.5,
				},
				{ kind: "spot", t0: out + 110 + BOUNCE_MS, at: { ...rest, z: 0 } },
			);
		} else if (u < 0.46) {
			scorer.hold(c0 + 1400, "ballUp", playedOut(scorer));
		} else if (u < 0.6) {
			scorer.act("leap", c0 + 800, playedOut(scorer));
		} else if (u < 0.78) {
			scorer.act("griddy", c0 + GRIDDY_MS, playedOut(scorer));
		} else if (u < 0.9) {
			scorer.hold(c0 + 1500, "pointSky", playedOut(scorer));
		} else {
			scorer.hold(c0 + 1800, "kneelPoint", playedOut(scorer));
		}
		const went = this.meetUp(scorer, mobFrom);
		this.until = scorer.t;
		scorer.hold(scorer.t + 2600, "stand");
		this.mob.add(scorer.pid);
		this.mobRound(scorer, mobFrom);
		if (went && this.party) {
			// (The picture on the both of where he was and where they meet.)
			const meet = this.party.at;
			this.party.at = {
				x: (went.x + meet.x) / 2,
				y: (went.y + meet.y) / 2,
			};
		}
	}

	// Away on his own, his teammates still a long way back up the field: out
	// to meet the first of them as he comes, after his moment - not stood on
	// his own in the end zone till they get there. (Where he was, if he goes.)
	meetUp(scorer: Actor, c0: number): Pt | undefined {
		const r = this.r;
		const at = scorer.at;
		const start = c0 + 150;
		let first: Actor | undefined;
		let far = Infinity;
		for (const m of this.men) {
			if (
				m === scorer ||
				m.team !== scorer.team ||
				DOWN.has(legAt(m, start).move)
			) {
				continue;
			}
			const d = dist(m.posAt(start), at);
			if (d < far) {
				first = m;
				far = d;
			}
		}
		if (!first || far < MEET_FROM) {
			return undefined;
		}
		// (Where the two of them get to at about the one time: he at a jog
		// once he is off, the other flat out from the start - see mobRound.)
		const off = scorer.t + MEET_AFTER[0] + r() * MEET_AFTER[1];
		const v = first.top * 0.775;
		const lead =
			((start - off) / 1000 + (far - MOB_RING) / v) / (1 / MEET_FPS + 1 / v);
		const go = Math.min(MEET_MOST, lead);
		if (go < MEET_LEAST) {
			return undefined;
		}
		const q = first.posAt(start);
		this.went = at;
		scorer.hold(off);
		scorer.runUp(
			{
				x: at.x + ((q.x - at.x) * go) / far,
				y: at.y + ((q.y - at.y) * go) / far,
			},
			MEET_FPS,
			"jog",
		);
		return at;
	}

	// A touchdown seen from up the field: the quarterback who threw it, and
	// now and then a man of his not in on the mob, both arms straight up as
	// the man gets there - while he is on his feet with his hands free.
	tdArms() {
		const s = this.scored;
		if (!s) {
			return;
		}
		const thrown = this.ball.some((b) => b.kind === "fly" && b.spiral);
		for (const m of this.men) {
			if (m.team !== s.scorer.team || m === s.scorer || this.mob.has(m.pid)) {
				continue;
			}
			const odds = m.role === "QB" && thrown ? TD_ARMS_QB : TD_ARMS;
			if (countOf(m, this.input.key, 13) >= odds) {
				continue;
			}
			const t0 =
				s.t +
				TD_ARMS_REACT[0] +
				countOf(m, this.input.key, 14) * TD_ARMS_REACT[1];
			let t1 = Math.min(
				m.t,
				t0 + TD_ARMS_MS[0] + countOf(m, this.input.key, 15) * TD_ARMS_MS[1],
			);
			for (const l of m.legs) {
				if (l.t1 <= t0 || l.t0 >= t1) {
					continue;
				}
				const kind = moveKind(l.move);
				if (
					l.ball ||
					l.arms ||
					DOWN.has(l.move) ||
					LINE_PLAY.has(l.move) ||
					(kind !== "still" && kind !== "cycle")
				) {
					t1 = Math.min(t1, l.t0);
				}
			}
			if (t1 - t0 >= TD_ARMS_LEAST) {
				this.reaches.push({ m, move: "sigTD", t0, t1 });
			}
		}
	}

	// A score celebrated together, bowling: three of his teammates set up as
	// pins down the end zone from him, the ball rolled along the turf at
	// them - and down they go. (Not enough of them near: none of it.)
	bowl(scorer: Actor, c0: number): boolean {
		const r = this.r;
		const at = scorer.at;
		const start = c0 + 150;
		const pins = this.men
			.filter(
				(m) =>
					m !== scorer &&
					m.team === scorer.team &&
					!DOWN.has(legAt(m, start).move) &&
					dist(m.posAt(start), at) < PIN_FROM,
			)
			.sort((a, b) => dist(a.posAt(start), at) - dist(b.posAt(start), at))
			.slice(0, PIN_SPOTS.length);
		if (pins.length < PIN_SPOTS.length) {
			return false;
		}
		// Across the end zone, toward the middle of the field.
		const way = (at.y < MID_Y ? 1 : -1) * (Math.PI / 2) + (r() - 0.5) * 0.3;
		const ux = Math.cos(way);
		const uy = Math.sin(way);
		const spotOf = (f: number, s: number): Pt =>
			onField({
				x: clamp(at.x + ux * f - uy * s, PIN_IN, FIELD_L - PIN_IN),
				y: at.y + uy * f + ux * s,
			});
		const face = Math.atan2(-uy, -ux);
		// (All of it over well inside the moment after a score - or none of
		// it.)
		// (Pulled up as he gets to his spot, hard - not stopped dead off a
		// sprint.)
		const easeOf = (m: Actor, far: number) =>
			Math.min(far * 0.5, (m.top * m.top) / (2 * STOP_ACCEL * STOP_HARD));
		// (Not the less often for it.)
		let set = c0 + PIN_SET;
		let due = set;
		pins.forEach((m, k) => {
			const p = PIN_SPOTS[k]!;
			const far = dist(m.posAt(start), spotOf(p.f, p.s));
			const ease = easeOf(m, far);
			set = Math.max(set, start + ((far + ease) / m.top) * 1000 + 300);
			due = Math.max(due, start + (far / m.top) * 1000 + 300);
		});
		if (due + BOWL_REST > this.whistle + BOWL_MOST) {
			return false;
		}
		pins.forEach((m, k) => {
			const p = PIN_SPOTS[k]!;
			m.cut(start);
			m.hold(start);
			const to = spotOf(p.f, p.s);
			const far = dist(m.at, to);
			const ease = easeOf(m, far);
			const u = far > 0 ? (far - ease) / far : 1;
			m.run(
				{ x: m.at.x + (to.x - m.at.x) * u, y: m.at.y + (to.y - m.at.y) * u },
				m.top,
				"sprint",
			);
			m.easeOff(ease);
		});
		// The ball up while they set up; then the roll, swung back and let go
		// low, along the turf.
		const b0 = Math.max(c0 + 400, set - PIN_BOWL_AT * BOWL_MS);
		scorer.hold(b0 - 300, "ballUp", way);
		scorer.hold(b0, "stand", way);
		scorer.act("bowl", b0 + BOWL_MS, way);
		const out = b0 + PIN_BOWL_AT * BOWL_MS;
		const from = { x: at.x + ux * 2.2, y: at.y + uy * 2.2, z: 0.4 };
		const head = spotOf(PIN_SPOTS[0]!.f - 1, 0);
		const hit = out + (dist(from, head) / ROLL_FPS) * 1000;
		const rest = spotOf(PIN_SPOTS[0]!.f + 8 + r() * 6, (r() - 0.5) * 6);
		this.ball.push(
			{
				kind: "fly",
				t0: out,
				t1: hit,
				from,
				to: { ...head, z: 0.3 },
				peak: 0,
			},
			{
				kind: "fly",
				t0: hit,
				t1: hit + BOUNCE_MS,
				from: { ...head, z: 0.3 },
				to: { ...rest, z: 0.3 },
				peak: 1.2,
			},
			{ kind: "spot", t0: hit + BOUNCE_MS, at: { ...rest, z: 0 } },
		);
		// Stood waiting for it, and knocked flat - the head pin first, the
		// others falling away to each side of him.
		pins.forEach((m, k) => {
			const p = PIN_SPOTS[k]!;
			m.hold(set, "stand", face);
			const down = hit + (k === 0 ? 0 : PIN_AFTER + r() * 120);
			m.hold(down, "stand", face);
			const off = spotOf(p.f + 2.5, p.s * 1.8);
			m.act("tackledBack", down + 450, face, off);
			m.hold(down + 450 + PIN_DOWN_MS + r() * 200, "supine");
			m.act("getUpBack", m.t + GET_UP_MS);
			// (Up, and over to him.)
			const by = Math.atan2(m.at.y - at.y, m.at.x - at.x) + (k - 1) * 0.5;
			const ring = {
				x: at.x + Math.cos(by) * MOB_RING,
				y: at.y + Math.sin(by) * MOB_RING,
			};
			const v = m.top * 0.8;
			const far = dist(m.at, ring);
			const ease = Math.min(far * 0.5, (v * v) / (2 * STOP_ACCEL * STOP_HARD));
			// (Round any man down on the way - not over him.)
			const way = this.wayRound(m.at, ring, m.t);
			for (const p of way.slice(0, -1)) {
				m.run(p, v, "run");
			}
			const last = dist(m.at, ring);
			const u = last > 0 ? Math.max(0, last - ease) / last : 1;
			m.run(
				{
					x: m.at.x + (ring.x - m.at.x) * u,
					y: m.at.y + (ring.y - m.at.y) * u,
				},
				v,
				"run",
			);
			m.easeOff(ease);
			m.act("leap", m.t + 800, this.toward(m.at, at));
			m.hold(m.t + 1400, "stand", this.toward(m.at, at));
			this.mob.add(m.pid);
			this.until = Math.max(this.until, m.t);
		});
		// And him, at it.
		scorer.hold(hit + 250, "stand", way);
		scorer.act("leap", scorer.t + 800, way);
		scorer.hold(this.until, "stand");
		this.mob.add(scorer.pid);
		this.until = Math.max(this.until, scorer.t);
		this.mobRound(scorer, c0, true);
		// (The camera on them all.)
		this.party = { from: c0, at: spotOf(PIN_SPOTS[0]!.f / 2, 0) };
		return true;
	}

	// The way from `from` to `to` at t round every man down then but `but`,
	// past his head or his feet - by a corner or two, the shortest way clear
	// of them all (see taunt); straight there, if there is none - or, with
	// `near`, if he is too near one to start with (stepped off it after:
	// see offMenDown). The corners on it, and `to`.
	wayRound(from: Pt, to: Pt, t: number, but: Actor[] = [], near = false): Pt[] {
		const bodies = this.men
			.filter((o) => !but.includes(o) && DOWN.has(legAt(o, t).move))
			.map((o) => {
				const at = o.posAt(t);
				const yaw = yawThen(o, t + 1);
				const head = LIE_BACKWARD.has(legAt(o, t + 1).move) ? -1 : 1;
				const u = { x: Math.cos(yaw) * head, y: Math.sin(yaw) * head };
				const along = (d: number, x: number): Pt => ({
					x: at.x + u.x * d - u.y * x,
					y: at.y + u.y * d + u.x * x,
				});
				return {
					tip: along(HELP_CHEST + TAUNT_HEAD_FT, 0),
					toe: along(-HELP_LEGS, 0),
					corners: [1, -1].flatMap((x) => [
						along(
							HELP_CHEST + TAUNT_HEAD_FT + TAUNT_ROUND_FT,
							x * TAUNT_ROUND_FT,
						),
						along(-HELP_LEGS - TAUNT_ROUND_FT, x * TAUNT_ROUND_FT),
					]),
				};
			});
		// (No nearer any of them on the way than either end of it, if that
		// is nearer - a little.)
		const clear = (a: Pt, b: Pt) =>
			bodies.every(
				(o) =>
					segGap(a, b, o.tip, o.toe) >=
					Math.min(
						TAUNT_CLEAR_FT,
						segGap(a, a, o.tip, o.toe),
						segGap(b, b, o.tip, o.toe),
					) -
						TAUNT_SLACK_FT,
			);
		if (
			bodies.length === 0 ||
			clear(from, to) ||
			(near &&
				bodies.some((o) => segGap(from, from, o.tip, o.toe) < TAUNT_CLEAR_FT))
		) {
			return [to];
		}
		const corners = bodies.flatMap((o) => o.corners);
		let best: Pt[] = [to];
		let most = Infinity;
		for (const a of corners) {
			if (!clear(from, a)) {
				continue;
			}
			const la = dist(from, a);
			if (la + dist(a, to) < most && clear(a, to)) {
				most = la + dist(a, to);
				best = [a, to];
			}
			for (const b of corners) {
				const len = la + dist(a, b) + dist(b, to);
				if (b !== a && len < most && clear(a, b) && clear(b, to)) {
					most = len;
					best = [a, b, to];
				}
			}
		}
		return best;
	}

	// The moment after: `hero`'s teammates nearest running to him from c0,
	// to leap with him, the other side's nearest hands on hips, turned away.
	mobRound(hero: Actor, c0: number, alone = false) {
		const r = this.r;
		const at = hero.at;
		this.mobAt = at;
		this.party = { from: c0, at, hero: hero.pid };
		// His teammates nearest, to him.
		const start = c0 + 150;
		// (Away on his own, a long way ahead of them all: the nearest of them
		// coming down the field after him, however long it takes.)
		const mates = this.men
			.filter(
				(m) =>
					m !== hero && m.team === hero.team && !DOWN.has(legAt(m, start).move),
			)
			.sort((a, b) => dist(a.posAt(c0), at) - dist(b.posAt(c0), at))
			.filter((m, k) => k < 2 || dist(m.posAt(c0), at) < MOB_FROM)
			.slice(0, alone ? 0 : this.crunch ? CRUNCH_MOB : MOB);
		const rings = spreadRound(
			mates.map((m, k) => {
				const from = m.posAt(start);
				return Math.atan2(from.y - at.y, from.x - at.x) + (k - 1.5) * 0.25;
			}),
			MOB_GAP,
		);
		// (Round him clear of any man down there - in a pile, say - and of
		// the others round him.)
		const others = this.men.filter((m) => m !== hero && !mates.includes(m));
		const lyingFrom = (t0: number): Pt[] =>
			MOB_LIE_AT.flatMap((d) =>
				others
					.filter((m) => DOWN.has(legAt(m, t0 + d).move))
					.map((m) => m.posAt(t0 + d)),
			);
		const late = lyingFrom(start);
		let lying = late;
		const taken: Pt[] = [];
		const ringAt = (ang: number): Pt => ({
			x: at.x + Math.cos(ang) * MOB_RING,
			y: at.y + Math.sin(ang) * MOB_RING,
		});
		// (Nowhere that clear: the clearest.)
		const room = (p: Pt) =>
			Math.min(
				...lying.map((q) => dist(q, p)),
				...taken.map((q) => dist(q, p)),
			);
		mates.forEach((m, k) => {
			const near = dist(m.posAt(c0), at) < MOB_FROM;
			m.cut(start);
			// (Off as soon as he is free - round any man down there then, too.)
			lying = m.t < start ? [...late, ...lyingFrom(m.t)] : late;
			const ang = rings[k]!;
			const ways = PILE_TURNS.slice(0, -1).map((d) => ringAt(ang + d));
			const to =
				ways.find((p) => room(p) >= MOB_ROOM) ??
				ways.reduce((a, b) => (room(b) > room(a) + 0.01 ? b : a));
			taken.push(to);
			// (Pulled up as he gets there, hard - not leapt in off a sprint.)
			const v = m.top * (0.7 + r() * 0.15);
			const far = dist(m.at, to);
			const ease = Math.min(far * 0.5, (v * v) / (2 * STOP_ACCEL * STOP_HARD));
			// (Round any man down on the way - not over him.)
			const way = this.wayRound(m.at, to, m.t);
			for (const p of way.slice(0, -1)) {
				m.run(p, v, "sprint");
			}
			const last = dist(m.at, to);
			const u = last > 0 ? Math.max(0, last - ease) / last : 1;
			m.run(
				{ x: m.at.x + (to.x - m.at.x) * u, y: m.at.y + (to.y - m.at.y) * u },
				v,
				"sprint",
			);
			m.easeOff(ease);
			const t = m.t;
			const face = this.toward(m.at, at);
			if (r() < 0.65) {
				m.act("leap", t + 800, face);
			}
			if (near) {
				this.until = Math.max(this.until, m.t);
			}
			m.hold(m.t + 1800 + r() * 800, "stand", face);
			this.mob.add(m.pid);
		});
		// (Those down by him - a pile under him - in on it as they are up.)
		const under = this.men.filter(
			(m) =>
				m !== hero &&
				m.team === hero.team &&
				!this.mob.has(m.pid) &&
				DOWN.has(legAt(m, start).move) &&
				dist(m.posAt(start), at) < MOB_UP_NEAR,
		);
		for (const m of under) {
			const l = m.legs.findLast((x) => DOWN.has(x.move));
			if (l) {
				m.cut(l.t1);
			}
			const face = this.toward(m.at, at);
			m.act(r() < 0.5 ? "leap" : "fistPump", m.t + 1000, face);
			m.hold(m.t + 1400 + r() * 600, "stand", face);
			this.mob.add(m.pid);
			this.until = Math.max(this.until, m.t);
		}
		// The other side's nearest: hands on hips, turned away.
		const beaten = this.men
			.filter((m) => m.team !== hero.team && dist(m.posAt(c0), at) < 40)
			.sort((a, b) => dist(a.posAt(c0), at) - dist(b.posAt(c0), at))
			.slice(0, 3);
		// (Off the way the scorer came out to meet his - not into it.)
		const w = this.went;
		const along = (p: Pt): Pt => {
			if (!w) {
				return at;
			}
			const dx = at.x - w.x;
			const dy = at.y - w.y;
			const u = Math.min(
				1,
				Math.max(
					0,
					((p.x - w.x) * dx + (p.y - w.y) * dy) / (dx * dx + dy * dy),
				),
			);
			return { x: w.x + dx * u, y: w.y + dy * u };
		};
		for (const [k, m] of beaten.entries()) {
			const t = Math.max(m.t, c0 + 300 + r() * 700);
			m.hold(t);
			// (The man on him for a pass, now and then, at the official on the
			// nearer sideline for a flag first: his arms out.)
			if (
				k === 0 &&
				this.input.kind === "pass" &&
				dist(m.posAt(c0), at) < APPEAL_NEAR &&
				r() < APPEAL
			) {
				m.act(
					"protest",
					t + APPEAL_MS,
					m.at.y > MID_Y ? FACING_NEAR : -FACING_NEAR,
				);
			}
			const away = this.toward(along(m.at), m.at) + (r() - 0.5) * 0.8;
			const from = m.t;
			m.go(
				{ x: m.at.x + Math.cos(away) * 6, y: m.at.y + Math.sin(away) * 6 },
				from + 1800,
				"walk",
			);
			if (r() < RUE_HIPS) {
				m.arms("onHips", from, m.t);
			}
			m.hold(m.t + 2400, "blowing");
			this.mob.add(m.pid);
		}
	}

	// A turnover: the man who came up with it, once he is up - the ball held
	// up, or a leap - his side's men round him.
	takeaway() {
		const r = this.r;
		const { kind, offense, events, i0, i1 } = this.input;
		if (kind !== "pass" && kind !== "run") {
			return;
		}
		let turned = false;
		for (let i = i0; i < i1; i++) {
			const e = events[i];
			if (e?.type === "interception" || e?.lost === true) {
				turned = true;
			}
		}
		const held = this.ball.findLast((b) => b.kind === "held");
		const hero = held?.kind === "held" ? this.man(held.pid) : undefined;
		if (
			!turned ||
			!hero ||
			hero.team === offense ||
			r() > (this.crunch ? CRUNCH_PARTY : TAKEAWAY)
		) {
			return;
		}
		const legs = hero.legs;
		let k = legs.length - 1;
		while (k > 1 && legs[k]!.move === "stand") {
			k--;
		}
		const c0 = Math.max(this.whistle + 300, legs[k]!.t1);
		hero.cut(c0);
		hero.hold(c0);
		if (r() < 0.6) {
			hero.hold(c0 + 1400, "ballUp", playedOut(hero));
		} else {
			hero.act("leap", c0 + 800, playedOut(hero));
		}
		this.until = hero.t;
		hero.hold(hero.t + 2200, "stand");
		this.mob.add(hero.pid);
		this.mobRound(hero, c0);
	}

	// Stopped short: the man who made the stop up and into it - a flex, a
	// fist pumped, a leap - and the nearest of his teammates over to him,
	// one chest to chest with him now and then, the other clapping.
	stopParty() {
		const r = this.r;
		const hero =
			this.man(this.statPids("defTckSolo", "defTckAst")[0]) ??
			this.downer ??
			this.man(this.statPids("defPssDef")[0]);
		if (!hero || hero.team === this.input.offense || this.mob.has(hero.pid)) {
			return;
		}
		const legs = hero.legs;
		let k = legs.length - 1;
		while (k > 1 && legs[k]!.move === "stand") {
			k--;
		}
		const c0 = Math.max(this.whistle + STOP_AFTER, legs[k]!.t1);
		hero.cut(c0);
		hero.hold(c0);
		// (A few steps away from the man he stopped first - out the side of
		// the ball the defense was on, off the offense's way to its huddle.)
		const ball = this.ballAt(c0);
		if (ball) {
			const far = dist(hero.at, ball);
			const ux = far > 0.5 ? (hero.at.x - ball.x) / far : 0;
			const uy = far > 0.5 ? (hero.at.y - ball.y) / far : 0;
			const way = Math.atan2(uy, ux + this.input.dir);
			if (far < STOP_AWAY) {
				hero.run(
					{
						x: hero.at.x + Math.cos(way) * (STOP_AWAY - far),
						y: hero.at.y + Math.sin(way) * (STOP_AWAY - far),
					},
					STOP_STEP,
				);
			}
		}
		const u = r();
		hero.act(
			u < 0.4 ? "flex" : u < 0.7 ? "fistPump" : "leap",
			hero.t + (u < 0.7 ? 1200 : 800),
			playedOut(hero),
		);
		this.mob.add(hero.pid);
		const at = hero.at;
		this.mobAt = at;
		this.party = { from: c0, at, hero: hero.pid };
		const start = c0 + 150;
		const mates = this.men
			.filter(
				(m) =>
					m !== hero &&
					m.team === hero.team &&
					!this.mob.has(m.pid) &&
					!DOWN.has(legAt(m, start).move) &&
					dist(m.posAt(start), at) < STOP_NEAR,
			)
			.sort((a, b) => dist(a.posAt(start), at) - dist(b.posAt(start), at))
			.slice(0, (r() < 0.5 ? 2 : 1) + (this.crunch ? 1 : 0));
		let end = hero.t;
		mates.forEach((m, j) => {
			const from = m.posAt(start);
			const toHim = dist(from, at) > 0.5 ? this.toward(at, from) : r() * 6.28;
			const bump = j === 0 && r() < STOP_BUMP;
			const five = j === 0 && !bump && r() < STOP_FIVE;
			const way = bump || five ? sideOn(toHim) : toHim;
			const off = bump ? STOP_BUMP_APART : five ? FIVE.apart : STOP_RING;
			const spot = {
				x: at.x + Math.cos(way) * off,
				y: at.y + Math.sin(way) * off,
			};
			m.cut(start);
			// (Still going, quick: planted and round to him - not stopped dead
			// and off back the other way.)
			if (GAITS.has(m.last.move) && legPace(m.last) >= STOP_LEAST) {
				m.plantFor(spot);
			} else {
				m.hold(start);
			}
			// (There already, near enough: turned to him where he is - not
			// a blink of a run on the spot.)
			// (A step or two off it: stepped over to it, still facing him -
			// not turned and run there, his back to him.)
			const left = dist(m.at, spot);
			if (left > STOP_THERE_FT && left <= PARTY_STEP_FT) {
				m.go(
					spot,
					m.t + Math.max(PARTY_STEP_MS, (left / PARTY_STEP_FPS) * 1000),
					"walk",
					way + Math.PI,
				);
			} else if (left > STOP_THERE_FT) {
				m.runUp(spot, Math.min(m.top * 0.6, STOP_FPS));
			}
			if (bump) {
				const t = Math.max(m.t, hero.t) + FIVE_SET_MS;
				m.hold(t, "stand", way + Math.PI);
				hero.hold(t, "stand", way);
				m.act("chestBump", t + STOP_BUMP_MS, way + Math.PI);
				hero.act("chestBump", t + STOP_BUMP_MS, way);
			} else if (five) {
				const t = Math.max(m.t, hero.t) + FIVE_SET_MS;
				m.hold(t, "stand", way + Math.PI - FIVE.turn);
				hero.hold(t, "stand", way - FIVE.turn);
				m.act("highFive", t + FIVE_MS, way + Math.PI - FIVE.turn);
				hero.act("highFive", t + FIVE_MS, way - FIVE.turn);
			} else {
				m.act(r() < 0.5 ? "clap" : "fistPump", m.t + 1200, way + Math.PI);
			}
			m.hold(m.t + 500, "stand");
			this.mob.add(m.pid);
			end = Math.max(end, m.t);
		});
		hero.hold(hero.t + 500, "stand");
		this.until = Math.max(this.until, end, hero.t);
	}

	// A new set of downs, now and then: the man who got it up off the turf
	// quick - or pulled up on his feet - and pointing the way they are going.
	pointFirst() {
		const r = this.r;
		const held = this.ball.findLast((b) => b.kind === "held");
		const m = held?.kind === "held" ? this.man(held.pid) : undefined;
		if (
			!m ||
			m.team !== this.input.offense ||
			r() > (this.crunch ? CRUNCH_POINT : POINT_FIRST)
		) {
			return;
		}
		// (Past whatever he was stood doing after.)
		const legs = m.legs;
		let k = legs.length - 1;
		while (k > 1 && legs[k]!.move === "stand") {
			k--;
		}
		const rise = legs[k]!;
		const lay = legs[k - 1]!;
		let t0: number;
		if (RISING.has(rise.move) && moveKind(lay.move) === "still") {
			legs.length = k + 1;
			const ms = rise.t1 - rise.t0;
			// (Not up from under a man still lying across him, though - nor
			// where he would stand: up once the last of them is.)
			let quick = lay.t0 + UP_QUICK;
			for (const o of this.men) {
				for (let t = quick; o !== m && t < lay.t1; t += UP_OFF_EVERY_MS) {
					if (this.nearOf(o, t, m.at, DOWN_LEGS) < DOWN_CLEAR) {
						quick = t + UP_OFF_MS;
					}
				}
			}
			lay.t1 = Math.max(lay.t0 + 1, Math.min(lay.t1, quick));
			rise.t0 = lay.t1;
			rise.t1 = rise.t0 + ms * 0.8;
			t0 = rise.t1;
		} else if (legs.some((l) => l.t1 > this.whistle && DOWN.has(l.move))) {
			return;
		} else {
			t0 = Math.max(this.whistle + 500, Math.min(m.t, this.whistle + 900));
			m.cut(t0);
			m.hold(t0);
		}
		m.act("pointFirst", t0 + POINT_MS, this.ahead + (r() - 0.5) * 0.7);
		m.hold(m.t + 300, "stand");
		// (After a long way gained, his moment: the camera in on it.)
		const { events, i0, i1 } = this.input;
		const gain = num(
			events
				.slice(i0, i1)
				.find((e) => e?.type === "run" || e?.type === "passComplete")?.yds,
			0,
		);
		if (gain >= POINT_BIG) {
			this.party = {
				from: Math.max(this.whistle, t0 - POINT_LEAD),
				at: m.at,
				hero: m.pid,
			};
			this.until = Math.max(this.until, m.t);
		}
	}

	// Stood where his part left him a long while, the play still going
	// till `until`: turned to the ball as it goes - see WATCH_IDLE_LEAST_MS.
	private watchIdle(m: Actor, until: number) {
		const legs = m.legs;
		const still = (l: Leg) =>
			l.move === "stand" &&
			!l.arms &&
			!l.ball &&
			!l.turn &&
			l.from.x === l.to.x &&
			l.from.y === l.to.y;
		let k = legs.length;
		while (k > 1 && still(legs[k - 1]!)) {
			k--;
		}
		const first = legs[k];
		if (!first || first.t0 < this.input.snap) {
			return;
		}
		const end = legs.at(-1)!.t1;
		if (Math.min(end, until) - first.t0 < WATCH_IDLE_LEAST_MS) {
			return;
		}
		const at = first.from;
		let yaw = first.yaw ?? m.yaw;
		const out: Leg[] = [];
		let t0 = first.t0;
		let c = t0;
		for (let j = 0; ; j++) {
			c +=
				WATCH_IDLE_EVERY_MS[0] +
				WATCH_IDLE_EVERY_MS[1] * countOf(m, this.input.key, 165 + (j % 8));
			if (c >= Math.min(end, until) - 200) {
				break;
			}
			const ball = this.ballAt(c);
			if (!ball || dist(ball, at) < 1) {
				continue;
			}
			const off = angleOff(this.toward(at, ball) - yaw);
			if (Math.abs(off) < WATCH_IDLE_LEAST) {
				continue;
			}
			out.push({ ...first, t0, t1: c, yaw });
			yaw += clamp(off, -WATCH_IDLE_MOST, WATCH_IDLE_MOST);
			t0 = c;
		}
		if (out.length === 0) {
			return;
		}
		out.push({ ...first, t0, t1: end, yaw });
		legs.splice(k, legs.length - k, ...out);
		m.yaw = yaw;
	}

	// Everyone not already given his part to the end: pulled up where the
	// whistle finds him - a few steps on the way he was going, slowing -
	// and stood there.
	afterWhistle() {
		const r = this.r;
		const t = this.whistle;
		const { kind } = this.input;
		const last = this.ball.findLast((b) => b.t0 <= t);
		const carrier =
			(kind === "pass" || kind === "run") && last?.kind === "held"
				? this.man(last.pid)
				: undefined;
		// (Gone over the sideline with it: pulled up there already - see
		// runOut.)
		const out =
			this.outAt && last?.kind === "held" ? this.man(last.pid) : undefined;
		// (Since when he has stood, his arms to himself.)
		const idleFrom = (m: Actor) => {
			const legs = m.legs;
			let k = legs.length - 1;
			while (
				k > 0 &&
				legs[k]!.move === "stand" &&
				!legs[k]!.arms &&
				legs[k]!.from.x === legs[k]!.to.x &&
				legs[k]!.from.y === legs[k]!.to.y
			) {
				k--;
			}
			return legs[k]!.t1;
		};
		const ends: Pt[] = [];
		const ball = this.ballAt(t);
		for (const m of this.men) {
			if (!this.done.has(m.pid)) {
				// (Nothing for him: watching from where he was.)
				m.hold(Math.max(m.t, this.input.snap + READ_MS));
			}
			// His part done (stood where it left him) and the ball still
			// going, a long way: on after it, keeping off the man with it.
			const idle =
				carrier && m !== carrier
					? Math.max(idleFrom(m), this.over.get(m) ?? 0)
					: t;
			if (
				carrier &&
				idle < t - FOLLOW_LEFT &&
				dist(carrier.posAt(idle), carrier.posAt(t)) > FOLLOW_GONE
			) {
				m.cut(idle);
			}
			if (
				carrier &&
				m !== carrier &&
				m.t < t - FOLLOW_LEFT &&
				!DOWN.has(m.last.move) &&
				dist(carrier.posAt(m.t), carrier.posAt(t)) > FOLLOW_GONE
			) {
				const seen = m.t + FOLLOW_REACT[0] + r() * FOLLOW_REACT[1];
				// (On the move still - backpedaling, too - slowing up as he
				// sees it: not stopped dead for it and off again.)
				if (STEPS.has(m.last.move) && legPace(m.last) >= STOP_LEAST) {
					m.easeOff(EASE_OFF_FT);
				}
				const go = Math.max(seen, m.t);
				const c = carrier.posAt(t);
				const far = dist(m.at, c);
				const mate = m.team === carrier.team;
				const keep = mate
					? FOLLOW_KEEP[0] + r() * FOLLOW_KEEP[1]
					: FOLLOW_CLOSE[0] + r() * FOLLOW_CLOSE[1];
				if (far > keep + 3 && t - go >= FOLLOW_LEAST_MS) {
					const most =
						(OL.has(m.role) || DL.has(m.role)
							? FOLLOW_FPS.line
							: m.role === "QB"
								? FOLLOW_FPS.qb
								: FOLLOW_FPS.other) + (mate ? 0 : FOLLOW_FPS.after);
					// (Where he gets to by the whistle, off where the others
					// after it get to - in from a little to one side, if not.)
					const from = Math.atan2(m.at.y - c.y, m.at.x - c.x);
					const room = (most * (t - go)) / 1000;
					// (Not on to - nor through - a man down then.)
					const downs = this.men
						.filter((x) => x !== m && DOWN.has(legAt(x, t).move))
						.map((x) => x.posAt(t));
					const clear = (p: Pt) =>
						downs.every((d) => {
							for (let u = 0; u <= 1.001; u += 0.1) {
								const q = {
									x: m.at.x + (p.x - m.at.x) * u,
									y: m.at.y + (p.y - m.at.y) * u,
								};
								if (dist(q, d) < FOLLOW_DOWN_CLEAR) {
									return false;
								}
							}
							return true;
						});
					let to = c;
					let end = c;
					for (const turn of FOLLOW_TURNS) {
						to = {
							x: c.x + Math.cos(from + turn) * keep,
							y: c.y + Math.sin(from + turn) * keep,
						};
						const d = dist(m.at, to);
						const u = d > room ? room / d : 1;
						end = {
							x: m.at.x + (to.x - m.at.x) * u,
							y: m.at.y + (to.y - m.at.y) * u,
						};
						if (!ends.some((e) => dist(e, end) < FOLLOW_APART) && clear(end)) {
							break;
						}
					}
					ends.push(end);
					const fps = Math.min(most, (dist(m.at, to) / (t - go)) * 1000);
					// (The same way at the same pace, each pulled up on his
					// own count - see countOf - not all on the whistle.)
					const by = Math.max(
						go + (t - go) * FOLLOW_ON_LEAST,
						t - FOLLOW_UP_SPREAD_MS * countOf(m, this.input.key, 9),
					);
					m.hold(go);
					// (Round the men still in blocks on the way, if there are
					// any in it - not on through them.)
					// (As far as he gets on the way by then - up to speed
					// as he gets going: see rampInto.)
					const set = SET.has(m.last.move);
					const v0 = set ? 0 : paceOn(m.last, m.at, to);
					const A = set ? RELEASE_ACCEL : RAMP_ACCEL;
					const all = rampedMs(dist(m.at, to), fps, v0, A);
					const u = all > by - m.t ? (by - m.t) / all : 1;
					const gets = {
						x: m.at.x + (to.x - m.at.x) * u,
						y: m.at.y + (to.y - m.at.y) * u,
					};
					const there = Math.min(by, m.t + all);
					const round =
						there > m.t
							? this.roundBlocks(m, gets, there, m, fps / m.top)
							: undefined;
					if (round) {
						// (At his pace round it, no quicker: as far round as
						// that gets him by then.)
						this.alongAt(m, round, fps, v0, A);
						if (m.t > by) {
							m.cut(by);
						}
					} else {
						m.chase(to, by, fps, gait(fps));
					}
				}
			}
			if (m.t > t + 400 && m !== out) {
				// (Nothing left for him but running on - a route, after a man
				// who is down: pulled up as he sees it is over.)
				// (Each as quick to see it as he is: no dice thrown for it,
				// not to reshuffle what comes after.)
				let seen =
					t +
					WHISTLE_SEEN_MS[0] +
					(((m.pid * 37) % 100) / 100) * WHISTLE_SEEN_MS[1];
				const on = m.legs.filter((l) => l.t1 > seen);
				// (Not with a sliver of a step left in: from the one before.)
				const k = m.legs.length - on.length;
				const before = m.legs[k - 1];
				if (
					on.length > 0 &&
					seen - on[0]!.t0 < WHISTLE_SLIVER_MS &&
					before &&
					before.t1 === on[0]!.t0 &&
					GAITS.has(before.move) &&
					legPace(before) >= STOP_LEAST
				) {
					seen = on[0]!.t0;
				}
				const going = on[0] && seen > on[0].t0 ? on[0] : before;
				if (
					on.length > 0 &&
					on[0]!.t0 <= seen &&
					going &&
					on.every((l) => GAITS.has(l.move) || l.move === "stand") &&
					legPace(going) >= STOP_LEAST
				) {
					m.cut(seen);
					// (Nor pulled up out of a stride of a new gait: out of the
					// one he was in.)
					const [a, b] = m.legs.slice(-2);
					if (
						a &&
						b &&
						b.t1 - b.t0 < WHISTLE_SLIVER_MS &&
						a.t1 === b.t0 &&
						GAITS.has(a.move) &&
						GAITS.has(b.move)
					) {
						b.move = a.move;
					}
				}
				// (Still on his way in when his part ends: pulled up the way
				// he was going - short of the ball.)
				// (Short of it by half the way, or as much as pulling up hard
				// takes, short of the ball.)
				// (Or by it, if he cannot pull up short of it.)
				// (Nor over a man down on the way: bent off by him.)
				this.roundBegun(m);
				if (GAITS.has(m.last.move) && ball) {
					const v = legPace(m.last);
					const near = dist(m.at, ball);
					const stop = (v * v) / (2 * STOP_ACCEL * STOP_HARD);
					if (stop > near - 2.5) {
						m.easeOff(Math.min(EASE_OFF_FT, stop), ball);
					} else {
						const ft = Math.min(EASE_OFF_FT, Math.max(near * 0.5, stop));
						m.easeOff(ft, this.lyingOn(m, ft));
					}
				}
				m.hold(m.t + 600, "stand");
				continue;
			}
			this.roundBegun(m);
			// (Pulled up clear of the blocks and tackles still going on -
			// see easeShort - not on into them.)
			if (GAITS.has(m.last.move)) {
				this.easeShort(m, m.t - FILL_BACK_MOST_MS, EASE_OFF_FT);
			}
			const l = m.last;
			const moving = l.from.x !== l.to.x || l.from.y !== l.to.y;
			if (moving) {
				// (Nor over a man down on the way: bent off by him.)
				const v = legPace(l);
				m.easeOff(
					EASE_OFF_FT,
					this.lyingOn(m, Math.min(EASE_OFF_FT, (v * v) / (2 * STOP_ACCEL))),
				);
			}
			m.hold(Math.max(m.t, t) + 600, "stand");
			if ((kind === "pass" || kind === "run") && m !== carrier) {
				this.watchIdle(m, t);
			}
		}
	}

	// A cut he had only begun as the whistle goes - the last of his legs
	// no more than WHISTLE_CUT_MS long, sharply off the one before:
	// rounded into the way he was cutting, at his pace (see
	// Actor.bendFor), for him to pull up along - not turned in a stride
	// at full tilt.
	private roundBegun(m: Actor) {
		const [p, q] = m.legs.slice(-2);
		const qFar = q ? dist(q.from, q.to) : 0;
		if (
			!p ||
			!q ||
			qFar <= 0 ||
			q.t1 - q.t0 >= WHISTLE_CUT_MS ||
			p.t1 !== q.t0 ||
			!GAITS.has(p.move) ||
			!GAITS.has(q.move) ||
			p.arms ||
			q.arms ||
			dist(p.from, p.to) <= 0 ||
			legPace(p) < WHISTLE_CUT_PACE ||
			!(angleAt(p.from, p.to, q.to) > BEND_LEAST)
		) {
			return;
		}
		const v = legPace(p);
		const ft = qFar + EASE_OFF_FT;
		const to = {
			x: q.from.x + ((q.to.x - q.from.x) / qFar) * ft,
			y: q.from.y + ((q.to.y - q.from.y) / qFar) * ft,
		};
		m.cut(q.t0);
		if (
			m.bendFor(to, m.t + (dist(m.at, to) / Math.max(v, 1)) * 1000, 0) ===
			undefined
		) {
			m.legs.push(q);
		}
	}

	// Where a man down is in m's way on along the way he is going, the
	// next `ft` feet of it, if anybody is - as he gets there, near enough
	// (see easeOff).
	private lyingOn(m: Actor, ft: number): Pt | undefined {
		const l = m.last;
		const far = dist(l.from, l.to);
		const v = legPace(l);
		if (far <= 0 || v <= 0) {
			return undefined;
		}
		const ux = (l.to.x - l.from.x) / far;
		const uy = (l.to.y - l.from.y) / far;
		for (let s = 0; s <= ft * 2; s += 1) {
			const p = { x: m.at.x + ux * s, y: m.at.y + uy * s };
			const t = m.t + (s / v) * 1000;
			for (const o of this.men) {
				const ol = legAt(o, t);
				if (o !== m && DOWN.has(ol.move)) {
					// (Going down still - in the air on a dive, say: where he
					// comes down, not where he is.)
					const at = moveKind(ol.move) === "act" ? ol.to : o.posAt(t);
					if (dist(at, p) < EASE_PAST_FT) {
						return at;
					}
				}
			}
		}
		return undefined;
	}

	// UP AGAIN WITH IT STILL GOING: a man up off the turf with the play
	// going on a while yet - in on a loose ball and missed it, knocked down
	// as it came out - back in after it at an easy run, each on his own
	// count, to a place of his own round where it ends up: not stood where
	// he got up till the whistle.
	upAgain() {
		const { snap } = this.input;
		const end = this.ballAt(this.whistle);
		if (!end) {
			return;
		}
		const still = (l: Leg) =>
			l.move === "stand" &&
			!l.arms &&
			!l.ball &&
			l.from.x === l.to.x &&
			l.from.y === l.to.y;
		const up = new Map<Actor, number>();
		for (const m of this.men) {
			const legs = m.legs;
			let k = legs.length - 1;
			while (k > 0 && still(legs[k]!) && dist(legs[k]!.to, m.at) < 0.01) {
				k--;
			}
			const l = legs[k];
			if (
				l &&
				RISING.has(l.move) &&
				l.move !== "sitUp" &&
				l.t1 >= snap &&
				this.whistle - l.t1 >= UP_AGAIN_LEAST_MS
			) {
				up.set(m, l.t1);
			}
		}
		// (The nearest it first, to the nearest places round it - each clear
		// of the others' and of anybody stood there then.)
		const taken = this.men
			.filter((m) => !up.has(m))
			.map((m) => m.posAt(this.whistle));
		// (When the ball is in the spot it ends in: from then on no further
		// off it than SETTLED_FT.)
		let settled = this.whistle;
		for (let c = this.whistle; c > snap; c -= 50) {
			const at = this.ballAt(c);
			if (!at || dist(at, end) > SETTLED_FT) {
				break;
			}
			settled = c;
		}
		for (const [m, t] of [...up].sort(
			(a, b) => dist(a[0].at, end) - dist(b[0].at, end),
		)) {
			const ring =
				UP_AGAIN_FT[0] + UP_AGAIN_FT[1] * countOf(m, this.input.key, 25);
			const far = dist(m.at, end);
			if (far <= ring + 2) {
				continue;
			}
			// (Round it with room for him - further out, if it is crowded
			// there: not left stood where he got up.)
			const spot = () => {
				const a = Math.atan2(m.at.y - end.y, m.at.x - end.x);
				for (const out of UP_AGAIN_OUT_FT) {
					const R = ring + out;
					if (far <= R + 2) {
						break;
					}
					const q = GATHER_ROUND.map((turn) => ({
						x: end.x + Math.cos(a + turn / R) * R,
						y: end.y + Math.sin(a + turn / R) * R,
					})).find((q) => taken.every((p) => dist(p, q) >= GATHER_APART));
					if (q) {
						return q;
					}
				}
				return undefined;
			};
			const go =
				t +
				UP_AGAIN_REACT_MS[0] +
				(UP_AGAIN_REACT_MS[1] - UP_AGAIN_REACT_MS[0]) *
					countOf(m, this.input.key, 26);
			const pace = m.top * UP_AGAIN_PACE;
			// (The ball still going a good while after he would be there - a
			// return, a fumble picked up and taken on: with it as it goes,
			// kept off it, till it is in its spot - not off to that spot
			// ahead of it, to stand there waiting for it.)
			const early =
				settled - go - ((far - ring) / pace) * 1000 > UP_AGAIN_EARLY_MS;
			const to = early ? undefined : spot();
			if (!early && !to) {
				continue;
			}
			if (m.t > go) {
				m.cut(go);
			}
			m.hold(go);
			if (early) {
				this.alongWith(m, pace, ring, settled);
			}
			const at = to ?? spot();
			if (!at) {
				continue;
			}
			taken.push(at);
			if (m.t + (dist(m.at, at) / pace) * 1000 <= this.whistle) {
				m.runUp(at, pace);
			} else {
				m.chase(at, this.whistle, pace);
			}
		}
	}

	// Stood with nothing to do while the play goes on: turned, now and then,
	// to keep the ball in front of him - not a statue looking off at nothing.
	// TAUNTING, as the sim flags it: the man it names, free of what the play
	// left him in, over to the nearest of the other side - one down on the
	// turf, stood over him beside his chest pointing down at him, or flexing
	// (him kept down for it); on his feet, in his face, flexing (him stood
	// there taking it) - the flag coming in on it (see fouls.ts). Not over
	// a man hurt, nor out of a party. (No dice: nothing else in the play
	// changes with it.)
	taunt() {
		const { events, i0, i1 } = this.input;
		let k = -1;
		for (let i = i0; i < i1; i++) {
			const e = events[i];
			if (e?.type === "penalty" && e.penaltyName === "Taunting") {
				k = i;
			}
		}
		const m =
			k >= 0 ? this.man(this.statPidsIn(k, this.hi, "pen")[0]) : undefined;
		if (!m || this.mob.has(m.pid)) {
			return;
		}
		const w = this.whistle;
		let from = w + TAUNT_AFTER_MS;
		for (const l of m.legs) {
			if (
				l.t0 <= from &&
				l.t1 > from &&
				!GAITS.has(l.move) &&
				l.move !== "stand"
			) {
				from = l.t1;
			}
		}
		if (from > w + TAUNT_LATE_MS) {
			return;
		}
		const me = m.posAt(from);
		const lies = (o: Actor) => TAUNT_LIES.has(legAt(o, from).move);
		// (Stood still from: pulled up.)
		const stillAt = (o: Actor) =>
			o.legs.find(
				(l) =>
					l.t1 > from &&
					l.move === "stand" &&
					l.from.x === l.to.x &&
					l.from.y === l.to.y,
			)?.t0 ?? o.t;
		const foes = this.men
			.filter(
				(o) =>
					o.team !== m.team &&
					!this.mob.has(o.pid) &&
					!o.legs.some((l) => l.move === "hurt" || l.move === "hurtProne") &&
					(lies(o) || !DOWN.has(legAt(o, from).move)) &&
					dist(o.posAt(from), me) <= TAUNT_FAR,
			)
			.sort((a, b) => dist(a.posAt(from), me) - dist(b.posAt(from), me));
		const others = this.men.filter((o) => o !== m);
		for (const foe of [...foes.filter(lies), ...foes.filter((o) => !lies(o))]) {
			const down = lies(foe);
			// (Where he lies once he is down, through the fall first - or
			// stands once he has pulled up.)
			const fall = legAt(foe, from);
			const kept = down
				? moveKind(fall.move) === "act"
					? fall.t1
					: from
				: Math.max(from, stillAt(foe));
			const at = foe.posAt(kept);
			let spots: Pt[];
			let chest = at;
			// (The way there: round his head or his feet, not over him.)
			let round = (p: Pt): Pt[] => [p];
			if (down) {
				// (Where a hand up would be, either side - see regroup.)
				const yaw = yawThen(foe, kept + 1);
				const u = { x: Math.cos(yaw), y: Math.sin(yaw) };
				const head = LIE_BACKWARD.has(legAt(foe, kept + 1).move) ? -1 : 1;
				const along = (d: number): Pt => ({
					x: at.x + u.x * head * d,
					y: at.y + u.y * head * d,
				});
				chest = along(HELP_CHEST);
				const n = { x: -u.y, y: u.x };
				// (Him, head to feet.)
				const tip = along(HELP_CHEST + TAUNT_HEAD_FT);
				const toe = along(-HELP_LEGS);
				spots = [1, -1].map((x) => ({
					x: chest.x + n.x * x * HELP_BESIDE,
					y: chest.y + n.y * x * HELP_BESIDE,
				}));
				// (Round him by a corner past either end, or two, the shortest
				// way clear of him.)
				const corner = (d: number, x: number): Pt => {
					const c = along(d);
					return {
						x: c.x + n.x * x * TAUNT_ROUND_FT,
						y: c.y + n.y * x * TAUNT_ROUND_FT,
					};
				};
				const heads = [1, -1].map((x) =>
					corner(HELP_CHEST + TAUNT_HEAD_FT + TAUNT_ROUND_FT, x),
				);
				const feet = [1, -1].map((x) => corner(-HELP_LEGS - TAUNT_ROUND_FT, x));
				const ways = [
					[],
					...[...heads, ...feet].map((c) => [c]),
					...[heads, feet].flatMap(([a, b]) => [
						[a!, b!],
						[b!, a!],
					]),
				];
				round = (p) => {
					let best: Pt[] = [p];
					let most = Infinity;
					for (const w of ways) {
						const path = [me, ...w, p];
						let len = 0;
						let clear = true;
						// (No nearer him on the way than either end of it, if
						// that is nearer - a little.)
						for (let j = 1; j < path.length; j++) {
							const a = path[j - 1]!;
							const b = path[j]!;
							len += dist(a, b);
							clear &&=
								segGap(a, b, tip, toe) >=
								Math.min(
									TAUNT_CLEAR_FT,
									segGap(a, a, tip, toe),
									segGap(b, b, tip, toe),
								) -
									TAUNT_SLACK_FT;
						}
						if (clear && len < most) {
							most = len;
							best = [...w, p];
						}
					}
					return best;
				};
			} else {
				// (In his face already: where he is.)
				const d = Math.max(0.01, dist(me, at));
				spots = [
					d <= TAUNT_FACE_FT + TAUNT_STAY_FT
						? me
						: {
								x: at.x + ((me.x - at.x) / d) * TAUNT_FACE_FT,
								y: at.y + ((me.y - at.y) / d) * TAUNT_FACE_FT,
							},
				];
			}
			const far = (p: Pt) => {
				let d = 0;
				let q = me;
				for (const x of round(p)) {
					d += dist(q, x);
					q = x;
				}
				return d;
			};
			// (The far side of him first, facing the camera over him - not
			// for a long way round.)
			const cost = (p: Pt) => far(p) + (p.y > chest.y ? TAUNT_FAR_SIDE_FT : 0);
			spots.sort((a, b) => cost(a) - cost(b));
			const time = (p: Pt) =>
				from + Math.max(TAUNT_STEP_MS, (far(p) / TAUNT_FPS) * 1000);
			// (Room there: clear of anybody else down there, or stood.)
			const spot = spots.find((p) =>
				others.every(
					(o) =>
						o === foe ||
						[time(p), time(p) + TAUNT_MS].every((t) => {
							const move = legAt(o, t).move;
							return (
								this.nearOf(o, t, p, HELP_LEGS) >=
								(LIE_FACING.has(move) || LIE_BACKWARD.has(move)
									? HELP_CLEAR
									: HELP_ROOM)
							);
						}),
				),
			);
			// (Not there before he is down, or stood to take it.)
			const there = spot
				? Math.max(time(spot), kept + TAUNT_TURN_MS)
				: Infinity;
			if (
				!spot ||
				there > w + TAUNT_THERE_MS ||
				(down && !TAUNT_LIES.has(legAt(foe, kept + 1).move))
			) {
				continue;
			}
			const face = this.toward(spot, chest);
			const end = there + TAUNT_MS;
			m.cut(from);
			m.hold(from);
			if (spot !== me) {
				// (Each stretch of the way its share of the time.)
				const all = Math.max(0.01, far(spot));
				let t = from;
				let q = me;
				for (const x of round(spot)) {
					t += ((there - from) * dist(q, x)) / all;
					m.go(x, t);
					q = x;
				}
			}
			m.hold(there);
			// (Pointing down at him where the camera sees it, over him; or
			// flexing, the arms flung out wide, his back to it.)
			m.act(down && spot.y < chest.y ? "taunt" : "flex", end, face);
			m.hold(end + TAUNT_AFTER_MS, "stand");
			this.busy.set(m.pid, m.t);
			if (down) {
				// (Kept down till it is over: up as he would have got up.)
				const up = foe.legs.find((l) => RISING.has(l.move) && l.t0 >= kept);
				if (up && up.t0 < end) {
					const lie = foe.legs[foe.legs.indexOf(up) - 1];
					const ms = up.t1 - up.t0;
					foe.cut(up.t0);
					foe.hold(end, lie?.move, lie?.yaw);
					foe.act(up.move, end + ms, up.yaw);
				}
			} else {
				// (Stood there, round to him, taking it.)
				foe.cut(kept);
				foe.hold(there - TAUNT_TURN_MS);
				foe.hold(end, "stand", this.toward(at, spot));
				this.busy.set(foe.pid, end);
			}
			return;
		}
	}

	watch() {
		const from = this.input.snap + WATCH_FROM_MS;
		const to = this.whistle;
		for (const m of this.men) {
			const turns: { t: number; yaw: number }[] = [];
			let yaw = m.legs[0]?.yaw ?? 0;
			for (const l of m.legs) {
				// (A moment stood first: up out of what he was in.)
				const a = Math.max(l.t0 + WATCH_STEP_MS, from);
				const b = Math.min(l.t1, to);
				if (
					l.move === "stand" &&
					!l.arms &&
					!l.ball &&
					!l.turn &&
					l.from.x === l.to.x &&
					l.from.y === l.to.y &&
					b - a >= WATCH_STEP_MS / 2
				) {
					let face = yaw;
					for (let t = a; t + WATCH_STEP_MS / 2 <= b; t += WATCH_STEP_MS) {
						const ball = this.ballAt(t);
						if (!ball || dist(ball, l.to) < WATCH_NEAR_FT) {
							continue;
						}
						const at = Math.atan2(ball.y - l.to.y, ball.x - l.to.x);
						const off = Math.atan2(Math.sin(at - face), Math.cos(at - face));
						if (Math.abs(off) > WATCH_TURN) {
							face = at;
							turns.push({ t, yaw: at });
						}
					}
				}
				yaw = yawOf(l, yaw);
			}
			for (const x of turns) {
				m.split(x.t);
			}
			for (const [k, x] of turns.entries()) {
				const until = turns[k + 1]?.t ?? Infinity;
				for (const l of m.legs) {
					if (l.t0 >= x.t && l.t0 < until && l.move === "stand") {
						l.yaw = x.yaw;
					}
				}
			}
		}
	}

	// ---- between plays -------------------------------------------------------------

	// Where the ball is at t: on the turf, in the air, or with a man.
	ballAt(t: number): Pt | undefined {
		const s = this.ball.findLast((b) => b.t0 <= t);
		if (!s) {
			return undefined;
		}
		if (s.kind === "spot") {
			return s.at;
		}
		if (s.kind === "fly") {
			return s.to;
		}
		return this.man(s.pid)?.posAt(t);
	}

	// Once it is over: the man brought down helped up off the turf by a
	// teammate - now and then by the man who put him there - the ball let go
	// of, and everyone not in a moment of his own on his way: back toward
	// his huddle, a few yards off the ball - or, after a kick, a score or a
	// change of hands, off toward his sideline for the next unit to come on.
	regroup() {
		const r = this.r;
		const { kind, offense, dir, events, i0, i1, winded = 0 } = this.input;
		const t = this.whistle;
		const spot = this.ballAt(t);
		if (!spot) {
			return;
		}
		const last = this.ball.at(-1);
		const holder = last?.kind === "held" ? this.man(last.pid) : undefined;
		let scored = false;
		for (let i = i0; i < i1; i++) {
			if (events[i]?.td === true) {
				scored = true;
			}
		}
		const downs =
			this.input.stopped === "fourth" ||
			(this.input.stopped === "third" && this.input.kicksNext === true);
		const off =
			KICK_KINDS.has(kind) ||
			scored ||
			downs ||
			(holder !== undefined && holder.team !== offense);
		// (The side scored on, that had the ball taken off it, gave it up on
		// downs or was stopped on third with a kick next: slow off the field,
		// heads down.)
		// (Scored: whoever had it last, whatever he did with it after.)
		const scorer =
			holder ??
			(scored
				? this.man(
						this.ball.findLast(
							(b): b is Extract<BallSeg, { kind: "held" }> => b.kind === "held",
						)?.pid,
					)
				: undefined);
		const won = downs
			? ((1 - offense) as Side)
			: scored || (!KICK_KINDS.has(kind) && holder?.team !== offense)
				? scorer?.team
				: undefined;
		const y = clamp(spot.y, HUDDLE_Y[0], HUDDLE_Y[1]);
		const huddle = (team: Side): Pt =>
			team === offense
				? { x: spot.x - dir * HUDDLE_BACK, y }
				: { x: spot.x + dir * DEFENSE_BACK, y };
		// (When each is done with what the play gave him: up off the turf, or
		// pulled up.)
		const freeOf = (m: Actor) => {
			const legs = m.legs;
			let k = legs.length - 1;
			while (
				k > 0 &&
				legs[k]!.move === "stand" &&
				legs[k]!.from.x === legs[k]!.to.x &&
				legs[k]!.from.y === legs[k]!.to.y
			) {
				k--;
			}
			return Math.max(t, legs[k]!.t1, this.busy.get(m.pid) ?? 0);
		};
		const free = new Map<number, number>();
		for (const m of this.men) {
			free.set(m.pid, freeOf(m));
		}
		// The man down with the ball, a hand up.
		const down = holder ?? undefined;
		const up = down?.legs.find(
			(l) => (l.move === "getUp" || l.move === "getUpBack") && l.t0 >= t,
		);
		if (down && up && !this.mob.has(down.pid) && r() < HELP_UP) {
			const at = down.posAt(up.t0);
			const mates = (team: Side) =>
				this.men
					.filter(
						(m) =>
							m !== down &&
							m.team === team &&
							!this.mob.has(m.pid) &&
							!DOWN.has(legAt(m, up.t0).move) &&
							free.get(m.pid)! < up.t0 - 600,
					)
					.sort((a, b) => dist(a.posAt(up.t0), at) - dist(b.posAt(up.t0), at));
			const helper = (
				r() < HELP_FOE ? mates(down.team === 0 ? 1 : 0) : mates(down.team)
			)[0];
			if (helper) {
				const from = helper.posAt(free.get(helper.pid)!);
				// (Beside him at his chest, on the side he comes from - not
				// stood on him, come round from his head or his feet: he lies
				// a man's length along the way he faces, his head that way
				// down on his front, the other way on his back. Nor on any
				// other man down there, nor where a man on his feet is - one
				// up off the pile, say: pushed aside from there as he helps,
				// slid along the turf. Taken up there, at his hips, at his
				// head; or round the other side, if the way there is clear of
				// them - and him; or no hand up.)
				const yaw = yawThen(down, up.t0);
				const u = { x: Math.cos(yaw), y: Math.sin(yaw) };
				const n = { x: -u.y, y: u.x };
				const head = up.move === "getUpBack" ? -1 : 1;
				const chest = {
					x: at.x + u.x * head * HELP_CHEST,
					y: at.y + u.y * head * HELP_CHEST,
				};
				const near =
					(from.x - chest.x) * n.x + (from.y - chest.y) * n.y >= 0 ? 1 : -1;
				const others = this.men.filter((m) => m !== down && m !== helper);
				const lies = (m: Actor, t: number) => {
					const move = legAt(m, t).move;
					return LIE_FACING.has(move) || LIE_BACKWARD.has(move);
				};
				const when = [up.t0 - 80, (up.t0 + up.t1) / 2, up.t1];
				const spot = (along: number, side: number): Pt => ({
					x: at.x + u.x * head * along + n.x * side,
					y: at.y + u.y * head * along + n.y * side,
				});
				const roomAt = (p: Pt) =>
					others.every((m) =>
						when.every(
							(t) =>
								this.nearOf(m, t, p, HELP_LEGS) >=
								(lies(m, t) ? HELP_CLEAR : HELP_ROOM),
						),
					);
				const wayClear = (p: Pt) => {
					const steps = Math.ceil(dist(from, p));
					for (let k = 1; k < steps; k++) {
						const q = {
							x: from.x + ((p.x - from.x) * k) / steps,
							y: from.y + ((p.y - from.y) * k) / steps,
						};
						// (Clear of where he is already.)
						if (
							dist(q, from) >= HELP_PATH &&
							[down, ...others].some(
								(m) =>
									lies(m, up.t0 - 1) &&
									this.nearOf(m, up.t0 - 1, q) < HELP_PATH,
							)
						) {
							return false;
						}
					}
					return true;
				};
				// (Where he stands for it: where his hand meets the one held
				// up to him - see handUp - or, no room there, just over beside
				// him with it held down to him. A step or so over taken, not
				// hopped.)
				const back = up.move === "getUpBack";
				const sides = [
					[HELP_CHEST, near],
					[0, near],
					[HELP_HEAD, 0],
					[HELP_CHEST, -near],
					[0, -near],
				] as const;
				const ways: { at: Pt; face: number; left?: boolean }[] = [
					...sides.map(([along, side]) =>
						this.handUp(
							at,
							yaw,
							back,
							spot(along, side * HELP_BESIDE),
							(side || near) < 0,
						),
					),
					...sides.map(([along, side]) => {
						const p = spot(along, side * HELP_BESIDE);
						return { at: p, face: this.toward(p, chest) };
					}),
				];
				const leaveFor = (p: Pt) =>
					up.t0 -
					80 -
					Math.max(HELP_STEP_MS, (dist(from, p) / HELP_FPS) * 1000);
				const by =
					dist(from, at) < HELP_FROM
						? ways.find(
								(h) =>
									roomAt(h.at) &&
									wayClear(h.at) &&
									leaveFor(h.at) >= free.get(helper.pid)!,
							)
						: undefined;
				if (by) {
					const leave = leaveFor(by.at);
					helper.cut(leave);
					helper.hold(leave);
					helper.go(by.at, up.t0 - 80, undefined, by.face);
					helper.act("helpUp", up.t1, by.face);
					free.set(helper.pid, helper.t);
					if (by.left !== undefined) {
						down.arms(handUpOf(back, by.left), up.t0, up.t1);
					}
				}
			}
		}
		// The side it went for, a hand slapped or two on the way: a score, the
		// ball taken off the other side, a long way gained, a sack.
		const gain = num(
			events
				.slice(i0, i1)
				.find((e) => e?.type === "run" || e?.type === "passComplete")?.yds,
			0,
		);
		// (A kick through the posts: its side; wide, the other.)
		const goal =
			kind === "fieldGoal" || kind === "extraPoint" ? this.find(kind) : -1;
		const cheered =
			won ??
			(goal >= 0
				? this.ev(goal).made === true
					? offense
					: ((1 - offense) as Side)
				: gain >= FIVE_GAIN
					? offense
					: this.find("sack") >= 0
						? ((1 - offense) as Side)
						: undefined);
		if (cheered !== undefined) {
			this.fives(cheered, free, holder);
		}
		let toss: { t: number; yaw: number } | undefined;
		// (Picked off: the man who threw it.)
		const passer =
			this.find("interception") >= 0
				? this.o.find((m) => m.role === "QB")
				: undefined;
		// (Where along his sideline each of those off so far goes.)
		const offXs: number[][] = [[], []];
		for (const m of this.men) {
			if (this.mob.has(m.pid)) {
				continue;
			}
			// (Worn out there a long drive: bent over where he is getting his
			// breath as soon as he pulls up, and back slower, hands on his
			// hips as often as not - stood blowing once there.)
			const worn = !off && m.team !== offense && winded > 0 && m !== holder;
			const bent = worn && r() < winded * WINDED_BENT;
			let start = free.get(m.pid)! + (bent ? 100 + r() * 300 : 250 + r() * 900);
			const tosses = m === holder && last !== undefined && last.t0 <= start;
			if (tosses) {
				start = Math.max(
					start,
					free.get(m.pid)! + TOSS_HANDS_MS + TOSS_BEFORE_MS,
				);
			}
			m.cut(start);
			m.hold(start);
			// (The ball let go of: tossed down by his feet - once everybody's
			// way off is known.)
			// (Before he is off - still facing the way he was, not coming
			// round for where he goes: see TOSS_BEFORE_MS.)
			if (tosses) {
				toss = {
					t: Math.max(last.t0, free.get(m.pid)!, start - TOSS_BEFORE_MS),
					yaw: m.yaw,
				};
			}
			if (bent) {
				m.hold(m.t + WINDED_BENT_MS + r() * 800, "winded");
			}
			// (Off toward his own bench, back up the field if it is past it -
			// OFF_APART_FT along from any of the others off there: not on to
			// a man stood on the spot.)
			const going = off
				? {
						x: offSpot(
							offXs[m.team]!,
							clamp(m.at.x, TEAM_AREA[0], TEAM_AREA[1]) + (r() - 0.5) * 24,
						),
						y: sidelineOf(m.team) === 0 ? 3 : FIELD_W - 3,
					}
				: huddle(m.team);
			const ang = r() * Math.PI * 2;
			const ring = off ? 0 : 2 + r() * 5;
			const to = {
				x: going.x + Math.cos(ang) * ring,
				y: going.y + Math.sin(ang) * ring,
			};
			const big = OL.has(m.role) || DL.has(m.role);
			const beaten = won !== undefined && m.team !== won;
			const glad = won !== undefined && m.team === won;
			if (beaten && m === passer && r() < PICK_QB) {
				// (Picked off: the quarterback as a rule his own way with it,
				// more often than not the same way each time - see
				// PICK_QB_LOOK - once the other side's moment with it is over:
				// the picture may come in on him for it, see the director's
				// qbRue.)
				if (this.until > 0) {
					m.hold(Math.max(m.t, this.until + PICK_QB_AFTER_MS));
				}
				const t0 = m.t;
				const u =
					(2 * countOf(m, 0, 134) + countOf(m, this.input.key, 135)) / 3;
				const meant = this.man(this.statPids("tgt")[0]);
				if (
					u >= PICK_QB_LOOK[1] &&
					meant &&
					meant !== m &&
					dist(meant.posAt(m.t), m.at) >= PICK_QB_AT_FT
				) {
					const at = this.toward(m.at, meant.posAt(m.t));
					m.hold(m.t + PICK_QB_TURN_MS, "stand", at);
					m.act("protest", m.t + PICK_QB_AT_MS, at);
				} else if (u >= PICK_QB_LOOK[0]) {
					m.hold(m.t + PICK_QB_BENT_MS * (0.8 + 0.4 * u), "winded");
				} else {
					m.act("handsOnHelmet", m.t + RUE_HELMET_MS + r() * 400);
				}
				this.qbRue = { pid: m.pid, t0, t1: m.t };
			} else if (beaten && r() < (m.role === "QB" ? RUE_QB : RUE_HELMET)) {
				m.act("handsOnHelmet", m.t + RUE_HELMET_MS + r() * 400);
			} else if (glad && r() < GLAD) {
				m.act(r() < 0.5 ? "fistPump" : "clap", m.t + 1200 + r() * 300);
			}
			const fps = beaten
				? (big ? 2.7 : 3.2) + r() * 0.9
				: off && r() < (glad ? 0.85 : 0.6)
					? 6.8 + r() * 2
					: (big ? 3 : 3.6) + r() * 1.1;
			if (dist(m.at, to) > 2) {
				const from = m.t;
				// (Round any man still down on the way, too - one down at his
				// feet as well, stepped off first (see offMenDown): not back
				// across him after.)
				const way = this.mobAt
					? skirt(m.at, to, this.mobAt)
					: this.wayRound(m.at, to, m.t, [m]);
				const pace = worn ? fps * (1 - WINDED_SLOWER * winded) : fps;
				way.forEach((p, k) => {
					if (k === way.length - 1 && pace >= 7) {
						m.runUp(p, pace);
					} else {
						m.run(p, pace);
					}
				});
				if (beaten && r() < RUE_HIPS) {
					m.arms("onHips", from, m.t);
				} else if (worn && r() < winded * WINDED_HIPS) {
					m.arms("onHips", from, m.t);
					m.hold(m.t + WINDED_BENT_MS, "blowing");
				}
			}
		}
		// (Tossed out the way he faces, or round from it as far as it takes
		// to miss anybody by him then.)
		if (holder && toss) {
			const tossAt = toss.t;
			const way = this.tossWay(holder, tossAt, toss.yaw);
			const from = { ...way.from, z: 3.2 };
			const to = { ...way.to, z: 0.3 };
			this.ball.push(
				{
					kind: "fly",
					t0: tossAt,
					t1: tossAt + TOSS_MS,
					from,
					to,
					peak: 1.2,
					down: true,
				},
				{ kind: "spot", t0: tossAt + TOSS_MS, at: { ...to, z: 0 } },
			);
		}
	}

	// Two of a side near each other once done with the play (not the man
	// with the ball, not in a crowd round somebody), now and then: over to
	// each other, a high five, and on (each free from the end of it).
	fives(team: Side, free: Map<number, number>, holder: Actor | undefined) {
		const r = this.r;
		const left = this.men.filter((m) => {
			const t = free.get(m.pid)!;
			return (
				m.team === team &&
				m !== holder &&
				!this.mob.has(m.pid) &&
				t < this.whistle + FIVE_LATE &&
				!DOWN.has(legAt(m, t).move)
			);
		});
		let pairs = 0;
		while (left.length > 1 && pairs < FIVE_MOST) {
			const a = left.splice(Math.floor(r() * left.length), 1)[0]!;
			if (r() > FIVE_WANT) {
				continue;
			}
			const pa = a.posAt(free.get(a.pid)!);
			// (Not over to each other across a man down between them.)
			const across = (b: Actor) => {
				const from = Math.max(free.get(a.pid)!, free.get(b.pid)!);
				const pb = b.posAt(free.get(b.pid)!);
				return this.men.some((o) =>
					FIVE_DOWN_LOOKS.some((after) => {
						const c = from + after;
						if (o === a || o === b || !DOWN.has(legAt(o, c).move)) {
							return false;
						}
						const at = o.posAt(c);
						const yaw = yawThen(o, c);
						const ux = Math.cos(yaw) * LIE_HEAD;
						const uy = Math.sin(yaw) * LIE_HEAD;
						return (
							segGap(
								pa,
								pb,
								{ x: at.x - ux, y: at.y - uy },
								{ x: at.x + ux, y: at.y + uy },
							) < DOWN_CLEAR
						);
					}),
				);
			};
			let k = -1;
			for (let j = 0; j < left.length; j++) {
				const d = dist(pa, left[j]!.posAt(free.get(left[j]!.pid)!));
				if (
					d < FIVE_NEAR &&
					!across(left[j]!) &&
					(k < 0 || d < dist(pa, left[k]!.posAt(free.get(left[k]!.pid)!)))
				) {
					k = j;
				}
			}
			if (k < 0) {
				continue;
			}
			const b = left.splice(k, 1)[0]!;
			this.highFive(
				a,
				b,
				Math.max(free.get(a.pid)!, free.get(b.pid)!) + 200 + r() * 400,
			);
			free.set(a.pid, a.t);
			free.set(b.pid, b.t);
			pairs++;
		}
	}

	// The holder up and over to the kicker after a miss, from `go`: in
	// behind him on the side he is already on, a little turned in to him,
	// his near hand out onto the kicker's shoulder (see PAT_ON) - a pat or
	// two on it - and off it. The kicker stood where he is from `go`, facing
	// `look`.
	holderPat(h: Actor, k: Actor, go: number, look: number) {
		const at = k.posAt(go);
		const fx = Math.cos(look);
		const fy = Math.sin(look);
		// (His left: see bodyPoint.)
		const side = (h.at.x - at.x) * fy - (h.at.y - at.y) * fx >= 0 ? 1 : -1;
		const yaw = look + side * PAT_TURN;
		const on = {
			x: at.x + fx * PAT_ON.f + fy * side * PAT_ON.s,
			y: at.y + fy * PAT_ON.f - fx * side * PAT_ON.s,
		};
		const hf = PAT_HAND.f;
		const hs = PAT_HAND.s * side;
		const stand = {
			x: on.x - (Math.cos(yaw) * hf + Math.sin(yaw) * hs),
			y: on.y - (Math.sin(yaw) * hf - Math.cos(yaw) * hs),
		};
		h.cut(go);
		h.hold(go);
		if ((dist(h.at, stand) / PAT_FPS) * 1000 < FIVE_STEP_MS) {
			h.go(stand, h.t + FIVE_STEP_MS, undefined, yaw);
		} else {
			h.run(stand, PAT_FPS);
		}
		h.hold(h.t + PAT_SET_MS, "stand", yaw);
		h.act(side > 0 ? "pat" : "patL", h.t + PAT_MS, yaw);
		h.hold(h.t + PAT_AFTER_MS, "stand", yaw);
	}

	// Two men, from `go`, over to each other at FIVE_FPS (to meet halfway)
	// and a high five.
	highFive(a: Actor, b: Actor, go: number) {
		for (const m of [a, b]) {
			m.cut(go);
			m.hold(go);
		}
		const mid = { x: (a.at.x + b.at.x) / 2, y: (a.at.y + b.at.y) / 2 };
		const way = sideOn(
			dist(a.at, b.at) > 0.5 ? this.toward(a.at, b.at) : this.r() * 6.28,
		);
		const half = FIVE.apart / 2;
		const ux = Math.cos(way) * half;
		const uy = Math.sin(way) * half;
		// (A step or so across: taken, not hopped - no quicker than
		// FIVE_STEP_MS. Further: walked over the way he goes, turned to him
		// once there - not side on all the way.)
		const over = (m: Actor, to: Pt, yaw: number) => {
			if ((dist(m.at, to) / FIVE_FPS) * 1000 < FIVE_STEP_MS) {
				m.go(to, m.t + FIVE_STEP_MS, undefined, yaw);
			} else {
				m.run(to, FIVE_FPS);
			}
		};
		over(a, { x: mid.x - ux, y: mid.y - uy }, way);
		over(b, { x: mid.x + ux, y: mid.y + uy }, way + Math.PI);
		const t = Math.max(a.t, b.t) + FIVE_SET_MS;
		a.hold(t, "stand", way - FIVE.turn);
		b.hold(t, "stand", way + Math.PI - FIVE.turn);
		a.act("highFive", t + FIVE_MS, way - FIVE.turn);
		b.act("highFive", t + FIVE_MS, way + Math.PI - FIVE.turn);
	}
}

// How near the segments a-b and c-d come to each other.
const segGap = (a: Pt, b: Pt, c: Pt, d: Pt): number => {
	const cross = (o: Pt, p: Pt, q: Pt) =>
		(p.x - o.x) * (q.y - o.y) - (p.y - o.y) * (q.x - o.x);
	const d1 = cross(a, b, c);
	const d2 = cross(a, b, d);
	const d3 = cross(c, d, a);
	const d4 = cross(c, d, b);
	if (d1 * d2 < 0 && d3 * d4 < 0) {
		return 0;
	}
	const toSeg = (p: Pt, q: Pt, r: Pt) => {
		const dx = r.x - q.x;
		const dy = r.y - q.y;
		const len2 = dx * dx + dy * dy;
		const u =
			len2 > 0 ? clamp(((p.x - q.x) * dx + (p.y - q.y) * dy) / len2, 0, 1) : 0;
		return dist(p, { x: q.x + dx * u, y: q.y + dy * u });
	};
	return Math.min(
		toSeg(a, c, d),
		toSeg(b, c, d),
		toSeg(c, a, b),
		toSeg(d, a, b),
	);
};

// The way from a to b round a crowd at c, as far off it as SKIRT: straight
// there, or by a point off to the side of it.
const skirt = (a: Pt, b: Pt, c: Pt): Pt[] => {
	const dx = b.x - a.x;
	const dy = b.y - a.y;
	const len = Math.hypot(dx, dy);
	if (len < 1) {
		return [b];
	}
	const u = clamp(((c.x - a.x) * dx + (c.y - a.y) * dy) / (len * len), 0, 1);
	const q = { x: a.x + dx * u, y: a.y + dy * u };
	if (dist(a, c) >= SKIRT && (dist(q, c) >= SKIRT || u <= 0 || u >= 1)) {
		return [b];
	}
	// (Round it on the side he is going by - or that he is on.)
	let nx = -dy / len;
	let ny = dx / len;
	const by = dist(q, c) > 0.5 ? q : a;
	if ((by.x - c.x) * nx + (by.y - c.y) * ny < 0) {
		nx = -nx;
		ny = -ny;
	}
	return [{ x: c.x + nx * SKIRT, y: c.y + ny * SKIRT }, b];
};

// A man's own count on a play, 0 to 1 - the same each time it is asked
// for (not off the play's dice, so nothing else in the play changes with
// it), by the play's own number (see ActInput's key: not its time, so not
// another play's for anything earlier in the game going a moment longer);
// `salt` for another of his.
const countOf = (m: Actor, key: number, salt = 0): number => {
	let h =
		Math.imul(m.pid + 0x9e37, 0x85ebca6b) ^
		Math.imul(((key * 2147483647) | 0) + salt * 7919, 0xc2b2ae35);
	h ^= h >>> 16;
	h = Math.imul(h, 0x7feb352d);
	h ^= h >>> 15;
	h = Math.imul(h, 0x846ca68b);
	h ^= h >>> 16;
	return (h >>> 0) / 4294967296;
};

// The leg he is in at t (or the last).
const legAt = (m: Actor, t: number): Leg =>
	m.legs[Math.max(0, legIndexAt(m.legs, t))]!;

// The index of the last of `legs` begun by t (-1: none of them) - they go
// in time order.
const legIndexAt = (legs: readonly Leg[], t: number): number => {
	let lo = 0;
	let hi = legs.length;
	while (lo < hi) {
		const mid = (lo + hi) >> 1;
		if (legs[mid]!.t0 <= t) {
			lo = mid + 1;
		} else {
			hi = mid;
		}
	}
	return lo - 1;
};

// Down on the turf until `until` at least: the last time he lies there made
// longer, and what he does after it later by as much.
const stayDown = (m: Actor, until: number) => {
	const legs = m.legs;
	const k = legs.findLastIndex(
		(l) => l.move === "prone" || l.move === "supine",
	);
	const l = legs[k];
	if (!l || l.t1 >= until) {
		return;
	}
	const more = until - l.t1;
	l.t1 = until;
	for (const x of legs.slice(k + 1)) {
		x.t0 += more;
		x.t1 += more;
	}
};

// A touchdown's arms up (see tdArms): the passer this often, any other man
// of his away from the mob this often; from this long after the score
// (and up to this much more), for this long (and up to this much more) -
// for at least TD_ARMS_LEAST of it, or not at all.
const TD_ARMS_QB = 0.8;
const TD_ARMS = 0.3;
const TD_ARMS_REACT = [250, 400] as const;
const TD_ARMS_MS = [1300, 600] as const;
const TD_ARMS_LEAST = 700;
// After a score: the scorer's moment, then his teammates' around him -
// at the most this many of them, from no further than this away.
const MOB = 4;
const MOB_FROM = 75;
// How far off the scorer each of them pulls up (feet) - no two nearer each
// other round him than this (radians) - and a ball spiked:
// down into the turf this long after he starts, and up off it.
const MOB_RING = 3.2;
const MOB_GAP = 0.85;
// (Clear of a man down there - looked for this long after they are off,
// and after he is - by this much, feet.)
const MOB_LIE_AT = [400, 1200, 2000];
const MOB_ROOM = 1.8;
// (Down this near him: in on it once up.)
const MOB_UP_NEAR = 12;
const SPIKE_MS = 720;
// Out to meet them (see meetUp): his first teammate no nearer than this
// (feet) - off this long after his moment (ms, and up to this much more),
// at this pace - no further than this, and not for less.
const MEET_FROM = 55;
const MEET_AFTER = [200, 400] as const;
const MEET_FPS = 9;
const MEET_MOST = 30;
const MEET_LEAST = 6;
// The griddy this long.
const GRIDDY_MS = 2400;
// Bowling, this often (with three teammates no further than this off): the
// pins this far down the end zone and across (feet), no nearer the end line
// than this, set no sooner than this after he is in; the roll this long, let
// go this far through it, along the turf at this pace; the pins behind the
// head pin down this long after him.
const BOWL = 0.2;
// A finger back at the man who threw it (see celebrate), this often, this
// long (ms).
const THANKS = 0.35;
const THANKS_MS = 1000;
const PIN_FROM = 40;
const PIN_SPOTS = [
	{ f: 13, s: 0 },
	{ f: 15.5, s: -1.7 },
	{ f: 15.5, s: 1.7 },
];
const PIN_IN = 2;
const PIN_SET = 900;
const BOWL_MS = 1000;
const PIN_BOWL_AT = 0.68;
const ROLL_FPS = 26;
const PIN_AFTER = 110;
// The pins down this long; and them in round him no later than this after
// the score - that this long after they are set.
const PIN_DOWN_MS = 400;
const BOWL_MOST = 7300;
const BOWL_REST = 3600;
const SPIKE_AT = 0.6;
const BOUNCE_MS = 650;
// Peeling back off the line on a punt: turned round over this long, this
// far, before he is flat out.
const PEEL_TURN_MS = 400;
const PEEL_TURN_FT = 2;
// With this long (ms) to go to the whistle and the ball gone this far
// (feet) since, a man whose part is done after it - a moment later, to as
// near the man with it as this (a man of the other side's, this) - at
// these paces at the most (feet a second; this much more for the other
// side's, after him).
const FOLLOW_LEFT = 900;
const FOLLOW_GONE = 15;
const FOLLOW_REACT = [150, 350] as const;
// (Running on at the whistle, nothing more to do: pulled up this long
// after it, ms, give or take.)
const WHISTLE_SEEN_MS = [250, 300] as const;
// Stood watching (from so long after the snap): looked again every so
// often, ms, and turned to it if it is more than so far round (radians) -
// not if it is right by him, ft.
const WATCH_FROM_MS = 400;
const WATCH_STEP_MS = 450;
const WATCH_TURN = 0.6;
const WATCH_NEAR_FT = 6;
// (Pulled up from the step before rather than this little way into one.)
// (A cut he had begun no more than this long (ms) before the whistle -
// see Action.roundBegun.)
const WHISTLE_CUT_MS = 300;
// (Only one begun at this pace (ft/s) or more - slowed into, it is a
// plant, cut in a stride as it was.)
const WHISTLE_CUT_PACE = 14;
const WHISTLE_SLIVER_MS = 150;
// (Not with less time than this, ms, to go after it.)
const FOLLOW_LEAST_MS = 400;
const FOLLOW_KEEP = [9, 8] as const;
const FOLLOW_CLOSE = [5, 6] as const;
const FOLLOW_FPS = { line: 8, qb: 6.5, other: 12, after: 3 };
// No two of them nearer than this (feet) when it is over: one in from a
// little to the side (radians), if so.
const FOLLOW_APART = 3.5;
// (Stood this long (ms) or more with the play still going: turned to the
// ball as it goes, a look every so often (WATCH_IDLE_EVERY_MS, from the
// least to that much more, his own count) - round as far as
// WATCH_IDLE_MOST at a time, if it is WATCH_IDLE_LEAST or more off the way
// he faces - not stood the while like a statue faced where his last step
// left him.)
const WATCH_IDLE_LEAST_MS = 2500;
const WATCH_IDLE_EVERY_MS = [700, 800] as const;
const WATCH_IDLE_LEAST = 0.35;
const WATCH_IDLE_MOST = 1.4;
const FOLLOW_TURNS = [0, 0.45, -0.45, 0.9, -0.9];
// (Nor to, or by, a man down then any nearer than this, feet.)
const FOLLOW_DOWN_CLEAR = 2.5;
// Between plays: the offense's huddle this far back of the ball, the
// defense gathering this far off it, between these (across the field); a
// man down with the ball helped up this often - by the other side's man
// this often - by a man no further off than this, getting to him at this
// pace; and the ball tossed down this long.
const HUDDLE_BACK = 8 * YD;
const DEFENSE_BACK = 5 * YD;
const HUDDLE_Y: [number, number] = [MID_Y - 30, MID_Y + 30];
const HELP_UP = 0.6;
const HELP_FOE = 0.25;
const HELP_FROM = 22;
const HELP_FPS = 6;
// (No quicker over to him than this, ms.)
const HELP_STEP_MS = 450;
// (His chest this far along him from where he is, feet - see skeleton's
// prone and supine; the helper this far out beside it.)
const HELP_CHEST = 1.8;
// (Further than this to go, feet, to be walked there turned the way he
// goes rather than backward.)
const HELP_TURN_FT = 1.5;
// (Round where he stands first, this long, ms.)
const HELP_PIVOT_MS = 300;
const HELP_BESIDE = 2;
// (Or this far along him, past his head; and on the way there no nearer
// any man down than this.)
const HELP_HEAD = 4.4;
const HELP_PATH = 1.1;
// (Clear by this much of any other man down there, feet - the length of
// him, see nearOf, his legs this far back of his hips.)
const HELP_CLEAR = 1.5;
const HELP_LEGS = 3.2;
// (And this far from any man on his feet, feet - see apart.ts APART.)
const HELP_ROOM = 2.2;
const TOSS_MS = 380;
// (Round from the way he faces, radians, to toss it clear of anybody - no
// nearer the ball's way than this, feet, past this share of it - from his
// hands held out this far in front of him to this far out.)
const TOSS_TURNS = [0, 0.6, -0.6, 1.2, -1.2, 1.7, -1.7];
const TOSS_CLEAR = 1.5;
const TOSS_PAST = 0.3;
// (Nobody over it where it lands these long after (ms), either.)
const TOSS_LATER = [300, 700, 1100, 1500];
const TOSS_HANDS = 2;
const TOSS_OUT = 4;
// (So long before he is off after it, ms - he is round for it a little
// before he goes: see pace.ts GO_MS.)
const TOSS_BEFORE_MS = 250;
// (Out in his hands this long first, at least, ms - not flicked out from
// under his arm and gone in a frame.)
const TOSS_HANDS_MS = 220;
// The man who got home on the quarterback off his block: a swim this
// often, else a rip (see rushMove) - from this long before he is off it to
// this long after, at the most (no less than this long of it, ms); only off
// a man no further from him than this (feet), and no sooner after the snap
// than this.
const RUSH_SWIM = 0.6;
const RUSH_MOVE_BEFORE = 150;
const RUSH_MOVE_AFTER = 400;
const RUSH_MOVE_LEAST_MS = 380;
const RUSH_MOVE_NEAR = 6;
const RUSH_MOVE_LEAST = 700;
// (A spin instead, this often - with this long left to run on after it at
// least, ms, and going this far in it, feet.)
const RUSH_SPIN = 0.3;
const RUSH_SPIN_AFTER = 150;
const RUSH_SPIN_LEAST_FT = 2;
// A man hurt with no part in it the sim names: pulled up lame no later
// than this long before the whistle (ms), nor sooner than this long after
// the snap, going all out (at least this quick, ft/s) at least this far
// off the ball (feet) - pulled up within this far (feet), down over this
// long (ms), on his back this often, else on his front; lying there till
// this long past the whistle at least (see hurt.ts).
const HURT_LAME_LEAD_MS = 900;
const HURT_LAME_FROM_MS = 800;
const HURT_LAME_FPS = 13;
const HURT_LAME_OFF_FT = 12;
const HURT_LAME_PULL_FT = 5;
const HURT_LAME_FALL_MS = 500;
const HURT_LAME_BACK = 0.5;
const HURT_LAME_PAST_MS = 500;
// Or in a block - in it, a man of the other side no further off than this
// (feet), no later than this long before the whistle nor sooner than this
// long after the snap (ms): down there, this far back (feet).
const HURT_BLOCKS = new Set<Move>(["engage", "drive", "passSet"]);
const HURT_BLOCK_NEAR = 3.5;
const HURT_BLOCK_LEAD_MS = 700;
const HURT_BLOCK_FROM_MS = 700;
const HURT_BLOCK_BACK_FT = 1.2;
// The quarterback hit as he throws: this often (on a pass from the
// pocket, caught or not), this long after it leaves his hand (ms, from the
// first to the first and second; flagged for it, late, this long after);
// the man who hits him off his block no sooner than this long before
// that, nor more than this long before the throw (ms) - from no further
// than this off (feet) - at no more than this
// share of his top speed, into him from this far off him (feet), and on
// through it this far over this long (in a run from this quick on, ft/s;
// off his block at this pace, pushing on already); the shove from this
// long before it to this long after; stood over him till this long after
// it. The quarterback knocked this far (feet) over this long (onto his
// back unless that turns him this much more, radians), down this long
// (hurt on it, as long as he is hurt: no less than this).
// The blocker he got off: no further from him than this (feet), seeing it
// this long after (ms).
const QB_HIT = 0.1;
const QB_HIT_AFTER_MS = [120, 140] as const;
const QB_HIT_LATE_MS = 300;
const QB_HIT_LEAST_MS = 350;
const QB_HIT_EARLY_MS = 700;
const QB_HIT_FAR = 14;
const QB_HIT_PACE = 0.85;
const QB_HIT_ON = 1.9;
const QB_HIT_THROUGH_FT = 1.6;
const QB_HIT_THROUGH_MS = 260;
// (Into him quicker than that takes him: eased off to a stand no harder
// than this, ft/s/s, in this many steps - see hitThrough.)
const QB_HIT_STOP_ACCEL = 60;
const QB_HIT_THROUGH_STEPS = 5;
// (On through past him off to one side, radians off straight at him - the
// far side of him from the cameras - not on through onto his legs as he
// lies.)
const QB_HIT_PEEL = 1;
const QB_HIT_RUN_FPS = 6;
const QB_HIT_FROM_FPS = 4;
const QB_HIT_SHOVE_MS = [220, 240] as const;
const QB_HIT_STAND_MS = 1300;
const QB_HIT_BACK_FT = 2.4;
const QB_HIT_FALL_MS = 450;
const QB_HIT_BACK_BIAS = 0.35;
const QB_HIT_DOWN_MS = 700;
const QB_HURT_DOWN_MS = 60_000;
const QB_HIT_BLOCKER_FT = 5;
// (Off the man he was on round his shoulder, no nearer him than this
// (feet), if his way in to the quarterback goes through him - not run
// through him.)
const QB_HIT_ROUND_FT = 2.4;
const QB_HIT_ROUND_PTS = 12;
// (Coming in onto him no more than this far round (radians) from the way
// he would have straight; not round at all if that leaves less than this
// far (feet) out to the side to go.)
const QB_HIT_ROUND_END = 0.7;
const QB_HIT_ROUND_LEAST = 0.4;
// (His way in to the quarterback no nearer than this to any other man
// blocking (feet), past the first QB_HIT_CLEAR_FROM of it.)
const QB_HIT_CLEAR_FT = 1.6;
const QB_HIT_CLEAR_FROM = 1.5;
const QB_HIT_SEE_MS = 200;
// A pass knocked down at the line (see batAt): by the man in the rush the
// sim says got to it - or, this often with nobody named on it, one in the
// way of it anyway - this far in front of the quarterback (feet), no
// further than this to the side of the lane it goes up (feet; the man the
// sim names, this far - a step across into it, at this pace, ft/s, no
// quicker than this, ms), where it goes by no higher than he gets his
// hands (feet up). His hands this far up stood (feet) - more off the
// turf, see the leap's lift (LEAP_UP, at most, at so far through it); off
// both feet over this long (ms), and no nearer anybody than this (feet)
// as he goes up. The man on him seeing he is off it this long after (ms).
// The ball: off his hands up this high (a fly's peak, feet, from one to
// the other) and down this far off (feet, the most of it in the air), the
// way back to the quarterback give or take this much (radians). The rest
// seeing it go up this long after (ms, give or take).
const LINE_BAT = 0.2;
const LINE_BAT_ALONG = [4, 16] as const;
const LINE_BAT_LANE_FT = 2;
const LINE_BAT_STEP_FT = 4;
const LINE_BAT_SIDE_FT = 0.5;
const LINE_BAT_STEP_FPS = 9;
const LINE_BAT_STEP_MS = 250;
// (Less time than this left, pulled up, to step across into the lane: up
// from where he stops.)
const LINE_BAT_STEP_LEAST_MS = 120;
// (Pulled up for it: how much sooner each look back - see batAt.)
const LINE_BAT_SEEK_MS = 20;
const LINE_BAT_TOP_Z = 9.8;
const LINE_BAT_HANDS_Z = 8.3;
const LEAP_UP = 1.7;
const LEAP_UP_FROM = 0.3;
const LEAP_UP_SPAN = 0.52;
const LINE_BAT_LEAP_MS = 800;
const LINE_BAT_ROOM = 2.2;
const LINE_BAT_SEE_MS = 150;
const LINE_BAT_POP = [4, 9] as const;
const LINE_BAT_FT = [6, 16] as const;
const LINE_BAT_AIR = 0.75;
const LINE_BAT_TURN = 1.2;
const LINE_BAT_SEEN_MS = [350, 300] as const;
// Helped up after it this often (see HELP_UP) - the man to do it off
// after him this long after he is through it (ms), him down no longer for
// it than this (ms), one of his line no further off than this (ft) to be
// the one.
const QB_HAND_UP = 0.5;
const QB_HAND_SEE_MS = 250;
const QB_HAND_WAIT_MS = 2000;
const QB_HAND_MATE_FT = 12;
// Taunting (see taunt): from this long after the whistle (no later than
// this, and there no later than this), at a man of the other side no further off than this (ft), over to
// him at this pace (ft/s, no quicker than this, ms) - this far from him on
// his feet (ft) - where he is, if no further off than this more (ft) - at
// it this long (ms), him round to it this long before.
const TAUNT_AFTER_MS = 200;
const TAUNT_LATE_MS = 1500;
const TAUNT_THERE_MS = 2200;
const TAUNT_FAR = 15;
const TAUNT_FPS = 7;
const TAUNT_STEP_MS = 400;
const TAUNT_FACE_FT = 2.6;
const TAUNT_STAY_FT = 1.5;
// (Round a man down - his head, or his hands out past it, this far past
// his chest, ft - by corners
// this far out past his ends and beside him, ft, the way there no nearer
// him than this, ft - or than where it starts or ends, less this, ft.)
const TAUNT_HEAD_FT = 1.8;
const TAUNT_ROUND_FT = 2.2;
const TAUNT_CLEAR_FT = 1.8;
const TAUNT_SLACK_FT = 0.3;
// (The far side of him, facing the camera, worth this much further to go,
// ft.)
const TAUNT_FAR_SIDE_FT = 7;
const TAUNT_MS = 1500;
const TAUNT_TURN_MS = 300;
// (Down on the turf to be stood over: lying there, or going down.)
const TAUNT_LIES = new Set<Move>([
	"prone",
	"supine",
	"tackled",
	"tackledBack",
	"dive",
]);
// The side it went for (a long way gained: this many yards): this often
// a man of it after a high five - with the nearest of them done with the
// play no later than this after the whistle (ms), no further off than
// this, coming to him at this pace - at the most this many pairs of them.
const FIVE_GAIN = 15;
const FIVE_WANT = 0.35;
const FIVE_LATE = 4000;
const FIVE_NEAR = 14;
const FIVE_FPS = 5;
// (A step or so to him, no quicker than this, ms.)
const FIVE_STEP_MS = 450;
const FIVE_MOST = 2;
// (Not paired across a man down between them as they go - looked for this
// long after the later of them is free, ms.)
const FIVE_DOWN_LOOKS = [0, 600, 1200];
// The side that lost it off the field: this often a man's hands to his
// helmet first (for this long, give or take - the quarterback's more often),
// this often his hands on his hips on the way; and round a crowd of the
// other side's, no nearer it than this.
const RUE_HELMET = 0.2;
const RUE_QB = 0.55;
const RUE_HELMET_MS = 1500;
const RUE_HIPS = 0.45;
// Picked off, the quarterback this often shows it his own way (his own
// count, two thirds of it the same every time): hands on his helmet under
// the first share, bent over with his hands on his knees under the second
// (this long, give or take a fifth, ms), else at the man it was meant for,
// his arms out - turned to him over this long first, ms, and at it so
// long.
const PICK_QB = 0.85;
const PICK_QB_LOOK = [0.4, 0.7] as const;
const PICK_QB_BENT_MS = 1700;
const PICK_QB_TURN_MS = 300;
const PICK_QB_AT_MS = 1300;
// (Not before this long after the other side's moment with it is over,
// ms; nor at the man it was meant for nearer him than this, feet - his
// hands to his helmet instead.)
const PICK_QB_AFTER_MS = 650;
const PICK_QB_AT_FT = 10;
// A catch for a score, or a pick: the man beaten on it - this near the man
// with it - this often at the officials for a flag, his arms out, for this
// long (ms).
const APPEAL_NEAR = 12;
const APPEAL = 0.4;
const APPEAL_MS = 1300;
const FACING_NEAR = Math.PI / 2;
const SKIRT = 11;
// ...and the side that got it: this often a fist pumped, or a clap, first.
const GLAD = 0.3;
// A defense worn all the way (see ActInput.winded): how many of it bent
// over getting their breath after the whistle (for this long, ms, and up
// to most of a second more), how many back to the huddle hands on hips,
// and how much slower.
const WINDED_BENT = 0.4;
const WINDED_BENT_MS = 1600;
const WINDED_HIPS = 0.6;
const WINDED_SLOWER = 0.2;
// Stopped short: this often a moment for it, from this long after the
// whistle at the soonest - his teammates no further off than this to him,
// at no more than this pace, to this far off him, or (this often, the first
// of them) chest to chest with him, this far apart, this long - or else
// (this often) a high five, this long.
const STOP_PARTY = 0.75;
const STOP_AFTER = 250;
// (The man who made it first this far off the ball, stepping away at this
// pace.)
const STOP_AWAY = 6;
const STOP_STEP = 7;
const STOP_NEAR = 30;
const STOP_FPS = 13;
const STOP_RING = 3.6;
// (There already, this near it, feet.)
const STOP_THERE_FT = 0.6;
// (No further than this, feet: stepped over to it at this pace, ft/s, over
// this long at the least, ms.)
const PARTY_STEP_FT = 3;
const PARTY_STEP_FPS = 3.5;
const PARTY_STEP_MS = 350;
const STOP_BUMP = 0.55;
const STOP_BUMP_APART = 2;
const STOP_BUMP_MS = 900;
const STOP_FIVE = 0.5;
// Getting under a kick: no faster than this (feet a second), he goes with
// his eyes on it - across (shuffling, the way he goes no nearer straight at
// or away from the kick than this, as a cosine - and no quicker than push
// steps go), or backpedaling. Further: run there this much quicker than he
// has to be, with the time for it, and pulled up.
const DRIFT_FPS = 9;
// (Not stepped up to speed and down again to go less than this far, feet.)
const STEP_TO_LEAST_FT = 2;
const DRIFT_ACROSS = 0.6;
const UNDER_EARLY = 1.25;
// (Stood under it at least this long before he reaches up for it, ms -
// off to it, a long way, as hard as this, ft/s/s.)
const UNDER_SETTLE_MS = 100;
const UNDER_OFF_ACCEL = RELEASE_ACCEL * RAMP_HARD;

// How far he gets from a stand in `ms` and is stood there at the end of
// it: up to a pace no quicker than `most` (ft/s) at `off` (ft/s/s), on at
// it and eased off it at STOP_ACCEL * STOP_HARD (as stepTo goes) - feet.
function setReach(ms: number, most: number, off: number): number {
	const T = Math.max(0, ms) / 1000;
	const per = 1 / (2 * off) + 1 / (2 * STOP_ACCEL * STOP_HARD);
	const v = Math.min(most, T / (2 * per));
	return v * T - v * v * per;
}

// A fair catch: his arm waved from this long before the ball comes down
// to this long before (ms).
const FAIR_FROM = 2000;
const FAIR_TO = 700;
// No return, and let go instead, this often (his own count): it comes down
// this far short of where it stops (feet, a range), bounds on and rolls
// there - over these long (ms) - him off to the side of it this far
// (feet), waving his men off it; the first of the cover over it, bent to
// it, downed.
// A punt (on the punter's own count - see countOf): aimed out toward the
// numbers this often, this far out from the middle of the field (feet).
// One coming down within POOCH_IN yards of the goal line, no more than
// POOCH_YDS, a pooch this often: up this long, end over end. Any other
// under SHANK_YDS shanked this often - low, this long in the air (ms), end
// over end - and any under SHANK_SURE_YDS, always: a ball barely past the
// line was never up there for four seconds and more.
const AIM_OUT = 0.45;
// A gunner held up at the line on a return of this many yards or more, this
// much longer (ms), the two of them this far on down the field over it
// (feet).
const JAM_HELD_YDS = 10;
const JAM_HELD_MS = [1300, 2100] as const;
const JAM_HELD_FT = 4;
// (Off the line, a gunner and the jammer on him locked up only so far
// (feet) and so long after the snap (ms) - then off the jam into their
// runs, not chopping their feet at full tilt in it.)
const JAM_FIGHT_FT = 3;
const JAM_FIGHT_MS = 800;
const AIM_OUT_FT = [24, 62] as const;
const SHANK = 0.4;
const SHANK_YDS = 36;
const SHANK_SURE_YDS = 15;
const SHANK_HANG = [2500, 3100] as const;
const POOCH = 0.45;
const POOCH_YDS = 45;
const POOCH_IN = 15;
const POOCH_HANG = [4300, 4800] as const;
// Fielding it (on the returner's own count): one coming down this far
// (feet) or more past where he stood, caught going back this often - the
// last of it this far (feet) at this pace (feet a second); one this far
// short of him, caught coming up at a run this often (one about where he
// stood, this often) - when no gunner is within this far (feet) of it as
// it comes down - off from this far back (feet) at this much of his top
// speed, from off to one side up to this much (feet). There this long
// before he goes (ms).
const FIELD_BACK_LEAST = 10;
const FIELD_BACK = 0.6;
const FIELD_BACK_RUN = 5;
const FIELD_BACK_FPS = 7.5;
const FIELD_UP_LEAST = 10;
const FIELD_UP = 0.6;
const FIELD_UP_ANY = 0.15;
const FIELD_UP_ROOM = 18;
const FIELD_UP_RUN = [12, 22] as const;
const FIELD_UP_PACE = [0.55, 0.75] as const;
const FIELD_UP_SLANT = 8;
const FIELD_SET_MS = 250;
// (No time to set back off it: straight up into it at a run if it is in
// front of him - no more than this far round from straight at the kicker,
// the cosine - and it takes this pace at least, feet a second.)
const FIELD_UP_FRONT = 0.5;
const FIELD_UP_LEAST_FPS = 8;
// A kick caught set under it in a basket this often: in at his belly, this
// high (feet) and this far out in front of him (feet).
const BASKET = 0.5;
const BASKET_Z = 3.7;
const BASKET_IN = 0.7;
// Juggled this often (less, the surer his hands - this much less at the
// surest), his hands on it again this long after (ms), it up this high off
// them (feet), him this far on into it (feet).
const JUGGLE = 0.07;
const JUGGLE_HANDS = 0.6;
const JUGGLE_MS = 520;
const JUGGLE_UP = 1.8;
const JUGGLE_ON = 1.2;
const LET_GO = 0.35;
const LET_SHORT = [16, 30] as const;
const LET_HOPS_MS = [650, 420, 520] as const;
const LET_ASIDE = 9;
const LET_WAVE_FROM = 1800;
const LET_WAVE_TO = 300;
const LET_DOWN_MS = 900;
// (The cover man downing it there as much as this much later (ms), pulled
// up into it.)
const LET_DOWN_SPARE_MS = 400;
// A kickoff aimed out toward the numbers this often (on the kicker's own
// count), this far (feet) out from the middle.
const KICK_AIM_OUT = 0.35;
const KICK_OUT_FT = [36, 60] as const;
// The returner going back under one over him this often, up into one
// short of him this often (see FIELD_BACK, FIELD_UP).
const KICK_BACK = 0.5;
const KICK_UP = 0.55;
// Downed in the end zone: his knee this long after the catch (ms).
const KICK_KNEE_MS = [350, 1000] as const;
// One coming down this far (feet) or more into the end zone, nobody's to
// get to it, let go this often: on a hop (feet, and up to this much more)
// and a roll (the same) toward the back of it, this long each (ms), the hop
// this high (feet); him this far off to the side of it.
const KICK_LET_IN = 3;
const KICK_LET = 0.3;
const KICK_LET_HOP = [8, 6] as const;
const KICK_LET_ROLL = [10, 12] as const;
const KICK_LET_MS = [600, 650] as const;
const KICK_LET_UP = 2.6;
const KICK_LET_ASIDE = 8;
// A miss wide, now and then off an upright instead - doink - and back off
// it (this often of them): hit this far (feet) out from the middle of the
// post at most; back off it at this pace (ft/s, a range), this far round
// from straight back out off it (radians, a range); a hop on the turf this
// much again of the way, over this long (ms), and lying there.
const DOINK = 0.12;
// A kick bent on its way, at the most this far (feet) out from straight
// halfway along - this much of the most (a range) on the kick's count - at
// this many yards, more the longer it is (no more than this many times
// as much).
const KICK_BEND_FT = [1.2, 4.5] as const;
const KICK_BEND_YDS = 40;
const KICK_BEND_MOST = 1.4;
const DOINK_OUT = 0.25;
const DOINK_FPS: [number, number] = [10, 22];
const DOINK_TURN: [number, number] = [0.3, 1.1];
const DOINK_HOP = 0.3;
const DOINK_HOP_MS = 420;
// A field goal good, a try good: this often the holder and the kicker slap
// hands after it - the kicker this often with his fist pumped first.
const KICK_FIVE = 0.75;
const TRY_FIVE = 0.4;
const KICK_PUMP = 0.35;
// (A long field goal: this far, yards.)
const KICK_BIG = 45;
// The rest of them as it goes over or by (see fieldGoal): this often glad
// of it - a field goal good, a try good, a kick missed - or rueing a miss;
// the kicker, this often; seeing it this long after it crosses (from 0.4
// to 1.4 of it).
const KICK_GLAD = 0.35;
const TRY_GLAD = 0.15;
const MISS_GLAD = 0.4;
const KICK_RUE = 0.2;
const KICKER_RUE = 0.8;
// The kicker's way with a miss (see fieldGoal), his own count: hands on his
// helmet under the first share, on his hips (his head down) under the
// second, else down on a knee: either pulled up first this long after he
// sees it (ms), then there so long (ms, give or take a fifth); up out of
// either over this long (ms).
const KICKER_LOOK = [0.45, 0.75] as const;
const KICKER_STOP_MS = 350;
const KICKER_HIPS_MS = 2200;
const KICKER_KNEE_MS = 1500;
const KICKER_UP_MS = 500;
const KICK_SEEN_MS = 300;
// A kick missed, the kicker stood there with it: the holder this often up
// and over to him (see holderPat), from this long after the kicker sees it
// (ms, and up to this much later); at this pace (ft/s), turned in to him
// this much (radians), his hand out onto this spot on him (his frame, feet:
// on top of his near shoulder, behind the arm); there this long (ms) first,
// then at it this long, and stood there after this long.
const HOLDER_PAT = 0.65;
const HOLDER_PAT_MS: [number, number] = [250, 400];
const PAT_FPS = 4.5;
const PAT_TURN = 0.2;
const PAT_ON = { f: -0.5, s: 0.4 };
const PAT_SET_MS = 150;
const PAT_MS = 1400;
const PAT_AFTER_MS = 400;
// (Where his hand is on him, out on the shoulder: see PAT.)
const PAT_HAND = handOf(drawnBody(footballBody()), "pat", PAT_AT);
// (A man pulled up by the hand - see handUp: his hand held up, on his
// front or his back, and the one held down to him, where they meet - in
// each one's own frame; how far out in front of him the one helping him
// has it, and turned how far to his right.)
const TAKE_HAND = handWith(
	drawnBody(footballBody()),
	"getUp",
	"takeHand",
	CLASP_UP_AT,
);
const TAKE_HAND_BACK = handWith(
	drawnBody(footballBody()),
	"getUpBack",
	"takeHandBack",
	CLASP_UP_AT,
);
const HELP_HAND = handOf(drawnBody(footballBody()), "helpUp", CLASP_HELP_AT);
const HELP_HAND_FT = Math.hypot(HELP_HAND.f, HELP_HAND.s);
const HELP_HAND_TURN = Math.atan2(-HELP_HAND.s, HELP_HAND.f);
// (Come from right at it: out from it to the side of him it is on.)
const HAND_UP_LEAST_FT = 0.1;
// (The hand he holds up: his right or his left, up off his front or his
// back.)
const handUpOf = (back: boolean, left: boolean): Move =>
	back
		? left
			? "takeHandBackL"
			: "takeHandBack"
		: left
			? "takeHandL"
			: "takeHand";
const FIVE_MS = 800;
// (Squared up to each other this long first, the later there of the two -
// not into it off his last step.)
const FIVE_SET_MS = 200;
// (Two men slapping hands: how far apart, how far turned - see handsMeet.)
const FIVE = handsMeet(drawnBody(footballBody()), "highFive", HIGH_FIVE_AT);
// A punt going in for a touchback: the cover easing up this far short of
// the goal line (feet) and as much as this much further.
const TB_SHORT = 6;
const TB_SPREAD = 18;
const KICK_KINDS = new Set<PlayKind>([
	"kickoff",
	"onside",
	"punt",
	"fieldGoal",
	"extraPoint",
]);

// The play from the snap: what the sim says happened, acted out - or
// undefined, a play this does not act out (yet), left standing in its
// stances.
export const actOut = (input: ActInput): Acted | undefined =>
	new Action(input).run();
