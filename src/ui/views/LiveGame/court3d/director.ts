import { makeCourtRng } from "../courtRng.ts";
import {
	HEAVE_MAX_SECONDS,
	MOTION_HANDLER_SLOT,
	MOTION_OFFENSE_SPOTS,
	RIM_INSET,
	TRANSITION_OFFENSE_SPOTS,
	possessionBeats,
} from "../courtSpots.ts";
import {
	attackDir,
	benchX,
	clampPt,
	seatSpot,
	checkInPath,
	COURT_H,
	COURT_W,
	dist,
	FT_LINE_DEPTH,
	FT_SHOOTER_DEPTH,
	ftDefenseSpot,
	ftOfficialBall,
	ftOffenseSpot,
	guardSpot,
	HUDDLE_Y,
	inPlay,
	OFFICIALS_SETTLE,
	sideOn,
	SIDELINE_GAP,
	huddleSpots,
	other,
	RIM_R,
	RIM_Z,
	rimPt,
	rimX,
	spot,
	TABLE,
	type Pt,
	type Pt3,
	type Side,
} from "./geometry.ts";
import {
	BURST,
	HARDEST_PULL_UP,
	alongShape,
	keepThrough,
	paceFor,
	paceAfter,
	pullUpFrom,
	pushFrom,
	reachIn,
	runMs,
	runShape,
	type RunShape,
} from "./motion.ts";
import {
	BACKSPIN,
	CROSS_RATE,
	DRIBBLE_RATE,
	FLOW_MS,
	GRAVITY,
	PASS_TURN,
	PASS_PICK_UP,
	SEP_MS,
	TURN_RATE,
	evalBall,
	floorSpotOf,
	forgetTurns,
	liveSeg,
} from "./evaluate.ts";
import {
	findShot,
	playAt,
	playShot,
	SAMPLE_MS,
	type FoundShot,
	type ShotKind,
	type ShotPlay,
	type ShotWant,
} from "./physics.ts";
import {
	bodyOf,
	JUMPER,
	mirror as mirrorPose,
	poseAt,
	releaseAt,
	shotForm,
	skeleton,
	standingReach,
	type AnimName,
	type DribbleMove,
	type Hand,
	type V3,
} from "./poses.ts";
import {
	callAny,
	callShot,
	callTurnover,
	castPlay,
	PLAYS,
	walkPlay,
	type Called,
	type Cast,
	type Play,
	type PlayAction,
	type PlayCategory,
	type PlayOption,
	type PlayRisk,
	type PlayZone,
	type Role,
	type TurnoverKind,
	spotXY,
} from "./plays.ts";
import { BREAK_SHARE } from "./nbaRates.ts";
import {
	callOrder,
	INTRO_CALL,
	INTRO_FIRST,
	INTRO_HOLD,
	INTRO_LIGHT,
	rowSpot,
	tunnelSpot,
	type Intro,
	type IntroCall,
} from "./intro.ts";
import {
	finishOf,
	type Finish,
} from "../../../util/liveGameWording.basketball.ts";

// THE DIRECTOR.
//
// The sim says WHAT happened - who shot, from which zone, who rebounded - and
// when, by the game clock. It never says where anybody was. The director reads
// the whole game's play-by-play up front and stages it: a track for every
// player (where he runs, what his body does, which way he faces), a path for
// the ball, and the little effects (a swish, a rattle, a whistle).
//
// Every line of play-by-play becomes a BEAT with two halves:
//
//   preStart ... actionStart   the lead-in the sim never mentions: the inbound,
//                              the ball coming up the floor, a swing or two -
//                              paced by how much clock the possession burned
//   actionStart ... end        what the line describes; its text appears at
//                              actionStart, the moment it happens on screen
//
// Beats tile the timeline with no gaps, so a playback cursor (events consumed)
// maps to exactly one moment: the actionStart of the next line not yet shown.
//
// It is a pure function of the event list and the game, so every device in a
// multiplayer game - and every rewatch - stages the same game. Where the
// play-by-play says HOW a play finished ("throws it down", "the layup is
// good", "rims out"), the court acts out exactly that (see liveGameWording).
// Times are milliseconds at 1x speed.

export type RawEvent = { type: string; [key: string]: any };
export type CourtPlayer = {
	pid: number;
	team: Side;
	pos?: string;
	// The sim's skill tags ("B" a ball handler, "Ps" a passer, ...), where
	// they are known.
	skills?: string[];
	// Hurt in this game: what it is, and how many games it keeps him out.
	injury?: { type: string; games: number };
};

export type Move = {
	t0: number;
	t1: number;
	from: Pt;
	to: Pt;
	anim: AnimName;
	// Set when he was told which way to face on the way (a defender sliding
	// with his man), rather than just running where he is going.
	face?: 1 | -1;
	// How fast he is going (feet a second) as it starts and as it ends, where
	// that is known - a defender's path worked out stride by stride (see
	// mark) - rather than read off the runs either side of it.
	v0?: number;
	v1?: number;
};
export type Act = {
	t0: number;
	t1: number;
	anim: AnimName;
	// A parabolic jump over [start, end] (fractions of the act) peaking at
	// `peak` feet, or explicit height keys for a dunk's hang on the rim.
	jump?: [number, number, number];
	zKeys?: [number, number][];
	// A dunk: the rim, and over the act how much his hands are on it (0 to
	// 1). Its heights are a typical player's - a longer reach jumps less to
	// get there, a shorter one more.
	rim?: { at: Pt3; grip: [number, number][] };
	// Up for the ball where it will be: the jump is a typical player's, to
	// get his hands there - his own reach decides how high he really goes.
	reach?: true;
	// What he looks at while he does it: the rim he shoots at, the man he
	// passes to.
	look?: Pt;
	// Done the other way round: his left hand doing what the move does with
	// his right - a block or a contest with the hand on the ball's side.
	mirror?: true;
};
// Something he says with an arm while the rest of him goes on with whatever
// it is doing - running, sliding, dribbling: a point (at the man he has, or
// the screen coming), a hand up calling for the ball, a wave to come on, a
// slap of hands with a teammate going by - or a shooter's follow-through,
// held up after he lands till the ball gets there.
export type Gesture = {
	t0: number;
	t1: number;
	kind: "point" | "hand" | "wave" | "slap" | "follow";
	// What he points or waves at: a man, wherever he is, or a spot.
	at?: number | Pt;
	// A follow-through off his left hand.
	mirror?: true;
};
export type Track = {
	pid: number;
	team: Side;
	start: Pt;
	moves: Move[];
	acts: Act[];
	arms: Gesture[];
	faces: [number, 1 | -1][];
	// From each moment until his next move, what he stands looking at (the
	// middle of a huddle, the rim from the free throw line).
	looks: [number, Pt][];
	shown: [number, boolean][];
	// Eased aside off a man he would otherwise be standing on (see
	// keepApart), in order of when each starts.
	nudges?: Nudge[];
	// Kept off a man he would still be in (see keepOff), in order.
	sep?: Sep[];
};
// From t0, how far over he is kept (feet), every SEP_MS - and how far that
// has taken him all told.
export type Sep = { t0: number; dx: number[]; dy: number[]; dl: number[] };
// A step aside: from t0 he eases over by (dx, dy), and back by t1 - over
// `ramp` ms each way (by default, a quick step).
export type Nudge = {
	t0: number;
	t1: number;
	dx: number;
	dy: number;
	ramp?: number;
};
// Acts with nothing on the ball that can be cut down to a moment (see
// finish).
const GLIMPSE_MS = 60;
const GLIMPSED = new Set<string>([
	"fight",
	"boxOut",
	"bump",
	"contest",
	"reach",
]);
// Meeting a shot from the camera's side of the shooter (which looks across
// the floor from the near side), a defender this near him across the floor
// (feet) would stand in front of him in the picture: he comes at him from
// the side instead - this far across at the least, toward `toward`, level
// with him (and as far off him as he would have been).
const SIDE_ON = 2.8;
const SIDE_AT = 2.2;
const besideShot = (P: Pt, p: Pt, toward: number): Pt => {
	const dx = Math.abs(p.x - P.x);
	const dy = (p.y - P.y) * 0.2;
	const off = Math.sqrt(Math.max(0, dist(P, p) ** 2 - dy * dy));
	return p.y > P.y + 0.3 && dx < SIDE_ON
		? {
				x: P.x + (toward >= P.x ? 1 : -1) * Math.max(dx, SIDE_AT, off),
				y: P.y + dy,
			}
		: p;
};

// How often a post-up set for the hook ends in a fadeaway instead.
const POST_FADE = 0.3;

export type BallEnd = Pt3 | { pid: number; hand?: "near" | "far" | "both" };
export type BallSeg =
	| {
			kind: "hold";
			t0: number;
			pid: number;
			// A crossover switches hands every bounce.
			style: "hold" | "dribble" | "cross";
			// The hand he dribbles with (a crossover: starts in); right if
			// unsaid. A crossover goes across in front of him, between his
			// legs or behind his back.
			hand?: Hand;
			move?: DribbleMove;
	  }
	| {
			kind: "fly";
			t0: number;
			t1: number;
			from: BallEnd;
			to: BallEnd;
	  }
	| {
			kind: "bounce";
			t0: number;
			t1: number;
			from: Pt3;
			to: Pt;
			hops: number;
			h0: number;
	  }
	| { kind: "rest"; t0: number; at: Pt3 }
	// Off the rim and the glass, down through the net (see physics.ts):
	// where it is every SAMPLE_MS from t0 (x, y, z, x, y, z, ...), how fast
	// it is going at the start, and how it turns - from `roll0`, `spin`
	// radians a second.
	| {
			kind: "path";
			t0: number;
			t1: number;
			pts: number[];
			v0: Pt3;
			roll0: number;
			spin: number;
	  };
// "roar" is a big play - a dunk, a three, an and-one - that brings the bench
// and the crowd up.
export type FxKind =
	| "swish"
	| "clank"
	| "dunk"
	| "block"
	| "whistle"
	| "cheer"
	| "roar"
	// The official throws the jump ball up.
	| "toss"
	// The game won a playoff series (`big`: the championship), and confetti
	// down from the rafters.
	| "clinch"
	| "confetti";
// What a whistle was for, for the officials to signal: a foul (on a shot,
// or through one that counts), a travel, an offensive foul, the ball out of
// bounds, the shot clock or five seconds, or play stopped.
export type Call =
	| "foul"
	| "shootingFoul"
	| "andOne"
	| "travel"
	| "offensive"
	| "out"
	| "clock"
	| "stop";
// `big` marks a dunk worth a replay: on somebody, off a lob, or through
// contact - or, on a swish (not a dunk's), a three in a tight finish, a
// basket at the buzzer, or one through contact (an and-one). A roar says
// what it was for, for the boards to shout about.
export type Fx = {
	kind: FxKind;
	t: number;
	rim?: Side;
	team?: Side;
	big?: boolean;
	what?: "three" | "dunk" | "andOne";
	// A whistle's call, where it was, and (as `team`) who gets the ball.
	call?: Call;
	at?: Pt;
};
// A look round the building while play is stopped - before the opening tip,
// through a timeout, between periods: the whole arena wide, or low in the
// seats looking up at the crowd and the rafters. The picture cuts in and out
// of it.
export type ArenaShot = { t0: number; t1: number; kind: "wide" | "crowd" };
export type Beat = {
	i: number;
	type: string;
	preStart: number;
	actionStart: number;
	end: number;
};
export type CourtTimeline = {
	tracks: Map<number, Track>;
	ball: BallSeg[];
	fx: Fx[];
	beats: Beat[];
	// Which display team has the ball, over time (offense stands, defense crouches).
	poss: [number, Side][];
	// Moments the picture cuts: everyone may be somewhere else just after.
	cuts: number[];
	// Of those, the cuts from one clip of a highlight reel to the next: the
	// picture dips to black across each.
	clips?: number[];
	// Jump balls, until they are tipped: both teams just ready.
	jumps?: [number, number][];
	shots: ArenaShot[];
	// How tense the building is, over time (0 to 1): a close game late in
	// the last period or in overtime.
	tension: [number, number][];
	// How full the seats are, over time (1: everybody in them): emptier
	// coming back from halftime, and as the home crowd heads for the exits
	// in a blowout loss.
	seats?: [number, number][];
	// Stretches the picture runs through fast rather than cutting past: the
	// ball taken out and brought up the floor, the walk to the line.
	fast: [number, number][];
	// The men going to the scorer's table to check in, before each comes on.
	checkIns?: CheckIn[];
	// The starting lineups called out before the opening tip (see intro.ts).
	intro?: Intro;
	// The warm-ups, before it is ready (see pregame.ts): the pre-game show
	// on the floor.
	warmup?: boolean;
	end: number;
};

// A sub on his way in: up out of his chair at t0, along the path to the
// table, down on a knee there from kneel, pulling his warm-up top off from
// strip, and on at t1 - from the end of the path.
export type CheckIn = {
	pid: number;
	team: Side;
	path: Pt[];
	t0: number;
	kneel: number;
	strip: number;
	t1: number;
};
// How long pulling the warm-up off takes.
export const STRIP_MS = 1000;
const CHECK_IN_WALK = 4.4;

const NOT_A_LINE = new Set(["stat", "timeouts", "init"]);
export const isLineItem = (e: RawEvent | undefined): boolean =>
	!!e &&
	typeof e.type === "string" &&
	!NOT_A_LINE.has(e.type) &&
	!(e.type === "sub" && e.silent);

type Zone = "atRim" | "lowPost" | "midRange" | "three" | "tipIn" | "putBack";

const ATTEMPT_ZONE: Record<string, Zone> = {
	fgaAtRim: "atRim",
	fgaLowPost: "lowPost",
	fgaMidRange: "midRange",
	fgaTp: "three",
	fgaTpFake: "three",
	fgaTipIn: "tipIn",
	fgaPutBack: "putBack",
};
const resultOf = (
	type: string,
): { zone: Zone; kind: "make" | "miss" | "block" } | undefined => {
	const m =
		/^(fg|miss|blk)(AtRim|LowPost|MidRange|Tp|TipIn|PutBack)(AndOne)?$/.exec(
			type,
		);
	if (type === "tp" || type === "tpAndOne") {
		return { zone: "three", kind: "make" };
	}
	if (!m) {
		return undefined;
	}
	const zone: Zone =
		m[2] === "Tp"
			? "three"
			: ((m[2]![0]!.toLowerCase() + m[2]!.slice(1)) as Zone);
	return {
		zone,
		kind: m[1] === "fg" ? "make" : m[1] === "miss" ? "miss" : "block",
	};
};

// How a man goes down with what he hurt (the injury's name, as the game
// gives it): a knee or an ankle bad enough to keep him out has him down on
// the floor holding it; his face or head, bent over with his hands to it; a
// hand or an arm, holding it; anything else, doubled over.
const hurtFor = (
	injury: { type: string; games: number } | undefined,
): { anim: AnimName; down: boolean } => {
	const type = injury?.type.toLowerCase() ?? "";
	const bad = (injury?.games ?? 0) >= 3;
	if (/knee|acl|mcl|pcl|menisc|patell/.test(type)) {
		return bad
			? { anim: "hurtKnee", down: true }
			: { anim: "hurt", down: false };
	}
	if (/ankle|achilles|foot|toe|plantar|calf|leg|peroneal/.test(type)) {
		return bad
			? { anim: "hurtAnkle", down: true }
			: { anim: "hurt", down: false };
	}
	if (
		/head|concussion|orbital|cheek|jaw|nose|eye|face|facial|tooth|neck|whiplash|throat/.test(
			type,
		)
	) {
		return { anim: "hurtHead", down: false };
	}
	if (/hand|finger|thumb|wrist/.test(type)) {
		return { anim: "hurtHand", down: false };
	}
	if (/shoulder|collarbone|bicep|tricep|elbow|arm|rotator/.test(type)) {
		return { anim: "hurtArm", down: false };
	}
	return { anim: "hurt", down: false };
};

// Positions in the order a lineup fills its slots: guards out top, bigs low.
const POS_RANK: Record<string, number> = {
	PG: 0,
	G: 1,
	SG: 2,
	GF: 3,
	SF: 4,
	F: 5,
	PF: 6,
	FC: 7,
	C: 8,
};

// Speeds in feet per second, true to life: the timeline plays at real speed.
// Where the sim's clock runs on with nothing worth watching - the walk up the
// floor after a basket, the trip to the free throw line - the picture runs
// through it fast (see hurry) rather than anybody hurrying. Animation cycles
// advance by distance covered, so feet never skate.
const RUN = 21;
const SPRINT = 24;
const DRIBBLE = 19;
const JOG = 13;
// Called out for the starting lineups: running out, pumped up.
const INTRO_RUN = 17;
const WALK = 6;
// Not a pace but a jab: one quick lunge of a step (see liven).
const JAB = 0;
// A pass: this fast (feet a second) on top of a moment for it to get going.
// Hand to hand that is 29-37 ft/s over 13-30 feet - 9-11 m/s, a good
// player's chest pass, not a lob of one.
const PASS_FTPS = 46;
const PASS_BASE_MS = 100;
// A basketball's radius, feet: its middle when it touches the floor.
const BALL_R = 0.39;
// One bounce of a crossover, from one hand to the other, and one of a
// dribble (see evaluate.ts).
const CROSS_MS = 1000 / CROSS_RATE;
const DRIBBLE_MS = 1000 / DRIBBLE_RATE;
// A pass over the top goes up over his head first: this much longer from
// the start of his throw to the ball leaving his hands.
const OVERHEAD_WIND = 140;
const RELEASE_MS = 120;
// Down taking a charge (ms from the contact until he is back up); his
// teammates there to help him up from this long after it, if no further off
// than so far (feet).
const CHARGE_DOWN_MS = 2400;
const HELP_UP_AT = 1100;
const HELP_UP_FROM = 18;
// (Still down until so far into it, then up onto a knee - as a teammate's
// hand gets to him, if one is coming: there by so long after the contact.)
const CHARGE_UP_AT = 0.6;
const HELP_UP_BY = 1700;
// A helper's hand down to him so long (ms) after he gets there; the pull up
// over so long after that.
const HELP_HAND_MS = 300;
const HELP_PULL_MS = 650;
// Of passes off a live dribble, how many go one-handed: on the move, stood.
const PUSH_SHARE: [number, number] = [0.9, 0.5];
const PUSH_OFF_MS = 60;
// Of those on the move to a man out the side of his other hand - this far
// off (feet), ahead of him or square to him - how many go behind his back.
const BEHIND_SHARE = 0.45;
const BEHIND_FT: [number, number] = [8, 30];
const BEHIND_AHEAD: [number, number] = [-0.45, 0.85];
// (On the move at this pace at least, feet a second, until no longer than
// this before it goes, ms.)
const BEHIND_FTPS = 7;
const BEHIND_STILL_MS = 150;
// A three let go with no more than this left (seconds): at the buzzer.
const BUZZER_SECS = 1.5;
// The last look round the building once the game is over (see gameOver),
// before the picture holds on it.
const LAST_LOOK_MS = 3500;
// A spin move (see spinBy): how often a drive with his man square in front
// of him goes into one rather than a crossover, from how far out (feet), how
// far short of his man he plants (feet), and how fast he goes round him
// (feet a second).
const SPIN_SHARE = 0.3;
const SPIN_FROM_FT = 11;
const SPIN_SHORT = 2.6;
const SPIN_FTPS = 10;
// A reverse layup (see the finish at the rim): how often a finish come in
// along the baseline goes on under the rim to the far side, and how far out
// past the middle of it there he goes up (feet).
const REVERSE_SHARE = 0.5;
const REVERSE_OUT = 2.4;
// ...and the ball, in the hand up and back at the rim, no further behind the
// middle of it than this as it leaves his hand (feet): clear of the glass.
const REVERSE_BALL_BEHIND = 0.4;
// Going up for a finish beside the rim, he is at least this far (feet) out
// in front of the middle of it: his hand, reaching out to the side with the
// ball, still in front of the glass - not round behind it, where the ball can
// only get to the rim through the board or up through the ring.
const FINISH_FRONT = 1;
// ...and at least this far (feet) out from the middle of it.
const FINISH_OUT = 2.2;
// And let go from there, the ball goes up this far (feet) over the rim,
// and down onto it.
const LAYUP_LOFT = 1;
// The room round a man going up over another for a dunk (feet): nobody else
// of theirs nearer him than that.
const POSTER_ROOM = 3.2;
// And out of his way in: at the least this far (feet) to the side of the
// line he comes in on.
const POSTER_LANE = 3;
// How far off the dunker the man he dunked on is, once both are down (ft).
const DUNKED_ON_ROOM = 3.4;
// The least time (ms) a reaction stood in one place is worth doing at all.
const IN_PLACE_MIN = 400;
// A drop step or an up-and-under goes up this far out (feet): a low-post
// shot, not one from under the rim.
const POST_FINISH_OUT = 3.4;
// A pass thrown away goes this far wide of the man it was meant for (feet,
// out where he is): past his hands, not through him.
const WIDE_OF = 3;
// How far out in front of his feet a man bent to the floor has his hands.
const PICKUP_REACH = releaseAt("pickup", 0.65).f;
// A ball bouncing on the floor keeps its middle this far off a man's
// (feet): off him, not through him.
const BOUNCE_CLEAR = 1.2;
// How far through a bounce of his dribble the ball has come back up near
// enough to his hands to take it in both (see evaluate.ts).
const CATCH_UP = 0.7;
// After a whistle, how long the official's signal holds the picture.
const WHISTLE_HOLD = 950;
// A rebound, from leaving the floor to the ball chinned once he is down.
const REBOUND_MS = 1100;
// How long a man takes to read a shot off the shooter's hand and go.
const READ_MS = 180;
// How long before a miss comes off the rim the man who gets it is after it
// - reading it off the shot - and how long before the top of his jump for
// it he can still be on his way there, the last stride carrying him up.
const REBOUND_READ = 1200;
const BOARD_CARRY = 150;
// The jump ball: round the circle a step outside it (feet from the middle),
// at these angles (degrees, from along the floor, the far sideline side
// left open for the official) - the two teams in turn - and one of each
// back this far, behind his own jumper.
const TIP_RING = 7.6;
const TIP_SLOTS: Record<Side, readonly number[]> = {
	0: [-40, 70, 155],
	1: [25, 110, -140],
};
const TIP_BACK = 13;
// The man of theirs who goes up with him for it: from this near where he goes
// up (feet) he goes up where he is; from as far as RIVAL_IN, he gets in a step
// or two first - seeing it coming this long before (ms).
const RIVAL_UP = 4;
const RIVAL_IN = 9;
const RIVAL_STEP_MS = 450;
// A defender whose man stays out goes in to the glass and puts a body on the
// nearest man coming in that nobody else has - from this far off (feet).
const GLASS_FIND = 10;
// Busy with something else as the shot goes up (a contest, a cut still on
// its way), a man still plays the glass once he is done - if that leaves
// him this long (ms) before it comes off the rim.
const GLASS_LATE = 550;
// The rest of them near a rebound as it comes down - within this far of
// where it is taken (feet) - play it too, from as it comes loose (but this
// long before he has it at the least, this long at the most - ms): a step or
// two at it, no nearer than this.
const SCRAMBLE_FT = 8;
const SCRAMBLE_MS = 450;
const SCRAMBLE_MAX = 1400;
const SCRAMBLE_STEP = 4;
const SCRAMBLE_NEAR = 2.6;
// Men going in to the glass end up at least this far apart (feet) - each
// tried this much further round the rim (radians) till he is.
const GLASS_APART = 3;
const GLASS_TURN = 0.4;
// Backing out after a shot only if that keeps the rim within this (cosine)
// of straight ahead of him.
const BACK_FACING = 0.6;
// The ball theirs, the other five turn and get back (see getBack): from this
// long after (ms), anybody further than this from the rim at their end
// (feet), this much nearer it - and back in to the top of the key, not
// stopped at half court where the ball comes up: as far as this from it -
// to no nearer than this.
const BACK_REACT = 250;
// How long a pause (ms) a man's last stride up at the rim can follow on
// from the end of his drive without his pulling up for it (see carry).
const ON_FROM_MS = 700;
// Driving to the rim, how far out (feet, from the middle of the rim) he
// gathers it for a layup, and for a dunk; the most his last strides in to a
// layup cover; and how fast he is still going (feet a second) as it leaves
// his hand, carrying him on in the air.
const LAYUP_GATHER = 7;
const DUNK_GATHER = 6;
const LAYUP_STRIDE = 4.5;
const AIR_DRIFT = 3;
// A man cutting to the rim is hit on the move (see leadTheCut): the ball in
// his hands this far out (feet from the middle of the rim), still going;
// thrown no sooner than this long (ms) after the passer has it; and a cut
// that has him there sooner than this (ms) before the ball - stood under
// the rim waiting on it - is timed to it instead.
const CUT_CATCH_OUT = 7.5;
const CATCH_TO_THROW = 300;
const PARKED_MS = 150;
// His cut in is laid from where he comes this near the rim (feet) - and he
// has it no sooner than this far (feet) into it, up to speed.
const CUT_FROM = 16;
const CUT_RUN_UP = 4;
// Getting to where his cut starts sooner than he sets off on it, he takes
// his run there no slower than this on average (feet a second) - and stands
// there only what is left.
const IN_STRIDE_PACE = 8;
// The longest pause between two runs the same way (ms) a man runs on
// through instead (see flowOn).
const FLOW_ON_MS = 1500;
// Pulling up off a handoff, he dribbles this far (feet) on with it first.
const PULL_UP_OFF = 4.5;
// Pulling up for three off a ball screen, he dribbles this far (feet) off
// it, past the screener's shoulder, first - turning the corner.
const TURN_THE_CORNER = 7;
// The furthest (feet) a man dribbles it back the way he faces - a retreat
// dribble, eyes still on the defense (see go).
const RETREAT_MOST = 6;
// The furthest (feet) a shooter hops back to get behind the line for a
// three: further in, he dribbles out to it.
const STEP_BACK_MOST = 3.5;
// A pull-up jumper from the elbow, at most this far out (feet).
const MID_PULL_UP_IN = 20.5;
// A roll to the rim with the man with the ball driving into the lane - to
// within ROLL_AWAY_IN feet of the rim, more than ROLL_AWAY_MID off the middle
// of it - goes in to the rim's far side from him: this far out from it, and
// across (feet).
const ROLL_AWAY_IN = 12;
const ROLL_AWAY_MID = 1.5;
const ROLL_AWAY_OUT = 2.5;
const ROLL_AWAY_ACROSS = 4.5;
// Holding it this near the rim (feet) or nearer, a man fakes going up with
// it - never a jab step (see liveHands).
const IN_CLOSE_FAKE = 9;
// A whistle off the ball with the man with it stood waiting (see probe):
// he takes a beat with it first (ms), probes for at most the last this long
// (ms) before it goes, and in toward the rim no nearer than this (feet).
const PROBE_LOOK = 500;
const PROBE_MS = 1800;
const PROBE_IN = 15;
// A run in counts as a cut to the rim for the ball if it ends this near
// the rim (feet) - or, rolling or cutting into the lane for a hook or a
// floater there, this near.
const RIM_CUT_TO = 6.5;
const LANE_CUT_TO = 12;
// A basket, the team that scored turns and gets back this long (ms) after
// it is through (see inboundAfterMake).
const MADE_BACK_MS = 350;
// The point guard taking it in after a basket: up the floor this far
// (feet), then back to the ball.
const COME_BACK = 7;
const BACK_FROM = 50;
// Back down the floor to the paint and round it: this far out from the rim
// (feet) down the middle, and this much further for every foot across from
// it; and drawn this much in toward the middle from the lane he came back in.
const BACK_DEEP = 6;
const BACK_WIDE = 0.55;
const BACK_PINCH = 0.35;
// Back down the floor no two nearer each other across it than this (feet),
// nor nearer a sideline than this; a man starting this near one gone ahead
// of him (feet) goes this long after him (ms).
const BACK_LANES = 7;
const BACK_SIDE = 5;
const BACK_BESIDE = 5;
const BACK_BEHIND = 300;
// How far out from the rim (feet) a long rebound comes down: where a guard
// or a wing with nobody to box out waits on it.
const LONG_BOARD: [number, number] = [11, 16];
// The last few inches onto a spot: a shuffle this long (ms).
const SETTLE_MS = 160;
// A loose ball on the hop is taken out of the air about where a man reaching
// for it has his hands (see the snatch in poses.ts) - from a little under
// them to a little over (feet) - this far out in front of him.
const SNATCH_AT = releaseAt("snatch", 0.36);
const SNATCH_LOW = SNATCH_AT.u - 0.6;
const SNATCH_HIGH = SNATCH_AT.u + 0.8;
const SNATCH_OUT = SNATCH_AT.f;
// Up for a rebound, at the top of his jump - 400ms after he leaves the floor
// - a typical player's hands are this high over his feet (feet), and this far
// out in front of him.
const BOARD_TOP = 400;
const BOARD_AT = releaseAt("board", BOARD_TOP / REBOUND_MS);
const BOARD_HANDS = BOARD_AT.u;
const BOARD_OUT = BOARD_AT.f;
// Up for one he only gets a hand to - knocked away, not taken in: the same
// jump, the arms up through it.
const TIP_AT = releaseAt("rebound", BOARD_TOP / REBOUND_MS);
// Two men slapping a low five stand this far apart (feet, middle to
// middle): each one's hand out in front of him at the hip, meeting.
const LOW_FIVE_APART = (() => {
	const sk = skeleton(bodyOf(), poseAt("lowFive", 0.5));
	return 2 * sk.armR.end.f + 0.3;
})();
// The ball held in front of him, both hands on it, at the line.
const FT_HOLD = releaseAt("hold", 0);
// A poke at a man's dribble, from the start of the jab to the hand back:
// the jabbing hand at the end of its reach, a typical player's (forward,
// to his side, up), and how far through the poke that is.
const POKE_MS = 450;
// Pushing it up for a man still on his way, he takes longer about it rather
// than stand waiting longer than this (ms) - on a push of this far (feet)
// at the least (see keepComing).
const PUSH_WAIT = 600;
const PUSH_FT = 15;
// A man farther off than this (feet) from the ball he pokes away comes to
// meet it: the man with it brings it on at him, this much of the way.
const POKE_FAR = 12;
const POKE_MEET = 0.4;
const POKE_HIT = 0.42;
// Reaching out for a ball going by (from the start of the reach to the
// hands back): his hands - between them - at full reach, forward of him
// and up, and how far through the reach that is.
const REACH_MS = 400;
const REACH_HIT = 0.45;
const REACH_AT = (() => {
	const sk = skeleton(bodyOf(), poseAt("reach", REACH_HIT));
	return {
		f: (sk.armR.end.f + sk.armL.end.f) / 2,
		u: (sk.armR.end.u + sk.armL.end.u) / 2,
	};
})();
// A catch, from hands up to it taken in: where the ball meets his hands, a
// typical player's, and how far through the catch that is.
const CATCH_MS = 200;
const CATCH_HIT = 0.45;
const CATCH_AT = releaseAt("catch", CATCH_HIT);
// Stood waiting on a pass this long (feet) or longer, out this far from
// the rim at the least (feet), he cuts to get open for it: a step in this
// far (feet) from this long before it goes (ms), and back out to meet it
// this far toward the man throwing it (feet).
const V_CUT_PASS = 14;
const V_CUT_OUT = 15;
const V_CUT_IN = 2.6;
const V_CUT_MS = 750;
const V_CUT_MEET = 1.4;
// Caught and away on the dribble: both hands on it this long first (ms),
// not out of the air straight down onto the floor.
const GATHER_MS = 180;
const pokeHand = (mirrored: boolean): V3 => {
	const q = poseAt("poke", POKE_HIT);
	const sk = skeleton(bodyOf(), mirrored ? mirrorPose(q) : q);
	return mirrored ? sk.armL.end : sk.armR.end;
};
// A dunk, for a typical player (taller ones jump less to get there, shorter
// ones more - see withBody): how high he gets (feet) - his hands well over
// the rim - and how far out from the middle of the rim he goes up.
const DUNK_LEAP = RIM_Z + 1.45 - standingReach(bodyOf());
const DUNK_FROM = 1.8;
// The furthest round to the side of the rim he goes up from (radians off
// straight in front of it) - for a dunk, or a finish off a post move.
const DUNK_SIDE = (55 * Math.PI) / 180;

// A spot clear behind the three-point line - his toes too - for a shooter
// at `p`, or nothing if he already is: out along the corner, or straight
// back from the rim round the arc.
const THREE_CLEAR = 1.5;
// Stood this near the line (feet) inside it, a man out there is on it - a
// shooter's spot: a drift along it takes him behind it, not along it with
// his toes over.
const ON_THE_LINE = 1;
// Out in the corner there is less than a stride between the line and the
// sideline: the corner man stands as wide as his feet stay in (see
// inPlay), and that is behind it - as a set's corner spot has him.
const CORNER_WIDE = COURT_H / 2 - SIDELINE_GAP;
const CORNER_BEHIND = Math.min(22 + THREE_CLEAR, spotXY("R_corner")[0]);
const behindArc = (team: Side, p: Pt): Pt | undefined => {
	const rim = { x: rimX(team), y: COURT_H / 2 };
	const depth = team === 0 ? p.x : COURT_W - p.x;
	if (depth < 14) {
		const across = Math.abs(p.y - rim.y);
		if (across >= CORNER_BEHIND) {
			return undefined;
		}
		const side = p.y >= rim.y ? 1 : -1;
		return onTheFloor({
			x: p.x,
			y: rim.y + side * Math.min(22 + THREE_CLEAR + 0.5, CORNER_WIDE),
		});
	}
	const d = dist(p, rim);
	if (d >= 23.75 + THREE_CLEAR) {
		return undefined;
	}
	// A real step, not a shuffle too small to take.
	const u = unitVec(rim, p);
	const r = 23.75 + THREE_CLEAR + 0.5;
	return onTheFloor({ x: rim.x + u.x * r, y: rim.y + u.y * r });
};
// Where a man out on the floor in play can stand: inside the lines (see
// inPlay) - and out wide, down in the corner, a step in from the baseline,
// not on it.
const CORNER_DEEP = 2;
const onTheFloor = (p: Pt): Pt => {
	const q = inPlay(p);
	return Math.abs(q.y - COURT_H / 2) > 8
		? { x: Math.min(COURT_W - CORNER_DEEP, Math.max(CORNER_DEEP, q.x)), y: q.y }
		: q;
};

const PASS_ACTS = new Set<AnimName>([
	"pass",
	"passBounce",
	"passOverhead",
	"passPush",
	"passBouncePush",
	"passBehind",
]);

// A step aside eases over and back this fast (ms each way) unless it says.
const NUDGE_RAMP = 450;

// The last of a list in time order begun by t (-1 if none yet).
const lastBy = <X>(list: X[], t: number, key: (x: X) => number): number => {
	let lo = 0;
	let hi = list.length - 1;
	let found = -1;
	while (lo <= hi) {
		const m = (lo + hi) >> 1;
		if (key(list[m]!) <= t) {
			found = m;
			lo = m + 1;
		} else {
			hi = m - 1;
		}
	}
	return found;
};

const passMs = (d: number) =>
	Math.min(1500, Math.max(220, PASS_BASE_MS + (d * 1000) / PASS_FTPS));

type PassStyle = "chest" | "bounce" | "overhead" | "lob";

// How a set's pass is thrown: a pocket pass or a post entry is often bounced,
// a skip or an outlet goes over the top.
const passStyleOf = (
	kind: string | undefined,
	rng: () => number,
): PassStyle | undefined => {
	switch (kind) {
		case "bounce":
			return "bounce";
		case "lob":
			return "lob";
		case "overhead":
		case "skip":
		case "outlet":
			return "overhead";
		case "pocket":
		case "dump_off":
		case "entry":
			return rng() < 0.6 ? "bounce" : "chest";
		case "chest":
		case "swing":
		case "kickout":
		case "inbound":
			return "chest";
		default:
			return undefined;
	}
};

// Whether the next step of a set (or the shot it springs) needs this man.
const needed = (
	run: { option?: PlayOption },
	next: PlayAction[] | undefined,
	who: Role,
): boolean =>
	run.option?.shooter === who ||
	run.option?.assist === who ||
	(next ?? []).some((a) =>
		a.type === "screen"
			? a.who.includes(who) || a.for === who
			: a.who === who ||
				((a.type === "pass" || a.type === "handoff") && a.to === who),
	);

// How far along a run he is, as a share of it, u of the way through its
// time: getting going and pulling up as the evaluator has him (see
// motion.ts) - from and to the pace it says, where it says, from and to a
// standstill where not. (Worked out once a run, again if its times change.)
const runShapes = new WeakMap<Move, { t0: number; t1: number; r: RunShape }>();
const shareAlong = (m: Move, u: number): number => {
	const L = dist(m.from, m.to);
	if (L < 1e-6) {
		return u;
	}
	let got = runShapes.get(m);
	if (!got || got.t0 !== m.t0 || got.t1 !== m.t1) {
		got = {
			t0: m.t0,
			t1: m.t1,
			r: runShape(L, (m.t1 - m.t0) / 1000, m.v0 ?? 0, m.v1 ?? 0),
		};
		runShapes.set(m, got);
	}
	return alongShape(got.r, u * got.r.T) / L;
};

// How fast each cut and each dribble in a set goes, feet per second: the
// tracking numbers (a curl off a screen about 16, a pick-and-pop about 11),
// sped up like the rest of the court so a set reads in the time it has - but
// never past what the fastest men in the league reach.
// The man trailing a break - the big who got the rebound and threw the
// outlet - runs the floor behind it (feet a second).
const TRAIL_SPEED = 16;
// Coming off a screen, he gets to it this long (ms) after it is set - and
// goes by it this far (feet) off the screener's middle: at his shoulder.
const SCREEN_BY = 150;
const SCREEN_SHOULDER = 2;
const MOVE_SPEED: Record<string, number> = {
	walk: 6,
	jog: 12,
	sprint: 22,
	v_cut: 19,
	backdoor: 21,
	curl: 19,
	flare: 17,
	fade: 14,
	pop: 14,
	roll: 19,
	short_roll: 16,
	slip: 19,
	lift: 13,
	drift: 12,
	rip_cut: 20,
	shallow_cut: 18,
	iverson_cut: 21,
	flash: 19,
	seal: 9,
	relocate: 13,
	clear_out: 17,
};
const DRIBBLE_SPEED: Record<string, number> = {
	advance: 16,
	// Up the floor on the break: as fast as a man goes with the ball.
	push: 20,
	attack: 19,
	drive_baseline: 20,
	drive_middle: 20,
	reject: 19,
	snake: 14,
	retreat: 9,
	hesitation: 12,
	crossover: 16,
	step_back: 10,
};
// Screens set for the man with the ball.
const BALL_SCREENS = new Set([
	"ball",
	"step_up",
	"drag",
	"double_drag",
	"spain",
	"ghost",
]);
// How many moves he puts on his man getting by him off the dribble, for
// a draw k: one close in already (d feet to go), else one to three.
const crossCount = (d: number, k: number): number =>
	d < 10 || k < 0.6 ? 1 : k < 0.85 ? 2 : 3;
// The shortest a step of a set takes, milliseconds.
const STEP_MIN = 600;
// A handoff taken on the move, at most this fast (feet a second).
const HANDOFF_PACE = 9;
// Run out on the break: a trip that gets its shot up within this long (sim
// seconds) of a defensive board, a steal, or a field goal at the other end.
// In the league a third of the trips off a board end inside six seconds, and
// most of those off a steal - but only a few off a basket (see BREAK_SHARE in
// nbaRates.ts). The sim's trips run longer than the league's, so it is the
// same share of its quickest ones that break: its fastest third off a board,
// not quite half off a steal (more would race the clock), one in sixteen off
// a basket.
const BREAK_GAP = { board: 9.6, steal: 11, make: 7.1 };
// A trip ending in a turnover or a whistle sooner than this (seconds of the
// sim's clock) has no time for the ball to be brought up and a set run.
const RUSH_GAP = 6;
// A shot this soon (game clock seconds) after the ball is inbounded in the
// frontcourt comes straight off the catch.
const QUICK_IN = 2.5;
// The ball a long way back from where the set wants it (feet): the man with
// it brings it up to about this far from there before he gives it up, and
// never from farther out from the rim than this.
const ENTRY_FAR = 34;
const ENTRY_PASS = 18;
const ENTRY_DEEP = 34;
// Flat out up the floor with it, a heave to beat the buzzer (feet a second).
const HEAVE_RUN = 19;
// Coming off a ball screen with the man who set it right in his way, he
// goes round him this far off (feet) - the ball on the far side.
const ROUND_SCREEN = 2.0;
// A ball screen still there for him to use: set no longer ago than this
// (ms) as he sets off, and the man who set it this near him (feet).
const SCREEN_FRESH = 2000;
const SCREEN_HERE = 9;
// And by where the shot came from: a break ends at the rim far more often
// than a trip does, seldom with a floater and hardly ever with a pull-up from
// mid-range (see BREAK_SHARE) - so a trip ending at the rim was a break on a
// longer clock than one ending in the paint short of it. The sim's clock
// can't run far ahead of the picture: never over 12 seconds.
const BREAK_FROM: Record<Zone, number> = {
	atRim: 2.4,
	lowPost: -1.3,
	midRange: -2.6,
	three: 0.2,
	tipIn: 0,
	putBack: 0,
};
// How far a defender leaves his man to help (feet): any farther and he
// could not get back out to him.
const HELP_REACH: [number, number] = [8, 14];
// On the break, the most (feet) a man already up the floor goes back down
// it to his spot.
const BREAK_NO_BACK = 4;
// ... unless he is in under the rim, or would be once over to its side of
// the floor: this near it (feet).
const BREAK_DEEP = 12;
// The sets there are to run with the ball over half court already: none
// that starts with it back over (see setsFor).
const OVER_SETS = PLAYS.filter(
	(p) => spotXY(p.start[p.ball]!)[1] <= COURT_W / 2 - RIM_INSET,
);
// How far up the floor (feet out from the rim) an off-ball defender goes
// with his man: out to the top of the key, a step past the arc. For every
// foot his man is further out than that, he waits this much (feet) further
// in - up to this far.
const UP_FLOOR = 27;
const SINK_PER_FT = 0.5;
const SINK_MAX = 9;
// The man with the ball a long way back up the floor - taking it out under
// the other basket, bringing it up - is picked up about three-quarter court
// (feet from the rim defended), not chased down to the far baseline.
const PICK_UP = 60;
// On a break, though, the defense running back, he stops the ball at about
// half court: not back out to three-quarter court for him from the paint.
const BREAK_PICK_UP = 38;
// Lining up with the ball dead, everybody where he has to be.
const DEAD_SETUPS =
	/^(jumpBall|ft|missFt|sub|timeout|endOfPeriod|period|overtime|gameOver)/;
// The last of the clock, ahead, dribbled out at real speed at the end of a
// game (ms) - and, behind, the last look for a shot that never comes.
const FINAL_DRIBBLE = 3600;
// How high the man with it at the horn, winning, throws it (feet).
const GAME_BALL_UP = 27;
const FINAL_LOOK = 1600;
// How long a defender takes to read a pass (ms), and how fast he closes out
// once he has (feet a second) - breaking down for the last steps.
const CLOSE_READ = 150;
const CLOSE_RUN = 21;
const CLOSE_CHOP = 8;
const CLOSE_BREAK = 3.5;
// How much room a jump shooter gets (by how far off him his man meets the
// shot, feet): right on him, an arm's length, a step late - or none of it,
// left all alone. Not every jumper has a hand in his face: the odds of each
// (tight, contested, open, wide open), on a make and on a miss.
const ROOM_FT = [2.2, 3.2, 4.8, 6.8] as const;
const ROOM_MADE = [0.05, 0.32, 0.36, 0.27] as const;
const ROOM_MISSED = [0.09, 0.42, 0.33, 0.16] as const;
// (A three, more often open: nearly half of them with nobody within six
// feet, as the league's tracking has them.)
const ROOM_MADE_3 = [0.01, 0.07, 0.34, 0.58] as const;
const ROOM_MISSED_3 = [0.02, 0.12, 0.38, 0.48] as const;
// Left all alone off a pass, his man had sunk in to help as it was swung
// to him: as far off him as he can get back out from in the time it takes
// - though no farther than this (feet) - and there from this long before
// the pass went (ms), at this pace (feet a second).
const HELP_MAX = 17;
const HELP_EARLY = 900;
const HELP_FTPS = 11;
// A step late: that much longer before he is there (ms), still on his way
// as it goes up.
const CLOSE_LATE = [0, 0, 260] as const;
// Leaving him all alone, way late: still ROOM_FT[3] off him as it goes up,
// and out there up to this long after (ms) - flying at him, a hand up as
// it goes over, or never there in time even for that.
const OPEN_LATE = 300;
// All alone only if his man is this far off him already (feet).
const ALONE_FT = 9;
// A hand up at the shot from no farther off than this (feet).
const CONTEST_FT = 6;
// The finishes in close - and the shots over a man right on him in the
// post (see wallUp).
const CLOSE_FINISHES = new Set<AnimName>([
	"layup",
	"fingerRoll",
	"powerLayup",
	"scoop",
	"reverseLayup",
	"floater",
	"hook",
	"fade",
]);
// Anybody else of theirs this near (feet) a shot in close goes up with his
// arms high (see shoot).
const WALL_FT = 4.5;
// How close (feet, middle to middle) two men get before they are into each
// other: a screen, a post-up.
const BODY = 1.85;
// The lanes up the floor (feet across it): out wide, in between, down the
// middle. Two men never run the one lane, nor one the ball's - this far off
// it at the least; and a man takes one across the floor from his side
// only if it is this much nearer what he wants (feet).
const LANES = [6, 15, 21.5, 28.5, 35, 44] as const;
const LANE_OFF_BALL = 7;
const LANE_ACROSS = 8;
// A run at least this far up the floor (feet) is run in a lane.
const LANE_RUN = 30;
// A trailer's way up to the top of the play keeps this far (feet) off the
// man bringing the ball up, if it can.
const TRAIL_CLEAR = 8;
// A man this far from the rim (feet) or farther, with the ball well in
// front of him in the front court, is left back up the floor too (see
// trailUp) - stood out at half court.
const TRAIL_DEEP = 36;
// Where a trailer comes up to (see trailSpot): this far out from the rim
// (feet) at the top of the key, and this much less out in a slot.
const TRAIL_OUT: [number, number] = [25, 27.5];
const TRAIL_SLOT_IN = 2.5;

// How far along a run (0 to 1) a man is `u` of the way through its `ms`:
// setting off from a standstill and pulling up at the end, the way the
// court runs it (see alongRun in evaluate.ts) - or, already going at one
// end or going on at the other, at his pace there.
const runProgress = (
	ms: number,
	u: number,
	setsOff = true,
	pullsUp = true,
): number => {
	const T = ms / 1000;
	if (T <= 0.001) {
		return u;
	}
	const ta = setsOff ? Math.min(T / 3, 0.45) : 0;
	const td = pullsUp ? Math.min(T / 3, 0.38) : 0;
	const vc = 1 / (T - ta / 2 - td / 2);
	const ramp = (x: number) => x * x * x - (x * x * x * x) / 2;
	const s = Math.min(T, Math.max(0, u * T));
	const d =
		s < ta
			? vc * ta * ramp(s / ta)
			: s < T - td
				? (vc * ta) / 2 + vc * (s - ta)
				: (vc * ta) / 2 + vc * (s - ta) - vc * td * ramp((s - (T - td)) / td);
	return Math.min(1, Math.max(0, d));
};
// How far through a run of `ms` (0 to 1) he is `f` of the way along it.
const timeTo = (ms: number, f: number): number => {
	let lo = 0;
	let hi = 1;
	for (let k = 0; k < 24; k++) {
		const mid = (lo + hi) / 2;
		if (runProgress(ms, mid) < f) {
			lo = mid;
		} else {
			hi = mid;
		}
	}
	return (lo + hi) / 2;
};
// How far a run goes, start to end (feet).
const lenOf = (m: Move): number => dist(m.from, m.to);
// How far (feet) from Q the way u he first comes within r of the rim -
// none if he is already, never if he never does.
const alongTo = (Q: Pt, u: Pt, rim: Pt, r: number): number => {
	const w = { x: Q.x - rim.x, y: Q.y - rim.y };
	const b = w.x * u.x + w.y * u.y;
	const c = w.x * w.x + w.y * w.y - r * r;
	if (c <= 0) {
		return 0;
	}
	return b * b - c < 0 ? Infinity : Math.max(0, -b - Math.sqrt(b * b - c));
};
// A man set where he stands, for nobody to run through: in a screen, or
// sealing in the post.
type Body2 = { team: Side; t0: number; t1: number; at: Pt; post: boolean };
// What a man does standing where he is - set in a screen or a post-up,
// celebrating, having words - and stops doing once he moves.
const IN_PLACE = new Set<AnimName>([
	"screen",
	"postUp",
	"protest",
	"hips",
	"point",
	"flex",
	"roar",
	"chestPound",
	"celebrate",
	"clap",
	"highFive",
	"lowFive",
	"chestBump",
	"waitFive",
]);
// Of those, what he does reacting to something - having words, a flex,
// hands on his hips - rather than set for a teammate.
const REACTIONS = new Set<AnimName>(
	[...IN_PLACE].filter((a) => a !== "screen" && a !== "postUp"),
);
// How far round the arc from straight out a man spacing the floor goes
// (radians): into the corner, and no farther.
const ARC_EDGE = 1.62;
// Inside the arc, a man this near (feet) where a drive ends, or this near its
// way there, gets out of it.
const DRIVE_ROOM = 7;
const DRIVE_LANE = 3.5;
// Where a man who has passed out of the lane gets back out to.
// The ways round the five may take the motion set's spots (see fitSpots):
// the point his own; the two wings either way; the two bigs either way.
const SPOT_ORDERS = [
	[0, 1, 2, 3, 4],
	[0, 2, 1, 3, 4],
	[0, 1, 2, 4, 3],
	[0, 2, 1, 4, 3],
];
// Every way of putting n men with n others.
const permsOf = (n: number): number[][] => {
	const out: number[][] = [];
	const walk = (left: number[], acc: number[]) => {
		if (left.length === 0) {
			out.push(acc);
		}
		for (const x of left) {
			walk(
				left.filter((y) => y !== x),
				[...acc, x],
			);
		}
	};
	walk(
		Array.from({ length: n }, (_, i) => i),
		[],
	);
	return out;
};
const PERMS4 = permsOf(4);
const PERMS5 = permsOf(5);
// What a defender picking up a man a position away from his own costs, in
// feet of running (see matchUp).
const MATCH_RANK = 7;
// ... and on a break, matched up on the fly.
const BREAK_MATCH_RANK = 2;
// What every foot a man has to go to get to his part in a set costs, against
// how badly he fits it (see castPlay): thirty feet, about a guard's part
// played by a forward.
const TRAVEL_COST = 0.035;
// What it costs to have a man who is no ball handler bring it up himself as
// the set's man with the ball, when it is in his hands back down the floor -
// more than any other fit or any run to his spot (see castNearest).
const OUTLET_COST = 4;
// Two going farther than this (feet) to their parts across each other's
// path trade them, if that costs the set no more than this (see castPlay).
// Out of his own board to an open spot, he goes where the ball can come back
// to him in a pass about this long (feet), not one thrown across the floor.
const KICK_BACK = 24;
const CROSS_RUN = 15;
const CROSS_TRADE = 0.7;
// Whether the runs from a to b and from c to d cross.
const crossing = (a: Pt, b: Pt, c: Pt, d: Pt): boolean => {
	const o = (p: Pt, q: Pt, r: Pt) =>
		Math.sign((q.x - p.x) * (r.y - p.y) - (q.y - p.y) * (r.x - p.x));
	return o(a, b, c) !== o(a, b, d) && o(c, d, a) !== o(c, d, b);
};
const RESPACE_SPOTS = [
	"L_corner",
	"R_corner",
	"L_wing",
	"R_wing",
	"L_slot",
	"R_slot",
	"top",
];
// And a big in close, round the rim.
const BIG_SPOTS = [
	"L_dunker",
	"R_dunker",
	"L_short_corner",
	"R_short_corner",
	"L_elbow",
	"R_elbow",
	"high_post",
];

// Past a line: out of bounds.
const outOfPlay = (p: Pt) =>
	p.x < 0 || p.x > COURT_W || p.y < 0 || p.y > COURT_H;
// The furthest past a line a loose ball ends up (feet): into the front row,
// no further.
const OUT_CAP = 4;

// A way off the rim a rebound really goes: out in front of it, or off to
// its side - never back through the glass behind it (at most this far round
// from straight out, radians).
const OFF_RIM_SIDE = (75 * Math.PI) / 180;
const offRimWay = (u: Pt, dir: 1 | -1): Pt =>
	-dir * u.x >= Math.cos(OFF_RIM_SIDE)
		? u
		: {
				x: -dir * Math.cos(OFF_RIM_SIDE),
				y: (u.y < 0 ? -1 : 1) * Math.sin(OFF_RIM_SIDE),
			};

const unitVec = (from: Pt, to: Pt): Pt => {
	const dx = to.x - from.x;
	const dy = to.y - from.y;
	const l = Math.hypot(dx, dy) || 1;
	return { x: dx / l, y: dy / l };
};

// A number from 0 to 1 that depends only on a and b (whole numbers, or
// rounded to them) - the same on every device.
const hash01 = (a: number, b: number): number => {
	let h =
		Math.imul(Math.round(a) | 0, 0x9e3779b1) ^
		Math.imul(Math.round(b) | 0, 0x85ebca77);
	h = Math.imul(h ^ (h >>> 16), 0x7feb352d);
	h = Math.imul(h ^ (h >>> 15), 0x846ca68b);
	return ((h ^ (h >>> 16)) >>> 0) / 4294967296;
};

// A shooter's own stroke: his jumper takes this much of the usual time,
// from the quickest to the slowest.
const FORM_LO = 0.93;
const FORM_HI = 1.07;

// Nobody of theirs within this far of a shooter as it left his hands
// (feet): he was left alone for it.
const WIDE_OPEN = 8;

// LEFT-HANDERS. About one man in ten shoots it with his left - the same
// men every game. A jumper, a hook, a one-handed dunk go up with his own
// hand; a layup with the outside hand, the one on the side of the rim he
// comes in on: a left-hander's own nearly always, a right-hander's left
// as often as not on his left (see shootsLeft).
const LEFTIES = 0.1;
const OWN_HAND = new Set<AnimName>([
	"shoot",
	"setShot",
	"follow",
	"fade",
	"shotFake",
	"hook",
	"floater",
	"dunk1",
	"tomahawk",
	"windmill",
]);
const OUTSIDE_HAND = new Set<AnimName>(["layup", "fingerRoll", "scoop"]);
// Up with his own shot, coming down: nobody goes up for the rebound off it
// on top of him.
const UP_WITH_SHOT = new Set<AnimName>([
	"shoot",
	"setShot",
	"fade",
	"hook",
	"floater",
	"layup",
	"fingerRoll",
	"powerLayup",
	"scoop",
	"reverseLayup",
	"dunk",
	"dunk1",
	"tomahawk",
	"windmill",
]);
// A man of theirs in his way in gives way this far off him (feet, middle to
// middle) - from this long (ms) before he goes up - taking this long (ms)
// about the step aside.
const GIVE_WAY = 1.5;
const GIVE_WAY_BEFORE = 400;
const GIVE_WAY_MS = 300;
// Going up with a shot in close: off the glass or over the rim.
const UP_IN_CLOSE = new Set<AnimName>([
	"layup",
	"fingerRoll",
	"powerLayup",
	"scoop",
	"reverseLayup",
	"floater",
]);
// (Worked out from where the schedule has them - which the picture, a
// stride at a time, can have up to half a foot or so nearer: that much more.)
const GIVE_WAY_SLACK = 0.5;
// How near him (feet, middle to middle) a man going up for that rebound gets
// - round him from where he would have taken it, by no more than this (feet
// for every second it is in the air to him).
const OFF_HIS_SHOT = 1.5;
const BOARD_SHIFT_FTPS = 2.5;
// (From this far either side of the rim, feet, he is on that side of it.)
const RIM_SIDE = 2;

// How the offense gets from wherever the ball is into its next set: off an
// inbound, on a break straight off the rebound or the steal, or the five
// flowing into it as the ball comes up.
type Entry = "inbound" | "break" | "flow";

// How a ball screen is played: the big sits back in the paint, the two
// defenders swap men, the big jumps out and recovers, or both go at the ball.
// Off the ball the man coming off the screen is chased, or switched onto.
type Coverage = "drop" | "switch" | "hedge" | "blitz" | "chase";

// A set being run.
type Running = {
	play: Play;
	team: Side;
	// The pid in each role.
	roles: number[];
	mirror: 1 | -1;
	// What it ends in: a shot, or a turnover.
	option?: PlayOption;
	risk?: PlayRisk;
	// The first step run: the five go straight to where the steps before it
	// would have put them (the trip up the floor, run through fast), and the
	// set picks up at the action that makes the play.
	from: number;
	// How much clock the trip took (seconds), when known.
	gap?: number;
	// A little give in every spot, so no two trips down look stamped out.
	jitter: Map<string, Pt>;
	// Spots taken on the far side of the floor from the set's (see
	// playDribble).
	across?: Set<string>;
	// Defenders the play-by-play has at the shot - the shot blocker, the man
	// who fouls him, the one he dunks on - who work their way to it.
	help?: number[];
	// When the step before this one got going.
	stepAt?: number;
	// The screen set in the step before, for the defense to play.
	screen?: {
		screeners: number[];
		user: number;
		ball: boolean;
		at: Pt;
		step: number;
		coverage: Coverage;
		// When the first of them is planted in it.
		set: number;
		// A handoff he took on the move, to go on with: when he had it.
		handed?: number;
	};
};

type ShotStyle =
	| "plain"
	| "crossover"
	| "euro"
	| "stepBack"
	| "fade"
	| "post"
	| "hook"
	| "floater";

type PostMove = "hook" | "fade" | "dropStep" | "upUnder";

type Phase =
	| "start"
	| "tip"
	| "inboundBase"
	| "inboundSide"
	| "loose"
	| "set"
	| "ft"
	| "dead"
	| "shootout";

type ShotPlan = {
	kind: "make" | "miss" | "block" | "foul";
	assist?: number;
	blocker?: number;
	fouler?: number;
	// How the line describes the finish, when it says.
	finish?: Finish;
	// Who he dunked on, and who lobbed it to him.
	defender?: number;
	lobber?: number;
	// Who comes down with it, if it misses.
	rebounder?: number;
};

// A shot played out at the rim (see physics.ts): from when, and how - and
// when (ms in) its line shows: as it drops through, or off the iron the last
// time.
type AtRim = { t0: number; found: FoundShot; line: number; falls?: Pt };

// A spot out at the edge of the room round a dunk (see keepClear) - moved
// off to the side of the way the dunker comes in, if it is on it: a man
// who trailed him in isn't stood where he comes through on his last
// strides, to be shoved on in ahead of him, into the dunk. To the side it
// was on; on the line, the side the room is cleared to.
const offTheLane = (B: Pt, c: { at: Pt; out: Pt; r: number; lane: Pt }): Pt => {
	const { at, lane } = c;
	const along = (B.x - at.x) * lane.x + (B.y - at.y) * lane.y;
	const off = (B.x - at.x) * -lane.y + (B.y - at.y) * lane.x;
	if (along <= 0 || Math.abs(off) >= POSTER_LANE) {
		return B;
	}
	const side =
		Math.abs(off) > 0.1
			? Math.sign(off)
			: c.out.x * -lane.y + c.out.y * lane.x >= 0
				? 1
				: -1;
	const a = Math.max(
		along,
		Math.sqrt(Math.max(0, c.r * c.r - POSTER_LANE * POSTER_LANE)),
	);
	return clampPt({
		x: at.x + lane.x * a - lane.y * side * POSTER_LANE,
		y: at.y + lane.y * a + lane.x * side * POSTER_LANE,
	});
};

class Director {
	T = 0;
	private readonly rng: () => number;
	readonly tracks = new Map<number, Track>();
	private readonly pos = new Map<number, Pt>();
	private readonly face = new Map<number, 1 | -1>();
	private readonly free = new Map<number, number>();
	private readonly team = new Map<number, Side>();
	private readonly rank = new Map<number, number>();
	private readonly skills = new Map<number, string[]>();
	private readonly injuries = new Map<
		number,
		{ type: string; games: number }
	>();
	// Where each player's chair is on his bench.
	private readonly seat = new Map<number, Pt>();
	private readonly lineup: [number[], number[]] = [[], []];
	readonly ball: BallSeg[] = [];
	readonly fx: Fx[] = [];
	readonly beats: Beat[] = [];
	readonly poss: [number, Side][] = [];
	readonly cuts: number[] = [];
	// The cuts between one clip of a highlight reel and the next.
	readonly clips: number[] = [];
	readonly tension: [number, number][] = [];
	readonly seats: [number, number][] = [];
	// Each with how short it may be and still be run through (ms; see
	// hurried).
	readonly fast: [number, number, number?][] = [];
	// When each break got going: a break is played at real speed.
	private readonly breaks: number[] = [];
	// Each jump ball, from the players taking their places until it is
	// tipped: nobody is on offense or defense yet, and nobody sets off
	// before the tip - how they stood or went would give away who won it.
	readonly jumps: [number, number][] = [];
	readonly checkIns: CheckIn[] = [];
	// Each man subbed out: who came on for him, when he went off, when the
	// man coming on was out there, and when he came back on himself (see
	// mark).
	private readonly replaced = new Map<
		number,
		{ by: number; from: number; on: number; until: number }[]
	>();
	// Room kept round a man going up for a dunk, for the one he dunks on:
	// nobody else of theirs inside it from t0 to t1 (see keepClear).
	private readonly clears: {
		team: Side;
		at: Pt;
		out: Pt;
		r: number;
		t0: number;
		t1: number;
		except: number;
		// The way the dunker comes in to `at`: out from it, back along his
		// way (unit).
		lane: Pt;
		// Where and by when the man he dunks on meets him, facing him - and
		// where the dunker comes down.
		meet: { at: Pt; by: number; face: 1 | -1; dunker: Pt };
	}[] = [];
	// The period being played, and whether it is overtime.
	private periodNo = 1;
	private overtime = false;
	// `stretch`: may run on to the next cut (see finish).
	readonly shots: (ArenaShot & { stretch?: boolean })[] = [];
	// The starting lineups, when the game opens with them (see intro.ts).
	intro: Intro | undefined;
	private readonly introKind: "regular" | "playoffs" | undefined;
	private readonly stakes: Stakes | undefined;
	// Each side's starters in the order they are called.
	private readonly introOrder: [number[], number[]] = [[], []];
	// When the lineups started being called, until the tip they lead to.
	private lineupsFrom: number | undefined;

	// The ball at the end of everything scheduled so far - and which hand it
	// is in, while he dribbles.
	private holder: number | undefined;
	private ballHand: Hand = "R";
	private ballHandOf: number | undefined;
	private ballAt: Pt3 = { x: COURT_W / 2, y: COURT_H / 2, z: 4 };
	private offense: Side = 1;
	private phase: Phase = "start";
	private inboundAt: Pt | undefined;
	private motion = 0;
	private motionTeam: Side | undefined;
	// The way round the five take the spots of the set they settle into this
	// trip - mirrored side to side or not, and which wing and which of the
	// two spots for the bigs each takes (`order`: the slot whose part each
	// plays) - fitted to where they are when it starts (see fitSpots).
	private spotFit?: { team: Side; flip: boolean; order: number[] };
	private lastClock: number | undefined;
	private postUps = 0;
	// The man the ball just went in to off an inbound with no time for
	// anything but his shot (see develop).
	private quickIn?: number;
	// Set by a shot attempt that is still in the air, for its result to pick up.
	private pending:
		| {
				pid: number;
				team: Side;
				target: Pt3;
				dunk: boolean;
				finish?: Finish;
				zone: Zone;
				arrive: number;
				rim?: AtRim;
		  }
		| undefined;
	private readonly score: [number, number] = [0, 0];
	// Who is guarding whom this possession, once a switch has changed it from
	// position against position.
	private readonly guarding = new Map<number, number>();
	// The runs that take a defender to where his man has him be, and whose man
	// that is: in the end (see mark) he follows his man every step of the way.
	private readonly marking = new Map<Move, number>();
	// Shot fakes on the floor: who, when, and where.
	private readonly fakes: { pid: number; t: number; at: Pt }[] = [];
	// Shots in close, for whoever of theirs is right there to go up with his
	// arms high (see wallUp).
	private readonly walls: { shooter: number; release: number }[] = [];
	// The little shoves of a battle for position (see jostle): cut short
	// when he comes out of it (see letGo).
	private readonly jostles = new Set<Move>();

	private readonly events: RawEvent[];
	private readonly gid: number | undefined;
	private readonly gender: "female" | "male";

	constructor(
		events: RawEvent[],
		players: CourtPlayer[],
		gid: number | undefined,
		gender: "female" | "male",
		intro?: "regular" | "playoffs",
		stakes?: Stakes,
	) {
		this.events = events;
		this.gid = gid;
		this.gender = gender;
		this.introKind = intro;
		this.stakes = stakes;
		this.rng = makeCourtRng(`court|${gid ?? 0}`);
		const seats: [number, number] = [0, 0];
		for (const p of players) {
			this.team.set(p.pid, p.team);
			this.rank.set(p.pid, POS_RANK[p.pos ?? ""] ?? 4);
			if (p.skills) {
				this.skills.set(p.pid, p.skills);
			}
			if (p.injury) {
				this.injuries.set(p.pid, p.injury);
			}
			this.seat.set(p.pid, seatSpot(p.team, seats[p.team]++));
		}

		// Starters are announced as "gs" stats ahead of the first line.
		const starters: [number[], number[]] = [[], []];
		for (const e of events) {
			if (isLineItem(e)) {
				break;
			}
			if (e.type === "stat" && e.s === "gs" && typeof e.pid === "number") {
				const t = this.team.get(e.pid);
				if (t !== undefined) {
					starters[t].push(e.pid);
				}
			}
		}
		for (const t of [0, 1] as const) {
			if (starters[t].length === 0) {
				starters[t] = players
					.filter((p) => p.team === t)
					.slice(0, 5)
					.map((p) => p.pid);
			}
			this.lineup[t] = starters[t];
		}

		// With the lineups to call, the starters wait in front of their
		// benches.
		if (intro) {
			for (const t of [0, 1] as const) {
				this.introOrder[t] = callOrder(
					this.lineup[t].map((pid) => ({
						pid,
						pos: players.find((p) => p.pid === pid)?.pos,
					})),
				).map((p) => p.pid);
			}
		}
		for (const p of players) {
			const on = this.lineup[p.team].includes(p.pid);
			const slot = on ? this.slots(p.team).indexOf(p.pid) : 0;
			const called = this.introOrder[p.team].indexOf(p.pid);
			const start = !on
				? this.seatOf(p.pid)
				: called >= 0
					? tunnelSpot(p.team, called)
					: { x: COURT_W / 2 + (p.team === 1 ? -9 : 9), y: 9 + slot * 8 };
			this.tracks.set(p.pid, {
				pid: p.pid,
				team: p.team,
				start,
				moves: [],
				acts: [],
				arms: [],
				faces: [[-Infinity, attackDir(p.team)]],
				looks: called >= 0 ? [[-Infinity, { x: start.x, y: COURT_H / 2 }]] : [],
				shown: [[-Infinity, on]],
			});
			this.pos.set(p.pid, start);
			this.face.set(p.pid, attackDir(p.team));
			this.free.set(p.pid, 0);
		}
		this.ball.push({ kind: "rest", t0: -Infinity, at: { ...this.ballAt } });
		this.poss.push([-Infinity, 1]);
	}

	// ---- primitives ---------------------------------------------------------

	private track(pid: number): Track | undefined {
		return this.tracks.get(pid);
	}

	private posOf(pid: number): Pt {
		return this.pos.get(pid) ?? { x: COURT_W / 2, y: COURT_H / 2 };
	}

	// Run/walk a player somewhere, after whatever he is already doing. Returns
	// when he gets there.
	private go(
		pid: number,
		to: Pt,
		t0: number,
		speed: number,
		anim: AnimName = "run",
		face?: 1 | -1,
		// How hard he goes about it (BURST: all out, for a loose ball), and
		// the pace he goes straight on at into whatever comes next, if he
		// does not pull up at the end of it.
		how: { effort?: number; onto?: number } = {},
	): number {
		const tr = this.track(pid);
		if (!tr) {
			return t0;
		}
		const from = this.posOf(pid);
		const start = Math.max(t0, this.free.get(pid) ?? 0);
		const d = dist(from, to);
		if (d < 0.3) {
			if (face !== undefined) {
				this.turn(pid, start, face);
			}
			return start;
		}
		// Taking it a long way back out - out of the lane, up out of the
		// corner, back to the top to set up - he turns and dribbles it out:
		// a retreat dribble is a couple of steps, not a backpedal all the way
		// out with the ball, facing the rim he is leaving.
		if (
			face !== undefined &&
			(anim === "dribble" || anim === "dribbleWalk") &&
			-(to.x - from.x) * face > RETREAT_MOST &&
			-(to.x - from.x) * face > d * 0.4
		) {
			face = undefined;
		}
		// As long as it takes him to get going, get there and pull up.
		const dur = Math.max(
			240,
			runMs(
				d,
				speed,
				this.carried(pid, to, start, speed),
				how.effort ?? 1,
				how.onto ?? 0,
			),
		);
		const f =
			face ??
			(Math.abs(to.x - from.x) > 0.4
				? to.x > from.x
					? 1
					: -1
				: (this.face.get(pid) ?? 1));
		tr.moves.push({
			t0: start,
			t1: start + dur,
			from: { ...from },
			to,
			anim,
			...(face === undefined ? {} : { face }),
		});
		tr.faces.push([start, f]);
		this.pos.set(pid, to);
		this.face.set(pid, f);
		this.free.set(pid, start + dur);
		return start + dur;
	}

	// Room round a dunk (see clears): anybody of theirs following his man
	// in there - the man beaten on the way in, a rotation a beat late - is
	// held at the edge of it, a step off, until it is over.
	private keepClear() {
		for (const c of this.clears) {
			for (const tr of this.tracks.values()) {
				if (tr.team !== c.team || tr.pid === c.except) {
					continue;
				}
				let inside = false;
				for (let t = c.t0; t <= c.t1 && !inside; t += 50) {
					inside = dist(this.posAt(tr.pid, t), c.at) < c.r;
				}
				if (!inside) {
					continue;
				}
				let a = c.t0 - 300;
				let A = this.posAt(tr.pid, a);
				const u = dist(A, c.at) > 1 ? unitVec(c.at, A) : c.out;
				const B = offTheLane(
					clampPt({ x: c.at.x + u.x * c.r, y: c.at.y + u.y * c.r }),
					c,
				);
				// Out to the edge of it, and back on his way after, no faster
				// than a man can go.
				for (let n = 0; n < 3; n++) {
					const need = c.t0 - 50 - Math.max(250, runMs(dist(A, B), SPRINT));
					if (need >= a) {
						break;
					}
					a = need;
					A = this.posAt(tr.pid, a);
				}
				let z = c.t1 + 300;
				let Z = this.posAt(tr.pid, z);
				for (let n = 0; n < 3; n++) {
					const need = c.t1 + Math.max(300, runMs(dist(B, Z), SPRINT));
					if (need <= z) {
						break;
					}
					z = need;
					Z = this.posAt(tr.pid, z);
				}
				const kept: Move[] = [];
				for (const m of tr.moves) {
					if (m.t1 <= a || m.t0 >= z) {
						kept.push(m);
					} else if (m.t0 < a) {
						kept.push({ ...m, t1: a, to: { ...A }, v1: undefined });
					} else if (m.t1 > z) {
						kept.push({ ...m, t0: z, from: { ...Z }, v0: undefined });
					}
				}
				kept.push(
					{ t0: a, t1: c.t0 - 50, from: { ...A }, to: B, anim: "run" },
					{ t0: c.t1, t1: z, from: { ...B }, to: { ...Z }, anim: "run" },
				);
				tr.moves = kept.sort((x, y) => x.t0 - y.t0);
			}
			this.meetDunk(c.except, c.meet);
		}
	}

	// The man dunked on is there for it, set, wherever following his man
	// took him in the meantime: he leaves for it in time to get there flat
	// out, if he has to.
	private meetDunk(
		pid: number,
		meet: { at: Pt; by: number; face: 1 | -1; dunker: Pt },
	) {
		const tr = this.track(pid);
		if (!tr) {
			return;
		}
		// (There for it already, he has no way to go in - but down again he
		// steps back all the same: not stood under the rim, the ball coming
		// down through the net onto him.)
		const there = dist(this.posAt(pid, meet.by), meet.at) < 0.5;
		const { by } = meet;
		let t0 = there ? by : by - 600;
		for (let n = 0; n < 3 && !there; n++) {
			const need = runMs(dist(this.posAt(pid, t0), meet.at), SPRINT) + 150;
			if (by - t0 >= need) {
				break;
			}
			t0 = by - need;
		}
		const P = this.posAt(pid, t0);
		const V = there ? P : meet.at;
		// Back from the rim: there through whatever he goes up for at it,
		// then flat out to where he was to be - picking up where following
		// his man takes him at the first moment a run from the rim gets him
		// there, not off across the floor in the blink of an eye.
		// (The block he goes up for begins just after he is there.)
		let leave = by;
		for (const a of tr.acts) {
			if (a.t0 <= by + 300 && a.t1 > leave && a.t1 < by + 1500) {
				leave = a.t1;
			}
		}
		// Down again, he gives the man who dunked on him room: a step or so
		// out from him, the way he is from him - not stood in his face while
		// he lets everybody know about it.
		const u = dist(meet.dunker, V) > 0.3 ? unitVec(meet.dunker, V) : undefined;
		const R = u
			? clampPt({
					x: meet.dunker.x + u.x * DUNKED_ON_ROOM,
					y: meet.dunker.y + u.y * DUNKED_ON_ROOM,
				})
			: V;
		const stepped =
			dist(V, R) > 0.3 ? leave + runMs(dist(V, R), 9, 0, 1) : leave;
		// Whatever he does standing there once he is down - hands on his
		// hips, say - he does out there, once he has stepped back, and he
		// stays to finish it before he goes (see IN_PLACE): as long as he
		// can, and still be in time for what he does next (picking the ball
		// up to take it out, say), or not at all.
		const next = tr.acts
			.filter((a) => !IN_PLACE.has(a.anim) && a.t0 > leave)
			.sort((x, y) => x.t0 - y.t0)[0];
		const due = next
			? next.t0 - runMs(dist(R, this.posAt(pid, next.t0)), SPRINT) - 100
			: Infinity;
		// (One he starts a moment after he is back counts too: he does not
		// slide off across the floor in the middle of it.)
		let hold = stepped;
		for (const a of [...tr.acts].sort((x, y) => x.t0 - y.t0)) {
			if (
				!IN_PLACE.has(a.anim) ||
				a.t1 <= leave ||
				a.t0 >= Math.max(hold, stepped + 1000)
			) {
				continue;
			}
			if (a.t0 < stepped) {
				a.t1 += stepped - a.t0;
				a.t0 = stepped;
			}
			a.t1 = Math.min(a.t1, due);
			if (a.t1 - a.t0 < IN_PLACE_MIN) {
				tr.acts.splice(tr.acts.indexOf(a), 1);
				continue;
			}
			hold = Math.max(hold, a.t1);
		}
		tr.acts.sort((x, y) => x.t0 - y.t0);
		let z = hold;
		let Z = this.posAt(pid, z);
		for (let n = 0; n < 80; n++) {
			if (runMs(dist(R, Z), SPRINT) + 100 <= z - hold) {
				break;
			}
			z += 100;
			Z = this.posAt(pid, z);
		}
		const kept: Move[] = [];
		for (const m of tr.moves) {
			if (m.t1 <= t0 || m.t0 >= z) {
				kept.push(m);
			} else if (m.t0 < t0) {
				kept.push({ ...m, t1: t0, to: { ...P }, v1: undefined });
			} else if (m.t1 > z) {
				kept.push({ ...m, t0: z, from: { ...Z }, v0: undefined });
			}
		}
		const d = dist(P, V);
		if (!there) {
			kept.push({
				t0,
				t1: by,
				from: { ...P },
				to: { ...V },
				anim: d > 6 ? "run" : "slide",
				...(d > 6 ? {} : { face: meet.face }),
			});
		}
		if (stepped > leave) {
			kept.push({
				t0: leave,
				t1: stepped,
				from: { ...V },
				to: { ...R },
				anim: "back",
				face: meet.face,
			});
		}
		if (z > hold && dist(R, Z) > 0.05) {
			kept.push({
				t0: hold,
				t1: z,
				from: { ...R },
				to: { ...Z },
				anim: dist(R, Z) > 6 ? "run" : "shuffle",
			});
		}
		tr.moves = kept.sort((x, y) => x.t0 - y.t0);
	}

	// Whatever run he is on at t, he stops it there - to go somewhere else -
	// and any he was to go on after it, he never does.
	private cutShort(pid: number, t: number) {
		const tr = this.track(pid);
		if (!tr) {
			return;
		}
		while ((tr.moves.at(-1)?.t0 ?? -Infinity) >= t) {
			const m = tr.moves.pop()!;
			this.pos.set(pid, { ...m.from });
			this.free.set(pid, t);
		}
		const m = tr.moves.at(-1);
		if (!m || m.t1 <= t) {
			return;
		}
		const at = this.posAt(pid, t);
		m.to = { ...at };
		m.t1 = t;
		// (Stopped there: whatever pace it was to end at, it doesn't.)
		m.v1 = undefined;
		this.pos.set(pid, { ...at });
		this.free.set(pid, t);
	}

	// On the very spot, where the ball will be: a run there leaves him short
	// of it by less than a step (see go) - the last few inches, a shuffle.
	// Returns when he is there.
	private settleOn(pid: number, to: Pt, t: number): number {
		const tr = this.track(pid);
		const from = this.posOf(pid);
		if (!tr || dist(from, to) < 0.02) {
			return t;
		}
		const t0 = Math.max(t, this.free.get(pid) ?? 0);
		tr.moves.push({
			t0,
			t1: t0 + SETTLE_MS,
			from: { ...from },
			to: { ...to },
			anim: "shuffle",
		});
		this.pos.set(pid, { ...to });
		this.free.set(pid, t0 + SETTLE_MS);
		return t0 + SETTLE_MS;
	}

	// Nothing worth watching from t0 to t1 (the ball brought up, a walk to the
	// line): the picture runs through it fast - however short, for the dead
	// time around the free throws.
	private hurry(t0: number, t1: number, dead = false) {
		if (t1 - t0 >= 900) {
			this.fast.push(dead ? [t0, t1, DEAD_MIN] : [t0, t1]);
		}
	}

	// Whatever run `go` just gave him, he makes shadowing `man`.
	private marks(d: number, man: number, before: number) {
		const moves = this.track(d)?.moves;
		if (moves && moves.length > before) {
			this.marking.set(moves.at(-1)!, man);
		}
	}

	// The same, but arriving no later than `by` if he can (a slower man takes
	// the time he needs).
	private goBy(
		pid: number,
		to: Pt,
		t0: number,
		by: number,
		anim: AnimName = "run",
		face?: 1 | -1,
		effort = 1,
	): number {
		const start = Math.max(t0, this.free.get(pid) ?? 0);
		const d = dist(this.posOf(pid), to);
		const speed = Math.max(
			JOG,
			paceFor(
				d,
				Math.max(250, by - start),
				this.carried(pid, to, start, SPRINT),
				SPRINT,
				effort,
			),
		);
		return this.go(pid, to, start, speed, anim, face, { effort });
	}

	// UP THE FLOOR IN LANES. Not two men side by side: a wing runs out wide,
	// down his side of the floor, a big down the middle, a second man on a
	// side the lane inside him - and from half court each in to his spot (or,
	// told to go only `upTo` so far up the floor for now, there in his lane).
	private fillLanes(
		team: Side,
		men: { pid: number; to: Pt; j: number; by?: number; upTo?: number }[],
		t0: number,
		by: number,
		// Across the floor, where the ball comes up: nobody runs his lane.
		ballY?: number,
	) {
		const dir = attackDir(team);
		const rim = rimPt(team);
		const taken: number[] = [];
		// The lane he would run, or - that one gone, or the ball's - the
		// nearest one free, his side of the ball for choice.
		const laneFor = (want: number, side: number) => {
			let best = want;
			let cost = Infinity;
			for (const y of LANES) {
				if (
					taken.some((q) => Math.abs(q - y) < 5) ||
					(ballY !== undefined && Math.abs(y - ballY) < LANE_OFF_BALL)
				) {
					continue;
				}
				const c =
					Math.abs(y - want) +
					(Math.sign(y - COURT_H / 2) === -side ? LANE_ACROSS : 0);
				if (c < cost) {
					cost = c;
					best = y;
				}
			}
			taken.push(best);
			return best;
		};
		const lanes = men
			.map((m) => {
				const big = Math.abs(m.to.y - COURT_H / 2) < 9 && dist(m.to, rim) < 17;
				// (Only going so far for now, he takes the side he is on - not
				// across the others to his spot's.)
				const y = m.upTo === undefined ? m.to.y : this.posOf(m.pid).y;
				const side = y < COURT_H / 2 ? -1 : 1;
				return { ...m, big, side, y };
			})
			// Out wide first: whoever is to go nearer his sideline has it.
			.sort((a, b) => Math.abs(b.y - 25) - Math.abs(a.y - 25));
		for (const m of lanes) {
			const from = this.posOf(m.pid);
			const at = m.by ?? by;
			const endX = m.upTo ?? m.to.x;
			if ((endX - from.x) * dir < 24) {
				const end =
					m.upTo === undefined ? m.to : clampPt({ x: endX, y: from.y });
				this.goBy(m.pid, end, t0 + m.j * 90, at, "run");
				continue;
			}
			const laneY = laneFor(COURT_H / 2 + m.side * (m.big ? 3.5 : 19), m.side);
			const start = Math.max(t0 + m.j * 90, this.free.get(m.pid) ?? 0);
			if (m.upTo !== undefined) {
				this.goBy(m.pid, clampPt({ x: endX, y: laneY }), start, at, "run");
				continue;
			}
			const W = clampPt({ x: from.x + (m.to.x - from.x) * 0.55, y: laneY });
			const d1 = dist(from, W);
			const d2 = dist(W, m.to);
			const mid = start + (at - start) * (d1 / Math.max(1, d1 + d2));
			this.goBy(m.pid, W, start, mid, "run");
			this.goBy(m.pid, m.to, mid, at, "run");
		}
	}

	// Carried somewhere as part of something he does - the gather and leap
	// of a dunk, a fadeaway's drift, the last stride into a layup - in just
	// the time it takes, at an even pace, rather than run there. Returns
	// when he is there.
	private carry(
		pid: number,
		to: Pt,
		t0: number,
		ms: number,
		anim: AnimName = "run",
		face?: 1 | -1,
		how: {
			// Straight on out of the run that brings him there (a drive to the
			// rim into his last stride up), not pulled up dead at the end of
			// it and off again: from the moment it ends, as fast as it has him
			// going that way.
			onFrom?: boolean;
			// How fast he is still going at the end (feet a second), if not
			// at the pace he went all along.
			v1?: number;
		} = {},
	): number {
		const tr = this.track(pid);
		if (!tr) {
			return t0;
		}
		const from = this.posOf(pid);
		let start = Math.max(t0, this.free.get(pid) ?? 0);
		const d = dist(from, to);
		if (d < 0.3) {
			return start;
		}
		const end = start + ms;
		let v0: number | undefined;
		const last = tr.moves.at(-1);
		if (
			how.onFrom &&
			last &&
			last.t1 <= start &&
			start - last.t1 <= ON_FROM_MS &&
			dist(last.to, from) < 0.25 &&
			dist(last.from, last.to) >= 0.3
		) {
			const la = dist(last.from, last.to);
			const cos =
				((last.to.x - last.from.x) * (to.x - from.x) +
					(last.to.y - last.from.y) * (to.y - from.y)) /
				(la * d);
			const keep = keepThrough(cos);
			if (keep >= 0.1) {
				start = last.t1;
				// (As fast as he was going on it, as the turn allows - but not so
				// fast this could not slow to how it ends and still take as long
				// as it does.)
				const avg = d / ((end - start) / 1000);
				const out = how.v1 ?? avg;
				const was = la / Math.max(0.05, (last.t1 - last.t0) / 1000);
				v0 = Math.max(0, Math.min(was * keep, 2 * avg - out, 1.5 * avg));
				last.v1 = v0;
			}
		}
		const pace = d / ((end - start) / 1000);
		tr.moves.push({
			t0: start,
			t1: end,
			from: { ...from },
			to,
			anim,
			...(face === undefined ? {} : { face }),
			v0: v0 ?? pace,
			v1: how.v1 ?? pace,
		});
		if (face !== undefined) {
			tr.faces.push([start, face]);
			this.face.set(pid, face);
		}
		this.pos.set(pid, to);
		this.free.set(pid, end);
		return end;
	}

	// How fast a man is already going setting off for `to` at `start`: the
	// pace he carries on with from a run that ends there and then, as much
	// of it as the turn allows (see keepThrough) - none from standing.
	private carried(pid: number, to: Pt, start: number, speed: number): number {
		const last = this.track(pid)?.moves.at(-1);
		const from = this.posOf(pid);
		if (
			!last ||
			start - last.t1 > 300 ||
			start < last.t1 - 1 ||
			dist(last.to, from) > 0.5
		) {
			return 0;
		}
		const la = dist(last.from, last.to);
		const lb = dist(from, to);
		if (la < 0.3 || lb < 0.3) {
			return 0;
		}
		const cos =
			((last.to.x - last.from.x) * (to.x - from.x) +
				(last.to.y - last.from.y) * (to.y - from.y)) /
			(la * lb);
		const pace = la / Math.max(0.05, (last.t1 - last.t0) / 1000);
		return Math.min(pace, speed) * keepThrough(cos);
	}

	private turn(pid: number, t: number, f: 1 | -1) {
		this.track(pid)?.faces.push([t, f]);
		this.face.set(pid, f);
	}

	private act(
		pid: number,
		anim: AnimName,
		t0: number,
		t1: number,
		o: Omit<Act, "t0" | "t1" | "anim"> & { face?: 1 | -1 } = {},
	) {
		const tr = this.track(pid);
		if (!tr) {
			return;
		}
		const { face, ...rest } = o;
		// A pass is thrown from both hands: off his dribble, he picks it up
		// first. (Pushed off it one-handed, it comes up into his hand.)
		if (PASS_ACTS.has(anim)) {
			this.gather(pid, t0);
		}
		const left =
			rest.mirror === undefined && this.shootsLeft(pid, anim, t0, rest.look);
		tr.acts.push({
			t0,
			t1,
			anim,
			...rest,
			...(left ? { mirror: true as const } : {}),
		});
		if (face !== undefined) {
			this.turn(pid, t0, face);
		}
	}

	// How he celebrates a three, his own way every time: both arms up, the
	// goggles, a shimmy - or, most of all, a finger at the bench.
	private threeCelebration(pid: number): AnimName {
		const h = hash01(pid, 5531);
		return h < 0.25
			? "threeUp"
			: h < 0.4
				? "goggles"
				: h < 0.52
					? "shimmy"
					: "point";
	}

	// After a big finish, if it is his thing: down in a crouch with a roar,
	// or a fist pounded on his chest - the same man the same way, most times.
	private bigFinish(pid: number, at: number): AnimName | undefined {
		const h = hash01(pid, 6151);
		const his: AnimName | undefined =
			h < 0.22 ? "roar" : h < 0.4 ? "chestPound" : undefined;
		return his && hash01(pid, at) < 0.65 ? his : undefined;
	}

	// How he throws it down, `r` of the way through his own odds: a big man
	// two hands, mostly; an athlete up over his head, as often as not - and
	// each man his own way of it, the same every game.
	private dunkFor(pid: number, r: number): AnimName {
		const big = (this.rank.get(pid) ?? 4) >= 6;
		const athlete = this.skills.get(pid)?.includes("A") ?? false;
		const own = hash01(pid, 9131);
		const two = (big ? 0.6 : athlete ? 0.2 : 0.4) * (1.5 - own);
		const one = 0.35;
		const over = (big ? 0.05 : athlete ? 0.45 : 0.2) * (0.5 + own);
		// (A windmill takes a flier: an athlete, now and then, on his own.)
		const mill = (big ? 0 : athlete ? 0.3 : 0.03) * own;
		const x = r * (two + one + over + mill);
		return x < two
			? "dunk"
			: x < two + one
				? "dunk1"
				: x < two + one + over
					? "tomahawk"
					: "windmill";
	}

	// His move with it on the block, `r` of the way through his odds: a man
	// has a go-to - the one he goes to half the time and more, the same every
	// game - and the rest now and then.
	private postMoveFor(pid: number, r: number): PostMove {
		const odds: [PostMove, number][] = [
			["hook", 0.36],
			["fade", 0.24],
			["dropStep", 0.22],
			["upUnder", 0.18],
		];
		let k = hash01(pid, 2203);
		const go = odds.find(([, w]) => (k -= w) < 0)?.[0] ?? "hook";
		const own = odds.map(([m, w]): [PostMove, number] => [
			m,
			w + (m === go ? 0.6 : 0),
		]);
		let x = r * own.reduce((a, [, w]) => a + w, 0);
		return own.find(([, w]) => (x -= w) < 0)?.[0] ?? go;
	}

	private lefty(pid: number): boolean {
		return hash01(pid, 7211) < LEFTIES;
	}

	// The hand he goes to with it: his left, a left-hander.
	private strongHand(pid: number): Hand {
		return this.lefty(pid) ? "L" : "R";
	}

	// Whether he goes up with his left (see LEFTIES): at the rim (`look`), on
	// the side of it he is on as he goes up.
	private shootsLeft(
		pid: number,
		anim: AnimName,
		t: number,
		look: Pt | undefined,
	): boolean {
		if (OWN_HAND.has(anim)) {
			return this.lefty(pid);
		}
		if (!OUTSIDE_HAND.has(anim) || !look) {
			return false;
		}
		const P = this.posAt(pid, t);
		// (Facing the rim, his right is the way the floor's y runs, coming at
		// it from the left of the floor.)
		const side = (P.y - look.y) * Math.sign(look.x - P.x);
		if (Math.abs(side) < RIM_SIDE) {
			return this.lefty(pid);
		}
		const r = hash01(pid * 7 + 3, t);
		return side < 0 ? this.lefty(pid) || r < 0.55 : this.lefty(pid) && r < 0.35;
	}

	// A man contesting a shot in close: an arm's length off where the
	// shooter comes down, his own side of the way in, a little nearer the
	// rim.
	private besideLanding(d: number, down: Pt, toRim: Pt): Pt {
		const L = Math.hypot(toRim.x, toRim.y) || 1;
		const u = { x: toRim.x / L, y: toRim.y / L };
		const D = this.posOf(d);
		const s = (D.x - down.x) * -u.y + (D.y - down.y) * u.x >= 0 ? 1 : -1;
		return clampPt({
			x: down.x - u.y * s * 2 + u.x * 0.6,
			y: down.y + u.x * s * 2 + u.y * 0.6,
		});
	}

	// On his way to meet a shot, not through the shooter. Run straight to
	// where he meets it, the shooter going on in and up meanwhile, a man
	// chasing the drive from behind him - or cutting across his way in -
	// would go right through him: he takes instead the nearest spot his own
	// side of the shooter's way that he gets to clear of him - level with
	// where he comes down (`down`), or a step or two behind, a hand at the
	// ball from his hip. Runs him there (from t0, there by t1 if he can be)
	// and returns where to, and when he gets there. (C as it was, if that is
	// clear already - or if nothing is.)
	private meetClear(
		d: number,
		shooter: number,
		C: Pt,
		t0: number,
		t1: number,
		down: Pt,
	): { at: Pt; there: number } {
		const CLEAR = 1.8;
		// (Right on him to start with, how quick he gets clear: feet a
		// second, across.)
		const APART_FTPS = 4;
		const P = this.posOf(shooter);
		const run = (X: Pt) =>
			this.goBy(d, X, t0, t1, "run", (P.x >= X.x ? 1 : -1) as 1 | -1, BURST);
		const tr = this.track(d);
		if (!tr) {
			return { at: C, there: run(C) };
		}
		const G = this.posOf(d);
		const was = {
			moves: tr.moves.length,
			faces: tr.faces.length,
			face: this.face.get(d),
			free: this.free.get(d),
		};
		const undo = () => {
			tr.moves.length = was.moves;
			tr.faces.length = was.faces;
			this.pos.set(d, G);
			if (was.face !== undefined) {
				this.face.set(d, was.face);
			}
			if (was.free !== undefined) {
				this.free.set(d, was.free);
			}
		};
		// No nearer him than this on the way - read off the run as it is set
		// down, and the shooter's own - or, right on him already, no nearer
		// than that, and clear of him within a few strides.
		const from = Math.max(t0, was.free ?? 0);
		const d0 = dist(G, this.posAt(shooter, from));
		// (How far clear of that he stays - below 0, into him.)
		const room = (there: number) => {
			let r = dist(this.posOf(d), down) - CLEAR;
			for (let t = from + 40; t <= Math.max(there, t1) + 400; t += 40) {
				const near = Math.min(
					CLEAR,
					d0 - 0.1 + (APART_FTPS * (t - from)) / 1000,
				);
				// (However either of them really paces his run.)
				for (const a of [false, true]) {
					for (const b of [false, true]) {
						r = Math.min(
							r,
							dist(this.posAt(d, t, a), this.posAt(shooter, t, b)) - near,
						);
					}
				}
			}
			return r;
		};
		// The shooter's way in, and which side of it he is (or C is, if he
		// is right on it).
		const S = this.posAt(shooter, from);
		const u = dist(S, down) > 0.5 ? unitVec(S, down) : unitVec(G, down);
		const across = (p: Pt) => (p.x - S.x) * -u.y + (p.y - S.y) * u.x;
		const a = across(G);
		const side = (Math.abs(a) > 0.3 ? a : across(C)) >= 0 ? 1 : -1;
		// Behind him, chasing - or right on him, the drive going by him: never
		// past him - from his hip, a step or two back from where he comes
		// down.
		const behind = (G.x - S.x) * u.x + (G.y - S.y) * u.y < -0.5 || d0 < 1.2;
		const spots = behind ? [] : [C];
		for (const back of behind ? [-1.2, -2, -2.8] : [0.6, -0.6, -1.6, -2.6]) {
			for (const wide of behind ? [2.4, 3] : [2.2, 3]) {
				spots.push(
					clampPt({
						x: down.x - u.y * side * wide + u.x * back,
						y: down.y + u.x * side * wide + u.y * back,
					}),
				);
			}
		}
		// The first of them he gets to clear of him - or, if none, the one
		// that keeps him the most clear.
		let best = { at: C, room: -Infinity };
		for (const X of spots) {
			const there = run(X);
			const r = room(there);
			if (r >= 0) {
				return { at: X, there };
			}
			undo();
			if (r > best.room) {
				best = { at: X, room: r };
			}
		}
		return { at: best.at, there: run(best.at) };
	}

	// In the lane off the drive or the roll, short of the rim: floated up
	// over the big, or a hook in over him - a guard the floater, all but
	// now and then, a big mostly the hook, each man more one way than
	// another, the same every game. (Never a jumper from in there.)
	private laneFinish(pid: number, t: number): ShotStyle {
		const rank = this.rank.get(pid) ?? 4;
		const floats = rank >= 6 ? 0.3 : rank >= 3 ? 0.6 : 1;
		const f = Math.min(0.95, floats * (0.75 + 0.5 * hash01(pid, 9137)));
		return hash01(pid * 13 + 5, Math.round(t)) < f ? "floater" : "hook";
	}

	// Whether the shot he has going up now (as last listed) is off his left.
	private offHisLeft(pid: number): boolean {
		return (
			this.track(pid)?.acts.findLast(
				(a) =>
					OWN_HAND.has(a.anim) ||
					OUTSIDE_HAND.has(a.anim) ||
					a.anim === "dunk" ||
					a.anim === "powerLayup",
			)?.mirror === true
		);
	}

	// Two teammates meet halfway and bump chests in the air.
	private chestBump(a: number, b: number, t: number) {
		const A = this.posOf(a);
		const B = this.posOf(b);
		const u = unitVec(A, B);
		const mid = clampPt({ x: (A.x + B.x) / 2, y: (A.y + B.y) / 2 });
		const start = Math.max(t, this.free.get(b) ?? 0);
		const ta = this.go(
			a,
			clampPt({ x: mid.x - u.x * 1.15, y: mid.y - u.y * 1.15 }),
			Math.max(t, this.free.get(a) ?? 0),
			10,
			"jog",
		);
		const tb = this.go(
			b,
			clampPt({ x: mid.x + u.x * 1.15, y: mid.y + u.y * 1.15 }),
			start,
			12,
			"run",
		);
		const at = Math.max(ta, tb) + 60;
		const face = (u.x >= 0 ? 1 : -1) as 1 | -1;
		this.act(a, "chestBump", at, at + 900, { face, jump: [0.32, 0.72, 1.5] });
		this.act(b, "chestBump", at, at + 900, {
			face: -face as 1 | -1,
			jump: [0.32, 0.72, 1.5],
		});
		for (const p of [a, b]) {
			this.free.set(p, Math.max(this.free.get(p) ?? 0, at + 900));
		}
	}

	// Something to say about what just happened - at the official (out
	// toward the sideline nearer him), or at whoever he means - for a moment
	// before he goes on: he does not set off until he has said it.
	// A man down on the floor at F, facing A: the nearest of his teammates
	// (one, or two of them close) come over, stand over him to his side away
	// from X, lean in and pull him up as he gets up.
	// Returns when the first of them has a hand on him (the contact itself,
	// if nobody comes).
	private helpUp(down: number, F: Pt, A: Pt, X: Pt, hit: number): number {
		const team = this.teamOf(down);
		const near = this.slots(team)
			.filter((p) => p !== down)
			.map((p) => ({ p, d: dist(this.posOf(p), F) }))
			.filter((x) => x.d < HELP_UP_FROM)
			.sort((a, b) => a.d - b.d);
		const n = near.length >= 2 && hash01(down, hit) < 0.4 ? 2 : 1;
		const u = unitVec(F, A);
		const side = { x: -u.y, y: u.x };
		const away = (X.x - F.x) * side.x + (X.y - F.y) * side.y > 0 ? -1 : 1;
		let first: number | undefined;
		near.slice(0, n).forEach(({ p }, k) => {
			const s = k === 0 ? away : -away;
			const spot = clampPt({
				x: F.x + side.x * s * 2.3 + u.x * 1.1,
				y: F.y + side.y * s * 2.3 + u.y * 1.1,
			});
			const from = Math.max(hit + 200, this.free.get(p) ?? 0);
			// (Only one who gets there while he is still down.)
			const need = runMs(dist(this.posOf(p), spot), SPRINT);
			if (from + need > hit + HELP_UP_BY) {
				return;
			}
			const there = this.goBy(p, spot, from, from + need + 120, "run");
			const t0 = Math.max(there, hit + HELP_UP_AT);
			// (A hand down to him a moment after he gets there; a second man
			// in the pull as it comes.)
			const hand = t0 + HELP_HAND_MS;
			first ??= hand;
			const t1 = Math.max(hand, first) + HELP_PULL_MS;
			this.act(p, "helpUp", t0, t1, { look: { ...F } });
			this.free.set(p, Math.max(this.free.get(p) ?? 0, t1));
		});
		return first ?? hit;
	}

	private react(
		pid: number,
		anim: AnimName,
		t: number,
		dur: number,
		look?: Pt,
	) {
		const P = this.posOf(pid);
		this.act(pid, anim, t, t + dur, {
			look: look ?? { x: P.x, y: P.y >= COURT_H / 2 ? COURT_H + 4 : -4 },
		});
		this.free.set(pid, Math.max(this.free.get(pid) ?? 0, t + dur));
	}

	// Said with an arm, on the way (see Gesture) - when the moment calls for
	// it, about `p` of the time: decided by who and when, not drawn from the
	// game's own luck, so whatever else happens stays as it was.
	private gesture(
		pid: number,
		kind: Gesture["kind"],
		t0: number,
		t1: number,
		at?: number | Pt,
		p = 1,
		mirror = false,
	) {
		const tr = this.track(pid);
		if (!tr || t1 - t0 < 300 || hash01(pid, t0) >= p) {
			return;
		}
		tr.arms.push({
			t0,
			t1,
			kind,
			...(at === undefined
				? {}
				: { at: typeof at === "number" ? at : { x: at.x, y: at.y } }),
			...(mirror ? { mirror: true as const } : {}),
		});
	}

	private lookAt(pid: number, t: number, at: Pt) {
		this.track(pid)?.looks.push([t, { x: at.x, y: at.y }]);
	}

	private show(pid: number, t: number, on: boolean) {
		this.track(pid)?.shown.push([t, on]);
	}

	// A newer instruction for the ball supersedes anything that had been
	// scheduled for after it (a bounce that was still rolling, say), or for
	// the same moment.
	private pushBall(seg: BallSeg) {
		while (this.ball.length > 1 && this.ball.at(-1)!.t0 >= seg.t0) {
			this.ball.pop();
		}
		this.ball.push(seg);
	}

	// Where a player is at a moment, read off his own schedule (eased in and
	// out - or, `even`, at an even pace along each run, as a man already going
	// at either end of it more nearly goes; or, `real`, getting going and
	// pulling up just as the evaluator has him: where he really is, once
	// everybody's runs are laid out).
	private posAt(pid: number, t: number, even = false, real = false): Pt {
		const moves = this.track(pid)?.moves ?? [];
		for (let k = moves.length - 1; k >= 0; k--) {
			const m = moves[k]!;
			if (m.t0 <= t) {
				if (t >= m.t1) {
					return m.to;
				}
				const u = (t - m.t0) / (m.t1 - m.t0);
				const e = even
					? u
					: real
						? shareAlong(m, u)
						: 0.5 - 0.5 * Math.cos(Math.PI * u);
				return {
					x: m.from.x + (m.to.x - m.from.x) * e,
					y: m.from.y + (m.to.y - m.from.y) * e,
				};
			}
		}
		return this.track(pid)?.start ?? this.posOf(pid);
	}

	// Where a flight of the ball starts from: the holder's hands, or where it lies.
	private ballOrigin(): BallEnd {
		return this.holder === undefined ? this.ballAt : { pid: this.holder };
	}

	// The ball his from t - held, dribbled or crossed over. Returns when he
	// really has it that way: off his dribble, not before it comes up to
	// him (see offDribble).
	private hold(
		pid: number,
		t0: number,
		style: "hold" | "dribble" | "cross" = "hold",
		hand?: Hand,
		move?: DribbleMove,
	): number {
		let t = style === "dribble" ? t0 : this.offDribble(pid, t0, style);
		// Off a run of crossovers, he dribbles on once the ball comes up into
		// a hand - the hand it comes up into. (Off the last of the ball's moves
		// begun before this one: any from then on, this takes the place of.)
		const last = this.ball.findLast((s) => s.t0 < t);
		let crossed: Hand | undefined;
		if (
			style === "dribble" &&
			last?.kind === "hold" &&
			last.pid === pid &&
			last.style === "cross" &&
			last.t0 < t
		) {
			const k = Math.max(1, Math.ceil((t - last.t0) / CROSS_MS - 1e-6));
			t = last.t0 + k * CROSS_MS;
			const first = last.hand ?? "R";
			crossed = k % 2 ? (first === "R" ? "L" : "R") : first;
		}
		// The hand the ball is in, unless he is told otherwise - the one his
		// dribble has it in, or his strong one once he has had it in both.
		const h =
			crossed ??
			hand ??
			(t !== t0 ? this.dribbleHandAt(t) : undefined) ??
			(this.ballHandOf === pid ? this.ballHand : this.strongHand(pid));
		this.pushBall({
			kind: "hold",
			t0: t,
			pid,
			style,
			...(style !== "hold" ? { hand: h } : {}),
			...(move ? { move } : {}),
		});
		this.holder = pid;
		this.ballHand = style === "hold" ? this.strongHand(pid) : h;
		this.ballHandOf = pid;
		return t;
	}

	// Which way a crossover goes: between his legs, behind his back, or -
	// most often - across in front of him.
	private crossMove(legs: number, back: number): DribbleMove {
		const r = this.rng();
		return r < legs ? "legs" : r < legs + back ? "back" : "front";
	}

	// Whether his left hand is the one on the ball's side, for a man at
	// `from` going at a shooter at P as he lets it go: the ball up on the
	// shooter's right, a little out from him - so, face to face, the left.
	private leftToBall(from: Pt, shooter: number, P: Pt): boolean {
		const f = unitVec(P, rimPt(this.teamOf(shooter)));
		// (Up in his right hand - or his left.)
		const k = this.offHisLeft(shooter) ? -0.7 : 0.7;
		const B = { x: P.x - f.y * k, y: P.y + f.x * k };
		const g = unitVec(from, P);
		return (B.x - from.x) * -g.y + (B.y - from.y) * g.x < 0;
	}

	// A run of `n` dribble moves as ball segments, from t and starting in
	// `hand`, a bounce each and each into the other hand - the same move
	// again making one segment of two bounces. Returns them and the hand the
	// last comes up into.
	private runOfMoves(
		pid: number,
		t: number,
		n: number,
		legs: number,
		back: number,
		hand: Hand,
	): { segs: BallSeg[]; hand: Hand } {
		const segs: BallSeg[] = [];
		let at = t;
		let h = hand;
		let last: DribbleMove | undefined;
		for (let i = 0; i < n; i++) {
			const move = this.crossMove(legs, back);
			if (move !== last) {
				segs.push({ kind: "hold", t0: at, pid, style: "cross", hand: h, move });
				last = move;
			}
			at += CROSS_MS;
			h = h === "R" ? "L" : "R";
		}
		return { segs, hand: h };
	}

	// A run of `n` dribble moves from t, a bounce each and each into the
	// other hand: the same one again (a double crossover, back and forth
	// between his legs) or one into another (between his legs into a
	// crossover). Starts in `hand` if said. Returns when the last comes up
	// into his hand.
	private moveRun(
		pid: number,
		t: number,
		n: number,
		legs: number,
		back: number,
		hand?: Hand,
	): number {
		let run:
			| { move: DribbleMove; t0: number; hand: Hand; n: number }
			| undefined;
		for (let i = 0; i < n; i++) {
			const move = this.crossMove(legs, back);
			if (run && move === run.move) {
				run.n += 1;
				continue;
			}
			const from: Hand | undefined = run
				? run.n % 2
					? run.hand === "R"
						? "L"
						: "R"
					: run.hand
				: hand;
			const t0 = this.hold(
				pid,
				run ? run.t0 + run.n * CROSS_MS : t,
				"cross",
				from,
				move,
			);
			const seg = this.ball.at(-1);
			run = {
				move,
				t0,
				hand: (seg?.kind === "hold" ? seg.hand : undefined) ?? "R",
				n: 1,
			};
		}
		return run ? run.t0 + run.n * CROSS_MS : t;
	}

	// When his dribble started, if he is dribbling at t: his dribbles back to
	// back keep one beat from the first of them (see evaluate.ts).
	private dribbleFrom(pid: number, t: number): number | undefined {
		const dribbling = (s: BallSeg | undefined) =>
			s?.kind === "hold" && s.pid === pid && s.style === "dribble";
		let j = this.ball.length - 1;
		if (!dribbling(this.ball[j]) || this.ball[j]!.t0 > t) {
			return undefined;
		}
		while (j > 0 && dribbling(this.ball[j - 1])) {
			j--;
		}
		return this.ball[j]!.t0;
	}

	// The last top of a bounce of his dribble, at or before t, if he is
	// dribbling then.
	private dribbleTop(pid: number, t: number): number | undefined {
		const from = this.dribbleFrom(pid, t);
		return from === undefined
			? undefined
			: from + Math.floor((t - from) / DRIBBLE_MS + 1e-6) * DRIBBLE_MS;
	}

	// When he can take the ball out of his dribble, asked to at about t: in
	// both hands as it comes up to him (or at the top), across into the
	// other hand at the top - never on its way to the floor. Off his first
	// bounce he waits for it; after that he takes it the bounce before.
	private offDribble(pid: number, t: number, style: "hold" | "cross"): number {
		const from = this.dribbleFrom(pid, t);
		if (from === undefined) {
			return t;
		}
		const b = (t - from) / DRIBBLE_MS + 1e-6;
		const k = Math.floor(b);
		const ph = b - k;
		if (ph < 0.02 || (style === "hold" && ph >= CATCH_UP)) {
			return t;
		}
		return k >= 1
			? from + k * DRIBBLE_MS
			: from + (style === "hold" ? CATCH_UP : 1) * DRIBBLE_MS;
	}

	// The hand his dribble has the ball in at t.
	private dribbleHandAt(t: number): Hand | undefined {
		for (let j = this.ball.length - 1; j >= 0; j--) {
			const s = this.ball[j]!;
			if (s.t0 <= t) {
				return s.kind === "hold" ? s.hand : undefined;
			}
		}
		return undefined;
	}

	// He picks up his dribble about `by`, as the ball comes up into his
	// hands instead of jumping there off the floor. Returns when he has it
	// (`by` if he was not dribbling).
	private gather(pid: number, by: number): number {
		// (Off a crossover: as it comes up into a hand at the top of a
		// bounce - the last before, or the first, if it is not up yet.)
		const last = this.ball.at(-1);
		if (
			last?.kind === "hold" &&
			last.pid === pid &&
			last.style === "cross" &&
			last.t0 < by
		) {
			const k = Math.max(1, Math.floor((by - last.t0) / CROSS_MS + 1e-6));
			return this.hold(pid, last.t0 + k * CROSS_MS, "hold");
		}
		return this.dribbleFrom(pid, by) === undefined
			? by
			: this.hold(pid, by, "hold");
	}

	// Through the net, down off the floor under it: the man taking it out
	// gets to it and takes it on the hop - or scoops it up as it rolls - not
	// stood over it till it lies still. Returns when he has it; undefined,
	// the ball left as it was, if he can't get to it on its way.
	private offTheHop(pid: number, start: number): number | undefined {
		const n = this.ball.length;
		const rest = this.ball[n - 1];
		const drop = this.ball[n - 2];
		if (rest?.kind !== "rest" || drop?.kind !== "bounce") {
			return undefined;
		}
		// (Down to the floor first, if it isn't there already.)
		const high = drop.from.z - BALL_R;
		const land =
			drop.t0 + (high > 0.3 ? Math.sqrt((2 * high) / GRAVITY) * 1000 : 0);
		const F = { x: drop.from.x, y: drop.from.y, z: BALL_R };
		const span = Math.max(0.3, (drop.t1 - land) / 1000);
		const vel = {
			x: (drop.to.x - F.x) / span,
			y: (drop.to.y - F.y) / span,
			z: 0,
		};
		if (land > drop.t0) {
			this.fly(drop.t0, land, drop.from, F);
		}
		const got = this.chaseDown(
			pid,
			start,
			F,
			land,
			drop.h0,
			vel,
			undefined,
			true,
		);
		if (got === undefined) {
			while (this.ball.length > n - 2) {
				this.ball.pop();
			}
			this.ball.push(drop, rest);
			this.holder = undefined;
			this.ballAt = rest.at;
		}
		return got;
	}

	// The ball left bouncing at a whistle or a basket - down through the
	// net, say - goes off the floor and away, not on through a man stood
	// there (one with his hands on his hips, or having words, holds his
	// ground: see keepOff): it comes off the floor away from whoever is
	// there, the nearest way round to where it was going that misses them
	// all, as far.
	private bounceClear(men: number[]) {
		const n = this.ball.length;
		const rest = this.ball[n - 1];
		const drop = this.ball[n - 2];
		if (rest?.kind !== "rest" || drop?.kind !== "bounce") {
			return;
		}
		const F = drop.from;
		const clearOf = (to: Pt) => {
			for (let s = drop.t0; s <= drop.t1; s += 40) {
				const u = (s - drop.t0) / (drop.t1 - drop.t0);
				const B = { x: F.x + (to.x - F.x) * u, y: F.y + (to.y - F.y) * u };
				if (men.some((pid) => dist(this.posAt(pid, s), B) < BOUNCE_CLEAR)) {
					return false;
				}
			}
			return true;
		};
		if (clearOf(drop.to)) {
			return;
		}
		const L = Math.max(2, dist(F, drop.to));
		const a0 = Math.atan2(drop.to.y - F.y, drop.to.x - F.x);
		for (let k = 1; k <= 6; k++) {
			for (const s of [1, -1]) {
				const a = a0 + (s * k * Math.PI) / 6;
				const Q = inPlay({
					x: F.x + Math.cos(a) * L,
					y: F.y + Math.sin(a) * L,
				});
				if (clearOf(Q)) {
					drop.to = Q;
					rest.at = { ...Q, z: rest.at.z };
					this.ballAt = rest.at;
					return;
				}
			}
		}
	}

	// He gets to the ball on the floor and scoops it up - stopped a stride
	// short of it, so it is under his hands as he bends - and comes up with
	// it, or comes up dribbling. Returns when he gets to it; he has it up
	// 300ms later.
	private pickUp(
		pid: number,
		t: number,
		speed: number,
		style: "hold" | "dribble" = "hold",
		effort = 1,
	): number {
		const b = this.ballPoint();
		const from = this.posOf(pid);
		this.ownRun(pid, t);
		// (Off the floor for it if that is where it is.)
		const stop =
			dist(from, b) > PICKUP_REACH
				? (() => {
						const u = unitVec(from, b);
						return clampPt(
							{
								x: b.x - u.x * PICKUP_REACH,
								y: b.y - u.y * PICKUP_REACH,
							},
							-OUT_CAP,
						);
					})()
				: from;
		// (Not off a bounce: he takes it once it has come to rest.)
		const last = this.ball.at(-1);
		const got = Math.max(
			this.go(pid, stop, t, speed, "run", undefined, { effort }),
			last?.kind === "rest" ? last.t0 - 150 : -Infinity,
		);
		// (Nowhere till he has it, there before it is still or not.)
		this.free.set(pid, Math.max(this.free.get(pid) ?? 0, got));
		// (Bending for it as he comes in on it - see the pickup in poses.ts.)
		this.act(pid, "pickup", got - 200, got + 300, { look: { x: b.x, y: b.y } });
		this.hold(pid, got + 150, "hold");
		if (style === "dribble") {
			this.hold(pid, got + 300, "dribble");
		}
		return got;
	}

	// Which hand a ball handler going from `a` to `b`, facing the rim his
	// team attacks, dribbles with: the one on the side he is going.
	private handFor(a: Pt, b: Pt, dir: 1 | -1): Hand {
		const lateral = (b.y - a.y) * dir;
		return lateral < -1 ? "L" : lateral > 1 ? "R" : this.ballHand;
	}

	private fly(t0: number, t1: number, from: BallEnd, to: BallEnd) {
		this.pushBall({ kind: "fly", t0, t1, from, to });
		if ("pid" in to) {
			this.holder = to.pid;
		} else {
			this.holder = undefined;
			this.ballAt = { ...to };
		}
	}

	private bounce(
		t0: number,
		t1: number,
		from: Pt3,
		to: Pt,
		hops = 2,
		h0 = 2.2,
	) {
		this.pushBall({ kind: "bounce", t0, t1, from, to, hops, h0 });
		this.pushBall({ kind: "rest", t0: t1, at: { ...to, z: 0.4 } });
		this.holder = undefined;
		this.ballAt = { ...to, z: 0.4 };
	}

	private rest(t: number, at: Pt3) {
		this.pushBall({ kind: "rest", t0: t, at });
		this.holder = undefined;
		this.ballAt = at;
	}

	// Where the ball is "now" (end of the schedule), as a point.
	private ballPoint(): Pt3 {
		if (this.holder !== undefined) {
			const p = this.posOf(this.holder);
			return { x: p.x, y: p.y, z: 3.5 };
		}
		return this.ballAt;
	}

	// Let go of the ball wherever it is: it drops to the floor and sits.
	private deadBall(t: number) {
		if (this.holder !== undefined) {
			const p = this.posAt(this.holder, t);
			const f = this.face.get(this.holder) ?? 1;
			this.bounce(
				t,
				t + 500,
				{ x: p.x + f * 0.8, y: p.y + 0.5, z: 3 },
				{ x: p.x + f * 1.8, y: p.y + 0.8 },
				1,
				0.8,
			);
		}
	}

	private setOffense(t: number, team: Side) {
		if (this.poss.at(-1)?.[1] !== team) {
			this.poss.push([t, team]);
		}
		if (this.offense !== team) {
			// A new trip: man for man again, whoever switched last time.
			this.guarding.clear();
		}
		this.offense = team;
	}

	private effect(kind: FxKind, t: number, o: Omit<Fx, "kind" | "t"> = {}) {
		this.fx.push({ kind, t, ...o });
	}

	private rand(lo: number, hi: number) {
		return lo + this.rng() * (hi - lo);
	}

	// Draws of a man's own, from who he is and when (see hash01) - for one
	// brought into something the game's own luck never had him in, so all
	// else stays as it was.
	private ownDraws(pid: number, t: number): () => number {
		let k = 0;
		return () => hash01(pid * 7919 + 104729 * k++, Math.round(t));
	}

	private finishFor(e: RawEvent | undefined): Finish | undefined {
		return e ? finishOf(e, this.gid, this.gender) : undefined;
	}

	// A lineup in slot order: whoever brings the ball up at the top, then by
	// position, guards out top and bigs low.
	private slots(team: Side): number[] {
		const byPos = this.byPos(team);
		const pg = this.handlerOf(team, byPos);
		return [pg, ...byPos.filter((p) => p !== pg)];
	}

	private byPos(team: Side): number[] {
		return [...this.lineup[team]].sort(
			(a, b) => (this.rank.get(a) ?? 4) - (this.rank.get(b) ?? 4) || a - b,
		);
	}

	// Who on the floor brings the ball up: a ball handler by his skills, a
	// guard where those aren't known - and, lacking either, the point.
	private handlerOf(team: Side, slots = this.byPos(team)): number {
		return (
			slots.find((p) => this.skills.get(p)?.includes("B")) ??
			slots.find((p) => !this.skills.has(p) && (this.rank.get(p) ?? 4) <= 2) ??
			slots[0]!
		);
	}

	// Whether he would bring it up himself rather than look for a guard: a
	// ball handler, or, his skills unknown, anyone short of a big.
	private handles(pid: number): boolean {
		const skills = this.skills.get(pid);
		return skills
			? skills.includes("B") || pid === this.handlerOf(this.teamOf(pid))
			: (this.rank.get(pid) ?? 4) < 5;
	}

	// Who pushes a break: the man who has it if he would bring it up himself,
	// else the ball handler he gives it to.
	private breakHolder(team: Side): number | undefined {
		const h = this.holder;
		return h === undefined || this.handles(h) ? h : this.handlerOf(team);
	}

	private teamOf(pid: number): Side {
		return this.team.get(pid) ?? 0;
	}

	private seatOf(pid: number): Pt {
		return { ...(this.seat.get(pid) ?? TABLE) };
	}

	// ---- between plays -----------------------------------------------------------

	// THE PICTURE NEVER CUTS. Where it would have cut past dead time - the ball
	// taken out after a basket and brought up, the walk to an inbound - the
	// five on each side go there for real, and the picture runs through it
	// fast (see hurry).

	// The ball to him: handed on by whoever has it, or picked up off the floor
	// by whoever is nearest and given to him. Returns when he has it.
	private ballTo(pid: number, t: number): number {
		const h = this.holder;
		if (h === pid) {
			return t;
		}
		if (h !== undefined) {
			return this.passTo(
				h,
				pid,
				Math.max(t, this.free.get(h) ?? 0),
				"chest",
				true,
			);
		}
		const ten = [...this.slots(0), ...this.slots(1)];
		this.bounceClear(ten);
		const b = this.ballPoint();
		const near = ten.sort(
			(a, c) => dist(this.posOf(a), b) - dist(this.posOf(c), b),
		)[0];
		if (near === undefined) {
			return t;
		}
		const got = this.pickUp(near, t, JOG);
		return near === pid
			? got + 300
			: this.passTo(near, pid, got + 400, "chest", true);
	}

	// When the man with the ball is free to go somewhere with it: done with
	// what he was doing, and the ball really in his hands by then - not still
	// on its way to him, or bouncing loose for him to pick up.
	private hasItFrom(pid: number): number {
		return Math.max(this.free.get(pid) ?? 0, this.ball.at(-1)?.t0 ?? 0);
	}

	// The ball left on the floor at the whistle, before an inbound: whoever
	// is nearest it now picks it up - before anyone sets off for his place,
	// not whoever ends up nearest it once they are all there (from the far
	// end of the floor, say, with the inbound to come to him there).
	private pickUpLoose(t: number) {
		if (this.holder !== undefined) {
			return;
		}
		const ten = [...this.slots(0), ...this.slots(1)];
		this.bounceClear(ten);
		const b = this.ballPoint();
		const near = ten.sort(
			(a, c) => dist(this.posOf(a), b) - dist(this.posOf(c), b),
		)[0];
		if (near !== undefined) {
			// Nowhere with it, nor rid of it, till he has it.
			const got = this.pickUp(near, t, JOG);
			this.free.set(near, Math.max(this.free.get(near) ?? 0, got + 300));
		}
	}

	// The ball tossed to a spot (an official's hands), by whoever has it - or
	// by whoever picks it up off the floor. Returns when it gets there.
	private tossTo(to: Pt3, t: number): number {
		let pid = this.holder;
		let start = t;
		if (pid === undefined) {
			const ten = [...this.slots(0), ...this.slots(1)];
			this.bounceClear(ten);
			const b = this.ballPoint();
			pid = ten.sort(
				(a, c) => dist(this.posOf(a), b) - dist(this.posOf(c), b),
			)[0];
			if (pid === undefined) {
				this.fly(t, t + 650, this.ballOrigin(), to);
				return t + 650;
			}
			start = this.pickUp(pid, t, JOG) + 350;
		}
		start = Math.max(start, this.free.get(pid) ?? 0);
		const flight = Math.max(550, passMs(dist(this.posOf(pid), to)) * 1.4);
		this.act(pid, "pass", start, start + 300, { look: { x: to.x, y: to.y } });
		const thrown = Math.max(start + 120, this.gather(pid, start));
		this.fly(thrown, thrown + flight, { pid }, to);
		// He goes nowhere till it is gone.
		this.free.set(pid, Math.max(this.free.get(pid) ?? 0, start + 300));
		return thrown + flight;
	}

	// Everyone to his spot - walking, or jogging if it is a long way - facing
	// the way given when he gets there. Returns when the last of them is there.
	private walkTo(
		spots: { pid: number; at: Pt; face?: 1 | -1; man?: number }[],
		t: number,
	): number {
		let done = t;
		spots.forEach(({ pid, at, face, man }, j) => {
			const n = this.track(pid)?.moves.length ?? 0;
			const from = this.posOf(pid);
			const d = dist(from, at);
			const far = d > 25;
			// Each at his own pace, off when he is ready - and, a long way to
			// go, not on the same line as the man beside him.
			const pace = (far ? JOG : WALK * 1.4) * (0.88 + 0.24 * hash01(pid, t));
			const off = t + 80 + j * 60 + 260 * hash01(pid, t + 1);
			let there: number;
			if (d > 15) {
				const u = unitVec(from, at);
				const side = (hash01(pid, t + 2) - 0.5) * 5;
				const W = clampPt({
					x: from.x + (at.x - from.x) * 0.5 - u.y * side,
					y: from.y + (at.y - from.y) * 0.5 + u.x * side,
				});
				const anim = far ? "run" : "walk";
				there = this.go(pid, W, off, pace, anim, undefined, { onto: pace });
				there = this.go(pid, at, there, pace, anim);
			} else {
				there = this.go(pid, at, off, pace, far ? "run" : "walk");
			}
			if (man !== undefined) {
				this.marks(pid, man, n);
			}
			if (face !== undefined) {
				this.turn(pid, there, face);
			}
			done = Math.max(done, there);
		});
		return done;
	}

	// After a basket: the ball out of the net, the nearest of his teammates
	// takes it out of bounds behind the baseline and inbounds it to the point
	// guard, who comes back for it - while the rest head up the floor and the
	// team that scored gets back. Returns when the point guard has it.
	private inboundAfterMake(team: Side, t: number, quick = false): number {
		const dir = attackDir(team);
		const off = this.slots(team);
		const def = this.slots(other(team));
		const pg = off[0]!;
		this.bounceClear([...off, ...def]);
		// Where the ball ends up, and when it gets there.
		const b = this.ballPoint();
		const last = this.ball.at(-1);
		const still = last?.kind === "rest" ? last.t0 : t;
		const inb =
			off
				.filter((p) => p !== pg)
				.sort((a, c) => dist(this.posOf(a), b) - dist(this.posOf(c), b))[0] ??
			pg;
		const side = b.y < COURT_H / 2 ? -1 : 1;
		const out = {
			x: dir === 1 ? -1.4 : COURT_W + 1.4,
			y: COURT_H / 2 + side * this.rand(4, 9),
		};
		// When it went through: everybody is on with what comes next from a
		// moment after that - once he is done with what he is doing (coming
		// down from the shot, a bump with a teammate).
		const made = this.beats.at(-1);
		const scored =
			made && resultOf(made.type)?.kind === "make"
				? Math.min(t, made.actionStart + MADE_BACK_MS)
				: t;
		// (No later than they would have been on with it anyway.)
		const readyAt = (pid: number, from: number, most: number) => {
			for (const a of this.track(pid)?.acts ?? []) {
				if (a.t1 > from && a.t0 < t) {
					from = Math.max(from, a.t1);
				}
			}
			return Math.min(from, most);
		};
		const on = readyAt(inb, scored + 150, t);
		const got =
			this.offTheHop(inb, on) ??
			this.pickUp(inb, Math.max(on, still - 700), quick ? RUN : JOG);
		const there = quick
			? this.go(inb, out, got + 120, JOG, "run")
			: this.go(inb, out, got + 350, WALK * 1.5, "walk");
		this.turn(inb, there, dir);
		// The point guard comes back to a few strides in from him.
		const gets = clampPt({
			x: out.x + dir * this.rand(11, 15),
			y: out.y - side * this.rand(2, 6),
		});
		// (A few steps on up the floor first, then back to the ball as it
		// is thrown in - not stood waiting on it.)
		const away = clampPt({ x: gets.x + dir * COME_BACK, y: gets.y });
		this.go(pg, away, readyAt(pg, scored + 150, t + 150), JOG, "run");
		this.go(
			pg,
			gets,
			Math.max(
				this.free.get(pg) ?? 0,
				there + (quick ? 60 : 250) - runMs(COME_BACK, JOG),
			),
			JOG,
			"run",
		);
		// The rest up the floor, getting there a beat before the ball does;
		// the team that scored back down it, each picking up the man nearest
		// him.
		this.fitSpots(team);
		const spots = this.setSpots(team, 0);
		this.matchUp(team, spots);
		const upBy =
			there +
			600 +
			runMs(dist({ x: out.x + dir * 13, y: out.y }, spots[0]!), DRIBBLE);
		const up = off.filter((pid) => pid !== pg && pid !== inb);
		for (const pid of up) {
			this.free.set(
				pid,
				Math.max(this.free.get(pid) ?? 0, readyAt(pid, scored + 200, t + 200)),
			);
		}
		this.fillLanes(
			team,
			off
				.map((pid, j) => ({ pid, to: spots[j] ?? spots[0]!, j }))
				.filter(({ pid }) => up.includes(pid)),
			scored + 200,
			upBy,
			(gets.y + spots[0]!.y) / 2,
		);
		// (The team that scored on its way back from as soon as it is through
		// - once each is done with what he is doing, the shooter coming down,
		// a bump with a teammate - not stood at the wrong end till it is
		// taken out.)
		off.forEach((man, j) => {
			const pid = this.defenderOf(man);
			if (pid === undefined || !def.includes(pid)) {
				return;
			}
			const n = this.track(pid)?.moves.length ?? 0;
			const back = this.goBy(
				pid,
				guardSpot(team, spots[j] ?? spots[0]!),
				readyAt(pid, scored + 250 + j * 80, t + 250 + j * 80),
				t + 3600,
				"run",
			);
			this.marks(pid, man, n);
			// Back down the floor, he calls out who he has.
			this.gesture(
				pid,
				"point",
				t + 700 + j * 110,
				Math.min(back, t + 1600 + j * 110),
				man,
				0.3,
			);
		});
		const tIn = this.passTo(inb, pg, there + (quick ? 60 : 250));
		if (!quick) {
			this.hurry(t + 300, tIn);
		}
		return tIn;
	}

	// A dead ball: everybody to his place for the inbound - the inbounder out
	// of bounds where it went out, with the ball - and it is thrown in. In the
	// frontcourt they set up around it; back in the backcourt the point guard
	// comes to get it and the rest go on up the floor. Returns when the man it
	// is thrown to has it.
	private inboundFrom(
		team: Side,
		t: number,
		at: Pt,
		// In to him, there, rather than to the point guard come back for it.
		to?: { pid: number; at: Pt },
	): number {
		const dir = attackDir(team);
		const off = this.slots(team);
		const def = this.slots(other(team));
		// One of theirs with it already - brought back from the huddle -
		// takes it out himself (in to the next man, if it was to go in to
		// him).
		const h0 = this.holder;
		const takes = h0 !== undefined && off.includes(h0) && h0 !== to?.pid;
		const receiver =
			to?.pid ?? (takes && h0 === off[0] ? (off[1] ?? off[0]!) : off[0]!);
		const inbounder =
			takes && h0 !== receiver
				? h0
				: [off[2], off[1], off.at(-1), off[0]].find(
						(pid) => pid !== undefined && pid !== receiver,
					)!;
		const far = at.y < COURT_H / 2;
		// From behind the baseline, or from the sideline.
		const baseline = at.x < 0 || at.x > COURT_W;
		const oob = baseline
			? { x: at.x, y: Math.min(44, Math.max(6, at.y)) }
			: {
					x: Math.min(COURT_W - 3, Math.max(3, at.x)),
					y: far ? -1.4 : COURT_H + 1.4,
				};
		const back = this.inBackcourt(team, oob);
		this.fitSpots(team);
		const spots = this.setSpots(team, 0);
		// From the spot it goes in from, k feet into the floor.
		const inward = baseline
			? { x: at.x < 0 ? 1 : -1, y: 0 }
			: { x: 0, y: far ? 1 : -1 };
		const inside = (k: number): Pt => ({
			x: oob.x + inward.x * k,
			y: oob.y + inward.y * k,
		});
		const target = (pid: number, j: number): Pt =>
			pid === inbounder
				? oob
				: pid === receiver
					? to
						? clampPt(to.at)
						: baseline
							? clampPt({ x: oob.x - dir * 9, y: oob.y + (25 - oob.y) * 0.4 })
							: clampPt({
									x: oob.x + dir * (back ? 4 : 7),
									y: far ? 9 : COURT_H - 9,
								})
					: (spots[j] ?? spots[0]!);
		this.pickUpLoose(t);
		// Whoever has the ball gives it up to the inbounder before he goes
		// anywhere himself - never off up the floor with it.
		const holding = this.holder !== inbounder ? this.holder : undefined;
		let ready = t;
		const placeOff = (pid: number, j: number, from: number) => {
			const P = target(pid, j);
			const d = dist(this.posOf(pid), P);
			const arrive = this.go(
				pid,
				P,
				from,
				d > 25 ? JOG : WALK * 1.4,
				d > 25 ? "run" : "walk",
			);
			this.turn(pid, arrive, dir);
			if (pid === inbounder || pid === receiver || !back) {
				ready = Math.max(ready, arrive);
			}
		};
		// Each defender picks up the man nearest him, mostly across the floor
		// (see matchUp), not a man on the far side past his teammates.
		this.matchUp(
			team,
			off.map((pid, j) => target(pid, j)),
		);
		const placeDef = (pid: number, j: number, from: number) => {
			const man = off[j] ?? off[0]!;
			// The man on the inbounder: a step inside the line, shading the side
			// the pass wants to go, hands up at it.
			const P =
				man === inbounder
					? baseline
						? {
								x: inside(3.4).x,
								y: oob.y + (oob.y > COURT_H / 2 ? -1.4 : 1.4),
							}
						: { x: inside(3.4).x + dir * 1.4, y: inside(3.4).y }
					: guardSpot(team, target(man, j), 0.25);
			const n = this.track(pid)?.moves.length ?? 0;
			const d = dist(this.posOf(pid), P);
			const arrive = this.go(
				pid,
				P,
				from,
				d > 25 ? JOG : WALK * 1.4,
				d > 25 ? "run" : "walk",
			);
			this.marks(pid, man, n);
			if (man === inbounder) {
				this.lookAt(pid, arrive, oob);
			} else {
				this.turn(pid, arrive, -dir as 1 | -1);
			}
		};
		// Whoever picked it up doesn't stand on the spot it goes in from, for
		// the inbounder to walk into: a step aside and in, and he gives it up
		// from there.
		if (holding !== undefined) {
			const hp = this.posOf(holding);
			if (dist(hp, oob) < 5) {
				const side = baseline
					? hp.y >= oob.y
						? 1
						: -1
					: hp.x >= oob.x
						? 1
						: -1;
				const step = inside(3);
				this.go(
					holding,
					clampPt(
						baseline
							? { x: step.x, y: step.y + side * 4.5 }
							: { x: step.x + side * 4.5, y: step.y },
					),
					t + 60,
					WALK * 1.4,
					"walk",
				);
			}
		}
		off.forEach((pid, j) => {
			if (pid !== holding) {
				placeOff(pid, j, t + 80 + j * 70);
			}
		});
		off.forEach((man, j) => {
			const pid = this.defenderOf(man);
			if (pid !== undefined && pid !== holding && def.includes(pid)) {
				placeDef(pid, j, t + 120 + j * 70);
			}
		});
		// The ball to him from the official - as he gets there, but not
		// long before the rest are set: he doesn't stand holding it.
		const has = this.ballTo(
			inbounder,
			Math.max(t + 300, (this.free.get(inbounder) ?? t) - 1200, ready - 1500),
		);
		if (holding !== undefined) {
			const j = off.indexOf(holding);
			if (j >= 0) {
				placeOff(holding, j, has);
			} else if (def.includes(holding)) {
				const k = off.findIndex((m) => this.defenderOf(m) === holding);
				placeDef(holding, k >= 0 ? k : def.indexOf(holding), has);
			}
		}
		this.hold(inbounder, Math.max(has, this.free.get(inbounder) ?? 0), "hold");
		this.lookAt(inbounder, ready + 1, this.posOf(receiver));
		const go = Math.max(ready, has) + 500;
		this.hurry(t + 300, go - 200);
		const tIn = this.passTo(inbounder, receiver, go);
		const guard = this.defenderOf(inbounder);
		if (guard !== undefined) {
			const up = Math.max(this.free.get(guard) ?? 0, go - 900);
			const down = Math.min(go + 150, tIn - 650);
			if (down - up > 300) {
				this.act(guard, "guard", up, down, { look: oob });
			}
		}
		// In after it, up the line - wide of the man he gave it to, not
		// through him.
		const stepIn = this.go(
			inbounder,
			clampPt(
				baseline
					? {
							x: oob.x + inward.x * 3.5,
							y: oob.y + (oob.y > COURT_H / 2 ? 4 : -4),
						}
					: { x: oob.x + dir * 4, y: oob.y + inward.y * 3.6 },
			),
			tIn - 100,
			JOG,
			"run",
		);
		if (!back) {
			this.settle(team, 0, tIn - 600, tIn + 500, [receiver, inbounder]);
			const j = off.indexOf(inbounder);
			this.go(inbounder, spots[j] ?? spots[0]!, stepIn, JOG, "run");
			this.turn(
				inbounder,
				Math.max(tIn + 500, this.free.get(inbounder) ?? 0),
				dir,
			);
		}
		this.motionTeam = team;
		this.motion = 0;
		return tIn;
	}

	// ---- formations -----------------------------------------------------------

	private setSpots(team: Side, motion: number): Pt[] {
		const phase =
			MOTION_OFFENSE_SPOTS[Math.min(MOTION_OFFENSE_SPOTS.length - 1, motion)]!;
		const fit = this.spotFit?.team === team ? this.spotFit : undefined;
		return phase.map((_, j) => {
			const s = phase[fit?.order[j] ?? j] ?? phase[j]!;
			return spot(team, s.depth, fit?.flip ? COURT_H - s.across : s.across);
		});
	}

	// THE SIDE HE IS ON. Coming up the floor into the set, a man takes the
	// spot on the side he is already on: the set is run to whichever side
	// fits the five as they are, and the two wings - and the two bigs - take
	// whichever of their two spots is nearer. Nobody runs the width of the
	// floor across his teammates to get to his.
	private fitSpots(team: Side) {
		const men = this.slots(team);
		const phase = MOTION_OFFENSE_SPOTS[0]!;
		let best: { flip: boolean; order: number[]; cost: number } | undefined;
		for (const flip of [false, true]) {
			for (const order of SPOT_ORDERS) {
				let cost = 0;
				men.forEach((pid, j) => {
					const s = phase[order[j]!] ?? phase[j]!;
					const S = spot(team, s.depth, flip ? COURT_H - s.across : s.across);
					const P = this.posOf(pid);
					// (Across the floor counts double: up it, everybody goes.)
					cost += dist(P, S) + Math.abs(P.y - S.y);
				});
				if (!best || cost < best.cost - 0.01) {
					best = { flip, order, cost };
				}
			}
		}
		if (best) {
			this.spotFit = { team, flip: best.flip, order: best.order };
		}
	}

	// Who picks up whom, coming back: each defender the man nearest him -
	// across the floor, mostly, since they all run its length - among those
	// near his own size: a guard is not left on the center for the sake of a
	// few strides.
	private matchUp(team: Side, spots: Pt[], rankCost = MATCH_RANK) {
		const men = this.slots(team);
		const def = this.slots(other(team));
		if (men.length !== 5 || def.length !== 5) {
			return;
		}
		let best: { perm: number[]; cost: number } | undefined;
		for (const perm of PERMS5) {
			let cost = 0;
			for (let j = 0; j < 5; j++) {
				const d = def[perm[j]!]!;
				const D = this.posOf(d);
				const S = spots[j] ?? spots[0]!;
				cost +=
					Math.abs(D.y - S.y) * 1.5 +
					Math.abs(D.x - S.x) * 0.3 +
					Math.abs((this.rank.get(d) ?? 4) - (this.rank.get(men[j]!) ?? 4)) *
						rankCost;
			}
			if (!best || cost < best.cost - 0.01) {
				best = { perm, cost };
			}
		}
		if (best) {
			men.forEach((m, j) => {
				this.guarding.set(m, def[best.perm[j]!]!);
			});
		}
	}

	// Everybody but `except` into the half-court set at motion phase `motion`,
	// the defense shadowing them. (The length of the floor to go, up it in
	// their lanes - not straight up it beside the man bringing it up, where
	// he comes up it `ballY` across the floor.)
	private settle(
		team: Side,
		motion: number,
		t0: number,
		by: number,
		except: number[] = [],
		ballY?: number,
	) {
		const spots = this.setSpots(team, motion);
		const off = this.slots(team);
		const dir = attackDir(team);
		const lanes: { pid: number; to: Pt; j: number }[] = [];
		off.forEach((pid, j) => {
			const target = spots[j] ?? spots[0]!;
			if (!except.includes(pid)) {
				if ((target.x - this.posOf(pid).x) * dir >= LANE_RUN) {
					lanes.push({ pid, to: target, j });
				} else {
					this.goBy(pid, target, t0 + j * 40, by, "run");
					this.turn(pid, Math.max(by, this.free.get(pid) ?? 0), dir);
				}
			}
			const d = this.defenderOf(pid);
			if (d !== undefined && !except.includes(d)) {
				const n = this.track(d)?.moves.length ?? 0;
				this.goBy(
					d,
					guardSpot(team, target),
					t0 + 60 + j * 50,
					by + 120,
					"run",
				);
				this.marks(d, pid, n);
				this.turn(d, Math.max(by + 120, this.free.get(d) ?? 0), -dir as 1 | -1);
			}
		});
		if (lanes.length > 0) {
			this.fillLanes(team, lanes, t0, by, ballY);
			for (const m of lanes) {
				this.turn(m.pid, Math.max(by, this.free.get(m.pid) ?? 0), dir);
			}
		}
	}

	// ---- possession -------------------------------------------------------------

	// A pass - chest, bounce or overhead, by how far it goes, unless the set
	// says how. Thrown so it gets there as he does: a man still on his cut is
	// led, not waited on with the ball hanging in the air. Returns when the
	// receiver has it.
	private passTo(
		from: number,
		to: number,
		t: number,
		style?: PassStyle,
		// Handed over at a dead ball - not a pass in play.
		handOff = false,
	): number {
		// Never back over half court: a man left back there comes up over it
		// for the ball first.
		const dir = attackDir(this.teamOf(from));
		const R = this.posOf(to);
		if (
			!handOff &&
			(this.posOf(from).x - COURT_W / 2) * dir > 0 &&
			(R.x - COURT_W / 2) * dir < 2
		) {
			this.go(
				to,
				clampPt({ x: COURT_W / 2 + dir * 5, y: R.y }),
				Math.max(t, this.free.get(to) ?? 0),
				RUN * 0.85,
				"run",
			);
		}
		this.backOut(from, to, t);
		const a = this.posOf(from);
		const b = this.posOf(to);
		const d = dist(a, b);
		const toward = b.x >= a.x ? 1 : -1;
		const kind: PassStyle =
			style ??
			(d >= 22 && this.rng() < 0.55
				? "overhead"
				: d >= 9 && d < 22 && this.rng() < 0.3
					? "bounce"
					: "chest");
		const flight =
			kind === "lob"
				? Math.max(750, passMs(d) * 1.7)
				: passMs(d) * (kind === "bounce" ? 1.15 : 1);
		const over = kind === "overhead" || kind === "lob";
		const wind = RELEASE_MS + (over ? OVERHEAD_WIND : 0);
		const ready = Math.max(
			t,
			this.free.get(from) ?? 0,
			(this.free.get(to) ?? 0) + 40 - flight - wind,
		);
		if (!handOff) {
			this.keepComing(from, ready);
		}
		// Off a live dribble - on the move, mostly - a pass that is not up over
		// the top goes out of the hand he dribbles with, one hand behind it -
		// a bounce pass pushed at the floor (see PUSH_SHARE).
		const dribbled =
			(kind === "chest" || kind === "bounce") && !handOff
				? this.dribbleFrom(from, ready) !== undefined
				: false;
		const pushHand = dribbled
			? (this.dribbleHandAt(ready) ?? this.ballHand)
			: undefined;
		const onTheMove = (this.track(from)?.moves ?? []).some(
			(m) => m.t0 <= ready && m.t1 > ready + 60,
		);
		// Off the dribble, once he has it in both hands (or in the one).
		const got = this.gather(from, ready);
		let start = Math.max(ready, got);
		// Just caught facing the man who threw it, he turns to the man he
		// throws it on to before it goes - not sending it off over his
		// shoulder halfway round.
		const caught = this.track(from)?.acts.findLast(
			(x) => x.anim === "catch" && x.t0 < start && x.t1 >= start - 60,
		);
		if (caught?.look) {
			let turn =
				Math.atan2(b.y - a.y, b.x - a.x) -
				Math.atan2(caught.look.y - a.y, caught.look.x - a.x);
			turn = Math.abs(turn - Math.round(turn / (2 * Math.PI)) * 2 * Math.PI);
			start += Math.max(0, (turn - 0.5) / TURN_RATE - wind);
		}
		// (Only straight off the bounce: not once he has held it a while in
		// both hands.)
		const push =
			pushHand !== undefined &&
			start - got <= PUSH_OFF_MS &&
			hash01(from * 13 + 5, Math.floor(ready)) < PUSH_SHARE[onTheMove ? 0 : 1];
		// Now and then, on the move to a man out the side of his other hand -
		// ahead of him, or square to him - behind his back: flipped round
		// behind him with him going on the way he goes, his eyes up the floor.
		// (On the move: still going, or let go of as he gets there.)
		const going = this.track(from)?.moves.findLast(
			(m) => m.t0 <= ready && m.t1 >= ready - BEHIND_STILL_MS,
		);
		const g =
			going &&
			dist(going.from, going.to) >= (BEHIND_FTPS * (going.t1 - going.t0)) / 1000
				? unitVec(going.from, going.to)
				: undefined;
		const ahead = g ? ((b.x - a.x) * g.x + (b.y - a.y) * g.y) / (d || 1) : 0;
		// (His left, going that way, and whether the man is on it.)
		const onLeft = g ? (b.x - a.x) * g.y - (b.y - a.y) * g.x > 0 : false;
		const behind =
			push &&
			kind === "chest" &&
			g !== undefined &&
			d >= BEHIND_FT[0] &&
			d <= BEHIND_FT[1] &&
			ahead > BEHIND_AHEAD[0] &&
			ahead < BEHIND_AHEAD[1] &&
			onLeft === (pushHand === "R") &&
			hash01(from * 7 + 3, Math.floor(ready)) < BEHIND_SHARE;
		// Open, he calls for it: a hand up.
		if (d >= 12) {
			this.gesture(to, "hand", start - 700, start + wind - 60, undefined, 0.45);
		}
		// Stood waiting on it out on the floor, he gets himself open for it:
		// a step in at the rim, then back out to meet it as it comes - his
		// eyes on the ball all the while.
		const rim = { x: rimX(this.teamOf(to)), y: COURT_H / 2 };
		if (
			!handOff &&
			d >= V_CUT_PASS &&
			dist(b, rim) >= V_CUT_OUT &&
			(this.free.get(to) ?? 0) < start - V_CUT_MS &&
			(b.x - COURT_W / 2) * dir > 0
		) {
			const inn = unitVec(b, rim);
			const inSpot = clampPt({
				x: b.x + inn.x * V_CUT_IN,
				y: b.y + inn.y * V_CUT_IN,
			});
			const back = unitVec(b, a);
			const met = clampPt({
				x: b.x + back.x * V_CUT_MEET,
				y: b.y + back.y * V_CUT_MEET,
			});
			// (Out on the arc, he meets it there - still behind the line, set
			// to shoot it: not drawn in over it toward the man throwing it,
			// and caught a step inside to back out of with it.)
			const meet =
				dist(b, rim) >= 23.75 - ON_THE_LINE
					? (behindArc(this.teamOf(to), met) ?? met)
					: met;
			const arrive0 = start + wind + flight;
			const face = -toward as 1 | -1;
			const inAt = this.goBy(
				to,
				inSpot,
				start - V_CUT_MS,
				start - V_CUT_MS + 380,
				"run",
				face,
			);
			this.goBy(
				to,
				meet,
				Math.max(inAt, arrive0 - 480),
				arrive0 - 40,
				"run",
				face,
			);
		}
		this.act(
			from,
			behind
				? "passBehind"
				: over
					? "passOverhead"
					: kind === "bounce"
						? push
							? "passBouncePush"
							: "passBounce"
						: push
							? "passPush"
							: "pass",
			start,
			start + wind + 180,
			{
				face: behind && g ? (g.x >= 0 ? 1 : -1) : toward,
				look: behind && g ? { x: a.x + g.x * 12, y: a.y + g.y * 12 } : { ...b },
				...(push && pushHand === "L" ? { mirror: true as const } : {}),
			},
		);
		const release = start + wind;
		const arrive = release + flight;
		if (kind === "bounce") {
			// Off the floor two-thirds of the way there, up into his hands.
			const hit = {
				x: a.x + (b.x - a.x) * 0.64,
				y: a.y + (b.y - a.y) * 0.64,
				z: BALL_R,
			};
			const tHit = release + (arrive - release) * 0.58;
			this.fly(release, tHit, { pid: from }, hit);
			this.fly(tHit, arrive, hit, { pid: to });
		} else {
			this.fly(release, arrive, { pid: from }, { pid: to });
		}
		this.act(to, "catch", arrive - 90, arrive + 110, {
			face: -toward as 1 | -1,
			look: { ...this.posOf(from) },
		});
		this.hold(to, arrive, "hold");
		this.free.set(from, Math.max(this.free.get(from) ?? 0, start + wind + 180));
		this.free.set(to, Math.max(this.free.get(to) ?? 0, arrive + 110));
		return arrive + 110;
	}

	// Pushing it up the floor with nothing for him to do yet when he gets
	// where he is going - the man he throws to still on his way, the wings
	// not up with him for what comes next - he doesn't get there and stand
	// bouncing it: he takes longer about getting there (up to half his
	// pace), still coming as they get there. Until `t`, if he has the ball,
	// his last run was a push of it up the floor, and nothing since.
	private keepComing(pid: number, t: number) {
		const tr = this.track(pid);
		const came = tr?.moves.at(-1);
		if (
			!tr ||
			!came ||
			this.holder !== pid ||
			came.anim !== "dribble" ||
			t - came.t1 <= PUSH_WAIT ||
			dist(came.from, came.to) < PUSH_FT ||
			tr.acts.some((a) => a.t1 > came.t1 && a.t0 < t)
		) {
			return;
		}
		came.t1 = Math.min(t - 150, came.t0 + (came.t1 - came.t0) * 2);
		came.v1 = undefined;
	}

	// The whistle going away from the ball: the man with it, stood with it
	// in the front court since he got it, isn't still stood there waiting on
	// it - he puts it on the floor and probes, a few dribbles in at his man
	// (from in close already, across), and is on the move as it goes. His
	// man goes with him.
	private probe(pid: number, t: number) {
		const tr = this.track(pid);
		if (!tr || this.holder !== pid) {
			return;
		}
		const team = tr.team;
		const dir = attackDir(team);
		const since = Math.max(this.free.get(pid) ?? 0, tr.moves.at(-1)?.t1 ?? 0);
		const start = Math.max(since + PROBE_LOOK, t - PROBE_MS);
		const A = this.posOf(pid);
		if (
			t - start < 700 ||
			(A.x - COURT_W / 2) * dir < 8 ||
			tr.acts.some((a) => a.t1 > start && a.t0 < t)
		) {
			return;
		}
		const rim = { x: rimX(team), y: COURT_H / 2 };
		const out = dist(A, rim);
		const u = unitVec(A, rim);
		// In toward the rim - not inside the lane's top - or, there already,
		// across toward the middle.
		const across = A.y >= COURT_H / 2 ? -1 : 1;
		const P = clampPt(
			out > PROBE_IN + 3
				? {
						x: A.x + u.x * Math.min(6, out - PROBE_IN),
						y: A.y + u.y * Math.min(6, out - PROBE_IN),
					}
				: { x: A.x - dir * 1, y: A.y + across * 4.5 },
		);
		const L = dist(A, P);
		if (L < 2.5) {
			return;
		}
		const end = t - 120;
		const speed = Math.min(9, Math.max(4, L / ((end - start) / 1000)));
		this.hold(pid, start, "dribble");
		const there = this.go(pid, P, start, speed, "dribble", dir);
		const d = this.defenderOf(pid);
		if (d !== undefined && (this.free.get(d) ?? 0) <= start) {
			this.shadow(
				d,
				this.defensePoint(team, P, P, true),
				start + 120,
				there + 120,
				team,
				pid,
			);
		}
	}

	// The outlet: the man who came down with it holds it, looking up the
	// floor, while the ball handler comes back to the wing ahead of him
	// calling for it - and gives it up as he gets there.
	// Into the lane on the dribble with the man he wants not there yet, he
	// doesn't stand in it bouncing the ball: he dribbles back out, facing
	// the rim, and throws it from there.
	private backOut(from: number, to: number, t: number) {
		const team = this.teamOf(from);
		const dir = attackDir(team);
		const rim = rimPt(team);
		const A = this.posOf(from);
		const start = Math.max(t, this.free.get(from) ?? 0);
		if (dist(A, rim) > 15 || this.dribbleFrom(from, start) === undefined) {
			return;
		}
		const wait =
			(this.free.get(to) ?? 0) +
			40 -
			passMs(dist(A, this.posOf(to))) -
			RELEASE_MS -
			start;
		const k = Math.min(9, ((wait - 500) / 1000) * DRIBBLE_SPEED.retreat!);
		if (k < 3) {
			return;
		}
		const out = unitVec(rim, A);
		const u = unitVec({ x: 0, y: 0 }, { x: out.x - dir * 0.8, y: out.y });
		this.go(
			from,
			inPlay({ x: A.x + u.x * k, y: A.y + u.y * k }),
			start + 200,
			DRIBBLE_SPEED.retreat!,
			"dribble",
			dir,
		);
	}

	private outlet(
		from: number,
		to: number,
		t: number,
		speed: number,
		ahead: number,
	): number {
		const team = this.teamOf(to);
		const h = this.posOf(from);
		// On his own side of him: he doesn't cut across in front of the man
		// with it - and the men running their lanes - to get to the other.
		const side = this.posOf(to).y < h.y ? -1 : 1;
		const meet = clampPt({
			x: h.x + attackDir(team) * ahead,
			y: Math.min(44, Math.max(6, h.y + side * 8)),
		});
		const there = this.go(to, meet, t, speed, "run");
		const pass = Math.max(t + 150, there - 450);
		const still = Math.max(t, this.free.get(from) ?? 0);
		const look = Math.min(pass, still + 800);
		if (look - still > 200) {
			// Looking up the floor for him - with it in his hands, or still on
			// his dribble if he is (not holding it with the ball bouncing on
			// under him).
			const seg = this.ball.findLast((s) => s.t0 <= still);
			if (seg?.kind === "hold" && seg.pid === from && seg.style !== "hold") {
				this.turn(from, still, attackDir(team));
				this.lookAt(from, still, { ...meet });
			} else {
				this.act(from, "hold", still, look, {
					face: attackDir(team),
					look: { ...meet },
				});
			}
		}
		if (pass - look > 400) {
			// A long way back for it: he puts it down and brings it a few
			// dribbles toward him meanwhile.
			const u = unitVec(h, meet);
			const k = Math.min(dist(h, meet) - 6, ((pass - look) / 1000) * 8);
			if (k > 1) {
				this.hold(from, look, "dribble");
				this.go(
					from,
					clampPt({ x: h.x + u.x * k, y: h.y + u.y * k }),
					look,
					8,
					"dribble",
					attackDir(team),
				);
			}
		}
		return this.passTo(from, to, pass);
	}

	private inBackcourt(team: Side, p: Pt): boolean {
		return team === 1 ? p.x < COURT_W / 2 : p.x > COURT_W / 2;
	}

	// The ball theirs in the open floor, the other five turn and get back at
	// once - not stood in their stance at the wrong end until it is brought
	// up: back down the floor into the paint and round it, the way a
	// defense gets back - the rim first, then the man (whatever they do
	// next, they do from there) - flat out, if it is a break.
	private getBack(def: Side, t: number, flatOut: boolean) {
		const rx = rimX(other(def));
		const runs: { pid: number; P: Pt; to: Pt; at: number }[] = [];
		this.slots(def).forEach((pid, j) => {
			const P = this.posOf(pid);
			const out = Math.abs(P.x - rx);
			if (out < BACK_FROM) {
				return;
			}
			runs.push({
				pid,
				P,
				to: { x: rx, y: P.y + (COURT_H / 2 - P.y) * BACK_PINCH },
				at: t + BACK_REACT + j * 60,
			});
		});
		// Each back down his own lane - two that went in to the glass
		// together not run the length of the floor shoulder to shoulder -
		// and the one going a beat after another beside him, a step behind.
		const lanes = [...runs].sort((a, b) => a.to.y - b.to.y);
		for (let k = 0; k < 12; k++) {
			for (let i = 1; i < lanes.length; i++) {
				const gap = lanes[i]!.to.y - lanes[i - 1]!.to.y;
				if (gap < BACK_LANES) {
					lanes[i - 1]!.to.y -= (BACK_LANES - gap) / 2;
					lanes[i]!.to.y += (BACK_LANES - gap) / 2;
				}
			}
			for (const r of lanes) {
				r.to.y = Math.min(COURT_H - BACK_SIDE, Math.max(BACK_SIDE, r.to.y));
			}
		}
		// Back into the paint and round it: the man down the middle back at
		// the rim, the others out to the side of it a little higher - each
		// as far out from it as he is across from the middle (BACK_DEEP and
		// BACK_WIDE), the way a defense back in time sits waiting.
		const side = Math.sign(COURT_W / 2 - rx);
		for (const r of runs) {
			const across = Math.abs(r.to.y - COURT_H / 2);
			r.to.x = rx + side * (BACK_DEEP + BACK_WIDE * across);
		}
		runs.forEach((r, i) => {
			for (const q of runs.slice(0, i)) {
				if (dist(q.P, r.P) < BACK_BESIDE) {
					r.at = Math.max(r.at, q.at + BACK_BEHIND);
				}
			}
			// (On a break, flat out.)
			this.go(
				r.pid,
				clampPt(r.to),
				r.at,
				flatOut ? SPRINT : RUN,
				"run",
				undefined,
				{ effort: flatOut ? BURST : 1 },
			);
		});
	}

	// The point guard brings it up and the five settle into their set.
	private bringUp(team: Side, t: number): number {
		const pg = this.handlerOf(team);
		// (The rest on their way up from now - not stood round the man with
		// it till he has got it out.)
		const t0 = t;
		if (this.holder !== pg && this.holder !== undefined) {
			// Get it to the point guard first.
			t = this.outlet(this.holder, pg, t, RUN, 9);
		}
		this.fitSpots(team);
		if (this.guarding.size === 0) {
			this.matchUp(team, this.setSpots(team, 0));
		}
		const top = this.setSpots(team, 0)[0]!;
		const d = dist(this.posOf(pg), top);
		const ballY = (this.posOf(pg).y + top.y) / 2;
		this.hold(pg, t, "dribble");
		const arrive = this.go(pg, top, t, DRIBBLE, "dribble", attackDir(team));
		this.settle(team, 0, t0, Math.max(t + 900, arrive - 200), [pg], ballY);
		this.motionTeam = team;
		this.motion = 0;
		return Math.max(arrive, t + (d < 4 ? 300 : 0));
	}

	// A fast break: outlet if a big has it, the ball handler pushes, the wings
	// fill the lanes, the defense sprints back with one man protecting the rim.
	private pushBreak(
		team: Side,
		t: number,
		shot?: Zone,
		shooter?: number,
	): number {
		const slots = this.slots(team);
		const pg = this.handlerOf(team);
		let handler = this.holder ?? pg;
		// (The rest are off up the floor from now, as he gets it out.)
		const t0 = t;
		if (!this.handles(handler) && pg !== handler) {
			t = this.outlet(handler, pg, t, SPRINT, 10);
			handler = pg;
		}
		// The lanes, filled by whoever is on that side already - the break
		// run to whichever side fits them.
		const others = slots.filter((p) => p !== handler);
		const laneSpots = (flip: boolean, swap: boolean): Pt[] => {
			const at = (k: number) => {
				const s = TRANSITION_OFFENSE_SPOTS[k] ?? TRANSITION_OFFENSE_SPOTS[0]!;
				return spot(team, s.depth, flip ? COURT_H - s.across : s.across);
			};
			return [
				at(0),
				...others.map((_, j) => at(swap && j < 2 ? 2 - j : j + 1)),
			];
		};
		let spots = laneSpots(false, false);
		let across = Infinity;
		for (const flip of [false, true]) {
			for (const swap of [false, true]) {
				const S = laneSpots(flip, swap);
				const c = others.reduce(
					(sum, pid, j) => sum + Math.abs(this.posOf(pid).y - S[j + 1]!.y),
					0,
				);
				if (c < across - 0.01) {
					across = c;
					spots = S;
				}
			}
		}
		this.hold(handler, t, "dribble");
		// Pushing it himself into a pull-up: he pulls up on the way, where he
		// shoots it from - not all the way in to the rim and back out.
		const pullUp =
			shooter === handler && (shot === "three" || shot === "midRange")
				? this.shotSpotNear(team, shot, this.posOf(handler))
				: undefined;
		const pushTo = pullUp ?? spots[0]!;
		const ballY = (this.posOf(handler).y + pushTo.y) / 2;
		const arrive = this.go(
			handler,
			pushTo,
			t,
			SPRINT - 3,
			"dribble",
			attackDir(team),
		);
		// (The length of the floor to go, each up it in his lane, clear of
		// the ball's - see fillLanes.)
		const lanes: { pid: number; to: Pt; j: number; by: number }[] = [];
		others.forEach((pid, j) => {
			const S = spots[j + 1] ?? spots[0]!;
			if ((S.x - this.posOf(pid).x) * attackDir(team) >= LANE_RUN) {
				lanes.push({ pid, to: S, j, by: arrive + j * 250 });
				return;
			}
			this.goBy(pid, S, t0 + j * 60, arrive + j * 250, "run");
		});
		if (lanes.length > 0) {
			this.fillLanes(team, lanes, t0, arrive, ballY);
		}
		const def0 = this.slots(other(team));
		const back4 = def0.slice(0, -1);
		let def = def0;
		if (back4.length === 4 && others.length === 4) {
			let best = Infinity;
			for (const perm of PERMS4) {
				let c = 0;
				for (let j = 0; j < 4; j++) {
					const d = back4[perm[j]!]!;
					c +=
						Math.abs(this.posOf(d).y - spots[j + 1]!.y) +
						Math.abs(
							(this.rank.get(d) ?? 4) - (this.rank.get(others[j]!) ?? 4),
						) *
							MATCH_RANK;
				}
				if (c < best - 0.01) {
					best = c;
					def = [...perm.map((k) => back4[k]!), def0.at(-1)!];
				}
			}
		}
		def.forEach((pid, j) => {
			const rimGuard = j === def.length - 1;
			const target = rimGuard
				? spot(team, 6, 25)
				: clampPt({
						x: (spots[j + 1] ?? spots[0]!).x - attackDir(team) * 7,
						y: 25 + ((spots[j + 1] ?? spots[0]!).y - 25) * 0.8,
					});
			const back = this.goBy(
				pid,
				target,
				t + 40 * j,
				arrive + 200,
				"run",
				undefined,
				BURST,
			);
			// Getting back, he calls out who he has: the last man back the
			// ball.
			const his = rimGuard ? handler : others[j];
			if (his !== undefined) {
				this.gesture(
					pid,
					"point",
					t + 350 + 90 * j,
					Math.min(back, t + 1300 + 90 * j),
					his,
					rimGuard ? 0.6 : 0.3,
				);
			}
			this.turn(
				pid,
				Math.max(arrive + 200, this.free.get(pid) ?? 0),
				-attackDir(team) as 1 | -1,
			);
		});
		this.motionTeam = team;
		this.motion = 0;
		return arrive;
	}

	// Swing it: the ball goes to the next handler and the offense moves into the
	// next phase of its motion.
	private swing(team: Side, t: number): number {
		const m = Math.min(MOTION_OFFENSE_SPOTS.length - 1, this.motion + 1);
		const slots = this.slots(team);
		const part = MOTION_HANDLER_SLOT[m] ?? 1;
		const fit = this.spotFit?.team === team ? this.spotFit : undefined;
		let receiver = slots[fit ? fit.order.indexOf(part) : part];
		if (receiver === undefined || receiver === this.holder) {
			receiver = slots.find((p) => p !== this.holder) ?? slots[0]!;
		}
		this.settle(team, m, t, t + 700);
		const tCatch = this.passTo(
			this.holder ?? slots[0]!,
			receiver,
			Math.max(t + 350, (this.free.get(receiver) ?? 0) - 250),
		);
		this.hold(receiver, tCatch, "dribble");
		this.motion = m;
		return tCatch + 150;
	}

	// Get the offense from wherever the ball is to "in a set, ball in hand",
	// spending the clock the sim says the possession took. Given a way to call
	// a set, it runs into that set instead of the plain motion offense.
	private develop(
		team: Side,
		t: number,
		gap: number | undefined,
		call?: (entry: Entry) => Running | undefined,
		// Where the trip's shot comes from, if it ends in one - not a turnover
		// or a whistle.
		shot?: Zone,
		// A heave at the buzzer: just the ball in somebody's hands - no break
		// to run, no set, nobody bringing it up for one.
		heave = false,
		// Who takes the shot, if it is known.
		shooter?: number,
	): { t: number; run?: Running } {
		const phase = this.phase;
		const changed = this.offense !== team;
		if (changed) {
			this.guarding.clear();
		}
		this.setOffense(t, team);
		this.quickIn = undefined;
		let transition = false;
		let run: Running | undefined;

		// Brought up the floor or set up already by the time this is done - the
		// ball inbounded after a basket, say - and from when (the dead time
		// before the set, the picture runs through fast).
		let into: "flow" | "set" | undefined;
		const from = t;
		// What the trip started from: a defensive board, a steal, a basket. A
		// trip that ends in a shot is a break if the shot came quick enough
		// (see BREAK_GAP); one that ends in a turnover or a whistle comes just
		// as quick in the sim either way, so it breaks as often as the
		// league's do.
		const last = this.beats.at(-1)?.type ?? "";
		const quick = (start: keyof typeof BREAK_GAP) =>
			!heave &&
			(shot
				? gap !== undefined &&
					gap < Math.min(12, BREAK_GAP[start] + BREAK_FROM[shot])
				: this.rng() < BREAK_SHARE[start]);
		if (phase === "inboundBase" && !changed) {
			// After a basket: taken out under the basket and inbounded, then up
			// the floor - every man back on his own man. Now and then - right
			// after a field goal - in quick and pushed before the defense is back.
			this.guarding.clear();
			transition = resultOf(last)?.kind === "make" && quick("make");
			t = this.inboundAfterMake(team, t, transition);
			into = "flow";
		} else if (phase === "loose" || phase === "tip" || phase === "set") {
			if (this.holder === undefined || this.teamOf(this.holder) !== team) {
				// Loose ball (or it was ours to take): the nearest man picks it up.
				const b = this.ballPoint();
				const near = this.slots(team).sort(
					(a, c) => dist(this.posOf(a), b) - dist(this.posOf(c), b),
				)[0]!;
				t = this.pickUp(near, t, RUN) + 300;
			}
			const handler = this.posOf(this.holder ?? 0);
			transition =
				phase === "loose" &&
				this.inBackcourt(team, handler) &&
				quick(last === "stl" || last === "tov" ? "steal" : "board");
			if (phase === "loose") {
				this.getBack(other(team), t, transition);
			}
			// No break on: he turns and brings it up (fast, below).
		} else {
			// A dead ball: the inbound, from wherever it went dead - in the
			// frontcourt into a set or a play drawn up for it, in the backcourt
			// then up the floor.
			const at = this.inboundAt;
			this.guarding.clear();
			if (at && !this.inBackcourt(team, at)) {
				// Next to no clock for it - a shot right off the inbound (a
				// putback the sim has after a timeout, say): the ball goes in to
				// the man who shoots it, where he shoots it from. No set.
				const quickTo =
					shot !== undefined &&
					shooter !== undefined &&
					gap !== undefined &&
					gap < QUICK_IN
						? shooter
						: undefined;
				run = quickTo === undefined ? call?.("inbound") : undefined;
				if (run && (run.play.cat === "blob" || run.play.cat === "slob")) {
					t = this.inboundPlay(run, t);
				} else {
					t = this.inboundFrom(
						team,
						t,
						at,
						quickTo === undefined
							? undefined
							: { pid: quickTo, at: this.quickSpot(team, shot!, at) },
					);
					if (run) {
						t = this.flowToPlay(run, t);
					}
					this.quickIn = quickTo;
				}
				into = "set";
			} else {
				t = at
					? this.inboundFrom(team, t, at)
					: this.ballTo(this.slots(team)[0]!, t);
				into = "flow";
			}
		}

		const handlerPos = this.posOf(this.holder ?? this.slots(team)[0]!);
		// A trip the sim ends in a turnover or a whistle only seconds in -
		// lost on the way up, fouled at once (on purpose, late in a game) -
		// is over before any set: it happens where the ball is.
		const rushed =
			heave || (!shot && !transition && gap !== undefined && gap < RUSH_GAP);
		if (transition) {
			this.breaks.push(t);
			run = call?.("break");
			t = run
				? this.startBreak(run, t)
				: this.pushBreak(team, t, shot, shooter);
		} else if (rushed) {
			// Up the floor if it isn't, fast - and no set.
			if (!heave && this.inBackcourt(team, handlerPos)) {
				const up = t;
				t = this.bringUp(team, t);
				this.hurry(Math.max(from + 400, up - 200), t - 1000);
			}
		} else if (
			into === "flow" ||
			(into === undefined &&
				(this.inBackcourt(team, handlerPos) || this.motionTeam !== team))
		) {
			run = call?.("flow");
			const first = run && this.firstAction(team, run, gap);
			const up = t;
			t = first
				? this.flowToPlay(first.run, t)
				: run
					? this.flowToPlay(run, t)
					: this.bringUp(team, t);
			// Up the floor, fast - until a beat before the set.
			this.hurry(Math.max(from + 400, up - 200), t - 1000);
			if (first && run) {
				t = this.runSteps(first.run, t, 0, first.steps - 1);
				t = this.flowToPlay(run, t, false);
			}
		} else if (into === undefined && gap !== undefined && gap >= 7) {
			// Still in the half court (an offensive rebound): kick it out and
			// reset into a set - when there is time for one.
			run = call?.("flow");
			const first = run && this.firstAction(team, run, gap + 4);
			if (first && run) {
				t = this.flowToPlay(first.run, t);
				t = this.runSteps(first.run, t, 0, first.steps - 1);
				t = this.flowToPlay(run, t, false);
			} else if (run) {
				t = this.flowToPlay(run, t);
			}
		}

		if (!run) {
			const beats = possessionBeats(gap, transition);
			// A reversal only when the possession really ground on - the shot
			// itself usually comes off a pass or two (see stageShot).
			const swings =
				transition || beats < 2 ? 0 : gap !== undefined && gap >= 18 ? 1 : 0;
			for (let s = 0; s < swings; s++) {
				t = this.swing(team, t);
			}
		}
		this.phase = "set";
		this.inboundAt = undefined;
		return { t, run };
	}

	// Where the ball goes in to a man who shoots it straight off the catch:
	// in close for a shot at the rim, on the block for a post shot, out at
	// his spot for a jumper - on the side it comes in from.
	private quickSpot(team: Side, zone: Zone, at: Pt): Pt {
		const side = at.y < COURT_H / 2 ? -1 : 1;
		if (zone === "atRim" || zone === "tipIn" || zone === "putBack") {
			return spot(team, this.rand(5, 7), COURT_H / 2 + side * this.rand(3, 6));
		}
		if (zone === "lowPost") {
			return spot(team, this.rand(7, 9), COURT_H / 2 + side * this.rand(6, 8));
		}
		const P = this.shotSpot(team, zone);
		return (P.y - COURT_H / 2) * side >= 0 ? P : { x: P.x, y: COURT_H - P.y };
	}

	// ---- sets -----------------------------------------------------------------

	private five(team: Side): Cast[] {
		return this.slots(team).map((pid) => ({
			pid,
			rank: this.rank.get(pid) ?? 4,
		}));
	}

	// A spot from the playbook on this floor: measured from the rim the team
	// attacks, flipped side to side if the set is run the other way.
	private spotFor(team: Side, mirror: 1 | -1, name: string): Pt {
		const [x, y] = spotXY(name);
		return spot(
			team,
			RIM_INSET + y,
			COURT_H / 2 + attackDir(team) * mirror * x,
		);
	}

	private at(run: Running, name: string): Pt {
		const p = this.spotFor(
			run.team,
			run.across?.has(name) ? (-run.mirror as 1 | -1) : run.mirror,
			name,
		);
		if (name.startsWith("inbound") || name === "rim") {
			return p;
		}
		let j = run.jitter.get(name);
		if (!j) {
			j = { x: this.rand(-0.9, 0.9), y: this.rand(-0.9, 0.9) };
			run.jitter.set(name, j);
		}
		// A corner man keeps his feet behind the line.
		const q =
			name.endsWith("_corner") && !name.includes("short")
				? { x: p.x + j.x, y: p.y }
				: { x: p.x + j.x, y: p.y + j.y };
		const [cx, cy] = spotXY(name);
		const r = Math.hypot(cx, cy);
		const rim = { x: rimX(run.team), y: COURT_H / 2 };
		const out = dist(q, rim);
		if (r >= 22 && out < r) {
			// And a three stays a three.
			const u = unitVec(rim, q);
			return { x: rim.x + u.x * r, y: rim.y + u.y * r };
		}
		return q;
	}

	// "At the rim" means a step in front of it, on the side he comes from -
	// not under the net.
	private nearRim(team: Side, from: Pt, k = 3.2): Pt {
		const rim = { x: rimX(team), y: COURT_H / 2 };
		let u = unitVec(rim, from);
		// Never from behind the backboard.
		if (u.x * attackDir(team) > -0.15) {
			u = unitVec(rim, {
				x: rim.x - attackDir(team) * 3,
				y: from.y,
			});
		}
		return clampPt({ x: rim.x + u.x * k, y: rim.y + u.y * k });
	}

	// Where the five stand, and who has the ball, as the picture picks the
	// set up.
	private formation(run: Running): { at: string[]; ball: number } {
		const w = walkPlay(run.play, run.from);
		return { at: w.at, ball: w.holder };
	}

	// The side the set is run to that the five are already nearest.
	private nearestMirror(run: Running): 1 | -1 {
		const { at } = this.formation(run);
		const cost = (m: 1 | -1) =>
			run.roles.reduce(
				(sum, pid, r) =>
					sum + dist(this.posOf(pid), this.spotFor(run.team, m, at[r]!)),
				0,
			);
		return cost(1) <= cost(-1) ? 1 : -1;
	}

	private running<X>(
		team: Side,
		called: Called<X>,
		entry: Entry,
		end: { option?: PlayOption; risk?: PlayRisk },
		gap: number | undefined,
	): Running {
		const play = called.play;
		const upTo = end.option
			? end.option.after
			: end.risk
				? end.risk.step
				: play.steps.length - 1;
		// How much of it to show: the step that springs the shot, and the ones
		// before it the trip took long enough to have run (on top of a first
		// action - see firstAction). A break, early offense or an inbound play
		// is shown whole.
		const keep =
			entry === "break" ||
			play.cat === "early" ||
			play.cat === "blob" ||
			play.cat === "slob"
				? 99
				: gap === undefined || gap < 10
					? 1
					: gap < 18
						? 2
						: 3;
		let from = Math.max(0, Math.min(upTo, play.steps.length) - keep + 1);
		// A man going off a screen is seen going off it: the step that sets
		// it is never cut from the one he uses it in - else he just turns and
		// goes, off nothing anybody can see.
		if (from > 0 && from <= upTo) {
			const going = new Set(
				(play.steps[from] ?? [])
					.filter((a) => a.type === "dribble" || a.type === "move")
					.map((a) => a.who),
			);
			if (
				(play.steps[from - 1] ?? []).some(
					(a) => a.type === "screen" && going.has(a.for),
				)
			) {
				from -= 1;
			}
		}
		const run: Running = {
			play,
			team,
			roles: called.roles,
			mirror: called.mirror,
			jitter: new Map(),
			from,
			...(gap === undefined ? {} : { gap }),
			...end,
		};
		if (entry === "break" || entry === "flow") {
			run.mirror = this.nearestMirror(run);
		} else if (entry === "inbound" && this.inboundAt) {
			// The inbounder takes it out where the ball went out.
			const inbounder = run.play.start[run.play.ball]!;
			const at = this.inboundAt;
			run.mirror =
				dist(this.spotFor(team, 1, inbounder), at) <=
				dist(this.spotFor(team, -1, inbounder), at)
					? 1
					: -1;
		}
		return run;
	}

	// The sets there are to run from where the ball is. Over half court
	// with it already - an offensive board kicked out, an inbound in the
	// frontcourt, a steal picked up there - none that starts with it back
	// over (a two-for-one pushed up from the backcourt): once it is over, it
	// never goes back.
	private setsFor(team: Side, entry: Entry): Play[] {
		const h = this.holder;
		const over =
			entry === "inbound" ||
			(entry === "flow" &&
				h !== undefined &&
				!this.inBackcourt(team, this.posOf(h)));
		return over ? OVER_SETS : PLAYS;
	}

	// Which sets fit the moment: a break off a rebound or a steal, early
	// offense on a quick trip, something for the end of the clock, a play
	// drawn up for an inbound.
	private playCats(
		entry: Entry,
		gap: number | undefined,
		clock: number | undefined,
	): Partial<Record<PlayCategory, number>> {
		if (entry === "break") {
			return { break: 1 };
		}
		const cats: Partial<Record<PlayCategory, number>> = { half: 1 };
		if (
			(gap !== undefined && gap >= 19) ||
			(clock !== undefined && clock < 5)
		) {
			cats.late = 2.5;
		} else if (gap !== undefined && gap < 11) {
			// Pushed up and into something before the defense is set: a drag
			// screen, a step-up, a handoff on the way.
			cats.early = entry === "flow" ? 3 : 0.6;
		}
		if (entry === "inbound" && this.inboundAt) {
			const at = this.inboundAt;
			if (at.x < 0.5 || at.x > COURT_W - 0.5) {
				cats.blob = 3;
			} else {
				const depth = Math.abs(at.x - rimX(this.offense)) + RIM_INSET;
				if (Math.abs(depth - 28) < 12) {
					cats.slob = 1.5;
				}
			}
		}
		return cats;
	}

	private callForShot(
		entry: Entry,
		team: Side,
		shooter: number,
		zone: Zone,
		plan: ShotPlan,
		gap: number | undefined,
		clock: number | undefined,
	): Running | undefined {
		const pz: PlayZone | undefined =
			zone === "atRim"
				? "rim"
				: zone === "lowPost"
					? "post"
					: zone === "midRange"
						? "mid"
						: zone === "three"
							? "three"
							: undefined;
		if (!pz) {
			return undefined;
		}
		const five = this.five(team);
		const on = (p: number | undefined) =>
			p === undefined || five.some((c) => c.pid === p);
		const assist =
			plan.assist !== undefined &&
			plan.assist !== shooter &&
			this.teamOf(plan.assist) === team
				? plan.assist
				: undefined;
		const holder = entry === "break" ? this.breakHolder(team) : undefined;
		if (!on(shooter) || !on(assist) || !on(holder)) {
			return undefined;
		}
		const sets = this.setsFor(team, entry);
		const ask = (cats: Partial<Record<PlayCategory, number>>) =>
			callShot(
				this.rng,
				{
					cats,
					zone: pz,
					shooter,
					assist,
					unassisted: plan.kind === "make" && assist === undefined,
					holder,
					five,
				},
				sets,
			);
		const called =
			ask(this.playCats(entry, gap, clock)) ??
			(entry === "break" ? undefined : ask({ half: 1 }));
		if (!called) {
			return undefined;
		}
		const run = this.running(team, called, entry, { option: called.pick }, gap);
		run.help = [
			plan.kind === "block" ? plan.blocker : undefined,
			plan.kind === "foul" ? plan.fouler : undefined,
			plan.finish === "poster" ? plan.defender : undefined,
		].filter((p): p is number => p !== undefined && this.teamOf(p) !== team);
		return run;
	}

	private callForTurnover(
		entry: Entry,
		team: Side,
		victim: number,
		kinds: Partial<Record<TurnoverKind, number>>,
		gap: number | undefined,
		clock: number | undefined,
	): Running | undefined {
		const five = this.five(team);
		const holder = entry === "break" ? this.breakHolder(team) : undefined;
		if (
			!five.some((c) => c.pid === victim) ||
			(holder !== undefined && !five.some((c) => c.pid === holder))
		) {
			return undefined;
		}
		const sets = this.setsFor(team, entry);
		const ask = (cats: Partial<Record<PlayCategory, number>>) =>
			callTurnover(this.rng, { cats, victim, kinds, holder, five }, sets);
		const called =
			ask(this.playCats(entry, gap, clock)) ??
			(entry === "break" ? undefined : ask({ half: 1 }));
		return called
			? this.running(team, called, entry, { risk: called.pick }, gap)
			: undefined;
	}

	private callForAny(
		entry: Entry,
		team: Side,
		gap: number | undefined,
		clock: number | undefined,
	): Running | undefined {
		const five = this.five(team);
		const holder = entry === "break" ? this.breakHolder(team) : undefined;
		if (holder !== undefined && !five.some((c) => c.pid === holder)) {
			return undefined;
		}
		const called = callAny(
			this.rng,
			{ cats: this.playCats(entry, gap, clock), five, holder },
			this.setsFor(team, entry),
		);
		return called ? this.running(team, called, entry, {}, gap) : undefined;
	}

	// THE FIRST ACTION.
	//
	// The set that gets the shot is seldom the first thing a trip runs: given
	// the clock, the offense runs something first - a pick-and-roll the
	// defense takes away, a pin-down, a handoff - and flows out of it into
	// the next. A step of it on a trip of 12 seconds or more, two on one of
	// 16 or more.
	private firstAction(
		team: Side,
		run: Running,
		gap: number | undefined,
	): { run: Running; steps: number } | undefined {
		const n = gap === undefined || gap < 12 ? 0 : gap < 16 ? 1 : 2;
		if (
			n === 0 ||
			!run.option ||
			(run.play.cat !== "half" && run.play.cat !== "late")
		) {
			return undefined;
		}
		// Most often a ball screen up top, or a handoff.
		const called = callAny(this.rng, {
			cats: { half: 1 },
			five: this.five(team),
			favor: (p) =>
				p.steps[0]?.some(
					(a) =>
						a.type === "screen" && a.for === p.ball && BALL_SCREENS.has(a.kind),
				)
					? 3
					: p.steps[0]?.some((a) => a.type === "handoff")
						? 1.5
						: 1,
		});
		if (!called || called.play.id === run.play.id) {
			return undefined;
		}
		// A screen it sets is used: the man it is for comes off it before they
		// flow into the next set - not a screen set for the man with the ball
		// and him stood there with it, everybody then off to somewhere else.
		const { steps } = called.play;
		let k = Math.min(n, steps.length);
		const last = steps[k - 1] ?? [];
		const after = steps[k] ?? [];
		if (
			last.some(
				(a) =>
					a.type === "screen" &&
					after.some(
						(b) =>
							(b.type === "dribble" || b.type === "move") && b.who === a.for,
					),
			)
		) {
			k += 1;
		}
		return {
			run: {
				play: called.play,
				team,
				roles: called.roles,
				mirror: called.mirror,
				jitter: new Map(),
				from: 0,
				...(gap === undefined ? {} : { gap }),
			},
			steps: k,
		};
	}

	// SIZING HIM UP.
	//
	// Brought up the floor with time on the clock, the man with the ball does
	// not run the set the instant everybody is in it: sometimes he calls it
	// from the top, a hand up; sometimes he works his man first - a
	// crossover, one between his legs - and then goes.
	private sizeUp(run: Running, bh: number, t: number): number {
		if (this.holder !== bh || (run.gap !== undefined && run.gap < 9)) {
			return t;
		}
		// Whoever is still back from bringing it up - the inbounder trailing
		// the play - comes on up over half court while he does.
		for (const pid of run.roles) {
			const P = this.posOf(pid);
			if (pid !== bh && this.inBackcourt(run.team, P)) {
				this.go(
					pid,
					clampPt({
						x: COURT_W / 2 + attackDir(run.team) * 6,
						y: P.y + (COURT_H / 2 - P.y) * 0.3,
					}),
					Math.max(t, this.free.get(pid) ?? 0),
					RUN * 0.85,
					"run",
				);
			}
		}
		const r = this.rng();
		if (r < 0.12) {
			return t;
		}
		if (r < 0.27) {
			const dur = this.rand(850, 1150);
			this.act(bh, "callPlay", t, t + dur);
			return t + dur;
		}
		if (r < 0.6 && (run.gap === undefined || run.gap >= 11)) {
			// The ball out to a wing and back - a look at what the defense
			// gives - before he goes.
			const rim = { x: rimX(run.team), y: COURT_H / 2 };
			const B = this.posOf(bh);
			const mate = run.roles
				.filter((p) => {
					const P = this.posOf(p);
					const d = dist(P, B);
					return p !== bh && dist(P, rim) >= 17 && d >= 10 && d <= 32;
				})
				.sort((a, b) => dist(this.posOf(a), B) - dist(this.posOf(b), B))[0];
			if (mate !== undefined) {
				const out = this.passTo(bh, mate, t);
				this.hold(mate, out, "hold");
				this.guardStep(run, [], out, out + 500);
				// Now and then on around the arc first, a man further on.
				const M = this.posOf(mate);
				const on =
					run.gap === undefined || run.gap < 14 || this.rng() < 0.6
						? undefined
						: run.roles
								.filter((p) => {
									const P = this.posOf(p);
									return (
										p !== bh &&
										p !== mate &&
										dist(P, rim) >= 17 &&
										dist(P, M) >= 10 &&
										dist(P, M) <= 32 &&
										dist(P, B) >= 10
									);
								})
								.sort(
									(a, b) => dist(this.posOf(a), M) - dist(this.posOf(b), M),
								)[0];
				let last = mate;
				let at = out;
				if (on !== undefined) {
					at = this.passTo(mate, on, out + this.rand(250, 550));
					this.hold(on, at, "hold");
					this.guardStep(run, [], at, at + 500);
					last = on;
				}
				const back = this.passTo(last, bh, at + this.rand(350, 750));
				this.hold(bh, back, "dribble");
				this.guardStep(run, [], back, back + 500);
				return back + 150;
			}
		}
		// On the next beat of his dribble, a move or a few of them.
		const top = this.dribbleTop(bh, t) ?? t;
		const k = this.rng();
		const back = this.moveRun(
			bh,
			top < t - 1 ? top + DRIBBLE_MS : top,
			k < 0.4 ? 1 : k < 0.8 ? 2 : 3,
			0.3,
			0.2,
		);
		this.hold(bh, back, "dribble");
		return back + 120;
	}

	// An inbound play: everybody to his spot in it as drawn up, the inbounder
	// out of bounds with the ball, the defense picking them up - the walk
	// there run through fast.
	private inboundPlay(run: Running, t: number): number {
		const { team, play } = run;
		const dir = attackDir(team);
		const bh = run.roles[play.ball]!;
		const ball = this.at(run, play.start[play.ball]!);
		this.pickUpLoose(t);
		const ready = this.walkTo(
			[
				...run.roles.map((pid, r) => ({
					pid,
					at: this.at(run, play.start[r]!),
					face: dir,
				})),
				...run.roles.flatMap((pid, r) => {
					const d = this.defenderOf(pid);
					return d === undefined
						? []
						: [
								{
									pid: d,
									at: this.defensePoint(
										team,
										this.at(run, play.start[r]!),
										ball,
										pid === bh,
									),
									face: -dir as 1 | -1,
									man: pid,
								},
							];
				}),
			],
			t,
		);
		const has = this.ballTo(
			bh,
			Math.max(t + 300, (this.free.get(bh) ?? t) - 1200),
		);
		const set = Math.max(ready, has);
		this.hold(bh, Math.max(has, this.free.get(bh) ?? 0), "hold");
		this.lookAt(bh, set + 1, { x: rimX(team), y: COURT_H / 2 });
		this.hurry(t + 300, set - 200);
		this.motionTeam = team;
		this.motion = 0;
		return set + 650;
	}

	// No cut: from wherever they are into the set's spots, the ball brought
	// up or kicked out to whoever starts with it.
	private flowToPlay(run: Running, t: number, sized = true): number {
		this.castNearest(run);
		const { team } = run;
		const f = this.formation(run);
		const dir = attackDir(team);
		const bh = run.roles[f.ball]!;
		const had = this.holder;
		let ready = t + 600;
		// Where each goes - never two of them to the one spot: the man with
		// the ball has his, and anybody the set puts on top of somebody
		// already placed stands a few steps off him instead.
		const spotOf = run.roles.map((_, r) => this.at(run, f.at[r]!));
		{
			const taken: Pt[] = [];
			const order = run.roles
				.map((_, r) => r)
				.sort((a, b) => (a === f.ball ? -1 : b === f.ball ? 1 : a - b));
			for (const r of order) {
				let S = spotOf[r]!;
				for (const q of taken) {
					const d = dist(S, q);
					if (d < 4) {
						const u =
							d > 0.1
								? unitVec(q, S)
								: unitVec({ x: 0, y: 0 }, { x: -dir, y: q.y < 25 ? 1 : -1 });
						S = clampPt({ x: q.x + u.x * 6, y: q.y + u.y * 6 });
					}
				}
				taken.push(S);
				spotOf[r] = S;
			}
		}
		{
			// The man the set starts with the ball isn't stood right under the
			// rim with it, waiting on the rest: out on the block, the side he
			// comes from.
			const rim = { x: rimX(team), y: COURT_H / 2 };
			const B = spotOf[f.ball]!;
			if (dist(B, rim) < 6) {
				const P = this.posOf(bh);
				const u =
					dist(P, rim) > 3
						? unitVec(rim, P)
						: unitVec(rim, { x: rim.x - dir * 6, y: B.y >= rim.y ? 32 : 18 });
				spotOf[f.ball] = clampPt({ x: rim.x + u.x * 9, y: rim.y + u.y * 9 });
			}
		}
		// A long way back with it - the inbound caught in the backcourt, a
		// board at the other end - it is brought up and the entry made from up
		// the floor, not thrown the length of it to a man still on his way
		// there. A big who has it gets it to a guard first (see outlet), and
		// the guard brings it up - straight into the set, if it starts in his
		// hands.
		let up: number | undefined;
		if (
			had !== undefined &&
			had !== bh &&
			dist(this.posOf(had), spotOf[f.ball]!) > ENTRY_FAR
		) {
			const g = this.handlerOf(team);
			up = this.handles(had) || !run.roles.includes(g) ? had : g;
		}
		// The length of the floor to go, the rest run it in their lanes.
		const lanes: { pid: number; to: Pt; j: number }[] = [];
		run.roles.forEach((pid, r) => {
			if ((pid === had && had !== bh) || (pid === up && up !== had)) {
				return;
			}
			const S = spotOf[r]!;
			const P = this.posOf(pid);
			const far = dist(P, S) > 20;
			if (pid === had) {
				this.hold(pid, Math.max(t, this.free.get(pid) ?? 0), "dribble");
				ready = Math.max(
					ready,
					this.go(pid, S, t, DRIBBLE * (far ? 0.85 : 0.6), "dribble", dir),
				);
			} else if ((S.x - P.x) * dir >= LANE_RUN) {
				lanes.push({ pid, to: S, j: r });
			} else {
				ready = Math.max(
					ready,
					this.go(pid, S, t + r * 40, far ? RUN * 0.85 : JOG, "run"),
				);
			}
		});
		if (lanes.length > 0) {
			const by =
				t +
				Math.max(
					...lanes.map((m) =>
						runMs(dist(this.posOf(m.pid), m.to) * 1.08, RUN * 0.85),
					),
				);
			const ballMan = up ?? had ?? bh;
			this.fillLanes(
				team,
				lanes,
				t,
				by,
				(this.posOf(ballMan).y + spotOf[f.ball]!.y) / 2,
			);
			ready = Math.max(ready, by);
		}
		if (had !== undefined && had !== bh) {
			// (Where the man coming for it was: if the set has no place of
			// his own for the man with it, or puts him where it is coming to,
			// that is where he goes once he gives it up.)
			const came = this.posAt(bh, t);
			const B = spotOf[f.ball]!;
			let from = had;
			let at = t + 150;
			if (up !== undefined && up !== had) {
				at = this.outlet(had, up, t, RUN, 9);
				from = up;
				ready = Math.max(ready, at);
			}
			if (up !== undefined) {
				// Up the floor on the dribble: into the set if it is his, else
				// to a pass away from the man it starts with - never from back
				// over the hash.
				let E = B;
				if (from !== bh) {
					const u = unitVec(B, this.posOf(from));
					const rx = rimX(team);
					E = clampPt({
						x: B.x + u.x * ENTRY_PASS,
						y: B.y + u.y * ENTRY_PASS,
					});
					if (Math.abs(E.x - rx) > ENTRY_DEEP) {
						E.x = rx - dir * ENTRY_DEEP;
					}
				}
				this.hold(from, Math.max(at, this.free.get(from) ?? 0), "dribble");
				ready = Math.max(
					ready,
					this.go(from, E, at, DRIBBLE * 0.85, "dribble", dir),
				);
			}
			if (from !== bh) {
				ready = Math.max(ready, this.passTo(from, bh, at));
				if (from !== had) {
					const G = spotOf[run.roles.indexOf(from)];
					if (G) {
						ready = Math.max(ready, this.go(from, G, at, JOG, "run"));
					}
				}
			}
			const r = run.roles.indexOf(had);
			const mine = r >= 0 ? spotOf[r] : undefined;
			const D = mine && dist(mine, spotOf[f.ball]!) >= 4 ? mine : came;
			ready = Math.max(ready, this.go(had, D, t, JOG, "run"));
		}
		this.guardStep(run, [], t + 150, ready);
		this.motionTeam = team;
		this.motion = 0;
		return sized ? this.sizeUp(run, bh, ready + 100) : ready + 100;
	}

	// Who plays which part in a set is first who fits it - but of the men who
	// could, the one already nearest it plays it, and the set is run to the
	// side they are already on: two spot-up men don't trade corners, running
	// the length of the baseline past each other, to get it started. (The
	// man it ends with - the shooter, his passer, the man who loses it -
	// keeps his part.)
	private castNearest(run: Running, holder?: number) {
		const five = this.five(run.team);
		if (
			five.length !== 5 ||
			!run.roles.every((pid) => five.some((c) => c.pid === pid))
		) {
			return;
		}
		const pinned = new Map<number, Role>();
		for (const r of [run.option?.shooter, run.option?.assist, run.risk?.who]) {
			if (r !== undefined) {
				pinned.set(run.roles[r]!, r);
			}
		}
		// (On the break, the man pushing it keeps it.)
		if (holder !== undefined && !pinned.has(holder)) {
			if ([...pinned.values()].includes(run.play.ball)) {
				return;
			}
			pinned.set(holder, run.play.ball);
		}
		const { at, ball } = this.formation(run);
		let best: { roles: number[]; mirror: 1 | -1; cost: number } | undefined;
		// A man with it back down the floor who is no ball handler - a big off
		// the defensive glass - gives it up to one, not brings it up himself as
		// the man the set starts with it (unless the set has him there).
		const had = this.holder;
		const outlet =
			had !== undefined &&
			this.teamOf(had) === run.team &&
			!this.handles(had) &&
			this.inBackcourt(run.team, this.posOf(had))
				? had
				: undefined;
		const travel = (spots: Pt[]) => (pid: number, r: number) => {
			// (Across the floor counts double: up it, everybody goes.)
			const P = this.posOf(pid);
			const S = spots[r]!;
			return (
				(dist(P, S) + Math.abs(P.y - S.y)) * TRAVEL_COST +
				(pid === outlet && r === ball ? OUTLET_COST : 0)
			);
		};
		for (const mirror of [run.mirror, -run.mirror as 1 | -1]) {
			const spots = at.map((name) => this.spotFor(run.team, mirror, name));
			const cast = castPlay(run.play, five, pinned, travel(spots));
			if (cast && (!best || cast.cost < best.cost)) {
				best = { roles: cast.roles, mirror, cost: cast.cost };
			}
		}
		// Two of them running across each other's path a long way - X-ing up
		// the floor to the other's side of it - trade parts, if the set is
		// little the worse for it.
		if (best) {
			const spots = at.map((name) =>
				this.spotFor(run.team, best!.mirror, name),
			);
			for (let i = 0; i < 5; i++) {
				for (let j = i + 1; j < 5; j++) {
					const a = best.roles[i]!;
					const b = best.roles[j]!;
					const A = this.posOf(a);
					const B = this.posOf(b);
					if (
						pinned.has(a) ||
						pinned.has(b) ||
						dist(A, spots[i]!) < CROSS_RUN ||
						dist(B, spots[j]!) < CROSS_RUN ||
						!crossing(A, spots[i]!, B, spots[j]!) ||
						crossing(A, spots[j]!, B, spots[i]!)
					) {
						continue;
					}
					const all = new Map<number, Role>(
						best.roles.map((pid, r) => [pid, r as Role]),
					);
					all.set(a, j as Role);
					all.set(b, i as Role);
					const traded = castPlay(run.play, five, all, travel(spots));
					// (Two parts that need the same - two shooters - just trade.)
					const same =
						(run.play.needs[i] ?? []).join() ===
						(run.play.needs[j] ?? []).join();
					if (traded && (same || traded.cost - best.cost < CROSS_TRADE)) {
						best = { ...best, roles: traded.roles, cost: traded.cost };
					}
				}
			}
		}
		if (best) {
			run.roles = best.roles;
			run.mirror = best.mirror;
		}
	}

	// A break runs from wherever they are when the ball is won; whoever the
	// set does not send somewhere right away runs the floor to his lane.
	private startBreak(run: Running, t: number): number {
		this.castNearest(run, run.roles[run.play.ball]);
		const bh = run.roles[run.play.ball]!;
		// (The rest are off up the floor from now - not stood round the man
		// who has it till he has got it out.)
		const t0 = t;
		if (this.holder !== undefined && this.holder !== bh) {
			t = this.outlet(this.holder, bh, t, SPRINT, 10);
		}
		this.hold(bh, t, "dribble");
		const f = this.formation(run);
		const going = new Set<number>();
		for (const a of run.play.steps[run.from] ?? []) {
			if (a.type === "screen") {
				for (const w of a.who) {
					going.add(w);
				}
			} else if (a.type !== "pass" && a.type !== "handoff") {
				going.add(a.who);
			}
		}
		// (Never back down the floor to it, from farther up already: he goes on
		// from where he is - see trailUp.)
		const out = (p: Pt) => Math.abs(p.x - rimX(run.team));
		const dir = attackDir(run.team);
		// The length of the floor to go, in their lanes - not side by side
		// straight up it from wherever the board left them.
		const lanes: {
			pid: number;
			to: Pt;
			j: number;
			by: number;
			upTo?: number;
		}[] = [];
		const lane = (pid: number, to: Pt, j: number, upTo?: number) => {
			const end = upTo === undefined ? to : { x: upTo, y: to.y };
			lanes.push({
				pid,
				to,
				j,
				by: t0 + runMs(dist(this.posOf(pid), end) * 1.08, RUN),
				...(upTo !== undefined ? { upTo } : {}),
			});
		};
		run.roles.forEach((pid, r) => {
			if (pid === bh) {
				return;
			}
			const S = this.at(run, f.at[r]!);
			const P = this.posOf(pid);
			if (going.has(r)) {
				// His part comes in the first step; back in the backcourt, he
				// is on his way up for it meanwhile - on his side of the floor.
				if (this.inBackcourt(run.team, P)) {
					lane(pid, S, r, COURT_W / 2 + dir * 4);
				}
				return;
			}
			if (!(this.inBackcourt(run.team, S) && out(P) < out(S) - 6)) {
				if ((S.x - P.x) * dir >= LANE_RUN) {
					lane(pid, S, r);
				} else {
					this.go(pid, S, t0, RUN, "run");
				}
			}
		});
		if (lanes.length > 0) {
			// (Clear of the ball's way up the floor: from him to where the
			// set's first step dribbles it, if it does.)
			const first = (run.play.steps[run.from] ?? []).find(
				(a) =>
					a.type === "dribble" && a.who === run.play.ball && a.to !== "rim",
			);
			const B = this.posOf(bh);
			const ballY =
				first?.type === "dribble"
					? (B.y +
							this.spotFor(
								run.team,
								run.across?.has(first.to)
									? (-run.mirror as 1 | -1)
									: run.mirror,
								first.to,
							).y) /
						2
					: B.y;
			this.fillLanes(run.team, lanes, t0, t0, ballY);
		}
		// Back on defense, each picks up whoever is coming up the floor his
		// side of it - not the man he would have had, across the floor and
		// back through his teammates in the paint to get to him. A break is
		// matched up on the fly, a guard on a big if that is who is there.
		const opening = (run.play.steps[run.from] ?? []).find(
			(a) => a.type === "dribble" && a.who === run.play.ball && a.to !== "rim",
		);
		this.matchUp(
			run.team,
			this.slots(run.team).map((pid) =>
				pid === bh && opening?.type === "dribble"
					? this.spotFor(
							run.team,
							run.across?.has(opening.to)
								? (-run.mirror as 1 | -1)
								: run.mirror,
							opening.to,
						)
					: this.posOf(pid),
			),
			BREAK_MATCH_RANK,
		);
		this.motionTeam = run.team;
		this.motion = 0;
		return t;
	}

	// Steps `from` to `to` of the set, in order. Returns when the last is done.
	private runSteps(run: Running, t: number, from: number, to: number): number {
		const steps = run.play.steps;
		for (let k = from; k <= to && k < steps.length; k++) {
			t = this.runStep(run, steps[k]!, t, steps[k + 1], k);
		}
		return t;
	}

	// One step of a set: its actions in their order, each when its man is
	// free to do it (a pass waits for the catch before it), then the defense.
	private runStep(
		run: Running,
		acts: PlayAction[],
		t0: number,
		next: PlayAction[] | undefined,
		k: number,
	): number {
		const { team } = run;
		const rim = { x: rimX(team), y: COURT_H / 2 };
		let end = t0 + STEP_MIN;
		const planted: { pid: number; t: number; anim: AnimName; look: Pt }[] = [];
		const before = run.screen;
		const prev = Math.min(t0, run.stepAt ?? t0);
		run.stepAt = t0;
		// Down in the paint with it, he doesn't stand there bouncing it while
		// the others finish what the last step had them doing - the roller
		// still on his way in, say: he goes on from there as he gets there.
		const deep = (pid: number) =>
			this.holder === pid && dist(this.posOf(pid), rim) < 15
				? Math.min(t0, Math.max(prev, this.hasItFrom(pid)))
				: t0;
		for (const a of acts) {
			if (a.type === "screen") {
				const user = run.roles[a.for]!;
				const onBall = BALL_SCREENS.has(a.kind) && this.holder === user;
				const screeners = a.who.map((r) => run.roles[r]!);
				const U = this.posOf(user);
				let first: Pt | undefined;
				let set = Infinity;
				screeners.forEach((s, j) => {
					const marked = this.at(run, a.at[j] ?? a.at[0]!);
					let S: Pt;
					if (onBall) {
						// On the handler's man: up against his shoulder - not on top
						// of him - on the side the handler turns the corner.
						const dribble = next?.find(
							(x) => x.type === "dribble" && x.who === a.for,
						);
						const D =
							dribble && dribble.type === "dribble"
								? this.at(run, dribble.to)
								: rim;
						const ur = unitVec(U, rim);
						const cross = ur.x * (D.y - U.y) - ur.y * (D.x - U.x);
						const side = (cross >= 0 ? 1 : -1) * (j === 0 ? 1 : -1);
						S = {
							x: U.x + ur.x * 2.8 - ur.y * side * 2.0,
							y: U.y + ur.y * 2.8 + ur.x * side * 2.0,
						};
					} else {
						// Off the ball: planted at its spot, a step toward the man
						// he frees.
						const u = unitVec(marked, U);
						const kk = Math.min(2, dist(U, marked) * 0.4);
						S = { x: marked.x + u.x * kk, y: marked.y + u.y * kk };
					}
					S = clampPt(S);
					first ??= S;
					const off = Math.max(t0, this.free.get(s) ?? 0);
					const far = dist(this.posOf(s), S);
					const there = this.go(s, S, off, 16, "run");
					// His man calls it out: where it is coming.
					const sd = this.defenderOf(s);
					if (sd !== undefined && there - off > 500) {
						this.gesture(
							sd,
							"point",
							off + 150,
							Math.min(there + 150, off + 1100),
							S,
							0.65,
						);
					}
					// The man with the ball waves him up.
					if (onBall && far > 10) {
						this.gesture(user, "wave", off, off + 900, s, 0.5);
					}
					planted.push({ pid: s, t: there, anim: "screen", look: U });
					set = Math.min(set, there);
					end = Math.max(end, there + 150);
				});
				run.screen = {
					screeners,
					user,
					ball: onBall,
					at: first ?? rim,
					step: k,
					coverage: this.coverageFor(run, screeners[0]!, user, onBall, next),
					set: Number.isFinite(set) ? set : t0,
				};
			} else if (a.type === "dribble") {
				// Bringing it up, he goes as soon as he has it - not once the
				// others have got where the last step sent them.
				const pid = run.roles[a.who]!;
				const handed = this.handedOn(run, pid);
				const since =
					a.kind === "advance" && this.holder === pid
						? Math.min(t0, this.hasItFrom(pid))
						: handed !== undefined
							? Math.min(t0, handed)
							: deep(pid);
				const came = this.playDribble(
					run,
					a.who,
					a.to,
					a.kind,
					since,
					handed !== undefined,
				);
				end = Math.max(end, came);
				// Off up the floor with it before the step - his man goes with
				// him from then: not still on his way to where the last step had
				// the ball, back up the floor, the ball coming on past him.
				if (a.kind === "advance" && since < t0 - 50 && this.holder === pid) {
					const d = this.defenderOf(pid);
					if (d !== undefined && (this.free.get(d) ?? 0) > since) {
						this.cutShort(d, since);
						const P = this.posOf(pid);
						this.shadow(
							d,
							this.defensePoint(
								team,
								P,
								P,
								true,
								run.play.cat === "break" ? BREAK_PICK_UP : PICK_UP,
							),
							since,
							came,
							team,
							pid,
						);
					}
				}
			} else if (a.type === "move") {
				const pid = run.roles[a.who]!;
				// On the break, a man running the floor goes on as soon as he
				// has done what he was doing - the outlet thrown, his lane
				// filled - not once everybody has.
				const since =
					run.play.cat === "break" && this.holder !== pid
						? Math.max(prev, Math.min(t0, this.free.get(pid) ?? 0))
						: t0;
				// Off a screen set for him in this same step, he comes by it once
				// it is set - not past where it will be before the screener
				// has got there - and round it, not round the last one.
				const sc = run.screen;
				const off =
					sc && sc.step === k && sc.user === pid && !sc.ball ? sc : undefined;
				const by = off
					? Math.max(
							since,
							off.set +
								SCREEN_BY -
								runMs(dist(this.posOf(pid), off.at), MOVE_SPEED[a.style] ?? 13),
						)
					: since;
				const handed = this.handedOn(run, pid);
				const done =
					this.holder === pid
						? this.playDribble(
								run,
								a.who,
								a.to,
								"attack",
								handed !== undefined ? Math.min(t0, handed) : deep(pid),
								handed !== undefined,
							)
						: this.playMove(
								run,
								pid,
								a.to,
								a.style,
								by,
								off ?? before,
								off !== undefined,
							);
				// On the break nobody waits for the man trailing the play - not
				// unless it comes to him: jogging up behind it, or on his way to
				// a spot back up the floor from the ball (the big still coming
				// up from the far end).
				// (Only running on again after it - not the ball coming to him,
				// a screen of his - the next step goes on from wherever he is.)
				const behind = (): boolean => {
					const h = this.holder;
					if (h === undefined || h === pid) {
						return false;
					}
					const B = this.posAt(h, done);
					return dist(this.posOf(pid), rim) > dist(B, rim) + 4;
				};
				const forBall =
					run.option?.shooter === a.who ||
					run.option?.assist === a.who ||
					(next ?? []).some((b) =>
						b.type === "screen"
							? b.who.includes(a.who) || b.for === a.who
							: (b.type === "pass" || b.type === "handoff") &&
								(b.to === a.who || b.who === a.who),
					);
				if (
					!(
						run.play.cat === "break" &&
						(a.style === "jog" || behind()) &&
						!forBall
					)
				) {
					end = Math.max(end, done);
				}
			} else if (a.type === "pass") {
				const from = this.holder ?? run.roles[a.who]!;
				const to = run.roles[a.to]!;
				if (from !== to) {
					if (run.play.cat === "break" && from === this.holder) {
						this.pushAhead(team, from, to, t0);
					}
					end = Math.max(
						end,
						this.passTo(from, to, t0, passStyleOf(a.kind, this.rng)),
					);
				}
			} else if (a.type === "handoff") {
				end = Math.max(
					end,
					this.playHandoff(
						run,
						run.roles[a.who]!,
						run.roles[a.to]!,
						a.get,
						t0,
						k,
						planted,
						next,
						[...acts.slice(acts.indexOf(a) + 1), ...(next ?? [])],
					),
				);
			} else {
				end = Math.max(
					end,
					this.playPost(run, run.roles[a.who]!, a.at, a.move, t0, planted),
				);
			}
		}
		for (const p of planted) {
			// Planted until the step is over, or until he moves on in it (as he
			// gets there, even: a handoff and away).
			const later = this.track(p.pid)?.moves.find((m) => m.t0 >= p.t - 1);
			const until = Math.min(Math.max(p.t + 450, end), later?.t0 ?? Infinity);
			if (until > p.t + 100) {
				this.act(p.pid, p.anim, p.t, until, { look: p.look });
			}
		}
		this.clearTheRim(run, next);
		this.trailUp(run, acts, next, t0);
		this.spaceTheDrive(run, t0);
		this.guardStep(run, acts, t0, end, before, k);
		return end;
	}

	// CLEARING THE RIM. A man who cut or rolled to the rim and was not hit
	// doesn't park under it for the rest of the set, the paint clogged and a
	// defender stood on him: a beat after he gets there, he clears out to the
	// dunker spot on his side - there for a dump-off, out of the way of the
	// next drive. (Not if the set still has a use for him there.)
	private clearTheRim(run: Running, next: PlayAction[] | undefined) {
		const { team } = run;
		const rim = { x: rimX(team), y: COURT_H / 2 };
		const out = -attackDir(team);
		run.roles.forEach((pid, role) => {
			if (pid === this.holder || needed(run, next, role as Role)) {
				return;
			}
			const P = this.posOf(pid);
			if (dist(P, rim) > 6.5 || Math.abs(P.y - rim.y) > 5) {
				return;
			}
			const tr = this.track(pid);
			const there = Math.max(
				this.free.get(pid) ?? 0,
				...(tr?.acts ?? []).map((a) => a.t1),
			);
			const side = P.y >= rim.y ? 1 : -1;
			const spot = (k: number) =>
				clampPt({ x: rim.x - out * 2.5, y: rim.y + k * 8.5 });
			// (His own side, unless a teammate is there already.)
			const Q = [spot(side), spot(-side)].find((q) =>
				run.roles.every((o) => o === pid || dist(this.posOf(o), q) > 5),
			);
			if (Q) {
				this.go(pid, Q, there + 450, 10, "jog");
			}
		});
	}

	// THE PITCH AHEAD. On the break, the man with it does not stand in the
	// backcourt waiting for the man he is throwing to to get out ahead: he
	// pushes it up the floor until it is time to let it go - never past
	// him, nor far over half court.
	private pushAhead(team: Side, from: number, to: number, t0: number) {
		const P = this.posOf(from);
		if (!this.inBackcourt(team, P)) {
			return;
		}
		const R = this.posOf(to);
		const dir = attackDir(team);
		const half = COURT_W / 2;
		const Q = clampPt({
			x:
				dir === 1 ? Math.min(half + 4, R.x - 10) : Math.max(half - 4, R.x + 10),
			y: P.y + (R.y - P.y) * 0.25,
		});
		if ((Q.x - P.x) * dir < 4) {
			return;
		}
		// There as it is time to let it go, for it to get to him as he gets
		// where he is going.
		const start = Math.max(t0, this.hasItFrom(from));
		const letGo =
			(this.free.get(to) ?? 0) - passMs(dist(Q, R)) - RELEASE_MS - 80;
		if (letGo - start < 700) {
			return;
		}
		this.hold(from, start, "dribble");
		this.goBy(from, Q, start, letGo, "dribble", dir);
	}

	// THE TRAILER. A man the set leaves back up the floor - the big who took
	// it out after a basket, the one who stayed home on the break, the man
	// who pitched it ahead - with no part in this step or the next does not
	// stand and watch from back there (in the backcourt, or out at half
	// court with the ball well in front of him): he runs up to the top of
	// the play, to the side of it with the most room.
	private trailUp(
		run: Running,
		acts: PlayAction[],
		next: PlayAction[] | undefined,
		t0: number,
		// (Called where it never was before: his own draws for all of them.)
		own = false,
	) {
		const { team } = run;
		const busy = new Set<number>();
		// (Only sent back to a spot over half court - the safety a break
		// leaves at home - he has no part here: see playMove.)
		const home = new Set<number>();
		// (Nor is a run to a spot in the next step: that waits on the rest of
		// this one - the wings still sprinting the floor, a cut - and he is
		// not stood out there till then, but on his way up, and goes on from
		// there to it. His own draws.)
		const later = new Set<number>();
		for (const a of next ?? []) {
			if (a.type === "move") {
				later.add(a.who);
			}
		}
		for (const a of [
			...acts,
			...(next ?? []).filter((a) => a.type !== "move"),
		]) {
			if (a.type === "screen") {
				a.who.forEach((w) => busy.add(w));
				busy.add(a.for);
			} else if (
				a.type === "move" &&
				spotXY(a.to)[1] > COURT_W / 2 - RIM_INSET
			) {
				home.add(a.who);
			} else {
				busy.add(a.who);
				if (a.type === "pass" || a.type === "handoff") {
					busy.add(a.to);
				}
			}
		}
		const rim = { x: rimX(team), y: COURT_H / 2 };
		const h = this.holder;
		const B =
			h !== undefined && this.teamOf(h) === team ? this.posOf(h) : undefined;
		run.roles.forEach((pid, r) => {
			const P = this.posOf(pid);
			if (busy.has(r) || pid === this.holder) {
				return;
			}
			const back = this.inBackcourt(team, P);
			if (
				!back &&
				!(
					B &&
					!this.inBackcourt(team, B) &&
					dist(P, rim) >= TRAIL_DEEP &&
					dist(B, rim) <= dist(P, rim) - 8
				)
			) {
				return;
			}
			// (Out at half court, his own draws: the game's luck as it was.)
			const { Q, start } = this.trailSpot(
				run,
				pid,
				t0,
				back && !own && !home.has(r) && !later.has(r)
					? undefined
					: this.ownDraws(pid, t0),
			);
			this.go(pid, Q, start, RUN * 0.85, "run");
		});
	}

	// Where a trailer runs up to (see trailUp), and when he sets off - drawn
	// from the game's luck unless `own` draws are given.
	private trailSpot(
		run: Running,
		pid: number,
		t0: number,
		own: (() => number) | undefined,
	): { Q: Pt; start: number } {
		const { team } = run;
		const rand = (lo: number, hi: number) =>
			own ? lo + own() * (hi - lo) : this.rand(lo, hi);
		const P = this.posOf(pid);
		const h = this.holder;
		const mates = run.roles.filter((o) => o !== pid).map((o) => this.posOf(o));
		// (A step or two behind the arc, there to shoot it or swing it: the
		// top of the key, or a slot either side of it - not stood out past it
		// at 30 feet and more, out of the play.)
		const out = rand(TRAIL_OUT[0], TRAIL_OUT[1]);
		const spots = [COURT_H / 2 - 12, COURT_H / 2, COURT_H / 2 + 12].map(
			(y, i) =>
				spot(
					team,
					RIM_INSET + (i === 1 ? out : out - TRAIL_SLOT_IN),
					y + rand(-1.5, 1.5),
				),
		);
		const start = Math.max(t0 + rand(150, 450), this.free.get(pid) ?? 0);
		// The room he has there from the rest - and, all the way there,
		// from the man bringing the ball up: not up the floor at his side.
		const room = (S: Pt) => {
			let r = Math.min(...mates.map((M) => dist(M, S)));
			if (h !== undefined && this.teamOf(h) === team) {
				const ms = runMs(dist(P, S), RUN * 0.85);
				for (let k = 1; k <= 6; k++) {
					const u = k / 6;
					const gap = dist(
						{ x: P.x + (S.x - P.x) * u, y: P.y + (S.y - P.y) * u },
						this.posAt(h, start + ms * u),
					);
					if (gap < TRAIL_CLEAR) {
						r = Math.min(r, gap);
					}
				}
			}
			return r;
		};
		const Q = spots
			.map((S) => ({ S, room: room(S) }))
			.sort((a, b) => b.room - a.room)[0]!.S;
		return { Q: clampPt(Q), start };
	}

	// SPACING THE DRIVE. The ball going hard at the rim, nobody off it stays
	// rooted where he was: the men on the arc on its side slide along it,
	// away from where the help comes from, into the driver's line of sight -
	// one drifting down to the corner, one lifting up out of it - and a big
	// in the way drops off to the dunker spot on the far side, under the
	// help, for the dump-off. Nobody inside the arc stays stood where it is
	// going, or in its way there: he clears out the far side.
	private spaceTheDrive(run: Running, t0: number) {
		const { team } = run;
		const holder = this.holder;
		if (holder === undefined) {
			return;
		}
		const rim = { x: rimX(team), y: COURT_H / 2 };
		const out = -attackDir(team);
		const drive = this.track(holder)
			?.moves.filter((m) => m.t0 >= t0 - 1)
			.findLast((m) => dist(m.from, rim) - dist(m.to, rim) > 6);
		if (!drive || dist(drive.to, rim) > 18) {
			return;
		}
		const D = drive.to;
		// Round the rim from straight out (0) toward either sideline.
		const angle = (p: Pt) => Math.atan2(p.y - rim.y, (p.x - rim.x) * out);
		const aD = angle(D);
		const a = drive.t0 + (drive.t1 - drive.t0) * 0.25;
		const b = drive.t1 + 150;
		// Where the rest of them will be - nobody drifts on top of a
		// teammate.
		const taken = new Map(
			this.slots(team)
				.filter((p) => p !== holder)
				.map((p) => [p, this.posOf(p)] as const),
		);
		const clear = (pid: number, q: Pt) =>
			[...taken].every(([p, at]) => p === pid || dist(at, q) >= 6);
		// How far off the way he goes, from where he set off to where it ends.
		const A = drive.from;
		const toLine = (q: Pt): number => {
			const vx = D.x - A.x;
			const vy = D.y - A.y;
			const L = vx * vx + vy * vy;
			const u =
				L > 0
					? Math.max(0, Math.min(1, ((q.x - A.x) * vx + (q.y - A.y) * vy) / L))
					: 0;
			return Math.hypot(q.x - A.x - vx * u, q.y - A.y - vy * u);
		};
		for (const pid of this.slots(team)) {
			const tr = this.track(pid);
			if (
				pid === holder ||
				!tr ||
				(this.free.get(pid) ?? 0) > a ||
				tr.acts.some((x) => x.t1 > a && x.t0 < b)
			) {
				continue;
			}
			const P = this.posOf(pid);
			const r = dist(P, rim);
			let Q: Pt | undefined;
			let from = a;
			if (r >= 21) {
				// On the arc, on the ball's side of the floor: along it, away.
				const aP = angle(P);
				const diff = aP - aD;
				if (Math.abs(diff) < 0.15 || Math.abs(diff) > 1.3) {
					continue;
				}
				// (Out of room in the corner, with the drive coming baseline at
				// him: he lifts up out of it instead.)
				const step = this.rand(3.5, 5) / r;
				const way =
					Math.abs(aP + Math.sign(diff) * step) > ARC_EDGE
						? -Math.sign(diff)
						: Math.sign(diff);
				// (Behind the line, he stays well behind it - his toes too.)
				const deep = Math.abs(P.x - rim.x) + RIM_INSET;
				const behind =
					deep < 14
						? Math.abs(P.y - rim.y) > 22 - ON_THE_LINE
						: r > 23.75 - ON_THE_LINE;
				const R = behind ? Math.max(r, 23.75 + 0.75) : r;
				const along = (w: number) => {
					const to = Math.max(-ARC_EDGE, Math.min(ARC_EDGE, aP + w * step));
					return inPlay({
						x: rim.x + Math.cos(to) * R * out,
						y: rim.y + Math.sin(to) * R,
					});
				};
				// (Not into a teammate's spot: the other way, or nowhere.)
				Q = [along(way), along(-way)].find((q) => clear(pid, q));
			} else if (
				r < 14 &&
				((this.rank.get(pid) ?? 4) >= 6 ||
					this.slots(team).indexOf(pid) >= 4) &&
				dist(P, D) < 9
			) {
				// A big in the way: off to the dunker spot on the far side,
				// along the baseline.
				const side = D.y >= rim.y ? -1 : 1;
				Q = clampPt({ x: rim.x - out * 2.5, y: rim.y + side * 8 });
			} else if (
				r < 21 &&
				(dist(P, D) < DRIVE_ROOM || toLine(P) < DRIVE_LANE)
			) {
				// (Off as it starts: stood in its way, he is gone by the time it
				// comes by.)
				from = drive.t0;
				// Anybody else inside the arc where it is going - the man who
				// drove and kicked it out, still in there; a cutter, a screener
				// stood where the attack comes: out the far side - a big down to
				// the dunker spot or the short corner, anybody else to the short
				// corner, the wing or the slot - whichever is nearest and open.
				const side = D.y >= rim.y ? -1 : 1;
				const big =
					(this.rank.get(pid) ?? 4) >= 6 || this.slots(team).indexOf(pid) >= 4;
				// (Depth from the baseline, and across from the middle.)
				const spots: [number, number][] = big
					? [
							[RIM_INSET - 2.5, 8],
							[RIM_INSET, 15],
						]
					: [
							[RIM_INSET, 15],
							[RIM_INSET + 13.2, 21.8],
							[RIM_INSET + 22, 13],
						];
				Q = spots
					.map(([deep, across]) =>
						clampPt(spot(team, deep, rim.y + side * across)),
					)
					.filter(
						(q) =>
							clear(pid, q) &&
							dist(q, D) >= DRIVE_ROOM &&
							toLine(q) >= DRIVE_LANE,
					)
					.sort((u, v) => dist(P, u) - dist(P, v))[0];
			}
			if (Q && dist(P, Q) > 1.5 && clear(pid, Q)) {
				this.goBy(
					pid,
					Q,
					from,
					b,
					r >= 21 ? "drift" : dist(P, Q) > 8 ? "run" : "slide",
				);
				taken.set(pid, Q);
			}
		}
	}

	private playDribble(
		run: Running,
		who: number,
		to: string,
		kind: string,
		t0: number,
		// Off a handoff he took on the move: he goes on with it.
		handed = false,
	): number {
		const pid = run.roles[who]!;
		const dir = attackDir(run.team);
		// Driven across the floor into the middle, he goes on into the lane
		// the far side of it - not back the way he came.
		if (/^[LR]_(lane|float)$/.test(to) && this.holder === pid) {
			const last = this.track(pid)?.moves.at(-1);
			if (last?.anim === "dribble") {
				const across = last.to.y - last.from.y;
				if (
					Math.abs(across) >= 8 &&
					across * (this.at(run, to).y - last.to.y) < 0
				) {
					(run.across ??= new Set()).add(to);
					run.jitter.delete(to);
				}
			}
		}
		let P =
			to === "rim" ? this.nearRim(run.team, this.posOf(pid)) : this.at(run, to);
		const start = Math.max(t0, this.free.get(pid) ?? 0);
		if (this.holder !== pid) {
			// The ball never got to him: he just goes.
			return this.go(pid, P, start, 14, "run");
		}
		// Over half court with it, he never takes it back over - not to the
		// spot a break has him bringing it up to, from farther up already.
		if (
			(this.posOf(pid).x - COURT_W / 2) * dir > 2 &&
			(P.x - COURT_W / 2) * dir < 3
		) {
			P = { x: COURT_W / 2 + dir * 3.5, y: P.y };
		}
		// Nor, on the break, back up the floor at all to bring it up: ahead of
		// it already - the ball won up the floor - he goes on from where he is.
		// (Not out to the arc and back in again with it.)
		if (
			kind === "advance" &&
			run.play.cat === "break" &&
			(P.x - this.posOf(pid).x) * dir < -BREAK_NO_BACK
		) {
			return start;
		}
		this.keepComing(pid, start);
		// The hand on the side he goes (away from his man). Coming off a ball
		// screen with the man who set it in the way on that side, he goes
		// round his shoulder - not dribbles it into him going by.
		const A = this.posOf(pid);
		const hand = this.handFor(A, P, dir);
		let around: Pt | undefined;
		const sc = run.screen;
		if (sc?.ball && sc.user === pid && dist(A, P) > 1) {
			const u = unitVec(A, P);
			// (His right is to the left of the way he faces, on this floor.)
			const k = hand === "R" ? 1 : -1;
			for (const s of sc.screeners) {
				const S = this.posOf(s);
				const ahead = (S.x - A.x) * u.x + (S.y - A.y) * u.y;
				const side = u.x * (S.y - A.y) - u.y * (S.x - A.x);
				if (
					ahead > 1 &&
					ahead < dist(A, P) - 1 &&
					Math.abs(side) < ROUND_SCREEN &&
					side * k > -1
				) {
					around = clampPt({
						x: S.x - u.y * k * ROUND_SCREEN,
						y: S.y + u.x * k * ROUND_SCREEN,
					});
				}
			}
		}
		// A drive at his man - now and then with a move to get by him first.
		// (Not straight off a handoff taken on the move: the handoff is what
		// gets him by his man, and he is on his way - though the draws are
		// made all the same. Nor off a ball screen just set for him: the
		// screen is what gets him by, and he comes off it as it is set - not
		// stood in front of it crossing over while the big holds it and the
		// rest of the floor comes in round them.)
		const screened =
			sc?.ball === true &&
			sc.user === pid &&
			start - sc.set < SCREEN_FRESH &&
			sc.screeners.some((s) => dist(this.posOf(s), A) < SCREEN_HERE);
		const move = kind === "crossover" || kind === "hesitation";
		const drive =
			!move &&
			(kind === "attack" ||
				kind === "drive_baseline" ||
				kind === "drive_middle") &&
			dist(this.posOf(pid), P) > 7 &&
			this.rng() < 0.5;
		if ((handed || screened) && drive) {
			this.skipCrossover(pid, P, start);
		} else if (move || drive) {
			const t = this.driveTo(pid, P, start, dir, "crossover");
			this.hold(pid, t, "dribble");
			return t;
		}
		if (kind === "step_back") {
			const t = this.driveTo(pid, P, start, dir, "stepBack");
			this.hold(pid, t, "hold");
			return t;
		}
		// (Off a handoff on the move, a stride with it in both hands first.)
		if (handed) {
			this.hold(pid, start, "hold");
		}
		this.hold(
			pid,
			handed ? start + GATHER_MS : start,
			kind === "snake" ? "cross" : "dribble",
			kind === "snake" ? undefined : hand,
		);
		let t = start;
		if (kind === "snake") {
			// Back across his man first, then around the corner.
			const from = this.posOf(pid);
			const u = unitVec(from, P);
			t = this.go(
				pid,
				clampPt({
					x: from.x + (P.x - from.x) * 0.4 - u.y * 2.5,
					y: from.y + (P.y - from.y) * 0.4 + u.x * 2.5,
				}),
				t,
				DRIBBLE_SPEED.snake!,
				"dribble",
			);
		}
		const push = kind === "advance" && run.play.cat === "break";
		const speed = DRIBBLE_SPEED[push ? "push" : kind] ?? 16;
		if (around && kind !== "snake") {
			// (Round him and on, in stride: no pulling up at his shoulder to
			// set off again from there.)
			const a = unitVec(this.posOf(pid), around);
			const b = unitVec(around, P);
			const onto =
				Math.min(
					speed,
					paceAfter(
						dist(this.posOf(pid), around),
						this.carried(pid, around, t, speed),
					),
				) * keepThrough(a.x * b.x + a.y * b.y);
			t = this.go(pid, around, t, speed, "dribble", undefined, { onto });
		}
		return this.go(
			pid,
			P,
			t,
			speed,
			"dribble",
			kind === "retreat" ? dir : undefined,
		);
	}

	private playMove(
		run: Running,
		pid: number,
		to: string,
		style: string,
		t0: number,
		screen: Running["screen"],
		// The screen is one set for him this step, to come off now.
		fresh = false,
	): number {
		const rim = { x: rimX(run.team), y: COURT_H / 2 };
		const from = this.posOf(pid);
		let P = to === "rim" ? this.nearRim(run.team, from, 3.6) : this.at(run, to);
		// Once the ball is over half court nobody goes back over it - not to
		// trail the play, not for a pass.
		const dir = attackDir(run.team);
		// Rolling to the rim as the man with the ball drives into the lane -
		// off the handoff he gave him, the screen he set - he goes in to the
		// far side of the rim from where the drive is headed: not down the
		// lane at the driver's shoulder, the two of them and both their men
		// one pack all the way in. (There for the dump-off across, and out of
		// the floater's way.)
		if (to === "rim" && (style === "roll" || style === "dive")) {
			const h = this.holder;
			const drive =
				h !== undefined && h !== pid && this.teamOf(h) === run.team
					? this.track(h)?.moves.at(-1)
					: undefined;
			const across = drive ? drive.to.y - rim.y : 0;
			if (
				drive?.anim === "dribble" &&
				drive.t1 > t0 &&
				dist(drive.to, rim) < ROLL_AWAY_IN &&
				Math.abs(across) > ROLL_AWAY_MID
			) {
				P = clampPt({
					x: rim.x - dir * ROLL_AWAY_OUT,
					y: rim.y - Math.sign(across) * ROLL_AWAY_ACROSS,
				});
			}
		}
		const ballOver =
			this.holder !== undefined &&
			(this.posAt(this.holder, t0).x - COURT_W / 2) * dir > 2;
		if (ballOver && (P.x - COURT_W / 2) * dir < 3) {
			// (A spot the play has back in the backcourt - the big staying
			// home, say - with the ball over already: he trails the play, up
			// to the top of it (see trailUp) - not out to the sideline at half
			// court, to stand and watch from there.)
			P = this.trailSpot(run, pid, t0, this.ownDraws(pid, t0)).Q;
		}
		// On the break, nobody runs back down the floor to the spot the play
		// has him trailing in, from farther up it already - the big who beat
		// everybody down after the basket, say: he holds where he is (over
		// to its side of the floor, if it is a way across), and the ball is
		// not held up waiting on him.
		// (Down low already, though, he is not slid across into the paint
		// under the rim: he holds where he is - or, the ball to come to him,
		// comes back out to his spot for it, not stood under the rim for it.)
		if (run.play.cat === "break" && (P.x - from.x) * dir < -BREAK_NO_BACK) {
			const across = { x: from.x, y: P.y };
			const deep =
				dist(across, rim) < BREAK_DEEP || dist(from, rim) < BREAK_DEEP;
			const role = run.roles.indexOf(pid);
			if (!(deep && role >= 0 && needed(run, undefined, role as Role))) {
				// (Nor, for a spot back over half court with him over it
				// already, across to the sideline out there.)
				if (
					deep ||
					Math.abs(P.y - from.y) < BREAK_NO_BACK ||
					((P.x - COURT_W / 2) * dir < 0 && (from.x - COURT_W / 2) * dir > 0)
				) {
					return Math.max(t0, this.free.get(pid) ?? 0);
				}
				P = across;
			}
		}
		// (Trailing the break is no jog: he runs the floor.)
		const speed =
			run.play.cat === "break" && style === "jog"
				? TRAIL_SPEED
				: (MOVE_SPEED[style] ?? 13);
		let t = Math.max(t0, this.free.get(pid) ?? 0);
		// Still set in a screen: he holds it until the man it was for has come
		// past his shoulder and the man chasing him has run into it - then
		// rolls, or pops. (A slip leaves it early.)
		const set = this.track(pid)?.acts.findLast((x) => x.anim === "screen");
		if (
			screen?.screeners.includes(pid) &&
			style !== "slip" &&
			set &&
			set.t1 >= t0 - 60
		) {
			const user = this.track(screen.user)?.moves.findLast(
				(m) => m.t0 >= t0 - 1,
			);
			let by = t0 + 450;
			if (user) {
				const dx = user.to.x - user.from.x;
				const dy = user.to.y - user.from.y;
				const L2 = dx * dx + dy * dy;
				const u =
					L2 > 0.01
						? Math.min(
								1,
								Math.max(
									0,
									((from.x - user.from.x) * dx + (from.y - user.from.y) * dy) /
										L2,
								),
							)
						: 0;
				by = user.t0 + (user.t1 - user.t0) * u + 450;
			}
			t = Math.max(t, Math.min(by, t0 + 1500));
			set.t1 = Math.max(set.t1, t);
		}
		if (style === "v_cut" && dist(from, P) > 6) {
			// In a few steps, then back out hard: the V.
			const u = unitVec(from, rim);
			t = this.go(
				pid,
				clampPt({ x: from.x + u.x * 3.5, y: from.y + u.y * 3.5 }),
				t,
				speed * 0.7,
				"run",
			);
		} else if (style === "backdoor") {
			// A step out as if for the ball, then behind his man to the rim.
			const u = unitVec(rim, from);
			t = this.go(
				pid,
				clampPt({ x: from.x + u.x * 1.8, y: from.y + u.y * 1.8 }),
				t,
				8,
				"run",
			);
		} else if (
			screen &&
			screen.user === pid &&
			(style === "curl" ||
				style === "rip_cut" ||
				style === "iverson_cut" ||
				style === "shallow_cut")
		) {
			// Tight around the screen, then on.
			const u = unitVec(screen.at, rim);
			const W = clampPt({
				x: screen.at.x + u.x * 2.2,
				y: screen.at.y + u.y * 2.2,
			});
			if (dist(from, W) > 2 && dist(W, P) > 2) {
				t = this.go(pid, W, t, speed, "run");
			}
		}
		if (
			fresh &&
			screen &&
			screen.user === pid &&
			!screen.ball &&
			style !== "curl" &&
			style !== "rip_cut" &&
			style !== "iverson_cut" &&
			style !== "shallow_cut"
		) {
			// Any other way off a screen set for him (two in a row, a
			// stagger): by each screener's shoulder in turn on his way - not
			// off past it somewhere it isn't, the screen set for nobody.
			const A = this.posOf(pid);
			const L = dist(A, P);
			if (L > 2) {
				const u = unitVec(A, P);
				const by = screen.screeners
					.map((q) => {
						const S = this.posOf(q);
						return {
							S,
							along: (S.x - A.x) * u.x + (S.y - A.y) * u.y,
							side: (S.x - A.x) * -u.y + (S.y - A.y) * u.x,
						};
					})
					.filter(
						(q) =>
							q.along > 1 &&
							q.along < L - 1 &&
							Math.abs(q.side) > SCREEN_SHOULDER,
					)
					.sort((a, b) => a.along - b.along);
				for (const q of by) {
					const k = Math.sign(q.side) * SCREEN_SHOULDER;
					t = this.go(
						pid,
						clampPt({ x: q.S.x + u.y * k, y: q.S.y - u.x * k }),
						t,
						speed,
						"run",
					);
				}
			}
		}
		return this.go(
			pid,
			P,
			t,
			speed,
			style === "walk" || style === "seal" ? "walk" : "run",
		);
	}

	// A handoff: he comes to the man with the ball and takes it off his hip;
	// the man who gave it turns into a screen for him.
	private playHandoff(
		run: Running,
		giver: number,
		recv: number,
		get: boolean,
		t0: number,
		k: number,
		planted: { pid: number; t: number; anim: AnimName; look: Pt }[],
		next: PlayAction[] | undefined,
		// What the set has anybody do after it, this step and the next.
		after: PlayAction[] = next ?? [],
	): number {
		const rim = { x: rimX(run.team), y: COURT_H / 2 };
		const G = this.posOf(giver);
		const R = this.posOf(recv);
		if (!get) {
			// The fake: he runs on past and the ball stays put.
			const u = unitVec(R, G);
			return this.go(
				recv,
				clampPt({ x: G.x + u.x * 4, y: G.y + u.y * 4 }),
				Math.max(t0, this.free.get(recv) ?? 0),
				14,
				"run",
			);
		}
		if (this.holder !== giver) {
			return this.holder === undefined
				? t0
				: this.passTo(this.holder, recv, t0);
		}
		const meet = clampPt({
			x: G.x + (R.x - G.x) * 0.15,
			y: G.y + (R.y - G.y) * 0.15,
		});
		const held = this.hold(
			giver,
			Math.max(t0, this.free.get(giver) ?? 0),
			"hold",
		);
		// Going on with it - a drive, a run, from there - he takes it on the
		// move: timed to come by as the man with it is ready to give it, the
		// ball handed to him going by and him on his way with it, not stood
		// there waiting on it and then stood there with it.
		// (Pulling up off it, the same: off the giver's hip and up - see
		// playToShot.)
		const role = run.roles.indexOf(recv);
		const o = run.option;
		const pulls =
			o !== undefined &&
			o.after === k &&
			o.branch.length === 0 &&
			o.shooter === role &&
			o.kind === "pull_up" &&
			(o.zone === "three" || o.zone === "mid");
		const on =
			role >= 0 &&
			(pulls ||
				after.some(
					(x) => (x.type === "dribble" || x.type === "move") && x.who === role,
				));
		const ready = Math.max(this.free.get(giver) ?? 0, held + 180);
		let start = Math.max(t0, this.free.get(recv) ?? 0);
		let onto = 0;
		if (on && dist(R, meet) > 1) {
			const L = dist(R, meet);
			const pace = (at: number) =>
				Math.min(HANDOFF_PACE, paceAfter(L, this.carried(recv, meet, at, 12)));
			const ms = (at: number) =>
				Math.max(
					240,
					runMs(L, 12, this.carried(recv, meet, at, 12), 1, pace(at)),
				);
			start = Math.max(start, ready + 120 - ms(start));
			onto = pace(start);
		}
		const there = this.go(recv, meet, start, 12, "run", undefined, { onto });
		// (On the move, it is in his hands as he gets there.)
		const tx = Math.max(there - (onto > 0 ? 130 : 120), ready);
		const moving = onto > 0 && tx <= there - 129;
		this.act(giver, "pass", tx - 180, tx + 120, { look: meet });
		this.fly(tx, tx + 130, { pid: giver }, { pid: recv });
		this.act(recv, "catch", tx + 30, tx + 200);
		this.hold(recv, tx + 130, "hold");
		this.hold(recv, tx + 130 + GATHER_MS, "dribble");
		this.free.set(
			recv,
			Math.max(this.free.get(recv) ?? 0, moving ? there : tx + 200),
		);
		const u = unitVec(G, rim);
		const S = clampPt({ x: G.x + u.x * 2.2, y: G.y + u.y * 2.2 });
		const set = this.go(giver, S, tx + 140, 7, "run");
		planted.push({ pid: giver, t: set, anim: "screen", look: meet });
		run.screen = {
			screeners: [giver],
			user: recv,
			ball: true,
			at: S,
			step: k,
			coverage: this.coverageFor(run, giver, recv, true, next),
			set,
			...(moving ? { handed: there } : {}),
		};
		return Math.max(tx + 350, set);
	}

	// Where a man down low with the ball goes to feed the post from, the
	// post man at S: up to the elbow his side - a high-low, a couple of
	// dribbles - not all the way back out to the 3-point line on the wing
	// to throw it back in, the man in the post stood waiting on it. (The
	// wing, if a teammate is at the elbow already.)
	private entryFrom(team: Side, pid: number, S: Pt): Pt {
		const side = S.y >= COURT_H / 2 ? 1 : -1;
		const elbow = spot(team, FT_LINE_DEPTH, COURT_H / 2 + side * 8);
		const taken = this.slots(team).some(
			(m) => m !== pid && dist(this.posOf(m), elbow) < 6,
		);
		return taken ? spot(team, 17.5, COURT_H / 2 + side * 19) : elbow;
	}

	// When he took the handoff he is going on with, if he has the ball off
	// one this step or the last (see playHandoff).
	private handedOn(run: Running, pid: number): number | undefined {
		const sc = run.screen;
		return sc?.handed !== undefined &&
			sc.user === pid &&
			this.holder === pid &&
			this.hasItFrom(pid) <= sc.handed + 400
			? sc.handed
			: undefined;
	}

	private playPost(
		run: Running,
		pid: number,
		at: string,
		move: string,
		t0: number,
		planted: { pid: number; t: number; anim: AnimName; look: Pt }[],
	): number {
		const dir = attackDir(run.team);
		const start = Math.max(t0, this.free.get(pid) ?? 0);
		if (this.holder === pid) {
			if (move === "back_down" || move === "seal") {
				return this.backDown(pid, start, dir);
			}
			if (move === "face_up") {
				this.turn(pid, start, dir);
				this.hold(pid, start, "hold");
				return start + 450;
			}
			// A drop step, an up-and-under: one big step at the rim.
			const from = this.posOf(pid);
			const u = unitVec(from, { x: rimX(run.team), y: COURT_H / 2 });
			this.hold(pid, start, "dribble");
			const t = this.go(
				pid,
				clampPt({ x: from.x + u.x * 2.4, y: from.y + u.y * 2.4 }),
				start + 80,
				9,
				"run",
				dir,
			);
			this.hold(pid, t, "hold");
			return t;
		}
		// Without it: to the block, sealing his man, calling for it.
		const there = this.go(pid, this.at(run, at), start, 10, "run");
		const ball =
			this.holder !== undefined ? this.posOf(this.holder) : this.ballAt;
		planted.push({ pid, t: there, anim: "postUp", look: ball });
		return there + 300;
	}

	private coverageFor(
		run: Running,
		screener: number,
		user: number,
		onBall: boolean,
		next: PlayAction[] | undefined,
	): Coverage {
		const rs = this.rank.get(screener) ?? 4;
		const ru = this.rank.get(user) ?? 4;
		const r = this.rng();
		if (!onBall) {
			return Math.abs(rs - ru) <= 2 && r < 0.3 ? "switch" : "chase";
		}
		// Mostly, the way it was played is what the shot that came off it
		// reads: the handler pulls up because the big sat back, the roller
		// is open because the big came up to the ball, two on the ball leave
		// a man open for the kick, a switch leaves a big on a guard.
		const o = run.option;
		if (o && r < 0.8) {
			const shooter = run.roles[o.shooter];
			const close = o.zone === "rim" || o.kind === "floater";
			const read: Coverage =
				shooter === user
					? close && rs >= 5
						? "switch"
						: close
							? "hedge"
							: "drop"
					: shooter === screener
						? o.zone === "post"
							? "switch"
							: close
								? this.rng() < 0.6
									? "hedge"
									: "blitz"
								: "drop"
						: this.rng() < 0.6
							? "blitz"
							: "hedge";
			return read;
		}
		if (rs <= 4 && ru <= 4) {
			return r < 0.65 ? "switch" : "chase";
		}
		const passes =
			next?.some((a) => a.type === "pass" && run.roles[a.who] === user) ??
			false;
		if (passes && r < 0.25) {
			return "blitz";
		}
		return r < 0.55
			? "drop"
			: r < 0.75
				? "hedge"
				: r < 0.88
					? "switch"
					: "drop";
	}

	// Where a defender wants to be, by the numbers from player tracking: on the
	// ball, between his man and the rim, tighter the nearer the rim; off it,
	// sagged toward the rim and shaded toward the ball, more the farther his
	// man is from it - so the weak side sinks into the paint.
	private defensePoint(
		team: Side,
		at: Pt,
		ball: Pt,
		onBall: boolean,
		// How far up the floor (feet from the rim) the man on the ball goes to
		// pick him up.
		pickUp = PICK_UP,
	): Pt {
		const rim = { x: rimX(team), y: COURT_H / 2 };
		// A man still up the floor - coming up it, trailing the play, still at
		// the other end - is nobody's worry yet: off the ball, his man waits
		// on him back inside the arc - the farther up the floor he still is,
		// the deeper, down in the paint for one a long way off - and picks him
		// up as he comes. (Not stood out level with the ball, the five of
		// them a line across the floor, nobody back at the rim.)
		const deep = Math.abs(at.x - rim.x);
		const reach = onBall ? pickUp : UP_FLOOR;
		const sink = onBall
			? 0
			: Math.min(SINK_MAX, Math.max(0, deep - UP_FLOOR) * SINK_PER_FT);
		const man =
			deep > reach
				? {
						x: rim.x + ((at.x - rim.x) * reach) / deep,
						y: rim.y + ((at.y - rim.y) * reach) / deep,
					}
				: at;
		const toRim = dist(man, rim);
		const ur = unitVec(man, rim);
		if (onBall) {
			// Up on him: an arm's length at the arc, a step more out past it,
			// picking him up loose only coming up the floor.
			const gap =
				toRim < 10
					? 2.2
					: toRim < 18
						? 2.8
						: toRim < 24
							? 3.3
							: toRim < 30
								? 3.9
								: 5.5;
			// (Right under it with the ball, he is played from out in front
			// of him, toward the floor - not from on top of him.)
			if (toRim < 1.5) {
				const out = unitVec(rim, { x: COURT_W / 2, y: COURT_H / 2 });
				return clampPt({ x: man.x + out.x * 2.2, y: man.y + out.y * 2.2 });
			}
			const k = Math.min(gap, toRim * 0.5);
			return clampPt(
				sideOn(man, { x: man.x + ur.x * k, y: man.y + ur.y * k }, rim.x),
			);
		}
		const dBall = dist(man, ball);
		const [off, sag] =
			dBall < 10
				? [3.5, 0.8]
				: dBall < 18
					? [5.3, 3.6]
					: dBall < 25
						? [6.4, 5.3]
						: dBall < 32
							? [7.5, 6.3]
							: dBall < 40
								? [9.2, 8.2]
								: [10.7, 9.7];
		const s = Math.min(sag, toRim * 0.6);
		const side = Math.sqrt(Math.max(0, off * off - s * s));
		const ub = unitVec(man, ball);
		const p = {
			x: man.x + ur.x * s + ub.x * side,
			y: man.y + ur.y * s + ub.y * side,
		};
		// (Further in, waiting on him.)
		if (sink > 0) {
			const u = unitVec(p, rim);
			const k = Math.min(sink, dist(p, rim) - 3);
			if (k > 0) {
				p.x += u.x * k;
				p.y += u.y * k;
			}
		}
		// Never camped under the rim.
		const pr = dist(p, rim);
		if (pr < 3) {
			const u = unitVec(rim, pr > 0.1 ? p : man);
			return clampPt({ x: rim.x + u.x * 3, y: rim.y + u.y * 3 });
		}
		return clampPt(sideOn(man, p, rim.x));
	}

	// A defender to his spot by `by`: sliding if it is close, running if not -
	// shadowing `man`, if that is what the spot is for.
	private shadow(
		d: number,
		P: Pt,
		from: number,
		by: number,
		team: Side,
		man?: number,
	) {
		const start = Math.max(from, this.free.get(d) ?? 0);
		const dd = dist(this.posOf(d), P);
		if (dd < 0.4) {
			return;
		}
		const n = this.track(d)?.moves.length ?? 0;
		const secs = Math.max(0.3, (by - start) / 1000);
		// (A long way off - the other end of the floor - he runs there, not
		// eases there over all the time there is.)
		const speed = Math.min(
			SPRINT,
			Math.max(dd > 25 ? RUN : dd > 14 ? JOG : 4, dd / secs),
		);
		// He shuffles to stay with his man, square to him - turning and
		// running only to cover real ground fast.
		const slide = dd < 14 && speed <= 17;
		this.go(
			d,
			P,
			start,
			speed,
			slide ? "slide" : "run",
			slide ? (-attackDir(team) as 1 | -1) : undefined,
		);
		if (man !== undefined) {
			this.marks(d, man, n);
		}
	}

	// The defense through a step: every man's defender goes where the ball
	// and his man say he should be; the screen from the step before is played
	// the way it was called; a drive gets a man stepping in front of it.
	private guardStep(
		run: Running,
		acts: PlayAction[],
		t0: number,
		t1: number,
		prev?: Running["screen"],
		k = -1,
	) {
		const { team } = run;
		const rim = { x: rimX(team), y: COURT_H / 2 };
		const sc = prev && prev.step === k - 1 ? prev : undefined;
		if (sc?.coverage === "switch") {
			const a = this.defenderOf(sc.user);
			const b = this.defenderOf(sc.screeners[0]!);
			if (a !== undefined && b !== undefined && a !== b) {
				this.guarding.set(sc.user, b);
				this.guarding.set(sc.screeners[0]!, a);
				// "Switch!" - each points out the man he has now.
				this.gesture(a, "point", t0 + 40, t0 + 900, sc.screeners[0], 0.7);
				this.gesture(b, "point", t0 + 80, t0 + 940, sc.user, 0.7);
			}
		}
		const holder = this.holder;
		const ball = holder !== undefined ? this.posOf(holder) : this.ballAt;
		const targets = new Map<number, Pt>();
		// Whose man each defender's spot shadows - until something else (a
		// screen to play, a drive to help on) takes him.
		const manOf = new Map<number, number>();
		for (const pid of this.slots(team)) {
			const d = this.defenderOf(pid);
			if (d !== undefined) {
				targets.set(
					d,
					this.defensePoint(
						team,
						this.posOf(pid),
						ball,
						pid === holder,
						run.play.cat === "break" ? BREAK_PICK_UP : PICK_UP,
					),
				);
				manOf.set(d, pid);
			}
		}
		const special = (d: number | undefined, P: Pt) => {
			if (d !== undefined) {
				targets.set(d, P);
				manOf.delete(d);
			}
		};
		// A man posting up has his man on his body: behind him, between him
		// and the rim, or fronting him three-quarters on the side of the ball.
		for (const pid of this.slots(team)) {
			const d = this.defenderOf(pid);
			if (
				d === undefined ||
				pid === holder ||
				manOf.get(d) !== pid ||
				!this.track(pid)?.acts.some(
					(a) => a.anim === "postUp" && a.t1 > t0 && a.t0 < t1,
				)
			) {
				continue;
			}
			const M = this.posOf(pid);
			const ur = unitVec(M, rim);
			const ub = unitVec(M, ball);
			const front = this.rng() < 0.4;
			const u = front
				? unitVec(
						{ x: 0, y: 0 },
						{ x: ub.x * 0.7 + ur.x * 0.3, y: ub.y * 0.7 + ur.y * 0.3 },
					)
				: unitVec(
						{ x: 0, y: 0 },
						{ x: ur.x * 0.8 + ub.x * 0.2, y: ur.y * 0.8 + ub.y * 0.2 },
					);
			targets.set(
				d,
				clampPt({ x: M.x + u.x * BODY * 1.05, y: M.y + u.y * BODY * 1.05 }),
			);
		}
		const via = new Map<number, { at: Pt; by: number }>();
		const late = new Map<number, number>();
		const chase = new Map<number, { man: number; at: Pt; from: number }>();
		if (sc) {
			const ud = this.defenderOf(sc.user);
			const sd = this.defenderOf(sc.screeners[0]!);
			const U = this.posOf(sc.user);
			if (sc.ball && sd !== undefined) {
				if (sc.coverage === "drop") {
					// Back in the paint, between the ball and the rim.
					const u = unitVec(rim, U);
					const kk = Math.min(12, Math.max(4, dist(U, rim) - 7));
					special(sd, { x: rim.x + u.x * kk, y: rim.y + u.y * kk });
				} else if (sc.coverage === "hedge") {
					// Out at the ball for a beat, then back to his man.
					const u = unitVec(sc.at, U);
					via.set(sd, {
						at: clampPt({ x: sc.at.x + u.x * 2.6, y: sc.at.y + u.y * 2.6 }),
						by: t0 + 450,
					});
				} else if (sc.coverage === "blitz" && ud !== undefined) {
					const ur = unitVec(U, rim);
					special(
						sd,
						clampPt({
							x: U.x + ur.x * 2 - ur.y * 2,
							y: U.y + ur.y * 2 + ur.x * 2,
						}),
					);
					special(
						ud,
						clampPt({
							x: U.x + ur.x * 2 + ur.y * 2,
							y: U.y + ur.y * 2 - ur.x * 2,
						}),
					);
				}
			}
			if (sc.coverage === "chase" && ud !== undefined && sc.user !== holder) {
				// Over the top of the screen, a step behind him.
				const u = unitVec(U, sc.at);
				special(ud, clampPt({ x: U.x + u.x * 2.6, y: U.y + u.y * 2.6 }));
				late.set(ud, 300);
				// Not before his man goes: where the step takes him can come
				// seconds into it - after he has given up the ball, say - and
				// till then he is played where he is.
				const off = this.track(sc.user)?.moves.find((m) => m.t0 >= t0 - 1);
				if (off && off.t0 - t0 > 600) {
					const seg = this.ball.findLast((b) => b.t0 <= t0 + 120);
					const has = seg?.kind === "hold" && seg.pid === sc.user;
					chase.set(ud, {
						man: sc.user,
						at: this.defensePoint(team, off.from, has ? off.from : ball, has),
						from: off.t0,
					});
				}
			}
		}
		// A drive at the rim - or round a screen to the elbow - and what the
		// defense does about it.
		const onBall = holder !== undefined ? this.defenderOf(holder) : undefined;
		const D = holder !== undefined ? this.posOf(holder) : undefined;
		// (One that starts in this step: a drive set off only once the step is
		// done is the next step's to play - played here as well, it had the man
		// on the ball off after it before the step was over, and still on his
		// way when the next step wanted him somewhere else.)
		const drive =
			holder === undefined
				? undefined
				: this.track(holder)
						?.moves.filter((m) => m.t0 >= t0 - 1 && m.t0 < t1)
						.findLast((m) => dist(m.from, rim) - dist(m.to, rim) > 4);
		// The man on the ball goes with the drive when it goes - not off for
		// where it ends up from the start of the step, his man still stood
		// there with it.
		const goes =
			drive && onBall !== undefined && manOf.get(onBall) === holder
				? drive.t0
				: undefined;
		const drove =
			holder !== undefined &&
			acts.some(
				(a) =>
					a.type === "dribble" &&
					run.roles[a.who] === holder &&
					a.kind !== "retreat",
			);
		// The man the ball goes out to next - kicked out of it, skipped across,
		// dumped off to under the rim - is open because his man left him to
		// help: in front of the drive, on the man rolling to the rim, or sunk
		// into the lane. He pays for it on the closeout.
		const o = run.option;
		const last = o ? Math.min(o.after, run.play.steps.length - 1) : -1;
		const kicked =
			o &&
			(o.pass === "kickout" || o.pass === "skip" || o.pass === "dump_off") &&
			k === (o.branch.length > 0 ? last + 1 : last)
				? run.roles[o.shooter]
				: undefined;
		const hd =
			kicked === undefined || kicked === holder
				? undefined
				: this.defenderOf(kicked);
		let help: { d: number; at: Pt; from: number; by: number } | undefined;
		if (
			kicked !== undefined &&
			hd !== undefined &&
			hd !== onBall &&
			manOf.get(hd) === kicked &&
			!via.has(hd) &&
			!late.has(hd) &&
			!run.help?.includes(hd)
		) {
			const M = this.posOf(kicked);
			// What there is to help on: the ball, in close enough to matter, or
			// a man rolling or cutting to the rim.
			const threats: Pt[] = [];
			if (D && dist(D, rim) < 21) {
				threats.push(D);
			}
			for (const pid of this.slots(team)) {
				const m = this.track(pid)?.moves.at(-1);
				if (
					pid !== holder &&
					pid !== kicked &&
					m &&
					m.t0 >= t0 - 1 &&
					dist(m.to, rim) < 9 &&
					dist(m.from, rim) - dist(m.to, rim) > 4
				) {
					threats.push(m.to);
				}
			}
			const X = threats.sort((a, b) => dist(a, M) - dist(b, M))[0];
			const N = targets.get(hd)!;
			let H: Pt;
			if (X) {
				// In front of it, coming from his man's side.
				const u = unitVec(X, rim);
				const s = unitVec(X, M);
				H = {
					x: X.x + u.x * 2.6 + s.x * 1.2,
					y: X.y + u.y * 2.6 + s.y * 1.2,
				};
			} else {
				// Nothing there yet: sunk into the lane, off his man.
				const u = unitVec(N, rim);
				const kk = Math.min(5, dist(N, rim) * 0.35);
				H = { x: N.x + u.x * kk, y: N.y + u.y * kk };
			}
			// Never so far off him he could not get back out to him: from a
			// stunt - a hard step or two at the ball - all the way over (the
			// far side's man, the skip pass coming).
			const reach =
				this.rand(HELP_REACH[0], HELP_REACH[1]) + (o?.pass === "skip" ? 2 : 0);
			const far = dist(M, H);
			if (far > reach) {
				H = {
					x: M.x + ((H.x - M.x) * reach) / far,
					y: M.y + ((H.y - M.y) * reach) / far,
				};
			}
			// Off his man as the drive gets going, there as it gets there.
			const from = drive
				? drive.t0 + (drive.t1 - drive.t0) * 0.4
				: t0 + (t1 - t0) * 0.35;
			const by = drive ? drive.t1 + 60 : Math.max(from + 450, t1 - 100);
			help = { d: hd, at: clampPt(H), from, by };
			// He had to come because the man on the ball was beaten: that one
			// ends up on the driver's hip, not in front of him.
			if (drive && D && onBall !== undefined && manOf.get(onBall) === holder) {
				const u = unitVec(D, rim);
				const side =
					(help.at.x - D.x) * -u.y + (help.at.y - D.y) * u.x >= 0 ? 1 : -1;
				targets.set(
					onBall,
					clampPt({
						x: D.x + u.x * 0.5 + u.y * side * 1.9,
						y: D.y + u.y * 0.5 - u.x * side * 1.9,
					}),
				);
			}
		} else if (drove && D && dist(D, rim) < 14) {
			// Nobody to kick it to: the nearest help walls up in front of it,
			// between it and the rim - with his man until the drive is on its
			// way, then over (not off him from the start, his man left alone).
			let near: number | undefined;
			let best = Infinity;
			for (const [d, P] of targets) {
				const dd = dist(P, D);
				if (d !== onBall && dd < best) {
					best = dd;
					near = d;
				}
			}
			if (near !== undefined && best > 2.5) {
				const u = unitVec(rim, D);
				const kk = Math.min(8, Math.max(3.5, dist(D, rim) - 3.2));
				const at = clampPt({ x: rim.x + u.x * kk, y: rim.y + u.y * kk });
				if (
					manOf.has(near) &&
					!via.has(near) &&
					!late.has(near) &&
					!run.help?.includes(near)
				) {
					const from = drive
						? drive.t0 + (drive.t1 - drive.t0) * 0.4
						: t0 + (t1 - t0) * 0.35;
					const by = drive ? drive.t1 + 60 : Math.max(from + 450, t1 - 100);
					help = { d: near, at, from, by };
				} else {
					special(near, at);
				}
			}
		}
		// The others near the drive stunt at it as it goes by: a hard step or
		// two at the ball - to slow it, to make the driver look - and straight
		// back out to his man.
		const lag = run.play.cat === "break" ? 500 : 150;
		const stunts = new Map<number, { at: Pt; from: number; peak: number }>();
		if (
			drive &&
			holder !== undefined &&
			run.play.cat !== "break" &&
			dist(drive.from, rim) < 32
		) {
			const dx = drive.to.x - drive.from.x;
			const dy = drive.to.y - drive.from.y;
			const L2 = dx * dx + dy * dy || 1;
			const dur = drive.t1 - drive.t0;
			for (const [d, P] of targets) {
				const man = manOf.get(d);
				if (
					man === undefined ||
					man === holder ||
					d === help?.d ||
					via.has(d) ||
					late.has(d)
				) {
					continue;
				}
				// The gap he stands in, off the drive's line.
				const u = Math.min(
					1,
					Math.max(
						0,
						((P.x - drive.from.x) * dx + (P.y - drive.from.y) * dy) / L2,
					),
				);
				const q = { x: drive.from.x + dx * u, y: drive.from.y + dy * u };
				const off = dist(P, q);
				if (off < 3 || off > 14 || this.rng() > 0.6) {
					continue;
				}
				// With his man first, at the ball as it comes by, and back on
				// his man by the time the step is done - if he has the time.
				const from = Math.max(t0 + 120, drive.t0 + dur * (0.1 + u * 0.35));
				const peak = Math.min(
					drive.t0 + dur * (0.4 + u * 0.45),
					t1 + lag - 400,
				);
				if (
					peak - from < 180 ||
					dist(this.posOf(d), P) > ((from - t0 - 120) / 1000) * 18 + 1.5
				) {
					continue;
				}
				const step = Math.min(3.2, off * 0.45, ((peak - from) / 1000) * 14);
				const w = unitVec(P, q);
				stunts.set(d, {
					at: clampPt({ x: P.x + w.x * step, y: P.y + w.y * step }),
					from,
					peak,
				});
			}
		}
		// The screener rolling to the rim off a ball screen his own man
		// jumped out on - a hedge, a blitz - is open going there: the low man
		// (the help nearest where he is going) steps up into his path to tag
		// him, then gets back out to his own man.
		let tag: { d: number; at: Pt; from: number; peak: number } | undefined;
		const roller =
			sc?.ball && (sc.coverage === "hedge" || sc.coverage === "blitz")
				? sc.screeners[0]
				: undefined;
		const roll =
			roller === undefined
				? undefined
				: this.track(roller)
						?.moves.filter((m) => m.t0 >= t0 - 1)
						.findLast(
							(m) =>
								dist(m.from, rim) - dist(m.to, rim) > 6 && dist(m.to, rim) < 11,
						);
		if (roll && run.play.cat !== "break") {
			let low: number | undefined;
			let near = 17;
			for (const [d, P] of targets) {
				const man = manOf.get(d);
				if (
					man === undefined ||
					man === holder ||
					man === roller ||
					d === help?.d ||
					via.has(d) ||
					late.has(d) ||
					stunts.has(d)
				) {
					continue;
				}
				const away = dist(P, roll.to);
				if (away < near) {
					near = away;
					low = d;
				}
			}
			if (low !== undefined && this.rng() < 0.75) {
				const P = targets.get(low)!;
				// Off as the screen is set, in his path as he gets there: square
				// in it, a step short of where he is going - or as far toward
				// it as he can get.
				const from = Math.max(t0 + 120, roll.t0 - 200);
				const peak = Math.min(roll.t1 - 80, t1 + lag - 450);
				const u = unitVec(roll.to, roll.from);
				const want = clampPt({
					x: roll.to.x + u.x * 2.4,
					y: roll.to.y + u.y * 2.4,
				});
				const reach = Math.min(dist(P, want), ((peak - from) / 1000) * 19);
				const w = unitVec(P, want);
				if (
					peak - from >= 300 &&
					reach >= 3 &&
					dist(this.posOf(low), P) <= ((from - t0 - 120) / 1000) * 18 + 1.5
				) {
					tag = {
						d: low,
						at: clampPt({ x: P.x + w.x * reach, y: P.y + w.y * reach }),
						from,
						peak,
					};
				}
			}
		}
		// Whoever meets the shot gives a step toward it - but stays on his man:
		// the shot brings him in, in time to get there from wherever he is
		// (see shoot). Not gone to the spot steps ahead of it and stood there,
		// seconds of his man left alone in the corner.
		if (o && run.help && run.help.length > 0) {
			const shooter = run.roles[o.shooter]!;
			// (Once the step that gets him there is laid out, from where it
			// has him - not where the set would have had him.)
			const final = k === (o.branch.length > 0 ? last + 1 : last);
			const S =
				o.at === "rim"
					? this.nearRim(team, this.posOf(shooter))
					: final
						? this.posOf(shooter)
						: this.at(run, o.at);
			const u = unitVec(S, rim);
			const H = clampPt({ x: S.x + u.x * 2.4, y: S.y + u.y * 2.4 });
			for (const d of run.help) {
				const cur = targets.get(d);
				const man = manOf.get(d);
				// Up on the ball, he stays on it till the step it goes to the
				// shot in.
				if (cur && d === onBall && man === holder && k < o.after && !final) {
					continue;
				}
				late.delete(d);
				if (cur && man !== undefined) {
					const n = dist(cur, H);
					const s = Math.min(n * 0.3, 4);
					if (n > 0.1) {
						targets.set(
							d,
							clampPt({
								x: cur.x + ((H.x - cur.x) * s) / n,
								y: cur.y + ((H.y - cur.y) * s) / n,
							}),
						);
					}
					continue;
				}
				special(d, H);
			}
		}
		for (const [d, P] of targets) {
			const v = via.get(d);
			if (v) {
				this.shadow(d, v.at, t0 + 80, v.by, team);
			}
			const st = stunts.get(d);
			if (st && manOf.get(d) !== undefined) {
				// With his man; at the ball as it goes by; back on him.
				this.shadow(d, P, t0 + 120, st.from, team, manOf.get(d));
				this.goBy(
					d,
					st.at,
					st.from,
					st.peak,
					"slide",
					-attackDir(team) as 1 | -1,
				);
				this.shadow(d, P, st.peak + 60, t1 + lag, team, manOf.get(d));
				continue;
			}
			if (tag?.d === d && manOf.get(d) !== undefined) {
				// With his man; in the roller's path; back out to his man.
				this.shadow(d, P, t0 + 120, tag.from, team, manOf.get(d));
				const n = dist(this.posOf(d), tag.at);
				this.goBy(
					d,
					tag.at,
					tag.from,
					tag.peak,
					n > 9 ? "run" : "slide",
					n > 9 ? undefined : (-attackDir(team) as 1 | -1),
				);
				this.shadow(d, P, tag.peak + 120, t1 + lag + 250, team, manOf.get(d));
				continue;
			}
			if (help?.d === d) {
				// With his man, until he leaves him to help.
				this.shadow(d, P, t0 + 120, help.from, team, manOf.get(d));
				const n = dist(this.posOf(d), help.at);
				this.goBy(
					d,
					help.at,
					help.from,
					help.by,
					n > 9 ? "run" : "slide",
					n > 9 ? undefined : (-attackDir(team) as 1 | -1),
				);
				continue;
			}
			// (Only if he can be up on him there before he goes: still getting
			// back down the floor, he would be sent to where the drive started
			// long after it had gone.)
			const upOn =
				drive && this.defensePoint(team, drive.from, drive.from, true);
			if (
				d === onBall &&
				drive &&
				upOn &&
				goes !== undefined &&
				goes - Math.max(t0, this.free.get(d) ?? 0) > 400 &&
				Math.max(t0 + 120, this.free.get(d) ?? 0) +
					runMs(dist(this.posOf(d), upOn), RUN) <
					goes
			) {
				// Up on him where he waits with it, then with him as he goes.
				this.shadow(d, upOn, t0 + 120, goes, team, manOf.get(d));
				this.shadow(
					d,
					P,
					goes,
					t1 + lag + (late.get(d) ?? 0),
					team,
					manOf.get(d),
				);
				continue;
			}
			const ch = chase.get(d);
			if (ch && ch.from - Math.max(t0, this.free.get(d) ?? 0) > 400) {
				if (ch.from >= t1) {
					// His man only goes once the step is done: on him where he
					// is, all of it - the chase is the next step's.
					this.shadow(d, ch.at, t0 + 120, t1 + lag, team, ch.man);
				} else {
					this.shadow(d, ch.at, t0 + 120, ch.from, team, ch.man);
					this.shadow(d, P, ch.from, t1 + lag + (late.get(d) ?? 0), team);
				}
				continue;
			}
			this.shadow(
				d,
				P,
				t0 + 120,
				t1 + lag + (late.get(d) ?? 0),
				team,
				manOf.get(d),
			);
		}
	}

	private clockGap(e: RawEvent): number | undefined {
		if (typeof e.clock !== "number" || this.lastClock === undefined) {
			return undefined;
		}
		const g = this.lastClock - e.clock;
		return g >= 0 && g <= 30 ? g : undefined;
	}

	// ---- shots ----------------------------------------------------------------

	private shotSpot(team: Side, zone: Zone, heaveSecs?: number): Pt {
		if (heaveSecs !== undefined) {
			const room = Math.min(1, Math.max(0, heaveSecs / HEAVE_MAX_SECONDS));
			return spot(
				team,
				this.rand(46 - 19 * room, 56 - 22 * room),
				25 + this.rand(-10, 10),
			);
		}
		// A three: far enough out that his toes are behind the line too - in
		// the corner, the line runs 22 feet out along the sideline.
		if (zone === "three" && this.rng() < 0.28) {
			return spot(
				team,
				this.rand(2.5, 9),
				this.rng() < 0.5 ? this.rand(1.6, 2.1) : this.rand(47.9, 48.4),
			);
		}
		const [r0, r1, th0, th1] =
			zone === "atRim" || zone === "tipIn" || zone === "putBack"
				? [2.4, 4.6, 30, 150]
				: zone === "lowPost"
					? [4.5, 9.5, 30, 150]
					: zone === "midRange"
						? [11, 19, 20, 160]
						: [25.4, 27.2, 32, 148];
		const r = this.rand(r0, r1);
		const th = (this.rand(th0, th1) * Math.PI) / 180;
		const depth = Math.max(1.5, 5.25 + r * Math.sin(th));
		return spot(
			team,
			Math.min(44, depth),
			Math.min(47, Math.max(3, 25 + r * Math.cos(th))),
		);
	}

	// Where he shoots from, given where he is: the spot the shot calls for
	// nearest him - round the rim from where he stands, a step or so either
	// way, in from the line for a pull-up, out to it for a three - not
	// anywhere at all on the floor, for him to go across the whole of it to
	// get there.
	private shotSpotNear(team: Side, zone: Zone, from: Pt): Pt {
		const depth = team === 0 ? from.x : COURT_W - from.x;
		const dx = depth - RIM_INSET;
		const dy = from.y - COURT_H / 2;
		// In the corner, a three is a corner three: down the line where he is.
		if (zone === "three" && depth < 14 && Math.abs(dy) > 17) {
			return spot(
				team,
				Math.min(9, Math.max(2.5, depth + this.rand(-1.5, 1.5))),
				dy > 0 ? this.rand(47.9, 48.4) : this.rand(1.6, 2.1),
			);
		}
		const [r0, r1, th0, th1] =
			zone === "atRim" || zone === "tipIn" || zone === "putBack"
				? [2.4, 4.6, 30, 150]
				: zone === "lowPost"
					? [4.5, 9.5, 30, 150]
					: zone === "midRange"
						? [11, 19, 20, 160]
						: [25.4, 27.2, 32, 148];
		const rs = Math.hypot(dx, dy);
		// His angle round the rim (90: straight out; 0 and 180, along the
		// baseline) - right under it, either side.
		// (Under the rim, or behind the line of it, he is on the baseline on
		// his side.)
		const at =
			rs > 2
				? (Math.atan2(Math.max(dx, 0.3), dy) * 180) / Math.PI
				: dy >= 0
					? this.rand(40, 80)
					: this.rand(100, 140);
		const th =
			(Math.min(th1, Math.max(th0, at + this.rand(-12, 12))) * Math.PI) / 180;
		const r = Math.min(r1, Math.max(r0, rs + this.rand(-1.5, 1.5)));
		return spot(
			team,
			Math.min(44, Math.max(1.5, RIM_INSET + r * Math.sin(th))),
			Math.min(47, Math.max(3, COURT_H / 2 + r * Math.cos(th))),
		);
	}

	// Off the dribble to his spot, with a move on the way.
	// A spin: going at his man on the side the ball is on, he is cut off -
	// so he plants, turns his back on him and spins away round him the
	// other way, the ball pulled round with him in the hand it was in, and
	// is by him to go on off the other one. His man, turned, chases him from
	// behind. Returns when he is round.
	private spinBy(
		pid: number,
		guard: number,
		P: Pt,
		t: number,
		dir: 1 | -1,
		goHand: Hand,
	): number {
		const from = this.posOf(pid);
		const G = this.posOf(guard);
		const u = unitVec(from, P);
		const start: Hand = goHand === "R" ? "L" : "R";
		// (The side the ball starts on - his right, or his left.)
		const k = start === "R" ? 1 : -1;
		const side = { x: -u.y * k, y: u.x * k };
		const along = (G.x - from.x) * u.x + (G.y - from.y) * u.y;
		const t0 = this.hold(pid, t, "dribble", start);
		const A = inPlay({
			x: from.x + u.x * (along - SPIN_SHORT) + side.x * 1.4,
			y: from.y + u.y * (along - SPIN_SHORT) + side.y * 1.4,
		});
		// (There as the ball comes up into his hand: he takes it out of his
		// dribble as he plants, not a bounce before.)
		const beat = this.dribbleFrom(pid, t0) ?? t0;
		const top =
			beat +
			Math.max(
				1,
				Math.ceil((t0 + runMs(dist(from, A), DRIBBLE) - beat) / DRIBBLE_MS),
			) *
				DRIBBLE_MS;
		const ta = this.goBy(pid, A, t0, top, "dribble", dir);
		// His man slides over to cut him off.
		const cut = inPlay({ x: G.x + side.x * 1.2, y: G.y + side.y * 1.2 });
		this.go(
			guard,
			cut,
			Math.max(t + 150, this.free.get(guard) ?? 0),
			9,
			"slide",
			-dir as 1 | -1,
		);
		const tg = Math.max(ta, this.hold(pid, Math.max(ta, top), "hold"));
		// Round him, not through him: out to his side, then on by him.
		const by = (a: number, b: number) =>
			inPlay({
				x: cut.x + u.x * a - side.x * b,
				y: cut.y + u.y * a - side.y * b,
			});
		const B = by(0.6, 2.6);
		const tm = this.go(pid, by(-1.8, 2.2), tg, SPIN_FTPS, "run", dir, {
			onto: SPIN_FTPS,
		});
		const tb = this.go(pid, B, tm, SPIN_FTPS, "run", dir, {
			onto: SPIN_FTPS,
		});
		this.act(pid, "spinMove", tg, tb, {
			face: dir,
			look: { ...P },
			...(start === "L" ? { mirror: true as const } : {}),
		});
		this.shadow(
			guard,
			inPlay({ x: P.x - u.x * 3, y: P.y - u.y * 3 }),
			tb + 120,
			tb + runMs(dist(B, P), DRIBBLE) + 320,
			this.teamOf(pid),
			pid,
		);
		return tb;
	}

	// The draws a move off the dribble to get by his man makes (see
	// driveTo), made and thrown away: the move not made, every draw after it
	// as it was.
	private skipCrossover(pid: number, P: Pt, t: number) {
		if (this.spinsBy(pid, P, t)) {
			return;
		}
		const n = crossCount(dist(this.posOf(pid), P) || 1, this.rng());
		for (let i = 0; i < n; i++) {
			this.crossMove(0.25, 0.2);
		}
	}

	// Whether, setting off for P at t with his man square in front of him,
	// he spins away round him rather than crossing over (see driveTo).
	private spinsBy(pid: number, P: Pt, t: number): boolean {
		const from = this.posOf(pid);
		const dx = P.x - from.x;
		const dy = P.y - from.y;
		const d = Math.hypot(dx, dy) || 1;
		const cutOff = this.defenderOf(pid);
		if (cutOff === undefined) {
			return false;
		}
		const C = this.posOf(cutOff);
		const ahead = ((C.x - from.x) * dx + (C.y - from.y) * dy) / d;
		return (
			d >= SPIN_FROM_FT &&
			ahead > 3.5 &&
			ahead < d - 3 &&
			Math.abs((C.x - from.x) * -dy + (C.y - from.y) * dx) / d < 3 &&
			hash01(pid, t + 5) < SPIN_SHARE
		);
	}

	private driveTo(
		pid: number,
		P: Pt,
		t: number,
		dir: 1 | -1,
		style: ShotStyle,
	): number {
		const from = this.posOf(pid);
		const dx = P.x - from.x;
		const dy = P.y - from.y;
		const d = Math.hypot(dx, dy) || 1;
		// Across his path.
		const ax = -dy / d;
		const ay = dx / d;
		const goHand = this.handFor(from, P, dir);
		// His man square in front of him, now and then he spins away round
		// him rather than crossing over.
		const cutOff = this.defenderOf(pid);
		if (
			style === "crossover" &&
			cutOff !== undefined &&
			this.spinsBy(pid, P, t)
		) {
			t = this.spinBy(pid, cutOff, P, t, dir, goHand);
		} else if (style === "crossover") {
			// A hesitation and a jab one way, then a quick move or a few -
			// across in front of him, between his legs, behind his back - that
			// end in the hand on the side he goes, and the drive.
			// (Close in already, just the one.)
			const n = crossCount(d, this.rng());
			const start: Hand = n % 2 ? (goHand === "R" ? "L" : "R") : goHand;
			this.hold(pid, t, "dribble", start);
			// His right is toward the camera when he faces the right rim.
			const jab = (start === "R" ? 1 : -1) * dir * 1.6;
			const tj = this.go(
				pid,
				inPlay({ x: from.x, y: from.y + jab }),
				t + 80,
				6,
				"dribble",
				dir,
			);
			// His man, up on him, bites on it - a step the way it went - and
			// is a step behind when it comes back across.
			const guard = this.defenderOf(pid);
			const G = guard === undefined ? undefined : this.posOf(guard);
			const bit =
				guard !== undefined &&
				G !== undefined &&
				dist(G, from) < 6.5 &&
				dist(from, P) >= 8 &&
				hash01(pid, t) < 0.7;
			if (bit) {
				this.go(
					guard,
					inPlay({ x: G.x, y: G.y + Math.sign(jab) * 2.4 }),
					Math.max(t + 220, this.free.get(guard) ?? 0),
					9,
					"slide",
					-dir as 1 | -1,
				);
			}
			// Across as the ball comes up into his hand: on the next beat of
			// his dribble.
			const top = this.dribbleTop(pid, tj) ?? tj;
			t = this.moveRun(
				pid,
				top < tj - 1 ? top + DRIBBLE_MS : top,
				n,
				0.25,
				0.2,
				start,
			);
			if (bit) {
				// Now and then he is crossed up so badly his feet go out from
				// under him - and he is that much later after it.
				let late = 80;
				if (hash01(guard, t) < 0.22) {
					this.act(guard, "stumble", t - 60, t + 640, {
						face: -dir as 1 | -1,
						look: this.posOf(pid),
					});
					this.free.set(guard, Math.max(this.free.get(guard) ?? 0, t + 640));
					late = 640;
				}
				// Then turned, chasing him to the rim from behind.
				const u = unitVec(from, P);
				this.shadow(
					guard,
					inPlay({
						x: P.x - u.x * (late > 80 ? 4.5 : 2.6),
						y: P.y - u.y * (late > 80 ? 4.5 : 2.6),
					}),
					t + late,
					t + runMs(dist(from, P), DRIBBLE) + 200 + late,
					this.teamOf(pid),
					pid,
				);
			}
		}
		this.hold(pid, t, "dribble", goHand);
		if (style === "euro") {
			// One way, then the other, then up.
			const side = this.rng() < 0.5 ? 1 : -1;
			const k = Math.max(0, d - 6);
			const a = inPlay({
				x: from.x + (dx / d) * k + ax * side * 2,
				y: from.y + (dy / d) * k + ay * side * 2,
			});
			t = this.go(pid, a, t, DRIBBLE, "dribble", dir);
			t = Math.max(t, this.hold(pid, t, "hold"));
			const picked = t;
			const b = inPlay({
				x: P.x - (dx / d) * 2.2 - ax * side * 1.6,
				y: P.y - (dy / d) * 2.2 - ay * side * 1.6,
			});
			t = this.go(pid, b, t, RUN * 0.8, "run", dir);
			const there = this.go(pid, P, t, RUN * 0.8, "run", dir);
			this.act(pid, "euroStep", picked, there, { face: dir });
			return there;
		}
		if (style === "stepBack") {
			// Often a move first - between his legs, a crossover - then into
			// his man, and a hop back to where he shoots from.
			if (this.rng() < 0.5) {
				this.hold(pid, t, "dribble");
				const top = this.dribbleTop(pid, t + 1) ?? t;
				t = this.moveRun(
					pid,
					top < t ? top + DRIBBLE_MS : top,
					this.rng() < 0.6 ? 1 : 2,
					0.4,
					0.15,
				);
				this.hold(pid, t, "dribble");
			}
			// (On his spot already, into his man is at the rim.)
			const u =
				Math.hypot(dx, dy) > 1
					? { x: dx / d, y: dy / d }
					: unitVec(P, rimPt(this.teamOf(pid)));
			const inside = inPlay({ x: P.x + u.x * 2.6, y: P.y + u.y * 2.6 });
			t = this.go(pid, inside, t, DRIBBLE, "dribble", dir);
			t = Math.max(t, this.hold(pid, t, "hold"));
			const there = this.go(pid, P, t + 40, 9, "back", dir);
			this.act(pid, "stepBack", t, there, {
				face: dir,
				jump: [0.25, 0.85, 0.7],
			});
			return there;
		}
		// Out from the rim to where he shoots from - an offensive board taken
		// back out to the line, say - he backs out facing it: a retreat
		// dribble, not a drive's pace backwards.
		const rim = rimPt(this.teamOf(pid));
		const away = dist(P, rim) - dist(from, rim) > 3;
		t = this.go(
			pid,
			P,
			t,
			away ? DRIBBLE_SPEED.retreat! + 2 : DRIBBLE,
			"dribble",
			dir,
		);
		if (style === "post") {
			t = this.backDown(pid, t, dir);
		}
		return t;
	}

	// The finish off a post-up that takes him round or under his man: a drop
	// step - a pivot on the foot nearer the baseline and one long step round
	// him, the ball swung through, to the rim - or a shot fake, his man up off
	// his feet for it, and the step through under him. Returns when he is
	// gathered at the rim to go up.
	private postFinish(
		pid: number,
		team: Side,
		t: number,
		move: "dropStep" | "upUnder",
	): number {
		const rim = rimPt(team);
		const P = this.posOf(pid);
		const guard = this.defenderOf(pid);
		const G = guard !== undefined ? this.posOf(guard) : undefined;
		const toRim = unitVec(P, rim);
		// Round him on the baseline side: the side away from the middle.
		const base = P.y >= 25 ? 1 : -1;
		const side = { x: -toRim.y, y: toRim.x };
		const s = side.y * base >= 0 ? 1 : -1;
		// (Gathered a stride out from the rim - a post finish, not one
		// from under it.)
		const off = {
			x: -toRim.x * 2.9 + side.x * s * 1.6,
			y: -toRim.y * 2.9 + side.y * s * 1.6,
		};
		// (Still in front of the rim, not round past it on the baseline -
		// going up from behind the glass.)
		const dir = attackDir(team);
		const r = Math.hypot(off.x, off.y);
		if (r > 0.01 && (-dir * off.x) / r < Math.cos(DUNK_SIDE)) {
			off.x = -dir * r * Math.cos(DUNK_SIDE);
			off.y = (off.y < 0 ? -1 : 1) * r * Math.sin(DUNK_SIDE);
		}
		const to = clampPt({ x: rim.x + off.x, y: rim.y + off.y });
		const face = (rim.x >= P.x ? 1 : -1) as 1 | -1;
		const start = Math.max(t, this.free.get(pid) ?? 0);
		if (move === "upUnder") {
			// Up as if to shoot - and his man goes with it.
			this.act(pid, "shotFake", start, start + 640, { face, look: rim });
			if (guard !== undefined && G && dist(G, P) < 6) {
				this.act(guard, "contest", start + 140, start + 760, {
					look: { x: P.x, y: P.y },
					jump: [0.2, 0.8, 1.4],
				});
			}
			t = start + 640;
		} else {
			// The pivot, the ball swung through low and away from him.
			this.act(pid, "jab", start, start + 300, { face, look: rim });
			t = start + 260;
		}
		// The long step through, round or under him, to the rim.
		this.hold(pid, t, "hold");
		return this.carry(pid, to, t, 380, "run", face);
	}

	// THE HEAVE AT THE BUZZER.
	//
	// A second or two on the clock and the ball at the wrong end: whoever has
	// it gets it to him, and he races up the floor with it, flat out, and lets
	// it go from wherever he has got to as time runs out - from half court,
	// or from well back of it. His man chases him. Returns when he is up to
	// let it go.
	private raceToHeave(
		team: Side,
		shooter: number,
		t: number,
		gap: number | undefined,
	): number {
		const t0 = t;
		const rim = rimPt(team);
		const dir = attackDir(team);
		t = this.develop(team, t, gap, undefined, undefined, true).t;
		const h = this.holder;
		if (h === undefined) {
			t = this.pickUp(shooter, t, SPRINT) + 150;
		} else if (h !== shooter) {
			// Run on ahead of it inside the arc, he comes back out to meet it -
			// round it from the corner, not out over the sideline.
			const S0 = this.posOf(shooter);
			if (dist(S0, rim) < 29) {
				const out = unitVec(rim, S0);
				const lim = COURT_H / 2 - SIDELINE_GAP - 1;
				const dy = Math.max(-lim, Math.min(lim, out.y * 31));
				this.goBy(
					shooter,
					clampPt({
						x: rim.x - dir * Math.sqrt(31 * 31 - dy * dy),
						y: rim.y + dy,
					}),
					t,
					t + 900,
					"run",
					dir,
				);
			}
			// Not stood there with it while he gets out ahead: up the floor
			// with it meanwhile, behind him, and it goes ahead when he is due.
			const H = this.posOf(h);
			const wait =
				(this.free.get(shooter) ?? t) -
				Math.max(t, this.free.get(h) ?? 0) -
				900;
			const ahead = (this.posOf(shooter).x - H.x) * dir - 14;
			const push = Math.min(ahead, (HEAVE_RUN * 0.8 * wait) / 1000);
			if (push > 4) {
				this.hold(h, Math.max(t, this.free.get(h) ?? 0), "dribble");
				this.go(
					h,
					clampPt({ x: H.x + dir * push, y: H.y + (COURT_H / 2 - H.y) * 0.3 }),
					t,
					HEAVE_RUN * 0.8,
					"dribble",
					dir,
				);
			}
			t = this.passTo(h, shooter, t);
		}
		// The rest up the floor after it - out from under the far basket,
		// not stood back there as it goes up - in a lane of their own, wide
		// of the man who brought it up.
		const by = h !== undefined && h !== shooter ? h : undefined;
		for (const pid of this.slots(team)) {
			const Q = this.posOf(pid);
			if (pid === shooter || (Q.x - COURT_W / 2) * dir > -4) {
				continue;
			}
			const B = by !== undefined && pid !== by ? this.posOf(by) : undefined;
			const y =
				B && Math.abs(Q.y - B.y) < 10 ? B.y + (Q.y < B.y ? -12 : 12) : Q.y;
			this.go(
				pid,
				clampPt({
					x: COURT_W / 2 + dir * this.rand(2, 8),
					y: Math.min(COURT_H - 5, Math.max(5, y)),
				}),
				t0,
				RUN,
				"run",
				dir,
			);
		}
		// What is left of the sim's time for the trip, on the run - never
		// inside the arc.
		const left = Math.max(0.5, (gap ?? 2) - (t - t0) / 1000);
		const S = this.posOf(shooter);
		const room = dist(S, rim) - 28;
		const run = Math.max(0, Math.min(room, left * HEAVE_RUN));
		if (run > 2) {
			const u = unitVec(S, rim);
			const P = clampPt({ x: S.x + u.x * run, y: S.y + u.y * run });
			this.hold(shooter, t, "dribble");
			const there = this.go(shooter, P, t, HEAVE_RUN, "dribble", dir);
			// His man after him, a step behind.
			const d = this.defenderOf(shooter);
			if (d !== undefined) {
				const back = unitVec(rim, P);
				this.goBy(
					d,
					clampPt({ x: P.x + back.x * 4, y: P.y + back.y * 4 }),
					t + 150,
					there,
					"run",
				);
			}
			t = there;
		}
		return Math.max(t, this.hold(shooter, t, "hold"));
	}

	// Back to the rim, a dribble or two to back his man down: in at the rim
	// - not on along the baseline, round behind the glass - and no nearer
	// it than a step off the block.
	private backDown(pid: number, t: number, dir: 1 | -1): number {
		const at = this.posOf(pid);
		const rim = rimPt(this.teamOf(pid));
		const aim = { x: rim.x - dir, y: rim.y };
		const u = unitVec(at, aim);
		const k = Math.min(2.2, Math.max(0, dist(at, aim) - 4.5));
		const to = clampPt({ x: at.x + u.x * k, y: at.y + u.y * k });
		this.hold(pid, t, "dribble");
		const done = this.go(pid, to, t + 60, 2.6, "post", -dir as 1 | -1);
		// His man gives ground with him - if he is on him, not off somewhere
		// else (gone to meet the shot, say).
		const guard = this.defenderOf(pid);
		if (guard !== undefined && dist(this.posOf(guard), at) < 5) {
			this.go(
				guard,
				clampPt({ x: to.x + u.x * BODY, y: to.y + u.y * BODY }),
				t + 60,
				2.6,
				"back",
				-dir as 1 | -1,
			);
		}
		return Math.max(done, this.hold(pid, done, "hold"));
	}

	// The lob's set-up: the inbound, Y out of bounds with the ball near the
	// frontcourt, X on the far wing - and X breaks for the rim.
	private setUpLob(
		team: Side,
		shooter: number,
		lobber: number,
		t: number,
	): number {
		this.setOffense(t, team);
		const dir = attackDir(team);
		const rim = rimPt(team);
		const off = this.slots(team);
		const def = this.slots(other(team));
		const spots = this.setSpots(team, 0);
		const target = (pid: number, j: number): Pt =>
			pid === lobber
				? { x: rim.x - dir * 21, y: -1.4 }
				: pid === shooter
					? clampPt({ x: rim.x - dir * 17, y: 41 })
					: (spots[j] ?? spots[0]!);
		const ready = this.walkTo(
			[
				...off.map((pid, j) => ({ pid, at: target(pid, j), face: dir })),
				...def.map((pid, j) => ({
					pid,
					at: guardSpot(team, target(off[j] ?? off[0]!, j), 0.25),
					face: -dir as 1 | -1,
					man: off[j] ?? off[0]!,
				})),
			],
			t,
		);
		const has = this.ballTo(
			lobber,
			Math.max(t + 300, (this.free.get(lobber) ?? t) - 1200),
		);
		const set = Math.max(ready, has);
		this.hold(lobber, Math.max(has, this.free.get(lobber) ?? 0), "hold");
		this.lookAt(lobber, set + 1, { x: rim.x, y: rim.y });
		this.hurry(t + 300, set - 200);
		this.motionTeam = team;
		this.motion = 0;
		return this.go(
			shooter,
			clampPt({ x: rim.x - dir * 4.2, y: 25 + 2.4 }),
			set + 650,
			SPRINT,
			"run",
			dir,
		);
	}

	// His man: position against position, unless a switch changed it.
	// His man: whoever a switch put on him, or else his own number's man
	// position for position - one man each, every defender on somebody.
	// Their board: the defense that went to the glass for it gets back out
	// of there to its men - not left standing under the rim while the ball
	// is kicked back out.
	// Who he kicks it out to from in under the rim: the nearest of his
	// teammates out on the perimeter.
	private kickOutTo(team: Side, pid: number): number | undefined {
		const rim = rimPt(team);
		const P = this.posOf(pid);
		return this.slots(team)
			.filter((m) => m !== pid && dist(this.posOf(m), rim) > 19)
			.sort((a, b) => dist(this.posOf(a), P) - dist(this.posOf(b), P))[0];
	}

	// Off an offensive board the rest don't stand round the rim watching
	// him. Whoever went in after it and isn't a big gets back out to an open
	// spot on the arc, for the kick-out; a big right on top of him steps off
	// to the dunker spot or the short corner, there for the dump-off. (Their
	// men go with them - see findMen.)
	// (So too the man who gets the next shot in there - the dump-off, a
	// post-up: he stays in, not out to the arc and straight back in for it.)
	private spreadOut(team: Side, rebounder: number, t: number, inside?: number) {
		const rim = { x: rimX(team), y: COURT_H / 2 };
		const ball = this.posOf(rebounder);
		// Nearest the rim first: the most crowded in, the first out.
		const men = this.slots(team)
			.filter((p) => p !== rebounder)
			.sort((a, b) => dist(this.posOf(a), rim) - dist(this.posOf(b), rim));
		men.forEach((pid, j) => {
			const P = this.posOf(pid);
			const big = (this.rank.get(pid) ?? 4) >= 6 || pid === inside;
			if (big ? dist(P, ball) > 8 : dist(P, rim) > 17) {
				return;
			}
			const others = this.slots(team)
				.filter((x) => x !== pid)
				.map((x) => this.posOf(x));
			let best: Pt | undefined;
			let score = -Infinity;
			// (The man with the shot coming: in at the dunker spot or the
			// block - not out in the short corner.)
			for (const name of pid === inside
				? ["L_dunker", "R_dunker", "L_block", "R_block"]
				: big
					? ["L_dunker", "R_dunker", "L_short_corner", "R_short_corner"]
					: RESPACE_SPOTS) {
				const S = this.spotFor(team, 1, name);
				const room = Math.min(...others.map((m) => dist(m, S)));
				const sc = Math.min(room, 16) - dist(P, S) * 0.8;
				if (room >= (big ? 7 : 10) && sc > score) {
					score = sc;
					best = S;
				}
			}
			if (!best) {
				return;
			}
			const there = this.go(
				pid,
				best,
				Math.max(t + 200 + j * 150, this.free.get(pid) ?? 0),
				JOG + 3,
				"run",
			);
			this.lookAt(pid, there, ball);
		});
	}

	private findMen(team: Side, t: number) {
		const ball = this.ballPoint();
		this.slots(team).forEach((man, j) => {
			const d = this.defenderOf(man);
			if (d === undefined) {
				return;
			}
			const P = this.defensePoint(
				team,
				this.posOf(man),
				ball,
				man === this.holder,
			);
			this.shadow(d, P, t + 200 + j * 70, t + 1300 + j * 70, team, man);
		});
	}

	private defenderOf(pid: number): number | undefined {
		const team = this.teamOf(pid);
		const men = this.slots(team);
		const def = this.slots(other(team));
		const taken = new Set<number>();
		const mine = new Map<number, number>();
		for (const m of men) {
			const g = this.guarding.get(m);
			if (g !== undefined && def.includes(g) && !taken.has(g)) {
				mine.set(m, g);
				taken.add(g);
			}
		}
		men.forEach((m, j) => {
			if (!mine.has(m)) {
				const d = [def[j], ...def].find(
					(x) => x !== undefined && !taken.has(x),
				);
				if (d !== undefined) {
					mine.set(m, d);
					taken.add(d);
				}
			}
		});
		return mine.get(pid) ?? def[0];
	}

	// A set run to its shot: the steps it takes for the option to open, the
	// read's own moves, the last pass. Returns when the shooter is set to go
	// up, how he gets it off, and - for an alley-oop - who throws the lob.
	private playToShot(
		run: Running,
		t: number,
		plan: ShotPlan,
	): { t: number; style: ShotStyle; lob?: number; move?: PostMove } {
		const o = run.option!;
		const { play, team } = run;
		const dir = attackDir(team);
		const last = Math.min(o.after, play.steps.length - 1);
		for (let k = run.from; k <= last; k++) {
			const next =
				k < last
					? play.steps[k + 1]
					: o.branch.length > 0
						? o.branch
						: undefined;
			t = this.runStep(run, play.steps[k]!, t, next, k);
		}
		if (o.branch.length > 0) {
			t = this.runStep(run, o.branch, t, undefined, last + 1);
		}
		const shooter = run.roles[o.shooter]!;
		const passer = o.assist === undefined ? undefined : run.roles[o.assist];
		const rim = { x: rimX(team), y: COURT_H / 2 };
		// With it in his own hands at the end of it - pulling up off the
		// screen, say - he goes up as he gets there: he doesn't dribble in
		// place while the others finish what the step had them doing.
		// (Never before the screen he is coming off is there to come off:
		// behind it, he pulls up as his man goes under it.)
		// (Caught on the move and not a dribble since, it is a catch-and-shoot:
		// he goes up off the catch, however long the rest of the step has the
		// others doing - not stood there with it for two seconds first.)
		if (this.holder === shooter) {
			const mine = this.hasItFrom(shooter);
			if (mine < t - 150) {
				const screen =
					run.screen?.user === shooter
						? (run.screen.handed ?? run.screen.set + SCREEN_USE)
						: 0;
				let k = this.ball.length - 1;
				for (; k >= 0; k--) {
					const s = this.ball[k]!;
					if (s.kind !== "hold" || s.pid !== shooter || s.style !== "hold") {
						break;
					}
				}
				const pass = this.ball[k];
				const caught =
					k < this.ball.length - 1 &&
					pass?.kind === "fly" &&
					"pid" in pass.to &&
					pass.to.pid === shooter;
				const tr = this.track(shooter);
				const set = caught
					? Math.max(
							pass.t1 + 200,
							tr?.moves.at(-1)?.t1 ?? 0,
							tr?.acts.at(-1)?.t1 ?? 0,
						)
					: mine + 80;
				t = Math.min(t, Math.max(set, screen, t - (caught ? 4000 : 1500)));
			}
		}
		const dunk =
			o.zone === "rim" && (plan.finish === "dunk" || plan.finish === "poster");
		let stride = false;
		if (this.holder !== shooter) {
			const from = this.holder ?? passer ?? shooter;
			if (
				dunk &&
				from !== shooter &&
				(o.kind === "alley_oop" || o.pass === "lob")
			) {
				// An alley-oop: he heads for the rim and the lob meets him up there
				// - on his way in, not stood there for it (see leadTheLob).
				const led =
					this.holder === from
						? this.leadTheLob(run, shooter, from, t)
						: undefined;
				if (led !== undefined) {
					return { t: led, style: "plain", lob: from };
				}
				const P = this.nearRim(team, this.posOf(shooter), 3.4);
				t = this.go(
					shooter,
					P,
					t,
					SPRINT,
					"run",
					(rim.x >= P.x ? 1 : -1) as 1 | -1,
				);
				return { t, style: "plain", lob: from };
			}
			if (from !== shooter) {
				// Into the post: he seals his man for it first. (Only a man the
				// set has posting up: a roller or a cutter into the lane is hit
				// on his way there, from wherever the ball is - the pocket pass,
				// the dump-off off the drive - not stood there while the man
				// with it backs out to the wing to throw it in to him. The
				// draws are made all the same.)
				const S = this.posOf(shooter);
				const posting = o.zone === "post" && this.postsUp(run);
				const fed =
					o.zone === "post" &&
					dist(S, rim) > 4 &&
					dist(S, rim) < 15 &&
					this.rng() < 0.85;
				const entry = fed && posting;
				if (fed && !entry) {
					this.rng();
				}
				// Fed from up the floor - not handed it from a step away by a
				// man down in the lane with him (see entryFrom).
				const F = this.posOf(from);
				if (
					entry &&
					this.holder === from &&
					(dist(F, S) < 11 || dist(F, rim) < 11)
				) {
					this.hold(from, t, "dribble");
					t = this.go(
						from,
						this.entryFrom(team, from, S),
						t,
						DRIBBLE,
						"dribble",
					);
				}
				const held = entry
					? this.seal(
							shooter,
							from,
							Math.max(t, this.free.get(shooter) ?? 0),
							dir,
						)
					: undefined;
				const style = fed
					? this.rng() < 0.6
						? "bounce"
						: "chest"
					: passStyleOf(o.pass, this.rng);
				// To a man cutting to the rim, or rolling into the lane: on his
				// way in (see leadTheCut).
				const led =
					(o.zone === "rim" || (o.zone === "post" && !posting)) &&
					this.holder === from
						? this.leadTheCut(run, shooter, from, t, style)
						: undefined;
				stride = led !== undefined && o.zone === "rim";
				// A three: out behind the line before it comes, and caught
				// there set to shoot - not caught on the line and backed out
				// with it (see shoot).
				const out =
					o.zone === "three" ? behindArc(team, this.posOf(shooter)) : undefined;
				if (out) {
					this.go(
						shooter,
						out,
						Math.max(t - 400, this.free.get(shooter) ?? 0),
						7,
						"back",
						dir,
					);
				}
				t = this.passTo(from, shooter, led ?? t, style);
				held?.(t - 110);
				if (o.pass === "kickout" || o.pass === "skip") {
					this.respace(team, from, shooter);
				}
			}
		}
		const P = this.posOf(shooter);
		const out = dist(P, rim);
		if (o.zone === "rim" && out > 4.5 && !stride) {
			// Caught short of the rim: one hard dribble and up. (Hit on his cut
			// in, he goes up in stride.)
			this.hold(shooter, t, "dribble");
			t = this.go(
				shooter,
				this.nearRim(team, P, 3),
				t,
				RUN * 0.85,
				"dribble",
				dir,
			);
		} else if (o.zone === "post" && out < 3.5) {
			// Caught under the rim: a dribble back out to the block to shoot
			// it from.
			const u =
				out > 0.5
					? unitVec(rim, P)
					: { x: -dir * 0.6, y: P.y >= rim.y ? 0.8 : -0.8 };
			this.hold(shooter, t, "dribble");
			t = this.go(
				shooter,
				clampPt({ x: rim.x + u.x * 6.5, y: rim.y + u.y * 6.5 }),
				t,
				DRIBBLE * 0.5,
				"dribble",
				dir,
			);
		} else if (o.zone === "post" && out > 11) {
			// Caught out on the baseline: a dribble or two in to where a post
			// shot comes from.
			this.hold(shooter, t, "dribble");
			t = this.go(
				shooter,
				this.nearRim(team, P, 9),
				t,
				DRIBBLE * 0.6,
				"dribble",
			);
		} else if (o.zone === "mid" && out < 8) {
			// Too deep for a jumper: a step back out to shoot it.
			const u = unitVec(rim, P);
			t = this.go(
				shooter,
				clampPt({ x: rim.x + u.x * 10, y: rim.y + u.y * 10 }),
				t,
				9,
				"back",
				dir,
			);
		} else if (
			o.kind === "pull_up" &&
			(o.zone === "three" ||
				(o.zone === "mid" && run.screen?.handed !== undefined)) &&
			this.holder === shooter &&
			run.screen?.ball === true &&
			run.screen.user === shooter &&
			run.screen.step === last &&
			(this.track(shooter)?.moves.at(-1)?.t1 ?? -Infinity) < run.screen.set
		) {
			// A pull-up three off a ball screen he has not used yet: off it
			// first - a hard dribble or two past the big's shoulder, the way he
			// turns the corner - and up behind it, his man caught on it. Not
			// stood where he was all along and shooting over it.
			// (Off a handoff he took on the move - for three, or from the
			// elbow - he goes on with it as he has it, the way he was going,
			// past the man who gave it him: not stood with it beside him while
			// he turns to screen, nor up off the catch with the two of them
			// shoulder to shoulder.)
			const sc = run.screen;
			const S = this.posOf(sc.screeners[0]!);
			const ur = unitVec(P, rim);
			const came = this.track(shooter)?.moves.at(-1);
			const way =
				sc.handed !== undefined && came && dist(came.from, came.to) > 1
					? unitVec(came.from, came.to)
					: undefined;
			const side =
				Math.sign(
					way
						? way.x * -ur.y + way.y * ur.x
						: (S.x - P.x) * -ur.y + (S.y - P.y) * ur.x,
				) || (shooter % 2 ? 1 : -1);
			const off = sc.handed !== undefined ? PULL_UP_OFF : TURN_THE_CORNER;
			let Q = clampPt({
				x: P.x - ur.y * side * off + ur.x * 0.8,
				y: P.y + ur.x * side * off + ur.y * 0.8,
			});
			const out = o.zone === "three" ? 24.4 : MID_PULL_UP_IN;
			if (o.zone === "three" ? dist(Q, rim) < out : dist(Q, rim) > out) {
				// (A three still well behind the line; a jumper from the elbow
				// still inside it.)
				const u = unitVec(rim, Q);
				Q = clampPt({ x: rim.x + u.x * out, y: rim.y + u.y * out });
			}
			if (sc.handed !== undefined) {
				t = sc.handed;
				this.hold(shooter, t, "hold");
				this.hold(shooter, t + GATHER_MS, "dribble");
			} else {
				t = Math.max(t, sc.set + SCREEN_USE);
				this.hold(shooter, t, "dribble");
			}
			const off0 = t;
			t = this.go(shooter, Q, t, DRIBBLE * 0.7, "dribble", dir);
			if (sc.handed === undefined) {
				// The man who set it doesn't stand there at his shoulder as he
				// goes up: as the ball comes off it, he rolls to the rim - or
				// pops, if that is what the set has him do next.
				const big = sc.screeners[0]!;
				const role = run.roles.indexOf(big);
				const then = play.steps[last + 1]?.find(
					(a) => a.type === "move" && a.who === role,
				);
				this.playMove(
					run,
					big,
					then?.type === "move" ? then.to : "rim",
					then?.type === "move" ? then.style : "roll",
					off0,
					sc,
				);
			}
		} else if (
			o.kind === "step_back" &&
			!this.track(shooter)?.acts.some(
				(a) => a.anim === "stepBack" && a.t1 > t - 200,
			)
		) {
			// Into his man, then the hop back to where he shoots from (unless
			// the set's last dribble was just that).
			const u = unitVec(P, rim);
			this.hold(shooter, t, "dribble");
			t = this.go(
				shooter,
				clampPt({ x: P.x + u.x * 2.6, y: P.y + u.y * 2.6 }),
				t,
				DRIBBLE * 0.6,
				"dribble",
				dir,
			);
			t = Math.max(t, this.hold(shooter, t, "hold"));
			const hop = t;
			t = this.go(shooter, P, t + 40, 9, "back", dir);
			this.act(shooter, "stepBack", hop, t, {
				face: dir,
				jump: [0.25, 0.85, 0.7],
			});
		}
		t = Math.max(t, this.hold(shooter, t, "hold"));
		if (o.kind === "floater") {
			// Off the drive, still going: up on one foot and floated over the
			// big - not a stop, his back to the rim and a post move. Or hooked
			// in over him (see laneFinish).
			return { t, style: this.laneFinish(shooter, t) };
		}
		if (o.zone === "post" && o.playType === "Postup") {
			// Fed in the post: he backs his man down and goes to work - mostly
			// with the move the set is run for.
			if (dist(this.posOf(shooter), rim) > 5.5) {
				t = this.backDown(shooter, t, dir);
			}
			// (A set run for the hook still ends, now and then, in the
			// turnaround fadeaway over him: spread through the game's post-ups
			// - the first of them, and every third or fourth after - not
			// bunched up, or none at all, by the luck of the draw.)
			const n = this.postUps++;
			const fade =
				Math.floor((n + 1) * POST_FADE + 0.7) > Math.floor(n * POST_FADE + 0.7);
			return {
				t,
				style: "post",
				move: o.kind === "fadeaway" || fade ? "fade" : "hook",
			};
		}
		return {
			t,
			style:
				o.kind === "fadeaway" ? "fade" : o.kind === "hook" ? "hook" : "plain",
		};
	}

	// THE CUT, TIMED TO THE BALL. A man cutting to the rim for the pass is
	// hit on the move: it is in his hands a stride or two out, him still
	// going, and he goes up with it in stride - not thrown in once he has got
	// there and stood under the rim waiting on it, the passer only now up at
	// the arc with it. Off the dribble, if that is when he comes open: the man
	// with it pulls up and hits him - the hit-ahead to the big running the
	// floor, the pocket pass to the roller. Or, the ball not his to throw yet,
	// the cutter holds his cut until it can come to him. Returns when the
	// passer throws it, if it is timed so.
	private leadTheCut(
		run: Running,
		shooter: number,
		passer: number,
		t: number,
		style: PassStyle | undefined,
	): number | undefined {
		// (Into the lane for a hook or a floater: he has it as he gets to his
		// spot there.)
		const lane = run.option?.zone === "post";
		const cut = this.cutIn(run, shooter, lane ? LANE_CUT_TO : RIM_CUT_TO);
		if (!cut) {
			return undefined;
		}
		const rim = { x: rimX(run.team), y: COURT_H / 2 };
		// (Over the top and as long as it might take, the pass not said.)
		const wind =
			RELEASE_MS +
			(style === "chest" || style === "bounce" ? 0 : OVERHEAD_WIND);
		const flight = (d: number) =>
			style === "lob"
				? Math.max(750, passMs(d) * 1.7)
				: passMs(d) * (style === "chest" || style === "overhead" ? 1 : 1.15);
		// Thrown now, it would find him stood there.
		const now = Math.max(t, this.free.get(passer) ?? 0);
		if (
			cut.there + PARKED_MS >=
			now + wind + flight(dist(this.posOf(passer), cut.end))
		) {
			return undefined;
		}
		const lo = this.throwFrom(run, passer, t);
		if (lo === undefined) {
			return undefined;
		}
		// Where he has it: on his way in, as far out as that - a stride or two
		// into his cut, though (a short one, at the end of it).
		const { Q, end, speed } = cut;
		const u = unitVec(Q, end);
		const s = lane
			? Infinity
			: Math.max(CUT_RUN_UP, alongTo(Q, u, rim, CUT_CATCH_OUT));
		const C =
			s < dist(Q, end) - 0.5 ? { x: Q.x + u.x * s, y: Q.y + u.y * s } : end;
		const thrown = (tC: number) => {
			let x = tC - wind - flight(dist(this.posAt(passer, tC), C));
			for (let k = 0; k < 2; k++) {
				x = tC - wind - flight(dist(this.posAt(passer, x), C));
			}
			return x;
		};
		// His run in, laid again from there: on as he was going, the ball
		// thrown to meet him - or, held, timed to it.
		this.cutFrom(shooter, cut);
		let go = this.setOff(shooter, C, cut.tQ, -Infinity, speed, C !== end);
		let at = thrown(go.there);
		if (at < lo) {
			at = lo;
			go = this.setOff(
				shooter,
				C,
				cut.tQ,
				lo + wind + flight(dist(this.posAt(passer, lo), C)),
				speed,
				C !== end,
			);
		}
		this.inStride(shooter, go.start);
		this.go(shooter, C, go.start, speed, "run", undefined, { onto: go.onto });
		this.stopFor(passer, at);
		return at;
	}

	// The same for an alley-oop: his run in timed to go up as he gets there,
	// the lob already on its way to meet him - not there first and stood
	// under the rim, waiting on the passer to get up the floor and throw it.
	// Returns when he gets there, if it is timed so.
	private leadTheLob(
		run: Running,
		shooter: number,
		lobber: number,
		t: number,
	): number | undefined {
		const cut = this.cutIn(run, shooter, RIM_CUT_TO);
		if (!cut || cut.there + PARKED_MS >= t) {
			return undefined;
		}
		const lo = this.throwFrom(run, lobber, t);
		if (lo === undefined) {
			return undefined;
		}
		const team = run.team;
		const rim = rimPt(team);
		const { Q, speed } = cut;
		const P = this.nearRim(team, cut.end, 3.4);
		const face = (rim.x >= P.x ? 1 : -1) as 1 | -1;
		const la = dist(Q, P);
		// When the lob goes up (see stageShot): his last strides gathered a
		// way out, up off them, and it meets him at the top - from where the
		// passer is when he throws it. (With a little to spare: pulled up and
		// the ball up over his head before it goes.)
		const back = Math.min(DUNK_GATHER - dist(P, rim), la * 0.5);
		const gathered = (ms: number) =>
			la >= 4 && dist(Q, rim) - dist(P, rim) >= 2 && back > 0.3
				? ms * timeTo(ms, (la - back) / la)
				: ms;
		const flight = (x: number) =>
			Math.max(700, 380 + dist(this.posAt(lobber, x), rim) * 20);
		const lead = 1300 * 0.4 + 60 - RELEASE_MS - OVERHEAD_WIND - 200;
		this.cutFrom(shooter, cut);
		let go = this.setOff(shooter, P, cut.tQ, -Infinity, speed, false);
		const up = go.there - go.ms + gathered(go.ms) + lead;
		let at = up - flight(up - 900);
		at = up - flight(at);
		if (at < lo) {
			// The ball not his to throw yet: the cut held until it is.
			at = lo;
			const want = lo + flight(lo) - lead;
			go = this.setOff(shooter, P, cut.tQ, want, speed, false);
			for (let k = 0; k < 2; k++) {
				go = this.setOff(
					shooter,
					P,
					cut.tQ,
					want + go.ms - gathered(go.ms),
					speed,
					false,
				);
			}
		}
		this.inStride(shooter, go.start);
		const there = this.go(shooter, P, go.start, speed, "run", face);
		this.stopFor(lobber, at);
		return there;
	}

	// When he sets off from where he is for P - no sooner than `from` - to be
	// there by `by` (or as soon as he can), going for `speed`: from a
	// standstill, or straight on from the run he is on; and if `on`, going on
	// past it at the pace he has got up to there. How long it takes him, when
	// he is there, and that pace.
	private setOff(
		pid: number,
		P: Pt,
		from: number,
		by: number,
		speed: number,
		on: boolean,
	): { start: number; ms: number; there: number; onto: number } {
		const L = dist(this.posOf(pid), P);
		const take = (start: number) => {
			const v0 = this.carried(pid, P, start, speed);
			const onto = on ? Math.min(speed, paceAfter(L, v0)) : 0;
			return { onto, ms: Math.max(240, runMs(L, speed, v0, 1, onto)) };
		};
		let start = from;
		let r = take(start);
		for (let k = 0; k < 3 && from + r.ms < by; k++) {
			start = Math.max(from, by - r.ms);
			r = take(start);
		}
		return { start, ms: r.ms, there: start + r.ms, onto: r.onto };
	}

	// Whether the set has the man it is run for posting up for the ball -
	// sealed on the block or in the lane, his back to the rim - rather than
	// rolling or cutting in for it.
	private postsUp(run: Running): boolean {
		const o = run.option;
		if (!o) {
			return false;
		}
		if (o.playType === "Postup") {
			return true;
		}
		const at = Math.min(o.after, run.play.steps.length - 1);
		for (const acts of [
			o.branch,
			...run.play.steps.slice(0, at + 1).reverse(),
		]) {
			const a = acts.findLast(
				(x) => (x.type === "move" || x.type === "post") && x.who === o.shooter,
			);
			if (a) {
				return a.type === "post" || (a.type === "move" && a.style === "seal");
			}
		}
		return false;
	}

	// His run in to the rim (ending within `reach` of it), if the set has
	// him make one for the ball: the runs he goes straight on through to get
	// there (not a step or two once he is there) - from where he comes
	// within CUT_FROM of it on them, or from where he set off, nearer than
	// that; when he is there, where he ends up and when; and how fast the
	// set has him go.
	private cutIn(
		run: Running,
		pid: number,
		reach: number,
	):
		| {
				k: number;
				Q: Pt;
				tQ: number;
				end: Pt;
				there: number;
				speed: number;
		  }
		| undefined {
		const o = run.option;
		const moves = this.track(pid)?.moves ?? [];
		const rim = { x: rimX(run.team), y: COURT_H / 2 };
		let j = moves.length - 1;
		for (let n = 0; n < 3 && j > 0 && lenOf(moves[j]!) < 2.5; n++) {
			j--;
		}
		const last = moves[j];
		if (!o || !last || last.anim !== "run" || dist(last.to, rim) > reach) {
			return undefined;
		}
		let k = j;
		while (
			k > 0 &&
			moves[k - 1]!.anim === "run" &&
			moves[k - 1]!.t1 >= moves[k]!.t0 - 80
		) {
			k--;
		}
		while (k < j && dist(moves[k]!.to, rim) > CUT_FROM) {
			k++;
		}
		const m = moves[k]!;
		let Q = m.from;
		let tQ = m.t0;
		if (dist(m.from, rim) > CUT_FROM) {
			for (let x = m.t0; x < m.t1; x += 20) {
				const e = 0.5 - 0.5 * Math.cos((Math.PI * (x - m.t0)) / (m.t1 - m.t0));
				const P = {
					x: m.from.x + (m.to.x - m.from.x) * e,
					y: m.from.y + (m.to.y - m.from.y) * e,
				};
				if (dist(P, rim) <= CUT_FROM) {
					Q = P;
					tQ = x;
					break;
				}
			}
		}
		// (In toward the rim - or, into the lane, up from the baseline too: a
		// flash.)
		if (
			dist(Q, last.to) < CUT_RUN_UP ||
			(reach <= RIM_CUT_TO && dist(Q, rim) - dist(last.to, rim) < CUT_RUN_UP)
		) {
			return undefined;
		}
		const at = Math.min(o.after, run.play.steps.length - 1);
		const steps = [o.branch, ...run.play.steps.slice(0, at + 1).reverse()];
		const cut = { k, Q, tQ, end: last.to, there: last.t1 };
		for (const acts of steps) {
			const a = acts.find((x) => x.type === "move" && x.who === o.shooter);
			if (a?.type === "move") {
				return {
					...cut,
					speed:
						run.play.cat === "break" && a.style === "jog"
							? TRAIL_SPEED
							: (MOVE_SPEED[a.style] ?? 13),
				};
			}
		}
		// (Not one of the set's own runs - running his lane on the break, say:
		// at about the pace he was going.)
		let len = 0;
		for (let n = k; n <= j; n++) {
			len += lenOf(moves[n]!);
		}
		const secs = (last.t1 - moves[k]!.t0) / 1000;
		return {
			...cut,
			speed: Math.min(SPRINT, Math.max(12, (1.2 * len) / Math.max(0.3, secs))),
		};
	}

	// His run in taken back to where it starts (see cutIn): there then, and
	// none of it after.
	private cutFrom(pid: number, cut: { k: number; Q: Pt; tQ: number }) {
		const tr = this.track(pid);
		const m = tr?.moves[cut.k];
		if (!tr || !m) {
			return;
		}
		if (cut.tQ > m.t0 + 1) {
			tr.moves.splice(cut.k + 1);
			m.to = { ...cut.Q };
			m.t1 = cut.tQ;
			m.v1 = undefined;
		} else {
			tr.moves.splice(cut.k);
		}
		this.pos.set(pid, { ...cut.Q });
		this.free.set(pid, cut.tQ);
	}

	// The soonest the man with the ball can throw it on in the step the set
	// gets to its shot in: once he has it - caught, his eyes up - and not in
	// the middle of a move with it.
	private throwFrom(run: Running, pid: number, t: number): number | undefined {
		let since: number | undefined;
		for (let k = this.ball.length - 1; k >= 0; k--) {
			const s = this.ball[k]!;
			if (s.kind === "hold" && s.pid === pid) {
				since = s.t0;
				continue;
			}
			if (s.kind === "fly" && "pid" in s.to && s.to.pid === pid) {
				since = s.t1;
			}
			break;
		}
		if (since === undefined) {
			return undefined;
		}
		let lo = Math.max(since + CATCH_TO_THROW, run.stepAt ?? t);
		for (const a of this.track(pid)?.acts ?? []) {
			if (a.anim !== "catch" && a.t0 < t && a.t1 > lo) {
				lo = a.t1;
			}
		}
		return lo;
	}

	// His cut timed to the ball, he sets off on it at `start`: the run that
	// gets him to where it starts takes him there as he does - a man running
	// the floor easing up, a cutter coming slower off his spot - not there
	// first at full tilt and stood at the free-throw line waiting on it.
	// (No slower than IN_STRIDE_PACE: past that, he stands what is left.)
	private inStride(pid: number, start: number) {
		const tr = this.track(pid);
		const moves = tr?.moves ?? [];
		const A = moves.at(-1);
		// (A run of his own: not the picture cutting to him somewhere else.)
		const running = (m: Move) =>
			(m.anim === "run" || m.anim === "sprint" || m.anim === "jog") &&
			m.t1 - m.t0 >= 100 &&
			!this.cuts.some((c) => c >= m.t0 - 1 && c <= start + 1);
		if (
			!tr ||
			!A ||
			!running(A) ||
			A.t1 >= start - PARKED_MS ||
			tr.acts.some((a) => a.t1 > A.t1 && a.t0 < start)
		) {
			return;
		}
		// (The runs that take him there one into the next, the last few: each
		// eased up on, the last first.)
		let k = moves.length - 1;
		while (
			k > 0 &&
			moves.length - k < 3 &&
			running(moves[k - 1]!) &&
			moves[k - 1]!.t1 >= moves[k]!.t0 - 80 &&
			!tr.acts.some((a) => a.t1 > moves[k - 1]!.t0 && a.t0 < moves[k]!.t0)
		) {
			k--;
		}
		let left = start - A.t1;
		const extra = moves.map(() => 0);
		for (let i = moves.length - 1; i >= k && left > 0; i--) {
			const m = moves[i]!;
			const len = dist(m.from, m.to);
			if (len < CUT_RUN_UP) {
				continue;
			}
			const slack = (len / IN_STRIDE_PACE) * 1000 - (m.t1 - m.t0);
			extra[i] = Math.max(0, Math.min(slack, left));
			left -= extra[i]!;
		}
		// Off a screen of his - rolling, slipping out of it - he holds it the
		// rest of that long: set until it is time to go, then gone in one.
		let shift = 0;
		const held = tr.acts.find(
			(a) => a.anim === "screen" && Math.abs(a.t1 - moves[k]!.t0) < 120,
		);
		if (held && left > PARKED_MS) {
			held.t1 += left;
			shift = left;
		}
		for (let i = k; i < moves.length; i++) {
			const m = moves[i]!;
			if (shift === 0 && extra[i] === 0) {
				continue;
			}
			m.t0 += shift;
			shift += extra[i]!;
			m.t1 += shift;
			// (How fast he is going along them, read off them afresh.)
			m.v1 = undefined;
			if (i > k) {
				m.v0 = undefined;
			}
		}
	}

	// Pulled up where he is at t, to throw it: whatever run he was on with
	// it ends there.
	private stopFor(pid: number, t: number) {
		if ((this.track(pid)?.moves.at(-1)?.t1 ?? -Infinity) > t) {
			this.cutShort(pid, t);
		}
	}

	// Kicked out of the lane, the ball gone, the passer does not stand there
	// watching it: he gets back out to the arc, to the open spot nearest him
	// - and his man goes with him.
	private respace(team: Side, pid: number, shooter: number) {
		const rim = { x: rimX(team), y: COURT_H / 2 };
		const P = this.posOf(pid);
		// (A big stays in there, for the rebound.)
		if (dist(P, rim) > 17 || (this.rank.get(pid) ?? 4) >= 6) {
			return;
		}
		const mates = this.slots(team)
			.filter((x) => x !== pid)
			.map((x) => this.posOf(x));
		let best: Pt | undefined;
		let score = -Infinity;
		for (const name of RESPACE_SPOTS) {
			const S = this.spotFor(team, 1, name);
			const room = Math.min(...mates.map((m) => dist(m, S)));
			const sc = Math.min(room, 16) - dist(P, S) * 0.8;
			if (room >= 10 && sc > score) {
				score = sc;
				best = S;
			}
		}
		if (!best) {
			return;
		}
		const start = (this.free.get(pid) ?? 0) + 60;
		const there = this.go(pid, best, start, JOG + 3, "run");
		const d = this.defenderOf(pid);
		if (d !== undefined) {
			this.shadow(
				d,
				this.defensePoint(team, best, this.posOf(shooter), false),
				start + 200,
				there + 250,
				team,
				pid,
			);
		}
	}

	// Stage everything up to the moment the ball reaches its target - the rim,
	// a blocker's hand, or (on a shooting foul) the release. Returns the moment
	// the shot motion starts (the attempt's line) and when it is decided.
	private stageShot(
		team: Side,
		shooter: number,
		zone: Zone,
		plan: ShotPlan,
		t: number,
		gap: number | undefined,
		heaveSecs: number | undefined,
		clock?: number,
	): {
		gather: number;
		decided: number;
		arrive: number;
		target: Pt3;
		dunk: boolean;
		rim?: AtRim;
	} {
		const dir = attackDir(team);
		const rim = rimPt(team);
		let close = zone === "atRim" || zone === "tipIn" || zone === "putBack";
		const putback = zone === "putBack" || zone === "tipIn";
		// A putback goes back up on his side of the rim - from between it and
		// where he got the ball - not across the lane through everybody.
		const from = this.holder === shooter ? this.posOf(shooter) : undefined;
		// (A jump shot's spot is worked out once he gets the ball - see
		// shotSpotNear.)
		let P: Pt | undefined = putback
			? from && dist(from, rim) > 1
				? (() => {
						const u = unitVec(rim, from);
						// (Never from behind the backboard.)
						const ux = -dir * Math.max(0.35, -dir * u.x);
						const L = Math.hypot(ux, u.y);
						const r = Math.min(dist(from, rim), this.rand(2.5, 4.5));
						return clampPt({
							x: rim.x + (ux / L) * r,
							y: rim.y + (u.y / L) * r,
						});
					})()
				: clampPt({
						x: rim.x - dir * this.rand(2, 4),
						y: 25 + this.rand(-3, 3),
					})
			: heaveSecs !== undefined
				? this.shotSpot(team, zone, heaveSecs)
				: undefined;
		const spotNow = (): Pt =>
			(P ??= this.shotSpotNear(team, zone, this.posOf(shooter)));

		// An alley-oop: "X cuts to the rim as Y lobs up the inbound pass".
		const lob =
			zone === "tipIn" &&
			plan.lobber !== undefined &&
			plan.lobber !== shooter &&
			this.teamOf(plan.lobber) === team
				? plan.lobber
				: undefined;
		// How he gets his own shot off.
		let style: ShotStyle = "plain";
		let lobber = lob;
		// The post move the set is run for, if it is.
		let called: PostMove | undefined;
		if (lob !== undefined) {
			t = this.setUpLob(team, shooter, lob, t);
		} else if (heaveSecs !== undefined && !putback) {
			t = this.raceToHeave(team, shooter, t, gap);
		} else if (!putback || this.holder !== shooter) {
			let run: Running | undefined;
			if (!putback) {
				const dev = this.develop(
					team,
					t,
					gap,
					heaveSecs === undefined
						? (entry) =>
								this.callForShot(entry, team, shooter, zone, plan, gap, clock)
						: undefined,
					zone,
					false,
					shooter,
				);
				t = dev.t;
				run = dev.run;
			} else {
				this.setOffense(t, team);
				if (this.holder === undefined) {
					// It is lying loose: he gets to it first.
					t = this.pickUp(shooter, t, RUN) + 300;
				}
			}
			if (run?.option) {
				const shot = this.playToShot(run, t, plan);
				t = shot.t;
				style = shot.style;
				lobber = shot.lob;
				called = shot.move;
			} else {
				let handler = this.holder ?? this.slots(team)[0]!;
				// In to him off the inbound with no time for anything else: he
				// goes straight up with it.
				const quickIn = this.quickIn === shooter && handler === shooter;
				this.quickIn = undefined;
				// Who sets him up: the real assister on a make; on a miss, the handler
				// about half the time, so a pass never gives the result away.
				let passer = quickIn
					? undefined
					: (plan.assist ??
						(plan.kind !== "make" && handler !== shooter && this.rng() < 0.55
							? handler
							: undefined));
				if (passer === shooter) {
					passer = undefined;
				}
				if (
					passer !== undefined &&
					passer !== handler &&
					this.teamOf(passer) === team
				) {
					t = this.passTo(handler, passer, t);
					handler = passer;
				}
				if (passer !== undefined && this.teamOf(passer) === team) {
					// An entry pass to the post: he seals his man first, a hand up
					// for it, and it comes in to him as he has him sealed - then
					// he backs him down.
					const entry = zone === "lowPost" && this.rng() < 0.9;
					// Fed from up the floor - not handed it from a step away by a
					// man down in the lane with him (see entryFrom).
					const H = this.posOf(handler);
					const P = spotNow();
					if (
						entry &&
						(dist(H, P) < 11 || dist(H, rim) < 11) &&
						this.holder === handler
					) {
						this.hold(handler, t, "dribble");
						t = this.go(
							handler,
							this.entryFrom(team, handler, P),
							t,
							DRIBBLE,
							"dribble",
						);
					}
					const arrive = this.go(shooter, P, t, RUN, "run");
					const d = dist(this.posOf(handler), P);
					const held = entry
						? this.seal(shooter, handler, arrive, dir)
						: undefined;
					const send = Math.max(t, arrive - passMs(d) - 120);
					const caught = this.passTo(
						handler,
						shooter,
						send,
						entry ? (this.rng() < 0.6 ? "bounce" : "chest") : undefined,
					);
					held?.(caught - 110);
					this.respace(team, handler, shooter);
					t = Math.max(arrive, caught);
					if (entry) {
						style = "post";
						t = this.backDown(shooter, t, dir);
					}
				} else if (
					!quickIn &&
					handler === shooter &&
					plan.kind !== "make" &&
					heaveSecs === undefined &&
					(zone === "three" || zone === "midRange") &&
					dist(this.posOf(shooter), rim) < 12 &&
					this.kickOutTo(team, shooter) !== undefined
				) {
					// In under the rim with it himself, his shot a jumper out at
					// the line - off an offensive board, say - and nothing on the
					// line to say he got it up on his own: he kicks it out and
					// goes out to get it back, rather than dribbling all the way
					// back out. His man goes with him.
					const mate = this.kickOutTo(team, shooter)!;
					t = this.passTo(shooter, mate, t);
					// Out to an open spot, not to where he threw it - and a pass
					// away from him, not a throw back across the floor.
					const S0 = this.posOf(shooter);
					const M = this.posOf(mate);
					const others = this.slots(team)
						.filter((m) => m !== shooter)
						.map((m) => this.posOf(m));
					const cost = (X: Pt) =>
						dist(X, S0) + Math.max(0, dist(X, M) - KICK_BACK) * 0.8;
					const Q =
						(zone === "three"
							? RESPACE_SPOTS
							: ["L_elbow", "R_elbow", "L_mid_wing", "R_mid_wing", "high_post"]
						)
							.map((name) => this.spotFor(team, 1, name))
							.filter((X) => others.every((m) => dist(m, X) >= 10))
							.sort((a, b) => cost(a) - cost(b))[0] ?? spotNow();
					P = Q;
					const arrive = this.go(shooter, Q, t, RUN, "run");
					const g = this.defenderOf(shooter);
					if (g !== undefined) {
						this.shadow(
							g,
							this.defensePoint(team, Q, this.posOf(mate), false),
							t + 200,
							arrive + 250,
							team,
							shooter,
						);
					}
					const send = Math.max(
						t + 250,
						arrive - passMs(dist(this.posOf(mate), Q)) - 250,
					);
					t = Math.max(arrive, this.passTo(mate, shooter, send));
					t = Math.max(t, this.hold(shooter, t, "hold"));
				} else {
					if (handler !== shooter) {
						t = this.passTo(handler, shooter, t);
					}
					// His own shot: a move to get it.
					const r = this.rng();
					style = quickIn
						? "plain"
						: zone === "atRim"
							? r < 0.5
								? "crossover"
								: r < 0.72 && plan.finish === "layup"
									? "euro"
									: "plain"
							: zone === "lowPost"
								? "post"
								: zone === "midRange"
									? r < 0.28
										? "stepBack"
										: r < 0.44
											? "fade"
											: r < 0.66
												? "crossover"
												: "plain"
									: zone === "three" && heaveSecs === undefined
										? r < 0.28
											? "stepBack"
											: r < 0.42
												? "crossover"
												: "plain"
										: "plain";
					t = this.driveTo(shooter, spotNow(), t, dir, style);
					t = Math.max(t, this.hold(shooter, t, "hold"));
				}
			}
		}
		// A putback off a long rebound - swatted out, say: he takes it back in
		// to the rim first.
		const drove =
			putback &&
			lob === undefined &&
			this.holder === shooter &&
			P !== undefined &&
			dist(this.posOf(shooter), P) > 6;
		if (drove && P) {
			t = this.driveTo(shooter, P, t, dir, "plain");
			t = Math.max(t, this.hold(shooter, t, "hold"));
		}

		// A three goes up from behind the line: a man a step in front of it,
		// or right on it, hops back out behind it first - a step-back - or,
		// further in, takes a retreat dribble or two out to it. Not walked
		// backward out to it with the ball in his hands.
		if (zone === "three" && heaveSecs === undefined) {
			const out = behindArc(team, this.posOf(shooter));
			if (out && dist(this.posOf(shooter), out) <= STEP_BACK_MOST) {
				t = Math.max(t, this.hold(shooter, t, "hold"));
				const hop = t;
				t = this.go(shooter, out, t + 40, 9, "back", dir);
				this.act(shooter, "stepBack", hop, t, {
					face: dir,
					jump: [0.25, 0.85, 0.7],
				});
			} else if (out) {
				this.hold(shooter, t, "dribble");
				t = this.go(shooter, out, t, DRIBBLE * 0.6, "dribble", dir);
				t = Math.max(t, this.hold(shooter, t, "hold"));
			}
		}

		// Down on the block, his back to the rim and his man backed down: the
		// move. A hook over him, a turnaround fadeaway, a drop step round him
		// to the rim, or the shot fake he bites on and the step through under
		// him.
		let postMove: PostMove | undefined;
		if (style === "post") {
			const r = this.rng();
			postMove = called
				? r < 0.7
					? called
					: r < 0.86
						? "dropStep"
						: "upUnder"
				: this.postMoveFor(shooter, r);
			if (postMove === "dropStep" || postMove === "upUnder") {
				t = this.postFinish(shooter, team, t, postMove);
				close = true;
			}
		}

		// Driving in for a layup or a dunk, he gathers it a few feet out - and
		// his last strides, at the pace he drove, take him in and up (see
		// carry) - not right under the rim, pulled up there and gone up from
		// a standstill.
		const slam =
			close &&
			plan.kind !== "foul" &&
			(plan.finish === "dunk" || plan.finish === "poster");
		const drive = this.track(shooter)?.moves.at(-1);
		if (
			close &&
			postMove === undefined &&
			drive &&
			(drive.anim === "dribble" || drive.anim === "run") &&
			drive.t1 <= t &&
			t - drive.t1 <= 120
		) {
			const la = dist(drive.from, drive.to);
			const near = dist(drive.to, rim);
			const back = Math.min(
				(slam ? DUNK_GATHER : LAYUP_GATHER) - near,
				la * 0.5,
			);
			if (la >= 4 && dist(drive.from, rim) - near >= 2 && back > 0.3) {
				// Gathered there as he gets there, at the pace he drives - the
				// rest of his way in is his last strides, the ball in his hands.
				const ms = drive.t1 - drive.t0;
				const f = (la - back) / la;
				let lo = 0;
				let hi = 1;
				for (let k = 0; k < 24; k++) {
					const mid = (lo + hi) / 2;
					if (runProgress(ms, mid) < f) {
						lo = mid;
					} else {
						hi = mid;
					}
				}
				const u = unitVec(drive.from, drive.to);
				const was = drive.t1;
				drive.to = clampPt({
					x: drive.to.x - u.x * back,
					y: drive.to.y - u.y * back,
				});
				drive.t1 = drive.t0 + ms * ((lo + hi) / 2);
				this.pos.set(shooter, { ...drive.to });
				if ((this.free.get(shooter) ?? 0) <= was) {
					this.free.set(shooter, drive.t1);
				}
				// (Into his hands as he gets there - if it is in his hands: cut
				// to the rim for a lob, it is still up in the air.)
				t =
					this.holder === shooter
						? Math.max(drive.t1, this.hold(shooter, drive.t1, "hold"))
						: drive.t1;
			}
		}

		// The defense: his man closes out, or the blocker / fouler gets there.
		const guard = this.defenderOf(shooter);
		const P1 = this.posOf(shooter);
		const toRim = { x: rim.x - P1.x, y: rim.y - P1.y };
		const len = Math.hypot(toRim.x, toRim.y) || 1;
		// Where his man meets the shot: between him and the rim - from the
		// side, where in front of him would hide the shooter from the camera.
		const ahead = (k: number) =>
			clampPt(
				besideShot(
					P1,
					{
						x: P1.x + (toRim.x / len) * k,
						y: P1.y + (toRim.y / len) * k,
					},
					rim.x,
				),
			);

		const gather = t + 60;
		// A slam when the words say so: "throws it down", "blocked the dunk
		// attempt", "blows the dunk".
		const dunk = slam;
		// The lob, timed to meet him at the top of his jump.
		const caught =
			lobber === undefined
				? -Infinity
				: gather + (dunk ? 1300 * 0.4 : 760 * 0.45);
		if (lobber !== undefined) {
			const catchT = caught;
			const flight = Math.max(
				700,
				380 + dist(this.posOf(lobber), rimPt(team)) * 20,
			);
			this.act(
				lobber,
				"passOverhead",
				catchT - flight - RELEASE_MS - OVERHEAD_WIND,
				catchT - flight + 180,
				{ look: { x: rim.x, y: rim.y } },
			);
			this.fly(catchT - flight, catchT, { pid: lobber }, { pid: shooter });
		}
		let decided: number;
		let arrive: number;
		let target: Pt3;
		let atRim: AtRim | undefined;
		// When he lands off a jumper, if that is what it is.
		let landed: number | undefined;
		const faceRim = (rim.x >= P1.x ? 1 : -1) as 1 | -1;
		const look = { x: rim.x, y: rim.y };

		if (dunk) {
			const dur = 1300;
			// A made dunk hangs on the rim; one that is stuffed or rattles out
			// comes straight back down.
			const hang = plan.kind === "make";
			// (On a man: one hand, or up over his head.)
			const own = this.dunkFor(shooter, this.rng());
			const anim: AnimName =
				plan.finish === "poster" && own === "dunk" ? "dunk1" : own;
			// He goes up in front of the rim, on the side he comes from, close
			// enough that his hand goes over the front of it - and a make, he
			// hangs there by both hands.
			let back = unitVec(
				rim,
				P1.x === rim.x && P1.y === rim.y ? { x: rim.x - dir, y: rim.y } : P1,
			);
			// (In front of it, though, not round the side of it along the
			// baseline: his hand comes down over the front of the rim, not
			// back behind the glass.)
			if (-dir * back.x < Math.cos(DUNK_SIDE)) {
				back = {
					x: -dir * Math.cos(DUNK_SIDE),
					y: (back.y < 0 ? -1 : 1) * Math.sin(DUNK_SIDE),
				};
			}
			const under = clampPt({
				x: rim.x + back.x * DUNK_FROM,
				y: rim.y + back.y * DUNK_FROM,
			});
			// Dunked on: the man the words name meets him at the rim - there
			// in time to be set, right in his way under it, facing him - goes up
			// with him, and loses. Whoever had him on the way in, if somebody
			// else, is a step behind it: beaten.
			const victim = plan.defender;
			if (
				plan.finish === "poster" &&
				victim !== undefined &&
				this.teamOf(victim) !== team
			) {
				const across = { x: -back.y, y: back.x };
				const V0 = this.posOf(victim);
				const s =
					(V0.x - rim.x) * across.x + (V0.y - rim.y) * across.y >= 0 ? 1 : -1;
				const V = clampPt({
					x: rim.x + back.x * 0.6 + across.x * s * 0.9,
					y: rim.y + back.y * 0.6 + across.y * s * 0.9,
				});
				const d = dist(V0, V);
				this.goBy(
					victim,
					V,
					gather - Math.max(500, runMs(d, SPRINT) + 250),
					gather + 60,
					d > 6 ? "run" : "slide",
					-faceRim as 1 | -1,
				);
				this.act(victim, "block", gather + 180, gather + 980, {
					face: -faceRim as 1 | -1,
					look: { ...P1 },
					jump: [0.12, 0.88, 2.3],
				});
				// (And nobody else of theirs in there with them.)
				this.clears.push({
					team: other(team),
					at: under,
					out: unitVec(under, {
						x: under.x + back.x - across.x * s,
						y: under.y + back.y - across.y * s,
					}),
					r: POSTER_ROOM,
					t0: gather - 150,
					t1: gather + 900,
					except: victim,
					lane: dist(P1, under) > 0.3 ? unitVec(under, P1) : back,
					meet: {
						at: V,
						by: gather + 60,
						face: -faceRim as 1 | -1,
						dunker: under,
					},
				});
				const had = this.defenderOf(shooter);
				for (const d of this.slots(other(team))) {
					const Q = this.posOf(d);
					if (d === victim || (d !== had && dist(Q, under) >= 4)) {
						continue;
					}
					this.cutShort(d, gather - 150);
					const away = dist(Q, under) > 0.5 ? unitVec(under, Q) : back;
					// (Out at the edge of the room round it - never nearer him
					// than the man he goes up over.)
					const w = {
						x: away.x + back.x - across.x * s,
						y: away.y + back.y - across.y * s,
					};
					const L = Math.hypot(w.x, w.y);
					const u = L > 0.3 ? { x: w.x / L, y: w.y / L } : away;
					const B = offTheLane(
						clampPt({
							x: under.x + u.x * POSTER_ROOM,
							y: under.y + u.y * POSTER_ROOM,
						}),
						this.clears.at(-1)!,
					);
					this.goBy(d, B, gather - 150, gather + 420, "run");
					if (d === had) {
						this.act(d, "reach", gather + 360, gather + 760, {
							look: { x: rim.x, y: rim.y },
						});
					}
				}
			} else if (
				guard !== undefined &&
				!(plan.kind === "block" && plan.blocker === guard)
			) {
				// Beaten to the rim, his man is still after him: in a stride
				// behind him as he goes up, a hand swiped at it too late.
				const G = this.posOf(guard);
				if (dist(G, under) < 15) {
					const away = dist(G, under) > 0.5 ? unitVec(under, G) : back;
					const w = { x: away.x + back.x, y: away.y + back.y };
					const L = Math.hypot(w.x, w.y);
					const u = L > 0.5 ? { x: w.x / L, y: w.y / L } : away;
					const B = clampPt({ x: under.x + u.x * 2.6, y: under.y + u.y * 2.6 });
					if ((this.free.get(guard) ?? 0) > gather - 300) {
						this.cutShort(guard, gather - 300);
					}
					const there = this.goBy(
						guard,
						B,
						gather - 300,
						gather + 380,
						"run",
						undefined,
						BURST,
					);
					if (there < gather + 700) {
						this.act(
							guard,
							"reach",
							Math.max(there - 100, gather + 300),
							gather + 760,
							{
								look: { x: rim.x, y: rim.y },
							},
						);
					}
				}
			}
			this.act(shooter, anim, gather, gather + dur, {
				face: faceRim,
				look,
				zKeys: hang
					? [
							[0, 0],
							[0.22, 0],
							[0.42, DUNK_LEAP],
							[0.47, DUNK_LEAP - 0.05],
							[0.53, DUNK_LEAP - 0.55],
							[0.7, DUNK_LEAP - 0.68],
							[0.86, 0],
							[1, 0],
						]
					: [
							[0, 0],
							[0.22, 0],
							[0.44, DUNK_LEAP],
							[0.6, DUNK_LEAP - 0.8],
							[0.8, 0],
							[1, 0],
						],
				rim: {
					at: {
						x: rim.x + back.x * (RIM_R - 0.08),
						y: rim.y + back.y * (RIM_R - 0.08),
						z: RIM_Z + 0.05,
					},
					grip: hang
						? [
								[0.47, 0],
								[0.53, 1],
								[0.69, 1],
								[0.74, 0],
							]
						: [],
				},
			});
			this.carry(
				shooter,
				under,
				gather + 60,
				Math.max(
					240,
					(dist(this.posOf(shooter), under) / SPRINT) * 1000,
					dur * 0.42 - 60,
				),
				"run",
				faceRim,
				// (Up off the end of it: all he had going into the rim goes into
				// going up.)
				{ onFrom: true, v1: 0 },
			);
			if (plan.kind === "block" && plan.blocker !== undefined) {
				// Met at the rim - off a lob, once he has it. From a way off -
				// chasing him down - he sets off for it in time to get there,
				// whatever he was doing.
				const contact = Math.max(gather + dur * 0.42, caught + 100);
				const b = plan.blocker;
				const B = clampPt({ x: rim.x - dir * 1.7, y: 25 - 1.1 });
				const by = contact - 300;
				let from = gather - 300;
				for (let n = 0; n < 4; n++) {
					const need =
						runMs(dist(this.posAt(b, from), B), SPRINT, 0, BURST) + 60;
					if (by - from >= need) {
						break;
					}
					from = by - need;
				}
				if ((this.free.get(b) ?? 0) > from) {
					this.cutShort(b, from);
				}
				this.goBy(b, B, from, by, "run", -faceRim as 1 | -1, BURST);
				const left = this.leftToBall(
					{ x: rim.x - dir * 1.7, y: 25 - 1.1 },
					shooter,
					P1,
				);
				this.act(b, "block", contact - 330, contact + 420, {
					face: -faceRim as 1 | -1,
					look: { ...P1 },
					jump: [0.1, 0.9, 3.3],
					...(left ? { mirror: true as const } : {}),
				});
				this.fly(
					contact - 80,
					contact,
					{ pid: shooter },
					{ pid: b, hand: left ? "far" : "near" },
				);
				decided = contact;
				arrive = contact;
				target = { x: rim.x - dir * 1.4, y: 25, z: RIM_Z - 0.3 };
			} else if (plan.kind === "miss") {
				// Hammered off the back iron.
				decided = gather + dur * 0.47;
				arrive = decided;
				target = { x: rim.x + dir * (RIM_R + 0.1), y: 25, z: RIM_Z + 0.25 };
				this.fly(decided - 90, decided, { pid: shooter }, target);
			} else {
				// Thrown down at the top of the slam.
				decided = gather + dur * 0.47;
				arrive = decided;
				target = rimPt(team, 0.45);
			}
			// (Whoever of theirs is right there goes up with him - see wallUp.
			// Not round a man dunked on: the room there is cleared.)
			if (plan.finish !== "poster" && plan.kind !== "block") {
				this.walls.push({ shooter, release: gather + dur * 0.38 });
			}
		} else {
			// "Tips it in": a one-handed tap at the top of the jump.
			const tip = zone === "tipIn" && plan.finish === "tip" && !drove;
			// A floater: one hand, off the wrong foot on the move, let go on the
			// way up and lofted high.
			const floater = style === "floater" && !close && !tip;
			let anim: AnimName = tip
				? "block"
				: close
					? this.layupFor(shooter, gather, plan)
					: floater
						? "floater"
						: "shoot";
			if (postMove) {
				anim =
					postMove === "hook"
						? "hook"
						: postMove === "fade"
							? "fade"
							: postMove === "dropStep"
								? "powerLayup"
								: "layup";
			} else if (style === "fade") {
				anim = "fade";
			} else if (style === "hook") {
				anim = "hook";
			}
			// Come in along the baseline, now and then he goes on under the rim
			// and lays it in off the far side of it - a reverse - facing along
			// the baseline the way he goes, his hand nearer the rim up and back
			// at it.
			const depth = Math.abs(P1.x - (rim.x + dir * RIM_INSET));
			const across = Math.abs(P1.y - rim.y);
			const reverse =
				close &&
				!tip &&
				anim !== "powerLayup" &&
				postMove === undefined &&
				style !== "euro" &&
				zone === "atRim" &&
				(plan.kind === "make" || plan.kind === "miss") &&
				depth < 8 &&
				across >= 2 &&
				across <= 11 &&
				hash01(shooter, gather + 3) < REVERSE_SHARE;
			const under = Math.sign(P1.y - rim.y) || 1;
			const far = clampPt({
				x: rim.x - dir * FINISH_FRONT,
				y: rim.y - under * REVERSE_OUT,
			});
			const going = unitVec(P1, far);
			// (His left, going that way, and whether the rim is on it.)
			const offLeft = (rim.x - far.x) * going.y - (rim.y - far.y) * going.x > 0;
			if (reverse) {
				anim = "reverseLayup";
			}
			// A jumper takes a second from the dip to landing (the ball gone
			// just past halfway: about two-thirds of a second off the catch,
			// the league's typical catch-and-shoot).
			const jumper = anim === "shoot" || anim === "fade";
			// (Each shooter his own stroke, every time: a quick one off a low
			// lift, or a slower one rising higher - up as long as gravity
			// takes for the height.)
			const form = jumper
				? FORM_LO + (FORM_HI - FORM_LO) * hash01(shooter, 6151)
				: 1;
			const dur =
				(close || floater ? 760 : zone === "lowPost" ? 900 : 1000) * form;
			if (jumper) {
				landed = gather + dur;
			}
			if (anim === "fade") {
				// Drifting back as he rises.
				this.carry(
					shooter,
					clampPt({
						x: P1.x - (toRim.x / len) * 1.4,
						y: P1.y - (toRim.y / len) * 1.4,
					}),
					gather + dur * JUMPER.off - 40,
					350,
					"run",
					faceRim,
				);
			}
			// How high he gets up: up off the floor on a jumper as long as
			// gravity takes for that - a little higher pulling up from mid-range
			// than catching and shooting a three.
			const peak = tip
				? 2.8
				: close
					? 2.4
					: floater
						? 1.9
						: zone === "lowPost"
							? 1.1
							: zone === "midRange"
								? 1.55
								: 1.4;
			const jump: [number, number, number] = jumper
				? [JUMPER.off, JUMPER.land, peak * form * form]
				: [0.24, 0.93, peak];
			this.act(shooter, anim, gather, gather + dur, {
				face: faceRim,
				look: reverse
					? { x: far.x + going.x * 8, y: far.y + going.y * 8 }
					: look,
				jump,
				...(reverse && offLeft ? { mirror: true as const } : {}),
			});
			const letGo = close
				? 0.6
				: floater
					? 0.5
					: jumper
						? JUMPER.release
						: 0.55;
			const release = gather + dur * letGo;
			// Where it leaves his hand - and once it has, the last of his way
			// in carries him on a foot or so in the air (see below).
			let drift: Pt | undefined;
			if (close || floater) {
				// The last strides in: up from a couple of feet out, where it
				// can go up and over the front of the rim or off the glass - not
				// from under it. (A floater, a shorter one, well short of it; a
				// finish off the block, from the stride out his move took him.)
				const out = floater
					? 5.5
					: zone === "lowPost"
						? POST_FINISH_OUT
						: 2.4 + 0.7 * hash01(shooter, gather);
				const step = Math.max(
					0,
					Math.min(floater ? 1.4 : LAYUP_STRIDE, len - out),
				);
				let S = reverse
					? far
					: clampPt({
							x: P1.x + (toRim.x / len) * step,
							y: P1.y + (toRim.y / len) * step,
						});
				if (reverse) {
					// (Going along the baseline, the hand up and back at the rim
					// reaches out toward it: he goes up far enough out from it
					// that the ball, not just he, is in front of the glass.)
					const h = releaseAt("reverseLayup", letGo);
					const hs = offLeft ? -h.s : h.s;
					const bx = going.x * h.f + going.y * hs;
					const short =
						-REVERSE_BALL_BEHIND - ((rim.x - far.x) * dir - bx * dir);
					if (short > 0) {
						S = clampPt({ x: far.x - dir * short, y: far.y });
					}
				}
				if (!reverse) {
					// (In front of the glass, and out from under the ring: see
					// FINISH_FRONT.)
					if ((rim.x - S.x) * dir < FINISH_FRONT) {
						S = { x: rim.x - dir * FINISH_FRONT, y: S.y };
					}
					const r = dist(S, rim);
					if (r < FINISH_OUT) {
						const u = r > 0.3 ? unitVec(rim, S) : { x: -dir, y: 0 };
						S = clampPt({
							x: rim.x + u.x * FINISH_OUT,
							y: rim.y + u.y * FINISH_OUT,
						});
					}
				}
				this.carry(
					shooter,
					S,
					gather + 40,
					Math.max(240, release - gather - 40),
					"run",
					faceRim,
					{ onFrom: true, v1: AIR_DRIFT },
				);
				const room = reverse ? 1.2 : Math.max(0, dist(S, rim) - 1.6);
				const on = Math.min(
					room,
					((AIR_DRIFT / 2) * (dur * (jump[1] - letGo))) / 1000,
				);
				// (On a reverse, on the way he goes - out from under it.)
				const way = reverse ? going : { x: toRim.x / len, y: toRim.y / len };
				if (on > 0.3) {
					drift = clampPt({
						x: S.x + way.x * on,
						y: S.y + way.y * on,
					});
				}
			}
			// The ball on his fingers then, as high as his jump has him.
			const v = (letGo - jump[0]) / (jump[1] - jump[0]);
			// (Up off his own jumper, the ball in his hand on it.)
			let fingers = releaseAt(
				anim === "shoot" ? shotForm(shooter) : anim,
				letGo,
			);
			fingers.u += v > 0 && v < 1 ? 4 * jump[2] * v * (1 - v) : 0;
			if (reverse) {
				// (Facing along the baseline, not at the rim: put back as if he
				// faced it, as the shot from there is worked out.)
				const s = offLeft ? -fingers.s : fingers.s;
				const wx = going.x * fingers.f + going.y * s;
				const wy = going.y * fingers.f - going.x * s;
				const u = unitVec(far, rim);
				fingers = {
					f: wx * u.x + wy * u.y,
					s: wx * u.y - wy * u.x,
					u: fingers.u,
				};
			} else if (this.offHisLeft(shooter)) {
				fingers.s = -fingers.s;
			}
			const d = dist(P1, rim);
			// (In close, it goes up soft: up over the rim and down onto it -
			// never up into the ring from under it.)
			const lift = Math.sqrt(
				2 * GRAVITY * Math.max(0.5, RIM_Z + LAYUP_LOFT - fingers.u),
			);
			const fall = Math.sqrt(
				Math.max(
					0,
					lift * lift - 2 * GRAVITY * Math.max(0, RIM_Z + 0.2 - fingers.u),
				),
			);
			const flight = close
				? Math.max(300, ((lift + fall) / GRAVITY) * 1000)
				: floater
					? 760 + d * 24
					: 620 + d * 22;
			if (plan.kind === "block" && plan.blocker !== undefined) {
				const b = plan.blocker;
				// He is there as it goes up, and gets a hand on it as it leaves
				// his - from a way off, setting off in time to get there,
				// whatever he was doing (not still on his way as it flies, the
				// ball hung up in the air till he gets to it).
				// (Ahead of where he comes down, a floater carried on in the air
				// - not where he took off, the shooter drifting into him.)
				const on = dist(P1, drift ?? this.posOf(shooter));
				const B = ahead(Math.min(2.6, Math.max(1.4, len - 1 - on)) + on);
				const by = release - 80;
				let from = gather - 600;
				for (let n = 0; n < 4; n++) {
					const need =
						runMs(dist(this.posAt(b, from), B), SPRINT, 0, BURST) + 60;
					if (by - from >= need) {
						break;
					}
					from = by - need;
				}
				if ((this.free.get(b) ?? 0) > from) {
					this.cutShort(b, from);
				}
				const there = this.goBy(
					b,
					B,
					from,
					by,
					"run",
					-faceRim as 1 | -1,
					BURST,
				);
				const contact = Math.max(release + 110, there + 180);
				// With the hand on the ball's side, straight up at it.
				const left = this.leftToBall(this.posOf(b), shooter, P1);
				this.act(b, "block", contact - 300, contact + 420, {
					face: -faceRim as 1 | -1,
					look: { ...P1 },
					jump: [0.1, 0.9, 2.7],
					...(left ? { mirror: true as const } : {}),
				});
				this.fly(
					release,
					contact,
					{ pid: shooter },
					{ pid: b, hand: left ? "far" : "near" },
				);
				decided = contact;
				arrive = contact;
				target = { x: P1.x, y: P1.y, z: 9 };
			} else if (plan.kind === "foul") {
				const f = plan.fouler ?? guard;
				if (f !== undefined) {
					// (From a way off, setting off in time to get there.)
					const F = ahead(1.6);
					const by = release - 120;
					let from = gather - 300;
					for (let n = 0; n < 4; n++) {
						const need =
							runMs(dist(this.posAt(f, from), F), SPRINT, 0, BURST) + 60;
						if (by - from >= need) {
							break;
						}
						from = by - need;
					}
					if ((this.free.get(f) ?? 0) > from) {
						this.cutShort(f, from);
					}
					this.goBy(f, F, from, by, "run", -faceRim as 1 | -1, BURST);
					this.act(f, "reach", release - 200, release + 260, {
						face: -faceRim as 1 | -1,
						look: { ...P1 },
					});
				}
				target = { x: rim.x - dir * (RIM_R + 0.1), y: 24.7, z: RIM_Z + 0.1 };
				this.fly(release, release + flight, { pid: shooter }, target);
				decided = release + 130;
				arrive = release + flight;
			} else {
				atRim = this.shootAtRim(
					team,
					shooter,
					zone,
					plan,
					release,
					close ? [380, 560] : [flight * 0.92, flight * 1.08],
					fingers,
				);
				if (atRim) {
					const p = atRim.found.play;
					decided = atRim.t0 + atRim.line;
					arrive = atRim.t0 + (p.made ? p.decided : p.end.t);
					target = playAt(p.pts, arrive - atRim.t0);
				} else {
					target =
						plan.kind === "make"
							? rimPt(team, 0.35)
							: {
									x: rim.x + (this.rng() < 0.7 ? -dir : dir) * (RIM_R + 0.05),
									y: 25 + this.rand(-0.5, 0.5),
									z: RIM_Z + 0.12,
								};
					this.fly(release, release + flight, { pid: shooter }, target);
					decided = release + flight;
					arrive = decided;
				}
				// How much room he gets: at the rim, a body on him; out on the
				// floor, anything from a hand in his face to nobody near.
				const odds =
					zone === "three"
						? plan.kind === "make"
							? ROOM_MADE_3
							: ROOM_MISSED_3
						: plan.kind === "make"
							? ROOM_MADE
							: ROOM_MISSED;
				let room = 0;
				for (let x = hash01(shooter * 5 + 2, release); room < 3; room++) {
					x -= odds[room]!;
					if (x < 0) {
						break;
					}
				}
				const outAt =
					guard === undefined
						? release
						: release + OPEN_LATE * hash01(guard * 7 + 3, release);
				if (
					room === 3 &&
					guard !== undefined &&
					!this.helpOff(guard, shooter, P1, rim, gather, outAt) &&
					dist(this.posOf(guard), P1) < ALONE_FT
				) {
					room = 2;
				}
				if (guard !== undefined && room === 3 && !close) {
					// Out of the help, or from wherever he was, flat out - only
					// as it goes up: a hand up as it goes over him, if that.
					this.closeOut(
						guard,
						shooter,
						ahead(ROOM_FT[1]),
						gather,
						release,
						close,
						0,
						outAt,
					);
				} else if (guard !== undefined) {
					this.closeOut(
						guard,
						shooter,
						// (An arm's length off him at the least: his hand at the
						// ball, not his body in the shooter's. In close, beside
						// where he comes down - not stood in his way in, the
						// shooter going up through him.)
						close
							? this.besideLanding(guard, drift ?? this.posOf(shooter), toRim)
							: ahead(ROOM_FT[room]!),
						gather,
						release,
						close,
						close ? 0 : CLOSE_LATE[room]!,
						undefined,
						close ? (drift ?? this.posOf(shooter)) : undefined,
					);
				}
				// (Anybody else of theirs right there goes up with him - see
				// wallUp.)
				if (close || floater) {
					this.walls.push({ shooter, release });
				}
			}
			if (drift) {
				// On in the air from where it left his hand, slowing to land.
				this.carry(
					shooter,
					drift,
					release,
					Math.max(120, gather + dur * jump[1] - release),
					"run",
					faceRim,
					{ onFrom: true, v1: 0 },
				);
			}
		}

		// A shooter holds his follow-through after he lands, till the ball
		// gets there - whatever his feet do next. (With the hand he shot it
		// with.)
		if (
			landed !== undefined &&
			(plan.kind === "make" || plan.kind === "miss")
		) {
			this.gesture(
				shooter,
				"follow",
				landed,
				Math.min(decided + 120, landed + 1200),
				undefined,
				1,
				this.offHisLeft(shooter),
			);
		}
		if (!dunk && !putback && plan.kind !== "block") {
			const reads =
				atRim?.falls &&
				plan.rebounder !== undefined &&
				plan.rebounder !== shooter
					? { pid: plan.rebounder, at: atRim.falls }
					: undefined;
			this.crashTheGlass(
				team,
				shooter,
				gather,
				arrive,
				[shooter, ...(plan.fouler === undefined ? [] : [plan.fouler])],
				reads,
			);
		}
		return {
			gather,
			decided,
			arrive,
			target,
			dunk,
			...(atRim ? { rim: atRim } : {}),
		};
	}

	// UP AND AT THE RIM.
	//
	// The shot is thrown from his hands at the rim, and the last of it - in
	// clean, in off the iron or the glass, rimmed out, bricked - is played out
	// for real (see physics.ts): the kind of make or miss the line says, and
	// a miss coming off toward whoever gets the rebound. How it goes in goes
	// with where he shoots from: off the glass from the side and in close,
	// never from straight on or far out.
	private shootAtRim(
		team: Side,
		shooter: number,
		zone: Zone,
		plan: ShotPlan,
		release: number,
		flight: [number, number],
		// The ball on his fingers as he lets it go, from his feet.
		hand: V3,
	): AtRim | undefined {
		if (plan.kind !== "make" && plan.kind !== "miss") {
			return undefined;
		}
		const rim = rimPt(team);
		const P = this.posOf(shooter);
		const u = unitVec(P, rim);
		const from = {
			x: P.x + u.x * hand.f + u.y * hand.s,
			y: P.y + u.y * hand.f - u.x * hand.s,
			z: hand.u,
		};
		const roll = hash01(shooter, release);
		// From the side, at an angle to the glass, it is there to use.
		const side = Math.abs(P.y - rim.y) / (dist(P, rim) || 1);
		const glass = side > 0.35 && side < 0.95 && dist(P, rim) < 17;
		let kinds: ShotKind[];
		if (plan.kind === "make") {
			const close = zone !== "three" && zone !== "midRange";
			const bank = glass ? (close ? 0.38 : 0.14) : 0;
			const swish = close ? 0.3 : zone === "three" ? 0.58 : 0.5;
			kinds =
				roll < bank ? ["bank"] : roll < bank + swish ? ["swish"] : ["rim"];
		} else {
			const f = plan.finish;
			kinds =
				f === "rimOut"
					? ["rimOut", "rollOut"]
					: f === "rollOut"
						? ["rollOut", "rimOut"]
						: f === "brick"
							? ["brick"]
							: f === "airball"
								? ["air"]
								: ["off", "rimOut", "brick"];
		}
		return this.playAtRim(team, shooter, from, release, flight, {
			made: plan.kind === "make",
			kinds,
			...(plan.rebounder === undefined
				? { inPlay: true }
				: {
						toward: this.towardOf(team, plan.rebounder, dist(P, rim)),
					}),
		});
	}

	// Out of his hands at `release`, from `from`, and played out at the rim
	// the way `want` says (see physics.ts) - tried the same way on every
	// device, from who shot it and when. Undefined if it can't be.
	private playAtRim(
		team: Side,
		shooter: number,
		from: Pt3,
		release: number,
		flight: [number, number],
		want: ShotWant,
	): AtRim | undefined {
		const rim = rimPt(team);
		let seed = Math.round(hash01(shooter * 31 + 7, release) * 2 ** 31);
		const rand = () => {
			seed = (seed + 0x6d2b79f5) | 0;
			let x = Math.imul(seed ^ (seed >>> 15), 1 | seed);
			x = (x + Math.imul(x ^ (x >>> 7), 61 | x)) ^ x;
			return ((x ^ (x >>> 14)) >>> 0) / 4294967296;
		};
		const found = findShot(team, from, flight, want, rand);
		if (!found) {
			return undefined;
		}
		// Out of his hands to where the play at the rim takes it over, spinning
		// back the way it was thrown.
		const t0 = release + found.handoff;
		this.fly(release, t0, { pid: shooter }, found.p);
		const spin = -Math.PI * 2 * BACKSPIN * (Math.sign(rim.x - from.x) || 1);
		this.pushBall({
			kind: "path",
			t0,
			t1: t0 + (found.play.pts.length / 3 - 1) * SAMPLE_MS,
			pts: found.play.pts,
			v0: found.v,
			roll0: (spin * found.handoff) / 1000,
			spin,
		});
		this.holder = undefined;
		this.ballAt = { ...found.play.end.p };
		const play = found.play;
		const last = play.touches.filter((h) => h.t <= play.decided).at(-1);
		// Off the rim, where it comes down into a rebounder's reach.
		const e = play.end;
		const drop = Math.max(0, e.p.z - (BOARD_HANDS + 1.6));
		const tau =
			(e.v.z + Math.sqrt(e.v.z * e.v.z + 2 * GRAVITY * drop)) / GRAVITY;
		return {
			t0,
			found,
			line: play.made || !last ? play.decided : last.t,
			...(play.made
				? {}
				: { falls: { x: e.p.x + e.v.x * tau, y: e.p.y + e.v.y * tau } }),
		};
	}

	// Every time a shot played out at the rim hit the iron or the glass on
	// the way to being decided, the basket shakes with it.
	private rimClanks(play: AtRim, team: Side) {
		for (const h of play.found.play.touches) {
			if (h.t <= play.found.play.decided + 1) {
				this.effect("clank", play.t0 + h.t, { rim: team });
			}
		}
	}

	// In: out of the net to the floor (the play at the rim from t0), up off
	// it as hard as it came down (less what the hardwood takes), a smaller
	// hop, and away to `settle`. Returns when it comes to rest.
	private dropThrough(t0: number, play: ShotPlay, settle: Pt): number {
		const pts = play.pts;
		const n = pts.length;
		const out = t0 + (n / 3 - 1) * SAMPLE_MS;
		const down = 0.75 * play.end.v.z;
		const h0 = Math.max(0.8, Math.min(3.4, (down * down) / (2 * GRAVITY)));
		const end = out + this.bounceSpan(h0, 2, 1150);
		this.bounce(
			out,
			end,
			{ x: pts[n - 3]!, y: pts[n - 2]!, z: pts[n - 1]! },
			settle,
			2,
			h0,
		);
		return end;
	}

	// Off the rim toward a man, as far out his way as a miss comes - the
	// longer the shot, the longer it comes off (`from` feet out).
	private towardOf(team: Side, pid: number, from = 0): Pt {
		const rim = rimPt(team);
		const R = this.posOf(pid);
		const u = unitVec(rim, R);
		const k = Math.min(dist(rim, R), 6 + Math.min(from, 26) * 0.22);
		return { x: rim.x + u.x * k, y: rim.y + u.y * k };
	}

	// All alone off a pass: his man had sunk in to help - in toward the
	// ball and the rim as it was swung out to him - and, flat out from the
	// moment he reads it, is out there still well off him only at `outAt`
	// (see closeOut). Not off the shooter's own dribble: with no pass to
	// have helped on, he is open at most. Whether he did.
	private helpOff(
		guard: number,
		shooter: number,
		P: Pt,
		rim: Pt,
		gather: number,
		outAt: number,
	): boolean {
		const pass = this.ball.findLast(
			(s) => s.kind === "fly" && "pid" in s.to && s.to.pid === shooter,
		);
		if (
			pass?.kind !== "fly" ||
			pass.t1 <= gather - 1200 ||
			!("pid" in pass.from)
		) {
			return false;
		}
		const ball = this.posAt(pass.from.pid, pass.t0);
		const into = unitVec(P, {
			x: (ball.x + rim.x) / 2,
			y: (ball.y + rim.y) / 2,
		});
		const k = Math.min(
			HELP_MAX,
			ROOM_FT[3] +
				reachIn(
					outAt - Math.min(gather - 200, pass.t0 + CLOSE_READ),
					CLOSE_RUN,
					BURST,
				),
		);
		const H = clampPt({ x: P.x + into.x * k, y: P.y + into.y * k });
		if (dist(H, P) < ALONE_FT) {
			return false;
		}
		// (Off him that far already.)
		if (dist(this.posAt(guard, pass.t0), P) >= dist(H, P)) {
			return true;
		}
		// (Sooner the farther in he goes: in there as the pass goes. But not
		// before the man who threw it had it, nor back across a cut.)
		let had = pass.t0;
		for (let k = this.ball.indexOf(pass) - 1; k >= 0; k--) {
			const s = this.ball[k]!;
			if (s.kind !== "hold" || s.pid !== pass.from.pid) {
				break;
			}
			had = s.t0;
		}
		const t0 = Math.max(
			had,
			...this.cuts.filter((c) => c <= pass.t0),
			pass.t0 -
				Math.min(
					2 * HELP_EARLY,
					Math.max(
						HELP_EARLY,
						runMs(dist(this.posAt(guard, pass.t0 - HELP_EARLY), H), HELP_FTPS) +
							CLOSE_READ,
					),
				),
		);
		if (t0 > pass.t0 - HELP_EARLY / 2) {
			return false;
		}
		this.cutShort(guard, t0);
		this.go(guard, H, t0, HELP_FTPS);
		return true;
	}

	// His man is going up with it: he gets out to him. From a step or two
	// away he is just there, a hand up. From out in the lane - he had left
	// him to help - he reads the pass, sprints out and breaks down into
	// short, choppy steps, a hand high, and gets there when he gets there:
	// up into the shot, or a hand in the shooter's face as it goes over him.
	private closeOut(
		d: number,
		shooter: number,
		C: Pt,
		gather: number,
		release: number,
		close: boolean,
		// How late he is (ms): still on his way as it goes up.
		late = 0,
		// Or, leaving him all alone, when he is out there still ROOM_FT[3]
		// off him, flat out to it and breaking down only then.
		outAt?: number,
		// Where the shooter comes down, if not where he goes up from.
		down?: Pt,
	) {
		const P = this.posOf(shooter);
		let face = (P.x >= C.x ? 1 : -1) as 1 | -1;
		// With the hand on the ball's side.
		let left = this.leftToBall(C, shooter, P);
		const contest = (t: number, peak: number) => {
			// (Not from way off him, nor once he is up for the rebound off it.)
			if (
				dist(C, P) > CONTEST_FT ||
				this.track(d)?.acts.some(
					(a) =>
						(a.anim === "board" || a.anim === "rebound") &&
						a.t1 > t &&
						a.t0 < t + 680,
				)
			) {
				return;
			}
			this.act(d, "contest", t, t + 680, {
				face,
				look: { ...P },
				jump: [0.15, 0.9, peak],
				...(left ? { mirror: true as const } : {}),
			});
		};
		// The pass out to him, if that is how he got it.
		const pass = this.ball.findLast(
			(s) => s.kind === "fly" && "pid" in s.to && s.to.pid === shooter,
		);
		const thrown =
			pass?.kind === "fly" && pass.t1 > gather - 1200 ? pass.t0 : undefined;
		// Off whatever he was doing the moment he sees it coming: the pass
		// out to his man, or his man going up with it.
		let sees =
			(thrown === undefined
				? gather - 200
				: Math.min(gather - 200, thrown + CLOSE_READ)) + late;
		// Where he breaks down from a run in from G.
		const breakFrom = (G: Pt): Pt => {
			const u = unitVec(G, C);
			let s = CLOSE_BREAK;
			let B = clampPt({ x: C.x - u.x * s, y: C.y - u.y * s });
			while (
				outAt !== undefined &&
				dist(B, P) < ROOM_FT[3] &&
				s < dist(G, C) - 0.5
			) {
				s += 0.25;
				B = clampPt({ x: C.x - u.x * s, y: C.y - u.y * s });
			}
			return B;
		};
		if (outAt !== undefined) {
			// On with whatever he was doing till he has to go.
			for (let n = 0; n < 2; n++) {
				const G = this.posAt(d, sees);
				sees = Math.max(
					sees,
					outAt - runMs(dist(G, breakFrom(G)), CLOSE_RUN, 0, BURST, CLOSE_CHOP),
				);
			}
		}
		if ((this.free.get(d) ?? 0) > sees) {
			this.cutShort(d, sees);
		}
		const G = this.posOf(d);
		const far = dist(G, C);
		if (outAt === undefined && (far < 7 || thrown === undefined)) {
			const by = release - 60 + late;
			// (In close, round the shooter going in and up - not through him.)
			const met = down
				? this.meetClear(d, shooter, C, sees, by, down)
				: { at: C, there: this.goBy(d, C, sees, by, "run", face, BURST) };
			if (met.at !== C) {
				C = met.at;
				face = (P.x >= C.x ? 1 : -1) as 1 | -1;
				left = this.leftToBall(C, shooter, P);
			}
			const there = met.there;
			if (dist(G, P) >= 9) {
				return;
			}
			if (there <= release + 100) {
				contest(Math.max(there - 150, release - 260), close ? 2 : 1.3);
			} else if (there < release + 700) {
				// Late: a hand in his face as it goes up over him.
				contest(there - 120, 0.5);
			}
			return;
		}
		const brk = breakFrom(G);
		// Flat out to a few feet off him, then choppy steps the rest of the
		// way in - straight on from the one into the other.
		const broke = this.go(d, brk, sees, CLOSE_RUN, "run", undefined, {
			effort: BURST,
			onto: CLOSE_CHOP,
		});
		const there = this.go(d, C, broke, CLOSE_CHOP, "closeout", face, {
			effort: BURST,
		});
		if (outAt !== undefined) {
			// Leaving him all alone: a hand flying up at it as it goes over,
			// on his way in - if he is out there in time even for that.
			if (broke < release + 450) {
				contest(Math.max(broke + 100, release - 100), 0.4);
			}
		} else if (there <= release + 100) {
			contest(Math.max(there - 150, release - 260), close ? 1.8 : 1.1);
		} else if (there < release + 700) {
			// Late: a hand in his face as it goes up over him.
			contest(there - 120, 0.5);
		}
	}

	// The shot goes up and everybody plays it. The bigs crash the glass, and
	// the rest of the offense follows them in or gets back; every defender
	// turns and finds his man - backs into the one coming in and sits on him,
	// arms out wide - or, his man leaving, goes in to the glass himself. All
	// eyes on the ball, until it comes off the rim (`land`).
	private crashTheGlass(
		team: Side,
		shooter: number,
		gather: number,
		land: number,
		busy0: number[],
		// Who reads where it is coming down off the rim, and gets there.
		reads?: { pid: number; at: Pt },
	) {
		// Everybody plays it as it goes up: into each other well before it
		// comes down.
		const go = Math.max(gather + 250, land - 1500);
		const meet = Math.max(go + 380, land - 900);
		const until = land + 150;
		if (until - meet < 280) {
			return;
		}
		const rim = { x: rimX(team), y: COURT_H / 2 };
		const dir = attackDir(team);
		const busy = [...busy0];
		// When he is free to: done running where he was going, done with what
		// he was doing (a contest of the shot, say).
		const readyAt = (pid: number): number => {
			let t = Math.max(go, this.free.get(pid) ?? 0);
			for (const a of this.track(pid)?.acts ?? []) {
				if (a.t1 > t && a.t0 < meet) {
					t = Math.max(t, a.t1);
				}
			}
			return t;
		};
		// The shot up, nobody runs on with what he was doing before it -
		// shadowing his man out on the floor, a cut still on its way: each
		// plays the shot from where he is as it goes. (Not out of anything
		// he does about it - a contest of it, say - or is in the middle of.)
		// A man that leaves free to play it who wasn't before draws his lot
		// for it from who he is and when (see ownDraws), not the game's luck.
		const was = new Map<number, number>();
		for (const pid of [...this.slots(team), ...this.slots(other(team))]) {
			was.set(pid, readyAt(pid));
			if (
				pid !== shooter &&
				!busy.includes(pid) &&
				(this.free.get(pid) ?? 0) > go &&
				!this.track(pid)?.acts.some((a) => a.t1 > go && a.t0 < until)
			) {
				this.cutShort(pid, go);
			}
		}
		const luck = (pid: number, by: number): (() => number) =>
			(was.get(pid) ?? Infinity) <= by
				? () => this.rng()
				: this.ownDraws(pid, gather);
		// The man who gets it reads it off the shot: from the moment the
		// shooter goes up with it (once he is done with whatever he is doing),
		// to where it will come down into his reach - facing the rim, to take
		// it in front of him - as fast as it takes to be there and set before
		// it does, flat out if it is a long way. There he holds his ground.
		// (Not only once it is out of the shooter's hands: from out on the
		// floor - a dozen feet and more, most of them - that has him there too
		// late to go up for it, and half the boards were let bounce first.)
		const spot =
			reads &&
			clampPt({
				x: reads.at.x + unitVec(rim, reads.at).x * BOARD_OUT,
				y: reads.at.y + unitVec(rim, reads.at).y * BOARD_OUT,
			});
		if (reads && spot && !busy.includes(reads.pid)) {
			let r0 = Math.max(gather + READ_MS, this.free.get(reads.pid) ?? 0);
			for (const a of this.track(reads.pid)?.acts ?? []) {
				if (a.t1 > r0 && a.t0 < land - 300) {
					r0 = Math.max(r0, a.t1);
				}
			}
			const there = this.goBy(
				reads.pid,
				spot,
				r0,
				land - 350,
				"run",
				undefined,
				BURST,
			);
			if (there < until - 250) {
				this.act(
					reads.pid,
					this.teamOf(reads.pid) === team ? "fight" : "boxOut",
					there,
					until,
					{ look: rim },
				);
			}
			busy.push(reads.pid);
		}
		// Where each man going in to the glass ends up: a step apart, never
		// two on the one spot - round the rim from it, if it is taken.
		const taken: Pt[] = reads && spot ? [spot] : [];
		const apart = (P: Pt): Pt => {
			const d = dist(P, rim);
			const a0 = Math.atan2(P.y - rim.y, P.x - rim.x);
			for (const k of [0, 1, -1, 2, -2, 3, -3]) {
				const a = a0 + k * GLASS_TURN;
				const Q = clampPt({
					x: rim.x + Math.cos(a) * d,
					y: rim.y + Math.sin(a) * d,
				});
				if (taken.every((T) => dist(T, Q) >= GLASS_APART)) {
					taken.push(Q);
					return Q;
				}
			}
			taken.push(P);
			return P;
		};
		// Who crashes and who gets back - and where the men crashing go.
		const crash = new Map<number, Pt>();
		const inAt = new Map<number, number>();
		this.slots(team).forEach((pid, j) => {
			const r0 = readyAt(pid);
			if (busy.includes(pid) || r0 > Math.max(meet - 200, until - GLASS_LATE)) {
				return;
			}
			const draw = luck(pid, meet - 200);
			const M = this.posOf(pid);
			const out = dist(M, rim);
			const big = (this.rank.get(pid) ?? 4) >= 6 || j >= 4;
			const r = draw();
			if (big ? r < 0.85 : out < 17 ? r < 0.45 : r < 0.18) {
				const u = unitVec(M, rim);
				const k = Math.max(0, Math.min(out - (5 + 3 * draw()), 9));
				const C = apart(clampPt({ x: M.x + u.x * k, y: M.y + u.y * k }));
				crash.set(pid, C);
				inAt.set(pid, this.goBy(pid, C, r0, meet, "run", undefined, BURST));
			} else if (out < 30) {
				// Back on defense, eyes on the shot - backing out if that keeps
				// the rim in front of him, turning and jogging back if not (out of
				// a corner, say, where backing up the floor would have him face
				// the stands).
				const back = clampPt({
					x: M.x - dir * (5 + 4 * draw()),
					y: M.y + (COURT_H / 2 - M.y) * 0.3,
				});
				const going = unitVec(M, back);
				const away = unitVec(rim, M);
				this.goBy(
					pid,
					back,
					r0,
					until + 300,
					going.x * away.x + going.y * away.y > BACK_FACING ? "back" : "jog",
				);
			}
		});
		// Which of them his own man is free to put a body on.
		const onHim = (man: number): number | undefined => {
			const d = this.defenderOf(man);
			return d === undefined ||
				busy.includes(d) ||
				readyAt(d) > Math.max(meet - 150, until - GLASS_LATE)
				? undefined
				: d;
		};
		// Each man coming in with somebody on him: who.
		const boxed = new Map<number, number>();
		for (const man of this.slots(team)) {
			const d = onHim(man);
			if (d === undefined) {
				continue;
			}
			const r0 = readyAt(d);
			const draw = luck(d, meet - 150);
			let C = crash.get(man);
			let mark = man;
			if (!C) {
				// His man is gone: the nearest man coming in that nobody else
				// has, if there is one near enough to get a body on.
				const D0 = this.posOf(d);
				const other = [...crash.keys()]
					.filter(
						(o) =>
							onHim(o) === undefined &&
							!boxed.has(o) &&
							dist(crash.get(o)!, D0) <= GLASS_FIND,
					)
					.sort((a, b) => dist(crash.get(a)!, D0) - dist(crash.get(b)!, D0))[0];
				if (other !== undefined) {
					C = crash.get(other)!;
					mark = other;
				}
			}
			let spot: Pt;
			if (C) {
				// Into him: a step in front of where he is coming, between him
				// and the rim.
				const u = unitVec(C, rim);
				spot = clampPt({ x: C.x + u.x * 1.8, y: C.y + u.y * 1.8 });
			} else {
				// Nobody to put a body on: a big in to the glass himself, eyes
				// on it; anybody else no further in than where a long one comes
				// down.
				const D0 = this.posOf(d);
				const u = unitVec(rim, D0);
				const big = (this.rank.get(d) ?? 4) >= 6;
				const k = Math.min(
					dist(D0, rim),
					big
						? 6 + 3 * draw()
						: LONG_BOARD[0] + (LONG_BOARD[1] - LONG_BOARD[0]) * draw(),
				);
				spot = apart(clampPt({ x: rim.x + u.x * k, y: rim.y + u.y * k }));
			}
			const there = this.goBy(d, spot, r0 + 80, meet, "run", undefined, BURST);
			if (C && there < until - 250) {
				this.act(d, "boxOut", Math.max(meet, there), until, { look: rim });
				boxed.set(mark, Math.max(meet, there));
				this.jostle(
					mark,
					d,
					Math.max(meet, there, inAt.get(mark) ?? until),
					until,
					rim,
					draw,
				);
			}
		}
		// The men coming in fight whoever is on them for position - one with
		// nobody on him just gets in there.
		for (const [man, from] of boxed) {
			const there = inAt.get(man) ?? until;
			if (there < until - 250) {
				this.act(man, "fight", Math.max(meet, there, from - 150), until, {
					look: rim,
				});
			}
		}
	}

	// THE BATTLE FOR POSITION, once they are into each other: the man coming
	// in leans one way and then the other, trying to get round, and the man
	// on him slides with him to stay in front - backing him off the glass
	// a little with each one - until the ball comes off the rim.
	private jostle(
		o: number,
		d: number,
		from: number,
		until: number,
		rim: Pt,
		draw: () => number = () => this.rng(),
	) {
		const rand = (lo: number, hi: number) => lo + draw() * (hi - lo);
		const O = this.posOf(o);
		const D = this.posOf(d);
		const u = unitVec(D, rim);
		const lat = { x: -u.y, y: u.x };
		let side: 1 | -1 = draw() < 0.5 ? 1 : -1;
		let off = 0;
		let back = 0;
		let t = from + rand(80, 200);
		const shove = (pid: number, to: Pt, t0: number, ms: number) => {
			const tr = this.track(pid);
			// (Still on his way in: no leaning on anybody until he is there.)
			if (!tr || (tr.moves.at(-1)?.t1 ?? 0) > t0 + 1) {
				return;
			}
			const m: Move = {
				t0,
				t1: t0 + ms,
				from: { ...this.posOf(pid) },
				to: clampPt(to),
				anim: "shuffle",
			};
			tr.moves.push(m);
			this.jostles.add(m);
			this.pos.set(pid, m.to);
		};
		while (t + 380 < until - 80) {
			// Around one side - or a second go at the same one.
			const was = off;
			off = side * rand(0.6, 1.25);
			if (Math.abs(off - was) < 0.4) {
				off = was + side * 0.5;
			}
			back = Math.min(1.4, back + rand(0.15, 0.4));
			const at = (P: Pt, k: number, extra = 0): Pt => ({
				x: P.x + lat.x * off * k - u.x * (back + extra),
				y: P.y + lat.y * off * k - u.y * (back + extra),
			});
			shove(o, at(O, 1, 0.1), t, 300);
			shove(d, at(D, 0.9), t + 80, 300);
			t += rand(480, 760);
			if (draw() < 0.75) {
				side = -side as 1 | -1;
			}
		}
		for (const pid of [o, d]) {
			this.free.set(pid, Math.max(this.free.get(pid) ?? 0, until));
		}
	}

	// Which way he finishes at the rim: mostly laid up off the glass or
	// rolled in off his fingertips; a big as often strong off two feet; now
	// and then scooped up underhand under a man coming to block it.
	private layupFor(shooter: number, t: number, plan: ShotPlan): AnimName {
		const big = (this.rank.get(shooter) ?? 4) >= 6;
		const r = hash01(shooter, Math.round(t / 7));
		const under = plan.kind === "block" || plan.kind === "foul";
		// (And each man his own touch: some roll it in off their fingers
		// more, some go up strong with it more.)
		const soft = 0.5 + hash01(shooter, 4409);
		const odds: [AnimName, number][] = big
			? [
					["powerLayup", 0.42 * (2 - soft)],
					["layup", 0.36],
					["fingerRoll", 0.12 * soft],
					["scoop", 0.14],
				]
			: [
					["layup", 0.36],
					["fingerRoll", 0.3 * soft],
					["powerLayup", 0.16 * (2 - soft)],
					["scoop", under ? 0.3 : 0.22],
				];
		const total = odds.reduce((a, [, w]) => a + w, 0);
		let x = r * total;
		for (const [anim, w] of odds) {
			x -= w;
			if (x <= 0) {
				return anim;
			}
		}
		return "layup";
	}

	// Done shadowing his man (see mark) by t, to go and do something of his
	// own: from wherever that had him, back to where the plan has him - so
	// what he does next starts from there.
	private unshadow(pid: number, t: number) {
		const tr = this.track(pid);
		const P = this.posOf(pid);
		const leave = Math.max(t - 450, this.free.get(pid) ?? 0);
		if (tr && leave < t) {
			tr.moves.push({
				t0: leave,
				t1: t,
				from: { ...P },
				to: { ...P },
				anim: "run",
			});
			this.free.set(pid, t);
		}
	}

	// The run that gets him to the ball - to pick it up, to poke it away - is
	// his own from `t` back a little (or from `since`, if that is further
	// back): not one reworked later to shadow his man (see mark) and left
	// short of the spot, the ball then jumping to or off his hand from feet
	// away - or put on the far side of the man with it, to come back through
	// him to get to it.
	private ownRun(pid: number, t: number, since = t - 2500) {
		const from = Math.min(t - 2500, since);
		for (const m of this.track(pid)?.moves ?? []) {
			if (m.t1 > from) {
				this.marking.delete(m);
			}
		}
	}

	// Whatever he was to do from `at` on, he never does: his runs cut off
	// there, his acts with them.
	private dropFrom(pid: number, at: number) {
		if (at >= (this.free.get(pid) ?? 0)) {
			return;
		}
		this.cutShort(pid, at);
		const tr = this.track(pid);
		if (tr) {
			tr.acts = tr.acts.filter((a) => a.t0 < at);
			for (const a of tr.acts) {
				a.t1 = Math.min(a.t1, at);
			}
		}
		this.free.set(pid, at);
	}

	// A help man a long way off the ball when it is to be taken off it: he
	// has sagged in off his man already - a step or two from it by `by` -
	// not run across the floor at the last moment.
	private sagIn(pid: number, at: Pt, by: number, within = 7) {
		const lead = 1400;
		const W = this.posAt(pid, by - lead);
		if (dist(W, at) <= within + 4) {
			return;
		}
		const u = unitVec(at, W);
		this.dropFrom(pid, by - lead);
		this.goBy(
			pid,
			clampPt({ x: at.x + u.x * within, y: at.y + u.y * within }),
			by - lead,
			by - 100,
			"run",
		);
	}

	// THE SEAL, for an entry pass: his back into his man down on the block,
	// wide, a hand up where he wants it - his man leaning on him, an arm up
	// to keep it from him - held until the pass comes. (The pass, timed off
	// him, comes in as he lets go.)
	private seal(
		pid: number,
		passer: number,
		t: number,
		dir: 1 | -1,
	): (caught: number) => void {
		const P = this.posOf(pid);
		const until = t + this.rand(550, 900);
		const guard = this.defenderOf(pid);
		const held: Act[] = [];
		if (guard !== undefined && dist(this.posOf(guard), P) < 14) {
			const there = this.goBy(
				guard,
				clampPt({ x: P.x + dir * BODY, y: P.y }),
				t - 300,
				t + 80,
				"run",
				-dir as 1 | -1,
			);
			this.act(guard, "fight", Math.max(there, t), until + 200, {
				face: -dir as 1 | -1,
				look: this.posOf(passer),
			});
			const g = this.track(guard)?.acts.at(-1);
			if (g?.anim === "fight") {
				held.push(g);
			}
			this.free.set(guard, Math.max(this.free.get(guard) ?? 0, until + 200));
		}
		this.act(pid, "postUp", t, until, {
			face: -dir as 1 | -1,
			look: this.posOf(passer),
		});
		this.free.set(pid, Math.max(this.free.get(pid) ?? 0, until));
		const me = this.track(pid)?.acts.at(-1);
		// However long the pass takes to come, he holds it until it does.
		return (caught: number) => {
			if (me?.anim === "postUp" && caught - 150 > me.t1) {
				me.t1 = caught - 150;
			}
			for (const a of held) {
				a.t1 = Math.max(a.t1, caught + 150);
			}
		};
	}

	// He comes out of his box-out (or out of fighting one) at t.
	private letGo(pid: number, t: number, free = false) {
		const tr = this.track(pid);
		let held = false;
		const acts = tr?.acts ?? [];
		for (let k = acts.length - 1; k >= 0; k--) {
			const a = acts[k]!;
			if ((a.anim === "boxOut" || a.anim === "fight") && a.t1 > t) {
				held = true;
				// (One he was only to get into after then, he never does.)
				if (a.t0 >= t) {
					acts.splice(k, 1);
				} else {
					a.t1 = t;
				}
			}
		}
		if (free && held && tr) {
			// Free from then - whatever else he was still doing aside.
			this.free.set(
				pid,
				Math.max(
					t,
					...tr.moves
						.filter((m) => !this.jostles.has(m) || m.t0 < t)
						.map((m) => Math.min(m.t1, this.jostles.has(m) ? t : m.t1)),
					...tr.acts.map((a) => a.t1),
				),
			);
		}
		// Out of the battle for position where he is at that moment.
		if (tr && tr.moves.some((m) => this.jostles.has(m) && m.t1 > t)) {
			tr.moves = tr.moves.filter((m) => !this.jostles.has(m) || m.t0 < t);
			const m = tr.moves.findLast((m) => this.jostles.has(m));
			if (m && m.t1 > t) {
				const f = (t - m.t0) / (m.t1 - m.t0);
				m.to = {
					x: m.from.x + (m.to.x - m.from.x) * f,
					y: m.from.y + (m.to.y - m.from.y) * f,
				};
				m.t1 = t;
				m.v1 = undefined;
			}
			const last = tr.moves.reduce<Move | undefined>(
				(a, m) => (!a || m.t1 >= a.t1 ? m : a),
				undefined,
			);
			if (last) {
				this.pos.set(pid, { ...last.to });
			}
		}
	}

	// The ball comes off the rim (or a blocker's hand) and goes to whatever the
	// next line says happened to it. Returns when that next line happens.
	private afterMiss(
		t: number,
		from: Pt3,
		team: Side,
		idx: number,
		blocked: boolean,
		hard = false,
		vel?: Pt3,
		// Where it really leaves from, if not `from` (a blocker's hand).
		start?: BallEnd,
	): number {
		const next = this.peek(idx, 3).find(
			(x) => x.e.type !== "sub" && x.e.type !== "foulOut",
		);
		const rim = rimPt(team);
		const dir = attackDir(team);
		if (
			next &&
			(next.e.type === "drb" || next.e.type === "orb") &&
			typeof next.e.pid === "number"
		) {
			const r = next.e.pid;
			if (vel) {
				return this.reboundOff(r, t, from, vel, team, blocked, start);
			}
			const toward = offRimWay(unitVec(rim, this.posOf(r)), dir);
			const k = blocked
				? this.rand(4, 7)
				: hard
					? this.rand(7, 11)
					: this.rand(4, 8);
			const catchAt = clampPt({
				x: rim.x + toward.x * k - (blocked ? dir * 2 : 0),
				y: rim.y + toward.y * k,
			});
			const catchT =
				t + (blocked ? 820 : hard ? this.rand(950, 1150) : this.rand(700, 900));
			// Out of his box-out (or his fight through one) and after it.
			this.letGo(r, Math.max(t - 450, this.free.get(r) ?? 0));
			const arrive = this.goBy(
				r,
				catchAt,
				t - 450,
				catchT - 380,
				"run",
				(rim.x >= catchAt.x ? 1 : -1) as 1 | -1,
			);
			this.goUpFor(
				r,
				Math.max(arrive, catchT - 420),
				catchAt,
				team,
				blocked ? 1.2 : 2.4,
				!blocked,
			);
			this.fly(t, catchT, from, { pid: r });
			this.hold(r, catchT, "hold");
			this.scramble(r, catchT, catchAt, t);
			return catchT;
		}
		if (next && next.e.type === "outOfBounds") {
			return this.missOut(t, from, team, next.e.t, blocked, vel, start);
		}
		// Nobody's: off the rim on its own, down to the floor first.
		const floor = vel ? this.toFloor(t, from, vel, start) : undefined;
		const t0 = floor?.t ?? t;
		// The period ran out, or nobody got it yet: it bounces free.
		const to = clampPt({
			x: rim.x - dir * this.rand(5, 10),
			y: 25 + this.rand(-8, 8),
		});
		const span = floor ? this.bounceSpan(floor.h0, 3, 1000) : 1000;
		this.bounce(
			t0,
			t0 + span,
			floor?.at ?? from,
			to,
			3,
			floor?.h0 ?? (blocked ? 1.4 : 3.2),
		);
		return t0 + span - 100;
	}

	// OUT OFF A MISS.
	//
	// Out of bounds is out off whoever touched it last - and the sim says
	// which side that was (`raw`, its team). Swatted straight out by the man
	// who blocked it, if that is his side; otherwise one of theirs, the
	// nearest where it comes down, gets only a hand to it - up for it in the
	// air, or as he gets to it on the bounce - and it is knocked away, out.
	// Returns when it is out.
	private missOut(
		t: number,
		from: Pt3,
		team: Side,
		raw: unknown,
		blocked: boolean,
		vel?: Pt3,
		start?: BallEnd,
	): number {
		const side: Side = raw === 0 ? 1 : raw === 1 ? 0 : other(team);
		const rim = rimPt(team);
		this.outOffMiss = true;
		// Which way it is going when he knocks it: on the way it was going,
		// turned - plainly - by his hand.
		const away = (at: Pt3): Pt => {
			// (The way it was really going as it got to him: from where it
			// left, if it has come any way since.)
			const h =
				dist(from, at) > 0.5
					? { x: at.x - from.x, y: at.y - from.y }
					: vel && Math.hypot(vel.x, vel.y) > 1
						? vel
						: undefined;
			const u = h
				? unitVec({ x: 0, y: 0 }, h)
				: dist(rim, at) > 0.5
					? unitVec(rim, at)
					: { x: attackDir(team), y: 0 };
			const a = (this.rng() < 0.5 ? -1 : 1) * this.rand(0.5, 1.1);
			return {
				x: u.x * Math.cos(a) - u.y * Math.sin(a),
				y: u.x * Math.sin(a) + u.y * Math.cos(a),
			};
		};
		if (
			blocked &&
			vel &&
			start &&
			"pid" in start &&
			this.teamOf(start.pid) === side
		) {
			// Swatted out: off his hand, down, and on out.
			const floor = this.toFloor(t, from, vel, start);
			if (outOfPlay(floor.at)) {
				return floor.t;
			}
			const u =
				Math.hypot(vel.x, vel.y) > 1
					? unitVec({ x: 0, y: 0 }, vel)
					: unitVec(rim, floor.at);
			const out = this.rollsOut(
				floor.at,
				dist(floor.at, this.outPoint(floor.at, u)) <= 30
					? u
					: unitVec(floor.at, this.nearestOut(floor.at)),
			);
			const span = this.bounceSpan(floor.h0, 2, 400 + dist(floor.at, out) * 40);
			this.bounce(floor.t, floor.t + span, floor.at, out, 2, floor.h0);
			return floor.t + span;
		}
		const knock = (at: Pt3, tt: number) => this.knockOut(tt, at, away(at));
		if (blocked && start && "pid" in start && this.teamOf(start.pid) !== side) {
			// Swatted straight back off the man nearest it on the other side -
			// the shooter, mostly - and off him out of bounds.
			const back = this.slots(side).sort(
				(a, b) => dist(this.posAt(a, t), from) - dist(this.posAt(b, t), from),
			)[0];
			if (back !== undefined) {
				// (Off his chest, where he is then: plainly off him, whoever
				// else is about.)
				const P = this.posAt(back, t);
				const u = unitVec(P, from);
				const at = { x: P.x + u.x * 0.1, y: P.y + u.y * 0.1, z: 4.2 };
				const hit = t + Math.max(140, (dist(from, at) / 30) * 1000);
				this.fly(t, hit, start, at);
				// Hands up to it as it comes back at him - and off them.
				this.act(back, "catch", hit - 200, hit + 160, {
					face: from.x >= P.x ? 1 : -1,
					look: { x: from.x, y: from.y },
				});
				return knock(at, hit);
			}
		}
		// Where it comes down, near enough, for who is nearest it.
		const lands: Pt = vel
			? (() => {
					const fall =
						(vel.z +
							Math.sqrt(
								vel.z * vel.z + 2 * GRAVITY * Math.max(0, from.z - BALL_R),
							)) /
						GRAVITY;
					return clampPt({
						x: from.x + vel.x * fall,
						y: from.y + vel.y * fall,
					});
				})()
			: from;
		const near = this.slots(side)
			.filter((p) => !(start && "pid" in start && start.pid === p))
			.sort((a, b) => dist(this.posOf(a), lands) - dist(this.posOf(b), lands));
		// (Not one still up with the shot it came off.)
		const r =
			near.find(
				(p) =>
					!this.track(p)?.acts.some(
						(a) =>
							a.t1 > t &&
							a.anim !== "boxOut" &&
							a.anim !== "fight" &&
							a.anim !== "rebound" &&
							a.anim !== "board",
					),
			) ?? near[0];
		if (r === undefined) {
			const floor = vel ? this.toFloor(t, from, vel, start) : undefined;
			return this.knockOut(floor?.t ?? t, floor?.at ?? from, away(from));
		}
		if (vel) {
			return this.reboundOff(r, t, from, vel, team, blocked, start, knock);
		}
		// Off the rim with no flight worked out: up for it where it comes off.
		const rp = this.posOf(r);
		const toward = offRimWay(unitVec(rim, rp), attackDir(team));
		const k = blocked ? this.rand(4, 7) : this.rand(4, 8);
		const spot = clampPt({
			x: rim.x + toward.x * (k + TIP_AT.f),
			y: rim.y + toward.y * (k + TIP_AT.f),
		});
		this.letGo(r, Math.max(t - 450, this.free.get(r) ?? 0));
		const arrive = this.goBy(
			r,
			spot,
			t - 450,
			t + this.rand(300, 500),
			"run",
			(rim.x >= spot.x ? 1 : -1) as 1 | -1,
		);
		const top = Math.max(arrive + BOARD_TOP, t + this.rand(700, 900));
		const peak = 2.2;
		this.goUpFor(r, top - BOARD_TOP, spot, team, peak, !blocked, "rebound");
		const back = unitVec(spot, rim);
		const at = {
			x: spot.x + back.x * TIP_AT.f,
			y: spot.y + back.y * TIP_AT.f,
			z: TIP_AT.u + peak,
		};
		this.fly(t, top, start ?? from, at);
		return knock(at, top);
	}

	// OUT IN THE HALF COURT.
	//
	// Some of a set, as long as the clock ran, and then the ball is gone
	// out of bounds off `touched`: off a defender's hand - reaching in on
	// the dribble, or a finger to a pass in the lane - or, if it is off
	// the offense, off the man with it, lost off his foot. `team` keeps
	// it (or gets it). Returns when it is out.
	private knockedOut(
		touched: Side,
		team: Side,
		T: number,
		gap: number | undefined,
		e: RawEvent,
	): number {
		const offense = other(touched) === team ? team : touched;
		const dev = this.develop(offense, T, gap, (entry) =>
			this.callForAny(
				entry,
				offense,
				gap,
				typeof e.clock === "number" ? e.clock : undefined,
			),
		);
		let t = dev.t;
		if (dev.run && (gap ?? 0) >= 4) {
			const run = dev.run;
			const steps = run.play.steps;
			const last =
				run.from + Math.floor(this.rng() * (steps.length - run.from)) - 1;
			// (Over half court first, however little of it he gets to.)
			for (let k = run.from; k < steps.length; k++) {
				const back =
					this.holder !== undefined &&
					this.inBackcourt(offense, this.posOf(this.holder));
				if (k > last && !back) {
					break;
				}
				t = this.runStep(run, steps[k]!, t, steps[k + 1], k);
			}
		}
		const h = this.holder;
		if (h === undefined) {
			const b = this.ballPoint();
			return this.knockOut(t, b, unitVec(b, this.nearestOut(b)));
		}
		const start = Math.max(t, this.free.get(h) ?? 0);
		const A = this.posOf(h);
		const dir = attackDir(offense);
		if (touched === offense) {
			// Lost off his own foot on the dribble.
			this.hold(h, start, "dribble");
			const tf = start + 380;
			const F = this.posAt(h, tf);
			const at = { x: F.x + dir * 0.7, y: F.y + 0.5, z: 0.9 };
			this.fly(tf - 110, tf, { pid: h }, at);
			this.react(h, "protest", tf + 500, 800);
			return this.knockOut(tf, at, unitVec(F, this.nearestOut(F)));
		}
		// A finger to the pass, in the lane - a pass of some length, that a
		// defender can get to on its way.
		const lane = (() => {
			if (this.rng() >= 0.7) {
				return undefined;
			}
			const release = start + RELEASE_MS;
			for (const q of this.slots(offense)) {
				const B = this.posOf(q);
				const d = dist(A, B);
				if (q === h || d < 10 || d > 30) {
					continue;
				}
				for (let f = 0.4; f <= 0.8; f += 0.05) {
					const I = clampPt({
						x: A.x + (B.x - A.x) * f,
						y: A.y + (B.y - A.y) * f,
					});
					const tI = release + passMs(d) * f;
					for (const m of this.slots(touched)) {
						const go = Math.max(start - 300, this.free.get(m) ?? 0);
						if (go + runMs(dist(this.posOf(m), I), SPRINT) + 60 <= tI) {
							return { B, I, tI, m, go, release };
						}
					}
				}
			}
			return undefined;
		})();
		if (lane) {
			const { B, I, tI, m, go, release } = lane;
			// Reading it: done shadowing his man as he goes for it.
			this.unshadow(m, go);
			this.act(h, "pass", start, release + 180, {
				face: B.x >= A.x ? 1 : -1,
				look: { ...B },
			});
			// He gets there with his hands out to it - a step off the lane, to
			// the side he came from: not stood on the line itself, where the
			// pass would go through him on its way to his hands.
			const u = unitVec(A, B);
			const M0 = this.posOf(m);
			const s = -u.y * (M0.x - I.x) + u.x * (M0.y - I.y) >= 0 ? 1 : -1;
			const side = { x: -u.y * s, y: u.x * s };
			this.goBy(
				m,
				clampPt({
					x: I.x + side.x * REACH_AT.f,
					y: I.y + side.y * REACH_AT.f,
				}),
				go,
				tI - 40,
				"run",
			);
			const r0 = tI - REACH_MS * REACH_HIT;
			this.act(m, "reach", r0, r0 + REACH_MS, { look: I });
			const I3 = { ...I, z: REACH_AT.u };
			this.fly(release, tI, { pid: h }, I3);
			// (Off line enough to see it was touched.)
			const a = (this.rng() < 0.5 ? -1 : 1) * this.rand(0.3, 0.75);
			return this.knockOut(tI, I3, {
				x: u.x * Math.cos(a) - u.y * Math.sin(a),
				y: u.x * Math.sin(a) + u.y * Math.cos(a),
			});
		}
		// Reaching in on his dribble, and it is knocked away off his hand -
		// by whoever of theirs is nearest him: not his own man sprinting the
		// length of the floor to get to it, the ball held up waiting on him.
		const away = (p: number) => dist(A, this.posAt(p, start));
		let m = this.defenderOf(h) ?? this.slots(touched)[0]!;
		const nearest = [...this.slots(touched)].sort(
			(p, q) => away(p) - away(q),
		)[0];
		if (
			nearest !== undefined &&
			away(m) > POKE_FAR &&
			away(nearest) < away(m)
		) {
			m = nearest;
		}
		this.hold(h, start, "dribble");
		// (Off whatever he was on his way to do, for the ball.)
		if ((this.free.get(m) ?? 0) > start) {
			this.cutShort(m, start);
		}
		// Still a way up the floor from him: he brings it on up at him, and
		// is met on the way.
		let S = start;
		let P = A;
		const D = this.posAt(m, start);
		if (dist(A, D) > POKE_FAR && (D.x - A.x) * dir > 0) {
			P = clampPt({
				x: A.x + (D.x - A.x) * POKE_MEET,
				y: A.y + (D.y - A.y) * POKE_MEET,
			});
			S = this.go(h, P, start, DRIBBLE * 0.8, "dribble", dir);
		}
		const toward = (P.x >= this.posOf(m).x ? 1 : -1) as 1 | -1;
		const mirrored = this.ballHandOf === h && this.ballHand === "R";
		// In front of him, at the length of his arm from the ball out on
		// the dribble - a foot and more off the man with it.
		// (Right on top of him, from out toward the floor.)
		const into =
			dist(P, this.posOf(m)) > 0.5
				? unitVec(P, this.posOf(m))
				: unitVec(P, { x: COURT_W / 2, y: COURT_H / 2 });
		const reach = pokeHand(mirrored).f + 1.3;
		// (All his way there this trip: where he pokes from is the side of
		// the man that way had him on.)
		this.ownRun(m, S, T);
		// (Set before he pokes, not mid-slide: a hand put out while he is still
		// pulling up off his feet is short of the ball, and the hand left on
		// it is the man's own. Off to meet him as he comes, if he does.)
		const hit =
			this.goBy(
				m,
				clampPt({ x: P.x + into.x * reach, y: P.y + into.y * reach }),
				start,
				S + 420,
				"slide",
				toward,
			) + 320;
		const t0 = hit - POKE_MS * POKE_HIT;
		this.act(m, "poke", t0, t0 + POKE_MS, {
			face: toward,
			look: { x: P.x, y: P.y },
			...(mirrored ? { mirror: true as const } : {}),
		});
		// Off the end of his hand, at full reach - facing the man he pokes
		// at (see bodyPoint in evaluate.ts).
		const M = this.posOf(m);
		const v = pokeHand(mirrored);
		const th = Math.atan2(P.y - M.y, P.x - M.x);
		const at = {
			x: M.x + v.f * Math.cos(th) + v.s * Math.sin(th),
			y: M.y + v.f * Math.sin(th) - v.s * Math.cos(th),
			z: Math.max(1.2, v.u),
		};
		this.fly(hit - 100, hit, { pid: h }, at);
		// Squirting away to his side and back - the side of him it was on,
		// clear of him and his hands (he faces the rim with it).
		const f = unitVec(P, rimPt(offense));
		const sideways =
			(at.x - P.x) * -f.y + (at.y - P.y) * f.x >= 0
				? { x: -f.y, y: f.x }
				: { x: f.y, y: -f.x };
		const back = this.rand(0.1, 0.5);
		return this.knockOut(
			hit,
			at,
			unitVec(
				{ x: 0, y: 0 },
				{ x: sideways.x - f.x * back, y: sideways.y - f.y * back },
			),
		);
	}

	// Knocked loose at `at` - a hand to it in the air, or off him on the
	// bounce - and on out of bounds along `u`, or, if that is the length of
	// the floor away, off the nearest line. Returns when it is out.
	private knockOut(t: number, at: Pt3, u: Pt, came?: Pt): number {
		// (Turned toward a nearer line, as little as it takes, if that one is
		// the length of the floor away - though never back onto the line it
		// `came` in on, if it came off somebody's hands: it was turned there.)
		let way: Pt | undefined;
		for (const a of [0, 0.3, -0.3, 0.6, -0.6, 0.9, -0.9, 1.2, -1.2]) {
			const v = {
				x: u.x * Math.cos(a) - u.y * Math.sin(a),
				y: u.x * Math.sin(a) + u.y * Math.cos(a),
			};
			if (came && v.x * came.x + v.y * came.y > Math.cos(0.3)) {
				continue;
			}
			if (dist(at, this.outPoint(at, v)) <= 30) {
				way = v;
				break;
			}
		}
		way ??= unitVec(at, this.nearestOut(at));
		const out = this.rollsOut(at, way);
		const h0 = Math.max(0.8, Math.min(3, at.z * 0.35));
		const span = this.bounceSpan(
			h0,
			2,
			450 + dist(at, out) * 38,
			Math.max(0, at.z - BALL_R),
		);
		this.bounce(t, t + span, at, out, 2, h0);
		// When it crosses the line (see the roll of a bounce in evaluate.ts):
		// the clock stops there, rolling on as it may.
		const far = dist(at, out);
		const frac =
			far > 0.01
				? Math.min(
						1,
						Math.max(0, (dist(at, this.outPoint(at, way)) - 1.6) / far),
					)
				: 1;
		return t + 0.85 * (1 - (1 - frac) ** (2 / 3)) * span;
	}

	// Where a loose ball going from `p` along `u` comes to rest: on over the
	// line, still rolling - a few feet past it, not stopped dead on it (and
	// short of the scorer's table and the front row).
	private rollsOut(p: Pt, u: Pt): Pt {
		const over = this.outPoint(p, u);
		const k = this.rand(1.2, 2.4);
		return { x: over.x + u.x * k, y: over.y + u.y * k };
	}

	// Off the rim on its own, down to the floor where it comes down: where
	// and when, and how high it comes up off it (what the hardwood leaves
	// it).
	private toFloor(
		t: number,
		from: Pt3,
		vel: Pt3,
		start?: BallEnd,
	): { at: Pt3; t: number; h0: number } {
		const fall =
			(vel.z +
				Math.sqrt(vel.z * vel.z + 2 * GRAVITY * Math.max(0, from.z - BALL_R))) /
			GRAVITY;
		// (Out over the line and on into the front row - the scorer's table,
		// the first seats - it stops there and drops, rather than carrying on
		// into the crowd.)
		const over = (p: number, v: number, hi: number) =>
			v > 1e-6
				? (hi + OUT_CAP - p) / v
				: v < -1e-6
					? (-OUT_CAP - p) / v
					: Infinity;
		const stop = Math.min(
			over(from.x, vel.x, COURT_W),
			over(from.y, vel.y, COURT_H),
		);
		if (stop > 0 && stop < fall) {
			const hit = {
				x: from.x + vel.x * stop,
				y: from.y + vel.y * stop,
				z: Math.max(
					BALL_R,
					from.z + vel.z * stop - 0.5 * GRAVITY * stop * stop,
				),
			};
			const t1 = t + stop * 1000;
			this.fly(t, t1, start ?? from, hit);
			const span = this.bounceSpan(0.5, 1, 300, hit.z - BALL_R);
			this.bounce(t1, t1 + span, hit, hit, 1, 0.5);
			return { at: { ...hit, z: BALL_R }, t: t1 + span, h0: 0.4 };
		}
		const at = {
			x: from.x + vel.x * fall,
			y: from.y + vel.y * fall,
			z: BALL_R,
		};
		this.fly(t, t + fall * 1000, start ?? from, at);
		const up = 0.7 * (GRAVITY * fall - vel.z);
		return {
			at,
			t: t + fall * 1000,
			h0: Math.max(0.4, Math.min(3.2, (up * up) / (2 * GRAVITY))),
		};
	}

	// How long a ball takes to bounce `hops` times from `h0` feet - in the
	// time gravity gives it - and roll a little after (at least `least`):
	// after falling `drop` feet to the floor first, if it is up in the air.
	private bounceSpan(
		h0: number,
		hops: number,
		least: number,
		drop = 0,
	): number {
		let s = drop > 0.005 ? Math.sqrt((2 * drop) / GRAVITY) : 0;
		for (let k = 0; k < hops; k++) {
			s += 2 * Math.sqrt((2 * h0 * 0.42 ** k) / GRAVITY);
		}
		return Math.max(least, (s / 0.85) * 1000);
	}

	// Up for a rebound from `at`, leaving the floor at `t` - and, a fight for
	// it, somebody from the other side goes up too.
	private goUpFor(
		r: number,
		t: number,
		at: Pt,
		team: Side,
		peak: number,
		contested: boolean,
		// Only a hand to it ("rebound"), not taken in.
		anim: "board" | "rebound" = "board",
	) {
		const rim = rimPt(team);
		// Back to him off his own shot that quickly, he goes up for it again
		// once it is out of his hands - not before.
		const shot = this.ball.findLast(
			(s) =>
				s.kind === "fly" &&
				"pid" in s.from &&
				s.from.pid === r &&
				s.t0 > t - 100,
		);
		if (shot) {
			t = Math.max(t, shot.t0 + 20);
		}
		// (Off any late hand up at the shot he was getting to: he goes up for
		// the ball instead.)
		const tr = this.track(r);
		if (tr) {
			tr.acts = tr.acts.filter(
				(a) => a.anim !== "contest" || a.t0 < t || a.t0 >= t + REBOUND_MS,
			);
		}
		// Up for it, and once he lands, chinned - elbows out - a beat before
		// he looks up the floor.
		this.act(r, anim, t, t + REBOUND_MS, {
			face: (rim.x >= at.x ? 1 : -1) as 1 | -1,
			look: { x: rim.x, y: rim.y },
			jump: [96 / REBOUND_MS, 704 / REBOUND_MS, peak],
			reach: true,
		});
		this.free.set(r, Math.max(this.free.get(r) ?? 0, t + REBOUND_MS));
		if (contested) {
			this.goUpWith(r, t + 60, at, rim);
		}
	}

	// Where he takes a ball coming down at c, up for it over [t0, t1]: out
	// in front of him, facing the rim (`back` the way out from it, `f` how
	// far out in front of him his hands are). With a man still up with his
	// own shot - or just down from it - right there, the shooter coming down
	// under it, from round the side of him instead, reaching out for it if
	// he has to: not up on top of him. Undefined if there is no taking it
	// there clear of him. (`flight`: how long, ms, it is in the air off the
	// rim till then.)
	private boardSpot(
		r: number,
		c: Pt,
		back: Pt,
		f: number,
		t0: number,
		t1: number,
		flight: number,
	): Pt | undefined {
		const up = [...this.tracks.values()].filter(
			(tr) =>
				tr.pid !== r &&
				tr.acts.some((a) => UP_WITH_SHOT.has(a.anim) && a.t0 < t1 && a.t1 > t0),
		);
		const clear = (p: Pt) =>
			up.every((tr) =>
				[t0, (t0 + t1) / 2, t1].every(
					(t) => dist(p, this.posAt(tr.pid, t)) >= OFF_HIS_SHOT,
				),
			);
		// (Only as far round as the ball can still come to his hands in the
		// time it is in the air off the rim - not swerving to them.)
		const own = clampPt({ x: c.x + back.x * f, y: c.y + back.y * f });
		const most = (BOARD_SHIFT_FTPS * flight) / 1000;
		for (const reach of [f, f + 0.5, f + 0.9]) {
			for (const deg of [0, 35, -35, 70, -70, 110, -110, 150, -150, 180]) {
				const th = (deg * Math.PI) / 180;
				const b = {
					x: back.x * Math.cos(th) - back.y * Math.sin(th),
					y: back.x * Math.sin(th) + back.y * Math.cos(th),
				};
				const spot = clampPt({ x: c.x + b.x * reach, y: c.y + b.y * reach });
				if (dist(spot, own) <= most && clear(spot)) {
					return spot;
				}
			}
		}
		return undefined;
	}

	// The nearest of theirs to where he goes up for it goes up with him: out
	// of his box-out, a step or two in first if he is not right there - only
	// one near enough to get a hand near it, not one way out on the floor
	// jumping at nothing. (Not one busy with something else - up with his own
	// shot, say, or on his way somewhere.)
	private goUpWith(r: number, up: number, at: Pt, rim: Pt) {
		const sees = up - RIVAL_STEP_MS;
		const busy = (p: number) => {
			const tr = this.track(p);
			return (
				!tr ||
				tr.acts.some(
					(a) =>
						a.anim !== "boxOut" &&
						a.anim !== "fight" &&
						a.t1 > sees &&
						a.t0 < up + 700,
				) ||
				tr.moves.some(
					(m) => !this.jostles.has(m) && m.t1 > sees && m.t0 < up + 700,
				)
			);
		};
		const far = (p: number) => dist(this.posAt(p, sees), at);
		const rival = this.slots(other(this.teamOf(r)))
			.filter((p) => !busy(p) && far(p) <= RIVAL_IN)
			.sort((a, b) => far(a) - far(b))[0];
		if (rival === undefined) {
			return;
		}
		let t = up;
		if (far(rival) > RIVAL_UP) {
			this.letGo(rival, sees, true);
			const u = unitVec(at, this.posOf(rival));
			const spot = clampPt({
				x: at.x + u.x * RIVAL_UP * 0.75,
				y: at.y + u.y * RIVAL_UP * 0.75,
			});
			const there = this.goBy(
				rival,
				spot,
				sees,
				up,
				"run",
				(at.x >= spot.x ? 1 : -1) as 1 | -1,
				BURST,
			);
			if (there > up + 200) {
				// Too late to go up for it: a hand at it as he gets there.
				this.act(rival, "reach", there - 150, there + 320, {
					look: { x: at.x, y: at.y },
				});
				return;
			}
			t = Math.max(up, there - 60);
		} else {
			this.letGo(rival, up);
		}
		this.act(rival, "rebound", t, t + 700, {
			look: { x: rim.x, y: rim.y },
			jump: [0.15, 0.9, 1.6],
		});
	}

	// OFF THE RIM ON ITS OWN.
	//
	// The ball comes down where its bounces send it (see physics.ts), and
	// the man who gets it goes after it from the moment it is coming off:
	// up for it where it drops into his reach - at the top of a jump just
	// as high as that takes - if he can get there in time; if not, it comes
	// down on the floor and he gathers it up off its bounce. Returns when he
	// has it.
	private reboundOff(
		r: number,
		t: number,
		from: Pt3,
		vel: Pt3,
		team: Side,
		// Off a blocker's hand, not the rim.
		blocked: boolean,
		// Where it really leaves from, if not `from`.
		leaves?: BallEnd,
		// He only gets a hand to it, and it is knocked away: where it is when
		// he does, and when - and on from there (returns when that is over).
		out?: (at: Pt3, t: number) => number,
	): number {
		const rim = rimPt(team);
		const hands = out ? TIP_AT : BOARD_AT;
		// He reads where it is coming down off the shot, and goes - once he
		// is done with what he was doing (up contesting it, say, or up with
		// the shot himself; out of a box-out he just comes), off any run he
		// was still on, after it. (A block nobody reads before it happens: he
		// goes once it is off the hand that blocked it.)
		let read = blocked ? t + READ_MS : t - REBOUND_READ;
		for (const a of [...(this.track(r)?.acts ?? [])].sort(
			(x, y) => x.t0 - y.t0,
		)) {
			if (
				a.anim !== "boxOut" &&
				a.anim !== "fight" &&
				a.t1 > read &&
				(a.t0 <= read || (UP_WITH_SHOT.has(a.anim) && a.t0 < t))
			) {
				read = a.t1;
			}
		}
		// (Out of the box-out as he reads it.)
		this.letGo(r, read, true);
		// On from wherever he was already going (in to where it comes down,
		// most likely), or - if that will not get him there in time - off
		// it, straight there.
		const plans = [
			{
				cut: false,
				R: this.posOf(r),
				start: Math.max(read, this.free.get(r) ?? 0),
			},
			...((this.free.get(r) ?? 0) > read
				? [{ cut: true, R: this.posAt(r, read), start: read }]
				: []),
		];
		for (let ms = 40; ms <= 1600; ms += 20) {
			const tau = ms / 1000;
			const h = from.z + vel.z * tau - 0.5 * GRAVITY * tau * tau;
			if (h > hands.u + 2.6) {
				continue;
			}
			if (h < hands.u + 0.3) {
				break;
			}
			// He takes it out in front of him, facing the rim.
			const c = { x: from.x + vel.x * tau, y: from.y + vel.y * tau };
			// (A hand to it while it is still over the floor.)
			if (out && outOfPlay(c)) {
				break;
			}
			const top = t + ms;
			const up = top - BOARD_TOP;
			const spot = this.boardSpot(r, c, unitVec(rim, c), hands.f, up, top, ms);
			if (!spot) {
				continue;
			}
			const f = (rim.x >= spot.x ? 1 : -1) as 1 | -1;
			// Off the floor in time to be at the top of his jump as it gets
			// there - and there by just before then: the last stride of his
			// way there carries him up into it. (A shuffle of a few inches is
			// quick, but no run is over in less than a quarter of a second.)
			const by = top - BOARD_CARRY;
			const plan = plans.find(({ cut, R, start }) => {
				const d = dist(R, spot);
				// On the spot as his run in gets there: up off the end of it,
				// the last stride carrying him into the jump.
				if (d < 0.3) {
					return start + (d < 0.02 ? 0 : SETTLE_MS) <= by;
				}
				const v0 = cut ? 0 : this.carried(r, spot, start, SPRINT);
				const need = Math.max(250, runMs(d, SPRINT, v0, BURST));
				return start <= up && start + need <= by;
			});
			if (!plan) {
				continue;
			}
			if (plan.cut) {
				this.cutShort(r, read);
			}
			this.letGo(r, plan.start);
			const d = dist(plan.R, spot);
			this.settleOn(
				r,
				spot,
				this.goBy(r, spot, plan.start, d < 0.3 ? up : by, "run", f, BURST),
			);
			this.goUpFor(
				r,
				up,
				spot,
				team,
				h - hands.u,
				!blocked,
				out ? "rebound" : "board",
			);
			if (out) {
				const at = { ...c, z: h };
				this.fly(t, top, leaves ?? from, at);
				return out(at, top);
			}
			this.fly(t, top, leaves ?? from, { pid: r });
			this.hold(r, top, "hold");
			this.scramble(r, top, spot, t);
			return top;
		}
		// Out of his reach before he can get there: down on the floor - and
		// he is on it as it comes up off the floor, taking it out of the air
		// on the hop, or scooping it up as it rolls away; never standing over
		// it, waiting for it to stop.
		if ((this.free.get(r) ?? 0) > read) {
			this.cutShort(r, read);
		}
		const start = Math.max(read, this.free.get(r) ?? 0);
		this.letGo(r, start);
		const R = this.posOf(r);
		const { at: F, t: land, h0 } = this.toFloor(t, from, vel, leaves);
		const caught = this.chaseDown(r, start, F, land, h0, vel, out);
		if (caught !== undefined) {
			if (!out) {
				this.scramble(r, caught, this.posOf(r), t);
			}
			return caught;
		}
		if (out) {
			// Nobody near it: on out off the floor where it lands.
			return out(F, land);
		}
		const hop = 2 * Math.sqrt((2 * h0) / GRAVITY);
		const G = clampPt({
			x: F.x + vel.x * hop * 0.8,
			y: F.y + vel.y * hop * 0.8,
		});
		const still = land + this.bounceSpan(h0, 1, 500);
		this.bounce(land, still, F, G, 1, h0);
		const run = runMs(dist(R, G), SPRINT, 0, BURST);
		const got = this.pickUp(
			r,
			Math.max(start, still - 150 - run),
			SPRINT,
			"hold",
			BURST,
		);
		this.scramble(r, got, this.posOf(r), t);
		return got + 150;
	}

	// Everybody else near it as it comes down plays it too - out of his
	// box-out as it comes loose (`loose`), a step or two at it, a beat behind
	// the man who gets it, theirs with a hand out at it - rather than stood
	// there watching. (Not one busy with anything else, or on his way
	// somewhere.)
	private scramble(r: number, tc: number, at: Pt, loose: number) {
		const t0 = Math.min(
			tc - SCRAMBLE_MS,
			Math.max(tc - SCRAMBLE_MAX, loose + READ_MS),
		);
		const mine = this.teamOf(r);
		for (const p of [...this.slots(0), ...this.slots(1)]) {
			const tr = this.track(p);
			if (
				p === r ||
				!tr ||
				tr.acts.some(
					(a) =>
						a.anim !== "boxOut" &&
						a.anim !== "fight" &&
						a.t1 > t0 &&
						a.t0 < tc + 300,
				) ||
				tr.moves.some(
					(m) =>
						(!this.jostles.has(m) && m.t1 > t0 && m.t0 < tc + 300) ||
						m.t0 >= tc + 300,
				)
			) {
				continue;
			}
			const P = this.posAt(p, t0);
			const d = dist(P, at);
			if (d > SCRAMBLE_FT) {
				continue;
			}
			this.letGo(p, t0, true);
			const face = (at.x >= P.x ? 1 : -1) as 1 | -1;
			if (d > SCRAMBLE_NEAR + 0.5) {
				const u = unitVec(P, at);
				const k = Math.min(SCRAMBLE_STEP, d - SCRAMBLE_NEAR);
				this.goBy(
					p,
					clampPt({ x: P.x + u.x * k, y: P.y + u.y * k }),
					t0,
					tc - 80,
					"run",
					face,
					BURST,
				);
			}
			if (this.teamOf(p) !== mine) {
				this.act(p, "reach", tc - 220, tc + 260, {
					face,
					look: { x: at.x, y: at.y },
				});
			}
		}
	}

	// A loose ball, down on the floor at F at `land` and coming up off it
	// `h0` feet, on the way it was going (`vel`, less what the floor takes):
	// he gets to it as soon as he can from `start`, flat out - into its path,
	// a stride short of it, the ball in front of him - and takes it out of
	// the air on the hop (about waist high), or scoops it up as it rolls.
	// The nearest of theirs is after it too, a step late. Returns when he has
	// it - undefined if he cannot get there inside a couple of seconds.
	private chaseDown(
		r: number,
		start: number,
		F: Pt3,
		land: number,
		h0: number,
		vel: Pt3,
		// Off his hands as he gets to it, not taken in (see reboundOff).
		out?: (at: Pt3, t: number) => number,
		// Nobody else's: no hurry, and nobody after it with him.
		easy = false,
	): number | undefined {
		const pace = easy ? RUN : SPRINT;
		const effort = easy ? 1 : BURST;
		const up = Math.sqrt(2 * GRAVITY * h0);
		const hop = (2 * up) / GRAVITY;
		const vx = vel.x * 0.75;
		const vy = vel.y * 0.75;
		// Where it is s seconds after it hits the floor: up off it and down,
		// then low and rolling, slowing.
		const ballAt = (s: number): Pt3 => {
			if (s <= hop) {
				return {
					x: F.x + vx * s,
					y: F.y + vy * s,
					z: BALL_R + up * s - 0.5 * GRAVITY * s * s,
				};
			}
			const k = hop + 0.9 * (1 - Math.exp(-(s - hop) / 0.9));
			return { x: F.x + vx * k, y: F.y + vy * k, z: BALL_R };
		};
		const R = this.posOf(r);
		for (let ms = 80; ms <= 2600; ms += 20) {
			const s = ms / 1000;
			const B = ballAt(s);
			const inAir = s < hop;
			if (inAir && (B.z < SNATCH_LOW || B.z > SNATCH_HIGH)) {
				continue;
			}
			// (Got before it gets to the line: he is not off the floor for it.)
			if (dist(inPlay(B), B) > 0.9) {
				break;
			}
			const toward = unitVec(B, R);
			const spot = inPlay({
				x: B.x + toward.x * (inAir ? SNATCH_OUT : PICKUP_REACH),
				y: B.y + toward.y * (inAir ? SNATCH_OUT : PICKUP_REACH),
			});
			const d = dist(R, spot);
			const tc = land + ms;
			const by = tc - (inAir ? 140 : 170);
			const v0 = this.carried(r, spot, start, pace);
			const need =
				d < 0.02
					? 0
					: d < 0.3
						? SETTLE_MS
						: Math.max(250, runMs(d, pace, v0, effort));
			if (start + need > by) {
				continue;
			}
			const f = (B.x >= spot.x ? 1 : -1) as 1 | -1;
			this.settleOn(r, spot, this.goBy(r, spot, start, by, "run", f, effort));
			if (out) {
				// A hand to it - and it squirts away off him.
				if (inAir) {
					this.act(r, "snatch", tc - 200, tc + 350, {
						look: { x: B.x, y: B.y },
					});
				} else {
					this.act(r, "pickup", tc - 350, tc + 150, {
						look: { x: B.x, y: B.y },
					});
				}
				if (inAir) {
					this.fly(land, tc, F, B);
				} else {
					this.bounce(land, tc, F, B, 1, h0);
				}
				this.free.set(r, Math.max(this.free.get(r) ?? 0, tc + 250));
				return out(B, tc);
			}
			if (inAir) {
				this.act(r, "snatch", tc - 200, tc + 350, { look: { x: B.x, y: B.y } });
				this.fly(land, tc, F, { pid: r });
			} else {
				this.act(r, "pickup", tc - 350, tc + 150, { look: { x: B.x, y: B.y } });
				this.bounce(land, tc, F, B, 1, h0);
			}
			this.hold(r, tc, "hold");
			this.free.set(r, Math.max(this.free.get(r) ?? 0, tc + 300));
			if (easy) {
				return tc;
			}
			// The nearest of theirs dives in after it, a step late - out of
			// his box-out, if he was still in one.
			const rival = this.slots(other(this.teamOf(r)))
				.filter(
					(p) =>
						!this.track(p)?.acts.some(
							(a) =>
								a.anim !== "boxOut" &&
								a.anim !== "fight" &&
								a.t1 > start &&
								a.t0 < tc + 300,
						),
				)
				.sort((a, b) => dist(this.posOf(a), B) - dist(this.posOf(b), B))[0];
			if (rival !== undefined && dist(this.posOf(rival), B) < 18) {
				this.letGo(rival, start + 120, true);
				const near = clampPt({
					x: B.x + unitVec(B, this.posOf(rival)).x * 2.4,
					y: B.y + unitVec(B, this.posOf(rival)).y * 2.4,
				});
				const there = this.goBy(
					rival,
					near,
					start + 120,
					tc + 160,
					"run",
					undefined,
					BURST,
				);
				// A hand at it as he gets there - unless he is too late to.
				if (there <= tc + 200) {
					this.act(rival, "reach", Math.max(tc - 60, there - 150), tc + 320, {
						look: { x: B.x, y: B.y },
					});
				}
			}
			return tc;
		}
		return undefined;
	}

	// ---- beats ----------------------------------------------------------------

	// Who comes down with the ball after the line at `idx`, if the next line
	// (subs aside) says somebody does.
	// Whether the line before this one (past any substitutions) is an
	// offensive board of theirs - `team` as the sim has it.
	private offBoard(idx: number, team: unknown): boolean {
		for (let i = idx - 1; i >= 0; i--) {
			const e = this.events[i];
			if (!e || !isLineItem(e) || e.type === "sub" || e.type === "foulOut") {
				continue;
			}
			return e.type === "orb" && e.t === team;
		}
		return false;
	}

	// Who of `team` other than `pid` has the next shot (subs aside) in close
	// - at the rim, in the post, a tip - if anybody does.
	private nextInClose(
		idx: number,
		team: Side,
		pid: number,
	): number | undefined {
		const next = this.peek(idx, 3).find(
			(x) => x.e.type !== "sub" && x.e.type !== "foulOut",
		)?.e;
		if (!next || typeof next.pid !== "number" || next.pid === pid) {
			return undefined;
		}
		const zone = ATTEMPT_ZONE[next.type] ?? resultOf(next.type)?.zone;
		return zone !== undefined &&
			zone !== "midRange" &&
			zone !== "three" &&
			this.teamOf(next.pid) === team
			? next.pid
			: undefined;
	}

	private reboundBy(idx: number): number | undefined {
		const next = this.peek(idx, 3).find(
			(x) => x.e.type !== "sub" && x.e.type !== "foulOut",
		);
		return next &&
			(next.e.type === "drb" || next.e.type === "orb") &&
			typeof next.e.pid === "number"
			? next.e.pid
			: undefined;
	}

	private peek(idx: number, n: number): { e: RawEvent; i: number }[] {
		const out: { e: RawEvent; i: number }[] = [];
		for (let i = idx + 1; i < this.events.length && out.length < n; i++) {
			const e = this.events[i];
			if (e && isLineItem(e)) {
				out.push({ e, i });
			}
		}
		return out;
	}

	// Crunch time: a close game in the last minutes of the fourth or of
	// overtime has the building on its feet - more so the closer it is.
	private tensionNow(i: number): number {
		const e = this.events[i];
		if (e?.type === "period" || e?.type === "overtime") {
			this.periodNo =
				typeof e.period === "number" ? e.period : this.periodNo + 1;
			this.overtime = e.type === "overtime";
		}
		if (!this.overtime && this.periodNo < 4) {
			return 0;
		}
		const clock =
			typeof e?.clock === "number" ? e.clock : (this.lastClock ?? 720);
		const margin = Math.abs(this.score[0] - this.score[1]);
		return clock <= 120 && margin <= 6
			? 1
			: clock <= 300 && margin <= 8
				? 0.5
				: 0;
	}

	// The seats: half of them still empty as the third quarter starts,
	// filling up over its first few minutes; and with the home team down big
	// in the fourth, emptying as the clock runs.
	private seatsNow(i: number): number {
		const e = this.events[i];
		const clock =
			typeof e?.clock === "number" ? e.clock : (this.lastClock ?? 720);
		let full = 1;
		if (!this.overtime && this.periodNo === 3) {
			full = 0.55 + 0.45 * Math.min(1, (720 - clock) / 240);
		}
		const down = this.score[0] - this.score[1];
		if (!this.overtime && this.periodNo >= 4 && down >= 16) {
			full =
				1 -
				0.45 * Math.min(1, (720 - clock) / 420) * Math.min(1, (down - 13) / 10);
		}
		return Math.round(full * 20) / 20;
	}

	private beat(i: number, type: string, actionStart: number, end: number) {
		const preStart = this.T;
		const a = Math.max(preStart, actionStart);
		let e = Math.max(a + 350, end);
		// A whistle gets its signal seen before the picture moves on.
		for (let k = this.fx.length - 1; k >= 0; k--) {
			const f = this.fx[k]!;
			if (f.t < preStart) {
				break;
			}
			if (f.kind === "whistle" && f.call && f.t <= e) {
				e = Math.max(e, f.t + WHISTLE_HOLD);
			}
		}
		this.beats.push({ i, type, preStart, actionStart: a, end: e });
		const level = this.tensionNow(i);
		if (level !== (this.tension.at(-1)?.[1] ?? 0)) {
			this.tension.push([preStart, level]);
		}
		const full = this.seatsNow(i);
		if (full !== (this.seats.at(-1)?.[1] ?? 1)) {
			this.seats.push([preStart, full]);
		}
		this.T = e;
	}

	handle(e: RawEvent, i: number) {
		const T = this.T;
		const raw = e.t;
		const d: Side | undefined = raw === 0 ? 1 : raw === 1 ? 0 : undefined;
		const gap = this.clockGap(e);
		const type = e.type;

		const zone = ATTEMPT_ZONE[type];
		const result = resultOf(type);

		if (zone && d !== undefined && typeof e.pid === "number") {
			// A shot goes up. Look ahead for what happens to it.
			const next = this.peek(i, 2)[0]?.e;
			const r = next ? resultOf(next.type) : undefined;
			const plan: ShotPlan =
				next && (next.type === "pfFG" || next.type === "pfTP")
					? { kind: "foul", fouler: next.pid }
					: r
						? {
								kind: r.kind,
								assist:
									typeof next?.pidAst === "number" ? next.pidAst : undefined,
								blocker: r.kind === "block" ? next?.pid : undefined,
								fouler:
									typeof next?.pidFoul === "number" ? next.pidFoul : undefined,
								finish: this.finishFor(next),
								defender:
									typeof next?.pidDefense === "number"
										? next.pidDefense
										: undefined,
							}
						: { kind: "miss" };
			if (typeof e.pidPass === "number") {
				plan.lobber = e.pidPass;
			}
			const res = this.peek(i, 1)[0];
			if (res) {
				plan.rebounder = this.reboundBy(res.i);
			}
			const heave =
				zone === "three" &&
				e.desperation === true &&
				typeof e.clock === "number" &&
				e.clock <= HEAVE_MAX_SECONDS
					? e.clock
					: undefined;
			const shot = this.stageShot(
				d,
				e.pid,
				zone,
				plan,
				T,
				gap,
				heave,
				typeof e.clock === "number" ? e.clock : undefined,
			);
			this.pending = {
				pid: e.pid,
				team: d,
				target: shot.target,
				dunk: shot.dunk,
				finish: plan.finish,
				zone,
				arrive: shot.arrive,
				...(shot.rim ? { rim: shot.rim } : {}),
			};
			this.beat(i, type, shot.gather, shot.decided);
			this.phase = "set";
			return;
		}

		if (result && d !== undefined && typeof e.pid === "number") {
			const shooterTeam = result.kind === "block" ? other(d) : d;
			let pending = this.pending;
			// No attempt line before it (the sim never logs one for a putback):
			// shoot it now, in this beat's lead-in. (A putback with no board of
			// theirs to put back - the period over in between, say - is a shot
			// at the rim like any other, worked for first.)
			if (!pending || (result.kind !== "block" && pending.pid !== e.pid)) {
				const shooter =
					result.kind === "block"
						? (this.holder ?? this.slots(shooterTeam)[0]!)
						: e.pid;
				const zone =
					(result.zone === "putBack" || result.zone === "tipIn") &&
					!this.offBoard(i, e.t)
						? "atRim"
						: result.zone;
				const plan: ShotPlan = {
					kind: result.kind,
					assist: typeof e.pidAst === "number" ? e.pidAst : undefined,
					blocker: result.kind === "block" ? e.pid : undefined,
					fouler: typeof e.pidFoul === "number" ? e.pidFoul : undefined,
					finish: this.finishFor(e),
					defender: typeof e.pidDefense === "number" ? e.pidDefense : undefined,
					rebounder: this.reboundBy(i),
				};
				const shot = this.stageShot(
					shooterTeam,
					shooter,
					zone,
					plan,
					T,
					gap,
					undefined,
				);
				pending = {
					pid: shooter,
					team: shooterTeam,
					target: shot.target,
					dunk: shot.dunk,
					finish: plan.finish,
					zone,
					arrive: shot.arrive,
					...(shot.rim ? { rim: shot.rim } : {}),
				};
				this.beatShotResult(e, i, result.kind, pending, shot.decided);
			} else {
				this.beatShotResult(e, i, result.kind, pending, T);
			}
			this.pending = undefined;
			return;
		}
		const inAir = this.pending;
		this.pending = undefined;

		switch (type) {
			case "drb":
			case "orb": {
				// The ball is already in his hands (the miss beat sent it there).
				const pid = e.pid as number;
				if (this.holder !== pid) {
					const tArr = this.pickUp(pid, T, RUN);
					this.beat(i, type, tArr + 150, tArr + 600);
				} else {
					// Not before he has it chinned.
					this.beat(
						i,
						type,
						T,
						Math.max(T + 650, (this.free.get(pid) ?? 0) + 60),
					);
				}
				const team = this.teamOf(pid);
				this.setOffense(T, team);
				const f = attackDir(team);
				this.turn(pid, T + 400, f);
				this.phase = type === "drb" ? "loose" : "set";
				if (type === "orb") {
					this.motionTeam = team;
					this.spreadOut(team, pid, T, this.nextInClose(i, team, pid));
					this.findMen(team, T);
				}
				break;
			}
			case "ft":
			case "missFt": {
				this.beatFreeThrow(e, i, type === "ft");
				break;
			}
			case "tov":
			case "stl": {
				this.beatTurnover(e, i);
				break;
			}
			case "pfNonShooting":
			case "pfBonus": {
				const fouler = e.pid as number;
				const team = other(this.teamOf(fouler));
				// Some of a set, then the whistle.
				const dev = this.develop(team, T, gap, (entry) =>
					this.callForAny(
						entry,
						team,
						gap,
						typeof e.clock === "number" ? e.clock : undefined,
					),
				);
				let t = dev.t;
				if (dev.run) {
					const run = dev.run;
					const steps = run.play.steps;
					const last =
						run.from + Math.floor(this.rng() * (steps.length - run.from)) - 1;
					// However little of it he gets to, the ball is brought over
					// half court first - never fouled where it was inbounded, the
					// man with it waiting on a break that never got going.
					for (let k = run.from; k < steps.length; k++) {
						const back =
							this.holder !== undefined &&
							this.inBackcourt(team, this.posOf(this.holder));
						if (k > last && !back) {
							break;
						}
						t = this.runStep(run, steps[k]!, t, steps[k + 1], k);
					}
					// (Cut short by the whistle: those left back up the floor
					// are on their way up as it goes, not stood out there.)
					this.trailUp(run, [], undefined, t, true);
				}
				const { hit, at: vp } = this.stageFoul(
					fouler,
					team,
					t,
					typeof e.pidShooting === "number" ? e.pidShooting : undefined,
					gap !== undefined && gap < 4 && this.foulingToStopClock(fouler, e),
				);
				this.effect("whistle", hit, { call: "foul", at: vp, team });
				if (this.rng() < 0.3) {
					this.react(fouler, "protest", hit + 420, 950);
				}
				this.beat(i, type, hit, hit + 900);
				this.inboundAt = {
					x: vp.x,
					y: vp.y < COURT_H / 2 ? -1.4 : COURT_H + 1.4,
				};
				this.phase = type === "pfBonus" ? "ft" : "inboundSide";
				break;
			}
			case "pfFG":
			case "pfTP": {
				// The shot already went up with the hand in his face (see stageShot).
				const rimTeam = other(this.teamOf(e.pid));
				this.effect("whistle", T, {
					call: "shootingFoul",
					at:
						typeof e.pidShooting === "number"
							? this.posOf(e.pidShooting)
							: undefined,
					team: rimTeam,
				});
				const target = inAir?.target ?? rimPt(rimTeam);
				const hits = Math.max(T + 200, inAir?.arrive ?? T + 800);
				const land = clampPt({
					x: rimX(rimTeam) - attackDir(rimTeam) * this.rand(4, 8),
					y: 25 + this.rand(-6, 6),
				});
				this.effect("clank", hits, { rim: rimTeam });
				// (Down off the rim from up there: as long as that takes.)
				this.bounce(hits, hits + 2000, target, land, 2, 2.5);
				if (this.rng() < 0.4) {
					this.react(
						e.pid,
						"protest",
						Math.max(T + 300, this.free.get(e.pid) ?? 0),
						950,
					);
				}
				this.beat(i, type, T, T + 1100);
				this.phase = "ft";
				break;
			}
			case "outOfBounds": {
				// Out off the side the sim says touched it last (`d`, its team):
				// the other side's ball. Already out if a miss sent it there;
				// otherwise it is knocked out in the half court.
				const touched: Side = d ?? other(this.offense);
				const nextTeam = other(touched);
				let t = T;
				if (!this.outOffMiss) {
					t = this.knockedOut(touched, nextTeam, T, gap, e);
				}
				this.outOffMiss = false;
				this.effect("whistle", t, {
					call: "out",
					at: { x: this.ballAt.x, y: this.ballAt.y },
					team: nextTeam,
				});
				const b = this.ballAt;
				this.inboundAt =
					b.x < 0 || b.x > COURT_W
						? {
								x: b.x < 0 ? -1.4 : COURT_W + 1.4,
								y: Math.min(46, Math.max(4, b.y)),
							}
						: { x: b.x, y: b.y < COURT_H / 2 ? -1.4 : COURT_H + 1.4 };
				this.offense = nextTeam;
				this.phase = "inboundSide";
				this.beat(i, type, t, t + 700);
				break;
			}
			case "jumpBall": {
				this.beatJumpBall(e, i);
				break;
			}
			case "sub": {
				this.beatSub(e, i, d);
				break;
			}
			case "timeout":
			case "endOfPeriod": {
				if (
					type === "endOfPeriod" &&
					this.peek(i, 1)[0]?.e.type === "gameOver"
				) {
					this.finalHorn(e, i, d ?? this.offense, gap);
					break;
				}
				// (The end of a period: whatever is left on the clock played out
				// first.)
				const at =
					type === "endOfPeriod"
						? this.playOutClock(e, d ?? this.offense, gap)
						: T;
				this.effect("whistle", at, { call: "stop", team: this.offense });
				// (Where play stopped: where it comes back in from.)
				const ballX = (
					this.holder !== undefined ? this.posAt(this.holder, at) : this.ballAt
				).x;
				// The ball goes to the huddle with a man of the side that has it
				// after the timeout - handed to him there and then, or picked up
				// - or, at the end of a period, with whoever has it: not left
				// lying out on the floor through the break.
				const B = this.ballPoint();
				const taker =
					type === "timeout"
						? [...this.slots(this.offense)].sort(
								(a, b) => dist(this.posOf(a), B) - dist(this.posOf(b), B),
							)[0]
						: undefined;
				const over = this.toHuddles(i, at, type === "timeout", taker);
				this.beat(i, type, at, over);
				const team = this.offense;
				this.inboundAt =
					type === "timeout" && e.advancesBall
						? { x: COURT_W / 2 + attackDir(team) * 19, y: -1.4 }
						: { x: Math.min(COURT_W - 4, Math.max(4, ballX)), y: -1.4 };
				this.phase = type === "timeout" ? "inboundSide" : "dead";
				break;
			}
			case "period":
			case "overtime": {
				// Still in the huddles (the ball with whoever took it there);
				// the next possession starts with the inbound at half court. A
				// period that ended on a shot at the horn (or with free throws
				// after it) has had no line for the break: the horn and the
				// walk off to the huddles, then, before this one starts.
				let over = T + 1100;
				if (this.beats.at(-1)?.type !== "endOfPeriod") {
					this.effect("whistle", T, { call: "stop", team: this.offense });
					over = this.toHuddles(i, T, false);
				} else if (this.holder !== undefined) {
					this.hold(this.holder, T, "hold");
				}
				this.inboundAt = { x: COURT_W / 2, y: -1.4 };
				this.phase = "inboundSide";
				this.beat(i, type, T, over);
				break;
			}
			case "injury": {
				const pid = e.pid as number;
				this.deadBall(T);
				this.effect("whistle", T + 200, {
					call: "stop",
					at: this.posOf(pid),
				});
				const { anim, down } = hurtFor(this.injuries.get(pid));
				const dur = down ? 3800 : 2200;
				this.act(pid, anim, T + 200, T + 200 + dur, {
					face: this.face.get(pid) ?? 1,
				});
				// A teammate or two over to him; down, they bend over him.
				const P = this.posOf(pid);
				const team = this.teamOf(pid);
				this.slots(team)
					.filter((q) => q !== pid)
					.sort((a, b) => dist(this.posOf(a), P) - dist(this.posOf(b), P))
					.slice(0, down ? 2 : 1)
					.forEach((q, j) => {
						const u = unitVec(P, this.posOf(q));
						const side = j === 0 ? 1 : -1;
						const at = clampPt({
							x: P.x + (u.x - u.y * side * 0.6) * 2.8,
							y: P.y + (u.y + u.x * side * 0.6) * 2.8,
						});
						const there = this.go(q, at, T + 500 + j * 250, JOG, "jog");
						this.lookAt(q, there, P);
						if (down) {
							this.act(q, "crouch", there, T + 200 + dur, { look: P });
						}
					});
				this.beat(i, type, T + 200, T + dur);
				this.phase = "inboundSide";
				break;
			}
			case "gameOver": {
				const winner: Side = this.score[0] > this.score[1] ? 0 : 1;
				const loser = other(winner);
				const margin = Math.abs(this.score[0] - this.score[1]);
				// The man with it at the horn, if his side won it: up into the
				// air with it, as high as he can throw it. (Lost, he lets it go.)
				const h = this.holder;
				if (h !== undefined && this.teamOf(h) === winner) {
					const P = this.posAt(h, T);
					const t0 = Math.max(T + 150, this.free.get(h) ?? 0);
					const up = t0 + RELEASE_MS + OVERHEAD_WIND;
					const apex = clampPt({
						x: P.x + this.rand(-2, 2),
						y: P.y + this.rand(-2, 2),
					});
					const top = { ...apex, z: GAME_BALL_UP };
					const fall = Math.sqrt((2 * (GAME_BALL_UP - 5)) / GRAVITY) * 1000;
					this.act(h, "passOverhead", t0, up + 180, { look: apex });
					this.fly(up, up + fall, { pid: h }, top);
					this.bounce(
						up + fall,
						up + fall + 4400,
						top,
						clampPt({
							x: apex.x + this.rand(-5, 5),
							y: apex.y + this.rand(-5, 5),
						}),
						3,
						5,
					);
					this.free.set(h, Math.max(this.free.get(h) ?? 0, t0 + 450));
				} else {
					this.deadBall(T);
				}
				// A game-winner: the winning side's last basket, in the last
				// seconds of a game it decided.
				let hero: number | undefined;
				for (let k = i - 1; k >= 0 && k > i - 16; k--) {
					const ev = this.events[k];
					if (!ev || !isLineItem(ev)) {
						continue;
					}
					if (resultOf(ev.type)?.kind === "make") {
						const side = ev.t === 0 ? 1 : ev.t === 1 ? 0 : undefined;
						if (
							side === winner &&
							typeof ev.pid === "number" &&
							typeof ev.clock === "number" &&
							ev.clock <= 3 &&
							margin <= 3
						) {
							hero = ev.pid;
						}
						break;
					}
				}
				const crowd = { x: COURT_W / 2, y: COURT_H + 20 };
				let done = T + 2200;
				// A playoff game: won, a series won, the championship.
				const stakes = this.stakes;
				const clinch = !!stakes && stakes.won[winner] + 1 >= stakes.toWin;
				const title = clinch && stakes.finals;
				const mob = clinch && hero === undefined;
				// The winners: all over the man who won it - mobbed where he
				// stands - or, a close one, celebrating where the buzzer found
				// them; a blowout, a clap and that is all. The losers: hands on
				// their hips, or - beaten at the buzzer - doubled over.
				// A series won and not at the buzzer: together at center court,
				// all over each other.
				const middle = { x: COURT_W / 2, y: COURT_H / 2 };
				this.slots(winner).forEach((pid, j) => {
					const from = Math.max(T + 150 + j * 80, this.free.get(pid) ?? 0);
					if (mob) {
						const a = (j / 5) * Math.PI * 2;
						const there = this.go(
							pid,
							{
								x: middle.x + Math.cos(a) * 2.2,
								y: middle.y + Math.sin(a) * 2.2,
							},
							from,
							SPRINT,
							"run",
						);
						const long = title ? 4200 : 2600;
						this.react(pid, "celebrate", there, long, middle);
						done = Math.max(done, there + long);
					} else if (
						hero !== undefined &&
						pid !== hero &&
						this.team.has(hero)
					) {
						const H = this.posOf(hero);
						const a = (j / 5) * Math.PI * 2;
						const there = this.go(
							pid,
							clampPt({
								x: H.x + Math.cos(a) * 2.4,
								y: H.y + Math.sin(a) * 2.4,
							}),
							from,
							SPRINT,
							"run",
						);
						const long = clinch ? (title ? 4200 : 2600) : 1800;
						this.react(pid, "celebrate", there, long, H);
						done = Math.max(done, there + long);
					} else if (pid === hero) {
						const long = clinch ? (title ? 3300 : 1700) : 1800;
						this.react(pid, "flex", from, 900, crowd);
						this.react(pid, "celebrate", from + 900, long, crowd);
						done = Math.max(done, from + 900 + long);
					} else if (margin <= (stakes ? 14 : 6)) {
						// (Any playoff win but a rout.)
						this.react(pid, "celebrate", from, 1700, crowd);
					} else {
						this.react(pid, "clap", from, 1200, crowd);
					}
				});
				this.slots(loser).forEach((pid, j) => {
					const from = Math.max(T + 350 + j * 90, this.free.get(pid) ?? 0);
					if (hero !== undefined && j < 2) {
						this.react(pid, "crouch", from, 1800);
					} else if (this.rng() < 0.75) {
						this.react(pid, "hips", from, 1500);
					}
				});
				// Then the handshake line at center court: each down the other
				// team's line, a dap with one man and then the next.
				const cx = COURT_W / 2;
				const spot = (side: Side, k: number): Pt => ({
					x: cx + (side === 0 ? -1 : 1) * (LOW_FIVE_APART / 2),
					y: 14 + k * 4.4,
				});
				const W = this.slots(winner);
				const L = this.slots(loser);
				let t1 = done + 300;
				for (const round of [0, 1]) {
					let met = t1;
					const fw = (winner === 0 ? 1 : -1) as 1 | -1;
					const arrived = new Map<number, number>();
					W.forEach((pid, j) => {
						const k = (j + round) % W.length;
						const S = spot(winner, k);
						let off = t1 + j * 60;
						if (k < j) {
							// (Each on down the line a place - and the man at the end
							// of it round the back of his own line to the head of it,
							// not up through it.)
							const back = (winner === 0 ? -1 : 1) * 3;
							const P = this.posOf(pid);
							off = this.go(pid, { x: S.x + back, y: P.y }, off, JOG, "run");
							off = this.go(pid, { x: S.x + back, y: S.y }, off, JOG, "run");
						}
						const a = this.go(pid, S, off, WALK * 1.4, "walk");
						this.turn(pid, a, fw);
						arrived.set(pid, a);
						met = Math.max(met, a);
					});
					L.forEach((pid, j) => {
						const a = this.go(
							pid,
							spot(loser, j),
							t1 + j * 60,
							WALK * 1.4,
							"walk",
						);
						this.turn(pid, a, -fw as 1 | -1);
						arrived.set(pid, a);
						met = Math.max(met, a);
					});
					// (Turned to each other before the hands come up.)
					met += 250;
					const n = Math.min(W.length, L.length);
					for (let j = 0; j < n; j++) {
						const w = W[(j - round + W.length) % W.length]!;
						const l = L[j]!;
						const anim = (j + round) % 2 ? "lowFive" : "highFive";
						for (const [p, them] of [
							[w, spot(loser, j)],
							[l, spot(winner, j)],
						] as const) {
							const a = arrived.get(p) ?? met;
							if (met + 40 - a > 120) {
								this.act(p, "waitFive", a, met + 40, { look: them });
							}
							this.act(p, anim, met + 40, met + 560, { look: them });
						}
					}
					t1 = met + 700;
				}
				this.effect("cheer", T, { team: winner });
				if (stakes) {
					// The bench and the building up for it.
					this.effect("roar", T, { team: winner });
				}
				if (clinch) {
					this.effect("clinch", T, { team: winner, big: title });
				}
				// Confetti, for the home team winning it all - at home.
				if (title && winner === 1) {
					this.effect("confetti", T + 400, { team: winner });
				}
				// The last look, the hands all shaken: the whole building from
				// up high - the crowd as the result left it, the board over
				// center court with the final score - and the picture holds on
				// it once the game is over, not on the two lines stood still in
				// the middle of the floor. (It never ends: the game does first.)
				const over = t1 + (title ? 5000 : 400);
				this.shots.push({
					t0: over,
					t1: over + LAST_LOOK_MS + 60_000,
					kind: "wide",
					stretch: false,
				});
				this.beat(i, type, T, over + LAST_LOOK_MS);
				this.phase = "dead";
				break;
			}
			case "shootoutStart": {
				for (const t of [0, 1] as const) {
					const spots = huddleSpots(t);
					this.slots(t).forEach((pid, j) => {
						const at = this.go(
							pid,
							spots[j] ?? spots[0]!,
							T + j * 60,
							WALK,
							"walk",
						);
						this.lookAt(pid, at, { x: benchX(t), y: HUDDLE_Y });
					});
				}
				this.deadBall(T);
				this.phase = "shootout";
				this.beat(i, type, T, T + 1800);
				break;
			}
			case "shootoutTeam": {
				const pid = e.pid as number;
				const at = spot(1, 26, 25);
				const tArr = this.go(pid, at, T, JOG, "walk", 1);
				this.fly(
					Math.max(T, tArr - 300),
					Math.max(T + 300, tArr),
					this.ballOrigin(),
					{ pid },
				);
				this.hold(pid, tArr, "dribble");
				this.beat(i, type, T, tArr + 300);
				break;
			}
			case "shootoutShot": {
				const pid = e.pid as number;
				const P1 = this.posOf(pid);
				this.hold(pid, T, "hold");
				this.act(pid, "shoot", T + 100, T + 1100, {
					face: 1,
					look: { x: rimX(1), y: 25 },
					jump: [JUMPER.off, JUMPER.land, 1.4],
				});
				const release = T + 100 + 1000 * JUMPER.release;
				const made = e.made === true;
				const finish = this.finishFor(e);
				const edge = { x: rimX(1) - RIM_R - 0.05, y: 25, z: RIM_Z + 0.12 };
				const target = made
					? finish === "rattle"
						? edge
						: rimPt(1, 0.35)
					: finish === "airball"
						? { x: rimX(1) - 2.6, y: 25 + this.rand(-1, 1), z: RIM_Z - 1.5 }
						: edge;
				const flight = 620 + dist(P1, rimPt(1)) * 22;
				this.fly(release, release + flight, { pid }, target);
				const at = release + flight;
				if (made) {
					let t = at;
					if (finish === "rattle") {
						this.effect("clank", at, { rim: 1 });
						({ t } = this.rollAround(1, at, edge));
						this.fly(t, t + 90, this.ballAt, rimPt(1, 0.2));
						t += 90;
					}
					this.fly(t, t + 140, rimPt(1, 0.2), rimPt(1, -2.3));
					this.bounce(
						t + 140,
						t + 800,
						rimPt(1, -2.3),
						{ x: rimX(1) - 3, y: 26 },
						2,
						1.8,
					);
					this.effect("swish", t + 20, { rim: 1 });
				} else if (finish === "airball") {
					this.bounce(
						at,
						at + 900,
						target,
						{ x: rimX(1) + 2, y: 25 + this.rand(-6, 6) },
						2,
						1.6,
					);
				} else {
					this.effect("clank", at, { rim: 1 });
					let from: Pt3 = target;
					let t = at;
					if (finish === "rimOut") {
						({ t, at: from } = this.rollAround(1, at, edge));
					}
					this.bounce(
						t,
						t + 1000,
						from,
						{
							x: rimX(1) - (finish === "brick" ? 14 : 9),
							y: 25 + this.rand(-8, 8),
						},
						2,
						finish === "brick" ? 4.5 : 3,
					);
				}
				this.beat(i, type, at, at + 900);
				break;
			}
			default: {
				// A line with nothing to stage (Elam target, a foul-out, a shootout
				// tie): hold the picture long enough to read it.
				this.beat(i, type, T, T + 900);
			}
		}
	}

	// The ball goes around the iron before it decides: "rims out", "rolls out",
	// "rattles around". Returns when and where it leaves the rim.
	private rollAround(team: Side, t: number, from: Pt3): { t: number; at: Pt3 } {
		const rim = rimPt(team);
		const a0 = Math.atan2(from.y - rim.y, from.x - rim.x);
		const turn = (this.rng() < 0.5 ? 1 : -1) * this.rand(2.4, 4.4);
		const steps = 5;
		let at = from;
		for (let k = 1; k <= steps; k++) {
			const a = a0 + (turn * k) / steps;
			const p = {
				x: rim.x + Math.cos(a) * RIM_R,
				y: rim.y + Math.sin(a) * RIM_R,
				z: RIM_Z + 0.42,
			};
			this.fly(t, t + 95, at, p);
			at = p;
			t += 95;
		}
		return { t, at };
	}

	private beatShotResult(
		e: RawEvent,
		i: number,
		kind: "make" | "miss" | "block",
		shot: {
			pid: number;
			team: Side;
			target: Pt3;
			dunk: boolean;
			finish?: Finish;
			zone?: Zone;
			rim?: AtRim;
		},
		at: number,
	) {
		const team = shot.team;
		const rim = rimPt(team);
		// Played out at the rim: every time it hit the iron or the glass on
		// the way to deciding, the basket shakes with it.
		const play = shot.dunk ? undefined : shot.rim;
		if (play) {
			this.rimClanks(play, team);
		}
		const dir = attackDir(team);
		if (kind === "make") {
			const top = rimPt(team, 0.35);
			const under = rimPt(team, -2.3);
			if (shot.dunk) {
				this.fly(at, at + 70, { pid: shot.pid }, top);
				this.effect("dunk", at + 50, {
					rim: team,
					big:
						shot.finish === "poster" ||
						shot.zone === "tipIn" ||
						typeof e.pidFoul === "number",
				});
			}
			const t0 = shot.dunk ? at + 70 : at;
			let settle = clampPt({
				x: rim.x - dir * this.rand(1.5, 4),
				y: 25 + this.rand(-3, 3),
			});
			// Not at the feet of the man who put it in, coming down under it:
			// out the other side of the rim from him.
			const S = this.posOf(shot.pid);
			if (dist(settle, S) < 3) {
				const side = S.y >= 25 ? -1 : 1;
				settle = clampPt({
					x: settle.x,
					y: 25 + side * Math.max(2.5, Math.abs(settle.y - 25)),
				});
			}
			// Out of the net and down to the floor, a bounce, and taken out
			// (see inboundAfterMake). Slammed, it goes down through the net
			// hard.
			const slam = shot.dunk
				? playShot(team, top, { x: -dir * 1.2, y: 0, z: -15 })
				: undefined;
			if (play) {
				this.dropThrough(play.t0, play.found.play, settle);
			} else if (slam?.made) {
				this.pushBall({
					kind: "path",
					t0,
					t1: t0 + (slam.pts.length / 3 - 1) * SAMPLE_MS,
					pts: slam.pts,
					v0: { x: -dir * 1.2, y: 0, z: -15 },
					roll0: 0,
					spin: 0,
				});
				this.dropThrough(t0, slam, settle);
			} else {
				this.fly(t0, t0 + 140, top, under);
				this.bounce(t0 + 140, t0 + 1350, under, settle, 1, 2.2);
			}
			// Worth a replay (a dunk is shown again for its own - see the
			// dunk's): a three in a tight finish, anything at the buzzer, and
			// one through contact.
			const buzzer = typeof e.clock === "number" && e.clock < BUZZER_SECS;
			this.effect("swish", t0 + 20, {
				rim: team,
				...(!shot.dunk &&
				(buzzer ||
					typeof e.pidFoul === "number" ||
					(shot.zone === "three" && (this.tension.at(-1)?.[1] ?? 0) >= 1))
					? { big: true }
					: {}),
			});
			this.effect("cheer", t0 + 60, { team });
			let end = at + 1100;
			if (typeof e.pidFoul === "number") {
				this.effect("whistle", at + 120, {
					call: "andOne",
					at: this.posOf(shot.pid),
					team,
				});
				// (Up with him already, the man he dunked on: that was the foul.)
				if (
					!this.track(e.pidFoul)?.acts.some(
						(a) => a.t1 > at - 150 && a.t0 < at + 300,
					)
				) {
					this.act(e.pidFoul, "reach", at - 150, at + 300);
				}
				end = at + 1400;
			}
			// Left all alone for it from out there, the man who had him turns
			// to a teammate as it drops - that was yours - half the time.
			if (!shot.dunk && (shot.zone === "three" || shot.zone === "midRange")) {
				const rel = this.ball.findLast(
					(s) =>
						s.kind === "fly" &&
						"pid" in s.from &&
						s.from.pid === shot.pid &&
						s.t0 <= at,
				)?.t0;
				const theirs = this.slots(other(team));
				const S = rel === undefined ? undefined : this.posAt(shot.pid, rel);
				const near =
					rel === undefined || !S
						? Infinity
						: Math.min(...theirs.map((p) => dist(this.posAt(p, rel), S)));
				const d = this.defenderOf(shot.pid);
				if (near >= WIDE_OPEN && d !== undefined && theirs.includes(d)) {
					const D = this.posAt(d, at);
					const mate = theirs
						.filter((p) => p !== d)
						.sort(
							(a, b) => dist(this.posAt(a, at), D) - dist(this.posAt(b, at), D),
						)[0];
					if (mate !== undefined) {
						this.gesture(d, "point", at + 200, at + 1100, mate, 0.5);
					}
				}
			}
			// The big ones bring the bench up - and the scorer shows it.
			const andOne = typeof e.pidFoul === "number";
			const big = shot.dunk || shot.zone === "three" || andOne;
			if (big) {
				this.effect("roar", t0 + 100, {
					team,
					what: shot.dunk ? "dunk" : shot.zone === "three" ? "three" : "andOne",
				});
			}
			const free = Math.max(at + 450, this.free.get(shot.pid) ?? 0);
			const assist =
				typeof e.pidAst === "number" && e.pidAst !== shot.pid
					? e.pidAst
					: undefined;
			if (!shot.dunk && !andOne && assist !== undefined && this.rng() < 0.55) {
				// A finger at the man who found him.
				this.react(shot.pid, "point", free + 100, 800, this.posOf(assist));
			} else if (big || this.rng() < 0.3) {
				// (A three: his own way of it, every time - a finger at the
				// bench, or both arms up. A dunk or an and-one: a roar or a
				// pound of the chest, for a man whose thing that is, more
				// often than not.)
				const own = this.bigFinish(shot.pid, at);
				const cel: AnimName = shot.dunk
					? this.rng() < 0.5
						? (own ?? "flex")
						: (own ?? "celebrate")
					: andOne
						? (own ?? "flex")
						: shot.zone === "three"
							? this.threeCelebration(shot.pid)
							: "point";
				// Or, after a dunk or an and-one, the nearest man on his team
				// comes over and they meet in the air, chest to chest.
				const S = this.posOf(shot.pid);
				const mate =
					(shot.dunk || andOne) && hash01(shot.pid, at) < 0.45
						? this.slots(team)
								.filter((p) => p !== shot.pid)
								.map((p) => ({ p, d: dist(this.posOf(p), S) }))
								.sort((a, b) => a.d - b.d)[0]
						: undefined;
				if (mate && mate.d < 16) {
					this.chestBump(shot.pid, mate.p, free + 80);
				} else {
					this.act(shot.pid, cel, free + 100, free + 900, {
						face: -dir as 1 | -1,
					});
				}
			}
			// The man who fouled him wants to know what for; the man he dunked
			// on stands there, hands on his hips.
			if (andOne && this.rng() < 0.5) {
				this.react(e.pidFoul, "protest", at + 450, 1000);
			}
			const victim =
				shot.finish === "poster" && typeof e.pidDefense === "number"
					? e.pidDefense
					: undefined;
			if (victim !== undefined && this.rng() < 0.6) {
				this.react(
					victim,
					"hips",
					Math.max(at + 500, this.free.get(victim) ?? 0),
					1300,
				);
			}
			this.beat(i, e.type, at, end);
			this.offense = other(team);
			this.phase = typeof e.pidFoul === "number" ? "ft" : "inboundBase";
			return;
		}
		if (kind === "miss") {
			if (play) {
				// Off the rim (see shootAtRim), and on from where it leaves it.
				const end = play.found.play.end;
				const next = this.afterMiss(
					play.t0 + end.t,
					end.p,
					team,
					i,
					false,
					shot.finish === "brick",
					end.v,
				);
				this.beat(i, e.type, at, next);
				this.phase = "loose";
				return;
			}
			this.effect("clank", at, { rim: team });
			let from = shot.target;
			let t = at;
			if (
				!shot.dunk &&
				(shot.finish === "rimOut" || shot.finish === "rollOut")
			) {
				({ t, at: from } = this.rollAround(team, at, from));
				this.effect("clank", t, { rim: team });
			}
			const next = this.afterMiss(
				t,
				from,
				team,
				i,
				false,
				shot.dunk || shot.finish === "brick",
			);
			this.beat(i, e.type, at, next);
			this.phase = "loose";
			return;
		}
		// Blocked: swatted off his hand - back out the way it came, and down -
		// and on, bouncing, to whoever gets it (see afterMiss).
		this.effect("block", at, { team: other(team) });
		const sp = this.posOf(shot.pid);
		const bp = this.posOf(e.pid);
		const back = unitVec(rim, sp);
		const off = this.rand(-0.45, 0.45);
		// Only knocked loose, if they get it back and put it straight back up.
		const after = this.peek(i, 4).filter(
			(x) => x.e.type !== "sub" && x.e.type !== "foulOut",
		);
		const again =
			after[0]?.e.type === "orb" &&
			[
				ATTEMPT_ZONE[after[1]?.e.type ?? ""],
				resultOf(after[1]?.e.type ?? "")?.zone,
			].some((z) => z === "putBack" || z === "tipIn");
		let speed = again ? this.rand(3, 6) : this.rand(11, 19);
		const vz = again ? this.rand(-2, 3) : this.rand(-7, 1);
		const B0 = {
			x: bp.x + (sp.x - bp.x) * 0.3,
			y: bp.y + (sp.y - bp.y) * 0.3,
			z: 9.8,
		};
		let u = { x: back.x - back.y * off, y: back.y + back.x * off };
		// Knocked away toward the man who comes up with it, down on the floor
		// short of him: he goes after it once it is off the hand (see
		// reboundOff), and gets to it on the bounce - not waiting where it
		// comes down before it is ever blocked.
		const r =
			(after[0]?.e.type === "drb" || after[0]?.e.type === "orb") &&
			typeof after[0].e.pid === "number"
				? after[0].e.pid
				: undefined;
		if (r !== undefined) {
			const R = this.posAt(r, at);
			const D = dist(B0, R);
			if (D > 3 && r !== e.pid) {
				const fall =
					(vz + Math.sqrt(vz * vz + 2 * GRAVITY * (B0.z - BALL_R))) / GRAVITY;
				const wide = this.rand(-0.25, 0.25);
				const d = unitVec(B0, R);
				u = { x: d.x - d.y * wide, y: d.y + d.x * wide };
				speed = Math.min(22, Math.max(5, (D - 3) / fall));
			} else {
				// (Right there - or his own block: only knocked down, not away.)
				speed = Math.min(speed, this.rand(3, 6));
			}
		}
		const next = this.afterMiss(
			at,
			B0,
			team,
			i,
			true,
			false,
			{ x: u.x * speed, y: u.y * speed, z: vz },
			// Off the hand he went up with.
			{
				pid: e.pid,
				hand: this.track(e.pid)?.acts.findLast((a) => a.anim === "block")
					?.mirror
					? "far"
					: "near",
			},
		);
		this.beat(i, e.type, at, next);
		this.phase = "loose";
	}

	// HIS ROUTINE AT THE LINE.
	//
	// Every man has his own, the same every trip: bounces it two or three
	// times; flips it up to himself with backspin first; sits down into his
	// legs, ball on his hip, and breathes before he bounces it; or one quick
	// bounce and the flip. Quick, all of them. Returns when he is done with
	// it, the ball in his hands.
	private ftRoutine(shooter: number, t: number, S: Pt, rim: Pt): number {
		const kind = Math.floor(hash01(shooter * 13 + 5, 7) * 4);
		const many = 2 + Math.floor(hash01(shooter * 5 + 1, 11) * 2);
		const dribble = (n: number) => {
			this.hold(shooter, t, "dribble");
			t += (n * 1000) / DRIBBLE_RATE;
			this.hold(shooter, t, "hold");
			t += 120;
		};
		const flip = () => {
			// Up off his fingertips a foot or so, spinning back, and into his
			// hands again.
			const u = unitVec(S, rim);
			const up = {
				x: S.x + u.x * FT_HOLD.f,
				y: S.y + u.y * FT_HOLD.f,
				z: FT_HOLD.u + 1.1,
			};
			this.act(shooter, "hold", t, t + 500, { look: rim });
			this.fly(t, t + 300, { pid: shooter }, up);
			this.fly(t + 300, t + 600, up, { pid: shooter });
			this.act(shooter, "catch", t + 510, t + 710, { look: rim });
			this.hold(shooter, t + 600, "hold");
			t += 720;
		};
		if (kind === 0) {
			dribble(many);
		} else if (kind === 1) {
			flip();
			dribble(1);
		} else if (kind === 2) {
			// Down into his legs, a breath, and up.
			this.act(shooter, "triple", t, t + 750, { look: rim });
			t += 800;
			dribble(many - 1);
		} else {
			dribble(1);
			flip();
		}
		return t;
	}

	private beatFreeThrow(e: RawEvent, i: number, made: boolean) {
		const T = this.T;
		const shooter = e.pid as number;
		const team = this.teamOf(shooter);
		const dir = attackDir(team);
		const prev = this.beats.at(-1);
		const first = !(
			prev &&
			(prev.type === "ft" || prev.type === "missFt") &&
			this.lastFtShooter === shooter
		);
		this.lastFtShooter = shooter;
		this.setOffense(T, team);
		const line = spot(team, FT_SHOOTER_DEPTH, 25);
		const rimSpot = { x: rimX(team), y: 25 };
		const tall = (t: Side) =>
			this.slots(t).sort(
				(a, b) => (this.rank.get(b) ?? 4) - (this.rank.get(a) ?? 4) || a - b,
			);
		const def = tall(other(team));
		const off = tall(team).filter((p) => p !== shooter);
		const next = this.peek(i, 3).find(
			(x) =>
				x.e.type !== "sub" && x.e.type !== "timeout" && x.e.type !== "timeouts",
		);
		const more =
			next &&
			(next.e.type === "ft" || next.e.type === "missFt") &&
			next.e.pid === shooter;
		let ready = T;
		const official = ftOfficialBall(team);
		if (first) {
			// To the line: the defense on the blocks, the shooter at the line,
			// the official at the side of the lane with the ball - the walk
			// there run through fast.
			const has = this.tossTo(official, T + 250);
			const there = this.walkTo(
				[
					...def.map((pid, j) => {
						const [dd, ac] = ftDefenseSpot(j);
						return { pid, at: spot(team, dd, ac), face: -dir as 1 | -1 };
					}),
					...off.map((pid, j) => {
						const [dd, ac] = ftOffenseSpot(j);
						return { pid, at: spot(team, dd, ac), face: dir };
					}),
					{ pid: shooter, at: line, face: dir },
				],
				T + 400,
			);
			def.forEach((pid, j) => {
				this.lookAt(pid, there + 1, j < 3 ? rimSpot : line);
			});
			off.forEach((pid, j) => {
				this.lookAt(pid, there + 1, j < 2 ? rimSpot : line);
			});
			this.lookAt(shooter, there + 1, rimSpot);
			// (And the official there to hand it to him.)
			ready = Math.max(there, has, T + OFFICIALS_SETTLE) + 300;
			this.hurry(T, ready - 300, true);
		} else if (dist(this.ballPoint(), official) > 1) {
			// Tossed back out to the official.
			this.fly(T, T + 650, this.ballOrigin(), official);
			ready = T + 1000;
		}
		// He bounces it in, and the shooter goes through his routine: a dribble
		// or two - his own number, every trip to the line - then sets, shoots
		// and holds the follow-through until it gets there.
		const S = this.posOf(shooter);
		const hit = {
			x: official.x + (S.x - official.x) * 0.6,
			y: official.y + (S.y - official.y) * 0.6,
			z: BALL_R,
		};
		const caught = ready + 520;
		this.fly(ready, ready + 300, official, hit);
		this.fly(ready + 300, caught, hit, { pid: shooter });
		this.act(shooter, "catch", caught - 90, caught + 110, {
			face: dir,
			look: { x: official.x, y: official.y },
		});
		this.hold(shooter, caught, "hold");
		this.lookAt(shooter, caught + 111, rimSpot);
		const set = this.ftRoutine(shooter, caught + 160, S, rimSpot) + 200;
		this.hold(shooter, set, "hold");
		if (!first) {
			// The ball back out to the official and in to him again, and his
			// routine - seen once already this trip - run through fast.
			this.hurry(T, set - 250, true);
		}
		const shotAt = set + 260;
		// (His own stroke, as on his jumper.)
		const stroke =
			1000 * (FORM_LO + (FORM_HI - FORM_LO) * hash01(shooter, 6151));
		this.act(shooter, "setShot", shotAt, shotAt + stroke, {
			face: dir,
			look: rimSpot,
		});
		const release = shotAt + stroke * JUMPER.release;
		// Off his fingers (a typical player's: up over his eyes, his elbow
		// under it) and played out at the rim: in clean or rolled in; off the
		// front of the rim or the back, or rattled out - the last one off
		// toward whoever gets the rebound.
		const u = unitVec(S, rimSpot);
		const hand = releaseAt("setShot", JUMPER.release);
		if (this.offHisLeft(shooter)) {
			hand.s = -hand.s;
		}
		const roll = hash01(shooter * 3 + 1, release);
		const reb = made || more ? undefined : this.reboundBy(i);
		const play = this.playAtRim(
			team,
			shooter,
			{
				x: S.x + u.x * hand.f + u.y * hand.s,
				y: S.y + u.y * hand.f - u.x * hand.s,
				z: hand.u,
			},
			release,
			// Up in a proper arc, three feet over the rim.
			[1000, 1120],
			{
				made,
				kinds: made
					? roll < 0.55
						? ["swish"]
						: ["rim"]
					: roll < 0.3
						? ["rimOut", "rollOut"]
						: ["off", "brick", "rimOut"],
				...(reb === undefined
					? { inPlay: true }
					: { toward: this.towardOf(team, reb) }),
			},
		);
		let target: Pt3;
		let at: number;
		if (play) {
			this.rimClanks(play, team);
			at = play.t0 + play.line;
			target = playAt(play.found.play.pts, play.line);
		} else {
			target = made
				? rimPt(team, 0.35)
				: {
						x: rimX(team) - dir * (RIM_R + 0.05),
						y: 25 + this.rand(-0.4, 0.4),
						z: RIM_Z + 0.12,
					};
			at = release + 720;
			this.fly(release, at, { pid: shooter }, target);
		}
		this.act(shooter, "follow", shotAt + stroke, at + 260, {
			face: dir,
			look: rimSpot,
		});
		// On the lane: with another to come, stood easy watching it - hands on
		// his hips, on his knees (a defender: the shooter's men may be off to
		// slap his hand), or just stood there (the ball dead: see
		// evaluate.ts); on the last, stood easy till the shooter has it, then
		// set - down in his legs, hands out in front - and as it leaves his
		// hand, in to box out.
		const lane = [...def.slice(0, 3), ...off.slice(0, 2)];
		for (const pid of lane) {
			const h = hash01(pid, 17);
			if (!more) {
				this.act(
					pid,
					"laneSet",
					caught + 150 + 350 * hash01(pid, 19),
					release,
					{ look: rimSpot },
				);
			} else if (h < 0.75) {
				this.act(
					pid,
					h < 0.45 || off.includes(pid) ? "hips" : "crouch",
					Math.max(T, ready),
					at + 600,
					{ look: S },
				);
			}
		}
		if (!more) {
			this.laneBattle(team, def, off, shooter, release, at);
		}
		// (Done with, before the next.)
		let fives = at;
		if (more && (made || this.rng() < 0.6)) {
			// His men on the lane come in to him - make or miss - and slap him
			// low fives, palm to palm at the hip, one and then the other, and
			// walk back to their spaces. He takes a step to meet them.
			const mates = off
				.slice(0, 2)
				.filter((_, j) => j === 0 || this.rng() < (made ? 0.75 : 0.45));
			const toward = mates.length
				? {
						x: mates.reduce((a, p) => a + this.posOf(p).x, 0) / mates.length,
						y: mates.reduce((a, p) => a + this.posOf(p).y, 0) / mates.length,
					}
				: S;
			const step = unitVec(S, toward);
			const S2 = { x: S.x + step.x * 0.8, y: S.y + step.y * 0.8 };
			const stepped = this.go(shooter, S2, at + 250, WALK, "walk");
			let t = at + 150;
			mates.forEach((mate, j) => {
				const home = this.posOf(mate);
				const u = unitVec(S2, home);
				// (Each a long arm's length out in front of himself: hands
				// meeting halfway.)
				const meet = {
					x: S2.x + u.x * LOW_FIVE_APART,
					y: S2.y + u.y * LOW_FIVE_APART,
				};
				const met = Math.max(
					this.go(mate, meet, t + j * 120, WALK * 1.5, "walk"),
					stepped,
					t + j * 520,
				);
				// (Once he is there: a planted pose ends as a run starts.)
				this.act(mate, "lowFive", met + 20, met + 520, { look: S2 });
				this.act(shooter, "lowFive", met + 20, met + 520, { look: meet });
				this.go(mate, home, met + 560, WALK * 1.5, "walk");
				this.lookAt(mate, met + 561, rimSpot);
				t = met + 140;
			});
			if (mates.length) {
				fives = this.go(shooter, S, t + 450, WALK, "walk");
				this.lookAt(shooter, t + 451, rimSpot);
			}
		}
		if (made) {
			const settle = { x: rimX(team) - dir * 2.5, y: 25 + this.rand(-2, 2) };
			let still = at + 700;
			if (play) {
				still = this.dropThrough(play.t0, play.found.play, settle);
			} else {
				const top = rimPt(team, 0.35);
				this.fly(at, at + 140, top, rimPt(team, -2.3));
				this.bounce(at + 140, at + 800, rimPt(team, -2.3), settle, 2, 1.8);
			}
			this.effect("swish", at + 20, { rim: team });
			// (Another to come: on once it has come to rest, to go back out.)
			this.beat(
				i,
				e.type,
				at,
				more ? Math.max(at + 700, still, fives) : at + 700,
			);
			this.phase = more ? "ft" : "inboundBase";
			if (!more) {
				this.offense = other(team);
			}
		} else if (more) {
			// Off the rim and away - nobody goes after it with another to come.
			const away = { x: rimX(team) - dir * 3, y: 25 + this.rand(-4, 4) };
			let still = at + 700;
			if (play) {
				const end = play.found.play.end;
				const floor = this.toFloor(play.t0 + end.t, end.p, end.v);
				still = floor.t + this.bounceSpan(floor.h0, 2, 700);
				this.bounce(floor.t, still, floor.at, away, 2, floor.h0);
			} else {
				this.effect("clank", at, { rim: team });
				this.bounce(at, at + 700, target, away, 2, 1.6);
			}
			this.beat(i, e.type, at, Math.max(at + 650, still, fives));
			this.phase = "ft";
		} else if (play) {
			const end = play.found.play.end;
			const nextT = this.afterMiss(
				play.t0 + end.t,
				end.p,
				team,
				i,
				false,
				false,
				end.v,
			);
			this.beat(i, e.type, at, nextT);
			this.phase = "loose";
		} else {
			this.effect("clank", at, { rim: team });
			const nextT = this.afterMiss(at, target, team, i, false);
			this.beat(i, e.type, at, nextT);
			this.phase = "loose";
		}
	}

	// THE LAST FREE THROW: as it leaves his hand, the lane is open. The
	// shooter's men on it step in for the ball; the defenders in the spaces
	// below them step across into them first and sit on them, and the man in
	// the space up top turns and finds the shooter. Held till it is decided
	// at the rim (`at`) - and on, if it is a miss, till somebody has it.
	private laneBattle(
		team: Side,
		def: number[],
		off: number[],
		shooter: number,
		release: number,
		at: number,
	) {
		const rim = { x: rimX(team), y: COURT_H / 2 };
		const go = release + READ_MS;
		const until = at + 500;
		const coming = new Map<number, Pt>();
		const inAt = new Map<number, number>();
		for (const pid of off.slice(0, 2)) {
			const M = this.posOf(pid);
			const u = unitVec(M, rim);
			const C = clampPt({ x: M.x + u.x * 2.6, y: M.y + u.y * 2.6 });
			coming.set(pid, C);
			const there = this.goBy(
				pid,
				C,
				go + 60,
				go + 700,
				"run",
				undefined,
				BURST,
			);
			this.act(pid, "fight", Math.min(there, at), until, { look: rim });
			inAt.set(pid, Math.min(there, at));
		}
		def.slice(0, 3).forEach((pid, j) => {
			// The man on his side of the lane, or - up top - the shooter.
			const D = this.posOf(pid);
			const mine =
				j < 2
					? off
							.slice(0, 2)
							.find(
								(o) => Math.sign(this.posOf(o).y - 25) === Math.sign(D.y - 25),
							)
					: shooter;
			if (mine === undefined) {
				return;
			}
			const C = coming.get(mine) ?? this.posOf(mine);
			const u = unitVec(C, rim);
			const spot = clampPt({ x: C.x + u.x * 1.8, y: C.y + u.y * 1.8 });
			const there = this.goBy(pid, spot, go, go + 600, "run", undefined, BURST);
			this.act(pid, "boxOut", Math.min(there, at), until, { look: rim });
			const fights = inAt.get(mine);
			if (fights !== undefined) {
				this.jostle(mine, pid, Math.max(there, fights), until, rim);
			}
		});
	}

	// THE FOUL AWAY FROM A SHOT, played out the way it comes about: a reach
	// at the ball on his drive by the man guarding him, a help man a step
	// late sliding over into it, a grab at a cutter going by - or, with
	// next to no time gone, wrapped up on purpose as soon as it is in.
	// `victim` is who the sim says was fouled, where it says. Returns the
	// moment of contact and where it was.
	// Down late, fouling to stop the clock. (A foul straight after the last
	// play otherwise is a reach on the way up, a hold off the ball - not a
	// man jogging over to wrap up a handler stood waiting for him.)
	private foulingToStopClock(fouler: number, e: RawEvent): boolean {
		if (!this.overtime && this.periodNo < 4) {
			return false;
		}
		const team = this.teamOf(fouler);
		const down = this.score[other(team)] - this.score[team];
		const clock = typeof e.clock === "number" ? e.clock : 720;
		return clock <= 120 && down > 0 && down <= 12;
	}

	private stageFoul(
		fouler: number,
		team: Side,
		t: number,
		victim: number | undefined,
		deliberate: boolean,
	): { hit: number; at: Pt } {
		const dir = attackDir(team);
		const rim = { x: rimX(team), y: COURT_H / 2 };
		const manOf = this.slots(team).find((p) => this.defenderOf(p) === fouler);
		let handler = this.holder ?? this.slots(team)[0]!;
		// (A help man too far from the ball to have been in it grabs his own
		// man instead.)
		const far =
			dist(this.posOf(fouler), this.posOf(handler)) > 20 &&
			this.defenderOf(handler) !== fouler;
		const offBall =
			manOf !== undefined &&
			manOf !== handler &&
			(victim === undefined ? far || this.rng() < 0.4 : victim === manOf);
		const fouled = offBall ? manOf : (victim ?? handler);
		if (!offBall && fouled !== handler) {
			t = this.passTo(handler, fouled, t);
			handler = fouled;
		}
		const contact = (v: number, hit: number) => {
			const V = this.posOf(v);
			const F = this.posOf(fouler);
			this.act(fouler, "reach", hit - 140, hit + 320, {
				face: (V.x >= F.x ? 1 : -1) as 1 | -1,
				look: { ...V },
			});
			if (this.holder !== undefined) {
				this.hold(
					this.holder,
					Math.max(hit, this.free.get(this.holder) ?? 0),
					"hold",
				);
			}
			return { hit, at: V };
		};
		const start = (pid: number) => Math.max(t, this.free.get(pid) ?? 0);

		if (deliberate && !offBall) {
			// On purpose: straight at him, both arms round him.
			const V = this.posOf(handler);
			const u = unitVec(V, this.posOf(fouler));
			const hit = this.go(
				fouler,
				clampPt({ x: V.x + u.x * BODY, y: V.y + u.y * BODY }),
				start(fouler),
				SPRINT,
				"run",
			);
			// (Bringing it up, he is still on his way as they get to him.)
			this.keepComing(handler, hit);
			return contact(handler, hit);
		}

		if (offBall) {
			// A cut, and his man holding him up on it: there with him, a
			// hand on him as he tries to go by.
			const V = this.posOf(fouled);
			const C = this.nearRim(team, V, Math.max(6, dist(V, rim) - 10));
			const u = unitVec(C, V);
			const set = this.go(
				fouler,
				clampPt({ x: C.x + u.x * BODY, y: C.y + u.y * BODY }),
				start(fouler),
				RUN,
				"run",
			);
			const s0 = Math.max(start(fouled), set - runMs(dist(V, C), RUN) + 100);
			const there = this.go(fouled, C, s0, RUN, "run");
			const hit = Math.max(set, there - 150);
			// The man pushing it up the floor still coming as the whistle
			// goes - not stood waiting on it.
			if (this.holder !== undefined) {
				this.keepComing(this.holder, hit);
				this.probe(this.holder, hit);
			}
			return contact(fouled, hit);
		}

		// On the ball: he puts it on the floor and goes. (Stood with it a
		// while first - the set still coming - he has been probing.)
		this.probe(handler, start(handler));
		const A = this.posOf(handler);
		const D = this.nearRim(team, A, Math.max(7, dist(A, rim) - 11));
		const mine = this.defenderOf(handler);
		if (mine !== fouler) {
			// A help man sliding over into his path - a step late. He goes
			// as the drive does, and the drive is at its pace to get there
			// as he does.
			const H = clampPt({
				x: D.x + unitVec(D, rim).x * BODY,
				y: D.y + unitVec(D, rim).y * BODY,
			});
			const leave = Math.max(start(fouler), start(handler) - 400);
			const set = this.go(fouler, H, leave, SPRINT, "run");
			const L = dist(A, D);
			const speed = Math.min(
				15,
				Math.max(8, L / Math.max(0.1, (set + 60 - (leave - 250)) / 1000)),
			);
			let s0 = Math.max(start(handler), set + 60 - (L / speed) * 1000);
			this.hold(handler, start(handler), "dribble");
			if (s0 - start(handler) > 300) {
				// Working his man a moment first - a hesitation across.
				const w = this.rng() < 0.5 ? -1 : 1;
				s0 = Math.max(
					s0,
					this.go(
						handler,
						clampPt({ x: A.x - dir * 0.8, y: A.y + w * 2.5 }),
						start(handler),
						Math.max(4, Math.min(9, 2.6 / ((s0 - start(handler)) / 1000))),
						"dribble",
						dir,
					),
				);
			}
			const arrive = this.go(handler, D, s0, speed, "dribble", dir);
			if (mine !== undefined) {
				// His own man, a step behind.
				const u = unitVec(D, A);
				this.shadow(
					mine,
					clampPt({ x: D.x + u.x * 2.4, y: D.y + u.y * 2.4 }),
					s0 + 100,
					arrive,
					team,
				);
			}
			return contact(handler, Math.max(arrive, set));
		}
		const s0 = start(handler);
		this.hold(handler, s0, "dribble");
		const arrive = this.go(handler, D, s0, 15, "dribble", dir);
		// Riding his hip all the way, and reaching across for it.
		const side = Math.sign((A.y - D.y) * dir || 1);
		const lat = { x: -unitVec(A, D).y * side, y: unitVec(A, D).x * side };
		const r = unitVec(D, rim);
		this.shadow(
			fouler,
			clampPt({
				x: D.x + r.x * 1.2 + lat.x * 1.3,
				y: D.y + r.y * 1.2 + lat.y * 1.3,
			}),
			s0 + 100,
			arrive - 80,
			team,
		);
		return contact(
			handler,
			Math.max(arrive - 120, (this.free.get(fouler) ?? 0) - 60),
		);
	}

	private lastFtShooter: number | undefined;
	// The ball sent out of bounds off a miss already (see missOut): the
	// out-of-bounds line after it needs only the whistle.
	private outOffMiss = false;

	private beatTurnover(e: RawEvent, i: number) {
		const T = this.T;
		const stl = e.type === "stl";
		const victim = (stl ? e.pidTov : e.pid) as number;
		const team = this.teamOf(victim);
		const gap = this.clockGap(e);
		const oob = e.outOfBounds === true;
		// How it gets lost, in the shares the NBA loses it: a steal is a pass
		// picked off or the ball stripped; without one, it goes out of bounds
		// or a whistle stops it - an offensive foul, a travel, the shot clock.
		const kinds: Partial<Record<TurnoverKind, number>> = stl
			? oob
				? { lost: 0.6, pass: 0.4 }
				: { pass: 0.62, lost: 0.38 }
			: oob
				? { pass: 0.59, lost: 0.41 }
				: {
						screen: 0.4,
						charge: 0.25,
						travel: 0.25,
						fiveSeconds: 0.05,
						shotClock: gap !== undefined && gap >= 20 ? 2 : 0,
					};
		const dev = this.develop(team, T, gap, (entry) =>
			this.callForTurnover(
				entry,
				team,
				victim,
				kinds,
				gap,
				typeof e.clock === "number" ? e.clock : undefined,
			),
		);
		const run = dev.run;
		let t = dev.t;
		if (run?.risk) {
			t = this.runSteps(run, t, run.from, run.risk.step - 1);
			if (this.loseIt(run, run.risk, e, i, t)) {
				return;
			}
		}
		if (this.holder !== victim) {
			t = this.passTo(this.holder ?? this.slots(team)[0]!, victim, t);
		}
		let vp = this.posOf(victim);
		const dir = attackDir(team);
		if (stl) {
			const thief = e.pid as number;
			const rim = { x: rimX(team), y: COURT_H / 2 };
			const T0 = this.posOf(thief);
			let hit: number | undefined;
			// Too far off him to be the man on him: a help man, and the ball
			// taken off a drive - he attacks, the help steps up (or slides
			// across, or runs him down from behind) and strips it where they
			// meet. Not stood there holding it while a man runs at him from
			// under the basket or across the floor.
			const start = Math.max(t, this.free.get(victim) ?? 0);
			const go = Math.max(t, this.free.get(thief) ?? 0);
			const DRIVE = DRIBBLE * 0.75;
			let best: { M: Pt; miss: number } | undefined;
			if (
				this.holder === victim &&
				dist(vp, T0) > 9 &&
				dist(T0, rim) < dist(vp, rim) + 6
			) {
				for (let f = 0.2; f <= 0.75; f += 0.05) {
					const M = clampPt({
						x: vp.x + (rim.x - vp.x) * f,
						y: vp.y + (rim.y - vp.y) * f,
					});
					const miss = Math.abs(
						start + runMs(dist(vp, M), DRIVE) - (go + runMs(dist(T0, M), RUN)),
					);
					if (!best || miss < best.miss) {
						best = { M, miss };
					}
				}
			}
			if (best && best.miss < 600) {
				const M = best.M;
				const u = unitVec(vp, rim);
				this.hold(victim, start, "dribble");
				const arrive = this.go(victim, M, start, DRIVE, "dribble", dir);
				const mine = this.defenderOf(victim);
				if (mine !== undefined && mine !== thief) {
					// His own man, a step behind.
					this.shadow(
						mine,
						clampPt({ x: M.x - u.x * 2.4, y: M.y - u.y * 2.4 }),
						start + 100,
						arrive,
						team,
					);
				}
				hit = Math.max(
					arrive,
					this.goBy(
						thief,
						clampPt({ x: M.x + u.x * 1.6, y: M.y + u.y * 1.6 }),
						go,
						arrive - 60,
						"run",
						(M.x >= T0.x ? 1 : -1) as 1 | -1,
					),
				);
				vp = M;
			}
			if (hit === undefined && this.holder === victim) {
				this.sagIn(thief, vp, t, 5);
			}
			const toward = (vp.x >= this.posOf(thief).x ? 1 : -1) as 1 | -1;
			hit ??= this.goBy(
				thief,
				clampPt({ x: vp.x - dir * 1.4, y: vp.y + 0.6 }),
				t,
				t + 420,
				"run",
				toward,
			);
			// A quick poke at it, with the hand on the ball's side.
			this.act(thief, "poke", hit - 150, hit + 300, {
				face: toward,
				...(this.ballHandOf === victim && this.ballHand === "R"
					? { mirror: true as const }
					: {}),
			});
			if (e.outOfBounds) {
				const outY = vp.y < COURT_H / 2 ? -1.8 : COURT_H + 1.8;
				this.bounce(
					hit,
					hit + 900,
					{ x: vp.x, y: vp.y, z: 2.5 },
					{
						x: Math.min(COURT_W - 1, Math.max(1, vp.x + this.rand(-6, 6))),
						y: outY,
					},
					2,
					1.4,
				);
				this.effect("whistle", hit + 800, {
					call: "out",
					at: { x: vp.x, y: outY },
					team: other(team),
				});
				this.inboundAt = { x: vp.x, y: outY < 0 ? -1.4 : COURT_H + 1.4 };
				this.offense = other(team);
				this.phase = "inboundSide";
				this.beat(i, e.type, hit, hit + 1000);
			} else {
				this.fly(hit, hit + 160, { pid: victim }, { pid: thief });
				this.hold(thief, hit + 160, "hold");
				this.hold(thief, hit + 160 + GATHER_MS, "dribble");
				this.setOffense(hit, other(team));
				this.phase = "loose";
				this.beat(i, e.type, hit, hit + 650);
			}
			return;
		}
		// A turnover with no steal: thrown away out of bounds, or just lost.
		if (e.outOfBounds) {
			const outY = vp.y < COURT_H / 2 ? -2.2 : COURT_H + 2.2;
			// (Out over the sideline - not on past the baseline into the
			// crowd behind it.)
			const to = {
				x: Math.min(COURT_W - 5, Math.max(5, vp.x + dir * this.rand(6, 14))),
				y: outY,
			};
			this.act(victim, "pass", t, t + 300, { face: dir });
			this.fly(t + 120, t + 700, { pid: victim }, { ...to, z: 3 });
			this.bounce(
				t + 700,
				t + 1300,
				{ ...to, z: 3 },
				{ x: to.x + dir * 3, y: to.y },
				1,
				1,
			);
			this.effect("whistle", t + 900, {
				call: "out",
				at: to,
				team: other(team),
			});
			this.inboundAt = {
				x: Math.min(COURT_W - 2, Math.max(2, to.x + dir * 3)),
				y: outY < 0 ? -1.4 : COURT_H + 1.4,
			};
			this.beat(i, e.type, t, t + 1200);
		} else {
			this.bounce(
				t,
				t + 700,
				{ x: vp.x, y: vp.y, z: 2.5 },
				clampPt({
					x: vp.x + dir * this.rand(2, 5),
					y: vp.y + this.rand(-3, 3),
				}),
				2,
				1.2,
			);
			this.effect("whistle", t + 300, {
				call: "travel",
				at: vp,
				team: other(team),
			});
			if (this.rng() < 0.35) {
				this.react(victim, "protest", t + 650, 900);
			}
			this.inboundAt = {
				x: vp.x,
				y: vp.y < COURT_H / 2 ? -1.4 : COURT_H + 1.4,
			};
			this.beat(i, e.type, t, t + 900);
		}
		this.offense = other(team);
		this.phase = "inboundSide";
	}

	// Where a ball heading from `p` along `u` leaves the floor, and a little past.
	private outPoint(p: Pt, u: Pt): Pt {
		let k = Infinity;
		if (u.x > 1e-6) {
			k = Math.min(k, (COURT_W - p.x) / u.x);
		} else if (u.x < -1e-6) {
			k = Math.min(k, -p.x / u.x);
		}
		if (u.y > 1e-6) {
			k = Math.min(k, (COURT_H - p.y) / u.y);
		} else if (u.y < -1e-6) {
			k = Math.min(k, -p.y / u.y);
		}
		if (!Number.isFinite(k)) {
			k = 0;
		}
		return { x: p.x + u.x * (k + 1.6), y: p.y + u.y * (k + 1.6) };
	}

	// The nearest way off the floor from a spot: whichever line is closest.
	private nearestOut(p: Pt): Pt {
		const sides: [number, Pt][] = [
			[p.y, { x: 0, y: -1 }],
			[COURT_H - p.y, { x: 0, y: 1 }],
			[p.x, { x: -1, y: 0 }],
			[COURT_W - p.x, { x: 1, y: 0 }],
		];
		sides.sort((a, b) => a[0] - b[0]);
		return this.outPoint(p, sides[0]![1]);
	}

	// A dead ball the other way, taken out where it went out.
	private turnOver(team: Side, at: Pt) {
		this.inboundAt =
			at.x < 0 || at.x > COURT_W
				? {
						x: at.x < 0 ? -1.4 : COURT_W + 1.4,
						y: Math.min(46, Math.max(4, at.y)),
					}
				: {
						x: Math.min(COURT_W - 2, Math.max(2, at.x)),
						y: at.y < COURT_H / 2 ? -1.4 : COURT_H + 1.4,
					};
		this.offense = other(team);
		this.phase = "inboundSide";
	}

	// The turnover itself, at the step of the set where it goes wrong: the
	// pass he throws picked off or thrown away, the drive stripped, the screen
	// that moves, the charge, the extra step. False if the set gives it no
	// way to happen (the plain version is staged instead).
	private loseIt(
		run: Running,
		risk: PlayRisk,
		e: RawEvent,
		i: number,
		t: number,
	): boolean {
		const { team, play } = run;
		const dir = attackDir(team);
		const rim = { x: rimX(team), y: COURT_H / 2 };
		const victim = run.roles[risk.who]!;
		const thief = e.type === "stl" ? (e.pid as number) : undefined;
		const oob = e.outOfBounds === true;
		const acts = play.steps[risk.step] ?? [];
		const failAt = acts.findIndex((a) =>
			risk.kind === "pass"
				? (a.type === "pass" || a.type === "handoff") &&
					run.roles[a.who] === victim
				: risk.kind === "screen"
					? a.type === "screen" && a.who.some((r) => run.roles[r] === victim)
					: (a.type === "dribble" || a.type === "move") &&
						run.roles[a.who] === victim,
		);
		const fail = failAt >= 0 ? acts[failAt] : undefined;
		// What the step does before it goes wrong, and everyone else's part of
		// it alongside.
		const lead = acts.filter(
			(a, j) =>
				j !== failAt &&
				(j < failAt ||
					(a.type !== "pass" &&
						a.type !== "handoff" &&
						a.type !== "dribble" &&
						!(a.type === "move" && run.roles[a.who] === victim))),
		);
		if (risk.kind === "shotClock") {
			// The whole set and nothing came of it: the clock runs out on him.
			t = this.runSteps(run, t, risk.step, play.steps.length - 1);
			const h = this.holder ?? victim;
			this.hold(h, t, "dribble");
			const tw = t + 700;
			this.hold(h, tw, "hold");
			this.effect("whistle", tw, {
				call: "clock",
				at: this.posOf(h),
				team: other(team),
			});
			this.turnOver(team, this.posOf(h));
			this.beat(i, e.type, tw, tw + 900);
			return true;
		}
		if (risk.kind === "fiveSeconds") {
			// Nobody gets open for the inbound.
			const tw = t + 1500;
			this.effect("whistle", tw, {
				call: "clock",
				at: this.posOf(victim),
				team: other(team),
			});
			this.turnOver(team, this.posOf(victim));
			this.beat(i, e.type, tw, tw + 900);
			return true;
		}
		if (lead.length > 0) {
			// Everyone's part of the step runs on around it.
			this.runStep(run, lead, t, undefined, risk.step);
		}
		if (risk.kind === "screen") {
			if (!fail || fail.type !== "screen") {
				return false;
			}
			// The screen goes up and he leans into the man coming over it.
			const end = this.runStep(run, [fail], t, undefined, risk.step);
			const user = run.roles[fail.for]!;
			const ud = this.defenderOf(user);
			const S = this.posOf(victim);
			let hit = end;
			// Off the ball, it is as the man it frees comes by it that his man
			// runs into it - not once the cut is run and done, the man with the
			// ball stood waiting on the whistle all that while.
			let from = end - 350;
			let by = end + 150;
			const set = run.screen?.set;
			const off = user !== this.holder && set !== undefined;
			if (off) {
				let near = Infinity;
				let past = set;
				for (let q = set; q <= (this.free.get(user) ?? set); q += 50) {
					const d = dist(this.posAt(user, q), S);
					if (d < near - 0.01) {
						near = d;
						past = q;
					}
				}
				const U = this.posAt(user, set - 150);
				if (near > 4.5 && dist(U, S) < 26) {
					// Not by it yet - his cut off it is the set's next step: he
					// comes off it now, brushing the screener's shoulder, on his
					// way to where it was to take him.
					const cut = play.steps[risk.step + 1]?.find(
						(a) => a.type === "move" && a.who === fail.for,
					);
					const on = unitVec(U, S);
					const P =
						cut?.type === "move"
							? this.at(run, cut.to)
							: clampPt({ x: S.x + on.x * 8, y: S.y + on.y * 8 });
					const l = unitVec(U, P);
					const side = l.x * (S.y - U.y) - l.y * (S.x - U.x) >= 0 ? -1 : 1;
					const B = clampPt({
						x: S.x - l.y * side * 2.2,
						y: S.y + l.x * side * 2.2,
					});
					// (Timed to come by it as it goes up: off as the screener
					// gets there, less the run.)
					const leave = Math.max(
						t,
						set + 100 - runMs(dist(U, B), RUN, 0, 1, RUN),
					);
					this.dropFrom(user, leave);
					past = this.go(user, B, leave, RUN, "run", undefined, {
						onto: RUN,
					});
					// (A few strides on, until the whistle pulls him up.)
					const w = unitVec(B, P);
					const ahead = Math.min(6, dist(B, P));
					this.go(
						user,
						clampPt({ x: B.x + w.x * ahead, y: B.y + w.y * ahead }),
						past,
						RUN,
						"run",
					);
				}
				by = Math.max(past, set + 150) + 50;
				from = by - 550;
			}
			if (ud !== undefined) {
				if (off) {
					this.dropFrom(ud, from);
				}
				const u = unitVec(S, this.posOf(ud));
				hit = this.goBy(
					ud,
					clampPt({ x: S.x + u.x * BODY, y: S.y + u.y * BODY }),
					from,
					by,
					"run",
				);
				// (On the cutter's hip already, he bumps it as the cutter goes by.)
				if (off) {
					hit = Math.max(hit, by - 50);
				}
			}
			this.act(victim, "reach", hit - 160, hit + 300, {
				look: this.posOf(user),
			});
			if (this.holder !== undefined) {
				this.hold(this.holder, hit, "hold");
			}
			this.effect("whistle", hit + 80, {
				call: "offensive",
				at: S,
				team: other(team),
			});
			if (this.rng() < 0.55) {
				this.react(victim, "protest", hit + 450, 1000);
			}
			this.turnOver(team, S);
			this.beat(i, e.type, hit + 80, hit + 950);
			return true;
		}
		// Tipped out of bounds, a pass has to come off the man it was meant
		// for - his side's turnover - so he has to be where the picture has
		// him: not a man still shadowing somebody from the trip before (see
		// mark). With nobody like that to throw to, it is stripped instead.
		const settled = (q: number | undefined) =>
			q !== undefined &&
			q !== victim &&
			!(this.track(q)?.moves ?? []).some(
				(m) => this.marking.has(m) && m.t1 > t - 4000,
			);
		const kind =
			risk.kind === "pass" &&
			oob &&
			thief !== undefined &&
			!run.roles.some(settled)
				? "lost"
				: risk.kind;
		if (kind === "pass") {
			if (this.holder !== victim) {
				t = this.passTo(this.holder ?? victim, victim, t);
			}
			let to =
				fail && (fail.type === "pass" || fail.type === "handoff")
					? run.roles[fail.to]!
					: undefined;
			if (to === undefined || to === victim) {
				to = run.roles.find((p) => p !== victim);
			}
			if (oob && thief !== undefined && !settled(to)) {
				to = run.roles.find(settled);
			}
			if (to === undefined) {
				return false;
			}
			const A = this.posOf(victim);
			const T0 = thief !== undefined ? this.posOf(thief) : undefined;
			// A pass to `q`, and - for a steal - where along it the thief
			// gets to it: farther along if the lane is a long way off, and,
			// if even that is too far, late (the pass hanging for him).
			const throwTo = (q: number, read = 400) => {
				const B = this.posOf(q);
				const d = dist(A, B);
				const flight = passMs(d);
				const over = d >= 22;
				const wind = RELEASE_MS + (over ? OVERHEAD_WIND : 0);
				// (Picked off, it needn't wait on him to be there for it: not
				// long, anyway.)
				const ready = (this.free.get(q) ?? 0) + 40 - flight - wind;
				const start = Math.max(
					t,
					this.free.get(victim) ?? 0,
					thief === undefined ? ready : Math.min(ready, t + 500),
				);
				const release = start + wind;
				if (thief === undefined || T0 === undefined) {
					return { B, d, flight, over, wind, start, release };
				}
				// He reads it a beat before it is thrown (or sits in the lane,
				// waiting on it).
				const go = Math.max(start - read, this.free.get(thief) ?? 0);
				const along =
					d > 0.1
						? ((T0.x - A.x) * (B.x - A.x) + (T0.y - A.y) * (B.y - A.y)) /
							(d * d)
						: 0.6;
				const at = (f: number) => {
					const I = clampPt({
						x: A.x + (B.x - A.x) * f,
						y: A.y + (B.y - A.y) * f,
					});
					return {
						I,
						tI: release + flight * f,
						need: go + runMs(dist(T0, I), SPRINT) + 40,
					};
				};
				let f = Math.min(0.85, Math.max(0.35, along));
				let pick = at(f);
				while (pick.need > pick.tI && f < 0.85) {
					f = Math.min(0.85, f + 0.05);
					pick = at(f);
				}
				return {
					B,
					d,
					flight,
					over,
					wind,
					start,
					release,
					lane: { ...pick, go, late: pick.need > pick.tI },
				};
			};
			let receiver: number = to;
			let thrown = throwTo(receiver);
			// The pass he jumps is one he can get to: another outlet of the
			// man with it, if not the one the set had him make - or one he
			// has been sitting in the lane for.
			for (const read of [400, 1500]) {
				for (const q of [receiver, ...run.roles] as (number | undefined)[]) {
					if (!thrown.lane?.late) {
						break;
					}
					if (q === undefined || q === victim || (oob && !settled(q))) {
						continue;
					}
					const alt = throwTo(q, read);
					// (One he can throw now, not one he has to wait on.)
					if (alt.lane && !alt.lane.late && alt.start <= thrown.start + 250) {
						receiver = q;
						thrown = alt;
					}
				}
			}
			// Still a step short of the lane, he gets there as it does: the
			// pass is thrown a beat later, at its own pace - never floated
			// up for him.
			const wait = thrown.lane
				? Math.max(0, thrown.lane.need - thrown.lane.tI)
				: 0;
			const { B, over, wind } = thrown;
			const start = thrown.start + wait;
			const release = thrown.release + wait;
			this.act(
				victim,
				over ? "passOverhead" : "pass",
				start,
				start + wind + 180,
				{
					face: B.x >= A.x ? 1 : -1,
					look: { ...B },
				},
			);
			const u = unitVec(A, B);
			if (thief !== undefined && thrown.lane) {
				const { I, go } = thrown.lane;
				const tI = thrown.lane.tI + wait;
				this.goBy(thief, I, go, tI - 40, "run");
				if (!oob) {
					this.fly(release, tI, { pid: victim }, { pid: thief });
					this.act(thief, "catch", tI - 90, tI + 110, { look: A });
					this.hold(thief, tI, "hold");
					this.hold(thief, tI + GATHER_MS, "dribble");
					this.setOffense(tI, other(team));
					this.phase = "loose";
					this.beat(i, e.type, tI, tI + 650);
					return true;
				}
				// Got a hand on it - and on it goes, off the hands of the man
				// it was meant for (his the last touch, his side's turnover),
				// and out of bounds.
				const I3 = { ...I, z: 3.6 };
				this.act(thief, "reach", tI - 150, tI + 250, { look: A });
				this.fly(release, tI, { pid: victim }, I3);
				// He stops where he is to take it - and cannot hold it.
				this.cutShort(receiver, tI - 200);
				const R = this.posOf(receiver);
				const tR = tI + Math.max(150, (dist(I, R) / 24) * 1000);
				const back = unitVec(R, I);
				const R3 = {
					x: R.x + back.x * CATCH_AT.f,
					y: R.y + back.y * CATCH_AT.f,
					z: CATCH_AT.u,
				};
				this.fly(tI, tR, I3, R3);
				const c0 = tR - CATCH_MS * CATCH_HIT;
				this.act(receiver, "catch", c0, c0 + CATCH_MS, { look: I });
				// (Plainly off his hands: turned by them, not on through on the
				// line it came in on.)
				const r = this.rand(-1, 1);
				const a = (r < 0 ? -1 : 1) * (0.35 + 0.45 * Math.abs(r));
				const tOut = this.knockOut(
					tR,
					R3,
					{
						x: -back.x * Math.cos(a) + back.y * Math.sin(a),
						y: -back.x * Math.sin(a) - back.y * Math.cos(a),
					},
					{ x: -back.x, y: -back.y },
				);
				const out = this.ballAt;
				this.effect("whistle", tOut + 150, {
					call: "out",
					at: out,
					team: other(team),
				});
				this.turnOver(team, out);
				this.beat(i, e.type, tI, tOut);
				return true;
			}
			// Thrown away: behind him, wide of where he is, and out - he
			// reaches for it as it goes by, not a hand to it (not through him,
			// either).
			const side = hash01(victim, Math.round(release)) < 0.5 ? 1 : -1;
			const W = clampPt({
				x: B.x - u.y * side * WIDE_OF,
				y: B.y + u.x * side * WIDE_OF,
			});
			const out = this.outPoint(W, unitVec(A, W));
			const tOut = release + passMs(dist(A, out));
			const tBy =
				release + (tOut - release) * Math.min(1, dist(A, B) / dist(A, out));
			this.act(receiver, "reach", tBy - 220, tBy + 200, { look: { ...W } });
			this.fly(release, tOut, { pid: victim }, { ...out, z: 3 });
			this.bounce(
				tOut,
				tOut + 600,
				{ ...out, z: 3 },
				{ x: out.x + u.x * 3, y: out.y + u.y * 3 },
				1,
				1,
			);
			this.effect("whistle", tOut + 300, {
				call: "out",
				at: out,
				team: other(team),
			});
			this.turnOver(team, out);
			this.beat(i, e.type, tOut, tOut + 700);
			return true;
		}
		// The rest happen on his drive.
		if (this.holder !== victim) {
			t = this.passTo(this.holder ?? victim, victim, t);
		}
		const A = this.posOf(victim);
		const D =
			fail && (fail.type === "dribble" || fail.type === "move")
				? fail.to === "rim"
					? this.nearRim(team, A)
					: this.at(run, fail.to)
				: this.nearRim(team, A, 6);
		const start = Math.max(t, this.free.get(victim) ?? 0);
		this.hold(victim, start, "dribble");
		if (kind === "lost") {
			// Stripped on the way: by his own man, in front of him - or, beaten,
			// tapped away from behind as he goes by - or by a help man digging
			// down on the drive from his side, where it comes past him.
			const who = thief ?? this.defenderOf(victim);
			const mine = this.defenderOf(victim);
			const help = who !== undefined && who !== mine;
			const uAD = unitVec(A, D);
			const along = (() => {
				if (!help) {
					return 0.55;
				}
				const W = this.posOf(who);
				const L2 = dist(A, D) ** 2 || 1;
				const f = ((W.x - A.x) * (D.x - A.x) + (W.y - A.y) * (D.y - A.y)) / L2;
				return Math.min(0.8, Math.max(0.35, f));
			})();
			const S = clampPt({
				x: A.x + (D.x - A.x) * along,
				y: A.y + (D.y - A.y) * along,
			});
			const behind = !help && this.rng() < 0.4;
			const ur = unitVec(S, rim);
			const P = help
				? (() => {
						const v = unitVec(S, this.posOf(who));
						return clampPt({ x: S.x + v.x * 1.6, y: S.y + v.y * 1.6 });
					})()
				: behind
					? clampPt({
							x: S.x - uAD.x * 1.3 - uAD.y * 0.8,
							y: S.y - uAD.y * 1.3 + uAD.x * 0.8,
						})
					: clampPt({ x: S.x + ur.x * 1.5, y: S.y + ur.y * 1.5 });
			if (help && mine !== undefined) {
				// His own man a step behind him all the way.
				this.shadow(
					mine,
					clampPt({ x: S.x - uAD.x * 2.2, y: S.y - uAD.y * 2.2 }),
					start + 100,
					start + 100 + runMs(dist(A, S), 14),
					team,
				);
			}
			// The man who strips him gets there first, and the drive comes to
			// him: no faster than he can be there for it.
			let tS: number;
			if (who !== undefined) {
				if (help) {
					this.sagIn(who, S, start);
				}
				this.unshadow(who, start);
				const near = dist(this.posOf(who), P) < 9;
				const arr = this.go(
					who,
					P,
					start,
					near ? 11 : SPRINT,
					near ? "slide" : "run",
					near ? (-dir as 1 | -1) : undefined,
				);
				const ms = Math.max(250, arr + 60 - start);
				tS = Math.max(
					arr + 60,
					this.go(
						victim,
						S,
						start,
						Math.max(4, Math.min(15, dist(A, S) / (ms / 1000))),
						"dribble",
						dir,
					),
				);
				this.act(who, "poke", tS - 160, tS + 260, {
					look: S,
					...(this.ballHandOf === victim && this.ballHand === "R"
						? { mirror: true as const }
						: {}),
				});
			} else {
				tS = this.go(victim, S, start, 12, "dribble", dir);
			}
			if (thief !== undefined && !oob) {
				const side = this.rng() < 0.5 ? 1 : -1;
				const u = unitVec(A, D);
				// Knocked loose past the man who poked it, he scoops it up in
				// stride; poked away by somebody else, it squirts toward the man
				// who comes up with it - still rolling as he gets there.
				const Q = this.posOf(thief);
				const toQ = dist(S, Q);
				const loose =
					thief === who && behind
						? // Tapped on ahead of him, for the man behind to run onto.
							clampPt({
								x: S.x + u.x * 3.5 - u.y * side * 1.2,
								y: S.y + u.y * 3.5 + u.x * side * 1.2,
							})
						: thief === who
							? clampPt({
									x: S.x + u.x * 1.5 - u.y * side * 3,
									y: S.y + u.y * 1.5 + u.x * side * 3,
								})
							: clampPt({
									x: S.x + ((Q.x - S.x) / (toQ || 1)) * Math.min(toQ * 0.6, 9),
									y: S.y + ((Q.y - S.y) / (toQ || 1)) * Math.min(toQ * 0.6, 9),
								});
				const off = thief === who ? tS + 260 : tS + 120;
				const there =
					Math.max(off, this.free.get(thief) ?? 0) +
					runMs(Math.max(0, dist(Q, loose) - PICKUP_REACH), SPRINT);
				this.bounce(
					tS,
					Math.min(
						tS + 1600,
						Math.max(there, tS + this.bounceSpan(1, 1, 450, 1)),
					),
					{ ...S, z: 2 },
					loose,
					1,
					1,
				);
				// After it, once his hand is back from the swipe.
				const got = this.pickUp(thief, off, SPRINT, "dribble");
				this.setOffense(tS, other(team));
				this.phase = "loose";
				this.beat(i, e.type, tS, got + 450);
				return true;
			}
			// Knocked down off his own knee - his the last hand on it, his
			// the turnover - and away out of bounds.
			const away = unitVec(P, S);
			const knee = {
				x: S.x + away.x * 0.6,
				y: S.y + away.y * 0.6,
				z: 1.4,
			};
			this.fly(tS, tS + 90, { pid: victim }, knee);
			const tOut = this.knockOut(tS + 90, knee, unitVec(S, this.nearestOut(S)));
			const out = this.ballAt;
			this.effect("whistle", tOut + 150, {
				call: "out",
				at: out,
				team: other(team),
			});
			this.turnOver(team, out);
			this.beat(i, e.type, tS, tOut);
			return true;
		}
		if (risk.kind === "charge") {
			// A help man beats him to the spot and takes it in the chest.
			const E = clampPt({
				x: A.x + (D.x - A.x) * 0.85,
				y: A.y + (D.y - A.y) * 0.85,
			});
			const mine = this.defenderOf(victim);
			const helper =
				this.slots(other(team))
					.filter((p) => p !== mine)
					.sort((a, b) => dist(this.posOf(a), E) - dist(this.posOf(b), E))[0] ??
				mine;
			if (helper === undefined) {
				return false;
			}
			const u = unitVec(E, rim);
			const C = clampPt({ x: E.x + u.x * 1.6, y: E.y + u.y * 1.6 });
			const near = dist(this.posOf(helper), C) < 9;
			const set = this.goBy(
				helper,
				C,
				start,
				start + 450,
				near ? "slide" : "run",
				near ? (-dir as 1 | -1) : undefined,
			);
			const drive = (dist(A, E) / 17) * 1000;
			const hit = this.go(
				victim,
				E,
				Math.max(start, set + 150 - drive),
				17,
				"dribble",
				dir,
			);
			// Down on the floor with it - and a teammate or two over to give
			// him a hand up: up off the floor as they pull him up.
			const reach = this.helpUp(helper, C, A, E, hit);
			const up = Math.max(
				hit + CHARGE_DOWN_MS,
				hit - 60 + (reach + 60 - hit) / CHARGE_UP_AT,
			);
			this.act(helper, "takeCharge", hit - 60, up, { look: A });
			this.free.set(helper, Math.max(this.free.get(helper) ?? 0, up));
			this.hold(victim, hit, "hold");
			this.effect("whistle", hit + 120, {
				call: "offensive",
				at: E,
				team: other(team),
			});
			if (this.rng() < 0.6) {
				this.react(victim, "protest", hit + 500, 1000);
			}
			this.turnOver(team, E);
			this.beat(i, e.type, hit + 120, up - 300);
			return true;
		}
		// A travel: the drive, the gather, a step too many.
		const G = clampPt({
			x: A.x + (D.x - A.x) * 0.6,
			y: A.y + (D.y - A.y) * 0.6,
		});
		let tt = this.go(victim, G, start, 16, "dribble", dir);
		tt = Math.max(tt, this.hold(victim, tt, "hold"));
		const u = unitVec(A, D);
		tt = this.go(
			victim,
			clampPt({ x: G.x + u.x * 2.5, y: G.y + u.y * 2.5 }),
			tt,
			8,
			"run",
			dir,
		);
		this.effect("whistle", tt + 100, {
			call: "travel",
			at: G,
			team: other(team),
		});
		if (this.rng() < 0.35) {
			this.react(victim, "protest", tt + 450, 900);
		}
		this.turnOver(team, G);
		this.beat(i, e.type, tt + 100, tt + 900);
		return true;
	}

	// THE STARTING LINEUPS (see intro.ts), from t: the road team's starters
	// called out one by one, then the home team's, each running out from in
	// front of his bench to his place in his team's line - the man called
	// before him turning to slap his hand as he gets there - and the rest
	// of the line clapping him in. Returns when the lights are back up.
	private callLineups(t0: number): number {
		const big = this.introKind === "playoffs" ? 1 : 0;
		const calls: IntroCall[] = [];
		let t = t0 + INTRO_FIRST[big];
		for (const team of [0, 1] as const) {
			const order = this.introOrder[team];
			const len = INTRO_CALL[team][big];
			order.forEach((pid, k) => {
				calls.push({ pid, team, t0: t, t1: t + len });
				this.callOut(pid, team, k, t, len);
				t += len;
			});
		}
		// All ten out: back to the game's own picture for the lights coming up.
		this.cuts.push(t);
		const end = t + INTRO_HOLD[big] + INTRO_LIGHT;
		this.intro = { t0, t1: end, big: big === 1, calls };
		// Nobody on offense or defense yet: everybody just stands, the whole
		// way through to the tip (see beatJumpBall).
		this.lineupsFrom = t0;
		return end;
	}

	// One man called: out to his place in the line, and turned to face the
	// cameras once he is there.
	private callOut(pid: number, team: Side, k: number, t: number, len: number) {
		const order = this.introOrder[team];
		const spot = rowSpot(team, k);
		// Toward the middle of the floor, along the line.
		const inward = team === 0 ? 1 : -1;
		const cameras = { x: spot.x, y: COURT_H + 40 };
		// Those already out clap him in.
		for (let j = 0; j < k - 1; j++) {
			this.act(order[j]!, "clap", t + 120 + j * 40, t + len - 60, {
				look: spot,
			});
		}
		const prev = k > 0 ? order[k - 1] : undefined;
		if (prev === undefined) {
			const there = this.go(pid, spot, t + 120, INTRO_RUN, "run");
			this.react(pid, "flex", there, 900, cameras);
			this.lookAt(pid, there + 900, cameras);
			return;
		}
		// Hands slapped on the way past the man before him, then on into
		// his own place.
		const P = rowSpot(team, k - 1);
		const meet = { x: P.x + inward * LOW_FIVE_APART, y: spot.y };
		const met = this.go(pid, meet, t + 120, INTRO_RUN, "run") + 30;
		const five = hash01(pid, t) < 0.5 ? "highFive" : "lowFive";
		this.act(prev, five, met, met + 520, { look: meet });
		this.act(pid, five, met, met + 520, { look: P });
		this.lookAt(prev, met + 520, { x: P.x, y: COURT_H + 40 });
		const there = this.go(pid, spot, met + 540, WALK, "walk");
		// The last man called - the star, as often as not - plays to the crowd.
		if (k === order.length - 1) {
			this.react(
				pid,
				hash01(pid, t + 1) < 0.5 ? "flex" : "point",
				there,
				900,
				cameras,
			);
		}
		this.lookAt(pid, there + (k === order.length - 1 ? 900 : 0), cameras);
	}

	// The whistle, then off to the huddles - fast - with the ball (handed to
	// `taker` there and then if he is given, else with whoever has it, or
	// picked up off the floor): not left lying out on the floor through the
	// break. Over it, a look round the building. Returns when it is over.
	private toHuddles(
		i: number,
		at: number,
		long: boolean,
		taker?: number,
	): number {
		if (taker !== undefined && taker !== this.holder) {
			this.ballTo(taker, at + 200);
		} else if (this.holder !== undefined) {
			this.hold(this.holder, at, "hold");
		} else {
			this.pickUpLoose(at + 200);
		}
		// Each to the place in the huddle on his side of it, so nobody walks
		// across anybody to get there.
		const there = this.walkTo(
			([0, 1] as const).flatMap((t) => {
				const spots = huddleSpots(t);
				const c = { x: benchX(t), y: HUDDLE_Y };
				const angle = (p: Pt) => Math.atan2(p.y - c.y, p.x - c.x);
				const men = [...this.slots(t)].sort(
					(a, b) => angle(this.posOf(a)) - angle(this.posOf(b)),
				);
				const places = [...spots].sort((a, b) => angle(a) - angle(b));
				return men.map((pid, j) => ({
					pid,
					at: places[j] ?? places[0]!,
				}));
			}),
			at + 600,
		);
		for (const t of [0, 1] as const) {
			this.slots(t).forEach((pid) => {
				this.lookAt(pid, there + 1, { x: benchX(t), y: HUDDLE_Y });
			});
		}
		this.hurry(at + 800, there);
		const tc = Math.max(at + 800, there);
		const over = tc + (long ? 1700 : 1300);
		this.shots.push({ t0: tc, t1: over, kind: i % 3 ? "wide" : "crowd" });
		return over;
	}

	// The last of the clock at the end of the game: the side with the ball
	// gets it up the floor - fast - and dribbles out what is left of it (or,
	// behind, is still looking for a shot when time runs out); then the horn,
	// and it is over where they are - nobody off to the bench first, only to
	// come back out for the handshakes (see gameOver).
	private finalHorn(
		e: RawEvent,
		i: number,
		team: Side,
		gap: number | undefined,
	) {
		const t = this.playOutClock(e, team, gap);
		this.effect("whistle", t, { call: "stop", team: this.offense });
		if (this.holder !== undefined) {
			this.hold(this.holder, t, "hold");
		}
		this.beat(i, e.type, t, t + 900);
		this.phase = "dead";
	}

	// The seconds left on the clock when a period runs out with no shot: the
	// side with the ball gets it up the floor - fast - and dribbles out what
	// is left of it (or, looking for a shot, never finds one). Returns when
	// the horn goes.
	private playOutClock(
		e: RawEvent,
		team: Side,
		gap: number | undefined,
	): number {
		let t = this.T;
		if (gap === undefined || gap < 2) {
			return t;
		}
		const from = t;
		t = this.develop(team, t, gap, undefined, undefined, true).t;
		if (this.holder !== undefined && this.teamOf(this.holder) === team) {
			t = this.bringUp(team, t);
			// (What the clock has left once he is up the floor - and all of
			// it run through fast but for the last of it.)
			const out = e.reason === "noShot" ? FINAL_LOOK : FINAL_DRIBBLE;
			t += Math.max(out, gap * 1000 - (t - from));
			this.hurry(from, t - out);
		}
		return t;
	}

	private beatJumpBall(e: RawEvent, i: number) {
		// The opening tip: the picture starts on the whole building while
		// they take their places, then cuts in for the toss - or, with the
		// lineups to call first, on the starters waiting at their benches.
		const opening = this.beats.length === 0 && this.shots.length === 0;
		const T =
			opening && this.introKind && this.clips.length === 0 && !this.intro
				? this.callLineups(this.T)
				: this.T;
		const winnerTeam: Side = e.t === 0 ? 1 : 0;
		const jumper = e.pid as number;
		const loser = e.pid2 as number;
		const c = { x: COURT_W / 2, y: COURT_H / 2 };
		const side = (t: Side) => (t === 1 ? -1 : 1); // each team lines up on the side it defends
		// Out of the huddles for overtime, the ball comes out with them:
		// whoever has it - or picks it up off the floor - carries it out and
		// gives it to the official at the circle once he is at his place.
		if (!opening) {
			this.pickUpLoose(T);
		}
		const carrier = opening ? undefined : this.holder;
		let out = T;
		let ready = T;
		for (const t of [0, 1] as const) {
			const j0 = t === winnerTeam ? jumper : loser;
			const atCircle = this.go(
				j0,
				{ x: c.x + side(t) * 1.3, y: c.y },
				T,
				WALK,
				"walk",
				attackDir(t),
			);
			this.lookAt(j0, atCircle, c);
			ready = Math.max(ready, atCircle);
			if (j0 === carrier) {
				out = atCircle;
			}
			// Round the circle, a step outside it, the two teams in turn - the
			// way the official backs out after the toss left clear - and the
			// smallest man back behind his own jumper, in case it is lost.
			const rest = this.slots(t)
				.filter((p) => p !== j0)
				.sort(
					(a, b) => (this.rank.get(a) ?? 4) - (this.rank.get(b) ?? 4) || a - b,
				);
			rest.forEach((pid, j) => {
				const a = j > 0 ? TIP_SLOTS[t][j - 1] : undefined;
				const target =
					a === undefined
						? {
								x: c.x + side(t) * TIP_BACK,
								y: c.y + (t === 0 ? 2.5 : -2.5),
							}
						: {
								x: c.x + Math.cos((a * Math.PI) / 180) * TIP_RING,
								y: c.y + Math.sin((a * Math.PI) / 180) * TIP_RING,
							};
				const arrived = this.go(
					pid,
					target,
					T + j * 60,
					WALK,
					"walk",
					attackDir(t),
				);
				this.lookAt(pid, arrived, c);
				ready = Math.max(ready, arrived);
				if (pid === carrier) {
					out = arrived;
				}
			});
		}
		const held = { x: c.x, y: c.y, z: 5 };
		let handed = T;
		if (carrier !== undefined) {
			handed = this.tossTo(held, out + 150);
			// (The walk out run through fast.)
			this.hurry(T, Math.min(ready, out) - 1200, true);
		} else {
			this.rest(T, held);
		}
		const toss = Math.max(
			T + 600,
			ready + 200,
			opening && !this.intro ? T + 3000 : 0,
			handed + 800,
		);
		if (opening && !this.intro) {
			this.shots.push({ t0: T, t1: toss - 600, kind: "wide", stretch: false });
		}
		const apex = { x: c.x, y: c.y, z: 12.3 };
		this.effect("toss", toss);
		this.fly(toss, toss + 520, held, apex);
		this.act(jumper, "block", toss + 120, toss + 900, {
			face: attackDir(winnerTeam),
			jump: [0.1, 0.9, 2.8],
		});
		this.act(loser, "block", toss + 160, toss + 940, {
			face: attackDir(other(winnerTeam)),
			jump: [0.1, 0.9, 2.5],
		});
		const receiver = this.slots(winnerTeam).find((p) => p !== jumper) ?? jumper;
		this.jumps.push([Math.min(T, this.lineupsFrom ?? T), toss + 520]);
		this.lineupsFrom = undefined;
		this.fly(toss + 520, toss + 1000, apex, { pid: receiver });
		this.act(receiver, "catch", toss + 900, toss + 1080);
		this.hold(receiver, toss + 1000, "hold");
		this.setOffense(toss + 520, winnerTeam);
		this.phase = "tip";
		this.motionTeam = undefined;
		this.beat(i, e.type, toss + 520, toss + 1300);
	}

	// A man coming on has been down at the scorer's table a while, waiting
	// for the whistle - got there from his chair while play went on, if he
	// has been sitting long enough to have.
	private checkIn(
		pid: number,
		team: Side,
		k: number,
		t1: number,
	): CheckIn | undefined {
		const tr = this.track(pid);
		const last = tr?.shown.filter(([ts]) => ts <= t1).at(-1);
		if (!tr || last?.[1]) {
			return undefined;
		}
		const sat = Math.max(
			last?.[0] ?? 0,
			this.checkIns.findLast((c) => c.pid === pid)?.t1 ?? 0,
		);
		const path = checkInPath(this.seatOf(pid), team, k);
		let len = 0;
		for (let n = 1; n < path.length; n++) {
			len += dist(path[n - 1]!, path[n]!);
		}
		const walkMs = (len / CHECK_IN_WALK) * 1000;
		const strip = t1 - STRIP_MS - 200;
		const kneel = Math.max(
			strip - 2500 - hash01(pid, t1) * 4000,
			sat + 1500 + walkMs,
		);
		if (kneel > strip - 600) {
			return undefined;
		}
		const ci: CheckIn = {
			pid,
			team,
			path,
			t0: kneel - walkMs,
			kneel,
			strip,
			t1,
		};
		this.checkIns.push(ci);
		return ci;
	}

	private beatSub(e: RawEvent, i: number, d: Side | undefined) {
		const T = this.T;
		const team: Side = d ?? 0;
		const on: number[] = Array.isArray(e.pids) ? e.pids : [];
		const off: number[] = Array.isArray(e.pidsOff) ? e.pidsOff : [];
		if (this.holder !== undefined && off.includes(this.holder)) {
			this.deadBall(T);
		}
		// Play goes on once the men going off are off the floor and the men
		// coming on are out there.
		let ready = T + 1300;
		// Where each man coming on takes over: where the man he is in for
		// stood - a step off anybody else stood there, not on top of him.
		const taken: Pt[] = [];
		const roomAt = (P: Pt): Pt => {
			if (dist(inPlay(P), P) > 0.5) {
				return P;
			}
			const near = [...this.lineup[0], ...this.lineup[1]]
				.filter((q) => !off.includes(q))
				.map((q) => this.posOf(q))
				.concat(taken);
			const clear = (Q: Pt) => near.every((o) => dist(o, Q) >= 3);
			if (clear(P)) {
				return P;
			}
			for (const r of [3, 4.5, 6]) {
				let best: Pt | undefined;
				for (let k = 0; k < 12; k++) {
					const a = (k * Math.PI) / 6;
					const Q = inPlay({
						x: P.x + Math.cos(a) * r,
						y: P.y + Math.sin(a) * r,
					});
					if (clear(Q) && (!best || dist(Q, P) < dist(best, P))) {
						best = Q;
					}
				}
				if (best) {
					return best;
				}
			}
			return P;
		};
		off.forEach((pid, j) => {
			const from = this.posOf(pid);
			const at = roomAt(from);
			taken.push(at);
			const incoming = on[j];
			// Off the floor at a jog, back to his chair, where he sits down.
			const seat = this.seatOf(pid);
			const t0 = T + j * 80;
			const gone = this.go(pid, seat, t0, JOG, "run");
			this.show(pid, gone, false);
			const offFloor =
				from.y > 0 && seat.y < 0
					? from.y / (from.y - seat.y)
					: from.y <= 0
						? 0
						: 1;
			ready = Math.max(ready, t0 + (gone - t0) * offFloor + 150);
			if (incoming !== undefined) {
				const tr = this.track(incoming);
				if (tr) {
					// Up off the bench and out to take his man - or, sent straight
					// back in on his way off, from wherever he has got to.
					const t0 = T + j * 80;
					const last = tr.moves.at(-1);
					if (last && last.t1 > t0 && last.t0 < t0) {
						const here = this.posAt(incoming, t0);
						last.to = here;
						last.t1 = t0;
						this.pos.set(incoming, here);
					} else {
						const ci = this.checkIn(incoming, team, j, t0);
						this.pos.set(
							incoming,
							ci ? ci.path.at(-1)! : this.seatOf(incoming),
						);
					}
					tr.shown = tr.shown.filter(([ts, on]) => on || ts <= t0);
					this.free.set(incoming, t0);
					this.show(incoming, t0, true);
					const there = this.go(incoming, at, t0, RUN * 0.8, "run");
					ready = Math.max(ready, there);
					const list = this.replaced.get(pid) ?? [];
					list.push({ by: incoming, from: T, on: there, until: Infinity });
					this.replaced.set(pid, list);
					for (const r of this.replaced.get(incoming) ?? []) {
						r.until = Math.min(r.until, T);
					}
					// They slap hands going by.
					let meet = t0;
					let close = Infinity;
					for (let t = t0; t <= t0 + 6000; t += 40) {
						const d = dist(this.posAt(pid, t), this.posAt(incoming, t));
						if (d < close) {
							close = d;
							meet = t;
						}
					}
					if (close < 4.5) {
						this.gesture(pid, "slap", meet - 260, meet + 260, incoming);
						this.gesture(incoming, "slap", meet - 260, meet + 260, pid);
					}
				}
			}
		});
		this.lineup[team] = [
			...this.lineup[team].filter((p) => !off.includes(p)),
			...on.filter((p) => !this.lineup[team].includes(p)),
		];
		this.beat(i, e.type, T, ready);
	}

	trackScore(e: RawEvent) {
		if (e.type === "stat" && e.s === "pts" && (e.t === 0 || e.t === 1)) {
			this.score[e.t === 0 ? 1 : 0] += e.amt ?? 0;
		}
		if (e.type === "sub" && e.silent) {
			const t: Side = e.t === 0 ? 1 : 0;
			const on: number[] = e.pids ?? [];
			const off: number[] = e.pidsOff ?? [];
			for (const pid of off) {
				this.show(pid, this.T, false);
			}
			for (const pid of on) {
				this.show(pid, this.T, true);
				// (Back on: nobody is on in his place any more - see mark.)
				for (const r of this.replaced.get(pid) ?? []) {
					r.until = Math.min(r.until, this.T);
				}
			}
			this.lineup[t] = [
				...this.lineup[t].filter((p) => !off.includes(p)),
				...on,
			];
		}
	}

	// A HIGHLIGHT REEL'S NEXT CLIP (see filterPlayerHighlights): not however
	// everybody got from the last one to here, but a cut - to the trip it
	// comes from, under way: the offense in its set in the half court with
	// the ball up top, every man on his man.
	newClip(e: RawEvent) {
		const pid =
			typeof e.pid === "number" && this.team.has(e.pid) ? e.pid : undefined;
		// (A line with nobody named - the ball out of bounds - names a team.)
		const own: Side | undefined =
			pid !== undefined
				? this.teamOf(pid)
				: e.t === 0
					? 1
					: e.t === 1
						? 0
						: undefined;
		if (own === undefined) {
			return;
		}
		const T = this.T;
		const offense = /^blk|^stl$|^drb$/.test(e.type) ? other(own) : own;
		this.cuts.push(T);
		this.clips.push(T);
		this.guarding.clear();
		this.setOffense(T, offense);
		const spots = this.setSpots(offense, 0);
		const dir = attackDir(offense);
		const warp = (q: number, to: Pt, face: 1 | -1) => {
			const tr = this.track(q);
			if (!tr) {
				return;
			}
			tr.moves.push({
				t0: T,
				t1: T + 1,
				from: { ...this.posOf(q) },
				to: { ...to },
				anim: "run",
			});
			tr.faces.push([T, face]);
			this.pos.set(q, { ...to });
			this.face.set(q, face);
			this.free.set(q, Math.max(this.free.get(q) ?? 0, T + 1));
		};
		this.slots(offense).forEach((q, j) => {
			warp(q, spots[j] ?? spots[0]!, dir);
		});
		this.slots(other(offense)).forEach((q, j) => {
			warp(q, guardSpot(offense, spots[j] ?? spots[0]!), -dir as 1 | -1);
		});
		this.hold(this.slots(offense)[0]!, T + 1, "dribble");
		this.phase = "set";
		this.motionTeam = offense;
		this.motion = 0;
		this.inboundAt = undefined;
		this.lastClock = undefined;
	}

	noteClock(e: RawEvent) {
		if (typeof e.clock === "number") {
			this.lastClock = e.clock;
		}
	}

	// ON HIS WAY. Off the ball on the offense, a man going somewhere in two
	// runs the same way - to a spot the set had him at, then on from it - with
	// a moment between them he does nothing in, doesn't pull up dead there
	// and set off again: he takes the first a little easier and runs on into
	// the second (see FLOW_MS) - no slower than IN_STRIDE_PACE, though: past
	// that, he stops what is left.
	private flowOn() {
		const RUNS = new Set<AnimName>(["run", "sprint", "jog"]);
		const holds = new Map<number, [number, number][]>();
		this.ball.forEach((b, i) => {
			if (b.kind === "hold") {
				const list = holds.get(b.pid) ?? [];
				list.push([b.t0, this.ball[i + 1]?.t0 ?? Infinity]);
				holds.set(b.pid, list);
			}
		});
		const offenseAt = (t: number): Side | undefined =>
			this.poss[lastBy(this.poss, t, (x) => x[0])]?.[1];
		for (const tr of this.tracks.values()) {
			const ms = tr.moves;
			for (let i = 0; i + 1 < ms.length; i++) {
				const a = ms[i]!;
				const b = ms[i + 1]!;
				const gap = b.t0 - a.t1;
				if (
					!RUNS.has(a.anim) ||
					!RUNS.has(b.anim) ||
					gap <= FLOW_MS ||
					gap > FLOW_ON_MS ||
					dist(a.to, b.from) > 0.25 ||
					offenseAt(a.t1) !== tr.team
				) {
					continue;
				}
				const la = dist(a.from, a.to);
				const lb = dist(b.from, b.to);
				if (la < 6 || lb < CUT_RUN_UP) {
					continue;
				}
				const cos =
					((a.to.x - a.from.x) * (b.to.x - b.from.x) +
						(a.to.y - a.from.y) * (b.to.y - b.from.y)) /
					(la * lb);
				// (Not across a cut of the picture, everybody somewhere new.)
				if (
					cos < 0.5 ||
					a.t1 - a.t0 < 100 ||
					this.cuts.some((c) => c >= a.t0 - 1 && c <= b.t0 + 1) ||
					tr.acts.some((x) => x.t1 > a.t1 - 50 && x.t0 < b.t0 + 50) ||
					(holds.get(tr.pid) ?? []).some(([h0, h1]) => h1 > a.t0 && h0 < b.t0)
				) {
					continue;
				}
				const t1 = Math.min(
					b.t0 - FLOW_MS / 2,
					a.t0 + (la / IN_STRIDE_PACE) * 1000,
				);
				if (t1 <= a.t1) {
					continue;
				}
				a.t1 = t1;
				a.v1 = undefined;
				if (b.v0 === 0) {
					b.v0 = undefined;
				}
			}
		}
	}

	// OFF THE BALL, ON THE MOVE.
	//
	// The set says where each man goes when his part in it comes. In
	// between, a man left out of it while the ball is worked somewhere else
	// does not stand rooted to his spot for seconds on end. Out on the
	// perimeter he drifts a few feet along the arc - away from the teammate
	// nearest him - and after a moment back the other way, for as long as he
	// is left there; a big in close shifts round the rim, and steps out of
	// the lane before the count of three; a man left back at the other end
	// trails up the floor. His man goes with him. Before anything that needs
	// him where the set put him (a catch, a shot, a screen) he is back on
	// his spot in time, and out on the perimeter he never steps inside the
	// line.
	private liven() {
		const atTime = <X>(list: [number, X][], t: number, fallback: X): X => {
			let v = fallback;
			for (const [t0, x] of list) {
				if (t0 > t) {
					break;
				}
				v = x;
			}
			return v;
		};
		// A man's last run begun by t (his runs are in order by now too).
		const runAt = (tr: Track, t: number): Move | undefined => {
			let lo = 0;
			let hi = tr.moves.length - 1;
			let found: Move | undefined;
			while (lo <= hi) {
				const mid = (lo + hi) >> 1;
				if (tr.moves[mid]!.t0 <= t) {
					found = tr.moves[mid];
					lo = mid + 1;
				} else {
					hi = mid - 1;
				}
			}
			return found;
		};
		// The ball's schedule is in order by now: the last of it begun by t.
		const ballAt = (t: number): number => {
			let lo = 0;
			let hi = this.ball.length - 1;
			let k = 0;
			while (lo <= hi) {
				const mid = (lo + hi) >> 1;
				if (this.ball[mid]!.t0 <= t) {
					k = mid;
					lo = mid + 1;
				} else {
					hi = mid - 1;
				}
			}
			return k;
		};
		// (And a timeout or the end of a period: the ball goes to the huddle
		// in a man's hands, which is not the ball in play - the huddle stood
		// round it, not men sent off on runs.)
		const stops = this.beats.filter(
			(bt) =>
				bt.type === "ft" ||
				bt.type === "missFt" ||
				bt.type === "timeout" ||
				bt.type === "endOfPeriod",
		);
		// How long from t the ball stays in play: in a man's hands, or on its
		// way between two - and no free throw, timeout or break.
		const liveUntil = (a: number): number => {
			let i = ballAt(a);
			let end = Infinity;
			for (; i < this.ball.length; i++) {
				if (!liveSeg(this.ball, i)) {
					end = Math.max(a, this.ball[i]!.t0);
					break;
				}
			}
			for (const bt of stops) {
				if (bt.end > a && bt.preStart < end) {
					end = Math.max(a, bt.preStart);
				}
			}
			return end;
		};
		// Whether the ball is his at any time from a to b.
		const hasBall = (pid: number, a: number, b: number): boolean => {
			const i = ballAt(a);
			for (let k = i; k < this.ball.length && this.ball[k]!.t0 < b; k++) {
				const s = this.ball[k]!;
				if (s.kind === "hold" && s.pid === pid) {
					return true;
				}
			}
			return false;
		};
		// Things a man does where the set put him to do them: a catch (the
		// pass is thrown to where he stands), a shot, a screen, a post-up, a
		// contest of the shot - or picking up the ball where it lies.
		const PLANTED = new Set<AnimName>([
			"catch",
			"pickup",
			"snatch",
			"screen",
			"postUp",
			"shoot",
			"setShot",
			"fade",
			"hook",
			"layup",
			"fingerRoll",
			"floater",
			"powerLayup",
			"scoop",
			"reverseLayup",
			"dunk",
			"dunk1",
			"tomahawk",
			"windmill",
			"contest",
			"block",
		]);
		type Still = {
			from: number;
			to: number;
			at: Pt;
			// His next run, and whether before it he does anything where the
			// set put him - a catch, for one.
			next?: number;
			planted: boolean;
			catching: boolean;
		};
		// When and where a man stands still: from the end of one thing he does
		// to the start of the next.
		const stills = (tr: Track): Still[] => {
			const busy = [
				...tr.moves.map((m, i) => ({
					t0: m.t0,
					t1: m.t1,
					move: i as number | undefined,
					planted: false,
					catching: false,
				})),
				...tr.acts.map((a) => ({
					t0: a.t0,
					t1: a.t1,
					move: undefined,
					planted: PLANTED.has(a.anim),
					catching: a.anim === "catch",
				})),
			].sort((a, b) => a.t0 - b.t0 || (a.move === undefined ? 1 : -1));
			const out: Still[] = [];
			let end = -Infinity;
			let at: Pt = tr.start;
			busy.forEach((b, i) => {
				if (b.t0 > end && Number.isFinite(end)) {
					let k = i;
					let planted = false;
					let catching = false;
					while (k < busy.length && busy[k]!.move === undefined) {
						planted ||= busy[k]!.planted;
						catching ||= busy[k]!.catching;
						k++;
					}
					out.push({
						from: end,
						to: b.t0,
						at,
						next: busy[k]?.move,
						planted,
						catching,
					});
				}
				end = Math.max(end, b.t1);
				if (b.move !== undefined) {
					at = tr.moves[b.move]!.to;
				}
			});
			return out;
		};
		// A run from where he stood - unless it is the picture cutting to him
		// somewhere else.
		// (Where each run set off from before any of this moved its start:
		// what is fast enough for it goes by that.)
		const setFrom = new Map<Move, Pt>();
		// From Q, can he still make run m in its time: no farther than it
		// was to have him go, or than a man going for it gets in that long
		// from standing - not an average pace that leaves no time to get going
		// or to pull up.
		const makes = (Q: Pt, m: Move | undefined): boolean =>
			!m ||
			m.t1 - m.t0 <= 1 ||
			dist(Q, m.to) <=
				Math.max(
					dist(setFrom.get(m) ?? m.from, m.to),
					reachIn(m.t1 - m.t0, RUN),
				);
		// (Out of the lane, or going with his man, it has to be done: then
		// as long as it is no faster on average than a run.)
		const makesAtAll = (Q: Pt, m: Move | undefined): boolean => {
			if (!m || m.t1 - m.t0 <= 1) {
				return true;
			}
			const secs = (m.t1 - m.t0) / 1000;
			return (
				dist(Q, m.to) / secs <=
				Math.max(dist(setFrom.get(m) ?? m.from, m.to) / secs, RUN)
			);
		};
		// (Farther now than it was to have him go, he sets off for it as much
		// sooner as he needs to make it - not before he is there to.)
		const setOff = (
			tr: Track,
			next: number | undefined,
			from: Pt,
			there: number,
		) => {
			const m = next === undefined ? undefined : tr.moves[next];
			if (m && (m.t1 - m.t0 > 1 || dist(m.from, m.to) > 0.01)) {
				if (!setFrom.has(m)) {
					setFrom.set(m, m.from);
				}
				m.from = { ...from };
				const need = runMs(dist(from, m.to), RUN, 0, BURST);
				if (m.t1 - m.t0 > 1 && m.t1 - m.t0 < need) {
					// (Nor in the middle of something he is doing there.)
					let t0 = Math.max(there + 60, m.t1 - need);
					for (const a of tr.acts) {
						if (a.t1 > t0 && a.t0 < m.t0) {
							t0 = Math.max(t0, a.t1);
						}
					}
					m.t0 = Math.min(m.t0, t0);
				}
			}
		};
		const all = [...this.tracks.values()];
		const still = new Map(all.map((tr) => [tr.pid, stills(tr)]));
		// The first of a man's spells of standing begun after t.
		const stillAfter = (list: Still[], t: number): number => {
			let lo = 0;
			let hi = list.length;
			while (lo < hi) {
				const mid = (lo + hi) >> 1;
				if (list[mid]!.from <= t) {
					lo = mid + 1;
				} else {
					hi = mid;
				}
			}
			return lo;
		};
		const shownAt = (tr: Track, a: number, b: number) =>
			atTime(tr.shown, a, false) &&
			!tr.shown.some(([t0, on]) => t0 > a && t0 < b && !on);
		// Spells of drifting before a run of his: the run starts from where
		// the last of them left him - and the next spell, from there. (Not
		// from where a later one does: a spell he stands in on into the next
		// trip, the ball the other way - he is no further on in this one for
		// it.)
		const taken = new Map<string, { t: number; at: Pt }[]>();
		const take = (tr: Track, w: Still, at: Pt) => {
			const k = `${tr.pid}:${w.next}`;
			taken.set(k, [...(taken.get(k) ?? []), { t: w.from, at }]);
		};
		const free = (_tr: Track, w: Still) => w.next !== undefined;
		const startOf = (tr: Track, w: Still): Pt => {
			let at = w.at;
			let t = -Infinity;
			for (const e of taken.get(`${tr.pid}:${w.next}`) ?? []) {
				if (e.t <= w.from && e.t >= t) {
					at = e.at;
					t = e.t;
				}
			}
			return at;
		};
		// From when the ball is in play again at or after t: a loose ball
		// picked up, a rebound come down into somebody's hands.
		const liveFrom = (t: number): number => {
			const i = ballAt(t);
			for (let k = i; k < this.ball.length; k++) {
				if (liveSeg(this.ball, k)) {
					return Math.max(t, this.ball[k]!.t0);
				}
			}
			return Infinity;
		};
		// Where a man is at t, on the runs he has by now.
		const whereAt = (tr: Track, t: number): Pt => {
			const m = runAt(tr, t);
			if (!m) {
				return tr.start;
			}
			if (t >= m.t1) {
				return m.to;
			}
			const e = 0.5 - 0.5 * Math.cos((Math.PI * (t - m.t0)) / (m.t1 - m.t0));
			return {
				x: m.from.x + (m.to.x - m.from.x) * e,
				y: m.from.y + (m.to.y - m.from.y) * e,
			};
		};
		// Every time the ball goes from one man to another: when it gets
		// there, to whom, from whom.
		const catches: { t: number; pid: number; from: number }[] = [];
		for (const s of this.ball) {
			if (
				s.kind === "fly" &&
				"pid" in s.from &&
				"pid" in s.to &&
				s.from.pid !== s.to.pid
			) {
				catches.push({ t: s.t1, pid: s.to.pid, from: s.from.pid });
			}
		}
		catches.sort((a, b) => a.t - b.t);
		// Whose ball it is at t (on its way to him counts).
		const holderAt = (t: number): number | undefined => {
			const s = this.ball[ballAt(t)];
			return s?.kind === "hold"
				? s.pid
				: s?.kind === "fly" && "pid" in s.to
					? s.to.pid
					: undefined;
		};
		// The first of them at or after t.
		const firstCatch = (t: number): number => {
			let lo = 0;
			let hi = catches.length;
			while (lo < hi) {
				const mid = (lo + hi) >> 1;
				if (catches[mid]!.t < t) {
					lo = mid + 1;
				} else {
					hi = mid;
				}
			}
			return lo;
		};
		// Spots men are already headed for, off the ball, and till when.
		const claims: {
			team: Side;
			pid: number;
			at: Pt;
			t0: number;
			t1: number;
		}[] = [];
		const added: { tr: Track; move: Move }[] = [];
		const addedActs: { tr: Track; act: Act }[] = [];
		// A spell of standing can run on through the ball going dead and back
		// in play - a shot, a block, the offensive board - and the play going
		// on: he is no more rooted to the spot after it than before. One piece
		// for each stretch of it with the ball in play; each starts where the
		// last left him (see startOf).
		const pieces = (w: Still): Still[] => {
			const out: Still[] = [];
			let a = w.from;
			while (a < w.to && out.length < 8) {
				const f = liveFrom(a);
				if (f >= w.to) {
					break;
				}
				const e = Math.min(w.to, liveUntil(f));
				out.push({ ...w, from: a, to: e >= w.to - 1 ? w.to : e });
				a = Math.max(e, f) + 50;
			}
			return out.length > 0 ? out : [w];
		};
		for (const tr of all) {
			for (const w of still.get(tr.pid)!.flatMap(pieces)) {
				if (w.to - w.from < 1000 || !free(tr, w)) {
					continue;
				}
				// The part of it with the ball in play - and not in his hands -
				// and still his team's ball. Before something he does right
				// where he stands (a catch and shoot), he is back on his spot,
				// set, before the ball comes.
				const from = liveFrom(w.from);
				const live = Math.min(w.to, liveUntil(from));
				const change = this.poss.find(([t0]) => t0 > from && t0 < live)?.[0];
				const until =
					Math.min(live, change ?? Infinity) - (w.planted ? 700 : 0);
				if (hasBall(tr.pid, from, until)) {
					continue;
				}
				const team = atTime(this.poss, from, 1 as Side);
				if (
					until - from < 1000 ||
					team !== tr.team ||
					!shownAt(tr, w.from, w.to)
				) {
					continue;
				}
				const rim = { x: rimX(team), y: COURT_H / 2 };
				const P = startOf(tr, w);
				// Where his teammates are at tt - or are headed, to stand there in
				// the next few seconds.
				const othersAt = (tt: number): Pt[] => [
					...all
						.filter(
							(o) =>
								o !== tr && o.team === tr.team && atTime(o.shown, tt, false),
						)
						.flatMap((o) => {
							const list = still.get(o.pid)!;
							const out = [whereAt(o, tt)];
							for (
								let k = stillAfter(list, tt);
								k < list.length && list[k]!.from < tt + 4000;
								k++
							) {
								out.push(list[k]!.at);
							}
							return out;
						}),
					...claims
						.filter(
							(c) =>
								c.team === team &&
								c.pid !== tr.pid &&
								c.t0 <= tt + 4000 &&
								c.t1 >= tt,
						)
						.map((c) => c.at),
				];
				// In the frontcourt (see below) - or not.
				const front = Math.abs(P.x - rim.x) <= COURT_W / 2 - 6;
				// Left back at the other end while his team has it - or at half
				// court, having thrown it ahead: he trails up the floor to the
				// top of the play - if he can still get where he goes next in
				// time from it - wherever up there nobody else is.
				if (!w.planted && !front) {
					const deep = this.rand(27, 31);
					const y0 = Math.min(38, Math.max(12, P.y));
					const others = othersAt(from + 1500);
					const nm = w.next === undefined ? undefined : tr.moves[w.next];
					// (Up there, he has to be: where his next run is still one a
					// man makes, if anywhere is.)
					const spots = [y0, y0 - 8, y0 + 8, y0 - 14, y0 + 14]
						.filter((y) => y >= 8 && y <= COURT_H - 8)
						.map((y) => clampPt(spot(team, deep, y)))
						.map((q, k) => ({
							q,
							room: Math.min(9, ...others.map((m) => dist(m, q))) - k * 0.5,
						}))
						.sort((a, b) => b.room - a.room);
					const Q = (spots.find((x) => makes(x.q, nm)) ?? spots[0]!).q;
					const t0 = from + 300 + this.rng() * 300;
					const room = until - t0;
					const d = dist(P, Q);
					if (makesAtAll(Q, nm) && room > 400 && d / (room / 1000) <= SPRINT) {
						const speed = Math.max(JOG, d / (room / 1000));
						added.push({
							tr,
							move: {
								t0,
								t1: t0 + (d / speed) * 1000,
								from: { ...P },
								to: Q,
								anim: speed >= RUN ? "sprint" : "run",
							},
						});
						setOff(tr, w.next, Q, t0 + (d / speed) * 1000);
						take(tr, w, Q);
					}
					continue;
				}
				// In the frontcourt: out on the perimeter (and kept behind the
				// line), in close round the rim, or anywhere between.
				const out = behindArc(team, P) === undefined;
				const big = !out && dist(P, rim) < 17;
				if (!front) {
					continue;
				}
				// The lane, where he cannot stand three seconds.
				const base = team === 0 ? 0 : COURT_W;
				const inLane = (q: Pt) =>
					Math.abs(q.x - base) < 19 && Math.abs(q.y - COURT_H / 2) < 8;
				// His man: whoever is marking him then - or, failing that, the
				// nearest of them standing there with him.
				const marker = (s0: number, d0: number) => {
					let best: { tr: Track; w: Still; his: boolean } | undefined;
					for (const o of all) {
						if (o.team === tr.team || !atTime(o.shown, s0, false)) {
							continue;
						}
						const ow = still
							.get(o.pid)!
							.find(
								(x) =>
									x.from <= s0 &&
									x.to >= s0 + d0 &&
									free(o, x) &&
									(!x.planted || w.planted),
							);
						const last = runAt(o, s0);
						const his = last !== undefined && this.marking.get(last) === tr.pid;
						// (The man marking him, sagged off him in help, goes too.)
						if (!ow || dist(ow.at, P) >= (his ? 24 : 10)) {
							continue;
						}
						if (
							!best ||
							(his && !best.his) ||
							(his === best.his && dist(ow.at, P) < dist(best.w.at, P))
						) {
							best = { tr: o, w: ow, his };
						}
					}
					return best;
				};
				// What he does with himself while the ball is worked somewhere
				// else - all of it for a reason. The ball goes to a teammate: if
				// that leaves him crowding it, or on top of another man, he gets
				// himself to the open spot that spreads the floor - straight
				// there, a man with somewhere to be. Just rid of it himself, he
				// doesn't stand and watch it: he cuts hard to the rim and out the
				// other side, or gets out of the way to an open spot. Left there
				// with nothing happening, he makes his man work: a hard step in
				// and a cut back out - to a new spot if there is one open, or to
				// his own; with a pass to come to him there, back to it just as
				// it comes. A big in close ducks in on his man, seals him, and
				// steps back out of the lane. In between he is set, ready for it.
				const steps: {
					t0: number;
					t1: number;
					from: Pt;
					to: Pt;
					anim: AnimName;
				}[] = [];
				let at = P;
				let t = from + this.rand(150, 350);
				// As long as getting there takes him (see motion.ts) - or, a
				// jab, one quick lunge of a step.
				const dur = (a: Pt, b: Pt, speed: number) =>
					speed === JAB
						? 220 + 110 * dist(a, b)
						: Math.max(240, runMs(dist(a, b), speed));
				const room = (ms: number) => t + ms <= until - 100;
				const step = (to: Pt, speed: number, anim: AnimName, pause = 0) => {
					const t1 = t + dur(at, to, speed);
					steps.push({ t0: t, t1, from: at, to, anim });
					at = to;
					t = t1 + pause;
				};
				// There, if he has the time to be.
				const goTo = (
					Q: Pt,
					speed: number,
					anim: AnimName,
					pause: number,
				): boolean => {
					if (!room(dur(at, Q, speed))) {
						return false;
					}
					step(Q, speed, anim, pause);
					return true;
				};
				// A hard step in at his man, to send him the wrong way - then
				// there, if he has the time for both.
				const jabTo = (
					Q: Pt,
					speed: number,
					anim: AnimName,
					pause: number,
				): boolean => {
					const u = unitVec(at, rim);
					// (His weight goes a step's worth less far than his foot.)
					const L = this.rand(2.2, 3) * 0.55;
					const J = clampPt({ x: at.x + u.x * L, y: at.y + u.y * L });
					const plant = this.rand(60, 110);
					if (!room(dur(at, J, JAB) + plant + dur(J, Q, speed))) {
						return false;
					}
					step(J, JAB, "run", plant);
					step(Q, speed, anim, pause);
					return true;
				};
				const ballSpot = (tt: number): Pt => {
					const h = holderAt(tt);
					const htr = h === undefined ? undefined : this.track(h);
					return htr ? whereAt(htr, tt) : rim;
				};
				// Somewhere he can still make his next run from, in its time -
				// no faster than he would have gone anyway, or a hard run.
				const next = w.next === undefined ? undefined : tr.moves[w.next];
				const fits = (Q: Pt): boolean => makes(Q, next);
				// The open spot the floor needs filled, from where he is: well
				// away from the ball and from his teammates, not far off, and on
				// his way.
				const openSpot = (tt: number, near: Pt, least: number, most = 18) => {
					const B = ballSpot(tt);
					const others = othersAt(tt);
					let best: Pt | undefined;
					let score = -Infinity;
					for (const name of big ? BIG_SPOTS : RESPACE_SPOTS) {
						const S = this.spotFor(team, 1, name);
						const d = dist(S, near);
						if (
							d < least ||
							d > most ||
							dist(S, B) < (big ? 8 : 13) ||
							!fits(S)
						) {
							continue;
						}
						const room = Math.min(99, ...others.map((m) => dist(m, S)));
						if (room < (big ? 7 : 10)) {
							continue;
						}
						const sc = Math.min(room, 18) - d * 0.6;
						if (sc > score) {
							score = sc;
							best = S;
						}
					}
					return best;
				};
				const claim = (Q: Pt, t0: number) =>
					claims.push({ team, pid: tr.pid, at: Q, t0, t1: until });
				// A big ducks in on his man toward the ball, seals him, and is
				// back out where he was before the count of three.
				const seal = (): boolean => {
					const B = ballSpot(t);
					const side = B.y >= rim.y ? 1 : -1;
					const S = clampPt({
						x: rim.x - attackDir(team) * this.rand(4, 6),
						y: rim.y + side * this.rand(4.5, 6),
					});
					const back = at;
					const hold = this.rand(600, 900);
					if (
						dist(S, at) < 2 ||
						dist(S, B) < 7 ||
						othersAt(t).some((m) => dist(m, S) < 4) ||
						!room(dur(at, S, 9) + hold + dur(S, back, 8))
					) {
						return false;
					}
					step(S, 9, "run");
					addedActs.push({
						tr,
						act: { t0: t, t1: t + hold, anim: "fight", look: B },
					});
					t += hold;
					step(back, 8, "back", 300);
					return true;
				};
				// Out of the lane: to the side of it he is on - or the other, or a
				// step up or down it, whichever no teammate has.
				const outside = (() => {
					const near = at.y < COURT_H / 2 ? -1 : 1;
					const others = othersAt(t + 900);
					let best: Pt | undefined;
					let room = -Infinity;
					for (const side of [near, -near]) {
						for (const dx of [0, -3, 3]) {
							const q = inPlay({
								x: at.x + dx,
								y: COURT_H / 2 + side * 9,
							});
							const r =
								Math.min(30, ...others.map((m) => dist(m, q))) -
								dist(q, at) * 0.15;
							if (r > room) {
								room = r;
								best = q;
							}
						}
					}
					return best!;
				})();
				// Out of the lane before the official counts three - and, there
				// for something (a lob, a dump-off), back in on his spot just as
				// it comes.
				const back = dur(outside, P, 14);
				const pace = w.planted ? 14 : 9;
				if (
					big &&
					inLane(at) &&
					(w.planted
						? until - (from + 500) - dur(at, outside, pace) - back > 300
						: makesAtAll(outside, next))
				) {
					t = Math.max(t, from + (w.planted ? 500 : 900));
					const t0 = t;
					if (goTo(outside, pace, "run", 300)) {
						claim(outside, t0);
						if (w.planted) {
							t = Math.max(t, until - back - 150);
							step(P, 14, "run");
						}
					}
				}
				// Sent where a teammate is already standing: two men do not stand
				// on top of each other. Whoever got there second takes the open
				// spot beside it, straight off - unless it is his to do something
				// on (a catch, a screen), and the other's is not.
				const crowded = all.some((o) => {
					if (o === tr || o.team !== tr.team || !atTime(o.shown, t, false)) {
						return false;
					}
					const list = still.get(o.pid)!;
					const k = stillAfter(list, t + 300) - 1;
					const x = k >= 0 && list[k]!.to > t + 800 ? list[k] : undefined;
					if (!x || dist(x.at, at) >= (big ? 4 : 5.5) || w.planted) {
						return false;
					}
					return (
						x.planted ||
						x.from < w.from ||
						(x.from === w.from && tr.pid > o.pid)
					);
				});
				if (crowded) {
					const t0 = t;
					const Q = openSpot(t, at, 3);
					if (
						Q &&
						goTo(Q, this.rand(11, 14), dist(at, Q) < 8 ? "drift" : "run", 300)
					) {
						claim(Q, t0);
					}
				}
				// Just rid of it himself: on the move.
				let passed = false;
				for (
					let k = firstCatch(from - 900);
					k < catches.length && catches[k]!.t <= from + 150;
					k++
				) {
					passed ||= catches[k]!.from === tr.pid;
				}
				if (passed && !big && !w.planted) {
					t = Math.max(t, from + 80);
					const u = unitVec(at, rim);
					const deep = dist(at, rim) - 6;
					const cut = clampPt({ x: at.x + u.x * deep, y: at.y + u.y * deep });
					const after = openSpot(t + 1200, cut, 6, 30);
					const fast = this.rand(15, 18);
					const out = this.rand(12, 14);
					if (
						deep > 8 &&
						after &&
						this.rng() < 0.45 &&
						room(dur(at, cut, fast) + 120 + dur(cut, after, out))
					) {
						// Give and go: hard to the rim, and out the other side.
						claim(after, t);
						step(cut, fast, "sprint", 120);
						step(after, out, "run", this.rand(300, 600));
					} else {
						const t0 = t;
						const Q = openSpot(t, at, 5);
						if (Q && goTo(Q, this.rand(11, 13), "run", this.rand(300, 600))) {
							claim(Q, t0);
						}
					}
				}
				// From here on, in order: each time the ball moves (a teammate
				// catches it) he moves with it, if he needs to; in between, now
				// and then, he makes his man work - and with a pass to come to
				// him where he is, he is back there, set, just as it comes.
				const home = w.planted ? P : undefined;
				const react = () => this.rand(200, 380);
				const breather = () => this.rand(500, 1100);
				const busyMan = (): boolean => {
					if (big) {
						if (seal()) {
							return true;
						}
						const t0 = t;
						const Q = home ?? openSpot(t, at, 3, 12);
						if (Q && dist(Q, at) > 1 && goTo(Q, 9, "run", 300)) {
							if (!home) {
								claim(Q, t0);
							}
							return true;
						}
						return jabTo(at, 8, "back", 300);
					}
					const t0 = t;
					const Q = home ?? openSpot(t, at, 5);
					if (Q && jabTo(Q, this.rand(13, 16), "sprint", this.rand(300, 600))) {
						if (!home) {
							claim(Q, t0);
						}
						return true;
					}
					// Nowhere better to be: in a step, and back out to his spot -
					// or, as often, a step or two along the line one way or the
					// other, easy, keeping his spacing: not the same twitch over
					// and over.
					if (!home && this.rng() < 0.5) {
						const u = unitVec(at, rim);
						const others = othersAt(t);
						const L = this.rand(2, 3.5);
						// (Out behind the line, he stays behind it.)
						const deep =
							Math.abs(at.x - rim.x) < 14 - 5.25
								? Math.abs(at.y - rim.y) >= 22 - ON_THE_LINE
								: dist(at, rim) >= 23.75 - ON_THE_LINE;
						// (In bounds: down in the corner, the way along the line
						// toward the baseline runs out of floor in a step - he
						// goes the other way, up it.)
						const Q = [1, -1]
							.map((side) => {
								const q = onTheFloor({
									x: at.x - u.y * side * L,
									y: at.y + u.x * side * L,
								});
								return (deep ? behindArc(team, q) : undefined) ?? q;
							})
							.filter((q) => dist(q, at) >= L * 0.6)
							.map((q) => ({
								q,
								room: Math.min(99, ...others.map((m) => dist(m, q))),
							}))
							.sort((a, b) => b.room - a.room)[0]?.q;
						if (
							Q &&
							fits(Q) &&
							goTo(Q, this.rand(5, 7), "drift", this.rand(300, 600))
						) {
							return true;
						}
					}
					return jabTo(at, this.rand(12, 15), "run", this.rand(300, 600));
				};
				const last = home && w.catching && !big ? until - 1000 : until;
				let due = t + breather();
				let k = firstCatch(from - 300);
				for (let n = 0; n < 40; n++) {
					const c =
						k < catches.length && catches[k]!.t < until - 600
							? catches[k]
							: undefined;
					if (due < (c?.t ?? Infinity) && last - due >= 1200) {
						t = Math.max(t, due);
						if (!busyMan()) {
							due = Infinity;
							continue;
						}
						due = t + breather();
						continue;
					}
					if (!c) {
						break;
					}
					k++;
					if (
						home ||
						c.pid === tr.pid ||
						this.team.get(c.pid) !== team ||
						c.t + 200 < t
					) {
						continue;
					}
					const t0 = Math.max(t, c.t + react());
					const B = ballSpot(t0);
					const crowd =
						dist(at, B) < (big ? 7 : 12) ||
						othersAt(t0).some((m) => dist(m, at) < (big ? 6 : 8));
					if (!crowd && this.rng() >= 0.3) {
						continue;
					}
					const Q = openSpot(t0, at, crowd ? 3 : 6);
					if (!Q) {
						continue;
					}
					const d = dist(at, Q);
					const speed = this.rand(11, 14);
					const anim: AnimName = d < 8 ? "drift" : "run";
					const pause = this.rand(250, 500);
					const was = t;
					t = t0;
					if (
						(!big &&
							d > 7 &&
							this.rng() < 0.35 &&
							jabTo(Q, speed, anim, pause)) ||
						goTo(Q, speed, anim, pause)
					) {
						claim(Q, t0);
						due = t + breather();
					} else {
						t = was;
					}
				}
				if (home && w.catching && !big && until - t >= 900) {
					// The pass to him: in a step, and out to meet it.
					t = Math.max(t, until - 1000);
					jabTo(home, this.rand(13, 16), "sprint", 0);
				}
				if (home && dist(at, home) > 0.01) {
					step(home, 12, "run");
				}
				if (steps.length === 0) {
					continue;
				}
				for (const st of steps) {
					added.push({
						tr,
						move: {
							t0: st.t0,
							t1: st.t1,
							from: { ...st.from },
							to: st.to,
							anim: st.anim,
						},
					});
				}
				setOff(tr, w.next, at, steps.at(-1)!.t1);
				take(tr, w, at);
				// His man goes with him, each time he is still there to - and
				// can still make his own next run from where it takes him.
				const s0 = steps[0]!.t0 + 120;
				const best = marker(s0, steps[0]!.t1 - steps[0]!.t0 + 200);
				if (best) {
					const his =
						best.w.next === undefined ? undefined : best.tr.moves[best.w.next];
					const fitsHim = (Q: Pt): boolean => makesAtAll(Q, his);
					let D = startOf(best.tr, best.w);
					let slid = steps[0]!.t0;
					for (const st of steps) {
						if (st.t1 + 320 > best.w.to) {
							break;
						}
						const to = clampPt({
							x: D.x + (st.to.x - st.from.x) * 0.85,
							y: D.y + (st.to.y - st.from.y) * 0.85,
						});
						if (!fitsHim(to)) {
							break;
						}
						const slide: Move = {
							t0: st.t0 + 120,
							t1: st.t1 + 120,
							from: { ...D },
							to,
							anim: "slide",
						};
						added.push({ tr: best.tr, move: slide });
						this.marking.set(slide, tr.pid);
						D = to;
						slid = slide.t1;
					}
					setOff(best.tr, best.w.next, D, slid);
					take(best.tr, best.w, D);
				}
			}
		}
		for (const { tr, move } of added) {
			tr.moves.push(move);
		}
		for (const { tr, act } of addedActs) {
			tr.acts.push(act);
		}
		for (const tr of all) {
			tr.moves.sort((a, b) => a.t0 - b.t0);
			tr.acts.sort((a, b) => a.t0 - b.t0);
		}
	}

	// AT A PLAYER'S PACE. The schedule has each man where he has to be by
	// the moment he has to be there; taken as it stands, he would wait to
	// the last instant and then dash. Players don't: tracking has them
	// standing under a fifth of the time and flat out almost never, walking
	// and jogging the rest. So a run that had him standing first sets off
	// sooner, at an easier pace, and gets there at the same moment - unless
	// he had something to do where he stood: the ball in his hands (unless he
	// was dribbling it, and goes on dribbling), a pass on its way to him, a
	// screen coming for him, a shot.
	private pace() {
		// The pace he would rather go, feet a second, and how much sooner he
		// sets off at most.
		const EASY: Partial<Record<AnimName, number>> = {
			run: 10,
			sprint: 16,
			dribble: 12,
		};
		const SOONER = 2500;
		// When the ball is in each man's hands - and whether he has it on
		// the dribble (he can set off with it then).
		const held = new Map<number, [number, number, boolean][]>();
		this.ball.forEach((s, k) => {
			if (s.kind === "hold") {
				const list = held.get(s.pid) ?? [];
				list.push([s.t0, this.ball[k + 1]?.t0 ?? Infinity, s.style !== "hold"]);
				held.set(s.pid, list);
			}
		});
		// Screens, from a beat before they are set: whoever is to come off
		// one waits for it where he is.
		const screens: { t0: number; t1: number; at: Pt }[] = [];
		for (const tr of this.tracks.values()) {
			for (const a of tr.acts) {
				if (a.anim === "screen" && a.look) {
					screens.push({ t0: a.t0 - 1500, t1: a.t1, at: a.look });
				}
			}
		}
		// Never sooner than a cut, the ball changing hands, or a jump ball
		// being tipped.
		const stops = [
			...this.cuts,
			...this.poss.map(([t]) => t),
			...this.jumps.map(([, tip]) => tip),
		];
		// Nor so slow that he goes by a man set in a screen while it is set
		// (the schedule had him by it when it wasn't).
		const walls = this.bodies();
		const past = (m: Move, t0: number, w: Body2): boolean => {
			if (w.t1 <= t0 || w.t0 >= m.t1) {
				return false;
			}
			const dx = m.to.x - m.from.x;
			const dy = m.to.y - m.from.y;
			const u = Math.min(
				1,
				Math.max(
					0,
					((w.at.x - m.from.x) * dx + (w.at.y - m.from.y) * dy) /
						(dx * dx + dy * dy || 1),
				),
			);
			return dist({ x: m.from.x + dx * u, y: m.from.y + dy * u }, w.at) < 3;
		};
		for (const tr of this.tracks.values()) {
			tr.moves.forEach((m, i) => {
				const easy = EASY[m.anim];
				const d = dist(m.from, m.to);
				if (easy === undefined || d < 4 || this.marking.has(m)) {
					return;
				}
				const need = (d / easy) * 1000;
				if (m.t1 - m.t0 >= need) {
					return;
				}
				let t0 = Math.max(
					m.t1 - need,
					m.t0 - SOONER,
					(tr.moves[i - 1]?.t1 ?? -Infinity) + 80,
				);
				// (One he sets off on as it starts - picking the ball up, say -
				// he is still doing first, too.)
				for (const a of tr.acts) {
					if (a.t1 > t0 && a.t0 <= m.t0) {
						t0 = Math.max(t0, a.t1);
					}
				}
				for (const [h0, h1, dribbling] of held.get(tr.pid) ?? []) {
					if (h1 > t0 && h0 < m.t0 && !(dribbling && m.anim === "dribble")) {
						t0 = Math.max(t0, h1);
					}
				}
				for (const sc of screens) {
					if (sc.t1 > t0 && sc.t0 < m.t0 && dist(sc.at, m.from) < 5) {
						t0 = Math.max(t0, sc.t1);
					}
				}
				for (const c of stops) {
					if (c > t0 && c <= m.t0) {
						t0 = Math.max(t0, c);
					}
				}
				if (
					t0 < m.t0 - 60 &&
					tr.shown.every(([at]) => at <= t0 || at > m.t0) &&
					!walls.some((w) => w.team !== tr.team && past(m, t0, w))
				) {
					m.t0 = t0;
				}
			});
		}
	}

	// NO DASHES. A short run the schedule left only a moment for - him there
	// just in time - would be taken flat out from a standstill and pulled up
	// dead, a blink each way, the way nobody moves. Where he has nothing to
	// do either side of it, he takes it easier: on into the time after it
	// (nothing waiting on him getting there - nothing he does, no run of
	// his, the ball not coming to him), and failing that, setting off sooner
	// (not before the ball moves, a cut, the ball changing hands - what he
	// goes on). And running on into a run he pulls up at the end of, he
	// goes into it no faster than he can pull up in it.
	private ease() {
		// (Past this much harder than he usually goes, he takes it easier,
		// as easy as he can.)
		const HARD = BURST;
		const FLAT_OUT = 26;
		// Kept standing alone: further from his run either side than he runs
		// one into the next (see evaluate).
		const ALONE = 320;
		const MARGIN = 60;
		const edges: number[] = [];
		for (const s of this.ball) {
			edges.push(s.t0);
			if (s.kind !== "hold" && s.kind !== "rest") {
				edges.push(s.t1);
			}
		}
		const stops = [
			...this.cuts,
			...this.poss.map(([t]) => t),
			...this.jumps.map(([, tip]) => tip),
		];
		const his = (pid: number, s: BallSeg) =>
			(s.kind === "hold" && s.pid === pid) ||
			(s.kind === "fly" &&
				(("pid" in s.from && s.from.pid === pid) ||
					("pid" in s.to && s.to.pid === pid)));
		for (const tr of this.tracks.values()) {
			const mine: number[] = [];
			for (const s of this.ball) {
				if (his(tr.pid, s)) {
					mine.push(s.t0);
					if (s.kind === "fly") {
						mine.push(s.t1);
					}
				}
			}
			const shown = tr.shown.map(([at]) => at);
			// (What he has to be where he is for, after a run - and what he
			// goes on, before one. Neither across a cut.)
			const waits = [...mine, ...shown, ...this.cuts];
			const cues = [...edges, ...stops, ...shown];
			const moves = tr.moves;
			for (let i = 0; i < moves.length; i++) {
				const m = moves[i]!;
				const prev = moves[i - 1];
				const next = moves[i + 1];
				const L = dist(m.from, m.to);
				if (
					L < 0.5 ||
					m.t1 - m.t0 < 1 ||
					this.cuts.some((c) => c > m.t0 - 1 && c < m.t1 + 1)
				) {
					continue;
				}
				const aloneIn = m.v0 === 0 || !prev || m.t0 - prev.t1 > ALONE;
				const aloneOut = m.v1 === 0 || !next || next.t0 - m.t1 > ALONE;
				if (!aloneOut) {
					continue;
				}
				// Into one he pulls up at the end of, from the one before.
				let v0 = 0;
				if (!aloneIn) {
					if (m.v0 === undefined || prev?.v1 === undefined) {
						continue;
					}
					const most = pullUpFrom(L, HARD);
					if (m.v0 > most) {
						m.v0 = most;
						prev.v1 = Math.min(prev.v1, most);
					}
					v0 = m.v0;
				}
				const speed = Math.max(FLAT_OUT, v0);
				if (m.t1 - m.t0 >= runMs(L, speed, v0, HARD)) {
					continue;
				}
				const want = runMs(L, speed, v0, 1);
				// On into the time after it.
				let t1 = Math.min(m.t0 + want, (next?.t0 ?? Infinity) - ALONE);
				for (const a of tr.acts) {
					if (a.t1 > m.t1 && a.t0 < t1) {
						t1 = Math.min(t1, a.t0 - MARGIN);
					}
				}
				for (const at of waits) {
					if (at > m.t1 - 1 && at < t1 + MARGIN) {
						t1 = Math.min(t1, at - MARGIN);
					}
				}
				t1 = Math.max(t1, m.t1);
				// Setting off sooner, from a standstill.
				let t0 = m.t0;
				if (aloneIn && t1 - m.t0 < want) {
					t0 = Math.max(t1 - want, (prev?.t1 ?? -Infinity) + ALONE);
					for (const a of tr.acts) {
						if (a.t1 > t0 && a.t0 < m.t0) {
							t0 = Math.max(t0, a.t1 + MARGIN);
						}
					}
					for (const at of cues) {
						if (at > t0 - MARGIN && at <= m.t0) {
							t0 = Math.max(t0, at + MARGIN);
						}
					}
					t0 = Math.min(t0, m.t0);
				}
				m.t0 = t0;
				m.t1 = t1;
			}
		}
	}

	// THE DRIBBLE ALIVE. A man dribbling where he stands - waiting on a
	// screen, for the set to come together - works his man the way a
	// ball handler does, never just lunging at him and backing out again
	// and again: a low, steady dribble most of the time; now and then a
	// run of moves where he stands (a crossover, between his legs, behind
	// his back); walking the ball a few steps along the arc and back,
	// facing his man the whole way, his man sliding with him; and, given
	// the time, one hard attack past his man's shoulder that gets cut off
	// - and a dribble back out. Whatever he does, he is back on his spot,
	// on the beat of his dribble, for whatever he does next.
	// His man, up on him, gives with a run of dribble moves where he stands:
	// a shade the way the ball first goes across, and back as it comes back.
	private giveWith(tr: Track, here: Pt, tc: number, n: number, inHand: Hand) {
		const guard = [...this.tracks.values()]
			.filter((o) => o.team !== tr.team)
			.map((o) => ({ o, d: dist(this.posAt(o.pid, tc), here) }))
			.filter((x) => x.d < 7)
			.sort((a, b) => a.d - b.d)[0]?.o;
		if (!guard) {
			return;
		}
		// Across from his right to his left, or the other way: square to the
		// line from him to his man (or the rim, if they are on top of each
		// other).
		const G = this.posAt(guard.pid, tc);
		const u =
			dist(here, G) > 0.5
				? unitVec(here, G)
				: unitVec(here, { x: rimX(tr.team), y: COURT_H / 2 });
		const v = { x: -u.y, y: u.x };
		const way = inHand === "R" ? -1 : 1;
		(guard.nudges ??= []).push({
			t0: tc + 90,
			t1: tc + n * CROSS_MS + 350,
			dx: v.x * way * 0.9,
			dy: v.y * way * 0.9,
		});
		guard.nudges.sort((a, b) => a.t0 - b.t0);
	}

	private keepDribbling() {
		const ball = this.ball;
		const added: [Track, Move][] = [];
		const segs: BallSeg[] = [];
		for (let i = 0; i < ball.length; i++) {
			const s = ball[i]!;
			if (s.kind !== "hold" || s.style !== "dribble") {
				continue;
			}
			// To when the ball leaves his dribble: a pass, a shot, picked up.
			let j = i + 1;
			while (
				j < ball.length &&
				ball[j]!.kind === "hold" &&
				(ball[j] as { pid: number }).pid === s.pid &&
				(ball[j] as { style: string }).style !== "hold"
			) {
				j++;
			}
			const end = ball[j]?.t0 ?? Infinity;
			const tr = this.track(s.pid);
			const first = i;
			// (The rest of this dribble is taken care of here.)
			i = j - 1;
			if (!tr || !Number.isFinite(end) || end - s.t0 < 1600) {
				continue;
			}
			// Each stretch of it where he stands, with nothing else to do - his
			// dribble moves, done where he stands, among those things.
			const moves: (readonly [number, number])[] = [];
			for (let k = first; k < j; k++) {
				if ((ball[k] as { style: string }).style === "cross") {
					moves.push([ball[k]!.t0, ball[k + 1]?.t0 ?? end]);
				}
			}
			const busy = [
				...tr.moves.map((m) => [m.t0, m.t1] as const),
				...tr.acts.map((a) => [a.t0, a.t1] as const),
				...this.fast.filter(([a, b]) => b > s.t0 && a < end),
				...moves,
				// (And the ball switching hands, as his dribble already has it.)
				...ball
					.slice(first + 1, j)
					.map((x) => [x.t0 - 250, x.t0 + 250] as const),
			].sort((x, y) => x[0] - y[0]);
			let at = s.t0 + 250;
			const gaps: [number, number][] = [];
			for (const [b0, b1] of busy) {
				if (b1 <= at) {
					continue;
				}
				if (b0 >= end - 150) {
					break;
				}
				if (b0 - at >= 1400) {
					gaps.push([at, b0 - 150]);
				}
				at = Math.max(at, b1 + 150);
			}
			if (end - 150 - at >= 1400) {
				gaps.push([at, end - 150]);
			}
			// The ball's own pieces of this dribble, and whether anything of
			// his hands after the stretch says which hand it is in then.
			const own = ball.slice(first, j);
			for (const [g0, g1] of gaps) {
				const P = this.posAt(s.pid, g0);
				const rim = { x: rimX(tr.team), y: COURT_H / 2 };
				const u = unitVec(P, rim);
				// Along the arc, either way.
				const v = { x: -u.y, y: u.x };
				const face = attackDir(tr.team);
				// The beat of his dribble: the top of each bounce, counted from
				// where this run of plain dribbling began.
				const k0 = own.findLastIndex((x) => x.t0 <= g0);
				let k1 = k0;
				while (
					k1 > 0 &&
					(own[k1 - 1] as { style: string }).style === "dribble"
				) {
					k1--;
				}
				const beat0 = own[Math.max(0, k1)]!.t0;
				const onBeat = (t: number) =>
					beat0 + Math.ceil((t - beat0) / DRIBBLE_MS - 1e-6) * DRIBBLE_MS;
				// The hand the ball is in through it.
				const hand: Hand =
					(own[Math.max(0, k0)] as { hand?: Hand }).hand ?? "R";
				// A later piece of this dribble that says its hand: his moves
				// must leave the ball back in that one.
				const keepHand = own.some(
					(x) => x.t0 > g1 && (x as { style: string }).style !== "hold",
				);
				// Walking the ball: the side with the more room first.
				const side: 1 | -1 =
					Math.abs(P.y + v.y * 4 - COURT_H / 2) <
					Math.abs(P.y - v.y * 4 - COURT_H / 2)
						? 1
						: -1;
				let t = g0 + this.rand(250, 600);
				let here: Pt = { ...P };
				let attacked = false;
				let walked = false;
				let combos = 0;
				const step = (to: Pt, ms: number, anim: AnimName) => {
					added.push([
						tr,
						{ t0: t, t1: t + ms, from: { ...here }, to, anim, face },
					]);
					here = to;
					t += ms;
				};
				// Time to get back on his spot from wherever he is.
				const home = () =>
					dist(here, P) < 0.1 ? 0 : Math.max(450, (dist(here, P) / 5) * 1000);
				// Moves where he stands: three of them (two bounces' time), or
				// six - the ball back in the hand it started in, if what comes
				// after says so.
				const combo = (): boolean => {
					const n3 = keepHand ? 6 : this.rng() < 0.7 ? 3 : 6;
					const tc = onBeat(t);
					if (combos >= 2 || tc + n3 * CROSS_MS + home() > g1 - 300) {
						return false;
					}
					// (In the hand his dribble has it in just then: the last
					// piece of it begun, his own moves' included.)
					const lastSeg = [
						...own,
						...segs.filter((x) => (x as { pid?: number }).pid === s.pid),
					]
						.filter((x) => x.t0 <= tc)
						.sort((a, b) => a.t0 - b.t0)
						.at(-1) as { hand?: Hand } | undefined;
					const inHand: Hand = lastSeg?.hand ?? hand;
					combos += 1;
					const made = this.runOfMoves(s.pid, tc, n3, 0.3, 0.2, inHand);
					this.giveWith(tr, here, tc, n3, inHand);
					segs.push(...made.segs, {
						kind: "hold",
						t0: tc + n3 * CROSS_MS,
						pid: s.pid,
						style: "dribble",
						hand: made.hand,
					});
					t = tc + n3 * CROSS_MS;
					wait(this.rand(500, 1000));
					return true;
				};
				// (A pause, never so long he cannot get back on his spot.)
				const wait = (ms: number) => {
					t = Math.max(t, Math.min(t + ms, g1 - home()));
				};
				for (let n = 0; n < 8; n++) {
					const left = g1 - t - home();
					if (left < 700) {
						break;
					}
					const r = this.rng();
					const L = this.rand(3.6, 4.8);
					const go = Math.max(300, (L / 14) * 1000);
					const out = Math.max(560, (L / 7) * 1000);
					if (
						!walked &&
						!attacked &&
						dist(here, P) < 0.1 &&
						go + out + 1000 <= left &&
						left >= 3000 &&
						r < 0.2
					) {
						// One hard go past his man's shoulder - cut off - and a
						// dribble back out, still facing him.
						attacked = true;
						const Q = clampPt({
							x: P.x + (u.x * 0.75 + v.x * side * 0.66) * L,
							y: P.y + (u.y * 0.75 + v.y * side * 0.66) * L,
						});
						step(Q, go, "dribble");
						wait(this.rand(180, 320));
						step({ ...P }, out, "back");
						wait(this.rand(500, 900));
						continue;
					}
					// (Back of half court, he walks it up toward the line instead:
					// never stood there with it.)
					const behind = (P.x - COURT_W / 2) * face < 1;
					const W = clampPt(
						behind
							? { x: P.x + u.x * (L - 1), y: P.y + u.y * (L - 1) }
							: {
									x: P.x + v.x * side * (L - 1),
									y: P.y + v.y * side * (L - 1),
								},
					);
					const walk = Math.max(500, (dist(P, W) / this.rand(4.5, 6)) * 1000);
					const back = Math.max(450, (dist(P, W) / 5) * 1000);
					if (
						!walked &&
						dist(here, P) < 0.1 &&
						walk + back + (behind ? 200 : 900) <= left &&
						(r < 0.55 || behind)
					) {
						// Walking it a few steps along the arc, squared up to his
						// man - setting up the angle, back over in time.
						walked = true;
						step(W, walk, "dribble");
						wait(this.rand(500, 1000));
					} else if (r < 0.8 && combo()) {
						// (Done.)
					} else {
						// Just his dribble, low and steady, a beat or three.
						wait(this.rand(600, 1300));
						if (t >= g1 - home() - 1) {
							break;
						}
					}
				}
				if (dist(here, P) > 0.1) {
					t = Math.max(t, g1 - home());
					step(
						{ ...P },
						Math.max(250, Math.min(home(), g1 + 100 - t)),
						"dribble",
					);
				}
			}
		}
		for (const [tr, m] of added) {
			tr.moves.push(m);
		}
		for (const tr of this.tracks.values()) {
			tr.moves.sort((a, b) => a.t0 - b.t0);
		}
		if (segs.length > 0) {
			ball.push(...segs);
			ball.sort((a, b) => a.t0 - b.t0);
		}
	}

	// THE BALL IN HIS HANDS. Caught with nothing to do with it yet - the set
	// still coming to him, a screen on its way - nobody stands there holding
	// it like a statue: he faces up in his triple threat and jabs at his man
	// or shows him a fake; given longer, he puts it on the floor and keeps his
	// dribble alive (see keepDribbling), and picks it up as it comes up to
	// him, in time for whatever he does next.
	private liveHands() {
		const ball = this.ball;
		const added: BallSeg[] = [];
		const freeThrows = this.beats.filter(
			(bt) => bt.type === "ft" || bt.type === "missFt",
		);
		const teamAt = (t: number): Side => {
			let side: Side = 1;
			for (const [t0, x] of this.poss) {
				if (t0 > t) {
					break;
				}
				side = x;
			}
			return side;
		};
		for (let i = 0; i + 1 < ball.length; i++) {
			const s = ball[i]!;
			const next = ball[i + 1]!;
			if (s.kind !== "hold" || s.style !== "hold") {
				continue;
			}
			const pid = s.pid;
			const tr = this.track(pid);
			if (
				!tr ||
				tr.team !== teamAt(s.t0) ||
				this.poss.some(([t0]) => t0 > s.t0 && t0 <= next.t0) ||
				this.cuts.some((c) => c > s.t0 && c <= next.t0) ||
				freeThrows.some((bt) => bt.end > s.t0 && bt.preStart < next.t0)
			) {
				continue;
			}
			// Out on the floor - not inbounding it.
			const P = this.posAt(pid, s.t0);
			const team = tr.team;
			const dir = attackDir(team);
			if (P.x < 1 || P.x > COURT_W - 1 || P.y < 1 || P.y > COURT_H - 1) {
				continue;
			}
			// The time that is his: after he catches it, before whatever he
			// does next with it - and standing the whole while.
			let from = s.t0 + 200;
			let to = next.t0;
			for (const a of tr.acts) {
				if (a.t1 <= s.t0 || a.t0 >= next.t0) {
					continue;
				}
				if (a.t0 <= s.t0 + 250) {
					from = Math.max(from, a.t1 + 60);
				} else {
					to = Math.min(to, a.t0 - 40);
				}
			}
			if (
				to - from < 700 ||
				tr.moves.some((m) => m.t1 > from - 50 && m.t0 < to - 10)
			) {
				continue;
			}
			const own = next.kind === "hold" && next.pid === pid;
			if ((P.x - COURT_W / 2) * dir < 2) {
				// Back in his own end with it - an outlet, say - he puts it on
				// the floor straight away to bring it up.
				const start = from + 150;
				if (own && next.t0 - start >= DRIBBLE_MS * 2) {
					added.push({
						kind: "hold",
						t0:
							next.t0 - Math.floor((next.t0 - start) / DRIBBLE_MS) * DRIBBLE_MS,
						pid,
						style: "dribble",
						hand: "R",
					});
				}
				continue;
			}
			const rim = { x: rimX(team), y: COURT_H / 2 };
			// What comes next: his own dribble, or the ball out of his hands -
			// a pass or a shot.
			const throws =
				next.kind === "fly" && "pid" in next.from && next.from.pid === pid;
			const fake = (t: number): number => {
				// (In close - a board in his hands under the rim, say - he
				// shows it to go up: a pump fake, not a jab step at his man.)
				const anim =
					this.rng() < 0.55 && dist(P, rim) >= IN_CLOSE_FAKE
						? "jab"
						: "shotFake";
				const dur = anim === "jab" ? 520 : 640;
				this.act(pid, anim, t, t + dur, { look: rim });
				if (anim === "shotFake") {
					this.fakes.push({ pid, t, at: { ...P } });
				}
				return dur;
			};
			if (to - from < 1900 || !(own || throws)) {
				// A moment with it: facing up, one jab or fake.
				this.act(pid, "triple", from, to, { look: rim });
				if (to - from >= 900) {
					fake(from + 120 + this.rng() * (to - from - 900));
				}
				continue;
			}
			// Longer: a beat facing up, then he puts it on the floor - on the
			// beat of the dribble he goes on with, if he goes on dribbling.
			let td = from + 760;
			if (own) {
				td = next.t0 - Math.floor((next.t0 - td) / DRIBBLE_MS) * DRIBBLE_MS;
			}
			let pick = to;
			if (throws) {
				const k = Math.floor((to - 120 - td) / DRIBBLE_MS);
				if (k < 2) {
					this.act(pid, "triple", from, to, { look: rim });
					fake(from + 120);
					continue;
				}
				pick = td + k * DRIBBLE_MS;
			}
			// Often he works his man where he stands: a run of moves
			// - between his legs and back, a crossover, behind his back - ending
			// as he goes on with it or picks it up, the dribble before it on
			// its beat.
			const r = this.rng();
			const n = r < 0.55 ? 0 : r < 0.78 ? 2 : r < 0.91 ? 1 : 3;
			let moves: { tc: number; end: number } | undefined;
			if (n > 0) {
				const span = n * CROSS_MS;
				const first = from + 760;
				if (throws) {
					const k = Math.floor((to - 120 - span - first) / DRIBBLE_MS);
					if (k >= 1) {
						moves = { tc: first + k * DRIBBLE_MS, end: 0 };
						moves.end = moves.tc + span;
						td = first;
						pick = moves.end;
					}
				} else {
					const tc = next.t0 - span;
					const k = Math.floor((tc - first) / DRIBBLE_MS);
					if (k >= 1) {
						moves = { tc, end: next.t0 };
						td = tc - k * DRIBBLE_MS;
					}
				}
			}
			// The hand he ends in is the one he goes on with.
			const then =
				own && next.kind === "hold" && next.style !== "hold"
					? (next.hand ?? "R")
					: "R";
			const start: Hand = moves && n % 2 ? (then === "R" ? "L" : "R") : then;
			this.act(pid, "triple", from, td, { look: rim });
			fake(from + 100);
			added.push({ kind: "hold", t0: td, pid, style: "dribble", hand: start });
			if (moves) {
				const made = this.runOfMoves(pid, moves.tc, n, 0.45, 0.2, start);
				added.push(...made.segs);
				this.giveWith(tr, P, moves.tc, n, start);
				if (throws) {
					added.push({ kind: "hold", t0: moves.end, pid, style: "hold" });
				}
			} else if (throws) {
				added.push({ kind: "hold", t0: pick, pid, style: "hold" });
			}
			if (throws) {
				this.act(pid, "triple", pick, to, { look: rim });
			}
		}
		for (const seg of added) {
			ball.push(seg);
		}
		ball.sort((a, b) => a.t0 - b.t0);
		for (const tr of this.tracks.values()) {
			tr.acts.sort((a, b) => a.t0 - b.t0);
		}
	}

	// THE DEFENSE, MAN TO MAN. Each step of a set sent a defender to where his
	// man would have him be once it was done; here he gets there the way a
	// defender does, following his man the whole way: between him and the rim,
	// up on him with the ball and sagged off him without it, shading toward
	// the ball wherever it goes - in the air on a pass too, so the whole
	// defense shifts with it - a beat behind whatever his man does (a little
	// more off the ball), and no faster than his feet allow. Square to the
	// ball, he slides; to cover real ground he turns and runs; closing out on
	// his man with the ball he chops his feet with a hand up.
	private mark() {
		// He is worked out a tick (ms) at a time.
		const DT = 100;
		// How far behind his man he reacts (ms): quicker up on the ball.
		const REACT_ON = 160;
		const REACT_OFF = 240;
		// How fast he slides and runs (feet a second), how quickly he gets
		// going (feet a second, a second), and how hard he goes after where he
		// should be (a second).
		const SLIDE_MAX = 10.5;
		const RUN_MAX = 23;
		// Flat out, at the very most.
		const FASTEST = SPRINT * 1.1;
		const ACCEL = 24;
		const GAIN = 3;
		// Too short a stretch to follow anybody in (ms).
		const SHORTEST = 250;
		// How far apart two of them keep (feet) - and how near he lets his
		// man come before he gives ground.
		const APART = 3;
		const GIVE = 2.4;
		// (Set further off the man with the ball, out from the rim: it is out
		// in front of him, not to be run into his chest.)
		const CUSHION = 3;
		// The longest one run of his path (ms) and how far it bends (radians)
		// before the next; how fast he must go to set off from standing, and
		// how slow, once going, is standing again (feet a second).
		const RUN_MS = 1000;
		const BEND = 0.5;
		const START = 1.2;
		const KEEP = 0.45;
		// (And back how many ticks, at most, to his first step.)
		const SET_OFF = 5;
		// How many ticks either way his aim is smoothed over.
		const SMOOTH = 2;
		const lastAt = <X>(list: X[], t: number, key: (x: X) => number): number => {
			let lo = 0;
			let hi = list.length - 1;
			let found = -1;
			while (lo <= hi) {
				const mid = (lo + hi) >> 1;
				if (key(list[mid]!) <= t) {
					found = mid;
					lo = mid + 1;
				} else {
					hi = mid - 1;
				}
			}
			return found;
		};
		const whereIn = (tr: Track, t: number): Pt => {
			const k = lastAt(tr.moves, t, (m) => m.t0);
			if (k < 0) {
				return tr.start;
			}
			const m = tr.moves[k]!;
			if (t >= m.t1) {
				return m.to;
			}
			const e = 0.5 - 0.5 * Math.cos((Math.PI * (t - m.t0)) / (m.t1 - m.t0));
			return {
				x: m.from.x + (m.to.x - m.from.x) * e,
				y: m.from.y + (m.to.y - m.from.y) * e,
			};
		};
		const where = (pid: number, t: number): Pt => {
			const tr = this.track(pid);
			return tr ? whereIn(tr, t) : { x: COURT_W / 2, y: COURT_H / 2 };
		};
		const shownAt = (pid: number, t: number): boolean => {
			const sh = this.track(pid)?.shown ?? [];
			const k = lastAt(sh, t, (x) => x[0]);
			return k >= 0 && sh[k]![1];
		};
		const ball = this.ball;
		const endOf = (e: BallEnd, t: number): Pt =>
			"pid" in e ? where(e.pid, t) : { x: e.x, y: e.y };
		const ballAt = (t: number): { at: Pt; holder?: number } => {
			const s = ball[lastAt(ball, t, (x) => x.t0)];
			if (!s) {
				return { at: { x: this.ballAt.x, y: this.ballAt.y } };
			}
			if (s.kind === "hold") {
				return { at: where(s.pid, t), holder: s.pid };
			}
			if (s.kind === "rest") {
				return { at: { x: s.at.x, y: s.at.y } };
			}
			if (s.kind === "path") {
				return { at: playAt(s.pts, t - s.t0) };
			}
			const u = Math.min(1, Math.max(0, (t - s.t0) / Math.max(1, s.t1 - s.t0)));
			const A = s.kind === "fly" ? endOf(s.from, s.t0) : s.from;
			const B = s.kind === "fly" ? endOf(s.to, s.t1) : s.to;
			return { at: { x: A.x + (B.x - A.x) * u, y: A.y + (B.y - A.y) * u } };
		};
		const freeThrows = this.beats.filter(
			(bt) => bt.type === "ft" || bt.type === "missFt",
		);
		// From t, how long the ball stays live: in a man's hands or passed
		// between two - not shot, loose, or at the line.
		const liveUntil = (t: number): number => {
			let end = Infinity;
			for (
				let i = Math.max(
					0,
					lastAt(ball, t, (x) => x.t0),
				);
				i < ball.length;
				i++
			) {
				if (!liveSeg(ball, i)) {
					end = Math.max(t, ball[i]!.t0);
					break;
				}
			}
			for (const bt of freeThrows) {
				if (bt.end > t && bt.preStart < end) {
					end = Math.max(t, bt.preStart);
				}
			}
			return end;
		};
		const firstAfter = (list: number[], t: number) => {
			const k = lastAt(list, t, (x) => x);
			return list[k + 1] ?? Infinity;
		};
		const cuts = [...this.cuts].sort((a, b) => a - b);
		const changes = this.poss.map(([t]) => t).sort((a, b) => a - b);
		// On a break - from when it got going till the ball changes hands -
		// the man on the ball stops it about half court (see BREAK_PICK_UP).
		const breaks = [...this.breaks].sort((a, b) => a - b);
		const pickUpAt = (t: number): number => {
			const b = breaks[lastAt(breaks, t, (x) => x)];
			return b !== undefined && t - b < 12000 && firstAfter(changes, b) > t
				? BREAK_PICK_UP
				: PICK_UP;
		};

		// Bodies he cannot run through (see bodies). Following his man round
		// one, he runs into it, is held up a moment, and fights his way round.
		const walls = this.bodies();
		const bumped = new Set<Track>();

		// Into whatever comes next he arrives where the schedule had him: at
		// the shot he contests, the help spot he steps into.
		const SETTLE = 600;
		// The most a body he is inside puts him back out of it a tick (feet).
		const PUSH_OUT = 0.5;
		// A run of his, laid down at his pace and then told to end slower
		// than that (or set off slower) - turning back on himself, say - goes
		// no slower there than its length and time let him get to, as hard
		// as a man can pull up (or get going): not stopped on a dime at the
		// end of it. (What is left of the turn he takes going round it - see
		// rounded in evaluate.ts.)
		const farthest = (v0: number, v1: number, T: number, up: number) => {
			const down = HARDEST_PULL_UP;
			const k = Math.min(T, Math.max(0, (v1 - v0 + down * T) / (up + down)));
			const top = v0 + up * k;
			return ((v0 + top) / 2) * k + ((top + v1) / 2) * (T - k);
		};
		const slowest = (
			want: number,
			was: number,
			gets: (v: number) => boolean,
		): number => {
			if (want >= was || gets(want)) {
				return want;
			}
			if (!gets(was)) {
				return was;
			}
			let lo = want;
			let hi = was;
			for (let n = 0; n < 12; n++) {
				const mid = (lo + hi) / 2;
				if (gets(mid)) {
					hi = mid;
				} else {
					lo = mid;
				}
			}
			return hi;
		};
		const slowestEnd = (m: Move, want: number): number => {
			const T = (m.t1 - m.t0) / 1000;
			const L = dist(m.from, m.to) - 0.05;
			return slowest(
				want,
				m.v1 ?? 0,
				(v) => farthest(m.v0 ?? 0, v, T, ACCEL * BURST) >= L,
			);
		};
		const slowestStart = (m: Move, want: number): number => {
			const T = (m.t1 - m.t0) / 1000;
			const L = dist(m.from, m.to) - 0.05;
			// (Run backwards, getting going is pulling up.)
			return slowest(
				want,
				m.v0 ?? 0,
				(v) => farthest(m.v1 ?? 0, v, T, HARDEST_PULL_UP) >= L,
			);
		};
		for (const tr of this.tracks.values()) {
			const moves = tr.moves;
			if (!moves.some((m) => this.marking.has(m))) {
				continue;
			}
			// How much slower than the quickest of them he is to read what his
			// man and the ball do (ms): each man his own.
			const quick = 120 * hash01(tr.pid, 9241);
			// His teammates - read off in time order as he is worked out.
			const mates = [...this.tracks.values()]
				.filter((o) => o !== tr && o.team === tr.team)
				.map((o) => {
					let mi = -1;
					let si = -1;
					return {
						tr: o,
						shown: (t: number) => {
							while (si + 1 < o.shown.length && o.shown[si + 1]![0] <= t) {
								si++;
							}
							return si;
						},
						at: (t: number): Pt => {
							while (mi + 1 < o.moves.length && o.moves[mi + 1]!.t0 <= t) {
								mi++;
							}
							if (mi < 0) {
								return o.start;
							}
							const m = o.moves[mi]!;
							if (t >= m.t1) {
								return m.to;
							}
							const e =
								0.5 - 0.5 * Math.cos((Math.PI * (t - m.t0)) / (m.t1 - m.t0));
							return {
								x: m.from.x + (m.to.x - m.from.x) * e,
								y: m.from.y + (m.to.y - m.from.y) * e,
							};
						},
					};
				});
			const planned = { ...tr, moves: [...moves] };
			const acts = tr.acts.map((a) => a.t0);
			const out: Move[] = [];
			// Where the rewritten track has him at t.
			const at = (t: number): Pt => {
				const m = out.findLast((x) => x.t0 <= t);
				if (!m) {
					return tr.start;
				}
				if (t >= m.t1) {
					return m.to;
				}
				const e = (t - m.t0) / (m.t1 - m.t0);
				return {
					x: m.from.x + (m.to.x - m.from.x) * e,
					y: m.from.y + (m.to.y - m.from.y) * e,
				};
			};
			// Where he is, when a run of his was rewritten: the next one he
			// makes starts from there - and gets no faster than his legs for
			// it: if following his man left him farther off than the schedule
			// had him, he goes as far as he can get (and the run after that
			// sets off from there).
			let left: Pt | undefined;
			// How far he gets in a run of his set off from there: from
			// standing - unless it follows straight on from a run of his he is
			// still going at the end of, or takes him to the spot he does
			// something on (the shot he contests from the side), however hard
			// he has to go to get there.
			const reach = (m: Move): number => {
				const prev = out.at(-1);
				const onto = tr.acts.some(
					(a) => a.t0 <= m.t1 + 300 && a.t1 >= m.t1 - 50,
				);
				return onto ||
					(prev &&
						(prev.v1 === undefined || prev.v1 > 0) &&
						m.t0 - prev.t1 <= 300)
					? (FASTEST * (m.t1 - m.t0)) / 1000
					: reachIn(m.t1 - m.t0, FASTEST, BURST);
			};
			// A run set off from farther back than it was planned from, no
			// faster than his legs: all the way there, taking the time it
			// takes, if nothing of his comes next before he could be - or as
			// far as he gets (and false).
			const getsThere = (m: Move, next: Move | undefined): boolean => {
				const far = dist(m.from, m.to);
				const most = reach(m);
				if (far <= most || far <= 0.01) {
					return true;
				}
				const t1 = m.t0 + runMs(far, FASTEST, 0, BURST);
				if (
					t1 <= (next?.t0 ?? Infinity) &&
					!acts.some((a) => a > m.t1 && a <= t1)
				) {
					m.t1 = t1;
					return true;
				}
				const k = most / far;
				m.to = {
					x: m.from.x + (m.to.x - m.from.x) * k,
					y: m.from.y + (m.to.y - m.from.y) * k,
				};
				return false;
			};
			// Following his man on from one stretch straight into the next, how
			// fast he was going at the end of the last, and when that was.
			let carry: { vx: number; vy: number; t: number } | undefined;
			let i = 0;
			while (i < moves.length) {
				const m = moves[i]!;
				const man = this.marking.get(m);
				if (man === undefined) {
					let short = false;
					// (Across a cut he is wherever it has him.)
					const atCut = cuts.some((c) => Math.abs(c - m.t0) < 2);
					if (atCut) {
						left = undefined;
					}
					if (left) {
						m.from = { ...left };
						short = !getsThere(m, moves[i + 1]);
					}
					left = short ? { ...m.to } : undefined;
					carry = undefined;
					out.push(m);
					i++;
					continue;
				}
				const a = m.t0;
				let hard = Infinity;
				// (A long stretch of it is worked out a piece at a time, each
				// from the start of the run of his that begins the next - so
				// one picks up just where the last leaves off.)
				let cap = Infinity;
				for (let j = i + 1; j < moves.length; j++) {
					if (!this.marking.has(moves[j]!)) {
						hard = moves[j]!.t0;
						break;
					}
					if (cap === Infinity && moves[j]!.t0 >= a + 15000) {
						cap = moves[j]!.t0;
					}
				}
				const b = Math.min(
					hard,
					firstAfter(acts, a - 1),
					firstAfter(cuts, a),
					firstAfter(changes, a),
					liveUntil(a),
					cap,
				);
				// The men he shadows through it, from when.
				const men: [number, number][] = [];
				let j = i;
				for (; j < moves.length && moves[j]!.t0 < b; j++) {
					const mm = this.marking.get(moves[j]!);
					if (mm === undefined) {
						break;
					}
					men.push([moves[j]!.t0, mm]);
				}
				if (
					b - a < SHORTEST ||
					!shownAt(tr.pid, a) ||
					men.some(([, x]) => this.teamOf(x) === tr.team)
				) {
					// Too short to follow anybody in, or not his to follow: as
					// scheduled - from wherever following left him, and no faster
					// than his legs for that either.
					let short = false;
					if (left) {
						m.from = { ...left };
						short = !getsThere(m, moves[i + 1]);
					}
					left = short ? { ...m.to } : undefined;
					carry = undefined;
					out.push(m);
					i++;
					continue;
				}
				// Where the schedule had him getting to: if it had him still on
				// his way when the next thing starts (into a contest, say), all
				// the way there.
				let stop = b;
				const going = planned.moves[lastAt(planned.moves, b, (x) => x.t0)];
				if (going && this.marking.has(going) && going.t1 > b) {
					stop = Math.min(going.t1, hard, moves[j]?.t0 ?? Infinity);
				}
				// (Not if this is only as far as one stretch of following goes,
				// with more of it to come: he picks that up from wherever he is.)
				const end =
					b === cap && moves[j] && this.marking.has(moves[j]!)
						? undefined
						: whereIn(planned, stop);
				// Straight on into his next run, if it follows on from this at
				// once (a closeout, crashing the glass): he goes into it at
				// speed - as much as the turn into it lets him keep, and no
				// faster than it goes - rather than pulling up for it first.
				const nextUp = moves[j];
				const into =
					end &&
					nextUp &&
					!this.marking.has(nextUp) &&
					Math.abs(nextUp.t0 - stop) <= 1 &&
					!cuts.some((c) => Math.abs(c - nextUp.t0) < 2) &&
					!acts.some((x) => Math.abs(x - stop) <= 1) &&
					dist(end, nextUp.to) > 1
						? nextUp
						: undefined;
				const intoDir = into && end ? unitVec(end, into.to) : undefined;
				const intoPace =
					into && end
						? dist(end, into.to) / Math.max(0.2, (into.t1 - into.t0) / 1000)
						: 0;
				// (Carrying on from the last stretch of following: from where it
				// ended - no pause between the two.)
				const onT = carry && a - carry.t <= DT ? carry.t : undefined;
				// Where he should be, tick by tick: where his man and the ball were
				// a beat ago say.
				const ticks: { t: number; aim: Pt; ball: Pt; on: boolean; man: Pt }[] =
					[];
				let k = 0;
				let last: Pt | undefined;
				for (let t = a; t <= stop + 0.5; t += DT) {
					while (k + 1 < men.length && men[k + 1]![0] <= t) {
						k++;
					}
					// (Subbed out, his man is gone: he stands his ground while the
					// man coming on gets out there, then picks him up.)
					let who = men[k]![1];
					let waiting = false;
					for (let n = 0; n < 4; n++) {
						const r = this.replaced
							.get(who)
							?.find((x) => x.from <= t - REACT_OFF && t - REACT_OFF < x.until);
						if (!r) {
							break;
						}
						if (t - REACT_OFF < r.on) {
							waiting = true;
						}
						who = r.by;
					}
					const B0 = ballAt(t);
					const on = B0.holder === who;
					const R = (on ? REACT_ON : REACT_OFF) + quick;
					const { at: Bp, holder } = ballAt(t - R);
					// Up on the ball he reads where his man is headed, not only
					// where he was a beat ago: he gives ground as the drive comes,
					// rather than letting it run up his back.
					let seen = where(who, t - R);
					if (holder === who) {
						const was = where(who, t - R - DT);
						const lead = Math.min(
							1,
							2.5 / Math.max(0.01, dist(was, seen) * (R / DT)),
						);
						seen = {
							x: seen.x + (seen.x - was.x) * (R / DT) * lead,
							y: seen.y + (seen.y - was.y) * (R / DT) * lead,
						};
					}
					let aim =
						(shownAt(who, t - R) && !waiting) || !last
							? this.defensePoint(
									this.teamOf(who),
									seen,
									Bp,
									holder === who,
									pickUpAt(t),
								)
							: last;
					// Up on the man with it, out from the rim, never nearer him
					// than the ball out in front of him leaves room for - unless
					// that would back him into a screen set behind him.
					if (aim !== last && holder === who) {
						const rim = { x: rimX(this.teamOf(who)), y: COURT_H / 2 };
						const d = dist(seen, aim);
						if (d > 0.1 && d < CUSHION && dist(seen, rim) > 10) {
							const C = clampPt({
								x: seen.x + ((aim.x - seen.x) * CUSHION) / d,
								y: seen.y + ((aim.y - seen.y) * CUSHION) / d,
							});
							if (
								!walls.some(
									(w) =>
										w.team !== tr.team &&
										w.t0 <= t + 600 &&
										w.t1 >= t &&
										dist(w.at, C) < 4,
								)
							) {
								aim = C;
							}
						}
					}
					last = aim;
					ticks.push({ t, aim, ball: B0.at, on, man: where(who, t) });
				}
				// Never where a teammate already is: two defenders don't share a
				// spot - helping off the same way, the one keeps a step off the
				// other.
				const apart = (Q: Pt, t: number): Pt => {
					let x = Q.x;
					let y = Q.y;
					for (const m of mates) {
						const o = m.tr;
						const sk = m.shown(t);
						if (sk < 0 || !o.shown[sk]![1]) {
							continue;
						}
						const M = m.at(t);
						const ox = x - M.x;
						const oy = y - M.y;
						const d = Math.hypot(ox, oy);
						if (d >= APART) {
							continue;
						}
						// (Right on him: off to the side, the same way every time.)
						const s = tr.pid > o.pid ? 1 : -1;
						const nx = d > 0.05 ? ox / d : s;
						const ny = d > 0.05 ? oy / d : 0;
						x = M.x + nx * APART;
						y = M.y + ny * APART;
					}
					return clampPt({ x, y });
				};
				// Following it, as fast and as quick as a defender's feet - and
				// on to his mark in time for what comes next: setting off for it
				// sooner, the farther it is.
				// The bodies in his way through it - and, following, where he ran
				// into one.
				const inWay = walls.filter(
					(w) => w.team !== tr.team && w.t1 > a && w.t0 < stop,
				);
				let hits: { t: number; at: Pt }[] = [];
				const spaced = ticks.map((x) => apart(x.aim, x.t));
				const follow = (settle: number) => {
					hits = [];
					const aims = ticks.map((x, i) => {
						if (!end || x.t <= stop - settle - 200) {
							return spaced[i]!;
						}
						const u = Math.min(1, (x.t - (stop - settle - 200)) / settle);
						const w = u * u * (3 - 2 * u);
						return {
							x: x.aim.x + (end.x - x.aim.x) * w,
							y: x.aim.y + (end.y - x.aim.y) * w,
						};
					});
					// Smoothed: a man's feet don't follow every flicker of the ball.
					const smooth = aims.map((_, i) => {
						let sx = 0;
						let sy = 0;
						let sw = 0;
						for (let d = -SMOOTH; d <= SMOOTH; d++) {
							const y = aims[i + d];
							if (y) {
								const w = Math.exp(-((d / (SMOOTH / 2)) ** 2) / 2);
								sx += y.x * w;
								sy += y.y * w;
								sw += w;
							}
						}
						return { x: sx / sw, y: sy / sw };
					});
					let p = left ?? at(a);
					// (Straight on from the last stretch of it: as fast as he was
					// going.)
					const on = carry && a - carry.t <= DT ? carry : undefined;
					let vx = on?.vx ?? 0;
					let vy = on?.vy ?? 0;
					let running = Math.hypot(vx, vy) > SLIDE_MAX;
					const out: {
						t: number;
						p: Pt;
						v: number;
						vx: number;
						vy: number;
						ball: Pt;
						on: boolean;
						man: Pt;
					}[] = [{ ...ticks[0]!, p, v: Math.hypot(vx, vy), vx, vy }];
					for (let i = 1; i < ticks.length; i++) {
						const T = smooth[i]!;
						const prev = smooth[i - 1]!;
						// His man's pace, to keep up with.
						const fx = (T.x - prev.x) / (DT / 1000);
						const fy = (T.y - prev.y) / (DT / 1000);
						const ex = T.x - p.x;
						const ey = T.y - p.y;
						const e = Math.hypot(ex, ey);
						let wx = fx + ex * GAIN;
						let wy = fy + ey * GAIN;
						// Up against his man he gives ground, as quick as his feet
						// let him - never into him (or not for long: see keepOff).
						{
							const M = ticks[i]!.man;
							const mx = p.x - M.x;
							const my = p.y - M.y;
							const md = Math.hypot(mx, my);
							if (md < GIVE) {
								const k = ((GIVE - md) * GAIN * 2) / Math.max(md, 0.3);
								wx += mx * k;
								wy += my * k;
							}
						}
						// Turning and running to cover ground, until he has caught up.
						running = running ? e > 2.5 : e > 7;
						const vmax = running ? RUN_MAX : SLIDE_MAX;
						const w = Math.hypot(wx, wy);
						// Into what he does next he arrives standing (it sets off
						// from there): no faster, as it nears, than he can pull up
						// from in the time left - not stopped dead at the end.
						// (Into a run straight on from it: down only to the pace he
						// goes on into it at.)
						const cur = Math.hypot(vx, vy);
						const flow = intoDir
							? Math.min(intoPace * 1.2, RUN_MAX) *
								keepThrough(
									cur > 0.5 ? (vx * intoDir.x + vy * intoDir.y) / cur : 1,
								)
							: 0;
						const most = end
							? flow + ACCEL * 0.85 * Math.max(0, (stop - ticks[i]!.t) / 1000)
							: Infinity;
						if (w > Math.min(vmax, most)) {
							wx *= Math.min(vmax, most) / w;
							wy *= Math.min(vmax, most) / w;
						}
						let dx = wx - vx;
						let dy = wy - vy;
						const dv = Math.hypot(dx, dy);
						// (Getting quicker, the less so the quicker he is already.)
						const sp = Math.hypot(vx, vy);
						const cap =
							((Math.hypot(wx, wy) > sp
								? Math.min(ACCEL, pushFrom(sp))
								: ACCEL) *
								DT) /
							1000;
						if (dv > cap) {
							dx *= cap / dv;
							dy *= cap / dv;
						}
						vx += dx;
						vy += dy;
						// (The lines are a wall to him: going after a man along
						// the baseline or down in the corner, he pulls up at
						// them, not carried on out of bounds.)
						const q = {
							x: p.x + (vx * DT) / 1000,
							y: p.y + (vy * DT) / 1000,
						};
						// (Out there already, he goes no further out.)
						const F = inPlay(q);
						if (q.x !== F.x && (q.x - F.x) * vx > 0) {
							q.x = (p.x - F.x) * (q.x - F.x) > 0 ? p.x : F.x;
							vx = 0;
						}
						if (q.y !== F.y && (q.y - F.y) * vy > 0) {
							q.y = (p.y - F.y) * (q.y - F.y) > 0 ? p.y : F.y;
							vy = 0;
						}
						p = clampPt(q);
						const now = ticks[i]!.t;
						for (const w of inWay) {
							if (now < w.t0 || now > w.t1) {
								continue;
							}
							const ox = p.x - w.at.x;
							const oy = p.y - w.at.y;
							const d = Math.hypot(ox, oy);
							if (d >= BODY) {
								continue;
							}
							// Out to the edge of him, the way he was going round - a
							// step at a time if he finds himself well inside him (a
							// screen set on top of him), not jumped there.
							const sp = Math.hypot(vx, vy) || 1;
							const nx = d > 0.05 ? ox / d : -vy / sp;
							const ny = d > 0.05 ? oy / d : vx / sp;
							const by = Math.min(BODY - d, PUSH_OUT);
							p = clampPt({ x: p.x + nx * by, y: p.y + ny * by });
							const into = vx * nx + vy * ny;
							if (into < 0) {
								vx -= into * nx;
								vy -= into * ny;
							}
							// Run into, it stops him in his tracks a moment - the
							// harder, the more.
							if (into < -1.2 && !hits.some((h) => h.at === w.at)) {
								hits.push({ t: now, at: w.at });
								const k = Math.max(0.3, 1 - -into / 10);
								vx *= k;
								vy *= k;
							}
						}
						out.push({ ...ticks[i]!, p, v: Math.hypot(vx, vy), vx, vy });
					}
					return out;
				};
				let settle = SETTLE;
				let path = follow(settle);
				while (
					end &&
					dist(path.at(-1)!.p, end) > 2.5 &&
					settle < stop - a - 200
				) {
					settle = Math.min(stop - a - 200, settle * 2);
					path = follow(settle);
				}
				// On a man sealing him in the post: leaning into him, an arm on
				// his back, fighting him for the spot.
				for (const w of inWay) {
					if (!w.post) {
						continue;
					}
					let from: number | undefined;
					const close = (q: number) => {
						const x = path[q]!;
						const v =
							q + 1 < path.length ? dist(x.p, path[q + 1]!.p) / (DT / 1000) : 0;
						return (
							x.t >= w.t0 &&
							x.t <= w.t1 &&
							dist(x.p, w.at) < BODY * 1.4 &&
							v < 5
						);
					};
					for (let q = 0; q <= path.length; q++) {
						const on = q < path.length && close(q);
						if (on && from === undefined) {
							from = path[q]!.t;
						} else if (!on && from !== undefined) {
							const to = path[q - 1]!.t;
							if (
								to - from >= 450 &&
								!tr.acts.some((x) => x.t1 > from! && x.t0 < to)
							) {
								tr.acts.push({
									t0: from,
									t1: to,
									anim: "fight",
									look: { ...w.at },
								});
								bumped.add(tr);
							}
							from = undefined;
						}
					}
				}
				// Each screen he ran into: the jolt of it.
				for (const h of hits) {
					const t0 = h.t - 60;
					const t1 = h.t + 360;
					if (!tr.acts.some((x) => x.t1 > t0 && x.t0 < t1)) {
						tr.acts.push({ t0, t1, anim: "bump", look: { ...h.at } });
						bumped.add(tr);
					}
				}
				// The last of the way onto his mark exactly, if he is all but
				// there: worked into his last few steps, not taken in one stride
				// at the end (and never, having run on past it, a stride back).
				const fin = path.at(-1)!;
				if (end && dist(fin.p, end) < 3) {
					const ox = end.x - fin.p.x;
					const oy = end.y - fin.p.y;
					const span = Math.min(800, fin.t - path[0]!.t);
					for (const x of path) {
						const u = span > 0 ? (x.t - (fin.t - span)) / span : 1;
						if (u > 0) {
							const w = u >= 1 ? 1 : u * u * (3 - 2 * u);
							x.p = { x: x.p.x + ox * w, y: x.p.y + oy * w };
						}
					}
				}
				// Into runs: each one heading one way, getting quicker or slower
				// but not both - so a whole burst, or the whole of pulling up, is
				// one run - and none where he barely moves. Each starts and ends
				// at the pace his feet had him going there.
				const pace = (q: number) =>
					dist(path[q]!.p, path[q + 1]!.p) / (DT / 1000);
				// (At either end, as fast as he goes on from the stretch before
				// into the next - if he does.)
				const paceAt = (q: number) =>
					q <= 0
						? path[0]!.v
						: q >= path.length - 1
							? end && !into
								? 0
								: path.at(-1)!.v
							: (pace(q - 1) + pace(q)) / 2;
				const heading = (q: number) =>
					Math.atan2(
						path[q + 1]!.p.y - path[q]!.p.y,
						path[q + 1]!.p.x - path[q]!.p.x,
					);
				let q = 0;
				// (Where the last run ended.)
				let ended = 0;
				let moving = path[0]!.v >= KEEP;
				// Where the last run left him: the creep of his feet standing
				// there is no run, so the next one sets off from there.
				let was: Pt = path[0]!.p;
				const first = out.length;
				while (q + 1 < path.length) {
					// Off from standing at a step's pace; on the move, he keeps
					// going down to a crawl.
					if (pace(q) < (moving ? KEEP : START)) {
						moving = false;
						q++;
						continue;
					}
					const setOff = !moving;
					moving = true;
					// Off from standing, from his first step - not from once he
					// is already going a step's pace, the steps that got him going
					// crammed into the rest of it.
					let q0 = q;
					while (
						setOff &&
						q0 > ended &&
						q - q0 < SET_OFF &&
						pace(q0 - 1) > 0.15
					) {
						q0--;
					}
					const h0 = heading(q);
					let e = q + 1;
					let trend = 0;
					while (e + 1 < path.length && (e - q) * DT < RUN_MS) {
						const v = pace(e);
						const turn = Math.abs(
							Math.atan2(Math.sin(heading(e) - h0), Math.cos(heading(e) - h0)),
						);
						if (v < KEEP || turn > BEND) {
							break;
						}
						const dv = v - pace(e - 1);
						const way = Math.abs(dv) < 0.4 ? 0 : Math.sign(dv);
						if (trend !== 0 && way !== 0 && way !== trend) {
							break;
						}
						if (way !== 0) {
							trend = way;
						}
						e++;
					}
					const A = {
						...path[q0]!,
						t:
							q0 === 0 && onT !== undefined
								? Math.min(onT, path[0]!.t)
								: path[q0]!.t,
						p: dist(was, path[q0]!.p) < 1.5 ? was : path[q0]!.p,
					};
					const Z = path[e]!;
					const stops =
						e + 1 >= path.length
							? (!!end && !into) || path.at(-1)!.v < KEEP
							: pace(e) < KEEP;
					const v0 = setOff ? 0 : paceAt(q);
					const v1 = stops ? 0 : paceAt(e);
					q = e;
					ended = e;
					const d = dist(A.p, Z.p);
					if (d < 0.05) {
						continue;
					}
					const secs = (Z.t - A.t) / 1000;
					const speed = d / secs;
					const ux = (Z.p.x - A.p.x) / d;
					const uy = (Z.p.y - A.p.y) / d;
					const tb = unitVec(A.p, A.ball);
					const toBall = ux * tb.x + uy * tb.y;
					// Closing out: at his man with the ball, who is set - not
					// chasing him on a drive.
					const set = dist(A.man, Z.man) / secs < 6;
					// Beaten: his man going away from him the way he goes, a
					// step and more ahead of him - he turns and runs with him,
					// not slides along behind.
					const gone = dist(A.man, Z.man);
					const chasing =
						gone / secs > 8 &&
						((Z.man.x - A.man.x) * ux + (Z.man.y - A.man.y) * uy) / gone >
							0.7 &&
						(A.man.x - A.p.x) * ux + (A.man.y - A.p.y) * uy > 2.5;
					const anim: AnimName =
						speed >= SLIDE_MAX - 0.5 || chasing
							? "run"
							: Z.on &&
								  set &&
								  toBall > 0.5 &&
								  speed > 4 &&
								  dist(A.p, A.man) > 4.5
								? "closeout"
								: toBall < -0.55
									? "slide"
									: "shuffle";
					out.push({
						t0: A.t,
						t1: Z.t,
						from: A.p,
						to: Z.p,
						anim,
						// (No faster at either end than covering it in the time
						// allows.)
						v0: Math.min(v0, 2 * speed),
						v1: Math.min(v1, 2 * speed),
					});
					was = Z.p;
				}
				// His feet don't change step for a single tick between two of
				// the same: a push step, a drop step, a push step is all push.
				for (let k = first + 1; k + 1 < out.length; k++) {
					const a = out[k - 1]!;
					const b = out[k]!;
					const c = out[k + 1]!;
					if (
						a.anim === c.anim &&
						b.anim !== a.anim &&
						b.t1 - b.t0 <= 150 &&
						b.t0 - a.t1 < 1 &&
						c.t0 - b.t1 < 1
					) {
						b.anim = a.anim;
					}
				}
				// Turning back on himself from one run to the next, he gets
				// there pulling up, not at full tilt.
				for (let k = first; k + 1 < out.length; k++) {
					const x = out[k]!;
					const y = out[k + 1]!;
					if (y.t0 - x.t1 > 1 || !x.v1 || !y.v0) {
						continue;
					}
					const lx = dist(x.from, x.to);
					const ly = dist(y.from, y.to);
					const cos =
						lx > 0.05 && ly > 0.05
							? ((x.to.x - x.from.x) * (y.to.x - y.from.x) +
									(x.to.y - x.from.y) * (y.to.y - y.from.y)) /
								(lx * ly)
							: 1;
					if (cos < 0) {
						const k0 = keepThrough(cos) / keepThrough(0);
						x.v1 = slowestEnd(x, x.v1 * k0);
						y.v0 = slowestStart(y, y.v0 * k0);
					}
				}
				// Following his man left him short of where the schedule has him
				// next: on to it, as fast as he can - there before anything
				// that counts on him being there (taking the ball out, say), if
				// he can be.
				left = fin.p;
				carry = end ? undefined : { vx: fin.vx, vy: fin.vy, t: fin.t };
				// On into his next run at the pace he got there at, straight on
				// from the end of this - or, if that can't be, pulled up.
				const lastRun = out.at(-1);
				if (into && intoDir && lastRun?.v1) {
					if (Math.abs(lastRun.t1 - fin.t) < 1) {
						const lx = dist(lastRun.from, lastRun.to);
						const cos =
							lx > 0.05
								? ((lastRun.to.x - lastRun.from.x) * intoDir.x +
										(lastRun.to.y - lastRun.from.y) * intoDir.y) /
									lx
								: 1;
						const v = Math.min(lastRun.v1 * keepThrough(cos), intoPace * 1.5);
						lastRun.v1 = slowestEnd(lastRun, v);
						into.v0 = v;
						into.t0 = fin.t;
					} else {
						lastRun.v1 = 0;
					}
				}
				const gone = end ? dist(fin.p, end) : 0;
				if (end && gone > 0.5) {
					const need = Math.max(240, runMs(gone, RUN_MAX));
					const t1 = Math.min(fin.t + need, moves[j]?.t0 ?? Infinity);
					if (t1 - fin.t >= 100) {
						// (As far as he gets in the time there is, from standing.)
						const k =
							t1 - fin.t >= need
								? 1
								: Math.min(1, reachIn(t1 - fin.t, RUN_MAX) / gone);
						const to = {
							x: fin.p.x + (end.x - fin.p.x) * k,
							y: fin.p.y + (end.y - fin.p.y) * k,
						};
						out.push({ t0: fin.t, t1, from: fin.p, to, anim: "run" });
						left = k >= 1 ? undefined : to;
					}
				}
				i = j;
			}
			tr.moves = out;
		}
		for (const tr of bumped) {
			tr.acts.sort((x, y) => x.t0 - y.t0);
		}
	}

	// Bodies nobody runs through: a man set in a screen, or sealing in the
	// post, where he stands while he does it.
	private bodies(): Body2[] {
		const out: Body2[] = [];
		for (const o of this.tracks.values()) {
			for (const act of o.acts) {
				if (act.anim === "screen" || act.anim === "postUp") {
					const m = o.moves.findLast((x) => x.t0 <= act.t0);
					out.push({
						team: o.team,
						t0: act.t0,
						t1: act.t1,
						at: m && act.t0 >= m.t1 ? { ...m.to } : this.posAt(o.pid, act.t0),
						post: act.anim === "postUp",
					});
				}
			}
		}
		return out;
	}

	// And a run of his that would take a man straight through one goes round
	// him instead: bent out past his shoulder where it came closest.
	private aroundBodies() {
		// Out of one he has ended up inside, no quicker than this (feet a
		// second).
		const STEP_OUT = 10;
		const walls = this.bodies();
		const tracks = [...this.tracks.values()];
		for (const tr of tracks) {
			const theirs = walls.filter((w) => w.team !== tr.team);
			// And with the ball, a man of theirs just standing there in his way:
			// round him too, not through him.
			for (const m of tr.moves) {
				if (m.anim !== "dribble") {
					continue;
				}
				for (const o of tracks) {
					if (o.team === tr.team) {
						continue;
					}
					// (From when he stops there, if he does before it is over.)
					const k = lastBy(o.moves, m.t1, (x) => x.t0);
					const from = Math.max(m.t0, k >= 0 ? o.moves[k]!.t1 : -Infinity);
					const si = lastBy(o.shown, from, (x) => x[0]);
					if (from >= m.t1 - 100 || si < 0 || !o.shown[si]![1]) {
						continue;
					}
					theirs.push({
						team: o.team,
						t0: from,
						t1: m.t1,
						at: k >= 0 ? { ...o.moves[k]!.to } : { ...o.start },
						post: false,
					});
				}
			}
			if (theirs.length === 0) {
				continue;
			}
			// Round each one in his way in turn - the first he comes to, then on
			// from there.
			const split = (
				m: Move,
				depth: number,
				before: Move | undefined,
				after: Move | undefined,
			): Move[] => {
				const dx = m.to.x - m.from.x;
				const dy = m.to.y - m.from.y;
				const L2 = dx * dx + dy * dy;
				let bend: { t: number; at: Pt; u: number } | undefined;
				const dur = m.t1 - m.t0;
				for (const w of L2 > 0.25 ? theirs : []) {
					if (w.t1 <= m.t0 || w.t0 >= m.t1) {
						continue;
					}
					// Where he comes closest to him while the man is there - how
					// far along it he is by then reckoned both at an even pace
					// and getting going and pulling up, as he really runs it.
					const lo = dur > 0 ? Math.max(0, (w.t0 - m.t0) / dur) : 0;
					const hi = dur > 0 ? Math.min(1, (w.t1 - m.t0) / dur) : 1;
					const u = Math.min(
						Math.max(hi, runProgress(dur, hi)),
						Math.max(
							Math.min(lo, runProgress(dur, lo)),
							((w.at.x - m.from.x) * dx + (w.at.y - m.from.y) * dy) / L2,
						),
					);
					const q = { x: m.from.x + dx * u, y: m.from.y + dy * u };
					const tq = m.t0 + dur * u;
					const d = dist(q, w.at);
					if (d >= BODY || (bend && bend.u <= u)) {
						continue;
					}
					const L = Math.sqrt(L2);
					const n =
						d > 0.05
							? { x: (q.x - w.at.x) / d, y: (q.y - w.at.y) / d }
							: { x: -dy / L, y: dx / L };
					bend = {
						t: tq,
						u,
						at: clampPt({
							x: w.at.x + n.x * BODY * 1.05,
							y: w.at.y + n.y * BODY * 1.05,
						}),
					};
				}
				const edge = Math.min(60, dur * 0.3);
				if (!bend) {
					return [m];
				}
				// (At an even pace round him: each leg of it takes the time its
				// length does - out at his shoulder, the bend can sit well along
				// the way from where he passes closest.)
				const la = dist(m.from, bend.at);
				const lb = dist(bend.at, m.to);
				// (Not round him at all, a step too short to: the long way
				// round would have it a dash. He is only kept off him.)
				const L = Math.sqrt(L2);
				if (la + lb > L * 1.2 && ((la + lb) * 1000) / Math.max(1, dur) > 12) {
					return [m];
				}
				// (Each leg the time it takes him there as he really runs it:
				// the last of it, pulling up, slower than the rest.)
				if (la + lb > 0.05) {
					const f = la / (la + lb);
					const setsOff =
						(m.v0 ?? 0) === 0 &&
						(m.v0 === 0 || !before || m.t0 - before.t1 > FLOW_MS);
					const pullsUp =
						(m.v1 ?? 0) === 0 &&
						(m.v1 === 0 || !after || after.t0 - m.t1 > FLOW_MS);
					let lo = 0;
					let hi = 1;
					for (let k = 0; k < 24; k++) {
						const mid = (lo + hi) / 2;
						if (runProgress(dur, mid, setsOff, pullsUp) < f) {
							lo = mid;
						} else {
							hi = mid;
						}
					}
					bend.t = m.t0 + dur * ((lo + hi) / 2);
				}
				if (bend.t - m.t0 <= edge || m.t1 - bend.t <= edge) {
					return [m];
				}
				// (Going on round him, not pulling up for him: what pace he had
				// coming in and going out stays at the ends of the whole run, and
				// through the bend he goes as fast as the turn in it lets him.)
				const a = { ...m, t1: bend.t, to: bend.at, v1: undefined };
				const b = { ...m, t0: bend.t, from: bend.at, v0: undefined };
				return depth >= 3 ? [a, b] : [a, ...split(b, depth + 1, a, after)];
			};
			// No run ends inside one - nor does a man stand where one is set:
			// he stops at his shoulder (and the next run sets off from there).
			// Then, those runs as they now go, round any in the way. (Not one
			// that takes him to where he gets the ball - up for a board, say:
			// that is where it comes down.)
			const gets = this.ball.flatMap((s) =>
				s.kind === "fly" && "pid" in s.to && s.to.pid === tr.pid ? [s.t1] : [],
			);
			const ends = [...tr.moves];
			ends.forEach((m, i) => {
				const next = ends[i + 1]?.t0 ?? Infinity;
				if (gets.some((t) => t >= m.t1 && t <= Math.min(next, m.t1 + 1500))) {
					return;
				}
				for (const w of theirs) {
					const d = dist(m.to, w.at);
					const until = ends[i + 1]?.t0 ?? Infinity;
					// (Getting there just as he steps out of it is too late too.)
					if (until < w.t0 || m.t1 > w.t1 + 500 || d >= BODY) {
						continue;
					}
					const ref = d > 0.05 ? m.to : m.from;
					const dd = dist(ref, w.at) || 1;
					const was = m.to;
					let to = clampPt({
						x: w.at.x + ((ref.x - w.at.x) / dd) * BODY * 1.05,
						y: w.at.y + ((ref.y - w.at.y) / dd) * BODY * 1.05,
					});
					// (No faster for it than a quick step: a run too short to
					// take him all the way out takes him part of it, and the
					// ones after it the rest.)
					const most = Math.max(
						dist(m.from, m.to),
						((m.t1 - m.t0) / 1000) * STEP_OUT,
					);
					const len = dist(m.from, to);
					if (len > most) {
						to = {
							x: m.from.x + ((to.x - m.from.x) * most) / len,
							y: m.from.y + ((to.y - m.from.y) * most) / len,
						};
					}
					ends[i] = { ...m, to };
					const next = ends[i + 1];
					if (next && dist(next.from, was) < 0.01) {
						ends[i + 1] = { ...next, from: { ...to } };
					}
				}
			});
			tr.moves = ends.flatMap((m, i) => split(m, 0, ends[i - 1], ends[i + 1]));
		}
	}

	// A man planted for something - a screen, a post-up, a word with the
	// official - is planted no longer once he sets off: the pose ends as his
	// next run starts, rather than him sliding off across the floor in it.
	private unplant() {
		for (const tr of this.tracks.values()) {
			let cut = false;
			for (const a of tr.acts) {
				if (!IN_PLACE.has(a.anim)) {
					continue;
				}
				for (const m of tr.moves) {
					if (m.t0 >= a.t1) {
						break;
					}
					if (m.t1 > a.t0 + 1 && dist(m.from, m.to) > 1.5) {
						a.t1 = Math.max(a.t0, Math.min(a.t1, m.t0));
						cut = true;
					}
				}
			}
			if (cut) {
				tr.acts = tr.acts.filter(
					(a) => !IN_PLACE.has(a.anim) || a.t1 - a.t0 > 120,
				);
			}
		}
	}

	// BODIES APART. Whatever else has them where they are, two men are
	// never on top of each other: a man who would be is eased a step aside
	// for as long as it lasts - teammates a step and a half apart, a man
	// and his opponent no closer than shoulder to shoulder - unless they
	// are in it together (a screen, a post-up, a box-out, a high five). The
	// man with the ball, or in the middle of a shot or a catch, holds his
	// ground; of two others, the defender gives way, or the one standing.
	// Where he is at t, his steps aside (see keepApart) and all.
	private spotAt(tr: Track, t: number): Pt {
		const k = lastBy(tr.moves, t, (m) => m.t0);
		let x = tr.start.x;
		let y = tr.start.y;
		if (k >= 0) {
			const m = tr.moves[k]!;
			const e =
				t >= m.t1
					? 1
					: 0.5 - 0.5 * Math.cos((Math.PI * (t - m.t0)) / (m.t1 - m.t0));
			x = m.from.x + (m.to.x - m.from.x) * e;
			y = m.from.y + (m.to.y - m.from.y) * e;
		}
		const list = tr.nudges ?? [];
		for (let i = lastBy(list, t, (n) => n.t0); i >= 0; i--) {
			const n = list[i]!;
			if (t - n.t0 > 20000) {
				break;
			}
			if (t >= n.t1) {
				continue;
			}
			const q = Math.min(n.ramp ?? NUDGE_RAMP, (n.t1 - n.t0) / 2);
			const u = Math.min(1, (t - n.t0) / q, (n.t1 - t) / q);
			const w = u * u * (3 - 2 * u);
			x += n.dx * w;
			y += n.dy * w;
		}
		return { x, y };
	}

	// Two of a side going by each other - crossing on their cuts, one running
	// past another who stands - go by at each other's shoulder, not through
	// each other: each veers a little as they pass (the one standing, if one
	// is, takes the step). Too quick for keepApart to see, which lets men on
	// the move go by.
	private passBy(fast: [number, number][]) {
		const STEP = 100;
		// Clear of each other (feet, middle to middle) and how fast a man is
		// going by (feet a second).
		const BY = 1.5;
		const MOVING = 4;
		// Over and back again (ms).
		const SPAN = 450;
		const RAMP = 400;
		// Nobody steps out of these for it.
		const KEEP = new Set<AnimName>([
			"screen",
			"postUp",
			"boxOut",
			"fight",
			"rebound",
			"board",
			"snatch",
			"pickup",
			"poke",
			"block",
			"highFive",
			"lowFive",
			"chestBump",
			"waitFive",
			"fall",
			"takeCharge",
			"helpUp",
			"hurt",
			"hurtKnee",
			"hurtAnkle",
			"hurtHead",
			"hurtHand",
			"hurtArm",
		]);
		// Standing in one of these - up contesting a shot, letting one go,
		// taking a pass - or set in one of those above (a screen, say), he
		// stays put: the man going by veers round him - off the screen at
		// his shoulder, not through him. (Going up with a shot in close too:
		// where he lets it go from is where it goes from - in front of the
		// glass, not round behind it.)
		const STAND = new Set<AnimName>([
			...UP_IN_CLOSE,
			...IN_PLACE,
			"contest",
			"shoot",
			"setShot",
			"fade",
			"hook",
			"catch",
			"pass",
			"passBounce",
			"passOverhead",
			"passPush",
			"passBouncePush",
			"passBehind",
		]);
		const busy = (tr: Track, t: number) =>
			tr.acts.some(
				(a) => a.t0 < t + SPAN && a.t1 > t - SPAN && KEEP.has(a.anim),
			);
		const stands = (tr: Track, t: number) =>
			tr.acts.some(
				(a) => a.t0 < t + SPAN && a.t1 > t - SPAN && STAND.has(a.anim),
			);
		const shownAt = (tr: Track, t: number) => {
			const k = lastBy(tr.shown, t, (x) => x[0]);
			return k >= 0 && tr.shown[k]![1];
		};
		const tracks = [...this.tracks.values()].filter((tr) =>
			tr.shown.some(([, on]) => on),
		);
		type Pass = {
			a: Track;
			b: Track;
			t: number;
			d: number;
			dx: number;
			dy: number;
			va: number;
			vb: number;
		};
		const found = new Map<string, Pass>();
		const passes: Pass[] = [];
		let from = 0;
		const live: [number, number][] = [];
		for (const [a, b] of fast) {
			if (a > from) {
				live.push([from, a]);
			}
			from = Math.max(from, b);
		}
		live.push([from, this.T]);
		const N = tracks.length;
		const ax = new Float64Array(N);
		const ay = new Float64Array(N);
		const aOn = new Uint8Array(N);
		const bx = new Float64Array(N);
		const by = new Float64Array(N);
		const bOn = new Uint8Array(N);
		for (const [l0, l1] of live) {
			aOn.fill(0);
			for (let t = l0; t < l1; t += STEP) {
				// (b: now; a: a look ago.)
				for (let i = 0; i < N; i++) {
					const tr = tracks[i]!;
					bOn[i] = 0;
					if (shownAt(tr, t)) {
						const P = this.spotAt(tr, t);
						if (P.y > 0 && P.y < COURT_H && P.x > 0 && P.x < COURT_W) {
							bx[i] = P.x;
							by[i] = P.y;
							bOn[i] = 1;
						}
					}
				}
				for (let i = 0; i < N; i++) {
					if (!aOn[i] || !bOn[i]) {
						continue;
					}
					const a = tracks[i]!;
					for (let j = i + 1; j < N; j++) {
						if (!aOn[j] || !bOn[j]) {
							continue;
						}
						const b = tracks[j]!;
						if (a.team !== b.team) {
							continue;
						}
						// Nearest between the two looks: the gap between them
						// closes along a line.
						const rx = ax[i]! - ax[j]!;
						const ry = ay[i]! - ay[j]!;
						const wx = bx[i]! - bx[j]! - rx;
						const wy = by[i]! - by[j]! - ry;
						if (
							Math.min(rx * rx + ry * ry, (rx + wx) ** 2 + (ry + wy) ** 2) >
							(BY + Math.hypot(wx, wy)) ** 2
						) {
							continue;
						}
						const ww = wx * wx + wy * wy;
						const u =
							ww > 1e-9
								? Math.max(0, Math.min(1, -(rx * wx + ry * wy) / ww))
								: 0;
						const dx = rx + wx * u;
						const dy = ry + wy * u;
						const d = Math.hypot(dx, dy);
						if (d >= BY) {
							continue;
						}
						const va =
							(Math.hypot(bx[i]! - ax[i]!, by[i]! - ay[i]!) * 1000) / STEP;
						const vb =
							(Math.hypot(bx[j]! - ax[j]!, by[j]! - ay[j]!) * 1000) / STEP;
						if (va < MOVING && vb < MOVING) {
							continue;
						}
						const key = `${a.pid}:${b.pid}`;
						const at = t - STEP + u * STEP;
						const was = found.get(key);
						if (was && at - was.t < SPAN * 2) {
							if (d < was.d) {
								Object.assign(was, { t: at, d, dx, dy, va, vb });
							}
							continue;
						}
						const p = { a, b, t: at, d, dx, dy, va, vb };
						found.set(key, p);
						passes.push(p);
					}
				}
				ax.set(bx);
				ay.set(by);
				aOn.set(bOn);
			}
		}
		for (const p of passes) {
			const t = Math.round(p.t);
			// Which way apart: the way they are already apart - or, right on
			// each other, across the way they are going.
			let ux = p.dx;
			let uy = p.dy;
			let u = Math.hypot(ux, uy);
			if (u < 0.2) {
				const A = this.spotAt(p.a, t + STEP);
				const B = this.spotAt(p.b, t + STEP);
				const vx = A.x - B.x;
				const vy = A.y - B.y;
				const v = Math.hypot(vx, vy) || 1;
				ux = -vy / v;
				uy = vx / v;
				u = 1;
			}
			ux /= u;
			uy /= u;
			const k = BY - p.d;
			const steps: [Track, number][] =
				p.va >= MOVING && p.vb >= MOVING
					? [
							[p.a, k / 2],
							[p.b, -k / 2],
						]
					: p.va < MOVING
						? stands(p.a, t) || busy(p.a, t)
							? [[p.b, -k]]
							: [[p.a, k]]
						: stands(p.b, t) || busy(p.b, t)
							? [[p.a, k]]
							: [[p.b, -k]];
			for (const [tr, by] of steps) {
				if (busy(tr, t)) {
					continue;
				}
				// (Clapping, having words: his step aside is over before he
				// starts, or starts once he is done - not sliding him in the
				// middle of it.)
				let n0 = t - SPAN;
				let n1 = t + SPAN;
				for (const a of tr.acts) {
					if (!REACTIONS.has(a.anim) || a.t1 <= n0 || a.t0 >= n1) {
						continue;
					}
					if (a.t0 > t) {
						n1 = Math.min(n1, a.t0);
					} else if (a.t1 < t) {
						n0 = Math.max(n0, a.t1);
					} else {
						n1 = n0;
					}
				}
				if (n1 - n0 < RAMP) {
					continue;
				}
				const P = this.spotAt(tr, t);
				// On the move, square across the way he is going - a veer, no
				// quicker or slower for it (his stride is his speed).
				let sx = ux;
				let sy = uy;
				const P1 = this.spotAt(tr, t + 50);
				const vx = P1.x - P.x;
				const vy = P1.y - P.y;
				const v = Math.hypot(vx, vy);
				if ((v * 1000) / 50 >= MOVING) {
					const side = Math.sign(-vy * ux + vx * uy) || 1;
					sx = (-vy / v) * side;
					sy = (vx / v) * side;
				}
				const Q = { x: P.x + sx * by, y: P.y + sy * by };
				const into = tracks.some(
					(o) =>
						o !== tr &&
						o !== p.a &&
						o !== p.b &&
						shownAt(o, t) &&
						dist(this.spotAt(o, t), Q) < 1.2,
				);
				if (into || dist(inPlay(Q), Q) > dist(inPlay(P), P) + 0.01) {
					continue;
				}
				const list = (tr.nudges ??= []);
				list.push({
					t0: n0,
					t1: n1,
					dx: sx * by,
					dy: sy * by,
					ramp: RAMP,
				});
				list.sort((x, y) => x.t0 - y.t0);
			}
		}
	}

	// Kept off a man (see keepOff), a defender can end up where a pass goes
	// by after all: he takes a step off its line as it does - the ball by
	// his shoulder, not through his chest. (Read off the finished picture,
	// `view`: where the ball really goes, from hand to hand.)
	private clearTheBall(view: CourtTimeline, fast: [number, number][]) {
		// Over his head; how near his middle it can come and miss him - and
		// the step's length past that, and how long (ms) he is over.
		const BODY = 6.9;
		const NEAR = 1.1;
		const ROOM = 0.3;
		const SPAN = 380;
		const RAMP = 300;
		const ROUGH = 2.5;
		const ROUGH_MS = 30;
		const OUT = 1.3;
		const IN = 1;
		const BUSY = new Set<AnimName>([
			"screen",
			"postUp",
			"boxOut",
			"fight",
			"rebound",
			"board",
			"snatch",
			"pickup",
			"poke",
			"reach",
			"block",
			"contest",
			"fall",
			"takeCharge",
			"helpUp",
		]);
		const body = bodyOf();
		const bodyFor = () => body;
		const live = (t: number) => !fast.some(([a, b]) => t >= a && t < b);
		const tracks = [...this.tracks.values()];
		for (let i = 0; i < this.ball.length; i++) {
			const s = this.ball[i]!;
			if (s.kind !== "fly" || !("pid" in s.from) || !live(s.t0)) {
				continue;
			}
			// (A bounce pass: to the floor, and up off it to him.)
			const after = this.ball[i + 1];
			const last =
				!("pid" in s.to) && after?.kind === "fly" && after.t0 === s.t1
					? after
					: s;
			if (last.kind !== "fly" || !("pid" in last.to)) {
				continue;
			}
			const team = this.teamOf(s.from.pid);
			if (this.teamOf(last.to.pid) !== team) {
				continue;
			}
			// (Only near a man anywhere near its way is where it goes worked
			// out exactly: from the one man to the other, give or take.)
			const from = this.track(s.from.pid);
			const to = this.track(last.to.pid);
			const A0 = from && floorSpotOf(from, s.t0);
			const C0 = to && floorSpotOf(to, last.t1);
			if (!A0 || !C0) {
				continue;
			}
			// (Hand to hand: out in front of each.)
			const L0 = dist(A0, C0) || 1;
			const ux = (C0.x - A0.x) / L0;
			const uy = (C0.y - A0.y) / L0;
			const A = { x: A0.x + ux * OUT, y: A0.y + uy * OUT };
			const C = { x: C0.x - ux * IN, y: C0.y - uy * IN };
			const them = tracks.filter(
				(tr) => tr.team !== team && floorSpotOf(tr, s.t0) !== undefined,
			);
			type Hit = { tr: Track; t: number; d: number; ux: number; uy: number };
			let hit: Hit | undefined;
			// Its way, exactly, once a man is anywhere near it: out of the
			// one's hands and into the other's along a line (by way of the
			// floor, a bounce pass), up and down under gravity.
			let way: ((t: number) => Pt3) | undefined;
			const wayOf = (): ((t: number) => Pt3) => {
				const P0 = evalBall(view, s.t0, bodyFor);
				const P1 = evalBall(view, last.t1, bodyFor);
				const legs =
					last === s
						? [{ t0: s.t0, t1: s.t1, a: P0, b: P1 }]
						: [
								{ t0: s.t0, t1: s.t1, a: P0, b: s.to as Pt3 },
								{ t0: last.t0, t1: last.t1, a: s.to as Pt3, b: P1 },
							];
				return (t: number) => {
					const l = legs.find((g) => t < g.t1) ?? legs.at(-1)!;
					const T = (l.t1 - l.t0) / 1000;
					const tau = Math.max(0, Math.min(T, (t - l.t0) / 1000));
					const u = T > 0 ? tau / T : 1;
					const vz = T > 0 ? (l.b.z - l.a.z) / T + 0.5 * GRAVITY * T : 0;
					return {
						x: l.a.x + (l.b.x - l.a.x) * u,
						y: l.a.y + (l.b.y - l.a.y) * u,
						z: l.a.z + vz * tau - 0.5 * GRAVITY * tau * tau,
					};
				};
			};
			for (let t = s.t0 + 45; t < last.t1 - 45; t += ROUGH_MS) {
				const u = (t - s.t0) / (last.t1 - s.t0);
				const W = { x: A.x + (C.x - A.x) * u, y: A.y + (C.y - A.y) * u };
				for (const tr of them) {
					const P = floorSpotOf(tr, t);
					if (P === undefined || dist(P, W) >= ROUGH) {
						continue;
					}
					way ??= wayOf();
					const B = way(t);
					const d = dist(P, B);
					if (B.z <= BODY && d < NEAR && (!hit || d < hit.d)) {
						hit = { tr, t, d, ux, uy };
					}
				}
			}
			const got = hit;
			if (
				!got ||
				got.tr.acts.some(
					(a) => a.t0 < got.t + SPAN && a.t1 > got.t - SPAN && BUSY.has(a.anim),
				)
			) {
				continue;
			}
			// Off to the side of its line he is on already (either, right on
			// it).
			const t = Math.round(got.t);
			const P = floorSpotOf(got.tr, t)!;
			const B = evalBall(view, t, bodyFor);
			const side = Math.sign((P.x - B.x) * -got.uy + (P.y - B.y) * got.ux) || 1;
			const k = NEAR + ROOM - got.d;
			const dx = -got.uy * side * k;
			const dy = got.ux * side * k;
			const Q = { x: P.x + dx, y: P.y + dy };
			if (
				dist(inPlay(Q), Q) > 0.01 ||
				tracks.some((o) => {
					if (o === got.tr) {
						return false;
					}
					const O = floorSpotOf(o, t);
					return O !== undefined && dist(O, Q) < 1.2;
				})
			) {
				continue;
			}
			const list = (got.tr.nudges ??= []);
			list.push({ t0: t - SPAN, t1: t + SPAN, dx, dy, ramp: RAMP });
			list.sort((x, y) => x.t0 - y.t0);
		}
	}

	// A pass never goes through a man in its way. With one in the lane -
	// his own man up on him, a help defender in between - it is bounced by
	// him, off the floor beside his feet; or, too far to bounce, thrown by
	// his ear, the passer turned a little away from him (and a pivot step
	// aside, if that is what it takes).
	private aroundTheMan(fast: [number, number][]) {
		// Out in front of him as it leaves his hands; the height it goes at,
		// and how near a man's middle it can come and miss him.
		const OUT = 2.3;
		const CHEST = 4.1;
		const LOW = 3;
		const CATCH = 4;
		const BODY = 6.9;
		const NEAR = 1.1;
		// Over the top, from up over his head.
		const OUT_OVER = 1.6;
		const HIGH = 5.4;
		// Bounced only so far; his own man is no further off him than this.
		const BOUNCE_MAX = 24;
		const NEAR_PASSER = 7;
		// A pivot step aside (feet), taken this quick (ms).
		const PIVOT = 1.1;
		const PIVOT_MS = 200;
		// Off the floor this far to the side of him, a little past him - and
		// not at the passer's feet. Each leg takes this long at least (ms).
		const CLEAR = 1.8;
		const PAST = 0.8;
		const BOUNCE_NEAR = 4.8;
		const LEG_MS = 150;
		const inBounds = (p: Pt) =>
			p.x > 0.5 && p.x < COURT_W - 0.5 && p.y > 0.5 && p.y < COURT_H - 0.5;
		// A step off the ball's line (beyond missing him), taken this quick;
		// and what nobody steps out of to do it.
		const SHADE = 0.3;
		const SHADE_MS = 350;
		const BUSY = new Set<AnimName>([
			"screen",
			"postUp",
			"boxOut",
			"fight",
			"rebound",
			"board",
			"snatch",
			"pickup",
			"poke",
			"reach",
			"block",
			"contest",
			"dunk",
		]);
		const shownAt = (tr: Track, t: number) => {
			const k = lastBy(tr.shown, t, (x) => x[0]);
			return k >= 0 && tr.shown[k]![1];
		};
		type Leg = { t0: number; t1: number; a: Pt3; b: Pt3 };
		const ballAt = (legs: Leg[], t: number): Pt3 => {
			const l = legs.find((g) => t < g.t1) ?? legs.at(-1)!;
			const T = (l.t1 - l.t0) / 1000;
			const tau = Math.max(0, Math.min(T, (t - l.t0) / 1000));
			const u = T > 0 ? tau / T : 1;
			const vz = T > 0 ? (l.b.z - l.a.z) / T + 0.5 * GRAVITY * T : 0;
			return {
				x: l.a.x + (l.b.x - l.a.x) * u,
				y: l.a.y + (l.b.y - l.a.y) * u,
				z: l.a.z + vz * tau - 0.5 * GRAVITY * tau * tau,
			};
		};
		// The first man the ball would go through, and when.
		const inTheWay = (
			team: Side,
			legs: Leg[],
		): { tr: Track; t: number } | undefined => {
			const t0 = legs[0]!.t0;
			const t1 = legs.at(-1)!.t1;
			for (let t = t0 + 60; t < t1 - 60; t += 30) {
				const b = ballAt(legs, t);
				if (b.z > BODY) {
					continue;
				}
				for (const tr of this.tracks.values()) {
					if (tr.team === team) {
						continue;
					}
					if (!shownAt(tr, t)) {
						continue;
					}
					if (dist(this.spotAt(tr, t), b) < NEAR) {
						return { tr, t };
					}
				}
			}
			return undefined;
		};
		const live = (t: number) => !fast.some(([a, b]) => t >= a && t < b);
		for (let i = 0; i < this.ball.length; i++) {
			const s = this.ball[i]!;
			if (s.kind !== "fly" || !("pid" in s.from) || !live(s.t0)) {
				continue;
			}
			// A bounce pass: to the floor, and up off it to him.
			const after = this.ball[i + 1];
			const next =
				!("pid" in s.to) &&
				after?.kind === "fly" &&
				after.t0 === s.t1 &&
				!("pid" in after.from) &&
				"pid" in after.to
					? { seg: after, to: after.to.pid }
					: undefined;
			const to = "pid" in s.to ? s.to.pid : next?.to;
			const from = this.track(s.from.pid);
			const recv = to === undefined ? undefined : this.track(to);
			if (!from || !recv || recv === from || recv.team !== from.team) {
				continue;
			}
			const release = s.t0;
			const arrive = next ? next.seg.t1 : s.t1;
			const act = from.acts.find(
				(a) => PASS_ACTS.has(a.anim) && a.t0 <= release && a.t1 > release,
			);
			if (!act || arrive - release < 2 * LEG_MS + 60) {
				continue;
			}
			const over = act.anim === "passOverhead";
			const A = this.spotAt(from, release);
			const B = this.spotAt(recv, arrive);
			const L = dist(A, B);
			if (L < 7) {
				continue;
			}
			// Which way he faces as it goes: turned from where he was looking
			// (whatever he did last, or the rim) toward it, as far as he
			// gets by then (see yawTarget).
			const before = from.acts.findLast(
				(a) => a !== act && a.t0 < act.t0 && a.t1 > act.t0 - PASS_TURN,
			);
			// On the move up to it, he faces where he was going (squared up
			// to the rim, walking it a few steps).
			const mv = from.moves[lastBy(from.moves, act.t0, (m) => m.t0)];
			const going =
				mv && mv.t1 > act.t0 - PASS_TURN && dist(mv.from, mv.to) >= 3
					? mv
					: undefined;
			const back = going?.anim === "back" ? -1 : 1;
			const heading =
				going &&
				!(
					going.anim === "dribble" &&
					going.face !== undefined &&
					dist(going.from, going.to) < 8
				)
					? {
							x: A.x + (going.to.x - going.from.x) * back,
							y: A.y + (going.to.y - going.from.y) * back,
						}
					: undefined;
			const facing = before?.look ?? heading ?? rimPt(from.team);
			const turnFrom = Math.max(
				act.t0 - PASS_TURN,
				before?.t1 ?? -Infinity,
				going && !(PASS_PICK_UP.has(going.anim) && going.t1 <= act.t0 + 40)
					? Math.min(going.t1, act.t0)
					: -Infinity,
			);
			const was = Math.atan2(facing.y - A.y, facing.x - A.x);
			const most = TURN_RATE * Math.max(0, release - turnFrom);
			const legsVia = (look: Pt, hit?: Pt, step = { x: 0, y: 0 }): Leg[] => {
				let d = Math.atan2(look.y - A.y, look.x - A.x) - was;
				d -= Math.round(d / (2 * Math.PI)) * 2 * Math.PI;
				const yaw = was + Math.max(-most, Math.min(most, d));
				const out = over ? OUT_OVER : OUT;
				const R = {
					x: A.x + step.x + Math.cos(yaw) * out,
					y: A.y + step.y + Math.sin(yaw) * out,
				};
				const Bz = { ...B, z: CATCH };
				if (!hit) {
					return [
						{
							t0: release,
							t1: arrive,
							a: { ...R, z: over ? HIGH : CHEST },
							b: Bz,
						},
					];
				}
				const d1 = dist(R, hit);
				const d2 = dist(hit, B);
				// (Neither leg a flick: thrown down hard, or up off the floor
				// into his hands.)
				const tHit = Math.round(
					Math.min(
						arrive - LEG_MS,
						Math.max(
							release + LEG_MS,
							release + ((arrive - release) * 0.9 * d1) / (d1 + d2 || 1),
						),
					),
				);
				const H = { x: hit.x, y: hit.y, z: BALL_R };
				return [
					{ t0: release, t1: tHit, a: { ...R, z: LOW }, b: H },
					{ t0: tHit, t1: arrive, a: H, b: Bz },
				];
			};
			const path = next && !("pid" in s.to) ? legsVia(s.to, s.to) : legsVia(B);
			const hit = inTheWay(from.team, path);
			if (!hit) {
				continue;
			}
			const u = unitVec(A, B);
			const n = { x: -u.y, y: u.x };
			const D = this.spotAt(hit.tr, hit.t);
			const along = (D.x - A.x) * u.x + (D.y - A.y) * u.y;
			const side = (D.x - A.x) * n.x + (D.y - A.y) * n.y;
			const pref = side >= 0 ? -1 : 1;
			// Bounced by him, off the floor beside his feet.
			let done = false;
			if (!over && (next || L <= BOUNCE_MAX) && along >= 2 && along <= L - 4) {
				const at = Math.min(L - 4, Math.max(BOUNCE_NEAR, along + PAST));
				for (const sgn of [pref, -pref]) {
					const off = side + sgn * CLEAR;
					const P = {
						x: A.x + u.x * at + n.x * off,
						y: A.y + u.y * at + n.y * off,
					};
					if (!inBounds(P)) {
						continue;
					}
					const legs = legsVia(P, P);
					if (inTheWay(from.team, legs)) {
						continue;
					}
					act.anim = "passBounce";
					act.look = { ...P };
					const H = legs[0]!.b;
					const tHit = legs[0]!.t1;
					if (next) {
						s.t1 = tHit;
						s.to = { ...H };
						next.seg.t0 = tHit;
						next.seg.from = { ...H };
					} else {
						this.ball.splice(
							i,
							1,
							{
								kind: "fly",
								t0: release,
								t1: tHit,
								from: s.from,
								to: { ...H },
							},
							{ kind: "fly", t0: tHit, t1: arrive, from: { ...H }, to: s.to },
						);
						i++;
					}
					done = true;
					break;
				}
			}
			// Or, his own man up on him, thrown by his ear: turned a little
			// away from him to get it out past him - a pivot step away from
			// him first, if that is what it takes.
			turns: for (const [k, pivot] of done || next || along > NEAR_PASSER
				? []
				: ([
						[0.3, 0],
						[0.5, 0],
						[0.3, PIVOT],
						[0.5, PIVOT],
						[0.7, PIVOT],
					] as const)) {
				for (const sgn of [pref, -pref]) {
					// (Turned toward n, round to his left, for sgn 1.)
					const yaw = Math.atan2(u.y, u.x) + sgn * k;
					const look = {
						x: A.x + Math.cos(yaw) * 10,
						y: A.y + Math.sin(yaw) * 10,
					};
					const step = { x: n.x * sgn * pivot, y: n.y * sgn * pivot };
					if (pivot > 0 && !inBounds({ x: A.x + step.x, y: A.y + step.y })) {
						continue;
					}
					if (inTheWay(from.team, legsVia(look, undefined, step))) {
						continue;
					}
					act.look = look;
					if (pivot > 0) {
						(from.nudges ??= []).push({
							t0: act.t0 - PIVOT_MS,
							t1: release + 500,
							dx: step.x,
							dy: step.y,
							ramp: PIVOT_MS,
						});
						from.nudges.sort((a, b) => a.t0 - b.t0);
					}
					done = true;
					break turns;
				}
			}
			// Or - closing out on the catch, or in the lane under a pass that
			// goes over him - he is a step off its line as it goes by, not
			// standing in it.
			const busy = hit.tr.acts.some(
				(a) =>
					a.t0 < hit.t + SHADE_MS &&
					a.t1 > hit.t - SHADE_MS &&
					BUSY.has(a.anim),
			);
			if (done || busy) {
				continue;
			}
			const b = ballAt(path, hit.t);
			const legAt = path.find((g) => hit.t < g.t1) ?? path.at(-1)!;
			const w = unitVec(legAt.a, legAt.b);
			const off = (D.x - b.x) * -w.y + (D.y - b.y) * w.x;
			const by = (off >= 0 ? 1 : -1) * (NEAR + SHADE - Math.abs(off));
			const nudge = {
				t0: hit.t - SHADE_MS - 100,
				t1: hit.t + SHADE_MS + 200,
				dx: -w.y * by,
				dy: w.x * by,
				ramp: SHADE_MS,
			};
			const stepTo = { x: D.x + nudge.dx, y: D.y + nudge.dy };
			const crowded = [...this.tracks.values()].some(
				(tr) =>
					tr !== hit.tr &&
					shownAt(tr, hit.t) &&
					dist(this.spotAt(tr, hit.t), stepTo) < 1.3,
			);
			if (crowded || !inBounds(stepTo)) {
				continue;
			}
			const list = (hit.tr.nudges ??= []);
			list.push(nudge);
			list.sort((a, c) => a.t0 - c.t0);
			if (inTheWay(from.team, path)?.tr === hit.tr) {
				list.splice(list.indexOf(nudge), 1);
			}
		}
	}

	// The way in clear. Whatever everything before left him doing - beaten
	// on the drive, walled up in front of it, in to the glass, on his way
	// back to his man - a man of theirs whose schedule has him in the
	// shooter's way as he goes in and up for a shot in close, or in him as
	// he comes down, gives a step to the side of it as the shooter comes by,
	// and is back after. (Not the man up to block it: he is meant to be
	// right there - nor one knocking the ball away: where his hand is, it
	// goes.)
	private clearTheWayIn() {
		const FOR_THE_BALL = new Set<AnimName>(["block", "poke", "strip"]);
		const tracks = [...this.tracks.values()];
		// Where he is - nudged aside already, too - running each run as the
		// court does, or (`even`) at an even pace along it, as a man already
		// going at either end of it more nearly does.
		const at = (tr: Track, t: number, even: boolean): Pt => {
			const P = this.spotAt(tr, t);
			if (!even) {
				return P;
			}
			const m = tr.moves[lastBy(tr.moves, t, (x) => x.t0)];
			if (!m || t >= m.t1) {
				return P;
			}
			const u = (t - m.t0) / (m.t1 - m.t0);
			const e = 0.5 - 0.5 * Math.cos(Math.PI * u);
			return {
				x: P.x + (m.to.x - m.from.x) * (u - e),
				y: P.y + (m.to.y - m.from.y) * (u - e),
			};
		};
		// One step aside for him, if his schedule has him in the shooter's
		// way over [t0, t1].
		const aside = (sh: Track, tr: Track, t0: number, t1: number) => {
			let worst: { d: number; t: number; O: Pt; S: Pt } | undefined;
			let ta = Infinity;
			let tb = -Infinity;
			for (let t = t0; t <= t1; t += 40) {
				for (const e of [false, true]) {
					for (const f of [false, true]) {
						const O = at(tr, t, e);
						const S = at(sh, t, f);
						const d = dist(O, S);
						if (d < GIVE_WAY + 0.3) {
							ta = Math.min(ta, t);
							tb = Math.max(tb, t);
						}
						if (!worst || d < worst.d) {
							worst = { d, t, O, S };
						}
					}
				}
			}
			if (!worst || worst.d >= GIVE_WAY + GIVE_WAY_SLACK) {
				return;
			}
			// Off to his side of the way the shooter is going then - the way
			// they are going by each other: a man running across his way steps
			// off the line they close on, not on along it, into him again a
			// moment later.
			const A = this.spotAt(sh, worst.t - 120);
			const B = this.spotAt(sh, worst.t + 120);
			const OA = this.spotAt(tr, worst.t - 120);
			const OB = this.spotAt(tr, worst.t + 120);
			const rel = {
				x: B.x - A.x - (OB.x - OA.x),
				y: B.y - A.y - (OB.y - OA.y),
			};
			const u =
				Math.hypot(rel.x, rel.y) > 0.2
					? unitVec({ x: 0, y: 0 }, rel)
					: unitVec(worst.S, rimPt(sh.team));
			const n = { x: -u.y, y: u.x };
			const off = (worst.O.x - worst.S.x) * n.x + (worst.O.y - worst.S.y) * n.y;
			const n0 = ta - GIVE_WAY_MS;
			let n1 = tb + GIVE_WAY_MS;
			// (Stepped aside already about then - a man sliding across his way,
			// say, on one side of it and then the other: the same way again,
			// not back across into him, the two steps undoing each other.)
			const was = (tr.nudges ?? [])
				.filter((x) => x.t0 < n1 && x.t1 > n0)
				.reduce((a, x) => a + x.dx * n.x + x.dy * n.y, 0);
			const s =
				Math.abs(was) > 0.1
					? Math.sign(was)
					: Math.abs(off) > 0.1
						? Math.sign(off)
						: tr.pid % 2
							? 1
							: -1;
			const k = s * (GIVE_WAY + GIVE_WAY_SLACK + 0.2) - off;
			// (Back where he was by the time the ball is in the air to him -
			// off the rim, say: where his hands are, it goes.)
			const to = this.ball.find(
				(b) =>
					b.kind === "fly" &&
					"pid" in b.to &&
					b.to.pid === tr.pid &&
					b.t1 > n0 &&
					b.t0 < n1,
			);
			if (to) {
				n1 = Math.min(n1, to.t0);
				if (n1 - n0 < 2 * GIVE_WAY_MS) {
					return;
				}
			}
			// (And back before he has words with the official over it, say -
			// not sliding back in the middle of that.)
			const then = tr.acts.find(
				(x) => REACTIONS.has(x.anim) && x.t0 > n0 && x.t0 < n1,
			);
			if (then) {
				n1 = then.t0;
				if (n1 - n0 < 2 * GIVE_WAY_MS) {
					return;
				}
			}
			(tr.nudges ??= []).push({
				t0: n0,
				t1: n1,
				dx: n.x * k,
				dy: n.y * k,
				ramp: GIVE_WAY_MS,
			});
			tr.nudges.sort((x, y) => x.t0 - y.t0);
		};
		// (Again, from where that leaves everybody - the way the shooter goes
		// turning under him, one step aside can leave him short.)
		for (let round = 0; round < 3; round++) {
			for (const sh of tracks) {
				for (const a of sh.acts) {
					if (!UP_IN_CLOSE.has(a.anim)) {
						continue;
					}
					const t0 = a.t0 - GIVE_WAY_BEFORE;
					const t1 = a.t1 + 200;
					for (const tr of tracks) {
						if (
							tr.team !== sh.team &&
							!tr.acts.some(
								(b) =>
									FOR_THE_BALL.has(b.anim) &&
									b.t0 < t1 + GIVE_WAY_MS + 1000 &&
									b.t1 > t0 - GIVE_WAY_MS,
							)
						) {
							aside(sh, tr, t0, t1);
						}
					}
				}
			}
		}
	}

	private keepApart(fast: [number, number][]) {
		const STEP = 100;
		const MATES = 2.6;
		const OPPS = 1.4;
		// In it together - a screen, a box-out - no nearer than this.
		const TOUCH = 1.3;
		// (Easing over and back, and the longest a step aside lasts.)
		const RAMP = NUDGE_RAMP;
		const LONGEST = 12000;
		// Stepping aside off a man he comes up on, he drifts over to it as he
		// comes - not a quick sidestep on top of pulling up, the two stopping
		// him dead together (ms).
		const ASIDE = 800;
		// Going faster than this (feet a second), he is only passing by.
		const MOVING = 4;
		// Two running the same way, though, keep this far apart (feet) - the
		// one coming up on the other goes by at his shoulder, not through him
		// - drifting over across the way they go (over this long, ms).
		const ALONGSIDE = 1.9;
		const DRIFT = 900;
		const TOGETHER = new Set<AnimName>([
			"screen",
			"postUp",
			"fight",
			"boxOut",
			"bump",
			"highFive",
			"lowFive",
			"chestBump",
			"waitFive",
			"reach",
			"poke",
			"block",
			"dunk",
			"dunk1",
			"tomahawk",
			"windmill",
			"fall",
			"takeCharge",
			"helpUp",
			"hurt",
			"hurtKnee",
			"hurtAnkle",
			"hurtHead",
			"hurtHand",
			"hurtArm",
			"rebound",
			"board",
			"snatch",
			"pickup",
		]);
		const MATES_TOGETHER = new Set<AnimName>([
			"highFive",
			"lowFive",
			"chestBump",
			"waitFive",
			"fall",
			"takeCharge",
			"helpUp",
			"hurt",
			"hurtKnee",
			"hurtAnkle",
			"hurtHead",
			"hurtHand",
			"hurtArm",
		]);
		const HOLDS = new Set<AnimName>([
			"shoot",
			"setShot",
			"fade",
			"hook",
			"layup",
			"fingerRoll",
			"floater",
			"powerLayup",
			"scoop",
			"reverseLayup",
			"catch",
			"pass",
			"passBounce",
			"passOverhead",
			"passPush",
			"passBouncePush",
			"passBehind",
			"contest",
			"triple",
			"jab",
			"shotFake",
			"follow",
		]);
		// Going up for the ball himself - off the rim, a hand to it, after it
		// on the floor - he is where it comes to: nobody moves him off it,
		// not even a man holding his own ground.
		const ON_BALL = new Set<AnimName>([
			"rebound",
			"board",
			"snatch",
			"pickup",
			"poke",
		]);
		// Set - in a screen, sealing in the post - he is a wall: whoever
		// comes by him, his own man off it included, goes round.
		const SET = new Set<AnimName>(["screen", "postUp"]);
		const tracks = [...this.tracks.values()];
		// Read off in time order: where each list is up to (the last of it
		// begun by t), never going back.
		const cursor = <X>(list: X[], key: (x: X) => number) => {
			let i = -1;
			return (t: number): number => {
				while (i + 1 < list.length && key(list[i + 1]!) <= t) {
					i++;
				}
				return i;
			};
		};
		type Reader = {
			tr: Track;
			move: (t: number) => number;
			shown: (t: number) => number;
			act: (t: number) => number;
			nudge: (t: number) => number;
		};
		const readers = (): Reader[] =>
			tracks.map((tr) => ({
				tr,
				move: cursor(tr.moves, (m) => m.t0),
				shown: cursor(tr.shown, (x) => x[0]),
				act: cursor(tr.acts, (a) => a.t0),
				nudge: cursor(tr.nudges ?? [], (n) => n.t0),
			}));
		const at = (r: Reader, t: number): Pt => {
			const { tr } = r;
			const k = r.move(t);
			let x: number;
			let y: number;
			if (k < 0) {
				x = tr.start.x;
				y = tr.start.y;
			} else {
				const m = tr.moves[k]!;
				if (t >= m.t1) {
					x = m.to.x;
					y = m.to.y;
				} else {
					const e =
						0.5 - 0.5 * Math.cos((Math.PI * (t - m.t0)) / (m.t1 - m.t0));
					x = m.from.x + (m.to.x - m.from.x) * e;
					y = m.from.y + (m.to.y - m.from.y) * e;
				}
			}
			const list = tr.nudges ?? [];
			for (let i = r.nudge(t); i >= 0; i--) {
				const n = list[i]!;
				if (t - n.t0 > LONGEST + 2 * DRIFT + STEP) {
					break;
				}
				if (t >= n.t1) {
					continue;
				}
				const q = Math.min(n.ramp ?? RAMP, (n.t1 - n.t0) / 2);
				const u = Math.min(1, (t - n.t0) / q, (n.t1 - t) / q);
				const w = u * u * (3 - 2 * u);
				x += n.dx * w;
				y += n.dy * w;
			}
			return { x, y };
		};
		const shown = (r: Reader, t: number) => {
			const k = r.shown(t);
			return k >= 0 && r.tr.shown[k]![1];
		};
		const huddled = (team: Side, x: number, y: number) =>
			Math.hypot(x - benchX(team), y - HUDDLE_Y) < 4.5;
		const doing = (r: Reader, t: number): AnimName | undefined => {
			const acts = r.tr.acts;
			for (let k = r.act(t); k >= 0; k--) {
				const a = acts[k]!;
				if (a.t1 > t) {
					return a.anim;
				}
				if (t - a.t0 > 4000) {
					break;
				}
			}
			return undefined;
		};
		// Where a man is at any t, and whether he is on the floor (read
		// straight off his runs, for the few times it is needed).
		const anyTime = new Map<Track, Reader>();
		const posAt = (tr: Track, t: number): Pt => {
			let r = anyTime.get(tr);
			if (!r) {
				r = {
					tr,
					move: (u) => lastBy(tr.moves, u, (m) => m.t0),
					shown: (u) => lastBy(tr.shown, u, (x) => x[0]),
					act: (u) => lastBy(tr.acts, u, (a) => a.t0),
					nudge: (u) => lastBy(tr.nudges ?? [], u, (n) => n.t0),
				};
				anyTime.set(tr, r);
			}
			return at(r, t);
		};
		// The first of his acts (as listed) going on at t.
		const byStart = new Map<
			Track,
			{ list: { a: Act; i: number }[]; span: number }
		>();
		const actAt = (tr: Track, t: number): Act | undefined => {
			let got = byStart.get(tr);
			if (!got) {
				got = {
					list: tr.acts
						.map((a, i) => ({ a, i }))
						.sort((x, y) => x.a.t0 - y.a.t0),
					span: Math.max(0, ...tr.acts.map((a) => a.t1 - a.t0)),
				};
				byStart.set(tr, got);
			}
			const { list, span } = got;
			let first: { a: Act; i: number } | undefined;
			for (let k = lastBy(list, t, (x) => x.a.t0); k >= 0; k--) {
				const x = list[k]!;
				if (t - x.a.t0 > span) {
					break;
				}
				if (x.a.t1 > t && (!first || x.i < first.i)) {
					first = x;
				}
			}
			return first?.a;
		};
		const shownAtT = (tr: Track, t: number) => {
			const k = lastBy(tr.shown, t, (x) => x[0]);
			return k >= 0 && tr.shown[k]![1];
		};
		// When the ball is in each man's hands.
		const holds = new Map<number, [number, number][]>();
		this.ball.forEach((b, i) => {
			if (b.kind === "hold") {
				const list = holds.get(b.pid) ?? [];
				list.push([b.t0, this.ball[i + 1]?.t0 ?? Infinity]);
				holds.set(b.pid, list);
			}
		});
		// The stretches shown at real speed.
		const live: [number, number][] = [];
		let from = 0;
		for (const [a, b] of fast) {
			if (a > from) {
				live.push([from, a]);
			}
			from = Math.max(from, b);
		}
		live.push([from, this.T]);
		// (The second time through, only round where the first found any.)
		let looks = live;
		for (let pass = 0; pass < 2; pass++) {
			type Run = {
				a: Track;
				b: Track;
				t0: number;
				t1: number;
				near: number;
				ux: number;
				uy: number;
				n: number;
				vx: number;
				vy: number;
				par: boolean;
				// (His own way, running together.)
				mx: number;
				my: number;
			};
			const open = new Map<string, Run>();
			const done: Run[] = [];
			// (Only men who ever get on the floor.)
			const rd = readers().filter((r) => r.tr.shown.some(([, on]) => on));
			const N = rd.length;
			const px = new Float64Array(N);
			const py = new Float64Array(N);
			const pv = new Float64Array(N);
			const pvx = new Float64Array(N);
			const pvy = new Float64Array(N);
			const lastT = new Float64Array(N).fill(-Infinity);
			const on: number[] = [];
			const acts: (AnimName | undefined | null)[] = Array.from(
				{ length: N },
				() => null,
			);
			const ballAt = cursor(this.ball, (x) => x.t0);
			const possAt = cursor(this.poss, (x) => x[0]);
			const close = (key: string) => {
				const r = open.get(key);
				if (r) {
					open.delete(key);
					if (r.t1 - r.t0 >= STEP * 2) {
						done.push(r);
					}
				}
			};
			const actOf = (i: number, t: number): AnimName | undefined => {
				let a = acts[i];
				if (a === null) {
					a = doing(rd[i]!, t);
					acts[i] = a;
				}
				return a;
			};
			for (const [l0, l1] of looks) {
				for (let t = l0; t < l1; t += STEP) {
					const pk = possAt(t);
					const off = pk >= 0 ? this.poss[pk]![1] : 1;
					const bs = this.ball[ballAt(t)];
					const holder = bs?.kind === "hold" ? bs.pid : undefined;
					on.length = 0;
					for (let i = 0; i < N; i++) {
						const r = rd[i]!;
						if (!shown(r, t)) {
							lastT[i] = -Infinity;
							continue;
						}
						const p = at(r, t);
						// How fast he is going: on the move, he is past it in a
						// moment - it is a man standing on another that shows.
						const fresh = lastT[i] === t - STEP;
						pvx[i] = fresh ? (p.x - px[i]!) / (STEP / 1000) : 0;
						pvy[i] = fresh ? (p.y - py[i]!) / (STEP / 1000) : 0;
						pv[i] = Math.hypot(pvx[i]!, pvy[i]!);
						px[i] = p.x;
						py[i] = p.y;
						lastT[i] = t;
						acts[i] = null;
						on.push(i);
					}
					for (let a = 0; a < on.length; a++) {
						for (let b = a + 1; b < on.length; b++) {
							const ia = on[a]!;
							const ib = on[b]!;
							const A = rd[ia]!.tr;
							const B = rd[ib]!.tr;
							const mates = A.team === B.team;
							// (On the floor: off it, coming on and going off at the
							// table, they pass as they please.)
							const par =
								py[ia]! > 0 &&
								py[ib]! > 0 &&
								pv[ia]! > MOVING &&
								pv[ib]! > MOVING &&
								(pvx[ia]! * pvx[ib]! + pvy[ia]! * pvy[ib]!) /
									(pv[ia]! * pv[ib]!) >
									0.7;
							const lim = par ? ALONGSIDE : mates ? MATES : OPPS;
							const ddx = px[ia]! - px[ib]!;
							const ddy = py[ia]! - py[ib]!;
							if (ddx * ddx + ddy * ddy >= lim * lim) {
								continue;
							}
							// (In a huddle, shoulder to shoulder is where they are
							// meant to be.)
							if (
								mates &&
								huddled(A.team, px[ia]!, py[ia]!) &&
								huddled(A.team, px[ib]!, py[ib]!)
							) {
								continue;
							}
							const d = Math.sqrt(ddx * ddx + ddy * ddy);
							// (What each is doing, only now it matters.)
							const aa = actOf(ia, t);
							const ab = actOf(ib, t);
							// Into each other is what they are doing - but never
							// through each other. (Two of a side are in it together
							// only slapping hands: boxing out, going up for it, they
							// are in it with the other side - not on top of each
							// other.)
							const together =
								(aa !== undefined && TOGETHER.has(aa)) ||
								(ab !== undefined && TOGETHER.has(ab));
							const hands =
								(aa !== undefined && MATES_TOGETHER.has(aa)) ||
								(ab !== undefined && MATES_TOGETHER.has(ab));
							if (mates ? hands : together && d >= TOUCH) {
								continue;
							}
							// Who gives way.
							// (The man with the ball, only dribbling it, gives way to
							// one set in a screen or busy where he is.)
							// (Going up with a shot in close he is never moved: where
							// he lets it go from is where the ball goes from, in
							// front of the glass - see clearTheWayIn.)
							// (Nor, before anybody else, a man having words, hands on
							// his hips, flexing: he does it where he stands - not
							// sliding off across the floor in the middle of it.)
							const ground = (pid: number, act: AnimName | undefined) =>
								act !== undefined && UP_IN_CLOSE.has(act)
									? 3
									: act !== undefined && ON_BALL.has(act)
										? 2
										: act !== undefined && (HOLDS.has(act) || SET.has(act))
											? 1.5
											: pid === holder
												? 1
												: act !== undefined && REACTIONS.has(act)
													? 0.5
													: 0;
							const ga = ground(A.pid, aa);
							const gb = ground(B.pid, ab);
							// (Two of them at it, though, one gives way all the same.)
							const fixA = ga >= 1;
							// (On the move, he is only going by: a teammate's shoulder,
							// or - not in it with him - an opponent's.)
							const going = (i: number) =>
								(mates || !together) && !par && pv[i]! > MOVING;
							let im: number;
							let io: number;
							if (ga !== gb) {
								[im, io] = ga > gb ? [ib, ia] : [ia, ib];
								if (going(im)) {
									continue;
								}
							} else if (fixA) {
								continue;
							} else {
								// (Of two of a side, the one in it with the other side
								// - up to block, boxing out - holds his ground.)
								const inA = aa !== undefined && TOGETHER.has(aa);
								const inB = ab !== undefined && TOGETHER.has(ab);
								[im, io] = !mates
									? A.team === off
										? [ib, ia]
										: [ia, ib]
									: inA !== inB
										? inA
											? [ib, ia]
											: [ia, ib]
										: A.pid > B.pid
											? [ia, ib]
											: [ib, ia];
								if (going(im)) {
									if (going(io)) {
										continue;
									}
									[im, io] = [io, im];
								}
							}
							const mover = rd[im]!;
							const other = rd[io]!;
							const k2 = `${mover.tr.pid}:${other.tr.pid}`;
							let r = open.get(k2);
							if (r && (t - r.t1 > STEP * 1.5 || t - r.t0 > LONGEST)) {
								close(k2);
								r = undefined;
							}
							const ux = d > 0.05 ? (px[im]! - px[io]!) / d : 0;
							const uy = d > 0.05 ? (py[im]! - py[io]!) / d : 0;
							const o1 = at(other, t + STEP);
							if (!r) {
								r = {
									a: mover.tr,
									b: other.tr,
									t0: t,
									t1: t,
									near: d,
									ux: 0,
									uy: 0,
									n: 0,
									vx: 0,
									vy: 0,
									par: false,
									mx: 0,
									my: 0,
								};
								open.set(k2, r);
							}
							r.t1 = t;
							r.par ||= par;
							if (par) {
								r.mx += pvx[im]!;
								r.my += pvy[im]!;
							}
							r.near = Math.min(r.near, d);
							r.ux += ux;
							r.uy += uy;
							r.vx += o1.x - px[io]!;
							r.vy += o1.y - py[io]!;
							r.n += 1;
						}
					}
					// (Done with, those not still on each other.)
					for (const [key, r] of open) {
						if (r.t1 < t) {
							close(key);
						}
					}
				}
				for (const key of open.keys()) {
					close(key);
				}
			}
			if (done.length === 0) {
				break;
			}
			for (const r of done) {
				const mates = r.a.team === r.b.team;
				let ux = r.ux / r.n;
				let uy = r.uy / r.n;
				let u = Math.hypot(ux, uy);
				if (u < 0.3) {
					// Through him, or right on him: off to the side of the way
					// the other is going - or, standing, the same way every time.
					const v = Math.hypot(r.vx, r.vy);
					const s = r.a.pid > r.b.pid ? 1 : -1;
					ux = v > 0.01 ? (-r.vy / v) * s : 0;
					uy = v > 0.01 ? (r.vx / v) * s : s;
					u = 1;
				}
				if (r.par) {
					// Running together: over to the side, across the way they
					// go - never on ahead or back, which is only faster or
					// slower.
					const v = Math.hypot(r.mx, r.my);
					if (v > 0.01) {
						const qx = -r.my / v;
						const qy = r.mx / v;
						const along = ux * qx + uy * qy;
						const s =
							Math.abs(along) > 0.05 * u
								? Math.sign(along)
								: r.a.pid > r.b.pid
									? 1
									: -1;
						ux = qx * s;
						uy = qy * s;
						u = 1;
					}
				}
				const k =
					((r.par ? ALONGSIDE : mates ? MATES : OPPS) - r.near + 0.25) / u;
				// Clear of anything of his own that needs him where he is: in
				// it together with somebody, shooting, catching - or the ball
				// in his hands.
				const easeMs = r.par ? DRIFT : ASIDE;
				let n0 = r.t0 - easeMs;
				let n1 = r.t1 + STEP + easeMs;
				// (Not before he is out there: coming on, he comes on where he
				// comes on, and steps aside from there.)
				{
					const k = lastBy(r.a.shown, r.t0, (x) => x[0]);
					if (k >= 0 && r.a.shown[k]![1]) {
						n0 = Math.max(n0, r.a.shown[k]![0]);
					}
				}
				const busy = [
					...r.a.acts
						.filter(
							(a) =>
								TOGETHER.has(a.anim) ||
								HOLDS.has(a.anim) ||
								IN_PLACE.has(a.anim),
						)
						.map((a) => [a.t0, a.t1] as const),
					...(holds.get(r.a.pid) ?? []),
				];
				for (const [b0, b1] of busy) {
					if (b0 > r.t1 && b0 < n1) {
						n1 = b0;
					}
					if (b1 < r.t0 && b1 > n0) {
						n0 = b1;
					}
				}
				// In the middle of a reaction of his own while they are on each
				// other - a flex, a clap - he steps aside once he is done with it
				// (or, done before it, is back before it): not sliding over while
				// he does it. All through it, he holds where he is.
				let holdsOn = false;
				for (const a of r.a.acts) {
					if (!REACTIONS.has(a.anim) || a.t1 <= n0 || a.t0 >= n1) {
						continue;
					}
					if (a.t0 <= r.t0 && a.t1 >= r.t1) {
						holdsOn = true;
					} else if (a.t1 < r.t1) {
						n0 = Math.max(n0, a.t1);
					} else if (a.t0 > r.t0) {
						n1 = Math.min(n1, a.t0);
					}
				}
				if (holdsOn || n1 - n0 < 400) {
					continue;
				}
				// Aside to where nobody else is, either: the way away from the
				// other if that is clear, else whichever side is clearest.
				{
					const mid = (r.t0 + r.t1) / 2;
					const P = posAt(r.a, mid);
					const room = (dx: number, dy: number) => {
						const Q = { x: P.x + dx, y: P.y + dy };
						// (Not over a line, either.)
						let worst = -Math.max(0, dist(inPlay(Q), Q) - dist(inPlay(P), P));
						worst = worst < 0 ? worst - 1 : Infinity;
						for (const o of tracks) {
							if (o === r.a || !shownAtT(o, mid)) {
								continue;
							}
							const lim = o.team === r.a.team ? MATES : OPPS;
							worst = Math.min(worst, dist(posAt(o, mid), Q) - lim);
						}
						return worst;
					};
					const ways: [number, number][] = r.par
						? [
								[ux, uy],
								[-ux, -uy],
							]
						: [
								[ux, uy],
								[-uy, ux],
								[uy, -ux],
							];
					let best = ways[0]!;
					let most = room(ux * k, uy * k);
					if (most < 0) {
						for (const w of ways.slice(1)) {
							const m = room(w[0] * k, w[1] * k);
							if (m > most) {
								most = m;
								best = w;
							}
						}
					}
					[ux, uy] = best;
					// However he goes, no farther over a line than he was.
					const over = (s: number) =>
						dist(inPlay({ x: P.x + ux * k * s, y: P.y + uy * k * s }), {
							x: P.x + ux * k * s,
							y: P.y + uy * k * s,
						}) >
						dist(inPlay(P), P) + 0.01;
					if (over(1)) {
						let lo = 0;
						let hi = 1;
						for (let n = 0; n < 10; n++) {
							const m = (lo + hi) / 2;
							if (over(m)) {
								hi = m;
							} else {
								lo = m;
							}
						}
						ux *= lo;
						uy *= lo;
					}
				}
				if (Math.hypot(ux, uy) * k < 0.3) {
					continue;
				}
				// Back where he was by the time he gets a hand to the ball - a
				// poke at it, a board: where it comes to is where he is.
				const hand = r.a.acts.find(
					(a) => ON_BALL.has(a.anim) && a.t1 > n0 && a.t0 < n1 + RAMP,
				);
				if (hand) {
					if (hand.t0 <= n0 + 400) {
						continue;
					}
					n1 = Math.min(n1, hand.t0);
				}
				// Off him again as he goes on his way - and back over before
				// that takes him into anybody he would have gone clear of.
				const ramp = Math.min(easeMs, (n1 - n0) / 2);
				for (let t = r.t1 + STEP; t < n1; t += 100) {
					const u1 = Math.min(1, (n1 - t) / ramp);
					const w = u1 * u1 * (3 - 2 * u1);
					const P = posAt(r.a, t);
					const Q = { x: P.x + ux * k * w, y: P.y + uy * k * w };
					const into = tracks.some((o) => {
						if (o === r.a || !shownAtT(o, t)) {
							return false;
						}
						const O = posAt(o, t);
						const lim = o.team === r.a.team ? MATES : OPPS;
						return dist(O, Q) < lim && dist(O, Q) < dist(O, P) - 0.01;
					});
					if (into) {
						n1 = Math.max(t, n0 + 400);
						break;
					}
				}
				// (Running, he drifts over and back or not at all: no lurch -
				// one drift at a time, and none of it into anybody.)
				if (r.par) {
					if (
						n1 - n0 < 2 * easeMs ||
						r.a.nudges?.some(
							(n) => n.ramp !== undefined && n.t0 < n1 && n.t1 > n0,
						)
					) {
						continue;
					}
					let into = false;
					for (let t = n0; t < n1 && !into; t += 100) {
						const u1 = Math.min(1, (t - n0) / ramp, (n1 - t) / ramp);
						const w = u1 * u1 * (3 - 2 * u1);
						const P = posAt(r.a, t);
						const Q = { x: P.x + ux * k * w, y: P.y + uy * k * w };
						into = tracks.some((o) => {
							if (o === r.a || !shownAtT(o, t)) {
								return false;
							}
							const O = posAt(o, t);
							const lim = o.team === r.a.team ? MATES : OPPS;
							return dist(O, Q) < lim && dist(O, Q) < dist(O, P) - 0.01;
						});
					}
					if (into) {
						continue;
					}
				}
				// (Never into a man set in a screen or the post: he is a wall.)
				{
					const ramp = Math.min(r.par ? easeMs : RAMP, (n1 - n0) / 2);
					let wall = false;
					for (let t = n0; t < n1 && !wall; t += 100) {
						const u1 = Math.min(1, (t - n0) / ramp, (n1 - t) / ramp);
						const w = u1 * u1 * (3 - 2 * u1);
						const P = posAt(r.a, t);
						const Q = { x: P.x + ux * k * w, y: P.y + uy * k * w };
						wall = tracks.some((o) => {
							if (o.team === r.a.team || !shownAtT(o, t)) {
								return false;
							}
							const act = actAt(o, t);
							if (!act || !SET.has(act.anim)) {
								return false;
							}
							const O = posAt(o, t);
							return dist(O, Q) < TOUCH && dist(O, Q) < dist(O, P) - 0.01;
						});
					}
					if (wall) {
						continue;
					}
				}
				(r.a.nudges ??= []).push({
					t0: n0,
					t1: n1,
					dx: ux * k,
					dy: uy * k,
					ramp: easeMs,
				});
			}
			for (const tr of tracks) {
				tr.nudges?.sort((x, y) => x.t0 - y.t0);
			}
			looks = done
				.map((r) => [r.t0 - 1500, r.t1 + 1500] as [number, number])
				.sort((x, y) => x[0] - y[0])
				.reduce<[number, number][]>((out, w) => {
					const last = out.at(-1);
					if (last && w[0] <= last[1]) {
						last[1] = Math.max(last[1], w[1]);
					} else {
						out.push([...w]);
					}
					return out;
				}, []);
		}
	}

	// Never one man drawn in another, however everything before left them:
	// wherever two would still be in each other at real speed, the one not
	// holding his ground is kept off him - both a little, if neither is -
	// eased over a few steps before he gets there and back after, so he
	// bends his way round rather than going through.
	private keepOff(fast: [number, number][]) {
		const DT = SEP_MS;
		// (Pushed over no faster than this, feet a sample.)
		const SEP_STEP = 0.7;
		// Center to center (feet): up against each other, no nearer - and in
		// it together (a screen, a box-out, the post), chest to chest.
		const APART = 1.45;
		const TOUCH = 1.25;
		// (Going by a man set in a screen, though, it is his shoulder he
		// comes off - not through it.)
		const BY_SCREEN = 1.35;
		// (And a man in front of the one with the ball, further: the ball is
		// out in front of him, not in the other man's chest.)
		const HOLD_APART = 2.7;
		// (Two of a side running the same way abreast - both going faster
		// than this, feet a second, within this of the same way, cosine -
		// a stride or so apart: not shoulder to shoulder the length of the
		// floor.)
		const RUN_APART = 3;
		const RUN_FTPS = 8;
		const RUN_SAME = 0.85;
		const NEAREST = Math.max(HOLD_APART, RUN_APART);
		// How long before he gets to him he starts over, and after he is by
		// he is back (samples); how smoothly (samples); how many goes at it.
		const AHEAD = 6;
		const SOFT = 3;
		const ROUNDS = 8;
		// (Too short a stretch to bother with, samples.) Where they are is
		// asked every few samples.
		const EDGE = 6;
		const EVERY = 4;
		// The picture only gets up to speed gently, and is back down a moment
		// before it is over (see fastAt): kept off each other on into it, and
		// from before the end of it, this far (ms) - eased back over it, so
		// there is no hurry about it while it is still slow - and all through
		// one too short to get going.
		const INTO_FAST = 800;
		const OUT_OF_FAST = 500;
		// (How far off where he really is the schedule can have him, feet.)
		const LOOSE = 3;
		// Going faster than this (feet a second), he is on the move.
		const MOVING = 4;
		// His middle no nearer the ball's than this (feet) - and how high
		// off the floor (feet) the ball is in his way.
		const BALL_APART = 1.05;
		// A pass thrown past him, a little more: hand to hand, not from
		// where the thrower stands to where the catcher does.
		const PASS_APART = 1.7;
		const BALL_HIGH = 6.3;
		const PASS_HIGH = 7.5;
		// The man who gets it to it, this long (ms) before it is his.
		const GATHER_MS = 400;
		// Where he is, he is: the ball in his hands or coming to them, up for
		// it, at the rim, set in a screen or the post, down on the floor.
		const FIXED = new Set<AnimName>([
			"shoot",
			"setShot",
			"fade",
			"hook",
			"layup",
			"fingerRoll",
			"floater",
			"powerLayup",
			"scoop",
			"reverseLayup",
			"euroStep",
			"spinMove",
			"stepBack",
			"dunk",
			"dunk1",
			"tomahawk",
			"windmill",
			"catch",
			"pass",
			"passBounce",
			"passOverhead",
			"passPush",
			"passBouncePush",
			"passBehind",
			"triple",
			"jab",
			"shotFake",
			"follow",
			"rebound",
			"board",
			"snatch",
			"pickup",
			"poke",
			"strip",
			"block",
			"screen",
			"postUp",
			"fall",
			"takeCharge",
			"helpUp",
			"hurt",
			"hurtKnee",
			"hurtAnkle",
			"hurtHead",
			"hurtHand",
			"hurtArm",
			"kneel",
			"kneelShoot",
			"sit",
		]);
		const TOGETHER = new Set<AnimName>([
			"screen",
			"postUp",
			"fight",
			"boxOut",
			"bump",
			"reach",
			"poke",
			"strip",
			"block",
			"contest",
			"rebound",
			"board",
			"snatch",
			"pickup",
			"fall",
			"takeCharge",
			"helpUp",
		]);
		const MATES_TOGETHER = new Set<AnimName>([
			"highFive",
			"lowFive",
			"chestBump",
			"waitFive",
			"fall",
			"takeCharge",
			"helpUp",
			"hurt",
			"hurtKnee",
			"hurtAnkle",
			"hurtHead",
			"hurtHand",
			"hurtArm",
		]);
		const tracks = [...this.tracks.values()];
		const N = tracks.length;
		const live: [number, number][] = [];
		{
			const runs: [number, number][] = [];
			for (const [a, b] of [...fast].sort((x, y) => x[0] - y[0])) {
				const last = runs.at(-1);
				if (last && a <= last[1]) {
					last[1] = Math.max(last[1], b);
				} else {
					runs.push([a, b]);
				}
			}
			let from = 0;
			for (const [a, b] of runs) {
				if (b - a > INTO_FAST + OUT_OF_FAST) {
					live.push([from, a + INTO_FAST]);
					from = b - OUT_OF_FAST;
				}
			}
			live.push([from, this.T]);
		}
		const doing = (tr: Track, t: number): AnimName | undefined => {
			for (let k = lastBy(tr.acts, t, (a) => a.t0); k >= 0; k--) {
				const a = tr.acts[k]!;
				if (a.t1 > t) {
					return a.anim;
				}
				if (t - a.t0 > 4000) {
					break;
				}
			}
			return undefined;
		};
		const holderAt = (t: number): number | undefined => {
			const b = this.ball[lastBy(this.ball, t, (x) => x.t0)];
			return b?.kind === "hold" ? b.pid : undefined;
		};
		const offenseAt = (t: number): Side | undefined =>
			this.poss[lastBy(this.poss, t, (x) => x[0])]?.[1];
		// Over a few steps either side of him, as far as the most he needs
		// to be over - and smoothly.
		const R = 3 * SOFT;
		const K: number[] = [];
		for (let d = -R; d <= R; d++) {
			K.push(Math.exp(-(d * d) / (2 * SOFT * SOFT)));
		}
		const kSum = K.reduce((a, b) => a + b, 0);
		// Added into (ex, ey) round each bunch of samples he was in somebody
		// at (in order).
		const spread = (
			cx: Float64Array,
			cy: Float64Array,
			at: number[],
			ex: Float64Array,
			ey: Float64Array,
			// (How much of it he takes, sample by sample: see held.)
			pin?: Float64Array,
		) => {
			const n = cx.length;
			let k = 0;
			while (k < at.length) {
				let e = k;
				while (e + 1 < at.length && at[e + 1]! - at[e]! <= 2 * (AHEAD + R)) {
					e++;
				}
				const a = Math.max(0, at[k]! - AHEAD - R);
				const b = Math.min(n - 1, at[e]! + AHEAD + R);
				const m = b - a + 1;
				const wx = new Float64Array(m);
				const wy = new Float64Array(m);
				for (let s = a; s <= b; s++) {
					let best = 0;
					const q1 = Math.min(b, s + AHEAD);
					for (let q = Math.max(a, s - AHEAD); q <= q1; q++) {
						const v = cx[q]! * cx[q]! + cy[q]! * cy[q]!;
						if (v > best) {
							best = v;
							wx[s - a] = cx[q]!;
							wy[s - a] = cy[q]!;
						}
					}
				}
				for (let s = a; s <= b; s++) {
					let sx = 0;
					let sy = 0;
					for (let d = -R; d <= R; d++) {
						const q = s + d - a;
						if (q >= 0 && q < m) {
							sx += wx[q]! * K[d + R]!;
							sy += wy[q]! * K[d + R]!;
						}
					}
					const w = pin ? pin[s]! : 1;
					ex[s]! += (sx / kSum) * w;
					ey[s]! += (sy / kSum) * w;
				}
				k = e + 1;
			}
		};
		// Where the schedule has him, if he is out there (see mark): near
		// enough to say who might be in whom.
		const roughly = (tr: Track, t: number): Pt | undefined => {
			const si = lastBy(tr.shown, t, (x) => x[0]);
			if (si < 0 || !tr.shown[si]![1]) {
				return undefined;
			}
			const m = tr.moves[lastBy(tr.moves, t, (x) => x.t0)];
			if (!m) {
				return tr.start;
			}
			if (t >= m.t1) {
				return m.to;
			}
			// (Along it as he really runs it: on a long run he is at speed most
			// of the way, not easing the whole length of it.)
			const e = runProgress(m.t1 - m.t0, (t - m.t0) / (m.t1 - m.t0));
			return {
				x: m.from.x + (m.to.x - m.from.x) * e,
				y: m.from.y + (m.to.y - m.from.y) * e,
			};
		};
		// Where the ball is at t, if it is out of a man's hands - loose, or
		// thrown - and low enough to be in a man's way: who gets it next,
		// and who let it go.
		const nextHolder = new Map<number, { pid: number; t: number }>();
		const handedFrom = new Map<number, Pt>();
		const thrown = new Map<
			number,
			{ A: Pt3; B: Pt3; high: number; apart: number }
		>();
		const looseAt = (
			t: number,
		):
			| { x: number; y: number; next: number; by: number; apart: number }
			| undefined => {
			const k = lastBy(this.ball, t, (x) => x.t0);
			const seg = this.ball[k];
			if (!seg || seg.kind === "hold") {
				return undefined;
			}
			let got = nextHolder.get(k);
			if (got === undefined) {
				got = { pid: -1, t: Infinity };
				for (let q = k + 1; q < this.ball.length; q++) {
					const b = this.ball[q]!;
					if (b.kind === "hold") {
						got = { pid: b.pid, t: b.t0 };
						break;
					}
				}
				nextHolder.set(k, got);
			}
			// (The man who gets it is only in its way as he gathers it.)
			const next = got.t - t < GATHER_MS ? got.pid : -1;
			if (seg.kind === "rest") {
				return { x: seg.at.x, y: seg.at.y, next, by: -1, apart: BALL_APART };
			}
			if (seg.kind === "path") {
				const q = playAt(seg.pts, Math.min(t, seg.t1) - seg.t0);
				return q.z < BALL_HIGH
					? { x: q.x, y: q.y, next, by: -1, apart: BALL_APART }
					: undefined;
			}
			if (seg.kind === "fly") {
				let ends = thrown.get(k);
				if (!ends) {
					// Out of his hands at about his waist - higher, an overhead
					// pass - up and down under gravity (see evalBall).
					const tr = "pid" in seg.from ? this.track(seg.from.pid) : undefined;
					const over = tr?.acts.some(
						(a) =>
							a.anim === "passOverhead" && a.t0 <= seg.t0 && a.t1 >= seg.t0,
					);
					const A =
						"pid" in seg.from
							? {
									...((tr && floorSpotOf(tr, seg.t0)) ??
										this.posAt(seg.from.pid, seg.t0)),
									z: over ? 5.5 : 4.2,
								}
							: seg.from;
					// (Thrown to a man: into his hands, about as high - and in
					// the way of anybody up to the top of his head.)
					const to = seg.to;
					const rt = "pid" in to ? this.track(to.pid) : undefined;
					const B =
						"pid" in to
							? {
									...((rt && floorSpotOf(rt, seg.t1)) ??
										this.posAt(to.pid, seg.t1)),
									z: 4.3,
								}
							: to;
					ends =
						"pid" in to
							? { A, B, high: PASS_HIGH, apart: PASS_APART }
							: { A, B, high: BALL_HIGH, apart: BALL_APART };
					thrown.set(k, ends);
				}
				const { A, B } = ends;
				const T = Math.max(0.001, (seg.t1 - seg.t0) / 1000);
				const tau = Math.min(T, Math.max(0, (t - seg.t0) / 1000));
				const vz = (B.z - A.z) / T + 0.5 * GRAVITY * T;
				const z = A.z + vz * tau - 0.5 * GRAVITY * tau * tau;
				if (z >= ends.high) {
					return undefined;
				}
				const u = tau / T;
				return {
					x: A.x + (B.x - A.x) * u,
					y: A.y + (B.y - A.y) * u,
					next,
					by: "pid" in seg.from ? seg.from.pid : -1,
					apart: ends.apart,
				};
			}
			// (Out of a man's hands, it drops from where he had it.)
			let from = handedFrom.get(k);
			if (!from) {
				const prev = this.ball[k - 1];
				const tr = prev?.kind === "hold" ? this.track(prev.pid) : undefined;
				from = (tr && floorSpotOf(tr, seg.t0)) ?? seg.from;
				handedFrom.set(k, from);
			}
			const u = Math.min(1, Math.max(0, (t - seg.t0) / (seg.t1 - seg.t0)));
			const roll = 1 - (1 - Math.min(1, u / 0.85)) ** 1.5;
			return {
				x: from.x + (seg.to.x - from.x) * roll,
				y: from.y + (seg.to.y - from.y) * roll,
				next,
				by: -1,
				apart: BALL_APART,
			};
		};
		const catmull = (
			p0: number,
			p1: number,
			p2: number,
			p3: number,
			u: number,
		) =>
			p1 +
			u *
				(0.5 * (p2 - p0) +
					u *
						(p0 -
							2.5 * p1 +
							2 * p2 -
							0.5 * p3 +
							u * (0.5 * (p3 - p0) + 1.5 * (p1 - p2))));
		// Taking it off a rim or the floor, his hands are where it comes down
		// - nobody eases him off that: whoever he would be in gives way, and
		// what he was kept off before is eased away by then.
		const CATCH_PRE = 150;
		const CATCH_POST = 80;
		const CATCH_RAMP = 400;
		// (So too a hand that knocks it away - a poke at it, a block: where
		// it is when it changes course.)
		const KNOCKS = new Set<AnimName>(["poke", "strip", "block"]);
		const catches = new Map<number, number[]>();
		const pinAt = (pid: number, t: number) => {
			const list = catches.get(pid) ?? [];
			list.push(t);
			catches.set(pid, list);
		};
		this.ball.forEach((b, k) => {
			if (b.kind !== "fly") {
				return;
			}
			if ("pid" in b.to) {
				if (!("pid" in b.from)) {
					pinAt(b.to.pid, b.t1);
				}
				return;
			}
			const next = this.ball[k + 1];
			if (!next || next.t0 > b.t1 + 1) {
				return;
			}
			for (const tr of tracks) {
				const a = tr.acts[lastBy(tr.acts, b.t1, (x) => x.t0)];
				if (a && a.t1 >= b.t1 && KNOCKS.has(a.anim)) {
					pinAt(tr.pid, b.t1);
				}
			}
		});
		const out = new Map<Track, Sep[]>();
		for (const [l0, l1] of live) {
			const n = Math.floor((l1 - l0) / DT) + 1;
			if (n < 2 * EDGE) {
				continue;
			}
			// The ball loose and low - bouncing, rolling, lying there, down
			// through the net - and who gets to it next.
			const lx = new Float64Array(n);
			const ly = new Float64Array(n);
			const lon = new Uint8Array(n);
			const lfor = new Float64Array(n).fill(-1);
			const lby = new Float64Array(n).fill(-1);
			const lap = new Float64Array(n);
			for (let s = 0; s < n; s++) {
				const B = looseAt(l0 + s * DT);
				if (B) {
					lx[s] = B.x;
					ly[s] = B.y;
					lon[s] = 1;
					lfor[s] = B.next;
					lby[s] = B.by;
					lap[s] = B.apart;
				}
			}
			// Who could be anywhere near anybody, and when: where the schedule
			// has them (near enough), every few samples.
			const xs = tracks.map(() => new Float64Array(n));
			const ys = tracks.map(() => new Float64Array(n));
			const on = tracks.map(() => new Uint8Array(n));
			const need = tracks.map(() => new Uint8Array(n));
			{
				const near: (Pt | undefined)[] = [];
				for (let s = 0; s < n; s += EVERY) {
					const t = l0 + s * DT;
					for (let i = 0; i < N; i++) {
						near[i] = roughly(tracks[i]!, t);
					}
					for (let i = 0; i < N; i++) {
						const P = near[i];
						if (!P) {
							continue;
						}
						// (The ball each sample in between: thrown, it is across
						// a man's way and gone between one asking and the next.)
						for (let k = s; k < Math.min(n, s + EVERY); k++) {
							if (
								lon[k] &&
								Math.hypot(P.x - lx[k]!, P.y - ly[k]!) < lap[k]! + LOOSE
							) {
								need[i]!.fill(
									1,
									Math.max(0, s - 2 * EVERY),
									Math.min(n - 1, s + 3 * EVERY) + 1,
								);
								break;
							}
						}
						for (let j = i + 1; j < N; j++) {
							const Q = near[j];
							if (
								!Q ||
								Math.abs(P.x - Q.x) > APART + LOOSE ||
								Math.abs(P.y - Q.y) > APART + LOOSE ||
								dist(P, Q) > APART + LOOSE
							) {
								continue;
							}
							const a = Math.max(0, s - 2 * EVERY);
							const b = Math.min(n - 1, s + 2 * EVERY);
							need[i]!.fill(1, a, b + 1);
							need[j]!.fill(1, a, b + 1);
						}
					}
				}
			}
			// Where those are, exactly: every few samples asked, the ones
			// between run through those (Catmull-Rom) - where he is on the
			// floor all through; asked, at each where he goes on or off it.
			for (let i = 0; i < N; i++) {
				const nd = need[i]!;
				if (!nd.includes(1)) {
					continue;
				}
				const x = xs[i]!;
				const y = ys[i]!;
				const o = on[i]!;
				const asked = new Uint8Array(n);
				const ask = (s: number) => {
					asked[s] = 1;
					const p = floorSpotOf(tracks[i]!, l0 + s * DT);
					if (p) {
						x[s] = p.x;
						y[s] = p.y;
						o[s] = 1;
					}
				};
				for (let s0 = 0; s0 < n - 1; s0 += EVERY) {
					const s1 = Math.min(n - 1, s0 + EVERY);
					let any = false;
					for (let s = s0; s <= s1 && !any; s++) {
						any = nd[s] === 1;
					}
					if (!any) {
						continue;
					}
					if (!asked[s0]) {
						ask(s0);
					}
					ask(s1);
					if (!o[s0] || !o[s1]) {
						for (let s = s0 + 1; s < s1; s++) {
							ask(s);
						}
						continue;
					}
					const sp = s0 - EVERY >= 0 && o[s0 - EVERY] ? s0 - EVERY : s0;
					let sn = s1;
					if (s1 + EVERY < n) {
						if (!asked[s1 + EVERY]) {
							ask(s1 + EVERY);
						}
						if (o[s1 + EVERY]) {
							sn = s1 + EVERY;
						}
					}
					for (let s = s0 + 1; s < s1; s++) {
						const u = (s - s0) / (s1 - s0);
						x[s] = catmull(x[sp]!, x[s0]!, x[s1]!, x[sn]!, u);
						y[s] = catmull(y[sp]!, y[s0]!, y[s1]!, y[sn]!, u);
						o[s] = 1;
					}
				}
			}
			// Who is out there, sample by sample.
			const floor: number[][] = [];
			for (let s = 0; s < n; s++) {
				const list: number[] = [];
				for (let i = 0; i < N; i++) {
					if (on[i]![s]) {
						list.push(i);
					}
				}
				floor.push(list);
			}
			// What each is doing, asked only of men in each other.
			const acts = new Map<number, AnimName | undefined>();
			const actOf = (i: number, s: number) => {
				const key = i * n + s;
				if (!acts.has(key)) {
					acts.set(key, doing(tracks[i]!, l0 + s * DT));
				}
				return acts.get(key);
			};
			// Two going the same way flat out, the one beside the other rather
			// than on his heels.
			const runAbreast = (
				i: number,
				j: number,
				s: number,
				ddx: number,
				ddy: number,
				d: number,
			) => {
				const p0 = Math.max(0, s - 1);
				const p1 = Math.min(n - 1, s + 1);
				if (!on[i]![p0] || !on[i]![p1] || !on[j]![p0] || !on[j]![p1]) {
					return false;
				}
				const k = 1000 / ((p1 - p0) * DT || 1);
				const ax = (xs[i]![p1]! - xs[i]![p0]!) * k;
				const ay = (ys[i]![p1]! - ys[i]![p0]!) * k;
				const bx = (xs[j]![p1]! - xs[j]![p0]!) * k;
				const by = (ys[j]![p1]! - ys[j]![p0]!) * k;
				const va = Math.hypot(ax, ay);
				const vb = Math.hypot(bx, by);
				if (va < RUN_FTPS || vb < RUN_FTPS) {
					return false;
				}
				if ((ax * bx + ay * by) / (va * vb) < RUN_SAME) {
					return false;
				}
				const wx = ax / va + bx / vb;
				const wy = ay / va + by / vb;
				const w = Math.hypot(wx, wy) || 1;
				return Math.abs(ddx * wx + ddy * wy) / (w * (d || 1)) < 0.7;
			};
			const ox = tracks.map(() => new Float64Array(n));
			const oy = tracks.map(() => new Float64Array(n));
			const moved = new Set<number>();
			const held = tracks.map((tr) => {
				const list = catches.get(tr.pid);
				if (!list?.some((t) => t > l0 - CATCH_POST && t < l1 + CATCH_PRE)) {
					return undefined;
				}
				const w = new Float64Array(n).fill(1);
				for (const tc of list) {
					const a = Math.max(
						0,
						Math.floor((tc - CATCH_PRE - CATCH_RAMP - l0) / DT),
					);
					const b = Math.min(
						n - 1,
						Math.ceil((tc + CATCH_POST + CATCH_RAMP - l0) / DT),
					);
					for (let k = a; k <= b; k++) {
						const t = l0 + k * DT;
						const off = Math.max(0, tc - CATCH_PRE - t, t - tc - CATCH_POST);
						const u = Math.min(1, off / CATCH_RAMP);
						w[k] = Math.min(w[k]!, u * u * (3 - 2 * u));
					}
				}
				return w;
			});
			// (And, last, one more for a man set in a screen or the post
			// alone: kept off him in the end, whoever else he was kept off.)
			for (let round = 0; round <= ROUNDS; round++) {
				const walls = round === ROUNDS;
				const cx = new Map<number, Float64Array>();
				const cy = new Map<number, Float64Array>();
				const cat = new Map<number, number[]>();
				const push = (
					i: number,
					s: number,
					x: number,
					y: number,
					// (Out of the way of a ball going by him, straight off it.)
					ball = false,
				) => {
					let ax = cx.get(i);
					let ay = cy.get(i);
					let at = cat.get(i);
					if (!ax || !ay || !at) {
						ax = new Float64Array(n);
						ay = new Float64Array(n);
						at = [];
						cx.set(i, ax);
						cy.set(i, ay);
						cat.set(i, at);
					}
					if (at.at(-1) !== s) {
						at.push(s);
					}
					// On the move, he goes round him - over across the way he
					// is going, never on ahead or back (which would only have
					// him going faster or slower than his legs).
					const p = Math.max(0, s - 1);
					const q = Math.min(n - 1, s + 1);
					const vx = xs[i]![q]! - xs[i]![p]!;
					const vy = ys[i]![q]! - ys[i]![p]!;
					const v = Math.hypot(vx, vy);
					if (!ball && v > (MOVING * (q - p) * DT) / 1000) {
						const cx = -vy / v;
						const cy = vx / v;
						const along = x * cx + y * cy;
						let sg = Math.abs(along) > 0.01 ? Math.sign(along) : i % 2 ? 1 : -1;
						const k = Math.hypot(x, y);
						// (Not over into a man of theirs set in a screen, though,
						// if the other way round is clear of him.)
						const into = (g: number) => {
							const px = xs[i]![s]! + ox[i]![s]! + cx * k * g;
							const py = ys[i]![s]! + oy[i]![s]! + cy * k * g;
							return floor[s]!.some(
								(j) =>
									tracks[j]!.team !== tracks[i]!.team &&
									actOf(j, s) === "screen" &&
									Math.hypot(
										xs[j]![s]! + ox[j]![s]! - px,
										ys[j]![s]! + oy[j]![s]! - py,
									) < BY_SCREEN,
							);
						};
						if (into(sg) && !into(-sg)) {
							sg = -sg;
						}
						x = cx * k * sg;
						y = cy * k * sg;
					}
					ax[s]! += x;
					ay[s]! += y;
				};
				for (let s = 0; s < n; s++) {
					const t = l0 + s * DT;
					let holder: number | null | undefined = null;
					let off: Side | undefined | null = null;
					const L = floor[s]!;
					// Nobody stands in the ball, either (but the man who gets
					// to it next, or anybody playing it).
					if (lon[s] && !walls) {
						for (const i of L) {
							if (
								tracks[i]!.pid === lfor[s] ||
								tracks[i]!.pid === lby[s] ||
								(round > 0 && ox[i]![s] === 0 && oy[i]![s] === 0)
							) {
								continue;
							}
							const ddx = xs[i]![s]! + ox[i]![s]! - lx[s]!;
							const ddy = ys[i]![s]! + oy[i]![s]! - ly[s]!;
							const d = Math.hypot(ddx, ddy);
							if (d >= lap[s]!) {
								continue;
							}
							const act = actOf(i, s);
							if (act !== undefined && (FIXED.has(act) || TOGETHER.has(act))) {
								continue;
							}
							const sg = tracks[i]!.pid % 2 ? 1 : -1;
							const ux = d > 0.05 ? ddx / d : 0;
							const uy = d > 0.05 ? ddy / d : sg;
							const short = lap[s]! - d + 0.05;
							push(i, s, ux * short, uy * short, true);
						}
					}
					for (let a = 0; a < L.length; a++) {
						const i = L[a]!;
						const xi = xs[i]![s]! + ox[i]![s]!;
						const yi = ys[i]![s]! + oy[i]![s]!;
						const still = ox[i]![s] === 0 && oy[i]![s] === 0;
						for (let b = a + 1; b < L.length; b++) {
							const j = L[b]!;
							// (Neither kept off anybody yet, two found clear the
							// first time through still are.)
							if (round > 0 && still && ox[j]![s] === 0 && oy[j]![s] === 0) {
								continue;
							}
							const ddx = xi - (xs[j]![s]! + ox[j]![s]!);
							const ddy = yi - (ys[j]![s]! + oy[j]![s]!);
							if (Math.abs(ddx) >= NEAREST || Math.abs(ddy) >= NEAREST) {
								continue;
							}
							const d = Math.hypot(ddx, ddy);
							if (d >= NEAREST) {
								continue;
							}
							const A = tracks[i]!;
							const B = tracks[j]!;
							const mates = A.team === B.team;
							const abreast =
								mates && d < RUN_APART && runAbreast(i, j, s, ddx, ddy, d);
							// (A man in front of the one with the ball - the ball out
							// there between them - is kept further off: the more
							// squarely in front of him, the further.)
							let inFront = 0;
							if (!mates) {
								if (holder === null) {
									holder = holderAt(t);
									off = offenseAt(t);
								}
								const ih = A.pid === holder ? 1 : B.pid === holder ? -1 : 0;
								if (ih !== 0) {
									const H = ih === 1 ? A : B;
									const hx = ih === 1 ? xi : xi - ddx;
									const hy = ih === 1 ? yi : yi - ddy;
									const rx = rimX(H.team) - hx;
									const ry = COURT_H / 2 - hy;
									const cos =
										(-ih * ddx * rx - ih * ddy * ry) /
										(d * (Math.hypot(rx, ry) || 1));
									// (Set in front of him, not running at him: one on the
									// move is only kept off him across the way he goes.)
									const o = ih === 1 ? j : i;
									const p0 = Math.max(0, s - 1);
									const p1 = Math.min(n - 1, s + 1);
									const v =
										Math.hypot(
											xs[o]![p1]! - xs[o]![p0]!,
											ys[o]![p1]! - ys[o]![p0]!,
										) / (((p1 - p0) * DT) / 1000 || 1);
									inFront =
										v > MOVING
											? 0
											: Math.min(1, Math.max(0, (cos - 0.2) / 0.5));
								}
							}
							if (
								d >=
								(abreast ? RUN_APART : APART + (HOLD_APART - APART) * inFront)
							) {
								continue;
							}
							const aa = actOf(i, s);
							const ab = actOf(j, s);
							if (
								walls &&
								(mates ||
									(aa !== "screen" &&
										ab !== "screen" &&
										aa !== "postUp" &&
										ab !== "postUp"))
							) {
								continue;
							}
							if (
								mates &&
								((aa !== undefined && MATES_TOGETHER.has(aa)) ||
									(ab !== undefined && MATES_TOGETHER.has(ab)) ||
									(Math.hypot(xi - benchX(A.team), yi - HUDDLE_Y) < 4.5 &&
										Math.hypot(xi - ddx - benchX(A.team), yi - ddy - HUDDLE_Y) <
											4.5))
							) {
								continue;
							}
							const lim = mates
								? abreast
									? RUN_APART
									: APART
								: aa === "screen" || ab === "screen"
									? BY_SCREEN
									: (aa !== undefined && TOGETHER.has(aa)) ||
										  (ab !== undefined && TOGETHER.has(ab))
										? TOUCH
										: APART + (HOLD_APART - APART) * inFront;
							if (d >= lim) {
								continue;
							}
							// Who gives way, and how much.
							if (holder === null) {
								holder = holderAt(t);
								off = offenseAt(t);
							}
							// (A man having words, hands on his hips, a flex: where he
							// stands to do it, he mostly stays - see keepApart.)
							const give = (tr: Track, act: AnimName | undefined, k: number) =>
								tr.pid === holder ||
								(act !== undefined && FIXED.has(act)) ||
								(held[k]?.[s] ?? 1) < 1
									? 0
									: act !== undefined && REACTIONS.has(act)
										? 0.15
										: act === "contest"
											? 0.3
											: tr.team === off
												? 0.5
												: 1;
							const ga = give(A, aa, i);
							const gb = give(B, ab, j);
							if (ga + gb === 0) {
								continue;
							}
							let ux: number;
							let uy: number;
							if (d > 0.05) {
								ux = ddx / d;
								uy = ddy / d;
							} else {
								// Right on him: off to the side of the way they pass,
								// the same way every time.
								const p = Math.max(0, s - 1);
								const q = Math.min(n - 1, s + 1);
								const rx = xs[i]![q]! - xs[i]![p]! - (xs[j]![q]! - xs[j]![p]!);
								const ry = ys[i]![q]! - ys[i]![p]! - (ys[j]![q]! - ys[j]![p]!);
								const r = Math.hypot(rx, ry);
								const sg = A.pid > B.pid ? 1 : -1;
								ux = r > 0.01 ? (-ry / r) * sg : 0;
								uy = r > 0.01 ? (rx / r) * sg : sg;
							}
							const short = lim - d + 0.05;
							const fa = ga / (ga + gb);
							if (fa > 0) {
								push(i, s, ux * short * fa, uy * short * fa);
							}
							if (fa < 1) {
								push(j, s, -ux * short * (1 - fa), -uy * short * (1 - fa));
							}
						}
					}
				}
				if (cx.size === 0) {
					break;
				}
				for (const [i, ax] of cx) {
					spread(ax, cy.get(i)!, cat.get(i)!, ox[i]!, oy[i]!, held[i]);
					moved.add(i);
				}
			}
			// Back to where he was by where the picture hurries on.
			for (const i of moved) {
				const ex = ox[i]!;
				const ey = oy[i]!;
				for (let s = 0; s < n; s++) {
					const a = l0 > 0 ? (s * DT) / OUT_OF_FAST : 1;
					const b = l1 < this.T ? ((n - 1 - s) * DT) / INTO_FAST : 1;
					const u = Math.min(1, a, b);
					if (u < 1) {
						const w = u * u * (3 - 2 * u);
						ex[s]! *= w;
						ey[s]! *= w;
					}
				}
				// Doing something where he stands - having words, hands on his
				// hips, a flex - he is not slid over in the middle of it: kept
				// off as he was as he started it, and eased on from there once
				// he is done.
				for (const a of tracks[i]!.acts) {
					if (!REACTIONS.has(a.anim) || a.t1 <= l0 || a.t0 >= l1) {
						continue;
					}
					const s0 = Math.max(0, Math.ceil((a.t0 - l0) / DT));
					const s1 = Math.min(n - 1, Math.floor((a.t1 - l0) / DT));
					if (s1 <= s0) {
						continue;
					}
					const fx = ex[s0]!;
					const fy = ey[s0]!;
					for (let s = s0 + 1; s <= s1; s++) {
						ex[s] = fx;
						ey[s] = fy;
					}
					const E = 8;
					for (let s = s1 + 1; s <= Math.min(n - 1, s1 + E); s++) {
						const u = (s - s1) / E;
						const w = u * u * (3 - 2 * u);
						ex[s] = fx + (ex[s]! - fx) * w;
						ey[s] = fy + (ey[s]! - fy) * w;
					}
				}
				// Never pushed over faster than a quick step aside, however
				// fast whoever he is kept off comes at him: each way through
				// it in turn, so he gives a little sooner and comes back a
				// little later rather than jumping.
				for (let pass = 0; pass < 2; pass++) {
					for (let k = 1; k < n; k++) {
						const s = pass === 0 ? k : n - 1 - k;
						const r = pass === 0 ? s - 1 : s + 1;
						const ddx = ex[s]! - ex[r]!;
						const ddy = ey[s]! - ey[r]!;
						const d = Math.hypot(ddx, ddy);
						if (d > SEP_STEP) {
							ex[s] = ex[r]! + (ddx * SEP_STEP) / d;
							ey[s] = ey[r]! + (ddy * SEP_STEP) / d;
						}
					}
				}
				// Kept as the stretches he is over at all, from and back to
				// where he was.
				const list = out.get(tracks[i]!) ?? [];
				let s = 0;
				while (s < n) {
					if (Math.abs(ex[s]!) < 0.005 && Math.abs(ey[s]!) < 0.005) {
						s++;
						continue;
					}
					const a = Math.max(0, s - 1);
					let b = s;
					let quiet = 0;
					while (b + 1 < n && quiet < 2 * AHEAD) {
						b++;
						quiet =
							Math.abs(ex[b]!) < 0.005 && Math.abs(ey[b]!) < 0.005
								? quiet + 1
								: 0;
					}
					const dx: number[] = [];
					const dy: number[] = [];
					const dl: number[] = [];
					let len = 0;
					for (let q = a; q <= b; q++) {
						const x = Math.round(ex[q]! * 1000) / 1000;
						const y = Math.round(ey[q]! * 1000) / 1000;
						if (dx.length > 0) {
							len += Math.hypot(x - dx.at(-1)!, y - dy.at(-1)!);
						}
						dx.push(x);
						dy.push(y);
						dl.push(Math.round(len * 1000) / 1000);
					}
					// (Ending back where he was.)
					dx.push(0);
					dy.push(0);
					len += Math.hypot(dx.at(-2)!, dy.at(-2)!);
					dl.push(Math.round(len * 1000) / 1000);
					list.push({ t0: l0 + a * DT, dx, dy, dl });
					s = b + 1;
				}
				out.set(tracks[i]!, list);
			}
		}
		for (const [tr, list] of out) {
			if (list.length > 0) {
				tr.sep = list.sort((a, b) => a.t0 - b.t0);
			}
		}
	}

	// BACKING OUT, FACING THE PLAY. However his runs have been reworked since
	// they were laid out (staggered, joined up, kept clear of others), a
	// backpedal of his own - not one facing a man - that would now have him
	// backing toward the rim he is backing out from, it behind his shoulder,
	// he turns and jogs instead.
	private backOutFacing() {
		for (const tr of this.tracks.values()) {
			for (const m of tr.moves) {
				if (m.anim !== "back" || m.face !== undefined) {
					continue;
				}
				const gx = m.to.x - m.from.x;
				const gy = m.to.y - m.from.y;
				if (Math.hypot(gx, gy) < 2) {
					continue;
				}
				const rim = {
					x: m.from.x < COURT_W / 2 ? RIM_INSET : COURT_W - RIM_INSET,
					y: COURT_H / 2,
				};
				if (-gx * (rim.x - m.from.x) - gy * (rim.y - m.from.y) < 0) {
					m.anim = "jog";
				}
			}
		}
	}

	// Every run of his sets off from where the last one left him. Whatever
	// reworked the one (following his man, say) but not the next, he gets
	// over there in the time between - or, a hair off, or with no time to
	// spare, the next sets off from where he is.
	private joinUp() {
		for (const tr of this.tracks.values()) {
			const moves = tr.moves;
			for (let k = 1; k < moves.length; k++) {
				const a = moves[k - 1]!;
				const b = moves[k]!;
				const d = dist(a.to, b.from);
				if (
					d < 0.05 ||
					this.cuts.some((c) => c >= a.t1 - 1 && c <= b.t0 + 1) ||
					tr.shown.some(([t]) => t > a.t1 - 1 && t <= b.t0 + 1)
				) {
					continue;
				}
				const need = Math.max(250, (d / 6) * 1000);
				if (d >= 0.3 && b.t0 - a.t1 >= need) {
					moves.splice(k, 0, {
						t0: b.t0 - need,
						t1: b.t0,
						from: { ...a.to },
						to: { ...b.from },
						anim: d < 3 ? "shuffle" : "run",
					});
					k++;
				} else {
					b.from = { ...a.to };
				}
			}
		}
	}

	// How much of a play's lead-in is shown at real speed (ms): for a shot,
	// from just before the pass that found him - however late that came in
	// the play - within limits.
	private liveLead(b: Beat): number {
		const pid = this.events[b.i]?.pid;
		if (!/^fga/.test(b.type) || typeof pid !== "number") {
			return LIVE_LEAD;
		}
		let thrown: { t0: number; t1: number; from: number } | undefined;
		for (let k = this.ball.length - 1; k >= 0; k--) {
			const s = this.ball[k]!;
			if (s.t0 < b.actionStart - LIVE_MAX) {
				break;
			}
			if (
				s.kind === "fly" &&
				s.t1 <= b.actionStart &&
				"pid" in s.to &&
				s.to.pid === pid &&
				"pid" in s.from
			) {
				thrown = { t0: s.t0, t1: s.t1, from: s.from.pid };
				break;
			}
		}
		if (thrown !== undefined) {
			const lead = Math.max(
				LIVE_MIN,
				Math.min(LIVE_MAX, b.actionStart - thrown.t0 + 600),
			);
			// From the start of what got him it: his cut to it, the drive
			// that drew the help off him - not the last stride of either.
			const pass = thrown;
			const long = (m: Move, l: number) => dist(m.from, m.to) >= l;
			const cut = this.track(pid)?.moves.find(
				(m) => m.t1 <= pass.t1 + 300 && m.t1 > pass.t1 - 1500 && long(m, 6),
			);
			const drive = this.track(pass.from)?.moves.find(
				(m) =>
					m.anim === "dribble" &&
					m.t1 <= pass.t0 + 300 &&
					m.t1 > pass.t0 - 1500 &&
					long(m, 8),
			);
			const from = Math.min(cut?.t0 ?? Infinity, drive?.t0 ?? Infinity);
			return Number.isFinite(from)
				? Math.max(lead, Math.min(LIVE_ACTION, b.actionStart - from + 300))
				: lead;
		}
		// Off the dribble, off a screen: from the screen being set - what he
		// is going off, and why he goes where he goes - not just the last
		// strides of it.
		const team = this.teamOf(pid);
		let set: number | undefined;
		for (const tr of this.tracks.values()) {
			if (tr.pid === pid || tr.team !== team) {
				continue;
			}
			for (const a of tr.acts) {
				if (
					a.anim === "screen" &&
					a.t0 <= b.actionStart - LIVE_LEAD + SCREEN_SEEN &&
					a.t0 >= b.actionStart - LIVE_SCREEN + SCREEN_SEEN &&
					(set === undefined || a.t0 > set) &&
					dist(this.posAt(tr.pid, a.t0 + 300), this.posAt(pid, a.t0 + 300)) <
						SCREEN_NEAR
				) {
					set = a.t0;
				}
			}
		}
		return set === undefined ? LIVE_LEAD : b.actionStart - set + SCREEN_SEEN;
	}

	// NOBODY GOES ON THE SAME COUNT. Three or more setting off from standing
	// in the same instant - five running back the moment the ball changes
	// hands, the whole floor moving as a shot goes up - is a drill, not a
	// game: each sees it and goes in his own time. All but the first of them
	// are a beat later getting going - each his own beat - and there that
	// much later, if nothing waits on him; if something does (the ball, a
	// screen, his next run), there just the same, a touch quicker.
	private stagger() {
		const WINDOW = 120;
		// When the ball gets to each man.
		const gets = new Map<number, number[]>();
		for (const b of this.ball) {
			const [pid, t] =
				b.kind === "hold"
					? [b.pid, b.t0]
					: b.kind === "fly" && "pid" in b.to
						? [b.to.pid, b.t1]
						: [undefined, 0];
			if (pid !== undefined) {
				const list = gets.get(pid) ?? [];
				list.push(t);
				gets.set(pid, list);
			}
		}
		// (Not lining up for something with the ball dead: the tip, a free
		// throw, a sub, a timeout.)
		const set = this.beats
			.filter((b) => DEAD_SETUPS.test(b.type))
			.map((b) => [b.preStart - 300, b.end] as const);
		const starts: { tr: Track; k: number }[] = [];
		for (const tr of this.tracks.values()) {
			tr.moves.forEach((m, k) => {
				const prev = tr.moves[k - 1];
				if (
					m.t1 - m.t0 < 450 ||
					(m.v0 ?? 0) > 0 ||
					dist(m.from, m.to) < 2 ||
					(prev !== undefined && m.t0 - prev.t1 < 250) ||
					set.some(([a, b]) => m.t1 > a && m.t0 < b)
				) {
					return;
				}
				starts.push({ tr, k });
			});
		}
		const t0Of = (x: { tr: Track; k: number }) => x.tr.moves[x.k]!.t0;
		starts.sort((a, b) => t0Of(a) - t0Of(b));
		for (let i = 0; i < starts.length;) {
			let n = 1;
			while (
				i + n < starts.length &&
				t0Of(starts[i + n]!) - t0Of(starts[i]!) < WINDOW
			) {
				n++;
			}
			if (n >= 3) {
				for (const { tr, k } of starts.slice(i + 1, i + n)) {
					const m = tr.moves[k]!;
					const late = 90 + 230 * hash01(tr.pid, m.t0);
					const next = tr.moves[k + 1];
					let room =
						(m.v1 ?? 0) > 0 || (next !== undefined && next.t0 - m.t1 < 30)
							? 0
							: next
								? next.t0 - m.t1 - 30
								: Infinity;
					// (Not into anything he does as he gets there - nor out from
					// under something timed to his getting there, a poke at the
					// ball, a catch.)
					for (const a of tr.acts) {
						if (a.t1 > m.t1 - 50 && a.t0 < m.t1 + late) {
							room = Math.min(room, Math.max(0, a.t0 - m.t1));
						}
					}
					if (
						(gets.get(tr.pid) ?? []).some(
							(c) => c >= m.t0 && c <= m.t1 + late + 200,
						)
					) {
						room = 0;
					}
					const shift = Math.max(0, Math.min(late, room));
					m.t0 += shift;
					m.t1 += shift;
					// (Only where it is no hurry: well inside how quickly he could
					// cover it.)
					const quickest = runMs(dist(m.from, m.to), SPRINT, 0, 1, m.v1 ?? 0);
					m.t0 += Math.max(
						0,
						Math.min(
							late - shift,
							(m.t1 - m.t0) * 0.2,
							m.t1 - m.t0 - quickest * 1.4,
						),
					);
				}
			}
			i += n;
		}
	}

	// A shot fake: whoever is up on the man with it - where everybody really
	// is, once all their runs are worked out - comes up out of his stance
	// for it: off his feet, if he bites (and is not on the move); a hand up,
	// if he does not.
	// Going up in close, the shooter has whoever of theirs is right there -
	// the help stepped in, the big at the rim, the man who crashed in under
	// it - going straight up with him, arms high: not stood there in his
	// slide while the shooter goes up through him. (Here, once everybody's
	// way there is laid out.)
	private wallUp() {
		// Every shot in close, however it came about - a putback, a layup off
		// a steal - not just those planned as one: whatever close finish the
		// ball leaves his hands in, once it is all laid out (and when it
		// really leaves them).
		const walls: { shooter: number; release: number }[] = [];
		for (const b of this.ball) {
			if (b.kind !== "fly" || !("pid" in b.from)) {
				continue;
			}
			const shooter = b.from.pid;
			const planned = this.walls.some(
				(w) =>
					w.shooter === shooter &&
					b.t0 >= w.release - 300 &&
					b.t0 <= w.release + 1500,
			);
			const finish = this.track(shooter)?.acts.some(
				(a) => CLOSE_FINISHES.has(a.anim) && a.t0 <= b.t0 && a.t1 >= b.t0,
			);
			if (planned || finish) {
				walls.push({ shooter, release: b.t0 });
			}
		}
		for (const w of this.walls) {
			if (
				!walls.some(
					(x) =>
						x.shooter === w.shooter &&
						x.release >= w.release - 300 &&
						x.release <= w.release + 1500,
				)
			) {
				walls.push(w);
			}
		}
		for (const { shooter, release } of walls) {
			const team = this.teamOf(shooter);
			const S = this.posAt(shooter, release, false, true);
			for (const tr of this.tracks.values()) {
				if (
					tr.team === team ||
					dist(this.posAt(tr.pid, release, false, true), S) > WALL_FT
				) {
					continue;
				}
				// (A hand up only once it has gone - his closeout reckoned him
				// still on his way - is put up as it goes instead: he is right
				// there.)
				const busy = tr.acts.filter(
					(a) => a.t1 > release - 300 && a.t0 < release + 500,
				);
				if (busy.some((a) => a.anim !== "contest" || a.t0 < release - 120)) {
					continue;
				}
				tr.acts = tr.acts.filter((a) => !busy.includes(a));
				const D = this.posAt(tr.pid, release, false, true);
				tr.acts.push({
					t0: release - 120,
					t1: release + 560,
					anim: "contest",
					look: { ...S },
					jump: [0.15, 0.85, 0.9],
					...(this.leftToBall(D, shooter, S) ? { mirror: true as const } : {}),
				});
				tr.acts.sort((a, b) => a.t0 - b.t0);
			}
		}
	}

	private biteOnFakes() {
		for (const { pid, t, at } of this.fakes) {
			const me = this.posAt(pid, t);
			let g: number | undefined;
			let near = 6;
			for (const o of this.tracks.values()) {
				const d = dist(this.posAt(o.pid, t), me);
				if (o.team !== this.teamOf(pid) && d < near) {
					near = d;
					g = o.pid;
				}
			}
			const tr = g === undefined ? undefined : this.track(g);
			if (g === undefined || !tr) {
				continue;
			}
			const still =
				!tr.acts.some((x) => x.t1 > t + 130 && x.t0 < t + 690) &&
				!tr.moves.some((m) => m.t1 > t + 130 && m.t0 < t + 690);
			if (still && hash01(g, t) < 0.45) {
				tr.acts.push({
					t0: t + 130,
					t1: t + 690,
					anim: "contest",
					look: { ...at },
					jump: [0.2, 0.8, 0.9],
				});
				tr.acts.sort((a, b) => a.t0 - b.t0);
			} else if (!tr.arms.some((x) => x.t1 > t + 90 && x.t0 < t + 640)) {
				this.gesture(g, "hand", t + 90, t + 640);
				tr.arms.sort((a, b) => a.t0 - b.t0);
			}
		}
	}

	// WORKING HIS MAN. Stood with the ball on his dribble in the half court,
	// his man up on him, a ball handler is never just bouncing it: he jabs at
	// him, or hesitates as if to pull up - and his man gives ground to the
	// jab, and comes up at the hesitation.
	private workHisMan() {
		const ball = this.ball;
		const added: Track[] = [];
		for (let i = 0; i < ball.length; i++) {
			const s = ball[i]!;
			if (s.kind !== "hold" || s.style === "hold") {
				continue;
			}
			let j = i + 1;
			while (
				j < ball.length &&
				ball[j]!.kind === "hold" &&
				(ball[j] as { pid: number }).pid === s.pid &&
				(ball[j] as { style: string }).style !== "hold"
			) {
				j++;
			}
			const first = i;
			i = j - 1;
			const end = ball[j]?.t0 ?? Infinity;
			const tr = this.track(s.pid);
			if (!tr || !Number.isFinite(end) || end - s.t0 < 1400) {
				continue;
			}
			const own = ball.slice(first, j);
			const busy = [
				...tr.moves.map((m) => [m.t0, m.t1] as const),
				...tr.acts.map((a) => [a.t0, a.t1] as const),
				...own.flatMap((x, k) =>
					(x as { style: string }).style === "cross"
						? [[x.t0, own[k + 1]?.t0 ?? end] as const]
						: [],
				),
				...own.slice(1).map((x) => [x.t0 - 250, x.t0 + 250] as const),
			].sort((x, y) => x[0] - y[0]);
			const gaps: [number, number][] = [];
			let at = s.t0 + 200;
			for (const [b0, b1] of busy) {
				if (b1 <= at) {
					continue;
				}
				if (b0 >= end - 200) {
					break;
				}
				if (b0 - 150 - at >= 1100) {
					gaps.push([at, b0 - 150]);
				}
				at = Math.max(at, b1 + 150);
			}
			if (end - 200 - at >= 1100) {
				gaps.push([at, end - 200]);
			}
			const face = attackDir(tr.team);
			for (const [g0, g1] of gaps) {
				let t = g0 + 250 + 350 * hash01(s.pid, g0);
				for (let n = 0; n < 4; n++) {
					const ms = 560 + 160 * hash01(s.pid, t + 1);
					if (t + ms + 200 > g1) {
						break;
					}
					const P = this.posAt(s.pid, t);
					// (Bringing it up, he walks it - see keepDribbling.)
					if ((P.x - COURT_W / 2) * face < 1) {
						break;
					}
					let guard: Track | undefined;
					let G: Pt | undefined;
					let near = 7.5;
					for (const o of this.tracks.values()) {
						if (o.team === tr.team) {
							continue;
						}
						const q = this.posAt(o.pid, t);
						const d = dist(q, P);
						if (d < near) {
							near = d;
							guard = o;
							G = q;
						}
					}
					if (!guard || !G) {
						break;
					}
					const pick = hash01(s.pid, t + 2);
					const hesi = pick < 0.35;
					const inOut = !hesi && pick < 0.6;
					// (The hand it is in then.)
					const hand =
						(own.findLast((x) => x.t0 <= t) as { hand?: Hand } | undefined)
							?.hand ?? "R";
					tr.acts.push({
						t0: t,
						t1: t + ms,
						anim: hesi
							? "dribbleHesi"
							: inOut
								? hand === "L"
									? "dribbleInOutL"
									: "dribbleInOutR"
								: "dribbleJab",
						look: { ...G },
					});
					// Off him on the jab; up at him on the hesitation; across with
					// the ball on the in-and-out, to his left as it goes in from his
					// right hand.
					const u = unitVec(P, G);
					const k = hesi ? -0.6 : inOut ? 0 : 1.1;
					const side = inOut ? (hand === "L" ? -0.9 : 0.9) : 0;
					(guard.nudges ??= []).push({
						t0: t + 120,
						t1: t + ms + 240,
						dx: u.x * k + u.y * side,
						dy: u.y * k - u.x * side,
					});
					added.push(guard, tr);
					t += ms + 350 + 550 * hash01(s.pid, t + 3);
				}
			}
		}
		for (const tr of new Set(added)) {
			tr.acts.sort((a, b) => a.t0 - b.t0);
			tr.nudges?.sort((a, b) => a.t0 - b.t0);
		}
	}

	finish(): CourtTimeline {
		const byT0 = (a: { t0: number }, b: { t0: number }) => a.t0 - b.t0;
		for (const tr of this.tracks.values()) {
			tr.moves.sort(byT0);
			tr.acts.sort(byT0);
			tr.faces.sort((a, b) => a[0] - b[0]);
			tr.looks.sort((a, b) => a[0] - b[0]);
			tr.shown.sort((a, b) => a[0] - b[0]);
		}
		this.ball.sort(byT0);
		this.pace();
		this.liveHands();
		this.keepDribbling();
		this.flowOn();
		this.liven();
		this.stagger();
		for (const tr of this.tracks.values()) {
			// One thing at a time with his arm: the first he started.
			const arms: Gesture[] = [];
			for (const g of tr.arms.sort(byT0)) {
				if (g.t0 >= (arms.at(-1)?.t1 ?? -Infinity)) {
					arms.push(g);
				}
			}
			tr.arms = arms;
		}
		this.unplant();
		this.mark();
		this.unplant();
		this.stagger();
		this.keepClear();
		this.aroundBodies();
		this.biteOnFakes();
		this.wallUp();
		this.workHisMan();
		this.joinUp();
		// A fight for position or a box-out cut to a moment by what came
		// after it never really shows - and left in, an arm thrown up for a
		// frame is all it adds.
		for (const tr of this.tracks.values()) {
			tr.acts = tr.acts.filter(
				(a) => a.t1 - a.t0 >= GLIMPSE_MS || !GLIMPSED.has(a.anim),
			);
		}
		// (Nor, with those gone, does a man pull up dead where one was.)
		this.flowOn();
		// The lead-in to a play - the ball brought up, the set getting going
		// - is run through fast, back at real speed for the last few seconds
		// of it: whatever leads straight to the shot, the steal, the foul. A
		// break is played at real speed from the push up the floor on.
		for (const b of this.beats) {
			if (!LIVE_PLAY.test(b.type)) {
				continue;
			}
			const to =
				b.actionStart -
				(this.breaks.some((t) => t >= b.preStart && t < b.actionStart)
					? BREAK_LIVE
					: this.liveLead(b));
			if (to - b.preStart >= FAST_MIN) {
				this.fast.push([b.preStart, to]);
			}
		}
		// A substitution at a dead ball too: the man coming in jogs on, the
		// man going out walks off - on with the game.
		for (const b of this.beats) {
			if (b.type === "sub") {
				this.fast.push([b.preStart, b.end, DEAD_MIN]);
			}
		}
		this.fx.sort((a, b) => a.t - b.t);
		this.ease();
		this.clearTheWayIn();
		const fast = hurried(this.fast, this.beats);
		this.keepApart(fast);
		this.passBy(fast);
		this.aroundTheMan(fast);
		this.keepOff(fast);
		this.backOutFacing();
		// A look round the building runs on to the picture's next cut when
		// that comes soon after (the substitutions over a timeout, the walk
		// out for the next period), so the game picks up at a cut.
		const shots: ArenaShot[] = [];
		for (const sh of this.shots) {
			const next = this.cuts.find((c) => c > sh.t0 + 1);
			let t1 = sh.t1;
			if (
				next !== undefined &&
				next < sh.t1 + (sh.stretch === false ? 0 : 4500)
			) {
				t1 = next;
			}
			const prev = shots.at(-1);
			if (prev && sh.t0 < prev.t1) {
				prev.t1 = Math.max(prev.t1, t1);
			} else if (t1 > sh.t0 + 300) {
				shots.push({ t0: sh.t0, t1, kind: sh.kind });
			}
		}
		const tl: CourtTimeline = {
			tracks: this.tracks,
			ball: this.ball,
			fx: this.fx,
			beats: this.beats,
			poss: this.poss,
			cuts: this.cuts,
			clips: this.clips,
			jumps: this.jumps,
			shots,
			...(this.intro ? { intro: this.intro } : {}),
			tension: this.tension,
			seats: this.seats,
			fast,
			checkIns: this.checkIns,
			end: this.T,
		};
		// (Worked out on a look at it of its own, and the turns that look
		// worked out forgotten after: where men face is read afresh off
		// where they end up.)
		this.clearTheBall({ ...tl }, fast);
		forgetTurns(this.tracks.values());
		return tl;
	}
}

// Fast stretches in order, overlapping ones run together.
// Whatever just happened gets a moment at real speed before the picture
// hurries on (ms after its line's action is over) - time to take it in: a
// basket longest, the ball down through the net and both teams turning and
// heading back up the floor. A free throw with another to come has been
// seen to the floor by the end of its line already, and a substitution
// needs none.
const TAKE_IN = 800;
const TAKE_IN_SCORE = 1500;
const TAKE_IN_FT = 200;
const FT_LINE = /^(ft|missFt)$/;
const HUDDLE_BEAT = /^(timeout|endOfPeriod|period|overtime)$/;
const sunkIn = (b: Beat, next: Beat | undefined): number =>
	b.type === "sub"
		? b.actionStart
		: // A timeout, the end of a period: the whistle is what happened - the
			// walk off to the huddles that is the rest of it is hurried on
			// through.
			HUDDLE_BEAT.test(b.type)
			? b.actionStart + TAKE_IN
			: b.end +
				(FT_LINE.test(b.type) && next && FT_LINE.test(next.type)
					? TAKE_IN_FT
					: resultOf(b.type)?.kind === "make" || b.type === "ft"
						? TAKE_IN_SCORE
						: TAKE_IN);
// Too short a stretch to bother hurrying through (ms): run through fast, it
// would only be a lurch - except in the dead time around the free throws,
// with nobody else moving.
const FAST_MIN = 2500;
const DEAD_MIN = 1400;
// Lines whose lead-in is the ball in play, building to them - and how much
// of it before the line's action is shown at real speed (ms; see liveLead).
const LIVE_PLAY = /^(fga|tov$|stl$|pfNonShooting$|pfBonus$)/;
const LIVE_LEAD = 2600;
const LIVE_MIN = 2200;
const LIVE_MAX = 3400;
// Off a screen, the soonest he goes up after it is set (ms).
const SCREEN_USE = 450;
// A catch: back at real speed for the start of his cut to it, or of the
// drive that drew the help off him - as far back as this (ms).
const LIVE_ACTION = 5000;
// A jumper off a screen: back at real speed this long (ms) before the
// screen is set - when it is set no further back than LIVE_SCREEN - and the
// screener no farther from him than SCREEN_NEAR feet as it is.
const SCREEN_SEEN = 400;
const LIVE_SCREEN = 6000;
const SCREEN_NEAR = 9;
const BREAK_LIVE = 5000;
// The stretches the picture runs through fast, in order, run together where
// they meet - each starting only once the line before it has sunk in.
const hurried = (
	list: [number, number, number?][],
	beats: Beat[],
): [number, number][] => {
	// (Each line's next, past any substitutions.)
	const after: (Beat | undefined)[] = [];
	for (let j = beats.length - 1; j >= 0; j--) {
		const n = beats[j + 1];
		after[j] = n && n.type === "sub" ? after[j + 1] : n;
	}
	const ends = beats
		.map((b, j) => [b.actionStart, sunkIn(b, after[j])] as const)
		.sort((x, y) => x[0] - y[0]);
	const settled: [number, number, number][] = [];
	let k = 0;
	let seen = -Infinity;
	for (const [a, b, min = FAST_MIN] of [...list].sort((x, y) => x[0] - y[0])) {
		while (k < ends.length && ends[k]![0] <= a) {
			seen = Math.max(seen, ends[k]![1]);
			k++;
		}
		const from = Math.max(a, seen);
		if (b > from) {
			settled.push([from, b, min]);
		}
	}
	const out: [number, number, number][] = [];
	for (const [a, b, min] of settled) {
		const last = out.at(-1);
		if (last && a <= last[1]) {
			last[1] = Math.max(last[1], b);
			last[2] = Math.min(last[2], min);
		} else {
			out.push([a, b, min]);
		}
	}
	return out.filter(([a, b, min]) => b - a >= min).map(([a, b]) => [a, b]);
};

// What a playoff game is for: the wins each side (away, home) had in the
// series going in, how many it takes, and whether it is for the title.
export type Stakes = {
	won: [number, number];
	toWin: number;
	finals: boolean;
};

export type CourtInput = {
	events: RawEvent[];
	players: CourtPlayer[];
	// The starting lineups to call before the opening tip (see intro.ts).
	intro?: "regular" | "playoffs";
	// A playoff game's (celebrated accordingly at the end).
	stakes?: Stakes;
	// The game, which seeds everything the sim leaves open (where the shooter
	// stood, who boxed out) and picks the play-by-play's wording.
	gid: number | undefined;
	gender?: "female" | "male";
};

export const compileCourt = ({
	events,
	players,
	gid,
	gender = "male",
	intro,
	stakes,
}: CourtInput): CourtTimeline => {
	const d = new Director(events, players, gid, gender, intro, stakes);
	for (let i = 0; i < events.length; i++) {
		const e = events[i];
		if (!e || typeof e.type !== "string") {
			continue;
		}
		if (!isLineItem(e)) {
			d.trackScore(e);
			continue;
		}
		if (e.clipStart === true) {
			d.newClip(e);
		}
		d.handle(e, i);
		d.noteClock(e);
	}
	return d.finish();
};

// Where the animation should stand while the playback cursor (events consumed)
// is at `cursor`: the moment the next unshown line happens. Past the last line,
// the end of the game.
// How dark the picture is at t for a cut between clips of a highlight reel:
// a quick dip to black and back across each.
const CLIP_DIP = 170;
export const clipDipAt = (tl: CourtTimeline, t: number): number => {
	let dark = 0;
	for (const c of tl.clips ?? []) {
		const d = Math.abs(t - c);
		if (d < CLIP_DIP) {
			dark = Math.max(dark, 1 - d / CLIP_DIP);
		}
	}
	return dark;
};

export const targetForCursor = (tl: CourtTimeline, cursor: number): number => {
	const beat = tl.beats.find((b) => b.i >= cursor);
	return beat ? beat.actionStart : tl.end;
};

// Where to cut to when playback jumps (a rewind, a fast-forward, joining a
// broadcast late): the end of the last line already shown.
// How many play-by-play lines the page showed going from one cursor to
// another. A line can come with events that are not lines - a basket with the
// points, rebounds and assists it adds up - so this, not how far the cursor
// moved, says whether the page jumped ahead.
export const linesBetween = (
	tl: CourtTimeline,
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

export const snapForCursor = (tl: CourtTimeline, cursor: number): number => {
	let t = 0;
	for (const b of tl.beats) {
		if (b.i >= cursor) {
			break;
		}
		t = b.end;
	}
	return t;
};
