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
	COURT_H,
	COURT_W,
	dist,
	FT_BACK,
	FT_DEFENSE,
	FT_SHOOTER_DEPTH,
	FT_OFFENSE,
	ftOfficialBall,
	guardSpot,
	sideOn,
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
import { CROSS_RATE, DRIBBLE_RATE } from "./evaluate.ts";
import { bodyOf, standingReach, type AnimName, type Hand } from "./poses.ts";
import {
	callAny,
	callShot,
	callTurnover,
	walkPlay,
	type Called,
	type Cast,
	type Play,
	type PlayAction,
	type PlayCategory,
	type PlayOption,
	type PlayRisk,
	type PlayZone,
	type TurnoverKind,
	spotXY,
} from "./plays.ts";
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
export type CourtPlayer = { pid: number; team: Side; pos?: string };

export type Move = {
	t0: number;
	t1: number;
	from: Pt;
	to: Pt;
	anim: AnimName;
	// Set when he was told which way to face on the way (a defender sliding
	// with his man), rather than just running where he is going.
	face?: 1 | -1;
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
	// What he looks at while he does it: the rim he shoots at, the man he
	// passes to.
	look?: Pt;
};
export type Track = {
	pid: number;
	team: Side;
	start: Pt;
	moves: Move[];
	acts: Act[];
	faces: [number, 1 | -1][];
	// From each moment until his next move, what he stands looking at (the
	// middle of a huddle, the rim from the free throw line).
	looks: [number, Pt][];
	shown: [number, boolean][];
};
export type BallEnd = Pt3 | { pid: number; hand?: "near" | "both" };
export type BallSeg =
	| {
			kind: "hold";
			t0: number;
			pid: number;
			// A crossover switches hands every bounce.
			style: "hold" | "dribble" | "cross";
			// The hand he dribbles with (a crossover: starts in); right if
			// unsaid. A crossover goes across in front of him or between his
			// legs.
			hand?: Hand;
			move?: "front" | "legs";
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
	| { kind: "rest"; t0: number; at: Pt3 };
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
	| "toss";
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
// contact. A roar says what it was for, for the boards to shout about.
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
	shots: ArenaShot[];
	// How tense the building is, over time (0 to 1): a close game late in
	// the last period or in overtime.
	tension: [number, number][];
	end: number;
};

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
// floor after a basket, the trip to the free throw line - the picture cuts
// instead of hurrying (see cutAt). Animation cycles advance by distance
// covered, so feet never skate.
const RUN = 21;
const SPRINT = 26;
const DRIBBLE = 19;
const JOG = 13;
const WALK = 6;
const PASS_FTPS = 42;
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
// How far out in front of his feet a man bent to the floor has his hands.
const PICKUP_REACH = 2.9;
// How far through a bounce of his dribble the ball has come back up near
// enough to his hands to take it in both (see evaluate.ts).
const CATCH_UP = 0.7;
// After a whistle, how long the official's signal holds the picture.
const WHISTLE_HOLD = 950;
// A rebound, from leaving the floor to the ball chinned once he is down.
const REBOUND_MS = 1100;
// A dunk, for a typical player (taller ones jump less to get there, shorter
// ones more - see withBody): how high he gets (feet) - his hands well over
// the rim - and how far out from the middle of the rim he goes up.
const DUNK_LEAP = RIM_Z + 1.45 - standingReach(bodyOf());
const DUNK_FROM = 1.8;

// A spot clear behind the three-point line - his toes too - for a shooter
// at `p`, or nothing if he already is: out along the corner, or straight
// back from the rim round the arc.
const THREE_CLEAR = 1.5;
const behindArc = (team: Side, p: Pt): Pt | undefined => {
	const rim = { x: rimX(team), y: COURT_H / 2 };
	const depth = team === 0 ? p.x : COURT_W - p.x;
	if (depth < 14) {
		const across = Math.abs(p.y - rim.y);
		if (across >= 22 + THREE_CLEAR) {
			return undefined;
		}
		const side = p.y >= rim.y ? 1 : -1;
		return { x: p.x, y: rim.y + side * (22 + THREE_CLEAR + 0.5) };
	}
	const d = dist(p, rim);
	if (d >= 23.75 + THREE_CLEAR) {
		return undefined;
	}
	// A real step, not a shuffle too small to take.
	const u = unitVec(rim, p);
	const r = 23.75 + THREE_CLEAR + 0.5;
	return clampPt({ x: rim.x + u.x * r, y: rim.y + u.y * r });
};

const passMs = (d: number) =>
	Math.min(1500, Math.max(260, 180 + (d * 1000) / PASS_FTPS));

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

// How fast each cut and each dribble in a set goes, feet per second: the
// tracking numbers (a curl off a screen about 16, a pick-and-pop about 11),
// sped up like the rest of the court so a set reads in the time it has.
const MOVE_SPEED: Record<string, number> = {
	walk: 6,
	jog: 12,
	sprint: 24,
	v_cut: 20,
	backdoor: 23,
	curl: 21,
	flare: 18,
	fade: 14,
	pop: 14,
	roll: 20,
	short_roll: 16,
	slip: 21,
	lift: 13,
	drift: 12,
	rip_cut: 22,
	shallow_cut: 18,
	iverson_cut: 23,
	flash: 20,
	seal: 9,
	relocate: 13,
	clear_out: 17,
};
const DRIBBLE_SPEED: Record<string, number> = {
	advance: 16,
	attack: 20,
	drive_baseline: 22,
	drive_middle: 22,
	reject: 20,
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
// The shortest a step of a set takes, milliseconds.
const STEP_MIN = 600;

const unitVec = (from: Pt, to: Pt): Pt => {
	const dx = to.x - from.x;
	const dy = to.y - from.y;
	const l = Math.hypot(dx, dy) || 1;
	return { x: dx / l, y: dy / l };
};

// How the offense gets from wherever the ball is into its next set: the
// picture cuts to it (after a basket, a long walk up), to an inbound, a break
// runs straight off the rebound or the steal, or the five flow into it.
type Entry = "cut" | "inbound" | "break" | "flow";

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
	// The first step shown: the picture cuts in late, to the action that
	// makes the play, with everybody where the steps before put them.
	from: number;
	// How much clock the trip took (seconds), when known.
	gap?: number;
	// A little give in every spot, so no two trips down look stamped out.
	jitter: Map<string, Pt>;
	// Defenders the play-by-play has at the shot - the shot blocker, the man
	// who fouls him, the one he dunks on - who work their way to it.
	help?: number[];
	// The screen set in the step before, for the defense to play.
	screen?: {
		screeners: number[];
		user: number;
		ball: boolean;
		at: Pt;
		step: number;
		coverage: Coverage;
	};
};

type ShotStyle =
	| "plain"
	| "crossover"
	| "euro"
	| "stepBack"
	| "fade"
	| "post"
	| "hook";

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
	// Where each player's chair is on his bench.
	private readonly seat = new Map<number, Pt>();
	private readonly lineup: [number[], number[]] = [[], []];
	readonly ball: BallSeg[] = [];
	readonly fx: Fx[] = [];
	readonly beats: Beat[] = [];
	readonly poss: [number, Side][] = [];
	readonly cuts: number[] = [];
	readonly tension: [number, number][] = [];
	// The period being played, and whether it is overtime.
	private periodNo = 1;
	private overtime = false;
	// `stretch`: may run on to the next cut (see finish).
	readonly shots: (ArenaShot & { stretch?: boolean })[] = [];

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
	private lastClock: number | undefined;
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
		  }
		| undefined;
	private readonly score: [number, number] = [0, 0];
	// Who is guarding whom this possession, once a switch has changed it from
	// position against position.
	private readonly guarding = new Map<number, number>();

	private readonly events: RawEvent[];
	private readonly gid: number | undefined;
	private readonly gender: "female" | "male";

	constructor(
		events: RawEvent[],
		players: CourtPlayer[],
		gid: number | undefined,
		gender: "female" | "male",
	) {
		this.events = events;
		this.gid = gid;
		this.gender = gender;
		this.rng = makeCourtRng(`court|${gid ?? 0}`);
		const seats: [number, number] = [0, 0];
		for (const p of players) {
			this.team.set(p.pid, p.team);
			this.rank.set(p.pid, POS_RANK[p.pos ?? ""] ?? 4);
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

		for (const p of players) {
			const on = this.lineup[p.team].includes(p.pid);
			const slot = on ? this.slots(p.team).indexOf(p.pid) : 0;
			const start = on
				? { x: COURT_W / 2 + (p.team === 1 ? -9 : 9), y: 9 + slot * 8 }
				: this.seatOf(p.pid);
			this.tracks.set(p.pid, {
				pid: p.pid,
				team: p.team,
				start,
				moves: [],
				acts: [],
				faces: [[-Infinity, attackDir(p.team)]],
				looks: [],
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
		const dur = Math.max(240, (d / speed) * 1000);
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

	// The same, but arriving no later than `by` if he can (a slower man takes
	// the time he needs).
	private goBy(
		pid: number,
		to: Pt,
		t0: number,
		by: number,
		anim: AnimName = "run",
		face?: 1 | -1,
	): number {
		const start = Math.max(t0, this.free.get(pid) ?? 0);
		const d = dist(this.posOf(pid), to);
		const speed = Math.max(
			JOG,
			Math.min(SPRINT, d / Math.max(0.25, (by - start) / 1000)),
		);
		return this.go(pid, to, start, speed, anim, face);
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
		// first.
		if (anim === "pass" || anim === "passBounce" || anim === "passOverhead") {
			this.gather(pid, t0);
		}
		tr.acts.push({ t0, t1, anim, ...rest });
		if (face !== undefined) {
			this.turn(pid, t0, face);
		}
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

	// Where a player is at a moment, read off his own schedule (the same easing
	// the evaluator uses).
	private posAt(pid: number, t: number): Pt {
		const moves = this.track(pid)?.moves ?? [];
		for (let k = moves.length - 1; k >= 0; k--) {
			const m = moves[k]!;
			if (m.t0 <= t) {
				if (t >= m.t1) {
					return m.to;
				}
				const e = 0.5 - 0.5 * Math.cos((Math.PI * (t - m.t0)) / (m.t1 - m.t0));
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
		move?: "front" | "legs",
	): number {
		let t = style === "dribble" ? t0 : this.offDribble(pid, t0, style);
		// Off a run of crossovers, he dribbles on once the ball comes up into
		// a hand - the hand it comes up into.
		const last = this.ball.at(-1);
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
			(this.ballHandOf === pid ? this.ballHand : "R");
		this.pushBall({
			kind: "hold",
			t0: t,
			pid,
			style,
			...(style !== "hold" ? { hand: h } : {}),
			...(move ? { move } : {}),
		});
		this.holder = pid;
		this.ballHand = style === "hold" ? "R" : h;
		this.ballHandOf = pid;
		return t;
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
		return this.dribbleFrom(pid, by) === undefined
			? by
			: this.hold(pid, by, "hold");
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
	): number {
		const b = this.ballPoint();
		const from = this.posOf(pid);
		const stop =
			dist(from, b) > PICKUP_REACH
				? (() => {
						const u = unitVec(from, b);
						return clampPt({
							x: b.x - u.x * PICKUP_REACH,
							y: b.y - u.y * PICKUP_REACH,
						});
					})()
				: from;
		const got = this.go(pid, stop, t, speed, "run");
		this.act(pid, "pickup", got, got + 300, { look: { x: b.x, y: b.y } });
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
		this.offense = team;
	}

	private effect(kind: FxKind, t: number, o: Omit<Fx, "kind" | "t"> = {}) {
		this.fx.push({ kind, t, ...o });
	}

	private rand(lo: number, hi: number) {
		return lo + this.rng() * (hi - lo);
	}

	private finishFor(e: RawEvent | undefined): Finish | undefined {
		return e ? finishOf(e, this.gid, this.gender) : undefined;
	}

	private slots(team: Side): number[] {
		return [...this.lineup[team]].sort(
			(a, b) => (this.rank.get(a) ?? 4) - (this.rank.get(b) ?? 4) || a - b,
		);
	}

	private teamOf(pid: number): Side {
		return this.team.get(pid) ?? 0;
	}

	private seatOf(pid: number): Pt {
		return { ...(this.seat.get(pid) ?? TABLE) };
	}

	// ---- cuts -------------------------------------------------------------------

	// THE CUT, like a condensed game: everything in motion stops at tc, and
	// whoever stages what comes next places the five on the floor where they
	// would be by then. Everyone else sits back down on his bench.
	private cutAt(tc: number) {
		if (this.cuts.at(-1) === tc) {
			return;
		}
		this.cuts.push(tc);
		this.guarding.clear();
		for (const tr of this.tracks.values()) {
			const at = this.posAt(tr.pid, tc);
			tr.moves = tr.moves
				.filter((m) => m.t0 < tc)
				.map((m) => (m.t1 > tc ? { ...m, t1: tc, to: at } : m));
			tr.acts = tr.acts
				.filter((a) => a.t0 < tc)
				.map((a) => (a.t1 > tc ? { ...a, t1: tc } : a));
			tr.faces = tr.faces.filter((f) => f[0] < tc);
			tr.looks = tr.looks.filter((l) => l[0] < tc);
			tr.shown = tr.shown.filter((x) => x[0] < tc);
			this.pos.set(tr.pid, at);
			this.free.set(tr.pid, tc);
			const on = this.lineup[tr.team].includes(tr.pid);
			if (!on) {
				this.place(tr.pid, this.seatOf(tr.pid), tc);
			}
			this.show(tr.pid, tc, on);
		}
		while (this.ball.length > 1 && this.ball.at(-1)!.t0 >= tc) {
			this.ball.pop();
		}
	}

	// Put him somewhere at once - only ever at a cut.
	private place(pid: number, at: Pt, t: number, face?: 1 | -1) {
		const tr = this.track(pid);
		if (!tr) {
			return;
		}
		tr.moves.push({
			t0: t,
			t1: t + 1,
			from: { ...at },
			to: { ...at },
			anim: "walk",
		});
		if (face !== undefined) {
			this.turn(pid, t, face);
		}
		this.pos.set(pid, { ...at });
		this.free.set(pid, t + 1);
	}

	// Cut to the ball crossing half court, everyone a few steps short of
	// their spots and the defense picking them up.
	private cutToSet(team: Side, t: number): number {
		const tc = t + 120;
		this.cutAt(tc);
		const dir = attackDir(team);
		const off = this.slots(team);
		const def = this.slots(other(team));
		const spots = this.setSpots(team, 0);
		off.forEach((pid, j) => {
			const s = spots[j] ?? spots[0]!;
			const back = j === 0 ? this.rand(5, 8) : this.rand(3, 7);
			this.place(
				pid,
				clampPt({
					x: s.x - dir * back,
					y: s.y + (COURT_H / 2 - s.y) * 0.2,
				}),
				tc,
				dir,
			);
		});
		def.forEach((pid, j) => {
			const man = this.posOf(off[j] ?? off[0]!);
			this.place(pid, guardSpot(team, man, 0.3), tc, -dir as 1 | -1);
		});
		const pg = off[0]!;
		this.hold(pg, tc, "dribble");
		const arrive = this.go(
			pg,
			spots[0]!,
			tc + 60,
			DRIBBLE * 0.75,
			"dribble",
			dir,
		);
		this.settle(team, 0, tc + 60, Math.max(tc + 1000, arrive - 150), [pg]);
		this.motionTeam = team;
		this.motion = 0;
		return Math.max(arrive, tc + 600);
	}

	// A dead ball in the frontcourt: cut to the inbounder at the spot with the
	// ball and the rest in their places, then the inbound.
	private cutToInbound(team: Side, t: number, at: Pt): number {
		const tc = t + 120;
		this.cutAt(tc);
		const dir = attackDir(team);
		const off = this.slots(team);
		const def = this.slots(other(team));
		const receiver = off[0]!;
		const inbounder = off[2] ?? off[1] ?? off.at(-1)!;
		const far = at.y < COURT_H / 2;
		// From behind the baseline, or from the sideline.
		const baseline = at.x < 0 || at.x > COURT_W;
		const oob = baseline
			? { x: at.x, y: Math.min(44, Math.max(6, at.y)) }
			: {
					x: Math.min(COURT_W - 3, Math.max(3, at.x)),
					y: far ? -1.4 : COURT_H + 1.4,
				};
		const spots = this.setSpots(team, 0);
		off.forEach((pid, j) => {
			const s =
				pid === inbounder
					? oob
					: pid === receiver
						? baseline
							? clampPt({ x: oob.x - dir * 9, y: oob.y + (25 - oob.y) * 0.4 })
							: clampPt({ x: oob.x + dir * 7, y: far ? 9 : COURT_H - 9 })
						: (spots[j] ?? spots[0]!);
			this.place(pid, s, tc, dir);
		});
		def.forEach((pid, j) => {
			const man = this.posOf(off[j] ?? off[0]!);
			this.place(pid, guardSpot(team, man, 0.25), tc, -dir as 1 | -1);
		});
		this.lookAt(inbounder, tc + 1, this.posOf(receiver));
		this.hold(inbounder, tc, "hold");
		const tIn = this.passTo(inbounder, receiver, tc + 450);
		this.settle(team, 0, tc + 300, tIn + 500, [receiver]);
		this.motionTeam = team;
		this.motion = 0;
		return tIn;
	}

	// ---- formations -----------------------------------------------------------

	private setSpots(team: Side, motion: number): Pt[] {
		const phase =
			MOTION_OFFENSE_SPOTS[Math.min(MOTION_OFFENSE_SPOTS.length - 1, motion)]!;
		return phase.map((s) => spot(team, s.depth, s.across));
	}

	// Everybody but `except` into the half-court set at motion phase `motion`,
	// the defense shadowing them.
	private settle(
		team: Side,
		motion: number,
		t0: number,
		by: number,
		except: number[] = [],
	) {
		const spots = this.setSpots(team, motion);
		const off = this.slots(team);
		const def = this.slots(other(team));
		const dir = attackDir(team);
		off.forEach((pid, j) => {
			const target = spots[j] ?? spots[0]!;
			if (!except.includes(pid)) {
				this.goBy(pid, target, t0 + j * 40, by, "run");
				this.turn(pid, Math.max(by, this.free.get(pid) ?? 0), dir);
			}
			const d = def[j];
			if (d !== undefined && !except.includes(d)) {
				this.goBy(
					d,
					guardSpot(team, target),
					t0 + 60 + j * 50,
					by + 120,
					"run",
				);
				this.turn(d, Math.max(by + 120, this.free.get(d) ?? 0), -dir as 1 | -1);
			}
		});
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
	): number {
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
		// Off the dribble, once he has it in both hands.
		const start = Math.max(ready, this.gather(from, ready));
		this.act(
			from,
			over ? "passOverhead" : kind === "bounce" ? "passBounce" : "pass",
			start,
			start + wind + 180,
			{
				face: toward,
				look: { ...b },
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

	private inBackcourt(team: Side, p: Pt): boolean {
		return team === 1 ? p.x < COURT_W / 2 : p.x > COURT_W / 2;
	}

	// The point guard brings it up and the five settle into their set.
	private bringUp(team: Side, t: number): number {
		const slots = this.slots(team);
		const pg = slots[0]!;
		if (this.holder !== pg && this.holder !== undefined) {
			// Get it to the point guard first.
			const h = this.posOf(this.holder);
			const meet = clampPt({
				x: h.x + attackDir(team) * 9,
				y: h.y < 25 ? 8 : 42,
			});
			this.go(pg, meet, t, RUN, "run");
			t = this.passTo(this.holder, pg, t + 200);
		}
		const top = this.setSpots(team, 0)[0]!;
		const d = dist(this.posOf(pg), top);
		this.hold(pg, t, "dribble");
		const arrive = this.go(pg, top, t, DRIBBLE, "dribble", attackDir(team));
		this.settle(team, 0, t, Math.max(t + 900, arrive - 200), [pg]);
		this.motionTeam = team;
		this.motion = 0;
		return Math.max(arrive, t + (d < 4 ? 300 : 0));
	}

	// A fast break: outlet if a big has it, the ball handler pushes, the wings
	// fill the lanes, the defense sprints back with one man protecting the rim.
	private pushBreak(team: Side, t: number): number {
		const slots = this.slots(team);
		let handler = this.holder ?? slots[0]!;
		if ((this.rank.get(handler) ?? 4) >= 5 && slots[0] !== handler) {
			const h = this.posOf(handler);
			const pg = slots[0]!;
			this.go(
				pg,
				clampPt({ x: h.x + attackDir(team) * 10, y: h.y < 25 ? 6 : 44 }),
				t,
				SPRINT,
				"run",
			);
			t = this.passTo(handler, pg, t + 150);
			handler = pg;
		}
		const spots = TRANSITION_OFFENSE_SPOTS.map((s) =>
			spot(team, s.depth, s.across),
		);
		this.hold(handler, t, "dribble");
		const arrive = this.go(
			handler,
			spots[0]!,
			t,
			SPRINT - 3,
			"dribble",
			attackDir(team),
		);
		const others = slots.filter((p) => p !== handler);
		others.forEach((pid, j) => {
			this.goBy(
				pid,
				spots[j + 1] ?? spots[0]!,
				t + j * 60,
				arrive + j * 250,
				"run",
			);
		});
		const def = this.slots(other(team));
		def.forEach((pid, j) => {
			const rimGuard = j === def.length - 1;
			const target = rimGuard
				? spot(team, 6, 25)
				: clampPt({
						x: (spots[j + 1] ?? spots[0]!).x - attackDir(team) * 7,
						y: 25 + ((spots[j + 1] ?? spots[0]!).y - 25) * 0.8,
					});
			this.goBy(pid, target, t + 40 * j, arrive + 200, "run");
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
		let receiver = slots[MOTION_HANDLER_SLOT[m] ?? 1];
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
	): { t: number; run?: Running } {
		const phase = this.phase;
		const changed = this.offense !== team;
		if (changed) {
			this.guarding.clear();
		}
		this.setOffense(t, team);
		let transition = false;
		let run: Running | undefined;

		let cut = false;
		if (phase === "inboundBase" && !changed) {
			// After a basket: skip the inbound and the walk up the floor.
			run = call?.("cut");
			t = run ? this.cutToPlay(run, t) : this.cutToSet(team, t);
			cut = true;
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
				gap !== undefined &&
				gap < 7 &&
				this.inBackcourt(team, handler);
			if (
				!transition &&
				phase === "loose" &&
				this.inBackcourt(team, handler) &&
				Math.abs(handler.x - COURT_W / 2) > 18
			) {
				// No break on: he turns up the floor, and the picture cuts to the
				// ball crossing half court.
				run = call?.("cut");
				t = run ? this.cutToPlay(run, t + 450) : this.cutToSet(team, t + 450);
				cut = true;
			}
		} else {
			// A dead ball: the inbound, from wherever it went dead.
			const at = this.inboundAt;
			if (at && !this.inBackcourt(team, at)) {
				run = call?.("inbound");
				if (run && (run.play.cat === "blob" || run.play.cat === "slob")) {
					t = this.cutToInboundPlay(run, t);
				} else {
					t = this.cutToInbound(team, t, at);
					if (run) {
						t = this.flowToPlay(run, t);
					}
				}
			} else {
				run = call?.("cut");
				t = run ? this.cutToPlay(run, t) : this.cutToSet(team, t);
			}
			cut = true;
		}

		const handlerPos = this.posOf(this.holder ?? this.slots(team)[0]!);
		if (transition) {
			run = call?.("break");
			t = run ? this.startBreak(run, t) : this.pushBreak(team, t);
		} else if (
			!cut &&
			(this.inBackcourt(team, handlerPos) || this.motionTeam !== team)
		) {
			run = call?.("flow");
			t = run ? this.flowToPlay(run, t) : this.bringUp(team, t);
		} else if (!cut && gap !== undefined && gap >= 7) {
			// Still in the half court (an offensive rebound): kick it out and
			// reset into a set - when there is time for one.
			run = call?.("flow");
			if (run) {
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
		const p = this.spotFor(run.team, run.mirror, name);
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
		// How much of it to show: the step that springs the shot, and the one
		// before it when the trip took long enough to have run it. A break or an
		// inbound play is shown whole.
		const keep =
			entry === "break" || play.cat === "blob" || play.cat === "slob"
				? 99
				: gap === undefined || gap < 12
					? 1
					: 2;
		const run: Running = {
			play,
			team,
			roles: called.roles,
			mirror: called.mirror,
			jitter: new Map(),
			from: Math.max(0, Math.min(upTo, play.steps.length) - keep + 1),
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
		} else if (gap !== undefined && gap < 10) {
			cats.early = entry === "flow" ? 1.6 : 0.6;
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
		const holder = entry === "break" ? this.holder : undefined;
		if (!on(shooter) || !on(assist) || !on(holder)) {
			return undefined;
		}
		const ask = (cats: Partial<Record<PlayCategory, number>>) =>
			callShot(this.rng, {
				cats,
				zone: pz,
				shooter,
				assist,
				unassisted: plan.kind === "make" && assist === undefined,
				holder,
				five,
			});
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
		const holder = entry === "break" ? this.holder : undefined;
		if (
			!five.some((c) => c.pid === victim) ||
			(holder !== undefined && !five.some((c) => c.pid === holder))
		) {
			return undefined;
		}
		const ask = (cats: Partial<Record<PlayCategory, number>>) =>
			callTurnover(this.rng, { cats, victim, kinds, holder, five });
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
		const holder = entry === "break" ? this.holder : undefined;
		if (holder !== undefined && !five.some((c) => c.pid === holder)) {
			return undefined;
		}
		const called = callAny(this.rng, {
			cats: this.playCats(entry, gap, clock),
			five,
			holder,
		});
		return called ? this.running(team, called, entry, {}, gap) : undefined;
	}

	// Cut to the set: the five a step short of its spots with the ball coming
	// up, the defense picking them up.
	private cutToPlay(run: Running, t: number): number {
		const tc = t + 120;
		this.cutAt(tc);
		const { team } = run;
		const f = this.formation(run);
		const dir = attackDir(team);
		const bh = run.roles[f.ball]!;
		run.roles.forEach((pid, r) => {
			const S = this.at(run, f.at[r]!);
			const back = pid === bh ? this.rand(3, 5) : this.rand(1.5, 3.5);
			this.place(
				pid,
				clampPt({
					x: S.x - dir * back,
					y: S.y + (COURT_H / 2 - S.y) * 0.06,
				}),
				tc,
				dir,
			);
		});
		this.hold(bh, tc, "dribble");
		this.placeDefense(run, tc);
		let ready = tc + 200;
		run.roles.forEach((pid, r) => {
			const S = this.at(run, f.at[r]!);
			ready = Math.max(
				ready,
				pid === bh
					? this.go(pid, S, tc + 60, DRIBBLE * 0.7, "dribble", dir)
					: this.go(pid, S, tc + 60 + r * 40, JOG * 0.7, "run"),
			);
		});
		this.guardStep(run, [], tc + 80, ready);
		this.motionTeam = team;
		this.motion = 0;
		return this.sizeUp(run, bh, ready + 60);
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
		const r = this.rng();
		if (r < 0.4) {
			return t;
		}
		if (r < 0.65) {
			const dur = this.rand(850, 1150);
			this.act(bh, "callPlay", t, t + dur);
			return t + dur;
		}
		// On the next beat of his dribble, once or twice across.
		const top = this.dribbleTop(bh, t) ?? t;
		const tc = this.hold(
			bh,
			top < t - 1 ? top + DRIBBLE_MS : top,
			"cross",
			undefined,
			this.rng() < 0.4 ? "legs" : "front",
		);
		const back = tc + (this.rng() < 0.5 ? 2 : 1) * CROSS_MS;
		this.hold(bh, back, "dribble");
		return back + 120;
	}

	// An inbound play: cut to it drawn up, the inbounder out of bounds with
	// the ball.
	private cutToInboundPlay(run: Running, t: number): number {
		const tc = t + 120;
		this.cutAt(tc);
		const { team, play } = run;
		const dir = attackDir(team);
		const bh = run.roles[play.ball]!;
		run.roles.forEach((pid, r) => {
			this.place(pid, this.at(run, play.start[r]!), tc, dir);
		});
		this.hold(bh, tc, "hold");
		this.lookAt(bh, tc + 1, { x: rimX(team), y: COURT_H / 2 });
		this.placeDefense(run, tc);
		this.motionTeam = team;
		this.motion = 0;
		return tc + 650;
	}

	// No cut: from wherever they are into the set's spots, the ball brought
	// up or kicked out to whoever starts with it.
	private flowToPlay(run: Running, t: number): number {
		const { team } = run;
		const f = this.formation(run);
		const dir = attackDir(team);
		const bh = run.roles[f.ball]!;
		const had = this.holder;
		let ready = t + 600;
		run.roles.forEach((pid, r) => {
			if (pid === had && had !== bh) {
				return;
			}
			const S = this.at(run, f.at[r]!);
			const far = dist(this.posOf(pid), S) > 20;
			if (pid === had) {
				this.hold(pid, Math.max(t, this.free.get(pid) ?? 0), "dribble");
				ready = Math.max(
					ready,
					this.go(pid, S, t, DRIBBLE * (far ? 0.85 : 0.6), "dribble", dir),
				);
			} else {
				ready = Math.max(
					ready,
					this.go(pid, S, t + r * 40, far ? RUN * 0.85 : JOG, "run"),
				);
			}
		});
		if (had !== undefined && had !== bh) {
			ready = Math.max(ready, this.passTo(had, bh, t + 150));
			const r = run.roles.indexOf(had);
			if (r >= 0) {
				ready = Math.max(
					ready,
					this.go(had, this.at(run, f.at[r]!), t, JOG, "run"),
				);
			}
		}
		this.guardStep(run, [], t + 150, ready);
		this.motionTeam = team;
		this.motion = 0;
		return ready + 100;
	}

	// A break runs from wherever they are when the ball is won; whoever the
	// set does not send somewhere right away runs the floor to his lane.
	private startBreak(run: Running, t: number): number {
		const bh = run.roles[run.play.ball]!;
		if (this.holder !== undefined && this.holder !== bh) {
			t = this.passTo(this.holder, bh, t + 100);
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
		run.roles.forEach((pid, r) => {
			if (pid !== bh && !going.has(r)) {
				this.go(pid, this.at(run, f.at[r]!), t, RUN, "run");
			}
		});
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
		for (const a of acts) {
			if (a.type === "screen") {
				const user = run.roles[a.for]!;
				const onBall = BALL_SCREENS.has(a.kind) && this.holder === user;
				const screeners = a.who.map((r) => run.roles[r]!);
				const U = this.posOf(user);
				let first: Pt | undefined;
				screeners.forEach((s, j) => {
					const marked = this.at(run, a.at[j] ?? a.at[0]!);
					let S: Pt;
					if (onBall) {
						// On the handler's man: between them, to the side he will
						// turn the corner.
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
							x: U.x + ur.x * 2.8 - ur.y * side * 1.3,
							y: U.y + ur.y * 2.8 + ur.x * side * 1.3,
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
					const there = this.go(
						s,
						S,
						Math.max(t0, this.free.get(s) ?? 0),
						16,
						"run",
					);
					planted.push({ pid: s, t: there, anim: "screen", look: U });
					end = Math.max(end, there + 150);
				});
				run.screen = {
					screeners,
					user,
					ball: onBall,
					at: first ?? rim,
					step: k,
					coverage: this.coverageFor(run, screeners[0]!, user, onBall, next),
				};
			} else if (a.type === "dribble") {
				end = Math.max(end, this.playDribble(run, a.who, a.to, a.kind, t0));
			} else if (a.type === "move") {
				const pid = run.roles[a.who]!;
				end = Math.max(
					end,
					this.holder === pid
						? this.playDribble(run, a.who, a.to, "attack", t0)
						: this.playMove(run, pid, a.to, a.style, t0, before),
				);
			} else if (a.type === "pass") {
				const from = this.holder ?? run.roles[a.who]!;
				const to = run.roles[a.to]!;
				if (from !== to) {
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
			// Planted until the step is over, or until he moves on in it.
			const later = this.track(p.pid)?.moves.find((m) => m.t0 > p.t + 1);
			const until = Math.min(Math.max(p.t + 450, end), later?.t0 ?? Infinity);
			if (until > p.t + 100) {
				this.act(p.pid, p.anim, p.t, until, { look: p.look });
			}
		}
		this.guardStep(run, acts, t0, end, before, k);
		return end;
	}

	private playDribble(
		run: Running,
		who: number,
		to: string,
		kind: string,
		t0: number,
	): number {
		const pid = run.roles[who]!;
		const dir = attackDir(run.team);
		const P =
			to === "rim" ? this.nearRim(run.team, this.posOf(pid)) : this.at(run, to);
		const start = Math.max(t0, this.free.get(pid) ?? 0);
		if (this.holder !== pid) {
			// The ball never got to him: he just goes.
			return this.go(pid, P, start, 14, "run");
		}
		// A drive at his man - now and then with a move to get by him first.
		if (
			kind === "crossover" ||
			kind === "hesitation" ||
			((kind === "attack" ||
				kind === "drive_baseline" ||
				kind === "drive_middle") &&
				dist(this.posOf(pid), P) > 9 &&
				this.rng() < 0.22)
		) {
			const t = this.driveTo(pid, P, start, dir, "crossover");
			this.hold(pid, t, "dribble");
			return t;
		}
		if (kind === "step_back") {
			const t = this.driveTo(pid, P, start, dir, "stepBack");
			this.hold(pid, t, "hold");
			return t;
		}
		this.hold(
			pid,
			start,
			kind === "snake" ? "cross" : "dribble",
			kind === "snake" ? undefined : this.handFor(this.posOf(pid), P, dir),
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
		return this.go(
			pid,
			P,
			t,
			DRIBBLE_SPEED[kind] ?? 16,
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
	): number {
		const rim = { x: rimX(run.team), y: COURT_H / 2 };
		const from = this.posOf(pid);
		const P =
			to === "rim" ? this.nearRim(run.team, from, 3.6) : this.at(run, to);
		const speed = MOVE_SPEED[style] ?? 13;
		let t = Math.max(t0, this.free.get(pid) ?? 0);
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
		const there = this.go(
			recv,
			meet,
			Math.max(t0, this.free.get(recv) ?? 0),
			12,
			"run",
		);
		const tx = Math.max(there - 120, this.free.get(giver) ?? 0, held + 180);
		this.act(giver, "pass", tx - 180, tx + 120, { look: meet });
		this.fly(tx, tx + 130, { pid: giver }, { pid: recv });
		this.act(recv, "catch", tx + 30, tx + 200);
		this.hold(recv, tx + 130, "dribble");
		this.free.set(recv, Math.max(this.free.get(recv) ?? 0, tx + 200));
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
		};
		return Math.max(tx + 350, set);
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
	private defensePoint(team: Side, man: Pt, ball: Pt, onBall: boolean): Pt {
		const rim = { x: rimX(team), y: COURT_H / 2 };
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
		// Never camped under the rim.
		const pr = dist(p, rim);
		if (pr < 3) {
			const u = unitVec(rim, pr > 0.1 ? p : man);
			return clampPt({ x: rim.x + u.x * 3, y: rim.y + u.y * 3 });
		}
		return clampPt(sideOn(man, p, rim.x));
	}

	private placeDefense(run: Running, tc: number) {
		const dir = attackDir(run.team);
		const ball =
			this.holder !== undefined ? this.posOf(this.holder) : this.ballAt;
		for (const pid of run.roles) {
			const d = this.defenderOf(pid);
			if (d !== undefined) {
				this.place(
					d,
					this.defensePoint(
						run.team,
						this.posOf(pid),
						ball,
						pid === this.holder,
					),
					tc,
					-dir as 1 | -1,
				);
			}
		}
	}

	// A defender to his spot by `by`: sliding if it is close, running if not.
	private shadow(d: number, P: Pt, from: number, by: number, team: Side) {
		const start = Math.max(from, this.free.get(d) ?? 0);
		const dd = dist(this.posOf(d), P);
		if (dd < 0.4) {
			return;
		}
		const secs = Math.max(0.3, (by - start) / 1000);
		const speed = Math.min(SPRINT, Math.max(4, dd / secs));
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
			}
		}
		const holder = this.holder;
		const ball = holder !== undefined ? this.posOf(holder) : this.ballAt;
		const targets = new Map<number, Pt>();
		for (const pid of this.slots(team)) {
			const d = this.defenderOf(pid);
			if (d !== undefined) {
				targets.set(
					d,
					this.defensePoint(team, this.posOf(pid), ball, pid === holder),
				);
			}
		}
		const via = new Map<number, { at: Pt; by: number }>();
		const late = new Map<number, number>();
		if (sc) {
			const ud = this.defenderOf(sc.user);
			const sd = this.defenderOf(sc.screeners[0]!);
			const U = this.posOf(sc.user);
			if (sc.ball && sd !== undefined) {
				if (sc.coverage === "drop") {
					// Back in the paint, between the ball and the rim.
					const u = unitVec(rim, U);
					const kk = Math.min(12, Math.max(4, dist(U, rim) - 7));
					targets.set(sd, { x: rim.x + u.x * kk, y: rim.y + u.y * kk });
				} else if (sc.coverage === "hedge") {
					// Out at the ball for a beat, then back to his man.
					const u = unitVec(sc.at, U);
					via.set(sd, {
						at: clampPt({ x: sc.at.x + u.x * 2.6, y: sc.at.y + u.y * 2.6 }),
						by: t0 + 450,
					});
				} else if (sc.coverage === "blitz" && ud !== undefined) {
					const ur = unitVec(U, rim);
					targets.set(
						sd,
						clampPt({
							x: U.x + ur.x * 2 - ur.y * 2,
							y: U.y + ur.y * 2 + ur.x * 2,
						}),
					);
					targets.set(
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
				targets.set(ud, clampPt({ x: U.x + u.x * 2.6, y: U.y + u.y * 2.6 }));
				late.set(ud, 300);
			}
		}
		// A drive at the rim: the nearest help steps in front of it.
		const drove =
			holder !== undefined &&
			acts.some(
				(a) =>
					a.type === "dribble" &&
					run.roles[a.who] === holder &&
					a.kind !== "retreat",
			);
		if (drove && holder !== undefined) {
			const D = this.posOf(holder);
			if (dist(D, rim) < 14) {
				const onBall = this.defenderOf(holder);
				let help: number | undefined;
				let best = Infinity;
				for (const [d, P] of targets) {
					const dd = dist(P, D);
					if (d !== onBall && dd < best) {
						best = dd;
						help = d;
					}
				}
				if (help !== undefined && best > 2.5) {
					const u = unitVec(D, rim);
					targets.set(
						help,
						clampPt({ x: D.x + u.x * 3.2, y: D.y + u.y * 3.2 }),
					);
				}
			}
		}
		// Whoever meets the shot drifts toward it, and is there for it.
		const o = run.option;
		if (o && run.help && run.help.length > 0) {
			const S =
				o.at === "rim"
					? this.nearRim(team, this.posOf(run.roles[o.shooter]!))
					: this.at(run, o.at);
			const u = unitVec(S, rim);
			const H = clampPt({ x: S.x + u.x * 2.4, y: S.y + u.y * 2.4 });
			for (const d of run.help) {
				const cur = targets.get(d);
				targets.set(
					d,
					k >= o.after || !cur
						? H
						: { x: (cur.x + H.x) / 2, y: (cur.y + H.y) / 2 },
				);
				late.delete(d);
			}
		}
		const lag = run.play.cat === "break" ? 500 : 150;
		for (const [d, P] of targets) {
			const v = via.get(d);
			if (v) {
				this.shadow(d, v.at, t0 + 80, v.by, team);
			}
			this.shadow(d, P, t0 + 120, t1 + lag + (late.get(d) ?? 0), team);
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
				this.rng() < 0.5 ? this.rand(1, 1.5) : this.rand(48.5, 49),
			);
		}
		const [r0, r1, th0, th1] =
			zone === "atRim" || zone === "tipIn" || zone === "putBack"
				? [1.2, 3.6, 30, 150]
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

	// Off the dribble to his spot, with a move on the way.
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
		if (style === "crossover") {
			// A hesitation with the ball in the other hand and a jab that way,
			// then one quick bounce across - in front of him or between his
			// legs - into the hand on the side he goes, and the drive.
			const start: Hand = goHand === "R" ? "L" : "R";
			this.hold(pid, t, "dribble", start);
			// His right is toward the camera when he faces the right rim.
			const jab = (start === "R" ? 1 : -1) * dir * 1.6;
			const tj = this.go(
				pid,
				clampPt({ x: from.x, y: from.y + jab }),
				t + 80,
				6,
				"dribble",
				dir,
			);
			// Across as the ball comes up into his hand: on the next beat of
			// his dribble.
			const top = this.dribbleTop(pid, tj) ?? tj;
			const tc = this.hold(
				pid,
				top < tj - 1 ? top + DRIBBLE_MS : top,
				"cross",
				start,
				this.rng() < 0.3 ? "legs" : "front",
			);
			t = tc + CROSS_MS;
		}
		this.hold(pid, t, "dribble", goHand);
		if (style === "euro") {
			// One way, then the other, then up.
			const side = this.rng() < 0.5 ? 1 : -1;
			const k = Math.max(0, d - 6);
			const a = clampPt({
				x: from.x + (dx / d) * k + ax * side * 2,
				y: from.y + (dy / d) * k + ay * side * 2,
			});
			t = this.go(pid, a, t, DRIBBLE, "dribble", dir);
			t = Math.max(t, this.hold(pid, t, "hold"));
			const b = clampPt({
				x: P.x - (dx / d) * 2.2 - ax * side * 1.6,
				y: P.y - (dy / d) * 2.2 - ay * side * 1.6,
			});
			t = this.go(pid, b, t, RUN * 0.8, "run", dir);
			return this.go(pid, P, t, RUN * 0.8, "run", dir);
		}
		if (style === "stepBack") {
			// Into his man, then a hop back to where he shoots from.
			const inside = clampPt({
				x: P.x + (dx / d) * 2.6,
				y: P.y + (dy / d) * 2.6,
			});
			t = this.go(pid, inside, t, DRIBBLE, "dribble", dir);
			t = Math.max(t, this.hold(pid, t, "hold"));
			return this.go(pid, P, t + 40, 9, "back", dir);
		}
		t = this.go(pid, P, t, DRIBBLE, "dribble", dir);
		if (style === "post") {
			t = this.backDown(pid, t, dir);
		}
		return t;
	}

	// Back to the rim, a dribble or two to back his man down.
	private backDown(pid: number, t: number, dir: 1 | -1): number {
		const at = this.posOf(pid);
		const to = clampPt({ x: at.x + dir * 2.2, y: at.y + (25 - at.y) * 0.15 });
		this.hold(pid, t, "dribble");
		const done = this.go(pid, to, t + 60, 2.6, "post", -dir as 1 | -1);
		const guard = this.defenderOf(pid);
		if (guard !== undefined) {
			this.go(
				guard,
				clampPt({ x: to.x + dir * 1.5, y: to.y }),
				t + 60,
				2.6,
				"back",
				-dir as 1 | -1,
			);
		}
		return Math.max(done, this.hold(pid, done, "hold"));
	}

	// The lob's set-up: cut to the inbound, Y out of bounds with the ball near
	// the frontcourt, X on the far wing - and X breaks for the rim.
	private setUpLob(
		team: Side,
		shooter: number,
		lobber: number,
		t: number,
	): number {
		this.setOffense(t, team);
		const tc = t + 120;
		this.cutAt(tc);
		const dir = attackDir(team);
		const rim = rimPt(team);
		const off = this.slots(team);
		const def = this.slots(other(team));
		const spots = this.setSpots(team, 0);
		off.forEach((pid, j) => {
			const s =
				pid === lobber
					? { x: rim.x - dir * 21, y: -1.4 }
					: pid === shooter
						? clampPt({ x: rim.x - dir * 17, y: 41 })
						: (spots[j] ?? spots[0]!);
			this.place(pid, s, tc, dir);
		});
		def.forEach((pid, j) => {
			const man = this.posOf(off[j] ?? off[0]!);
			this.place(pid, guardSpot(team, man, 0.25), tc, -dir as 1 | -1);
		});
		this.lookAt(lobber, tc + 1, { x: rim.x, y: rim.y });
		this.hold(lobber, tc, "hold");
		this.motionTeam = team;
		this.motion = 0;
		return this.go(
			shooter,
			clampPt({ x: rim.x - dir * 4.2, y: 25 + 2.4 }),
			tc + 650,
			SPRINT,
			"run",
			dir,
		);
	}

	// His man: position against position, unless a switch changed it.
	private defenderOf(pid: number): number | undefined {
		const team = this.teamOf(pid);
		const g = this.guarding.get(pid);
		if (g !== undefined && this.lineup[other(team)].includes(g)) {
			return g;
		}
		const j = this.slots(team).indexOf(pid);
		const def = this.slots(other(team));
		return def[j] ?? def[0];
	}

	// A set run to its shot: the steps it takes for the option to open, the
	// read's own moves, the last pass. Returns when the shooter is set to go
	// up, how he gets it off, and - for an alley-oop - who throws the lob.
	private playToShot(
		run: Running,
		t: number,
		plan: ShotPlan,
	): { t: number; style: ShotStyle; lob?: number } {
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
		const dunk =
			o.zone === "rim" && (plan.finish === "dunk" || plan.finish === "poster");
		if (this.holder !== shooter) {
			const from = this.holder ?? passer ?? shooter;
			if (
				dunk &&
				from !== shooter &&
				(o.kind === "alley_oop" || o.pass === "lob")
			) {
				// An alley-oop: he heads for the rim and the lob meets him up there.
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
				t = this.passTo(from, shooter, t, passStyleOf(o.pass, this.rng));
			}
		}
		const P = this.posOf(shooter);
		const out = dist(P, rim);
		if (o.zone === "rim" && out > 4.5) {
			// Caught short of the rim: one hard dribble and up.
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
		} else if (o.kind === "step_back") {
			// Into his man, then the hop back to where he shoots from.
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
			t = this.go(shooter, P, t + 40, 9, "back", dir);
		}
		t = Math.max(t, this.hold(shooter, t, "hold"));
		return {
			t,
			style:
				o.kind === "fadeaway" ? "fade" : o.kind === "hook" ? "hook" : "plain",
		};
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
	} {
		const dir = attackDir(team);
		const rim = rimPt(team);
		const close = zone === "atRim" || zone === "tipIn" || zone === "putBack";
		const putback = zone === "putBack" || zone === "tipIn";
		const P = putback
			? clampPt({ x: rim.x - dir * this.rand(2, 4), y: 25 + this.rand(-3, 3) })
			: this.shotSpot(team, zone, heaveSecs);

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
		if (lob !== undefined) {
			t = this.setUpLob(team, shooter, lob, t);
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
			} else {
				let handler = this.holder ?? this.slots(team)[0]!;
				// Who sets him up: the real assister on a make; on a miss, the handler
				// about half the time, so a pass never gives the result away.
				let passer =
					plan.assist ??
					(plan.kind !== "make" && handler !== shooter && this.rng() < 0.55
						? handler
						: undefined);
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
					const arrive = this.go(shooter, P, t, RUN, "run");
					const d = dist(this.posOf(handler), P);
					const send = Math.max(t, arrive - passMs(d) - 120);
					const caught = this.passTo(handler, shooter, send);
					t = Math.max(arrive, caught);
					// An entry pass to the post: he backs his man down first.
					if (zone === "lowPost" && this.rng() < 0.6) {
						style = "post";
						t = this.backDown(shooter, t, dir);
					}
				} else {
					if (handler !== shooter) {
						t = this.passTo(handler, shooter, t);
					}
					// His own shot: a move to get it.
					const r = this.rng();
					style =
						zone === "atRim"
							? r < 0.35
								? "crossover"
								: r < 0.62 && plan.finish === "layup"
									? "euro"
									: "plain"
							: zone === "lowPost"
								? "post"
								: zone === "midRange"
									? r < 0.3
										? "stepBack"
										: r < 0.5
											? "fade"
											: "plain"
									: zone === "three" && heaveSecs === undefined && r < 0.3
										? "stepBack"
										: "plain";
					t = this.driveTo(shooter, P, t, dir, style);
					t = Math.max(t, this.hold(shooter, t, "hold"));
				}
			}
		}

		// A three goes up from behind the line: a man a step in front of it,
		// or right on it, steps back out first.
		if (zone === "three" && heaveSecs === undefined) {
			const out = behindArc(team, this.posOf(shooter));
			if (out) {
				t = this.go(shooter, out, t, 9, "back", dir);
			}
		}

		// The defense: his man closes out, or the blocker / fouler gets there.
		const guard = this.defenderOf(shooter);
		const P1 = this.posOf(shooter);
		const toRim = { x: rim.x - P1.x, y: rim.y - P1.y };
		const len = Math.hypot(toRim.x, toRim.y) || 1;
		// Where his man meets the shot: between him and the rim - from the
		// side, where that would hide the shooter from the camera.
		const ahead = (k: number) =>
			clampPt(
				sideOn(
					P1,
					{
						x: P1.x + (toRim.x / len) * k,
						y: P1.y + (toRim.y / len) * k,
					},
					rim.x,
					1.9,
				),
			);

		const gather = t + 60;
		// A slam when the words say so: "throws it down", "blocked the dunk
		// attempt", "blows the dunk".
		const dunk =
			close &&
			plan.kind !== "foul" &&
			(plan.finish === "dunk" || plan.finish === "poster");
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
		const faceRim = (rim.x >= P1.x ? 1 : -1) as 1 | -1;
		const look = { x: rim.x, y: rim.y };

		if (dunk) {
			const dur = 1300;
			// A made dunk hangs on the rim; one that is stuffed or rattles out
			// comes straight back down.
			const hang = plan.kind === "make";
			const r = this.rng();
			const anim: AnimName =
				plan.finish === "poster"
					? r < 0.5
						? "dunk1"
						: "tomahawk"
					: r < 0.45
						? "dunk"
						: r < 0.8
							? "dunk1"
							: "tomahawk";
			// Dunked on: his man meets him at the rim, and loses.
			const victim = plan.defender;
			if (
				plan.finish === "poster" &&
				victim !== undefined &&
				this.teamOf(victim) !== team
			) {
				this.goBy(
					victim,
					clampPt({ x: rim.x - dir * 2.1, y: 25 + 0.7 }),
					gather - 400,
					gather + 200,
					"run",
					-faceRim as 1 | -1,
				);
				this.act(victim, "block", gather + 220, gather + 980, {
					face: -faceRim as 1 | -1,
					look: { ...P1 },
					jump: [0.1, 0.9, 2.3],
				});
			}
			// He goes up in front of the rim, on the side he comes from, close
			// enough that his hand goes over the front of it - and a make, he
			// hangs there by both hands.
			const back = unitVec(
				rim,
				P1.x === rim.x && P1.y === rim.y ? { x: rim.x - dir, y: rim.y } : P1,
			);
			const under = clampPt({
				x: rim.x + back.x * DUNK_FROM,
				y: rim.y + back.y * DUNK_FROM,
			});
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
			this.go(shooter, under, gather + 60, SPRINT, "run", faceRim);
			if (plan.kind === "block" && plan.blocker !== undefined) {
				// Met at the rim - off a lob, once he has it.
				const contact = Math.max(gather + dur * 0.42, caught + 100);
				const b = plan.blocker;
				this.goBy(
					b,
					clampPt({ x: rim.x - dir * 1.7, y: 25 - 1.1 }),
					gather - 300,
					contact - 300,
					"run",
					-faceRim as 1 | -1,
				);
				this.act(b, "block", contact - 330, contact + 420, {
					face: -faceRim as 1 | -1,
					look: { ...P1 },
					jump: [0.1, 0.9, 3.3],
				});
				this.fly(
					contact - 80,
					contact,
					{ pid: shooter },
					{ pid: b, hand: "near" },
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
		} else {
			// "Tips it in": a one-handed tap at the top of the jump.
			const tip = zone === "tipIn" && plan.finish === "tip";
			let anim: AnimName = tip ? "block" : close ? "layup" : "shoot";
			if (style === "post") {
				// A hook, or a turnaround fadeaway.
				anim = this.rng() < 0.55 ? "hook" : "fade";
			} else if (style === "fade") {
				anim = "fade";
			} else if (style === "hook") {
				anim = "hook";
			}
			const dur = close ? 760 : zone === "lowPost" ? 840 : 920;
			if (anim === "fade") {
				// Drifting back as he rises.
				this.go(
					shooter,
					clampPt({
						x: P1.x - (toRim.x / len) * 1.4,
						y: P1.y - (toRim.y / len) * 1.4,
					}),
					gather + dur * 0.2,
					4,
					"run",
					faceRim,
				);
			}
			const peak = tip
				? 2.8
				: close
					? 2.4
					: zone === "lowPost"
						? 0.9
						: zone === "midRange"
							? 1.5
							: 1.8;
			this.act(shooter, anim, gather, gather + dur, {
				face: faceRim,
				look,
				jump: [0.24, 0.93, peak],
			});
			if (close) {
				this.go(
					shooter,
					ahead(Math.min(2.2, len - 1.4)),
					gather + 40,
					RUN,
					"run",
					faceRim,
				);
			}
			const release = gather + dur * (close ? 0.6 : 0.55);
			const d = dist(P1, rim);
			const flight = close ? 300 : 620 + d * 22;
			if (plan.kind === "block" && plan.blocker !== undefined) {
				const contact = release + 110;
				const b = plan.blocker;
				this.goBy(
					b,
					ahead(Math.min(2.6, Math.max(1.4, len - 1))),
					gather - 300,
					release - 80,
					"run",
					-faceRim as 1 | -1,
				);
				this.act(b, "block", contact - 300, contact + 420, {
					face: -faceRim as 1 | -1,
					look: { ...P1 },
					jump: [0.1, 0.9, 2.7],
				});
				this.fly(release, contact, { pid: shooter }, { pid: b, hand: "near" });
				decided = contact;
				arrive = contact;
				target = { x: P1.x, y: P1.y, z: 9 };
			} else if (plan.kind === "foul") {
				const f = plan.fouler ?? guard;
				if (f !== undefined) {
					this.goBy(
						f,
						ahead(1.6),
						gather - 300,
						release - 120,
						"run",
						-faceRim as 1 | -1,
					);
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
				if (guard !== undefined) {
					const reach = dist(this.posOf(guard), P1) < 9;
					this.goBy(
						guard,
						ahead(close ? 1.8 : 2.4),
						gather - 200,
						release - 60,
						"run",
						-faceRim as 1 | -1,
					);
					if (reach) {
						this.act(guard, "contest", release - 260, release + 420, {
							face: -faceRim as 1 | -1,
							look: { ...P1 },
							jump: [0.15, 0.9, close ? 2 : 1.3],
						});
					}
				}
			}
		}

		// The bigs crash the glass while the ball is up.
		if ((!dunk || plan.kind === "miss") && plan.kind !== "block") {
			for (const side of [team, other(team)] as const) {
				const big = this.slots(side).at(-1);
				// Not the man contesting the shot: he is busy.
				if (
					big !== undefined &&
					big !== shooter &&
					big !== plan.fouler &&
					big !== guard
				) {
					const box = {
						x: rim.x - dir * this.rand(4, 7),
						y: 25 + (side === team ? -1 : 1) * this.rand(2, 5),
					};
					this.go(
						big,
						clampPt(box),
						Math.max(gather, this.free.get(big) ?? 0),
						JOG,
						"run",
						dir,
					);
				}
			}
		}
		return { gather, decided, arrive, target, dunk };
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
			const rp = this.posOf(r);
			const toward = { x: rp.x - rim.x, y: rp.y - rim.y };
			const l = Math.hypot(toward.x, toward.y) || 1;
			const k = blocked
				? this.rand(4, 7)
				: hard
					? this.rand(7, 11)
					: this.rand(4, 8);
			const catchAt = clampPt({
				x: rim.x + (toward.x / l) * k - (blocked ? dir * 2 : 0),
				y: rim.y + (toward.y / l) * k,
			});
			const catchT =
				t + (blocked ? 820 : hard ? this.rand(950, 1150) : this.rand(700, 900));
			const arrive = this.goBy(
				r,
				catchAt,
				t - 450,
				catchT - 380,
				"run",
				(rim.x >= catchAt.x ? 1 : -1) as 1 | -1,
			);
			const jumpStart = Math.max(arrive, catchT - 420);
			// Up for it, and once he lands, chinned - elbows out - a beat
			// before he looks up the floor.
			this.act(r, "board", jumpStart, jumpStart + REBOUND_MS, {
				face: (rim.x >= catchAt.x ? 1 : -1) as 1 | -1,
				look: { x: rim.x, y: rim.y },
				jump: [96 / REBOUND_MS, 704 / REBOUND_MS, blocked ? 1.2 : 2.4],
			});
			this.free.set(r, Math.max(this.free.get(r) ?? 0, jumpStart + REBOUND_MS));
			this.fly(t, catchT, from, { pid: r });
			// Somebody from the other side goes up for it too.
			const rival = this.slots(other(this.teamOf(r))).sort(
				(a, b) => dist(this.posOf(a), catchAt) - dist(this.posOf(b), catchAt),
			)[0];
			if (rival !== undefined && !blocked) {
				this.act(rival, "rebound", jumpStart + 60, jumpStart + 760, {
					look: { x: rim.x, y: rim.y },
					jump: [0.15, 0.9, 1.6],
				});
			}
			this.hold(r, catchT, "hold");
			return catchT;
		}
		if (next && next.e.type === "outOfBounds") {
			// Off a hand and out: over the baseline, or the sideline.
			const outY = from.y < COURT_H / 2 ? -1.8 : COURT_H + 1.8;
			const to =
				this.rng() < 0.4
					? { x: rim.x + dir * this.rand(6.6, 8), y: 25 + this.rand(-16, 16) }
					: { x: from.x - dir * this.rand(6, 16), y: outY };
			this.bounce(t, t + 1100, from, to, 2, blocked ? 1.6 : 3);
			return t + 1000;
		}
		// The period ran out, or nobody got it yet: it bounces free.
		const to = clampPt({
			x: rim.x - dir * this.rand(5, 10),
			y: 25 + this.rand(-8, 8),
		});
		this.bounce(t, t + 1000, from, to, 3, blocked ? 1.4 : 3.2);
		return t + 900;
	}

	// ---- beats ----------------------------------------------------------------

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
			};
			this.beat(i, type, shot.gather, shot.decided);
			this.phase = "set";
			return;
		}

		if (result && d !== undefined && typeof e.pid === "number") {
			const shooterTeam = result.kind === "block" ? other(d) : d;
			let pending = this.pending;
			// No attempt line before it (the sim never logs one for a putback):
			// shoot it now, in this beat's lead-in.
			if (!pending || (result.kind !== "block" && pending.pid !== e.pid)) {
				const shooter =
					result.kind === "block"
						? (this.holder ?? this.slots(shooterTeam)[0]!)
						: e.pid;
				const plan: ShotPlan = {
					kind: result.kind,
					assist: typeof e.pidAst === "number" ? e.pidAst : undefined,
					blocker: result.kind === "block" ? e.pid : undefined,
					fouler: typeof e.pidFoul === "number" ? e.pidFoul : undefined,
					finish: this.finishFor(e),
					defender: typeof e.pidDefense === "number" ? e.pidDefense : undefined,
				};
				const shot = this.stageShot(
					shooterTeam,
					shooter,
					result.zone,
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
					zone: result.zone,
					arrive: shot.arrive,
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
					const n = dev.run.play.steps.length - dev.run.from;
					t = this.runSteps(
						dev.run,
						t,
						dev.run.from,
						dev.run.from + Math.floor(this.rng() * n) - 1,
					);
				}
				const victim =
					typeof e.pidShooting === "number"
						? e.pidShooting
						: (this.holder ?? this.slots(team)[0]!);
				const vp = this.posOf(victim);
				const toward = (vp.x >= this.posOf(fouler).x ? 1 : -1) as 1 | -1;
				const hit = this.goBy(
					fouler,
					clampPt({ x: vp.x - toward * 1.6, y: vp.y + 0.4 }),
					t,
					t + 380,
					"run",
					toward,
				);
				this.act(fouler, "reach", hit - 120, hit + 320, { face: toward });
				this.effect("whistle", hit, { call: "foul", at: vp, team });
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
				this.bounce(hits, hits + 700, target, land, 2, 2.5);
				this.beat(i, type, T, T + 1100);
				this.phase = "ft";
				break;
			}
			case "outOfBounds": {
				// The ball is already out if a miss sent it there; otherwise it
				// squirts off somebody in the half court.
				let t = T;
				if (this.holder !== undefined) {
					const h = this.posOf(this.holder);
					const outY = h.y < COURT_H / 2 ? -1.8 : COURT_H + 1.8;
					this.bounce(
						t + 100,
						t + 900,
						{ x: h.x, y: h.y, z: 3 },
						{ x: h.x + this.rand(-6, 6), y: outY },
						2,
						1.5,
					);
					t += 800;
				}
				const outOn: Side | undefined = d;
				const nextTeam = outOn === undefined ? this.offense : other(outOn);
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
				this.effect("whistle", T, { call: "stop", team: this.offense });
				this.deadBall(T);
				const ballX = this.ballAt.x;
				// The whistle, then the picture cuts to the huddles.
				const tc = T + 800;
				this.cutAt(tc);
				for (const t of [0, 1] as const) {
					const spots = huddleSpots(t);
					this.slots(t).forEach((pid, j) => {
						this.place(pid, spots[j] ?? spots[0]!, tc);
						this.lookAt(pid, tc + 1, { x: benchX(t), y: 1.6 });
					});
				}
				const over = tc + (type === "timeout" ? 1700 : 1300);
				this.beat(i, type, T, over);
				// Over the break, a look round the building.
				this.shots.push({ t0: tc, t1: over, kind: i % 3 ? "wide" : "crowd" });
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
				// Still in the huddles; the next possession cuts to the inbound
				// at half court.
				this.deadBall(T);
				this.inboundAt = { x: COURT_W / 2, y: -1.4 };
				this.phase = "inboundSide";
				this.beat(i, type, T, T + 1100);
				break;
			}
			case "injury": {
				const pid = e.pid as number;
				this.deadBall(T);
				this.effect("whistle", T + 200, {
					call: "stop",
					at: this.posOf(pid),
				});
				this.act(pid, "hurt", T + 200, T + 2000);
				this.beat(i, type, T + 200, T + 1800);
				this.phase = "inboundSide";
				break;
			}
			case "gameOver": {
				const winner: Side = this.score[0] > this.score[1] ? 0 : 1;
				this.deadBall(T);
				for (const t of [0, 1] as const) {
					this.slots(t).forEach((pid, j) => {
						const target = {
							x: COURT_W / 2 + (t === 0 ? -6 : 6) + (j - 2) * 2.5,
							y: 18 + j * 3.5,
						};
						this.go(pid, target, T + j * 90, WALK, "walk");
						if (t === winner) {
							this.act(pid, "celebrate", T + 900 + j * 80, T + 3600);
						}
					});
				}
				this.effect("cheer", T, { team: winner });
				this.beat(i, type, T, T + 3600);
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
						this.lookAt(pid, at, { x: benchX(t), y: 1.6 });
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
				this.act(pid, "shoot", T + 100, T + 1020, {
					face: 1,
					look: { x: rimX(1), y: 25 },
					jump: [0.24, 0.93, 1.6],
				});
				const release = T + 100 + 920 * 0.55;
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
		},
		at: number,
	) {
		const team = shot.team;
		const rim = rimPt(team);
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
			this.fly(t0, t0 + 140, top, under);
			const settle = clampPt({
				x: rim.x - dir * this.rand(1.5, 4),
				y: 25 + this.rand(-3, 3),
			});
			// Out of the net and down to the floor, two bounces, and rolling.
			this.bounce(t0 + 140, t0 + 2400, under, settle, 2, 2.2);
			this.effect("swish", t0 + 20, { rim: team });
			this.effect("cheer", t0 + 60, { team });
			let end = at + 1100;
			if (typeof e.pidFoul === "number") {
				this.effect("whistle", at + 120, {
					call: "andOne",
					at: this.posOf(shot.pid),
					team,
				});
				this.act(e.pidFoul, "reach", at - 150, at + 300);
				end = at + 1400;
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
			if (big || this.rng() < 0.3) {
				const cel: AnimName = shot.dunk
					? this.rng() < 0.5
						? "flex"
						: "celebrate"
					: andOne
						? "flex"
						: "point";
				this.act(shot.pid, cel, free + 100, free + 900, {
					face: -dir as 1 | -1,
				});
			}
			this.beat(i, e.type, at, end);
			this.offense = other(team);
			this.phase = typeof e.pidFoul === "number" ? "ft" : "inboundBase";
			return;
		}
		if (kind === "miss") {
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
		// Blocked: it comes off his hand back toward the shooter and down.
		this.effect("block", at, { team: other(team) });
		const sp = this.posOf(shot.pid);
		const down = {
			...clampPt({
				x: sp.x - dir * this.rand(2, 4),
				y: sp.y + this.rand(-3, 3),
			}),
			z: 0.3,
		};
		this.fly(at, at + 300, { pid: e.pid, hand: "near" }, down);
		const next = this.afterMiss(at + 300, down, team, i, true);
		this.beat(i, e.type, at, next);
		this.phase = "loose";
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
			// Cut to the lineup: the defense on the blocks, the shooter at the
			// line, the official at the side of the lane with the ball.
			const tc = T + 150;
			this.cutAt(tc);
			def.forEach((pid, j) => {
				const [dd, ac] = j < 3 ? FT_DEFENSE[j]! : FT_BACK[j - 3]!;
				this.place(pid, spot(team, dd, ac), tc, -dir as 1 | -1);
				this.lookAt(pid, tc + 1, j < 3 ? rimSpot : line);
			});
			off.forEach((pid, j) => {
				const [dd, ac] = j < 2 ? FT_OFFENSE[j]! : FT_BACK[j]!;
				this.place(pid, spot(team, dd, ac), tc, dir);
				this.lookAt(pid, tc + 1, j < 2 ? rimSpot : line);
			});
			this.place(shooter, line, tc, dir);
			this.lookAt(shooter, tc + 1, rimSpot);
			this.rest(tc, official);
			ready = tc + 300;
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
		const dribbles = 1 + (Math.abs(shooter * 7 + 3) % 2);
		const bounce0 = caught + 160;
		this.hold(shooter, bounce0, "dribble");
		const set = bounce0 + (dribbles * 1000) / DRIBBLE_RATE;
		this.hold(shooter, set, "hold");
		const shotAt = set + 260;
		this.act(shooter, "shoot", shotAt, shotAt + 900, {
			face: dir,
			look: rimSpot,
		});
		const release = shotAt + 900 * 0.55;
		const target = made
			? rimPt(team, 0.35)
			: {
					x: rimX(team) - dir * (RIM_R + 0.05),
					y: 25 + this.rand(-0.4, 0.4),
					z: RIM_Z + 0.12,
				};
		const at = release + 720;
		this.fly(release, at, { pid: shooter }, target);
		this.act(shooter, "follow", shotAt + 900, at + 260, {
			face: dir,
			look: rimSpot,
		});
		// On the lane: hands on their knees, until the last one - then set to
		// box out.
		for (const pid of [...def.slice(0, 3), ...off.slice(0, 2)]) {
			this.act(pid, more ? "crouch" : "stance", Math.max(T, ready), release, {
				look: rimSpot,
			});
		}
		if (made && more) {
			// A teammate comes over to slap hands, and goes back to the lane.
			const mate = off[0];
			if (mate !== undefined) {
				const home = this.posOf(mate);
				const meet = {
					x: home.x + (S.x - home.x) * 0.62,
					y: home.y + (S.y - home.y) * 0.62,
				};
				const met = this.go(mate, meet, at + 120, WALK * 1.6, "walk");
				this.act(mate, "highFive", met, met + 420, { look: S });
				this.act(shooter, "highFive", met, met + 420, { look: meet });
				this.go(mate, home, met + 420, WALK * 1.6, "walk");
				this.lookAt(mate, met + 421, rimSpot);
				this.lookAt(shooter, met + 421, rimSpot);
			}
		}
		if (made) {
			const top = rimPt(team, 0.35);
			this.fly(at, at + 140, top, rimPt(team, -2.3));
			this.bounce(
				at + 140,
				at + 800,
				rimPt(team, -2.3),
				{ x: rimX(team) - dir * 2.5, y: 25 + this.rand(-2, 2) },
				2,
				1.8,
			);
			this.effect("swish", at + 20, { rim: team });
			this.beat(i, e.type, at, at + 700);
			this.phase = more ? "ft" : "inboundBase";
			if (!more) {
				this.offense = other(team);
			}
		} else if (more) {
			this.effect("clank", at, { rim: team });
			this.bounce(
				at,
				at + 700,
				target,
				{ x: rimX(team) - dir * 3, y: 25 + this.rand(-4, 4) },
				2,
				1.6,
			);
			this.beat(i, e.type, at, at + 650);
			this.phase = "ft";
		} else {
			this.effect("clank", at, { rim: team });
			const nextT = this.afterMiss(at, target, team, i, false);
			this.beat(i, e.type, at, nextT);
			this.phase = "loose";
		}
	}

	private lastFtShooter: number | undefined;

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
		const vp = this.posOf(victim);
		const dir = attackDir(team);
		if (stl) {
			const thief = e.pid as number;
			const toward = (vp.x >= this.posOf(thief).x ? 1 : -1) as 1 | -1;
			const hit = this.goBy(
				thief,
				clampPt({ x: vp.x - dir * 1.4, y: vp.y + 0.6 }),
				t,
				t + 420,
				"run",
				toward,
			);
			this.act(thief, "reach", hit - 150, hit + 300, { face: toward });
			if (e.outOfBounds) {
				const outY = vp.y < COURT_H / 2 ? -1.8 : COURT_H + 1.8;
				this.bounce(
					hit,
					hit + 900,
					{ x: vp.x, y: vp.y, z: 2.5 },
					{ x: vp.x + this.rand(-6, 6), y: outY },
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
				this.hold(thief, hit + 160, "dribble");
				this.setOffense(hit, other(team));
				this.phase = "loose";
				this.beat(i, e.type, hit, hit + 650);
			}
			return;
		}
		// A turnover with no steal: thrown away out of bounds, or just lost.
		if (e.outOfBounds) {
			const outY = vp.y < COURT_H / 2 ? -2.2 : COURT_H + 2.2;
			const to = { x: vp.x + dir * this.rand(6, 14), y: outY };
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
			this.inboundAt = { x: to.x, y: outY < 0 ? -1.4 : COURT_H + 1.4 };
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
			if (ud !== undefined) {
				const u = unitVec(S, this.posOf(ud));
				hit = this.goBy(
					ud,
					clampPt({ x: S.x + u.x * 1.4, y: S.y + u.y * 1.4 }),
					end - 350,
					end + 150,
					"run",
				);
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
			this.turnOver(team, S);
			this.beat(i, e.type, hit + 80, hit + 950);
			return true;
		}
		if (risk.kind === "pass") {
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
			if (to === undefined) {
				return false;
			}
			const A = this.posOf(victim);
			const B = this.posOf(to);
			const d = dist(A, B);
			const flight = passMs(d);
			const over = d >= 22;
			const wind = RELEASE_MS + (over ? OVERHEAD_WIND : 0);
			const start = Math.max(
				t,
				this.free.get(victim) ?? 0,
				(this.free.get(to) ?? 0) + 40 - flight - wind,
			);
			const release = start + wind;
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
			if (thief !== undefined) {
				// He reads it and jumps the lane.
				const T0 = this.posOf(thief);
				const along =
					d > 0.1
						? ((T0.x - A.x) * (B.x - A.x) + (T0.y - A.y) * (B.y - A.y)) /
							(d * d)
						: 0.6;
				const f = Math.min(0.85, Math.max(0.35, along));
				const I = clampPt({
					x: A.x + (B.x - A.x) * f,
					y: A.y + (B.y - A.y) * f,
				});
				const tI = release + flight * f;
				this.goBy(thief, I, start - 250, tI - 40, "run");
				if (!oob) {
					this.fly(release, tI, { pid: victim }, { pid: thief });
					this.act(thief, "catch", tI - 90, tI + 110, { look: A });
					this.hold(thief, tI, "dribble");
					this.setOffense(tI, other(team));
					this.phase = "loose";
					this.beat(i, e.type, tI, tI + 650);
					return true;
				}
				// Got a hand on it - and it is gone out of bounds: on along the
				// pass, or, if that is the length of the floor away, off the
				// nearest line.
				const I3 = { ...I, z: 3.6 };
				this.act(thief, "reach", tI - 150, tI + 250, { look: A });
				this.fly(release, tI, { pid: victim }, I3);
				const onward = this.outPoint(I, unitVec(A, I));
				const out = dist(I, onward) <= 24 ? onward : this.nearestOut(I);
				this.bounce(tI, tI + 900, I3, out, 2, 1.4);
				this.effect("whistle", tI + 800, {
					call: "out",
					at: out,
					team: other(team),
				});
				this.turnOver(team, out);
				this.beat(i, e.type, tI, tI + 1000);
				return true;
			}
			// Thrown away: over his head and out.
			const out = this.outPoint(B, u);
			const tOut = release + passMs(dist(A, out));
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
		if (risk.kind === "lost") {
			// Stripped on the way.
			const S = clampPt({
				x: A.x + (D.x - A.x) * 0.55,
				y: A.y + (D.y - A.y) * 0.55,
			});
			const tS = this.go(victim, S, start, 15, "dribble", dir);
			const who = thief ?? this.defenderOf(victim);
			if (who !== undefined) {
				const u = unitVec(S, rim);
				const P = clampPt({ x: S.x + u.x * 1.5, y: S.y + u.y * 1.5 });
				const near = dist(this.posOf(who), P) < 9;
				this.goBy(
					who,
					P,
					start,
					tS - 60,
					near ? "slide" : "run",
					near ? (-dir as 1 | -1) : undefined,
				);
				this.act(who, "reach", tS - 160, tS + 260, { look: S });
			}
			if (thief !== undefined && !oob) {
				const side = this.rng() < 0.5 ? 1 : -1;
				const u = unitVec(A, D);
				const loose = clampPt({
					x: S.x + u.x * 2.5 - u.y * side * 3,
					y: S.y + u.y * 2.5 + u.x * side * 3,
				});
				this.bounce(tS, tS + 600, { ...S, z: 2 }, loose, 1, 1.2);
				const got = this.pickUp(thief, tS + 120, SPRINT, "dribble");
				this.setOffense(tS, other(team));
				this.phase = "loose";
				this.beat(i, e.type, tS, got + 450);
				return true;
			}
			const out = this.nearestOut(S);
			this.bounce(tS, tS + 800, { ...S, z: 1.5 }, out, 2, 1.2);
			this.effect("whistle", tS + 700, {
				call: "out",
				at: out,
				team: other(team),
			});
			this.turnOver(team, out);
			this.beat(i, e.type, tS, tS + 1000);
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
			this.act(helper, "fall", hit - 60, hit + 900, { look: A });
			this.hold(victim, hit, "hold");
			this.effect("whistle", hit + 120, {
				call: "offensive",
				at: E,
				team: other(team),
			});
			this.turnOver(team, E);
			this.beat(i, e.type, hit + 120, hit + 1000);
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
		this.turnOver(team, G);
		this.beat(i, e.type, tt + 100, tt + 900);
		return true;
	}

	private beatJumpBall(e: RawEvent, i: number) {
		const T = this.T;
		const winnerTeam: Side = e.t === 0 ? 1 : 0;
		const jumper = e.pid as number;
		const loser = e.pid2 as number;
		const c = { x: COURT_W / 2, y: COURT_H / 2 };
		const side = (t: Side) => (t === 1 ? -1 : 1); // each team lines up on the side it defends
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
			const rest = this.slots(t).filter((p) => p !== j0);
			rest.forEach((pid, j) => {
				const a = ((j + 0.5) / rest.length) * Math.PI - Math.PI / 2;
				const target = {
					x: c.x + side(t) * (7 + Math.cos(a) * 3),
					y: c.y + Math.sin(a) * 9,
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
			});
		}
		// The opening tip: the picture starts on the whole building while
		// they take their places, then cuts in for the toss.
		const opening = this.beats.length === 0 && this.shots.length === 0;
		const toss = Math.max(T + 600, ready + 200, opening ? T + 3000 : 0);
		if (opening) {
			this.shots.push({ t0: T, t1: toss - 600, kind: "wide", stretch: false });
		}
		this.rest(T, { x: c.x, y: c.y, z: 5 });
		const apex = { x: c.x, y: c.y, z: 12.3 };
		this.effect("toss", toss);
		this.fly(toss, toss + 520, { x: c.x, y: c.y, z: 5 }, apex);
		this.act(jumper, "block", toss + 120, toss + 900, {
			face: attackDir(winnerTeam),
			jump: [0.1, 0.9, 2.8],
		});
		this.act(loser, "block", toss + 160, toss + 940, {
			face: attackDir(other(winnerTeam)),
			jump: [0.1, 0.9, 2.5],
		});
		const receiver = this.slots(winnerTeam).find((p) => p !== jumper) ?? jumper;
		this.fly(toss + 520, toss + 1000, apex, { pid: receiver });
		this.act(receiver, "catch", toss + 900, toss + 1080);
		this.hold(receiver, toss + 1000, "hold");
		this.setOffense(toss + 1000, winnerTeam);
		this.phase = "tip";
		this.motionTeam = undefined;
		this.beat(i, e.type, toss + 520, toss + 1300);
	}

	private beatSub(e: RawEvent, i: number, d: Side | undefined) {
		const T = this.T;
		const team: Side = d ?? 0;
		const on: number[] = Array.isArray(e.pids) ? e.pids : [];
		const off: number[] = Array.isArray(e.pidsOff) ? e.pidsOff : [];
		if (this.holder !== undefined && off.includes(this.holder)) {
			this.deadBall(T);
		}
		off.forEach((pid, j) => {
			const at = this.posOf(pid);
			const incoming = on[j];
			// Back to his chair, where he sits down.
			const gone = this.go(pid, this.seatOf(pid), T + j * 80, WALK, "walk");
			this.show(pid, gone, false);
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
						this.pos.set(incoming, this.seatOf(incoming));
					}
					tr.shown = tr.shown.filter(([ts, on]) => on || ts <= t0);
					this.free.set(incoming, t0);
					this.show(incoming, t0, true);
					this.go(incoming, at, t0, RUN * 0.8, "run");
				}
			}
		});
		this.lineup[team] = [
			...this.lineup[team].filter((p) => !off.includes(p)),
			...on.filter((p) => !this.lineup[team].includes(p)),
		];
		this.beat(i, e.type, T, T + 1300);
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
			}
			this.lineup[t] = [
				...this.lineup[t].filter((p) => !off.includes(p)),
				...on,
			];
		}
	}

	noteClock(e: RawEvent) {
		if (typeof e.clock === "number") {
			this.lastClock = e.clock;
		}
	}

	// OFF THE BALL, ON THE MOVE.
	//
	// The set says where each man goes when his part in it comes. In
	// between, a man out on the perimeter while the ball is worked somewhere
	// else does not stand rooted to his spot for seconds on end: he drifts a
	// few feet along the arc - away from the teammate nearest him - and his
	// man slides with him. Never on his way into anything that needs him
	// where the set put him (a shot, a screen, a catch), never inside the
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
		// How long from t the ball stays in play: in a man's hands, or on its
		// way between two - and no free throw.
		const liveUntil = (a: number): number => {
			let i = Math.max(
				0,
				this.ball.findLastIndex((s) => s.t0 <= a),
			);
			let end = Infinity;
			for (; i < this.ball.length; i++) {
				const s = this.ball[i]!;
				if (
					!(
						s.kind === "hold" ||
						(s.kind === "fly" && "pid" in s.from && "pid" in s.to)
					)
				) {
					end = Math.max(a, s.t0);
					break;
				}
			}
			for (const bt of this.beats) {
				if (
					(bt.type === "ft" || bt.type === "missFt") &&
					bt.end > a &&
					bt.preStart < end
				) {
					end = Math.max(a, bt.preStart);
				}
			}
			return end;
		};
		// Things a man does where the set put him to do them: a shot, a
		// screen, a post-up, a contest of the shot.
		const PLANTED = new Set<AnimName>([
			"screen",
			"postUp",
			"shoot",
			"fade",
			"hook",
			"layup",
			"dunk",
			"dunk1",
			"tomahawk",
			"contest",
			"block",
		]);
		type Still = {
			from: number;
			to: number;
			at: Pt;
			// His next run, and whether before it he does anything where the
			// set put him.
			next?: number;
			planted: boolean;
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
				})),
				...tr.acts.map((a) => ({
					t0: a.t0,
					t1: a.t1,
					move: undefined,
					planted: PLANTED.has(a.anim),
				})),
			].sort((a, b) => a.t0 - b.t0 || (a.move === undefined ? 1 : -1));
			const out: Still[] = [];
			let end = -Infinity;
			let at: Pt = tr.start;
			busy.forEach((b, i) => {
				if (b.t0 > end && Number.isFinite(end)) {
					let k = i;
					let planted = false;
					while (k < busy.length && busy[k]!.move === undefined) {
						planted ||= busy[k]!.planted;
						k++;
					}
					out.push({
						from: end,
						to: b.t0,
						at,
						next: busy[k]?.move,
						planted,
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
		const setOff = (tr: Track, next: number | undefined, from: Pt) => {
			const m = next === undefined ? undefined : tr.moves[next];
			if (m && (m.t1 - m.t0 > 1 || dist(m.from, m.to) > 0.01)) {
				m.from = { ...from };
			}
		};
		const all = [...this.tracks.values()];
		const still = new Map(all.map((tr) => [tr.pid, stills(tr)]));
		const shownAt = (tr: Track, a: number, b: number) =>
			atTime(tr.shown, a, false) &&
			!tr.shown.some(([t0, on]) => t0 > a && t0 < b && !on);
		// One drift before any run of his: that run starts from where the
		// drift left him.
		const taken = new Set<string>();
		const take = (tr: Track, w: Still) => taken.add(`${tr.pid}:${w.next}`);
		const free = (tr: Track, w: Still) =>
			w.next !== undefined && !w.planted && !taken.has(`${tr.pid}:${w.next}`);
		const added: { tr: Track; move: Move }[] = [];
		for (const tr of all) {
			for (const w of still.get(tr.pid)!) {
				if (w.to - w.from < 1500 || !free(tr, w)) {
					continue;
				}
				// The part of it with the ball in play.
				const until = Math.min(w.to, liveUntil(w.from));
				const team = atTime(this.poss, w.from, 1 as Side);
				if (
					until - w.from < 1500 ||
					team !== tr.team ||
					this.poss.some(([t0]) => t0 > w.from && t0 < until) ||
					!shownAt(tr, w.from, w.to)
				) {
					continue;
				}
				const rim = { x: rimX(team), y: COURT_H / 2 };
				const P = w.at;
				// Out on the perimeter, clear behind the line, in the frontcourt.
				if (
					behindArc(team, P) !== undefined ||
					Math.abs(P.x - rim.x) > COURT_W / 2 - 6
				) {
					continue;
				}
				const u = unitVec(rim, P);
				const L = this.rand(2, 4.2);
				const mates = all
					.filter(
						(o) =>
							o !== tr && o.team === tr.team && atTime(o.shown, w.from, false),
					)
					.map((o) => {
						const m = o.moves.findLast((x) => x.t0 <= w.from);
						return m ? m.to : o.start;
					});
				const room = (q: Pt) =>
					Math.min(Infinity, ...mates.map((m) => dist(m, q)));
				const ends = [1, -1].map((side) => {
					const q = clampPt({
						x: P.x - u.y * side * L,
						y: P.y + u.x * side * L,
					});
					return behindArc(team, q) ?? q;
				});
				const Q = room(ends[0]!) >= room(ends[1]!) ? ends[0]! : ends[1]!;
				const d = dist(P, Q);
				const dur = Math.max(300, (d / 6.5) * 1000);
				const t0 = w.from + 400 + this.rng() * 700;
				if (d < 1 || t0 + dur + 250 > until) {
					continue;
				}
				const move: Move = {
					t0,
					t1: t0 + dur,
					from: { ...P },
					to: Q,
					anim: "drift",
				};
				added.push({ tr, move });
				setOff(tr, w.next, Q);
				take(tr, w);
				// His man goes with him.
				const step = { x: (Q.x - P.x) * 0.85, y: (Q.y - P.y) * 0.85 };
				const s0 = t0 + 120;
				let best: { tr: Track; w: Still } | undefined;
				for (const o of all) {
					if (o.team === tr.team || !atTime(o.shown, s0, false)) {
						continue;
					}
					const ow = still
						.get(o.pid)!
						.find((x) => x.from <= s0 && x.to >= s0 + dur + 200 && free(o, x));
					if (
						ow &&
						dist(ow.at, P) < 10 &&
						(!best || dist(ow.at, P) < dist(best.w.at, P))
					) {
						best = { tr: o, w: ow };
					}
				}
				if (best) {
					const D = best.w.at;
					const to = clampPt({ x: D.x + step.x, y: D.y + step.y });
					added.push({
						tr: best.tr,
						move: { t0: s0, t1: s0 + dur, from: { ...D }, to, anim: "slide" },
					});
					setOff(best.tr, best.w.next, to);
					take(best.tr, best.w);
				}
			}
		}
		for (const { tr, move } of added) {
			tr.moves.push(move);
		}
		for (const tr of all) {
			tr.moves.sort((a, b) => a.t0 - b.t0);
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
		this.liven();
		this.fx.sort((a, b) => a.t - b.t);
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
		return {
			tracks: this.tracks,
			ball: this.ball,
			fx: this.fx,
			beats: this.beats,
			poss: this.poss,
			cuts: this.cuts,
			shots,
			tension: this.tension,
			end: this.T,
		};
	}
}

export const compileCourt = ({
	events,
	players,
	gid,
	gender = "male",
}: {
	events: RawEvent[];
	players: CourtPlayer[];
	// The game, which seeds everything the sim leaves open (where the shooter
	// stood, who boxed out) and picks the play-by-play's wording.
	gid: number | undefined;
	gender?: "female" | "male";
}): CourtTimeline => {
	const d = new Director(events, players, gid, gender);
	for (let i = 0; i < events.length; i++) {
		const e = events[i];
		if (!e || typeof e.type !== "string") {
			continue;
		}
		if (!isLineItem(e)) {
			d.trackScore(e);
			continue;
		}
		d.handle(e, i);
		d.noteClock(e);
	}
	return d.finish();
};

// Where the animation should stand while the playback cursor (events consumed)
// is at `cursor`: the moment the next unshown line happens. Past the last line,
// the end of the game.
export const targetForCursor = (tl: CourtTimeline, cursor: number): number => {
	const beat = tl.beats.find((b) => b.i >= cursor);
	return beat ? beat.actionStart : tl.end;
};

// Where to cut to when playback jumps (a rewind, a fast-forward, joining a
// broadcast late): the end of the last line already shown.
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
