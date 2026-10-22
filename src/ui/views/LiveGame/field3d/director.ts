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
	FIELD_L,
	FIELD_W,
	HASH_FAR_Y,
	HASH_NEAR_Y,
	MID_Y,
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
import { actOut } from "./action.ts";
import { stageCrew, type CrewPart } from "./crew.ts";

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
// and his team (raw: 0 the home team).
export type StatLine = { s: string; pid: number; t: number };

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
};

// How a man has the ball: under his hand on the turf over it (a center or
// a long snapper), tucked away under his arm, in both hands, cocked back in
// his throwing hand.
export type BallHold = "snap" | "carry" | "hands" | "throw";

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
	parts: Part[];
	// The officials (see crew.ts).
	crew: CrewPart[];
	ball: BallSeg[];
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
// going one after another over a moment.
const BREAK_AT = 500;
const BREAK_SPREAD = 350;
// How far back to the huddle (feet) a man is when the picture takes up the
// play - from no farther than this off his spot, at an easy jog, or at a
// walk for a short way.
const HUDDLE_BACK = 27;
const DEF_HUDDLE_BACK = 30;
const WALK_UP_MAX = 26;
const JOG_FPS = 9;
const WALK_FPS = 4.6;
const WALK_ONLY = 10;
// Set this long after the last man is, before the snap; and the cadence
// from there (shortest, longest).
const SET_HOLD = 450;
const CADENCE = [700, 1500] as const;
// A kickoff: everyone lined up, the kicker off on his run-up this long in.
const KICK_AT = 2600;
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

const clampY = (y: number, lo = 4, hi = FIELD_W - 4) =>
	Math.min(hi, Math.max(lo, y));

// Never back past the end line: a punter on his own 2 stands at the back of
// the end zone, not out of it.
const clampX = (x: number, margin = 3) =>
	Math.min(FIELD_L - margin, Math.max(margin, x));

// Where the ball is spotted after a play that ended in the field of play:
// on the nearer hash, or where it was if it ended between them.
const HASHES = [HASH_FAR_Y, HASH_NEAR_Y] as const;
const spotBetweenHashes = (r: () => number): number => {
	const u = r();
	return u < 0.35
		? HASHES[0]
		: u < 0.7
			? HASHES[1]
			: HASHES[0] + (HASHES[1] - HASHES[0]) * r();
};

const ORDINALS = ["1st", "2nd", "3rd", "4th"];
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

	constructor({ events, players, gid, numPeriods = 4 }: FieldInput) {
		this.events = events;
		this.gid = gid ?? 0;
		this.numPeriods = numPeriods;
		const init = events.find((e) => e?.type === "init");
		this.depth = depthCharts(init, players);
		this.roster = [
			players.filter((p) => p.team === 0).map((p) => p.pid),
			players.filter((p) => p.team === 1).map((p) => p.pid),
		];
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

	// Who did what in a play, by its stats - on a two-point try, by the
	// names in its lines.
	statsOf(i0: number, i1: number): StatLine[] {
		const out: StatLine[] = [];
		let isTry = false;
		let onside: RawEvent | undefined;
		for (let i = i0; i < i1; i++) {
			const e = this.events[i];
			if (e?.type === "stat" && typeof e.pid === "number") {
				out.push({ s: e.s, pid: e.pid, t: e.t });
			} else if (e?.type === "twoPointConversion") {
				isTry = true;
			} else if (e?.type === "onsideKickRecovery") {
				onside = e;
			}
		}
		if (onside) {
			return this.onsideStats(out, onside);
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
							out.push({ s, pid, t });
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
	onsideStats(stats: StatLine[], e: RawEvent): StatLine[] {
		const t = e.t === 1 ? 1 : 0;
		const name = Array.isArray(e.names) ? e.names[0] : undefined;
		const pid =
			stats.find((x) => x.s === "kr")?.pid ??
			(typeof name === "string"
				? this.byName[(1 - t) as Side].get(name)
				: undefined);
		const out = stats.filter((x) => !RETURN_STATS.has(x.s));
		if (pid !== undefined) {
			out.push({ s: "onsRec", pid, t });
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
		const scrimmage: number =
			typeof clock.scrimmage === "number" ? clock.scrimmage : 25;
		const losX = yardLineX(scrimmage, dir);
		const kick = kind === "kickoff" || kind === "onside";
		const ballY =
			kick || kind === "extraPoint"
				? MID_Y
				: clampY(this.ballY, HASH_FAR_Y, HASH_NEAR_Y);
		const t0 = this.T;
		this.cuts.push(t0);
		const r = this.rng(i0, "play");

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

		// Up to the line, set, and the snap.
		const parts: Part[] = [];
		let set = t0 + KICK_AT;
		if (kick) {
			for (const p of placed) {
				parts.push({
					pid: p.pid,
					team: p.team,
					role: p.role,
					legs: [
						{
							t0,
							t1: t0,
							from: p.at,
							to: p.at,
							move: p.move,
							yaw: p.yaw,
							...(p.mirrored ? { mirrored: true } : {}),
						},
					],
				});
			}
		} else {
			set = this.walkUp(placed, parts, offense, dir, losX, ballY, t0, r);
		}
		const snap = kick
			? t0 + KICK_AT
			: kind === "none"
				? set + 400
				: set + CADENCE[0] + (CADENCE[1] - CADENCE[0]) * r();

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
			ball.push({ kind: "held", t0: over.t0, pid: center.pid });
		}

		// The play itself, from the snap.
		const acted = ACTED.has(kind)
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
					r: this.rng(i0, "action"),
				})
			: undefined;
		if (acted) {
			ball.push(...acted.ball);
		}

		// The lines.
		this.lastLine = Math.max(this.lastLine, t0 - LINE_GAP);
		let afterSnap = false;
		let results = 0;
		let lastAt = t0;
		let pre = 0;
		for (let i = i0; i < i1; i++) {
			const e = events[i];
			if (!e) {
				continue;
			}
			if (e.type === "quarter") {
				this.quarter =
					typeof e.quarter === "number" ? e.quarter : this.quarter + 1;
			} else if (e.type === "overtime") {
				this.quarter =
					this.numPeriods + (typeof e.overtimes === "number" ? e.overtimes : 1);
			} else if (e.type === "injury" && typeof e.injuredPID === "number") {
				this.out.add(e.injuredPID);
			}
			if (!isLineItem(e)) {
				continue;
			}
			let want: number;
			if (i === i0) {
				want = t0 + CLOCK_AT;
			} else if (BEFORE_SNAP.has(e.type) && !afterSnap) {
				// (The kickoff team's onside call too, though it is the kick.)
				want = t0 + 900 + pre * 900;
				pre += 1;
				if (e.type === "onsideKick") {
					afterSnap = i >= kindAt;
				}
			} else if (e.type === "flag" && !afterSnap) {
				// Before the snap: a false start, offside.
				want = kindAt < 0 || i < kindAt ? set + 600 : snap + 1300;
				afterSnap = kindAt >= 0 && i > kindAt;
			} else if (acted?.at.has(i)) {
				want = acted.at.get(i)!;
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
		const t1 = Math.max(
			this.lastLine + TAIL,
			snap + 1500,
			acted ? acted.whistle + AFTER_WHISTLE : 0,
		);
		// Each line of the play ends where its own action does; the last one,
		// with the play.
		const last = this.beats.at(-1);
		if (last && last.i >= i0) {
			last.end = t1;
		}
		// And everybody stays as the play left him until the next one.
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
		// The officials, with the play.
		const minutes = typeof clock.clock === "number" ? clock.clock : 15;
		const crew = stageCrew({
			kind,
			offense,
			dir,
			losX,
			ballY,
			t0,
			set,
			snap,
			follow: acted?.follow ?? snap,
			whistle: acted ? acted.whistle : snap + 1200,
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
			r: this.rng(i0, "crew"),
		});
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
			parts,
			crew,
			ball,
		});
		this.T = t1;
		this.ballY = this.nextSpot(kind, i0, i1, ballY);
	}

	// Across the field, where the next snap is spotted.
	nextSpot(kind: PlayKind, i0: number, i1: number, ballY: number): number {
		const r = this.rng(i0, "spot");
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
			return moved ? spotBetweenHashes(r) : MID_Y;
		}
		if (kind === "extraPoint" || kind === "fieldGoal") {
			return MID_Y;
		}
		return moved ? spotBetweenHashes(r) : ballY;
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
			const mirrored = (a.mirrored === true) !== flip;
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
				...(a.role === "C" || a.role === "LS" ? { ball: "snap" as const } : {}),
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
				const leave =
					t0 + BREAK_AT + BREAK_SPREAD * r() + (isOffense ? 0 : 150);
				const arrive = leave + (d / (jog ? JOG_FPS : WALK_FPS)) * 1000;
				const look = Math.atan2(center.y - from.y, center.x - from.x);
				parts.push({
					pid: p.pid,
					team: p.team,
					role: p.role,
					legs: [
						keep > 0
							? { t0, t1: leave, from, to: from, move: "stand", yaw: p.yaw }
							: { t0, t1: leave, from, to: from, move: "hands", yaw: look },
						{
							t0: leave,
							t1: arrive,
							from,
							to: p.at,
							move: jog ? "jog" : "walk",
						},
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
