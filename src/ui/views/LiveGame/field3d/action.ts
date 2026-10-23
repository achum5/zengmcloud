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
	MID_Y,
	POST_W,
	YD,
	type Dir,
	type Pt,
	type Pt3,
	type Side,
} from "./geometry.ts";
import { moveKind, type Move } from "./poses.ts";
import { sidelineOf } from "./sideline.ts";
import {
	alongRoute,
	decoyRoute,
	layRoute,
	targetRoute,
	type Bounds,
	type OPt,
	type Route,
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
	r: () => number;
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
	// A big play's moment after it (see celebrate): when it is over.
	until?: number;
};

// ---- who is who ------------------------------------------------------------

const OL = new Set<Role>(["C", "LG", "RG", "LT", "RT"]);
const DL = new Set<Role>(["DE", "DT", "NT"]);
const LB = new Set<Role>(["SAM", "MIKE", "WILL", "BUCK"]);
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

const clamp = (x: number, lo: number, hi: number) =>
	Math.min(hi, Math.max(lo, x));

const onField = (p: Pt): Pt => ({
	x: clamp(p.x, 1, FIELD_L - 1),
	y: clamp(p.y, 1, FIELD_W - 1),
});

// ---- one man's part, a stretch at a time ------------------------------------

class Actor {
	readonly part: Part;
	readonly top: number;
	// The way he faces at the end of what he has been given so far.
	yaw: number;

	constructor(part: Part) {
		this.part = part;
		this.top = topOf(part.role);
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
		for (let k = legs.length - 1; k >= 0; k--) {
			const l = legs[k]!;
			if (t >= l.t0) {
				const u = l.t1 > l.t0 ? clamp((t - l.t0) / (l.t1 - l.t0), 0, 1) : 1;
				return {
					x: l.from.x + (l.to.x - l.from.x) * u,
					y: l.from.y + (l.to.y - l.from.y) * u,
				};
			}
		}
		return legs[0]!.from;
	}

	private push(l: Leg) {
		this.part.legs.push(l);
		this.yaw = yawOf(l, this.yaw);
	}

	// Where he is until t, in `move` (by default what he was doing, if it
	// was standing in something) facing `yaw` (the way he was).
	hold(t: number, move?: Move, yaw?: number, extra?: Partial<Leg>) {
		const l = this.last;
		if (t <= l.t1) {
			return;
		}
		const m = move ?? (moveKind(l.move) === "still" ? l.move : "stand");
		const still = l.from.x === l.to.x && l.from.y === l.to.y;
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
		this.push({
			t0: l.t1,
			t1: t,
			from: l.to,
			to: l.to,
			move: m,
			yaw: yaw ?? this.yaw,
			...(l.mirrored && m === l.move ? { mirrored: true } : {}),
			...extra,
		});
	}

	// To `to`, getting there at t1, in `move` (by default by how fast that
	// is), facing `yaw` (by default the way he goes).
	go(to: Pt, t1: number, move?: Move, yaw?: number, extra?: Partial<Leg>) {
		const l = this.last;
		const p = onField(to);
		const t0 = l.t1;
		const end = Math.max(t1, t0 + 1);
		const fps = dist(l.to, p) / ((end - t0) / 1000);
		this.push({
			t0,
			t1: end,
			from: l.to,
			to: p,
			move: move ?? gait(fps),
			...(yaw !== undefined ? { yaw } : {}),
			...extra,
		});
	}

	// To `to` at `fps`; when he gets there.
	run(to: Pt, fps: number, move?: Move, yaw?: number): number {
		const t1 = this.t + (dist(this.at, onField(to)) / Math.max(1, fps)) * 1000;
		this.go(to, t1, move, yaw);
		return this.t;
	}

	// Toward `to` by t1, at no more than `fps`: there, or as far as that
	// gets him.
	chase(to: Pt, t1: number, fps: number, move?: Move, yaw?: number) {
		const p = onField(to);
		const from = this.at;
		const far = dist(from, p);
		const room = (fps * Math.max(0, t1 - this.t)) / 1000;
		const u = far > room ? room / far : 1;
		this.go(
			{ x: from.x + (p.x - from.x) * u, y: from.y + (p.y - from.y) * u },
			t1,
			move,
			yaw,
		);
	}

	// An act where he is (or going a little way), over t0..t1.
	act(move: Move, t1: number, yaw?: number, to?: Pt) {
		const l = this.last;
		this.push({
			t0: l.t1,
			t1: Math.max(t1, l.t1 + 1),
			from: l.to,
			to: to ? onField(to) : l.to,
			move,
			yaw: yaw ?? this.yaw,
		});
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
		};
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
			l.t1 = t;
		}
		let yaw = legs[0]!.yaw ?? 0;
		for (const x of legs) {
			yaw = yawOf(x, yaw);
		}
		this.yaw = yaw;
	}
}

const yawOf = (l: Leg, before: number): number =>
	l.yaw ??
	(l.from.x !== l.to.x || l.from.y !== l.to.y
		? Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x)
		: before);

// ---- the ball ---------------------------------------------------------------

// How a man has the ball, by what he is doing: over it, a center; cocking
// it to throw; in both hands in the pocket, catching it, handing it off,
// taking a knee; otherwise tucked away.
const IN_HANDS = new Set<Move>([
	"qbSet",
	"handOff",
	"kneelDown",
	"catch",
	"scoop",
	"back",
	"slide",
	"walk",
	"shotgun",
	"underCenter",
	"stand",
	"punt",
	"holdKneel",
]);
// (Up over his head in his throwing hand, his arms doing something of
// their own: spiking it, holding it up.)
const IN_HAND = new Set<Move>(["throwWind", "spike", "ballUp", "leap"]);
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
const CATCH_Z = 4.3;
const G = 32;
const flightOf = (from: Pt, to: Pt): { ms: number; peak: number } => {
	const l = dist(from, to);
	const v = Math.min(66, 50 + l * 0.12);
	const s = l / v;
	return { ms: s * 1000, peak: Math.max(0.4, (G * s * s) / 8) };
};

// ---- the play ----------------------------------------------------------------

// After the whistle, how long a man down stays down, and getting up.
const DOWN_MS = 900;
const GET_UP_MS = 1000;
// Each man reads the play this long after the snap before he goes to the
// ball.
const READ_MS = 350;
// A defender sees the ball thrown and breaks on it this long after.
const BREAK_MS = 250;
// A tackler gets to the carrier this long before he has him down, coming
// at him at no more than most of his speed - all but flat out, if he has
// to.
const HIT_MS = 280;
// A tackler met him head on if he came at him from this near straight in
// front (the cosine of it); and then, this often, drove him back.
const HEAD_ON = 0.5;
const DRIVEN_BACK = 0.55;
// More men in on a tackle: the first, and the second, there in time to (as
// late as this after it), this often.
// The rush: working a blocker's shoulder, a step over (feet) this often
// (ms); off the edge, on a tackle's outside shoulder (feet out, and in front
// of him), and round him by the end (feet behind).
const HAND_MS = 420;
const HAND_FT = 0.9;
const EDGE_OUT = 1.7;
const EDGE_ON = 1.6;
const EDGE_PAST = 1.8;
const GANG = [0.5, 0.35];
const GANG_LATE = 250;
// A carrier making a man miss, this often on a stretch this long (feet, and
// ms) at least; a stiff arm, this often when the man comes at his free arm.
// The cut: this long, this far on and over (feet).
const EVADE = 0.5;
const EVADE_FAR = 30;
const EVADE_MS = 1200;
const STIFF_ARM = 0.5;
const JUKE_MS = 300;
const JUKE_ON = 2;
const JUKE_OVER = 5;
// How much later than he would have been a carrier can be brought down, his
// run slowed, for the man the sim says made the tackle to get there.
const SLACK_MS = 700;
// How far (feet) the man who jumps early before the snap goes, for the
// foul: out of his stance, or over the line.
const JUMP: Record<string, number> = {
	"False start": 1.5,
	"Illegal motion": 1.5,
	"Illegal shift": 1,
	"Neutral zone infraction": 3,
	Encroachment: 3.5,
};
// The ball out of his hands this long after the hit that knocks it loose;
// and what a man is doing on the turf, down.
const POP_MS = 120;
const DOWN = new Set<Move>([
	"tackled",
	"tackledBack",
	"dive",
	"prone",
	"supine",
	"getUp",
	"getUpBack",
]);
const CLOSE = 0.85;
const FLAT_OUT = 0.97;
// A receiver down his route, at this much of his top speed.
const ROUTE_PACE = 0.86;

// The kicking game. A place kick: the plant, and the foot into the ball this
// far through it. A punt: from the punter's first step, the ball dropped and
// the foot into it. A kickoff's run-up, from the kicker's first step to his
// plant. How fast a long snap comes back.
const KICK_MS = 800;
const KICK_CONTACT = 0.4;
const PUNT_MS = 1400;
const PUNT_DROP = 0.42;
const PUNT_CONTACT = 0.55;
const KICKOFF_RUN = 1250;
const LONG_SNAP_FPS = 60;
// The ball on its tee (its middle, off the turf); and how far behind the
// end line the net behind the posts catches a kick.
const TEE_Z = 0.8;
const NET_BACK = 20;
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
	// Arms of their own over what the rest of a man does (see Leg.arms),
	// put on once everybody has the rest.
	readonly reaches: { m: Actor; move: Move; t0: number; t1: number }[] = [];
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
	// The stats said of what is being acted out: those of its lines from
	// just after `lo` through `hi` - the play up to the first time the ball
	// is out, then each recovery in turn.
	lo: number;
	hi: number;

	// The men in a moment of their own after it (see celebrate).
	readonly mob = new Set<number>();

	constructor(input: ActInput) {
		this.input = input;
		this.r = input.r;
		this.men = input.parts.map((p) => new Actor(p));
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
	toward(from: Pt, to: Pt) {
		return Math.atan2(to.y - from.y, to.x - from.x);
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
		this.afterWhistle();
		this.celebrate();
		this.regroup();
		for (const x of this.reaches) {
			x.m.arms(x.move, x.t0, x.t1);
		}
		this.handBall();
		return {
			at: this.at,
			ball: this.ball,
			whistle: this.whistle,
			...(this.follow !== undefined ? { follow: this.follow } : {}),
			...(this.until > 0 ? { until: this.until } : {}),
		};
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
		this.at.set(flagI, t);
		const spot = this.w({ d: 0, a: 0 });
		this.ball.push({ kind: "spot", t0: this.whistle, at: { ...spot, z: 0 } });
		for (const m of this.men) {
			const up = this.whistle + 150 + r() * 450;
			m.hold(up);
			if (r() < 0.5) {
				const s = this.of(m.at);
				const back = m.team === offense ? -1 : 1;
				m.go(
					this.w({
						d: s.d + back * (2 + r() * 5),
						a: s.a + (r() - 0.5) * 4,
					}),
					up + 900 + r() * 700,
					"walk",
				);
			}
			m.hold(m.t + 400, "stand");
			this.done.add(m.pid);
		}
	}

	// The ball drawn with whoever has it, the way what he is doing holds it
	// (in both hands, reaching for it): each man's legs split where he gets
	// it and where it leaves him.
	handBall() {
		const segs = this.ball;
		for (const [k, s] of segs.entries()) {
			if (s.kind !== "held") {
				continue;
			}
			const m = this.man(s.pid);
			if (!m) {
				continue;
			}
			const until = segs[k + 1]?.t0 ?? Infinity;
			m.split(s.t0);
			if (until < Infinity) {
				m.split(until);
			}
			for (const l of m.legs) {
				if (l.t0 >= s.t0 && l.t0 < until) {
					l.ball = l.arms ? "hands" : holdFor(l.move);
				}
			}
		}
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
		// Nobody wants any of it: up out of their stances and standing.
		for (const m of this.men) {
			if (m === qb) {
				continue;
			}
			m.hold(snap + 300 + this.r() * 300);
			m.hold(this.whistle, "stand");
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
		const target = this.man(this.statPids("tgt")[0]);
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
		const rushers = this.d.filter(
			(m) => (DL.has(m.role) || m === sacker) && m !== picker,
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
		const sackD = Math.min(
			-2,
			(typeof sackYds === "number" ? sackYds : -6) * YD,
		);

		// The quarterback's drop: none for a screen, three steps for the
		// short game, five or seven going deep (in the gun, a step or two) -
		// sacked, to a few steps short of where they get him.
		const qbStart = this.of(qb.at);
		const dropD = sacked
			? clamp(qbStart.d - sackD - 2 - r() * 4, 0, 24)
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
		const setAt = this.w({ d: qbStart.d - dropD, a: qbStart.a });
		const dropMs = dropD === 0 ? 0 : 300 + dropD * 55;
		if (dropD > 0) {
			qb.go(setAt, has + dropMs, "back", this.ahead);
		}
		const ready = qb.t + 150;
		qb.hold(ready, "qbSet", this.ahead);

		// The routes, each man off the line a moment after the snap.
		const routes = new Map<number, Route>();
		for (const m of runners) {
			const start = this.of(m.at);
			const inSign: 1 | -1 =
				start.a > 0 ? -1 : start.a < 0 ? 1 : r() < 0.5 ? 1 : -1;
			if (m === target && thrown) {
				const name = targetRoute(spotOf(m.role), airYds, r());
				const u = r();
				// A back out of the backfield to the side of the man who
				// brings him down.
				const ta = first === cmpI ? this.tacklerA(m, snap) : undefined;
				const v =
					start.a === 0 && ta !== undefined
						? (ta < 0 ? 0 : 0.5) + r() * 0.5
						: r();
				routes.set(
					m.pid,
					layRoute(name, start, inSign, catchD, this.bounds, u, v),
				);
			} else {
				const { name, depth } = decoyRoute(spotOf(m.role), r(), r());
				routes.set(
					m.pid,
					layRoute(name, start, inSign, depth, this.bounds, r(), r()),
				);
			}
		}
		const releases = new Map(
			runners.map((m) => [m.pid, snap + 120 + r() * 100]),
		);

		// When it is thrown, and caught: as the man it goes to comes open -
		// or, open before the quarterback is set, as soon as he can.
		let throwAt = ready + 300 + r() * 900;
		let catchAt = throwAt;
		let catchPt: Pt | undefined;
		const tr = target ? routes.get(target.pid) : undefined;
		if (thrown && target && tr) {
			catchPt = this.w(tr.catchAt);
			const go = releases.get(target.pid)!;
			const onTime =
				go + this.routeMs(tr, target.top * ROUTE_PACE, tr.catchAlong);
			const fl = flightOf(
				this.w({ d: this.of(setAt).d + 1, a: this.of(setAt).a }),
				catchPt,
			);
			const hold = tr.settles ? r() * 600 : 0;
			catchAt = Math.max(onTime, ready + 300 + fl.ms) + hold;
			throwAt = catchAt - fl.ms;
			this.runRoute(target, tr, go, catchAt);
			this.at.set(first, catchAt);
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

		// The quarterback: set, through his progressions, and the throw.
		if (thrown && catchPt) {
			qb.hold(throwAt - 320, "qbSet", this.toward(qb.at, catchPt));
			qb.act("throwWind", throwAt, this.toward(qb.at, catchPt));
			qb.act("throwFollow", throwAt + 450, this.toward(qb.at, catchPt));
			const release: Pt3 = {
				...this.w({ d: this.of(qb.at).d + 0.8, a: this.of(qb.at).a }),
				z: RELEASE_Z,
			};
			const fl = flightOf(release, catchPt);
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
		const onBall = thrown
			? this.man(this.statPids(first === intI ? "defInt" : "defPssDef")[0])
			: undefined;
		this.cover(
			coverage,
			runners,
			thrown ? throwAt + BREAK_MS : outT,
			catchPt,
			catchAt,
			onBall && target ? [onBall, target] : undefined,
		);
		const beaten = sacker ? this.man(this.statPids("skAlw")[0]) : undefined;
		this.passRush(
			rushers,
			blockers,
			qb,
			(thrown ? catchAt : outT) + 300,
			sacker && beaten ? [sacker, beaten] : undefined,
		);

		// And what came of it.
		if (first === cmpI && thrown && target && catchPt) {
			this.complete(cmpI, qb, target, catchPt, catchAt, gain);
		} else if (first === incI && thrown && target && catchPt) {
			this.incomplete(incI, qb, target, catchPt, catchAt, coverage);
		} else if (first === intI && thrown && target && catchPt) {
			this.intercepted(intI, qb, target, catchPt, catchAt, coverage);
		} else if (sacked) {
			this.sacked(sackI, qb, outT, sackD, sacker);
		} else if (scrambled) {
			this.scramble(scrI, qb, outT);
		} else if (stripped) {
			this.strip(fumI, qb, outT);
		} else {
			this.whistle = Math.max(ready + 1500, qb.t);
		}
		for (const m of [qb, ...runners]) {
			this.done.add(m.pid);
		}
	}

	// How far downfield (yards) a pass that gained `yds` was caught: the
	// rest is run after the catch. Most of a short one is in the air; a
	// long one is mostly the throw, now and then a short one taken a long
	// way.
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
		if (yds <= 4) {
			return yds - Math.round(r() * Math.min(yds, 3));
		}
		if (yds > 15 && r() < 0.3) {
			return Math.round(5 + r() * 7);
		}
		return Math.max(1, Math.round(yds * (0.45 + 0.5 * r())));
	}

	// How long a route takes him, out of his stance to `upto` feet along it
	// at `speed`: a little slower off the line, a beat at each break.
	routeMs(route: Route, speed: number, upto: number): number {
		const pts = route.path.map((p) => this.w(p));
		let along = 0;
		let ms = 0;
		for (let k = 1; k < pts.length && along < upto; k++) {
			const go = Math.min(dist(pts[k - 1]!, pts[k]!), upto - along);
			ms += (go / (speed * (k === 1 ? 0.8 : 1))) * 1000 + (k > 1 ? 90 : 0);
			along += go;
		}
		return ms;
	}

	// A receiver down his route, off the line at `release`: at his route
	// speed - or, thrown to, timed to be where the ball is when it gets
	// there (slowed a little to come open on time, or sat down in it
	// waiting for it). Running on, he goes until `until`.
	runRoute(
		m: Actor,
		route: Route,
		release: number,
		catchAt?: number,
		until?: number,
	) {
		const speed = m.top * ROUTE_PACE;
		m.hold(release);
		const target = catchAt !== undefined;
		const endAlong = target ? route.catchAlong : Infinity;
		const pace =
			catchAt !== undefined && !route.settles
				? clamp(
						this.routeMs(route, speed, route.catchAlong) /
							Math.max(1, catchAt - release),
						0.6,
						1,
					)
				: 1;
		const pts = route.path.map((p) => this.w(p));
		let along = 0;
		for (let k = 1; k < pts.length; k++) {
			const seg = dist(pts[k - 1]!, pts[k]!);
			const room = endAlong - along;
			if (room <= 0) {
				break;
			}
			const go = Math.min(seg, room);
			const to = go < seg ? this.w(alongRoute(route, along + go)) : pts[k]!;
			// Out of his stance a little slower.
			const v = speed * (k === 1 ? 0.8 : 1);
			const t1 = m.t + ((go / v) * 1000 + (k > 1 ? 90 : 0)) / pace;
			if (until !== undefined && t1 > until) {
				const u = (until - m.t) / (t1 - m.t);
				const p = m.at;
				m.go({ x: p.x + (to.x - p.x) * u, y: p.y + (to.y - p.y) * u }, until);
				return;
			}
			m.go(to, t1);
			along += go;
		}
		if (!target && !route.settles && until !== undefined && m.t < until) {
			// Past the end of his route: on the way it goes.
			const p = route.path;
			const a = this.w(p.at(-2)!);
			const b = this.w(p.at(-1)!);
			const len = dist(a, b) || 1;
			const left = ((until - m.t) / 1000) * speed;
			m.go(
				{
					x: b.x + ((b.x - a.x) / len) * left,
					y: b.y + ((b.y - a.y) / len) * left,
				},
				until,
			);
		}
		if (route.settles || target) {
			// Sat down in it, turned to the quarterback.
			const qb = this.role("QB");
			const face = qb ? this.toward(m.at, qb.at) : this.behind;
			const t = catchAt !== undefined ? catchAt - 380 : (until ?? m.t);
			m.hold(Math.max(m.t, t), "settle", face);
		}
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
		const inStride = this.reachFor(rec, catchAt, qb);
		// After the catch: up the field to where the sim says it ended -
		// toward the man who brings him down.
		let across = this.of(catchPt).a + (r() - 0.5) * 24;
		const ta = this.tacklerA(rec, catchAt);
		if (ta !== undefined) {
			across += (ta - across) * 0.5;
		}
		const end = this.endOf(gain, across, e.td === true, e.safety === true);
		const yac = dist(catchPt, end.pt);
		const v = rec.top * (yac > 30 ? 0.95 : 0.8);
		let t = Math.max(rec.t, catchAt + 260);
		if (yac > 2 || inStride) {
			t = rec.run(end.pt, v, "sprint");
		}
		this.ball.push({ kind: "held", t0: catchAt, pid: rec.pid });
		this.endCarry(i, rec, end, t, this.d);
	}

	// Reaching for it as it comes - in stride, his arms up for it as he
	// runs on, or sat down in his route waiting on it, and in to his chest.
	// Whether in stride.
	reachFor(rec: Actor, catchAt: number, qb: Actor | undefined): boolean {
		const reach = catchAt - 300;
		const l = rec.last;
		if (
			Math.abs(rec.t - catchAt) < 5 &&
			(l.from.x !== l.to.x || l.from.y !== l.to.y)
		) {
			this.reaches.push({
				m: rec,
				move: "catch",
				t0: reach,
				t1: catchAt + 220,
			});
			return true;
		}
		const face = qb ? this.toward(rec.at, qb.at) : rec.yaw;
		rec.hold(reach);
		rec.act("catch", catchAt + 220, face);
		return false;
	}

	// On a few steps the way he was going, slowing up.
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
	): { pt: Pt; td: boolean; d: number } {
		const r = this.r;
		let d = yds * YD;
		if (td) {
			d = this.goalD + 4 + r() * 14;
		} else if (safety) {
			d = this.ownGoalD - 2 - r() * 6;
		}
		d = clamp(d, this.ownGoalD - 27, this.goalD + 27);
		const a = clamp(across, this.bounds.left + 6, this.bounds.right - 6);
		return { pt: this.w({ d, a }), td, d };
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
		end: { pt: Pt; td: boolean },
		t: number,
		chasers: Actor[],
		by?: Actor[],
	) {
		if (end.td) {
			this.evade(carrier, [], chasers);
			// Into the end zone, slowing up, the ball held up.
			const goal = this.goalLineX();
			const crossT = this.crossing(carrier, goal, t);
			this.at.set(i, crossT);
			this.whistle = crossT;
			const p = carrier.at;
			const ahead = {
				x: p.x + Math.cos(carrier.yaw) * 10,
				y: p.y + Math.sin(carrier.yaw) * 10,
			};
			carrier.go(ahead, t + 900, "jog");
			carrier.hold(t + 2600, "stand");
			this.done.add(carrier.pid);
			this.pursue(chasers, end.pt, t, []);
			return;
		}
		const down = this.downBy(carrier, end.pt, t, chasers, by);
		this.evade(carrier, down.by, chasers);
		this.at.set(i, down.t);
		this.whistle = down.t;
		const fumI = this.fumbleAfter(i);
		if (fumI >= 0) {
			this.fumbled(fumI, carrier, end.pt, down.t, down.by, chasers);
			return;
		}
		const pile = this.tackle(carrier, down.by, end.pt, down.t);
		this.gangUp(chasers, pile, down.t, down.by);
		this.pursue(chasers, end.pt, down.t, down.by);
	}

	// Making a man miss on the way, now and then on a long enough carry: one
	// of `chasers` (not the men who bring him down) there to meet him
	// partway along his last stretch - and a cut away from him, a plant and
	// off the other way, the man left on the turf behind him; or, coming at
	// his free arm, put on his back with a stiff arm.
	evade(carrier: Actor, by: Actor[], chasers: Actor[]) {
		const r = this.r;
		const l = carrier.last;
		const ms = l.t1 - l.t0;
		if (
			moveKind(l.move) !== "cycle" ||
			dist(l.from, l.to) < EVADE_FAR ||
			ms < EVADE_MS ||
			r() > EVADE
		) {
			return;
		}
		const ang = Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x);
		const tc = l.t0 + ms * (0.3 + r() * 0.35);
		const at = carrier.posAt(tc);
		const m = chasers
			.filter(
				(c) =>
					c.team !== carrier.team &&
					!this.done.has(c.pid) &&
					!by.includes(c) &&
					c.t < tc - 400,
			)
			.map((c) => ({
				c,
				t: this.canReach(c, at),
				from: c.posAt(Math.max(this.seen, Math.min(c.t, tc - 700))),
			}))
			// (Coming at him, not from behind.)
			.filter(
				(x) =>
					x.t < tc - 250 &&
					Math.cos(Math.atan2(x.from.y - at.y, x.from.x - at.x) - ang) > -0.3,
			)
			.sort((a, b) => a.t - b.t)[0];
		if (!m) {
			return;
		}
		const toHim = Math.atan2(m.from.y - at.y, m.from.x - at.x);
		// (Which side he comes from: +1 the side the ball is carried on, his
		// right.)
		const side = Math.sin(toHim - ang) > 0 ? 1 : -1;
		const beside = (p: Pt, a: number, by: number) => ({
			x: p.x + Math.cos(a) * by,
			y: p.y + Math.sin(a) * by,
		});
		if (side < 0 && r() < STIFF_ARM) {
			const hit = beside(at, ang - Math.PI / 2, 2.6);
			const face = this.toward(hit, at);
			this.closeOn(m.c, hit, tc - 80, face);
			m.c.act("tackledBack", tc + 520, face, beside(hit, face, -1.6));
			m.c.hold(tc + 520 + 600, "supine");
			m.c.act("getUpBack", tc + 520 + 600 + GET_UP_MS);
			carrier.arms("stiffArm", tc - 280, tc + 300);
		} else {
			const { to, t1, move } = l;
			const off = beside(
				beside(at, ang, JUKE_ON),
				ang - (side * Math.PI) / 2,
				JUKE_OVER,
			);
			// (Not if it would take more than he has to get there still.)
			if (dist(off, to) / ((t1 - tc - JUKE_MS) / 1000) > carrier.top) {
				return;
			}
			carrier.cut(tc);
			carrier.act("juke", tc + JUKE_MS, ang, off);
			if (side < 0) {
				// (To his right: the cut the other way round.)
				carrier.last.mirrored = true;
			}
			carrier.go(to, t1, move);
			// (Off his feet at where he was.)
			const hit = beside(at, toHim, 3.5);
			const face = this.toward(hit, at);
			this.closeOn(m.c, hit, tc - 100, face);
			m.c.act("dive", tc + 380, face, beside(at, face, 0.8));
			m.c.hold(tc + 380 + 700, "prone");
			m.c.act("getUp", tc + 380 + 700 + GET_UP_MS);
		}
		this.done.add(m.c.pid);
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
	slow(m: Actor, ms: number) {
		const l = m.last;
		const far = dist(l.from, l.to);
		if (far > 1 && moveKind(l.move) !== "act") {
			if (this.weave && ms > 300) {
				const v = Math.min(m.top * 0.9, far / ((l.t1 - l.t0) / 1000));
				const half = (v * (l.t1 - l.t0 + ms)) / 2000;
				const h = Math.min(
					24,
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
					const t1 = l.t1 + ms;
					const w = onField({
						x: mid.x - Math.sin(ang) * h * side,
						y: mid.y + Math.cos(ang) * h * side,
					});
					const a = dist(l.from, w);
					const tw = l.t0 + ((t1 - l.t0) * a) / (a + dist(w, end));
					l.to = w;
					l.t1 = tw;
					m.go(end, t1, l.move);
					return;
				}
			}
			l.t1 += ms;
			if (l.move !== "drive") {
				l.move = gait(far / ((l.t1 - l.t0) / 1000));
			}
		} else {
			m.hold(m.t + ms);
		}
	}

	goalLineX() {
		return this.input.dir === 1 ? GOAL_R : GOAL_L;
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
		const fall = carrier.yaw;
		const lead = t - HIT_MS;
		// (Where each comes at him from.)
		const froms = by.map((m) =>
			m.posAt(Math.max(this.seen, Math.min(m.t, lead - 600))),
		);
		const first = froms[0];
		const back =
			!keptUp &&
			first !== undefined &&
			Math.cos(Math.atan2(first.y - pt.y, first.x - pt.x) - fall) > HEAD_ON &&
			r() < DRIVEN_BACK;
		const along = (p: Pt, f: number, s: number) => ({
			x: p.x + Math.cos(fall) * f - Math.sin(fall) * s,
			y: p.y + Math.sin(fall) * f + Math.cos(fall) * s,
		});
		const downAt = along(pt, back ? -1.4 : 2.5, 0);
		if (keptUp) {
			this.runOn(carrier, 450);
		} else {
			carrier.act(back ? "tackledBack" : "tackled", t + 450, fall, downAt);
			carrier.hold(t + 450 + DOWN_MS, back ? "supine" : "prone");
			carrier.act(back ? "getUpBack" : "getUp", t + 450 + DOWN_MS + GET_UP_MS);
			this.done.add(carrier.pid);
		}
		by.forEach((m, k) => {
			// From the way he comes at him: there at the hit.
			const side = k === 0 ? 0 : (r() < 0.5 ? -1 : 1) * 1.2;
			const from = froms[k]!;
			const ang = Math.atan2(from.y - pt.y, from.x - pt.x) + side;
			const hit = {
				x: pt.x + Math.cos(ang) * 2.2,
				y: pt.y + Math.sin(ang) * 2.2,
			};
			const face = this.toward(hit, pt);
			this.closeOn(m, hit, lead, face);
			// Down alongside him, not on top of him - the man who drove him
			// back down across his legs.
			const by = (k % 2 === 0 ? 1 : -1) * 2.1;
			const lands = !back
				? along(downAt, -1.2, by)
				: k === 0
					? along(downAt, 2.6, 2)
					: along(downAt, 0.6, by);
			m.act("dive", t + 450, face, lands);
			m.hold(t + 450 + DOWN_MS + 150 * k, "prone");
			m.act("getUp", t + 450 + DOWN_MS + GET_UP_MS + 150 * k);
			this.done.add(m.pid);
		});
		return downAt;
	}

	// More of them in on it - the first one or two of `men` there as he
	// goes down, off their feet onto the pile at `pile`, each in from the
	// way he comes.
	gangUp(men: Actor[], pile: Pt, t: number, by: Actor[]) {
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
			const hit = {
				x: pile.x + Math.cos(ang) * 4.5,
				y: pile.y + Math.sin(ang) * 4.5,
			};
			const go = t + 150 + k * 200 + r() * 150;
			if (this.canReach(m, hit) > go) {
				return;
			}
			const face = this.toward(hit, pile);
			this.closeOn(m, hit, go, face);
			m.act("dive", go + 450, face, {
				x: pile.x + Math.cos(ang) * 1.6,
				y: pile.y + Math.sin(ang) * 1.6,
			});
			m.hold(go + 450 + DOWN_MS, "prone");
			m.act("getUp", go + 450 + DOWN_MS + GET_UP_MS);
			this.done.add(m.pid);
		});
	}

	// A man to `hit` by `lead`, facing `face` there: leaving what he was
	// doing as late as he can and still get there at most of his speed (or
	// whenever gets him there easiest) - with time to spare, up to a few
	// steps off to break down, and in from there.
	closeOn(m: Actor, hit: Pt, lead: number, face: number) {
		const need = (c: number) =>
			dist(m.posAt(c), hit) / Math.max(0.001, (lead - c) / 1000);
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
		m.cut(c);
		m.hold(c);
		const far = dist(m.at, hit);
		const secs = (lead - m.t) / 1000;
		if (far > 8 && far < m.top * 0.5 * secs) {
			const off = {
				x: hit.x + ((m.at.x - hit.x) / far) * 5,
				y: hit.y + ((m.at.y - hit.y) / far) * 5,
			};
			m.run(off, m.top * 0.7);
			m.hold(lead - 300, "backerStance", face);
			m.chase(hit, lead, m.top * FLAT_OUT, "run", face);
		} else {
			m.chase(hit, lead, m.top * FLAT_OUT);
		}
	}

	// The rest of the defense after the ball - whoever is not busy with
	// something of his own through it: each to where the play ends up,
	// near it, as fast as he can get there - those too far off never do,
	// and pull up at the whistle.
	pursue(men: Actor[], pt: Pt, t: number, tacklers: Actor[]) {
		const r = this.r;
		for (const m of men) {
			if (this.done.has(m.pid) || tacklers.includes(m) || m.t >= t) {
				continue;
			}
			m.hold(Math.max(m.t, this.seen));
			const ang = Math.atan2(m.at.y - pt.y, m.at.x - pt.x) + (r() - 0.5) * 0.8;
			const ring = 5 + r() * 9;
			m.chase(
				{ x: pt.x + Math.cos(ang) * ring, y: pt.y + Math.sin(ang) * ring },
				t + 200 + r() * 500,
				m.top * 0.95,
			);
		}
	}

	// ---- a ball that is not caught -------------------------------------------------

	incomplete(
		i: number,
		qb: Actor,
		rec: Actor,
		catchPt: Pt,
		catchAt: number,
		coverage: Actor[],
	) {
		const r = this.r;
		const breakup = this.man(this.statPids("defPssDef")[0]);
		const ground = catchAt + 350;
		let lands: Pt;
		if (breakup && coverage.includes(breakup)) {
			// Got a hand on it: knocked away.
			this.breakOn(breakup, catchPt, catchAt, 150);
			breakup.act("swat", catchAt + 250, this.toward(breakup.at, catchPt));
			this.done.add(breakup.pid);
			const ang = r() * Math.PI * 2;
			lands = {
				x: catchPt.x + Math.cos(ang) * 9,
				y: catchPt.y + Math.sin(ang) * 9,
			};
			if (this.reachFor(rec, catchAt, qb)) {
				this.runOn(rec, 600);
			}
		} else if (r() < 0.5) {
			// Off his hands.
			if (this.reachFor(rec, catchAt, qb)) {
				this.runOn(rec, 600);
			}
			const ang = r() * Math.PI * 2;
			lands = {
				x: catchPt.x + Math.cos(ang) * 4,
				y: catchPt.y + Math.sin(ang) * 4,
			};
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
			rec.act("dive", catchAt + 450, toward, {
				x: rec.at.x + Math.cos(toward) * 5,
				y: rec.at.y + Math.sin(toward) * 5,
			});
			rec.hold(catchAt + 450 + 500, "prone");
			rec.act("getUp", catchAt + 450 + 500 + GET_UP_MS);
		}
		this.done.add(rec.pid);
		// It drops, and bounces.
		const last = this.ball.at(-1);
		if (last?.kind === "fly") {
			last.t1 = catchAt;
		}
		const p0: Pt3 = { ...catchPt, z: CATCH_Z };
		const mid = { x: (p0.x + lands.x) / 2, y: (p0.y + lands.y) / 2 };
		this.ball.push(
			{
				kind: "fly",
				t0: catchAt,
				t1: ground,
				from: p0,
				to: { ...mid, z: 0 },
				peak: 0.8,
			},
			{
				kind: "fly",
				t0: ground,
				t1: ground + 420,
				from: { ...mid, z: 0 },
				to: { ...lands, z: 0 },
				peak: 1.6,
			},
			{ kind: "spot", t0: ground + 420, at: { ...lands, z: 0 } },
		);
		this.at.set(i, ground);
		this.whistle = ground;
		this.pursue(coverage, catchPt, catchAt + 200, []);
	}

	// A defender breaking on the ball: from his coverage for where it comes
	// down, there `early` ms before it (after it, negative) - just to the
	// quarterback's side of the receiver - or as near as he gets.
	breakOn(m: Actor, catchPt: Pt, catchAt: number, early: number) {
		const qb = this.role("QB");
		const toQb = qb ? this.toward(catchPt, qb.at) : this.behind;
		const spot = {
			x: catchPt.x + Math.cos(toQb) * 2.2,
			y: catchPt.y + Math.sin(toQb) * 2.2,
		};
		const go = Math.max(this.seen, catchAt - 700);
		m.cut(go);
		m.hold(go);
		m.chase(spot, catchAt - early, m.top * FLAT_OUT, "sprint");
	}

	intercepted(
		i: number,
		qb: Actor,
		rec: Actor,
		catchPt: Pt,
		catchAt: number,
		coverage: Actor[],
	) {
		const r = this.r;
		const picker =
			this.man(this.statPids("defInt")[0]) ??
			this.nearest(coverage, catchPt, 1)[0];
		this.at.set(i, catchAt);
		if (this.reachFor(rec, catchAt, qb)) {
			this.runOn(rec, 500);
		}
		this.done.add(rec.pid);
		if (!picker) {
			this.whistle = catchAt + 600;
			return;
		}
		// He jumps the route and takes it the other way.
		this.breakOn(picker, catchPt, catchAt, 60);
		picker.act("catch", catchAt + 220, this.toward(picker.at, catchPt));
		const ball = this.ball.at(-1);
		if (ball?.kind === "fly") {
			ball.to = { ...picker.at, z: CATCH_Z };
		}
		this.ball.push({ kind: "held", t0: catchAt, pid: picker.pid });
		const retI = this.find("interceptionReturn", i);
		const ret = retI >= 0 ? this.ev(retI) : undefined;
		const yds = typeof ret?.yds === "number" ? ret.yds : 0;
		// (Taken back from where the sim says it was picked off.)
		const at = this.ev(i).yds;
		const pickD =
			typeof at === "number"
				? Math.min(at * YD, this.goalD + 25)
				: this.of(picker.at).d;
		const td = ret?.td === true;
		let d = pickD - yds * YD;
		if (td) {
			d = this.ownGoalD - 4 - r() * 12;
		} else if (ret?.touchback) {
			d = Math.max(pickD, this.goalD + 3);
		}
		const pt = this.w({
			d: clamp(d, this.ownGoalD - 27, this.goalD + 27),
			a: clamp(
				this.of(picker.at).a + (r() - 0.5) * 30,
				this.bounds.left + 6,
				this.bounds.right - 6,
			),
		});
		let t = picker.t;
		if (dist(picker.at, pt) > 2) {
			t = picker.run(pt, picker.top * 0.9, "sprint");
		}
		// The offense sees it picked off, and comes after him.
		this.seen = catchAt + BREAK_MS;
		const others = this.o.filter((m) => m !== rec && !this.done.has(m.pid));
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
			this.endCarry(i, carrier, { pt, td: false }, t, chasers);
			return;
		}
		const crossT = this.crossingBack(carrier, t);
		this.at.set(i, crossT);
		this.whistle = crossT;
		carrier.hold(t + 2200, "stand");
		this.done.add(carrier.pid);
		this.pursue(chasers, pt, t, []);
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
	) {
		const r = this.r;
		const at = this.w({
			d: sackD,
			a: clamp(
				this.of(qb.at).a + (r() - 0.5) * 14,
				this.bounds.left + 6,
				this.bounds.right - 6,
			),
		});
		// Feeling it, he tries to get away from it.
		qb.hold(t - 800, "qbSet", this.ahead);
		qb.go(at, t);
		this.endCarry(
			i,
			qb,
			{ pt: at, td: false },
			t,
			this.d,
			sacker ? [sacker] : undefined,
		);
	}

	scramble(i: number, qb: Actor, go: number) {
		const r = this.r;
		const e = this.ev(i);
		const yds = typeof e.yds === "number" ? e.yds : 0;
		qb.hold(go, "qbSet", this.ahead);
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
	strip(i: number, qb: Actor, t: number) {
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
		qb.hold(Math.max(qb.t, t - Math.max(800, step)), "qbSet", this.ahead);
		qb.go(at, t);
		const down = this.downBy(qb, at, t, this.d, forcer ? [forcer] : undefined);
		this.at.set(i, down.t);
		this.whistle = down.t;
		this.fumbled(i, qb, at, down.t, down.by, this.d);
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
		const reach = go + (dist(rec.at, restPt) / (rec.top * 0.92)) * 1000;
		const takeT = scoop
			? Math.max(restT, reach)
			: Math.max(restT + 100, reach + 350);
		// It stops (or is in his hands) - batted on by the first men at it,
		// if he has a long way to go for it.
		const upTo = scoop ? takeT : takeT - 300;
		const batted = upTo - restT > 700;
		const stopT = batted ? upTo : restT;
		const stopZ = scoop && takeT - stopT < 50 ? 1.2 : 0;
		if (batted) {
			const bat = this.w({
				d: spot.d + (restD - spot.d) * 0.35 + (r() - 0.5) * 4,
				a: spot.a + (a - spot.a) * 0.35 + (r() - 0.5) * 6,
			});
			const batT = t + clamp((dist(pt, bat) / 15) * 1000, 350, 900);
			this.hops({ ...pt, z: 3 }, { ...bat, z: 0.5 }, t, batT);
			this.hops({ ...bat, z: 0.5 }, { ...restPt, z: stopZ }, batT, stopT);
			const near = this.nearest(
				others.filter((m) => m !== from),
				bat,
				2,
			);
			near.forEach((m, k) => {
				const yaw = this.toward(m.posAt(this.seen), bat);
				const short = {
					x: bat.x - Math.cos(yaw) * 3.5,
					y: bat.y - Math.sin(yaw) * 3.5,
				};
				const at = Math.max(batT + k * 160, this.canReach(m, short) + 300);
				if (at > stopT - 200) {
					return;
				}
				m.cut(this.seen);
				m.hold(this.seen);
				m.chase(short, at - 300, m.top * FLAT_OUT, "sprint");
				m.act("dive", at, yaw, {
					x: bat.x + Math.cos(yaw) * 1.5,
					y: bat.y + Math.sin(yaw) * 1.5,
				});
				m.hold(at + 900, "prone");
				m.act("getUp", at + 900 + GET_UP_MS);
				this.done.add(m.pid);
			});
		} else {
			this.hops({ ...pt, z: 3 }, { ...restPt, z: stopZ }, t, stopT);
		}
		if (takeT > stopT + 1) {
			this.ball.push({ kind: "spot", t0: stopT, at: { ...restPt, z: 0 } });
		}
		this.ball.push({ kind: "held", t0: takeT, pid: rec.pid });
		const yaw = this.toward(rec.at, restPt);

		if (!scoop) {
			// Fallen on, and everybody near in on top.
			const short = {
				x: restPt.x - Math.cos(yaw) * 4,
				y: restPt.y - Math.sin(yaw) * 4,
			};
			rec.chase(short, takeT - 350, rec.top * FLAT_OUT, "sprint");
			rec.act("dive", takeT, yaw, restPt);
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
			return;
		}

		// Picked up on the run, and away with it.
		rec.chase(restPt, takeT, rec.top * FLAT_OUT, "sprint");
		const endPt = this.w({
			d: endD,
			a: clamp(
				a + (r() - 0.5) * 16,
				this.bounds.left + 6,
				this.bounds.right - 6,
			),
		});
		const on = Math.atan2(endPt.y - rec.at.y, endPt.x - rec.at.x);
		rec.act("scoop", takeT + 380, on, {
			x: rec.at.x + Math.cos(on) * 5,
			y: rec.at.y + Math.sin(on) * 5,
		});
		const tEnd =
			dist(rec.at, endPt) > 2
				? rec.run(endPt, rec.top * 0.88, "sprint")
				: rec.t;
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

	// ---- the pass rush and the coverage --------------------------------------------

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
		for (const m of rushers) {
			if (this.done.has(m.pid) || engaged.has(m)) {
				continue;
			}
			const a = this.of(m.at).a;
			const free = blockers.filter((b) => ![...engaged.values()].includes(b));
			const pool = free.length > 0 ? free : blockers;
			const b = pool.sort(
				(x, y) => Math.abs(this.of(x.at).a - a) - Math.abs(this.of(y.at).a - a),
			)[0];
			if (b) {
				engaged.set(m, b);
			}
		}
		// The blockers' sets: back off the line, the tackles out wide; a
		// back kept in, up beside the quarterback.
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
			b.hold(snap + 60);
			const setPt =
				b.role === "RB" || b.role === "FB"
					? {
							d: pocket.d + 3,
							a: pocket.a + (Math.sign(s.a - pocket.a) || 1) * 3.5,
						}
					: { d: Math.max(pocket.d + 4, s.d - back), a: s.a + out };
			const far = dist(b.at, this.w(setPt));
			b.go(
				this.w(setPt),
				Math.max(snap + 650, b.t + (far / (b.top * 0.75)) * 1000),
				"passSet",
				this.ahead,
			);
			// Giving ground as the rush comes.
			const give = 2 + r() * 4;
			b.go(
				this.w({ d: setPt.d - give, a: setPt.a + (pocket.a - setPt.a) * 0.1 }),
				end,
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
		for (const [m, b] of engaged) {
			const s = this.of(m.at);
			const side = Math.sign(s.a) || 1;
			// Off the edge - lined up outside the last man on the line: up the
			// arc, onto his outside shoulder, and on round him, ridden past
			// the quarterback. Or in over the man across from him, working a
			// shoulder one way and then the other, the blocker with him.
			const edge =
				b === edgeOf(side) && Math.abs(s.a) > Math.abs(this.of(b.at).a) + 0.5;
			m.hold(snap + 40 + r() * 60);
			// Into him as soon as he can get there.
			const far = dist(m.at, b.posAt(snap + 450));
			let hitT = Math.max(
				snap + 380 + r() * 150,
				m.t + (far / (m.top * 0.8)) * 1000,
			);
			// (Where he is on the blocker at t - `lat` feet over, across the
			// field.)
			const on = (t: number, lat: number) => {
				const p = b.posAt(t);
				const u = clamp((t - hitT) / Math.max(1, end - hitT), 0, 1);
				const ahead = edge ? EDGE_ON - u * (EDGE_ON + EDGE_PAST) : 2.6;
				return {
					x: p.x + dir * ahead,
					y: p.y + dir * (edge ? side * EDGE_OUT : lat),
				};
			};
			if (edge) {
				// (Up the arc, and in onto him, at the one pace.)
				const arc = this.w({ d: s.d - 2, a: s.a + side * 1.5 });
				const a = dist(m.at, arc);
				const way = a + dist(arc, on(hitT, 0));
				hitT = Math.max(hitT, m.t + (way / (m.top * 0.85)) * 1000);
				m.go(arc, m.t + ((hitT - m.t) * a) / Math.max(1, way), "run");
			}
			m.go(on(hitT, (r() - 0.5) * 0.6), hitT, "run");
			const per = HAND_MS * (0.8 + r() * 0.5);
			const amp = edge ? 0 : HAND_FT * (0.5 + r());
			let way = r() < 0.5 ? 1 : -1;
			const steps: { t: number; lat: number }[] = [];
			for (let t = hitT + per; t < end - per / 2; t += per) {
				steps.push({ t, lat: way * amp });
				way = -way;
			}
			steps.push({ t: end, lat: 0 });
			const held = steps.map((x) => b.posAt(x.t));
			for (const x of steps) {
				const p = on(x.t, x.lat);
				m.go(
					p,
					x.t,
					"engage",
					edge ? this.toward(p, qb.posAt(x.t)) : this.behind,
				);
			}
			// (A man doubled moves with the first of them.)
			if (amp > 0 && !moved.has(b) && !this.done.has(b.pid)) {
				moved.add(b);
				b.cut(hitT);
				steps.forEach((x, k) => {
					b.go(
						{ x: held[k]!.x, y: held[k]!.y + dir * x.lat * 0.6 },
						x.t,
						"passSet",
						this.ahead,
					);
				});
			}
		}
		for (const m of rushers) {
			if (!engaged.has(m) && !this.done.has(m.pid)) {
				m.hold(snap + 60);
				m.run(qb.posAt(snap + 1800), m.top * 0.7);
			}
		}
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
		until: number,
		catchPt: Pt | undefined,
		catchAt: number,
		marked?: [Actor, Actor],
	) {
		const { snap, dir } = this.input;
		const r = this.r;
		// Everybody where he lined up.
		const set = (m: Actor) => this.of(m.posAt(snap));
		const free = coverage.filter((m) => !this.done.has(m.pid));
		const pairs = new Map<Actor, Actor>();
		if (marked && free.includes(marked[0]) && runners.includes(marked[1])) {
			pairs.set(marked[0], marked[1]);
		}
		const order = runners
			.filter((m) => !(marked && marked[1] === m && pairs.has(marked[0])))
			.sort((a, b) => Math.abs(set(b).a) - Math.abs(set(a).a));
		for (const rec of order) {
			const ra = set(rec);
			let best: Actor | undefined;
			let bestScore = Infinity;
			for (const m of free) {
				if (pairs.has(m) || m.role === "FS") {
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
			if (rec) {
				// On his man: a step behind him and to the inside, until the
				// ball is in the air - backpedaling out of his stance, then
				// turned and running with him.
				const rs = set(rec);
				const inside = -Math.sign(rs.a || 1);
				for (let t = snap + 200; t <= until; t += 220) {
					const p = this.of(rec.posAt(t - 180));
					const cushion = Math.max(
						2.5,
						(start.d - rs.d) * (1 - (t - snap) / 1800),
					);
					const to = this.w({ d: p.d + cushion, a: p.a + inside * 1.8 });
					const back = t - snap < 600 && (to.x - m.at.x) * dir > 0;
					m.chase(
						to,
						t,
						m.top * (back ? 0.6 : 0.95),
						back ? "back" : undefined,
						back ? this.behind : undefined,
					);
				}
			} else {
				// Back into his zone: deep middle, a deep half, under the
				// routes over the middle.
				const zone =
					m.role === "FS"
						? { d: Math.max(start.d, 42 + r() * 12), a: start.a * 0.4 }
						: m.role === "SS" || m.role === "CB"
							? { d: Math.max(start.d, 36 + r() * 10), a: start.a * 0.9 }
							: { d: 18 + r() * 10, a: start.a * 0.8 };
				m.hold(snap + 150);
				m.chase(
					this.w(zone),
					Math.min(until, snap + 1500),
					m.top * 0.6,
					"back",
					this.behind,
				);
				m.hold(until, "backerStance", this.behind);
			}
			// The ball in the air: everyone near breaks for it, each to a
			// spot of his own round it.
			if (catchPt) {
				const to = catchPt;
				const far = dist(m.at, to);
				if (far < 60) {
					const ang = Math.atan2(m.at.y - to.y, m.at.x - to.x);
					let spot = to;
					for (let k = 0; k < 8; k++) {
						const a = ang + Math.ceil(k / 2) * 0.7 * (k % 2 === 0 ? 1 : -1);
						spot = { x: to.x + Math.cos(a) * 4.5, y: to.y + Math.sin(a) * 4.5 };
						if (closing.every((p) => dist(p, spot) > 3.5)) {
							break;
						}
					}
					closing.push(spot);
					m.go(
						spot,
						Math.max(catchAt + 150, m.t + (dist(m.at, spot) / m.top) * 1000),
					);
				}
			}
		}
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
		const outside = !sneak && r() < 0.35;
		const cs = this.of(carrier.at);
		const sweep = spotOf(carrier.role) !== "back" && carrier !== qb;
		const holeA = sneak
			? (r() - 0.5) * 3
			: sweep
				? -Math.sign(cs.a || 1) * (18 + r() * 6)
				: strong * (outside ? 15 + r() * 6 : 2.5 + r() * 7);
		const blockersBack = this.statPids("rba", "rbw");

		// The quarterback: a reverse pivot under center to hand it off
		// deep, or a ride in the gun - and on with his fake. A receiver
		// coming round for it on a sweep takes it behind him.
		const qs = this.of(qb.at);
		const side = sweep ? (cs.a > 0 ? 1 : -1) : strong;
		const meshPt =
			carrier === qb
				? qb.at
				: this.w(
						sweep
							? { d: gun ? qs.d + 2 : qs.d - 2.5, a: side * 1.5 }
							: {
									d: gun ? qs.d + 0.5 : qs.d - 9,
									a: side * (gun ? 2.5 : 3),
								},
					);
		const qbMesh = this.w({
			...this.of(meshPt),
			a: this.of(meshPt).a - (sweep ? side : strong) * 2,
		});
		const lead = cs.a === 0 && cs.d < -10 ? 0 : 150;
		let mesh =
			carrier === qb
				? has
				: sweep
					? has + (gun ? 150 : 250) + r() * 100
					: Math.max(
							has + 300 + (dist(qb.at, qbMesh) / 15) * 1000 + r() * 200,
							snap +
								lead +
								(dist(carrier.at, meshPt) / (carrier.top * 0.75)) * 1000,
						);
		if (sweep) {
			// In motion before the snap, across behind the line, to take it
			// on the run just as the quarterback has it - going as soon as he
			// has been set a moment, if it is a long way.
			const set = carrier.legs.findLast((l) => l.t0 < snap) ?? carrier.last;
			const far = dist(set.from, meshPt);
			const go = Math.max(set.t0 + 300, mesh - (far / 19) * 1000);
			mesh = Math.max(mesh, go + (far / (carrier.top * 0.85)) * 1000);
			carrier.cut(go);
			carrier.go(meshPt, mesh, undefined, this.toward(carrier.at, meshPt));
		}
		if (handoffI >= 0 && carrier !== qb) {
			this.at.set(handoffI, mesh);
		}
		if (carrier !== qb) {
			qb.go(
				qbMesh,
				mesh - 60,
				gun ? "slide" : "back",
				this.toward(qb.at, meshPt),
			);
			qb.act("handOff", mesh + 160, this.toward(qbMesh, meshPt));
			qb.go(
				this.w({ d: this.of(qbMesh).d - 6, a: this.of(qbMesh).a - strong * 9 }),
				mesh + 1400,
				"run",
			);
			qb.hold(mesh + 2400, "stand");
			this.done.add(qb.pid);
			if (!sweep) {
				// The back to the mesh.
				carrier.hold(snap + lead);
				carrier.go(meshPt, mesh, "run");
			}
			this.ball.push({ kind: "held", t0: mesh, pid: carrier.pid });
		}

		// Through the hole and on to where the sim says he was brought
		// down.
		let across = holeA + (r() - 0.5) * (yds > 8 ? 30 : 8);
		if (ta !== undefined) {
			across += (ta - across) * (yds > 8 ? 0.5 : 0.25);
		}
		const end = this.endOf(yds, across, e.td === true, e.safety === true);
		const holePt = this.w({ d: sneak ? 1 : 0.5, a: holeA });
		let t = carrier.t;
		const endD = this.of(end.pt).d;
		if (sneak) {
			// Behind the center, into the pile.
			carrier.hold(has + 150, "qbSet", this.ahead);
			carrier.go(
				end.pt,
				Math.max(snap + 1300, carrier.t + 600),
				"drive",
				this.ahead,
			);
			t = carrier.t;
		} else if (
			endD < 0.5 &&
			dist(carrier.at, end.pt) < dist(carrier.at, holePt)
		) {
			// Met in the backfield.
			t = carrier.run(end.pt, carrier.top * 0.6);
		} else {
			// Pressing the hole, patient - then through it (on a sweep,
			// flat out for the edge).
			carrier.run(holePt, carrier.top * (sweep ? 0.88 : 0.62));
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
		this.runBlock(carrier, holeA, end.pt, down.t, blockersBack);
		this.endCarry(
			runI,
			carrier,
			end,
			down.t,
			this.d,
			end.td ? undefined : down.by,
		);
	}

	runBlock(
		carrier: Actor,
		holeA: number,
		endPt: Pt,
		t: number,
		backBlockers: number[],
	) {
		const { snap, dir } = this.input;
		const r = this.r;
		const blockers = this.o.filter(
			(m) =>
				m !== carrier &&
				!this.done.has(m.pid) &&
				(OL.has(m.role) ||
					backBlockers.includes(m.pid) ||
					m.role === "TE" ||
					m.role === "TE2"),
		);
		const front = this.d.filter((m) => DL.has(m.role) && !this.done.has(m.pid));
		const drive = this.of(endPt).d > 3 ? 4 : this.of(endPt).d < 0 ? -2 : 1;
		const side = Math.sign(holeA) || 1;
		const until = t + 300;
		for (const b of blockers) {
			const s = this.of(b.at);
			b.hold(snap + 40);
			const fire = { d: s.d + 3 + drive * 0.3, a: s.a + side * 2 };
			b.go(this.w(fire), snap + 550, "drive", this.ahead);
			b.go(
				this.w({ d: fire.d + drive, a: fire.a + side * 1.5 }),
				until,
				"drive",
				this.ahead,
			);
		}
		for (const m of front) {
			const s = this.of(m.at);
			const b = blockers.sort(
				(x, y) =>
					Math.abs(this.of(x.at).a - s.a) - Math.abs(this.of(y.at).a - s.a),
			)[0];
			m.hold(snap + 50);
			if (b) {
				const at = (time: number) => {
					const p = b.posAt(time);
					return { x: p.x + dir * 2.6, y: p.y + (r() - 0.5) * 0.6 };
				};
				m.chase(at(snap + 450), snap + 450, m.top * 0.9, "run");
				m.go(at(until), until, "engage", this.behind);
			}
		}
		// The receivers out on the corners and safeties.
		for (const m of this.o) {
			if (this.done.has(m.pid) || m === carrier || blockers.includes(m)) {
				continue;
			}
			const s = this.of(m.at);
			m.hold(snap + 80);
			m.go(this.w({ d: s.d + 9 + r() * 8, a: s.a * 0.92 }), snap + 1300, "run");
			m.hold(until, "drive", this.ahead);
		}
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
		return out;
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
	): Lock {
		const vc = c.top * 0.85;
		c.hold(go);
		const cGo = c.t;
		const cAt = c.at;
		let meet: Pt;
		let tm: number;
		if (bv <= 0) {
			meet = b.at;
			tm = Math.max(b.t, cGo + (dist(cAt, meet) / vc) * 1000);
		} else {
			b.hold(go);
			const bAt = b.at;
			const u = vc / (vc + bv);
			meet = {
				x: cAt.x + (bAt.x - cAt.x) * u,
				y: cAt.y + (bAt.y - cAt.y) * u,
			};
			tm = Math.max(b.t, cGo) + 150 + (dist(bAt, cAt) / (vc + bv)) * 1000;
		}
		// Squared up to each other, a step apart.
		const ang = Math.atan2(cAt.y - meet.y, cAt.x - meet.x);
		const cHit = {
			x: meet.x + Math.cos(ang) * 1.3,
			y: meet.y + Math.sin(ang) * 1.3,
		};
		const bHit = {
			x: meet.x - Math.cos(ang) * 1.3,
			y: meet.y - Math.sin(ang) * 1.3,
		};
		if (bv <= 0) {
			b.hold(tm - 200, "backerStance", ang);
		}
		b.go(bHit, tm, undefined, ang);
		c.go(cHit, tm, undefined, ang + Math.PI);
		const run = c.last;
		// Along with the return a little, each fighting the other.
		const end = this.of(endPt);
		const m = this.of(meet);
		const dd = clamp(end.d - m.d, -5, 5) * 0.4;
		const da = clamp(end.a - m.a, -10, 10) * 0.45;
		const shift = (p: Pt) => {
			const o = this.of(p);
			return this.w({ d: o.d + dd, a: o.a + da });
		};
		const t1 = Math.max(until, tm + 300);
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
		m.chase(
			{ x: pt.x + Math.cos(ang) * ring, y: pt.y + Math.sin(ang) * ring },
			Math.max(m.t + 400, this.whistle + 300),
			m.top * 0.7,
		);
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
		if (run > 24) {
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
	getUnder(m: Actor, land: OPt, from: number, catchAt: number): Pt3 {
		m.hold(from);
		m.chase(
			this.w({ d: land.d + 3.5, a: land.a }),
			catchAt - 700,
			m.top * 0.85,
		);
		m.hold(catchAt - 300, "stand", this.behind);
		m.act(
			"catch",
			catchAt + 220,
			this.behind,
			this.w({ d: land.d + 0.8, a: land.a }),
		);
		const at = this.of(m.posAt(catchAt));
		return { ...this.w({ d: at.d - 1, a: at.a }), z: CATCH_Z };
	}

	// No return: the cover men on down the field a way and pulling up, the
	// rest up out of their stances.
	coverDown(cover: Actor[], others: Actor[]) {
		const r = this.r;
		for (const m of cover) {
			m.hold(this.seen);
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

		// Off to one side of the landing zone or the other - or, out the
		// back, deep in the end zone.
		const deep = kickTo <= -10;
		const land: OPt = {
			d: deep
				? this.goalD + 15 + r() * 12
				: Math.min(this.goalD - kickTo * YD, this.goalD + 27),
			a: clamp((r() - 0.5) * 80, this.bounds.left + 14, this.bounds.right - 14),
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

		// The catch - on the fly, or a line drive fielded on the hop.
		const hop = kickTo >= 0 && r() < 0.3;
		const catchAt = landT + (hop ? 650 : 0);
		const hands = this.getUnder(kr, land, kickT + 250, catchAt);
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
		this.ball.push({ kind: "held", t0: catchAt, pid: kr.pid });
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

		if (!ret || e.touchback === true) {
			// Downed in the end zone: a knee.
			kr.hold(catchAt + 650);
			kr.hold(catchAt + 1800, "kneelDown", this.behind);
			this.whistle = catchAt + 700;
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

		// Up for it as it comes, facing it, his hands on it at `t`.
		const handsOn = (m: Actor, at: Pt, t: number) => {
			const yaw = this.toward(at, down);
			m.hold(goOf(m));
			m.chase(
				{ x: at.x + Math.cos(yaw) * 1.5, y: at.y + Math.sin(yaw) * 1.5 },
				t - 250,
				m.top * 0.9,
			);
			m.hold(t - 300, "stand", yaw);
			m.act("catch", t + 220, yaw, at);
		};
		let takeT: number;
		if (caught) {
			// Caught coming down, the hands team's man facing the kick, the
			// kicking team's going with it.
			const near = this.w({ d: at.d + (ahead ? -1.5 : 1.5), a: at.a });
			rec.hold(goOf(rec));
			rec.chase(near, landT - 250, rec.top * 0.9);
			rec.hold(landT - 300, "stand", face);
			rec.act("catch", landT + 220, face, restPt);
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
			rec.chase(restPt, takeT - 350, rec.top * FLAT_OUT, "sprint");
			rec.act("dive", takeT, rec.yaw, restPt);
			rec.hold(takeT + 1400, "prone");
			rec.act("getUp", takeT + 1400 + GET_UP_MS);
			if (muff) {
				// And after it again, too late.
				const yaw = this.toward(muff.at, restPt);
				const by = {
					x: restPt.x - Math.cos(yaw) * 1.6,
					y: restPt.y - Math.sin(yaw) * 1.6,
				};
				muff.chase(by, takeT - 200, muff.top * 0.8);
				muff.act("dive", takeT + 250, yaw, by);
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
		pile.forEach((m, n) => {
			const t1 = hitT + 150 + n * 170 + r() * 200;
			const ang = Math.atan2(m.at.y - pt.y, m.at.x - pt.x) + (r() - 0.5) * 0.9;
			const ring = 2.4 + n * 0.5 + r() * 1.5;
			const hit = {
				x: pt.x + Math.cos(ang) * ring,
				y: pt.y + Math.sin(ang) * ring,
			};
			const inward = this.toward(hit, pt);
			if (n < 3) {
				m.chase(hit, t1 - 400, m.top * FLAT_OUT, "sprint");
			}
			if (n < 3 && dist(m.at, hit) < 3) {
				m.act("dive", t1, inward, {
					x: pt.x + Math.cos(ang) * 1.2,
					y: pt.y + Math.sin(ang) * 1.2,
				});
				m.hold(t1 + 1100 + n * 150, "prone");
				m.act("getUp", t1 + 1100 + n * 150 + GET_UP_MS);
			} else {
				m.chase(hit, t1, m.top * 0.92);
				m.hold(this.whistle + 700 + r() * 400, "engage", inward);
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

	// The kicker, his kick away, on after it at a jog.
	kickerAfter(k: Actor, at: OPt, until: number) {
		if (this.done.has(k.pid)) {
			return;
		}
		k.go(
			this.w({ d: at.d * 0.45 + this.r() * 8, a: at.a * 0.4 }),
			Math.max(k.t + 600, until),
			"jog",
		);
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
		const { snap } = this.input;
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
		// end zone or bounding into it.
		const land: OPt = {
			d: touchback
				? clamp(yds * YD, this.goalD - 12, this.goalD + 22)
				: Math.min(yds * YD, this.goalD - 1),
			a: clamp(
				(pr ? this.of(pr.at).a : 0) + (r() - 0.5) * 36,
				this.bounds.left + 12,
				this.bounds.right - 12,
			),
		};
		const landPt = this.w(land);
		const hang = clamp(2800 + yds * 30 + r() * 400, 3300, 4900);
		const landT = kickT + hang;
		this.seen = kickT + 120;
		p.go(this.w({ d: p0.d + 30, a: land.a * 0.2 }), landT + 600, "jog");

		// The protection, until the ball is away: the line and the shield in
		// their sets, the middle of the rush into them.
		const line = this.o.filter((m) => m !== p && m.role !== "GUNNER");
		const free = this.seen;
		for (const m of line) {
			const s = this.of(m.at);
			if (m.role !== "LS") {
				m.hold(snap + 60);
			}
			m.go(
				this.w({ d: s.d + (m.role === "UP" ? 1 : -1.5), a: s.a }),
				free,
				"passSet",
				this.ahead,
			);
		}
		const rush = this.d.filter((m) => m.role === "RUSH");
		const inside = rush.filter((m) => Math.abs(this.of(m.at).a) < 10);
		const up = line.filter((m) => m.role !== "UP");
		for (const m of inside) {
			const b = this.acrossFrom(up, this.of(m.at).a);
			m.hold(snap + 50);
			if (!b) {
				continue;
			}
			const at = (time: number) => {
				const q = this.of(b.posAt(time));
				return this.w({ d: q.d + 2.6, a: q.a });
			};
			m.chase(at(snap + 450), snap + 450, m.top * 0.9, "run");
			m.go(at(free), free, "engage", this.behind);
		}

		// The gunners off the line through the men on them and flat out for
		// where it is coming down, the jammers running with them.
		const gunners = this.o.filter((m) => m.role === "GUNNER");
		const jammers = this.d.filter((m) => m.role === "JAMMER");
		const goalPull = this.goalD - 4;
		for (const g of gunners) {
			const s = this.of(g.at);
			const side = Math.sign(s.a) || 1;
			const j = this.acrossFrom(jammers, s.a);
			g.hold(snap + 60);
			g.go(
				this.w({ d: s.d + 9, a: s.a - side * 2 }),
				snap + 900,
				"engage",
				this.ahead,
			);
			g.run(
				this.w({
					d: Math.min(land.d - 10 - r() * 6, goalPull),
					a: land.a + side * (6 + r() * 6),
				}),
				g.top * 0.95,
				"sprint",
			);
			g.hold(g.t + 300, "backerStance", this.toward(g.at, landPt));
			if (j) {
				jammers.splice(jammers.indexOf(j), 1);
				j.hold(snap + 60);
				j.go(
					this.w({ d: s.d + 11.6, a: s.a - side * 2 }),
					snap + 900,
					"engage",
					this.behind,
				);
				// A step behind him and inside, all the way down.
				for (let time = snap + 1300; time < g.t; time += 400) {
					const q = this.of(g.posAt(time));
					j.go(this.w({ d: q.d - 2.5, a: q.a - side * 2.5 }), time);
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
			m.run(
				this.w({
					d: Math.min(land.d - 26 - k * 7 - r() * 8, goalPull - 20),
					a: land.a + wallSide * (8 + k * 7 + r() * 5),
				}),
				m.top * 0.85,
				"sprint",
			);
		});

		// The rest of the cover in their lanes, spread across the field.
		const lanes = [...line].sort((x, y) => this.of(x.at).a - this.of(y.at).a);
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
					spiral: true,
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
			lanes.forEach((m, k) => {
				m.chase(this.w(laneEnd(k)), this.whistle + 900, m.top * 0.85);
			});
			for (const m of this.d) {
				if (!this.done.has(m.pid)) {
					m.hold(Math.max(m.t, free) + 300);
					m.hold(this.whistle + 600, "stand");
				}
			}
			return;
		}

		// The catch.
		const hands = this.getUnder(pr, land, kickT + 200, landT);
		this.ball.push(
			{
				kind: "fly",
				t0: kickT,
				t1: landT,
				from: off,
				to: { ...hands, z: CATCH_Z + 0.6 },
				peak: peakFor(hang),
				spiral: true,
			},
			{ kind: "held", t0: landT, pid: pr.pid },
		);
		const retYds = num(ret?.yds, 0);
		const tacklers = this.statPids("defTckSolo", "defTckAst");
		if (!ret || (retYds === 0 && tacklers.length === 0)) {
			// A fair catch: his hand up as it comes, and nobody touches him.
			this.reaches.push({
				m: pr,
				move: "swat",
				t0: landT - 1700,
				t1: landT - 900,
			});
			this.whistle = landT + 400;
			if (retI >= 0) {
				this.at.set(retI, this.whistle);
			}
			this.done.add(pr.pid);
			lanes.forEach((m, k) => {
				m.chase(this.w(laneEnd(k)), this.whistle + 900, m.top * 0.85);
			});
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
		const t = this.returnRun(pr, endD, landT, land.a);
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
			),
		);
		lanes.forEach((m, k) => {
			if (!pairs.some(([, c]) => c === m)) {
				m.run(this.w(laneEnd(k)), m.top * (0.8 + r() * 0.12), "sprint");
			}
		});
		this.endReturn(retI, pr, endPt, t, td, this.o);
		this.release(locks, endPt);
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
		const down = has + 260;
		const kickT = snap + 1280 + r() * 100;
		h.hold(down, "holdKneel");
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
		const crossA =
			postA +
			(made
				? (r() - 0.5) * 11
				: short
					? (r() - 0.5) * 8
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
		const tEnd = Math.min(tLand, tNet);
		const end = along(tEnd);
		const endT = kickT + tEnd * 1000;
		this.ball.push({
			kind: "fly",
			t0: kickT,
			t1: endT,
			from,
			to: end,
			peak: peakFor(tEnd * 1000),
		});
		if (tNet < tLand) {
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
		const held = kickT + 250;
		for (const m of wall) {
			const s = this.of(m.at);
			if (m.role !== "LS") {
				m.hold(snap + 40);
			}
			m.go(
				this.w({ d: s.d - 0.8, a: s.a * 0.96 }),
				held,
				"passSet",
				this.ahead,
			);
		}
		for (const m of this.d) {
			const s = this.of(m.at);
			const edge = Math.abs(s.a) > 12;
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
				m.chase(at(snap + 400), snap + 400, m.top * 0.9, "run");
				m.go(at(held), held, "engage", this.behind);
			} else if (m.role === "RUSH" || m.role === "CB") {
				const side = Math.sign(s.a) || 1;
				m.chase(
					this.w({
						d: spot.d + 13 + r() * 4,
						a: spot.a + side * (7 + r() * 3),
					}),
					kickT - 150,
					m.top * 0.95,
					"sprint",
				);
				m.act("swat", kickT + 350, this.toward(m.at, this.w(spot)));
			} else {
				m.hold(kickT - 250);
				m.act("swat", kickT + 350, this.behind);
			}
		}
		// Everybody turned to watch it go.
		const posts = this.w({ d: lineD, a: postA });
		for (const m of this.men) {
			if (m !== k) {
				m.hold(Math.max(m.t, kickT + 350) + r() * 200);
				m.hold(crossT + 300, "stand", this.toward(m.at, posts));
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
			if (sacker && r() < 0.7) {
				// (Up off the quarterback, and straight into it.)
				const up = sacker.legs.findLast((l) => DOWN.has(l.move));
				if (up) {
					sacker.cut(up.t1);
				}
				sacker.act("flex", sacker.t + 1100);
				this.until = sacker.t;
				sacker.hold(sacker.t + 400, "stand");
				this.mob.add(sacker.pid);
			}
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
		const at = scorer.at;
		const u = r();
		if (u < 0.4) {
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
		} else if (u < 0.7) {
			scorer.hold(c0 + 1400, "ballUp");
		} else {
			scorer.act("leap", c0 + 800);
		}
		this.until = scorer.t;
		scorer.hold(scorer.t + 2600, "stand");
		this.mob.add(scorer.pid);
		// His teammates nearest, to him.
		const start = c0 + 150;
		// (Away on his own, a long way ahead of them all: the nearest of them
		// coming down the field after him, however long it takes.)
		const mates = this.men
			.filter(
				(m) =>
					m !== scorer &&
					m.team === scorer.team &&
					!DOWN.has(legAt(m, start).move),
			)
			.sort((a, b) => dist(a.posAt(c0), at) - dist(b.posAt(c0), at))
			.filter((m, k) => k < 2 || dist(m.posAt(c0), at) < MOB_FROM)
			.slice(0, MOB);
		mates.forEach((m, k) => {
			const near = dist(m.posAt(c0), at) < MOB_FROM;
			const from = m.posAt(start);
			m.cut(start);
			const ang = Math.atan2(from.y - at.y, from.x - at.x) + (k - 1.5) * 0.25;
			const to = {
				x: at.x + Math.cos(ang) * MOB_RING,
				y: at.y + Math.sin(ang) * MOB_RING,
			};
			const t = m.run(to, m.top * (0.7 + r() * 0.15), "sprint");
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
		// The other side's nearest: hands on hips, turned away.
		const beaten = this.men
			.filter((m) => m.team !== scorer.team && dist(m.posAt(c0), at) < 40)
			.sort((a, b) => dist(a.posAt(c0), at) - dist(b.posAt(c0), at))
			.slice(0, 3);
		for (const m of beaten) {
			const t = Math.max(m.t, c0 + 300 + r() * 700);
			m.hold(t);
			const away = this.toward(at, m.at) + (r() - 0.5) * 0.8;
			m.go(
				{ x: m.at.x + Math.cos(away) * 6, y: m.at.y + Math.sin(away) * 6 },
				t + 1800,
				"walk",
			);
			m.hold(m.t + 2400, "onHips");
			this.mob.add(m.pid);
		}
	}

	// Everyone not already given his part to the end: pulled up where the
	// whistle finds him - a few steps on the way he was going, slowing -
	// and stood there.
	afterWhistle() {
		const t = this.whistle;
		for (const m of this.men) {
			if (!this.done.has(m.pid)) {
				// (Nothing for him: watching from where he was.)
				m.hold(Math.max(m.t, this.input.snap + READ_MS));
			}
			if (m.t > t + 400) {
				m.hold(m.t + 600, "stand");
				continue;
			}
			const l = m.last;
			const moving = l.from.x !== l.to.x || l.from.y !== l.to.y;
			if (moving && l.t1 >= t - 100) {
				const v = dist(l.from, l.to) / Math.max(0.001, (l.t1 - l.t0) / 1000);
				const ang = Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x);
				const run = Math.min(10, v * 0.35);
				m.go(
					{ x: l.to.x + Math.cos(ang) * run, y: l.to.y + Math.sin(ang) * run },
					m.t + 700,
					gait(v * 0.45),
				);
			}
			m.hold(Math.max(m.t, t) + 600, "stand");
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
		const { kind, offense, dir, events, i0, i1 } = this.input;
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
		const off =
			KICK_KINDS.has(kind) ||
			scored ||
			(holder !== undefined && holder.team !== offense);
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
			return Math.max(t, legs[k]!.t1);
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
				const ang = Math.atan2(from.y - at.y, from.x - at.x);
				const by = {
					x: at.x + Math.cos(ang) * 2.3,
					y: at.y + Math.sin(ang) * 2.3,
				};
				const need = (dist(from, by) / HELP_FPS) * 1000;
				const leave = up.t0 - 80 - need;
				if (dist(from, at) < HELP_FROM && leave >= free.get(helper.pid)!) {
					helper.cut(leave);
					helper.hold(leave);
					helper.go(by, up.t0 - 80, undefined, this.toward(by, at));
					helper.act("helpUp", up.t1, this.toward(by, at));
					free.set(helper.pid, helper.t);
				}
			}
		}
		for (const m of this.men) {
			if (this.mob.has(m.pid)) {
				continue;
			}
			const start = free.get(m.pid)! + 250 + r() * 900;
			m.cut(start);
			m.hold(start);
			// (The ball let go of: tossed down by his feet.)
			if (m === holder && last && last.t0 <= start) {
				const a = m.yaw;
				const from = {
					x: m.at.x + Math.cos(a),
					y: m.at.y + Math.sin(a),
					z: 3.2,
				};
				const to = {
					x: m.at.x + Math.cos(a) * 4,
					y: m.at.y + Math.sin(a) * 4,
					z: 0.3,
				};
				this.ball.push(
					{ kind: "fly", t0: start, t1: start + TOSS_MS, from, to, peak: 1.2 },
					{ kind: "spot", t0: start + TOSS_MS, at: { ...to, z: 0 } },
				);
			}
			const going = off
				? {
						x: m.at.x + (r() - 0.5) * 24,
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
			const fps =
				off && r() < 0.6 ? 6.8 + r() * 2 : (big ? 3 : 3.6) + r() * 1.1;
			if (dist(m.at, to) > 2) {
				m.run(to, fps);
			}
		}
	}
}

// The leg he is in at t (or the last).
const legAt = (m: Actor, t: number): Leg =>
	m.legs.findLast((l) => l.t0 <= t) ?? m.legs[0]!;

// After a score: the scorer's moment, then his teammates' around him -
// at the most this many of them, from no further than this away.
const MOB = 4;
const MOB_FROM = 75;
// How far off the scorer each of them pulls up (feet), and a ball spiked:
// down into the turf this long after he starts, and up off it.
const MOB_RING = 3.2;
const SPIKE_MS = 720;
const SPIKE_AT = 0.6;
const BOUNCE_MS = 650;
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
const TOSS_MS = 380;
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
