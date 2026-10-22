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
	dist,
	FIELD_L,
	FIELD_W,
	GOAL_L,
	GOAL_R,
	YD,
	type Dir,
	type Pt,
	type Pt3,
	type Side,
} from "./geometry.ts";
import { moveKind, type Move } from "./poses.ts";
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
	"back",
	"slide",
	"walk",
	"shotgun",
	"underCenter",
	"stand",
]);
const holdFor = (m: Move): BallHold =>
	m === "center"
		? "snap"
		: m === "throwWind"
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
// How much later than he would have been a carrier can be brought down, his
// run slowed, for the man the sim says made the tackle to get there.
const SLACK_MS = 700;
const CLOSE = 0.85;
const FLAT_OUT = 0.97;
// A receiver down his route, at this much of his top speed.
const ROUTE_PACE = 0.86;

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
		const out: number[] = [];
		for (const x of this.input.stats) {
			if (names.includes(x.s) && !out.includes(x.pid)) {
				out.push(x.pid);
			}
		}
		return out;
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
		if (kind === "kneel" && kneel >= 0) {
			this.kneel(kneel);
		} else if (kind === "pass" && drop >= 0) {
			this.pass(drop);
		} else if ((kind === "run" && runI >= 0) || handoff >= 0) {
			if (runI < 0) {
				return undefined;
			}
			this.runPlay(handoff, runI);
		} else {
			return undefined;
		}
		this.afterWhistle();
		for (const x of this.reaches) {
			x.m.arms(x.move, x.t0, x.t1);
		}
		this.handBall();
		return { at: this.at, ball: this.ball, whistle: this.whistle };
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
		// The rush: the front, and a blitzer if one got home.
		const sacker = sacked ? this.man(this.statPids("defSk")[0]) : undefined;
		const rushers = this.d.filter((m) => DL.has(m.role) || m === sacker);
		if (!sacker && r() < 0.22) {
			const lb = this.d.find((m) => LB.has(m.role) && !rushers.includes(m));
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
			airYds = this.airYards(
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
		this.at.set(i, down.t);
		this.whistle = down.t;
		this.tackle(carrier, down.by, end.pt, down.t);
		this.pursue(chasers, end.pt, down.t, down.by);
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
	// slowed to it (or, standing, held there).
	slow(m: Actor, ms: number) {
		const l = m.last;
		const far = dist(l.from, l.to);
		if (far > 1 && moveKind(l.move) !== "act") {
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
		for (const l of m.legs) {
			const a = (l.from.x - gx) * this.input.dir;
			const b = (l.to.x - gx) * this.input.dir;
			if (a < 0 && b >= 0) {
				return l.t0 + ((l.t1 - l.t0) * -a) / (b - a);
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
	// round him, the second in on it.
	tackle(carrier: Actor, by: Actor[], pt: Pt, t: number) {
		const r = this.r;
		const fall = carrier.yaw;
		const downAt = {
			x: pt.x + Math.cos(fall) * 2.5,
			y: pt.y + Math.sin(fall) * 2.5,
		};
		carrier.act("tackled", t + 450, fall, downAt);
		carrier.hold(t + 450 + DOWN_MS, "prone");
		carrier.act("getUp", t + 450 + DOWN_MS + GET_UP_MS);
		this.done.add(carrier.pid);
		const lead = t - HIT_MS;
		by.forEach((m, k) => {
			// From the way he comes at him: there at the hit.
			const side = k === 0 ? 0 : (r() < 0.5 ? -1 : 1) * 1.2;
			const from = m.posAt(Math.max(this.seen, Math.min(m.t, lead - 600)));
			const ang = Math.atan2(from.y - pt.y, from.x - pt.x) + side;
			const hit = {
				x: pt.x + Math.cos(ang) * 2.2,
				y: pt.y + Math.sin(ang) * 2.2,
			};
			const face = this.toward(hit, pt);
			this.closeOn(m, hit, lead, face);
			// Down alongside him, not on top of him.
			const by = (k % 2 === 0 ? 1 : -1) * 2.1;
			m.act("dive", t + 450, face, {
				x: downAt.x - Math.sin(fall) * by - Math.cos(fall) * 1.2,
				y: downAt.y + Math.cos(fall) * by - Math.sin(fall) * 1.2,
			});
			m.hold(t + 450 + DOWN_MS + 150 * k, "prone");
			m.act("getUp", t + 450 + DOWN_MS + GET_UP_MS + 150 * k);
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
		const pickD = this.of(picker.at).d;
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
		} else if (td) {
			const crossT = this.crossingBack(picker, t);
			this.at.set(retI, crossT);
			this.whistle = crossT;
			picker.hold(t + 2200, "stand");
			this.pursue(others, pt, t, []);
		} else {
			this.endCarry(retI, picker, { pt, td: false }, t, others);
		}
	}

	crossingBack(m: Actor, fallback: number): number {
		const gx = this.input.dir === 1 ? GOAL_L : GOAL_R;
		for (const l of m.legs) {
			const a = (l.from.x - gx) * -this.input.dir;
			const b = (l.to.x - gx) * -this.input.dir;
			if (a < 0 && b >= 0) {
				return l.t0 + ((l.t1 - l.t0) * -a) / (b - a);
			}
		}
		return fallback;
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

	// Stripped in the pocket: the ball on the ground behind the line, and
	// somebody on it.
	strip(i: number, qb: Actor, t: number) {
		const forcer =
			this.man(this.statPids("defFmbFrc")[0]) ??
			this.nearest(this.d, qb.at, 1)[0];
		qb.hold(t, "qbSet", this.ahead);
		const down = this.downBy(qb, qb.at, t, this.d, forcer ? [forcer] : []);
		this.at.set(i, down.t);
		this.tackle(qb, down.by, qb.at, down.t);
		this.loose(i, qb.at, down.t);
	}

	// A fumble: the ball out on the ground from `pt` at t - and the
	// recovery the sim says, fallen on.
	loose(fumI: number, pt: Pt, t: number) {
		const r = this.r;
		const recI = this.find("fumbleRecovery", fumI);
		const ang = r() * Math.PI * 2;
		const rest = {
			x: pt.x + Math.cos(ang) * (5 + r() * 6),
			y: pt.y + Math.sin(ang) * (5 + r() * 6),
		};
		this.ball.push(
			{
				kind: "fly",
				t0: t,
				t1: t + 500,
				from: { ...pt, z: 2.5 },
				to: { ...rest, z: 0 },
				peak: 1.5,
			},
			{ kind: "spot", t0: t + 500, at: { ...rest, z: 0 } },
		);
		const rec = this.man(this.statPids("defFmbRec")[0]);
		let onIt = t + 1100;
		if (rec && !this.done.has(rec.pid)) {
			const go = Math.max(this.seen, t - 200);
			onIt = Math.max(
				onIt,
				go + (dist(rec.posAt(go), rest) / (rec.top * 0.9)) * 1000 + 350,
			);
			rec.cut(go);
			rec.hold(go);
			rec.chase(rest, onIt - 350, rec.top * FLAT_OUT, "sprint");
			rec.act("dive", onIt, rec.yaw, rest);
			this.ball.push({ kind: "held", t0: onIt, pid: rec.pid });
			rec.hold(onIt + 1400, "prone");
			rec.act("getUp", onIt + 1400 + GET_UP_MS);
			this.done.add(rec.pid);
		}
		if (recI >= 0) {
			this.at.set(recI, onIt + 200);
		}
		this.whistle = onIt + 200;
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
		for (const [m, b] of engaged) {
			const at = (t: number) => {
				const p = b.posAt(t);
				return { x: p.x + this.input.dir * 2.6, y: p.y + (r() - 0.5) * 0.6 };
			};
			m.hold(snap + 40 + r() * 60);
			// Into him as soon as he can get there.
			const far = dist(m.at, b.posAt(snap + 450));
			const hitT = Math.max(
				snap + 380 + r() * 150,
				m.t + (far / (m.top * 0.8)) * 1000,
			);
			m.go(at(hitT), hitT, "run");
			m.go(at(end), end, "engage", this.behind);
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

	// ---- after the whistle --------------------------------------------------------

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
}

// The play from the snap: what the sim says happened, acted out - or
// undefined, a play this does not act out (yet), left standing in its
// stances.
export const actOut = (input: ActInput): Acted | undefined =>
	new Action(input).run();
