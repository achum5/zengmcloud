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
	sideOn,
	TEAM_AREA,
	YD,
	type Dir,
	type Pt,
	type Pt3,
	type Side,
} from "./geometry.ts";
import { drawnBody, footballBody } from "./body.ts";
import {
	HIGH_FIVE_AT,
	handsMeet,
	heldBall,
	liftOf,
	moveKind,
	poseOfMove,
	type Move,
} from "./poses.ts";
import { SET } from "./pace.ts";
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
	// The man with it got out of bounds (see the director).
	out?: boolean;
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
	// A big play's moment after it (see celebrate): when it is over - and
	// when it starts, where.
	until?: number;
	party?: { from: number; at: Pt };
	// Where the man with it went out of bounds.
	out?: Pt;
};

// Where a carry ends: the spot, how far downfield, a score - or over a
// sideline (+1 the one on the offense's +a side).
type Sign = 1 | -1;
type End = { pt: Pt; td: boolean; d: number; out?: Sign };

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
// (From no slower than this, ft/s.)
const STOP_LEAST = 8;
// Sinking into a cut: down at this (ft/s/s).
const CUT_ACCEL = 28;
// (Easing down to steps this much slower, ft/s, or more - or for a turn
// sharper than this, radians, that he plants for: see slowFor.)
const SLOW_DROP = 6;
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
// One stretch of his, pulling up over it (see slowInto): legs no further
// round from each other than this (radians), none quicker than the next by
// more than this (ft/s), over no longer than this (ms) - and still there
// this long (ms) before he goes on, if he gets there later for it.
const STRETCH_TURN = 0.12;
const STRETCH_SLACK = 1.5;
const STRETCH_MS = 3000;
const HOLD_LEFT_MS = 150;
// (Down at as much as this many times as hard, if he has to be.)
const STOP_HARD = 1.5;
// (Cut short no more than this long into a leg, ms: none of it kept - or,
// in a gait, this long: kept as the one before it.)
const CUT_SLIVER_MS = 1;
const CUT_MERGE_MS = 60;
const GAITS = new Set<Move>(["walk", "jog", "run", "sprint"]);
const STEPS = new Set<Move>([...GAITS, "back", "slide", "shuffle"]);
// (Stood in it, or sat in it on the spot, alive under him - a receiver
// sat down for the ball.)
const sits = (m: Move) => moveKind(m) === "still" || m === "settle";
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
// (Kept on in push steps or a backpedal he was in, to HOLD_ON times as
// quick; turned and running on the same way already, back in them only
// under TURNED times as quick - not round and back each step.)
const HOLD_ON = 1.2;
const TURNED = 0.65;
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
		return { move, yaw: way + Math.PI };
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
			yaw: way + (d > 0 ? 1 : -1) * (Math.PI / 2),
		};
	}
	if (off > BACK_OFF && fps <= BACK_MOST * k) {
		return { move: "back", yaw: way + Math.PI };
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
// How far round (radians) going a to b and on to c turns him at b.
const angleAt = (a: Pt, b: Pt, c: Pt): number => {
	const t = Math.atan2(c.y - b.y, c.x - b.x) - Math.atan2(b.y - a.y, b.x - a.x);
	return Math.abs(Math.atan2(Math.sin(t), Math.cos(t)));
};

// Planted from v (ft/s) down to ve on the way he is going, where he is
// going `ahead` feet on along it: no further on than that (see CUT_ACCEL)
// - not past it and back. (The pace he is down to.)
const plantedTo = (v: number, ve: number, ahead: number): number =>
	Math.min(
		v,
		ahead > 0
			? Math.max(ve, Math.sqrt(Math.max(0, v * v - 2 * CUT_ACCEL * ahead)))
			: ve,
	);

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

// (Or as far as `roam` past a sideline.)
const onField = (p: Pt, roam = 0): Pt => ({
	x: clamp(p.x, 1, FIELD_L - 1),
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

// ---- one man's part, a stretch at a time ------------------------------------

class Actor {
	readonly part: Part;
	readonly top: number;
	// The way he faces at the end of what he has been given so far.
	yaw: number;
	// How far past a sideline he may go: out of bounds.
	roam = 0;
	// Till when where he has been is fixed - where he takes the ball, say:
	// no turn after it is bent into short of where he was then (see
	// bendInto).
	pinned = -Infinity;

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
			if (m === "stand" && RAMPS.has(p.move) && legPace(p) >= STOP_LEAST) {
				this.easeOff(Math.min(EASE_OFF_FT, (legPace(p) * (t - p.t1)) / 2000));
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
		const round =
			turn > PLANT_TURN ? Math.max(CUT_KEEP, (1 + Math.cos(turn)) / 2) : 1;
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
	private bendInto(to: Pt, w: number): boolean {
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
		const most = Math.sqrt(BEND_LAT * R);
		if (most < v * BEND_SLOW) {
			pace = this.slowInto(most / legPace(lk), CUT_ACCEL) ?? v;
		}
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
		const n = Math.max(1, Math.round((ta * 1000) / RAMP_STEP_MS));
		const dt = ta / n;
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

	// Pulled up to a stand by the end of the leg he is on, going at a pace
	// he could not stop dead from (see slowInto).
	pullUpOn() {
		const l = this.last;
		if (GAITS.has(l.move) && legPace(l) >= SLOW_DROP) {
			this.slowInto(0, STOP_ACCEL);
		}
	}

	// Broken down by the end of the leg he is on, into what he meets there:
	// down to `keep` of his pace over the last of it (see slowInto).
	breakDown(keep: number) {
		const l = this.last;
		if (GAITS.has(l.move) && legPace(l) * (1 - keep) >= SLOW_DROP) {
			this.slowInto(keep, STOP_ACCEL);
		}
	}

	// Cutting to `to` by t1 off the leg he is on: down to `keep` of his pace
	// over the last of it (see CUT_ACCEL), his foot planted, and off the new
	// way up from there.
	cutTo(to: Pt, t1: number, keep: number) {
		const ve = this.slowInto(keep, CUT_ACCEL);
		const leg = this.legTo(to, t1);
		this.rampInto(leg, ve);
		this.push(leg);
	}

	private legTo(
		to: Pt,
		t1: number,
		move?: Move,
		yaw?: number,
		extra?: Partial<Leg>,
	): Leg {
		const l = this.last;
		const p = onField(to, this.roam);
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
	private slowInto(keep = 0, A = STOP_ACCEL, spare = 0): number | undefined {
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
		if (T * 1000 < RAMP_LEG_MS || vl < STOP_LEAST || D <= 0) {
			return this.slowLast(keep, A);
		}
		const cos = (leg.to.x - first.from.x) / D;
		const sin = (leg.to.y - first.from.y) / D;
		const before = legs[j - 1];
		const vf = dist(first.from, first.to) / ((first.t1 - first.t0) / 1000);
		const vs =
			!before || SET.has(before.move)
				? vf
				: Math.min(vf, paceOn(before, first.from, leg.to));
		const ve = vl * keep;
		const Au = RAMP_ACCEL;
		const most = Math.max(vl, Math.min((D / T) * RAMP_OVER, RAMP_TOP));
		// (Up from vs to vc, on at it, and down to ve at Ad: as slow a vc as
		// gets him there on time - or, too quick, at no more than `most`, as
		// soon as that gets him there. Down at A if that does it, harder if
		// it takes it - see STOP_HARD.)
		const room = Math.max(0, spare - HOLD_LEFT_MS) / 1000;
		const plan = (Ad: number) => {
			const qa = 1 / (2 * Au) + 1 / (2 * Ad);
			const qb = T + vs / Au + ve / Ad;
			const qc = (vs * vs) / (2 * Au) + (ve * ve) / (2 * Ad) + D;
			const disc = qb * qb - 4 * qa * qc;
			let vc = disc >= 0 ? (qb - Math.sqrt(disc)) / (2 * qa) : Infinity;
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
				? { vc, Tn, ta, td, tc: Math.max(0, tc), Ad }
				: undefined;
		};
		const p = plan(A) ?? plan(A * STOP_HARD);
		if (!p) {
			return this.slowLast(keep, A);
		}
		const { vc, Tn, ta, td, tc, Ad } = p;
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
		const l = this.last;
		const v = legPace(l);
		const far = dist(l.from, l.to);
		if (!GAITS.has(l.move) || v < STOP_LEAST || t1 <= this.t) {
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
	plantFor(to: Pt, down?: number) {
		const l = this.last;
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
		const want = down ?? v * Math.max(CUT_KEEP, (1 + Math.cos(turn)) / 2);
		const ve = plantedTo(
			v,
			want,
			(to.x - l.to.x) * cos + (to.y - l.to.y) * sin,
		);
		// (No room to plant for it short of where he goes: bent round into
		// it, if he can be.)
		if (ve - want > (v - want) / 2 && this.bendInto(to, v)) {
			return;
		}
		if (!(ve < v)) {
			return;
		}
		const T = (v - ve) / CUT_ACCEL;
		const n = Math.max(1, Math.round((T * 1000) / STOP_STEP_MS));
		const dt = T / n;
		const paceOf = (i: number) => v - (v - ve) * ((i + 0.5) / n);
		const moveOf = slower(l.move, n, dt, paceOf);
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
		const t = this.t - ((2 * ft) / Math.max(1, v)) * 1000;
		if (RAMPS.has(l.move) && v >= least && t > l.t0) {
			this.cut(t);
			this.easeOff(2 * ft);
		}
	}

	// On the way he was going, slowing to a stop no more than `most` feet
	// on (see STOP_ACCEL) - bent off to the side of `past` (the pile at the
	// ball), as little as takes him by it, if he would run on through it.
	easeOff(most: number, past?: Pt) {
		const l = this.last;
		const ms = l.t1 - l.t0;
		const far = dist(l.from, l.to);
		if (ms <= 0 || far <= 0) {
			return;
		}
		const v = far / (ms / 1000);
		const d = Math.min(most, (v * v) / (2 * STOP_ACCEL));
		if (d < 0.5) {
			return;
		}
		const way = Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x);
		const gaited = GAITS.has(l.move);
		const T = (2 * d) / v;
		const n = Math.max(1, Math.round((T * 1000) / STOP_STEP_MS));
		const dt = T / n;
		const paceOf = (i: number) => v * (1 - (i + 0.5) / n);
		const moveOf = slower(l.move, n, dt, paceOf);
		const stepsOf = (bend: number) => {
			const out: Pt[] = [];
			let at = this.at;
			for (let i = 0; i < n; i++) {
				const h = way + (bend * (i + 0.5)) / n;
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
	private rampInto(leg: Leg, into = this.paceInto(leg.to)) {
		const T = (leg.t1 - leg.t0) / 1000;
		if (!RAMPS.has(leg.move) || T * 1000 < RAMP_LEG_MS) {
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

	// To `to` at `fps`; when he gets there.
	// (On the move already, and that a good deal quicker: up to it first -
	// see RAMP_ACCEL - there the later for it.)
	run(to: Pt, fps: number, move?: Move, yaw?: number): number {
		const p = onField(to, this.roam);
		if (move === undefined || RAMPS.has(move)) {
			this.bendInto(p, fps);
		}
		const from = this.at;
		const far = dist(from, p);
		// (Out of his stance: up from nothing, at RELEASE_ACCEL.)
		const into = this.paceInto(p);
		const v0 = into ?? 0;
		const g = move ?? gait(fps);
		let gone = 0;
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
		}
		if (far - gone > 0.01) {
			this.go(p, this.t + ((far - gone) / Math.max(1, fps)) * 1000, move, yaw);
		}
		return this.t;
	}

	// As run, to stop there: pulled up over the last of it, hard (see
	// STOP_HARD) - not stopped dead off it.
	runUp(to: Pt, fps: number, move?: Move): number {
		const p = onField(to, this.roam);
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
		const p = onField(to, this.roam);
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
		const p = onField(to, this.roam);
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
		this.pin(t1);
	}

	// Toward `to` by t1 at no more than `fps`, as chase - but getting up to
	// it from the pace he was going at (see RAMP_ACCEL) as he goes: there,
	// or as far as that gets him.
	chaseUp(to: Pt, t1: number, fps: number, move?: Move) {
		const p = onField(to, this.roam);
		// (Planted first, well round from the way he is going - all the
		// time there is, maybe.)
		this.plantFor(p);
		if (t1 - this.t < STOP_STEP_MS) {
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
			to: to ? onField(to, this.roam) : l.to,
			move,
			yaw: yaw ?? this.yaw,
		});
	}

	// Reaching for it, his hands on it at `at` (see CATCH_REACH_MS) - only
	// so much of the reach as there is still time for, if he gets to it
	// late: his hands there as it gets there, not up for it after.
	catchOn(at: number, yaw?: number, to?: Pt) {
		this.act("catch", at + CATCH_IN_MS, yaw, to);
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

// ---- the ball ---------------------------------------------------------------

// How a man has the ball, by what he is doing: over it, a center; cocking
// it to throw; in both hands in the pocket, catching it, handing it off,
// taking a knee; otherwise tucked away.
const IN_HANDS = new Set<Move>([
	"qbSet",
	"pocket",
	"handOff",
	"kneelDown",
	"catch",
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
const CATCH_Z = 4.3;
const G = 32;
// A pass: on a line at this pace (ft/s), lofted a little slower over this
// much more for each foot it goes, down to no slower than this.
const PASS_FPS = 70;
const PASS_LOFT = 0.05;
const PASS_LEAST_FPS = 60;
const flightOf = (from: Pt, to: Pt): { ms: number; peak: number } => {
	const l = dist(from, to);
	const v = Math.max(PASS_LEAST_FPS, PASS_FPS - l * PASS_LOFT);
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
// (No quicker about it than this, ms.)
const SWAT_LEAST_MS = 450;
// (A man breaking up a pass no nearer where it was to be caught than this,
// feet, for it.)
const SWAT_CLEAR = 2;
// A gunner set in front of the returner by this long after he would have
// got there at full tilt, ms.
const GUNNER_SET_MS = 900;

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
const BLOCK_CLEAR_FPS = 4;
// (Driven back, the block won, no quicker than this - ft/s.)
const BLOCK_DRIVE_FPS = 6;
// A lineman off the ball into the man on him: no nearer than this (feet),
// a step or two up into him, his arms out into him - not a run on the spot.
const FIRE_FT = 4;
const FIRE_HELD_MS = 250;
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
const DOUBLE_FT = 2.2;
const FILL_MS = 1050;
const FILL_PACE = 0.7;
const CLIMB_PACE = 0.85;
const SHED_MS = 550;
const SHED_SPREAD = 350;
const OFF_SPREAD = 350;
const STALK_FAR = 36;
const STALK_WON = 0.6;
// (The man he goes out onto - or a linebacker over to fill - pulled up
// there no more than this far on, feet.)
const STALK_EASE_FT = 5;

// After the ball: at this much of his speed at most - this much faster than
// he needs to be, to get there in time - a leg this long at a time (ms), at
// where the carrier will be this long after it, this far off him (feet),
// closing in as he goes down - each this far at most to one side or the
// other of the way he is going, a lane of his own.
const PURSUIT_PACE = 0.85;
const PURSUIT_URGE = 1.2;
const PURSUIT_STEP = 350;
const PURSUIT_LEAD = 300;
const PURSUIT_RING = 10;
// (In nearer him than that already, closing in this much a step, no nearer
// than so much.)
const PURSUIT_IN = 1.5;
const PURSUIT_NEAREST = 5;
const PURSUIT_LANE = 7;
// (Given up, or at the whistle: pulled up no more than this far on - feet;
// bent round off the pile at the ball, if it is in the way, by as little of
// these as (radians, all told) keeps him this far off it.)
const EASE_OFF_FT = 12;
const EASE_BENDS = [0.35, 0.7, 1.05, 1.4];
const EASE_PAST_FT = 3.5;
// (Carrying on the way he was going, before he eases off, ms - see carryOn.)
const CARRY_MOST_MS = 500;
// (Broken down a few steps off a man to hit, this long at least - see
// closeOn.)
const BREAK_DOWN_MS = 450;
// (Not broken down at all with less than this long, ms, to be set in it.)
const BREAK_SET_MS = 150;
// (No quicker than a walk, ft/s - see gait: going in on a man, or for the
// ball, not walked there.)
const WALK_MOST_FPS = 6.5;
// (And as slow as a jog up to it goes, ft/s - a little over that.)
const JOG_LEAST_FPS = 7.5;
// (A guard pulling down the line, no slower than this, ft/s - and a man in
// on a pile round a loose ball.)
const PULL_FPS = 12;
const PILE_RUN_FPS = 12;
// (Within a few steps of it already, and going no quicker than this (feet a
// second): broken down if he has this long before he goes in, this far off
// it, shuffled there at this pace.)
const BREAK_NEAR_MOST = 10;
const BREAK_NEAR_MS = 350;
const BREAK_NEAR_FT = 3;
const BREAK_NEAR_FPS = 6;
// (Off out of it into the hit up to speed (see GET_OFF_ACCEL) - this long
// before it at least, ms.)
const BREAK_FIRE_MS = 300;
// (His run slowed by more than this much over: in the gait he goes in now.)
const SLOW_RELABEL = 1.3;
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
const POINT_MS = 1000;

// A turnover: the man who came up with it mobbed this often.
const TAKEAWAY = 0.75;

// Out of bounds: the whistle as he gets this far over the sideline (feet),
// on another stride or two - no further over than OUT_MOST - and pulled up;
// the man on him there into him with both hands this often. Not from more
// than OUT_FAR across the field from it on a run, OUT_NEAR from a catch (or
// most of the way he goes downfield, if more).
const OUT_PAST = 1.5;
const OUT_ON = 7;
const OUT_MOST = 9;
// (Into a team area on the near sideline - or this near one - no further
// over than this: the men stood there between him and the camera.)
const OUT_MOST_NEAR = 4.5;
const OUT_CROWD = 10;
const SHOVE = 0.6;
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
const CATCH_IN_MS = 220;
// Picking it off going at least this fast: caught on the run, his arms
// up for it as he goes - not stopped dead to catch it.
const PICK_ON_FPS = 10;
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
const COVER_FRONT = 1;
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
const ZONE_DRIFT_FPS = 6;
const ZONE_STIR_FT = 1.5;
const ZONE_SIT_MS = 300;
const ZONE_EASE_LEAST_MS = 200;
// At the mesh, the quarterback this far to the side of the back's way
// through it (feet): the two of them drawn the size they are (see body.ts),
// his hands out to the ball, and not into each other. (Turned to it as he
// goes there, side on, so he is facing it when the back gets there.)
const MESH_APART = 3;
// (The quarterback's arms out with the ball from this long before the
// back gets to it, into him this far ahead of the middle of him.)
const HAND_OUT_MS = 200;
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
const RIDE_EDGE = 13;
// The line gives no ground nearer than this to where the quarterback sets
// (feet).
const POCKET_ROOM = 7;
// A back kept in to block sets up this far in front of where the
// quarterback sets and this far to his side of him - no nearer the ball
// than this (feet).
const BACK_SET_UP = 4.5;
const BACK_SET_OUT = 4;
const BACK_OFF_LINE = 9;
// (Further off it than this (feet): up into it at a run, at no more than
// this much of his top speed, and set there this long before he takes
// anybody on.)
const BACK_STEP_FT = 5;
const BACK_UP_PACE = 0.6;
const BACK_SET_MS = 150;
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
// mesh toward the way he came, in so many steps.)
const PA_ROUND = 0.4;
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
// given this much ground at most to meet him head on - see tackle.)
const IN_WAY_FT = 2.5;
const GIVE_MOST_FT = 8;
const GIVE_COS = -0.5;
// A tackler met him head on if he came at him from this near straight in
// front (the cosine of it); and then, this often, drove him back.
const HEAD_ON = 0.5;
const DRIVEN_BACK = 0.55;
// Come at from the side (the sine of it at least this): this often swung
// round as he goes down, about this far (radians) - away from him, down
// the way he is thrown.
const SWING_SIDE = 0.7;
// (A tackler from behind him: the cosine of it under this.)
const BEHIND = -0.3;
const SWING = 0.4;
const SWING_TURN = 1.6;
// One man in the secondary bringing him down from the side or behind: this
// often on his feet still after it, stood over him.
const UPRIGHT = 0.3;
// Driving or fighting off a man with nobody of the other side this near:
// nobody there; on at a walk, under this pace (feet a second), a jog over.
const AIR_BLOCK = 5;
const WALK_UNDER = 6;
const FIRST_STEP = 150;
// A man on the line with the man he had off him this long before the end
// of the blocking: up out of his block this long after.
const FREED_MS = 600;
const FREED_REACT = 200;
// The men who bring him down down no nearer one another than this.
const LANDED_APART = 2.4;
// Met head on, another man in on it by then (as late as this after, ms):
// this often stood up instead - held there and driven back this far (feet)
// over this long, the whistle on forward progress, him still on his feet.
const STOOD = 0.22;
const STOOD_LATE = 350;
const STOOD_BACK = 2.6;
const STOOD_MS = 1100;
const STOOD_APART = 1;
// (In on him off a run this quick or more, ft/s: the last of it eased off
// over twice this far, feet.)
const STOOD_IN_LEAST = 8;
const STOOD_IN_FT = 1.2;
// A cover man into the blocker he meets: down to this much of his pace
// as they meet (see breakDown).
const LOCK_KEEP = 0.5;
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
// Up for a ball at its highest, this often, thrown this deep at least
// (feet past the quarterback) with no more than this far to go after it,
// and a man on him (this near) or the end zone; how long he is up (ms), and
// how high it is caught (feet).
const LEAP = 0.5;
const LEAP_DEEP = 36;
const LEAP_YAC = 9;
const CONTESTED = 6;
const LEAP_MS = 760;
const LEAP_Z = 8.4;
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
const TO_LINE = new Set<Route["name"]>(["out", "comeback"]);
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
// The rush: working a blocker's shoulder, a step over (feet) this often
// (ms) - eased over and back, a step this long (ms) at a time, not swung
// like a pendulum; off the edge, on a tackle's outside shoulder (feet out,
// and in front of him), and round him by the end (feet behind).
const HAND_MS = 420;
const HAND_FT = 0.9;
const HAND_STEP_MS = 90;
const EDGE_OUT = 1.7;
const EDGE_ON = 1.6;
const EDGE_PAST = 1.8;
// (Up the arc onto him bent through so many points.)
const EDGE_ARC_PTS = 6;
// Off the ball into the man across from him: up to speed at GET_OFF_ACCEL
// ft/s/s, no quicker than RUSH_HIT_PACE of his top speed by the time he
// gets there - not at full tilt from his first step.
const GET_OFF_ACCEL = 28;
const RUSH_HIT_PACE = 0.7;
// (Square to the line over his first steps, coming up out of his stance -
// turned the way he goes once he is up.)
const GET_OFF_SQUARE_MS = 200;
// (The blocker knocked back by him, feet for each ft/s he hits him at - no
// more than JOLT_MOST - over JOLT_MS.)
const JOLT_PER_FPS = 0.06;
const JOLT_MOST = 1.1;
const JOLT_MS = 250;
const GANG = [0.5, 0.35];
const GANG_LATE = 250;
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
// this far off the middle of it (and up to this much more), no two in from
// nearer the same way than this (radians).
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
const PILE_OFF = 2.5;
const PILE_OFF_MORE = 1.1;
const PILE_APART = 0.6;
// (From further off than this, a run in first.) Over the top of it, the
// most of them, from the third man in on, each this likely - his middle
// this far off the middle of it.
const PILE_REACH = 6;
const PILE_TOPS = 2;
const PILE_OVER = 0.6;
const PILE_TOP_OFF = 0.7;
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
const STIFF_ARM = 0.5;
const JUKE_MS = 300;
const JUKE_ON = 2;
const JUKE_OVER = 5;
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
const GATHER_LEAST_MS = 400;
const GATHER_FALLEN_MS = 1400;
// A loose ball batted about: no nearer than this to the spot he dives from
// (feet), he is laid out for it from where he is, not stepping in first.
const BAT_RUN_LEAST_FT = 1.5;
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
const CLOSE_SLIVER_MS = 120;
// (A gait no longer than this between two others, ms: see noFlicks.)
const FLICK_MS = 150;
const FLAT_OUT = 0.97;
// A receiver down his route, at this much of his top speed - sat down at
// the end of it for the ball, up for it this long before it comes.
const ROUTE_PACE = 0.76;
const CATCH_LEAD_MS = 380;
// (With no more than this left of it to the catch (feet), no break.)
const ROUTE_LEFT = 0.4;
// A break in it sharper than CUT_LEAST (radians), cut: into it at as much
// of his pace as goes on the new way (half of it square to it), no less
// than CUT_KEEP of it.
const CUT_LEAST = 0.5;
const CUT_KEEP = 0.25;
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
const breakOf = (a: Pt, b: Pt, c: Pt, vIn: number, vOut: number): Break => {
	const turn = angleAt(a, b, c);
	if (turn <= CUT_LEAST) {
		return { keep: 1, down: 0, up: 0, v: vIn, accel: CUT_ACCEL };
	}
	const keep = Math.max(CUT_KEEP, (1 + Math.cos(turn)) / 2);
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
	outAt?: Pt;
	// The stats said of what is being acted out: those of its lines from
	// just after `lo` through `hi` - the play up to the first time the ball
	// is out, then each recovery in turn.
	lo: number;
	hi: number;

	// The men in a moment of their own after it (see celebrate), and where
	// they crowd round the man who made it.
	readonly mob = new Set<number>();
	mobAt?: Pt;
	party?: { from: number; at: Pt };
	// The man who brought the carrier down, the last time one did.
	downer?: Actor;
	// A pile on the carrier when he goes down (see pileUp): the most men in
	// it.
	heap = 0;

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
		this.watch();
		this.celebrate();
		this.regroup();
		this.noAirBlocks();
		this.noFlicks();
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
			...(this.party && this.until > 0 ? { party: this.party } : {}),
			...(this.outAt ? { out: this.outAt } : {}),
		};
	}

	// No stride or two of another gait between one and the next (a run
	// slowed into a block, its last step a jog - cut short by it): on in
	// the gait he was in. Nor a step of one between two of the same other
	// stride (a blocker's drive, a step on to the man): on in that, facing
	// the way he was.
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
				}
			}
		}
	}

	// Nobody there to block, or to fight off, all the while: a man still
	// driving into a man who has gone - the one he was on away after the
	// carrier, the man he went out to never there - eases up instead, on at
	// a walk or stood watching it.
	noAirBlocks() {
		for (const m of this.men) {
			const them = m.team === this.input.offense ? this.d : this.o;
			const air = m.legs.map((l) => {
				// (Not his first step out of his stance: low and hard, at
				// whoever comes.)
				if (
					(l.move !== "drive" && l.move !== "engage") ||
					l.t0 < this.input.snap + FIRST_STEP
				) {
					return false;
				}
				const steps = Math.max(1, Math.ceil((l.t1 - l.t0) / 100));
				for (let k = 0; k <= steps; k++) {
					const t = l.t0 + ((l.t1 - l.t0) * k) / steps;
					const at = m.posAt(t);
					if (them.some((o) => dist(o.posAt(t), at) < AIR_BLOCK)) {
						return false;
					}
				}
				return true;
			});
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
				if (air[i]) {
					const run = m.legs.slice(i, j);
					const a = run[0]!.from;
					const b = run.at(-1)!.to;
					const t0 = run[0]!.t0;
					const span = Math.max(1, run.at(-1)!.t1 - t0);
					const fps = dist(a, b) / (span / 1000);
					const move = fps < 1 ? "stand" : fps < WALK_UNDER ? "walk" : "jog";
					const on = (t: number) => ({
						x: a.x + ((b.x - a.x) * (t - t0)) / span,
						y: a.y + ((b.y - a.y) * (t - t0)) / span,
					});
					for (const l of run) {
						l.from = on(l.t0);
						l.to = on(l.t1);
						l.move = move;
					}
				}
				i = j;
			}
		}
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
					l.ball = l.arms
						? IN_HAND.has(l.arms)
							? "throw"
							: "hands"
						: holdFor(l.move);
					// (On his feet with it in both hands: up at his chest,
					// not swinging.)
					if (!l.arms && l.ball === "hands" && HELD_UP.has(l.move)) {
						l.arms = "qbSet";
					}
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

		// The route the ball goes to him on.
		const targetName =
			thrown && target
				? targetRoute(
						spotOf(target.role),
						airYds,
						r(),
						first === cmpI && !!this.input.out,
					)
				: undefined;
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
			const route = layRoute(name, start, inSign, catchD, this.bounds, u, v);
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
			const meshPt = this.w(
				gun
					? { d: qbStart.d + 0.5, a: side * 2.5 }
					: { d: qbStart.d - 9, a: side * 3 },
			);
			const qbMesh = this.w({
				d: this.of(meshPt).d,
				a: this.of(meshPt).a - side * MESH_APART,
			});
			// (Out of his stance to it, up to speed - see the rider in runPlay
			// - not there flat out off his first step.)
			const reach = rampedMs(
				dist(faker.at, meshPt),
				faker.top * 0.75,
				0,
				RELEASE_ACCEL,
			);
			fakeT = Math.max(
				Math.max(has, qb.t) +
					PA_MS / 2 +
					qbStepMs(dist(qb.at, qbMesh)) +
					r() * 150,
				snap + 150 + reach,
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
					d: Math.min(qbStart.d - dropD, this.of(qbMesh).d - (gun ? 4 : 7)),
					a: qbStart.a,
				});
				qb.go(setAt, qb.t + 250 + dist(qb.at, setAt) * 50, "back", this.ahead);
			}
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
				d: this.of(meshPt).d + past,
				a: this.of(meshPt).a + side * on * past,
			});
			const inFt = dist(into.from, into.to);
			if (on > 0 || inFt <= 0 || v <= 0) {
				faker.go(onTo, fakeT + 300, "run", on > 0 ? undefined : this.ahead);
			} else {
				// (Staying in to block: on off it round from the way he came
				// in, pulled up as he goes - not turned square in a stride at
				// full tilt and stopped dead in his set. Stepped off at even
				// times along it, as far each time as his pace then takes
				// him.)
				const via = {
					x: meshPt.x + ((into.to.x - into.from.x) / inFt) * far * PA_ROUND,
					y: meshPt.y + ((into.to.y - into.from.y) / inFt) * far * PA_ROUND,
				};
				const fine = [meshPt, ...this.curve(meshPt, via, onTo, 32)];
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
			target!.go(
				this.w({ d: this.of(setAt).d + 3, a: qbStart.a + slipSide * 3 }),
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
			);
			const hold =
				tr.settles && tr.name !== "screen" ? r() * SETTLE_HOLD_MS : 0;
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
			const due =
				tr.settles && tr.name !== "screen" && !slip
					? Math.min(least, onTime + SETTLE_MOST_MS)
					: least;
			catchAt = Math.max(onTime, ready + 300 + fl.ms, due) + hold;
			throwAt = catchAt - fl.ms;
			catchPt = this.runRoute(target, tr, go, catchAt) ?? catchPt;
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
			if (bootSide) {
				this.onTheRun(qb, bootSide, catchPt, throwAt);
			} else {
				this.reads(qb, target!, catchPt, throwAt - WIND_MS, runners);
				this.pocketThrow(qb, catchPt, throwAt);
			}
			const at = this.of(qb.posAt(throwAt));
			const release: Pt3 = {
				...this.w({ d: at.d + 0.8, a: at.a }),
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
			fakeT,
			first === cmpI ? Math.max(0, (gain - airYds) * YD) : 0,
		);
		const beaten = sacker ? this.man(this.statPids("skAlw")[0]) : undefined;
		this.passRush(
			rushers,
			blockers,
			qb,
			(thrown ? catchAt : outT) + 300,
			sacker && beaten ? [sacker, beaten] : undefined,
		);

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
	) {
		const r = this.r;
		const spare = windAt - qb.t;
		const to = this.toward(qb.at, catchPt);
		if (spare > LOOK_OFF_MS && r() < LOOK_OFF) {
			const at = qb.t + spare * 0.3;
			const others = runners
				.filter((m) => m !== target)
				.map((m) => this.toward(qb.at, m.posAt(at)))
				.filter(
					(y) => Math.abs(Math.atan2(Math.sin(y - to), Math.cos(y - to))) > 0.5,
				);
			const other = others[Math.floor(r() * others.length)];
			qb.hold(qb.t + spare * (0.35 + r() * 0.2), "pocket", other ?? this.ahead);
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
			qb.hold(windAt - PUMP_MS - 250 - r() * 150, "pocket", to);
			qb.act("pump", qb.t + PUMP_MS, to);
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
	pocketThrow(qb: Actor, catchPt: Pt, throwAt: number) {
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
	}

	// Rolled out round the edge, on at an easy run - the ball up in both
	// hands, then cocked and let go, still going - and slowing up after.
	onTheRun(qb: Actor, side: number, catchPt: Pt, throwAt: number) {
		const end = throwAt + 500;
		if (qb.t > end) {
			qb.cut(end);
		} else {
			const s = this.of(qb.at);
			const v = BOOT_FPS * 0.75;
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
		const q = this.of(qb.at);
		qb.go(
			this.w({ d: q.d + 1, a: q.a + side * 2 }),
			end + 450,
			"walk",
			this.toward(qb.at, catchPt),
		);
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
		for (const b of men) {
			const m = near.shift();
			if (!m) {
				return;
			}
			b.cut(from);
			const fps = b.top * 0.9;
			let meet: { t: number; at: Pt } | undefined;
			for (
				let c = Math.max(catchAt - 150, b.t + 300);
				c <= catchAt + 1500;
				c += 50
			) {
				const p = this.of(m.posAt(c));
				const to = this.w({ d: p.d - BLOCK_GAP, a: p.a });
				if ((dist(b.at, to) / fps) * 1000 <= c - b.t) {
					meet = { t: c, at: to };
					break;
				}
			}
			if (!meet) {
				continue;
			}
			b.go(meet.at, meet.t);
			m.cut(meet.t);
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
	routeMs(route: Route, speed: number, upto: number, v0 = 0): number {
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
				route.settles,
			);
			ms += stretchMs(go, routeFps(speed, k), cut, next);
			cut = next;
			along += go;
		}
		return ms;
	}

	// The break at the end of the kth stretch of a route (see breakOf) - none,
	// if it ends there (`ends`) or goes on no further: but for the time it
	// takes him to pull up there, if he sits down in it (`settles`).
	private breakAt(
		pts: Pt[],
		k: number,
		speed: number,
		ends: boolean,
		settles: boolean,
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
		const v0 = this.releasePace(m, route, release);
		m.hold(release);
		const target = catchAt !== undefined;
		const endAlong = target ? route.catchAlong : Infinity;
		const pace =
			catchAt !== undefined && !route.settles
				? clamp(
						this.routeMs(route, speed, route.catchAlong, v0) /
							Math.max(1, catchAt - release),
						0.6,
						1,
					)
				: 1;
		const pts = route.path.map((p) => this.w(p));
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
				route.settles,
			);
			const t1 = m.t + stretchMs(go, routeFps(speed, k), cut, next) / pace;
			if (until !== undefined && t1 > until) {
				// (Still cut into the last of it, as far as he gets: not
				// turned square at his pace.)
				const u = (until - m.t) / (t1 - m.t);
				const p = m.at;
				const part = { x: p.x + (to.x - p.x) * u, y: p.y + (to.y - p.y) * u };
				if (k > 1 && cut.keep < 1) {
					m.cutTo(part, until, cut.keep);
				} else {
					m.go(part, until);
				}
				return;
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
				m.cutTo(to, t1, cut.keep);
			} else {
				m.go(to, t1);
			}
			cut = next;
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
			const faceAt = (p: Pt) => (qb ? this.toward(p, qb.at) : this.behind);
			const t =
				catchAt !== undefined ? catchAt - CATCH_LEAD_MS : (until ?? m.t);
			const work = route.settles ? this.workTo(m, route, t) : undefined;
			if (work) {
				m.hold(m.t + work.sit, "settle", faceAt(m.at));
				m.go(work.at, t - WORK_RESET_MS, "slide", faceAt(m.at));
			}
			m.hold(Math.max(m.t, t), "settle", faceAt(m.at));
			return work ? m.at : undefined;
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
		// toward the man who brings him down.
		let across = this.of(catchPt).a + (r() - 0.5) * 24;
		const ta = this.tacklerA(rec, catchAt);
		if (ta !== undefined) {
			across += (ta - across) * 0.5;
		}
		let end = this.endOf(
			gain,
			across,
			e.td === true,
			e.safety === true,
			this.outSide(
				this.of(catchPt).a,
				Math.max(OUT_NEAR, (gain * YD - this.of(catchPt).d) * 0.8),
			),
		);
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
			r() < LEAP;
		// Or, on the run and down about where he would land laid out for it,
		// off his feet for it, some of the time - down there, then.
		const l = rec.last;
		const heading = Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x);
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
			r() < DIVE_CATCH;
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
		const inStride = !leap && !dive && !toe && this.reachFor(rec, catchAt, qb);
		// (Met in his stride, on past where it was thrown to: thrown to where
		// he is as it gets there - not in behind him.)
		const fly = this.ball.findLast((b) => b.kind === "fly");
		if (inStride && fly?.kind === "fly" && Math.abs(fly.t1 - catchAt) < 1) {
			const at = rec.posAt(catchAt);
			fly.to = { ...fly.to, x: at.x, y: at.y };
		}
		if (leap) {
			this.leapFor(rec, catchPt, catchAt, qb);
		} else if (dive) {
			this.diveFor(rec, end.pt, catchAt);
		} else if (toe) {
			this.toeTapFor(rec, end.pt, catchAt, qb);
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
			const tc = off + (inStride ? CATCH_BEAT_STRIDE_MS : CATCH_BEAT_MS);
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
	diveFor(rec: Actor, at: Pt, catchAt: number) {
		const t0 = catchAt - DIVE_BEFORE;
		rec.cut(t0);
		rec.hold(t0);
		rec.act("diveCatch", t0 + DIVE_CATCH_MS, rec.yaw, at);
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
	leapFor(rec: Actor, catchPt: Pt, catchAt: number, qb: Actor) {
		const t0 = catchAt - LEAP_MS / 2;
		rec.cut(t0);
		rec.hold(t0);
		const from = rec.at;
		const far = dist(from, catchPt);
		const ang = Math.atan2(catchPt.y - from.y, catchPt.x - from.x);
		const to =
			far > 1
				? {
						x: catchPt.x + Math.cos(ang) * far * 0.9,
						y: catchPt.y + Math.sin(ang) * far * 0.9,
					}
				: from;
		rec.act(
			"leapCatch",
			t0 + LEAP_MS,
			far > 4 ? ang : this.toward(from, qb.posAt(catchAt)),
			to,
		);
		const fly = this.ball.findLast((b) => b.kind === "fly");
		if (fly?.kind === "fly" && Math.abs(fly.t1 - catchAt) < 1) {
			fly.to = { ...fly.to, z: LEAP_Z };
		}
	}

	// Reaching for it as it comes - in stride, his arms up for it as he
	// runs on, or sat down in his route waiting on it, and in to his chest.
	// Whether in stride.
	reachFor(rec: Actor, catchAt: number, qb: Actor | undefined): boolean {
		const reach = catchAt - CATCH_REACH_MS;
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
				move: "catch",
				t0: reach,
				t1: catchAt + CATCH_IN_MS,
			});
			return true;
		}
		const face = qb ? this.toward(rec.at, qb.at) : rec.yaw;
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
		rec.catchOn(catchAt, face);
		return false;
	}

	// On a few steps the way he was going, slowing up.
	// From what he is doing to a stop over a step or two, a walk the way he
	// was going.
	slowUp(m: Actor) {
		const l = m.last;
		const ang =
			l.from.x === l.to.x && l.from.y === l.to.y
				? m.yaw
				: Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x);
		m.go(
			{ x: m.at.x + Math.cos(ang) * 2.5, y: m.at.y + Math.sin(ang) * 2.5 },
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
			d = this.ownGoalD - 2 - r() * 6;
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
			this.evade(carrier, by ?? [], chasers, t),
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
		this.gangUp(chasers, pile, down.t, down.by);
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
		const on = toe ? OUT_ON / 2 : OUT_ON;
		const p1 = { d: to.d + dd * on, a: over(to.a + side * da * on) };
		const p2 = {
			d: p1.d + Math.sign(dd || 1) * 3,
			a: over(p1.a + side * 1.5),
		};
		carrier.go(this.w(p1), down.t + 330, toe ? "jog" : "run");
		carrier.go(this.w(p2), down.t + 330 + 520, toe ? "walk" : "jog");
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
			return hit;
		};
		if (first) {
			const hit = inside(first, 0);
			if (r() < SHOVE) {
				// Both hands into him, and through it to the line.
				const on = this.of(hit);
				first.go(
					this.w({ d: on.d + dd * 3, a: on.a + side * 2.5 }),
					down.t + 380,
					"jog",
				);
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
		this.pursue(chasers, near, down.t, down.by, carrier);
	}

	// Making a man miss on the way, now and then on a long enough carry: one
	// of `chasers` (not the men who bring him down) there to meet him
	// partway along his last stretch - and a cut away from him, a plant and
	// off the other way, the man left on the turf behind him; or, coming at
	// his free arm, put on his back with a stiff arm; or a spin out of his
	// hands, or over the top of him gone low. Gives when the carry (to have
	// ended at t) ends now: a cut or a spin costs him a beat.
	evade(carrier: Actor, by: Actor[], chasers: Actor[], t: number): number {
		const r = this.r;
		const l = carrier.last;
		const { t1 } = l;
		const ms = t1 - l.t0;
		if (
			moveKind(l.move) !== "cycle" ||
			dist(l.from, l.to) < EVADE_FAR ||
			ms < EVADE_MS ||
			r() > EVADE
		) {
			return t;
		}
		// (Not the men the sim has bring him down.)
		const tacklers = this.statPids("defTckSolo", "defTckAst");
		const ang = Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x);
		const tc = l.t0 + ms * (0.3 + r() * 0.35);
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
			// (Coming at him, not from behind.)
			.filter(
				(x) =>
					x.t < tc - 250 &&
					Math.cos(Math.atan2(x.from.y - at.y, x.from.x - at.x) - ang) > -0.3,
			)
			.sort((a, b) => a.t - b.t)[0];
		if (!m) {
			return t;
		}
		return this.makeMiss(carrier, m.c, m.from, tc, l, t);
	}

	// `m`, coming from `from`, made to miss the carrier at tc on his stretch
	// `l` (see evade) - by the move `how`, if it has to be that one. Gives
	// when the carry (to have ended at t) ends now.
	makeMiss(
		carrier: Actor,
		m: Actor,
		from: Pt,
		tc: number,
		l: Leg,
		t: number,
		how?: "spin" | "juke",
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
		if (!how && side < 0 && r() < STIFF_ARM) {
			const hit = beside(at, ang - Math.PI / 2, 2.6);
			const face = this.toward(hit, at);
			this.closeOn(m, hit, tc - 80, face, carrier);
			m.act("tackledBack", tc + 520, face, beside(hit, face, -1.6));
			m.hold(tc + 520 + 600, "supine");
			m.act("getUpBack", tc + 520 + 600 + GET_UP_MS);
			carrier.arms("stiffArm", tc - 280, tc + 300);
		} else {
			const { to, move } = l;
			// (Off it as fast as he went into it.)
			const v = Math.min(carrier.top, dist(l.from, to) / (ms / 1000));
			const on = () =>
				carrier.go(
					to,
					Math.max(t1, carrier.t + (dist(carrier.at, to) / v) * 1000),
					move,
				);
			const u = how === "spin" ? HURDLE : how === "juke" ? 1 : r();
			const half = HURDLE_MS / 2;
			if (
				u < HURDLE &&
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
				this.closeOn(m, hit, tc - 300, face, carrier);
				m.act("dive", tc + 250, face, beside(at, face, 1.2));
				m.hold(tc + 250 + 700, "prone");
				m.act("getUp", tc + 250 + 700 + GET_UP_MS);
				this.done.add(m.pid);
				return t + carrier.t - t1;
			}
			if (u < HURDLE + SPIN) {
				// Got a hand on him: round on a planted foot, out of it, and on.
				const off = beside(
					beside(at, ang, SPIN_ON),
					ang - (side * Math.PI) / 2,
					SPIN_OVER,
				);
				carrier.cut(tc);
				carrier.act("spin", tc + SPIN_MS, ang, off);
				carrier.turn(side * Math.PI * 2);
				on();
				const hit = beside(at, toHim, out(2.2));
				const face = this.toward(hit, at);
				this.closeOn(m, hit, tc + 60, face, carrier);
				m.act("dive", tc + 520, face, beside(at, face, 0.2));
				m.hold(tc + 520 + 600, "prone");
				m.act("getUp", tc + 520 + 600 + GET_UP_MS);
				this.done.add(m.pid);
				return t + carrier.t - t1;
			}
			const off = beside(
				beside(at, ang, JUKE_ON),
				ang - (side * Math.PI) / 2,
				JUKE_OVER,
			);
			carrier.cut(tc);
			carrier.act("juke", tc + JUKE_MS, ang, off);
			if (side < 0) {
				// (To his right: the cut the other way round.)
				carrier.last.mirrored = true;
			}
			on();
			// (Off his feet at where he was.)
			const hit = beside(at, toHim, out(3.5));
			const face = this.toward(hit, at);
			this.closeOn(m, hit, tc - 100, face, carrier);
			m.act("dive", tc + 380, face, beside(at, face, 0.8));
			m.hold(tc + 380 + 700, "prone");
			m.act("getUp", tc + 380 + 700 + GET_UP_MS);
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
	slow(m: Actor, ms: number) {
		const l = m.last;
		const far = dist(l.from, l.to);
		if (far > 1 && moveKind(l.move) !== "act" && this.weave && ms > 300) {
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
			return on > 0 &&
				on < run &&
				off < IN_WAY_FT &&
				run - on + HIT_FT < GIVE_MOST_FT
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
			r() < DRIVEN_BACK;
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
			r() < UPRIGHT;
		const along = (p: Pt, f: number, s: number) => ({
			x: p.x + Math.cos(fall) * f - Math.sin(fall) * s,
			y: p.y + Math.sin(fall) * f + Math.cos(fall) * s,
		});
		const downAt = lying
			? carrier.at
			: swing
				? along(pt, 1.4, -from1 * 2)
				: along(pt, back ? -1.4 : 2.5, 0);
		if (keptUp) {
			this.runOn(carrier, 450);
		} else if (lying) {
			carrier.hold(t + 450 + DOWN_MS, "prone");
			carrier.act("getUp", t + 450 + DOWN_MS + GET_UP_MS);
			this.done.add(carrier.pid);
		} else {
			carrier.act(back ? "tackledBack" : "tackled", t + 450, fall, downAt);
			if (swing) {
				carrier.turn(-from1 * SWING_TURN * (0.8 + r() * 0.4));
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
			if (upright) {
				// Through him and up, over him.
				m.go(
					{ x: hit.x + Math.cos(face) * 1.6, y: hit.y + Math.sin(face) * 1.6 },
					t + 450,
					"drive",
					face,
				);
				m.hold(t + 450 + DOWN_MS + 300, "stand", face);
				this.done.add(m.pid);
				return;
			}
			// Down alongside him on the side he came in from, not on top of
			// him - the man who drove him back down across his legs.
			// (Two in from the same side not down in the one place: the next
			// over the other side of him, or further down his legs.)
			const landing = (by: number) =>
				swing
					? {
							x: downAt.x - Math.sin(fall) * by,
							y: downAt.y + Math.cos(fall) * by,
						}
					: !back
						? along(downAt, -1.2, by)
						: k === 0
							? along(downAt, 2.6, 2)
							: along(downAt, 0.6, by);
			const by = (Math.sin(ang - fall) > 0 ? 1 : -1) * 2.1;
			const free = (p: Pt) => landed.every((q) => dist(p, q) >= LANDED_APART);
			const lands =
				[landing(by), landing(-by), along(landing(by), -LANDED_APART, 0)].find(
					free,
				) ?? along(landing(by), -LANDED_APART * 2, 0);
			landed.push(lands);
			m.act("dive", t + 450, face, lands);
			m.hold(t + 450 + DOWN_MS + 150 * k, "prone");
			m.act("getUp", t + 450 + DOWN_MS + GET_UP_MS + 150 * k);
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
		if (more.length === 0 || r() > STOOD) {
			return false;
		}
		const end = t + STOOD_MS;
		const back = {
			x: -Math.cos(fall) * STOOD_BACK,
			y: -Math.sin(fall) * STOOD_BACK,
		};
		this.downer = by[0]!;
		carrier.go({ x: pt.x + back.x, y: pt.y + back.y }, end, "drive", fall);
		carrier.hold(end + 500, "stand", fall);
		this.done.add(carrier.pid);
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
		men.forEach(({ m, at }, k) => {
			const ang = angs[k]!;
			const hit = {
				x: pt.x + Math.cos(ang) * 2.3,
				y: pt.y + Math.sin(ang) * 2.3,
			};
			const face = this.toward(hit, pt);
			// (In on it where it has got to by then.)
			const there = Math.max(lead, at + 100);
			const u = clamp((there - t) / STOOD_MS, 0, 1);
			this.closeOn(
				m,
				{ x: hit.x + back.x * u, y: hit.y + back.y * u },
				there,
				face,
				carrier,
			);
			m.easeIn(STOOD_IN_FT, STOOD_IN_LEAST);
			// (Not into it for a moment and straight on to the drive.)
			if (t - m.t >= WAIT_LEAST_MS) {
				m.hold(t, "engage", face);
			}
			m.go({ x: hit.x + back.x, y: hit.y + back.y }, end, "drive", face);
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
			const off = Math.max(HIT_FT, Math.min(4.5, dist(from, pile)));
			const hit = {
				x: pile.x + Math.cos(ang) * off,
				y: pile.y + Math.sin(ang) * off,
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
		const on: { m: Actor; at: Pt; way: number; go: number }[] = [];
		const ways: number[] = [];
		near.forEach(({ m, from }, k) => {
			if (r() > PILE_IN[k]!) {
				return;
			}
			let way = Math.atan2(from.y - at.y, from.x - at.x) + (r() - 0.5) * 0.5;
			for (
				let n = 0;
				n < 6 &&
				ways.some(
					(b) =>
						Math.abs(Math.atan2(Math.sin(way - b), Math.cos(way - b))) <
						PILE_APART,
				);
				n++
			) {
				way += PILE_APART * (k % 2 === 0 ? 1 : -1);
			}
			ways.push(way);
			const far = PILE_OFF + r() * PILE_OFF_MORE;
			on.push({
				m,
				at: { x: at.x + Math.cos(way) * far, y: at.y + Math.sin(way) * far },
				way,
				go: t + PILE_FIRST + on.length * PILE_GO + r() * 100,
			});
		});
		if (on.length === 0) {
			return;
		}
		const n = on.length;
		let tops = 0;
		on.forEach((x, k) => {
			// (One of the last in, now and then, over the top of it - laid out
			// across it.)
			const top = k >= 2 && tops < PILE_TOPS && r() < PILE_OVER;
			if (top) {
				tops++;
				const far = PILE_TOP_OFF + r() * 0.5;
				x.at = {
					x: at.x + Math.cos(x.way) * far,
					y: at.y + Math.sin(x.way) * far,
				};
			}
			const face = this.toward(x.at, at);
			x.m.cut(x.go);
			x.m.hold(x.go);
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
			x.m.act(top ? "pileDive" : "pileIn", x.m.t + PILE_FALL_MS, face, x.at);
			const off = Math.max(
				x.m.t + 400,
				t + PILE_HOLD + (n - 1 - k) * PILE_PEEL + r() * 120,
			);
			x.m.hold(off, top ? "pileTop" : "overPile", face);
			x.m.act(top ? "offTop" : "offPile", off + OFF_PILE_MS, face);
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

	// A man to `hit` by `lead`, facing `face` there: leaving what he was
	// doing as late as he can and still get there at most of his speed (or
	// whenever gets him there easiest) - with time to spare, up to a few
	// steps off to break down, and in from there.
	closeOn(m: Actor, hit: Pt, lead: number, face: number, watch?: Actor) {
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
			const ve = plantedTo(
				v,
				v * Math.max(CUT_KEEP, (1 + Math.cos(turn)) / 2),
				(hit.x - at.x) * cos + (hit.y - at.y) * sin,
			);
			const T = (v - ve) / CUT_ACCEL;
			const d = ((v + ve) / 2) * T;
			const off = { x: at.x + cos * d, y: at.y + sin * d };
			return { at: off, t: c + T * 1000, v: (ve * (1 + Math.cos(turn))) / 2 };
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
			if (began.t0 >= this.seen) {
				c = began.t0;
			} else if (sits(m.legs[k - 1]!.move)) {
				m.cut(began.t0);
			}
		}
		m.cut(c);
		m.hold(c);
		m.plantFor(hit);
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
		const downBy =
			m.t +
			rampedMs(far - 5 - ease, v, paceOn(m.last, m.at, off)) +
			((2 * ease) / v) * 1000;
		if (far > 8 && far < m.top * 0.5 * secs && downBy <= fire - BREAK_SET_MS) {
			const u = (far - 5 - ease) / (far - 5);
			m.run(
				{ x: m.at.x + (off.x - m.at.x) * u, y: m.at.y + (off.y - m.at.y) * u },
				v,
			);
			m.easeOff(ease);
			m.hold(fire, "backerStance", look());
			m.getOff(hit, lead, 0, GET_OFF_ACCEL, "run", 0, face);
		} else if (
			far <= 8 &&
			legPace(m.last) < BREAK_NEAR_MOST &&
			fireNear - m.t >= nearMs + BREAK_NEAR_MS
		) {
			// (Right there already, and with time: squared up to him a step
			// or so off, his feet chopping, till he comes - not walked in
			// on him.)
			if (far > BREAK_NEAR_FT + 1) {
				const at = {
					x: hit.x + ((m.at.x - hit.x) / far) * BREAK_NEAR_FT,
					y: hit.y + ((m.at.y - hit.y) / far) * BREAK_NEAR_FT,
				};
				m.go(at, m.t + nearMs, "shuffle", face);
			}
			m.hold(fireNear, "backerStance", look());
			m.getOff(hit, lead, 0, GET_OFF_ACCEL, "run", 0, face);
		} else {
			// (Giving ground in front of him: backpedaling, square to him,
			// if he can go that quick so.)
			const gives =
				far > 0 &&
				Math.cos(Math.atan2(hit.y - m.at.y, hit.x - m.at.x) - face) < GIVE_COS;
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
					m.hold(lead - fireMs, "backerStance", look());
				}
				m.getOff(hit, lead, 0, GET_OFF_ACCEL, "run", 0, face);
			} else {
				m.press(
					hit,
					lead,
					m.top * FLAT_OUT,
					undefined,
					gives ? face : undefined,
				);
			}
		}
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
					m.chaseUp(
						{
							x: m.at.x + him.x - now.x - Math.cos(a) * by,
							y: m.at.y + him.y - now.y - Math.sin(a) * by,
						},
						c,
						m.top * PURSUIT_PACE,
					);
					continue;
				}
				m.chaseUp(
					{
						x: him.x + Math.cos(a) * off - (him.y - was.y) * side,
						y: him.y + Math.sin(a) * off + (him.x - was.x) * side,
					},
					c,
					clamp(need * PURSUIT_URGE, m.top * 0.45, m.top * PURSUIT_PACE),
				);
			}
			m.chaseUp(end(), arrive, m.top * 0.95);
		}
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
					{ x: him.x + Math.cos(a) * ring, y: him.y + Math.sin(a) * ring },
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
			// (Carried on a step or so with it, off his last stride - by the
			// ball, not through the man it was for.)
			const l = breakup.last;
			const far = dist(l.from, l.to);
			const ux = far > 0 ? (l.to.x - l.from.x) / far : 0;
			const uy = far > 0 ? (l.to.y - l.from.y) / far : 0;
			let on = Math.min(SWAT_ON_MOST, legPace(l) * SWAT_ON);
			while (
				on > 0.25 &&
				dist({ x: l.to.x + ux * on, y: l.to.y + uy * on }, catchPt) < SWAT_CLEAR
			) {
				on -= 0.25;
			}
			breakup.act("swat", catchAt + 250, this.toward(breakup.at, catchPt), {
				x: l.to.x + ux * on,
				y: l.to.y + uy * on,
			});
			this.done.add(breakup.pid);
			const ang = r() * Math.PI * 2;
			lands = {
				x: catchPt.x + Math.cos(ang) * 9,
				y: catchPt.y + Math.sin(ang) * 9,
			};
			if (this.reachFor(rec, catchAt, qb)) {
				this.runOn(rec, 600);
			}
			if (r() < WAVE_OFF) {
				this.slowUp(breakup);
				breakup.act(
					"sigWave",
					breakup.t + WAVE_MS,
					this.toward(breakup.at, qb.at),
				);
			}
		} else if (r() < 0.5) {
			// Off his hands.
			if (this.reachFor(rec, catchAt, qb)) {
				this.runOn(rec, 600);
				if (r() < RUE) {
					this.slowUp(rec);
					rec.act("handsOnHelmet", rec.t + RUE_MS);
				}
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
		// (Not on in at it after, all of them round the man it was for.)
		for (const m of coverage) {
			const seen = ground + DEAD_SEEN_MS[0] + r() * DEAD_SEEN_MS[1];
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
		m.press(spot, catchAt - early, m.top * FLAT_OUT, "sprint");
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
		const ball = this.ball.at(-1);
		const fly = ball?.kind === "fly" ? ball : undefined;
		const onRun =
			GAITS.has(picker.last.move) && legPace(picker.last) >= PICK_ON_FPS;
		if (onRun) {
			this.reaches.push({
				m: picker,
				move: "catch",
				t0: catchAt - CATCH_REACH_MS,
				t1: catchAt + CATCH_IN_MS,
			});
		} else {
			picker.catchOn(catchAt, this.toward(picker.at, catchPt));
		}
		if (fly) {
			fly.to = { ...picker.at, z: CATCH_Z };
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
		// (Caught on the run: where he was as it got to him.)
		if (onRun && fly) {
			fly.to = { ...picker.posAt(catchAt), z: CATCH_Z };
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
		qb.hold(t - 800, "pocket", this.ahead);
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
		qb.hold(Math.max(qb.t, t - Math.max(800, step)), "pocket", this.ahead);
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
		// (Up to speed for it off what he was doing - out of his stance, from
		// nothing.)
		const into = SET.has(rec.last.move)
			? undefined
			: paceOn(rec.last, rec.at, restPt);
		const reach =
			go +
			rampedMs(
				dist(rec.at, restPt),
				rec.top * 0.92,
				into ?? 0,
				into === undefined ? RELEASE_ACCEL : RAMP_ACCEL,
			);
		const takeT = scoop
			? Math.max(restT, reach)
			: Math.max(restT + 100, reach + 350);
		this.gather(
			others.filter((m) => m !== from),
			restPt,
			t,
			scoop ? takeT : takeT + GATHER_FALLEN_MS,
		);
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
				const from = m.posAt(this.seen);
				const yaw = this.toward(from, bat);
				const off = Math.min(3.5, dist(from, bat));
				const short = {
					x: bat.x - Math.cos(yaw) * off,
					y: bat.y - Math.sin(yaw) * off,
				};
				// (Up to speed from what he was doing, a few strides of it -
				// or, hardly any way to go, laid out from where he is.)
				const run = dist(from, short);
				const l = m.legs.findLast((x) => x.t0 < this.seen && x.t1 > x.t0);
				const v0 = l && RAMPS.has(l.move) ? paceOn(l, from, short) : 0;
				const A = v0 > 0 ? RAMP_ACCEL : RELEASE_ACCEL;
				const at = Math.max(
					batT + k * 160,
					this.seen +
						(run > BAT_RUN_LEAST_FT
							? rampedMs(run, m.top * FLAT_OUT, v0, A)
							: 0) +
						300,
				);
				if (at > stopT - 200) {
					return;
				}
				m.cut(this.seen);
				m.hold(this.seen);
				if (run > BAT_RUN_LEAST_FT && at - 300 > m.t) {
					m.getOff(short, at - 300, v0, A, "sprint");
				} else if (off > TURN_IN_FT) {
					m.hold(at - 300);
				} else {
					m.hold(at - 300, undefined, yaw);
				}
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
			const lands = fallOn(restPt, yaw);
			const short = {
				x: lands.x - Math.cos(yaw) * FALL_ON_FT,
				y: lands.y - Math.sin(yaw) * FALL_ON_FT,
			};
			rec.press(short, takeT - 350, rec.top * FLAT_OUT, "sprint");
			rec.act("dive", takeT, yaw, lands);
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
		const down = takeT - SCOOP_DOWN_MS;
		if (off < down && rec.t === takeT) {
			rec.split(down);
			rec.joinFrom(down);
			Object.assign(rec.last, {
				move: "scoop",
				span: [0, SCOOP_LOW],
				yaw,
			});
		}
		const endPt = this.w({
			d: endD,
			a: clamp(
				a + (r() - 0.5) * 16,
				this.bounds.left + 6,
				this.bounds.right - 6,
			),
		});
		const on = Math.atan2(endPt.y - rec.at.y, endPt.x - rec.at.x);
		const up = rec.last.move === "scoop" ? SCOOP_LOW : 0;
		rec.act("scoop", takeT + SCOOP_UP_MS * (1 - up), on, {
			x: rec.at.x + Math.cos(on) * 5 * (1 - up),
			y: rec.at.y + Math.sin(on) * 5 * (1 - up),
		});
		rec.last.span = [up, 1];
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

	// Everybody about nothing else as it comes loose (at t): in toward it, a
	// beat after it is out, each from his own way - to stand off round where
	// it ends up, the man going for it in among them - until `until` (after
	// whoever comes up with it, then: see pursue). Not stood about where they
	// were while it is loose about them.
	gather(men: Actor[], at: Pt, t: number, until: number) {
		const r = this.r;
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
				continue;
			}
			const to = {
				x: at.x + Math.cos(ang) * ring,
				y: at.y + Math.sin(ang) * ring,
			};
			m.plantFor(to);
			if (m.t + (dist(m.at, to) / pace) * 1000 <= until) {
				m.run(to, pace);
			} else {
				m.chaseUp(to, until, pace);
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
		// back kept in, up beside the quarterback (two of them, one either
		// side of him) - but never up in among the line.
		const backSides = new Set<number>();
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
			if (inBack) {
				side = backSides.has(side) ? -side : side;
				backSides.add(side);
			}
			b.hold(snap + 60);
			// (A back through a fake already: set where it left him.)
			const setPt =
				b.t > snap + 300
					? { d: s.d - 1, a: s.a }
					: inBack
						? {
								d: Math.min(pocket.d + BACK_SET_UP, -BACK_OFF_LINE),
								a: pocket.a + side * BACK_SET_OUT,
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
				b.go(this.w(setPt), t1);
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
				: Math.min(2 + r() * 4, Math.max(0, setPt.d - pocket.d - POCKET_ROOM));
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
			// Into him as soon as he can get there out of his stance (see
			// GET_OFF_ACCEL).
			const least = snap + 380 + r() * 150;
			const offMs = (far: number) =>
				rampedMs(far, m.top * RUSH_HIT_PACE, 0, GET_OFF_ACCEL);
			let hitT = least;
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
			// (Up the arc first, off the edge, and in onto him.)
			const arc = edge ? this.w({ d: s.d - 2, a: s.a + side * 1.5 }) : m.at;
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
			hitT = Math.min(hitT, Math.max(least, end - HAND_MS));
			const hitLat = (r() - 0.5) * 0.6;
			const hit = on(hitT, hitLat);
			// (Bent up round it onto him, not off one way and turned the
			// other.)
			const vHit = edge
				? this.getOffAlong(
						m,
						this.curve(m.at, arc, hit, EDGE_ARC_PTS),
						hitT,
						GET_OFF_ACCEL,
						m.top * RUSH_HIT_PACE,
						"run",
						GET_OFF_SQUARE_MS,
					)
				: m.getOff(hit, hitT, 0, GET_OFF_ACCEL, "run", GET_OFF_SQUARE_MS);
			const per = HAND_MS * (0.8 + r() * 0.5);
			const amp = edge ? 0 : HAND_FT * (0.5 + r());
			let way = r() < 0.5 ? 1 : -1;
			const turns: { t: number; lat: number }[] = [];
			for (let t = hitT + per; t < end - per / 2; t += per) {
				turns.push({ t, lat: way * amp });
				way = -way;
			}
			turns.push({ t: end, lat: 0 });
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
			const held = steps.map((x) => b.posAt(x.t));
			// (Knocked back a step as he gets into him, the two of them - see
			// JOLT_PER_FPS - and that ground made up as he gives the rest.)
			const moves = !moved.has(b) && !this.done.has(b.pid);
			const jolt = moves ? Math.min(JOLT_MOST, vHit * JOLT_PER_FPS) : 0;
			const shove = (t: number) => {
				const u = clamp((t - hitT) / JOLT_MS, 0, 1);
				const left = clamp(1 - (t - hitT) / Math.max(1, end - hitT), 0, 1);
				return jolt * (1 - (1 - u) * (1 - u)) * left;
			};
			for (const x of steps) {
				const p = on(x.t, x.lat);
				p.x -= dir * shove(x.t);
				m.go(
					p,
					x.t,
					"engage",
					edge ? this.toward(p, qb.posAt(x.t)) : this.behind,
				);
			}
			// (A man doubled moves with the first of them.)
			if (moves) {
				moved.add(b);
				b.cut(hitT);
				steps.forEach((x, k) => {
					b.go(
						{
							x: held[k]!.x - dir * shove(x.t),
							y: held[k]!.y + dir * x.lat * 0.6,
						},
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
		fake?: number,
		yac = 0,
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
					const cushion =
						COVER_MIN +
						COVER_SOFT * Math.log1p(Math.exp((room - COVER_MIN) / COVER_SOFT));
					const to = this.w({ d: p.d + cushion, a: p.a + inside * 1.8 });
					const front = this.of(m.at).d > p.d + COVER_FRONT;
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
				// Back into his zone: deep middle, a deep half, under the
				// routes over the middle.
				const zone =
					m.role === "FS"
						? { d: Math.max(start.d, 42 + r() * 12), a: start.a * 0.4 }
						: m.role === "SS" || m.role === "CB"
							? { d: Math.max(start.d, 36 + r() * 10), a: start.a * 0.9 }
							: { d: 18 + r() * 10, a: start.a * 0.8 };
				m.hold(snap + 150);
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
				// there at a backpedal's pace, not stepping in place.)
				const spot = this.w({
					d: Math.max(zone.d, start.d + READ_STEPS_FT),
					a: zone.a,
				});
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
				let fps = Math.min(m.top * 0.6, (far + ease) / T);
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
				m.chase(
					settleAt,
					by - ((2 * ease) / Math.max(1, fps)) * 1000,
					fps,
					bail ? gait(fps) : "back",
					bail ? undefined : this.behind,
				);
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
					m.hold(t, "backerStance", this.behind);
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
					const u =
						near && nd < ZONE_SEE_FT
							? Math.min(ZONE_REACH_FT, nd * ZONE_LEAN) / nd
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
					const spot =
						spots.find((p) => closing.every((q) => dist(q, p) > 3.5)) ??
						spots.at(-1)!;
					closing.push(spot);
					// (Planted first, going the other way - see plantFor - and up
					// to speed out of it.)
					m.plantFor(spot);
					const v0 = paceOn(m.last, m.at, spot);
					const due = catchAt + 150;
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
					if (
						D < ZONE_STIR_FT &&
						due - m.t >= BREAK_SET_MS &&
						legPace(m.last) < STOP_LEAST
					) {
						// (There already: broken down where he is.)
						m.hold(due, "backerStance", this.toward(m.at, to));
					} else if (fps > 0 && due >= quick) {
						// (Up to it on the break, pulled up there, and set square
						// to it the while - or, only a moment over, there that
						// much later, on his feet: see hold.)
						m.runUp(spot, fps);
						m.hold(
							due,
							due - quick >= BREAK_SET_MS ? "backerStance" : undefined,
							due - quick >= BREAK_SET_MS ? this.toward(spot, to) : undefined,
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
		// (Out of bounds at the end of it: as a rule a run to the edge.)
		const toEdge = !sneak && !!this.input.out && yds < 15;
		const outside = !sneak && (toEdge || r() < 0.35);
		const cs = this.of(carrier.at);
		const sweep = spotOf(carrier.role) !== "back" && carrier !== qb;
		// The quarterback keeping it (not a sneak), as a rule off a ride: the
		// read option in the gun, a fake to the back under center - the back
		// one way, and he the other, round the edge. (Under center, always:
		// the back too far behind him to get out in front of him.)
		const rider =
			carrier === qb && !sneak && (r() < QB_RIDE || !gun)
				? this.o.find((m) => m.role === "RB" && !this.done.has(m.pid))
				: undefined;
		const holeA = sneak
			? (r() - 0.5) * 3
			: sweep
				? -Math.sign(cs.a || 1) * (18 + r() * 6)
				: rider
					? strong * (RIDE_EDGE + r() * 6)
					: strong * (outside ? 15 + r() * 6 : 2.5 + r() * 7);
		const blockersBack = this.statPids("rba", "rbw");
		// Up front: a sneak, outside zone on a run to the edge - or inside,
		// zone, or now and then power.
		const scheme: RunScheme = sneak
			? "sneak"
			: outside || sweep
				? "stretch"
				: r() < POWER
					? "power"
					: "zone";

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
			a: this.of(meshPt).a - (sweep ? side : strong) * MESH_APART,
		});
		const lead = cs.a === 0 && cs.d < -10 ? 0 : 150;
		let mesh =
			carrier === qb
				? has
				: sweep
					? has + (gun ? 150 : 250) + r() * 100
					: Math.max(
							has + 180 + qbStepMs(dist(qb.at, qbMesh)) + r() * 150,
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
			const set = mesh - 60;
			const out = Math.min(set, Math.max(qb.t + 1, mesh - HAND_OUT_MS));
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
			// (Into his hands as they come to the quarterback's: just short of
			// the mesh, out in front of him.)
			this.ball.push({
				kind: "held",
				t0: mesh - TAKE_LEAD_MS,
				pid: carrier.pid,
			});
			carrier.pin(mesh - TAKE_LEAD_MS);
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
					sweep ? carrier.last.t0 : snap + lead + 100,
				),
				t1: mesh + TAKE_AFTER_MS,
			});
		}

		if (rider) {
			const way = -strong;
			const riderMesh = this.w(
				gun ? { d: qs.d + 0.5, a: way * 2.5 } : { d: qs.d - 7, a: way * 3 },
			);
			const qbRide = this.w({
				...this.of(riderMesh),
				a: this.of(riderMesh).a - way * MESH_APART,
			});
			// (Out of his stance to it, up to speed: see the back below.)
			const reach = rampedMs(
				dist(rider.at, riderMesh),
				rider.top * 0.75,
				0,
				RELEASE_ACCEL,
			);
			const rideT = Math.max(
				Math.max(has, qb.t) +
					RIDE_MS / 2 +
					qbStepMs(dist(qb.at, qbRide)) +
					r() * 150,
				snap + 150 + reach,
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
			const rm = this.of(riderMesh);
			rider.runUp(
				this.w({ d: rm.d + RIDE_PAST, a: rm.a + way * 3 }),
				rider.top * 0.7,
			);
			rider.hold(rider.t + 600, "stand");
			rider.arms("fakeCarry", rideT - 200, rideT + 500);
			this.done.add(rider.pid);
		}

		// Through the hole and on to where the sim says he was brought
		// down.
		let across = holeA + (r() - 0.5) * (yds > 8 ? 30 : 8);
		if (ta !== undefined) {
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
			// Pressing the hole, patient - then through it (on a sweep or a
			// keeper, flat out for the edge; on outside zone, hard for it).
			carrier.run(
				holePt,
				carrier.top * (sweep || rider ? 0.88 : outside ? 0.76 : 0.64),
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
		this.runBlock(
			carrier,
			holeA,
			end.pt,
			down.t,
			blockersBack,
			scheme,
			down.by,
		);
		// (Short and up the middle, like as not a pile on him - or, in from
		// right there, over the top of it.)
		const short = sneak || yds <= 1 || end.td;
		if (
			!end.out &&
			scheme !== "stretch" &&
			yds <= PILE_YDS &&
			r() < (end.td ? OVER_TOP : short ? PILE_SHORT : PILE)
		) {
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
		this.heap = 0;
	}

	// Up front on a run, the sim's blocks: each man on the line on whoever
	// is over him - driving him back if the sim has him win it, pushed back
	// and beaten if not, the man he had off him in a moment to go after the
	// ball; one with nobody over him up to the next level, onto a
	// linebacker (or, none left, in on the double team beside him); on a
	// power play the backside guard pulled round to the hole and up it, as
	// a lead back goes up it, on whoever fills it; the receivers out onto
	// the corners and safeties.
	runBlock(
		carrier: Actor,
		holeA: number,
		endPt: Pt,
		t: number,
		backBlockers: number[],
		scheme: RunScheme,
		tacklers: Actor[],
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
		const until = t + 300;
		const atHole = Math.min(this.reachesAt(carrier, 0), t);
		// (Off a block to go after him as he goes by a man `d` deep, well
		// before he is down.)
		const offAt = (d: number) => {
			const by = this.reachesAt(carrier, d - 3);
			return by < t - 400
				? Math.min(t - 250, by + 150 + r() * OFF_SPREAD)
				: until;
		};

		// The first step, all as one: across to the hole on zone, flat down
		// the line on outside zone, down away from the hole on power - and
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
						: { d: 3, a: 2 * side };
		const backs = blockers.filter((b) => b !== pull && this.of(b.at).d < -8);
		const front = blockers.filter((b) => b !== pull && !backs.includes(b));
		for (const b of front) {
			const s = this.of(b.at);
			b.hold(snap + 40);
			b.go(
				this.w({
					d: s.d + step.d * (drive > 1 ? 1 : 0.5) + drive * 0.3,
					a: s.a + step.a,
				}),
				snap + 550,
				"drive",
				this.ahead,
			);
		}

		// Who is on whom up front.
		const on = new Map<Actor, Actor[]>();
		for (const m of this.d) {
			if (!DL.has(m.role) || this.done.has(m.pid)) {
				continue;
			}
			const b = closest(front, m.at);
			if (b) {
				on.set(b, [...(on.get(b) ?? []), m]);
			}
		}
		for (const [b, men] of on) {
			const s = this.of(b.at);
			const wins = beat(b);
			if (wins) {
				b.go(
					this.w({ d: s.d + drive + 1, a: s.a + step.a * 0.4 }),
					until,
					"drive",
					this.ahead,
				);
			} else {
				b.go(
					this.w({ d: s.d - 1.6, a: s.a }),
					snap + 1150,
					"drive",
					this.ahead,
				);
				b.go(this.w({ d: s.d - 2.4, a: s.a }), until, "drive", this.ahead);
			}
			let locked = until;
			let freed = snap;
			for (const m of men) {
				const at = (time: number, lat: number) => {
					const p = this.of(b.posAt(time));
					return this.w({ d: p.d + BLOCK_GAP, a: p.a + lat });
				};
				// (Beaten, off him - if he is not the one to make the tackle,
				// who goes from wherever he is when he goes.)
				const shed = snap + SHED_MS * 1.6 + r() * SHED_SPREAD;
				const end =
					wins || !free(m) || shed > t - 300
						? wins && free(m)
							? offAt(this.of(m.at).d)
							: until
						: shed;
				m.hold(snap + 50);
				const lat = (r() - 0.5) * 0.6;
				this.fireInto(m, (time) => at(time, lat), snap + 450, end);
				m.go(at(end, (r() - 0.5) * 0.6), end, "engage", this.behind);
				locked = Math.min(locked, end);
				freed = Math.max(freed, end);
			}
			this.jostle([b, ...men], snap + JOSTLE_FROM, locked);
			// (Nobody left on him well before it is over: up out of it.)
			if (freed < until - FREED_MS) {
				b.cut(freed + FREED_REACT);
			}
		}

		// The linebackers: a read step, then over to fill - but for one to
		// make the tackle, who goes from where he is.
		const backers = this.d.filter((m) => LB.has(m.role) && free(m));
		const filled = snap + FILL_MS;
		for (const m of backers) {
			const s = this.of(m.at);
			m.hold(snap + 80);
			m.go(
				this.w({ d: s.d - 1.5, a: s.a + side }),
				snap + 450,
				"slide",
				this.behind,
			);
			const fill = {
				d: Math.max(3, s.d - (drive > 1 ? 4 : 7)),
				a: holeA + (s.a - holeA) * 0.4,
			};
			const across = Math.abs(fill.a - s.a);
			m.chase(
				this.w(fill),
				filled + r() * 250,
				m.top * FILL_PACE,
				across > 5 ? "run" : "slide",
				across > 5 ? undefined : this.behind,
			);
			// (Pulled up there, nobody on him yet - see STALK_EASE_FT.)
			if (across > 5) {
				m.easeOff(STALK_EASE_FT);
			}
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
			const by = Math.max(atHole - 250, snap + 700);
			if (
				(dist(pull.at, pre) / Math.max(1, by - pull.t)) * 1000 <
				WALK_MOST_FPS
			) {
				// (Flat down the line at a run all the same - not walked there -
				// and set at the hole for the back.)
				pull.run(pre, PULL_FPS, "run");
				pull.setInto(by, "passSet", this.ahead);
			} else {
				pull.chase(pre, by, pull.top * 0.9, "run");
			}
			pull.chase(
				this.w({ d: 2, a: holeA - side }),
				pull.t + 250,
				pull.top * 0.9,
				"run",
			);
			this.blockOn(pull, pick(holePt), beat(pull), offAt, until, carrier);
		}
		for (const b of backs) {
			b.hold(snap + 40);
			b.chase(
				this.w({ d: -1, a: holeA - side }),
				Math.max(atHole - 300, snap + 500),
				b.top * 0.85,
				"run",
			);
			this.blockOn(b, pick(holePt), beat(b), offAt, until, carrier);
		}
		// Nobody over him: up onto a linebacker - or, none left, in beside
		// the man on the nearest one, on him too.
		for (const b of front) {
			if (on.has(b)) {
				continue;
			}
			const m = pick(b.at);
			if (m) {
				this.blockOn(b, m, beat(b), offAt, until, carrier);
				continue;
			}
			const mate = closest([...on.keys()], b.at);
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
			b.chase(beside(snap + 800), snap + 800, b.top * 0.8, "drive", this.ahead);
			b.go(beside(until), until, "drive", this.ahead);
		}

		// The receivers: out onto the nearest man off the ball, and on him -
		// or, nobody near, on downfield.
		const dbs = this.d.filter((m) => DB.has(m.role) && free(m));
		for (const m of this.o) {
			if (this.done.has(m.pid) || m === carrier || blockers.includes(m)) {
				continue;
			}
			const s = this.of(m.at);
			m.hold(snap + 80);
			const db = closest(dbs, m.at);
			if (!db || dist(db.at, m.at) > STALK_FAR) {
				m.go(
					this.w({ d: s.d + 9 + r() * 8, a: s.a * 0.92 }),
					snap + 1300,
					"run",
				);
				// (Pulled up there, a step or two on.)
				m.easeOff(STALK_EASE_FT);
				m.hold(until, "drive", this.ahead);
				continue;
			}
			dbs.splice(dbs.indexOf(db), 1);
			// (He backs off at the snap, reading it, then up.)
			const c = this.of(db.at);
			db.hold(snap + 80);
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
			m.go(
				this.w({ d: s.d + 4, a: s.a + (c.a - s.a) * 0.3 }),
				snap + 650,
				"run",
			);
			this.blockOn(m, db, r() < STALK_WON, offAt, until, carrier);
		}
	}

	// A blocker onto a man off the line: as soon as he can be in front of
	// him, then on him - driving him back if he wins it; if not, the man
	// off him in a moment. Till `until` - or the man off it as the carrier
	// goes by, after him.
	blockOn(
		b: Actor,
		m: Actor | undefined,
		won: boolean,
		offAt: (d: number) => number,
		until: number,
		clear?: Actor,
	) {
		const r = this.r;
		let meet: { t: number; at: Pt } | undefined;
		if (m) {
			// (Up to speed for it off what he is doing - see rampedMs.)
			const fps = b.top * CLIMB_PACE;
			for (let c = b.t + 100; c <= until; c += 50) {
				const p = this.of(m.posAt(c));
				const at = this.w({ d: p.d - BLOCK_GAP, a: p.a });
				const far = dist(b.at, at);
				if (rampedMs(far, fps, paceOn(b.last, b.at, at)) <= c - b.t) {
					meet = { t: c, at };
					break;
				}
			}
		}
		if (!m || !meet) {
			// (Up to the next level, and nobody there.)
			const s = this.of(b.at);
			b.chase(this.w({ d: s.d + 9, a: s.a }), b.t + 900, b.top * 0.6, "run");
			b.hold(until, "stand", this.ahead);
			return;
		}
		b.go(meet.at, meet.t);
		m.cut(meet.t);
		m.hold(meet.t);
		const p = this.of(m.at);
		const off = Math.max(meet.t + 200, offAt(p.d));
		const end = won ? off : Math.min(off, meet.t + SHED_MS + r() * SHED_SPREAD);
		const give = won
			? Math.min(2.5 + r() * 2, (BLOCK_DRIVE_FPS * (end - meet.t)) / 1000)
			: -1;
		// (Turned off the way the man with the ball comes by - driven over
		// to the side of it he is on, not stood in it as he comes through.)
		// (As little as does it, or as far off him as it gets them.)
		let over = 0;
		if (clear) {
			const most = (BLOCK_CLEAR_FPS * (end - meet.t)) / 1000;
			const nearest = (o: number) => {
				let near = Infinity;
				for (let c = meet.t; c <= end + CLEAR_LATE_MS; c += 50) {
					const u = Math.min(1, (c - meet.t) / Math.max(1, end - meet.t));
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
			end,
			"drive",
			this.ahead,
		);
		b.hold(Math.max(end, until), "stand", this.ahead);
	}

	// Men locked up, from t0 to t1: shoved this way and that together - a
	// little way across the field and back, a half step up and back - each
	// fighting the other.
	// Off the ball into the man across from him (`at`, where that is at t)
	// by `by`: a step or two up into him - or, lined up further off him
	// than he gets to by then out of his stance (see GET_OFF_ACCEL), up to
	// speed and into him as soon as he can (on him FIRE_HELD_MS before
	// `until` at the latest, when he is off him).
	fireInto(m: Actor, at: (t: number) => Pt, by: number, until: number) {
		const near = at(by);
		if (fireOff(m, near) === "engage") {
			m.chase(near, by, m.top * 0.9, "engage");
			return;
		}
		let t = by;
		for (let k = 0; k < 2; k++) {
			t = Math.max(
				by,
				m.t +
					rampedMs(dist(m.at, at(t)), m.top * RUSH_HIT_PACE, 0, GET_OFF_ACCEL),
			);
		}
		t = Math.min(t, Math.max(by, until - FIRE_HELD_MS));
		m.getOff(at(t), t, 0, GET_OFF_ACCEL, "run", GET_OFF_SQUARE_MS);
	}

	// Off from still along `path` (on from where he is), up to speed at `a`
	// - no quicker than `top` - and on at it: at the end of it by t1, or as
	// soon after as he can be; still facing the way he was over the first
	// `square` ms. (The pace he ends at.)
	getOffAlong(
		m: Actor,
		path: Pt[],
		t1: number,
		a: number,
		top: number,
		move: Move,
		square = 0,
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
		const shifts = times.map(() => {
			side = -side;
			return {
				d: (r() - 0.5) * 2 * JOSTLE_D,
				a: side * (JOSTLE_A[0] + r() * (JOSTLE_A[1] - JOSTLE_A[0])),
			};
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
			for (const t of steps) {
				m.split(t);
			}
			for (const l of m.legs) {
				if (l.t0 > t0 && l.t0 < t1) {
					l.from = over(l.from, offAt(l.t0));
				}
				if (l.t1 > t0 && l.t1 < t1) {
					l.to = over(l.to, offAt(l.t1));
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
		// (Up to speed off what he was doing - out of his stance, from
		// nothing - no sooner there than that gets him there.)
		const cInto = SET.has(c.last.move) ? undefined : paceOn(c.last, cAt, b.at);
		const cV0 = cInto ?? 0;
		const cA = cInto === undefined ? RELEASE_ACCEL : RAMP_ACCEL;
		let meet: Pt;
		let tm: number;
		if (bv <= 0) {
			meet = b.at;
			tm = Math.max(b.t, cGo + rampedMs(dist(cAt, meet), vc, cV0, cA));
		} else {
			b.hold(go);
			const bAt = b.at;
			const u = vc / (vc + bv);
			meet = {
				x: cAt.x + (bAt.x - cAt.x) * u,
				y: cAt.y + (bAt.y - cAt.y) * u,
			};
			tm = Math.max(
				Math.max(b.t, cGo) + 150 + (dist(bAt, cAt) / (vc + bv)) * 1000,
				cGo + rampedMs(dist(cAt, meet), vc, cV0, cA),
			);
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
		c.go(
			cHit,
			tm,
			gait(Math.min(vc, paceFor(dist(cAt, cHit), (tm - c.t) / 1000, cV0, cA))),
			ang + Math.PI,
		);
		c.breakDown(LOCK_KEEP);
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
		const to = {
			x: pt.x + Math.cos(ang) * ring,
			y: pt.y + Math.sin(ang) * ring,
		};
		// (Well round from the way he was going: planted first - see
		// plantFor - not turned at full speed.)
		m.plantFor(to);
		m.chase(to, Math.max(m.t + 400, this.whistle + 300), m.top * 0.7);
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
		const to = this.w({ d: land.d + 3.5, a: land.a });
		const by = catchAt - 700;
		const fps = (dist(m.at, to) / Math.max(1, by - m.t)) * 1000;
		if (fps < DRIFT_FPS) {
			// (Not far to go: under it with his eyes on it all the way, facing
			// the kick - back, across, or in toward it.)
			const rel = this.toward(m.at, to) - this.behind;
			const c = Math.cos(rel);
			m.press(
				to,
				by,
				m.top * 0.85,
				Math.abs(c) < DRIFT_ACROSS ? "slide" : c < 0 ? "back" : gait(fps),
				this.behind,
			);
		} else {
			m.press(to, by, m.top * 0.85);
		}
		m.hold(catchAt - CATCH_REACH_MS, "stand", this.behind);
		m.catchOn(catchAt, this.behind, this.w({ d: land.d + 0.8, a: land.a }));
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
			m.press(
				{ x: at.x + Math.cos(yaw) * 1.5, y: at.y + Math.sin(yaw) * 1.5 },
				t - 250,
				m.top * 0.9,
			);
			m.hold(t - CATCH_REACH_MS, "stand", yaw);
			m.catchOn(t, yaw, at);
		};
		let takeT: number;
		if (caught) {
			// Caught coming down, the hands team's man facing the kick, the
			// kicking team's going with it.
			const near = this.w({ d: at.d + (ahead ? -1.5 : 1.5), a: at.a });
			rec.hold(goOf(rec));
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
			rec.act("dive", takeT, yaw, lands);
			rec.hold(takeT + 1400, "prone");
			rec.act("getUp", takeT + 1400 + GET_UP_MS);
			if (muff) {
				// And after it again, too late.
				const yaw = this.toward(muff.at, restPt);
				const by = {
					x: restPt.x - Math.cos(yaw) * 1.6,
					y: restPt.y - Math.sin(yaw) * 1.6,
				};
				muff.press(by, takeT - 200, muff.top * 0.8);
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
			// (No further out than he is: not stepping off it to come back.)
			const ring = Math.max(
				HIT_LEAST_FT,
				Math.min(2.4 + n * 0.5 + r() * 1.5, dist(m.at, pt)),
			);
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
				m.act("dive", t1, inward, {
					x: pt.x + Math.cos(ang) * 1.2,
					y: pt.y + Math.sin(ang) * 1.2,
				});
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

	// The kicker, his kick away, on after it at a jog.
	kickerAfter(k: Actor, at: OPt, until: number) {
		if (this.done.has(k.pid)) {
			return;
		}
		// (At a jog at most, from wherever he has got to.)
		const to = this.w({ d: at.d * 0.45 + this.r() * 8, a: at.a * 0.4 });
		k.go(
			to,
			Math.max(k.t + 600, k.t + (dist(k.at, to) / KICKER_JOG) * 1000, until),
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
			this.fireInto(m, at, snap + 450, free);
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
			// (Broken down there, slowing into it over his last strides.)
			g.hold(g.t + GUNNER_SET_MS, "backerStance", this.toward(g.at, landPt));
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
			m.run(to, m.top * 0.85, "sprint");
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
			// (Easing up short of the goal line once it is going in, scattered
			// - not all of them stood along it.)
			lanes.forEach((m, k) => {
				const end = laneEnd(k);
				const off = Math.abs(k - (lanes.length - 1) / 2);
				m.chase(
					this.w({
						d: Math.min(
							end.d,
							goalPull - TB_SHORT - r() * TB_SPREAD - off * 1.5,
						),
						a: end.a,
					}),
					this.whistle + 900,
					m.top * 0.85,
				);
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
				this.fireInto(m, at, snap + 400, held);
				m.go(at(held), held, "engage", this.behind);
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
				// (Laid out for it off his last stride: carried on a little
				// way with it, not stopped dead.)
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
		// Everybody turned to watch it go.
		const posts = this.w({ d: lineD, a: postA });
		for (const m of this.men) {
			if (m !== k) {
				m.hold(Math.max(m.t, kickT + 350) + r() * 200);
				m.hold(crossT + 300, "stand", this.toward(m.at, posts));
			}
		}
		// Good: this often the holder over to the kicker, a high five - the
		// kicker's fist pumped first, now and then.
		if (
			made &&
			r() < (this.input.kind === "fieldGoal" ? KICK_FIVE : TRY_FIVE)
		) {
			const go = crossT + r() * 150;
			k.cut(go);
			k.hold(go, "stand", this.toward(k.at, posts));
			if (r() < KICK_PUMP) {
				k.act("fistPump", go + 900);
			}
			this.highFive(h, k, Math.max(go + 100, k.t));
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
				this.party = { from: sacker.t, at: sacker.at };
				sacker.act("flex", sacker.t + 1100);
				this.until = sacker.t;
				sacker.hold(sacker.t + 400, "stand");
				this.mob.add(sacker.pid);
				return;
			}
			if (this.input.firstDown) {
				this.pointFirst();
			}
			if (this.input.stopped && r() < STOP_PARTY) {
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
		const at = scorer.at;
		const u = r();
		if (u < BOWL && this.bowl(scorer, c0)) {
			return;
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
			scorer.hold(c0 + 1400, "ballUp");
		} else if (u < 0.6) {
			scorer.act("leap", c0 + 800);
		} else if (u < 0.78) {
			scorer.act("griddy", c0 + GRIDDY_MS);
		} else if (u < 0.9) {
			scorer.hold(c0 + 1500, "pointSky");
		} else {
			scorer.hold(c0 + 1800, "kneelPoint");
		}
		this.until = scorer.t;
		scorer.hold(scorer.t + 2600, "stand");
		this.mob.add(scorer.pid);
		this.mobRound(scorer, c0);
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
			const u = far > 0 ? (far - ease) / far : 1;
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

	// The moment after: `hero`'s teammates nearest running to him from c0,
	// to leap with him, the other side's nearest hands on hips, turned away.
	mobRound(hero: Actor, c0: number, alone = false) {
		const r = this.r;
		const at = hero.at;
		this.mobAt = at;
		this.party = { from: c0, at };
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
			.slice(0, alone ? 0 : MOB);
		const rings = spreadRound(
			mates.map((m, k) => {
				const from = m.posAt(start);
				return Math.atan2(from.y - at.y, from.x - at.x) + (k - 1.5) * 0.25;
			}),
			MOB_GAP,
		);
		mates.forEach((m, k) => {
			const near = dist(m.posAt(c0), at) < MOB_FROM;
			m.cut(start);
			const ang = rings[k]!;
			const to = {
				x: at.x + Math.cos(ang) * MOB_RING,
				y: at.y + Math.sin(ang) * MOB_RING,
			};
			// (Pulled up as he gets there, hard - not leapt in off a sprint.)
			const v = m.top * (0.7 + r() * 0.15);
			const far = dist(m.at, to);
			const ease = Math.min(far * 0.5, (v * v) / (2 * STOP_ACCEL * STOP_HARD));
			const u = far > 0 ? (far - ease) / far : 1;
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
		if (!turned || !hero || hero.team === offense || r() > TAKEAWAY) {
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
			hero.hold(c0 + 1400, "ballUp");
		} else {
			hero.act("leap", c0 + 800);
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
		);
		this.mob.add(hero.pid);
		const at = hero.at;
		this.mobAt = at;
		this.party = { from: c0, at };
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
			.slice(0, r() < 0.5 ? 2 : 1);
		let end = hero.t;
		mates.forEach((m, j) => {
			const from = m.posAt(start);
			m.cut(start);
			m.hold(start);
			const toHim = dist(from, at) > 0.5 ? this.toward(at, from) : r() * 6.28;
			const bump = j === 0 && r() < STOP_BUMP;
			const five = j === 0 && !bump && r() < STOP_FIVE;
			const way = bump || five ? sideOn(toHim) : toHim;
			const off = bump ? STOP_BUMP_APART : five ? FIVE.apart : STOP_RING;
			const spot = {
				x: at.x + Math.cos(way) * off,
				y: at.y + Math.sin(way) * off,
			};
			m.runUp(spot, Math.min(m.top * 0.6, STOP_FPS));
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
		if (!m || m.team !== this.input.offense || r() > POINT_FIRST) {
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
			lay.t1 = Math.max(lay.t0 + 1, Math.min(lay.t1, lay.t0 + UP_QUICK));
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
			const idle = carrier && m !== carrier ? idleFrom(m) : t;
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
				// (On the move still, slowing up as he sees it - not stopped
				// dead for it and off again.)
				if (GAITS.has(m.last.move) && legPace(m.last) >= STOP_LEAST) {
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
						if (!ends.some((e) => dist(e, end) < FOLLOW_APART)) {
							break;
						}
					}
					ends.push(end);
					const fps = Math.min(most, (dist(m.at, to) / (t - go)) * 1000);
					m.hold(go);
					m.chase(to, t, fps, gait(fps));
				}
			}
			if (m.t > t + 400) {
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
				if (GAITS.has(m.last.move) && ball) {
					const v = legPace(m.last);
					const near = dist(m.at, ball);
					const stop = (v * v) / (2 * STOP_ACCEL * STOP_HARD);
					if (stop > near - 2.5) {
						m.easeOff(Math.min(EASE_OFF_FT, stop), ball);
					} else {
						m.easeOff(Math.min(EASE_OFF_FT, Math.max(near * 0.5, stop)));
					}
				}
				m.hold(m.t + 600, "stand");
				continue;
			}
			const l = m.last;
			const moving = l.from.x !== l.to.x || l.from.y !== l.to.y;
			if (moving) {
				m.easeOff(EASE_OFF_FT);
			}
			m.hold(Math.max(m.t, t) + 600, "stand");
		}
	}

	// Stood with nothing to do while the play goes on: turned, now and then,
	// to keep the ball in front of him - not a statue looking off at nothing.
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
		const downs = this.input.stopped === "fourth";
		const off =
			KICK_KINDS.has(kind) ||
			scored ||
			downs ||
			(holder !== undefined && holder.team !== offense);
		// (The side scored on, that had the ball taken off it or gave it up on
		// downs: slow off the field, heads down.)
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
				// (A step or so over taken, not hopped.)
				const need = Math.max(HELP_STEP_MS, (dist(from, by) / HELP_FPS) * 1000);
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
		// The side it went for, a hand slapped or two on the way: a score, the
		// ball taken off the other side, a long way gained, a sack.
		const gain = num(
			events
				.slice(i0, i1)
				.find((e) => e?.type === "run" || e?.type === "passComplete")?.yds,
			0,
		);
		const cheered =
			won ??
			(gain >= FIVE_GAIN
				? offense
				: this.find("sack") >= 0
					? ((1 - offense) as Side)
					: undefined);
		if (cheered !== undefined) {
			this.fives(cheered, free, holder);
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
			// (Off toward his own bench, back up the field if it is past it.)
			const going = off
				? {
						x: clamp(m.at.x, TEAM_AREA[0], TEAM_AREA[1]) + (r() - 0.5) * 24,
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
			if (beaten && r() < (m.role === "QB" ? RUE_QB : RUE_HELMET)) {
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
				const way = this.mobAt ? skirt(m.at, to, this.mobAt) : [to];
				way.forEach((p, k) => {
					if (k === way.length - 1 && fps >= 7) {
						m.runUp(p, fps);
					} else {
						m.run(p, fps);
					}
				});
				if (beaten && r() < RUE_HIPS) {
					m.arms("onHips", from, m.t);
				}
			}
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
			let k = -1;
			for (let j = 0; j < left.length; j++) {
				const d = dist(pa, left[j]!.posAt(free.get(left[j]!.pid)!));
				if (
					d < FIVE_NEAR &&
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
		// FIVE_STEP_MS.)
		const over = (m: Actor, to: Pt, yaw: number) => {
			if ((dist(m.at, to) / FIVE_FPS) * 1000 < FIVE_STEP_MS) {
				m.go(to, m.t + FIVE_STEP_MS, undefined, yaw);
			} else {
				m.run(to, FIVE_FPS, undefined, yaw);
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

// The leg he is in at t (or the last).
const legAt = (m: Actor, t: number): Leg =>
	m.legs.findLast((l) => l.t0 <= t) ?? m.legs[0]!;

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

// After a score: the scorer's moment, then his teammates' around him -
// at the most this many of them, from no further than this away.
const MOB = 4;
const MOB_FROM = 75;
// How far off the scorer each of them pulls up (feet) - no two nearer each
// other round him than this (radians) - and a ball spiked:
// down into the turf this long after he starts, and up off it.
const MOB_RING = 3.2;
const MOB_GAP = 0.85;
// (Down this near him: in on it once up.)
const MOB_UP_NEAR = 12;
const SPIKE_MS = 720;
// The griddy this long.
const GRIDDY_MS = 2400;
// Bowling, this often (with three teammates no further than this off): the
// pins this far down the end zone and across (feet), no nearer the end line
// than this, set no sooner than this after he is in; the roll this long, let
// go this far through it, along the turf at this pace; the pins behind the
// head pin down this long after him.
const BOWL = 0.2;
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
const WHISTLE_SLIVER_MS = 150;
// (Not with less time than this, ms, to go after it.)
const FOLLOW_LEAST_MS = 400;
const FOLLOW_KEEP = [9, 8] as const;
const FOLLOW_CLOSE = [5, 6] as const;
const FOLLOW_FPS = { line: 8, qb: 6.5, other: 12, after: 3 };
// No two of them nearer than this (feet) when it is over: one in from a
// little to the side (radians), if so.
const FOLLOW_APART = 3.5;
const FOLLOW_TURNS = [0, 0.45, -0.45, 0.9, -0.9];
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
const TOSS_MS = 380;
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
// The side that lost it off the field: this often a man's hands to his
// helmet first (for this long, give or take - the quarterback's more often),
// this often his hands on his hips on the way; and round a crowd of the
// other side's, no nearer it than this.
const RUE_HELMET = 0.2;
const RUE_QB = 0.55;
const RUE_HELMET_MS = 1500;
const RUE_HIPS = 0.45;
const SKIRT = 11;
// ...and the side that got it: this often a fist pumped, or a clap, first.
const GLAD = 0.3;
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
const STOP_BUMP = 0.55;
const STOP_BUMP_APART = 2;
const STOP_BUMP_MS = 900;
const STOP_FIVE = 0.5;
// Getting under a kick: no faster than this (feet a second), he goes with
// his eyes on it - across (shuffling, the way he goes no nearer straight at
// or away from the kick than this, as a cosine), or backpedaling.
const DRIFT_FPS = 9;
const DRIFT_ACROSS = 0.6;
// A fair catch: his arm waved from this long before the ball comes down
// to this long before (ms).
const FAIR_FROM = 2000;
const FAIR_TO = 700;
// A field goal good, a try good: this often the holder and the kicker slap
// hands after it - the kicker this often with his fist pumped first.
const KICK_FIVE = 0.75;
const TRY_FIVE = 0.4;
const KICK_PUMP = 0.35;
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
