import { makeCourtRng } from "../courtRng.ts";
import {
	HEAVE_MAX_SECONDS,
	MOTION_HANDLER_SLOT,
	MOTION_OFFENSE_SPOTS,
	TRANSITION_OFFENSE_SPOTS,
	possessionBeats,
} from "../courtSpots.ts";
import {
	attackDir,
	benchX,
	clampPt,
	COURT_H,
	COURT_W,
	dist,
	FT_BACK,
	FT_DEFENSE,
	FT_LINE_DEPTH,
	FT_OFFENSE,
	guardSpot,
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
import type { AnimName } from "./poses.ts";
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
	| { kind: "hold"; t0: number; pid: number; style: "hold" | "dribble" }
	| {
			kind: "fly";
			t0: number;
			t1: number;
			from: BallEnd;
			to: BallEnd;
			peak: number;
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
export type FxKind = "swish" | "clank" | "dunk" | "block" | "whistle" | "cheer";
export type Fx = { kind: FxKind; t: number; rim?: Side; team?: Side };
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

// Speeds in feet per second at 1x. A little quicker than life - this is a
// highlight pace, not a broadcast - and the animation cycles keep up, since
// they advance by distance covered.
const RUN = 28;
const SPRINT = 32;
const DRIBBLE = 27;
const JOG = 18;
const PASS_FTPS = 45;

const passMs = (d: number) =>
	Math.min(900, Math.max(260, 180 + (d * 1000) / PASS_FTPS));

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
	private readonly lineup: [number[], number[]] = [[], []];
	readonly ball: BallSeg[] = [];
	readonly fx: Fx[] = [];
	readonly beats: Beat[] = [];
	readonly poss: [number, Side][] = [];

	// The ball at the end of everything scheduled so far.
	private holder: number | undefined;
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
		for (const p of players) {
			this.team.set(p.pid, p.team);
			this.rank.set(p.pid, POS_RANK[p.pos ?? ""] ?? 4);
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
				: { ...TABLE };
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
	// scheduled for after it (a bounce that was still rolling, say).
	private pushBall(seg: BallSeg) {
		while (this.ball.length > 1 && this.ball.at(-1)!.t0 > seg.t0) {
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

	private hold(pid: number, t: number, style: "hold" | "dribble" = "hold") {
		this.pushBall({ kind: "hold", t0: t, pid, style });
		this.holder = pid;
	}

	private fly(
		t0: number,
		t1: number,
		from: BallEnd,
		to: BallEnd,
		peak: number,
	) {
		this.pushBall({ kind: "fly", t0, t1, from, to, peak });
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

	private effect(kind: FxKind, t: number, o: { rim?: Side; team?: Side } = {}) {
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

	// A pass. Returns when the receiver has it.
	private passTo(from: number, to: number, t: number, peak = 5.5): number {
		const start = Math.max(t, this.free.get(from) ?? 0);
		const d = dist(this.posOf(from), this.posOf(to));
		const toward = this.posOf(to).x >= this.posOf(from).x ? 1 : -1;
		this.act(from, "pass", start, start + 300, {
			face: toward,
			look: { ...this.posOf(to) },
		});
		const release = start + 120;
		const arrive = Math.max(release + passMs(d), (this.free.get(to) ?? 0) + 40);
		this.fly(release, arrive, { pid: from }, { pid: to }, peak);
		this.act(to, "catch", arrive - 90, arrive + 110, {
			face: -toward as 1 | -1,
			look: { ...this.posOf(from) },
		});
		this.hold(to, arrive, "hold");
		this.free.set(from, Math.max(this.free.get(from) ?? 0, start + 300));
		this.free.set(to, Math.max(this.free.get(to) ?? 0, arrive + 110));
		return arrive + 110;
	}

	private inBackcourt(team: Side, p: Pt): boolean {
		return team === 1 ? p.x < COURT_W / 2 : p.x > COURT_W / 2;
	}

	// The man guarding the ball picks him up where he catches it (and then
	// slides back with him), instead of waiting at the far end of the floor.
	private pickUp(team: Side, receiver: number, at: Pt, t: number): number[] {
		const j = this.slots(team).indexOf(receiver);
		const guard = this.slots(other(team))[j];
		if (guard === undefined) {
			return [];
		}
		const toward = guardSpot(team, at, 0.12);
		this.go(
			guard,
			clampPt({ x: toward.x + attackDir(team) * 3, y: toward.y }),
			t,
			RUN,
			"run",
			-attackDir(team) as 1 | -1,
		);
		return [guard];
	}

	// After a basket: a big fetches the ball from under the net, steps out
	// behind the baseline, and inbounds it to the point guard.
	private inboundFromBaseline(team: Side, t: number): number {
		const slots = this.slots(team);
		const inbounder = slots[4] ?? slots.at(-1)!;
		const receiver = slots[0] === inbounder ? slots[1]! : slots[0]!;
		const at = this.ballAt;
		const leftEnd = at.x < COURT_W / 2;
		const oob = { x: leftEnd ? -1.7 : COURT_W + 1.7, y: 25 + this.rand(-3, 3) };
		let t1 = this.go(
			inbounder,
			{ x: at.x + (leftEnd ? 0.8 : -0.8), y: at.y },
			t,
			JOG,
			"run",
		);
		this.act(inbounder, "pickup", t1, t1 + 240);
		this.hold(inbounder, t1 + 130, "hold");
		t1 = this.go(inbounder, oob, t1 + 240, 15, "carry");
		this.turn(inbounder, t1, leftEnd ? 1 : -1);
		const recv = {
			x: leftEnd ? 9 : COURT_W - 9,
			y: 25 + (this.rng() < 0.5 ? -9 : 9),
		};
		this.go(receiver, recv, t, RUN, "run");
		const guard = this.pickUp(team, receiver, recv, t + 200);
		// Everyone else jogs up the floor with the play rather than racing it.
		this.settle(team, 0, t + 300, t1 + 2600, [inbounder, receiver, ...guard]);
		return this.passTo(inbounder, receiver, t1 + 120, 4.5);
	}

	// A dead ball inbounded from the sideline (a foul, a timeout, out of bounds).
	private inboundFromSide(team: Side, t: number): number {
		const slots = this.slots(team);
		const receiver = slots[0]!;
		const inbounder = slots[2] ?? slots[1] ?? slots.at(-1)!;
		const at = this.inboundAt ?? {
			x: COURT_W / 2 + attackDir(team) * -6,
			y: -1.4,
		};
		const far = at.y < COURT_H / 2;
		const oob = {
			x: Math.min(COURT_W - 3, Math.max(3, at.x)),
			y: far ? -1.4 : COURT_H + 1.4,
		};
		const t1 = this.go(inbounder, oob, t, RUN, "run");
		this.turn(inbounder, t1, attackDir(team));
		// The official hands it over.
		this.fly(
			Math.max(t, t1 - 380),
			Math.max(t + 380, t1),
			this.ballOrigin(),
			{ pid: inbounder },
			5,
		);
		const recv = clampPt({
			x: oob.x + attackDir(team) * 7,
			y: far ? 9 : COURT_H - 9,
		});
		this.go(receiver, recv, t, RUN, "run");
		const frontcourt = !this.inBackcourt(team, oob);
		const guard = this.pickUp(team, receiver, recv, t + 150);
		this.settle(team, 0, t + 150, t + 2000, [inbounder, receiver, ...guard]);
		const tIn = this.passTo(
			inbounder,
			receiver,
			Math.max(t1, t + 400) + 150,
			4.5,
		);
		if (frontcourt) {
			this.motionTeam = team;
			this.motion = 0;
		}
		return tIn;
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
			t = this.passTo(this.holder, pg, t + 200, 5);
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
			t = this.passTo(handler, pg, t + 150, 6);
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
	// spending the clock the sim says the possession took.
	private develop(team: Side, t: number, gap: number | undefined): number {
		const phase = this.phase;
		const changed = this.offense !== team;
		this.setOffense(t, team);
		let transition = false;

		if (phase === "inboundBase" && !changed) {
			t = this.inboundFromBaseline(team, t);
		} else if (phase === "loose" || phase === "tip" || phase === "set") {
			if (this.holder === undefined || this.teamOf(this.holder) !== team) {
				// Loose ball (or it was ours to take): the nearest man picks it up.
				const b = this.ballPoint();
				const near = this.slots(team).sort(
					(a, c) => dist(this.posOf(a), b) - dist(this.posOf(c), b),
				)[0]!;
				const tArr = this.go(near, { x: b.x, y: b.y }, t, RUN, "run");
				this.act(near, "pickup", tArr, tArr + 300);
				this.hold(near, tArr + 150, "hold");
				t = tArr + 300;
			}
			transition =
				phase === "loose" &&
				gap !== undefined &&
				gap < 7 &&
				this.inBackcourt(team, this.posOf(this.holder ?? 0));
		} else {
			t = this.inboundFromSide(team, t);
		}

		const handlerPos = this.posOf(this.holder ?? this.slots(team)[0]!);
		if (transition) {
			t = this.pushBreak(team, t);
		} else if (this.inBackcourt(team, handlerPos) || this.motionTeam !== team) {
			t = this.bringUp(team, t);
		}

		const beats = possessionBeats(gap, transition);
		// One reversal for an ordinary possession, two for a real grind.
		const swings =
			transition || beats < 2 ? 0 : gap !== undefined && gap >= 18 ? 2 : 1;
		for (let s = 0; s < swings; s++) {
			t = this.swing(team, t);
		}
		this.phase = "set";
		this.inboundAt = undefined;
		return t;
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
		if (zone === "three" && this.rng() < 0.28) {
			return spot(
				team,
				this.rand(2.5, 9),
				this.rng() < 0.5 ? this.rand(1.6, 2.6) : this.rand(47.4, 48.4),
			);
		}
		const [r0, r1, th0, th1] =
			zone === "atRim" || zone === "tipIn" || zone === "putBack"
				? [1.2, 3.6, 30, 150]
				: zone === "lowPost"
					? [4.5, 9.5, 30, 150]
					: zone === "midRange"
						? [11, 19, 20, 160]
						: [24.4, 26.4, 32, 148];
		const r = this.rand(r0, r1);
		const th = (this.rand(th0, th1) * Math.PI) / 180;
		const depth = Math.max(1.5, 5.25 + r * Math.sin(th));
		return spot(
			team,
			Math.min(44, depth),
			Math.min(47, Math.max(3, 25 + r * Math.cos(th))),
		);
	}

	private defenderOf(pid: number): number | undefined {
		const team = this.teamOf(pid);
		const j = this.slots(team).indexOf(pid);
		const def = this.slots(other(team));
		return def[j] ?? def[0];
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

		if (!putback || this.holder !== shooter) {
			if (!putback) {
				t = this.develop(team, t, gap);
			} else {
				this.setOffense(t, team);
			}
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
				t = this.passTo(handler, passer, t, 5);
				handler = passer;
			}
			if (passer !== undefined && this.teamOf(passer) === team) {
				const arrive = this.go(shooter, P, t, RUN, "run");
				const d = dist(this.posOf(handler), P);
				const send = Math.max(t, arrive - passMs(d) - 120);
				const caught = this.passTo(handler, shooter, send, close ? 4 : 5.5);
				t = Math.max(arrive, caught);
			} else {
				if (handler !== shooter) {
					t = this.passTo(handler, shooter, t, 5);
				}
				this.hold(shooter, t, "dribble");
				t = this.go(shooter, P, t, DRIBBLE, "dribble", dir);
				this.hold(shooter, t, "hold");
			}
		}

		// The defense: his man closes out, or the blocker / fouler gets there.
		const guard = this.defenderOf(shooter);
		const P1 = this.posOf(shooter);
		const toRim = { x: rim.x - P1.x, y: rim.y - P1.y };
		const len = Math.hypot(toRim.x, toRim.y) || 1;
		const ahead = (k: number) =>
			clampPt({
				x: P1.x + (toRim.x / len) * k,
				y: P1.y + (toRim.y / len) * k + 0.8,
			});

		const gather = t + 60;
		// A slam when the words say so: "throws it down", "blocked the dunk
		// attempt", "blows the dunk".
		const dunk = close && plan.kind !== "foul" && plan.finish === "dunk";
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
			this.act(shooter, "dunk", gather, gather + dur, {
				face: faceRim,
				look,
				zKeys: hang
					? [
							[0, 0],
							[0.22, 0],
							[0.45, 3.7],
							[0.52, 3.55],
							[0.68, 3.3],
							[0.86, 0],
							[1, 0],
						]
					: [
							[0, 0],
							[0.22, 0],
							[0.45, 3.5],
							[0.62, 2.4],
							[0.82, 0],
							[1, 0],
						],
			});
			const under = clampPt({ x: rim.x - dir * 0.9, y: 25 + 1.3 });
			this.go(shooter, under, gather + 60, SPRINT, "run", faceRim);
			if (plan.kind === "block" && plan.blocker !== undefined) {
				// Met at the rim.
				const contact = gather + dur * 0.42;
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
					0,
				);
				decided = contact;
				arrive = contact;
				target = { x: rim.x - dir * 1.4, y: 25, z: RIM_Z - 0.3 };
			} else if (plan.kind === "miss") {
				// Hammered off the back iron.
				decided = gather + dur * 0.47;
				arrive = decided;
				target = { x: rim.x + dir * (RIM_R + 0.1), y: 25, z: RIM_Z + 0.25 };
				this.fly(decided - 90, decided, { pid: shooter }, target, RIM_Z + 0.9);
			} else {
				decided = gather + dur * 0.5;
				arrive = decided;
				target = rimPt(team, 0.45);
			}
		} else {
			// "Tips it in": a one-handed tap at the top of the jump.
			const tip = zone === "tipIn" && plan.finish === "tip";
			const anim: AnimName = tip ? "block" : close ? "layup" : "shoot";
			const dur = close ? 760 : zone === "lowPost" ? 840 : 920;
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
				this.fly(
					release,
					contact,
					{ pid: shooter },
					{ pid: b, hand: "near" },
					0,
				);
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
				this.fly(
					release,
					release + flight,
					{ pid: shooter },
					target,
					RIM_Z + 2 + d * 0.22,
				);
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
				this.fly(
					release,
					release + flight,
					{ pid: shooter },
					target,
					close ? RIM_Z + 1.3 : RIM_Z + 2.2 + d * 0.26,
				);
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
				if (big !== undefined && big !== shooter && big !== plan.fouler) {
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
			this.act(r, "rebound", jumpStart, jumpStart + 800, {
				face: (rim.x >= catchAt.x ? 1 : -1) as 1 | -1,
				look: { x: rim.x, y: rim.y },
				jump: [0.12, 0.88, blocked ? 1.2 : 2.4],
			});
			this.fly(
				t,
				catchT,
				from,
				{ pid: r },
				blocked ? 6 : hard ? RIM_Z + 6.5 : RIM_Z + 3.5,
			);
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
			const outY = from.y < COURT_H / 2 ? -1.8 : COURT_H + 1.8;
			const to = { x: from.x - dir * this.rand(6, 16), y: outY };
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

	private beat(i: number, type: string, actionStart: number, end: number) {
		const preStart = this.T;
		const a = Math.max(preStart, actionStart);
		const e = Math.max(a + 350, end);
		this.beats.push({ i, type, preStart, actionStart: a, end: e });
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
							}
						: { kind: "miss" };
			const heave =
				zone === "three" &&
				e.desperation === true &&
				typeof e.clock === "number" &&
				e.clock <= HEAVE_MAX_SECONDS
					? e.clock
					: undefined;
			const shot = this.stageShot(d, e.pid, zone, plan, T, gap, heave);
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
					const b = this.ballPoint();
					const tArr = this.go(pid, { x: b.x, y: b.y }, T, RUN, "run");
					this.act(pid, "pickup", tArr, tArr + 300);
					this.hold(pid, tArr + 150, "hold");
					this.beat(i, type, tArr + 150, tArr + 600);
				} else {
					this.beat(i, type, T, T + 650);
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
				const t = this.develop(team, T, gap);
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
				this.effect("whistle", hit);
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
				this.effect("whistle", T);
				const rimTeam = other(this.teamOf(e.pid));
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
				this.effect("whistle", t);
				const outOn: Side | undefined = d;
				const nextTeam = outOn === undefined ? this.offense : other(outOn);
				this.inboundAt = {
					x: this.ballAt.x,
					y: this.ballAt.y < COURT_H / 2 ? -1.4 : COURT_H + 1.4,
				};
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
				this.effect("whistle", T);
				this.deadBall(T);
				const ballX = this.ballAt.x;
				for (const t of [0, 1] as const) {
					const spots = huddleSpots(t);
					this.slots(t).forEach((pid, j) => {
						const at = this.go(
							pid,
							spots[j] ?? spots[0]!,
							T + 150 + j * 70,
							JOG,
							"walk",
						);
						this.lookAt(pid, at, { x: benchX(t), y: 1.6 });
					});
				}
				this.beat(i, type, T, T + (type === "timeout" ? 2400 : 2000));
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
				// Out of the huddles to the middle of the floor.
				this.deadBall(T);
				for (const t of [0, 1] as const) {
					this.slots(t).forEach((pid, j) => {
						const target = {
							x: COURT_W / 2 + (t === 1 ? -1 : 1) * (5 + (j % 3) * 4),
							y: 12 + j * 6.5,
						};
						this.go(pid, target, T + j * 60, JOG, "walk", attackDir(t));
					});
				}
				this.fly(
					T + 200,
					T + 1100,
					this.ballOrigin(),
					{ x: COURT_W / 2, y: -1, z: 3 },
					6,
				);
				this.inboundAt = { x: COURT_W / 2, y: -1.4 };
				this.phase = "inboundSide";
				this.beat(i, type, T + 300, T + 1400);
				break;
			}
			case "injury": {
				const pid = e.pid as number;
				this.deadBall(T);
				this.effect("whistle", T + 200);
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
						this.go(pid, target, T + j * 90, JOG, "walk");
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
							JOG,
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
					6,
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
				this.fly(release, release + flight, { pid }, target, RIM_Z + 8);
				const at = release + flight;
				if (made) {
					let t = at;
					if (finish === "rattle") {
						this.effect("clank", at, { rim: 1 });
						({ t } = this.rollAround(1, at, edge));
						this.fly(t, t + 90, this.ballAt, rimPt(1, 0.2), 0);
						t += 90;
					}
					this.fly(t, t + 140, rimPt(1, 0.2), rimPt(1, -2.3), 0);
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
			this.fly(t, t + 95, at, p, 0);
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
				this.fly(at, at + 70, { pid: shot.pid }, top, RIM_Z + 0.6);
				this.effect("dunk", at + 50, { rim: team });
			}
			const t0 = shot.dunk ? at + 70 : at;
			this.fly(t0, t0 + 140, top, under, 0);
			const settle = clampPt({
				x: rim.x - dir * this.rand(1.5, 4),
				y: 25 + this.rand(-3, 3),
			});
			this.bounce(t0 + 140, t0 + 800, under, settle, 2, 2);
			this.effect("swish", t0 + 20, { rim: team });
			this.effect("cheer", t0 + 60, { team });
			let end = at + 1100;
			if (typeof e.pidFoul === "number") {
				this.effect("whistle", at + 120);
				this.act(e.pidFoul, "reach", at - 150, at + 300);
				end = at + 1400;
			}
			const free = Math.max(at + 450, this.free.get(shot.pid) ?? 0);
			this.act(shot.pid, "celebrate", free + 100, free + 800, {
				face: -dir as 1 | -1,
			});
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
		this.effect("block", at);
		const sp = this.posOf(shot.pid);
		const down = {
			...clampPt({
				x: sp.x - dir * this.rand(2, 4),
				y: sp.y + this.rand(-3, 3),
			}),
			z: 0.3,
		};
		this.fly(at, at + 300, { pid: e.pid, hand: "near" }, down, 0);
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
		const line = spot(team, FT_LINE_DEPTH, 25);
		let ready = T;
		if (first) {
			const tall = (t: Side) =>
				this.slots(t).sort(
					(a, b) => (this.rank.get(b) ?? 4) - (this.rank.get(a) ?? 4) || a - b,
				);
			const def = tall(other(team));
			const off = tall(team).filter((p) => p !== shooter);
			const rimSpot = { x: rimX(team), y: 25 };
			def.forEach((pid, j) => {
				const [dd, ac] = j < 3 ? FT_DEFENSE[j]! : FT_BACK[j - 3]!;
				const at = this.go(
					pid,
					spot(team, dd, ac),
					T + j * 50,
					JOG,
					"walk",
					-dir as 1 | -1,
				);
				this.lookAt(pid, at, j < 3 ? rimSpot : line);
			});
			off.forEach((pid, j) => {
				const [dd, ac] = j < 2 ? FT_OFFENSE[j]! : FT_BACK[j]!;
				const at = this.go(
					pid,
					spot(team, dd, ac),
					T + j * 60,
					JOG,
					"walk",
					dir,
				);
				this.lookAt(pid, at, j < 2 ? rimSpot : line);
			});
			ready = this.go(shooter, line, T, JOG, "walk", dir);
			this.lookAt(shooter, ready, rimSpot);
		}
		if (this.holder !== shooter) {
			this.fly(
				Math.max(T, ready - 420),
				Math.max(T + 420, ready),
				this.ballOrigin(),
				{ pid: shooter },
				6,
			);
			ready = Math.max(T + 420, ready);
		}
		this.turn(shooter, ready, dir);
		this.hold(shooter, ready + 100, "dribble");
		const set = ready + 600;
		this.hold(shooter, set, "hold");
		this.act(shooter, "shoot", set, set + 900, {
			face: dir,
			look: { x: rimX(team), y: 25 },
		});
		const release = set + 900 * 0.55;
		const target = made
			? rimPt(team, 0.35)
			: {
					x: rimX(team) - dir * (RIM_R + 0.05),
					y: 25 + this.rand(-0.4, 0.4),
					z: RIM_Z + 0.12,
				};
		const at = release + 820;
		this.fly(release, at, { pid: shooter }, target, RIM_Z + 3.2);

		const next = this.peek(i, 3).find(
			(x) =>
				x.e.type !== "sub" && x.e.type !== "timeout" && x.e.type !== "timeouts",
		);
		const more =
			next &&
			(next.e.type === "ft" || next.e.type === "missFt") &&
			next.e.pid === shooter;
		if (made) {
			const top = rimPt(team, 0.35);
			this.fly(at, at + 140, top, rimPt(team, -2.3), 0);
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
		let t = this.develop(team, T, gap);
		if (this.holder !== victim) {
			t = this.passTo(this.holder ?? this.slots(team)[0]!, victim, t, 5);
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
				this.effect("whistle", hit + 800);
				this.inboundAt = { x: vp.x, y: outY < 0 ? -1.4 : COURT_H + 1.4 };
				this.offense = other(team);
				this.phase = "inboundSide";
				this.beat(i, e.type, hit, hit + 1000);
			} else {
				this.fly(hit, hit + 160, { pid: victim }, { pid: thief }, 3);
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
			this.fly(t + 120, t + 700, { pid: victim }, { ...to, z: 3 }, 6);
			this.bounce(
				t + 700,
				t + 1300,
				{ ...to, z: 3 },
				{ x: to.x + dir * 3, y: to.y },
				1,
				1,
			);
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
			this.effect("whistle", t + 300);
			this.inboundAt = {
				x: vp.x,
				y: vp.y < COURT_H / 2 ? -1.4 : COURT_H + 1.4,
			};
			this.beat(i, e.type, t, t + 900);
		}
		this.offense = other(team);
		this.phase = "inboundSide";
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
				JOG,
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
					JOG,
					"walk",
					attackDir(t),
				);
				this.lookAt(pid, arrived, c);
				ready = Math.max(ready, arrived);
			});
		}
		const toss = Math.max(T + 600, ready + 200);
		this.rest(T, { x: c.x, y: c.y, z: 5 });
		const apex = { x: c.x, y: c.y, z: 12.3 };
		this.fly(toss, toss + 520, { x: c.x, y: c.y, z: 5 }, apex, 12.5);
		this.act(jumper, "block", toss + 120, toss + 900, {
			face: attackDir(winnerTeam),
			jump: [0.1, 0.9, 2.8],
		});
		this.act(loser, "block", toss + 160, toss + 940, {
			face: attackDir(other(winnerTeam)),
			jump: [0.1, 0.9, 2.5],
		});
		const receiver = this.slots(winnerTeam).find((p) => p !== jumper) ?? jumper;
		this.fly(toss + 520, toss + 1000, apex, { pid: receiver }, 12.4);
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
			const gone = this.go(
				pid,
				{ x: TABLE.x + (j - 1) * 1.5, y: TABLE.y },
				T + j * 80,
				JOG,
				"walk",
			);
			this.show(pid, gone, false);
			if (incoming !== undefined) {
				const tr = this.track(incoming);
				if (tr) {
					const enter = { x: TABLE.x + (j - 1) * 1.5, y: TABLE.y };
					this.pos.set(incoming, enter);
					this.free.set(incoming, T + j * 80);
					this.show(incoming, T + j * 80, true);
					this.go(incoming, at, T + j * 80, JOG, "walk");
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
		this.fx.sort((a, b) => a.t - b.t);
		return {
			tracks: this.tracks,
			ball: this.ball,
			fx: this.fx,
			beats: this.beats,
			poss: this.poss,
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
