import type { Official } from "./crew.ts";
import type { BallSeg, Leg, Part, PlayKind, RawEvent } from "./director.ts";
import { dist, type Dir, type Pt, type Side } from "./geometry.ts";
import { moveKind, type Move } from "./poses.ts";

// THE FLAGS: each foul the sim calls on a play - what it was and on whom,
// when in the play it happened and where, the officials who would have
// seen it - and the referee's signal for it after the play. The sim says
// only that there was a flag, and after the play what it was for: when and
// where are the 3D's, by what kind of foul it is.

export type Foul = {
	// The flag line, and the penalty line that goes with it (or -1).
	flagI: number;
	penI: number;
	name: string;
	// The side that fouled, and the man, if the field has him.
	side: Side;
	pid: number | undefined;
	accepted: boolean;
	signal: Move;
	t: number;
	at: Pt;
	// Who throws for it: whichever of these is nearest.
	by: readonly Official[];
};

export type FoulInput = {
	kind: PlayKind;
	offense: Side;
	dir: Dir;
	losX: number;
	ballY: number;
	set: number;
	snap: number;
	whistle: number;
	events: RawEvent[];
	i0: number;
	i1: number;
	parts: Part[];
	ball: BallSeg[];
	// The man a penalty line names.
	pidOf: (side: Side, name: string) => number | undefined;
	// When the play's own action has a flag (one before the snap).
	at: ReadonlyMap<number, number>;
	r: () => number;
};

// When in a play each kind of foul happens: before the snap; at it (lined
// up wrong, early off the ball); in the blocking; on a man running a route;
// on the passer; on the tackle; after the whistle; in the blocking on a
// return.
type When =
	| "pre"
	| "snap"
	| "line"
	| "cover"
	| "passer"
	| "tackle"
	| "after"
	| "return";

const WHEN: Record<string, When> = {
	"False start": "pre",
	"Neutral zone infraction": "pre",
	Encroachment: "pre",
	"Delay of game": "pre",
	"Illegal shift": "pre",
	"Illegal motion": "pre",
	"Illegal Substitution": "pre",
	"Unsportsmanlike conduct": "pre",
	Offsides: "snap",
	"Illegal formation": "snap",
	"Too many men on the field": "snap",
	Holding: "line",
	"Illegal use of hands": "line",
	"Chop block": "line",
	Clipping: "line",
	"Illegal low block": "line",
	"Illegal blindside block": "line",
	"Illegal crackback block": "line",
	Tripping: "line",
	Leverage: "line",
	"Unnecessary roughness": "line",
	"Illegal block in the back": "return",
	"Pass interference": "cover",
	"Illegal contact": "cover",
	"Illegal touching": "cover",
	"Ineligible receiver downfield": "cover",
	"Ineligible man downfield": "cover",
	"Roughing the passer": "passer",
	"Face mask": "tackle",
	"Horse collar tackle": "tackle",
	"Lowering the head to initiate contact": "tackle",
	Taunting: "after",
};
const ORDER: When[] = [
	"pre",
	"snap",
	"line",
	"return",
	"cover",
	"passer",
	"tackle",
	"after",
];

// Who would see it: the wings and the umpire on the line, the referee on
// the passer, the deep men on the routes - anybody, a hit on a ball carrier
// or what goes on after the play.
const ALL: readonly Official[] = ["R", "U", "DJ", "LJ", "SJ", "FJ", "BJ"];
const BY: Record<When, readonly Official[]> = {
	pre: ["LJ", "DJ", "U"],
	snap: ["LJ", "DJ", "U", "R"],
	line: ["U", "R", "LJ", "DJ"],
	return: ALL,
	cover: ["SJ", "FJ", "BJ", "LJ", "DJ"],
	passer: ["R"],
	tackle: ALL,
	after: ALL,
};

// The referee's signal for each.
const SIGNAL: Record<string, Move> = {
	Holding: "sigHold",
	"False start": "sigRoll",
	"Illegal formation": "sigRoll",
	"Illegal shift": "sigRoll",
	"Illegal motion": "sigRoll",
	"Neutral zone infraction": "sigHips",
	Offsides: "sigHips",
	Encroachment: "sigHips",
	"Pass interference": "sigPush",
	"Illegal block in the back": "sigPush",
	"Illegal contact": "sigContact",
	"Illegal use of hands": "sigContact",
	"Illegal touching": "sigContact",
	"Unnecessary roughness": "sigChop",
	"Roughing the passer": "sigChop",
	"Horse collar tackle": "sigChop",
	"Lowering the head to initiate contact": "sigChop",
	Clipping: "sigChop",
	"Chop block": "sigChop",
	"Illegal crackback block": "sigChop",
	"Illegal low block": "sigChop",
	"Illegal blindside block": "sigChop",
	Tripping: "sigChop",
	Leverage: "sigChop",
	"Face mask": "sigMask",
	"Unsportsmanlike conduct": "sigT",
	Taunting: "sigT",
	"Delay of game": "sigFold",
	"Too many men on the field": "sigHead",
	"Illegal Substitution": "sigHead",
	"Ineligible receiver downfield": "sigHead",
	"Ineligible man downfield": "sigHead",
};

// The play itself, from the snap - and with none (nothing in the events
// but the flag), the foul came before it.
const SNAPPED = new Set([
	"dropback",
	"handoff",
	"run",
	"kneel",
	"punt",
	"kickoff",
	"onsideKickRecovery",
	"fieldGoal",
	"extraPoint",
]);
const RETURNS = new Set<PlayKind>(["kickoff", "punt", "onside"]);

const posOf = (part: Part, t: number): Pt => {
	const legs = part.legs;
	for (let k = legs.length - 1; k >= 0; k--) {
		const l = legs[k]!;
		if (t >= l.t0) {
			const u =
				l.t1 > l.t0 ? Math.min(1, Math.max(0, (t - l.t0) / (l.t1 - l.t0))) : 1;
			return {
				x: l.from.x + (l.to.x - l.from.x) * u,
				y: l.from.y + (l.to.y - l.from.y) * u,
			};
		}
	}
	return legs[0]!.from;
};

export const foulsOf = (input: FoulInput): Foul[] => {
	const { events, i0, i1, parts, ball, snap, whistle, r } = input;
	const flags: number[] = [];
	const pens: number[] = [];
	let snapped = false;
	for (let i = i0; i < i1; i++) {
		const type = events[i]?.type;
		if (type === "flag") {
			flags.push(i);
		} else if (type === "penalty") {
			pens.push(i);
		} else if (type !== undefined && SNAPPED.has(type)) {
			snapped = true;
		}
	}
	if (flags.length === 0) {
		return [];
	}
	// What it was for, each - in the order they would have happened, to go
	// with the flags as they came.
	const whenOf = (name: string): When =>
		!snapped
			? "pre"
			: RETURNS.has(input.kind) && (WHEN[name] ?? "line") === "line"
				? "return"
				: (WHEN[name] ?? "line");
	const calls = pens
		.map((i) => {
			const e = events[i]!;
			const name = typeof e.penaltyName === "string" ? e.penaltyName : "";
			return { i, e, name, when: whenOf(name) };
		})
		.sort((a, b) => ORDER.indexOf(a.when) - ORDER.indexOf(b.when));

	// The play's moments: the throw and the catch, the kick fielded.
	const pass = ball.find(
		(s): s is Extract<BallSeg, { kind: "fly" }> =>
			s.kind === "fly" && s.spiral === true && s.t0 >= snap,
	);
	const fielded = ball.find(
		(s): s is Extract<BallSeg, { kind: "held" }> =>
			s.kind === "held" &&
			s.t0 > snap &&
			parts.find((p) => p.pid === s.pid)?.team !== input.offense,
	);
	const carrierAt = (t: number): Pt | undefined => {
		let s: BallSeg | undefined;
		for (const x of ball) {
			if (x.t0 <= t) {
				s = x;
			}
		}
		if (!s) {
			return undefined;
		}
		if (s.kind === "held") {
			const pid = s.pid;
			const p = parts.find((x) => x.pid === pid);
			return p ? posOf(p, t) : undefined;
		}
		return s.kind === "spot" ? s.at : s.to;
	};
	const line = (): Pt => ({
		x: input.losX,
		y: input.ballY + (r() - 0.5) * 20,
	});

	return flags.map((flagI, k): Foul => {
		const call = calls[k];
		const name = call?.name ?? "";
		const when = call?.when ?? (snapped ? "line" : "pre");
		const rawT = call?.e.t === 1 ? 1 : 0;
		const side = (1 - rawT) as Side;
		// (The man: off the stat line that goes with the penalty, when it
		// was taken - or by his name.)
		let pid: number | undefined;
		for (let j = (call?.i ?? i1) + 1; j < i1; j++) {
			const e = events[j];
			if (e?.type === "penalty") {
				break;
			}
			if (e?.type === "stat" && e.s === "pen" && typeof e.pid === "number") {
				pid = e.pid;
				break;
			}
		}
		const named = call?.e.names?.[0];
		if (pid === undefined && typeof named === "string") {
			pid = input.pidOf(side, named);
		}
		const man =
			pid === undefined ? undefined : parts.find((p) => p.pid === pid);
		let t: number;
		switch (when) {
			case "pre":
				t = input.at.get(flagI) ?? input.set + 500;
				break;
			case "snap":
				t = snap + 150 + r() * 150;
				break;
			case "line":
				t = snap + 700 + r() * 900;
				break;
			case "return":
				t = fielded
					? fielded.t0 + (whistle - fielded.t0) * (0.3 + 0.4 * r())
					: (snap + whistle) / 2;
				break;
			case "cover":
				t = pass ? pass.t1 - 100 : snap + 1500;
				break;
			case "passer":
				t = pass ? pass.t0 + 300 : snap + 2000;
				break;
			case "tackle":
				t = whistle - 120;
				break;
			case "after":
				t = whistle + 600;
		}
		if (when !== "pre" && when !== "after") {
			t = Math.max(snap + 100, Math.min(t, whistle - 100));
		}
		// Where: the man who did it, then - or wherever that kind happens.
		let at: Pt | undefined = man ? posOf(man, t) : undefined;
		if (!at) {
			at =
				when === "cover"
					? (pass?.to ?? line())
					: when === "pre" || when === "snap" || when === "line"
						? line()
						: (carrierAt(t) ?? line());
		}
		// (A man nowhere near what the foul is: where it was instead.)
		if (when === "cover" && pass && man && dist(at, pass.to) > 30) {
			at = pass.to;
		}
		return {
			flagI,
			penI: call?.i ?? -1,
			name,
			side,
			pid: man ? pid : undefined,
			accepted: call?.e.decision !== "decline",
			signal: SIGNAL[name] ?? "sigSide",
			t,
			at: { x: at.x, y: at.y },
			by: name === "Delay of game" ? ["BJ", "R"] : BY[when],
		};
	});
};

// THE FOUL ITSELF, where the field has the man who did it and he is on his
// feet at it, face to face with a man of the other side: his arms in it
// over whatever his feet are doing - a hold (on a block, on a man running a
// route); hands to the face; a shove in the back, after the whistle, or
// into the passer with the ball gone. (A foul on the tackle, the tackle
// itself: nothing more.)
const FOUL_ARMS: Record<string, Move> = {
	Holding: "holdOn",
	"Pass interference": "holdOn",
	"Illegal contact": "holdOn",
	"Illegal use of hands": "handsFace",
	"Illegal block in the back": "shove",
	"Unnecessary roughness": "shove",
	"Roughing the passer": "shove",
};
// Over this long before the foul's moment and this long after, cut down to
// the stretch he is on his feet and his arms free (no less than this) - the
// man he does it to no further off than this, and in front of him.
const FOUL_BEFORE = 450;
const FOUL_AFTER = 450;
const FOUL_LEAST = 450;
const FOUL_NEAR = 5;
const FOUL_FACING = Math.PI / 3;

// The legs his arms can do it over: running, blocking, stood.
const freeArms = (l: Leg) =>
	!l.arms && !l.ball && (moveKind(l.move) === "cycle" || l.move === "stand");

// The leg he is in at t made two, there (an act through it, a turn, his
// arms in one of their own, split with it).
const splitLeg = (legs: Leg[], t: number) => {
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
	legs.splice(k + 1, 0, {
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
	});
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
};

// Which way he faces in a leg: as it says, or the way he goes.
const facingOf = (l: Leg): number | undefined =>
	l.yaw ??
	(dist(l.from, l.to) > 0.5
		? Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x)
		: undefined);

// His arms in `move` over t0..t1, whatever the rest of him is doing.
const armsOver = (legs: Leg[], move: Move, t0: number, t1: number) => {
	splitLeg(legs, t0);
	splitLeg(legs, t1);
	for (const l of legs) {
		if (l.t0 >= t0 && l.t1 <= t1 && l.t1 > l.t0) {
			l.arms = move;
			l.armsSpan = [(l.t0 - t0) / (t1 - t0), (l.t1 - t0) / (t1 - t0)];
		}
	}
};

// After the whistle, this often, the man flagged for a foul in the play
// (not one before the snap) with his arms out to the officials, palms up -
// what was that? - from this long after it (and up to this much later),
// this long, over whatever he is doing then if his arms are free all of
// it. (His own count, from who he is and the flag - not the play's dice.)
const PROTEST = 0.55;
const PROTEST_AFTER = 300;
const PROTEST_LATE = 700;
const PROTEST_MS = 1300;
const countOf = (pid: number, flagI: number, salt: number) => {
	let x = (pid * 2654435761 + flagI * 40503 + salt * 97) >>> 0;
	x ^= x >>> 15;
	x = Math.imul(x, 2246822519) >>> 0;
	x ^= x >>> 13;
	return (x >>> 0) / 4294967296;
};
const protest = (f: Foul, man: Part, whistle: number) => {
	const when = WHEN[f.name];
	if (
		when === "pre" ||
		when === "snap" ||
		countOf(man.pid, f.flagI, 1) >= PROTEST
	) {
		return;
	}
	const t0 =
		whistle + PROTEST_AFTER + countOf(man.pid, f.flagI, 2) * PROTEST_LATE;
	const t1 = t0 + PROTEST_MS;
	const legs = man.legs;
	if (
		(legs.at(-1)?.t1 ?? 0) < t1 ||
		legs.some((l) => l.t1 > t0 && l.t0 < t1 && !freeArms(l))
	) {
		return;
	}
	armsOver(legs, "protest", t0, t1);
};

export const actFouls = (fouls: Foul[], parts: Part[], whistle: number) => {
	for (const f of fouls) {
		const move = FOUL_ARMS[f.name];
		const man = parts.find((p) => p.pid === f.pid);
		if (man) {
			protest(f, man, whistle);
		}
		if (!move || !man) {
			continue;
		}
		const legs = man.legs;
		const k = legs.findIndex((l) => l.t0 <= f.t && f.t < l.t1);
		const at = legs[k];
		if (!at || !freeArms(at)) {
			continue;
		}
		// (Who he did it to: the nearest of the other side, in front of him -
		// the passer, roughed.)
		const me = posOf(man, f.t);
		const way = facingOf(at);
		const foe = parts
			.filter(
				(p) =>
					p.team !== man.team &&
					(f.name !== "Roughing the passer" || p.role === "QB"),
			)
			.map((p) => ({ p, d: dist(posOf(p, f.t), me) }))
			.sort((a, b) => a.d - b.d)[0];
		if (!foe || foe.d > FOUL_NEAR || way === undefined) {
			continue;
		}
		const to = posOf(foe.p, f.t);
		let off = Math.atan2(to.y - me.y, to.x - me.x) - way;
		off = Math.atan2(Math.sin(off), Math.cos(off));
		if (Math.abs(off) > FOUL_FACING) {
			continue;
		}
		// (Over the legs either side of it his arms are free in.)
		let a = k;
		while (a > 0 && legs[a - 1]!.t1 === legs[a]!.t0 && freeArms(legs[a - 1]!)) {
			a--;
		}
		let b = k;
		while (
			b + 1 < legs.length &&
			legs[b + 1]!.t0 === legs[b]!.t1 &&
			freeArms(legs[b + 1]!)
		) {
			b++;
		}
		const t0 = Math.max(f.t - FOUL_BEFORE, legs[a]!.t0);
		const t1 = Math.min(f.t + FOUL_AFTER, legs[b]!.t1);
		if (t1 - t0 < FOUL_LEAST) {
			continue;
		}
		armsOver(legs, move, t0, t1);
	}
};
