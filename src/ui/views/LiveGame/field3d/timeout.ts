import { posOf, type CrewPart } from "./crew.ts";
import type { BallSeg, Leg, Part } from "./director.ts";
import {
	COACH_LINE,
	dist,
	FIELD_W,
	MID_X,
	type Pt,
	type Side,
} from "./geometry.ts";
import { medicId } from "./hurt.ts";
import type { Move } from "./poses.ts";
import { cutAt } from "./postgame.ts";
import { coachId, coordinatorAt, headCoachAt, sidelineOf } from "./sideline.ts";

// A TIMEOUT (the two-minute warning too): the referee's signal, then the
// picture cuts to the side that called it gathered at the edge of the field
// in front of its bench - the head coach out at the sideline going over it
// with the quarterback, the rest stood round them, hands on hips, bent over
// getting a breath - and the other side in a ring of its own out on the
// field by its numbers; the trainers out to each with the water, and a
// drink for the men near them. Then the picture cuts on to the next snap.

// The scene this long (and up to this much longer).
const TIMEOUT_MS = 5200;
const TIMEOUT_MORE = 1200;
// The side that called it: no further along the field from midfield than
// this (in front of the coaches); the head coach a step out onto it, the
// man he talks to this far, and the middle of the two this far; the rest
// round them, in rows this far off that (this many in the first), spread
// through this much of a half circle either way of straight out onto the
// field.
const ALONG = 30;
const COACH_IN = 0.8;
const TALKER_IN = 3.6;
const MIDDLE_IN = 3;
const ROWS = [6.4, 9.6];
const FIRST_ROW = 5;
const SPREAD = 1.25;
// (Across the field: none of them nearer straight out than this - the man
// he talks to this far to one side - the coach to be seen past them.)
const OPEN = 0.5;
const ASIDE = 1.8;
// The other side: in a ring this far across, this far in off its sideline,
// no further along from midfield than this.
const RING = 5.6;
const RING_IN = 40;
const RING_ALONG = 60;
// The officials no nearer either than this past its edge - and no further
// off the field than this.
const CLEAR = 6;
const CREW_OUT = 3;
// How each man stands it: this share with his hands on his hips, this share
// bent over with his hands on his knees (the rest just stood) - and this
// share of them shifting, to another way of standing it, part way through.
const HIPS = 0.3;
const BENT = 0.18;
const SHIFT = 0.4;
// How the side that called it gathers, on its own draw: in rows round the
// head coach (up to the first share), the same with the front row on a
// knee (up to the second) - or the quarterback (the man he talks to) over
// to him on his own, the rest in a ring of their own out on the field, this
// far in off the sideline and this far along it from them.
const GATHER = [0.45, 0.7];
const OVER_IN = 15;
const OVER_ASIDE = 10;
// And the other side: in its ring (up to the first share), the ring with
// this share of them on a knee (up to the second) - or at its own sideline
// round a coordinator of its own, in rows too (the two-minute warning,
// always: a long break, the both of them off to their sidelines).
const THEIRS = [0.55, 0.75];
const KNEEL_RING = 0.6;
// The head coach's points, each this long, a moment between them.
const TALK_MS = 2300;
const TALK_GAP = 400;
// TIME OUT, CALLED BY HIM (the 3D court's: see its crew.ts timeoutsCalled):
// his hands up in a T at the official from this long before the signal
// till this long after it, turned no further from straight out onto the
// field than this - then on out to meet them at this pace (feet a second).
// From across the field, the picture on him from no sooner than this
// before the signal (the T going up) and no later than this, for this
// long at least.
const T_BEFORE = 900;
const T_AFTER = 500;
const T_TURN = 0.3;
const T_WALK_FPS = 4.5;
const T_SHOT_BEFORE = 650;
const T_SHOT_LATEST = 200;
const T_SHOT_LEAST = 1300;
// The trainers with the water, this many to a side, a carrier of bottles
// each: to the side that called it from just off the field, round the ends
// of it; to the other side, this far short of its ring at the cut, in at
// this pace (feet a second) to stand just off it. Of the men this near one
// of them, this share take a drink once he is there - this long.
const WATER = 2;
const WATER_OFF = 2.5;
const WATER_ROUND = 1.45;
const WATER_FROM = 16;
const WATER_FPS = 10;
const WATER_BY = 2.4;
const DRINK_NEAR = 10;
const DRINKS = 0.5;
const DRINK_MS = 1700;
// (No less of what he was at before it than this, ms.)
const DRINK_LEAD_LEAST_MS = 300;

export type Timeout = {
	// When the picture cuts to it, and where it looks.
	cut: number;
	at: Pt;
	t1: number;
	// The head coach out of the team area, and the trainers out with the
	// water.
	coaches: CrewPart[];
	// Where each side is gathered, and how far round it to keep clear.
	clear: { at: Pt; far: number }[];
	// The picture on the head coach calling it, from when (to the cut).
	call?: { from: number; pid: number };
};

const toward = (a: Pt, b: Pt) => Math.atan2(b.y - a.y, b.x - a.x);
const clamp = (x: number, lo: number, hi: number) =>
	Math.min(hi, Math.max(lo, x));

// A man's way of standing it.
const stoodOf = (r: () => number): Move => {
	const u = r();
	return u < HIPS ? "blowing" : u < HIPS + BENT ? "winded" : "stand";
};

export const stageTimeout = (input: {
	parts: Part[];
	ball: BallSeg[];
	// When the picture cuts to it.
	cut: number;
	// Who called it, and where the ball is.
	team: Side;
	spot: Pt;
	t0: number;
	playI: number;
	r: () => number;
	// His head coach calling it: the official's signal - and, if the
	// picture may go to him, no sooner than when.
	call?: { at: number; shot?: number };
	// The two-minute warning (see THEIRS).
	warning?: boolean;
}): Timeout => {
	const { parts, ball, cut, team, spot, r } = input;
	const t1 = cut + TIMEOUT_MS + r() * TIMEOUT_MORE;

	// Each man as the play left him until the cut, then where the scene has
	// him.
	const spots: { p: Part; at: Pt }[] = [];
	const kneel = new Set<Part>();
	const place = (p: Part, at: Pt, yaw: number, move: Move) => {
		spots.push({ p, at });
		cutAt(p.legs, cut);
		const l = p.legs.at(-1)!;
		if (l.t1 < cut) {
			p.legs.push({ t0: l.t1, t1: cut, from: l.to, to: l.to, move: "stand" });
		}
		p.legs.push({ t0: cut, t1, from: at, to: at, move, yaw });
		// (On a knee, all of it - not up and down.)
		if (move === "takeKnee") {
			kneel.add(p);
			return;
		}
		// (Part way through, now and then, another way of standing it.)
		if (r() < SHIFT) {
			const l = p.legs.at(-1)!;
			const then = cut + 1200 + r() * (t1 - cut - 2400);
			const was = stoodOf(r);
			const next = was === move ? "stand" : was;
			if (next !== move) {
				l.t1 = then;
				p.legs.push({ t0: then, t1, from: at, to: at, move: next, yaw });
			}
		}
	};

	// A side gathered at its sideline round a coach: the man he talks to
	// (its quarterback, or its middle linebacker) with him, the rest round
	// the two in rows - the front row on a knee, if `knee` - or, `over`, the
	// rest in a ring of their own out on the field.
	const talkerOf = (men: Part[]) =>
		men.find((p) => p.role === "QB") ??
		men.find((p) => p.role === "MIKE") ??
		men[0];
	const gather = (
		men: Part[],
		side: Side,
		along: number,
		how: "rows" | "knee" | "over",
	) => {
		const line = sidelineOf(side);
		const into = line === 0 ? 1 : -1;
		const edge = line === 0 ? 0 : FIELD_W;
		const x = clamp(along, MID_X - ALONG, MID_X + ALONG) + (r() - 0.5) * 6;
		const middle = { x, y: edge + into * MIDDLE_IN };
		const coachAt = { x, y: edge + into * COACH_IN };
		const talker = talkerOf(men);
		const out = Math.atan2(into, 0);
		// (Over: off to the side of the two, toward the middle of the field
		// - seen past them, not behind them.)
		const aside = x < MID_X ? 1 : -1;
		const ringAt =
			how === "over"
				? { x: x + aside * OVER_ASIDE, y: edge + into * OVER_IN }
				: undefined;
		const rest = men.filter((p) => p !== talker);
		if (talker) {
			const at = {
				x: x + (line === 0 ? ASIDE : 0) + (r() - 0.5) * 1.2,
				y: edge + into * TALKER_IN,
			};
			place(talker, at, toward(at, coachAt), "stand");
		}
		if (ringAt) {
			ringOf(rest, ringAt, 0);
		} else {
			rest.forEach((p, k) => {
				const row = k < FIRST_ROW ? 0 : 1;
				const inRow =
					row === 0
						? Math.min(FIRST_ROW, rest.length)
						: rest.length - FIRST_ROW;
				const j = row === 0 ? k : k - FIRST_ROW;
				// (Across the field from the camera, gathered either side of
				// the two, not in front of them.)
				const u = inRow > 1 ? j / (inRow - 1) : 0.5;
				const wing = j % 2 === 0 ? -1 : 1;
				const v =
					inRow > 2
						? Math.floor(j / 2) / Math.max(1, Math.ceil(inRow / 2) - 1)
						: 0;
				const a =
					out +
					(line === 0
						? wing * (OPEN + (SPREAD - OPEN) * v)
						: (u * 2 - 1) * SPREAD) +
					(r() - 0.5) * 0.12 +
					row * 0.1;
				const far = ROWS[row]! + (r() - 0.5) * 0.8;
				const at = {
					x: middle.x + Math.cos(a) * far,
					y: middle.y + Math.sin(a) * far,
				};
				place(
					p,
					at,
					toward(at, middle) + (r() - 0.5) * 0.4,
					how === "knee" && row === 0 ? "takeKnee" : stoodOf(r),
				);
			});
		}
		return { line, into, edge, x, middle, coachAt, out, ringAt };
	};
	// A side in a ring, `knees` of them on a knee.
	const ringOf = (men: Part[], at: Pt, knees: number) => {
		const turn = r() * Math.PI * 2;
		men.forEach((p, j) => {
			const a = turn + (j / men.length) * Math.PI * 2 + (r() - 0.5) * 0.15;
			const far = RING + (r() - 0.5) * 0.7;
			const to = { x: at.x + Math.cos(a) * far, y: at.y + Math.sin(a) * far };
			place(
				p,
				to,
				toward(to, at) + (r() - 0.5) * 0.3,
				r() < knees ? "takeKnee" : stoodOf(r),
			);
		});
	};

	// The side that called it.
	const mine = parts.filter((p) => p.pid >= 0 && p.team === team);
	const u = r();
	const how = u < GATHER[0]! ? "rows" : u < GATHER[1]! ? "knee" : "over";
	const { line, into, edge, x, middle, coachAt, out, ringAt } = gather(
		mine,
		team,
		spot.x,
		how,
	);

	// The other side: in a ring of its own - or at its sideline too.
	const other = parts.filter((p) => p.pid >= 0 && p.team !== team);
	const them = (1 - team) as Side;
	const v = r();
	const there = input.warning
		? "sideline"
		: v < THEIRS[0]!
			? "ring"
			: v < THEIRS[1]!
				? "knee"
				: "sideline";
	const theirs = line === 0 ? FIELD_W : 0;
	const ring = {
		x: clamp(spot.x, MID_X - RING_ALONG, MID_X + RING_ALONG),
		y: theirs + (line === 0 ? -RING_IN : RING_IN),
	};
	const atLine =
		there === "sideline"
			? gather(other, them, spot.x + (r() - 0.5) * 30, "rows")
			: undefined;
	if (!atLine) {
		ringOf(other, ring, there === "knee" ? KNEEL_RING : 0);
	}

	// The ball, if anybody had it, spotted - nothing more done with it.
	const after = ball.findIndex((b) => b.t0 >= cut);
	if (after >= 0) {
		ball.length = after;
	}
	const last = ball.at(-1);
	if (last?.kind === "held") {
		ball.push({ kind: "spot", t0: cut, at: { x: spot.x, y: spot.y, z: 0 } });
	} else if (last?.kind === "fly") {
		ball.push({
			kind: "spot",
			t0: Math.max(cut, last.t1),
			at: { ...last.to, z: 0 },
		});
	}

	// The trainers: stood just off the field in front of the bench until
	// the cut - then out with it, the men near enough each taking a drink
	// once he is there.
	const trainers: CrewPart[] = [];
	const drinks = new Set<Part>();
	const water = (side: Side, n: number, start: Pt, stop: Pt, at: number) => {
		const line = sidelineOf(side);
		const bench = {
			x: clamp(start.x, MID_X - ALONG, MID_X + ALONG) + (n - 0.5) * 8,
			y: line === 0 ? -(COACH_LINE - 1) : FIELD_W + COACH_LINE - 1,
		};
		const there = at + (dist(start, stop) / WATER_FPS) * 1000;
		const legs: Leg[] = [
			{
				t0: input.t0,
				t1: cut,
				from: bench,
				to: bench,
				move: "stand",
				yaw: toward(bench, { x: bench.x, y: FIELD_W / 2 }),
			},
			{
				t0: cut,
				t1: at,
				from: start,
				to: start,
				move: "stand",
				arms: "carryWater",
				yaw: toward(start, stop),
			},
			{
				t0: at,
				t1: there,
				from: start,
				to: stop,
				move: dist(start, stop) / ((there - at) / 1000) > 7 ? "jog" : "walk",
				arms: "carryWater",
			},
			{
				t0: there,
				t1,
				from: stop,
				to: stop,
				move: "stand",
				arms: "carryWater",
			},
		];
		trainers.push({ pid: medicId(side, 1, n), team: side, legs });
		// (Those near him, a drink - one at a time or so, not all at once.)
		const near = spots
			.filter((s) => s.p.team === side && !drinks.has(s.p) && !kneel.has(s.p))
			.filter((s) => dist(s.at, stop) < DRINK_NEAR && r() < DRINKS);
		for (const { p } of near) {
			const from = there + 300;
			const then = from + r() * (t1 - 400 - DRINK_MS - from);
			if (then < from) {
				continue;
			}
			const k = p.legs.findIndex(
				(l) => l.t0 >= cut && l.t0 <= then && then + DRINK_MS <= l.t1,
			);
			const l = p.legs[k];
			if (!l) {
				continue;
			}
			drinks.add(p);
			// (Only a blink of what he was doing first: the drink from the
			// first of it, not a flick of it between.)
			const at = then - l.t0 < DRINK_LEAD_LEAST_MS ? l.t0 : then;
			p.legs.splice(
				k,
				1,
				...(at > l.t0 ? [{ ...l, t1: at }] : []),
				{ ...l, t0: at, t1: at + DRINK_MS, move: "drink" as const },
				{ ...l, t0: at + DRINK_MS },
			);
		}
	};
	// (Round either end of a side gathered at its sideline, from off the
	// field behind them.)
	const toRows = (
		side: Side,
		g: { middle: Pt; out: number; edge: number; into: number },
		at: () => number,
	) => {
		for (let n = 0; n < WATER; n++) {
			const wing = n === 0 ? -1 : 1;
			const a = g.out + wing * WATER_ROUND;
			const stop = {
				x: g.middle.x + Math.cos(a) * (ROWS[1]! + 0.6),
				y: g.middle.y + Math.sin(a) * (ROWS[1]! + 0.6),
			};
			const start = { x: stop.x + wing * 3, y: g.edge - g.into * WATER_OFF };
			water(side, n, start, stop, at());
		}
	};
	// (In to a ring from its side's sideline.)
	const toRing = (side: Side, ring: Pt, at: () => number) => {
		const toLine = sidelineOf(side) === 0 ? -1 : 1;
		for (let n = 0; n < WATER; n++) {
			const by = (n === 0 ? -1 : 1) * WATER_BY;
			const stop = { x: ring.x + by, y: ring.y + toLine * (RING + 2.2) };
			const start = {
				x: stop.x + by * 1.5,
				y: stop.y + toLine * WATER_FROM,
			};
			water(side, n, start, stop, at());
		}
	};
	if (ringAt) {
		toRing(team, ringAt, () => cut + 200 + r() * 500);
	} else {
		toRows(team, { middle, out, edge, into }, () => cut + 200 + r() * 500);
	}
	if (atLine) {
		toRows(them, atLine, () => cut + r() * 300);
	} else {
		toRing(them, ring, () => cut + r() * 300);
	}

	// The head coach: stood where he stands until the cut (calling it
	// first, if he did - then on out toward them), then out at the sideline
	// with his man.
	const from = headCoachAt(team, input.playI);
	const face = toward(coachAt, middle);
	const call = input.call;
	const tUp = call ? call.at - T_BEFORE : 0;
	const tDown = call ? call.at + T_AFTER : 0;
	const calling = !!call && tUp > input.t0 + 300 && tDown < cut;
	const legs: Leg[] = [
		{
			t0: input.t0,
			t1: calling ? tUp : cut,
			from,
			to: from,
			move: "armsFolded",
			yaw: out,
		},
	];
	if (calling) {
		const at = toward(from, spot) - out;
		const turn = Math.atan2(Math.sin(at), Math.cos(at));
		legs.push({
			t0: tUp,
			t1: tDown,
			from,
			to: from,
			move: "timeoutT",
			yaw: out + clamp(turn, -T_TURN, T_TURN),
		});
		const way = dist(from, coachAt);
		const go = Math.min(way, (T_WALK_FPS * (cut - tDown)) / 1000);
		const to =
			go > 1
				? {
						x: from.x + ((coachAt.x - from.x) * go) / way,
						y: from.y + ((coachAt.y - from.y) * go) / way,
					}
				: from;
		legs.push({
			t0: tDown,
			t1: cut,
			from,
			to,
			move: go > 1 ? "walk" : "stand",
			...(go > 1 ? {} : { yaw: out }),
		});
	}
	legs.push({
		t0: cut,
		t1: cut + 500,
		from: coachAt,
		to: coachAt,
		move: "stand",
		yaw: face,
	});
	// (The picture on him: the T going up, if it may.)
	const shotFrom =
		calling && call.shot !== undefined
			? Math.max(call.shot, call.at - T_SHOT_BEFORE)
			: undefined;
	const shot =
		call &&
		shotFrom !== undefined &&
		cut - shotFrom >= T_SHOT_LEAST &&
		shotFrom <= call.at - T_SHOT_LATEST
			? shotFrom
			: undefined;
	// (Going over it with them, a point at a time, till it is over.)
	const talk = (legs: Leg[], at: Pt, yaw: number, from: number) => {
		let t = from;
		while (t + TALK_MS <= t1) {
			const ms = TALK_MS * (0.85 + r() * 0.3);
			legs.push({
				t0: t,
				t1: t + ms,
				from: at,
				to: at,
				move: "coachTalk",
				yaw,
			});
			t += ms;
			const gap = Math.min(t1, t + TALK_GAP);
			legs.push({ t0: t, t1: gap, from: at, to: at, move: "stand", yaw });
			t = gap;
		}
		if (t < t1) {
			legs.push({ t0: t, t1, from: at, to: at, move: "stand", yaw });
		}
	};
	talk(legs, coachAt, face, cut + 500);

	// The other side at its sideline: its coordinator (the one for the
	// side of the ball it is on) out with it, from where he stands.
	const coaches: CrewPart[] = [{ pid: coachId(team, 0), team, legs }];
	if (atLine) {
		const i = other.some((p) => p.role === "QB") ? 1 : 2;
		const was = coordinatorAt(them, i, input.playI);
		const yaw = toward(atLine.coachAt, atLine.middle);
		const theirLegs: Leg[] = [
			{
				t0: input.t0,
				t1: cut,
				from: was,
				to: was,
				move: "stand",
				yaw: atLine.out,
			},
			{
				t0: cut,
				t1: cut + 300,
				from: atLine.coachAt,
				to: atLine.coachAt,
				move: "stand",
				yaw,
			},
		];
		talk(theirLegs, atLine.coachAt, yaw, cut + 300);
		coaches.push({ pid: coachId(them, i), team: them, legs: theirLegs });
	}

	return {
		cut,
		// (Over: between the two at the sideline and the rest out on the
		// field.)
		at: ringAt
			? { x: (x + ringAt.x) / 2, y: edge + into * (OVER_IN / 2 + 2) }
			: { x, y: edge + into * (MIDDLE_IN + 3) },
		t1,
		coaches: [...coaches, ...trainers],
		...(shot !== undefined
			? { call: { from: shot, pid: coachId(team, 0) } }
			: {}),
		clear: [
			{ at: middle, far: (ringAt ? TALKER_IN : ROWS[1]!) + CLEAR },
			...(ringAt ? [{ at: ringAt, far: RING + CLEAR }] : []),
			atLine
				? { at: atLine.middle, far: ROWS[1]! + CLEAR }
				: { at: ring, far: RING + CLEAR },
		],
	};
};

// The officials, from the cut: any of them stood where a side is gathered
// stood off it instead, turned to it.
export const crewClear = (crew: CrewPart[], timeout: Timeout, t1: number) => {
	const { cut } = timeout;
	for (const part of crew) {
		const at = posOf(part.legs, cut);
		for (const c of timeout.clear) {
			const d = dist(at, c.at);
			if (d >= c.far) {
				continue;
			}
			const a = d > 0.5 ? toward(c.at, at) : Math.PI / 2;
			const to = {
				x: c.at.x + Math.cos(a) * c.far,
				y: clamp(c.at.y + Math.sin(a) * c.far, -CREW_OUT, FIELD_W + CREW_OUT),
			};
			// (Kept on the field that way, still in among them: off to the
			// side of them instead, along the sideline.)
			const dy = to.y - c.at.y;
			if (dist(to, c.at) < c.far - 0.01) {
				const side = at.x >= c.at.x ? 1 : -1;
				to.x = c.at.x + side * Math.sqrt(Math.max(0, c.far * c.far - dy * dy));
			}
			cutAt(part.legs, cut);
			const l = part.legs.at(-1)!;
			if (l.t1 < cut) {
				part.legs.push({
					t0: l.t1,
					t1: cut,
					from: l.to,
					to: l.to,
					move: "stand",
				});
			}
			part.legs.push({
				t0: cut,
				t1,
				from: to,
				to,
				move: "stand",
				yaw: toward(to, c.at),
			});
			break;
		}
	}
};
