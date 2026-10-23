import { posOf, type CrewPart } from "./crew.ts";
import type { BallSeg, Leg, Part } from "./director.ts";
import { dist, FIELD_W, MID_X, type Pt, type Side } from "./geometry.ts";
import type { Move } from "./poses.ts";
import { cutAt } from "./postgame.ts";
import { coachId, headCoachAt, sidelineOf } from "./sideline.ts";

// A TIMEOUT (the two-minute warning too): the referee's signal, then the
// picture cuts to the side that called it gathered at the edge of the field
// in front of its bench - the head coach out at the sideline going over it
// with the quarterback, the rest stood round them, hands on hips, bent over
// getting a breath - and the other side in a ring of its own out on the
// field by its numbers. Then the picture cuts on to the next snap.

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
// The head coach's points, each this long, a moment between them.
const TALK_MS = 2300;
const TALK_GAP = 400;

export type Timeout = {
	// When the picture cuts to it, and where it looks.
	cut: number;
	at: Pt;
	t1: number;
	// The head coach out of the team area.
	coaches: CrewPart[];
	// Where each side is gathered, and how far round it to keep clear.
	clear: { at: Pt; far: number }[];
};

const toward = (a: Pt, b: Pt) => Math.atan2(b.y - a.y, b.x - a.x);
const clamp = (x: number, lo: number, hi: number) =>
	Math.min(hi, Math.max(lo, x));

// A man's way of standing it.
const stoodOf = (r: () => number): Move => {
	const u = r();
	return u < HIPS ? "onHips" : u < HIPS + BENT ? "hands" : "stand";
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
}): Timeout => {
	const { parts, ball, cut, team, spot, r } = input;
	const t1 = cut + TIMEOUT_MS + r() * TIMEOUT_MORE;

	// Each man as the play left him until the cut, then where the scene has
	// him.
	const place = (p: Part, at: Pt, yaw: number, move: Move) => {
		cutAt(p.legs, cut);
		const l = p.legs.at(-1)!;
		if (l.t1 < cut) {
			p.legs.push({ t0: l.t1, t1: cut, from: l.to, to: l.to, move: "stand" });
		}
		p.legs.push({ t0: cut, t1, from: at, to: at, move, yaw });
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

	// The side that called it, at its sideline.
	const line = sidelineOf(team);
	const into = line === 0 ? 1 : -1;
	const edge = line === 0 ? 0 : FIELD_W;
	const x = clamp(spot.x, MID_X - ALONG, MID_X + ALONG) + (r() - 0.5) * 6;
	const middle = { x, y: edge + into * MIDDLE_IN };
	const coachAt = { x, y: edge + into * COACH_IN };
	const mine = parts.filter((p) => p.pid >= 0 && p.team === team);
	const talker =
		mine.find((p) => p.role === "QB") ??
		mine.find((p) => p.role === "MIKE") ??
		mine[0];
	const out = Math.atan2(into, 0);
	let k = 0;
	for (const p of mine) {
		if (p === talker) {
			const at = {
				x: x + (line === 0 ? ASIDE : 0) + (r() - 0.5) * 1.2,
				y: edge + into * TALKER_IN,
			};
			place(p, at, toward(at, coachAt), "stand");
			continue;
		}
		const row = k < FIRST_ROW ? 0 : 1;
		const inRow =
			row === 0
				? Math.min(FIRST_ROW, mine.length - 1)
				: mine.length - 1 - FIRST_ROW;
		const j = row === 0 ? k : k - FIRST_ROW;
		// (Across the field from the camera, gathered either side of the two,
		// not in front of them.)
		const u = inRow > 1 ? j / (inRow - 1) : 0.5;
		const wing = j % 2 === 0 ? -1 : 1;
		const v =
			inRow > 2 ? Math.floor(j / 2) / Math.max(1, Math.ceil(inRow / 2) - 1) : 0;
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
		place(p, at, toward(at, middle) + (r() - 0.5) * 0.4, stoodOf(r));
		k++;
	}

	// The other side, in a ring of its own.
	const other = parts.filter((p) => p.pid >= 0 && p.team !== team);
	const theirs = line === 0 ? FIELD_W : 0;
	const ring = {
		x: clamp(spot.x, MID_X - RING_ALONG, MID_X + RING_ALONG),
		y: theirs + (line === 0 ? -RING_IN : RING_IN),
	};
	const turn = r() * Math.PI * 2;
	other.forEach((p, j) => {
		const a = turn + (j / other.length) * Math.PI * 2 + (r() - 0.5) * 0.15;
		const far = RING + (r() - 0.5) * 0.7;
		const at = { x: ring.x + Math.cos(a) * far, y: ring.y + Math.sin(a) * far };
		place(p, at, toward(at, ring) + (r() - 0.5) * 0.3, stoodOf(r));
	});

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

	// The head coach: stood where he stands until the cut, then out at the
	// sideline with his man.
	const from = headCoachAt(team, input.playI);
	const face = toward(coachAt, middle);
	const legs: Leg[] = [
		{
			t0: input.t0,
			t1: cut,
			from,
			to: from,
			move: "armsFolded",
			yaw: Math.atan2(into, 0),
		},
		{
			t0: cut,
			t1: cut + 500,
			from: coachAt,
			to: coachAt,
			move: "stand",
			yaw: face,
		},
	];
	let t = cut + 500;
	while (t + TALK_MS <= t1) {
		const ms = TALK_MS * (0.85 + r() * 0.3);
		legs.push({
			t0: t,
			t1: t + ms,
			from: coachAt,
			to: coachAt,
			move: "coachTalk",
			yaw: face,
		});
		t += ms;
		const gap = Math.min(t1, t + TALK_GAP);
		legs.push({
			t0: t,
			t1: gap,
			from: coachAt,
			to: coachAt,
			move: "stand",
			yaw: face,
		});
		t = gap;
	}
	if (t < t1) {
		legs.push({
			t0: t,
			t1,
			from: coachAt,
			to: coachAt,
			move: "stand",
			yaw: face,
		});
	}

	return {
		cut,
		at: { x, y: edge + into * (MIDDLE_IN + 3) },
		t1,
		coaches: [{ pid: coachId(team, 0), team, legs }],
		clear: [
			{ at: middle, far: ROWS[1]! + CLEAR },
			{ at: ring, far: RING + CLEAR },
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
