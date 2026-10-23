import { CREW, officialId, type CrewPart } from "./crew.ts";
import type { Leg } from "./director.ts";
import { MID_X, MID_Y, type Pt, type Side } from "./geometry.ts";
import type { Move } from "./poses.ts";
import { Man, meet } from "./postgame.ts";
import { sidelineOf } from "./sideline.ts";

// THE COIN TOSS, before the opening kickoff - and overtime's: each side's
// captains out to the middle of the field to face each other, the referee
// at the end of the two of them and the rest of the crew stood behind him;
// the coin up off his thumb, a look at it, his arm out to the side that won
// it; the captains shaking hands across, and off back to their sidelines.
// Then the picture cuts to the kickoff, everybody set for it - the
// captains back on their sidelines.

// Out from this far off the middle (feet, and up to this much further), at
// this pace (feet a second, give or take), to this far off it - this far
// apart side by side, and this far along the field from the middle (the
// referee this far the other way, facing them).
const FROM = 30;
const FROM_MORE = 4;
const PACE = 5.4;
const STOP = 6;
const APART = 3.3;
const ALONG = 1.6;
const REF_ALONG = 5.4;
// The coin up this long after they are all there; the referee's look at it,
// his arm out to the side that won it - and the handshakes this long after.
const FLIP_AFTER = 700;
const FLIP_MS = 1400;
const LOOK_MS = 500;
const POINT_MS = 1300;
const SHAKE_AFTER = 150;
// Then off toward their sidelines, this far, at a jog - the cut this long
// after the last of them goes.
const OFF = 16;
const JOG = 8.5;
const CUT_AFTER = 1300;
// The rest of the crew stood behind the referee: this far behind him, in a
// shallow arc (this much further back each man out from its middle), this
// far apart.
const CREW_BACK = 6;
const CREW_ARC = 1.4;
const CREW_GAP = 3.4;

export type Toss = {
	// The captains, out and back.
	men: CrewPart[];
	// Each official until the cut (by id).
	crew: Map<number, Leg[]>;
	at: Pt;
	flip: number;
	cut: number;
};

const toward = (a: Pt, b: Pt) => Math.atan2(b.y - a.y, b.x - a.x);
// Off the middle toward a side's sideline (the home team's across the field).
const awayOf = (team: Side) => (sidelineOf(team) === 0 ? -1 : 1);

export const stageToss = ({
	captains,
	t0,
	r,
}: {
	captains: [number[], number[]];
	t0: number;
	r: () => number;
}): Toss => {
	const at = { x: MID_X, y: MID_Y };
	const sides = ([0, 1] as Side[]).map((team) => {
		const sgn = awayOf(team);
		const pids = captains[team];
		return pids.map((pid, k) => {
			const x = MID_X + ALONG + (k - (pids.length - 1) / 2) * APART;
			const from = {
				x: x + (r() - 0.5) * 1.6,
				y: MID_Y + sgn * (FROM + r() * FROM_MORE),
			};
			const man = new Man(
				team,
				[
					{
						t0,
						t1: t0,
						from,
						to: from,
						move: "walk",
						yaw: sgn > 0 ? -Math.PI / 2 : Math.PI / 2,
					},
				],
				PACE * (0.92 + r() * 0.16),
			);
			man.walk({ x, y: MID_Y + sgn * STOP });
			return { pid, man };
		});
	});
	const all = sides.flat();
	const there = Math.max(t0, ...all.map((c) => c.man.t));
	const flip = there + FLIP_AFTER;
	const won = (r() < 0.5 ? 0 : 1) as Side;

	// The referee: facing down between them for the toss, then an arm out
	// to the winners to give it them.
	const refAt = { x: MID_X - REF_ALONG, y: MID_Y };
	const ref = new Man(
		0,
		[{ t0, t1: t0, from: refAt, to: refAt, move: "stand", yaw: 0 }],
		PACE,
	);
	ref.hold(flip, "stand", 0);
	ref.act("coinFlip", FLIP_MS, 0);
	ref.hold(ref.t + LOOK_MS, "stand", 0);
	const winners = sides[won]!.map((c) => c.man.at);
	const to = {
		x: winners.reduce((a, p) => a + p.x, 0) / Math.max(1, winners.length),
		y: MID_Y + awayOf(won) * STOP,
	};
	ref.act("pointFirst", POINT_MS, toward(refAt, to));
	const pointed = ref.t;
	ref.hold(pointed + 200, "stand", toward(refAt, to) / 2);

	// The captains: watching it, then across to the man opposite.
	for (const { man } of all) {
		man.hold(flip, "stand", toward(man.at, refAt));
		man.hold(pointed + SHAKE_AFTER, "stand", toward(man.at, refAt));
	}
	const [near, far] = sides as [
		{ pid: number; man: Man }[],
		{ pid: number; man: Man }[],
	];
	let done = pointed + SHAKE_AFTER;
	for (let k = 0; k < Math.min(near.length, far.length); k++) {
		const a = far[k]!.man;
		const b = near[k]!.man;
		const m = meet(a, b, r, { x: (a.at.x + b.at.x) / 2, y: MID_Y }, 0);
		done = Math.max(done, m.end);
	}
	// And off, each the way he came.
	let gone = done;
	for (const { man } of all) {
		man.hold(Math.max(man.t, done - 300 + r() * 500));
		man.walk(
			{
				x: man.at.x + (r() - 0.5) * 5,
				y: MID_Y + awayOf(man.team) * (STOP + OFF + r() * 4),
			},
			JOG * (0.9 + r() * 0.2),
		);
		gone = Math.max(gone, man.t);
	}
	const cut = Math.max(gone - 400, done + CUT_AFTER);
	for (const { man } of all) {
		man.legs.splice(0, man.legs.length, ...clip(man.legs, cut));
	}
	ref.hold(cut, "stand", 0);

	// The rest of the crew stood by.
	const crew = new Map<number, Leg[]>();
	crew.set(officialId("R"), clip(ref.legs, cut));
	const rest = CREW.filter((o) => o !== "R");
	rest.forEach((o, j) => {
		const off = j - (rest.length - 1) / 2;
		const p = {
			x: refAt.x - CREW_BACK - Math.abs(off) * CREW_ARC + (r() - 0.5) * 0.8,
			y: MID_Y + off * CREW_GAP + (r() - 0.5) * 0.6,
		};
		crew.set(officialId(o), [
			{
				t0,
				t1: cut,
				from: p,
				to: p,
				move: "stand" as Move,
				yaw: toward(p, at) + (r() - 0.5) * 0.3,
			},
		]);
	});

	return {
		men: all.map(({ pid, man }) => ({ pid, team: man.team, legs: man.legs })),
		crew,
		at,
		flip,
		cut,
	};
};

// Legs until t: those after it gone, the one across it cut short there.
const clip = (legs: Leg[], t: number): Leg[] => {
	const out: Leg[] = [];
	for (const l of legs) {
		if (l.t0 >= t) {
			break;
		}
		if (l.t1 <= t) {
			out.push(l);
			continue;
		}
		const u = l.t1 > l.t0 ? (t - l.t0) / (l.t1 - l.t0) : 1;
		out.push({
			...l,
			t1: t,
			to: {
				x: l.from.x + (l.to.x - l.from.x) * u,
				y: l.from.y + (l.to.y - l.from.y) * u,
			},
			...(l.span
				? { span: [l.span[0], l.span[0] + (l.span[1] - l.span[0]) * u] }
				: {}),
		});
	}
	const last = out.at(-1);
	if (last && last.t1 < t) {
		out.push({ ...last, t0: last.t1, t1: t, from: last.to, to: last.to });
	}
	return out;
};

// The officials' play legs from the cut on, after what they did until it.
export const tossCrew = (toss: Toss, crew: CrewPart[]) => {
	for (const part of crew) {
		const before = toss.crew.get(part.pid);
		if (before) {
			part.legs.unshift(...before);
		}
	}
};
