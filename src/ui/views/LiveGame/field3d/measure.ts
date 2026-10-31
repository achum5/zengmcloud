import { chainId } from "./chains.ts";
import { officialId, posOf, type CrewPart } from "./crew.ts";
import type { BallSeg, Leg, Part } from "./director.ts";
import { dist, FIELD_W, type Dir, type Pt, type Side } from "./geometry.ts";
import type { Move } from "./poses.ts";
import { cutAt } from "./postgame.ts";

// A MEASUREMENT: a first down made by no more than the nose of the ball,
// and the picture cuts to the chain out on the field beside it - the rear
// rod back where the series began, the front one on the line to gain, the
// down judge down on a knee with the chain at its clip, the referee bent
// over the front rod - both sides gathered round to see. Then the referee
// up, and his arm out for the first down: the offense's men glad of it.

// The scene this long; the call this far into it.
const MEASURE_MS = 6200;
const CALL_AT = 3400;
// The chain this far off the ball, toward its sideline; each rod's man this
// far outside his rod (where his hands have it); the referee this far past the front one, across from
// it; the umpire stood off to the side.
const CHAIN_OFF = 1.6;
const MAN_BACK = 1;
const REF_PAST = 0.9;
const REF_ACROSS = 1.6;
const UMP_OFF = 4.5;
// The rest of the crew no nearer than this.
const CREW_CLEAR = 15;
// The men round it: this near at the nearest, in rows this far apart;
// the offense back toward its own end, the defense out ahead - each side's
// men through this much of a turn either way of it, leaning off toward the
// far side; none of them in the way of the camera (this much of a turn
// either side of straight at it), nor stood on the chain (this far off it).
const NEAREST = 11.5;
const ROW = 3.4;
const PER_ROW = 6;
const SPREAD = 1.2;
const LEAN = 0.35;
const CAMERA_CLEAR = 0.6;
const CHAIN_CLEAR = 2.6;
// How each stands it (see timeout.ts), and the offense's men glad of it.
const HIPS = 0.3;
const BENT = 0.2;
const GLAD = 0.45;
const RUE = 0.2;
// The first-down signal held this long.
const SIGNAL_MS = 1500;

export type Measure = {
	// When the picture cuts to it, where it looks, and when the call comes.
	cut: number;
	at: Pt;
	call: number;
	t1: number;
};

const toward = (a: Pt, b: Pt) => Math.atan2(b.y - a.y, b.x - a.x);
// (The way to the camera, on the near side.)
const CAMERA = Math.PI / 2;

// When the picture cuts to it, and when it ends: from just after the play
// is over.
export const measureTimes = (after: number) => ({
	cut: after,
	call: after + CALL_AT,
	t1: after + MEASURE_MS,
});

export const stageMeasure = (input: {
	parts: Part[];
	crew: CrewPart[];
	chains: CrewPart[];
	ball: BallSeg[];
	cut: number;
	// Where the ball is spotted, and the line to gain.
	spot: Pt;
	gainX: number;
	dir: Dir;
	offense: Side;
	// The chain's sideline (0 the far one).
	side: 0 | 1;
	r: () => number;
}): Measure => {
	const { parts, crew, chains, ball, cut, spot, gainX, dir, offense, r } =
		input;
	const { call, t1 } = measureTimes(cut);
	// (Toward the chain's sideline from the ball: +1 the near one.)
	const toChain = input.side === 0 ? -1 : 1;
	const chainY = spot.y + toChain * CHAIN_OFF;
	const rearX = gainX - dir * 30;
	const at = { x: gainX, y: spot.y };

	// Each from the cut where the scene has him, as he stands it.
	const from = (legs: Leg[]) => {
		cutAt(legs, cut);
		const l = legs.at(-1)!;
		if (l.t1 < cut) {
			legs.push({ t0: l.t1, t1: cut, from: l.to, to: l.to, move: "stand" });
		}
	};
	const still = (
		legs: Leg[],
		t0: number,
		t: number,
		p: Pt,
		move: Move,
		yaw: number,
		extra?: Partial<Leg>,
	) => {
		if (t > t0) {
			legs.push({ t0, t1: t, from: p, to: p, move, yaw, ...extra });
		}
	};

	// The rods: the rear one back where the series began, the front one on
	// the line to gain - each man just outside his, along the chain, facing
	// it (his rod in his hands in front of him), the chain taut between.
	for (const [c, x, way] of [
		["rear", rearX, -1],
		["front", gainX, 1],
	] as const) {
		const man = chains.find((p) => p.pid === chainId(c));
		if (!man) {
			continue;
		}
		from(man.legs);
		still(
			man.legs,
			cut,
			t1,
			{ x: x + way * dir * MAN_BACK, y: chainY },
			"holdPole",
			way * dir > 0 ? Math.PI : 0,
		);
	}

	// The officials: the down judge on a knee at the clip, the referee bent
	// over the front rod and then up for the call, the umpire by - the rest
	// stood off.
	for (const part of crew) {
		const o = part.pid;
		from(part.legs);
		if (o === officialId("DJ")) {
			const p = { x: gainX - dir * 15, y: chainY - toChain * 0.6 };
			still(part.legs, cut, t1, p, "holdDown", dir === 1 ? 0 : Math.PI);
		} else if (o === officialId("R")) {
			const p = {
				x: gainX + dir * REF_PAST,
				y: spot.y - toChain * REF_ACROSS,
			};
			const look = toward(p, { x: gainX, y: chainY });
			still(part.legs, cut, call, p, "hands", look);
			// (Up, faced to the press box, his arm out up the field.)
			const yaw = Math.PI / 2;
			const left = -Math.sin(yaw) * dir < 0;
			still(part.legs, call, call + 400, p, "stand", yaw);
			still(part.legs, call + 400, call + 400 + SIGNAL_MS, p, "stand", yaw, {
				arms: "sigSide",
				...(left ? { mirrored: true } : {}),
			});
			still(part.legs, call + 400 + SIGNAL_MS, t1, p, "stand", yaw);
		} else if (o === officialId("U")) {
			const p = { x: gainX - dir * 2.5, y: spot.y - toChain * UMP_OFF };
			still(part.legs, cut, t1, p, "stand", toward(p, at));
		} else {
			const was = posOf(part.legs, cut);
			const d = dist(was, at);
			const p =
				d >= CREW_CLEAR
					? was
					: {
							x: at.x + ((was.x - at.x) / (d || 1)) * CREW_CLEAR,
							y: at.y + ((was.y - at.y) / (d || 1)) * CREW_CLEAR,
						};
			still(
				part.legs,
				cut,
				t1,
				{ x: p.x, y: Math.min(FIELD_W + 4, Math.max(-4, p.y)) },
				"stand",
				toward(p, at),
			);
		}
	}

	// The men round it, each side's in rows of its own.
	for (const team of [offense, 1 - offense] as Side[]) {
		const mine = parts.filter((p) => p.pid >= 0 && p.team === team);
		const isOffense = team === offense;
		const mid = Math.atan2(-LEAN, isOffense ? -dir : dir);
		mine.forEach((p, k) => {
			const row = Math.floor(k / PER_ROW);
			const inRow = Math.min(PER_ROW, mine.length - row * PER_ROW);
			const j = k - row * PER_ROW;
			const u = inRow > 1 ? j / (inRow - 1) : 0.5;
			let a = mid + (u * 2 - 1) * SPREAD + (row % 2) * 0.12 + (r() - 0.5) * 0.1;
			// (Not between the camera and it.)
			const off = Math.atan2(Math.sin(a - CAMERA), Math.cos(a - CAMERA));
			if (Math.abs(off) < CAMERA_CLEAR) {
				a = CAMERA + Math.sign(off || dir) * CAMERA_CLEAR;
			}
			const far = NEAREST + row * ROW + (r() - 0.5) * 1.2;
			const spotOf = {
				x: at.x + Math.cos(a) * far,
				y: at.y + Math.sin(a) * far,
			};
			// (Off the chain: over it, or short of it if that is out of
			// bounds.)
			const along = (spotOf.x - rearX) * dir;
			if (
				along > -1 &&
				along < 31 &&
				Math.abs(spotOf.y - chainY) < CHAIN_CLEAR
			) {
				const over = chainY + toChain * CHAIN_CLEAR;
				spotOf.y =
					over > 2 && over < FIELD_W - 2
						? over
						: chainY - toChain * CHAIN_CLEAR;
			}
			spotOf.y = Math.min(FIELD_W - 2, Math.max(2, spotOf.y));
			const look = toward(spotOf, at) + (r() - 0.5) * 0.3;
			const u2 = r();
			const stood: Move =
				u2 < HIPS ? "blowing" : u2 < HIPS + BENT ? "winded" : "stand";
			from(p.legs);
			const said = call + 450 + r() * 500;
			still(p.legs, cut, said, spotOf, stood, look);
			const glad = isOffense ? r() < GLAD : false;
			const rue = !isOffense && r() < RUE;
			if (glad || rue) {
				const move: Move = rue
					? "handsOnHelmet"
					: r() < 0.5
						? "clap"
						: "fistPump";
				const ms = rue ? 1500 : 1100;
				still(p.legs, said, said + ms, spotOf, move, look);
				still(p.legs, said + ms, t1, spotOf, "stand", look);
			} else {
				still(p.legs, said, t1, spotOf, stood, look);
			}
		});
	}

	// The ball where it was spotted, nothing more done with it.
	const after = ball.findIndex((b) => b.t0 >= cut);
	if (after >= 0) {
		ball.length = after;
	}
	ball.push({ kind: "spot", t0: cut, at: { x: spot.x, y: spot.y, z: 0 } });

	return { cut, at, call, t1 };
};
