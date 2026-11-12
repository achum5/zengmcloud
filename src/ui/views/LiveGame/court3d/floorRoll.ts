import { COURT_H, COURT_W, type Pt } from "./geometry.ts";

// LOOSE ON THE FLOOR, NOBODY AFTER IT. Down off the rim with nobody to take
// it - the horn gone, say - a ball bounces the way a ball does: up off the
// hardwood with most of the speed it came down with, on the way it was
// going (a little slower each time it touches it - skidding into a roll the
// first time), straight between, each hop lower than the last - then it
// rolls to a stop. Worked out here for the director (where and when it
// stops) and for the picture (where it is meanwhile) alike.

const GRAVITY = 32.2;
const BALL_R = 0.39;
// How much of the speed it comes down with it goes back up with.
const BOUNCE_E = 0.76;
// How much of its speed along the floor it keeps touching down: skidding
// into a roll the first time, rolling already after.
const SKID_KEEP = 0.8;
const ROLL_KEEP = 0.92;
// Coming off the floor slower than this (feet a second), it just rolls.
const HOP_LEAST = 2.5;
// How hard it slows rolling (feet a second, a second).
const ROLL_DECEL = 2;

export type FloorRoll = {
	// Its way along the floor (a unit).
	ux: number;
	uy: number;
	// How fast along it, and up, as it comes off the floor the first time
	// (feet a second); how hard it slows rolling.
	h: number;
	up: number;
	decel: number;
	// Stopped dead this far along - up against the front row - if it gets
	// that far.
	wall?: number;
};

// Its hops - how long each is in the air, and how fast it goes along the
// floor meanwhile - and how fast it rolls after.
const hopsOf = (r: FloorRoll) => {
	const hops: { s: number; h: number; up: number }[] = [];
	let up = r.up;
	let h = r.h;
	while (up >= HOP_LEAST && hops.length < 30) {
		hops.push({ s: (2 * up) / GRAVITY, h, up });
		up *= BOUNCE_E;
		h *= ROLL_KEEP;
	}
	return { hops, roll: h };
};

// Up against the wall, if it gets there: how long after it first comes off
// the floor, and how high it is then and how fast it is going up. (There it
// is dead: it drops where it is.)
const wallHit = (r: FloorRoll) => {
	if (r.wall === undefined) {
		return undefined;
	}
	const { hops, roll } = hopsOf(r);
	let s = 0;
	let along = 0;
	for (const k of hops) {
		if (k.h > 0 && along + k.h * k.s >= r.wall) {
			const tau = (r.wall - along) / k.h;
			return {
				s: s + tau,
				z: BALL_R + k.up * tau - 0.5 * GRAVITY * tau * tau,
				vz: Math.min(0, k.up - GRAVITY * tau),
			};
		}
		s += k.s;
		along += k.h * k.s;
	}
	const d = r.wall - along;
	if (roll > 0 && (roll * roll) / (2 * r.decel) >= d) {
		const t = (roll - Math.sqrt(roll * roll - 2 * r.decel * d)) / r.decel;
		return { s: s + t, z: BALL_R, vz: 0 };
	}
	return undefined;
};
const dropS = (z: number, vz: number) =>
	(vz + Math.sqrt(vz * vz + 2 * GRAVITY * Math.max(0, z - BALL_R))) / GRAVITY;

// How long it is on the move (s), and how far it goes.
export const floorRollSpan = (r: FloorRoll): { s: number; far: number } => {
	const hit = wallHit(r);
	if (hit) {
		return { s: hit.s + dropS(hit.z, hit.vz), far: r.wall! };
	}
	const { hops, roll } = hopsOf(r);
	let s = 0;
	let far = 0;
	for (const k of hops) {
		s += k.s;
		far += k.h * k.s;
	}
	const rollS = roll / r.decel;
	return { s: s + rollS, far: far + (roll * rollS) / 2 };
};

// Where it is `s` seconds after it first comes off the floor: how far along
// its way, and how high its middle is.
export const floorRollAt = (
	r: FloorRoll,
	s: number,
): { along: number; z: number } => {
	const hit = wallHit(r);
	if (hit && s >= hit.s) {
		const d = Math.min(s - hit.s, dropS(hit.z, hit.vz));
		return {
			along: r.wall!,
			z: Math.max(BALL_R, hit.z + hit.vz * d - 0.5 * GRAVITY * d * d),
		};
	}
	const { hops, roll } = hopsOf(r);
	let left = Math.max(0, s);
	let along = 0;
	for (const k of hops) {
		if (left <= k.s) {
			return {
				along: along + k.h * left,
				z: BALL_R + k.up * left - 0.5 * GRAVITY * left * left,
			};
		}
		left -= k.s;
		along += k.h * k.s;
	}
	const t = Math.min(left, roll / r.decel);
	return { along: along + roll * t - 0.5 * r.decel * t * t, z: BALL_R };
};

// How far it can go from `at` along (ux, uy) before it is `margin` short of
// a line - or, `margin` below 0, that far past one.
export const roomOnFloor = (
	at: Pt,
	ux: number,
	uy: number,
	margin: number,
): number => {
	const to = (p: number, u: number, hi: number) =>
		u > 1e-9 ? (hi - margin - p) / u : u < -1e-9 ? (p - margin) / -u : Infinity;
	return Math.max(0, Math.min(to(at.x, ux, COURT_W), to(at.y, uy, COURT_H)));
};

// Come down on the floor at `at` going `v` (feet a second; v.z below 0):
// how it bounces and rolls - no farther than `room` feet, and stopped
// within `most` seconds (lower and slower, if it would not be). Or, `wall`,
// as far as it goes - stopped dead `room` feet on, if it gets there.
export const floorRoll = (
	v: { x: number; y: number; z: number },
	room: number,
	most: number,
	wall = false,
): FloorRoll => {
	const along = Math.hypot(v.x, v.y);
	const ux = along > 1e-6 ? v.x / along : 1;
	const uy = along > 1e-6 ? v.y / along : 0;
	// (Its hops all over in most of the time it has: they take
	// 2 up / g / (1 - e) between them, near enough.)
	const up = Math.min(
		BOUNCE_E * Math.max(0, -v.z),
		(0.8 * most * GRAVITY * (1 - BOUNCE_E)) / 2,
	);
	const r: FloorRoll = { ux, uy, h: along * SKID_KEEP, up, decel: ROLL_DECEL };
	const { hops, roll } = hopsOf(r);
	let hopS = 0;
	let air = 0;
	for (const k of hops) {
		hopS += k.s;
		air += k.h * k.s;
	}
	if (wall) {
		r.wall = room;
		const rollV = roll;
		if (rollV > 0) {
			r.decel = Math.max(ROLL_DECEL, rollV / Math.max(0.2, most - hopS));
		}
		return r;
	}
	// Farther in the air than it has room for: it goes slower along.
	if (air > room * 0.9 && air > 0) {
		const f = (room * 0.9) / air;
		r.h *= f;
		air *= f;
	}
	const rollV = roll * (r.h / (along * SKID_KEEP || 1));
	// And rolls to a stop in the room - and the time - it has left.
	if (rollV > 0) {
		const byRoom = (rollV * rollV) / (2 * Math.max(0.3, room - air));
		const byTime = rollV / Math.max(0.2, most - hopS);
		r.decel = Math.max(ROLL_DECEL, byRoom, byTime);
	}
	return r;
};
