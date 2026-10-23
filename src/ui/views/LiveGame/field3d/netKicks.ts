import { makeCourtRng } from "../courtRng.ts";
import type { Play } from "./director.ts";
import { dist, GOAL_L, GOAL_R, YD, type Pt, type Pt3 } from "./geometry.ts";
import { strideOf, type Move } from "./poses.ts";
import type { FieldPlayerState } from "./sprite.ts";

// THE KICKERS AT THEIR NET: his side driving into range, the place kicker
// off a tee into the net at the end of the bench - his back to the field -
// now and then, while the play goes on; his side backed up on third down,
// the punter dropping one into it. Up to it, through it, the ball into the
// net and down at the foot of it; a moment watching, and back to where he
// stands.

// How often, a play, the kicker does - with his side this near the goal at
// most; the punter, with his this far off it at least, on third down.
const PLACE = 0.3;
const RANGE = 50 * YD;
const PUNT = 0.5;
const DEEP = 55 * YD;
// No sooner into the play than this, nor later than this before its end.
const FROM = 1500;
const BEFORE_END = 1800;
// The place kick (see action.ts placeKick): the tee this far out from the
// net; the kicker stood back this far and off to his left this far, up to
// his plant (back of it and to its left) over this long at a jog, and
// through the ball - his foot into it this far through the kick.
const TEE = 9;
const BACK = 5.5;
const LEFT = 4.5;
const PLANT_BACK = 1.7;
const PLANT_LEFT = 1;
const RUN_MS = 850;
const KICK_MS = 800;
const KICK_CONTACT = 0.4;
const TEE_Z = 0.8;
// The punt (see action.ts): from this far back of the net, two steps on
// with the ball held out, dropped onto his foot.
const PUNT_BACK = 12.6;
const PUNT_MS = 1400;
const PUNT_STEP = 6;
const PUNT_DROP = 0.42;
const PUNT_CONTACT = 0.55;
// Into the net this high (a range), in this long; down the net to its
// foot, a little back off it.
const NET_Z: [number, number] = [3.2, 5.6];
const INTO_MS = 140;
const G = 32;
const OFF_NET = 0.7;
// Watching it this long, then back to his place at a walk.
const WATCH_MS = 1300;
const WALK_FPS = 4.2;
// Into and out of each thing he does over this long.
const BLEND_MS = 220;

export type NetKick = {
	kind: "place" | "punt";
	// Facing the net (and that way), and his left.
	yaw: number;
	fwd: Pt;
	left: Pt;
	// Where he starts, where his foot meets the ball, when.
	start: Pt;
	ball: Pt;
	contact: number;
	// The ball into the net here, down at its foot here.
	net: Pt3;
	rest: Pt;
	// Back to his place.
	home: Pt;
};

const plus = (a: Pt, b: Pt, k: number): Pt => ({
	x: a.x + b.x * k,
	y: a.y + b.y * k,
});

// Whether he kicks into the net on a play, and how: the middle of the net
// at `net`, kicked into going `fwd`; his own place `home`.
export const netKickOf = (
	kind: "place" | "punt",
	id: number,
	play: Play,
	net: Pt,
	fwd: Pt,
	home: Pt,
): NetKick | null => {
	if (play.kind !== "run" && play.kind !== "pass") {
		return null;
	}
	if (play.postgame || play.toss) {
		return null;
	}
	const toGo = play.dir === 1 ? GOAL_R - play.losX : play.losX - GOAL_L;
	const r = makeCourtRng(`net|${id}|${play.i}`);
	const will =
		kind === "place"
			? toGo <= RANGE && r() < PLACE
			: toGo >= DEEP && play.situation.startsWith("3rd") && r() < PUNT;
	if (!will) {
		return null;
	}
	const latest = play.t1 - BEFORE_END;
	if (latest < play.t0 + FROM) {
		return null;
	}
	const contact = play.t0 + FROM + r() * (latest - play.t0 - FROM);
	const yaw = Math.atan2(fwd.y, fwd.x);
	const left = { x: Math.sin(yaw), y: -Math.cos(yaw) };
	const across = (r() - 0.5) * 2.4;
	const ballAt = plus(plus(net, fwd, -TEE), left, across);
	const start =
		kind === "place"
			? plus(plus(ballAt, fwd, -BACK), left, LEFT)
			: plus(plus(net, fwd, -PUNT_BACK), left, across);
	const hit = {
		...plus(net, left, across + (r() - 0.5) * 1.6),
		z: NET_Z[0] + r() * (NET_Z[1] - NET_Z[0]),
	};
	return {
		kind,
		yaw,
		fwd,
		left,
		start,
		ball:
			kind === "place"
				? ballAt
				: plus(plus(start, fwd, PUNT_STEP * PUNT_CONTACT + 2.3), left, -0.6),
		contact,
		net: hit,
		rest: plus(hit, fwd, -OFF_NET),
		home,
	};
};

const toward = (a: Pt, b: Pt) => Math.atan2(b.y - a.y, b.x - a.x);
const lerp = (a: Pt, b: Pt, u: number): Pt => ({
	x: a.x + (b.x - a.x) * u,
	y: a.y + (b.y - a.y) * u,
});
const clamp01 = (u: number) => Math.min(1, Math.max(0, u));

// The times of it: the approach, the kick, done, home.
const timesOf = (k: NetKick) => {
	const place = k.kind === "place";
	const begin = place
		? k.contact - KICK_MS * KICK_CONTACT
		: k.contact - PUNT_MS * PUNT_CONTACT;
	const run = place ? begin - RUN_MS : begin;
	const done = begin + (place ? KICK_MS : PUNT_MS);
	const after = place
		? plus(plus(k.ball, k.fwd, 1.4), k.left, 1)
		: plus(k.start, k.fwd, PUNT_STEP);
	const leave = Math.max(done, k.contact + WATCH_MS);
	const back = leave + (dist(after, k.home) / WALK_FPS) * 1000;
	return { run, begin, done, after, leave, back };
};

// Him at t - or nothing, once he is back in his place.
export const netKickAt = (
	k: NetKick,
	id: number,
	t: number,
): Omit<FieldPlayerState, "id" | "team"> | null => {
	const { run, begin, done, after, leave, back } = timesOf(k);
	if (t >= back) {
		return null;
	}
	const place = k.kind === "place";
	const plant = place
		? plus(plus(k.ball, k.fwd, -PLANT_BACK), k.left, PLANT_LEFT)
		: k.start;
	const offset = (Math.abs(id) * 0.618) % 1;
	const blend = (move: Move, phase: number, since: number) =>
		since < BLEND_MS ? { from: { move, phase, w: 1 - since / BLEND_MS } } : {};
	const still = (at: Pt, yaw: number, move: Move, phase = 0) => ({
		x: at.x,
		y: at.y,
		z: 0,
		yaw,
		move,
		phase,
	});
	const kickMove: Move = place ? "kick" : "punt";
	const kickMs = place ? KICK_MS : PUNT_MS;
	if (t < run) {
		return {
			...still(k.start, k.yaw, "stand"),
			...(place ? {} : { ball: "hands" as const }),
		};
	}
	if (t < begin) {
		// (Up to his plant, at a jog.)
		const u = (t - run) / RUN_MS;
		const at = lerp(k.start, plant, u);
		return {
			...still(at, toward(k.start, plant), "jog"),
			phase: ((dist(k.start, at) / strideOf("jog")) % 1) + offset,
			...blend("stand", 0, t - run),
		};
	}
	if (t < done) {
		const u = (t - begin) / kickMs;
		const holding = !place && u < PUNT_DROP;
		return {
			...still(lerp(plant, after, u), k.yaw, kickMove, u),
			...(holding ? { ball: "hands" as const } : {}),
			...(place ? blend("jog", 0, t - begin) : {}),
		};
	}
	if (t < leave) {
		return {
			...still(after, k.yaw, "stand"),
			...blend(kickMove, 1, t - done),
		};
	}
	const u = clamp01((t - leave) / Math.max(1, back - leave));
	const at = lerp(after, k.home, u);
	return {
		...still(at, toward(after, k.home), "walk"),
		phase: dist(after, at) / strideOf("walk") + offset,
		...blend("stand", 0, t - leave),
	};
};

// The ball at t: on its tee, in the air, down at the foot of the net - or
// in his hands (nothing, here).
export const netBallAt = (
	k: NetKick,
	t: number,
): { at: Pt3; axis?: Pt3; stood?: "tee" } | null => {
	const place = k.kind === "place";
	const { begin } = timesOf(k);
	const into = k.contact + INTO_MS;
	const fall = Math.sqrt((2 * k.net.z) / G) * 1000;
	const tumble = (since: number, a: Pt3) => {
		const turn = (since / 1000) * 9;
		const flat = Math.hypot(a.x, a.y) || 1;
		return {
			x: (a.x / flat) * Math.cos(turn),
			y: (a.y / flat) * Math.cos(turn),
			z: Math.sin(turn),
		};
	};
	if (t < k.contact) {
		if (place) {
			return { at: { ...k.ball, z: 0 }, stood: "tee" };
		}
		// (Dropped from his hands onto his foot.)
		const drop = begin + PUNT_MS * PUNT_DROP;
		if (t < drop) {
			return null;
		}
		const u = (t - drop) / Math.max(1, k.contact - drop);
		return {
			at: { ...k.ball, z: 3.2 + (1.7 - 3.2) * u },
			axis: { x: k.fwd.x * 0.6, y: k.fwd.y * 0.6, z: 0.8 },
		};
	}
	const from: Pt3 = { ...k.ball, z: place ? TEE_Z : 1.7 };
	if (t < into) {
		const u = (t - k.contact) / INTO_MS;
		return {
			at: {
				x: from.x + (k.net.x - from.x) * u,
				y: from.y + (k.net.y - from.y) * u,
				z: from.z + (k.net.z - from.z) * u,
			},
			axis: tumble(t - k.contact, {
				x: k.net.x - from.x,
				y: k.net.y - from.y,
				z: 0,
			}),
		};
	}
	if (t < into + fall) {
		const s = (t - into) / 1000;
		const u = (t - into) / fall;
		return {
			at: {
				x: k.net.x + (k.rest.x - k.net.x) * u,
				y: k.net.y + (k.rest.y - k.net.y) * u,
				z: Math.max(0, k.net.z - 0.5 * G * s * s),
			},
			axis: tumble((t - into) * 0.4, { ...k.fwd, z: 0 }),
		};
	}
	return { at: { ...k.rest, z: 0 } };
};
