import type { Act, BallSeg, FxKind, Fx, RetroTimeline } from "./director.ts";
import { K, persp, PX_PER_FT, type Pt3, type Side } from "./geometry.ts";
import {
	actFrame,
	ANIMS,
	cycleFrame,
	loopFrame,
	poseFor,
	skeleton,
	type AnimName,
	type Body,
} from "./poses.ts";

// WHERE EVERYTHING IS AT A MOMENT.
//
// The director wrote down intentions over time; this reads them back at any
// instant t. Pure, so the renderer can ask about any frame (and a seek can jump
// anywhere) and the tests can sample a whole game.

const clamp01 = (u: number) => Math.min(1, Math.max(0, u));
const ease = (u: number) => 0.5 - 0.5 * Math.cos(Math.PI * clamp01(u));

// Index of the last item with key <= t (items sorted by key), or -1.
const lastIndex = <T>(list: T[], t: number, key: (x: T) => number): number => {
	let lo = 0;
	let hi = list.length - 1;
	let ans = -1;
	while (lo <= hi) {
		const mid = (lo + hi) >> 1;
		if (key(list[mid]!) <= t) {
			ans = mid;
			lo = mid + 1;
		} else {
			hi = mid - 1;
		}
	}
	return ans;
};

export type PlayerState = {
	pid: number;
	team: Side;
	shown: boolean;
	x: number;
	y: number;
	z: number;
	face: 1 | -1;
	anim: AnimName;
	frame: number;
	moving: boolean;
};

export const offenseAt = (tl: RetroTimeline, t: number): Side => {
	const i = lastIndex(tl.poss, t, (p) => p[0]);
	return i >= 0 ? tl.poss[i]![1] : 1;
};

const ballSegAt = (tl: RetroTimeline, t: number): BallSeg | undefined => {
	const i = lastIndex(tl.ball, t, (s) => s.t0);
	return i >= 0 ? tl.ball[i] : tl.ball[0];
};

const jumpZ = (act: Act, u: number): number => {
	if (act.zKeys) {
		const k = act.zKeys;
		for (let i = 1; i < k.length; i++) {
			const [u1, z1] = k[i]!;
			if (u <= u1) {
				const [u0, z0] = k[i - 1]!;
				const f = u1 === u0 ? 1 : (u - u0) / (u1 - u0);
				// Up fast and decelerating, down accelerating - a jump, not a lift.
				const g = z1 >= z0 ? Math.sin((f * Math.PI) / 2) : f * f;
				return z0 + (z1 - z0) * g;
			}
		}
		return 0;
	}
	if (!act.jump) {
		return 0;
	}
	const [a, b, peak] = act.jump;
	const v = (u - a) / (b - a);
	return v <= 0 || v >= 1 ? 0 : 4 * peak * v * (1 - v);
};

export const evalPlayer = (
	tl: RetroTimeline,
	pid: number,
	t: number,
): PlayerState => {
	const tr = tl.tracks.get(pid);
	if (!tr) {
		return {
			pid,
			team: 0,
			shown: false,
			x: 0,
			y: 0,
			z: 0,
			face: 1,
			anim: "ready",
			frame: 0,
			moving: false,
		};
	}
	const si = lastIndex(tr.shown, t, (s) => s[0]);
	const shown = si >= 0 ? tr.shown[si]![1] : false;

	const mi = lastIndex(tr.moves, t, (m) => m.t0);
	const mv = mi >= 0 ? tr.moves[mi] : undefined;
	let x = tr.start.x;
	let y = tr.start.y;
	let moving = false;
	let traveled = 0;
	if (mv) {
		if (t < mv.t1) {
			const e = ease((t - mv.t0) / (mv.t1 - mv.t0));
			x = mv.from.x + (mv.to.x - mv.from.x) * e;
			y = mv.from.y + (mv.to.y - mv.from.y) * e;
			moving = true;
			traveled = Math.hypot(mv.to.x - mv.from.x, mv.to.y - mv.from.y) * e;
		} else {
			x = mv.to.x;
			y = mv.to.y;
		}
	}

	const fi = lastIndex(tr.faces, t, (f) => f[0]);
	const face = fi >= 0 ? tr.faces[fi]![1] : 1;

	// The act running now, if any (they rarely overlap; the later one wins).
	let act: Act | undefined;
	const ai = lastIndex(tr.acts, t, (a) => a.t0);
	for (let k = ai; k >= 0 && k >= ai - 3; k--) {
		const a = tr.acts[k]!;
		if (t < a.t1) {
			act = a;
			break;
		}
	}

	let anim: AnimName;
	let frame: number;
	let z = 0;
	if (act) {
		const u = (t - act.t0) / (act.t1 - act.t0);
		anim = act.anim;
		frame =
			ANIMS[anim].kind === "act"
				? actFrame(anim, u)
				: loopFrame(anim, t - act.t0);
		z = jumpZ(act, u);
	} else if (moving && mv) {
		anim = mv.anim;
		frame = cycleFrame(anim, traveled);
	} else {
		const seg = ballSegAt(tl, t);
		if (seg && seg.kind === "hold" && seg.pid === pid) {
			anim = seg.style === "dribble" ? "dribbleIdle" : "hold";
		} else {
			anim = offenseAt(tl, t) === tr.team ? "ready" : "stance";
		}
		frame = loopFrame(anim, t, pid * 0.37);
	}
	return { pid, team: tr.team, shown, x, y, z, face, anim, frame, moving };
};

// A hand, in world feet, from the sprite's own skeleton - so the ball sits in
// the hands the sprite actually draws.
export const handWorld = (
	st: PlayerState,
	body: Body,
	which: "near" | "both" = "both",
): Pt3 => {
	const sk = skeleton(body, poseFor(st.anim, st.frame));
	const h =
		which === "near"
			? sk.armN.hand
			: {
					x: (sk.armN.hand.x + sk.armF.hand.x) / 2,
					y: (sk.armN.hand.y + sk.armF.hand.y) / 2,
				};
	return {
		x: st.x + (h.x * st.face) / (K * persp(st.y)),
		y: st.y + 0.05,
		z: st.z + h.y / PX_PER_FT,
	};
};

export type BallState = { x: number; y: number; z: number; holder?: number };

export const evalBall = (
	tl: RetroTimeline,
	t: number,
	bodyFor: (pid: number) => Body,
): BallState => {
	const seg = ballSegAt(tl, t);
	if (!seg) {
		return { x: 47, y: 25, z: 0 };
	}
	const handOf = (pid: number, at: number, which: "near" | "both") =>
		handWorld(evalPlayer(tl, pid, at), bodyFor(pid), which);
	const resolve = (
		p: Pt3 | { pid: number; hand?: "near" | "both" },
		at: number,
	): Pt3 => ("pid" in p ? handOf(p.pid, at, p.hand ?? "both") : p);

	if (seg.kind === "hold") {
		const st = evalPlayer(tl, seg.pid, t);
		if (seg.style === "dribble") {
			const h = handWorld(st, bodyFor(seg.pid), "near");
			const ph = (((t - seg.t0) / 1000) * (st.moving ? 2.4 : 1.9)) % 1;
			const tri = 1 - Math.abs(2 * ph - 1);
			const fx = st.x + st.face * 1.3;
			const fy = st.y + 0.6;
			return {
				x: h.x + (fx - h.x) * tri,
				y: h.y + (fy - h.y) * tri,
				z: h.z * (1 - tri),
				holder: seg.pid,
			};
		}
		return { ...handWorld(st, bodyFor(seg.pid), "both"), holder: seg.pid };
	}
	if (seg.kind === "fly") {
		const a = resolve(seg.from, seg.t0);
		const b = resolve(seg.to, seg.t1);
		const u = clamp01((t - seg.t0) / (seg.t1 - seg.t0));
		const arc = Math.max(0, seg.peak - (a.z + b.z) / 2);
		return {
			x: a.x + (b.x - a.x) * u,
			y: a.y + (b.y - a.y) * u,
			z: a.z + (b.z - a.z) * u + 4 * arc * u * (1 - u),
		};
	}
	if (seg.kind === "bounce") {
		// A drop from where it was, then shrinking hops, then a roll.
		const u = clamp01((t - seg.t0) / (seg.t1 - seg.t0));
		const hops = [Math.sqrt(Math.max(0.02, seg.from.z))];
		for (let i = 0; i < seg.hops; i++) {
			hops.push(2 * Math.sqrt(seg.h0 * 0.42 ** i));
		}
		const total = hops.reduce((s, h) => s + h, 0) / 0.85;
		let w = u * total;
		let z = 0;
		for (let i = 0; i < hops.length; i++) {
			if (w <= hops[i]!) {
				const v = w / hops[i]!;
				z =
					i === 0
						? seg.from.z * (1 - v * v)
						: 4 * seg.h0 * 0.42 ** (i - 1) * v * (1 - v);
				break;
			}
			w -= hops[i]!;
		}
		const roll = 1 - (1 - Math.min(1, u / 0.85)) ** 1.5;
		return {
			x: seg.from.x + (seg.to.x - seg.from.x) * roll,
			y: seg.from.y + (seg.to.y - seg.from.y) * roll,
			z: Math.max(z, u >= 1 ? 0.4 : 0),
		};
	}
	return { ...seg.at };
};

// The most recent effect of a kind (optionally on one rim) within `windowMs`.
export const recentFx = (
	tl: RetroTimeline,
	t: number,
	kinds: FxKind[],
	windowMs: number,
	rim?: Side,
): Fx | undefined => {
	const i = lastIndex(tl.fx, t, (f) => f.t);
	for (let k = i; k >= 0; k--) {
		const f = tl.fx[k]!;
		if (t - f.t > windowMs) {
			break;
		}
		if (kinds.includes(f.kind) && (rim === undefined || f.rim === rim)) {
			return f;
		}
	}
	return undefined;
};
