import {
	ANIMS,
	legPhases,
	stepBeat,
	stepLength,
	type AnimName,
} from "./poses.ts";

// HIS STRIDES THROUGH A STRETCH OF THEM, any sport's - worked out step by
// step along it (every STEP_MS), once a stretch: how far round them he is,
// and how long each leg's stride is (see poses.ts shortSteps). A foot down
// keeps the stride it came down on, so it stays where it is on the ground
// as he picks up pace or slows; his strides go round as fast as that foot
// goes back under him; and a foot up goes over to a stride for the pace he
// will be going as it comes down - looking ahead to it, so pulling up his
// last steps come down short, not long and then stuck under him while he
// slows; and getting going, each step a little longer than the last. (Both
// feet off the ground, his strides go round as they did - or, slowing, at
// the beat.) A sport carries them on from the stretch before, where he goes
// straight on from it (see the court's steppingOf).

// The stretch: from s0 to s1 (ms), how far along it he is at t (feet - on
// past the end at the pace he ends on), and how fast he is going at t (feet
// a second).
export type Stretch = {
	s0: number;
	s1: number;
	along: (t: number) => number;
	pace: (t: number) => number;
};

export const STEP_MS = 10;
// The shortest stride he takes (of a full one), and the quickest his
// strides go round (a second).
export const LEN_MIN = 0.1;
const BEAT_MOST = 2.6;
// (Looking no further ahead than this, ms.)
const LAND_AHEAD_MS = 300;
// (Getting going, each step no more than this much longer than the other
// foot's - his strides lengthen a step at a time, quicker meanwhile.)
const STEP_GROW = 2.5;
// (A foot's stride changes no quicker than this - so many full ones a
// second.)
const LEN_RATE = 10;
// A slide's push steps go round no quicker as he goes faster than
// REACH_FROM (feet a second): each one spreads his feet further instead, up
// to REACH_MOST times as far. (His stride's that long then - its "length",
// though more than a full one.)
const REACH_FROM = 6.5;
const REACH_MOST = 1.45;
export const reachOf = (v: number) =>
	Math.min(REACH_MOST, Math.max(1, v / REACH_FROM));
// (And goes over to a new spread no quicker than this - so many full
// strides' worth a second: going on into a run at another pace, his feet
// spread wider or closer over a step, not in a frame.)
const REACH_RATE = 2;
// Setting off, he is a quarter of a stride in: over the foot he pushes off
// with, the other coming through under him - nearest how he stood, so his
// legs ease out of that into the stride with his feet staying where they
// are, not splayed apart across the floor in a frame or two.
export const STRIDE_START = 0.25;

export type Stepping = {
	s0: number;
	s1: number;
	ph: number[];
	r: number[];
	l: number[];
	// Each leg at the end: its stride, and whether its foot is up.
	legR: StepLeg;
	legL: StepLeg;
};
// (Up, the stride it goes over from toward the next - the one it left the
// floor on - and from how far through its swing: from the start of it, or
// from where it was as the stretch before ended.)
export type StepLeg = { len: number; up?: number; from?: number };

// How long a full stride of a gait is (feet).
export const strideOf = (anim: AnimName): number => {
	const a = ANIMS[anim];
	return a.kind === "cycle" ? a.stride : 5;
};

const frac = (x: number) => x - Math.floor(x);
const clamp01 = (u: number) => Math.min(1, Math.max(0, u));
const smooth01 = (u: number) => {
	const v = clamp01(u);
	return v * v * (3 - 2 * v);
};

// The stride for a foot coming down at t: for his pace over the time it
// will be down (at the beat) - so pulling up he steps short and his strides
// keep the beat as he slows.
const pacing = (anim: AnimName, st: Stretch) => {
	const beat = stepBeat(anim);
	const duty = legPhases(anim, 0)?.duty ?? 0.5;
	const DOWN_MS = beat > 0 ? (duty * 1000) / beat : 0;
	const over = (t: number, ms = DOWN_MS) =>
		ms > 0 ? ((st.along(t + ms) - st.along(t)) * 1000) / ms : st.pace(t);
	const lenAt = (t: number) => Math.max(LEN_MIN, stepLength(anim, over(t)));
	return { beat, over, lenAt };
};

// Setting off fresh: a quarter of a stride in, each foot's stride for his
// pace while the foot he pushes off on is still down - only a moment more.
// (Steps that keep no beat - a shuffle, say - full ones throughout. A foot
// up as he sets off goes over from there, from where it is through its
// swing.)
export const freshStart = (
	anim: AnimName,
	st: Stretch,
): { ph: number; R: StepLeg; L: StepLeg } => {
	const { beat, over } = pacing(anim, st);
	const ph = STRIDE_START;
	const first = legPhases(anim, ph);
	const firstMs =
		first && beat > 0
			? (Math.max(
					0,
					...[first.r, first.l]
						.filter((phi) => phi < first.duty)
						.map((phi) => (first.back ? phi : first.duty - phi)),
				) *
					1000) /
				beat
			: 0;
	const R: StepLeg = {
		len: first ? Math.max(LEN_MIN, stepLength(anim, over(st.s0, firstMs))) : 1,
		from: -1,
	};
	return { ph, R, L: { ...R } };
};

// Through the stretch from where his strides are as it starts (`ph`, each
// leg's `R`, `L`).
export const stepThrough = (
	anim: AnimName,
	st: Stretch,
	ph0: number,
	R0: StepLeg,
	L0: StepLeg,
): Stepping => {
	const { lenAt } = pacing(anim, st);
	const S = strideOf(anim);
	let ph = ph0;
	let R = R0;
	let L = L0;
	if (anim === "shuffle" && R.from === -1) {
		R = { len: reachOf(st.pace(st.s0)) };
		L = { ...R };
	}
	const n = Math.max(1, Math.ceil((st.s1 - st.s0) / STEP_MS));
	const dt = (st.s1 - st.s0) / n;
	const out: Stepping = {
		s0: st.s0,
		s1: st.s1,
		ph: [ph],
		r: [R.len],
		l: [L.len],
		legR: R,
		legL: L,
	};
	for (let i = 1; i <= n; i++) {
		const u = st.s0 + (i - 0.5) * dt;
		const v = st.pace(u);
		const was = legPhases(anim, ph);
		if (anim === "shuffle") {
			// (Through the push, the foot he pushes off stays where it is as
			// his feet spread further, and the other through the close - so
			// his strides go round that much slower or quicker for it.)
			const most = (REACH_RATE * dt) / 1000;
			const reach = R.len + Math.max(-most, Math.min(most, reachOf(v) - R.len));
			const f = frac(ph);
			const spread = f < 0.5 ? 2 * f : 2 - 2 * f;
			const grew = (reach - R.len) * (f < 0.5 ? 1 : -1);
			ph += Math.max(
				0,
				((v / (S * reach)) * dt) / 1000 - (grew * spread) / (2 * reach),
			);
			R = { len: reach };
			L = R;
		} else if (!was) {
			ph += ((v / S) * dt) / 1000;
		} else {
			// (Both feet up, at the stride of the one that left the floor last
			// - or, slowing, his pace's.)
			const down =
				R.up === undefined && L.up === undefined
					? (R.len + L.len) / 2
					: R.up === undefined
						? R.len
						: L.up === undefined
							? L.len
							: Math.min(
									(was.back ? was.r > was.l : was.r < was.l) ? R.up : L.up,
									lenAt(u),
								);
			const rate = Math.min(BEAT_MOST, v / (S * down));
			ph += (rate * dt) / 1000;
			const now = legPhases(anim, ph)!;
			// Each foot: up off the floor once it is through its time down;
			// then over from the stride it left the floor on to the one for
			// when it comes down - so far ahead as it has yet to swing, at the
			// rate it is swinging. (Never more than so much longer than the
			// other foot's stride, as it is now - not as it was as that foot
			// left the floor, or the cap would jump as it came down.)
			const step = (leg: StepLeg, phi: number, other: StepLeg): StepLeg => {
				if (phi < now.duty) {
					return leg.up === undefined ? leg : { len: leg.len };
				}
				const through = Math.min(
					1,
					(now.back ? 1 - phi : phi - now.duty) / Math.max(1e-6, 1 - now.duty),
				);
				const up = leg.up ?? leg.len;
				const from =
					leg.from === -1
						? through
						: leg.up === undefined
							? 0
							: (leg.from ?? 0);
				const ahead = Math.min(
					LAND_AHEAD_MS,
					((1 - through) * (1 - now.duty) * 1000) / Math.max(rate, 0.5),
				);
				const to = Math.min(lenAt(u + ahead), other.len * STEP_GROW);
				const s0 = smooth01(from);
				const share =
					s0 >= 1 ? 1 : clamp01((smooth01(through) - s0) / (1 - s0));
				// (And no quicker than LEN_RATE: pulling up late in a swing,
				// the step it comes down on shortened over the last of it, not
				// snapped short in a frame.)
				const most = (LEN_RATE * dt) / 1000;
				const len =
					leg.len +
					Math.max(-most, Math.min(most, up + (to - up) * share - leg.len));
				return { len, up, from };
			};
			const r = step(R, now.r, L);
			L = step(L, now.l, R);
			R = r;
		}
		out.ph.push(ph);
		out.r.push(R.len);
		out.l.push(L.len);
	}
	out.legR = R;
	out.legL = L;
	return out;
};

// How far round his strides he is at t, and how long each leg's stride is.
export const stridesThen = (
	st: Stepping,
	t: number,
): { ph: number; len: [number, number] } => {
	const n = st.ph.length - 1;
	const f =
		st.s1 > st.s0
			? ((Math.min(st.s1, Math.max(st.s0, t)) - st.s0) * n) / (st.s1 - st.s0)
			: n;
	const i = Math.max(0, Math.min(n - 1, Math.floor(f)));
	const u = n > 0 ? f - i : 0;
	const at = (a: number[]) => (n > 0 ? a[i]! + (a[i + 1]! - a[i]!) * u : a[0]!);
	return { ph: at(st.ph), len: [at(st.r), at(st.l)] };
};
