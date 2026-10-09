import { generate, type FaceConfig } from "facesjs";
import { makeCourtRng } from "../courtRng.ts";
import type { BallEnd, CourtTimeline, Fx } from "./director.ts";
import {
	arenaShotAt,
	cameraCuts,
	evalPlayer,
	lastCut,
	offenseAt,
	recentFx,
	tensionAt,
	type PlayerState,
} from "./evaluate.ts";
import { shade, type Look } from "./figure.ts";
import {
	attackDir,
	benchX,
	COURT_H,
	COURT_W,
	FT_LINE_DEPTH,
	FT_OFFICIAL,
	FT_SHOOTER_DEPTH,
	ftOfficialBall,
	other,
	RIM_Z,
	spot,
	type Pt,
	type Pt3,
	type Side,
} from "./geometry.ts";
import { playAt } from "./physics.ts";
import { ANIMS, type AnimName } from "./poses.ts";

// THE PEOPLE ON THE FLOOR WHO DON'T PLAY: three officials working the game,
// each team's head coach in front of his bench, and the photographers down
// the baselines.
//
// None of them is in the play-by-play. Like the crowd, they are staged from
// the game as the director wrote it: the officials follow the ball and
// signal what each whistle was for, the coaches pace their boxes and react
// to the plays, the photographers shoot what happens at their end. All of it
// is read off the timeline at any moment, so a replay plays out the same.

export type CrewRole = "ref" | "coach" | "photo";
export type CrewMember = {
	// Never a player's: crew are numbered below zero.
	pid: number;
	role: CrewRole;
	// A coach's team; a photographer's end of the floor (0 the left).
	team: Side;
	// A photographer's place on the baseline.
	spot?: Pt;
	hgt: number;
	weight: number;
	face: FaceConfig;
	// What he wears (the head comes from his face; see Court3D).
	dress: Pick<Look, "kit" | "gear" | "outfit">;
};

type TeamLike = {
	region?: string;
	name?: string;
	abbrev?: string;
	colors?: [string, string, string];
};

// faces.js draws on Math.random; seeded here, so the same people turn up
// every time the game is shown. Officials and coaches wear nothing on their
// heads - no caps, no headbands.
const faceFor = (seed: string, female: boolean): FaceConfig => {
	const real = Math.random;
	Math.random = makeCourtRng(`face|${seed}`);
	try {
		const face = generate(undefined, { gender: female ? "female" : "male" });
		return { ...face, accessories: { ...face.accessories, id: "none" } };
	} finally {
		Math.random = real;
	}
};

// Clothes, not a uniform: a top, trousers (tights under shorts of the same
// color), shoes.
const dress = (
	top: string,
	pants: string,
	outfit: Look["outfit"],
	shoe = "#141416",
	trim = top,
): CrewMember["dress"] => ({
	kit: {
		jersey: top,
		trim,
		number: top,
		numberEdge: top,
		shorts: pants,
		stripe: pants,
		sock: pants,
		shoe,
		sole: shoe,
	},
	gear: {
		tights: { legs: "RL", color: pants },
		shoe,
		sole: shade(shoe, shoe === "#141416" ? 0.25 : -0.25),
		sock: pants,
	},
	outfit,
});

const REF_DRESS = dress("#f2f2ef", "#16161a", {
	sleeves: "short",
	stripes: "#16161a",
});
const SUITS = ["#1d2333", "#24262c", "#151517", "#3a3f4a", "#2f2924"];
const PHOTO_TOPS = ["#18181c", "#262a33", "#3a3a3d", "#1f2a24", "#2d2230"];
const PHOTO_PANTS = ["#18181a", "#4b4334", "#2b2e35", "#3c3f46"];

const teamKey = (t: TeamLike | undefined) =>
	`${t?.region ?? ""}|${t?.name ?? t?.abbrev ?? ""}`;

// Who works the game: the officials (three from the league's pool of them,
// so the same faces come round), each side's head coach (his own, every
// game), and the home building's photographers.
export const crewFor = (
	gid: number,
	away: TeamLike | undefined,
	home: TeamLike | undefined,
): CrewMember[] => {
	const out: CrewMember[] = [];
	const rng = makeCourtRng(`crew|${gid}`);
	const picked: number[] = [];
	while (picked.length < 3) {
		const n = Math.floor(rng() * 40);
		if (!picked.includes(n)) {
			picked.push(n);
		}
	}
	picked.forEach((n, i) => {
		const r = makeCourtRng(`ref|${n}`);
		out.push({
			pid: -1 - i,
			role: "ref",
			team: 0,
			hgt: 71 + r() * 6,
			weight: 185 + r() * 45,
			face: faceFor(`ref|${n}`, r() < 0.12),
			dress: REF_DRESS,
		});
	});
	([away, home] as const).forEach((team, t) => {
		const key = teamKey(team);
		const r = makeCourtRng(`coach|${key}`);
		const c0 = team?.colors?.[0] ?? "#2d3a55";
		const c1 = team?.colors?.[1] ?? "#c9b26b";
		let clothes: CrewMember["dress"];
		if (r() < 0.25) {
			// A team quarter-zip and dark slacks.
			clothes = dress(
				shade(c0, -0.12),
				"#23252b",
				{ sleeves: "long" },
				"#141416",
				c1,
			);
		} else {
			const suit = SUITS[Math.floor(r() * SUITS.length)]!;
			clothes = dress(suit, suit, {
				sleeves: "long",
				shirt: r() < 0.7 ? "#f2f2ee" : "#cfdcef",
				tie: r() < 0.6 ? c0 : c1,
			});
		}
		out.push({
			pid: -11 - t,
			role: "coach",
			team: t as Side,
			hgt: 70 + r() * 8,
			weight: 180 + r() * 60,
			face: faceFor(`coach|${key}`, r() < 0.12),
			dress: clothes,
		});
	});
	const pr = makeCourtRng(`photo|${teamKey(home)}|${gid}`);
	let n = 0;
	for (const end of [0, 1] as const) {
		for (const y of [5.5, 9.5, 13.5, 36.5, 40.5, 44.5]) {
			if (pr() > 0.72) {
				continue;
			}
			const seed = `photo|${teamKey(home)}|${gid}|${n}`;
			const r = makeCourtRng(seed);
			out.push({
				pid: -21 - n,
				role: "photo",
				team: end,
				spot: {
					x: end === 0 ? -3.1 - r() * 0.8 : COURT_W + 3.1 + r() * 0.8,
					y: y + (r() - 0.5) * 1.2,
				},
				hgt: 66 + r() * 8,
				weight: 150 + r() * 70,
				face: faceFor(seed, r() < 0.3),
				dress: dress(
					PHOTO_TOPS[Math.floor(r() * PHOTO_TOPS.length)]!,
					PHOTO_PANTS[Math.floor(r() * PHOTO_PANTS.length)]!,
					{ sleeves: r() < 0.5 ? "short" : "long", camera: true },
					r() < 0.5 ? "#141416" : "#e6e6e2",
				),
			});
			n += 1;
		}
	}
	return out;
};

// ---- reading the game ----------------------------------------------------------

const clamp = (v: number, lo: number, hi: number) =>
	Math.min(hi, Math.max(lo, v));
const lerp = (a: Pt, b: Pt, u: number): Pt => ({
	x: a.x + (b.x - a.x) * u,
	y: a.y + (b.y - a.y) * u,
});
const dist = (a: Pt, b: Pt) => Math.hypot(a.x - b.x, a.y - b.y);

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

// A cheap hash to [0, 1), so a choice made at a moment is made the same
// every time.
const unit = (a: number, b: number): number => {
	let h =
		Math.imul((a | 0) ^ 0x9e3779b9, 0x85ebca6b) ^ Math.imul(b + 1, 0xc2b2ae35);
	h ^= h >>> 15;
	h = Math.imul(h, 0x2c1b3c6d);
	h ^= h >>> 12;
	return (h >>> 0) / 2 ** 32;
};

// Where the ball is on the floor at t, near enough for watching it: in
// someone's hands, in the air between two places, bouncing, lying still.
const ballSpot = (tl: CourtTimeline, t: number): Pt => {
	const i = lastIndex(tl.ball, t, (s) => s.t0);
	const seg = tl.ball[Math.max(0, i)];
	if (!seg) {
		return { x: COURT_W / 2, y: COURT_H / 2 };
	}
	const end = (e: BallEnd, at: number): Pt => {
		if ("pid" in e) {
			const p = evalPlayer(tl, e.pid, at);
			return { x: p.x, y: p.y };
		}
		return { x: e.x, y: e.y };
	};
	switch (seg.kind) {
		case "hold": {
			const p = evalPlayer(tl, seg.pid, t);
			return { x: p.x, y: p.y };
		}
		case "rest":
			return { x: seg.at.x, y: seg.at.y };
		case "fly":
			return lerp(
				end(seg.from, seg.t0),
				end(seg.to, seg.t1),
				clamp((t - seg.t0) / Math.max(1, seg.t1 - seg.t0), 0, 1),
			);
		case "bounce":
			return lerp(
				seg.from,
				seg.to,
				clamp((t - seg.t0) / Math.max(1, seg.t1 - seg.t0), 0, 1),
			);
		case "path":
			return playAt(seg.pts, t - seg.t0);
	}
};

// Where the last shot went up from, before t: the start of the ball's last
// flight at a rim.
const shotFrom = (tl: CourtTimeline, t: number): Pt | undefined => {
	for (let i = lastIndex(tl.ball, t, (s) => s.t0); i >= 0; i--) {
		const seg = tl.ball[i]!;
		if (t - seg.t0 > 4000) {
			break;
		}
		if (
			seg.kind === "fly" &&
			!("pid" in seg.to) &&
			(tl.ball[i + 1]?.kind === "path" || Math.abs(seg.to.z - RIM_Z) < 1.6)
		) {
			return "pid" in seg.from
				? (() => {
						const p = evalPlayer(tl, seg.from.pid, seg.t0);
						return { x: p.x, y: p.y };
					})()
				: { x: seg.from.x, y: seg.from.y };
		}
	}
	return undefined;
};

const beatAt = (tl: CourtTimeline, t: number) => {
	const i = lastIndex(tl.beats, t, (b) => b.preStart);
	return i >= 0 ? tl.beats[i] : undefined;
};

// The first fx of a kind at or after t (within a few seconds).
const nextFx = (
	tl: CourtTimeline,
	t: number,
	kind: Fx["kind"],
): Fx | undefined => {
	for (
		let i = Math.max(
			0,
			lastIndex(tl.fx, t, (f) => f.t),
		);
		i < tl.fx.length;
		i++
	) {
		const f = tl.fx[i]!;
		if (f.t >= t && f.kind === kind) {
			return f;
		}
		if (f.t > t + 8000) {
			break;
		}
	}
	return undefined;
};

// A loop played on the clock.
const loopPhase = (anim: AnimName, t: number, seed: number) => {
	const a = ANIMS[anim];
	return a.kind === "loop" ? (t / 1000) * (a.fps / a.n) + seed : t / 1000;
};

// Facing a point on the floor.
const yawTo = (from: Pt, to: Pt) => Math.atan2(to.y - from.y, to.x - from.x);

// ---- the officials ----------------------------------------------------------------

// Where the official who hands the shooter the ball stands: under the
// basket, in the lane, the ball held out in front of him.
const ftAdminAt = (team: Side): Pt => spot(team, ...FT_OFFICIAL);

// The ball still his at the line: resting in his hands, on its way to him,
// or only just bounced in.
const withOfficial = (tl: CourtTimeline, t: number, team: Side): boolean => {
	const ball = ftOfficialBall(team);
	const near = (p: BallEnd) =>
		!("pid" in p) && Math.hypot(p.x - ball.x, p.y - ball.y) < 0.6;
	const seg = tl.ball[lastIndex(tl.ball, t, (b) => b.t0)];
	if (!seg) {
		return false;
	}
	return (
		(seg.kind === "rest" && near(seg.at)) ||
		(seg.kind === "fly" &&
			(near(seg.to) || (near(seg.from) && t < seg.t0 + 250)))
	);
};

// Whether, since `from`, he has bounced the shooter the ball - and it is
// gone from him.
const handedOver = (
	tl: CourtTimeline,
	t: number,
	team: Side,
	from: number,
): boolean => {
	if (withOfficial(tl, t, team)) {
		return false;
	}
	const ball = ftOfficialBall(team);
	for (let i = lastIndex(tl.ball, t, (b) => b.t0); i >= 0; i--) {
		const seg = tl.ball[i]!;
		if (seg.t0 < from) {
			break;
		}
		if (
			seg.kind === "fly" &&
			!("pid" in seg.from) &&
			Math.hypot(seg.from.x - ball.x, seg.from.y - ball.y) < 0.6
		) {
			return true;
		}
	}
	return false;
};

// The last of a trip to the line.
const lastFreeThrow = (tl: CourtTimeline, t: number): boolean => {
	const i = lastIndex(tl.beats, t, (b) => b.preStart);
	const next = tl.beats[i + 1];
	return !next || (next.type !== "ft" && next.type !== "missFt");
};

// The three of them, by crew position: the lead under the basket the ball is
// going at, the trail behind the play on the far (table) side, the slot on
// the near sideline. Lead and trail trade places every change of possession,
// as a crew does in transition; the third keeps the near side. In a break
// they gather by the table; for a jump ball the third throws it up.
const TOSS = { x: COURT_W / 2, y: COURT_H / 2 - 1.65 };
const refTargets = (tl: CourtTimeline, t: number): [Pt, Pt, Pt] => {
	const beat = beatAt(tl, t);
	const shot = arenaShotAt(tl, t);
	if (shot && shot !== tl.shots[0]) {
		return [
			{ x: COURT_W / 2 - 2.4, y: 3.2 },
			{ x: COURT_W / 2 + 2.4, y: 3.2 },
			{ x: COURT_W / 2, y: 4.5 },
		];
	}
	if (beat?.type === "ft" || beat?.type === "missFt") {
		// Free throws: the lead, under the basket, bounces the shooter the
		// ball from the lane - and once it is in his hands, steps out to the
		// end line, out of the lane before the shot: for good after the last
		// of them, otherwise back in for the ball as the shot comes down. The
		// trail stands out by the arc on the table side, the slot across from
		// the line.
		const team = offenseAt(tl, t);
		const d = attackDir(team);
		const X = (depth: number) => (d > 0 ? COURT_W - depth : depth);
		const lead =
			handedOver(tl, t, team, beat.preStart) &&
			(lastFreeThrow(tl, t) || t < beat.end - 600)
				? { x: X(-1.3), y: 36.5 }
				: ftAdminAt(team);
		const trail = { x: X(29), y: 3.2 };
		const slot = { x: X(FT_LINE_DEPTH), y: COURT_H + 1.2 };
		const k = lastIndex(tl.poss, t, (p) => p[0]);
		return k % 2 === 0 ? [lead, trail, slot] : [trail, lead, slot];
	}
	if (beat?.type === "jumpBall") {
		// (The ball up out of his hands, he backs out of there.)
		const toss = nextFx(tl, beat.preStart, "toss");
		if (toss && t < toss.t + 150) {
			return [
				{ x: COURT_W / 2 - 7, y: 0.9 },
				{ x: COURT_W / 2 + 7, y: COURT_H + 1.2 },
				TOSS,
			];
		}
	}
	const team = offenseAt(tl, t);
	const d = attackDir(team);
	const X = (depth: number) => (d > 0 ? COURT_W - depth : depth);
	const b = ballSpot(tl, t);
	const bd = d > 0 ? COURT_W - b.x : b.x;
	const lead = { x: X(-1.4), y: 25 + clamp((b.y - 25) * 0.7, -13, 13) };
	const trail = { x: X(clamp(bd + 12, 26, 54)), y: 0.9 };
	const slot = { x: X(clamp(bd + 2, 17, 47)), y: COURT_H + 1.2 };
	const k = lastIndex(tl.poss, t, (p) => p[0]);
	return k % 2 === 0 ? [lead, trail, slot] : [trail, lead, slot];
};

// Where each official is: running after the places the game calls for, as
// fast as an official runs and no faster - even end to end in transition -
// and settled at once where the picture cuts away. Worked out a step at a
// time over the whole game, once, and blended between steps, so the glide
// is smooth at any frame rate.
const STEP = 250;
// How hard he goes after his place (a second), how fast he can run (feet a
// second) and how quickly he gets going (feet a second, a second).
const REF_GAIN = 1.8;
const REF_RUN = 24;
const REF_ACCEL = 20;
type Places = { at: Pt[]; vel: Pt[] };
const refPaths = new WeakMap<CourtTimeline, Pt[][]>();
const refPath = (tl: CourtTimeline): Pt[][] => {
	let out = refPaths.get(tl);
	if (out) {
		return out;
	}
	out = [];
	const cuts = cameraCuts(tl);
	let ci = 0;
	const dt = STEP / 1000;
	let at: Pt[] = refTargets(tl, 0).map((q) => ({ ...q }));
	let vel: Pt[] = at.map(() => ({ x: 0, y: 0 }));
	const n = Math.ceil(tl.end / STEP) + 2;
	for (let k = 0; k < n; k++) {
		const t = k * STEP;
		const goal = refTargets(tl, t);
		let cut = false;
		while (ci < cuts.length && cuts[ci]! <= t) {
			cut ||= cuts[ci]! > t - STEP;
			ci++;
		}
		if (cut) {
			at = goal.map((q) => ({ ...q }));
			vel = at.map(() => ({ x: 0, y: 0 }));
		} else if (k > 0) {
			at = at.map((p, i) => {
				const g = goal[i]!;
				let wx = (g.x - p.x) * REF_GAIN;
				let wy = (g.y - p.y) * REF_GAIN;
				const w = Math.hypot(wx, wy);
				// No faster than he can pull up from by the time he gets there.
				const most = Math.min(
					REF_RUN,
					Math.sqrt(2 * REF_ACCEL * Math.hypot(g.x - p.x, g.y - p.y)),
				);
				if (w > most) {
					wx *= most / w;
					wy *= most / w;
				}
				const v = vel[i]!;
				let dx = wx - v.x;
				let dy = wy - v.y;
				const dv = Math.hypot(dx, dy);
				if (dv > REF_ACCEL * dt) {
					dx *= (REF_ACCEL * dt) / dv;
					dy *= (REF_ACCEL * dt) / dv;
				}
				vel[i] = { x: v.x + dx, y: v.y + dy };
				// Never out past where an official stands, off the floor.
				return {
					x: Math.min(COURT_W + 2.8, Math.max(-2.8, p.x + vel[i]!.x * dt)),
					y: Math.min(COURT_H + 1.8, Math.max(-0.9, p.y + vel[i]!.y * dt)),
				};
			});
		}
		out.push(at);
	}
	refPaths.set(tl, out);
	return out;
};
const refPlaces = (tl: CourtTimeline, t: number): Places => {
	const path = refPath(tl);
	const g = Math.max(0, Math.min(path.length - 2, Math.floor(t / STEP)));
	const a = path[g]!;
	const b = path[g + 1]!;
	let u = Math.max(0, Math.min(1, (t - g * STEP) / STEP));
	// The picture cutting in between: settled at once, not slid there.
	const cut = lastCut(tl, (g + 1) * STEP);
	if (cut > g * STEP) {
		u = t < cut ? 0 : 1;
	}
	return {
		at: a.map((p, i) => lerp(p, b[i]!, u)),
		vel: a.map((p, i) => ({
			x: ((b[i]!.x - p.x) / STEP) * 1000,
			y: ((b[i]!.y - p.y) / STEP) * 1000,
		})),
	};
};

type Signal = {
	ref: number;
	t0: number;
	// Each move of it: the anim, its span, which way he faces.
	moves: { anim: AnimName; t0: number; t1: number; yaw: number }[];
	// Where he stood to make it, and how long he then takes to run back to
	// where the play has him.
	still: Pt;
	back: number;
};

const FACING_US = Math.PI / 2;
// His right arm out toward x's direction `dir`.
const pointing = (dir: number) => (dir > 0 ? -Math.PI / 2 : Math.PI / 2);

// Where an official is while he signals: still where he made the call,
// then running back to his place in the play.
const signalPos = (s: Signal, t: number, liveAt: (when: number) => Pt): Pt => {
	const t1 = s.moves.at(-1)!.t1;
	if (t < t1) {
		return s.still;
	}
	// (Back to where the play will have him once he gets there.)
	return t < t1 + s.back
		? lerp(s.still, liveAt(t1 + s.back), (t - t1) / s.back)
		: liveAt(t);
};

// Everything the officials have had to signal lately: each whistle's call,
// a three going up (an arm raised while it is in the air) and going in
// (both arms).
type Cue = { at: Pt; t0: number; moves: Signal["moves"] };
// (Long enough for the signal and the jog back to his place after it.)
const CUE_MS = 11000;
const cuesBefore = (tl: CourtTimeline, t: number): Cue[] => {
	const out: Cue[] = [];
	for (let i = lastIndex(tl.fx, t, (f) => f.t); i >= 0; i--) {
		const f = tl.fx[i]!;
		if (t - f.t > CUE_MS) {
			break;
		}
		if (f.kind === "whistle" && f.call) {
			const dir = f.team === undefined ? 1 : attackDir(f.team);
			const side = { anim: "signalSide" as const, yaw: pointing(dir) };
			const up = { anim: "signalUp" as const, yaw: FACING_US };
			const plan: [{ anim: AnimName; yaw: number }, number][] =
				f.call === "out"
					? [[side, 1300]]
					: f.call === "travel"
						? [
								[{ anim: "travel", yaw: FACING_US }, 1000],
								[side, 950],
							]
						: f.call === "shootingFoul" || f.call === "stop"
							? [[up, 1100]]
							: [
									[up, 850],
									[side, 950],
								];
			let t0 = f.t;
			out.push({
				at: f.at ?? ballSpot(tl, f.t),
				t0: f.t,
				moves: plan.map(([m, ms]) => {
					const move = { ...m, t0, t1: t0 + ms };
					t0 += ms;
					return move;
				}),
			});
		} else if (f.kind === "roar" && f.what === "three") {
			const from = shotFrom(tl, f.t);
			if (from) {
				out.push({
					at: from,
					t0: f.t,
					moves: [{ anim: "threeUp", t0: f.t, t1: f.t + 1500, yaw: FACING_US }],
				});
			}
		}
	}
	for (let i = lastIndex(tl.beats, t, (b) => b.actionStart); i >= 0; i--) {
		const b = tl.beats[i]!;
		if (t - b.actionStart > CUE_MS) {
			break;
		}
		if (b.type === "fgaTp" || b.type === "fgaTpFake") {
			out.push({
				at: ballSpot(tl, b.actionStart),
				t0: b.actionStart,
				moves: [
					{
						anim: "signalUp",
						t0: b.actionStart,
						t1: b.actionStart + 1000,
						yaw: FACING_US,
					},
				],
			});
		}
	}
	return out.sort((x, y) => x.t0 - y.t0);
};

// What each official is signaling at t, if anything. Each call goes to
// whoever is nearest it then - played through from the last few seconds, so
// one who is still signaling or running back is where he really is.
const signalsAt = (tl: CourtTimeline, t: number): (Signal | undefined)[] => {
	const per: (Signal | undefined)[] = [undefined, undefined, undefined];
	// A cut settles everyone where the play has them.
	const cut = lastCut(tl, t);
	for (const cue of cuesBefore(tl, t)) {
		if (cue.t0 < cut) {
			continue;
		}
		const live = refPlaces(tl, cue.t0).at;
		const pos = live.map((p, i) => {
			const s = per[i];
			return s ? signalPos(s, cue.t0, (when) => refPlaces(tl, when).at[i]!) : p;
		});
		let ref = 0;
		for (let i = 1; i < 3; i++) {
			if (dist(pos[i]!, cue.at) < dist(pos[ref]!, cue.at)) {
				ref = i;
			}
		}
		const still = pos[ref]!;
		const t1 = cue.moves.at(-1)!.t1;
		// As long as it takes him to jog back to where the play will have him
		// by then.
		let back = 250;
		for (let k = 0; k < 3; k++) {
			back = clamp(
				(dist(still, refPlaces(tl, t1 + back).at[ref]!) / 15) * 1000,
				250,
				6500,
			);
		}
		per[ref] = { ref, t0: cue.t0, moves: cue.moves, still, back };
	}
	return per.map((s) => (s && t < s.moves.at(-1)!.t1 + s.back ? s : undefined));
};

// The official at the line with the ball: catching it when it's tossed out
// to him, holding it, bouncing it in to the shooter.
const handingIn = (
	tl: CourtTimeline,
	t: number,
): { anim: AnimName; phase: number } | undefined => {
	const beat = beatAt(tl, t);
	if (beat?.type !== "ft" && beat?.type !== "missFt") {
		return undefined;
	}
	const ball = ftOfficialBall(offenseAt(tl, t));
	const near = (p: BallEnd) =>
		!("pid" in p) && Math.hypot(p.x - ball.x, p.y - ball.y) < 0.6;
	const seg = tl.ball[lastIndex(tl.ball, t, (b) => b.t0)];
	if (!seg) {
		return undefined;
	}
	if (seg.kind === "fly" && near(seg.from)) {
		return t < seg.t0 + 200
			? { anim: "passBounce", phase: (t - seg.t0 + 120) / 300 }
			: undefined;
	}
	if (seg.kind === "fly" && near(seg.to)) {
		return t < seg.t1 + 150
			? { anim: "catch", phase: clamp((t - seg.t1 + 200) / 350, 0, 1) }
			: { anim: "hold", phase: loopPhase("hold", t, 0) };
	}
	if (seg.kind === "rest" && near(seg.at)) {
		return { anim: "hold", phase: loopPhase("hold", t, 0) };
	}
	return undefined;
};

const refStates = (
	tl: CourtTimeline,
	t: number,
	refs: CrewMember[],
	ball: Pt,
): PlayerState[] => {
	const { at, vel } = refPlaces(tl, t);
	const signals = signalsAt(tl, t);
	// The jump ball: the third holds it out between the two of them, then
	// throws it up.
	const beat = beatAt(tl, t);
	const toss =
		beat?.type === "jumpBall" ? nextFx(tl, beat.preStart, "toss") : undefined;
	const handing = handingIn(tl, t);
	const ftTeam =
		beat?.type === "ft" || beat?.type === "missFt"
			? offenseAt(tl, t)
			: undefined;
	return refs.map((m, i) => {
		let p = at[i]!;
		const v = vel[i]!;
		const speed = Math.hypot(v.x, v.y);
		let anim: AnimName = speed > 8 ? "run" : speed > 1.6 ? "walk" : "ready";
		let phase =
			anim === "run"
				? (t / 1000) * 1.6
				: anim === "walk"
					? (t / 1000) * 1.1
					: loopPhase(anim, t, i * 0.37);
		let yaw = speed > 9 ? Math.atan2(v.y, v.x) : yawTo(p, ball);
		if (ftTeam !== undefined && dist(p, ftAdminAt(ftTeam)) < 0.8) {
			// The one with the ball at the line: facing the shooter.
			yaw = yawTo(p, spot(ftTeam, FT_SHOOTER_DEPTH, 25));
			if (handing) {
				({ anim, phase } = handing);
			}
		}
		if (i === 2 && toss && t < toss.t + 600 && dist(p, TOSS) < 0.6) {
			anim = "toss";
			phase = t < toss.t ? 0 : (t - toss.t) / 600;
			yaw = FACING_US;
		}
		const signal = signals[i];
		if (signal) {
			const move = signal.moves.find((s) => t >= s.t0 && t < s.t1);
			const live = p;
			p = signalPos(signal, t, (when) =>
				when === t ? live : refPlaces(tl, when).at[i]!,
			);
			if (move) {
				anim = move.anim;
				const a = ANIMS[anim];
				phase =
					a.kind === "act"
						? (t - move.t0) / (move.t1 - move.t0)
						: loopPhase(anim, t - move.t0, 0);
				yaw = move.yaw;
			} else {
				anim = "run";
				phase = (t / 1000) * 1.6;
				yaw = yawTo(signal.still, live);
			}
		}

		return {
			pid: m.pid,
			team: 0,
			shown: true,
			x: p.x,
			y: p.y,
			z: 0,
			yaw,
			anim,
			phase,
			moving: anim === "run" || anim === "walk",
		};
	});
};

// ---- the coaches --------------------------------------------------------------

// He paces his box - a few steps to a new spot every few seconds - and
// stands there arms folded, bent over watching, or calling something out.
// His team's big plays get a clap; a call against them, sometimes, arms out
// in disbelief. Over a break he is in the huddle.
const PACE_MS = 6500;
const COACH_Y = -2.7;
const IDLE: AnimName[] = ["crossed", "crossed", "crouch", "ready", "talk"];
// A tight finish: bent over watching every play, calling out to his five.
const IDLE_TENSE: AnimName[] = ["crouch", "talk", "crouch", "talk", "point"];
const coachState = (
	tl: CourtTimeline,
	t: number,
	m: CrewMember,
	ball: Pt,
): PlayerState => {
	const team = m.team;
	const home = benchX(team);
	const spotOf = (j: number) => ({
		x: home + (unit(j, team * 97 + 13) - 0.5) * 11,
		y: COACH_Y,
	});
	const shot = arenaShotAt(tl, t);
	const base = {
		pid: m.pid,
		team,
		shown: true,
		z: 0,
	};
	if (shot && shot !== tl.shots[0]) {
		// In the huddle with his five.
		return {
			...base,
			x: home,
			y: -0.5,
			yaw: FACING_US,
			anim: "talk",
			phase: loopPhase("talk", t, team * 0.5),
			moving: false,
		};
	}
	const j = Math.floor(t / PACE_MS);
	const from = spotOf(j - 1);
	const to = spotOf(j);
	const walkMs = (Math.abs(to.x - from.x) / 3.6) * 1000;
	const since = t - j * PACE_MS;
	if (since < walkMs) {
		const p = lerp(from, to, since / walkMs);
		return {
			...base,
			x: p.x,
			y: p.y,
			yaw: to.x > from.x ? 0 : Math.PI,
			anim: "walk",
			phase: (t / 1000) * 0.85,
			moving: true,
		};
	}
	const idle = tensionAt(tl, t) >= 1 ? IDLE_TENSE : IDLE;
	let anim = idle[Math.floor(unit(j, team * 31 + 7) * idle.length)]!;
	let phase = loopPhase(anim, t, team * 0.5);
	const roar = recentFx(tl, t, ["roar"], 1600);
	const w = recentFx(tl, t, ["whistle"], 1600);
	if (roar?.team === team) {
		anim = "clap";
		phase = loopPhase("clap", t - roar.t, 0);
	} else if (
		w?.call &&
		w.call !== "stop" &&
		w.team !== undefined &&
		w.team !== team &&
		unit(Math.round(w.t), team) < 0.45
	) {
		anim = "protest";
		phase = (t - w.t) / 1600;
	}
	return {
		...base,
		x: to.x,
		y: to.y,
		yaw: yawTo(to, ball),
		anim,
		phase,
		moving: false,
	};
};

// ---- the photographers ------------------------------------------------------

// Down on a knee, the camera up when the play is at his end.
const photoState = (
	tl: CourtTimeline,
	t: number,
	m: CrewMember,
	ball: Pt,
): PlayerState => {
	const at = m.spot!;
	const goal = { x: m.team === 0 ? 12 : COURT_W - 12, y: COURT_H / 2 };
	const depth = m.team === 0 ? ball.x : COURT_W - ball.x;
	const anim: AnimName =
		depth < 38 && !arenaShotAt(tl, t) ? "kneelShoot" : "kneel";
	return {
		pid: m.pid,
		team: m.team,
		shown: true,
		x: at.x,
		y: at.y,
		z: 0,
		yaw: yawTo(at, goal),
		anim,
		phase: loopPhase(anim, t, m.pid * 0.31),
		moving: false,
	};
};

// A camera going off at his end after a big play there: a few frames each,
// at moments of their own.
const flashOf = (
	tl: CourtTimeline,
	t: number,
	st: PlayerState,
): Pt3 | undefined => {
	if (st.anim !== "kneelShoot") {
		return undefined;
	}
	const fx = recentFx(tl, t, ["dunk", "roar", "block"], 1600);
	if (!fx) {
		return undefined;
	}
	// A dunk names its rim; a roar's team attacks the rim at its end, a
	// block's defends it.
	const end =
		fx.kind === "dunk"
			? fx.rim
			: fx.kind === "block"
				? fx.team === undefined
					? undefined
					: other(fx.team)
				: fx.team;
	if (end !== st.team) {
		return undefined;
	}
	for (let k = 0; k < 3; k++) {
		const fire = 80 + unit(Math.round(fx.t) + k * 7, -st.pid) * 1300;
		const age = t - fx.t - fire;
		if (age >= 0 && age < 90) {
			return {
				x: st.x + Math.cos(st.yaw) * 0.9,
				y: st.y + Math.sin(st.yaw) * 0.9,
				z: 3.1,
			};
		}
	}
	return undefined;
};

// Everyone working the game, at t: where they are and what they are doing,
// and any camera flashing.
export const crewAt = (
	tl: CourtTimeline,
	t: number,
	crew: CrewMember[],
): { states: PlayerState[]; flashes: Pt3[] } => {
	const ball = ballSpot(tl, t);
	const states: PlayerState[] = [];
	const refs = crew.filter((m) => m.role === "ref");
	if (refs.length > 0) {
		states.push(...refStates(tl, t, refs, ball));
	}
	const flashes: Pt3[] = [];
	for (const m of crew) {
		if (m.role === "coach") {
			states.push(coachState(tl, t, m, ball));
		} else if (m.role === "photo") {
			const st = photoState(tl, t, m, ball);
			states.push(st);
			const f = flashOf(tl, t, st);
			if (f) {
				flashes.push(f);
			}
		}
	}
	return { states, flashes };
};
