import { generate, type FaceConfig } from "facesjs";
import { makeCourtRng } from "../courtRng.ts";
import type { BallEnd, CourtTimeline, Fx } from "./director.ts";
import {
	arenaShotAt,
	ballBearing,
	cameraCuts,
	evalPlayer,
	floorSpotOf,
	lastCut,
	offenseAt,
	recentFx,
	tensionAt,
	type Blend,
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
	HUDDLE_Y,
	other,
	RIM_Z,
	rimX,
	spot,
	type Pt,
	type Pt3,
	type Side,
} from "./geometry.ts";
import { playAt } from "./physics.ts";
import { callAt, coachSpot } from "./intro.ts";
import { ANIMS, HUG_APART, HUG_MS, HUG_SIDE, type AnimName } from "./poses.ts";
import { arenaSpecFor, type ArenaSpec } from "./arenaSpec.ts";

// THE PEOPLE ON THE FLOOR WHO DON'T PLAY: three officials working the game,
// each team's head coach in front of his bench, and the photographers down
// the baselines.
//
// None of them is in the play-by-play. Like the crowd, they are staged from
// the game as the director wrote it: the officials follow the ball and
// signal what each whistle was for, the coaches pace their boxes and react
// to the plays, the photographers shoot what happens at their end. All of it
// is read off the timeline at any moment, so a replay plays out the same.

export type CrewRole =
	| "ref"
	| "coach"
	| "photo"
	| "table"
	| "media"
	| "mop"
	| "staff"
	| "dance";
export type CrewMember = {
	// Never a player's: crew are numbered below zero.
	pid: number;
	role: CrewRole;
	// A coach's team; a photographer's end of the floor (0 the left).
	team: Side;
	// A photographer's place on the baseline; a chair at the scorer's table;
	// a dancer's in the routine.
	spot?: Pt;
	// A dancer: of the timeouts, how many the dance team comes out for.
	share?: number;
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

// An NBA official's: a gray shirt, faint stripes down it, black sleeves and
// a black panel down each side; black trousers. (Or the old stripes: see
// ArenaSpec.officials.)
const REF_DRESS = dress("#a9adb4", "#16161a", {
	sleeves: "short",
	stripes: "#b6bac1",
	sleeveColor: "#1b1c21",
	sides: "#1b1c21",
});
const REF_STRIPES = dress("#f2f2ef", "#16161a", {
	sleeves: "short",
	stripes: "#16161a",
});
const SUITS = ["#1d2333", "#24262c", "#151517", "#3a3f4a", "#2f2924"];
const PHOTO_TOPS = ["#18181c", "#262a33", "#3a3a3d", "#1f2a24", "#2d2230"];
const PHOTO_PANTS = ["#18181a", "#4b4334", "#2b2e35", "#3c3f46"];

const TABLE_TOPS = ["#16181d", "#22252c", "#1b2233", "#2a2a2e"];
const TABLE_SEATS = [40.5, 45, 49.5, 54];
export const TABLE_SEAT_Y = -8.1;
// The people behind the scorer's table are drawn before it, which hides
// them from the waist down.
export const atTable = (pid: number): boolean => pid <= -41 && pid > -50;

const teamKey = (t: TeamLike | undefined) =>
	`${t?.region ?? ""}|${t?.name ?? t?.abbrev ?? ""}`;

// Who works the game: the officials (three from the league's pool of them,
// so the same faces come round), each side's head coach (his own, every
// game), and the home building's photographers.
export const crewFor = (
	gid: number,
	away: TeamLike | undefined,
	home: TeamLike | undefined,
	spec: ArenaSpec = arenaSpecFor(home),
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
			dress: spec.officials === "stripes" ? REF_STRIPES : REF_DRESS,
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
	// At the scorer's table: the official scorer, the clock and the shot
	// clock, the announcer - in the league's dark polos and jackets.
	const tr = makeCourtRng(`table|${teamKey(home)}`);
	TABLE_SEATS.forEach((x, n) => {
		const seed = `table|${teamKey(home)}|${n}`;
		const r = makeCourtRng(seed);
		out.push({
			pid: -41 - n,
			role: "table",
			team: 0,
			spot: { x: x + (tr() - 0.5) * 0.4, y: TABLE_SEAT_Y },
			hgt: 64 + r() * 10,
			weight: 140 + r() * 80,
			face: faceFor(seed, r() < 0.35),
			dress: dress(
				TABLE_TOPS[Math.floor(r() * TABLE_TOPS.length)]!,
				"#1c1c20",
				r() < 0.5
					? { sleeves: "short" }
					: { sleeves: "long", shirt: "#f2f2ee" },
			),
		});
	});
	// The pre-game show, out on the floor in front of the table through the
	// warm-ups: the host with the microphone, the analyst beside him, and
	// their camera.
	const mr = makeCourtRng(`media|${teamKey(home)}|${gid}`);
	MEDIA.forEach((m, n) => {
		const seed = `media|${teamKey(home)}|${gid}|${n}`;
		const r = makeCourtRng(seed);
		const suit = SUITS[Math.floor(mr() * SUITS.length)]!;
		out.push({
			pid: -51 - n,
			role: "media",
			team: 0,
			spot: m.at,
			hgt: 66 + r() * 10,
			weight: 150 + r() * 60,
			face: faceFor(seed, r() < 0.4),
			dress:
				m.anim === "shootStand"
					? dress(
							PHOTO_TOPS[Math.floor(r() * PHOTO_TOPS.length)]!,
							PHOTO_PANTS[Math.floor(r() * PHOTO_PANTS.length)]!,
							{ sleeves: "short", camera: true },
						)
					: dress(suit, suit, {
							sleeves: "long",
							shirt: r() < 0.7 ? "#f2f2ee" : "#cfdcef",
							tie: TIES[Math.floor(r() * TIES.length)],
							...(m.anim === "mic" ? { mic: true } : {}),
						}),
		});
	});
	// The moppers, one at each end, in the home team's colors (see
	// mopState).
	([0, 1] as const).forEach((end) => {
		const seed = `mop|${teamKey(home)}|${end}`;
		const r = makeCourtRng(seed);
		out.push({
			pid: -61 - end,
			role: "mop",
			team: end,
			spot: MOP_HOME[end],
			hgt: 64 + r() * 10,
			weight: 130 + r() * 50,
			face: faceFor(seed, r() < 0.4),
			dress: dress(
				home?.colors?.[0] ?? "#8c1d40",
				"#1c1c20",
				{ sleeves: "short", towel: true },
				"#f2f2f2",
			),
		});
	});
	// The dance team, in the home colors (see danceState).
	DANCE_SPOTS.slice(0, spec.entertainment.dancers).forEach((at, n) => {
		const seed = `dance|${teamKey(home)}|${n}`;
		const r = makeCourtRng(seed);
		out.push({
			pid: -81 - n,
			role: "dance",
			team: 0,
			spot: at,
			share: spec.entertainment.danceShare,
			hgt: 64 + r() * 5,
			weight: 118 + r() * 20,
			face: faceFor(seed, true),
			dress: dress(
				home?.colors?.[0] ?? "#8c1d40",
				"#141416",
				{},
				"#f4f4f2",
				home?.colors?.[1] ?? "#f2c14e",
			),
		});
	});
	// Stood about the floor all game: the sideline reporter by the corner
	// past the home bench (out of the way of the team going off at the
	// end), microphone in hand; security at the two far corners, arms
	// folded; and a camera at each near corner (see staffState).
	STAFF.forEach((st, n) => {
		const seed = `staff|${teamKey(home)}|${n}`;
		const r = makeCourtRng(seed);
		const suit = SUITS[Math.floor(r() * SUITS.length)]!;
		out.push({
			pid: -71 - n,
			role: "staff",
			team: 0,
			spot: st.at,
			hgt: st.anim === "mic" ? 64 + r() * 6 : 70 + r() * 6,
			weight: st.anim === "mic" ? 125 + r() * 30 : 200 + r() * 50,
			face: faceFor(seed, st.anim === "mic" ? r() < 0.75 : r() < 0.15),
			dress:
				st.anim === "mic"
					? dress(suit, suit, {
							sleeves: "long",
							shirt: r() < 0.6 ? "#f2f2ee" : "#e7d6c9",
							mic: true,
						})
					: st.anim === "shootStand"
						? dress(
								PHOTO_TOPS[Math.floor(r() * PHOTO_TOPS.length)]!,
								PHOTO_PANTS[Math.floor(r() * PHOTO_PANTS.length)]!,
								{ sleeves: "short", camera: true },
							)
						: dress("#141519", "#141519", { sleeves: "long", plain: true }),
		});
	});
	return out;
};

// STAFF ABOUT THE FLOOR, stood where they stand all game, facing the play
// side of the floor.
const STAFF: { at: Pt; anim: AnimName }[] = [
	{ at: { x: COURT_W + 0.8, y: -4.8 }, anim: "mic" },
	{ at: { x: -6.8, y: -5.2 }, anim: "crossed" },
	{ at: { x: COURT_W + 6.8, y: -5.2 }, anim: "crossed" },
	// (The broadcast's own, a camera on his shoulder, at each near corner
	// of the floor.)
	{ at: { x: -3.6, y: COURT_H - 2.2 }, anim: "shootStand" },
	{ at: { x: COURT_W + 3.6, y: COURT_H - 2.2 }, anim: "shootStand" },
];
const staffState = (t: number, m: CrewMember): PlayerState => {
	const k = -71 - m.pid;
	const at = m.spot!;
	const anim = STAFF[k]!.anim;
	return {
		pid: m.pid,
		team: m.team,
		shown: true,
		x: at.x,
		y: at.y,
		z: 0,
		yaw: yawTo(at, { x: COURT_W / 2, y: COURT_H / 2 }),
		anim,
		phase: loopPhase(anim, t, m.pid * 0.37),
		moving: false,
	};
};

// THE MOPPERS: one at each end, down on a knee behind the photographers,
// in front of the seats - outside the clear lane round the stanchion, back
// from where the official works the baseline - in the home team's colors. At a timeout or a break - the floor
// clear, everybody at the benches - each jogs out with his towel to the
// paint at his end (beside where a man went down there, if one did since
// the last), wipes the floor down low, and jogs back.
const MOP_HOME: [Pt, Pt] = [
	{ x: -5.6, y: 16.2 },
	{ x: COURT_W + 5.6, y: 16.2 },
];
// Into the break (ms); wiping (ms); his pace there and back (feet a
// second); how far short of where a man went down he stops (feet); and
// back with this long (ms) to spare before the break is over.
const MOP_AFTER = 1500;
const MOP_MS = 2000;
const MOP_PACE = 12;
const MOP_SHORT = 1.3;
const MOP_SPARE = 1000;
const MOP_EASE = 250;
const MOP_BREAKS = new Set(["timeout", "period", "endOfPeriod"]);
const FELL = new Set<AnimName>([
	"fall",
	"takeCharge",
	"chargeDown",
	"hurt",
	"hurtKnee",
	"hurtAnkle",
	"hurtHead",
	"hurtHand",
	"hurtArm",
]);
type Wipe = { t0: number; at: Pt; there: number; done: number; back: number };
const wipeCache = new WeakMap<CourtTimeline, [Wipe[], Wipe[]]>();
const wipesOf = (tl: CourtTimeline): [Wipe[], Wipe[]] => {
	const had = wipeCache.get(tl);
	if (had) {
		return had;
	}
	const downs: { t: number; at: Pt }[] = [];
	for (const tr of tl.tracks.values()) {
		for (const a of tr.acts) {
			if (FELL.has(a.anim)) {
				const p = evalPlayer(tl, tr.pid, (a.t0 + a.t1) / 2);
				if (p.x > 0 && p.x < COURT_W && p.y > 0 && p.y < COURT_H) {
					downs.push({ t: a.t0, at: { x: p.x, y: p.y } });
				}
			}
		}
	}
	const out: [Wipe[], Wipe[]] = [[], []];
	let since = -Infinity;
	for (const brk of tl.beats) {
		if (!MOP_BREAKS.has(brk.type)) {
			continue;
		}
		// (From the horn, or the whistle for the timeout - a period's break
		// from its start - till play starts again.)
		const t0 =
			(brk.type === "period" ? brk.preStart : brk.actionStart) + MOP_AFTER;
		const t1 = (brk.type === "period" ? brk.actionStart : brk.end) - MOP_SPARE;
		for (const end of [0, 1] as const) {
			const home = MOP_HOME[end];
			const down = downs.findLast(
				(d) =>
					d.t > since &&
					d.t < brk.preStart &&
					(d.at.x < COURT_W / 2 ? 0 : 1) === end,
			);
			// (Otherwise the paint, somewhere about the rim.)
			const h = unit(Math.round(brk.preStart), end * 7 + 3);
			const spot = down?.at ?? {
				x: end === 0 ? 4 + h * 9 : COURT_W - 4 - h * 9,
				y: 25 + (unit(Math.round(brk.preStart), end * 11 + 5) - 0.5) * 9,
			};
			const L = Math.hypot(spot.x - home.x, spot.y - home.y);
			const go = Math.max(0, L - (down ? MOP_SHORT : 0));
			const run = (go / MOP_PACE) * 1000;
			if (t0 + 2 * run + MOP_MS > t1) {
				continue;
			}
			out[end].push({
				t0,
				at: {
					x: home.x + ((spot.x - home.x) * go) / (L || 1),
					y: home.y + ((spot.y - home.y) * go) / (L || 1),
				},
				there: t0 + run,
				done: t0 + run + MOP_MS,
				back: t0 + 2 * run + MOP_MS,
			});
		}
		since = brk.preStart;
	}
	wipeCache.set(tl, out);
	return out;
};
const mopState = (tl: CourtTimeline, t: number, m: CrewMember): PlayerState => {
	const home = MOP_HOME[m.team];
	const base = { pid: m.pid, team: m.team, shown: true, z: 0 };
	const court = { x: m.team === 0 ? 10 : COURT_W - 10, y: COURT_H / 2 };
	const w = wipesOf(tl)[m.team].findLast((x) => x.t0 <= t);
	// (Into each thing he does over a moment, out of what he did before.)
	const eased = (st: PlayerState, since: number, was: Blend): PlayerState => {
		const k = 1 - smooth01(since / MOP_EASE);
		return k > 0 ? { ...st, from: { ...was, w: k } } : st;
	};
	const jog = (A: Pt, B: Pt, a: number, b: number): PlayerState => {
		const u = Math.min(1, (t - a) / Math.max(1, b - a));
		return {
			...base,
			x: A.x + (B.x - A.x) * u,
			y: A.y + (B.y - A.y) * u,
			yaw: yawTo(A, B),
			anim: "jog",
			phase: (t / 1000) * 1.5,
			moving: true,
		};
	};
	const kneel: PlayerState = {
		...base,
		x: home.x,
		y: home.y,
		yaw: yawTo(home, court),
		anim: "kneel",
		phase: loopPhase("kneel", t, m.pid * 0.3),
		moving: false,
	};
	if (!w || t >= w.back) {
		return w
			? eased(kneel, t - w.back, {
					anim: "jog",
					phase: (t / 1000) * 1.5,
					w: 1,
				})
			: kneel;
	}
	if (t < w.there) {
		return eased(jog(home, w.at, w.t0, w.there), t - w.t0, {
			anim: "kneel",
			phase: 0,
			w: 1,
		});
	}
	if (t < w.done) {
		return eased(
			{
				...base,
				x: w.at.x,
				y: w.at.y,
				yaw: yawTo(home, w.at),
				anim: "wipe",
				phase: loopPhase("wipe", t, m.pid * 0.3),
				moving: false,
			},
			t - w.there,
			{ anim: "jog", phase: (t / 1000) * 1.5, w: 1 },
		);
	}
	return eased(jog(w.at, home, w.done, w.back), t - w.done, {
		anim: "wipe",
		phase: loopPhase("wipe", t, m.pid * 0.3),
		w: 1,
	});
};

// THE DANCE TEAM: at a timeout, the teams in their huddles, out they run
// from the seats on the near side to center court - two rows, facing that
// side - and the routine, all of them as one (see dance), till the teams
// come back out; then off at a run the way they came, straight off the
// floor, not across it through the teams. Not every
// timeout (ArenaSpec.entertainment) - and only one the picture looks out
// over the whole floor for.
const DANCE_SPOTS: Pt[] = [
	{ x: COURT_W / 2 - 7.5, y: 29.5 },
	{ x: COURT_W / 2 - 2.5, y: 29.5 },
	{ x: COURT_W / 2 + 2.5, y: 29.5 },
	{ x: COURT_W / 2 + 7.5, y: 29.5 },
	{ x: COURT_W / 2 - 5, y: 24 },
	{ x: COURT_W / 2, y: 24 },
	{ x: COURT_W / 2 + 5, y: 24 },
];
const DANCE_OFF = COURT_H + 6;
// Into the timeout before they run out - the teams in their huddles - one
// after another (ms); their pace (feet a second); the shortest timeout they
// come out for (ms).
const DANCE_AFTER = 2500;
const DANCE_NEXT = 110;
const DANCE_PACE = 14;
const DANCE_LONGEST = 7000;
type Dance = { t0: number; end: number };
const danceCache = new WeakMap<CourtTimeline, Dance[]>();
const dancesOf = (tl: CourtTimeline, share: number): Dance[] => {
	const had = danceCache.get(tl);
	if (had) {
		return had;
	}
	const out: Dance[] = [];
	for (const b of tl.beats) {
		// (When the picture is on the whole floor for it, not the crowd.)
		if (
			b.type === "timeout" &&
			b.end - b.actionStart >= DANCE_LONGEST &&
			tl.shots.some(
				(x) => x.kind === "wide" && x.t0 < b.end && x.t1 > b.actionStart,
			) &&
			unit(Math.round(b.preStart), 4242) < share
		) {
			out.push({ t0: b.actionStart + DANCE_AFTER, end: b.end + 150 });
		}
	}
	danceCache.set(tl, out);
	return out;
};
const danceState = (
	tl: CourtTimeline,
	t: number,
	m: CrewMember,
): PlayerState | undefined => {
	const k = -81 - m.pid;
	const d = dancesOf(tl, m.share ?? 0).findLast((x) => x.t0 <= t);
	const at = m.spot!;
	const door = { x: at.x, y: DANCE_OFF };
	const run = ((DANCE_OFF - at.y) / DANCE_PACE) * 1000;
	const out0 = d && d.t0 + k * DANCE_NEXT;
	const there = out0 !== undefined ? out0 + run : 0;
	const off = d ? d.end + k * DANCE_NEXT * 0.8 : 0;
	if (!d || t < out0! || t >= off + run) {
		return undefined;
	}
	const base = { pid: m.pid, team: m.team, shown: true, z: 0, moving: false };
	const running = (A: Pt, B: Pt, a: number): PlayerState => {
		const u = Math.min(1, Math.max(0, (t - a) / run));
		return {
			...base,
			x: A.x + (B.x - A.x) * u,
			y: A.y + (B.y - A.y) * u,
			yaw: yawTo(A, B),
			anim: "run",
			phase: (t / 1000) * 1.7 + k * 0.13,
			moving: true,
		};
	};
	if (t < there) {
		return running(door, at, out0!);
	}
	if (t >= off) {
		return running(at, door, off);
	}
	// (All as one: the same count, from when the first of them is there.)
	const st: PlayerState = {
		...base,
		x: at.x,
		y: at.y,
		yaw: Math.PI / 2,
		anim: "dance",
		phase: loopPhase("dance", t - (d.t0 + run), 0),
	};
	const k0 = 1 - smooth01((t - there) / 300);
	return k0 > 0
		? { ...st, from: { anim: "run", phase: (t / 1000) * 1.7, w: k0 } }
		: st;
};

// Where the pre-game show stands, and what each of them does.
const MEDIA: { at: Pt; anim: AnimName }[] = [
	{ at: { x: 45.6, y: 3.3 }, anim: "mic" },
	{ at: { x: 48.4, y: 3.1 }, anim: "talk" },
	{ at: { x: 52.6, y: 6.4 }, anim: "shootStand" },
];
const TIES = ["#7a1f2b", "#1f3f7a", "#2b2b2f", "#6b5a2a"];

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

// When a shot taken from t comes down at the rim - the arm up for a three
// held till then, as an official does, not dropped with it in the air; and
// going in, held on till both arms go up for it.
const comesDown = (tl: CourtTimeline, t: number): number => {
	const i = Math.max(
		0,
		lastIndex(tl.ball, t, (s) => s.t0),
	);
	for (let j = i; j < tl.ball.length && j < i + 12; j++) {
		const seg = tl.ball[j]!;
		if (seg.t0 > t + 3000) {
			break;
		}
		if (
			seg.kind === "fly" &&
			!("pid" in seg.to) &&
			(tl.ball[j + 1]?.kind === "path" || Math.abs(seg.to.z - RIM_Z) < 1.6)
		) {
			const roar = nextFx(tl, seg.t1, "roar");
			return roar?.kind === "roar" &&
				roar.what === "three" &&
				roar.t < seg.t1 + 1500
				? roar.t
				: seg.t1 + 150;
		}
	}
	return -Infinity;
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

// Out of the way: a man working along the line where an official means to
// stand, the official gives him room - along the sideline, or the end line,
// whichever he works.
const ROOM = 4.5;
const clearOfPlayers = (
	tl: CourtTimeline,
	t: number,
	goal: [Pt, Pt, Pt],
): [Pt, Pt, Pt] =>
	goal.map((g) => {
		const sideline = g.y < 2 || g.y > COURT_H - 2;
		let q = { ...g };
		for (const [, tr] of tl.tracks) {
			const p = floorSpotOf(tr, t);
			if (!p || Math.hypot(p.x - q.x, p.y - q.y) >= ROOM) {
				continue;
			}
			q = sideline
				? { x: p.x + (q.x >= p.x ? 1 : -1) * ROOM, y: q.y }
				: { x: q.x, y: p.y + (q.y >= p.y ? 1 : -1) * ROOM };
		}
		return q;
	}) as [Pt, Pt, Pt];

// The three of them, by crew position: the lead under the basket the ball is
// going at, the trail behind the play on the far (table) side, the slot on
// the near sideline. Lead and trail trade places every change of possession,
// as a crew does in transition; the third keeps the near side. In a break
// they gather by the table; for a jump ball the third throws it up.
const TOSS = { x: COURT_W / 2, y: COURT_H / 2 - 1.65 };
// (Out of the way at the top of the circle while they come out to it.)
const TOSS_WAIT = { x: COURT_W / 2, y: COURT_H / 2 - 8.5 };
const atCenter = (p: BallEnd) =>
	!("pid" in p) && Math.hypot(p.x - COURT_W / 2, p.y - COURT_H / 2) < 0.6;
// Whether the ball is in his hands there yet - not still on its way out
// to him from the huddle.
const tossBallIn = (tl: CourtTimeline, t: number): boolean => {
	const seg = tl.ball[lastIndex(tl.ball, t, (b) => b.t0)];
	return (
		seg !== undefined &&
		((seg.kind === "rest" && atCenter(seg.at)) ||
			(seg.kind === "fly" &&
				(atCenter(seg.from) || (atCenter(seg.to) && t >= seg.t1))))
	);
};
// When the ball is given to him out there, if it is (from `t0` on).
const tossBallSent = (tl: CourtTimeline, t0: number): number | undefined => {
	for (let i = lastIndex(tl.ball, t0, (b) => b.t0); i < tl.ball.length; i++) {
		const seg = tl.ball[i];
		if (seg?.kind === "fly" && seg.t0 >= t0 && atCenter(seg.to)) {
			return seg.t0;
		}
	}
	return undefined;
};
// Which of the three throws it up: the one who is to be the trail once it
// is tipped - out on the table side, where he stands to throw it - so he
// backs straight out to his place, not through the two of them.
const tosserOf = (tl: CourtTimeline, toss: Fx): number =>
	lastIndex(tl.poss, toss.t + 600, (p) => p[0]) % 2 === 0 ? 1 : 0;
// (From the horn, ms.)
const REFS_OFF = 1200;
const refTargets = (tl: CourtTimeline, t: number): [Pt, Pt, Pt] => {
	const beat = beatAt(tl, t);
	const shot = arenaShotAt(tl, t);
	// (Over a break; not the last look round the building, the game over.)
	if (shot && shot !== tl.shots[0] && beat?.type !== "gameOver") {
		return [
			{ x: COURT_W / 2 - 2.4, y: 3.2 },
			{ x: COURT_W / 2 + 2.4, y: 3.2 },
			{ x: COURT_W / 2, y: 4.5 },
		];
	}
	if (beat?.type === "gameOver" && t >= beat.actionStart + REFS_OFF) {
		// The game over, the three of them off the floor together, straight
		// away - over by the near corner, out of the way of the handshakes
		// (as far off it as an official goes: see refStates).
		return [
			{ x: COURT_W + 2.4, y: COURT_H + 1.5 },
			{ x: COURT_W + 0.5, y: COURT_H + 1.7 },
			{ x: COURT_W - 1.4, y: COURT_H + 1.5 },
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
			// Out to the circle once they are there and the ball is coming
			// out to him.
			const sent = tossBallSent(tl, beat.preStart);
			const out: [Pt, Pt, Pt] = [
				{ x: COURT_W / 2 - 7, y: 0.9 },
				{ x: COURT_W / 2 - 7, y: 0.9 },
				{ x: COURT_W / 2 + 7, y: COURT_H + 1.2 },
			];
			out[tosserOf(tl, toss)] =
				sent === undefined || t >= sent - 1500 ? TOSS : TOSS_WAIT;
			return out;
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
	return clearOfPlayers(
		tl,
		t,
		k % 2 === 0 ? [lead, trail, slot] : [trail, lead, slot],
	);
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
type Places = { at: Pt[] };
// Round the edge of the floor, not across it: from the sideline down to the
// end line - or back up - by way of the corner.
const viaCorner = (p: Pt, g: Pt): Pt => {
	const side = (q: Pt) => q.y < 2 || q.y > COURT_H - 2;
	const end = (q: Pt) => q.x < 0.5 || q.x > COURT_W - 0.5;
	if (Math.abs(g.x - p.x) < 6) {
		return g;
	}
	if (side(p) && end(g)) {
		return {
			x: g.x,
			y: p.y < COURT_H / 2 ? Math.min(p.y, 0.9) : Math.max(p.y, COURT_H + 1.2),
		};
	}
	if (end(p) && side(g) && Math.abs(p.y - g.y) > 6) {
		return { x: p.x, y: g.y };
	}
	return g;
};
// How far it is from p to g that way, and where he is a fraction `u` of the
// way along it.
const edgeDist = (p: Pt, g: Pt): number => {
	const w = viaCorner(p, g);
	return dist(p, w) + dist(w, g);
};
const alongEdge = (p: Pt, g: Pt, u: number): Pt => {
	const w = viaCorner(p, g);
	const a = dist(p, w);
	const s = u * (a + dist(w, g));
	return s < a ? lerp(p, w, s / a) : lerp(w, g, (s - a) / (dist(w, g) || 1));
};
// (Worked out as far as it is needed - the game so far, a step at a time as
// it goes on - not the whole game at once before the first picture: that
// takes seconds. And after a jump ahead - on to the next key play, joining
// late - from ten seconds short of where it lands, not every step of what
// it jumped: the picture cuts there anyway, and that many steps would hold
// it up most of a second.)
type RefRun = {
	// The step it works out next, and where the officials are as of the one
	// before.
	k: number;
	at: Pt[];
	vel: Pt[];
	// The next camera cut to pass.
	ci: number;
	// Starting afresh: everyone where the play has them.
	fresh: boolean;
	// Steps it has gone on over ones worked out from a later fresh start,
	// till it meets up with them.
	over: number;
};
type RefPath = {
	out: (Pt[] | undefined)[];
	runs: RefRun[];
	cuts: number[];
	n: number;
};
// How far ahead (steps) a run goes on to rather than a fresh start, and how
// far short of where it is wanted one starts.
const JUMP = 40;
const WARM = 40;
const refPaths = new WeakMap<CourtTimeline, RefPath>();
const refRunFrom = (track: RefPath, k: number): RefRun => {
	let ci = 0;
	while (ci < track.cuts.length && track.cuts[ci]! <= k * STEP) {
		ci++;
	}
	return { k, at: [], vel: [], ci, fresh: true, over: 0 };
};
// The officials at the run's next step.
const refStep = (tl: CourtTimeline, track: RefPath, run: RefRun): Pt[] => {
	const { cuts } = track;
	const t = run.k * STEP;
	const dt = STEP / 1000;
	const goal = refTargets(tl, t);
	let cut = run.fresh;
	while (run.ci < cuts.length && cuts[run.ci]! <= t) {
		cut ||= cuts[run.ci]! > t - STEP;
		run.ci++;
	}
	run.fresh = false;
	run.k++;
	if (cut) {
		run.at = goal.map((q) => ({ ...q }));
		run.vel = run.at.map(() => ({ x: 0, y: 0 }));
		return run.at;
	}
	const vel = run.vel;
	run.at = run.at.map((p, i) => {
		const g = goal[i]!;
		const W = viaCorner(p, g);
		// (How far he has to go, round the corner if he goes that way.)
		const d0 = dist(p, W);
		const left = d0 + dist(W, g);
		const u =
			d0 > 0.01 ? { x: (W.x - p.x) / d0, y: (W.y - p.y) / d0 } : { x: 0, y: 0 };
		let wx = u.x * left * REF_GAIN;
		let wy = u.y * left * REF_GAIN;
		const w = Math.hypot(wx, wy);
		// No faster than he can pull up from by the time he gets there.
		const most = Math.min(REF_RUN, Math.sqrt(2 * REF_ACCEL * left));
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
	return run.at;
};
// Step `want` worked out: by the run that has got nearest it, if one is
// near enough, or one started afresh a little short of it. A run that
// catches up with steps a fresh start already worked out goes on over them
// till it has the officials where they had them - no jump where they meet.
const refFill = (tl: CourtTimeline, track: RefPath, want: number) => {
	const { out, runs } = track;
	for (let guard = 0; guard < 8; guard++) {
		const have = out[want] !== undefined;
		let run: RefRun | undefined;
		for (const q of runs) {
			if (
				q.k <= want &&
				want - q.k <= JUMP &&
				(!have || out[q.k] !== undefined) &&
				(!run || q.k > run.k)
			) {
				run = q;
			}
		}
		if (!run) {
			if (have) {
				return;
			}
			run = refRunFrom(track, Math.max(0, want - WARM));
			runs.push(run);
		}
		while (run.k <= want) {
			const was = out[run.k];
			const at = refStep(tl, track, run);
			if (
				was &&
				(run.over >= JUMP || was.every((q, i) => dist(q, at[i]!) < 0.25))
			) {
				runs.splice(runs.indexOf(run), 1);
				break;
			}
			if (was) {
				run.over++;
			}
			out[run.k - 1] = at;
			if (run.k >= track.n) {
				runs.splice(runs.indexOf(run), 1);
				break;
			}
			if (out[run.k] === undefined) {
				// (Where the run it went on over had got to: this one goes on
				// from here for it.)
				run.over = 0;
				for (let j = runs.length - 1; j >= 0; j--) {
					if (runs[j] !== run && runs[j]!.k === run.k) {
						runs.splice(j, 1);
					}
				}
			}
		}
	}
};
const refPlaces = (tl: CourtTimeline, t: number): Places => {
	let track = refPaths.get(tl);
	if (!track) {
		track = {
			out: [],
			runs: [],
			cuts: cameraCuts(tl),
			n: Math.ceil(tl.end / STEP) + 2,
		};
		refPaths.set(tl, track);
	}
	// Through his steps (a quarter of a second apart) on a curve: half way
	// between each two of them at the half steps, and round the step
	// between, so his speed and the way he goes change as he goes rather
	// than all at once at each step.
	const s0 = t / STEP - 0.5;
	const g = Math.max(0, Math.min(track.n - 3, Math.floor(s0)));
	const u = Math.max(0, Math.min(1, s0 - g));
	// (The picture cutting anywhere in it: from one step to the next, and
	// settled at once where it cuts, not slid there.)
	const cut = lastCut(tl, (g + 2) * STEP);
	if (cut > g * STEP || s0 < 0) {
		const h = Math.max(0, Math.min(track.n - 2, Math.floor(t / STEP)));
		refFill(tl, track, h);
		refFill(tl, track, h + 1);
		const a = track.out[h]!;
		const b = track.out[h + 1]!;
		let v = Math.max(0, Math.min(1, (t - h * STEP) / STEP));
		const at = lastCut(tl, (h + 1) * STEP);
		if (at > h * STEP) {
			v = t < at ? 0 : 1;
		}
		return { at: a.map((p, i) => lerp(p, b[i]!, v)) };
	}
	refFill(tl, track, g);
	refFill(tl, track, g + 1);
	refFill(tl, track, g + 2);
	const [a, b, c] = [track.out[g]!, track.out[g + 1]!, track.out[g + 2]!];
	const wa = 0.5 * (1 - u) * (1 - u);
	const wc = 0.5 * u * u;
	const wb = 1 - wa - wc;
	return {
		at: a.map((p, i) => ({
			x: p.x * wa + b[i]!.x * wb + c[i]!.x * wc,
			y: p.y * wa + b[i]!.y * wb + c[i]!.y * wc,
		})),
	};
};

type Signal = {
	ref: number;
	t0: number;
	// Each move of it: the anim, its span, which way he faces.
	moves: { anim: AnimName; t0: number; t1: number; yaw: number }[];
	// Where he stood to make it, and how long he then takes to run back to
	// where the play has him - and, if he ran in to make it, from where.
	still: Pt;
	back: number;
	from?: Pt;
};

const FACING_US = Math.PI / 2;
// His right arm out toward x's direction `dir`.
const pointing = (dir: number) => (dir > 0 ? -Math.PI / 2 : Math.PI / 2);

// The officials' average pace (feet a second) running back after a signal.
const BACK_PACE = 13;
const BACK_LEAST = 500;
// Where an official is while he signals: still where he made the call,
// then running back to his place in the play.
const signalPos = (s: Signal, t: number, liveAt: (when: number) => Pt): Pt => {
	const t1 = s.moves.at(-1)!.t1;
	if (t < t1) {
		const m0 = s.moves[0]!.t0;
		if (s.from && t < m0) {
			const k = smooth01((t - s.t0) / (m0 - s.t0));
			return {
				x: s.from.x + (s.still.x - s.from.x) * k,
				y: s.from.y + (s.still.y - s.from.y) * k,
			};
		}
		return s.still;
	}
	if (t >= t1 + s.back) {
		return liveAt(t);
	}
	// Back to where the play will have him once he gets there - round the
	// edge of the floor, as he goes anywhere - getting going from where he
	// stood, and coming into his place as quick as it goes on: not off at a
	// run from a standstill and pulled up dead, or stopped short as it runs
	// on.
	const goal = liveAt(t1 + s.back);
	const far = edgeDist(s.still, goal);
	const u = (t - t1) / s.back;
	if (far < 0.05) {
		return alongEdge(s.still, goal, u);
	}
	const w = viaCorner(s.still, goal);
	const from = dist(w, goal) > 0.01 ? w : s.still;
	const way = { x: goal.x - from.x, y: goal.y - from.y };
	const len = Math.hypot(way.x, way.y) || 1;
	const a = liveAt(t1 + s.back - 50);
	const b = liveAt(t1 + s.back + 50);
	const ends = (((b.x - a.x) * way.x + (b.y - a.y) * way.y) / len) * 10;
	// (How quick it goes, as a share of his average, at most three times it:
	// a curve from standing to that.)
	const m = clamp((ends * s.back) / 1000 / far, 0, 3);
	return alongEdge(s.still, goal, (m - 2) * u * u * u + (3 - m) * u * u);
};

// Everything the officials have had to signal lately: each whistle's call,
// a three going up (an arm raised while it is in the air) and going in
// (both arms). Some he runs in this far toward to make (feet) - and some go
// to another official than the one before (`pair`).
type Cue = {
	at: Pt;
	t0: number;
	moves: Signal["moves"];
	onto?: number;
	pair?: boolean;
};
// A shot after the horn waved off: run in at it - no nearer it than this
// (feet), as quick as this (feet a second) - waving it off this long (ms).
const WAVE_ONTO = 7;
const WAVE_SHORT = 5;
const WAVE_PACE = 11;
const WAVE_MS = 1600;
// (Long enough for the signal and the jog back to his place after it.)
const CUE_MS = 11000;
const cuesBefore = (tl: CourtTimeline, t: number): Cue[] => {
	const out: Cue[] = [];
	for (let i = lastIndex(tl.fx, t, (f) => f.t); i >= 0; i--) {
		const f = tl.fx[i]!;
		if (t - f.t > CUE_MS) {
			break;
		}
		if (f.kind === "whistle" && f.call === "noBasket") {
			// The one nearest the man who let it go runs in at him, another
			// in at the basket, both waving it off.
			const wave = (at: Pt, pair: boolean): Cue => ({
				at,
				t0: f.t,
				moves: [
					{ anim: "waveOff", t0: f.t, t1: f.t + WAVE_MS, yaw: FACING_US },
				],
				onto: WAVE_ONTO,
				pair,
			});
			out.push(wave(f.at ?? ballSpot(tl, f.t), false));
			if (f.rim !== undefined) {
				out.push(wave({ x: rimX(f.rim), y: COURT_H / 2 }, true));
			}
		} else if (f.kind === "whistle" && f.call) {
			const dir = f.team === undefined ? 1 : attackDir(f.team);
			const side = { anim: "signalSide" as const, yaw: pointing(dir) };
			const up = { anim: "signalUp" as const, yaw: FACING_US };
			// (A foul, after the fist, what it was: hit, held, blocked,
			// pushed; on a shot, the hit; the charge, a fist punched out.)
			const what = (anim: AnimName) => ({ anim, yaw: FACING_US });
			const how: AnimName | undefined =
				f.call === "offensive"
					? "signalCharge"
					: f.call === "shootingFoul" || f.call === "andOne"
						? "signalHit"
						: f.call === "foul"
							? f.how === "hold"
								? "signalHold"
								: f.how === "block"
									? "signalBlock"
									: f.how === "push"
										? "signalPush"
										: "signalHit"
							: undefined;
			const plan: [{ anim: AnimName; yaw: number }, number][] =
				f.call === "out"
					? [[side, 1300]]
					: f.call === "travel"
						? [
								[{ anim: "travel", yaw: FACING_US }, 1000],
								[side, 950],
							]
						: f.call === "stop"
							? [[up, 1100]]
							: f.call === "shootingFoul" && how
								? [
										[up, 800],
										[what(how), 900],
									]
								: how
									? [
											[up, 700],
											[what(how), 900],
											[side, 850],
										]
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
						t1: Math.max(b.actionStart + 1000, comesDown(tl, b.actionStart)),
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
// (And with `after`, one he has been through with as long ago as that.)
const signalsAt = (
	tl: CourtTimeline,
	t: number,
	after = 0,
): (Signal | undefined)[] => {
	const per: (Signal | undefined)[] = [undefined, undefined, undefined];
	// A cut settles everyone where the play has them.
	const cut = lastCut(tl, t);
	let last = -1;
	for (const cue of cuesBefore(tl, t)) {
		if (cue.t0 < cut) {
			continue;
		}
		const live = refPlaces(tl, cue.t0).at;
		const pos = live.map((p, i) => {
			const s = per[i];
			return s ? signalPos(s, cue.t0, (when) => refPlaces(tl, when).at[i]!) : p;
		});
		const taken = cue.pair ? last : -1;
		let ref = taken === 0 ? 1 : 0;
		for (let i = ref + 1; i < 3; i++) {
			if (i !== taken && dist(pos[i]!, cue.at) < dist(pos[ref]!, cue.at)) {
				ref = i;
			}
		}
		last = ref;
		let still = pos[ref]!;
		let moves = cue.moves;
		let from: Pt | undefined;
		// (Running in to make it: the signal once he is there.)
		const onto = cue.onto
			? Math.min(cue.onto, dist(still, cue.at) - WAVE_SHORT)
			: 0;
		if (onto > 0.5) {
			const k = onto / dist(still, cue.at);
			from = still;
			still = {
				x: still.x + (cue.at.x - still.x) * k,
				y: still.y + (cue.at.y - still.y) * k,
			};
			const ms = (onto / WAVE_PACE) * 1000;
			moves = moves.map((m) => ({ ...m, t0: m.t0 + ms, t1: m.t1 + ms }));
		}
		const t1 = moves.at(-1)!.t1;
		// As long as it takes him to jog back to where the play will have him
		// by then (longer, never shorter, till he can make it) - BACK_PACE
		// feet a second, on average (see signalPos) - and never so short he
		// is spun round to it: BACK_LEAST.
		let back = BACK_LEAST;
		for (let k = 0; k < 8; k++) {
			const need = clamp(
				(edgeDist(still, refPlaces(tl, t1 + back).at[ref]!) / BACK_PACE) * 1000,
				BACK_LEAST,
				6500,
			);
			if (need <= back) {
				break;
			}
			back = need;
		}
		per[ref] = {
			ref,
			t0: cue.t0,
			moves,
			still,
			back,
			...(from ? { from } : {}),
		};
	}
	return per.map((s) =>
		s && t < s.moves.at(-1)!.t1 + s.back + after ? s : undefined,
	);
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
	const k = lastIndex(tl.ball, t, (b) => b.t0);
	const seg = tl.ball[k];
	if (!seg) {
		return undefined;
	}
	if (seg.kind === "fly" && near(seg.from)) {
		return t < seg.t0 + PASS_OUT
			? { anim: "passBounce", phase: (t - seg.t0 + PASS_WIND) / 300 }
			: undefined;
	}
	// (Still in his hands, the bounce pass under way a moment before it
	// leaves them - not begun half done as it goes.)
	const next = tl.ball[k + 1];
	const holding =
		(seg.kind === "fly" && near(seg.to) && t >= seg.t1 + 150) ||
		(seg.kind === "rest" && near(seg.at));
	if (
		holding &&
		next?.kind === "fly" &&
		near(next.from) &&
		next.t0 - t < PASS_WIND
	) {
		return { anim: "passBounce", phase: (t - next.t0 + PASS_WIND) / 300 };
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
// The bounce pass in to the shooter: how long before it leaves his hands it
// starts, and how long after he is through with it (ms).
const PASS_WIND = 120;
const PASS_OUT = 200;
// Just through with it, how much of his pass still shows as he goes back
// to how the play has him (over SIGNAL_EASE).
const passedIn = (tl: CourtTimeline, t: number): number => {
	const beat = beatAt(tl, t);
	if (beat?.type !== "ft" && beat?.type !== "missFt") {
		return 0;
	}
	const ball = ftOfficialBall(offenseAt(tl, t));
	// (Back past its bounce on the way in.)
	const k = lastIndex(tl.ball, t, (b) => b.t0);
	const seg = [0, 1, 2]
		.map((j) => tl.ball[k - j])
		.find(
			(b) =>
				b?.kind === "fly" &&
				!("pid" in b.from) &&
				Math.hypot(b.from.x - ball.x, b.from.y - ball.y) < 0.6,
		);
	const after = seg ? t - seg.t0 - PASS_OUT : -1;
	return !seg || after < 0 || after >= SIGNAL_EASE
		? 0
		: 1 - smooth01(after / SIGNAL_EASE);
};

// WHICH WAY AN OFFICIAL FACES: the ball - or, running with the play, the
// way he goes, more of it the quicker he goes (from FACE_RUN_FROM to
// FACE_RUN_TO, feet a second), his head turned to keep the ball in sight.
// How quick, over a moment either side (FACE_SPAN): his places are worked
// out a STEP apart, and from one stretch to the next he would turn in a
// frame. A signal's facing - and the run back to his place after it - is
// come round to over SIGNAL_TURN, from whichever way he faced before.
const FACE_SPAN = 400;
const FACE_RUN_FROM = 6;
const FACE_RUN_TO = 12;
const SIGNAL_TURN = 220;
const LOOK_MOST = 70;
const LOOK_BACK = 115;
const smooth01 = (u: number) => {
	const x = clamp(u, 0, 1);
	return x * x * (3 - 2 * x);
};
// An official's gait by how quick he goes (feet a second, over GAIT_SPAN):
// stood, walking, running - the quicker one eased in from the slower across
// a band of speeds (GAIT_BANDS), not switched in a frame as he crosses a
// line.
const GAIT_SPAN = 200;
const GAIT_BANDS: [AnimName, AnimName, number, number][] = [
	["ready", "walk", 0.8, 2.4],
	["walk", "run", 6, 10],
];
const gaitOf = (
	speed: number,
): { anim: AnimName; from?: { anim: AnimName; w: number } } => {
	const [a, b, lo, hi] =
		speed < (GAIT_BANDS[0]![3] + GAIT_BANDS[1]![2]) / 2
			? GAIT_BANDS[0]!
			: GAIT_BANDS[1]!;
	const u = smooth01((speed - lo) / (hi - lo));
	return u <= 0
		? { anim: a }
		: u >= 1
			? { anim: b }
			: { anim: b, from: { anim: a, w: 1 - u } };
};
// Easing out of one thing (was, so much of it still showing) while his
// gait eases between two of its own: the three of them in the shares the
// two easings give - as one blend, the gait's other inside it with that.
const SIGNAL_EASE = 200;
const over = (gait: Blend | undefined, was: Blend): Blend => {
	if (!gait || gait.w <= 0) {
		return was;
	}
	const w = 1 - (1 - gait.w) * (1 - was.w);
	return { ...gait, w, from: { ...was, w: was.w / w } };
};
// As turnToward, but the way `way` says when it is a long way round: not
// one way one frame and the other the next as `to` passes right behind.
const turnRound = (from: number, to: number, w: number, way: number) => {
	let d = to - from;
	d -= Math.round(d / (Math.PI * 2)) * Math.PI * 2;
	let k = way;
	k -= Math.round(k / (Math.PI * 2)) * Math.PI * 2;
	if (Math.abs(d) > Math.PI / 2 && k !== 0 && Math.sign(d) !== Math.sign(k)) {
		d += Math.sign(k) * Math.PI * 2;
	}
	return from + d * w;
};
const turnToward = (from: number, to: number, w: number) => {
	let d = to - from;
	d -= Math.round(d / (Math.PI * 2)) * Math.PI * 2;
	return from + d * w;
};
const refStates = (
	tl: CourtTimeline,
	t: number,
	refs: CrewMember[],
	ball: Pt,
	// (How far back through one signal after another this is looking.)
	depth = 0,
): PlayerState[] => {
	const { at } = refPlaces(tl, t);
	// How quick each goes over a moment either side - not reaching across
	// a cut, where they are settled at once wherever the play has them:
	// not run there, nor run from there. (Feet a second.)
	const pace = (span: number): Pt[] => {
		const was = lastCut(tl, t);
		const next = lastCut(tl, t + span / 2);
		const lo = Math.max(t - span / 2, was);
		const hi = next > t ? next - 1 : t + span / 2;
		if (hi - lo < 20) {
			return at.map(() => ({ x: 0, y: 0 }));
		}
		const a = refPlaces(tl, lo).at;
		const b = refPlaces(tl, hi).at;
		// (The way from the one end to the other, as quick as he went along
		// it - by way of where he is now, not straight across a corner he
		// went round, which has him slow as he turns it.)
		const m = Math.min(hi, Math.max(lo, t));
		const c = m > lo && m < hi ? refPlaces(tl, m).at : undefined;
		return a.map((p, i) => {
			const chord = dist(p, b[i]!);
			const along = c ? dist(p, c[i]!) + dist(c[i]!, b[i]!) : chord;
			const k = chord > 1e-6 ? along / chord : 0;
			return {
				x: ((b[i]!.x - p.x) * k * 1000) / (hi - lo),
				y: ((b[i]!.y - p.y) * k * 1000) / (hi - lo),
			};
		});
	};
	const going = pace(FACE_SPAN);
	const gaitGoing = pace(GAIT_SPAN);
	// (Just back from one, how quick he goes and the way he goes still taken
	// over the run back as well.)
	const signals = signalsAt(tl, t, FACE_SPAN / 2);
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
	// (The one who passed it in: the nearest of them to where he stood.)
	const passed = handing || ftTeam === undefined ? 0 : passedIn(tl, t);
	const passer =
		passed > 0
			? at.reduce(
					(best, p, i) =>
						dist(p, ftAdminAt(ftTeam!)) < dist(at[best]!, ftAdminAt(ftTeam!))
							? i
							: best,
					0,
				)
			: -1;
	return refs.map((m, i) => {
		let p = at[i]!;
		const gx = going[i]!.x;
		const gy = going[i]!.y;
		// Stood, walking or running by how quick he goes over that moment
		// too, and between two of them, the one eased into the other.
		const gaitPhase = (a: AnimName) =>
			a === "run"
				? (t / 1000) * 1.6
				: a === "walk"
					? (t / 1000) * 1.1
					: loopPhase(a, t, i * 0.37);
		const gait = gaitOf(Math.hypot(gaitGoing[i]!.x, gaitGoing[i]!.y));
		let anim = gait.anim;
		let phase = gaitPhase(anim);
		let gaitFrom: Blend | undefined = gait.from
			? {
					anim: gait.from.anim,
					phase: gaitPhase(gait.from.anim),
					w: gait.from.w,
					paced: true,
				}
			: undefined;
		// (The ball in his own hands, or going right by him: facing us.)
		const seen = ballBearing(tl, t, p);
		const toBall = seen
			? turnToward(FACING_US, seen.yaw, seen.near)
			: yawTo(p, ball);
		let yaw = turnToward(
			toBall,
			Math.atan2(gy, gx),
			smooth01(
				(Math.hypot(gx, gy) - FACE_RUN_FROM) / (FACE_RUN_TO - FACE_RUN_FROM),
			),
		);
		let watching = true;
		// (Doing something of his own the play has him do.)
		let busy = false;
		if (ftTeam !== undefined) {
			// The one with the ball at the line: facing the shooter (come
			// round to as he gets there).
			const d = dist(p, ftAdminAt(ftTeam));
			yaw = turnToward(
				yaw,
				yawTo(p, spot(ftTeam, FT_SHOOTER_DEPTH, 25)),
				smooth01((1.6 - d) / 0.8),
			);
			if (handing && d < 0.8) {
				({ anim, phase } = handing);
				gaitFrom = undefined;
				watching = false;
				busy = true;
			} else if (i === passer && d < 2.5) {
				// (Through with the bounce pass: out of it over a moment.)
				gaitFrom = over(gaitFrom, {
					anim: "passBounce",
					phase: (PASS_OUT + PASS_WIND) / 300,
					w: passed,
				});
				busy = true;
			}
		}
		if (
			toss &&
			i === tosserOf(tl, toss) &&
			t < toss.t + 600 &&
			dist(p, TOSS) < 0.6 &&
			(t >= toss.t || tossBallIn(tl, t))
		) {
			anim = "toss";
			gaitFrom = undefined;
			phase = t < toss.t ? 0 : (t - toss.t) / 600;
			yaw = FACING_US;
			watching = false;
			busy = true;
		}
		const ends = signals[i] && signals[i].moves.at(-1)!.t1 + signals[i].back;
		const signal =
			ends !== undefined && (t < ends || !busy) ? signals[i] : undefined;
		if (signal) {
			const move = signal.moves.find((s) => t >= s.t0 && t < s.t1);
			const live = p;
			const liveAt = (when: number) =>
				when === t ? live : refPlaces(tl, when).at[i]!;
			p = signalPos(signal, t, liveAt);
			// (The last of it he came round from: the move before, or as he
			// faced before it.)
			const done = signal.moves.filter((s) => s.t1 <= t).at(-1);
			// How he was at the end of a move: its last frame.
			const endOf = (s: Signal["moves"][number]): Blend => ({
				anim: s.anim,
				phase:
					ANIMS[s.anim].kind === "act" ? 1 : loopPhase(s.anim, s.t1 - s.t0, 0),
				w: 1,
			});
			if (move) {
				anim = move.anim;
				const a = ANIMS[anim];
				phase =
					a.kind === "act"
						? (t - move.t0) / (move.t1 - move.t0)
						: loopPhase(anim, t - move.t0, 0);
				const chained = done && done.t1 >= move.t0 - 1;
				// (Straight on from another call's signal - a three's arm up
				// as it goes in, say: round from how that left him, not from
				// how the play would have him.)
				const prior =
					!chained &&
					depth < 2 &&
					t - move.t0 < Math.max(SIGNAL_TURN, SIGNAL_EASE) &&
					signalsAt(tl, move.t0 - 16)[i]?.t0 !== signal.t0 &&
					signalsAt(tl, move.t0 - 16)[i] !== undefined
						? refStates(
								tl,
								move.t0 - 16,
								refs,
								ballSpot(tl, move.t0 - 16),
								depth + 1,
							)[i]
						: undefined;
				// (Run in to make it: round from the way he ran.)
				const ran =
					signal.from && move === signal.moves[0]
						? Math.atan2(
								signal.still.y - signal.from.y,
								signal.still.x - signal.from.x,
							)
						: undefined;
				yaw = turnToward(
					chained ? done.yaw : (ran ?? prior?.yaw ?? yaw),
					move.yaw,
					smooth01((t - move.t0) / SIGNAL_TURN),
				);
				// Into it from the move before, or from his stride as he
				// pulled up to make it - over a moment, not in a frame.
				const w = 1 - smooth01((t - move.t0) / SIGNAL_EASE);
				gaitFrom =
					w <= 0
						? undefined
						: chained
							? { ...endOf(done), w }
							: prior
								? { anim: prior.anim, phase: prior.phase, w }
								: {
										anim: gait.anim,
										phase: gaitPhase(gait.anim),
										w,
										...(gaitFrom ? { from: gaitFrom } : {}),
									};
			} else {
				// Running back - or walking, or stood, as quick as he goes -
				// the way he goes, and as he gets there, round to the way he
				// would face there; out of the last move over a moment.
				const a = signalPos(signal, t - GAIT_SPAN / 2, liveAt);
				const b = signalPos(signal, t + GAIT_SPAN / 2, liveAt);
				const speed = (dist(a, b) * 1000) / GAIT_SPAN;
				const back = gaitOf(speed);
				anim = back.anim;
				phase = gaitPhase(anim);
				const easing: Blend | undefined = back.from
					? {
							anim: back.from.anim,
							phase: gaitPhase(back.from.anim),
							w: back.from.w,
							paced: true,
						}
					: undefined;
				const out = done ? 1 - smooth01((t - done.t1) / SIGNAL_EASE) : 0;
				gaitFrom =
					out > 0 && done ? over(easing, { ...endOf(done), w: out }) : easing;
				// (Which way round he comes, about to face the other way - into
				// the play from running back to his place in it: the same
				// way all through, never one way one frame and the other the
				// next as the two come to face away from each other.)
				const end = signal.moves.at(-1)!.t1 + signal.back;
				const home = liveAt(end);
				const leg = viaCorner(signal.still, home);
				const into =
					Math.atan2(
						home.y - (dist(leg, home) > 0.01 ? leg : signal.still).y,
						home.x - (dist(leg, home) > 0.01 ? leg : signal.still).x,
					) - yawTo(home, ballSpot(tl, end));
				// (The way he goes over the longer moment, as he faces in
				// the play: round a corner of the floor, not swung round it.)
				const fa = signalPos(signal, t - FACE_SPAN / 2, liveAt);
				const fb = signalPos(signal, t + FACE_SPAN / 2, liveAt);
				const going = turnRound(
					yaw,
					dist(fa, fb) > 1e-3
						? Math.atan2(fb.y - fa.y, fb.x - fa.x)
						: Math.atan2(b.y - a.y, b.x - a.x),
					smooth01(
						(speed - GAIT_BANDS[0]![2]) /
							(GAIT_BANDS[0]![3] - GAIT_BANDS[0]![2]),
					),
					into,
				);
				// (Round the way he will be going once he is round - not the
				// short way each frame, which, turning right about, is one way
				// one frame and the other the next.)
				const from = done ? done.yaw : yaw;
				const round = (done ? done.t1 : signal.t0) + SIGNAL_TURN;
				const ra = signalPos(signal, round - GAIT_SPAN / 2, liveAt);
				const rb = signalPos(signal, round + GAIT_SPAN / 2, liveAt);
				const toward = turnRound(
					from,
					going,
					smooth01((t - (done ? done.t1 : signal.t0)) / SIGNAL_TURN),
					dist(ra, rb) > 1e-3 ? Math.atan2(rb.y - ra.y, rb.x - ra.x) - from : 0,
				);
				yaw = turnRound(yaw, toward, smooth01((end - t) / SIGNAL_TURN), into);
				// Running in to make it: the way he runs, round from how he
				// was as he set off - another signal he was making, say - and
				// out of it over a moment.
				if (signal.from && t < signal.moves[0]!.t0) {
					const since = t - signal.t0;
					const prior =
						depth < 2 &&
						since < Math.max(SIGNAL_TURN, SIGNAL_EASE) &&
						signalsAt(tl, signal.t0 - 16)[i] !== undefined
							? refStates(
									tl,
									signal.t0 - 16,
									refs,
									ballSpot(tl, signal.t0 - 16),
									depth + 1,
								)[i]
							: undefined;
					yaw = turnToward(
						prior?.yaw ?? yaw,
						Math.atan2(
							signal.still.y - signal.from.y,
							signal.still.x - signal.from.x,
						),
						smooth01(since / SIGNAL_TURN),
					);
					const w = 1 - smooth01(since / SIGNAL_EASE);
					if (prior && w > 0) {
						gaitFrom = over(gaitFrom, {
							anim: prior.anim,
							phase: prior.phase,
							w,
						});
					}
				}
			}
			watching = !move;
		}
		// His head turned to keep the ball in sight, as far as a neck goes.
		let rel = (toBall - yaw) % (Math.PI * 2);
		rel -= Math.round(rel / (Math.PI * 2)) * Math.PI * 2;
		const a = (Math.abs(rel) * 180) / Math.PI;
		const look = watching
			? Math.sign(rel) *
				Math.min(a, LOOK_MOST) *
				(a <= LOOK_BACK ? 1 : Math.max(0, (180 - a) / (180 - LOOK_BACK)))
			: 0;

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
			...(Math.abs(look) > 0.5 ? { look } : {}),
			...(gaitFrom ? { from: gaitFrom } : {}),
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
	// The starting lineups: off at the end of his line, clapping his own
	// five out.
	if (tl.intro && t >= tl.intro.t0 && t < tl.intro.t1) {
		const at = coachSpot(team);
		const ours = callAt(tl.intro, t)?.team === team;
		return {
			...base,
			x: at.x,
			y: at.y,
			yaw: FACING_US,
			anim: ours ? "clap" : "crossed",
			phase: loopPhase(ours ? "clap" : "crossed", t, team * 0.5),
			moving: false,
		};
	}
	// The game over: across in front of the table to the other coach, and
	// off to his own end of the floor (see coachAfter).
	const last = tl.beats.at(-1);
	if (last?.type === "gameOver" && t >= last.actionStart + COACH_OFF) {
		return coachAfter(tl, t, m, ball, last.actionStart + COACH_OFF);
	}
	// (Not the last look round the building, the game over.)
	if (shot && shot !== tl.shots[0] && beatAt(tl, t)?.type !== "gameOver") {
		// In the huddle with his five, in the middle of them - not out on the
		// edge of it by the bench, where a man coming off walks to.
		return {
			...base,
			x: home,
			y: HUDDLE_Y,
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
	// (Facing the play, a moment behind it - see ballBearing.)
	const facing = (p: Pt) => ballBearing(tl, t, p)?.yaw ?? yawTo(p, ball);
	// Stood at his spot at tt: arms folded, bent over watching, calling out
	// - clapping a big play of his team's, or arms out at a call against
	// it.
	const standing = (tt: number): { anim: AnimName; phase: number } => {
		const roar = recentFx(tl, tt, ["roar"], 1600);
		if (roar?.team === team) {
			return { anim: "clap", phase: loopPhase("clap", tt - roar.t, 0) };
		}
		const w = recentFx(tl, tt, ["whistle"], 1600);
		if (
			w?.call &&
			w.call !== "stop" &&
			w.team !== undefined &&
			w.team !== team &&
			unit(Math.round(w.t), team) < 0.45
		) {
			return { anim: "protest", phase: (tt - w.t) / 1600 };
		}
		const idle = tensionAt(tl, tt) >= 1 ? IDLE_TENSE : IDLE;
		const anim =
			idle[
				Math.floor(unit(Math.floor(tt / PACE_MS), team * 31 + 7) * idle.length)
			]!;
		return { anim, phase: loopPhase(anim, tt, team * 0.5) };
	};
	if (since < walkMs) {
		// Pacing along the sideline: turned from the play to the way he
		// walks, and back to it as he gets there, over a moment either end -
		// his head on the play all the while - and out of how he stood.
		const p = lerp(from, to, since / walkMs);
		const toPlay = facing(p);
		const yaw = turnToward(
			toPlay,
			to.x > from.x ? 0 : Math.PI,
			smooth01(Math.min(since, walkMs - since) / COACH_TURN),
		);
		let rel = toPlay - yaw;
		rel -= Math.round(rel / (Math.PI * 2)) * Math.PI * 2;
		const look = clamp((rel * 180) / Math.PI, -LOOK_MOST, LOOK_MOST);
		const was = standing(j * PACE_MS - 1);
		const fw = 1 - smooth01(since / COACH_EASE);
		return {
			...base,
			x: p.x,
			y: p.y,
			yaw,
			anim: "walk",
			phase: (t / 1000) * 0.85,
			moving: true,
			...(Math.abs(look) > 0.5 ? { look } : {}),
			...(fw > 0 ? { from: { ...was, w: fw } } : {}),
		};
	}
	const { anim, phase } = standing(t);
	// Going from one to the next of those over a moment too, not in a frame
	// (as it was when it changed, so much of it still showing) - and
	// stopping, out of his walk.
	let changed: Blend | undefined;
	const back = Math.max(j * PACE_MS + walkMs, t - COACH_EASE);
	if (standing(back).anim !== anim) {
		let lo = back;
		let hi = t;
		for (let k = 0; k < 7; k++) {
			const mid = (lo + hi) / 2;
			if (standing(mid).anim === anim) {
				hi = mid;
			} else {
				lo = mid;
			}
		}
		changed = { ...standing(lo), w: 1 - smooth01((t - hi) / COACH_EASE) };
	}
	// (A walk too short to have got all the way into: as far as it had,
	// from how he stood before it.)
	const fw = 1 - smooth01((since - walkMs) / COACH_EASE);
	const into = 1 - smooth01(walkMs / COACH_EASE);
	const walked: Blend | undefined =
		fw > 0 && walkMs > 0
			? {
					anim: "walk",
					phase: (t / 1000) * 0.85,
					w: fw,
					...(into > 0
						? { from: { ...standing(j * PACE_MS - 1), w: into } }
						: {}),
				}
			: undefined;
	const eased = walked ? over(changed, walked) : changed;
	return {
		...base,
		x: to.x,
		y: to.y,
		yaw: facing(to),
		anim,
		phase,
		moving: false,
		...(eased && eased.w > 0 ? { from: eased } : {}),
	};
};
// The game over, a moment after the horn, the two head coaches go to each
// other in front of the table, at center court - a hand and a hug (see
// dapHug), right shoulder to right shoulder - and off to his own end of
// the floor, where his team goes off.
const COACH_OFF = 1500;
const COACH_MEET: Pt = { x: COURT_W / 2, y: 1.2 };
const COACH_PACE = 4.2;
const coachAfter = (
	tl: CourtTimeline,
	t: number,
	m: CrewMember,
	ball: Pt,
	t0: number,
): PlayerState => {
	const team = m.team;
	const u = team === 0 ? 1 : -1;
	const where = (side: Side) => {
		const st = coachState(tl, t0 - 1, { ...m, team: side }, ball);
		return { x: st.x, y: st.y };
	};
	const P = where(team);
	const Q = where(other(team));
	// (Each over to his right of the other - see HUG_SIDE.)
	const spotFor = (k: 1 | -1): Pt => ({
		x: COACH_MEET.x - (k * HUG_APART) / 2,
		y: COACH_MEET.y - (k * HUG_SIDE) / 2,
	});
	const at = spotFor(u);
	const walk = (A: Pt, B: Pt) =>
		(Math.hypot(B.x - A.x, B.y - A.y) / COACH_PACE) * 1000;
	const there = t0 + walk(P, at);
	const met = t0 + Math.max(walk(P, at), walk(Q, spotFor(-u as 1 | -1))) + 60;
	const done = met + HUG_MS;
	const off = {
		x: benchX(team) < COURT_W / 2 ? 4 : COURT_W - 4,
		y: COACH_Y - 1.4,
	};
	const gone = done + walk(at, off);
	const base = { pid: m.pid, team, shown: true, z: 0 };
	const facing = team === 0 ? 0 : Math.PI;
	const walking = (A: Pt, B: Pt, a: number, b: number): PlayerState => {
		const p = lerp(A, B, Math.min(1, (t - a) / Math.max(1, b - a)));
		return {
			...base,
			x: p.x,
			y: p.y,
			yaw: Math.atan2(B.y - A.y, B.x - A.x),
			anim: "walk",
			phase: (t / 1000) * 0.85,
			moving: true,
		};
	};
	// (Out of how he stood, or what he did before, over a moment.)
	const eased = (st: PlayerState, since: number, was: Blend): PlayerState => {
		const w = 1 - smooth01(since / COACH_EASE);
		return w > 0 ? { ...st, from: { ...was, w } } : st;
	};
	if (t < there) {
		const was = coachState(tl, t0 - 1, m, ball);
		return eased(walking(P, at, t0, there), t - t0, {
			anim: was.anim,
			phase: was.phase,
			w: 1,
		});
	}
	const walkBlend: Blend = { anim: "walk", phase: (t / 1000) * 0.85, w: 1 };
	if (t < met) {
		return eased(
			{
				...base,
				x: at.x,
				y: at.y,
				yaw: facing,
				anim: "ready",
				phase: 0,
				moving: false,
			},
			t - there,
			walkBlend,
		);
	}
	if (t < done) {
		return eased(
			{
				...base,
				x: at.x,
				y: at.y,
				yaw: facing,
				anim: "dapHug",
				phase: (t - met) / HUG_MS,
				moving: false,
			},
			t - met,
			{ anim: "ready", phase: 0, w: 1 },
		);
	}
	if (t < gone) {
		return eased(walking(at, off, done, gone), t - done, {
			anim: "dapHug",
			phase: 1,
			w: 1,
		});
	}
	return eased(
		{
			...base,
			x: off.x,
			y: off.y,
			yaw: FACING_US,
			anim: "crossed",
			phase: loopPhase("crossed", t, team * 0.5),
			moving: false,
		},
		t - gone,
		walkBlend,
	);
};

// (A coach turning to and from the way he walks, and easing in and out of
// his walk - ms.)
const COACH_TURN = 300;
const COACH_EASE = 250;

// ---- the scorer's table ---------------------------------------------------------

// Sitting at the table, turned toward the play.
const tableState = (m: CrewMember, ball: Pt, t: number): PlayerState => {
	const at = m.spot!;
	const toBall = yawTo(at, ball);
	return {
		pid: m.pid,
		team: m.team,
		shown: true,
		x: at.x,
		y: at.y,
		z: 0,
		yaw: Math.PI / 2 + clamp(toBall - Math.PI / 2, -0.7, 0.7),
		anim: "sit",
		phase: loopPhase("sit", t, m.pid * 0.29),
		moving: false,
	};
};

// ---- the photographers ------------------------------------------------------

// Down on a knee, the camera up when the play is at his end - brought up
// as it comes (over PHOTO_BAND feet of the ball's way to him either side of
// PHOTO_AT), and down as it goes, not snapped up and down in a frame.
const PHOTO_AT = 38;
const PHOTO_BAND = 8;
const photoState = (
	tl: CourtTimeline,
	t: number,
	m: CrewMember,
	ball: Pt,
): PlayerState => {
	const at = m.spot!;
	const goal = { x: m.team === 0 ? 12 : COURT_W - 12, y: COURT_H / 2 };
	const depth = m.team === 0 ? ball.x : COURT_W - ball.x;
	const up = arenaShotAt(tl, t)
		? 0
		: smooth01((PHOTO_AT + PHOTO_BAND / 2 - depth) / PHOTO_BAND);
	const anim: AnimName = up > 0 ? "kneelShoot" : "kneel";
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
		...(up > 0 && up < 1
			? {
					from: {
						anim: "kneel",
						phase: loopPhase("kneel", t, m.pid * 0.31),
						w: 1 - up,
					},
				}
			: {}),
	};
};

// The pre-game show: the two on air turned to their camera, it on them.
const mediaState = (m: CrewMember, t: number): PlayerState => {
	const k = -51 - m.pid;
	const at = m.spot!;
	const cam = MEDIA[2]!.at;
	const anim = MEDIA[k]!.anim;
	return {
		pid: m.pid,
		team: m.team,
		shown: true,
		x: at.x,
		y: at.y,
		z: 0,
		yaw:
			anim === "shootStand"
				? yawTo(at, { x: (MEDIA[0]!.at.x + MEDIA[1]!.at.x) / 2, y: 3.2 })
				: yawTo(at, cam),
		anim,
		phase: loopPhase(anim, t, m.pid * 0.37),
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
	// (The camera up to his eye.)
	if (st.anim !== "kneelShoot" || (st.from?.w ?? 0) > 0.5) {
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
		} else if (m.role === "table") {
			states.push(tableState(m, ball, t));
		} else if (m.role === "media") {
			if (tl.warmup) {
				states.push(mediaState(m, t));
			}
		} else if (m.role === "mop") {
			states.push(mopState(tl, t, m));
		} else if (m.role === "staff") {
			states.push(staffState(t, m));
		} else if (m.role === "dance") {
			const st = danceState(tl, t, m);
			if (st) {
				states.push(st);
			}
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
