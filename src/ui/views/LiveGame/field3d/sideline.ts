import { makeCourtRng } from "../courtRng.ts";
import { cheerAt } from "./crowd.ts";
import type { FieldTimeline, Play } from "./director.ts";
import type { FieldMoment } from "./evaluate.ts";
import {
	BENCH_OFF,
	COACH_LINE,
	dist,
	FIELD_L,
	FIELD_W,
	MID_X,
	TEAM_AREA,
	type Pt,
	type Pt3,
	type Side,
} from "./geometry.ts";
import type { RosterMan } from "./personnel.ts";
import { netBallAt, netKickAt, netKickOf, type NetKick } from "./netKicks.ts";
import { liftOf, strideOf, type Move } from "./poses.ts";
import type { FieldPlayerState } from "./sprite.ts";

// THE SIDELINES: each team on its own, the home team across the field and
// the visitors on the near side under the press box - between the 25-yard
// lines (the NFL's team area), behind the coaches' yellow line twelve feet
// off the field. The whole roster is there, every man not in for the snap:
// packed in along the front of the team area, shoulder to shoulder, the
// crowd thickest in the middle - the backups of the unit out on the field
// up front - and the unit just off it back on the benches thirty feet off,
// most of them sat down, a position coach or two stood in front of them.
// The kickers are down at the end by their net; the head coach and his
// coordinators up in front, toward midfield; the staff - trainers,
// equipment men - about the back. Each watches the ball. A score, a
// takeaway, a big play, and the side that made it is up and jumping.

// The benches: along the middle of the team area, a few long ones end to
// end.
export const BENCHES = [
	[MID_X - 52, MID_X - 28],
	[MID_X - 24, MID_X],
	[MID_X + 4, MID_X + 28],
] as const;
// Which sideline a team has (0 the far one).
export const sidelineOf = (team: Side): 0 | 1 => (team === 1 ? 0 : 1);
// The kicking net: near the end of the team area - the left end across the
// field, the right on the near side - this far back off the sideline, this
// wide, its face to the field (kicked into away from it).
export const netX = (line: 0 | 1) =>
	line === 0 ? TEAM_AREA[0] + 9 : TEAM_AREA[1] - 9;
export const NET_OFF = 27;
export const NET_W = 7;
const yOf = (line: 0 | 1, off: number) => (line === 0 ? -off : FIELD_W + off);
// Nobody but the kickers stood in front of it: this far either side of it,
// out to this far off the sideline.
const NET_LANE = NET_W / 2 + 4;
const NET_LANE_FROM = NET_OFF - 16;

export type SidelineMan = {
	id: number;
	team: Side;
	// What he is: a player on the offense, the defense, a kicker - or a
	// coach (and which), or one of the staff.
	unit: Unit;
	coach?: number;
	// Where he stands all game, for those with a place of their own (the
	// staff, the kickers), and how he stands.
	at?: Pt;
	move: Move;
	// His place among the rest in the crowd (0 first), and whether he takes
	// a seat while his unit rests.
	rank: number;
	sits: boolean;
	// How far he will turn from facing the field to watch the ball.
	turn: number;
	// How he takes a big play of his side's: jumps, an arm up, both.
	cheer: Move;
	delay: number;
	// How ready he is to have his helmet off (0 to 1: the lower, the
	// sooner).
	bare?: number;
	// A kicker: what he kicks into the net, when he does (see netKicks.ts).
	kicks?: "place" | "punt";
};
type Unit = "offense" | "defense" | "kicker" | "coach" | "staff";

const OFFENSE = new Set(["QB", "RB", "FB", "WR", "TE", "OL"]);
const KICKERS = new Set(["K", "P"]);
const STANDING: Move[] = [
	"stand",
	"stand",
	"stand",
	"onHips",
	"armsFolded",
	"hands",
];
const CHEERS: Move[] = ["leap", "leap", "ballUp", "flex"];
// The crowd along the front: shoulder to shoulder this far apart, rows this
// far behind one another (a row back half a man over), at most this many
// deep - its slots taken from the middle out, a row further back worth this
// much further out (feet) - each man this far off his slot at most.
const COL = 2.9;
const ROW = 2.5;
const ROWS = 4;
const FRONT = COACH_LINE + 1.6;
const ROW_WORTH = 24;
const JITTER = 0.5;
// Nobody this near the head coach along the field, under the cooler at the
// end (see shower.ts).
const LANE = 4.4;
// The unit just off the field: this share of it sit down.
const SITS = 0.7;
const SEAT = 2.6;
// The coaches - the head coach, his coordinators, the position coaches -
// and the staff, ids of their own.
export const COACHES = 9;
export const coachId = (team: Side, i: number) => -100 - team * 10 - i;
export const STAFF = 6;
export const staffId = (team: Side, i: number) => -120 - team * 10 - i;
// Plays both units watch on their feet.
const KICKS = new Set(["kickoff", "onside", "punt", "fieldGoal", "extraPoint"]);

const unitOf = (pos: string): Unit =>
	KICKERS.has(pos) ? "kicker" : OFFENSE.has(pos) ? "offense" : "defense";

// (Over out of the kickers' way, if in front of their net.)
const offNet = (line: 0 | 1, p: Pt): Pt => {
	const along = p.x - netX(line);
	const off = line === 0 ? -p.y : p.y - FIELD_W;
	return Math.abs(along) < NET_LANE && off > NET_LANE_FROM && off < NET_OFF + 2
		? { x: netX(line) + (along < 0 ? -1 : 1) * (NET_LANE + 1), y: p.y }
		: p;
};

export const sidelineFor = (
	roster: RosterMan[],
	gid: number,
): SidelineMan[] => {
	const out: SidelineMan[] = [];
	for (const team of [0, 1] as Side[]) {
		const line = sidelineOf(team);
		const lean = line === 0 ? 1 : -1;
		const men = roster
			.filter((p) => p.team === team)
			.sort((a, b) => a.pid - b.pid);
		const rng = makeCourtRng(`sideline|${gid}|${team}`);
		const watch = (r: () => number) => 0.5 + r() * 0.7;
		const common = (r: () => number) => ({
			turn: watch(r),
			cheer: CHEERS[Math.floor(r() * CHEERS.length)]!,
			delay: r() * 500,
		});
		let kickers = 0;
		for (const p of men) {
			const r = makeCourtRng(`sideline|${gid}|${p.pid}`);
			const unit = unitOf(p.pos);
			out.push({
				id: p.pid,
				team,
				unit,
				// (The kickers by their net.)
				...(unit === "kicker"
					? {
							at: {
								x: netX(line) + lean * (NET_W / 2 + 3 + kickers++ * 3.5),
								y: yOf(line, FRONT + 2 + r() * 2),
							},
							...(p.pos === "K" || p.pos === "P"
								? { kicks: p.pos === "K" ? "place" : "punt" }
								: {}),
						}
					: {}),
				move:
					unit === "kicker"
						? "stand"
						: STANDING[Math.floor(r() * STANDING.length)]!,
				rank: r(),
				sits: r() < SITS,
				...common(r),
				bare: makeCourtRng(`bare|${gid}|${p.pid}`)(),
			});
		}
		for (let i = 0; i < COACHES; i++) {
			out.push({
				id: coachId(team, i),
				team,
				unit: "coach",
				coach: i,
				move: i === 0 ? "armsFolded" : rng() < 0.5 ? "stand" : "onHips",
				rank: rng(),
				sits: false,
				turn: 0.9,
				cheer: "ballUp",
				delay: rng() * 300,
			});
		}
		// (The staff about the back of it, behind the crowd and the benches,
		// and out at the ends.)
		for (let i = 0; i < STAFF; i++) {
			const back = i % 2 === 0;
			out.push({
				id: staffId(team, i),
				team,
				unit: "staff",
				at: offNet(
					line,
					back
						? {
								x: MID_X + (rng() - 0.5) * 90,
								y: yOf(line, BENCH_OFF + 3 + rng() * 3),
							}
						: {
								x:
									MID_X +
									(i % 4 === 1 ? -1 : 1) * (52 + rng() * 14) +
									(rng() - 0.5) * 4,
								y: yOf(line, FRONT + 4 + rng() * 8),
							},
				),
				move: rng() < 0.6 ? "stand" : "armsFolded",
				rank: rng(),
				sits: false,
				turn: 0.7,
				cheer: "ballUp",
				delay: rng() * 300,
			});
		}
	}
	return out;
};

// Where the head coach stands for a play: up front, about midfield.
export const headCoachAt = (team: Side, playI: number): Pt => {
	const r = makeCourtRng(`coach|${coachId(team, 0)}|${playI}`);
	return {
		x: MID_X + (r() - 0.5) * 16,
		y: yOf(sidelineOf(team), COACH_LINE - 2 - r() * 1.4),
	};
};

// Helmets off on the sideline - most of the unit sat on the benches, some
// of it stood, a few of the men who could be sent in, the kickers - and
// whether a man has his.
const BARE = { seated: 0.75, resting: 0.4, next: 0.12, kicker: 0.5 };
const bareOf = (s: SidelineMan, share: number) =>
	s.bare !== undefined && s.bare < share;

// Where a man is for a play, and how: stood or sat, and facing the field
// (or, a coach at the bench, the men on it).
type Spot = {
	at: Pt;
	move: Move;
	seated?: boolean;
	facing?: number;
	bare?: boolean;
};

const layouts = new WeakMap<SidelineMan[], WeakMap<Play, Map<number, Spot>>>();
const layoutFor = (men: SidelineMan[], play: Play): Map<number, Spot> => {
	let byPlay = layouts.get(men);
	if (!byPlay) {
		byPlay = new WeakMap();
		layouts.set(men, byPlay);
	}
	let spots = byPlay.get(play);
	if (!spots) {
		spots = lay(men, play);
		byPlay.set(play, spots);
	}
	return spots;
};

// MAKING WAY. A man run on out of bounds into the team area - pulled up
// there, shoved there, gone on after a catch at the line: those stood
// where he comes step back out of his way as he does, and back after, no
// nearer him than CLEAR (feet). Worked out once a play, every WAY_MS, eased
// over WAY_EASE samples either side (twice over); quicker about it than
// WAY_STEP (feet a second), they step it.
const CLEAR = 4.5;
const WAY_MS = 100;
const WAY_EASE = 4;
const WAY_STEP = 1.5;
type Way = {
	t0: number;
	dx: Float32Array;
	dy: Float32Array;
	walked: Float32Array;
};
const ways = new WeakMap<Map<number, Spot>, Map<number, Way>>();
const waysFor = (spots: Map<number, Spot>, play: Play): Map<number, Way> => {
	let got = ways.get(spots);
	if (got) {
		return got;
	}
	got = new Map();
	ways.set(spots, got);
	const past = (p: Pt) => p.y < -1 || p.y > FIELD_W + 1;
	const outs = play.parts.filter(
		(p) => p.pid >= 0 && p.legs.some((l) => past(l.from) || past(l.to)),
	);
	if (outs.length === 0) {
		return got;
	}
	const t0 = play.t0;
	const n = Math.max(2, Math.ceil((play.t1 - t0) / WAY_MS) + 1);
	const tracks = outs.map((p) => trackOf(p.legs, t0, n));
	for (const [id, spot] of spots) {
		if (spot.seated) {
			continue;
		}
		const at = spot.at;
		let dx: Float32Array | undefined;
		let dy: Float32Array | undefined;
		for (let k = 0; k < n; k++) {
			for (const tr of tracks) {
				const ex = at.x - tr.x[k]!;
				const ey = at.y - tr.y[k]!;
				if (Math.abs(ex) >= CLEAR || Math.abs(ey) >= CLEAR) {
					continue;
				}
				const d = Math.hypot(ex, ey);
				if (d >= CLEAR) {
					continue;
				}
				dx ??= new Float32Array(n);
				dy ??= new Float32Array(n);
				// (Right where he is: straight back, off the field.)
				const ux = d > 1e-3 ? ex / d : 0;
				const uy = d > 1e-3 ? ey / d : at.y < 0 ? -1 : 1;
				dx[k] = dx[k]! + ux * (CLEAR - d);
				dy[k] = dy[k]! + uy * (CLEAR - d);
			}
		}
		if (dx && dy) {
			const sx = ease(ease(dx));
			const sy = ease(ease(dy));
			const walked = new Float32Array(n);
			for (let k = 1; k < n; k++) {
				walked[k] =
					walked[k - 1]! + Math.hypot(sx[k]! - sx[k - 1]!, sy[k]! - sy[k - 1]!);
			}
			got.set(id, { t0, dx: sx, dy: sy, walked });
		}
	}
	return got;
};

// Where a man is every WAY_MS from t0, his legs as they go.
const trackOf = (
	legs: Play["parts"][number]["legs"],
	t0: number,
	n: number,
) => {
	const x = new Float32Array(n);
	const y = new Float32Array(n);
	let k = 0;
	for (let s = 0; s < n; s++) {
		const t = t0 + s * WAY_MS;
		while (k + 1 < legs.length && legs[k + 1]!.t0 <= t) {
			k++;
		}
		const l = legs[k]!;
		const u =
			l.t1 > l.t0 ? Math.min(1, Math.max(0, (t - l.t0) / (l.t1 - l.t0))) : 1;
		x[s] = l.from.x + (l.to.x - l.from.x) * u;
		y[s] = l.from.y + (l.to.y - l.from.y) * u;
	}
	return { x, y };
};

// Each sample the mean of those either side of it.
const ease = (a: Float32Array): Float32Array => {
	const out = new Float32Array(a.length);
	for (let s = 0; s < a.length; s++) {
		let sum = 0;
		let k = 0;
		for (
			let j = Math.max(0, s - WAY_EASE);
			j <= Math.min(a.length - 1, s + WAY_EASE);
			j++
		) {
			sum += a[j]!;
			k++;
		}
		out[s] = sum / k;
	}
	return out;
};

// How far over he is at t, how far he has stepped, and how fast (feet a
// second) and which way he is going about it.
const wayAt = (w: Way, t: number) => {
	const f = Math.min(w.dx.length - 1, Math.max(0, (t - w.t0) / WAY_MS));
	const i = Math.min(w.dx.length - 2, Math.floor(f));
	const u = f - i;
	const vx = w.dx[i + 1]! - w.dx[i]!;
	const vy = w.dy[i + 1]! - w.dy[i]!;
	return {
		x: w.dx[i]! + vx * u,
		y: w.dy[i]! + vy * u,
		walked: w.walked[i]! + (w.walked[i + 1]! - w.walked[i]!) * u,
		fps: (Math.hypot(vx, vy) * 1000) / WAY_MS,
		way: Math.atan2(vy, vx),
	};
};

// The team area laid out for a play, the men not in it on their sideline.
const lay = (men: SidelineMan[], play: Play): Map<number, Spot> => {
	const out = new Map<number, Spot>();
	const on = onField(play);
	const kick = KICKS.has(play.kind);
	for (const team of [0, 1] as Side[]) {
		const line = sidelineOf(team);
		const lean = line === 0 ? 1 : -1;
		const toField = line === 0 ? Math.PI / 2 : -Math.PI / 2;
		const mine = men.filter((s) => s.team === team && !on.has(s.id));
		const working = play.offense === team ? "offense" : "defense";
		const resting = working === "offense" ? "defense" : "offense";
		for (const s of mine) {
			if (s.at) {
				out.set(s.id, {
					at: s.at,
					move: s.move,
					...(bareOf(s, BARE.kicker) ? { bare: true } : {}),
				});
			}
		}
		// The unit just off, on the benches - from the middle of them out.
		const seats: Pt[] = [];
		for (const [x0, x1] of BENCHES) {
			for (let x = x0 + SEAT / 2; x < x1; x += SEAT) {
				seats.push({ x, y: yOf(line, BENCH_OFF + 0.7) });
			}
		}
		const benchMid = (BENCHES[0][0] + BENCHES.at(-1)![1]) / 2;
		seats.sort((a, b) => Math.abs(a.x - benchMid) - Math.abs(b.x - benchMid));
		const byRank = (a: SidelineMan, b: SidelineMan) => a.rank - b.rank;
		const sitting = kick
			? []
			: mine
					.filter((s) => s.unit === resting && s.sits)
					.sort(byRank)
					.slice(0, seats.length);
		// (Spread along the benches, a gap here and there, in the order of
		// the seats they would take all game.)
		const taken = seats
			.slice(0, Math.min(seats.length, Math.ceil(sitting.length * 1.25)))
			.sort((a, b) => a.x - b.x);
		const every = taken.length / Math.max(1, sitting.length);
		const benched = [...sitting].sort(
			(a, b) => (a.id % 97) - (b.id % 97) || a.id - b.id,
		);
		benched.forEach((s, k) => {
			out.set(s.id, {
				at: taken[Math.min(taken.length - 1, Math.floor(k * every))]!,
				move: "sit",
				seated: true,
				...(bareOf(s, BARE.seated) ? { bare: true } : {}),
			});
		});
		// The coaches: the head coach and his coordinators up front, the
		// position coaches of the unit resting stood before its bench, the
		// rest of them in among the crowd's front.
		const coaches = mine.filter((s) => s.unit === "coach");
		const seated = benched.map((s) => out.get(s.id)!.at.x);
		const benchX =
			seated.length > 0
				? seated.reduce((a, b) => a + b, 0) / seated.length
				: benchMid;
		for (const c of coaches) {
			const i = c.coach ?? 0;
			const r = makeCourtRng(`coach|${c.id}|${play.i}`);
			if (i === 0) {
				out.set(c.id, { at: headCoachAt(team, play.i), move: c.move });
			} else if (i <= 2) {
				out.set(c.id, {
					at: {
						x: MID_X + lean * (i === 1 ? -1 : 1) * (14 + r() * 12),
						y: yOf(line, COACH_LINE - 1.6 - r() * 1.6),
					},
					move: c.move,
				});
			} else if (i <= 4 && seated.length > 0) {
				// (Facing the bench, his back to the field.)
				out.set(c.id, {
					at: {
						x: benchX + (i === 3 ? -1 : 1) * (5 + r() * 6),
						y: yOf(line, BENCH_OFF - 3.4 - r()),
					},
					move: r() < 0.5 ? "stand" : "hands",
					facing: toField + Math.PI,
				});
			}
		}
		// Everyone else stood in the crowd: first the backups of the unit
		// out on the field, then the rest of the coaches, then the men just
		// off it - each in his order - into its slots from the middle out.
		const order = (s: SidelineMan) =>
			s.unit === working ? 0 : s.unit === "coach" ? 1 : 2;
		const standing = mine
			.filter((s) => !out.has(s.id))
			.sort((a, b) => order(a) - order(b) || byRank(a, b));
		const slots: { x: number; y: number; cost: number }[] = [];
		for (let row = 0; row < ROWS; row++) {
			for (let col = -30; col <= 30; col++) {
				const x = MID_X + (col + (row % 2) * 0.5) * COL;
				if (x < TEAM_AREA[0] + 4 || x > TEAM_AREA[1] - 4) {
					continue;
				}
				slots.push({
					x,
					y: yOf(line, FRONT + row * ROW),
					cost: Math.abs(x - MID_X) + row * ROW_WORTH,
				});
			}
		}
		// (A lane left through them for the men with the cooler for the
		// head coach: see shower.ts.)
		const shower = play.postgame?.shower;
		const lane =
			shower?.team === team
				? slots.filter((p) => Math.abs(p.x - shower.at.x) >= LANE)
				: slots;
		lane.sort((a, b) => a.cost - b.cost);
		standing.forEach((s, k) => {
			const slot = lane[k];
			if (!slot) {
				return;
			}
			const r = makeCourtRng(`spot|${s.id}`);
			// (Helmet in hand: not stood with his arms folded or on his
			// hips.)
			const bare = bareOf(s, s.unit === working ? BARE.next : BARE.resting);
			out.set(s.id, {
				at: {
					x: slot.x + (r() - 0.5) * 2 * JITTER,
					y: slot.y + (r() - 0.5) * 2 * JITTER,
				},
				move:
					bare && (s.move === "armsFolded" || s.move === "onHips")
						? "stand"
						: s.move,
				...(bare ? { bare: true } : {}),
			});
		});
	}
	return out;
};

// The home team's players out of their tunnel before the game, not yet on
// their sideline (see tunnel.ts).
const runningOut = (s: SidelineMan, play: Play, t: number) =>
	play.runOut !== undefined &&
	t < play.runOut.cut &&
	s.team === play.runOut.team &&
	s.unit !== "coach" &&
	s.unit !== "staff";

// Who is on the field for a play (a head coach out to shake hands too):
// everyone else is on his sideline - but for the captains out for the coin
// toss, until it is done.
const inFor = new WeakMap<Play, Set<number>>();
const onField = (play: Play) => {
	let s = inFor.get(play);
	if (!s) {
		s = new Set([...play.parts, ...(play.coaches ?? [])].map((p) => p.pid));
		inFor.set(play, s);
	}
	return s;
};

// How long one jump takes (ms), and how far into a cheer the side's men are
// up for it.
const LEAP_MS = 900;
const UP_LEVEL = 0.55;

// The game over (see postgame.ts): most of each side's players out onto the
// field after the men on it, from a while after - the men sat down and the
// kickers the last of them - at a walk, to stand about round the middle on
// their own side of it: along the field about as far off the middle as
// they stood off midfield, a share of it - the near side's off to either
// side and back a way, not in front of it all - a few clapping as they get
// there. Some of them at a jog; all of them there by a while after the
// head coaches shake hands.
const OUT_SHARE = 0.6;
const OUT_AFTER = 1800;
const OUT_SPREAD = 4200;
const OUT_LATE = 1000;
const OUT_FPS: [number, number] = [4.2, 5.6];
const OUT_JOG = 0.35;
const JOG_FPS = 7.5;
const OUT_BY = 1500;
const OUT_DEEP: [number, number] = [26, 48];
const OUT_BACK = 12;
const OUT_ALONG = 0.55;
const OUT_WIDE = 16;
const OUT_CLEAR = 20;
const OUT_CLAP = 0.3;
const CLAP_MS = 1400;
// Won on the last play: the winners' bench out onto the field at once, this
// share of it, at a run (these paces), round the men all over the man who
// won it - this far off him - jumping as they get there; the beaten side's
// out after the rest of it, later still. (A playoff round won, the same at
// once, at a run: see Postgame clinch.)
const RUSH_SHARE = 0.85;
const RUSH_AFTER = 250;
const RUSH_SPREAD = 1400;
const RUSH_FPS: [number, number] = [12.5, 17];
const RUSH_RING: [number, number] = [7, 30];
const RUSH_LEAPS = 2;
const BEATEN_LATE = 2500;
// Up off the bench into it, and round to face the middle, over this long.
const RISE_MS = 260;
const TURN_MS = 380;

type Out = {
	leave: number;
	from: Pt;
	to: Pt;
	arrive: number;
	gait: "walk" | "jog" | "run";
	// Jumps he takes when he gets there, claps then; what he turns to.
	leaps: number;
	claps: boolean;
	face: Pt;
};
const outs = new WeakMap<Play, Map<number, Out | null>>();
const outOf = (s: SidelineMan, spot: Spot, play: Play, at: Pt): Out | null => {
	let byMan = outs.get(play);
	if (!byMan) {
		byMan = new Map();
		outs.set(play, byMan);
	}
	let o = byMan.get(s.id);
	if (o === undefined) {
		o = null;
		const r = makeCourtRng(`out|${s.id}`);
		const post = play.postgame!;
		const mob = post.mob;
		const player = s.unit !== "coach" && s.unit !== "staff";
		// (A playoff round won: the winners' men out to stand about the
		// middle all the same - but at once, at a run, jumping as they get
		// there.)
		const rushing = !!post.clinch && s.team === post.winner;
		if (mob && s.team === post.winner) {
			if (player && r() < RUSH_SHARE) {
				const leave =
					post.from +
					RUSH_AFTER +
					r() * RUSH_SPREAD +
					(spot.seated ? RISE_MS * 2 : 0);
				const a =
					Math.atan2(spot.at.y - mob.at.y, spot.at.x - mob.at.x) +
					(r() - 0.5) * 2.4;
				const ring = RUSH_RING[0] + r() * (RUSH_RING[1] - RUSH_RING[0]);
				const to = {
					x: Math.min(FIELD_L - 2, Math.max(2, mob.at.x + Math.cos(a) * ring)),
					y: Math.min(FIELD_W - 2, Math.max(2, mob.at.y + Math.sin(a) * ring)),
				};
				const fps = RUSH_FPS[0] + r() * (RUSH_FPS[1] - RUSH_FPS[0]);
				o = {
					leave,
					from: spot.at,
					to,
					arrive: leave + (dist(spot.at, to) / fps) * 1000,
					gait: "run",
					leaps: RUSH_LEAPS + (r() < 0.5 ? 1 : 0),
					claps: false,
					face: mob.at,
				};
			}
		} else if (player && r() < (rushing ? RUSH_SHARE : OUT_SHARE)) {
			const near = spot.at.y > 0;
			const leave = rushing
				? post.from +
					RUSH_AFTER +
					r() * RUSH_SPREAD +
					(spot.seated ? RISE_MS * 2 : 0)
				: post.from +
					OUT_AFTER +
					r() * OUT_SPREAD +
					(spot.seated || s.unit === "kicker" ? OUT_LATE : 0) +
					(mob ? BEATEN_LATE : 0) +
					// (Watching the head coach under the cooler first.)
					(post.shower && s.team === post.shower.team
						? post.shower.until - post.from
						: 0);
			let along = (spot.at.x - MID_X) * OUT_ALONG + (r() - 0.5) * 2 * OUT_WIDE;
			if (near) {
				along = (along < 0 ? -1 : 1) * (OUT_CLEAR + Math.abs(along));
			}
			const deep =
				OUT_DEEP[0] + r() * (OUT_DEEP[1] - OUT_DEEP[0]) + (near ? OUT_BACK : 0);
			const to = { x: at.x + along, y: at.y + (near ? deep : -deep) };
			const far = dist(spot.at, to);
			const fps = rushing
				? RUSH_FPS[0] + r() * (RUSH_FPS[1] - RUSH_FPS[0])
				: Math.max(
						r() < OUT_JOG
							? JOG_FPS
							: OUT_FPS[0] + r() * (OUT_FPS[1] - OUT_FPS[0]),
						far / (Math.max(1000, post.shake + OUT_BY - leave) / 1000),
					);
			o = {
				leave,
				from: spot.at,
				to,
				arrive: leave + (far / fps) * 1000,
				gait: fps < 6.5 ? "walk" : fps < 12.5 ? "jog" : "run",
				leaps: rushing ? RUSH_LEAPS + (r() < 0.5 ? 1 : 0) : 0,
				claps: !rushing && r() < OUT_CLAP,
				face: at,
			};
		}
		byMan.set(s.id, o);
	}
	return o;
};

const turnTo = (a: number, b: number, u: number) => {
	const d = Math.atan2(Math.sin(b - a), Math.cos(b - a));
	return a + d * Math.min(1, Math.max(0, u));
};

// Out on the field at t, on his way or there.
const outAt = (
	s: SidelineMan,
	spot: Spot,
	o: Out,
	t: number,
): FieldPlayerState => {
	const u = Math.min(1, (t - o.leave) / Math.max(1, o.arrive - o.leave));
	const x = o.from.x + (o.to.x - o.from.x) * u;
	const y = o.from.y + (o.to.y - o.from.y) * u;
	const way = Math.atan2(o.to.y - o.from.y, o.to.x - o.from.x);
	const gait = o.gait;
	const stride = strideOf(gait);
	const offset = (Math.abs(s.id) * 0.618) % 1;
	const walked = (dist(o.from, o.to) * u) / stride + offset;
	const base = {
		id: s.id,
		team: s.team,
		x,
		y,
		z: 0,
		...(spot.bare ? { bare: true as const } : {}),
	};
	if (u < 1) {
		const up = t - o.leave;
		return {
			...base,
			yaw: way,
			move: gait,
			phase: walked,
			...(up < RISE_MS
				? { from: { move: spot.move, phase: 0, w: 1 - up / RISE_MS } }
				: {}),
		};
	}
	const since = t - o.arrive;
	const leaping = since < o.leaps * LEAP_MS;
	const clapping = !leaping && o.claps && since < CLAP_MS;
	const phase = leaping
		? (since / LEAP_MS) % 1
		: clapping
			? since / CLAP_MS
			: 0;
	return {
		...base,
		z: leaping ? liftOf("leap", phase) : 0,
		yaw: turnTo(way, Math.atan2(o.face.y - y, o.face.x - x), since / TURN_MS),
		move: leaping ? "leap" : clapping ? "clap" : "stand",
		phase,
		...(since < RISE_MS
			? { from: { move: gait, phase: walked, w: 1 - since / RISE_MS } }
			: {}),
	};
};

// A kicker's kick into his net on a play, if he takes one.
const netKicks = new WeakMap<Play, Map<number, NetKick | null>>();
const netKickFor = (s: SidelineMan, spot: Spot, play: Play): NetKick | null => {
	if (!s.kicks || !s.at || spot.at !== s.at) {
		return null;
	}
	let byMan = netKicks.get(play);
	if (!byMan) {
		byMan = new Map();
		netKicks.set(play, byMan);
	}
	let k = byMan.get(s.id);
	if (k === undefined) {
		const line = sidelineOf(s.team);
		k =
			play.offense === s.team
				? netKickOf(
						s.kicks,
						s.id,
						play,
						{ x: netX(line), y: yOf(line, NET_OFF) },
						{ x: 0, y: line === 0 ? -1 : 1 },
						s.at,
					)
				: null;
		byMan.set(s.id, k);
	}
	return k;
};

// The balls kicked into the nets at t.
export const netBallsAt = (
	men: SidelineMan[],
	m: FieldMoment,
): { at: Pt3; axis?: Pt3; stood?: "tee" }[] => {
	const play = m.play;
	if (!play) {
		return [];
	}
	const spots = layoutFor(men, play);
	const out: { at: Pt3; axis?: Pt3; stood?: "tee" }[] = [];
	for (const s of men) {
		const spot =
			s.kicks && !runningOut(s, play, m.t) ? spots.get(s.id) : undefined;
		const k = spot ? netKickFor(s, spot, play) : null;
		const b = k ? netBallAt(k, m.t) : null;
		if (b) {
			out.push(b);
		}
	}
	return out;
};

export const sidelineAt = (
	men: SidelineMan[],
	tl: FieldTimeline,
	m: FieldMoment,
): FieldPlayerState[] => {
	const play = m.play;
	if (!play) {
		return [];
	}
	const out: FieldPlayerState[] = [];
	const spots = layoutFor(men, play);
	const making = waysFor(spots, play);
	const cheer = cheerAt(tl, m.t);
	const ball = m.focus;
	const post = play.postgame;
	const toss = play.toss && m.t < play.toss.cut ? play.toss.men : [];
	for (const s of men) {
		const spot = spots.get(s.id);
		if (!spot || toss.some((p) => p.pid === s.id) || runningOut(s, play, m.t)) {
			continue;
		}
		const o = post ? outOf(s, spot, play, post.at) : null;
		if (o && post && m.t >= o.leave) {
			out.push(outAt(s, spot, o, m.t));
			continue;
		}
		// (A kicker out to his net has his helmet on, all that play.)
		const kick = netKickFor(s, spot, play);
		const kicking = kick ? netKickAt(kick, s.id, m.t) : null;
		if (kicking) {
			out.push({ id: s.id, team: s.team, ...kicking });
			continue;
		}
		// (Out of the way of a man run out of bounds into them.)
		const way = making.get(s.id);
		const off = way ? wayAt(way, m.t) : undefined;
		const at = off ? { x: spot.at.x + off.x, y: spot.at.y + off.y } : spot.at;
		const line = at.y < 0 ? 0 : 1;
		const facing = spot.facing ?? (line === 0 ? Math.PI / 2 : -Math.PI / 2);
		let yaw = facing;
		if (ball && !spot.seated && spot.facing === undefined) {
			let d = Math.atan2(ball.y - at.y, ball.x - at.x) - facing;
			d = Math.atan2(Math.sin(d), Math.cos(d));
			yaw = facing + Math.max(-s.turn, Math.min(s.turn, d));
		}
		let move = spot.move;
		let phase = 0;
		if (off && off.fps > WAY_STEP) {
			// (Stepping it: back, across, or on - as he faces.)
			const c = Math.cos(off.way - yaw);
			move = c < -0.5 ? "back" : c > 0.5 ? "walk" : "slide";
			phase = off.walked / strideOf(move);
		}
		let arms: FieldPlayerState["arms"];
		if (cheer && cheer.side === s.team && cheer.level > UP_LEVEL) {
			const e = cheer.since - s.delay;
			if (e > 0) {
				if (spot.seated) {
					arms = { move: "ballUp", phase: 0 };
				} else {
					move = s.cheer;
					phase =
						s.cheer === "leap" || s.cheer === "flex"
							? (e / (s.cheer === "leap" ? LEAP_MS : 2 * LEAP_MS)) % 1
							: 0;
				}
			}
		}
		out.push({
			id: s.id,
			team: s.team,
			x: at.x,
			y: at.y,
			z: liftOf(move, phase),
			yaw,
			move,
			phase,
			...(arms ? { arms } : {}),
			...(spot.bare && !kick ? { bare: true as const } : {}),
		});
	}
	return out;
};
