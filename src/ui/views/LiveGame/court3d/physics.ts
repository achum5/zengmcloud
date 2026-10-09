import {
	attackDir,
	COURT_H,
	RIM_R,
	RIM_Z,
	rimX,
	type Pt,
	type Pt3,
	type Side,
} from "./geometry.ts";

// THE BALL AT THE RIM.
//
// A shot's last few feet - off the iron, off the glass, down through the net
// - worked out the way a ball really moves: falling under gravity, and coming
// off whatever it hits with some of its speed, the way it came in mirrored
// about the surface. Nothing here is drawn from chance: given where and how
// fast it comes in, the bounces are what they are. (And nothing but adding,
// multiplying, dividing and square roots, which every device does to the
// same last bit - so every device sees the same bounces.)
//
// Which bounces a shot gets is a matter of where it was aimed, and the sim
// has already said how it ends - in or out, rimmed out or bricked, and who
// comes down with the rebound. So a shot is tried a few dozen ways, each a
// shade off the last, and the one that plays out the way the sim says is the
// one that is shown (see findShot).

const GRAVITY = 32.2;
// A basketball is 9.4 inches across.
const BALL_R = 0.39;
// The step the ball is moved on by (seconds), and how often where it is gets
// written down (ms).
const STEP = 0.002;
export const SAMPLE_MS = 10;
// The rim: a ring of steel bar, five-eighths of an inch thick, round the
// eighteen inches of the hoop.
const BAR = 0.026;
const RING = RIM_R + BAR;
const TOUCH = BALL_R + BAR;
// How much of its speed into the rim (or the glass) it keeps coming off it,
// and of its speed along it.
const RIM_BOUNCE = 0.55;
const RIM_SLIDE = 0.9;
const BOARD_BOUNCE = 0.62;
const BOARD_SLIDE = 0.88;
// The glass: its face fifteen inches behind the middle of the hoop, six feet
// across and from nine and a half feet up to thirteen.
const BOARD_BACK = 1.25;
const BOARD_HALF = 3;
const BOARD_LOW = 9.5;
const BOARD_HIGH = 13;
// The net hangs a foot and a half, narrowing to about a foot across, and
// slows whatever goes down through it.
const NET_DEPTH = 1.55;
const NET_LOW_R = RIM_R * 0.6;
const NET_DRAG = 7;

export type Touch = { t: number; at: Pt3; rim: boolean; hard: number };

// How far the ball is from touching the rim or the glass (feet; less than
// nothing, into it).
const CLEAR = 0.15;
const gapOf = (side: Side, q: Pt3): number => {
	const cx = rimX(side);
	const cy = COURT_H / 2;
	const hx = q.x - cx;
	const hy = q.y - cy;
	const dr = Math.sqrt(hx * hx + hy * hy) - RING;
	const dz = q.z - RIM_Z;
	const rim = Math.sqrt(dr * dr + dz * dz) - TOUCH;
	const off = (q.x - (cx + attackDir(side) * BOARD_BACK)) * -attackDir(side);
	return Math.abs(q.y - cy) < BOARD_HALF + BALL_R &&
		q.z > BOARD_LOW - BALL_R &&
		q.z < BOARD_HIGH + BALL_R
		? Math.min(rim, off - BALL_R)
		: rim;
};

export type ShotPlay = {
	// Where it is every SAMPLE_MS from the start: x, y, z, x, y, z, ...
	pts: number[];
	made: boolean;
	// When it is decided (ms from the start): down through the hoop, or off
	// the rim for good.
	decided: number;
	// When, and how hard (feet a second into it), it hit the rim or the glass.
	touches: Touch[];
	// Where it is, and how fast it is going, at the end: off the rim for
	// good, or - in - as it gets to the floor.
	end: { t: number; p: Pt3; v: Pt3 };
};

// The ball from p0, going v0 (feet, feet a second), until it is decided -
// and, if it goes in, on down through the net to the floor.
export const playShot = (
	side: Side,
	p0: Pt3,
	v0: Pt3,
	maxMs = 2600,
): ShotPlay => {
	const cx = rimX(side);
	const cy = COURT_H / 2;
	const dir = attackDir(side);
	// The glass, and which way off it is the court.
	const bx = cx + dir * BOARD_BACK;
	const away = -dir;
	let x = p0.x;
	let y = p0.y;
	let z = p0.z;
	let vx = v0.x;
	let vy = v0.y;
	let vz = v0.z;
	const pts: number[] = [x, y, z];
	const touches: Touch[] = [];
	let made = false;
	let decided: number | undefined;
	let lastTouch = -Infinity;
	let landed: Pt3 | undefined;
	const steps = Math.ceil(maxMs / (STEP * 1000));
	const every = Math.round(SAMPLE_MS / (STEP * 1000));
	let k = 0;
	for (k = 1; k <= steps; k++) {
		const t = k * STEP * 1000;
		vz -= GRAVITY * STEP;
		x += vx * STEP;
		y += vy * STEP;
		z += vz * STEP;
		// The rim: the nearest point of the ring to the ball's middle.
		const hx = x - cx;
		const hy = y - cy;
		const rho = Math.sqrt(hx * hx + hy * hy);
		const qx = rho > 1e-9 ? cx + (hx / rho) * RING : cx + RING;
		const qy = rho > 1e-9 ? cy + (hy / rho) * RING : cy;
		const dx = x - qx;
		const dy = y - qy;
		const dz = z - RIM_Z;
		const d = Math.sqrt(dx * dx + dy * dy + dz * dz);
		if (d < TOUCH && d > 1e-9) {
			const nx = dx / d;
			const ny = dy / d;
			const nz = dz / d;
			const vn = vx * nx + vy * ny + vz * nz;
			if (vn < 0) {
				// Off it: the speed into it turned round (and some of it lost),
				// the speed along it kept but for a little the rim drags off it.
				const tx = vx - vn * nx;
				const ty = vy - vn * ny;
				const tz = vz - vn * nz;
				vx = tx * RIM_SLIDE - vn * RIM_BOUNCE * nx;
				vy = ty * RIM_SLIDE - vn * RIM_BOUNCE * ny;
				vz = tz * RIM_SLIDE - vn * RIM_BOUNCE * nz;
				if (t - lastTouch > 30) {
					touches.push({
						t,
						at: { x: qx, y: qy, z: RIM_Z },
						rim: true,
						hard: -vn,
					});
				}
				lastTouch = t;
			}
			x = qx + nx * TOUCH;
			y = qy + ny * TOUCH;
			z = RIM_Z + nz * TOUCH;
		}
		// The glass.
		const s = (x - bx) * away;
		if (
			s < BALL_R &&
			s > -BALL_R &&
			Math.abs(y - cy) < BOARD_HALF &&
			z > BOARD_LOW &&
			z < BOARD_HIGH
		) {
			const vn = vx * away;
			if (vn < 0) {
				vx = -vx * BOARD_BOUNCE;
				vy *= BOARD_SLIDE;
				vz *= BOARD_SLIDE;
				touches.push({
					t,
					at: { x: bx, y, z },
					rim: false,
					hard: -vn,
				});
			}
			x = bx + away * BALL_R;
		}
		// The net: inside the hoop and below it, it gives a little and slows
		// the ball - and keeps it from going out sideways.
		const depth = RIM_Z - z;
		if (depth > 0 && depth < NET_DEPTH && rho < RING) {
			const room =
				RIM_R - (RIM_R - NET_LOW_R) * (depth / NET_DEPTH) - BALL_R * 0.55;
			if (rho > room && rho > 1e-9) {
				const ux = hx / rho;
				const uy = hy / rho;
				const out = vx * ux + vy * uy;
				if (out > 0) {
					vx -= out * ux * 1.3;
					vy -= out * uy * 1.3;
				}
			}
			const k2 = 1 - NET_DRAG * STEP;
			vx *= k2;
			vy *= k2;
			vz = vz < 0 ? vz * (1 - NET_DRAG * 0.6 * STEP) : vz * k2;
		}
		// In, it drops on through the net to the floor - and how fast it is
		// going as it gets there is how high it bounces.
		if (made && z <= BALL_R) {
			landed ??= { x: vx, y: vy, z: vz };
			z = BALL_R;
			vz = 0;
		}
		if (decided === undefined) {
			if (rho < RIM_R - 0.12 && z < RIM_Z - 0.45 && vz < 0) {
				// Down through the hoop.
				made = true;
				decided = t;
			} else if (
				(z < RIM_Z - 1.1 && rho > RING) ||
				(rho > 4 && z < RIM_Z + 0.5 && vz < 0)
			) {
				// Off the rim, below it and away.
				decided = t;
			}
		}
		// Written down on the beat - and done once it is off the rim for good,
		// or in and on the floor.
		if (k % every === 0) {
			pts.push(x, y, z);
			if ((decided !== undefined && !made) || landed) {
				break;
			}
		}
	}
	const t = Math.min(k, steps) * STEP * 1000;
	return {
		pts,
		made,
		decided: decided ?? t,
		touches,
		end: {
			t,
			p: { x, y, z },
			v: landed ?? { x: vx, y: vy, z: vz },
		},
	};
};

// Where the ball is `ms` into a play.
export const playAt = (pts: number[], ms: number): Pt3 => {
	const n = pts.length / 3;
	const f = Math.max(0, Math.min(n - 1, ms / SAMPLE_MS));
	const i = Math.min(n - 2, Math.floor(f));
	if (i < 0) {
		return { x: pts[0]!, y: pts[1]!, z: pts[2]! };
	}
	const u = f - i;
	return {
		x: pts[i * 3]! + (pts[i * 3 + 3]! - pts[i * 3]!) * u,
		y: pts[i * 3 + 1]! + (pts[i * 3 + 4]! - pts[i * 3 + 1]!) * u,
		z: pts[i * 3 + 2]! + (pts[i * 3 + 5]! - pts[i * 3 + 2]!) * u,
	};
};

// How a shot came out, the way the play-by-play might put it.
export type ShotKind =
	// In: clean, off the iron, off the glass.
	| "swish"
	| "rim"
	| "bank"
	// Out: never touched anything, rimmed out, rolled round and out, bricked,
	// or just off.
	| "air"
	| "rimOut"
	| "rollOut"
	| "brick"
	| "off";

export const kindOf = (p: ShotPlay): ShotKind => {
	const hits = p.touches.filter((h) => h.t <= p.decided + 1);
	if (p.made) {
		return hits.length === 0 ? "swish" : hits[0]!.rim ? "rim" : "bank";
	}
	if (hits.length === 0) {
		return "air";
	}
	const speed = Math.sqrt(
		p.end.v.x * p.end.v.x + p.end.v.y * p.end.v.y + p.end.v.z * p.end.v.z,
	);
	const onRim = hits.at(-1)!.t - hits[0]!.t;
	if (hits.length >= 3 && speed < 9) {
		return "rollOut";
	}
	if (hits.length >= 2 && onRim >= 120) {
		return "rimOut";
	}
	if (hits.length === 1 && hits[0]!.hard >= 13) {
		return "brick";
	}
	return "off";
};

// About as high as a man going up for a rebound takes it.
const REACH = 7.4;

// What the sim says has to happen to a shot.
export type ShotWant = {
	made: boolean;
	// What it should look like, if it matters: tried for first.
	kinds?: ShotKind[];
	// Off the rim, toward whom (where the rebound is caught).
	toward?: Pt;
};

export type FoundShot = {
	// Where it was aimed (the middle of the ball as it comes level with the
	// rim, `flight` ms after the release).
	aim: Pt3;
	flight: number;
	// When (ms after the release) and where it is handed over to the play
	// at the rim, and how fast it is going then.
	handoff: number;
	p: Pt3;
	v: Pt3;
	play: ShotPlay;
	kind: ShotKind;
};

// A shot from `from`, released and getting to the rim between `flight[0]`
// and `flight[1]` ms later (the longer, the higher the arc): tried aimed a
// shade differently each time (offsets from `rand`) until one plays out the
// way `want` says - the first that is the right kind, or, off the rim, the
// one that comes off most toward the man who gets the rebound. Undefined if
// none does.
export const findShot = (
	side: Side,
	from: Pt3,
	flight: [number, number],
	want: ShotWant,
	rand: () => number,
	tries = 48,
): FoundShot | undefined => {
	const cx = rimX(side);
	const cy = COURT_H / 2;
	const dir = attackDir(side);
	const hx = cx - from.x;
	const hy = cy - from.y;
	const far = Math.sqrt(hx * hx + hy * hy) || 1;
	// Along the shot, and across it.
	const ax = hx / far;
	const ay = hy / far;
	let best: { f: FoundShot; score: number } | undefined;
	let fallback: FoundShot | undefined;
	for (let k = 0; k < tries; k++) {
		// Tight round the middle at first, wider as it goes - wide of it to
		// miss. Sometimes off the glass: aimed at the square above the rim.
		const spread = want.made
			? 0.25 + (k / tries) * 0.4
			: 0.55 + (k / tries) * 0.5;
		const u1 = rand() * 2 - 1;
		const u2 = rand() * 2 - 1;
		const glass = rand() < (want.made ? 0.18 : 0.08) && far < 20;
		const ms = flight[0] + (flight[1] - flight[0]) * rand();
		const T = ms / 1000;
		let along = u1 * spread;
		const across = u2 * spread * 0.7;
		if (!want.made && Math.abs(along) < 0.3 && Math.abs(across) < 0.25) {
			along = (along < 0 ? -1 : 1) * (0.3 + Math.abs(along));
		}
		let aim = {
			x: cx + ax * along - ay * across,
			y: cy + ay * along + ax * across,
			z: RIM_Z,
		};
		if (glass) {
			// Off the glass: at the point on it in line with the rim's mirror
			// image behind it - as far again behind it as the glass takes the
			// pace off the ball - a little either side, and up on the square.
			const mx = cx + dir * BOARD_BACK * (1 + 1 / BOARD_BOUNCE);
			const bx = cx + dir * BOARD_BACK;
			const k2 = (bx - from.x) / (mx - from.x);
			aim = {
				x: bx - dir * (BALL_R + 0.02),
				y: from.y + (cy - from.y) * k2 + u2 * 0.3,
				z: RIM_Z + 0.8 + rand() * 0.7,
			};
		}
		// The flight there: straight on across the floor, up and down under
		// gravity.
		const vx = (aim.x - from.x) / T;
		const vy = (aim.y - from.y) / T;
		const vz0 = (aim.z - from.z) / T + 0.5 * GRAVITY * T;
		// Handed over a couple of feet out from the rim - sooner, if by then it
		// would be any nearer the iron or the glass than a few inches, so the
		// play at the rim starts with nothing touching. (Not a shot that
		// starts out that close.)
		const at = (s: number): Pt3 => ({
			x: from.x + vx * s,
			y: from.y + vy * s,
			z: from.z + vz0 * s - 0.5 * GRAVITY * s * s,
		});
		if (gapOf(side, at(0)) < CLEAR) {
			continue;
		}
		const speedH = Math.sqrt(vx * vx + vy * vy) || 1;
		const lead = Math.min(T * 0.6, Math.max(0.06, 2.6 / speedH));
		const until = Math.max(0, T - lead);
		let tau = 0;
		for (let s = SAMPLE_MS / 1000; tau < until; s += SAMPLE_MS / 1000) {
			const q = Math.min(s, until);
			if (gapOf(side, at(q)) < CLEAR) {
				break;
			}
			tau = q;
		}
		const p = at(tau);
		const v = { x: vx, y: vy, z: vz0 - GRAVITY * tau };
		// (Not from behind the glass.)
		if ((p.x - (cx + dir * BOARD_BACK)) * -dir < BALL_R) {
			continue;
		}
		const play = playShot(side, p, v);
		if (play.made !== want.made) {
			continue;
		}
		const kind = kindOf(play);
		// A miss that touches nothing is an air ball - only when it was one.
		if (!want.made && kind === "air" && !want.kinds?.includes("air")) {
			continue;
		}
		const f: FoundShot = {
			aim,
			flight: ms,
			handoff: tau * 1000,
			p,
			v,
			play,
			kind,
		};
		fallback ??= f;
		const right = !want.kinds || want.kinds.includes(kind);
		if (want.made) {
			if (right) {
				return f;
			}
			continue;
		}
		// Off the rim: toward the rebound - coming down near the man who gets
		// it, as it falls into a rebounder's reach - and decided soon enough.
		let score = right ? 0 : 2;
		if (want.toward) {
			const e = play.end;
			const drop = Math.max(0, e.p.z - REACH);
			const tau =
				(e.v.z + Math.sqrt(Math.max(0, e.v.z * e.v.z + 2 * GRAVITY * drop))) /
				GRAVITY;
			const dx = e.p.x + e.v.x * tau - want.toward.x;
			const dy = e.p.y + e.v.y * tau - want.toward.y;
			score += Math.min(2, Math.sqrt(dx * dx + dy * dy) / 6);
		}
		score += Math.max(0, play.decided - 1600) / 2000;
		// (Near enough is good enough.)
		if (score < 1) {
			return f;
		}
		if (!best || score < best.score) {
			best = { f, score };
		}
	}
	return best?.f ?? fallback;
};
