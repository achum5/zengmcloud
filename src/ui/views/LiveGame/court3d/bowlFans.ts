import { makeCourtRng } from "../courtRng.ts";
import {
	type ArenaTeam,
	CLAP_SHARE,
	clothes,
	drawFan,
	type Fan,
	JACKETS,
	JERSEY_SHARE,
	PANTS,
	SKINS,
	teamColor,
} from "./arena.ts";
import type { Bowl } from "./bowlData.ts";
import type { CrowdFace } from "./crowdFaces.ts";
import { COURT_W } from "./geometry.ts";

// THE CROWD IN THE BOWL'S SEATS (see bowlData.ts): today's fans - the same
// way of drawing each, the same share of them in the home colors, on their
// feet after a big play, clapping along, a playoff color-out - each sat in
// a seat of the bowl, in the light there, and the seats empty that nobody
// bought.
//
// Not a fan a seat painted: a few hundred of them, each painted four ways
// (sat, up with his arms high, up waving, sat clapping) into one picture
// (see FanPool), and each seat given one of them - who he is, how badly he
// wants his seat (see takenAt), how soon he gets up.

// Each fan's picture: this many feet across (his arms out, a towel), this
// tall, the bottom of his seat this far up from its foot - at so many px a
// foot.
export const FAN_CELL = { w: 5, h: 6.6, seat: 0.3, px: 10 } as const;
// How many fans (a few times the faces there are to give them: see
// crowdFaces).
const POOL = 192;
// The four ways each is painted, in this order down the picture.
export const FAN_POSES = ["sat", "up", "wave", "clap"] as const;
// The picture: this many fans across it.
const COLS = 32;

export type FanPool = {
	atlas: HTMLCanvasElement;
	n: number;
	cols: number;
	rows: number;
};

// Who is in a seat, four numbers to it: which fan (0..n-1), how badly he
// wants it, how soon he gets up (65535: he never does), and two flags -
// which stands he is in (0 along the side, 1 and 2 behind each basket) and
// whether he claps along (4).
export type SeatedCrowd = {
	pool: FanPool;
	seat: Uint16Array;
	// Every seat's want, low to high (see takenAt).
	wants: Uint16Array;
	// How many came, of how many it seats.
	fill: number;
};

// (A want of 0..WANT_MAX, kept as 0..65535.)
const WANT_MAX = 2;
const KEEP = 65535;

export type FanLook = {
	color: string;
	homeFan: boolean;
	kid: boolean;
	claps: boolean;
	fan: Omit<Fan, "x" | "yb" | "arms" | "holding" | "fist">;
	phone: boolean;
	fist: boolean;
	reach: number;
	towel?: string;
};

// Who the fans are: as paintStands picks them - a good third of the house
// in the home colors, a few in the visitors', the rest in what people wear;
// at a playoff game the first half of them in the home team's first color
// and the rest in its second.
export const fanLooks = (
	home: ArenaTeam | undefined,
	away: ArenaTeam | undefined,
	seed: string,
	faces: CrowdFace[] | undefined,
	playoffs: boolean,
	n = POOL,
): FanLook[] => {
	const rng = makeCourtRng(`bowl-fans|${seed}`);
	const homeC = [teamColor(home, 0, "#8c1d40"), teamColor(home, 1, "#f2c14e")];
	const awayC = [teamColor(away, 0, "#1d3461"), teamColor(away, 1, "#f28c28")];
	const out: FanLook[] = [];
	for (let i = 0; i < n; i++) {
		const r = rng();
		const picked =
			r < 0.26
				? homeC[0]!
				: r < 0.36
					? homeC[1]!
					: r < 0.41
						? awayC[0]!
						: clothes(rng());
		const color = playoffs ? homeC[i < n / 2 ? 0 : 1]! : picked;
		const homeFan = color === homeC[0] || color === homeC[1];
		const kid = rng() < 0.05;
		const face =
			faces && faces.length > 0
				? faces[Math.floor(rng() * faces.length)]!
				: undefined;
		const own = SKINS[Math.floor(rng() * SKINS.length)]!;
		const jersey = homeFan && rng() < JERSEY_SHARE;
		const number = Math.floor(rng() * 40);
		const jacket =
			!jersey && rng() < 0.16 && !playoffs
				? JACKETS[Math.floor(rng() * JACKETS.length)]!
				: undefined;
		const cap =
			rng() < 0.09
				? rng() < 0.5 || playoffs
					? color
					: clothes(rng())
				: undefined;
		const phone = rng() < 0.05;
		const fist = !phone && rng() < 0.05;
		const towel =
			homeFan && rng() < 0.35
				? rng() < 0.5
					? "#f4f4f4"
					: homeC[0]!
				: undefined;
		out.push({
			color,
			homeFan,
			kid,
			claps: homeFan && rng() < CLAP_SHARE,
			phone,
			fist,
			reach: 1.25 + rng() * 0.45,
			...(towel ? { towel } : {}),
			fan: {
				bodyW: (1.3 + rng() * 0.38) * (kid ? 0.84 : 1.08),
				color,
				skin: face?.skin ?? own,
				...(face ? { face } : {}),
				pants: PANTS[Math.floor(rng() * PANTS.length)]!,
				...(jersey ? { number } : {}),
				...(jacket && jacket !== color ? { jacket } : {}),
				...(cap ? { cap } : {}),
			},
		});
	}
	return out;
};

export const paintFanPool = (looks: FanLook[]): FanPool => {
	const { w, h, seat, px } = FAN_CELL;
	const cw = Math.round(w * px);
	const ch = Math.round(h * px);
	const n = looks.length;
	const rows = Math.ceil((n * FAN_POSES.length) / COLS);
	const atlas = document.createElement("canvas");
	atlas.width = cw * COLS;
	atlas.height = ch * rows;
	const ctx = atlas.getContext("2d")!;
	FAN_POSES.forEach((pose, k) => {
		looks.forEach((l, i) => {
			const c = k * n + i;
			const x0 = (c % COLS) * cw;
			const y0 = Math.floor(c / COLS) * ch;
			// (Each in his own box: none of him over the next one's.)
			ctx.save();
			ctx.beginPath();
			ctx.rect(x0, y0, cw, ch);
			ctx.clip();
			const fpx = px * (l.kid ? 0.84 : 1.08);
			const fan: Fan = {
				...l.fan,
				bodyW: l.fan.bodyW * px,
				x: x0 + cw / 2,
				yb: y0 + ch - seat * px,
			};
			if (pose === "up" || pose === "wave") {
				fan.arms = {
					up: pose === "up" ? 1 : 2,
					reach: l.reach,
					...(l.towel ? { towel: l.towel } : {}),
				};
			} else if (pose === "clap" && l.claps) {
				fan.holding = "clap";
			} else {
				if (l.phone) {
					fan.holding = "phone";
				}
				fan.fist = l.fist;
			}
			drawFan(ctx, fan, fpx);
			ctx.restore();
		});
	});
	return { atlas, n, cols: COLS, rows };
};

// Which stands a seat is in, by the way it faces: along the far side (0),
// or behind the basket at the left end (1) or the right (2).
const standsOf = (x: number, fx: number) =>
	Math.abs(fx) > 0.7 ? (x < COURT_W / 2 ? 1 : 2) : 0;

export const seatCrowd = (
	bowl: Bowl,
	looks: FanLook[],
	pool: FanPool,
	seed: string,
	playoffs: boolean,
	// How many came, of how many it seats - a full house if unknown.
	crowd?: { att?: number; capacity?: number },
): SeatedCrowd => {
	const rng = makeCourtRng(`bowl-seats|${seed}`);
	const n = bowl.seats;
	const seat = new Uint16Array(n * 4);
	const u = bowl.unit;
	const half = Math.floor(pool.n / 2);
	for (let j = 0; j < n; j++) {
		const x = bowl.seat[j * 4]! * u;
		const y = bowl.seat[j * 4 + 1]! * u;
		const z = bowl.seat[j * 4 + 2]! * u;
		const a = (bowl.seat[j * 4 + 3]! / 32767) * Math.PI;
		const stands = standsOf(x, Math.cos(a));
		// PLAYOFF COLOR-OUT: every seat in a section in one of the home
		// colors, the next section over in the other.
		const section =
			stands === 0 ? Math.floor((x + 13) / 26) : Math.floor((y + 12) / 25);
		const who = playoffs
			? (((section % 2) + 2) % 2) * half + Math.floor(rng() * half)
			: Math.floor(rng() * pool.n);
		const look = looks[who]!;
		// The best seats taken first - low and near half court - and the
		// rest scattered up the upper deck.
		const want =
			Math.min(1, Math.max(0, z / 100)) * 0.9 +
			(stands === 0 ? Math.abs(x - COURT_W / 2) / 150 : 0.3) +
			rng() * 0.45;
		// Up on his feet once the crowd is this far up (see the card's fans):
		// most of the home fans, under half of the rest.
		const rise = rng() / (look.homeFan ? 0.88 : 0.45);
		seat[j * 4] = who;
		seat[j * 4 + 1] = Math.round(Math.min(1, want / WANT_MAX) * KEEP);
		seat[j * 4 + 2] = rise >= 1 ? KEEP : Math.floor(rise * (KEEP - 1));
		seat[j * 4 + 3] = stands | (look.claps ? 4 : 0);
	}
	const wants = new Uint16Array(n);
	for (let j = 0; j < n; j++) {
		wants[j] = seat[j * 4 + 1]!;
	}
	wants.sort();
	const fill =
		crowd?.att !== undefined && crowd.capacity
			? Math.min(0.97, Math.max(0.04, crowd.att / crowd.capacity))
			: 0.93;
	return { pool, seat, wants, fill };
};

// The want a fan needs to be in his seat, with this share of the house
// there: everyone who wants his seat no less than that.
export const takenAt = (c: SeatedCrowd, share: number): number => {
	const n = c.wants.length;
	if (n === 0 || share <= 0) {
		return -1;
	}
	const i = Math.floor(Math.min(1, share) * n);
	return i >= n ? KEEP : c.wants[i]! - 0.5;
};

// The share of the seats taken at a moment: as many as came, fewer as they
// leave (see seatsAt - at 0.55 of it, half of them gone).
export const shareAt = (c: SeatedCrowd, full: number): number =>
	c.fill * (0.5 + 0.5 * Math.min(1, Math.max(0, (full - 0.55) / 0.45)));
