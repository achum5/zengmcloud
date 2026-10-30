import { makeCourtRng } from "../courtRng.ts";
import type { CrowdFace } from "./crowdFaces.ts";
import { arenaSpecFor, type ArenaSpec } from "./arenaSpec.ts";
import { depthOf, project, type Camera } from "./camera.ts";
import { drawTexturedPlane } from "./planes.ts";
import { BALL_R } from "./evaluate.ts";
import { shade } from "./figure.ts";
import {
	ballTurn,
	CHANNEL,
	CHANNEL_LINES,
	floorMirror,
	LEATHER,
	paintBall,
	viewFrame,
} from "./gameBall.ts";
import {
	BENCH_SEATS,
	BENCH_Y,
	benchStart,
	COURT_H,
	COURT_W,
	RIM_R,
	RIM_Z,
	SEAT_GAP,
	type Pt3,
	type Side,
} from "./geometry.ts";

// THE BUILDING: the stands, the LED boards along their front, the scorer's
// table and the benches - flat pictures painted once per game and stood up in
// the world where the camera sees them (see Court3D) - and the baskets, the
// one part of the building that moves, drawn every frame.

export type ArenaTeam = {
	abbrev?: string;
	name?: string;
	region?: string;
	colors?: [string, string, string];
};

// A flat picture in the world: its top-left corner, how far one of its px
// goes along its width and down its height, and its size in px.
export type Plane = {
	key: string;
	origin: Pt3;
	alongX: Pt3;
	alongY: Pt3;
	w: number;
	h: number;
};

const X0 = -40;
const X1 = COURT_W + 40;
// The front row of the stands, behind the far sideline, and their rake.
const STANDS_Y = -12;
const WALL_H = 3.6;
const RAKE = (28 * Math.PI) / 180;
const SLOPE = 66;

const plane = (
	key: string,
	origin: Pt3,
	across: Pt3,
	down: Pt3,
	lengthX: number,
	lengthY: number,
	px: number,
): Plane => ({
	key,
	origin,
	alongX: { x: across.x / px, y: across.y / px, z: across.z / px },
	alongY: { x: down.x / px, y: down.y / px, z: down.z / px },
	w: Math.round(lengthX * px),
	h: Math.round(lengthY * px),
});

export const STANDS = plane(
	"stands",
	{
		x: X0,
		y: STANDS_Y - SLOPE * Math.cos(RAKE),
		z: WALL_H + SLOPE * Math.sin(RAKE),
	},
	{ x: 1, y: 0, z: 0 },
	{ x: 0, y: Math.cos(RAKE), z: -Math.sin(RAKE) },
	X1 - X0,
	SLOPE,
	8,
);
export const LED_WALL = plane(
	"wall",
	{ x: X0, y: STANDS_Y, z: WALL_H },
	{ x: 1, y: 0, z: 0 },
	{ x: 0, y: 0, z: -1 },
	X1 - X0,
	WALL_H,
	12,
);
export const FLOOR = plane(
	"floor",
	{ x: X0, y: STANDS_Y, z: 0 },
	{ x: 1, y: 0, z: 0 },
	{ x: 0, y: 1, z: 0 },
	X1 - X0,
	62 - STANDS_Y,
	2,
);
// BEHIND EACH BASKET: the stands go on round the ends - a wall of LED boards
// this far behind the baseline, a row or two of courtside seats in front of
// it, and the seats rising from its top, the way they do along the side.
export const END_BACK = 13;
const END_Y0 = STANDS_Y;
const END_Y1 = 62;
// The whole floor, out to the foot of the stands all round.
export const FLOOR_EDGE = {
	x0: -END_BACK,
	y0: STANDS_Y,
	x1: COURT_W + END_BACK,
	y1: END_Y1,
};
// (Each picture laid on them from its left to its right as seen from the
// floor - the floor's y runs toward the camera: at the left end from the
// near side to the far, at the right end from the far side to the near.
// The other way, the boards' words came out backwards.)
export const END_STANDS: [Plane, Plane] = [0, 1].map((side) => {
	const s = side === 0 ? -1 : 1;
	const x = side === 0 ? -END_BACK : COURT_W + END_BACK;
	return plane(
		`endStands${side}`,
		{
			x: x + s * SLOPE * Math.cos(RAKE),
			y: side === 0 ? END_Y1 : END_Y0,
			z: WALL_H + SLOPE * Math.sin(RAKE),
		},
		{ x: 0, y: side === 0 ? -1 : 1, z: 0 },
		{ x: -s * Math.cos(RAKE), y: 0, z: -Math.sin(RAKE) },
		END_Y1 - END_Y0,
		SLOPE,
		8,
	);
}) as [Plane, Plane];
export const END_WALL: [Plane, Plane] = [0, 1].map((side) =>
	plane(
		`endWall${side}`,
		{
			x: side === 0 ? -END_BACK : COURT_W + END_BACK,
			y: side === 0 ? END_Y1 : END_Y0,
			z: WALL_H,
		},
		{ x: 0, y: side === 0 ? -1 : 1, z: 0 },
		{ x: 0, y: 0, z: -1 },
		END_Y1 - END_Y0,
		WALL_H,
		12,
	),
) as [Plane, Plane];

// THE BOARD OVER CENTER COURT, hung high: a screen on the side toward the
// camera showing what the other boards show, the two teams under it.
const JUMBO_W = 26;
const JUMBO_H = 13;
const JUMBO_Z = 34;
const JUMBO_D = 22;
const JUMBO_Y = COURT_H / 2 + JUMBO_D / 2;
export const JUMBO_FRONT = plane(
	"jumbo",
	{ x: COURT_W / 2 - JUMBO_W / 2, y: JUMBO_Y, z: JUMBO_Z + JUMBO_H },
	{ x: 1, y: 0, z: 0 },
	{ x: 0, y: 0, z: -1 },
	JUMBO_W,
	JUMBO_H,
	10,
);

export const TABLE_X0 = 37;
export const TABLE_X1 = 57;
const TABLE_Y = -5;
const TABLE_D = 2.2;
const TABLE_H = 2.7;
export const TABLE_TOP = plane(
	"tableTop",
	{ x: TABLE_X0, y: TABLE_Y - TABLE_D, z: TABLE_H },
	{ x: 1, y: 0, z: 0 },
	{ x: 0, y: 1, z: 0 },
	TABLE_X1 - TABLE_X0,
	TABLE_D,
	12,
);
export const TABLE_FRONT = plane(
	"tableFront",
	{ x: TABLE_X0, y: TABLE_Y, z: TABLE_H },
	{ x: 1, y: 0, z: 0 },
	{ x: 0, y: 0, z: -1 },
	TABLE_X1 - TABLE_X0,
	TABLE_H,
	12,
);
// Each team's bench: a row of high-backed padded chairs beside the table,
// the backs up to a sitting man's shoulders - just behind where each sits
// (see seatSpot).
const BENCH_LEN = BENCH_SEATS * SEAT_GAP + 0.6;
const BENCH_H = 4;
const BENCH_SEAT_Z = 1.45;
export const benchPlane = (t: Side) =>
	plane(
		`bench${t}`,
		{ x: benchStart(t), y: BENCH_Y + 0.3, z: BENCH_H },
		{ x: 1, y: 0, z: 0 },
		{ x: 0, y: 0, z: -1 },
		BENCH_LEN,
		BENCH_H,
		12,
	);

const PALETTE = [
	"#f2f2f2",
	"#1c1c1c",
	"#3b3f46",
	"#7c8592",
	"#24365c",
	"#8b2331",
	"#2f5d3a",
	"#c9b79c",
	"#d9d9d9",
	"#5a4636",
];

const teamColor = (team: ArenaTeam | undefined, i: number, fallback: string) =>
	team?.colors?.[i] || fallback;

// A point on the stands: x along the court, `up` feet up the rake from the
// front row.
export const standsPoint = (x: number, up: number): Pt3 => ({
	x,
	y: STANDS_Y - up * Math.cos(RAKE),
	z: WALL_H + up * Math.sin(RAKE),
});

// The stands' rows, front to back, and the one given over to the ribbon
// board between the lower and upper deck.
const ROW_FT = 2.75;
const RIBBON_ROW = 13;
// How tall a fan's head is drawn, hair and all (feet).
const FACE_FT = 0.98;
// Home fans in a jersey of their team's, of them; and what fans wear below.
const JERSEY_SHARE = 0.22;
const PANTS = ["#262b36", "#1d2330", "#3a3e47", "#4b3b2c", "#2f3a52"];
// The home fans down low who hold a sign up (the D and the fence): in the
// first so many rows, so many of them.
const SIGN_ROWS = 9;
// A tunnel in under the lower deck: how wide (feet), and the rows it takes
// up, from the front.
const TUNNEL_W = 7;
const TUNNEL_ROWS: [number, number] = [2, 4];

// The ribbon board, along the front of the upper deck.
export const RIBBON = plane(
	"ribbon",
	standsPoint(X0, (RIBBON_ROW + 1) * ROW_FT),
	{ x: 1, y: 0, z: 0 },
	{ x: 0, y: Math.cos(RAKE), z: -Math.sin(RAKE) },
	X1 - X0,
	ROW_FT,
	8,
);

// The rafters over the far stands: a truss across the building, its lights,
// and the banners hanging from it.
export const RAFTERS = plane(
	"rafters",
	{ x: X0, y: -58, z: 47 },
	{ x: 1, y: 0, z: 0 },
	{ x: 0, y: 0, z: -1 },
	X1 - X0,
	24,
	8,
);
// Its lights, along under the lower chord: the first this far (ft) in from
// its end, and the rest this far apart (see paintRafters, and drawGlints).
const LIGHT_FIRST = 6;
const LIGHT_GAP = 11;

// ONE FAN IN HIS SEAT (or up out of it), drawn into a picture of the stands
// `px` px to the foot: the bottom of his seat at `yb`, his middle at `x`.
// Square at the shoulders, in a little at the waist, a neck, his arms down
// his sides and his hands in his lap (or up over his head) - in a jersey
// of his team's now and then, arms bare.
export type Fan = {
	x: number;
	yb: number;
	bodyW: number;
	color: string;
	skin: string;
	face?: CrowdFace;
	pants: string;
	// In a jersey, its number.
	number?: number;
	// On his feet (see paintStands' `up`): how high his hands go (feet), and
	// a towel in one of them.
	arms?: { up: 1 | 2; reach: number; towel?: string };
	// A fist up in the air, sat where he is.
	fist?: boolean;
	// Sat with his phone up in front of him, or a drink in his hand - or
	// his hands together at his chest, clapping.
	holding?: "phone" | "drink" | "clap";
};
const fanHeadY = (f: Fan, px: number) =>
	f.yb - (f.arms ? 2 : 1.45) * px - 0.75 * px - (f.arms ? 0.55 * px : 0);
export const drawFan = (ctx: CanvasRenderingContext2D, f: Fan, px: number) => {
	const { x, yb, bodyW, color, skin, pants } = f;
	const standing = f.arms !== undefined;
	const jersey = f.number !== undefined;
	const bodyH = (standing ? 2 : 1.45) * px;
	const lift = standing ? 0.55 * px : 0;
	const top = yb - bodyH - 0.5 * px - lift;
	const bottom = yb - 0.5 * px - lift;
	const sw = bodyW / 2;
	const ww = bodyW * 0.4;
	ctx.fillStyle = skin;
	ctx.fillRect(x - 0.13 * px, top - 0.2 * px, 0.26 * px, 0.28 * px);
	ctx.fillStyle = pants;
	ctx.fillRect(x - ww, bottom - 0.1 * px, 2 * ww, 0.38 * px);
	ctx.fillStyle = color;
	ctx.beginPath();
	ctx.moveTo(x - sw, top + 0.32 * px);
	ctx.quadraticCurveTo(x - sw, top, x - sw + 0.3 * px, top);
	ctx.lineTo(x + sw - 0.3 * px, top);
	ctx.quadraticCurveTo(x + sw, top, x + sw, top + 0.32 * px);
	ctx.lineTo(x + ww, bottom);
	ctx.lineTo(x - ww, bottom);
	ctx.closePath();
	ctx.fill();
	if (jersey) {
		// (The arm holes, and a number across his chest.)
		ctx.fillStyle = skin;
		for (const side of [-1, 1]) {
			ctx.fillRect(
				x + side * sw - (side > 0 ? 0.28 * px : 0),
				top + 0.05 * px,
				0.28 * px,
				0.55 * px,
			);
		}
		ctx.fillStyle = inkOn(color);
		ctx.font = `800 ${Math.round(0.5 * px)}px sans-serif`;
		ctx.textAlign = "center";
		ctx.textBaseline = "middle";
		ctx.fillText(String(f.number), x, top + bodyH * 0.42);
		ctx.textAlign = "start";
	}
	const sleeve = jersey ? skin : shade(color, -0.14);
	ctx.lineCap = "round";
	if (!standing) {
		for (const side of [-1, 1]) {
			const ex = x + side * (sw + 0.02 * px);
			const ey = top + 0.85 * px;
			// (Up at his chest round his phone, both hands; his right hand up
			// with a drink.)
			const up =
				f.holding === "phone" ||
				f.holding === "clap" ||
				(f.holding === "drink" && side > 0);
			const out =
				f.holding === "drink" ? 0.32 : f.holding === "clap" ? 0.05 : 0.1;
			const hx = up ? x + side * out * px : x + side * 0.2 * px;
			const hy = up
				? top + (f.holding === "clap" ? 0.85 : 0.95) * px
				: bottom - 0.12 * px;
			ctx.strokeStyle = sleeve;
			ctx.lineWidth = 0.28 * px;
			ctx.beginPath();
			ctx.moveTo(x + side * (sw - 0.14 * px), top + 0.22 * px);
			ctx.lineTo(ex, ey);
			ctx.stroke();
			ctx.strokeStyle = skin;
			ctx.lineWidth = 0.22 * px;
			ctx.beginPath();
			ctx.moveTo(ex, ey);
			ctx.lineTo(hx, hy);
			ctx.stroke();
		}
		if (f.holding === "phone") {
			ctx.fillStyle = "#16181d";
			ctx.fillRect(x - 0.17 * px, top + 0.45 * px, 0.34 * px, 0.56 * px);
			ctx.fillStyle = "rgba(170, 205, 255, 0.85)";
			ctx.fillRect(x - 0.12 * px, top + 0.5 * px, 0.24 * px, 0.44 * px);
		} else if (f.holding === "drink") {
			const cx = x + 0.32 * px;
			ctx.fillStyle = "#c8202a";
			ctx.fillRect(cx - 0.15 * px, top + 0.55 * px, 0.3 * px, 0.45 * px);
			ctx.fillStyle = "#f2f2f0";
			ctx.fillRect(cx - 0.16 * px, top + 0.52 * px, 0.32 * px, 0.07 * px);
		}
	}
	const headY = fanHeadY(f, px);
	if (f.arms) {
		// Arms up in a V - some waving a towel.
		const { up, reach, towel } = f.arms;
		const spread = (up === 1 ? 0.3 : 0.95) * px;
		const shoulderY = headY + 0.55 * px;
		const hands: [number, number][] = [];
		for (const side of [-1, 1]) {
			const sx = x + (side * bodyW) / 2.4;
			const hx = sx + side * spread;
			const hy = shoulderY - reach * px + (up === 2 ? 0.3 * px : 0);
			// A sleeve to the elbow, his forearm and his hand.
			const ex = (sx + hx) / 2 + side * 0.12 * px;
			const ey = (shoulderY + hy) / 2;
			ctx.strokeStyle = sleeve;
			ctx.lineWidth = 0.34 * px;
			ctx.beginPath();
			ctx.moveTo(sx, shoulderY);
			ctx.lineTo(ex, ey);
			ctx.stroke();
			ctx.strokeStyle = skin;
			ctx.lineWidth = 0.26 * px;
			ctx.beginPath();
			ctx.moveTo(ex, ey);
			ctx.lineTo(hx, hy);
			ctx.stroke();
			ctx.fillStyle = skin;
			ctx.beginPath();
			ctx.arc(hx, hy, 0.19 * px, 0, Math.PI * 2);
			ctx.fill();
			hands.push([hx, hy]);
		}
		if (towel) {
			const [hx, hy] = hands[up === 1 ? 1 : 0]!;
			ctx.fillStyle = towel;
			ctx.fillRect(hx - 0.45 * px, hy - 0.95 * px, 0.9 * px, 0.75 * px);
		}
	}
	if (f.face) {
		// (The middle of his face a little below the middle of the picture
		// of his head: the hair above it.)
		const fh = FACE_FT * px;
		const fw = (fh * f.face.img.width) / f.face.img.height;
		ctx.drawImage(f.face.img, x - fw / 2, headY - fh * 0.54, fw, fh);
	} else {
		ctx.fillStyle = skin;
		ctx.beginPath();
		ctx.arc(x, headY, 0.36 * px, 0, Math.PI * 2);
		ctx.fill();
	}
	if (f.fist) {
		// Somebody with a hand up - a fist in the air.
		const sx = x + sw - 0.12 * px;
		const ex = sx + 0.3 * px;
		const ey = top - 0.4 * px;
		ctx.strokeStyle = sleeve;
		ctx.lineWidth = 0.3 * px;
		ctx.beginPath();
		ctx.moveTo(sx, top + 0.2 * px);
		ctx.lineTo(ex, ey);
		ctx.stroke();
		ctx.strokeStyle = skin;
		ctx.lineWidth = 0.24 * px;
		ctx.beginPath();
		ctx.moveTo(ex, ey);
		ctx.lineTo(sx + 0.2 * px, top - 1.1 * px);
		ctx.stroke();
		ctx.fillStyle = skin;
		ctx.beginPath();
		ctx.arc(sx + 0.2 * px, top - 1.2 * px, 0.2 * px, 0, Math.PI * 2);
		ctx.fill();
	}
};

// The crowd, painted once - and twice more on its feet (`up` 1 and 2: arms
// high, then waving wide), the same people in the same seats, for when the
// building erupts.
export const paintStands = (
	home: ArenaTeam | undefined,
	away: ArenaTeam | undefined,
	seed: string,
	up: 0 | 1 | 2 = 0,
	// How many came, of how many it seats - a full house if unknown.
	crowd?: { att?: number; capacity?: number },
	// The stands behind a basket, not the long side.
	end?: 0 | 1,
	// Faces for the fans (see crowdFaces), once there are - till then a head
	// of skin for each.
	faces?: CrowdFace[],
	spec: ArenaSpec = arenaSpecFor(home),
): HTMLCanvasElement => {
	const P = end === undefined ? STANDS : END_STANDS[end];
	const A0 = end === undefined ? X0 : END_Y0;
	const A1 = end === undefined ? X1 : END_Y1;
	const { w, h } = P;
	const px = w / (A1 - A0);
	const canvas = document.createElement("canvas");
	canvas.width = w;
	canvas.height = h;
	const ctx = canvas.getContext("2d")!;
	const rng = makeCourtRng(`stands|${seed}${end ?? ""}`);
	// Who gets up is decided apart from who is there, so both pictures have
	// the same crowd.
	const rise = makeCourtRng(`stands-up|${seed}${end ?? ""}`);
	// (Whose face each fan has is decided apart from all that, too.)
	const faceRng = makeCourtRng(`stands-face|${seed}${end ?? ""}`);
	const g = ctx.createLinearGradient(0, 0, 0, h);
	g.addColorStop(0, "#07080b");
	g.addColorStop(1, "#1a1c22");
	ctx.fillStyle = g;
	ctx.fillRect(0, 0, w, h);

	const homeC = [teamColor(home, 0, "#8c1d40"), teamColor(home, 1, "#f2c14e")];
	const awayC = [teamColor(away, 0, "#1d3461"), teamColor(away, 1, "#f28c28")];
	const shirt = () => {
		const r = rng();
		if (r < 0.32) {
			return homeC[0]!;
		}
		if (r < 0.44) {
			return homeC[1]!;
		}
		if (r < 0.5) {
			return awayC[0]!;
		}
		return PALETTE[Math.floor(rng() * PALETTE.length)]!;
	};
	const skins = ["#f1c7a5", "#d9a77f", "#b07a52", "#8a5a3b", "#5e3b26"];
	const rows = Math.floor(SLOPE / ROW_FT);
	const aisle = 31;
	// Who came: as many as bought tickets, in the best seats first - low and
	// near half court - and the rest scattered up the upper deck. Even a
	// sellout has a few seats empty.
	const fill =
		crowd?.att !== undefined && crowd.capacity
			? Math.min(0.97, Math.max(0.04, crowd.att / crowd.capacity))
			: 0.93;
	const seatRng = makeCourtRng(`seats|${seed}${end ?? ""}`);
	const seats: { r: number; xf: number; want: number }[] = [];
	for (let r = 0; r < rows; r++) {
		if (r === RIBBON_ROW && end === undefined) {
			continue;
		}
		for (let xf = A0 + 0.6; xf < A1 - 0.6; xf += 1.85) {
			const mod = (((xf - A0) % aisle) + aisle) % aisle;
			if (mod < 3) {
				continue;
			}
			seats.push({
				r,
				xf,
				want:
					(r / rows) * 0.9 +
					(end === undefined ? Math.abs(xf - COURT_W / 2) / 150 : 0.3) +
					seatRng() * 0.45,
			});
		}
	}
	const wants = seats.map((st) => st.want).sort((a, b) => a - b);
	const taken =
		wants[Math.min(wants.length - 1, Math.floor(fill * wants.length))]!;
	const seatColor = spec.stands.seat;
	let next = 0;
	const signs: { x: number; top: number }[] = [];
	for (let r = 0; r < rows; r++) {
		// Rows from the front (bottom of the picture) up.
		const yb = h - r * ROW_FT * px;
		if (r === RIBBON_ROW && end === undefined) {
			// The ribbon board goes here (see RIBBON).
			ctx.fillStyle = "#050507";
			ctx.fillRect(0, yb - ROW_FT * px, w, ROW_FT * px);
			continue;
		}
		// Seat backs.
		ctx.fillStyle = r % 2 ? "#22252c" : "#1d2026";
		ctx.fillRect(0, yb - 0.9 * px, w, 0.9 * px);
		for (; next < seats.length && seats[next]!.r === r; next++) {
			const xf = seats[next]!.xf;
			if (seats[next]!.want > taken) {
				// An empty seat.
				ctx.fillStyle = seatColor;
				ctx.beginPath();
				ctx.roundRect(
					(xf - A0) * px - 0.62 * px,
					yb - 1.55 * px,
					1.24 * px,
					1.2 * px,
					0.25 * px,
				);
				ctx.fill();
				continue;
			}
			const x = (xf - A0) * px + (rng() - 0.5) * 2;
			const bodyW = (1.25 + rng() * 0.35) * px;
			const color = shirt();
			const face =
				faces && faces.length > 0
					? faces[Math.floor(faceRng() * faces.length)]!
					: undefined;
			// (Drawn whether he has a face or not: the same crowd either way.)
			const own = skins[Math.floor(rng() * skins.length)]!;
			const homeFan = color === homeC[0] || color === homeC[1];
			const standing = up > 0 && rise() < (homeFan ? 0.88 : 0.45);
			const jersey = homeFan && faceRng() < JERSEY_SHARE;
			const pants = PANTS[Math.floor(faceRng() * PANTS.length)]!;
			const number = jersey ? Math.floor(faceRng() * 40) : 0;
			const phone = faceRng() < 0.05;
			let arms: Fan["arms"];
			if (standing) {
				const reach = 1.25 + rise() * 0.45;
				const towel =
					homeFan && rise() < 0.35
						? rise() < 0.5
							? "#f4f4f4"
							: homeC[0]!
						: undefined;
				arms = { up: up as 1 | 2, reach, ...(towel ? { towel } : {}) };
			}
			const fan: Fan = {
				x,
				yb,
				bodyW,
				color,
				skin: face?.skin ?? own,
				...(face ? { face } : {}),
				pants,
				...(jersey ? { number } : {}),
				...(arms ? { arms } : {}),
				...(phone && !arms ? { holding: "phone" as const } : {}),
			};
			const headY = fanHeadY(fan, px);
			// Now and then a home fan down low holds a sign up over his head
			// (painted once the rows behind him are: see below).
			if (homeFan && r < SIGN_ROWS && seatRng() < spec.stands.signShare) {
				signs.push({ x, top: headY - 0.55 * px - (standing ? 0.6 * px : 0) });
			}
			fan.fist = rng() < 0.05 && !standing && !fan.holding;
			drawFan(ctx, fan, px);
		}
		// The aisles - a light on each step.
		ctx.fillStyle = "#2b2e35";
		for (let xf = A0; xf < A1; xf += aisle) {
			ctx.fillRect((xf - A0) * px, yb - ROW_FT * px, 2.4 * px, ROW_FT * px);
		}
		ctx.fillStyle = "rgba(255,214,140,0.55)";
		for (let xf = A0; xf < A1; xf += aisle) {
			ctx.fillRect((xf - A0 + 0.15) * px, yb - 0.35 * px, 0.35 * px, 0.2 * px);
		}
	}
	// The signs held up: the D and the fence, in the home team's color.
	for (const { x, top } of signs) {
		const sw = 2.2 * px;
		const sh = 1.3 * px;
		const sx = x - sw / 2;
		const sy = top - sh;
		ctx.fillStyle = "#f4f4f2";
		ctx.fillRect(sx, sy, sw, sh);
		ctx.strokeStyle = "#1b1c20";
		ctx.lineWidth = Math.max(1, 0.08 * px);
		ctx.strokeRect(sx, sy, sw, sh);
		ctx.fillStyle = homeC[0]!;
		ctx.font = `900 ${Math.round(sh * 0.9)}px sans-serif`;
		ctx.textBaseline = "middle";
		ctx.fillText("D", sx + 0.12 * px, sy + sh / 2 + 0.05 * px);
		ctx.strokeStyle = homeC[0]!;
		ctx.lineWidth = Math.max(1, 0.12 * px);
		ctx.beginPath();
		for (let k = 0; k < 4; k++) {
			const fx = sx + sw * (0.52 + k * 0.13);
			ctx.moveTo(fx, sy + sh * 0.18);
			ctx.lineTo(fx, sy + sh * 0.86);
		}
		ctx.moveTo(sx + sw * 0.47, sy + sh * 0.45);
		ctx.lineTo(sx + sw * 0.93, sy + sh * 0.45);
		ctx.stroke();
	}
	// The tunnels in under the lower deck, at every other aisle (see
	// ArenaSpec.stands): a dark mouth a few rows up from the front, a rail
	// along its top.
	const every = spec.stands.tunnelEvery;
	for (let k = 1, xf = A0 + aisle; xf < A1 - 4; k++, xf += aisle) {
		if (every <= 0 || k % every) {
			continue;
		}
		const x = (xf - A0 + 1.2 - TUNNEL_W / 2) * px;
		const top = h - (TUNNEL_ROWS[1] + 1) * ROW_FT * px;
		const tall = (TUNNEL_ROWS[1] - TUNNEL_ROWS[0] + 1) * ROW_FT * px;
		ctx.fillStyle = "#0a0b0e";
		ctx.fillRect(x, top, TUNNEL_W * px, tall);
		ctx.fillStyle = "#3a3e47";
		ctx.fillRect(
			x - 0.4 * px,
			top - 0.35 * px,
			(TUNNEL_W + 0.8) * px,
			0.35 * px,
		);
	}
	// The light falls on the court: the upper deck fades into the dark.
	const dark = ctx.createLinearGradient(0, 0, 0, h);
	const bowl = spec.lighting.bowl;
	dark.addColorStop(0, `rgba(3,3,6,${Math.min(1, 0.88 * bowl)})`);
	dark.addColorStop(0.45, `rgba(3,3,6,${Math.min(1, 0.5 * bowl)})`);
	dark.addColorStop(1, `rgba(3,3,6,${Math.min(1, 0.12 * bowl)})`);
	ctx.fillStyle = dark;
	ctx.fillRect(0, 0, w, h);
	return canvas;
};

// ---- the LED boards --------------------------------------------------------

// What the boards can show: the team, a chant for the crowd, DEFENSE while
// the road team has it (blinking: two pictures), and a shout after a big
// play by the home side.
// THE SEATS ON THE FLOOR: rows of padded chairs behind each baseline, past
// the photographers - the back row up a step - and in the far corners past
// the ends of the benches; the best seats in the building, every one taken
// (see ArenaSpec.courtside). Each of the people in them drawn once, sat in
// his chair, into one picture (see courtsideFor), and stood up in the world
// where he sits each frame, the size the camera has him there (see
// drawCourtside) - seen from anywhere, a person in a chair.
const CS_FRONT = 8;
const CS_ROW = 2.6;
const CS_STEP = 1.1;
// Along the baseline, and the aisles through them (feet across the floor).
const CS_Y0 = -3;
const CS_Y1 = COURT_H + 3;
const CS_AISLES = [8.5, COURT_H - 8.5];
// In the far corners: from past the corner of the floor to the bench.
const CS_CORNER_Y = BENCH_Y - 0.4;
// Behind each bench, up a step: its staff - so many of them, this far back
// of the bench (feet), the step this high.
const STAFF_N = 7;
const STAFF_Y = BENCH_Y - 2;
const STAFF_Z = 0.6;
const CS_CORNER_W = 9.5;
// Waving his arms, each way this long (ms); clapping, hands together and
// apart.
const WAVE_MS = 260;
const CLAP_MS = 140;
// The nearest in front of the camera one is drawn (feet).
const NEAREST = 2.5;
// A chair is this wide, and a seat this high off the floor (feet); and each
// one's picture this big (feet), drawn this many px to the foot.
const CS_CHAIR = 2.05;
const CS_SEAT = 1.5;
const CS_CELL_W = 2.4;
const CS_CELL_H = 7.2;
const CS_PX = 12;
// (Space between the pictures, px - scaled down, none bleeds into the next.)
const CS_GAP = 3;
export type Courtside = {
	// Each in his chair - and on his feet, arms up, both ways he waves them.
	atlas: HTMLCanvasElement;
	atlasUp: HTMLCanvasElement;
	atlasWave: HTMLCanvasElement;
	// Sat, his hands together at his chest - clapping.
	atlasClap: HTMLCanvasElement;
	cols: number;
	// Where each sits - his feet - and which picture is his; whether he is
	// for the home team; the end he is at (if behind a basket); and how soon
	// he is up when the building is (0 to 1, the first up first).
	fans: { at: Pt3; cell: number; home: boolean; end?: Side; rise: number }[];
	// The steps the back rows are up on: each a box along the floor.
	steps: { x0: number; x1: number; y0: number; y1: number; z: number }[];
	// At the far end of each bench, its cooler and its towels.
	props: { at: Pt3; kind: "cooler" | "towels" }[];
	cooler: string;
	towels: string;
};
export const courtsideFor = (
	home: ArenaTeam | undefined,
	away: ArenaTeam | undefined,
	spec: ArenaSpec,
	faces?: CrowdFace[],
): Courtside => {
	const cs = spec.courtside;
	const at: Pt3[] = [];
	const endOf: (Side | undefined)[] = [];
	const steps: Courtside["steps"] = [];
	for (const side of [0, 1] as const) {
		const s = side === 0 ? -1 : 1;
		const line = side === 0 ? 0 : COURT_W;
		for (let r = 0; r < cs.endRows; r++) {
			const x = line + s * (CS_FRONT + r * CS_ROW);
			if (r > 0) {
				const a = line + s * (CS_FRONT + (r - 0.5) * CS_ROW);
				const b = line + s * (CS_FRONT + (r + 0.5) * CS_ROW);
				steps.push({
					x0: Math.min(a, b),
					x1: Math.max(a, b),
					y0: CS_Y0,
					y1: CS_Y1,
					z: r * CS_STEP,
				});
			}
			for (
				let y = CS_Y0 + CS_CHAIR / 2 + (r % 2) * 0.5;
				y < CS_Y1;
				y += CS_CHAIR
			) {
				if (CS_AISLES.some((aisle) => Math.abs(y - aisle) < 1.4)) {
					continue;
				}
				at.push({ x, y, z: r * CS_STEP });
				endOf.push(side);
			}
		}
		const x0 =
			side === 0
				? benchStart(0) - 0.8 - CS_CORNER_W
				: benchStart(1) + BENCH_LEN + 0.8;
		for (let r = 0; r < cs.cornerRows; r++) {
			const y = CS_CORNER_Y - r * CS_ROW;
			if (r > 0) {
				steps.push({
					x0,
					x1: x0 + CS_CORNER_W,
					y0: y - CS_ROW / 2,
					y1: y + CS_ROW / 2,
					z: r * CS_STEP,
				});
			}
			for (let x = x0 + CS_CHAIR / 2; x < x0 + CS_CORNER_W; x += CS_CHAIR) {
				at.push({ x, y, z: r * CS_STEP });
				endOf.push(undefined);
			}
		}
	}
	// The assistants, the trainers, the video staff, behind their bench -
	// in the team's polo or black, a tablet in hand now and then.
	const staffOf: (Side | undefined)[] = at.map(() => undefined);
	for (const t of [0, 1] as const) {
		const x0 = benchStart(t) + 1.5;
		const x1 = benchStart(t) + BENCH_LEN - 1.5;
		steps.push({
			x0: benchStart(t),
			x1: benchStart(t) + BENCH_LEN,
			y0: STAFF_Y - 1.1,
			y1: STAFF_Y + 1.1,
			z: STAFF_Z,
		});
		for (let k = 0; k < STAFF_N; k++) {
			at.push({
				x: x0 + ((x1 - x0) * k) / (STAFF_N - 1),
				y: STAFF_Y,
				z: STAFF_Z,
			});
			endOf.push(undefined);
			staffOf.push(t);
		}
	}
	// Each one's picture: his chair, his legs down to his shoes, him.
	const cols = 16;
	const cw = Math.round(CS_CELL_W * CS_PX);
	const ch = Math.round(CS_CELL_H * CS_PX);
	const atlas = document.createElement("canvas");
	atlas.width = cols * (cw + CS_GAP);
	atlas.height = Math.ceil(at.length / cols) * (ch + CS_GAP);
	const ctx = atlas.getContext("2d")!;
	const atlasUp = document.createElement("canvas");
	atlasUp.width = atlas.width;
	atlasUp.height = atlas.height;
	const up = atlasUp.getContext("2d")!;
	const atlasWave = document.createElement("canvas");
	atlasWave.width = atlas.width;
	atlasWave.height = atlas.height;
	const wave = atlasWave.getContext("2d")!;
	const atlasClap = document.createElement("canvas");
	atlasClap.width = atlas.width;
	atlasClap.height = atlas.height;
	const clap = atlasClap.getContext("2d")!;
	const homes: boolean[] = [];
	const rng = makeCourtRng(
		`courtside|${home?.abbrev ?? home?.name ?? ""}|${away?.abbrev ?? away?.name ?? ""}`,
	);
	const pick = <T>(a: readonly T[]): T => a[Math.floor(rng() * a.length)]!;
	const homeC = [teamColor(home, 0, "#8c1d40"), teamColor(home, 1, "#f2c14e")];
	const skins = ["#f1c7a5", "#d9a77f", "#b07a52", "#8a5a3b", "#5e3b26"];
	const px = CS_PX;
	at.forEach((_, i) => {
		const ox = (i % cols) * (cw + CS_GAP);
		const oy = Math.floor(i / cols) * (ch + CS_GAP);
		const x = ox + cw / 2;
		const floor = oy + ch;
		const yb = floor - CS_SEAT * px + 0.5 * px;
		const staff = staffOf[i];
		const crew = staff === undefined ? undefined : staff === 0 ? away : home;
		const chair =
			staff === undefined
				? cs.chair
				: shade(teamColor(crew, 0, "#2d3a55"), -0.3);
		const team = staff === undefined ? rng() < cs.homeShare : staff === 1;
		const jersey = staff === undefined && team && rng() < cs.jerseyShare;
		const color =
			staff !== undefined
				? rng() < 0.65
					? teamColor(crew, 0, "#2d3a55")
					: "#16171b"
				: team
					? pick(homeC)
					: pick(COURTSIDE_DRESS);
		const face = faces && faces.length > 0 ? pick(faces) : undefined;
		const skin = face?.skin ?? pick(skins);
		const pants = pick(PANTS);
		const shoes = rng() < 0.5 ? "#f2f2f2" : "#1a1a1d";
		const bodyW = (1.3 + rng() * 0.3) * px;
		const number = jersey ? Math.floor(rng() * 40) : undefined;
		const hold = rng();
		const holding =
			hold < (staff === undefined ? 0.16 : 0.3)
				? ("phone" as const)
				: hold < 0.28 && staff === undefined
					? ("drink" as const)
					: undefined;
		homes.push(team);
		// Sat in his chair - and clapping (see drawCourtside).
		const seated = (g: CanvasRenderingContext2D, f: Fan) => {
			g.fillStyle = chair;
			g.beginPath();
			g.roundRect(x - 0.85 * px, yb - 2.25 * px, 1.7 * px, 1.9 * px, 0.3 * px);
			g.fill();
			g.fillStyle = shade(cs.chair, -0.4);
			g.fillRect(
				x - 0.75 * px,
				yb - 0.4 * px,
				0.12 * px,
				floor - yb + 0.4 * px,
			);
			g.fillRect(
				x + 0.63 * px,
				yb - 0.4 * px,
				0.12 * px,
				floor - yb + 0.4 * px,
			);
			for (const side of [-1, 1]) {
				g.fillStyle = pants;
				g.fillRect(
					x + side * 0.08 * px - (side < 0 ? 0.42 * px : 0),
					yb - 0.5 * px,
					0.42 * px,
					floor - yb + 0.3 * px,
				);
				g.fillStyle = shoes;
				g.beginPath();
				g.ellipse(
					x + side * 0.3 * px,
					floor - 0.14 * px,
					0.28 * px,
					0.14 * px,
					0,
					0,
					Math.PI * 2,
				);
				g.fill();
			}
			drawFan(g, f, px);
		};
		const fan: Fan = {
			x,
			yb,
			bodyW,
			color,
			skin,
			...(face ? { face } : {}),
			pants,
			...(number === undefined ? {} : { number }),
			...(holding ? { holding } : {}),
		};
		seated(ctx, fan);
		seated(clap, { ...fan, holding: "clap" });
		// On his feet: in front of his chair, legs straight, arms up - a
		// towel going, some of the home fans - and waving them, the other
		// way.
		const arms = rng() < 0.5 ? 1 : 2;
		const reach = 1.2 + rng() * 0.4;
		const towel = team && rng() < 0.3;
		for (const [g, a] of [
			[up, arms],
			[wave, 3 - arms],
		] as const) {
			g.fillStyle = chair;
			g.beginPath();
			g.roundRect(x - 0.85 * px, yb - 2.25 * px, 1.7 * px, 1.9 * px, 0.3 * px);
			g.fill();
			const hips = floor - 3.05 * px;
			for (const side of [-1, 1]) {
				g.fillStyle = pants;
				g.fillRect(
					x + side * 0.06 * px - (side < 0 ? 0.4 * px : 0),
					hips - 0.1 * px,
					0.4 * px,
					floor - hips,
				);
				g.fillStyle = shoes;
				g.beginPath();
				g.ellipse(
					x + side * 0.26 * px,
					floor - 0.14 * px,
					0.28 * px,
					0.14 * px,
					0,
					0,
					Math.PI * 2,
				);
				g.fill();
			}
			drawFan(
				g,
				{
					...fan,
					holding: undefined,
					yb: floor - 2 * px,
					arms: {
						up: a as 1 | 2,
						reach,
						...(towel ? { towel: "#f4f4f2" } : {}),
					},
				},
				px,
			);
		}
	});
	const props: Courtside["props"] = [];
	for (const t of [0, 1] as const) {
		// (Its end away from the table.)
		const x = t === 0 ? benchStart(0) - 0.9 : benchStart(1) + BENCH_LEN + 0.9;
		props.push(
			{ at: { x, y: BENCH_Y + 0.4, z: 0 }, kind: "cooler" },
			{
				at: { x: x + (t === 0 ? 1.6 : -1.6), y: BENCH_Y + 0.9, z: 0 },
				kind: "towels",
			},
		);
	}
	return {
		atlas,
		atlasUp,
		atlasWave,
		atlasClap,
		cols,
		fans: at.map((p, cell) => ({
			at: p,
			cell,
			home: homes[cell]!,
			...(endOf[cell] === undefined ? {} : { end: endOf[cell] }),
			rise: rng(),
		})),
		steps,
		props,
		cooler: spec.bench.cooler,
		towels: spec.bench.towels,
	};
};
// A cooler on its stand (feet): a drum this wide and tall, its white lid,
// a stack of cups on top; towels, a pile this wide and high.
const COOLER_W = 1.25;
const COOLER_H = 1.55;
const STAND_H = 1.0;
const TOWELS_W = 1.6;
const TOWELS_H = 0.6;
const drawProp = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	c: Courtside,
	p: Courtside["props"][number],
) => {
	const q = project(cam, p.at);
	const k = q.k;
	if (
		q.x < -4 * k ||
		q.x > cam.viewW + 4 * k ||
		q.y < 0 ||
		q.y > cam.viewH + 6 * k
	) {
		return;
	}
	if (p.kind === "towels") {
		ctx.fillStyle = shade(c.towels, -0.12);
		ctx.beginPath();
		ctx.roundRect(
			q.x - (TOWELS_W / 2) * k,
			q.y - TOWELS_H * k,
			TOWELS_W * k,
			TOWELS_H * k,
			0.12 * k,
		);
		ctx.fill();
		ctx.fillStyle = c.towels;
		for (let i = 0; i < 3; i++) {
			ctx.fillRect(
				q.x - (TOWELS_W / 2 - 0.05) * k,
				q.y - (TOWELS_H - 0.04 - i * 0.19) * k,
				(TOWELS_W - 0.1) * k,
				0.12 * k,
			);
		}
		return;
	}
	// The stand: a dark frame.
	ctx.fillStyle = "#1c1d22";
	ctx.fillRect(q.x - 0.55 * k, q.y - STAND_H * k, 1.1 * k, STAND_H * k);
	const top = q.y - (STAND_H + COOLER_H) * k;
	const g = ctx.createLinearGradient(
		q.x - (COOLER_W / 2) * k,
		0,
		q.x + (COOLER_W / 2) * k,
		0,
	);
	g.addColorStop(0, shade(c.cooler, -0.25));
	g.addColorStop(0.45, shade(c.cooler, 0.12));
	g.addColorStop(1, shade(c.cooler, -0.3));
	ctx.fillStyle = g;
	ctx.beginPath();
	ctx.roundRect(
		q.x - (COOLER_W / 2) * k,
		top,
		COOLER_W * k,
		COOLER_H * k,
		0.12 * k,
	);
	ctx.fill();
	// Its lid, its tap, the cups on top.
	ctx.fillStyle = "#f2f2f0";
	ctx.fillRect(
		q.x - (COOLER_W / 2 + 0.03) * k,
		top - 0.12 * k,
		(COOLER_W + 0.06) * k,
		0.2 * k,
	);
	ctx.fillStyle = "#e9e9e6";
	ctx.fillRect(q.x - 0.13 * k, top - 0.7 * k, 0.26 * k, 0.58 * k);
	ctx.fillStyle = "#1c1d22";
	ctx.fillRect(q.x - 0.06 * k, top + (COOLER_H - 0.38) * k, 0.12 * k, 0.16 * k);
};
// Each frame: the steps, then the people - the furthest first.
// (`up`: how far up the building is - this end of it, or the side - 0 to
// 1: the home fans up first, most of them by the time it is all the way;
// the rest, some.)
export const drawCourtside = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	c: Courtside,
	up: (end: Side | undefined) => number = () => 0,
	// Waving them, the time (ms) to wave them by.
	wave: (end: Side | undefined) => boolean = () => false,
	now = 0,
	// Sat clapping: the home fans, or the visitors' (a basket by their team).
	clapping: (home: boolean) => boolean = () => false,
) => {
	const cw = Math.round(CS_CELL_W * CS_PX);
	const ch = Math.round(CS_CELL_H * CS_PX);
	// (The people and the things at the benches, the furthest first.)
	const placed: {
		d: number;
		x: number;
		y: number;
		k: number;
		cell: number;
		stood?: boolean;
		waving?: boolean;
		clapped?: boolean;
		prop?: Courtside["props"][number];
	}[] = [];
	// (Not one at the camera, or behind it - a replay's from among them.)
	for (const f of c.fans) {
		if (depthOf(cam, f.at) < NEAREST) {
			continue;
		}
		const p = project(cam, f.at);
		const w = CS_CELL_W * p.k;
		const h = CS_CELL_H * p.k;
		if (p.x + w < 0 || p.x - w > cam.viewW || p.y < 0 || p.y - h > cam.viewH) {
			continue;
		}
		placed.push({
			d: p.depth,
			x: p.x,
			y: p.y,
			k: p.k,
			cell: f.cell,
			stood: f.rise < up(f.end) * (f.home ? 0.95 : 0.4),
			waving: wave(f.end) && Math.floor(now / WAVE_MS + f.rise * 7) % 2 === 1,
			clapped:
				clapping(f.home) && Math.floor(now / CLAP_MS + f.rise * 5) % 2 === 1,
		});
	}
	for (const p of c.props) {
		if (depthOf(cam, p.at) < NEAREST) {
			continue;
		}
		const q = project(cam, p.at);
		placed.push({ d: q.depth, x: q.x, y: q.y, k: q.k, cell: -1, prop: p });
	}
	placed.sort((a, b) => b.d - a.d);
	// (Each step a box: its top and its sides, dark.)
	ctx.fillStyle = "#121317";
	for (const s of c.steps) {
		if (
			[s.x0, s.x1].some((x) =>
				[s.y0, s.y1].some((y) => depthOf(cam, { x, y, z: 0 }) < NEAREST),
			)
		) {
			continue;
		}
		const corner = (x: number, y: number, z: number) =>
			project(cam, { x, y, z });
		const faces: [number, number][][] = [
			[
				[s.x0, s.y0],
				[s.x1, s.y0],
				[s.x1, s.y1],
				[s.x0, s.y1],
			],
		];
		for (const [a, b] of [
			[faces[0]![0]!, faces[0]![1]!],
			[faces[0]![1]!, faces[0]![2]!],
			[faces[0]![2]!, faces[0]![3]!],
			[faces[0]![3]!, faces[0]![0]!],
		] as const) {
			const q = [
				corner(a[0], a[1], 0),
				corner(b[0], b[1], 0),
				corner(b[0], b[1], s.z),
				corner(a[0], a[1], s.z),
			];
			ctx.beginPath();
			q.forEach((v, i) => (i ? ctx.lineTo(v.x, v.y) : ctx.moveTo(v.x, v.y)));
			ctx.fill();
		}
		ctx.beginPath();
		faces[0]!.forEach(([x, y], i) => {
			const v = corner(x, y, s.z);
			if (i) {
				ctx.lineTo(v.x, v.y);
			} else {
				ctx.moveTo(v.x, v.y);
			}
		});
		ctx.fill();
	}
	const smoothing = ctx.imageSmoothingEnabled;
	ctx.imageSmoothingEnabled = true;
	for (const q of placed) {
		if (q.prop) {
			drawProp(ctx, cam, c, q.prop);
			continue;
		}
		const w = CS_CELL_W * q.k;
		const h = CS_CELL_H * q.k;
		ctx.drawImage(
			q.stood
				? q.waving
					? c.atlasWave
					: c.atlasUp
				: q.clapped
					? c.atlasClap
					: c.atlas,
			(q.cell % c.cols) * (cw + CS_GAP),
			Math.floor(q.cell / c.cols) * (ch + CS_GAP),
			cw,
			ch,
			q.x - w / 2,
			q.y - h,
			w,
			h,
		);
	}
	ctx.imageSmoothingEnabled = smoothing;
};
// Courtside, dressed for a night out.
const COURTSIDE_DRESS = [
	"#16171b",
	"#1c1f26",
	"#2b2f38",
	"#3b3f46",
	"#f2f2f0",
	"#e4e1da",
	"#24365c",
	"#5a4636",
	"#6b1f2a",
	"#c9b79c",
];

export const BOARD_SCREENS = [
	"name",
	"letsGo",
	"noise",
	"defense",
	"defense2",
	"three",
	"dunk",
	"andOne",
	"block",
] as const;
export type BoardScreen = (typeof BOARD_SCREENS)[number];

const luminance = (c: string): number => {
	const m = /^#?([\da-f]{6})$/i.exec(c.trim());
	if (!m) {
		return 0.5;
	}
	const n = Number.parseInt(m[1]!, 16);
	return (
		(0.299 * ((n >> 16) & 255) + 0.587 * ((n >> 8) & 255) + 0.114 * (n & 255)) /
		255
	);
};
// Lettering that reads on a background: white, or near-black on a light one.
const inkOn = (bg: string) => (luminance(bg) > 0.62 ? "#111216" : "#ffffff");

// One screen, painted across a board `w` x `h` px at `px` px to a foot.
const paintScreen = (
	w: number,
	h: number,
	px: number,
	screen: BoardScreen,
	home: ArenaTeam | undefined,
): HTMLCanvasElement => {
	const canvas = document.createElement("canvas");
	canvas.width = w;
	canvas.height = h;
	const ctx = canvas.getContext("2d")!;
	ctx.fillStyle = "#06070a";
	ctx.fillRect(0, 0, w, h);
	const c0 = teamColor(home, 0, "#8c1d40");
	const c1 = teamColor(home, 1, "#f2c14e");
	// A second color too close to black reads as nothing on a dark board.
	const accent = luminance(c1) < 0.16 ? "#f4f4f4" : c1;
	const name = (home?.name || home?.abbrev || "").toUpperCase();
	const region = (home?.region || "").toUpperCase();
	ctx.textAlign = "center";
	ctx.textBaseline = "middle";
	const font = (f: number) => {
		ctx.font = `800 ${Math.max(6, Math.round(h * f))}px Arial, sans-serif`;
	};
	// Panels of `seg` feet across the board, each painted by `panel`.
	const panels = (
		seg: number,
		panel: (i: number, x: number, sw: number) => void,
	) => {
		const sw = seg * px;
		for (let i = 0, x = 0; x < w; i++, x += sw) {
			panel(i, x, sw);
		}
	};
	const block = (x: number, sw: number, bg: string) => {
		ctx.fillStyle = bg;
		ctx.fillRect(x + 2, h * 0.12, sw - 4, h * 0.76);
	};
	const text = (t: string, x: number, color: string, f = 0.5) => {
		font(f);
		ctx.fillStyle = color;
		ctx.fillText(t, x, h * 0.53);
	};
	switch (screen) {
		case "name":
			panels(22, (i, x, sw) => {
				block(x, sw, i % 2 ? c0 : "#0b0c10");
				text(
					i % 2 || !region ? name : region,
					x + sw / 2,
					i % 2 ? inkOn(c0) : accent,
				);
			});
			break;
		case "letsGo":
			panels(30, (i, x, sw) => {
				block(x, sw, c0);
				text(`LET'S GO ${name}`, x + sw / 2, inkOn(c0), 0.46);
			});
			break;
		case "noise":
			panels(30, (i, x, sw) => {
				// A level meter either side of the words.
				for (let k = 0; k < 6; k++) {
					const bh = h * (0.2 + 0.1 * ((k * 7 + i * 3) % 6));
					ctx.fillStyle = k % 2 ? accent : c0;
					ctx.fillRect(x + (1 + k) * px * 0.9, h - bh - h * 0.1, px * 0.6, bh);
					ctx.fillRect(
						x + sw - (2 + k) * px * 0.9,
						h - bh - h * 0.1,
						px * 0.6,
						bh,
					);
				}
				text("MAKE SOME NOISE", x + sw / 2, accent, 0.44);
			});
			break;
		case "defense":
		case "defense2": {
			const lit = screen === "defense";
			panels(18, (i, x, sw) => {
				const bg = lit === (i % 2 === 0) ? c0 : "#0b0c10";
				block(x, sw, bg);
				text("DEFENSE", x + sw / 2, bg === c0 ? inkOn(c0) : accent, 0.56);
			});
			break;
		}
		case "three":
		case "dunk":
		case "andOne":
		case "block": {
			const word = {
				three: "THREE!",
				dunk: "SLAM DUNK!",
				andOne: "AND ONE!",
				block: "REJECTED!",
			}[screen];
			panels(20, (i, x, sw) => {
				const bg = i % 2 ? accent : c0;
				block(x, sw, bg);
				text(word, x + sw / 2, inkOn(bg), 0.58);
			});
			break;
		}
	}
	return canvas;
};

// Every screen, for the boards along the front of the stands and the ribbon
// round the upper deck - painted once a game, switched between as it goes.
export type BoardSet = "wall" | "ribbon" | "end" | "table" | "jumbo";
export const paintBoards = (
	home: ArenaTeam | undefined,
	away?: ArenaTeam,
): Record<BoardSet, Record<BoardScreen, HTMLCanvasElement>> => {
	const out = { wall: {}, ribbon: {}, end: {}, table: {}, jumbo: {} } as Record<
		BoardSet,
		Record<BoardScreen, HTMLCanvasElement>
	>;
	const trim = teamColor(home, 1, "#f2c14e");
	for (const screen of BOARD_SCREENS) {
		out.end[screen] = paintScreen(
			END_WALL[0].w,
			END_WALL[0].h,
			END_WALL[0].w / (END_Y1 - END_Y0),
			screen,
			home,
		);
		// The scorer's table is one long LED board too, with a strip of the
		// team's color along its top edge.
		const table = paintScreen(
			TABLE_FRONT.w,
			TABLE_FRONT.h,
			TABLE_FRONT.w / (TABLE_X1 - TABLE_X0),
			screen,
			home,
		);
		const tctx = table.getContext("2d")!;
		const edge = Math.max(2, Math.round(TABLE_FRONT.h * 0.09));
		tctx.fillStyle = "#0b0c10";
		tctx.fillRect(0, 0, table.width, edge * 2);
		tctx.fillStyle = trim;
		tctx.fillRect(0, edge * 0.6, table.width, edge);
		out.table[screen] = table;
		out.jumbo[screen] = paintJumbo(screen, home, away);
		out.wall[screen] = paintScreen(
			LED_WALL.w,
			LED_WALL.h,
			LED_WALL.w / (X1 - X0),
			screen,
			home,
		);
		out.ribbon[screen] = paintScreen(
			RIBBON.w,
			RIBBON.h,
			RIBBON.w / (X1 - X0),
			screen,
			home,
		);
	}
	return out;
};

// The board over center court: the screen above, and the two teams in
// their colors along the bottom (their scores beside them - see
// jumboScored).
const paintJumbo = (
	screen: BoardScreen,
	home: ArenaTeam | undefined,
	away: ArenaTeam | undefined,
): HTMLCanvasElement => {
	const { w, h } = JUMBO_FRONT;
	const px = w / JUMBO_W;
	const canvas = document.createElement("canvas");
	canvas.width = w;
	canvas.height = h;
	const ctx = canvas.getContext("2d")!;
	ctx.fillStyle = "#0b0c10";
	ctx.fillRect(0, 0, w, h);
	const strip = Math.round(3 * px);
	const edge = Math.max(1, Math.round(0.4 * px));
	// The screen: words to fill it, a line or two, on the team's color.
	const c0 = teamColor(home, 0, "#8c1d40");
	const c1 = teamColor(home, 1, "#f2c14e");
	const accent = luminance(c1) < 0.16 ? "#f4f4f4" : c1;
	const name = (home?.name || home?.abbrev || "").toUpperCase();
	const region = (home?.region || "").toUpperCase();
	const lines: Record<BoardScreen, string[]> = {
		name: region ? [region, name] : [name],
		letsGo: ["LET'S GO", name],
		noise: ["MAKE SOME", "NOISE!"],
		defense: ["DEFENSE"],
		defense2: ["DEFENSE"],
		three: ["THREE!"],
		dunk: ["SLAM", "DUNK!"],
		andOne: ["AND ONE!"],
		block: ["REJECTED!"],
	};
	const inverted = screen === "defense2";
	const sx = edge;
	const sy = edge;
	const sw = w - 2 * edge;
	const sh = h - strip - 2 * edge;
	const g = ctx.createLinearGradient(0, sy, 0, sy + sh);
	g.addColorStop(0, inverted ? "#0b0c10" : shade(c0, 0.12));
	g.addColorStop(1, inverted ? "#16171c" : shade(c0, -0.3));
	ctx.fillStyle = g;
	ctx.fillRect(sx, sy, sw, sh);
	const words = lines[screen];
	const lh = sh / (words.length + 0.4);
	ctx.textAlign = "center";
	ctx.textBaseline = "middle";
	words.forEach((word, i) => {
		let size = lh * (words.length > 1 && i === 0 ? 0.62 : 0.86);
		ctx.font = `800 ${Math.round(size)}px Arial, sans-serif`;
		const tw = ctx.measureText(word).width;
		if (tw > sw * 0.9) {
			size *= (sw * 0.9) / tw;
			ctx.font = `800 ${Math.round(size)}px Arial, sans-serif`;
		}
		ctx.fillStyle = inverted ? c0 : i === words.length - 1 ? accent : inkOn(c0);
		ctx.fillText(word, sx + sw / 2, sy + lh * (i + 0.7));
	});
	paintJumboStrip(ctx, home, away);
	ctx.fillStyle = teamColor(home, 1, "#f2c14e");
	ctx.fillRect(0, 0, w, edge);
	jumboTeams.set(canvas, { home, away });
	return canvas;
};

// The strip along its foot: each side in its colors, road team first -
// and, once there is one, its score beside it in white on black, the way
// the board in a building carries it.
export const paintJumboStrip = (
	ctx: CanvasRenderingContext2D,
	home: ArenaTeam | undefined,
	away: ArenaTeam | undefined,
	score?: [number, number],
) => {
	const { w, h } = JUMBO_FRONT;
	const px = w / JUMBO_W;
	const strip = Math.round(3 * px);
	const edge = Math.max(1, Math.round(0.4 * px));
	const half = (w - 3 * edge) / 2;
	const y = h - strip;
	const sh = strip - edge;
	ctx.textBaseline = "middle";
	([away, home] as const).forEach((team, i) => {
		const x = edge + i * (half + edge);
		const name = (team?.abbrev ?? (i ? "HOME" : "AWAY")).toUpperCase();
		const bg = teamColor(team, 0, i ? "#8c1d40" : "#1d3461");
		// (The score's box: about two fifths of the side.)
		const box = score ? Math.round(half * 0.42) : 0;
		const nw = half - box;
		ctx.fillStyle = bg;
		ctx.fillRect(x, y, nw, sh);
		ctx.fillStyle = luminance(bg) > 0.6 ? "#111" : "#fff";
		let size = strip * 0.62;
		ctx.font = `800 ${Math.round(size)}px Arial, sans-serif`;
		const tw = ctx.measureText(name).width;
		if (tw > nw * 0.86) {
			size *= (nw * 0.86) / tw;
			ctx.font = `800 ${Math.round(size)}px Arial, sans-serif`;
		}
		ctx.textAlign = "center";
		ctx.fillText(name, x + nw / 2, y + sh / 2);
		if (score) {
			ctx.fillStyle = "#050507";
			ctx.fillRect(x + nw, y, box, sh);
			ctx.fillStyle = "#ffffff";
			const digits = String(score[i]);
			let ds = strip * 0.74;
			ctx.font = `800 ${Math.round(ds)}px Arial, sans-serif`;
			const dw = ctx.measureText(digits).width;
			if (dw > box * 0.86) {
				ds *= (box * 0.86) / dw;
				ctx.font = `800 ${Math.round(ds)}px Arial, sans-serif`;
			}
			ctx.fillText(digits, x + nw + box / 2, y + sh / 2);
		}
	});
};

// THE SCORE ON THE BOARD over center court, as the game goes: the board's
// picture for a screen with the score in its strip - made again only when
// the score changes, and only while the board is in the picture (a look
// round the building).
const jumboTeams = new WeakMap<
	HTMLCanvasElement,
	{ home: ArenaTeam | undefined; away: ArenaTeam | undefined }
>();
const jumboScores = new WeakMap<
	HTMLCanvasElement,
	{ key: string; canvas: HTMLCanvasElement }
>();
export const jumboScored = (
	face: HTMLCanvasElement,
	score: [number, number],
): HTMLCanvasElement => {
	const teams = jumboTeams.get(face);
	if (!teams) {
		return face;
	}
	const key = `${score[0]}-${score[1]}`;
	const was = jumboScores.get(face);
	if (was?.key === key) {
		return was.canvas;
	}
	const canvas = document.createElement("canvas");
	canvas.width = face.width;
	canvas.height = face.height;
	const ctx = canvas.getContext("2d")!;
	ctx.drawImage(face, 0, 0);
	paintJumboStrip(ctx, teams.home, teams.away, score);
	jumboScores.set(face, { key, canvas });
	return canvas;
};

// Hung from the roof: its top, a side if the camera is off to one, and the
// cables up into the dark. (Drawn only when most of it is in the picture -
// over a break, looking round the building - not as a strip along the top
// edge of the game.)
export const drawJumbo = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	face: HTMLCanvasElement,
	draw: (plane: Plane, img: HTMLCanvasElement) => void,
) => {
	const x0 = COURT_W / 2 - JUMBO_W / 2;
	const x1 = COURT_W / 2 + JUMBO_W / 2;
	const y1 = JUMBO_Y;
	const y0 = y1 - JUMBO_D;
	const z0 = JUMBO_Z;
	const z1 = JUMBO_Z + JUMBO_H;
	const bottom = project(cam, { x: COURT_W / 2, y: y1, z: z0 });
	if (bottom.y < cam.viewH * 0.3) {
		return;
	}
	ctx.strokeStyle = "#2a2c33";
	ctx.lineWidth = Math.max(1, 0.12 * bottom.k);
	for (const x of [x0 + 3, x1 - 3]) {
		for (const y of [y0 + 3, y1 - 3]) {
			const a = project(cam, { x, y, z: z1 });
			const b = project(cam, { x, y, z: z1 + 60 });
			ctx.beginPath();
			ctx.moveTo(a.x, a.y);
			ctx.lineTo(b.x, b.y);
			ctx.stroke();
		}
	}
	const quad = (pts: Pt3[], fill: string) => {
		ctx.fillStyle = fill;
		poly(ctx, cam, pts);
		ctx.fill();
	};
	if (cam.pos.x < x0) {
		quad(
			[
				{ x: x0, y: y0, z: z0 },
				{ x: x0, y: y1, z: z0 },
				{ x: x0, y: y1, z: z1 },
				{ x: x0, y: y0, z: z1 },
			],
			"#14151a",
		);
	} else if (cam.pos.x > x1) {
		quad(
			[
				{ x: x1, y: y0, z: z0 },
				{ x: x1, y: y1, z: z0 },
				{ x: x1, y: y1, z: z1 },
				{ x: x1, y: y0, z: z1 },
			],
			"#14151a",
		);
	}
	if (cam.pos.z > z1) {
		quad(
			[
				{ x: x0, y: y0, z: z1 },
				{ x: x1, y: y0, z: z1 },
				{ x: x1, y: y1, z: z1 },
				{ x: x0, y: y1, z: z1 },
			],
			"#1b1c21",
		);
	} else if (cam.pos.z < z0) {
		quad(
			[
				{ x: x0, y: y0, z: z0 },
				{ x: x1, y: y0, z: z0 },
				{ x: x1, y: y1, z: z0 },
				{ x: x0, y: y1, z: z0 },
			],
			"#101115",
		);
	}
	draw(JUMBO_FRONT, face);
};

// ---- the rafters -------------------------------------------------------------

// What hangs there: the championships won, each year its own banner (or,
// for a dynasty, a few years to a banner), and the numbers retired - each in
// the colors (and a title's with the logo) the team had then.
export type RafterInfo = {
	titles: number[];
	titleLooks?: { colors: [string, string, string]; imgURL?: string }[];
	retired: {
		number: string;
		name?: string;
		colors?: [string, string, string];
	}[];
};

// A banner as the game draws one on the playoffs and team history pages,
// in its own units (see ChampionshipBanner): 164 across its top bar, a
// field in the team's first color edged in its third, the bar across the
// top, and a notched tail.
const BANNER_W = 164;

export const paintRafters = (
	home: ArenaTeam | undefined,
	info: RafterInfo | undefined,
	logos?: Map<string, HTMLCanvasElement>,
): HTMLCanvasElement => {
	const { w, h } = RAFTERS;
	const px = w / (X1 - X0);
	const canvas = document.createElement("canvas");
	canvas.width = w;
	canvas.height = h;
	const ctx = canvas.getContext("2d")!;
	const c0 = teamColor(home, 0, "#8c1d40");
	const c1 = teamColor(home, 1, "#f2c14e");
	const trim = luminance(c1) < 0.16 || c1 === c0 ? "#f4f4f4" : c1;
	const X = (x: number) => (x - X0) * px;

	// The truss: two chords and the lattice between them, and the lights.
	const top = 0.6 * px;
	const bottom = 2.2 * px;
	ctx.strokeStyle = "#2a2d34";
	ctx.lineWidth = Math.max(2, 0.28 * px);
	ctx.beginPath();
	ctx.moveTo(0, top);
	ctx.lineTo(w, top);
	ctx.moveTo(0, bottom);
	ctx.lineTo(w, bottom);
	for (let x = 0, k = 0; x < w; x += 3 * px, k++) {
		ctx.moveTo(x, k % 2 ? top : bottom);
		ctx.lineTo(x + 3 * px, k % 2 ? bottom : top);
	}
	ctx.stroke();
	for (let x = LIGHT_FIRST; x < X1 - X0; x += LIGHT_GAP) {
		const cx = x * px;
		const g = ctx.createRadialGradient(
			cx,
			bottom + 2,
			0,
			cx,
			bottom + 2,
			1.6 * px,
		);
		g.addColorStop(0, "rgba(255,250,232,0.95)");
		g.addColorStop(0.35, "rgba(255,244,214,0.35)");
		g.addColorStop(1, "rgba(255,244,214,0)");
		ctx.fillStyle = g;
		ctx.fillRect(cx - 1.6 * px, bottom - 1.4 * px, 3.2 * px, 3.2 * px);
	}

	const titles = info?.titles ?? [];
	const retired = info?.retired ?? [];
	// A dynasty's banners carry several years each, so they all fit.
	const per = Math.max(1, Math.ceil(titles.length / 12));
	const titleLooks = info?.titleLooks ?? [];
	const groups: { years: number[]; look?: (typeof titleLooks)[number] }[] = [];
	for (let i = 0; i < titles.length; i += per) {
		const last = Math.min(titles.length, i + per) - 1;
		groups.push({ years: titles.slice(i, i + per), look: titleLooks[last] });
	}
	const n = groups.length + retired.length;
	if (n === 0) {
		return canvas;
	}
	const bw = 5;
	const pitch = Math.min(6.8, 150 / n);
	const x0 = COURT_W / 2 - (pitch * (n - 1)) / 2 - bw / 2;
	const hang = bottom + 0.3 * px;
	const own = [c0, c1, trim] as const;
	// Its colors: [field, lettering, edge] - lettering that reads on the
	// field whatever they were.
	const colorsOf = (colors: readonly string[] | undefined) => {
		const field = colors?.[0] || own[0];
		const edge = colors?.[2] || own[2];
		const letters = colors?.[1] || own[1];
		return [
			field,
			Math.abs(luminance(letters) - luminance(field)) < 0.2
				? inkOn(field)
				: letters,
			edge,
		] as const;
	};
	const banner = (
		i: number,
		colors: readonly string[] | undefined,
		paint: (
			at: (x: number, y: number) => [number, number],
			s: number,
			ink: string,
		) => void,
	) => {
		const [field, ink, edge] = colorsOf(colors);
		const s = (Math.min(bw, pitch - 0.6) * px) / BANNER_W;
		const left = X(x0 + i * pitch) - 9 * s;
		const top = hang - 16 * s;
		const at = (x: number, y: number): [number, number] => [
			left + x * s,
			top + y * s,
		];
		// The wires it hangs from.
		ctx.strokeStyle = "rgba(160,160,170,0.6)";
		ctx.lineWidth = 1;
		ctx.beginPath();
		for (const x of [15, 167]) {
			ctx.moveTo(...at(x, 0));
			ctx.lineTo(at(x, 0)[0], bottom);
		}
		ctx.stroke();
		const path = (pts: [number, number][]) => {
			ctx.beginPath();
			pts.forEach(([x, y], k) => {
				if (k === 0) {
					ctx.moveTo(...at(x, y));
				} else {
					ctx.lineTo(...at(x, y));
				}
			});
			ctx.closePath();
		};
		ctx.fillStyle = field;
		path([
			[12, 23],
			[170, 23],
			[170, 222.9],
			[91, 247],
			[12, 222.9],
		]);
		ctx.fill();
		ctx.strokeStyle = edge;
		ctx.lineWidth = Math.max(1, 4 * s);
		path([
			[14, 25],
			[168, 25],
			[168, 221.4],
			[91, 244.9],
			[14, 221.4],
		]);
		ctx.stroke();
		ctx.fillStyle = field;
		ctx.lineWidth = Math.max(1, 3 * s);
		ctx.beginPath();
		ctx.roundRect(...at(9, 16), 164 * s, 16 * s, 6 * s);
		ctx.fill();
		ctx.stroke();
		paint(at, s, ink);
	};
	ctx.textAlign = "center";
	ctx.textBaseline = "middle";
	groups.forEach(({ years, look }, i) => {
		banner(i, look?.colors, (at, s, ink) => {
			const [cx] = at(91, 0);
			// The year (a dynasty's, stacked), the logo, and the words.
			ctx.fillStyle = ink;
			const big = years.length === 1 ? 36 : years.length === 2 ? 22 : 15;
			years.forEach((y, k) => {
				ctx.font = `600 ${Math.max(1, Math.round(big * s))}px Arial, sans-serif`;
				ctx.fillText(
					String(y),
					cx,
					at(0, 68 + (k - (years.length - 1) / 2) * big * 1.05)[1],
				);
			});
			const logo = look?.imgURL ? logos?.get(look.imgURL) : undefined;
			const [, y0] = at(0, 102);
			if (logo) {
				const k = Math.min((136 * s) / logo.width, (74 * s) / logo.height);
				const lw = logo.width * k;
				const lh = logo.height * k;
				ctx.drawImage(logo, cx - lw / 2, y0 + (74 * s - lh) / 2, lw, lh);
			} else {
				// No logo to be had: the trophy.
				const ty = y0 + 26 * s;
				const u = 40 * s;
				ctx.fillStyle = "#e8c24a";
				ctx.beginPath();
				ctx.moveTo(cx - u * 0.5, ty - u * 0.35);
				ctx.lineTo(cx + u * 0.5, ty - u * 0.35);
				ctx.lineTo(cx + u * 0.22, ty + u * 0.3);
				ctx.lineTo(cx - u * 0.22, ty + u * 0.3);
				ctx.closePath();
				ctx.fill();
				ctx.fillRect(cx - u * 0.08, ty + u * 0.3, u * 0.16, u * 0.25);
				ctx.fillRect(cx - u * 0.3, ty + u * 0.52, u * 0.6, u * 0.14);
			}
			ctx.fillStyle = ink;
			ctx.font = `600 ${Math.max(1, Math.round(16 * s))}px Arial, sans-serif`;
			ctx.fillText("League Champions", cx, at(0, 194)[1], 150 * s);
		});
	});
	retired.forEach((r, j) => {
		banner(groups.length + j, r.colors, (at, s, ink) => {
			const [cx] = at(91, 0);
			ctx.fillStyle = ink;
			if (r.name) {
				ctx.font = `600 ${Math.max(1, Math.round(22 * s))}px Arial, sans-serif`;
				ctx.fillText(r.name.toUpperCase(), cx, at(0, 62)[1], 140 * s);
			}
			const digits = r.number.length;
			ctx.font = `700 ${Math.max(1, Math.round((digits > 2 ? 64 : 92) * s))}px Arial, sans-serif`;
			ctx.fillText(r.number, cx, at(0, 140)[1], 140 * s);
		});
	});
	return canvas;
};

// The championship trophy in the middle of the scorer's table front, for a
// finals game whose floor picture keeps its own center court.
export const withTrophy = (
	board: HTMLCanvasElement,
	trophy: HTMLCanvasElement,
): HTMLCanvasElement => {
	const out = document.createElement("canvas");
	out.width = board.width;
	out.height = board.height;
	const ctx = out.getContext("2d")!;
	ctx.drawImage(board, 0, 0);
	const h = out.height * 0.86;
	const w = (trophy.width / trophy.height) * h;
	const x = (out.width - w) / 2;
	ctx.fillStyle = "#0b0c10";
	ctx.fillRect(x - w * 0.35, 0, w * 1.7, out.height);
	ctx.drawImage(trophy, x, out.height - h, w, h);
	return out;
};

export const paintTable = (
	home: ArenaTeam | undefined,
	away: ArenaTeam | undefined,
): { front: HTMLCanvasElement; top: HTMLCanvasElement } => {
	const front = document.createElement("canvas");
	front.width = TABLE_FRONT.w;
	front.height = TABLE_FRONT.h;
	const ctx = front.getContext("2d")!;
	const w = front.width;
	const h = front.height;
	ctx.fillStyle = "#0c0d11";
	ctx.fillRect(0, 0, w, h);
	ctx.fillStyle = "#050507";
	ctx.fillRect(0, h * 0.2, w, h * 0.55);
	ctx.textAlign = "center";
	ctx.textBaseline = "middle";
	ctx.font = `800 ${Math.round(h * 0.34)}px Arial, sans-serif`;
	ctx.fillStyle = teamColor(away, 0, "#1d3461");
	ctx.fillRect(w * 0.04, h * 0.26, w * 0.28, h * 0.43);
	ctx.fillStyle = teamColor(home, 0, "#8c1d40");
	ctx.fillRect(w * 0.68, h * 0.26, w * 0.28, h * 0.43);
	ctx.fillStyle = "#ffffff";
	ctx.fillText((away?.abbrev ?? "").toUpperCase(), w * 0.18, h * 0.48);
	ctx.fillText((home?.abbrev ?? "").toUpperCase(), w * 0.82, h * 0.48);
	ctx.fillStyle = "#ffb547";
	ctx.fillText("VS", w * 0.5, h * 0.48);
	const top = document.createElement("canvas");
	top.width = TABLE_TOP.w;
	top.height = TABLE_TOP.h;
	const tctx = top.getContext("2d")!;
	tctx.fillStyle = "#2a2c33";
	tctx.fillRect(0, 0, top.width, top.height);
	tctx.fillStyle = "#3a3d46";
	tctx.fillRect(0, top.height * 0.7, top.width, top.height * 0.3);
	return { front, top };
};

export const paintBench = (team: ArenaTeam | undefined): HTMLCanvasElement => {
	const p = benchPlane(0);
	const canvas = document.createElement("canvas");
	canvas.width = p.w;
	canvas.height = p.h;
	const ctx = canvas.getContext("2d")!;
	const px = p.w / BENCH_LEN;
	const c = teamColor(team, 0, "#333a44");
	const trim = teamColor(team, 1, "#d9d9d9");
	const seat = BENCH_SEAT_Z * px;
	for (let i = 0; i < BENCH_SEATS; i++) {
		const x = (0.15 + i * SEAT_GAP) * px;
		const w = 1.8 * px;
		// Legs, the seat's padded front, the high back - a stripe of the
		// team's other color across its top.
		ctx.fillStyle = "#111";
		ctx.fillRect(x + 0.15 * px, p.h - seat, 0.12 * px, seat);
		ctx.fillRect(x + w - 0.27 * px, p.h - seat, 0.12 * px, seat);
		ctx.fillStyle = shade(c, -0.25);
		ctx.beginPath();
		ctx.roundRect(x, p.h - seat - 0.3 * px, w, 0.38 * px, 0.12 * px);
		ctx.fill();
		const g = ctx.createLinearGradient(x, 0, x + w, 0);
		g.addColorStop(0, shade(c, -0.18));
		g.addColorStop(0.5, shade(c, 0.08));
		g.addColorStop(1, shade(c, -0.18));
		ctx.fillStyle = g;
		ctx.beginPath();
		ctx.roundRect(x, 0, w, p.h - seat - 0.2 * px, 0.35 * px);
		ctx.fill();
		ctx.fillStyle = trim;
		ctx.fillRect(x + 0.25 * px, 0.35 * px, w - 0.5 * px, 0.16 * px);
	}
	return canvas;
};

// ---- the baskets --------------------------------------------------------------

const line = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	a: Pt3,
	b: Pt3,
	width: number,
) => {
	const p = project(cam, a);
	const q = project(cam, b);
	ctx.lineWidth = Math.max(0.6, width * (p.k + q.k) * 0.5);
	ctx.beginPath();
	ctx.moveTo(p.x, p.y);
	ctx.lineTo(q.x, q.y);
	ctx.stroke();
};

// The same, inked: a dark edge drawn first, the color down its middle.
const INK = "rgba(22, 15, 13, 0.92)";
const inkLine = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	a: Pt3,
	b: Pt3,
	width: number,
	color: string,
) => {
	const p = project(cam, a);
	const q = project(cam, b);
	const k = (p.k + q.k) * 0.5;
	const w = Math.max(0.8, width * k);
	ctx.lineCap = "round";
	ctx.beginPath();
	ctx.moveTo(p.x, p.y);
	ctx.lineTo(q.x, q.y);
	ctx.strokeStyle = INK;
	ctx.lineWidth = w + 2 * Math.max(1, 0.05 * k);
	ctx.stroke();
	ctx.strokeStyle = color;
	ctx.lineWidth = w;
	ctx.stroke();
};

const poly = (ctx: CanvasRenderingContext2D, cam: Camera, pts: Pt3[]) => {
	ctx.beginPath();
	pts.forEach((pt, i) => {
		const p = project(cam, pt);
		if (i === 0) {
			ctx.moveTo(p.x, p.y);
		} else {
			ctx.lineTo(p.x, p.y);
		}
	});
	ctx.closePath();
};

// THE SHOT CLOCK on top of the board: a dark box, the seconds lit in red on
// each side of it - so from wherever the camera is, the side it sees has
// them, the way a broadcast shows it from the sideline. (Drawn flat across
// the top of the board, as it was, the side camera saw only its edge, and
// the seconds floated over the board on nothing.)
const CLOCK_HALF = 0.9;
const CLOCK_IN = 3.55;
const CLOCK_OUT = 4.55;
const CLOCK_LOW = 13.08;
const CLOCK_HIGH = 14.05;
const drawShotClock = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	X: (d: number) => number,
	rock: number,
	bz: number,
	shotClock: string,
	inkW: number,
) => {
	const xa = Math.min(X(CLOCK_IN), X(CLOCK_OUT));
	const xb = Math.max(X(CLOCK_IN), X(CLOCK_OUT));
	const ya = 25 - CLOCK_HALF + rock;
	const yb = 25 + CLOCK_HALF + rock;
	const z0 = CLOCK_LOW + bz;
	const z1 = CLOCK_HIGH + bz;
	const P = (x: number, y: number, z: number): Pt3 => ({ x, y, z });
	// Each side: its top corners as seen from outside it, left then right
	// (the floor's y runs toward the camera: see camera.ts), and which way
	// it faces. (The top is seen from above.)
	const sides: { tl: Pt3; tr: Pt3; nx: number; ny: number }[] = [
		{ tl: P(xa, yb, z1), tr: P(xb, yb, z1), nx: 0, ny: 1 },
		{ tl: P(xb, ya, z1), tr: P(xa, ya, z1), nx: 0, ny: -1 },
		{ tl: P(xb, yb, z1), tr: P(xb, ya, z1), nx: 1, ny: 0 },
		{ tl: P(xa, ya, z1), tr: P(xa, yb, z1), nx: -1, ny: 0 },
	];
	const seen = sides.filter(
		(s) =>
			(cam.pos.x - (s.tl.x + s.tr.x) / 2) * s.nx +
				(cam.pos.y - (s.tl.y + s.tr.y) / 2) * s.ny >
			0,
	);
	ctx.lineJoin = "round";
	ctx.strokeStyle = INK;
	ctx.lineWidth = 2 * inkW;
	const faces: Pt3[][] = seen.map((s) => [
		s.tl,
		s.tr,
		{ ...s.tr, z: z0 },
		{ ...s.tl, z: z0 },
	]);
	if (cam.pos.z > z1) {
		faces.push([P(xa, ya, z1), P(xb, ya, z1), P(xb, yb, z1), P(xa, yb, z1)]);
	}
	for (const f of faces) {
		poly(ctx, cam, f);
		ctx.stroke();
	}
	faces.forEach((f, i) => {
		ctx.fillStyle = i < seen.length ? "#0d0d10" : "#2a2b31";
		poly(ctx, cam, f);
		ctx.fill();
	});
	if (!shotClock) {
		return;
	}
	// The seconds on each side seen, laid on it: drawn in its own frame -
	// across it and down it, a unit a pixel of the picture.
	ctx.fillStyle = "#ff3b2f";
	ctx.textAlign = "center";
	ctx.textBaseline = "middle";
	for (const s of seen) {
		const tl = project(cam, s.tl);
		const tr = project(cam, s.tr);
		const bl = project(cam, { ...s.tl, z: z0 });
		const w = Math.hypot(tr.x - tl.x, tr.y - tl.y);
		const h = Math.hypot(bl.x - tl.x, bl.y - tl.y);
		// (Not on a side seen too nearly edge on to read: its area on the
		// picture against what it would have square on.)
		const across = Math.hypot(s.tr.x - s.tl.x, s.tr.y - s.tl.y);
		const k = (tl.k + tr.k) / 2;
		const area = Math.abs(
			(tr.x - tl.x) * (bl.y - tl.y) - (tr.y - tl.y) * (bl.x - tl.x),
		);
		if (h < 2.5 || area < 0.45 * across * (z1 - z0) * k * k) {
			continue;
		}
		ctx.save();
		ctx.transform(
			(tr.x - tl.x) / w,
			(tr.y - tl.y) / w,
			(bl.x - tl.x) / h,
			(bl.y - tl.y) / h,
			tl.x,
			tl.y,
		);
		ctx.font = `700 ${0.8 * h}px "Courier New", monospace`;
		const fit = (0.86 * w) / Math.max(1, ctx.measureText(shotClock).width);
		ctx.translate(w / 2, h * 0.53);
		if (fit < 1) {
			ctx.scale(fit, 1);
		}
		ctx.fillText(shotClock, 0, 0);
		ctx.restore();
	}
};

// The lit panel on the faces of a basket's base: the home team's name in
// white on its color, a glow along the edges - painted once.
export const paintStanchionPanel = (
	home: ArenaTeam | undefined,
	padColor: string,
	spec: ArenaSpec = arenaSpecFor(home),
): HTMLCanvasElement => {
	const canvas = document.createElement("canvas");
	canvas.width = 160;
	canvas.height = 52;
	const ctx = canvas.getContext("2d")!;
	const g = ctx.createLinearGradient(0, 0, 0, canvas.height);
	g.addColorStop(0, shade(padColor, 0.18));
	g.addColorStop(0.5, padColor);
	g.addColorStop(1, shade(padColor, -0.2));
	ctx.fillStyle = g;
	ctx.fillRect(0, 0, canvas.width, canvas.height);
	ctx.fillStyle = "rgba(255,255,255,0.35)";
	ctx.fillRect(0, 0, canvas.width, 2);
	ctx.fillRect(0, canvas.height - 2, canvas.width, 2);
	const name = spec.stanchion.panel;
	ctx.fillStyle = inkOn(padColor);
	ctx.textAlign = "center";
	ctx.textBaseline = "middle";
	let size = 34;
	ctx.font = `900 ${size}px sans-serif`;
	while (size > 12 && ctx.measureText(name).width > canvas.width - 16) {
		size -= 2;
		ctx.font = `900 ${size}px sans-serif`;
	}
	ctx.fillText(name, canvas.width / 2, canvas.height / 2 + 1);
	return canvas;
};

export type HoopFx = {
	// 0..1, fading: the net snapping up after a make, the iron ringing after a
	// miss, the rim bent by a dunk.
	swish: number;
	clank: number;
	dunk: number;
	// A dunk worth a replay rattles the whole basket harder.
	big?: boolean;
	// The period's clock run out: the lights round the edge of the glass on,
	// red (see drawHoop).
	out?: boolean;
	t: number;
};

// The basket at one end: stanchion, backboard and its shot clock, then the
// rim and net - with `between` drawn inside them (the ball going through).
export const drawHoop = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	side: Side,
	padColor: string,
	shotClock: string,
	fx: HoopFx,
	between?: () => void,
	// The lit panel on the faces of its base (see paintStanchionPanel).
	panel?: HTMLCanvasElement,
) => {
	const sgn = side === 0 ? 1 : -1;
	const base = side === 0 ? 0 : COURT_W;
	const X = (d: number) => base + sgn * d;
	// After a dunk the whole basket rings: the board and rim shake up and
	// down and rock side to side, dying away over a second; while he hangs
	// on it the rim is bent down.
	const ringing = fx.dunk * fx.dunk * (fx.big ? 1.5 : 1);
	const shake =
		fx.clank * 0.06 * Math.sin(fx.t / 22) +
		ringing * 0.42 * Math.sin(fx.t / 22);
	const rock = ringing * 0.16 * Math.sin(fx.t / 31);
	const bend = Math.max(0, (fx.dunk - 0.55) / 0.45) * 0.32;

	const bx = X(4);
	const bz = shake;
	// From behind the board - a dunk's replay, looking up the lane from off
	// the baseline - the stanchion is nearest and the rim is seen through
	// the glass: drawn the other way round.
	const behind = (cam.pos.x - bx) * sgn < 0;
	const inkW = Math.max(1, 0.05 * project(cam, { x: X(4), y: 25, z: 0 }).k);
	const stanchion = () => {
		// The stanchion: a dark padded base behind the baseline with a lit
		// board in the team's color on its faces, the post, and the arm out to
		// the board with a sign along it.
		const b0 = X(-9.5);
		const b1 = X(-5);
		const box = (y0: number, y1: number, z1: number) => {
			const pts = (x: number, y: number, z: number): Pt3 => ({ x, y, z });
			// Top, the face toward the court, the face toward the camera.
			ctx.lineJoin = "round";
			ctx.lineWidth = inkW * 2;
			ctx.strokeStyle = INK;
			for (const [fill, face] of [
				[
					"#24262c",
					[pts(b0, y0, z1), pts(b1, y0, z1), pts(b1, y1, z1), pts(b0, y1, z1)],
				],
				[
					"#17181c",
					[pts(b1, y0, 0), pts(b1, y1, 0), pts(b1, y1, z1), pts(b1, y0, z1)],
				],
				[
					"#101115",
					[pts(b0, y1, 0), pts(b1, y1, 0), pts(b1, y1, z1), pts(b0, y1, z1)],
				],
			] as const) {
				poly(ctx, cam, [...face]);
				ctx.stroke();
				ctx.fillStyle = fill;
				ctx.fill();
			}
			// The boards on its faces: the team's color, a bright band across
			// - or the team's name, lit, read left to right from the floor.
			const lit = (face: (u: number, v: number) => Pt3) => {
				const quad = (v0: number, v1: number, fill: string) => {
					ctx.fillStyle = fill;
					poly(ctx, cam, [face(0, v0), face(1, v0), face(1, v1), face(0, v1)]);
					ctx.fill();
				};
				quad(0.15, 0.85, padColor);
				if (!panel) {
					quad(0.44, 0.56, shade(padColor, 0.45));
					return;
				}
				const o = face(0, 0.85);
				const a = face(1, 0.85);
				const b = face(0, 0.15);
				drawTexturedPlane(
					ctx,
					cam,
					{
						origin: o,
						alongX: {
							x: (a.x - o.x) / panel.width,
							y: (a.y - o.y) / panel.width,
							z: (a.z - o.z) / panel.width,
						},
						alongY: {
							x: (b.x - o.x) / panel.height,
							y: (b.y - o.y) / panel.height,
							z: (b.z - o.z) / panel.height,
						},
						w: panel.width,
						h: panel.height,
					},
					panel,
					4,
					1,
				);
			};
			const lo = 0.5;
			const hi = z1 - 0.45;
			// (Left to right as seen from the floor: along the baseline the far
			// way round at the left basket; across the end toward the court at
			// the left basket, away from it at the right.)
			lit((u, v) => {
				const w = side === 0 ? 1 - u : u;
				return pts(b1, y0 + 0.35 + w * (y1 - y0 - 0.7), lo + v * (hi - lo));
			});
			lit((u, v) => {
				const w = side === 0 ? u : 1 - u;
				return pts(b0 + (b1 - b0) * (0.06 + w * 0.88), y1, lo + v * (hi - lo));
			});
		};
		box(22.4, 27.6, 3.4);
		inkLine(
			ctx,
			cam,
			{ x: X(-6.6), y: 25, z: 3.4 },
			{ x: X(-6.2), y: 25, z: 12.4 },
			0.75,
			"#24262c",
		);
		inkLine(
			ctx,
			cam,
			{ x: X(-6.2), y: 25, z: 12.4 },
			{ x: X(3.7), y: 25 + rock, z: 11.9 + shake },
			0.48,
			"#2a2c33",
		);
		inkLine(
			ctx,
			cam,
			{ x: X(-6.4), y: 25, z: 8.6 },
			{ x: X(3.7), y: 25 + rock, z: 10.2 + shake },
			0.3,
			"#2a2c33",
		);
		// The sign along the arm, on the side the camera sees.
		{
			const along = (d: number) => (d + 6.3) / 10;
			const top = (d: number) => 12.4 + (11.9 + shake - 12.4) * along(d);
			const low = (d: number) => 8.6 + (10.2 + shake - 8.6) * along(d);
			const at = (d: number, z: number): Pt3 => ({
				x: X(d),
				y: 25.3 + rock * along(d),
				z,
			});
			const [d0, d1] = [-4.3, 2.1];
			// (Inked round, so it reads as a sign against a floor its own color.)
			ctx.lineJoin = "round";
			ctx.strokeStyle = INK;
			ctx.lineWidth = 2 * inkW;
			ctx.fillStyle = padColor;
			poly(ctx, cam, [
				at(d0, low(d0) + 0.35),
				at(d1, low(d1) + 0.35),
				at(d1, top(d1) - 0.35),
				at(d0, top(d0) - 0.35),
			]);
			ctx.stroke();
			ctx.fill();
			ctx.fillStyle = "rgba(255,255,255,0.85)";
			const mid = (d: number) => (low(d) + top(d)) / 2;
			poly(ctx, cam, [
				at(d0 + 0.6, mid(d0) - 0.18),
				at(d1 - 0.6, mid(d1) - 0.18),
				at(d1 - 0.6, mid(d1) + 0.18),
				at(d0 + 0.6, mid(d0) + 0.18),
			]);
			ctx.fill();
		}
		// Padding round the bottom of the post.
		inkLine(
			ctx,
			cam,
			{ x: X(-6.6), y: 25, z: 3.4 },
			{ x: X(-6.5), y: 25, z: 7.4 },
			1.15,
			padColor,
		);
	};
	const board = () => {
		// The backboard: glass in a white frame, the shooter's square, and the
		// shot clock on top.
		const glass = [
			{ x: bx, y: 22 + rock, z: 9.5 + bz },
			{ x: bx, y: 28 + rock, z: 9.5 + bz },
			{ x: bx, y: 28 + rock, z: 13 + bz },
			{ x: bx, y: 22 + rock, z: 13 + bz },
		];
		const gk = project(cam, glass[0]!).k;
		ctx.fillStyle = "rgba(214, 232, 244, 0.34)";
		poly(ctx, cam, glass);
		ctx.fill();
		ctx.lineJoin = "round";
		ctx.strokeStyle = INK;
		ctx.lineWidth = Math.max(0.8, 0.22 * gk) + 2 * inkW;
		ctx.stroke();
		ctx.strokeStyle = "#ffffff";
		ctx.lineWidth = Math.max(0.8, 0.22 * gk);
		ctx.stroke();
		// The period over - its clock at zero - the strip of lights round the
		// inside of the frame is lit red, the way an NBA board's is at the horn,
		// and stays lit until the clock is set again.
		if (fx.out) {
			const inset = (p: Pt3, dy: number, dz: number): Pt3 => ({
				...p,
				y: p.y + dy,
				z: p.z + dz,
			});
			const d = 0.16;
			poly(ctx, cam, [
				inset(glass[0]!, d, d),
				inset(glass[1]!, -d, d),
				inset(glass[2]!, -d, -d),
				inset(glass[3]!, d, -d),
			]);
			ctx.strokeStyle = "rgba(255, 52, 36, 0.35)";
			ctx.lineWidth = Math.max(1.5, 0.34 * gk);
			ctx.stroke();
			ctx.strokeStyle = "#ff4a32";
			ctx.lineWidth = Math.max(0.8, 0.12 * gk);
			ctx.stroke();
		}
		// The padding along the bottom of the board.
		inkLine(
			ctx,
			cam,
			{ x: bx, y: 22 + rock, z: 9.45 + bz },
			{ x: bx, y: 28 + rock, z: 9.45 + bz },
			0.3,
			padColor,
		);
		ctx.strokeStyle = "rgba(255,255,255,0.92)";
		ctx.lineWidth = Math.max(0.6, 0.12 * gk);
		poly(ctx, cam, [
			{ x: bx, y: 24 + rock, z: 10.05 + bz },
			{ x: bx, y: 26 + rock, z: 10.05 + bz },
			{ x: bx, y: 26 + rock, z: 11.5 + bz },
			{ x: bx, y: 24 + rock, z: 11.5 + bz },
		]);
		ctx.stroke();
		drawShotClock(ctx, cam, X, rock, bz, shotClock, inkW);
	};
	const rimAndNet = () => {
		// The rim and its net.
		const rx = X(5.25);
		const rz = RIM_Z - bend + shake * 0.5;
		const N = 16;
		const ring = (r: number, z: number, sway = 0): Pt3[] =>
			Array.from({ length: N }, (_, i) => {
				const a = (i / N) * Math.PI * 2;
				return {
					x: rx + Math.cos(a) * r + sway,
					y: 25 + rock + Math.sin(a) * r,
					z,
				};
			});
		const rim = ring(RIM_R, rz);
		// After a make the net whips up and sways; a dunk yanks it down first.
		const lift = fx.swish * 0.75 - ringing * 0.5 * Math.cos(fx.t / 40);
		const sway = (fx.swish * 0.12 + ringing * 0.1) * Math.sin(fx.t / 45);
		const mid = ring(RIM_R * 0.78, rz - 0.75 + lift * 0.5, sway * 0.5);
		const bottom = ring(RIM_R * 0.6, rz - 1.55 + lift, sway);
		// Back half: the side of the ring away from the camera - the far
		// side from the sideline, the court side from behind the baseline.
		const near = Math.atan2(cam.pos.y - (25 + rock), cam.pos.x - rx);
		const isBack = (i: number) => Math.cos((i / N) * Math.PI * 2 - near) < 0;
		const netLines = (back: boolean) => {
			ctx.strokeStyle = back
				? "rgba(225,225,225,0.7)"
				: "rgba(252,252,252,0.95)";
			ctx.lineCap = "round";
			for (let i = 0; i < N; i++) {
				if (isBack(i) !== back) {
					continue;
				}
				const j = (i + 1) % N;
				const k = (i + N - 1) % N;
				line(ctx, cam, rim[i]!, mid[j]!, 0.055);
				line(ctx, cam, rim[i]!, mid[k]!, 0.055);
				line(ctx, cam, mid[i]!, bottom[j]!, 0.05);
				line(ctx, cam, mid[i]!, bottom[k]!, 0.05);
			}
		};
		// The ring itself, one solid tube of iron round each half - not a run of
		// separate pieces with an edge round each - with the light along its top.
		const R_N = 48;
		const ringAt = (a: number, z = rz): Pt3 => ({
			x: rx + Math.cos(a) * RIM_R,
			y: 25 + rock + Math.sin(a) * RIM_R,
			z,
		});
		const rimArc = (back: boolean) => {
			const pts: { x: number; y: number }[] = [];
			let k = 0;
			const n = R_N / 2;
			for (let i = 0; i <= n; i++) {
				const a =
					near + (back ? Math.PI / 2 : -Math.PI / 2) + (i / n) * Math.PI;
				const p = project(cam, ringAt(a));
				pts.push(p);
				k += p.k;
			}
			k /= pts.length;
			const w = Math.max(0.9, 0.17 * k);
			const path = () => {
				ctx.beginPath();
				pts.forEach((p, i) =>
					i === 0 ? ctx.moveTo(p.x, p.y) : ctx.lineTo(p.x, p.y),
				);
			};
			ctx.lineCap = "round";
			ctx.lineJoin = "round";
			path();
			ctx.strokeStyle = INK;
			ctx.lineWidth = w + 2 * Math.max(1, 0.05 * k);
			ctx.stroke();
			ctx.strokeStyle = back ? "#c4501f" : "#ef6526";
			ctx.lineWidth = w;
			ctx.stroke();
			if (!back && w > 2) {
				ctx.beginPath();
				for (let i = 0; i <= n; i++) {
					const a = near - Math.PI / 2 + (i / n) * Math.PI;
					const p = project(cam, ringAt(a, rz + 0.045));
					if (i === 0) {
						ctx.moveTo(p.x, p.y);
					} else {
						ctx.lineTo(p.x, p.y);
					}
				}
				ctx.strokeStyle = "rgba(255, 196, 150, 0.75)";
				ctx.lineWidth = Math.max(0.6, w * 0.3);
				ctx.stroke();
			}
		};
		// The bracket from the board to the rim.
		inkLine(
			ctx,
			cam,
			{ x: X(4.05), y: 25 + rock, z: rz - 0.25 },
			{ x: X(4.55), y: 25 + rock, z: rz },
			0.17,
			"#c4501f",
		);
		rimArc(true);
		netLines(true);
		between?.();
		netLines(false);
		rimArc(false);
	};
	if (behind) {
		rimAndNet();
		board();
		stanchion();
	} else {
		stanchion();
		board();
		rimAndNet();
	}
};

// How close the ball is to going through a rim: drawn between the rim's back
// and front halves when it is - but not out on the front of the rim, or
// coming down in front of the net, where it is in front of all of it.
export const ballAtRim = (ball: Pt3): Side | undefined => {
	for (const side of [0, 1] as const) {
		const rx = side === 0 ? 5.25 : COURT_W - 5.25;
		const dx = ball.x - rx;
		const front =
			Math.abs(dx) < RIM_R ? 25 + Math.sqrt(RIM_R * RIM_R - dx * dx) : 25;
		if (
			Math.hypot(dx, ball.y - 25) < RIM_R + BALL_R + 0.5 &&
			ball.z > RIM_Z - 2.2 &&
			ball.z < RIM_Z + 1.4 &&
			ball.y < front + 0.1
		) {
			return side;
		}
	}
	return undefined;
};

// The ball (see gameBall.ts): painted a pixel at a time, its print and its
// channels turning with its spin; where there is nothing to paint on, its
// leather shaded and its channels drawn as lines. mirrored: its reflection
// in the floor (drawn at its mirror image's place).
export const drawBall = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	ball: Pt3,
	spin: number,
	mirrored = false,
	// The way it spins on (see ballTurn).
	way = 0,
) => {
	const c = project(cam, ball);
	const r = Math.max(1.6, BALL_R * c.k);
	ctx.beginPath();
	ctx.arc(c.x, c.y, r, 0, Math.PI * 2);
	ctx.strokeStyle = INK;
	ctx.lineWidth = Math.max(1, 0.05 * c.k) * 2;
	ctx.stroke();
	const frame = viewFrame(cam);
	const turn = ballTurn(frame, spin, way);
	const mirror = mirrored ? floorMirror(frame) : undefined;
	if (paintBall(ctx, c.x, c.y, r, turn, mirror)) {
		return;
	}
	const g = ctx.createRadialGradient(
		c.x - r * 0.35,
		c.y - r * 0.4,
		r * 0.1,
		c.x,
		c.y,
		r,
	);
	g.addColorStop(0, "#de8a55");
	g.addColorStop(0.5, `rgb(${LEATHER.join(", ")})`);
	g.addColorStop(1, "#6e3417");
	ctx.fillStyle = g;
	ctx.fill();
	if (r > 2.2) {
		ctx.strokeStyle = `rgba(${CHANNEL.join(", ")}, ${r > 4 ? 1 : 0.7})`;
		ctx.lineWidth = Math.max(0.55, r * 0.07);
		ctx.lineCap = "round";
		ctx.lineJoin = "round";
		const [X, Y, Z] = turn;
		for (const line of CHANNEL_LINES) {
			let drawing = false;
			ctx.beginPath();
			for (const [x0, y0, z0] of line) {
				let vx = X[0] * x0 + Y[0] * y0 + Z[0] * z0;
				let vy = X[1] * x0 + Y[1] * y0 + Z[1] * z0;
				let vz = X[2] * x0 + Y[2] * y0 + Z[2] * z0;
				if (mirror) {
					// (A mirror is its own inverse.)
					const mx = mirror[0][0] * vx + mirror[0][1] * vy + mirror[0][2] * vz;
					const my = mirror[1][0] * vx + mirror[1][1] * vy + mirror[1][2] * vz;
					vz = mirror[2][0] * vx + mirror[2][1] * vy + mirror[2][2] * vz;
					vx = mx;
					vy = my;
				}
				if (vz < 0.02) {
					drawing = false;
					continue;
				}
				const px = c.x + vx * r * 0.97;
				const py = c.y - vy * r * 0.97;
				if (drawing) {
					ctx.lineTo(px, py);
				} else {
					ctx.moveTo(px, py);
					drawing = true;
				}
			}
			ctx.stroke();
		}
	}
};

// THE LIGHTS IN THE FLOOR. A broadcast's floor is polished: the lights
// along the truss over the far stands (see paintRafters) shine back off
// it, a row of soft bright patches across the far half of the court. Each
// is where the line from the camera to that light's mirror image under the
// floor meets the floor - so they slide along it as the camera pans, more
// slowly than the floor does, the way a reflection does - drawn out a
// little toward the camera, as light off a floor that is not a mirror is.
const LIGHT_Y = RAFTERS.origin.y;
// (The lights hang just under the truss's lower chord - see paintRafters.)
const LIGHT_Z = RAFTERS.origin.z - 2.2 - 2 / 8;
const GLINT_R = 1.6;
const GLINT_DRAWN = 2.8;
const GLINT_ALPHA = 0.42;
let glint: HTMLCanvasElement | undefined;
const glintSprite = (): HTMLCanvasElement => {
	if (!glint) {
		glint = document.createElement("canvas");
		glint.width = 32;
		glint.height = 32;
		const c = glint.getContext("2d")!;
		const g = c.createRadialGradient(16, 16, 0, 16, 16, 16);
		g.addColorStop(0, "rgba(255,252,240,1)");
		g.addColorStop(0.3, "rgba(255,248,228,0.55)");
		g.addColorStop(1, "rgba(255,248,228,0)");
		c.fillStyle = g;
		c.fillRect(0, 0, 32, 32);
	}
	return glint;
};
// Where each light shows in the floor, on the picture: its middle, and how
// far it reaches across and up and down.
export const glintsAt = (
	cam: Camera,
): { x: number; y: number; rx: number; ry: number }[] => {
	const C = cam.pos;
	// How far along from the camera to the light's mirror image the floor
	// is.
	const s = C.z / (C.z + LIGHT_Z);
	const y = C.y + s * (LIGHT_Y - C.y);
	if (y < FLOOR_EDGE.y0 || y > FLOOR_EDGE.y1) {
		return [];
	}
	const out: { x: number; y: number; rx: number; ry: number }[] = [];
	for (let lx = X0 + LIGHT_FIRST; lx < X1; lx += LIGHT_GAP) {
		const x = C.x + s * (lx - C.x);
		if (x < FLOOR_EDGE.x0 || x > FLOOR_EDGE.x1) {
			continue;
		}
		const p = project(cam, { x, y, z: 0 });
		const near = project(cam, { x, y: y + GLINT_R, z: 0 });
		const rx = GLINT_R * p.k;
		const ry = Math.max(rx * 0.5, (near.y - p.y) * GLINT_DRAWN);
		if (p.x + rx < 0 || p.x - rx > cam.viewW || rx < 0.5) {
			continue;
		}
		out.push({ x: p.x, y: p.y, rx, ry });
	}
	return out;
};
export const drawGlints = (ctx: CanvasRenderingContext2D, cam: Camera) => {
	const glints = glintsAt(cam);
	if (glints.length === 0) {
		return;
	}
	const sprite = glintSprite();
	const alpha = ctx.globalAlpha;
	const smoothing = ctx.imageSmoothingEnabled;
	ctx.imageSmoothingEnabled = true;
	ctx.globalAlpha = alpha * GLINT_ALPHA;
	for (const g of glints) {
		ctx.drawImage(sprite, g.x - g.rx, g.y - g.ry, 2 * g.rx, 2 * g.ry);
	}
	ctx.globalAlpha = alpha;
	ctx.imageSmoothingEnabled = smoothing;
};

// A soft dark patch on the floor under something: a player's feet, the ball.
export const drawShadow = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	x: number,
	y: number,
	radius: number,
	strength: number,
) => {
	if (strength <= 0.01) {
		return;
	}
	const c = project(cam, { x, y, z: 0 });
	const near = project(cam, { x, y: y + radius, z: 0 });
	const far = project(cam, { x, y: y - radius, z: 0 });
	const rx = radius * c.k;
	const ry = Math.max(0.5, Math.abs(near.y - far.y) / 2);
	ctx.save();
	ctx.translate(c.x, c.y);
	ctx.scale(1, ry / rx);
	const g = ctx.createRadialGradient(0, 0, 0, 0, 0, rx);
	g.addColorStop(0, `rgba(0,0,0,${0.5 * strength})`);
	g.addColorStop(0.6, `rgba(0,0,0,${0.3 * strength})`);
	g.addColorStop(1, "rgba(0,0,0,0)");
	ctx.fillStyle = g;
	ctx.beginPath();
	ctx.arc(0, 0, rx, 0, Math.PI * 2);
	ctx.fill();
	ctx.restore();
};
