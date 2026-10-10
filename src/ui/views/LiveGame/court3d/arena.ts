import { makeCourtRng } from "../courtRng.ts";
import { project, type Camera } from "./camera.ts";
import { BALL_R } from "./evaluate.ts";
import { shade } from "./figure.ts";
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
export const END_STANDS: [Plane, Plane] = [0, 1].map((side) => {
	const s = side === 0 ? -1 : 1;
	const x = side === 0 ? -END_BACK : COURT_W + END_BACK;
	return plane(
		`endStands${side}`,
		{
			x: x + s * SLOPE * Math.cos(RAKE),
			y: side === 0 ? END_Y0 : END_Y1,
			z: WALL_H + SLOPE * Math.sin(RAKE),
		},
		{ x: 0, y: side === 0 ? 1 : -1, z: 0 },
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
			y: side === 0 ? END_Y0 : END_Y1,
			z: WALL_H,
		},
		{ x: 0, y: side === 0 ? 1 : -1, z: 0 },
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
// Each team's bench: a row of chairs beside the table.
const BENCH_LEN = BENCH_SEATS * SEAT_GAP + 0.6;
export const benchPlane = (t: Side) =>
	plane(
		`bench${t}`,
		{ x: benchStart(t), y: BENCH_Y, z: 3.1 },
		{ x: 1, y: 0, z: 0 },
		{ x: 0, y: 0, z: -1 },
		BENCH_LEN,
		3.1,
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
	// Seats in the team's color, or plain gray ones.
	const seatColor = seatRng() < 0.55 ? shade(homeC[0]!, -0.28) : "#4b505b";
	let next = 0;
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
			const skin = skins[Math.floor(rng() * skins.length)]!;
			const homeFan = color === homeC[0] || color === homeC[1];
			const standing = up > 0 && rise() < (homeFan ? 0.88 : 0.45);
			// On his feet he is taller, and his arms are up.
			const bodyH = (standing ? 2 : 1.45) * px;
			const lift = standing ? 0.55 * px : 0;
			ctx.fillStyle = color;
			ctx.beginPath();
			ctx.roundRect(
				x - bodyW / 2,
				yb - bodyH - 0.5 * px - lift,
				bodyW,
				bodyH,
				0.35 * px,
			);
			ctx.fill();
			const headY = yb - bodyH - 0.75 * px - lift;
			if (standing) {
				// Arms up in a V - some waving a towel.
				const reach = (1.25 + rise() * 0.45) * px;
				const spread = (up === 1 ? 0.3 : 0.95) * px;
				const shoulderY = headY + 0.55 * px;
				const towel = homeFan && rise() < 0.35;
				ctx.strokeStyle = skin;
				ctx.lineWidth = 0.46 * px;
				ctx.lineCap = "round";
				ctx.beginPath();
				const hands: [number, number][] = [];
				for (const side of [-1, 1]) {
					const sx = x + (side * bodyW) / 2.4;
					const hx = sx + side * spread;
					const hy = shoulderY - reach + (up === 2 ? 0.3 * px : 0);
					ctx.moveTo(sx, shoulderY);
					ctx.lineTo(hx, hy);
					hands.push([hx, hy]);
				}
				ctx.stroke();
				if (towel) {
					const [hx, hy] = hands[up === 1 ? 1 : 0]!;
					ctx.fillStyle = rise() < 0.5 ? "#f4f4f4" : homeC[0]!;
					ctx.fillRect(hx - 0.45 * px, hy - 0.95 * px, 0.9 * px, 0.75 * px);
				}
			}
			ctx.fillStyle = skin;
			ctx.beginPath();
			ctx.arc(x, headY, 0.36 * px, 0, Math.PI * 2);
			ctx.fill();
			if (rng() < 0.05) {
				// Somebody's on his feet.
				ctx.fillRect(
					x - bodyW / 2 - 0.15 * px,
					yb - bodyH - 1.6 * px,
					0.25 * px,
					1.1 * px,
				);
			}
		}
		// The aisles.
		ctx.fillStyle = "#2b2e35";
		for (let xf = A0; xf < A1; xf += aisle) {
			ctx.fillRect((xf - A0) * px, yb - ROW_FT * px, 2.4 * px, ROW_FT * px);
		}
	}
	// The light falls on the court: the upper deck fades into the dark.
	const dark = ctx.createLinearGradient(0, 0, 0, h);
	dark.addColorStop(0, "rgba(3,3,6,0.88)");
	dark.addColorStop(0.45, "rgba(3,3,6,0.5)");
	dark.addColorStop(1, "rgba(3,3,6,0.12)");
	ctx.fillStyle = dark;
	ctx.fillRect(0, 0, w, h);
	return canvas;
};

// ---- the LED boards --------------------------------------------------------

// What the boards can show: the team, a chant for the crowd, DEFENSE while
// the road team has it (blinking: two pictures), and a shout after a big
// play by the home side.
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
// their colors along the bottom.
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
	const half = (w - 3 * edge) / 2;
	([away, home] as const).forEach((team, i) => {
		const x = edge + i * (half + edge);
		const y = h - strip;
		ctx.fillStyle = teamColor(team, 0, i ? "#8c1d40" : "#1d3461");
		ctx.fillRect(x, y, half, strip - edge);
		ctx.fillStyle = luminance(ctx.fillStyle as string) > 0.6 ? "#111" : "#fff";
		ctx.font = `800 ${Math.round(strip * 0.62)}px Arial, sans-serif`;
		ctx.textAlign = "center";
		ctx.textBaseline = "middle";
		ctx.fillText(
			(team?.abbrev ?? (i ? "HOME" : "AWAY")).toUpperCase(),
			x + half / 2,
			y + (strip - edge) / 2,
		);
	});
	ctx.fillStyle = teamColor(home, 1, "#f2c14e");
	ctx.fillRect(0, 0, w, edge);
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
	for (let x = 6; x < X1 - X0; x += 11) {
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
	for (let i = 0; i < BENCH_SEATS; i++) {
		const x = (0.4 + i * SEAT_GAP) * px;
		// Legs, seat, back.
		ctx.fillStyle = "#111";
		ctx.fillRect(x + 0.1 * px, p.h - 1.5 * px, 0.12 * px, 1.5 * px);
		ctx.fillRect(x + 1.3 * px, p.h - 1.5 * px, 0.12 * px, 1.5 * px);
		ctx.fillStyle = shade(c, -0.25);
		ctx.fillRect(x, p.h - 1.75 * px, 1.55 * px, 0.35 * px);
		ctx.fillStyle = c;
		ctx.beginPath();
		ctx.roundRect(x, 0, 1.55 * px, 1.6 * px, 0.25 * px);
		ctx.fill();
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

export type HoopFx = {
	// 0..1, fading: the net snapping up after a make, the iron ringing after a
	// miss, the rim bent by a dunk.
	swish: number;
	clank: number;
	dunk: number;
	// A dunk worth a replay rattles the whole basket harder.
	big?: boolean;
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

	// The stanchion: a dark padded base behind the baseline with a lit
	// board in the team's color on its faces, the post, and the arm out to
	// the board with a sign along it.
	const b0 = X(-9.5);
	const b1 = X(-5);
	const inkW = Math.max(1, 0.05 * project(cam, { x: X(4), y: 25, z: 0 }).k);
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
		// The boards on its faces: the team's color, a bright band across.
		const lit = (face: (u: number, v: number) => Pt3) => {
			const quad = (v0: number, v1: number, fill: string) => {
				ctx.fillStyle = fill;
				poly(ctx, cam, [face(0, v0), face(1, v0), face(1, v1), face(0, v1)]);
				ctx.fill();
			};
			quad(0.15, 0.85, padColor);
			quad(0.44, 0.56, shade(padColor, 0.45));
		};
		const lo = 0.5;
		const hi = z1 - 0.45;
		lit((u, v) => pts(b1, y0 + 0.35 + u * (y1 - y0 - 0.7), lo + v * (hi - lo)));
		lit((u, v) =>
			pts(b0 + (b1 - b0) * (0.06 + u * 0.88), y1, lo + v * (hi - lo)),
		);
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
		ctx.fillStyle = padColor;
		poly(ctx, cam, [
			at(d0, low(d0) + 0.35),
			at(d1, low(d1) + 0.35),
			at(d1, top(d1) - 0.35),
			at(d0, top(d0) - 0.35),
		]);
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

	// The backboard: glass in a white frame, the shooter's square, and the
	// shot clock on top.
	const bx = X(4);
	const bz = shake;
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
	const sc = [
		{ x: X(3.85), y: 24.15 + rock, z: 13.1 + bz },
		{ x: X(3.85), y: 25.85 + rock, z: 13.1 + bz },
		{ x: X(3.85), y: 25.85 + rock, z: 14.05 + bz },
		{ x: X(3.85), y: 24.15 + rock, z: 14.05 + bz },
	];
	ctx.fillStyle = "#0d0d10";
	poly(ctx, cam, sc);
	ctx.fill();
	if (shotClock) {
		const c = project(cam, { x: X(3.8), y: 25 + rock, z: 13.58 + bz });
		ctx.fillStyle = "#ff3b2f";
		ctx.font = `700 ${Math.max(5, 0.72 * c.k)}px "Courier New", monospace`;
		ctx.textAlign = "center";
		ctx.textBaseline = "middle";
		ctx.fillText(shotClock, c.x, c.y);
	}

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
	// Back half: the far side of the ring (smaller y is farther away).
	const isBack = (i: number) => Math.sin((i / N) * Math.PI * 2) < 0;
	const netLines = (back: boolean) => {
		ctx.strokeStyle = back ? "rgba(225,225,225,0.7)" : "rgba(252,252,252,0.95)";
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
			const a = (back ? Math.PI : 0) + (i / n) * Math.PI;
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
				const a = (i / n) * Math.PI;
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

export const drawBall = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	ball: Pt3,
	spin: number,
) => {
	const c = project(cam, ball);
	const r = Math.max(1.6, BALL_R * c.k);
	const g = ctx.createRadialGradient(
		c.x - r * 0.35,
		c.y - r * 0.4,
		r * 0.1,
		c.x,
		c.y,
		r,
	);
	g.addColorStop(0, "#f9a964");
	g.addColorStop(0.55, "#e2702a");
	g.addColorStop(1, "#a44716");
	ctx.beginPath();
	ctx.arc(c.x, c.y, r, 0, Math.PI * 2);
	ctx.strokeStyle = INK;
	ctx.lineWidth = Math.max(1, 0.1 * c.k) * 2;
	ctx.stroke();
	ctx.fillStyle = g;
	ctx.fill();
	if (r > 3) {
		// The seams, on the side of it facing the camera: turned with its
		// spin, about an axis tipped off level, so it rolls over as it goes.
		const a = spin;
		const ca = Math.cos(a);
		const sa = Math.sin(a);
		ctx.strokeStyle = "rgba(40, 16, 6, 0.85)";
		ctx.lineWidth = Math.max(0.6, r * 0.09);
		ctx.lineCap = "round";
		ctx.lineJoin = "round";
		for (const seam of SEAMS) {
			let drawing = false;
			ctx.beginPath();
			for (const [x0, y0, z0] of seam) {
				// Spun about x, then set at its resting tilt.
				const y1 = y0 * ca - z0 * sa;
				const z1 = y0 * sa + z0 * ca;
				const x2 = x0 * TILT_C + z1 * TILT_S;
				const z2 = -x0 * TILT_S + z1 * TILT_C;
				if (z2 < 0.02) {
					drawing = false;
					continue;
				}
				const px = c.x + x2 * r * 0.97;
				const py = c.y - y1 * r * 0.97;
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

// A basketball's seams on a ball of radius 1: two great circles at right
// angles, and the two curved seams - circles in planes either side of the
// one, crossing only the other - that make its eight panels.
const SEAMS: [number, number, number][][] = (() => {
	const n = 56;
	const circle = (
		f: (a: number) => [number, number, number],
	): [number, number, number][] =>
		Array.from({ length: n + 1 }, (_, i) => f((i / n) * Math.PI * 2));
	const d = 0.62;
	const rr = Math.sqrt(1 - d * d);
	return [
		circle((a) => [Math.cos(a), Math.sin(a), 0]),
		circle((a) => [0, Math.cos(a), Math.sin(a)]),
		circle((a) => [d, rr * Math.cos(a), rr * Math.sin(a)]),
		circle((a) => [-d, rr * Math.cos(a), rr * Math.sin(a)]),
	];
})();
const TILT_C = Math.cos(0.55);
const TILT_S = Math.sin(0.55);

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
