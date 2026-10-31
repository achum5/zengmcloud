import type { ArenaLooks } from "../../../../common/types.ts";
import { makeCourtRng } from "../courtRng.ts";
import { luminance, shade } from "./color.ts";
import type { TexturedPlane } from "../court3d/planes.ts";
import type { FieldTeam } from "./fieldPaint.ts";
import {
	END_ROOM,
	FIELD_L,
	FIELD_W,
	SIDE_ROOM,
	TUNNEL_H,
	TUNNEL_W,
	TUNNEL_X,
	type Pt3,
} from "./geometry.ts";
import { venueFor, type Venue } from "./venue.ts";

// THE STADIUM: an open bowl round the field - a padded wall with an LED
// board along it, the lower bowl rising from it, a club level's ribbon board,
// the upper deck over that, the light banks on the rim against the night sky
// - and the stands and video boards behind each end zone, and the home
// team's tunnel out from under them by the left end zone. Each a flat
// picture painted once a game and stood up in the world (see planes.ts), as
// the 3D court's arena is.
//
// What a team might want its own way - its seats' color, the walls, day or
// night, a dome - is in StadiumStyle, for a stadium editor to set later.

export type StadiumStyle = {
	seats: string;
	wall: string;
	// The concrete and steel of the bowl.
	structure: string;
	sky: "night" | "day";
	// Open to the sky, or under a roof (see venue.ts).
	roof: Venue["roof"];
	// A day game's shadow across the field (see venue.ts).
	shadow?: [number, number];
};

// (The same seats every game at a team's home.)
export const stadiumStyleFor = (
	home: FieldTeam | undefined,
	venue: Venue = venueFor(home),
): StadiumStyle => {
	const rng = makeCourtRng(
		`stadium|${home?.region ?? ""}|${home?.name ?? ""}|${home?.abbrev ?? ""}`,
	);
	const c0 = home?.colors?.[0] ?? "#8c1d40";
	return {
		seats: rng() < 0.6 ? shade(c0, -0.3) : "#4b505b",
		wall: shade(c0, -0.42),
		structure: "#5b5e66",
		sky: venue.sky,
		roof: venue.roof,
		...(venue.shadow ? { shadow: venue.shadow } : {}),
	};
};

const plane = (
	origin: Pt3,
	across: Pt3,
	down: Pt3,
	lengthX: number,
	lengthY: number,
	ppf: number,
): TexturedPlane => ({
	origin,
	alongX: { x: across.x / ppf, y: across.y / ppf, z: across.z / ppf },
	alongY: { x: down.x / ppf, y: down.y / ppf, z: down.z / ppf },
	w: Math.round(lengthX * ppf),
	h: Math.round(lengthY * ppf),
});

// Along the far side, the stands run past both end lines into the corners.
const X0 = -END_ROOM - 70;
const X1 = FIELD_L + END_ROOM + 70;
const WALL_Y = -SIDE_ROOM;
export const WALL_H = 8;
const RAD = Math.PI / 180;
const LOWER_RAKE = 27 * RAD;
const LOWER_SLOPE = 92;
const CLUB_H = 7;
const UPPER_RAKE = 34 * RAD;
const UPPER_SLOPE = 118;
const UPPER_SET_BACK = 6;
const ROW_FT = 2.9;

// The lower bowl's top edge, the club level's ribbon over it, then the
// upper deck from there.
const LOWER_TOP = {
	y: WALL_Y - LOWER_SLOPE * Math.cos(LOWER_RAKE),
	z: WALL_H + LOWER_SLOPE * Math.sin(LOWER_RAKE),
};
const UPPER_FRONT = {
	y: LOWER_TOP.y - UPPER_SET_BACK,
	z: LOWER_TOP.z + CLUB_H,
};
const UPPER_TOP = {
	y: UPPER_FRONT.y - UPPER_SLOPE * Math.cos(UPPER_RAKE),
	z: UPPER_FRONT.z + UPPER_SLOPE * Math.sin(UPPER_RAKE),
};
export const RIM_Z = UPPER_TOP.z;

const SEAT_PPF = 5;
export const WALL = plane(
	{ x: X0, y: WALL_Y, z: WALL_H },
	{ x: 1, y: 0, z: 0 },
	{ x: 0, y: 0, z: -1 },
	X1 - X0,
	WALL_H,
	6,
);
export const LOWER = plane(
	{ x: X0, y: LOWER_TOP.y, z: LOWER_TOP.z },
	{ x: 1, y: 0, z: 0 },
	{ x: 0, y: Math.cos(LOWER_RAKE), z: -Math.sin(LOWER_RAKE) },
	X1 - X0,
	LOWER_SLOPE,
	SEAT_PPF,
);
// The club level's face: the ribbon board, and the dark glass of the boxes
// set back over it.
export const CLUB = plane(
	{ x: X0, y: UPPER_FRONT.y, z: UPPER_FRONT.z },
	{ x: 1, y: 0, z: 0 },
	{ x: 0, y: 0, z: -1 },
	X1 - X0,
	CLUB_H,
	6,
);
// The upper deck's front, a wall at the foot of its first row: the ring of
// honor along it (see paintFascia).
const FASCIA_H = 4.5;
export const FASCIA = plane(
	{ x: X0, y: UPPER_FRONT.y, z: UPPER_FRONT.z + FASCIA_H },
	{ x: 1, y: 0, z: 0 },
	{ x: 0, y: 0, z: -1 },
	X1 - X0,
	FASCIA_H,
	8,
);
export const UPPER = plane(
	{ x: X0, y: UPPER_TOP.y, z: UPPER_TOP.z },
	{ x: 1, y: 0, z: 0 },
	{ x: 0, y: Math.cos(UPPER_RAKE), z: -Math.sin(UPPER_RAKE) },
	X1 - X0,
	UPPER_SLOPE,
	SEAT_PPF * 0.8,
);
// The rim over the upper deck: the light banks.
const RIM_H = 18;
export const RIM = plane(
	{ x: X0, y: UPPER_TOP.y, z: UPPER_TOP.z + RIM_H },
	{ x: 1, y: 0, z: 0 },
	{ x: 0, y: 0, z: -1 },
	X1 - X0,
	RIM_H,
	3,
);

// A roof, if it has one: from the top of the rim across the field and on
// over the near side, rising a little as it goes.
const ROOF_RISE = 12 * RAD;
const ROOF_SPAN = 470;
export const ROOF = plane(
	{ x: X0, y: UPPER_TOP.y, z: UPPER_TOP.z + RIM_H },
	{ x: 1, y: 0, z: 0 },
	{ x: 0, y: Math.cos(ROOF_RISE), z: Math.sin(ROOF_RISE) },
	X1 - X0,
	ROOF_SPAN,
	1,
);

// Behind each end zone: a wall, the lower bowl rising from it, and the
// video board over the top - each running from the near side to the far one
// at the left end, the other way at the right, to read as it should from
// the field.
const END_Y0 = WALL_Y;
const END_Y1 = FIELD_W + SIDE_ROOM;
const END_SLOPE = 80;
const END_RAKE = 28 * RAD;
export const END_WALLS: [TexturedPlane, TexturedPlane] = [0, 1].map((side) =>
	plane(
		{
			x: side === 0 ? -END_ROOM : FIELD_L + END_ROOM,
			y: side === 0 ? END_Y1 : END_Y0,
			z: WALL_H,
		},
		{ x: 0, y: side === 0 ? -1 : 1, z: 0 },
		{ x: 0, y: 0, z: -1 },
		END_Y1 - END_Y0,
		WALL_H,
		6,
	),
) as [TexturedPlane, TexturedPlane];
export const END_STANDS: [TexturedPlane, TexturedPlane] = [0, 1].map((side) => {
	const s = side === 0 ? -1 : 1;
	const x = side === 0 ? -END_ROOM : FIELD_L + END_ROOM;
	return plane(
		{
			x: x + s * END_SLOPE * Math.cos(END_RAKE),
			y: side === 0 ? END_Y1 : END_Y0,
			z: WALL_H + END_SLOPE * Math.sin(END_RAKE),
		},
		{ x: 0, y: side === 0 ? -1 : 1, z: 0 },
		{ x: -s * Math.cos(END_RAKE), y: 0, z: -Math.sin(END_RAKE) },
		END_Y1 - END_Y0,
		END_SLOPE,
		SEAT_PPF * 0.8,
	);
}) as [TexturedPlane, TexturedPlane];
const BOARD_W = 150;
const BOARD_H = 54;
export const END_BOARDS: [TexturedPlane, TexturedPlane] = [0, 1].map((side) => {
	const s = side === 0 ? -1 : 1;
	const x =
		(side === 0 ? -END_ROOM : FIELD_L + END_ROOM) +
		s * (END_SLOPE * Math.cos(END_RAKE) + 6);
	const z = WALL_H + END_SLOPE * Math.sin(END_RAKE) + 6 + BOARD_H;
	const yc = FIELD_W / 2;
	return plane(
		{ x, y: side === 0 ? yc + BOARD_W / 2 : yc - BOARD_W / 2, z },
		{ x: 0, y: side === 0 ? -1 : 1, z: 0 },
		{ x: 0, y: 0, z: -1 },
		BOARD_W,
		BOARD_H,
		4,
	);
}) as [TexturedPlane, TexturedPlane];

// The tunnel: its opening in a face stood out from the wall, up past the top
// of it, with a header over the opening.
const PORTAL_W = TUNNEL_W + 8;
const PORTAL_H = TUNNEL_H + 4;
export const TUNNEL = plane(
	{ x: TUNNEL_X - PORTAL_W / 2, y: WALL_Y + 0.1, z: PORTAL_H },
	{ x: 1, y: 0, z: 0 },
	{ x: 0, y: 0, z: -1 },
	PORTAL_W,
	PORTAL_H,
	8,
);

const canvasOf = (w: number, h: number): HTMLCanvasElement => {
	const c = document.createElement("canvas");
	c.width = w;
	c.height = h;
	return c;
};

const teamColor = (t: FieldTeam | undefined, i: number, fallback: string) =>
	t?.colors?.[i] ?? fallback;

// Clothes the crowd wears when not the home team's.
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
const SKINS = ["#f1c7a5", "#d9a77f", "#b07a52", "#8a5a3b", "#5e3b26"];
// Over the top of a fan's head: his hair (every kind of it - now and then
// none), or a cap, as often as not in the home team's colors.
const HAIRS = [
	"#14100c",
	"#2a1a10",
	"#3a2414",
	"#5a3a22",
	"#8a5a2e",
	"#c9a05a",
	"#e2c27c",
	"#8d8a86",
];
const BALD = 0.07;
const CAPS = 0.16;
const CAP_COLORS = ["#1d2330", "#f2f2f0", "#5b5d62"];

// A section of seats, its front row at the bottom of the picture: rows of
// people in the home colors (and some in the visitors', and plain clothes),
// as full as the gate - the best seats first, low and near midfield - with
// aisles every so often. `up` puts most of the home crowd on its feet, arms
// up (1), or waving (2).
// The home fans up and waving a towel, this share of them; and down low, in
// the first so many rows, this share holding up a sign (the 3D court's: see
// court3d/arena.ts).
const TOWELS = 0.35;
const SIGN_ROWS = 9;
const SIGNS = 0.03;
const paintSeating = ({
	w,
	h,
	ppf,
	rowFt,
	home,
	away,
	style,
	seed,
	fill,
	up,
	// Where midfield is across the picture (feet from its left edge), if it
	// is along the side.
	midAt,
	dim,
}: {
	w: number;
	h: number;
	ppf: number;
	rowFt: number;
	home: FieldTeam | undefined;
	away: FieldTeam | undefined;
	style: StadiumStyle;
	seed: string;
	fill: number;
	up: 0 | 1 | 2;
	midAt?: number;
	// How far the light falls off up the section (0 none, 1 dark at the top).
	dim: number;
}) => {
	const c = canvasOf(w, h);
	const g = c.getContext("2d")!;
	const bg = g.createLinearGradient(0, 0, 0, h);
	bg.addColorStop(0, shade(style.structure, -0.55));
	bg.addColorStop(1, shade(style.structure, -0.35));
	g.fillStyle = bg;
	g.fillRect(0, 0, w, h);
	const rng = makeCourtRng(`fans|${seed}`);
	const rise = makeCourtRng(`fans-up|${seed}`);
	const seatRng = makeCourtRng(`seats|${seed}`);
	const headRng = makeCourtRng(`heads|${seed}`);
	const homeC = [teamColor(home, 0, "#8c1d40"), teamColor(home, 1, "#f2c14e")];
	const awayC = [teamColor(away, 0, "#1d3461"), teamColor(away, 1, "#f28c28")];
	const shirt = () => {
		const r = rng();
		if (r < 0.36) {
			return homeC[0]!;
		}
		if (r < 0.5) {
			return homeC[1]!;
		}
		if (r < 0.57) {
			return awayC[0]!;
		}
		return PALETTE[Math.floor(rng() * PALETTE.length)]!;
	};
	const wFt = w / ppf;
	const rows = Math.floor(h / ppf / rowFt);
	const aisle = 34;
	const seats: { r: number; xf: number; want: number }[] = [];
	for (let r = 0; r < rows; r++) {
		for (let xf = 0.6; xf < wFt - 0.6; xf += 1.9) {
			if (xf % aisle < 3.2) {
				continue;
			}
			seats.push({
				r,
				xf,
				want:
					(r / rows) * 0.9 +
					(midAt === undefined ? 0.25 : Math.abs(xf - midAt) / 420) +
					seatRng() * 0.45,
			});
		}
	}
	const wants = seats.map((s) => s.want).sort((a, b) => a - b);
	const taken =
		wants[Math.min(wants.length - 1, Math.floor(fill * wants.length))]!;
	const px = ppf;
	let next = 0;
	const signs: { x: number; top: number }[] = [];
	for (let r = 0; r < rows; r++) {
		const yb = h - r * rowFt * px;
		g.fillStyle =
			r % 2 ? shade(style.structure, -0.48) : shade(style.structure, -0.52);
		g.fillRect(0, yb - 0.9 * px, w, 0.9 * px);
		for (; next < seats.length && seats[next]!.r === r; next++) {
			const xf = seats[next]!.xf;
			if (seats[next]!.want > taken) {
				g.fillStyle = style.seats;
				g.beginPath();
				g.roundRect(
					(xf - 0.62) * px,
					yb - 1.55 * px,
					1.24 * px,
					1.2 * px,
					0.25 * px,
				);
				g.fill();
				continue;
			}
			const x = xf * px + (rng() - 0.5) * 1.5;
			const bodyW = (1.3 + rng() * 0.35) * px;
			const color = shirt();
			const skin = SKINS[Math.floor(rng() * SKINS.length)]!;
			const homeFan = color === homeC[0] || color === homeC[1];
			const standing = up > 0 && rise() < (homeFan ? 0.88 : 0.45);
			const bodyH = (standing ? 2 : 1.45) * px;
			const lift = standing ? 0.55 * px : 0;
			g.fillStyle = color;
			g.beginPath();
			g.roundRect(
				x - bodyW / 2,
				yb - bodyH - 0.5 * px - lift,
				bodyW,
				bodyH,
				0.35 * px,
			);
			g.fill();
			const headY = yb - bodyH - 0.75 * px - lift;
			if (standing) {
				const reach = (1.25 + rise() * 0.45) * px;
				const spread = (up === 1 ? 0.3 : 0.95) * px;
				const shoulderY = headY + 0.55 * px;
				g.strokeStyle = skin;
				g.lineWidth = 0.46 * px;
				g.lineCap = "round";
				g.beginPath();
				for (const side of [-1, 1]) {
					const sx = x + (side * bodyW) / 2.4;
					g.moveTo(sx, shoulderY);
					g.lineTo(sx + side * spread, shoulderY - reach);
				}
				g.stroke();
				// (Some of the home fans waving a towel: in one hand, and the
				// other, by turns - see crowdAt's wave.)
				if (homeFan && rise() < TOWELS) {
					const side = up === 1 ? 1 : -1;
					g.fillStyle = rise() < 0.5 ? "#f4f4f4" : homeC[0]!;
					g.fillRect(
						x + (side * bodyW) / 2.4 + side * spread - 0.45 * px,
						shoulderY - reach - 0.95 * px,
						0.9 * px,
						0.75 * px,
					);
				}
			}
			// Now and then a home fan down low holds a sign up over his head
			// (painted once the rows behind him are: see below) - in the same
			// place sat or stood, for the stands up over the stands sat (see
			// scene.ts) not to show it twice.
			if (homeFan && r < SIGN_ROWS && seatRng() < SIGNS) {
				signs.push({ x, top: yb - 3.35 * px });
			}
			g.fillStyle = skin;
			g.beginPath();
			g.arc(x, headY, 0.36 * px, 0, Math.PI * 2);
			g.fill();
			const top = headRng();
			if (top >= BALD) {
				const cap = top < BALD + CAPS;
				g.fillStyle = cap
					? headRng() < 0.5
						? homeC[Math.floor(headRng() * 2)]!
						: CAP_COLORS[Math.floor(headRng() * CAP_COLORS.length)]!
					: HAIRS[Math.floor(headRng() * HAIRS.length)]!;
				g.beginPath();
				g.ellipse(
					x,
					headY - 0.08 * px,
					(cap ? 0.4 : 0.38) * px,
					(cap ? 0.3 : 0.32) * px,
					0,
					Math.PI,
					0,
				);
				g.fill();
			}
		}
		g.fillStyle = shade(style.structure, -0.3);
		for (let xf = 0; xf < wFt; xf += aisle) {
			g.fillRect(xf * px, yb - rowFt * px, 2.6 * px, rowFt * px);
		}
	}
	// The signs held up: the D and the fence, in the home team's color.
	for (const { x, top } of signs) {
		const sw = 2.2 * px;
		const sh = 1.3 * px;
		const sx = x - sw / 2;
		const sy = top - sh;
		g.fillStyle = "#f4f4f2";
		g.fillRect(sx, sy, sw, sh);
		g.fillStyle = homeC[0]!;
		g.font = `900 ${Math.round(sh * 0.9)}px sans-serif`;
		g.textAlign = "left";
		g.textBaseline = "middle";
		g.fillText("D", sx + 0.12 * px, sy + sh / 2 + 0.05 * px);
		g.strokeStyle = homeC[0]!;
		g.lineWidth = Math.max(1, 0.12 * px);
		g.lineCap = "butt";
		g.beginPath();
		for (let k = 0; k < 4; k++) {
			const fx = sx + sw * (0.52 + k * 0.13);
			g.moveTo(fx, sy + sh * 0.18);
			g.lineTo(fx, sy + sh * 0.86);
		}
		g.moveTo(sx + sw * 0.47, sy + sh * 0.45);
		g.lineTo(sx + sw * 0.93, sy + sh * 0.45);
		g.stroke();
	}
	if (dim > 0) {
		const dark = g.createLinearGradient(0, 0, 0, h);
		dark.addColorStop(0, `rgba(3,3,8,${0.75 * dim})`);
		dark.addColorStop(0.6, `rgba(3,3,8,${0.35 * dim})`);
		dark.addColorStop(1, "rgba(3,3,8,0.05)");
		g.fillStyle = dark;
		g.fillRect(0, 0, w, h);
	}
	return c;
};

// A long LED board: the home team's name and colors repeated along it, a
// strip of its second color top and bottom.
// WHAT THE BOARDS SHOW (the 3D court's: see court3d/arena.ts paintScreen):
// the team's name, the chants, DEFENSE (blinking, one screen and the
// other), and a shout for a big play of the home side's (see crowd.ts
// boardAt).
export const SHOUTS = {
	touchdown: "TOUCHDOWN!",
	fieldGoal: "IT'S GOOD!",
	turnover: "TURNOVER!",
	sack: "SACK!",
	safety: "SAFETY!",
	stop: "STOPPED!",
} as const;
export type Shout = keyof typeof SHOUTS;
export type BoardScreen =
	| "name"
	| "letsGo"
	| "noise"
	| "defense"
	| "defense2"
	| Shout;
// (Lettering that reads on it: white, or near-black on a light one.)
const inkOn = (bg: string) => (luminance(bg) > 0.62 ? "#111216" : "#f6f6f6");

const paintBoard = (
	w: number,
	h: number,
	ppf: number,
	home: FieldTeam | undefined,
	words: string[],
	screen: BoardScreen = "name",
) => {
	const c = canvasOf(w, h);
	const g = c.getContext("2d")!;
	const c0 = teamColor(home, 0, "#8c1d40");
	const c1 = teamColor(home, 1, "#f2c14e");
	const band = shade(c0, -0.1);
	const dark = "#15161b";
	g.fillStyle = "#0a0b0f";
	g.fillRect(0, 0, w, h);
	// Panel by panel along it: what each says, on what.
	const name = words.at(-1) ?? "";
	const chant = screen === "letsGo" || screen === "noise";
	const say =
		screen === "name"
			? words
			: screen === "letsGo"
				? [`LET'S GO ${name}`]
				: screen === "noise"
					? ["MAKE SOME NOISE"]
					: screen === "defense" || screen === "defense2"
						? ["DEFENSE"]
						: [SHOUTS[screen]];
	const bgOf = (i: number) =>
		screen === "defense" || screen === "defense2"
			? (screen === "defense") === (i % 2 === 0)
				? band
				: dark
			: screen in SHOUTS
				? i % 2
					? c1
					: band
				: band;
	const spacing = (screen === "name" ? 70 : chant ? 90 : 56) * ppf;
	g.font = `900 ${h * 0.5}px "Arial Black", "Helvetica Neue", Arial, sans-serif`;
	g.textAlign = "center";
	g.textBaseline = "middle";
	let i = 0;
	for (let x = 0; x < w; x += spacing) {
		const bg = bgOf(i);
		g.fillStyle = bg;
		g.fillRect(x, h * 0.12, spacing + 1, h * 0.76);
		g.fillStyle =
			bg === dark
				? c1
				: screen === "noise" && luminance(c1) > 0.3
					? c1
					: inkOn(bg);
		g.fillText(say[i % say.length]!, x + spacing / 2, h * 0.52, spacing * 0.86);
		i += 1;
	}
	g.fillStyle = c1;
	g.fillRect(0, h * 0.06, w, h * 0.06);
	g.fillRect(0, h * 0.88, w, h * 0.06);
	return c;
};

// THE RING OF HONOR, along the upper deck's front (the 3D court's banners in
// its rafters: see court3d/arena.ts paintRafters): the championships won
// before this season in the middle over midfield - each year on the colors
// the team had that season - and the numbers retired out either side of
// them, each on the colors of the jersey it is shown in. On the dark of
// the team's first color, edged in its second; plain, with nothing to
// honor.
const HONOR_GAP = 3;
const HONOR_PAD = 1.4;
const paintFascia = (
	w: number,
	h: number,
	ppf: number,
	home: FieldTeam | undefined,
	arena: ArenaLooks | undefined,
) => {
	const c = canvasOf(w, h);
	const g = c.getContext("2d")!;
	const c0 = teamColor(home, 0, "#8c1d40");
	const c1 = teamColor(home, 1, "#f2c14e");
	g.fillStyle = shade(c0, -0.45);
	g.fillRect(0, 0, w, h);
	g.fillStyle = c1;
	g.fillRect(0, 0, w, h * 0.1);
	g.fillRect(0, h * 0.9, w, h * 0.1);
	type Panel = { say: string; bg: string; ink: string };
	const panel = (say: string, colors?: [string, string, string]): Panel => {
		const bg = colors?.[0] ?? c0;
		const ink = colors?.[1] ?? c1;
		return {
			say,
			bg,
			ink: Math.abs(luminance(ink) - luminance(bg)) > 0.3 ? ink : inkOn(bg),
		};
	};
	const titles = arena?.titles ?? [];
	const middle: Panel[] =
		titles.length > 0
			? [
					panel("CHAMPIONS", [c1, c0, c0]),
					...titles.map((year, i) =>
						panel(String(year), arena?.titleLooks?.[i]?.colors),
					),
				]
			: [];
	const retired = (arena?.retired ?? []).map((r) =>
		panel(r.name ? `${r.number} ${r.name.toUpperCase()}` : r.number, r.colors),
	);
	if (middle.length === 0 && retired.length === 0) {
		return c;
	}
	g.font = `900 ${h * 0.5}px "Arial Black", "Helvetica Neue", Arial, sans-serif`;
	g.textAlign = "center";
	g.textBaseline = "middle";
	const widthOf = (p: Panel) =>
		g.measureText(p.say).width + 2 * HONOR_PAD * ppf;
	const gap = HONOR_GAP * ppf;
	const draw = (p: Panel, x: number) => {
		const pw = widthOf(p);
		g.fillStyle = p.bg;
		g.fillRect(x, h * 0.18, pw, h * 0.64);
		g.fillStyle = p.ink;
		g.fillText(p.say, x + pw / 2, h * 0.52);
	};
	// The titles together, round midfield; then the numbers, one to each
	// side by turns, as far as there is room.
	const mid = (-X0 + FIELD_L / 2) * ppf;
	const run =
		middle.reduce((a, p) => a + widthOf(p), 0) + gap * (middle.length - 1);
	let left = mid - Math.max(0, run) / 2;
	let right = left;
	for (const p of middle) {
		draw(p, right);
		right += widthOf(p) + gap;
	}
	if (middle.length === 0) {
		right = mid;
		left = mid;
	} else {
		left -= gap;
	}
	retired.forEach((p, k) => {
		const pw = widthOf(p);
		if (k % 2 === 0 && right + pw < w) {
			draw(p, right);
			right += pw + gap;
		} else if (left - pw > 0) {
			left -= pw;
			draw(p, left);
			left -= gap;
		}
	});
	return c;
};

// The tunnel's face: the wall's color, the team's name over the opening in
// the header, a stripe of the second color under it; the opening dark, the
// walls, floor and ceiling of it running back to the light at its far end.
const paintTunnel = (
	w: number,
	h: number,
	ppf: number,
	home: FieldTeam | undefined,
	name: string,
	style: StadiumStyle,
) => {
	const c = canvasOf(w, h);
	const g = c.getContext("2d")!;
	const c0 = teamColor(home, 0, "#8c1d40");
	const c1 = teamColor(home, 1, "#f2c14e");
	g.fillStyle = style.wall;
	g.fillRect(0, 0, w, h);
	const ox = ((PORTAL_W - TUNNEL_W) / 2) * ppf;
	const oy = (PORTAL_H - TUNNEL_H) * ppf;
	const ow = TUNNEL_W * ppf;
	const oh = TUNNEL_H * ppf;
	g.fillStyle = shade(c0, -0.08);
	g.fillRect(0, 0, w, oy * 0.86);
	g.fillStyle = c1;
	g.fillRect(0, oy * 0.86, w, oy * 0.14);
	g.font = `900 ${oy * 0.56}px "Arial Black", "Helvetica Neue", Arial, sans-serif`;
	g.textAlign = "center";
	g.textBaseline = "middle";
	g.fillStyle = "#f6f6f6";
	g.fillText(name, w / 2, oy * 0.45, w * 0.9);
	// (Its far end: this much of the opening, its foot this far down it.)
	const bw = ow * 0.34;
	const bh = oh * 0.4;
	const bx = ox + (ow - bw) / 2;
	const by = oy + oh * 0.68 - bh;
	const quad = (pts: number[], fill: string | CanvasGradient) => {
		g.beginPath();
		g.moveTo(pts[0]!, pts[1]!);
		for (let k = 2; k < pts.length; k += 2) {
			g.lineTo(pts[k]!, pts[k + 1]!);
		}
		g.closePath();
		g.fillStyle = fill;
		g.fill();
	};
	const floor = g.createLinearGradient(0, oy + oh, 0, by + bh);
	floor.addColorStop(0, "#34353b");
	floor.addColorStop(1, "#1a1b20");
	quad([ox, oy + oh, ox + ow, oy + oh, bx + bw, by + bh, bx, by + bh], floor);
	quad([ox, oy, bx, by, bx, by + bh, ox, oy + oh], "#111216");
	quad(
		[ox + ow, oy, bx + bw, by, bx + bw, by + bh, ox + ow, oy + oh],
		"#111216",
	);
	quad([ox, oy, ox + ow, oy, bx + bw, by, bx, by], "#08090b");
	const glow = g.createRadialGradient(
		bx + bw / 2,
		by + bh * 0.55,
		0,
		bx + bw / 2,
		by + bh * 0.55,
		bw * 0.75,
	);
	glow.addColorStop(0, "#efe6cc");
	glow.addColorStop(1, "#7a7058");
	g.fillStyle = glow;
	g.fillRect(bx, by, bw, bh);
	// (A rim of the second color round the opening.)
	g.strokeStyle = c1;
	g.lineWidth = Math.max(1, ppf * 0.35);
	g.strokeRect(ox, oy, ow, oh + g.lineWidth);
	return c;
};

// The light banks along the rim: rows of lamps, glowing.
const paintRim = (w: number, h: number, ppf: number, style: StadiumStyle) => {
	const c = canvasOf(w, h);
	const g = c.getContext("2d")!;
	g.fillStyle = shade(style.structure, -0.45);
	g.fillRect(0, h * 0.55, w, h * 0.45);
	const bank = 26 * ppf;
	// (Off, in the afternoon: rows of gray lamps.)
	const off = style.sky === "day" && style.roof === "open";
	for (let x = bank * 0.6; x < w; x += bank * 1.9) {
		g.fillStyle = off ? "#5d6169" : "#202228";
		g.fillRect(x, h * 0.08, bank, h * 0.45);
		for (let r = 0; r < 3; r++) {
			for (let k = 0; k < 8; k++) {
				const lx = x + (k + 0.5) * (bank / 8);
				const ly = h * (0.16 + r * 0.13);
				if (off) {
					g.fillStyle = "#9da2aa";
					g.fillRect(lx - bank / 22, ly - bank / 22, bank / 11, bank / 11);
					continue;
				}
				const glow = g.createRadialGradient(lx, ly, 0, lx, ly, bank / 8);
				glow.addColorStop(0, "rgba(255,255,244,1)");
				glow.addColorStop(0.4, "rgba(255,250,225,0.8)");
				glow.addColorStop(1, "rgba(255,250,225,0)");
				g.fillStyle = glow;
				g.fillRect(lx - bank / 8, ly - bank / 8, bank / 4, bank / 4);
			}
		}
	}
	return c;
};

// The video board over an end zone: the home logo or name on its color, in a
// dark frame.
const paintVideoBoard = (
	w: number,
	h: number,
	home: FieldTeam | undefined,
	logo: HTMLCanvasElement | undefined,
) => {
	const c = canvasOf(w, h);
	const g = c.getContext("2d")!;
	g.fillStyle = "#0b0c10";
	g.fillRect(0, 0, w, h);
	const m = Math.round(h * 0.05);
	const c0 = teamColor(home, 0, "#8c1d40");
	const sky = g.createLinearGradient(0, m, 0, h - m);
	sky.addColorStop(0, shade(c0, 0.08));
	sky.addColorStop(1, shade(c0, -0.25));
	g.fillStyle = sky;
	g.fillRect(m, m, w - 2 * m, h - 2 * m);
	if (logo && logo.width > 0 && logo.height > 0) {
		const k = Math.min((w * 0.5) / logo.width, (h * 0.78) / logo.height);
		g.drawImage(
			logo,
			(w - logo.width * k) / 2,
			(h - logo.height * k) / 2,
			logo.width * k,
			logo.height * k,
		);
	} else {
		g.fillStyle = "#f6f6f6";
		g.font = `900 ${h * 0.34}px "Arial Black", "Helvetica Neue", Arial, sans-serif`;
		g.textAlign = "center";
		g.textBaseline = "middle";
		g.fillText(
			(home?.name ?? home?.abbrev ?? "").toUpperCase(),
			w / 2,
			h / 2,
			w * 0.86,
		);
	}
	return c;
};

// The underside of the roof: dark panels between the trusses, which run
// along the field and across it, and rows of lights hung from them -
// brightest out over the field.
const paintRoof = (w: number, h: number, ppf: number, seed: string) => {
	const c = canvasOf(w, h);
	const g = c.getContext("2d")!;
	g.fillStyle = "#1d1f25";
	g.fillRect(0, 0, w, h);
	const glow = g.createLinearGradient(0, 0, 0, h);
	glow.addColorStop(0, "rgba(120,124,136,0.05)");
	glow.addColorStop(0.55, "rgba(150,154,166,0.16)");
	glow.addColorStop(1, "rgba(120,124,136,0.04)");
	g.fillStyle = glow;
	g.fillRect(0, 0, w, h);
	const along = 32 * ppf;
	const across = 48 * ppf;
	g.fillStyle = "#3a3d46";
	for (let y = along / 2; y < h; y += along) {
		g.fillRect(0, y, w, Math.max(1, 1.6 * ppf));
	}
	g.fillStyle = "#2f323a";
	for (let x = across / 2; x < w; x += across) {
		g.fillRect(x, 0, Math.max(1, 1.2 * ppf), h);
	}
	const rng = makeCourtRng(`roof|${seed}`);
	for (let y = along / 2; y < h; y += along * 2) {
		for (let x = across / 4; x < w; x += across / 2) {
			const r = (1.6 + rng() * 0.4) * ppf;
			const lamp = g.createRadialGradient(x, y, 0, x, y, r * 2.2);
			lamp.addColorStop(0, "rgba(255,252,236,1)");
			lamp.addColorStop(0.35, "rgba(255,248,220,0.7)");
			lamp.addColorStop(1, "rgba(255,248,220,0)");
			g.fillStyle = lamp;
			g.fillRect(x - r * 2.2, y - r * 2.2, r * 4.4, r * 4.4);
		}
	}
	return c;
};

export type StadiumPaint = {
	style: StadiumStyle;
	wall: HTMLCanvasElement;
	lower: [HTMLCanvasElement, HTMLCanvasElement, HTMLCanvasElement];
	// Half of them, for when the seats are emptier (see crowd.ts).
	lowerSparse: HTMLCanvasElement;
	club: HTMLCanvasElement;
	fascia: HTMLCanvasElement;
	upper: HTMLCanvasElement;
	rim: HTMLCanvasElement;
	endWall: HTMLCanvasElement;
	endStands: [HTMLCanvasElement, HTMLCanvasElement, HTMLCanvasElement];
	endSparse: HTMLCanvasElement;
	board: HTMLCanvasElement;
	tunnel: HTMLCanvasElement;
	roof?: HTMLCanvasElement;
	// The wall's, the club level's and the end walls' boards on another
	// screen than the name (painted the first time each is up) - and the
	// club level's ribbon on the name, with the score in among it (the 3D
	// court's board over center court: see court3d/arena.ts jumboScored),
	// the visitors' and the home team's, repainted as it changes.
	screen: (
		set: BoardSet,
		screen: BoardScreen,
		score?: [number, number],
	) => HTMLCanvasElement;
};
export type BoardSet = "wall" | "club" | "end";

export const paintStadium = (
	home: FieldTeam | undefined,
	away: FieldTeam | undefined,
	seed: string,
	crowd?: { att?: number; capacity?: number },
	logo?: HTMLCanvasElement,
	venue: Venue = venueFor(home),
	// The home team's honors (see paintFascia).
	arena?: ArenaLooks,
): StadiumPaint => {
	const style = stadiumStyleFor(home, venue);
	const fill =
		crowd?.att !== undefined && crowd.capacity
			? Math.min(0.97, Math.max(0.04, crowd.att / crowd.capacity))
			: 0.93;
	const name = (home?.name ?? home?.abbrev ?? "").toUpperCase();
	const region = (home?.region ?? "").toUpperCase();
	const words = region ? [region, name] : [name];
	const sizes: Record<BoardSet, [number, number, number]> = {
		wall: [WALL.w, WALL.h, WALL.w / (X1 - X0)],
		club: [CLUB.w, CLUB.h, CLUB.w / (X1 - X0)],
		end: [END_WALLS[0].w, END_WALLS[0].h, END_WALLS[0].w / (END_Y1 - END_Y0)],
	};
	const boardOf = (
		set: BoardSet,
		screen: BoardScreen = "name",
		say = words,
	) => {
		const [w, h, ppf] = sizes[set];
		return paintBoard(w, h, ppf, home, say, screen);
	};
	const abbrev = (t: FieldTeam | undefined, or: string) =>
		(t?.abbrev ?? or).toUpperCase();
	let scored: { key: string; c: HTMLCanvasElement } | undefined;
	const screens = new Map<string, HTMLCanvasElement>();
	const seating = (
		P: TexturedPlane,
		key: string,
		up: 0 | 1 | 2,
		dim: number,
		mid: boolean,
		gate = fill,
	) =>
		paintSeating({
			w: P.w,
			h: P.h,
			ppf: P.w / (mid ? X1 - X0 : END_Y1 - END_Y0),
			rowFt: ROW_FT,
			home,
			away,
			style,
			seed: `${seed}|${key}`,
			fill: gate,
			up,
			...(mid ? { midAt: -X0 + FIELD_L / 2 } : {}),
			// (In the sun, the backs of the stands hardly in the dark.)
			dim: style.sky === "day" && style.roof === "open" ? dim * 0.4 : dim,
		});
	const lower = ([0, 1, 2] as const).map((up) =>
		seating(LOWER, "lower", up, 0.35, true),
	) as [HTMLCanvasElement, HTMLCanvasElement, HTMLCanvasElement];
	const endStands = ([0, 1, 2] as const).map((up) =>
		seating(END_STANDS[0], "end", up, 0.45, false),
	) as [HTMLCanvasElement, HTMLCanvasElement, HTMLCanvasElement];
	const wall = boardOf("wall");
	const club = boardOf("club");
	const endWall = boardOf("end");
	return {
		style,
		wall,
		lower,
		lowerSparse: seating(LOWER, "lower", 0, 0.35, true, fill * 0.5),
		club,
		fascia: paintFascia(FASCIA.w, FASCIA.h, FASCIA.w / (X1 - X0), home, arena),
		upper: seating(UPPER, "upper", 0, 0.7, true),
		rim: paintRim(RIM.w, RIM.h, RIM.w / (X1 - X0), style),
		endWall,
		endStands,
		endSparse: seating(END_STANDS[0], "end", 0, 0.45, false, fill * 0.5),
		board: paintVideoBoard(END_BOARDS[0].w, END_BOARDS[0].h, home, logo),
		tunnel: paintTunnel(
			TUNNEL.w,
			TUNNEL.h,
			TUNNEL.w / PORTAL_W,
			home,
			name || region,
			style,
		),
		...(style.roof === "dome"
			? { roof: paintRoof(ROOF.w, ROOF.h, ROOF.w / (X1 - X0), seed) }
			: {}),
		screen: (set, screen, score) => {
			if (screen === "name" && set === "club" && score) {
				const key = score.join("-");
				if (scored?.key !== key) {
					const line = `${abbrev(away, "AWAY")} ${score[0]}  ${abbrev(home, "HOME")} ${score[1]}`;
					scored = { key, c: boardOf("club", "name", [...words, line]) };
				}
				return scored.c;
			}
			if (screen === "name") {
				return set === "wall" ? wall : set === "club" ? club : endWall;
			}
			const key = `${set}|${screen}`;
			let got = screens.get(key);
			if (!got) {
				got = boardOf(set, screen);
				screens.set(key, got);
			}
			return got;
		},
	};
};

// The sky over the open bowl: night, a deep blue going black overhead, a
// glow off the lights low down. (Or a roof over it.)
export const drawSky = (ctx: CanvasRenderingContext2D, style: StadiumStyle) => {
	const { width: w, height: h } = ctx.canvas;
	const g = ctx.createLinearGradient(0, 0, 0, h * 0.6);
	if (style.roof === "dome") {
		// (Under the roof - drawn over this - and the dark of it.)
		g.addColorStop(0, "#141519");
		g.addColorStop(1, "#1d1f25");
	} else if (style.sky === "night") {
		g.addColorStop(0, "#04060d");
		g.addColorStop(0.7, "#0d1424");
		g.addColorStop(1, "#1b2236");
	} else {
		g.addColorStop(0, "#5f8fc8");
		g.addColorStop(1, "#b9d1ea");
	}
	ctx.fillStyle = g;
	ctx.fillRect(0, 0, w, h);
};
