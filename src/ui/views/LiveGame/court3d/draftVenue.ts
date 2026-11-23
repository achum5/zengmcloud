import { makeCourtRng } from "../courtRng.ts";
import { drawFan, type Fan } from "./arena.ts";
import { depthOf, project, type Camera } from "./camera.ts";
import type { CrowdFace } from "./crowdFaces.ts";
import {
	familyAt,
	JET_MS,
	JETS,
	MARK,
	PODIUM,
	seatAt,
	SEATS,
	STAGE,
	STEPS,
	TABLE_H,
	TABLE_R,
	TABLES,
	groundAt,
	type DraftRoom,
} from "./draftStage.ts";
import { lightness, shade } from "./figure.ts";
import { COURT_W, type Pt, type Pt3 } from "./geometry.ts";
import { drawTexturedPlane, type TexturedPlane } from "./planes.ts";
import { drawChair, type Venue } from "./scene.ts";

// THE DRAFT'S ROOM (see draftStage.ts): the building's floor given over to
// the green room - round tables in black cloths, families and agents sat at
// them - and at the far end the stage: the LED wall behind it in the picking
// team's colors with its logo the size of a house, a dark podium before a
// pale panel and the team's color either side of it, steps down the middle
// of its front, the draft board up beside it and the team's own tower on the
// other side. Stands full all the way round; the house lights down; the
// stage lit.
//
// All of it painted once, a few pictures stood up in the world, and the
// people round the tables each a picture of their own (as the courtside seats
// are - see courtsideFor), sat facing in or, on the near side, their backs to
// the cameras.

export type DraftVenueInfo = {
	season: number;
	team: {
		region: string;
		name: string;
		abbrev: string;
		colors: [string, string, string];
	};
	// The team's logo, drawn big - if it can be.
	logo?: HTMLCanvasElement;
	// The draft board: the pick in, and the next ones up.
	board: { overall: number; label: string; now?: boolean }[];
	faces?: CrowdFace[];
};

// A wall standing at `y` facing the cameras, from x0 to x1 and z0 up to z1,
// a picture `w` by `h` px laid on it.
const wall = (
	x0: number,
	x1: number,
	y: number,
	z0: number,
	z1: number,
	w: number,
	h: number,
): TexturedPlane => ({
	origin: { x: x0, y, z: z1 },
	alongX: { x: (x1 - x0) / w, y: 0, z: 0 },
	alongY: { x: 0, y: 0, z: -(z1 - z0) / h },
	w,
	h,
});

const canvas = (w: number, h: number) => {
	const c = document.createElement("canvas");
	c.width = w;
	c.height = h;
	return c;
};
const rgba = (hex: string, a: number) => {
	const h = hex.replace("#", "");
	const full =
		h.length === 3
			? h
					.split("")
					.map((ch) => ch + ch)
					.join("")
			: h.padEnd(6, "0");
	const n = Number.parseInt(full.slice(0, 6), 16);
	return Number.isFinite(n)
		? `rgba(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255}, ${a})`
		: `rgba(255,255,255,${a})`;
};
const inkOn = (c: string) => (lightness(c) > 0.62 ? "#111216" : "#ffffff");

// The team's colors as an LED wall shows them: its main color, unless that
// is next to black - then its second.
const ledColors = (colors: [string, string, string]): [string, string] => {
	const [a, b, c] = colors;
	const main = lightness(a) < 0.1 ? b : a;
	const accent = main === a ? (lightness(b) < 0.1 ? c : b) : c;
	return [main, accent === main ? "#ffffff" : accent];
};

// The team's color for the dots either side of the podium: its first, if
// that is a color and not near white or black - its boldest otherwise.
const rgbOf = (hex: string): [number, number, number] => {
	const m = /rgba\((\d+), (\d+), (\d+)/.exec(rgba(hex, 1));
	return m ? [+m[1]!, +m[2]!, +m[3]!] : [255, 255, 255];
};
const boldColor = (colors: [string, string, string], main: string) => {
	const scoreOf = (c: string) => {
		const [r, g, b] = rgbOf(c);
		const chroma = (Math.max(r, g, b) - Math.min(r, g, b)) / 255;
		const l = lightness(c);
		return l < 0.06 || l > 0.85 ? 0 : chroma * (1 - Math.abs(l - 0.35));
	};
	if (scoreOf(colors[0]) > 0.2) {
		return colors[0];
	}
	let best = main;
	let score = -1;
	for (const c of colors) {
		const s = scoreOf(c);
		if (s > score) {
			score = s;
			best = c;
		}
	}
	return score > 0.12 ? best : main;
};

// The logo fit in a box round (cx, cy).
const drawLogo = (
	g: CanvasRenderingContext2D,
	logo: HTMLCanvasElement | undefined,
	cx: number,
	cy: number,
	size: number,
	fallback: string,
	ink: string,
) => {
	if (logo) {
		const k = size / Math.max(logo.width, logo.height);
		const w = logo.width * k;
		const h = logo.height * k;
		g.imageSmoothingEnabled = true;
		g.drawImage(logo, cx - w / 2, cy - h / 2, w, h);
		return;
	}
	g.fillStyle = ink;
	g.textAlign = "center";
	g.textBaseline = "middle";
	g.font = `900 ${Math.round(size * 0.42)}px sans-serif`;
	g.fillText(fallback, cx, cy);
};

// Words as big as fit across `w` px, no bigger than `size`.
const fitText = (
	g: CanvasRenderingContext2D,
	text: string,
	x: number,
	y: number,
	w: number,
	size: number,
	weight = 900,
) => {
	let s = size;
	g.font = `${weight} ${s}px sans-serif`;
	const m = g.measureText(text).width;
	if (m > w) {
		s = Math.max(6, Math.floor((s * w) / m));
		g.font = `${weight} ${s}px sans-serif`;
	}
	g.fillText(text, x, y);
};

// The look of an LED panel up close: its rows of lights.
const ledRows = (g: CanvasRenderingContext2D, w: number, h: number) => {
	g.fillStyle = "rgba(0, 0, 0, 0.18)";
	for (let y = 0; y < h; y += 3) {
		g.fillRect(0, y, w, 1);
	}
};

// ---- the stage's pictures -------------------------------------------------

// The LED wall behind the stage (feet).
const WALL = { x0: STAGE.x0 - 6, x1: STAGE.x1 + 6, z1: STAGE.z + 24 };
const WALL_PX = 10;
// The draft board, stage right of the room (the left as the cameras see it),
// and the team's tower on the other side - stood on the floor.
const BOARD = { x0: 10, x1: 23, y: -7, z0: 0, z1: 28 };
const TOWER = { x0: 71, x1: 79, y: -7, z0: 0, z1: 30 };
const TOWER_PX = 10;

const paintWall = (info: DraftVenueInfo, main: string, accent: string) => {
	const w = Math.round((WALL.x1 - WALL.x0) * WALL_PX);
	const h = Math.round((WALL.z1 - STAGE.z) * WALL_PX);
	const c = canvas(w, h);
	const g = c.getContext("2d")!;
	g.fillStyle = "#05060b";
	g.fillRect(0, 0, w, h);
	// The middle panel, the logo on it as big as it goes.
	const mid = (MARK.x - 0.6 - WALL.x0) * WALL_PX;
	const pw = 22 * WALL_PX;
	const glow = g.createRadialGradient(
		mid,
		h * 0.48,
		10,
		mid,
		h * 0.48,
		pw * 0.7,
	);
	glow.addColorStop(0, shade(main, 0.28));
	glow.addColorStop(0.55, main);
	glow.addColorStop(1, shade(main, -0.45));
	g.fillStyle = glow;
	g.beginPath();
	g.roundRect(mid - pw / 2, 8, pw, h - 16, 12);
	g.fill();
	// Its edges lit in the second color.
	g.strokeStyle = accent;
	g.lineWidth = 4;
	g.stroke();
	drawLogo(g, info.logo, mid, h * 0.5, h * 0.8, info.team.abbrev, inkOn(main));
	// The wings either side: panels raked back at an angle, the second color
	// in a band across each, the team's name on them.
	const wing = (left: boolean) => {
		const s = left ? -1 : 1;
		const inner = mid + s * (pw / 2 + 10);
		const outer = left ? 6 : w - 6;
		g.fillStyle = shade(main, -0.22);
		g.beginPath();
		g.moveTo(inner, 14);
		g.lineTo(outer, 40);
		g.lineTo(outer, h - 30);
		g.lineTo(inner, h - 12);
		g.closePath();
		g.fill();
		g.fillStyle = accent;
		g.beginPath();
		g.moveTo(inner, h * 0.62);
		g.lineTo(outer, h * 0.7);
		g.lineTo(outer, h * 0.7 + 10);
		g.lineTo(inner, h * 0.62 + 10);
		g.closePath();
		g.fill();
		const cx = (inner + outer) / 2;
		const room = Math.abs(outer - inner) - 24;
		g.fillStyle = inkOn(shade(main, -0.22));
		g.textAlign = "center";
		g.textBaseline = "middle";
		fitText(
			g,
			(left ? info.team.region : info.team.name).toUpperCase(),
			cx,
			h * 0.36,
			room,
			44,
		);
		g.globalAlpha = 0.8;
		fitText(g, `${info.season} DRAFT`, cx, h * 0.84, room * 0.8, 20, 800);
		g.globalAlpha = 1;
	};
	wing(true);
	wing(false);
	// A strip of lights along the top.
	g.fillStyle = "rgba(255, 255, 255, 0.85)";
	for (let x = 8; x < w; x += 14) {
		g.fillRect(x, 2, 4, 3);
	}
	ledRows(g, w, h);
	return c;
};

// Behind the podium, its own backdrop: a pale stone panel no wider than the
// man reading, lit, and a panel of the team's color either side of it - rows
// of dots of light all over them - the commissioner read out in front of it.
export const BACKDROP = {
	x0: PODIUM.x - 1.35,
	x1: PODIUM.x + 1.35,
	y: PODIUM.y - PODIUM.d / 2 - 2.4,
	z0: STAGE.z,
	z1: STAGE.z + 13,
	side: 4.6,
};
const BACKDROP_PX = 24;
const paintBackdrop = () => {
	const w = Math.round((BACKDROP.x1 - BACKDROP.x0) * BACKDROP_PX);
	const h = Math.round((BACKDROP.z1 - BACKDROP.z0) * BACKDROP_PX);
	const c = canvas(w, h);
	const g = c.getContext("2d")!;
	const grad = g.createLinearGradient(0, 0, w, 0);
	grad.addColorStop(0, "#d4d6da");
	grad.addColorStop(0.45, "#f1f2f3");
	grad.addColorStop(1, "#c9cbd0");
	g.fillStyle = grad;
	g.fillRect(0, 0, w, h);
	// A stone's grain to it: streaks down it, flecks.
	const rng = makeCourtRng("draft|backdrop");
	for (let i = 0; i < 60; i++) {
		g.fillStyle = `rgba(90, 92, 100, ${0.04 + rng() * 0.06})`;
		g.fillRect(rng() * w, rng() * h, 1 + rng() * 2, 8 + rng() * 40);
	}
	for (let i = 0; i < 1600; i++) {
		g.fillStyle = `rgba(0, 0, 0, ${0.04 + rng() * 0.07})`;
		g.fillRect(rng() * w, rng() * h, 1 + rng() * 2, 1 + rng() * 2);
	}
	// Its edges: lit down the one, in shadow down the other.
	g.fillStyle = "rgba(255, 255, 255, 0.6)";
	g.fillRect(0, 0, 3, h);
	g.fillStyle = "#26272d";
	g.fillRect(w - 5, 0, 5, h);
	return c;
};
const paintDots = (main: string) => {
	const w = Math.round(BACKDROP.side * BACKDROP_PX);
	const h = Math.round((BACKDROP.z1 - BACKDROP.z0) * BACKDROP_PX);
	const c = canvas(w, h);
	const g = c.getContext("2d")!;
	const grad = g.createLinearGradient(0, 0, 0, h);
	grad.addColorStop(0, shade(main, 0.2));
	grad.addColorStop(0.6, main);
	grad.addColorStop(1, shade(main, -0.3));
	g.fillStyle = grad;
	g.fillRect(0, 0, w, h);
	// The dots: each a dark hole with the light catching under it.
	const step = 8;
	for (let y = step / 2; y < h; y += step) {
		for (let x = step / 2; x < w; x += step) {
			g.fillStyle = "rgba(255, 255, 255, 0.16)";
			g.beginPath();
			g.arc(x, y + 0.8, 2.1, 0, Math.PI * 2);
			g.fill();
			g.fillStyle = "rgba(0, 0, 0, 0.45)";
			g.beginPath();
			g.arc(x, y, 1.9, 0, Math.PI * 2);
			g.fill();
		}
	}
	return c;
};

// The podium: a dark wood lectern, two bands of brushed steel round its top,
// the team's logo small low on its front.
const paintPodium = (info: DraftVenueInfo, main: string) => {
	const px = 24;
	const w = Math.round(PODIUM.w * px);
	const h = Math.round(PODIUM.h * px);
	const c = canvas(w, h);
	const g = c.getContext("2d")!;
	const grad = g.createLinearGradient(0, 0, w, 0);
	grad.addColorStop(0, "#1a1716");
	grad.addColorStop(0.5, "#2b2624");
	grad.addColorStop(1, "#171413");
	g.fillStyle = grad;
	g.fillRect(0, 0, w, h);
	// The wood's grain.
	const rng = makeCourtRng("draft|podium");
	for (let i = 0; i < 40; i++) {
		g.fillStyle = `rgba(255, 240, 220, ${0.015 + rng() * 0.03})`;
		g.fillRect(0, rng() * h, w, 1);
	}
	for (const y of [0.04, 0.1]) {
		g.fillStyle = "#d9dadd";
		g.fillRect(0, Math.round(h * y), w, 2);
		g.fillStyle = "#7d7e84";
		g.fillRect(0, Math.round(h * y) + 2, w, 1);
	}
	g.fillStyle = "#0b0c10";
	g.fillRect(0, h - 6, w, 6);
	drawLogo(
		g,
		info.logo,
		w / 2,
		h * 0.6,
		w * 0.32,
		info.team.abbrev,
		inkOn(main),
	);
	return c;
};

const FASCIA_PX = 12;
const paintFascia = (info: DraftVenueInfo, main: string) => {
	const w = Math.round((STAGE.x1 - STAGE.x0) * FASCIA_PX);
	const h = Math.round(STAGE.z * FASCIA_PX);
	const c = canvas(w, h);
	const g = c.getContext("2d")!;
	g.fillStyle = "#0a0b11";
	g.fillRect(0, 0, w, h);
	g.fillStyle = main;
	g.fillRect(0, 0, w, 5);
	g.fillStyle = "rgba(255, 255, 255, 0.2)";
	g.textAlign = "center";
	g.textBaseline = "middle";
	g.font = `800 ${Math.round(h * 0.42)}px sans-serif`;
	for (let x = 70; x < w; x += 150) {
		g.fillText(`${info.season} DRAFT`, x, h * 0.58);
	}
	return c;
};

const paintTower = (info: DraftVenueInfo, main: string, accent: string) => {
	const w = Math.round((TOWER.x1 - TOWER.x0) * TOWER_PX);
	const h = Math.round((TOWER.z1 - TOWER.z0) * TOWER_PX);
	const c = canvas(w, h);
	const g = c.getContext("2d")!;
	const grad = g.createLinearGradient(0, 0, 0, h);
	grad.addColorStop(0, shade(main, 0.15));
	grad.addColorStop(1, shade(main, -0.5));
	g.fillStyle = grad;
	g.fillRect(0, 0, w, h);
	g.fillStyle = accent;
	g.fillRect(0, 0, 5, h);
	g.fillRect(w - 5, 0, 5, h);
	drawLogo(
		g,
		info.logo,
		w / 2,
		w * 0.62,
		w * 0.8,
		info.team.abbrev,
		inkOn(main),
	);
	g.save();
	g.translate(w / 2, h * 0.62);
	g.rotate(-Math.PI / 2);
	g.fillStyle = inkOn(main);
	g.textAlign = "center";
	g.textBaseline = "middle";
	fitText(g, info.team.name.toUpperCase(), 0, 0, h * 0.62, w * 0.62);
	g.restore();
	ledRows(g, w, h);
	return c;
};

const paintBoard = (info: DraftVenueInfo, main: string) => {
	const w = Math.round((BOARD.x1 - BOARD.x0) * TOWER_PX);
	const h = Math.round((BOARD.z1 - BOARD.z0) * TOWER_PX);
	const c = canvas(w, h);
	const g = c.getContext("2d")!;
	g.fillStyle = "#0a1022";
	g.fillRect(0, 0, w, h);
	g.fillStyle = "#ffffff";
	g.textAlign = "center";
	g.textBaseline = "middle";
	fitText(g, String(info.season), w / 2, 22, w - 16, 26);
	fitText(g, "DRAFT", w / 2, 46, w - 16, 20, 800);
	g.fillStyle = "rgba(255,255,255,0.35)";
	g.fillRect(8, 62, w - 16, 2);
	const rows = info.board.slice(0, 9);
	const rowH = Math.min(24, (h - 80) / Math.max(1, rows.length));
	rows.forEach((r, i) => {
		const y = 74 + i * rowH;
		if (r.now) {
			g.fillStyle = main;
			g.fillRect(4, y, w - 8, rowH - 3);
		}
		g.fillStyle = r.now ? inkOn(main) : "#e8ebf4";
		g.textAlign = "left";
		g.textBaseline = "middle";
		g.font = `900 ${Math.round(rowH * 0.6)}px sans-serif`;
		g.fillText(String(r.overall), 10, y + rowH / 2 - 1);
		fitText(
			g,
			r.label.toUpperCase(),
			38,
			y + rowH / 2 - 1,
			w - 46,
			rowH * 0.55,
			800,
		);
	});
	ledRows(g, w, h);
	return c;
};

// ---- the people round the tables -----------------------------------------

// Each one's picture this big (feet), this many px to the foot - and the
// nearest in front of a camera one is drawn (feet).
const CELL_W = 2.4;
const CELL_H = 7.2;
const PX = 10;
const GAP = 3;
const COLS_N = 16;
const NEAREST = 2.6;
const SUITS = [
	"#16171b",
	"#1c1f26",
	"#2b2f38",
	"#1f2a44",
	"#3b3f46",
	"#24262c",
];
const SHIRTS = ["#f2f2f0", "#e4e1da", "#cfdcef", "#f2f2f0"];
const DRESSES = [
	"#6b1f2a",
	"#24365c",
	"#f2f2f0",
	"#c9a227",
	"#2f4f4a",
	"#8a3b6b",
	"#d9b8a0",
	"#101012",
	"#b8452e",
];
const SKINS = ["#f1c7a5", "#d9a77f", "#b07a52", "#8a5a3b", "#5e3b26"];
const HAIR = ["#141110", "#221813", "#2f2219", "#4a3322", "#7a5a38", "#b99560"];
const CHAIR = "#1b1c22";

type Guest = {
	at: Pt;
	cell: number;
	// Sat with his back to the cameras (the near side of a table).
	back: boolean;
	// At his family's table: up on their feet when his name is read.
	his: boolean;
	// Of the room, how soon each claps (0 to 1).
	rise: number;
};

export type DraftVenuePaint = {
	wall: HTMLCanvasElement;
	podium: HTMLCanvasElement;
	backdrop: HTMLCanvasElement;
	dots: HTMLCanvasElement;
	fascia: HTMLCanvasElement;
	tower: HTMLCanvasElement;
	board: HTMLCanvasElement;
	// Each of them sat, sat clapping, and up on their feet both ways they wave.
	sat: HTMLCanvasElement;
	clap: HTMLCanvasElement;
	up: HTMLCanvasElement;
	wave: HTMLCanvasElement;
	guests: Guest[];
	main: string;
	accent: string;
};

export const paintDraftVenue = (
	info: DraftVenueInfo,
	room: DraftRoom,
): DraftVenuePaint => {
	const [main, accent] = ledColors(info.team.colors);
	const rng = makeCourtRng(
		`draft|${info.season}|${info.team.abbrev}|${info.team.name}`,
	);
	const pick = <T>(a: readonly T[]): T => a[Math.floor(rng() * a.length)]!;

	const guests: Guest[] = [];
	for (const table of TABLES) {
		const family = familyAt(room, table);
		const his = family !== undefined;
		for (const a of family?.guests ?? SEATS) {
			// (Now and then an empty chair - somebody off talking to somebody.)
			if (!his && rng() < 0.12) {
				continue;
			}
			const s = seatAt(table, a);
			guests.push({
				at: s,
				cell: guests.length,
				back: Math.sin(a) > 0.2,
				his,
				rise: rng(),
			});
		}
	}

	const cw = Math.round(CELL_W * PX);
	const ch = Math.round(CELL_H * PX);
	const rows = Math.ceil(guests.length / COLS_N);
	const atlas = () => canvas(COLS_N * (cw + GAP), rows * (ch + GAP));
	const sat = atlas();
	const clap = atlas();
	const up = atlas();
	const wave = atlas();
	const px = PX;
	const faces = info.faces ?? [];
	for (const gu of guests) {
		const ox = (gu.cell % COLS_N) * (cw + GAP);
		const oy = Math.floor(gu.cell / COLS_N) * (ch + GAP);
		const x = ox + cw / 2;
		const floor = oy + ch;
		const yb = floor - 1.5 * px + 0.5 * px;
		const face = faces.length > 0 ? pick(faces) : undefined;
		const female = face ? face.female === true : rng() < 0.4;
		const skin = face?.skin ?? pick(SKINS);
		const suit = pick(SUITS);
		const fan: Fan = {
			x,
			yb,
			bodyW: (female ? 1.2 : 1.35 + rng() * 0.25) * px,
			color: female ? pick(DRESSES) : pick(SHIRTS),
			skin,
			...(face ? { face } : {}),
			pants: female ? "#18181b" : suit,
			...(female ? {} : { jacket: suit }),
			...(rng() < 0.12 ? { holding: "phone" as const } : {}),
		};
		const hair = pick(HAIR);
		const shoes = "#111114";
		const seatedLegs = (g: CanvasRenderingContext2D) => {
			g.fillStyle = CHAIR;
			g.beginPath();
			g.roundRect(x - 0.85 * px, yb - 2.25 * px, 1.7 * px, 1.9 * px, 0.3 * px);
			g.fill();
			for (const side of [-1, 1]) {
				g.fillStyle = fan.pants;
				g.fillRect(
					x + side * 0.08 * px - (side < 0 ? 0.42 * px : 0),
					yb - 0.5 * px,
					0.42 * px,
					floor - yb + 0.3 * px,
				);
				g.fillStyle = shoes;
				g.fillRect(
					x + side * 0.3 * px - 0.28 * px,
					floor - 0.25 * px,
					0.56 * px,
					0.25 * px,
				);
			}
		};
		// From behind: the back of his coat (or her dress), the back of the
		// head, the chair's back over the bottom of it.
		const backOf = (
			g: CanvasRenderingContext2D,
			standing: boolean,
			armsUp: 0 | 1 | 2,
		) => {
			const ybb = standing ? floor - 2 * px : yb;
			const bodyH = (standing ? 2 : 1.45) * px;
			const lift = standing ? 0.55 * px : 0;
			const top = ybb - bodyH - 0.5 * px - lift;
			const bottom = ybb - 0.5 * px - lift;
			const sw = fan.bodyW / 2;
			const ww = fan.bodyW * 0.4;
			const coat = fan.jacket ?? fan.color;
			if (standing) {
				for (const side of [-1, 1]) {
					g.fillStyle = fan.pants;
					g.fillRect(
						x + side * 0.06 * px - (side < 0 ? 0.4 * px : 0),
						bottom - 0.1 * px,
						0.4 * px,
						floor - bottom,
					);
				}
			}
			g.fillStyle = coat;
			g.beginPath();
			g.moveTo(x - sw, top + 0.32 * px);
			g.quadraticCurveTo(x - sw, top, x - sw + 0.3 * px, top);
			g.lineTo(x + sw - 0.3 * px, top);
			g.quadraticCurveTo(x + sw, top, x + sw, top + 0.32 * px);
			g.lineTo(x + ww, bottom);
			g.lineTo(x - ww, bottom);
			g.closePath();
			g.fill();
			// (A seam down the back, the light on the shoulders.)
			g.fillStyle = shade(coat, -0.18);
			g.fillRect(
				x - 0.03 * px,
				top + 0.3 * px,
				0.06 * px,
				bottom - top - 0.3 * px,
			);
			g.fillStyle = shade(coat, 0.14);
			g.fillRect(x - sw + 0.3 * px, top, fan.bodyW - 0.6 * px, 0.12 * px);
			const headY = top - 0.42 * px;
			g.lineCap = "round";
			if (armsUp) {
				const spread = (armsUp === 1 ? 0.35 : 0.95) * px;
				for (const side of [-1, 1]) {
					const sx = x + (side * fan.bodyW) / 2.4;
					const sy = top + 0.18 * px;
					const hx = sx + side * spread;
					const hy = sy - 1.4 * px;
					g.strokeStyle = coat;
					g.lineWidth = 0.34 * px;
					g.beginPath();
					g.moveTo(sx, sy);
					g.lineTo((sx + hx) / 2 + side * 0.1 * px, (sy + hy) / 2);
					g.lineTo(hx, hy);
					g.stroke();
					g.fillStyle = skin;
					g.beginPath();
					g.arc(hx, hy - 0.1 * px, 0.18 * px, 0, Math.PI * 2);
					g.fill();
				}
			} else {
				g.strokeStyle = shade(coat, -0.1);
				g.lineWidth = 0.3 * px;
				for (const side of [-1, 1]) {
					g.beginPath();
					g.moveTo(x + side * (sw - 0.12 * px), top + 0.22 * px);
					g.lineTo(x + side * (sw + 0.02 * px), top + 0.95 * px);
					g.stroke();
				}
			}
			g.fillStyle = skin;
			g.fillRect(x - 0.13 * px, headY + 0.2 * px, 0.26 * px, 0.32 * px);
			g.fillStyle = hair;
			g.beginPath();
			g.ellipse(
				x,
				headY,
				0.36 * px,
				(female ? 0.46 : 0.4) * px,
				0,
				0,
				Math.PI * 2,
			);
			g.fill();
			if (female && rng() < 0.6) {
				g.fillRect(x - 0.34 * px, headY, 0.68 * px, 0.7 * px);
			}
			if (!standing) {
				// The chair's back, over his.
				g.fillStyle = CHAIR;
				g.beginPath();
				g.roundRect(
					x - 0.85 * px,
					yb - 1.55 * px,
					1.7 * px,
					1.15 * px,
					0.25 * px,
				);
				g.fill();
			}
		};
		const front = (g: CanvasRenderingContext2D, f: Fan) => {
			seatedLegs(g);
			drawFan(g, f, px);
		};
		const standingFront = (g: CanvasRenderingContext2D, arms: 1 | 2) => {
			const hips = floor - 3.05 * px;
			for (const side of [-1, 1]) {
				g.fillStyle = fan.pants;
				g.fillRect(
					x + side * 0.06 * px - (side < 0 ? 0.4 * px : 0),
					hips - 0.1 * px,
					0.4 * px,
					floor - hips,
				);
			}
			drawFan(
				g,
				{
					...fan,
					holding: undefined,
					yb: floor - 2 * px,
					arms: { up: arms, reach: 1.25 + gu.rise * 0.35 },
				},
				px,
			);
		};
		const at = (cv: HTMLCanvasElement) => {
			const g = cv.getContext("2d")!;
			g.save();
			g.beginPath();
			g.rect(ox, oy, cw, ch);
			g.clip();
			return g;
		};
		const gs = at(sat);
		const gc = at(clap);
		const gup = at(up);
		const gw = at(wave);
		if (gu.back) {
			backOf(gs, false, 0);
			backOf(gc, false, 0);
			backOf(gup, true, 1);
			backOf(gw, true, 2);
		} else {
			front(gs, fan);
			front(gc, { ...fan, holding: "clap" });
			standingFront(gup, 1);
			standingFront(gw, 2);
		}
		for (const g of [gs, gc, gup, gw]) {
			g.restore();
		}
	}
	return {
		wall: paintWall(info, main, accent),
		podium: paintPodium(info, main),
		backdrop: paintBackdrop(),
		dots: paintDots(boldColor(info.team.colors, main)),
		fascia: paintFascia(info, main),
		tower: paintTower(info, main, accent),
		board: paintBoard(info, main),
		sat,
		clap,
		up,
		wave,
		guests,
		main,
		accent,
	};
};

// ---- drawn ------------------------------------------------------------------

type Ctx = CanvasRenderingContext2D;
const fill = (
	ctx: Ctx,
	cam: Camera,
	pts: Pt3[],
	color: string | CanvasGradient,
) => {
	ctx.fillStyle = color;
	ctx.beginPath();
	for (const p of pts) {
		const q = project(cam, p);
		ctx.lineTo(q.x, q.y);
	}
	ctx.closePath();
	ctx.fill();
};
// A box's faces that the camera sees (its front toward the cameras, its top,
// the side it is on) - the far ones under the near.
const drawBox = (
	ctx: Ctx,
	cam: Camera,
	x0: number,
	x1: number,
	y0: number,
	y1: number,
	z0: number,
	z1: number,
	colors: { front: string; top: string; side: string },
	front?: { img: HTMLCanvasElement; cols: number; rows: number },
) => {
	if (cam.pos.x < x0) {
		fill(
			ctx,
			cam,
			[
				{ x: x0, y: y0, z: z0 },
				{ x: x0, y: y1, z: z0 },
				{ x: x0, y: y1, z: z1 },
				{ x: x0, y: y0, z: z1 },
			],
			colors.side,
		);
	} else if (cam.pos.x > x1) {
		fill(
			ctx,
			cam,
			[
				{ x: x1, y: y0, z: z0 },
				{ x: x1, y: y1, z: z0 },
				{ x: x1, y: y1, z: z1 },
				{ x: x1, y: y0, z: z1 },
			],
			colors.side,
		);
	}
	if (cam.pos.z > z1) {
		fill(
			ctx,
			cam,
			[
				{ x: x0, y: y0, z: z1 },
				{ x: x1, y: y0, z: z1 },
				{ x: x1, y: y1, z: z1 },
				{ x: x0, y: y1, z: z1 },
			],
			colors.top,
		);
	}
	if (front) {
		const pw = front.img.width;
		const ph = front.img.height;
		drawTexturedPlane(
			ctx,
			cam,
			wall(x0, x1, y1, z0, z1, pw, ph),
			front.img,
			front.cols,
			front.rows,
		);
	} else {
		fill(
			ctx,
			cam,
			[
				{ x: x0, y: y1, z: z0 },
				{ x: x1, y: y1, z: z0 },
				{ x: x1, y: y1, z: z1 },
				{ x: x0, y: y1, z: z1 },
			],
			colors.front,
		);
	}
};

// A round table in its black cloth, down to the floor.
const CLOTH = "#202127";
const drawTable = (ctx: Ctx, cam: Camera, c: Pt) => {
	const toward = Math.atan2(cam.pos.y - c.y, cam.pos.x - c.x);
	const N = 14;
	const ring = (z: number, from: number, to: number) => {
		const out: Pt3[] = [];
		for (let i = 0; i <= N; i++) {
			const a = from + ((to - from) * i) / N;
			out.push({
				x: c.x + TABLE_R * Math.cos(a),
				y: c.y + TABLE_R * Math.sin(a),
				z,
			});
		}
		return out;
	};
	const a0 = toward - Math.PI / 2;
	const a1 = toward + Math.PI / 2;
	const top = ring(TABLE_H, a0, a1);
	const bottom = ring(0, a0, a1).reverse();
	// The skirt: lit from above, darker down to the floor and round the sides.
	const left = project(cam, top[0]!);
	const right = project(cam, top[N]!);
	const g = ctx.createLinearGradient(left.x, 0, right.x, 0);
	g.addColorStop(0, "#0c0c10");
	g.addColorStop(0.35, "#1b1c22");
	g.addColorStop(0.65, "#1b1c22");
	g.addColorStop(1, "#0c0c10");
	fill(ctx, cam, [...top, ...bottom], g);
	fill(ctx, cam, ring(TABLE_H, 0, Math.PI * 2), CLOTH);
	// Something in the middle of it: a little low arrangement, a glass or two.
	const mid = project(cam, { x: c.x, y: c.y, z: TABLE_H });
	ctx.fillStyle = "#2b2e36";
	ctx.fillRect(
		mid.x - 0.25 * mid.k,
		mid.y - 0.5 * mid.k,
		0.5 * mid.k,
		0.5 * mid.k,
	);
	ctx.fillStyle = "rgba(220, 235, 255, 0.8)";
	ctx.fillRect(
		mid.x + 0.6 * mid.k,
		mid.y - 0.45 * mid.k,
		0.14 * mid.k,
		0.42 * mid.k,
	);
	ctx.fillRect(
		mid.x - 0.9 * mid.k,
		mid.y - 0.42 * mid.k,
		0.14 * mid.k,
		0.4 * mid.k,
	);
};

// Smoke going up, a puff at a time, thinning as it goes.
const drawJet = (ctx: Ctx, cam: Camera, at: Pt3, since: number) => {
	const u = since / JET_MS;
	if (u <= 0 || u >= 1.6) {
		return;
	}
	const fade = u < 1 ? 1 : Math.max(0, 1 - (u - 1) / 0.6);
	const height = Math.min(1, u * 3) * 17;
	for (let i = 0; i < 9; i++) {
		const f = i / 8;
		const z = at.z + 0.5 + f * height;
		const r = 0.7 + f * 3.2 + u * 1.2;
		const p = project(cam, {
			x: at.x + Math.sin(i * 1.7 + since / 300) * 0.3 * f,
			y: at.y,
			z,
		});
		const rad = Math.max(2, r * p.k);
		const g = ctx.createRadialGradient(p.x, p.y, 0, p.x, p.y, rad);
		const a = (0.5 - f * 0.32) * fade;
		g.addColorStop(0, `rgba(245, 247, 255, ${a})`);
		g.addColorStop(1, "rgba(245, 247, 255, 0)");
		ctx.fillStyle = g;
		ctx.beginPath();
		ctx.arc(p.x, p.y, rad, 0, Math.PI * 2);
		ctx.fill();
	}
};

// The light on the stage from the truss over it: beams down onto where the
// pick and the commissioner stand.
const drawBeams = (ctx: Ctx, cam: Camera, t: number) => {
	ctx.save();
	ctx.globalCompositeOperation = "lighter";
	for (const [dx, alpha] of [
		[-2.6, 0.08],
		[1.4, 0.1],
	] as const) {
		const foot = project(cam, { x: MARK.x + dx, y: MARK.y, z: STAGE.z });
		const top = project(cam, {
			x: MARK.x + dx * 0.4,
			y: MARK.y - 4,
			z: STAGE.z + 34,
		});
		const r = Math.max(4, foot.k * 3.4);
		const g = ctx.createLinearGradient(0, top.y, 0, foot.y);
		const a = alpha * (0.85 + 0.15 * Math.sin(t / 900 + dx));
		g.addColorStop(0, "rgba(255, 248, 230, 0)");
		g.addColorStop(1, `rgba(255, 248, 230, ${a})`);
		ctx.fillStyle = g;
		ctx.beginPath();
		ctx.moveTo(top.x - r * 0.2, top.y);
		ctx.lineTo(top.x + r * 0.2, top.y);
		ctx.lineTo(foot.x + r, foot.y);
		ctx.lineTo(foot.x - r, foot.y);
		ctx.closePath();
		ctx.fill();
	}
	ctx.restore();
};

// Colored lights moving slowly over the floor of the room.
const SWEEPS = 3;
const sweepAt = (i: number, t: number): Pt => ({
	x: COURT_W / 2 + 38 * Math.sin(t / 2600 + i * 2.1),
	y: 26 + 16 * Math.sin(t / 3300 + i * 1.3 + 0.7),
});

export const draftVenue = (paint: DraftVenuePaint, room: DraftRoom): Venue => {
	const cw = Math.round(CELL_W * PX);
	const ch = Math.round(CELL_H * PX);
	const wallPlane = wall(
		WALL.x0,
		WALL.x1,
		STAGE.y0,
		STAGE.z,
		WALL.z1,
		paint.wall.width,
		paint.wall.height,
	);
	const towerPlane = wall(
		TOWER.x0,
		TOWER.x1,
		TOWER.y,
		TOWER.z0,
		TOWER.z1,
		paint.tower.width,
		paint.tower.height,
	);
	const boardPlane = wall(
		BOARD.x0,
		BOARD.x1,
		BOARD.y,
		BOARD.z0,
		BOARD.z1,
		paint.board.width,
		paint.board.height,
	);
	const backdropPlane = wall(
		BACKDROP.x0,
		BACKDROP.x1,
		BACKDROP.y,
		BACKDROP.z0,
		BACKDROP.z1,
		paint.backdrop.width,
		paint.backdrop.height,
	);
	const dotPlanes = [BACKDROP.x0 - BACKDROP.side, BACKDROP.x1].map((x0) =>
		wall(
			x0,
			x0 + BACKDROP.side,
			BACKDROP.y - 0.3,
			BACKDROP.z0,
			BACKDROP.z1,
			paint.dots.width,
			paint.dots.height,
		),
	);
	return {
		round: true,
		ground: groundAt,
		floor: (ctx, cam, t) => {
			// The house lights down.
			ctx.save();
			ctx.setTransform(1, 0, 0, 1, 0, 0);
			ctx.fillStyle = "rgba(4, 8, 30, 0.72)";
			ctx.fillRect(0, 0, ctx.canvas.width, ctx.canvas.height);
			ctx.restore();
			// The floor: dark, a pool of each team-colored light moving over it.
			fill(
				ctx,
				cam,
				[
					{ x: -13, y: -12, z: 0 },
					{ x: COURT_W + 13, y: -12, z: 0 },
					{ x: COURT_W + 13, y: 62, z: 0 },
					{ x: -13, y: 62, z: 0 },
				],
				"#0d0e15",
			);
			ctx.save();
			ctx.globalCompositeOperation = "lighter";
			for (let i = 0; i < SWEEPS; i++) {
				const at = sweepAt(i, t);
				const p = project(cam, { x: at.x, y: at.y, z: 0 });
				const far = project(cam, { x: at.x, y: at.y - 7, z: 0 });
				const rx = Math.max(2, p.k * 7);
				const ry = Math.max(1, Math.abs(p.y - far.y));
				ctx.save();
				ctx.translate(p.x, p.y);
				ctx.scale(1, ry / rx);
				const g = ctx.createRadialGradient(0, 0, 0, 0, 0, rx);
				const c = i % 2 === 0 ? paint.main : "#3a6dff";
				g.addColorStop(0, rgba(c, 0.22));
				g.addColorStop(1, rgba(c, 0));
				ctx.fillStyle = g;
				ctx.beginPath();
				ctx.arc(0, 0, rx, 0, Math.PI * 2);
				ctx.fill();
				ctx.restore();
			}
			ctx.restore();
			// The LED wall, the board and the tower.
			drawTexturedPlane(ctx, cam, wallPlane, paint.wall, 10, 5);
			drawTexturedPlane(ctx, cam, boardPlane, paint.board, 3, 6);
			drawTexturedPlane(ctx, cam, towerPlane, paint.tower, 2, 6);
			// The stage: its floor lit from the wall behind it, its front.
			const back = project(cam, { x: MARK.x, y: STAGE.y0, z: STAGE.z });
			const front = project(cam, { x: MARK.x, y: STAGE.y1, z: STAGE.z });
			const sheen = ctx.createLinearGradient(0, back.y, 0, front.y);
			sheen.addColorStop(0, shade(paint.main, -0.55));
			sheen.addColorStop(1, "#101118");
			drawBox(
				ctx,
				cam,
				STAGE.x0,
				STAGE.x1,
				STAGE.y0,
				STAGE.y1,
				0,
				STAGE.z,
				{ front: "#0a0b11", top: "#101118", side: "#08090d" },
				{ img: paint.fascia, cols: 8, rows: 1 },
			);
			if (cam.pos.z > STAGE.z) {
				fill(
					ctx,
					cam,
					[
						{ x: STAGE.x0, y: STAGE.y0, z: STAGE.z },
						{ x: STAGE.x1, y: STAGE.y0, z: STAGE.z },
						{ x: STAGE.x1, y: STAGE.y1, z: STAGE.z },
						{ x: STAGE.x0, y: STAGE.y1, z: STAGE.z },
					],
					sheen,
				);
			}
			// The podium's backdrop, stood on the stage behind it.
			for (const d of dotPlanes) {
				drawTexturedPlane(ctx, cam, d, paint.dots, 2, 6);
			}
			drawTexturedPlane(ctx, cam, backdropPlane, paint.backdrop, 2, 6);
			// Its steps: the furthest first, each a riser and a tread, an edge
			// of light along each.
			const tread = (STEPS.y1 - STAGE.y1) / STEPS.n;
			const rise = STAGE.z / STEPS.n;
			for (let i = STEPS.n - 1; i >= 0; i--) {
				const y0 = STAGE.y1 + (STEPS.n - 1 - i) * tread;
				const y1 = y0 + tread;
				const z1 = rise * (i + 1);
				drawBox(ctx, cam, STEPS.x0, STEPS.x1, y0, y1, 0, z1, {
					front: "#0c0d13",
					top: "#1a1b24",
					side: "#08090d",
				});
				fill(
					ctx,
					cam,
					[
						{ x: STEPS.x0, y: y1, z: z1 - 0.12 },
						{ x: STEPS.x1, y: y1, z: z1 - 0.12 },
						{ x: STEPS.x1, y: y1, z: z1 },
						{ x: STEPS.x0, y: y1, z: z1 },
					],
					paint.accent,
				);
			}
		},
		items: (ctx, cam, t) => {
			const items: { depth: number; draw: () => void }[] = [];
			const near = (p: Pt) => depthOf(cam, { x: p.x, y: p.y, z: 0 }) < NEAREST;
			for (const c of TABLES) {
				if (near(c)) {
					continue;
				}
				items.push({
					depth: depthOf(cam, { x: c.x, y: c.y, z: 3 }),
					draw: () => {
						drawTable(ctx, cam, c);
					},
				});
			}
			// The chairs that the pick and his family get up out of.
			for (const c of room.families.flatMap((f) => f.chairs)) {
				if (near(c)) {
					continue;
				}
				const back = {
					x: c.x - Math.cos(c.yaw) * 0.72,
					y: c.y - Math.sin(c.yaw) * 0.72,
				};
				for (const [part, p, nudge] of [
					["seat", c, 0.05],
					["back", back, 0],
				] as const) {
					items.push({
						depth: depthOf(cam, { x: p.x, y: p.y, z: 3 }) + nudge,
						draw: () => {
							drawChair(ctx, cam, c, 1.55, "#2a2b31", part);
						},
					});
				}
			}
			// The podium.
			const pod = {
				x0: PODIUM.x - PODIUM.w / 2,
				x1: PODIUM.x + PODIUM.w / 2,
				y0: PODIUM.y - PODIUM.d / 2,
				y1: PODIUM.y + PODIUM.d / 2,
			};
			items.push({
				depth: depthOf(cam, { x: PODIUM.x, y: pod.y1, z: 3 }),
				draw: () => {
					drawBox(
						ctx,
						cam,
						pod.x0,
						pod.x1,
						pod.y0,
						pod.y1,
						STAGE.z,
						STAGE.z + PODIUM.h,
						{ front: "#211d1c", top: "#2e2927", side: "#120f0e" },
						{ img: paint.podium, cols: 2, rows: 2 },
					);
				},
			});
			// The people round the tables: sat, clapping, his family up.
			const smoothing = ctx.imageSmoothingEnabled;
			const upNow = room.upAt(t);
			const clapNow = room.clapAt(t);
			for (const gu of paint.guests) {
				if (near(gu.at)) {
					continue;
				}
				const p = project(cam, { x: gu.at.x, y: gu.at.y, z: 0 });
				const w = CELL_W * p.k;
				const h = CELL_H * p.k;
				if (
					p.x + w < 0 ||
					p.x - w > cam.viewW ||
					p.y < 0 ||
					p.y - h > cam.viewH
				) {
					continue;
				}
				const stood = gu.his && upNow;
				const img = stood
					? Math.floor(t / 280 + gu.rise * 7) % 2 === 1
						? paint.wave
						: paint.up
					: clapNow &&
						  gu.rise < 0.6 &&
						  Math.floor(t / 150 + gu.rise * 5) % 2 === 1
						? paint.clap
						: paint.sat;
				items.push({
					depth: depthOf(cam, { x: gu.at.x, y: gu.at.y, z: 3 }),
					draw: () => {
						ctx.imageSmoothingEnabled = true;
						ctx.drawImage(
							img,
							(gu.cell % COLS_N) * (cw + GAP),
							Math.floor(gu.cell / COLS_N) * (ch + GAP),
							cw,
							ch,
							p.x - w / 2,
							p.y - h,
							w,
							h,
						);
						ctx.imageSmoothingEnabled = smoothing;
					},
				});
			}
			return items;
		},
		over: (ctx, cam, t) => {
			drawBeams(ctx, cam, t);
			for (const j of JETS) {
				drawJet(ctx, cam, j, t - room.jetFrom);
			}
		},
	};
};
