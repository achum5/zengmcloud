import { type ArenaAd, paintAd } from "./arenaAds.ts";
import type { ArenaTeam, BoardScreen } from "./arena.ts";
import { depthOf, project, type Camera } from "./camera.ts";
import { shade } from "./figure.ts";
import { COURT_H, COURT_W, type Pt3 } from "./geometry.ts";
import { drawTexturedPlane, type TexturedPlane } from "./planes.ts";

// THE BOARD OVER CENTER COURT, the way a building hangs one: big video
// screens on all four sides, a ring of LED round the top carrying the score
// and the clock, a thinner one round the bottom showing what the ribbon
// boards show, the dark housing over it all, the speakers hung at its
// corners and the cables up into the roof. Each building has its own (see
// ArenaSpec.jumbotron): a cube with its corners cut off, a big oval halo
// over the screens, or a long low one with its screens bowed out.

export type JumboStyle = "cube" | "halo" | "wide";
export const JUMBO_STYLES: JumboStyle[] = ["cube", "halo", "wide"];

// What the board shows of the game: the score [road, home], the clock (or
// HALF, END Q1, FINAL), the period ("Q3").
export type JumboLive = {
	score?: [number, number];
	clock: string;
	period: string;
};

type Pt2 = { x: number; y: number };

// What a face shows: a slice (u0 to u1, across) of one of the board's
// pictures - or it is the housing, a color of its own.
type PicKind =
	| "mainLong"
	| "mainEnd"
	| "barLong"
	| "barEnd"
	| "cornerMain"
	| "cornerBar"
	| "ribbon";
// (`cut`: where one picture ends and the next begins, at its left and
// right edges - a seam there; across one picture, none.)
type Skin =
	| { pic: PicKind; u0: number; u1: number; cut: [boolean, boolean] }
	| { color: string };

// One level of the board: its outline seen from above, the corners in
// reading order - starting at the left end of the side facing the main
// camera and going round toward the right, so that each side's picture
// reads left to right from in front of it - from z0 up to z1, and what each
// side shows.
type Tier = {
	foot: Pt2[];
	z0: number;
	z1: number;
	sides: Skin[];
	top: string;
	bottom: string;
	// (The bottom of the board: a screen of its own, lit, under it.)
	belly?: true;
};

export type JumboShape = {
	style: JumboStyle;
	tiers: Tier[];
	// Where the speakers hang, and the cables go up from.
	speakers: Pt2[];
	cables: Pt2[];
	// The sizes of its screens (feet across): the long sides and the ends,
	// the corners', the ring's long sides and ends; and how tall the screens
	// and the ring.
	longW: number;
	endW: number;
	cornerW: number;
	barLongW: number;
	barEndW: number;
	mainH: number;
	barH: number;
	// The whole of it, for knowing if it is in the picture.
	x0: number;
	x1: number;
	y0: number;
	y1: number;
	z0: number;
	z1: number;
};

const CX = COURT_W / 2;
const CY = COURT_H / 2;
const HOUSING = "#17181d";
const HOUSING_TOP = "#24252b";

// A box w across (x) and d deep (y), its corners cut off by c, about center
// court - in reading order (see Tier).
const boxFoot = (w: number, d: number, c: number): Pt2[] => {
	const x0 = CX - w / 2;
	const x1 = CX + w / 2;
	const y0 = CY - d / 2;
	const y1 = CY + d / 2;
	if (c <= 0) {
		return [
			{ x: x0, y: y1 },
			{ x: x1, y: y1 },
			{ x: x1, y: y0 },
			{ x: x0, y: y0 },
		];
	}
	return [
		{ x: x0 + c, y: y1 },
		{ x: x1 - c, y: y1 },
		{ x: x1, y: y1 - c },
		{ x: x1, y: y0 + c },
		{ x: x1 - c, y: y0 },
		{ x: x0 + c, y: y0 },
		{ x: x0, y: y0 + c },
		{ x: x0, y: y1 - c },
	];
};

// A box with its corners rounded off (radius r, `steps` faces a corner),
// the long sides and the ends straight: a halo seen from above.
const roundFoot = (w: number, d: number, r: number, steps: number): Pt2[] => {
	const out: Pt2[] = [];
	// Round the corners in reading order, each from where the side before it
	// ends: front right, back right, back left, front left.
	const corners: [number, number, number][] = [
		[CX + w / 2 - r, CY + d / 2 - r, Math.PI / 2],
		[CX + w / 2 - r, CY - d / 2 + r, 0],
		[CX - w / 2 + r, CY - d / 2 + r, -Math.PI / 2],
		[CX - w / 2 + r, CY + d / 2 - r, -Math.PI],
	];
	for (const [cx, cy, a0] of corners) {
		for (let i = 0; i <= steps; i++) {
			const a = a0 - (i / steps) * (Math.PI / 2);
			out.push({ x: cx + r * Math.cos(a), y: cy + r * Math.sin(a) });
		}
	}
	// (Starting at the front's left end.)
	const last = out.pop()!;
	return [last, ...out];
};

// A long low box whose long sides bow out by `bow` at their middle, each
// in `segs` faces: the ends straight.
const bowFoot = (w: number, d: number, bow: number, segs: number): Pt2[] => {
	const x0 = CX - w / 2;
	const out: Pt2[] = [];
	const at = (i: number) => {
		const u = i / segs;
		return { x: x0 + u * w, b: bow * Math.sin(u * Math.PI) };
	};
	for (let i = 0; i <= segs; i++) {
		const { x, b } = at(i);
		out.push({ x, y: CY + d / 2 + b });
	}
	for (let i = segs; i >= 0; i--) {
		const { x, b } = at(i);
		out.push({ x, y: CY - d / 2 - b });
	}
	return out;
};

const len = (a: Pt2, b: Pt2) => Math.hypot(b.x - a.x, b.y - a.y);

// The sides of an outline, each a slice of the picture for the run of sides
// it belongs to: the long sides (front and back), the ends, and the corners
// between - by how each side faces.
const skinsFor = (
	foot: Pt2[],
	long: PicKind,
	end: PicKind,
	corner: PicKind | string,
): Skin[] => {
	const n = foot.length;
	const facing = (i: number) => {
		const a = foot[i]!;
		const b = foot[(i + 1) % n]!;
		const dx = b.x - a.x;
		const dy = b.y - a.y;
		const l = Math.hypot(dx, dy) || 1;
		// Outward: square to the way it reads (see Tier).
		const nx = -dy / l;
		const ny = dx / l;
		return Math.abs(ny) > 0.985
			? "long"
			: Math.abs(nx) > 0.985
				? "end"
				: "corner";
	};
	const kinds = foot.map((_, i) => facing(i));
	// Each run of sides facing the same way shares one picture, a slice each
	// by its length.
	const out: Skin[] = [];
	for (let i = 0; i < n;) {
		let j = i;
		while (j + 1 < n && kinds[j + 1] === kinds[i]) {
			j++;
		}
		const kind = kinds[i]!;
		const pic: PicKind | string =
			kind === "long" ? long : kind === "end" ? end : corner;
		let total = 0;
		for (let k = i; k <= j; k++) {
			total += len(foot[k]!, foot[(k + 1) % n]!);
		}
		let run = 0;
		for (let k = i; k <= j; k++) {
			const l = len(foot[k]!, foot[(k + 1) % n]!);
			if (pic.startsWith("#")) {
				out.push({ color: pic });
			} else {
				out.push({
					pic: pic as PicKind,
					u0: run / total,
					u1: (run + l) / total,
					cut: [k === i, k === j],
				});
			}
			run += l;
		}
		i = j + 1;
	}
	return out;
};

// The lower ring: one run of the ribbon boards' picture all the way round.
const ribbonSkins = (foot: Pt2[], ribbonFt: number): Skin[] => {
	const n = foot.length;
	let run = 0;
	return foot.map((a, i) => {
		const l = len(a, foot[(i + 1) % n]!);
		const s = {
			pic: "ribbon" as const,
			u0: run / ribbonFt,
			u1: (run + l) / ribbonFt,
			cut: [false, false] as [boolean, boolean],
		};
		run += l;
		return s;
	});
};

// The length of the ribbon boards' picture, in feet (see RIBBON in arena.ts).
const RIBBON_FT = COURT_W + 80;

// How much lower than drawn below each hangs: low enough that its ring of
// score clears the line of play written over the top of the picture on a
// phone.
const DROP = 2.5;
export const jumboShape = (style: JumboStyle): JumboShape => {
	const s = shapeOf(style);
	return {
		...s,
		tiers: s.tiers.map((t) => ({ ...t, z0: t.z0 - DROP, z1: t.z1 - DROP })),
		z0: s.z0 - DROP,
		z1: s.z1 - DROP,
	};
};
const shapeOf = (style: JumboStyle): JumboShape => {
	if (style === "halo") {
		const main = boxFoot(30, 21, 0.9);
		const halo = roundFoot(41, 31, 7.5, 3);
		const trim = boxFoot(27.5, 18.5, 0);
		const crown = boxFoot(24, 16, 0);
		return {
			style,
			tiers: [
				{
					foot: trim,
					z0: 27.4,
					z1: 28.6,
					sides: ribbonSkins(trim, RIBBON_FT),
					top: HOUSING,
					bottom: HOUSING,
					belly: true,
				},
				{
					foot: main,
					z0: 28.6,
					z1: 44.4,
					sides: skinsFor(main, "mainLong", "mainEnd", HOUSING),
					top: HOUSING,
					bottom: HOUSING,
				},
				{
					foot: halo,
					z0: 44.4,
					z1: 49.6,
					sides: skinsFor(halo, "barLong", "barEnd", "cornerBar"),
					top: HOUSING_TOP,
					bottom: HOUSING,
				},
				{
					foot: crown,
					z0: 49.6,
					z1: 51.4,
					sides: crown.map(() => ({ color: HOUSING })),
					top: HOUSING_TOP,
					bottom: HOUSING,
				},
			],
			speakers: [
				{ x: CX - 17, y: CY + 12.5 },
				{ x: CX + 17, y: CY + 12.5 },
				{ x: CX + 17, y: CY - 12.5 },
				{ x: CX - 17, y: CY - 12.5 },
			],
			cables: crown,
			longW: 28.2,
			endW: 19.2,
			cornerW: 0,
			barLongW: 26,
			barEndW: 16,
			mainH: 44.4 - 28.6,
			barH: 49.6 - 44.4,
			x0: CX - 20.5,
			x1: CX + 20.5,
			y0: CY - 15.5,
			y1: CY + 15.5,
			z0: 27.4,
			z1: 51.4,
		};
	}
	if (style === "wide") {
		const main = bowFoot(42, 14, 1.8, 6);
		const bar = bowFoot(42.6, 14.6, 1.8, 6);
		const lower = bowFoot(39, 11.5, 1.5, 6);
		const crown = boxFoot(30, 8, 0);
		return {
			style,
			tiers: [
				{
					foot: lower,
					z0: 27.6,
					z1: 29.6,
					sides: ribbonSkins(lower, RIBBON_FT),
					top: HOUSING,
					bottom: HOUSING,
					belly: true,
				},
				{
					foot: main,
					z0: 29.6,
					z1: 45,
					sides: skinsFor(main, "mainLong", "mainEnd", HOUSING),
					top: HOUSING,
					bottom: HOUSING,
				},
				{
					foot: bar,
					z0: 45,
					z1: 48.8,
					sides: skinsFor(bar, "barLong", "barEnd", HOUSING),
					top: HOUSING_TOP,
					bottom: HOUSING,
				},
				{
					foot: crown,
					z0: 48.8,
					z1: 50.6,
					sides: crown.map(() => ({ color: HOUSING })),
					top: HOUSING_TOP,
					bottom: HOUSING,
				},
			],
			speakers: [
				{ x: CX - 23.5, y: CY + 6 },
				{ x: CX + 23.5, y: CY + 6 },
				{ x: CX + 23.5, y: CY - 6 },
				{ x: CX - 23.5, y: CY - 6 },
			],
			cables: crown,
			longW: 42.2,
			endW: 14,
			cornerW: 0,
			barLongW: 42.8,
			barEndW: 14.6,
			mainH: 45 - 29.6,
			barH: 48.8 - 45,
			x0: CX - 23.5,
			x1: CX + 23.5,
			y0: CY - 9.5,
			y1: CY + 9.5,
			z0: 27.6,
			z1: 50.6,
		};
	}
	const W = 30;
	const D = 24;
	const C = 3.4;
	const main = boxFoot(W, D, C);
	const bar = boxFoot(W + 0.5, D + 0.5, C);
	const lower = boxFoot(W - 2.6, D - 2.6, C - 1);
	const crown = boxFoot(W - 8, D - 8, 2);
	return {
		style,
		tiers: [
			{
				foot: lower,
				z0: 26.8,
				z1: 29,
				sides: ribbonSkins(lower, RIBBON_FT),
				top: HOUSING,
				bottom: HOUSING,
				belly: true,
			},
			{
				foot: main,
				z0: 29,
				z1: 45,
				sides: skinsFor(main, "mainLong", "mainEnd", "cornerMain"),
				top: HOUSING,
				bottom: HOUSING,
			},
			{
				foot: bar,
				z0: 45,
				z1: 49.2,
				sides: skinsFor(bar, "barLong", "barEnd", "cornerBar"),
				top: HOUSING_TOP,
				bottom: HOUSING,
			},
			{
				foot: crown,
				z0: 49.2,
				z1: 51.2,
				sides: crown.map(() => ({ color: HOUSING })),
				top: HOUSING_TOP,
				bottom: HOUSING,
			},
		],
		speakers: [
			{ x: CX - W / 2 - 1.2, y: CY + D / 2 + 1.2 },
			{ x: CX + W / 2 + 1.2, y: CY + D / 2 + 1.2 },
			{ x: CX + W / 2 + 1.2, y: CY - D / 2 - 1.2 },
			{ x: CX - W / 2 - 1.2, y: CY - D / 2 - 1.2 },
		],
		cables: crown,
		longW: W - 2 * C,
		endW: D - 2 * C,
		cornerW: C * Math.SQRT2,
		barLongW: W + 0.5 - 2 * C,
		barEndW: D + 0.5 - 2 * C,
		mainH: 16,
		barH: 4.2,
		x0: CX - W / 2 - 2,
		x1: CX + W / 2 + 2,
		y0: CY - D / 2 - 2,
		y1: CY + D / 2 + 2,
		z0: 26.8,
		z1: 51.2,
	};
};

// ---- the pictures -----------------------------------------------------------

// Pixels to a foot: the screens, and the rings (finer, for the score).
const MAIN_PX = 11;
const BAR_PX = 15;

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
const inkOn = (bg: string) => (luminance(bg) > 0.62 ? "#111216" : "#ffffff");

// (`vivid`: the color that shows up on a dark screen - the first, unless it
// is near black itself.)
type Colors = { main: string; trim: string; accent: string; vivid: string };
const colorsOf = (team: ArenaTeam | undefined, fallback: string): Colors => {
	const main = team?.colors?.[0] || fallback;
	const trim = team?.colors?.[1] || "#f2c14e";
	// A second color too close to black - or to the first - reads as
	// nothing beside it on a dark board.
	const accent =
		luminance(trim) < 0.16 || Math.abs(luminance(trim) - luminance(main)) < 0.22
			? luminance(main) > 0.7
				? "#111216"
				: "#f4f4f4"
			: trim;
	const third = team?.colors?.[2];
	const vivid =
		luminance(main) >= 0.12
			? main
			: luminance(trim) >= 0.12
				? trim
				: third && luminance(third) >= 0.12
					? third
					: "#d9dce1";
	return { main, trim, accent, vivid };
};

const canvasOf = (w: number, h: number) => {
	const c = document.createElement("canvas");
	c.width = Math.max(1, Math.round(w));
	c.height = Math.max(1, Math.round(h));
	return c;
};

// Board lettering: heavy and a little narrow, as a scoreboard's is - as big
// as `size` px, made smaller to fit `maxW`.
const FACE = "Arial, 'Helvetica Neue', sans-serif";
const SQUEEZE = 0.86;
export const fitText = (
	ctx: CanvasRenderingContext2D,
	text: string,
	x: number,
	y: number,
	size: number,
	maxW: number,
	color: string,
	opts: { italic?: boolean; align?: "left" | "center" | "right" } = {},
) => {
	if (!text) {
		return;
	}
	let s = size;
	const font = () =>
		`${opts.italic ? "italic " : ""}900 ${Math.max(4, Math.round(s))}px ${FACE}`;
	ctx.font = font();
	const w = ctx.measureText(text).width * SQUEEZE;
	if (w > maxW && w > 0) {
		s *= maxW / w;
		ctx.font = font();
	}
	ctx.save();
	ctx.fillStyle = color;
	ctx.textAlign = opts.align ?? "center";
	ctx.textBaseline = "middle";
	ctx.translate(x, y);
	ctx.scale(SQUEEZE, 1);
	ctx.fillText(text, 0, 0);
	ctx.restore();
};

// A logo fitted into a box, centered.
const drawLogo = (
	ctx: CanvasRenderingContext2D,
	logo: HTMLCanvasElement,
	cx: number,
	cy: number,
	box: number,
) => {
	const k = box / Math.max(logo.width, logo.height);
	const w = logo.width * k;
	const h = logo.height * k;
	ctx.drawImage(logo, cx - w / 2, cy - h / 2, w, h);
};

// Streaks across a screen in the team's colors, the motion graphics a
// building's boards run behind the team's logo.
const streaks = (
	ctx: CanvasRenderingContext2D,
	w: number,
	h: number,
	c: Colors,
	seed: number,
) => {
	const g = ctx.createLinearGradient(0, 0, w, h);
	g.addColorStop(0, shade(c.main, 0.18));
	g.addColorStop(0.55, c.main);
	g.addColorStop(1, shade(c.main, -0.45));
	ctx.fillStyle = g;
	ctx.fillRect(0, 0, w, h);
	ctx.save();
	ctx.transform(1, 0, -0.55, 1, 0, 0);
	for (let i = 0; i < 9; i++) {
		const y = ((i * 0.137 + seed * 0.31) % 1) * h;
		const bh = h * (0.025 + ((i * 7 + seed) % 5) * 0.012);
		const x = ((i * 0.29 + seed * 0.17) % 1) * w * 0.8;
		const bw = w * (0.35 + ((i * 3) % 4) * 0.12);
		ctx.globalAlpha = 0.16 + ((i * 5) % 3) * 0.1;
		ctx.fillStyle =
			i % 3 === 0 ? c.accent : i % 3 === 1 ? "#ffffff" : shade(c.main, -0.5);
		ctx.fillRect(x + y * 0.55, y, bw, bh);
	}
	ctx.restore();
	ctx.globalAlpha = 1;
};

// A burst of rays from the middle, for a big play.
const burst = (
	ctx: CanvasRenderingContext2D,
	w: number,
	h: number,
	c: Colors,
) => {
	ctx.fillStyle = shade(c.vivid, -0.35);
	ctx.fillRect(0, 0, w, h);
	const cx = w / 2;
	const cy = h / 2;
	const r = Math.hypot(w, h);
	const n = 18;
	ctx.fillStyle = c.vivid;
	for (let i = 0; i < n; i += 2) {
		const a0 = (i / n) * Math.PI * 2;
		const a1 = ((i + 1) / n) * Math.PI * 2;
		ctx.beginPath();
		ctx.moveTo(cx, cy);
		ctx.lineTo(cx + r * Math.cos(a0), cy + r * Math.sin(a0));
		ctx.lineTo(cx + r * Math.cos(a1), cy + r * Math.sin(a1));
		ctx.closePath();
		ctx.fill();
	}
	const g = ctx.createRadialGradient(cx, cy, 0, cx, cy, Math.max(w, h) * 0.6);
	g.addColorStop(0, "rgba(255,255,255,0.35)");
	g.addColorStop(1, "rgba(0,0,0,0.35)");
	ctx.fillStyle = g;
	ctx.fillRect(0, 0, w, h);
};

export type JumboTeams = {
	home: ArenaTeam | undefined;
	away: ArenaTeam | undefined;
	// [road, home], once loaded.
	logos: [HTMLCanvasElement?, HTMLCanvasElement?];
	ads: ArenaAd[];
};

// THE SCORE ROUND THE RING: each side's name in its colors with its logo,
// its score beside it on black - the road team on the left - and the period
// and the clock between them.
export const paintScoreBar = (
	ctx: CanvasRenderingContext2D,
	w: number,
	h: number,
	teams: Pick<JumboTeams, "home" | "away" | "logos">,
	live: JumboLive | undefined,
) => {
	ctx.fillStyle = "#050608";
	ctx.fillRect(0, 0, w, h);
	const home = colorsOf(teams.home, "#8c1d40");
	const away = colorsOf(teams.away, "#1d3461");
	// The middle, the scores, and each side's name: shares of the width.
	const mid = Math.min(w * 0.22, h * 2.6);
	const score = Math.min(w * 0.13, h * 1.55);
	const side = (w - mid - 2 * score) / 2;
	const pad = Math.max(1, Math.round(h * 0.07));
	const inner = h - 2 * pad;
	([0, 1] as const).forEach((i) => {
		const team = i === 0 ? teams.away : teams.home;
		const c = i === 0 ? away : home;
		const x = i === 0 ? 0 : w - side;
		const g = ctx.createLinearGradient(0, pad, 0, h - pad);
		g.addColorStop(0, shade(c.main, 0.14));
		g.addColorStop(1, shade(c.main, -0.22));
		ctx.fillStyle = g;
		ctx.fillRect(x, pad, side, inner);
		// A stripe of its second color along the inside edge.
		ctx.fillStyle = c.accent;
		const stripe = Math.max(1, Math.round(side * 0.025));
		ctx.fillRect(i === 0 ? x + side - stripe : x, pad, stripe, inner);
		const logo = teams.logos[i];
		const name = (team?.abbrev ?? (i === 0 ? "AWAY" : "HOME")).toUpperCase();
		const room = side - stripe;
		if (logo && room > inner * 2.4) {
			const lx = i === 0 ? x + inner * 0.62 : x + side - inner * 0.62;
			drawLogo(ctx, logo, lx, h / 2, inner * 0.9);
			const tx = i === 0 ? x + inner * 1.25 : x + stripe;
			fitText(
				ctx,
				name,
				tx + (room - inner * 1.25) / 2,
				h / 2 + h * 0.03,
				inner * 0.78,
				(room - inner * 1.25) * 0.88,
				inkOn(c.main),
			);
		} else {
			fitText(
				ctx,
				name,
				(i === 0 ? x : x + stripe) + room / 2,
				h / 2 + h * 0.03,
				inner * 0.78,
				room * 0.86,
				inkOn(c.main),
			);
		}
		// The score, on black.
		const sx = i === 0 ? side : w - side - score;
		ctx.fillStyle = "#000000";
		ctx.fillRect(sx, pad, score, inner);
		const pts = live?.score?.[i];
		fitText(
			ctx,
			pts === undefined ? "" : String(pts),
			sx + score / 2,
			h / 2 + h * 0.04,
			inner * 0.98,
			score * 0.9,
			"#ffffff",
		);
	});
	// The middle: the period over the clock.
	const mx = side + score;
	ctx.fillStyle = "#101217";
	ctx.fillRect(mx + pad, pad, mid - 2 * pad, inner);
	const clock = live?.clock ?? "";
	const period = live?.period ?? "";
	const word = !/\d/.test(clock) || /^(END|FINAL|HALF)/.test(clock);
	if (period && !word) {
		fitText(
			ctx,
			period,
			mx + mid / 2,
			pad + inner * 0.24,
			inner * 0.38,
			mid * 0.8,
			"#ffc531",
		);
		fitText(
			ctx,
			clock,
			mx + mid / 2,
			pad + inner * 0.68,
			inner * 0.62,
			mid * 0.86,
			"#ffffff",
		);
	} else {
		fitText(
			ctx,
			clock,
			mx + mid / 2,
			h / 2 + h * 0.03,
			inner * 0.62,
			mid * 0.86,
			"#ffc531",
		);
	}
	// The ring's lit edges, in the home team's trim.
	ctx.fillStyle = home.accent;
	ctx.fillRect(0, 0, w, Math.max(1, pad * 0.6));
	ctx.fillRect(0, h - Math.max(1, pad * 0.6), w, Math.max(1, pad * 0.6));
};

// The ring between the scores, at the corners: the home team's color.
const paintCornerBar = (w: number, h: number, home: Colors) => {
	const c = canvasOf(w, h);
	const ctx = c.getContext("2d")!;
	const g = ctx.createLinearGradient(0, 0, 0, h);
	g.addColorStop(0, shade(home.main, 0.12));
	g.addColorStop(1, shade(home.main, -0.3));
	ctx.fillStyle = g;
	ctx.fillRect(0, 0, w, h);
	ctx.fillStyle = home.accent;
	ctx.fillRect(0, h * 0.42, w, Math.max(1, h * 0.12));
	ctx.fillRect(0, 0, w, Math.max(1, h * 0.05));
	ctx.fillRect(0, h - Math.max(1, h * 0.05), w, Math.max(1, h * 0.05));
	return c;
};

// The screens on the corners between the big ones: the logo at the top
// and the team's name down them, a letter over the next, the way the old
// boards spelled their sponsor down theirs.
const paintCornerMain = (
	w: number,
	h: number,
	home: Colors,
	team: ArenaTeam | undefined,
	logo: HTMLCanvasElement | undefined,
) => {
	const c = canvasOf(w, h);
	const ctx = c.getContext("2d")!;
	const g = ctx.createLinearGradient(0, 0, 0, h);
	g.addColorStop(0, shade(home.main, -0.2));
	g.addColorStop(1, shade(home.main, -0.55));
	ctx.fillStyle = g;
	ctx.fillRect(0, 0, w, h);
	let top = w * 0.2;
	if (logo) {
		drawLogo(ctx, logo, w / 2, w * 0.62, w * 0.86);
		top = w * 1.2;
	}
	const word = (team?.abbrev || team?.name || "").toUpperCase().slice(0, 4);
	const room = h - top - w * 0.15;
	const step = room / Math.max(1, word.length);
	[...word].forEach((ch, i) => {
		fitText(
			ctx,
			ch,
			w / 2,
			top + step * (i + 0.5),
			Math.min(step * 0.86, w * 0.92),
			w * 0.84,
			home.accent,
		);
	});
	return c;
};

// THE BIG SCREENS. Over a break, the score - each team's logo over its
// score, the clock between - or the home team's logo on its colors; on the
// building's cues (see boardAt) what the other boards shout; and its
// sponsors in turn with them.
const paintMain = (
	w: number,
	h: number,
	screen: BoardScreen,
	alt: boolean,
	teams: JumboTeams,
	live: JumboLive | undefined,
): HTMLCanvasElement => {
	const c = canvasOf(w, h);
	const ctx = c.getContext("2d")!;
	const home = colorsOf(teams.home, "#8c1d40");
	const away = colorsOf(teams.away, "#1d3461");
	const logo = teams.logos[1];
	const name = (teams.home?.name || teams.home?.abbrev || "").toUpperCase();
	const region = (teams.home?.region || "").toUpperCase();
	const ad = /^ad(\d)$/.exec(screen);
	if (ad) {
		const a = teams.ads[Number(ad[1])];
		if (a) {
			paintAd(ctx, 0, 0, w, h, a);
			return c;
		}
		screen = "name";
		alt = true;
	}
	switch (screen) {
		case "name": {
			if (alt || !live?.score) {
				streaks(ctx, w, h, home, 3);
				if (logo) {
					drawLogo(ctx, logo, w / 2, h * 0.44, h * 0.66);
					fitText(
						ctx,
						name,
						w / 2,
						h * 0.87,
						h * 0.15,
						w * 0.8,
						inkOn(home.main),
					);
				} else {
					if (region) {
						fitText(
							ctx,
							region,
							w / 2,
							h * 0.3,
							h * 0.17,
							w * 0.8,
							home.accent,
						);
					}
					fitText(
						ctx,
						name,
						w / 2,
						h * 0.58,
						h * 0.32,
						w * 0.88,
						inkOn(home.main),
					);
				}
				break;
			}
			paintScoreboard(ctx, w, h, teams, live, home, away);
			break;
		}
		case "letsGo": {
			streaks(ctx, w, h, home, 5);
			if (logo) {
				drawLogo(ctx, logo, w * 0.5, h * 0.25, h * 0.32);
			}
			fitText(
				ctx,
				"LET'S GO",
				w / 2,
				h * (logo ? 0.55 : 0.33),
				h * 0.2,
				w * 0.8,
				home.accent,
				{ italic: true },
			);
			fitText(
				ctx,
				name,
				w / 2,
				h * (logo ? 0.79 : 0.65),
				h * 0.28,
				w * 0.9,
				inkOn(home.main),
				{ italic: true },
			);
			break;
		}
		case "noise": {
			ctx.fillStyle = "#07080c";
			ctx.fillRect(0, 0, w, h);
			// The level meter, rising across the screen.
			const bars = 16;
			const bw = w / bars;
			for (let i = 0; i < bars; i++) {
				const lvl = 0.25 + 0.7 * Math.pow(i / (bars - 1), 1.3);
				const cells = Math.round(lvl * 10);
				for (let k = 0; k < cells; k++) {
					ctx.fillStyle = k > 7 ? "#ff3b30" : k > 5 ? home.accent : home.vivid;
					ctx.fillRect(
						i * bw + bw * 0.12,
						h - (k + 1) * (h / 11),
						bw * 0.76,
						h / 11 - 2,
					);
				}
			}
			ctx.fillStyle = "rgba(0,0,0,0.45)";
			ctx.fillRect(0, h * 0.14, w, h * 0.5);
			fitText(ctx, "MAKE SOME", w / 2, h * 0.27, h * 0.17, w * 0.8, "#ffffff", {
				italic: true,
			});
			fitText(ctx, "NOISE!", w / 2, h * 0.5, h * 0.3, w * 0.86, home.accent, {
				italic: true,
			});
			break;
		}
		case "defense":
		case "defense2": {
			const lit = screen === "defense";
			const bg = lit ? home.vivid : "#07080c";
			ctx.fillStyle = bg;
			ctx.fillRect(0, 0, w, h);
			// The fence along the bottom.
			ctx.fillStyle = lit ? inkOn(home.vivid) : home.accent;
			const posts = 11;
			const pw = w / posts;
			for (let i = 0; i < posts; i++) {
				const x = i * pw + pw * 0.3;
				ctx.beginPath();
				ctx.moveTo(x, h * 0.95);
				ctx.lineTo(x, h * 0.74);
				ctx.lineTo(x + pw * 0.2, h * 0.68);
				ctx.lineTo(x + pw * 0.4, h * 0.74);
				ctx.lineTo(x + pw * 0.4, h * 0.95);
				ctx.fill();
			}
			ctx.fillRect(0, h * 0.78, w, h * 0.04);
			ctx.fillRect(0, h * 0.88, w, h * 0.04);
			fitText(
				ctx,
				"DEFENSE",
				w / 2,
				h * 0.36,
				h * 0.42,
				w * 0.92,
				lit ? inkOn(home.vivid) : home.vivid,
				{ italic: true },
			);
			break;
		}
		case "three":
		case "dunk":
		case "andOne":
		case "block": {
			burst(ctx, w, h, home);
			const word = {
				three: "THREE!",
				dunk: "SLAM DUNK!",
				andOne: "AND ONE!",
				block: "REJECTED!",
			}[screen];
			fitText(
				ctx,
				word,
				w / 2 + h * 0.02,
				h * 0.52,
				h * 0.4,
				w * 0.92,
				"rgba(0,0,0,0.55)",
				{ italic: true },
			);
			fitText(ctx, word, w / 2, h * 0.5, h * 0.4, w * 0.92, "#ffffff", {
				italic: true,
			});
			break;
		}
	}
	return c;
};

// The score, a screen to itself.
const paintScoreboard = (
	ctx: CanvasRenderingContext2D,
	w: number,
	h: number,
	teams: JumboTeams,
	live: JumboLive,
	home: Colors,
	away: Colors,
) => {
	ctx.fillStyle = "#06070b";
	ctx.fillRect(0, 0, w, h);
	// Each half in its team's color, slanting in toward the middle.
	([0, 1] as const).forEach((i) => {
		const c = i === 0 ? away : home;
		const g = ctx.createLinearGradient(0, 0, 0, h);
		g.addColorStop(0, shade(c.main, 0.1));
		g.addColorStop(1, shade(c.main, -0.5));
		ctx.fillStyle = g;
		ctx.beginPath();
		if (i === 0) {
			ctx.moveTo(0, 0);
			ctx.lineTo(w * 0.42, 0);
			ctx.lineTo(w * 0.36, h);
			ctx.lineTo(0, h);
		} else {
			ctx.moveTo(w, 0);
			ctx.lineTo(w * 0.58, 0);
			ctx.lineTo(w * 0.64, h);
			ctx.lineTo(w, h);
		}
		ctx.fill();
		ctx.fillStyle = c.accent;
		ctx.beginPath();
		if (i === 0) {
			ctx.moveTo(w * 0.42, 0);
			ctx.lineTo(w * 0.44, 0);
			ctx.lineTo(w * 0.38, h);
			ctx.lineTo(w * 0.36, h);
		} else {
			ctx.moveTo(w * 0.58, 0);
			ctx.lineTo(w * 0.56, 0);
			ctx.lineTo(w * 0.62, h);
			ctx.lineTo(w * 0.64, h);
		}
		ctx.fill();
		const team = i === 0 ? teams.away : teams.home;
		const cx = i === 0 ? w * 0.2 : w * 0.8;
		const logo = teams.logos[i];
		if (logo) {
			drawLogo(ctx, logo, cx, h * 0.3, h * 0.4);
		} else {
			fitText(
				ctx,
				(team?.abbrev ?? "").toUpperCase(),
				cx,
				h * 0.3,
				h * 0.24,
				w * 0.3,
				inkOn(c.main),
			);
		}
		const pts = live.score?.[i];
		fitText(
			ctx,
			pts === undefined ? "" : String(pts),
			cx,
			h * 0.72,
			h * 0.44,
			w * 0.32,
			"#ffffff",
		);
	});
	// The clock, between.
	const word = !/\d/.test(live.clock) || /^(END|FINAL|HALF)/.test(live.clock);
	ctx.fillStyle = "#000000";
	ctx.fillRect(w * 0.42, h * 0.3, w * 0.16, h * 0.42);
	if (live.period && !word) {
		fitText(ctx, live.period, w / 2, h * 0.4, h * 0.12, w * 0.14, "#ffc531");
		fitText(ctx, live.clock, w / 2, h * 0.58, h * 0.17, w * 0.15, "#ffffff");
	} else {
		fitText(ctx, live.clock, w / 2, h * 0.51, h * 0.15, w * 0.15, "#ffc531");
	}
};

// ---- the board, for a game ----------------------------------------------------

export type Jumbotron = {
	shape: JumboShape;
	teams: JumboTeams;
	// The pictures made so far, by what is on them.
	made: Map<string, HTMLCanvasElement>;
};

export const makeJumbotron = (
	style: JumboStyle,
	teams: JumboTeams,
): Jumbotron => ({ shape: jumboShape(style), teams, made: new Map() });

// (Kept to so many: the score's pictures go out of date as it changes.)
const MADE_MAX = 40;
const made = (
	j: Jumbotron,
	key: string,
	paint: () => HTMLCanvasElement,
): HTMLCanvasElement => {
	let c = j.made.get(key);
	if (!c) {
		if (j.made.size >= MADE_MAX) {
			for (const k of j.made.keys()) {
				// (The ones that never change are kept.)
				if (k.startsWith("s|")) {
					j.made.delete(k);
					if (j.made.size < MADE_MAX * 0.75) {
						break;
					}
				}
			}
		}
		c = paint();
		j.made.set(key, c);
	}
	return c;
};

const liveKey = (live: JumboLive | undefined) =>
	live ? `${live.score?.join("-") ?? ""}|${live.clock}|${live.period}` : "";

// A slice (u0 to u1, across) of a picture, cut out once.
const slices = new WeakMap<HTMLCanvasElement, Map<string, HTMLCanvasElement>>();
const sliceOf = (
	img: HTMLCanvasElement,
	u0: number,
	u1: number,
): HTMLCanvasElement => {
	if (u0 <= 0 && u1 >= 1) {
		return img;
	}
	let m = slices.get(img);
	if (!m) {
		m = new Map();
		slices.set(img, m);
	}
	const key = `${u0.toFixed(4)}|${u1.toFixed(4)}`;
	let c = m.get(key);
	if (!c) {
		// (Round the end of the picture and on from its start, if it runs
		// past it.)
		const x0 = u0 * img.width;
		const w = Math.max(1, (u1 - u0) * img.width);
		c = canvasOf(w, img.height);
		const g = c.getContext("2d")!;
		const from = ((x0 % img.width) + img.width) % img.width;
		const first = Math.min(w, img.width - from);
		g.drawImage(img, from, 0, first, img.height, 0, 0, first, img.height);
		if (first < w) {
			g.drawImage(
				img,
				0,
				0,
				w - first,
				img.height,
				first,
				0,
				w - first,
				img.height,
			);
		}
		m.set(key, c);
	}
	return c;
};

const picture = (
	j: Jumbotron,
	kind: PicKind,
	screen: BoardScreen,
	alt: boolean,
	live: JumboLive | undefined,
	ribbon: HTMLCanvasElement,
): HTMLCanvasElement => {
	const s = j.shape;
	const home = colorsOf(j.teams.home, "#8c1d40");
	switch (kind) {
		case "ribbon":
			return ribbon;
		case "mainLong":
		case "mainEnd": {
			const w = (kind === "mainLong" ? s.longW : s.endW) * MAIN_PX;
			const scored = screen === "name" && !alt && !!live?.score;
			const key = `${scored ? "l" : "s"}|${kind}|${screen}|${alt}|${scored ? liveKey(live) : ""}`;
			return made(j, key, () =>
				paintMain(w, s.mainH * MAIN_PX, screen, alt, j.teams, live),
			);
		}
		case "barLong":
		case "barEnd": {
			const w = (kind === "barLong" ? s.barLongW : s.barEndW) * BAR_PX;
			return made(j, `l|${kind}|${liveKey(live)}`, () => {
				const c = canvasOf(w, s.barH * BAR_PX);
				paintScoreBar(c.getContext("2d")!, c.width, c.height, j.teams, live);
				return c;
			});
		}
		case "cornerBar":
			return made(j, "k|cornerBar", () =>
				paintCornerBar(Math.max(s.cornerW, 3) * BAR_PX, s.barH * BAR_PX, home),
			);
		case "cornerMain":
			return made(j, "k|cornerMain", () =>
				paintCornerMain(
					s.cornerW * MAIN_PX,
					s.mainH * MAIN_PX,
					home,
					j.teams.home,
					j.teams.logos[1],
				),
			);
	}
};

// ---- drawing it ---------------------------------------------------------------

// The light on the housing (from up over the camera's shoulder).
const LIGHT = (() => {
	const l = [-0.35, 0.55, 0.76];
	const n = Math.hypot(l[0]!, l[1]!, l[2]!);
	return { x: l[0]! / n, y: l[1]! / n, z: l[2]! / n };
})();
const lit = (color: string, nx: number, ny: number, nz: number) =>
	shade(color, (nx * LIGHT.x + ny * LIGHT.y + nz * LIGHT.z - 0.35) * 0.32);

const SEAM = "rgba(4, 5, 7, 0.95)";
// How strongly its screens light the dark round it.
const GLOW = 0.1;

const fillPoly = (
	ctx: CanvasRenderingContext2D,
	pts: { x: number; y: number }[],
	fill: string,
	seam: number,
) => {
	ctx.beginPath();
	pts.forEach((p, i) => (i ? ctx.lineTo(p.x, p.y) : ctx.moveTo(p.x, p.y)));
	ctx.closePath();
	ctx.fillStyle = fill;
	ctx.fill();
	ctx.strokeStyle = SEAM;
	ctx.lineWidth = seam;
	ctx.stroke();
};

// Whether a level's top (or, from under it, its bottom) is toward the
// camera: a step along the floor toward the camera, at its height, lower in
// the picture.
const seenFromAbove = (cam: Camera, z: number) => {
	const at = { x: CX, y: CY, z };
	const dx = cam.pos.x - CX;
	const dy = cam.pos.y - CY;
	const l = Math.hypot(dx, dy) || 1;
	const near = { x: CX + dx / l, y: CY + dy / l, z };
	return project(cam, near).y > project(cam, at).y;
};

// Hung from the roof, in the picture when it is: each level from the one
// furthest from the camera's height in, so what is nearer is drawn over.
// THE SCORE ON THE RIBBON round the bowl (the Garden's, in 2K): the same
// bar as the ring's, along the middle of the far side's ribbon board.
export const ribbonScore = (
	j: Jumbotron,
	w: number,
	h: number,
	live: JumboLive,
): HTMLCanvasElement =>
	made(
		j,
		`s|ribbon|${live.score?.join("-") ?? ""}|${live.clock}|${live.period}`,
		() => {
			const canvas = document.createElement("canvas");
			canvas.width = w;
			canvas.height = h;
			paintScoreBar(canvas.getContext("2d")!, w, h, j.teams, live);
			return canvas;
		},
	);

// A box round all of it in the world - the light off its screens round it,
// the cables up from it into the dark - for the part of the picture it can
// take up.
export const jumbotronBox = (j: Jumbotron): Pt3[] => {
	const s = j.shape;
	const r = s.longW * 1.15;
	const mid = (s.z0 + s.z1) / 2;
	const xs = [Math.min(s.x0, CX - r), Math.max(s.x1, CX + r)];
	const ys = [Math.min(s.y0, CY - r), Math.max(s.y1, CY + r)];
	const zs = [Math.min(s.z0, mid - r), s.z1 + 70];
	return xs.flatMap((x) => ys.flatMap((y) => zs.map((z) => ({ x, y, z }))));
};

export const drawJumbotron = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	j: Jumbotron,
	screen: BoardScreen,
	ribbon: HTMLCanvasElement,
	live: JumboLive | undefined,
	t: number,
) => {
	const s = j.shape;
	// (Drawn only when most of it is in the picture - over a break, looking
	// round the building, the ring with the score on it and all - not as a
	// strip along the top edge of the game, or a screen cut off across the
	// top of a look up at the crowd.)
	const bottom = project(cam, { x: CX, y: s.y1, z: s.z0 });
	const ring = s.tiers.find((tr) =>
		tr.sides.some((x) => "pic" in x && x.pic === "barLong"),
	);
	const score = project(cam, { x: CX, y: s.y1, z: ring?.z0 ?? s.z1 });
	if (
		bottom.y < cam.viewH * 0.3 ||
		score.y < 0 ||
		depthOf(cam, { x: CX, y: CY, z: s.z0 }) < 5
	) {
		return;
	}
	// The score's own screen and the home team's logo by turns, over a
	// break.
	const alt = Math.floor(t / 8000) % 8 >= 4;
	const k = bottom.k;
	const seam = Math.max(1, 0.16 * k);

	// The cables up from the top, into the dark.
	ctx.strokeStyle = "#2a2c33";
	ctx.lineWidth = Math.max(1, 0.1 * k);
	ctx.beginPath();
	for (const p of s.cables) {
		const a = project(cam, { x: p.x, y: p.y, z: s.z1 });
		const b = project(cam, { x: p.x * 1.02 - CX * 0.02, y: p.y, z: s.z1 + 70 });
		ctx.moveTo(a.x, a.y);
		ctx.lineTo(b.x, b.y);
	}
	ctx.stroke();

	// The light off its screens, out into the dark round it (drawn under
	// it).
	const main = s.tiers.find((tr) =>
		tr.sides.some((x) => "pic" in x && x.pic === "mainLong"),
	);
	if (main) {
		const c = project(cam, { x: CX, y: CY, z: (main.z0 + main.z1) / 2 });
		const r = Math.max(8, s.longW * 1.1 * c.k);
		const home = colorsOf(j.teams.home, "#8c1d40");
		const g = ctx.createRadialGradient(c.x, c.y, r * 0.3, c.x, c.y, r);
		g.addColorStop(0, home.vivid);
		g.addColorStop(1, "rgba(0,0,0,0)");
		ctx.save();
		ctx.globalCompositeOperation = "lighter";
		ctx.globalAlpha = GLOW;
		ctx.fillStyle = g;
		ctx.fillRect(c.x - r, c.y - r, 2 * r, 2 * r);
		ctx.restore();
	}
	const above = cam.pos.z;
	const order = [...s.tiers].sort(
		(a, b) =>
			Math.abs(above - (b.z0 + b.z1) / 2) - Math.abs(above - (a.z0 + a.z1) / 2),
	);
	// The speakers behind the board first, those in front of it after.
	const mid = depthOf(cam, { x: CX, y: CY, z: 0 });
	const speakers = s.speakers.map((p) => ({
		p,
		d: depthOf(cam, { ...p, z: 0 }),
	}));
	for (const sp of speakers) {
		if (sp.d >= mid) {
			drawSpeaker(ctx, cam, sp.p, s.z1 - 7, seam);
		}
	}
	for (const tier of order) {
		drawTier(ctx, cam, j, tier, screen, alt, live, ribbon, seam);
	}
	for (const sp of speakers) {
		if (sp.d < mid) {
			drawSpeaker(ctx, cam, sp.p, s.z1 - 7, seam);
		}
	}
};

const drawTier = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	j: Jumbotron,
	tier: Tier,
	screen: BoardScreen,
	alt: boolean,
	live: JumboLive | undefined,
	ribbon: HTMLCanvasElement,
	seam: number,
) => {
	const { foot, z0, z1 } = tier;
	const n = foot.length;
	for (let i = 0; i < n; i++) {
		const a = foot[i]!;
		const b = foot[(i + 1) % n]!;
		const tl = project(cam, { x: a.x, y: a.y, z: z1 });
		const tr = project(cam, { x: b.x, y: b.y, z: z1 });
		const bl = project(cam, { x: a.x, y: a.y, z: z0 });
		// (Toward the camera: its top-left, top-right and bottom-left go
		// round the right way in the picture.)
		const area = (tr.x - tl.x) * (bl.y - tl.y) - (tr.y - tl.y) * (bl.x - tl.x);
		if (area <= 0) {
			continue;
		}
		const br = project(cam, { x: b.x, y: b.y, z: z0 });
		const skin = tier.sides[i]!;
		if ("color" in skin) {
			const dx = b.x - a.x;
			const dy = b.y - a.y;
			const l = Math.hypot(dx, dy) || 1;
			fillPoly(
				ctx,
				[tl, tr, br, bl],
				lit(skin.color, -dy / l, dx / l, 0),
				seam,
			);
			continue;
		}
		const img = sliceOf(
			picture(j, skin.pic, screen, alt, live, ribbon),
			skin.u0,
			skin.u1,
		);
		const plane: TexturedPlane = {
			origin: { x: a.x, y: a.y, z: z1 },
			alongX: { x: (b.x - a.x) / img.width, y: (b.y - a.y) / img.width, z: 0 },
			alongY: { x: 0, y: 0, z: -(z1 - z0) / img.height },
			w: img.width,
			h: img.height,
		};
		const across = Math.abs(tr.x - tl.x);
		drawTexturedPlane(
			ctx,
			cam,
			plane,
			img,
			across > 140 ? 3 : across > 50 ? 2 : 1,
			1,
		);
		ctx.beginPath();
		ctx.moveTo(tl.x, tl.y);
		ctx.lineTo(tr.x, tr.y);
		if (skin.cut[1]) {
			ctx.lineTo(br.x, br.y);
		} else {
			ctx.moveTo(br.x, br.y);
		}
		ctx.lineTo(bl.x, bl.y);
		if (skin.cut[0]) {
			ctx.lineTo(tl.x, tl.y);
		}
		ctx.strokeStyle = SEAM;
		ctx.lineWidth = seam;
		ctx.stroke();
	}
	const up = seenFromAbove(cam, z1);
	const down = !seenFromAbove(cam, z0);
	if (up || down) {
		const z = up ? z1 : z0;
		const pts = foot.map((p) => project(cam, { x: p.x, y: p.y, z }));
		fillPoly(
			ctx,
			pts,
			lit(up ? tier.top : tier.bottom, 0, 0, up ? 1 : -1),
			seam,
		);
		if (down && tier.belly) {
			// The screen under it, the home team's color lit.
			const home = colorsOf(j.teams.home, "#8c1d40");
			const inset = foot.map((p) =>
				project(cam, { x: CX + (p.x - CX) * 0.8, y: CY + (p.y - CY) * 0.8, z }),
			);
			fillPoly(ctx, inset, shade(home.main, -0.15), seam);
		}
	}
};

// A cluster of speakers hung beside the board: a tall dark box.
const drawSpeaker = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	p: Pt2,
	z0: number,
	seam: number,
) => {
	const r = 0.55;
	const z1 = z0 + 5.5;
	const foot = [
		{ x: p.x - r, y: p.y + r },
		{ x: p.x + r, y: p.y + r },
		{ x: p.x + r, y: p.y - r },
		{ x: p.x - r, y: p.y - r },
	];
	for (let i = 0; i < 4; i++) {
		const a = foot[i]!;
		const b = foot[(i + 1) % 4]!;
		const tl = project(cam, { ...a, z: z1 });
		const tr = project(cam, { ...b, z: z1 });
		const bl = project(cam, { ...a, z: z0 });
		const area = (tr.x - tl.x) * (bl.y - tl.y) - (tr.y - tl.y) * (bl.x - tl.x);
		if (area <= 0) {
			continue;
		}
		const br = project(cam, { ...b, z: z0 });
		const dx = b.x - a.x;
		const dy = b.y - a.y;
		const l = Math.hypot(dx, dy) || 1;
		fillPoly(
			ctx,
			[tl, tr, br, bl],
			lit("#121317", -dy / l, dx / l, 0),
			seam * 0.5,
		);
	}
	const top = project(cam, { x: p.x, y: p.y, z: z1 });
	const sky = project(cam, { x: p.x, y: p.y, z: z1 + 60 });
	ctx.strokeStyle = "#2a2c33";
	ctx.lineWidth = Math.max(1, seam * 0.5);
	ctx.beginPath();
	ctx.moveTo(top.x, top.y);
	ctx.lineTo(sky.x, sky.y);
	ctx.stroke();
};
