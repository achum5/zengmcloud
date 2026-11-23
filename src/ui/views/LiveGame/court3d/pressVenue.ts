import { drawBall } from "./arena.ts";
import { depthOf, project, type Camera } from "./camera.ts";
import { kitsFor, lightness, shade, type Kit } from "./figure.ts";
import type { Pt3 } from "./geometry.ts";
import { drawTexturedPlane, type TexturedPlane } from "./planes.ts";
import {
	BACKDROP,
	CX,
	groundAt,
	PODIUM,
	RISER,
	SEAT_Y,
	TABLE,
	tableOf,
	type Hands,
	type PressSeen,
	type PressTake,
	type Seats,
} from "./pressStage.ts";
import { drawChair, type Venue } from "./scene.ts";

// THE PRESS ROOM (see pressStage.ts): a dim room, carpet, a drape along the
// back wall; standing in front of it the step-and-repeat - the team's logo
// and its name over and over, in one of a few looks - and a low riser before
// it. For the talk, a table across the riser in a skirt of the team's color
// with its logo, the chairs behind it, a microphone and a bottle of water in
// front of each of them. For the pictures, each of the players' new jerseys
// held up in his hands, its back to the cameras: his name and number on it.
//
// All of it painted once.

export type PressVenueInfo = {
	team: {
		region: string;
		name: string;
		abbrev: string;
		colors: [string, string, string];
		jersey?: string;
	};
	logo?: HTMLCanvasElement;
	take: PressTake;
	// Each one's name and number, for the back of his jersey.
	jerseys: { pid: number; lastName: string; number?: string }[];
	seats: Seats;
};

const canvas = (w: number, h: number) => {
	const c = document.createElement("canvas");
	c.width = w;
	c.height = h;
	return c;
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

// Words as big as fit across `w` px, no bigger than `size`.
const fitText = (
	g: CanvasRenderingContext2D,
	text: string,
	x: number,
	y: number,
	w: number,
	size: number,
	font = "sans-serif",
	weight = 900,
) => {
	let s = size;
	g.font = `${weight} ${s}px ${font}`;
	const m = g.measureText(text).width;
	if (m > w) {
		s = Math.max(6, Math.floor((s * w) / m));
		g.font = `${weight} ${s}px ${font}`;
	}
	g.fillText(text, x, y);
	return s;
};

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
	fitText(g, fallback, cx, cy, size * 1.2, Math.round(size * 0.5));
};

// The team's main color as a backdrop shows it - its second, if the first
// is next to black or white.
const mainOf = (colors: [string, string, string]) => {
	const [a, b] = colors;
	const l = lightness(a);
	return l < 0.08 || l > 0.9 ? b : a;
};
const accentOf = (colors: [string, string, string], main: string) => {
	const c = colors.find(
		(x) => x !== main && Math.abs(lightness(x) - lightness(main)) > 0.25,
	);
	return c ?? (lightness(main) > 0.5 ? "#111216" : "#ffffff");
};

// ---- the step-and-repeat ------------------------------------------------------

const BACKDROP_PX = 40;
// Tiles across it, feet: how wide, how tall.
const TILE_W = 3.4;
const TILE_H = 2.1;
const paintBackdrop = (info: PressVenueInfo) => {
	const { team, logo, take } = info;
	const w = Math.round((BACKDROP.x1 - BACKDROP.x0) * BACKDROP_PX);
	const h = Math.round((BACKDROP.z1 - BACKDROP.z0) * BACKDROP_PX);
	const c = canvas(w, h);
	const g = c.getContext("2d")!;
	const main = mainOf(team.colors);
	const accent = accentOf(team.colors, main);
	// The four looks: deep in its color, black, its color, white.
	const ground =
		take.backdrop === 0
			? shade(main, lightness(main) > 0.4 ? -0.62 : -0.38)
			: take.backdrop === 1
				? "#0d0e11"
				: take.backdrop === 2
					? lightness(main) > 0.8
						? shade(main, -0.25)
						: main
					: "#eceef1";
	const ink =
		take.backdrop === 3
			? lightness(main) > 0.75
				? "#14161b"
				: main
			: lightness(ground) > 0.6
				? "#14161b"
				: "#ffffff";
	g.fillStyle = ground;
	g.fillRect(0, 0, w, h);
	const tw = TILE_W * BACKDROP_PX;
	const th = TILE_H * BACKDROP_PX;
	const rows = Math.ceil(h / th) + 1;
	const cols = Math.ceil(w / tw) + 2;
	const word = team.name.toUpperCase();
	g.textAlign = "center";
	g.textBaseline = "middle";
	for (let r = 0; r < rows; r++) {
		for (let col = -1; col < cols; col++) {
			const x = (col + (r % 2 === 0 ? 0.5 : 0)) * tw;
			const y = (r + 0.5) * th - th * 0.2;
			if ((col + r) % 2 === 0) {
				drawLogo(g, logo, x, y, th * 0.72, team.abbrev, ink);
			} else {
				g.fillStyle = ink;
				fitText(g, word, x, y, tw * 0.82, Math.round(th * 0.34));
			}
		}
	}
	// A band of its color along the bottom, the team's name in it.
	const band = Math.round(BACKDROP_PX * 0.9);
	g.fillStyle = take.backdrop === 2 ? shade(main, -0.35) : main;
	g.fillRect(0, h - band, w, band);
	g.fillStyle = accent;
	g.fillRect(0, h - band, w, Math.max(2, Math.round(band * 0.08)));
	// The lights on it: brightest in the middle where they stand, falling off
	// to its ends and its top.
	const light = g.createRadialGradient(
		w / 2,
		h * 0.55,
		h * 0.2,
		w / 2,
		h * 0.55,
		w * 0.62,
	);
	light.addColorStop(0, "rgba(255, 250, 240, 0.10)");
	light.addColorStop(0.55, "rgba(0, 0, 0, 0)");
	light.addColorStop(1, "rgba(0, 0, 0, 0.42)");
	g.fillStyle = light;
	g.fillRect(0, 0, w, h);
	return c;
};

// The drape along the back wall behind it: dark folds, lit a little
// where they stand.
const paintDrape = () => {
	const w = 320;
	const h = 32;
	const c = canvas(w, h);
	const g = c.getContext("2d")!;
	for (let x = 0; x < w; x++) {
		const f = 0.5 + 0.5 * Math.sin(x * 0.55);
		const lit = Math.max(0, 1 - Math.abs(x / w - 0.5) * 2.4);
		const v = Math.round(30 + f * 16 + lit * 14);
		g.fillStyle = `rgb(${v}, ${v + 1}, ${v + 5})`;
		g.fillRect(x, 0, 1, h);
	}
	return c;
};

// ---- the table ----------------------------------------------------------------

const SKIRT_PX = 40;
const paintSkirt = (info: PressVenueInfo) => {
	const t = tableOf(info.seats);
	const w = Math.round((t.x1 - t.x0) * SKIRT_PX);
	const h = Math.round(TABLE.h * SKIRT_PX);
	const c = canvas(w, h);
	const g = c.getContext("2d")!;
	const main = mainOf(info.team.colors);
	const accent = accentOf(info.team.colors, main);
	const ground = lightness(main) > 0.7 ? shade(main, -0.3) : shade(main, -0.12);
	g.fillStyle = ground;
	g.fillRect(0, 0, w, h);
	// (Folds down it.)
	for (let x = 0; x < w; x += 14) {
		g.fillStyle = "rgba(0, 0, 0, 0.08)";
		g.fillRect(x, 0, 6, h);
	}
	g.fillStyle = accent;
	g.fillRect(0, 0, w, Math.round(h * 0.07));
	const ink = lightness(ground) > 0.6 ? "#14161b" : "#ffffff";
	drawLogo(g, info.logo, w / 2, h * 0.52, h * 0.66, info.team.abbrev, ink);
	g.fillStyle = ink;
	g.textAlign = "center";
	g.textBaseline = "middle";
	const word = info.team.name.toUpperCase();
	for (const x of [w * 0.2, w * 0.8]) {
		fitText(g, word, x, h * 0.54, w * 0.26, Math.round(h * 0.2));
	}
	// Lit from above: darker toward the floor.
	const shadeDown = g.createLinearGradient(0, 0, 0, h);
	shadeDown.addColorStop(0, "rgba(0,0,0,0)");
	shadeDown.addColorStop(1, "rgba(0,0,0,0.4)");
	g.fillStyle = shadeDown;
	g.fillRect(0, 0, w, h);
	return c;
};

// A card stood on the table in front of each of them: the team's logo.
const paintPlacard = (info: PressVenueInfo) => {
	const c = canvas(96, 40);
	const g = c.getContext("2d")!;
	g.fillStyle = "#f4f4f2";
	g.fillRect(0, 0, 96, 40);
	drawLogo(g, info.logo, 48, 20, 32, info.team.abbrev, "#14161b");
	return c;
};

// The front of the podium: the team's color, its logo up top and its name
// under it, a band of its other color along the bottom.
const PODIUM_PX = 60;
const paintPodium = (info: PressVenueInfo) => {
	const w = Math.round(PODIUM.w * PODIUM_PX);
	const h = Math.round(PODIUM.h * PODIUM_PX);
	const c = canvas(w, h);
	const g = c.getContext("2d")!;
	const main = mainOf(info.team.colors);
	const accent = accentOf(info.team.colors, main);
	const ground = lightness(main) > 0.7 ? shade(main, -0.3) : shade(main, -0.1);
	g.fillStyle = ground;
	g.fillRect(0, 0, w, h);
	// (A lip round its top, a panel set into its front.)
	g.fillStyle = "rgba(0, 0, 0, 0.28)";
	g.fillRect(0, 0, w, Math.round(h * 0.05));
	g.strokeStyle = "rgba(255, 255, 255, 0.12)";
	g.lineWidth = 2;
	g.strokeRect(w * 0.08, h * 0.09, w * 0.84, h * 0.72);
	const ink = lightness(ground) > 0.6 ? "#14161b" : "#ffffff";
	drawLogo(g, info.logo, w / 2, h * 0.33, w * 0.62, info.team.abbrev, ink);
	g.fillStyle = ink;
	g.textAlign = "center";
	g.textBaseline = "middle";
	fitText(
		g,
		info.team.name.toUpperCase(),
		w / 2,
		h * 0.66,
		w * 0.76,
		Math.round(h * 0.075),
	);
	g.fillStyle = accent;
	g.fillRect(0, Math.round(h * 0.86), w, Math.round(h * 0.05));
	const shadeDown = g.createLinearGradient(0, 0, 0, h);
	shadeDown.addColorStop(0, "rgba(255,255,255,0.06)");
	shadeDown.addColorStop(0.5, "rgba(0,0,0,0)");
	shadeDown.addColorStop(1, "rgba(0,0,0,0.42)");
	g.fillStyle = shadeDown;
	g.fillRect(0, 0, w, h);
	return c;
};

// ---- his jersey -----------------------------------------------------------------

// The jersey as it hangs (feet), and where its straps are across its top -
// where his hands have it.
export const JERSEY = { w: 1.92, h: 2.5, straps: [0.24, 0.76] as const };
const JERSEY_PX = 125;

// The back of it: its shape - the straps, the shallow scoop of the neck, the
// armholes - in its color, the trim round the neck and arms, his name across
// the top of the back and his number big under it.
export const paintJersey = (kit: Kit, lastName: string, number?: string) => {
	const w = Math.round(JERSEY.w * JERSEY_PX);
	const h = Math.round(JERSEY.h * JERSEY_PX);
	const c = canvas(w, h);
	const g = c.getContext("2d")!;
	const [sl, sr] = JERSEY.straps;
	const half = w * 0.068;
	const a = w * sl - half;
	const b = w * sl + half;
	const d = w * sr - half;
	const e = w * sr + half;
	const pit = h * 0.37;
	const shape = () => {
		g.beginPath();
		g.moveTo(a, 0);
		g.lineTo(b, 0);
		// The back of the neck: a shallow scoop.
		g.quadraticCurveTo(w / 2, h * 0.13, d, 0);
		g.lineTo(e, 0);
		// The armhole, down and out to his side.
		g.bezierCurveTo(
			e + w * 0.005,
			h * 0.17,
			w - w * 0.04,
			pit - h * 0.02,
			w - 3,
			pit,
		);
		g.lineTo(w - 1, h - 1);
		g.lineTo(1, h - 1);
		g.lineTo(3, pit);
		g.bezierCurveTo(w * 0.04, pit - h * 0.02, a - w * 0.005, h * 0.17, a, 0);
		g.closePath();
	};
	g.save();
	shape();
	g.clip();
	g.fillStyle = kit.jersey;
	g.fillRect(0, 0, w, h);
	// Hanging from his hands: folds down from where he holds it, the sides
	// falling away.
	for (const s of JERSEY.straps) {
		const fold = g.createLinearGradient(
			w * s - w * 0.09,
			0,
			w * s + w * 0.09,
			0,
		);
		fold.addColorStop(0, "rgba(0,0,0,0)");
		fold.addColorStop(0.5, "rgba(0,0,0,0.10)");
		fold.addColorStop(1, "rgba(0,0,0,0)");
		g.fillStyle = fold;
		g.fillRect(w * s - w * 0.09, 0, w * 0.18, h);
	}
	const sides = g.createLinearGradient(0, 0, w, 0);
	sides.addColorStop(0, "rgba(0,0,0,0.12)");
	sides.addColorStop(0.16, "rgba(0,0,0,0)");
	sides.addColorStop(0.84, "rgba(0,0,0,0)");
	sides.addColorStop(1, "rgba(0,0,0,0.12)");
	g.fillStyle = sides;
	g.fillRect(0, 0, w, h);
	// His name, arched a little across the top of the back.
	const ink = kit.name ?? kit.number;
	const name = lastName.toUpperCase();
	g.textAlign = "center";
	g.textBaseline = "middle";
	const nameY = h * 0.205;
	let size = Math.round(h * 0.075);
	g.font = `800 ${size}px "Arial Narrow", "Roboto Condensed", sans-serif`;
	const spread = 1.06;
	let width = g.measureText(name).width * spread;
	const most = w * 0.62;
	if (width > most) {
		size = Math.max(8, Math.floor((size * most) / width));
		g.font = `800 ${size}px "Arial Narrow", "Roboto Condensed", sans-serif`;
		width = g.measureText(name).width * spread;
	}
	const R = w * 2.2;
	let x = w / 2 - width / 2;
	g.fillStyle = ink;
	for (const ch of name) {
		const cw = g.measureText(ch).width * spread;
		const mid = x + cw / 2;
		const ang = (mid - w / 2) / R;
		g.save();
		g.translate(w / 2 + Math.sin(ang) * R, nameY + R - Math.cos(ang) * R);
		g.rotate(ang);
		g.fillText(ch, 0, 0);
		g.restore();
		x += cw;
	}
	// His number.
	if (number) {
		const ny = h * 0.475;
		let ns = Math.round(h * 0.34);
		g.font = `900 ${ns}px "Arial Black", "Arial", sans-serif`;
		const nw = g.measureText(number).width;
		if (nw > w * 0.66) {
			ns = Math.floor((ns * w * 0.66) / nw);
			g.font = `900 ${ns}px "Arial Black", "Arial", sans-serif`;
		}
		g.lineJoin = "round";
		g.lineWidth = Math.max(3, ns * 0.07);
		g.strokeStyle = kit.numberEdge;
		g.strokeText(number, w / 2, ny);
		g.fillStyle = kit.number;
		g.fillText(number, w / 2, ny);
	}
	g.restore();
	// The trim round its neck and its arms.
	g.save();
	g.strokeStyle = kit.trim;
	g.lineWidth = Math.max(3, w * 0.03);
	g.lineCap = "round";
	g.beginPath();
	g.moveTo(b, 1);
	g.quadraticCurveTo(w / 2, h * 0.13, d, 1);
	g.moveTo(e, 1);
	g.bezierCurveTo(
		e + w * 0.005,
		h * 0.17,
		w - w * 0.04,
		pit - h * 0.02,
		w - 4,
		pit,
	);
	g.moveTo(a, 1);
	g.bezierCurveTo(a - w * 0.005, h * 0.17, w * 0.04, pit - h * 0.02, 4, pit);
	g.stroke();
	g.restore();
	return c;
};

// ---- all of it ------------------------------------------------------------------

export type PressVenuePaint = {
	backdrop: HTMLCanvasElement;
	drape: HTMLCanvasElement;
	skirt: HTMLCanvasElement;
	placard: HTMLCanvasElement;
	podium: HTMLCanvasElement;
	jerseys: Map<number, HTMLCanvasElement>;
	main: string;
};

export const paintPressVenue = (info: PressVenueInfo): PressVenuePaint => {
	const [road, home] = kitsFor(info.team, info.team);
	const kit = info.take.edition === "home" ? home : road;
	return {
		backdrop: paintBackdrop(info),
		drape: paintDrape(),
		skirt: paintSkirt(info),
		placard: paintPlacard(info),
		podium: paintPodium(info),
		jerseys: new Map(
			info.jerseys.map((j) => [j.pid, paintJersey(kit, j.lastName, j.number)]),
		),
		main: mainOf(info.team.colors),
	};
};

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
// A box's faces the camera sees, its front toward the cameras.
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
	front?: HTMLCanvasElement,
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
		drawTexturedPlane(
			ctx,
			cam,
			wall(x0, x1, y1, z0, z1, front.width, front.height),
			front,
			6,
			1,
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

// A jersey held up by its shoulders, hanging straight down from his hands -
// its picture the right way round however the camera sees it.
const jerseyPlane = (cam: Camera, hands: Hands, img: HTMLCanvasElement) => {
	const pr = project(cam, hands.r);
	const pl = project(cam, hands.l);
	const [A, B] = pr.x <= pl.x ? [hands.r, hands.l] : [hands.l, hands.r];
	const dx = B.x - A.x;
	const dy = B.y - A.y;
	const dz = B.z - A.z;
	const len = Math.hypot(dx, dy, dz) || 1;
	const ux = dx / len;
	const uy = dy / len;
	const uz = dz / len;
	const [sl, sr] = JERSEY.straps;
	// (Its straps' middle where his hands' middle is, the tops of the
	// straps at his knuckles - a little behind his hands.)
	const mid = {
		x: (A.x + B.x) / 2,
		y: (A.y + B.y) / 2,
		z: (A.z + B.z) / 2 + 0.05,
	};
	const off = ((sl + sr) / 2) * JERSEY.w;
	return {
		origin: {
			x: mid.x - ux * off,
			y: mid.y - uy * off,
			z: mid.z - uz * off,
		},
		alongX: {
			x: (ux * JERSEY.w) / img.width,
			y: (uy * JERSEY.w) / img.width,
			z: (uz * JERSEY.w) / img.width,
		},
		alongY: { x: 0, y: 0, z: -JERSEY.h / img.height },
		w: img.width,
		h: img.height,
	};
};

// His hands over its shoulders, gripping them: the backs of his fingers
// down the front of it (his forearms behind it).
const drawGrip = (ctx: Ctx, cam: Camera, at: Pt3, skin: string) => {
	const p = project(cam, at);
	const k = p.k;
	if (k * 0.3 < 1.5) {
		return;
	}
	const hw = 0.15 * k;
	const top = p.y - 0.07 * k;
	const bottom = p.y + 0.27 * k;
	const r = hw * 0.7;
	ctx.fillStyle = skin;
	ctx.beginPath();
	ctx.moveTo(p.x - hw, top + r);
	ctx.quadraticCurveTo(p.x - hw, top, p.x - hw + r, top);
	ctx.lineTo(p.x + hw - r, top);
	ctx.quadraticCurveTo(p.x + hw, top, p.x + hw, top + r);
	ctx.lineTo(p.x + hw * 0.92, bottom - r * 0.6);
	ctx.quadraticCurveTo(p.x + hw * 0.9, bottom, p.x + hw * 0.55, bottom);
	ctx.lineTo(p.x - hw * 0.55, bottom);
	ctx.quadraticCurveTo(
		p.x - hw * 0.9,
		bottom,
		p.x - hw * 0.92,
		bottom - r * 0.6,
	);
	ctx.closePath();
	ctx.fill();
	ctx.strokeStyle = shade(skin, -0.4);
	ctx.lineWidth = Math.max(0.8, k * 0.018);
	ctx.stroke();
	// (Between his fingers, and the knuckles across them.)
	ctx.strokeStyle = shade(skin, -0.28);
	ctx.lineWidth = Math.max(0.6, k * 0.012);
	ctx.beginPath();
	for (const f of [-1 / 3, 0, 1 / 3]) {
		ctx.moveTo(p.x + f * hw * 1.6, p.y + 0.11 * k);
		ctx.lineTo(p.x + f * hw * 1.5, bottom - 0.01 * k);
	}
	ctx.moveTo(p.x - hw * 0.8, p.y + 0.08 * k);
	ctx.quadraticCurveTo(p.x, p.y + 0.05 * k, p.x + hw * 0.8, p.y + 0.08 * k);
	ctx.stroke();
};

// A microphone on its stand on the table, its head up toward his mouth.
const drawMic = (ctx: Ctx, cam: Camera, x: number, top: number) => {
	const base = project(cam, { x, y: SEAT_Y + TABLE.from + 0.55, z: top });
	const head = project(cam, { x, y: SEAT_Y + TABLE.from + 0.2, z: top + 1.05 });
	ctx.fillStyle = "#1a1b1f";
	ctx.beginPath();
	ctx.ellipse(base.x, base.y, 0.22 * base.k, 0.07 * base.k, 0, 0, Math.PI * 2);
	ctx.fill();
	ctx.strokeStyle = "#202126";
	ctx.lineWidth = Math.max(1, 0.05 * base.k);
	ctx.beginPath();
	ctx.moveTo(base.x, base.y);
	ctx.quadraticCurveTo(
		base.x,
		head.y + (base.y - head.y) * 0.3,
		head.x,
		head.y,
	);
	ctx.stroke();
	ctx.fillStyle = "#121316";
	ctx.beginPath();
	ctx.ellipse(head.x, head.y, 0.08 * head.k, 0.13 * head.k, 0, 0, Math.PI * 2);
	ctx.fill();
};
// A microphone on a gooseneck, up off the top of the podium toward his
// mouth.
const drawGooseneck = (ctx: Ctx, cam: Camera, base: Pt3, head: Pt3) => {
	const b = project(cam, base);
	const h = project(cam, head);
	const bend = project(cam, {
		x: (base.x + head.x) / 2,
		y: base.y,
		z: (base.z + head.z) / 2 + 0.15,
	});
	ctx.strokeStyle = "#1c1d22";
	ctx.lineWidth = Math.max(1, 0.04 * b.k);
	ctx.lineCap = "round";
	ctx.beginPath();
	ctx.moveTo(b.x, b.y);
	ctx.quadraticCurveTo(bend.x, bend.y, h.x, h.y);
	ctx.stroke();
	ctx.fillStyle = "#101114";
	ctx.beginPath();
	ctx.ellipse(h.x, h.y, 0.075 * h.k, 0.12 * h.k, 0, 0, Math.PI * 2);
	ctx.fill();
	ctx.fillStyle = "#26272c";
	ctx.beginPath();
	ctx.ellipse(b.x, b.y, 0.09 * b.k, 0.04 * b.k, 0, 0, Math.PI * 2);
	ctx.fill();
};
// A bottle of water.
const drawBottle = (ctx: Ctx, cam: Camera, x: number, top: number) => {
	const b = project(cam, { x, y: SEAT_Y + TABLE.from + 0.9, z: top });
	const t = project(cam, { x, y: SEAT_Y + TABLE.from + 0.9, z: top + 0.7 });
	const r = 0.12 * b.k;
	ctx.fillStyle = "rgba(205, 225, 245, 0.75)";
	ctx.fillRect(b.x - r, t.y, r * 2, b.y - t.y);
	ctx.fillStyle = "#2c6fbf";
	ctx.fillRect(b.x - r, t.y + (b.y - t.y) * 0.38, r * 2, (b.y - t.y) * 0.24);
	ctx.fillStyle = "#e8edf2";
	ctx.fillRect(b.x - r * 0.45, t.y - r * 0.7, r * 0.9, r * 0.7);
};

// (Feet up off the floor, for what stands on the riser.)
const lifted = (cam: Camera, by: number): Camera => ({
	...cam,
	pos: { ...cam.pos, z: cam.pos.z - by },
});

export const pressVenue = (
	paint: PressVenuePaint,
	seats: Seats,
	seen: PressSeen,
	skinOf: (pid: number) => string | undefined,
): Venue => {
	const backdropPlane = wall(
		BACKDROP.x0,
		BACKDROP.x1,
		BACKDROP.y,
		BACKDROP.z0,
		BACKDROP.z1,
		paint.backdrop.width,
		paint.backdrop.height,
	);
	const drapePlane = wall(
		CX - 46,
		CX + 46,
		BACKDROP.y - 0.8,
		0,
		22,
		paint.drape.width,
		paint.drape.height,
	);
	const table = tableOf(seats);
	const top = RISER.h + TABLE.h;
	return {
		ground: groundAt,
		floor: (ctx, cam) => {
			// The room: dim, the ceiling darker.
			ctx.save();
			ctx.setTransform(1, 0, 0, 1, 0, 0);
			const g = ctx.createLinearGradient(0, 0, 0, ctx.canvas.height);
			g.addColorStop(0, "#08090b");
			g.addColorStop(1, "#16171b");
			ctx.fillStyle = g;
			ctx.fillRect(0, 0, ctx.canvas.width, ctx.canvas.height);
			ctx.restore();
			// Its carpet.
			fill(
				ctx,
				cam,
				[
					{ x: CX - 46, y: BACKDROP.y - 0.8, z: 0 },
					{ x: CX + 46, y: BACKDROP.y - 0.8, z: 0 },
					{ x: CX + 46, y: 70, z: 0 },
					{ x: CX - 46, y: 70, z: 0 },
				],
				"#25272d",
			);
			drawTexturedPlane(ctx, cam, drapePlane, paint.drape, 8, 1);
			drawTexturedPlane(ctx, cam, backdropPlane, paint.backdrop, 10, 4);
			// The riser: its carpet, its front.
			drawBox(ctx, cam, RISER.x0, RISER.x1, RISER.y0, RISER.y1, 0, RISER.h, {
				front: "#111215",
				top: "#1c1e23",
				side: "#0d0e10",
			});
			fill(
				ctx,
				cam,
				[
					{ x: RISER.x0, y: RISER.y1, z: RISER.h - 0.05 },
					{ x: RISER.x1, y: RISER.y1, z: RISER.h - 0.05 },
					{ x: RISER.x1, y: RISER.y1, z: RISER.h },
					{ x: RISER.x0, y: RISER.y1, z: RISER.h },
				],
				"#2b2d33",
			);
		},
		items: (ctx, cam, t) => {
			const items: { depth: number; draw: () => void }[] = [];
			if (seen.table(t)) {
				const up = lifted(cam, RISER.h);
				// The chairs behind it.
				for (const s of seats) {
					const c = { x: s.x, y: s.y, yaw: Math.PI / 2 };
					const back = { x: c.x, y: c.y - 0.72 };
					for (const [part, p, nudge] of [
						["seat", c, 0.05],
						["back", back, 0],
					] as const) {
						items.push({
							depth: depthOf(cam, { x: p.x, y: p.y, z: 3 }) + nudge + 0.4,
							draw: () => {
								drawChair(ctx, up, c, 1.55, "#16171b", part);
							},
						});
					}
				}
				// Its top, under their arms on it - and its skirt in front of
				// them, hiding them from the waist down.
				items.push(
					{
						depth: depthOf(cam, { x: CX, y: SEAT_Y - 0.2, z: 3 }),
						draw: () => {
							if (cam.pos.z > top) {
								fill(
									ctx,
									cam,
									[
										{ x: table.x0, y: table.y0, z: top },
										{ x: table.x1, y: table.y0, z: top },
										{ x: table.x1, y: table.y1, z: top },
										{ x: table.x0, y: table.y1, z: top },
									],
									"#15161a",
								);
							}
						},
					},
					{
						depth: depthOf(cam, { x: CX, y: table.y1, z: 3 }),
						draw: () => {
							drawBox(
								ctx,
								up,
								table.x0,
								table.x1,
								table.y1 - 0.05,
								table.y1,
								0,
								TABLE.h,
								{ front: "#15161a", top: "#15161a", side: "#0c0d10" },
								paint.skirt,
							);
							// (Its ends.)
							const camX = cam.pos.x;
							if (camX < table.x0 || camX > table.x1) {
								const x = camX < table.x0 ? table.x0 : table.x1;
								fill(
									ctx,
									cam,
									[
										{ x, y: table.y0, z: RISER.h },
										{ x, y: table.y1, z: RISER.h },
										{ x, y: table.y1, z: top },
										{ x, y: table.y0, z: top },
									],
									"#0f1013",
								);
							}
						},
					},
				);
				// On it: a microphone, a bottle of water, a card with the logo.
				for (const s of seats) {
					items.push(
						{
							depth: depthOf(cam, {
								x: s.x,
								y: SEAT_Y + TABLE.from + 0.3,
								z: 3,
							}),
							draw: () => {
								drawMic(ctx, cam, s.x, top);
								drawBottle(ctx, cam, s.x + 0.85, top);
							},
						},
						{
							depth: depthOf(cam, { x: s.x, y: table.y1, z: 3 }) - 0.01,
							draw: () => {
								const p0 = { x: s.x - 0.48, y: table.y1 - 0.25, z: top + 0.4 };
								drawTexturedPlane(
									ctx,
									cam,
									{
										origin: p0,
										alongX: { x: 0.96 / paint.placard.width, y: 0, z: 0 },
										alongY: {
											x: 0,
											y: 0.1 / paint.placard.height,
											z: -0.4 / paint.placard.height,
										},
										w: paint.placard.width,
										h: paint.placard.height,
									},
									paint.placard,
									1,
									1,
								);
							},
						},
					);
				}
			}
			// The podium, and its microphones over the top of it.
			if (seen.podium(t)) {
				const up = lifted(cam, RISER.h);
				const x0 = PODIUM.x - PODIUM.w / 2;
				const x1 = PODIUM.x + PODIUM.w / 2;
				const y0 = PODIUM.y - PODIUM.d / 2;
				const y1 = PODIUM.y + PODIUM.d / 2;
				const ptop = RISER.h + PODIUM.h;
				items.push({
					depth: depthOf(cam, { x: PODIUM.x, y: y1, z: 3 }),
					draw: () => {
						drawBox(
							ctx,
							up,
							x0,
							x1,
							y0,
							y1,
							0,
							PODIUM.h,
							{
								front: "#15161a",
								top: "#202127",
								side: shade(paint.main, -0.55),
							},
							paint.podium,
						);
						for (const s of [-1, 1]) {
							drawGooseneck(
								ctx,
								cam,
								{ x: PODIUM.x + s * 0.2, y: y0 + 0.25, z: ptop },
								{ x: PODIUM.x + s * 0.13, y: y0 - 0.15, z: ptop + 0.8 },
							);
						}
					},
				});
			}
			// Their jerseys, held up - and their fists on them.
			for (const j of seen.jerseys(t)) {
				const img = paint.jerseys.get(j.pid);
				if (!img) {
					continue;
				}
				const mid = {
					x: (j.hands.r.x + j.hands.l.x) / 2,
					y: (j.hands.r.y + j.hands.l.y) / 2,
					z: (j.hands.r.z + j.hands.l.z) / 2,
				};
				items.push({
					depth: depthOf(cam, mid) - 0.05,
					draw: () => {
						drawTexturedPlane(
							ctx,
							cam,
							jerseyPlane(cam, j.hands, img),
							img,
							2,
							3,
						);
						const skin = skinOf(j.pid) ?? "#8a5a3b";
						drawGrip(ctx, cam, j.hands.r, skin);
						drawGrip(ctx, cam, j.hands.l, skin);
					},
				});
			}
			// The balls in the execs' hands.
			for (const b of seen.balls(t)) {
				items.push({
					depth: depthOf(cam, b) - 0.1,
					draw: () => {
						drawBall(ctx, cam, b, 0);
					},
				});
			}
			return items;
		},
	};
};
