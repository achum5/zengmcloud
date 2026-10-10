import type { ScoreBugPiece, ScoreBugShow, ScoreBugStyle } from "./types.ts";

// THE LEAGUE'S SCORE BUG, CHECKED (see ScoreBugStyle) - and a few to start
// from.

const SHOWS = new Set<ScoreBugShow>([
	"awayLogo",
	"homeLogo",
	"awayAbbrev",
	"homeAbbrev",
	"awayRegion",
	"homeRegion",
	"awayName",
	"homeName",
	"awayScore",
	"homeScore",
	"awayFouls",
	"homeFouls",
	"awayTimeouts",
	"homeTimeouts",
	"awayBall",
	"homeBall",
	"period",
	"clock",
	"shotClock",
	"box",
	"text",
	"image",
]);
const PIECE_NUMBERS = new Set([
	"x",
	"y",
	"w",
	"h",
	"size",
	"weight",
	"radius",
	"opacity",
]);
const PIECE_STRINGS = new Set(["color", "background", "font", "text", "image"]);
const MAX_PIECES = 80;

const isNumber = (v: unknown): v is number =>
	typeof v === "number" && Number.isFinite(v);

export const parseScoreBug = (raw: unknown): ScoreBugStyle | string => {
	if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
		return "Expected a JSON object.";
	}
	const r = raw as Record<string, unknown>;
	for (const key of Object.keys(r)) {
		if (
			![
				"width",
				"height",
				"span",
				"place",
				"background",
				"image",
				"radius",
				"pieces",
			].includes(key)
		) {
			return `Unknown field "${key}".`;
		}
	}
	if (
		!isNumber(r.width) ||
		r.width <= 0 ||
		!isNumber(r.height) ||
		r.height <= 0
	) {
		return `"width" and "height" should be positive numbers.`;
	}
	if (
		r.span !== undefined &&
		(!isNumber(r.span) || r.span <= 0 || r.span > 1)
	) {
		return `"span" should be a share of the picture's width, up to 1.`;
	}
	if (
		r.place !== undefined &&
		r.place !== "center" &&
		r.place !== "left" &&
		r.place !== "right"
	) {
		return `"place" should be "center", "left" or "right".`;
	}
	for (const key of ["background", "image"] as const) {
		if (r[key] !== undefined && typeof r[key] !== "string") {
			return `"${key}" should be text.`;
		}
	}
	if (r.radius !== undefined && !isNumber(r.radius)) {
		return `"radius" should be a number.`;
	}
	if (!Array.isArray(r.pieces) || r.pieces.length > MAX_PIECES) {
		return `"pieces" should be a list (at most ${MAX_PIECES}).`;
	}
	const pieces: ScoreBugPiece[] = [];
	for (const [i, p] of r.pieces.entries()) {
		const at = `piece ${i + 1}`;
		if (typeof p !== "object" || p === null || Array.isArray(p)) {
			return `${at} should be an object.`;
		}
		const piece = p as Record<string, unknown>;
		if (
			typeof piece.show !== "string" ||
			!SHOWS.has(piece.show as ScoreBugShow)
		) {
			return `${at}: "show" should be one of ${[...SHOWS].join(", ")}.`;
		}
		for (const [k, v] of Object.entries(piece)) {
			if (k === "show") {
				continue;
			}
			if (PIECE_NUMBERS.has(k)) {
				if (!isNumber(v)) {
					return `${at}: "${k}" should be a number.`;
				}
			} else if (PIECE_STRINGS.has(k)) {
				if (typeof v !== "string") {
					return `${at}: "${k}" should be text.`;
				}
			} else if (k === "align") {
				if (v !== "left" && v !== "center" && v !== "right") {
					return `${at}: "align" should be "left", "center" or "right".`;
				}
			} else if (k === "italic") {
				if (typeof v !== "boolean") {
					return `${at}: "italic" should be true or false.`;
				}
			} else {
				return `${at}: unknown field "${k}".`;
			}
		}
		for (const k of ["x", "y", "w", "h"]) {
			if (!isNumber(piece[k])) {
				return `${at}: "${k}" is needed.`;
			}
		}
		pieces.push(piece as unknown as ScoreBugPiece);
	}
	return { ...(r as unknown as ScoreBugStyle), pieces };
};

// TO START FROM.
const side = (who: "away" | "home", x: number): ScoreBugPiece[] => [
	{ show: "box", x, y: 0, w: 200, h: 46, background: `${who}0` },
	{ show: "box", x, y: 46, w: 200, h: 4, background: `${who}1` },
	{ show: "box", x, y: 0, w: 46, h: 46, background: "#f4f4f4" },
	{ show: `${who}Logo`, x: x + 5, y: 5, w: 36, h: 36 },
	{ show: `${who}Ball`, x: x + 52, y: 15, w: 12, h: 16, color: "#ffffff" },
	{
		show: `${who}Abbrev`,
		x: x + 66,
		y: 0,
		w: 80,
		h: 46,
		color: "#ffffff",
		size: 20,
		weight: 700,
	},
	{
		show: "box",
		x: x + 146,
		y: 0,
		w: 54,
		h: 46,
		background: "rgba(0,0,0,.28)",
	},
	{
		show: `${who}Score`,
		x: x + 146,
		y: 0,
		w: 54,
		h: 46,
		color: "#ffffff",
		size: 26,
		weight: 800,
		align: "center",
	},
	{
		show: `${who}Fouls`,
		x: x + 100,
		y: 50,
		w: 96,
		h: 18,
		color: "#d8d4de",
		size: 11,
		align: "right",
	},
	{ show: `${who}Timeouts`, x: x + 6, y: 55, w: 80, h: 8 },
];

export const SCORE_BUG_PRESETS: { name: string; style: ScoreBugStyle }[] = [
	{
		name: "Bar",
		style: {
			width: 560,
			height: 68,
			span: 0.62,
			background: "rgba(10,10,14,.92)",
			radius: 6,
			pieces: [
				...side("away", 0),
				...side("home", 200),
				{
					show: "period",
					x: 404,
					y: 0,
					w: 50,
					h: 50,
					color: "#bdb8c6",
					size: 18,
					weight: 700,
					align: "center",
				},
				{
					show: "clock",
					x: 450,
					y: 0,
					w: 70,
					h: 50,
					color: "#f4f4f4",
					size: 22,
					weight: 800,
					align: "center",
				},
				{
					show: "shotClock",
					x: 520,
					y: 8,
					w: 36,
					h: 34,
					color: "#ffb547",
					background: "#1c1408",
					size: 20,
					weight: 800,
					align: "center",
					radius: 4,
				},
			],
		},
	},
	{
		name: "Corner box",
		style: {
			width: 330,
			height: 104,
			span: 0.34,
			place: "left",
			background: "#101318",
			radius: 4,
			pieces: [
				{ show: "box", x: 0, y: 0, w: 6, h: 52, background: "away0" },
				{ show: "awayLogo", x: 12, y: 8, w: 36, h: 36 },
				{
					show: "awayAbbrev",
					x: 56,
					y: 0,
					w: 110,
					h: 52,
					color: "#ffffff",
					size: 22,
					weight: 800,
				},
				{ show: "awayBall", x: 168, y: 18, w: 12, h: 16, color: "#ffffff" },
				{
					show: "awayScore",
					x: 180,
					y: 0,
					w: 64,
					h: 52,
					color: "#ffffff",
					size: 30,
					weight: 800,
					align: "right",
				},
				{ show: "box", x: 0, y: 52, w: 6, h: 52, background: "home0" },
				{ show: "homeLogo", x: 12, y: 60, w: 36, h: 36 },
				{
					show: "homeAbbrev",
					x: 56,
					y: 52,
					w: 110,
					h: 52,
					color: "#ffffff",
					size: 22,
					weight: 800,
				},
				{ show: "homeBall", x: 168, y: 70, w: 12, h: 16, color: "#ffffff" },
				{
					show: "homeScore",
					x: 180,
					y: 52,
					w: 64,
					h: 52,
					color: "#ffffff",
					size: 30,
					weight: 800,
					align: "right",
				},
				{ show: "box", x: 252, y: 0, w: 78, h: 104, background: "#1c2230" },
				{
					show: "period",
					x: 252,
					y: 6,
					w: 78,
					h: 26,
					color: "#9aa6bd",
					size: 16,
					weight: 700,
					align: "center",
				},
				{
					show: "clock",
					x: 252,
					y: 32,
					w: 78,
					h: 36,
					color: "#ffffff",
					size: 24,
					weight: 800,
					align: "center",
				},
				{
					show: "shotClock",
					x: 270,
					y: 70,
					w: 42,
					h: 26,
					color: "#ffd166",
					size: 18,
					weight: 800,
					align: "center",
				},
			],
		},
	},
	{
		name: "Minimal",
		style: {
			width: 420,
			height: 40,
			span: 0.44,
			background: "rgba(0,0,0,.72)",
			radius: 20,
			pieces: [
				{
					show: "awayAbbrev",
					x: 8,
					y: 6,
					w: 64,
					h: 28,
					color: "#ffffff",
					background: "away0",
					size: 16,
					weight: 800,
					align: "center",
					radius: 14,
				},
				{
					show: "awayScore",
					x: 78,
					y: 0,
					w: 54,
					h: 40,
					color: "#ffffff",
					size: 22,
					weight: 800,
					align: "right",
				},
				{
					show: "homeScore",
					x: 146,
					y: 0,
					w: 54,
					h: 40,
					color: "#ffffff",
					size: 22,
					weight: 800,
				},
				{
					show: "homeAbbrev",
					x: 206,
					y: 6,
					w: 64,
					h: 28,
					color: "#ffffff",
					background: "home0",
					size: 16,
					weight: 800,
					align: "center",
					radius: 14,
				},
				{
					show: "period",
					x: 284,
					y: 0,
					w: 40,
					h: 40,
					color: "#bbbbbb",
					size: 15,
					weight: 600,
					align: "center",
				},
				{
					show: "clock",
					x: 324,
					y: 0,
					w: 60,
					h: 40,
					color: "#ffffff",
					size: 18,
					weight: 700,
					align: "center",
				},
				{
					show: "shotClock",
					x: 384,
					y: 0,
					w: 30,
					h: 40,
					color: "#ffb547",
					size: 15,
					weight: 700,
					align: "center",
				},
			],
		},
	},
];

// The pictures a score bug names, uploaded ("pic:<id>").
export const scoreBugPictureIds = (bug: ScoreBugStyle | null | undefined) =>
	bug
		? [bug.image, ...bug.pieces.map((p) => p.image)]
				.filter(
					(u): u is string => typeof u === "string" && u.startsWith("pic:"),
				)
				.map((u) => u.slice(4))
		: [];
