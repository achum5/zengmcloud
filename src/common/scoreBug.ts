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
	"awayRecord",
	"homeRecord",
	"awayBall",
	"homeBall",
	"awayBonus",
	"homeBonus",
	"homeLead",
	"series",
	"period",
	"clock",
	"shotClock",
	"down",
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
	"spacing",
	"radius",
	"opacity",
	"rotate",
	"zoom",
	"bonusShift",
	"sub",
	"lowAt",
]);
const PIECE_STRINGS = new Set([
	"color",
	"background",
	"font",
	"text",
	"image",
	"border",
	"clip",
	"shadow",
	"blend",
	"limit",
	"off",
	"low",
	"lowBackground",
	"undated",
]);
const PIECE_BOOLEANS = new Set(["italic", "upper", "colon", "proportional"]);
const FITS = new Set(["contain", "cover", "fill"]);
const ORDINALS = new Set(["plain", "sup", "small"]);
const WHENS = new Set(["noBonus", "bonus", "awayBonus", "homeBonus"]);
const MAX_PIECES = 80;
const MAX_FONTS = 6;
const MAX_TEAM_COLORS = 100;

const isNumber = (v: unknown): v is number =>
	typeof v === "number" && Number.isFinite(v);

// (Said when a team looks file is loaded where one team's bug or court is.)
export const NOT_TEAM_LOOKS =
	"That's a team looks file. Import it in Manage Teams (Import team looks).";

export const parseScoreBug = (raw: unknown): ScoreBugStyle | string => {
	if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
		return "Expected a JSON object.";
	}
	const r = raw as Record<string, unknown>;
	if ("teamLooks" in r) {
		return NOT_TEAM_LOOKS;
	}
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
				"bottom",
				"edge",
				"shadow",
				"font",
				"fonts",
				"teamColors",
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
	for (const key of ["background", "image", "shadow", "font"] as const) {
		if (r[key] !== undefined && typeof r[key] !== "string") {
			return `"${key}" should be text.`;
		}
	}
	for (const key of ["radius", "bottom", "edge"] as const) {
		if (r[key] !== undefined && !isNumber(r[key])) {
			return `"${key}" should be a number.`;
		}
	}
	if (r.fonts !== undefined) {
		if (!Array.isArray(r.fonts) || r.fonts.length > MAX_FONTS) {
			return `"fonts" should be a list (at most ${MAX_FONTS}).`;
		}
		for (const [i, f] of r.fonts.entries()) {
			if (
				typeof f !== "object" ||
				f === null ||
				typeof f.name !== "string" ||
				!f.name.trim() ||
				typeof f.src !== "string" ||
				Object.keys(f).some((k) => k !== "name" && k !== "src")
			) {
				return `font ${i + 1} should be { "name": ..., "src": ... }.`;
			}
		}
	}
	if (r.teamColors !== undefined) {
		const teams = r.teamColors;
		if (
			typeof teams !== "object" ||
			teams === null ||
			Array.isArray(teams) ||
			Object.keys(teams).length > MAX_TEAM_COLORS
		) {
			return `"teamColors" should be an object of abbreviations (at most ${MAX_TEAM_COLORS}).`;
		}
		for (const [abbrev, colors] of Object.entries(teams)) {
			if (
				!Array.isArray(colors) ||
				colors.length < 1 ||
				colors.length > 3 ||
				colors.some((c) => typeof c !== "string")
			) {
				return `"teamColors" ${abbrev} should be a list of 1 to 3 colors.`;
			}
		}
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
			} else if (PIECE_BOOLEANS.has(k)) {
				if (typeof v !== "boolean") {
					return `${at}: "${k}" should be true or false.`;
				}
			} else if (k === "fit") {
				if (typeof v !== "string" || !FITS.has(v)) {
					return `${at}: "fit" should be "contain", "cover" or "fill".`;
				}
			} else if (k === "ordinal") {
				if (typeof v !== "string" || !ORDINALS.has(v)) {
					return `${at}: "ordinal" should be "plain", "sup" or "small".`;
				}
			} else if (k === "when") {
				if (typeof v !== "string" || !WHENS.has(v)) {
					return `${at}: "when" should be one of ${[...WHENS].join(", ")}.`;
				}
			} else if (k === "side") {
				if (v !== "away" && v !== "home") {
					return `${at}: "side" should be "away" or "home".`;
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
	{
		show: `${who}Record`,
		x: x + 92,
		y: 50,
		w: 60,
		h: 18,
		color: "#d8d4de",
		size: 11,
	},
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
					h: 44,
					color: "#ffffff",
					size: 22,
					weight: 800,
				},
				{
					show: "awayRecord",
					x: 56,
					y: 35,
					w: 110,
					h: 14,
					color: "#9aa6bd",
					size: 11,
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
					h: 44,
					color: "#ffffff",
					size: 22,
					weight: 800,
				},
				{
					show: "homeRecord",
					x: 56,
					y: 87,
					w: 110,
					h: 14,
					color: "#9aa6bd",
					size: 11,
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

// Football's, the same but for the down and distance where the shot clock
// was (and the room for it), and no fouls.
const FOOTBALL_DOWN: Record<
	string,
	{ width: number; span?: number; down: Omit<ScoreBugPiece, "show"> }
> = {
	Bar: {
		width: 650,
		span: 0.68,
		down: {
			x: 524,
			y: 8,
			w: 118,
			h: 34,
			color: "#ffb547",
			background: "#1c1408",
			size: 17,
			weight: 800,
			align: "center",
			radius: 4,
		},
	},
	"Corner box": {
		width: 330,
		down: {
			x: 252,
			y: 70,
			w: 78,
			h: 26,
			color: "#ffd166",
			size: 13,
			weight: 800,
			align: "center",
		},
	},
	Minimal: {
		width: 500,
		span: 0.5,
		down: {
			x: 386,
			y: 0,
			w: 104,
			h: 40,
			color: "#ffb547",
			size: 15,
			weight: 700,
			align: "center",
		},
	},
};
export const FOOTBALL_SCORE_BUG_PRESETS: {
	name: string;
	style: ScoreBugStyle;
}[] = SCORE_BUG_PRESETS.map(({ name, style }) => {
	const f = FOOTBALL_DOWN[name];
	if (!f) {
		return { name, style };
	}
	return {
		name,
		style: {
			...style,
			width: f.width,
			...(f.span === undefined ? {} : { span: f.span }),
			pieces: style.pieces.flatMap((p): ScoreBugPiece[] =>
				p.show === "awayFouls" || p.show === "homeFouls"
					? []
					: p.show === "shotClock"
						? [{ ...f.down, show: "down" }]
						: [p],
			),
		},
	};
});

// The pictures a score bug names, uploaded ("pic:<id>") - its fonts too.
export const scoreBugPictureIds = (bug: ScoreBugStyle | null | undefined) =>
	bug
		? [
				bug.image,
				...bug.pieces.map((p) => p.image),
				...(bug.fonts ?? []).map((f) => f.src),
			]
				.filter(
					(u): u is string => typeof u === "string" && u.startsWith("pic:"),
				)
				.map((u) => u.slice(4))
		: [];

// Every URL in a score bug that names a picture or a font, rewritten.
export const mapScoreBugURLs = async (
	bug: ScoreBugStyle,
	f: (url: string) => Promise<string | undefined>,
): Promise<ScoreBugStyle> => {
	const out: ScoreBugStyle = { ...bug };
	if (bug.image !== undefined) {
		const image = await f(bug.image);
		if (image === undefined) {
			delete out.image;
		} else {
			out.image = image;
		}
	}
	if (bug.fonts) {
		const fonts = [];
		for (const font of bug.fonts) {
			const src = await f(font.src);
			if (src !== undefined) {
				fonts.push({ ...font, src });
			}
		}
		out.fonts = fonts;
	}
	out.pieces = [];
	for (const p of bug.pieces) {
		if (p.image === undefined) {
			out.pieces.push(p);
			continue;
		}
		const image = await f(p.image);
		if (image === undefined) {
			const { image: _, ...rest } = p;
			out.pieces.push(rest);
		} else {
			out.pieces.push({ ...p, image });
		}
	}
	return out;
};

// 3 as "rd".
export const ordinalSuffix = (n: number) =>
	n % 100 >= 11 && n % 100 <= 13
		? "th"
		: (["th", "st", "nd", "rd"][n % 10] ?? "th");

// A record as a bug writes it (see ScoreBugPiece's `text`): {rec}, and
// {rank} and {conf} ({CONF} in capitals) - the record alone where the
// standing isn't known, and nothing for a standing alone.
export const recordText = (
	text: string | undefined,
	t: { record?: string; rank?: number; conf?: string } | undefined,
) => {
	const rec = t?.record ?? "";
	if (text === undefined || rec === "") {
		return rec;
	}
	if (
		(text.includes("{rank}") && t?.rank === undefined) ||
		(text.toLowerCase().includes("{conf}") && t?.conf === undefined)
	) {
		return text.includes("{rec}") ? rec : "";
	}
	return text
		.replaceAll("{rec}", rec)
		.replaceAll(
			"{rank}",
			t?.rank === undefined ? "" : `${t.rank}${ordinalSuffix(t.rank)}`,
		)
		.replaceAll("{conf}", t?.conf ?? "")
		.replaceAll("{CONF}", (t?.conf ?? "").toUpperCase());
};

// What a "text" or "series" piece says (see ScoreBugPiece's `text`): {DAY}
// the game's weekday ("TUESDAY"), {Day} as written ("Tuesday") - or, with
// no date to tell it (`day` undefined), its `undated` words, else the words
// around it.
const DAY_TOKEN = /{(?:DAY|Day)}/g;
const HAS_DAY = /{(?:DAY|Day)}/;
export const dayText = <T extends string | undefined>(
	text: T,
	undated: string | undefined,
	day: string | undefined,
): T | string => {
	if (text === undefined || !HAS_DAY.test(text)) {
		return text;
	}
	if (day === undefined) {
		return (
			undated ??
			text
				.replace(DAY_TOKEN, "")
				.replaceAll(/\s{2,}/g, " ")
				.trim()
		);
	}
	return text.replaceAll("{DAY}", day.toUpperCase()).replaceAll("{Day}", day);
};

// (How long it says it, with a day in it.)
export const dayTextLength = (text: string) =>
	text.replace(DAY_TOKEN, "SATURDAY").length;

// WHICH BUG A GAME CARRIES: the broadcast you'd be watching - your own
// team's when it plays, the home team's otherwise - if that team has a bug
// of its own; the league's if not; null for the default. `teams`: the
// game's, home first.
export const scoreBugForGame = (
	league: ScoreBugStyle | null | undefined,
	teams: { tid: number; scoreBug?: ScoreBugStyle }[],
	userTid: number | undefined,
): ScoreBugStyle | null => {
	const mine = teams.find((t) => t.tid === userTid);
	return mine?.scoreBug ?? teams[0]?.scoreBug ?? league ?? null;
};

// A GAME ON ITS TEAMS' OWN BROADCASTS, either one to watch it on: each
// team's bug (its own, or the league's - null for the default), home first,
// and `tid`, whose the game is on until the viewer picks (the one
// scoreBugForGame gives). Nothing to pick when no team has a bug of its own,
// or both look the same.
export type LocalBroadcasts = {
	tid: number;
	bugs: { tid: number; scoreBug: ScoreBugStyle | null }[];
};

export const localBroadcasts = (
	league: ScoreBugStyle | null | undefined,
	teams: { tid: number; scoreBug?: ScoreBugStyle }[],
	userTid: number | undefined,
): LocalBroadcasts | undefined => {
	if (teams.length !== 2 || !teams.some((t) => t.scoreBug)) {
		return undefined;
	}
	const bugs = teams.map((t) => ({
		tid: t.tid,
		scoreBug: t.scoreBug ?? league ?? null,
	}));
	if (JSON.stringify(bugs[0]!.scoreBug) === JSON.stringify(bugs[1]!.scoreBug)) {
		return undefined;
	}
	const mine = teams.find((t) => t.tid === userTid && t.scoreBug);
	return { tid: (mine ?? teams[0]!).tid, bugs };
};
