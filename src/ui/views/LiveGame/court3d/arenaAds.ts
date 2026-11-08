import { makeCourtRng } from "../courtRng.ts";

// THE ADS ROUND THE FLOOR: on the scorer's table, the boards along the
// stands and behind the baskets, the board over center court - in turn with
// the team's own screens (see boardAt). Each building has its sponsors, the
// same every game in it: made-up ones picked for it (see adsFor), or the
// team's own, set on its court (CourtStyle.arenaAds).

// A sponsor's mark beside its name, drawn: a ring, a bolt, a star...
export type AdMark =
	| "ring"
	| "bolt"
	| "star"
	| "chevron"
	| "wave"
	| "plus"
	| "peak"
	| "drop"
	| "shield"
	| "play"
	| "leaf"
	| "arrow"
	| "diamond"
	| "none";

export type ArenaAd = {
	text: string;
	// A line under the name (what it is: "Credit Union", "Sports Drink").
	sub?: string;
	// The board's colors, left to right; the name's; and the mark's (and
	// the line under it).
	bg: string;
	bg2: string;
	fg: string;
	accent: string;
	mark: AdMark;
	italic?: boolean;
	// The team's own picture for it, once loaded: drawn in place of the
	// name and mark, fitted to the board.
	picture?: HTMLCanvasElement | HTMLImageElement;
};

// Sponsors nobody has heard of - none a real company.
const SPONSORS: ArenaAd[] = [
	{
		text: "ZESTORA",
		sub: "COLA",
		bg: "#b3121b",
		bg2: "#e3232c",
		fg: "#ffffff",
		accent: "#ffd23f",
		mark: "bolt",
		italic: true,
	},
	{
		text: "BRIGHTWELL",
		sub: "BANK",
		bg: "#0f3b2e",
		bg2: "#1d6b4f",
		fg: "#ffffff",
		accent: "#d9c27a",
		mark: "peak",
	},
	{
		text: "ORBITRON",
		sub: "AIRLINES",
		bg: "#0b1f4d",
		bg2: "#1e5fbf",
		fg: "#ffffff",
		accent: "#7fd3ff",
		mark: "ring",
	},
	{
		text: "HARBORLY",
		sub: "HEALTH",
		bg: "#0a6e78",
		bg2: "#14a3a8",
		fg: "#ffffff",
		accent: "#ffffff",
		mark: "plus",
	},
	{
		text: "VELOCADE",
		sub: "FITNESS",
		bg: "#0c0d10",
		bg2: "#1b1e24",
		fg: "#c8ff2e",
		accent: "#c8ff2e",
		mark: "wave",
		italic: true,
	},
	{
		text: "PIXELPOP",
		sub: "GAMES",
		bg: "#5a138f",
		bg2: "#d12a8c",
		fg: "#ffffff",
		accent: "#4ff0ff",
		mark: "diamond",
	},
	{
		text: "FROSTBYTE",
		sub: "SPORTS DRINK",
		bg: "#0d4fa3",
		bg2: "#3aa6f2",
		fg: "#ffffff",
		accent: "#d6f3ff",
		mark: "drop",
		italic: true,
	},
	{
		text: "IRONVALE",
		sub: "TRUCKS",
		bg: "#2a2c30",
		bg2: "#45484f",
		fg: "#ffcc1a",
		accent: "#ffcc1a",
		mark: "shield",
	},
	{
		text: "GLIMMERBOX",
		sub: "STREAMING",
		bg: "#08080a",
		bg2: "#24180a",
		fg: "#f4c24a",
		accent: "#f4c24a",
		mark: "play",
	},
	{
		text: "COPPER HOLLOW",
		sub: "BREWING CO.",
		bg: "#5c2d12",
		bg2: "#a4552a",
		fg: "#fff3e0",
		accent: "#f2b46d",
		mark: "leaf",
	},
	{
		text: "AUROWAY",
		sub: "MOTORS",
		bg: "#101820",
		bg2: "#16424d",
		fg: "#ffffff",
		accent: "#4fe3c2",
		mark: "chevron",
		italic: true,
	},
	{
		text: "QUIKRUN",
		sub: "DELIVERY",
		bg: "#e0560f",
		bg2: "#f78a1d",
		fg: "#ffffff",
		accent: "#1a1a1a",
		mark: "arrow",
		italic: true,
	},
	{
		text: "STARLIO'S",
		sub: "PIZZA",
		bg: "#b51717",
		bg2: "#d8341c",
		fg: "#ffe14d",
		accent: "#ffffff",
		mark: "star",
	},
	{
		text: "KESTRELLE",
		sub: "WATCHES",
		bg: "#121316",
		bg2: "#2b2e34",
		fg: "#e6e6e6",
		accent: "#b7b7b7",
		mark: "ring",
	},
	{
		text: "BLUEPINE",
		sub: "CREDIT UNION",
		bg: "#123c7a",
		bg2: "#1f63b5",
		fg: "#ffffff",
		accent: "#9fd4ff",
		mark: "peak",
	},
	{
		text: "SUNHARBOR",
		sub: "SOLAR",
		bg: "#f2a20c",
		bg2: "#ffd23c",
		fg: "#1b1b1b",
		accent: "#c0390b",
		mark: "star",
	},
	{
		text: "NORTHLAKE",
		sub: "INSURANCE",
		bg: "#1d2b45",
		bg2: "#2f4a78",
		fg: "#ffffff",
		accent: "#f2c14e",
		mark: "shield",
	},
	{
		text: "WAVELINK",
		sub: "WIRELESS",
		bg: "#6b0fb3",
		bg2: "#2e3bd6",
		fg: "#ffffff",
		accent: "#ffffff",
		mark: "wave",
	},
	{
		text: "GREENLEAF",
		sub: "GROCERS",
		bg: "#1f7a2e",
		bg2: "#3fae3b",
		fg: "#ffffff",
		accent: "#e9f7b0",
		mark: "leaf",
	},
	{
		text: "TOPSPIN",
		sub: "SPORTSBOOK",
		bg: "#07120b",
		bg2: "#0e3b1d",
		fg: "#3dff7a",
		accent: "#3dff7a",
		mark: "diamond",
		italic: true,
	},
];

// How many sponsors a building has up (see BOARD_SCREENS' ad screens).
export const ADS_SHOWN = 4;

// A team's own ad, as set on its court: words, colors, a picture - any of
// them left out filled in from the team's colors.
export type ArenaAdStyle = {
	text?: string;
	sub?: string;
	bg?: string;
	fg?: string;
	imageURL?: string;
};

const HEX = /^#[\da-f]{6}$/i;
const lighten = (c: string, k: number) => {
	const n = Number.parseInt(c.slice(1), 16);
	const ch = (v: number) =>
		Math.max(0, Math.min(255, Math.round(v + (255 - v) * k)));
	return `#${[(n >> 16) & 255, (n >> 8) & 255, n & 255]
		.map((v) => ch(v).toString(16).padStart(2, "0"))
		.join("")}`;
};

// The building's ads: the team's own first, as many as it set (to
// ADS_SHOWN), the rest made-up sponsors picked for the building - the same
// every game in it.
export const adsFor = (
	key: string,
	colors: [string, string, string] | undefined,
	own: ArenaAdStyle[] | undefined,
	pictures?: (url: string) => ArenaAd["picture"],
): ArenaAd[] => {
	const c0 = colors?.[0] && HEX.test(colors[0]) ? colors[0] : "#8c1d40";
	const c1 = colors?.[1] && HEX.test(colors[1]) ? colors[1] : "#ffffff";
	const out: ArenaAd[] = [];
	for (const a of own ?? []) {
		if (out.length >= ADS_SHOWN) {
			break;
		}
		const text = (a.text ?? "").trim();
		const picture = a.imageURL ? pictures?.(a.imageURL) : undefined;
		if (!text && !a.imageURL) {
			continue;
		}
		const bg = a.bg && HEX.test(a.bg) ? a.bg : c0;
		out.push({
			text: text.toUpperCase(),
			...(a.sub?.trim() ? { sub: a.sub.trim().toUpperCase() } : {}),
			bg,
			bg2: lighten(bg, 0.18),
			fg: a.fg && HEX.test(a.fg) ? a.fg : c1,
			accent: a.fg && HEX.test(a.fg) ? a.fg : c1,
			mark: "none",
			...(picture ? { picture } : {}),
		});
	}
	const r = makeCourtRng(`ads|${key}`);
	const pool = SPONSORS.slice();
	while (out.length < ADS_SHOWN && pool.length > 0) {
		out.push(pool.splice(Math.floor(r() * pool.length), 1)[0]!);
	}
	return out;
};

// One sponsor's mark in a square `s` px across centred at (x, y).
const drawMark = (
	ctx: CanvasRenderingContext2D,
	mark: AdMark,
	x: number,
	y: number,
	s: number,
	color: string,
) => {
	const r = s / 2;
	ctx.save();
	ctx.translate(x, y);
	ctx.fillStyle = color;
	ctx.strokeStyle = color;
	ctx.lineWidth = Math.max(1, s * 0.14);
	ctx.lineJoin = "round";
	ctx.lineCap = "round";
	const poly = (pts: [number, number][]) => {
		ctx.beginPath();
		pts.forEach(([px, py], i) =>
			i ? ctx.lineTo(px * r, py * r) : ctx.moveTo(px * r, py * r),
		);
		ctx.closePath();
		ctx.fill();
	};
	switch (mark) {
		case "ring":
			ctx.beginPath();
			ctx.arc(0, 0, r * 0.78, 0, Math.PI * 2);
			ctx.stroke();
			ctx.beginPath();
			ctx.ellipse(0, 0, r * 0.95, r * 0.32, -0.45, 0, Math.PI * 2);
			ctx.lineWidth = Math.max(1, s * 0.07);
			ctx.stroke();
			break;
		case "bolt":
			poly([
				[0.2, -1],
				[-0.55, 0.12],
				[-0.02, 0.12],
				[-0.25, 1],
				[0.55, -0.15],
				[0.05, -0.15],
			]);
			break;
		case "star": {
			const pts: [number, number][] = [];
			for (let k = 0; k < 10; k++) {
				const a = -Math.PI / 2 + (k * Math.PI) / 5;
				const rr = k % 2 ? 0.42 : 1;
				pts.push([Math.cos(a) * rr, Math.sin(a) * rr]);
			}
			poly(pts);
			break;
		}
		case "chevron":
			poly([
				[-0.9, -0.7],
				[-0.2, -0.7],
				[0.6, 0],
				[-0.2, 0.7],
				[-0.9, 0.7],
				[-0.1, 0],
			]);
			break;
		case "wave":
			for (const dy of [-0.35, 0.25]) {
				ctx.beginPath();
				ctx.moveTo(-r * 0.95, dy * r);
				ctx.bezierCurveTo(
					-r * 0.4,
					(dy - 0.55) * r,
					r * 0.1,
					(dy + 0.55) * r,
					r * 0.95,
					dy * r,
				);
				ctx.stroke();
			}
			break;
		case "plus":
			ctx.fillRect(-r * 0.25, -r * 0.85, r * 0.5, r * 1.7);
			ctx.fillRect(-r * 0.85, -r * 0.25, r * 1.7, r * 0.5);
			break;
		case "peak":
			poly([
				[-1, 0.8],
				[-0.25, -0.75],
				[0.12, -0.05],
				[0.38, -0.45],
				[1, 0.8],
			]);
			break;
		case "drop":
			ctx.beginPath();
			ctx.moveTo(0, -r);
			ctx.bezierCurveTo(r * 0.9, -r * 0.05, r * 0.75, r, 0, r);
			ctx.bezierCurveTo(-r * 0.75, r, -r * 0.9, -r * 0.05, 0, -r);
			ctx.fill();
			break;
		case "shield":
			poly([
				[-0.8, -0.85],
				[0.8, -0.85],
				[0.8, 0.05],
				[0, 0.95],
				[-0.8, 0.05],
			]);
			break;
		case "play":
			ctx.beginPath();
			ctx.arc(0, 0, r * 0.9, 0, Math.PI * 2);
			ctx.lineWidth = Math.max(1, s * 0.09);
			ctx.stroke();
			poly([
				[-0.3, -0.45],
				[0.5, 0],
				[-0.3, 0.45],
			]);
			break;
		case "leaf":
			ctx.beginPath();
			ctx.moveTo(-r * 0.85, r * 0.85);
			ctx.quadraticCurveTo(-r * 0.9, -r * 0.9, r * 0.85, -r * 0.85);
			ctx.quadraticCurveTo(r * 0.9, r * 0.9, -r * 0.85, r * 0.85);
			ctx.fill();
			break;
		case "arrow":
			poly([
				[-0.95, -0.25],
				[0.15, -0.25],
				[0.15, -0.7],
				[0.95, 0],
				[0.15, 0.7],
				[0.15, 0.25],
				[-0.95, 0.25],
			]);
			break;
		case "diamond":
			poly([
				[0, -1],
				[0.75, 0],
				[0, 1],
				[-0.75, 0],
			]);
			break;
		case "none":
			break;
	}
	ctx.restore();
};

// One ad across the board from (x, y), `w` x `h` px: its colors, its mark
// and name (and the line under it) - or the team's picture for it, fitted
// in - with the board's sheen along its top.
export const paintAd = (
	ctx: CanvasRenderingContext2D,
	x: number,
	y: number,
	w: number,
	h: number,
	ad: ArenaAd,
) => {
	ctx.save();
	ctx.beginPath();
	ctx.rect(x, y, w, h);
	ctx.clip();
	const g = ctx.createLinearGradient(x, 0, x + w, 0);
	g.addColorStop(0, ad.bg);
	g.addColorStop(1, ad.bg2);
	ctx.fillStyle = g;
	ctx.fillRect(x, y, w, h);
	if (ad.picture && ad.picture.width > 0 && ad.picture.height > 0) {
		const k = Math.min(
			(w * 0.92) / ad.picture.width,
			(h * 0.84) / ad.picture.height,
		);
		const pw = ad.picture.width * k;
		const ph = ad.picture.height * k;
		ctx.drawImage(ad.picture, x + (w - pw) / 2, y + (h - ph) / 2, pw, ph);
	} else {
		// (A long low board - the table, a strip along the stands - has
		// the line beside the name, the name as big as the board lets it be;
		// a squarer one, under it.)
		const wide = w > h * 5;
		const markS = ad.mark === "none" ? 0 : h * (wide ? 0.66 : 0.62);
		const font = (px: number, weight: number) =>
			`${ad.italic ? "italic " : ""}${weight} ${Math.max(5, Math.round(px))}px Arial, sans-serif`;
		let big = h * (ad.sub && !wide ? 0.44 : 0.6);
		let small = h * (wide ? 0.27 : 0.2);
		const measure = () => {
			ctx.font = font(big, 900);
			const tw = ctx.measureText(ad.text).width;
			ctx.font = font(small, 700);
			const sw = ad.sub ? ctx.measureText(ad.sub).width : 0;
			return { tw, sw };
		};
		let { tw, sw } = measure();
		const gap = markS > 0 ? markS * 0.3 : 0;
		const sgap = wide && ad.sub ? big * 0.35 : 0;
		const across = () =>
			markS + gap + (wide ? tw + sgap + sw : Math.max(tw, sw));
		const room = w * 0.88;
		if (across() > room) {
			const k = (room - markS - gap) / (across() - markS - gap);
			big *= k;
			small *= k;
			({ tw, sw } = measure());
		}
		const left = x + (w - across()) / 2;
		const mid = y + h / 2;
		if (markS > 0) {
			drawMark(ctx, ad.mark, left + markS / 2, mid, markS, ad.accent);
		}
		ctx.textAlign = "left";
		ctx.textBaseline = "middle";
		ctx.fillStyle = ad.fg;
		ctx.font = font(big, 900);
		const tx = left + markS + gap;
		ctx.fillText(ad.text, tx, ad.sub && !wide ? mid - h * 0.1 : mid);
		if (ad.sub) {
			ctx.font = font(small, 700);
			ctx.fillStyle = ad.accent;
			if (wide) {
				ctx.fillText(ad.sub, tx + tw + sgap, mid + big * 0.08);
			} else {
				ctx.fillText(ad.sub, tx, mid + h * 0.26);
			}
		}
	}
	// The board's sheen, top to middle.
	const sheen = ctx.createLinearGradient(0, y, 0, y + h * 0.5);
	sheen.addColorStop(0, "rgba(255,255,255,0.16)");
	sheen.addColorStop(1, "rgba(255,255,255,0)");
	ctx.fillStyle = sheen;
	ctx.fillRect(x, y, w, h * 0.5);
	ctx.restore();
};
