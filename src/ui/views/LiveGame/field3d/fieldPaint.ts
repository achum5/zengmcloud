import { makeCourtRng } from "../courtRng.ts";
import { luminance, mix, shade } from "./color.ts";
import {
	END_ROOM,
	END_ZONE,
	FIELD_L,
	FIELD_W,
	GOAL_L,
	GOAL_R,
	MID_X,
	MID_Y,
	NUMBER_H,
	NUMBER_IN,
	NUMBER_W,
	SIDE_ROOM,
	YD,
} from "./geometry.ts";
import type { Pt3 } from "./geometry.ts";
import type { TexturedPlane } from "../court3d/planes.ts";
import { venueFor, type Venue } from "./venue.ts";

// THE HOME TEAM'S FIELD, PAINTED - the turf with its mowed stripes, the end
// zones in the home colors with the team's name across them, the numbers and
// the logo at midfield - once a game, each a flat picture for the field to
// lay in perspective (see planes.ts).
//
// The turf is painted coarse: it is only green, a stripe every five yards
// and a little grain. What has to be read is painted on its own, finer -
// each end zone, each number, the logo - and the lines are not painted at
// all but drawn every frame as strips of paint on the ground (see
// fieldLines.ts), so they stay crisp up close and never vanish far away.
//
// Everything a team might want its own way is in FieldStyle, so a stadium
// editor can set it later the way the court editor sets a court.

export type EndZoneStyle = {
	// Painted solid, left green (only the letters painted), or painted and
	// striped corner to corner.
	pattern: Venue["endZones"];
	fill: string;
	text: string;
	textColor: string;
	edge: string;
};

export type FieldStyle = {
	turf: string;
	// How much lighter every other five yards is mowed (0 to 1), and how
	// much each blade differs from the next.
	stripe: number;
	grain: number;
	lines: string;
	// The grass past the white border, round the field to the walls.
	surround: string;
	// [left, right].
	endZones: [EndZoneStyle, EndZoneStyle];
	numbers: string;
};

export type FieldTeam = {
	abbrev?: string;
	name?: string;
	region?: string;
	colors?: [string, string, string];
};

const endZoneOf = (
	team: FieldTeam | undefined,
	fallback: string,
	pattern: Venue["endZones"],
	turf: string,
): EndZoneStyle => {
	const fill = team?.colors?.[0] ?? fallback;
	const second = team?.colors?.[1] ?? "#ffffff";
	const third = team?.colors?.[2] ?? "#ffffff";
	const text = (team?.name ?? team?.abbrev ?? "").toUpperCase();
	if (pattern === "green") {
		// On the grass: lettered in the team's color, if it shows on green,
		// edged in another.
		const textColor =
			[fill, second, third].find((c) => contrast(c, turf) > 0.18) ?? "#ffffff";
		const edge =
			[second, third, fill, "#ffffff"].find(
				(c) => c !== textColor && contrast(c, textColor) > 0.2,
			) ?? shade(textColor, -0.4);
		return { pattern, fill: turf, text, textColor, edge };
	}
	// Lettered in the team's second color where it shows, edged in its third;
	// white (or black on a light end zone) otherwise.
	const light = luminance(fill) > 0.55;
	const textColor =
		Math.abs(luminance(second) - luminance(fill)) > 0.25
			? second
			: light
				? "#141414"
				: "#ffffff";
	const edge =
		Math.abs(luminance(third) - luminance(textColor)) > 0.2 && third !== fill
			? third
			: shade(fill, -0.35);
	return { pattern, fill, text, textColor, edge };
};

const contrast = (a: string, b: string) =>
	Math.abs(luminance(a) - luminance(b));

// The home team's field, as its venue has it (see venue.ts): natural grass
// mowed in five-yard stripes, or artificial turf - brighter, even, its
// stripes woven in faint - white paint, and the home team's colors and name
// in both end zones. At a neutral site the two teams split the ends.
export const fieldStyleFor = (
	home: FieldTeam | undefined,
	away: FieldTeam | undefined,
	neutralSite = false,
	venue: Venue = venueFor(home),
): FieldStyle => {
	const grass = venue.turf === "grass";
	const turf = grass ? "#3f7d34" : "#2f8a3d";
	const left = endZoneOf(home, "#8c1d40", venue.endZones, turf);
	const right = neutralSite
		? endZoneOf(away, "#1d3461", venue.endZones, turf)
		: left;
	return {
		turf,
		stripe: grass ? 0.075 : 0.04,
		grain: grass ? 14 : 7,
		lines: "#f7f7f2",
		surround: grass ? "#356b2d" : "#2b7f38",
		endZones: [left, right],
		numbers: "#f7f7f2",
	};
};

// A picture on the ground: its top left corner at (x0, y0), `w` by `h`
// feet, painted `ppf` pixels to a foot.
const groundPlane = (
	x0: number,
	y0: number,
	w: number,
	h: number,
	ppf: number,
): TexturedPlane => ({
	origin: { x: x0, y: y0, z: 0 } satisfies Pt3,
	alongX: { x: 1 / ppf, y: 0, z: 0 },
	alongY: { x: 0, y: 1 / ppf, z: 0 },
	w: Math.round(w * ppf),
	h: Math.round(h * ppf),
});

const TURF_PPF = 2;
export const TURF_PLANE = groundPlane(
	-END_ROOM,
	-SIDE_ROOM,
	FIELD_L + 2 * END_ROOM,
	FIELD_W + 2 * SIDE_ROOM,
	TURF_PPF,
);
const END_PPF = 6;
export const END_ZONE_PLANES: [TexturedPlane, TexturedPlane] = [
	groundPlane(0, 0, END_ZONE, FIELD_W, END_PPF),
	groundPlane(GOAL_R, 0, END_ZONE, FIELD_W, END_PPF),
];
const LOGO_FT = 30;
const LOGO_PPF = 8;
export const MID_LOGO_PLANE = groundPlane(
	MID_X - LOGO_FT / 2,
	MID_Y - LOGO_FT / 2,
	LOGO_FT,
	LOGO_FT,
	LOGO_PPF,
);

const canvasOf = (w: number, h: number): HTMLCanvasElement => {
	const c = document.createElement("canvas");
	c.width = w;
	c.height = h;
	return c;
};

// The grass: past the border all round in the surround's green, inside it
// mowed in stripes five yards wide - across the end zones too, under their
// paint - with a grain of lighter and darker blades so it reads as turf, not
// a sheet of green.
export const paintTurf = (style: FieldStyle, seed: string) => {
	const P = TURF_PLANE;
	const c = canvasOf(P.w, P.h);
	const g = c.getContext("2d")!;
	g.fillStyle = style.surround;
	g.fillRect(0, 0, P.w, P.h);
	const X = (x: number) => (x + END_ROOM) * TURF_PPF;
	const Y = (y: number) => (y + SIDE_ROOM) * TURF_PPF;
	const light = mix(style.turf, "#ffffff", style.stripe);
	for (let i = 0; i < 24; i++) {
		g.fillStyle = i % 2 === 0 ? style.turf : light;
		g.fillRect(
			X(i * 5 * YD),
			Y(0),
			5 * YD * TURF_PPF + 0.5,
			FIELD_W * TURF_PPF,
		);
	}
	const rng = makeCourtRng(`turf|${seed}`);
	const img = g.getImageData(0, 0, P.w, P.h);
	const d = img.data;
	for (let i = 0; i < d.length; i += 4) {
		const n = (rng() - 0.5) * style.grain;
		d[i] = Math.max(0, Math.min(255, d[i]! + n * 0.6));
		d[i + 1] = Math.max(0, Math.min(255, d[i + 1]! + n));
		d[i + 2] = Math.max(0, Math.min(255, d[i + 2]! + n * 0.4));
	}
	g.putImageData(img, 0, 0);
	return c;
};

// The end zone at `side` (0 left, 1 right): its color, a darker band along
// the end line, and the team's name in big block letters reading up the
// field from each side, the way they are painted - the left one's bottom
// toward the goal line.
export const paintEndZone = (style: FieldStyle, side: 0 | 1) => {
	const z = style.endZones[side];
	const P = END_ZONE_PLANES[side];
	const c = canvasOf(P.w, P.h);
	const g = c.getContext("2d")!;
	// (Left green, the grass shows through all but the letters.)
	if (z.pattern !== "green") {
		g.fillStyle = z.fill;
		g.fillRect(0, 0, P.w, P.h);
		if (z.pattern === "striped") {
			// Bands two yards wide, corner to corner, a shade darker.
			const band = 2 * YD * END_PPF;
			g.fillStyle = shade(z.fill, -0.2);
			for (let x = -P.h; x < P.w + P.h; x += band * 2) {
				g.beginPath();
				g.moveTo(x, 0);
				g.lineTo(x + band, 0);
				g.lineTo(x + band + P.h, P.h);
				g.lineTo(x + P.h, P.h);
				g.closePath();
				g.fill();
			}
		}
		// (The back of the end zone a shade darker, so a whole block of color
		// still reads as ground.)
		const back = g.createLinearGradient(
			side === 0 ? 0 : P.w,
			0,
			side === 0 ? P.w : 0,
			0,
		);
		back.addColorStop(0, shade(z.fill, -0.1));
		back.addColorStop(1, z.fill);
		g.fillStyle = back;
		g.globalAlpha = 0.6;
		g.fillRect(0, 0, P.w, P.h);
		g.globalAlpha = 1;
	}
	if (z.text) {
		// Letters 7 yards tall, run across the end zone between the hash
		// marks and out toward the numbers.
		const tall = 7 * YD * END_PPF;
		const room = (FIELD_W - 16 * YD) * END_PPF;
		g.save();
		g.translate(P.w / 2, P.h / 2);
		g.rotate(side === 0 ? -Math.PI / 2 : Math.PI / 2);
		g.font = `900 ${tall}px "Arial Black", "Helvetica Neue", Arial, sans-serif`;
		g.textAlign = "center";
		g.textBaseline = "middle";
		g.lineJoin = "round";
		const width = g.measureText(z.text).width;
		const squeeze = width > room ? room / width : 1;
		g.scale(squeeze, 1);
		g.lineWidth = tall * 0.09;
		g.strokeStyle = z.edge;
		g.strokeText(z.text, 0, tall * 0.04);
		g.fillStyle = z.textColor;
		g.fillText(z.text, 0, tall * 0.04);
		g.restore();
	}
	return c;
};

// The home logo at midfield, a little faded so it reads as paint on grass.
export const paintMidLogo = (logo: HTMLCanvasElement | HTMLImageElement) => {
	const P = MID_LOGO_PLANE;
	const c = canvasOf(P.w, P.h);
	const g = c.getContext("2d")!;
	const w = logo.width;
	const h = logo.height;
	if (w > 0 && h > 0) {
		const k = Math.min(P.w / w, P.h / h) * 0.94;
		g.globalAlpha = 0.9;
		g.drawImage(logo, (P.w - w * k) / 2, (P.h - h * k) / 2, w * k, h * k);
	}
	return c;
};

// THE NUMBERS: every ten yards, both sides, six feet tall with their bottoms
// twelve yards in from the sideline - each set facing its own sideline, so
// the far side's read upside down from the press box, as they do - with the
// little arrow beside each pointing to the nearer goal line.
export type NumberMark = { plane: TexturedPlane; img: HTMLCanvasElement };
const NUM_PPF = 14;
const NUM_ROOM = 10;
export const paintNumbers = (style: FieldStyle): NumberMark[] => {
	const out: NumberMark[] = [];
	const cache = new Map<string, HTMLCanvasElement>();
	const pic = (label: string, arrow: -1 | 0 | 1, flip: boolean) => {
		const key = `${label}|${arrow}|${flip}`;
		let c = cache.get(key);
		if (!c) {
			const w = NUM_ROOM * 2 * NUM_PPF;
			const h = NUMBER_H * NUM_PPF;
			c = canvasOf(w, h);
			const g = c.getContext("2d")!;
			if (flip) {
				g.translate(w, h);
				g.rotate(Math.PI);
			}
			g.fillStyle = style.numbers;
			g.font = `700 ${h * 1.02}px "Clarendon", "Rockwell", "Georgia", serif`;
			g.textAlign = "center";
			g.textBaseline = "alphabetic";
			// Each digit four feet wide, a foot and a half apart.
			const digits = label.split("");
			const pitch = (NUMBER_W + 1.5) * NUM_PPF;
			digits.forEach((dg, i) => {
				const cx = w / 2 + (i - (digits.length - 1) / 2) * pitch;
				g.save();
				g.translate(cx, h);
				const m = g.measureText(dg).width || 1;
				g.scale((NUMBER_W * NUM_PPF) / m, 1);
				g.fillText(dg, 0, -h * 0.02);
				g.restore();
			});
			if (arrow !== 0) {
				// The arrow: a small triangle off the outside digit, at the top
				// of the numbers, pointing at the goal line.
				const half = (digits.length * pitch) / 2;
				const ax = w / 2 + arrow * (half + 0.9 * NUM_PPF);
				const ay = h * 0.18;
				g.beginPath();
				g.moveTo(ax + arrow * 1.2 * NUM_PPF, ay);
				g.lineTo(ax, ay - 0.6 * NUM_PPF);
				g.lineTo(ax, ay + 0.6 * NUM_PPF);
				g.closePath();
				g.fill();
			}
			cache.set(key, c);
		}
		return c;
	};
	for (let yl = 10; yl <= 90; yl += 10) {
		const shown = yl <= 50 ? yl : 100 - yl;
		const x = GOAL_L + yl * YD;
		// Toward the nearer goal line, as the field runs left to right.
		const toward: -1 | 0 | 1 = yl < 50 ? -1 : yl > 50 ? 1 : 0;
		for (const far of [false, true]) {
			// Near side: the bottom of the number toward the near sideline.
			const yBottom = far ? NUMBER_IN : FIELD_W - NUMBER_IN;
			const y0 = far ? yBottom : yBottom - NUMBER_H;
			out.push({
				plane: groundPlane(x - NUM_ROOM, y0, 2 * NUM_ROOM, NUMBER_H, NUM_PPF),
				img: pic(String(shown), far ? (-toward as -1 | 0 | 1) : toward, far),
			});
		}
	}
	return out;
};
