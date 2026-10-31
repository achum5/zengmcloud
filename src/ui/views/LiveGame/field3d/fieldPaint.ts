import type { StadiumLook } from "../../../../common/types.ts";
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
	// Its home stadium as it has set it (the home team's, that is).
	stadium?: StadiumLook;
};

const endZoneOf = (
	team: FieldTeam | undefined,
	fallback: string,
	pattern: Venue["endZones"],
	turf: string,
): EndZoneStyle => {
	// (What the team has set for them over what its colors and name make.)
	const own = team?.stadium;
	const fill = own?.endZoneColor ?? team?.colors?.[0] ?? fallback;
	const second = team?.colors?.[1] ?? "#ffffff";
	const third = team?.colors?.[2] ?? "#ffffff";
	const text =
		own?.endZoneText ?? (team?.name ?? team?.abbrev ?? "").toUpperCase();
	if (pattern === "green") {
		// On the grass: lettered in the team's color, if it shows on green,
		// edged in another.
		const textColor =
			own?.endZoneTextColor ??
			[fill, second, third].find((c) => contrast(c, turf) > 0.18) ??
			"#ffffff";
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
		own?.endZoneTextColor ??
		(Math.abs(luminance(second) - luminance(fill)) > 0.25
			? second
			: light
				? "#141414"
				: "#ffffff");
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
	// (A field of its own color, the ground round it a shade darker - and
	// painted in black on one too light for white.)
	const own = home?.stadium?.turfColor;
	const turf = own ?? (grass ? "#3f7d34" : "#2f8a3d");
	const paint =
		home?.stadium?.lineColor ?? (luminance(turf) > 0.6 ? "#1b1b1d" : "#f7f7f2");
	const left = endZoneOf(home, "#8c1d40", venue.endZones, turf);
	const right = neutralSite
		? endZoneOf(away, "#1d3461", venue.endZones, turf)
		: left;
	return {
		turf,
		stripe: grass ? 0.075 : 0.04,
		grain: grass ? 14 : 7,
		lines: paint,
		surround:
			home?.stadium?.sidelineColor ??
			(own ? shade(own, -0.12) : grass ? "#356b2d" : "#2b7f38"),
		endZones: [left, right],
		numbers: paint,
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
// toward the goal line. (Or the team's own picture there instead - see
// StadiumLook.)
export const paintEndZone = (
	style: FieldStyle,
	side: 0 | 1,
	picture?: HTMLCanvasElement | HTMLImageElement,
) => {
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
	// Letters 7 yards tall, run across the end zone between the hash marks
	// and out toward the numbers - a picture as big as it fits there.
	const tall = 7 * YD * END_PPF;
	const room = (FIELD_W - 16 * YD) * END_PPF;
	if (picture && picture.width > 0 && picture.height > 0) {
		const k = Math.min(room / picture.width, (tall * 1.15) / picture.height);
		g.save();
		g.translate(P.w / 2, P.h / 2);
		g.rotate(side === 0 ? -Math.PI / 2 : Math.PI / 2);
		g.drawImage(
			picture,
			(-picture.width * k) / 2,
			(-picture.height * k) / 2,
			picture.width * k,
			picture.height * k,
		);
		g.restore();
	} else if (z.text) {
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

// THE CHAMPIONSHIP GAME'S MIDFIELD (the 3D court's finals: see LiveCourt's
// trophy at center court): in place of the logo, a silver trophy painted big
// - a football up on its point on a tapering stand - the home logo in front
// of its base. Laid out in feet across the plane, its top the far side.
const TROPHY_W = 34;
const TROPHY_D = 56;
export const MID_TROPHY_PLANE = groundPlane(
	MID_X - TROPHY_W / 2,
	MID_Y - TROPHY_D / 2,
	TROPHY_W,
	TROPHY_D,
	LOGO_PPF,
);
// The ball: half as long and half as fat (ft), leaning over this far
// (rad), its point down on the top of the stand; the stand from there down
// to its base, this wide at either end; the base, this deep; the logo,
// this big, its middle this far down.
const BALL_A = 10.5;
const BALL_B = 5.6;
const BALL_LEAN = 0.22;
const STAND_TOP = 22.5;
const STAND_BOTTOM = 44.5;
const STAND_W: [number, number] = [1.6, 9];
const BASE_D = 3.5;
const TROPHY_LOGO = 16;
const TROPHY_LOGO_AT = 46;
export const paintMidTrophy = (
	logo: HTMLCanvasElement | HTMLImageElement | undefined,
) => {
	const P = MID_TROPHY_PLANE;
	const c = canvasOf(P.w, P.h);
	const g = c.getContext("2d")!;
	const k = P.w / TROPHY_W;
	g.scale(k, k);
	g.globalAlpha = 0.92;
	g.lineJoin = "round";
	const cx = TROPHY_W / 2;
	const silver = (x0: number, x1: number) => {
		const grad = g.createLinearGradient(x0, 0, x1, 0);
		grad.addColorStop(0, "#eef1f4");
		grad.addColorStop(0.45, "#b9c1c8");
		grad.addColorStop(1, "#77818a");
		return grad;
	};
	const edge = "#4f5860";

	// The base, and the stand up off it: its two faces this way narrowing,
	// concave, up to the ball - the left lit, the right in shade.
	const [wt, wb] = STAND_W;
	g.beginPath();
	g.roundRect(cx - wb / 2 - 0.9, STAND_BOTTOM, wb + 1.8, BASE_D, 0.6);
	g.fillStyle = silver(cx - wb / 2, cx + wb / 2);
	g.fill();
	g.lineWidth = 0.3;
	g.strokeStyle = edge;
	g.stroke();
	const midY = (STAND_TOP + STAND_BOTTOM) / 2;
	const side = (s: -1 | 1) => {
		g.moveTo(cx, STAND_TOP);
		g.lineTo(cx + (s * wt) / 2, STAND_TOP);
		g.quadraticCurveTo(
			cx + (s * (wt + wb)) / 6,
			midY + 4,
			cx + (s * wb) / 2,
			STAND_BOTTOM,
		);
		g.lineTo(cx, STAND_BOTTOM);
		g.closePath();
	};
	for (const s of [-1, 1] as const) {
		g.beginPath();
		side(s);
		g.fillStyle =
			s < 0 ? silver(cx - wb / 2, cx + wb) : silver(cx - wb, cx + wb / 2);
		g.fill();
	}
	g.beginPath();
	side(-1);
	side(1);
	g.stroke();
	g.beginPath();
	g.moveTo(cx, STAND_TOP);
	g.lineTo(cx, STAND_BOTTOM);
	g.lineWidth = 0.18;
	g.strokeStyle = "#ffffff";
	g.stroke();

	// The ball, its point down on the stand.
	const down = { x: Math.sin(BALL_LEAN), y: Math.cos(BALL_LEAN) };
	const bx = cx - down.x * BALL_A;
	const by = STAND_TOP - down.y * BALL_A;
	g.save();
	g.translate(bx, by);
	g.rotate(-BALL_LEAN);
	// (A football's ends come to points, not round: two arcs, tip to tip.)
	const r = (BALL_A * BALL_A + BALL_B * BALL_B) / (2 * BALL_B);
	const half = Math.asin(BALL_A / r);
	g.beginPath();
	g.arc(r - BALL_B, 0, r, Math.PI - half, Math.PI + half);
	g.arc(BALL_B - r, 0, r, -half, half);
	g.closePath();
	g.fillStyle = silver(-BALL_B, BALL_B);
	g.fill();
	g.lineWidth = 0.3;
	g.strokeStyle = edge;
	g.stroke();
	// A shine down its lit side; the laces up the middle.
	g.beginPath();
	g.ellipse(
		-BALL_B * 0.45,
		-BALL_A * 0.1,
		BALL_B * 0.18,
		BALL_A * 0.55,
		0,
		0,
		2 * Math.PI,
	);
	g.fillStyle = "rgba(255, 255, 255, 0.55)";
	g.fill();
	g.strokeStyle = "#6d767e";
	g.lineWidth = 0.35;
	g.beginPath();
	g.moveTo(0, -BALL_A * 0.42);
	g.lineTo(0, BALL_A * 0.3);
	g.stroke();
	g.lineWidth = 0.28;
	for (let i = 0; i < 7; i++) {
		const y = -BALL_A * 0.36 + (i * BALL_A * 0.6) / 6;
		g.beginPath();
		g.moveTo(-0.9, y);
		g.lineTo(0.9, y);
		g.stroke();
	}
	g.restore();

	// The home logo, in front of the base.
	if (logo && logo.width > 0 && logo.height > 0) {
		const s = TROPHY_LOGO / Math.max(logo.width, logo.height);
		const w = logo.width * s;
		const h = logo.height * s;
		g.globalAlpha = 0.9;
		g.drawImage(logo, cx - w / 2, TROPHY_LOGO_AT - h / 2, w, h);
	}
	return c;
};

// Or the team's own picture of it (see StadiumLook), stood up the field as
// the drawn one is - laid on the grass whole, as the logo is.
export const paintTrophyPicture = (
	picture: HTMLCanvasElement | HTMLImageElement,
) => {
	const P = MID_TROPHY_PLANE;
	const c = canvasOf(P.w, P.h);
	const g = c.getContext("2d")!;
	const w = picture.width;
	const h = picture.height;
	if (w > 0 && h > 0) {
		const k = Math.min(P.w / w, P.h / h) * 0.96;
		g.drawImage(picture, (P.w - w * k) / 2, (P.h - h * k) / 2, w * k, h * k);
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
