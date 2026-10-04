import type { FaceConfig } from "facesjs";
import {
	poseFor,
	skeleton,
	type AnimName,
	type Body,
	type Pose,
} from "./poses.ts";

// PIXEL PLAYERS, DRAWN FROM WHAT THE GAME ALREADY KNOWS.
//
// No sprite sheets: every frame is drawn in code from the player's skeleton,
// in his own colors - skin and hair from his facesjs face, his team's uniform,
// his jersey number - then outlined. A frame is drawn once and cached.

export type HairStyle =
	| "bald"
	| "buzz"
	| "short"
	| "fade"
	| "curly"
	| "afro"
	| "locs"
	| "long"
	| "spiky";

export type Uniform = {
	jersey: string;
	trim: string;
	numberColor: string;
	shoe: string;
};

export type Look = Uniform & {
	skin: string;
	hair: HairStyle;
	hairColor: string;
	beard: "none" | "goatee" | "full";
	headband: boolean;
	number: string;
};

// facesjs hair ids, by what they look like from thirty feet.
export const hairStyleFor = (id: string | undefined): HairStyle => {
	if (!id) {
		return "short";
	}
	if (id.startsWith("afro")) {
		return "afro";
	}
	if (id === "bald") {
		return "bald";
	}
	if (id === "short-bald") {
		return "buzz";
	}
	if (id === "dreads" || id === "cornrows") {
		return "locs";
	}
	if (id.startsWith("curly")) {
		return "curly";
	}
	if (id.includes("fade") || id === "crop") {
		return "fade";
	}
	if (id.startsWith("spike") || id.includes("hawk")) {
		return "spiky";
	}
	if (
		id.startsWith("female") ||
		id.startsWith("shaggy") ||
		id === "longHair" ||
		id === "emo"
	) {
		return "long";
	}
	return "short";
};

const SKINS = [
	"#f2d6cb",
	"#ddb7a0",
	"#ce967d",
	"#bb876f",
	"#a67358",
	"#74453d",
];
const HAIRS = ["#1a1110", "#2b1d16", "#3d2b1f", "#6b4a2b", "#a07040"];

export const lookFor = ({
	pid,
	face,
	uniform,
	jerseyNumber,
}: {
	pid: number;
	face: FaceConfig | undefined;
	uniform: Uniform;
	jerseyNumber: string | undefined;
}): Look => {
	const facial = face?.facialHair?.id ?? "none";
	return {
		...uniform,
		skin: face?.body?.color ?? SKINS[pid % SKINS.length]!,
		hair: face ? hairStyleFor(face.hair?.id) : "short",
		hairColor: face?.hair?.color ?? HAIRS[pid % HAIRS.length]!,
		beard:
			facial === "none" || facial === ""
				? "none"
				: facial.includes("goatee") && !facial.startsWith("full")
					? "goatee"
					: "full",
		headband: face?.accessories?.id?.startsWith("headband") ?? false,
		number: (jerseyNumber ?? "").replaceAll(/\D/g, "").slice(0, 2),
	};
};

const hex = (h: string): [number, number, number] => {
	const m = /^#?([\da-f]{6})$/i.exec(h.trim());
	const n = m ? Number.parseInt(m[1]!, 16) : 0x888888;
	return [(n >> 16) & 255, (n >> 8) & 255, n & 255];
};
const luminance = (h: string): number => {
	const [r, g, b] = hex(h);
	return (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255;
};

// Home in white, the road team in its colors - unless the road colors are
// light too, in which case it wears its darkest one.
export const uniformsFor = (
	away: [string, string, string] | undefined,
	home: [string, string, string] | undefined,
): [Uniform, Uniform] => {
	const a = away ?? ["#1d3461", "#f28c28", "#ffffff"];
	const h = home ?? ["#8c1d40", "#f2c14e", "#ffffff"];
	const darkest = [...a].sort((x, y) => luminance(x) - luminance(y))[0]!;
	const roadJersey = luminance(a[0]) < 0.6 ? a[0] : darkest;
	const roadTrim =
		a.find(
			(c) =>
				c !== roadJersey &&
				Math.abs(luminance(c) - luminance(roadJersey)) > 0.25,
		) ?? "#ffffff";
	const homeTrim =
		luminance(h[0]) < 0.7
			? h[0]
			: (h.find((c) => luminance(c) < 0.6) ?? "#222222");
	return [
		{
			jersey: roadJersey,
			trim: roadTrim,
			numberColor: luminance(roadJersey) < 0.45 ? "#ffffff" : "#111111",
			shoe: "#f2f2f2",
		},
		{
			jersey: "#f3efe6",
			trim: homeTrim,
			numberColor: homeTrim,
			shoe: "#1b1820",
		},
	];
};

// 3x5 pixel glyphs: jersey numbers, name tags, the floor and the ad boards.
const FONT: Record<string, string> = {
	A: "010101111101101",
	B: "110101110101110",
	C: "011100100100011",
	D: "110101101101110",
	E: "111100110100111",
	F: "111100110100100",
	G: "011100101101011",
	H: "101101111101101",
	I: "111010010010111",
	J: "001001001101010",
	K: "101101110101101",
	L: "100100100100111",
	M: "101111111101101",
	N: "110101101101101",
	O: "010101101101010",
	P: "110101110100100",
	Q: "010101101110011",
	R: "110101110101101",
	S: "011100010001110",
	T: "111010010010010",
	U: "101101101101111",
	V: "101101101101010",
	W: "101101111111101",
	X: "101101010101101",
	Y: "101101010010010",
	Z: "111001010100111",
	"0": "111101101101111",
	"1": "010110010010111",
	"2": "110001010100111",
	"3": "110001010001110",
	"4": "101101111001001",
	"5": "111100110001110",
	"6": "011100111101111",
	"7": "111001010010010",
	"8": "111101111101111",
	"9": "111101111001110",
	"-": "000000111000000",
	".": "000000000000010",
	"'": "010010000000000",
	" ": "000000000000000",
};
const glyph = (ch: string): string => FONT[ch.toUpperCase()] ?? FONT[" "]!;
export const pixelTextWidth = (s: string, sx = 1): number =>
	Math.max(0, s.length * 4 - 1) * sx;
export const drawPixelText = (
	g: CanvasRenderingContext2D,
	s: string,
	x: number,
	y: number,
	color: string,
	sx = 1,
	sy = sx,
) => {
	g.fillStyle = color;
	for (let i = 0; i < s.length; i++) {
		const gl = glyph(s[i]!);
		for (let r = 0; r < 5; r++) {
			for (let c = 0; c < 3; c++) {
				if (gl[r * 3 + c] === "1") {
					g.fillRect(x + (i * 4 + c) * sx, y + r * sy, sx, sy);
				}
			}
		}
	}
};

const OUTLINE: [number, number, number] = [22, 18, 30];
const shade = (c: [number, number, number], f: number) =>
	c.map((v) => Math.max(0, Math.min(255, Math.round(v * f)))) as [
		number,
		number,
		number,
	];
const mix = (
	a: [number, number, number],
	b: [number, number, number],
	f: number,
) =>
	a.map((v, i) => Math.round(v + (b[i]! - v) * f)) as [number, number, number];

export type Sprite = {
	canvas: HTMLCanvasElement;
	// The point between the feet, in canvas pixels.
	ax: number;
	ay: number;
	// Height of the top of the drawing above the feet (for a name tag).
	top: number;
};

const rasterize = (look: Look, b: Body, q: Pose): Sprite => {
	const sk = skeleton(b, q);
	const W = Math.ceil(b.H * 1.3) + 8;
	const Hc = Math.ceil(b.H * 1.34) + 8;
	const ax = Math.floor(W / 2);
	const ay = Hc - 3;
	const data = new Uint8ClampedArray(W * Hc * 4);
	const put = (x: number, y: number, c: [number, number, number]) => {
		const cx = Math.round(ax + x);
		const cy = Math.round(ay - y);
		if (cx < 0 || cy < 0 || cx >= W || cy >= Hc) {
			return;
		}
		const i = (cy * W + cx) * 4;
		data[i] = c[0];
		data[i + 1] = c[1];
		data[i + 2] = c[2];
		data[i + 3] = 255;
	};
	const stamp = (
		x: number,
		y: number,
		th: number,
		c: [number, number, number],
	) => {
		const o = (th - 1) / 2;
		for (let dx = 0; dx < th; dx++) {
			for (let dy = 0; dy < th; dy++) {
				put(x - o + dx, y - o + dy, c);
			}
		}
	};
	type V = { x: number; y: number };
	const seg = (
		p0: V,
		p1: V,
		th: number,
		c: [number, number, number],
		from = 0,
		to = 1,
	) => {
		const d = Math.hypot(p1.x - p0.x, p1.y - p0.y);
		const n = Math.max(1, Math.ceil(d * 2));
		for (let i = Math.floor(from * n); i <= Math.ceil(to * n); i++) {
			const f = i / n;
			stamp(p0.x + (p1.x - p0.x) * f, p0.y + (p1.y - p0.y) * f, th, c);
		}
	};

	const skin = hex(look.skin);
	const skinF = shade(skin, 0.78);
	const skinD = shade(skin, 0.68);
	const jersey = hex(look.jersey);
	const jerseyD = shade(jersey, 0.8);
	const jerseyF = shade(jersey, 0.74);
	const trim = hex(look.trim);
	const numC = hex(look.numberColor);
	const sock: [number, number, number] = [242, 240, 234];
	const sockF = shade(sock, 0.8);
	const shoe = hex(look.shoe);
	const shoeF = shade(shoe, 0.8);
	const hairC = hex(look.hairColor);
	const hairL = mix(hairC, [255, 255, 255], 0.16);

	const drawArm = (
		a: (typeof sk)["armN"],
		c: [number, number, number],
		outline: boolean,
	) => {
		if (outline) {
			seg(a.s0, a.elbow, b.armT + 2, OUTLINE);
			seg(a.elbow, a.hand, b.armT + 2, OUTLINE);
			stamp(a.hand.x, a.hand.y, 4, OUTLINE);
		}
		seg(a.s0, a.elbow, b.armT, c);
		seg(a.elbow, a.hand, b.armT, c);
		stamp(a.hand.x, a.hand.y, 2, c);
	};
	const drawLeg = (
		l: (typeof sk)["legN"],
		c: [number, number, number],
		sockC: [number, number, number],
		shoeC: [number, number, number],
		shortsC: [number, number, number],
		outline: boolean,
	) => {
		if (outline) {
			seg(sk.hip, l.knee, b.legT + 2, OUTLINE);
			seg(l.knee, l.ankle, b.legT + 2, OUTLINE);
		}
		seg(sk.hip, l.knee, b.legT, c);
		seg(l.knee, l.ankle, b.legT, c, 0, 0.62);
		seg(l.knee, l.ankle, b.legT, sockC, 0.62, 1);
		seg(sk.hip, l.knee, b.legT + 1, shortsC, 0, 0.5);
		const fl = Math.max(2, Math.round(b.H * 0.06));
		for (let dx = -1; dx <= fl; dx++) {
			put(l.ankle.x + dx, l.ankle.y - 1, shoeC);
			if (dx < fl) {
				put(l.ankle.x + dx, l.ankle.y - 2, shoeC);
			}
		}
	};

	drawArm(sk.armF, skinF, false);
	drawLeg(sk.legF, skinF, sockF, shoeF, jerseyF, false);
	drawLeg(sk.legN, skin, sock, shoe, jerseyD, true);

	// Shorts and jersey: one sheared column from the hips to the shoulders.
	const shortsH = Math.round(b.H * 0.1);
	for (
		let y = Math.round(sk.hip.y - shortsH);
		y <= Math.round(sk.shoulder.y);
		y++
	) {
		const f = Math.min(
			1,
			Math.max(0, (y - sk.hip.y) / (sk.shoulder.y - sk.hip.y)),
		);
		const cx = sk.hip.x + (sk.shoulder.x - sk.hip.x) * f;
		const half = b.torsoW / 2 - (f > 0.15 && f < 0.5 ? 0.5 : 0);
		const x0 = Math.round(cx - half);
		const x1 = Math.round(cx + half) - 1;
		for (let x = x0; x <= x1; x++) {
			put(x, y, x === x0 ? jerseyD : jersey);
		}
		if (y === Math.round(sk.hip.y)) {
			for (let x = x0; x <= x1; x++) {
				put(x, y, trim);
			}
		}
		if (y < sk.hip.y) {
			put(x0 + 1, y, trim);
		}
	}
	const topY = Math.round(sk.shoulder.y);
	for (const dx of [-1, 0, 1]) {
		put(sk.shoulder.x + dx, topY, trim);
	}
	put(sk.shoulder.x, topY - 1, trim);

	// The number, if he has one that fits.
	const num = look.number;
	if (num) {
		const nw = num.length * 4 - 1;
		const midY = Math.round(sk.hip.y + (sk.shoulder.y - sk.hip.y) * 0.72);
		const midX = Math.round(
			sk.hip.x + (sk.shoulder.x - sk.hip.x) * 0.72 - nw / 2 + 0.5,
		);
		for (let i = 0; i < num.length; i++) {
			const gl = glyph(num[i]!);
			for (let r = 0; r < 5; r++) {
				for (let c = 0; c < 3; c++) {
					if (gl[r * 3 + c] === "1") {
						put(midX + i * 4 + c, midY - r, numC);
					}
				}
			}
		}
	}

	// Head.
	const hh = b.headH;
	const hw = b.headW;
	const hx0 = Math.round(sk.headC.x - hw / 2);
	const hx1 = hx0 + hw - 1;
	const hy0 = Math.round(sk.headC.y - hh / 2);
	const hy1 = hy0 + hh - 1; // y is up: hy1 is the top row
	for (const dx of [-1, 0, 1]) {
		put(Math.round(sk.shoulder.x) + dx, topY + 1, skinD);
	}
	for (let y = hy0; y <= hy1; y++) {
		for (let x = hx0; x <= hx1; x++) {
			const corner = (y === hy0 || y === hy1) && (x === hx0 || x === hx1);
			if (!corner) {
				put(x, y, skin);
			}
		}
	}
	const eyeY = hy1 - Math.round(hh * 0.42);
	put(hx1 - 1, eyeY, [24, 18, 22]);
	put(hx1 + 1, eyeY - 2, skin);
	put(hx1, eyeY - 3, skinD);
	put(hx0 + Math.round(hw * 0.4), eyeY - 1, skinD);

	const row = (
		y: number,
		xa: number,
		xb: number,
		c: [number, number, number],
	) => {
		for (let x = xa; x <= xb; x++) {
			put(x, y, c);
		}
	};
	switch (look.hair) {
		case "short":
		case "fade":
		case "spiky": {
			row(hy1, hx0, hx1 - 1, hairL);
			row(hy1 - 1, hx0, look.hair === "fade" ? hx1 - 3 : hx1 - 1, hairC);
			for (let y = eyeY + (look.hair === "fade" ? 1 : 0); y < hy1 - 1; y++) {
				put(hx0, y, hairC);
			}
			if (look.hair === "spiky") {
				for (let x = hx0; x < hx1; x += 2) {
					put(x, hy1 + 1, hairC);
				}
			}
			break;
		}
		case "buzz": {
			row(hy1, hx0 + 1, hx1 - 1, mix(hairC, skin, 0.45));
			break;
		}
		case "curly": {
			row(hy1 + 1, hx0, hx1 - 1, hairC);
			row(hy1, hx0 - 1, hx1, hairL);
			row(hy1 - 1, hx0 - 1, hx1 - 2, hairC);
			put(hx0 - 1, hy1 - 2, hairC);
			break;
		}
		case "afro": {
			const cx = hx0 + hw / 2 - 1;
			const cy = hy1 - 0.5;
			const rx = hw / 2 + 1.8;
			const ry = 3.6;
			for (let y = eyeY; y <= hy1 + 3; y++) {
				for (let x = hx0 - 2; x <= hx1 + 1; x++) {
					if (x >= hx1 - 1 && y <= hy1 - 2) {
						continue;
					}
					if (((x - cx) / rx) ** 2 + ((y - cy) / ry) ** 2 <= 1) {
						put(x, y, (x + y) % 3 === 0 ? hairL : hairC);
					}
				}
			}
			break;
		}
		case "locs": {
			row(hy1 + 1, hx0, hx1 - 2, hairC);
			row(hy1, hx0 - 1, hx1 - 1, hairL);
			row(hy1 - 1, hx0 - 1, hx1 - 2, hairC);
			for (let k = 0; k < 3; k++) {
				for (let y = hy1 - 2; y >= hy0 - 3 + k; y--) {
					put(hx0 - 1 + k * 2, y, k % 2 ? hairL : hairC);
				}
			}
			break;
		}
		case "long": {
			row(hy1 + 1, hx0, hx1 - 2, hairC);
			row(hy1, hx0 - 1, hx1, hairL);
			for (let y = hy0 - 2; y < hy1; y++) {
				put(hx0 - 1, y, hairC);
				put(hx0, y, hairC);
				if (y > eyeY) {
					put(hx0 + 1, y, hairL);
				}
			}
			break;
		}
		case "bald": {
			put(hx0 + 1, hy1, mix(skin, [255, 255, 255], 0.3));
			break;
		}
	}
	if (look.headband) {
		row(hy1 - 1, hx0 - 1, hx1, trim);
	}
	if (look.beard === "full") {
		row(hy0, hx0 + 2, hx1 - 1, hairC);
		row(hy0 + 1, hx1 - 2, hx1, hairC);
		put(hx0 + 2, hy0 + 1, hairC);
	} else if (look.beard === "goatee") {
		row(hy0, hx1 - 2, hx1 - 1, hairC);
	}

	drawArm(sk.armN, skin, true);

	// Outline: every empty pixel touching the body goes dark.
	const out = new Uint8ClampedArray(data);
	const solid = (x: number, y: number) =>
		x >= 0 && y >= 0 && x < W && y < Hc && data[(y * W + x) * 4 + 3]! > 0;
	for (let y = 0; y < Hc; y++) {
		for (let x = 0; x < W; x++) {
			const i = (y * W + x) * 4;
			if (data[i + 3]) {
				continue;
			}
			if (
				solid(x - 1, y) ||
				solid(x + 1, y) ||
				solid(x, y - 1) ||
				solid(x, y + 1)
			) {
				out[i] = OUTLINE[0];
				out[i + 1] = OUTLINE[1];
				out[i + 2] = OUTLINE[2];
				out[i + 3] = 255;
			}
		}
	}
	const canvas = document.createElement("canvas");
	canvas.width = W;
	canvas.height = Hc;
	canvas.getContext("2d")?.putImageData(new ImageData(out, W, Hc), 0, 0);
	return {
		canvas,
		ax,
		ay,
		top: Math.max(hy1 + 2, sk.armN.hand.y, sk.armF.hand.y),
	};
};

// Frames are drawn on first use and kept; a player's frames are thrown away
// when his look or build changes (his face finished loading, say).
export class SpriteCache {
	private readonly frames = new Map<string, Sprite>();
	private readonly keys = new Map<number, string>();
	private readonly lookKeys = new WeakMap<Look, string>();

	get(
		pid: number,
		look: Look,
		body: Body,
		anim: AnimName,
		frame: number,
	): Sprite {
		let lk = this.lookKeys.get(look);
		if (lk === undefined) {
			lk = JSON.stringify(look);
			this.lookKeys.set(look, lk);
		}
		const lookKey = `${lk}|${body.H.toFixed(2)}|${body.torsoW}`;
		if (this.keys.get(pid) !== lookKey) {
			for (const k of this.frames.keys()) {
				if (k.startsWith(`${pid}|`)) {
					this.frames.delete(k);
				}
			}
			this.keys.set(pid, lookKey);
		}
		const key = `${pid}|${anim}|${frame}`;
		let s = this.frames.get(key);
		if (!s) {
			s = rasterize(look, body, poseFor(anim, frame));
			this.frames.set(key, s);
		}
		return s;
	}
}
