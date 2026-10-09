import { parseUniform } from "../../../../common/uniform.ts";
import { makeCourtRng } from "../courtRng.ts";
import { project, type Camera, type Projected } from "./camera.ts";
import { bodyPoint, poseOf, type PlayerState } from "./evaluate.ts";
import type { HairCut, HeadSprite, Profile } from "./faces.ts";
import type { KitArt } from "./kitArt.ts";
import { skeleton, type Body, type V3 } from "./poses.ts";

// WHAT HE LOOKS LIKE - and his head (see sprite.ts).
//
// What a player wears - his team's uniform, his own gear - or an official, a
// coach or a photographer his clothes; his skin, his hair. His body is
// sculpted from it (see sculpt.ts); his head is drawn here: his BBGM face
// when it turns to the camera, his head in profile side on, the back of his
// head facing away.

export type Kit = {
	jersey: string;
	trim: string;
	// Lettering and numbers, and the edge round them.
	number: string;
	numberEdge: string;
	shorts: string;
	stripe: string;
	sock: string;
	shoe: string;
	sole: string;
	// His name across his back, if not in his number's color.
	name?: string;
	// The team's name across his chest: its color, if not his number's, and
	// what it says, if not the team's name (at home) or its city (away).
	chest?: string;
	chestText?: string;
};

// What a player wears besides the uniform, and makes him himself: a sleeve
// on his shooting arm, tights down a leg, a wristband, a knee pad, his own
// shoes and socks.
export type Gear = {
	sleeve?: { arms: "R" | "L" | "RL"; color: string; elbow: boolean };
	tights?: { legs: "R" | "L" | "RL"; color: string };
	wrist?: { arms: "R" | "L" | "RL"; color: string };
	knee?: { legs: "R" | "L"; color: string };
	shoe: string;
	sole: string;
	sock: string;
};

// Not a uniform: what the people on the floor who don't play wear (see
// crew.ts). Trousers are tights down both legs under shorts of the same
// color; the rest is here.
export type Outfit = {
	// Shirt sleeves, in the shirt's color: to the elbow, or the wrist.
	sleeves?: "short" | "long";
	// No zip at the neck of a long-sleeved top: a warm-up shirt.
	plain?: boolean;
	// Stripes down the shirt: a referee's.
	stripes?: string;
	// The shirt and tie in the open neck of a jacket: a coach's suit.
	shirt?: string;
	tie?: string;
	// A camera in his hands: a photographer's.
	camera?: boolean;
};
export const CAMERA_BODY = "#1c1d22";
export const CAMERA_LENS = "#34363e";

export type Look = {
	kit: Kit;
	// The team's uniform drawn from a picture (see kitArt.ts).
	kitArt?: KitArt;
	gear?: Gear;
	outfit?: Outfit;
	skin: string;
	hair: string;
	// How his hair sits on the back of his head (short, if unsaid).
	cut?: HairCut;
	// What shows of his face side on, beyond his skin and hair.
	profile?: Profile;
	// A player whose face is a photo: solid black, head to toe, no face -
	// only his uniform on him.
	silhouette?: boolean;
	jerseyNumber: string;
	// His full name, shown under him while he has the ball.
	name: string;
	// Across his back, over the number.
	lastName: string;
	// Across his chest: the team's name at home, the city on the road.
	wordmark: string;
	head?: HeadSprite;
};

type P2 = { x: number; y: number };

// His face is drawn a little above the true middle of his head (in head
// radii): from the camera up in the stands his chin would hide his neck, and
// a cartoon shows it.
const FACE_LIFT = 0.15;

// His head is inked round, the cartoon way - the back of it like the line
// round his face - this thick (feet, at his size on screen), in this.
const OUTLINE = 0.05;
const INK = "#17120f";

const lerp2 = (a: P2, b: P2, f: number): P2 => ({
	x: a.x + (b.x - a.x) * f,
	y: a.y + (b.y - a.y) * f,
});

// A closed shape through points, smoothed - for cloth.
const softPoly = (pts: P2[]): Path2D => {
	const p = new Path2D();
	const n = pts.length;
	const mid = (i: number) => lerp2(pts[i % n]!, pts[(i + 1) % n]!, 0.5);
	const m0 = mid(0);
	p.moveTo(m0.x, m0.y);
	for (let i = 1; i <= n; i++) {
		const c = pts[i % n]!;
		const m = mid(i);
		p.quadraticCurveTo(c.x, c.y, m.x, m.y);
	}
	p.closePath();
	return p;
};

const parse = (c: string): [number, number, number] => {
	const m = /^#?([\da-f]{6})$/i.exec(c.trim());
	if (m) {
		const n = Number.parseInt(m[1]!, 16);
		return [(n >> 16) & 255, (n >> 8) & 255, n & 255];
	}
	const r = /rgba?\(\s*([\d.]+)\s*,\s*([\d.]+)\s*,\s*([\d.]+)/i.exec(c);
	if (r) {
		return [Number(r[1]), Number(r[2]), Number(r[3])];
	}
	return [128, 128, 128];
};

// A color a little darker (f < 0) or lighter (f > 0).
const shades = new Map<string, string>();
// How light a color is, 0 (black) to 1 (white).
export const lightness = (c: string): number => {
	const [r, g, b] = parse(c);
	return (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255;
};

export const shade = (c: string, f: number): string => {
	const key = `${c}|${f}`;
	let out = shades.get(key);
	if (out === undefined) {
		const [r, g, b] = parse(c);
		const t = f < 0 ? 0 : 255;
		const k = Math.abs(f);
		const mix = (v: number) => Math.round(v + (t - v) * k);
		out = `rgb(${mix(r)}, ${mix(g)}, ${mix(b)})`;
		shades.set(key, out);
	}
	return out;
};

// The ball, when he has it in his hands: its leather and its seams. (Kept
// in step with the loose ball's colors in arena.ts.)
export const BALL_ORANGE = "#e2702a";
export const BALL_SEAM = "#3a1608";

// His head on its own, for a sprite to draw over his body.
export const drawHeadAt = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	st: PlayerState,
	body: Body,
	look: Look,
) => {
	const sk = skeleton(body, poseOf(st));
	const at = (v: V3): Projected => project(cam, bodyPoint(st, v));
	const toCamX = cam.pos.x - st.x;
	const toCamY = cam.pos.y - st.y;
	const toCamL = Math.hypot(toCamX, toCamY) || 1;
	const front =
		(Math.cos(st.yaw) * toCamX + Math.sin(st.yaw) * toCamY) / toCamL;
	const headC = at(sk.head);
	drawHead(
		ctx,
		headC,
		body.headR * headC.k,
		look,
		front,
		at,
		sk.head,
		OUTLINE * headC.k,
	);
};

// How far round to the camera he is (1 facing it, -1 away) when his head
// shows in profile, and when his face comes in over it - over this much more.
// (Drawn in sixteen turns, he shows his profile square on and a turn either
// side of it toward the camera, his face from two turns on.)
const PROFILE_FROM = -0.3;
const FACE_FROM = 0.5;
const FACE_IN = 0.04;

// A headband, side on: how high (head radii above the middle of his face) its
// top edge sits at his brow and at the back of his head, and how wide it is.
const BAND_FRONT = -0.58;
const BAND_BACK = -0.2;
const BAND_WIDE = 0.24;

// His head side on: the line of his brow, nose, lips and chin, an eye, his
// ear, and his hair - and beard, headband - as they sit on his skull, in his
// own skin and hair. `turn` is the way his nose points on screen.
const profileHead = (
	ctx: CanvasRenderingContext2D,
	c: P2,
	r: number,
	turn: 1 | -1,
	look: Look,
	ink: number,
) => {
	const P = (u: number, v: number): P2 => ({
		x: c.x + turn * u * r,
		y: c.y + v * r,
	});
	const cut = look.cut ?? "short";
	const hairy = cut !== "bald" && look.hair !== look.skin;
	const pro = look.profile ?? {};
	// Skull, brow, the bridge of his nose and its tip, lips, chin, the line
	// of his jaw, and round the back of his head.
	const head = softPoly([
		P(-0.14, -1.06),
		P(0.46, -1.0),
		P(0.78, -0.66),
		P(0.86, -0.32),
		P(0.92, -0.14),
		P(0.8, -0.03),
		P(0.94, 0.1),
		P(1.12, 0.22),
		P(0.92, 0.3),
		P(0.9, 0.34),
		P(0.94, 0.4),
		P(0.84, 0.47),
		P(0.92, 0.54),
		P(0.8, 0.64),
		P(0.9, 0.78),
		P(0.7, 0.92),
		P(0.26, 0.86),
		P(-0.08, 0.64),
		P(-0.58, 0.5),
		P(-1.0, 0.1),
		P(-0.9, -0.62),
	]);
	// A little bigger, for what grows on it.
	const grown = softPoly([
		P(-0.15, -1.12),
		P(0.5, -1.06),
		P(0.84, -0.68),
		P(0.92, -0.32),
		P(0.98, 0.0),
		P(1.0, 0.5),
		P(0.96, 0.82),
		P(0.72, 0.98),
		P(0.24, 0.92),
		P(-0.1, 0.7),
		P(-0.62, 0.56),
		P(-1.07, 0.12),
		P(-0.96, -0.66),
	]);
	const ear = new Path2D();
	ear.ellipse(
		P(-0.1, 0.08).x,
		P(-0.1, 0.08).y,
		r * 0.16,
		r * 0.26,
		0,
		0,
		Math.PI * 2,
	);
	const inner = new Path2D();
	inner.ellipse(
		P(-0.08, 0.09).x,
		P(-0.08, 0.09).y,
		r * 0.08,
		r * 0.16,
		0,
		0,
		Math.PI * 2,
	);
	// Where his hair ends: across his forehead, back over his ear, and down
	// to the nape of his neck - lower at the back the more there is of it.
	const nape = cut === "long" ? 1.25 : cut === "big" ? 0.62 : 0.46;
	const line = [
		P(0.74, -0.66),
		P(0.4, -0.56),
		P(0.1, -0.4),
		P(-0.2, -0.12),
		P(-0.36, 0.26),
		P(-0.56, nape),
		P(-1.6, nape + 0.2),
	];
	const above = new Path2D();
	above.moveTo(line[0]!.x, line[0]!.y);
	for (const q of line.slice(1)) {
		above.lineTo(q.x, q.y);
	}
	for (const q of [P(-1.6, -1.8), P(1.6, -1.8), P(1.6, -0.66)]) {
		above.lineTo(q.x, q.y);
	}
	above.closePath();
	const hair = new Path2D();
	if (cut === "big") {
		const h = P(-0.16, -0.34);
		hair.ellipse(h.x, h.y, r * 1.14, r * 1.02, 0, 0, Math.PI * 2);
	} else if (cut === "long") {
		hair.addPath(
			softPoly([
				P(-0.15, -1.12),
				P(0.52, -1.06),
				P(0.86, -0.68),
				P(0.4, 0.0),
				P(-0.4, 1.3),
				P(-0.9, 1.3),
				P(-1.1, 0.2),
				P(-0.98, -0.68),
			]),
		);
	} else {
		hair.addPath(grown);
	}
	// Inked round like the rest of him: the lines first, the colors over
	// their inner half.
	if (ink > 0) {
		ctx.strokeStyle = INK;
		ctx.lineWidth = ink * 2;
		ctx.lineJoin = "round";
		ctx.stroke(head);
		ctx.stroke(ear);
		if (hairy) {
			ctx.save();
			ctx.clip(above);
			ctx.stroke(hair);
			ctx.restore();
		}
	}
	ctx.fillStyle = look.skin;
	ctx.fill(head);
	const shadow = shade(look.skin, -0.18);
	// His beard, on his jaw, his chin, his lip - and by his ear.
	const whiskers = new Path2D();
	if (pro.jaw) {
		whiskers.addPath(
			softPoly([
				P(-0.1, 0.12),
				P(0.3, 0.3),
				P(0.66, 0.4),
				P(0.86, 0.5),
				P(1.0, 0.7),
				P(0.8, 1.02),
				P(0.2, 0.98),
				P(-0.2, 0.62),
			]),
		);
	}
	if (pro.chin) {
		whiskers.addPath(
			softPoly([P(0.6, 0.54), P(0.98, 0.54), P(0.98, 0.98), P(0.56, 0.98)]),
		);
	}
	if (pro.lip) {
		whiskers.addPath(
			softPoly([P(0.6, 0.3), P(0.98, 0.32), P(0.94, 0.42), P(0.62, 0.4)]),
		);
	}
	if (pro.burns) {
		whiskers.addPath(
			softPoly([P(-0.02, -0.22), P(0.16, -0.2), P(0.16, 0.34), P(-0.02, 0.32)]),
		);
	}
	if (pro.jaw || pro.chin || pro.lip || pro.burns) {
		ctx.save();
		ctx.clip(head);
		ctx.fillStyle = cut === "bald" ? shade(look.skin, -0.55) : look.hair;
		ctx.fill(whiskers);
		ctx.restore();
	}
	ctx.fillStyle = look.skin;
	ctx.fill(ear);
	ctx.fillStyle = shadow;
	ctx.fill(inner);
	if (hairy) {
		ctx.save();
		ctx.clip(above);
		ctx.fillStyle = look.hair;
		ctx.fill(hair);
		ctx.restore();
	}
	if (pro.band) {
		// Round his head at his brow (or up over his hair), his team's
		// colors - high on his forehead, sloping down over the tops of his
		// ears to the back of his head, bowed a little where it rounds his
		// skull toward the camera.
		const lift = pro.band.high ? -0.32 : 0;
		const edge = (v: number) => {
			const a = P(0.98, BAND_FRONT + v + lift);
			const m = P(-0.1, (BAND_FRONT + BAND_BACK) / 2 + v + lift + 0.06);
			const b = P(-1.2, BAND_BACK + v + lift);
			return { a, m, b };
		};
		const top = edge(0);
		const bottom = edge(BAND_WIDE);
		const band = new Path2D();
		band.moveTo(top.a.x, top.a.y);
		band.quadraticCurveTo(top.m.x, top.m.y, top.b.x, top.b.y);
		band.lineTo(bottom.b.x, bottom.b.y);
		band.quadraticCurveTo(bottom.m.x, bottom.m.y, bottom.a.x, bottom.a.y);
		band.closePath();
		ctx.save();
		ctx.clip(grown);
		ctx.fillStyle = pro.band.color;
		ctx.fill(band);
		ctx.strokeStyle = pro.band.stripe;
		ctx.lineWidth = r * 0.05;
		const mid = edge(BAND_WIDE / 2);
		ctx.beginPath();
		ctx.moveTo(mid.a.x, mid.a.y);
		ctx.quadraticCurveTo(mid.m.x, mid.m.y, mid.b.x, mid.b.y);
		ctx.stroke();
		ctx.restore();
	}
	if (look.silhouette) {
		return;
	}
	// His eye, looking where his nose points, under his brow.
	const eye = P(0.64, -0.06);
	ctx.fillStyle = "#f6f2ec";
	ctx.beginPath();
	ctx.ellipse(eye.x, eye.y, r * 0.1, r * 0.075, 0, 0, Math.PI * 2);
	ctx.fill();
	const iris = P(0.7, -0.055);
	ctx.fillStyle = INK;
	ctx.beginPath();
	ctx.ellipse(iris.x, iris.y, r * 0.05, r * 0.07, 0, 0, Math.PI * 2);
	ctx.fill();
	ctx.strokeStyle = INK;
	ctx.lineCap = "round";
	ctx.lineWidth = Math.max(0.6, r * 0.05);
	ctx.beginPath();
	const lid0 = P(0.54, -0.1);
	const lid1 = P(0.76, -0.1);
	ctx.moveTo(lid0.x, lid0.y);
	ctx.quadraticCurveTo(P(0.65, -0.16).x, P(0.65, -0.16).y, lid1.x, lid1.y);
	ctx.stroke();
	ctx.strokeStyle =
		cut === "bald" || !hairy ? shade(look.skin, -0.5) : shade(look.hair, -0.1);
	ctx.lineWidth = Math.max(0.8, r * 0.09);
	ctx.beginPath();
	const brow0 = P(0.48, -0.24);
	const brow1 = P(0.84, -0.22);
	ctx.moveTo(brow0.x, brow0.y);
	ctx.quadraticCurveTo(P(0.66, -0.3).x, P(0.66, -0.3).y, brow1.x, brow1.y);
	ctx.stroke();
	if (pro.eyeBlack) {
		ctx.fillStyle = INK;
		ctx.fill(
			softPoly([P(0.5, 0.04), P(0.78, 0.04), P(0.76, 0.13), P(0.52, 0.13)]),
		);
	}
	// His nostril and the line of his mouth.
	ctx.strokeStyle = shade(look.skin, -0.45);
	ctx.lineWidth = Math.max(0.6, r * 0.045);
	ctx.beginPath();
	const n0 = P(0.84, 0.25);
	ctx.moveTo(n0.x, n0.y);
	ctx.quadraticCurveTo(
		P(0.9, 0.2).x,
		P(0.9, 0.2).y,
		P(0.96, 0.24).x,
		P(0.96, 0.24).y,
	);
	ctx.stroke();
	ctx.strokeStyle = INK;
	ctx.beginPath();
	const m0 = P(0.88, 0.47);
	ctx.moveTo(m0.x, m0.y);
	ctx.lineTo(P(0.74, 0.48).x, P(0.74, 0.48).y);
	ctx.stroke();
	ctx.lineCap = "butt";
};

const drawHead = (
	ctx: CanvasRenderingContext2D,
	middle: Projected,
	r: number,
	look: Look,
	front: number,
	at: (v: V3) => Projected,
	head: V3,
	// How thick the ink round the back of his head (px).
	ink = 0,
) => {
	const c = { x: middle.x, y: middle.y - r * FACE_LIFT };
	// Which way his nose points on screen.
	const ahead = at({ f: head.f + 1, s: head.s, u: head.u });
	const turn = Math.sign(ahead.x - c.x) || 1;
	const sprite = look.head;
	const cut = look.cut ?? "short";
	const hairy = cut !== "bald" && look.hair !== look.skin;
	// The back of his head, sized to the face that turns into it: his skull,
	// his hair over it down to the nape of his neck, his ears. `side` (0 to
	// 1) is how side on he is: from behind, the hairline runs straight across
	// and both ears show; side on, it rises toward his ear - the one on his
	// face, just in front - so the hair is a cap on his skull, not a curtain
	// hanging down behind his face.
	const back = (dx = 0, side = 0) => {
		const x = c.x + dx;
		const skull = new Path2D();
		// Side on, the back of his skull rounds off above the nape and into
		// his neck, rather than bulging down behind his jaw.
		const top = c.y - r * (0.08 + 0.14 * side);
		const tall = r * (1.1 - 0.14 * side);
		skull.ellipse(x, top, r * 0.95, tall, 0, 0, Math.PI * 2);
		const ears: { outer: Path2D; inner: Path2D }[] = [];
		if (side < 0.5 && cut !== "big" && cut !== "long") {
			for (const s of [-1, 1]) {
				const ex = x + s * r * 0.93;
				const outer = new Path2D();
				outer.ellipse(
					ex,
					c.y + r * 0.04,
					r * 0.16,
					r * 0.27,
					0,
					0,
					Math.PI * 2,
				);
				const inner = new Path2D();
				inner.ellipse(
					ex + s * r * 0.03,
					c.y + r * 0.04,
					r * 0.08,
					r * 0.17,
					0,
					0,
					Math.PI * 2,
				);
				ears.push({ outer, inner });
			}
		}
		// Where his hair ends. From behind it comes down to the nape; side on
		// - today's cut, short or faded at the sides and back - it stops above
		// his ear, skin below it, so it reads as a man's haircut and not hair
		// down to his jaw. (Long, it hangs past his neck either way.)
		const nape =
			c.y +
			r *
				(cut === "long"
					? 1.15
					: (cut === "big" ? 0.66 : 0.72) -
						side * (cut === "big" ? 0.52 : 0.78));
		const over = c.y - r * (cut === "long" ? 0.2 : 0.3);
		const xBack = x - turn * r * 0.95;
		const xEar = x + turn * r * 0.6;
		const hairline = (px: number) => {
			const u = Math.min(1, Math.max(0, (px - xBack) / (xEar - xBack)));
			// (Seen from behind, it dips a little in the middle of his neck.)
			const w = Math.max(0, 1 - ((px - x) / (r * 0.95)) ** 2);
			return (
				nape +
				(over - nape) * side * u * u * (3 - 2 * u) +
				r * 0.07 * (1 - side) * w
			);
		};
		const cap = new Path2D();
		const x0 = x - r * 1.5;
		const x1 = x + r * 1.5;
		cap.moveTo(x0, c.y - r * 2.5);
		for (let k = 0; k <= 24; k++) {
			const px = x0 + ((x1 - x0) * k) / 24;
			cap.lineTo(px, hairline(px));
		}
		cap.lineTo(x1, c.y - r * 2.5);
		cap.closePath();
		// Cropped close it hugs his skull; with some to it, it stands off it.
		const hair = new Path2D();
		if (cut === "big") {
			hair.ellipse(
				x - turn * r * 0.1 * side,
				c.y - r * 0.24,
				r * 1.12,
				r * 1.14,
				0,
				0,
				Math.PI * 2,
			);
		} else if (cut === "long") {
			hair.ellipse(x, c.y + r * 0.1, r * 1.0, r * 1.32, 0, 0, Math.PI * 2);
		} else {
			hair.ellipse(
				x,
				top - r * 0.02,
				r * 0.99,
				tall + r * 0.03,
				0,
				0,
				Math.PI * 2,
			);
		}
		// Inked round like the rest of him: the line first, the colors over
		// its inner half.
		if (ink > 0) {
			ctx.strokeStyle = INK;
			ctx.lineWidth = ink * 2;
			ctx.lineJoin = "round";
			ctx.stroke(skull);
			for (const e of ears) {
				ctx.stroke(e.outer);
			}
			if (hairy) {
				ctx.save();
				ctx.clip(cap);
				ctx.stroke(hair);
				ctx.restore();
			}
		}
		ctx.fillStyle = look.skin;
		ctx.fill(skull);
		for (const e of ears) {
			ctx.fillStyle = look.skin;
			ctx.fill(e.outer);
			ctx.fillStyle = shade(look.skin, -0.18);
			ctx.fill(e.inner);
		}
		if (hairy) {
			ctx.save();
			ctx.clip(hair);
			ctx.fillStyle = look.hair;
			ctx.fill(cap);
			ctx.restore();
		}
		const band = look.profile?.band;
		if (band) {
			// His headband, round the back of his head as high as it sits at
			// the side - dipping a little in the middle, seen from above.
			const lift = band.high ? -0.32 : 0;
			const y0 = c.y + r * (BAND_BACK + 0.04 + lift);
			const y1 = y0 + r * BAND_WIDE;
			const sag = r * 0.12;
			const strip = new Path2D();
			strip.moveTo(x - r * 1.3, y0);
			strip.quadraticCurveTo(x, y0 + sag, x + r * 1.3, y0);
			strip.lineTo(x + r * 1.3, y1);
			strip.quadraticCurveTo(x, y1 + sag, x - r * 1.3, y1);
			strip.closePath();
			const head = new Path2D();
			head.addPath(skull);
			if (hairy) {
				head.addPath(hair);
			}
			ctx.save();
			ctx.clip(head);
			ctx.fillStyle = band.color;
			ctx.fill(strip);
			ctx.strokeStyle = band.stripe;
			ctx.lineWidth = r * 0.05;
			ctx.beginPath();
			const mid = (y0 + y1) / 2;
			ctx.moveTo(x - r * 1.3, mid);
			ctx.quadraticCurveTo(x, mid + sag, x + r * 1.3, mid);
			ctx.stroke();
			ctx.restore();
		}
	};
	// Facing away: the back of his head. Side on: his head in profile - not
	// his face turned to the camera over the back of his skull - and, as he
	// comes round to the camera, his face over it.
	if (front < PROFILE_FROM) {
		back();
		return;
	}
	if (front < FACE_FROM + FACE_IN) {
		profileHead(ctx, c, r, turn as 1 | -1, look, ink);
	}
	const vis = sprite ? Math.min(1, (front - FACE_FROM) / FACE_IN) : 0;
	if (!sprite || vis <= 0) {
		if (!sprite && front >= FACE_FROM + FACE_IN) {
			back();
		}
		return;
	}
	// Round to the camera: his face, cheated toward it the way a cartoon's
	// is - shifted a little the way he looks.
	const squash = 0.82 + 0.18 * Math.max(0, front);
	const shift = turn * (1 - Math.max(0, front)) * r * 0.14;
	const scale = (r * 2.45) / sprite.h;
	ctx.save();
	ctx.globalAlpha *= vis;
	ctx.translate(c.x + shift, c.y);
	ctx.scale(scale * squash, scale);
	ctx.drawImage(sprite.img, -sprite.cx, -sprite.cy);
	ctx.restore();
};

const luminance = (c: string): number => {
	const [r, g, b] = parse(c);
	return (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255;
};

const contrast = (a: string, b: string) =>
	Math.abs(luminance(a) - luminance(b));

// A team's two uniforms: at home, white with its main color on it; on the
// road, its main color (or its darkest, if the main one is light) with white
// on it.
type Edition = "home" | "road";

const WHITE = "#f4f1ea";
const INK_DARK = "#1b1b20";

const uniform = (colors: [string, string, string], edition: Edition): Kit => {
	const darkest = [...colors].sort((x, y) => luminance(x) - luminance(y))[0]!;
	const main = luminance(colors[0]) < 0.6 ? colors[0] : darkest;
	const base = edition === "home" ? WHITE : main;
	const trim = edition === "home" ? main : WHITE;
	// Round the numbers, another of the team's colors where it shows on both.
	const edge =
		colors.find(
			(c) =>
				c !== trim &&
				c !== base &&
				contrast(c, trim) > 0.2 &&
				contrast(c, base) > 0.12,
		) ?? shade(edition === "home" ? main : base, -0.4);
	const light = edition === "home";
	return {
		jersey: base,
		trim,
		number: trim,
		numberEdge: edge,
		shorts: base,
		stripe: trim,
		sock: light ? "#f1f1f1" : shade(base, -0.25),
		shoe: light ? "#f4f4f4" : "#1d1d22",
		sole: light ? trim : "#e9e6df",
	};
};

// #rgb, #rgba and #rrggbbaa as #rrggbb.
const hex6 = (c: string): string => {
	const h = c.replace("#", "");
	if (h.length === 3 || h.length === 4) {
		return `#${h[0]}${h[0]}${h[1]}${h[1]}${h[2]}${h[2]}`;
	}
	return `#${h.slice(0, 6)}`;
};

// What a team wears: its colors, and the jersey it has made its own (see
// common/uniform.ts), if it has.
export type TeamDress = {
	colors?: [string, string, string];
	jersey?: string;
};

// A team's uniform. Its own jersey, if it has made one, is worn on the side
// its color suits - a light one at home, a dark one away - with the shorts,
// trim, lettering and name across the chest it was made with; on the other
// side the team wears the usual uniform in that jersey's color.
const kitOf = (
	dress: TeamDress | undefined,
	fallback: [string, string, string],
	edition: Edition,
): Kit => {
	const colors = dress?.colors ?? fallback;
	const spec = parseUniform(dress?.jersey);
	if (!spec) {
		return uniform(colors, edition);
	}
	const base = hex6(spec.base ?? colors[0]);
	if (luminance(base) >= 0.6 !== (edition === "home")) {
		return uniform([base, colors[1], colors[2]], edition);
	}
	const auto = uniform(colors, edition);
	const band = spec.collar?.at(-1) ?? spec.arm?.at(-1);
	const trim = band ? hex6(band.color) : auto.trim;
	const light = edition === "home";
	return {
		...auto,
		jersey: base,
		trim,
		number: spec.number?.color ? hex6(spec.number.color) : trim,
		numberEdge: spec.number?.outline
			? hex6(spec.number.outline)
			: auto.numberEdge,
		shorts: spec.shorts?.base ? hex6(spec.shorts.base) : base,
		stripe: spec.shorts?.side ? hex6(spec.shorts.side) : trim,
		sock: light ? "#f1f1f1" : shade(base, -0.25),
		...(spec.wordmark?.color ? { chest: hex6(spec.wordmark.color) } : {}),
		...(spec.wordmark?.text ? { chestText: spec.wordmark.text } : {}),
	};
};

// What the two teams wear: the home team in white, the visitors in their
// color.
export const kitsFor = (
	away: TeamDress | undefined,
	home: TeamDress | undefined,
): [Kit, Kit] => [
	kitOf(away, ["#1d3461", "#f28c28", "#ffffff"], "road"),
	kitOf(home, ["#8c1d40", "#f2c14e", "#ffffff"], "home"),
];

// A player's own gear, the same every game he plays: decided by who he is,
// in the colors of whatever his team wears tonight.
export const gearFor = (pid: number, kit: Kit): Gear => {
	const rng = makeCourtRng(`gear|${pid}`);
	// Gear comes in his uniform's color, black or white.
	const color = () => {
		const r = rng();
		return r < 0.45 ? kit.trim : r < 0.75 ? INK_DARK : WHITE;
	};
	const arms = (): "R" | "L" | "RL" => {
		const r = rng();
		return r < 0.7 ? "R" : r < 0.85 ? "L" : "RL";
	};
	const gear: Gear = {
		shoe: kit.shoe,
		sole: kit.sole,
		sock: kit.sock,
	};
	const shoe = rng();
	if (shoe < 0.3) {
		gear.shoe = INK_DARK;
		gear.sole = WHITE;
	} else if (shoe < 0.55) {
		gear.shoe = "#f4f4f4";
		gear.sole = shoe < 0.42 ? kit.trim : INK_DARK;
	} else if (shoe < 0.75) {
		gear.shoe = kit.trim;
		gear.sole = WHITE;
	} else if (shoe < 0.83) {
		// Loud ones.
		gear.shoe = ["#e63946", "#f4b400", "#3ddc84", "#ff7a00"][
			Math.floor(rng() * 4)
		]!;
		gear.sole = WHITE;
	}
	const sock = rng();
	gear.sock = sock < 0.45 ? kit.sock : sock < 0.75 ? "#f1f1f1" : INK_DARK;
	if (rng() < 0.3) {
		gear.sleeve = { arms: arms(), color: color(), elbow: rng() < 0.35 };
	}
	if (rng() < 0.24) {
		const r = rng();
		gear.tights = {
			legs: r < 0.45 ? "R" : r < 0.8 ? "L" : "RL",
			color: color(),
		};
	}
	if (rng() < 0.32) {
		gear.wrist = { arms: arms(), color: color() };
	}
	if (rng() < 0.12) {
		gear.knee = {
			legs: rng() < 0.5 ? "R" : "L",
			color: rng() < 0.5 ? INK_DARK : WHITE,
		};
	}
	return gear;
};
