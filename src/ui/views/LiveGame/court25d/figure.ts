import { makeCourtRng } from "../courtRng.ts";
import { project, type Camera, type Projected } from "./camera.ts";
import { bodyPoint, type PlayerState } from "./evaluate.ts";
import type { HeadSprite } from "./faces.ts";
import {
	holdBall,
	posed,
	skeleton,
	type Body,
	type Limb,
	type V3,
} from "./poses.ts";

// ONE PLAYER, DRAWN - the body of his sprite (see sprite.ts).
//
// His skeleton is posed, turned to face where he faces, and every joint is
// put through the camera. Then he is drawn small, shape by shape: each limb a
// shaped silhouette rather than a tube - a deltoid capping the shoulder, a
// forearm swelling below the elbow, a calf behind the shin - in two tones of
// light. Shorts hang like cloth, sneakers are sneakers. Farthest parts first.
// The sprite then makes pixel art of it, and puts his face on.

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
	gear?: Gear;
	outfit?: Outfit;
	skin: string;
	hair: string;
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

// Where the light comes from on screen: up and a little to the left. The
// other side of every limb is in shadow.
const LIGHT = { x: -0.42, y: -0.91 };

const lerp2 = (a: P2, b: P2, f: number): P2 => ({
	x: a.x + (b.x - a.x) * f,
	y: a.y + (b.y - a.y) * f,
});

// A smooth curve through points (quadratic, through the midpoints).
const smoothThrough = (p: Path2D, pts: P2[], start: boolean) => {
	if (pts.length === 0) {
		return;
	}
	if (start) {
		p.moveTo(pts[0]!.x, pts[0]!.y);
	} else {
		p.lineTo(pts[0]!.x, pts[0]!.y);
	}
	for (let i = 1; i < pts.length - 1; i++) {
		const m = lerp2(pts[i]!, pts[i + 1]!, 0.5);
		p.quadraticCurveTo(pts[i]!.x, pts[i]!.y, m.x, m.y);
	}
	const last = pts.at(-1)!;
	p.lineTo(last.x, last.y);
};

// A station along a limb: how far along (0 at a, 1 at b), and its half-width
// in feet - plus how much more it swells on the limb's back (a calf).
type Station = [t: number, w: number, back?: number];

type Shaped = {
	path: Path2D;
	// The edge on the shadow side, for the second tone.
	shadowEdge: P2[];
	width: number;
};

// A limb's silhouette from a to b. `backDir` is the screen direction his
// limb's back faces (where a calf bulges), if it can be told.
const limbShape = (
	a: P2,
	ka: number,
	b: P2,
	kb: number,
	stations: Station[],
	backDir?: P2,
): Shaped => {
	const dx = b.x - a.x;
	const dy = b.y - a.y;
	const L = Math.hypot(dx, dy);
	const ux = L > 0.01 ? dx / L : 0;
	const uy = L > 0.01 ? dy / L : 1;
	const nx = -uy;
	const ny = ux;
	let backSign = 0;
	if (backDir) {
		const d = backDir.x * nx + backDir.y * ny;
		const m = Math.hypot(backDir.x, backDir.y);
		if (m > 0 && Math.abs(d) > 0.35 * m) {
			backSign = Math.sign(d);
		}
	}
	const left: P2[] = [];
	const right: P2[] = [];
	let maxW = 0;
	for (const [t, w, back = 0] of stations) {
		const k = ka + (kb - ka) * t;
		const wl = (w + (backSign > 0 ? back : -back * 0.25)) * k;
		const wr = (w + (backSign < 0 ? back : -back * 0.25)) * k;
		maxW = Math.max(maxW, wl, wr);
		const cx = a.x + dx * t;
		const cy = a.y + dy * t;
		left.push({ x: cx + nx * wl, y: cy + ny * wl });
		right.push({ x: cx - nx * wr, y: cy - ny * wr });
	}
	const p = new Path2D();
	smoothThrough(p, left, true);
	// Round the far end.
	const rEnd = stations.at(-1)![1] * kb + 0.001;
	const angN = Math.atan2(ny, nx);
	p.arc(b.x, b.y, rEnd, angN, angN - Math.PI, true);
	smoothThrough(p, [...right].reverse(), false);
	const rStart = stations[0]![1] * ka + 0.001;
	p.arc(a.x, a.y, rStart, angN + Math.PI, angN, true);
	p.closePath();
	// The shadow is on the side facing away from the light.
	const lightSide = nx * LIGHT.x + ny * LIGHT.y;
	return { path: p, shadowEdge: lightSide > 0 ? right : left, width: maxW };
};

const polyPath = (pts: P2[]): Path2D => {
	const p = new Path2D();
	pts.forEach((q, i) => {
		if (i === 0) {
			p.moveTo(q.x, q.y);
		} else {
			p.lineTo(q.x, q.y);
		}
	});
	p.closePath();
	return p;
};

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

// Convex hull (monotone chain).
const hull = (pts: P2[]): P2[] => {
	const p = [...pts].sort((a, b) => a.x - b.x || a.y - b.y);
	if (p.length < 3) {
		return p;
	}
	const cross = (o: P2, a: P2, b: P2) =>
		(a.x - o.x) * (b.y - o.y) - (a.y - o.y) * (b.x - o.x);
	const lower: P2[] = [];
	for (const q of p) {
		while (lower.length >= 2 && cross(lower.at(-2)!, lower.at(-1)!, q) <= 0) {
			lower.pop();
		}
		lower.push(q);
	}
	const upper: P2[] = [];
	for (let i = p.length - 1; i >= 0; i--) {
		const q = p[i]!;
		while (upper.length >= 2 && cross(upper.at(-2)!, upper.at(-1)!, q) <= 0) {
			upper.pop();
		}
		upper.push(q);
	}
	lower.pop();
	upper.pop();
	return [...lower, ...upper];
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

// The ball, when he has it in his hands: its leather, its shadow side, its
// seams. (Kept in step with the loose ball's colors in arena.ts.)
const HELD_BALL_R = 0.39;
export const BALL_ORANGE = "#e2702a";
export const BALL_SHADE = "#a44716";
export const BALL_SEAM = "#3a1608";

type Shape = {
	path: Path2D;
	fill: string | CanvasGradient;
	// The second tone, along the side away from the light.
	shadow?: { edge: P2[]; width: number; color: string };
};
type Part = {
	depth: number;
	shapes: Shape[];
	// Drawn after his head: an arm thrown up in front of his face.
	late?: boolean;
	// Drawn over the part's colors: seams, stripes, lettering - on the canvas
	// the part is painted on.
	detail?: (c: CanvasRenderingContext2D) => void;
};

// Where things landed on screen, for whatever is drawn over him.
export type FigureAnchors = {
	head: { x: number; y: number; r: number };
	// The middle of the lettering on his chest or back, its size, and which
	// side shows (1 chest, -1 back, 0 neither).
	number: { x: number; y: number; h: number; side: 1 | -1 | 0 };
	// Across the chest or back, above the number, and how wide the jersey is
	// there (screen px).
	letters: { x: number; y: number; w: number };
	front: number;
	// The ball is drawn in his hands - where on the picture, and whether it is
	// in front of his jersey (hiding the lettering behind it).
	holding: boolean;
	ball?: { x: number; y: number; r: number; front: boolean };
	// What goes over his head once it is drawn: an arm raised in front of his
	// face, which the head would otherwise hide.
	over?: (ctx: CanvasRenderingContext2D) => void;
};

export const drawFigure = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	st: PlayerState,
	body: Body,
	look: Look,
	// Screen pixels to a sprite pixel: the finest line worth drawing.
	px: number,
): FigureAnchors => {
	const q = posed(st.anim, st.phase, st.dribble);
	const held = st.holding ? holdBall(body, q, st.anim) : undefined;
	const sk = held ? held.sk : skeleton(body, q);
	const at = (v: V3): Projected => project(cam, bodyPoint(st, v));
	const off = (v: V3, df: number, ds: number, du = 0): V3 => ({
		f: v.f + df,
		s: v.s + ds,
		u: v.u + du,
	});

	// Which way he faces relative to the camera: front > 0 shows his chest,
	// side > 0 his left.
	const toCamX = cam.pos.x - st.x;
	const toCamY = cam.pos.y - st.y;
	const toCamL = Math.hypot(toCamX, toCamY) || 1;
	const cy = Math.cos(st.yaw);
	const sy = Math.sin(st.yaw);
	const front = (cy * toCamX + sy * toCamY) / toCamL;
	const side = (sy * toCamX - cy * toCamY) / toCamL;

	const kit = look.kit;
	const gear = look.gear;
	const skin = look.skin;
	const pelvis = at(sk.pelvis);
	const torsoDepth = (at(sk.chest).depth + pelvis.depth) / 2;
	// The side turned away from the camera is in his own shadow.
	const dim = (c: string, far: boolean) => (far ? shade(c, -0.14) : c);
	const leftFar = side < 0;
	// On screen, the way his body faces - so a calf can bulge behind his shin.
	const fwd2 = (() => {
		const a = at(sk.pelvis);
		const b = at(off(sk.pelvis, 1, 0));
		return { x: b.x - a.x, y: b.y - a.y };
	})();
	const behind = { x: -fwd2.x, y: -fwd2.y };

	const parts: Part[] = [];
	const shaped = (
		s: Shaped,
		fill: string,
		shadowColor: string | undefined,
	): Shape => ({
		path: s.path,
		fill,
		shadow: shadowColor
			? { edge: s.shadowEdge, width: s.width * 0.55, color: shadowColor }
			: undefined,
	});

	const leg = (limb: Limb, far: boolean, outer: 1 | -1) => {
		const hip = at(limb.root);
		const knee = at(limb.mid);
		const ankle = at(limb.end);
		const toe = at(limb.tip ?? limb.end);
		const which = outer < 0 ? "R" : "L";
		// Tights down the leg instead of bare skin, his own socks and shoes.
		const tights = gear?.tights?.legs.includes(which) ? gear.tights : undefined;
		const sk0 = dim(tights ? tights.color : skin, far);
		const skinShadow = shade(sk0, -0.2);
		const sockColor = gear?.sock ?? kit.sock;
		const shoeColor = gear?.shoe ?? kit.shoe;
		const soleColor = gear?.sole ?? kit.sole;
		const shapes: Shape[] = [];
		// Thigh, mostly under the shorts.
		shapes.push(
			shaped(
				limbShape(hip, hip.k, knee, knee.k, [
					[0, body.thighR * 1.05],
					[0.45, body.thighR * 1.0],
					[1, body.kneeR * 1.05],
				]),
				sk0,
				skinShadow,
			),
		);
		// Shin and calf: the calf swells high up behind the shin.
		const shin = limbShape(
			knee,
			knee.k,
			ankle,
			ankle.k,
			[
				[0, body.kneeR * 1.02],
				[0.2, body.calfR * 1.0, body.calfR * 0.3],
				[0.45, body.calfR * 0.86, body.calfR * 0.12],
				[0.8, body.ankleR * 1.12],
				[1, body.ankleR],
			],
			behind,
		);
		shapes.push(shaped(shin, sk0, skinShadow));
		if (gear?.knee?.legs === which) {
			// A pad round the knee.
			shapes.push({
				path: limbShape(
					lerp2(hip, knee, 0.86),
					knee.k,
					lerp2(knee, ankle, 0.18),
					knee.k,
					[
						[0, body.kneeR * 1.22],
						[1, body.kneeR * 1.16],
					],
				).path,
				fill: dim(gear.knee.color, far),
			});
		}
		// Crew socks, up to mid-calf.
		const sockTop = lerp2(knee, ankle, 0.6);
		shapes.push(
			shaped(
				limbShape(sockTop, knee.k, ankle, ankle.k, [
					[0, body.calfR * 0.84],
					[1, body.ankleR * 1.08],
				]),
				dim(sockColor, far),
				shade(dim(sockColor, far), -0.18),
			),
		);
		// The sneaker: a sole along the floor, a toe box, a high collar at the
		// ankle.
		const shoe = sneaker(ankle, toe, ankle.k, Math.abs(front) > 0.62);
		shapes.push({ path: shoe.upper, fill: dim(shoeColor, far) });
		// Shorts: wide and loose to just above the knee.
		const hem = lerp2(hip, knee, 0.74);
		const tx = knee.x - hip.x;
		const ty = knee.y - hip.y;
		const tl = Math.hypot(tx, ty) || 1;
		const nx = -ty / tl;
		const ny = tx / tl;
		const wTop = body.thighR * 1.3 * hip.k;
		const wHem = body.thighR * 1.4 * knee.k;
		const hemL = { x: hem.x + nx * wHem, y: hem.y + ny * wHem };
		const hemR = { x: hem.x - nx * wHem, y: hem.y - ny * wHem };
		const shorts = softPoly([
			{ x: hip.x + nx * wTop, y: hip.y + ny * wTop },
			hemL,
			lerp2(hemL, hemR, 0.5),
			hemR,
			{ x: hip.x - nx * wTop, y: hip.y - ny * wTop },
		]);
		shapes.push({ path: shorts, fill: dim(kit.shorts, far) });
		parts.push({
			// Under the torso, always - his shorts hang over his legs.
			depth: Math.max(knee.depth, torsoDepth) + (far ? 0.6 : 0.3),
			shapes,
			detail: () => {
				ctx.fillStyle = soleColor;
				ctx.fill(shoe.sole);
				// The stripe down the outside of the shorts.
				const a = at(off(limb.root, 0, outer * body.thighR * 1.32));
				const b = at(
					off(
						{
							f: limb.root.f + (limb.mid.f - limb.root.f) * 0.76,
							s: limb.root.s + (limb.mid.s - limb.root.s) * 0.76,
							u: limb.root.u + (limb.mid.u - limb.root.u) * 0.76,
						},
						0,
						outer * body.thighR * 1.4,
					),
				);
				ctx.strokeStyle = dim(kit.stripe, far);
				ctx.lineCap = "butt";
				ctx.lineWidth = Math.max(px, 0.12 * hip.k);
				ctx.beginPath();
				ctx.moveTo(a.x, a.y);
				ctx.lineTo(b.x, b.y);
				ctx.stroke();
			},
		});
	};
	leg(sk.legR, !leftFar, -1);
	leg(sk.legL, leftFar, 1);

	// Rings round his middle at a height up the spine (0 hips, 1 shoulders):
	// a torso or a waistband, seen from wherever the camera is.
	const ring = (lambda: number, lat: number, dep: number): Projected[] => {
		const f = sk.pelvis.f + (sk.chest.f - sk.pelvis.f) * lambda;
		const u = sk.pelvis.u + (sk.chest.u - sk.pelvis.u) * lambda;
		const out: Projected[] = [];
		for (let i = 0; i < 16; i++) {
			const a = (i / 16) * Math.PI * 2;
			out.push(at({ f: f + Math.cos(a) * dep, s: Math.sin(a) * lat, u }));
		}
		return out;
	};

	// The torso: broad through the chest and shoulders, tapering to the waist,
	// the shorts' waistband under it, a thick neck above.
	const chest = at(sk.chest);
	const headC = at(sk.head);
	const neckBase = at(off(sk.chest, 0, 0, body.H * 0.01));
	const jerseyPts = hull([
		...ring(1.0, body.shoulderW * 0.8, body.depth * 0.44),
		...ring(0.82, body.shoulderW * 0.94, body.depth * 0.56),
		...ring(0.5, body.hipW * 1.7, body.depth * 0.54),
		...ring(0.06, body.hipW * 1.56, body.depth * 0.5),
	]);
	const waistPts = hull([
		...ring(0.14, body.hipW * 1.62, body.depth * 0.52),
		...ring(-0.18, body.hipW * 1.72, body.depth * 0.55),
	]);
	const neck = limbShape(neckBase, neckBase.k, headC, headC.k, [
		[0, body.headR * 0.52],
		[0.5, body.headR * 0.4],
		[1, body.headR * 0.36],
	]);
	// The jersey's shadow side: the half of him turned from the light.
	const jerseyShadow = (() => {
		const cx = jerseyPts.reduce((s, p) => s + p.x, 0) / jerseyPts.length;
		const cyy = jerseyPts.reduce((s, p) => s + p.y, 0) / jerseyPts.length;
		return jerseyPts.filter(
			(p) => (p.x - cx) * LIGHT.x + (p.y - cyy) * LIGHT.y < 0,
		);
	})();
	const jerseyPath = softPoly(jerseyPts);
	parts.push({
		depth: torsoDepth,
		shapes: [
			shaped(neck, skin, shade(skin, -0.2)),
			{ path: polyPath(waistPts), fill: kit.shorts },
			{
				path: jerseyPath,
				fill: kit.jersey,
				shadow: jerseyShadow
					? {
							edge: jerseyShadow,
							width: body.depth * 0.5 * chest.k,
							color: shade(kit.jersey, -0.12),
						}
					: undefined,
			},
		],
		detail: () => {
			drawStripes();
			drawCollar();
		},
	});

	// A referee's stripes, down the shirt.
	const drawStripes = () => {
		const color = look.outfit?.stripes;
		if (!color) {
			return;
		}
		let x0 = Infinity;
		let x1 = -Infinity;
		let y0 = Infinity;
		let y1 = -Infinity;
		for (const q of jerseyPts) {
			x0 = Math.min(x0, q.x);
			x1 = Math.max(x1, q.x);
			y0 = Math.min(y0, q.y);
			y1 = Math.max(y1, q.y);
		}
		const w = Math.max(px, 0.17 * chest.k);
		ctx.save();
		ctx.clip(jerseyPath);
		ctx.fillStyle = color;
		for (let x = x0 + w * 0.5; x < x1; x += w * 2) {
			ctx.fillRect(x, y0, w, y1 - y0);
		}
		ctx.restore();
	};

	// The neckline, in the trim color: a V at the front, a scoop at the back.
	const drawCollar = () => {
		if (Math.abs(front) <= 0.15) {
			return;
		}
		ctx.strokeStyle = kit.trim;
		ctx.lineCap = "round";
		ctx.lineJoin = "round";
		ctx.lineWidth = px;
		const lift = sk.chest.u - sk.pelvis.u;
		const face = (front >= 0 ? 1 : -1) * body.depth * 0.44;
		const top = (s: number) =>
			at({ f: sk.chest.f + face, s, u: sk.chest.u - 0.03 * lift });
		const v = at({
			f: sk.chest.f + face * 1.1 + (sk.pelvis.f - sk.chest.f) * 0.22,
			s: 0,
			u: sk.chest.u - lift * (front >= 0 ? 0.24 : 0.1),
		});
		const l = top(body.shoulderW * 0.42);
		const r = top(-body.shoulderW * 0.42);
		const outfit = look.outfit;
		if (outfit?.shirt && front >= 0) {
			// The open neck of his jacket: his shirt in a deep V, his tie down
			// the middle of it.
			const deep = at({
				f: sk.chest.f + face * 1.12 + (sk.pelvis.f - sk.chest.f) * 0.4,
				s: 0,
				u: sk.chest.u - lift * 0.42,
			});
			ctx.fillStyle = outfit.shirt;
			ctx.beginPath();
			ctx.moveTo(l.x, l.y);
			ctx.lineTo(deep.x, deep.y);
			ctx.lineTo(r.x, r.y);
			ctx.closePath();
			ctx.fill();
			if (outfit.tie) {
				const knot = lerp2(lerp2(l, r, 0.5), deep, 0.12);
				ctx.strokeStyle = outfit.tie;
				ctx.lineWidth = Math.max(px, 0.16 * chest.k);
				ctx.beginPath();
				ctx.moveTo(knot.x, knot.y);
				ctx.lineTo(deep.x, deep.y + 0.12 * chest.k);
				ctx.stroke();
			}
			return;
		}
		ctx.beginPath();
		ctx.moveTo(l.x, l.y);
		if (front >= 0) {
			ctx.lineTo(v.x, v.y);
			ctx.lineTo(r.x, r.y);
		} else {
			ctx.quadraticCurveTo(v.x, v.y, r.x, r.y);
		}
		ctx.stroke();
	};

	const arm = (limb: Limb, far: boolean, which: "R" | "L") => {
		const sh = at(limb.root);
		const el = at(limb.mid);
		const wrist = at(limb.end);
		const c = dim(skin, far);
		// A sleeve over the arm (or just the forearm), a band at the wrist.
		const sleeve = gear?.sleeve?.arms.includes(which) ? gear.sleeve : undefined;
		// Or the sleeves of his shirt (an official's, a coach's jacket).
		const shirt = look.outfit?.sleeves;
		const shirtC = dim(kit.jersey, far);
		const sl = shirt === "long" ? shirtC : sleeve ? dim(sleeve.color, far) : c;
		const upperC = shirt ? shirtC : sleeve && !sleeve.elbow ? sl : c;
		const band = gear?.wrist?.arms.includes(which)
			? dim(gear.wrist.color, far)
			: undefined;
		// The hand: a mitt a little past the wrist, pointing the way the
		// wrist bends it (along the forearm when it points at the camera).
		const tip = at(limb.tip ?? limb.end);
		let hx = tip.x - wrist.x;
		let hy = tip.y - wrist.y;
		if (Math.hypot(hx, hy) < 0.08 * wrist.k) {
			hx = wrist.x - el.x;
			hy = wrist.y - el.y;
		}
		const hl = Math.hypot(hx, hy) || 1;
		const handC = {
			x: wrist.x + (hx / hl) * body.handR * 0.55 * wrist.k,
			y: wrist.y + (hy / hl) * body.handR * 0.55 * wrist.k,
		};
		const hand = new Path2D();
		hand.ellipse(
			handC.x,
			handC.y,
			body.handR * 1.12 * wrist.k,
			body.handR * 0.86 * wrist.k,
			Math.atan2(hy, hx),
			0,
			Math.PI * 2,
		);
		parts.push({
			depth: (el.depth + wrist.depth) / 2 + (far ? 0.5 : -0.2),
			late:
				limb.end.u > limb.root.u + body.H * 0.08 &&
				(el.depth + wrist.depth) / 2 < headC.depth,
			shapes: [
				// The deltoid capping the shoulder, the upper arm, the forearm
				// swelling below the elbow and slimming to the wrist.
				shaped(
					limbShape(sh, sh.k, el, el.k, [
						[0, body.upperR * 1.28],
						[0.22, body.upperR * 1.16],
						[0.55, body.upperR * 1.0],
						[1, body.foreR * 0.92],
					]),
					upperC,
					shade(upperC, -0.2),
				),
				shaped(
					limbShape(el, el.k, wrist, wrist.k, [
						[0, body.foreR * 0.92],
						[0.22, body.foreR * 1.1],
						[0.6, body.foreR * 0.86],
						[1, body.foreR * 0.62],
					]),
					sl,
					shade(sl, -0.2),
				),
				...(band
					? [
							{
								path: limbShape(
									lerp2(el, wrist, 0.78),
									wrist.k,
									wrist,
									wrist.k,
									[
										[0, body.foreR * 0.86],
										[1, body.foreR * 0.78],
									],
								).path,
								fill: band,
							},
						]
					: []),
				{ path: hand, fill: c },
			],
		});
	};
	arm(sk.armR, !leftFar, "R");
	arm(sk.armL, leftFar, "L");
	if (look.outfit?.camera) {
		// His camera, in both hands: the body between them, the long lens out
		// the way he faces - up in front of his face when he is shooting.
		const mid: V3 = {
			f: (sk.armR.end.f + sk.armL.end.f) / 2 + 0.14,
			s: (sk.armR.end.s + sk.armL.end.s) / 2,
			u: (sk.armR.end.u + sk.armL.end.u) / 2 + 0.06,
		};
		const c0 = at(mid);
		const tipC = at({ ...mid, f: mid.f + 0.8 });
		const box = new Path2D();
		box.rect(c0.x - 0.3 * c0.k, c0.y - 0.24 * c0.k, 0.6 * c0.k, 0.46 * c0.k);
		parts.push({
			depth: c0.depth - 0.05,
			late: mid.u > sk.chest.u - 0.3 && c0.depth < headC.depth,
			shapes: [
				{ path: box, fill: CAMERA_BODY },
				{
					path: limbShape(c0, c0.k, tipC, tipC.k, [
						[0, 0.17],
						[1, 0.15],
					]).path,
					fill: CAMERA_LENS,
				},
			],
		});
	}
	let ballAt: FigureAnchors["ball"];
	if (held) {
		// The ball in his hands: drawn with him, behind the near hand and in
		// front of the far one, its seams and its shadow side.
		const c = at(held.ball);
		const r = HELD_BALL_R * c.k;
		const disc = new Path2D();
		disc.arc(c.x, c.y, r, 0, Math.PI * 2);
		const rim: P2[] = [];
		for (let a = -0.2; a <= Math.PI * 0.85; a += 0.25) {
			rim.push({ x: c.x + Math.cos(a) * r, y: c.y + Math.sin(a) * r });
		}
		ballAt = { x: c.x, y: c.y, r, front: c.depth < torsoDepth };
		parts.push({
			depth: c.depth,
			// Up in front of his face, or up over his head: drawn over it.
			late:
				held.ball.u > sk.armR.root.u &&
				(c.depth < headC.depth || held.ball.u > sk.head.u),
			shapes: [
				{
					path: disc,
					fill: BALL_ORANGE,
					shadow: { edge: rim, width: r * 0.75, color: BALL_SHADE },
				},
			],
			detail: (g) => {
				g.strokeStyle = BALL_SEAM;
				g.lineWidth = Math.max(px * 0.9, r * 0.16);
				g.lineCap = "round";
				g.beginPath();
				g.moveTo(c.x - r * 0.92, c.y - r * 0.08);
				g.lineTo(c.x + r * 0.92, c.y + r * 0.08);
				g.stroke();
				g.beginPath();
				g.ellipse(c.x, c.y, r * 0.42, r * 0.95, 0.15, 0, Math.PI * 2);
				g.stroke();
			},
		});
	}

	parts.sort((p, q) => q.depth - p.depth);
	const paint = (c: CanvasRenderingContext2D, p: Part) => {
		for (const s of p.shapes) {
			c.fillStyle = s.fill;
			c.fill(s.path);
			if (s.shadow && s.shadow.edge.length > 1) {
				// The second tone: a band along the shadow side, kept inside.
				c.save();
				c.clip(s.path);
				c.strokeStyle = s.shadow.color;
				c.lineWidth = s.shadow.width;
				c.lineJoin = "round";
				c.lineCap = "round";
				c.beginPath();
				s.shadow.edge.forEach((q, i) => {
					if (i === 0) {
						c.moveTo(q.x, q.y);
					} else {
						c.lineTo(q.x, q.y);
					}
				});
				c.stroke();
				c.restore();
			}
		}
		p.detail?.(c);
	};
	for (const p of parts) {
		if (!p.late) {
			paint(ctx, p);
		}
	}
	const late = parts.filter((p) => p.late);

	const anchors: FigureAnchors = {
		head: { x: headC.x, y: headC.y, r: body.headR * headC.k },
		number: { x: 0, y: 0, h: 0, side: 0 },
		letters: { x: 0, y: 0, w: 0 },
		front,
		holding: held !== undefined,
		...(ballAt ? { ball: ballAt } : {}),
		...(late.length > 0
			? {
					over: (c: CanvasRenderingContext2D) => {
						for (const p of late) {
							paint(c, p);
						}
					},
				}
			: {}),
	};
	if (Math.abs(front) >= 0.28) {
		const lambda = 0.52;
		const face = (front > 0 ? 1 : -1) * body.depth * 0.56;
		const c = at({
			f: sk.pelvis.f + (sk.chest.f - sk.pelvis.f) * lambda + face,
			s: 0,
			u: sk.pelvis.u + (sk.chest.u - sk.pelvis.u) * lambda,
		});
		anchors.number = {
			x: c.x,
			y: c.y,
			h: 0.62 * c.k,
			side: front > 0 ? 1 : -1,
		};
		const top = at({
			f: sk.pelvis.f + (sk.chest.f - sk.pelvis.f) * 0.84 + face,
			s: 0,
			u: sk.pelvis.u + (sk.chest.u - sk.pelvis.u) * 0.84,
		});
		anchors.letters = {
			x: top.x,
			y: top.y,
			w: body.shoulderW * 1.5 * top.k * Math.abs(front),
		};
	}
	return anchors;
};

// His head on its own, for a sprite to draw over his body.
export const drawHeadAt = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	st: PlayerState,
	body: Body,
	look: Look,
) => {
	const sk = skeleton(body, posed(st.anim, st.phase, st.dribble));
	const at = (v: V3): Projected => project(cam, bodyPoint(st, v));
	const toCamX = cam.pos.x - st.x;
	const toCamY = cam.pos.y - st.y;
	const toCamL = Math.hypot(toCamX, toCamY) || 1;
	const front =
		(Math.cos(st.yaw) * toCamX + Math.sin(st.yaw) * toCamY) / toCamL;
	const headC = at(sk.head);
	drawHead(ctx, headC, body.headR * headC.k, look, front, at, sk.head);
};

// A sneaker, from the ankle to the toe: the upper - low at the toe, high at
// the collar - and the sole along its bottom.
const sneaker = (
	ankle: Projected,
	toe: Projected,
	k: number,
	headOn: boolean,
): { upper: Path2D; sole: Path2D } => {
	const dx = toe.x - ankle.x;
	const dy = toe.y - ankle.y;
	const len = Math.hypot(dx, dy);
	const h = 0.36 * k;
	// Pointing at or away from the camera, a shoe is its rounded front: a toe
	// box over a sole.
	if (headOn || len < 0.42 * k) {
		const c = lerp2(ankle, toe, 0.7);
		const w = 0.3 * k;
		const upper = new Path2D();
		upper.ellipse(c.x, c.y + h * 0.1, w, h * 0.58, 0, 0, Math.PI * 2);
		const sole = new Path2D();
		sole.ellipse(c.x, c.y + h * 0.48, w * 0.96, h * 0.2, 0, 0, Math.PI * 2);
		return { upper, sole };
	}
	const along = len;
	const ux = len > 0.01 ? dx / len : 1;
	const uy = len > 0.01 ? dy / len : 0;
	// Screen up for the shoe is straight up; the floor is below.
	const heel = {
		x: ankle.x - ux * along * 0.32,
		y: ankle.y - uy * along * 0.32,
	};
	const tip = {
		x: ankle.x + ux * along * 1.08,
		y: ankle.y + uy * along * 1.08,
	};
	const floorY = (p: P2) => p.y + h * 0.42;
	const instep = lerp2(heel, tip, 0.62);
	const upper = softPoly([
		{ x: heel.x, y: floorY(heel) },
		{ x: tip.x, y: floorY(tip) },
		{ x: tip.x + ux * h * 0.15, y: tip.y - h * 0.05 },
		{ x: instep.x, y: instep.y - h * 0.42 },
		{ x: ankle.x + ux * h * 0.08, y: ankle.y - h * 0.78 },
		{ x: heel.x - ux * h * 0.12, y: heel.y - h * 0.62 },
	]);
	const sole = softPoly([
		{ x: heel.x - ux * h * 0.06, y: floorY(heel) - h * 0.22 },
		{ x: tip.x + ux * h * 0.12, y: floorY(tip) - h * 0.2 },
		{ x: tip.x + ux * h * 0.1, y: floorY(tip) + h * 0.08 },
		{ x: heel.x - ux * h * 0.04, y: floorY(heel) + h * 0.08 },
	]);
	return { upper, sole };
};

const drawHead = (
	ctx: CanvasRenderingContext2D,
	c: Projected,
	r: number,
	look: Look,
	front: number,
	at: (v: V3) => Projected,
	head: V3,
) => {
	// Which way his nose points on screen.
	const ahead = at({ f: head.f + 1, s: head.s, u: head.u });
	const turn = Math.sign(ahead.x - c.x) || 1;
	const sprite = look.head;
	// The back of his head - his hair over it, his ears either side - sized to
	// the face that turns into it.
	const back = () => {
		const p = new Path2D();
		p.ellipse(c.x, c.y + r * 0.04, r * 0.8, r * 0.98, 0, 0, Math.PI * 2);
		ctx.fillStyle = look.skin;
		ctx.fill(p);
		ctx.fillStyle = shade(look.skin, -0.1);
		for (const s of [-1, 1]) {
			ctx.beginPath();
			ctx.ellipse(
				c.x + s * r * 0.78,
				c.y + r * 0.12,
				r * 0.13,
				r * 0.22,
				0,
				0,
				Math.PI * 2,
			);
			ctx.fill();
		}
		if (look.hair !== look.skin) {
			ctx.fillStyle = look.hair;
			ctx.beginPath();
			ctx.ellipse(c.x, c.y - r * 0.1, r * 0.78, r * 0.86, 0, 0, Math.PI * 2);
			ctx.fill();
		}
	};
	if (!sprite) {
		back();
		return;
	}
	// Facing away, or turning away: the back of his head (under his face, as
	// he turns, so the head is never see-through).
	if (front < -0.16) {
		back();
	}
	// His face, cheated toward the camera the way a cartoon is: even side on,
	// most of it shows, shifted the way he looks.
	const vis = Math.min(1, (front + 0.3) / 0.14);
	if (vis <= 0) {
		return;
	}
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

// What the two teams wear: the home team in white, the visitors in their
// color.
export const kitsFor = (
	away: [string, string, string] | undefined,
	home: [string, string, string] | undefined,
): [Kit, Kit] => [
	uniform(away ?? ["#1d3461", "#f28c28", "#ffffff"], "road"),
	uniform(home ?? ["#8c1d40", "#f2c14e", "#ffffff"], "home"),
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
