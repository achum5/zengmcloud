import { project, type Camera, type Projected } from "./camera.ts";
import { bodyPoint, type PlayerState } from "./evaluate.ts";
import type { HeadSprite } from "./faces.ts";
import { poseAt, skeleton, type Body, type Limb, type V3 } from "./poses.ts";

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

export type Look = {
	kit: Kit;
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

type Shape = {
	path: Path2D;
	fill: string | CanvasGradient;
	// The second tone, along the side away from the light.
	shadow?: { edge: P2[]; width: number; color: string };
};
type Part = {
	depth: number;
	shapes: Shape[];
	// Drawn over the part's colors: seams, stripes, lettering.
	detail?: () => void;
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
	const sk = skeleton(body, poseAt(st.anim, st.phase));
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
		const sk0 = dim(skin, far);
		const skinShadow = shade(sk0, -0.2);
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
		// Crew socks, up to mid-calf.
		const sockTop = lerp2(knee, ankle, 0.6);
		shapes.push(
			shaped(
				limbShape(sockTop, knee.k, ankle, ankle.k, [
					[0, body.calfR * 0.84],
					[1, body.ankleR * 1.08],
				]),
				dim(kit.sock, far),
				shade(dim(kit.sock, far), -0.18),
			),
		);
		// The sneaker: a sole along the floor, a toe box, a high collar at the
		// ankle.
		const shoe = sneaker(ankle, toe, ankle.k, Math.abs(front) > 0.62);
		shapes.push({ path: shoe.upper, fill: dim(kit.shoe, far) });
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
				ctx.fillStyle = kit.sole;
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
	parts.push({
		depth: torsoDepth,
		shapes: [
			shaped(neck, skin, shade(skin, -0.2)),
			{ path: polyPath(waistPts), fill: kit.shorts },
			{
				path: softPoly(jerseyPts),
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
			drawCollar();
		},
	});

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

	const arm = (limb: Limb, far: boolean) => {
		const sh = at(limb.root);
		const el = at(limb.mid);
		const wrist = at(limb.end);
		const c = dim(skin, far);
		const cs = shade(c, -0.2);
		// The hand: a mitt a little past the wrist.
		const hx = wrist.x - el.x;
		const hy = wrist.y - el.y;
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
					c,
					cs,
				),
				shaped(
					limbShape(el, el.k, wrist, wrist.k, [
						[0, body.foreR * 0.92],
						[0.22, body.foreR * 1.1],
						[0.6, body.foreR * 0.86],
						[1, body.foreR * 0.62],
					]),
					c,
					cs,
				),
				{ path: hand, fill: c },
			],
		});
	};
	arm(sk.armR, !leftFar);
	arm(sk.armL, leftFar);

	parts.sort((p, q) => q.depth - p.depth);
	for (const p of parts) {
		for (const s of p.shapes) {
			ctx.fillStyle = s.fill;
			ctx.fill(s.path);
			if (s.shadow && s.shadow.edge.length > 1) {
				// The second tone: a band along the shadow side, kept inside.
				ctx.save();
				ctx.clip(s.path);
				ctx.strokeStyle = s.shadow.color;
				ctx.lineWidth = s.shadow.width;
				ctx.lineJoin = "round";
				ctx.lineCap = "round";
				ctx.beginPath();
				s.shadow.edge.forEach((q, i) => {
					if (i === 0) {
						ctx.moveTo(q.x, q.y);
					} else {
						ctx.lineTo(q.x, q.y);
					}
				});
				ctx.stroke();
				ctx.restore();
			}
		}
		p.detail?.();
	}

	const anchors: FigureAnchors = {
		head: { x: headC.x, y: headC.y, r: body.headR * headC.k },
		number: { x: 0, y: 0, h: 0, side: 0 },
		letters: { x: 0, y: 0, w: 0 },
		front,
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
	const sk = skeleton(body, poseAt(st.anim, st.phase));
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

// NBA style: home in white with the team's colors on it, the road team in its
// main color - or its darkest one, if its main color is light too.
export const kitsFor = (
	away: [string, string, string] | undefined,
	home: [string, string, string] | undefined,
): [Kit, Kit] => {
	const a = away ?? ["#1d3461", "#f28c28", "#ffffff"];
	const h = home ?? ["#8c1d40", "#f2c14e", "#ffffff"];
	const darkest = [...a].sort((x, y) => luminance(x) - luminance(y))[0]!;
	const road = luminance(a[0]) < 0.6 ? a[0] : darkest;
	const roadTrim =
		a.find((c) => c !== road && contrast(c, road) > 0.25) ?? "#ffffff";
	const roadEdge =
		a.find(
			(c) => c !== road && c !== roadTrim && contrast(c, roadTrim) > 0.2,
		) ?? road;
	const homeMain =
		luminance(h[0]) < 0.7
			? h[0]
			: (h.find((c) => luminance(c) < 0.6) ?? "#222222");
	const homeEdge =
		h.find((c) => c !== homeMain && contrast(c, "#f4f1ea") > 0.2) ?? homeMain;
	const WHITE = "#f4f1ea";
	return [
		{
			jersey: road,
			trim: roadTrim,
			number: roadTrim,
			numberEdge: roadEdge === roadTrim ? shade(road, -0.4) : roadEdge,
			shorts: road,
			stripe: roadTrim,
			sock: shade(road, -0.25),
			shoe: "#1d1d22",
			sole: "#e9e6df",
		},
		{
			jersey: WHITE,
			trim: homeMain,
			number: homeMain,
			numberEdge: homeEdge === homeMain ? shade(homeMain, -0.35) : homeEdge,
			shorts: WHITE,
			stripe: homeMain,
			sock: "#f1f1f1",
			shoe: "#f4f4f4",
			sole: homeMain,
		},
	];
};
