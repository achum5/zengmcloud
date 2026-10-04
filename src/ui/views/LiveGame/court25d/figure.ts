import { project, type Camera, type Projected } from "./camera.ts";
import { bodyPoint, type PlayerState } from "./evaluate.ts";
import type { HeadSprite } from "./faces.ts";
import { poseAt, skeleton, type Body, type Limb, type V3 } from "./poses.ts";

// ONE PLAYER, DRAWN.
//
// His skeleton is posed, turned to face where he faces, and every joint is
// put through the camera. Each limb is then drawn as a tapered tube (two
// circles and the lines touching both), the torso as the outline around his
// shoulders and hips, and his face on top - farthest parts first, so an arm
// raised in front of his chest covers it and one behind it does not.

export type Kit = {
	jersey: string;
	trim: string;
	number: string;
	sock: string;
	shoe: string;
};

export type Look = {
	kit: Kit;
	skin: string;
	hair: string;
	jerseyNumber: string;
	head?: HeadSprite;
};

type P2 = { x: number; y: number };

// The outline of two circles and the lines touching both: a limb.
const capsule = (
	ctx: CanvasRenderingContext2D,
	a: P2,
	ra: number,
	b: P2,
	rb: number,
) => {
	const dx = b.x - a.x;
	const dy = b.y - a.y;
	const d = Math.hypot(dx, dy);
	ctx.beginPath();
	if (d <= Math.abs(ra - rb) + 0.01) {
		const big = ra >= rb ? a : b;
		ctx.arc(big.x, big.y, Math.max(ra, rb), 0, Math.PI * 2);
	} else {
		const al = Math.atan2(dy, dx);
		const be = Math.acos(Math.max(-1, Math.min(1, (ra - rb) / d)));
		ctx.arc(a.x, a.y, ra, al + be, al - be + Math.PI * 2);
		ctx.arc(b.x, b.y, rb, al - be, al + be);
		ctx.closePath();
	}
	ctx.fill();
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

const fillPoly = (ctx: CanvasRenderingContext2D, pts: P2[]) => {
	if (pts.length < 3) {
		return;
	}
	ctx.beginPath();
	ctx.moveTo(pts[0]!.x, pts[0]!.y);
	for (let i = 1; i < pts.length; i++) {
		ctx.lineTo(pts[i]!.x, pts[i]!.y);
	}
	ctx.closePath();
	ctx.fill();
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

const lerp2 = (a: P2, b: P2, f: number): P2 => ({
	x: a.x + (b.x - a.x) * f,
	y: a.y + (b.y - a.y) * f,
});

type Part = { depth: number; draw: () => void };

export type FigureMode = "body" | "reflection";

export const drawFigure = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	st: PlayerState,
	body: Body,
	look: Look,
	mode: FigureMode = "body",
) => {
	const sk = skeleton(body, poseAt(st.anim, st.phase));
	const mirror = mode === "reflection";
	const at = (v: V3): Projected => {
		const p = bodyPoint(st, v);
		return project(cam, mirror ? { ...p, z: -p.z } : p);
	};
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

	const skin = look.skin;
	const kit = look.kit;
	// The side away from the camera is in his own shadow.
	const dim = (c: string, far: boolean) => (far ? shade(c, -0.18) : c);
	const leftFar = side < 0;

	const parts: Part[] = [];

	// A limb, lit from the arena lights above: the edges fall into shadow
	// and a lighter core runs down the middle.
	const tube = (a: P2, ra: number, b: P2, rb: number, color: string) => {
		if (mirror) {
			ctx.fillStyle = color;
			capsule(ctx, a, ra, b, rb);
			return;
		}
		ctx.fillStyle = shade(color, -0.16);
		capsule(ctx, a, ra, b, rb);
		ctx.fillStyle = shade(color, 0.05);
		capsule(
			ctx,
			{ x: a.x - ra * 0.14, y: a.y - ra * 0.16 },
			ra * 0.62,
			{ x: b.x - rb * 0.14, y: b.y - rb * 0.16 },
			rb * 0.62,
		);
	};
	const ball = (c: P2, r: number, color: string) => {
		tube(c, r, c, r, color);
	};

	// Rings round his middle at a height up the spine (0 hips, 1 shoulders):
	// a torso or a waistband, seen from wherever the camera is.
	const ring = (lambda: number, lat: number, dep: number): Projected[] => {
		const f = sk.pelvis.f + (sk.chest.f - sk.pelvis.f) * lambda;
		const u = sk.pelvis.u + (sk.chest.u - sk.pelvis.u) * lambda;
		const out: Projected[] = [];
		for (let i = 0; i < 12; i++) {
			const a = (i / 12) * Math.PI * 2;
			out.push(at({ f: f + Math.cos(a) * dep, s: Math.sin(a) * lat, u }));
		}
		return out;
	};
	// Lit from above: lighter at the shoulders, darker at the waist.
	const litFill = (top: P2, bottom: P2, color: string) => {
		if (mirror) {
			return color;
		}
		const g = ctx.createLinearGradient(top.x, top.y, bottom.x, bottom.y);
		g.addColorStop(0, shade(color, 0.12));
		g.addColorStop(0.5, color);
		g.addColorStop(1, shade(color, -0.2));
		return g;
	};

	const leg = (limb: Limb, far: boolean) => {
		const hip = at(limb.root);
		const knee = at(limb.mid);
		const ankle = at(limb.end);
		const toe = at(limb.tip ?? limb.end);
		parts.push({
			depth: knee.depth + (far ? 0.6 : 0),
			draw: () => {
				const sk0 = dim(skin, far);
				tube(knee, body.kneeR * knee.k, ankle, body.ankleR * ankle.k, sk0);
				tube(hip, body.thighR * hip.k, knee, body.kneeR * knee.k, sk0);
				if (!mirror) {
					const sockTop = lerp2(knee, ankle, 0.62);
					tube(
						sockTop,
						body.calfR * 0.8 * ankle.k,
						ankle,
						body.ankleR * 1.15 * ankle.k,
						dim(kit.sock, far),
					);
				}
				tube(
					ankle,
					body.ankleR * 1.55 * ankle.k,
					toe,
					body.ankleR * 1.3 * toe.k,
					dim(kit.shoe, far),
				);
				// Baggy shorts down to just above the knee.
				const hem = lerp2(hip, knee, 0.8);
				tube(
					hip,
					body.thighR * 1.34 * hip.k,
					hem,
					body.thighR * 1.2 * knee.k,
					dim(kit.jersey, far),
				);
			},
		});
	};
	leg(sk.legR, !leftFar);
	leg(sk.legL, leftFar);

	// The top of the shorts, so the two legs read as one pair.
	const pelvis = at(sk.pelvis);
	const waist = [
		...ring(0.1, body.hipW * 1.55, body.depth * 0.5),
		...ring(-0.16, body.hipW * 1.6, body.depth * 0.5),
	];
	parts.push({
		depth: pelvis.depth + 0.05,
		draw: () => {
			ctx.fillStyle = litFill(
				at(off(sk.pelvis, 0, 0, 0.3)),
				pelvis,
				kit.jersey,
			);
			fillPoly(ctx, hull(waist));
		},
	});

	// The torso: rounded through the chest, narrower at the waist, the
	// jersey's straps inside the shoulders.
	const chest = at(sk.chest);
	const torsoPts = [
		...ring(1, body.shoulderW * 0.86, body.depth * 0.4),
		...ring(0.72, body.shoulderW * 0.95, body.depth * 0.52),
		...ring(0.36, body.hipW * 1.55, body.depth * 0.48),
		...ring(0.08, body.hipW * 1.45, body.depth * 0.46),
	];
	const neckBase = at(off(sk.chest, 0, 0, body.H * 0.01));
	const headC = at(sk.head);
	parts.push({
		depth: (chest.depth + pelvis.depth) / 2,
		draw: () => {
			// The neck, under the collar.
			tube(
				neckBase,
				body.headR * 0.5 * neckBase.k,
				headC,
				body.headR * 0.42 * headC.k,
				skin,
			);
			const outline = hull(torsoPts);
			ctx.fillStyle = litFill(chest, pelvis, kit.jersey);
			fillPoly(ctx, outline);
			if (mirror) {
				return;
			}
			// Piping round the jersey's edge.
			ctx.strokeStyle = kit.trim;
			ctx.lineWidth = Math.max(0.6, 0.05 * chest.k);
			ctx.lineJoin = "round";
			ctx.stroke();
			drawNumber();
		},
	});

	// The number, on his chest or his back, whichever the camera sees.
	const drawNumber = () => {
		if (Math.abs(front) < 0.3 || !look.jerseyNumber) {
			return;
		}
		const onFront = front > 0;
		const lift = body.torso * 0.5;
		const face = onFront ? body.depth * 0.52 : -body.depth * 0.5;
		const o = at(off(sk.pelvis, face, 0, lift));
		// His left-to-right across the screen, and his spine.
		let a = at(off(sk.pelvis, face, body.hipW, lift));
		let b = at(off(sk.pelvis, face, -body.hipW, lift));
		if (a.x > b.x) {
			[a, b] = [b, a];
		}
		const up = at(off(sk.pelvis, face, 0, lift + 0.4));
		const ux = (b.x - a.x) / (2 * body.hipW);
		const uy = (b.y - a.y) / (2 * body.hipW);
		const vx = (o.x - up.x) / 0.4;
		const vy = (o.y - up.y) / 0.4;
		ctx.save();
		ctx.setTransform(
			ctx.getTransform().multiply(new DOMMatrix([ux, uy, vx, vy, o.x, o.y])),
		);
		ctx.font = "800 0.5px Arial, Helvetica, sans-serif";
		ctx.textAlign = "center";
		ctx.textBaseline = "middle";
		ctx.lineWidth = 0.06;
		ctx.strokeStyle = kit.trim;
		ctx.strokeText(look.jerseyNumber, 0, 0);
		ctx.fillStyle = kit.number;
		ctx.fillText(look.jerseyNumber, 0, 0);
		ctx.restore();
	};

	const arm = (limb: Limb, far: boolean) => {
		const sh = at(limb.root);
		const el = at(limb.mid);
		const hand = at(limb.end);
		parts.push({
			depth: (el.depth + hand.depth) / 2 + (far ? 0.5 : -0.2),
			draw: () => {
				const c = dim(skin, far);
				// The shoulder, the upper arm, the forearm, the hand.
				ball(sh, body.upperR * 1.25 * sh.k, c);
				tube(sh, body.upperR * sh.k, el, body.foreR * 1.05 * el.k, c);
				tube(el, body.foreR * el.k, hand, body.handR * 0.8 * hand.k, c);
				ball(hand, body.handR * hand.k, c);
			},
		});
	};
	arm(sk.armR, !leftFar);
	arm(sk.armL, leftFar);

	parts.sort((p, q) => q.depth - p.depth);
	for (const p of parts) {
		p.draw();
	}

	drawHead(ctx, headC, body.headR * headC.k, look, front, mirror, at, sk.head);
};

const drawHead = (
	ctx: CanvasRenderingContext2D,
	c: Projected,
	r: number,
	look: Look,
	front: number,
	mirror: boolean,
	at: (v: V3) => Projected,
	head: V3,
) => {
	// Which way his nose points on screen.
	const ahead = at({ f: head.f + 1, s: head.s, u: head.u });
	const turn = Math.sign(ahead.x - c.x) || 1;
	const sprite = look.head;
	if (mirror || !sprite || front < -0.15) {
		// The back (or the reflection) of his head.
		ctx.fillStyle = look.skin;
		ctx.beginPath();
		ctx.ellipse(c.x, c.y, r * 0.84, r * 1.02, 0, 0, Math.PI * 2);
		ctx.fill();
		if (!mirror) {
			ctx.fillStyle = look.hair;
			ctx.beginPath();
			ctx.ellipse(
				c.x - turn * r * 0.12 * (1 - Math.abs(front)),
				c.y - r * 0.1,
				r * 0.82,
				r * 0.9,
				0,
				0,
				Math.PI * 2,
			);
			ctx.fill();
		}
		if (mirror || !sprite) {
			return;
		}
	}
	// His face, turned with him: narrower and shifted toward where he looks
	// as he turns from the camera.
	const vis = Math.min(1, (front + 0.15) / 0.4);
	if (vis <= 0) {
		return;
	}
	const squash = 0.62 + 0.38 * Math.max(0, front);
	const shift = turn * (1 - Math.max(0, front)) * r * 0.34;
	const scale = (r * 2.18) / sprite.h;
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

// Home in white, the road team in its colors - unless those are light too, in
// which case it wears its darkest one.
export const kitsFor = (
	away: [string, string, string] | undefined,
	home: [string, string, string] | undefined,
): [Kit, Kit] => {
	const a = away ?? ["#1d3461", "#f28c28", "#ffffff"];
	const h = home ?? ["#8c1d40", "#f2c14e", "#ffffff"];
	const darkest = [...a].sort((x, y) => luminance(x) - luminance(y))[0]!;
	const road = luminance(a[0]) < 0.6 ? a[0] : darkest;
	const roadTrim =
		a.find(
			(c) => c !== road && Math.abs(luminance(c) - luminance(road)) > 0.25,
		) ?? "#ffffff";
	const homeTrim =
		luminance(h[0]) < 0.7
			? h[0]
			: (h.find((c) => luminance(c) < 0.6) ?? "#222222");
	return [
		{
			jersey: road,
			trim: roadTrim,
			number: luminance(road) < 0.45 ? "#ffffff" : "#111111",
			sock: "#f1f1f1",
			shoe: "#1d1d22",
		},
		{
			jersey: "#f4f1ea",
			trim: homeTrim,
			number: homeTrim,
			sock: "#f1f1f1",
			shoe: "#f4f4f4",
		},
	];
};
