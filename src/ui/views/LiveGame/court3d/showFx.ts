import { standsPoint } from "./arena.ts";
import { depthOf, project, type Camera } from "./camera.ts";
import { COURT_H, COURT_W, type Pt3 } from "./geometry.ts";
import type { ShowFx } from "./pregameShow.ts";

// THE BUILDING'S OWN SHOW, drawn over the dark (see lights.ts) in the
// pregame show (see pregameShow.ts): the fire going up off the floor, the
// beams swept over it from the roof, and the phones held up all round the
// stands - each added to the picture as light.

const rgba = (hex: string, a: number): string => {
	const h = hex.replace("#", "");
	const full =
		h.length === 3
			? h
					.split("")
					.map((ch) => ch + ch)
					.join("")
			: h.padEnd(6, "0");
	const n = Number.parseInt(full.slice(0, 6), 16);
	if (!Number.isFinite(n)) {
		return `rgba(255,255,255,${a})`;
	}
	return `rgba(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255}, ${a})`;
};

const ahead = (cam: Camera, p: Pt3) => depthOf(cam, p) > 1;

// A soft round light at a point, `r` feet across as the camera sees it.
const blob = (
	c: CanvasRenderingContext2D,
	x: number,
	y: number,
	r: number,
	inner: string,
	outer: string,
) => {
	const g = c.createRadialGradient(x, y, 0, x, y, r);
	g.addColorStop(0, inner);
	g.addColorStop(1, outer);
	c.fillStyle = g;
	c.beginPath();
	c.arc(x, y, r, 0, Math.PI * 2);
	c.fill();
};

// One jet of fire: up to this high (feet) at its fullest, flickering - a
// fat column, white-hot at the foot and billowing out orange toward the
// top, the air round it lit.
const JET_H = 18;
const drawJet = (
	c: CanvasRenderingContext2D,
	cam: Camera,
	at: Pt3,
	k: number,
	now: number,
	i: number,
) => {
	if (!ahead(cam, at)) {
		return;
	}
	const base = project(cam, at);
	const h = JET_H * k * (0.92 + 0.08 * Math.sin(now / 37 + i * 1.9));
	// The floor round it, lit orange.
	const glowR = Math.max(2, base.k * 9 * k);
	c.save();
	c.translate(base.x, base.y);
	c.scale(1, 0.35);
	blob(
		c,
		0,
		0,
		glowR,
		`rgba(255, 140, 40, ${0.45 * k})`,
		"rgba(255, 90, 20, 0)",
	);
	c.restore();
	// The air round it.
	const mid = project(cam, { x: at.x, y: at.y, z: h * 0.55 });
	blob(
		c,
		mid.x,
		mid.y,
		Math.max(3, mid.k * 9),
		`rgba(255, 120, 30, ${0.2 * k})`,
		"rgba(255, 80, 10, 0)",
	);
	// The column: blobs up it, close enough to run together.
	const N = 16;
	for (let n = 0; n < N; n++) {
		const u = n / (N - 1);
		const z = h * u;
		const sway =
			(Math.sin(now / 70 + i + u * 4) + 0.5 * Math.sin(now / 23 + n)) *
			0.45 *
			u;
		const p = project(cam, { x: at.x + sway, y: at.y, z });
		const r = Math.max(1.5, p.k * (0.9 + 1.9 * u) * (0.7 + 0.3 * k));
		const a = (0.62 - 0.32 * u) * k;
		const core =
			u < 0.25
				? `rgba(255, 248, 224, ${a})`
				: u < 0.6
					? `rgba(255, 196, 80, ${a})`
					: `rgba(255, 112, 28, ${a * 0.85})`;
		blob(c, p.x, p.y, r, core, "rgba(255, 70, 10, 0)");
	}
};

// A beam from up in the roof down to the floor, in the team's color: thin
// at the top, wide where it comes down, and a pool where it lands.
const drawBeam = (
	c: CanvasRenderingContext2D,
	cam: Camera,
	b: ShowFx["beams"][number],
	dark: number,
) => {
	if (!ahead(cam, b.to)) {
		return;
	}
	const to = project(cam, b.to);
	// The top: where the beam leaves the roof, or the top of the picture if
	// that is behind the camera.
	const src = ahead(cam, b.from)
		? project(cam, b.from)
		: { x: to.x, y: -cam.viewH * 0.2, k: to.k };
	const w0 = Math.max(1, src.k * 0.8);
	const w1 = Math.max(2, to.k * 3.6);
	const dx = to.x - src.x;
	const dy = to.y - src.y;
	const l = Math.hypot(dx, dy) || 1;
	const nx = -dy / l;
	const ny = dx / l;
	const g = c.createLinearGradient(src.x, src.y, to.x, to.y);
	g.addColorStop(0, rgba(b.color, 0));
	g.addColorStop(1, rgba(b.color, 0.2 * dark));
	c.fillStyle = g;
	c.beginPath();
	c.moveTo(src.x + nx * w0, src.y + ny * w0);
	c.lineTo(src.x - nx * w0, src.y - ny * w0);
	c.lineTo(to.x - nx * w1, to.y - ny * w1);
	c.lineTo(to.x + nx * w1, to.y + ny * w1);
	c.closePath();
	c.fill();
	c.save();
	c.translate(to.x, to.y);
	c.scale(1, 0.4);
	blob(c, 0, 0, w1 * 1.4, rgba(b.color, 0.3 * dark), rgba(b.color, 0));
	c.restore();
};

// THE PHONES, held up all round the stands in the dark: little lights
// scattered up the far stands and the near ones, each twinkling on its own.
const PHONES = 150;
const phonePoints: Pt3[] = (() => {
	const out: Pt3[] = [];
	let a = 1234567;
	const r = () => {
		a = (Math.imul(a, 1103515245) + 12345) >>> 0;
		return a / 4294967296;
	};
	for (let i = 0; i < PHONES; i++) {
		const p = standsPoint(-30 + r() * (COURT_W + 60), 3 + r() * 55);
		out.push(
			i % 3 === 2
				? // The near stands: the far ones turned half way round.
					{ x: COURT_W - p.x, y: COURT_H - p.y, z: p.z }
				: p,
		);
	}
	return out;
})();

const drawPhones = (
	c: CanvasRenderingContext2D,
	cam: Camera,
	dark: number,
	now: number,
) => {
	const px = Math.max(1, Math.round(cam.viewH / 360));
	for (let i = 0; i < phonePoints.length; i++) {
		const p = phonePoints[i]!;
		if (!ahead(cam, p)) {
			continue;
		}
		const s = project(cam, p);
		if (s.x < 0 || s.y < 0 || s.x > cam.viewW || s.y > cam.viewH) {
			continue;
		}
		const tw = 0.5 + 0.5 * Math.sin(now / 520 + i * 2.39);
		c.fillStyle = `rgba(235, 242, 255, ${(0.35 + 0.55 * tw) * dark})`;
		c.fillRect(Math.round(s.x), Math.round(s.y), px, px);
	}
};

export const drawShowFx = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	fx: ShowFx,
	now: number,
) => {
	ctx.save();
	ctx.setTransform(1, 0, 0, 1, 0, 0);
	ctx.globalCompositeOperation = "lighter";
	if (fx.phones) {
		drawPhones(ctx, cam, fx.dark, now);
	}
	for (const b of fx.beams) {
		drawBeam(ctx, cam, b, fx.dark);
	}
	fx.jets.forEach((j, i) => drawJet(ctx, cam, j.at, j.k, now, i));
	ctx.restore();
};
