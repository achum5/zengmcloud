import { depthOf, project, type Camera } from "./camera.ts";
import type { Pt3 } from "./geometry.ts";

// A FLAT PICTURE ON A PLANE IN THE WORLD - the floor, the stands, the
// scorer's table - drawn on the canvas in perspective.
//
// A canvas can only stretch a picture evenly (an affine map), so the plane is
// cut into a grid of small triangles and each is drawn with the affine map
// that fits its own three corners: piece by piece, perspective.

export type TexturedPlane = {
	origin: Pt3;
	// The world step for one texture pixel along its width, and down its height.
	alongX: Pt3;
	alongY: Pt3;
	// Its size, in texture pixels.
	w: number;
	h: number;
};

type Q = { x: number; y: number };

const drawTriangle = (
	ctx: CanvasRenderingContext2D,
	img: HTMLCanvasElement,
	p0: Q,
	p1: Q,
	p2: Q,
	t0: Q,
	t1: Q,
	t2: Q,
) => {
	const d = (t1.x - t0.x) * (t2.y - t0.y) - (t2.x - t0.x) * (t1.y - t0.y);
	if (Math.abs(d) < 1e-9) {
		return;
	}
	// The map from the texture to the screen that puts each corner in place.
	const a = ((p1.x - p0.x) * (t2.y - t0.y) - (p2.x - p0.x) * (t1.y - t0.y)) / d;
	const b = ((p1.y - p0.y) * (t2.y - t0.y) - (p2.y - p0.y) * (t1.y - t0.y)) / d;
	const c = ((p2.x - p0.x) * (t1.x - t0.x) - (p1.x - p0.x) * (t2.x - t0.x)) / d;
	const e = ((p2.y - p0.y) * (t1.x - t0.x) - (p1.y - p0.y) * (t2.x - t0.x)) / d;
	const f = p0.x - a * t0.x - c * t0.y;
	const g = p0.y - b * t0.x - e * t0.y;
	// A hair bigger than the triangle, so neighbors overlap and no seam shows.
	const cx = (p0.x + p1.x + p2.x) / 3;
	const cy = (p0.y + p1.y + p2.y) / 3;
	const grow = (p: Q) => {
		const dx = p.x - cx;
		const dy = p.y - cy;
		const l = Math.hypot(dx, dy) || 1;
		return { x: p.x + (dx / l) * 0.6, y: p.y + (dy / l) * 0.6 };
	};
	const q0 = grow(p0);
	const q1 = grow(p1);
	const q2 = grow(p2);
	// Only the part of the texture this triangle shows.
	const sx = Math.max(0, Math.floor(Math.min(t0.x, t1.x, t2.x)) - 1);
	const sy = Math.max(0, Math.floor(Math.min(t0.y, t1.y, t2.y)) - 1);
	const ex = Math.ceil(Math.max(t0.x, t1.x, t2.x)) + 1;
	const ey = Math.ceil(Math.max(t0.y, t1.y, t2.y)) + 1;
	ctx.save();
	ctx.beginPath();
	ctx.moveTo(q0.x, q0.y);
	ctx.lineTo(q1.x, q1.y);
	ctx.lineTo(q2.x, q2.y);
	ctx.closePath();
	ctx.clip();
	ctx.transform(a, b, c, e, f, g);
	ctx.drawImage(img, sx, sy, ex - sx, ey - sy, sx, sy, ex - sx, ey - sy);
	ctx.restore();
};

// A plane whose rows run straight along the court, seen by a camera that
// faces straight across it (the broadcast camera, which slides along the
// sideline without turning): each row of the picture is one row of the
// texture, stretched across. So it is drawn a row of the picture at a time -
// exact, and far cheaper than the triangles. Where the camera turns, it
// can't be, and returns false.
const drawRows = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	plane: TexturedPlane,
	img: HTMLCanvasElement,
	alpha: number,
): boolean => {
	const { origin: o, alongX: ax, alongY: ay } = plane;
	if (
		Math.abs(cam.right.y) > 1e-9 ||
		Math.abs(cam.fwd.x) > 1e-9 ||
		ax.y !== 0 ||
		ax.z !== 0 ||
		ay.x !== 0
	) {
		return false;
	}
	// How far up the picture, and how deep, a row v of the plane is - each
	// a straight line in v (the floor beneath it, when the camera is
	// upright) - so v comes straight back from a row of the picture.
	const p = cam.pos;
	const floorZ = (z: number) => (cam.upright ? 0 : z);
	const lift = (z: number) => (cam.upright ? z : 0);
	const up0 =
		(o.y - p.y) * cam.up.y + (floorZ(o.z) - p.z) * cam.up.z + lift(o.z);
	const upV = ay.y * cam.up.y + floorZ(ay.z) * cam.up.z + lift(ay.z);
	const dep0 = (o.y - p.y) * cam.fwd.y + (floorZ(o.z) - p.z) * cam.fwd.z;
	const depV = ay.y * cam.fwd.y + floorZ(ay.z) * cam.fwd.z;
	if (dep0 < 1 || dep0 + depV * plane.h < 1) {
		return false;
	}
	const rowY = (v: number) =>
		cam.cy - (cam.f * (up0 + upV * v)) / (dep0 + depV * v);
	const yA = rowY(0);
	const yB = rowY(plane.h);
	const top = Math.max(0, Math.floor(Math.min(yA, yB)));
	const bottom = Math.min(cam.viewH, Math.ceil(Math.max(yA, yB)));
	const sx = img.width / plane.w;
	const sy = img.height / plane.h;
	ctx.save();
	ctx.globalAlpha *= alpha;
	for (let r = top; r < bottom; r++) {
		// The plane's row at the middle of this row of the picture.
		const c = cam.cy - (r + 0.5);
		const v = (cam.f * up0 - c * dep0) / (c * depV - cam.f * upV);
		if (!(v >= 0 && v < plane.h)) {
			continue;
		}
		const k = cam.f / (dep0 + depV * v);
		const x0 = cam.cx + (o.x - p.x) * k;
		const perU = ax.x * k;
		// Only the part of the row on the picture.
		const left = Math.max(0, Math.min(x0, x0 + perU * plane.w));
		const right = Math.min(cam.viewW, Math.max(x0, x0 + perU * plane.w));
		if (right - left < 0.5) {
			continue;
		}
		const u0 = (left - x0) / perU;
		const u1 = (right - x0) / perU;
		const su = Math.min(u0, u1) * sx;
		const sw = Math.max(0.5, Math.abs(u1 - u0) * sx);
		ctx.drawImage(
			img,
			su,
			Math.min(img.height - 1, Math.floor(v * sy)),
			Math.min(sw, img.width - su),
			1,
			left,
			r,
			right - left,
			1,
		);
	}
	ctx.restore();
	return true;
};

export const drawTexturedPlane = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	plane: TexturedPlane,
	img: HTMLCanvasElement,
	cols: number,
	rows: number,
	alpha = 1,
) => {
	if (alpha <= 0.004) {
		return;
	}
	if (drawRows(ctx, cam, plane, img, alpha)) {
		return;
	}
	// The texture may be drawn at another resolution than the plane counts.
	const sx = img.width / plane.w;
	const sy = img.height / plane.h;
	const pts: (Q | undefined)[] = [];
	for (let j = 0; j <= rows; j++) {
		for (let i = 0; i <= cols; i++) {
			const u = (i / cols) * plane.w;
			const v = (j / rows) * plane.h;
			const world = {
				x: plane.origin.x + plane.alongX.x * u + plane.alongY.x * v,
				y: plane.origin.y + plane.alongX.y * u + plane.alongY.y * v,
				z: plane.origin.z + plane.alongX.z * u + plane.alongY.z * v,
			};
			pts.push(depthOf(cam, world) > 1 ? project(cam, world) : undefined);
		}
	}
	const W = cam.viewW;
	const H = cam.viewH;
	ctx.save();
	ctx.globalAlpha *= alpha;
	for (let j = 0; j < rows; j++) {
		for (let i = 0; i < cols; i++) {
			const a = pts[j * (cols + 1) + i];
			const b = pts[j * (cols + 1) + i + 1];
			const c = pts[(j + 1) * (cols + 1) + i + 1];
			const d = pts[(j + 1) * (cols + 1) + i];
			if (!a || !b || !c || !d) {
				continue;
			}
			// Off the picture: nothing to draw.
			if (
				Math.max(a.x, b.x, c.x, d.x) < 0 ||
				Math.min(a.x, b.x, c.x, d.x) > W ||
				Math.max(a.y, b.y, c.y, d.y) < 0 ||
				Math.min(a.y, b.y, c.y, d.y) > H
			) {
				continue;
			}
			const ta = { x: (i / cols) * plane.w * sx, y: (j / rows) * plane.h * sy };
			const tb = {
				x: ((i + 1) / cols) * plane.w * sx,
				y: (j / rows) * plane.h * sy,
			};
			const tc = {
				x: ((i + 1) / cols) * plane.w * sx,
				y: ((j + 1) / rows) * plane.h * sy,
			};
			const td = {
				x: (i / cols) * plane.w * sx,
				y: ((j + 1) / rows) * plane.h * sy,
			};
			drawTriangle(ctx, img, a, b, c, ta, tb, tc);
			drawTriangle(ctx, img, a, c, d, ta, tc, td);
		}
	}
	ctx.restore();
};
