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

// THE PICTURE FROM FURTHER OFF. Shrunk on the screen to fewer pixels than
// it has - the far side of the floor, the stands, a smaller picture - each
// pixel of the screen would take just one of the picture's and skip those
// in between: a plank's seam there in one frame and gone the next, the
// floor crawling as the camera moves. So each picture is also kept halved
// across, down or both (up to MAX_LEVEL times each way), every pixel the
// average of the ones it stands for, made the first time it is wanted -
// and each row of the screen drawn from the one with about a pixel of it
// to a pixel of the screen. (The pictures drawn this way are painted once
// - the building's a game, see ArenaPaint, and the stadium's in field3d -
// and never changed after, so these never go stale.)
const MAX_LEVEL = 2;
const levels = new WeakMap<HTMLCanvasElement, Map<number, HTMLCanvasElement>>();
const levelOf = (
	img: HTMLCanvasElement,
	lx: number,
	ly: number,
): HTMLCanvasElement => {
	if (lx === 0 && ly === 0) {
		return img;
	}
	let made = levels.get(img);
	if (!made) {
		made = new Map();
		levels.set(img, made);
	}
	const key = lx * (MAX_LEVEL + 1) + ly;
	let out = made.get(key);
	if (!out) {
		// Halved one way from the one a step finer: each pixel the average
		// of two.
		const down = ly > 0;
		const from = down ? levelOf(img, lx, ly - 1) : levelOf(img, lx - 1, ly);
		out = document.createElement("canvas");
		out.width = down ? from.width : Math.max(1, Math.ceil(from.width / 2));
		out.height = down ? Math.max(1, Math.ceil(from.height / 2)) : from.height;
		const g = out.getContext("2d")!;
		g.imageSmoothingEnabled = true;
		g.drawImage(from, 0, 0, out.width, out.height);
		made.set(key, out);
	}
	return out;
};
// Which of them for so many of the picture's pixels to one of the screen's.
const levelFor = (perPixel: number) =>
	perPixel >= 2 ? Math.min(MAX_LEVEL, Math.floor(Math.log2(perPixel))) : 0;

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
	if (Math.abs(depV) < 1e-9) {
		// Upright and square to the camera (a wall, the front of the table):
		// the same depth all the way down, so the whole of it is one picture
		// stretched evenly - drawn at once, not a row at a time.
		const k = cam.f / dep0;
		const x0 = cam.cx + (o.x - p.x) * k;
		const perU = ax.x * k;
		const pic = levelOf(
			img,
			levelFor(img.width / Math.abs(perU * plane.w)),
			levelFor(img.height / Math.abs(yB - yA)),
		);
		ctx.save();
		ctx.globalAlpha *= alpha;
		ctx.transform(perU, 0, 0, (yB - yA) / plane.h, x0, yA);
		ctx.drawImage(pic, 0, 0, pic.width, pic.height, 0, 0, plane.w, plane.h);
		ctx.restore();
		return true;
	}
	const top = Math.max(0, Math.floor(Math.min(yA, yB)));
	const bottom = Math.min(cam.viewH, Math.ceil(Math.max(yA, yB)));
	// The plane's row at the middle of a row of the picture.
	const vAt = (r: number) => {
		const c = cam.cy - (r + 0.5);
		return (cam.f * up0 - c * dep0) / (c * depV - cam.f * upV);
	};
	ctx.save();
	ctx.globalAlpha *= alpha;
	for (let r = top; r < bottom; r++) {
		const v = vAt(r);
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
		// Drawn from the picture with about a pixel of it to the screen's,
		// across and down.
		const pic = levelOf(
			img,
			levelFor(img.width / plane.w / Math.abs(perU)),
			levelFor((img.height / plane.h) * Math.abs(vAt(r + 1) - v)),
		);
		const sx = pic.width / plane.w;
		const sy = pic.height / plane.h;
		const u0 = (left - x0) / perU;
		const u1 = (right - x0) / perU;
		const su = Math.min(u0, u1) * sx;
		const sw = Math.max(0.5, Math.abs(u1 - u0) * sx);
		ctx.drawImage(
			pic,
			su,
			Math.min(pic.height - 1, Math.floor(v * sy)),
			Math.min(sw, pic.width - su),
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

// A FLAT PLANE SEEN BY A CAMERA TURNED ALONG THE COURT (a replay's, a
// huddle's, the look round the building): the floor - or anything else
// lying flat - is still the same depth all along a row of the picture (the
// camera is level), so a row of the picture is still a straight run across
// the plane, only no longer along a row of its picture: a slant across it.
// So it too is drawn a row of the picture at a time, each row filled with
// the picture laid on it slanted - exact, every pixel of the picture filled
// once. (In triangles, each clipped to its own edges, every edge showed: a
// lattice of faint slanting lines across the floor in every close-up.)
// Anything not lying flat, or a camera tilted over, can't be: false.
const patterns = new WeakMap<HTMLCanvasElement, CanvasPattern | null>();
const patternOf = (ctx: CanvasRenderingContext2D, img: HTMLCanvasElement) => {
	let p = patterns.get(img);
	if (p === undefined) {
		p = ctx.createPattern(img, "no-repeat");
		patterns.set(img, p);
	}
	return p ?? undefined;
};
const drawFlatRows = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	plane: TexturedPlane,
	img: HTMLCanvasElement,
	alpha: number,
): boolean => {
	const { origin: o, alongX: ax, alongY: ay } = plane;
	// Where a point is from the camera: across, up and in (as project has
	// it) - for the plane's corner and a step along each of its sides.
	const p = cam.pos;
	const camOf = (q: Pt3) => {
		const dx = q.x - p.x;
		const dy = q.y - p.y;
		const dz = cam.upright ? -p.z : q.z - p.z;
		return {
			X: dx * cam.right.x + dy * cam.right.y + dz * cam.right.z,
			Y:
				dx * cam.up.x + dy * cam.up.y + dz * cam.up.z + (cam.upright ? q.z : 0),
			Z: dx * cam.fwd.x + dy * cam.fwd.y + dz * cam.fwd.z,
		};
	};
	const c0 = camOf(o);
	const cu = camOf({ x: o.x + ax.x, y: o.y + ax.y, z: o.z + ax.z });
	const cv = camOf({ x: o.x + ay.x, y: o.y + ay.y, z: o.z + ay.z });
	const du = { X: cu.X - c0.X, Y: cu.Y - c0.Y, Z: cu.Z - c0.Z };
	const dv = { X: cv.X - c0.X, Y: cv.Y - c0.Y, Z: cv.Z - c0.Z };
	// The plane's (u, v) to the picture's (x, y), and back.
	const { f, cx, cy } = cam;
	const h00 = f * du.X + cx * du.Z;
	const h01 = f * dv.X + cx * dv.Z;
	const h02 = f * c0.X + cx * c0.Z;
	const h10 = -f * du.Y + cy * du.Z;
	const h11 = -f * dv.Y + cy * dv.Z;
	const h12 = -f * c0.Y + cy * c0.Z;
	const h20 = du.Z;
	const h21 = dv.Z;
	const h22 = c0.Z;
	const i00 = h11 * h22 - h12 * h21;
	const i01 = h02 * h21 - h01 * h22;
	const i02 = h01 * h12 - h02 * h11;
	const i10 = h12 * h20 - h10 * h22;
	const i11 = h00 * h22 - h02 * h20;
	const i12 = h02 * h10 - h00 * h12;
	const i20 = h10 * h21 - h11 * h20;
	const i21 = h01 * h20 - h00 * h21;
	const i22 = h00 * h11 - h01 * h10;
	const W = cam.viewW;
	// Not the same depth all along a row: not flat to this camera.
	const wMid = i21 * (cam.viewH / 2) + i22;
	if (!(Math.abs(i20) * W <= 1e-6 * Math.abs(wMid)) || wMid === 0) {
		return false;
	}
	const H = cam.viewH;
	// The point of the plane (u, v) at (x, y) on the picture.
	const at = (x: number, y: number) => {
		const w = i20 * x + i21 * y + i22;
		return {
			u: (i00 * x + i01 * y + i02) / w,
			v: (i10 * x + i11 * y + i12) / w,
		};
	};
	// The run of row r on the plane (x0 to x1; none if x1 <= x0) - or none
	// at all, the row behind the camera or right up against it.
	const runOf = (r: number) => {
		const yc = r + 0.5;
		const w0 = i21 * yc + i22;
		if (Math.abs(w0) < 1e-12) {
			return undefined;
		}
		const ua = i00 / w0;
		const ub = (i01 * yc + i02) / w0;
		const va = i10 / w0;
		const vb = (i11 * yc + i12) / w0;
		const um = ua * (W / 2) + ub;
		const vm = va * (W / 2) + vb;
		if (du.Z * um + dv.Z * vm + c0.Z < 1) {
			return undefined;
		}
		let x0 = 0;
		let x1 = W;
		for (const [a, b, top] of [
			[ua, ub, plane.w],
			[va, vb, plane.h],
		] as const) {
			if (Math.abs(a) < 1e-12) {
				if (b < 0 || b > top) {
					x1 = x0;
				}
				continue;
			}
			const e0 = -b / a;
			const e1 = (top - b) / a;
			x0 = Math.max(x0, Math.min(e0, e1));
			x1 = Math.min(x1, Math.max(e0, e1));
		}
		return x1 - x0 < 0.5 ? undefined : { x0, x1 };
	};
	const sxu = img.width / plane.w;
	const syv = img.height / plane.h;
	// A few rows at a time where one slant does for all of them - to within
	// a third of a pixel at their corners.
	const band = (r: number, k: number, x0: number, x1: number) => {
		const yc = r + k / 2;
		const xc = (x0 + x1) / 2;
		const c = at(xc, yc);
		const ex = at(xc + 1, yc);
		const ey = at(xc, yc + 1);
		const ua = ex.u - c.u;
		const va = ex.v - c.v;
		const uy = ey.u - c.u;
		const vy = ey.v - c.v;
		const pic = levelOf(
			img,
			levelFor(sxu * Math.max(Math.abs(ua), Math.abs(uy))),
			levelFor(syv * Math.max(Math.abs(va), Math.abs(vy))),
		);
		const sx = pic.width / plane.w;
		const sy = pic.height / plane.h;
		// How far off it puts the plane at the band's corners, in pixels of
		// the screen (the slant's own map, undone).
		let err = 0;
		const jd = ua * vy - uy * va;
		if (k > 1 && Math.abs(jd) > 1e-12) {
			for (const x of [x0, x1]) {
				for (const y of [r + 0.5, r + k - 0.5]) {
					const t = at(x, y);
					const eu = t.u - (c.u + ua * (x - xc) + uy * (y - yc));
					const ev = t.v - (c.v + va * (x - xc) + vy * (y - yc));
					err = Math.max(
						err,
						Math.abs(vy * eu - uy * ev) / Math.abs(jd),
						Math.abs(ua * ev - va * eu) / Math.abs(jd),
					);
				}
			}
		}
		return { err, pic, sx, sy, xc, yc, c, ua, va, uy, vy };
	};
	const BAND_ERR = 0.75;
	const BAND_ROWS = [2, 3, 4, 6, 8, 12, 16, 24, 32];
	ctx.save();
	ctx.globalAlpha *= alpha;
	let r = 0;
	while (r < H) {
		const run = runOf(r);
		if (!run) {
			r++;
			continue;
		}
		let k = 1;
		let x0 = run.x0;
		let x1 = run.x1;
		let b = band(r, 1, x0, x1);
		for (const next of BAND_ROWS) {
			if (r + next > H) {
				break;
			}
			const last = runOf(r + next - 1);
			if (!last) {
				break;
			}
			const nx0 = Math.min(x0, last.x0);
			const nx1 = Math.max(x1, last.x1);
			const nb = band(r, next, nx0, nx1);
			if (nb.err > BAND_ERR) {
				break;
			}
			k = next;
			x0 = nx0;
			x1 = nx1;
			b = nb;
		}
		const pat = patternOf(ctx, b.pic);
		if (!pat) {
			ctx.restore();
			return false;
		}
		// The picture's pixel (px, py) at the screen's (x, y): px = sx u,
		// u = c.u + ua (x - xc) + uy (y - yc), the same for py - the pattern
		// wants it the other way round.
		const m00 = b.sx * b.ua;
		const m01 = b.sx * b.uy;
		const m10 = b.sy * b.va;
		const m11 = b.sy * b.vy;
		const t0 = b.sx * (b.c.u - b.ua * b.xc - b.uy * b.yc);
		const t1 = b.sy * (b.c.v - b.va * b.xc - b.vy * b.yc);
		const det = m00 * m11 - m01 * m10;
		if (Math.abs(det) > 1e-12) {
			const a = m11 / det;
			const bb = -m10 / det;
			const c = -m01 / det;
			const d = m00 / det;
			pat.setTransform({
				a,
				b: bb,
				c,
				d,
				e: -(a * t0 + c * t1),
				f: -(bb * t0 + d * t1),
			});
			ctx.fillStyle = pat;
			// (Each row of it only as far as the plane goes - the picture is
			// no wider than the plane, so past its edge the pattern is clear.)
			ctx.fillRect(x0, r, x1 - x0, k);
		}
		r += k;
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
	if (
		drawRows(ctx, cam, plane, img, alpha) ||
		drawFlatRows(ctx, cam, plane, img, alpha)
	) {
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
