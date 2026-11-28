import { BOWL_FAR, BOWL_NEAR } from "../bowlData.ts";
import { depthOf, project, type Camera } from "../camera.ts";
import { courtLineOutlines } from "../courtLines.ts";
import type { Pt3 } from "../geometry.ts";
import type { Painter } from "../painter.ts";
import type { FrameItem } from "../scene.ts";
import { attrib, buffer, compile, css, type Prog } from "./gl.ts";

// THE FLOOR ROUND THE COURT ON THE GRAPHICS CARD (see scene.ts dressFloor,
// told the same as the page's drawing is - painter.ts): each piece, as it
// comes, onto a list - flat shapes and pictures on planes where they are in
// the world, the people and things stood up facing the camera where they
// are on the picture, at their depth - then all of it drawn in that order,
// a few draws (one each run of pieces from one picture). And the court's
// lines, made once.

const VERT = `#version 300 es
in vec3 aPos;
in vec4 aCol;
in vec2 aUv;
uniform mat4 uM;
// (1: aPos already on the picture - x, y from -1 to 1, and its depth.)
uniform float uOn;
out vec4 vCol;
out vec2 vUv;
void main() {
	gl_Position = uOn > 0.5 ? vec4(aPos, 1.0) : uM * vec4(aPos, 1.0);
	vCol = aCol;
	vUv = aUv;
}`;
const FRAG = `#version 300 es
precision highp float;
in vec4 vCol;
in vec2 vUv;
uniform sampler2D uPic;
// (All of it in a color, as strong as it is: the court's lines.)
uniform vec4 uTint;
out vec4 o;
void main() {
	vec4 c = vCol * uTint * texture(uPic, vUv);
	if (c.a < 0.004) {
		discard;
	}
	o = c;
}`;

// Each point: where (3), its color (4, as strong as it is), where on its
// picture (2).
const STRIDE = 9;
type Run = {
	tex: WebGLTexture;
	// On the picture already (see VERT uOn).
	on: boolean;
	// Added to what is under it.
	add: boolean;
	// Standing in front of the men drawn after it (see Painter.solid), or
	// of what is further off (the baskets: see itemsDraw).
	solid: boolean;
	first: number;
	count: number;
};
export type FloorGl = {
	prog: Prog;
	vao: WebGLVertexArrayObject;
	buf: WebGLBuffer;
	// Room on the card (points).
	room: number;
	data: Float32Array;
	n: number;
	runs: Run[];
	pics: WeakMap<HTMLCanvasElement, WebGLTexture>;
	white: WebGLTexture;
	// A pool of shadow (see floorPools), made the first time.
	pool?: WebGLTexture;
	// The widest and tallest picture the card takes.
	most: number;
	// The court's lines: so many points of paint, then of their middles.
	lines: { vao: WebGLVertexArrayObject; paint: number; all: number };
};

export const floorOnto = (gl: WebGL2RenderingContext): FloorGl | undefined => {
	const prog = compile(gl, VERT, FRAG);
	if (!prog) {
		return undefined;
	}
	gl.bindVertexArray(null);
	const vao = gl.createVertexArray();
	gl.bindVertexArray(vao);
	const room = 4096;
	const buf = gl.createBuffer();
	gl.bindBuffer(gl.ARRAY_BUFFER, buf);
	gl.bufferData(gl.ARRAY_BUFFER, room * STRIDE * 4, gl.DYNAMIC_DRAW);
	attrib(gl, prog, "aPos", buf, 3, STRIDE * 4, 0);
	attrib(gl, prog, "aCol", buf, 4, STRIDE * 4, 12);
	attrib(gl, prog, "aUv", buf, 2, STRIDE * 4, 28);
	gl.bindVertexArray(null);
	const white = gl.createTexture();
	gl.bindTexture(gl.TEXTURE_2D, white);
	gl.texImage2D(
		gl.TEXTURE_2D,
		0,
		gl.RGBA,
		1,
		1,
		0,
		gl.RGBA,
		gl.UNSIGNED_BYTE,
		new Uint8Array([255, 255, 255, 255]),
	);
	return {
		prog,
		vao,
		buf,
		room,
		data: new Float32Array(room * STRIDE),
		n: 0,
		runs: [],
		pics: new WeakMap(),
		white,
		most: gl.getParameter(gl.MAX_TEXTURE_SIZE) as number,
		lines: linesOnto(gl, prog),
	};
};

// THE COURT'S LINES (see courtLines.ts): each strip of paint in pieces, a
// piece from each point to the next - then each line's middle, a hair wide,
// so one far off and nearly edge-on is still there.
const linesOnto = (gl: WebGL2RenderingContext, prog: Prog) => {
	const pts: number[] = [];
	const at = (p: { x: number; y: number }) =>
		pts.push(p.x, p.y, 0, 1, 1, 1, 1, 0, 0);
	for (const { strip, left, right } of courtLineOutlines()) {
		const n = left.length;
		for (let i = 0; i < (strip.closed ? n : n - 1); i++) {
			const j = (i + 1) % n;
			for (const p of [left[i]!, left[j]!, right[j]!]) {
				at(p);
			}
			for (const p of [left[i]!, right[j]!, right[i]!]) {
				at(p);
			}
		}
	}
	const paint = pts.length / STRIDE;
	// (Each middle in separate pieces: the end of one line never joined to
	// the next.)
	for (const { strip } of courtLineOutlines()) {
		const n = strip.pts.length;
		for (let i = 0; i < (strip.closed ? n : n - 1); i++) {
			at(strip.pts[i]!);
			at(strip.pts[(i + 1) % n]!);
		}
	}
	gl.bindVertexArray(null);
	const vao = gl.createVertexArray();
	gl.bindVertexArray(vao);
	const buf = buffer(gl, new Float32Array(pts));
	attrib(gl, prog, "aPos", buf, 3, STRIDE * 4, 0);
	attrib(gl, prog, "aCol", buf, 4, STRIDE * 4, 12);
	attrib(gl, prog, "aUv", buf, 2, STRIDE * 4, 28);
	gl.bindVertexArray(null);
	return { vao, paint, all: pts.length / STRIDE };
};

// A picture on the card - once, the first time it is wanted (fine down to
// small, if it is laid on a plane).
const picOf = (
	gl: WebGL2RenderingContext,
	fl: FloorGl,
	img: HTMLCanvasElement,
	planed: boolean,
): WebGLTexture => {
	let t = fl.pics.get(img);
	if (!t) {
		t = gl.createTexture();
		gl.bindTexture(gl.TEXTURE_2D, t);
		gl.pixelStorei(gl.UNPACK_PREMULTIPLY_ALPHA_WEBGL, true);
		gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA, gl.UNSIGNED_BYTE, img);
		gl.pixelStorei(gl.UNPACK_PREMULTIPLY_ALPHA_WEBGL, false);
		gl.texParameteri(
			gl.TEXTURE_2D,
			gl.TEXTURE_MIN_FILTER,
			planed ? gl.LINEAR_MIPMAP_LINEAR : gl.LINEAR,
		);
		gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
		gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
		gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
		if (planed) {
			gl.generateMipmap(gl.TEXTURE_2D);
		}
		fl.pics.set(img, t);
	}
	return t;
};

// Room on the list for `n` more points.
const room = (fl: FloorGl, n: number) => {
	if ((fl.n + n) * STRIDE <= fl.data.length) {
		return;
	}
	const more = new Float32Array(
		Math.max(fl.data.length * 2, (fl.n + n) * STRIDE),
	);
	more.set(fl.data.subarray(0, fl.n * STRIDE));
	fl.data = more;
};
const runFor = (
	fl: FloorGl,
	tex: WebGLTexture,
	on: boolean,
	add: boolean,
	solid: boolean,
	n: number,
) => {
	room(fl, n);
	const last = fl.runs.at(-1);
	if (
		last &&
		last.tex === tex &&
		last.on === on &&
		last.add === add &&
		last.solid === solid
	) {
		last.count += n;
	} else {
		fl.runs.push({ tex, on, add, solid, first: fl.n, count: n });
	}
};
const point = (
	fl: FloorGl,
	x: number,
	y: number,
	z: number,
	c: readonly [number, number, number, number],
	u: number,
	v: number,
) => {
	const o = fl.n * STRIDE;
	const d = fl.data;
	d[o] = x;
	d[o + 1] = y;
	d[o + 2] = z;
	d[o + 3] = c[0];
	d[o + 4] = c[1];
	d[o + 5] = c[2];
	d[o + 6] = c[3];
	d[o + 7] = u;
	d[o + 8] = v;
	fl.n += 1;
};

// The card's painter for one frame's floor (see painter.ts): onto the list,
// for floorDraw.
export const floorPainter = (
	gl: WebGL2RenderingContext,
	fl: FloorGl,
	cam: Camera,
): Painter => {
	fl.n = 0;
	fl.runs = [];
	const A = (BOWL_FAR + BOWL_NEAR) / (BOWL_FAR - BOWL_NEAR);
	const B = (-2 * BOWL_FAR * BOWL_NEAR) / (BOWL_FAR - BOWL_NEAR);
	// (All of it under the men, as on the page - but for what is said to
	// stand in front of them: see Painter.solid.)
	let solid = false;
	return {
		solid: (on) => {
			solid = on;
		},
		plane: (pl, img, _cols, _rows, alpha = 1, add = false) => {
			const tex = picOf(gl, fl, img, true);
			const c = [alpha, alpha, alpha, alpha] as const;
			const at = (u: number, v: number) => ({
				x: pl.origin.x + pl.alongX.x * u * pl.w + pl.alongY.x * v * pl.h,
				y: pl.origin.y + pl.alongX.y * u * pl.w + pl.alongY.y * v * pl.h,
				z: pl.origin.z + pl.alongX.z * u * pl.w + pl.alongY.z * v * pl.h,
			});
			runFor(fl, tex, false, add, solid && !add, 6);
			for (const [u, v] of [
				[0, 0],
				[1, 0],
				[1, 1],
				[0, 0],
				[1, 1],
				[0, 1],
			] as const) {
				const p = at(u, v);
				point(fl, p.x, p.y, p.z, c, u, v);
			}
		},
		poly: (pts, color) => {
			const [r, g, b, a] = css(color);
			if (a <= 0 || pts.length < 3) {
				return;
			}
			const c = [r * a, g * a, b * a, a] as const;
			const n = (pts.length - 2) * 3;
			runFor(fl, fl.white, false, false, solid, n);
			for (let i = 1; i < pts.length - 1; i++) {
				for (const p of [pts[0]!, pts[i]!, pts[i + 1]!]) {
					point(fl, p.x, p.y, p.z, c, 0, 0);
				}
			}
		},
		sprite: (img, sx, sy, sw, sh, dx, dy, dw, dh, depth, alpha = 1) => {
			const tex = picOf(gl, fl, img, false);
			const W = cam.viewW;
			const H = cam.viewH;
			const x0 = (dx / W) * 2 - 1;
			const x1 = ((dx + dw) / W) * 2 - 1;
			const y0 = 1 - (dy / H) * 2;
			const y1 = 1 - ((dy + dh) / H) * 2;
			// (A hair nearer than its foot, so the floor it is stood on never
			// shows through it.)
			const z =
				depth === undefined ? 0 : A + B / Math.max(BOWL_NEAR, depth - 0.05);
			const u0 = sx / img.width;
			const u1 = (sx + sw) / img.width;
			const v0 = sy / img.height;
			const v1 = (sy + sh) / img.height;
			const c = [alpha, alpha, alpha, alpha] as const;
			runFor(fl, tex, true, false, solid && depth !== undefined, 6);
			point(fl, x0, y0, z, c, u0, v0);
			point(fl, x1, y0, z, c, u1, v0);
			point(fl, x1, y1, z, c, u1, v1);
			point(fl, x0, y0, z, c, u0, v0);
			point(fl, x1, y1, z, c, u1, v1);
			point(fl, x0, y1, z, c, u0, v1);
		},
	};
};

// A SOFT POOL OF SHADOW (see arena.ts drawShadow) round each of these on the
// floor - (x, y) its middle, r its radius (feet), so dark - each over what is
// under it, after all else on the list.
const POOL_PX = 128;
export const floorPools = (
	gl: WebGL2RenderingContext,
	fl: FloorGl,
	pools: readonly { x: number; y: number; r: number; dark: number }[],
) => {
	if (!fl.pool) {
		const t = gl.createTexture();
		const px = new Uint8Array(POOL_PX * POOL_PX * 4);
		for (let j = 0; j < POOL_PX; j++) {
			for (let i = 0; i < POOL_PX; i++) {
				const d = Math.hypot(
					((i + 0.5) / POOL_PX) * 2 - 1,
					((j + 0.5) / POOL_PX) * 2 - 1,
				);
				const a =
					d < 0.6
						? 0.5 + (0.3 - 0.5) * (d / 0.6)
						: 0.3 * (1 - Math.min(1, (d - 0.6) / 0.4));
				// (Black, its strength as it is laid over: premultiplied.)
				px[(j * POOL_PX + i) * 4 + 3] = Math.round(a * 255);
			}
		}
		gl.activeTexture(gl.TEXTURE0);
		gl.bindTexture(gl.TEXTURE_2D, t);
		gl.texImage2D(
			gl.TEXTURE_2D,
			0,
			gl.RGBA,
			POOL_PX,
			POOL_PX,
			0,
			gl.RGBA,
			gl.UNSIGNED_BYTE,
			px,
		);
		gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR);
		gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
		gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
		gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
		fl.pool = t;
	}
	for (const { x, y, r, dark } of pools) {
		if (dark <= 0.01) {
			continue;
		}
		const c = [dark, dark, dark, dark] as const;
		// (A hair over the floor.)
		const z = 0.005;
		runFor(fl, fl.pool, false, false, false, 6);
		point(fl, x - r, y - r, z, c, 0, 0);
		point(fl, x + r, y - r, z, c, 1, 0);
		point(fl, x + r, y + r, z, c, 1, 1);
		point(fl, x - r, y - r, z, c, 0, 0);
		point(fl, x + r, y + r, z, c, 1, 1);
		point(fl, x - r, y + r, z, c, 0, 1);
	}
};

// What is on the list, in order, each over what is under it - writing how
// far off it is where it stands in front of the men drawn after it (see
// Painter.solid); then the list emptied.
export const floorDraw = (
	gl: WebGL2RenderingContext,
	fl: FloorGl,
	m: Float32Array,
	// (Behind what is nearer, already drawn - not simply over it.)
	tested = false,
) => {
	if (fl.n === 0) {
		return;
	}
	const { prog } = fl;
	gl.useProgram(prog.p);
	gl.uniformMatrix4fv(prog.u("uM"), false, m);
	gl.uniform1i(prog.u("uPic"), 0);
	gl.uniform4f(prog.u("uTint"), 1, 1, 1, 1);
	gl.bindVertexArray(fl.vao);
	gl.bindBuffer(gl.ARRAY_BUFFER, fl.buf);
	if (fl.n > fl.room) {
		fl.room = Math.max(fl.room * 2, fl.n);
		gl.bufferData(gl.ARRAY_BUFFER, fl.room * STRIDE * 4, gl.DYNAMIC_DRAW);
	}
	gl.bufferSubData(gl.ARRAY_BUFFER, 0, fl.data, 0, fl.n * STRIDE);
	gl.enable(gl.BLEND);
	gl.depthFunc(tested ? gl.LEQUAL : gl.ALWAYS);
	gl.activeTexture(gl.TEXTURE0);
	// (Only what changes from one to the next told to the card.)
	let was: Run | undefined;
	for (const r of fl.runs) {
		if (r.tex !== was?.tex) {
			gl.bindTexture(gl.TEXTURE_2D, r.tex);
		}
		if (r.on !== was?.on) {
			gl.uniform1f(prog.u("uOn"), r.on ? 1 : 0);
		}
		if (r.add !== was?.add) {
			gl.blendFunc(gl.ONE, r.add ? gl.ONE : gl.ONE_MINUS_SRC_ALPHA);
		}
		if (r.solid !== was?.solid) {
			gl.depthMask(r.solid);
		}
		gl.drawArrays(gl.TRIANGLES, r.first, r.count);
		was = r;
	}
	gl.depthMask(true);
	gl.depthFunc(gl.LEQUAL);
	gl.disable(gl.BLEND);
	gl.bindVertexArray(null);
	fl.n = 0;
	fl.runs = [];
};

// The court's lines (see courtLines.ts drawCourtLines), in their color.
export const linesDraw = (
	gl: WebGL2RenderingContext,
	fl: FloorGl,
	m: Float32Array,
	color: string,
) => {
	const { prog, lines } = fl;
	const [r, g, b] = css(color);
	// (As the page has them: not quite solid.)
	const a = 0.92;
	gl.useProgram(prog.p);
	gl.uniformMatrix4fv(prog.u("uM"), false, m);
	gl.uniform1i(prog.u("uPic"), 0);
	gl.uniform1f(prog.u("uOn"), 0);
	gl.activeTexture(gl.TEXTURE0);
	gl.bindTexture(gl.TEXTURE_2D, fl.white);
	gl.bindVertexArray(lines.vao);
	gl.uniform4f(prog.u("uTint"), r * a, g * a, b * a, a);
	gl.enable(gl.BLEND);
	gl.blendFunc(gl.ONE, gl.ONE_MINUS_SRC_ALPHA);
	gl.depthFunc(gl.ALWAYS);
	gl.depthMask(false);
	gl.drawArrays(gl.TRIANGLES, 0, lines.paint);
	gl.drawArrays(gl.LINES, lines.paint, lines.all - lines.paint);
	gl.depthMask(true);
	gl.depthFunc(gl.LEQUAL);
	gl.disable(gl.BLEND);
	gl.bindVertexArray(null);
};

// THE PAGE'S SHEET: all the page draws for the picture each frame - the
// baskets and the ball, what goes over everything, the heads drawn again
// (see gpu/scene.ts heads) - on one sheet, sent to the card once. (Each
// sheet sent on its own, on a phone, is a wait of its own.) Each laid out on
// it first (sheetPut), then all painted on it (sheetPaint), then sent
// (sheetSend).
export type PageSheet = {
	ctx: CanvasRenderingContext2D | null;
	tex: WebGLTexture;
	// (For copying out of it on the card: see sheetCopy.)
	fbo: WebGLFramebuffer;
	// The size of it on the card.
	w: number;
	h: number;
	// The widest and tallest picture the card takes.
	most: number;
	// This frame's: each put on it, where on the sheet; the row being filled,
	// how far along it and how tall; how far across and down any is.
	puts: Put[];
	left: number;
	top: number;
	row: number;
	wide: number;
	tall: number;
};
// A piece of the picture `w` x `h` from (`x0`, `y0`), on the sheet at
// (`sx`, `sy`): `draw` draws it, as on the picture.
export type Put = {
	x0: number;
	y0: number;
	w: number;
	h: number;
	sx: number;
	sy: number;
	draw: (ctx: CanvasRenderingContext2D) => void;
};
const GAP = 2;
const SHEET_W = 2048;
// (Kept off the graphics card by the browser - drawn by the page, read back
// each frame to go on the card, it would only have to be fetched back.)
export const sheetOnto = (gl: WebGL2RenderingContext): PageSheet => ({
	ctx:
		typeof document === "undefined"
			? null
			: document
					.createElement("canvas")
					.getContext("2d", { willReadFrequently: true }),
	tex: gl.createTexture(),
	fbo: gl.createFramebuffer(),
	w: 0,
	h: 0,
	most: gl.getParameter(gl.MAX_TEXTURE_SIZE) as number,
	puts: [],
	left: GAP,
	top: GAP,
	row: 0,
	wide: 0,
	tall: 0,
});
export const sheetBegin = (s: PageSheet) => {
	s.puts = [];
	s.left = GAP;
	s.top = GAP;
	s.row = 0;
	s.wide = 0;
	s.tall = 0;
};
// Where it goes on the sheet - in a row, another row under it if the row is
// full - if there is room on the card for it.
export const sheetPut = (
	s: PageSheet,
	x0: number,
	y0: number,
	w: number,
	h: number,
	draw: Put["draw"],
): Put | undefined => {
	if (!s.ctx || w <= 0 || h <= 0) {
		return undefined;
	}
	if (s.left + w + GAP > SHEET_W && s.left > GAP) {
		s.left = GAP;
		s.top += s.row + GAP;
		s.row = 0;
	}
	if (s.left + w + GAP > s.most || s.top + h + GAP > s.most) {
		return undefined;
	}
	const put = { x0, y0, w, h, sx: s.left, sy: s.top, draw };
	s.puts.push(put);
	s.left += w + GAP;
	s.row = Math.max(s.row, h);
	s.wide = Math.max(s.wide, s.left);
	s.tall = Math.max(s.tall, s.top + s.row + GAP);
	return put;
};
export const sheetPaint = (s: PageSheet) => {
	const ctx = s.ctx;
	if (!ctx || s.puts.length === 0) {
		return;
	}
	const cv = ctx.canvas;
	if (cv.width < s.wide || cv.height < s.tall) {
		cv.width = Math.max(cv.width, Math.ceil(s.wide / 64) * 64);
		cv.height = Math.max(cv.height, Math.ceil(s.tall / 64) * 64);
	}
	ctx.setTransform(1, 0, 0, 1, 0, 0);
	ctx.clearRect(0, 0, s.wide, s.tall);
	for (const p of s.puts) {
		ctx.save();
		ctx.setTransform(1, 0, 0, 1, p.sx - p.x0, p.sy - p.y0);
		ctx.beginPath();
		ctx.rect(p.x0, p.y0, p.w, p.h);
		ctx.clip();
		p.draw(ctx);
		ctx.restore();
	}
};
export const sheetSend = (gl: WebGL2RenderingContext, s: PageSheet) => {
	const ctx = s.ctx;
	if (!ctx || s.puts.length === 0) {
		return;
	}
	const cv = ctx.canvas;
	gl.activeTexture(gl.TEXTURE0);
	// (On the card as big as the sheet, made so once; only the part of it
	// drawn on sent over each frame.)
	gl.bindTexture(gl.TEXTURE_2D, s.tex);
	if (s.w !== cv.width || s.h !== cv.height) {
		gl.texImage2D(
			gl.TEXTURE_2D,
			0,
			gl.RGBA8,
			cv.width,
			cv.height,
			0,
			gl.RGBA,
			gl.UNSIGNED_BYTE,
			null,
		);
		gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.NEAREST);
		gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.NEAREST);
		gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
		gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
		s.w = cv.width;
		s.h = cv.height;
		gl.bindFramebuffer(gl.READ_FRAMEBUFFER, s.fbo);
		gl.framebufferTexture2D(
			gl.READ_FRAMEBUFFER,
			gl.COLOR_ATTACHMENT0,
			gl.TEXTURE_2D,
			s.tex,
			0,
		);
		gl.bindFramebuffer(gl.READ_FRAMEBUFFER, null);
	}
	gl.pixelStorei(gl.UNPACK_PREMULTIPLY_ALPHA_WEBGL, true);
	gl.texSubImage2D(
		gl.TEXTURE_2D,
		0,
		0,
		0,
		Math.min(s.wide, cv.width),
		Math.min(s.tall, cv.height),
		gl.RGBA,
		gl.UNSIGNED_BYTE,
		cv,
	);
	gl.pixelStorei(gl.UNPACK_PREMULTIPLY_ALPHA_WEBGL, false);
};
// Pieces of the sheet, once it is on the card, copied each into a picture
// of its own (made `w` x `h` if it is not), there for frames after.
export const sheetCopy = (
	gl: WebGL2RenderingContext,
	s: PageSheet,
	into: readonly {
		put: Put;
		tex: WebGLTexture;
		made: boolean;
		filter: number;
	}[],
) => {
	if (into.length === 0 || s.w === 0) {
		return;
	}
	gl.activeTexture(gl.TEXTURE0);
	gl.bindFramebuffer(gl.READ_FRAMEBUFFER, s.fbo);
	for (const { put, tex, made, filter } of into) {
		gl.bindTexture(gl.TEXTURE_2D, tex);
		if (!made) {
			gl.texImage2D(
				gl.TEXTURE_2D,
				0,
				gl.RGBA8,
				put.w,
				put.h,
				0,
				gl.RGBA,
				gl.UNSIGNED_BYTE,
				null,
			);
			gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, filter);
			gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, filter);
			gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
			gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
		}
		gl.copyTexSubImage2D(gl.TEXTURE_2D, 0, 0, 0, put.sx, put.sy, put.w, put.h);
	}
	gl.bindFramebuffer(gl.READ_FRAMEBUFFER, null);
};

// THE BASKETS AND THE BALL WHERE NOBODY HAS IT (see scene.ts hoopsAndBalls),
// AND WHAT GOES OVER EVERYTHING: each drawn as the page draws it, onto the
// page's sheet; each stood where it is on the picture, as far off as it is
// put among the men - what is nearer in front of it, what is further
// behind.
// Where a box comes out on the picture: the part of it in front of the
// camera - each corner there, and where each line from one to a corner
// behind comes through (as near as the picture is drawn true: see
// camera.ts project) - nothing, if none of it is.
const NEAREST = 0.5;
const boxOn = (
	cam: Camera,
	box: readonly Pt3[],
): [number, number, number, number] => {
	let x0 = Infinity;
	let y0 = Infinity;
	let x1 = -Infinity;
	let y1 = -Infinity;
	const take = (p: Pt3) => {
		const q = project(cam, p);
		x0 = Math.min(x0, q.x);
		y0 = Math.min(y0, q.y);
		x1 = Math.max(x1, q.x);
		y1 = Math.max(y1, q.y);
	};
	const d = box.map((p) => depthOf(cam, p));
	for (const [i, a] of box.entries()) {
		const da = d[i]!;
		if (da >= NEAREST) {
			take(a);
			continue;
		}
		for (const [j, b] of box.entries()) {
			const db = d[j]!;
			if (db >= NEAREST) {
				const u = (NEAREST - da) / (db - da);
				take({
					x: a.x + (b.x - a.x) * u,
					y: a.y + (b.y - a.y) * u,
					z: a.z + (b.z - a.z) * u,
				});
			}
		}
	}
	return [x0, y0, x1, y1];
};
// Each on the sheet (all of the picture, if neither its box nor its part of
// the picture is said).
export type ItemPut = { it: FrameItem; put: Put };
export const itemsPut = (
	s: PageSheet,
	cam: Camera,
	items: readonly FrameItem[],
): ItemPut[] => {
	const W = cam.viewW;
	const H = cam.viewH;
	const out: ItemPut[] = [];
	for (const it of items) {
		let [x0, y0, x1, y1] = it.rect ?? [0, 0, W, H];
		if (it.box) {
			[x0, y0, x1, y1] = boxOn(cam, it.box);
		}
		x0 = Math.max(0, Math.floor(x0) - 4);
		y0 = Math.max(0, Math.floor(y0) - 4);
		x1 = Math.min(W, Math.ceil(x1) + 4);
		y1 = Math.min(H, Math.ceil(y1) + 4);
		const put = sheetPut(s, x0, y0, x1 - x0, y1 - y0, it.draw);
		if (put) {
			out.push({ it, put });
		}
	}
	return out;
};
// And them on the picture, from the sheet once it is on the card.
export const itemsDraw = (
	gl: WebGL2RenderingContext,
	fl: FloorGl,
	m: Float32Array,
	cam: Camera,
	s: PageSheet,
	puts: readonly ItemPut[],
) => {
	if (puts.length === 0 || s.w === 0) {
		return;
	}
	const W = cam.viewW;
	const H = cam.viewH;
	const A = (BOWL_FAR + BOWL_NEAR) / (BOWL_FAR - BOWL_NEAR);
	const B = (-2 * BOWL_FAR * BOWL_NEAR) / (BOWL_FAR - BOWL_NEAR);
	fl.n = 0;
	fl.runs = [];
	const one = [1, 1, 1, 1] as const;
	for (const { it, put: c } of puts) {
		const X0 = (c.x0 / W) * 2 - 1;
		const X1 = ((c.x0 + c.w) / W) * 2 - 1;
		const Y0 = 1 - (c.y0 / H) * 2;
		const Y1 = 1 - ((c.y0 + c.h) / H) * 2;
		const z = A + B / Math.max(BOWL_NEAR, it.depth);
		const u0 = c.sx / s.w;
		const u1 = (c.sx + c.w) / s.w;
		const v0 = c.sy / s.h;
		const v1 = (c.sy + c.h) / s.h;
		runFor(fl, s.tex, true, false, true, 6);
		point(fl, X0, Y0, z, one, u0, v0);
		point(fl, X1, Y0, z, one, u1, v0);
		point(fl, X1, Y1, z, one, u1, v1);
		point(fl, X0, Y0, z, one, u0, v0);
		point(fl, X1, Y1, z, one, u1, v1);
		point(fl, X0, Y1, z, one, u0, v1);
	}
	floorDraw(gl, fl, m, true);
};
