import type { Camera } from "./camera.ts";
import type { TexturedPlane } from "./planes.ts";

// THE FLOOR AND THE BUILDING, ON THE GRAPHICS CARD.
//
// The stands, the boards, the floor round the court and the court itself are
// flat pictures on planes in the world (see planes.ts). Drawn on a canvas,
// each goes a row of the picture at a time - a draw for every row, thousands
// a frame at a screen's full resolution. Here each is one draw, in
// perspective, on a layer of its own under the rest of the picture (the
// players, the baskets, the lines - see drawFrame), so the building costs
// next to nothing however sharp the picture. Where the graphics card can't be
// had, the planes are drawn on the canvas as before.

export type GlLayer = {
	// Start a frame w x h, cleared to this color.
	frame: (w: number, h: number, clear: string) => void;
	plane: (
		cam: Camera,
		plane: TexturedPlane,
		img: HTMLCanvasElement,
		alpha?: number,
	) => void;
	// A patch of the floor, x0..x1 along the court and y0..y1 across it.
	floor: (
		cam: Camera,
		x0: number,
		y0: number,
		x1: number,
		y1: number,
		color: string,
	) => void;
	// Put a picture on the card ahead of its first frame - a moment's work
	// that would otherwise hitch the frame it first shows in.
	warm: (img: HTMLCanvasElement) => void;
	// False once the graphics card has been lost (the device under strain,
	// say): draw on the canvas until it is back.
	ok: () => boolean;
};

type GL = WebGLRenderingContext | WebGL2RenderingContext;

const VERTEX = `
attribute vec4 a_pos;
attribute vec2 a_uv;
varying vec2 v_uv;
void main() {
	gl_Position = a_pos;
	v_uv = a_uv;
}`;

// Pictures come in with their alpha already multiplied through (see
// UNPACK_PREMULTIPLY_ALPHA_WEBGL), and so does the plain color.
const FRAGMENT = `
precision mediump float;
varying vec2 v_uv;
uniform sampler2D u_tex;
uniform vec4 u_color;
uniform float u_useTex;
void main() {
	gl_FragColor = u_useTex > 0.5 ? texture2D(u_tex, v_uv) * u_color.a : u_color;
}`;

// A color as the GPU takes it, 0..1 - from anything a canvas takes.
const colors = new Map<string, [number, number, number]>();
let colorCtx: CanvasRenderingContext2D | null | undefined;
const parseColor = (c: string): [number, number, number] => {
	const known = colors.get(c);
	if (known) {
		return known;
	}
	// The canvas puts every color it is given one of two ways: #rrggbb, or
	// rgba(r, g, b, a).
	colorCtx ??= document.createElement("canvas").getContext("2d");
	let norm = c;
	if (colorCtx) {
		colorCtx.fillStyle = "#000000";
		colorCtx.fillStyle = c;
		norm = String(colorCtx.fillStyle);
	}
	let rgb: [number, number, number] = [0, 0, 0];
	const hex = /^#([\da-f]{6})$/i.exec(norm);
	const fn = /^rgba?\(([^)]*)\)$/i.exec(norm);
	if (hex) {
		const n = Number.parseInt(hex[1]!, 16);
		rgb = [((n >> 16) & 255) / 255, ((n >> 8) & 255) / 255, (n & 255) / 255];
	} else if (fn) {
		const [r = 0, g = 0, b = 0] = fn[1]!.split(",").map(Number);
		rgb = [r / 255, g / 255, b / 255];
	}
	colors.set(c, rgb);
	return rgb;
};

const compile = (gl: GL, type: number, src: string) => {
	const s = gl.createShader(type);
	if (!s) {
		return undefined;
	}
	gl.shaderSource(s, src);
	gl.compileShader(s);
	return gl.getShaderParameter(s, gl.COMPILE_STATUS) ? s : undefined;
};

export const makeGlLayer = (canvas: HTMLCanvasElement): GlLayer | undefined => {
	// Only on a real graphics card: drawn in software instead, this is slower
	// than the canvas (a test page can say otherwise).
	const anyCard = !!(globalThis as { __courtGlSoftware?: unknown })
		.__courtGlSoftware;
	const options: WebGLContextAttributes = {
		alpha: false,
		antialias: false,
		depth: false,
		stencil: false,
		premultipliedAlpha: true,
		preserveDrawingBuffer: false,
		failIfMajorPerformanceCaveat: !anyCard,
	};
	let gl: GL | null = null;
	let gl2 = false;
	try {
		gl = canvas.getContext("webgl2", options);
		gl2 = !!gl;
		gl ??= canvas.getContext("webgl", options);
	} catch {
		gl = null;
	}
	if (!gl) {
		return undefined;
	}
	const g = gl;
	// (Software that doesn't own up to it, above.)
	const info = g.getExtension("WEBGL_debug_renderer_info");
	const renderer = String(
		info ? g.getParameter(info.UNMASKED_RENDERER_WEBGL) : "",
	);
	if (!anyCard && /swiftshader|llvmpipe|software/i.test(renderer)) {
		g.getExtension("WEBGL_lose_context")?.loseContext();
		return undefined;
	}
	const maxSize = Number(g.getParameter(g.MAX_TEXTURE_SIZE)) || 2048;
	let lost = false;
	// A picture the card cannot take (too big, or not ours to read): back to
	// the canvas for good.
	let broken = false;
	let textures = new WeakMap<HTMLCanvasElement, WebGLTexture>();
	type Prog = {
		program: WebGLProgram;
		pos: number;
		uv: number;
		color: WebGLUniformLocation | null;
		useTex: WebGLUniformLocation | null;
		buffer: WebGLBuffer | null;
	};
	let prog: Prog | undefined;
	const setUp = (): Prog | undefined => {
		const vs = compile(g, g.VERTEX_SHADER, VERTEX);
		const fs = compile(g, g.FRAGMENT_SHADER, FRAGMENT);
		const program = g.createProgram();
		if (!vs || !fs || !program) {
			return undefined;
		}
		g.attachShader(program, vs);
		g.attachShader(program, fs);
		g.linkProgram(program);
		if (!g.getProgramParameter(program, g.LINK_STATUS)) {
			return undefined;
		}
		g.useProgram(program);
		const p: Prog = {
			program,
			pos: g.getAttribLocation(program, "a_pos"),
			uv: g.getAttribLocation(program, "a_uv"),
			color: g.getUniformLocation(program, "u_color"),
			useTex: g.getUniformLocation(program, "u_useTex"),
			buffer: g.createBuffer(),
		};
		g.bindBuffer(g.ARRAY_BUFFER, p.buffer);
		g.enableVertexAttribArray(p.pos);
		g.vertexAttribPointer(p.pos, 4, g.FLOAT, false, 24, 0);
		g.enableVertexAttribArray(p.uv);
		g.vertexAttribPointer(p.uv, 2, g.FLOAT, false, 24, 16);
		g.uniform1i(g.getUniformLocation(program, "u_tex"), 0);
		g.enable(g.BLEND);
		g.blendFunc(g.ONE, g.ONE_MINUS_SRC_ALPHA);
		g.pixelStorei(g.UNPACK_PREMULTIPLY_ALPHA_WEBGL, true);
		return p;
	};
	prog = setUp();
	if (!prog) {
		return undefined;
	}
	canvas.addEventListener("webglcontextlost", (e) => {
		e.preventDefault();
		lost = true;
	});
	canvas.addEventListener("webglcontextrestored", () => {
		textures = new WeakMap();
		prog = setUp();
		lost = !prog;
	});

	const textureOf = (img: HTMLCanvasElement): WebGLTexture | undefined => {
		let t = textures.get(img);
		if (t) {
			return t;
		}
		if (img.width > maxSize || img.height > maxSize) {
			broken = true;
			return undefined;
		}
		t = g.createTexture() ?? undefined;
		if (!t) {
			return undefined;
		}
		g.bindTexture(g.TEXTURE_2D, t);
		try {
			g.texImage2D(g.TEXTURE_2D, 0, g.RGBA, g.RGBA, g.UNSIGNED_BYTE, img);
		} catch {
			// A picture with something in it from another site that does not
			// say it may be read - a team's logo, say, on its court: the card
			// is not allowed it. Back to the canvas, which is.
			g.deleteTexture(t);
			broken = true;
			return undefined;
		}
		g.texParameteri(g.TEXTURE_2D, g.TEXTURE_WRAP_S, g.CLAMP_TO_EDGE);
		g.texParameteri(g.TEXTURE_2D, g.TEXTURE_WRAP_T, g.CLAMP_TO_EDGE);
		g.texParameteri(g.TEXTURE_2D, g.TEXTURE_MAG_FILTER, g.LINEAR);
		if (gl2) {
			// Seen from far off, smaller, without shimmering.
			g.generateMipmap(g.TEXTURE_2D);
			g.texParameteri(
				g.TEXTURE_2D,
				g.TEXTURE_MIN_FILTER,
				g.LINEAR_MIPMAP_LINEAR,
			);
		} else {
			g.texParameteri(g.TEXTURE_2D, g.TEXTURE_MIN_FILTER, g.LINEAR);
		}
		textures.set(img, t);
		return t;
	};

	let W = 1;
	let H = 1;
	const verts = new Float32Array(24);
	// A point of the world where the GPU wants it: the picture's position in
	// clip space, before the divide by depth - the same perspective as
	// project(), exact across each plane (and clipped by the GPU, even past
	// the camera).
	const put = (cam: Camera, i: number, x: number, y: number, z: number) => {
		const dx = x - cam.pos.x;
		const dy = y - cam.pos.y;
		let depth: number;
		let across: number;
		let up: number;
		if (cam.upright) {
			const dz = -cam.pos.z;
			depth = dx * cam.fwd.x + dy * cam.fwd.y + dz * cam.fwd.z;
			across = dx * cam.right.x + dy * cam.right.y + dz * cam.right.z;
			up = dx * cam.up.x + dy * cam.up.y + dz * cam.up.z + z;
		} else {
			const dz = z - cam.pos.z;
			depth = dx * cam.fwd.x + dy * cam.fwd.y + dz * cam.fwd.z;
			across = dx * cam.right.x + dy * cam.right.y + dz * cam.right.z;
			up = dx * cam.up.x + dy * cam.up.y + dz * cam.up.z;
		}
		// Screen x = cx + f * across / depth, y = cy - f * up / depth; to clip
		// space (x right, y up, -1..1) times depth.
		const sx = cam.cx * depth + cam.f * across;
		const sy = cam.cy * depth - cam.f * up;
		const o = i * 6;
		verts[o] = (2 * sx) / W - depth;
		verts[o + 1] = depth - (2 * sy) / H;
		verts[o + 2] = 0;
		verts[o + 3] = depth;
	};
	const draw = () => {
		g.bufferData(g.ARRAY_BUFFER, verts, g.DYNAMIC_DRAW);
		g.drawArrays(g.TRIANGLE_STRIP, 0, 4);
	};

	return {
		ok: () => !lost && !broken,
		warm: (img) => {
			if (!lost) {
				textureOf(img);
			}
		},
		frame: (w, h, clear) => {
			if (lost) {
				return;
			}
			if (canvas.width !== w || canvas.height !== h) {
				canvas.width = w;
				canvas.height = h;
			}
			W = w;
			H = h;
			g.viewport(0, 0, w, h);
			const [r, gg, b] = parseColor(clear);
			g.clearColor(r, gg, b, 1);
			g.clear(g.COLOR_BUFFER_BIT);
		},
		plane: (cam, plane, img, alpha = 1) => {
			if (lost || !prog || alpha <= 0.004) {
				return;
			}
			const t = textureOf(img);
			if (!t) {
				return;
			}
			const { origin: o, alongX: ax, alongY: ay, w, h } = plane;
			// The corners, in strip order, each with where in the picture it is.
			const corners: [number, number][] = [
				[0, 0],
				[w, 0],
				[0, h],
				[w, h],
			];
			corners.forEach(([u, v], i) => {
				put(
					cam,
					i,
					o.x + ax.x * u + ay.x * v,
					o.y + ax.y * u + ay.y * v,
					o.z + ax.z * u + ay.z * v,
				);
				verts[i * 6 + 4] = u / w;
				verts[i * 6 + 5] = v / h;
			});
			g.bindTexture(g.TEXTURE_2D, t);
			g.uniform1f(prog.useTex, 1);
			g.uniform4f(prog.color, 1, 1, 1, alpha);
			draw();
		},
		floor: (cam, x0, y0, x1, y1, color) => {
			if (lost || !prog) {
				return;
			}
			const pts: [number, number][] = [
				[x0, y0],
				[x1, y0],
				[x0, y1],
				[x1, y1],
			];
			pts.forEach(([x, y], i) => {
				put(cam, i, x, y, 0);
				verts[i * 6 + 4] = 0;
				verts[i * 6 + 5] = 0;
			});
			const [r, gg, b] = parseColor(color);
			g.uniform1f(prog.useTex, 0);
			g.uniform4f(prog.color, r, gg, b, 1);
			draw();
		},
	};
};
