// THE LITTLE THE GPU DRAWING NEEDS OF WEBGL2 ITSELF (see scene.ts): a
// program from its two shaders (and what of its vertex shader's it writes
// into a buffer, if anything - see men.ts SKIN_VERT), a buffer, an
// attribute read from one.

export type Prog = {
	p: WebGLProgram;
	u: (name: string) => WebGLUniformLocation | null;
};

export const compile = (
	gl: WebGL2RenderingContext,
	vert: string,
	frag: string,
	kept?: string[],
): Prog | undefined => {
	const p = gl.createProgram();
	for (const [type, src] of [
		[gl.VERTEX_SHADER, vert],
		[gl.FRAGMENT_SHADER, frag],
	] as const) {
		const sh = gl.createShader(type);
		if (!sh) {
			return undefined;
		}
		gl.shaderSource(sh, src);
		gl.compileShader(sh);
		if (!gl.getShaderParameter(sh, gl.COMPILE_STATUS)) {
			console.warn("court gl:", gl.getShaderInfoLog(sh));
			return undefined;
		}
		gl.attachShader(p, sh);
	}
	if (kept) {
		gl.transformFeedbackVaryings(p, kept, gl.INTERLEAVED_ATTRIBS);
	}
	gl.linkProgram(p);
	if (!gl.getProgramParameter(p, gl.LINK_STATUS)) {
		console.warn("court gl:", gl.getProgramInfoLog(p));
		return undefined;
	}
	const at = new Map<string, WebGLUniformLocation | null>();
	return {
		p,
		u: (name) => {
			if (!at.has(name)) {
				at.set(name, gl.getUniformLocation(p, name));
			}
			return at.get(name)!;
		},
	};
};

export const buffer = (
	gl: WebGL2RenderingContext,
	data: ArrayBufferView,
	target: number = gl.ARRAY_BUFFER,
) => {
	const b = gl.createBuffer();
	gl.bindBuffer(target, b);
	gl.bufferData(target, data, gl.STATIC_DRAW);
	return b;
};

export const attrib = (
	gl: WebGL2RenderingContext,
	prog: Prog,
	name: string,
	buf: WebGLBuffer,
	size: number,
	stride: number,
	offset: number,
) => {
	const loc = gl.getAttribLocation(prog.p, name);
	if (loc < 0) {
		return;
	}
	gl.bindBuffer(gl.ARRAY_BUFFER, buf);
	gl.enableVertexAttribArray(loc);
	gl.vertexAttribPointer(loc, size, gl.FLOAT, false, stride, offset);
};

// A color as the shaders take it (0 - 1 each): "#rrggbb" or "#rgb".
export const rgb = (
	hex: string | undefined,
	or: string,
): [number, number, number] => {
	let h =
		hex && /^#(?:[\da-f]{3}){1,2}$/i.test(hex) ? hex.slice(1) : or.slice(1);
	if (h.length === 3) {
		h = [...h].map((c) => c + c).join("");
	}
	return [0, 2, 4].map((i) => Number.parseInt(h.slice(i, i + 2), 16) / 255) as [
		number,
		number,
		number,
	];
};

// Any color the page takes as the shaders do (0 - 1 each, and how solid):
// "#rgb", "#rrggbb", "rgb(...)", "rgba(...)" - black, if not one of those.
const colors = new Map<string, [number, number, number, number]>();
export const css = (c: string): readonly [number, number, number, number] => {
	let out = colors.get(c);
	if (!out) {
		const s = c.trim();
		const fn = /^rgba?\(([^)]*)\)$/i.exec(s);
		if (fn) {
			const v = fn[1]!
				.split(/[\s,/]+/)
				.filter(Boolean)
				.map(Number);
			out = [
				(v[0] ?? 0) / 255,
				(v[1] ?? 0) / 255,
				(v[2] ?? 0) / 255,
				v[3] ?? 1,
			];
		} else if (/^#(?:[\da-f]{3}){1,2}$/i.test(s)) {
			out = [...rgb(s, "#000"), 1];
		} else {
			out = [0, 0, 0, 1];
		}
		colors.set(c, out);
	}
	return out;
};
