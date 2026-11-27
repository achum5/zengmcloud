import type { Camera } from "./camera.ts";
import {
	type Bowl,
	BOWL_MATERIALS,
	type BowlMaterial,
	bowlMatrix,
	TONE_URL,
} from "./bowlData.ts";
import {
	FAN_CELL,
	FAN_POSES,
	type SeatedCrowd,
	shareAt,
	takenAt,
} from "./bowlFans.ts";
import { COURT_W } from "./geometry.ts";
import { glForced } from "./sculptGl.ts";

// THE BOWL ON THE GRAPHICS CARD (see bowlData.ts): drawn each frame into a
// picture of its own the size of the game's, and that laid down first, under
// everything the page draws - the floor, the court, the people on it.
//
// - Its shell: each point's own color for what it is made of, times the
//   light baked into it in Blender - or, for what gives off light (the suite
//   lights, the tunnels' ends, the step lights), that light. The LED wall
//   and the ribbon boards show the game's own pictures of them.
// - Its seats, each in the light baked at it, and a fan in each that is
//   taken (see bowlFans.ts): a picture of him stood up in it, turned to the
//   camera - sat, up, waving or clapping as the crowd is.
// - The glass of the suites and the rails last, seen through.
// - Every color as Blender's camera makes it (AgX, its contrast - see
//   bowl.py export_tone): a table of them, looked up.
//
// Where there is no WebGL2, or it is lost, or the table isn't in yet: not
// drawn (false) - and the page draws today's stands instead (see scene.ts).

// The colors of what it is made of (as bowl.py's MATS: sRGB), and what
// gives off light (its light, linear).
const ALBEDO: Partial<Record<BowlMaterial, string>> = {
	concrete: "#3a3b40",
	tread: "#2b2c31",
	riser: "#1c1d21",
	black: "#0d0e11",
	fascia: "#15161a",
	seat: "#24262c",
	suiteWall: "#b89a74",
	suiteFloor: "#4a3a2e",
	suiteCeil: "#d8d0c4",
	mullion: "#1a1b1f",
	floor: "#241e18",
	apron: "#1d2a44",
	tunnelWall: "#24221f",
	roof: "#08080a",
	steel: "#2c2e33",
	banner0: "#1f2a44",
	banner1: "#1aa39a",
	tunnelFloor: "#3a3b40",
};
const EMIT: Partial<Record<BowlMaterial, [number, number, number, number]>> = {
	suiteLight: [1, 0.72, 0.45, 9],
	stepLight: [1, 0.78, 0.5, 0.45],
	tunnelLight: [1, 0.8, 0.6, 0.6],
	concourse: [1, 0.8, 0.58, 0.06],
	fixture: [1, 0.97, 0.92, 60],
	portal: [1, 0.82, 0.62, 0.8],
};
// The glass: mostly see-through, a little of the room's light on it.
const GLASS: Partial<Record<BowlMaterial, number>> = { glass: 0.12, rail: 0.2 };
const GLASS_COLOR = [0.6, 0.7, 0.75];
// The boards: their pictures given off this bright.
const BOARD_GLOW = 2.2;
const EXPOSURE = 2 ** -0.3;
// The table of colors: 33 a side, each color in stops from 2^LO to 2^HI.
const TONE_N = 33;
const TONE_LO = -12.47393;
const TONE_HI = 4.026069;
// The fans: a little less light on them than the probe at their seat
// takes (their clothes are no lighter than the shell's concrete).
const FAN_LIGHT = 0.8;
// The rafters' picture hung over the far side (see paintRafters): along
// the court, this high (feet) - its lights lit.
const RAFTER_LIGHT = 0.5;

const linear = (hex: string): [number, number, number] => {
	const h = hex.replace("#", "");
	const c = [0, 2, 4].map((i) => Number.parseInt(h.slice(i, i + 2), 16) / 255);
	return c.map((v) =>
		v <= 0.04045 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4,
	) as [number, number, number];
};

const M = BOWL_MATERIALS.length;
const WALL = BOWL_MATERIALS.indexOf("wall");
const RIBBON = BOWL_MATERIALS.indexOf("ribbon");

const TONE = `
uniform highp sampler3D uTone;
vec3 tone(vec3 c) {
	vec3 t = clamp((log2(max(c * ${EXPOSURE.toFixed(6)}, vec3(1e-9))) - (${TONE_LO.toFixed(5)})) / ${(TONE_HI - TONE_LO).toFixed(5)}, 0.0, 1.0);
	return texture(uTone, t * ${((TONE_N - 1) / TONE_N).toFixed(6)} + ${(0.5 / TONE_N).toFixed(6)}).rgb;
}`;

const SHELL_VERT = `#version 300 es
in vec3 aPos;
in vec2 aUv;
in vec4 aLM;
uniform mat4 uM;
uniform float uUnit;
uniform float uBright;
out vec2 vUv;
out vec3 vLight;
flat out int vMat;
void main() {
	gl_Position = uM * vec4(aPos * uUnit, 1.0);
	vUv = aUv;
	vLight = aLM.rgb * aLM.rgb * uBright;
	vMat = int(aLM.a * 255.0 + 0.5);
}`;
const SHELL_FRAG = `#version 300 es
precision highp float;
in vec2 vUv;
in vec3 vLight;
flat in int vMat;
uniform vec3 uAlbedo[${M}];
uniform vec3 uEmit[${M}];
uniform float uGlass[${M}];
uniform sampler2D uWall;
uniform sampler2D uRibbon;
uniform int uClear;
out vec4 o;
${TONE}
void main() {
	vec3 c;
	float a = 1.0;
	if (vMat == ${WALL}) {
		c = texture(uWall, vUv).rgb * ${BOARD_GLOW.toFixed(2)};
	} else if (vMat == ${RIBBON}) {
		c = texture(uRibbon, vUv).rgb * ${BOARD_GLOW.toFixed(2)};
	} else {
		c = uAlbedo[vMat] * vLight + uEmit[vMat];
	}
	if (uClear == 1) {
		// (Glass: a little light on it, and what is behind through it.)
		a = uGlass[vMat];
		c = ${`vec3(${GLASS_COLOR.join(", ")})`} * vLight * a;
		o = vec4(tone(c), a);
		return;
	}
	o = vec4(tone(c), 1.0);
}`;

// A seat: its back and its pan, in feet along the row, out from its back
// toward the floor, and up - turned the way it faces.
const SEAT_VERT = `#version 300 es
in vec3 aLocal;
in vec4 aSeat;
in vec4 aSeatL;
uniform mat4 uM;
uniform float uUnit;
uniform float uBright;
out vec3 vLight;
void main() {
	float an = aSeat.w / 32767.0 * 3.14159265;
	vec2 f = vec2(cos(an), sin(an));
	vec2 r = vec2(-f.y, f.x);
	vec3 p = aSeat.xyz * uUnit + vec3(r * aLocal.x + f * aLocal.y, aLocal.z);
	gl_Position = uM * vec4(p, 1.0);
	vLight = aSeatL.rgb * aSeatL.rgb * uBright;
}`;
const SEAT_FRAG = `#version 300 es
precision highp float;
in vec3 vLight;
uniform vec3 uSeat;
out vec4 o;
${TONE}
void main() {
	o = vec4(tone(uSeat * vLight), 1.0);
}`;

// A fan: his picture stood up in his seat, square to the camera - which of
// the four of him as the crowd is in his stands (or none, his seat empty).
const FAN_VERT = `#version 300 es
in vec2 aCorner;
in vec4 aSeat;
in vec4 aSeatL;
in vec4 aFan;
uniform mat4 uM;
uniform float uUnit;
uniform float uBright;
uniform vec3 uRight;
uniform vec3 uUp;
uniform float uTaken;
// Each stands' crowd: how far up, waving, clapping.
uniform vec3 uStand[3];
uniform float uPool;
uniform vec2 uGrid;
out vec2 vUv;
out vec3 vLight;
void main() {
	if (aFan.y > uTaken) {
		gl_Position = vec4(2.0, 2.0, 2.0, 1.0);
		return;
	}
	int flags = int(aFan.w + 0.5);
	vec3 st = uStand[flags & 3];
	float pose = 0.0;
	if (st.x * 65535.0 > aFan.z) {
		pose = st.y > 0.5 ? 2.0 : 1.0;
	} else if (st.z > 0.5 && (flags & 4) != 0) {
		pose = 3.0;
	}
	float an = aSeat.w / 32767.0 * 3.14159265;
	vec2 f = vec2(cos(an), sin(an));
	vec3 at = aSeat.xyz * uUnit + vec3(f * 0.15, 0.0);
	// Square to the camera, up the picture - all of him as far off as
	// where he sits (so the row in front covers his legs, and he the row
	// behind, however he leans back toward it).
	vec3 p = at + uRight * (aCorner.x - 0.5) * ${FAN_CELL.w.toFixed(2)} +
		uUp * (aCorner.y * ${FAN_CELL.h.toFixed(2)} - ${FAN_CELL.seat.toFixed(2)});
	vec4 base = uM * vec4(at, 1.0);
	vec4 clip = uM * vec4(p, 1.0);
	gl_Position = vec4(clip.xy, base.z / base.w * clip.w, clip.w);
	float c = aFan.x + pose * uPool;
	float col = mod(c, uGrid.x);
	float row = floor(c / uGrid.x);
	vUv = vec2((col + aCorner.x) / uGrid.x, (row + 1.0 - aCorner.y) / uGrid.y);
	// (Down at his seat, in the shadow of the row in front.)
	vLight = aSeatL.rgb * aSeatL.rgb * uBright * ${FAN_LIGHT.toFixed(2)} *
		mix(0.5, 1.0, smoothstep(0.0, 0.32, aCorner.y));
}`;
const FAN_FRAG = `#version 300 es
precision highp float;
in vec2 vUv;
in vec3 vLight;
uniform sampler2D uAtlas;
out vec4 o;
${TONE}
void main() {
	vec4 t = texture(uAtlas, vUv);
	if (t.a < 0.5) {
		discard;
	}
	o = vec4(tone(t.rgb / t.a * vLight), 1.0);
}`;

// A picture laid flat in the building (the score on the ribbon, the
// rafters): given off (uLit 0) or lit (its light, 1).
const CARD_VERT = `#version 300 es
in vec3 aPos;
in vec2 aUv;
uniform mat4 uM;
out vec2 vUv;
void main() {
	gl_Position = uM * vec4(aPos, 1.0);
	vUv = aUv;
}`;
const CARD_FRAG = `#version 300 es
precision highp float;
in vec2 vUv;
uniform sampler2D uPic;
uniform float uGain;
out vec4 o;
${TONE}
void main() {
	vec4 t = texture(uPic, vUv);
	if (t.a < 0.5) {
		discard;
	}
	o = vec4(tone(t.rgb / t.a * uGain), 1.0);
}`;

type Prog = {
	p: WebGLProgram;
	u: (name: string) => WebGLUniformLocation | null;
};
type Slot = { canvas?: HTMLCanvasElement; tex: WebGLTexture };
type Card = {
	gl: WebGL2RenderingContext;
	canvas: HTMLCanvasElement;
	shell: Prog;
	seats: Prog;
	fans: Prog;
	card: Prog;
	shellVao: WebGLVertexArrayObject;
	seatVao: WebGLVertexArrayObject;
	fanVao: WebGLVertexArrayObject;
	cardVao: WebGLVertexArrayObject;
	cardBuf: WebGLBuffer;
	fanBuf: WebGLBuffer;
	tone?: WebGLTexture;
	slots: Record<"wall" | "ribbon" | "atlas" | "score" | "rafters", Slot>;
	// Whose bowl and crowd are on it.
	bowl: Bowl;
	crowd?: SeatedCrowd;
	lost: boolean;
};

let card: Card | null | undefined;
let toneImg: HTMLImageElement | undefined;

const compile = (
	gl: WebGL2RenderingContext,
	vert: string,
	frag: string,
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
			console.warn("bowl gl:", gl.getShaderInfoLog(sh));
			return undefined;
		}
		gl.attachShader(p, sh);
	}
	gl.linkProgram(p);
	if (!gl.getProgramParameter(p, gl.LINK_STATUS)) {
		console.warn("bowl gl:", gl.getProgramInfoLog(p));
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

const attrib = (
	gl: WebGL2RenderingContext,
	prog: Prog,
	name: string,
	buf: WebGLBuffer,
	size: number,
	type: number,
	normalized: boolean,
	stride: number,
	offset: number,
	divisor = 0,
) => {
	const loc = gl.getAttribLocation(prog.p, name);
	if (loc < 0) {
		return;
	}
	gl.bindBuffer(gl.ARRAY_BUFFER, buf);
	gl.enableVertexAttribArray(loc);
	gl.vertexAttribPointer(loc, size, type, normalized, stride, offset);
	gl.vertexAttribDivisor(loc, divisor);
};

const buffer = (
	gl: WebGL2RenderingContext,
	data: ArrayBufferView,
	target: number = gl.ARRAY_BUFFER,
) => {
	const b = gl.createBuffer();
	gl.bindBuffer(target, b);
	gl.bufferData(target, data, gl.STATIC_DRAW);
	return b;
};

const texture2d = (gl: WebGL2RenderingContext) => {
	const t = gl.createTexture();
	gl.bindTexture(gl.TEXTURE_2D, t);
	gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR);
	gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
	gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
	gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
	gl.texImage2D(
		gl.TEXTURE_2D,
		0,
		gl.RGBA,
		1,
		1,
		0,
		gl.RGBA,
		gl.UNSIGNED_BYTE,
		new Uint8Array(4),
	);
	return t;
};

// A picture onto the card (again, if it is a new one): its colors taken
// back to light (sRGB), shrunk smoothly as it goes off into the distance.
const upload = (
	gl: WebGL2RenderingContext,
	slot: Slot,
	canvas: HTMLCanvasElement,
	maxLevel = 1000,
	sharp = false,
) => {
	gl.bindTexture(gl.TEXTURE_2D, slot.tex);
	if (slot.canvas === canvas) {
		return;
	}
	slot.canvas = canvas;
	gl.pixelStorei(gl.UNPACK_PREMULTIPLY_ALPHA_WEBGL, true);
	gl.texImage2D(
		gl.TEXTURE_2D,
		0,
		gl.SRGB8_ALPHA8,
		gl.RGBA,
		gl.UNSIGNED_BYTE,
		canvas,
	);
	gl.pixelStorei(gl.UNPACK_PREMULTIPLY_ALPHA_WEBGL, false);
	gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAX_LEVEL, maxLevel);
	gl.texParameteri(
		gl.TEXTURE_2D,
		gl.TEXTURE_MIN_FILTER,
		gl.LINEAR_MIPMAP_LINEAR,
	);
	gl.texParameteri(
		gl.TEXTURE_2D,
		gl.TEXTURE_MAG_FILTER,
		sharp ? gl.NEAREST : gl.LINEAR,
	);
	gl.generateMipmap(gl.TEXTURE_2D);
};

const start = (bowl: Bowl): Card | null => {
	if (typeof document === "undefined") {
		return null;
	}
	const canvas = document.createElement("canvas");
	canvas.width = 1;
	canvas.height = 1;
	const gl = canvas.getContext("webgl2", {
		alpha: false,
		antialias: true,
		depth: true,
		stencil: false,
		premultipliedAlpha: false,
		preserveDrawingBuffer: false,
		// (A card that is no card: today's stands instead - unless told to
		// use whatever there is, see sculptGl.ts glForced.)
		failIfMajorPerformanceCaveat: !glForced(),
	});
	if (!gl) {
		return null;
	}
	return onto(gl, canvas, bowl);
};
// (Its programs and buffers on a card, whoever's it is - its own, above, or
// the one the whole game is drawn on: see gpu/scene.ts.)
const onto = (
	gl: WebGL2RenderingContext,
	canvas: HTMLCanvasElement,
	bowl: Bowl,
): Card | null => {
	const shell = compile(gl, SHELL_VERT, SHELL_FRAG);
	const seats = compile(gl, SEAT_VERT, SEAT_FRAG);
	const fans = compile(gl, FAN_VERT, FAN_FRAG);
	const cardP = compile(gl, CARD_VERT, CARD_FRAG);
	if (!shell || !seats || !fans || !cardP) {
		return null;
	}
	// The shell.
	const shellVao = gl.createVertexArray();
	gl.bindVertexArray(shellVao);
	attrib(gl, shell, "aPos", buffer(gl, bowl.pos), 3, gl.SHORT, false, 6, 0);
	attrib(
		gl,
		shell,
		"aUv",
		buffer(gl, bowl.uv),
		2,
		gl.UNSIGNED_SHORT,
		true,
		4,
		0,
	);
	attrib(
		gl,
		shell,
		"aLM",
		buffer(gl, bowl.lightMat),
		4,
		gl.UNSIGNED_BYTE,
		true,
		4,
		0,
	);
	buffer(gl, bowl.index, gl.ELEMENT_ARRAY_BUFFER);
	// The seats, and the fans in them.
	const seatBuf = buffer(gl, bowl.seat);
	const seatLBuf = buffer(gl, bowl.seatLight);
	const seatVao = gl.createVertexArray();
	gl.bindVertexArray(seatVao);
	// (Its back, then its pan.)
	const hw = 0.75;
	const local = new Float32Array([
		-hw,
		-0.55,
		0,
		hw,
		-0.55,
		0,
		hw,
		-0.55,
		1.5,
		-hw,
		-0.55,
		1.5,
		-hw,
		-0.55,
		0,
		hw,
		-0.55,
		0,
		hw,
		0.6,
		0,
		-hw,
		0.6,
		0,
	]);
	attrib(gl, seats, "aLocal", buffer(gl, local), 3, gl.FLOAT, false, 12, 0);
	attrib(gl, seats, "aSeat", seatBuf, 4, gl.SHORT, false, 8, 0, 1);
	attrib(gl, seats, "aSeatL", seatLBuf, 4, gl.UNSIGNED_BYTE, true, 4, 0, 1);
	buffer(
		gl,
		new Uint16Array([0, 1, 2, 0, 2, 3, 4, 5, 6, 4, 6, 7]),
		gl.ELEMENT_ARRAY_BUFFER,
	);
	const fanVao = gl.createVertexArray();
	gl.bindVertexArray(fanVao);
	attrib(
		gl,
		fans,
		"aCorner",
		buffer(gl, new Float32Array([0, 0, 1, 0, 1, 1, 0, 1])),
		2,
		gl.FLOAT,
		false,
		8,
		0,
	);
	attrib(gl, fans, "aSeat", seatBuf, 4, gl.SHORT, false, 8, 0, 1);
	attrib(gl, fans, "aSeatL", seatLBuf, 4, gl.UNSIGNED_BYTE, true, 4, 0, 1);
	const fanBuf = gl.createBuffer();
	gl.bindBuffer(gl.ARRAY_BUFFER, fanBuf);
	gl.bufferData(
		gl.ARRAY_BUFFER,
		new Uint16Array(bowl.seats * 4),
		gl.DYNAMIC_DRAW,
	);
	attrib(gl, fans, "aFan", fanBuf, 4, gl.UNSIGNED_SHORT, false, 8, 0, 1);
	buffer(gl, new Uint16Array([0, 1, 2, 0, 2, 3]), gl.ELEMENT_ARRAY_BUFFER);
	// The pictures laid in it.
	const cardVao = gl.createVertexArray();
	gl.bindVertexArray(cardVao);
	const cardBuf = gl.createBuffer();
	gl.bindBuffer(gl.ARRAY_BUFFER, cardBuf);
	gl.bufferData(gl.ARRAY_BUFFER, 4 * 5 * 4, gl.DYNAMIC_DRAW);
	attrib(gl, cardP, "aPos", cardBuf, 3, gl.FLOAT, false, 20, 0);
	attrib(gl, cardP, "aUv", cardBuf, 2, gl.FLOAT, false, 20, 12);
	buffer(gl, new Uint16Array([0, 1, 2, 0, 2, 3]), gl.ELEMENT_ARRAY_BUFFER);
	gl.bindVertexArray(null);

	// What each material is.
	gl.useProgram(shell.p);
	const albedo = new Float32Array(M * 3);
	const emit = new Float32Array(M * 3);
	const glass = new Float32Array(M);
	BOWL_MATERIALS.forEach((name, i) => {
		const a = ALBEDO[name];
		if (a) {
			albedo.set(linear(a), i * 3);
		}
		const e = EMIT[name];
		if (e) {
			emit.set([e[0] * e[3], e[1] * e[3], e[2] * e[3]], i * 3);
		}
		glass[i] = GLASS[name] ?? 0;
	});
	gl.uniform3fv(shell.u("uAlbedo"), albedo);
	gl.uniform3fv(shell.u("uEmit"), emit);
	gl.uniform1fv(shell.u("uGlass"), glass);
	gl.uniform1i(shell.u("uTone"), 0);
	gl.uniform1i(shell.u("uWall"), 1);
	gl.uniform1i(shell.u("uRibbon"), 2);
	for (const p of [seats, fans, cardP]) {
		gl.useProgram(p.p);
		gl.uniform1i(p.u("uTone"), 0);
	}
	gl.useProgram(fans.p);
	gl.uniform1i(fans.u("uAtlas"), 3);
	gl.useProgram(cardP.p);
	gl.uniform1i(cardP.u("uPic"), 4);

	const c: Card = {
		gl,
		canvas,
		shell,
		seats,
		fans,
		card: cardP,
		shellVao,
		seatVao,
		fanVao,
		cardVao,
		cardBuf,
		fanBuf,
		slots: {
			wall: { tex: texture2d(gl) },
			ribbon: { tex: texture2d(gl) },
			atlas: { tex: texture2d(gl) },
			score: { tex: texture2d(gl) },
			rafters: { tex: texture2d(gl) },
		},
		bowl,
		lost: false,
	};
	canvas.addEventListener("webglcontextlost", () => {
		c.lost = true;
	});
	return c;
};

// The table of colors, from its picture: once it is in.
const toneOf = (c: Card): WebGLTexture | undefined => {
	if (c.tone) {
		return c.tone;
	}
	if (!toneImg) {
		toneImg = new Image();
		toneImg.src = TONE_URL;
	}
	if (!toneImg.complete || toneImg.naturalWidth !== TONE_N * TONE_N) {
		return undefined;
	}
	const pic = document.createElement("canvas");
	pic.width = TONE_N * TONE_N;
	pic.height = TONE_N;
	const ctx = pic.getContext("2d")!;
	ctx.drawImage(toneImg, 0, 0);
	const px = ctx.getImageData(0, 0, pic.width, pic.height).data;
	// (Blue across in blocks, red along each, green down - into red, green,
	// blue as the card has them.)
	const data = new Uint8Array(TONE_N * TONE_N * TONE_N * 4);
	for (let b = 0; b < TONE_N; b++) {
		for (let g = 0; g < TONE_N; g++) {
			for (let r = 0; r < TONE_N; r++) {
				const from = (g * pic.width + b * TONE_N + r) * 4;
				const to = ((b * TONE_N + g) * TONE_N + r) * 4;
				data[to] = px[from]!;
				data[to + 1] = px[from + 1]!;
				data[to + 2] = px[from + 2]!;
				data[to + 3] = 255;
			}
		}
	}
	const { gl } = c;
	const t = gl.createTexture();
	gl.activeTexture(gl.TEXTURE0);
	gl.bindTexture(gl.TEXTURE_3D, t);
	gl.texParameteri(gl.TEXTURE_3D, gl.TEXTURE_MIN_FILTER, gl.LINEAR);
	gl.texParameteri(gl.TEXTURE_3D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
	for (const w of [gl.TEXTURE_WRAP_S, gl.TEXTURE_WRAP_T, gl.TEXTURE_WRAP_R]) {
		gl.texParameteri(gl.TEXTURE_3D, w, gl.CLAMP_TO_EDGE);
	}
	gl.texImage3D(
		gl.TEXTURE_3D,
		0,
		gl.RGBA8,
		TONE_N,
		TONE_N,
		TONE_N,
		0,
		gl.RGBA,
		gl.UNSIGNED_BYTE,
		data,
	);
	c.tone = t;
	return t;
};

// The card for this bowl: made the first time it is wanted.
const cardFor = (bowl: Bowl): Card | undefined => {
	if (card === undefined || (card && card.bowl !== bowl)) {
		try {
			card = start(bowl);
		} catch {
			card = null;
		}
	}
	return card && !card.lost ? card : undefined;
};
export const bowlCardOk = (bowl: Bowl): boolean => cardFor(bowl) !== undefined;

export type BowlStand = { up: number; wave: boolean; clap?: boolean };
export type BowlLook = {
	crowd: SeatedCrowd;
	// The seats' own color (see ArenaSpec.stands).
	seatColor: string;
	// How full it is now (see seatsAt).
	full: number;
	// The crowd along the side, and behind each basket.
	stands: [BowlStand, BowlStand, BowlStand];
	wall: HTMLCanvasElement;
	ribbon: HTMLCanvasElement;
	// The score along the middle of the lower ribbon, this many feet of it.
	score?: { pic: HTMLCanvasElement; ft: number };
	rafters?: HTMLCanvasElement;
};

const quad = (
	c: Card,
	corners: [number, number, number][],
	pic: HTMLCanvasElement,
	slot: Slot,
	gain: number,
) => {
	const { gl } = c;
	const uv = [
		[0, 1],
		[1, 1],
		[1, 0],
		[0, 0],
	];
	const data = new Float32Array(20);
	corners.forEach((p, i) => data.set([...p, ...uv[i]!], i * 5));
	gl.bindBuffer(gl.ARRAY_BUFFER, c.cardBuf);
	gl.bufferSubData(gl.ARRAY_BUFFER, 0, data);
	gl.activeTexture(gl.TEXTURE4);
	upload(gl, slot, pic);
	gl.uniform1f(c.card.u("uGain"), gain);
	gl.drawElements(gl.TRIANGLES, 6, gl.UNSIGNED_SHORT, 0);
};

// Draws the bowl into the game's picture (all of it: it goes first) - false
// if it can't be, yet or at all.
export const drawBowl = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	bowl: Bowl,
	look: BowlLook,
): boolean => {
	const c = cardFor(bowl);
	if (!c) {
		return false;
	}
	const tone = toneOf(c);
	if (!tone) {
		return false;
	}
	const W = ctx.canvas.width;
	const H = ctx.canvas.height;
	if (c.canvas.width !== W || c.canvas.height !== H) {
		c.canvas.width = W;
		c.canvas.height = H;
	}
	paint(c, tone, cam, bowl, look, W, H);
	ctx.drawImage(c.canvas, 0, 0);
	return true;
};

// THE BOWL ON THE CARD THE WHOLE GAME IS DRAWN ON (see gpu/scene.ts), first,
// into the picture `W` x `H` - false if it can't be yet.
const onCards = new WeakMap<WebGL2RenderingContext, Card | null>();
export const paintBowlOn = (
	gl: WebGL2RenderingContext,
	cam: Camera,
	bowl: Bowl,
	look: BowlLook,
	W: number,
	H: number,
): boolean => {
	let c = onCards.get(gl);
	if (c === undefined || (c && c.bowl !== bowl)) {
		try {
			c = onto(gl, gl.canvas as HTMLCanvasElement, bowl);
		} catch {
			c = null;
		}
		onCards.set(gl, c);
	}
	if (!c || c.lost) {
		return false;
	}
	const tone = toneOf(c);
	if (!tone) {
		return false;
	}
	paint(c, tone, cam, bowl, look, W, H);
	return true;
};

const paint = (
	c: Card,
	tone: WebGLTexture,
	cam: Camera,
	bowl: Bowl,
	look: BowlLook,
	W: number,
	H: number,
) => {
	const { gl } = c;
	gl.viewport(0, 0, W, H);
	gl.clearColor(0.027, 0.024, 0.039, 1);
	gl.clear(gl.COLOR_BUFFER_BIT | gl.DEPTH_BUFFER_BIT);
	gl.enable(gl.DEPTH_TEST);
	gl.depthFunc(gl.LEQUAL);
	gl.depthMask(true);
	gl.disable(gl.BLEND);
	gl.disable(gl.CULL_FACE);
	// (In true perspective, even for the upright main camera: the floor is
	// the same either way, and the stands behind the baskets, upright, stood
	// in each other's way - see bowlMatrix.)
	const m = bowlMatrix({ ...cam, upright: false });
	gl.activeTexture(gl.TEXTURE0);
	gl.bindTexture(gl.TEXTURE_3D, tone);

	// The shell.
	gl.useProgram(c.shell.p);
	gl.uniformMatrix4fv(c.shell.u("uM"), false, m);
	gl.uniform1f(c.shell.u("uUnit"), bowl.unit);
	gl.uniform1f(c.shell.u("uBright"), bowl.brightest);
	gl.uniform1i(c.shell.u("uClear"), 0);
	gl.activeTexture(gl.TEXTURE1);
	upload(gl, c.slots.wall, look.wall);
	gl.activeTexture(gl.TEXTURE2);
	upload(gl, c.slots.ribbon, look.ribbon);
	gl.bindVertexArray(c.shellVao);
	const wide = bowl.index instanceof Uint32Array;
	const type = wide ? gl.UNSIGNED_INT : gl.UNSIGNED_SHORT;
	gl.drawElements(gl.TRIANGLES, bowl.opaque, type, 0);

	// The seats.
	gl.useProgram(c.seats.p);
	gl.uniformMatrix4fv(c.seats.u("uM"), false, m);
	gl.uniform1f(c.seats.u("uUnit"), bowl.unit);
	gl.uniform1f(c.seats.u("uBright"), bowl.brightest);
	gl.uniform3fv(c.seats.u("uSeat"), linear(look.seatColor));
	gl.bindVertexArray(c.seatVao);
	gl.drawElementsInstanced(gl.TRIANGLES, 12, gl.UNSIGNED_SHORT, 0, bowl.seats);

	// The fans in them.
	const crowd = look.crowd;
	gl.useProgram(c.fans.p);
	if (c.crowd !== crowd) {
		c.crowd = crowd;
		gl.bindBuffer(gl.ARRAY_BUFFER, c.fanBuf);
		gl.bufferSubData(gl.ARRAY_BUFFER, 0, crowd.seat);
	}
	gl.activeTexture(gl.TEXTURE3);
	upload(gl, c.slots.atlas, crowd.pool.atlas, 3);
	gl.uniformMatrix4fv(c.fans.u("uM"), false, m);
	gl.uniform1f(c.fans.u("uUnit"), bowl.unit);
	gl.uniform1f(c.fans.u("uBright"), bowl.brightest);
	const rl = Math.hypot(cam.right.x, cam.right.y) || 1;
	gl.uniform3f(c.fans.u("uRight"), cam.right.x / rl, cam.right.y / rl, 0);
	gl.uniform3f(c.fans.u("uUp"), cam.up.x, cam.up.y, cam.up.z);
	gl.uniform1f(c.fans.u("uTaken"), takenAt(crowd, shareAt(crowd, look.full)));
	gl.uniform3fv(
		c.fans.u("uStand"),
		look.stands.flatMap((s) => [s.up, s.wave ? 1 : 0, s.clap ? 1 : 0]),
	);
	gl.uniform1f(c.fans.u("uPool"), crowd.pool.n);
	gl.uniform2f(c.fans.u("uGrid"), crowd.pool.cols, crowd.pool.rows);
	gl.bindVertexArray(c.fanVao);
	gl.drawElementsInstanced(gl.TRIANGLES, 6, gl.UNSIGNED_SHORT, 0, bowl.seats);

	// The pictures laid in it: the score on the ribbon, the rafters.
	gl.useProgram(c.card.p);
	gl.uniformMatrix4fv(c.card.u("uM"), false, m);
	gl.bindVertexArray(c.cardVao);
	const a = bowl.anchors;
	if (look.score) {
		const y = a.y0 - a.balcony + 0.08;
		const x0 = COURT_W / 2 - look.score.ft / 2;
		const x1 = COURT_W / 2 + look.score.ft / 2;
		quad(
			c,
			[
				[x0, y, a.ribbon1Bottom],
				[x1, y, a.ribbon1Bottom],
				[x1, y, a.ribbon1Top],
				[x0, y, a.ribbon1Top],
			],
			look.score.pic,
			c.slots.score,
			BOARD_GLOW,
		);
	}
	if (look.rafters) {
		const y = a.y0 - a.balcony - 18;
		const top = a.catwalk + 14;
		const x0 = -40;
		const x1 = COURT_W + 40;
		const h = ((x1 - x0) * look.rafters.height) / look.rafters.width;
		quad(
			c,
			[
				[x0, y, top - h],
				[x1, y, top - h],
				[x1, y, top],
				[x0, y, top],
			],
			look.rafters,
			c.slots.rafters,
			RAFTER_LIGHT,
		);
	}

	// The glass, seen through.
	if (bowl.clear > 0) {
		gl.useProgram(c.shell.p);
		gl.uniform1i(c.shell.u("uClear"), 1);
		gl.bindVertexArray(c.shellVao);
		gl.enable(gl.BLEND);
		gl.blendFunc(gl.ONE, gl.ONE_MINUS_SRC_ALPHA);
		gl.depthMask(false);
		gl.drawElements(
			gl.TRIANGLES,
			bowl.clear,
			type,
			bowl.opaque * (wide ? 4 : 2),
		);
		gl.depthMask(true);
		gl.disable(gl.BLEND);
	}
	gl.bindVertexArray(null);
};

// (For the tests and whoever looks into it: the poses, in the atlas's
// order.)
export const BOWL_POSES = FAN_POSES;
