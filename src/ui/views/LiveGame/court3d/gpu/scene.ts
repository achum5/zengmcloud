import { FLOOR_EDGE } from "../arena.ts";
import { project } from "../camera.ts";
import { bowlMatrix } from "../bowlData.ts";
import { paintBowlOn } from "../bowlGl.ts";
import { drawHeadAt } from "../figure.ts";
import { bodyPoint } from "../evaluate.ts";
import { MESH_LIGHTS } from "../meshBody.ts";
import { bowlLookOf, type Frame, warmupLook } from "../scene.ts";
import { glForced } from "../sculptGl.ts";
import { CAST_ALPHA, CASTS, OUTLINE, SOLID } from "../sprite.ts";
import { attrib, buffer, compile, type Prog, rgb } from "./gl.ts";
import {
	drawHands,
	drawMan,
	type ManReady,
	menCamera,
	menOnto,
	type MenGl,
	menRows,
	readyMan,
} from "./men.ts";

// THE GAME DRAWN ON THE GRAPHICS CARD, ALL OF IT IN ONE PICTURE (Global
// Settings, "3D Drawing": a test of it): the bowl (see bowlGl.ts), the floor
// and the court, the men on it - each one mesh bent and painted on the card
// (see men.ts) - his head as today's court draws it (see figure.ts), the
// ball. What stands in front of what, the card works out. None of today's
// pictures of the men (see sprite.ts), nor the floor cut into pieces (see
// planes.ts).

const FLAT_VERT = `#version 300 es
in vec3 aPos;
in vec2 aUv;
uniform mat4 uM;
out vec2 vUv;
void main() {
	gl_Position = uM * vec4(aPos, 1.0);
	vUv = aUv;
}`;
const FLAT_FRAG = `#version 300 es
precision highp float;
in vec2 vUv;
uniform sampler2D uPic;
uniform vec4 uColor;
uniform float uPicOn;
// A soft pool of shadow (see arena.ts drawShadow): so dark at its middle.
uniform float uPool;
out vec4 o;
void main() {
	if (uPool > 0.0) {
		float d = length(vUv * 2.0 - 1.0);
		float a = d < 0.6 ? mix(0.5, 0.3, d / 0.6) : mix(0.3, 0.0, min(1.0, (d - 0.6) / 0.4));
		o = vec4(0.0, 0.0, 0.0, a * uPool);
		return;
	}
	o = uPicOn > 0.5 ? texture(uPic, vUv) : uColor;
}`;

// A round thing - his head, the ball: a sphere, so wide each way.
const ROUND_VERT = `#version 300 es
in vec3 aPos;
uniform mat4 uM;
uniform vec3 uAt;
uniform vec3 uR;
out vec3 vN;
void main() {
	vN = aPos;
	gl_Position = uM * vec4(uAt + aPos * uR, 1.0);
}`;
const ROUND_FRAG = `#version 300 es
precision highp float;
in vec3 vN;
uniform vec3 uColor;
uniform mat3 uCam;
uniform vec3 uLight;
out vec4 o;
void main() {
	vec3 n = uCam * normalize(vN);
	o = vec4(uColor * (0.45 + 0.65 * max(0.0, dot(n, uLight))), 1.0);
}`;

// A head: today's, drawn on a sheet of them (see heads), stood up where his
// head is - its front as near the camera as the front of his head, round
// to its edge (so his own arm up in front of his face is in front of it).
const HEAD_VERT = `#version 300 es
in vec2 aCorner;
// Where it is on the picture (px), and on the sheet (0 - 1); the picture's
// size; his head's middle on the picture and its radius (px); how near the
// camera its middle and its front are.
uniform vec4 uBox;
uniform vec4 uUv;
uniform vec2 uSize;
uniform vec2 uZ;
out vec2 vUv;
out vec2 vPx;
void main() {
	vPx = mix(uBox.xy, uBox.zw, aCorner);
	vUv = mix(uUv.xy, uUv.zw, aCorner);
	gl_Position = vec4(
		vPx.x / uSize.x * 2.0 - 1.0,
		1.0 - vPx.y / uSize.y * 2.0,
		uZ.x,
		1.0
	);
}`;
const HEAD_FRAG = `#version 300 es
precision highp float;
in vec2 vUv;
in vec2 vPx;
uniform sampler2D uPic;
uniform vec3 uC;
uniform vec2 uZ;
out vec4 o;
const float SOLID = ${SOLID / 255};
const vec3 RIM = vec3(${OUTLINE.map((c) => (c / 255).toFixed(4)).join(", ")});
void main() {
	o = texture(uPic, vUv);
	vec2 d = (vPx - uC.xy) / uC.z;
	if (o.a < SOLID) {
		// The dark rim round him (see sprite.ts rim): a pixel wide, under the
		// edge's own soft pixels - as far back as the back of his head, so his
		// own neck in front of it is not rimmed.
		vec2 t = 1.0 / vec2(textureSize(uPic, 0));
		float n = max(
			max(texture(uPic, vUv + vec2(t.x, 0.0)).a, texture(uPic, vUv - vec2(t.x, 0.0)).a),
			max(texture(uPic, vUv + vec2(0.0, t.y)).a, texture(uPic, vUv - vec2(0.0, t.y)).a)
		);
		if (n >= SOLID) {
			o = vec4(o.rgb + RIM * (1.0 - o.a), 1.0);
			gl_FragDepth = (2.0 * uZ.x - uZ.y) * 0.5 + 0.5;
			return;
		}
	}
	if (o.a < 0.004) {
		discard;
	}
	float z = mix(uZ.x, uZ.y, sqrt(max(0.0, 1.0 - dot(d, d))));
	gl_FragDepth = z * 0.5 + 0.5;
}`;

// Which of his faces are left out: his back ones (the mesh's faces go
// round clockwise seen from outside him, on the card's picture), his front
// ones for the dark edge round him, and his reflection's - upside down, so
// the other way round.
const BODY_CULL = WebGL2RenderingContext.BACK;
const EDGE_CULL = WebGL2RenderingContext.FRONT;
const MIRRORED_CULL = WebGL2RenderingContext.FRONT;
// The floor is polished: everybody on it shows in it this faintly (see
// scene.ts SHINE).
const SHINE = 0.24;

type Scene = {
	gl: WebGL2RenderingContext;
	flat: Prog;
	round: Prog;
	flatVao: WebGLVertexArrayObject;
	flatBuf: WebGLBuffer;
	roundVao: WebGLVertexArrayObject;
	roundCount: number;
	court: WebGLTexture;
	courtOf?: HTMLCanvasElement;
	men?: MenGl;
	head: Prog;
	headVao: WebGLVertexArrayObject;
	heads: CanvasRenderingContext2D;
	headTex: WebGLTexture;
	// The shadows the men cast, drawn small (see softOf).
	soft?: Soft;
	ready: ManReady[];
	// The time the last frame spent working out the men (ms).
	menMs: number;
	lost: boolean;
};
export type GpuScene = Scene;

const sphere = (rings = 10, segs = 14) => {
	const pos: number[] = [];
	for (let r = 0; r <= rings; r++) {
		const th = (r / rings) * Math.PI;
		for (let s = 0; s <= segs; s++) {
			const ph = (s / segs) * 2 * Math.PI;
			pos.push(
				Math.sin(th) * Math.cos(ph),
				Math.sin(th) * Math.sin(ph),
				Math.cos(th),
			);
		}
	}
	const index: number[] = [];
	for (let r = 0; r < rings; r++) {
		for (let s = 0; s < segs; s++) {
			const a = r * (segs + 1) + s;
			const b = a + segs + 1;
			index.push(a, b, a + 1, a + 1, b, b + 1);
		}
	}
	return { pos: new Float32Array(pos), index: new Uint16Array(index) };
};

// The card, on the canvas the game is shown on - undefined if there is
// none to be had (today's drawing, then).
export const gpuScene = (canvas: HTMLCanvasElement): GpuScene | undefined => {
	const gl = canvas.getContext("webgl2", {
		alpha: false,
		antialias: true,
		depth: true,
		stencil: true,
		premultipliedAlpha: false,
		preserveDrawingBuffer: false,
		failIfMajorPerformanceCaveat: !glForced(),
	});
	if (!gl) {
		return undefined;
	}
	const flat = compile(gl, FLAT_VERT, FLAT_FRAG);
	const round = compile(gl, ROUND_VERT, ROUND_FRAG);
	const head = compile(gl, HEAD_VERT, HEAD_FRAG);
	const heads = document.createElement("canvas").getContext("2d");
	if (!flat || !round || !head || !heads) {
		return undefined;
	}
	const flatVao = gl.createVertexArray();
	gl.bindVertexArray(flatVao);
	const flatBuf = gl.createBuffer();
	gl.bindBuffer(gl.ARRAY_BUFFER, flatBuf);
	gl.bufferData(gl.ARRAY_BUFFER, 4 * 5 * 4, gl.DYNAMIC_DRAW);
	attrib(gl, flat, "aPos", flatBuf, 3, 20, 0);
	attrib(gl, flat, "aUv", flatBuf, 2, 20, 12);
	buffer(gl, new Uint16Array([0, 1, 2, 0, 2, 3]), gl.ELEMENT_ARRAY_BUFFER);
	const ball = sphere();
	const roundVao = gl.createVertexArray();
	gl.bindVertexArray(roundVao);
	attrib(gl, round, "aPos", buffer(gl, ball.pos), 3, 12, 0);
	buffer(gl, ball.index, gl.ELEMENT_ARRAY_BUFFER);
	const headVao = gl.createVertexArray();
	gl.bindVertexArray(headVao);
	attrib(
		gl,
		head,
		"aCorner",
		buffer(gl, new Float32Array([0, 0, 1, 0, 1, 1, 0, 1])),
		2,
		8,
		0,
	);
	buffer(gl, new Uint16Array([0, 1, 2, 0, 2, 3]), gl.ELEMENT_ARRAY_BUFFER);
	gl.bindVertexArray(null);
	const court = gl.createTexture();
	const headTex = gl.createTexture();
	gl.bindTexture(gl.TEXTURE_2D, headTex);
	gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR);
	gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
	gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
	gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
	const sc: Scene = {
		gl,
		flat,
		round,
		flatVao,
		flatBuf,
		roundVao,
		roundCount: ball.index.length,
		court,
		head,
		headVao,
		heads,
		headTex,
		ready: [],
		menMs: 0,
		lost: false,
	};
	canvas.addEventListener("webglcontextlost", () => {
		sc.lost = true;
	});
	return sc;
};

const flatQuad = (
	sc: Scene,
	x0: number,
	y0: number,
	x1: number,
	y1: number,
	color?: [number, number, number],
	z = 0,
) => {
	const { gl, flat } = sc;
	const d = new Float32Array([
		x0,
		y0,
		z,
		0,
		0,
		x1,
		y0,
		z,
		1,
		0,
		x1,
		y1,
		z,
		1,
		1,
		x0,
		y1,
		z,
		0,
		1,
	]);
	gl.bindBuffer(gl.ARRAY_BUFFER, sc.flatBuf);
	gl.bufferSubData(gl.ARRAY_BUFFER, 0, d);
	gl.uniform1f(flat.u("uPicOn"), color ? 0 : 1);
	if (color) {
		gl.uniform4f(flat.u("uColor"), color[0], color[1], color[2], 1);
	}
	gl.drawElements(gl.TRIANGLES, 6, gl.UNSIGNED_SHORT, 0);
};

// The court's picture onto the card (again, if it is a new one).
const courtPic = (sc: Scene, pic: HTMLCanvasElement) => {
	const { gl } = sc;
	gl.activeTexture(gl.TEXTURE0);
	gl.bindTexture(gl.TEXTURE_2D, sc.court);
	if (sc.courtOf === pic) {
		return;
	}
	sc.courtOf = pic;
	gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA, gl.UNSIGNED_BYTE, pic);
	gl.texParameteri(
		gl.TEXTURE_2D,
		gl.TEXTURE_MIN_FILTER,
		gl.LINEAR_MIPMAP_LINEAR,
	);
	gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
	gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
	gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
	gl.generateMipmap(gl.TEXTURE_2D);
};

// THE FRAME, as scene.ts drawFrame has it - onto the card's canvas, `W` x
// `H`. False if it can't be (the card lost): today's drawing, then.
export const drawGpu = (
	sc: GpuScene,
	f: Frame,
	W: number,
	H: number,
): boolean => {
	const { gl } = sc;
	if (sc.lost || gl.isContextLost()) {
		return false;
	}
	const { cam } = f;
	const look = f.black ? undefined : bowlLookOf(f);
	const bowl =
		look !== undefined && paintBowlOn(gl, cam, f.arena.bowl!.data, look, W, H);
	if (!bowl) {
		gl.viewport(0, 0, W, H);
		gl.clearColor(0.027, 0.024, 0.039, 1);
		gl.clear(gl.COLOR_BUFFER_BIT | gl.DEPTH_BUFFER_BIT);
	}
	if (f.black) {
		return true;
	}
	const m = bowlMatrix(cam);
	// The camera's right, down and toward it, for the light on what is
	// round (as meshBody.ts lights him).
	const cam3 = new Float32Array([
		cam.right.x,
		-cam.up.x,
		-cam.fwd.x,
		cam.right.y,
		-cam.up.y,
		-cam.fwd.y,
		cam.right.z,
		-cam.up.z,
		-cam.fwd.z,
	]);
	gl.disable(gl.BLEND);
	gl.disable(gl.CULL_FACE);
	gl.enable(gl.DEPTH_TEST);
	gl.depthMask(true);

	// The floor round the court and the court: over the bowl's own floor,
	// whatever it is (as the page's drawing lays them over it).
	gl.depthFunc(gl.ALWAYS);
	gl.useProgram(sc.flat.p);
	gl.uniformMatrix4fv(sc.flat.u("uM"), false, m);
	gl.uniform1i(sc.flat.u("uPic"), 0);
	gl.bindVertexArray(sc.flatVao);
	const E = FLOOR_EDGE;
	flatQuad(sc, E.x0, E.y0, E.x1, E.y1, rgb(f.apron ?? f.padColor, "#3a2a1a"));
	const court = f.arena.court;
	if (court) {
		courtPic(sc, court.canvas);
		flatQuad(sc, court.x, court.y, court.x + court.w, court.y + court.h);
	}
	gl.depthFunc(gl.LEQUAL);

	// The men: each one worked out (the page's own work), then drawn,
	// nearest first.
	sc.men ??= menOnto(gl);
	const ready = performance.now();
	const men: ManReady[] = [];
	if (sc.men) {
		for (const [i, st] of f.moment.players.entries()) {
			const look = f.lookFor(st.pid);
			const r = readyMan(
				cam,
				st,
				f.bodyFor(st.pid),
				f.moment.warm ? warmupLook(look, f.warmups[st.team]) : look,
				sc.ready[i],
			);
			sc.ready[i] = r;
			men.push(r);
		}
		men.sort((a, b) => a.depth - b.depth);
	}
	sc.menMs = performance.now() - ready;
	if (sc.men) {
		const g = sc.men;
		menRows(gl, g, men);
		menCamera(gl, g, m);
		// What is laid over the floor under them - their reflections in it,
		// soft pools of shadow round their feet, the shadows they cast - each
		// over each pixel once (see once), blended in, behind all of them.
		gl.enable(gl.STENCIL_TEST);
		gl.clearStencil(0);
		gl.clear(gl.STENCIL_BUFFER_BIT);
		let ref = 0;
		const once = () => {
			ref += 1;
			if (ref > 255) {
				gl.clear(gl.STENCIL_BUFFER_BIT);
				ref = 1;
			}
			gl.stencilFunc(gl.NOTEQUAL, ref, 0xff);
			gl.stencilOp(gl.KEEP, gl.KEEP, gl.REPLACE);
		};
		gl.enable(gl.BLEND);
		gl.blendFunc(gl.ONE, gl.ONE_MINUS_SRC_ALPHA);
		gl.depthMask(false);
		gl.depthFunc(gl.ALWAYS);
		gl.enable(gl.CULL_FACE);
		gl.cullFace(MIRRORED_CULL);
		for (let i = men.length - 1; i >= 0; i--) {
			const man = men[i]!;
			once();
			drawMan(
				gl,
				g,
				man,
				i,
				{ mode: 3, shine: man.st.anim === "sit" ? 0 : SHINE },
				W,
				H,
			);
		}
		gl.disable(gl.CULL_FACE);
		gl.depthFunc(gl.LEQUAL);
		gl.disable(gl.STENCIL_TEST);
		pools(sc, f, men, m);
		gl.useProgram(g.prog.p);
		// (Drawn a third the size and laid over the floor drawn back up,
		// soft-edged, as a shadow is - see softOf. Sharp, if that can't be.)
		const soft = softOf(sc, W, H);
		if (soft) {
			gl.bindFramebuffer(gl.FRAMEBUFFER, soft.fb);
			gl.viewport(0, 0, soft.w, soft.h);
			gl.clearColor(0, 0, 0, 0);
			gl.clear(gl.COLOR_BUFFER_BIT | gl.STENCIL_BUFFER_BIT);
			ref = 0;
			gl.disable(gl.DEPTH_TEST);
		}
		gl.enable(gl.STENCIL_TEST);
		for (const [i, man] of men.entries()) {
			const lift = Math.max(0, man.st.z) / (man.body.H * 1.05);
			if (lift >= 1) {
				continue;
			}
			for (const [lean, run] of CASTS) {
				once();
				drawMan(
					gl,
					g,
					man,
					i,
					{
						mode: 2,
						cast: castOf(cam, man.st, lean, run),
						alpha: CAST_ALPHA * (1 - lift),
					},
					W,
					H,
				);
			}
		}
		gl.disable(gl.STENCIL_TEST);
		if (soft) {
			gl.bindFramebuffer(gl.FRAMEBUFFER, null);
			gl.viewport(0, 0, W, H);
			gl.enable(gl.DEPTH_TEST);
			gl.depthFunc(gl.ALWAYS);
			gl.useProgram(sc.flat.p);
			gl.uniformMatrix4fv(sc.flat.u("uM"), false, WHOLE);
			gl.bindVertexArray(sc.flatVao);
			gl.activeTexture(gl.TEXTURE0);
			gl.bindTexture(gl.TEXTURE_2D, soft.tex);
			flatQuad(sc, -1, -1, 1, 1);
			gl.depthFunc(gl.LEQUAL);
			gl.useProgram(g.prog.p);
		}
		gl.disable(gl.BLEND);
		gl.depthMask(true);
		// Each man: the dark edge round him, then him.
		gl.enable(gl.CULL_FACE);
		for (const [i, man] of men.entries()) {
			gl.cullFace(EDGE_CULL);
			drawMan(gl, g, man, i, { mode: 1 }, W, H);
			gl.cullFace(BODY_CULL);
			drawMan(gl, g, man, i, { mode: 0 }, W, H);
		}
		gl.disable(gl.CULL_FACE);
		// Their hands, over them (blended in at their soft edges).
		gl.enable(gl.BLEND);
		gl.blendFunc(gl.ONE, gl.ONE_MINUS_SRC_ALPHA);
		men.forEach((man, i) => drawHands(gl, g, man, i, W, H));
		gl.disable(gl.BLEND);
	}
	// The ball - unless it is in a man's hands (his, then: see drawHands).
	const { round } = sc;
	gl.useProgram(round.p);
	gl.bindVertexArray(sc.roundVao);
	gl.uniformMatrix4fv(round.u("uM"), false, m);
	gl.uniformMatrix3fv(round.u("uCam"), false, cam3);
	gl.uniform3fv(round.u("uLight"), MESH_LIGHTS.LIGHT);
	const ball = f.moment.ball;
	if (!men.some((man) => man.ball > 0)) {
		gl.uniform3f(round.u("uAt"), ball.x, ball.y, ball.z);
		gl.uniform3f(round.u("uR"), 0.39, 0.39, 0.39);
		gl.uniform3f(round.u("uColor"), 0.78, 0.38, 0.14);
		gl.drawElements(gl.TRIANGLES, sc.roundCount, gl.UNSIGNED_SHORT, 0);
	}
	heads(sc, f, men, m, W, H);
	gl.bindVertexArray(null);
	return true;
};

// The shadows the men cast, drawn a third the size of the picture (on a
// sheet of their own) and laid over the floor drawn back up, so soft-edged
// (as sprite.ts darkOf has them today) - undefined if the card can't.
const SOFT = 3;
type Soft = {
	fb: WebGLFramebuffer;
	tex: WebGLTexture;
	stencil: WebGLRenderbuffer;
	w: number;
	h: number;
	ok: boolean;
};
const softOf = (sc: Scene, W: number, H: number): Soft | undefined => {
	const { gl } = sc;
	const w = Math.max(1, Math.ceil(W / SOFT));
	const h = Math.max(1, Math.ceil(H / SOFT));
	sc.soft ??= {
		fb: gl.createFramebuffer(),
		tex: gl.createTexture(),
		stencil: gl.createRenderbuffer(),
		w: 0,
		h: 0,
		ok: false,
	};
	const s = sc.soft;
	if (s.w !== w || s.h !== h) {
		s.w = w;
		s.h = h;
		gl.activeTexture(gl.TEXTURE0);
		gl.bindTexture(gl.TEXTURE_2D, s.tex);
		gl.texImage2D(
			gl.TEXTURE_2D,
			0,
			gl.RGBA8,
			w,
			h,
			0,
			gl.RGBA,
			gl.UNSIGNED_BYTE,
			null,
		);
		gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR);
		gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
		gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
		gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
		gl.bindRenderbuffer(gl.RENDERBUFFER, s.stencil);
		gl.renderbufferStorage(gl.RENDERBUFFER, gl.DEPTH24_STENCIL8, w, h);
		gl.bindFramebuffer(gl.FRAMEBUFFER, s.fb);
		gl.framebufferTexture2D(
			gl.FRAMEBUFFER,
			gl.COLOR_ATTACHMENT0,
			gl.TEXTURE_2D,
			s.tex,
			0,
		);
		gl.framebufferRenderbuffer(
			gl.FRAMEBUFFER,
			gl.DEPTH_STENCIL_ATTACHMENT,
			gl.RENDERBUFFER,
			s.stencil,
		);
		s.ok =
			gl.checkFramebufferStatus(gl.FRAMEBUFFER) === gl.FRAMEBUFFER_COMPLETE;
		gl.bindFramebuffer(gl.FRAMEBUFFER, null);
	}
	return s.ok ? s : undefined;
};
// (The whole picture, corner to corner.)
const WHOLE = new Float32Array([
	1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1,
]);

// A shadow cast by the lights (see sprite.ts CASTS): today laid on the
// picture from his feet - for each pixel he is up, `lean` across it and
// `run` down it - here on the floor: so far across it (x) and toward the
// camera (y) for each foot up he is, to come out the same on the picture.
const castOf = (
	cam: Frame["cam"],
	st: { x: number; y: number },
	lean: number,
	run: number,
): [number, number] => {
	const o = project(cam, { x: st.x, y: st.y, z: 0 });
	const up = project(cam, { x: st.x, y: st.y, z: 1 });
	const ax = project(cam, { x: st.x + 1, y: st.y, z: 0 });
	const ay = project(cam, { x: st.x, y: st.y + 1, z: 0 });
	const h = o.y - up.y;
	// Across the picture and down it, for a foot each way over the floor.
	const a = ax.x - o.x;
	const b = ay.x - o.x;
	const c = ax.y - o.y;
	const d = ay.y - o.y;
	const det = a * d - b * c;
	if (Math.abs(det) < 1e-9) {
		return [0, 0];
	}
	const px = lean * h;
	const py = run * h;
	return [(d * px - b * py) / det, (a * py - c * px) / det];
};

// Soft pools of shadow round their feet, shrinking as they leave the floor
// (as scene.ts lays them), and under the ball.
const pools = (
	sc: Scene,
	f: Frame,
	men: readonly ManReady[],
	m: Float32Array,
) => {
	const { gl, flat } = sc;
	gl.useProgram(flat.p);
	gl.uniformMatrix4fv(flat.u("uM"), false, m);
	gl.bindVertexArray(sc.flatVao);
	const pool = (x: number, y: number, r: number, strength: number) => {
		if (strength <= 0.01) {
			return;
		}
		gl.uniform1f(flat.u("uPool"), strength);
		flatQuad(sc, x - r, y - r, x + r, y + r, undefined, 0.005);
	};
	for (const { st } of men) {
		const lift = Math.min(1, Math.max(0, st.z) / 4);
		pool(st.x, st.y, 1.25 * (1 - lift * 0.35), 1 - lift * 0.6);
	}
	const ball = f.moment.ball;
	pool(ball.x, ball.y, 0.45, Math.max(0, 1 - ball.z / 14) * 0.9);
	gl.uniform1f(flat.u("uPool"), 0);
};

// Where a point is in the card's picture: its x, y (-1 to 1) and how near.
const clip = (m: Float32Array, p: { x: number; y: number; z: number }) => {
	const c = [0, 1, 2, 3].map(
		(i) => m[i]! * p.x + m[4 + i]! * p.y + m[8 + i]! * p.z + m[12 + i]!,
	);
	return { x: c[0]! / c[3]!, y: c[1]! / c[3]!, z: c[2]! / c[3]!, w: c[3]! };
};

// THE HEADS, as today's court draws them (see figure.ts drawHeadAt): each
// on a sheet of them, in a row; the sheet onto the card; each stood up
// where his head is, furthest first, over what is behind it.
const GAP = 3;
const heads = (
	sc: Scene,
	f: Frame,
	men: readonly ManReady[],
	m: Float32Array,
	W: number,
	H: number,
) => {
	const { gl } = sc;
	const ctx = sc.heads;
	const { cam } = f;
	const cells = men.map((man) => {
		const { x, y, r } = man.b.head;
		const x0 = Math.floor(x - r * 1.9) - 2;
		const y0 = Math.floor(y - r * 2) - 2;
		return {
			man,
			x0,
			y0,
			w: Math.ceil(x + r * 1.9) + 2 - x0,
			h: Math.ceil(y + r * 1.6) + 2 - y0,
			at: 0,
		};
	});
	const wide = cells.reduce((s, c) => s + c.w + GAP, GAP);
	const tall = cells.reduce((s, c) => Math.max(s, c.h), 0) + GAP * 2;
	if (wide <= GAP || tall <= GAP * 2) {
		return;
	}
	const cv = ctx.canvas;
	if (cv.width < wide || cv.height < tall) {
		cv.width = Math.max(cv.width, Math.ceil(wide / 64) * 64);
		cv.height = Math.max(cv.height, Math.ceil(tall / 16) * 16);
	}
	ctx.setTransform(1, 0, 0, 1, 0, 0);
	ctx.clearRect(0, 0, cv.width, cv.height);
	let left = GAP;
	for (const c of cells) {
		c.at = left;
		const { st, body, look, b } = c.man;
		ctx.save();
		ctx.setTransform(1, 0, 0, 1, left - c.x0, GAP - c.y0);
		drawHeadAt(ctx, cam, st, body, look, b.sk.head);
		ctx.restore();
		left += c.w + GAP;
	}
	gl.activeTexture(gl.TEXTURE0);
	gl.bindTexture(gl.TEXTURE_2D, sc.headTex);
	gl.pixelStorei(gl.UNPACK_PREMULTIPLY_ALPHA_WEBGL, true);
	gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA, gl.UNSIGNED_BYTE, cv);
	gl.pixelStorei(gl.UNPACK_PREMULTIPLY_ALPHA_WEBGL, false);
	const { head } = sc;
	gl.useProgram(head.p);
	gl.bindVertexArray(sc.headVao);
	gl.uniform1i(head.u("uPic"), 0);
	gl.uniform2f(head.u("uSize"), W, H);
	gl.enable(gl.BLEND);
	gl.blendFunc(gl.ONE, gl.ONE_MINUS_SRC_ALPHA);
	gl.depthMask(false);
	const toCam = (p: { x: number; y: number; z: number }, d: number) => {
		const dx = cam.pos.x - p.x;
		const dy = cam.pos.y - p.y;
		const dz = cam.pos.z - p.z;
		const l = Math.hypot(dx, dy, dz) || 1;
		return {
			x: p.x + (dx / l) * d,
			y: p.y + (dy / l) * d,
			z: p.z + (dz / l) * d,
		};
	};
	for (let i = cells.length - 1; i >= 0; i--) {
		const c = cells[i]!;
		const { st, body, b } = c.man;
		const mid = bodyPoint(st, b.sk.head);
		const zc = clip(m, mid).z;
		const zf = clip(m, toCam(mid, body.headR)).z;
		gl.uniform4f(head.u("uBox"), c.x0, c.y0, c.x0 + c.w, c.y0 + c.h);
		gl.uniform4f(
			head.u("uUv"),
			c.at / cv.width,
			GAP / cv.height,
			(c.at + c.w) / cv.width,
			(GAP + c.h) / cv.height,
		);
		gl.uniform3f(head.u("uC"), b.head.x, b.head.y, b.head.r);
		gl.uniform2f(head.u("uZ"), zc, zf);
		gl.drawElements(gl.TRIANGLES, 6, gl.UNSIGNED_SHORT, 0);
	}
	gl.depthMask(true);
	gl.disable(gl.BLEND);
};
