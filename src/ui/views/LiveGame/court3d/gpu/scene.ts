import { FLOOR_EDGE } from "../arena.ts";
import { bowlMatrix } from "../bowlData.ts";
import { paintBowlOn } from "../bowlGl.ts";
import { bodyPoint } from "../evaluate.ts";
import { MESH_LIGHTS } from "../meshBody.ts";
import { bowlLookOf, type Frame } from "../scene.ts";
import { glForced } from "../sculptGl.ts";
import { attrib, buffer, compile, type Prog, rgb } from "./gl.ts";
import {
	drawMan,
	type ManReady,
	menCamera,
	menOnto,
	type MenGl,
	paintsOf,
	readyMan,
} from "./men.ts";

// THE GAME DRAWN ON THE GRAPHICS CARD, ALL OF IT IN ONE PICTURE (Global
// Settings, "3D Drawing": a test of how fast it is): the bowl (see
// bowlGl.ts), the floor and the court, the men on it - each one mesh bent
// on the card (see men.ts) - his head, the ball. What stands in front of
// what, the card works out. None of today's pictures of the men (see
// sprite.ts), nor the floor cut into pieces (see planes.ts).

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
out vec4 o;
void main() {
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
		stencil: false,
		premultipliedAlpha: false,
		preserveDrawingBuffer: false,
		failIfMajorPerformanceCaveat: !glForced(),
	});
	if (!gl) {
		return undefined;
	}
	const flat = compile(gl, FLAT_VERT, FLAT_FRAG);
	const round = compile(gl, ROUND_VERT, ROUND_FRAG);
	if (!flat || !round) {
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
	gl.bindVertexArray(null);
	const court = gl.createTexture();
	const sc: Scene = {
		gl,
		flat,
		round,
		flatVao,
		flatBuf,
		roundVao,
		roundCount: ball.index.length,
		court,
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
) => {
	const { gl, flat } = sc;
	const d = new Float32Array([
		x0,
		y0,
		0,
		0,
		0,
		x1,
		y0,
		0,
		1,
		0,
		x1,
		y1,
		0,
		1,
		1,
		x0,
		y1,
		0,
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

	// The men.
	// The men: each one's pieces worked out (the page's own work), then
	// drawn.
	sc.men ??= menOnto(gl);
	const ready = performance.now();
	const men: ManReady[] = [];
	if (sc.men) {
		for (const [i, st] of f.moment.players.entries()) {
			const r = readyMan(st, f.bodyFor(st.pid), f.lookFor(st.pid), sc.ready[i]);
			sc.ready[i] = r;
			men.push(r);
		}
	}
	sc.menMs = performance.now() - ready;
	if (sc.men) {
		menCamera(gl, sc.men, m, cam3);
		for (const man of men) {
			drawMan(gl, sc.men, man);
		}
	}
	// Their heads, and the ball.
	const { round } = sc;
	gl.useProgram(round.p);
	gl.bindVertexArray(sc.roundVao);
	gl.uniformMatrix4fv(round.u("uM"), false, m);
	gl.uniformMatrix3fv(round.u("uCam"), false, cam3);
	gl.uniform3fv(round.u("uLight"), MESH_LIGHTS.LIGHT);
	for (const { st, body, look, head } of men) {
		const at = bodyPoint(st, head);
		const r = body.headR;
		gl.uniform3f(round.u("uAt"), at.x, at.y, at.z);
		gl.uniform3f(round.u("uR"), r, r, r * 1.2);
		gl.uniform3fv(round.u("uColor"), paintsOf(look).skin);
		gl.drawElements(gl.TRIANGLES, sc.roundCount, gl.UNSIGNED_SHORT, 0);
	}
	const ball = f.moment.ball;
	gl.uniform3f(round.u("uAt"), ball.x, ball.y, ball.z);
	gl.uniform3f(round.u("uR"), 0.39, 0.39, 0.39);
	gl.uniform3f(round.u("uColor"), 0.78, 0.38, 0.14);
	gl.drawElements(gl.TRIANGLES, sc.roundCount, gl.UNSIGNED_SHORT, 0);
	gl.bindVertexArray(null);
	return true;
};
