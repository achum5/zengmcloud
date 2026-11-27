import { BODY_META } from "../bodyMesh.meta.ts";
import {
	gripped,
	holdOf,
	onRim,
	poseOf,
	type PlayerState,
} from "../evaluate.ts";
import type { Look } from "../figure.ts";
import {
	MESH_KINDS,
	MESH_LIGHTS,
	MESH_REST,
	meshData,
	meshLocals,
	meshTurns,
	poseBones,
} from "../meshBody.ts";
import { bodyOf, MESH_BUILD, skeleton, type Body } from "../poses.ts";
import { attrib, buffer, compile, type Prog, rgb } from "./gl.ts";

// THE MEN AS ONE MESH EACH, BENT ON THE GRAPHICS CARD: the body as the court
// has it (see meshBody.ts), sent to the card once - each point, the pieces
// that hold it and where in each - and each frame only each man's 23 pieces,
// as meshBody.ts skinned has them (a turn and a move each, and how far each
// is stretched along and round): the card does what skinned does for every
// point of him. In his kit's plain colors (a test of how fast it is - see
// scene.ts): his jersey, his shorts, his shoes, his skin.

const B = BODY_META.bones.length;
// (What each kind of the mesh is painted: his skin, his jersey - his skin
// at its armholes, see meshBody.ts armOf - his shorts, his shoes.)
const PAINT: Record<string, number> = {
	torso: 1,
	shortsL: 2,
	shortsR: 2,
	shoeL: 3,
	shoeR: 3,
};

const VERT = `#version 300 es
in vec4 aBone;
in vec4 aW;
in vec3 aL0;
in vec3 aL1;
in vec3 aL2;
in vec3 aL3;
in vec3 aN;
in vec2 aPaint;
uniform mat4 uM;
// Each piece at rest: where it starts, along it, forward, his left.
uniform vec4 uRest[${B * 3}];
// Each piece's turn and move, and how far it is stretched along and round.
uniform vec4 uQ[${B * 2}];
uniform vec2 uS[${B}];
uniform vec4 uAt;
uniform float uH;
out vec3 vN;
out vec2 vPaint;
vec3 restAt(int b, vec3 l) {
	vec4 r0 = uRest[b * 3];
	vec4 r1 = uRest[b * 3 + 1];
	vec4 r2 = uRest[b * 3 + 2];
	vec2 st = uS[b];
	return (r0.xyz + vec3(r0.w, r1.xy) * (l.x * st.x) +
		vec3(r1.zw, r2.x) * (l.y * st.y) + r2.yzw * (l.z * st.y)) * uH;
}
void main() {
	vec3 y = vec3(0.0);
	vec4 R = vec4(0.0);
	vec4 D = vec4(0.0);
	vec4 q0 = uQ[int(aBone.x + 0.5) * 2];
	vec3 L[4] = vec3[4](aL0, aL1, aL2, aL3);
	for (int k = 0; k < 4; k++) {
		float w = aW[k];
		if (w <= 0.0) {
			continue;
		}
		int b = int(aBone[k] + 0.5);
		vec4 r = uQ[b * 2];
		vec4 d = uQ[b * 2 + 1];
		float sg = dot(r, q0) < 0.0 ? -w : w;
		R += sg * r;
		D += sg * d;
		y += w * restAt(b, L[k]);
	}
	float n = length(R);
	R /= n;
	D /= n;
	vec3 qv = R.yzw;
	vec3 t = 2.0 * cross(qv, y);
	vec3 p = y + R.x * t + cross(qv, t) +
		2.0 * (R.x * D.yzw - D.x * qv + cross(qv, D.yzw));
	vec3 tn = 2.0 * cross(qv, aN);
	vec3 nn = aN + R.x * tn + cross(qv, tn);
	// His frame on the floor: forward, his left, up.
	float c = cos(uAt.w);
	float s = sin(uAt.w);
	vec3 world = vec3(uAt.x + p.x * c + p.y * s, uAt.y + p.x * s - p.y * c, uAt.z + p.z);
	vN = vec3(nn.x * c + nn.y * s, nn.x * s - nn.y * c, nn.z);
	vPaint = aPaint;
	gl_Position = uM * vec4(world, 1.0);
}`;
const FRAG = `#version 300 es
precision highp float;
in vec3 vN;
in vec2 vPaint;
uniform vec3 uSkin;
uniform vec3 uJersey;
uniform vec3 uShorts;
uniform vec3 uShoe;
// The camera's right, down and toward it - and the light in those.
uniform mat3 uCam;
uniform vec3 uLight;
uniform vec3 uBack;
out vec4 o;
void main() {
	vec3 n = uCam * normalize(vN);
	int p = int(vPaint.x + 0.5);
	vec3 c = p == 1 ? (vPaint.y > 0.5 ? uSkin : uJersey) :
		p == 2 ? uShorts : p == 3 ? uShoe : uSkin;
	float lam = max(0.0, dot(n, uLight));
	float back = max(0.0, dot(n, uBack));
	o = vec4(c * (0.45 + 0.65 * lam + 0.2 * back), 1.0);
}`;

export type MenGl = {
	prog: Prog;
	vao: WebGLVertexArrayObject;
	count: number;
	type: number;
	q: Float32Array;
	s: Float32Array;
};

const TYPICAL = bodyOf(undefined, undefined, MESH_BUILD);

// The body onto the card: once, when the mesh is in (undefined till then).
export const menOnto = (gl: WebGL2RenderingContext): MenGl | undefined => {
	const m = meshData();
	if (!m) {
		return undefined;
	}
	const prog = compile(gl, VERT, FRAG);
	if (!prog) {
		return undefined;
	}
	const local = meshLocals();
	// Each point at rest (a man 1 foot tall), for its normal.
	const R = MESH_REST;
	const rest = new Float32Array(m.n * 3);
	for (let i = 0; i < m.n; i++) {
		for (let j = m.start[i]!; j < m.start[i + 1]!; j++) {
			const b = m.bone[j]! * 12;
			const w = m.weight[j]!;
			for (let k = 0; k < 3; k++) {
				rest[i * 3 + k]! +=
					w *
					(R[b + k]! +
						R[b + 3 + k]! * local[j * 3]! +
						R[b + 6 + k]! * local[j * 3 + 1]! +
						R[b + 9 + k]! * local[j * 3 + 2]!);
			}
		}
	}
	const nrm = new Float32Array(m.n * 3);
	const tris = m.tris;
	for (let t = 0; t < tris.length / 3; t++) {
		const a = tris[t * 3]! * 3;
		const b = tris[t * 3 + 1]! * 3;
		const c = tris[t * 3 + 2]! * 3;
		const ux = rest[b]! - rest[a]!;
		const uy = rest[b + 1]! - rest[a + 1]!;
		const uz = rest[b + 2]! - rest[a + 2]!;
		const vx = rest[c]! - rest[a]!;
		const vy = rest[c + 1]! - rest[a + 1]!;
		const vz = rest[c + 2]! - rest[a + 2]!;
		const nx = uy * vz - uz * vy;
		const ny = uz * vx - ux * vz;
		const nz = ux * vy - uy * vx;
		for (const o of [a, b, c]) {
			nrm[o]! += nx;
			nrm[o + 1]! += ny;
			nrm[o + 2]! += nz;
		}
	}
	// (Out of him, whichever way round the mesh's faces go: his trunk's
	// normals point away from his spine.)
	let out = 0;
	for (let i = 0; i < m.n; i++) {
		out += nrm[i * 3]! * rest[i * 3]! + nrm[i * 3 + 1]! * rest[i * 3 + 1]!;
	}
	const sign = out < 0 ? -1 : 1;
	for (let i = 0; i < m.n; i++) {
		const l =
			Math.hypot(nrm[i * 3]!, nrm[i * 3 + 1]!, nrm[i * 3 + 2]!) * sign || 1;
		nrm[i * 3]! /= l;
		nrm[i * 3 + 1]! /= l;
		nrm[i * 3 + 2]! /= l;
	}
	// A point once for each paint it is in (where his jersey meets his arm,
	// twice).
	const ROW = 25;
	const at = new Map<number, number>();
	const rows: number[] = [];
	const index: number[] = [];
	for (let t = 0; t < tris.length / 3; t++) {
		const paint = PAINT[MESH_KINDS[m.kind[t]!]!] ?? 0;
		for (let c = 0; c < 3; c++) {
			const i = tris[t * 3 + c]!;
			const key = i * 4 + paint;
			let v = at.get(key);
			if (v === undefined) {
				v = at.size;
				at.set(key, v);
				const row = new Array<number>(ROW).fill(0);
				let k = 0;
				for (let j = m.start[i]!; j < m.start[i + 1]! && k < 4; j++, k++) {
					row[k] = m.bone[j]!;
					row[4 + k] = m.weight[j]!;
					row[8 + k * 3] = local[j * 3]!;
					row[9 + k * 3] = local[j * 3 + 1]!;
					row[10 + k * 3] = local[j * 3 + 2]!;
				}
				row[20] = nrm[i * 3]!;
				row[21] = nrm[i * 3 + 1]!;
				row[22] = nrm[i * 3 + 2]!;
				row[23] = paint;
				row[24] = m.arm[i]! / 255;
				rows.push(...row);
			}
			index.push(v);
		}
	}
	const vao = gl.createVertexArray();
	gl.bindVertexArray(vao);
	const buf = buffer(gl, new Float32Array(rows));
	const S = ROW * 4;
	attrib(gl, prog, "aBone", buf, 4, S, 0);
	attrib(gl, prog, "aW", buf, 4, S, 16);
	attrib(gl, prog, "aL0", buf, 3, S, 32);
	attrib(gl, prog, "aL1", buf, 3, S, 44);
	attrib(gl, prog, "aL2", buf, 3, S, 56);
	attrib(gl, prog, "aL3", buf, 3, S, 68);
	attrib(gl, prog, "aN", buf, 3, S, 80);
	attrib(gl, prog, "aPaint", buf, 2, S, 92);
	const wide = at.size > 65535;
	buffer(
		gl,
		wide ? new Uint32Array(index) : new Uint16Array(index),
		gl.ELEMENT_ARRAY_BUFFER,
	);
	gl.bindVertexArray(null);
	gl.useProgram(prog.p);
	gl.uniform4fv(prog.u("uRest"), new Float32Array(R));
	const { LIGHT, BACK_LIGHT } = MESH_LIGHTS;
	gl.uniform3f(prog.u("uLight"), LIGHT[0], LIGHT[1], LIGHT[2]);
	gl.uniform3f(prog.u("uBack"), BACK_LIGHT[0], BACK_LIGHT[1], BACK_LIGHT[2]);
	return {
		prog,
		vao,
		count: index.length,
		type: wide ? gl.UNSIGNED_INT : gl.UNSIGNED_SHORT,
		q: new Float32Array(B * 8),
		s: new Float32Array(B * 2),
	};
};

// His skeleton as the court has him this frame (as sculpt.ts build does),
// and its pieces as the mesh has them.
export const posedMan = (st: PlayerState, body: Body) => {
	const q = poseOf(st);
	const held =
		st.holding || st.grip !== undefined ? holdOf(st, body, q) : undefined;
	const sk = held
		? gripped(held.sk, st, body, q)
		: onRim(skeleton(body, q), st, body);
	return { sk, bones: poseBones(sk, body, TYPICAL) };
};

// Before the men: the camera, the same for all of them.
export const menCamera = (
	gl: WebGL2RenderingContext,
	g: MenGl,
	m: Float32Array,
	cam3: Float32Array,
) => {
	gl.useProgram(g.prog.p);
	gl.bindVertexArray(g.vao);
	gl.uniformMatrix4fv(g.prog.u("uM"), false, m);
	gl.uniformMatrix3fv(g.prog.u("uCam"), false, cam3);
};

// One man.
export const drawMan = (
	gl: WebGL2RenderingContext,
	g: MenGl,
	st: PlayerState,
	body: Body,
	look: Look,
) => {
	const { sk, bones } = posedMan(st, body);
	const dq = meshTurns(bones, body.H);
	for (let i = 0; i < B * 8; i++) {
		g.q[i] = dq[i]!;
	}
	for (let i = 0; i < B; i++) {
		g.s[i * 2] = bones[i]!.ax;
		g.s[i * 2 + 1] = bones[i]!.rad;
	}
	const u = g.prog.u;
	gl.uniform4fv(u("uQ"), g.q);
	gl.uniform2fv(u("uS"), g.s);
	gl.uniform4f(u("uAt"), st.x, st.y, st.z, st.yaw);
	gl.uniform1f(u("uH"), body.H);
	gl.uniform3fv(u("uSkin"), rgb(look.skin, "#8d5a3b"));
	gl.uniform3fv(u("uJersey"), rgb(look.kit.jersey, "#ffffff"));
	gl.uniform3fv(u("uShorts"), rgb(look.kit.shorts, "#ffffff"));
	gl.uniform3fv(u("uShoe"), rgb(look.kit.shoe, "#202020"));
	gl.drawElements(gl.TRIANGLES, g.count, g.type, 0);
	return sk;
};
