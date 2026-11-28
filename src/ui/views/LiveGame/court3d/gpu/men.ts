import { BODY_META } from "../bodyMesh.meta.ts";
import type { Camera } from "../camera.ts";
import type { PlayerState } from "../evaluate.ts";
import type { FaceTex } from "../faces.ts";
import type { Look } from "../figure.ts";
import type { PrintLevel } from "../gameBall.ts";
import { ART_W, type KitArt } from "../kitArt.ts";
import {
	MESH_KINDS,
	MESH_REST,
	meshData,
	meshLocals,
	meshRaised,
	meshTrunkOf,
	meshTurns,
} from "../meshBody.ts";
import { MESH_PAINT, meshFaceOf, meshPaintOf } from "../meshGl.ts";
import type { Body } from "../poses.ts";
import { build, type Built, hemOf, MESH_KEEPS, meshRig } from "../sculpt.ts";
import { BOWL_FAR, BOWL_NEAR } from "../bowlData.ts";
import {
	MASS_TABLE,
	massTable,
	PAINT_COMMON,
	PAINT_HEAD,
	paintParams,
	SCULPT_AT,
} from "../sculptGl.ts";
import { shortsFlareFor } from "../shortsEra.ts";
import { OUTLINE, SHINE_BANDS } from "../sprite.ts";
import { attrib, buffer, compile, type Prog } from "./gl.ts";

// THE MEN AS ONE MESH EACH, BENT AND PAINTED ON THE GRAPHICS CARD: the body
// as the court has it (see meshBody.ts), sent to the card once - each point,
// the pieces that hold it and where in each - and each frame only each
// man's 23 pieces as posed (a row of numbers each: see BONE). The card does
// for every point of him what meshBody.ts skinned does - where it is, where
// his trunk alone carries it, his ribs kept under a raised arm - and for
// every pixel of him what the sprite's card does (see meshGl.ts MESH_PAINT):
// what he wears there, painted as sculpt.ts paints it (his jersey's cut and
// trim, his team's picture, his number and name, his shorts, socks, shoes,
// sleeves and bands), and lit the same.

const B = BODY_META.bones.length;
// A piece's numbers, in texels: its turn and move (a dual quaternion, two),
// where it starts and how far it is stretched along, which way along it,
// forward, his left (and how far it is stretched round).
const BONE = 6;
// (Men drawn at most in a frame: rows of numbers on the card.)
const ROWS = 64;
export const MEN_ROWS = ROWS;

const SKIN_VERT = `#version 300 es
precision highp float;
precision highp int;
precision highp sampler2D;
in vec4 aBone;
in vec4 aW;
in vec3 aN;
// The kind of face it is in, how much it goes with his arm, how much it is
// kept to his trunk under a raised arm, and which arm.
in vec4 aMisc;
// Where it is held in each piece's frame...
in vec3 aL0;
in vec3 aL1;
in vec3 aL2;
in vec3 aL3;
// ... and in the frame that carries it with his trunk alone, and which.
in vec3 aC0;
in vec3 aC1;
in vec3 aC2;
in vec3 aC3;
in vec4 aCB;
// Each piece at rest: where it starts, along it, forward, his left.
uniform vec4 uRest[${B * 3}];
uniform sampler2D uBones;
uniform int uRow;
uniform float uH;
uniform vec2 uRaised;
// Where it is in his frame, where his trunk alone carries it, which way
// its face looks (for each drawing of him - see VERT).
out vec3 xP;
out vec3 xT;
out vec3 xN;
vec4 bt(int b, int j) {
	return texelFetch(uBones, ivec2(b * ${BONE} + j, uRow), 0);
}
vec3 restAt(int b, vec3 l) {
	vec4 r0 = uRest[b * 3];
	vec4 r1 = uRest[b * 3 + 1];
	vec4 r2 = uRest[b * 3 + 2];
	vec2 st = vec2(bt(b, 2).w, bt(b, 3).w);
	return (r0.xyz + vec3(r0.w, r1.xy) * (l.x * st.x) +
		vec3(r1.zw, r2.x) * (l.y * st.y) + r2.yzw * (l.z * st.y)) * uH;
}
vec3 frameAt(int b, vec3 l) {
	vec4 h = bt(b, 2);
	vec4 a = bt(b, 3);
	return h.xyz + a.xyz * (l.x * h.w * uH) + bt(b, 4).xyz * (l.y * a.w * uH) +
		bt(b, 5).xyz * (l.z * a.w * uH);
}
void main() {
	vec3 y = vec3(0.0);
	vec4 R = vec4(0.0);
	vec4 D = vec4(0.0);
	vec3 T = vec3(0.0);
	float tw = 0.0;
	vec4 q0 = bt(int(aBone.x + 0.5), 0);
	vec3 L[4] = vec3[4](aL0, aL1, aL2, aL3);
	vec3 C[4] = vec3[4](aC0, aC1, aC2, aC3);
	for (int k = 0; k < 4; k++) {
		float w = aW[k];
		if (w <= 0.0) {
			continue;
		}
		int b = int(aBone[k] + 0.5);
		vec4 r = bt(b, 0);
		vec4 d = bt(b, 1);
		float sg = dot(r, q0) < 0.0 ? -w : w;
		R += sg * r;
		D += sg * d;
		y += w * restAt(b, L[k]);
		T += w * frameAt(int(aCB[k] + 0.5), C[k]);
		tw += w;
	}
	float n = length(R);
	R /= n;
	D /= n;
	vec3 qv = R.yzw;
	vec3 t = 2.0 * cross(qv, y);
	vec3 p = y + R.x * t + cross(qv, t) +
		2.0 * (R.x * D.yzw - D.x * qv + cross(qv, D.yzw));
	T /= tw;
	// (Under a raised arm, his ribs where his trunk has them.)
	float kept = aMisc.z * (aMisc.w > 0.5 ? uRaised.y : uRaised.x);
	p += (T - p) * kept;
	vec3 tn = 2.0 * cross(qv, aN);
	vec3 nn = aN + R.x * tn + cross(qv, tn);
	xP = p;
	xT = T;
	xN = nn;
	gl_Position = vec4(0.0);
	gl_PointSize = 1.0;
}`;
// (Bent, not drawn: no pixels.)
const SKIN_FRAG = `#version 300 es
precision mediump float;
out vec4 oCol;
void main() {
	oCol = vec4(0.0);
}`;

// Each drawing of him, from the points as bent this frame (see SKIN_VERT).
const VERT = `#version 300 es
precision highp float;
precision highp int;
in vec3 aP;
in vec3 aT;
in vec3 aN;
in vec4 aFace;
// The kind of face it is in, how much it goes with his arm (and, for
// SKIN_VERT, how much it is kept to his trunk, and which arm).
in vec4 aMisc;
uniform mat4 uView;
uniform vec4 uAt;
// Drawn as: 0 himself, 1 the dark edge round him (his back faces, a pixel
// out), 2 his shadow cast over the floor, 3 his reflection in it.
uniform int uMode;
// His shadow: so far across the floor and toward the camera for each foot
// up he is.
uniform vec2 uCast;
// A pixel, in the picture's -1 to 1.
uniform vec2 uPx;
// How far up him a point is (off the floor under him, feet).
out float vUp;
out vec3 vP;
out vec3 vT;
out vec3 vN;
out vec4 vFace;
out float vArm;
flat out int vKind;
void main() {
	vec3 p = aP;
	vec3 nn = aN;
	vP = p;
	vT = aT;
	vN = nn;
	vFace = aFace;
	vArm = aMisc.y;
	vKind = int(aMisc.x + 0.5);
	vUp = p.z;
	// His frame on the floor: forward, his left, up.
	float c = cos(uAt.w);
	float s = sin(uAt.w);
	vec3 w = vec3(uAt.x + p.x * c + p.y * s, uAt.y + p.x * s - p.y * c, uAt.z + p.z);
	if (uMode == 2) {
		w = vec3(w.xy + uCast * w.z, 0.01);
	} else if (uMode == 3) {
		w.z = -w.z;
	}
	vec4 at = uView * vec4(w, 1.0);
	if (uMode == 1) {
		vec3 wn = vec3(nn.x * c + nn.y * s, nn.x * s - nn.y * c, nn.z);
		vec4 out1 = uView * vec4(w + wn * 0.05, 1.0);
		vec2 d = out1.xy / out1.w - at.xy / at.w;
		float l = length(d / uPx);
		if (l > 0.0) {
			at.xy += d / l * at.w;
		}
	}
	gl_Position = at;
}`;
const FRAG = `${PAINT_HEAD}${PAINT_COMMON}${MESH_PAINT}
in vec3 vP;
in vec3 vT;
in vec3 vN;
in vec4 vFace;
in float vArm;
flat in int vKind;
in float vUp;
// The camera's right, down and toward it, in his frame.
uniform mat3 uSee;
uniform int uMode;
// His edge's color (1); how dark his shadow (2, its a).
uniform vec4 uInk;
// His reflection: how strong, and how tall his picture is (feet).
uniform vec2 uShine;
out vec4 oCol;
void main() {
	if (uMode == 1) {
		oCol = vec4(uInk.rgb, 1.0);
		return;
	}
	if (uMode == 2) {
		oCol = vec4(0.0, 0.0, 0.0, uInk.a);
		return;
	}
	// (Upside down under him, his feet plain in it, his head hardly: in
	// steps, as today's picture of him is - see sprite.ts stamp.)
	float a = 1.0;
	if (uMode == 3) {
		float h = vUp / uShine.y;
		a = uShine.x * (h < ${SHINE_BANDS[0]![0]} ? 1.0 : h < ${SHINE_BANDS[0]![0] + SHINE_BANDS[1]![0]} ? ${SHINE_BANDS[1]![1]} : 0.0);
		if (a <= 0.0) {
			discard;
		}
	}
	meshPaint(vKind, vP, vT, vArm, vFace);
	vec3 col = meshLit(normalize(vN) * uSee);
	oCol = vec4(clamp(col / 255.0, 0.0, 1.0) * a, a);
}`;

// HIS HANDS, and what is in them, as the sprite's sculptor makes them (see
// sculptGl.ts SCULPT_AT): over a box of the picture round them, at its own
// pixels - each as near the camera as the sculptor has it there.
const HANDS_VERT = `#version 300 es
uniform vec4 uBox;
uniform vec2 uSize;
const vec2 C[6] = vec2[6](
	vec2(0.0, 0.0), vec2(1.0, 0.0), vec2(1.0, 1.0),
	vec2(0.0, 0.0), vec2(1.0, 1.0), vec2(0.0, 1.0)
);
void main() {
	vec2 px = mix(uBox.xy, uBox.zw, C[gl_VertexID]);
	gl_Position = vec4(px.x / uSize.x * 2.0 - 1.0, 1.0 - px.y / uSize.y * 2.0, 0.0, 1.0);
}`;
const HANDS_FRAG = `${PAINT_HEAD}
// The picture's size; how far from the camera a depth of 1 is, and a depth
// of 0 (see bowlMatrix).
uniform vec2 uSize;
out vec4 oCol;
${PAINT_COMMON}${SCULPT_AT}
void main() {
	vec2 p = vec2(gl_FragCoord.x, uSize.y - gl_FragCoord.y);
	vec3 c;
	float near;
	int g;
	float cover = sculptAt(p, c, near, g);
	if (cover <= 0.0) {
		discard;
	}
	float a = min(1.0, cover);
	oCol = vec4(clamp(c / 255.0, 0.0, 1.0) * a, a);
	gl_FragDepth = 0.5 * (${(BOWL_FAR + BOWL_NEAR) / (BOWL_FAR - BOWL_NEAR)}
		+ ${(-2 * BOWL_FAR * BOWL_NEAR) / (BOWL_FAR - BOWL_NEAR)} / max(near, 0.5)) + 0.5;
}`;

// The texture units the men read (the court's picture is on 0).
const UNITS = {
	uSheet: 1,
	uArt: 2,
	uPrint: 3,
	uFeat: 4,
	uHairA: 5,
	uBones: 6,
	uM: 7,
} as const;

export type MenGl = {
	// Each man's points bent as posed, once a frame (see menSkin)...
	skin: Prog;
	// ... and him drawn from them, each way he is drawn (see drawMan).
	prog: Prog;
	hands: Prog;
	// The points as bent, each man's in turn (rows of them for so many men,
	// made more of as wanted), and what writes them; for each man's place in
	// it, a vertex array that reads them.
	bent: WebGLBuffer;
	bentRows: number;
	feedback: WebGLTransformFeedback;
	drawn: WebGLVertexArrayObject[];
	// Each man's masses, a picture of numbers each (see massTable).
	tables: WebGLTexture[];
	// What is the same of every point whatever the length of his shorts...
	fixed: WebGLBuffer;
	index: WebGLBuffer;
	count: number;
	type: number;
	// ... and a vertex array for each length (see meshLocals).
	byHem: WeakMap<Float32Array, WebGLVertexArrayObject>;
	// Each point of the mesh once for each kind of face it is in: which.
	from: Uint32Array;
	bones: WebGLTexture;
	rows: Float32Array;
	blank: WebGLTexture;
	sheets: WeakMap<object, WebGLTexture>;
	arts: WeakMap<KitArt, WebGLTexture>;
	prints: WeakMap<PrintLevel, WebGLTexture>;
	faces: WeakMap<FaceTex, { feat: WebGLTexture; hair: WebGLTexture }>;
	// What is on each of the men's texture units, and the unit last made
	// active, as far as is known (see bind, menUnknown) - so nothing is sent
	// to the card twice.
	bound: (WebGLTexture | null)[];
	active: number;
};

// Each frame, and whenever something other than the men has had the card
// (its texture units may be anything; the active one is).
export const menUnknown = (g: MenGl, units: boolean) => {
	g.active = -1;
	if (units) {
		g.bound.fill(null);
	}
};

type Known = Pick<MenGl, "bound" | "active">;
const tex = (
	gl: WebGL2RenderingContext,
	g: Known,
	unit: number,
	filter: number,
) => {
	const t = gl.createTexture();
	bind(gl, g, unit, t, t);
	gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, filter);
	gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, filter);
	gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
	gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
	return t;
};

// The body onto the card: once, when the mesh is in (undefined till then).
export const menOnto = (gl: WebGL2RenderingContext): MenGl | undefined => {
	const m = meshData();
	if (!m) {
		return undefined;
	}
	const skin = compile(gl, SKIN_VERT, SKIN_FRAG, ["xP", "xT", "xN"]);
	const prog = compile(gl, VERT, FRAG);
	const hands = compile(gl, HANDS_VERT, HANDS_FRAG);
	if (!skin || !prog || !hands) {
		return undefined;
	}
	// Each point at rest (a man 1 foot tall), for its normal.
	const local = meshLocals();
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
	// A point once for each kind of face it is in (where his shorts meet his
	// thigh, twice): what of it is the same whatever he wears.
	const ROW = 19;
	const at = new Map<number, number>();
	const rows: number[] = [];
	const from: number[] = [];
	const index: number[] = [];
	const K = MESH_KINDS.length;
	for (let t = 0; t < tris.length / 3; t++) {
		const kind = m.kind[t]!;
		for (let c = 0; c < 3; c++) {
			const i = tris[t * 3 + c]!;
			const key = i * K + kind;
			let v = at.get(key);
			if (v === undefined) {
				v = at.size;
				at.set(key, v);
				from.push(i);
				const row = new Array<number>(ROW).fill(0);
				let k = 0;
				for (let j = m.start[i]!; j < m.start[i + 1]! && k < 4; j++, k++) {
					row[k] = m.bone[j]!;
					row[4 + k] = m.weight[j]!;
				}
				row[8] = nrm[i * 3]!;
				row[9] = nrm[i * 3 + 1]!;
				row[10] = nrm[i * 3 + 2]!;
				for (let j = 0; j < 4; j++) {
					row[11 + j] = m.face[i * 4 + j]! / (j < 2 ? 8 : 1000);
				}
				row[15] = kind;
				row[16] = m.arm[i]! / 255;
				row[17] = m.pit[i]!;
				row[18] = m.pitSide[i]!;
				rows.push(...row);
			}
			index.push(v);
		}
	}
	// (Its faces held by no vertex array till one is made - see arrayFor.)
	gl.bindVertexArray(null);
	const fixed = buffer(gl, new Float32Array(rows));
	const wide = at.size > 65535;
	const ix = buffer(
		gl,
		wide ? new Uint32Array(index) : new Uint16Array(index),
		gl.ELEMENT_ARRAY_BUFFER,
	);
	gl.bindBuffer(gl.ELEMENT_ARRAY_BUFFER, null);
	for (const pr of [skin, prog]) {
		gl.useProgram(pr.p);
		gl.uniform4fv(pr.u("uRest"), new Float32Array(R));
		for (const [name, unit] of Object.entries(UNITS)) {
			gl.uniform1i(pr.u(name), unit);
		}
	}
	gl.useProgram(hands.p);
	for (const [name, unit] of Object.entries(UNITS)) {
		gl.uniform1i(hands.u(name), unit);
	}
	// (Nothing known on the units yet.)
	const known = { bound: [] as (WebGLTexture | null)[], active: -1 };
	const bones = tex(gl, known, UNITS.uBones, gl.NEAREST);
	gl.texImage2D(
		gl.TEXTURE_2D,
		0,
		gl.RGBA32F,
		B * BONE,
		ROWS,
		0,
		gl.RGBA,
		gl.FLOAT,
		null,
	);
	const blank = tex(gl, known, UNITS.uM, gl.NEAREST);
	gl.texImage2D(
		gl.TEXTURE_2D,
		0,
		gl.RGBA8,
		1,
		1,
		0,
		gl.RGBA,
		gl.UNSIGNED_BYTE,
		new Uint8Array(4),
	);
	const bent = gl.createBuffer();
	return {
		skin,
		prog,
		hands,
		bent,
		bentRows: 0,
		feedback: gl.createTransformFeedback(),
		drawn: [],
		tables: [],
		fixed,
		index: ix,
		count: index.length,
		type: wide ? gl.UNSIGNED_INT : gl.UNSIGNED_SHORT,
		byHem: new WeakMap(),
		from: new Uint32Array(from),
		bones,
		rows: new Float32Array(B * BONE * ROWS * 4),
		blank,
		sheets: new WeakMap(),
		arts: new WeakMap(),
		prints: new WeakMap(),
		faces: new WeakMap(),
		bound: Object.values(UNITS)
			.map(() => null)
			.concat([null]),
		active: -1,
	};
};

// The vertex array for shorts this long (made the first time).
const arrayFor = (
	gl: WebGL2RenderingContext,
	g: MenGl,
	LOCAL: Float32Array,
): WebGLVertexArrayObject => {
	const had = g.byHem.get(LOCAL);
	if (had) {
		return had;
	}
	const m = meshData()!;
	const trunk = meshTrunkOf(LOCAL);
	const ROW = 28;
	const n = g.from.length;
	const data = new Float32Array(n * ROW);
	for (let v = 0; v < n; v++) {
		const i = g.from[v]!;
		let k = 0;
		for (let j = m.start[i]!; j < m.start[i + 1]! && k < 4; j++, k++) {
			for (let c = 0; c < 3; c++) {
				data[v * ROW + k * 3 + c] = LOCAL[j * 3 + c]!;
				data[v * ROW + 12 + k * 3 + c] = trunk.local[j * 3 + c]!;
			}
			data[v * ROW + 24 + k] = trunk.bone[j]!;
		}
	}
	const vao = gl.createVertexArray();
	gl.bindVertexArray(vao);
	const prog = g.skin;
	const S = 19 * 4;
	attrib(gl, prog, "aBone", g.fixed, 4, S, 0);
	attrib(gl, prog, "aW", g.fixed, 4, S, 16);
	attrib(gl, prog, "aN", g.fixed, 3, S, 32);
	attrib(gl, prog, "aFace", g.fixed, 4, S, 44);
	attrib(gl, prog, "aMisc", g.fixed, 4, S, 60);
	const held = buffer(gl, data);
	const S2 = ROW * 4;
	for (let k = 0; k < 4; k++) {
		attrib(gl, prog, `aL${k}`, held, 3, S2, k * 12);
		attrib(gl, prog, `aC${k}`, held, 3, S2, 48 + k * 12);
	}
	attrib(gl, prog, "aCB", held, 4, S2, 96);
	gl.bindBuffer(gl.ELEMENT_ARRAY_BUFFER, g.index);
	gl.bindVertexArray(null);
	g.byHem.set(LOCAL, vao);
	return vao;
};

// THE MEN'S POINTS BENT AS POSED, ONCE A FRAME (see SKIN_VERT): each the
// sum of his pieces' turns and moves, a few texture reads each, worked out
// once and kept on the card for every drawing of him after (his
// reflection, his two shadows, his edge, him) - not again for each. After
// menRows; the `i`th of `men` into the `i`th place.
const BENT = 9 * 4;
export const menSkin = (
	gl: WebGL2RenderingContext,
	g: MenGl,
	men: readonly ManReady[],
) => {
	const n = Math.min(ROWS, men.length);
	if (n === 0) {
		return;
	}
	const V = g.from.length;
	if (n > g.bentRows) {
		// (Room for more of them: a new place, so new vertex arrays for it.)
		g.bentRows = Math.min(ROWS, Math.max(n, 2 * g.bentRows, 16));
		gl.bindBuffer(gl.ARRAY_BUFFER, g.bent);
		gl.bufferData(gl.ARRAY_BUFFER, g.bentRows * V * BENT, gl.DYNAMIC_COPY);
		gl.bindBuffer(gl.ARRAY_BUFFER, null);
		for (const vao of g.drawn) {
			gl.deleteVertexArray(vao);
		}
		g.drawn = [];
	}
	gl.useProgram(g.skin.p);
	const { u } = g.skin;
	bind(gl, g, UNITS.uBones, g.bones, g.blank);
	gl.enable(gl.RASTERIZER_DISCARD);
	gl.bindTransformFeedback(gl.TRANSFORM_FEEDBACK, g.feedback);
	for (let i = 0; i < n; i++) {
		const man = men[i]!;
		gl.bindVertexArray(arrayFor(gl, g, man.local));
		gl.uniform1i(u("uRow"), i);
		gl.uniform1f(u("uH"), man.body.H);
		gl.uniform2f(u("uRaised"), man.raised[0], man.raised[1]);
		gl.bindBufferRange(
			gl.TRANSFORM_FEEDBACK_BUFFER,
			0,
			g.bent,
			i * V * BENT,
			V * BENT,
		);
		gl.beginTransformFeedback(gl.POINTS);
		gl.drawArrays(gl.POINTS, 0, V);
		gl.endTransformFeedback();
	}
	gl.bindBufferBase(gl.TRANSFORM_FEEDBACK_BUFFER, 0, null);
	gl.bindTransformFeedback(gl.TRANSFORM_FEEDBACK, null);
	gl.bindBuffer(gl.TRANSFORM_FEEDBACK_BUFFER, null);
	gl.disable(gl.RASTERIZER_DISCARD);
	gl.bindVertexArray(null);
};

// The vertex array for the `i`th man's place among the points as bent
// (made the first time).
const drawnAt = (
	gl: WebGL2RenderingContext,
	g: MenGl,
	i: number,
): WebGLVertexArrayObject => {
	let vao = g.drawn[i];
	if (!vao) {
		vao = gl.createVertexArray();
		gl.bindVertexArray(vao);
		const { prog } = g;
		const S = 19 * 4;
		attrib(gl, prog, "aFace", g.fixed, 4, S, 44);
		attrib(gl, prog, "aMisc", g.fixed, 4, S, 60);
		const at = i * g.from.length * BENT;
		attrib(gl, prog, "aP", g.bent, 3, BENT, at);
		attrib(gl, prog, "aT", g.bent, 3, BENT, at + 12);
		attrib(gl, prog, "aN", g.bent, 3, BENT, at + 24);
		gl.bindBuffer(gl.ELEMENT_ARRAY_BUFFER, g.index);
		gl.bindVertexArray(null);
		// (Not left bound where the bending would find it.)
		gl.bindBuffer(gl.ARRAY_BUFFER, null);
		g.drawn[i] = vao;
	}
	return vao;
};

// Each man, ready for the card: worked out for all of them before any is
// drawn (so the readout can tell the two apart - see meter.ts).
export type ManReady = {
	st: PlayerState;
	body: Body;
	look: Look;
	b: Built;
	// What his paint is told (see PAINT_COMMON, MESH_PAINT), and what from.
	params: Float32Array;
	twists: Float32Array;
	mk: Float32Array;
	foot: Float32Array;
	see: Float32Array;
	raised: [number, number];
	local: Float32Array;
	sheet?: HTMLCanvasElement;
	art?: KitArt;
	print?: PrintLevel;
	// His hands' masses (see massTable): how many, and the box round them
	// on the picture (none: x1 < x0).
	table: Float32Array;
	masses: number;
	box: { x0: number; y0: number; x1: number; y1: number };
	// The radius of the ball in his hands (0: none).
	ball: number;
	// How far from the camera he is (for the nearest first).
	depth: number;
};
export const readyMan = (
	cam: Camera,
	st: PlayerState,
	body: Body,
	look: Look,
	into: ManReady | undefined,
): ManReady => {
	// His masses as the sprite's sculptor has them, at the picture's own
	// pixels (his hands, later), and his mesh's rig.
	const b = build(cam, st, body, look, 1, 0, 0, true, MESH_KEEPS);
	const hem = hemOf(look);
	const rig = meshRig(b, st, body, hem);
	const { mk, foot } = meshPaintOf(rig, body);
	const t = massTable(b, true);
	const p = paintParams(
		cam,
		st,
		body,
		look,
		b,
		false,
		t?.lineW ?? 0,
		t?.margin ?? 0,
		t?.ball ?? 0,
	);
	const r: ManReady = into ?? {
		st,
		body,
		look,
		b,
		params: new Float32Array(p.params.length),
		twists: new Float32Array(p.twists.length),
		mk: new Float32Array(mk.length),
		foot: new Float32Array(foot.length),
		see: new Float32Array(9),
		raised: [0, 0],
		local: meshLocals(),
		table: new Float32Array(MASS_TABLE.w * MASS_TABLE.h * 4),
		masses: 0,
		box: { x0: 0, y0: 0, x1: -1, y1: -1 },
		ball: 0,
		depth: 0,
	};
	r.st = st;
	r.body = body;
	r.look = look;
	r.b = b;
	r.params.set(p.params);
	r.twists.set(p.twists);
	r.mk.set(mk);
	r.foot.set(foot);
	const { rt, dn } = b.frame;
	const tw = rig.toward;
	r.see.set([rt.f, rt.s, rt.u, dn.f, dn.s, dn.u, tw.f, tw.s, tw.u]);
	r.raised = meshRaised(rig.posed);
	r.local = meshLocals(hem, shortsFlareFor(hem));
	r.sheet = p.sheet?.big;
	r.art = p.art;
	r.print = p.print;
	r.masses = t?.n ?? 0;
	r.ball = t?.ball ?? 0;
	if (t && t.n > 0) {
		r.table.set(t.table);
		r.box = {
			x0: Math.floor(t.x0 - t.margin),
			y0: Math.floor(t.y0 - t.margin),
			x1: Math.ceil(t.x1 + t.margin),
			y1: Math.ceil(t.y1 + t.margin),
		};
	} else {
		r.box = { x0: 0, y0: 0, x1: -1, y1: -1 };
	}
	r.depth = b.headDepth;
	// His pieces as posed, a row of numbers (see BONE): `bones`, on the card
	// with the rest of the men's (see menRows).
	rowOf(r, meshTurns(rig.posed, body.H), rig.posed);
	return r;
};
// (Each man's row, filled as he is made ready: see menRows.)
const rowsOf = new WeakMap<ManReady, Float32Array>();
const rowOf = (
	r: ManReady,
	dq: Float64Array,
	posed: ReturnType<typeof meshRig>["posed"],
) => {
	let o = rowsOf.get(r);
	if (!o) {
		o = new Float32Array(B * BONE * 4);
		rowsOf.set(r, o);
	}
	for (let i = 0; i < B; i++) {
		const t = i * BONE * 4;
		for (let k = 0; k < 8; k++) {
			o[t + k] = dq[i * 8 + k]!;
		}
		const { fr, ax, rad } = posed[i]!;
		o.set([fr.h.f, fr.h.s, fr.h.u, ax], t + 8);
		o.set([fr.a.f, fr.a.s, fr.a.u, rad], t + 12);
		o.set([fr.f.f, fr.f.s, fr.f.u, 0], t + 16);
		o.set([fr.s.f, fr.s.s, fr.s.u, 0], t + 20);
	}
};

// All the men's pieces onto the card, a row each (the order they are in).
export const menRows = (
	gl: WebGL2RenderingContext,
	g: MenGl,
	men: readonly ManReady[],
) => {
	const n = Math.min(ROWS, men.length);
	const W = B * BONE * 4;
	for (let i = 0; i < n; i++) {
		const o = rowsOf.get(men[i]!);
		if (o) {
			g.rows.set(o, i * W);
		}
	}
	if (n === 0) {
		return;
	}
	bind(gl, g, UNITS.uBones, g.bones, g.blank);
	gl.texSubImage2D(
		gl.TEXTURE_2D,
		0,
		0,
		0,
		B * BONE,
		n,
		gl.RGBA,
		gl.FLOAT,
		g.rows,
	);
};

// Before the men: the camera, the same for all of them.
export const menCamera = (
	gl: WebGL2RenderingContext,
	g: MenGl,
	m: Float32Array,
) => {
	gl.useProgram(g.prog.p);
	gl.uniformMatrix4fv(g.prog.u("uView"), false, m);
};

// What he is painted from, onto the card (each the first time).
const sheetTex = (gl: WebGL2RenderingContext, g: MenGl, sheet: object) => {
	let t = g.sheets.get(sheet);
	if (!t) {
		t = tex(gl, g, UNITS.uSheet, gl.LINEAR);
		gl.texParameteri(
			gl.TEXTURE_2D,
			gl.TEXTURE_MIN_FILTER,
			gl.LINEAR_MIPMAP_LINEAR,
		);
		gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.REPEAT);
		gl.pixelStorei(gl.UNPACK_PREMULTIPLY_ALPHA_WEBGL, true);
		gl.texImage2D(
			gl.TEXTURE_2D,
			0,
			gl.RGBA,
			gl.RGBA,
			gl.UNSIGNED_BYTE,
			sheet as HTMLCanvasElement,
		);
		gl.pixelStorei(gl.UNPACK_PREMULTIPLY_ALPHA_WEBGL, false);
		gl.generateMipmap(gl.TEXTURE_2D);
		g.sheets.set(sheet, t);
	}
	return t;
};
const artTex = (gl: WebGL2RenderingContext, g: MenGl, art: KitArt) => {
	let t = g.arts.get(art);
	if (!t) {
		const aw = ART_W * art.scale;
		t = tex(gl, g, UNITS.uArt, gl.NEAREST);
		gl.texImage2D(
			gl.TEXTURE_2D,
			0,
			gl.RGBA,
			aw,
			Math.floor(art.data.length / 4 / aw),
			0,
			gl.RGBA,
			gl.UNSIGNED_BYTE,
			new Uint8Array(art.data.buffer, art.data.byteOffset, art.data.length),
		);
		g.arts.set(art, t);
	}
	return t;
};
const printTex = (gl: WebGL2RenderingContext, g: MenGl, print: PrintLevel) => {
	let t = g.prints.get(print);
	if (!t) {
		const { n, all, ink } = print;
		const both = new Uint8Array(n * n * 2);
		for (let i = 0; i < n * n; i++) {
			both[i * 2] = all[i]!;
			both[i * 2 + 1] = ink[i]!;
		}
		t = tex(gl, g, UNITS.uPrint, gl.NEAREST);
		gl.texImage2D(
			gl.TEXTURE_2D,
			0,
			gl.RG8,
			n,
			n,
			0,
			gl.RG,
			gl.UNSIGNED_BYTE,
			both,
		);
		g.prints.set(print, t);
	}
	return t;
};
const faceTex = (gl: WebGL2RenderingContext, g: MenGl, ft: FaceTex) => {
	let t = g.faces.get(ft);
	if (!t) {
		const feat = tex(gl, g, UNITS.uFeat, gl.NEAREST);
		gl.texImage2D(
			gl.TEXTURE_2D,
			0,
			gl.RGBA8,
			ft.w,
			ft.h,
			0,
			gl.RGBA,
			gl.UNSIGNED_BYTE,
			new Uint8Array(ft.feat.buffer, ft.feat.byteOffset, ft.w * ft.h * 4),
		);
		const hair = tex(gl, g, UNITS.uHairA, gl.NEAREST);
		gl.texImage2D(
			gl.TEXTURE_2D,
			0,
			gl.R8,
			ft.w,
			ft.h,
			0,
			gl.RED,
			gl.UNSIGNED_BYTE,
			new Uint8Array(ft.hair.buffer, ft.hair.byteOffset, ft.w * ft.h),
		);
		t = { feat, hair };
		g.faces.set(ft, t);
	}
	return t;
};
function bind(
	gl: WebGL2RenderingContext,
	g: Known,
	unit: number,
	t: WebGLTexture | undefined,
	blank: WebGLTexture,
) {
	const want = t ?? blank;
	if (g.bound[unit] === want) {
		return;
	}
	if (g.active !== unit) {
		gl.activeTexture(gl.TEXTURE0 + unit);
		g.active = unit;
	}
	gl.bindTexture(gl.TEXTURE_2D, want);
	g.bound[unit] = want;
}

// How one of him is drawn (see VERT uMode): himself, the dark edge round
// him, a shadow he casts (so far across the floor and toward the camera
// for each foot up, so dark), his reflection in the floor (so strong).
export type Pass =
	| { mode: 0 }
	| { mode: 1 }
	| { mode: 2; cast: [number, number]; alpha: number }
	| { mode: 3; shine: number };
const EDGE = OUTLINE.map((c) => c / 255);

// One man: the `i`th of those given menRows (`W` x `H` the picture).
export const drawMan = (
	gl: WebGL2RenderingContext,
	g: MenGl,
	man: ManReady,
	i: number,
	pass: Pass,
	W: number,
	H: number,
) => {
	if (i >= ROWS) {
		return;
	}
	const { st, body, look } = man;
	const { u } = g.prog;
	gl.bindVertexArray(drawnAt(gl, g, i));
	gl.uniform4f(u("uAt"), st.x, st.y, st.z, st.yaw);
	gl.uniform1i(u("uMode"), pass.mode);
	if (pass.mode === 1) {
		gl.uniform2f(u("uPx"), 2 / W, 2 / H);
		gl.uniform4f(u("uInk"), EDGE[0]!, EDGE[1]!, EDGE[2]!, 1);
	} else if (pass.mode === 2) {
		gl.uniform2f(u("uCast"), pass.cast[0], pass.cast[1]);
		gl.uniform4f(u("uInk"), 0, 0, 0, pass.alpha);
	} else {
		if (pass.mode === 3) {
			// (His picture today is as tall as all of him drawn.)
			gl.uniform2f(u("uShine"), pass.shine, body.H * 1.05);
		}
		const ft = look.faceTex ?? look.head?.tex;
		const face = ft ? faceTex(gl, g, ft) : undefined;
		bind(gl, g, UNITS.uSheet, man.sheet && sheetTex(gl, g, man.sheet), g.blank);
		bind(gl, g, UNITS.uArt, man.art && artTex(gl, g, man.art), g.blank);
		bind(gl, g, UNITS.uPrint, man.print && printTex(gl, g, man.print), g.blank);
		bind(gl, g, UNITS.uFeat, face?.feat, g.blank);
		bind(gl, g, UNITS.uHairA, face?.hair, g.blank);
		bind(gl, g, UNITS.uM, g.blank, g.blank);
		gl.uniformMatrix3fv(u("uSee"), false, man.see);
		gl.uniform4fv(u("uP"), man.params);
		gl.uniform3fv(u("uT"), man.twists);
		gl.uniform4fv(u("uMK"), man.mk);
		gl.uniform4fv(u("uFoot"), man.foot);
		const f = meshFaceOf(look, face !== undefined);
		gl.uniform4fv(u("uTex0"), f.tex0);
		gl.uniform4fv(u("uTex1"), f.tex1);
		gl.uniform4fv(u("uHair"), f.hair);
		gl.uniform4fv(u("uOpt"), f.opt);
	}
	gl.drawElements(gl.TRIANGLES, g.count, g.type, 0);
};

// His hands, and what is in them: after all the men's bodies, the `i`th's
// All their hands, over all of them (blended in at their soft edges).
export const drawAllHands = (
	gl: WebGL2RenderingContext,
	g: MenGl,
	men: readonly ManReady[],
	W: number,
	H: number,
) => {
	gl.useProgram(g.hands.p);
	men.forEach((man, i) => drawHands(gl, g, man, i, W, H));
};

// (blended in at their soft edges). `W` x `H`: the picture.
const drawHands = (
	gl: WebGL2RenderingContext,
	g: MenGl,
	man: ManReady,
	i: number,
	W: number,
	H: number,
) => {
	const { box } = man;
	if (man.masses <= 0 || box.x1 < box.x0 || box.y1 < box.y0) {
		return;
	}
	let t = g.tables[i];
	if (!t) {
		t = tex(gl, g, UNITS.uM, gl.NEAREST);
		gl.texImage2D(
			gl.TEXTURE_2D,
			0,
			gl.RGBA32F,
			MASS_TABLE.w,
			MASS_TABLE.h,
			0,
			gl.RGBA,
			gl.FLOAT,
			null,
		);
		g.tables[i] = t;
	}
	bind(gl, g, UNITS.uM, t, t);
	gl.texSubImage2D(
		gl.TEXTURE_2D,
		0,
		0,
		0,
		MASS_TABLE.w,
		MASS_TABLE.h,
		gl.RGBA,
		gl.FLOAT,
		man.table,
	);
	bind(gl, g, UNITS.uPrint, man.print && printTex(gl, g, man.print), g.blank);
	const { u } = g.hands;
	gl.uniform4f(u("uBox"), box.x0, box.y0, box.x1 + 1, box.y1 + 1);
	gl.uniform2f(u("uSize"), W, H);
	gl.uniform4fv(u("uP"), man.params);
	gl.uniform3fv(u("uT"), man.twists);
	gl.uniform1i(u("uN"), man.masses);
	gl.drawArrays(gl.TRIANGLES, 0, 6);
};
