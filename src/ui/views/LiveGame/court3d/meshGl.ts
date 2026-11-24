import type { Camera } from "./camera.ts";
import type { PlayerState } from "./evaluate.ts";
import type { FaceTex } from "./faces.ts";
import type { Look } from "./figure.ts";
import {
	MESH_KINDS,
	MESH_LIGHTS,
	MESH_SS,
	meshData,
	meshPoints,
	restOf,
	type Mesh,
} from "./meshBody.ts";
import type { Body } from "./poses.ts";
import {
	type Built,
	hemOf,
	lensOf,
	MESH_CUTS,
	MESH_HIDDEN,
	MESH_LAYERED,
	MESH_LIFT,
	MESH_PAINTED,
	MESH_SHELLS,
	meshRig,
	NECK,
	rgb,
	THIGH,
	TORSO,
	UPPER,
} from "./sculpt.ts";
import { shortsFlareFor } from "./shortsEra.ts";

// HIS BODY FROM THE MESH OF A MAN, ON THE GRAPHICS CARD - what drawMesh
// (meshBody.ts) makes on the page, made there instead (see sculptGl.ts,
// whose card it is drawn on), the same way:
//
// - Where each point of him is as posed, and his normal there, are worked
//   out on the page (see meshPoints: a few thousand points, quick) and
//   handed to the card as a small picture of numbers, with the faces of
//   him turned toward the camera.
// - The card lays those faces down at SS x SS the sprite's pixels, nearest
//   the camera winning: just which face is there, and where on it.
// - Then, once for each of the sprite's own pixels, the nearest of its
//   SS x SS is painted and lit as drawMesh paints and lights it - what he
//   wears there (the same paint as his masses, see sculptGl.ts), his face
//   and hair where his neck is, the mesh's own light - with how many of
//   them he covers, how near he is there, and which part of him it is.
//
// The sculptor's own pass then lays his hands over him where they are
// nearer (see sculptGl.ts).

// ---- what each kind of his faces is ------------------------------------------

const K = MESH_KINDS.length;
const kindIx = (name: string) => MESH_KINDS.indexOf(name);
// (As sculpt.ts has them: see massAt there.)
const TORSO_K = Math.max(0, kindIx("torso"));
const UPPER_R_K = kindIx("upperR") >= 0 ? kindIx("upperR") : TORSO_K;
const UPPER_L_K = kindIx("upperL") >= 0 ? kindIx("upperL") : TORSO_K;
const SHORTS_R_K = kindIx("shortsR");
const SHORTS_L_K = kindIx("shortsL");
const THIGH_R_K = kindIx("thighR");
const THIGH_L_K = kindIx("thighL");
const HEAD_K = kindIx("head");
const NECK_K = kindIx("neck");
const SHELL_CODE = {
	"": 0,
	shorts: 1,
	shortsTop: 2,
	shoe: 3,
	foot: 4,
	jersey: 5,
	head: 6,
} as const;
const SHELLS = MESH_SHELLS.map(
	(s) => SHELL_CODE[s as keyof typeof SHELL_CODE] ?? 0,
);
const SIDES = MESH_KINDS.map((n) => (n.endsWith("L") ? 1 : 0));
// The masses he is painted as: one for each kind of face, then his shorts
// down each kind (for his thighs), then his shoes as they stand at rest.
const N_MASSES = K * 2 + 2;
const SHORTS_AT = K;
const SHOES_AT = K * 2;

// Whether the card can draw him: not if the mesh cuts any of him away
// where he wears something over him (see sculpt.ts keep) - his body drawn
// on the page then.
export const meshGlOk = (): boolean =>
	meshData() !== undefined && !MESH_CUTS.some(Boolean);

const f1 = (x: number) => (Number.isInteger(x) ? `${x}.0` : `${x}`);
const v3 = (c: readonly number[]) => `vec3(${c.map(f1).join(", ")})`;
const ints = (a: readonly number[]) =>
	`int[${Math.max(1, a.length)}](${(a.length ? a : [0]).join(", ")})`;

// ---- on the card ---------------------------------------------------------------

// The pictures of numbers: a texel (or a few) a point, or a face, ROW of
// them a row.
const ROW = 1024;
const ROW_BITS = Math.log2(ROW);
// Each point as posed: where it is on the sprite (at SS x SS its pixels,
// and how far from the camera), where it is on him, where his trunk alone
// carries it - with his normal there in their w's.
const PER_POINT = 3;
// Each point as the mesh has it: where on his face it is (see bake.py); how
// much it goes with his arm.
const PER_REST = 2;

// The faces turned toward the camera, each corner a vertex of its own (so
// a face knows which of its corners each is): just which face is there,
// and where on it.
export const MESH_VERT = `#version 300 es
precision highp float;
precision highp int;
precision highp sampler2D;
uniform sampler2D uV;
// The sheet's size (SS x SS the sprite's), the nearest and furthest of him.
uniform vec4 uBox;
uniform float uLift[${K}];
layout(location = 0) in uint aV;
layout(location = 1) in uint aKind;
flat out uint vTri;
out vec2 vAt;
void main() {
	int v = int(aV);
	int corner = gl_VertexID % 3;
	vTri = uint(gl_VertexID / 3);
	vAt = corner == 0 ? vec2(1.0, 0.0) : corner == 1 ? vec2(0.0, 1.0) : vec2(0.0);
	int i = v * ${PER_POINT};
	vec4 s0 = texelFetch(uV, ivec2(i & ${ROW - 1}, i >> ${ROW_BITS}), 0);
	float z = s0.z - uLift[int(aKind)];
	gl_Position = vec4(
		s0.x / uBox.x * 2.0 - 1.0,
		s0.y / uBox.y * 2.0 - 1.0,
		clamp((z - uBox.z) / uBox.w, 0.0, 1.0) * 2.0 - 1.0,
		1.0
	);
}`;
export const MESH_LAY = `#version 300 es
precision highp float;
precision highp int;
flat in uint vTri;
in vec2 vAt;
out uvec4 o;
void main() {
	o = uvec4(vTri, floatBitsToUint(vAt.x), floatBitsToUint(vAt.y), 1u);
}`;

// (After the sculptor's head and shared paint - see sculptGl.ts.)
export const MESH_MAIN = `
precision highp usampler2D;
uniform sampler2D uV;
uniform sampler2D uRest;
uniform usampler2D uTri;
uniform usampler2D uI;
uniform sampler2D uD;
// What each kind of his faces is painted as: per mass its ends, its kind
// (and side, 16 a side) and its group.
uniform vec4 uMK[${N_MASSES * 2}];
uniform vec4 uFoot[16];
uniform sampler2D uFeat;
uniform sampler2D uHairA;
// The face's picture: its x0, y0, scale, width; its height, his hair at
// the side and in front, his cut (1 bald, 2 long, 3 big); his hair's color
// and whether there is a picture; his trousers.
uniform vec4 uTex0;
uniform vec4 uTex1;
uniform vec4 uHair;
uniform vec4 uOpt;
// Where 0 and 1 of the depths laid down are (see uBox above).
uniform vec2 uZ;
layout(location = 0) out vec4 oMesh;
layout(location = 1) out uvec4 oZ;

const int SHELL[${K}] = ${ints(SHELLS)};
const int SIDE[${K}] = ${ints(SIDES)};
const vec3 MLIGHT = ${v3(MESH_LIGHTS.LIGHT)};
const vec3 MBACK = ${v3(MESH_LIGHTS.BACK_LIGHT)};
const vec3 MSHINE = ${v3(MESH_LIGHTS.SHINE_AT)};

float along;
vec3 mA(int i) { return uMK[i * 2].xyz; }
vec3 mB(int i) { return uMK[i * 2 + 1].xyz; }
int mKind(int i) { return int(uMK[i * 2].w + 0.5) & 15; }
float mSide(int i) { return float(int(uMK[i * 2].w + 0.5) >> 4); }
float raw(int i, vec3 P) {
	vec3 l = mB(i) - mA(i);
	float l2 = dot(l, l);
	return dot(P - mA(i), l) / (l2 == 0.0 ? 1.0 : l2);
}
// What a point of him is painted as (see sculpt.ts massAt), and how far
// along it (into along).
int massAt(int k, vec3 P, float arm) {
	int mi = k;
	if (SHELL[k] == 2 && ${SHORTS_R_K} >= 0 && ${SHORTS_L_K} >= 0) {
		float U = (P.x - uP[PEL].x) * uP[SPINE].x + (P.z - uP[PEL].z) * uP[SPINE].z;
		if (U < uP[CUT0].x) {
			mi = P.y > 0.0 ? ${SHORTS_L_K} : ${SHORTS_R_K};
			along = raw(mi, P);
			return mi;
		}
	}
	int mk = mKind(mi);
	if (${MESH_PAINTED} && (mk == ${UPPER} || mk == ${TORSO} || mk == ${NECK}) && k != ${HEAD_K}) {
		mi = arm < 0.5 ? ${TORSO_K} : P.y > 0.0 ? ${UPPER_L_K} : ${UPPER_R_K};
	}
	if (
		${MESH_PAINTED} && SHELL[k] == 1 && uOpt.x < 0.5
		&& ${THIGH_R_K} >= 0 && ${THIGH_L_K} >= 0 && raw(mi, P) > 1.0
	) {
		mi = mSide(mi) > 0.5 ? ${THIGH_L_K} : ${THIGH_R_K};
	}
	if (${MESH_PAINTED} && mKind(mi) == ${THIGH} && uOpt.x < 0.5) {
		int sh = ${SHORTS_AT} + k;
		float r = raw(sh, P);
		if (r <= 1.0) {
			along = r;
			return sh;
		}
	}
	along = raw(mi, P);
	return mi;
}

// His face and hair on his neck (see sculpt.ts faceOn).
vec4 texRGBA;
float sampleFeat(float fx, float fy) {
	float px = (fx - uTex0.x) * uTex0.z - 0.5;
	float py = (fy - uTex0.y) * uTex0.z - 0.5;
	float x0 = floor(px);
	float y0 = floor(py);
	float ax = px - x0;
	float ay = py - y0;
	texRGBA = vec4(0.0);
	for (int j = 0; j < 2; j++) {
		for (int i = 0; i < 2; i++) {
			float x = x0 + float(i);
			float y = y0 + float(j);
			if (x < 0.0 || y < 0.0 || x >= uTex0.w || y >= uTex1.x) {
				continue;
			}
			float wt = (i == 1 ? ax : 1.0 - ax) * (j == 1 ? ay : 1.0 - ay);
			vec4 c = texelFetch(uFeat, ivec2(int(x), int(y)), 0) * 255.0;
			float al = (c.a / 255.0) * wt;
			texRGBA += vec4(c.rgb * al, al);
		}
	}
	return texRGBA.a;
}
float sampleHair(float fx, float fy) {
	float px = (fx - uTex0.x) * uTex0.z - 0.5;
	float py = (fy - uTex0.y) * uTex0.z - 0.5;
	float x0 = floor(px);
	float y0 = floor(py);
	float ax = px - x0;
	float ay = py - y0;
	float a = 0.0;
	for (int j = 0; j < 2; j++) {
		for (int i = 0; i < 2; i++) {
			float x = x0 + float(i);
			float y = y0 + float(j);
			if (x >= 0.0 && y >= 0.0 && x < uTex0.w && y < uTex1.x) {
				a += texelFetch(uHairA, ivec2(int(x), int(y)), 0).r
					* (i == 1 ? ax : 1.0 - ax) * (j == 1 ? ay : 1.0 - ay);
			}
		}
	}
	return a;
}
float smooth3(float a, float z, float x) {
	float q = clamp((x - a) / (z - a), 0.0, 1.0);
	return q * q * (3.0 - 2.0 * q);
}
void over(vec3 c, float a) { ink += (c - ink) * a; }
void faceOn(vec4 at) {
	float fx = at.x;
	float fy = at.y;
	float front = smooth3(0.25, 0.6, at.z);
	if (front > 0.0) {
		float a = sampleFeat(fx, fy);
		if (a > 0.01) {
			over(texRGBA.rgb / a, min(1.0, a) * front);
		}
	}
	int cutOf = int(uTex1.w + 0.5);
	if (cutOf == 1) {
		return;
	}
	float hairA = 0.0;
	if (front > 0.0) {
		hairA = min(1.0, sampleHair(fx, fy)) * front;
	}
	if (front < 1.0) {
		float side = uTex1.y > 0.0 ? min(300.0, uTex1.y) : 290.0;
		float fore = uTex1.z > 0.0 ? uTex1.z : 170.0;
		float back = cutOf == 2 ? 640.0 : cutOf == 3 ? 485.0 : 455.0;
		float ct = at.w;
		float line = ct >= 0.0
			? side + (fore - side) * ct
			: side + (back - side) * min(1.0, -ct * 1.25);
		hairA += (1.0 - front) * smooth3(line + 6.0, line - 6.0, fy);
	}
	if (hairA > 0.01) {
		over(uHair.rgb, min(1.0, hairA));
		if (hairA > 0.5) {
			cloth = true;
		}
	}
}

vec4 pt(int v, int j) {
	int i = v * ${PER_POINT} + j;
	return texelFetch(uV, ivec2(i & ${ROW - 1}, i >> ${ROW_BITS}), 0);
}
vec4 rest(int v, int j) {
	int i = v * ${PER_REST} + j;
	return texelFetch(uRest, ivec2(i & ${ROW - 1}, i >> ${ROW_BITS}), 0);
}

void main() {
	// The nearest of the pixel's SS x SS of him, and how many he covers.
	ivec2 q = ivec2(gl_FragCoord.xy) * ${MESH_SS};
	float near = 2.0;
	ivec2 at = q;
	int cov = 0;
	for (int j = 0; j < ${MESH_SS}; j++) {
		for (int i = 0; i < ${MESH_SS}; i++) {
			ivec2 s = q + ivec2(i, j);
			float d = texelFetch(uD, s, 0).r;
			if (d < 1.0) {
				cov += 1;
				if (d < near) {
					near = d;
					at = s;
				}
			}
		}
	}
	if (cov == 0) {
		discard;
	}
	// Which face of him is there, and where on it.
	uvec4 id = texelFetch(uI, at, 0);
	int t = int(id.x);
	float w0 = uintBitsToFloat(id.y);
	float w1 = uintBitsToFloat(id.z);
	float w2 = 1.0 - w0 - w1;
	uvec4 tri = texelFetch(uTri, ivec2(t & ${ROW - 1}, t >> ${ROW_BITS}), 0);
	int a = int(tri.x);
	int b = int(tri.y);
	int c = int(tri.z);
	int k = int(tri.w);
	vec4 a0 = pt(a, 0);
	vec4 a1 = pt(a, 1);
	vec4 a2 = pt(a, 2);
	vec4 b0 = pt(b, 0);
	vec4 b1 = pt(b, 1);
	vec4 b2 = pt(b, 2);
	vec4 c0 = pt(c, 0);
	vec4 c1 = pt(c, 1);
	vec4 c2 = pt(c, 2);
	vec3 P = w0 * a1.xyz + w1 * b1.xyz + w2 * c1.xyz;
	vec3 T = w0 * a2.xyz + w1 * b2.xyz + w2 * c2.xyz;
	vec3 N = w0 * vec3(a0.w, a1.w, a2.w)
		+ w1 * vec3(b0.w, b1.w, b2.w)
		+ w2 * vec3(c0.w, c1.w, c2.w);
	float arm = w0 * rest(a, 1).x + w1 * rest(b, 1).x + w2 * rest(c, 1).x;

	along = 0.0;
	int mi;
	if (!${MESH_LAYERED} && SHELL[k] == 3) {
		// His shoe, painted where each point is on it as it stands at rest.
		int o = SIDE[k] * 8;
		vec3 d = P - uFoot[o].xyz;
		P = uFoot[o + 4].xyz
			+ uFoot[o + 5].xyz * dot(d, uFoot[o + 1].xyz)
			+ uFoot[o + 6].xyz * dot(d, uFoot[o + 2].xyz)
			+ uFoot[o + 7].xyz * dot(d, uFoot[o + 3].xyz);
		mi = ${SHOES_AT} + SIDE[k];
		along = raw(mi, P);
	} else {
		mi = massAt(k, P, arm);
		// (His kit on his torso painted where it is on his torso.)
		if (mi == ${TORSO_K}) {
			mi = massAt(k, T, arm);
			P = T;
		}
	}
	cloth = false;
	tone = 0.0;
	Hit h = Hit(0.0, 0.0, 0.0, 0.0, 1.0, clamp(along, 0.0, 1.0), 1.0);
	float group = uMK[mi * 2 + 1].w;
	paint(
		vec4(1.0, group, float(mKind(mi)), mSide(mi)),
		h,
		P,
		mA(mi),
		mB(mi),
		0.0
	);
	if (uHair.w > 0.5 && (k == ${HEAD_K} || k == ${NECK_K})) {
		faceOn(w0 * rest(a, 0) + w1 * rest(b, 0) + w2 * rest(c, 0));
	}
	// Lit as drawMesh lights him.
	float nl = length(N);
	vec3 s = N / (nl == 0.0 ? 1.0 : nl);
	float lam = dot(s, MLIGHT);
	float key = cloth ? max(0.0, lam) : max(0.0, (lam + 0.32) / 1.32);
	float amb = 0.34 + 0.22 * (0.5 - 0.5 * s.y);
	float kk = (amb + 0.66 * key) * (1.0 + tone);
	float shade1 = 1.0 - key;
	vec3 col = vec3(
		ink.r * kk,
		ink.g * kk * (cloth ? 1.0 : 1.0 - 0.06 * shade1),
		ink.b * kk * (cloth ? 1.0 : 1.0 - 0.1 * shade1)
	);
	float e3 = clamp((dot(s, MBACK) - 0.55) / 0.4, 0.0, 1.0);
	float lift = e3 * e3 * (3.0 - 2.0 * e3) * shade1 * (cloth ? 0.11 : 0.15);
	float h0 = dot(s, MSHINE);
	if (h0 > 0.0) {
		lift += pow(h0, cloth ? 6.0 : 18.0) * (cloth ? 0.05 : 0.2);
	}
	if (lift > 0.0) {
		col += (255.0 - col) * min(1.0, lift);
	}
	oMesh = vec4(clamp(col / 255.0, 0.0, 1.0), float(cov) / ${f1(MESH_SS * MESH_SS)});
	oZ = uvec4(floatBitsToUint(uZ.x + near * uZ.y), uint(group + 0.5), 0u, 0u);
}`;

// ---- what the card keeps -------------------------------------------------------

type Uniforms<N extends string> = Record<N, WebGLUniformLocation | null>;
export type MeshGl = {
	// Laying his faces down; painting and lighting him.
	lay: WebGLProgram;
	shade: WebGLProgram;
	vao: WebGLVertexArrayObject;
	// The faces turned toward the camera, this time (their corners).
	shown: WebGLBuffer;
	points: WebGLTexture;
	rows: number;
	// The faces laid down (SS x SS the sprite's pixels): which, where on it;
	// how near.
	layFbo: WebGLFramebuffer;
	ids: WebGLTexture;
	depth: WebGLTexture;
	// Him painted and lit, a pixel of the sprite each: color, how much of the
	// pixel he covers; how far from the camera, and which part of him.
	fbo: WebGLFramebuffer;
	col: WebGLTexture;
	z: WebGLTexture;
	fw: number;
	fh: number;
	// (Bound where his face would be, when he has none.)
	blank: WebGLTexture;
	ul: Uniforms<"uBox" | "uLift">;
	u: Uniforms<
		"uP" | "uT" | "uMK" | "uFoot" | "uTex0" | "uTex1" | "uHair" | "uOpt" | "uZ"
	>;
	faces: WeakMap<FaceTex, { feat: WebGLTexture; hair: WebGLTexture }>;
};

const tex = (gl: WebGL2RenderingContext): WebGLTexture => {
	const t = gl.createTexture();
	gl.bindTexture(gl.TEXTURE_2D, t);
	gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.NEAREST);
	gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.NEAREST);
	gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
	gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
	return t;
};

// The texture units the painting reads (after the sculptor's four - see
// sculptGl.ts).
const UNITS = ["uV", "uRest", "uTri", "uI", "uD", "uFeat", "uHairA"] as const;
const unit = (name: (typeof UNITS)[number]) => 4 + UNITS.indexOf(name);
// Where the sculptor finds him painted (see sculptGl.ts): past these.
export const MESH_COL_UNIT = 4 + UNITS.length;

// The mesh handed to the card, once: each corner of each of his faces, and
// what is the same about him whatever he does.
export const meshGlStart = (
	gl: WebGL2RenderingContext,
	lay: WebGLProgram,
	shade: WebGLProgram,
): MeshGl | undefined => {
	const me: Mesh | undefined = meshData();
	if (!me) {
		return undefined;
	}
	const nt = me.tris.length / 3;
	const vao = gl.createVertexArray();
	gl.bindVertexArray(vao);
	const aV = new Uint16Array(nt * 3);
	const aKind = new Uint8Array(nt * 3);
	for (let t = 0; t < nt; t++) {
		for (let c = 0; c < 3; c++) {
			aV[t * 3 + c] = me.tris[t * 3 + c]!;
			aKind[t * 3 + c] = me.kind[t]!;
		}
	}
	for (const [at, data, type] of [
		[0, aV, gl.UNSIGNED_SHORT],
		[1, aKind, gl.UNSIGNED_BYTE],
	] as const) {
		const b = gl.createBuffer();
		gl.bindBuffer(gl.ARRAY_BUFFER, b);
		gl.bufferData(gl.ARRAY_BUFFER, data, gl.STATIC_DRAW);
		gl.enableVertexAttribArray(at);
		gl.vertexAttribIPointer(at, 1, type, 0, 0);
	}
	const shown = gl.createBuffer();
	gl.bindBuffer(gl.ELEMENT_ARRAY_BUFFER, shown);
	gl.bufferData(gl.ELEMENT_ARRAY_BUFFER, nt * 3 * 2, gl.DYNAMIC_DRAW);
	gl.bindVertexArray(null);
	gl.bindBuffer(gl.ARRAY_BUFFER, null);

	// (Each made on its own unit, bound there for good: units past the
	// sculptor's are never any other's.)
	// His faces: their corners' points, and their kind.
	gl.activeTexture(gl.TEXTURE0 + unit("uTri"));
	tex(gl);
	const triRows = Math.ceil(nt / ROW);
	const triData = new Uint16Array(ROW * triRows * 4);
	for (let t = 0; t < nt; t++) {
		triData[t * 4] = me.tris[t * 3]!;
		triData[t * 4 + 1] = me.tris[t * 3 + 1]!;
		triData[t * 4 + 2] = me.tris[t * 3 + 2]!;
		triData[t * 4 + 3] = me.kind[t]!;
	}
	gl.texImage2D(
		gl.TEXTURE_2D,
		0,
		gl.RGBA16UI,
		ROW,
		triRows,
		0,
		gl.RGBA_INTEGER,
		gl.UNSIGNED_SHORT,
		triData,
	);
	// His points: where on his face each is, and how much it goes with his
	// arm.
	gl.activeTexture(gl.TEXTURE0 + unit("uRest"));
	tex(gl);
	const restRows = Math.ceil((me.n * PER_REST) / ROW);
	const restData = new Float32Array(ROW * restRows * 4);
	for (let v = 0; v < me.n; v++) {
		const o = v * PER_REST * 4;
		for (let j = 0; j < 4; j++) {
			restData[o + j] = me.face[v * 4 + j]! / (j < 2 ? 8 : 1000);
		}
		restData[o + 4] = me.arm[v]! / 255;
	}
	gl.texImage2D(
		gl.TEXTURE_2D,
		0,
		gl.RGBA32F,
		ROW,
		restRows,
		0,
		gl.RGBA,
		gl.FLOAT,
		restData,
	);
	gl.activeTexture(gl.TEXTURE0 + unit("uV"));
	const rows = Math.ceil((me.n * PER_POINT) / ROW);
	const points = tex(gl);
	gl.texImage2D(
		gl.TEXTURE_2D,
		0,
		gl.RGBA32F,
		ROW,
		rows,
		0,
		gl.RGBA,
		gl.FLOAT,
		null,
	);
	const blank = tex(gl);
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

	gl.useProgram(lay);
	gl.uniform1i(gl.getUniformLocation(lay, "uV"), unit("uV"));
	gl.useProgram(shade);
	for (const [i, name] of ["uM", "uSheet", "uArt", "uPrint"].entries()) {
		gl.uniform1i(gl.getUniformLocation(shade, name), i);
	}
	for (const name of UNITS) {
		gl.uniform1i(gl.getUniformLocation(shade, name), unit(name));
	}
	const at = (p: WebGLProgram) => (n: string) => gl.getUniformLocation(p, n);
	const ul = at(lay);
	const u = at(shade);
	return {
		lay,
		shade,
		vao,
		shown,
		points,
		rows,
		layFbo: gl.createFramebuffer(),
		ids: tex(gl),
		depth: tex(gl),
		fbo: gl.createFramebuffer(),
		col: tex(gl),
		z: tex(gl),
		fw: 0,
		fh: 0,
		blank,
		ul: { uBox: ul("uBox"), uLift: ul("uLift") },
		u: {
			uP: u("uP"),
			uT: u("uT"),
			uMK: u("uMK"),
			uFoot: u("uFoot"),
			uTex0: u("uTex0"),
			uTex1: u("uTex1"),
			uHair: u("uHair"),
			uOpt: u("uOpt"),
			uZ: u("uZ"),
		},
		faces: new WeakMap(),
	};
};

// Room for a sprite w x h: its sheets, and the faces' at SS x SS.
const GROW = 256;
const roomFor = (
	gl: WebGL2RenderingContext,
	m: MeshGl,
	w: number,
	h: number,
) => {
	if (m.fw >= w && m.fh >= h) {
		return;
	}
	const up = (n: number) => Math.ceil(n / GROW) * GROW;
	m.fw = Math.max(m.fw, up(w));
	m.fh = Math.max(m.fh, up(h));
	const W2 = m.fw * MESH_SS;
	const H2 = m.fh * MESH_SS;
	// (On the unit his points are given on, bound again before he is drawn:
	// never one the sculptor reads from.)
	gl.activeTexture(gl.TEXTURE0 + unit("uV"));
	const make = (
		t: WebGLTexture,
		inner: number,
		format: number,
		type: number,
		w: number,
		h: number,
	) => {
		gl.bindTexture(gl.TEXTURE_2D, t);
		gl.texImage2D(gl.TEXTURE_2D, 0, inner, w, h, 0, format, type, null);
	};
	make(m.ids, gl.RGBA32UI, gl.RGBA_INTEGER, gl.UNSIGNED_INT, W2, H2);
	make(m.depth, gl.DEPTH_COMPONENT32F, gl.DEPTH_COMPONENT, gl.FLOAT, W2, H2);
	make(m.col, gl.RGBA8, gl.RGBA, gl.UNSIGNED_BYTE, m.fw, m.fh);
	make(m.z, gl.RG32UI, gl.RG_INTEGER, gl.UNSIGNED_INT, m.fw, m.fh);
	gl.bindFramebuffer(gl.FRAMEBUFFER, m.layFbo);
	gl.framebufferTexture2D(
		gl.FRAMEBUFFER,
		gl.COLOR_ATTACHMENT0,
		gl.TEXTURE_2D,
		m.ids,
		0,
	);
	gl.framebufferTexture2D(
		gl.FRAMEBUFFER,
		gl.DEPTH_ATTACHMENT,
		gl.TEXTURE_2D,
		m.depth,
		0,
	);
	gl.drawBuffers([gl.COLOR_ATTACHMENT0]);
	gl.bindFramebuffer(gl.FRAMEBUFFER, m.fbo);
	gl.framebufferTexture2D(
		gl.FRAMEBUFFER,
		gl.COLOR_ATTACHMENT0,
		gl.TEXTURE_2D,
		m.col,
		0,
	);
	gl.framebufferTexture2D(
		gl.FRAMEBUFFER,
		gl.COLOR_ATTACHMENT1,
		gl.TEXTURE_2D,
		m.z,
		0,
	);
	gl.drawBuffers([gl.COLOR_ATTACHMENT0, gl.COLOR_ATTACHMENT1]);
};

let up = new Float32Array(0);
let shownIx = new Uint16Array(0);
const mk = new Float32Array(N_MASSES * 8);
const foot = new Float32Array(16 * 4);
const lifts = new Float32Array(MESH_LIFT);
const CUTS = { bald: 1, long: 2, big: 3 } as const;

export type MeshDrawn = {
	// A box round him on the sprite (its pixels, ends included).
	box: { x0: number; y0: number; x1: number; y1: number };
};

// HIS BODY, onto the card: in `m.col` (his colors, and how much of each
// pixel he covers) and `m.z` (how far from the camera he is there, as the
// bits of a float; and the group of him there). What he is painted with -
// the numbers his masses' sculptor is told, its sheets on units 0-3 - is
// already on the card (see sculptGl.ts glSculpt): `params` and `twists`
// are those numbers.
export const meshDraw = (
	gl: WebGL2RenderingContext,
	m: MeshGl,
	b: Built,
	cam: Camera,
	st: PlayerState,
	body: Body,
	look: Look,
	px: number,
	ox: number,
	oy: number,
	w: number,
	h: number,
	params: Float32Array,
	twists: Float32Array,
): MeshDrawn | undefined => {
	const me = meshData();
	if (!me) {
		return undefined;
	}
	// (His shorts as long as his season's: see hemOf.)
	const hem = hemOf(look);
	const rig = meshRig(b, st, body, hem);
	const { rt, dn } = b.frame;
	const pts = meshPoints(rig.posed, body.H, {
		see: b.P,
		lens: lensOf(cam, st, px, ox, oy),
		rt,
		dn,
		toward: rig.toward,
		hem,
		flare: shortsFlareFor(hem),
	});
	// His points, as numbers; how near and far he is, and his box.
	const n = me.n;
	if (up.length < ROW * m.rows * 4) {
		up = new Float32Array(ROW * m.rows * 4);
	}
	const { scr, pos, posT, nrm, away } = pts;
	let z0 = Infinity;
	let z1 = -Infinity;
	let x0 = Infinity;
	let y0 = Infinity;
	let x1 = -Infinity;
	let y1 = -Infinity;
	for (let i = 0; i < n; i++) {
		const o = i * 12;
		const x = scr[i * 3]!;
		const y = scr[i * 3 + 1]!;
		const z = scr[i * 3 + 2]!;
		up[o] = x;
		up[o + 1] = y;
		up[o + 2] = z;
		up[o + 3] = nrm[i * 3]!;
		up[o + 4] = pos[i * 3]!;
		up[o + 5] = pos[i * 3 + 1]!;
		up[o + 6] = pos[i * 3 + 2]!;
		up[o + 7] = nrm[i * 3 + 1]!;
		up[o + 8] = posT[i * 3]!;
		up[o + 9] = posT[i * 3 + 1]!;
		up[o + 10] = posT[i * 3 + 2]!;
		up[o + 11] = nrm[i * 3 + 2]!;
		z0 = Math.min(z0, z);
		z1 = Math.max(z1, z);
		x0 = Math.min(x0, x);
		x1 = Math.max(x1, x);
		y0 = Math.min(y0, y);
		y1 = Math.max(y1, y);
	}
	const liftMost = Math.max(0, ...MESH_LIFT);
	z0 -= liftMost + 0.01;
	z1 += 0.01;
	const span = Math.max(1e-3, z1 - z0);
	const box = {
		x0: Math.max(0, Math.floor(x0 / MESH_SS) - 1),
		y0: Math.max(0, Math.floor(y0 / MESH_SS) - 1),
		x1: Math.min(w - 1, Math.ceil(x1 / MESH_SS) + 1),
		y1: Math.min(h - 1, Math.ceil(y1 / MESH_SS) + 1),
	};
	if (box.x1 < box.x0 || box.y1 < box.y0) {
		return undefined;
	}
	// The faces turned toward the camera (see meshPoints), in order.
	const nt = me.tris.length / 3;
	if (shownIx.length < nt * 3) {
		shownIx = new Uint16Array(nt * 3);
	}
	let shownN = 0;
	for (let t = 0; t < nt; t++) {
		if (!away[t] && !MESH_HIDDEN[me.kind[t]!]) {
			shownIx[shownN++] = t * 3;
			shownIx[shownN++] = t * 3 + 1;
			shownIx[shownN++] = t * 3 + 2;
		}
	}

	// What each kind of face is painted as.
	const put = (
		i: number,
		c: {
			A: { f: number; s: number; u: number };
			B: { f: number; s: number; u: number };
			kind: number;
			group: number;
			side: number;
		},
	) => {
		const o = i * 8;
		mk[o] = c.A.f;
		mk[o + 1] = c.A.s;
		mk[o + 2] = c.A.u;
		mk[o + 3] = c.kind + c.side * 16;
		mk[o + 4] = c.B.f;
		mk[o + 5] = c.B.s;
		mk[o + 6] = c.B.u;
		mk[o + 7] = c.group;
	};
	rig.mk.forEach((c, i) => put(i, c));
	rig.mkShorts.forEach((c, i) => put(SHORTS_AT + i, c));
	rig.restShoes.forEach((c, i) => put(SHOES_AT + i, c));
	// Each foot as posed, and at rest (see atRest).
	const H = body.H;
	rig.feet.forEach((bi, side) => {
		const bone = rig.posed[bi]!;
		const R = restOf(bi);
		const o = side * 32;
		const set = (j: number, v: { f: number; s: number; u: number }, k = 1) => {
			foot[o + j * 4] = v.f * k;
			foot[o + j * 4 + 1] = v.s * k;
			foot[o + j * 4 + 2] = v.u * k;
		};
		set(0, bone.fr.h);
		set(1, bone.fr.a, 1 / (H * bone.ax));
		set(2, bone.fr.f, 1 / (H * bone.rad));
		set(3, bone.fr.s, 1 / (H * bone.rad));
		set(4, R.h, H);
		set(5, R.a, H);
		set(6, R.f, H);
		set(7, R.s, H);
	});

	roomFor(gl, m, w, h);
	const W2 = w * MESH_SS;
	const H2 = h * MESH_SS;
	gl.activeTexture(gl.TEXTURE0 + unit("uV"));
	gl.bindTexture(gl.TEXTURE_2D, m.points);
	gl.texSubImage2D(
		gl.TEXTURE_2D,
		0,
		0,
		0,
		ROW,
		Math.ceil((n * PER_POINT) / ROW),
		gl.RGBA,
		gl.FLOAT,
		up,
	);
	// (Nothing bound that is being drawn on.)
	for (const name of ["uI", "uD", "uFeat", "uHairA"] as const) {
		gl.activeTexture(gl.TEXTURE0 + unit(name));
		gl.bindTexture(gl.TEXTURE_2D, m.blank);
	}

	// His faces, laid down.
	gl.bindFramebuffer(gl.FRAMEBUFFER, m.layFbo);
	gl.viewport(0, 0, W2, H2);
	gl.enable(gl.SCISSOR_TEST);
	gl.scissor(0, 0, W2, H2);
	gl.clearBufferuiv(gl.COLOR, 0, [0, 0, 0, 0]);
	gl.clearBufferfv(gl.DEPTH, 0, [1]);
	gl.enable(gl.DEPTH_TEST);
	gl.depthFunc(gl.LESS);
	gl.depthMask(true);
	gl.useProgram(m.lay);
	gl.uniform4f(m.ul.uBox, W2, H2, z0, span);
	gl.uniform1fv(m.ul.uLift, lifts);
	gl.bindVertexArray(m.vao);
	gl.bufferSubData(gl.ELEMENT_ARRAY_BUFFER, 0, shownIx, 0, shownN);
	gl.drawElements(gl.TRIANGLES, shownN, gl.UNSIGNED_SHORT, 0);
	gl.bindVertexArray(null);
	gl.disable(gl.DEPTH_TEST);

	// Painted and lit, a pixel of the sprite each.
	gl.bindFramebuffer(gl.FRAMEBUFFER, m.fbo);
	gl.viewport(0, 0, w, h);
	gl.scissor(0, 0, w, h);
	gl.clearBufferfv(gl.COLOR, 0, [0, 0, 0, 0]);
	gl.clearBufferuiv(gl.COLOR, 1, [0, 0, 0, 0]);
	gl.scissor(box.x0, box.y0, box.x1 - box.x0 + 1, box.y1 - box.y0 + 1);
	gl.useProgram(m.shade);
	gl.activeTexture(gl.TEXTURE0 + unit("uI"));
	gl.bindTexture(gl.TEXTURE_2D, m.ids);
	gl.activeTexture(gl.TEXTURE0 + unit("uD"));
	gl.bindTexture(gl.TEXTURE_2D, m.depth);
	// His face and hair, for his neck.
	const ft = look.faceTex ?? look.head?.tex;
	let faced = false;
	if (ft) {
		let t = m.faces.get(ft);
		if (!t) {
			gl.activeTexture(gl.TEXTURE0 + unit("uFeat"));
			const feat = tex(gl);
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
			gl.activeTexture(gl.TEXTURE0 + unit("uHairA"));
			const hair = tex(gl);
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
			m.faces.set(ft, t);
		}
		gl.activeTexture(gl.TEXTURE0 + unit("uFeat"));
		gl.bindTexture(gl.TEXTURE_2D, t.feat);
		gl.activeTexture(gl.TEXTURE0 + unit("uHairA"));
		gl.bindTexture(gl.TEXTURE_2D, t.hair);
		gl.uniform4f(m.u.uTex0, ft.x0, ft.y0, ft.scale, ft.w);
		const cut = look.cut ?? "short";
		gl.uniform4f(
			m.u.uTex1,
			ft.h,
			ft.hairSide,
			ft.hairFront,
			cut in CUTS ? CUTS[cut as keyof typeof CUTS] : 0,
		);
		faced = true;
	}
	const hair = rgb(look.hair);
	gl.uniform4f(m.u.uHair, hair[0], hair[1], hair[2], faced ? 1 : 0);
	gl.uniform4f(m.u.uOpt, look.outfit?.trousers === true ? 1 : 0, 0, 0, 0);
	gl.uniform4fv(m.u.uP, params);
	gl.uniform3fv(m.u.uT, twists);
	gl.uniform4fv(m.u.uMK, mk);
	gl.uniform4fv(m.u.uFoot, foot);
	gl.uniform2f(m.u.uZ, z0, span);
	gl.drawArrays(gl.TRIANGLES, 0, 3);
	gl.disable(gl.SCISSOR_TEST);
	return { box };
};
