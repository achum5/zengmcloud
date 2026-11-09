import type { Camera } from "./camera.ts";
import type { PlayerState } from "./evaluate.ts";
import type { Look } from "./figure.ts";
import {
	CHANNEL,
	CURVE_NEAR,
	CURVE_OUT,
	facingCamera,
	HELD,
	LEATHER,
	PRINT_EDGE,
	PRINT_INK,
	type PrintLevel,
} from "./gameBall.ts";
import {
	ART_H,
	ART_PANELS,
	ART_W,
	type ArtWrap,
	type KitArt,
} from "./kitArt.ts";
import type { Body } from "./poses.ts";
import {
	BACK_LIGHT,
	BALL,
	bigSheet,
	build,
	CAM_BODY,
	CAM_BODY_RGB,
	CAM_LENS,
	CAM_LENS_RGB,
	CLOTH,
	FORE,
	G_ARM,
	G_BALL,
	G_FINGER,
	G_HAND,
	GROUPS,
	HAND,
	heldSurface,
	LIGHT,
	NAIL,
	NECK,
	paletteOf,
	RICH,
	RS,
	SHIN,
	SHINE_AT,
	SHOE,
	SHORTS,
	T_ZIP,
	THIGH,
	TONGUE,
	TONGUE_UP,
	TORSO,
	TOWEL,
	TOWEL_RGB,
	TURN_STEPS,
	UPPER,
	type RGB,
} from "./sculpt.ts";

// ONE PLAYER, SCULPTED ON THE GRAPHICS CARD - the same body sculpt.ts makes
// a pixel at a time on the page, made there instead.
//
// Sculpting a man's new pose is most of what a frame costs (see sprite.ts
// and farm.ts): every pixel of his sprite asks every mass near it, on the
// page's thread or a hand's. The arithmetic is the same for every pixel,
// which is what a graphics card is for - so here his masses, as sculpt.ts
// builds them, are handed to it as numbers (a small table of them) and a
// fragment shader does shade's work and paint's for all his pixels at once:
// the nearest mass, the joints melting, what he wears there, the two tones
// of light, the fine line where he passes in front of himself. A second
// pass lays the dark rim round him (see sprite.ts rim) - his body on the
// left of the sheet, and beside it whatever goes over his head.
//
// Nothing is read back: the sheet is a canvas, and his picture is drawn
// from it straight into the one kept (see sprite.ts glKeep).
//
// The page's own sculptor stays the word on what he looks like: this is
// held to it (see glAgrees), and is not used where it can't be - no WebGL2,
// a card that isn't one (software drawing would be slower than the page),
// the context lost - or where it turns out no faster (see glTook).

// ---- the table of masses ------------------------------------------------------

// A mass a row: thirteen texels of it (see packMasses). Under them, which
// joint melts which two groups (a row a group), and the joints.
const TEX_W = 16;
const MAX_MASSES = 112;
const JOIN_OF_ROW = MAX_MASSES;
const JOINS_ROW = JOIN_OF_ROW + GROUPS;
const JOINS_PER_ROW = TEX_W;
const MAX_JOINS = JOINS_PER_ROW * 4;
const TEX_H = JOINS_ROW + MAX_JOINS / JOINS_PER_ROW;
const RAD_AT = 6;

// ---- what else the shader is told (uP) ----------------------------------------

const names = [
	"RT",
	"DN",
	// (w: kap.)
	"TW",
	// zs, the fine line's width, how far a mass reaches, -
	"MISC",
	// waist, band, strap, side
	"CUT0",
	// top, pit, neckIn, vAt
	"CUT1",
	// backAt, trim, sockTop, sole
	"CUT2",
	// hipStripe, open, his torso's length, his thigh's
	"CUT3",
	// hipW, ankleR, where a quarter-zip's zip stops, -
	"BODY",
	"PEL",
	"SPINE",
	"FW",
	// outfit, shirt, tie, stripes
	"FLAG0",
	// sides, zip, short sleeves, a picture wrapped round him
	"FLAG1",
	// trim from the picture, lettering, turned at the waist, -
	"FLAG2",
	// up in front of his face: right arm, left arm, the ball; -
	"LATE",
	// the same color as his jersey: stripes, sides, tie, shirt
	"SAME0",
	// the same as his jersey: trim; as his skin: sock; sole as shoe; -
	"SAME1",
	// his skin showing (right, then left): upper arm, forearm, leg; a wristband
	"SKIN_R",
	"SKIN_L",
	// a wristband his skin's color (R, L); a knee pad (R, L)
	"BANDS",
	// a knee pad his skin's color (R, L); -, -
	"PADS",
	"C_JERSEY",
	"C_TRIM",
	"C_SHORTS",
	"C_BAND",
	"C_STRIPE",
	"C_SKIN",
	"C_PALM",
	"C_SOCK",
	"C_SHOE",
	"C_SOLE",
	"C_TREAD",
	"C_ACCENT",
	"C_LACES",
	"C_LEGS_R",
	"C_LEGS_L",
	"C_UPPER_R",
	"C_UPPER_L",
	"C_FORE_R",
	"C_FORE_L",
	"C_WRIST_R",
	"C_WRIST_L",
	"C_KNEE_R",
	"C_KNEE_L",
	"C_SHIRT",
	"C_TIE",
	"C_STRIPES",
	"C_SIDES",
	"C_LAPEL",
	"PN_R",
	"PN_L",
	// The ball in his hands: cos and sin of its turn, its print's strength,
	// its grain's fineness
	"HELD0",
	// its grain's strength, half a channel's width, how dark, the print's size
	"HELD1",
	// A wrap (jersey, then shorts): a, b, top, bottom
	"WJ0",
	// band y, band h, turn, frontS
	"WJ1",
	// sideF
	"WJ2",
	"WS0",
	"WS1",
	"WS2",
	// The picture: its scale, its width, the rows of it that are uniform
	"ART",
	// The lettering: the sheet's width and height, its pixels to a foot, aS
	"SHEET0",
	// u0, a row of the sheet on the big one, how many times it is shrunk
	"SHEET1",
] as const;
type Name = (typeof names)[number];
const U = Object.fromEntries(names.map((n, i) => [n, i])) as Record<
	Name,
	number
>;
const NP = names.length;

const f1 = (x: number) => (Number.isInteger(x) ? `${x}.0` : `${x}`);
const v3 = (c: readonly number[]) => `vec3(${c.map(f1).join(", ")})`;

const VERT = `#version 300 es
void main() {
	vec2 p = vec2(float((gl_VertexID << 1) & 2), float(gl_VertexID & 2));
	gl_Position = vec4(p * 2.0 - 1.0, 0.0, 1.0);
}`;

// shade and paint (see sculpt.ts), a pixel of him at a time.
const FRAG = `#version 300 es
precision highp float;
precision highp int;
precision highp sampler2D;
${names.map((n, i) => `#define ${n} ${i}`).join("\n")}
uniform sampler2D uM;
uniform sampler2D uSheet;
uniform sampler2D uArt;
uniform sampler2D uPrint;
uniform vec4 uP[${NP}];
uniform vec3 uT[${(TURN_STEPS + 1) * 3}];
uniform int uN;
layout(location = 0) out vec4 oCol;
layout(location = 1) out vec4 oLate;

const float PI = 3.141592653589793;
const float ZT = 0.9;
const vec3 LIGHT = ${v3(LIGHT)};

vec4 M(int c, int i) { return texelFetch(uM, ivec2(c, i), 0); }
float RAD(int i, int j) {
	return texelFetch(uM, ivec2(${RAD_AT} + (j >> 2), i), 0)[j & 3];
}
float smin(float a, float b, float k) {
	float h = max(k - abs(a - b), 0.0) / k;
	return min(a, b) - (h * h * k) / 4.0;
}
float atan2s(float y, float x) {
	return (x == 0.0 && y == 0.0) ? 0.0 : atan(y, x);
}

struct Hit { float s; float z; float hx; float hy; float hn; float t; float r; };

// What mass i has to say about the pixel at p (false: nothing).
bool hit(int i, vec2 p, float margin, float zs, out Hit h, out vec4 m2) {
	vec4 m3 = M(3, i);
	if (p.x < m3.y - margin || p.x > m3.w + margin || p.y < m3.z - margin) {
		return false;
	}
	vec4 m4 = M(4, i);
	if (p.y > m4.x + margin) {
		return false;
	}
	vec4 m0 = M(0, i);
	vec4 m1 = M(1, i);
	float raw = m1.x > 1e-6
		? ((p.x - m0.x) * m0.z + (p.y - m0.y) * m0.w) / m1.x
		: 0.0;
	float t = clamp(raw, 0.0, 1.0);
	float ex = p.x - (m0.x + m0.z * t);
	float ey = p.y - (m0.y + m0.w * t);
	float dist = sqrt(ex * ex + ey * ey);
	float fi = t * ${f1(RS)};
	int i0 = int(fi);
	float r;
	if (i0 >= ${RS}) {
		r = RAD(i, ${RS});
	} else {
		float ra = RAD(i, i0);
		r = ra + (RAD(i, i0 + 1) - ra) * (fi - float(i0));
	}
	float s = dist - r;
	if (m3.x > 0.5 && raw > 1.0) {
		s = max(s, (raw - 1.0) * m1.y);
		dist = min(dist, r);
	}
	if (s > margin) {
		return false;
	}
	float qn = dist < r ? dist / r : 1.0;
	float nz = sqrt(max(0.0, 1.0 - qn * qn));
	m2 = M(2, i);
	float z = m1.z + (m1.w - m1.z) * t - (nz * r * zs) / m2.x;
	float inv = dist > 1e-6 ? qn / dist : 0.0;
	h = Hit(s, z, ex * inv, ey * inv, nz, t, r);
	return true;
}

// ---- the uniform's picture (see kitArt.ts artAt, artColor) --------------------

vec2 artAt(int w, float U_, float S, float F, bool notSide) {
	vec4 w0 = uP[w];
	vec4 w1 = uP[w + 1];
	float sideF = uP[w + 2].x;
	float down = clamp((w0.z - U_) / (w0.z - w0.w), 0.0, 1.0);
	float y = w1.x + down * (w1.y - 1.0);
	float phi = abs(atan2s(S / w0.x, F / w0.y));
	float px;
	float pw;
	float across;
	if (notSide || phi <= w1.z || phi >= PI - w1.z) {
		bool front = notSide ? F >= 0.0 : phi <= w1.z;
		px = front ? ${f1(ART_PANELS.front.x)} : ${f1(ART_PANELS.back.x)};
		pw = front ? ${f1(ART_PANELS.front.w)} : ${f1(ART_PANELS.back.w)};
		across = (front ? S : -S) / w1.w;
	} else {
		px = S < 0.0 ? ${f1(ART_PANELS.right.x)} : ${f1(ART_PANELS.left.x)};
		pw = S < 0.0 ? ${f1(ART_PANELS.right.w)} : ${f1(ART_PANELS.left.w)};
		across = (S < 0.0 ? F : -F) / sideF;
	}
	return vec2(clamp(px + (across * 0.5 + 0.5) * pw, px, px + pw - 1.0), y);
}
// (rgb as it is painted, 0-255; a how much of it there is.)
vec4 artColor(vec2 at) {
	float s = uP[ART].x;
	float W = uP[ART].y;
	float Hh = uP[ART].z;
	float px = clamp(at.x * s + (s - 1.0) / 2.0, 0.0, W - 1.0);
	float py = clamp(at.y * s + (s - 1.0) / 2.0, 0.0, Hh - 1.0);
	float x0 = floor(px);
	float y0 = floor(py);
	float x1 = min(W - 1.0, x0 + 1.0);
	float y1 = min(Hh - 1.0, y0 + 1.0);
	float fx = px - x0;
	float fy = py - y0;
	vec4 c00 = texelFetch(uArt, ivec2(int(x0), int(y0)), 0);
	vec4 c10 = texelFetch(uArt, ivec2(int(x1), int(y0)), 0);
	vec4 c01 = texelFetch(uArt, ivec2(int(x0), int(y1)), 0);
	vec4 c11 = texelFetch(uArt, ivec2(int(x1), int(y1)), 0);
	float w00 = (1.0 - fx) * (1.0 - fy) * c00.a;
	float w10 = fx * (1.0 - fy) * c10.a;
	float w01 = (1.0 - fx) * fy * c01.a;
	float w11 = fx * fy * c11.a;
	float a = w00 + w10 + w01 + w11;
	if (a < 0.004) {
		return vec4(0.0);
	}
	return vec4(
		(c00.rgb * w00 + c10.rgb * w10 + c01.rgb * w01 + c11.rgb * w11) * 255.0 / a,
		a
	);
}
vec3 artOver(int w, float U_, float S, float F, bool notSide, vec3 c) {
	vec4 a = artColor(artAt(w, U_, S, F, notSide));
	return a.a <= 0.0 ? c : c + (a.rgb - c) * a.a;
}

// ---- the game ball in his hands (see gameBall.ts ballSurface) -----------------

float channelDistance(vec3 p) {
	float ax = abs(p.x);
	float rho = acos(min(1.0, ax));
	float phi = atan2s(abs(p.y), abs(p.z));
	float c = cos(phi);
	float want = ${f1(CURVE_NEAR)} + ${f1(CURVE_OUT)} * c * c;
	float slope = (${f1(CURVE_OUT)} * sin(2.0 * phi)) / max(0.2, sin(rho));
	float curved = abs(rho - want) / sqrt(1.0 + slope * slope);
	return min(abs(p.y), min(ax, curved));
}
float grainAt(vec3 p, float k) {
	uint h = (uint(int(floor(p.x * k))) * 0x27d4eb2du)
		^ (uint(int(floor(p.y * k))) * 0x165667b1u)
		^ (uint(int(floor(p.z * k))) * 0x5bd1e995u);
	h = (h ^ (h >> 15)) * 0x2c1b3c6du;
	h ^= h >> 13;
	return float(h & 0xffu) / 127.5 - 1.0;
}
vec2 printAt(float u, float v) {
	float n = uP[HELD1].w;
	float fx = ((u + 1.0) / 2.0) * n - 0.5;
	float fy = ((1.0 - v) / 2.0) * n - 0.5;
	float x0 = floor(fx);
	float y0 = floor(fy);
	if (x0 < 0.0 || y0 < 0.0 || x0 >= n - 1.0 || y0 >= n - 1.0) {
		return vec2(0.0);
	}
	float tx = fx - x0;
	float ty = fy - y0;
	ivec2 at = ivec2(int(x0), int(y0));
	return texelFetch(uPrint, at, 0).rg * (1.0 - tx) * (1.0 - ty)
		+ texelFetch(uPrint, at + ivec2(1, 0), 0).rg * tx * (1.0 - ty)
		+ texelFetch(uPrint, at + ivec2(0, 1), 0).rg * (1.0 - tx) * ty
		+ texelFetch(uPrint, at + ivec2(1, 1), 0).rg * tx * ty;
}
vec3 ballSurface(vec3 p, float pixel) {
	vec3 c = ${v3(LEATHER)};
	float printK = uP[HELD0].z;
	if (uP[HELD1].w > 0.0 && printK > 0.0 && (p.x > 0.12 || p.x < -0.12)) {
		bool front = p.x > 0.0;
		float u = front ? -p.z : p.z;
		if (front || p.y > 0.08) {
			vec2 cov = printAt(u, p.y);
			float all = cov.x * printK;
			if (all > 0.001) {
				float ink = cov.y * printK;
				float edge = max(0.0, all - ink);
				c = c * (1.0 - all) + ${v3(PRINT_INK)} * ink + ${v3(PRINT_EDGE)} * edge;
			}
		}
	}
	if (uP[HELD1].x > 0.0) {
		c *= 1.0 + grainAt(p, uP[HELD0].w) * uP[HELD1].x;
	}
	float d = channelDistance(p);
	float reach = uP[HELD1].y + pixel * 0.5;
	if (d < reach) {
		float e = min(1.0, (reach - d) / max(pixel, 1e-3)) * uP[HELD1].z;
		c += (${v3(CHANNEL)} - c) * e;
	}
	return c;
}

// ---- what he wears ------------------------------------------------------------

vec3 ink;
bool cloth;
// (So much lighter or darker there: see sculpt.ts CLOTH.)
float tone;

// The jersey, shirt or jacket at a point on his torso.
void torsoAt(float U_, float S, float F) {
	cloth = true;
	bool outfit = uP[FLAG0].x > 0.5;
	bool shirt = uP[FLAG0].y > 0.5;
	bool wraps = uP[FLAG1].w > 0.5;
	float waist = uP[CUT0].x;
	float band = uP[CUT0].y;
	float strap = uP[CUT0].z;
	float side = uP[CUT0].w;
	float top = uP[CUT1].x;
	float pit = uP[CUT1].y;
	float neckIn = uP[CUT1].z;
	float vAt = uP[CUT1].w;
	float backAt = uP[CUT2].x;
	float trim = uP[CUT2].y;
	vec3 c;
	float hem = shirt ? -waist : waist;
	float round = atan2s(S, F);
	if (U_ < hem) {
		tone = ${f1(CLOTH.shorts * 0.6)}
			* sin(round * ${f1(CLOTH.seat[0])} + U_ * ${f1(CLOTH.seat[1])});
		c = uP[C_SHORTS].rgb;
		if (wraps) {
			c = artOver(WS0, U_, S, F, false, c);
		} else if (!outfit && abs(F) < 0.08 && abs(S) > uP[CUT3].x) {
			c = uP[C_STRIPE].rgb;
		}
	} else if (U_ < waist + band && !shirt) {
		c = wraps
			? artOver(WS0, U_, S, F, false, uP[C_BAND].rgb)
			: uP[C_BAND].rgb;
	} else {
		float aS = abs(S);
		float wN = min(1.0, aS / neckIn);
		bool front = F >= 0.0;
		float neckDrop = outfit
			? (front ? backAt * 1.2 : backAt * 0.6) * (1.0 - wN * wN)
			: front
				? vAt * (1.0 - wN)
				: backAt * (1.0 - wN * wN);
		float slope = front && !outfit
			? vAt / neckIn
			: (2.0 * backAt * wN) / neckIn;
		float below = (top - neckDrop - U_) / sqrt(1.0 + slope * slope);
		float dNeck = aS >= neckIn
			? max(aS - neckIn, below)
			: below > 0.0
				? below
				: max(below, aS - neckIn);
		float dArm = 1e30;
		if (!outfit) {
			float sx = min(aS, side);
			float uy = min(U_, top);
			float ea = side - strap;
			float eb = top - pit;
			float gx = (sx - side) / ea;
			float gy = (uy - top) / eb;
			float g = gx * gx + gy * gy - 1.0;
			float gl = 2.0 * length(vec2(gx / ea, gy / eb));
			dArm = g / (gl == 0.0 ? 1.0 : gl);
		}
		float edge = min(dArm, dNeck);
		if (edge >= 0.0) {
			float low = clamp((top - U_) / (top - waist), 0.0, 1.0);
			float drape = sin(
					round * ${f1(CLOTH.drape[0])}
						+ ${f1(CLOTH.drape[1])} * sin(U_ * ${f1(CLOTH.drape[2])} + 0.7)
				)
				* (0.25 + 0.75 * low)
				* (0.45 + 0.55 * abs(sin(round)));
			float tuck = clamp(
				1.0 - (U_ - waist) / (${f1(CLOTH.gather[2])} * uP[CUT3].z),
				0.0,
				1.0
			);
			tone = ${f1(CLOTH.jersey)} * (
				0.7 * drape
				+ 0.5 * tuck
					* sin(round * ${f1(CLOTH.gather[0])} + U_ * ${f1(CLOTH.gather[1])})
			);
		}
		if (edge < 0.0) {
			c = uP[C_SKIN].rgb;
			cloth = false;
		} else if (edge < trim && !outfit && uP[FLAG2].x < 0.5) {
			c = uP[C_TRIM].rgb;
		} else {
			c = uP[C_JERSEY].rgb;
			bool isJersey = true;
			if (uP[FLAG0].w > 0.5) {
				float th = atan2s(S / (side * 0.8), F / (side * 0.4));
				if (mod(floor((th * side * 0.8) / 0.17 + 100.0), 2.0) == 0.0) {
					c = uP[C_STRIPES].rgb;
					isJersey = uP[SAME0].x > 0.5;
				}
				if (uP[FLAG1].x > 0.5 && abs(abs(th) - PI / 2.0) < 0.32) {
					c = uP[C_SIDES].rgb;
					isJersey = uP[SAME0].y > 0.5;
				}
			}
			if (shirt && front) {
				float depth = top - U_;
				float halfOpen = neckIn * max(0.0, 1.0 - depth / uP[CUT3].y);
				if (aS < halfOpen) {
					if (aS < neckIn * 0.16 + depth * 0.06 && uP[FLAG0].z > 0.5) {
						c = uP[C_TIE].rgb;
						isJersey = uP[SAME0].z > 0.5;
					} else {
						c = uP[C_SHIRT].rgb;
						isJersey = uP[SAME0].w > 0.5;
					}
				} else if (aS < halfOpen + trim * 1.6) {
					c = uP[C_LAPEL].rgb;
					isJersey = false;
				}
			} else if (uP[FLAG1].y > 0.5 && front && aS < trim * 0.6 && U_ > uP[BODY].z) {
				c = uP[C_TRIM].rgb;
				isJersey = uP[SAME1].x > 0.5;
			}
			if (wraps && isJersey) {
				c = artOver(WJ0, U_, S, F, false, c);
			}
			if (uP[FLAG2].y > 0.5 && isJersey) {
				// His number and lettering.
				float sw = uP[SHEET0].x;
				float sAS = uP[SHEET0].w;
				float across = clamp(S / sAS, -1.0, 1.0);
				float th = F >= 0.0
					? asin(across)
					: (across < 0.0 ? -PI : PI) - asin(across);
				float x = mod(sw / 2.0 + th * sAS * uP[SHEET0].z, sw);
				float y = (uP[SHEET1].x - U_) * uP[SHEET0].z;
				if (y >= 0.0 && y < uP[SHEET0].y - 1.0) {
					vec4 t = textureLod(
						uSheet,
						vec2((x + 0.5) / sw, (y + 0.5) * uP[SHEET1].y),
						uP[SHEET1].z
					);
					if (t.a > 0.004) {
						ink = c * (1.0 - t.a) + t.rgb * 255.0;
						return;
					}
				}
			}
		}
	}
	ink = c;
}

void paint(vec4 m2, Hit h, vec3 P, vec3 A, vec3 B, float len) {
	int kind = int(m2.z + 0.5);
	int side = int(m2.w + 0.5);
	float t = h.t;
	vec4 skinOf = side == 0 ? uP[SKIN_R] : uP[SKIN_L];
	if (kind == ${TORSO}) {
		vec3 r0 = P - uP[PEL].xyz;
		vec3 spine = uP[SPINE].xyz;
		vec3 fw = uP[FW].xyz;
		if (uP[FLAG2].z > 0.5) {
			// Where it sat before he turned.
			float up0 = (r0.x * spine.x + r0.z * spine.z) / uP[CUT3].z;
			int k = int(clamp(floor(up0 * ${f1(TURN_STEPS)} + 0.5), 0.0, ${f1(TURN_STEPS)})) * 3;
			r0 = vec3(dot(uT[k], r0), dot(uT[k + 1], r0), dot(uT[k + 2], r0));
		}
		torsoAt(r0.x * spine.x + r0.z * spine.z, r0.y, r0.x * fw.x + r0.z * fw.z);
	} else if (kind == ${SHORTS}) {
		cloth = true;
		vec3 l = B - A;
		float ll = length(l);
		if (ll == 0.0) { ll = 1.0; }
		vec3 o = P - A - l * t;
		float along = dot(o, l) / (ll * ll);
		o -= l * along;
		float ol = length(o);
		if (ol == 0.0) { ol = 1.0; }
		bool outfit = uP[FLAG0].x > 0.5;
		tone = ${f1(CLOTH.shorts)}
			* sin(
				atan2s(o.y, o.x) * ${f1(CLOTH.legs[0])}
					+ t * ${f1(CLOTH.legs[1])}
					+ float(side) * ${f1(CLOTH.legs[2])}
			)
			* (0.35 + 0.65 * t);
		vec3 c = !outfit && (o.y * (side == 0 ? -1.0 : 1.0)) / ol > 0.95
			? uP[C_STRIPE].rgb
			: uP[C_SHORTS].rgb;
		if (uP[FLAG1].w > 0.5) {
			vec3 a = l / ll;
			vec3 x = vec3(-a.x * a.y, 1.0 - a.y * a.y, -a.z * a.y);
			float xl = length(x);
			if (xl == 0.0) { xl = 1.0; }
			x /= xl;
			float across = dot(o, x);
			float ahead = dot(o, cross(a, x));
			float outS = side == 0 ? -1.0 : 1.0;
			c = artOver(
				WS0,
				-(0.16 + 0.68 * (t + along)) * uP[CUT3].w,
				outS * uP[BODY].x + across,
				ahead,
				across * outS < 0.0,
				uP[C_SHORTS].rgb
			);
		}
		ink = c;
	} else if (kind == ${UPPER} || kind == ${FORE}) {
		bool upper = kind == ${UPPER};
		vec3 c = upper
			? (side == 0 ? uP[C_UPPER_R].rgb : uP[C_UPPER_L].rgb)
			: (side == 0 ? uP[C_FORE_R].rgb : uP[C_FORE_L].rgb);
		bool skin = upper ? skinOf.x > 0.5 : skinOf.y > 0.5;
		if (!upper && skinOf.w > 0.5 && t > 0.76 && t < 0.97 && len > 0.0) {
			c = side == 0 ? uP[C_WRIST_R].rgb : uP[C_WRIST_L].rgb;
			skin = (side == 0 ? uP[BANDS].x : uP[BANDS].y) > 0.5;
		}
		if (uP[FLAG1].z > 0.5 && upper && t > 0.55) {
			c = uP[C_SKIN].rgb;
			skin = true;
		}
		cloth = !skin;
		if (skin) {
			float into = upper ? 1.0 - t : t;
			if (into < ${f1(CLOTH.jointSpan)}) {
				tone = -${f1(CLOTH.joint)} * (1.0 - into / ${f1(CLOTH.jointSpan)});
			}
		}
		ink = c;
	} else if (kind == ${THIGH} || kind == ${SHIN}) {
		vec3 c = side == 0 ? uP[C_LEGS_R].rgb : uP[C_LEGS_L].rgb;
		bool skin = skinOf.z > 0.5;
		if (kind == ${SHIN} && P.z < uP[CUT2].z) {
			c = uP[C_SOCK].rgb;
			skin = uP[SAME1].y > 0.5;
		}
		bool pad = (side == 0 ? uP[BANDS].z : uP[BANDS].w) > 0.5;
		if (pad && ((kind == ${THIGH} && t > 0.82) || (kind == ${SHIN} && t < 0.2))) {
			c = side == 0 ? uP[C_KNEE_R].rgb : uP[C_KNEE_L].rgb;
			skin = (side == 0 ? uP[PADS].x : uP[PADS].y) > 0.5;
		}
		cloth = !skin;
		if (skin) {
			float into = kind == ${THIGH} ? 1.0 - t : t;
			if (into < ${f1(CLOTH.jointSpan)}) {
				tone = -${f1(CLOTH.joint)} * (1.0 - into / ${f1(CLOTH.jointSpan)});
			}
		}
		ink = c;
	} else if (kind == ${SHOE}) {
		float sole = uP[CUT2].w;
		vec3 c = P.z < sole ? uP[C_SOLE].rgb : uP[C_SHOE].rgb;
		if (P.z < sole * 0.35) {
			c = uP[C_TREAD].rgb;
		} else if (P.z >= sole || uP[SAME1].z > 0.5) {
			vec3 l = B - A;
			float ll = length(l);
			if (ll == 0.0) { ll = 1.0; }
			if (ll > uP[BODY].y * 4.0) {
				vec3 o = P - A - l * t;
				float ol = length(o);
				if (ol == 0.0) { ol = 1.0; }
				float up = o.z / ol;
				float outS = abs(o.x * l.y - o.y * l.x) / (ll * ol);
				float mid = 0.42 - (0.5 * (t - 0.15)) / 0.7;
				float wide = 0.28 * (1.0 - abs(t - 0.5) * 1.6);
				if (outS > 0.5 && t > 0.15 && t < 0.85 && abs(up - mid) < wide) {
					c = uP[C_ACCENT].rgb;
				} else if (up > 0.78 && t > 0.38 && t < 0.72) {
					c = uP[C_LACES].rgb;
				}
			}
		}
		cloth = true;
		ink = c;
	} else if (kind == ${TONGUE}) {
		vec3 o = P - A - (B - A) * t;
		float ol = length(o);
		if (ol == 0.0) { ol = 1.0; }
		ink = o.z / ol > ${f1(TONGUE_UP)} && t > 0.12 && t < 0.92
			? uP[C_LACES].rgb
			: uP[C_SHOE].rgb;
		cloth = true;
	} else if (kind == ${BALL}) {
		float hc = uP[HELD0].x;
		float hs = uP[HELD0].y;
		float nz = h.hn;
		vec3 n = vec3(h.hx * hc - nz * hs, -h.hy, nz * hc + h.hx * hs);
		ink = ballSurface(
			vec3(dot(n, ${v3(HELD[0])}), dot(n, ${v3(HELD[1])}), dot(n, ${v3(HELD[2])})),
			1.0 / (h.r * max(0.3, sqrt(nz)))
		);
	} else if (kind == ${CAM_BODY}) {
		ink = ${v3(CAM_BODY_RGB)};
	} else if (kind == ${TOWEL}) {
		ink = ${v3(TOWEL_RGB)};
	} else if (kind == ${NAIL}) {
		vec3 c = uP[C_SKIN].rgb;
		ink = c + (uP[C_PALM].rgb - c) * 0.62 + vec3(6.0, 0.0, 0.0);
	} else if (kind == ${HAND}) {
		vec3 nb = h.hx * uP[RT].xyz + h.hy * uP[DN].xyz + h.hn * uP[TW].xyz;
		float inPalm = dot(nb, side == 0 ? uP[PN_R].xyz : uP[PN_L].xyz);
		vec3 c = uP[C_SKIN].rgb;
		float e = inPalm > 0.15 ? min(1.0, (inPalm - 0.15) / 0.25) * 0.42 : 0.0;
		ink = c + (uP[C_PALM].rgb - c) * e;
	} else if (kind == ${CAM_LENS}) {
		ink = ${v3(CAM_LENS_RGB)};
	} else {
		if (kind == ${NECK}) {
			float e = clamp(
				(t - ${f1(CLOTH.jaw[1])}) / ${f1(CLOTH.jaw[2] - CLOTH.jaw[1])},
				0.0,
				1.0
			);
			tone = -${f1(CLOTH.jaw[0])} * e * e * (3.0 - 2.0 * e);
		}
		ink = uP[C_SKIN].rgb;
	}
}

void main() {
	vec2 p = gl_FragCoord.xy;
	float zs = uP[MISC].x;
	float lineW = uP[MISC].y;
	float margin = uP[MISC].z;
	int best = -1;
	int least = -1;
	float bestZ = 1e30;
	float leastS = 1e30;
	Hit hb;
	Hit hl;
	vec4 mb;
	vec4 ml;
	Hit h;
	vec4 m2;
	for (int i = 0; i < uN; i++) {
		if (!hit(i, p, margin, zs, h, m2)) {
			continue;
		}
		if (h.s <= 0.0 && h.z < bestZ) {
			bestZ = h.z;
			best = i;
			hb = h;
			mb = m2;
		}
		if (h.s < leastS) {
			leastS = h.s;
			least = i;
			hl = h;
			ml = m2;
		}
	}
	if (least < 0 || leastS > 0.5 + margin) {
		discard;
	}
	int f = best >= 0 ? best : least;
	Hit hf = hl;
	vec4 mf = ml;
	if (best >= 0) {
		hf = hb;
		mf = mb;
	}
	int gf = int(mf.y + 0.5);
	float S = hf.s;
	vec3 nrm = vec3(hf.hx, hf.hy, hf.hn);
	bool behind = false;
	float shadow = 0.0;
	float groove = 0.0;
	for (int j = 0; j < uN; j++) {
		if (j == f || !hit(j, p, margin, zs, h, m2)) {
			continue;
		}
		float dz = h.z - hf.z;
		float K = 0.0;
		int gj = int(m2.y + 0.5);
		if (gj == gf) {
			K = min(h.r, hf.r) * ${f1(RICH.melt)};
		} else {
			float ji = texelFetch(uM, ivec2(gj >> 2, ${JOIN_OF_ROW} + gf), 0)[gj & 3];
			if (ji >= 0.0) {
				int jn = int(ji + 0.5);
				vec4 at = texelFetch(
					uM,
					ivec2(jn % ${JOINS_PER_ROW}, ${JOINS_ROW} + jn / ${JOINS_PER_ROW}),
					0
				);
				float dj = length(p - at.xy);
				K = at.z * 0.45 * max(0.0, 1.0 - dj / at.z);
			}
		}
		if (K > 0.0 && dz < ZT && dz > -ZT) {
			K *= 1.0 - abs(dz) / ZT;
			float before = S;
			S = smin(S, h.s, K);
			float wgt = 1.0 - abs(h.s - before) / K;
			if (wgt > 0.0) {
				nrm += vec3(h.hx, h.hy, h.hn) * wgt;
				float bend = 1.0 - dot(vec3(h.hx, h.hy, h.hn), vec3(hf.hx, hf.hy, hf.hn));
				groove = max(groove, wgt * min(1.0, bend * ${f1(RICH.bend)}));
			}
		} else if (h.s < 0.0 && dz > 0.05) {
			behind = true;
		} else if (dz < -0.05 && h.s > 0.0) {
			float sw = lineW * 3.5;
			if (h.s < sw) {
				shadow = max(shadow, 1.0 - h.s / sw);
			}
		}
	}
	float cover = 0.5 - S;
	if (cover <= 0.0) {
		discard;
	}
	float nl = length(nrm);
	vec3 sn = nrm / (nl == 0.0 ? 1.0 : nl);
	// The point on him this pixel shows (feet, his frame).
	vec4 m4 = M(4, f);
	vec4 m5 = M(5, f);
	vec3 A = m4.yzw;
	vec3 B = m5.xyz;
	float rf = hf.r / mf.x;
	vec3 tw = uP[TW].xyz;
	vec3 P = A + (B - A) * hf.t
		+ hf.hx * rf * uP[RT].xyz
		+ hf.hy * rf * uP[DN].xyz
		+ hf.hn * rf * vec3(tw.x, tw.y, tw.z + uP[TW].w);
	cloth = false;
	tone = 0.0;
	paint(mf, hf, P, A, B, M(1, f).y);
	// Cloth hangs smoother over him than skin does.
	if (cloth) {
		sn = normalize(sn + vec3(0.0, 0.0, 0.35));
	}
	float lam = dot(sn, LIGHT);
	// Two tones, the step between them soft.
	float e = clamp((lam - 0.34) / 0.16, 0.0, 1.0);
	float lit = e * e * (3.0 - 2.0 * e);
	float dark = 1.0 - 0.22 * shadow;
	// Turned well away from the lights, darker still; the edge of him the
	// lights behind catch; his skin shining (see sculpt.ts RICH).
	float e2 = clamp((lam - ${f1(RICH.deepFrom)}) / ${f1(RICH.deepOver)}, 0.0, 1.0);
	float deep = 1.0 - e2 * e2 * (3.0 - 2.0 * e2);
	float e3 = clamp(
		(dot(sn, ${v3(BACK_LIGHT)}) - ${f1(RICH.backFrom)}) / ${f1(RICH.backOver)},
		0.0,
		1.0
	);
	float lift = e3 * e3 * (3.0 - 2.0 * e3) * (1.0 - lit)
		* (cloth ? ${f1(RICH.back[0])} : ${f1(RICH.back[1])});
	if (!cloth) {
		float e4 = clamp(
			(dot(sn, ${v3(SHINE_AT)}) - ${f1(RICH.shineFrom)}) / ${f1(RICH.shineOver)},
			0.0,
			1.0
		);
		lift += e4 * e4 * (3.0 - 2.0 * e4) * ${f1(RICH.shine)};
	}
	vec3 c = cloth
		? ink * ((0.79 + 0.21 * lit - ${f1(RICH.deep[0])} * deep) * dark)
		// Skin in shadow warms.
		: ink * (vec3(0.84, 0.74, 0.72) + vec3(0.16, 0.26, 0.28) * lit
			- ${v3(RICH.deep.slice(1))} * deep) * dark;
	c *= 1.0 + tone
		- groove * (cloth ? ${f1(RICH.groove[0])} : ${f1(RICH.groove[1])});
	c += (255.0 - c) * lift;
	// A fine line where he passes in front of himself - and all round his
	// hands, so they read.
	int kind = int(mf.z + 0.5);
	if ((behind || kind == ${HAND} || kind == ${NAIL}) && S > -lineW) {
		c += (ink * 0.38 - c) * min(1.0, (S + lineW) / lineW);
	}
	float a = min(1.0, cover);
	oCol = vec4(clamp(c / 255.0, 0.0, 1.0) * a, a);
	// Up in front of his face: an arm, or the ball.
	bool right = gf == ${G_ARM} || gf == ${G_HAND}
		|| (gf >= ${G_FINGER} && gf < ${G_FINGER + 5});
	bool left = gf == ${G_ARM + 1} || gf == ${G_HAND + 1}
		|| (gf >= ${G_FINGER + 5} && gf < ${G_FINGER + 10});
	bool late = (right && uP[LATE].x > 0.5)
		|| (left && uP[LATE].y > 0.5)
		|| (gf == ${G_BALL} && uP[LATE].z > 0.5);
	oLate = vec4(late ? 1.0 : 0.0, 0.0, 0.0, 1.0);
}`;

// The dark rim (see sprite.ts rim) - round all of him, or round just what
// goes over his head - onto the sheet, the right way up.
const SOLID = 140;
const RIM: RGB = [22, 15, 13];
const FRAG_RIM = `#version 300 es
precision highp float;
uniform sampler2D uC;
uniform sampler2D uL;
uniform int uOver;
// The sprite's size; where its left edge is on the sheet, and the sheet's
// height.
uniform vec4 uBox;
out vec4 o;
vec4 at(ivec2 p) {
	if (p.x < 0 || p.y < 0 || p.x >= int(uBox.x) || p.y >= int(uBox.y)) {
		return vec4(0.0);
	}
	vec4 c = texelFetch(uC, p, 0);
	if (uOver == 1 && texelFetch(uL, p, 0).r < 0.5) {
		return vec4(0.0);
	}
	return c;
}
bool solid(ivec2 p) { return at(p).a * 255.0 >= ${f1(SOLID - 0.5)}; }
void main() {
	ivec2 p = ivec2(int(gl_FragCoord.x - uBox.z), int(uBox.w - gl_FragCoord.y));
	vec4 c = at(p);
	if (c.a * 255.0 < ${f1(SOLID - 0.5)} && (
		solid(p + ivec2(-1, 0)) || solid(p + ivec2(1, 0)) ||
		solid(p + ivec2(0, -1)) || solid(p + ivec2(0, 1))
	)) {
		c = vec4(c.rgb + ${v3(RIM.map((v) => v / 255))} * (1.0 - c.a), 1.0);
	}
	o = c;
}`;

// ---- the card -----------------------------------------------------------------

type Gl = {
	gl: WebGL2RenderingContext;
	canvas: HTMLCanvasElement;
	shade: WebGLProgram;
	rim: WebGLProgram;
	uP: WebGLUniformLocation | null;
	uT: WebGLUniformLocation | null;
	uN: WebGLUniformLocation | null;
	uOver: WebGLUniformLocation | null;
	uBox: WebGLUniformLocation | null;
	masses: WebGLTexture;
	blank: WebGLTexture;
	fbo: WebGLFramebuffer;
	col: WebGLTexture;
	late: WebGLTexture;
	// The size of the two pictures he is shaded into.
	fw: number;
	fh: number;
	sheets: WeakMap<HTMLCanvasElement, WebGLTexture>;
	arts: WeakMap<KitArt, WebGLTexture>;
	prints: WeakMap<PrintLevel, WebGLTexture>;
};

// The biggest sheet: a man this many pixels across or tall is sculpted on
// the page.
const MAX_SIDE = 2048;
const GROW = 256;

let card: Gl | null | undefined;
// How long the card has taken over its first men (see glTook): how many,
// and the time - and the page, over some of the same; n < 0 once it has
// been judged.
const took = { n: 0, ms: 0, page: 0, pages: 0 };
// Lost, or found out: not used again this visit.
let off = false;

// What the browser was told (for whoever looks into a device that draws
// him wrong): "0" never, "1" whatever the card is.
const asked = (): string | null => {
	try {
		return globalThis.localStorage?.getItem("court3dGl") ?? null;
	} catch {
		return null;
	}
};
export const glForced = () => asked() === "1";

const compile = (
	gl: WebGL2RenderingContext,
	vert: string,
	frag: string,
): WebGLProgram | undefined => {
	const prog = gl.createProgram();
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
		gl.attachShader(prog, sh);
	}
	gl.linkProgram(prog);
	if (!gl.getProgramParameter(prog, gl.LINK_STATUS)) {
		if (glForced()) {
			console.warn("court3d gl:", gl.getProgramInfoLog(prog));
		}
		return undefined;
	}
	return prog;
};

const texture = (gl: WebGL2RenderingContext, filter: number): WebGLTexture => {
	const t = gl.createTexture();
	gl.bindTexture(gl.TEXTURE_2D, t);
	gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, filter);
	gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, filter);
	gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
	gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
	return t;
};

const start = (): Gl | null => {
	if (typeof document === "undefined" || asked() === "0") {
		return null;
	}
	const canvas = document.createElement("canvas");
	canvas.width = GROW * 2;
	canvas.height = GROW;
	const gl = canvas.getContext("webgl2", {
		alpha: true,
		premultipliedAlpha: true,
		antialias: false,
		depth: false,
		stencil: false,
		preserveDrawingBuffer: true,
		// (A card that is no card - the page's own sums are quicker.)
		failIfMajorPerformanceCaveat: !glForced(),
	});
	if (!gl) {
		return null;
	}
	const shade = compile(gl, VERT, FRAG);
	const rim = compile(gl, VERT, FRAG_RIM);
	if (!shade || !rim) {
		return null;
	}
	canvas.addEventListener("webglcontextlost", () => {
		off = true;
	});
	gl.disable(gl.BLEND);
	gl.disable(gl.DEPTH_TEST);
	gl.pixelStorei(gl.UNPACK_ALIGNMENT, 1);
	const masses = texture(gl, gl.NEAREST);
	gl.texImage2D(
		gl.TEXTURE_2D,
		0,
		gl.RGBA32F,
		TEX_W,
		TEX_H,
		0,
		gl.RGBA,
		gl.FLOAT,
		null,
	);
	const blank = texture(gl, gl.NEAREST);
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
	gl.useProgram(shade);
	for (const [i, name] of ["uM", "uSheet", "uArt", "uPrint"].entries()) {
		gl.uniform1i(gl.getUniformLocation(shade, name), i);
	}
	gl.useProgram(rim);
	gl.uniform1i(gl.getUniformLocation(rim, "uC"), 0);
	gl.uniform1i(gl.getUniformLocation(rim, "uL"), 1);
	return {
		gl,
		canvas,
		shade,
		rim,
		uP: gl.getUniformLocation(shade, "uP"),
		uT: gl.getUniformLocation(shade, "uT"),
		uN: gl.getUniformLocation(shade, "uN"),
		uOver: gl.getUniformLocation(rim, "uOver"),
		uBox: gl.getUniformLocation(rim, "uBox"),
		masses,
		blank,
		fbo: gl.createFramebuffer(),
		col: texture(gl, gl.NEAREST),
		late: texture(gl, gl.NEAREST),
		fw: 0,
		fh: 0,
		sheets: new WeakMap(),
		arts: new WeakMap(),
		prints: new WeakMap(),
	};
};

// The card, if there is one to sculpt on.
export const glCard = (): Gl | undefined => {
	if (off) {
		return undefined;
	}
	if (card === undefined) {
		try {
			card = start();
		} catch {
			card = null;
		}
	}
	return card ?? undefined;
};
// No more of it this visit (see sprite.ts: it drew him wrong, or no
// faster).
export const glOff = () => {
	off = true;
};

// Room on the sheet for a sprite w x h and what goes over his head beside
// it, and on the pictures he is shaded into.
const roomFor = (g: Gl, w: number, h: number) => {
	const { gl, canvas } = g;
	const up = (n: number) => Math.ceil(n / GROW) * GROW;
	if (canvas.width < w * 2 || canvas.height < h) {
		canvas.width = Math.max(canvas.width, up(w * 2));
		canvas.height = Math.max(canvas.height, up(h));
	}
	if (g.fw < w || g.fh < h) {
		g.fw = Math.max(g.fw, up(w));
		g.fh = Math.max(g.fh, up(h));
		gl.bindFramebuffer(gl.FRAMEBUFFER, g.fbo);
		for (const [i, t] of [g.col, g.late].entries()) {
			gl.bindTexture(gl.TEXTURE_2D, t);
			gl.texImage2D(
				gl.TEXTURE_2D,
				0,
				gl.RGBA8,
				g.fw,
				g.fh,
				0,
				gl.RGBA,
				gl.UNSIGNED_BYTE,
				null,
			);
			gl.framebufferTexture2D(
				gl.FRAMEBUFFER,
				gl.COLOR_ATTACHMENT0 + i,
				gl.TEXTURE_2D,
				t,
				0,
			);
		}
		gl.drawBuffers([gl.COLOR_ATTACHMENT0, gl.COLOR_ATTACHMENT1]);
	}
};

// ---- him, as numbers ----------------------------------------------------------

const table = new Float32Array(TEX_W * TEX_H * 4);
const params = new Float32Array(NP * 4);
const twists = new Float32Array((TURN_STEPS + 1) * 9);

const set = (i: number, a = 0, b = 0, c = 0, d = 0) => {
	params[i * 4] = a;
	params[i * 4 + 1] = b;
	params[i * 4 + 2] = c;
	params[i * 4 + 3] = d;
};
const on = (x: unknown) => (x ? 1 : 0);
const wrap = (i: number, w: ArtWrap) => {
	set(i, w.a, w.b, w.top, w.bottom);
	set(i + 1, w.band.y, w.band.h, w.turn, w.frontS);
	set(i + 2, w.sideF);
};

export type GlSculpted = {
	// The sheet: his body at its top left, w x h; what goes over his head,
	// if anything does, to the right of it.
	canvas: HTMLCanvasElement;
	over: boolean;
	head: { x: number; y: number; r: number };
	// A box round everything drawn (sprite pixels, ends included) - no
	// tighter than it need be, nothing of him outside it.
	box: { x0: number; y0: number; x1: number; y1: number };
};

// HIS SPRITE, as sculpt has it (w x h of its pixels, each px screen pixels,
// its top left at (ox, oy) on the screen) - on the card's sheet. Nothing,
// if he is not one for the card: sculpt him on the page.
export const glSculpt = (
	cam: Camera,
	st: PlayerState,
	body: Body,
	look: Look,
	px: number,
	ox: number,
	oy: number,
	w: number,
	h: number,
): GlSculpted | undefined => {
	const g = glCard();
	if (!g || w > MAX_SIDE / 2 || h > MAX_SIDE) {
		return undefined;
	}
	if (g.gl.isContextLost()) {
		off = true;
		return undefined;
	}
	const b = build(cam, st, body, look, px, ox, oy, true);
	const masses = b.masses;
	const n = masses.length;
	if (n > MAX_MASSES || b.joins.length > MAX_JOINS) {
		return undefined;
	}
	const { gl } = g;

	// His masses, a row each.
	const lineW = Math.max(0.7, 0.028 * b.k);
	const margin = lineW * 3.5 + 1;
	let x0 = Infinity;
	let y0 = Infinity;
	let x1 = -Infinity;
	let y1 = -Infinity;
	let ball = 0;
	for (let i = 0; i < n; i++) {
		const c = masses[i]!;
		const o = i * TEX_W * 4;
		table[o] = c.ax;
		table[o + 1] = c.ay;
		table[o + 2] = c.dx;
		table[o + 3] = c.dy;
		table[o + 4] = c.l2;
		table[o + 5] = c.len;
		table[o + 6] = c.za;
		table[o + 7] = c.zb;
		table[o + 8] = c.k;
		table[o + 9] = c.group;
		table[o + 10] = c.kind;
		table[o + 11] = c.side;
		table[o + 12] = c.flat ? 1 : 0;
		table[o + 13] = c.x0;
		table[o + 14] = c.y0;
		table[o + 15] = c.x1;
		table[o + 16] = c.y1;
		table[o + 17] = c.A.f;
		table[o + 18] = c.A.s;
		table[o + 19] = c.A.u;
		table[o + 20] = c.B.f;
		table[o + 21] = c.B.s;
		table[o + 22] = c.B.u;
		table.set(c.rad, o + RAD_AT * 4);
		x0 = Math.min(x0, c.x0);
		y0 = Math.min(y0, c.y0);
		x1 = Math.max(x1, c.x1);
		y1 = Math.max(y1, c.y1);
		if (c.kind === BALL) {
			ball = c.rad[0]!;
		}
	}
	// Which joint melts two groups together, and the joints.
	table.fill(-1, JOIN_OF_ROW * TEX_W * 4, JOINS_ROW * TEX_W * 4);
	b.joins.forEach((j, i) => {
		table[(JOIN_OF_ROW + j.a) * TEX_W * 4 + j.b] = i;
		table[(JOIN_OF_ROW + j.b) * TEX_W * 4 + j.a] = i;
		const o = (JOINS_ROW * TEX_W + i) * 4;
		table[o] = j.x;
		table[o + 1] = j.y;
		table[o + 2] = j.reach;
	});

	const pal = paletteOf(look);
	const outfit = look.outfit;
	const { rt, dn, tw, zs, kap } = b.frame;
	const cut = b.cut;
	const col = (i: number, c: RGB | undefined) => {
		if (c) {
			set(i, c[0], c[1], c[2]);
		} else {
			set(i);
		}
	};
	const same = (a: RGB | undefined, c: RGB) => on(a === c);
	set(U.RT, rt.f, rt.s, rt.u);
	set(U.DN, dn.f, dn.s, dn.u);
	set(U.TW, tw.f, tw.s, tw.u, kap);
	set(U.MISC, zs, lineW, margin);
	set(U.CUT0, cut.waist, cut.band, cut.strap, cut.side);
	set(U.CUT1, cut.top, cut.pit, cut.neckIn, cut.vAt);
	set(U.CUT2, cut.backAt, cut.trim, cut.sockTop, cut.sole);
	set(U.CUT3, cut.hipStripe, cut.open, body.torso, body.thigh);
	set(U.BODY, body.hipW, body.ankleR, cut.top - T_ZIP * cut.top);
	set(U.PEL, b.pel.f, b.pel.s, b.pel.u);
	set(U.SPINE, b.spine.f, b.spine.s, b.spine.u);
	set(U.FW, b.fwd.f, b.fwd.s, b.fwd.u);
	const art = look.kitArt;
	const wraps = b.wraps && art ? b.wraps : undefined;
	const sheet = bigSheet(look, body, b.k * 1.2, cut.waist);
	set(U.FLAG0, on(outfit), on(pal.shirt), on(pal.tie), on(pal.stripes));
	set(
		U.FLAG1,
		on(pal.sides),
		on(outfit && !outfit.shirt && !outfit.plain && outfit.sleeves === "long"),
		on(outfit?.sleeves === "short"),
		on(wraps),
	);
	set(
		U.FLAG2,
		on(art?.fit?.trim === "picture" && wraps),
		on(sheet),
		on(b.untwist),
	);
	set(U.LATE, on(b.late[0]), on(b.late[1]), on(b.late[2]));
	set(
		U.SAME0,
		same(pal.stripes, pal.jersey),
		same(pal.sides, pal.jersey),
		same(pal.tie, pal.jersey),
		same(pal.shirt, pal.jersey),
	);
	set(
		U.SAME1,
		same(pal.trim, pal.jersey),
		same(pal.sock, pal.skin),
		same(pal.sole, pal.shoe),
	);
	for (const s of [0, 1] as const) {
		set(
			s === 0 ? U.SKIN_R : U.SKIN_L,
			same(pal.upper[s], pal.skin),
			same(pal.fore[s], pal.skin),
			same(pal.legs[s], pal.skin),
			on(pal.wrist[s]),
		);
	}
	set(
		U.BANDS,
		same(pal.wrist[0], pal.skin),
		same(pal.wrist[1], pal.skin),
		on(pal.knee[0]),
		on(pal.knee[1]),
	);
	set(U.PADS, same(pal.knee[0], pal.skin), same(pal.knee[1], pal.skin));
	col(U.C_JERSEY, pal.jersey);
	col(U.C_TRIM, pal.trim);
	col(U.C_SHORTS, pal.shorts);
	col(U.C_BAND, pal.band);
	col(U.C_STRIPE, pal.stripe);
	col(U.C_SKIN, pal.skin);
	col(U.C_PALM, pal.palm);
	col(U.C_SOCK, pal.sock);
	col(U.C_SHOE, pal.shoe);
	col(U.C_SOLE, pal.sole);
	col(U.C_TREAD, pal.tread);
	col(U.C_ACCENT, pal.accent);
	col(U.C_LACES, pal.laces);
	col(U.C_LEGS_R, pal.legs[0]);
	col(U.C_LEGS_L, pal.legs[1]);
	col(U.C_UPPER_R, pal.upper[0]);
	col(U.C_UPPER_L, pal.upper[1]);
	col(U.C_FORE_R, pal.fore[0]);
	col(U.C_FORE_L, pal.fore[1]);
	col(U.C_WRIST_R, pal.wrist[0]);
	col(U.C_WRIST_L, pal.wrist[1]);
	col(U.C_KNEE_R, pal.knee[0]);
	col(U.C_KNEE_L, pal.knee[1]);
	col(U.C_SHIRT, pal.shirt);
	col(U.C_TIE, pal.tie);
	col(U.C_STRIPES, pal.stripes);
	col(U.C_SIDES, pal.sides);
	col(U.C_LAPEL, pal.lapel);
	set(U.PN_R, b.pn[0].f, b.pn[0].s, b.pn[0].u);
	set(U.PN_L, b.pn[1].f, b.pn[1].s, b.pn[1].u);

	// The ball in his hands, and its print.
	let print: PrintLevel | undefined;
	if (ball > 0) {
		const heldTurn = st.yaw + (st.ballFace ?? 0) - facingCamera(cam.right);
		const s = heldSurface(ball);
		print = s.level;
		set(U.HELD0, Math.cos(heldTurn), Math.sin(heldTurn), s.printK, s.grainK);
		set(U.HELD1, s.grainA, s.half, s.seamK, print ? print.n : 0);
	} else {
		set(U.HELD0);
		set(U.HELD1);
	}
	if (wraps && art) {
		wrap(U.WJ0, wraps.jersey);
		wrap(U.WS0, wraps.shorts);
		set(U.ART, art.scale, ART_W * art.scale, ART_H * art.scale);
	}
	if (sheet) {
		set(U.SHEET0, sheet.w, sheet.h, sheet.ppf, sheet.aS);
		set(
			U.SHEET1,
			sheet.u0,
			sheet.bigPpf / sheet.ppf / sheet.big.height,
			Math.max(0, Math.log2(sheet.bigPpf / sheet.ppf)),
		);
	}
	if (b.untwist) {
		twists.set(b.untwist);
	}

	// What he is painted from.
	let sheetTex = g.blank;
	if (sheet) {
		let t = g.sheets.get(sheet.big);
		if (!t) {
			t = texture(gl, gl.LINEAR);
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
				sheet.big,
			);
			gl.pixelStorei(gl.UNPACK_PREMULTIPLY_ALPHA_WEBGL, false);
			gl.generateMipmap(gl.TEXTURE_2D);
			g.sheets.set(sheet.big, t);
		}
		sheetTex = t;
	}
	let artTex = g.blank;
	if (wraps && art) {
		let t = g.arts.get(art);
		if (!t) {
			const aw = ART_W * art.scale;
			t = texture(gl, gl.NEAREST);
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
		artTex = t;
	}
	let printTex = g.blank;
	if (print) {
		let t = g.prints.get(print);
		if (!t) {
			const { n: pn, all, ink } = print;
			const both = new Uint8Array(pn * pn * 2);
			for (let i = 0; i < pn * pn; i++) {
				both[i * 2] = all[i]!;
				both[i * 2 + 1] = ink[i]!;
			}
			t = texture(gl, gl.NEAREST);
			gl.texImage2D(
				gl.TEXTURE_2D,
				0,
				gl.RG8,
				pn,
				pn,
				0,
				gl.RG,
				gl.UNSIGNED_BYTE,
				both,
			);
			g.prints.set(print, t);
		}
		printTex = t;
	}

	// (Nothing of him is outside this box: no pixel outside it is asked.)
	const box = {
		x0: Math.max(0, Math.floor(x0 - margin)),
		y0: Math.max(0, Math.floor(y0 - margin)),
		x1: Math.min(w - 1, Math.ceil(x1 + margin)),
		y1: Math.min(h - 1, Math.ceil(y1 + margin)),
	};
	if (box.x1 < box.x0 || box.y1 < box.y0) {
		return undefined;
	}

	roomFor(g, w, h);
	// Shaded.
	gl.bindFramebuffer(gl.FRAMEBUFFER, g.fbo);
	gl.viewport(0, 0, w, h);
	gl.enable(gl.SCISSOR_TEST);
	gl.scissor(0, 0, w, h);
	gl.clearBufferfv(gl.COLOR, 0, [0, 0, 0, 0]);
	gl.clearBufferfv(gl.COLOR, 1, [0, 0, 0, 0]);
	gl.scissor(box.x0, box.y0, box.x1 - box.x0 + 1, box.y1 - box.y0 + 1);
	gl.useProgram(g.shade);
	gl.activeTexture(gl.TEXTURE0);
	gl.bindTexture(gl.TEXTURE_2D, g.masses);
	gl.texSubImage2D(
		gl.TEXTURE_2D,
		0,
		0,
		0,
		TEX_W,
		TEX_H,
		gl.RGBA,
		gl.FLOAT,
		table,
	);
	gl.activeTexture(gl.TEXTURE1);
	gl.bindTexture(gl.TEXTURE_2D, sheetTex);
	gl.activeTexture(gl.TEXTURE2);
	gl.bindTexture(gl.TEXTURE_2D, artTex);
	gl.activeTexture(gl.TEXTURE3);
	gl.bindTexture(gl.TEXTURE_2D, printTex);
	gl.uniform4fv(g.uP, params);
	gl.uniform3fv(g.uT, twists);
	gl.uniform1i(g.uN, n);
	gl.drawArrays(gl.TRIANGLES, 0, 3);

	// Rimmed, onto the sheet: all of him, then what goes over his head.
	const over = b.late.some(Boolean);
	const H = g.canvas.height;
	gl.bindFramebuffer(gl.FRAMEBUFFER, null);
	gl.useProgram(g.rim);
	gl.activeTexture(gl.TEXTURE0);
	gl.bindTexture(gl.TEXTURE_2D, g.col);
	gl.activeTexture(gl.TEXTURE1);
	gl.bindTexture(gl.TEXTURE_2D, g.late);
	for (const part of over ? [0, 1] : [0]) {
		gl.viewport(part * w, H - h, w, h);
		gl.scissor(part * w, H - h, w, h);
		gl.uniform1i(g.uOver, part);
		gl.uniform4f(g.uBox, w, h, part * w, H);
		gl.drawArrays(gl.TRIANGLES, 0, 3);
	}
	gl.disable(gl.SCISSOR_TEST);
	// (While the card is being timed - see glTook - its work is waited for,
	// so the time is the card's and not just the asking's.)
	if (glTimed()) {
		gl.finish();
	}
	if (gl.isContextLost()) {
		off = true;
		return undefined;
	}

	return { canvas: g.canvas, over, head: b.head, box };
};

// The sheet's pixels, w x h from its top left, top row first (for holding
// the card to the page's own sculptor - see sprite.ts glAgrees).
export const glPixels = (w: number, h: number): Uint8Array | undefined => {
	const g = glCard();
	if (!g) {
		return undefined;
	}
	const { gl } = g;
	const up = new Uint8Array(w * h * 4);
	gl.bindFramebuffer(gl.FRAMEBUFFER, null);
	gl.readPixels(0, g.canvas.height - h, w, h, gl.RGBA, gl.UNSIGNED_BYTE, up);
	const out = new Uint8Array(w * h * 4);
	for (let y = 0; y < h; y++) {
		out.set(up.subarray((h - 1 - y) * w * 4, (h - y) * w * 4), y * w * 4);
	}
	return out;
};

// ---- whether it is any quicker ------------------------------------------------

// The card is timed over its first men against the page's own sculptor
// making some of the same ones (`page`: how long it takes over the man just
// made, ms): a card that is no quicker than the page - one that is no card
// at all, the browser doing its sums for it - is no help, and not used.
// (The first few are slow anywhere: the shader's first use.)
const WARM = 6;
const JUDGED = 30;
const EVERY = 5;
// (Far slower than the page a third of the way in: no need of the rest.)
const EARLY = 10;
const FAR = 2;
// Whether the card is still being timed: its work is waited for then,
// here and in the picture kept (see sprite.ts glKeep) - a card only looks
// quick if what it is asked is not waited for.
export const glTimed = () => took.n >= 0 && !glForced();
export const glTook = (ms: number, page: () => number) => {
	if (took.n < 0 || glForced()) {
		return;
	}
	took.n += 1;
	const judged = took.n - WARM;
	if (judged <= 0) {
		return;
	}
	took.ms += ms;
	if (judged % EVERY === 0) {
		took.page += page();
		took.pages += 1;
	}
	const slower = (by: number) =>
		took.pages > 0 && took.ms / judged > (took.page / took.pages) * by;
	if (judged === EARLY ? slower(FAR) : judged >= JUDGED) {
		if (slower(judged >= JUDGED ? 1 : FAR)) {
			glOff();
		}
		took.n = -1;
	}
};
