import { BODY_META } from "./bodyMesh.meta.ts";
import type { Body, Limb, Skeleton, V3 } from "./poses.ts";

// HIS BODY AS A MAN'S IS MADE - a mesh of him (MakeHuman's, see
// tools/court3d-body/bake.py), not masses laid along his bones.
//
// Every point of it is held in the frames of the bones that move it, as
// much in each as its weights say. Posed, each bone's frame is worked out
// from the court's own skeleton - his hips and shoulders square to the way
// they face, each limb swung from the one it hangs off (never twisted round
// itself: no wrung-out elbows or knees) - and stretched along it to his own
// bones' lengths, and round it to his own build; each point goes where its
// bones carry it. Then he is drawn a pixel at a time, nearest the camera
// winning, each pixel painted and lit as the sculptor would have it (see
// sculpt.ts): his kit painted onto him where it is, the light falling on
// him smoothly.

// ---- the mesh, unpacked once -------------------------------------------------

const unpack = <T>(b64: string, make: (buf: ArrayBuffer) => T): T => {
	const bin = atob(b64);
	const buf = new ArrayBuffer(bin.length);
	const u8 = new Uint8Array(buf);
	for (let i = 0; i < bin.length; i++) {
		u8[i] = bin.charCodeAt(i);
	}
	return make(buf);
};
type Mesh = {
	n: number;
	start: Uint32Array;
	counts: Uint8Array;
	bone: Uint8Array;
	weight: Float32Array;
	local: Float32Array;
	tris: Uint16Array;
	kind: Uint8Array;
	arm: Uint8Array;
	face: Int16Array;
};
// (The mesh itself is fetched only when a man is to be drawn as one - see
// loadMesh: most never need it.)
let mesh: Mesh | undefined;
let loading: Promise<void> | undefined;
export const meshReady = () => mesh !== undefined;
export const loadMesh = (): Promise<void> =>
	(loading ??= import("./bodyMesh.data.ts").then(({ BODY_DATA: D }) => {
		const counts = unpack(D.counts, (b) => new Uint8Array(b));
		const start = new Uint32Array(counts.length + 1);
		for (let i = 0; i < counts.length; i++) {
			start[i + 1] = start[i]! + counts[i]!;
		}
		// (Weights as bytes, each point's made whole again; offsets in steps.)
		const wq = unpack(D.weight, (b) => new Uint8Array(b));
		const weight = new Float32Array(wq.length);
		for (let i = 0; i < counts.length; i++) {
			let sum = 0;
			for (let j = start[i]!; j < start[i + 1]!; j++) {
				sum += wq[j]!;
			}
			for (let j = start[i]!; j < start[i + 1]!; j++) {
				weight[j] = sum > 0 ? wq[j]! / sum : 1 / counts[i]!;
			}
		}
		const lq = unpack(D.local, (b) => new Int16Array(b));
		const local = new Float32Array(lq.length);
		for (let i = 0; i < lq.length; i++) {
			local[i] = lq[i]! * BODY_META.localStep;
		}
		mesh = {
			n: BODY_META.points,
			start,
			counts,
			bone: unpack(D.bone, (b) => new Uint8Array(b)),
			weight,
			local,
			tris: unpack(D.tris, (b) => new Uint16Array(b)),
			kind: unpack(D.kind, (b) => new Uint8Array(b)),
			arm: unpack(D.arm, (b) => new Uint8Array(b)),
			face: unpack(D.face, (b) => new Int16Array(b)),
		};
	}));
const meshOf = (): Mesh => {
	if (!mesh) {
		throw new Error("mesh not loaded");
	}
	return mesh;
};
export const MESH_KINDS: readonly string[] = BODY_META.kinds;
export type MeshKind = (typeof BODY_META.kinds)[number];

// ---- frames --------------------------------------------------------------------

type F3 = { h: V3; a: V3; f: V3; s: V3 };
const v3 = (f: number, s: number, u: number): V3 => ({ f, s, u });
const sub = (a: V3, b: V3) => v3(a.f - b.f, a.s - b.s, a.u - b.u);
const dot = (a: V3, b: V3) => a.f * b.f + a.s * b.s + a.u * b.u;
const scale = (a: V3, k: number) => v3(a.f * k, a.s * k, a.u * k);
const len = (a: V3) => Math.sqrt(dot(a, a));
const unit = (a: V3, or: V3) => {
	const l = len(a);
	return l > 1e-9 ? scale(a, 1 / l) : or;
};
const crossV = (a: V3, b: V3) =>
	v3(a.s * b.u - a.u * b.s, a.u * b.f - a.f * b.u, a.f * b.s - a.s * b.f);
const ortho = (v: V3, a: V3) => sub(v, scale(a, dot(v, a)));
const addV = (a: V3, b: V3) => v3(a.f + b.f, a.s + b.s, a.u + b.u);
// Rotate v by the rotation that takes unit a onto unit b, the short way.
const swingTo = (a: V3, b: V3) => {
	const c = dot(a, b);
	const ax = crossV(a, b);
	const s = len(ax);
	if (s < 1e-9) {
		return (v: V3) => (c > 0 ? v : scale(v, -1));
	}
	const k = scale(ax, 1 / s);
	return (v: V3) => {
		const kv = crossV(k, v);
		const kd = dot(k, v);
		return v3(
			v.f * c + kv.f * s + k.f * kd * (1 - c),
			v.s * c + kv.s * s + k.s * kd * (1 - c),
			v.u * c + kv.u * s + k.u * kd * (1 - c),
		);
	};
};
const J = BODY_META.joints as Record<string, readonly number[]>;
const jt = (name: string) => v3(J[name]![0]!, J[name]![1]!, J[name]![2]!);
// A bone at rest: along it, his forward and his left square to that (as
// bake.py has them).
const restFrame = (from: string, to: string, foot = false): F3 => {
	const h = jt(from);
	const a = unit(sub(jt(to), h), v3(0, 0, 1));
	const f = unit(ortho(foot ? v3(0, 0, 1) : v3(1, 0, 0), a), v3(1, 0, 0));
	return { h, a, f, s: crossV(a, f) };
};
const REST: Record<string, F3> = {
	pelvis: restFrame("pelvis", "chest"),
	chest: restFrame("pelvis", "chest"),
	neck: restFrame("chest", "head"),
};
// (His hand: along it to his middle knuckle, his palm its "forward".)
const handFrame = (s: string): F3 => {
	const h = jt(`wrist${s}`);
	const a = unit(sub(jt(`handTip${s}`), h), v3(0, 0, -1));
	const f = unit(ortho(sub(jt(`palm${s}`), h), a), v3(1, 0, 0));
	return { h, a, f, s: crossV(a, f) };
};
for (const s of ["R", "L"]) {
	REST[`hand${s}`] = handFrame(s);
	REST[`upper${s}`] = restFrame(`shoulder${s}`, `elbow${s}`);
	REST[`fore${s}`] = restFrame(`elbow${s}`, `wrist${s}`);
	REST[`thigh${s}`] = restFrame(`hip${s}`, `knee${s}`);
	REST[`shin${s}`] = restFrame(`knee${s}`, `ankle${s}`);
	REST[`foot${s}`] = restFrame(`ankle${s}`, `toe${s}`, true);
}
const restLen = (from: string, to: string) => len(sub(jt(to), jt(from)));

// A bone posed off its parent: the parent's turn from rest carried onto it,
// then swung straight onto where the skeleton has it (no twist of its own).
const swungFrom = (
	parentRest: F3,
	parentPosed: F3,
	rest: F3,
	h: V3,
	to: V3,
): F3 => {
	// The parent's turn: rest frame -> posed frame.
	const carry = (v: V3) => {
		const x = dot(v, parentRest.a);
		const y = dot(v, parentRest.f);
		const z = dot(v, parentRest.s);
		return v3(
			parentPosed.a.f * x + parentPosed.f.f * y + parentPosed.s.f * z,
			parentPosed.a.s * x + parentPosed.f.s * y + parentPosed.s.s * z,
			parentPosed.a.u * x + parentPosed.f.u * y + parentPosed.s.u * z,
		);
	};
	const a0 = carry(rest.a);
	const a = unit(sub(to, h), a0);
	const sw = swingTo(a0, a);
	return { h, a, f: sw(carry(rest.f)), s: sw(carry(rest.s)) };
};
// A frame square to `a` with its left as near `left` as can be.
const squareFrame = (h: V3, a: V3, left: V3): F3 => {
	const s = unit(ortho(left, a), v3(0, 1, 0));
	return { h, a, f: crossV(s, a), s };
};

// ---- posed ---------------------------------------------------------------------

// Half way between two frames at the same head: each way of it averaged,
// squared up again.
const between = (a: F3, b: F3, w: number): F3 => {
	const mix = (x: V3, y: V3) =>
		v3(x.f + (y.f - x.f) * w, x.s + (y.s - x.s) * w, x.u + (y.u - x.u) * w);
	const ax = unit(mix(a.a, b.a), a.a);
	const f = unit(ortho(mix(a.f, b.f), ax), a.f);
	return {
		h: mix(a.h, b.h),
		a: ax,
		f,
		s: crossV(ax, f),
	};
};

// EACH PIECE OF HIM, POSED (the pieces bake.py weights MakeHuman's skeleton
// to): its frame, and how far it is stretched along it and round it - for
// his skeleton and his build. His spine turns a little at a time from his
// hips to his shoulders, his neck the same up to his head; his shoulders go
// half with his chest and half with his arms.
type Posed = { fr: F3; ax: number; rad: number };
export const poseBones = (
	sk: Skeleton,
	body: Body,
	typical: Body,
	// Which way each hand points and its palm faces (his right, his left -
	// see sculpt.ts), if his hands are posed.
	hands?: { hd: [V3, V3]; pn: [V3, V3] },
): Posed[] => {
	const H = body.H;
	const girth = body.shoulderW / (typical.shoulderW * (body.H / typical.H));
	const limbG = (r: number, rT: number) => r / (rT * (body.H / typical.H));
	const upAx = unit(sub(sk.chest, sk.pelvis), v3(0, 0, 1));
	const hips = sub(sk.legL.root, sk.legR.root);
	const shoulders = sub(sk.armL.root, sk.armR.root);
	const pelvis = squareFrame(sk.pelvis, upAx, hips);
	const chest = squareFrame(sk.pelvis, upAx, shoulders);
	const torsoAx =
		len(sub(sk.chest, sk.pelvis)) / (restLen("pelvis", "chest") * H);
	const torso = (fr: F3): Posed => ({ fr, ax: torsoAx, rad: girth });
	// (Carried round with his chest, not swung onto anything of its own.)
	const carried = (rest: F3, h: V3): F3 => {
		const c = (v: V3) => {
			const x = dot(v, REST.chest!.a);
			const y = dot(v, REST.chest!.f);
			const z = dot(v, REST.chest!.s);
			return v3(
				chest.a.f * x + chest.f.f * y + chest.s.f * z,
				chest.a.s * x + chest.f.s * y + chest.s.s * z,
				chest.a.u * x + chest.f.u * y + chest.s.u * z,
			);
		};
		return { h, a: c(rest.a), f: c(rest.f), s: c(rest.s) };
	};
	const neck = swungFrom(REST.chest!, chest, REST.neck!, sk.chest, sk.head);
	const neckAx = Math.max(
		0.6,
		(len(sub(sk.head, sk.chest)) - body.headR * 0.55) /
			(restLen("chest", "head") * H),
	);
	const fr: Record<string, Posed> = {
		pelvis: torso(pelvis),
		spine1: torso(between(pelvis, chest, 0.25)),
		spine2: torso(between(pelvis, chest, 0.5)),
		spine3: torso(between(pelvis, chest, 0.7)),
		spine4: torso(between(pelvis, chest, 0.85)),
		chest: torso(chest),
		neck1: {
			fr: between(carried(REST.neck!, sk.chest), neck, 0.5),
			ax: neckAx,
			rad: girth,
		},
		neck2: { fr: neck, ax: neckAx, rad: girth },
		head: { fr: neck, ax: neckAx, rad: girth },
	};
	const limb = (
		l: Limb,
		s: "R" | "L",
		names: [string, string, string | undefined],
		parent: F3,
		parentRest: F3,
		rads: [number, number, number],
	) => {
		const [n1, n2, n3] = names;
		const r1 = REST[n1 + s]!;
		const r2 = REST[n2 + s]!;
		const f1 = swungFrom(parentRest, parent, r1, l.root, l.mid);
		const f2 = swungFrom(r1, f1, r2, l.mid, l.end);
		const ax1 =
			len(sub(l.mid, l.root)) / (len(sub(jtOf(n1, s, 1), jtOf(n1, s, 0))) * H);
		const ax2 =
			len(sub(l.end, l.mid)) / (len(sub(jtOf(n2, s, 1), jtOf(n2, s, 0))) * H);
		fr[n1 + s] = { fr: f1, ax: ax1, rad: rads[0] };
		fr[n2 + s] = { fr: f2, ax: ax2, rad: rads[1] };
		if (n3) {
			const r3 = REST[n3 + s]!;
			// (His foot as long as a man's: only pointed the way the
			// skeleton's is.)
			fr[n3 + s] = {
				fr: swungFrom(r2, f2, r3, l.end, l.tip ?? l.end),
				ax: 1,
				rad: rads[2],
			};
		} else {
			// His hand, the way the court's hand would be: pointed and turned
			// as it is.
			const i = s === "R" ? 0 : 1;
			if (hands) {
				const a = unit(hands.hd[i], f2.a);
				const f = unit(ortho(hands.pn[i], a), f2.f);
				fr["hand" + s] = {
					fr: { h: l.end, a, f, s: crossV(a, f) },
					ax: 1,
					rad: 1,
				};
			} else {
				fr["hand" + s] = {
					fr: swungFrom(
						r2,
						f2,
						REST[`hand${s}`]!,
						l.end,
						l.tip ?? addV(l.end, f2.a),
					),
					ax: 1,
					rad: 1,
				};
			}
			// His shoulder: half with his chest, half with his arm.
			fr["shoulder" + s] = {
				fr: between(carried(r1, l.root), f1, 0.5),
				ax: (1 + ax1) / 2,
				rad: (girth + rads[0]) / 2,
			};
		}
	};
	const thighG = limbG(body.thighR, typical.thighR);
	const calfG = limbG(body.calfR, typical.calfR);
	const upperG = limbG(body.upperR, typical.upperR);
	const foreG = limbG(body.foreR, typical.foreR);
	limb(sk.legR, "R", ["thigh", "shin", "foot"], pelvis, REST.pelvis!, [
		thighG,
		calfG,
		1,
	]);
	limb(sk.legL, "L", ["thigh", "shin", "foot"], pelvis, REST.pelvis!, [
		thighG,
		calfG,
		1,
	]);
	limb(sk.armR, "R", ["upper", "fore", undefined], chest, REST.chest!, [
		upperG,
		foreG,
		foreG,
	]);
	limb(sk.armL, "L", ["upper", "fore", undefined], chest, REST.chest!, [
		upperG,
		foreG,
		foreG,
	]);
	return BODY_META.bones.map((name) => fr[name] ?? fr.chest!);
};
const JOINT_OF: Record<string, [string, string]> = {
	upper: ["shoulder", "elbow"],
	fore: ["elbow", "wrist"],
	thigh: ["hip", "knee"],
	shin: ["knee", "ankle"],
	foot: ["ankle", "toe"],
};
const jtOf = (n: string, s: string, end: 0 | 1) => jt(JOINT_OF[n]![end] + s);

// (The pieces of his trunk: pelvis to chest.)
const TRUNK = BODY_META.bones.map(
	(n) => n === "pelvis" || n.startsWith("spine") || n === "chest",
);
// Each piece of him at rest (as bake.py has it): his spine the torso's
// frame, his neck the neck's, his shoulders their arms'.
const REST_OF: F3[] = BODY_META.bones.map((n) =>
	n === "pelvis" || n.startsWith("spine") || n === "chest"
		? REST.pelvis!
		: n.startsWith("neck") || n === "head"
			? REST.neck!
			: n.startsWith("shoulder")
				? REST[`upper${n.slice(-1)}`]!
				: REST[n]!,
);
const CHEST = BODY_META.bones.indexOf("chest");
const C0 = REST_OF[CHEST]!;
// Each piece's turn and move from rest to posed, as a dual quaternion
// (rotation w x y z, then its dual part) - so a point held by two pieces is
// carried by a turn between theirs, not to a point between where each would
// carry it: an arm raised over his head doesn't fold the skin of his
// armpit in on itself.
let dq = new Float64Array(0);
const turnsOf = (bones: Posed[], H: number) => {
	if (dq.length < bones.length * 8) {
		dq = new Float64Array(bones.length * 8);
	}
	for (let i = 0; i < bones.length; i++) {
		const P = bones[i]!.fr;
		const R0 = REST_OF[i]!;
		// R = posed axes x rest axes, transposed (f, s, u as x, y, z).
		const col = (k: "a" | "f" | "s", r: "f" | "s" | "u", c: "f" | "s" | "u") =>
			P[k][r] * R0[k][c];
		const m = (r: "f" | "s" | "u", c: "f" | "s" | "u") =>
			col("a", r, c) + col("f", r, c) + col("s", r, c);
		const m00 = m("f", "f");
		const m01 = m("f", "s");
		const m02 = m("f", "u");
		const m10 = m("s", "f");
		const m11 = m("s", "s");
		const m12 = m("s", "u");
		const m20 = m("u", "f");
		const m21 = m("u", "s");
		const m22 = m("u", "u");
		let w: number;
		let x: number;
		let y: number;
		let z: number;
		const tr = m00 + m11 + m22;
		if (tr > 0) {
			const k = 0.5 / Math.sqrt(tr + 1);
			w = 0.25 / k;
			x = (m21 - m12) * k;
			y = (m02 - m20) * k;
			z = (m10 - m01) * k;
		} else if (m00 > m11 && m00 > m22) {
			const k = 2 * Math.sqrt(1 + m00 - m11 - m22);
			w = (m21 - m12) / k;
			x = 0.25 * k;
			y = (m01 + m10) / k;
			z = (m02 + m20) / k;
		} else if (m11 > m22) {
			const k = 2 * Math.sqrt(1 + m11 - m00 - m22);
			w = (m02 - m20) / k;
			x = (m01 + m10) / k;
			y = 0.25 * k;
			z = (m12 + m21) / k;
		} else {
			const k = 2 * Math.sqrt(1 + m22 - m00 - m11);
			w = (m10 - m01) / k;
			x = (m02 + m20) / k;
			y = (m12 + m21) / k;
			z = 0.25 * k;
		}
		const ql = Math.sqrt(w * w + x * x + y * y + z * z) || 1;
		w /= ql;
		x /= ql;
		y /= ql;
		z /= ql;
		// Its move: where it is, less where the turn takes it from rest.
		const hx = R0.h.f * H;
		const hy = R0.h.s * H;
		const hz = R0.h.u * H;
		const tx = P.h.f - (m00 * hx + m01 * hy + m02 * hz);
		const ty = P.h.s - (m10 * hx + m11 * hy + m12 * hz);
		const tz = P.h.u - (m20 * hx + m21 * hy + m22 * hz);
		const o = i * 8;
		dq[o] = w;
		dq[o + 1] = x;
		dq[o + 2] = y;
		dq[o + 3] = z;
		// (Half of (0, t) times the turn.)
		dq[o + 4] = -0.5 * (tx * x + ty * y + tz * z);
		dq[o + 5] = 0.5 * (w * tx + (ty * z - tz * y));
		dq[o + 6] = 0.5 * (w * ty + (tz * x - tx * z));
		dq[o + 7] = 0.5 * (w * tz + (tx * y - ty * x));
	}
};
// Where every point of him is, posed (his frame, feet), into `out` - and,
// into `trunk`, where his trunk alone would carry it (what his arms and
// neck hold of it, carried as his chest is): where on his torso it is,
// however his arms pull it (his jersey's edges kept to it).
export const skinned = (
	bones: Posed[],
	H: number,
	out: Float32Array,
	trunk?: Float32Array,
) => {
	const m = meshOf();
	turnsOf(bones, H);
	for (let i = 0; i < m.n; i++) {
		// Him at rest, each piece stretched as it is posed - and the turns
		// that carry him, blended.
		let yx = 0;
		let yy = 0;
		let yz = 0;
		let rw = 0;
		let rx = 0;
		let ry = 0;
		let rz = 0;
		let dw = 0;
		let dx = 0;
		let dy = 0;
		let dz = 0;
		let tf = 0;
		let ts = 0;
		let tu = 0;
		let tw = 0;
		const j0 = m.start[i]!;
		const ref = m.bone[j0]! * 8;
		for (let j = j0; j < m.start[i + 1]!; j++) {
			const bi = m.bone[j]!;
			const b = bones[bi]!;
			const R0 = REST_OF[bi]!;
			const w = m.weight[j]!;
			const al = m.local[j * 3]! * b.ax;
			const fw = m.local[j * 3 + 1]! * b.rad;
			const sd = m.local[j * 3 + 2]! * b.rad;
			const px = (R0.h.f + R0.a.f * al + R0.f.f * fw + R0.s.f * sd) * H;
			const py = (R0.h.s + R0.a.s * al + R0.f.s * fw + R0.s.s * sd) * H;
			const pz = (R0.h.u + R0.a.u * al + R0.f.u * fw + R0.s.u * sd) * H;
			yx += w * px;
			yy += w * py;
			yz += w * pz;
			const o = bi * 8;
			// (Each turn the short way round from the first's.)
			const sg =
				dq[o]! * dq[ref]! +
					dq[o + 1]! * dq[ref + 1]! +
					dq[o + 2]! * dq[ref + 2]! +
					dq[o + 3]! * dq[ref + 3]! <
				0
					? -w
					: w;
			rw += sg * dq[o]!;
			rx += sg * dq[o + 1]!;
			ry += sg * dq[o + 2]!;
			rz += sg * dq[o + 3]!;
			dw += sg * dq[o + 4]!;
			dx += sg * dq[o + 5]!;
			dy += sg * dq[o + 6]!;
			dz += sg * dq[o + 7]!;
			if (trunk) {
				if (TRUNK[bi]) {
					const fr = b.fr;
					const la = al * H;
					const lf = fw * H;
					const ls = sd * H;
					tf += w * (fr.h.f + fr.a.f * la + fr.f.f * lf + fr.s.f * ls);
					ts += w * (fr.h.s + fr.a.s * la + fr.f.s * lf + fr.s.s * ls);
					tu += w * (fr.h.u + fr.a.u * la + fr.f.u * lf + fr.s.u * ls);
				} else {
					// (Held by his arm, or his neck: where it is at rest, carried
					// by his chest.)
					const l0 = m.local[j * 3]!;
					const l1 = m.local[j * 3 + 1]!;
					const l2 = m.local[j * 3 + 2]!;
					const rx = R0.h.f + R0.a.f * l0 + R0.f.f * l1 + R0.s.f * l2 - C0.h.f;
					const ry = R0.h.s + R0.a.s * l0 + R0.f.s * l1 + R0.s.s * l2 - C0.h.s;
					const rz = R0.h.u + R0.a.u * l0 + R0.f.u * l1 + R0.s.u * l2 - C0.h.u;
					const c = bones[CHEST]!;
					const la = (rx * C0.a.f + ry * C0.a.s + rz * C0.a.u) * H * c.ax;
					const lf = (rx * C0.f.f + ry * C0.f.s + rz * C0.f.u) * H * c.rad;
					const ls = (rx * C0.s.f + ry * C0.s.s + rz * C0.s.u) * H * c.rad;
					const fr = c.fr;
					tf += w * (fr.h.f + fr.a.f * la + fr.f.f * lf + fr.s.f * ls);
					ts += w * (fr.h.s + fr.a.s * la + fr.f.s * lf + fr.s.s * ls);
					tu += w * (fr.h.u + fr.a.u * la + fr.f.u * lf + fr.s.u * ls);
				}
				tw += w;
			}
		}
		const n = Math.sqrt(rw * rw + rx * rx + ry * ry + rz * rz) || 1;
		rw /= n;
		rx /= n;
		ry /= n;
		rz /= n;
		dw /= n;
		dx /= n;
		dy /= n;
		dz /= n;
		// The point turned...
		const cx = ry * yz - rz * yy + rw * yx;
		const cy = rz * yx - rx * yz + rw * yy;
		const cz = rx * yy - ry * yx + rw * yz;
		const f = yx + 2 * (ry * cz - rz * cy);
		const s = yy + 2 * (rz * cx - rx * cz);
		const u = yz + 2 * (rx * cy - ry * cx);
		// ... and moved.
		out[i * 3] = f + 2 * (rw * dx - dw * rx + (ry * dz - rz * dy));
		out[i * 3 + 1] = s + 2 * (rw * dy - dw * ry + (rz * dx - rx * dz));
		out[i * 3 + 2] = u + 2 * (rw * dz - dw * rz + (rx * dy - ry * dx));
		if (trunk) {
			trunk[i * 3] = tf / tw;
			trunk[i * 3 + 1] = ts / tw;
			trunk[i * 3 + 2] = tu / tw;
		}
	}
};

// ---- drawn ---------------------------------------------------------------------

export type MeshDraw = {
	// Where a point of him lands on the sprite (pixels) and how far it is
	// from the camera.
	see: (v: V3) => { x: number; y: number; depth: number };
	// The camera's right, down and toward it, in his frame.
	rt: V3;
	dn: V3;
	toward: V3;
	w: number;
	h: number;
	// What he is painted with at a point of him: the colour into `rgb`,
	// whether it is cloth, how much lighter or darker (a fold) - and which
	// part of him it is (its group, for what is drawn over his head).
	// `arm`: how much that point goes with his arm, not his body (0 - 1);
	// T: where it is moved by his trunk alone (see skinned).
	paint: (
		kind: number,
		Pf: number,
		Ps: number,
		Pu: number,
		arm: number,
		Tf: number,
		Ts: number,
		Tu: number,
	) => number;
	// Which kinds of face are never seen (his bare foot in his shoe), which
	// are cut away in places - and where (`keep` false: nothing of him
	// there, past the top of his shoe, say).
	hidden: readonly boolean[];
	cuts: readonly boolean[];
	keep: (
		kind: number,
		Pf: number,
		Ps: number,
		Pu: number,
		arm: number,
	) => boolean;
	// How much nearer the camera each kind of face is taken to be than it
	// is (feet): what he wears over him, over him wherever the two come close.
	lift: readonly number[];
	// Which kinds of face are his head's (and neck's), and where on his face
	// each point of them is, for `paint` - in faces.js's units, how much it
	// faces forward, how far round his head it is (see bake.py).
	faced: readonly boolean[];
	faceAt: Float32Array;
	ink: { r: number; g: number; b: number; cloth: boolean; tone?: number };
};
// The light (as sculpt.ts's), and how it falls on him.
const LIGHT = (() => {
	const l = [-0.45, -0.7, 0.55];
	const n = Math.hypot(l[0]!, l[1]!, l[2]!);
	return [l[0]! / n, l[1]! / n, l[2]! / n] as const;
})();
const BACK_LIGHT = (() => {
	const l = [0.75, -0.45, -0.2];
	const n = Math.hypot(l[0]!, l[1]!, l[2]!);
	return [l[0]! / n, l[1]! / n, l[2]! / n] as const;
})();
const SHINE_AT = (() => {
	const l = [LIGHT[0], LIGHT[1], LIGHT[2] + 1];
	const n = Math.hypot(l[0]!, l[1]!, l[2]!);
	return [l[0]! / n, l[1]! / n, l[2]! / n] as const;
})();
const SS = 2;
let pos = new Float32Array(0);
let posT = new Float32Array(0);
let away = new Uint8Array(0);

let nrm = new Float32Array(0);
let scr = new Float32Array(0);
let zb = new Float32Array(0);
let cb = new Float32Array(0);
let gb = new Int8Array(0);
let tb = new Int32Array(0);
let wb = new Float32Array(0);
// Him drawn into the sheet `d` (w x h, RGBA): `z` takes how far each pixel
// of him is from the camera (Infinity where he isn't), `owner` its group.
export const drawMesh = (
	bones: Posed[],
	H: number,
	m: MeshDraw,
	d: Uint8ClampedArray,
	z: Float32Array,
	owner?: Int8Array,
): { x0: number; y0: number; x1: number; y1: number } => {
	const me = meshOf();
	const n = me.n;
	if (pos.length < n * 3) {
		pos = new Float32Array(n * 3);
		posT = new Float32Array(n * 3);
		nrm = new Float32Array(n * 3);
		scr = new Float32Array(n * 3);
	}
	skinned(bones, H, pos, posT);
	// Normals, from his faces as they are now.
	nrm.fill(0, 0, n * 3);
	const T = me.tris;
	const nt = T.length / 3;
	if (away.length < nt) {
		away = new Uint8Array(nt);
	}
	for (let t = 0; t < nt; t++) {
		const a = T[t * 3]!;
		const b = T[t * 3 + 1]!;
		const c = T[t * 3 + 2]!;
		const ux = pos[b * 3]! - pos[a * 3]!;
		const uy = pos[b * 3 + 1]! - pos[a * 3 + 1]!;
		const uz = pos[b * 3 + 2]! - pos[a * 3 + 2]!;
		const vx = pos[c * 3]! - pos[a * 3]!;
		const vy = pos[c * 3 + 1]! - pos[a * 3 + 1]!;
		const vz = pos[c * 3 + 2]! - pos[a * 3 + 2]!;
		const nx = uy * vz - uz * vy;
		const ny = uz * vx - ux * vz;
		const nz = ux * vy - uy * vx;
		nrm[a * 3] = nrm[a * 3]! + nx;
		nrm[a * 3 + 1] = nrm[a * 3 + 1]! + ny;
		nrm[a * 3 + 2] = nrm[a * 3 + 2]! + nz;
		nrm[b * 3] = nrm[b * 3]! + nx;
		nrm[b * 3 + 1] = nrm[b * 3 + 1]! + ny;
		nrm[b * 3 + 2] = nrm[b * 3 + 2]! + nz;
		nrm[c * 3] = nrm[c * 3]! + nx;
		nrm[c * 3 + 1] = nrm[c * 3 + 1]! + ny;
		nrm[c * 3 + 2] = nrm[c * 3 + 2]! + nz;
		// (Turned away from the camera, the face itself - not its corners'
		// normals, which a fold of him can turn: behind the rest of him.)
		const tw = nx * m.toward.f + ny * m.toward.s + nz * m.toward.u;
		away[t] = tw < -0.15 * Math.sqrt(nx * nx + ny * ny + nz * nz) ? 1 : 0;
	}
	// Onto the sprite (at SS times its pixels), and the normals into the
	// camera's frame.
	const W2 = m.w * SS;
	const H2 = m.h * SS;
	for (let i = 0; i < n; i++) {
		const p = m.see(v3(pos[i * 3]!, pos[i * 3 + 1]!, pos[i * 3 + 2]!));
		scr[i * 3] = p.x * SS;
		scr[i * 3 + 1] = p.y * SS;
		scr[i * 3 + 2] = p.depth;
		const nf = nrm[i * 3]!;
		const ns = nrm[i * 3 + 1]!;
		const nu = nrm[i * 3 + 2]!;
		const x = nf * m.rt.f + ns * m.rt.s + nu * m.rt.u;
		const y = nf * m.dn.f + ns * m.dn.s + nu * m.dn.u;
		const zz = nf * m.toward.f + ns * m.toward.s + nu * m.toward.u;
		const l = Math.sqrt(x * x + y * y + zz * zz) || 1;
		nrm[i * 3] = x / l;
		nrm[i * 3 + 1] = y / l;
		nrm[i * 3 + 2] = zz / l;
	}
	if (zb.length < W2 * H2) {
		zb = new Float32Array(W2 * H2);
		cb = new Float32Array(W2 * H2 * 3);
		gb = new Int8Array(W2 * H2);
		tb = new Int32Array(W2 * H2);
		wb = new Float32Array(W2 * H2 * 2);
	}
	zb.fill(Infinity, 0, W2 * H2);
	gb.fill(-1, 0, W2 * H2);
	// First where he is - the face of him nearest the camera at each pixel,
	// and where on it - then each of those painted and lit, once.
	for (let t = 0; t < nt; t++) {
		const kind = me.kind[t]!;
		if (m.hidden[kind]) {
			continue;
		}
		const a = T[t * 3]!;
		const b = T[t * 3 + 1]!;
		const c = T[t * 3 + 2]!;
		if (away[t]) {
			continue;
		}
		const ax = scr[a * 3]!;
		const ay = scr[a * 3 + 1]!;
		const bx = scr[b * 3]!;
		const by = scr[b * 3 + 1]!;
		const cx = scr[c * 3]!;
		const cy = scr[c * 3 + 1]!;
		const den = (by - cy) * (ax - cx) + (cx - bx) * (ay - cy);
		if (Math.abs(den) < 1e-9) {
			continue;
		}
		const x0 = Math.max(0, Math.floor(Math.min(ax, bx, cx)));
		const x1 = Math.min(W2 - 1, Math.ceil(Math.max(ax, bx, cx)));
		const y0 = Math.max(0, Math.floor(Math.min(ay, by, cy)));
		const y1 = Math.min(H2 - 1, Math.ceil(Math.max(ay, by, cy)));
		if (x0 > x1 || y0 > y1) {
			continue;
		}
		const cuts = m.cuts[kind];
		const lift = m.lift[kind]!;
		const az = scr[a * 3 + 2]! - lift;
		const bz = scr[b * 3 + 2]! - lift;
		const cz = scr[c * 3 + 2]! - lift;
		const id = 1 / den;
		for (let y = y0; y <= y1; y++) {
			const py = y + 0.5;
			for (let x = x0; x <= x1; x++) {
				const pxx = x + 0.5;
				const w0 = ((by - cy) * (pxx - cx) + (cx - bx) * (py - cy)) * id;
				if (w0 < 0) {
					continue;
				}
				const w1 = ((cy - ay) * (pxx - cx) + (ax - cx) * (py - cy)) * id;
				const w2 = 1 - w0 - w1;
				if (w1 < 0 || w2 < 0) {
					continue;
				}
				const dz = w0 * az + w1 * bz + w2 * cz;
				const o = y * W2 + x;
				if (dz >= zb[o]!) {
					continue;
				}
				if (cuts) {
					// (Cut away there - past the top of his shoe, say: what is
					// behind it shows.)
					const Pf = w0 * pos[a * 3]! + w1 * pos[b * 3]! + w2 * pos[c * 3]!;
					const Ps =
						w0 * pos[a * 3 + 1]! + w1 * pos[b * 3 + 1]! + w2 * pos[c * 3 + 1]!;
					const Pu =
						w0 * pos[a * 3 + 2]! + w1 * pos[b * 3 + 2]! + w2 * pos[c * 3 + 2]!;
					const arm =
						(w0 * me.arm[a]! + w1 * me.arm[b]! + w2 * me.arm[c]!) / 255;
					if (!m.keep(kind, Pf, Ps, Pu, arm)) {
						continue;
					}
				}
				zb[o] = dz;
				tb[o] = t;
				wb[o * 2] = w0;
				wb[o * 2 + 1] = w1;
			}
		}
	}
	const ink = m.ink;
	for (let o = 0; o < W2 * H2; o++) {
		if (zb[o] === Infinity) {
			continue;
		}
		const t = tb[o]!;
		const a = T[t * 3]!;
		const b = T[t * 3 + 1]!;
		const c = T[t * 3 + 2]!;
		const w0 = wb[o * 2]!;
		const w1 = wb[o * 2 + 1]!;
		const w2 = 1 - w0 - w1;
		const kind = me.kind[t]!;
		// The point of him there, painted and lit.
		const Pf = w0 * pos[a * 3]! + w1 * pos[b * 3]! + w2 * pos[c * 3]!;
		const Ps =
			w0 * pos[a * 3 + 1]! + w1 * pos[b * 3 + 1]! + w2 * pos[c * 3 + 1]!;
		const Pu =
			w0 * pos[a * 3 + 2]! + w1 * pos[b * 3 + 2]! + w2 * pos[c * 3 + 2]!;
		const arm = (w0 * me.arm[a]! + w1 * me.arm[b]! + w2 * me.arm[c]!) / 255;
		if (m.faced[kind]) {
			const fa = me.face;
			const fo = m.faceAt;
			for (let j = 0; j < 4; j++) {
				fo[j] =
					(w0 * fa[a * 4 + j]! + w1 * fa[b * 4 + j]! + w2 * fa[c * 4 + j]!) /
					(j < 2 ? 8 : 1000);
			}
		}
		const Tf = w0 * posT[a * 3]! + w1 * posT[b * 3]! + w2 * posT[c * 3]!;
		const Ts =
			w0 * posT[a * 3 + 1]! + w1 * posT[b * 3 + 1]! + w2 * posT[c * 3 + 1]!;
		const Tu =
			w0 * posT[a * 3 + 2]! + w1 * posT[b * 3 + 2]! + w2 * posT[c * 3 + 2]!;
		ink.tone = 0;
		// (`paint` says which part of him it is: for what is drawn over his
		// head - see sculpt.ts.)
		gb[o] = m.paint(kind, Pf, Ps, Pu, arm, Tf, Ts, Tu);
		let sx = w0 * nrm[a * 3]! + w1 * nrm[b * 3]! + w2 * nrm[c * 3]!;
		let sy = w0 * nrm[a * 3 + 1]! + w1 * nrm[b * 3 + 1]! + w2 * nrm[c * 3 + 1]!;
		let sz = w0 * nrm[a * 3 + 2]! + w1 * nrm[b * 3 + 2]! + w2 * nrm[c * 3 + 2]!;
		const nl = Math.sqrt(sx * sx + sy * sy + sz * sz) || 1;
		sx /= nl;
		sy /= nl;
		sz /= nl;
		const cloth = ink.cloth;
		const lam = sx * LIGHT[0] + sy * LIGHT[1] + sz * LIGHT[2];
		const key = cloth ? Math.max(0, lam) : Math.max(0, (lam + 0.32) / 1.32);
		const amb = 0.34 + 0.22 * (0.5 - 0.5 * sy);
		const k = (amb + 0.66 * key) * (1 + (ink.tone ?? 0));
		const shade1 = 1 - key;
		let r = ink.r * k;
		let g = ink.g * k * (cloth ? 1 : 1 - 0.06 * shade1);
		let bb = ink.b * k * (cloth ? 1 : 1 - 0.1 * shade1);
		const e3 = Math.min(
			1,
			Math.max(
				0,
				(sx * BACK_LIGHT[0] + sy * BACK_LIGHT[1] + sz * BACK_LIGHT[2] - 0.55) /
					0.4,
			),
		);
		let lift = e3 * e3 * (3 - 2 * e3) * shade1 * (cloth ? 0.11 : 0.15);
		const h0 = sx * SHINE_AT[0] + sy * SHINE_AT[1] + sz * SHINE_AT[2];
		if (h0 > 0) {
			lift += Math.pow(h0, cloth ? 6 : 18) * (cloth ? 0.05 : 0.2);
		}
		if (lift > 0) {
			const l = Math.min(1, lift);
			r += (255 - r) * l;
			g += (255 - g) * l;
			bb += (255 - bb) * l;
		}
		cb[o * 3] = r;
		cb[o * 3 + 1] = g;
		cb[o * 3 + 2] = bb;
	}
	// Down to the sprite's own pixels: each the share of its SS x SS that he
	// covers, the nearest of them its depth.
	let bx0 = m.w;
	let by0 = m.h;
	let bx1 = -1;
	let by1 = -1;
	const inv = 1 / (SS * SS);
	for (let y = 0; y < m.h; y++) {
		for (let x = 0; x < m.w; x++) {
			let cov = 0;
			let r = 0;
			let g = 0;
			let bb = 0;
			let near = Infinity;
			let grp = -1;
			for (let j = 0; j < SS; j++) {
				for (let i = 0; i < SS; i++) {
					const o = (y * SS + j) * W2 + x * SS + i;
					const zz = zb[o]!;
					if (zz === Infinity) {
						continue;
					}
					cov++;
					r += cb[o * 3]!;
					g += cb[o * 3 + 1]!;
					bb += cb[o * 3 + 2]!;
					if (zz < near) {
						near = zz;
						grp = gb[o]!;
					}
				}
			}
			if (cov === 0) {
				continue;
			}
			const q = y * m.w + x;
			const o4 = q * 4;
			d[o4] = r / cov;
			d[o4 + 1] = g / cov;
			d[o4 + 2] = bb / cov;
			d[o4 + 3] = 255 * cov * inv;
			z[q] = near;
			if (owner) {
				owner[q] = grp;
			}
			if (x < bx0) {
				bx0 = x;
			}
			if (x > bx1) {
				bx1 = x;
			}
			if (y < by0) {
				by0 = y;
			}
			if (y > by1) {
				by1 = y;
			}
		}
	}
	return { x0: bx0, y0: by0, x1: bx1, y1: by1 };
};
