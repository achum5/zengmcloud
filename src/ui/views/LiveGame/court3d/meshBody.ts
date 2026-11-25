import { BODY_META } from "./bodyMesh.meta.ts";
import type { Body, Limb, Skeleton, V3 } from "./poses.ts";
import { HEM_TODAY } from "./shortsEra.ts";

// HIS BODY AS ONE SKIN - a mesh of him (the court's own player built in
// Blender: see tools/court3d-body), not masses laid along his bones.
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
export type Mesh = {
	n: number;
	start: Uint32Array;
	counts: Uint8Array;
	bone: Uint8Array;
	weight: Float32Array;
	local: Float32Array;
	tris: Uint16Array;
	kind: Uint8Array;
	arm: Uint8Array;
	// How much each point stays with his trunk as his arm goes up (0 - 1),
	// and which arm's (0 his right, 1 his left) - see underArm.
	pit: Float32Array;
	pitSide: Uint8Array;
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
		const bone = unpack(D.bone, (b) => new Uint8Array(b));
		const arm = armOf(start, bone, weight);
		const pit = underArm(start, bone, weight, local, arm);
		mesh = {
			n: BODY_META.points,
			start,
			counts,
			bone,
			weight,
			local,
			tris: unpack(D.tris, (b) => new Uint16Array(b)),
			kind: unpack(D.kind, (b) => new Uint8Array(b)),
			arm,
			pit: pit.pit,
			pitSide: pit.side,
			face: unpack(D.face, (b) => new Int16Array(b)),
		};
	}));
// How much each point goes with his arm, not his body (0 - 255): his
// jersey is cut where it does (see sculpt.ts). His upper arm, forearm and
// hand all his arm's; his shoulder - the top of his chest, the front of his
// armpit, his collarbone's end - only partly, so his jersey covers his chest
// out to his deltoid, and up over his shoulders, as a fitted one does.
// (Not the bake's own, which made more of his shoulder his arm's.)
const SHOULDER_ARM = 0.3;
const ARM_BONES = BODY_META.bones.map((b) =>
	/^(upper|fore|hand)/.test(b)
		? 1
		: b.startsWith("shoulder")
			? SHOULDER_ARM
			: 0,
);
const armOf = (start: Uint32Array, bone: Uint8Array, weight: Float32Array) => {
	const arm = new Uint8Array(start.length - 1);
	for (let i = 0; i < arm.length; i++) {
		let a = 0;
		for (let j = start[i]!; j < start[i + 1]!; j++) {
			a += weight[j]! * ARM_BONES[bone[j]!]!;
		}
		arm[i] = Math.round(Math.min(1, a) * 255);
	}
	return arm;
};
const meshOf = (): Mesh => {
	if (!mesh) {
		throw new Error("mesh not loaded");
	}
	return mesh;
};
// (The mesh as it is, for the card to be handed once - see meshGl.ts.)
export const meshData = (): Mesh | undefined => mesh;
export const MESH_KINDS: readonly string[] = BODY_META.kinds;
export type MeshKind = (typeof BODY_META.kinds)[number];

// ---- frames --------------------------------------------------------------------

export type F3 = { h: V3; a: V3; f: V3; s: V3 };
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

// EACH PIECE OF HIM, POSED (the pieces Blender weights his skeleton
// to): its frame, and how far it is stretched along it and round it - for
// his skeleton and his build. His spine turns a little at a time from his
// hips to his shoulders, his neck the same up to his head; his shoulders go
// half with his chest and half with his arms.
export type Posed = { fr: F3; ax: number; rad: number };
// An arm the court has stretched past its own length - thrown up over his
// head, or a jumper's up under the ball (see stretchOf in poses.ts) - is
// drawn near enough its own length, his shoulder come up toward his hand
// instead, as a man's does reaching up (his shoulder blade turned up under
// it): the hand where the court has it, the arm not drawn out long and
// thin. (This share of the stretch taken up at the shoulder.)
const SHOULDER_TAKES = 0.75;
export const ownArm = (l: Limb, upper: number, fore: number): Limb => {
	const a = len(sub(l.mid, l.root));
	const b = len(sub(l.end, l.mid));
	const over = (a + b - (upper + fore)) * SHOULDER_TAKES;
	if (over <= 1e-3) {
		return l;
	}
	const reach = sub(l.end, l.root);
	const d = len(reach);
	if (d < 1e-6) {
		return l;
	}
	const k = a / (a + b);
	const u = scale(reach, 1 / d);
	const root = addV(l.root, scale(u, over));
	// (The elbow where the arm, now that long, bends to: on the side it
	// bent to before.)
	const A = a - over * k;
	const B = b - over * (1 - k);
	const D = Math.max(Math.abs(A - B) + 1e-6, Math.min(A + B, d - over));
	const along = (A * A - B * B + D * D) / (2 * D);
	const h = Math.sqrt(Math.max(0, A * A - along * along));
	const side = unit(ortho(sub(l.mid, root), u), ortho(v3(0, 0, 1), u));
	const mid = addV(root, addV(scale(u, along), scale(side, h)));
	return { ...l, root, mid };
};
// HIS NECK UNDER HIS HEAD (the court's own head, drawn over him - see
// figure.ts drawHead): up into it, so far short of its middle (head radii),
// not stopping under his jaw - side on, his head sits on it rather than
// perched on the end of it - and so much thicker than the mesh's own, under
// a head drawn as big as his is.
const NECK_UNDER = 0.2;
const NECK_GIRTH = 1.2;
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
		(len(sub(sk.head, sk.chest)) - body.headR * NECK_UNDER) /
			(restLen("chest", "head") * H),
	);
	const neckG = girth * NECK_GIRTH;
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
			rad: neckG,
		},
		neck2: { fr: neck, ax: neckAx, rad: neckG },
		head: { fr: neck, ax: neckAx, rad: neckG },
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
	const armR = ownArm(sk.armR, body.upper, body.fore);
	const armL = ownArm(sk.armL, body.upper, body.fore);
	limb(armR, "R", ["upper", "fore", undefined], chest, REST.chest!, [
		upperG,
		foreG,
		foreG,
	]);
	limb(armL, "L", ["upper", "fore", undefined], chest, REST.chest!, [
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

// A joint of him at rest (feet, for a man `H` tall).
export const restJoint = (name: string, H: number): V3 => scale(jt(name), H);
// Which piece is which (BODY_META's order).
export const boneIndex = (name: string): number =>
	BODY_META.bones.indexOf(name as (typeof BODY_META.bones)[number]);
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
// HIS RIBS STAY PUT UNDER A RAISED ARM. Blender's weights give the side of
// his chest under his arm, and the top of it front and back, partly to his
// shoulder and upper arm - and his shoulder turns about the top of his arm
// (see poseBones). An arm thrown up over his head swung all of that out
// from his side with it: his ribs spread into a wing up to his elbow, his
// jersey stretched across it, as no man's are. A man's ribcage stays where
// it is whatever his arm does - his armpit opens, his lats drawn up into
// his arm. So each point of him painted as his trunk (see armOf), from the
// tops of his shoulders down, stays where his trunk alone carries it (see
// skinned), more the higher his arm is raised past how it hangs as made:
// his jersey and its armholes as they are on him standing. His arm's own
// skin goes with his arm, as before, and what stretches is his armpit.
// (How far below his shoulder joint a point is all his trunk's - up over
// his shoulder it is less and less, none of it at the first, for a man 1
// foot tall; how much of it goes with his arm before it is all his arm's -
// his jersey's edge at the first; and how far past the way his arm hangs
// as made it is raised before his ribs are all his own: radians.)
const PIT_BELOW = [-0.07, -0.01] as const;
const PIT_ARM = [0.5, 0.85] as const;
const PIT_RAISE = (80 * Math.PI) / 180;
const smoothStep = (r: readonly [number, number], x: number) => {
	const t = Math.min(1, Math.max(0, (x - r[0]) / (r[1] - r[0])));
	return t * t * (3 - 2 * t);
};
const ARM_HELD = BODY_META.bones.map((b) => /^(shoulder|upper)/.test(b));
const underArm = (
	start: Uint32Array,
	bone: Uint8Array,
	weight: Float32Array,
	local: Float32Array,
	arm: Uint8Array,
) => {
	const n = start.length - 1;
	const pit = new Float32Array(n);
	const side = new Uint8Array(n);
	for (let i = 0; i < n; i++) {
		let s = 0;
		let u = 0;
		let held = 0;
		for (let j = start[i]!; j < start[i + 1]!; j++) {
			const R = REST_OF[bone[j]!]!;
			const w = weight[j]!;
			const l0 = local[j * 3]!;
			const l1 = local[j * 3 + 1]!;
			const l2 = local[j * 3 + 2]!;
			s += w * (R.h.s + R.a.s * l0 + R.f.s * l1 + R.s.s * l2);
			u += w * (R.h.u + R.a.u * l0 + R.f.u * l1 + R.s.u * l2);
			if (ARM_HELD[bone[j]!]) {
				held += w;
			}
		}
		if (held <= 0) {
			continue;
		}
		side[i] = s > 0 ? 1 : 0;
		const sh = jt(s > 0 ? "shoulderL" : "shoulderR");
		pit[i] =
			smoothStep(PIT_BELOW, sh.u - u) *
			(1 - smoothStep(PIT_ARM, arm[i]! / 255));
	}
	return { pit, side };
};
// How far each arm is raised past the way it hangs as made (0 - 1, see
// PIT_RAISE): the angle down from it to his trunk's down.
const UPPER_OF = [
	BODY_META.bones.indexOf("upperR"),
	BODY_META.bones.indexOf("upperL"),
] as const;
const HUNG = Math.acos(-dot(REST.upperR!.a, REST.chest!.a));
const raisedOf = (bones: readonly Posed[], side: 0 | 1, chest: F3) => {
	const a = bones[UPPER_OF[side]]!.fr.a;
	const down = Math.acos(Math.min(1, Math.max(-1, -dot(a, chest.a))));
	return smoothStep([0, PIT_RAISE], down - HUNG);
};
// A point of him as posed, back where it is on him at rest (feet, stood
// as the mesh was made) - carried by piece `i` alone: where on his shoe a
// point of it is, however his foot is pointed.
export const atRest = (
	bones: readonly Posed[],
	i: number,
	H: number,
	f: number,
	s: number,
	u: number,
): V3 => {
	const b = bones[i]!;
	const R = REST_OF[i]!;
	const d = v3(f - b.fr.h.f, s - b.fr.h.s, u - b.fr.h.u);
	const l0 = dot(d, b.fr.a) / (H * b.ax);
	const l1 = dot(d, b.fr.f) / (H * b.rad);
	const l2 = dot(d, b.fr.s) / (H * b.rad);
	return v3(
		(R.h.f + R.a.f * l0 + R.f.f * l1 + R.s.f * l2) * H,
		(R.h.s + R.a.s * l0 + R.f.s * l1 + R.s.s * l2) * H,
		(R.h.u + R.a.u * l0 + R.f.u * l1 + R.s.u * l2) * H,
	);
};
// A piece of him at rest (feet, for a man 1 foot tall): where it starts,
// and which way along it, forward and to his side are (see atRest).
export const restOf = (i: number): F3 => REST_OF[i]!;
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
// (Each piece at rest as plain numbers - its middle, then its axes - and
// each piece as posed likewise, after its length and girth: read for every
// point of him, every picture.)
const REST_FLAT = new Float64Array(REST_OF.length * 12);
for (const [i, R] of REST_OF.entries()) {
	REST_FLAT.set(
		[
			R.h.f,
			R.h.s,
			R.h.u,
			R.a.f,
			R.a.s,
			R.a.u,
			R.f.f,
			R.f.s,
			R.f.u,
			R.s.f,
			R.s.s,
			R.s.u,
		],
		i * 12,
	);
}
let posedFlat = new Float64Array(0);

// HIS SHORTS AS LONG AS HIS SEASON'S (see shortsEra.ts): the mesh is made
// with today's, so the legs of them are drawn up his thighs or let down to
// his knees - the shorts' every ring of points slid along his thigh to fit
// between his hips and the new hem, the step in under the hem kept as
// sharp as it was, and his bare thigh below it his thigh's own shape, as
// thick there as a thigh is (thicker up toward his hip). `hem` is of the
// way from his hip to his knee, today's HEM_TODAY; `flare`, how loose
// round his thigh the hem is (1: today's).
// (From his hip down, how much of his shorts is never moved: where they
// meet his hips.)
const HEM_FIXED = 0.2;
// (Today's shorts as made are baggy - nearly twice his thigh round at the
// hem: their legs this much of that, a fitted pair's - but never tighter
// than the shortest's, which his thigh fills.)
const SHORTS_FIT = 0.9;
const SHORTS_TIGHTEST = 0.82;
const locals = new Map<string, Float32Array>();
let localsOf: Mesh | undefined;
const localFor = (m: Mesh, hem: number, flare: number): Float32Array => {
	if (localsOf !== m) {
		locals.clear();
		localsOf = m;
	}
	// (A step of a hundredth: one length per era, not one per season.)
	const h = Math.round(hem * 100) / 100;
	const fl =
		Math.round(Math.max(SHORTS_TIGHTEST, flare * SHORTS_FIT) * 100) / 100;
	const key = `${h} ${fl}`;
	const had = locals.get(key);
	if (had) {
		return had;
	}
	// Each point at rest.
	const rest = new Float64Array(m.n * 3);
	for (let i = 0; i < m.n; i++) {
		let f = 0;
		let s = 0;
		let u = 0;
		for (let j = m.start[i]!; j < m.start[i + 1]!; j++) {
			const R = REST_OF[m.bone[j]!]!;
			const w = m.weight[j]!;
			const l0 = m.local[j * 3]!;
			const l1 = m.local[j * 3 + 1]!;
			const l2 = m.local[j * 3 + 2]!;
			f += w * (R.h.f + R.a.f * l0 + R.f.f * l1 + R.s.f * l2);
			s += w * (R.h.s + R.a.s * l0 + R.f.s * l1 + R.s.s * l2);
			u += w * (R.h.u + R.a.u * l0 + R.f.u * l1 + R.s.u * l2);
		}
		rest[i * 3] = f;
		rest[i * 3 + 1] = s;
		rest[i * 3 + 2] = u;
	}
	const local = new Float32Array(m.local);
	for (const side of ["R", "L"] as const) {
		const thigh = boneIndex(`thigh${side}`);
		const hip = jt(`hip${side}`);
		const ax = unit(sub(jt(`knee${side}`), hip), v3(0, 0, -1));
		const L = restLen(`hip${side}`, `knee${side}`);
		// Each point of this leg (held by its thigh): how far down it
		// (hip 0, knee 1) and out from it (thigh lengths).
		const pts: { i: number; a: number; r: number; o: V3 }[] = [];
		for (let i = 0; i < m.n; i++) {
			let held = false;
			for (let j = m.start[i]!; j < m.start[i + 1]!; j++) {
				held ||= m.bone[j] === thigh;
			}
			const p = v3(rest[i * 3]!, rest[i * 3 + 1]!, rest[i * 3 + 2]!);
			if (!held || (side === "R" ? p.s > 0 : p.s < 0)) {
				continue;
			}
			const d = sub(p, hip);
			const a = dot(d, ax) / L;
			const o = ortho(d, ax);
			const r = len(o) / L;
			if (a >= HEM_FIXED && a <= 1 && r < 0.5) {
				pts.push({ i, a, r, o });
			}
		}
		// Where the shorts end as made (the last of their wide rings) and
		// his thigh begins under them.
		let g0 = 0;
		for (const p of pts) {
			if (p.r > 0.25 && p.a > g0) {
				g0 = p.a;
			}
		}
		let g1 = 1;
		for (const p of pts) {
			if (p.r < 0.2 && p.a > g0 && p.a < g1) {
				g1 = p.a;
			}
		}
		if (g0 <= HEM_FIXED || g1 <= g0) {
			continue;
		}
		const to = g0 + (h - HEM_TODAY);
		const step = Math.min(g1 - g0, Math.max(0.01, (0.98 - to) / 2));
		// (A thigh's girth down it: thicker up toward his hip.)
		const girth = (a: number) => 1 + 0.6 * (1 - a);
		for (const p of pts) {
			let a = p.a;
			let k = 1;
			if (p.a <= g0) {
				const q = (p.a - HEM_FIXED) / (g0 - HEM_FIXED);
				a = HEM_FIXED + q * (to - HEM_FIXED);
				k = 1 + (fl - 1) * q;
			} else if (p.a <= g1) {
				a = to + ((p.a - g0) / (g1 - g0)) * step;
				k = fl;
			} else {
				a = to + step + ((p.a - g1) / (1 - g1)) * (1 - to - step);
				k = girth(a) / girth(p.a);
			}
			const at = addV(addV(hip, scale(ax, a * L)), scale(p.o, k));
			for (let j = m.start[p.i]!; j < m.start[p.i + 1]!; j++) {
				const R = REST_OF[m.bone[j]!]!;
				const d = sub(at, R.h);
				local[j * 3] = dot(d, R.a);
				local[j * 3 + 1] = dot(d, R.f);
				local[j * 3 + 2] = dot(d, R.s);
			}
		}
	}
	hangJersey(m, rest, local);
	locals.set(key, local);
	return local;
};

// HIS JERSEY, HANGING: the mesh is one skin, his jersey only painted on it -
// so it would cling to every muscle of his stomach and the narrow of his
// waist. A jersey hangs from his chest and his shoulder blades straight
// down to his shorts, tucked in at their waistband: each point of his
// trunk from there up to under his arms is let out to where the jersey
// hangs - as far out round him, every way, as his chest is (a little less
// toward his waist), never in.
// (Up him from the floor, for a man 1 foot tall: the top of his shorts'
// waistband, where the jersey hangs straight from, and how far up that it
// is gathered in.)
const HANG_TUCK = 0.505;
const HANG_FROM = [0.6, 0.67] as const;
const HANG_GATHER = 0.022;
// (How much narrower it is at his waist than at his chest, and the most it
// stands off him anywhere.)
const HANG_TAPER = 0.02;
const HANG_MOST = 0.03;
const HANG_BINS = 48;
const hangJersey = (m: Mesh, rest: Float64Array, local: Float32Array) => {
	const torso = MESH_KINDS.indexOf("torso");
	if (torso < 0) {
		return;
	}
	const onTorso = new Uint8Array(m.n);
	for (let t = 0; t < m.kind.length; t++) {
		if (m.kind[t] === torso) {
			for (let c = 0; c < 3; c++) {
				onTorso[m.tris[t * 3 + c]!] = 1;
			}
		}
	}
	// How far out his chest is, every way round him (from his spine).
	const bin = (f: number, s: number) =>
		((Math.atan2(s, f) / (2 * Math.PI)) * HANG_BINS + HANG_BINS) % HANG_BINS;
	const chest = new Float64Array(HANG_BINS);
	for (let i = 0; i < m.n; i++) {
		const u = rest[i * 3 + 2]!;
		if (
			!onTorso[i] ||
			m.arm[i]! >= 128 ||
			u < HANG_FROM[0] ||
			u > HANG_FROM[1]
		) {
			continue;
		}
		const f = rest[i * 3]!;
		const s = rest[i * 3 + 1]!;
		const b = Math.floor(bin(f, s));
		chest[b] = Math.max(chest[b]!, Math.hypot(f, s));
	}
	// (Smoothed round him, and no gaps where the mesh is sparse.)
	const env = new Float64Array(HANG_BINS);
	for (let b = 0; b < HANG_BINS; b++) {
		let most = 0;
		for (let d = -2; d <= 2; d++) {
			most = Math.max(most, chest[(b + d + HANG_BINS) % HANG_BINS]!);
		}
		env[b] = most;
	}
	const smooth = (a: number, b: number, x: number) => {
		const t = Math.min(1, Math.max(0, (x - a) / (b - a)));
		return t * t * (3 - 2 * t);
	};
	for (let i = 0; i < m.n; i++) {
		const u = rest[i * 3 + 2]!;
		if (!onTorso[i] || u <= HANG_TUCK || u >= HANG_FROM[1]) {
			continue;
		}
		const f = rest[i * 3]!;
		const s = rest[i * 3 + 1]!;
		const r = Math.hypot(f, s);
		if (r === 0) {
			continue;
		}
		const x = bin(f, s);
		const b0 = Math.floor(x);
		const e = env[b0]! + (env[(b0 + 1) % HANG_BINS]! - env[b0]!) * (x - b0);
		const down = Math.min(
			1,
			Math.max(0, (HANG_FROM[0] - u) / (HANG_FROM[0] - HANG_TUCK)),
		);
		// (Gathered in at the waistband; from under his arms, his own.)
		const w =
			smooth(HANG_TUCK, HANG_TUCK + HANG_GATHER, u) *
			(1 - smooth(HANG_FROM[0], HANG_FROM[1], u)) *
			(1 - m.arm[i]! / 255);
		const out = Math.min(
			HANG_MOST,
			Math.max(0, e * (1 - HANG_TAPER * down) - r),
		);
		if (out * w <= 0) {
			continue;
		}
		const k = (r + out * w) / r;
		const at = v3(f * k, s * k, u);
		for (let j = m.start[i]!; j < m.start[i + 1]!; j++) {
			const R = REST_OF[m.bone[j]!]!;
			const d = sub(at, R.h);
			local[j * 3] = dot(d, R.a);
			local[j * 3 + 1] = dot(d, R.f);
			local[j * 3 + 2] = dot(d, R.s);
		}
	}
};

// (Where each point his arms and neck hold is at rest, as his chest's - the
// same whatever he does: worked out once, for each shape of him.)
let chestRest: Float64Array | undefined;
let chestRestOf: Float32Array | undefined;
const chestRestFor = (m: Mesh, LOCAL: Float32Array): Float64Array => {
	if (chestRest && chestRestOf === LOCAL) {
		return chestRest;
	}
	const out = new Float64Array(m.weight.length * 3);
	for (let j = 0; j < m.weight.length; j++) {
		const bi = m.bone[j]!;
		if (TRUNK[bi]) {
			continue;
		}
		const R0 = REST_OF[bi]!;
		const l0 = LOCAL[j * 3]!;
		const l1 = LOCAL[j * 3 + 1]!;
		const l2 = LOCAL[j * 3 + 2]!;
		const rx = R0.h.f + R0.a.f * l0 + R0.f.f * l1 + R0.s.f * l2 - C0.h.f;
		const ry = R0.h.s + R0.a.s * l0 + R0.f.s * l1 + R0.s.s * l2 - C0.h.s;
		const rz = R0.h.u + R0.a.u * l0 + R0.f.u * l1 + R0.s.u * l2 - C0.h.u;
		out[j * 3] = rx * C0.a.f + ry * C0.a.s + rz * C0.a.u;
		out[j * 3 + 1] = rx * C0.f.f + ry * C0.f.s + rz * C0.f.u;
		out[j * 3 + 2] = rx * C0.s.f + ry * C0.s.s + rz * C0.s.u;
	}
	chestRest = out;
	chestRestOf = LOCAL;
	return out;
};
export const skinned = (
	bones: Posed[],
	H: number,
	out: Float32Array,
	trunk?: Float32Array,
	hem = HEM_TODAY,
	flare = 1,
) => {
	const m = meshOf();
	turnsOf(bones, H);
	if (posedFlat.length < bones.length * 14) {
		posedFlat = new Float64Array(bones.length * 14);
	}
	const PF = posedFlat;
	for (let i = 0; i < bones.length; i++) {
		const { fr, ax, rad } = bones[i]!;
		const o = i * 14;
		PF[o] = ax;
		PF[o + 1] = rad;
		PF[o + 2] = fr.h.f;
		PF[o + 3] = fr.h.s;
		PF[o + 4] = fr.h.u;
		PF[o + 5] = fr.a.f;
		PF[o + 6] = fr.a.s;
		PF[o + 7] = fr.a.u;
		PF[o + 8] = fr.f.f;
		PF[o + 9] = fr.f.s;
		PF[o + 10] = fr.f.u;
		PF[o + 11] = fr.s.f;
		PF[o + 12] = fr.s.s;
		PF[o + 13] = fr.s.u;
	}
	const RF = REST_FLAT;
	const LOCAL = localFor(m, hem, flare);
	const CR = chestRestFor(m, LOCAL);
	const chestFr = bones[CHEST]!.fr;
	const raised = [raisedOf(bones, 0, chestFr), raisedOf(bones, 1, chestFr)];
	const PIT = m.pit;
	const PIT_SIDE = m.pitSide;
	const DQ = dq;
	const START = m.start;
	const BONE = m.bone;
	const WEIGHT = m.weight;
	const C = CHEST * 14;
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
		// (Kept to his trunk this much - see underArm - and so where his
		// trunk alone carries it wanted too.)
		const kept = PIT[i]! * raised[PIT_SIDE[i]!]!;
		const withT = trunk !== undefined || kept > 0;
		const j0 = START[i]!;
		const ref = BONE[j0]! * 8;
		const j1 = START[i + 1]!;
		for (let j = j0; j < j1; j++) {
			const bi = BONE[j]!;
			const pb = bi * 14;
			const rb = bi * 12;
			const w = WEIGHT[j]!;
			const al = LOCAL[j * 3]! * PF[pb]!;
			const fw = LOCAL[j * 3 + 1]! * PF[pb + 1]!;
			const sd = LOCAL[j * 3 + 2]! * PF[pb + 1]!;
			const px =
				(RF[rb]! + RF[rb + 3]! * al + RF[rb + 6]! * fw + RF[rb + 9]! * sd) * H;
			const py =
				(RF[rb + 1]! +
					RF[rb + 4]! * al +
					RF[rb + 7]! * fw +
					RF[rb + 10]! * sd) *
				H;
			const pz =
				(RF[rb + 2]! +
					RF[rb + 5]! * al +
					RF[rb + 8]! * fw +
					RF[rb + 11]! * sd) *
				H;
			yx += w * px;
			yy += w * py;
			yz += w * pz;
			const o = bi * 8;
			// (Each turn the short way round from the first's.)
			const sg =
				DQ[o]! * DQ[ref]! +
					DQ[o + 1]! * DQ[ref + 1]! +
					DQ[o + 2]! * DQ[ref + 2]! +
					DQ[o + 3]! * DQ[ref + 3]! <
				0
					? -w
					: w;
			rw += sg * DQ[o]!;
			rx += sg * DQ[o + 1]!;
			ry += sg * DQ[o + 2]!;
			rz += sg * DQ[o + 3]!;
			dw += sg * DQ[o + 4]!;
			dx += sg * DQ[o + 5]!;
			dy += sg * DQ[o + 6]!;
			dz += sg * DQ[o + 7]!;
			if (withT) {
				if (TRUNK[bi]) {
					const la = al * H;
					const lf = fw * H;
					const ls = sd * H;
					tf +=
						w *
						(PF[pb + 2]! +
							PF[pb + 5]! * la +
							PF[pb + 8]! * lf +
							PF[pb + 11]! * ls);
					ts +=
						w *
						(PF[pb + 3]! +
							PF[pb + 6]! * la +
							PF[pb + 9]! * lf +
							PF[pb + 12]! * ls);
					tu +=
						w *
						(PF[pb + 4]! +
							PF[pb + 7]! * la +
							PF[pb + 10]! * lf +
							PF[pb + 13]! * ls);
				} else {
					// (Held by his arm, or his neck: where it is at rest, carried
					// by his chest.)
					const la = CR[j * 3]! * H * PF[C]!;
					const lf = CR[j * 3 + 1]! * H * PF[C + 1]!;
					const ls = CR[j * 3 + 2]! * H * PF[C + 1]!;
					tf +=
						w *
						(PF[C + 2]! + PF[C + 5]! * la + PF[C + 8]! * lf + PF[C + 11]! * ls);
					ts +=
						w *
						(PF[C + 3]! + PF[C + 6]! * la + PF[C + 9]! * lf + PF[C + 12]! * ls);
					tu +=
						w *
						(PF[C + 4]! +
							PF[C + 7]! * la +
							PF[C + 10]! * lf +
							PF[C + 13]! * ls);
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
		let pf = f + 2 * (rw * dx - dw * rx + (ry * dz - rz * dy));
		let ps = s + 2 * (rw * dy - dw * ry + (rz * dx - rx * dz));
		let pu = u + 2 * (rw * dz - dw * rz + (rx * dy - ry * dx));
		// (Under a raised arm, his ribs where his trunk has them.)
		if (kept > 0) {
			pf += (tf / tw - pf) * kept;
			ps += (ts / tw - ps) * kept;
			pu += (tu / tw - pu) * kept;
		}
		out[i * 3] = pf;
		out[i * 3 + 1] = ps;
		out[i * 3 + 2] = pu;
		if (trunk) {
			trunk[i * 3] = tf / tw;
			trunk[i * 3 + 1] = ts / tw;
			trunk[i * 3 + 2] = tu / tw;
		}
	}
};

// ---- drawn ---------------------------------------------------------------------

// Where a point of him lands on the sprite, worked out once for the whole
// of him (see sculpt.ts lensOf): the camera's forward, right and up - each a
// constant and so much of each of f, s and u - then where the picture's
// middle is on the sprite (x, y), the lens and the sprite's pixel.
export type Lens = Float64Array;
export type MeshDraw = {
	// Where a point of him lands on the sprite (pixels) and how far it is
	// from the camera - by `lens`, if there is one: the same, quicker.
	see: (v: V3) => { x: number; y: number; depth: number };
	lens?: Lens;
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
	// How far down his thighs his shorts come, and how loose at the hem
	// (see localFor): today's, if unsaid.
	hem?: number;
	flare?: number;
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
// (The light, as drawMesh has it - for the card's drawing of him too.)
export const MESH_LIGHTS = { LIGHT, BACK_LIGHT, SHINE_AT } as const;
const SS = 2;
const BOX_HAIR = 1e-3;
let pos = new Float32Array(0);
let posT = new Float32Array(0);
let away = new Uint8Array(0);

let nrm = new Float32Array(0);
let scr = new Float32Array(0);
let zb = new Float32Array(0);
let tb = new Int32Array(0);
let wb = new Float32Array(0);
// Each point of him as posed (`pos`; `posT` as his trunk alone carries
// it - see skinned), where it lands on the sprite at SS times its pixels
// and how far from the camera (`scr`), his normal there in the camera's
// frame (`nrm`) - and which of his faces are turned away (`away`). Held
// here until the next of him: drawn here (see drawMesh), or on the card
// (see sculptGl.ts).
export type MeshPoints = {
	pos: Float32Array;
	posT: Float32Array;
	nrm: Float32Array;
	scr: Float32Array;
	away: Uint8Array;
};
export const MESH_SS = SS;
export const meshPoints = (
	bones: Posed[],
	H: number,
	m: Pick<MeshDraw, "see" | "lens" | "rt" | "dn" | "toward" | "hem" | "flare">,
): MeshPoints => {
	const me = meshOf();
	const n = me.n;
	if (pos.length < n * 3) {
		pos = new Float32Array(n * 3);
		posT = new Float32Array(n * 3);
		nrm = new Float32Array(n * 3);
		scr = new Float32Array(n * 3);
	}
	skinned(bones, H, pos, posT, m.hem, m.flare);
	// Normals, from his faces as they are now.
	nrm.fill(0, 0, n * 3);
	const T = me.tris;
	const nt = T.length / 3;
	if (away.length < nt) {
		away = new Uint8Array(nt);
	}
	// (Each buffer as a constant of its own here: read in the loops below,
	// thousands of times a picture.)
	const POS = pos;
	const NRM = nrm;
	const SCR = scr;
	const AWAY = away;
	for (let t = 0; t < nt; t++) {
		const a = T[t * 3]!;
		const b = T[t * 3 + 1]!;
		const c = T[t * 3 + 2]!;
		const ux = POS[b * 3]! - POS[a * 3]!;
		const uy = POS[b * 3 + 1]! - POS[a * 3 + 1]!;
		const uz = POS[b * 3 + 2]! - POS[a * 3 + 2]!;
		const vx = POS[c * 3]! - POS[a * 3]!;
		const vy = POS[c * 3 + 1]! - POS[a * 3 + 1]!;
		const vz = POS[c * 3 + 2]! - POS[a * 3 + 2]!;
		const nx = uy * vz - uz * vy;
		const ny = uz * vx - ux * vz;
		const nz = ux * vy - uy * vx;
		NRM[a * 3] = NRM[a * 3]! + nx;
		NRM[a * 3 + 1] = NRM[a * 3 + 1]! + ny;
		NRM[a * 3 + 2] = NRM[a * 3 + 2]! + nz;
		NRM[b * 3] = NRM[b * 3]! + nx;
		NRM[b * 3 + 1] = NRM[b * 3 + 1]! + ny;
		NRM[b * 3 + 2] = NRM[b * 3 + 2]! + nz;
		NRM[c * 3] = NRM[c * 3]! + nx;
		NRM[c * 3 + 1] = NRM[c * 3 + 1]! + ny;
		NRM[c * 3 + 2] = NRM[c * 3 + 2]! + nz;
		// (Turned away from the camera, the face itself - not its corners'
		// normals, which a fold of him can turn: behind the rest of him.)
		const tw = nx * m.toward.f + ny * m.toward.s + nz * m.toward.u;
		AWAY[t] = tw < -0.15 * Math.sqrt(nx * nx + ny * ny + nz * nz) ? 1 : 0;
	}
	// Onto the sprite (at SS times its pixels), and the normals into the
	// camera's frame.
	const L = m.lens;
	for (let i = 0; i < n; i++) {
		const pf = POS[i * 3]!;
		const ps = POS[i * 3 + 1]!;
		const pu = POS[i * 3 + 2]!;
		if (L) {
			const depth = Math.max(0.5, L[0]! + L[1]! * pf + L[2]! * ps + L[3]! * pu);
			const k = L[14]! / depth / L[15]!;
			SCR[i * 3] =
				(L[12]! / L[15]! + (L[4]! + L[5]! * pf + L[6]! * ps + L[7]! * pu) * k) *
				SS;
			SCR[i * 3 + 1] =
				(L[13]! / L[15]! -
					(L[8]! + L[9]! * pf + L[10]! * ps + L[11]! * pu) * k) *
				SS;
			SCR[i * 3 + 2] = depth;
		} else {
			const p = m.see(v3(pf, ps, pu));
			SCR[i * 3] = p.x * SS;
			SCR[i * 3 + 1] = p.y * SS;
			SCR[i * 3 + 2] = p.depth;
		}
		const nf = NRM[i * 3]!;
		const ns = NRM[i * 3 + 1]!;
		const nu = NRM[i * 3 + 2]!;
		const x = nf * m.rt.f + ns * m.rt.s + nu * m.rt.u;
		const y = nf * m.dn.f + ns * m.dn.s + nu * m.dn.u;
		const zz = nf * m.toward.f + ns * m.toward.s + nu * m.toward.u;
		const l = Math.sqrt(x * x + y * y + zz * zz) || 1;
		NRM[i * 3] = x / l;
		NRM[i * 3 + 1] = y / l;
		NRM[i * 3 + 2] = zz / l;
	}
	return { pos, posT, nrm, scr, away };
};
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
	const T = me.tris;
	const nt = T.length / 3;
	const {
		pos: POS,
		posT: POST,
		nrm: NRM,
		scr: SCR,
		away: AWAY,
	} = meshPoints(bones, H, m);
	const W2 = m.w * SS;
	const H2 = m.h * SS;
	if (zb.length < W2 * H2) {
		zb = new Float32Array(W2 * H2);
		tb = new Int32Array(W2 * H2);
		wb = new Float32Array(W2 * H2 * 2);
	}
	const ZB = zb;
	const TB = tb;
	const WB = wb;
	ZB.fill(Infinity, 0, W2 * H2);
	// First where he is - the face of him nearest the camera at each pixel,
	// and where on it - then each of those painted and lit, once.
	// (What is read for every face held here, not looked up each time; and
	// the box round all that is drawn kept, for going down to the sprite's
	// pixels - nothing of him outside it.)
	const KIND = me.kind;
	const HIDDEN = m.hidden;
	const CUTS = m.cuts;
	const LIFT = m.lift;
	let sx0 = W2;
	let sy0 = H2;
	let sx1 = -1;
	let sy1 = -1;
	for (let t = 0; t < nt; t++) {
		if (AWAY[t]) {
			continue;
		}
		const kind = KIND[t]!;
		if (HIDDEN[kind]) {
			continue;
		}
		const a = T[t * 3]!;
		const b = T[t * 3 + 1]!;
		const c = T[t * 3 + 2]!;
		const ax = SCR[a * 3]!;
		const ay = SCR[a * 3 + 1]!;
		const bx = SCR[b * 3]!;
		const by = SCR[b * 3 + 1]!;
		const cx = SCR[c * 3]!;
		const cy = SCR[c * 3 + 1]!;
		// (Each of these as the plain sums they were: the same to the last
		// bit, worked out once a face or a row rather than at every pixel.)
		const A0 = by - cy;
		const B0 = cx - bx;
		const A1 = cy - ay;
		const B1 = ax - cx;
		const den = A0 * B1 + B0 * (ay - cy);
		if (Math.abs(den) < 1e-9) {
			continue;
		}
		let lox = ax < bx ? ax : bx;
		let hix = ax > bx ? ax : bx;
		let loy = ay < by ? ay : by;
		let hiy = ay > by ? ay : by;
		if (cx < lox) {
			lox = cx;
		}
		if (cx > hix) {
			hix = cx;
		}
		if (cy < loy) {
			loy = cy;
		}
		if (cy > hiy) {
			hiy = cy;
		}
		// (Only the pixels whose middles are inside its box, give or take a
		// hair: no pixel further out than that is ever inside it.)
		const x0 = Math.max(0, Math.ceil(lox - 0.5 - BOX_HAIR));
		const x1 = Math.min(W2 - 1, Math.floor(hix - 0.5 + BOX_HAIR));
		const y0 = Math.max(0, Math.ceil(loy - 0.5 - BOX_HAIR));
		const y1 = Math.min(H2 - 1, Math.floor(hiy - 0.5 + BOX_HAIR));
		if (x0 > x1 || y0 > y1) {
			continue;
		}
		if (x0 < sx0) {
			sx0 = x0;
		}
		if (x1 > sx1) {
			sx1 = x1;
		}
		if (y0 < sy0) {
			sy0 = y0;
		}
		if (y1 > sy1) {
			sy1 = y1;
		}
		const cuts = CUTS[kind];
		const lift = LIFT[kind]!;
		const az = SCR[a * 3 + 2]! - lift;
		const bz = SCR[b * 3 + 2]! - lift;
		const cz = SCR[c * 3 + 2]! - lift;
		const id = 1 / den;
		for (let y = y0; y <= y1; y++) {
			const py = y + 0.5;
			const dy = py - cy;
			const r0 = B0 * dy;
			const r1 = B1 * dy;
			const row = y * W2;
			for (let x = x0; x <= x1; x++) {
				const dx = x + 0.5 - cx;
				const w0 = (A0 * dx + r0) * id;
				if (w0 < 0) {
					continue;
				}
				const w1 = (A1 * dx + r1) * id;
				const w2 = 1 - w0 - w1;
				if (w1 < 0 || w2 < 0) {
					continue;
				}
				const dz = w0 * az + w1 * bz + w2 * cz;
				const o = row + x;
				if (dz >= ZB[o]!) {
					continue;
				}
				if (cuts) {
					// (Cut away there - past the top of his shoe, say: what is
					// behind it shows.)
					const Pf = w0 * POS[a * 3]! + w1 * POS[b * 3]! + w2 * POS[c * 3]!;
					const Ps =
						w0 * POS[a * 3 + 1]! + w1 * POS[b * 3 + 1]! + w2 * POS[c * 3 + 1]!;
					const Pu =
						w0 * POS[a * 3 + 2]! + w1 * POS[b * 3 + 2]! + w2 * POS[c * 3 + 2]!;
					const arm =
						(w0 * me.arm[a]! + w1 * me.arm[b]! + w2 * me.arm[c]!) / 255;
					if (!m.keep(kind, Pf, Ps, Pu, arm)) {
						continue;
					}
				}
				ZB[o] = dz;
				TB[o] = t;
				WB[o * 2] = w0;
				WB[o * 2 + 1] = w1;
			}
		}
	}
	const ink = m.ink;
	// A point of him, painted and lit - once for each of the sprite's own
	// pixels, where the nearest of its SS x SS samples is (his edges as
	// smooth as SS x SS, the painting and the light a pixel at a time).
	const shaded = new Float64Array(3);
	let shadedGroup = -1;
	const shadeAt = (o: number) => {
		const t = TB[o]!;
		const a = T[t * 3]!;
		const b = T[t * 3 + 1]!;
		const c = T[t * 3 + 2]!;
		const w0 = WB[o * 2]!;
		const w1 = WB[o * 2 + 1]!;
		const w2 = 1 - w0 - w1;
		const kind = me.kind[t]!;
		// The point of him there, painted and lit.
		const Pf = w0 * POS[a * 3]! + w1 * POS[b * 3]! + w2 * POS[c * 3]!;
		const Ps =
			w0 * POS[a * 3 + 1]! + w1 * POS[b * 3 + 1]! + w2 * POS[c * 3 + 1]!;
		const Pu =
			w0 * POS[a * 3 + 2]! + w1 * POS[b * 3 + 2]! + w2 * POS[c * 3 + 2]!;
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
		const Tf = w0 * POST[a * 3]! + w1 * POST[b * 3]! + w2 * POST[c * 3]!;
		const Ts =
			w0 * POST[a * 3 + 1]! + w1 * POST[b * 3 + 1]! + w2 * POST[c * 3 + 1]!;
		const Tu =
			w0 * POST[a * 3 + 2]! + w1 * POST[b * 3 + 2]! + w2 * POST[c * 3 + 2]!;
		ink.tone = 0;
		// (`paint` says which part of him it is: for what is drawn over his
		// head - see sculpt.ts.)
		shadedGroup = m.paint(kind, Pf, Ps, Pu, arm, Tf, Ts, Tu);
		let sx = w0 * NRM[a * 3]! + w1 * NRM[b * 3]! + w2 * NRM[c * 3]!;
		let sy = w0 * NRM[a * 3 + 1]! + w1 * NRM[b * 3 + 1]! + w2 * NRM[c * 3 + 1]!;
		let sz = w0 * NRM[a * 3 + 2]! + w1 * NRM[b * 3 + 2]! + w2 * NRM[c * 3 + 2]!;
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
		shaded[0] = r;
		shaded[1] = g;
		shaded[2] = bb;
	};
	// Down to the sprite's own pixels: each the share of its SS x SS that he
	// covers, the nearest of them its depth.
	let bx0 = m.w;
	let by0 = m.h;
	let bx1 = -1;
	let by1 = -1;
	const inv = 1 / (SS * SS);
	const yTo = Math.min(m.h - 1, Math.floor(sy1 / SS));
	const xFrom = Math.floor(sx0 / SS);
	const xTo = Math.min(m.w - 1, Math.floor(sx1 / SS));
	for (let y = Math.floor(sy0 / SS); y <= yTo; y++) {
		for (let x = xFrom; x <= xTo; x++) {
			let cov = 0;
			let near = Infinity;
			let at = -1;
			for (let j = 0; j < SS; j++) {
				for (let i = 0; i < SS; i++) {
					const o = (y * SS + j) * W2 + x * SS + i;
					const zz = ZB[o]!;
					if (zz === Infinity) {
						continue;
					}
					cov++;
					if (zz < near) {
						near = zz;
						at = o;
					}
				}
			}
			if (cov === 0) {
				continue;
			}
			shadeAt(at);
			const grp = shadedGroup;
			const q = y * m.w + x;
			const o4 = q * 4;
			d[o4] = shaded[0]!;
			d[o4 + 1] = shaded[1]!;
			d[o4 + 2] = shaded[2]!;
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
