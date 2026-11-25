import { depthOf, project, type Camera } from "./camera.ts";
import { shade } from "./figure.ts";
import type { Pt3 } from "./geometry.ts";

// A PIECE OF THE BUILDING BUILT IN BLENDER (see tools/court3d-arena): solid
// parts of a few faces each, drawn every frame as the camera sees them - the
// parts furthest away first, each face turned toward the camera filled and
// lit by its slope, the part inked round its outline the way everything on
// the court is.
export type ArenaModel = {
	parts: {
		name: string;
		// What it is: a color of its own, or the team's (see drawModel).
		kind: string;
		// Its points (feet, the piece's own frame), its faces (each a loop of
		// them, wound to face out), and how far each point goes with what
		// moves (0 not at all, 1 all of it).
		pts: number[][];
		faces: number[][];
		flex: number[];
	}[];
	// Where pictures go on it: each the top-left, top-right and bottom-left
	// corners of one, read from in front.
	anchors: Record<string, number[][]>;
};

// The light (from up over the camera's shoulder), and how much it tells.
const LIGHT = (() => {
	const l = [-0.35, 0.55, 0.76];
	const n = Math.hypot(l[0]!, l[1]!, l[2]!);
	return { x: l[0]! / n, y: l[1]! / n, z: l[2]! / n };
})();
const LIT = 0.32;

// Each part's edges and the faces either side of each, worked out once.
type Edges = { a: number; b: number; f0: number; f1: number }[];
const edgesOf = new WeakMap<ArenaModel["parts"][number], Edges>();
const edges = (part: ArenaModel["parts"][number]): Edges => {
	let out = edgesOf.get(part);
	if (!out) {
		const at = new Map<string, number>();
		out = [];
		part.faces.forEach((f, fi) => {
			for (let i = 0; i < f.length; i++) {
				const a = f[i]!;
				const b = f[(i + 1) % f.length]!;
				const key = a < b ? `${a},${b}` : `${b},${a}`;
				const e = at.get(key);
				if (e === undefined) {
					at.set(key, out!.length);
					out!.push({ a, b, f0: fi, f1: -1 });
				} else {
					out![e]!.f1 = fi;
				}
			}
		});
		edgesOf.set(part, out);
	}
	return out;
};

export type ModelPlace = {
	// Where a point of the piece is in the world, moved `flex` of the way
	// with what moves.
	at: (p: number[], flex: number) => Pt3;
	// Turned the other way round (the other basket): its faces' winding too.
	mirrored: boolean;
};

// WHICH PART GOES OVER WHICH. Not by how far away each part's middle is:
// the stanchion's parts are all down its middle, so to the main camera -
// which reads how far away a thing is off the floor beneath it, straight
// across the court - most of them are the same distance away, and which
// went over which came down to rounding, different every frame as the
// camera slid: the pad on the boom, the post behind the column, flickering
// in and out. Instead, between each two parts a flat wall with one on each
// side of it (the parts are solid boxes, none through another but for a
// hair at a joint): the part on the camera's side of it can't be hidden by
// the other, so it goes over it. Worked out once for the piece; each frame
// only which side of each wall the camera is on.
type Split = { a: number; b: number; n: number[]; d: number };
// (Further in than this, two parts have no wall between them: left to how
// far away they are.)
const JOINT = 0.3;
const splitsOf = new WeakMap<ArenaModel, Split[]>();
const planesOf = (part: ArenaModel["parts"][number]) =>
	part.faces.map((f) => {
		// (Each face's plane, facing out - Newell's, for a face not quite flat.)
		let nx = 0;
		let ny = 0;
		let nz = 0;
		let d = 0;
		for (let i = 0; i < f.length; i++) {
			const p = part.pts[f[i]!]!;
			const q = part.pts[f[(i + 1) % f.length]!]!;
			nx += (p[1]! - q[1]!) * (p[2]! + q[2]!);
			ny += (p[2]! - q[2]!) * (p[0]! + q[0]!);
			nz += (p[0]! - q[0]!) * (p[1]! + q[1]!);
		}
		const l = Math.hypot(nx, ny, nz) || 1;
		const n = [nx / l, ny / l, nz / l];
		for (const i of f) {
			const p = part.pts[i]!;
			d += n[0]! * p[0]! + n[1]! * p[1]! + n[2]! * p[2]!;
		}
		return { n, d: d / f.length };
	});
export const splits = (model: ArenaModel): Split[] => {
	let out = splitsOf.get(model);
	if (!out) {
		out = [];
		const planes = model.parts.map(planesOf);
		for (let i = 0; i < model.parts.length; i++) {
			for (let j = i + 1; j < model.parts.length; j++) {
				// The face of either that the other is furthest out past (or
				// least far in): the wall, halfway into the joint if they meet
				// in one. Part a on its inside, b out past it.
				let best: Split | undefined;
				let bestIn = Infinity;
				for (const [a, b] of [
					[i, j],
					[j, i],
				] as const) {
					for (const { n, d } of planes[a]!) {
						let inside = 0;
						for (const p of model.parts[b]!.pts) {
							inside = Math.max(
								inside,
								d - (n[0]! * p[0]! + n[1]! * p[1]! + n[2]! * p[2]!),
							);
						}
						if (inside < bestIn) {
							bestIn = inside;
							best = { a, b, n, d: d - inside / 2 };
						}
					}
				}
				if (best && bestIn <= JOINT) {
					out.push(best);
				}
			}
		}
		splitsOf.set(model, out);
	}
	return out;
};

// Where the camera sees from, in the world. Upright (see camera.ts project),
// not where it stands: what it draws is in true perspective from a point
// behind and below it - the point every line of sight runs back to.
export const eyeOf = (cam: Camera): Pt3 => {
	if (!cam.upright) {
		return cam.pos;
	}
	const { pos, fwd, up } = cam;
	const h2 = fwd.x * fwd.x + fwd.y * fwd.y;
	if (h2 < 1e-9) {
		return pos;
	}
	const s = (pos.z * fwd.z) / h2;
	const gx = fwd.x * s;
	const gy = fwd.y * s;
	return {
		x: pos.x + gx,
		y: pos.y + gy,
		z: -(gx * up.x + gy * up.y - pos.z * up.z),
	};
};

// The parts in the order to draw them: each after every part it could be in
// front of.
export const drawOrder = (
	cam: Camera,
	model: ArenaModel,
	place: ModelPlace,
	depths: number[],
): number[] => {
	// The eye in the piece's own frame (place.at, still, undone).
	const o = place.at([0, 0, 0], 0);
	const ax = place.at([1, 0, 0], 0);
	const ay = place.at([0, 1, 0], 0);
	const az = place.at([0, 0, 1], 0);
	const u = [ax.x - o.x, ax.y - o.y, ax.z - o.z];
	const v = [ay.x - o.x, ay.y - o.y, ay.z - o.z];
	const w = [az.x - o.x, az.y - o.y, az.z - o.z];
	const e = eyeOf(cam);
	const r = [e.x - o.x, e.y - o.y, e.z - o.z];
	const det3 = (a: number[], b: number[], c: number[]) =>
		a[0]! * (b[1]! * c[2]! - b[2]! * c[1]!) -
		a[1]! * (b[0]! * c[2]! - b[2]! * c[0]!) +
		a[2]! * (b[0]! * c[1]! - b[1]! * c[0]!);
	// (Cramer's rule: r = eye[0] u + eye[1] v + eye[2] w.)
	const D = det3(u, v, w) || 1;
	const eye = [det3(r, v, w) / D, det3(u, r, w) / D, det3(u, v, r) / D];
	const n = model.parts.length;
	const before = model.parts.map(() => 0);
	const next: number[][] = model.parts.map(() => []);
	for (const s of splits(model)) {
		const side =
			s.n[0]! * eye[0]! + s.n[1]! * eye[1]! + s.n[2]! * eye[2]! - s.d;
		const [first, last] = side > 0 ? [s.a, s.b] : [s.b, s.a];
		next[first]!.push(last);
		before[last]! += 1;
	}
	// Each time, of the parts with nothing left to go under, the furthest
	// away (the first of them, if as far); should the walls ever go round in
	// a ring, the furthest of what is left.
	const done = model.parts.map(() => false);
	const order: number[] = [];
	while (order.length < n) {
		let pick = -1;
		let fallback = -1;
		for (let i = 0; i < n; i++) {
			if (done[i]) {
				continue;
			}
			if (fallback < 0 || depths[i]! > depths[fallback]! + 1e-6) {
				fallback = i;
			}
			if (before[i] === 0 && (pick < 0 || depths[i]! > depths[pick]! + 1e-6)) {
				pick = i;
			}
		}
		const i = pick >= 0 ? pick : fallback;
		done[i] = true;
		order.push(i);
		for (const j of next[i]!) {
			before[j]! -= 1;
		}
	}
	return order;
};

// Draws the piece, and - in each part's turn, as it is drawn - `after` (a
// picture on it, say).
export const drawModel = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	model: ArenaModel,
	place: ModelPlace,
	colorOf: (kind: string) => string,
	ink: { color: string; width: number },
	after?: (part: string) => void,
) => {
	const placed = model.parts.map((part) => {
		const world = part.pts.map((p, i) => place.at(p, part.flex[i] ?? 0));
		const screen = world.map((p) => project(cam, p));
		let depth = 0;
		for (const p of world) {
			depth += depthOf(cam, p);
		}
		return { part, world, screen, depth: depth / world.length };
	});
	const order = drawOrder(
		cam,
		model,
		place,
		placed.map((p) => p.depth),
	);
	const flip = place.mirrored ? -1 : 1;
	ctx.lineJoin = "round";
	for (const { part, world, screen } of order.map((i) => placed[i]!)) {
		const front = part.faces.map((f) => {
			// (Wound to face out: turned toward the camera if it goes round
			// the right way on the screen.)
			let area = 0;
			for (let i = 0; i < f.length; i++) {
				const p = screen[f[i]!]!;
				const q = screen[f[(i + 1) % f.length]!]!;
				area += p.x * q.y - q.x * p.y;
			}
			return area * flip < 0;
		});
		const base = colorOf(part.kind);
		part.faces.forEach((f, fi) => {
			if (!front[fi]) {
				return;
			}
			const a = world[f[0]!]!;
			const b = world[f[1]!]!;
			const c = world[f[2]!]!;
			const ux = b.x - a.x;
			const uy = b.y - a.y;
			const uz = b.z - a.z;
			const vx = c.x - a.x;
			const vy = c.y - a.y;
			const vz = c.z - a.z;
			let nx = (uy * vz - uz * vy) * flip;
			let ny = (uz * vx - ux * vz) * flip;
			let nz = (ux * vy - uy * vx) * flip;
			const nl = Math.hypot(nx, ny, nz) || 1;
			nx /= nl;
			ny /= nl;
			nz /= nl;
			const lam = nx * LIGHT.x + ny * LIGHT.y + nz * LIGHT.z;
			const fill = shade(base, (lam - 0.35) * LIT);
			ctx.beginPath();
			f.forEach((i, k) => {
				const p = screen[i]!;
				if (k === 0) {
					ctx.moveTo(p.x, p.y);
				} else {
					ctx.lineTo(p.x, p.y);
				}
			});
			ctx.closePath();
			ctx.fillStyle = fill;
			ctx.fill();
			// (Its own color round it too: no hairline between two faces.)
			ctx.strokeStyle = fill;
			ctx.lineWidth = 1;
			ctx.stroke();
		});
		// Its outline: every edge with the part turned toward the camera on
		// one side of it and away on the other.
		ctx.beginPath();
		for (const e of edges(part)) {
			const f0 = front[e.f0]!;
			const f1 = e.f1 < 0 ? false : front[e.f1]!;
			if (f0 === f1) {
				continue;
			}
			const p = screen[e.a]!;
			const q = screen[e.b]!;
			ctx.moveTo(p.x, p.y);
			ctx.lineTo(q.x, q.y);
		}
		ctx.strokeStyle = ink.color;
		ctx.lineWidth = ink.width;
		ctx.lineCap = "round";
		ctx.stroke();
		after?.(part.name);
	}
};

// One of the piece's pictures, where it is in the world: its corners - and
// read the right way round, whichever way round the piece is (see
// ModelPlace.mirrored) - if its face is toward the camera.
export const anchorOf = (
	cam: Camera,
	model: ArenaModel,
	name: string,
	place: ModelPlace,
): { tl: Pt3; tr: Pt3; bl: Pt3 } | undefined => {
	const q = model.anchors[name];
	if (!q) {
		return undefined;
	}
	let tl = place.at(q[0]!, 0);
	let tr = place.at(q[1]!, 0);
	const bl = place.at(q[2]!, 0);
	const pl = project(cam, tl);
	const pr = project(cam, tr);
	const pb = project(cam, bl);
	// (Facing the camera: top-left, top-right, bottom-left go round the
	// right way on the screen.)
	const area = (pr.x - pl.x) * (pb.y - pl.y) - (pr.y - pl.y) * (pb.x - pl.x);
	if (area > 0 !== !place.mirrored) {
		return undefined;
	}
	if (place.mirrored) {
		// (Mirrored, it would read backwards: its left and right swapped.)
		const br = {
			x: bl.x + tr.x - tl.x,
			y: bl.y + tr.y - tl.y,
			z: bl.z + tr.z - tl.z,
		};
		[tl, tr] = [tr, tl];
		return { tl, tr, bl: br };
	}
	return { tl, tr, bl };
};
