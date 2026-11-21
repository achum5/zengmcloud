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
	placed.sort((a, b) => b.depth - a.depth);
	const flip = place.mirrored ? -1 : 1;
	ctx.lineJoin = "round";
	for (const { part, world, screen } of placed) {
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
