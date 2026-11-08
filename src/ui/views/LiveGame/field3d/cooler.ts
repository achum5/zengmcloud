import { project, type Camera } from "../court3d/camera.ts";
import { FIGURE_SCALE } from "./body.ts";
import type { Pt3 } from "./geometry.ts";

// A COOLER, the big orange kind, stood where it is kept - on the drinks
// table at either end of a bench (see bench.ts) - its white lid on.

// How big round and how tall (feet, drawn to the size of the men - see
// body.ts).
const R = 0.68 * FIGURE_SCALE;
const H = 1.85 * FIGURE_SCALE;
const ORANGE = "#f27b1c";
const ORANGE_DARK = "#b3520d";
const LID = "#f2f0e8";
const MIN_PX = 1;
const RING = 16;

type P2 = { x: number; y: number };
const hullOf = (ps: P2[]): P2[] => {
	const pts = [...ps].sort((a, b) => a.x - b.x || a.y - b.y);
	const cross = (o: P2, a: P2, b: P2) =>
		(a.x - o.x) * (b.y - o.y) - (a.y - o.y) * (b.x - o.x);
	const lower: P2[] = [];
	for (const p of pts) {
		while (lower.length >= 2 && cross(lower.at(-2)!, lower.at(-1)!, p) <= 0) {
			lower.pop();
		}
		lower.push(p);
	}
	const upper: P2[] = [];
	for (const p of pts.reverse()) {
		while (upper.length >= 2 && cross(upper.at(-2)!, upper.at(-1)!, p) <= 0) {
			upper.pop();
		}
		upper.push(p);
	}
	return [...lower.slice(0, -1), ...upper.slice(0, -1)];
};
const fill = (
	ctx: CanvasRenderingContext2D,
	ps: P2[],
	color: string,
	edge: string,
	width: number,
) => {
	ctx.beginPath();
	ps.forEach((p, i) => (i === 0 ? ctx.moveTo(p.x, p.y) : ctx.lineTo(p.x, p.y)));
	ctx.closePath();
	ctx.fillStyle = color;
	ctx.fill();
	ctx.strokeStyle = edge;
	ctx.lineWidth = width;
	ctx.stroke();
};

// Its bottom at `bottom`.
export const drawStandingCooler = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	bottom: Pt3,
) => {
	const ring = (z: number) =>
		Array.from({ length: RING }, (_, i) => {
			const a = (i / RING) * Math.PI * 2;
			return project(cam, {
				x: bottom.x + Math.cos(a) * R,
				y: bottom.y + Math.sin(a) * R,
				z,
			});
		});
	const lo = ring(bottom.z);
	const hi = ring(bottom.z + H);
	const k = project(cam, { ...bottom, z: bottom.z + H }).k;
	ctx.save();
	ctx.lineJoin = "round";
	fill(ctx, hullOf([...lo, ...hi]), ORANGE, ORANGE_DARK, Math.max(1, 0.08 * k));
	fill(ctx, hi, LID, LID, Math.max(MIN_PX, 0.14 * k));
	ctx.restore();
};
