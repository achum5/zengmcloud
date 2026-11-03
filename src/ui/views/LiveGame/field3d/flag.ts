import { project, type Camera } from "../court3d/camera.ts";
import type { Pt3 } from "./geometry.ts";

// A PENALTY FLAG: a square of yellow cloth with a weight knotted in it -
// tossed up, turning over as it flutters down, then lying where it fell.
// (Or an official's bean bag: a small blue one, no knot.)

const YELLOW = "#f7d117";
const EDGE = "#b8960a";
const BAG = "#2457c5";
const BAG_EDGE = "#163a85";
// Half its width (feet) - a bean bag's.
const HALF = 0.62;
const BAG_HALF = 0.32;
const MIN_PX = 2.5;
const BAG_MIN_PX = 1.6;

export const drawFlag = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	at: Pt3,
	down: boolean,
	turn: number,
	bag = false,
) => {
	// (Never so small, from far off, it can't be made out - it's yellow so
	// it can be.)
	const half = Math.max(
		bag ? BAG_HALF : HALF,
		(bag ? BAG_MIN_PX : MIN_PX) / project(cam, at).k,
	);
	const c = Math.cos(turn);
	const s = Math.sin(turn);
	// Two edges of the cloth from its middle: flat on the turf, or, in the
	// air, one of them up and down.
	const u = { x: c * half, y: s * half, z: 0 };
	const v = down
		? { x: -s * half * 0.9, y: c * half * 0.9, z: 0 }
		: { x: -s * half * 0.35, y: c * half * 0.35, z: half * 0.85 };
	const z = down ? 0.02 : at.z;
	const corner = (a: number, b: number, crumple: number) =>
		project(cam, {
			x: at.x + u.x * a * crumple + v.x * b,
			y: at.y + u.y * a * crumple + v.y * b,
			z: z + u.z * a + v.z * b,
		});
	const pts = [
		corner(1, 1, 1),
		corner(-1, 1, 0.8),
		corner(-1, -1, 1),
		corner(1, -1, 0.85),
	];
	const k = pts[0]!.k;
	if (pts.every((p) => p.x < -10 || p.x > cam.viewW + 10)) {
		return;
	}
	ctx.save();
	if (!down) {
		// Its shadow on the turf below.
		const g = project(cam, { x: at.x, y: at.y, z: 0 });
		ctx.fillStyle = "rgba(0, 0, 0, 0.22)";
		ctx.beginPath();
		ctx.ellipse(
			g.x,
			g.y,
			Math.max(1, half * k),
			Math.max(0.5, half * 0.4 * k),
			0,
			0,
			Math.PI * 2,
		);
		ctx.fill();
	}
	ctx.fillStyle = bag ? BAG : YELLOW;
	ctx.strokeStyle = bag ? BAG_EDGE : EDGE;
	ctx.lineWidth = Math.max(0.6, k * 0.06);
	ctx.lineJoin = "round";
	ctx.beginPath();
	ctx.moveTo(pts[0]!.x, pts[0]!.y);
	for (const p of pts.slice(1)) {
		ctx.lineTo(p.x, p.y);
	}
	ctx.closePath();
	ctx.fill();
	ctx.stroke();
	// The knot with the weight in it, at a corner.
	if (bag) {
		ctx.restore();
		return;
	}
	ctx.fillStyle = EDGE;
	ctx.beginPath();
	ctx.arc(pts[0]!.x, pts[0]!.y, Math.max(0.8, k * 0.12), 0, Math.PI * 2);
	ctx.fill();
	ctx.restore();
};
