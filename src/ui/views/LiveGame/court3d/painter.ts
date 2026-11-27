import { project, type Camera } from "./camera.ts";
import type { Pt3 } from "./geometry.ts";
import { drawTexturedPlane, type TexturedPlane } from "./planes.ts";

// WHAT IS LAID ROUND THE COURT, SAID ONCE FOR BOTH WAYS OF DRAWING IT: the
// pieces of the table, the seats on the floor, the boards' shine (see
// scene.ts dressFloor) told to a painter - the page's own (ctxPainter, below)
// or the graphics card's (see gpu/floor.ts) - in the order they go down,
// furthest first.
export type Painter = {
	// A picture laid on a plane (see planes.ts) - on the page, in `cols` x
	// `rows` pieces - so strongly; added to what is under it, if `add` (a
	// lit board's shine in the floor).
	plane: (
		plane: TexturedPlane,
		img: HTMLCanvasElement,
		cols: number,
		rows: number,
		alpha?: number,
		add?: boolean,
	) => void;
	// A flat shape in the world, all of one color (any the page takes).
	poly: (pts: readonly Pt3[], color: string) => void;
	// A picture stood up facing the camera: so much of it (sx, sy, sw, sh)
	// over so much of the picture (dx, dy, dw, dh), smoothed - its foot
	// `depth` from the camera (see camera.ts depthOf), or undefined for one
	// lying in the floor (a light's glint) - so strongly.
	sprite: (
		img: HTMLCanvasElement,
		sx: number,
		sy: number,
		sw: number,
		sh: number,
		dx: number,
		dy: number,
		dw: number,
		dh: number,
		depth: number | undefined,
		alpha?: number,
	) => void;
	// Whether what follows stands in front of the men behind it, as well as
	// over what is under it: the table's top and front, over the four sat at
	// it (on the page, drawn before them anyway: nothing to do).
	solid: (on: boolean) => void;
};

// The page's: each straight onto it.
export const ctxPainter = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
): Painter => ({
	plane: (plane, img, cols, rows, alpha = 1, add = false) => {
		if (add) {
			ctx.globalCompositeOperation = "lighter";
		}
		drawTexturedPlane(ctx, cam, plane, img, cols, rows, alpha);
		if (add) {
			ctx.globalCompositeOperation = "source-over";
		}
	},
	poly: (pts, color) => {
		ctx.fillStyle = color;
		ctx.beginPath();
		for (const p of pts) {
			const q = project(cam, p);
			ctx.lineTo(q.x, q.y);
		}
		ctx.closePath();
		ctx.fill();
	},
	sprite: (img, sx, sy, sw, sh, dx, dy, dw, dh, _depth, alpha = 1) => {
		const smoothing = ctx.imageSmoothingEnabled;
		const was = ctx.globalAlpha;
		ctx.imageSmoothingEnabled = true;
		ctx.globalAlpha = was * alpha;
		ctx.drawImage(img, sx, sy, sw, sh, dx, dy, dw, dh);
		ctx.globalAlpha = was;
		ctx.imageSmoothingEnabled = smoothing;
	},
	solid: () => {},
});
