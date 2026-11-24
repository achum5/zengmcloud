// DRAWN CUT TO SHAPES - what a canvas's own clip does, the same on every
// kind of canvas. A canvas's clip edges its cut one way on the page and
// another off it (a helper's heads came out rougher at the edges of the
// hair and the face), but filling and compositing come out the same on
// both: so what is drawn is drawn on a layer of its own, cut there to each
// of `cuts` (all of them: inside every one), and laid over `ctx`.
//
// `near` is a box round what is cut (in `ctx`'s own space: within `reach`
// of x, y) - nothing outside it is drawn. `draw` draws with `g` as it would
// with `ctx`: `g` has `ctx`'s place and scale, and its lines, colors and
// see-through.

type Layer = {
	canvas: OffscreenCanvas | HTMLCanvasElement;
	g: CanvasRenderingContext2D;
};

export type Cut = Path2D | readonly [Path2D, "nonzero" | "evenodd"];

// (One a level deep - a cut drawn inside another is drawn on the next.)
const layers: Layer[] = [];
let depth = 0;

const layerAt = (i: number): Layer | undefined => {
	let layer = layers[i];
	if (!layer) {
		const canvas =
			typeof OffscreenCanvas !== "undefined"
				? new OffscreenCanvas(1, 1)
				: typeof document !== "undefined"
					? document.createElement("canvas")
					: undefined;
		const g = canvas?.getContext("2d", { willReadFrequently: true }) as
			| CanvasRenderingContext2D
			| null
			| undefined;
		if (!canvas || !g) {
			return undefined;
		}
		layer = { canvas, g };
		layers[i] = layer;
	}
	return layer;
};

export const cutTo = (
	ctx: CanvasRenderingContext2D,
	cuts: readonly Cut[],
	near: { x: number; y: number; reach: number },
	draw: (g: CanvasRenderingContext2D) => void,
): void => {
	const m = ctx.getTransform();
	let x0 = Infinity;
	let y0 = Infinity;
	let x1 = -Infinity;
	let y1 = -Infinity;
	for (const sx of [-1, 1]) {
		for (const sy of [-1, 1]) {
			const u = near.x + sx * near.reach;
			const v = near.y + sy * near.reach;
			const x = m.a * u + m.c * v + m.e;
			const y = m.b * u + m.d * v + m.f;
			x0 = Math.min(x0, x);
			y0 = Math.min(y0, y);
			x1 = Math.max(x1, x);
			y1 = Math.max(y1, y);
		}
	}
	const X0 = Math.max(0, Math.floor(x0) - 1);
	const Y0 = Math.max(0, Math.floor(y0) - 1);
	const X1 = Math.min(ctx.canvas.width, Math.ceil(x1) + 1);
	const Y1 = Math.min(ctx.canvas.height, Math.ceil(y1) + 1);
	const w = X1 - X0;
	const h = Y1 - Y0;
	if (!(w > 0 && h > 0)) {
		return;
	}
	const layer = layerAt(depth);
	if (!layer) {
		return;
	}
	const { canvas, g } = layer;
	if (canvas.width < w || canvas.height < h) {
		canvas.width = Math.max(canvas.width, w);
		canvas.height = Math.max(canvas.height, h);
	}
	g.setTransform(1, 0, 0, 1, 0, 0);
	g.globalAlpha = 1;
	g.globalCompositeOperation = "source-over";
	g.clearRect(0, 0, w, h);
	g.setTransform(m.a, m.b, m.c, m.d, m.e - X0, m.f - Y0);
	g.globalAlpha = ctx.globalAlpha;
	g.fillStyle = ctx.fillStyle;
	g.strokeStyle = ctx.strokeStyle;
	g.lineWidth = ctx.lineWidth;
	g.lineJoin = ctx.lineJoin;
	g.lineCap = ctx.lineCap;
	g.miterLimit = ctx.miterLimit;
	depth++;
	try {
		draw(g);
	} finally {
		depth--;
	}
	g.globalAlpha = 1;
	g.globalCompositeOperation = "destination-in";
	g.fillStyle = "#000";
	for (const cut of cuts) {
		if (cut instanceof Path2D) {
			g.fill(cut);
		} else {
			g.fill(cut[0], cut[1]);
		}
	}
	g.globalCompositeOperation = "source-over";
	ctx.save();
	ctx.setTransform(1, 0, 0, 1, 0, 0);
	ctx.globalAlpha = 1;
	ctx.globalCompositeOperation = "source-over";
	ctx.drawImage(canvas, 0, 0, w, h, X0, Y0, w, h);
	ctx.restore();
};
