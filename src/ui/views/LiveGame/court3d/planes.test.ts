import { afterEach, assert, beforeEach, test, vi } from "vitest";
import { courtFit, makeCamera, MAIN_RIG } from "./camera.ts";
import { COURT_W } from "./geometry.ts";
import {
	drawTexturedLayers,
	drawTexturedPlane,
	type TexturedPlane,
} from "./planes.ts";

// The floor shrunk to fewer pixels on the screen than its picture has is
// drawn, a row at a time, from the picture halved to about a pixel of it to
// a pixel of the screen - not every row of the screen from the full picture,
// skipping rows of it (planks' seams in one frame and gone the next).

type Canvas = { width: number; height: number; getContext: () => unknown };
const made: Canvas[] = [];
const stubCanvas = (): Canvas => {
	const canvas: Canvas = {
		width: 0,
		height: 0,
		getContext: () =>
			new Proxy({} as Record<string | symbol, unknown>, {
				get: (target, prop) => (prop in target ? target[prop] : () => {}),
				set: (target, prop, v) => {
					target[prop] = v;
					return true;
				},
			}),
	};
	made.push(canvas);
	return canvas;
};
beforeEach(() => {
	made.length = 0;
	vi.stubGlobal("document", { createElement: stubCanvas });
});
afterEach(() => {
	vi.unstubAllGlobals();
});

// The home floor's picture: the court and a margin, ten pixels a foot.
const COURT: TexturedPlane = {
	origin: { x: -5, y: -2.5, z: 0 },
	alongX: { x: 1, y: 0, z: 0 },
	alongY: { x: 0, y: 1, z: 0 },
	w: COURT_W + 10,
	h: 55,
};

// Each row drawn: which picture, from which of its rows, into which row of
// the screen.
const rowsOf = (W: number, H: number, width: number, picture: Canvas) => {
	const drawn: { from: unknown; sy: number; dy: number }[] = [];
	const ctx = {
		globalAlpha: 1,
		save: () => {},
		restore: () => {},
		drawImage: (
			from: unknown,
			_sx: number,
			sy: number,
			_sw: number,
			_sh: number,
			_dx: number,
			dy: number,
		) => {
			drawn.push({ from, sy, dy });
		},
	} as unknown as CanvasRenderingContext2D;
	const fit = courtFit(W / H);
	const cam = makeCamera({ x: 47, width, y: fit.y(width) }, W, H, MAIN_RIG);
	drawTexturedPlane(
		ctx,
		cam,
		COURT,
		picture as unknown as HTMLCanvasElement,
		26,
		12,
	);
	return drawn;
};

test("the floor far off is drawn from its picture halved, near from the whole of it", () => {
	const picture = stubCanvas();
	picture.width = 1040;
	picture.height = 550;
	// A small picture of the whole floor: the far side has three or four of
	// the picture's rows to a row of the screen, the near side under two.
	const drawn = rowsOf(640, 360, 94, picture);
	assert.isAbove(drawn.length, 100);
	const top = drawn[0]!;
	const bottom = drawn.at(-1)!;
	assert.notStrictEqual(top.from, picture);
	const far = top.from as Canvas;
	assert.isAtMost(far.height, picture.height / 2);
	assert.strictEqual(bottom.from, picture);
	// Every row of the screen once, each from a row of its picture further
	// down the floor than the last.
	for (let i = 1; i < drawn.length; i++) {
		const a = drawn[i - 1]!;
		const b = drawn[i]!;
		assert.strictEqual(b.dy, a.dy + 1);
		const at = (d: typeof a) =>
			d.sy / ((d.from as Canvas).height / picture.height);
		assert.isAtLeast(at(b), at(a) - 1e-9);
	}
	// The halved pictures are made once: the next frame makes none.
	const count = made.length;
	rowsOf(640, 360, 90, picture);
	assert.strictEqual(made.length, count);
	// Big on the screen, the whole picture all the way.
	const close = rowsOf(1200, 675, 50, picture);
	assert.isTrue(close.every((d) => d.from === picture));
});

// From a camera turned along the court (a replay's), the floor is drawn a
// band of the screen's rows at a time, each filled with its picture laid
// on it slanted - every row of the floor filled once, nothing clipped -
// not in triangles, whose edges all showed: a lattice of faint slanting
// lines across the floor.
test("the floor seen by a turned camera: in bands of whole rows, none twice, no edges", () => {
	const picture = stubCanvas();
	picture.width = 1040;
	picture.height = 550;
	const fills: { x: number; y: number; w: number; h: number }[] = [];
	let clips = 0;
	const pattern = { setTransform: () => {} };
	const ctx = {
		globalAlpha: 1,
		fillStyle: undefined as unknown,
		save: () => {},
		restore: () => {},
		clip: () => {
			clips++;
		},
		createPattern: () => pattern,
		fillRect: (x: number, y: number, w: number, h: number) => {
			fills.push({ x, y, w, h });
		},
		beginPath: () => {},
		moveTo: () => {},
		lineTo: () => {},
		closePath: () => {},
		transform: () => {},
		drawImage: () => {},
	} as unknown as CanvasRenderingContext2D;
	const cam = makeCamera({ x: 12, width: 30, y: 25, z: 4 }, 1200, 675, {
		...MAIN_RIG,
		back: 24,
		high: 14,
		slide: 0.92,
		upright: false,
	});
	drawTexturedPlane(
		ctx,
		cam,
		COURT,
		picture as unknown as HTMLCanvasElement,
		26,
		12,
	);
	assert.strictEqual(clips, 0);
	assert.isAbove(fills.length, 20);
	// Whole rows, top to bottom, each band starting where the last ended.
	for (let i = 0; i < fills.length; i++) {
		const f = fills[i]!;
		assert.strictEqual(f.y, Math.round(f.y));
		assert.isAtLeast(f.h, 1);
		if (i > 0) {
			assert.strictEqual(f.y, fills[i - 1]!.y + fills[i - 1]!.h);
		}
		assert.isAtLeast(f.x, 0);
		assert.isAtMost(f.x + f.w, 1200 + 1e-9);
	}
	// Down to the bottom of the picture: the floor right under the camera.
	const last = fills.at(-1)!;
	assert.strictEqual(last.y + last.h, 675);
});

// The stands behind a basket - drawn in triangles from the sideline - are
// several pictures one over another (the seats as full as they are, the
// crowd on its feet): all of them drawn in each triangle, under its one
// clip, before the next triangle. (A faint one drawn a triangle at a time
// showed each triangle's overlap with the next twice as strongly.)
test("pictures over one another on a plane in triangles: all of them in each triangle", () => {
	const calls: { op: string; alpha: number; img?: unknown }[] = [];
	const saved: number[] = [];
	const ctx = {
		globalAlpha: 1,
		save: () => {
			saved.push(ctx.globalAlpha);
		},
		restore: () => {
			ctx.globalAlpha = saved.pop()!;
		},
		beginPath: () => {},
		moveTo: () => {},
		lineTo: () => {},
		closePath: () => {},
		transform: () => {},
		clip: () => {
			calls.push({ op: "clip", alpha: ctx.globalAlpha });
		},
		drawImage: (img: unknown) => {
			calls.push({ op: "draw", alpha: ctx.globalAlpha, img });
		},
	};
	const pic = (w: number, h: number) =>
		({ width: w, height: h }) as HTMLCanvasElement;
	const seats = pic(1392, 528);
	const full = pic(1392, 528);
	const up = pic(1392, 528);
	// Upright, running along the end of the court, back from the floor -
	// seen from the sideline.
	const ends: TexturedPlane = {
		origin: { x: COURT_W + 20, y: 4, z: 18 },
		alongX: { x: 0, y: 1 / 8, z: 0 },
		alongY: { x: -0.08, y: 0, z: -0.06 },
		w: 1392 / 3,
		h: 528 / 3,
	};
	const fit = courtFit(16 / 9);
	const cam = makeCamera(
		{ x: 80, width: 60, y: fit.y(60) },
		1200,
		675,
		MAIN_RIG,
	);
	drawTexturedLayers(
		ctx as unknown as CanvasRenderingContext2D,
		cam,
		ends,
		[
			{ img: seats, alpha: 1 },
			{ img: full, alpha: 0.5 },
			{ img: up, alpha: 0 },
		],
		24,
		6,
	);
	const clips = calls.filter((c) => c.op === "clip").length;
	assert.isAbove(clips, 20);
	// Each clip, then the seats and the full stands over them - the crowd
	// sat down, not drawn at all.
	assert.strictEqual(calls.length, clips * 3);
	for (let i = 0; i < calls.length; i += 3) {
		assert.deepEqual(
			calls.slice(i, i + 3).map((c) => [c.op, c.img, c.alpha]),
			[
				["clip", undefined, 1],
				["draw", seats, 1],
				["draw", full, 0.5],
			],
		);
	}
});
