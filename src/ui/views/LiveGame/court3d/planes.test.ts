import { afterEach, assert, beforeEach, test, vi } from "vitest";
import { courtFit, makeCamera, MAIN_RIG } from "./camera.ts";
import { COURT_W } from "./geometry.ts";
import { drawTexturedPlane, type TexturedPlane } from "./planes.ts";

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
