import { afterEach, assert, beforeEach, test, vi } from "vitest";
import { makeCamera, MAIN_RIG } from "./camera.ts";
import type { PlayerState } from "./evaluate.ts";
import { kitsFor, type Look } from "./figure.ts";
import { bodyOf } from "./poses.ts";
import {
	drawSprite,
	makeScratch,
	makeSpriteCache,
	prepareSprites,
	startFrame,
} from "./sprite.ts";

// Out of time for every new picture a frame needs, it is the men furthest
// from the camera - smallest on the picture - who are drawn a frame behind
// their pose, never the ones nearest it. (The frame is drawn back to front;
// left to that, the time ran out on the nearest.)

// No canvas here: a stand-in that takes every call and draws nothing, and
// reads back clear pixels - noting what is drawn from, and how many pixels
// are read off each canvas and put on it.
const drawnFrom: unknown[] = [];
const read = new Map<unknown, number>();
// (And how many runs of lettering are laid out.)
let lettered = 0;
const put = new Map<unknown, number>();
// (And, as a canvas with a picture from another site on it, none of them
// to be read.)
let tainted = false;
const stubCanvas = () => {
	const ctx: Record<string | symbol, unknown> = {};
	const canvas = { width: 0, height: 0, getContext: () => proxy };
	const proxy: unknown = new Proxy(ctx, {
		get: (target, prop) =>
			prop in target
				? target[prop]
				: prop === "getImageData"
					? (_x: number, _y: number, w: number, h: number) => {
							if (tainted) {
								throw new Error("SecurityError");
							}
							read.set(canvas, (read.get(canvas) ?? 0) + w * h);
							return {
								data: new Uint8ClampedArray(w * h * 4),
								width: w,
								height: h,
							};
						}
					: prop === "putImageData"
						? (
								img: ImageData,
								_x: number,
								_y: number,
								_dx?: number,
								_dy?: number,
								dw?: number,
								dh?: number,
							) => {
								put.set(
									canvas,
									(put.get(canvas) ?? 0) +
										(dw ?? img.width) * (dh ?? img.height),
								);
							}
						: prop === "drawImage"
							? (from: unknown) => {
									drawnFrom.push(from);
								}
							: prop === "fillText"
								? () => {
										lettered += 1;
									}
								: () => proxy,
		set: (target, prop, v) => {
			target[prop] = v;
			return true;
		},
	});
	return canvas;
};

let clock = 0;
beforeEach(() => {
	vi.stubGlobal("document", { createElement: stubCanvas });
	vi.stubGlobal(
		"ImageData",
		class {
			data: Uint8ClampedArray;
			width: number;
			height: number;
			constructor(width: number, height: number) {
				this.width = width;
				this.height = height;
				this.data = new Uint8ClampedArray(width * height * 4);
			}
		},
	);
	vi.stubGlobal(
		"Path2D",
		class {
			constructor() {
				return new Proxy(this, { get: () => () => undefined });
			}
		},
	);
	// Each new picture takes half a millisecond.
	clock = 0;
	vi.spyOn(performance, "now").mockImplementation(() => (clock += 0.5));
});
afterEach(() => {
	vi.unstubAllGlobals();
	vi.restoreAllMocks();
});

test("short of time, the men nearest the camera get their new pose first", () => {
	const W = 640;
	const H = 360;
	const cam = makeCamera({ x: 47, width: 72, y: 24 }, W, H, MAIN_RIG);
	const body = bodyOf();
	const kits = kitsFor(undefined, undefined);
	// Five men, far side (small) to near side (big), drawn back to front.
	const men = [6, 14, 24, 34, 44].map((y, i) => {
		const look: Look = {
			kit: kits[i % 2]!,
			skin: "#8d5524",
			hair: "#1f1612",
			jerseyNumber: "",
			name: "",
			lastName: "",
			wordmark: "",
		};
		const st = (phase: number): PlayerState =>
			({
				pid: i,
				team: i % 2,
				shown: true,
				x: 30 + i * 8,
				y,
				z: 0,
				yaw: 0,
				anim: "run",
				phase,
				moving: true,
			}) as PlayerState;
		return { y, look, st };
	});
	const scratch = makeScratch();
	const cache = makeSpriteCache();
	const ctx = stubCanvas().getContext() as unknown as CanvasRenderingContext2D;
	const frame = (phase: number) => {
		startFrame(cache);
		const now = men.map((m) => ({ st: m.st(phase), body, look: m.look }));
		prepareSprites(scratch, cam, now, 1, cache);
		for (const m of now) {
			drawSprite(ctx, scratch, cam, m.st, m.body, m.look, 1, cache);
		}
		return now;
	};
	// Time for everybody: a picture each.
	frame(0);
	// Then a new pose for all five, and time for two new pictures.
	cache.budget = 1;
	const now = frame(0.25);
	const behind = now.map((m) => cache.last.get(m.st.pid)!.behind);
	assert.deepEqual(behind, [1, 1, 1, 0, 0]);
	// And the next frame, the next nearest.
	const later = frame(0.25);
	assert.deepEqual(
		later.map((m) => cache.last.get(m.st.pid)!.behind),
		[2, 0, 0, 0, 0],
	);
});

// Each new picture is kept as a picture of its own, its pixels put on it
// once. Drawn from a scratch sheet, the sheet would be held on to as it was
// until the kept picture is next painted, and the next man sculpted on it
// would wait while all of it - as big as the biggest man ever drawn on it -
// was copied. And only his head is drawn on a canvas: only the bit of the
// sheet round it is put on and read back, not all of him.
test("a kept picture is given its pixels once - the sheet is never drawn from, and only read round his head", () => {
	const cam = makeCamera({ x: 47, width: 30, y: 24 }, 640, 360, MAIN_RIG);
	const body = bodyOf();
	const look: Look = {
		kit: kitsFor(undefined, undefined)[0],
		skin: "#8d5524",
		hair: "#1f1612",
		jerseyNumber: "",
		name: "",
		lastName: "",
		wordmark: "",
	};
	const scratch = makeScratch();
	const cache = makeSpriteCache();
	const ctx = stubCanvas().getContext() as unknown as CanvasRenderingContext2D;
	drawnFrom.length = 0;
	read.clear();
	put.clear();
	for (let i = 0; i < 4; i++) {
		startFrame(cache);
		const st = {
			pid: 1,
			team: 0,
			shown: true,
			x: 40 + i,
			y: 25,
			z: 0,
			yaw: i,
			anim: "run",
			phase: i / 4,
			moving: true,
		} as PlayerState;
		prepareSprites(scratch, cam, [{ st, body, look }], 1, cache);
		drawSprite(ctx, scratch, cam, st, body, look, 1, cache);
	}
	assert.strictEqual(cache.kept.size, 4);
	assert.isFalse(drawnFrom.includes(scratch.canvas));
	// (What is drawn into the picture is each kept one.)
	assert.strictEqual(drawnFrom.length, 4);
	// Each kept picture is given all its pixels, once; off the sheet, a
	// small part of that is read - round his head.
	let keptArea = 0;
	for (const k of cache.kept.values()) {
		assert.strictEqual(put.get(k.canvas), k.w * k.h);
		assert.isUndefined(read.get(k.canvas));
		keptArea += k.w * k.h;
	}
	const sheetRead = read.get(scratch.canvas) ?? 0;
	assert.isAbove(sheetRead, 0);
	assert.isBelow(sheetRead, keptArea * 0.5);
	assert.isBelow(put.get(scratch.canvas) ?? 0, keptArea * 0.5);
	// A sheet that may not be read is drawn from all the same.
	tainted = true;
	try {
		startFrame(cache);
		const st = {
			pid: 1,
			team: 0,
			shown: true,
			x: 44,
			y: 25,
			z: 0,
			yaw: 4,
			anim: "run",
			phase: 0.9,
			moving: true,
		} as PlayerState;
		prepareSprites(scratch, cam, [{ st, body, look }], 1, cache);
		drawSprite(ctx, scratch, cam, st, body, look, 1, cache);
	} finally {
		tainted = false;
	}
	assert.strictEqual(cache.kept.size, 5);
	assert.isTrue(drawnFrom.includes(scratch.canvas));
});

// One man, run on a frame at a time with a new pose each frame and no time
// at all for new pictures: how many frames behind his pose he is drawn.
const behindRun = (
	frames: number,
	opts: { pace?: number; dark?: boolean; zoomAt?: number },
) => {
	const body = bodyOf();
	const look: Look = {
		kit: kitsFor(undefined, undefined)[1],
		skin: "#8d5524",
		hair: "#1f1612",
		jerseyNumber: "",
		name: "",
		lastName: "",
		wordmark: "",
	};
	const scratch = makeScratch();
	const cache = makeSpriteCache();
	const ctx = stubCanvas().getContext() as unknown as CanvasRenderingContext2D;
	const out: { behind: number; skipped: number }[] = [];
	for (let i = 0; i < frames; i++) {
		const width = opts.zoomAt !== undefined && i >= opts.zoomAt ? 30 : 60;
		const cam = makeCamera({ x: 47, width, y: 24 }, 640, 360, MAIN_RIG);
		startFrame(cache);
		// Time for his first picture, none after.
		cache.budget = i === 0 ? undefined : 0;
		cache.pace = opts.pace;
		cache.dark = i > 0 && opts.dark;
		const st = {
			pid: 1,
			team: 1,
			shown: true,
			x: 47,
			y: 25,
			z: 0,
			yaw: 0,
			anim: "run",
			phase: (i + 0.5) / 12,
			moving: true,
		} as PlayerState;
		prepareSprites(scratch, cam, [{ st, body, look }], 1, cache);
		drawSprite(ctx, scratch, cam, st, body, look, 1, cache);
		out.push({
			behind: cache.last.get(st.pid)?.behind ?? -1,
			skipped: cache.skipped,
		});
	}
	return out;
};

test("in slow motion a man may be drawn as he was for more frames - the same time behind his pose", () => {
	// At real speed, three frames behind at most; then made, whatever the
	// time.
	assert.deepEqual(
		behindRun(6, {}).map((f) => f.behind),
		[0, 1, 2, 3, 0, 1],
	);
	// A slow-motion replay (0.42 of real speed): seven.
	assert.deepEqual(
		behindRun(10, { pace: 0.42 }).map((f) => f.behind),
		[0, 1, 2, 3, 4, 5, 6, 7, 0, 1],
	);
});

test("black, a man much bigger since is left out till he is made, not drawn the size he was", () => {
	// The camera closes in (twice the size) at frame 2, the picture black.
	const dark = behindRun(4, { dark: true, zoomAt: 2 });
	assert.deepEqual(
		dark.map((f) => f.skipped),
		[0, 0, 1, 1],
	);
	// Not black: made at once, his new size.
	const lit = behindRun(4, { zoomAt: 2 });
	assert.deepEqual(
		lit.map((f) => [f.behind, f.skipped]),
		[
			[0, 0],
			[1, 0],
			[0, 0],
			[1, 0],
		],
	);
});

test("all black, nobody drawn, the books are kept as if they were: a man made a frame ago waits", () => {
	const body = bodyOf();
	const look: Look = {
		kit: kitsFor(undefined, undefined)[0],
		skin: "#8d5524",
		hair: "#1f1612",
		jerseyNumber: "",
		name: "",
		lastName: "",
		wordmark: "",
	};
	const scratch = makeScratch();
	const cache = makeSpriteCache();
	const cam = makeCamera({ x: 47, width: 60, y: 24 }, 640, 360, MAIN_RIG);
	const skipped: number[] = [];
	for (let i = 0; i < 4; i++) {
		startFrame(cache);
		// Time for his first picture only; black from the second frame on.
		cache.budget = i === 0 ? undefined : 0;
		cache.dark = i > 0;
		cache.black = i > 0;
		const st = {
			pid: 1,
			team: 0,
			shown: true,
			x: 47,
			y: 25,
			z: 0,
			yaw: 0,
			anim: "run",
			phase: (i + 0.5) / 12,
			moving: true,
		} as PlayerState;
		prepareSprites(scratch, cam, [{ st, body, look }], 1, cache);
		if (i === 0) {
			drawSprite(
				stubCanvas().getContext() as unknown as CanvasRenderingContext2D,
				scratch,
				cam,
				st,
				body,
				look,
				1,
				cache,
			);
		}
		skipped.push(cache.skipped);
	}
	// Never left out (which would hold the picture black for him): drawn,
	// were it drawn, as he was.
	assert.deepEqual(skipped, [0, 0, 0, 0]);
	assert.strictEqual(cache.last.get(1)!.behind, 3);
});

// A man's number and lettering are laid out on his jersey's sheet once -
// the font looked up, every glyph drawn - and each other size of it is
// shrunk from that, not laid out again each time the camera takes him a
// step bigger or smaller.
test("his lettering is laid out once, however many sizes he is drawn at", () => {
	const body = bodyOf();
	const look: Look = {
		kit: kitsFor(undefined, undefined)[0],
		skin: "#8d5524",
		hair: "#1f1612",
		jerseyNumber: "23",
		name: "",
		lastName: "Walker",
		wordmark: "Hawks",
	};
	const scratch = makeScratch();
	const ctx = stubCanvas().getContext() as unknown as CanvasRenderingContext2D;
	const st = {
		pid: 1,
		team: 0,
		shown: true,
		x: 47,
		y: 25,
		z: 0,
		yaw: 0.4,
		anim: "ready",
		phase: 0,
		moving: false,
	} as PlayerState;
	const at = (width: number) =>
		drawSprite(
			ctx,
			scratch,
			makeCamera({ x: 47, width, y: 24 }, 1280, 720, MAIN_RIG),
			st,
			body,
			look,
			1,
		);
	lettered = 0;
	// From the sideline camera, zooming, to a replay's close-up.
	at(72);
	const once = lettered;
	assert.isAbove(once, 0);
	for (const width of [90, 80, 64, 58, 46, 37, 30, 24]) {
		at(width);
	}
	assert.strictEqual(lettered, once);
	// In closer than any of that: laid out at that size.
	at(14);
	assert.strictEqual(lettered, once * 2);
});

// Out of time for new pictures, a man may be drawn a frame or two as he
// was - but not with the ball in his hands once he has let it go, or
// without it once he has it: the ball, drawn on its own when loose, would
// be on the picture twice, or not at all.
test("the ball caught or let go of: never drawn as he was", () => {
	const body = bodyOf();
	const look: Look = {
		kit: kitsFor(undefined, undefined)[1],
		skin: "#8d5524",
		hair: "#1f1612",
		jerseyNumber: "",
		name: "",
		lastName: "",
		wordmark: "",
	};
	const scratch = makeScratch();
	const cache = makeSpriteCache();
	const ctx = stubCanvas().getContext() as unknown as CanvasRenderingContext2D;
	const cam = makeCamera({ x: 47, width: 60, y: 24 }, 640, 360, MAIN_RIG);
	const behind: number[] = [];
	for (let i = 0; i < 6; i++) {
		startFrame(cache);
		// Time for his first picture, none after.
		cache.budget = i === 0 ? undefined : 0;
		const st = {
			pid: 1,
			team: 1,
			shown: true,
			x: 47,
			y: 25,
			z: 0,
			yaw: 0,
			anim: "run",
			phase: (i + 0.5) / 12,
			moving: true,
			// The ball comes into his hands at the third frame, and he lets
			// it go at the fifth.
			holding: i >= 2 && i < 4,
		} as PlayerState;
		prepareSprites(scratch, cam, [{ st, body, look }], 1, cache);
		drawSprite(ctx, scratch, cam, st, body, look, 1, cache);
		behind.push(cache.last.get(st.pid)!.behind);
	}
	// A frame behind while nothing about the ball changed; the frame it
	// came into his hands and the frame it left them, a picture of him now.
	assert.deepEqual(behind, [0, 1, 0, 1, 0, 1]);
});

// His look new (his face come in, the warm-up top off) is a new picture of
// him like a new pose's: drawn as he was while there is no time for it -
// not one made then and there for every man whose look changed at once.
test("a new look of his waits its turn, like a new pose", () => {
	const body = bodyOf();
	const plain: Look = {
		kit: kitsFor(undefined, undefined)[1],
		skin: "#b07a52",
		hair: "#1f1612",
		jerseyNumber: "",
		name: "",
		lastName: "",
		wordmark: "",
	};
	const faced: Look = { ...plain, skin: "#8d5524" };
	const scratch = makeScratch();
	const cache = makeSpriteCache();
	const ctx = stubCanvas().getContext() as unknown as CanvasRenderingContext2D;
	const cam = makeCamera({ x: 47, width: 60, y: 24 }, 640, 360, MAIN_RIG);
	const st = {
		pid: 7,
		team: 1,
		shown: true,
		x: 47,
		y: 25,
		z: 0,
		yaw: 0,
		anim: "run",
		phase: 0.25,
		moving: true,
	} as PlayerState;
	const drawn: { behind: number; made: number }[] = [];
	for (let i = 0; i < 4; i++) {
		startFrame(cache);
		// Time for his first picture, none the next frame, then plenty.
		cache.budget = i === 1 ? 0 : undefined;
		const look = i === 0 ? plain : faced;
		prepareSprites(scratch, cam, [{ st, body, look }], 1, cache);
		drawSprite(ctx, scratch, cam, st, body, look, 1, cache);
		drawn.push({
			behind: cache.last.get(st.pid)!.behind,
			made: cache.kept.size,
		});
	}
	assert.deepEqual(drawn, [
		{ behind: 0, made: 1 },
		{ behind: 1, made: 1 },
		{ behind: 0, made: 2 },
		{ behind: 0, made: 2 },
	]);
});
