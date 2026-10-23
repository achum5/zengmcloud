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
// reads back clear pixels - noting what is drawn from.
const drawnFrom: unknown[] = [];
const stubCanvas = () => {
	const ctx: Record<string | symbol, unknown> = {};
	const proxy: unknown = new Proxy(ctx, {
		get: (target, prop) =>
			prop in target
				? target[prop]
				: prop === "getImageData"
					? (_x: number, _y: number, w: number, h: number) => ({
							data: new Uint8ClampedArray(w * h * 4),
							width: w,
							height: h,
						})
					: prop === "drawImage"
						? (from: unknown) => {
								drawnFrom.push(from);
							}
						: () => proxy,
		set: (target, prop, v) => {
			target[prop] = v;
			return true;
		},
	});
	const canvas = { width: 0, height: 0, getContext: () => proxy };
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
	const behind = now.map((m) => {
		const id = cache.ids.get(m.look)!;
		return cache.last.get(id)!.behind;
	});
	assert.deepEqual(behind, [1, 1, 1, 0, 0]);
	// And the next frame, the next nearest.
	const later = frame(0.25);
	assert.deepEqual(
		later.map((m) => cache.last.get(cache.ids.get(m.look)!)!.behind),
		[2, 0, 0, 0, 0],
	);
});

// Each new picture is sculpted on one scratch sheet and kept as a picture
// of its own. Drawn from, the sheet would be held on to as it was until the
// kept picture is next painted, and the next man sculpted on it would wait
// while all of it - as big as the biggest man ever drawn on it - was copied:
// the pixels are handed over instead.
test("a kept picture takes the sheet's pixels - the sheet is never drawn from", () => {
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
});
