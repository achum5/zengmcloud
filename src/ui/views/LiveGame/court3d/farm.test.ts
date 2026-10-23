import { afterEach, assert, beforeEach, test, vi } from "vitest";
import { makeCamera, MAIN_RIG } from "./camera.ts";
import type { PlayerState } from "./evaluate.ts";
import { farmHand, makeFarm, type ToHand } from "./farm.ts";
import { kitsFor, type Look } from "./figure.ts";
import { bodyOf } from "./poses.ts";
import {
	drawSprite,
	makeScratch,
	makeSpriteCache,
	prepareSprites,
	releaseSprites,
	startFrame,
	type SpriteCache,
} from "./sprite.ts";

// A man who can be drawn a frame as he was is sculpted off the page's thread
// (see farm.ts): the page only finishes him and keeps the picture, and
// meanwhile draws him in the one he had - or, once it comes back, in it.

// No canvas here: a stand-in that takes every call and draws nothing, and
// reads back clear pixels.
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
					: () => proxy,
		set: (target, prop, v) => {
			target[prop] = v;
			return true;
		},
	});
	return { width: 0, height: 0, getContext: () => proxy };
};

// No worker either: one in the test's own thread, its messages handed over
// when the test says (see flush).
type Fake = {
	worker: Worker;
	jobs: number;
	sculpted: number;
	dead: boolean;
	flush: () => Promise<void>;
};
const fakes: Fake[] = [];
const spawn = () => {
	const inbox: { farm: ToHand }[] = [];
	const outbox: unknown[] = [];
	const hand = farmHand((msg) => {
		outbox.push(msg);
	});
	const fake: Fake = {
		worker: {
			postMessage: (m: { farm: ToHand }) => {
				if ("work" in m.farm) {
					fake.jobs += 1;
				}
				inbox.push(m);
			},
			terminate: () => {
				fake.dead = true;
			},
		} as unknown as Worker,
		jobs: 0,
		sculpted: 0,
		dead: false,
		flush: async () => {
			for (const m of inbox.splice(0)) {
				hand(m.farm);
			}
			// (Its jobs are done a turn at a time.)
			for (let i = 0; i < 20; i++) {
				await new Promise((r) => setTimeout(r, 0));
			}
			for (const data of outbox.splice(0)) {
				const back = (data as { farm: { shaped?: unknown } }).farm;
				if (back.shaped) {
					fake.sculpted += 1;
				}
				fake.worker.onmessage?.({ data } as MessageEvent);
			}
		},
	};
	fakes.push(fake);
	return fake.worker;
};

let clock = 0;
beforeEach(() => {
	fakes.length = 0;
	vi.stubGlobal("document", { createElement: stubCanvas });
	vi.stubGlobal(
		"OffscreenCanvas",
		class {
			constructor() {
				return stubCanvas();
			}
		},
	);
	vi.stubGlobal(
		"ImageData",
		class {
			data: Uint8ClampedArray;
			width: number;
			height: number;
			constructor(a: number | Uint8ClampedArray, b: number, c?: number) {
				this.width = typeof a === "number" ? a : b;
				this.height = typeof a === "number" ? b : c!;
				this.data =
					typeof a === "number"
						? new Uint8ClampedArray(a * b * 4)
						: (a as Uint8ClampedArray);
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
	vi.stubGlobal(
		"MessageChannel",
		class {
			port1: { onmessage: (() => void) | null } = { onmessage: null };
			port2 = {
				postMessage: () => {
					setTimeout(() => this.port1.onmessage?.(), 0);
				},
			};
		},
	);
	clock = 0;
	vi.spyOn(performance, "now").mockImplementation(() => (clock += 0.5));
});
afterEach(() => {
	vi.unstubAllGlobals();
	vi.restoreAllMocks();
});

const look: Look = {
	kit: kitsFor(undefined, undefined)[0],
	skin: "#8d5524",
	hair: "#1f1612",
	jerseyNumber: "",
	name: "",
	lastName: "",
	wordmark: "",
};
const body = bodyOf();
const cam = makeCamera({ x: 47, width: 60, y: 24 }, 640, 360, MAIN_RIG);
const man = (pid: number, i: number): PlayerState =>
	({
		pid,
		team: 0,
		shown: true,
		x: 40 + pid * 4,
		y: 25,
		z: 0,
		yaw: 0,
		anim: "run",
		phase: (i + 0.5) / 12,
		moving: true,
	}) as PlayerState;

// A frame of men (one look each), with time for every new picture.
const frame = (cache: SpriteCache, men: PlayerState[], looks: Look[]) => {
	const scratch = makeScratch();
	const ctx = stubCanvas().getContext() as unknown as CanvasRenderingContext2D;
	startFrame(cache);
	const all = men.map((st, i) => ({ st, body, look: looks[i]! }));
	prepareSprites(scratch, cam, all, 1, cache);
	for (const m of all) {
		drawSprite(ctx, scratch, cam, m.st, m.body, m.look, 1, cache);
	}
	return men.map((st, i) => ({
		plan: cache.plans.get(st)!,
		last: cache.last.get(st.pid)!,
	}));
};

test("a man who can wait is sculpted off the page, and drawn in it once it comes", async () => {
	const farm = makeFarm(spawn, 1);
	const hand = fakes[0]!;
	await hand.flush();
	const cache = makeSpriteCache();
	cache.farm = farm;
	// No picture to wait in: made on the page.
	const [first] = frame(cache, [man(1, 0)], [look]);
	assert.strictEqual(hand.jobs, 0);
	assert.strictEqual(first!.last.behind, 0);
	// His next pose is the hand's; he is drawn as he was meanwhile.
	const [second] = frame(cache, [man(1, 1)], [look]);
	assert.isTrue(second!.plan.farmed);
	assert.strictEqual(hand.jobs, 1);
	assert.isFalse(cache.kept.has(second!.plan.key!));
	assert.strictEqual(second!.last.behind, 1);
	assert.isTrue(farm.busy(cache, second!.plan.key!));
	// Back from the hand: kept, and - a pose on from the one he had - the
	// picture he is drawn in while his next is sculpted.
	await hand.flush();
	assert.strictEqual(hand.sculpted, 1);
	assert.isFalse(farm.busy(cache, second!.plan.key!));
	const [third] = frame(cache, [man(1, 2)], [look]);
	const kept = cache.kept.get(second!.plan.key!);
	assert.isDefined(kept);
	assert.strictEqual(third!.last.kept, kept);
	assert.strictEqual(third!.last.behind, 1);
	assert.isTrue(third!.plan.farmed);
	// And that pose again: his own picture, kept.
	const [again] = frame(cache, [man(1, 1)], [look]);
	assert.strictEqual(again!.last.kept, kept);
	assert.strictEqual(again!.last.behind, 0);
	farm.stop();
	assert.isTrue(hand.dead);
});

test("a newer pose of a man takes the place of one the hand has not got to", async () => {
	const farm = makeFarm(spawn, 1);
	const hand = fakes[0]!;
	await hand.flush();
	const cache = makeSpriteCache();
	cache.farm = farm;
	const looks = [look, { ...look }];
	frame(cache, [man(1, 0), man(2, 0)], looks);
	// Two new poses for one, one for the other, before the hand gets to any.
	const older = frame(cache, [man(1, 1), man(2, 1)], looks);
	const newer = frame(cache, [man(1, 2), man(2, 1)], looks);
	assert.strictEqual(hand.jobs, 3);
	await hand.flush();
	// The newest of each sculpted, the other answered with nothing.
	assert.strictEqual(hand.sculpted, 2);
	assert.isFalse(cache.kept.has(older[0]!.plan.key!));
	const now = frame(cache, [man(1, 3), man(2, 3)], looks);
	for (let i = 0; i < 2; i++) {
		const kept = cache.kept.get(newer[i]!.plan.key!);
		assert.isDefined(kept);
		assert.strictEqual(now[i]!.last.kept, kept);
	}
	assert.strictEqual(now[0]!.last.behind, 1);
	assert.strictEqual(now[1]!.last.behind, 2);
	farm.stop();
});

test("what comes back for a cache let go of is not kept; a hand gone, its men are made on the page", async () => {
	const farm = makeFarm(spawn, 1);
	const hand = fakes[0]!;
	await hand.flush();
	const cache = makeSpriteCache();
	cache.farm = farm;
	frame(cache, [man(1, 0)], [look]);
	frame(cache, [man(1, 1)], [look]);
	assert.strictEqual(hand.jobs, 1);
	releaseSprites(cache);
	await hand.flush();
	assert.deepEqual(farm.take(cache), []);
	// A new cache, and the hand dies with a job out: his next pose is made
	// on the page, and nothing more is asked of it.
	const next = makeSpriteCache();
	next.farm = farm;
	frame(next, [man(1, 0)], [look]);
	const [out] = frame(next, [man(1, 1)], [look]);
	assert.isTrue(out!.plan.farmed);
	hand.worker.onerror?.({ preventDefault: () => {} } as ErrorEvent);
	assert.isTrue(hand.dead);
	assert.isFalse(farm.busy(next, out!.plan.key!));
	const [later] = frame(next, [man(1, 2)], [look]);
	assert.isUndefined(later!.plan.farmed);
	assert.strictEqual(later!.last.behind, 0);
	assert.strictEqual(hand.jobs, 2);
});
