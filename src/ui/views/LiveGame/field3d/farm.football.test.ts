import { afterEach, assert, beforeEach, test, vi } from "vitest";
import {
	makeScratch,
	makeSpriteCache,
	releaseSprites,
	startFrame,
	type SpriteCache,
} from "../court3d/sprite.ts";
import { ART_H, ART_W, type KitArt } from "../court3d/kitArt.ts";
import { footballBody } from "./body.ts";
import { makeFieldCamera } from "./camera.ts";
import { fieldFarmHand, makeFieldFarm, type ToFieldHand } from "./farm.ts";
import { fieldGearFor, fieldKit, type FieldLook } from "./kit.ts";
import {
	drawFieldPlayer,
	prepareFieldSprites,
	shapeField,
	type FieldJob,
	type FieldPlayerState,
} from "./sprite.ts";

// A man who can be drawn a frame as he was is sculpted off the page's thread
// (see farm.ts): the page only keeps the picture, and meanwhile draws him in
// the one he had - or, once it comes back, in it.

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
	const inbox: { farm: ToFieldHand }[] = [];
	const outbox: unknown[] = [];
	const hand = fieldFarmHand((msg) => {
		outbox.push(msg);
	});
	const fake: Fake = {
		worker: {
			postMessage: (m: { farm: ToFieldHand }) => {
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

const kit = fieldKit(["#c8102e", "#ffb612", "#ffffff"], "home");
const lookOf = (pid: number): FieldLook => ({
	kit,
	gear: fieldGearFor(pid, "WR", kit),
	skin: "#8d5524",
	jerseyNumber: "",
	lastName: "",
});
const body = footballBody();
const cam = makeFieldCamera({ x: 150, width: 60, y: 80 }, 640, 360);
const man = (id: number, i: number): FieldPlayerState => ({
	id,
	team: 0,
	x: 140 + id * 4,
	y: 80,
	z: 0,
	yaw: 0,
	move: "run",
	phase: (i + 0.5) / 12,
});

// A frame of men (one look each), with time for every new picture.
const frame = (
	cache: SpriteCache,
	men: FieldPlayerState[],
	looks: FieldLook[],
	farm?: ReturnType<typeof makeFieldFarm>,
) => {
	const scratch = makeScratch();
	const ctx = stubCanvas().getContext() as unknown as CanvasRenderingContext2D;
	startFrame(cache);
	const all = men.map((st, i) => ({
		st,
		body,
		look: looks[i]!,
		shaded: false,
	}));
	prepareFieldSprites(scratch, cam, all, 1, cache, farm);
	for (const m of all) {
		drawFieldPlayer(ctx, scratch, cam, m.st, m.body, m.look, 1, cache);
	}
	return men.map((st) => cache.last.get(st.id)!);
};

test("a man who can wait is sculpted off the page, and drawn in it once it comes", async () => {
	const farm = makeFieldFarm(spawn, 1);
	const hand = fakes[0]!;
	await hand.flush();
	const cache = makeSpriteCache();
	const look = lookOf(1);
	// No picture to wait in: made on the page.
	const [first] = frame(cache, [man(1, 0)], [look], farm);
	assert.strictEqual(hand.jobs, 0);
	assert.strictEqual(first!.behind, 0);
	const made = cache.kept.size;
	// His next pose is the hand's; he is drawn as he was meanwhile.
	const [second] = frame(cache, [man(1, 1)], [look], farm);
	assert.strictEqual(hand.jobs, 1);
	assert.strictEqual(cache.kept.size, made);
	assert.strictEqual(second!.behind, 1);
	// Back from the hand: kept, and - a pose on from the one he had - the
	// picture he is drawn in while his next is sculpted.
	await hand.flush();
	assert.strictEqual(hand.sculpted, 1);
	const [third] = frame(cache, [man(1, 2)], [look], farm);
	assert.strictEqual(cache.kept.size, made + 1);
	assert.strictEqual(third!.behind, 1);
	assert.strictEqual(hand.jobs, 2);
	// And that pose again: his own picture, kept.
	const [again] = frame(cache, [man(1, 1)], [look], farm);
	assert.strictEqual(again!.behind, 0);
	assert.strictEqual(again!.kept, third!.kept);
	farm.stop();
	assert.isTrue(hand.dead);
});

test("a newer pose of a man takes the place of one the hand has not got to", async () => {
	const farm = makeFieldFarm(spawn, 1);
	const hand = fakes[0]!;
	await hand.flush();
	const cache = makeSpriteCache();
	const looks = [lookOf(1), lookOf(2)];
	frame(cache, [man(1, 0), man(2, 0)], looks, farm);
	// Two new poses for one, one for the other, before the hand gets to any.
	frame(cache, [man(1, 1), man(2, 1)], looks, farm);
	frame(cache, [man(1, 2), man(2, 1)], looks, farm);
	assert.strictEqual(hand.jobs, 3);
	await hand.flush();
	// The newest of each sculpted, the other answered with nothing.
	assert.strictEqual(hand.sculpted, 2);
	const now = frame(cache, [man(1, 3), man(2, 3)], looks, farm);
	assert.strictEqual(now[0]!.behind, 1);
	assert.strictEqual(now[1]!.behind, 2);
	farm.stop();
});

test("what comes back for a cache let go of is not kept; a hand gone, its men are made on the page", async () => {
	const farm = makeFieldFarm(spawn, 1);
	const hand = fakes[0]!;
	await hand.flush();
	const cache = makeSpriteCache();
	const look = lookOf(1);
	frame(cache, [man(1, 0)], [look], farm);
	frame(cache, [man(1, 1)], [look], farm);
	assert.strictEqual(hand.jobs, 1);
	releaseSprites(cache);
	await hand.flush();
	assert.deepEqual(farm.take(cache), []);
	// A new cache, and the hand dies with a job out: his next pose is made
	// on the page, and nothing more is asked of it.
	const next = makeSpriteCache();
	frame(next, [man(1, 0)], [look], farm);
	const [out] = frame(next, [man(1, 1)], [look], farm);
	assert.strictEqual(out!.behind, 1);
	hand.worker.onerror?.({ preventDefault: () => {} } as ErrorEvent);
	assert.isTrue(hand.dead);
	const [later] = frame(next, [man(1, 2)], [look], farm);
	assert.strictEqual(later!.behind, 0);
	assert.strictEqual(hand.jobs, 2);
});

test("a uniform drawn from a picture is sent to a hand once, and he wears it there as on the page", async () => {
	const farm = makeFieldFarm(spawn, 1);
	const hand = fakes[0]!;
	await hand.flush();
	// A picture all one green.
	const data = new Uint8ClampedArray(ART_W * ART_H * 4);
	for (let i = 0; i < data.length; i += 4) {
		data.set([40, 200, 90, 255], i);
	}
	const art: KitArt = { id: "green", data, scale: 1 };
	const looks = [1, 2].map((pid) => ({ ...lookOf(pid), kitArt: art }));
	const sent: ToFieldHand[] = [];
	const post = hand.worker.postMessage.bind(hand.worker);
	hand.worker.postMessage = (m: { farm: ToFieldHand }) => {
		sent.push(m.farm);
		post(m);
	};
	const back: Uint8ClampedArray[] = [];
	const cache = makeSpriteCache();
	frame(cache, [man(1, 0), man(2, 0)], looks, farm);
	frame(cache, [man(1, 1), man(2, 1)], looks, farm);
	const take = hand.worker.onmessage!;
	hand.worker.onmessage = (event: MessageEvent) => {
		const shaped = event.data.farm.shaped;
		if (shaped) {
			back.push(shaped.pixels[0]);
		}
		take.call(hand.worker, event);
	};
	await hand.flush();
	assert.strictEqual(sent.filter((m) => "art" in m).length, 1);
	const jobs = sent.filter(
		(m): m is { work: FieldJob } & ToFieldHand => "work" in m,
	);
	assert.strictEqual(jobs.length, 2);
	assert.strictEqual(back.length, 2);
	// What the hand made of him is what the page would have.
	const own = shapeField(jobs[0]!.work, looks[0]!)!;
	assert.deepEqual([...back[0]!], [...own.pixels.data]);
	// And it is the picture he wears: green, not his team's red.
	let green = 0;
	for (let i = 0; i < own.pixels.data.length; i += 4) {
		const [r, g] = [own.pixels.data[i]!, own.pixels.data[i + 1]!];
		if (g > r + 60) {
			green++;
		}
	}
	assert.isAbove(green, 50);
	farm.stop();
});
