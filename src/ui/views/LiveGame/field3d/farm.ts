import type { SpriteCache } from "../court3d/sprite.ts";
import type { FieldLook } from "./kit.ts";
import { shapeField, type FieldJob, type FieldShaped } from "./sprite.ts";

// NEW PICTURES SCULPTED OFF THE PAGE'S THREAD - as the court's are (see
// court3d/farm.ts).
//
// Sculpting a man's new pose (see sculpt.ts) is most of what a frame costs:
// several new ones a frame with two teams, the officials and the sidelines
// moving, a few milliseconds each. Done on the page's own thread, that is
// time the frame has not got - men drawn a frame or more behind their pose,
// and the poses kept in their coarser steps (see Field3D).
//
// So whoever can be drawn a frame as he was is sculpted by a hand off the
// page - a worker of its own (see src/field) - and drawn in the picture he
// had till his new one comes back a frame or two later, as pixels: all of
// him (a football player's head is sculpted with the rest of him), kept as
// it comes. Whoever has no picture to wait in is made on the page as
// before, and so is everybody where there is no hand to be had.
//
// A hand is sent each look once - its team's logo as pixels - and then only
// the job: the camera, his pose, his build and the box he is drawn in (see
// FieldJob).

// What a look is sent as: its logo by number, sent once.
type SentLook = Omit<FieldLook, "logo"> & { logo?: number };
// The page to a hand: each job in its lane (one a man - see makeFieldFarm)...
type JobMsg = { job: number; look: number; work: FieldJob; lane: string };
export type ToFieldHand =
	| { hello: true }
	| { asset: number; pic: ImageData }
	| { look: number; value: SentLook }
	| JobMsg;
// ... and back: whether it can sculpt at all, and each man sculpted (his
// pixels as a plain array; nothing if he could not be, or nothing of him
// shows).
type SentShape = Omit<FieldShaped, "pixels"> & {
	pixels: [Uint8ClampedArray<ArrayBuffer>, number, number];
};
// (And how long it took, if it was sculpted.)
type Back = { job: number; shaped?: SentShape; ms?: number };
export type FromFieldHand = { ready: boolean } | Back;

// A man sculpted off the page's thread, for the page to keep: whose it is,
// the frame it was asked for in, his size then and whether the ball was
// drawn with him.
export type FieldMade = {
	key: string;
	shaped: FieldShaped;
	id: number;
	frame: number;
	scale: number;
	held: boolean;
};
type Whose = { id: number; frame: number; scale: number; held: boolean };
export type FieldFarm = {
	// Takes on sculpting this picture for the cache, if it can now (false:
	// it can't - it is made here or not at all).
	ask: (
		cache: SpriteCache,
		key: string,
		job: FieldJob,
		look: FieldLook,
		whose: Whose,
	) => boolean;
	// Whether it is still being made.
	busy: (cache: SpriteCache, key: string) => boolean;
	// The pictures made for the cache since last asked.
	take: (cache: SpriteCache) => FieldMade[];
	stop: () => void;
};

// Another instance of the worker the hands are (see src/field).
export const spawnFieldWorker = () =>
	new Worker(
		__NODE_ENV === "production"
			? `/gen/field-${window.bbgmVersion}.js`
			: "/gen/field.js",
		{ type: "module" },
	);

// The most jobs a hand is given at once: as many as it takes it about two
// frames to do, by how long one has taken it lately (and never fewer than a
// few, or more than a dozen) - more waiting than that would only come back
// too late to be drawn, the men made on the page anyway.
const MIN_OUT = 3;
const MAX_OUT = 12;
const OUT_MS = 35;

type Hand = {
	worker: Worker;
	ready: boolean;
	// What it has been sent.
	looks: Set<number>;
	assets: Set<number>;
	out: number;
	// How long a job takes it, lately (ms).
	ms?: number;
};
// How many jobs a hand may have out now (see MAX_OUT).
const outFor = (hand: Hand) =>
	hand.ms === undefined
		? MIN_OUT
		: Math.min(MAX_OUT, Math.max(MIN_OUT, Math.ceil(OUT_MS / hand.ms)));
type Ticket = {
	cache: SpriteCache;
	key: string;
	hand: Hand;
	whose: Whose;
};

export const makeFieldFarm = (
	spawn: () => Worker,
	hands: number,
): FieldFarm => {
	let next = 1;
	const numbers = new WeakMap<object, number>();
	const numberOf = (o: object) => {
		let n = numbers.get(o);
		if (n === undefined) {
			n = next++;
			numbers.set(o, n);
		}
		return n;
	};
	// Each look as it is sent, and its logo (null: it can't be - a picture
	// on it that may not be read - and it is made on the page).
	const sent = new WeakMap<
		FieldLook,
		{ value: SentLook; assets: [number, ImageData][] } | null
	>();
	const sendable = (look: FieldLook) => {
		let s = sent.get(look);
		if (s === undefined) {
			const assets: [number, ImageData][] = [];
			const { logo, ...rest } = look;
			const value: SentLook = { ...rest };
			s = { value, assets };
			if (logo && logo.width > 0 && logo.height > 0) {
				let pic: ImageData | undefined;
				try {
					pic = logo
						.getContext("2d")!
						.getImageData(0, 0, logo.width, logo.height);
				} catch {
					pic = undefined;
				}
				if (pic) {
					value.logo = numberOf(logo);
					assets.push([value.logo, pic]);
				} else {
					s = null;
				}
			}
			sent.set(look, s);
		}
		return s ?? undefined;
	};

	const tickets = new Map<number, Ticket>();
	const busy = new WeakMap<SpriteCache, Set<string>>();
	const made = new WeakMap<SpriteCache, FieldMade[]>();
	let jobs = 0;
	const settle = (n: number, shaped?: SentShape, ms?: number) => {
		const t = tickets.get(n);
		if (!t) {
			return;
		}
		tickets.delete(n);
		t.hand.out -= 1;
		if (ms !== undefined) {
			const was = t.hand.ms;
			t.hand.ms = was === undefined ? ms : was + (ms - was) * 0.2;
		}
		busy.get(t.cache)?.delete(t.key);
		// (Nothing back - a newer pose of his came first, nothing of him
		// shows, or the hand is gone - or the cache let go of: nothing to
		// keep.)
		if (!shaped || t.cache.done) {
			return;
		}
		let list = made.get(t.cache);
		if (!list) {
			list = [];
			made.set(t.cache, list);
		}
		const [data, w, h] = shaped.pixels;
		list.push({
			key: t.key,
			shaped: { ...shaped, pixels: new ImageData(data, w, h) },
			...t.whose,
		});
	};
	// A hand gone (or never able): what it had out is made on the page.
	const lose = (hand: Hand) => {
		hand.ready = false;
		hand.worker.terminate();
		for (const [n, t] of tickets) {
			if (t.hand === hand) {
				settle(n);
			}
		}
	};

	const all: Hand[] = [];
	for (let i = 0; i < hands; i++) {
		let worker: Worker;
		try {
			worker = spawn();
		} catch {
			break;
		}
		const hand: Hand = {
			worker,
			ready: false,
			looks: new Set(),
			assets: new Set(),
			out: 0,
		};
		worker.onmessage = (event: MessageEvent<{ farm?: FromFieldHand }>) => {
			const msg = event.data?.farm;
			if (!msg) {
				return;
			}
			if ("ready" in msg) {
				if (msg.ready) {
					hand.ready = true;
				} else {
					lose(hand);
				}
				return;
			}
			settle(msg.job, msg.shaped, msg.ms);
		};
		worker.onerror = (event) => {
			event.preventDefault();
			lose(hand);
		};
		worker.postMessage({ farm: { hello: true } satisfies ToFieldHand });
		all.push(hand);
	}

	return {
		ask: (cache, key, work, look, whose) => {
			if (cache.done) {
				return false;
			}
			let hand: Hand | undefined;
			for (const h of all) {
				if (h.ready && h.out < outFor(h) && (!hand || h.out < hand.out)) {
					hand = h;
				}
			}
			const s = hand && sendable(look);
			if (!hand || !s) {
				return false;
			}
			const w = hand.worker;
			const ln = numberOf(look);
			const n = ++jobs;
			// (One lane a man: a newer pose of his takes the place of one the
			// hand has not got to yet.)
			const lane = `${numberOf(cache)}/${whose.id}`;
			// (Anything that can't be sent - it never should be - is made on
			// the page.)
			try {
				for (const [an, pic] of s.assets) {
					if (!hand.assets.has(an)) {
						w.postMessage({ farm: { asset: an, pic } satisfies ToFieldHand });
						hand.assets.add(an);
					}
				}
				if (!hand.looks.has(ln)) {
					w.postMessage({
						farm: { look: ln, value: s.value } satisfies ToFieldHand,
					});
					hand.looks.add(ln);
				}
				w.postMessage({
					farm: { job: n, look: ln, work, lane } satisfies ToFieldHand,
				});
			} catch {
				sent.set(look, null);
				return false;
			}
			hand.out += 1;
			tickets.set(n, { cache, key, hand, whose });
			let b = busy.get(cache);
			if (!b) {
				b = new Set();
				busy.set(cache, b);
			}
			b.add(key);
			return true;
		},
		busy: (cache, key) => busy.get(cache)?.has(key) === true,
		take: (cache) => {
			const list = made.get(cache) ?? [];
			made.delete(cache);
			return list;
		},
		stop: () => {
			for (const h of all) {
				h.ready = false;
				h.worker.terminate();
			}
			tickets.clear();
		},
	};
};

// THE HAND, in the worker: given looks, it sculpts the men asked of it
// exactly as the page would (see shapeField), and sends back their pixels.
export const fieldFarmHand = (
	post: (msg: { farm: FromFieldHand }, transfer?: ArrayBuffer[]) => void,
) => {
	const pics = new Map<number, HTMLCanvasElement>();
	const looks = new Map<number, FieldLook>();
	// A logo sent as pixels, on a canvas of its own to be read from (see
	// sculpt.ts decalOf).
	const canvasOf = (img: ImageData) => {
		const cv = new OffscreenCanvas(img.width, img.height);
		cv.getContext("2d", { willReadFrequently: true })!.putImageData(img, 0, 0);
		return cv as unknown as HTMLCanvasElement;
	};
	const sculpted = (msg: JobMsg) => {
		const look = looks.get(msg.look);
		let back: Back = { job: msg.job };
		const transfer: ArrayBuffer[] = [];
		const began = performance.now();
		try {
			const sh = look ? shapeField(msg.work, look) : undefined;
			if (sh) {
				const data = sh.pixels.data as Uint8ClampedArray<ArrayBuffer>;
				transfer.push(data.buffer);
				back = {
					job: msg.job,
					shaped: {
						dx: sh.dx,
						dy: sh.dy,
						anchor: sh.anchor,
						pixels: [data, sh.pixels.width, sh.pixels.height],
					},
				};
			}
		} catch {
			back = { job: msg.job };
		}
		back.ms = performance.now() - began;
		post({ farm: back }, transfer);
	};
	// The jobs waiting, a man's newest in his place in the line; done one a
	// turn, so a newer one can come in between.
	const waiting = new Map<string, JobMsg>();
	let due = false;
	const tick = new MessageChannel();
	tick.port1.onmessage = () => {
		due = false;
		const next = waiting.entries().next();
		if (next.done) {
			return;
		}
		const [lane, msg] = next.value;
		waiting.delete(lane);
		sculpted(msg);
		if (waiting.size > 0) {
			due = true;
			tick.port2.postMessage(0);
		}
	};
	return (msg: ToFieldHand) => {
		if ("hello" in msg) {
			// (No canvas to be had off the page - an older browser's worker -
			// for his jersey's lettering, and it can do nothing.)
			let ready = false;
			try {
				ready = new OffscreenCanvas(1, 1).getContext("2d") !== null;
			} catch {
				ready = false;
			}
			post({ farm: { ready } });
		} else if ("pic" in msg) {
			pics.set(msg.asset, canvasOf(msg.pic));
		} else if ("work" in msg) {
			// Done in its turn - unless a newer pose of his comes first.
			const was = waiting.get(msg.lane);
			if (was) {
				post({ farm: { job: was.job } });
			}
			waiting.set(msg.lane, msg);
			if (!due) {
				due = true;
				tick.port2.postMessage(0);
			}
		} else {
			const { logo, ...rest } = msg.value;
			const pic = logo === undefined ? undefined : pics.get(logo);
			looks.set(msg.look, { ...rest, ...(pic ? { logo: pic } : {}) });
		}
	};
};
