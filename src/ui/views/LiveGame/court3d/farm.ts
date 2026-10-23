import type { Look } from "./figure.ts";
import type { KitArt } from "./kitArt.ts";
import {
	shape,
	type Farm,
	type Job,
	type Made,
	type Shaped,
	type SpriteCache,
} from "./sprite.ts";

// NEW PICTURES SCULPTED OFF THE PAGE'S THREAD.
//
// Sculpting a man's new pose (see sculpt.ts) is most of what a frame costs:
// a few new ones a frame, every frame the play goes on, a millisecond or two
// each on a laptop. Done on the page's own thread, that is time the frame
// has not got - the men drawn a frame or more behind their pose, and on a
// slower device the picture stepped coarser (see resolution.ts).
//
// So whoever can be drawn a frame as he was is sculpted by a hand off the
// page - a worker, another instance of the one the game is staged in (see
// src/court) - and drawn in the picture he had till his body comes back a
// frame or two later, as pixels. The page only finishes him: his head drawn
// on (a worker's canvas draws its edges a little rougher than the page's),
// and the picture kept. Whoever has no picture to wait in is made on the
// page as before, and so is everybody where there is no hand to be had.
//
// A hand is sent each look once - his team's pictures as pixels - and then
// only the job: the camera, his pose, his build and the box he is drawn in
// (see Job).

// What a look is sent as: no head (the page draws that), its pictures by
// number, each sent once.
type SentLook = Omit<Look, "kitArt" | "crest" | "head"> & {
	kitArt?: number;
	crest?: number;
};
// The page to a hand: each job in its lane (one a man - see makeFarm)...
type JobMsg = { job: number; look: number; work: Job; lane: string };
export type ToHand =
	| { hello: true }
	| { asset: number; art: KitArt }
	| { asset: number; pic: ImageData }
	| { look: number; value: SentLook }
	| JobMsg;
// ... and back: whether it can sculpt at all, and each man sculpted (his
// pixels as plain arrays; nothing if he could not be).
type SentShape = Omit<Shaped, "img" | "over"> & {
	img: [Uint8ClampedArray<ArrayBuffer>, number, number];
	over?: [Uint8ClampedArray<ArrayBuffer>, number, number];
};
type Back = { job: number; shaped?: SentShape };
export type FromHand = { ready: boolean } | Back;

// The most jobs a hand is given at once: as many as it has done in about
// two frames lately (and never fewer than a few, or more than a dozen) -
// more waiting than that would only come back too late to be drawn, the
// men made on the page anyway.
const MIN_OUT = 3;
const MAX_OUT = 12;
const PACE_MS = 600;
const OUT_MS = 35;

type Hand = {
	worker: Worker;
	ready: boolean;
	// What it has been sent.
	looks: Set<number>;
	assets: Set<number>;
	out: number;
	// When it finished each of its last jobs (ms).
	done: number[];
};
// How many jobs a hand may have out now (see MAX_OUT).
const outFor = (hand: Hand, now: number) => {
	while (hand.done.length > 0 && hand.done[0]! < now - PACE_MS) {
		hand.done.shift();
	}
	return Math.min(
		MAX_OUT,
		Math.max(MIN_OUT, Math.ceil((hand.done.length * OUT_MS) / PACE_MS)),
	);
};
type Ticket = {
	cache: SpriteCache;
	key: string;
	hand: Hand;
	job: Job;
	look: Look;
	whose: { id: number; frame: number; scale: number };
};

export const makeFarm = (
	spawn: () => Worker,
	hands: number,
): Farm & { stop: () => void } => {
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
	// Each look as it is sent, and the pictures it takes (null: it can't be
	// - a picture on it that may not be read - and is made on the page).
	type Asset = { art: KitArt } | { pic: ImageData };
	const sent = new WeakMap<
		Look,
		{ value: SentLook; assets: [number, Asset][] } | null
	>();
	const sendable = (look: Look) => {
		let s = sent.get(look);
		if (s === undefined) {
			const assets: [number, Asset][] = [];
			const { kitArt, crest, head: _head, ...rest } = look;
			const value: SentLook = { ...rest };
			s = { value, assets };
			if (kitArt) {
				value.kitArt = numberOf(kitArt);
				assets.push([value.kitArt, { art: kitArt }]);
			}
			if (crest) {
				let pic: ImageData | undefined;
				try {
					pic = crest
						.getContext("2d")!
						.getImageData(0, 0, crest.width, crest.height);
				} catch {
					pic = undefined;
				}
				if (pic) {
					value.crest = numberOf(crest);
					assets.push([value.crest, { pic }]);
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
	const made = new WeakMap<SpriteCache, Made[]>();
	let jobs = 0;
	const settle = (n: number, shaped?: SentShape) => {
		const t = tickets.get(n);
		if (!t) {
			return;
		}
		tickets.delete(n);
		t.hand.out -= 1;
		if (shaped) {
			t.hand.done.push(performance.now());
		}
		busy.get(t.cache)?.delete(t.key);
		// (Nothing back - a newer pose of his came first, or the hand is
		// gone - or the cache let go of: nothing to keep.)
		if (!shaped || t.cache.done) {
			return;
		}
		let list = made.get(t.cache);
		if (!list) {
			list = [];
			made.set(t.cache, list);
		}
		const image = ([data, w, h]: [
			Uint8ClampedArray<ArrayBuffer>,
			number,
			number,
		]) => new ImageData(data, w, h);
		const { img, over, ...rest } = shaped;
		list.push({
			key: t.key,
			shaped: {
				...rest,
				img: image(img),
				...(over ? { over: image(over) } : {}),
			},
			job: t.job,
			look: t.look,
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
			done: [],
		};
		worker.onmessage = (event: MessageEvent<{ farm?: FromHand }>) => {
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
			settle(msg.job, msg.shaped);
		};
		worker.onerror = (event) => {
			event.preventDefault();
			lose(hand);
		};
		worker.postMessage({ farm: { hello: true } satisfies ToHand });
		all.push(hand);
	}

	return {
		ask: (cache, key, work, look, whose) => {
			if (cache.done) {
				return false;
			}
			let hand: Hand | undefined;
			const now = performance.now();
			for (const h of all) {
				if (h.ready && h.out < outFor(h, now) && (!hand || h.out < hand.out)) {
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
				for (const [an, a] of s.assets) {
					if (!hand.assets.has(an)) {
						w.postMessage({ farm: { asset: an, ...a } satisfies ToHand });
						hand.assets.add(an);
					}
				}
				if (!hand.looks.has(ln)) {
					w.postMessage({
						farm: { look: ln, value: s.value } satisfies ToHand,
					});
					hand.looks.add(ln);
				}
				w.postMessage({
					farm: { job: n, look: ln, work, lane } satisfies ToHand,
				});
			} catch {
				sent.set(look, null);
				return false;
			}
			hand.out += 1;
			tickets.set(n, { cache, key, hand, job: work, look, whose });
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
// exactly as the page would (see shape), and sends back their pixels.
export const farmHand = (
	post: (msg: { farm: FromHand }, transfer?: ArrayBuffer[]) => void,
) => {
	const arts = new Map<number, KitArt>();
	const pics = new Map<number, HTMLCanvasElement>();
	const looks = new Map<number, Look>();
	// A picture sent as pixels, on a canvas of its own to be drawn from (by
	// his lettering's sheet, read back - so kept in plain memory).
	const canvasOf = (img: ImageData) => {
		const cv = new OffscreenCanvas(img.width, img.height);
		cv.getContext("2d", { willReadFrequently: true })!.putImageData(img, 0, 0);
		return cv as unknown as HTMLCanvasElement;
	};
	const sculpted = (msg: JobMsg) => {
		const look = looks.get(msg.look);
		let back: Back = { job: msg.job };
		const transfer: ArrayBuffer[] = [];
		try {
			if (look) {
				const { img, over, ...sh } = shape(msg.work, look, true);
				const send = (img: ImageData) => {
					const data = img.data as Uint8ClampedArray<ArrayBuffer>;
					transfer.push(data.buffer);
					return [data, img.width, img.height] as [
						Uint8ClampedArray<ArrayBuffer>,
						number,
						number,
					];
				};
				back = {
					job: msg.job,
					shaped: {
						...sh,
						img: send(img),
						...(over ? { over: send(over) } : {}),
					},
				};
			}
		} catch {
			back = { job: msg.job };
		}
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
	return (msg: ToHand) => {
		if ("hello" in msg) {
			// (No canvas to be had off the page - an older browser's worker -
			// for his lettering, and it can do nothing.)
			let ready = false;
			try {
				ready = new OffscreenCanvas(1, 1).getContext("2d") !== null;
			} catch {
				ready = false;
			}
			post({ farm: { ready } });
		} else if ("art" in msg) {
			arts.set(msg.asset, msg.art);
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
			const { kitArt, crest, ...rest } = msg.value;
			const art = kitArt === undefined ? undefined : arts.get(kitArt);
			const pic = crest === undefined ? undefined : pics.get(crest);
			looks.set(msg.look, {
				...rest,
				...(art ? { kitArt: art } : {}),
				...(pic ? { crest: pic } : {}),
			});
		}
	};
};
