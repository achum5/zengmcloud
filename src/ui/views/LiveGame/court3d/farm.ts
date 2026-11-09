import type { Look } from "./figure.ts";
import type { KitArt } from "./kitArt.ts";
import {
	shape,
	type Farm,
	type Job,
	type Shaped,
	type SpriteCache,
} from "./sprite.ts";

// NEW PICTURES SCULPTED OFF THE PAGE'S THREAD - any sport's (the court's
// below, field3d/farm.ts football's: each says how its men go and come
// back - see FarmCodec).
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

// HOW A SPORT'S MEN GO TO A HAND AND COME BACK. A look is sent once, its
// pictures taken off it to go as pictures of their own - its uniform's
// (`art`) and one more (`pic`: a crest, a helmet's logo) - and put back
// together there; he is sculpted there as on the page (`shape` - nothing,
// if nothing of him shows), and what comes of it goes back as plain arrays
// (`pack`, its buffers in `transfer`) and is taken back (`unpack`).
export type FarmCodec<L extends object, J, Sh, Sent> = {
	split: (look: L) => {
		rest: object;
		art?: KitArt;
		pic?: HTMLCanvasElement;
	};
	join: (rest: object, art?: KitArt, pic?: HTMLCanvasElement) => L;
	shape: (job: J, look: L) => Sh | undefined;
	pack: (sh: Sh, transfer: ArrayBuffer[]) => Sent;
	unpack: (sent: Sent) => Sh;
};

// A man sculpted off the page's thread, for the page to keep (or finish):
// the job and look it was for, and whose it is (`W`: who he is, the frame it
// was asked for in, his size then...).
export type MadeOf<L, J, Sh, W> = {
	key: string;
	shaped: Sh;
	job: J;
	look: L;
} & W;
export type FarmOf<L, J, Sh, W> = {
	// Takes on sculpting this picture for the cache, if it can now (false:
	// it can't - it is made here or not at all).
	ask: (cache: SpriteCache, key: string, job: J, look: L, whose: W) => boolean;
	// Whether it is still being made.
	busy: (cache: SpriteCache, key: string) => boolean;
	// The pictures made for the cache since last asked.
	take: (cache: SpriteCache) => MadeOf<L, J, Sh, W>[];
	stop: () => void;
};

// What a look is sent as: its pictures by number, each sent once.
type SentLook = { rest: object; art?: number; pic?: number };
// The page to a hand: each job in its lane (one a man - see makeFarmOf)...
type JobMsg<J> = { job: number; look: number; work: J; lane: string };
export type ToHandOf<J> =
	| { hello: true }
	| { asset: number; art: KitArt }
	| { asset: number; pic: ImageData }
	| { look: number; value: SentLook }
	| JobMsg<J>;
// ... and back: whether it can sculpt at all, and each man sculpted
// (nothing if he could not be, or nothing of him shows).
type Back<Sent> = { job: number; shaped?: Sent };
export type FromHandOf<Sent> = { ready: boolean } | Back<Sent>;

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

export const makeFarmOf = <L extends object, J, Sh, Sent, W>(
	spawn: () => Worker,
	hands: number,
	codec: FarmCodec<L, J, Sh, Sent>,
): FarmOf<L, J, Sh, W> => {
	type Ticket = {
		cache: SpriteCache;
		key: string;
		hand: Hand;
		job: J;
		look: L;
		whose: W;
	};
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
		L,
		{ value: SentLook; assets: [number, Asset][] } | null
	>();
	const sendable = (look: L) => {
		let s = sent.get(look);
		if (s === undefined) {
			const assets: [number, Asset][] = [];
			const { rest, art, pic } = codec.split(look);
			const value: SentLook = { rest };
			s = { value, assets };
			if (art) {
				value.art = numberOf(art);
				assets.push([value.art, { art }]);
			}
			if (pic && pic.width > 0 && pic.height > 0) {
				let px: ImageData | undefined;
				try {
					px = pic.getContext("2d")!.getImageData(0, 0, pic.width, pic.height);
				} catch {
					px = undefined;
				}
				if (px) {
					value.pic = numberOf(pic);
					assets.push([value.pic, { pic: px }]);
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
	const made = new WeakMap<SpriteCache, MadeOf<L, J, Sh, W>[]>();
	let jobs = 0;
	const settle = (n: number, shaped?: Sent) => {
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
		list.push({
			key: t.key,
			shaped: codec.unpack(shaped),
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
		worker.onmessage = (event: MessageEvent<{ farm?: FromHandOf<Sent> }>) => {
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
		worker.postMessage({ farm: { hello: true } satisfies ToHandOf<J> });
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
			const lane = `${numberOf(cache)}/${(whose as { id: number }).id}`;
			// (Anything that can't be sent - it never should be - is made on
			// the page.)
			try {
				for (const [an, a] of s.assets) {
					if (!hand.assets.has(an)) {
						w.postMessage({ farm: { asset: an, ...a } satisfies ToHandOf<J> });
						hand.assets.add(an);
					}
				}
				if (!hand.looks.has(ln)) {
					w.postMessage({
						farm: { look: ln, value: s.value } satisfies ToHandOf<J>,
					});
					hand.looks.add(ln);
				}
				w.postMessage({
					farm: { job: n, look: ln, work, lane } satisfies ToHandOf<J>,
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
// exactly as the page would (see the codec's shape), and sends back their
// pixels.
export const farmHandOf = <L extends object, J, Sh, Sent>(
	post: (msg: { farm: FromHandOf<Sent> }, transfer?: ArrayBuffer[]) => void,
	codec: FarmCodec<L, J, Sh, Sent>,
) => {
	const arts = new Map<number, KitArt>();
	const pics = new Map<number, HTMLCanvasElement>();
	const looks = new Map<number, L>();
	// A picture sent as pixels, on a canvas of its own to be drawn from (by
	// his lettering's sheet, read back - so kept in plain memory).
	const canvasOf = (img: ImageData) => {
		const cv = new OffscreenCanvas(img.width, img.height);
		cv.getContext("2d", { willReadFrequently: true })!.putImageData(img, 0, 0);
		return cv as unknown as HTMLCanvasElement;
	};
	const sculpted = (msg: JobMsg<J>) => {
		const look = looks.get(msg.look);
		let back: Back<Sent> = { job: msg.job };
		const transfer: ArrayBuffer[] = [];
		try {
			const sh = look ? codec.shape(msg.work, look) : undefined;
			if (sh) {
				back = { job: msg.job, shaped: codec.pack(sh, transfer) };
			}
		} catch {
			back = { job: msg.job };
			transfer.length = 0;
		}
		post({ farm: back }, transfer);
	};
	// The jobs waiting, a man's newest in his place in the line; done one a
	// turn, so a newer one can come in between.
	const waiting = new Map<string, JobMsg<J>>();
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
	return (msg: ToHandOf<J>) => {
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
			const { rest, art, pic } = msg.value;
			looks.set(
				msg.look,
				codec.join(
					rest,
					art === undefined ? undefined : arts.get(art),
					pic === undefined ? undefined : pics.get(pic),
				),
			);
		}
	};
};

// THE COURT'S: no head (the page draws that on - a worker's canvas draws
// its edges a little rougher than the page's), the crest on his warm-up
// top as its picture; his body back as pixels, and what goes over his head
// (see Shaped).
export type Pixels = [Uint8ClampedArray<ArrayBuffer>, number, number];
type SentShape = Omit<Shaped, "img" | "over"> & {
	img: Pixels;
	over?: Pixels;
};
export const sendPixels = (img: ImageData, transfer: ArrayBuffer[]): Pixels => {
	const data = img.data as Uint8ClampedArray<ArrayBuffer>;
	transfer.push(data.buffer);
	return [data, img.width, img.height];
};
const COURT: FarmCodec<Look, Job, Shaped, SentShape> = {
	split: ({ kitArt, crest, head: _head, ...rest }) => ({
		rest,
		...(kitArt ? { art: kitArt } : {}),
		...(crest ? { pic: crest } : {}),
	}),
	join: (rest, art, pic) => ({
		...(rest as Omit<Look, "kitArt" | "crest" | "head">),
		...(art ? { kitArt: art } : {}),
		...(pic ? { crest: pic } : {}),
	}),
	shape: (job, look) => shape(job, look, true),
	pack: ({ img, over, ...sh }, transfer) => ({
		...sh,
		img: sendPixels(img, transfer),
		...(over ? { over: sendPixels(over, transfer) } : {}),
	}),
	unpack: ({ img, over, ...sh }) => ({
		...sh,
		img: new ImageData(...img),
		...(over ? { over: new ImageData(...over) } : {}),
	}),
};

export type ToHand = ToHandOf<Job>;
export type FromHand = FromHandOf<SentShape>;
export const makeFarm = (
	spawn: () => Worker,
	hands: number,
): Farm & { stop: () => void } => makeFarmOf(spawn, hands, COURT);
export const farmHand = (
	post: (msg: { farm: FromHand }, transfer?: ArrayBuffer[]) => void,
) => farmHandOf(post, COURT);
