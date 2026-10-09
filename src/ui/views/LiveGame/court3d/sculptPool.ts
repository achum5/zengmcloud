import type { Camera } from "./camera.ts";
import type { CourtTimeline, compileCourt } from "./director.ts";
import type { PlayerState } from "./evaluate.ts";
import type { Look } from "./figure.ts";
import type { Body } from "./poses.ts";
import type { Sculpted } from "./sculpt.ts";

// THE 3D COURT'S HEAVY LIFTING, ON THE SIDE.
//
// Two things the court does would otherwise hold up the page's own thread,
// so a few workers do them instead (see courtWorker.ts):
//
// Staging a game from its play-by-play (see director.ts) - seconds of work,
// all at once, that froze the whole page as a game opened.
//
// Sculpting sprites - every pixel of a man worked out from his pose (see
// sculpt.ts) - the costliest thing in a frame, and the picture is only as
// sharp as there is time for: a few new poses come round every frame, at a
// few milliseconds each, more the finer the picture. New poses are sculpted
// side by side, and the last picture of each man stands in for the frame or
// two until his new one is back (see drawSprite).
//
// Where there are no workers to be had - an old browser, a test, a build
// without the worker's script - it is all done on the spot, as before.

export type SculptJob = {
	cam: Camera;
	st: PlayerState;
	body: Body;
	px: number;
	ox: number;
	oy: number;
	w: number;
	h: number;
};

export type CompileInput = Parameters<typeof compileCourt>[0];

export type CourtRequest =
	| { type: "look"; id: number; look: Look }
	| { type: "job"; id: number; lookId: number; job: SculptJob }
	| { type: "compile"; id: number; refsId: number; input: CompileInput };

export type CourtReply = {
	id: number;
	made?: Sculpted;
	tl?: CourtTimeline;
	refs?: CourtTimeline["refs"];
};

type Slot = {
	worker: Worker;
	// Jobs in hand.
	busy: number;
	// The looks it has been sent.
	looks: Set<number>;
};

// How many jobs each worker may have in hand at once - enough to keep it
// busy, few enough that what comes back is still wanted.
const DEPTH = 2;
const MAX_WORKERS = 4;

type Pool = {
	slots: Slot[];
	waiting: Map<
		number,
		{ slot: Slot; weight: number; done: (reply?: CourtReply) => void }
	>;
	next: number;
	// Poses in a row the workers could not sculpt. A few, and they are let go:
	// a man left standing in for himself in an old pose for good is far worse
	// than sculpting him here.
	failed: number;
};
const MAX_FAILED = 4;

// undefined: not tried yet; null: none to be had.
let pool: Pool | null | undefined;

const lookIds = new WeakMap<Look, number>();
let nextLook = 1;

// Where the worker's script is: built alongside the page's (see
// tools/lib/rolldownConfig.ts) - or, for a test page, wherever it says.
const scriptUrl = (): string | undefined => {
	const set = (globalThis as { __courtWorkerUrl?: unknown }).__courtWorkerUrl;
	if (typeof set === "string") {
		return set;
	}
	if (typeof window === "undefined" || typeof window.bbgmVersion !== "string") {
		return undefined;
	}
	return typeof __NODE_ENV !== "undefined" && __NODE_ENV === "production"
		? `/gen/court-${window.bbgmVersion}.js`
		: "/gen/court.js";
};

// Anything going wrong - the script missing, a worker failing - and the
// workers are let go for good: everything back to the page's own thread.
const giveUp = () => {
	const p = pool;
	pool = null;
	if (!p) {
		return;
	}
	for (const s of p.slots) {
		s.worker.terminate();
	}
	for (const w of p.waiting.values()) {
		w.done(undefined);
	}
	p.waiting.clear();
};

const start = (): Pool | null => {
	const url = scriptUrl();
	if (
		!url ||
		typeof Worker === "undefined" ||
		typeof OffscreenCanvas === "undefined"
	) {
		return null;
	}
	const cores =
		typeof navigator !== "undefined" ? navigator.hardwareConcurrency || 4 : 4;
	// One core left for the page itself.
	const n = Math.max(1, Math.min(MAX_WORKERS, cores - 1));
	const p: Pool = { slots: [], waiting: new Map(), next: 1, failed: 0 };
	try {
		for (let i = 0; i < n; i++) {
			const worker = new Worker(url, { type: "module" });
			const slot: Slot = { worker, busy: 0, looks: new Set() };
			worker.onmessage = (e: MessageEvent<CourtReply>) => {
				const w = p.waiting.get(e.data.id);
				if (!w) {
					return;
				}
				p.waiting.delete(e.data.id);
				w.slot.busy -= w.weight;
				w.done(e.data);
			};
			worker.onerror = giveUp;
			worker.onmessageerror = giveUp;
			p.slots.push(slot);
		}
	} catch {
		for (const s of p.slots) {
			s.worker.terminate();
		}
		return null;
	}
	return p;
};

const ready = (): Pool | undefined => {
	if (pool === undefined) {
		pool = start();
	}
	return pool ?? undefined;
};

// The least busy worker with room for one more, if any.
const freest = (p: Pool, depth: number): Slot | undefined => {
	let slot: Slot | undefined;
	for (const s of p.slots) {
		if (s.busy < depth && (!slot || s.busy < slot.busy)) {
			slot = s;
		}
	}
	return slot;
};

// Each reply waited for, and how much of the worker's hands it takes up.
type Wait = {
	id: number;
	weight: number;
	done: (reply?: CourtReply) => void;
};
const send = (
	p: Pool,
	slot: Slot,
	msgs: CourtRequest[],
	waits: Wait[],
): boolean => {
	try {
		for (const m of msgs) {
			slot.worker.postMessage(m);
		}
	} catch {
		giveUp();
		return false;
	}
	for (const { id, weight, done } of waits) {
		slot.busy += weight;
		p.waiting.set(id, { slot, weight, done });
	}
	return true;
};

// Hand a pose off to be sculpted: `done` gets his body (rimmed), or
// nothing if it could not be. False if every worker has its hands full, or
// there are none - sculpt it here, or try again next frame.
export const sculptAside = (
	look: Look,
	job: SculptJob,
	done: (made?: Sculpted) => void,
): boolean => {
	const p = ready();
	const slot = p && freest(p, DEPTH);
	if (!p || !slot) {
		return false;
	}
	let lookId = lookIds.get(look);
	if (lookId === undefined) {
		lookId = nextLook++;
		lookIds.set(look, lookId);
	}
	const id = p.next++;
	const msgs: CourtRequest[] = [];
	if (!slot.looks.has(lookId)) {
		// Everything but his face, which is drawn here, on the page.
		const { head: _head, ...rest } = look;
		msgs.push({ type: "look", id: lookId, look: rest });
	}
	msgs.push({ type: "job", id, lookId, job });
	const sent = send(p, slot, msgs, [
		{
			id,
			weight: 1,
			done: (reply) => {
				done(reply?.made);
				if (pool === p && reply) {
					p.failed = reply.made ? 0 : p.failed + 1;
					if (p.failed >= MAX_FAILED) {
						giveUp();
					}
				}
			},
		},
	]);
	if (sent) {
		slot.looks.add(lookId);
	}
	return sent;
};

// Stage a game aside: `done` gets its timeline - or nothing, if it could
// not be staged there (stage it here then) - and `refs`, a little later,
// where its officials go all game (see crew.ts). False if there are no
// workers.
export const compileAside = (
	input: CompileInput,
	done: (tl?: CourtTimeline) => void,
	refs: (refs: CourtTimeline["refs"]) => void,
): boolean => {
	const p = ready();
	// (Whichever has least in hand, however much that is.)
	const slot = p && freest(p, Infinity);
	if (!p || !slot) {
		return false;
	}
	const id = p.next++;
	const refsId = p.next++;
	return send(
		p,
		slot,
		[{ type: "compile", id, refsId, input }],
		[
			{ id, weight: 1, done: (reply) => done(reply?.tl) },
			// (No sprites to it while it works on these: they would wait.)
			{ id: refsId, weight: DEPTH, done: (reply) => refs(reply?.refs) },
		],
	);
};
