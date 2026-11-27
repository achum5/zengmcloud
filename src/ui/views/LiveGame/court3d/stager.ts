import { setStagingTick } from "./stagingTick.ts";

// STAGING A GAME OFF THE PAGE, for any sport. A whole game takes a director
// seconds to stage - longer on a phone - and staged on the page's own
// thread, nothing on the page moves while it is: a black picture, a page
// that will not answer. So it is staged in a worker of its own (src/court
// for basketball, src/field for football), and only here if that worker
// cannot be had.

// A worker on a game says it is still at it every second or so (see
// stagingTick.ts). Silent this long, it is taken for put down - a phone
// puts a worker down for the memory a whole game takes, and says nothing -
// and the game goes back to whoever asked (see stageCourt.ts), not left
// waiting on an answer that will never come: the page sat on the warm-ups
// for good. (Till the first word on a game, longer: a worker that never
// says anything - not every sport's does - is given a whole game's time.)
const QUIET_MS = 40_000;
const QUIET_FIRST_MS = 240_000;

type Waiting<I, T> = {
	input: I;
	resolve: (tl: T) => void;
	reject: (error: Error) => void;
	// What the caller holds (see abandon).
	promise?: Promise<T>;
};

// `what` names it in errors ("court", "field").
export const makeStager = <I, T>({
	compile,
	spawn,
	what,
}: {
	compile: (input: I) => T;
	spawn: () => Worker;
	what: string;
}): Stager<I, T> => {
	let worker: Worker | undefined;
	// Whether it got going at all.
	let ready = false;
	let unavailable = false;
	let nextId = 0;
	const waiting = new Map<number, Waiting<I, T>>();
	// The game the worker is on, and how long it has to say it is still at
	// it before it is taken for put down (see QUIET_MS).
	let quiet: ReturnType<typeof setTimeout> | undefined;
	const hush = () => {
		clearTimeout(quiet);
		quiet = undefined;
	};
	const listen = (ms: number) => {
		hush();
		if (waiting.size > 0) {
			quiet = setTimeout(() => {
				quiet = undefined;
				lost();
			}, ms);
		}
	};

	const here = ({ input, resolve, reject }: Waiting<I, T>) => {
		try {
			resolve(compile(input));
		} catch (error) {
			reject(error instanceof Error ? error : new Error(String(error)));
		}
	};

	// The worker would not start: whatever it had is staged here, as is all
	// to come. Or it died on the job (out of memory, say): that game is not
	// to be had - better not try it again here - but the next may be, in a
	// fresh worker.
	const lost = () => {
		hush();
		worker?.terminate();
		worker = undefined;
		const left = [...waiting.values()];
		waiting.clear();
		if (!ready) {
			unavailable = true;
			for (const w of left) {
				here(w);
			}
			return;
		}
		for (const w of left) {
			w.reject(new Error(`3D ${what} worker died`));
		}
	};

	const workerFor = (): Worker | undefined => {
		if (worker || unavailable || typeof Worker === "undefined") {
			return worker;
		}
		try {
			worker = spawn();
		} catch {
			unavailable = true;
			return undefined;
		}
		ready = false;
		worker.onmessage = (
			event: MessageEvent<{
				ready?: true;
				alive?: true;
				id: number;
				timeline?: T;
				error?: string;
			}>,
		) => {
			const { id, timeline, error } = event.data;
			if (event.data.ready) {
				ready = true;
				return;
			}
			if (event.data.alive) {
				if (waiting.keys().next().value === id) {
					listen(QUIET_MS);
				}
				return;
			}
			const w = waiting.get(id);
			if (!w) {
				return;
			}
			waiting.delete(id);
			// (On to the next, if any.)
			listen(QUIET_FIRST_MS);
			if (timeline) {
				w.resolve(timeline);
			} else {
				w.reject(new Error(error ?? `3D ${what} failed to stage`));
			}
		};
		worker.onerror = (event) => {
			event.preventDefault();
			lost();
		};
		return worker;
	};

	// Which staging each promise is, to abandon it by.
	const ids = new WeakMap<Promise<T>, number>();
	const send = (entry: Waiting<I, T>) => {
		const w = workerFor();
		if (!w) {
			here(entry);
			return;
		}
		const id = nextId++;
		waiting.set(id, entry);
		if (waiting.size === 1) {
			listen(QUIET_FIRST_MS);
		}
		if (entry.promise) {
			ids.set(entry.promise, id);
		}
		try {
			w.postMessage({ id, input: entry.input });
		} catch {
			waiting.delete(id);
			here(entry);
		}
	};

	const stage = ((input: I): Promise<T> => {
		let entry: Waiting<I, T> | undefined;
		const promise = new Promise<T>((resolve, reject) => {
			entry = { input, resolve, reject };
		});
		entry!.promise = promise;
		send(entry!);
		return promise;
	}) as Stager<I, T>;

	// Not wanted after all. The worker stages one game at a time and cannot
	// be told to stop part way, so it is put down - and whatever else was
	// waiting goes to a fresh one. (Otherwise a game nobody is going to watch
	// kept a phone's worker busy for a minute, and the next game - this
	// device's own, say - waited behind it.)
	stage.abandon = (promise: Promise<T>) => {
		const id = ids.get(promise);
		const entry = id === undefined ? undefined : waiting.get(id);
		if (id === undefined || !entry) {
			return;
		}
		// (One still queued behind another is just not answered: putting the
		// worker down would lose the one it is on.)
		const onIt = waiting.keys().next().value === id;
		waiting.delete(id);
		entry.reject(new Error(`3D ${what} staging abandoned`));
		if (worker && onIt) {
			hush();
			worker.terminate();
			worker = undefined;
			const left = [...waiting.values()];
			waiting.clear();
			for (const w of left) {
				send(w);
			}
		}
	};

	// No worker to be had: staged here, on the page's own thread.
	stage.onPage = () => unavailable || typeof Worker === "undefined";

	return stage;
};

export type Stager<I, T> = ((input: I) => Promise<T>) & {
	abandon: (promise: Promise<T>) => void;
	onPage: () => boolean;
};

// THE WORKER'S SIDE: up and running, then each game it is asked for staged
// and sent back - and the farm's jobs (see farm.ts) handed to `hand`.
export const serveStaging = <I, H>(
	compile: (input: I) => unknown,
	hand: (job: H) => void,
) => {
	self.postMessage({ ready: true });
	self.onmessage = (
		event: MessageEvent<{ id: number; input: I } | { farm?: H }>,
	) => {
		if ("farm" in event.data) {
			if (event.data.farm) {
				hand(event.data.farm);
			}
			return;
		}
		if (!("input" in event.data)) {
			return;
		}
		const { id, input } = event.data;
		// (Still at it: see stagingTick.ts, and the page's side's QUIET_MS.)
		setStagingTick(() => self.postMessage({ id, alive: true }));
		try {
			self.postMessage({ id, timeline: compile(input) });
		} catch (error) {
			self.postMessage({
				id,
				error:
					error instanceof Error
						? (error.stack ?? error.message)
						: String(error),
			});
		} finally {
			setStagingTick(undefined);
		}
	};
};
