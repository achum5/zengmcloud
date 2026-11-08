// STAGING A GAME OFF THE PAGE, for any sport. A whole game takes a director
// seconds to stage - longer on a phone - and staged on the page's own
// thread, nothing on the page moves while it is: a black picture, a page
// that will not answer. So it is staged in a worker of its own (src/court
// for basketball, src/field for football), and only here if that worker
// cannot be had.

type Waiting<I, T> = {
	input: I;
	resolve: (tl: T) => void;
	reject: (error: Error) => void;
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
}): ((input: I) => Promise<T>) => {
	let worker: Worker | undefined;
	// Whether it got going at all.
	let ready = false;
	let unavailable = false;
	let nextId = 0;
	const waiting = new Map<number, Waiting<I, T>>();

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
			const w = waiting.get(id);
			if (!w) {
				return;
			}
			waiting.delete(id);
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

	return (input: I): Promise<T> =>
		new Promise((resolve, reject) => {
			const w = workerFor();
			const entry = { input, resolve, reject };
			if (!w) {
				here(entry);
				return;
			}
			const id = nextId++;
			waiting.set(id, entry);
			try {
				w.postMessage({ id, input });
			} catch {
				waiting.delete(id);
				here(entry);
			}
		});
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
		}
	};
};
