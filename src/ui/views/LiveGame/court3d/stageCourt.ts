import {
	compileCourt,
	type CourtInput,
	type CourtTimeline,
} from "./director.ts";

// A whole game takes the director seconds to stage - longer on a phone - and
// staged on the page's own thread, nothing on the page moves while it is: a
// black court, a page that will not answer. So it is staged in a worker of
// its own (src/court), and only here if that worker cannot be had.

type Waiting = {
	input: CourtInput;
	resolve: (tl: CourtTimeline) => void;
	reject: (error: Error) => void;
};

let worker: Worker | undefined;
// Whether it got going at all.
let ready = false;
let unavailable = false;
let nextId = 0;
const waiting = new Map<number, Waiting>();

const here = ({ input, resolve, reject }: Waiting) => {
	try {
		resolve(compileCourt(input));
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
		w.reject(new Error("3D court worker died"));
	}
};

const courtWorker = (): Worker | undefined => {
	if (worker || unavailable || typeof Worker === "undefined") {
		return worker;
	}
	try {
		worker = new Worker(
			__NODE_ENV === "production"
				? `/gen/court-${window.bbgmVersion}.js`
				: "/gen/court.js",
			{ type: "module" },
		);
	} catch {
		unavailable = true;
		return undefined;
	}
	ready = false;
	worker.onmessage = (
		event: MessageEvent<{
			ready?: true;
			id: number;
			timeline?: CourtTimeline;
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
			w.reject(new Error(error ?? "3D court failed to stage"));
		}
	};
	worker.onerror = (event) => {
		event.preventDefault();
		lost();
	};
	return worker;
};

export const stageCourt = (input: CourtInput): Promise<CourtTimeline> =>
	new Promise((resolve, reject) => {
		const w = courtWorker();
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
