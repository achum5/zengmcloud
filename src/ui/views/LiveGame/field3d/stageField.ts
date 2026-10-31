import {
	compileField,
	type FieldInput,
	type FieldTimeline,
} from "./director.ts";
import { spawnFieldWorker } from "./farm.ts";

// A whole game takes the director a good second to stage - longer on a
// phone - and staged on the page's own thread, nothing on the page moves
// while it is (see court3d/stageCourt.ts). So it is staged in a worker of
// its own (src/field), and only here if that worker cannot be had.

type Waiting = {
	input: FieldInput;
	resolve: (tl: FieldTimeline) => void;
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
		resolve(compileField(input));
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
		w.reject(new Error("3D field worker died"));
	}
};

const fieldWorker = (): Worker | undefined => {
	if (worker || unavailable || typeof Worker === "undefined") {
		return worker;
	}
	try {
		worker = spawnFieldWorker();
	} catch {
		unavailable = true;
		return undefined;
	}
	ready = false;
	worker.onmessage = (
		event: MessageEvent<{
			ready?: true;
			id: number;
			timeline?: FieldTimeline;
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
			w.reject(new Error(error ?? "3D field failed to stage"));
		}
	};
	worker.onerror = (event) => {
		event.preventDefault();
		lost();
	};
	return worker;
};

export const stageField = (input: FieldInput): Promise<FieldTimeline> =>
	new Promise((resolve, reject) => {
		const w = fieldWorker();
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
