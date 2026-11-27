import { afterEach, assert, beforeEach, describe, test, vi } from "vitest";
import { fakeGame } from "./testGame.ts";
import { compileStaged, type StageInput } from "./director.ts";
import { goesOn, type Staged } from "./parts.ts";
import { dropStagedCourt, stageCourt } from "./stageCourt.ts";
import { makeStager } from "./stager.ts";

describe("3D: a staged game kept", () => {
	const { events, players } = fakeGame("kept-a", 40);
	const input = { events, players, gid: 7, step: false };

	test("the same game asked for again is the same staging, not another", async () => {
		const a = stageCourt(input);
		// (A new array of the same events, as the page gets on coming back.)
		const b = stageCourt({ ...input, events: [...events] });
		assert.strictEqual(a, b);
		await a;
	}, 60_000);

	test("another game, or the same one let go, is staged afresh", async () => {
		const a = stageCourt(input);
		const other = fakeGame("kept-b", 40);
		const b = stageCourt({ ...input, ...other, gid: 8 });
		assert.notStrictEqual(a, b);
		await b;
		const c = stageCourt(input);
		assert.notStrictEqual(a, c);
		dropStagedCourt(7);
		assert.notStrictEqual(c, stageCourt(input));
	}, 60_000);
});

// A stand-in for the court's worker: stages what it is asked for, a moment
// later.
class StagingWorker {
	onmessage?: (event: { data: unknown }) => void;
	onerror?: unknown;
	static asked: StageInput[] = [];
	constructor() {
		setTimeout(() => this.onmessage?.({ data: { ready: true } }), 0);
	}
	postMessage(msg: { id: number; input?: StageInput }) {
		const input = msg.input;
		if (!input) {
			return;
		}
		StagingWorker.asked.push(input);
		setTimeout(() => {
			this.onmessage?.({
				data: { id: msg.id, timeline: compileStaged(input) },
			});
		}, 0);
	}
	terminate() {}
}

describe("3D: a game staged a period at a time", () => {
	beforeEach(() => {
		StagingWorker.asked = [];
		vi.stubGlobal("Worker", StagingWorker);
	});
	afterEach(() => {
		vi.unstubAllGlobals();
	});
	const { events, players } = fakeGame("kept-parts", 50);
	const input = { events, players, gid: 21, step: false };

	test("the first part to play, then the game so far as each part comes", async () => {
		const seen: Staged[] = [];
		let done!: () => void;
		const all = new Promise<void>((resolve) => {
			done = resolve;
		});
		const first = await stageCourt(input, {
			more: (tl) => {
				seen.push(tl);
				if (tl.upTo === undefined) {
					done();
				}
			},
		});
		assert.isDefined(first.upTo);
		await all;
		assert.strictEqual(seen.length, 2);
		assert.isTrue(goesOn(first, seen[0]!));
		assert.isAbove(seen[0]!.upTo!, first.upTo!);
		// (One part asked for at a time, each once the one before is in.)
		assert.deepEqual(
			StagingWorker.asked.map((a) => a.part?.from),
			[0, first.upTo, seen[0]!.upTo],
		);
		const whole = compileStaged(input);
		assert.deepEqual(seen.at(-1)!.beats, whole.beats);
		// (Asked for again, the game as far as it is staged.)
		assert.strictEqual(await stageCourt(input), seen.at(-1));
	}, 60_000);

	test("let go: no more of it staged", async () => {
		const off = new AbortController();
		let more = 0;
		await stageCourt(
			{ ...input, gid: 22 },
			{ more: () => (more += 1), signal: off.signal },
		);
		// (Another game asked for, and this one's page gone.)
		void stageCourt({ ...input, gid: 23 }).catch(() => {});
		off.abort();
		await new Promise((resolve) => setTimeout(resolve, 200));
		// (The part it was on put down, and none asked for after it.)
		assert.strictEqual(more, 0);
		assert.isAtMost(StagingWorker.asked.filter((a) => a.gid === 22).length, 2);
	}, 60_000);
});

// A stand-in worker: answers the jobs it is sent, in order, when told to.
const fakeWorkers = () => {
	const made: {
		sent: number[];
		terminated: boolean;
		answer: () => void;
		// (Says it is still at the game it is on - see stagingTick.ts.)
		alive: () => void;
		onmessage?: (event: { data: any }) => void;
	}[] = [];
	const spawn = () => {
		const w: (typeof made)[number] & {
			postMessage: (msg: { id: number; input: number }) => void;
			terminate: () => void;
			onerror?: unknown;
		} = {
			sent: [],
			terminated: false,
			postMessage(msg) {
				w.sent.push(msg.id);
				queue.push(msg);
			},
			terminate() {
				w.terminated = true;
			},
			answer() {
				const msg = queue.shift();
				if (msg) {
					w.onmessage?.({ data: { id: msg.id, timeline: msg.input * 10 } });
				}
			},
			alive() {
				const msg = queue[0];
				if (msg) {
					w.onmessage?.({ data: { id: msg.id, alive: true } });
				}
			},
		};
		const queue: { id: number; input: number }[] = [];
		made.push(w);
		setTimeout(() => {
			w.onmessage?.({ data: { ready: true } });
		}, 0);
		return w as unknown as Worker;
	};
	return { made, spawn };
};

describe("3D: a staging abandoned", () => {
	// (Workers to be had, as on a device - the stand-ins below.)
	beforeEach(() => {
		vi.stubGlobal("Worker", class {});
	});
	afterEach(() => {
		vi.unstubAllGlobals();
	});

	test("the one the worker is on: the worker put down, the rest sent to a fresh one", async () => {
		const { made, spawn } = fakeWorkers();
		const stage = makeStager<number, number>({
			compile: (n) => n * 10,
			spawn,
			what: "test",
		});
		const a = stage(1);
		const b = stage(2);
		let aFailed = false;
		a.catch(() => {
			aFailed = true;
		});
		stage.abandon(a);
		await Promise.resolve();
		assert.isTrue(aFailed);
		assert.isTrue(made[0]!.terminated);
		assert.strictEqual(made.length, 2);
		made[1]!.answer();
		assert.strictEqual(await b, 20);
	});

	test("one still queued: just not answered, the worker kept on", async () => {
		const { made, spawn } = fakeWorkers();
		const stage = makeStager<number, number>({
			compile: (n) => n * 10,
			spawn,
			what: "test",
		});
		const a = stage(1);
		const b = stage(2);
		b.catch(() => {});
		stage.abandon(b);
		assert.isFalse(made[0]!.terminated);
		made[0]!.answer();
		assert.strictEqual(await a, 10);
		// (Its answer, when it comes, goes nowhere.)
		made[0]!.answer();
	});
});

// A phone puts a worker down for the memory a whole game takes, and says
// nothing: the page was left on the warm-ups for good. A worker that goes
// quiet is taken for put down - the game handed back, the next to a fresh
// worker - while one still saying it is at it is left to it, however long.
describe("3D: a staging worker gone quiet", () => {
	beforeEach(() => {
		vi.stubGlobal("Worker", class {});
		vi.useFakeTimers();
	});
	afterEach(() => {
		vi.useRealTimers();
		vi.unstubAllGlobals();
	});
	const stagerOf = () => {
		const { made, spawn } = fakeWorkers();
		const stage = makeStager<number, number>({
			compile: (n) => n * 10,
			spawn,
			what: "test",
		});
		return { made, stage };
	};

	test("still saying it is at it: left to it, however long it takes", async () => {
		const { made, stage } = stagerOf();
		const a = stage(1);
		await vi.advanceTimersByTimeAsync(1);
		for (let k = 0; k < 30; k++) {
			made[0]!.alive();
			await vi.advanceTimersByTimeAsync(10_000);
		}
		assert.isFalse(made[0]!.terminated);
		made[0]!.answer();
		assert.strictEqual(await a, 10);
	});

	test("quiet after saying it was at it: put down, the game handed back, the next to a fresh worker", async () => {
		const { made, stage } = stagerOf();
		const a = stage(1);
		let failed = "";
		a.catch((error: unknown) => {
			failed = String(error);
		});
		await vi.advanceTimersByTimeAsync(1);
		made[0]!.alive();
		await vi.advanceTimersByTimeAsync(41_000);
		assert.include(failed, "worker died");
		assert.isTrue(made[0]!.terminated);
		const b = stage(2);
		assert.strictEqual(made.length, 2);
		await vi.advanceTimersByTimeAsync(1);
		made[1]!.answer();
		assert.strictEqual(await b, 20);
	});

	test("never a word: given a whole game's time, then handed back", async () => {
		const { made, stage } = stagerOf();
		const a = stage(1);
		let failed = "";
		a.catch((error: unknown) => {
			failed = String(error);
		});
		await vi.advanceTimersByTimeAsync(200_000);
		assert.strictEqual(failed, "");
		assert.isFalse(made[0]!.terminated);
		await vi.advanceTimersByTimeAsync(50_000);
		assert.include(failed, "worker died");
	});
});
