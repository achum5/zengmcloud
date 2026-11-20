import { afterEach, assert, beforeEach, describe, test, vi } from "vitest";
import { fakeGame } from "./testGame.ts";
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
	});

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
	});
});

// A stand-in worker: answers the jobs it is sent, in order, when told to.
const fakeWorkers = () => {
	const made: {
		sent: number[];
		terminated: boolean;
		answer: () => void;
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
