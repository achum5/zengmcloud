import { assert, beforeEach, describe, test } from "vitest";
import { changeTracker } from "./changeTracker.ts";

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

describe("changeTracker suppression", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
		changeTracker.enable();
	});

	test("overlapping runSuppressed calls can never leave recording off", async () => {
		// The exact interleaving that used to permanently wedge sync: call B starts
		// while call A is running, A finishes first, B finishes last. With the old
		// boolean save/restore, B restored the "suppressed" it saw at start (true),
		// so recording stayed off FOREVER - every later sim/trade was invisible to
		// sync until a page refresh.
		let releaseA!: () => void;
		let releaseB!: () => void;
		const a = changeTracker.runSuppressed(
			() => new Promise<void>((resolve) => (releaseA = resolve)),
		);
		const b = changeTracker.runSuppressed(
			() => new Promise<void>((resolve) => (releaseB = resolve)),
		);

		releaseA();
		await a;
		await flush();
		releaseB();
		await b;

		changeTracker.record("players", 1, "put");
		assert.strictEqual(changeTracker.size(), 1, "recording must be back on");
	});

	test("a suppressed call spanning a sim start cannot swallow sim writes", async () => {
		let release!: () => void;
		const suppressed = changeTracker.runSuppressed(
			() => new Promise<void>((resolve) => (release = resolve)),
		);

		// The sim begins while the suppressed call is still in flight; its writes
		// must be recorded anyway.
		changeTracker.beginSim();
		changeTracker.record("players", 1, "put");
		changeTracker.endSim();

		release();
		await suppressed;

		assert.strictEqual(changeTracker.size(), 1);
	});

	test("runSuppressed still suppresses while active", async () => {
		await changeTracker.runSuppressed(async () => {
			changeTracker.record("players", 1, "put");
		});
		assert.strictEqual(changeTracker.size(), 0);
	});
});
