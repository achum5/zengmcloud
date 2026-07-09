import { assert, beforeEach, describe, test } from "vitest";
import { changeTracker } from "./changeTracker.ts";

describe("changeTracker capture windows", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
		changeTracker.enable();
	});

	test("writes record only while a capture or sim window is open", async () => {
		// No window: a write from a non-tracked call (view load, import) records
		// nothing.
		changeTracker.record("players", 1, "put");
		assert.strictEqual(changeTracker.size(), 0);

		await changeTracker.runCaptured(async () => {
			changeTracker.record("players", 1, "put");
		});
		assert.strictEqual(changeTracker.size(), 1);
	});

	test("a concurrent non-tracked call can never switch recording off", async () => {
		// The old model suppressed recording globally around non-tracked calls, so
		// an action's writes made while a view load was in flight were silently
		// swallowed - and two overlapping suppressed calls could even leave
		// recording off FOREVER (the wedge that killed sync until refresh). Under
		// the opt-in model the action's own window keeps recording no matter what
		// else is running.
		let releaseSuppressed!: () => void;
		const suppressed = changeTracker.runSuppressed(
			() => new Promise<void>((resolve) => (releaseSuppressed = resolve)),
		);

		await changeTracker.runCaptured(async () => {
			changeTracker.record("players", 1, "put");
		});
		assert.strictEqual(
			changeTracker.size(),
			1,
			"capture wins over suppression",
		);

		releaseSuppressed();
		await suppressed;

		// And after everything settles, a new captured write still records.
		await changeTracker.runCaptured(async () => {
			changeTracker.record("players", 2, "put");
		});
		assert.strictEqual(changeTracker.size(), 2);
	});

	test("overlapping capture windows are counted, not toggled", async () => {
		let releaseA!: () => void;
		const a = changeTracker.runCaptured(
			() => new Promise<void>((resolve) => (releaseA = resolve)),
		);

		// B opens and closes while A is still in flight; A's window must survive.
		await changeTracker.runCaptured(async () => {});
		changeTracker.record("players", 1, "put");
		assert.strictEqual(changeTracker.size(), 1);

		releaseA();
		await a;

		// All windows closed: recording is off again.
		changeTracker.record("players", 2, "put");
		assert.strictEqual(changeTracker.size(), 1);
	});

	test("a sim window records fire-and-forget writes after the action resolved", () => {
		changeTracker.beginSim();
		changeTracker.record("players", 1, "put");
		changeTracker.endSim();
		assert.strictEqual(changeTracker.size(), 1);

		changeTracker.record("players", 2, "put");
		assert.strictEqual(
			changeTracker.size(),
			1,
			"closed sim window records nothing",
		);
	});
});
