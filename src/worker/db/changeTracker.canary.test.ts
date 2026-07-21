import { afterEach, assert, beforeEach, describe, test, vi } from "vitest";
import { changeTracker } from "./changeTracker.ts";

// The invisible-write canary: while a synced session is connected, any write
// that lands outside every capture/sim/apply window will never reach the other
// devices - the device is silently forking. It must be reported loudly (once
// per store), but never for writes that are uncaptured on purpose.
describe("changeTracker invisible-write canary", () => {
	let errorSpy: ReturnType<typeof vi.spyOn>;

	beforeEach(() => {
		changeTracker.enable();
		changeTracker.reset();
		changeTracker.setCanary(false);
		changeTracker.setCanary(true);
		errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
	});

	afterEach(() => {
		changeTracker.setCanary(false);
		changeTracker.disable();
		errorSpy.mockRestore();
	});

	test("reports an uncaptured write to a synced store, once per store", () => {
		changeTracker.record("teamSeasons", 5, "put");
		changeTracker.record("teamSeasons", 6, "put");
		changeTracker.record("players", 1, "put");
		assert.strictEqual(errorSpy.mock.calls.length, 2);
		assert.ok(String(errorSpy.mock.calls[0]![0]).includes("INVISIBLE WRITE"));
	});

	test("silent for writes inside a capture window", async () => {
		await changeTracker.runCaptured(async () => {
			changeTracker.record("teamSeasons", 5, "put");
		});
		assert.strictEqual(errorSpy.mock.calls.length, 0);
	});

	test("silent for writes inside a sim window", () => {
		changeTracker.beginSim();
		changeTracker.record("teamSeasons", 5, "put");
		changeTracker.endSim();
		assert.strictEqual(errorSpy.mock.calls.length, 0);
	});

	test("silent while applying a remote changeset", () => {
		changeTracker.beginApply();
		changeTracker.record("teamSeasons", 5, "put");
		changeTracker.endApply();
		assert.strictEqual(errorSpy.mock.calls.length, 0);
	});

	test("silent for per-device stores and per-device gameAttributes", () => {
		changeTracker.record("trade", 0, "put");
		changeTracker.record("savedTrades", "abc", "put");
		changeTracker.record("savedTradingBlock", 0, "delete");
		changeTracker.record("messages", 3, "put");
		changeTracker.record("gameAttributes", "userTid", "put");
		changeTracker.record("gameAttributes", "tradeProposalsSeed", "put");
		assert.strictEqual(errorSpy.mock.calls.length, 0);
		// A shared gameAttribute IS reported.
		changeTracker.record("gameAttributes", "salaryCap", "put");
		assert.strictEqual(errorSpy.mock.calls.length, 1);
	});

	test("silent when disarmed (dev logging without a synced session)", () => {
		changeTracker.setCanary(false);
		changeTracker.record("teamSeasons", 5, "put");
		assert.strictEqual(errorSpy.mock.calls.length, 0);
	});
});
