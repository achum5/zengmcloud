import { assert, afterEach, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { setSyncEngine } from "../sync/engineHolder.ts";
import genOrder from "./genOrder.ts";

// A real lottery run in a synced league must only ever happen inside a
// cloud-tracked capture window - otherwise its writes (lottery result, pick
// order, COLA penalties) exist only on this device and the room forks. This
// guards the guard.
describe("genOrder sync guard", () => {
	beforeEach(async () => {
		resetG();
		await resetCache({});
		changeTracker.reset();
	});

	afterEach(() => {
		setSyncEngine(undefined);
		changeTracker.disable();
	});

	const GUARD_MESSAGE = "Refusing to run the draft lottery";

	const runAndGetError = async (mock: boolean) => {
		try {
			await genOrder(mock);
			return undefined;
		} catch (error) {
			return error instanceof Error ? error.message : String(error);
		}
	};

	test("a real run in a synced league OUTSIDE a capture window is refused", async () => {
		// THE PHANTOM LOTTERY THIS PREVENTS: a view load (suppressed, uncaptured)
		// triggered genOrder(false), holding a private, unsynced lottery on one
		// device - repeatedly, compounding COLA penalties each time.
		setSyncEngine({} as any);
		changeTracker.enable();

		const message = await runAndGetError(false);
		assert.ok(message, "expected genOrder to throw");
		assert.ok(message.includes(GUARD_MESSAGE), message);
	});

	test("a real run in a synced league INSIDE a capture window passes the guard", async () => {
		setSyncEngine({} as any);
		changeTracker.enable();

		await changeTracker.runCaptured(async () => {
			const message = await runAndGetError(false);
			// It may fail later for unrelated reasons (test cache is empty), but it
			// must get PAST the guard.
			if (message !== undefined) {
				assert.ok(!message.includes(GUARD_MESSAGE), message);
			}
		});
	});

	test("a mock projection in a synced league is never refused", async () => {
		setSyncEngine({} as any);
		changeTracker.enable();

		const message = await runAndGetError(true);
		if (message !== undefined) {
			assert.ok(!message.includes(GUARD_MESSAGE), message);
		}
	});

	test("single-player (no sync engine) is never refused", async () => {
		const message = await runAndGetError(false);
		if (message !== undefined) {
			assert.ok(!message.includes(GUARD_MESSAGE), message);
		}
	});
});
