import { assert, describe, test } from "vitest";
import { autoPlayCountdownVisible } from "./autoPlayCountdownVisible.ts";

// THE CLOCK MUST NOT PROMISE A SIM THAT CANNOT RUN.
//
// A configured sim stop - a day stop, the trade deadline - becomes a ready-up
// gate in a shared league, and the scheduler refuses to fire across one: it
// PAUSES, stays enabled, and keeps re-arming while it waits for every team (see
// the simStop branch in autoPlayScheduler.ts). Both of those are true at once,
// which is what made the header lie - `enabled` and `nextRunAt` were still set,
// so an 18-minute countdown ticked down beside the "Ready 0/3" that was the
// only thing actually holding the league up.
describe("autoPlayCountdownVisible", () => {
	const base = { enabled: true, nextRunAt: 1000, gated: false };

	test("a live schedule shows its clock", () => {
		assert.isTrue(autoPlayCountdownVisible(base));
	});

	// The scheduler is deliberately left ENABLED and ARMED while gated - that is
	// what makes it carry on by itself once the room readies up - so neither of
	// those can be the thing that hides the clock.
	test("a pending ready-up hides it, enabled and armed though it is", () => {
		assert.isTrue(base.enabled);
		assert.isNumber(base.nextRunAt);
		assert.isFalse(autoPlayCountdownVisible({ ...base, gated: true }));
	});

	test("and it comes back the moment the gate clears", () => {
		assert.isTrue(autoPlayCountdownVisible({ ...base, gated: false }));
	});

	test("off, or armed at nothing, still hides it", () => {
		assert.isFalse(autoPlayCountdownVisible({ ...base, enabled: false }));
		assert.isFalse(autoPlayCountdownVisible({ ...base, nextRunAt: undefined }));
	});
});
