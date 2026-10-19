import { assert, describe, test } from "vitest";
import {
	autoPlayCountdownVisible,
	OVERDUE_GRACE_MS,
} from "./autoPlayCountdownVisible.ts";

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
	const base = { enabled: true, nextRunAt: 1000, gated: false, now: 0 };

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

	// A follower watching a live playoff game holds the snapshot from tip-off;
	// the sim it counted down to never comes (auto play stops when the regular
	// season ends), and the header sat on "0:00" for the rest of the game.
	test("at zero while the sim runs, it stays", () => {
		assert.isTrue(autoPlayCountdownVisible({ ...base, now: 1000 }));
		assert.isTrue(
			autoPlayCountdownVisible({ ...base, now: 1000 + OVERDUE_GRACE_MS - 1 }),
		);
	});

	test("long past zero, it goes", () => {
		assert.isFalse(
			autoPlayCountdownVisible({ ...base, now: 1000 + OVERDUE_GRACE_MS }),
		);
		// A held (frozen) snapshot hides the same way whether the schedule
		// re-armed or stopped, so going says nothing about the game.
		assert.isFalse(
			autoPlayCountdownVisible({ ...base, now: 1000 + 10 * OVERDUE_GRACE_MS }),
		);
	});
});
