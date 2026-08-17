import { assert, describe, test } from "vitest";
import {
	ARRIVED_MS,
	deferralRetryDelay,
	deferralSuperseded,
	type DeferredFire,
} from "./autoPlayDeferral.ts";
import { nextFireForRule, newRule } from "./scheduleTime.ts";

const held = (supersededAt: number | undefined): DeferredFire<string> => ({
	fire: "day",
	supersededAt,
});

const RETRY_MS = 15_000;

describe("deferralSuperseded", () => {
	test("a held fire keeps the timer until its handover moment", () => {
		const deferred = held(100_000);
		assert.isFalse(deferralSuperseded(deferred, 0));
		assert.isFalse(deferralSuperseded(deferred, 99_000));
	});

	test("hands over one tolerance EARLY, not late", () => {
		// Late is the whole bug: nextFireForRule only ever returns a fire strictly
		// after now, so arriving at or past the slot skips it too.
		const deferred = held(100_000);
		assert.isFalse(deferralSuperseded(deferred, 100_000 - ARRIVED_MS - 1));
		assert.isTrue(deferralSuperseded(deferred, 100_000 - ARRIVED_MS));
		assert.isTrue(deferralSuperseded(deferred, 100_000));
		assert.isTrue(deferralSuperseded(deferred, 200_000));
	});

	test("nothing else scheduled - the held fire keeps its claim", () => {
		assert.isFalse(deferralSuperseded(held(undefined), 0));
		assert.isFalse(deferralSuperseded(held(undefined), 10 ** 12));
	});
});

describe("deferralRetryDelay", () => {
	test("retries on its own cadence while the handover is far off", () => {
		assert.strictEqual(
			deferralRetryDelay(held(100_000), 0, RETRY_MS),
			RETRY_MS,
		);
	});

	test("never overshoots the handover moment", () => {
		// 5s short of the handover, so the retry lands ON it rather than past it.
		const now = 100_000 - ARRIVED_MS - 5_000;
		assert.strictEqual(deferralRetryDelay(held(100_000), now, RETRY_MS), 5_000);
	});

	test("always positive right up to the handover", () => {
		// deferralSuperseded gates this, so the last legal `now` is one ms before.
		const now = 100_000 - ARRIVED_MS - 1;
		assert.isFalse(deferralSuperseded(held(100_000), now));
		assert.isAbove(deferralRetryDelay(held(100_000), now, RETRY_MS), 0);
	});

	test("nothing else scheduled - plain cadence", () => {
		assert.strictEqual(
			deferralRetryDelay(held(undefined), 10 ** 12, RETRY_MS),
			RETRY_MS,
		);
	});
});

describe("the regression: a fixed-time fire blocked by a live sim", () => {
	// "Sim a day at 20:00 every day". The room is watching a league-mate's live
	// sim at 20:00, so the fire cannot run at its moment. Before this, armTimer
	// re-armed to the next occurrence - TOMORROW at 20:00 - and the league simply
	// never simmed that night.
	const rule = newRule();
	rule.mode = "at";
	rule.times = ["20:00"];
	rule.days = [];

	const at2000 = new Date(2026, 0, 5, 20, 0, 0, 0).getTime();
	const supersededAt = nextFireForRule(rule, new Date(at2000))!;

	test("the next scheduled fire really is a day away", () => {
		assert.strictEqual(
			supersededAt,
			new Date(2026, 0, 6, 20, 0, 0, 0).getTime(),
			"nextFireForRule wants a fire strictly after now, hence tomorrow",
		);
	});

	test("stays held all through a long live sim, so it can still run", () => {
		const deferred = held(supersededAt);
		for (const minutes of [0, 1, 5, 20, 90]) {
			const now = at2000 + minutes * 60_000;
			assert.isFalse(
				deferralSuperseded(deferred, now),
				`${minutes} minutes past the fire`,
			);
			assert.strictEqual(
				deferralRetryDelay(deferred, now, RETRY_MS),
				RETRY_MS,
				`${minutes} minutes past the fire`,
			);
		}
	});

	test("tomorrow's fire takes over rather than queueing behind it", () => {
		// Whatever happened overnight, the room sims the day it is due - not two
		// days back to back catching up.
		assert.isTrue(deferralSuperseded(held(supersededAt), supersededAt));
	});
});
