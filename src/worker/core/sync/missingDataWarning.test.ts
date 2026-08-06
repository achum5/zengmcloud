import { assert, describe, test } from "vitest";
import {
	decideMissingDataWarning,
	MISSING_DATA_WARN_AFTER_MS,
} from "./missingDataWarning.ts";

const NOW = 1_700_000_000_000;

describe("decideMissingDataWarning", () => {
	test("the first sighting starts the clock and stays quiet", () => {
		const decision = decideMissingDataWarning({
			since: undefined,
			alreadyWarned: false,
			now: NOW,
		});
		assert.strictEqual(decision.warn, false);
		assert.strictEqual(decision.since, NOW);
	});

	test("stays quiet through the grace period, keeping the original stamp", () => {
		// This is the case that made the error pop up on every launch: the device
		// reconnects repeatedly while a perfectly ordinary gap is waiting on the
		// simmer to open the app.
		for (const elapsed of [0, 1000, 60_000, MISSING_DATA_WARN_AFTER_MS - 1]) {
			const decision = decideMissingDataWarning({
				since: NOW,
				alreadyWarned: false,
				now: NOW + elapsed,
			});
			assert.strictEqual(decision.warn, false, `elapsed ${elapsed}`);
			// The stamp must not be pushed forward, or reconnecting often enough
			// would hold the warning off forever.
			assert.strictEqual(decision.since, NOW, `elapsed ${elapsed}`);
		}
	});

	test("warns once the gap outlasts the grace period", () => {
		const decision = decideMissingDataWarning({
			since: NOW,
			alreadyWarned: false,
			now: NOW + MISSING_DATA_WARN_AFTER_MS,
		});
		assert.strictEqual(decision.warn, true);
		assert.strictEqual(decision.since, NOW);
	});

	test("does not warn twice in one session", () => {
		const decision = decideMissingDataWarning({
			since: NOW,
			alreadyWarned: true,
			now: NOW + 10 * MISSING_DATA_WARN_AFTER_MS,
		});
		assert.strictEqual(decision.warn, false);
	});

	test("a stamp from the future restarts the clock instead of muting forever", () => {
		const decision = decideMissingDataWarning({
			since: NOW + 10 * MISSING_DATA_WARN_AFTER_MS,
			alreadyWarned: false,
			now: NOW,
		});
		assert.strictEqual(decision.warn, false);
		assert.strictEqual(decision.since, NOW);
		// And from there it behaves normally.
		assert.strictEqual(
			decideMissingDataWarning({
				since: decision.since,
				alreadyWarned: false,
				now: NOW + MISSING_DATA_WARN_AFTER_MS,
			}).warn,
			true,
		);
	});

	test("a gap that heals and returns waits out the grace period again", () => {
		// Healing clears the stamp (saveResyncNeeded drops syncMissingDataSince),
		// so the next gap is a first sighting - not an instant error.
		const first = decideMissingDataWarning({
			since: undefined,
			alreadyWarned: false,
			now: NOW,
		});
		assert.strictEqual(first.warn, false, "first sighting is quiet");

		const healed = undefined;
		const afterHeal = decideMissingDataWarning({
			since: healed,
			alreadyWarned: false,
			now: NOW + 10 * MISSING_DATA_WARN_AFTER_MS,
		});
		assert.strictEqual(afterHeal.warn, false);
		assert.strictEqual(afterHeal.since, NOW + 10 * MISSING_DATA_WARN_AFTER_MS);
	});
});
