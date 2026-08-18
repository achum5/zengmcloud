import { assert, describe, test } from "vitest";
import { dayAlreadyCounted } from "./dailyCountdownGate.ts";

// The double-countdown incident: an injured player's games-remaining stamp
// went 2 -> 0 across consecutive game days, and he suited up a game early.
// One schedule day was counted twice. The gate makes that impossible by
// data: the same (season, phase, day) is never counted again.

const day = (season: number, phase: number, dayNum: number) => ({
	season,
	phase,
	day: dayNum,
});

describe("dayAlreadyCounted", () => {
	test("the same day is counted once", () => {
		assert.isTrue(dayAlreadyCounted(day(2009, 1, 19), day(2009, 1, 19)));
	});

	test("the next day counts", () => {
		assert.isFalse(dayAlreadyCounted(day(2009, 1, 19), day(2009, 1, 20)));
	});

	test("a new phase restarts the calendar", () => {
		// Playoff day numbers may or may not continue the regular season's, so
		// the phase is part of the identity either way.
		assert.isFalse(dayAlreadyCounted(day(2009, 1, 19), day(2009, 3, 19)));
	});

	test("a new season restarts the calendar", () => {
		assert.isFalse(dayAlreadyCounted(day(2009, 1, 19), day(2010, 1, 19)));
	});

	test("no stamp yet means count", () => {
		// Every existing league starts here.
		assert.isFalse(dayAlreadyCounted(undefined, day(2009, 1, 19)));
	});

	test("no day identity means count, and the caller must not stamp", () => {
		// If the simmed day's number is somehow unknowable, behave exactly like
		// the code before the gate existed rather than skipping a countdown.
		assert.isFalse(dayAlreadyCounted(day(2009, 1, 19), undefined));
	});
});
