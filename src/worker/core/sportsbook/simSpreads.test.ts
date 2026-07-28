import { assert, beforeEach, describe, test } from "vitest";
import {
	__clearSimMargins,
	__setSimMargin,
	blendMargin,
	peekSimMargin,
	rosterFingerprint,
	simMarginKey,
	SIMS_PER_GAME,
	type SimMargin,
} from "./simSpreads.ts";

const sim = (mean: number, se: number): SimMargin => ({
	mean,
	se,
	n: SIMS_PER_GAME,
});

describe("blendMargin", () => {
	// The point of blending rather than replacing: the formula's error is a bias
	// and the sim's is noise, so the precision-weighted combination beats both.
	// A loose sample barely moves the line; a tight one all but replaces it.
	test("a noisy sample barely moves the formula", () => {
		const out = blendMargin(6, sim(0, 12), 2.5);
		assert.ok(out > 5, `expected to stay near 6, got ${out}`);
		assert.ok(out < 6);
	});

	test("a tight sample all but replaces it", () => {
		const out = blendMargin(6, sim(0, 0.1), 2.5);
		assert.ok(Math.abs(out) < 0.2, `expected to land near 0, got ${out}`);
	});

	test("the blend always lands between the two", () => {
		for (const se of [0.5, 1.75, 4, 10]) {
			const out = blendMargin(6, sim(0, se), 2.5);
			assert.ok(out >= 0 && out <= 6, `${out} outside [0, 6] at se=${se}`);
		}
	});

	test("more sim evidence means more movement", () => {
		const loose = blendMargin(6, sim(0, 4), 2.5);
		const tight = blendMargin(6, sim(0, 1), 2.5);
		assert.ok(tight < loose, `${tight} should be further from 6 than ${loose}`);
	});

	// Trusting the formula less moves the line further, and vice versa. This is
	// the single knob, so it had better point the right way.
	test("a less trusted formula gives the sim more weight", () => {
		const trusting = blendMargin(6, sim(0, 1.75), 1);
		const doubtful = blendMargin(6, sim(0, 1.75), 5);
		assert.ok(doubtful < trusting);
	});

	// A freak sample must not produce an absurd line.
	test("a wild sample is capped at 6 points of movement", () => {
		assert.strictEqual(blendMargin(6, sim(-100, 0.01), 2.5), 0);
		assert.strictEqual(blendMargin(6, sim(100, 0.01), 2.5), 12);
	});

	test("a zero-variance sample is taken at face value", () => {
		assert.strictEqual(blendMargin(6, sim(4, 0), 2.5), 4);
	});

	test("it is symmetric about the sign of the favorite", () => {
		const home = blendMargin(6, sim(2, 1.75), 2.5);
		const away = blendMargin(-6, sim(-2, 1.75), 2.5);
		assert.ok(Math.abs(home + away) < 1e-9);
	});
});

describe("cache keys", () => {
	const base = {
		settings: "a,b,c",
		homeRoster: "1,50,0",
		awayRoster: "2,40,0",
		neutralSite: false,
		daysInFuture: 0,
	};

	test("the same state is the same key", () => {
		assert.strictEqual(simMarginKey(base), simMarginKey({ ...base }));
	});

	// Each of these changes the game the engine would play, so each has to
	// invalidate the cached line rather than quietly serve a stale one.
	test("anything the engine reads changes it", () => {
		const keys = new Set([
			simMarginKey(base),
			simMarginKey({ ...base, settings: "a,b,d" }),
			simMarginKey({ ...base, homeRoster: "1,51,0" }),
			simMarginKey({ ...base, awayRoster: "2,40,1" }),
			simMarginKey({ ...base, neutralSite: true }),
			simMarginKey({ ...base, daysInFuture: 3 }),
		]);
		assert.strictEqual(keys.size, 6);
	});

	// Home and away are not the same game.
	test("swapping the sides is a different key", () => {
		assert.notStrictEqual(
			simMarginKey(base),
			simMarginKey({
				...base,
				homeRoster: base.awayRoster,
				awayRoster: base.homeRoster,
			}),
		);
	});
});

describe("rosterFingerprint", () => {
	const roster = [
		{ pid: 1, value: 60.4, injury: { gamesRemaining: 0 } },
		{ pid: 2, value: 55.1, injury: { gamesRemaining: 0 } },
	];

	test("an unchanged roster is unchanged", () => {
		assert.strictEqual(
			rosterFingerprint(roster),
			rosterFingerprint([...roster]),
		);
	});

	test("an injury changes it", () => {
		assert.notStrictEqual(
			rosterFingerprint(roster),
			rosterFingerprint([
				roster[0]!,
				{ ...roster[1]!, injury: { gamesRemaining: 4 } },
			]),
		);
	});

	test("a player developing changes it", () => {
		assert.notStrictEqual(
			rosterFingerprint(roster),
			rosterFingerprint([{ ...roster[0]!, value: 61.9 }, roster[1]!]),
		);
	});

	test("a signing changes it", () => {
		assert.notStrictEqual(
			rosterFingerprint(roster),
			rosterFingerprint([...roster, { pid: 3, value: 45 }]),
		);
	});

	test("missing fields don't throw", () => {
		assert.strictEqual(typeof rosterFingerprint([{ pid: 9 }]), "string");
	});
});

describe("peekSimMargin", () => {
	beforeEach(() => {
		__clearSimMargins();
	});

	test("an unknown game reads as nothing, so pricing falls back", () => {
		assert.strictEqual(peekSimMargin("nope"), undefined);
	});

	test("a simulated game reads back exactly", () => {
		__setSimMargin("k", sim(4.2, 1.75));
		assert.deepStrictEqual(peekSimMargin("k"), sim(4.2, 1.75));
	});

	// A game the engine couldn't load is remembered as a failure, so the board
	// doesn't queue it again on every render.
	test("a remembered failure reads as nothing but is not re-queued", () => {
		__setSimMargin("k", null);
		assert.strictEqual(peekSimMargin("k"), undefined);
	});
});
