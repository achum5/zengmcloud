import { assert, describe, test } from "vitest";
import {
	appendInjuryForensics,
	formatInjuryForensics,
	injuriesDiffer,
	suspiciousInjuryApply,
	type InjuryForensicsEntry,
} from "./injuryForensics.ts";

// The classifier is the part that must be exactly right: it decides which
// remote applies scream in the console and which are ordinary. Both field
// incidents shared one signature - a multi-game injury wiped toward healthy
// in a single live write - and ordinary sync traffic must never match it.

const injured = (gamesRemaining: number) => ({
	type: "Sprained Ankle",
	gamesRemaining,
});
const healthy = { type: "Healthy", gamesRemaining: 0 };

describe("suspiciousInjuryApply", () => {
	test("the field incidents match: a multi-game injury wiped in one live write", () => {
		// 4 -> 0 with the player suiting up, and the earlier 2 -> 0.
		assert.isTrue(suspiciousInjuryApply(injured(4), healthy, false));
		assert.isTrue(suspiciousInjuryApply(injured(3), injured(1), false));
	});

	test("one day of ordinary countdown arriving from the room is not suspicious", () => {
		assert.isFalse(suspiciousInjuryApply(injured(4), injured(3), false));
		assert.isFalse(suspiciousInjuryApply(injured(1), healthy, false));
	});

	test("a new injury arriving is not suspicious", () => {
		// The counter moves UP; only losing days is the signature.
		assert.isFalse(suspiciousInjuryApply(healthy, injured(12), false));
	});

	test("an ordered replay legitimately jumps multiple days", () => {
		// Catch-up walks several days of history; the last row seen locally may
		// be days newer than what was on disk. That is the replay doing its job.
		assert.isFalse(suspiciousInjuryApply(injured(4), healthy, true));
	});
});

describe("injuriesDiffer", () => {
	test("quiet for the common case of stats-only row churn", () => {
		assert.isFalse(injuriesDiffer(injured(5), injured(5)));
		assert.isFalse(injuriesDiffer(healthy, healthy));
		assert.isFalse(injuriesDiffer(undefined, undefined));
	});

	test("any change in days or type counts", () => {
		assert.isTrue(injuriesDiffer(injured(5), injured(4)));
		assert.isTrue(
			injuriesDiffer(injured(5), { type: "Sprained Knee", gamesRemaining: 5 }),
		);
		assert.isTrue(injuriesDiffer(undefined, injured(3)));
	});
});

describe("the ring", () => {
	const entry = (detail: string): InjuryForensicsEntry => ({
		at: 1,
		season: 2009,
		phase: 1,
		source: "apply",
		detail,
	});

	test("appends and caps, newest kept", () => {
		let ring: InjuryForensicsEntry[] | undefined;
		for (let i = 0; i < 10; i++) {
			ring = appendInjuryForensics(ring, entry(`e${i}`), 4);
		}
		assert.deepStrictEqual(
			ring!.map((e) => e.detail),
			["e6", "e7", "e8", "e9"],
		);
	});

	test("survives a corrupted prior value", () => {
		const ring = appendInjuryForensics("garbage" as any, entry("only"));
		assert.strictEqual(ring.length, 1);
	});
});

describe("formatInjuryForensics", () => {
	test("marks the suspicious entries so they jump out of a pasted report", () => {
		const text = formatInjuryForensics([
			{
				at: Date.parse("2026-08-18T12:00:00Z"),
				season: 2009,
				phase: 1,
				source: "apply",
				detail:
					"p2055 Rudy Fernandez 4(Sprained Ankle) > 0(Healthy) via=playMenu.day v=812",
				suspicious: true,
			},
		]);
		assert.include(text, "SUSPICIOUS apply:");
		assert.include(text, "v=812");
	});

	test("says so when empty", () => {
		assert.include(formatInjuryForensics(undefined), "none recorded");
		assert.include(formatInjuryForensics([]), "none recorded");
	});
});
