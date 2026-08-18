import { assert, describe, test } from "vitest";
import { playerInScope, replayStartAge } from "./applyFaceAgingToLeague.ts";

describe("playerInScope", () => {
	const fictional = {};
	const realByFlag = { real: true };
	const realBySrID = { srID: "jamesle01" };

	test("all means all", () => {
		for (const p of [fictional, realByFlag, realBySrID]) {
			assert.isTrue(playerInScope(p, "all"));
		}
	});

	test("real players are known by either marker", () => {
		// Imported rosters set one or the other depending on the source.
		assert.isTrue(playerInScope(realByFlag, "real"));
		assert.isTrue(playerInScope(realBySrID, "real"));
		assert.isFalse(playerInScope(fictional, "real"));
	});

	test("fictional is the exact complement", () => {
		assert.isTrue(playerInScope(fictional, "fictional"));
		assert.isFalse(playerInScope(realByFlag, "fictional"));
		assert.isFalse(playerInScope(realBySrID, "fictional"));
	});
});

describe("replayStartAge", () => {
	test("a normal player starts at the age he was drafted", () => {
		assert.strictEqual(
			replayStartAge({ draftYear: 2018, bornYear: 1999, currentAge: 27 }),
			19,
		);
	});

	test("never starts later than the player is now", () => {
		// A rookie's replay is a single frame, not a career run backwards.
		assert.strictEqual(
			replayStartAge({ draftYear: 2026, bornYear: 2007, currentAge: 19 }),
			19,
		);
		assert.strictEqual(
			replayStartAge({ draftYear: 2018, bornYear: 1999, currentAge: 17 }),
			17,
		);
	});

	test("a missing or nonsense draft year falls back to a normal draft age", () => {
		// Imported rosters and God Mode creations routinely have neither.
		for (const draftYear of [undefined, 0, -1]) {
			assert.strictEqual(
				replayStartAge({ draftYear, bornYear: 1999, currentAge: 27 }),
				19,
			);
		}
		// Born after his own draft, or drafted at 60: not usable.
		assert.strictEqual(
			replayStartAge({ draftYear: 2000, bornYear: 1999, currentAge: 27 }),
			19,
		);
		assert.strictEqual(
			replayStartAge({ draftYear: 2060, bornYear: 1999, currentAge: 27 }),
			19,
		);
	});
});
