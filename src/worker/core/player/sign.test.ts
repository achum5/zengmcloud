import { assert, beforeEach, describe, test } from "vitest";
import { resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import { HIGH_UPSIDE_POT, highUpsideSigningPot } from "./sign.ts";

// The free-agency notification quotes a potential, so it has to answer to the
// same rules every other user-facing rating does.
describe("announcing a high-upside signing", () => {
	beforeEach(() => {
		resetG();
		// fuzzRating drops fuzz entirely in multi-team and god mode, so the
		// cases below need a plain single-team league to say anything about it.
		g.setWithoutSavingToDB("userTids", [0]);
		g.setWithoutSavingToDB("godMode", false);
	});

	test("a promising free agent is worth a word", () => {
		assert.strictEqual(
			highUpsideSigningPot({ pot: HIGH_UPSIDE_POT, fuzz: 0 }),
			HIGH_UPSIDE_POT,
		);
	});

	test("an ordinary one is not", () => {
		assert.strictEqual(
			highUpsideSigningPot({ pot: HIGH_UPSIDE_POT - 1, fuzz: 0 }),
			undefined,
		);
		assert.strictEqual(highUpsideSigningPot(undefined), undefined);
	});

	// The bug: it read the true rating, so the notification was a better scout
	// than the scouting department.
	test("the potential quoted is the scouted one, not the true one", () => {
		assert.strictEqual(highUpsideSigningPot({ pot: 63, fuzz: 4 }), 67);
		// Down as well as up - as long as it still clears the bar, which is
		// what the next test is about.
		assert.strictEqual(highUpsideSigningPot({ pot: 63, fuzz: -2 }), 61);
	});

	test("and the threshold reads that same number, so the two can never disagree", () => {
		// True potential clears the bar; the scouted one does not, so there is
		// nothing to announce - rather than announcing a number below the bar.
		assert.strictEqual(highUpsideSigningPot({ pot: 61, fuzz: -3 }), undefined);
		// ...and the mirror image: scouting is high on a player who is not
		// really there, and the number shown is the one that qualified him.
		assert.strictEqual(highUpsideSigningPot({ pot: 58, fuzz: 4 }), 62);
	});

	// Suppressed outright, not merely stripped of its number: the notification
	// only fires for high potential, so its arrival is itself the rating.
	test("nothing is said when ratings are hidden", () => {
		g.setWithoutSavingToDB("challengeNoRatings", true);
		assert.strictEqual(highUpsideSigningPot({ pot: 75, fuzz: 0 }), undefined);
		assert.strictEqual(highUpsideSigningPot({ pot: 99, fuzz: 0 }), undefined);
	});

	// THE REPORT: a league reading its roster in tens signed a free agent and
	// the toast quoted his exact potential. Every other user-facing rating in
	// that mode shows its tens digit, and this one was the hole.
	test("the quoted potential is coarsened when the league hides the ones digit", () => {
		g.setWithoutSavingToDB("hideRatingsOnesDigit", true);
		assert.strictEqual(highUpsideSigningPot({ pot: 67, fuzz: 0 }), 6);
		assert.strictEqual(highUpsideSigningPot({ pot: 90, fuzz: 0 }), 9);
		// Coarsened AFTER the fuzz, so the band shown is the one that qualified
		// him rather than the band his true rating sits in.
		assert.strictEqual(highUpsideSigningPot({ pot: 58, fuzz: 4 }), 6);
	});

	test("who gets announced does not change with the display mode", () => {
		// The threshold keeps reading the full-resolution scouted number, so
		// coarsening cannot quietly widen or narrow who is worth a word.
		for (const pot of [58, 59, 60, 61, 75, 99]) {
			g.setWithoutSavingToDB("hideRatingsOnesDigit", false);
			const exact = highUpsideSigningPot({ pot, fuzz: 0 });
			g.setWithoutSavingToDB("hideRatingsOnesDigit", true);
			const coarse = highUpsideSigningPot({ pot, fuzz: 0 });
			assert.strictEqual(
				exact === undefined,
				coarse === undefined,
				`pot ${pot} announced differently`,
			);
		}
	});
});
