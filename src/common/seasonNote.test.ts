import { assert, describe, test } from "vitest";
import {
	hasSeasonNote,
	parseSeasonNote,
	upsertSeasonNote,
} from "./seasonNote.ts";

describe("upsertSeasonNote", () => {
	test("writes the first season into an empty note", () => {
		assert.strictEqual(
			upsertSeasonNote(undefined, 2005, "A rookie year."),
			"[2005]\nA rookie year.",
		);
		assert.strictEqual(
			upsertSeasonNote("", 2005, "A rookie year."),
			"[2005]\nA rookie year.",
		);
	});

	test("a newer season goes on top", () => {
		const note = upsertSeasonNote("[2005]\nRookie year.", 2006, "Sophomore.");
		assert.strictEqual(note, "[2006]\nSophomore.\n\n[2005]\nRookie year.");
	});

	test("an older season slots into place, not on top", () => {
		// Recaps can be run out of order (e.g. backfilling an old season).
		const note = upsertSeasonNote("[2006]\nSophomore.", 2005, "Rookie year.");
		assert.strictEqual(note, "[2006]\nSophomore.\n\n[2005]\nRookie year.");
	});

	test("re-running a season replaces that year and leaves the others", () => {
		const before = "[2007]\nThird.\n\n[2006]\nSecond.\n\n[2005]\nFirst.";
		const after = upsertSeasonNote(before, 2006, "Second, rewritten.");
		assert.strictEqual(
			after,
			"[2007]\nThird.\n\n[2006]\nSecond, rewritten.\n\n[2005]\nFirst.",
		);
	});

	test("re-running does not duplicate the year", () => {
		let note = upsertSeasonNote(undefined, 2005, "One.");
		note = upsertSeasonNote(note, 2005, "Two.");
		note = upsertSeasonNote(note, 2005, "Three.");
		assert.strictEqual(note, "[2005]\nThree.");
	});

	test("hand-written text is preserved, below the year sections", () => {
		// Someone's own note about a player must never be destroyed by a recap.
		const before = "My favorite player. Traded for him in a heist.";
		const after = upsertSeasonNote(before, 2005, "A rookie year.");
		assert.strictEqual(
			after,
			"[2005]\nA rookie year.\n\nMy favorite player. Traded for him in a heist.",
		);
		// And it survives a second season being written.
		const after2 = upsertSeasonNote(after, 2006, "Sophomore.");
		assert.ok(after2.includes("Traded for him in a heist."));
		assert.ok(after2.indexOf("[2006]") < after2.indexOf("[2005]"));
	});

	test("multi-paragraph recaps keep their paragraphs", () => {
		const recap = "First paragraph.\n\nSecond paragraph.";
		const note = upsertSeasonNote(undefined, 2005, recap);
		assert.strictEqual(note, "[2005]\nFirst paragraph.\n\nSecond paragraph.");
		// And the year sections still parse back apart correctly.
		const sections = parseSeasonNote(upsertSeasonNote(note, 2006, "Next."));
		assert.deepStrictEqual(
			sections.map((s) => s.season),
			[2006, 2005],
		);
		assert.strictEqual(sections[1]!.body, recap);
	});

	test("surrounding whitespace is normalized away", () => {
		assert.strictEqual(
			upsertSeasonNote(undefined, 2005, "\n\n  Padded.  \n\n"),
			"[2005]\nPadded.",
		);
	});

	test("a bracketed year inside prose is not treated as a header", () => {
		// Only a line that is EXACTLY a year header splits a section, so a recap
		// mentioning "[2005]" mid-sentence can't corrupt the structure.
		const note = upsertSeasonNote(
			undefined,
			2006,
			"He never matched his [2005] peak.",
		);
		assert.strictEqual(note, "[2006]\nHe never matched his [2005] peak.");
		assert.deepStrictEqual(
			parseSeasonNote(note).map((s) => s.season),
			[2006],
		);
	});
});

describe("hasSeasonNote", () => {
	test("finds a season that has been written", () => {
		const note = "[2006]\nSecond.\n\n[2005]\nFirst.";
		assert.strictEqual(hasSeasonNote(note, 2006), true);
		assert.strictEqual(hasSeasonNote(note, 2005), true);
		assert.strictEqual(hasSeasonNote(note, 2004), false);
	});

	test("an empty note has nothing", () => {
		assert.strictEqual(hasSeasonNote(undefined, 2005), false);
		assert.strictEqual(hasSeasonNote("", 2005), false);
	});

	test("a header with no body doesn't count as written", () => {
		assert.strictEqual(hasSeasonNote("[2005]\n", 2005), false);
	});
});
