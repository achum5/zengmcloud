import { assert, describe, test } from "vitest";
import {
	hasSeasonNote,
	parseSeasonNote,
	upsertSeasonNote,
} from "./seasonNote.ts";

const season = (
	note: string | undefined,
	yr: number,
	headline: string,
	body: string,
) => upsertSeasonNote(note, { season: yr, headline, body });

describe("upsertSeasonNote", () => {
	test("writes a headed section into an empty note", () => {
		assert.strictEqual(
			season(undefined, 2005, "The rookie who could", "A rookie year."),
			"[2005] The rookie who could\nA rookie year.",
		);
	});

	test("a headline is optional", () => {
		assert.strictEqual(
			season(undefined, 2005, "", "A rookie year."),
			"[2005]\nA rookie year.",
		);
	});

	test("newest season goes on top", () => {
		const note = season(
			season(undefined, 2005, "Rookie", "First."),
			2006,
			"Leap",
			"Second.",
		);
		assert.strictEqual(note, "[2006] Leap\nSecond.\n\n[2005] Rookie\nFirst.");
	});

	test("an older season backfills into place, not on top", () => {
		const note = season(
			season(undefined, 2006, "Leap", "Second."),
			2005,
			"Rookie",
			"First.",
		);
		assert.strictEqual(note, "[2006] Leap\nSecond.\n\n[2005] Rookie\nFirst.");
	});

	test("re-running a season replaces it and leaves the others", () => {
		let note = season(undefined, 2005, "A", "First.");
		note = season(note, 2006, "B", "Second.");
		note = season(note, 2007, "C", "Third.");
		note = season(note, 2006, "B2", "Second, rewritten.");
		assert.strictEqual(
			note,
			"[2007] C\nThird.\n\n[2006] B2\nSecond, rewritten.\n\n[2005] A\nFirst.",
		);
	});

	test("re-running never duplicates a year", () => {
		let note = season(undefined, 2005, "A", "One.");
		note = season(note, 2005, "B", "Two.");
		note = season(note, 2005, "C", "Three.");
		assert.strictEqual(note, "[2005] C\nThree.");
	});

	test("a retirement writeup sits above that same year's season recap", () => {
		// A player normally retires in a year he also played, so these must be
		// separate sections rather than one overwriting the other.
		let note = season(undefined, 2012, "Farewell tour", "His last year.");
		note = upsertSeasonNote(note, {
			season: 2012,
			kind: "retirement",
			headline: "The quiet exit",
			body: "After fourteen seasons...",
		});
		assert.strictEqual(
			note,
			"[2012] Retirement — The quiet exit\nAfter fourteen seasons...\n\n[2012] Farewell tour\nHis last year.",
		);
	});

	test("a retirement writeup can itself be re-run", () => {
		let note = upsertSeasonNote(undefined, {
			season: 2012,
			kind: "retirement",
			headline: "First take",
			body: "One.",
		});
		note = upsertSeasonNote(note, {
			season: 2012,
			kind: "retirement",
			headline: "Second take",
			body: "Two.",
		});
		assert.strictEqual(note, "[2012] Retirement — Second take\nTwo.");
	});

	test("hand-written text is preserved, below the headed sections", () => {
		const before = "My favorite player. Traded for him in a heist.";
		const after = season(before, 2005, "Rookie", "A rookie year.");
		assert.strictEqual(
			after,
			"[2005] Rookie\nA rookie year.\n\nMy favorite player. Traded for him in a heist.",
		);
		const after2 = season(after, 2006, "Leap", "Sophomore.");
		assert.ok(after2.includes("Traded for him in a heist."));
		assert.ok(after2.indexOf("[2006]") < after2.indexOf("[2005]"));
	});

	test("multi-paragraph bodies keep their paragraphs", () => {
		const body = "First paragraph.\n\nSecond paragraph.";
		const note = season(undefined, 2005, "Big year", body);
		const sections = parseSeasonNote(season(note, 2006, "Next", "Next."));
		assert.deepStrictEqual(
			sections.map((x) => x.season),
			[2006, 2005],
		);
		assert.strictEqual(sections[1]!.body, body);
		assert.strictEqual(sections[1]!.headline, "Big year");
	});

	test("a bracketed year inside prose is not treated as a header", () => {
		const note = season(
			undefined,
			2006,
			"Fading",
			"He never matched his [2005] peak.",
		);
		assert.deepStrictEqual(
			parseSeasonNote(note).map((x) => x.season),
			[2006],
		);
	});
});

describe("hasSeasonNote", () => {
	test("finds a season that has been written", () => {
		const note = season(
			season(undefined, 2005, "A", "First."),
			2006,
			"B",
			"Second.",
		);
		assert.strictEqual(hasSeasonNote(note, 2006), true);
		assert.strictEqual(hasSeasonNote(note, 2005), true);
		assert.strictEqual(hasSeasonNote(note, 2004), false);
	});

	test("a retirement writeup doesn't count as that year's season recap", () => {
		const note = upsertSeasonNote(undefined, {
			season: 2012,
			kind: "retirement",
			headline: "Exit",
			body: "Done.",
		});
		assert.strictEqual(hasSeasonNote(note, 2012), false);
		assert.strictEqual(hasSeasonNote(note, 2012, "retirement"), true);
	});

	test("an empty note has nothing", () => {
		assert.strictEqual(hasSeasonNote(undefined, 2005), false);
		assert.strictEqual(hasSeasonNote("", 2005), false);
	});
});
