import { assert, describe, test } from "vitest";
import {
	hasSeasonNote,
	parseSeasonNote,
	removeSeasonNote,
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

describe("a retirement writeup with no headline", () => {
	// This round-tripped as a SEASON section literally named "Retirement",
	// because the marker was only recognized with a dash after it. Re-running
	// then couldn't find its own section and appended a second block, so the
	// player's note ended up with the same year twice.
	const write = (note: string | undefined, headline: string) =>
		upsertSeasonNote(note, {
			season: 2012,
			kind: "retirement",
			headline,
			body: "After fourteen seasons...",
		});

	test("is parsed back as a retirement, not as a season named Retirement", () => {
		const sections = parseSeasonNote(write(undefined, ""));
		assert.strictEqual(sections.length, 1);
		assert.strictEqual(sections[0]!.kind, "retirement");
		assert.strictEqual(sections[0]!.headline, "");
	});

	test("re-running it replaces rather than duplicating the year", () => {
		let note = write(undefined, "");
		note = write(note, "");
		note = write(note, "");
		assert.strictEqual(note.split("[2012]").length - 1, 1);
	});

	test("adding a headline later still replaces the same section", () => {
		const note = write(write(undefined, ""), "The quiet exit");
		assert.strictEqual(note.split("[2012]").length - 1, 1);
		assert.ok(note.includes("[2012] Retirement — The quiet exit"));
	});

	test("it still doesn't collide with that year's season recap", () => {
		let note = season(undefined, 2012, "", "His last year.");
		note = write(note, "");
		const sections = parseSeasonNote(note);
		assert.deepStrictEqual(
			sections.map((x) => x.kind),
			["retirement", "season"],
		);
	});

	test("hasSeasonNote still tells the two apart", () => {
		const note = write(undefined, "");
		assert.strictEqual(hasSeasonNote(note, 2012, "retirement"), true);
		assert.strictEqual(hasSeasonNote(note, 2012), false);
	});

	test("a season headline that merely starts with Retirement is left alone", () => {
		// "Retirement" only marks the section when it stands alone or is followed
		// by a dash, so this stays an ordinary season recap.
		const note = season(undefined, 2012, "Retirement day", "He went out.");
		const sections = parseSeasonNote(note);
		assert.strictEqual(sections[0]!.kind, "season");
		assert.strictEqual(sections[0]!.headline, "Retirement day");
	});
});

describe("removeSeasonNote", () => {
	test("clears a misfiled retirement writeup and leaves the rest", () => {
		// A season-recap reply pasted into the retirement button filed a whole
		// batch of players as having retired. Re-running the season replaces only
		// the SEASON section, so the bogus one has to be removed explicitly.
		let note = season(undefined, 2000, "A good year", "He played well.");
		note = upsertSeasonNote(note, {
			season: 2000,
			kind: "retirement",
			headline: "Wrongly retired",
			body: "This should not be here.",
		});
		note = season(note, 1999, "The year before", "Earlier.");

		const cleaned = removeSeasonNote(note, 2000, "retirement");
		assert.ok(!cleaned.includes("Retirement"));
		assert.ok(cleaned.includes("[2000] A good year"));
		assert.ok(cleaned.includes("[1999] The year before"));
	});

	test("removing something that isn't there changes nothing", () => {
		const note = season(undefined, 2000, "A good year", "He played well.");
		assert.strictEqual(removeSeasonNote(note, 2000, "retirement"), note);
		assert.strictEqual(removeSeasonNote(undefined, 2000, "retirement"), "");
	});

	test("hand-written text survives the removal", () => {
		// Freeform text sits at the bottom, below the OLDEST season, so removing a
		// retirement writeup (which sorts to the top) never reaches it.
		let note = season(
			"My guy. Drafted him myself.",
			1999,
			"The year before",
			"Earlier.",
		);
		note = upsertSeasonNote(note, {
			season: 2000,
			kind: "retirement",
			headline: "Wrongly retired",
			body: "Bogus.",
		});
		note = removeSeasonNote(note, 2000, "retirement");
		assert.ok(note.includes("My guy. Drafted him myself."));
		assert.ok(note.includes("[1999] The year before"));
		assert.ok(!note.includes("Bogus."));
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
