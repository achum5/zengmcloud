import { assert, describe, test } from "vitest";
import {
	hasSeasonNote,
	splitPlayerNote,
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

	// Above the headed sections, not below. Below is unreadable: nothing marks
	// where a section ends, so the next parse folds trailing prose into the
	// oldest writeup and it stops being the player's own note at all.
	test("hand-written text is preserved, above the headed sections", () => {
		const before = "My favorite player. Traded for him in a heist.";
		const after = season(before, 2005, "Rookie", "A rookie year.");
		assert.strictEqual(
			after,
			"My favorite player. Traded for him in a heist.\n\n[2005] Rookie\nA rookie year.",
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

// Every piece of a career note has somewhere on the page it belongs, and the
// note block at the top is left holding only what a person typed. Gilbert
// Arenas is the worked example: drafted 2001, scouted under [2000] (the
// prospects pass runs a season early), first played in 2002.
describe("deciding where each part of a player's note goes", () => {
	const NOTE = [
		"[2003] Third year",
		"The leap.",
		"",
		"[2002] Rookie year",
		"He played.",
		"",
		"[2001] Taken fourth overall",
		"Did not play this season.",
		"",
		"[2000] A 6'4\" lead guard out of Arizona",
		"Built to score and create in equal measure.",
	].join("\n");

	const split = (note: string, overrides?: Partial<Parameters<typeof splitPlayerNote>[1]>) =>
		splitPlayerNote(note, {
			draftYear: 2001,
			undrafted: false,
			seasonsWithStats: new Set([2002, 2003]),
			seasonsWithRatings: new Set([2001, 2002, 2003]),
			...overrides,
		});

	test("the draft-year section is the draft recap", () => {
		const { draftRecap } = split(NOTE);
		assert.strictEqual(draftRecap.length, 1);
		assert.strictEqual(draftRecap[0]!.season, 2001);
		assert.strictEqual(draftRecap[0]!.headline, "Taken fourth overall");
	});

	// The whole reason this needs deciding: [2000] is a year he has no row for
	// at all, so left alone it would sit in the note block forever.
	test("a pre-draft scouting report is kept for the draft season's row", () => {
		const { scouting } = split(NOTE);
		assert.strictEqual(scouting.length, 1);
		assert.strictEqual(scouting[0]!.season, 2000);
	});

	test("seasons he played hang off their stats row", () => {
		const { bySeason } = split(NOTE);
		assert.deepStrictEqual([...bySeason.keys()].sort(), [2002, 2003]);
	});

	test("nothing routed is left in the note block", () => {
		assert.strictEqual(split(NOTE).leftover, "");
	});

	test("hand-written text stays in the note block", () => {
		const { leftover, bySeason } = split(`Fun guy.\n\n${NOTE}`);
		assert.strictEqual(leftover, "Fun guy.");
		assert.strictEqual(bySeason.size, 2);
	});

	// A year on a roster but never played has a ratings row and no stats row.
	// Without this it would be unreachable now that the note block is empty.
	test("a season missed entirely hangs off its ratings row", () => {
		const { byRatingsSeason, leftover } = split(
			"[2004] Lost year\nHe tore his achilles in October.",
			{
				seasonsWithRatings: new Set([2001, 2002, 2003, 2004]),
			},
		);
		assert.deepStrictEqual([...byRatingsSeason.keys()], [2004]);
		assert.strictEqual(leftover, "");
	});

	// His scouting report IS his page - there is no draft line to hang it off
	// and no career to speak of, so nothing moves.
	test("a prospect keeps his whole note at the top", () => {
		const { leftover, scouting, draftRecap } = split(NOTE, {
			undrafted: true,
		});
		assert.strictEqual(scouting.length, 0);
		assert.strictEqual(draftRecap.length, 0);
		assert.ok(leftover.includes("[2000]"));
	});

	// Better in the wrong place than gone.
	test("a scouting report with no draft-season row falls back to the note block", () => {
		const { scouting, leftover } = split(NOTE, {
			seasonsWithRatings: new Set([2002, 2003]),
		});
		assert.strictEqual(scouting.length, 0);
		assert.ok(leftover.includes("[2000]"));
	});
});

// The one thing in a note that cannot be regenerated, and the easiest to lose:
// a reader walking the note line by line cannot tell trailing prose from more
// of the section above it, so freeform stored at the bottom came back as part
// of the oldest writeup - and, once writeups moved onto their season's rows,
// was shown under that year instead of as the player's own note.
describe("hand-written text survives a writeup being added", () => {
	test("it is still its own section after a round trip", () => {
		const note = upsertSeasonNote("My own thoughts.", {
			season: 2005,
			body: "A season.",
		});
		const sections = parseSeasonNote(note);
		assert.strictEqual(sections.length, 2);
		const freeform = sections.find((s) => s.season === undefined);
		assert.strictEqual(freeform?.body, "My own thoughts.");
		assert.strictEqual(
			sections.find((s) => s.season === 2005)?.body,
			"A season.",
		);
	});

	test("it stays out of the writeups and in the note block", () => {
		let note = upsertSeasonNote("My own thoughts.", {
			season: 2005,
			body: "A season.",
		});
		note = upsertSeasonNote(note, { season: 2006, body: "Another season." });

		const { leftover, bySeason } = splitPlayerNote(note, {
			draftYear: 2004,
			undrafted: false,
			seasonsWithStats: new Set([2005, 2006]),
			seasonsWithRatings: new Set([2004, 2005, 2006]),
		});
		assert.strictEqual(leftover, "My own thoughts.");
		assert.strictEqual(bySeason.get(2005)![0]!.body, "A season.");
	});
});
