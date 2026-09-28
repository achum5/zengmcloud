import { assert, describe, test } from "vitest";

// THE SCHEDULE HAS AN ORDER BUT NO CALENDAR. Games are numbered days, and
// nothing in a league says how far apart two of them are, so a line about
// tomorrow, rest or a back-to-back is made up. The recap checker catches it
// at runtime; this catches it in the source, where a line added later would
// otherwise only surface when some seed happened to draw it.
const sources = (
	import.meta as unknown as {
		glob: (
			patterns: string[],
			options: { query: string; import: string; eager: true },
		) => Record<string, string>;
	}
).glob(
	[
		"./social*.ts",
		"!./*.test.ts",
		"../worker/util/socialFeed.ts",
		"../worker/util/recapBeats.ts",
		"../worker/util/getAutoRecap.ts",
		"../worker/util/getDayGamesForRecap.ts",
	],
	{ query: "?raw", import: "default", eager: true },
);

// "May" is left out: it is a verb far more often than a month.
// Two phrases that sound like the calendar and are not: "the last night of
// the season" and "back-to-back championships".
const CALENDAR =
	/\b(?:tomorrow|yesterday|last night(?! of)|in (?:two|three|four|five|six|seven|\d+) days|days' rest|days of rest|a day off|days off|rest day|back-to-back(?! champion)|back to back(?! champion)|the night before|two nights|in a month|next month|(?:all|this|next|every) week|(?:january|february|march|april|june|july|august|september|october|november|december)(?! \d))\b/i;

// Prose in comments explains why the lines are gone, so scan the code only.
const codeOf = (text: string): string[] =>
	text.split("\n").map((line) => {
		const trimmed = line.trimStart();
		return trimmed.startsWith("//") || trimmed.startsWith("*") ? "" : line;
	});

describe("no calendar talk", () => {
	test("the files are found", () => {
		assert.isAtLeast(Object.keys(sources).length, 8);
	});

	test("no recap or feed line says when anything happens", () => {
		const hits: string[] = [];
		for (const [path, text] of Object.entries(sources)) {
			codeOf(text).forEach((line, i) => {
				// The accuracy checker names these phrases in order to reject them.
				if (CALENDAR.test(line) && !line.includes('add("calendar"')) {
					hits.push(`${path}:${i + 1}: ${line.trim()}`);
				}
			});
		}
		assert.deepStrictEqual(hits, []);
	});
});
