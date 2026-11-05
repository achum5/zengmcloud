import { assert, describe, test } from "vitest";
import {
	feedRows,
	highlightOf,
	periodLabel,
	playTitle,
	type FeedEntry,
} from "./playFeed.ts";

const line = (
	key: number,
	type: string,
	t: 0 | 1 | undefined,
	extra: Partial<FeedEntry> = {},
): FeedEntry => ({
	key,
	type,
	t,
	text: `${type} text`,
	textOnly: false,
	time: "6:49",
	period: 2,
	...extra,
});

describe("the gamecast's play rows", () => {
	test("a shot is one row: opened by the attempt, finished by the result", () => {
		// (Newest first, as the live game keeps its lines.)
		const rows = feedRows([
			line(3, "drb", 0),
			line(2, "missTp", 1),
			line(1, "fgaTp", 1),
		]);
		assert.strictEqual(rows.length, 2);
		assert.strictEqual(rows[1]!.key, 1);
		assert.strictEqual(rows[1]!.title, "Missed 3PT");
		assert.deepEqual(rows[1]!.texts, ["fgaTp text", "missTp text"]);
		assert.isFalse(rows[1]!.pending);
		assert.strictEqual(rows[0]!.title, "Defensive Rebound");
	});

	test("the attempt alone is a row waiting for its result", () => {
		const rows = feedRows([line(1, "fgaMidRange", 0)]);
		assert.strictEqual(rows.length, 1);
		assert.strictEqual(rows[0]!.title, "Shot");
		assert.isTrue(rows[0]!.pending);
	});

	test("a make takes the score and the points; a block goes to the blocker's team", () => {
		const made = feedRows([
			line(2, "fgAtRim", 0, { pts: [12, 9], scored: 2 }),
			line(1, "fgaAtRim", 0, { pts: [10, 9] }),
		])[0]!;
		assert.strictEqual(made.title, "Made FG");
		assert.deepEqual(made.pts, [12, 9]);
		assert.strictEqual(made.scored, 2);

		const blocked = feedRows([line(2, "blkTp", 0), line(1, "fgaTp", 1)])[0]!;
		assert.strictEqual(blocked.title, "Blocked Shot");
		assert.strictEqual(blocked.t, 0);
	});

	test("a three's and-one and a shooting foul on a three", () => {
		assert.strictEqual(
			feedRows([line(2, "tpAndOne", 1), line(1, "fgaTp", 1)])[0]!.title,
			"And-One 3PT",
		);
		const fouled = feedRows([
			line(3, "ft", 1),
			line(2, "pfTP", 0),
			line(1, "fgaTp", 1),
		]);
		assert.strictEqual(fouled.length, 2);
		assert.strictEqual(fouled[1]!.title, "Shooting Foul");
		assert.strictEqual(fouled[0]!.title, "Made FT");
	});

	test("lines that aren't shots are rows of their own", () => {
		assert.strictEqual(playTitle("sub"), "Substitution");
		assert.strictEqual(playTitle("timeout"), "Timeout");
		assert.strictEqual(playTitle("missFt"), "Missed FT");
		assert.strictEqual(playTitle("tov"), "Turnover");
	});

	test("periods as a broadcast says them", () => {
		const ordinal = (n: number) => `${n}th`;
		assert.strictEqual(periodLabel(2, 4, ordinal), "2th");
		assert.strictEqual(periodLabel(5, 4, ordinal), "OT");
		assert.strictEqual(periodLabel(7, 4, ordinal), "3OT");
		assert.isUndefined(periodLabel(undefined, 4, ordinal));
	});

	test("the highlights: dunks, threes, blocks, and-ones - not layups or misses", () => {
		const one = (lines: FeedEntry[]) => highlightOf(feedRows(lines)[0]!);
		assert.strictEqual(
			one([line(2, "fgAtRim", 0, { finish: "dunk" }), line(1, "fgaAtRim", 0)]),
			"Dunk",
		);
		assert.strictEqual(
			one([
				line(2, "fgAtRim", 0, { finish: "poster" }),
				line(1, "fgaAtRim", 0),
			]),
			"Poster dunk",
		);
		assert.isUndefined(
			one([line(2, "fgAtRim", 0, { finish: "layup" }), line(1, "fgaAtRim", 0)]),
		);
		assert.strictEqual(one([line(2, "tp", 1), line(1, "fgaTp", 1)]), "Three");
		assert.strictEqual(
			one([line(2, "blkAtRim", 0), line(1, "fgaAtRim", 1)]),
			"Block",
		);
		assert.isUndefined(one([line(2, "missTp", 1), line(1, "fgaTp", 1)]));
		// (Still in the air: not yet.)
		assert.isUndefined(one([line(1, "fgaTp", 1)]));
	});
});
