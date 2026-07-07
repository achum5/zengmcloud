import { assert, describe, test } from "vitest";
import { buildRecapPrompt, parseRecaps } from "./gameRecap.ts";
import type { RecapGame } from "../../worker/util/getDayGamesForRecap.ts";

const player = (
	name: string,
	pts: number,
	extra: Record<string, number> = {},
) => ({
	name,
	min: 34,
	pts,
	reb: 8,
	ast: 5,
	stl: 1,
	blk: 0,
	tov: 2,
	fg: 10,
	fga: 18,
	tp: 2,
	tpa: 5,
	ft: 4,
	fta: 4,
	pf: 2,
	...extra,
});

const game = (gid: number): RecapGame => ({
	gid,
	day: 7,
	overtimes: 1,
	winnerTid: 0,
	teams: [
		{
			tid: 0,
			region: "Brooklyn",
			name: "Bagels",
			abbrev: "BKN",
			pts: 105,
			players: [player("Star Guy", 30), player("Role Guy", 12)],
		},
		{
			tid: 1,
			region: "Boston",
			name: "Massacre",
			abbrev: "BOS",
			pts: 103,
			players: [player("Their Ace", 28)],
		},
	],
	clutchPlays: ['<a href="/l/1/player/5">Star Guy</a> hit the game-winner.'],
});

describe("buildRecapPrompt", () => {
	test("bakes in ids, scores, stat lines, and cleaned highlights", () => {
		const prompt = buildRecapPrompt([game(42)], "Day 7");
		// Marker id the AI must echo back.
		assert.ok(prompt.includes("GAME 42"), prompt);
		// Both teams + scores.
		assert.ok(prompt.includes("BKN 105") || prompt.includes("105"), prompt);
		assert.ok(prompt.includes("Brooklyn Bagels"), prompt);
		assert.ok(prompt.includes("Boston Massacre"), prompt);
		// A stat line.
		assert.ok(prompt.includes("Star Guy: 30 PTS"), prompt);
		// Highlight, with the HTML link stripped to plain text.
		assert.ok(prompt.includes("Star Guy hit the game-winner."), prompt);
		assert.ok(!prompt.includes("<a href"), prompt);
	});
});

describe("parseRecaps", () => {
	test("files each recap to its game id, ignoring preamble", () => {
		const text = `Here are your recaps!

<!--game:42-->
**Bagels edge Massacre in OT**

Brooklyn survived a thriller.

<!--game:43-->
**Blowout in Boston**

Not close.`;
		const map = parseRecaps(text);
		assert.strictEqual(map.size, 2);
		assert.ok(
			map.get(42)!.startsWith("**Bagels edge Massacre in OT**"),
			map.get(42),
		);
		assert.ok(map.get(42)!.includes("Brooklyn survived a thriller."));
		assert.ok(map.get(43)!.includes("Not close."));
		// Preamble before the first marker is dropped.
		assert.ok(!map.get(42)!.includes("Here are your recaps"));
	});

	test("tolerates spacing variations in the marker", () => {
		const map = parseRecaps("<!-- game: 7 -->\nRecap seven.");
		assert.strictEqual(map.get(7), "Recap seven.");
	});

	test("no markers → empty map (so the UI can warn)", () => {
		const map = parseRecaps("The AI forgot the markers entirely.");
		assert.strictEqual(map.size, 0);
	});
});
