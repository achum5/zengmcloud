import { assert, beforeAll, describe, test } from "vitest";
import {
	buildSentenceGamesForDay,
	resolveSentenceGame,
	splitSentences,
	type SentenceGame,
} from "./linkifyRecap.ts";
import { localActions } from "./local.ts";

// The day-recap sentence links: a sentence whose names all point at one game
// gets that game's box score; anything ambiguous gets nothing rather than a
// guess. See RecapBanner / Markdown for the rendering half.

const games: SentenceGame[] = [
	{
		href: "/l/1/game_log/MIA_14/2016/100",
		names: [
			"Miami Heat",
			"Heat",
			"Miami",
			"Sacramento Kings",
			"Kings",
			"Sacramento",
			"Kendrick Perkins",
			"Dwyane Wade",
		],
	},
	{
		href: "/l/1/game_log/DET_8/2016/101",
		names: [
			"Detroit Pistons",
			"Pistons",
			"Detroit",
			"Atlanta Hawks",
			"Hawks",
			"Atlanta",
			"Jason Richardson",
		],
	},
	{
		href: "/l/1/game_log/WAS_24/2016/102",
		names: [
			"Washington Wizards",
			"Wizards",
			"Washington",
			"Brooklyn Nets",
			"Nets",
			"Brooklyn",
			"Beno Udrih",
		],
	},
];

describe("resolveSentenceGame", () => {
	test("a sentence about one game links to it", () => {
		assert.strictEqual(
			resolveSentenceGame(
				"The Heat held off the Kings 109-107 on Kendrick Perkins' game-winner with 10.9 seconds left.",
				games,
			),
			games[0]!.href,
		);
	});

	test("a player name alone is enough", () => {
		assert.strictEqual(
			resolveSentenceGame("Jason Richardson pours in 42", games),
			games[1]!.href,
		);
	});

	test("a round-up sentence spanning games resolves to nothing", () => {
		assert.isUndefined(
			resolveSentenceGame(
				"Elsewhere, the Pistons beat the Hawks and the Wizards routed the Nets by 32.",
				games,
			),
		);
	});

	test("markdown links are matched by their labels", () => {
		assert.strictEqual(
			resolveSentenceGame(
				"[Beno Udrih](/l/1/player/77) had 38 in the [Wizards](/l/1/roster/WAS_24/2016)' win.",
				games,
			),
			games[2]!.href,
		);
	});

	test("names only match on word boundaries", () => {
		// "Heat" must not fire inside another word; the possessive apostrophe is
		// not a word character, so "Kings'" still matches.
		assert.isUndefined(resolveSentenceGame("A Heatwave hit the arena.", games));
		assert.strictEqual(
			resolveSentenceGame("The Kings' bench emptied.", games),
			games[0]!.href,
		);
	});

	test("no names, no link", () => {
		assert.isUndefined(
			resolveSentenceGame("Five of the 14 games were close.", games),
		);
	});
});

describe("splitSentences", () => {
	test("splits on sentence ends, not decimals", () => {
		const segs = splitSentences(
			"The Heat won on a shot with 10.9 seconds left. Jason Richardson led all scorers.",
		);
		assert.deepStrictEqual(
			segs.map((s) => s.text),
			[
				"The Heat won on a shot with 10.9 seconds left.",
				" ",
				"Jason Richardson led all scorers.",
			],
		);
		assert.deepStrictEqual(
			segs.map((s) => s.boundary),
			[false, true, false],
		);
	});

	test("the sub-headline's · separator is a boundary", () => {
		const segs = splitSentences(
			"Jason Richardson pours in 42 · Beno Udrih goes for 38",
		);
		assert.deepStrictEqual(
			segs.map((s) => s.text),
			["Jason Richardson pours in 42", " · ", "Beno Udrih goes for 38"],
		);
	});

	test("reassembly is lossless", () => {
		const text =
			"One thing happened. Then another! Did a third? · A blurb · Done.";
		assert.strictEqual(
			splitSentences(text)
				.map((s) => s.text)
				.join(""),
			text,
		);
	});
});

describe("buildSentenceGamesForDay", () => {
	// leagueUrl prefixes /l/<lid>, and without a league loaded it degrades to "/".
	beforeAll(() => {
		localActions.update({ lid: 0 });
	});

	test("collects team and player names with a box score href", () => {
		const built = buildSentenceGamesForDay(
			[
				{
					gid: 100,
					season: 2016,
					teams: [
						{ tid: 14, players: [{ pid: 9, name: "Kendrick Perkins" }] },
						{ tid: 12, players: [] },
					],
				},
			],
			(tid) =>
				tid === 14
					? { abbrev: "MIA", region: "Miami", name: "Heat" }
					: { abbrev: "SAC", region: "Sacramento", name: "Kings" },
		);
		assert.strictEqual(built.length, 1);
		assert.strictEqual(built[0]!.href, "/l/0/game_log/MIA_14/2016/100");
		assert.include(built[0]!.names, "Miami Heat");
		assert.include(built[0]!.names, "Kings");
		assert.include(built[0]!.names, "Kendrick Perkins");
	});

	test("the All-Star Game's negative tids use the special slug", () => {
		const built = buildSentenceGamesForDay(
			[
				{
					gid: 200,
					season: 2016,
					teams: [
						{ tid: -1, players: [{ pid: 3, name: "Star One" }] },
						{ tid: -2, players: [] },
					],
				},
			],
			() => undefined,
		);
		assert.strictEqual(built[0]!.href, "/l/0/game_log/special/2016/200");
		assert.include(built[0]!.names, "Star One");
	});
});
