import { assert, beforeAll, describe, test } from "vitest";
import {
	buildSentenceGamesForDay,
	linkRecapSegments,
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

describe("linkRecapSegments", () => {
	// Reassembling the pieces must give back the prose exactly, minus the
	// emphasis delimiters the rebalancer adds at cuts - so this compares with
	// those stripped.
	const reassemble = (segs: { text: string }[]) =>
		segs
			.map((s) => s.text)
			.join("")
			.replaceAll("*", "");

	const linked2 = (text: string, gs: SentenceGame[]) =>
		linkRecapSegments(text, gs)
			.filter((s) => s.href !== undefined)
			.map((s) => [s.text.replaceAll("*", "").trim(), s.href] as const);
	const linked = (text: string) => linked2(text, games);

	test("a single-game sentence wins the whole sentence", () => {
		const text =
			"The Heat held off the Kings 109-107 on Kendrick Perkins' game-winner.";
		assert.deepStrictEqual(linked(text), [
			[
				"The Heat held off the Kings 109-107 on Kendrick Perkins' game-winner.",
				games[0]!.href,
			],
		]);
	});

	test("a round-up sentence links each clause to its own game", () => {
		// The reported gap: these sentences named several games, so the
		// whole-sentence rule could only shrug at them and half the recap linked
		// nothing.
		const text =
			"Also on the night, the Pistons beat the Hawks 105-84, and the Wizards routed the Nets by 32.";
		assert.deepStrictEqual(linked(text), [
			["the Pistons beat the Hawks 105-84", games[1]!.href],
			["and the Wizards routed the Nets by 32.", games[2]!.href],
		]);
		assert.strictEqual(reassemble(linkRecapSegments(text, games)), text);
	});

	test("a colon and an 'and' are clause boundaries too", () => {
		const text =
			"There was drama elsewhere: the Pistons edged the Hawks 113-111 and the Wizards routed the Nets by 27.";
		assert.deepStrictEqual(linked(text), [
			["the Pistons edged the Hawks 113-111", games[1]!.href],
			["the Wizards routed the Nets by 27.", games[2]!.href],
		]);
	});

	test("stat commas never fragment a sentence about one game", () => {
		// Splitting this at its commas would leave "9 rebounds" as its own
		// unlinked scrap in the middle of a sentence that resolves perfectly well
		// whole.
		const text =
			"Kendrick Perkins had 31 points, 9 rebounds, and 6 assists as the Heat beat the Kings.";
		assert.deepStrictEqual(linked(text), [[text, games[0]!.href]]);
	});

	test("commas inside a player's aside are not clause boundaries", () => {
		const text =
			"On the injury front, Kendrick Perkins (sprained knee, out ~13 games) and Jason Richardson (sprained ankle, out ~9 games) went down.";
		assert.deepStrictEqual(linked(text), [
			["Kendrick Perkins (sprained knee, out ~13 games)", games[0]!.href],
			[
				"Jason Richardson (sprained ankle, out ~9 games) went down.",
				games[1]!.href,
			],
		]);
		assert.strictEqual(reassemble(linkRecapSegments(text, games)), text);
	});

	test("a clause naming nobody links nothing, and the prose survives", () => {
		const text = "Five of the 14 games were decided by five points or fewer.";
		assert.deepStrictEqual(linked(text), []);
		assert.strictEqual(reassemble(linkRecapSegments(text, games)), text);
	});

	// The screenshot bug: "[O.J. Mayo](/l/33/player/2055)" carries a sentence
	// boundary inside its own label (". " + capital), so the splitter cut the
	// link in half and both halves printed as raw markdown. Links must be opaque
	// to every cut.
	test("a sentence boundary inside a link label never cuts the link", () => {
		const withInitials: SentenceGame[] = [
			...games,
			{
				href: "/l/1/game_log/MEM_29/2016/103",
				names: ["Memphis Grizzlies", "Grizzlies", "Memphis", "O.J. Mayo"],
			},
		];
		const text =
			"[O.J. Mayo](/l/1/player/2055) scored 28 for the [Grizzlies](/l/1/roster/MEM_29/2016). The Heat beat the Kings.";
		const segs = linkRecapSegments(text, withInitials);
		// No segment may hold a broken link fragment...
		for (const seg of segs) {
			assert.notMatch(seg.text, /\[[^\]]*$/, `link cut open in "${seg.text}"`);
			assert.notMatch(seg.text, /^[^[]*]\(/, `link cut open in "${seg.text}"`);
		}
		// ...the prose reassembles exactly, and both sentences still resolve.
		assert.strictEqual(segs.map((s) => s.text).join(""), text);
		assert.deepStrictEqual(linked2(text, withInitials), [
			[
				"[O.J. Mayo](/l/1/player/2055) scored 28 for the [Grizzlies](/l/1/roster/MEM_29/2016).",
				withInitials[3]!.href,
			],
			["The Heat beat the Kings.", games[0]!.href],
		]);
	});

	// The other screenshot bug: the "_" in a roster URL like /roster/GSW_7/2009
	// was counted as an open italic, so the rebalancer appended a "closing" _ at
	// the end of every paragraph with a team link in it.
	test("underscores inside link URLs are not emphasis", () => {
		const text =
			"The [Kings](/l/1/roster/SAC_12/2016) fell to the [Heat](/l/1/roster/MIA_14/2016) in the final game.";
		const segs = linkRecapSegments(text, games);
		assert.strictEqual(segs.map((s) => s.text).join(""), text);
	});

	// The sub-headline is ONE italic run of "·"-separated blurbs. Cutting it left
	// the opening "*" in the first blurb and the closing one in the last, so both
	// rendered as literal asterisks with nothing italic between them.
	test("an emphasis run spanning cuts is closed and reopened, not broken", () => {
		const text =
			"*Jason Richardson pours in 42 · Beno Udrih goes for 38 · Kendrick Perkins wins it*";
		const segs = linkRecapSegments(text, games);
		for (const seg of segs) {
			const stars = (seg.text.match(/\*/g) ?? []).length;
			assert.strictEqual(stars % 2, 0, `unbalanced emphasis in "${seg.text}"`);
		}
		assert.deepStrictEqual(linked(text), [
			["Jason Richardson pours in 42", games[1]!.href],
			["Beno Udrih goes for 38", games[2]!.href],
			["Kendrick Perkins wins it", games[0]!.href],
		]);
		assert.strictEqual(reassemble(segs), text.replaceAll("*", ""));
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
