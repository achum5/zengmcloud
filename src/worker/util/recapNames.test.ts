import { assert, describe, test } from "vitest";
import {
	cityReferences,
	expandSecondReferences,
	inWomensLeague,
	secondReferences,
	shortNames,
	surnameOf,
} from "./recapNames.ts";

const PACERS = { region: "Indiana", name: "Pacers" };
const LAKERS = { region: "Los Angeles", name: "Lakers" };

describe("surnameOf", () => {
	test("the name a paper uses after the first", () => {
		assert.strictEqual(surnameOf("Lonnie Ingram"), "Ingram");
		assert.strictEqual(surnameOf("Gary Trent Jr."), "Trent");
		assert.strictEqual(surnameOf("Robert Williams III"), "Williams");
		assert.strictEqual(surnameOf("Nick Van Exel"), "Van Exel");
		assert.strictEqual(surnameOf("Juan Carlos de la Fuente"), "de la Fuente");
		assert.strictEqual(surnameOf("Karl-Anthony Towns"), "Towns");
		assert.strictEqual(surnameOf("Mac McClung"), "McClung");
		assert.isUndefined(surnameOf("Nene"));
	});
});

describe("shortNames", () => {
	test("a surname two men share, or that is a first name or a team, stays whole", () => {
		const short = shortNames(
			[
				"Lonnie Ingram",
				"Herb Ingram",
				"Grant Jackson",
				"Jerami Grant",
				"Marcus Pacer",
				"Obi Indiana",
				"Zeke Fields",
			],
			[PACERS, LAKERS],
		);
		// Jerami Grant stays whole beside a Grant Jackson; Jackson is fine.
		assert.deepStrictEqual(
			[...short.entries()],
			[
				["Grant Jackson", "Jackson"],
				["Zeke Fields", "Fields"],
			],
		);
	});
});

describe("secondReferences", () => {
	const players = ["Lonnie Ingram", "Gary Trent Jr.", "Zeke Fields"];

	test("whole name first, surname after, headline untouched", () => {
		const text =
			"**Lonnie Ingram leads the Pacers**\n\nLonnie Ingram had 31 points. Lonnie Ingram's layup tied it.\n\nZeke Fields added 12. Lonnie Ingram made five threes.";
		assert.strictEqual(
			secondReferences(text, players, [PACERS, LAKERS]),
			"**Lonnie Ingram leads the Pacers**\n\nLonnie Ingram had 31 points. Ingram's layup tied it.\n\nZeke Fields added 12. Ingram made five threes.",
		);
	});

	test("a team tag goes once he has been introduced", () => {
		const text =
			"**Pacers win**\n\nThe Lakers' Zeke Fields had 30. The Pacers won. Los Angeles' Zeke Fields left hurt, as the Lakers' Zeke Fields often does.";
		assert.strictEqual(
			secondReferences(text, players, [PACERS, LAKERS]),
			"**Pacers win**\n\nThe Lakers' Zeke Fields had 30. The Pacers won. Fields left hurt, as Fields often does.",
		);
	});

	test("a day recap's deck is part of the head", () => {
		const text =
			"**Pacers win**\n\n*Zeke Fields scores 30*\n\nZeke Fields had 30. Zeke Fields also had 9 rebounds.";
		assert.strictEqual(
			secondReferences(text, players, [PACERS, LAKERS]),
			"**Pacers win**\n\n*Zeke Fields scores 30*\n\nZeke Fields had 30. Fields also had 9 rebounds.",
		);
	});

	test("the period of a Jr. that closed a sentence stays", () => {
		const text =
			"**Lakers win**\n\nGary Trent Jr. had 20. The last word went to Gary Trent Jr. The Lakers won. Gary Trent Jr.'s three iced it.";
		assert.strictEqual(
			secondReferences(text, players, [PACERS, LAKERS]),
			"**Lakers win**\n\nGary Trent Jr. had 20. The last word went to Trent. The Lakers won. Trent's three iced it.",
		);
	});

	test("a particle opening a sentence is capitalized", () => {
		const text =
			"**Lakers win**\n\nJuan Carlos de la Fuente had 20. Juan Carlos de la Fuente added 9 rebounds.";
		assert.strictEqual(
			secondReferences(text, ["Juan Carlos de la Fuente"], [PACERS, LAKERS]),
			"**Lakers win**\n\nJuan Carlos de la Fuente had 20. De la Fuente added 9 rebounds.",
		);
	});

	test("the checker reads it back to whole names", () => {
		const text =
			"**Lonnie Ingram leads the Pacers**\n\nLonnie Ingram had 31 points. Lonnie Ingram's layup tied it. Gary Trent Jr. had 20, and Gary Trent Jr. hit 4 threes.";
		const short = secondReferences(text, players, [PACERS, LAKERS]);
		assert.notStrictEqual(short, text);
		assert.strictEqual(
			expandSecondReferences(short, players, [PACERS, LAKERS]),
			text,
		);
	});
});

describe("cityReferences", () => {
	const text =
		"**Pacers beat the Lakers**\n\nThe Pacers (3-1) beat the Lakers (1-3) 104-99. Zeke Fields had 20 points for the Pacers.\n\nThe Lakers' bench scored 40. Lonnie Ingram led the Lakers with 22. The Lakers missed 21 threes. Gary Trent Jr. had 14 for the Lakers, who host the Pacers next. A long paragraph keeps going here so the story is long enough for two of them, and then some more words to get it over six hundred characters in all, which it now is, more or less, after all of this.";

	test("only where the grammar is the same either way, never in the lede", () => {
		const seen = new Set<string>();
		for (let seed = 0; seed < 200; seed++) {
			const out = cityReferences(
				text,
				[PACERS, LAKERS],
				["Zeke Fields", "Lonnie Ingram", "Gary Trent Jr."],
				seed,
			);
			const [head, lede] = out.split("\n\n");
			assert.strictEqual(head, "**Pacers beat the Lakers**");
			assert.match(
				lede!,
				/^The Pacers \(3-1\) beat the Lakers \(1-3\) 104-99\./,
			);
			// Subjects keep the nickname: "Los Angeles missed" is never written.
			assert.include(out, "The Lakers missed 21 threes.");
			for (const m of out.matchAll(
				/(?:for |with |to )?(?:Indiana|Los Angeles)(?:'s?|,)?(?: bench)?/g,
			)) {
				seen.add(m[0]);
			}
			assert.isAtMost((out.match(/Los Angeles/g) ?? []).length, 2);
		}
		assert.includeMembers(
			[...seen],
			["for Indiana", "Los Angeles' bench"],
			[...seen].join(" | "),
		);
		// "for Los Angeles, who host..." would need the plural the city does
		// not have.
		assert.notInclude([...seen].join(" | "), "Los Angeles,");
	});

	test("a pair written to match stays matched", () => {
		const pair =
			"**Pacers win**\n\nThe Pacers beat the Lakers. Zeke Fields scored for the Pacers. It was 15-of-28 for the Pacers, 5-of-25 for the Lakers. The Lakers' bench had 12, the Pacers' bench 30.";
		for (let seed = 0; seed < 100; seed++) {
			const out = cityReferences(pair, [PACERS, LAKERS], [], seed);
			assert.include(out, "15-of-28 for the Pacers, 5-of-25 for the Lakers");
			assert.include(out, "The Lakers' bench had 12, the Pacers' bench 30.");
		}
	});

	test("a city both teams share is never used", () => {
		for (let seed = 0; seed < 50; seed++) {
			const out = cityReferences(
				text.replaceAll("Pacers", "Clippers"),
				[{ region: "Los Angeles", name: "Clippers" }, LAKERS],
				[],
				seed,
			);
			assert.notInclude(out, "Los Angeles");
		}
	});
});

describe("inWomensLeague", () => {
	test("the prose turns, the names do not", () => {
		const text =
			"**Lin He leads the Iron Men**\n\nLin He had 20 points. He made his first six shots, and the Iron Men got six men in double figures. The ball found the open man all night. Nobody stopped him; he did it himself. His streak is at five.";
		assert.strictEqual(
			inWomensLeague(
				text,
				["Lin He"],
				[{ region: "Pittsburgh", name: "Iron Men" }],
			),
			"**Lin He leads the Iron Men**\n\nLin He had 20 points. She made her first six shots, and the Iron Men got six players in double figures. The ball found the open teammate all night. Nobody stopped her; she did it herself. Her streak is at five.",
		);
	});
});
