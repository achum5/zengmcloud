import { assert, describe, test } from "vitest";
import { contextLink, inlineLinks, teamForHashtag } from "./socialLinks.ts";

const TEAMS = [
	{ tid: 0, abbrev: "BOS", region: "Boston", name: "Celtics" },
	{ tid: 1, abbrev: "SAC", region: "Sacramento", name: "Kings" },
	{ tid: 2, abbrev: "MIA", region: "Miami", name: "Heat" },
	{ tid: 3, abbrev: "IND", region: "Indiana", name: "Pacers" },
];

const NAMES = { 5: "Jalen Mathis", 9: "A.J. Green" };

const post = (over: Partial<any> = {}): any => ({
	pids: [],
	tids: [],
	eventType: "gameResult",
	...over,
});

describe("inlineLinks", () => {
	test("only the people and clubs the post is actually about", () => {
		const links = inlineLinks(post({ pids: [5], tids: [0] }), NAMES, TEAMS);
		const labels = links.map((l) => l.label);
		assert.include(labels, "Jalen Mathis");
		assert.include(labels, "Boston Celtics");
		assert.include(labels, "Celtics");
		assert.include(labels, "BOS");
		// Nobody else in the league gets linked just for existing.
		assert.notInclude(labels, "Kings");
		assert.notInclude(labels, "A.J. Green");
	});

	test("the longest name comes first, so it is matched first", () => {
		// Otherwise "Celtics" eats the back of "Boston Celtics" and the link
		// starts halfway through the name.
		const links = inlineLinks(post({ tids: [0] }), NAMES, TEAMS);
		const lengths = links.map((l) => l.label.length);
		assert.deepStrictEqual(lengths, [...lengths].sort((a, b) => b - a));
		assert.strictEqual(links[0]!.label, "Boston Celtics");
	});

	test("a pid with no name known contributes nothing", () => {
		// Rather than linking an empty string, which would match everywhere.
		const links = inlineLinks(post({ pids: [404] }), NAMES, TEAMS);
		assert.lengthOf(links, 0);
	});

	test("the same club named twice is one entry", () => {
		const links = inlineLinks(post({ tids: [0], tid: 0 }), NAMES, TEAMS);
		const celtics = links.filter((l) => l.label === "Celtics");
		assert.lengthOf(celtics, 1);
	});
});

describe("teamForHashtag", () => {
	test("an abbrev or a nickname", () => {
		assert.strictEqual(teamForHashtag("#BOS", TEAMS), 0);
		assert.strictEqual(teamForHashtag("#Celtics", TEAMS), 0);
		assert.strictEqual(teamForHashtag("#celtics", TEAMS), 0);
	});

	test("a nickname with a word stuck on it", () => {
		assert.strictEqual(teamForHashtag("#GoPacers", TEAMS), 3);
		assert.strictEqual(teamForHashtag("#PacersNation", TEAMS), 3);
	});

	test("a tag that is nobody stays plain text", () => {
		assert.isUndefined(teamForHashtag("#tonight", TEAMS));
		assert.isUndefined(teamForHashtag("#1", TEAMS));
	});

	test("a nickname that is an ordinary word is not matched loosely", () => {
		// "Heat" and "Kings" are real words. A rule that only asks whether the
		// tag CONTAINS the nickname turns #Heater into Miami and #Kingston into
		// Sacramento. The exact forms still work.
		assert.strictEqual(teamForHashtag("#Heat", TEAMS), 2);
		assert.strictEqual(teamForHashtag("#MIA", TEAMS), 2);
		assert.isUndefined(teamForHashtag("#Heater", TEAMS));
		assert.isUndefined(teamForHashtag("#Kingston", TEAMS));
		assert.isUndefined(teamForHashtag("#heater", TEAMS));
		assert.isUndefined(teamForHashtag("#kingston", TEAMS));
	});

	test("a tag flattened to lower case still finds its club", () => {
		// The casual voices lowercase everything they post, so the camel seam
		// this normally reads is gone by the time the tag is seen.
		assert.strictEqual(teamForHashtag("#gopacers", TEAMS), 3);
		assert.strictEqual(teamForHashtag("#pacersnation", TEAMS), 3);
		assert.strictEqual(teamForHashtag("#celtics", TEAMS), 0);
	});
});

describe("contextLink", () => {
	test("a game gets its box score", () => {
		const link = contextLink(post({ gid: 42, tids: [1] }), TEAMS);
		assert.strictEqual(link?.label, "Box score");
		assert.deepStrictEqual(link?.target, { kind: "game", gid: 42, tid: 1 });
	});

	test("a transaction gets the transactions page", () => {
		for (const eventType of [
			"trade",
			"signing",
			"release",
			"draft",
			"retirement",
		]) {
			const link = contextLink(post({ eventType, tids: [2] }), TEAMS);
			assert.strictEqual(link?.label, "Transactions", eventType);
			assert.deepStrictEqual(link?.target, { kind: "transactions", tid: 2 });
		}
	});

	test("the standings and the bracket", () => {
		assert.strictEqual(
			contextLink(post({ eventType: "standings" }), TEAMS)?.label,
			"Standings",
		);
		assert.strictEqual(
			contextLink(post({ eventType: "playoffs" }), TEAMS)?.label,
			"Playoffs",
		);
	});

	test("nothing to point at means no link rather than a dead one", () => {
		assert.isUndefined(contextLink(post({ eventType: "performance" }), TEAMS));
		// A free agent carries no club, so a transactions link would land on
		// whatever team tid -1 resolves to.
		assert.isUndefined(
			contextLink(post({ eventType: "signing", tids: [-1] }), TEAMS),
		);
	});
});
