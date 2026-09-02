import { assert, describe, test } from "vitest";
import {
	assignHandles,
	baseHandle,
	playerAccountId,
	resolveAccounts,
	teamAccountId,
	type ImplicitPlayer,
	type ImplicitTeam,
	type SocialAccount,
} from "./socialAccounts.ts";
import { BASE_PERSONALITY } from "./socialPersonality.ts";

const team = (tid: number, region: string, name: string): ImplicitTeam => ({
	tid,
	region,
	name,
	abbrev: region.slice(0, 3).toUpperCase(),
});

const player = (
	pid: number,
	name: string,
	overrides: Partial<ImplicitPlayer> = {},
): ImplicitPlayer => ({
	pid,
	name,
	tid: 0,
	age: 26,
	ovr: 50,
	experience: 5,
	moodTraits: [],
	...overrides,
});

const TEAMS = [team(0, "Boston", "Celtics"), team(1, "Sacramento", "Kings")];

describe("baseHandle", () => {
	test("strips punctuation and spaces", () => {
		assert.strictEqual(baseHandle("Boston Celtics"), "BostonCeltics");
		assert.strictEqual(baseHandle("D'Angelo Russell"), "DAngeloRussell");
	});

	test("folds accents to ASCII so the handle can live in a URL", () => {
		assert.strictEqual(baseHandle("Nenê"), "Nene");
		assert.strictEqual(baseHandle("Andris Biedriņš"), "AndrisBiedrins");
	});

	test("never returns an empty handle", () => {
		// A name this strips to nothing would otherwise produce an unroutable
		// account page.
		assert.strictEqual(baseHandle("...").length > 0, true);
		assert.strictEqual(baseHandle(""), "account");
	});

	test("is capped so a long name still fits a handle", () => {
		assert.strictEqual(
			baseHandle("Bartholomew Fitzwilliam Throckmorton").length <= 15,
			true,
		);
	});
});

describe("assignHandles", () => {
	test("identical names get distinct handles", () => {
		const handles = assignHandles(
			[
				{ id: "a", name: "Chris Johnson" },
				{ id: "b", name: "Chris Johnson" },
				{ id: "c", name: "Chris Johnson" },
			],
			new Map(),
		);
		const values = [...handles.values()];
		assert.strictEqual(new Set(values).size, 3);
		assert.strictEqual(values[0], "ChrisJohnson");
	});

	test("a hand-typed handle wins its spot over a derived collision", () => {
		// The failure this prevents: a derived handle squatting on the name the
		// user deliberately typed, so the user's own edit is the one that ends
		// up with a number stuck on it.
		const handles = assignHandles(
			[
				{ id: "derived", name: "Celtics" },
				{ id: "chosen", name: "Some Fan Account" },
			],
			new Map([["chosen", "Celtics"]]),
		);
		assert.strictEqual(handles.get("chosen"), "Celtics");
		assert.notStrictEqual(handles.get("derived"), "Celtics");
	});

	test("collisions are case-insensitive", () => {
		const handles = assignHandles(
			[
				{ id: "a", name: "celtics" },
				{ id: "b", name: "Celtics" },
			],
			new Map(),
		);
		assert.notStrictEqual(handles.get("a"), handles.get("b"));
	});

	test("the same input always produces the same handles", () => {
		// Two devices must resolve the same account link for the same league.
		const input = [
			{ id: "a", name: "Chris Johnson" },
			{ id: "b", name: "Chris Johnson" },
		];
		assert.deepStrictEqual(
			[...assignHandles(input, new Map()).entries()],
			[...assignHandles(input, new Map()).entries()],
		);
	});
});

describe("resolveAccounts", () => {
	test("every player and team gets an account with nothing stored", () => {
		const accounts = resolveAccounts({
			players: [player(1, "Paul Pierce"), player(2, "Rajon Rondo")],
			teams: TEAMS,
			stored: [],
		});
		assert.strictEqual(accounts.length, 4);
		assert.strictEqual(
			accounts.every((a) => a.implicit),
			true,
		);
	});

	test("a player account carries his team, position and archetype", () => {
		const accounts = resolveAccounts({
			players: [player(1, "Paul Pierce", { pos: "SF", tid: 0 })],
			teams: TEAMS,
			stored: [],
		});
		const p = accounts.find((a) => a.pid === 1)!;
		assert.strictEqual(p.kind, "player");
		assert.strictEqual(p.archetypeId, "player");
		assert.strictEqual(p.tid, 0);
		assert.strictEqual(p.bio, "SF · Boston Celtics");
	});

	test("a retired player keeps his account and it says so", () => {
		const accounts = resolveAccounts({
			players: [player(1, "Paul Pierce", { pos: "SF", retired: true })],
			teams: TEAMS,
			stored: [],
		});
		assert.strictEqual(accounts.find((a) => a.pid === 1)!.bio, "SF · Retired");
	});

	test("a stored override edits one account and leaves the rest derived", () => {
		const stored: SocialAccount[] = [
			{
				id: playerAccountId(1),
				kind: "player",
				name: "The Truth",
				bio: "34",
			},
		];
		const accounts = resolveAccounts({
			players: [player(1, "Paul Pierce"), player(2, "Rajon Rondo")],
			teams: TEAMS,
			stored,
		});
		const edited = accounts.find((a) => a.pid === 1)!;
		const untouched = accounts.find((a) => a.pid === 2)!;
		assert.strictEqual(edited.name, "The Truth");
		assert.strictEqual(edited.bio, "34");
		assert.strictEqual(edited.implicit, false);
		assert.strictEqual(untouched.name, "Rajon Rondo");
		assert.strictEqual(untouched.implicit, true);
	});

	test("a tombstone removes an implicit account instead of re-deriving it", () => {
		const accounts = resolveAccounts({
			players: [player(1, "Paul Pierce"), player(2, "Rajon Rondo")],
			teams: TEAMS,
			stored: [{ id: playerAccountId(1), kind: "player", removed: true }],
		});
		assert.strictEqual(
			accounts.some((a) => a.pid === 1),
			false,
		);
		assert.strictEqual(
			accounts.some((a) => a.pid === 2),
			true,
		);
	});

	test("a disabled team has no account", () => {
		const accounts = resolveAccounts({
			players: [],
			teams: [{ ...team(0, "Boston", "Celtics"), disabled: true }],
			stored: [],
		});
		assert.strictEqual(accounts.length, 0);
	});

	test("an explicit media account appears alongside the derived ones", () => {
		const accounts = resolveAccounts({
			players: [player(1, "Paul Pierce")],
			teams: TEAMS,
			stored: [
				{
					id: "m:abc",
					kind: "media",
					name: "League Insider",
					handle: "Insider",
					archetypeId: "insider",
					tid: 0,
				},
			],
		});
		const media = accounts.find((a) => a.id === "m:abc")!;
		assert.strictEqual(media.kind, "media");
		assert.strictEqual(media.handle, "Insider");
		assert.strictEqual(media.archetypeId, "insider");
		// The insider archetype cares about transactions, not box scores.
		assert.strictEqual(media.personality.topics.trade > 0, true);
		assert.strictEqual(media.personality.topics.gameResult, 0);
	});

	test("a per-account edit beats the archetype it inherits", () => {
		const accounts = resolveAccounts({
			players: [],
			teams: [],
			stored: [
				{
					id: "m:abc",
					kind: "media",
					name: "Quiet Insider",
					archetypeId: "insider",
					personality: { postiness: 0.1, topics: { gameResult: 5 } },
				},
			],
		});
		const media = accounts[0]!;
		assert.strictEqual(media.personality.postiness, 0.1);
		// The override adds a topic without wiping the twelve it did not
		// mention - the trap a wholesale spread would set for the batch editor.
		assert.strictEqual(media.personality.topics.gameResult, 5);
		assert.strictEqual(media.personality.topics.trade > 0, true);
	});

	test("mood traits move a player's voice off the plain archetype", () => {
		const accounts = resolveAccounts({
			players: [
				player(1, "Loud Guy", { moodTraits: ["F"], age: 22, ovr: 65 }),
				player(2, "Quiet Guy", { moodTraits: ["W"], age: 33, ovr: 45 }),
			],
			teams: TEAMS,
			stored: [],
		});
		const loud = accounts.find((a) => a.pid === 1)!.personality;
		const quiet = accounts.find((a) => a.pid === 2)!.personality;
		// Fame wants an audience; Winning talks about the standings.
		assert.strictEqual(loud.postiness > quiet.postiness, true);
		assert.strictEqual(loud.topics.offTopic > quiet.topics.offTopic, true);
		assert.strictEqual(quiet.topics.standings > loud.topics.standings, true);
		// And a young player types less carefully than a 33-year-old.
		assert.strictEqual(loud.formality < quiet.formality, true);
	});

	test("editing one topic on a player keeps the ones his traits gave him", () => {
		// Two sparse layers stack on a player: the mood-trait derivation and the
		// user's own edit. Merging them wholesale would silently wipe the
		// derived topics the moment anyone touched a single unrelated one.
		const traitsOnly = resolveAccounts({
			players: [player(1, "Loud Guy", { moodTraits: ["F"], age: 22 })],
			teams: TEAMS,
			stored: [],
		}).find((a) => a.pid === 1)!.personality;

		const edited = resolveAccounts({
			players: [player(1, "Loud Guy", { moodTraits: ["F"], age: 22 })],
			teams: TEAMS,
			stored: [
				{
					id: playerAccountId(1),
					kind: "player",
					personality: { topics: { injury: 9 } },
				},
			],
		}).find((a) => a.pid === 1)!.personality;

		assert.strictEqual(edited.topics.injury, 9);
		assert.strictEqual(edited.topics.offTopic, traitsOnly.topics.offTopic);
		assert.strictEqual(edited.topics.awards, traitsOnly.topics.awards);
	});

	test("a team account is corporate and points at its own team", () => {
		const accounts = resolveAccounts({
			players: [],
			teams: TEAMS,
			stored: [],
		});
		const celtics = accounts.find((a) => a.id === teamAccountId(0))!;
		assert.strictEqual(celtics.kind, "team");
		assert.strictEqual(celtics.tid, 0);
		assert.strictEqual(celtics.personality.tone, "corporate");
		assert.strictEqual(celtics.personality.optimism, 1);
	});

	test("an unknown archetype falls back to the neutral base rather than throwing", () => {
		const accounts = resolveAccounts({
			players: [],
			teams: [],
			stored: [
				{ id: "m:x", kind: "media", name: "Mystery", archetypeId: "nope" },
			],
		});
		assert.strictEqual(accounts[0]!.personality.tone, BASE_PERSONALITY.tone);
	});

	test("handles are unique across the whole league", () => {
		const accounts = resolveAccounts({
			players: [
				player(1, "Boston Celtics"),
				player(2, "Boston Celtics"),
				player(3, "Chris Johnson"),
			],
			teams: TEAMS,
			stored: [],
		});
		const handles = accounts.map((a) => a.handle.toLowerCase());
		assert.strictEqual(new Set(handles).size, handles.length);
		// The franchise wins the plain handle over a player who happens to
		// share the string, because it is the more findable of the two.
		assert.strictEqual(
			accounts.find((a) => a.id === teamAccountId(0))!.handle,
			"BostonCeltics",
		);
	});
});
