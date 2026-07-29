import { assert, beforeEach, describe, test } from "vitest";
import {
	getRemoteTriviaScores,
	setupTriviaScores,
	teardownTriviaScores,
} from "./triviaScores.ts";
import type { SyncTransport, TriviaScoreEntry } from "./types.ts";

const entry = (
	overrides: Partial<TriviaScoreEntry> = {},
): TriviaScoreEntry => ({
	id: "1",
	game: "grids",
	ts: 1,
	score: 10,
	label: "a grid",
	detail: "",
	...overrides,
});

// Enough of a transport to exercise the subscribe/read path.
const fakeTransport = (clientId: string) => {
	let push: (
		scores: Record<string, TriviaScoreEntry[] | null> | undefined,
	) => void = () => {};
	const transport = {
		clientId,
		publish: async () => {},
		subscribe: () => () => {},
		subscribeTriviaScores: (onChange: typeof push) => {
			push = onChange;
			return () => {};
		},
	} as unknown as SyncTransport;
	return { transport, emit: (scores: any) => push(scores) };
};

describe("getRemoteTriviaScores", () => {
	beforeEach(() => {
		teardownTriviaScores();
	});

	test("nothing before the room has said anything", () => {
		const { transport } = fakeTransport("me");
		setupTriviaScores(transport);
		assert.deepStrictEqual(getRemoteTriviaScores("grids"), []);
	});

	// The local history is the authority for your own games; letting your own
	// bucket back in would list every game you played twice.
	test("this device's own bucket is excluded", () => {
		const { transport, emit } = fakeTransport("me");
		setupTriviaScores(transport);
		emit({
			me: [entry({ id: "mine" })],
			them: [entry({ id: "theirs" })],
		});
		assert.deepStrictEqual(
			getRemoteTriviaScores("grids").map((e) => e.id),
			["theirs"],
		);
	});

	// One document holds both games' results, so a bucket has to be filtered by
	// game or a roster quiz would turn up in the grid history.
	test("only the game asked for comes back", () => {
		const { transport, emit } = fakeTransport("me");
		setupTriviaScores(transport);
		emit({
			them: [
				entry({ id: "g", game: "grids" }),
				entry({ id: "t", game: "team" }),
			],
		});
		assert.deepStrictEqual(
			getRemoteTriviaScores("team").map((e) => e.id),
			["t"],
		);
	});

	test("an emptied bucket doesn't blow up", () => {
		const { transport, emit } = fakeTransport("me");
		setupTriviaScores(transport);
		emit({ them: null, other: [] });
		assert.deepStrictEqual(getRemoteTriviaScores("grids"), []);
	});

	test("after teardown nothing is reported", () => {
		const { transport, emit } = fakeTransport("me");
		setupTriviaScores(transport);
		emit({ them: [entry({ id: "theirs" })] });
		teardownTriviaScores();
		assert.deepStrictEqual(getRemoteTriviaScores("grids"), []);
	});
});
