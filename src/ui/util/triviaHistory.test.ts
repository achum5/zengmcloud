import { assert, beforeEach, describe, test } from "vitest";
import {
	addHistoryEntry,
	clearHistory,
	countPerfect,
	deleteHistoryEntry,
	filterHistory,
	loadHistory,
	mergeHistory,
	summarize,
	type TriviaHistoryEntry,
} from "./triviaHistory.ts";

// The worker-side test environment has no DOM, so the storage tests supply a
// real key/value store. Without one every read falls into the module's catch
// and returns an empty history, which would make the tests pass by accident.
const installLocalStorage = () => {
	const store = new Map<string, string>();
	(globalThis as any).localStorage = {
		getItem: (key: string) => store.get(key) ?? null,
		setItem: (key: string, value: string) => {
			store.set(key, value);
		},
		removeItem: (key: string) => {
			store.delete(key);
		},
		clear: () => {
			store.clear();
		},
	};
};

const entry = (
	overrides: Partial<TriviaHistoryEntry> = {},
): TriviaHistoryEntry => ({
	id: "1",
	ts: 1000,
	score: 100,
	label: "2054 Los Angeles Clippers",
	detail: "9/9 solved",
	...overrides,
});

describe("filterHistory", () => {
	const entries = [
		entry({ id: "a", ts: 300, score: 50, label: "2054 Clippers", tid: 3 }),
		entry({ id: "b", ts: 200, score: 220, label: "2072 Celtics", tid: 1 }),
		entry({ id: "c", ts: 100, score: 90, label: "2072 Hawks", tid: 1 }),
	];

	test("most recent first by default", () => {
		assert.deepStrictEqual(
			filterHistory(entries, {}).map((e) => e.id),
			["a", "b", "c"],
		);
	});

	test("highest score first when asked", () => {
		assert.deepStrictEqual(
			filterHistory(entries, { sort: "best" }).map((e) => e.id),
			["b", "c", "a"],
		);
	});

	// Two games with the same score should not come back in an arbitrary order.
	test("ties fall back to recency", () => {
		const tied = [
			entry({ id: "old", ts: 100, score: 40 }),
			entry({ id: "new", ts: 900, score: 40 }),
		];
		assert.deepStrictEqual(
			filterHistory(tied, { sort: "best" }).map((e) => e.id),
			["new", "old"],
		);
	});

	test("filters to one team", () => {
		assert.deepStrictEqual(
			filterHistory(entries, { tid: 1 }).map((e) => e.id),
			["b", "c"],
		);
	});

	test("finds a game by who played it", () => {
		const withAuthor = [
			entry({ id: "a", byName: "Alex" }),
			entry({ id: "b", byName: "Sam" }),
		];
		assert.deepStrictEqual(
			filterHistory(withAuthor, { query: "alex" }).map((e) => e.id),
			["a"],
		);
	});

	// The team filter has to catch both senses: a roster quiz ABOUT the Celtics,
	// and any game played BY whoever runs the Celtics.
	test("the team filter matches the subject team or the author's team", () => {
		const mixed = [
			entry({ id: "subject", tid: 5 }),
			entry({ id: "author", byTid: 5 }),
			entry({ id: "neither", tid: 9 }),
		];
		assert.deepStrictEqual(
			filterHistory(mixed, { tid: 5 })
				.map((e) => e.id)
				.sort(),
			["author", "subject"],
		);
	});

	test("searches the label and the detail, case-insensitively", () => {
		assert.deepStrictEqual(
			filterHistory(entries, { query: "celtics" }).map((e) => e.id),
			["b"],
		);
		assert.deepStrictEqual(
			filterHistory([entry({ id: "z", detail: "7/9 solved" })], {
				query: "7/9",
			}).map((e) => e.id),
			["z"],
		);
	});

	test("an empty query matches everything", () => {
		assert.strictEqual(filterHistory(entries, { query: "   " }).length, 3);
	});

	test("does not mutate the list it was given", () => {
		const original = [...entries];
		filterHistory(entries, { sort: "best" });
		assert.deepStrictEqual(entries, original);
	});
});

describe("summarize", () => {
	test("an empty history is all zeroes, not NaN", () => {
		assert.deepStrictEqual(summarize([]), {
			played: 0,
			best: 0,
			average: 0,
		});
	});

	test("counts, best and mean", () => {
		assert.deepStrictEqual(
			summarize([
				entry({ score: 100 }),
				entry({ score: 200 }),
				entry({ score: 300 }),
			]),
			{ played: 3, best: 300, average: 200 },
		);
	});
});

describe("mergeHistory", () => {
	const mine = [entry({ id: "mine", ts: 200, label: "my grid" })];

	test("keeps both sides, newest first", () => {
		const merged = mergeHistory(mine, [
			entry({ id: "theirs", ts: 300, label: "their grid", byName: "Alex" }),
		]);
		assert.deepStrictEqual(
			merged.map((e) => e.id),
			["theirs", "mine"],
		);
	});

	// A device that sees its own bucket come back from the room must not end up
	// with every game listed twice.
	test("an entry that comes back from the room is not duplicated", () => {
		const merged = mergeHistory(mine, [entry({ id: "mine", ts: 200 })]);
		assert.strictEqual(merged.length, 1);
	});

	test("junk from the room is ignored, not rendered", () => {
		const merged = mergeHistory(mine, [null, 7, {}, { id: "x" }]);
		assert.deepStrictEqual(
			merged.map((e) => e.id),
			["mine"],
		);
	});
});

describe("countPerfect", () => {
	test("counts only games with nothing left on the board", () => {
		assert.strictEqual(
			countPerfect([
				entry({ progress: { done: 9, total: 9 } }),
				entry({ progress: { done: 8, total: 9 } }),
				entry({ progress: { done: 15, total: 15 } }),
				entry(),
			]),
			2,
		);
	});
});

describe("storage", () => {
	beforeEach(() => {
		installLocalStorage();
		clearHistory("grids");
		clearHistory("team");
	});

	test("a new entry lands at the front", () => {
		addHistoryEntry("grids", {
			score: 10,
			label: "first",
			detail: "",
			ts: 1,
		});
		const after = addHistoryEntry("grids", {
			score: 20,
			label: "second",
			detail: "",
			ts: 2,
		});
		assert.deepStrictEqual(
			after.map((e) => e.label),
			["second", "first"],
		);
		assert.deepStrictEqual(
			loadHistory("grids").map((e) => e.label),
			["second", "first"],
		);
	});

	// Two games finished in the same millisecond must still be separately
	// deletable, so ids cannot be the timestamp alone.
	test("entries recorded in the same millisecond get distinct ids", () => {
		addHistoryEntry("grids", { score: 1, label: "a", detail: "", ts: 5 });
		const after = addHistoryEntry("grids", {
			score: 2,
			label: "b",
			detail: "",
			ts: 5,
		});
		assert.notStrictEqual(after[0]!.id, after[1]!.id);
	});

	test("delete removes exactly one entry", () => {
		addHistoryEntry("grids", { score: 1, label: "a", detail: "", ts: 1 });
		const two = addHistoryEntry("grids", {
			score: 2,
			label: "b",
			detail: "",
			ts: 2,
		});
		const after = deleteHistoryEntry("grids", two[0]!.id);
		assert.deepStrictEqual(
			after.map((e) => e.label),
			["a"],
		);
	});

	// Each game keeps its own list - a grid must never turn up in the roster
	// game's history.
	test("the two games do not share a list", () => {
		addHistoryEntry("grids", { score: 1, label: "grid", detail: "", ts: 1 });
		addHistoryEntry("team", { score: 2, label: "team", detail: "", ts: 1 });
		assert.deepStrictEqual(
			loadHistory("grids").map((e) => e.label),
			["grid"],
		);
		assert.deepStrictEqual(
			loadHistory("team").map((e) => e.label),
			["team"],
		);
	});

	test("garbage in storage reads as an empty history", () => {
		localStorage.setItem("triviaHistory:grids", "not json");
		assert.deepStrictEqual(loadHistory("grids"), []);
		localStorage.setItem("triviaHistory:grids", '{"not":"an array"}');
		assert.deepStrictEqual(loadHistory("grids"), []);
	});

	test("entries missing required fields are dropped, not returned", () => {
		localStorage.setItem(
			"triviaHistory:grids",
			JSON.stringify([{ id: "ok", ts: 1, score: 1, label: "x" }, { junk: 1 }]),
		);
		assert.deepStrictEqual(
			loadHistory("grids").map((e) => e.id),
			["ok"],
		);
	});
});
