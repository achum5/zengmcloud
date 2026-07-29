import { assert, beforeEach, describe, test } from "vitest";
import { clearProgress, loadProgress, saveProgress } from "./triviaProgress.ts";

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
	return store;
};

describe("triviaProgress", () => {
	let store: Map<string, string>;
	beforeEach(() => {
		store = installLocalStorage();
	});

	test("a saved game comes back", () => {
		saveProgress("grids", 3, { cells: [1, 2, 3], gaveUp: false });
		assert.deepStrictEqual(loadProgress("grids", 3), {
			cells: [1, 2, 3],
			gaveUp: false,
		});
	});

	// pids and tids mean different players in a different league, so a save from
	// one must never be restored into another.
	test("a save from another league is ignored", () => {
		saveProgress("grids", 3, { cells: [1] });
		assert.strictEqual(loadProgress("grids", 4), undefined);
		assert.strictEqual(loadProgress("grids", undefined), undefined);
	});

	test("each game has its own slot", () => {
		saveProgress("grids", 1, { which: "grids" });
		saveProgress("team", 1, { which: "team" });
		assert.deepStrictEqual(loadProgress("grids", 1), { which: "grids" });
		assert.deepStrictEqual(loadProgress("team", 1), { which: "team" });
	});

	test("nothing saved reads as nothing", () => {
		assert.strictEqual(loadProgress("higherLower", 1), undefined);
	});

	test("clearing removes it", () => {
		saveProgress("eightyTwoZero", 1, { picks: [] });
		clearProgress("eightyTwoZero");
		assert.strictEqual(loadProgress("eightyTwoZero", 1), undefined);
	});

	// An old save half-read into a new shape is worse than no save at all.
	test("a save from an older format is discarded", () => {
		store.set(
			"triviaProgress:grids",
			JSON.stringify({ v: 0, lid: 1, state: { cells: [] } }),
		);
		assert.strictEqual(loadProgress("grids", 1), undefined);
	});

	test("corrupt storage reads as nothing", () => {
		store.set("triviaProgress:grids", "not json");
		assert.strictEqual(loadProgress("grids", 1), undefined);
		store.set("triviaProgress:grids", JSON.stringify({ v: 1, lid: 1 }));
		assert.strictEqual(loadProgress("grids", 1), undefined);
		store.set(
			"triviaProgress:grids",
			JSON.stringify({ v: 1, lid: 1, state: "a string" }),
		);
		assert.strictEqual(loadProgress("grids", 1), undefined);
	});

	test("saving replaces the previous save rather than appending", () => {
		saveProgress("grids", 1, { n: 1 });
		saveProgress("grids", 1, { n: 2 });
		assert.deepStrictEqual(loadProgress("grids", 1), { n: 2 });
	});
});
