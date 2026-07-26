import { assert, describe, test } from "vitest";
import { buildHintOptions, HINT_OPTION_COUNT } from "./triviaHint.ts";

// Row criterion covers 1-40, column covers 30-70, so 30-40 is the cell.
const ROW = Array.from({ length: 40 }, (_, i) => i + 1);
const COL = Array.from({ length: 41 }, (_, i) => i + 30);
const CELL = ROW.filter((pid) => COL.includes(pid));

const rarity: Record<number, number> = {};
for (let pid = 1; pid <= 70; pid += 1) {
	rarity[pid] = pid;
}
const popByPid = new Map<number, number>();
for (let pid = 1; pid <= 70; pid += 1) {
	popByPid.set(pid, 100 - pid);
}

const build = (over: Partial<Parameters<typeof buildHintOptions>[0]> = {}) =>
	buildHintOptions({
		cellPids: CELL,
		rarity,
		rowPids: ROW,
		colPids: COL,
		usedPids: new Set(),
		popByPid,
		seed: "grid|4|0",
		...over,
	});

describe("buildHintOptions", () => {
	test("deals six options with exactly one correct", () => {
		const options = build();
		assert.strictEqual(options.length, HINT_OPTION_COUNT);
		assert.strictEqual(options.filter((o) => o.correct).length, 1);
	});

	test("the correct option actually satisfies the cell", () => {
		const correct = build().find((o) => o.correct)!;
		assert.ok(CELL.includes(correct.pid));
	});

	test("every distractor satisfies exactly one criterion - never both", () => {
		// This is the whole point of hint mode: a wrong answer has to LOOK right.
		for (const option of build().filter((o) => !o.correct)) {
			const inRow = ROW.includes(option.pid);
			const inCol = COL.includes(option.pid);
			assert.ok(inRow || inCol, `pid ${option.pid} satisfies neither`);
			assert.ok(!(inRow && inCol), `pid ${option.pid} satisfies both`);
		}
	});

	test("no duplicate players", () => {
		const pids = build().map((o) => o.pid);
		assert.strictEqual(new Set(pids).size, pids.length);
	});

	test("never offers a player already used elsewhere on the board", () => {
		const usedPids = new Set([1, 2, 3, 4, 5, 31, 32, 33, 55, 56, 57]);
		for (const option of build({ usedPids })) {
			assert.ok(!usedPids.has(option.pid));
		}
	});

	test("the correct answer is drawn from the most common qualifiers", () => {
		// Rarity here equals pid, so the cell's common end is its low pids.
		const correct = build().find((o) => o.correct)!;
		const sorted = [...CELL].sort((a, b) => rarity[a]! - rarity[b]!);
		const topFifth = sorted.slice(0, Math.max(1, Math.ceil(CELL.length * 0.2)));
		assert.ok(topFifth.includes(correct.pid));
	});

	test("same seed deals the same hand, so a re-render is stable", () => {
		assert.deepStrictEqual(build(), build());
	});

	test("reshuffling changes the hand", () => {
		const a = build({ seed: "grid|4|0" }).map((o) => o.pid);
		const b = build({ seed: "grid|4|1" }).map((o) => o.pid);
		assert.notStrictEqual(JSON.stringify(a), JSON.stringify(b));
	});

	test("the correct answer isn't always in the same slot", () => {
		const slots = new Set<number>();
		for (let i = 0; i < 12; i += 1) {
			slots.add(build({ seed: `grid|4|${i}` }).findIndex((o) => o.correct));
		}
		assert.ok(slots.size > 1, "correct answer never moved");
	});

	test("no eligible players left means no hint rather than a bogus one", () => {
		assert.deepStrictEqual(build({ usedPids: new Set(CELL) }), []);
	});

	test("degrades gracefully when there aren't five distractors available", () => {
		// Tiny criteria: only a couple of single-criterion players exist.
		const options = buildHintOptions({
			cellPids: [1],
			rarity: { 1: 10, 2: 20, 3: 30 },
			rowPids: [1, 2],
			colPids: [1, 3],
			usedPids: new Set(),
			popByPid: new Map([
				[1, 50],
				[2, 40],
				[3, 30],
			]),
			seed: "s",
		});
		assert.strictEqual(options.filter((o) => o.correct).length, 1);
		assert.ok(options.length <= HINT_OPTION_COUNT);
		assert.strictEqual(new Set(options.map((o) => o.pid)).size, options.length);
	});
});
