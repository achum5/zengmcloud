import { assert, describe, test } from "vitest";
import {
	countPlayerHighlights,
	filterPlayerHighlights,
	isPositivePlayForPid,
} from "./filterPlayerHighlights.ts";

// A stored play-by-play with several possessions. pid 7 = our target, pid 9 =
// a teammate/opponent. Each shot logs an attempt (fga*) then a result, so a
// highlight's lead-in is the attempt/miss just before it in the same possession.
const sample = () => [
	{ type: "init", boxScore: {} },
	{ type: "period", period: 1 },

	// Possession A: pid 7 scores a mid-range (attempt then make).
	{ type: "stat", t: 0, pid: 7, s: "pts", amt: 2 },
	{ type: "fgaMidRange", t: 0, pid: 7, clock: 700 },
	{ type: "fgMidRange", t: 0, pid: 7, clock: 699 },

	// Possession B: pid 9 makes a three (not our player) - dropped entirely.
	{ type: "stat", t: 0, pid: 9, s: "pts", amt: 3 },
	{ type: "fgaTp", t: 0, pid: 9, clock: 680 },
	{ type: "tp", t: 0, pid: 9, clock: 679 },

	// Possession C: pid 9 drives, pid 7 blocks it, pid 9 grabs the loose ball.
	{ type: "fgaAtRim", t: 0, pid: 9, clock: 660 },
	{ type: "blkAtRim", t: 1, pid: 7, clock: 659 },
	{ type: "drb", t: 0, pid: 9, clock: 658 }, // after the block, not pid 7 - dropped

	// Possession D: pid 9 misses, pid 7 grabs the defensive rebound.
	{ type: "fgaMidRange", t: 0, pid: 9, clock: 640 },
	{ type: "missMidRange", t: 0, pid: 9, clock: 639 },
	{ type: "drb", t: 1, pid: 7, clock: 638 },

	// Possession E: pid 7 turns it over (negative) - dropped, no lead-in kept.
	{ type: "fgaAtRim", t: 1, pid: 7, clock: 620 },
	{ type: "tov", t: 1, pid: 7, clock: 619 },

	{ type: "gameOver" },
];

const DESCRIPTIVE = (e: any) =>
	!["init", "stat", "timeouts", "period", "overtime", "gameOver"].includes(
		e.type,
	);

describe("isPositivePlayForPid", () => {
	test("counts a make, an assist, a steal, a rebound, and a block", () => {
		assert.strictEqual(
			isPositivePlayForPid({ type: "fgMidRange", pid: 7 }, 7),
			true,
		);
		assert.strictEqual(
			isPositivePlayForPid({ type: "fgAtRim", pid: 9, pidAst: 7 }, 7),
			true,
		);
		assert.strictEqual(isPositivePlayForPid({ type: "stl", pid: 7 }, 7), true);
		assert.strictEqual(isPositivePlayForPid({ type: "drb", pid: 7 }, 7), true);
		assert.strictEqual(
			isPositivePlayForPid({ type: "blkTp", pid: 7 }, 7),
			true,
		);
	});

	test("excludes misses, turnovers, attempts, and other players' plays", () => {
		assert.strictEqual(
			isPositivePlayForPid({ type: "missTp", pid: 7 }, 7),
			false,
		);
		assert.strictEqual(isPositivePlayForPid({ type: "tov", pid: 7 }, 7), false);
		// A shot ATTEMPT is not itself a highlight (only the make is).
		assert.strictEqual(
			isPositivePlayForPid({ type: "fgaMidRange", pid: 7 }, 7),
			false,
		);
		assert.strictEqual(isPositivePlayForPid({ type: "tp", pid: 9 }, 7), false);
		assert.strictEqual(
			isPositivePlayForPid({ type: "blkAtRim", pid: 9 }, 7),
			false,
		);
	});
});

describe("countPlayerHighlights", () => {
	test("counts only pid 7's positive plays (make, block, rebound = 3)", () => {
		assert.strictEqual(countPlayerHighlights(sample(), 7), 3);
	});

	test("is zero for a player with no positive plays", () => {
		assert.strictEqual(countPlayerHighlights(sample(), 123), 0);
	});
});

describe("filterPlayerHighlights", () => {
	test("keeps each highlight plus its possession's lead-in, dropping the rest", () => {
		const out = filterPlayerHighlights(sample(), 7);
		const descriptive = out.filter(DESCRIPTIVE).map((e) => e.type);

		// Each highlight is preceded by its build-up, and unrelated possessions
		// (pid 9's three) and post-highlight trailers (pid 9's rebound after the
		// block, pid 7's turnover) are gone.
		assert.deepStrictEqual(descriptive, [
			"fgaMidRange", // lead-in to pid 7's make
			"fgMidRange", // highlight: the make
			"fgaAtRim", // lead-in to pid 7's block (pid 9 driving)
			"blkAtRim", // highlight: the block
			"fgaMidRange", // lead-in to pid 7's rebound (pid 9's shot)
			"missMidRange", // lead-in: the miss
			"drb", // highlight: the rebound
		]);
	});

	test("keeps all housekeeping so the box score stays correct", () => {
		const out = filterPlayerHighlights(sample(), 7);
		const types = out.map((e) => e.type);
		assert.strictEqual(out.filter((e) => e.type === "stat").length, 2);
		assert.ok(types.includes("init"));
		assert.ok(types.includes("period"));
		assert.ok(types.includes("gameOver"));
	});

	test("drops pid 9's own scoring possession entirely", () => {
		const out = filterPlayerHighlights(sample(), 7);
		const descriptive = out.filter(DESCRIPTIVE);
		assert.ok(!descriptive.some((e) => e.type === "tp"));
		assert.ok(!descriptive.some((e) => e.type === "fgaTp"));
		assert.ok(!descriptive.some((e) => e.type === "tov"));
	});

	test("preserves order", () => {
		const out = filterPlayerHighlights(sample(), 7);
		const clocks = out
			.filter((e) => typeof e.clock === "number")
			.map((e) => e.clock);
		const sorted = [...clocks].sort((a, b) => b - a);
		assert.deepStrictEqual(clocks, sorted);
	});

	test("a player with no highlights yields no descriptive plays", () => {
		const out = filterPlayerHighlights(sample(), 123);
		assert.strictEqual(out.filter(DESCRIPTIVE).length, 0);
	});
});
