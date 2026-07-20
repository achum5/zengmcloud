import { assert, describe, test } from "vitest";
import {
	countPlayerHighlights,
	filterPlayerHighlights,
	isPositivePlayForPid,
} from "./filterPlayerHighlights.ts";

// A small stored-play-by-play sample: init, then interleaved stat updates and
// descriptive plays for two players (pid 7 = our target, pid 9 = a teammate/opp).
const sample = () => [
	{ type: "init", boxScore: {} },
	{ type: "period", period: 1 },
	// pid 7 made mid-range, assisted by pid 9
	{ type: "stat", t: 0, pid: 7, s: "pts", amt: 2 },
	{ type: "fgMidRange", t: 0, pid: 7, pidAst: 9, clock: 700 },
	// pid 9 made a three (not our player, and not assisted by us)
	{ type: "stat", t: 0, pid: 9, s: "pts", amt: 3 },
	{ type: "tp", t: 0, pid: 9, clock: 650 },
	// pid 7 assist on pid 9's at-rim make
	{ type: "fgAtRim", t: 0, pid: 9, pidAst: 7, clock: 600 },
	// pid 7 missed (negative - dropped)
	{ type: "missTp", t: 0, pid: 7, clock: 500 },
	// pid 7 steal
	{ type: "stl", t: 0, pid: 7, pidTov: 9, clock: 480 },
	// pid 7 defensive rebound
	{ type: "drb", t: 0, pid: 7, clock: 460 },
	// pid 9 block (not our player)
	{ type: "blkAtRim", t: 1, pid: 9, clock: 440 },
	// pid 7 block
	{ type: "blkTp", t: 1, pid: 7, clock: 420 },
	// pid 7 turnover (negative - dropped)
	{ type: "tov", t: 0, pid: 7, clock: 400 },
	{ type: "gameOver" },
];

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

	test("excludes misses, turnovers, and other players' plays", () => {
		assert.strictEqual(
			isPositivePlayForPid({ type: "missTp", pid: 7 }, 7),
			false,
		);
		assert.strictEqual(isPositivePlayForPid({ type: "tov", pid: 7 }, 7), false);
		assert.strictEqual(isPositivePlayForPid({ type: "tp", pid: 9 }, 7), false);
		assert.strictEqual(
			isPositivePlayForPid({ type: "blkAtRim", pid: 9 }, 7),
			false,
		);
		// Housekeeping is not itself a "highlight".
		assert.strictEqual(
			isPositivePlayForPid({ type: "stat", pid: 7 }, 7),
			false,
		);
		assert.strictEqual(isPositivePlayForPid({ type: "period" }, 7), false);
	});
});

describe("countPlayerHighlights", () => {
	test("counts only pid 7's positive plays", () => {
		// make, assist, steal, drb, block = 5
		assert.strictEqual(countPlayerHighlights(sample(), 7), 5);
	});

	test("is zero for a player with no positive plays", () => {
		assert.strictEqual(countPlayerHighlights(sample(), 123), 0);
	});
});

describe("filterPlayerHighlights", () => {
	test("keeps all housekeeping + quarter markers + only pid 7's positive plays", () => {
		const out = filterPlayerHighlights(sample(), 7);
		const types = out.map((e) => e.type);

		// Every stat/init/period/gameOver survives (box score stays correct).
		assert.strictEqual(out.filter((e) => e.type === "stat").length, 2);
		assert.ok(types.includes("init"));
		assert.ok(types.includes("period"));
		assert.ok(types.includes("gameOver"));

		// pid 7's plays kept; pid 9's own plays and pid 7's misses/tov dropped.
		const descriptive = out.filter(
			(e) =>
				![
					"init",
					"stat",
					"timeouts",
					"period",
					"overtime",
					"gameOver",
				].includes(e.type),
		);
		assert.deepStrictEqual(
			descriptive.map((e) => e.type),
			["fgMidRange", "fgAtRim", "stl", "drb", "blkTp"],
		);
		// The kept fgAtRim is the one pid 7 assisted (pidAst 7), not pid 9's tp.
		assert.ok(!descriptive.some((e) => e.type === "tp"));
		assert.ok(!descriptive.some((e) => e.type === "missTp"));
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
});
