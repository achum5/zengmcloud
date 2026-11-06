import { assert, describe, test } from "vitest";
import { playSentence } from "./sentence.ts";

const names: Record<number, string> = {
	1: "Devin Booker",
	2: "Franz Wagner",
	3: "Wendell Carter Jr.",
	4: "Jalen Green",
};
const say = (events: any[], finish?: string) =>
	playSentence({
		events,
		finish,
		name: (pid) => (pid === undefined ? undefined : names[pid]),
		teamName: (t) => (t === 0 ? "Phoenix Suns" : "Orlando Magic"),
	});

describe("plays as a broadcast says them", () => {
	test("shots: made, missed, blocked, fouled", () => {
		assert.strictEqual(
			say([
				{ type: "fgaTp", pid: 1 },
				{ type: "missTp", pid: 1 },
			]),
			"Devin Booker misses three point jumper",
		);
		assert.strictEqual(
			say(
				[
					{ type: "fgaAtRim", pid: 3 },
					{ type: "fgAtRim", pid: 3, pidAst: 2 },
				],
				"layup",
			),
			"Wendell Carter Jr. makes layup (Franz Wagner assists)",
		);
		assert.strictEqual(
			say(
				[
					{ type: "fgaAtRim", pid: 3 },
					{ type: "fgAtRim", pid: 3, pidDefense: 4 },
				],
				"poster",
			),
			"Wendell Carter Jr. dunks on Jalen Green",
		);
		assert.strictEqual(
			say([
				{ type: "fgaMidRange", pid: 1 },
				{ type: "blkMidRange", pid: 2 },
			]),
			"Franz Wagner blocks Devin Booker's jump shot",
		);
		assert.strictEqual(
			say([
				{ type: "fgaTp", pid: 1 },
				{ type: "pfTP", pid: 2 },
			]),
			"Franz Wagner shooting foul (Devin Booker to the line for 3)",
		);
		assert.strictEqual(
			say([
				{ type: "fgaTp", pid: 1 },
				{ type: "tpAndOne", pid: 1 },
			]),
			"Devin Booker makes three point jumper, and the foul",
		);
	});

	test("the rest of the game", () => {
		assert.strictEqual(
			say([{ type: "drb", pid: 4 }]),
			"Jalen Green defensive rebound",
		);
		assert.strictEqual(
			say([{ type: "stl", pid: 2, pidTov: 1 }]),
			"Devin Booker lost ball turnover (Franz Wagner steals)",
		);
		assert.strictEqual(
			say([{ type: "sub", pids: [3], pidsOff: [2] }]),
			"Wendell Carter Jr. enters for Franz Wagner",
		);
		assert.strictEqual(
			say([{ type: "timeout", t: 1, numLeft: 3 }]),
			"Orlando Magic timeout (3 left)",
		);
		// (A line it has no way of saying: as the game wrote it.)
		assert.isUndefined(say([{ type: "elamActive" }]));
		assert.isUndefined(say([{ type: "drb", pid: 99 }]));
	});
});
