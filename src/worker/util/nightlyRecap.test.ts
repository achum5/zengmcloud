import { assert, test } from "vitest";
import { starLine } from "./nightlyRecap.ts";

const line = (more: Partial<Parameters<typeof starLine>[0]>) =>
	starLine({ pts: 30, reb: 3, ast: 2, tp: 1, blk: 0, stl: 0, ...more }, "CHA");

test("the Player of the Night's line: the points, what stood out, who against", () => {
	assert.strictEqual(
		line({ pts: 47, reb: 6, ast: 3, tp: 10 }),
		"47 PTS, 6 REB, 10 3PM VS. CHA",
	);
	// Nothing else big: the boards and assists anyway.
	assert.strictEqual(line({}), "30 PTS, 3 REB, 2 AST VS. CHA");
	// A triple-double with blocks: no more than three after the points, the
	// biggest of them, in the box score's order.
	assert.strictEqual(
		line({ pts: 28, reb: 12, ast: 11, blk: 6, tp: 5 }),
		"28 PTS, 12 REB, 11 AST, 6 BLK VS. CHA",
	);
});
