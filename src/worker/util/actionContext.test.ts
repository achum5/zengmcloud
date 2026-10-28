import { assert, test } from "vitest";
import { beginAction, getCurrentAction } from "./actionContext.ts";

test("calls overlapping: the one started first keeps the credit, and a short one ending doesn't wipe it", () => {
	assert.strictEqual(getCurrentAction(), undefined);
	const endSim = beginAction("playMenu.untilPlayoffs");
	const endFaces = beginAction("main.getPlayerFaces");
	assert.strictEqual(getCurrentAction(), "playMenu.untilPlayoffs");
	endFaces();
	assert.strictEqual(getCurrentAction(), "playMenu.untilPlayoffs");
	endSim();
	endSim();
	assert.strictEqual(getCurrentAction(), undefined);
});
