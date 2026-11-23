import { assert, test } from "vitest";
import { spreadLine } from "./spreadLine.ts";

test("the favorite and its points, as a scoreboard writes the line", () => {
	assert.strictEqual(spreadLine(5.5, "BOS", "NYK"), "BOS -5.5");
	assert.strictEqual(spreadLine(-3, "BOS", "NYK"), "NYK -3");
	assert.strictEqual(spreadLine(0, "BOS", "NYK"), "PK");
	assert.strictEqual(spreadLine(undefined, "BOS", "NYK"), undefined);
});
