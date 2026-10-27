import { assert, test } from "vitest";
import { CLOUD_CHANGES, unseenChanges } from "./cloudChanges.ts";

test("changes are numbered one after another, newest first", () => {
	CLOUD_CHANGES.forEach((c, i) => {
		assert.strictEqual(c.n, CLOUD_CHANGES.length - i);
		assert.match(c.date, /^\d{4}-\d{2}-\d{2}$/);
		assert.isAbove(c.text.length, 0);
	});
});

test("only what he hasn't seen - everything if he has seen nothing", () => {
	assert.strictEqual(unseenChanges(undefined).length, CLOUD_CHANGES.length);
	assert.strictEqual(unseenChanges(CLOUD_CHANGES[0]!.n).length, 0);
	assert.deepEqual(
		unseenChanges(CLOUD_CHANGES[0]!.n - 1).map((c) => c.n),
		[CLOUD_CHANGES[0]!.n],
	);
});
