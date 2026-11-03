import { assert, describe, test } from "vitest";
import { ownsSync } from "./instanceOwner.ts";

describe("one copy of a shared league syncs on a device", () => {
	test("no owner recorded yet: free to go on", () => {
		assert.isTrue(ownsSync(undefined, 1, "a"));
	});

	test("this copy took it last: owner", () => {
		assert.isTrue(ownsSync({ id: "a", lid: 1, at: 0 }, 1, "a"));
	});

	test("another copy took it since: not the owner", () => {
		assert.isFalse(ownsSync({ id: "b", lid: 1, at: 0 }, 1, "a"));
	});

	test("another copy syncing a different league: no conflict", () => {
		assert.isTrue(ownsSync({ id: "b", lid: 2, at: 0 }, 1, "a"));
	});
});
