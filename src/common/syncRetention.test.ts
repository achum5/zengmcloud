import { assert, describe, test } from "vitest";
import { isTooFarBehind, RETENTION_MS, ttlAtMsFor } from "./syncRetention.ts";

describe("isTooFarBehind", () => {
	test("a device that has never synced is never too far behind", () => {
		// Watermark 0 means "read the whole log", so nothing can have been missed.
		assert.strictEqual(isTooFarBehind(0, 5_000), false);
		assert.strictEqual(isTooFarBehind(0, undefined), false);
	});

	test("an empty log is not a gap", () => {
		assert.strictEqual(isTooFarBehind(1_000, undefined), false);
	});

	test("caught up: the oldest surviving entry is older than our watermark", () => {
		assert.strictEqual(isTooFarBehind(9_000, 1_000), false);
	});

	test("behind but inside the window: still fine", () => {
		// We stopped at 5000; entries from 1000 onward are all still there.
		assert.strictEqual(isTooFarBehind(5_000, 1_000), false);
	});

	test("the oldest surviving entry is exactly our watermark", () => {
		// We already applied that entry, so there is nothing between it and us.
		assert.strictEqual(isTooFarBehind(1_000, 1_000), false);
	});

	test("gap: entries we needed were deleted while we were away", () => {
		// We stopped at 1000, but the oldest entry left is 5000 - everything in
		// between aged out and nothing will ever deliver it.
		assert.strictEqual(isTooFarBehind(1_000, 5_000), true);
	});

	test("off by one either side of the boundary", () => {
		assert.strictEqual(isTooFarBehind(1_000, 1_001), true);
		assert.strictEqual(isTooFarBehind(1_001, 1_000), false);
	});
});

describe("ttlAtMsFor", () => {
	test("stamps the retention window past the publish time", () => {
		assert.strictEqual(ttlAtMsFor(1_000), 1_000 + RETENTION_MS);
	});

	test("the window is long enough to cover a normal absence", () => {
		// A guard on the constant itself: a retention window shorter than a couple
		// of weeks would start locking out ordinary players who take a break.
		assert.ok(RETENTION_MS >= 14 * 24 * 60 * 60 * 1000);
	});
});
