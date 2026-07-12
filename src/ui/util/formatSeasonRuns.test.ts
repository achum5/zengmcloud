import { assert, describe, test } from "vitest";
import { formatSeasonRuns } from "./formatSeasonRuns.ts";

describe("formatSeasonRuns", () => {
	test("a single season is just the year", () => {
		const r = formatSeasonRuns([2057]);
		assert.strictEqual(r.short, "2057");
		assert.strictEqual(r.single, true);
	});

	test("a contiguous run is a range", () => {
		const r = formatSeasonRuns([2057, 2058]);
		assert.strictEqual(r.short, "2057-2058");
		assert.strictEqual(r.single, true);
	});

	test("gaps collapse to an N-seasons label with a full breakdown", () => {
		const r = formatSeasonRuns([2057, 2058, 2059, 2067, 2069]);
		assert.strictEqual(r.short, "5 seasons");
		assert.strictEqual(r.full, "2057-2059, 2067, 2069");
		assert.strictEqual(r.single, false);
	});

	test("unsorted, duplicate input is normalized", () => {
		const r = formatSeasonRuns([2059, 2057, 2058, 2058]);
		assert.strictEqual(r.short, "2057-2059");
		assert.strictEqual(r.single, true);
	});

	test("empty input is blank", () => {
		const r = formatSeasonRuns([]);
		assert.strictEqual(r.short, "");
	});
});
