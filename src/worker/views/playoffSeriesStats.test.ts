import { assert, describe, test } from "vitest";
import { roundFromSeriesKey, seriesKey } from "./playoffSeriesStats.ts";

describe("series keys", () => {
	test("a round and its key go both ways", () => {
		for (const round of [0, 1, 2, 3, 6]) {
			assert.strictEqual(roundFromSeriesKey(seriesKey(round)), round);
		}
		assert.strictEqual(seriesKey(0), "r1");
	});

	test("anything else is not a series", () => {
		for (const key of [
			undefined,
			"playoffs",
			"regularSeason",
			"combined",
			"r",
			"rx",
			"r1a",
		]) {
			assert.strictEqual(roundFromSeriesKey(key), undefined);
		}
	});
});
