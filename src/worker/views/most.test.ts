import { assert, describe, test } from "vitest";
import { coarsenMostForDisplay } from "./most.ts";

// Frivolities carry their headline number on `most`, which is an ATTR -
// playersPlus copies it straight through, so unlike the Ovr column beside it
// nothing here ever met the display coarsening. In a league hiding the ones
// digit these were the last place on the site printing exact 0-100 ratings, and
// Best Progs was the worst of them: a difference of two ovrs leaks more than
// either ovr does, because "+14" is only reachable from precise numbers.
describe("coarsening the frivolities' rating columns", () => {
	describe("progs", () => {
		// The difference of the two DISPLAYED ovrs, the same rule the dovr/dpot
		// columns use - not the true difference floored, which would still leak.
		test("a gain inside one decade shows as no change", () => {
			const out = coarsenMostForDisplay(
				{ value: 2, extra: { progFrom: 56 } },
				"progs",
			);
			// 56 -> 58 is 5 -> 5.
			assert.strictEqual(out.value, 0);
		});

		test("a gain across a decade shows as one", () => {
			const out = coarsenMostForDisplay(
				{ value: 4, extra: { progFrom: 58 } },
				"progs",
			);
			// 58 -> 62 is 5 -> 6.
			assert.strictEqual(out.value, 1);
		});

		test("the big prog that started this never shows two digits", () => {
			const out = coarsenMostForDisplay(
				{ value: 14, extra: { progFrom: 48 } },
				"progs",
			);
			// 48 -> 62 is 4 -> 6.
			assert.strictEqual(out.value, 2);
		});

		test("career progs coarsen the same way", () => {
			const out = coarsenMostForDisplay(
				{ value: 23, extra: { progFrom: 41 } },
				"progs_career",
			);
			// 41 -> 64 is 4 -> 6.
			assert.strictEqual(out.value, 2);
		});

		// Without the endpoint there is nothing to difference, so leave it rather
		// than invent a number.
		test("leaves the value alone when the endpoint is missing", () => {
			const out = coarsenMostForDisplay({ value: 14 }, "progs");
			assert.strictEqual(out.value, 14);
		});
	});

	// The injury drop is stored as a bare number, so there are no endpoints to
	// difference - the magnitude itself goes on the 0-10 scale.
	test("an injury's ovr drop loses its ones digit", () => {
		assert.strictEqual(
			coarsenMostForDisplay({ value: 17 }, "worst_injuries").value,
			1,
		);
		assert.strictEqual(
			coarsenMostForDisplay({ value: 24 }, "worst_injuries").value,
			2,
		);
	});

	test("plain ovrs on extra are floored to the tens digit", () => {
		for (const [type, key] of [
			["oldest", "ovr"],
			["oldest_peaks", "ovr"],
			["youngest_peaks", "ovr"],
			["oldest_mvp", "ovr"],
			["youngest_mvp", "ovr"],
			["rookies", "rookieOvr"],
		] as const) {
			const out = coarsenMostForDisplay(
				{ value: 38, extra: { [key]: 67, season: 2004 } },
				type,
			);
			assert.strictEqual(out.extra?.[key], 6, `${type}.${key}`);
			// Everything that isn't a rating is left alone - the Age in `value`
			// here, and the season beside it.
			assert.strictEqual(out.value, 38, `${type} value`);
			assert.strictEqual(out.extra?.season, 2004, `${type} season`);
		}
	});

	test("lists with no rating column are untouched", () => {
		const most = { value: 12345, extra: { tid: 3, season: 2004 } };
		assert.strictEqual(coarsenMostForDisplay(most, "earnings"), most);
	});

	// The caller reassigns the result, but these objects are shared with the
	// player rows the view returns, so mutating in place would corrupt them.
	test("does not mutate its input", () => {
		const extra = { progFrom: 48 };
		const most = { value: 14, extra };
		const out = coarsenMostForDisplay(most, "progs");
		assert.strictEqual(most.value, 14);
		assert.notStrictEqual(out, most);
	});
});
