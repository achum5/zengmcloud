import { describe, expect, test } from "vitest";
import { HEM_TODAY, shortsFlareFor, shortsHemFor } from "./shortsEra.ts";

// HIS SEASON'S SHORTS (see shortsEra.ts).
describe("shorts by season", () => {
	test("today's now and with no season", () => {
		expect(shortsHemFor(2026)).toBe(HEM_TODAY);
		expect(shortsHemFor(undefined)).toBe(HEM_TODAY);
		expect(shortsFlareFor(HEM_TODAY)).toBe(1);
	});

	test("short in the eighties, to the knee in the two-thousands", () => {
		expect(shortsHemFor(1985)).toBeLessThan(0.5);
		expect(shortsHemFor(1957)).toBeGreaterThan(shortsHemFor(1985));
		expect(shortsHemFor(2005)).toBeGreaterThan(HEM_TODAY);
		expect(shortsFlareFor(shortsHemFor(1985))).toBeLessThan(1);
		expect(shortsFlareFor(shortsHemFor(2005))).toBeGreaterThan(1);
	});

	test("one season to the next, never a jump", () => {
		for (let y = 1946; y < 2030; y++) {
			expect(
				Math.abs(shortsHemFor(y + 1) - shortsHemFor(y)),
			).toBeLessThanOrEqual(0.06);
		}
	});
});
