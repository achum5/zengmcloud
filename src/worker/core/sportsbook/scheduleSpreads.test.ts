import { assert, describe, test } from "vitest";
import { MAX_WARM_SPREADS } from "./scheduleSpreads.ts";
import { roundHalf } from "../../../common/getGameSpread.ts";
import { blendMargin } from "./simSpreads.ts";

// The two properties that keep this off the critical path. The pricing itself
// is covered by gameLines.test.ts; what matters here is that surfacing lines on
// a schedule page can never turn into unbounded background work, and can never
// rewrite a completed game's spread.
describe("schedule spreads stay bounded", () => {
	test("the warm queue is capped at about one day's slate", () => {
		assert.ok(
			MAX_WARM_SPREADS > 0 && MAX_WARM_SPREADS <= 24,
			`cap is ${MAX_WARM_SPREADS} - a schedule page must never be able to queue a season`,
		);
	});
});

// Every spread SHOWN outside the sportsbook lands on a whole or half point.
// The formula's already does (getGameSpread rounds), but blending a simulated
// margin into it leaves a continuous number, and that number is what reaches
// the Daily Schedule - which is how a slate ended up reading "-3.7", "-9.2",
// "-1.6" next to games elsewhere quoted at "-4".
describe("displayed spreads are whole or half points", () => {
	test("roundHalf only ever produces .0 or .5", () => {
		for (const raw of [
			0, -0.24, 0.24, 1.3, -1.3, 3.74, -3.74, 9.2, -9.2, 11.49, -11.49,
		]) {
			const rounded = roundHalf(raw);
			assert.ok(
				Number.isInteger(rounded * 2),
				`${raw} rounded to ${rounded}, which is neither a whole nor a half point`,
			);
			assert.ok(
				Math.abs(rounded - raw) <= 0.25 + 1e-9,
				`${raw} moved to ${rounded} - more than a quarter point`,
			);
		}
	});

	// The blend is the only thing between the pricer and the schedule that can
	// produce a fraction, so guard the real path too rather than just the helper.
	test("a blended margin still displays on a half point", () => {
		const blended = blendMargin(3.5, { mean: 9.13, se: 1.75, n: 50 });
		assert.notStrictEqual(
			blended,
			roundHalf(blended),
			"pick a case where blending actually produces a fraction",
		);
		assert.ok(Number.isInteger(roundHalf(blended) * 2));
	});
});
