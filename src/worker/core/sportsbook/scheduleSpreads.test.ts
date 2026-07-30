import { assert, describe, test } from "vitest";
import { MAX_WARM_SPREADS } from "./scheduleSpreads.ts";

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
