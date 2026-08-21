import { assert, describe, test } from "vitest";
import {
	CHASE_RATIO,
	MIN_SLOTS_TO_CHASE,
	shouldChase,
} from "./draftDayTrades.ts";

describe("when a team pays to move up", () => {
	test("a clear board gap is worth chasing", () => {
		assert.ok(
			shouldChase({
				topScore: 70,
				fallbackScore: 45,
				slotsUntilOwnPick: MIN_SLOTS_TO_CHASE + 2,
			}),
		);
	});

	test("a marginal preference is not", () => {
		assert.notOk(
			shouldChase({
				topScore: 60,
				fallbackScore: 55,
				slotsUntilOwnPick: 10,
			}),
		);
	});

	test("close enough to wait means wait", () => {
		assert.notOk(
			shouldChase({
				topScore: 100,
				fallbackScore: 10,
				slotsUntilOwnPick: MIN_SLOTS_TO_CHASE - 1,
			}),
		);
	});

	test("the ratio is the published constant, exactly at the line", () => {
		assert.ok(
			shouldChase({
				topScore: CHASE_RATIO * 50,
				fallbackScore: 50,
				slotsUntilOwnPick: MIN_SLOTS_TO_CHASE,
			}),
		);
		assert.notOk(
			shouldChase({
				topScore: CHASE_RATIO * 50 - 0.001,
				fallbackScore: 50,
				slotsUntilOwnPick: MIN_SLOTS_TO_CHASE,
			}),
		);
	});
});
