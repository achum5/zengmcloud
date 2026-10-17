import { assert, test } from "vitest";
import { PHASE } from "./constants.ts";
import { tradesBlocked } from "./tradesBlocked.ts";

test("trading shuts at the deadline unless God Mode reopened it", () => {
	assert.isFalse(tradesBlocked(PHASE.REGULAR_SEASON, false));
	assert.isTrue(tradesBlocked(PHASE.AFTER_TRADE_DEADLINE, false));
	assert.isTrue(tradesBlocked(PHASE.AFTER_TRADE_DEADLINE, undefined));
	assert.isFalse(tradesBlocked(PHASE.AFTER_TRADE_DEADLINE, true));
});

test("reopening never opens the playoffs", () => {
	assert.isTrue(tradesBlocked(PHASE.PLAYOFFS, false));
	assert.isTrue(tradesBlocked(PHASE.PLAYOFFS, true));
});

test("the offseason is untouched", () => {
	for (const phase of [
		PHASE.DRAFT_LOTTERY,
		PHASE.DRAFT,
		PHASE.AFTER_DRAFT,
		PHASE.RESIGN_PLAYERS,
		PHASE.FREE_AGENCY,
		PHASE.PRESEASON,
	]) {
		assert.isFalse(tradesBlocked(phase, false));
	}
});
