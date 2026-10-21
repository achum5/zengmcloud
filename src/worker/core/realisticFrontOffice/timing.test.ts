import { assert, test } from "vitest";
import { prefersWaiting, type DeadlineView } from "./timing.ts";

// Title chances that rise steeply with playoff strength and barely with the
// regular season's.
const view = (overrides: Partial<DeadlineView> = {}): DeadlineView => ({
	daysToDeadline: 60,
	daysLeft: 120,
	revealed: 2.5,
	curve: { a: -4, regular: 0.05, playoff: 0.45 },
	playerTradeRate: 0,
	...overrides,
});

test("a deal for later seasons is made now", () => {
	// Nothing changes this season, so there is nothing to learn by waiting, and
	// any chance of losing the deal makes now better.
	assert.isFalse(
		prefersWaiting({
			view: view({ playerTradeRate: 0.001 }),
			before: -3,
			after: -3,
			later: 0.02,
			numPlayers: 2,
		}),
	);
});

test("a contender with a doubtful deal waits to see how good it is", () => {
	// Worth a little now on average, but some of the teams it might turn out to
	// be would not want it.
	assert.isTrue(
		prefersWaiting({
			view: view(),
			before: 4,
			after: 5,
			later: -0.05,
			numPlayers: 2,
		}),
	);
});

test("nobody waits once the deadline is here", () => {
	assert.isFalse(
		prefersWaiting({
			view: view({ daysToDeadline: 0, revealed: 0 }),
			before: 4,
			after: 5,
			later: -0.05,
			numPlayers: 2,
		}),
	);
});

test("a deal likely to be gone by the deadline is made now", () => {
	assert.isFalse(
		prefersWaiting({
			view: view({ playerTradeRate: 0.02 }),
			before: 4,
			after: 5,
			later: -0.05,
			numPlayers: 2,
		}),
	);
});
