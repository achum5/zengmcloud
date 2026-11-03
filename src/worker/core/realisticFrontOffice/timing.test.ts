import { assert, test } from "vitest";
import {
	deadlineViewFor,
	measuredStrengthUncertainty,
	teamOutlookSettings,
} from "./outlook.ts";
import {
	buildDeadlineView,
	prefersWaiting,
	stillOnOffer,
	waitingValues,
	type DeadlineView,
} from "./timing.ts";

// Title chances that rise steeply with playoff strength and barely with the
// regular season's.
const view = (overrides: Partial<DeadlineView> = {}): DeadlineView => ({
	daysToDeadline: 60,
	daysLeft: 120,
	revealed: 2.5,
	curve: { a: -4, regular: 0.05, playoff: 0.45 },
	...overrides,
});

test("less of today's deal is on offer the longer a team waits", () => {
	assert.strictEqual(stillOnOffer(0), 1);
	assert.isBelow(stillOnOffer(14), stillOnOffer(7));
	assert.isBelow(stillOnOffer(42), stillOnOffer(14));
	assert.strictEqual(stillOnOffer(100), stillOnOffer(42));
});

test("a deal for later seasons is made now", () => {
	// Nothing changes this season, so there is nothing to learn by waiting, and
	// the deal may be gone.
	assert.isFalse(
		prefersWaiting({ view: view(), before: -3, after: -3, later: 0.02 }),
	);
});

test("a clearly good deal for a contender is made now", () => {
	assert.isFalse(
		prefersWaiting({ view: view(), before: 4, after: 5, later: -0.05 }),
	);
});

test("a contender with a doubtful deal waits to see how good it is", () => {
	// Barely worth it on average, and not at all for some of the teams it might
	// turn out to be.
	assert.isTrue(
		prefersWaiting({ view: view(), before: 4, after: 5, later: -0.06 }),
	);
});

test("nobody waits once the deadline is here", () => {
	assert.isFalse(
		prefersWaiting({
			view: view({ daysToDeadline: 0, revealed: 0 }),
			before: 4,
			after: 5,
			later: -0.06,
		}),
	);
});

test("by the measured read, a team learns less about itself before the deadline", () => {
	const built = buildDeadlineView({
		teams: [6, 2, -2, -6].map((strength, tid) => ({
			tid,
			cid: tid % 2,
			strength,
			won: 20,
			lost: 20,
			gamesRemaining: 42,
		})),
		numGamesPlayoffSeries: [7],
		playIn: false,
		byConference: false,
		uncertaintyNow: 1.5 + 2 * (1 - 0.3),
		uncertaintyAtDeadline: 1.5 + 2 * (1 - 0.6),
		measuredNow: measuredStrengthUncertainty(0, 0.3),
		measuredAtDeadline: measuredStrengthUncertainty(0, 0.6),
		daysToDeadline: 40,
		daysLeft: 100,
		seed: 1,
	});
	assert.isBelow(built.revealedMeasured!, 0.6);
	assert.isAbove(built.revealed, 1);

	teamOutlookSettings.set(1, { measuredDeadline: true });
	try {
		const outlook = { deadline: built };
		assert.strictEqual(deadlineViewFor(outlook, 0)!.revealed, built.revealed);
		assert.strictEqual(
			deadlineViewFor(outlook, 1)!.revealed,
			built.revealedMeasured,
		);
		// The same deal, waited on less by the team that expects to learn less.
		const deal = { before: 4, after: 5, later: -0.06 };
		const waits = (tid: number) => {
			const { now, wait } = waitingValues({
				view: deadlineViewFor(outlook, tid)!,
				...deal,
			});
			return wait - now;
		};
		assert.isBelow(waits(1), waits(0));
	} finally {
		teamOutlookSettings.clear();
	}
});
