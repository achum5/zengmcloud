import { assert, describe, test } from "vitest";
import {
	AGE_LEAN,
	DRAFT_FIT_CEILING,
	DRAFT_FIT_FLOOR,
	DRAFT_NEED_WEIGHT,
	draftAgeMultiplier,
	draftNeedMultiplier,
	repeatPositionMultiplier,
	scoreProspect,
	UPSIDE_LEAN,
	upsideMultiplier,
	type DraftProspect,
} from "./draftBoard.ts";
import type { TradePosture } from "../trade/tradePosture.ts";

const TIERS = ["teardown", "seller", "fringe", "buyer", "allIn"] as const;

const prospect = (o: Partial<DraftProspect> = {}): DraftProspect => ({
	pid: 1,
	ovr: 45,
	pot: 60,
	value: 50,
	age: 21,
	pos: "G",
	...o,
});

const posture = (o: Partial<TradePosture> = {}): TradePosture =>
	({
		tid: 0,
		tier: "fringe",
		needs: [],
		surpluses: [],
		targetPos: undefined,
		...o,
	}) as TradePosture;

describe("upside versus readiness", () => {
	// The one thing every draft argument is about, and the thing `value` averages
	// away: a raw 19-year-old and a finished 22-year-old can have the same value
	// and be completely different assets.
	test("a rebuild wants the ceiling and a contender wants the floor", () => {
		const raw = prospect({ ovr: 35, pot: 75 });
		assert.isAbove(upsideMultiplier("teardown", raw), 1);
		assert.isAbove(upsideMultiplier("seller", raw), 1);
		assert.isBelow(upsideMultiplier("buyer", raw), 1);
		assert.isBelow(upsideMultiplier("allIn", raw), 1);
	});

	test("the lean runs monotonically from teardown to all-in", () => {
		const raw = prospect({ ovr: 35, pot: 75 });
		const scores = TIERS.map((tier) => upsideMultiplier(tier, raw));
		for (let i = 1; i < scores.length; i++) {
			assert.isBelow(scores[i]!, scores[i - 1]!, TIERS[i]);
		}
	});

	// A finished prospect is a finished prospect for everyone - there is no
	// ceiling left to disagree about.
	test("nobody disagrees about a player with no gap left", () => {
		const finished = prospect({ ovr: 60, pot: 60 });
		for (const tier of TIERS) {
			assert.strictEqual(upsideMultiplier(tier, finished), 1, tier);
		}
	});

	test("a lower potential than overall is not negative upside", () => {
		for (const tier of TIERS) {
			assert.strictEqual(
				upsideMultiplier(tier, prospect({ ovr: 60, pot: 50 })),
				1,
				tier,
			);
		}
	});
});

describe("age inside the draft's narrow band", () => {
	// Free agency's age buckets put every prospect in one group. Within a class
	// three years is the difference between a project and a finished player.
	test("a rebuild pays for youth and a contender does not", () => {
		assert.isAbove(draftAgeMultiplier("teardown", 19), 1);
		assert.isBelow(draftAgeMultiplier("teardown", 23), 1);
		assert.isBelow(draftAgeMultiplier("allIn", 19), 1);
		assert.isAbove(draftAgeMultiplier("allIn", 23), 1);
	});

	test("the pivot age is neutral for everyone", () => {
		for (const tier of TIERS) {
			assert.strictEqual(draftAgeMultiplier(tier, 21), 1, tier);
		}
	});

	// An imported league can hand in anything; the lean must stay a lean.
	test("an absurd age cannot dominate the board", () => {
		for (const tier of TIERS) {
			for (const age of [-5, 0, 60, Number.NaN]) {
				const m = draftAgeMultiplier(tier, age);
				assert.isAtLeast(
					m,
					1 - 4 * Math.abs(AGE_LEAN[tier]) - 1e-9,
					`${tier}/${age}`,
				);
				assert.isAtMost(
					m,
					1 + 4 * Math.abs(AGE_LEAN[tier]) + 1e-9,
					`${tier}/${age}`,
				);
			}
		}
	});
});

describe("need, and what the roster already has", () => {
	// Best player available, then lean. A pick is an asset you hold for years;
	// the hole you have today is not the hole you have when he is good.
	test("need moves the board less than it moves free agency", () => {
		const need = posture({ needs: [{ pos: "C", severity: 40 }] });
		const bump = draftNeedMultiplier(need, "C") - 1;
		// The free-agency version of this same hole would be the full 0.35.
		assert.approximately(bump, 0.35 * DRAFT_NEED_WEIGHT, 1e-9);
		assert.isBelow(bump, 0.35);
	});

	test("a position the team is stacked at is worth less", () => {
		const deep = posture({ surpluses: [{ pos: "G", depth: 3 }] });
		assert.isBelow(draftNeedMultiplier(deep, "G"), 1);
		assert.strictEqual(draftNeedMultiplier(deep, "C"), 1);
	});

	// The mistake no human has ever made: taking a centre at pick 4 and another
	// at 34, because the posture was built before the draft started.
	test("taking one there already makes the next one worth less", () => {
		assert.strictEqual(repeatPositionMultiplier(0, "C"), 1);
		assert.isBelow(repeatPositionMultiplier(1, "C"), 1);
		assert.isBelow(
			repeatPositionMultiplier(2, "C"),
			repeatPositionMultiplier(1, "C"),
		);
	});

	test("it stops short of removing a position from the board", () => {
		assert.isAtLeast(repeatPositionMultiplier(99, "C"), 0.6);
	});
});

describe("the board itself", () => {
	// Same lesson as free agency's fit band. These multiply, so without a clamp
	// an old-for-the-draft, position-blocked, already-drafted-there prospect on a
	// win-now team sorts below players nobody would take at all.
	test("no combination can bury a prospect", () => {
		const worst = prospect({ ovr: 30, pot: 78, age: 25, pos: "G", value: 60 });
		const blocked = posture({
			tier: "allIn",
			surpluses: [{ pos: "G", depth: 5 }],
		});
		const score = scoreProspect({
			p: worst,
			posture: blocked,
			alreadyDraftedAtPos: 4,
		});
		assert.isAtLeast(score, 60 * DRAFT_FIT_FLOOR - 1e-9);
	});

	test("nor inflate one past the ceiling", () => {
		const best = prospect({ ovr: 35, pot: 80, age: 18, pos: "C", value: 55 });
		const hungry = posture({
			tier: "teardown",
			needs: [{ pos: "C", severity: 60 }],
		});
		const score = scoreProspect({ p: best, posture: hungry });
		assert.isAtMost(score, 55 * DRAFT_FIT_CEILING + 1e-9);
	});

	test("the score is always usable as a selection weight", () => {
		for (const value of [0, -3, Number.NaN]) {
			const score = scoreProspect({
				p: prospect({ value }),
				posture: posture(),
			});
			assert.isAbove(score, 0, `value ${value}`);
		}
	});

	// THE CASE THE WHOLE MODULE EXISTS FOR. Two prospects the league values
	// identically; the right answer differs by team, and used to not.
	test("two teams with the same board disagree about the same class", () => {
		const project = prospect({ pid: 1, ovr: 34, pot: 78, age: 19, value: 52 });
		const readyNow = prospect({ pid: 2, ovr: 52, pot: 56, age: 23, value: 52 });

		const rebuild = posture({ tier: "teardown" });
		const contender = posture({ tier: "allIn" });

		assert.isAbove(
			scoreProspect({ p: project, posture: rebuild }),
			scoreProspect({ p: readyNow, posture: rebuild }),
			"a teardown should take the ceiling",
		);
		assert.isAbove(
			scoreProspect({ p: readyNow, posture: contender }),
			scoreProspect({ p: project, posture: contender }),
			"a win-now team should take the player who can play",
		);
	});

	// And it is still a draft board, not a needs list: a clearly better player
	// is taken even at a position the team is deep at.
	test("a big enough talent gap beats any lean", () => {
		const star = prospect({ pid: 1, value: 70, pos: "G", ovr: 55, pot: 75 });
		const filler = prospect({ pid: 2, value: 48, pos: "C", ovr: 40, pot: 55 });
		const deepAtGuard = posture({
			tier: "allIn",
			needs: [{ pos: "C", severity: 40 }],
			surpluses: [{ pos: "G", depth: 4 }],
		});
		assert.isAbove(
			scoreProspect({ p: star, posture: deepAtGuard, alreadyDraftedAtPos: 2 }),
			scoreProspect({ p: filler, posture: deepAtGuard }),
		);
	});
});

describe("the leans are calibrated against each other", () => {
	// Every lean is a multiplier on value, so their combined range is what
	// decides how far a team may stray from the consensus board. If that band
	// ever grew past the clamp, the clamp - not the strategy - would be doing
	// the deciding.
	test("no single lean is large enough to reorder the class on its own", () => {
		for (const tier of TIERS) {
			assert.isAtMost(Math.abs(UPSIDE_LEAN[tier]), 0.6, tier);
			assert.isAtMost(Math.abs(AGE_LEAN[tier] * 4), 0.2, tier);
		}
	});
});
