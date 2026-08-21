import { assert, describe, test } from "vitest";
import {
	ageFitMultiplier,
	commitmentShare,
	COMMITMENT_FLOOR,
	upsideFitMultiplier,
	upsideShare,
	contractRiskMultiplier,
	isPrize,
	MAX_PURSUERS_PER_PRIZE,
	planCapHold,
	positionFitMultiplier,
	PURSUIT_GIVE_UP_DAYS,
	resolveCapHolds,
	FIT_CEILING,
	FIT_FLOOR,
	MAX_RETENTION_OVERPAY,
	RETENTION_MIN_EDGE,
	retentionOverpay,
	scoreFreeAgent,
	shouldLetWalk,
	type FaPlayer,
	signingYears,
} from "./frontOffice.ts";
import type { TradePosture } from "../trade/tradePosture.ts";

const SEASON = 2025;
const MIN = 1000;
const CAP = 100_000;
const MAX = 30_000;

const fa = (overrides: Partial<FaPlayer> = {}): FaPlayer => ({
	pid: 1,
	ovr: 55,
	pot: 60,
	value: 55,
	age: 27,
	pos: "SF",
	amount: 10_000,
	exp: SEASON + 2,
	injuredGames: 0,
	...overrides,
});

const posture = (overrides: Partial<TradePosture> = {}): TradePosture =>
	({
		tid: 0,
		tier: "buyer",
		aggression: 0.5,
		elite: false,
		winp: 0.5,
		ovrRank: 10,
		ovrRankPct: 0.3,
		contention: 0.5,
		avgAge: 26,
		youngCoreCount: 2,
		starGap: false,
		needs: [],
		surpluses: [],
		buildingBlockPids: [],
		shopVeteranPids: [],
		shoppableStar: false,
		cap: {
			payroll: 50_000,
			capSpace: 50_000,
			overCap: false,
			overLuxury: false,
			underFloor: false,
			wantsRelief: false,
			canAbsorb: true,
		},
		lookingFor: {} as TradePosture["lookingFor"],
		...overrides,
	}) as TradePosture;

describe("upside as a multiplier, not a bonus", () => {
	test("upsideShare runs 0 to 1 and clamps", () => {
		assert.strictEqual(upsideShare({ ovr: 60, pot: 60 }), 0);
		assert.strictEqual(upsideShare({ ovr: 40, pot: 70 }), 1);
		assert.strictEqual(upsideShare({ ovr: 40, pot: 90 }), 1);
		assert.strictEqual(upsideShare({ ovr: 60, pot: 50 }), 0);
	});

	test("a rebuild pays a premium for a raw player, a win-now team discounts him", () => {
		const raw = { ovr: 45, pot: 65 };
		assert.isAbove(upsideFitMultiplier("teardown", raw), 1);
		assert.isBelow(upsideFitMultiplier("allIn", raw), 1);
		assert.strictEqual(upsideFitMultiplier("fringe", raw), 1);
	});

	// THE BUG THE MULTIPLIER FIXES. The tilt used to be additive, outside the
	// fit clamp, so a rebuilder scored a 46-value prospect above a 72-value
	// star and free agency - which signs the first acceptable name in fit
	// order - gave the star's roster spot to the prospect. No preference gets
	// to outrank a genuinely better player.
	test("a teardown still takes the far better player", () => {
		const prospect = fa({
			pid: 1,
			age: 22,
			ovr: 44,
			pot: 62,
			value: 46,
			amount: 8_000,
		});
		const star = fa({
			pid: 2,
			age: 27,
			ovr: 72,
			pot: 72,
			value: 72,
			amount: 30_000,
		});
		const rebuild = posture({ tier: "teardown" });
		const score = (p: FaPlayer) =>
			scoreFreeAgent({
				p,
				posture: rebuild,
				season: SEASON,
				minContract: MIN,
				maxContract: MAX,
			});
		assert.isAbove(score(star), score(prospect));
	});
});

describe("fit scales with the size of the commitment", () => {
	test("a minimum deal keeps only a sliver of fit, a big deal keeps it all", () => {
		assert.strictEqual(
			commitmentShare({ amount: MIN, minContract: MIN, maxContract: MAX }),
			COMMITMENT_FLOOR,
		);
		assert.strictEqual(
			commitmentShare({
				amount: MAX / 2,
				minContract: MIN,
				maxContract: MAX,
			}),
			1,
		);
		const small = commitmentShare({
			amount: MIN * 2,
			minContract: MIN,
			maxContract: MAX,
		});
		const mid = commitmentShare({
			amount: MAX / 4,
			minContract: MIN,
			maxContract: MAX,
		});
		assert.isAbove(mid, small);
		assert.isAbove(small, COMMITMENT_FLOOR);
	});
});

describe("the structure of the deal follows the plan", () => {
	const base = {
		askedYears: 4,
		amount: 8_000,
		minContract: MIN,
		minLength: 1,
		maxLength: 5,
	};

	test("a seller keeps its veterans on expiring deals", () => {
		assert.strictEqual(signingYears({ ...base, tier: "seller", age: 29 }), 1);
		assert.strictEqual(signingYears({ ...base, tier: "teardown", age: 31 }), 1);
	});

	test("a rebuild locks up a real investment in a young player", () => {
		assert.isAtLeast(
			signingYears({ ...base, tier: "teardown", age: 22, askedYears: 2 }),
			3,
		);
		// A minimum-deal bench kid is not an investment.
		assert.strictEqual(
			signingYears({
				...base,
				tier: "teardown",
				age: 22,
				askedYears: 2,
				amount: MIN,
			}),
			2,
		);
	});

	test("a contender does not anchor itself to decline years", () => {
		assert.strictEqual(signingYears({ ...base, tier: "allIn", age: 33 }), 2);
		assert.strictEqual(
			signingYears({ ...base, tier: "buyer", age: 28 }),
			base.askedYears,
		);
	});

	test("a fringe team takes the ask as it comes", () => {
		assert.strictEqual(
			signingYears({ ...base, tier: "fringe", age: 30 }),
			base.askedYears,
		);
	});

	test("league length rules always win", () => {
		assert.strictEqual(
			signingYears({ ...base, tier: "seller", age: 30, minLength: 2 }),
			2,
		);
	});
});

describe("who a team should want", () => {
	// The most obviously wrong thing the old code did: everyone chased the same
	// player, so a team tearing it down handed multi-year money to a 34-year-old.
	test("a teardown prefers youth and a win-now team prefers the veteran", () => {
		const kid = fa({ pid: 1, age: 22, ovr: 50, pot: 70, value: 55 });
		const vet = fa({ pid: 2, age: 33, ovr: 60, pot: 60, value: 55 });

		const rebuilding = posture({ tier: "teardown" });
		const winNow = posture({ tier: "allIn" });

		const score = (p: FaPlayer, pos: TradePosture) =>
			scoreFreeAgent({
				p,
				posture: pos,
				season: SEASON,
				minContract: MIN,
				maxContract: MAX,
			});

		assert.ok(
			score(kid, rebuilding) > score(vet, rebuilding),
			"a teardown should take the 22-year-old",
		);
		assert.ok(
			score(vet, winNow) > score(kid, winNow),
			"a win-now team should take the proven veteran",
		);
	});

	test("age fit runs the opposite way for a rebuild and a contender", () => {
		assert.ok(
			ageFitMultiplier("teardown", 22) > ageFitMultiplier("teardown", 33),
		);
		assert.ok(ageFitMultiplier("allIn", 30) > ageFitMultiplier("allIn", 20));
	});

	// A team with three All-Star guards signing a fourth was the other one.
	test("a hole is worth more than another body where the team is deep", () => {
		const needsBigs = posture({
			needs: [{ pos: "C", severity: 12 }],
			surpluses: [{ pos: "G", depth: 2 }],
		});
		assert.ok(positionFitMultiplier(needsBigs, "C") > 1);
		assert.ok(positionFitMultiplier(needsBigs, "PG") < 1);

		const guard = fa({ pid: 1, pos: "PG" });
		const center = fa({ pid: 2, pos: "C" });
		const score = (p: FaPlayer) =>
			scoreFreeAgent({
				p,
				posture: needsBigs,
				season: SEASON,
				minContract: MIN,
				maxContract: MAX,
			});
		assert.ok(score(center) > score(guard));
	});

	test("only a win-now team accepts a long deal that ages badly", () => {
		const args = { age: 32, years: 4, amount: 20_000, minContract: MIN };
		assert.ok(
			contractRiskMultiplier({ tier: "allIn", ...args }) >
				contractRiskMultiplier({ tier: "teardown", ...args }),
		);
		// A short deal, or a minimum one, is never penalised.
		assert.strictEqual(
			contractRiskMultiplier({
				tier: "teardown",
				age: 34,
				years: 1,
				amount: 20_000,
				minContract: MIN,
			}),
			1,
		);
		assert.strictEqual(
			contractRiskMultiplier({
				tier: "teardown",
				age: 34,
				years: 4,
				amount: MIN,
				minContract: MIN,
			}),
			1,
		);
	});

	test("a taxpaying also-ran stops adding salary", () => {
		const p = fa({ amount: 20_000 });
		const normal = posture();
		const taxed = posture({
			cap: { ...normal.cap, overLuxury: true, wantsRelief: true },
		});
		assert.ok(
			scoreFreeAgent({
				p,
				posture: taxed,
				season: SEASON,
				minContract: MIN,
				maxContract: MAX,
			}) <
				scoreFreeAgent({
					p,
					posture: normal,
					season: SEASON,
					minContract: MIN,
					maxContract: MAX,
				}),
		);
	});
});

describe("fit tilts the market, it does not delete players from it", () => {
	// Every one of these multipliers is defensible alone. The bug was that they
	// MULTIPLY, and that they point the same way at nearly every team - so the
	// player they gang up on is not merely demoted, he is unemployable league-
	// wide. Over eight simulated seasons that left a 32-year-old 72 ovr without
	// a job while stock BBGM signed him immediately.
	test("the worst possible fit still cannot bury a much better player", () => {
		// Old, expensive, long deal, at a position of surplus, for a team that
		// wants cap relief: every penalty at once.
		const hated = fa({
			pid: 1,
			age: 34,
			ovr: 72,
			pot: 72,
			value: 72,
			pos: "SF",
			amount: 40_000,
			exp: SEASON + 4,
		});
		const p2 = posture({
			tier: "buyer",
			surpluses: [{ pos: "F", depth: 3 }] as any,
			cap: {
				payroll: 90_000,
				capSpace: 10_000,
				overCap: true,
				overLuxury: true,
				underFloor: false,
				wantsRelief: true,
				canAbsorb: false,
			},
		});

		const score = scoreFreeAgent({
			p: hated,
			posture: p2,
			season: SEASON,
			minContract: MIN,
			maxContract: MAX,
		});

		// Unclamped this came out near 0.14 * value. The clamp is the contract.
		assert.ok(
			score >= hated.value * FIT_FLOOR - 0.001,
			`fit drove a ${hated.value}-value player down to ${score.toFixed(1)}, below the ${FIT_FLOOR} floor`,
		);
	});

	test("the best possible fit cannot inflate a player without limit", () => {
		const loved = fa({
			pid: 1,
			age: 23,
			ovr: 50,
			pot: 50,
			value: 50,
			pos: "C",
			amount: MIN,
			exp: SEASON,
		});
		const p2 = posture({
			tier: "teardown",
			needs: [{ pos: "C", severity: 40 }] as any,
			targetPos: "C",
		});

		const score = scoreFreeAgent({
			p: loved,
			posture: p2,
			season: SEASON,
			minContract: MIN,
			maxContract: MAX,
		});

		// pot === ovr here so the additive tier tilt contributes nothing and the
		// ceiling is the only thing under test.
		assert.ok(
			score <= loved.value * FIT_CEILING + 0.001,
			`fit inflated a ${loved.value}-value player to ${score.toFixed(1)}, above the ${FIT_CEILING} ceiling`,
		);
	});

	test("on the last day of free agency the ordering is pure value", () => {
		// A rebuilder normally prefers the prospect: the additive pot tilt plus a
		// youth bonus beats an older, better player. That preference is fine in
		// July and absurd once the music is about to stop, so it has to unwind -
		// and unwind COMPLETELY, because "order by value" is precisely stock
		// BBGM's order, which is what makes market clearing structural.
		const prospect = fa({ pid: 1, age: 20, ovr: 44, pot: 70, value: 44 });
		const better = fa({
			pid: 2,
			age: 31,
			ovr: 62,
			pot: 62,
			value: 62,
			amount: 20_000,
			exp: SEASON + 3,
		});
		const rebuild = posture({ tier: "teardown" });

		const early = (p: FaPlayer) =>
			scoreFreeAgent({
				p,
				posture: rebuild,
				season: SEASON,
				minContract: MIN,
				maxContract: MAX,
				daysLeft: PURSUIT_GIVE_UP_DAYS,
			});
		assert.ok(
			early(prospect) > early(better),
			"a rebuilder should prefer the prospect while it still has time",
		);

		const last = (p: FaPlayer) =>
			scoreFreeAgent({
				p,
				posture: rebuild,
				season: SEASON,
				minContract: MIN,
				maxContract: MAX,
				daysLeft: 0,
			});
		assert.strictEqual(last(prospect), prospect.value);
		assert.strictEqual(last(better), better.value);
		assert.ok(
			last(better) > last(prospect),
			"with the market closing, the better player has to come out on top",
		);
	});

	test("outside free agency there is no countdown and fit applies in full", () => {
		const vet = fa({ age: 34, ovr: 62, pot: 62, value: 62, exp: SEASON + 4 });
		const rebuild = posture({ tier: "teardown" });
		const withoutDays = scoreFreeAgent({
			p: vet,
			posture: rebuild,
			season: SEASON,
			minContract: MIN,
			maxContract: MAX,
		});
		const plentyOfDays = scoreFreeAgent({
			p: vet,
			posture: rebuild,
			season: SEASON,
			minContract: MIN,
			maxContract: MAX,
			daysLeft: PURSUIT_GIVE_UP_DAYS + 10,
		});
		assert.strictEqual(withoutDays, plentyOfDays);
	});
});

describe("clearing cap space for a big free agent", () => {
	const star = fa({ pid: 99, ovr: 70, value: 70, amount: 30_000 });

	const plan = (
		overrides: Parameters<typeof planCapHold>[0] extends never
			? never
			: Partial<Parameters<typeof planCapHold>[0]>,
	) =>
		planCapHold({
			posture: posture(),
			prizes: [star],
			payroll: 50_000,
			salaryCap: CAP,
			salaryCapType: "soft",
			daysLeft: 30,
			season: SEASON,
			minContract: MIN,
			maxContract: MAX,
			...overrides,
		});

	test("a team with room earmarks it and can still spend the rest", () => {
		const hold = plan({});
		assert.strictEqual(hold?.pid, 99);
		// 30,000 asked, discounted by the patience factor, held back from the cap.
		assert.ok(hold!.spendCeiling < CAP);
		assert.ok(hold!.spendCeiling > CAP - 30_000);
	});

	// The whole point of holding: the price falls day by day, so a team should
	// wait on someone it cannot QUITE afford yet.
	test("a team waits on a player it cannot afford today but will", () => {
		assert.ok(plan({ payroll: CAP - 26_000 }) !== undefined);
		// But not on one it could never fit.
		assert.strictEqual(plan({ payroll: CAP - 5_000 }), undefined);
	});

	test("a teardown never sits on its space", () => {
		assert.strictEqual(
			plan({ posture: posture({ tier: "teardown" }) }),
			undefined,
		);
	});

	// A front office that has missed pivots rather than carrying the space into
	// the season - and this is what guarantees holds can't stall a market.
	test("the hold is abandoned as free agency runs out", () => {
		assert.ok(plan({ daysLeft: PURSUIT_GIVE_UP_DAYS }) !== undefined);
		assert.strictEqual(plan({ daysLeft: PURSUIT_GIVE_UP_DAYS - 1 }), undefined);
	});

	test("with no salary cap there is nothing to clear", () => {
		assert.strictEqual(plan({ salaryCapType: "none" }), undefined);
	});

	test("a marquee free agent is a genuine star at a real price", () => {
		assert.ok(isPrize({ p: star, starOvr: 65, minContract: MIN }));
		// Good but not a star.
		assert.strictEqual(
			isPrize({
				p: fa({ ovr: 60, amount: 30_000 }),
				starOvr: 65,
				minContract: MIN,
			}),
			false,
		);
		// A star on a minimum deal needs no space cleared for him.
		assert.strictEqual(
			isPrize({
				p: fa({ ovr: 70, amount: MIN * 2 }),
				starOvr: 65,
				minContract: MIN,
			}),
			false,
		);
	});
});

// Without a cap on pursuers, one tempting free agent freezes half the league's
// payroll and the market stops moving.
describe("only the most credible suitors get to wait", () => {
	test("the field is trimmed to the top few, per player", () => {
		const hold = { pid: 99, spendCeiling: 70_000 };
		const wanted = [0, 1, 2, 3, 4, 5].map((tid) => ({
			tid,
			hold,
			score: 100 - tid,
		}));
		const resolved = resolveCapHolds(wanted);
		assert.strictEqual(resolved.size, MAX_PURSUERS_PER_PRIZE);
		assert.deepStrictEqual([...resolved.keys()].sort(), [0, 1, 2]);
	});

	test("each player is counted separately", () => {
		const resolved = resolveCapHolds([
			{ tid: 0, hold: { pid: 1, spendCeiling: 1 }, score: 10 },
			{ tid: 1, hold: { pid: 2, spendCeiling: 1 }, score: 10 },
		]);
		assert.strictEqual(resolved.size, 2);
	});

	test("ties break on tid so a sim replays the same way", () => {
		const hold = { pid: 99, spendCeiling: 70_000 };
		const resolved = resolveCapHolds(
			[5, 2, 9].map((tid) => ({ tid, hold, score: 50 })),
			2,
		);
		assert.deepStrictEqual(
			[...resolved.keys()].sort((a, b) => a - b),
			[2, 5],
		);
	});
});

describe("letting a player walk", () => {
	const base = {
		age: 31,
		amount: MIN * 6,
		years: 3,
		isStar: false,
		minContract: MIN,
		maxContract: MAX,
	};

	test("a teardown lets an expensive veteran go", () => {
		assert.ok(shouldLetWalk({ tier: "teardown", ...base }));
	});

	test("a contender keeps him", () => {
		assert.strictEqual(shouldLetWalk({ tier: "allIn", ...base }), false);
		assert.strictEqual(shouldLetWalk({ tier: "buyer", ...base }), false);
	});

	// You keep a star and trade him; you don't hand him to someone for nothing.
	test("a star is never let go for nothing", () => {
		assert.strictEqual(
			shouldLetWalk({ tier: "teardown", ...base, isStar: true }),
			false,
		);
	});

	test("cheap or short deals are never the problem", () => {
		assert.strictEqual(
			shouldLetWalk({ tier: "teardown", ...base, amount: MIN * 2 }),
			false,
		);
		assert.strictEqual(
			shouldLetWalk({ tier: "teardown", ...base, years: 1 }),
			false,
		);
	});

	// "Star" is measured LEAGUE-wide - roughly the best player on an average
	// team - so the clubs most likely to be tearing down are precisely the ones
	// with nobody who qualifies. Left there, the exemption protected exactly the
	// teams that did not need protecting, and the worst team in the league would
	// sell off its entire rotation for nothing and never climb back.
	test("a bad team's own best players are not sold off for nothing", () => {
		const args = {
			tier: "teardown" as const,
			age: 30,
			amount: MIN * 8,
			years: 3,
			minContract: MIN,
			maxContract: MAX,
		};

		// Nobody special by league standards - this is the case that used to walk.
		assert.strictEqual(shouldLetWalk({ ...args, isStar: false }), true);

		// Same player, except he is one of this team's few best.
		assert.strictEqual(
			shouldLetWalk({ ...args, isStar: false, isCore: true }),
			false,
		);
	});

	test("being core does not resurrect a player nobody would keep anyway", () => {
		// Core or not, a cheap short deal was never the problem this solves, and a
		// contender was never letting him go in the first place. isCore must not
		// change answers that were already correct.
		assert.strictEqual(
			shouldLetWalk({
				tier: "teardown",
				age: 30,
				amount: MIN,
				years: 1,
				isStar: false,
				isCore: true,
				minContract: MIN,
			}),
			false,
		);
		assert.strictEqual(
			shouldLetWalk({
				tier: "allIn",
				age: 30,
				amount: MIN * 8,
				years: 3,
				isStar: false,
				isCore: false,
				minContract: MIN,
			}),
			false,
		);
	});

	// Money could not previously move mood at all, so a player who wanted out was
	// gone at any price. These cover the other half: what a team will PAY to stop
	// that happening.
	test("a rebuild never bids to keep anyone", () => {
		for (const tier of ["teardown", "seller"] as const) {
			assert.strictEqual(
				retentionOverpay({
					tier,
					rosterRank: 0,
					isStar: true,
					age: 27,
					wantsRelief: false,
					ovr: 70,
					replacementOvr: 45,
				}),
				1,
				`a ${tier} should not pay a premium to keep a player it is content to lose`,
			);
		}
	});

	test("a contender pays most for the players it is built around", () => {
		const base = {
			rosterRank: 0,
			isStar: true,
			age: 27,
			wantsRelief: false,
			ovr: 70,
			replacementOvr: 45,
		};
		const allIn = retentionOverpay({ ...base, tier: "allIn" });
		const buyer = retentionOverpay({ ...base, tier: "buyer" });
		assert.ok(allIn > buyer, "a win-now team should outbid a patient one");
		assert.ok(buyer > 1, "a buyer should be willing to pay something");
		assert.ok(
			allIn <= MAX_RETENTION_OVERPAY,
			`overpay ran past its own ceiling: ${allIn}`,
		);
	});

	test("the end of the bench is not worth a premium", () => {
		assert.strictEqual(
			retentionOverpay({
				tier: "allIn",
				rosterRank: 11,
				isStar: false,
				age: 27,
				wantsRelief: false,
				ovr: 70,
				replacementOvr: 45,
			}),
			1,
		);
	});

	// The check the first version of this forgot, and the one that decides
	// whether the whole idea helps or hurts.
	test("a replaceable player is let go however highly his team rates him", () => {
		const args = {
			tier: "allIn" as const,
			rosterRank: 0,
			isStar: false,
			age: 27,
			wantsRelief: false,
			replacementOvr: 50,
		};
		assert.strictEqual(
			retentionOverpay({ ...args, ovr: 50 + RETENTION_MIN_EDGE - 1 }),
			1,
			"paying over the odds for someone the market can replace is just spending more",
		);
		assert.ok(
			retentionOverpay({ ...args, ovr: 50 + RETENTION_MIN_EDGE + 8 }) > 1,
			"a genuinely irreplaceable player should be worth a premium",
		);
	});

	test("the premium scales with how irreplaceable he is", () => {
		// Below the saturation point - past about a starter's worth of separation
		// the premium is already maxed out, which is deliberate.
		const args = {
			tier: "allIn" as const,
			rosterRank: 0,
			isStar: true,
			age: 27,
			wantsRelief: false,
			replacementOvr: 45,
		};
		const barelyWorthIt = retentionOverpay({ ...args, ovr: 50 });
		const clearlyWorthIt = retentionOverpay({ ...args, ovr: 56 });
		assert.ok(
			barelyWorthIt < clearlyWorthIt,
			`expected a bigger premium for the less replaceable player: ${barelyWorthIt} vs ${clearlyWorthIt}`,
		);
		assert.ok(barelyWorthIt > 1);
	});

	test("a team already over the tax stops bidding against itself", () => {
		assert.strictEqual(
			retentionOverpay({
				tier: "allIn",
				rosterRank: 0,
				isStar: true,
				age: 27,
				wantsRelief: true,
				ovr: 70,
				replacementOvr: 45,
			}),
			1,
		);
	});

	test("an aging player is worth less of a premium", () => {
		const args = {
			tier: "buyer" as const,
			rosterRank: 0,
			isStar: true,
			wantsRelief: false,
			ovr: 70,
			replacementOvr: 45,
		};
		assert.ok(
			retentionOverpay({ ...args, age: 34 }) <
				retentionOverpay({ ...args, age: 27 }),
		);
	});

	test("a young player is kept even in a teardown", () => {
		assert.strictEqual(
			shouldLetWalk({ tier: "teardown", ...base, age: 24 }),
			false,
		);
	});
});
