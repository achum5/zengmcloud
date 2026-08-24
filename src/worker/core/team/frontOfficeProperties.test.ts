import { assert, beforeEach, describe, test } from "vitest";
import { resetG } from "../../../test/helpers.ts";
import {
	type CapHold,
	FIT_CEILING,
	FIT_FLOOR,
	type FaPlayer,
	MAX_PURSUERS_PER_PRIZE,
	MAX_RETENTION_OVERPAY,
	planCapHold,
	PURSUIT_GIVE_UP_DAYS,
	resolveCapHolds,
	retentionOverpay,
	scoreFreeAgent,
	signingYears,
} from "../freeAgents/frontOffice.ts";
import { classifyTier } from "../trade/tradePosture.ts";
import { planDumpPackage } from "../freeAgents/clearSpace.ts";
import {
	DRAFT_FIT_CEILING,
	DRAFT_FIT_FLOOR,
	scoreProspect,
} from "../draft/draftBoard.ts";
import { cutOrder, keepScore } from "./rosterCuts.ts";
import { colaAdjustedSlot, projectedSlot } from "../trade/futurePickOutlook.ts";
import type { TradePosture, TradeTier } from "../trade/tradePosture.ts";

// ---------------------------------------------------------------------------
// THE SAME SWEEP, OVER THE REST OF THE FRONT OFFICE.
//
// Sweeping the trade valuation for direction rather than for particular
// numbers found two defects that years of example tests had not, and every
// other decision the AI makes runs through a scoring function of the same
// kind. Those are all PURE - no database, no league - so they can be swept far
// harder than the valuation: every tier, every age, hundreds of points, in
// milliseconds.
//
// Two things are asserted, and only two, because they are the ones that are
// not a matter of taste:
//
//   DIRECTION. A better player scores higher. A cheaper contract scores
//   higher. More of a thing you already have scores lower. Whether the size of
//   the step is right is a tuning question that belongs in a simulation; that
//   the step points the wrong way never is.
//
//   THE ANSWER IS A NUMBER. Every one of these is fed hostile input - NaN,
//   both infinities, zero, negatives, absurd magnitudes - because an imported
//   league or a God Mode edit can produce any of them, and a scoring function
//   that returns NaN does not fail loudly. It sorts arbitrarily, and a front
//   office quietly starts making random decisions. scoreProspect already
//   carries a hand-written guard against exactly this, which is the tell that
//   it was found the hard way once.
// ---------------------------------------------------------------------------

const TIERS: TradeTier[] = ["teardown", "seller", "fringe", "buyer", "allIn"];

// Values chosen to break things: not a plausible league between them.
const HOSTILE = [
	Number.NaN,
	Number.POSITIVE_INFINITY,
	Number.NEGATIVE_INFINITY,
	0,
	-1,
	-1e9,
	1e9,
];

const SEASON = 2025;

const posture = (tier: TradeTier, over: Partial<TradePosture> = {}) =>
	({
		tid: 1,
		tier,
		contention: 0.5,
		needs: [],
		surpluses: [],
		targetPos: undefined,
		buildingBlockPids: [],
		shopVeteranPids: [],
		starGap: false,
		...over,
	}) as unknown as TradePosture;

const fa = (over: Partial<FaPlayer> = {}): FaPlayer => ({
	pid: 1,
	ovr: 55,
	pot: 60,
	value: 55,
	age: 27,
	pos: "SF",
	amount: 8000,
	exp: SEASON + 3,
	injuredGames: 0,
	...over,
});

const finite = (x: number, what: string) => {
	assert.isTrue(
		Number.isFinite(x),
		`${what} returned ${x}, which sorts arbitrarily and never throws`,
	);
};

const assertRising = (xs: number[], what: string) => {
	const shown = xs.map((x) => x.toPrecision(5)).join(" -> ");
	for (let i = 1; i < xs.length; i++) {
		assert.isAtLeast(xs[i]!, xs[i - 1]! - 1e-9, `${what}: ${shown}`);
	}
	assert.notStrictEqual(xs[0], xs.at(-1), `${what}: flat sweep (${shown})`);
};

const assertFalling = (xs: number[], what: string) => {
	const shown = xs.map((x) => x.toPrecision(5)).join(" -> ");
	for (let i = 1; i < xs.length; i++) {
		assert.isAtMost(xs[i]!, xs[i - 1]! + 1e-9, `${what}: ${shown}`);
	}
	assert.notStrictEqual(xs[0], xs.at(-1), `${what}: flat sweep (${shown})`);
};

describe("scoreFreeAgent", () => {
	beforeEach(() => {
		resetG();
	});

	test("a better player scores higher, for every tier", () => {
		for (const tier of TIERS) {
			const scores = [];
			for (const value of [35, 45, 55, 65, 75]) {
				scores.push(
					scoreFreeAgent({
						p: fa({ value, ovr: value, pot: value + 5 }),
						posture: posture(tier),
						season: SEASON,
						minContract: 1500,
						maxContract: 45000,
					}),
				);
			}
			assertRising(scores, `${tier} rated a worse free agent higher`);
		}
	});

	test("being hurt never helps, for every tier", () => {
		for (const tier of TIERS) {
			const healthy = scoreFreeAgent({
				p: fa({ injuredGames: 0 }),
				posture: posture(tier),
				season: SEASON,
				minContract: 1500,
				maxContract: 45000,
			});
			const hurt = scoreFreeAgent({
				p: fa({ injuredGames: 40 }),
				posture: posture(tier),
				season: SEASON,
				minContract: 1500,
				maxContract: 45000,
			});
			assert.isAtMost(hurt, healthy, `${tier} preferred the injured player`);
		}
	});

	test("the fit multiplier stays inside its own band", () => {
		for (const tier of TIERS) {
			for (const age of [18, 22, 26, 30, 34, 40]) {
				for (const years of [1, 3, 5]) {
					const p = fa({ age, exp: SEASON + years - 1, amount: 40000 });
					const score = scoreFreeAgent({
						p,
						posture: posture(tier),
						season: SEASON,
						minContract: 1500,
						maxContract: 45000,
					});
					finite(score, `scoreFreeAgent(${tier}, age ${age})`);
					assert.isAtLeast(score, p.value * FIT_FLOOR - 1e-9);
					assert.isAtMost(score, p.value * FIT_CEILING + 1e-9);
				}
			}
		}
	});

	test("survives anything a strange league can hand it", () => {
		for (const tier of TIERS) {
			for (const x of HOSTILE) {
				for (const key of [
					"ovr",
					"pot",
					"value",
					"age",
					"amount",
					"exp",
					"injuredGames",
				] as const) {
					const score = scoreFreeAgent({
						p: fa({ [key]: x }),
						posture: posture(tier),
						season: SEASON,
						minContract: 1500,
						maxContract: 45000,
					});
					// A player whose VALUE is not a number has no score worth
					// having; everything else must still come out usable.
					if (key !== "value") {
						finite(score, `scoreFreeAgent(${tier}, ${key}=${x})`);
					}
				}
			}
		}
	});
});

describe("scoreProspect", () => {
	const prospect = (over: Record<string, number | string> = {}) => ({
		pid: 1,
		ovr: 50,
		pot: 65,
		value: 50,
		age: 20,
		pos: "SF",
		...over,
	});

	test("a better prospect scores higher, for every tier", () => {
		for (const tier of TIERS) {
			const scores = [];
			for (const value of [30, 40, 50, 60, 70]) {
				scores.push(
					scoreProspect({
						p: prospect({ value, ovr: value }) as any,
						posture: posture(tier),
					}),
				);
			}
			assertRising(scores, `${tier} rated a worse prospect higher`);
		}
	});

	test("a position already taken today is never worth more", () => {
		for (const tier of TIERS) {
			const scores = [];
			for (const already of [0, 1, 2, 3, 4]) {
				scores.push(
					scoreProspect({
						p: prospect() as any,
						posture: posture(tier),
						alreadyDraftedAtPos: already,
					}),
				);
			}
			assertFalling(scores, `${tier} wanted a fourth player at one spot more`);
		}
	});

	test("stays inside its own band and never stops being a number", () => {
		for (const tier of TIERS) {
			for (const x of HOSTILE) {
				for (const key of ["ovr", "pot", "value", "age"] as const) {
					const p = prospect({ [key]: x }) as any;
					const score = scoreProspect({ p, posture: posture(tier) });
					// The band only binds when the whole prospect is readable.
					// scoreProspect deliberately drops anything it cannot price to
					// the bottom of the board rather than sorting it at random,
					// which is the behaviour the rest of this file is arguing for.
					finite(score, `scoreProspect(${tier}, ${key}=${x})`);
					assert.isAbove(score, 0, `scoreProspect(${tier}, ${key}=${x})`);
					const readable = ["ovr", "pot", "value", "age"].every((k) =>
						Number.isFinite(p[k]),
					);
					if (readable && p.value > 0) {
						assert.isAtLeast(score, p.value * DRAFT_FIT_FLOOR - 1e-9);
						assert.isAtMost(score, p.value * DRAFT_FIT_CEILING + 1e-9);
					}
				}
			}
		}
	});
});

describe("keepScore and cutOrder", () => {
	const cand = (over: Record<string, number | string> = {}) => ({
		pid: 1,
		value: 50,
		age: 27,
		pos: "SF",
		contractAmount: 5000,
		contractExp: SEASON + 2,
		...over,
	});

	test("the worse player is always cut first", () => {
		for (const tier of [...TIERS, undefined]) {
			const roster = [40, 45, 50, 55, 60, 65].map((value, i) =>
				cand({ pid: i, value, pos: ["PG", "SF", "C"][i % 3]! }),
			);
			const order = cutOrder(roster as any, tier, {
				season: SEASON,
				salaryCap: 90000,
			});
			// Not a full sort assertion - position scarcity is allowed to save the
			// last centre - but the very worst and the very best may not swap.
			assert.isBelow(
				(order[0] as any).value,
				(order.at(-1) as any).value,
				`${tier}: the best player was first out of the door`,
			);
		}
	});

	// The direction here is the opposite of the obvious one, and worth stating
	// because a reader will assume the obvious one. Cutting a player does not
	// release his money - the guaranteed years left become dead money - so the
	// man who is nearly off the books is the cheap one to let go and the man
	// with three years left is the expensive one. keepScore therefore RISES
	// with what is still owed, and it saturates at KEEP_COST_CEILING so nobody
	// is kept purely for being expensive.
	test("a player who costs more to cut is harder to cut", () => {
		for (const tier of TIERS) {
			const scores = [];
			for (const contractAmount of [1500, 6000, 12_000, 20_000]) {
				scores.push(
					keepScore({
						p: cand({ contractAmount, contractExp: SEASON + 2 }) as any,
						tier,
						counts: new Map(),
						season: SEASON,
						salaryCap: 90000,
					}),
				);
			}
			assertRising(scores, `${tier} let dead money go for nothing`);
		}
	});

	// And an expiring deal is free to cut by that measure, so it must not get
	// the bonus at all.
	test("an expiring contract carries no keep bonus", () => {
		for (const tier of TIERS) {
			const expiring = keepScore({
				p: cand({ contractAmount: 20_000, contractExp: SEASON }) as any,
				tier,
				counts: new Map(),
				season: SEASON,
				salaryCap: 90000,
			});
			const guaranteed = keepScore({
				p: cand({ contractAmount: 20_000, contractExp: SEASON + 2 }) as any,
				tier,
				counts: new Map(),
				season: SEASON,
				salaryCap: 90000,
			});
			assert.isBelow(
				expiring,
				guaranteed,
				`${tier} priced an expiring deal like a guaranteed one`,
			);
		}
	});

	test("never scores a player with something that is not a number", () => {
		for (const tier of [...TIERS, undefined]) {
			for (const x of HOSTILE) {
				for (const key of [
					"value",
					"age",
					"contractAmount",
					"contractExp",
				] as const) {
					const score = keepScore({
						p: cand({ [key]: x }) as any,
						tier,
						counts: new Map(),
						season: SEASON,
						salaryCap: 90000,
					});
					finite(score, `keepScore(${tier}, ${key}=${x})`);
				}
			}
		}
	});
});

describe("retentionOverpay and signingYears", () => {
	test("a bigger edge over the market is worth paying more for", () => {
		for (const tier of ["fringe", "buyer", "allIn"] as const) {
			const multipliers = [];
			for (const ovr of [50, 55, 60, 65, 70, 75]) {
				multipliers.push(
					retentionOverpay({
						tier,
						rosterRank: 0,
						isStar: false,
						age: 27,
						ovr,
						replacementOvr: 50,
					}),
				);
			}
			assertRising(multipliers, `${tier} paid less for a better player`);
			for (const m of multipliers) {
				assert.isAtMost(m, MAX_RETENTION_OVERPAY + 1e-9);
				assert.isAtLeast(m, 1);
			}
		}
	});

	test("nobody ever bids past the ceiling, whatever they are handed", () => {
		for (const tier of TIERS) {
			for (const x of HOSTILE) {
				for (const key of [
					"rosterRank",
					"age",
					"ovr",
					"replacementOvr",
				] as const) {
					const m = retentionOverpay({
						tier,
						rosterRank: 0,
						isStar: false,
						age: 27,
						ovr: 60,
						replacementOvr: 50,
						[key]: x,
					});
					finite(m, `retentionOverpay(${tier}, ${key}=${x})`);
					assert.isAtLeast(m, 1, `retentionOverpay(${tier}, ${key}=${x})`);
					assert.isAtMost(
						m,
						MAX_RETENTION_OVERPAY + 1e-9,
						`retentionOverpay(${tier}, ${key}=${x})`,
					);
				}
			}
		}
	});

	test("a contract length is always a whole number of seasons in range", () => {
		for (const tier of TIERS) {
			for (const x of HOSTILE) {
				for (const key of [
					"age",
					"askedYears",
					"amount",
					"ovr",
					"rotationOvr",
				] as const) {
					const years = signingYears({
						tier,
						age: 27,
						askedYears: 3,
						amount: 8000,
						ovr: 55,
						rotationOvr: 48,
						minContract: 1500,
						minLength: 1,
						maxLength: 5,
						[key]: x,
					});
					finite(years, `signingYears(${tier}, ${key}=${x})`);
					assert.isAtLeast(years, 1, `signingYears(${tier}, ${key}=${x})`);
					assert.isAtMost(years, 5, `signingYears(${tier}, ${key}=${x})`);
				}
			}
		}
	});
});

describe("pick outlook", () => {
	// A team that is tearing down picks earlier than one going all in, and the
	// ordering across the tiers is the whole point of the projection.
	test("the tiers project in the order they are meant to", () => {
		const slots = TIERS.map((tier) =>
			projectedSlot({ tier, avgAge: 26, seasons: 2, numPicksPerRound: 30 }),
		);
		assertRising(slots, "teardown did not project the earliest pick");
		for (const s of slots) {
			assert.isAtLeast(s, 1);
			assert.isAtMost(s, 30);
		}
	});

	test("more banked COLA chances never make a pick worse", () => {
		const slots = [];
		for (const chancesShare of [0, 0.1, 0.25, 0.5, 0.8, 1]) {
			slots.push(
				colaAdjustedSlot({
					recordSlot: 12,
					chancesShare,
					lotteryEligible: true,
					numLotteryPicks: 4,
					numPicksPerRound: 30,
				}),
			);
		}
		assertFalling(slots, "banking chances made the pick worse");
	});

	test("a slot is always a slot, whatever it is handed", () => {
		for (const tier of TIERS) {
			for (const x of HOSTILE) {
				for (const key of ["seasons", "avgAge", "numPicksPerRound"] as const) {
					const slot = projectedSlot({
						tier,
						avgAge: 26,
						seasons: 2,
						numPicksPerRound: 30,
						[key]: x,
					});
					finite(slot, `projectedSlot(${tier}, ${key}=${x})`);
				}
			}
			for (const x of HOSTILE) {
				for (const key of [
					"recordSlot",
					"chancesShare",
					"numLotteryPicks",
					"numPicksPerRound",
				] as const) {
					const slot = colaAdjustedSlot({
						recordSlot: 12,
						chancesShare: 0.3,
						lotteryEligible: true,
						numLotteryPicks: 4,
						numPicksPerRound: 30,
						[key]: x,
					});
					finite(slot, `colaAdjustedSlot(${key}=${x})`);
				}
			}
		}
	});
});

// ---------------------------------------------------------------------------

describe("planCapHold", () => {
	const hold = (over: Record<string, unknown> = {}) =>
		planCapHold({
			posture: posture("buyer"),
			prizes: [fa({ pid: 7, ovr: 74, value: 74, amount: 30_000 })],
			payroll: 40_000,
			salaryCap: 90_000,
			salaryCapType: "soft",
			daysLeft: 25,
			season: SEASON,
			minContract: 1500,
			maxContract: 45_000,
			...over,
		} as any);

	test("a hold is only ever for a prize the team could actually sign today", () => {
		const h = hold();
		assert.ok(h);
		assert.strictEqual(h.pid, 7);
		// The ceiling is the payroll it may still carry and keep the room for
		// him, so the team's own payroll has to fit under it or the hold is a
		// promise it cannot keep.
		assert.isAtLeast(h.spendCeiling, 40_000);
		assert.isAtMost(h.spendCeiling, 90_000);
	});

	// Once the money is gone it stays gone: there is no payroll at which a team
	// that could not afford to wait suddenly can again.
	test("holding stops for good as payroll rises", () => {
		let stopped = false;
		for (const payroll of [0, 20_000, 40_000, 60_000, 66_000, 70_000, 89_000]) {
			const h = hold({ payroll });
			if (h === undefined) {
				stopped = true;
			} else {
				assert.isFalse(
					stopped,
					`a team with ${payroll} held space that a poorer one could not`,
				);
				assert.isAtLeast(h.spendCeiling, payroll);
			}
		}
		assert.isTrue(stopped, "no payroll in the sweep was ever too high");
	});

	test("and stops for good as free agency runs out", () => {
		for (const daysLeft of [30, 20, 10, PURSUIT_GIVE_UP_DAYS, 3, 0, -5]) {
			const h = hold({ daysLeft });
			if (daysLeft < PURSUIT_GIVE_UP_DAYS) {
				assert.isUndefined(h, `still waiting with ${daysLeft} days left`);
			}
		}
	});

	test("nothing worth waiting for means shop instead", () => {
		assert.isUndefined(hold({ prizes: [] }));
		// Hopeless is hopeless, whatever the fit.
		assert.isUndefined(
			hold({
				prizes: [
					fa({ pid: 7, ovr: 78, value: 78, amount: 30_000, probWilling: 0 }),
				],
			}),
		);
		// And a teardown does not freeze an offseason for a star.
		assert.isUndefined(hold({ posture: posture("teardown") }));
	});

	test("survives anything a strange league can hand it", () => {
		for (const tier of TIERS) {
			for (const x of HOSTILE) {
				for (const key of ["payroll", "salaryCap", "daysLeft"] as const) {
					const h = hold({ posture: posture(tier), [key]: x });
					if (h) {
						finite(h.spendCeiling, `planCapHold(${tier}, ${key}=${x})`);
					}
				}
				for (const key of ["amount", "ovr", "value", "probWilling"] as const) {
					const h = hold({
						posture: posture(tier),
						prizes: [
							fa({ pid: 7, ovr: 74, value: 74, amount: 30_000, [key]: x }),
						],
					});
					if (h) {
						finite(h.spendCeiling, `planCapHold(${tier}, prize ${key}=${x})`);
					}
				}
			}
		}
	});
});

describe("resolveCapHolds", () => {
	const want = (tid: number, pid: number, score: number) => ({
		tid,
		hold: { pid, spendCeiling: 50_000 } as CapHold,
		score,
	});

	test("only the most credible few chase any one player", () => {
		const wanted = Array.from({ length: 9 }, (_, i) => want(i, 100, 9 - i));
		const resolved = resolveCapHolds(wanted);
		assert.strictEqual(resolved.size, MAX_PURSUERS_PER_PRIZE);
		// And it is the best of them, not just any three.
		for (let tid = 0; tid < MAX_PURSUERS_PER_PRIZE; tid++) {
			assert.isTrue(resolved.has(tid), `the ${tid} highest bid was dropped`);
		}
	});

	test("each prize is counted separately", () => {
		const wanted = [
			...Array.from({ length: 5 }, (_, i) => want(i, 100, 5 - i)),
			...Array.from({ length: 5 }, (_, i) => want(10 + i, 200, 5 - i)),
		];
		const resolved = resolveCapHolds(wanted);
		assert.strictEqual(resolved.size, 2 * MAX_PURSUERS_PER_PRIZE);
	});

	// THE ONE THAT MATTERS IN A SHARED LEAGUE. Two devices build this list by
	// walking their own team stores, which need not agree on order. If the
	// answer depended on that order they would freeze different teams'
	// offseasons and the save files would diverge - the same hazard cutOrder
	// breaks its ties on pid to avoid.
	test("the answer does not depend on what order the teams arrived in", () => {
		const wanted = [
			want(3, 100, 5),
			want(1, 100, 5), // an exact tie with tid 3, resolved by tid
			want(7, 100, 9),
			want(2, 100, 1),
			want(5, 200, 4),
			want(9, 200, 4), // and another
		];
		const expected = [...resolveCapHolds(wanted)]
			.map(([tid, h]) => `${tid}:${h.pid}`)
			.sort();
		// Every rotation of the input, which is what a different store order
		// looks like in practice.
		for (let i = 1; i < wanted.length; i++) {
			const rotated = [...wanted.slice(i), ...wanted.slice(0, i)];
			const got = [...resolveCapHolds(rotated)]
				.map(([tid, h]) => `${tid}:${h.pid}`)
				.sort();
			assert.deepStrictEqual(
				got,
				expected,
				`rotating the input by ${i} changed who holds`,
			);
		}
		// Reversed too, which no rotation produces.
		const reversed = [...resolveCapHolds([...wanted].reverse())]
			.map(([tid, h]) => `${tid}:${h.pid}`)
			.sort();
		assert.deepStrictEqual(reversed, expected);
	});
});

describe("classifyTier", () => {
	const ORDER: Record<string, number> = {
		teardown: 0,
		seller: 1,
		fringe: 2,
		buyer: 3,
		allIn: 4,
	};

	const tierAt = (winp: number, over: Record<string, unknown> = {}) =>
		classifyTier({
			winp,
			ovrRankPct: 0.5,
			avgAge: 27,
			youngCoreCount: 0,
			hasFoundation: false,
			...over,
		} as any);

	test("winning more never moves a team further from contending", () => {
		for (const avgAge of [23, 27, 31]) {
			for (const youngCoreCount of [0, 2]) {
				for (const hasFoundation of [false, true]) {
					const ranks: number[] = [];
					for (let winp = 0; winp <= 1.0001; winp += 0.05) {
						ranks.push(
							ORDER[tierAt(winp, { avgAge, youngCoreCount, hasFoundation })]!,
						);
					}
					assertRising(
						ranks,
						`age ${avgAge}/core ${youngCoreCount}/foundation ${hasFoundation}: winning more made a team sell`,
					);
				}
			}
		}
	});

	test("a better roster never moves a team further from contending either", () => {
		for (const winp of [0.2, 0.4, 0.55, 0.75]) {
			const ranks: number[] = [];
			// 0 is the best roster in the league and 1 the worst, so this walks
			// from worst to best and the tier must not fall.
			for (const ovrRankPct of [1, 0.75, 0.5, 0.25, 0]) {
				ranks.push(ORDER[tierAt(winp, { ovrRankPct })]!);
			}
			for (let i = 1; i < ranks.length; i++) {
				assert.isAtLeast(
					ranks[i]!,
					ranks[i - 1]!,
					`winp ${winp}: a better roster sold harder`,
				);
			}
		}
	});

	// A young core to build around is the difference between retooling and
	// tearing it down, and it may only ever soften the answer.
	test("something to build around never makes a team tear down harder", () => {
		for (let winp = 0; winp <= 1.0001; winp += 0.05) {
			for (const ovrRankPct of [0, 0.5, 1]) {
				const without =
					ORDER[tierAt(winp, { ovrRankPct, hasFoundation: false })]!;
				const with_ = ORDER[tierAt(winp, { ovrRankPct, hasFoundation: true })]!;
				assert.isAtLeast(
					with_,
					without,
					`winp ${winp}: a foundation made the team sell harder`,
				);
			}
		}
	});

	test("always answers with a tier, whatever it is handed", () => {
		for (const x of HOSTILE) {
			for (const key of [
				"winp",
				"ovrRankPct",
				"avgAge",
				"youngCoreCount",
			] as const) {
				const tier = tierAt(0.5, { [key]: x });
				assert.isTrue(
					Object.hasOwn(ORDER, tier),
					`classifyTier(${key}=${x}) returned ${tier}`,
				);
			}
		}
	});
});

describe("planDumpPackage", () => {
	const roster = [
		{ pid: 1, contractAmount: 3000, value: 40 },
		{ pid: 2, contractAmount: 8000, value: 52 },
		{ pid: 3, contractAmount: 12_000, value: 48 },
		{ pid: 4, contractAmount: 21_000, value: 55 },
		{ pid: 5, contractAmount: 5000, value: 61 },
	];

	// THE ONLY THING THAT REALLY MATTERS ABOUT THIS FUNCTION. A dump that does
	// not cover the shortfall is the one outcome clearSpace calls worse than
	// never trying: the team gives players away, then still cannot sign the man
	// it gave them away for. Undefined is a fine answer; a package that falls
	// short is not.
	test("whatever it returns actually covers the shortfall", () => {
		for (let shortfall = 0; shortfall <= 60_000; shortfall += 500) {
			for (const maxPlayers of [1, 2, 3, 5]) {
				const chosen = planDumpPackage({
					candidates: [...roster],
					shortfall,
					maxPlayers,
				});
				if (chosen === undefined) {
					continue;
				}
				const total = chosen.reduce((t, p) => t + p.contractAmount, 0);
				assert.isAtLeast(
					total,
					shortfall,
					`shortfall ${shortfall} (max ${maxPlayers}) came back ${total} short of covered`,
				);
				assert.isAtMost(
					chosen.length,
					maxPlayers,
					`shortfall ${shortfall}: dumped more players than allowed`,
				);
				// Nobody is dumped twice, and everybody dumped was on the roster.
				const pids = new Set(chosen.map((p) => p.pid));
				assert.strictEqual(pids.size, chosen.length);
				for (const p of chosen) {
					assert.include(
						roster.map((r) => r.pid),
						p.pid,
					);
				}
			}
		}
	});

	// And it gives up rather than gutting the roster: a shortfall bigger than
	// everything it could legally move has no package.
	test("a shortfall it cannot reach has no package at all", () => {
		const everything = roster.reduce((t, p) => t + p.contractAmount, 0);
		assert.isUndefined(
			planDumpPackage({
				candidates: [...roster],
				shortfall: everything + 1,
				maxPlayers: roster.length,
			}),
		);
		assert.isUndefined(planDumpPackage({ candidates: [], shortfall: 1000 }));
	});

	test("survives anything a strange league can hand it", () => {
		for (const x of HOSTILE) {
			for (const key of ["contractAmount", "value"] as const) {
				const candidates = roster.map((p, i) =>
					i === 0 ? { ...p, [key]: x } : { ...p },
				);
				const chosen = planDumpPackage({ candidates, shortfall: 10_000 });
				if (chosen) {
					for (const p of chosen) {
						assert.include(
							roster.map((r) => r.pid),
							p.pid,
						);
					}
				}
			}
			const chosen = planDumpPackage({
				candidates: [...roster],
				shortfall: x,
			});
			if (chosen) {
				const total = chosen.reduce((t, p) => t + p.contractAmount, 0);
				// A shortfall that is not a number cannot be covered, so the only
				// honest answers are undefined or a package that would cover a
				// real one - never a silent partial dump.
				assert.isTrue(
					Number.isFinite(total),
					`shortfall=${x} produced a package totalling ${total}`,
				);
			}
		}
	});
});
