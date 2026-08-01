import { assert, describe, test } from "vitest";
import {
	ageFitMultiplier,
	contractRiskMultiplier,
	isPrize,
	MAX_PURSUERS_PER_PRIZE,
	planCapHold,
	positionFitMultiplier,
	PURSUIT_GIVE_UP_DAYS,
	resolveCapHolds,
	scoreFreeAgent,
	shouldLetWalk,
	type FaPlayer,
} from "./frontOffice.ts";
import type { TradePosture } from "../trade/tradePosture.ts";

const SEASON = 2025;
const MIN = 1000;
const CAP = 100_000;

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

describe("who a team should want", () => {
	// The most obviously wrong thing the old code did: everyone chased the same
	// player, so a team tearing it down handed multi-year money to a 34-year-old.
	test("a teardown prefers youth and a win-now team prefers the veteran", () => {
		const kid = fa({ pid: 1, age: 22, ovr: 50, pot: 70, value: 55 });
		const vet = fa({ pid: 2, age: 33, ovr: 60, pot: 60, value: 55 });

		const rebuilding = posture({ tier: "teardown" });
		const winNow = posture({ tier: "allIn" });

		const score = (p: FaPlayer, pos: TradePosture) =>
			scoreFreeAgent({ p, posture: pos, season: SEASON, minContract: MIN });

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
		assert.ok(ageFitMultiplier("teardown", 22) > ageFitMultiplier("teardown", 33));
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
			scoreFreeAgent({ p, posture: needsBigs, season: SEASON, minContract: MIN });
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
			contractRiskMultiplier({ tier: "teardown", age: 34, years: 1, amount: 20_000, minContract: MIN }),
			1,
		);
		assert.strictEqual(
			contractRiskMultiplier({ tier: "teardown", age: 34, years: 4, amount: MIN, minContract: MIN }),
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
			scoreFreeAgent({ p, posture: taxed, season: SEASON, minContract: MIN }) <
				scoreFreeAgent({ p, posture: normal, season: SEASON, minContract: MIN }),
		);
	});
});

describe("clearing cap space for a big free agent", () => {
	const star = fa({ pid: 99, ovr: 70, value: 70, amount: 30_000 });

	const plan = (overrides: Parameters<typeof planCapHold>[0] extends never ? never : Partial<Parameters<typeof planCapHold>[0]>) =>
		planCapHold({
			posture: posture(),
			prizes: [star],
			payroll: 50_000,
			salaryCap: CAP,
			salaryCapType: "soft",
			daysLeft: 30,
			season: SEASON,
			minContract: MIN,
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
		assert.strictEqual(plan({ posture: posture({ tier: "teardown" }) }), undefined);
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
			isPrize({ p: fa({ ovr: 60, amount: 30_000 }), starOvr: 65, minContract: MIN }),
			false,
		);
		// A star on a minimum deal needs no space cleared for him.
		assert.strictEqual(
			isPrize({ p: fa({ ovr: 70, amount: MIN * 2 }), starOvr: 65, minContract: MIN }),
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
		assert.deepStrictEqual([...resolved.keys()].sort((a, b) => a - b), [2, 5]);
	});
});

describe("letting a player walk", () => {
	const base = { age: 31, amount: MIN * 6, years: 3, isStar: false, minContract: MIN };

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

	test("a young player is kept even in a teardown", () => {
		assert.strictEqual(shouldLetWalk({ tier: "teardown", ...base, age: 24 }), false);
	});
});
