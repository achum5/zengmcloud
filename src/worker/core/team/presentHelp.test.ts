import { assert, beforeEach, describe, test } from "vitest";
import { idb } from "../../db/index.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { trade } from "../index.ts";
import { ValueChangeCalculator } from "./ValueChangeCalculator.ts";
import {
	MAX_PRESENT_DROP,
	PRESENT_HELP_WEIGHT,
	presentHelpPremium,
} from "./presentHelp.ts";
import type { TradeTier } from "../trade/tradePosture.ts";
import {
	getLeagueTradeContext,
	getTradePosture,
} from "../trade/tradePosture.ts";
import {
	AI_TID,
	buildValuationLeague as build,
	USER_TID,
} from "../../../test/fixtures/valuationLeague.ts";

const TIERS: TradeTier[] = ["teardown", "seller", "fringe", "buyer", "allIn"];

describe("presentHelpPremium", () => {
	test("a selling team is charged nothing for getting worse", () => {
		for (const tier of ["teardown", "seller"] as const) {
			assert.strictEqual(
				presentHelpPremium({ tier, ovrBefore: 60, ovrAfter: 50 }),
				0,
			);
		}
	});

	test("nothing is charged when the floor holds or improves", () => {
		for (const tier of TIERS) {
			assert.strictEqual(
				presentHelpPremium({ tier, ovrBefore: 60, ovrAfter: 60 }),
				0,
			);
			assert.strictEqual(
				presentHelpPremium({ tier, ovrBefore: 60, ovrAfter: 63 }),
				0,
			);
		}
	});

	test("the more a team is trying to win, the more a point off the floor costs", () => {
		const at = (tier: TradeTier) =>
			presentHelpPremium({ tier, ovrBefore: 60, ovrAfter: 57 });
		assert.isAbove(at("allIn"), at("buyer"));
		assert.isAbove(at("buyer"), at("fringe"));
		assert.isAbove(at("fringe"), 0);
		assert.strictEqual(at("seller"), 0);
	});

	test("grows with the drop, up to a ceiling", () => {
		let prev = 0;
		for (const drop of [0.5, 1, 2, 4, 8]) {
			const now = presentHelpPremium({
				tier: "allIn",
				ovrBefore: 60,
				ovrAfter: 60 - drop,
			});
			assert.isAbove(now, prev);
			prev = now;
		}
		assert.strictEqual(
			presentHelpPremium({ tier: "allIn", ovrBefore: 60, ovrAfter: 20 }),
			PRESENT_HELP_WEIGHT.allIn * MAX_PRESENT_DROP,
		);
	});

	test("survives anything a strange league can hand it", () => {
		for (const tier of TIERS) {
			for (const [before, after] of [
				[Number.NaN, 50],
				[50, Number.NaN],
				[Infinity, 50],
				[50, -Infinity],
				[0, 0],
			]) {
				const v = presentHelpPremium({
					tier,
					ovrBefore: before!,
					ovrAfter: after!,
				});
				assert.isTrue(Number.isFinite(v), `${tier} ${before} ${after}`);
				assert.isAtLeast(v, 0);
				assert.isAtMost(v, PRESENT_HELP_WEIGHT[tier] * MAX_PRESENT_DROP);
			}
		}
	});
});

// The user offers `give` (players and picks) and asks for `get`.
const offer = async (give: number[], get: number[], dpids: number[] = []) => {
	await idb.cache.trade.clear();
	await idb.cache.trade.add({
		rid: 0,
		teams: [
			{
				tid: USER_TID,
				pids: give,
				pidsExcluded: [],
				dpids,
				dpidsExcluded: [],
			},
			{
				tid: AI_TID,
				pids: get,
				pidsExcluded: [],
				dpids: [],
				dpidsExcluded: [],
			},
		],
	} as any);
	return trade.propose(false);
};

const REFUSED_ON_VALUE = /Close, but not quite|not a good deal|are you crazy/;

// The AI's side in every test below: its best on-court player, chosen to sit
// under starValue so the all-in untouchable guard stays out of it - what is
// being measured is the price, not a refusal.
const STARTER = { ovr: 58, age: 29 };
// A record that makes the fixture's ageing roster go all-in.
const ALL_IN_WINS = 60;
// And one that tears it down.
const TEARDOWN_WINS = 20;

// dv(young + twin) - dv(young) - dv(twin) isolates the premium exactly:
// without it the three evaluations are additive, so the identity is zero;
// with it, the trade that takes the starter off the floor is charged extra
// and the one that replaces him like-for-like is not.
const floorCharge = async (aiWon: number) => {
	const { userExtra, aiExtra } = await build({
		user: [
			{ ovr: 50, pot: 68, age: 21 },
			{ ovr: 58, age: 29 },
		],
		ai: [STARTER],
		aiWon,
	});
	const posture = await getTradePosture(AI_TID, await getLeagueTradeContext());
	const vcc = new ValueChangeCalculator();
	const ev = (pidsAdd: number[], pidsRemove: number[]) =>
		vcc.evaluate({
			tid: AI_TID,
			pidsAdd,
			pidsRemove,
			dpidsAdd: [],
			dpidsRemove: [],
			tradingPartnerTid: USER_TID,
		});
	const young = userExtra[0]!.pid;
	const twin = userExtra[1]!.pid;
	const starter = aiExtra[0]!.pid;
	const youngAlone = await ev([young], [starter]);
	const youngAndTwin = await ev([young, twin], [starter]);
	const twinAlone = await ev([twin], []);
	return { tier: posture.tier, charge: youngAndTwin - youngAlone - twinAlone };
};

describe("a team trying to win charges for what leaves the floor", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	test("an all-in team wants more for its starter than the ledger says", async () => {
		const { tier, charge } = await floorCharge(ALL_IN_WINS);
		assert.strictEqual(tier, "allIn");
		assert.isAbove(charge, 0.1);
	});

	test("a team tearing down prices him off the ledger alone", async () => {
		const { tier, charge } = await floorCharge(TEARDOWN_WINS);
		assert.strictEqual(tier, "teardown");
		assert.closeTo(charge, 0, 1e-6);
	});

	// The complaint this exists for: a contender's good player going out for a
	// prospect whose number is bigger only because most of it is potential.
	test("a prospect that clears the ledger does not clear the floor", async () => {
		const { userExtra, aiExtra } = await build({
			user: [{ ovr: 50, pot: 64, age: 21 }],
			ai: [STARTER],
			aiWon: ALL_IN_WINS,
		});
		assert.isAbove(userExtra[0]!.value, aiExtra[0]!.value);
		const [ok, msg] = await offer([userExtra[0]!.pid], [aiExtra[0]!.pid]);
		assert.isFalse(ok);
		assert.match(msg ?? "", REFUSED_ON_VALUE);
	});

	// Less willing is not unwilling: a real overpay still gets him.
	test("but he is still for sale at a real price", async () => {
		const { userExtra, aiExtra } = await build({
			user: [{ ovr: 50, pot: 80, age: 21 }],
			ai: [STARTER],
			aiWon: ALL_IN_WINS,
		});
		const [ok] = await offer([userExtra[0]!.pid], [aiExtra[0]!.pid]);
		assert.isTrue(ok);
	});

	// And nothing changes for the deal a contender should always take.
	test("a better player today is taken straight up, as before", async () => {
		const { userExtra, aiExtra } = await build({
			user: [{ ovr: 60, age: 29 }],
			ai: [STARTER],
			aiWon: ALL_IN_WINS,
		});
		const [ok] = await offer([userExtra[0]!.pid], [aiExtra[0]!.pid]);
		assert.isTrue(ok);
	});

	test("the same prospect buys the same starter from a team that is selling", async () => {
		const { userExtra, aiExtra } = await build({
			user: [{ ovr: 50, pot: 64, age: 21 }],
			ai: [STARTER],
			aiWon: TEARDOWN_WINS,
		});
		const [ok] = await offer([userExtra[0]!.pid], [aiExtra[0]!.pid]);
		assert.isTrue(ok);
	});
});
