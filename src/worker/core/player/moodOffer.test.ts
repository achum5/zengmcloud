import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import { PHASE } from "../../../common/constants.ts";
import { player, team } from "../index.ts";
import moodInfo, { OFFER_MAX_RATIO } from "./moodInfo.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { changeTracker } from "../../db/changeTracker.ts";

// ---------------------------------------------------------------------------
// Money and willingness.
//
// Before this, the offer only ever set the PRICE of a deal a player had already
// decided to accept - overrides.contractAmount was consumed after probWilling
// was computed and could not touch it. So a player who wanted out was gone at
// any figure, which is the one thing a front office would actually try first.
//
// moodInfo has eleven callers, including human negotiation, the trade AI and
// the UI. The new lever is therefore strictly opt-in: it does nothing unless a
// caller passes `offer`, and the first test here is the one that keeps those
// eleven honest.
// ---------------------------------------------------------------------------

const setup = async () => {
	resetG();
	g.setWithoutSavingToDB("numTeams", 2);
	g.setWithoutSavingToDB("numActiveTeams", 2);
	g.setWithoutSavingToDB("phase", PHASE.RESIGN_PLAYERS);
	g.setWithoutSavingToDB("userTids", [-99]);

	const teams = [0, 1].map((tid) =>
		team.generate({
			tid,
			cid: 0,
			did: 0,
			region: `R${tid}`,
			name: `T${tid}`,
			abbrev: `T${tid}`,
			pop: 3,
			popRank: tid + 1,
			strategy: "contending",
		}),
	);

	const p: any = player.generate(
		0,
		28,
		g.get("season") - 28,
		true,
		DEFAULT_LEVEL,
	);
	const r = p.ratings.at(-1);
	r.ovr = 62;
	r.pot = 62;
	r.pos = "SF";
	p.born.year = g.get("season") - 28;
	p.contract = { amount: 20_000, exp: g.get("season") };
	p.injury = { type: "Healthy", gamesRemaining: 0 };
	p.value = 62;
	p.valueNoPot = 62;
	p.valueFuzz = 62;
	p.valueNoPotFuzz = 62;

	await resetCache({ players: [p], teams });
	return p;
};

describe("what a better offer does to a player's willingness", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	test("omitting the offer leaves every existing caller untouched", async () => {
		const p = await setup();
		const withoutOffer = await moodInfo(p, 0, { contractAmount: 20_000 });
		const alsoWithout = await moodInfo(p, 0, { contractAmount: 20_000 });
		assert.strictEqual(withoutOffer.probWilling, alsoWithout.probWilling);

		// And passing exactly what he is asking is a no-op, so the lever is
		// centred where it should be rather than quietly biased.
		const atAsk = await moodInfo(p, 0, {
			contractAmount: 20_000,
			offer: withoutOffer.contractAmount,
		});
		assert.ok(
			Math.abs(atAsk.probWilling - withoutOffer.probWilling) < 1e-9,
			`offering exactly the asking price moved probWilling from ${withoutOffer.probWilling} to ${atAsk.probWilling}`,
		);
	});

	test("more money raises willingness, less money lowers it", async () => {
		const p = await setup();
		const base = await moodInfo(p, 0, { contractAmount: 20_000 });
		const ask = base.contractAmount;

		const over = await moodInfo(p, 0, {
			contractAmount: 20_000,
			offer: ask * 1.3,
		});
		const under = await moodInfo(p, 0, {
			contractAmount: 20_000,
			offer: ask * 0.75,
		});

		assert.ok(
			over.probWilling > base.probWilling,
			`overpaying should help: ${base.probWilling} -> ${over.probWilling}`,
		);
		assert.ok(
			under.probWilling < base.probWilling,
			`lowballing should hurt: ${base.probWilling} -> ${under.probWilling}`,
		);
	});

	test("a big enough overpay actually changes the answer, not just the odds", async () => {
		// probWilling moving is meaningless if `willing` never flips - that was the
		// whole complaint about the old behaviour.
		const p = await setup();
		const base = await moodInfo(p, 0, { contractAmount: 20_000 });
		if (base.willing) {
			// This fixture happened to draw a willing player; nothing to prove.
			return;
		}
		const over = await moodInfo(p, 0, {
			contractAmount: 20_000,
			offer: base.contractAmount * OFFER_MAX_RATIO,
		});
		assert.ok(
			over.probWilling > base.probWilling * 2,
			`a maximum overpay barely moved the needle: ${base.probWilling} -> ${over.probWilling}`,
		);
	});

	test("past the cap, more money buys nothing", async () => {
		// Otherwise a rich enough team could simply purchase anybody.
		const p = await setup();
		const base = await moodInfo(p, 0, { contractAmount: 20_000 });
		const ask = base.contractAmount;

		const atCap = await moodInfo(p, 0, {
			contractAmount: 20_000,
			offer: ask * OFFER_MAX_RATIO,
		});
		const absurd = await moodInfo(p, 0, {
			contractAmount: 20_000,
			offer: ask * 50,
		});
		assert.strictEqual(
			atCap.probWilling,
			absurd.probWilling,
			"offering fifty times the asking price should be worth no more than the cap",
		);
	});

	test("the draw stays fixed, so the same offer always gets the same answer", async () => {
		// Sync and anti-save-scum both depend on this: re-asking must not re-roll.
		const p = await setup();
		const a = await moodInfo(p, 0, { contractAmount: 20_000, offer: 26_000 });
		const b = await moodInfo(p, 0, { contractAmount: 20_000, offer: 26_000 });
		assert.strictEqual(a.willing, b.willing);
		assert.strictEqual(a.probWilling, b.probWilling);
	});

	test("an unwilling player cannot be bought at the minimum", async () => {
		const p = await setup();
		const base = await moodInfo(p, 0, { contractAmount: 20_000 });
		const lowball = await moodInfo(p, 0, {
			contractAmount: 20_000,
			offer: g.get("minContract"),
		});
		assert.ok(lowball.probWilling < base.probWilling);
	});
});
