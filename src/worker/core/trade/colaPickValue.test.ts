import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { PHASE, PLAYER } from "../../../common/constants.ts";
import { draft, player, team } from "../index.ts";
import { ValueChangeCalculator } from "../team/ValueChangeCalculator.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { changeTracker } from "../../db/changeTracker.ts";

// ---------------------------------------------------------------------------
// WHAT A PICK IS WORTH UNDER CARRY-OVER LOTTERY ALLOCATION.
//
// COLA is not a lottery run on last season's record. Chances accumulate year
// on year, are cut by playoff success, and are spent entirely on winning the
// first pick - so two teams with identical records can be in completely
// different positions, and the trade AI has to know it.
//
// The rule that catches people out is stronger still: a first-round pick that
// has CHANGED HANDS is excluded from the lottery outright. The worst team's
// first is a lottery ticket while that team holds it and an ordinary
// mid-rounder the moment anybody trades for it. An AI that misses this pays
// lottery money for something that cannot win the lottery.
//
// These tests drive the real valuation rather than the pure model, because the
// pure model was already right the first time - what needed proving is that
// the stockpile and the eligibility rule actually reach it.
// ---------------------------------------------------------------------------

const NUM_TEAMS = 12;

// Where the stockpile lives, and who has one.
const STOCKED_TID = 1;
const SPENT_TID = 2;
// A team on the other end of a deal, good enough to be nobody's twin.
const BUYER_TID = 0;
const BIG_STOCKPILE = 8000;

const makePlayer = (tid: number, ovr: number, age: number) => {
	const p: any = player.generate(
		tid,
		age,
		g.get("season") - age,
		true,
		DEFAULT_LEVEL,
	);
	const ratings = p.ratings.at(-1);
	ratings.ovr = ovr;
	ratings.pot = ovr;
	p.born.year = g.get("season") - age;
	p.contract = { amount: g.get("minContract") * 3, exp: g.get("season") + 2 };
	p.injury = { type: "Healthy", gamesRemaining: 0 };
	p.value = ovr;
	p.valueNoPot = ovr;
	p.valueFuzz = ovr;
	p.valueNoPotFuzz = ovr;
	return p;
};

// Two equally bad teams, one of which has been banking chances for years.
// Everything else about them is identical, so any difference in what their
// picks are worth is the stockpile and nothing else.
const build = async (draftType: "cola" | "nba2019", chances: number) => {
	resetG();
	g.setWithoutSavingToDB("numTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("numActiveTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("phase", PHASE.DRAFT_LOTTERY);
	g.setWithoutSavingToDB("userTids", [999]);
	g.setWithoutSavingToDB("smartAiFrontOffice", true);
	g.setWithoutSavingToDB("draftType", draftType);
	g.setWithoutSavingToDB("realisticFaces", false);
	g.setWithoutSavingToDB("faceAging", false);

	const teams: any[] = [];
	const players: any[] = [];
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		const t: any = team.generate({
			tid,
			cid: tid % 2,
			did: tid % 2,
			region: `R${tid}`,
			name: `N${tid}`,
			abbrev: `T${tid}`,
			pop: 2,
			imgURL: "",
		} as any);
		// Only the one team under test ever banks anything, so its share of the
		// pool is the variable and nothing else is.
		t.draftLottery = {
			type: "cola",
			chances: tid === STOCKED_TID ? chances : 0,
		};
		teams.push(t);

		// The two teams under test are equally bad; the rest are good, so the
		// pair sit together at the bottom of the projection.
		const bad = tid === STOCKED_TID || tid === SPENT_TID;
		for (let i = 0; i < 10; i++) {
			players.push(makePlayer(tid, bad ? 40 + i : 55 + i, 25));
		}
	}

	await resetCache({ players, teams });
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		const bad = tid === STOCKED_TID || tid === SPENT_TID;
		await idb.cache.teamSeasons.add({
			...team.genSeasonRow((await idb.cache.teams.get(tid))!),
			tid,
			season: g.get("season"),
			won: bad ? 15 : 55,
			lost: bad ? 67 : 27,
			gp: 82,
		} as any);
	}

	// A draft class has to exist, and it has to be STEEP. Without one at all
	// every pick prices at null and the valuation comes out NaN - the trap
	// offseasonSim.test.ts warns about. With a flat one, every slot is worth the
	// same and a test comparing slots passes on floating-point noise, which is
	// what the first version of this file did.
	for (let i = 0; i < NUM_TEAMS * 2; i++) {
		const prospect = makePlayer(PLAYER.UNDRAFTED, 68 - 2 * i, 19);
		prospect.ratings.at(-1).pot = 78 - 2 * i;
		prospect.draft.year = g.get("season") + 1;
		await idb.cache.players.add(prospect);
	}

	await draft.genPicks();
};

// The first-round pick a team still owns itself, next season.
const ownFirst = async (tid: number) => {
	const picks = await idb.cache.draftPicks.getAll();
	const dp = picks.find(
		(p) =>
			p.originalTid === tid &&
			p.tid === tid &&
			p.round === 1 &&
			p.season === g.get("season") + 1,
	);
	assert.ok(dp, `no own first for team ${tid}`);
	return dp!;
};

// What this pick is worth to a team, either as something it is taking on or as
// something it is giving up. Sign is normalised so bigger always means more
// valuable.
const worth = async (
	dpid: number,
	{ tid, acquiring }: { tid: number; acquiring: boolean },
) => {
	const calc = new ValueChangeCalculator();
	const dv = await calc.evaluate({
		tid,
		pidsAdd: [],
		pidsRemove: [],
		dpidsAdd: acquiring ? [dpid] : [],
		dpidsRemove: acquiring ? [] : [dpid],
		tradingPartnerTid: undefined,
	});
	return acquiring ? dv : -dv;
};

describe("the trade AI reads the COLA stockpile", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	// What a team's own first is worth to it, in a league built twice: once
	// where it has banked chances and once where it has not. Same team, same
	// roster, same record, same rivals - the stockpile is the only thing that
	// moves, which is the only way to attribute the difference to it.
	const ownFirstWorth = async (
		draftType: "cola" | "nba2019",
		chances: number,
	) => {
		await build(draftType, chances);
		const dp = await ownFirst(STOCKED_TID);
		return worth(dp.dpid, { tid: STOCKED_TID, acquiring: false });
	};

	test("a banked stockpile makes a team's own first worth more", async () => {
		const stocked = await ownFirstWorth("cola", BIG_STOCKPILE);
		const spent = await ownFirstWorth("cola", 0);
		assert.isAbove(stocked, spent);
	});

	// What that same pick is worth to somebody TRADING FOR it. Same acquiring
	// team both times, so the only thing that varies is what the selling team
	// had banked.
	const acquiredWorth = async (chances: number) => {
		await build("cola", chances);
		const dp = await ownFirst(STOCKED_TID);
		return worth(dp.dpid, { tid: BUYER_TID, acquiring: true });
	};

	// THE ELIGIBILITY RULE, stated as the thing it actually implies: a
	// stockpile is worth a great deal to the team holding the pick and exactly
	// nothing to anyone who trades for it, because acquiring the pick is what
	// disqualifies it from the lottery. Both halves in one test - the first
	// would fail if the feature were removed, the second if it were applied to
	// the wrong side of the deal.
	test("the stockpile is worth everything to the holder and nothing to a buyer", async () => {
		const heldStocked = await ownFirstWorth("cola", BIG_STOCKPILE);
		const heldSpent = await ownFirstWorth("cola", 0);
		assert.isAbove(heldStocked, heldSpent);

		const boughtStocked = await acquiredWorth(BIG_STOCKPILE);
		const boughtSpent = await acquiredWorth(0);
		assert.isBelow(
			Math.abs(boughtStocked - boughtSpent),
			Math.abs(heldStocked - heldSpent) / 100,
			`a buyer paid for lottery odds the trade destroys: ${boughtStocked} vs ${boughtSpent}`,
		);
	});

	// And the control: none of this may leak into a league that is not playing
	// COLA, where chances are handed out fresh by record every year and a
	// traded pick keeps every bit of its lottery odds. Scale-free, because the
	// two are equal only up to floating point.
	test("under a normal lottery the stockpile is ignored entirely", async () => {
		const stocked = await ownFirstWorth("nba2019", BIG_STOCKPILE);
		const spent = await ownFirstWorth("nba2019", 0);
		assert.isBelow(
			Math.abs(stocked - spent),
			Math.abs(stocked) / 1000,
			`the stockpile moved a non-COLA valuation: ${stocked} vs ${spent}`,
		);
	});
});
