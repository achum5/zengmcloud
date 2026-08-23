import { assert, beforeEach, describe, test, vi } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { PHASE, PLAYER } from "../../../common/constants.ts";
import { player, team } from "../index.ts";
import clearSpaceForSignings, { planSalaryBack } from "./clearSpace.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { changeTracker } from "../../db/changeTracker.ts";

// ---------------------------------------------------------------------------
// TAKING BACK SALARY TO MAKE A DUMP LEGAL.
//
// A team clearing cap room used to need a partner with enough space under the
// cap to swallow the whole contract for nothing. In a league running at ninety
// percent of the cap there frequently is not one, and "no partner with room"
// was the commonest reason a cap-clearing plan died.
//
// The ordinary shape of this trade in real basketball is the partner sending a
// smaller contract the other way: what it sheds is what buys the room to
// absorb. That has to stay honest in both directions - the salary coming back
// is room the dumping team does not get, and a partner that has nothing small
// enough is still not a partner.
// ---------------------------------------------------------------------------

describe("planSalaryBack", () => {
	const salaryCap = 100;
	const back = (
		candidates: { pid: number; contractAmount: number; value: number }[],
		partnerPayroll: number,
		incomingSalary: number,
	) =>
		planSalaryBack({ candidates, partnerPayroll, incomingSalary, salaryCap });

	const roster = [
		{ pid: 1, contractAmount: 5, value: 40 },
		{ pid: 2, contractAmount: 20, value: 55 },
		{ pid: 3, contractAmount: 20, value: 45 },
		{ pid: 4, contractAmount: 40, value: 60 },
	];

	// Room for the whole thing already, so nothing has to come back and asking
	// for some would only cost the dumping team room it needs.
	test("a partner with room sends nothing", () => {
		assert.isUndefined(back(roster, 50, 30));
		assert.isUndefined(back(roster, 70, 30));
	});

	// Just over: the cheapest contract that covers the gap, not the first one
	// found and not the biggest.
	test("the cheapest contract that closes the gap", () => {
		assert.strictEqual(back(roster, 80, 25)?.pid, 1);
		assert.strictEqual(back(roster, 95, 25)?.pid, 3);
	});

	// Two contracts cover it equally; the partner parts with the one it minds
	// least. Without the tiebreak this is whichever happened to be listed first.
	test("among equal contracts the most expendable player goes", () => {
		assert.strictEqual(back(roster, 95, 25)?.value, 45);
		assert.strictEqual(back([...roster].reverse(), 95, 25)?.value, 45);
	});

	// Nothing on the roster is big enough, so this is not a partner - a team
	// cannot make room it does not have by sending out pocket change.
	test("a partner with nothing big enough is not a partner", () => {
		assert.isUndefined(back(roster, 95, 50));
		assert.isUndefined(back([], 95, 25));
	});
});

// ---------------------------------------------------------------------------
// The same thing driven end to end, because the pure planner being right is
// only half of it: the leg has to reach processTrade, be priced as a cost to
// the team taking it on, and leave the dumping team with enough room to
// actually sign the player it cleared for.
// ---------------------------------------------------------------------------

const NUM_TEAMS = 6;
const DUMPER_TID = 0;
const PARTNER_TID = 1;

const makePlayer = ({
	tid,
	ovr,
	age = 27,
	amount,
	exp,
}: {
	tid: number;
	ovr: number;
	age?: number;
	amount: number;
	exp: number;
}) => {
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
	p.contract = { amount, exp };
	p.injury = { type: "Healthy", gamesRemaining: 0 };
	p.value = ovr;
	p.valueNoPot = ovr;
	p.valueFuzz = ovr;
	p.valueNoPotFuzz = ovr;
	return p;
};

const stubLeagueDb = () => {
	const store = {
		index: () => store,
		getAll: async () => [],
		get: async () => undefined,
		async *iterate() {},
	};
	(idb as any).league = {
		getAll: async () => [],
		get: async () => undefined,
		transaction: () => ({
			store,
			objectStore: () => store,
			done: Promise.resolve(),
		}),
	};
};

const makeRng = (seed: number) => {
	let s = seed >>> 0;
	return () => {
		s = (s * 1_664_525 + 1_013_904_223) >>> 0;
		return s / 4_294_967_296;
	};
};

// One team desperate to clear room, one team that could help but is itself
// just over the line, and nobody else with a dollar to spare. The arithmetic
// is the whole fixture, so it is written out:
//
//   dumper  0.86 cap, of which one 0.30 contract is movable
//   partner 0.78 cap, so absorbing 0.30 puts it 0.08 over and it must shed
//           at least that much
//   prize   0.25 cap, and 0.86 - 0.30 + R + 0.25 <= cap needs R <= 0.31
//
// Every other team sits over the cap with nothing large enough to move, so the
// partner above is the league's only route to a legal deal - and before the
// salary-back leg there was no legal deal at all.
const DUMP_SHARE = 0.3;
const PARTNER_PAYROLL_SHARE = 0.78;
const PRIZE_SHARE = 0.25;

const build = async ({
	// The partner's one movable contract, as a share of the cap. Above the 0.08
	// it needs to shed, it can help; split into pieces below that, it cannot.
	partnerMovableShares,
}: {
	partnerMovableShares: number[];
}) => {
	resetG();
	g.setWithoutSavingToDB("numTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("numActiveTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
	g.setWithoutSavingToDB("daysLeft", 20);
	g.setWithoutSavingToDB("userTids", [999]);
	g.setWithoutSavingToDB("salaryCapType", "soft");
	g.setWithoutSavingToDB("smartAiFrontOffice", true);
	g.setWithoutSavingToDB("realisticFaces", false);
	g.setWithoutSavingToDB("faceAging", false);
	g.setWithoutSavingToDB("sonRate", 0);
	g.setWithoutSavingToDB("brotherRate", 0);

	const salaryCap = g.get("salaryCap");
	const season = g.get("season");
	const share = (x: number) => Math.round(salaryCap * x);
	const teams: any[] = [];
	const players: any[] = [];
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		teams.push({
			...team.generate({
				tid,
				cid: tid % 2,
				did: tid % 2,
				region: `R${tid}`,
				name: `N${tid}`,
				abbrev: `T${tid}`,
				pop: 10,
				imgURL: "",
			} as any),
			strategy: "contending",
		});
	}

	// The dumper: one large bad contract it can move, and filler around it.
	players.push(
		makePlayer({
			tid: DUMPER_TID,
			ovr: 48,
			age: 31,
			amount: share(DUMP_SHARE),
			exp: season + 2,
		}),
	);
	for (let i = 0; i < 11; i++) {
		players.push(
			makePlayer({
				tid: DUMPER_TID,
				ovr: 58 - i,
				age: 26,
				amount: share((0.86 - DUMP_SHARE) / 11),
				exp: season + 2,
			}),
		);
	}

	// The partner: right at the cap. Its payroll is the same either way - only
	// whether that money sits in one contract or several changes.
	const movableTotal = partnerMovableShares.reduce((a, x) => a + x, 0);
	for (const x of partnerMovableShares) {
		players.push({
			...makePlayer({
				tid: PARTNER_TID,
				ovr: 44,
				age: 30,
				amount: share(x),
				exp: season + 1,
			}),
		});
	}
	const partnerFillers = 12 - partnerMovableShares.length;
	for (let i = 0; i < partnerFillers; i++) {
		players.push(
			makePlayer({
				tid: PARTNER_TID,
				ovr: 50 - i,
				age: 27,
				amount: share((PARTNER_PAYROLL_SHARE - movableTotal) / partnerFillers),
				exp: season + 2,
			}),
		);
	}

	// Everyone else is over the cap with nothing large enough to move.
	for (let tid = 2; tid < NUM_TEAMS; tid++) {
		for (let i = 0; i < 12; i++) {
			players.push(
				makePlayer({
					tid,
					ovr: 52 - i,
					age: 27,
					amount: share(1.05 / 12),
					exp: season + 3,
				}),
			);
		}
	}

	// The prize, priced so the dumper cannot have him without shedding.
	players.push(
		makePlayer({
			tid: PLAYER.FREE_AGENT,
			ovr: 72,
			age: 26,
			amount: share(PRIZE_SHARE),
			exp: season + 4,
		}),
	);

	await resetCache({ players, teams });
	stubLeagueDb();
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		await idb.cache.teamSeasons.add({
			...team.genSeasonRow((await idb.cache.teams.get(tid))!),
			tid,
			season,
			won: 62,
			lost: 20,
			gp: 82,
			hype: 0.95,
		} as any);
	}
};

describe("a team clears room from a partner that has none", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	const run = async (partnerMovableShares: number[]) => {
		const spy = vi
			.spyOn(Math, "random")
			.mockImplementation(makeRng(20_260_823));
		try {
			await build({ partnerMovableShares });
			const before = await team.getPayroll(DUMPER_TID);
			await clearSpaceForSignings();
			const trades = (await idb.cache.events.getAll()).filter(
				(e: any) => e.type === "trade",
			);
			return {
				trades,
				before,
				after: await team.getPayroll(DUMPER_TID),
				dumperRoster: await idb.cache.players.indexGetAll(
					"playersByTid",
					DUMPER_TID,
				),
			};
		} finally {
			spy.mockRestore();
		}
	};

	// The whole point: the partner is over the cap, so it sends a contract back,
	// and both halves of the deal are real - salary leaves the dumper, and a
	// player arrives from the partner.
	test("the partner sends a contract back and the dump goes through", async () => {
		const { trades, before, after, dumperRoster } = await run([0.12]);
		assert.lengthOf(trades, 1, "no dump happened at all");

		// teams[i].assets is what team i RECEIVED, so index 0 is what came back
		// to the dumper and index 1 is what the partner absorbed.
		const assets = (trades[0] as any).teams as any[];
		const cameBack = assets[0].assets.filter((a: any) => a.pid !== undefined);
		assert.isAbove(
			cameBack.length,
			0,
			"the partner had no room, so it must have sent salary back",
		);

		// Net room, not gross: a dump that takes back as much as it sends is not
		// a dump. (Payroll AFTER is higher than before, because the whole point
		// was to spend the room on somebody - which is the next assertion.)
		const sentOut = assets[1].assets
			.filter((a: any) => a.pid !== undefined)
			.reduce((total: number, a: any) => total + a.contract.amount, 0);
		const tookBack = cameBack.reduce(
			(total: number, a: any) => total + a.contract.amount,
			0,
		);
		assert.isAbove(sentOut - tookBack, 0, "the dump cleared no room at all");

		const pids = dumperRoster.map((p) => p.pid);
		assert.include(
			pids,
			cameBack[0].pid,
			"the player coming back never arrived",
		);
		assert.include(
			dumperRoster.map((p) => p.ratings.at(-1)!.ovr),
			72,
			"the room was cleared and then not used",
		);
		// And the room went on the player, which is the only reason any of this
		// was allowed: payroll ends up HIGHER than it started.
		assert.isAbove(after, before);
	});

	// Same league, except the only contract the partner could move is pocket
	// change. It cannot make room, so there is no legal deal and the AI must not
	// invent one.
	test("a partner with nothing big enough to send is left alone", async () => {
		const { trades } = await run([0.06, 0.06]);
		assert.lengthOf(trades, 0);
	});

	// And the other end of the same rule. The partner CAN cover its gap, but
	// only with a contract so large that taking it back would leave the dumper
	// short of the player it was clearing for - which would be shedding salary
	// for nothing, the one outcome worse than never trying.
	test("salary coming back may not eat the room it was clearing", async () => {
		const { trades, before, after } = await run([0.25]);
		assert.lengthOf(trades, 0);
		assert.strictEqual(after, before, "payroll moved on a deal that never was");
	});
});
