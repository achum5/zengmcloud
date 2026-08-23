import { assert, beforeEach, describe, test, vi } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { PHASE, PLAYER } from "../../../common/constants.ts";
import { player, team } from "../index.ts";
import autoSign from "./autoSign.ts";
import decreaseDemands from "./decreaseDemands.ts";
import clearSpaceForSignings from "./clearSpace.ts";
import newPhaseResignPlayers from "../phase/newPhaseResignPlayers.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { changeTracker } from "../../db/changeTracker.ts";

// ---------------------------------------------------------------------------
// AN AI TEAM IS NOT RUNNING A BUSINESS.
//
// It has to navigate the salary CAP, because that is a rule of the game. It is
// not trying to duck the luxury tax, clear the spending floor, protect its
// cash, or turn a profit, and it never sheds a player to save money - the only
// reason to move salary is to buy something better with the room.
//
// That is easy to state and easy to erode: one plausible-looking "don't go
// deep into the tax" check added years from now would quietly turn every AI
// front office into an accountant, and nothing else in the suite would notice.
// So this pins it as a property rather than a comment. Every dial that is
// about MONEY rather than the cap gets swung to both extremes, and the league
// has to make exactly the same decisions either way.
//
// The last test is the control. Swinging the CAP - a rule - must change what
// teams do, otherwise the other three prove nothing except that the harness
// cannot see anything at all.
// ---------------------------------------------------------------------------

const NUM_TEAMS = 6;

const makePlayer = ({
	tid,
	ovr,
	age = 27,
	pos = "SF",
	amount,
	exp,
}: {
	tid: number;
	ovr: number;
	age?: number;
	pos?: string;
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
	ratings.pos = pos;
	p.born.year = g.get("season") - age;
	p.contract = { amount, exp };
	p.injury = { type: "Healthy", gamesRemaining: 0 };
	p.value = ovr;
	p.valueNoPot = ovr;
	p.valueFuzz = ovr;
	p.valueNoPotFuzz = ovr;
	return p;
};

// Everything a money dial could plausibly be wired to, so a test can move one
// without touching the cap.
type Money = {
	luxuryPayroll: number;
	minPayroll: number;
	cash: number;
	salaryCap?: number;
};

// Nothing here should reach past the cache; if something does, it gets an
// empty store rather than a crash. Same stub as settingsRoundTrip.test.ts.
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

// One full free agency, played out under the given financial conditions. The
// RNG is pinned before the fixture is built, not after, so both arms get the
// same league as well as the same rolls - see offseasonSim.test.ts for what
// happens when construction is left outside the spy.
const runOffseason = async (money: Money) => {
	const rng = makeRng(20_250_824);
	const spy = vi.spyOn(Math, "random").mockImplementation(rng);
	try {
		resetG();
		g.setWithoutSavingToDB("numTeams", NUM_TEAMS);
		g.setWithoutSavingToDB("numActiveTeams", NUM_TEAMS);
		g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
		g.setWithoutSavingToDB("userTids", [999]);
		g.setWithoutSavingToDB("salaryCapType", "soft");
		// Face generation draws from Math.random, so leaving it on would let a
		// hairline shift the stream the economics run on.
		g.setWithoutSavingToDB("realisticFaces", false);
		g.setWithoutSavingToDB("faceAging", false);
		// Relatives are beside the point here, and generating them reads the
		// league database this harness does not have.
		g.setWithoutSavingToDB("sonRate", 0);
		g.setWithoutSavingToDB("brotherRate", 0);

		if (money.salaryCap !== undefined) {
			g.setWithoutSavingToDB("salaryCap", money.salaryCap);
		}
		g.setWithoutSavingToDB("luxuryPayroll", money.luxuryPayroll);
		g.setWithoutSavingToDB("minPayroll", money.minPayroll);

		const salaryCap = g.get("salaryCap");
		const minContract = g.get("minContract");

		const teams = [];
		const players = [];
		for (let tid = 0; tid < NUM_TEAMS; tid++) {
			// Real generated teams, not plain objects: the mood system prices a
			// player's willingness partly on the facilities a team has been
			// paying for, and reads initialBudget to do it.
			teams.push({
				...team.generate({
					tid,
					cid: tid % 2,
					did: tid % 2,
					region: `R${tid}`,
					name: `N${tid}`,
					abbrev: `T${tid}`,
					pop: 2,
					imgURL: "",
				} as any),
				strategy: tid % 2 === 0 ? "contending" : "rebuilding",
			});
			// Payrolls spread across the cap, so some teams have room, some are
			// capped out, and - under the tighter tax line below - some are deep
			// into the tax while others are not.
			const share = 0.45 + 0.1 * tid;
			for (let i = 0; i < 9; i++) {
				players.push(
					makePlayer({
						tid,
						ovr: 44 + ((tid * 3 + i) % 9),
						age: 24 + (i % 6),
						pos: i % 3 === 0 ? "PG" : i % 3 === 1 ? "SF" : "C",
						amount: Math.round((salaryCap * share) / 9),
						// Two thirds of every roster is expiring, so the re-signing
						// phase has real decisions to make rather than none.
						exp: g.get("season") + (i % 3 === 0 ? 1 : 0),
					}),
				);
			}
			// A player worth fighting to keep, on an expiring deal. Retention only
			// gets interesting - the overpay ladder, the ceilings that used to
			// include the tax line - when the man leaving is one the team wants.
			players.push(
				makePlayer({
					tid,
					ovr: 70,
					age: 28,
					pos: "SF",
					amount: Math.round(salaryCap * 0.22),
					exp: g.get("season"),
				}),
			);
		}

		// A field worth spending on: one star, a band of starters, and filler.
		players.push(
			makePlayer({
				tid: PLAYER.FREE_AGENT,
				ovr: 74,
				age: 27,
				pos: "SF",
				amount: Math.round(salaryCap * 0.3),
				exp: g.get("season") + 3,
			}),
		);
		for (let i = 0; i < 16; i++) {
			players.push(
				makePlayer({
					tid: PLAYER.FREE_AGENT,
					ovr: 55 - (i % 6),
					age: 25 + (i % 7),
					pos: i % 3 === 0 ? "PG" : i % 3 === 1 ? "SF" : "C",
					amount: Math.round(salaryCap * (0.05 + 0.02 * (i % 5))),
					exp: g.get("season") + 2,
				}),
			);
		}
		for (let i = 0; i < 8; i++) {
			players.push(
				makePlayer({
					tid: PLAYER.FREE_AGENT,
					ovr: 42,
					age: 29,
					pos: "SG",
					amount: minContract,
					exp: g.get("season") + 1,
				}),
			);
		}

		await resetCache({ players, teams });
		stubLeagueDb();
		for (let tid = 0; tid < NUM_TEAMS; tid++) {
			await idb.cache.teamSeasons.add({
				...team.genSeasonRow((await idb.cache.teams.get(tid))!),
				tid,
				season: g.get("season"),
				won: tid % 2 === 0 ? 58 : 17,
				lost: tid % 2 === 0 ? 24 : 65,
				gp: 82,
				// The other thing a business would look at.
				cash: money.cash,
			} as any);
		}

		// Re-signing first, then free agency - the real offseason order. This is
		// where the tax ceiling used to live, so it is the likeliest place for a
		// budget check to reappear.
		g.setWithoutSavingToDB("phase", PHASE.RESIGN_PLAYERS);
		await newPhaseResignPlayers({} as any);

		g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
		for (let day = 30; day > 0; day--) {
			g.setWithoutSavingToDB("daysLeft", day);
			await decreaseDemands();
			await clearSpaceForSignings();
			await autoSign();
		}

		// Who ended up where, and on what. Contracts are included because a
		// budget-minded AI would not just sign different players, it would pay
		// them differently.
		const out: string[] = [];
		let rostered = 0;
		for (let tid = 0; tid < NUM_TEAMS; tid++) {
			const roster = await idb.cache.players.indexGetAll("playersByTid", tid);
			rostered += roster.length;
			out.push(
				`${tid}:${roster
					.map((p) => `${p.pid}@${p.contract.amount}x${p.contract.exp}`)
					.sort()
					.join(",")}`,
			);
		}
		return { snapshot: out.join("\n"), signings: rostered - NUM_TEAMS * 10 };
	} finally {
		spy.mockRestore();
	}
};

describe("an AI front office spends to win, not to profit", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	const cap = 90_000;

	// Two arms are only the same if they both actually did something. Without
	// this, a fixture that signed nobody would pass every invariance test.
	const assertSameDecisions = (
		a: { snapshot: string; signings: number },
		b: { snapshot: string; signings: number },
	) => {
		assert.isAbove(
			a.signings,
			5,
			"the fixture signed almost nobody, so this compares two empty leagues",
		);
		assert.strictEqual(a.snapshot, b.snapshot);
	};

	// Deep in the tax for everyone vs nobody anywhere near it. A team that
	// cared would sign fewer players, or cheaper ones, in the first arm.
	test("the luxury tax line changes nothing", async () => {
		const taxed = await runOffseason({
			salaryCap: cap,
			luxuryPayroll: 1,
			minPayroll: 0,
			cash: 10_000,
		});
		const untaxed = await runOffseason({
			salaryCap: cap,
			luxuryPayroll: cap * 100,
			minPayroll: 0,
			cash: 10_000,
		});
		assertSameDecisions(taxed, untaxed);
	});

	// The floor is the mirror image: a team that wanted to dodge the penalty
	// would spend UP to it. Whether it is unreachable or already cleared must
	// not matter either.
	test("the spending floor changes nothing", async () => {
		const highFloor = await runOffseason({
			salaryCap: cap,
			luxuryPayroll: cap,
			minPayroll: cap * 10,
			cash: 10_000,
		});
		const noFloor = await runOffseason({
			salaryCap: cap,
			luxuryPayroll: cap,
			minPayroll: 0,
			cash: 10_000,
		});
		assertSameDecisions(highFloor, noFloor);
	});

	test("being broke changes nothing", async () => {
		const broke = await runOffseason({
			salaryCap: cap,
			luxuryPayroll: cap,
			minPayroll: 0,
			cash: -500_000,
		});
		const rich = await runOffseason({
			salaryCap: cap,
			luxuryPayroll: cap,
			minPayroll: 0,
			cash: 5_000_000,
		});
		assertSameDecisions(broke, rich);
	});

	// THE CONTROL. The cap is a rule, and the AI is supposed to be sharp about
	// it. If moving the cap does not move the league, the three tests above are
	// only proving that this harness is blind.
	test("but the salary cap changes plenty", async () => {
		const tight = await runOffseason({
			salaryCap: cap,
			luxuryPayroll: cap,
			minPayroll: 0,
			cash: 10_000,
		});
		const loose = await runOffseason({
			salaryCap: cap * 3,
			luxuryPayroll: cap * 3,
			minPayroll: 0,
			cash: 10_000,
		});
		assert.notStrictEqual(tight.snapshot, loose.snapshot);
	});
});
