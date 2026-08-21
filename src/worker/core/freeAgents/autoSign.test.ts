import { assert, beforeEach, describe, test, vi } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { PHASE, PLAYER } from "../../../common/constants.ts";
import { player, team } from "../index.ts";
import autoSign from "./autoSign.ts";
import {
	MAX_PURSUERS_PER_PRIZE,
	PURSUIT_PRICE_PATIENCE,
} from "./frontOffice.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { changeTracker } from "../../db/changeTracker.ts";

// A small league whose free agent pool contains one obvious prize and a lot of
// merely-useful players, which is the situation the old code handled worst: the
// team with the money spent it before the prize's price came down.
const NUM_TEAMS = 6;

const makePlayer = ({
	tid,
	ovr,
	pot = ovr,
	age = 27,
	pos = "SF",
	amount,
	exp,
}: {
	tid: number;
	ovr: number;
	pot?: number;
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
	ratings.pot = pot;
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

const setup = async ({
	prizeAmount,
	rosterSalaryFraction,
}: {
	prizeAmount: number;
	// Existing payroll as a share of the cap, so every team starts with real
	// room to either use well or waste.
	rosterSalaryFraction: number;
}) => {
	resetG();
	g.setWithoutSavingToDB("numTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("numActiveTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
	g.setWithoutSavingToDB("daysLeft", 30);
	g.setWithoutSavingToDB("userTids", [999]);
	g.setWithoutSavingToDB("salaryCapType", "soft");
	// Faces are irrelevant here and their generation draws from Math.random,
	// which these tests replace with a seeded sequence - leaving it on would
	// make every face feature shift the stream the economics run on.
	g.setWithoutSavingToDB("realisticFaces", false);

	const salaryCap = g.get("salaryCap");
	const minContract = g.get("minContract");
	const rosterSalary = Math.round(salaryCap * rosterSalaryFraction);

	const teams = [];
	const players = [];
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		teams.push({
			tid,
			disabled: false,
			strategy: "contending",
			depth: undefined,
		});
		// Eight rostered players, so every team has room to add without being
		// forced into a signing by roster minimums.
		for (let i = 0; i < 8; i++) {
			players.push(
				makePlayer({
					tid,
					ovr: 45 + ((tid + i) % 7),
					age: 25,
					pos: i % 3 === 0 ? "PG" : i % 3 === 1 ? "SF" : "C",
					amount: Math.round(rosterSalary / 8),
					exp: g.get("season") + 2,
				}),
			);
		}
	}

	// The prize: a genuine star, asking for real money.
	const prize = makePlayer({
		tid: PLAYER.FREE_AGENT,
		ovr: 75,
		pot: 75,
		age: 27,
		pos: "SF",
		amount: prizeAmount,
		exp: g.get("season") + 3,
	});
	players.push(prize);

	// A deep field of decent players who, between them, could eat every dollar
	// of anybody's cap space.
	for (let i = 0; i < 20; i++) {
		players.push(
			makePlayer({
				tid: PLAYER.FREE_AGENT,
				ovr: 52 - (i % 5),
				age: 28,
				pos: i % 3 === 0 ? "PG" : i % 3 === 1 ? "SF" : "C",
				amount: Math.round(salaryCap * 0.18),
				exp: g.get("season") + 2,
			}),
		);
	}
	// Plus minimum-salary filler, so a waiting team can still add bodies.
	for (let i = 0; i < 10; i++) {
		players.push(
			makePlayer({
				tid: PLAYER.FREE_AGENT,
				ovr: 40,
				age: 29,
				pos: "SG",
				amount: minContract,
				exp: g.get("season") + 1,
			}),
		);
	}

	await resetCache({ players, teams });
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		await idb.cache.teamSeasons.add({
			...team.genSeasonRow((await idb.cache.teams.get(tid))!),
			tid,
			season: g.get("season"),
			won: 41,
			lost: 41,
			gp: 82,
		} as any);
	}

	return { prize, salaryCap, minContract };
};

describe("AI free agency runs on a plan", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	// The headline behavior, isolated: the star is asking MORE than anyone can
	// pay today, so nobody can simply sign him and be done. The only question
	// the test asks is what teams do with their money while he sits there - and
	// the old answer was "spend it on the best of the rest, immediately".
	//
	// Math.random is pinned so every team acts every day and the order is fixed;
	// the point is what teams CHOOSE, not how often they are asked.
	test("teams keep their powder dry for a marquee free agent", async () => {
		const { salaryCap } = await setup({
			// 100,000 against a 150,000 cap, with 60,000 already committed: out of
			// reach today, within reach once his price comes down.
			prizeAmount: 100_000,
			rosterSalaryFraction: 0.4,
		});

		const random = vi.spyOn(Math, "random").mockReturnValue(0.99);
		try {
			for (let day = 0; day < 10; day++) {
				await autoSign();
			}
		} finally {
			random.mockRestore();
		}

		// Room for the price they expect to actually pay (his ask, discounted).
		const expectedPrice = 100_000 * PURSUIT_PRICE_PATIENCE;
		let teamsWithRoom = 0;
		for (let tid = 0; tid < NUM_TEAMS; tid++) {
			const payroll = await team.getPayroll(tid);
			if (payroll + expectedPrice <= salaryCap) {
				teamsWithRoom += 1;
			}
		}
		assert.ok(
			teamsWithRoom >= MAX_PURSUERS_PER_PRIZE,
			`only ${teamsWithRoom} teams can still afford the star after ten days; expected at least ${MAX_PURSUERS_PER_PRIZE}`,
		);
	});

	test("a waiting team still fills out its roster with minimum deals", async () => {
		await setup({
			prizeAmount: 30_000,
			rosterSalaryFraction: 0.25,
		});

		const before = (
			await idb.cache.players.indexGetAll("playersByTid", [0, Infinity])
		).length;
		for (let day = 0; day < 10; day++) {
			await autoSign();
		}
		const after = (
			await idb.cache.players.indexGetAll("playersByTid", [0, Infinity])
		).length;
		assert.ok(
			after > before,
			"holding cap space must not freeze roster building",
		);
	});

	// A hold that never resolves would be worse than no hold at all, so it has to
	// expire on its own.
	test("with free agency nearly over, teams spend rather than keep waiting", async () => {
		const { salaryCap } = await setup({
			prizeAmount: 30_000,
			rosterSalaryFraction: 0.25,
		});
		g.setWithoutSavingToDB("daysLeft", 2);

		let payrollBefore = 0;
		for (let tid = 0; tid < NUM_TEAMS; tid++) {
			payrollBefore += await team.getPayroll(tid);
		}
		for (let day = 0; day < 8; day++) {
			await autoSign();
		}
		let payrollAfter = 0;
		for (let tid = 0; tid < NUM_TEAMS; tid++) {
			payrollAfter += await team.getPayroll(tid);
		}
		assert.ok(
			payrollAfter > payrollBefore,
			"a team out of time should be spending, not still holding",
		);
		assert.ok(payrollAfter > 0 && salaryCap > 0);
	});

	// Signing has to keep working when there is no posture to be had.
	test("in-season signings still happen", async () => {
		await setup({ prizeAmount: 30_000, rosterSalaryFraction: 0.25 });
		g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);

		const before = (
			await idb.cache.players.indexGetAll("playersByTid", [0, Infinity])
		).length;
		for (let day = 0; day < 10; day++) {
			await autoSign();
		}
		const after = (
			await idb.cache.players.indexGetAll("playersByTid", [0, Infinity])
		).length;
		assert.ok(after > before);
	});
});
