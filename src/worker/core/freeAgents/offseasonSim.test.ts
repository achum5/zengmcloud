import { assert, beforeEach, describe, test, vi } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { PHASE, PLAYER } from "../../../common/constants.ts";
import { player, team } from "../index.ts";
import autoSign from "./autoSign.ts";
import clearSpaceForSignings from "./clearSpace.ts";
import decreaseDemands from "./decreaseDemands.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import {
	captureFrontOfficeLog,
	type FrontOfficeEntry,
} from "../../util/frontOfficeLog.ts";
import { getLeagueTradeContext, getTradePosture } from "../trade/tradePosture.ts";

// ---------------------------------------------------------------------------
// A repeatable offseason, run many times over, with every front-office decision
// recorded.
//
// This exercises the real code paths - decreaseDemands, clearSpaceForSignings,
// autoSign - against a real cache, and then ASSERTS ON THE AGGREGATE: are stars
// getting signed, are contenders getting the veterans and rebuilds the kids, is
// anybody bankrupting themselves, does the market clear. Those are the questions
// a handful of unit tests cannot answer.
//
// What it does not do is simulate games; team records are synthesized from
// roster strength. That is deliberate - the behavior under test is what teams do
// BETWEEN seasons, and driving a full game sim here would make the harness slow
// and flaky without testing anything extra.
// ---------------------------------------------------------------------------

const NUM_TEAMS = 12;
const FA_DAYS = 30;

// Deterministic PRNG so a failing run can be reproduced exactly from its seed.
const makeRng = (seed: number) => {
	let s = seed >>> 0;
	return () => {
		s = (s * 1_664_525 + 1_013_904_223) >>> 0;
		return s / 4_294_967_296;
	};
};

const makePlayer = ({
	tid,
	ovr,
	pot,
	age,
	pos,
	amount,
	exp,
}: {
	tid: number;
	ovr: number;
	pot: number;
	age: number;
	pos: string;
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
	ratings.pot = Math.max(ovr, pot);
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

const POSITIONS = ["PG", "SG", "SF", "PF", "C"];

const buildLeague = async (
	rng: () => number,
	// How much of the cap each team already has committed. The tight band is a
	// league mid-lifecycle, where everybody is capped out and the only way to
	// sign anyone is to move money first.
	payrollBand: [number, number] = [0.45, 0.9],
) => {
	resetG();
	g.setWithoutSavingToDB("numTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("numActiveTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
	g.setWithoutSavingToDB("daysLeft", FA_DAYS);
	// No user team: every team is run by the AI, which is the point.
	g.setWithoutSavingToDB("userTids", [-99]);
	g.setWithoutSavingToDB("salaryCapType", "soft");

	const salaryCap = g.get("salaryCap");
	const minContract = g.get("minContract");
	const maxContract = g.get("maxContract");

	const teams = [];
	const players = [];
	const draftPicks = [];
	let dpid = 0;

	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		teams.push(
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

		// Team strength varies across the league so postures spread across all
		// five tiers rather than everyone looking identical.
		const strength = rng();
		const targetPayrollFraction =
			payrollBand[0] + rng() * (payrollBand[1] - payrollBand[0]);
		let budget = salaryCap * targetPayrollFraction;

		for (let i = 0; i < 10; i++) {
			const ovr = Math.round(
				42 + strength * 20 + (i < 3 ? 14 : 0) - i * 1.5 + rng() * 6,
			);
			const age = Math.round(21 + rng() * 14);
			const share = i < 3 ? 0.22 : 0.05;
			const amount = Math.max(
				minContract,
				Math.min(maxContract, Math.round(budget * share)),
			);
			budget -= amount;
			players.push(
				makePlayer({
					tid,
					ovr,
					pot: ovr + Math.round(rng() * 15),
					age,
					pos: POSITIONS[i % POSITIONS.length]!,
					amount,
					exp: g.get("season") + 1 + Math.floor(rng() * 3),
				}),
			);
		}

		for (let round = 1; round <= 2; round++) {
			draftPicks.push({
				dpid: dpid++,
				tid,
				originalTid: tid,
				round,
				pick: 0,
				season: g.get("season") + 1,
			});
		}
	}

	// The free agent market: a couple of genuine stars, a band of starters, and
	// minimum-salary filler.
	for (let i = 0; i < 3; i++) {
		players.push(
			makePlayer({
				tid: PLAYER.FREE_AGENT,
				ovr: 68 + Math.round(rng() * 7),
				pot: 72,
				age: 26 + Math.round(rng() * 4),
				pos: POSITIONS[Math.floor(rng() * POSITIONS.length)]!,
				amount: Math.round(salaryCap * (0.25 + rng() * 0.12)),
				exp: g.get("season") + 3,
			}),
		);
	}
	for (let i = 0; i < 25; i++) {
		players.push(
			makePlayer({
				tid: PLAYER.FREE_AGENT,
				ovr: 44 + Math.round(rng() * 12),
				pot: 52 + Math.round(rng() * 18),
				age: 22 + Math.round(rng() * 14),
				pos: POSITIONS[Math.floor(rng() * POSITIONS.length)]!,
				amount: Math.round(salaryCap * (0.05 + rng() * 0.14)),
				exp: g.get("season") + 1 + Math.floor(rng() * 3),
			}),
		);
	}
	for (let i = 0; i < 20; i++) {
		players.push(
			makePlayer({
				tid: PLAYER.FREE_AGENT,
				ovr: 33 + Math.round(rng() * 8),
				pot: 40,
				age: 24 + Math.round(rng() * 12),
				pos: POSITIONS[Math.floor(rng() * POSITIONS.length)]!,
				amount: minContract,
				exp: g.get("season") + 1,
			}),
		);
	}

	await resetCache({ players, teams, draftPicks });
	// processTrade reaches through to the league DB for team abbrevs; without a
	// stub every trade throws and the whole feature looks inert from the outside.
	// The shared mockIDBLeague has no index().get, which is the call this path
	// makes, so use a slightly fuller stub rather than change it for everyone.
	const emptyStore: any = {
		get: async () => undefined,
		getAll: async () => [],
		put: async () => undefined,
		 
		async *iterate () {},
		index: () => emptyStore,
	};
	idb.league = {
		get: async () => undefined,
		getAll: async () => [],
		transaction: () => ({
			store: emptyStore,
			objectStore: () => emptyStore,
			done: Promise.resolve(),
		}),
	} as any;

	// Records consistent with roster strength, so postures are meaningful.
	const context = await getLeagueTradeContext();
	for (const t of teams) {
		const rank = context.teamOvrsSorted.findIndex((x) => x.tid === t.tid);
		const winp = 0.8 - (0.6 * rank) / Math.max(1, NUM_TEAMS - 1);
		// A REAL season row: mood (and so willingness to sign) reads a team's
		// spending history off it, so a hand-rolled stub makes moodInfo throw.
		const row: any = team.genSeasonRow(t);
		row.won = Math.round(82 * winp);
		row.lost = 82 - row.won;
		row.gp = 82;
		await idb.cache.teamSeasons.add(row);
	}

	return { salaryCap, minContract };
};

type Report = {
	seed: number;
	starsSigned: number;
	starsAvailable: number;
	dumpAndSign: number;
	dumpFailures: number;
	teamsOverCap: number;
	teamsUnderMin: number;
	shortRosters: number;
	unsignedUseful: number;
	contenderAvgAgeAdded: number | undefined;
	rebuildAvgAgeAdded: number | undefined;
};

const runOneOffseason = async (
	seed: number,
	payrollBand?: [number, number],
): Promise<{
	report: Report;
	entries: FrontOfficeEntry[];
}> => {
	const rng = makeRng(seed);
	const { salaryCap } = await buildLeague(rng, payrollBand);

	const capture = captureFrontOfficeLog();

	const starPids = new Set(
		(await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT))
			.filter((p) => p.ratings.at(-1)!.ovr >= 65)
			.map((p) => p.pid),
	);
	const starsAvailable = starPids.size;

	// Who each team was before free agency, so we can measure what it added.
	const context = await getLeagueTradeContext();
	const tierByTid = new Map<number, string>();
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		tierByTid.set(tid, (await getTradePosture(tid, context)).tier);
	}
	const rosterBefore = new Map<number, Set<number>>();
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		rosterBefore.set(
			tid,
			new Set(
				(await idb.cache.players.indexGetAll("playersByTid", tid)).map(
					(p) => p.pid,
				),
			),
		);
	}

	// Math.random drives the skip rolls and team shuffle; pin it to this run's
	// stream so a failure is reproducible from the seed alone.
	const randomSpy = vi.spyOn(Math, "random").mockImplementation(rng);
	try {
		for (let day = FA_DAYS; day > 0; day--) {
			g.setWithoutSavingToDB("daysLeft", day);
			await decreaseDemands();
			await clearSpaceForSignings();
			await autoSign();
		}
	} finally {
		randomSpy.mockRestore();
	}

	const entries = capture.stop();

	let starsSigned = 0;
	for (const pid of starPids) {
		const p = await idb.cache.players.get(pid);
		if (p && p.tid >= 0) {
			starsSigned += 1;
		}
	}

	let teamsOverCap = 0;
	let shortRosters = 0;
	const addedAges: { tier: string; age: number }[] = [];
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		const payroll = await team.getPayroll(tid);
		if (payroll > salaryCap * 1.35) {
			teamsOverCap += 1;
		}
		const roster = await idb.cache.players.indexGetAll("playersByTid", tid);
		if (roster.length < g.get("minRosterSize")) {
			shortRosters += 1;
		}
		const before = rosterBefore.get(tid)!;
		for (const p of roster) {
			if (!before.has(p.pid)) {
				addedAges.push({
					tier: tierByTid.get(tid)!,
					age: g.get("season") - p.born.year,
				});
			}
		}
	}

	const remainingFas = await idb.cache.players.indexGetAll(
		"playersByTid",
		PLAYER.FREE_AGENT,
	);
	const unsignedUseful = remainingFas.filter(
		(p) => p.ratings.at(-1)!.ovr >= 55,
	).length;

	const avgAgeFor = (tiers: string[]) => {
		const xs = addedAges.filter((a) => tiers.includes(a.tier));
		return xs.length >= 3
			? xs.reduce((total, a) => total + a.age, 0) / xs.length
			: undefined;
	};

	return {
		report: {
			seed,
			starsSigned,
			starsAvailable,
			dumpAndSign: entries.filter((e) => e.event === "dump-and-sign").length,
			dumpFailures: entries.filter((e) => e.event.startsWith("dump-no")).length,
			teamsOverCap,
			teamsUnderMin: 0,
			shortRosters,
			unsignedUseful,
			contenderAvgAgeAdded: avgAgeFor(["allIn", "buyer"]),
			rebuildAvgAgeAdded: avgAgeFor(["seller", "teardown"]),
		},
		entries,
	};
};

describe("many simulated offseasons", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	const runScenario = async (
		label: string,
		payrollBand: [number, number] | undefined,
		runs: number,
	) => {
		const reports: Report[] = [];
		const allEntries: FrontOfficeEntry[] = [];
		for (let seed = 1; seed <= runs; seed++) {
			const { report, entries } = await runOneOffseason(seed, payrollBand);
			reports.push(report);
			allEntries.push(...entries);
		}

		const sum = (pick: (r: Report) => number) =>
			reports.reduce((total, r) => total + pick(r), 0);
		const avg = (pick: (r: Report) => number | undefined) => {
			const xs = reports.map(pick).filter((x): x is number => x !== undefined);
			return xs.length
				? Math.round((xs.reduce((a, x) => a + x, 0) / xs.length) * 100) / 100
				: undefined;
		};

		const starsSigned = sum((r) => r.starsSigned);
		const starsAvailable = sum((r) => r.starsAvailable);
		console.log(
			[
				"",
				`=== ${label} (${runs} runs, ${NUM_TEAMS} teams, ${FA_DAYS} FA days each) ===`,
				`stars signed:             ${starsSigned}/${starsAvailable} (${Math.round((100 * starsSigned) / starsAvailable)}%)`,
				`dump-and-sign deals:      ${sum((r) => r.dumpAndSign)}`,
				`dump attempts abandoned:  ${sum((r) => r.dumpFailures)}`,
				`runs w/ a blown payroll:  ${reports.filter((r) => r.teamsOverCap > 0).length}`,
				`runs w/ a short roster:   ${reports.filter((r) => r.shortRosters > 0).length}`,
				`useful FAs left unsigned: ${sum((r) => r.unsignedUseful)}`,
				`avg age added, contender: ${avg((r) => r.contenderAvgAgeAdded)}`,
				`avg age added, rebuild:   ${avg((r) => r.rebuildAvgAgeAdded)}`,
				"decision counts:",
				...[...new Set(allEntries.map((e) => e.event))]
					.sort()
					.map(
						(event) =>
							`  ${event}: ${allEntries.filter((e) => e.event === event).length}`,
					),
				...(() => {
					const t = allEntries.filter((e) => e.event === "dump-no-target");
					if (t.length === 0) {return [];}
					const tot = (k: string) =>
						t.reduce((a, e) => a + ((e.data[k] as number) ?? 0), 0);
					return [
						`  (no-target breakdown: prizes=${tot("prizes")} unwilling=${tot("unwilling")} affordable=${tot("affordable")})`,
					];
				})(),
				...(allEntries.some((e) => e.event === "dump-and-sign")
					? [
							"sample dump-and-sign deals:",
							...allEntries
								.filter((e) => e.event === "dump-and-sign")
								.slice(0, 6)
								.map((e) => `  tid ${e.tid}: ${JSON.stringify(e.data)}`),
						]
					: []),
				"",
			].join("\n"),
		);

		return { reports, allEntries, sum, avg, starsSigned, starsAvailable };
	};

	// A normal league: plenty of teams with room, so free agency should mostly
	// just work and nobody should get stuck.
	test(
		"a healthy market clears, and teams shop to type",
		async () => {
			const { sum, avg, starsSigned, starsAvailable } = await runScenario(
				"HEALTHY MARKET",
				undefined,
				25,
			);

			// A market that leaves its best players unsigned is broken - the failure
			// mode cap holds could plausibly cause, so it is checked first.
			assert.ok(
				starsSigned / starsAvailable >= 0.8,
				`only ${starsSigned}/${starsAvailable} stars found teams`,
			);

			// The two ways "hold your cap space" could go badly wrong.
			assert.strictEqual(sum((r) => r.teamsOverCap), 0, "a team blew past the cap");
			assert.strictEqual(
				sum((r) => r.shortRosters),
				0,
				"a team could not fill its roster",
			);

			// Win-now teams older, rebuilds younger - the "legitimate direction" claim.
			const contenderAge = avg((r) => r.contenderAvgAgeAdded);
			const rebuildAge = avg((r) => r.rebuildAvgAgeAdded);
			if (contenderAge !== undefined && rebuildAge !== undefined) {
				assert.ok(
					contenderAge > rebuildAge,
					`contenders added avg age ${contenderAge}, rebuilds ${rebuildAge} - expected contenders older`,
				);
			}
		},
		600_000,
	);

	// A capped-out league, which is where the interesting move lives: everyone is
	// already committed, so the only way to add a star is to pay someone to take
	// a contract off your hands first.
	test(
		"capped-out teams trade to clear room, and come out ahead doing it",
		async () => {
			const { sum, allEntries } = await runScenario(
				"CAPPED-OUT MARKET",
				[0.85, 1.05],
				25,
			);

			// Several a season across the league, not one freak occurrence, and not
			// so many that every roster is being churned for cap room.
			const deals = sum((r) => r.dumpAndSign);
			assert.ok(
				deals >= 5,
				`only ${deals} teams ever cleared space to sign anyone - the feature is close to dead code`,
			);
			assert.ok(
				deals <= 100,
				`${deals} dump-and-sign deals across 25 offseasons is a churn factory`,
			);

			// The deal may be lopsided; the transaction must not be. Every dump has
			// to be covered by what the signing is worth.
			for (const e of allEntries.filter((x) => x.event === "dump-and-sign")) {
				assert.ok(
					(e.data.signingGain as number) > (e.data.dumpCost as number),
					`a dump cost more than the signing was worth: ${JSON.stringify(e.data)}`,
				);
			}

			// Still no self-destruction.
			assert.strictEqual(sum((r) => r.teamsOverCap), 0, "a team blew past the cap");
			assert.strictEqual(
				sum((r) => r.shortRosters),
				0,
				"a team could not fill its roster",
			);
		},
		600_000,
	);
});
