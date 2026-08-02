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

// ---------------------------------------------------------------------------
// Determinism, and settings nobody sane would pick.
//
// DETERMINISM is a sync property, not a tidiness one. The same league driven by
// the same random stream has to land in the same place every time, or a replay
// diverges from the run it is replaying. Ordering is where this usually breaks:
// a Map iterated in hash order, a sort with no tiebreak, a Set of tids.
//
// The DEGENERATE SETTINGS exist because God Mode lets a league be configured
// into shapes the model never anticipated - no slack between the roster minimum
// and maximum, a cap below what one player costs, every free agent priced
// identically. None of these should be GOOD. They must merely not hang, throw,
// or corrupt a roster.
// ---------------------------------------------------------------------------

const NUM_TEAMS = 8;
const FA_DAYS = 15;
const POSITIONS = ["PG", "SG", "SF", "PF", "C"];

const makeRng = (seed: number) => {
	let s = seed >>> 0;
	return () => {
		s = (s * 1_664_525 + 1_013_904_223) >>> 0;
		return s / 4_294_967_296;
	};
};

const stubLeagueDb = () => {
	const store: any = {
		get: async () => undefined,
		getAll: async () => [],
		put: async () => undefined,
		async *iterate() {},
		index: () => store,
	};
	idb.league = {
		get: async () => undefined,
		getAll: async () => [],
		transaction: () => ({
			store,
			objectStore: () => store,
			done: Promise.resolve(),
		}),
	} as any;
};

type Tweaks = {
	rosterSize?: number;
	faAmount?: (minContract: number, rng: () => number) => number;
	gameAttributes?: Record<string, any>;
};

const build = async (rng: () => number, tweaks: Tweaks = {}) => {
	resetG();
	g.setWithoutSavingToDB("numTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("numActiveTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
	g.setWithoutSavingToDB("userTids", [-99]);
	g.setWithoutSavingToDB("salaryCapType", "soft");
	g.setWithoutSavingToDB("smartAiFrontOffice", true);
	for (const [key, value] of Object.entries(tweaks.gameAttributes ?? {})) {
		g.setWithoutSavingToDB(key as any, value);
	}

	const salaryCap = g.get("salaryCap");
	const minContract = g.get("minContract");
	const rosterSize = tweaks.rosterSize ?? 12;
	const teams: any[] = [];
	const players: any[] = [];
	const draftPicks: any[] = [];

	const mk = (tid: number, ovr: number, pos: string, amount: number) => {
		const age = Math.round(22 + rng() * 12);
		const p: any = player.generate(
			tid,
			age,
			g.get("season") - age,
			true,
			DEFAULT_LEVEL,
		);
		const r = p.ratings.at(-1);
		r.ovr = ovr;
		r.pot = Math.max(ovr, ovr + Math.round(rng() * 10));
		r.pos = pos;
		p.born.year = g.get("season") - age;
		p.contract = { amount, exp: g.get("season") + Math.floor(rng() * 4) };
		p.injury = { type: "Healthy", gamesRemaining: 0 };
		p.value = ovr;
		p.valueNoPot = ovr;
		p.valueFuzz = ovr;
		p.valueNoPotFuzz = ovr;
		return p;
	};

	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		teams.push(
			team.generate({
				tid,
				cid: tid % 2,
				did: tid % 2,
				region: `R${tid}`,
				name: `T${tid}`,
				abbrev: `T${tid}`,
				pop: 3,
				popRank: tid + 1,
				strategy: "contending",
			}),
		);
		const strength = rng();
		let budget = salaryCap * (0.55 + rng() * 0.35);
		for (let i = 0; i < rosterSize; i++) {
			const amount = Math.max(
				minContract,
				Math.round(budget * (i < 3 ? 0.2 : 0.045)),
			);
			budget -= amount;
			players.push(
				mk(
					tid,
					Math.round(42 + strength * 20 + (i < 3 ? 14 : 0) - i + rng() * 6),
					POSITIONS[i % POSITIONS.length]!,
					amount,
				),
			);
		}
		for (const round of [1, 2]) {
			draftPicks.push({
				dpid: draftPicks.length,
				tid,
				originalTid: tid,
				round,
				pick: 0,
				season: g.get("season") + 1,
			});
		}
	}

	const faAmount =
		tweaks.faAmount ??
		((min: number, r: () => number) =>
			Math.max(min, Math.round(min * (1 + r() * 14))));
	for (let i = 0; i < NUM_TEAMS * 5; i++) {
		players.push(
			mk(
				PLAYER.FREE_AGENT,
				Math.round(40 + rng() * 30),
				POSITIONS[i % POSITIONS.length]!,
				faAmount(minContract, rng),
			),
		);
	}

	await resetCache({ players, teams, draftPicks });
	stubLeagueDb();
};

const runFreeAgency = async (seed: number, tweaks: Tweaks = {}) => {
	// The spy covers construction too, so both runs of a determinism check get
	// byte-identical leagues - player.generate reaches the global RNG.
	const rng = makeRng(seed);
	const spy = vi.spyOn(Math, "random").mockImplementation(rng);
	try {
		await build(rng, tweaks);
		for (let day = FA_DAYS; day > 0; day--) {
			g.setWithoutSavingToDB("daysLeft", day);
			await decreaseDemands();
			await clearSpaceForSignings();
			await autoSign();
		}

		const rosters: string[] = [];
		for (let tid = 0; tid < NUM_TEAMS; tid++) {
			const roster = await idb.cache.players.indexGetAll("playersByTid", tid);
			rosters.push(
				roster
					.map((p) => p.pid)
					.sort((a, b) => a - b)
					.join("."),
			);
		}
		return rosters.join("|");
	} finally {
		spy.mockRestore();
	}
};

describe("determinism and degenerate settings", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	test("the same league and the same random stream replay identically", async () => {
		// If this ever fails, suspect ordering: a Map or Set iterated somewhere
		// that assumes insertion order it does not control, or a sort whose
		// comparator leaves ties unresolved.
		const first = await runFreeAgency(1337);
		const second = await runFreeAgency(1337);
		assert.strictEqual(
			first,
			second,
			"free agency produced different rosters from an identical starting point and an identical random stream - a replay of this run would diverge from it",
		);
		assert.ok(first.length > 0);
	}, 300_000);

	test("a different stream really does produce a different league", async () => {
		// Otherwise the test above would pass on a system that had stopped doing
		// anything at all.
		const a = await runFreeAgency(1337);
		const b = await runFreeAgency(4242);
		assert.notStrictEqual(
			a,
			b,
			"two unrelated seeds gave identical rosters, so the determinism check above is not measuring anything",
		);
	}, 300_000);

	test("no slack between the roster minimum and maximum", async () => {
		// getBest reasons in terms of maxRosterSize - 2, and the stripped-roster
		// rule in terms of minRosterSize. Collapse the gap and those two windows
		// overlap in a way neither was written for.
		await assertSurvives({
			rosterSize: 12,
			gameAttributes: { minRosterSize: 12, maxRosterSize: 12 },
		});
	}, 300_000);

	test("a salary cap below the price of a single player", async () => {
		await assertSurvives({
			gameAttributes: { salaryCap: 5000, minContract: 1500, maxContract: 4000 },
		});
	}, 300_000);

	test("every free agent priced identically", async () => {
		// Kills every tiebreak that leans on price, including the minimum-contract
		// exemptions threaded through the cap-hold filter.
		await assertSurvives({
			faAmount: (min: number) => min,
		});
	}, 300_000);

	test("a league with no free agents at all", async () => {
		const rng = makeRng(7);
		const spy = vi.spyOn(Math, "random").mockImplementation(rng);
		try {
			await build(rng);
			for (const p of await idb.cache.players.indexGetAll(
				"playersByTid",
				PLAYER.FREE_AGENT,
			)) {
				await idb.cache.players.delete(p.pid);
			}
			for (let day = 3; day > 0; day--) {
				g.setWithoutSavingToDB("daysLeft", day);
				await decreaseDemands();
				await clearSpaceForSignings();
				await autoSign();
			}
		} finally {
			spy.mockRestore();
		}
	}, 300_000);

	// Shared body: run it, then insist the league is still coherent. "Survives"
	// means no throw, no hang, and no roster left in a state the game could not
	// render - not that the decisions were good, which is not knowable for
	// settings like these.
	async function assertSurvives(tweaks: Tweaks) {
		const rng = makeRng(555);
		const spy = vi.spyOn(Math, "random").mockImplementation(rng);
		try {
			await build(rng, tweaks);
			for (let day = FA_DAYS; day > 0; day--) {
				g.setWithoutSavingToDB("daysLeft", day);
				await decreaseDemands();
				await clearSpaceForSignings();
				await autoSign();
			}
		} finally {
			spy.mockRestore();
		}

		// Free agency is ALLOWED to overshoot the roster limit - getBest signs
		// non-minimum players without consulting it at all ("will drop bad player
		// if necessary in checkRosterSizes"), and stock BBGM overshoots here too.
		// So the limit is only meaningful after the game's own trim runs, which is
		// what the real code does before games are played.
		await team.checkRosterSizes("other");

		const maxRosterSize = g.get("maxRosterSize");
		const minRosterSize = g.get("minRosterSize");
		const seen = new Set<number>();
		for (let tid = 0; tid < NUM_TEAMS; tid++) {
			const roster = await idb.cache.players.indexGetAll("playersByTid", tid);
			assert.ok(
				roster.length <= maxRosterSize,
				`team ${tid} still had ${roster.length} players after checkRosterSizes, over the maximum of ${maxRosterSize}`,
			);
			assert.ok(
				roster.length >= minRosterSize,
				`team ${tid} could not be filled to the minimum of ${minRosterSize} (had ${roster.length})`,
			);
			for (const p of roster) {
				assert.ok(
					!seen.has(p.pid),
					`player ${p.pid} is on more than one roster`,
				);
				seen.add(p.pid);
				assert.ok(
					Number.isFinite(p.contract.amount),
					`player ${p.pid} ended with a non-finite contract`,
				);
			}
			const payroll = await team.getPayroll(tid);
			assert.ok(
				Number.isFinite(payroll),
				`team ${tid} ended with a non-finite payroll`,
			);
		}
	}
});
