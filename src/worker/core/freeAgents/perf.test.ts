import { beforeEach, describe, test } from "vitest";
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
// How long does a full free agency period actually take?
//
// Every other test here asks whether the AI makes good decisions. This one asks
// what they COST, because the answer is not obviously small: autoSign and
// clearSpaceForSignings each rebuild the league trade context and a posture for
// every team, and they are called once per team per day for thirty days. In a
// real 30-team league with a full player pool that is a lot of work hidden
// behind a single "play" click, and nothing else in the suite would notice it
// getting slower.
// ---------------------------------------------------------------------------

const NUM_TEAMS = 30;
const ROSTER = 14;
const FA_POOL = 120;
const FA_DAYS = 30;

const makeRng = (seed: number) => {
	let s = seed >>> 0;
	return () => {
		s = (s * 1_664_525 + 1_013_904_223) >>> 0;
		return s / 4_294_967_296;
	};
};

const POSITIONS = ["PG", "SG", "SF", "PF", "C"];

const makePlayer = (
	rng: () => number,
	tid: number,
	ovr: number,
	amount: number,
) => {
	const age = Math.round(21 + rng() * 14);
	const p: any = player.generate(
		tid,
		age,
		g.get("season") - age,
		true,
		DEFAULT_LEVEL,
	);
	const r = p.ratings.at(-1);
	r.ovr = ovr;
	r.pot = Math.max(ovr, Math.round(ovr + rng() * 12));
	r.pos = POSITIONS[Math.floor(rng() * POSITIONS.length)]!;
	p.born.year = g.get("season") - age;
	p.contract = { amount, exp: g.get("season") + Math.floor(rng() * 4) };
	p.injury = { type: "Healthy", gamesRemaining: 0 };
	p.value = ovr;
	p.valueNoPot = ovr;
	p.valueFuzz = ovr;
	p.valueNoPotFuzz = ovr;
	return p;
};

describe("free agency performance at full league scale", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	const build = async (rng: () => number) => {
		resetG();
		g.setWithoutSavingToDB("numTeams", NUM_TEAMS);
		g.setWithoutSavingToDB("numActiveTeams", NUM_TEAMS);
		g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
		g.setWithoutSavingToDB("userTids", [-99]);
		g.setWithoutSavingToDB("salaryCapType", "soft");

		const salaryCap = g.get("salaryCap");
		const minContract = g.get("minContract");
		const teams: any[] = [];
		const players: any[] = [];
		const draftPicks: any[] = [];

		for (let tid = 0; tid < NUM_TEAMS; tid++) {
			teams.push(
				team.generate({
					tid,
					cid: tid % 2,
					did: tid % 4,
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
			for (let i = 0; i < ROSTER; i++) {
				const share = i < 3 ? 0.18 : 0.04;
				const amount = Math.max(minContract, Math.round(budget * share));
				budget -= amount;
				players.push(
					makePlayer(
						rng,
						tid,
						Math.round(40 + strength * 20 + (i < 3 ? 14 : 0) - i + rng() * 6),
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

		for (let i = 0; i < FA_POOL; i++) {
			players.push(
				makePlayer(
					rng,
					PLAYER.FREE_AGENT,
					Math.round(38 + rng() * 28),
					Math.max(minContract, Math.round(minContract * (1 + rng() * 12))),
				),
			);
		}

		await resetCache({ players, teams, draftPicks });
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

	const runFreeAgency = async (smart: boolean) => {
		await build(makeRng(9001));
		g.setWithoutSavingToDB("smartAiFrontOffice", smart);

		const start = performance.now();
		for (let day = FA_DAYS; day > 0; day--) {
			g.setWithoutSavingToDB("daysLeft", day);
			await decreaseDemands();
			await clearSpaceForSignings();
			await autoSign();
		}
		return performance.now() - start;
	};

	test("a full 30-team free agency period stays fast", async () => {
		// Vanilla first, so the smart run cannot benefit from a warm cache the
		// baseline never had.
		const vanillaMs = await runFreeAgency(false);
		const smartMs = await runFreeAgency(true);

		console.log(
			[
				"",
				`=== 30 teams, ${ROSTER}-man rosters, ${FA_POOL} free agents, ${FA_DAYS} days ===`,
				`vanilla: ${vanillaMs.toFixed(0)}ms`,
				`smart:   ${smartMs.toFixed(0)}ms  (${(smartMs / vanillaMs).toFixed(1)}x)`,
				`per day: ${(smartMs / FA_DAYS).toFixed(1)}ms`,
				"",
			].join("\n"),
		);

		// A ceiling in absolute wall-clock, not a ratio. Vanilla free agency is so
		// cheap that a ratio would trip on noise, and what a player actually feels
		// is the total: this is one click, and the phase change does plenty else
		// besides. CI machines vary, so the bar is loose enough not to flake but
		// tight enough to catch an order-of-magnitude regression.
		if (smartMs > 20_000) {
			throw new Error(
				`free agency took ${smartMs.toFixed(0)}ms for one offseason - too slow to hide behind a phase change`,
			);
		}
	}, 300_000);

	test("the daily in-season overhead is invisible next to a game sim", async () => {
		// The test above prices the offseason; this prices the season. autoSign
		// runs after EVERY regular-season sim day (game/play.ts), and with smart on
		// it rebuilds the league context and thirty postures each time - BEFORE the
		// skip roll, so the cost is paid even on the ~60-90% of days a team then
		// does nothing. Vanilla pays nearly nothing on those days. Nothing else in
		// the suite would notice this cost creeping up, and it multiplies by every
		// day of every season a league ever sims.
		const runSeason = async (smart: boolean) => {
			await build(makeRng(4077));
			g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
			g.setWithoutSavingToDB("smartAiFrontOffice", smart);
			const start = performance.now();
			for (let day = 0; day < 82; day++) {
				await autoSign();
			}
			return performance.now() - start;
		};

		const vanillaMs = await runSeason(false);
		const smartMs = await runSeason(true);

		console.log(
			[
				"",
				`=== 82 in-season days, 30 teams ===`,
				`vanilla: ${vanillaMs.toFixed(0)}ms (${(vanillaMs / 82).toFixed(2)}ms/day)`,
				`smart:   ${smartMs.toFixed(0)}ms (${(smartMs / 82).toFixed(2)}ms/day)`,
				"",
			].join("\n"),
		);

		// A regular-season day sims games, which costs a couple hundred ms; the
		// posture work must stay an order of magnitude below that. Measured here:
		// ~0.9ms/day smart against ~0.7ms/day vanilla. The bar is wall-clock and
		// generous for CI, but a per-day cost that grew toward a game sim's would
		// blow through it.
		if (smartMs > 10_000) {
			throw new Error(
				`82 in-season autoSign days took ${smartMs.toFixed(0)}ms - the daily posture work has grown to game-sim scale`,
			);
		}
	}, 300_000);
});
