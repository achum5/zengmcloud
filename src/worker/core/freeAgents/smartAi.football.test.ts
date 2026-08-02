import { assert, beforeEach, describe, test } from "vitest";
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
import { captureFrontOfficeLog } from "../../util/frontOfficeLog.ts";

// ---------------------------------------------------------------------------
// The smart front office in a sport it was not designed around.
//
// Everything else testing this feature is basketball, and basketball is where
// the model fits: posBucket sorts players into G/F/C, and team ovr is a
// position-blind function of the best players. Football is neither. Every
// football position falls through posBucket into "F", so the posture's notion of
// positional need is meaningless here, and getBest takes the DRAFT_BY_TEAM_OVR
// branch, which ignores the caller's ordering entirely.
//
// So this is not asking whether the decisions are clever. It is asking whether a
// feature tuned on one sport DEGRADES SAFELY in another: no crash on unfamiliar
// position strings, free agency still clears, and nobody is left unable to field
// a team. That is the bar a multi-sport codebase has to clear before shipping a
// setting that defaults to on.
// ---------------------------------------------------------------------------

const NUM_TEAMS = 8;
const FA_DAYS = 20;

const POSITIONS = [
	"QB",
	"RB",
	"TE",
	"WR",
	"OL",
	"CB",
	"S",
	"LB",
	"DL",
	"K",
	"P",
];

const makeRng = (seed: number) => {
	let s = seed >>> 0;
	return () => {
		s = (s * 1_664_525 + 1_013_904_223) >>> 0;
		return s / 4_294_967_296;
	};
};

const makePlayer = (
	rng: () => number,
	tid: number,
	ovr: number,
	pos: string,
	amount: number,
) => {
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
	r.ovrs = { ...(r.ovrs ?? {}), [pos]: ovr };
	p.born.year = g.get("season") - age;
	p.contract = { amount, exp: g.get("season") + Math.floor(rng() * 4) };
	p.injury = { type: "Healthy", gamesRemaining: 0 };
	p.value = ovr;
	p.valueNoPot = ovr;
	p.valueFuzz = ovr;
	p.valueNoPotFuzz = ovr;
	return p;
};

describe("smart AI front offices in football", () => {
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
		const minRosterSize = g.get("minRosterSize");
		const teams: any[] = [];
		const players: any[] = [];
		const draftPicks: any[] = [];

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
			let budget = salaryCap * (0.5 + rng() * 0.35);
			for (let i = 0; i < minRosterSize + 2; i++) {
				const amount = Math.max(
					minContract,
					Math.round(budget * (i < 3 ? 0.12 : 0.02)),
				);
				budget -= amount;
				players.push(
					makePlayer(
						rng,
						tid,
						Math.round(40 + strength * 18 + (i < 3 ? 12 : 0) + rng() * 8),
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

		// A free agent pool with real money in it, so cap holds and salary dumps
		// have something to actually be tempted by.
		for (let i = 0; i < NUM_TEAMS * 6; i++) {
			players.push(
				makePlayer(
					rng,
					PLAYER.FREE_AGENT,
					Math.round(38 + rng() * 30),
					POSITIONS[i % POSITIONS.length]!,
					Math.max(minContract, Math.round(minContract * (1 + rng() * 14))),
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

	const runFreeAgency = async (smart: boolean, seed: number) => {
		await build(makeRng(seed));
		g.setWithoutSavingToDB("smartAiFrontOffice", smart);

		const before = (
			await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT)
		).length;

		const capture = captureFrontOfficeLog();
		for (let day = FA_DAYS; day > 0; day--) {
			g.setWithoutSavingToDB("daysLeft", day);
			await decreaseDemands();
			await clearSpaceForSignings();
			await autoSign();
		}
		const entries = capture.stop();

		const sizes: number[] = [];
		for (let tid = 0; tid < NUM_TEAMS; tid++) {
			sizes.push(
				(await idb.cache.players.indexGetAll("playersByTid", tid)).length,
			);
		}
		const after = (
			await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT)
		).length;

		return { signed: before - after, sizes, entries };
	};

	test("free agency runs, clears, and leaves every team able to field a side", async () => {
		const smart = await runFreeAgency(true, 4242);
		const vanilla = await runFreeAgency(false, 4242);

		console.log(
			[
				"",
				`football, ${NUM_TEAMS} teams, ${FA_DAYS} days`,
				`smart:   signed ${smart.signed}, rosters ${smart.sizes.join(" ")}`,
				`vanilla: signed ${vanilla.signed}, rosters ${vanilla.sizes.join(" ")}`,
				`front-office decisions logged: ${smart.entries.length}`,
				"",
			].join("\n"),
		);

		// The market has to actually move. A silent no-op - every team frozen by an
		// exception swallowed somewhere, or by a cap hold nobody ever releases -
		// would otherwise look like a clean pass.
		assert.ok(
			smart.signed > 0,
			"smart front offices signed nobody at all in football",
		);

		// And it has to move about as much as stock BBGM's does.
		assert.ok(
			smart.signed >= vanilla.signed * 0.5,
			`smart signed ${smart.signed} against vanilla's ${vanilla.signed} - free agency is seizing up in football`,
		);

		// Degrading safely means nobody ends up short-handed.
		const minRosterSize = g.get("minRosterSize");
		for (const [tid, size] of smart.sizes.entries()) {
			assert.ok(
				size >= minRosterSize,
				`team ${tid} finished free agency with ${size} players, below the minimum of ${minRosterSize}`,
			);
		}
	}, 300_000);

	test("the kill switch really is off in football too", async () => {
		const vanilla = await runFreeAgency(false, 99);
		assert.strictEqual(
			vanilla.entries.length,
			0,
			"a front-office decision was logged with smartAiFrontOffice disabled",
		);
	}, 300_000);
});
