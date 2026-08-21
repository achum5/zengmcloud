import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { PHASE, PLAYER } from "../../../common/constants.ts";
import { player, team } from "../index.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import autoSign from "./autoSign.ts";

// THE WHOLE CHAIN, END TO END: a team's only centre goes down for two months,
// the posture's needs notice (analyzePositions discounts long absences), the
// daily in-season signing loop consults those needs, and a replacement centre
// is signed - the move a real front office makes within the week.

const NUM_TEAMS = 6;

const makeRng = (seed: number) => {
	let s = seed >>> 0;
	return () => {
		s = (s * 1_664_525 + 1_013_904_223) >>> 0;
		return s / 4_294_967_296;
	};
};

const stubLeagueDb = () => {
	const store = {
		index: () => store,
		getAll: async () => [],
		get: async () => undefined,
		async *iterate() {},
	};
	(idb as any).league = {
		transaction: () => ({
			store,
			objectStore: () => store,
			done: Promise.resolve(),
		}),
		getAll: async () => [],
		get: async () => undefined,
	};
};

const makePlayer = ({ pid, tid, ovr, age, pos, amount, exp }: any) => {
	const p: any = player.generate(
		tid,
		age,
		g.get("season") - age,
		true,
		DEFAULT_LEVEL,
	);
	p.pid = pid;
	p.born.year = g.get("season") - age;
	const r = p.ratings.at(-1);
	r.ovr = ovr;
	r.pot = ovr;
	r.pos = pos;
	p.value = ovr;
	p.valueNoPot = ovr;
	p.valueFuzz = ovr;
	p.valueNoPotFuzz = ovr;
	p.contract = { amount, exp };
	p.injury = { type: "Healthy", gamesRemaining: 0 };
	return p;
};

describe("patching an injury hole in-season", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	test("a team signs a centre when its only centre goes down long-term", async () => {
		const rng = makeRng(1234);
		const realRandom = Math.random;
		Math.random = rng;
		try {
			resetG();
			g.setWithoutSavingToDB("numActiveTeams", NUM_TEAMS);
			g.setWithoutSavingToDB("numTeams", NUM_TEAMS);
			g.setWithoutSavingToDB("userTids", []);
			g.setWithoutSavingToDB("userTid", 0);
			g.setWithoutSavingToDB("smartAiFrontOffice", true);
			g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);

			const season = g.get("season");
			const minContract = g.get("minContract");
			const teams: any[] = [];
			const players: any[] = [];
			let pid = 1;
			for (let tid = 0; tid < NUM_TEAMS; tid++) {
				teams.push(
					team.generate({
						tid,
						cid: 0,
						did: 0,
						region: `R${tid}`,
						name: `N${tid}`,
						abbrev: `T${tid}`,
						pop: 2,
						imgURL: "",
					} as any),
				);
				// Solid guards and forwards, exactly one centre. Rival teams get
				// extra bodies so their rosters are too full for minimum-contract
				// fills (getBest wants maxRosterSize - 2 spots free) - the test is
				// about the injured team's need chain, not a race to the wire.
				const bench = tid === 0 ? [] : ["PG", "SF", "PF"];
				for (const [i, pos] of [
					"PG",
					"SG",
					"SG",
					"SF",
					"SF",
					"PF",
					"PF",
					"PG",
					"SF",
					"PG",
					...bench,
				].entries()) {
					players.push(
						makePlayer({
							pid: pid++,
							tid,
							ovr: 50 + (i % 4),
							age: 26,
							pos,
							amount: 4000,
							exp: season + 1,
						}),
					);
				}
				players.push(
					makePlayer({
						pid: pid++,
						tid,
						ovr: 55,
						age: 27,
						pos: "C",
						amount: 6000,
						exp: season + 1,
					}),
				);
			}
			// The market: a decent centre and a comparable wing, both free agents
			// on cheap asks.
			const faCentre = makePlayer({
				pid: 9001,
				tid: PLAYER.FREE_AGENT,
				ovr: 44,
				age: 28,
				pos: "C",
				amount: minContract,
				exp: season,
			});
			const faWing = makePlayer({
				pid: 9002,
				tid: PLAYER.FREE_AGENT,
				ovr: 46,
				age: 28,
				pos: "SF",
				amount: minContract,
				exp: season,
			});
			players.push(faCentre, faWing);

			await resetCache({ players, teams, draftPicks: [] });
			stubLeagueDb();
			for (let tid = 0; tid < NUM_TEAMS; tid++) {
				const row: any = team.genSeasonRow((await idb.cache.teams.get(tid))!);
				row.season = season;
				row.won = 20 + tid * 5;
				row.lost = 41 - row.won / 2;
				row.gp = row.won + row.lost;
				await idb.cache.teamSeasons.add(row);
			}

			// Team 0's only centre goes down for two months.
			const roster0 = await idb.cache.players.indexGetAll("playersByTid", 0);
			const centre = roster0.find((p) => p.ratings.at(-1)!.pos === "C")!;
			centre.injury = { type: "Torn ACL", gamesRemaining: 30 };
			await idb.cache.players.put(centre);

			// A couple of weeks of daily signing loops.
			let signedCentre = false;
			for (let day = 0; day < 14 && !signedCentre; day++) {
				await autoSign();
				const roster = await idb.cache.players.indexGetAll("playersByTid", 0);
				signedCentre = roster.some((p) => p.pid === faCentre.pid);
			}

			assert.ok(
				signedCentre,
				"the team with the injured centre should sign the replacement centre",
			);
		} finally {
			Math.random = realRandom;
		}
	}, 60000);
});
