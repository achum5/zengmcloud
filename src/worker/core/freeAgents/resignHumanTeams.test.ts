import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g, local } from "../../util/index.ts";
import { PHASE } from "../../../common/constants.ts";
import { player, team } from "../index.ts";
import newPhaseResignPlayers from "../phase/newPhaseResignPlayers.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { changeTracker } from "../../db/changeTracker.ts";

// ---------------------------------------------------------------------------
// RE-SIGNING must leave human teams alone.
//
// The existing "never signs or trades on behalf of a human team" test drives
// only the free agency day loop - decreaseDemands, clearSpaceForSignings,
// autoSign. It never calls newPhaseResignPlayers, which is precisely where the
// tier-aware walk-away logic lives. So the one phase that can hand somebody
// else's expiring player to free agency was the one phase nobody was checking.
//
// In a synced room userTids holds EVERY friend's team, and only one device runs
// this code. If it let a friend's player walk, that friend would open the game
// to find a roster decision made for them - and it would sync, so there would be
// nothing to undo.
//
// Three separate places decide "is this team AI-controlled": autoSign's
// eligibleTeams filter, clearSpace's isAiControlled, and the branch in
// newPhaseResignPlayers. They agree today. This pins the third one.
// ---------------------------------------------------------------------------

const NUM_TEAMS = 10;
const HUMAN_TIDS = [0, 4, 9];
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

// Everyone's contract expires this season, so every team faces the full set of
// re-signing decisions and there is no chance of a pass that only means "nothing
// was up for renewal".
const build = async (rng: () => number, smart: boolean) => {
	resetG();
	g.setWithoutSavingToDB("numTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("numActiveTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("phase", PHASE.RESIGN_PLAYERS);
	g.setWithoutSavingToDB("userTids", HUMAN_TIDS);
	g.setWithoutSavingToDB("salaryCapType", "soft");
	g.setWithoutSavingToDB("smartAiFrontOffice", smart);
	local.autoPlayUntil = undefined;

	const salaryCap = g.get("salaryCap");
	const minContract = g.get("minContract");
	const teams: any[] = [];
	const players: any[] = [];

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
		let budget = salaryCap * (0.6 + rng() * 0.3);
		for (let i = 0; i < 12; i++) {
			const amount = Math.max(
				minContract,
				Math.round(budget * (i < 3 ? 0.2 : 0.045)),
			);
			budget -= amount;
			const age = Math.round(26 + rng() * 8);
			const p: any = player.generate(
				tid,
				age,
				g.get("season") - age,
				true,
				DEFAULT_LEVEL,
			);
			const r = p.ratings.at(-1);
			const ovr = Math.round(
				42 + strength * 18 + (i < 3 ? 12 : 0) - i + rng() * 6,
			);
			r.ovr = ovr;
			r.pot = ovr;
			r.pos = POSITIONS[i % POSITIONS.length]!;
			p.born.year = g.get("season") - age;
			// Expiring, and expensive enough that shouldLetWalk is in play.
			p.contract = { amount, exp: g.get("season") };
			p.injury = { type: "Healthy", gamesRemaining: 0 };
			p.value = ovr;
			p.valueNoPot = ovr;
			p.valueFuzz = ovr;
			p.valueNoPotFuzz = ovr;
			players.push(p);
		}
	}

	await resetCache({ players, teams, draftPicks: [] });
	stubLeagueDb();

	// Records, so postures are real rather than every team looking identical.
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		const row: any = team.genSeasonRow((await idb.cache.teams.get(tid))!);
		row.season = g.get("season");
		row.tid = tid;
		row.won = Math.round(82 * (0.8 - (0.6 * tid) / (NUM_TEAMS - 1)));
		row.lost = 82 - row.won;
		row.gp = 82;
		await idb.cache.teamSeasons.add(row);
	}
};

const rosterOf = async (tid: number) =>
	(await idb.cache.players.indexGetAll("playersByTid", tid))
		.map((p) => p.pid)
		.sort((a, b) => a - b)
		.join(",");

describe("re-signing leaves human teams alone", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	test("smart AI reaches exactly vanilla's decisions for human teams", async () => {
		// Note what "untouched" does NOT mean here. In stock BBGM a human team's
		// expiring players legitimately leave the roster and are paired with a
		// negotiation - that pairing IS the Re-sign Players screen. So the roster
		// changing is correct, and asserting otherwise would be asserting against
		// the game's own design.
		//
		// The invariant that actually matters is that the smart front office
		// changes NOTHING about which of a human's players end up in that state.
		// Running both arms over the same league and diffing is the only way to
		// say that without re-encoding BBGM's rules and getting them wrong.
		const collect = async (smart: boolean) => {
			await build(makeRng(606), smart);
			await newPhaseResignPlayers({} as any);
			const negotiations = await idb.cache.negotiations.getAll();
			const rosters: string[] = [];
			for (const tid of HUMAN_TIDS) {
				rosters.push(await rosterOf(tid));
			}
			return {
				negotiationPids: negotiations
					.map((n) => n.pid)
					.sort((a, b) => a - b)
					.join(","),
				rosters: rosters.join("|"),
			};
		};

		const smart = await collect(true);
		const vanilla = await collect(false);

		assert.strictEqual(
			smart.negotiationPids,
			vanilla.negotiationPids,
			"the smart front office changed which of the humans' players are up for re-signing",
		);
		assert.strictEqual(
			smart.rosters,
			vanilla.rosters,
			"the smart front office left the humans' rosters in a different state than stock BBGM would have",
		);
		assert.ok(
			smart.negotiationPids.length > 0,
			"no negotiations at all, so this run proves nothing",
		);
	}, 300_000);

	test("every human expiring player is offered a negotiation instead", async () => {
		await build(makeRng(606), true);

		const humanPids = new Set<number>();
		for (const tid of HUMAN_TIDS) {
			for (const p of await idb.cache.players.indexGetAll(
				"playersByTid",
				tid,
			)) {
				humanPids.add(p.pid);
			}
		}

		await newPhaseResignPlayers({} as any);

		// The human's expiring players should be waiting on THEM to decide, which
		// is what a negotiation is. Anything else means the choice was made for
		// them somewhere upstream.
		const negotiations = await idb.cache.negotiations.getAll();
		const negotiationPids = new Set(negotiations.map((n) => n.pid));
		const missing = [...humanPids].filter((pid) => !negotiationPids.has(pid));

		assert.deepStrictEqual(
			missing,
			[],
			`${missing.length} expiring player(s) on human teams got no re-signing negotiation`,
		);
	}, 300_000);

	test("under auto-play the AI does take over, deliberately", async () => {
		// The mirror image, so the exclusion is proven to be conditional rather
		// than a blanket "never touch these tids" that would quietly break
		// auto-play and spectator mode.
		await build(makeRng(606), true);
		local.autoPlayUntil = {
			season: g.get("season") + 1,
			phase: PHASE.PRESEASON,
			start: 0,
		};
		try {
			await newPhaseResignPlayers({} as any);
			const negotiations = await idb.cache.negotiations.getAll();
			assert.strictEqual(
				negotiations.length,
				0,
				"auto-play should let the AI decide, not queue negotiations for a human who is not watching",
			);
		} finally {
			local.autoPlayUntil = undefined;
		}
	}, 300_000);
});
