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

// ---------------------------------------------------------------------------
// EVERY roster move the smart AI makes has to end up in the changeset.
//
// In a synced room only one device runs this code; everyone else replays the
// changeset it publishes. So a write the tracker never saw is not a cosmetic
// bug - the simming device signs a player, nobody else does, and the leagues
// silently disagree from that moment on. That is the exact shape of the
// corruption this room has already been through once, and no amount of testing
// the DECISIONS catches it, because the decisions are correct.
//
// The check is deliberately independent of the AI's reasoning: snapshot who is
// on which team before and after, diff it, and require that every single player
// who moved has a corresponding tracked change. It cannot be satisfied by the
// AI simply choosing to do nothing, because it also insists something happened.
// ---------------------------------------------------------------------------

const NUM_TEAMS = 8;
const FA_DAYS = 20;
const POSITIONS = ["PG", "SG", "SF", "PF", "C"];

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
	p.born.year = g.get("season") - age;
	p.contract = { amount, exp: g.get("season") + Math.floor(rng() * 4) };
	p.injury = { type: "Healthy", gamesRemaining: 0 };
	p.value = ovr;
	p.valueNoPot = ovr;
	p.valueFuzz = ovr;
	p.valueNoPotFuzz = ovr;
	return p;
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

const build = async (rng: () => number) => {
	resetG();
	g.setWithoutSavingToDB("numTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("numActiveTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
	g.setWithoutSavingToDB("userTids", [-99]);
	g.setWithoutSavingToDB("salaryCapType", "soft");
	g.setWithoutSavingToDB("smartAiFrontOffice", true);

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
		let budget = salaryCap * (0.6 + rng() * 0.35);
		for (let i = 0; i < 12; i++) {
			const amount = Math.max(
				minContract,
				Math.round(budget * (i < 3 ? 0.2 : 0.045)),
			);
			budget -= amount;
			players.push(
				makePlayer(
					rng,
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

	// Includes genuine stars at real prices, which is what makes a team consider
	// clearing space rather than just filling out a bench.
	for (let i = 0; i < NUM_TEAMS * 5; i++) {
		players.push(
			makePlayer(
				rng,
				PLAYER.FREE_AGENT,
				Math.round(40 + rng() * 32),
				POSITIONS[i % POSITIONS.length]!,
				Math.max(minContract, Math.round(minContract * (1 + rng() * 16))),
			),
		);
	}

	await resetCache({ players, teams, draftPicks });
	stubLeagueDb();
};

const snapshotTids = async () => {
	const map = new Map<number, number>();
	for (const p of await idb.cache.players.indexGetAll("playersByTid", [
		PLAYER.FREE_AGENT,
		Infinity,
	])) {
		map.set(p.pid, p.tid);
	}
	return map;
};

describe("smart AI writes are complete for sync", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	test("every player the AI moves appears in the changeset", async () => {
		await build(makeRng(2718));

		const before = await snapshotTids();

		changeTracker.reset();
		changeTracker.enable();
		changeTracker.beginSim();
		try {
			for (let day = FA_DAYS; day > 0; day--) {
				g.setWithoutSavingToDB("daysLeft", day);
				await decreaseDemands();
				await clearSpaceForSignings();
				await autoSign();
			}
		} finally {
			changeTracker.endSim();
		}
		const changes = changeTracker.drain();
		changeTracker.disable();

		const after = await snapshotTids();

		const moved: number[] = [];
		for (const [pid, tid] of after) {
			if (before.get(pid) !== tid) {
				moved.push(pid);
			}
		}

		const trackedPids = new Set(
			changes.filter((c) => c.store === "players").map((c) => c.id),
		);

		console.log(
			[
				"",
				`players who changed team: ${moved.length}`,
				`tracked player changes:   ${trackedPids.size}`,
				`total tracked changes:    ${changes.length}`,
				`stores touched: ${[...new Set(changes.map((c) => c.store))].sort().join(", ")}`,
				"",
			].join("\n"),
		);

		// Guard against a vacuous pass: if the AI sat on its hands there would be
		// nothing to fail to track.
		assert.ok(
			moved.length > 0,
			"no player changed teams, so this proves nothing about sync",
		);

		const untracked = moved.filter((pid) => !trackedPids.has(pid));
		assert.deepStrictEqual(
			untracked,
			[],
			`${untracked.length} player(s) changed team without the change being recorded - a follower replaying this changeset would not move them, and the leagues would diverge from here`,
		);
	}, 300_000);

	test("nothing is recorded when no sim or capture window is open", async () => {
		// The mirror image. Writes made outside a window must NOT be silently
		// buffered and then attributed to whatever changeset happens to drain
		// next - that would publish one device's private state to the room.
		await build(makeRng(2718));

		changeTracker.reset();
		changeTracker.enable();
		try {
			for (let day = 3; day > 0; day--) {
				g.setWithoutSavingToDB("daysLeft", day);
				await decreaseDemands();
				await clearSpaceForSignings();
				await autoSign();
			}
		} finally {
			changeTracker.disable();
		}

		assert.strictEqual(
			changeTracker.drain().length,
			0,
			"writes were buffered with no capture or sim window open",
		);
	}, 300_000);
});
