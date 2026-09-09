import { assert, beforeEach, describe, test } from "vitest";
import { repairAwardLabels } from "./repairAwardLabels.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { resetCache, resetG } from "../../../test/helpers.ts";
import type { AwardSettings } from "../../../common/types.ts";
import { relabelAwardsFromSettings } from "../../../common/awards.ts";

// A league database small enough to read at a glance, and real enough for the
// sweep: it walks every awards row and reads back each player it names.
const fakeLeague = (data: { awards: any[]; players: any[] }) => {
	const stores: Record<string, Map<any, any>> = {
		awards: new Map(data.awards.map((row) => [row.season, row])),
		players: new Map(data.players.map((p) => [p.pid, p])),
	};

	let writes = 0;

	const save = (store: string, value: any) => {
		writes += 1;
		stores[store]!.set(store === "awards" ? value.season : value.pid, value);
	};

	return {
		writes: () => writes,
		stores,
		league: {
			getAll: async (store: string) => [...stores[store]!.values()],
			get: async (store: string, key: any) => stores[store]!.get(key),
			put: async (store: string, value: any) => {
				save(store, value);
			},
			// The players are swept with a cursor, so the fake has to hand out
			// one that can write back.
			transaction: (store: string) => ({
				store: {
					async *[Symbol.asyncIterator]() {
						for (const value of stores[store]!.values()) {
							yield {
								value,
								update: async (updated: any) => {
									save(store, updated);
								},
							};
						}
					},
				},
			}),
		} as any,
	};
};

const allLeague: AwardSettings[number] = {
	shortName: "ALL",
	name: "All-League",
	formula: "ewa / 22",
	showStats: "offense",
	numTeams: 3,
};

const allDefensive: AwardSettings[number] = {
	shortName: "DEF",
	name: "All-Defensive",
	formula: "ewa / 22",
	showStats: "defense",
	numTeams: 2,
};

const awardsRow = (season: number, name: string) => ({
	season,
	bestRecord: 0,
	bestRecordConfs: {},
	bestRecordDivs: {},
	awards: [
		{
			...allLeague,
			name,
			winner: [[{ pid: 1, tid: 3 }], [{ pid: 2, tid: 4 }], []],
		},
	],
});

// A player's own copy of a team award carries numTeams, which is what keeps an
// abbrev handed from a team award to an individual one from relabeling it.
const playerAward = (season: number, name: string) => ({
	season,
	name,
	shortName: "ALL",
	index: 0,
	rank: 1,
	numTeams: 3,
});

const player = (pid: number, seasons: number[], name: string) => ({
	pid,
	awards: seasons.map((season) => playerAward(season, name)),
});

describe("repairAwardLabels", () => {
	beforeEach(async () => {
		resetG();
		await resetCache();
	});

	// The case that has no diff behind it: the settings already say All-NBA and
	// every season still says All-League.
	test("a rename made before any of this existed still lands", async () => {
		const db = fakeLeague({
			awards: [awardsRow(2005, "All-League"), awardsRow(2006, "All-League")],
			players: [
				player(1, [2005, 2006], "All-League"),
				player(2, [2005, 2006], "All-League"),
			],
		});
		idb.league = db.league;
		g.setWithoutSavingToDB("awards", [{ ...allLeague, name: "All-NBA" }]);

		const result = await repairAwardLabels();
		assert.strictEqual(result?.seasons, 2);
		assert.strictEqual(result?.players, 2);

		for (const row of db.stores.awards!.values()) {
			assert.strictEqual(row.awards[0].name, "All-NBA");
			// The abbrev identifies the award and never moves.
			assert.strictEqual(row.awards[0].shortName, "ALL");
		}
		for (const p of db.stores.players!.values()) {
			for (const award of p.awards) {
				assert.strictEqual(award.name, "All-NBA");
				assert.strictEqual(award.shortName, "ALL");
			}
		}
	});

	test("running it again does nothing", async () => {
		const db = fakeLeague({
			awards: [awardsRow(2005, "All-League")],
			players: [
				player(1, [2005], "All-League"),
				player(2, [2005], "All-League"),
			],
		});
		idb.league = db.league;
		g.setWithoutSavingToDB("awards", [{ ...allLeague, name: "All-NBA" }]);

		await repairAwardLabels();
		const after = db.writes();
		await repairAwardLabels();
		assert.strictEqual(db.writes(), after);
	});

	test("a league that was never renamed is not touched at all", async () => {
		const db = fakeLeague({
			awards: [awardsRow(2005, "All-League")],
			players: [player(1, [2005], "All-League")],
		});
		idb.league = db.league;
		g.setWithoutSavingToDB("awards", [allLeague]);

		await repairAwardLabels();
		assert.strictEqual(db.writes(), 0);
	});

	// A player's own copy can be stale on its own - an interrupted sweep, a
	// season synced from a device that had already relabeled its row.
	test("a stale player copy is repaired even when the season is already right", async () => {
		const db = fakeLeague({
			awards: [awardsRow(2005, "All-NBA")],
			players: [player(1, [2005], "All-League")],
		});
		idb.league = db.league;
		g.setWithoutSavingToDB("awards", [{ ...allLeague, name: "All-NBA" }]);

		// The row itself agrees, so nothing there says a repair is needed - but
		// the check reads the rows, so this only happens alongside a season that
		// IS stale.
		db.stores.awards!.set(2006, awardsRow(2006, "All-League"));
		db.stores.players!.get(1)!.awards.push(playerAward(2006, "All-League"));

		await repairAwardLabels();
		for (const award of db.stores.players!.get(1)!.awards) {
			assert.strictEqual(award.name, "All-NBA");
		}
	});

	// An abbrev the settings no longer use, with the award that took its slot
	// still here under its own name: this one was deleted, and a deleted award
	// keeps the history it earned.
	test("a deleted award keeps its own history", async () => {
		const db = fakeLeague({
			awards: [
				{
					...awardsRow(2005, "All-League"),
					awards: [
						awardsRow(2005, "All-League").awards[0],
						{
							...allDefensive,
							winner: [[{ pid: 1, tid: 3 }], []],
						},
					],
				},
			],
			players: [
				{
					pid: 1,
					awards: [
						playerAward(2005, "All-League"),
						{
							...playerAward(2005, "All-Defensive"),
							shortName: "DEF",
							index: 1,
						},
					],
				},
			],
		});
		idb.league = db.league;
		// All-League is gone; All-Defensive has shifted up into its slot.
		g.setWithoutSavingToDB("awards", [allDefensive]);

		await repairAwardLabels();
		assert.strictEqual(db.writes(), 0);
		assert.strictEqual(
			db.stores.awards!.get(2005)!.awards[0].name,
			"All-League",
		);
	});

	// The same shape, but nothing else is claiming the slot and the new abbrev
	// is nowhere in the history: this award was renamed, abbrev and all, before
	// any of this existed.
	test("a rename that changed the abbrev too still lands", async () => {
		const db = fakeLeague({
			awards: [awardsRow(2005, "All-League")],
			players: [player(1, [2005], "All-League")],
		});
		idb.league = db.league;
		g.setWithoutSavingToDB("awards", [
			{ ...allLeague, name: "All-NBA", shortName: "ANBA" },
		]);

		const result = await repairAwardLabels();
		assert.strictEqual(result?.seasons, 1);
		assert.strictEqual(result?.players, 1);

		const award = db.stores.awards!.get(2005)!.awards[0];
		assert.strictEqual(award.name, "All-NBA");
		assert.strictEqual(award.shortName, "ANBA");
		const own = db.stores.players!.get(1)!.awards[0];
		assert.strictEqual(own.name, "All-NBA");
		assert.strictEqual(own.shortName, "ANBA");
	});

	// An individual award is not the same award as a team one, whatever slot it
	// lands in.
	test("a slot that changed kind is left alone", async () => {
		const db = fakeLeague({
			awards: [awardsRow(2005, "All-League")],
			players: [player(1, [2005], "All-League")],
		});
		idb.league = db.league;
		g.setWithoutSavingToDB("awards", [
			{
				shortName: "MVP",
				name: "Most Valuable Player",
				formula: "ewa",
				showStats: "overall",
			} as any,
		]);

		await repairAwardLabels();
		assert.strictEqual(db.writes(), 0);
	});
});

// A COPY THAT NO SEASON GIVES AWAY.
//
// Detection reads the awards rows. A player whose own copy is stale while
// every season already agrees is invisible to that - so the players in memory
// are checked too, and everybody else is relabeled as he is read (see
// relabelAwardsFromSettings), which is what a page actually shows.
describe("a stale player copy with no stale season", () => {
	beforeEach(async () => {
		resetG();
		await resetCache();
	});

	test("a player in memory is enough to trigger the sweep", async () => {
		const db = fakeLeague({
			awards: [awardsRow(2005, "All-NBA")],
			players: [player(1, [2005], "All-League")],
		});
		idb.league = db.league;
		// The same man, sitting in the cache the way an active player does.
		await idb.cache.players.add(player(1, [2005], "All-League") as any);
		g.setWithoutSavingToDB("awards", [{ ...allLeague, name: "All-NBA" }]);

		await repairAwardLabels();

		assert.strictEqual(
			db.stores.players!.get(1)!.awards[0].name,
			"All-NBA",
			"the stored copy",
		);
	});

	// A LEAGUE THAT HAS NO AWARDS ROWS AT ALL.
	//
	// Started from real rosters, its players carry decades of awards and the
	// awards store is empty. A sweep that looked for winners in the seasons
	// found nothing to look at and repaired nothing - on every league load,
	// forever, because the staleness it kept detecting was never fixed.
	test("the players are swept even with no season to name them", async () => {
		const db = fakeLeague({
			awards: [],
			players: [
				player(1, [1970, 1971], "All-League"),
				player(2, [1985], "All-League"),
			],
		});
		idb.league = db.league;
		await idb.cache.players.add(player(1, [1970], "All-League") as any);
		g.setWithoutSavingToDB("awards", [{ ...allLeague, name: "All-NBA" }]);

		const result = await repairAwardLabels();

		assert.strictEqual(result!.players, 2, "players relabeled");
		for (const pid of [1, 2]) {
			for (const award of db.stores.players!.get(pid)!.awards) {
				assert.strictEqual(award.name, "All-NBA", `player ${pid}`);
			}
		}

		// And having been fixed, it stays fixed: the next load finds nothing.
		const writesBefore = db.writes();
		await resetCache();
		await idb.cache.players.add(db.stores.players!.get(1) as any);
		await repairAwardLabels();
		assert.strictEqual(db.writes(), writesBefore, "a second load writes");
	});

	test("nobody in memory, so the stored copy waits for a read", async () => {
		const db = fakeLeague({
			awards: [awardsRow(2005, "All-NBA")],
			players: [player(1, [2005], "All-League")],
		});
		idb.league = db.league;
		g.setWithoutSavingToDB("awards", [{ ...allLeague, name: "All-NBA" }]);

		await repairAwardLabels();

		// Nothing detectable, so nothing written - and nothing needs to be,
		// because this is what every page will show him as.
		assert.strictEqual(
			relabelAwardsFromSettings(db.stores.players!.get(1)!.awards, [
				{ ...allLeague, name: "All-NBA" },
			])![0]!.name,
			"All-NBA",
		);
	});
});
