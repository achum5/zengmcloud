import { assert, beforeEach, describe, test } from "vitest";
import { repairAwardLabels } from "./repairAwardLabels.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { resetCache, resetG } from "../../../test/helpers.ts";
import type { AwardSettings } from "../../../common/types.ts";

// A league database small enough to read at a glance, and real enough for the
// sweep: it walks every awards row and reads back each player it names.
const fakeLeague = (data: { awards: any[]; players: any[] }) => {
	const stores: Record<string, Map<any, any>> = {
		awards: new Map(data.awards.map((row) => [row.season, row])),
		players: new Map(data.players.map((p) => [p.pid, p])),
	};

	let writes = 0;

	return {
		writes: () => writes,
		stores,
		league: {
			getAll: async (store: string) => [...stores[store]!.values()],
			get: async (store: string, key: any) => stores[store]!.get(key),
			put: async (store: string, value: any) => {
				writes += 1;
				stores[store]!.set(
					store === "awards" ? value.season : value.pid,
					value,
				);
			},
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

	// An abbrev the settings no longer use belongs to an award that was deleted
	// or given a new abbrev, and its history is not this sweep's to rewrite.
	test("an award the settings no longer name is left alone", async () => {
		const db = fakeLeague({
			awards: [awardsRow(2005, "All-League")],
			players: [player(1, [2005], "All-League")],
		});
		idb.league = db.league;
		g.setWithoutSavingToDB("awards", [
			{ ...allLeague, name: "All-NBA", shortName: "ANBA" },
		]);

		await repairAwardLabels();
		assert.strictEqual(db.writes(), 0);
		assert.strictEqual(
			db.stores.awards!.get(2005)!.awards[0].name,
			"All-League",
		);
	});
});
