import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { clearFiledRecaps } from "./clearFiledRecaps.ts";

// A stand-in for the league database's games store that supports what the sweep
// uses: an async cursor with update(). The shared mockIDBLeague is a stub that
// returns empty arrays and has no cursor, so it cannot exercise this at all.
const mockGamesDB = (rows: any[]) => {
	let done = false;
	const store = {
		async *[Symbol.asyncIterator]() {
			for (const [i, value] of rows.entries()) {
				yield {
					value,
					async update(updated: any) {
						rows[i] = updated;
					},
				};
			}
		},
	};
	return {
		rows,
		league: {
			transaction() {
				return {
					store,
					get done() {
						done = true;
						return Promise.resolve();
					},
				};
			},
			get transactionDone() {
				return done;
			},
		},
	};
};

describe("clearFiledRecaps", () => {
	beforeEach(async () => {
		resetG();
		await resetCache();
	});

	test("clears filed game recaps and filed day recaps, and counts each", async () => {
		const db = mockGamesDB([
			{ gid: 0, season: 2026, note: "filed", noteBool: 1 },
			{ gid: 1, season: 2026, dayNote: "filed day", dayNoteBool: 1 },
			{
				gid: 2,
				season: 2025,
				note: "filed",
				noteBool: 1,
				dayNote: "filed day",
				dayNoteBool: 1,
			},
			{ gid: 3, season: 2024 },
		]);
		idb.league = db.league as any;

		const counts = await clearFiledRecaps();

		assert.deepEqual(counts, { games: 2, days: 2 });
		for (const row of db.rows) {
			assert.equal(row.note, undefined, `gid ${row.gid} kept a note`);
			assert.equal(row.noteBool, undefined, `gid ${row.gid} kept noteBool`);
			assert.equal(row.dayNote, undefined, `gid ${row.gid} kept a dayNote`);
			assert.equal(
				row.dayNoteBool,
				undefined,
				`gid ${row.gid} kept dayNoteBool`,
			);
		}
	});

	test("spans every season, not just the current one", async () => {
		// The cache only ever holds the current season, so a sweep that worked
		// through it would leave every past season's recaps in place.
		const db = mockGamesDB([
			{ gid: 0, season: 1998, note: "old", noteBool: 1 },
			{ gid: 1, season: 2026, note: "new", noteBool: 1 },
		]);
		idb.league = db.league as any;

		const counts = await clearFiledRecaps();

		assert.equal(counts.games, 2);
		assert.equal(db.rows[0]!.note, undefined);
		assert.equal(db.rows[1]!.note, undefined);
	});

	test("leaves the rest of the game record alone", async () => {
		const db = mockGamesDB([
			{
				gid: 7,
				season: 2026,
				note: "filed",
				noteBool: 1,
				teams: [{ pts: 104 }, { pts: 96 }],
				won: { tid: 1 },
				overtimes: 1,
			},
		]);
		idb.league = db.league as any;

		await clearFiledRecaps();

		const row = db.rows[0]!;
		assert.equal(row.gid, 7);
		assert.equal(row.season, 2026);
		assert.equal(row.overtimes, 1);
		assert.deepEqual(row.teams, [{ pts: 104 }, { pts: 96 }]);
		assert.deepEqual(row.won, { tid: 1 });
	});

	test("is idempotent - a second run finds nothing", async () => {
		const db = mockGamesDB([
			{
				gid: 0,
				season: 2026,
				note: "filed",
				noteBool: 1,
				dayNote: "d",
				dayNoteBool: 1,
			},
		]);
		idb.league = db.league as any;

		assert.deepEqual(await clearFiledRecaps(), { games: 1, days: 1 });
		assert.deepEqual(await clearFiledRecaps(), { games: 0, days: 0 });
	});

	test("reports nothing when no recap was ever filed", async () => {
		const db = mockGamesDB([
			{ gid: 0, season: 2026 },
			{ gid: 1, season: 2026 },
		]);
		idb.league = db.league as any;

		assert.deepEqual(await clearFiledRecaps(), { games: 0, days: 0 });
	});
});
