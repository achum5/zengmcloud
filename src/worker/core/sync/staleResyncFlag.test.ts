import "fake-indexeddb/auto";
import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import { idb } from "../../db/index.ts";
import { findStrandedScheduleRows } from "./changeset.ts";

// THE DEADLOCK, straight off a field capture:
//
//   engine:catchup-count  behind=0 persistedSeq==maxSeq remaining=0
//   connect:auto-resync-no-usable-checkpoint
//   api:guard-refused  simGame  "This device is flagged for a repair pass"
//
// A phone at the head of the log, with nothing missing, refusing to sim - and
// it could never stop refusing. The repair flag's only exit on v1 is a
// successful room-snapshot restore, and that room had never published a
// snapshot, so there was nothing to restore and nothing to clear the flag. Every
// sim, forever: "I click sim game and nothing happens".
//
// The rule that ends it: the flag records that something WAS skipped, not that
// anything is STILL missing. These tests pin the evidence that is allowed to
// retire it - and, just as importantly, the evidence that must not.

describe("evidence that a repair flag is stale", () => {
	beforeEach(async () => {
		resetG();
		g.setWithoutSavingToDB("season", 2006);
		await resetCache({});
	});

	const seed = async ({
		games,
		schedule,
	}: {
		games: { gid: number; day: number }[];
		schedule: { gid: number; day: number }[];
	}) => {
		for (const game of games) {
			await idb.cache.games.add({
				...game,
				season: 2006,
				teams: [{ tid: 0 }, { tid: 1 }],
			} as any);
		}
		for (const row of schedule) {
			await idb.cache.schedule.add({
				...row,
				season: 2006,
				homeTid: 0,
				awayTid: 1,
			} as any);
		}
	};

	test("a device level with the league has nothing stranded", async () => {
		// Played through day 100; the rows left are day 100's unfinished slate.
		await seed({
			games: [
				{ gid: 1, day: 99 },
				{ gid: 2, day: 100 },
			],
			schedule: [
				{ gid: 3, day: 100 },
				{ gid: 4, day: 100 },
			],
		});
		const stranded = await findStrandedScheduleRows();
		assert.deepStrictEqual(stranded.gids, []);
		assert.strictEqual(stranded.maxPlayedDay, 100);
	});

	test("a genuinely dropped day IS stranded, and must keep the flag set", async () => {
		// Day 99 never arrived, but the league played on to 100. This is the
		// damage the flag exists for, and no amount of "caught up" may excuse it.
		await seed({
			games: [{ gid: 2, day: 100 }],
			schedule: [
				{ gid: 3, day: 99 },
				{ gid: 4, day: 100 },
			],
		});
		const stranded = await findStrandedScheduleRows();
		assert.deepStrictEqual(stranded.gids, [3]);
		assert.deepStrictEqual(stranded.days, [99]);
	});

	test("a playoff slate mid-round is not mistaken for damage", async () => {
		// Series finish at different times, so a day can hold anywhere from one
		// to four games. What matters is only that nothing sits BELOW the last
		// day played - the case the user was actually in.
		await seed({
			games: [
				{ gid: 1, day: 98 },
				{ gid: 2, day: 99 },
				{ gid: 3, day: 100 },
			],
			schedule: [
				{ gid: 4, day: 100 },
				{ gid: 5, day: 100 },
				{ gid: 6, day: 100 },
			],
		});
		assert.deepStrictEqual((await findStrandedScheduleRows()).gids, []);
	});

	test("a season with no games played yet proves nothing either way", async () => {
		// No maxPlayedDay to compare against, so the check abstains rather than
		// declaring a fresh preseason healthy.
		await seed({
			games: [],
			schedule: [{ gid: 1, day: 1 }],
		});
		const stranded = await findStrandedScheduleRows();
		assert.strictEqual(stranded.maxPlayedDay, undefined);
		assert.deepStrictEqual(stranded.gids, []);
	});
});
