import { assert, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import { idb } from "../../db/index.ts";
import { PHASE } from "../../../common/constants.ts";
import {
	getLeaguePosition,
	isBehindPosition,
	parseLeaguePosition,
} from "./leaguePosition.ts";

// The engine answers "am I caught up?" by comparing its watermark against the
// highest entry it has been HANDED, so an entry that never arrived leaves it
// confidently, silently behind - showing the next day as upcoming and waiting
// forever, with reconnecting no help because a fresh catch-up starts from the
// same banked watermark. This is the second opinion: the sim authority stamps
// where the league really is, and a follower compares it against its own data.
describe("league position", () => {
	test("reads the furthest day actually played", async () => {
		resetG();
		await resetCache({});
		for (const day of [42, 45, 44]) {
			await idb.cache.games.put({
				gid: 7000 + day,
				season: g.get("season"),
				day,
				teams: [],
			} as any);
		}

		const position = await getLeaguePosition();
		assert.strictEqual(position.day, 45);
		assert.strictEqual(position.season, g.get("season"));
		assert.strictEqual(position.phase, g.get("phase"));
	});

	test("is day 0 before anything has been played", async () => {
		resetG();
		await resetCache({});
		assert.strictEqual((await getLeaguePosition()).day, 0);
	});

	// Games from before the day field existed must not read as day 0 played and
	// must not crash the comparison.
	test("ignores games with no day", async () => {
		resetG();
		await resetCache({});
		await idb.cache.games.put({
			gid: 7100,
			season: g.get("season"),
			teams: [],
		} as any);
		assert.strictEqual((await getLeaguePosition()).day, 0);
	});

	describe("comparing", () => {
		const at = (season: number, phase: number, day: number) => ({
			season,
			phase,
			day,
		});

		test("a day behind counts as behind", () => {
			assert.strictEqual(
				isBehindPosition(
					at(2005, PHASE.REGULAR_SEASON, 44),
					at(2005, PHASE.REGULAR_SEASON, 45),
				),
				true,
			);
		});

		test("level is not behind", () => {
			assert.strictEqual(
				isBehindPosition(
					at(2005, PHASE.REGULAR_SEASON, 45),
					at(2005, PHASE.REGULAR_SEASON, 45),
				),
				false,
			);
		});

		test("ahead is not behind", () => {
			assert.strictEqual(
				isBehindPosition(
					at(2005, PHASE.REGULAR_SEASON, 46),
					at(2005, PHASE.REGULAR_SEASON, 45),
				),
				false,
			);
		});

		// Phase outranks day, because day resets across a phase boundary - a
		// follower still in the regular season is behind the playoffs even though
		// its day number can be higher.
		test("phase outranks day", () => {
			assert.strictEqual(
				isBehindPosition(
					at(2005, PHASE.REGULAR_SEASON, 170),
					at(2005, PHASE.PLAYOFFS, 3),
				),
				true,
			);
			assert.strictEqual(
				isBehindPosition(
					at(2005, PHASE.PLAYOFFS, 3),
					at(2005, PHASE.REGULAR_SEASON, 170),
				),
				false,
			);
		});

		// And season outranks both, since phase and day both reset with it.
		test("season outranks phase", () => {
			assert.strictEqual(
				isBehindPosition(
					at(2005, PHASE.FREE_AGENCY, 170),
					at(2006, PHASE.PRESEASON, 0),
				),
				true,
			);
		});
	});

	describe("parsing what came off the wire", () => {
		test("accepts a whole position", () => {
			assert.deepStrictEqual(
				parseLeaguePosition({ season: 2005, phase: 1, day: 45 }),
				{ season: 2005, phase: 1, day: 45 },
			);
		});

		// An older client in the room writes no position at all, and a half-written
		// one must never be read as "day 0" - that would make every follower think
		// it is ahead of the room.
		test("rejects anything incomplete", () => {
			for (const value of [
				undefined,
				null,
				{},
				{ season: 2005, phase: 1 },
				{ season: 2005, day: 45 },
				{ season: "2005", phase: 1, day: 45 },
			]) {
				assert.strictEqual(parseLeaguePosition(value), undefined);
			}
		});
	});
});
