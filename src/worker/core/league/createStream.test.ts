import { assert, describe, test } from "vitest";
import {
	applyTeamSeasonRidPolicy,
	importRowWeight,
	isDeviceLocalStoreForSyncedImport,
} from "./createStream.ts";

describe("isDeviceLocalStoreForSyncedImport", () => {
	test("a synced-league file's per-device stores are skipped on import", () => {
		// The file carries the EXPORTING device's staged trade / saved trades /
		// trading block; importing them would put a friend's personal state (e.g.
		// their team's trading block) on this device.
		for (const key of ["trade", "savedTrades", "savedTradingBlock"]) {
			assert.strictEqual(isDeviceLocalStoreForSyncedImport(key, true), true);
		}
		// Shared stores still import.
		assert.strictEqual(
			isDeviceLocalStoreForSyncedImport("players", true),
			false,
		);
		assert.strictEqual(
			isDeviceLocalStoreForSyncedImport("teamSeasons", true),
			false,
		);
	});

	test("single-player imports keep everything (restoring your own backup)", () => {
		for (const key of ["trade", "savedTrades", "savedTradingBlock"]) {
			assert.strictEqual(isDeviceLocalStoreForSyncedImport(key, false), false);
		}
	});
});

describe("applyTeamSeasonRidPolicy", () => {
	test("strips rids for a normal league file import", () => {
		const rows = [{ rid: 1 }, { rid: 2 }, { rid: 3 }];
		applyTeamSeasonRidPolicy(rows, false);
		assert.ok(rows.every((row) => row.rid === undefined));
	});

	test("preserves rids for a synced-league file (the join flow)", () => {
		// Renumbering these is what made a joining device's rids diverge from the
		// rest of its sync room, after which synced writes addressed by the
		// author's rid overwrote unrelated rows (the 2000-season wipe).
		const rows = [{ rid: 1 }, { rid: 3 }, { rid: 2 }];
		applyTeamSeasonRidPolicy(rows, true);
		assert.deepEqual(
			rows.map((row) => row.rid),
			[1, 3, 2],
		);
	});

	test("falls back to stripping when any rid is missing", () => {
		// Partial preservation could silently drop rows (two rows, one key), so
		// it's all-or-nothing.
		const rows: { rid?: unknown }[] = [{ rid: 1 }, {}, { rid: 3 }];
		applyTeamSeasonRidPolicy(rows, true);
		assert.ok(rows.every((row) => row.rid === undefined));
	});

	test("falls back to stripping when rids repeat", () => {
		const rows = [{ rid: 1 }, { rid: 1 }, { rid: 3 }];
		applyTeamSeasonRidPolicy(rows, true);
		assert.ok(rows.every((row) => row.rid === undefined));
	});

	test("falls back to stripping when a rid is not a number", () => {
		const rows: { rid?: unknown }[] = [{ rid: 1 }, { rid: "2" }, { rid: 3 }];
		applyTeamSeasonRidPolicy(rows, true);
		assert.ok(rows.every((row) => row.rid === undefined));
	});
});

describe("importRowWeight", () => {
	test("an ordinary row weighs one, so normal stores buffer as before", () => {
		assert.strictEqual(importRowWeight("players", { pid: 1 }), 1);
		assert.strictEqual(importRowWeight("games", { gid: 1 }), 1);
		assert.strictEqual(importRowWeight("events", { eid: 1 }), 1);
	});

	test("a saved replay weighs its play-by-play", () => {
		// The whole point: a replay is one row but the memory of thousands.
		// Buffering 10,000 of them (the old row-count cap) is gigabytes, which is
		// what killed a phone importing a league that had replays in it.
		assert.strictEqual(
			importRowWeight("liveGamePlayByPlay", {
				gid: 1,
				playByPlay: Array.from({ length: 850 }, () => ({ type: "fg" })),
			}),
			850,
		);
	});

	test("a replay of an unexpected shape is assumed heavy, not light", () => {
		// Guessing "light" on this store is the failure mode we are fixing, so an
		// unreadable row must not fall back to weight 1.
		assert.ok(importRowWeight("liveGamePlayByPlay", {}) >= 1000);
		assert.ok(
			importRowWeight("liveGamePlayByPlay", { playByPlay: [] }) >= 1000,
		);
		assert.ok(
			importRowWeight("liveGamePlayByPlay", { playByPlay: "nope" }) >= 1000,
		);
	});

	test("replays flush orders of magnitude sooner than the row cap would", () => {
		// Concretely: with ~850 events a game, a 10,000-weight budget flushes
		// after ~12 replays instead of 10,000 - roughly a 850x cut in peak
		// buffered memory for this store.
		const BUDGET = 10000;
		const perReplay = importRowWeight("liveGamePlayByPlay", {
			playByPlay: Array.from({ length: 850 }, () => ({ type: "fg" })),
		});
		const replaysBeforeFlush = Math.ceil(BUDGET / perReplay);
		assert.ok(replaysBeforeFlush < 20, `flushes after ${replaysBeforeFlush}`);
		assert.ok(replaysBeforeFlush >= 1, "must still buffer at least one");
	});
});
