import { assert, describe, test } from "vitest";
import {
	applyTeamSeasonRidPolicy,
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
