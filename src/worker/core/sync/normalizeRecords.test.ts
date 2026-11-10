import { assert, describe, test } from "vitest";
import { PHASE, PLAYER } from "../../../common/constants.ts";
import { helpers } from "../../util/index.ts";
import {
	contractSummaryOfRecord,
	hasLegacyContractRecords,
	upgradeLegacySnapshotStores,
} from "./normalizeRecords.ts";

const season = 2020;

// A player as a build from before p.contracts (league DB version 81) stores
// him: released by team 4 with two seasons left, now a free agent asking for
// less.
const legacyReleased = () => ({
	pid: 1,
	tid: PLAYER.FREE_AGENT,
	draft: { round: 2, pick: 5, tid: 9, originalTid: 9, year: season - 6 },
	retiredYear: Infinity,
	stats: [],
	transactions: [],
	contract: { amount: 1500, exp: season },
	salaries: [
		{ season: season - 1, amount: 3000 },
		{ season, amount: 3000 },
		{ season: season + 1, amount: 3000 },
	],
});

// A player already in the current shape, owed by team 3
const current = () => ({
	pid: 2,
	tid: PLAYER.FREE_AGENT,
	draft: { round: 1, pick: 5, tid: 9, originalTid: 9, year: season - 4 },
	retiredYear: Infinity,
	stats: [],
	contractDemand: { amount: 1000, exp: season },
	contracts: [
		{
			type: "freeAgent",
			start: season,
			amounts: [2000, 2000],
			payments: [],
			endedEarly: { season, phase: PHASE.PRESEASON, tid: 3 },
		},
	],
	owedByTids: [3],
});

const checkpointStores = () => ({
	gameAttributes: [
		{ key: "season", value: season },
		{ key: "phase", value: PHASE.REGULAR_SEASON },
		{ key: "numGames", value: [{ start: -Infinity, value: 82 }] },
	],
	players: [legacyReleased(), current()],
	releasedPlayers: [
		{ rid: 1, pid: 1, tid: 4, contract: { amount: 3000, exp: season + 1 } },
		// Already recorded on the current-shape player's contract
		{ rid: 2, pid: 2, tid: 3, contract: { amount: 2000, exp: season + 1 } },
	],
	events: [],
	teamSeasons: [],
});

describe("records from a build before p.contracts", () => {
	test("a checkpoint's players are converted, and its releasedPlayers store goes", () => {
		const stores: Record<string, unknown[]> = checkpointStores();
		upgradeLegacySnapshotStores(stores);

		assert.notProperty(stores, "releasedPlayers");

		const [legacy, converted] = stores.players as any[];
		assert.notProperty(legacy, "contract");
		assert.notProperty(legacy, "salaries");
		assert.deepEqual(legacy.contractDemand, { amount: 1500, exp: season });
		assert.deepEqual(legacy.owedByTids, [4]);
		const released = legacy.contracts.find(
			(contract: any) => contract.endedEarly?.tid === 4,
		);
		assert.ok(released, JSON.stringify(legacy.contracts));
		assert.deepEqual(released.amounts.at(-1), 3000);

		// Left exactly as it was
		assert.deepEqual(converted, current());
	});

	test("a checkpoint in the current shape is left alone", () => {
		const stores: Record<string, unknown[]> = {
			...checkpointStores(),
			players: [current()],
		};
		delete stores.releasedPlayers;
		const before = helpers.deepCopy(stores);
		upgradeLegacySnapshotStores(stores);
		assert.deepEqual(stores, before);
	});

	test("changes are recognized by shape", () => {
		assert.isFalse(
			hasLegacyContractRecords([
				{ store: "players", id: 2, type: "put", value: current() },
			]),
		);
		assert.isTrue(
			hasLegacyContractRecords([
				{ store: "players", id: 1, type: "put", value: legacyReleased() },
			]),
		);
		assert.isTrue(
			hasLegacyContractRecords([
				{ store: "releasedPlayers" as any, id: 1, type: "delete" },
			]),
		);
	});

	test("either shape's contract can be read", () => {
		assert.deepEqual(contractSummaryOfRecord(legacyReleased()), {
			amount: 1500,
			exp: season,
		});
		assert.deepEqual(contractSummaryOfRecord(current()), {
			amount: 1000,
			exp: season,
		});
		assert.isUndefined(contractSummaryOfRecord({ pid: 3 }));
	});
});
