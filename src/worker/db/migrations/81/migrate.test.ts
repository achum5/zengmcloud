import * as fakeIndexedDB from "fake-indexeddb";
import { openDB } from "@dumbmatter/idb";
import { afterAll, assert, beforeAll, test } from "vitest";
import { PHASE, PLAYER } from "../../../../common/constants.ts";
import { resetG } from "../../../../test/helpers.ts";
import connectLeague from "../../connectLeague.ts";

// A fork league is at version 80 when it meets upstream's contracts upgrade,
// which upstream numbers 75 - so it has to run as 81, or it would be skipped.

const IDB_GLOBALS = [
	"IDBCursor",
	"IDBCursorWithValue",
	"IDBDatabase",
	"IDBIndex",
	"IDBKeyRange",
	"IDBObjectStore",
	"IDBOpenDBRequest",
	"IDBRequest",
	"IDBTransaction",
] as const;
const previousGlobals = new Map<string, unknown>();

beforeAll(() => {
	for (const name of [...IDB_GLOBALS, "indexedDB"]) {
		previousGlobals.set(name, (globalThis as any)[name]);
	}
	for (const name of IDB_GLOBALS) {
		(globalThis as any)[name] = fakeIndexedDB[name];
	}
	globalThis.indexedDB = new fakeIndexedDB.IDBFactory();
	resetG();
});

afterAll(() => {
	for (const [name, value] of previousGlobals) {
		if (value === undefined) {
			delete (globalThis as any)[name];
		} else {
			(globalThis as any)[name] = value;
		}
	}
});

const season = 2030;
const LID = 81;

// Only the stores the version 81 upgrade reads, as they were at version 80
const createVersion80 = async (players: unknown[]) => {
	const db = await openDB(`league${LID}`, 80, {
		upgrade(db) {
			db.createObjectStore("gameAttributes", { keyPath: "key" });
			const teamSeasons = db.createObjectStore("teamSeasons", {
				keyPath: "rid",
				autoIncrement: true,
			});
			teamSeasons.createIndex("season, tid", ["season", "tid"], {
				unique: true,
			});
			db.createObjectStore("releasedPlayers", {
				keyPath: "rid",
				autoIncrement: true,
			});
			db.createObjectStore("events", { keyPath: "eid", autoIncrement: true });
			const playerStore = db.createObjectStore("players", {
				keyPath: "pid",
				autoIncrement: true,
			});
			playerStore.createIndex("tid", "tid");
		},
	});
	const tx = db.transaction(
		["gameAttributes", "releasedPlayers", "players"],
		"readwrite",
	);
	await tx.objectStore("gameAttributes").put({ key: "season", value: season });
	await tx
		.objectStore("gameAttributes")
		.put({ key: "phase", value: PHASE.FREE_AGENCY });
	await tx.objectStore("gameAttributes").put({ key: "numGames", value: 82 });
	await tx.objectStore("releasedPlayers").put({
		pid: 1,
		tid: 4,
		contract: { amount: 3000, exp: season + 1 },
	});
	for (const p of players) {
		await tx.objectStore("players").put(p);
	}
	await tx.done;
	db.close();
};

test("version 80 leagues get p.contracts, and records already converted are left alone", async () => {
	// Released by team 4 this past season, owed one more
	const legacy = {
		pid: 1,
		tid: PLAYER.FREE_AGENT,
		draft: { round: 1, pick: 3, tid: 2, originalTid: 2, year: season - 5 },
		retiredYear: Infinity,
		stats: [],
		transactions: [],
		contract: { amount: 1200, exp: season + 1 },
		salaries: [
			{ season: season - 1, amount: 3000 },
			{ season, amount: 3000 },
			{ season: season + 1, amount: 3000 },
		],
		// Something only this fork stores on players
		collegeStars: 4,
	};

	// Received from a league mate's device on the newer build before this one
	// updated
	const alreadyConverted = {
		pid: 2,
		tid: 0,
		draft: { round: 2, pick: 3, tid: 2, originalTid: 2, year: season - 5 },
		retiredYear: Infinity,
		stats: [],
		transactions: [],
		contracts: [
			{
				type: "freeAgent",
				start: season,
				amounts: [2000, 2000],
				payments: [{ season, tid: 0, amount: 2000 }],
			},
		],
	};

	await createVersion80([legacy, alreadyConverted]);

	const db = await connectLeague(LID);
	try {
		assert.strictEqual(db.version, 81);
		assert.isFalse(db.objectStoreNames.contains("releasedPlayers" as any));

		const p1: any = await db.get("players", 1);
		assert.notProperty(p1, "contract");
		assert.notProperty(p1, "salaries");
		assert.strictEqual(p1.collegeStars, 4);
		assert.deepEqual(p1.contractDemand, { amount: 1200, exp: season + 1 });
		assert.deepEqual(p1.owedByTids, [4]);
		assert.strictEqual(p1.contracts.at(-1).endedEarly.tid, 4);

		assert.deepEqual(await db.get("players", 2), alreadyConverted as any);

		// Players owed money are indexed, for payrolls
		const owed = await db.getAllFromIndex("players", "owedByTids", 4);
		assert.deepEqual(
			owed.map((p) => p.pid),
			[1],
		);
	} finally {
		db.close();
	}
});
