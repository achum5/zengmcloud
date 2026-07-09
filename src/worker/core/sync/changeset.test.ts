import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { player } from "../index.ts";
import { g, helpers, local } from "../../util/index.ts";
import { idb } from "../../db/index.ts";
import {
	DEFAULT_PHASE_CHANGE_REDIRECTS,
	PHASE,
} from "../../../common/constants.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import {
	applyChangeset,
	captureChangeset,
	phaseRedirectComponents,
} from "./changeset.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import type { Phase } from "../../../common/types.ts";

const genPlayer = () =>
	player.generate(g.get("userTid"), 30, 2017, true, DEFAULT_LEVEL);

// Simulate shipping a changeset over the network (JSON round-trip), the same as
// Firebase would.
const overTheWire = (changeset: unknown) =>
	JSON.parse(JSON.stringify(changeset));

describe("sync changeset", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	test("captures only changes made while enabled", async () => {
		resetG();
		await resetCache({ players: [genPlayer()] });

		// Setup happened while disabled, so nothing should be pending.
		changeTracker.enable();
		assert.strictEqual(changeTracker.size(), 0);

		// Writes record only inside a capture window (the worker wrapper opens one
		// around every cloud-tracked action).
		await changeTracker.runCaptured(async () => {
			const p = (await idb.cache.players.getAll())[0]!;
			p.tid = 5;
			await idb.cache.players.put(p);
		});

		const changeset = await captureChangeset();
		assert.strictEqual(changeset.changes.length, 1);
		assert.strictEqual(changeset.changes[0]!.store, "players");
		assert.strictEqual(changeset.changes[0]!.type, "put");
	});

	test("excludes device-local userTid but syncs userTids and league settings", async () => {
		resetG();
		await resetCache({});
		changeTracker.enable();
		changeTracker.reset();

		await changeTracker.runCaptured(async () => {
			await idb.cache.gameAttributes.put({ key: "userTid", value: 5 });
			await idb.cache.gameAttributes.put({ key: "userTids", value: [5, 6, 7] });
			await idb.cache.gameAttributes.put({ key: "salaryCap", value: 100000 });
		});

		const changeset = await captureChangeset();
		const keys = changeset.changes.map((c) => c.id);

		assert.ok(!keys.includes("userTid"), "userTid must NOT sync (per-device)");
		assert.ok(keys.includes("userTids"), "userTids must sync (multi-team set)");
		assert.ok(keys.includes("salaryCap"), "league settings must sync");
	});

	test("round-trips put, add, and delete to another device", async () => {
		// --- Source device: start with two players (pids 0 and 1) ---
		resetG();
		await resetCache({ players: [genPlayer(), genPlayer()] });
		changeTracker.enable();
		changeTracker.reset();

		const players = await idb.cache.players.getAll();
		const pA = players.find((p) => p.pid === 0)!;
		const pB = players.find((p) => p.pid === 1)!;

		// Modify pA, delete pB, add a brand new pC (gets pid 2).
		let pCid: unknown;
		await changeTracker.runCaptured(async () => {
			pA.tid = 5;
			await idb.cache.players.put(pA);
			await idb.cache.players.delete(pB.pid);
			pCid = await idb.cache.players.add(genPlayer());
		});

		const changeset = overTheWire(await captureChangeset());
		assert.strictEqual(changeset.changes.length, 3);

		// --- Target device: starts from the ORIGINAL state (pids 0 and 1) ---
		await resetCache({ players: [genPlayer(), genPlayer()] });
		changeTracker.disable(); // Receiver applies, doesn't record.

		await applyChangeset(changeset, { refreshUI: false });

		const after = await idb.cache.players.getAll();
		const byId = new Map(after.map((p) => [p.pid, p]));

		// pA updated
		assert.strictEqual(byId.get(0)!.tid, 5);
		// pB deleted
		assert.strictEqual(byId.has(1), false);
		// pC added
		assert.strictEqual(byId.has(pCid as number), true);
		assert.strictEqual(after.length, 2);
	});

	test("advancing daysLeft refreshes the free-agency status text on the receiver", async () => {
		resetG();
		g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
		await resetCache({});
		// Receiver is currently on day 29 of free agency.
		await idb.cache.gameAttributes.put({
			key: "phase",
			value: PHASE.FREE_AGENCY,
		});
		await idb.cache.gameAttributes.put({ key: "daysLeft", value: 29 });
		local.statusText = helpers.daysLeft(true, 29);
		assert.ok(local.statusText.includes("29"), local.statusText);

		// Sim authority device simmed a day and synced daysLeft → 28.
		changeTracker.disable();
		await applyChangeset(
			{
				changes: [
					{
						store: "gameAttributes",
						id: "daysLeft",
						type: "put",
						value: { key: "daysLeft", value: 28 },
					},
				],
			},
			{ refreshUI: false },
		);

		// g advanced AND the status line was recomputed (not frozen on 29).
		assert.strictEqual(g.get("daysLeft"), 28);
		assert.strictEqual(local.statusText, helpers.daysLeft(true, 28));
		assert.ok(local.statusText.includes("28"), local.statusText);
	});

	test("applying a remote change does NOT swallow a concurrent local edit", async () => {
		// The bug this guards: while applying a remote change, we must not drop a
		// local sim's writes to OTHER records - otherwise that sim is never
		// published (e.g. a sim right after this device takes over simming while
		// still catching up). Applying pid 0 must leave a local edit to pid 1
		// intact in the tracker.
		resetG();
		await resetCache({ players: [genPlayer(), genPlayer()] });
		changeTracker.enable();
		changeTracker.reset();

		const p0 = (await idb.cache.players.getAll()).find((p) => p.pid === 0)!;

		// A local edit to pid 1 (as a sim would make - sims hold a capture window).
		changeTracker.beginSim();
		const p1 = (await idb.cache.players.getAll()).find((p) => p.pid === 1)!;
		p1.tid = 9;
		await idb.cache.players.put(p1);
		changeTracker.endSim();

		// Apply a remote change to pid 0. It must forget only pid 0, not pid 1.
		await applyChangeset(
			{
				changes: [
					{ store: "players", id: 0, type: "put", value: { ...p0, tid: 5 } },
				],
			},
			{ refreshUI: false },
		);

		const captured = await captureChangeset();
		const ids = captured.changes.map((c) => c.id);
		assert.ok(ids.includes(1), JSON.stringify(captured.changes));
		// The applied record (pid 0) was forgotten - not re-broadcast.
		assert.ok(!ids.includes(0), JSON.stringify(captured.changes));
	});

	test("a synced phase change advances g.phase and refreshes on the receiver", async () => {
		// Every phase transition ships as one gameAttributes:phase change (only
		// finalize() writes it). The receiver must pick up the new phase in g - this
		// is what lets the header/play-menu/redirect all flip. Regression guard for
		// "follower stuck on the old phase".
		resetG();
		g.setWithoutSavingToDB("phase", PHASE.DRAFT_LOTTERY);
		await resetCache({});
		await idb.cache.gameAttributes.put({
			key: "phase",
			value: PHASE.DRAFT_LOTTERY,
		});
		assert.strictEqual(g.get("phase"), PHASE.DRAFT_LOTTERY);

		// The simmer advanced to the draft and synced phase → DRAFT.
		changeTracker.disable();
		await applyChangeset(
			{
				changes: [
					{
						store: "gameAttributes",
						id: "phase",
						type: "put",
						value: { key: "phase", value: PHASE.DRAFT },
					},
				],
			},
			{ refreshUI: false },
		);

		assert.strictEqual(g.get("phase"), PHASE.DRAFT);
	});

	test("phaseRedirectComponents mirrors finalize for every phase", () => {
		// The 6 phases whose newPhase* functions return a redirect - and which are
		// exactly the default phaseChangeRedirects. Each must map to the SAME
		// landing page finalize sends the simmer to.
		const expected: Partial<Record<Phase, string[]>> = {
			[PHASE.REGULAR_SEASON]: ["season_preview"],
			[PHASE.PLAYOFFS]: ["playoffs"],
			[PHASE.DRAFT_LOTTERY]: ["history"],
			[PHASE.DRAFT]: ["draft"],
			[PHASE.RESIGN_PLAYERS]: ["negotiation"],
			[PHASE.FREE_AGENCY]: ["free_agents"],
		};

		// The redirecting phases and the default setting must be the same set.
		assert.deepEqual(
			[...DEFAULT_PHASE_CHANGE_REDIRECTS].sort((a, b) => a - b),
			Object.keys(expected)
				.map(Number)
				.sort((a, b) => a - b),
		);

		for (const [phaseStr, components] of Object.entries(expected)) {
			const phase = Number(phaseStr) as Phase;
			assert.deepEqual(
				phaseRedirectComponents(phase, DEFAULT_PHASE_CHANGE_REDIRECTS),
				components,
				`phase ${phase} should redirect to ${components.join("/")}`,
			);
		}

		// Phases with no landing page never redirect (the receiver just refreshes
		// in place, same as the simmer).
		for (const phase of [
			PHASE.PRESEASON,
			PHASE.AFTER_TRADE_DEADLINE,
			PHASE.AFTER_DRAFT,
			PHASE.EXPANSION_DRAFT,
			PHASE.FANTASY_DRAFT,
		] as Phase[]) {
			assert.strictEqual(
				phaseRedirectComponents(phase, DEFAULT_PHASE_CHANGE_REDIRECTS),
				undefined,
				`phase ${phase} should not redirect`,
			);
		}

		// A phase dropped from the user's setting is not redirected, even though it
		// has a landing page (honors the opt-out, like finalize).
		assert.strictEqual(
			phaseRedirectComponents(
				PHASE.DRAFT,
				DEFAULT_PHASE_CHANGE_REDIRECTS.filter((p) => p !== PHASE.DRAFT),
			),
			undefined,
		);
	});

	test("heals a diverged-rid duplicate of a logically-keyed row (teamSeasons)", async () => {
		// The receiver already holds the 2075 row for team 4 under ITS own rid (7).
		// Another device created the SAME logical row under a different rid (99).
		// Naively putting it would leave two rows for (tid 4, season 2075), which
		// violates teamSeasons' unique index and aborts the next flush ("Index key
		// is not unique"). Reconciliation must drop the stale rid so exactly one
		// row - the author's - remains.
		resetG();
		await resetCache({
			teamSeasons: [{ rid: 7, tid: 4, season: 2075, won: 1, lost: 0 }],
		});
		changeTracker.disable();

		await applyChangeset(
			{
				changes: [
					{
						store: "teamSeasons",
						id: 99,
						type: "put",
						value: { rid: 99, tid: 4, season: 2075, won: 5, lost: 2 },
					},
				],
			},
			{ refreshUI: false },
		);

		const rows = await idb.cache.teamSeasons.getAll();
		const dupes = rows.filter((t) => t.tid === 4 && t.season === 2075);
		assert.strictEqual(dupes.length, 1, JSON.stringify(rows));
		assert.strictEqual(dupes[0]!.rid, 99);
		assert.strictEqual(dupes[0]!.won, 5);
	});

	test("reconciliation leaves other team-seasons untouched", async () => {
		// Only the row that shares the incoming logical identity is reconciled; a
		// different team's (or season's) row must never be collateral damage.
		resetG();
		await resetCache({
			teamSeasons: [
				{ rid: 7, tid: 4, season: 2075, won: 1, lost: 0 },
				{ rid: 8, tid: 5, season: 2075, won: 2, lost: 0 },
				{ rid: 9, tid: 4, season: 2074, won: 3, lost: 0 },
			],
		});
		changeTracker.disable();

		await applyChangeset(
			{
				changes: [
					{
						store: "teamSeasons",
						id: 99,
						type: "put",
						value: { rid: 99, tid: 4, season: 2075, won: 5, lost: 2 },
					},
				],
			},
			{ refreshUI: false },
		);

		const rows = await idb.cache.teamSeasons.getAll();
		// team 5 / 2075 and team 4 / 2074 are different identities - keep their rids.
		assert.strictEqual(
			rows.find((t) => t.tid === 5 && t.season === 2075)!.rid,
			8,
		);
		assert.strictEqual(
			rows.find((t) => t.tid === 4 && t.season === 2074)!.rid,
			9,
		);
		// team 4 / 2075 healed to a single row under the author's rid.
		const healed = rows.filter((t) => t.tid === 4 && t.season === 2075);
		assert.strictEqual(healed.length, 1);
		assert.strictEqual(healed[0]!.rid, 99);
	});

	test("applying a changeset does not itself get recorded", async () => {
		resetG();
		await resetCache({ players: [genPlayer()] });

		const p = (await idb.cache.players.getAll())[0]!;
		p.tid = 9;
		const changeset: any = {
			changes: [{ store: "players", id: p.pid, type: "put", value: p }],
		};

		// Receiver has tracking ON (e.g. it also makes local edits), but applying
		// a remote changeset must not re-capture those writes.
		changeTracker.enable();
		changeTracker.reset();
		await applyChangeset(changeset, { refreshUI: false });

		assert.strictEqual(changeTracker.size(), 0);
	});
});
