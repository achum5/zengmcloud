import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { player, team } from "../index.ts";
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
	orderChangesForApply,
	phaseRedirectComponents,
	dropStrandedScheduleRows,
	findStrandedScheduleRows,
	regressionReason,
	sweepPhantomScheduleRows,
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

	test("an events delete stays local (eid diverges), but the player revert and event adds still sync", async () => {
		// Mirrors the sign/release UNDO: it reverts the player (put, keyed by pid)
		// and deletes the sign event (keyed by a diverging autoincrement eid).
		resetG();
		await resetCache({ players: [genPlayer()] });
		changeTracker.enable();
		changeTracker.reset();

		await changeTracker.runCaptured(async () => {
			// A brand-new event, then a delete of an earlier event: the add must
			// broadcast, the delete must NOT.
			await idb.cache.events.add({ type: "reSigned", pids: [0] } as any);
			await idb.cache.events.delete(7); // some earlier sign event's local eid
			const p = (await idb.cache.players.getAll())[0]!;
			p.tid = -1; // reverted to free agent
			await idb.cache.players.put(p);
		});

		const changeset = overTheWire(await captureChangeset());
		const eventDeletes = changeset.changes.filter(
			(c: any) => c.store === "events" && c.type === "delete",
		);
		const eventPuts = changeset.changes.filter(
			(c: any) => c.store === "events" && c.type === "put",
		);
		const playerPuts = changeset.changes.filter(
			(c: any) => c.store === "players" && c.type === "put",
		);
		assert.strictEqual(eventDeletes.length, 0, "event delete must stay local");
		assert.strictEqual(eventPuts.length, 1, "event add must still sync");
		assert.strictEqual(playerPuts.length, 1, "player revert must still sync");
		assert.strictEqual(playerPuts[0].value.tid, -1);
	});

	test("watch flags never cross devices: incoming watch is dropped, local watch survives", async () => {
		// --- Source device: watches both players, then edits them ---
		resetG();
		await resetCache({ players: [genPlayer(), genPlayer()] });
		changeTracker.enable();
		changeTracker.reset();

		await changeTracker.runCaptured(async () => {
			const players = await idb.cache.players.getAll();
			for (const p of players) {
				p.watch = 1;
				p.tid = 5;
				await idb.cache.players.put(p);
			}
		});
		const changeset = overTheWire(await captureChangeset());
		assert.strictEqual(changeset.changes.length, 2);
		// The author's records DO carry its watch flags over the wire...
		assert.ok(changeset.changes.every((c: any) => c.value.watch === 1));

		// --- Target device: never watched pid 0, watches pid 1 with color 3 ---
		await resetCache({ players: [genPlayer(), genPlayer()] });
		const mine = await idb.cache.players.get(1);
		mine!.watch = 3;
		await idb.cache.players.put(mine!);
		changeTracker.disable();

		await applyChangeset(changeset, { refreshUI: false });

		// ...but the receiver never imports them: pid 0 stays unwatched, pid 1
		// keeps ITS OWN color, and the rest of the record still applied.
		const p0 = await idb.cache.players.get(0);
		const p1 = await idb.cache.players.get(1);
		assert.strictEqual(p0!.watch, undefined);
		assert.strictEqual(p1!.watch, 3);
		assert.strictEqual(p0!.tid, 5);
		assert.strictEqual(p1!.tid, 5);
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

	test("a synced teamSeasons delete removes the row by identity, not the author's rid", async () => {
		// THE WIPE THIS GUARDS AGAINST. teamSeasons `rid` is autoincrement and
		// diverges across devices. The author deleted ITS (tid 4, 2075) row, which
		// happened to be rid 7 there. On the receiver, rid 7 is a DIFFERENT, much
		// older season (tid 4, 2052), and the receiver's own (tid 4, 2075) row is
		// rid 500. A raw delete-by-rid would erase the wrong (2052) row - exactly
		// the observed multi-season teamSeasons wipe. The identity snapshot must
		// make the receiver delete (tid 4, 2075) and leave (tid 4, 2052) intact.
		resetG();
		await resetCache({
			teamSeasons: [
				{ rid: 7, tid: 4, season: 2052, won: 10, lost: 5 },
				{ rid: 500, tid: 4, season: 2075, won: 1, lost: 0 },
			],
		});
		changeTracker.disable();

		await applyChangeset(
			overTheWire({
				changes: [
					{
						store: "teamSeasons",
						id: 7, // the author's rid for ITS (tid 4, 2075) row
						type: "delete",
						value: { rid: 7, tid: 4, season: 2075, won: 3, lost: 2 },
					},
				],
			}),
			{ refreshUI: false },
		);

		const rows = await idb.cache.teamSeasons.getAll();
		// The 2075 row (the real identity match) is gone...
		assert.strictEqual(
			rows.find((t) => t.tid === 4 && t.season === 2075),
			undefined,
			JSON.stringify(rows),
		);
		// ...and the unrelated 2052 row that merely shared the author's rid survives.
		const survivor = rows.find((t) => t.tid === 4 && t.season === 2052);
		assert.ok(survivor, JSON.stringify(rows));
		assert.strictEqual(survivor!.rid, 7);
	});

	test("a teamSeasons delete for a missing identity is a safe no-op", async () => {
		// The author deleted a row the receiver never had (or already removed). The
		// identity lookup finds nothing, so nothing is deleted - and crucially no
		// unrelated row sharing the author's rid is touched.
		resetG();
		await resetCache({
			teamSeasons: [{ rid: 7, tid: 4, season: 2052, won: 10, lost: 5 }],
		});
		changeTracker.disable();

		await applyChangeset(
			overTheWire({
				changes: [
					{
						store: "teamSeasons",
						id: 7,
						type: "delete",
						value: { rid: 7, tid: 9, season: 2075, won: 3, lost: 2 },
					},
				],
			}),
			{ refreshUI: false },
		);

		const rows = await idb.cache.teamSeasons.getAll();
		assert.strictEqual(rows.length, 1, JSON.stringify(rows));
		assert.strictEqual(rows[0]!.tid, 4);
		assert.strictEqual(rows[0]!.season, 2052);
	});

	test("captures a teamSeasons delete with its identity snapshot", async () => {
		resetG();
		await resetCache({
			teamSeasons: [{ rid: 7, tid: 4, season: 2075, won: 1, lost: 0 }],
		});
		changeTracker.enable();
		changeTracker.reset();

		await changeTracker.runCaptured(async () => {
			const ts = await idb.cache.teamSeasons.indexGet(
				"teamSeasonsByTidSeason",
				[4, 2075],
			);
			await idb.cache.teamSeasons.delete(ts!.rid);
		});

		const changeset = overTheWire(await captureChangeset());
		assert.strictEqual(changeset.changes.length, 1);
		const c = changeset.changes[0]!;
		assert.strictEqual(c.type, "delete");
		assert.strictEqual(c.store, "teamSeasons");
		// The identity snapshot rides along so the receiver deletes by (tid, season).
		assert.strictEqual(c.value.tid, 4);
		assert.strictEqual(c.value.season, 2075);
	});

	test("a players delete ships no identity snapshot (only logically-keyed stores need one)", async () => {
		resetG();
		await resetCache({ players: [genPlayer(), genPlayer()] });
		changeTracker.enable();
		changeTracker.reset();

		await changeTracker.runCaptured(async () => {
			await idb.cache.players.delete(1);
		});

		const changeset = overTheWire(await captureChangeset());
		const del = changeset.changes.find((c: any) => c.type === "delete")!;
		assert.strictEqual(del.store, "players");
		assert.strictEqual(del.value, undefined);
	});

	test("a synced draftPicks lottery put lands on the pick with matching identity, not the author's diverged dpid", async () => {
		// THE "FUTURE LOTTERY" BUG. draftPicks `dpid` is autoincrement and diverges
		// across devices. The lottery writes the current (2084) season's draft ORDER
		// onto each pick and syncs it by the author's dpid (7 there). On this
		// re-imported receiver dpid 7 is a FUTURE (2085) pick, and its own 2084 pick
		// is dpid 9. A raw put-by-dpid set the 2085 pick's order (looked like next
		// year's lottery ran) and left a duplicate 2084 pick. Identity reconcile must
		// land the order on the one real (2084, R1, orig 4) pick.
		resetG();
		await resetCache({
			draftPicks: [
				{ dpid: 7, tid: 4, originalTid: 4, round: 1, pick: 0, season: 2085 },
				{ dpid: 9, tid: 4, originalTid: 4, round: 1, pick: 0, season: 2084 },
			],
		});
		changeTracker.disable();

		await applyChangeset(
			overTheWire({
				changes: [
					{
						store: "draftPicks",
						id: 7, // the author's dpid for ITS (2084, R1, orig 4) pick
						type: "put",
						value: {
							dpid: 7,
							tid: 4,
							originalTid: 4,
							round: 1,
							pick: 5,
							season: 2084,
						},
					},
				],
			}),
			{ refreshUI: false },
		);

		const picks = await idb.cache.draftPicks.getAll();
		// Exactly one 2084 R1 (orig 4) pick, carrying the lottery order.
		const current = picks.filter(
			(p) => p.season === 2084 && p.round === 1 && p.originalTid === 4,
		);
		assert.strictEqual(current.length, 1, JSON.stringify(picks));
		assert.strictEqual(current[0]!.pick, 5);
		// And no FUTURE (2085) pick was given the current lottery's order.
		assert.strictEqual(
			picks.find((p) => p.season === 2085 && p.pick === 5),
			undefined,
			JSON.stringify(picks),
		);
	});

	test("a synced draftPicks delete removes the pick by identity, not the author's dpid", async () => {
		resetG();
		await resetCache({
			draftPicks: [
				{ dpid: 7, tid: 4, originalTid: 4, round: 1, pick: 0, season: 2085 },
				{ dpid: 9, tid: 4, originalTid: 4, round: 1, pick: 0, season: 2084 },
			],
		});
		changeTracker.disable();

		await applyChangeset(
			overTheWire({
				changes: [
					{
						store: "draftPicks",
						id: 7, // the author's dpid for ITS (2084, R1, orig 4) pick
						type: "delete",
						value: {
							dpid: 7,
							tid: 4,
							originalTid: 4,
							round: 1,
							pick: 3,
							season: 2084,
						},
					},
				],
			}),
			{ refreshUI: false },
		);

		const picks = await idb.cache.draftPicks.getAll();
		// The 2084 pick (the real identity match) is gone...
		assert.strictEqual(
			picks.find(
				(p) => p.season === 2084 && p.round === 1 && p.originalTid === 4,
			),
			undefined,
			JSON.stringify(picks),
		);
		// ...and the future 2085 pick that merely shared the author's dpid survives.
		const survivor = picks.find(
			(p) => p.season === 2085 && p.round === 1 && p.originalTid === 4,
		);
		assert.ok(survivor, JSON.stringify(picks));
		assert.strictEqual(survivor!.dpid, 7);
	});

	test("captures a draftPicks delete with its identity snapshot", async () => {
		resetG();
		await resetCache({
			draftPicks: [
				{ dpid: 7, tid: 4, originalTid: 4, round: 1, pick: 0, season: 2084 },
			],
		});
		changeTracker.enable();
		changeTracker.reset();

		await changeTracker.runCaptured(async () => {
			await idb.cache.draftPicks.delete(7);
		});

		const changeset = overTheWire(await captureChangeset());
		const c = changeset.changes.find(
			(x: any) => x.type === "delete" && x.store === "draftPicks",
		)!;
		assert.ok(c);
		// The identity snapshot rides along so the receiver deletes by
		// (season, round, originalTid).
		assert.strictEqual(c.value.season, 2084);
		assert.strictEqual(c.value.round, 1);
		assert.strictEqual(c.value.originalTid, 4);
	});

	test("a put whose author rid points at an UNRELATED local row never clobbers it (the 2000-season wipe)", async () => {
		// THE INCIDENT THIS GUARDS AGAINST. The receiver joined by importing an
		// export, which renumbered its teamSeasons rids into per-team interleaved
		// order: (tid 1, 2001) landed at rid 4 and (tid 15, 2000) at rid 31. The
		// author (original device) has sequential numbering, where rid 31 is ITS
		// (tid 1, 2001) row. When the author simmed 2001 games, its put arrived
		// addressed to rid 31 - and used to blindly overwrite the receiver's
		// (tid 15, 2000) row sitting there, wiping that team's 2000 history. The
		// put must instead update (tid 1, 2001) in place under its LOCAL rid.
		resetG();
		await resetCache({
			teamSeasons: [
				{ rid: 4, tid: 1, season: 2001, won: 8, lost: 3 },
				{ rid: 31, tid: 15, season: 2000, won: 40, lost: 42 },
			],
		});
		changeTracker.disable();

		await applyChangeset(
			overTheWire({
				changes: [
					{
						store: "teamSeasons",
						id: 31, // the author's rid for ITS (tid 1, 2001) row
						type: "put",
						value: { rid: 31, tid: 1, season: 2001, won: 9, lost: 3 },
					},
				],
			}),
			{ refreshUI: false },
		);

		const rows = await idb.cache.teamSeasons.getAll();
		assert.strictEqual(rows.length, 2, JSON.stringify(rows));
		// The unrelated 2000-season row that merely held the author's rid survives.
		const survivor = rows.find((t) => t.tid === 15 && t.season === 2000);
		assert.ok(survivor, JSON.stringify(rows));
		assert.strictEqual(survivor!.rid, 31);
		assert.strictEqual(survivor!.won, 40);
		// The incoming row updated ITS identity in place, under the LOCAL rid.
		const updated = rows.find((t) => t.tid === 1 && t.season === 2001);
		assert.ok(updated, JSON.stringify(rows));
		assert.strictEqual(updated!.rid, 4);
		assert.strictEqual(updated!.won, 9);
	});

	test("a brand-new row whose author rid is occupied lands under a fresh local rid", async () => {
		// No local row shares the incoming identity, but the author's rid slot is
		// taken by an unrelated row: the new row must be INSERTED (fresh local
		// rid), not put on top of the occupant.
		resetG();
		await resetCache({
			teamSeasons: [{ rid: 31, tid: 15, season: 2000, won: 40, lost: 42 }],
		});
		changeTracker.disable();

		await applyChangeset(
			overTheWire({
				changes: [
					{
						store: "teamSeasons",
						id: 31,
						type: "put",
						value: { rid: 31, tid: 1, season: 2001, won: 9, lost: 3 },
					},
				],
			}),
			{ refreshUI: false },
		);

		const rows = await idb.cache.teamSeasons.getAll();
		assert.strictEqual(rows.length, 2, JSON.stringify(rows));
		const occupant = rows.find((t) => t.tid === 15 && t.season === 2000);
		assert.ok(occupant, JSON.stringify(rows));
		assert.strictEqual(occupant!.rid, 31);
		assert.strictEqual(occupant!.won, 40);
		const added = rows.find((t) => t.tid === 1 && t.season === 2001);
		assert.ok(added, JSON.stringify(rows));
		assert.notStrictEqual(added!.rid, 31);
		assert.strictEqual(added!.won, 9);
	});

	test("an in-place apply forgets the rid it wrote, not an unrelated pending edit at the author's rid", async () => {
		// The receiver has a concurrent local edit pending for the row at rid 31
		// (an unrelated 2000-season row). An incoming put addressed to rid 31 but
		// belonging to (tid 1, 2001) applies in place at rid 4. Forgetting must
		// target rid 4 (what we wrote) - forgetting the author's rid 31 would both
		// swallow the local pending edit AND leave our own write pending.
		resetG();
		await resetCache({
			teamSeasons: [
				{ rid: 4, tid: 1, season: 2001, won: 8, lost: 3 },
				{ rid: 31, tid: 15, season: 2000, won: 40, lost: 42 },
			],
		});
		changeTracker.enable();
		changeTracker.reset();

		// A local edit to the rid-31 row, as a concurrent sim would make.
		changeTracker.beginSim();
		const local2000 = (await idb.cache.teamSeasons.getAll()).find(
			(t) => t.rid === 31,
		)!;
		local2000.won = 41;
		await idb.cache.teamSeasons.put(local2000);
		changeTracker.endSim();

		await applyChangeset(
			overTheWire({
				changes: [
					{
						store: "teamSeasons",
						id: 31,
						type: "put",
						value: { rid: 31, tid: 1, season: 2001, won: 9, lost: 3 },
					},
				],
			}),
			{ refreshUI: false },
		);

		const captured = await captureChangeset();
		const ids = captured.changes.map((c) => c.id);
		// The local pending edit (rid 31) survives to broadcast...
		assert.ok(ids.includes(31), JSON.stringify(captured.changes));
		// ...and the applied write (landed at rid 4) is not re-broadcast.
		assert.ok(!ids.includes(4), JSON.stringify(captured.changes));
	});

	test("a synced releasedPlayers delete removes the row matching the contract, not the author's rid", async () => {
		// releasedPlayers rids are renumbered by import (expired contracts leave
		// gaps), so the author's rid addresses a different released contract here.
		// The delete must match by content (pid, tid, contract).
		resetG();
		await resetCache({
			releasedPlayers: [
				{ rid: 1, pid: 100, tid: 3, contract: { amount: 1000, exp: 2026 } },
				{ rid: 2, pid: 200, tid: 4, contract: { amount: 2000, exp: 2027 } },
			],
		});
		changeTracker.disable();

		await applyChangeset(
			overTheWire({
				changes: [
					{
						store: "releasedPlayers",
						id: 1, // the author's rid for ITS (pid 200) row
						type: "delete",
						value: {
							rid: 1,
							pid: 200,
							tid: 4,
							contract: { amount: 2000, exp: 2027 },
						},
					},
				],
			}),
			{ refreshUI: false },
		);

		const rows = await idb.cache.releasedPlayers.getAll();
		assert.strictEqual(rows.length, 1, JSON.stringify(rows));
		// The pid-100 row that merely held the author's rid survives.
		assert.strictEqual(rows[0]!.pid, 100);
	});

	test("a synced releasedPlayers put never clobbers a different released contract at the author's rid", async () => {
		resetG();
		await resetCache({
			releasedPlayers: [
				{ rid: 1, pid: 100, tid: 3, contract: { amount: 1000, exp: 2026 } },
			],
		});
		changeTracker.disable();

		// The author released pid 300; its row got rid 1 there.
		await applyChangeset(
			overTheWire({
				changes: [
					{
						store: "releasedPlayers",
						id: 1,
						type: "put",
						value: {
							rid: 1,
							pid: 300,
							tid: 5,
							contract: { amount: 3000, exp: 2028 },
						},
					},
				],
			}),
			{ refreshUI: false },
		);

		const rows = await idb.cache.releasedPlayers.getAll();
		assert.strictEqual(rows.length, 2, JSON.stringify(rows));
		const kept = rows.find((row) => row.pid === 100);
		assert.ok(kept, JSON.stringify(rows));
		assert.strictEqual(kept!.rid, 1);
		const added = rows.find((row) => row.pid === 300);
		assert.ok(added, JSON.stringify(rows));
		assert.notStrictEqual(added!.rid, 1);
	});

	test("a synced scheduledEvents delete removes the row matching the snapshot's content, not the author's id", async () => {
		// scheduledEvents ids are renumbered by import (processed events leave
		// gaps). The author processed and deleted ITS id 5; on this device that
		// content lives at id 6. The content match must delete id 6 and leave the
		// unrelated event at id 5 (which would otherwise fire twice/never).
		resetG();
		await resetCache({
			scheduledEvents: [
				{
					id: 5,
					type: "gameAttributes",
					season: 2031,
					phase: 0,
					info: { threePointers: true },
				},
				{
					id: 6,
					type: "gameAttributes",
					season: 2030,
					phase: 0,
					info: { salaryCap: 50000 },
				},
			],
		});
		changeTracker.disable();

		await applyChangeset(
			overTheWire({
				changes: [
					{
						store: "scheduledEvents",
						id: 5, // the author's id for ITS (2030, salaryCap) event
						type: "delete",
						value: {
							id: 5,
							type: "gameAttributes",
							season: 2030,
							phase: 0,
							info: { salaryCap: 50000 },
						},
					},
				],
			}),
			{ refreshUI: false },
		);

		const rows = await idb.cache.scheduledEvents.getAll();
		assert.strictEqual(rows.length, 1, JSON.stringify(rows));
		assert.strictEqual(rows[0]!.id, 5);
		assert.strictEqual(rows[0]!.season, 2031);
	});

	test("a scheduledEvents delete with no content match falls back to the raw id", async () => {
		resetG();
		await resetCache({
			scheduledEvents: [
				{
					id: 5,
					type: "gameAttributes",
					season: 2031,
					phase: 0,
					info: { threePointers: true },
				},
			],
		});
		changeTracker.disable();

		await applyChangeset(
			overTheWire({
				changes: [
					{
						store: "scheduledEvents",
						id: 5,
						type: "delete",
						value: {
							id: 5,
							type: "gameAttributes",
							season: 2031,
							phase: 0,
							// Content drifted (edited after the receiver last synced), so no
							// content match - same-id fallback still applies the delete.
							info: { threePointers: false },
						},
					},
				],
			}),
			{ refreshUI: false },
		);

		const rows = await idb.cache.scheduledEvents.getAll();
		assert.strictEqual(rows.length, 0, JSON.stringify(rows));
	});

	test("captures a scheduledEvents delete with its content snapshot", async () => {
		resetG();
		await resetCache({
			scheduledEvents: [
				{
					id: 5,
					type: "gameAttributes",
					season: 2030,
					phase: 0,
					info: { salaryCap: 50000 },
				},
			],
		});
		changeTracker.enable();
		changeTracker.reset();

		await changeTracker.runCaptured(async () => {
			await idb.cache.scheduledEvents.delete(5);
		});

		const changeset = overTheWire(await captureChangeset());
		assert.strictEqual(changeset.changes.length, 1);
		const c = changeset.changes[0]!;
		assert.strictEqual(c.type, "delete");
		assert.strictEqual(c.store, "scheduledEvents");
		// The content snapshot rides along so the receiver deletes by content.
		assert.strictEqual(c.value.season, 2030);
		assert.deepEqual(c.value.info, { salaryCap: 50000 });
	});

	test("a teamSeasons delete WITHOUT an identity snapshot is skipped, never applied by raw rid", async () => {
		// If the snapshot is missing (the row wasn't in the author's cache), a raw
		// delete-by-rid could erase an unrelated row on a diverged device. A stale
		// leftover row is recoverable; a wrong-row delete is not.
		resetG();
		await resetCache({
			teamSeasons: [{ rid: 7, tid: 4, season: 2052, won: 10, lost: 5 }],
		});
		changeTracker.disable();

		await applyChangeset(
			overTheWire({
				changes: [{ store: "teamSeasons", id: 7, type: "delete" }],
			}),
			{ refreshUI: false },
		);

		const rows = await idb.cache.teamSeasons.getAll();
		assert.strictEqual(rows.length, 1, JSON.stringify(rows));
		assert.strictEqual(rows[0]!.rid, 7);
	});

	test("an identity put sweeps stray duplicate rows for the same logical identity", async () => {
		// The cache's unique index can only surface ONE row per (tid, season), so
		// if a duplicate ever sneaks into the store (a transiently-missed lookup,
		// a partially-applied history), identity lookups can't see it - it
		// lingers, violating the on-disk unique index or resurfacing under a
		// fresh high rid where season-range consumers pick it up as the "latest"
		// row (that misdirected a game result onto the previous season). Applying
		// a put for the identity must leave exactly one row.
		resetG();
		await resetCache({
			teamSeasons: [
				{ rid: 18, tid: 8, season: 2001, won: 58, lost: 24 },
				{ rid: 88, tid: 8, season: 2001, won: 58, lost: 24 },
				{ rid: 67, tid: 8, season: 2002, won: 0, lost: 0 },
			],
		});
		changeTracker.disable();

		await applyChangeset(
			overTheWire({
				changes: [
					{
						store: "teamSeasons",
						id: 18,
						type: "put",
						value: { rid: 18, tid: 8, season: 2001, won: 58, lost: 25 },
					},
				],
			}),
			{ refreshUI: false },
		);

		const rows = await idb.cache.teamSeasons.getAll();
		const dupes = rows.filter((t) => t.tid === 8 && t.season === 2001);
		assert.strictEqual(dupes.length, 1, JSON.stringify(rows));
		assert.strictEqual(dupes[0]!.lost, 25);
		// The unrelated 2002 row is untouched.
		assert.ok(rows.some((t) => t.tid === 8 && t.season === 2002));
	});

	test("gameAttributes apply LAST, so an interrupted apply never leaves the season ahead of its data", async () => {
		// A season rollover writes `season` BEFORE creating the new season's rows,
		// so in capture order the season flip precedes the data. If the apply is
		// interrupted partway (a bad record, a killed tab), applying in capture
		// order would leave this device living in the new season with the new
		// season's teamSeasons/players missing - moods, standings, and rosters all
		// compute against a hole. Ordering gameAttributes last makes them the
		// commit point: the failed apply below must leave `season` untouched.
		resetG();
		await resetCache({});
		await idb.cache.gameAttributes.put({ key: "season", value: 2001 });
		changeTracker.disable();

		let threw = false;
		try {
			await applyChangeset(
				overTheWire({
					changes: [
						{
							store: "gameAttributes",
							id: "season",
							type: "put",
							value: { key: "season", value: 2002 },
						},
						// A poison data record: putting `undefined` throws in the cache.
						{ store: "players", id: 99, type: "put", value: undefined },
					],
				}),
				{ refreshUI: false },
			);
		} catch {
			threw = true;
		}

		assert.ok(threw, "expected the poison record to fail the apply");
		const season = await idb.cache.gameAttributes.get("season");
		assert.strictEqual(
			(season as any).value,
			2001,
			"season must NOT have advanced past its missing data",
		);
	});

	test("orderChangesForApply moves gameAttributes last and keeps relative order", () => {
		const ordered = orderChangesForApply([
			{ store: "gameAttributes", id: "season", type: "put", value: 1 },
			{ store: "teamSeasons", id: 1, type: "put", value: {} },
			{ store: "gameAttributes", id: "phase", type: "put", value: 2 },
			{ store: "players", id: 5, type: "put", value: {} },
		] as any);
		assert.deepEqual(
			ordered.map((c) => `${c.store}:${c.id}`),
			[
				"teamSeasons:1",
				"players:5",
				"gameAttributes:season",
				"gameAttributes:phase",
			],
		);
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

	test("one poison record does not block the rest of the changeset (schedule deletes still land)", async () => {
		// The field failure: a day's changeset carried game puts, a record that
		// deterministically failed on ONE device, and the day's schedule deletes.
		// Aborting at the failure left the games applied but the schedule rows
		// alive forever - the day showed as both played and upcoming. Now every
		// healthy record must apply, and the throw at the end still marks the
		// changeset failed so the engine retries it.
		resetG();
		await resetCache({});
		changeTracker.disable();

		// The receiver's stale state: an unplayed schedule row for gid 500.
		await idb.cache.schedule.add({ homeTid: 0, awayTid: 1, day: 38 } as any);
		const scheduleRow = (await idb.cache.schedule.getAll())[0]!;

		const changeset: any = {
			changes: [
				// The played game...
				{
					store: "games",
					id: scheduleRow.gid,
					type: "put",
					value: { gid: scheduleRow.gid, season: 2003, teams: [] },
				},
				// ...a poison record (no primary key on a non-autoincrement store
				// throws deterministically)...
				{ store: "games", id: 999999, type: "put", value: {} },
				// ...and the schedule delete that used to be held hostage behind it.
				{ store: "schedule", id: scheduleRow.gid, type: "delete" },
			],
		};

		let threw = false;
		try {
			await applyChangeset(changeset, { refreshUI: false });
		} catch {
			threw = true;
		}

		assert.ok(threw, "the aggregate failure must still throw (watermark pins)");
		const games = await idb.cache.games.getAll();
		assert.ok(
			games.some((game) => game.gid === scheduleRow.gid),
			"the game put before the poison record must have applied",
		);
		const schedule = await idb.cache.schedule.getAll();
		assert.strictEqual(
			schedule.length,
			0,
			"the schedule delete after the poison record must have applied",
		);
	});

	test("sweepPhantomScheduleRows removes played games from the schedule, keeps future ones", async () => {
		resetG();
		await resetCache({});
		changeTracker.disable();

		// A played game whose schedule row survived (the corruption), plus a
		// genuinely upcoming game that must be left alone.
		await idb.cache.schedule.add({ homeTid: 0, awayTid: 1, day: 38 } as any);
		await idb.cache.schedule.add({ homeTid: 2, awayTid: 3, day: 43 } as any);
		const rows = await idb.cache.schedule.getAll();
		const phantom = rows[0]!;
		const future = rows[1]!;
		await idb.cache.games.put({
			gid: phantom.gid,
			season: 2003,
			teams: [],
		} as any);

		const removed = await sweepPhantomScheduleRows();

		assert.strictEqual(removed, 1);
		const after = await idb.cache.schedule.getAll();
		assert.strictEqual(after.length, 1);
		assert.strictEqual(after[0]!.gid, future.gid);
	});
	// The failure this exists for: one device never received a day's changeset -
	// neither the games nor the schedule deletes - while every later day arrived
	// normally. Nothing detected it, and because getSchedule takes "today" from
	// the first schedule row (rows are keyed by gid, so the missed day sorts
	// first), the device sat pinned on a day the league had played through 13
	// days earlier, offering to re-sim games that already had results elsewhere.
	describe("stranded schedule rows", () => {
		const setup = async () => {
			resetG();
			await resetCache({});
			changeTracker.disable();
		};

		test("flags rows on a day the league has already played past", async () => {
			await setup();

			// Day 16 never landed: its rows are still here and it has no games.
			await idb.cache.schedule.add({ homeTid: 0, awayTid: 1, day: 16 } as any);
			await idb.cache.schedule.add({ homeTid: 2, awayTid: 3, day: 16 } as any);
			// Day 29 is upcoming, which is fine.
			await idb.cache.schedule.add({ homeTid: 4, awayTid: 5, day: 29 } as any);
			// ...but days 17 and 28 were played, so 16 can never still be upcoming.
			await idb.cache.games.put({
				gid: 9001,
				season: g.get("season"),
				day: 17,
				teams: [],
			} as any);
			await idb.cache.games.put({
				gid: 9002,
				season: g.get("season"),
				day: 28,
				teams: [],
			} as any);

			const stranded = await findStrandedScheduleRows();
			assert.deepStrictEqual(stranded.days, [16]);
			assert.strictEqual(stranded.gids.length, 2);
			assert.strictEqual(stranded.maxPlayedDay, 28);

			// And dropping them leaves the genuinely-upcoming day alone.
			const removed = await dropStrandedScheduleRows(stranded.gids);
			assert.strictEqual(removed, 2);
			const after = await idb.cache.schedule.getAll();
			assert.strictEqual(after.length, 1);
			assert.strictEqual(after[0]!.day, 29);
		});

		// A day being simmed right now has played games and unplayed rows on the
		// SAME day. Flagging that would delete the rest of a slate mid-sim.
		test("leaves a half-played day alone", async () => {
			await setup();

			await idb.cache.schedule.add({ homeTid: 0, awayTid: 1, day: 20 } as any);
			await idb.cache.schedule.add({ homeTid: 2, awayTid: 3, day: 21 } as any);
			await idb.cache.games.put({
				gid: 9003,
				season: g.get("season"),
				day: 20,
				teams: [],
			} as any);

			const stranded = await findStrandedScheduleRows();
			assert.deepStrictEqual(stranded.days, []);
			assert.strictEqual(stranded.gids.length, 0);
		});

		// A fresh season has a full schedule and no games yet - every row is
		// legitimately in the future.
		test("says nothing before any game has been played", async () => {
			await setup();

			await idb.cache.schedule.add({ homeTid: 0, awayTid: 1, day: 1 } as any);
			await idb.cache.schedule.add({ homeTid: 2, awayTid: 3, day: 2 } as any);

			const stranded = await findStrandedScheduleRows();
			assert.strictEqual(stranded.maxPlayedDay, undefined);
			assert.strictEqual(stranded.gids.length, 0);
		});

		// Games from before the day field existed must not be read as "day 0
		// played", which would strand nothing but would also mask a real gap.
		test("ignores games with no day", async () => {
			await setup();

			await idb.cache.schedule.add({ homeTid: 0, awayTid: 1, day: 5 } as any);
			await idb.cache.games.put({
				gid: 9004,
				season: g.get("season"),
				teams: [],
			} as any);

			const stranded = await findStrandedScheduleRows();
			assert.strictEqual(stranded.maxPlayedDay, undefined);
			assert.strictEqual(stranded.gids.length, 0);
		});
	});
	// The backstop for the whole class of ordering bugs.
	//
	// Applying changesets in the order they were authored is what makes
	// record-level last-write-wins correct, and getting it wrong has produced the
	// same failure three separate ways now. Each was fixed, but the cost when one
	// slips through is silent corruption found days later - a phase snapping back
	// to AFTER_DRAFT, a team's record going from 44-4 to 41-4 in the middle of a
	// conversation about it. So for fields that only move forward, an out-of-order
	// write is refused outright rather than trusted.
	describe("declining writes that move the league backwards", () => {
		const setup = async () => {
			resetG();
			g.setWithoutSavingToDB("season", 2005);
			g.setWithoutSavingToDB("phase", PHASE.AFTER_TRADE_DEADLINE);
			await resetCache({});
			changeTracker.disable();
		};

		test("a team record cannot go backwards", async () => {
			await setup();
			await idb.cache.teamSeasons.add({
				tid: 0,
				season: 2005,
				won: 44,
				lost: 4,
			} as any);

			// The exact report: this device is at 44-4, an older copy says 41-4.
			const stale = await regressionReason({
				store: "teamSeasons",
				id: 1,
				type: "put",
				value: { tid: 0, season: 2005, won: 41, lost: 4 },
			} as any);
			assert.ok(stale, "41-4 over 44-4 should be declined");
			assert.match(stale!, /41-4/);

			// Forward and equal still go through.
			assert.strictEqual(
				await regressionReason({
					store: "teamSeasons",
					id: 1,
					type: "put",
					value: { tid: 0, season: 2005, won: 45, lost: 4 },
				} as any),
				undefined,
			);
			assert.strictEqual(
				await regressionReason({
					store: "teamSeasons",
					id: 1,
					type: "put",
					value: { tid: 0, season: 2005, won: 44, lost: 4 },
				} as any),
				undefined,
			);
		});

		// A loss is not a regression - 44-4 to 44-5 is one more game played.
		test("taking a loss is not a regression", async () => {
			await setup();
			await idb.cache.teamSeasons.add({
				tid: 0,
				season: 2005,
				won: 44,
				lost: 4,
			} as any);
			assert.strictEqual(
				await regressionReason({
					store: "teamSeasons",
					id: 1,
					type: "put",
					value: { tid: 0, season: 2005, won: 44, lost: 5 },
				} as any),
				undefined,
			);
		});

		// Next season legitimately starts from 0-0, and it is a different row.
		test("a new season starting 0-0 is not a regression", async () => {
			await setup();
			await idb.cache.teamSeasons.add({
				tid: 0,
				season: 2005,
				won: 44,
				lost: 4,
			} as any);
			assert.strictEqual(
				await regressionReason({
					store: "teamSeasons",
					id: 2,
					type: "put",
					value: { tid: 0, season: 2006, won: 0, lost: 0 },
				} as any),
				undefined,
			);
		});

		test("the phase cannot go backwards", async () => {
			await setup();
			g.setWithoutSavingToDB("phase", PHASE.RESIGN_PLAYERS);

			// The offseason rollback, from the other report.
			const stale = await regressionReason({
				store: "gameAttributes",
				id: "phase",
				type: "put",
				value: { key: "phase", value: PHASE.AFTER_DRAFT },
			} as any);
			assert.ok(stale, "AFTER_DRAFT over RESIGN_PLAYERS should be declined");

			assert.strictEqual(
				await regressionReason({
					store: "gameAttributes",
					id: "phase",
					type: "put",
					value: { key: "phase", value: PHASE.FREE_AGENCY },
				} as any),
				undefined,
			);
		});

		test("the season cannot go backwards", async () => {
			await setup();
			assert.ok(
				await regressionReason({
					store: "gameAttributes",
					id: "season",
					type: "put",
					value: { key: "season", value: 2004 },
				} as any),
			);
			assert.strictEqual(
				await regressionReason({
					store: "gameAttributes",
					id: "season",
					type: "put",
					value: { key: "season", value: 2006 },
				} as any),
				undefined,
			);
		});

		// Deletes and unrelated stores are none of the guard's business.
		test("leaves everything else alone", async () => {
			await setup();
			for (const change of [
				{ store: "teamSeasons", id: 1, type: "delete" },
				{ store: "players", id: 1, type: "put", value: { pid: 1, tid: 0 } },
				{ store: "events", id: 1, type: "put", value: { eid: 1 } },
			]) {
				assert.strictEqual(await regressionReason(change as any), undefined);
			}
		});

		// The whole point: replaying old and new in ANY order lands on the newest,
		// because the older copy is simply declined. That is what makes the
		// ordering machinery's mistakes survivable instead of corrupting.
		test("applying old then new, or new then old, both end at the newest", async () => {
			for (const order of [
				[41, 44],
				[44, 41],
			]) {
				resetG();
				g.setWithoutSavingToDB("season", 2005);
				g.setWithoutSavingToDB("phase", PHASE.AFTER_TRADE_DEADLINE);
				const t = helpers.getTeamsDefault()[0]!;
				const seasonRow: any = team.genSeasonRow(t);
				seasonRow.season = 2005;
				seasonRow.won = 40;
				seasonRow.lost = 4;
				await resetCache({
					teams: [team.generate(t)],
					teamSeasons: [seasonRow],
				});
				changeTracker.disable();

				const stored: any = await idb.cache.teamSeasons.indexGet(
					"teamSeasonsByTidSeason",
					[0, 2005],
				);
				for (const won of order) {
					await applyChangeset(
						overTheWire({
							changes: [
								{
									store: "teamSeasons",
									id: stored.rid,
									type: "put",
									value: { ...stored, won, lost: 4 },
								},
							],
						}),
					);
				}

				const after: any = await idb.cache.teamSeasons.indexGet(
					"teamSeasonsByTidSeason",
					[0, 2005],
				);
				assert.strictEqual(
					after.won,
					44,
					`applying ${order.join(" then ")} should land on 44`,
				);
			}
		});
	});
});

// The guard exists to stop an out-of-order LIVE delivery moving the league
// backwards. Both of these are ways it did the opposite and moved a league
// somewhere it had never been - reported from the field as a device sitting in
// the 2005 regular season suddenly landing in free agency.
describe("the regression guard does not invent a phase", () => {
	test("a phase from an OLDER season is not read as a move forward", async () => {
		// Local: 2005, regular season. Incoming: an entry replayed out of the log
		// carrying free agency - but it is the 2004 offseason's free agency.
		//
		// Scored against the LOCAL season for both sides (the bug), 2005-free-
		// agency beats 2005-regular-season and applies, dropping the league into
		// free agency. Scored against the season the change belongs to, 2004-free-
		// agency is plainly behind 2005-regular-season and is declined.
		g.setWithoutSavingToDB("season", 2005);
		g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);

		const stale = await regressionReason(
			{
				store: "gameAttributes",
				id: "phase",
				type: "put",
				value: { key: "phase", value: PHASE.FREE_AGENCY },
			},
			2004,
		);
		assert.ok(stale !== undefined, "a 2004 phase must not apply in 2005");

		// The same phase, for the season we are actually in, is a real advance.
		const real = await regressionReason(
			{
				store: "gameAttributes",
				id: "phase",
				type: "put",
				value: { key: "phase", value: PHASE.FREE_AGENCY },
			},
			2005,
		);
		assert.strictEqual(real, undefined);
	});

	test("a season-crossing changeset supplies the season the phase belongs to", async () => {
		// The season is a SIBLING change in the same changeset, so the guard has to
		// be handed it - a phase change on its own carries no season.
		g.setWithoutSavingToDB("season", 2005);
		g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);

		await applyChangeset(
			{
				changes: [
					{
						store: "gameAttributes",
						id: "season",
						type: "put",
						value: { key: "season", value: 2006 },
					},
					{
						store: "gameAttributes",
						id: "phase",
						type: "put",
						value: { key: "phase", value: PHASE.PRESEASON },
					},
				],
			} as any,
			{ authorId: "other" } as any,
		);

		// Preseason is a LOWER phase number than free agency, so without the
		// season the guard would decline the rollover and strand the league in the
		// previous year's free agency forever.
		assert.strictEqual(g.get("season"), 2006);
		assert.strictEqual(g.get("phase"), PHASE.PRESEASON);
	});
});
