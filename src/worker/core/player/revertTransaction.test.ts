import { afterEach, assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { applyChangeset, captureChangeset } from "../sync/changeset.ts";
import { g, helpers } from "../../util/index.ts";
import {
	contractNegotiation,
	draft,
	freeAgents,
	player,
	team,
} from "../index.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { PHASE, PLAYER } from "../../../common/constants.ts";
import type { Player } from "../../../common/types.ts";
import revertTransaction, {
	planTransactionRevert,
} from "./revertTransaction.ts";
import { revertBefore } from "./revertSnapshot.ts";

const season = 2016;

// The parts of a player a move touches, to compare before against after.
const state = (p: Player) =>
	helpers.deepCopy({
		tid: p.tid,
		contract: p.contract,
		salaries: p.salaries,
		numDaysFreeAgent: p.numDaysFreeAgent,
		gamesUntilTradable: p.gamesUntilTradable,
		ptModifier: p.ptModifier,
		yearsFreeAgent: p.yearsFreeAgent,
		jerseyNumber: p.jerseyNumber,
		transactions: p.transactions,
		draft: p.draft,
	});

const getPlayer = async (pid: number) => (await idb.cache.players.get(pid))!;

const lastEvent = async () => (await idb.cache.events.getAll()).at(-1)!;

// Make an event look like one logged before moves saved a snapshot.
const stripSnapshot = (event: any) => {
	delete event.revert;
};

const numEvents = async (type: string) =>
	(await idb.cache.events.getAll()).filter((event) => event.type === type)
		.length;

// A free agent with a history: an old contract in his salary log, a past move
// in his transactions, days spent unsigned and a number from his last team.
const makeFreeAgent = async () => {
	const p = await getPlayer(0);
	p.tid = PLAYER.FREE_AGENT;
	p.contract = { amount: 5000, exp: season + 1 };
	p.salaries = [{ season: season - 1, amount: 3000 }];
	p.transactions = [
		{
			season: season - 1,
			phase: PHASE.REGULAR_SEASON,
			tid: 2,
			type: "freeAgent",
		},
	];
	p.numDaysFreeAgent = 7;
	p.gamesUntilTradable = 0;
	p.jerseyNumber = "8";
	p.numPlayersTradedAwayNormalized = {};
	await idb.cache.players.put(p);
	return p;
};

// A player on team 0 under a three-year deal.
const makeRostered = async () => {
	const p = await getPlayer(0);
	p.tid = 0;
	p.contract = { amount: 5000, exp: season + 2 };
	p.salaries = [
		{ season, amount: 5000 },
		{ season: season + 1, amount: 5000 },
		{ season: season + 2, amount: 5000 },
	];
	p.transactions = [];
	p.jerseyNumber = "23";
	p.ptModifier = 1.25;
	p.gamesUntilTradable = 5;
	delete p.numPlayersTradedAwayNormalized;
	await idb.cache.players.put(p);
	return p;
};

beforeEach(async () => {
	resetG();
	g.setWithoutSavingToDB("godMode", true);
	g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);

	const teams = helpers.getTeamsDefault().slice(0, 3).map(team.generate);
	g.setWithoutSavingToDB("numTeams", 3);
	g.setWithoutSavingToDB("numActiveTeams", 3);

	await resetCache({
		players: [
			player.generate(PLAYER.FREE_AGENT, 28, season - 6, true, DEFAULT_LEVEL),
			player.generate(0, 28, season - 6, true, DEFAULT_LEVEL),
			player.generate(1, 28, season - 6, true, DEFAULT_LEVEL),
		],
		teams,
	});
});

describe("revert a free agent signing", () => {
	test("he is back in free agency exactly as he was", async () => {
		const p = await makeFreeAgent();
		const before = state(p);

		await player.sign(p, 0, { amount: 6000, exp: season + 2 }, g.get("phase"));
		await idb.cache.players.put(p);
		const event = await lastEvent();
		assert.strictEqual(event.type, "freeAgent");
		assert.strictEqual((await getPlayer(0)).tid, 0);

		assert.strictEqual(await revertTransaction(event.eid), undefined);

		const after = await getPlayer(0);
		assert.deepEqual(state(after), before);
		assert.isDefined(after.numPlayersTradedAwayNormalized);

		// And the log reads as if it never happened.
		assert.strictEqual(await numEvents("freeAgent"), 0);
	});

	test("the asking contract is the one he asked for, not the one a team reshaped", async () => {
		// autoSign shortens a deal to the team's plan before signing, so it hands
		// sign() a snapshot from before that edit.
		const p = await makeFreeAgent();
		const before = revertBefore(p);
		p.contract.exp = season;

		await player.sign(p, 0, p.contract, g.get("phase"), before);
		await idb.cache.players.put(p);

		assert.strictEqual(
			await revertTransaction((await lastEvent()).eid),
			undefined,
		);
		assert.deepEqual((await getPlayer(0)).contract, {
			amount: 5000,
			exp: season + 1,
		});
	});

	test("refuses once he has moved on, and changes nothing", async () => {
		const p = await makeFreeAgent();
		await player.sign(p, 0, { amount: 6000, exp: season + 2 }, g.get("phase"));
		await idb.cache.players.put(p);
		const eid = (await lastEvent()).eid;

		// Traded to team 1.
		p.tid = 1;
		p.transactions!.push({
			season,
			phase: PHASE.REGULAR_SEASON,
			tid: 1,
			type: "trade",
			fromTid: 0,
		});
		await idb.cache.players.put(p);
		const traded = state(p);

		assert.isString(await revertTransaction(eid));
		assert.deepEqual(state(await getPlayer(0)), traded);
		assert.strictEqual(await numEvents("freeAgent"), 1);
	});

	test("refuses after a trade there and back", async () => {
		const p = await makeFreeAgent();
		await player.sign(p, 0, { amount: 6000, exp: season + 2 }, g.get("phase"));
		await idb.cache.players.put(p);
		const eid = (await lastEvent()).eid;

		// Same team and contract as the signing left him, but he has moved since.
		for (const [tid, fromTid] of [
			[1, 0],
			[0, 1],
		] as const) {
			p.transactions!.push({
				season,
				phase: PHASE.REGULAR_SEASON,
				tid,
				type: "trade",
				fromTid,
			});
		}
		await idb.cache.players.put(p);

		assert.isString(await revertTransaction(eid));
	});

	test("refuses once he has a new contract", async () => {
		const p = await makeFreeAgent();
		await player.sign(p, 0, { amount: 6000, exp: season + 2 }, g.get("phase"));
		p.contract = { amount: 9000, exp: season + 4 };
		await idb.cache.players.put(p);

		assert.isString(await revertTransaction((await lastEvent()).eid));
	});

	test("refuses once a season of the deal is over", async () => {
		const p = await makeFreeAgent();
		await player.sign(p, 0, { amount: 6000, exp: season + 2 }, g.get("phase"));
		await idb.cache.players.put(p);
		const eid = (await lastEvent()).eid;

		g.setWithoutSavingToDB("phase", PHASE.DRAFT_LOTTERY);
		assert.isString(await revertTransaction(eid));
	});

	test("refuses if his salary history was edited", async () => {
		const p = await makeFreeAgent();
		await player.sign(p, 0, { amount: 6000, exp: season + 2 }, g.get("phase"));
		p.salaries.at(-1)!.amount = 1;
		await idb.cache.players.put(p);

		assert.isString(await revertTransaction((await lastEvent()).eid));
	});

	test("requires God Mode", async () => {
		const p = await makeFreeAgent();
		await player.sign(p, 0, { amount: 6000, exp: season + 2 }, g.get("phase"));
		await idb.cache.players.put(p);
		g.setWithoutSavingToDB("godMode", false);

		assert.isString(await revertTransaction((await lastEvent()).eid));
		assert.strictEqual((await getPlayer(0)).tid, 0);
	});

	test("a reverted signing cannot be reverted again", async () => {
		const p = await makeFreeAgent();
		await player.sign(p, 0, { amount: 6000, exp: season + 2 }, g.get("phase"));
		await idb.cache.players.put(p);
		const eid = (await lastEvent()).eid;

		assert.strictEqual(await revertTransaction(eid), undefined);
		assert.isString(await revertTransaction(eid));
	});

	test("a signing logged before moves saved a snapshot still reverts", async () => {
		const p = await makeFreeAgent();
		const before = state(p);
		await player.sign(p, 0, { amount: 6000, exp: season + 2 }, g.get("phase"));
		await idb.cache.players.put(p);
		const event = await lastEvent();
		stripSnapshot(event);

		assert.strictEqual(await revertTransaction(event.eid), undefined);

		// Everything the league still records comes back exactly. What he asked
		// for as a free agent was never recorded, so he asks the market again.
		const after = state(await getPlayer(0));
		assert.strictEqual(after.tid, PLAYER.FREE_AGENT);
		assert.deepEqual(after.salaries, before.salaries);
		assert.deepEqual(after.transactions, before.transactions);
		assert.strictEqual(await numEvents("freeAgent"), 0);
	});

	test("an older signing still refuses once he has moved on", async () => {
		const p = await makeFreeAgent();
		await player.sign(p, 0, { amount: 6000, exp: season + 2 }, g.get("phase"));
		p.transactions!.push({
			season,
			phase: PHASE.REGULAR_SEASON,
			tid: 0,
			type: "trade",
			fromTid: 1,
		});
		await idb.cache.players.put(p);
		const event = await lastEvent();
		stripSnapshot(event);

		assert.isString(await revertTransaction(event.eid));
		assert.strictEqual((await getPlayer(0)).tid, 0);
	});

	test("a free agency board signing's line in that day's results goes too", async () => {
		g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
		const p = await makeFreeAgent();
		await player.sign(
			p,
			0,
			{ amount: 6000, exp: season + 2 },
			PHASE.FREE_AGENCY,
		);
		await idb.cache.players.put(p);

		const other = {
			type: "unopposed" as const,
			pid: 2,
			name: "Someone Else",
			round: 1,
			tid: 1,
			abbrev: "B",
			amount: 2000,
			exp: season + 1,
		};
		await idb.cache.faDayResults.put({
			key: `${season}-30`,
			season,
			daysLeft: 30,
			items: [
				{
					type: "contest",
					pid: 0,
					name: "Signed Man",
					round: 1,
					teams: [],
					roll: 37,
					winnerTid: 0,
					amount: 6000,
					exp: season + 2,
				},
				other,
			],
			boards: [],
			at: 0,
		});

		assert.strictEqual(
			await revertTransaction((await lastEvent()).eid),
			undefined,
		);
		assert.deepEqual(
			(await idb.cache.faDayResults.get(`${season}-30`))!.items,
			[other],
		);
	});

	test("a signing a season ago, before this season started, can still go", async () => {
		// Signed in free agency: his deal starts next season, so nothing of it
		// has been played when the new season begins.
		g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
		const p = await makeFreeAgent();
		await player.sign(p, 0, { amount: 6000, exp: season + 2 }, g.get("phase"));
		await idb.cache.players.put(p);
		const eid = (await lastEvent()).eid;

		g.setWithoutSavingToDB("season", season + 1);
		g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
		assert.strictEqual(await revertTransaction(eid), undefined);

		const after = await getPlayer(0);
		assert.strictEqual(after.tid, PLAYER.FREE_AGENT);
		// The deal's salary rows come off his log.
		assert.deepEqual(after.salaries, [{ season: season - 1, amount: 3000 }]);
		// Free agency has moved on since he signed, so he asks what the market
		// says now - for a deal that runs from this season, not last.
		assert.isAtLeast(after.contract.exp, season + 1);
	});
});

describe("revert a re-signing", () => {
	test("your re-signing reopens the talks while re-signing is still on", async () => {
		g.setWithoutSavingToDB("phase", PHASE.RESIGN_PLAYERS);

		// How newPhaseResignPlayers leaves your expiring player: a free agent with
		// re-signing talks open.
		const p = await getPlayer(0);
		p.tid = PLAYER.FREE_AGENT;
		p.contract = {
			amount: 4000,
			exp: season,
			rookie: true,
			rookieResign: true,
		};
		p.salaries = [{ season, amount: 1500 }];
		p.transactions = [];
		await idb.cache.players.put(p);
		await idb.cache.negotiations.put({ pid: 0, tid: 0, resigning: true });
		const before = state(p);

		// What accept() does.
		await player.sign(
			p,
			0,
			{ amount: 4000, exp: season + 3 },
			PHASE.RESIGN_PLAYERS,
		);
		await idb.cache.players.put(p);
		await contractNegotiation.cancel(0);
		const event = await lastEvent();
		assert.strictEqual(event.type, "reSigned");

		assert.strictEqual(await revertTransaction(event.eid), undefined);

		assert.deepEqual(state(await getPlayer(0)), before);
		assert.deepEqual(await idb.cache.negotiations.get(0), {
			pid: 0,
			tid: 0,
			resigning: true,
		});
		assert.strictEqual(await numEvents("reSigned"), 0);
	});

	test("an AI team's re-signing leaves him the free agent he'd have been", async () => {
		g.setWithoutSavingToDB("phase", PHASE.RESIGN_PLAYERS);

		// AI teams re-sign their own players without ever making them free agents.
		const p = await getPlayer(2);
		p.tid = 1;
		p.contract = { amount: 4000, exp: season + 2, rookieResign: true };
		p.salaries = [{ season, amount: 1500 }];
		p.transactions = [];
		await idb.cache.players.put(p);

		await player.sign(p, 1, { ...p.contract }, PHASE.RESIGN_PLAYERS);
		delete p.contract.rookieResign;
		await idb.cache.players.put(p);

		assert.strictEqual(
			await revertTransaction((await lastEvent()).eid),
			undefined,
		);

		const after = await getPlayer(2);
		assert.strictEqual(after.tid, PLAYER.FREE_AGENT);
		assert.isUndefined(after.contract.rookieResign);
		assert.deepEqual(after.salaries, [{ season, amount: 1500 }]);
		assert.strictEqual(after.ptModifier, 1);
		assert.isUndefined(await idb.cache.negotiations.get(2));
	});

	test("after re-signing is over, he's a free agent with no talks to reopen", async () => {
		g.setWithoutSavingToDB("phase", PHASE.RESIGN_PLAYERS);
		const p = await getPlayer(0);
		p.tid = PLAYER.FREE_AGENT;
		p.contract = { amount: 4000, exp: season };
		p.salaries = [];
		p.transactions = [];
		await idb.cache.players.put(p);
		await idb.cache.negotiations.put({ pid: 0, tid: 0, resigning: true });

		await player.sign(
			p,
			0,
			{ amount: 4000, exp: season + 3 },
			PHASE.RESIGN_PLAYERS,
		);
		await idb.cache.players.put(p);
		await contractNegotiation.cancel(0);
		const eid = (await lastEvent()).eid;

		g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
		assert.strictEqual(await revertTransaction(eid), undefined);

		assert.strictEqual((await getPlayer(0)).tid, PLAYER.FREE_AGENT);
		assert.isUndefined(await idb.cache.negotiations.get(0));
	});
});

describe("revert a release", () => {
	test("he is back on the team under his old deal and the dead money is gone", async () => {
		const p = await makeRostered();
		const before = state(p);

		await player.release(p, false);
		const event = await lastEvent();
		assert.strictEqual(event.type, "release");
		assert.lengthOf(await idb.cache.releasedPlayers.getAll(), 1);

		// What the release API does next: price him as a free agent.
		await freeAgents.normalizeContractDemands({
			type: "dummyExpiringContracts",
			pids: [0],
		});

		assert.strictEqual(await revertTransaction(event.eid), undefined);

		const after = await getPlayer(0);
		assert.deepEqual(state(after), before);
		assert.isUndefined(after.numPlayersTradedAwayNormalized);
		assert.lengthOf(await idb.cache.releasedPlayers.getAll(), 0);
		assert.strictEqual(await numEvents("release"), 0);
	});

	test("a just-drafted player gets his salary log back", async () => {
		const p = await makeRostered();
		p.contract.rookie = true;
		p.draft.year = season;
		await idb.cache.players.put(p);
		const before = state(p);

		await player.release(p, true);
		assert.lengthOf((await getPlayer(0)).salaries, 0);
		assert.lengthOf(await idb.cache.releasedPlayers.getAll(), 0);

		assert.strictEqual(
			await revertTransaction((await lastEvent()).eid),
			undefined,
		);
		assert.deepEqual(state(await getPlayer(0)), before);
	});

	test("a teammate who took his number in the meantime keeps it", async () => {
		const p = await makeRostered();
		await player.release(p, false);
		const eid = (await lastEvent()).eid;

		const teammate = await getPlayer(1);
		teammate.jerseyNumber = "23";
		await idb.cache.players.put(teammate);

		assert.strictEqual(await revertTransaction(eid), undefined);

		const after = await getPlayer(0);
		assert.strictEqual(after.tid, 0);
		assert.notStrictEqual(after.jerseyNumber, "23");
		assert.strictEqual((await getPlayer(1)).jerseyNumber, "23");
	});

	test("refuses once he has signed somewhere else", async () => {
		const p = await makeRostered();
		await player.release(p, false);
		const eid = (await lastEvent()).eid;

		const released = await getPlayer(0);
		await player.sign(
			released,
			1,
			{ amount: 2000, exp: season },
			g.get("phase"),
		);
		await idb.cache.players.put(released);

		assert.isString(await revertTransaction(eid));
		assert.strictEqual((await getPlayer(0)).tid, 1);
		assert.lengthOf(await idb.cache.releasedPlayers.getAll(), 1);
	});

	test("a release logged before moves saved a snapshot still reverts", async () => {
		const p = await makeRostered();
		const before = state(p);
		await player.release(p, false);
		stripSnapshot(await lastEvent());
		await freeAgents.normalizeContractDemands({
			type: "dummyExpiringContracts",
			pids: [0],
		});

		assert.strictEqual(
			await revertTransaction((await lastEvent()).eid),
			undefined,
		);

		// His contract comes back from the dead money row. Playing time was
		// never recorded, so it's back to normal.
		const after = state(await getPlayer(0));
		assert.strictEqual(after.tid, 0);
		assert.deepEqual(after.contract, before.contract);
		assert.deepEqual(after.salaries, before.salaries);
		assert.strictEqual(after.ptModifier, 1);
		assert.lengthOf(await idb.cache.releasedPlayers.getAll(), 0);
	});

	test("an older release that cost nothing can't be rebuilt", async () => {
		// No dead money row means nothing recorded his old contract.
		const p = await makeRostered();
		p.contract.rookie = true;
		p.draft.year = season;
		await idb.cache.players.put(p);
		await player.release(p, true);
		stripSnapshot(await lastEvent());

		assert.isString(await revertTransaction((await lastEvent()).eid));
		assert.strictEqual((await getPlayer(0)).tid, PLAYER.FREE_AGENT);
	});

	test("refuses once his old contract has run out", async () => {
		const p = await makeRostered();
		await player.release(p, false);
		const eid = (await lastEvent()).eid;

		g.setWithoutSavingToDB("season", season + 3);
		assert.isString(await revertTransaction(eid));
	});
});

describe("revert a draft pick", () => {
	const dp = {
		dpid: 5,
		tid: 1,
		originalTid: 2,
		round: 1,
		pick: 3,
		season,
	};

	const makeProspect = async () => {
		const p = await getPlayer(0);
		p.tid = PLAYER.UNDRAFTED;
		// How player.generate makes a prospect.
		p.draft = {
			round: 0,
			pick: 0,
			tid: -1,
			originalTid: -1,
			year: season,
			pot: 0,
			ovr: 0,
			skills: [],
		};
		p.contract = { amount: 1000, exp: season + 3, rookie: true };
		p.salaries = [];
		p.transactions = [];
		await idb.cache.players.put(p);
		return p;
	};

	beforeEach(async () => {
		g.setWithoutSavingToDB("phase", PHASE.DRAFT);
		await idb.cache.draftPicks.put({ ...dp });
	});

	test("he is back in the pool and the pick is back on the board", async () => {
		const p = await makeProspect();
		const before = state(p);

		await draft.selectPlayer((await idb.cache.draftPicks.get(5))!, 0);
		const event = await lastEvent();
		assert.strictEqual(event.type, "draft");
		assert.strictEqual((await getPlayer(0)).tid, 1);
		assert.isUndefined(await idb.cache.draftPicks.get(5));

		assert.strictEqual(await revertTransaction(event.eid), undefined);

		assert.deepEqual(state(await getPlayer(0)), before);
		assert.deepEqual(await idb.cache.draftPicks.get(5), dp);
		assert.strictEqual(await numEvents("draft"), 0);
	});

	test("a pick logged before moves saved a snapshot still reverts", async () => {
		const p = await makeProspect();
		const before = state(p);
		await draft.selectPlayer((await idb.cache.draftPicks.get(5))!, 0);
		const event = await lastEvent();
		stripSnapshot(event);

		assert.strictEqual(await revertTransaction(event.eid), undefined);

		const after = state(await getPlayer(0));
		assert.strictEqual(after.tid, PLAYER.UNDRAFTED);
		assert.deepEqual(after.salaries, before.salaries);
		assert.deepEqual(after.transactions, before.transactions);
		assert.deepEqual(after.draft, before.draft);
		assert.deepEqual(await idb.cache.draftPicks.get(5), dp);
	});

	test("only during the draft", async () => {
		await makeProspect();
		await draft.selectPlayer((await idb.cache.draftPicks.get(5))!, 0);
		const eid = (await lastEvent()).eid;

		g.setWithoutSavingToDB("phase", PHASE.AFTER_DRAFT);
		assert.isString(await revertTransaction(eid));
	});

	test("refuses once he has been traded", async () => {
		await makeProspect();
		await draft.selectPlayer((await idb.cache.draftPicks.get(5))!, 0);
		const eid = (await lastEvent()).eid;

		const p = await getPlayer(0);
		p.tid = 2;
		p.transactions!.push({
			season,
			phase: PHASE.DRAFT,
			tid: 2,
			type: "trade",
			fromTid: 1,
		});
		await idb.cache.players.put(p);

		assert.isString(await revertTransaction(eid));
		assert.isUndefined(await idb.cache.draftPicks.get(5));
	});

	test("the plan and the revert agree", async () => {
		await makeProspect();
		await draft.selectPlayer((await idb.cache.draftPicks.get(5))!, 0);
		const event = await lastEvent();

		assert.notProperty(await planTransactionRevert(event), "error");
		g.setWithoutSavingToDB("phase", PHASE.AFTER_DRAFT);
		assert.property(await planTransactionRevert(event), "error");
	});
});

describe("in a synced league", () => {
	// Ship a changeset the way the network does: as JSON, which (unlike
	// structuredClone) drops undefined fields.
	const overTheWire = <T>(value: T): T =>
		// eslint-disable-next-line unicorn/prefer-structured-clone
		JSON.parse(JSON.stringify(value));

	// Run a revert on this device as the cloud-tracked action it is, and return
	// what it would publish.
	const revertAndPublish = async (eid: number) => {
		changeTracker.enable();
		changeTracker.reset();
		await changeTracker.runCaptured(async () => {
			assert.strictEqual(await revertTransaction(eid), undefined);
		});
		const changeset = overTheWire(await captureChangeset());
		changeTracker.disable();
		return changeset;
	};

	afterEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	// A move made on one device, as another device sees it: everything it
	// changed arrives through the changeset, on top of that device's own log.
	const madeElsewhere = async (move: () => Promise<void>) => {
		const players = overTheWire(await idb.cache.players.getAll());
		const teams = overTheWire(await idb.cache.teams.getAll());

		changeTracker.enable();
		changeTracker.reset();
		await changeTracker.runCaptured(move);
		const changeset = overTheWire(await captureChangeset());
		changeTracker.disable();

		const theirOwnEvents = [0, 1, 2, 3, 4, 5, 6].map((i) => ({
			type: "award",
			text: `Something else ${i}.`,
			pids: [2],
			tids: [1],
			season,
		}));
		await resetCache({ players, teams, events: theirOwnEvents });
		await applyChangeset(changeset, { refreshUI: false });
		changeTracker.disable();
	};

	test("a signing made on another device can be reverted here", async () => {
		const p = await makeFreeAgent();
		const before = state(p);
		await madeElsewhere(async () => {
			await player.sign(
				p,
				0,
				{ amount: 6000, exp: season + 2 },
				g.get("phase"),
			);
			await idb.cache.players.put(p);
		});

		const event = (await idb.cache.events.getAll()).find(
			(row) => row.type === "freeAgent",
		)!;
		assert.notProperty(await planTransactionRevert(event), "error");
		assert.strictEqual(await revertTransaction(event.eid), undefined);
		assert.deepEqual(state(await getPlayer(0)), before);
	});

	test("a release made on another device can be reverted here", async () => {
		const p = await makeRostered();
		const before = state(p);
		await madeElsewhere(async () => {
			await player.release(p, false);
		});

		const event = (await idb.cache.events.getAll()).find(
			(row) => row.type === "release",
		)!;
		assert.notProperty(await planTransactionRevert(event), "error");
		assert.strictEqual(await revertTransaction(event.eid), undefined);
		assert.deepEqual(state(await getPlayer(0)), before);
		assert.lengthOf(await idb.cache.releasedPlayers.getAll(), 0);
	});

	test("a reverted release is reverted on a device whose ids differ", async () => {
		const p = await makeRostered();
		await player.release(p, false);
		const event = await lastEvent();
		const [releasedRow] = await idb.cache.releasedPlayers.getAll();

		// The other device's copy of the league right after the release. Its
		// ids came from its own counters, so the event and the dead money row sit
		// under different keys there - and an unrelated event holds this
		// device's eid.
		const players = overTheWire(await idb.cache.players.getAll());
		const teams = overTheWire(await idb.cache.teams.getAll());
		const theirEvent = { ...overTheWire(event), eid: event.eid + 40 };
		const theirRow = {
			...overTheWire(releasedRow!),
			rid: releasedRow!.rid + 7,
		};
		const bystander = {
			eid: event.eid,
			type: "award",
			text: "Someone won something.",
			pids: [2],
			tids: [1],
			season,
		};

		const changeset = await revertAndPublish(event.eid);

		await resetCache({
			players,
			teams,
			releasedPlayers: [theirRow],
			events: [bystander, theirEvent],
		});
		await applyChangeset(changeset, { refreshUI: false });

		assert.strictEqual((await getPlayer(0)).tid, 0);
		assert.deepEqual((await getPlayer(0)).contract, {
			amount: 5000,
			exp: season + 2,
		});
		assert.lengthOf(await idb.cache.releasedPlayers.getAll(), 0);
		// Their copy of the release is gone; the event at the same id is not.
		const events = await idb.cache.events.getAll();
		assert.deepEqual(
			events.map((row) => row.eid),
			[bystander.eid],
		);
	});

	test("a reverted re-signing reopens the talks on the other device too", async () => {
		g.setWithoutSavingToDB("phase", PHASE.RESIGN_PLAYERS);
		const p = await getPlayer(0);
		p.tid = PLAYER.FREE_AGENT;
		p.contract = { amount: 4000, exp: season };
		p.salaries = [];
		p.transactions = [];
		await idb.cache.players.put(p);
		await idb.cache.negotiations.put({ pid: 0, tid: 0, resigning: true });

		await player.sign(
			p,
			0,
			{ amount: 4000, exp: season + 3 },
			PHASE.RESIGN_PLAYERS,
		);
		await idb.cache.players.put(p);
		await contractNegotiation.cancel(0);
		const event = await lastEvent();

		const players = overTheWire(await idb.cache.players.getAll());
		const teams = overTheWire(await idb.cache.teams.getAll());
		const theirEvent = { ...overTheWire(event), eid: event.eid + 12 };

		const changeset = await revertAndPublish(event.eid);

		await resetCache({ players, teams, events: [theirEvent] });
		await applyChangeset(changeset, { refreshUI: false });

		assert.strictEqual((await getPlayer(0)).tid, PLAYER.FREE_AGENT);
		assert.deepEqual(await idb.cache.negotiations.get(0), {
			pid: 0,
			tid: 0,
			resigning: true,
		});
		assert.lengthOf(await idb.cache.events.getAll(), 0);
	});

	test("a reverted draft pick goes back on the board without touching another pick", async () => {
		g.setWithoutSavingToDB("phase", PHASE.DRAFT);
		const dp = {
			dpid: 5,
			tid: 1,
			originalTid: 2,
			round: 1,
			pick: 3,
			season,
		};
		await idb.cache.draftPicks.put({ ...dp });
		const p = await getPlayer(0);
		p.tid = PLAYER.UNDRAFTED;
		p.draft = {
			...p.draft,
			year: season,
			round: 0,
			pick: 0,
			tid: -1,
			originalTid: -1,
		};
		p.salaries = [];
		p.transactions = [];
		await idb.cache.players.put(p);

		await draft.selectPlayer((await idb.cache.draftPicks.get(5))!, 0);
		const event = await lastEvent();

		const players = overTheWire(await idb.cache.players.getAll());
		const teams = overTheWire(await idb.cache.teams.getAll());
		const theirEvent = { ...overTheWire(event), eid: event.eid + 3 };
		// There, dpid 5 is a different pick altogether: next year's.
		const theirOtherPick = {
			dpid: 5,
			tid: 0,
			originalTid: 0,
			round: 1,
			pick: 0,
			season: season + 1,
		};

		const changeset = await revertAndPublish(event.eid);

		await resetCache({
			players,
			teams,
			draftPicks: [theirOtherPick],
			events: [theirEvent],
		});
		await applyChangeset(changeset, { refreshUI: false });

		assert.strictEqual((await getPlayer(0)).tid, PLAYER.UNDRAFTED);
		const picks = await idb.cache.draftPicks.getAll();
		assert.lengthOf(picks, 2);
		assert.deepEqual(await idb.cache.draftPicks.get(5), theirOtherPick);
		const restored = picks.find((row) => row.season === season)!;
		assert.deepEqual(
			{ ...restored, dpid: undefined },
			{ ...dp, dpid: undefined },
		);
		assert.lengthOf(await idb.cache.events.getAll(), 0);
	});
});
