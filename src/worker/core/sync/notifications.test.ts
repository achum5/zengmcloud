import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { PHASE } from "../../../common/constants.ts";
import { g } from "../../util/index.ts";
import { buildNotifications } from "./notifications.ts";
import type { Changeset } from "./changeset.ts";

const opts = { isHost: true, authorName: "Alex" };

const playerPut = (pid: number, tid: number): Changeset["changes"][number] => ({
	store: "players",
	id: pid,
	type: "put",
	value: { pid, tid },
});

const phasePut = (phase: number): Changeset["changes"][number] => ({
	store: "gameAttributes",
	id: "phase",
	type: "put",
	value: { key: "phase", value: phase },
});

// A completed game: home team first, with won/lost carrying tid + points.
const gamePut = (
	gid: number,
	home: { tid: number; pts: number },
	away: { tid: number; pts: number },
): Changeset["changes"][number] => {
	const [won, lost] = home.pts >= away.pts ? [home, away] : [away, home];
	return {
		store: "games",
		id: gid,
		type: "put",
		value: {
			gid,
			season: 2026,
			teams: [{ tid: home.tid }, { tid: away.tid }],
			won,
			lost,
		},
	};
};

describe("buildNotifications", () => {
	beforeEach(async () => {
		resetG();
		g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
		g.setWithoutSavingToDB("season", 2026);
		g.setWithoutSavingToDB("userTids", [0]);
		await resetCache({
			teams: [
				{ tid: 0, region: "LA", name: "Lakers", abbrev: "LAL" },
				{ tid: 1, region: "Boston", name: "Celtics", abbrev: "BOS" },
			],
		});
	});

	// A completed game with box scores, so we can check top-performer stat lines.
	const gameWithBoxScore = (): Changeset["changes"][number] => ({
		store: "games",
		id: 5,
		type: "put",
		value: {
			gid: 5,
			season: 2026,
			teams: [
				{
					tid: 0,
					players: [
						{
							name: "Star Guy",
							min: 34,
							pts: 30,
							orb: 2,
							drb: 6,
							ast: 11,
							fg: 11,
							fga: 18,
							ft: 6,
							fta: 7,
							stl: 2,
							blk: 1,
							pf: 2,
							tov: 3,
						},
						{
							name: "Bench Guy",
							min: 12,
							pts: 4,
							orb: 0,
							drb: 1,
							ast: 0,
							fg: 2,
							fga: 5,
							ft: 0,
							fta: 0,
							stl: 0,
							blk: 0,
							pf: 1,
							tov: 1,
						},
					],
				},
				{
					tid: 1,
					players: [
						{
							name: "Opp Ace",
							min: 33,
							pts: 25,
							orb: 1,
							drb: 4,
							ast: 5,
							fg: 9,
							fga: 20,
							ft: 5,
							fta: 6,
							stl: 1,
							blk: 0,
							pf: 3,
							tov: 2,
						},
					],
				},
			],
			won: { tid: 0, pts: 110 },
			lost: { tid: 1, pts: 86 },
		},
	});

	test("host sim (multi-game) → per-team header + detailed blocks, targeted", async () => {
		const notifs = await buildNotifications(
			"playMenu.week",
			{
				changes: [
					gamePut(1, { tid: 0, pts: 110 }, { tid: 1, pts: 105 }),
					gamePut(2, { tid: 1, pts: 102 }, { tid: 0, pts: 98 }),
				],
			},
			opts,
		);
		assert.strictEqual(notifs.length, 1);
		assert.deepEqual(notifs[0]!.targetTids, [0]);
		// Multi-game: record is the title, per-game blocks are the body.
		assert.ok(notifs[0]!.title.includes("LA Lakers went 1-1 this week"));
		assert.ok(notifs[0]!.body.includes("W vs BOS 110-105"));
		assert.ok(notifs[0]!.body.includes("L @ BOS 98-102"));
	});

	test("single game → ESPN-style final-score title, top 2 stat lines per team", async () => {
		const notifs = await buildNotifications(
			"playMenu.day",
			{ changes: [gameWithBoxScore()] },
			opts,
		);
		// Winner first, team nicknames + final score.
		assert.strictEqual(notifs[0]!.title, "Lakers 110, Celtics 86");
		const body = notifs[0]!.body;
		// Winner's top two (Star Guy outscores Bench Guy on Game Score; REB = orb +
		// drb = 8), then the loser's scorer - each tagged with the team abbrev.
		assert.ok(body.includes("LAL Star Guy: 30 PTS, 8 REB, 11 AST"), body);
		assert.ok(body.includes("LAL Bench Guy:"), body);
		assert.ok(body.includes("BOS Opp Ace: 25 PTS, 5 REB, 5 AST"), body);
	});

	test("team with no game still gets a targeted 'league advanced' notice", async () => {
		const notifs = await buildNotifications(
			"playMenu.day",
			{ changes: [] },
			opts,
		);
		assert.strictEqual(notifs.length, 1);
		assert.deepEqual(notifs[0]!.targetTids, [0]);
		assert.ok(notifs[0]!.body.includes("No game for your LA Lakers"));
	});

	test("sim via a non-playMenu action (e.g. simToGame) is still a sim, not a trade", async () => {
		const notifs = await buildNotifications(
			"actions.simToGame",
			{
				changes: [
					gamePut(1, { tid: 0, pts: 110 }, { tid: 1, pts: 105 }),
					// A sim re-writes players across teams - must NOT read as a trade.
					playerPut(1, 0),
					playerPut(2, 1),
				],
			},
			opts,
		);
		// A boxscore-less game still reads as a sim (final-score headline), never a
		// trade. targetTids proves it went through the per-team sim path.
		assert.strictEqual(notifs[0]!.title, "Lakers 110, Celtics 105");
		assert.deepEqual(notifs[0]!.targetTids, [0]);
	});

	test("a bulk player update with no games is not a trade", async () => {
		// e.g. end-of-season progression touches every player across teams.
		const changes = [];
		for (let pid = 0; pid < 40; pid++) {
			changes.push(playerPut(pid, pid % 2));
		}
		const notifs = await buildNotifications(
			"main.newSchedule",
			{ changes },
			opts,
		);
		assert.deepEqual(notifs, []);
	});

	test("host sim that crosses into a new phase → single phase announcement to everyone", async () => {
		const notifs = await buildNotifications(
			"playMenu.week",
			{ changes: [phasePut(PHASE.DRAFT)] },
			opts,
		);
		assert.strictEqual(notifs.length, 1);
		assert.strictEqual(notifs[0]!.title, "Advanced to 2026 Draft!");
		assert.strictEqual(notifs[0]!.targetTids, null);
		assert.strictEqual(notifs[0]!.path, "draft");
	});

	test("advancing to free agency → top free agents with OVR/pot, to everyone", async () => {
		await resetCache({
			teams: [
				{ tid: 0, region: "LA", name: "Lakers", abbrev: "LAL" },
				{ tid: 1, region: "Boston", name: "Celtics", abbrev: "BOS" },
			],
			players: [
				{
					pid: 100,
					tid: -1,
					firstName: "Prime",
					lastName: "Target",
					ratings: [{ ovr: 82, pot: 84, pos: "SF" }],
					draft: { year: 2020 },
					retiredYear: Infinity,
				},
				{
					pid: 101,
					tid: -1,
					firstName: "Solid",
					lastName: "Starter",
					ratings: [{ ovr: 74, pot: 76, pos: "PG" }],
					draft: { year: 2018 },
					retiredYear: Infinity,
				},
				{
					pid: 102,
					tid: -1,
					firstName: "Deep",
					lastName: "Bench",
					ratings: [{ ovr: 61, pot: 62, pos: "C" }],
					draft: { year: 2019 },
					retiredYear: Infinity,
				},
			],
		});
		const notifs = await buildNotifications(
			"playMenu.day",
			{ changes: [phasePut(PHASE.FREE_AGENCY)] },
			opts,
		);
		assert.strictEqual(notifs.length, 1);
		assert.strictEqual(notifs[0]!.title, "Advanced to 2026 Free Agency!");
		assert.strictEqual(notifs[0]!.targetTids, null);
		const body = notifs[0]!.body;
		// Ranked best-first, each with ovr/pot.
		assert.ok(body.includes("Prime Target (82/84)"), body);
		assert.ok(body.includes("Solid Starter (74/76)"), body);
		assert.ok(body.includes("Deep Bench (61/62)"), body);
		assert.ok(
			body.indexOf("Prime Target") < body.indexOf("Solid Starter"),
			body,
		);
	});

	test("filing a game note (setNote) sends no notification", async () => {
		// A recap/note write mutates the game record, which must not look like a sim.
		const notifs = await buildNotifications(
			"main.setNote",
			{ changes: [gamePut(1, { tid: 0, pts: 110 }, { tid: 1, pts: 105 })] },
			opts,
		);
		assert.deepEqual(notifs, []);
	});

	test("non-host never announces a sim", async () => {
		const notifs = await buildNotifications(
			"playMenu.day",
			{ changes: [gamePut(1, { tid: 0, pts: 110 }, { tid: 1, pts: 105 })] },
			{ ...opts, isHost: false },
		);
		assert.deepEqual(notifs, []);
	});

	// A player record with ratings + contract, for trade/signing narration.
	const namedPlayer = (
		pid: number,
		tid: number,
		firstName: string,
		lastName: string,
		ovr: number,
		extra: Record<string, unknown> = {},
	): Changeset["changes"][number] => ({
		store: "players",
		id: pid,
		type: "put",
		value: {
			pid,
			tid,
			firstName,
			lastName,
			ratings: [{ ovr, pot: ovr, pos: "PG" }],
			...extra,
		},
	});

	test("two-team trade → Shams-style blurb naming both sides, to everyone", async () => {
		const notifs = await buildNotifications(
			"main.proposeTrade",
			{
				changes: [
					namedPlayer(1, 1, "Star", "Wing", 88), // now on Boston (tid 1)
					namedPlayer(2, 0, "Role", "Player", 74), // now on LA (tid 0)
					{
						store: "draftPicks",
						id: 9,
						type: "put",
						value: { dpid: 9, tid: 0, round: 1, season: 2027 },
					},
				],
			},
			opts,
		);
		assert.strictEqual(notifs.length, 1);
		assert.strictEqual(notifs[0]!.title, "Trade");
		assert.strictEqual(notifs[0]!.targetTids, null);
		const body = notifs[0]!.body;
		// Both sides named, both players and the pick described (direction follows
		// changeset order, so assert on content, not which team is listed first).
		assert.ok(body.includes("acquire"), body);
		assert.ok(body.includes("LA Lakers"), body);
		assert.ok(body.includes("Boston Celtics"), body);
		assert.ok(body.includes("Role Player (74/74)"), body);
		assert.ok(body.includes("Star Wing (88/88)"), body);
		assert.ok(body.includes("2027 1st-round pick"), body);
	});

	test("one-sided trade ('traded nothing for X') → Trade, not a Signing", async () => {
		const notifs = await buildNotifications(
			"main.proposeTrade",
			{
				changes: [
					// Chaney Johnson moves to LA (tid 0); Boston (tid 1) gets nothing, so
					// only one team's assets move - and he carries a contract, so without
					// the trade event this reads exactly like a free-agent signing.
					namedPlayer(1, 0, "Chaney", "Johnson", 51, {
						contract: { amount: 1000, exp: 2026 },
					}),
					{
						store: "events",
						id: 100,
						type: "put",
						value: { eid: 100, type: "trade", tids: [0, 1] },
					},
				],
			},
			opts,
		);
		assert.strictEqual(notifs[0]!.title, "Trade");
		const body = notifs[0]!.body;
		assert.ok(body.includes("acquire"), body);
		assert.ok(body.includes("Chaney Johnson"), body);
		assert.ok(body.includes("LA Lakers"), body);
		assert.ok(body.includes("Boston Celtics"), body);
	});

	const freeAgentEvent: Changeset["changes"][number] = {
		store: "events",
		id: 1,
		type: "put",
		value: { type: "freeAgent" },
	};

	test("single free-agent signing → contract terms", async () => {
		// beforeEach sets season 2026; 2026..2028 = 3 years, 15000/yr (thousands)
		// => $45M total.
		const notifs = await buildNotifications(
			"main.signFreeAgent",
			{
				changes: [
					freeAgentEvent,
					namedPlayer(1, 0, "New", "Guy", 80, {
						contract: { amount: 15000, exp: 2028 },
					}),
				],
			},
			opts,
		);
		assert.strictEqual(notifs[0]!.title, "Signing");
		assert.ok(
			notifs[0]!.body.includes("LA Lakers sign New Guy (80/80, PG)"),
			notifs[0]!.body,
		);
		assert.ok(notifs[0]!.body.includes("3-year, $45M"), notifs[0]!.body);
	});

	const reSignEvent = (pids: number[]): Changeset["changes"][number] => ({
		store: "events",
		id: 1,
		type: "put",
		value: { type: "reSigned", pids },
	});

	test("re-signing a 60+ pot player notifies", async () => {
		const notifs = await buildNotifications(
			"main.reSign",
			{
				changes: [
					reSignEvent([1]),
					namedPlayer(1, 0, "Young", "Star", 72, {
						contract: { amount: 20000, exp: 2030 },
					}),
				],
			},
			opts,
		);
		assert.strictEqual(notifs[0]!.title, "Re-signing");
		assert.ok(notifs[0]!.body.includes("re-sign Young Star"), notifs[0]!.body);
	});

	test("re-signing a sub-60 pot player is silent", async () => {
		const notifs = await buildNotifications(
			"main.reSign",
			{
				changes: [
					reSignEvent([1]),
					namedPlayer(1, 0, "Bench", "Guy", 50, {
						contract: { amount: 2000, exp: 2028 },
					}),
				],
			},
			opts,
		);
		assert.deepEqual(notifs, []);
	});

	test("bulk re-sign notifies only the 60+ pot players", async () => {
		const notifs = await buildNotifications(
			"main.reSignAll",
			{
				changes: [
					reSignEvent([1, 2, 3]),
					namedPlayer(1, 0, "Keeper", "One", 68, {
						contract: { amount: 15000, exp: 2029 },
					}),
					namedPlayer(2, 0, "Scrub", "Two", 45, {
						contract: { amount: 1500, exp: 2027 },
					}),
					namedPlayer(3, 0, "Keeper", "Three", 80, {
						contract: { amount: 30000, exp: 2031 },
					}),
				],
			},
			opts,
		);
		assert.strictEqual(notifs.length, 2);
		assert.ok(notifs.every((n) => n.title === "Re-signing"));
	});

	test("editing a player (no signing event) sends no notification", async () => {
		// A God Mode edit rewrites the whole player record - same team, same
		// contract - which must not read as a signing.
		const notifs = await buildNotifications(
			"main.upsertCustomizedPlayer",
			{
				changes: [
					namedPlayer(1, 0, "Trey", "Murphy", 67, {
						contract: { amount: 28000, exp: 2030 },
					}),
				],
			},
			opts,
		);
		assert.deepEqual(notifs, []);
	});

	test("draft picks → 'With the Nth pick...' per selection", async () => {
		g.setWithoutSavingToDB("phase", PHASE.DRAFT);
		g.setWithoutSavingToDB("numActiveTeams", 30);
		const notifs = await buildNotifications(
			"main.draftUser",
			{
				changes: [
					{
						store: "players",
						id: 7,
						type: "put",
						value: {
							pid: 7,
							tid: 0,
							firstName: "Rook",
							lastName: "Ie",
							college: "Duke",
							ratings: [{ ovr: 55, pot: 78, pos: "SF" }],
							draft: {
								round: 1,
								pick: 3,
								year: 2026,
								tid: 0,
								ovr: 55,
								pot: 78,
							},
						},
					},
				],
			},
			opts,
		);
		assert.strictEqual(notifs[0]!.title, "Draft pick");
		assert.strictEqual(
			notifs[0]!.body,
			"With the 3rd pick in the 2026 draft, the LA Lakers select Rook Ie (55, 78), SF from Duke.",
		);
		// Deep-links to the drafted player's page.
		assert.strictEqual(notifs[0]!.path, "player/7");
	});

	test("deep-link paths: trade → transactions, signing → player page", async () => {
		const trade = await buildNotifications(
			"main.proposeTrade",
			{
				changes: [
					namedPlayer(1, 1, "Star", "Wing", 88),
					namedPlayer(2, 0, "Role", "Player", 74),
				],
			},
			opts,
		);
		assert.strictEqual(trade[0]!.path, "transactions/all/2026/trade");

		const signing = await buildNotifications(
			"main.signFreeAgent",
			{
				changes: [
					freeAgentEvent,
					namedPlayer(9, 0, "New", "Guy", 80, {
						contract: { amount: 15000, exp: 2028 },
					}),
				],
			},
			opts,
		);
		assert.strictEqual(signing[0]!.path, "player/9");
	});

	test("a non-roster change → no notification", async () => {
		const notifs = await buildNotifications(
			"main.updateGameAttributes",
			{ changes: [{ store: "teams", id: 3, type: "put", value: { tid: 3 } }] },
			opts,
		);
		assert.deepEqual(notifs, []);
	});
});
