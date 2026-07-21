import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { PHASE } from "../../../common/constants.ts";
import { g } from "../../util/index.ts";
import { idb } from "../../db/index.ts";
import {
	beginLotteryReveal,
	buildNotifications,
	endLotteryReveal,
} from "./notifications.ts";
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

	test("single-game headline shows each team's record in parentheses", async () => {
		await resetCache({
			teams: [
				{ tid: 0, region: "LA", name: "Lakers", abbrev: "LAL" },
				{ tid: 1, region: "Boston", name: "Celtics", abbrev: "BOS" },
			],
			teamSeasons: [
				{ rid: 0, season: 2026, tid: 0, won: 5, lost: 2 },
				{ rid: 1, season: 2026, tid: 1, won: 3, lost: 4 },
			],
		});
		const notifs = await buildNotifications(
			"playMenu.day",
			{ changes: [gameWithBoxScore()] },
			opts,
		);
		assert.strictEqual(notifs[0]!.title, "Lakers (5-2) 110, Celtics (3-4) 86");
	});

	test("team with a bye gets a targeted 'Bye day' notice listing the day's games", async () => {
		const notifs = await buildNotifications(
			"playMenu.day",
			{
				// The user's team (tid 0) didn't play; two other games did.
				changes: [
					gamePut(20, { tid: 1, pts: 120 }, { tid: 2, pts: 114 }),
					gamePut(21, { tid: 3, pts: 99 }, { tid: 4, pts: 90 }),
				],
			},
			opts,
		);
		assert.strictEqual(notifs.length, 1);
		assert.deepEqual(notifs[0]!.targetTids, [0]);
		assert.strictEqual(notifs[0]!.title, "Bye day for the Lakers");
		// The other games' results are listed, winner first.
		assert.ok(notifs[0]!.body.includes("BOS 120-114"), notifs[0]!.body);
	});

	test("a bye with no other games falls back to a simple notice", async () => {
		const notifs = await buildNotifications(
			"playMenu.day",
			{ changes: [] },
			opts,
		);
		assert.strictEqual(notifs.length, 1);
		assert.deepEqual(notifs[0]!.targetTids, [0]);
		assert.strictEqual(notifs[0]!.title, "Bye day for the Lakers");
		assert.ok(notifs[0]!.body.includes("No other games"), notifs[0]!.body);
	});

	test("All-Star Weekend → room-wide recap with score, MVP, and contest winners", async () => {
		const notifs = await buildNotifications(
			"playMenu.day",
			{
				changes: [
					{
						store: "allStars",
						id: 2026,
						type: "put",
						value: {
							season: 2026,
							teamNames: ["Team LeBron", "Team Curry"],
							score: [148, 155],
							mvp: { pid: 1, tid: 0, name: "Star Guy" },
							dunk: {
								players: [{ pid: 2, name: "High Flyer" }],
								winner: 0,
							},
							three: {
								players: [{ pid: 3, name: "Sharp Shooter" }],
								winner: 0,
							},
						},
					},
					// The All-Star game itself is written with special tids -1/-2.
					gamePut(30, { tid: -1, pts: 148 }, { tid: -2, pts: 155 }),
				],
			},
			opts,
		);
		const allStar = notifs.find((n) => n.title === "All-Star Weekend");
		assert.ok(allStar, JSON.stringify(notifs));
		assert.strictEqual(allStar!.targetTids, null);
		// Winner first.
		assert.ok(
			allStar!.body.includes("Team Curry 155, Team LeBron 148"),
			allStar!.body,
		);
		assert.ok(allStar!.body.includes("MVP: Star Guy"), allStar!.body);
		assert.ok(
			allStar!.body.includes("Dunk contest: High Flyer"),
			allStar!.body,
		);
		assert.ok(
			allStar!.body.includes("3-point contest: Sharp Shooter"),
			allStar!.body,
		);
		// No per-team "bye day" notice during the All-Star break.
		assert.ok(
			!notifs.some((n) => n.title.startsWith("Bye day")),
			JSON.stringify(notifs),
		);
	});

	test("in the playoffs, series scores go to the WHOLE room (eliminated teams too)", async () => {
		g.setWithoutSavingToDB("phase", PHASE.PLAYOFFS);
		await idb.cache.playoffSeries.put({
			season: 2026,
			currentRound: 0,
			currentPlayoffs: undefined,
			series: [
				[
					{
						home: { tid: 0, abbrev: "LAL", seed: 1, cid: 0, won: 3 },
						away: { tid: 1, abbrev: "BOS", seed: 8, cid: 0, won: 1 },
					},
				],
			],
		} as any);

		const notifs = await buildNotifications(
			"playMenu.day",
			{ changes: [] },
			opts,
		);
		assert.strictEqual(notifs.length, 1);
		// null = everyone in the room - a device whose team is eliminated (or was
		// never in the bracket) must still get the playoff scores.
		assert.strictEqual(notifs[0]!.targetTids, null);
		assert.strictEqual(notifs[0]!.title, "Playoff scores");
		assert.ok(notifs[0]!.body.includes("LAL 3-1 BOS"));
		assert.strictEqual(notifs[0]!.path, "playoffs");
	});

	test("a playing team gets the room-wide bracket AND its own game result", async () => {
		g.setWithoutSavingToDB("phase", PHASE.PLAYOFFS);
		await idb.cache.playoffSeries.put({
			season: 2026,
			currentRound: 0,
			currentPlayoffs: undefined,
			series: [
				[
					{
						home: { tid: 0, abbrev: "LAL", seed: 1, cid: 0, won: 3 },
						away: { tid: 1, abbrev: "BOS", seed: 8, cid: 0, won: 1 },
					},
				],
			],
		} as any);

		const notifs = await buildNotifications(
			"playMenu.day",
			{ changes: [gameWithBoxScore()] },
			opts,
		);
		const bracket = notifs.find((n) => n.title === "Playoff scores");
		assert.ok(bracket);
		assert.strictEqual(bracket!.targetTids, null);
		const gameResult = notifs.find((n) => n.title.includes("Lakers"));
		assert.ok(gameResult, JSON.stringify(notifs));
		assert.deepEqual(gameResult!.targetTids, [0]);
	});

	test("during the play-in tournament, the day's scoreboard goes to the WHOLE room", async () => {
		g.setWithoutSavingToDB("phase", PHASE.PLAYOFFS);
		// currentRound === -1 marks the play-in tournament (games are single
		// elimination, stored in playIns rather than series).
		await idb.cache.playoffSeries.put({
			season: 2026,
			currentRound: -1,
			series: [[]],
			playIns: [],
		} as any);

		const notifs = await buildNotifications(
			"playMenu.day",
			// Two play-in games the user's team (tid 0) isn't in.
			{
				changes: [
					gamePut(10, { tid: 1, pts: 120 }, { tid: 2, pts: 114 }),
					gamePut(11, { tid: 3, pts: 98 }, { tid: 4, pts: 105 }),
				],
			},
			opts,
		);
		assert.strictEqual(notifs.length, 1);
		assert.strictEqual(notifs[0]!.targetTids, null);
		assert.strictEqual(notifs[0]!.title, "Play-in scores");
		assert.ok(notifs[0]!.body.includes("BOS 120-114"), notifs[0]!.body);
		assert.strictEqual(notifs[0]!.path, "playoffs");
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

	// A real trade event: tids plus each team's RECEIVED assets, exactly as
	// processTrade logs it. This (not the moved records) is what drives the trade
	// notification now, so it works for CPU-vs-CPU trades inside a sim too.
	const tradeEvent = (
		tids: [number, number],
		teamsAssets: [any[], any[]],
	): Changeset["changes"][number] => ({
		store: "events",
		id: 100,
		type: "put",
		value: {
			eid: 100,
			type: "trade",
			tids,
			teams: [{ assets: teamsAssets[0] }, { assets: teamsAssets[1] }],
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
					// LA (tid 0) gets Role Player + the pick; Boston (tid 1) gets Star Wing.
					tradeEvent(
						[0, 1],
						[
							[
								{ pid: 2, name: "Role Player" },
								{ dpid: 9, season: 2027, round: 1, originalTid: 0 },
							],
							[{ pid: 1, name: "Star Wing" }],
						],
					),
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
					// LA (tid 0) gets Chaney; Boston (tid 1) gets nothing.
					tradeEvent([0, 1], [[{ pid: 1, name: "Chaney Johnson" }], []]),
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

	test("a trade INSIDE a sim day is announced alongside the game results", async () => {
		// A CPU trade that lands during a sim used to be swallowed - the sim
		// short-circuits to game summaries. Now the trade event gets its own ping
		// on top of the game notification.
		const notifs = await buildNotifications(
			"playMenu.day",
			{
				changes: [
					gameWithBoxScore(),
					tradeEvent(
						[0, 1],
						[
							[{ pid: 21, name: "Traded Guy" }],
							[{ pid: 22, name: "Other Guy" }],
						],
					),
				],
			},
			opts,
		);
		const trade = notifs.find((n) => n.title === "Trade");
		assert.ok(trade, JSON.stringify(notifs));
		assert.ok(trade!.body.includes("Traded Guy"), trade!.body);
		assert.ok(trade!.body.includes("Other Guy"), trade!.body);
		// The game summary is still there too.
		assert.ok(
			notifs.some((n) => n.title !== "Trade"),
			JSON.stringify(notifs),
		);
	});

	test("the draft lottery result is announced (who won the #1 pick)", async () => {
		g.setWithoutSavingToDB("phase", PHASE.DRAFT_LOTTERY);
		const notifs = await buildNotifications(
			"main.draftLottery",
			{
				changes: [
					{
						store: "draftLotteryResults",
						id: 2026,
						type: "put",
						value: {
							season: 2026,
							result: [
								{ tid: 1, originalTid: 1, chances: 140, pick: 1, dpid: 10 },
								{ tid: 0, originalTid: 0, chances: 120, pick: 2, dpid: 11 },
							],
						},
					},
				],
			},
			opts,
		);
		const lotto = notifs.find((n) => n.title.includes("draft lottery"));
		assert.ok(lotto, JSON.stringify(notifs));
		assert.ok(lotto!.body.includes("#1 pick"), lotto!.body);
		// tid 1 (Boston) drew the top pick.
		assert.ok(lotto!.body.includes("Boston Celtics"), lotto!.body);
		assert.strictEqual(lotto!.path, "draft_lottery");
	});

	test("a live lottery reveal HOLDS the result push, then endLotteryReveal releases it", async () => {
		g.setWithoutSavingToDB("phase", PHASE.DRAFT_LOTTERY);
		const changeset: Changeset = {
			changes: [
				{
					store: "draftLotteryResults",
					id: 2026,
					type: "put",
					value: {
						season: 2026,
						result: [
							{ tid: 1, originalTid: 1, chances: 140, pick: 1, dpid: 10 },
							{ tid: 0, originalTid: 0, chances: 120, pick: 2, dpid: 11 },
						],
					},
				},
			],
		};

		// A reveal is in progress: the lottery push must NOT go out yet.
		beginLotteryReveal();
		const duringReveal = await buildNotifications(
			"main.draftLottery",
			changeset,
			opts,
		);
		assert.ok(
			!duringReveal.some((n) => n.title.includes("draft lottery")),
			JSON.stringify(duringReveal),
		);

		// When the reveal finishes, the held push is handed back so it can fire.
		const released = endLotteryReveal();
		const lotto = released.find((n) => n.title.includes("draft lottery"));
		assert.ok(lotto, JSON.stringify(released));
		assert.ok(lotto!.body.includes("Boston Celtics"), lotto!.body);
		assert.strictEqual(lotto!.path, "draft_lottery");

		// And the buffer is emptied - a second release yields nothing.
		assert.strictEqual(endLotteryReveal().length, 0);
	});

	describe("you're on the clock", () => {
		// A pick was just made in this changeset (its draft event), leaving the
		// picks below still on the board.
		const draftEvent: Changeset["changes"][number] = {
			store: "events",
			id: 1,
			type: "put",
			value: { type: "draft", pids: [999] },
		};

		const remainingPick = (dpid: number, tid: number, pick: number) => ({
			dpid,
			season: 2026,
			round: 1,
			pick,
			tid,
			originalTid: tid,
		});

		test("pings the user team now up after a pick", async () => {
			g.setWithoutSavingToDB("phase", PHASE.DRAFT);
			g.setWithoutSavingToDB("userTids", [0, 1]);
			await resetCache({
				teams: [
					{ tid: 0, region: "LA", name: "Lakers", abbrev: "LAL" },
					{ tid: 1, region: "Boston", name: "Celtics", abbrev: "BOS" },
				],
				draftPicks: [remainingPick(10, 1, 3), remainingPick(11, 0, 4)],
			});

			const notifs = await buildNotifications(
				"playMenu.onePick",
				{ changes: [draftEvent] },
				opts,
			);
			const clock = notifs.find((n) => n.title === "You're on the clock!");
			assert.ok(clock, JSON.stringify(notifs));
			// Boston owns the first remaining pick, so only Boston is pinged.
			assert.deepStrictEqual(clock!.targetTids, [1]);
			assert.ok(clock!.body.includes("Boston Celtics"), clock!.body);
			assert.strictEqual(clock!.path, "draft");
		});

		test("stays quiet when an AI team is up or nothing draft-related happened", async () => {
			g.setWithoutSavingToDB("phase", PHASE.DRAFT);
			g.setWithoutSavingToDB("userTids", [0]);
			await resetCache({
				teams: [
					{ tid: 0, region: "LA", name: "Lakers", abbrev: "LAL" },
					{ tid: 1, region: "Boston", name: "Celtics", abbrev: "BOS" },
				],
				// tid 1 is up next but is NOT a user team here.
				draftPicks: [remainingPick(10, 1, 3), remainingPick(11, 0, 4)],
			});

			const afterPick = await buildNotifications(
				"playMenu.onePick",
				{ changes: [draftEvent] },
				opts,
			);
			assert.ok(
				!afterPick.some((n) => n.title === "You're on the clock!"),
				JSON.stringify(afterPick),
			);

			// A draft-phase changeset with no picks made must not re-ping either,
			// even though a user team is on the clock.
			g.setWithoutSavingToDB("userTids", [0, 1]);
			const noPicks = await buildNotifications(
				"main.setNote2",
				{
					changes: [
						{ store: "players", id: 5, type: "put", value: { pid: 5 } },
					],
				},
				opts,
			);
			assert.ok(
				!noPicks.some((n) => n.title === "You're on the clock!"),
				JSON.stringify(noPicks),
			);
		});

		test("pings the first team up when the draft phase starts", async () => {
			g.setWithoutSavingToDB("phase", PHASE.DRAFT);
			g.setWithoutSavingToDB("userTids", [0]);
			await resetCache({
				teams: [
					{ tid: 0, region: "LA", name: "Lakers", abbrev: "LAL" },
					{ tid: 1, region: "Boston", name: "Celtics", abbrev: "BOS" },
				],
				draftPicks: [remainingPick(10, 0, 1), remainingPick(11, 1, 2)],
			});

			const notifs = await buildNotifications(
				"playMenu.day",
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
				opts,
			);
			const clock = notifs.find((n) => n.title === "You're on the clock!");
			assert.ok(clock, JSON.stringify(notifs));
			assert.deepStrictEqual(clock!.targetTids, [0]);
			assert.ok(clock!.body.includes("1st pick"), clock!.body);
		});
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

	test("a sub-$1M signing reads as $350k, not $0M", async () => {
		// season 2026, exp 2026 => 1 year at 350/yr (thousands) => $350k total.
		// This used to round to "$0M".
		const notifs = await buildNotifications(
			"main.signFreeAgent",
			{
				changes: [
					freeAgentEvent,
					namedPlayer(1, 0, "Min", "Deal", 80, {
						contract: { amount: 350, exp: 2026 },
					}),
				],
			},
			opts,
		);
		assert.ok(notifs[0]!.body.includes("1-year, $350k"), notifs[0]!.body);
		assert.ok(!notifs[0]!.body.includes("$0M"), notifs[0]!.body);
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
					// The draft event is the authoritative "a pick just happened" signal.
					{
						store: "events",
						id: 100,
						type: "put",
						value: { eid: 100, type: "draft", pids: [7], tids: [0] },
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

	test("every first-round pick is announced, even CPU picks the simmer advances", async () => {
		// The simmer advances CPU picks via playMenu.onePick (a "sim"-looking label)
		// and the pick can even land in AFTER_DRAFT. A first-round pick for a team
		// that ISN'T the user's must still be narrated to the whole room.
		g.setWithoutSavingToDB("phase", PHASE.AFTER_DRAFT);
		g.setWithoutSavingToDB("numActiveTeams", 30);
		const notifs = await buildNotifications(
			"playMenu.onePick",
			{
				changes: [
					{
						store: "players",
						id: 12,
						type: "put",
						value: {
							pid: 12,
							tid: 1, // Celtics - NOT the user's team (userTids = [0])
							firstName: "Cpu",
							lastName: "Prospect",
							ratings: [{ ovr: 60, pot: 80, pos: "PG" }],
							draft: {
								round: 1,
								pick: 5,
								year: 2026,
								tid: 1,
								ovr: 60,
								pot: 80,
							},
						},
					},
					{
						store: "events",
						id: 101,
						type: "put",
						value: { eid: 101, type: "draft", pids: [12], tids: [1] },
					},
				],
			},
			opts,
		);
		assert.strictEqual(notifs[0]!.title, "Draft pick");
		assert.ok(
			notifs[0]!.body.includes("Boston Celtics select Cpu Prospect"),
			notifs[0]!.body,
		);
		assert.strictEqual(notifs[0]!.targetTids, null); // everyone
	});

	test("a rookie's record WITHOUT a draft event never re-announces the pick", async () => {
		// A rookie's player record keeps matching draft-shaped predicates all
		// offseason (draft.year === season, still on the drafting team). Any later
		// changeset carrying the record - a phase change, a free-agency day - used
		// to re-fire "Draft pick" pushes hours after the draft, once per changeset.
		g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
		g.setWithoutSavingToDB("numActiveTeams", 30);
		const notifs = await buildNotifications(
			"playMenu.day",
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
							ratings: [{ ovr: 55, pot: 78, pos: "SF" }],
							draft: { round: 1, pick: 3, year: 2026, tid: 0 },
						},
					},
				],
			},
			opts,
		);
		assert.ok(
			!notifs.some((n) => n.title === "Draft pick"),
			JSON.stringify(notifs),
		);
	});

	test("deep-link paths: trade → transactions, signing → player page", async () => {
		const trade = await buildNotifications(
			"main.proposeTrade",
			{
				changes: [
					namedPlayer(1, 1, "Star", "Wing", 88),
					namedPlayer(2, 0, "Role", "Player", 74),
					tradeEvent(
						[0, 1],
						[
							[{ pid: 2, name: "Role Player" }],
							[{ pid: 1, name: "Star Wing" }],
						],
					),
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
