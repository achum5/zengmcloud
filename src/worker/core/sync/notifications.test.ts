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
		assert.ok(notifs[0]!.body.includes("LA Lakers went 1-1 this week"));
		// Home win vs BOS, away loss @ BOS.
		assert.ok(notifs[0]!.body.includes("W vs BOS 110-105"));
		assert.ok(notifs[0]!.body.includes("L @ BOS 98-102"));
	});

	test("single game → team result line + top game-score stat line per team", async () => {
		const notifs = await buildNotifications(
			"playMenu.day",
			{ changes: [gameWithBoxScore()] },
			opts,
		);
		const body = notifs[0]!.body;
		assert.ok(body.includes("LA Lakers W vs BOS 110-86"), body);
		// Star Guy outscores Bench Guy on Game Score; REB = orb + drb = 8.
		assert.ok(body.includes("Star Guy: 30 PTS, 8 REB, 11 AST"), body);
		assert.ok(body.includes("Opp Ace: 25 PTS, 5 REB, 5 AST"), body);
	});

	test("team with no game still gets a targeted 'league advanced' notice", async () => {
		const notifs = await buildNotifications("playMenu.day", { changes: [] }, opts);
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
		assert.strictEqual(notifs[0]!.title, "Sim complete");
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

	test("host sim that reaches a human phase → single 'your turn' to everyone", async () => {
		const notifs = await buildNotifications(
			"playMenu.week",
			{ changes: [phasePut(PHASE.DRAFT)] },
			opts,
		);
		assert.strictEqual(notifs.length, 1);
		assert.strictEqual(notifs[0]!.title, "Your league needs you");
		assert.strictEqual(notifs[0]!.targetTids, null);
	});

	test("non-host never announces a sim", async () => {
		const notifs = await buildNotifications(
			"playMenu.day",
			{ changes: [gamePut(1, { tid: 0, pts: 110 }, { tid: 1, pts: 105 })] },
			{ ...opts, isHost: false },
		);
		assert.deepEqual(notifs, []);
	});

	test("players moving to two teams → trade, to everyone", async () => {
		const notifs = await buildNotifications(
			"main.proposeTrade",
			{ changes: [playerPut(1, 0), playerPut(2, 1)] },
			opts,
		);
		assert.strictEqual(notifs[0]!.title, "Trade completed");
		assert.strictEqual(notifs[0]!.targetTids, null);
		assert.ok(notifs[0]!.body.includes("Alex"));
	});

	test("a single roster change → roster move", async () => {
		const notifs = await buildNotifications(
			"main.signFreeAgent",
			{ changes: [playerPut(1, 0)] },
			opts,
		);
		assert.strictEqual(notifs[0]!.title, "Roster move");
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
