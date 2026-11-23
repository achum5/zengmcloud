import { assert, beforeEach, describe, test } from "vitest";
import { mockIDBLeague, resetCache, resetG } from "../../test/helpers.ts";
import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";
import { changeTracker } from "../db/changeTracker.ts";
import { nationalBroadcastOf } from "./nationalBroadcast.ts";
import type { NationalBroadcast } from "../../common/types.ts";

const network = (name: string, nights: string[]): NationalBroadcast => ({
	network: name,
	nights,
	scoreBug: {
		width: 100,
		height: 10,
		pieces: [{ show: "text", x: 0, y: 0, w: 100, h: 10, text: name }],
	},
});

const game = (gid: number, day: number, home: number, away: number) =>
	({
		gid,
		season: 2026,
		day,
		playoffs: false,
		teams: [
			{ tid: home, pts: 100 },
			{ tid: away, pts: 90 },
		],
	}) as any;

// Six teams, rated 0 (worst) to 5 (best) as the season began.
const setup = async (games: any[]) => {
	resetG();
	g.setWithoutSavingToDB("season", 2026);
	g.setWithoutSavingToDB("nationalBroadcasts", [
		network("NBC", ["Tue"]),
		network("ESPN", ["Wed"]),
	]);
	await resetCache({
		teamSeasons: [0, 1, 2, 3, 4, 5].map((tid) => ({
			rid: tid,
			tid,
			season: 2026,
			ovrStart: 40 + tid,
		})),
	});
	for (const game of games) {
		await idb.cache.games.put(game);
	}
	idb.league = { ...mockIDBLeague(), getAll: async () => [] } as any;
};

describe("national broadcasts", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	test("opening night's biggest game is NBC's; the rest are nobody's", async () => {
		// Opening night (day 1, a Tuesday): 5 v 4 is the biggest.
		const night = [game(1, 1, 0, 1), game(2, 1, 5, 4), game(3, 1, 2, 3)];
		await setup(night);
		assert.strictEqual((await nationalBroadcastOf(night[1]))?.network, "NBC");
		assert.isUndefined(await nationalBroadcastOf(night[0]));
		assert.isUndefined(await nationalBroadcastOf(night[2]));
	});

	test("the next night, a Wednesday: ESPN's, ratings counting most a night in", async () => {
		// After opening night 0, 2, 5 are 1-0 (5 best rated); on day 2, a
		// Wednesday, 0 v 2 between two winners vs 1 v 4 between two losers.
		const played = [game(1, 1, 0, 1), game(2, 1, 5, 4), game(3, 1, 2, 3)];
		const night = [game(4, 2, 1, 4), game(5, 2, 0, 2)];
		await setup([...played, ...night]);
		// (Ratings still count for most of it: 1 v 4 rate 0.2 + 0.8 to 0 v
		// 2's 0 + 0.4, so 1 v 4 is still bigger after one night.)
		assert.strictEqual((await nationalBroadcastOf(night[0]))?.network, "ESPN");
		assert.isUndefined(await nationalBroadcastOf(night[1]));
	});

	test("a night with no network, a playoff game, none set", async () => {
		const night = [game(1, 3, 5, 4)];
		await setup(night);
		// Day 3 of a season that opened on day 3: a Tuesday.
		assert.strictEqual((await nationalBroadcastOf(night[0]))?.network, "NBC");
		const thursday = game(2, 5, 5, 4);
		await setup([night[0], thursday]);
		assert.isUndefined(await nationalBroadcastOf(thursday));
		const playoff = { ...thursday, playoffs: true };
		assert.include(
			["NBC", "ESPN"],
			(await nationalBroadcastOf(playoff))?.network,
		);
		g.setWithoutSavingToDB("nationalBroadcasts", []);
		assert.isUndefined(await nationalBroadcastOf(night[0]));
	});
});
