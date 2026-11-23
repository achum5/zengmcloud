import { assert, beforeEach, describe, test } from "vitest";
import { mockIDBLeague, resetCache, resetG } from "../../test/helpers.ts";
import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";
import { changeTracker } from "../db/changeTracker.ts";
import {
	nationalBroadcastOf,
	nationalSchedule,
	nationalTags,
} from "./nationalBroadcast.ts";
import { PHASE } from "../../common/constants.ts";
import type { NationalBroadcast } from "../../common/types.ts";
import { GENERIC_NATIONAL } from "../../common/genericNational.ts";

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

// Six teams, rated 0 (worst) to 5 (best) as the season began (and their
// records, if given).
const setup = async (
	games: any[],
	records: Record<number, [number, number]> = {},
) => {
	resetG();
	g.setWithoutSavingToDB("season", 2026);
	g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
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
			won: records[tid]?.[0] ?? 0,
			lost: records[tid]?.[1] ?? 0,
			tied: 0,
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

	test("a night with no network, a playoff game, none set (ours)", async () => {
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
		// None set: ours.
		g.setWithoutSavingToDB("nationalBroadcasts", []);
		assert.strictEqual(await nationalBroadcastOf(night[0]), GENERIC_NATIONAL);
	});

	test("previews: a night still to come has the game it'll have once played", async () => {
		const scheduled = (night: any[]) =>
			night.map((x) => ({
				gid: x.gid,
				day: x.day,
				homeTid: x.teams[0].tid,
				awayTid: x.teams[1].tid,
			}));
		// Opening night, nothing played yet.
		const opening = [game(1, 1, 0, 1), game(2, 1, 5, 4), game(3, 1, 2, 3)];
		await setup([]);
		let picks = await nationalSchedule(scheduled(opening));
		assert.deepStrictEqual(
			[...picks].map(([gid, n]) => [gid, n.network]),
			[[2, "NBC"]],
		);

		// A night in: 0, 2, 5 are 1-0.
		const night = [game(4, 2, 1, 4), game(5, 2, 0, 2)];
		await setup(opening, {
			0: [1, 0],
			2: [1, 0],
			5: [1, 0],
			1: [0, 1],
			3: [0, 1],
			4: [0, 1],
		});
		picks = await nationalSchedule(scheduled(night));
		assert.deepStrictEqual(
			[...picks].map(([gid, n]) => [gid, n.network]),
			[[4, "ESPN"]],
		);
		await setup([...opening, ...night]);
		assert.strictEqual((await nationalBroadcastOf(night[0]))?.network, "ESPN");

		// The playoffs: every game.
		g.setWithoutSavingToDB("phase", PHASE.PLAYOFFS);
		picks = await nationalSchedule(scheduled(night));
		assert.strictEqual(picks.size, 2);

		// Tags: the logo shown, else the name.
		g.setWithoutSavingToDB("nationalBroadcasts", [
			{ ...network("NBC", ["Tue"]), logo: "https://example.com/nbc.png" },
			network("ESPN", ["Wed"]),
		]);
		for (const tag of (await nationalTags(scheduled(night))).values()) {
			assert.strictEqual(
				tag.logo,
				tag.network === "NBC" ? "https://example.com/nbc.png" : undefined,
			);
		}
	});
});
