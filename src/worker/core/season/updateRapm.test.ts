import { assert, describe, test } from "vitest";
import { stintsFromGames, updateRapm } from "./updateRapm.ts";
import { encodeShifts } from "../../util/gameShifts.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { resetCache, resetG } from "../../../test/helpers.ts";
import player from "../player/index.ts";
import type { Game } from "../../../common/types.ts";

const game = ({
	playoffs = false,
	tids = [3, 7],
	shifts,
}: {
	playoffs?: boolean;
	tids?: [number, number];
	shifts?: number[];
}) =>
	({
		playoffs,
		numPlayersOnCourt: 5,
		shifts,
		teams: [{ tid: tids[0] }, { tid: tids[1] }],
	}) as unknown as Game;

const oneShift = encodeShifts(
	[
		{
			lineups: [
				[1, 2, 3, 4, 5],
				[11, 12, 13, 14, 15],
			],
			poss: [40, 41],
			pts: [44, 39],
		},
	],
	5,
);

describe("stintsFromGames", () => {
	// One matchup is two observations: each team's turn on offense against the
	// other five.
	test("a matchup becomes both sides of it", () => {
		const stints = stintsFromGames([game({ shifts: oneShift })]);
		assert.deepStrictEqual(stints, [
			{
				off: ["1|3", "2|3", "3|3", "4|3", "5|3"],
				def: ["11|7", "12|7", "13|7", "14|7", "15|7"],
				poss: 40,
				pts: 44,
			},
			{
				off: ["11|7", "12|7", "13|7", "14|7", "15|7"],
				def: ["1|3", "2|3", "3|3", "4|3", "5|3"],
				poss: 41,
				pts: 39,
			},
		]);
	});

	// A player is a separate regressor for each team he played for, so the same
	// man traded midseason never has his two stints averaged together.
	test("the same player on two teams is two keys", () => {
		const stints = stintsFromGames([
			game({ shifts: oneShift, tids: [3, 7] }),
			game({ shifts: oneShift, tids: [9, 7] }),
		]);
		const keys = new Set(stints.flatMap((stint) => stint.off));
		assert.isTrue(keys.has("1|3"));
		assert.isTrue(keys.has("1|9"));
	});

	test("the playoffs are not part of it", () => {
		assert.deepStrictEqual(
			stintsFromGames([game({ shifts: oneShift, playoffs: true })]),
			[],
		);
	});

	test("a game from before this was recorded is skipped", () => {
		assert.deepStrictEqual(stintsFromGames([game({})]), []);
	});

	// Nothing to say about a side that never had the ball.
	test("a side with no possessions is not an observation", () => {
		const shifts = encodeShifts(
			[
				{
					lineups: [
						[1, 2, 3, 4, 5],
						[11, 12, 13, 14, 15],
					],
					poss: [3, 0],
					pts: [4, 0],
				},
			],
			5,
		);
		const stints = stintsFromGames([game({ shifts })]);
		assert.strictEqual(stints.length, 1);
		assert.strictEqual(stints[0]!.poss, 3);
	});
});

// End to end, on a league small enough to reason about: two teams, ten men,
// a season of games in the cache, and the ratings landing on the stats rows
// the pages read.
describe("updateRapm", () => {
	const buildLeague = async () => {
		resetG();
		const season = g.get("season");

		const players = [];
		for (let i = 0; i < 10; i++) {
			const tid = i < 5 ? 0 : 1;
			const p: any = player.generate(tid, 25, season, true, 20);
			p.pid = i;
			p.tid = tid;
			p.stats = [{ season, tid, playoffs: false, gp: 82, min: 2400 } as any];
			players.push(p);
		}
		await resetCache({ players });

		// One matchup, played over and over, with team 0 the better side.
		const shifts = encodeShifts(
			[
				{
					lineups: [
						[0, 1, 2, 3, 4],
						[5, 6, 7, 8, 9],
					],
					poss: [100, 100],
					pts: [115, 100],
				},
			],
			5,
		);
		for (let gid = 0; gid < 82; gid++) {
			await idb.cache.games.add({
				gid,
				season,
				numPlayersOnCourt: 5,
				shifts,
				teams: [{ tid: 0 }, { tid: 1 }],
			} as any);
		}
	};

	test("the ratings land on the season's stats rows", async () => {
		await buildLeague();
		await updateRapm();

		const winner = (await idb.cache.players.get(0))!.stats[0] as any;
		const loser = (await idb.cache.players.get(9))!.stats[0] as any;

		assert.isNumber(winner.rapm);
		assert.strictEqual(winner.rapm, winner.orapm + winner.drapm);
		assert.isAbove(winner.rapm, loser.rapm);
	});

	test("a season with no lineup data changes nothing", async () => {
		resetG();
		const p: any = player.generate(0, 25, g.get("season"), true, 20);
		p.pid = 0;
		p.stats = [{ season: g.get("season"), tid: 0, playoffs: false } as any];
		await resetCache({ players: [p] });

		await updateRapm();
		assert.isUndefined((await idb.cache.players.get(0))!.stats[0]!.rapm);
	});
});
