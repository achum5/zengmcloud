import { afterAll, assert, beforeAll, test } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import { createArenaLeague } from "../../../test/arena/runLeague.ts";
import { currentOutlook } from "./market.ts";
import { teamPoints } from "./outlook.ts";

const importNode = (name: string) => import(/* @vite-ignore */ name);

const realRandom = Math.random;

beforeAll(async () => {
	await importNode("fake-indexeddb/auto");
	Math.random = makeSeededRandom(6);
	// The test setup shares modules between files, and another may have left a
	// stand-in database here; creating a league would try to close it.
	(idb as any).league = undefined;
	await createArenaLeague(9011);
	g.setWithoutSavingToDB("realisticFrontOffices", true);
	g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
}, 120_000);

afterAll(() => {
	Math.random = realRandom;
});

test(
	"before the deadline, a star back for the playoffs is worth his playoffs",
	{ timeout: 120_000 },
	async () => {
		const outlook = await currentOutlook();
		const tid = 3;
		const players = (
			await idb.cache.players.indexGetAll("playersByTid", tid)
		).sort((a, b) => b.valueNoPot - a.valueNoPot);
		const remaining = outlook.remaining.get(tid)!;
		const out = (games: number) =>
			players.map((p, i) =>
				i === 0
					? {
							...helpers.deepCopy(p),
							injury: { type: "Torn ACL", gamesRemaining: games },
						}
					: p,
			);
		const backForPlayoffs = out(remaining + 1);
		const outForTheYear = out(remaining + 200);
		const before = { ...outlook, playoffWeight: 0.7, playoffGames: 20 };
		const points = (o: typeof outlook, roster: typeof players) =>
			teamPoints(o, tid, roster, [])[0]!;

		// By the regular season alone, the two are the same.
		assert.closeTo(
			points({ ...outlook, playoffWeight: undefined }, backForPlayoffs),
			points({ ...outlook, playoffWeight: undefined }, outForTheYear),
			1e-9,
		);
		// With the playoffs counting, the one back for them is worth more, and
		// less than if he were healthy now.
		const back = points(before, backForPlayoffs);
		const gone = points(before, outForTheYear);
		const healthy = points(before, players);
		assert.isAbove(back, gone + 0.5);
		assert.isBelow(back, healthy);
		assert.closeTo(points(before, outForTheYear), gone, 1e-9);
	},
);
