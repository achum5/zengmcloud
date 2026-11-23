import { assert, describe, test } from "vitest";
import { simRealGame } from "../GameSim.basketball/realGameForTests.ts";
import { PHASE } from "../../../common/constants.ts";
import { ALL_STAR_GAME_TIDS } from "../../../common/specialGames.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import {
	RECAP_NIGHTS,
	saveRecapReels,
	wantsRecapReel,
} from "./saveRecapReels.ts";

// Everything a game's result says, but its play-by-play.
const box = (result: any) =>
	JSON.stringify(
		result.team.map((t: any) => ({
			id: t.id,
			stat: t.stat,
			players: t.player.map((p: any) => [p.id, p.stat, p.injury]),
		})),
	);

describe("saveRecapReels", () => {
	test("simming with play-by-play on changes no result", async () => {
		for (const seed of [3, 17, 40]) {
			const off = await simRealGame(seed, { playByPlay: false });
			const on = await simRealGame(seed, { playByPlay: true });
			assert.strictEqual(off.events, undefined);
			assert.isAbove(on.events.length, 1000);
			assert.strictEqual(box(on.result), box(off.result), `seed ${seed}`);
		}
	}, 120_000);

	test("which games get recap plays", () => {
		g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
		assert.isTrue(wantsRecapReel(3, 5));
		assert.isFalse(
			wantsRecapReel(ALL_STAR_GAME_TIDS[0], ALL_STAR_GAME_TIDS[1]),
		);
		g.setWithoutSavingToDB("phase", PHASE.PLAYOFFS);
		assert.isTrue(wantsRecapReel(3, 5));
		g.setWithoutSavingToDB("phase", PHASE.DRAFT);
		assert.isFalse(wantsRecapReel(3, 5));
	});

	test("saves each game's plays, keeping the last nights only", async () => {
		const { result } = await simRealGame(5);
		g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
		const season = g.get("season");
		// A game a night, nights 1 to 10 - and one from last season.
		await idb.cache.recapReels.put({
			gid: 900,
			season: season - 1,
			day: 50,
			reel: { events: [], plays: [] },
		});
		for (let day = 1; day <= 10; day++) {
			await saveRecapReels([{ ...result, gid: 100 + day, day }]);
		}
		const rows = await idb.cache.recapReels.getAll();
		assert.deepEqual(
			rows.map((row) => row.day).sort((a, b) => a - b),
			[4, 5, 6, 7, 8, 9, 10],
		);
		assert.strictEqual(rows.length, RECAP_NIGHTS);
		const row = rows.find((row) => row.gid === 110)!;
		assert.strictEqual(row.season, season);
		assert.isAbove(row.reel.plays.length, 10);

		// Each team's best player is one of its stars: their best plays are in.
		const pids = new Set(row.reel.plays.map((p) => p.pid));
		for (const t of result.team) {
			assert.isTrue(
				t.player.some((p: any) => pids.has(p.id)),
				`a play of team ${t.id}`,
			);
		}

		// A game simmed without play-by-play (not a recap game) is passed by.
		await saveRecapReels([
			{ ...result, gid: 200, day: 11, playByPlay: undefined },
		]);
		assert.isUndefined(await idb.cache.recapReels.get(200));
	}, 120_000);
});
