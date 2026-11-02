import { afterAll, assert, beforeAll, test } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { createArenaLeague } from "../../../test/arena/runLeague.ts";
import { game, phase, season } from "../index.ts";
import { getUpcoming } from "../../views/schedule.ts";

const importNode = (name: string) => import(/* @vite-ignore */ name);
const realRandom = Math.random;
const conditions = {} as any;

beforeAll(async () => {
	await importNode("fake-indexeddb/auto");
	Math.random = makeSeededRandom(11);
	// The test setup shares modules between files, and another may have left a
	// stand-in database here; creating a league would try to close it.
	(idb as any).league = undefined;
	await createArenaLeague(9077);
	await phase.newPhase(PHASE.REGULAR_SEASON, conditions);
}, 300_000);

afterAll(() => {
	Math.random = realRandom;
});

// A day's games: the spread every page quotes for each of them just before
// they are played (getUpcoming - the schedule, the score boxes, the ticker),
// then the one saved on each as it is played (Game.spread - what the finished
// game shows, and what ATS records grade it against).
const playDay = async () => {
	const schedule = await season.getSchedule(true);
	const today = schedule[0]?.day;
	if (today === undefined) {
		return [];
	}
	const quoted = new Map(
		(await getUpcoming({ day: today })).map((x) => [x.gid, x.spread]),
	);
	await game.play(1, conditions);
	const played = await idb.merged.games.getAll({ season: g.get("season") });
	const out: { gid: number; quoted?: number; saved?: number }[] = [];
	for (const [gid, spread] of quoted) {
		const gm = played.find((x) => x.gid === gid);
		if (gm) {
			out.push({ gid, quoted: spread, saved: gm.spread });
		}
	}
	return out;
};

// The line quoted before tipoff is the line the game keeps. It was not: the
// quote read its lineup off the roster ordered by value, the sim reads the
// roster order (and re-sorts CPU rosters right before each regular-season
// day's games), so the two priced different fives - the quote came out a point
// or more off the saved line in most games, and up to 8 in the playoffs, where
// synergy counts double.
test(
	"the spread quoted before a game is the one saved on it, regular season and playoffs",
	{ timeout: 900_000 },
	async () => {
		const regular = [];
		for (let i = 0; i < 8; i++) {
			regular.push(...(await playDay()));
		}
		await game.play(await season.getDaysLeftSchedule(), conditions);
		if (g.get("phase") < PHASE.PLAYOFFS) {
			await phase.newPhase(PHASE.PLAYOFFS, conditions);
		}
		const playoffs = [];
		for (let i = 0; i < 4; i++) {
			playoffs.push(...(await playDay()));
		}

		for (const [rows, what] of [
			[regular, "regular season"],
			[playoffs, "playoffs"],
		] as const) {
			assert.isAbove(rows.length, 10, what);
			const off = rows.filter(
				(r) => r.quoted === undefined || r.quoted !== r.saved,
			);
			assert.deepEqual(off, [], `${what}: quoted vs saved`);
		}
	},
);
