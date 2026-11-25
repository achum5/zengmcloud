import { afterAll, afterEach, assert, beforeAll, test } from "vitest";
import { PHASE } from "../../common/constants.ts";
import { makeSeededRandom } from "../../common/random.ts";
import { idb } from "../db/index.ts";
import { g } from "./index.ts";
import { createArenaLeague } from "../../test/arena/runLeague.ts";
import { game, phase, season } from "../core/index.ts";
import playerStatsInfo from "../core/player/stats.ts";
import { setLiveSpoilerProvider } from "./liveSpoilers.ts";
import { getLiveShown } from "./liveGameShown.ts";
import type { LiveShown } from "../../common/types.ts";

// A real league, a real day simmed: every page's numbers with that day's
// games held back must be exactly the numbers from before it was played.

const importNode = (name: string) => import(/* @vite-ignore */ name);
const realRandom = Math.random;
const conditions = {} as any;

beforeAll(async () => {
	await importNode("fake-indexeddb/auto");
	Math.random = makeSeededRandom(23);
	(idb as any).league = undefined;
	await createArenaLeague(9411);
	await phase.newPhase(PHASE.REGULAR_SEASON, conditions);
	await game.play(12, conditions);
}, 600_000);

afterEach(() => {
	setLiveSpoilerProvider(undefined);
});

afterAll(() => {
	Math.random = realRandom;
});

const TEAM_SEASON_KEYS = [
	"won",
	"lost",
	"tied",
	"otl",
	"wonHome",
	"lostHome",
	"wonAway",
	"lostAway",
	"wonDiv",
	"lostDiv",
	"wonConf",
	"lostConf",
	"streak",
	"lastTen",
	"gpHome",
	"att",
	"playoffRoundsWon",
] as const;

type Snapshot = Awaited<ReturnType<typeof snapshot>>;
type PlayerRow = Snapshot["players"][number];
type StatsRow = PlayerRow["stats"][number];

const snapshot = async () => {
	const s = g.get("season");
	return structuredClone({
		teamSeasons: await idb.cache.teamSeasons.indexGetAll(
			"teamSeasonsBySeasonTid",
			[[s], [s, "Z"]],
		),
		teamStats: (await idb.cache.teamStats.getAll()).filter(
			(ts) => ts.season === s,
		),
		players: await idb.cache.players.indexGetAll("playersByTid", [
			-1,
			Infinity,
		]),
		headToHead: await idb.cache.headToHeads.get(s),
	});
};

// Play a day with nobody watching, then hold all of its games back.
const playDayHeld = async () => {
	const before = await snapshot();
	const today = (await season.getSchedule(true)).map((x) => x.gid);
	assert.isAbove(today.length, 0, "games today");
	await game.play(1, conditions);
	setLiveSpoilerProvider(() => ({
		watching: new Set(today),
		room: new Set(),
	}));
	const shown = await getLiveShown();
	assert.isDefined(shown);
	const played = (await idb.cache.games.getAll()).filter((x) =>
		today.includes(x.gid),
	);
	assert.strictEqual(played.length, today.length);
	return { before, shown: shown!, played };
};

const closeRow = (
	actual: Record<string, any>,
	expected: Record<string, any>,
	keys: Iterable<string>,
	label: string,
) => {
	for (const key of keys) {
		const want = expected[key];
		const got = actual[key];
		if (typeof want === "number") {
			assert.closeTo(got, want, 1e-6, `${label} ${key}`);
		} else if (want !== undefined) {
			assert.deepEqual(got, want, `${label} ${key}`);
		}
	}
};

const checkTeams = async (before: Snapshot, shown: LiveShown) => {
	const after = await snapshot();
	let changed = 0;
	for (const ts of after.teamSeasons) {
		const was = before.teamSeasons.find((x) => x.tid === ts.tid)!;
		if (ts.won + ts.lost !== was.won + was.lost) {
			changed += 1;
		}
		closeRow(shown.teamSeason(ts), was, TEAM_SEASON_KEYS, `tid ${ts.tid}`);
	}
	for (const ts of after.teamStats) {
		const was = before.teamStats.find(
			(x) => x.tid === ts.tid && x.playoffs === ts.playoffs,
		);
		const out = shown.teamStats(ts) as Record<string, any>;
		if (!was) {
			// First playoff game: no playoff row going in.
			assert.isTrue(ts.playoffs);
			assert.isFalse(out.gp > 0, `tid ${ts.tid} new playoff row`);
			continue;
		}
		closeRow(
			out,
			was,
			Object.keys(was).filter((key) => key !== "rid"),
			`stats tid ${ts.tid} playoffs ${ts.playoffs}`,
		);
	}
	return changed;
};

const checkPlayers = async (before: Snapshot, shown: LiveShown) => {
	const after = await snapshot();
	const s = g.get("season");
	const keys = [...playerStatsInfo.raw, ...playerStatsInfo.max];
	let played = 0;
	let hurt = 0;
	for (const p of after.players) {
		const was = before.players.find((x) => x.pid === p.pid);
		if (!was) {
			continue;
		}
		const out = shown.player(p);
		// This season's rows, team by team.
		const rows = (x: PlayerRow): StatsRow[] =>
			x.stats.filter((row) => row.season === s);
		// (Matched in order: a player traded away and back has two rows for
		// the same team.)
		const same = (x: PlayerRow, row: StatsRow): StatsRow[] =>
			rows(x).filter((y) => y.tid === row.tid && y.playoffs === row.playoffs);
		for (const row of rows(was)) {
			const k: number = same(was, row).indexOf(row);
			const shownRow: StatsRow | undefined = same(out, row)[k];
			assert.isDefined(shownRow, `pid ${p.pid} row`);
			closeRow(shownRow!, row, keys, `pid ${p.pid}`);
		}
		// Nor one the game added (his first playoff game for a team).
		assert.strictEqual(rows(out).length, rows(was).length, `pid ${p.pid} rows`);
		if (rows(p).some((row, i) => row.gp !== rows(was)[i]?.gp)) {
			played += 1;
		}

		if (p.injuries.length > was.injuries.length) {
			hurt += 1;
			assert.deepEqual(
				{ type: out.injury.type, gamesRemaining: out.injury.gamesRemaining },
				{ type: was.injury.type, gamesRemaining: was.injury.gamesRemaining },
				`pid ${p.pid} injury`,
			);
			assert.strictEqual(out.injuries.length, was.injuries.length);
			assert.strictEqual(out.ratings.length, was.ratings.length);
		}
	}
	return { played, hurt };
};

const checkHeadToHead = (before: Snapshot, shown: LiveShown, after: any) => {
	const out = shown.headToHead(after);
	assert.deepEqual(out.regularSeason, before.headToHead?.regularSeason ?? {});
	assert.deepEqual(
		structuredClone(out.playoffs),
		structuredClone(before.headToHead?.playoffs ?? {}),
	);
};

test("a regular season day held back: records, team and player stats, highs, injuries, head-to-head", async () => {
	// Days until one with an injury in it, so that's covered too.
	let hurtSeen = 0;
	for (let i = 0; i < 6; i++) {
		const { before, shown } = await playDayHeld();
		const changed = await checkTeams(before, shown);
		assert.isAbove(changed, 0, "the day changed some records");
		const { played, hurt } = await checkPlayers(before, shown);
		assert.isAbove(played, 0, "the day changed some player lines");
		hurtSeen += hurt;
		checkHeadToHead(
			before,
			shown,
			await idb.cache.headToHeads.get(g.get("season")),
		);
		setLiveSpoilerProvider(undefined);
		if (hurtSeen > 0 && i >= 1) {
			break;
		}
	}
	assert.isAbove(hurtSeen, 0, "someone was hurt on a held day");
}, 600_000);

test("a playoff day held back: rounds won, playoff rows, the series", async () => {
	await game.play(await season.getDaysLeftSchedule(), conditions);
	if (g.get("phase") < PHASE.PLAYOFFS) {
		await game.play(await season.getDaysLeftSchedule(), conditions);
	}
	assert.strictEqual(g.get("phase"), PHASE.PLAYOFFS);
	let roundsWonSeen = 0;
	for (let i = 0; i < 30 && g.get("phase") === PHASE.PLAYOFFS; i++) {
		const series = structuredClone(
			(await idb.cache.playoffSeries.get(g.get("season")))!.series,
		);
		const { before, shown } = await playDayHeld();
		await checkTeams(before, shown);
		await checkPlayers(before, shown);
		checkHeadToHead(
			before,
			shown,
			await idb.cache.headToHeads.get(g.get("season")),
		);
		const after = await idb.cache.playoffSeries.get(g.get("season"));
		const unplayed = shown.series(after!.series);
		for (const [r, round] of series.entries()) {
			for (const [j, matchup] of round.entries()) {
				if (matchup.home && unplayed[r]?.[j]) {
					assert.strictEqual(unplayed[r]![j]!.home.won, matchup.home.won);
					assert.strictEqual(unplayed[r]![j]!.away?.won, matchup.away?.won);
				}
			}
		}
		const afterTeams = await snapshot();
		roundsWonSeen += afterTeams.teamSeasons.filter(
			(ts) =>
				ts.playoffRoundsWon >
				before.teamSeasons.find((x) => x.tid === ts.tid)!.playoffRoundsWon,
		).length;
		setLiveSpoilerProvider(undefined);
		if (roundsWonSeen > 0 && i >= 3) {
			break;
		}
	}
	assert.isAbove(roundsWonSeen, 0, "a series was won on a held day");
}, 600_000);
