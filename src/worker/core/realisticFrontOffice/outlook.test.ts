import { assert, beforeAll, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import {
	colaPaths,
	losingShifts,
	outlookSettings,
	resultsEdges,
	resultsWeight,
	retireChance,
} from "./outlook.ts";

beforeAll(() => {
	resetG();
});

test("nobody on a roster retires before 34", () => {
	for (let age = 20; age <= 33; age++) {
		assert.strictEqual(retireChance(age, 40, 40, 60), 0);
	}
	assert.isAbove(retireChance(34, 40, 40, 45), 0);
});

test("the better the old player, the likelier he keeps playing", () => {
	const scrub = retireChance(36, 44, 44, 50);
	const starter = retireChance(36, 58, 58, 64);
	assert.isAbove(scrub, 0.1);
	assert.isBelow(starter, 0.02);
	assert.isAbove(retireChance(38, 44, 44, 50), scrub);
});

test("a star far past his best may hang it up", () => {
	assert.isAbove(retireChance(35, 58, 48, 72), 0.2);
	assert.isBelow(retireChance(35, 58, 58, 72), 0.02);
});

test("tearing a team down costs it its players' mood", () => {
	const flat = Array.from({ length: 11 }, () => 0);
	const outlook = {
		basePoints: new Map([[0, flat]]),
		meanPoints: flat,
		hype: new Map([[0, 0.5]]),
		records: new Map([[0, { won: 0, lost: 0 }]]),
		remaining: new Map([[0, 0]]),
		progress: 0,
	};
	// Nothing changed: nothing to price again.
	assert.isUndefined(losingShifts(outlook, 0, flat));
	// Five points worse every season: worse moods - most this season and
	// next, less seasons out as the finish drifts back.
	const worse = losingShifts(
		outlook,
		0,
		flat.map(() => -5),
	)!;
	assert.isBelow(worse.mood[0]!, -2);
	assert.isAbove(worse.mood[6]!, worse.mood[0]!);
	// Better is worth less than worse costs: losing counts twice.
	const better = losingShifts(
		outlook,
		0,
		flat.map(() => 5),
	)!;
	assert.isBelow(better.mood[0]!, -worse.mood[0]!);
	// Hype and room count only when asked to (losingParts): measured, they
	// made teams worse.
	assert.strictEqual(worse.winp[1] ?? 0, 0);
	const was = outlookSettings.losingParts;
	outlookSettings.losingParts = { record: true, hype: true, room: true };
	try {
		const all = losingShifts(
			outlook,
			0,
			flat.map(() => -5),
		)!;
		assert.isBelow(all.winp[1]!, -0.1);
		assert.isBelow(all.mood[0]!, worse.mood[0]!);
	} finally {
		outlookSettings.losingParts = was;
	}
});

test("under COLA, a team that goes deep each year keeps few chances at the draw", () => {
	// Four playoff rounds: index 0 out of the playoffs, then 0-4 series won.
	const contender = [0, 0.1, 0.3, 0.3, 0.2, 0.1];
	const lottery = [0.9, 0.1, 0, 0, 0, 0];
	const middle = [0.4, 0.4, 0.2, 0, 0, 0];
	const tids = [0, 1, 2, 3];
	const finishes = new Map(
		tids.map((tid) => [
			tid,
			Array.from({ length: 11 }, () =>
				tid === 0 ? contender : tid === 1 ? lottery : middle,
			),
		]),
	);
	const path = (holds: Set<string>) =>
		colaPaths({
			cola: {
				points: new Map(tids.map((tid) => [tid, 2000])),
				meanPoints: 2000,
				addNow: true,
				numLottery: 3,
				numPicks: 2,
				alpha: 1000,
				matrices: new Map(),
			},
			finishes,
			holds,
			season: 2030,
			first: 2030,
			lastDraft: 2033,
			numPlayoffRounds: 4,
		});
	const none = path(new Set());
	// Level this season, as everyone adds the same before the playoffs...
	assert.strictEqual(none.get(0)!.get(2030), none.get(1)!.get(2030));
	// ...then the contender's runs cost it and the lottery team builds up.
	assert.isBelow(none.get(0)!.get(2032)!, 0.5);
	assert.isAbove(none.get(1)!.get(2032)!, none.get(2)!.get(2032)!);
	assert.isAbove(none.get(2)!.get(2032)!, none.get(0)!.get(2032)!);
	// Winning the draw with its own pick costs a team chances later.
	const own = path(new Set(["1:2030", "1:2031"]));
	assert.isBelow(own.get(1)!.get(2032)!, none.get(1)!.get(2032)!);
});

test("a team that has played better than its read is read better, for as much of it as isn't luck", async () => {
	const season = g.get("season");
	// Team 0 is read +5 and has played 26 games against team 1 (read -5) and
	// 26 against team 2 (read 0), half of them at home: the reads expected
	// +7.5 a game of it, and it won by 10.
	const reads = new Map([
		[0, 5],
		[1, -5],
		[2, 0],
	]);
	const pair = (won: number, pts: number, oppPts: number) => ({
		won,
		lost: 26 - won,
		tied: 0,
		otl: 0,
		otw: 0,
		pts,
		oppPts,
	});
	const record = (tid: number, home: number) => ({
		tid,
		season,
		won: 26,
		lost: 26,
		tied: 0,
		otl: 0,
		wonHome: home / 2,
		lostHome: home / 2,
		tiedHome: 0,
		otlHome: 0,
		wonAway: (52 - home) / 2,
		lostAway: (52 - home) / 2,
		tiedAway: 0,
		otlAway: 0,
	});
	const stats = (tid: number, pts: number, oppPts: number) => ({
		tid,
		season,
		playoffs: false,
		gp: 52,
		pts,
		oppPts,
	});
	await resetCache({
		teamStats: [
			stats(0, 52 * 110, 52 * 100),
			stats(1, 52 * 100, 52 * 106),
			stats(2, 52 * 100, 52 * 104),
		],
		// Team 2 played 34 of its games at home.
		teamSeasons: [record(0, 26), record(1, 26), record(2, 34)],
	});
	await idb.cache.headToHeads.put({
		season,
		regularSeason: {
			0: {
				1: pair(20, 26 * 112, 26 * 100),
				2: pair(16, 26 * 108, 26 * 100),
			},
			1: { 2: pair(13, 26 * 100, 26 * 100) },
		},
		playoffs: {},
	});
	const edges = await resultsEdges(season, [0, 1, 2], (tid) => reads.get(tid)!);
	const w = resultsWeight(52);
	assert.isAbove(w, 0);
	assert.isBelow(w, 0.5);
	// Its weak schedule counts: 2.5 a game over what was expected, not 5.
	assert.closeTo(edges.get(0)!, w * 2.5, 1e-9);
	// Team 1 (-6 against -5 - 2.5 = -7.5 expected) has played better than
	// read; team 2's home games were worth some of its margin.
	assert.closeTo(edges.get(1)!, w * 1.5, 1e-9);
	const homeEdge = edges.get(2)!;
	assert.isBelow(homeEdge, w * (-4 - 0));
	// The more games, the more of the margin is the team's.
	assert.isBelow(resultsWeight(10), resultsWeight(52));
	assert.isBelow(resultsWeight(52), resultsWeight(82));
	assert.strictEqual(resultsWeight(0), 0);
});
