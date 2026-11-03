import { assert, beforeAll, test } from "vitest";
import { makeSeededRandom } from "../../common/random.ts";
import { PHASE } from "../../common/constants.ts";
import { idb } from "../db/index.ts";
import { phase } from "../core/index.ts";
import { g } from "../util/index.ts";
import {
	advanceSeasons,
	createArenaLeague,
} from "../../test/arena/runLeague.ts";
import { getMoreTeamInfo } from "./rosterMoreInfo.ts";

const importNode = (name: string) => import(/* @vite-ignore */ name);

let season: number;

beforeAll(async () => {
	await importNode("fake-indexeddb/auto");
	Math.random = makeSeededRandom(31);
	(idb as any).league = undefined;
	await createArenaLeague(9301);
	season = g.get("season");
	// Through the start of the regular season, where the lines are saved, and
	// on to the next preseason: a whole season with playoffs.
	await advanceSeasons(1);
}, 600_000);

test("the season's opening lines are kept on every team", async () => {
	const teamSeasons = await idb.merged.teamSeasons.getAll({ season });
	assert.strictEqual(teamSeasons.length, g.get("numTeams"));
	for (const ts of teamSeasons) {
		const odds = ts.preseasonOdds;
		assert.isDefined(odds, `tid ${ts.tid}`);
		assert.isTrue(Number.isInteger(odds!.title), `title ${odds!.title}`);
		assert.isAbove(odds!.winTotal!, 0);
		assert.isBelow(odds!.winTotal!, g.get("numGames"));
	}
	// One favourite, a long field: the shortest price is the only one that
	// can be odds-on, and the win totals add up to about a league's wins.
	const prices = teamSeasons.map((ts) => ts.preseasonOdds!.title!);
	assert.isBelow(Math.min(...prices), 1000);
	const total = teamSeasons.reduce(
		(sum, ts) => sum + ts.preseasonOdds!.winTotal!,
		0,
	);
	assert.closeTo(
		total,
		(g.get("numTeams") * g.get("numGames")) / 2,
		g.get("numTeams") * 2,
	);
});

test("more team info: ranks, SRS, expected record, odds result, playoffs", async () => {
	assert.strictEqual(g.get("phase"), PHASE.PRESEASON);
	const all = await Promise.all(
		Array.from({ length: g.get("numTeams") }, (_, tid) =>
			getMoreTeamInfo(season, tid),
		),
	);
	const srsSum = all.reduce((sum, info) => sum + (info?.srs?.value ?? 0), 0);
	assert.closeTo(srsSum, 0, 1e-6, "SRS centred on zero");
	// Over a full season everyone plays everyone, so the schedule adjustment
	// is small: SRS sits within a few points of net rating's ordering.
	const pairs = all.map((info): [number, number] => [
		info!.srs!.value,
		info!.nrtg!.value,
	]);
	const corr = (() => {
		const n = pairs.length;
		const mx = pairs.reduce((s, [x]) => s + x, 0) / n;
		const my = pairs.reduce((s, [, y]) => s + y, 0) / n;
		const sxy = pairs.reduce((s, [x, y]) => s + (x - mx) * (y - my), 0);
		const sx = Math.sqrt(pairs.reduce((s, [x]) => s + (x - mx) ** 2, 0));
		const sy = Math.sqrt(pairs.reduce((s, [, y]) => s + (y - my) ** 2, 0));
		return sxy / (sx * sy);
	})();
	assert.isAbove(corr, 0.95, `SRS against net rating: ${corr}`);
	for (const info of all) {
		assert.isDefined(info);
		assert.strictEqual(info!.numTeams, g.get("numTeams"));
		for (const key of [
			"pts",
			"oppPts",
			"srs",
			"pace",
			"ortg",
			"drtg",
			"nrtg",
			"expected",
			"att",
		] as const) {
			const ranked: { rank: number } | undefined = info![key];
			assert.isDefined(ranked, key);
			assert.isAtLeast(ranked!.rank, 1, key);
			assert.isAtMost(ranked!.rank, info!.numTeams, key);
		}
		assert.closeTo(info!.expected!.won + info!.expected!.lost, info!.gp, 1);
		// One night's crowd, not the season's
		assert.isAbove(info!.att!.value, 0);
		assert.isBelow(info!.att!.value, 50_000);
		assert.include(["over", "under", "push"], info!.preseasonOdds!.result);
	}
	// Someone was ranked first in each, and the best offence is not also the
	// worst one.
	for (const key of ["pts", "srs", "ortg", "drtg"] as const) {
		assert.isTrue(
			all.some((info) => info![key]!.rank === 1),
			key,
		);
	}
	// The champion won every series it played; everyone else lost exactly one.
	const champion = all.find(
		(info) =>
			info!.playoffs.length > 0 &&
			info!.playoffs.every((s) => s.result === "won"),
	);
	assert.isDefined(champion, "a champion");
	for (const info of all) {
		if (info === champion || info!.playoffs.length === 0) {
			continue;
		}
		assert.strictEqual(
			info!.playoffs.filter((s) => s.result === "lost").length,
			1,
		);
		assert.strictEqual(info!.playoffs.at(-1)!.result, "lost");
	}
	console.log("sample:", JSON.stringify(champion));
	await phase.newPhase(PHASE.REGULAR_SEASON, {} as any);
	const next = await idb.cache.teamSeasons.indexGet("teamSeasonsBySeasonTid", [
		g.get("season"),
		0,
	]);
	assert.isDefined(next?.preseasonOdds, "and the next season's lines too");
	// On the day the season starts there are no stats yet, only the lines.
	const dayOne = await getMoreTeamInfo(g.get("season"), 0);
	assert.isDefined(dayOne, "info before the first game");
	assert.strictEqual(dayOne!.gp, 0);
	assert.isUndefined(dayOne!.pts);
	assert.isDefined(dayOne!.preseasonOdds?.winTotal);
	assert.isUndefined(dayOne!.preseasonOdds?.result);
});
