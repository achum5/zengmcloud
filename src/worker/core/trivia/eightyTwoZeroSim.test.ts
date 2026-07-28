import { assert, beforeAll, describe, test } from "vitest";
import { simulateEightyTwoZeroSeason } from "./eightyTwoZeroSim.ts";
import { player, team } from "../index.ts";
import { g, helpers } from "../../util/index.ts";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { range } from "../../../common/utils.ts";
import { idb } from "../../db/index.ts";

const NUM_TEAMS = 4;

let starPids: number[] = [];

beforeAll(async () => {
	resetG();
	const teamsDefault = helpers.getTeamsDefault().slice(0, NUM_TEAMS);
	g.setWithoutSavingToDB("numTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("numActiveTeams", NUM_TEAMS);
	g.setWithoutSavingToDB(
		"teamInfoCache",
		teamsDefault.map((t) => ({
			abbrev: t.abbrev,
			disabled: false,
			imgURL: t.imgURL,
			imgURLSmall: t.imgURLSmall,
			name: t.name,
			region: t.region,
		})),
	);

	// A generated player is not a league player yet: develop() fills in ovr/pot,
	// updateValues() fills in the value the engine ranks its rotation by, and
	// rosterOrder decides who starts. Without all three the opposition fields a
	// random five and loses by sixty, which says nothing about the game.
	const players = [];
	for (const [tid] of teamsDefault.entries()) {
		const squad = [];
		for (const _ of range(12)) {
			const p = player.generate(
				tid,
				25,
				g.get("season") - 5,
				false,
				DEFAULT_LEVEL,
			);
			await player.develop(p, 0);
			p.ratings[0]!.season = g.get("season");
			// Stand-in for updateValues, which needs a cache that doesn't exist yet
			// at this point. The engine only ever compares these, so the scale
			// doesn't matter - the ordering does.
			p.value = p.ratings[0]!.ovr;
			p.valueNoPot = p.ratings[0]!.ovr;
			squad.push(p);
		}
		squad.sort((a, b) => b.valueNoPot - a.valueNoPot);
		for (const [i, p] of squad.entries()) {
			p.rosterOrder = i;
		}
		players.push(...squad);
	}
	await resetCache({
		players,
		teams: teamsDefault.map(team.generate),
		teamSeasons: teamsDefault.map((t) => team.genSeasonRow(t)),
		teamStats: teamsDefault.map((t) => team.genStatsRow(t.tid)),
	});

	// The five best players in the league, which is the closest a test league
	// gets to an all-time lineup.
	const all = await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	]);
	starPids = all
		.map((p) => ({
			pid: p.pid,
			ovr: p.ratings.find((r) => r.season === g.get("season"))?.ovr ?? 0,
		}))
		.sort((a, b) => b.ovr - a.ovr)
		.slice(0, 5)
		.map((row) => row.pid);
});

const picks = () => starPids.map((pid) => ({ pid, season: g.get("season") }));

describe("simulateEightyTwoZeroSeason", () => {
	test("plays a whole season and the record adds up", async () => {
		const result = (await simulateEightyTwoZeroSeason(picks()))!;
		assert.ok(result, "no result");
		assert.strictEqual(result.won + result.lost, 82);
		assert.ok(result.ptsFor > 50 && result.ptsFor < 200, `${result.ptsFor}`);
		assert.ok(result.ptsAgainst > 50 && result.ptsAgainst < 200);
	});

	test("every drafted player gets a stat line, and only them", async () => {
		const result = (await simulateEightyTwoZeroSeason(picks()))!;
		assert.strictEqual(result.players.length, 5);
		assert.deepStrictEqual(
			result.players.map((p) => p.pid),
			starPids,
		);
		for (const line of result.players) {
			assert.ok(line.gp > 0, `${line.name} never played`);
			assert.ok(line.min / line.gp > 5, `${line.name}: ${line.min / line.gp}m`);
			assert.ok(line.pts > 0);
		}
	});

	// Five men can't play 82 games, so the engine gets a bench. If the bench
	// weren't there the five would run 48 minutes a night and fatigue would
	// decide the season instead of the picks.
	test("the drafted five play starter minutes, not all forty-eight", async () => {
		const result = (await simulateEightyTwoZeroSeason(picks()))!;
		for (const line of result.players) {
			const mpg = line.min / line.gp;
			assert.ok(mpg < 46, `${line.name} played ${mpg} minutes a night`);
		}
	});

	test("the same five always play the same season", async () => {
		const a = (await simulateEightyTwoZeroSeason(picks()))!;
		const b = (await simulateEightyTwoZeroSeason(picks()))!;
		assert.strictEqual(a.won, b.won);
		assert.strictEqual(a.ptsFor, b.ptsFor);
		assert.deepStrictEqual(
			a.players.map((p) => p.pts),
			b.players.map((p) => p.pts),
		);
	});

	test("a better five wins more than a worse five", async () => {
		const all = await idb.cache.players.indexGetAll("playersByTid", [
			0,
			Infinity,
		]);
		const byOvr = all
			.map((p) => ({
				pid: p.pid,
				ovr: p.ratings.find((r) => r.season === g.get("season"))?.ovr ?? 0,
			}))
			.sort((a, b) => b.ovr - a.ovr);
		const worst = byOvr
			.slice(-5)
			.map((row) => ({ pid: row.pid, season: g.get("season") }));

		const good = (await simulateEightyTwoZeroSeason(picks()))!;
		const bad = (await simulateEightyTwoZeroSeason(worst))!;
		assert.ok(
			good.won > bad.won,
			`best five won ${good.won}, worst five won ${bad.won}`,
		);
	});

	test("the real Math.random is restored afterward", async () => {
		const before = Math.random;
		await simulateEightyTwoZeroSeason(picks());
		assert.strictEqual(Math.random, before);
	});

	test("no picks, no season", async () => {
		assert.strictEqual(await simulateEightyTwoZeroSeason([]), undefined);
	});
});

describe("82-0 leaves the league alone", () => {
	// It's a game about your file, not a change to it. The season it plays runs
	// on copies: no roster moves, no stats written, no injuries carried over,
	// nobody's age or ratings edited. The picks are rebuilt as the players they
	// were that year, and that rebuilding must not touch the stored player.
	test("nothing in the database moves", async () => {
		const before = JSON.stringify(
			await idb.cache.players.indexGetAll("playersByTid", [0, Infinity]),
		);
		const beforeTeams = JSON.stringify(await idb.cache.teams.getAll());
		const beforeSeasons = JSON.stringify(await idb.cache.teamSeasons.getAll());

		const result = await simulateEightyTwoZeroSeason(picks());
		assert.ok(result, "the season didn't run, so this proves nothing");

		assert.strictEqual(
			JSON.stringify(
				await idb.cache.players.indexGetAll("playersByTid", [0, Infinity]),
			),
			before,
			"a player was modified",
		);
		assert.strictEqual(
			JSON.stringify(await idb.cache.teams.getAll()),
			beforeTeams,
			"a team was modified",
		);
		assert.strictEqual(
			JSON.stringify(await idb.cache.teamSeasons.getAll()),
			beforeSeasons,
			"a team season was modified",
		);
	});
});
