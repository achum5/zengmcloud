import { assert, beforeAll, describe, test } from "vitest";
import { simGameOutcomes } from "./simGameOutcomes.ts";
import { player, team } from "../index.ts";
import { g, helpers } from "../../util/index.ts";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { range } from "../../../common/utils.ts";

const setUpLeague = async () => {
	resetG();
	const teamsDefault = helpers.getTeamsDefault().slice(0, 2);
	await resetCache({
		players: [
			...range(13).map(() =>
				player.generate(0, 25, g.get("season") - 3, true, DEFAULT_LEVEL),
			),
			...range(13).map(() =>
				player.generate(1, 25, g.get("season") - 3, true, DEFAULT_LEVEL),
			),
		],
		teams: teamsDefault.map(team.generate),
		teamSeasons: teamsDefault.map((t) => team.genSeasonRow(t)),
		teamStats: teamsDefault.map((t) => team.genStatsRow(t.tid)),
	});
};

const sim = () =>
	simGameOutcomes({ gid: 0, homeTid: 0, awayTid: 1, neutralSite: false });

describe("simGameOutcomes", () => {
	beforeAll(setUpLeague);

	test("every simulated game contributes one sample to every stat", async () => {
		const result = (await sim())!;
		assert.ok(result, "sim produced nothing");
		assert.ok(result.numSims > 100, `only ${result.numSims} sims`);

		for (const t of result.teams) {
			assert.strictEqual(t.samples.pts.length, result.numSims);
			assert.strictEqual(t.samples.trb.length, result.numSims);
			assert.ok(t.players.length > 0);
			for (const p of t.players) {
				assert.strictEqual(
					p.samples.pts.length,
					result.numSims,
					`${p.name} has ${p.samples.pts.length} points samples`,
				);
				assert.strictEqual(p.samples.min.length, result.numSims);
				// Counted per simulated game, so this can never exceed the number of
				// games, and a triple-double is always also a double-double.
				assert.ok(p.dd <= result.numSims);
				assert.ok(p.td <= p.dd, `${p.name}: ${p.td} tds but ${p.dd} dds`);
			}
		}
	});

	test("the simulated box scores are a real game, not noise", async () => {
		const result = (await sim())!;

		for (const t of result.teams) {
			const mean =
				t.samples.pts.reduce((sum, x) => sum + x, 0) / t.samples.pts.length;
			assert.ok(mean > 50 && mean < 200, `team averaged ${mean} points`);

			// A team's points are its players' points.
			const playerTotal = t.players.reduce(
				(sum, p) =>
					sum + p.samples.pts.reduce((s, x) => s + x, 0) / p.samples.pts.length,
				0,
			);
			assert.ok(
				Math.abs(playerTotal - mean) < 1,
				`players scored ${playerTotal} of the team's ${mean}`,
			);
		}

		// Overtime is uncommon but not unheard of.
		assert.ok(result.overtimes >= 0);
		assert.ok(result.overtimes < result.numSims / 2);
	});

	test("the same league state always produces the same board", async () => {
		// Prop bets are validated by re-deriving this board server-side, so odds
		// that drifted between the quote and the bet would bounce every honest
		// wager. The engine draws from Math.random, so the batch has to seed it.
		const a = (await sim())!;
		const b = (await sim())!;
		assert.deepStrictEqual(a.teams[0].samples.pts, b.teams[0].samples.pts);
		assert.deepStrictEqual(a.teams[1].samples.trb, b.teams[1].samples.trb);
		assert.strictEqual(a.overtimes, b.overtimes);
	});

	test("the real Math.random is restored afterward", async () => {
		const before = Math.random;
		await sim();
		assert.strictEqual(Math.random, before);
	});

	test("an injury reprices the board instead of serving a stale one", async () => {
		// This is the thing a season-average prop model could not do: a player who
		// is out tonight has the same season averages he had yesterday, so his line
		// stayed up. Here he simply doesn't get minutes, and the board is
		// recomputed because the fingerprint it's cached against includes injuries.
		const mean = (samples: number[]) =>
			samples.reduce((sum, x) => sum + x, 0) / samples.length;

		const before = (await sim())!;
		const { idb } = await import("../../db/index.ts");
		const starter = before.teams[0].players.reduce((a, b) =>
			mean(a.samples.min) > mean(b.samples.min) ? a : b,
		);
		const minutesBefore = mean(starter.samples.min);
		assert.ok(
			minutesBefore > 5,
			`only played ${minutesBefore} minutes healthy`,
		);
		const best = (await idb.cache.players.get(starter.pid))!;

		const injuryBefore = best.injury;
		best.injury = { type: "Sprained ankle", gamesRemaining: 30 };
		await idb.cache.players.put(best);

		try {
			const after = (await sim())!;
			assert.ok(
				after.teams[0].samples.pts.some(
					(x, i) => x !== before.teams[0].samples.pts[i],
				),
				"injury did not invalidate the cached board",
			);
			const row = after.teams[0].players.find((p) => p.pid === best.pid)!;
			assert.ok(
				mean(row.samples.min) < 1,
				`injured player still played ${mean(row.samples.min)} minutes`,
			);
			assert.strictEqual(mean(row.samples.pts), 0);
		} finally {
			best.injury = injuryBefore;
			await idb.cache.players.put(best);
		}
	});
});
