import { assert, describe, test } from "vitest";
import type { Game } from "../../common/types.ts";
import {
	getTeamSpreadBias,
	spreadBiasAdjustment,
	spreadBiasPriorGames,
	MAX_SPREAD_BIAS,
} from "./getTeamSpreadBias.ts";

// home beat away by `margin`, on a line of `model`.
const game = (
	homeTid: number,
	awayTid: number,
	margin: number,
	model: number,
	extra: Partial<Game> = {},
): Game =>
	({
		gid: 1,
		season: 2026,
		teams: [
			{ tid: homeTid, pts: 100 + margin, ovr: 50 },
			{ tid: awayTid, pts: 100, ovr: 50 },
		],
		spread: model,
		...extra,
	}) as any;

const biasOf = async (games: Game[], tid: number) =>
	(await getTeamSpreadBias(2026, games)).get(tid)?.bias ?? 0;

describe("per-team spread bias", () => {
	test("no games, no opinion", async () => {
		assert.strictEqual((await getTeamSpreadBias(2026, [])).size, 0);
	});

	test("a team that keeps beating its number gets priced up", async () => {
		// Ten games, each won by 10 more than the line said.
		const games = Array.from({ length: 10 }, () => game(0, 1, 10, 0));
		const bias = await biasOf(games, 0);
		assert.isAbove(bias, 0);
		// Shrunk toward nothing: ten games of a +10 run is worth 10*10/(10+prior).
		const expected = 100 / (10 + spreadBiasPriorGames());
		assert.approximately(bias, expected, 1e-9);
	});

	test("what one side gains the other loses", async () => {
		const games = Array.from({ length: 6 }, () => game(0, 1, 8, 0));
		assert.approximately(
			await biasOf(games, 0),
			-(await biasOf(games, 1)),
			1e-9,
		);
	});

	test("a team that plays exactly to its number is left alone", async () => {
		const games = Array.from({ length: 20 }, () => game(0, 1, -4, -4));
		assert.approximately(await biasOf(games, 0), 0, 1e-9);
	});

	test("more evidence moves the number further", async () => {
		const few = Array.from({ length: 5 }, () => game(0, 1, 10, 0));
		const many = Array.from({ length: 40 }, () => game(0, 1, 10, 0));
		assert.isAbove(await biasOf(many, 0), await biasOf(few, 0));
		// ...but never past what was actually observed.
		assert.isBelow(await biasOf(many, 0), 10);
	});

	// The whole reason Game.spreadModel exists. If the correction were measured
	// against the line it had already moved, it would find no bias left and
	// collapse to zero - the estimator would be chasing its own tail.
	test("measured against the model's number, not the quoted one", async () => {
		// Quoted -3 (model -1, plus a +2 correction already applied); the home
		// side won by exactly the quoted line, so it beat the MODEL by 2.
		const games = Array.from({ length: 10 }, () =>
			game(0, 1, 3, -3, { spreadModel: -1 } as Partial<Game>),
		);
		const bias = await biasOf(games, 0);
		const expected = (10 * 4) / (10 + spreadBiasPriorGames());
		assert.approximately(bias, expected, 1e-9);
	});

	test("a game with no stored line teaches nothing", async () => {
		const games = [
			game(0, 1, 30, 0),
			{ ...game(0, 1, 30, 0), spread: undefined },
		];
		const bias = await biasOf(games as Game[], 0);
		assert.approximately(bias, 30 / (1 + spreadBiasPriorGames()), 1e-9);
	});

	test("All-Star and other special games are skipped", async () => {
		const games = [game(-1, -2, 40, 0), game(-3, -3, 40, 0)];
		assert.strictEqual((await getTeamSpreadBias(2026, games)).size, 0);
	});

	test("a freak run cannot produce an absurd line", async () => {
		const games = Array.from({ length: 200 }, () => game(0, 1, 60, 0));
		assert.approximately(await biasOf(games, 0), MAX_SPREAD_BIAS, 1e-9);
		assert.approximately(await biasOf(games, 1), -MAX_SPREAD_BIAS, 1e-9);
	});
});

describe("applying the correction", () => {
	const biases = new Map([
		[0, { bias: 1.5, games: 40 }],
		[1, { bias: -0.5, games: 40 }],
	]);

	test("home gains its own, away gives back its own", () => {
		assert.strictEqual(spreadBiasAdjustment(biases, 0, 1), 2);
		assert.strictEqual(spreadBiasAdjustment(biases, 1, 0), -2);
	});

	test("a team nobody has measured moves nothing", () => {
		assert.strictEqual(spreadBiasAdjustment(biases, 0, 99), 1.5);
		assert.strictEqual(spreadBiasAdjustment(undefined, 0, 1), 0);
	});
});
