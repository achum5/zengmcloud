import { assert, beforeAll, test } from "vitest";
import { resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import {
	finishedComponent,
	hypeRaw,
	loyaltyRaw,
	teamPerformanceRaw,
} from "../player/moodComponents.ts";
import {
	hypeAfter,
	willingAtExpiry,
	type MoodNow,
	type SeasonsAhead,
} from "./resignForecast.ts";

// THE RE-SIGNING FORECAST: his mood when his contract runs out, from the
// team's record and hype then.

beforeAll(() => {
	resetG();
	Object.assign(g, { userTids: [0], spectator: false, playerMoodTraits: true });
});

const CPU = 5;
const sigmoid = (x: number) => 1 / (1 + Math.exp(-0.7 * x));

// A player on a CPU team that went 30% this season with hype 0.1: his mood
// as the game counts it now.
const moodOn = (winp: number, hype: number, seasons: number, traits = []) => {
	const p = { moodTraits: traits };
	const teamPerformance = finishedComponent(
		"teamPerformance",
		teamPerformanceRaw(winp),
		p,
		CPU,
	);
	const hypeNow = finishedComponent("hype", hypeRaw(hype), p, CPU);
	const loyalty = finishedComponent(
		"loyalty",
		loyaltyRaw(seasons, true),
		p,
		CPU,
	);
	// A rookie deal's eagerness, and the rest about even.
	const rest = 8 / 1.5 - 0.5;
	return {
		sum: rest + teamPerformance + hypeNow + loyalty,
		teamPerformance,
		hype: hypeNow,
		loyalty,
		seasons,
		own: true,
		sure: false,
		moodTraits: traits,
	} satisfies MoodNow;
};

const seasons = (winp: number[], spread = 0, hypeNow = 0.1): SeasonsAhead => ({
	winp,
	spread: winp.map(() => spread),
	games: winp.map(() => 77),
	hypeNow,
	past: [0.3, 0.3],
});

test("forecast for this season's end with nothing changing is his mood now", () => {
	const mood = moodOn(0.3, 0.1, 3);
	// No games left to move hype or the record, no seasons added.
	const ahead = { ...seasons([0.3]), games: [0] };
	assert.closeTo(
		willingAtExpiry(mood, CPU, 0, ahead, 0),
		sigmoid(mood.sum),
		1e-9,
	);
});

test("a team expected to be better when his deal runs out keeps him more often", () => {
	const mood = moodOn(0.3, 0.1, 1);
	const stuck = willingAtExpiry(mood, CPU, 2, seasons([0.3, 0.3, 0.3]), 2);
	const better = willingAtExpiry(mood, CPU, 2, seasons([0.4, 0.5, 0.55]), 2);
	assert.isAbove(better, stuck + 0.2);
	// Stuck losing, his mood is no better than now save his seasons there.
	assert.isBelow(stuck, sigmoid(mood.sum) + 0.15);
});

test("a winner (W) cares more about the record", () => {
	const plain = moodOn(0.3, 0.1, 1);
	const winner = moodOn(0.3, 0.1, 1, ["W"] as any);
	const ahead = seasons([0.4, 0.5, 0.6]);
	const gainPlain =
		willingAtExpiry(plain, CPU, 2, ahead, 2) - sigmoid(plain.sum);
	const gainWinner =
		willingAtExpiry(winner, CPU, 2, ahead, 2) - sigmoid(winner.sum);
	assert.isAbove(gainWinner, gainPlain);
});

test("doubt about the record pulls the forecast toward the middle", () => {
	const mood = moodOn(0.3, 0.1, 1);
	const sure = willingAtExpiry(mood, CPU, 1, seasons([0.3, 0.25]), 1);
	const unsure = willingAtExpiry(mood, CPU, 1, seasons([0.3, 0.25], 0.15), 1);
	assert.isAbove(unsure, sure);
});

test("league rules that settle it settle the forecast", () => {
	const mood = { ...moodOn(0.2, 0, 0), sure: true };
	assert.strictEqual(willingAtExpiry(mood, CPU, 1, seasons([0.2, 0.2]), 1), 1);
});

test("hype after a season follows the game's rule", () => {
	// A winning season above the ones before builds hype; a losing one at
	// the bottom keeps it at none.
	assert.isAbove(hypeAfter(0.4, 0.6, 0.5, 77), 0.4);
	assert.strictEqual(hypeAfter(0.05, 0.25, 0.25, 77), 0);
	assert.closeTo(hypeAfter(0.5, 0.55, 0.55, 77), 0.5, 1e-12);
});

test("making the playoffs adds the game's hype for it", () => {
	const without = hypeAfter(0.5, 0.6, 0.6, 77);
	assert.closeTo(hypeAfter(0.5, 0.6, 0.6, 77, 0.48), without + 0.05, 1e-4);
	// Far below the line, next to nothing.
	assert.closeTo(hypeAfter(0.5, 0.3, 0.3, 0, 0.48), 0.5, 1e-4);
});
