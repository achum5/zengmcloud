import { assert, describe, test } from "vitest";
import { scheduleForSim } from "./singleGameSchedule.ts";

// THE INCIDENT: "I was live simming game 5 of my series. I won to make it 3-2.
// When I left the game, the next day's games had already been played, including
// my game 6."
//
// Watching one game asked play.ts to sim exactly that gid. When the gid was no
// longer on the slate - already played - the filter left an empty schedule, and
// an empty schedule during the playoffs is read further down as "the next
// playoff day hasn't been generated yet". So it generated one, re-read the
// schedule WITHOUT the single-game filter, and simulated the entire day: the
// user's game 6 and everyone else's, unasked, for the whole room.
//
// The distinction these tests exist to protect: "no games left today" and "the
// one game you named isn't here" look identical as an empty array and mean
// completely opposite things.

const game = (gid: number) => ({ gid });

describe("scheduleForSim", () => {
	test("a whole-day sim takes the slate as it stands", () => {
		const plan = scheduleForSim([game(1), game(2), game(3)], undefined);
		assert.deepStrictEqual(
			plan.games.map((g) => g.gid),
			[1, 2, 3],
		);
		assert.strictEqual(plan.dayOver, true);
		assert.strictEqual(plan.requestedGameMissing, false);
	});

	test("an empty slate on a whole-day sim is a real new-day signal", () => {
		// This is the case the playoff-day generation downstream exists for, and
		// it must keep working.
		const plan = scheduleForSim([], undefined);
		assert.deepStrictEqual(plan.games, []);
		assert.strictEqual(plan.requestedGameMissing, false);
	});

	test("a single game out of several leaves the day open", () => {
		const plan = scheduleForSim([game(1), game(2), game(3)], 2);
		assert.deepStrictEqual(
			plan.games.map((g) => g.gid),
			[2],
		);
		assert.strictEqual(
			plan.dayOver,
			false,
			"two games are still unplayed, so the day is not over",
		);
		assert.strictEqual(plan.requestedGameMissing, false);
	});

	test("the last game on the slate does end the day", () => {
		const plan = scheduleForSim([game(7)], 7);
		assert.deepStrictEqual(
			plan.games.map((g) => g.gid),
			[7],
		);
		assert.strictEqual(plan.dayOver, true);
		assert.strictEqual(plan.requestedGameMissing, false);
	});

	test("THE BUG: a game already played is missing, not a new day", () => {
		// Game 5 has just been simmed and is off the slate. Whatever asks for it
		// again - a double tap, a remount, a device that reloaded mid-playback -
		// must get "nothing to do", NOT an empty schedule that reads as "generate
		// the next playoff day and sim all of it".
		const plan = scheduleForSim([game(11), game(12)], 5);
		assert.deepStrictEqual(plan.games, []);
		assert.strictEqual(
			plan.requestedGameMissing,
			true,
			"the caller must be able to tell this from an ordinary empty slate",
		);
	});

	test("asking for a game when the slate is empty is also missing", () => {
		const plan = scheduleForSim([], 5);
		assert.deepStrictEqual(plan.games, []);
		assert.strictEqual(
			plan.requestedGameMissing,
			true,
			"an empty slate does not license simming a day nobody asked for",
		);
	});
});
