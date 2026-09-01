import { assert, describe, test } from "vitest";
import { championshipStakes } from "./liveGame.ts";

// ---------------------------------------------------------------------------
// THE TROPHY THAT WENT MISSING ON REPLAY.
//
// The live-game court draws a championship trophy at center court during a
// finals game, and throws confetti when the series ends on it. Both were
// worked out from the league as it stood AT THE MOMENT THE PAGE LOADED - the
// current phase, the current season's playoffSeries, whether the current round
// was the last one. That is the same answer as "was this game a finals game"
// only while the game is being played for the first time.
//
// Watch the same game back afterwards and none of it holds: the phase has
// moved on, the playoff series is a different season's, the round is over. So
// a finals rewatch drew an ordinary court. Reported from a live league.
//
// The game record knew all along - writeGameStats stamps `finals` on a
// final-round game and stores each side's series record as it stood after it -
// so the answer now comes from the game being watched and nothing else, which
// makes replay and live agree by construction.
// ---------------------------------------------------------------------------

const game = ({
	finals,
	numGamesToWinSeries,
	won = [0, 0],
}: {
	finals?: boolean;
	numGamesToWinSeries?: number;
	won?: [number, number];
}) => ({
	finals,
	numGamesToWinSeries,
	teams: [{ playoffs: { won: won[0] } }, { playoffs: { won: won[1] } }] as [
		{ playoffs?: { won: number } },
		{ playoffs?: { won: number } },
	],
});

describe("championshipStakes", () => {
	test("a finals game is a finals game, whenever it is watched", () => {
		assert.deepStrictEqual(
			championshipStakes(
				game({ finals: true, numGamesToWinSeries: 4, won: [2, 1] }),
			),
			{ finals: true, confetti: false },
		);
	});

	test("the game that ends the series brings the confetti", () => {
		assert.deepStrictEqual(
			championshipStakes(
				game({ finals: true, numGamesToWinSeries: 4, won: [4, 2] }),
			),
			{ finals: true, confetti: true },
		);
		// Either side can be the one that got there.
		assert.isTrue(
			championshipStakes(
				game({ finals: true, numGamesToWinSeries: 4, won: [2, 4] }),
			).confetti,
		);
	});

	test("a one-game final is decided the moment it is played", () => {
		assert.isTrue(
			championshipStakes(
				game({ finals: true, numGamesToWinSeries: 1, won: [1, 0] }),
			).confetti,
		);
	});

	// Winning a semi-final is not winning a championship, and the record says
	// which one this was.
	test("an ordinary playoff game gets neither, however the series stood", () => {
		assert.deepStrictEqual(
			championshipStakes(game({ numGamesToWinSeries: 4, won: [4, 1] })),
			{ finals: false, confetti: false },
		);
	});

	test("a regular-season game gets neither", () => {
		assert.deepStrictEqual(championshipStakes(game({})), {
			finals: false,
			confetti: false,
		});
	});

	// An old or imported game with no series length recorded still shows the
	// trophy; it just cannot know whether this was the clincher.
	test("a finals game with no series length still gets its trophy", () => {
		assert.deepStrictEqual(championshipStakes(game({ finals: true })), {
			finals: true,
			confetti: false,
		});
	});
});
