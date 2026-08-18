import { assert, describe, test } from "vitest";
import {
	canHideBoxScoreTeam,
	orderBoxScoreTeams,
} from "./liveBoxScoreLayout.ts";

// The ordering feeds two things that must agree: the box score sections (whose
// scroll anchors are assigned by position) and the jump-to-team buttons that
// target those anchors. If they ever disagree, a button labeled MIN scrolls to
// CLE - so the rule is pinned here rather than left to two call sites.

const away = { tid: 12, abbrev: "MIN" };
const home = { tid: 5, abbrev: "CLE" };
const game = [away, home];

describe("orderBoxScoreTeams", () => {
	test("hoists the device's team when it is the home team", () => {
		assert.deepStrictEqual(orderBoxScoreTeams(game, 5), [home, away]);
	});

	test("leaves the order alone when the device's team is already first", () => {
		// No pointless reshuffle, and identity is preserved for React keys.
		assert.deepStrictEqual(orderBoxScoreTeams(game, 12), game);
	});

	test("leaves the order alone when this device has no team in the game", () => {
		// Watching a league-mate's broadcast, or a neutral game: there is no
		// "your team" to hoist, so away-over-home stands.
		assert.deepStrictEqual(orderBoxScoreTeams(game, 20), game);
		assert.deepStrictEqual(orderBoxScoreTeams(game, undefined), game);
	});

	test("both teams survive the reorder exactly once", () => {
		const ordered = orderBoxScoreTeams(game, 5);
		assert.strictEqual(ordered.length, 2);
		assert.sameMembers([...ordered], game);
	});

	test("an All-Star game with negative tids is untouched", () => {
		const allStar = [
			{ tid: -1, abbrev: "EAST" },
			{ tid: -2, abbrev: "WEST" },
		];
		assert.deepStrictEqual(orderBoxScoreTeams(allStar, 5), allStar);
	});

	test("anything that is not a two-team game passes through", () => {
		assert.deepStrictEqual(orderBoxScoreTeams([], 5), []);
		assert.deepStrictEqual(orderBoxScoreTeams([away], 12), [away]);
	});
});

describe("canHideBoxScoreTeam", () => {
	const live = { userTid: 5, liveGameInProgress: true };

	test("the opponent can be hidden during a live game", () => {
		assert.isTrue(canHideBoxScoreTeam({ tid: 12, ...live }));
	});

	test("your own team can never be hidden", () => {
		// Hiding the thing you opened the page to watch is not a feature.
		assert.isFalse(canHideBoxScoreTeam({ tid: 5, ...live }));
	});

	test("nothing is hidden once the game is over", () => {
		// A finished box score is a record to read, not a live screen to tidy.
		assert.isFalse(
			canHideBoxScoreTeam({ tid: 12, userTid: 5, liveGameInProgress: false }),
		);
	});

	test("nothing is hidden when this device has no team in the game", () => {
		assert.isFalse(
			canHideBoxScoreTeam({
				tid: 12,
				userTid: undefined,
				liveGameInProgress: true,
			}),
		);
	});
});
