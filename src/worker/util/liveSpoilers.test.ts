import { assert, describe, test } from "vitest";
import { hideLiveResults, mentionsHiddenGame } from "./liveSpoilers.ts";

const game = (
	gid: number,
	pts?: [number, number],
	playoffs?: [number, number],
) => ({
	gid,
	overtimes: pts ? 1 : 0,
	teams: [0, 1].map((i) => ({
		tid: i,
		ovr: 50,
		pts: pts?.[i],
		...(playoffs
			? {
					playoffs: {
						seed: i + 1,
						won: playoffs[i]!,
						lost: playoffs[1 - i]!,
					},
				}
			: {}),
	})) as any,
});

describe("hideLiveResults", () => {
	test("a game playing out live reads as not played, and nothing after it shows", () => {
		const games = [
			game(1, [100, 90], [1, 0]),
			// The live one: the home side won, so its series is 2-0 after.
			game(2, [101, 99], [2, 0]),
			// The next game's record would give it away too.
			game(3, undefined, [2, 0]),
		];
		const out = hideLiveResults(games, new Set([2]));
		assert.strictEqual(out.length, 2);
		assert.deepStrictEqual(out[0], games[0]);
		const live = out[1]!;
		assert.strictEqual(live.gid, 2);
		assert.strictEqual(live.overtimes, 0);
		assert.strictEqual(live.teams[0].pts, undefined);
		assert.strictEqual(live.teams[1].pts, undefined);
		// The series as it stood going in.
		assert.strictEqual(live.teams[0].playoffs!.won, 1);
		assert.strictEqual(live.teams[1].playoffs!.lost, 1);
		assert.strictEqual(live.teams[1].playoffs!.won, 0);
		assert.strictEqual(live.teams[0].playoffs!.lost, 0);
	});

	test("nothing hidden, nothing changed", () => {
		const games = [game(1, [100, 90]), game(2)];
		assert.strictEqual(hideLiveResults(games, new Set()), games);
		assert.deepStrictEqual(hideLiveResults(games, new Set([7])), games);
	});
});

describe("mentionsHiddenGame", () => {
	const story = (gid: number) =>
		`The <a href="/l/23/roster/CLE/2020">Cavaliers</a> defeated the <a href="/l/23/roster/BOS/2020">Celtics</a> <a href="/l/23/game_log/BOS_0/2020/${gid}">132-114</a> in game 4.`;

	test("a story linking the game's box score gives it away", () => {
		assert.isTrue(mentionsHiddenGame(story(812), new Set([812])));
		assert.isFalse(mentionsHiddenGame(story(811), new Set([812])));
	});

	test("no story, or nothing hidden", () => {
		assert.isFalse(mentionsHiddenGame(undefined, new Set([812])));
		assert.isFalse(mentionsHiddenGame(story(812), new Set()));
	});
});
