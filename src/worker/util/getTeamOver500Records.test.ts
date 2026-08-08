import { assert, describe, test } from "vitest";
import {
	formatOver500Record,
	getTeamOver500Records,
} from "./getTeamOver500Records.ts";

// A finished game, in the shape getTeamOver500Records reads.
const game = (
	homeTid: number,
	homePts: number,
	awayTid: number,
	awayPts: number,
	extra: { playoffs?: boolean } = {},
) =>
	({
		playoffs: extra.playoffs ?? false,
		teams: [
			{ tid: homeTid, pts: homePts },
			{ tid: awayTid, pts: awayPts },
		],
	}) as any;

const records = (games: any[]) => getTeamOver500Records(2026, games);

describe("getTeamOver500Records", () => {
	// Four teams, arranged so each lands on a different side of the line:
	//   0 goes 3-1 (above .500)
	//   1 goes 2-2 (exactly .500 - NOT above)
	//   2 goes 1-3 (below)
	//   3 goes 2-2 (exactly .500), and split its two games with team 0
	// So the only qualifying opponent in the league is team 0.
	const league = [
		game(0, 110, 2, 100),
		game(0, 110, 2, 100),
		game(1, 110, 2, 100),
		game(2, 110, 1, 100),
		game(3, 110, 0, 100),
		game(0, 110, 3, 100),
		game(3, 110, 1, 100),
		game(1, 110, 3, 100),
	];

	test("counts only games against teams with a winning record", async () => {
		const result = await records(league);

		// Team 3 split with team 0, the league's only winning team.
		assert.strictEqual(formatOver500Record(result.get(3)), "1-1");
		// Team 2 lost twice to team 0 and its other games were against .500 teams.
		assert.strictEqual(formatOver500Record(result.get(2)), "0-2");
	});

	test("a team exactly at .500 is not above it", async () => {
		const result = await records(league);

		// Team 1 never played team 0; every opponent it faced finished .500 or
		// worse, so it has no qualifying games at all.
		assert.strictEqual(
			formatOver500Record(result.get(1)),
			"0-0",
			"'.500 or better' would quietly change what this column means",
		);
	});

	test("a team with no qualifying games still appears, at 0-0", async () => {
		const result = await records(league);
		assert.ok(result.has(1), "dropping out of the map blanks the column");
	});

	test("playoff games are excluded, matching the W-L beside it", async () => {
		const withPlayoffs = [
			...league,
			game(3, 130, 0, 100, { playoffs: true }),
			game(3, 130, 0, 100, { playoffs: true }),
		];
		const result = await records(withPlayoffs);
		assert.strictEqual(
			formatOver500Record(result.get(3)),
			"1-1",
			"two playoff wins over team 0 must not show up here",
		);
	});

	test("All-Star and other special games are ignored", async () => {
		// Negative tids are the special-game marker used throughout the game.
		const result = await records([...league, game(-1, 150, -2, 140)]);
		assert.strictEqual(formatOver500Record(result.get(3)), "1-1");
		assert.ok(!result.has(-1));
	});

	test("ties count as ties, not as losses", async () => {
		// Team 0 goes 2-0 to get above .500, then draws with team 3.
		const withTie = [
			game(0, 110, 1, 100),
			game(0, 110, 1, 100),
			game(3, 100, 0, 100),
		];
		const result = await records(withTie);
		assert.strictEqual(formatOver500Record(result.get(3)), "0-0-1");
	});

	test("an empty season produces an empty column rather than throwing", async () => {
		const result = await records([]);
		assert.strictEqual(result.size, 0);
		assert.strictEqual(formatOver500Record(undefined), "");
	});

	// The classification moves with the standings on purpose: a team you beat in
	// November stops counting if it finishes under .500.
	test("re-reads itself when an opponent falls below .500", async () => {
		const before = [game(0, 110, 1, 100), game(1, 110, 2, 100)];
		// Team 1 was 1-1... give it a win so it is 2-1 and above .500.
		const good = [...before, game(1, 110, 2, 100)];
		assert.strictEqual(
			formatOver500Record((await records(good)).get(0)),
			"1-0",
		);

		// Now team 1 loses three more and drops under .500, so team 0's win over
		// it no longer counts as a quality win.
		const collapsed = [
			...good,
			game(2, 110, 1, 100),
			game(2, 110, 1, 100),
			game(2, 110, 1, 100),
		];
		assert.strictEqual(
			formatOver500Record((await records(collapsed)).get(0)),
			"0-0",
		);
	});
});
