import { assert, describe, test } from "vitest";
import { compileCourt } from "../director.ts";
import { fakeGame, gidOf } from "../testGame.ts";

// A game staged by the new possession sim (see README.md).
const staged = (seed: string, possessions: number) => {
	const { events, players } = fakeGame(seed, possessions);
	return compileCourt({
		events,
		players,
		gid: gidOf(seed),
		step: true,
		possession: true,
	});
};

describe("3D possession sim: a game staged by it", () => {
	test("the same game stages the same way every time (every device agrees)", () => {
		const a = staged("same", 40);
		const b = staged("same", 40);
		assert.strictEqual(JSON.stringify(a.beats), JSON.stringify(b.beats));
		assert.strictEqual(JSON.stringify(a.ball), JSON.stringify(b.ball));
		assert.strictEqual(
			JSON.stringify([...a.tracks.values()]),
			JSON.stringify([...b.tracks.values()]),
		);
	}, 600_000);
});
