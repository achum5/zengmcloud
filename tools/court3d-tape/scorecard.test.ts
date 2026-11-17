// The scorecard (scorecard.ts) on staged games:
//
//   SCORECARD_GAMES=/dir/real_11.json,a SCORECARD_OUT=/out.json npx vitest \
//     run --project basketball tools/court3d-tape/scorecard.test.ts
//
// A .json is a real game as saved for the game tape ({events, players});
// anything else a test game (testGame.ts). Without SCORECARD_GAMES, only a
// quick check that it runs, on a short test game.

import { readFileSync, writeFileSync } from "node:fs";
import process from "node:process";
import { assert, test } from "vitest";
import { compileCourt } from "../../src/ui/views/LiveGame/court3d/director.ts";
import { plainly } from "../../src/ui/views/LiveGame/court3d/evaluate.ts";
import { compile as compileFake } from "../../src/ui/views/LiveGame/court3d/testGame.ts";
import { scorecard } from "./scorecard.ts";

const load = (game: string) => {
	if (game.endsWith(".json")) {
		const { events, players } = JSON.parse(readFileSync(game, "utf8"));
		return compileCourt({ events, players, gid: 1, step: true });
	}
	return compileFake(game, 90).tl;
};

test.skipIf(!process.env.SCORECARD_GAMES)(
	"3D scorecard",
	() => {
		const out = [];
		for (const game of process.env.SCORECARD_GAMES!.split(",")) {
			const name = game
				.split("/")
				.at(-1)!
				.replace(/\.json$/, "");
			const tl = load(game);
			// (SCORECARD_PLAIN: drawn as before men leaned over their feet.)
			const card = (
				process.env.SCORECARD_PLAIN ? plainly : (f: () => any) => f()
			)(() =>
				scorecard(tl, name, {
					poseMin: Number(process.env.SCORECARD_POSE_MIN ?? 6),
				}),
			);
			console.log(JSON.stringify(card));
			out.push(card);
		}
		if (process.env.SCORECARD_OUT) {
			writeFileSync(process.env.SCORECARD_OUT, JSON.stringify(out, null, 1));
		}
	},
	3_600_000,
);

test("the scorecard runs on a test game, its numbers in range", () => {
	const { tl } = compileFake("a", 16);
	const card = scorecard(tl, "a", { poseMin: 0.5 });
	assert.isAbove(card.liveMin, 0);
	assert.isAbove(card.tempo.mph, 0);
	assert.isAtMost(card.tempo.standing, 1);
	assert.isAtLeast(card.movement.slideIn, 0);
}, 300_000);
