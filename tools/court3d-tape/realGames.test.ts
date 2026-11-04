// A TOOL, not a check: whole games played by the real sim, with real
// ratings, written out as the play-by-play and roster the 3D court is built
// from - for the game tape (see gameTape.test.ts). Skipped unless asked:
//
//   GEN_DIR=/some/dir GEN_SEEDS=11,22,33 npx vitest run --project basketball \
//     tools/court3d-tape/realGames.test.ts
//
// writes /some/dir/real_11.json and so on. The same seed is the same game.
import { writeFileSync } from "node:fs";
import process from "node:process";
import { test } from "vitest";
import { simRealGame } from "../../src/worker/core/GameSim.basketball/realGameForTests.ts";

test.skipIf(!process.env.GEN_DIR)(
	"real games for the 3D game tape",
	async () => {
		const seeds = (process.env.GEN_SEEDS ?? "11").split(",").map(Number);
		for (const seed of seeds) {
			writeFileSync(
				`${process.env.GEN_DIR}/real_${seed}.json`,
				JSON.stringify(await simRealGame(seed)),
			);
		}
	},
	600_000,
);
