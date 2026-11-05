// A TOOL, not a check: whole games played by the football sim, written out
// as the play-by-play and roster the 3D field is built from - for the game
// tape (see gameTape.football.test.ts). Skipped unless asked:
//
//   GEN_DIR=/some/dir GEN_SEEDS=7,23,24 npx vitest run --project football \
//     tools/field3d-tape/realGames.football.test.ts
//
// writes /some/dir/real_7.json and so on. The same seed is the same game (the
// tests' own games: see field3d/testGames.ts).
import { writeFileSync } from "node:fs";
import process from "node:process";
import { test } from "vitest";
import { simGames } from "../../src/ui/views/LiveGame/field3d/testGames.ts";

test.skipIf(!process.env.GEN_DIR)(
	"real games for the football 3D game tape",
	async () => {
		const seeds = (process.env.GEN_SEEDS ?? "7").split(",").map(Number);
		for (const seed of seeds) {
			const [game] = await simGames([seed]);
			writeFileSync(
				`${process.env.GEN_DIR}/real_${seed}.json`,
				JSON.stringify(game),
			);
		}
	},
	600_000,
);
