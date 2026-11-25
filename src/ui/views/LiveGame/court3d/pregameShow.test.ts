import { assert, describe, test } from "vitest";
import { MAIN_RIG } from "./camera.ts";
import { WARMUP_MS } from "./pregame.ts";
import {
	BOARD_MS,
	planShow,
	showAim,
	showEndsAt,
	warmAt,
} from "./pregameShow.ts";
import { fakeGame, gidOf } from "./testGame.ts";

const show = (seed: string) => {
	const { players } = fakeGame(seed, 30);
	return planShow(players, gidOf(seed));
};

describe("the pregame show", () => {
	test("the two teams warming up under the board, the same on every device", () => {
		const plan = show("show-a");
		assert.isAbove(plan.warm.parts.length, 0);
		assert.strictEqual(show("show-a").lookFrom, plan.lookFrom);
	});

	test("over once the game is in and the board has been up long enough", () => {
		assert.strictEqual(showEndsAt(undefined), Infinity);
		assert.strictEqual(showEndsAt(1500), BOARD_MS);
		assert.strictEqual(showEndsAt(BOARD_MS + 4000), BOARD_MS + 4000);
	});

	test("the warm-ups go round again while the game takes longer", () => {
		assert.strictEqual(warmAt(1234), 1234);
		assert.strictEqual(warmAt(WARMUP_MS + 1234), 1234);
	});

	test("looks round the building, never in close on anybody", () => {
		for (const seed of ["show-b", "show-c", "show-d"]) {
			const plan = show(seed);
			const kinds = new Set<string>();
			for (let t = 0; t < 90_000; t += 250) {
				for (const narrow of [false, true]) {
					const aim = showAim(plan, t, narrow);
					assert.isAtLeast(aim.shot.width, 60, seed);
					if (aim.rig === MAIN_RIG) {
						kinds.add("main");
						continue;
					}
					// (Stood off the floor, looking up at the stands.)
					assert.isAtLeast(aim.rig.at!.z, 8, seed);
					assert.isAtLeast(aim.shot.z ?? 0, 12, seed);
					kinds.add(`${aim.rig.at!.z}`);
				}
			}
			assert.strictEqual(kinds.size, 3, seed);
		}
	});
});
