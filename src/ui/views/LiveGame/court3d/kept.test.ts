import { assert, describe, test } from "vitest";
import { evalPlayer, KEPT_MOST, keptFor, poseOf } from "./evaluate.ts";
import { compile } from "./testGame.ts";

// WATCHED TO THE END, NO HEAVIER THAN AT THE START. What the court works out
// once a run or a footfall and keeps for the frames after is kept only round
// the moment being shown - not every run of every man, all game, which by
// the fourth quarter was hundreds of megabytes and a phone crawling.
describe("3D: memory through a game", () => {
	test("played through, each man keeps only what is round now", () => {
		const { tl } = compile("kept", 40);
		const pids = [...tl.tracks.keys()].filter((pid) => pid >= 0);
		let most = 0;
		for (let t = 0; t < tl.end; t += 100) {
			for (const pid of pids) {
				const st = evalPlayer(tl, pid, t);
				if (st.shown) {
					poseOf(st);
				}
			}
			if (t % 60_000 < 100) {
				most = Math.max(most, keptFor(tl));
			}
		}
		most = Math.max(most, keptFor(tl));
		assert.isAbove(most, pids.length * 20);
		assert.isAtMost(most, pids.length * KEPT_MOST);
		// (Played back again from the start, all of it the same.)
		const again = compile("kept", 40).tl;
		for (let t = 0; t < tl.end; t += 2500) {
			for (const pid of pids) {
				const a = poseOf(evalPlayer(tl, pid, t));
				const b = poseOf(evalPlayer(again, pid, t));
				assert.deepEqual(a, b, `${pid} at ${t}`);
			}
		}
	}, 600_000);
});
