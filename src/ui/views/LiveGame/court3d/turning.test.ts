import { assert, describe, test } from "vitest";
import { evalPlayer, fastAt } from "./evaluate.ts";
import { compile } from "./testGame.ts";

describe("turning", () => {
	// Stood off the ball with nothing coming to him, a man follows it round
	// at an easy pace - not whipped round on the spot every time it moves.
	// (Only with a pass on its way to him does he come round quick.)
	test("a man stood waiting off the ball turns at an easy pace", () => {
		const { tl } = compile("turns", 40);
		const pids = [...tl.tracks.keys()];
		const STEP = 16;
		let prev = new Map<number, ReturnType<typeof evalPlayer>>();
		let frames = 0;
		let quick = 0;
		for (let t = 0; t < Math.min(tl.end, 240_000); t += STEP) {
			if (fastAt(tl, t) > 1) {
				prev = new Map();
				continue;
			}
			const cur = new Map<number, ReturnType<typeof evalPlayer>>();
			for (const pid of pids) {
				const st = evalPlayer(tl, pid, t);
				cur.set(pid, st);
				const p = prev.get(pid);
				if (
					!p ||
					!st.shown ||
					st.moving ||
					st.anim !== "ready" ||
					p.anim !== "ready"
				) {
					continue;
				}
				frames += 1;
				let d = st.yaw - p.yaw;
				d -= Math.round(d / (Math.PI * 2)) * Math.PI * 2;
				if ((Math.abs(d) * 1000) / STEP > 6) {
					quick += 1;
				}
			}
			prev = cur;
		}
		assert.isAbove(frames, 5000);
		assert.isBelow(quick, frames * 0.02, `${quick} of ${frames}`);
	}, 240_000);
});
