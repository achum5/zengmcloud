import { assert, describe, test } from "vitest";
import { evalBall, evalPlayer, fastAt } from "./evaluate.ts";
import { bodyOf } from "./poses.ts";
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

	// Off the ball, his head comes round to keep the ball in sight - running
	// back down the floor, cutting, in his stance - not set the way the rest
	// of him faces; turned through a moment, never whipped round in a frame.
	// (Before: facing more than 100 degrees from the ball 18% of the time.)
	test("off the ball, his eyes are on the ball, his head turned smoothly", () => {
		const body = bodyOf();
		const { tl } = compile("a", 60);
		const STEP = 16;
		let frames = 0;
		let away = 0;
		let whipped = 0;
		for (const tr of tl.tracks.values()) {
			let was: number | undefined;
			for (let t = 0; t < Math.min(tl.end, 600_000); t += STEP) {
				const st = evalPlayer(tl, tr.pid, t);
				if (fastAt(tl, t) > 1 || !st.shown) {
					was = undefined;
					continue;
				}
				const look = st.look ?? 0;
				if (was !== undefined && Math.abs(look - was) > 25) {
					whipped += 1;
				}
				was = look;
				if (st.holding || st.dribbleHand !== undefined) {
					continue;
				}
				frames += 1;
				const b = evalBall(tl, t, () => body);
				let off =
					st.yaw + (look * Math.PI) / 180 - Math.atan2(b.y - st.y, b.x - st.x);
				off -= Math.round(off / (Math.PI * 2)) * Math.PI * 2;
				if (Math.abs(off) > (100 * Math.PI) / 180) {
					away += 1;
				}
			}
		}
		assert.isAbove(frames, 50_000);
		assert.isBelow(away / frames, 0.13);
		assert.equal(whipped, 0);
	}, 240_000);
});
