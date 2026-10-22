import { assert, describe, test } from "vitest";
import { bodyPoint, evalPlayer, poseOf, withBody } from "./evaluate.ts";
import { bodyOf, skeleton } from "./poses.ts";
import { compile } from "./testGame.ts";

describe("footwork", () => {
	// Shuffling - straight across, or on a slant, partly ahead or back as
	// well - the foot he is not stepping with stays where it is on the floor:
	// his feet step out the way he goes, not out to his side and then slid
	// back under him.
	test("a shuffling man's planted foot stays put, on a slant too", () => {
		const body = bodyOf();
		const STEP = 32;
		let travel = 0;
		let slide = 0;
		const { tl } = compile("a", 120);
		for (const tr of tl.tracks.values()) {
			let prev:
				| { x: number; y: number; feet: { x: number; y: number }[] }
				| undefined;
			for (let t = 200_000; t < 500_000; t += STEP) {
				const st = evalPlayer(tl, tr.pid, t);
				if (
					!st.shown ||
					st.anim !== "shuffle" ||
					(st.from !== undefined && st.from.w > 0.05)
				) {
					prev = undefined;
					continue;
				}
				const me = withBody(st, body);
				const sk = skeleton(body, poseOf(me));
				const feet = [bodyPoint(me, sk.legR.end), bodyPoint(me, sk.legL.end)];
				if (prev) {
					const d = Math.hypot(st.x - prev.x, st.y - prev.y);
					// (At a shuffle's pace: just setting off or pulling up, his
					// feet come under him as he settles.)
					if ((d * 1000) / STEP >= 6) {
						travel += d;
						slide += Math.min(
							...feet.map((f, i) =>
								Math.hypot(f.x - prev!.feet[i]!.x, f.y - prev!.feet[i]!.y),
							),
						);
					}
				}
				prev = { x: st.x, y: st.y, feet };
			}
		}
		assert.isAbove(travel, 100);
		assert.isBelow(slide / travel, 0.42);
	}, 120_000);

	// Drifting along the arc with his eyes on the ball, a shooter side-steps
	// - a foot out the way he goes and the other closed up after it - not a
	// run with his legs turned only so far under him and his feet sliding
	// the rest of the way.
	test("a shooter drifting along the arc side-steps, a foot planted", () => {
		const body = bodyOf();
		const STEP = 32;
		let travel = 0;
		let slide = 0;
		for (const seed of ["a"]) {
			const { tl } = compile(seed, 140);
			for (const tr of tl.tracks.values()) {
				let prev:
					| { x: number; y: number; feet: { x: number; y: number }[] }
					| undefined;
				for (let t = 0; t < Math.min(tl.end, 900_000); t += STEP) {
					const st = evalPlayer(tl, tr.pid, t);
					if (
						!st.shown ||
						st.anim !== "sideStep" ||
						(st.from !== undefined && st.from.w > 0.05)
					) {
						prev = undefined;
						continue;
					}
					const me = withBody(st, body);
					const sk = skeleton(body, poseOf(me));
					const feet = [bodyPoint(me, sk.legR.end), bodyPoint(me, sk.legL.end)];
					if (prev) {
						const d = Math.hypot(st.x - prev.x, st.y - prev.y);
						if (d > 0.02) {
							travel += d;
							slide += Math.min(
								...feet.map((f, i) =>
									Math.hypot(f.x - prev!.feet[i]!.x, f.y - prev!.feet[i]!.y),
								),
							);
						}
					}
					prev = { x: st.x, y: st.y, feet };
				}
			}
		}
		assert.isAbove(travel, 20);
		assert.isBelow(slide / travel, 0.35);
	}, 120_000);
});
