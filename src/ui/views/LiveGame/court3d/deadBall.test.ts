import { assert, describe, test } from "vitest";
import { deadSpansOf, evalPlayer, fastAt } from "./evaluate.ts";
import { compile } from "./testGame.ts";

// Stood about with the ball dead, and down in it again with it live.
const EASY = new Set(["easy", "hips", "crouch", "laneSet"]);
const SET = new Set(["stance", "ready", "guard"]);

describe("the ball dead", () => {
	// After a whistle - a foul, a sub, the trip to the line - nobody stays
	// down in his stance for seconds on end; and he is back down in it
	// before the ball is back in play, never stood up easy in live play.
	test(
		"men stand easy through a stoppage and are set again before it is over",
		{ timeout: 240_000 },
		() => {
			const { tl } = compile("ftlane", 70);
			const spans = deadSpansOf(tl).filter(
				([t0, t1]) => Number.isFinite(t0) && t1 - t0 > 5000,
			);
			assert.isAbove(spans.length, 6);
			const inSpan = (t: number) => spans.some(([a, z]) => t >= a && t < z);
			let easy = 0;
			let set = 0;
			let after = 0;
			let late = 0;
			for (const [t0, t1] of spans) {
				for (let t = t0 + 1500; t < t1 - 2000; t += 250) {
					if (fastAt(tl, t) > 1) {
						continue;
					}
					for (const pid of tl.tracks.keys()) {
						const st = evalPlayer(tl, pid, t);
						if (!st.shown || st.moving) {
							continue;
						}
						easy += EASY.has(st.anim) ? 1 : 0;
						set += SET.has(st.anim) ? 1 : 0;
					}
				}
				// Back in play: whoever stands is set, not stood up.
				for (let t = t1; t < t1 + 400; t += 50) {
					if (inSpan(t)) {
						continue;
					}
					for (const pid of tl.tracks.keys()) {
						const st = evalPlayer(tl, pid, t);
						if (!st.shown || st.moving) {
							continue;
						}
						after += 1;
						late += st.anim === "easy" || st.anim === "crouch" ? 1 : 0;
					}
				}
			}
			assert.isAbove(easy, 150);
			assert.isBelow(set / (easy + set), 0.1, `${set} set, ${easy} easy`);
			assert.isAbove(after, 50);
			assert.strictEqual(late, 0);
			// Nor ever in live play.
			let live = 0;
			for (let t = 0; t < tl.end; t += 900) {
				if (inSpan(t) || deadSpansOf(tl).some(([a, z]) => t >= a && t < z)) {
					continue;
				}
				for (const pid of tl.tracks.keys()) {
					const st = evalPlayer(tl, pid, t);
					if (st.shown && st.anim === "easy") {
						live += 1;
					}
				}
			}
			assert.strictEqual(live, 0);
		},
	);

	// Off in the huddles - a timeout, the break between periods - and still
	// in them through what comes straight after (the next period called, the
	// subs made), the men stand about easy round their coach: nobody down in
	// his stance or squared up ready in the huddle, his arms out into his
	// teammates', before he has walked back out onto the floor.
	test(
		"nobody is down in his stance in the huddle",
		{ timeout: 240_000 },
		() => {
			const { tl } = compile("a", 140);
			const AFTER = new Set([
				"period",
				"overtime",
				"sub",
				"timeout",
				"timeouts",
			]);
			let stood = 0;
			const set: string[] = [];
			tl.beats.forEach((b, i) => {
				if (b.type !== "timeout" && b.type !== "endOfPeriod") {
					return;
				}
				let end = b.end;
				for (let k = i + 1; AFTER.has(tl.beats[k]?.type ?? ""); k++) {
					end = Math.max(end, tl.beats[k]!.end);
				}
				for (let t = b.actionStart + 1500; t < end - 600; t += 250) {
					if (fastAt(tl, t) > 1) {
						continue;
					}
					for (const pid of tl.tracks.keys()) {
						const st = evalPlayer(tl, pid, t);
						if (!st.shown || st.moving) {
							continue;
						}
						stood += 1;
						if (SET.has(st.anim)) {
							set.push(`${pid} ${st.anim} at ${Math.round(t)}`);
						}
					}
				}
			});
			assert.isAbove(stood, 100);
			assert.deepEqual(set.slice(0, 5), []);
		},
	);
});
