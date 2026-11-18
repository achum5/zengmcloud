import { assert, describe, test } from "vitest";
import { evalPlayer } from "./evaluate.ts";
import { rimX } from "./geometry.ts";
import { compile } from "./testGame.ts";

// UP ON THE BALL, A DEFENDER STAYS DOWN IN HIS STANCE: in front of the man
// with it - within a couple of steps of him, nearer the rim he goes at -
// he slides, steps up and gives ground, low. Not up out of it walking or
// jogging alongside him (broadcast film, 2026-11-18: hardly ever).

describe("up on the ball", () => {
	test("a defender in front of the man with the ball never walks or jogs alongside him", () => {
		let n = 0;
		let upright = 0;
		for (const seed of ["onball-a", "onball-b"]) {
			const { tl } = compile(seed, 120);
			const tracks = [...tl.tracks.values()].filter((tr) => tr.pid >= 0);
			let k = 0;
			for (let t = 0; t < tl.end; t += 100) {
				while (k + 1 < tl.ball.length && tl.ball[k + 1]!.t0 <= t) {
					k += 1;
				}
				const s = tl.ball[k];
				if (s?.kind !== "hold") {
					continue;
				}
				const holder = tl.tracks.get(s.pid);
				if (!holder) {
					continue;
				}
				const H = evalPlayer(tl, holder.pid, t);
				if (!H.shown) {
					continue;
				}
				const rim = { x: rimX(holder.team), y: 25 };
				const toRim = Math.hypot(H.x - rim.x, H.y - rim.y);
				// (The half court: not bringing it up.)
				if (toRim > 30) {
					continue;
				}
				for (const tr of tracks) {
					if (tr.team === holder.team) {
						continue;
					}
					const D = evalPlayer(tl, tr.pid, t);
					if (
						!D.shown ||
						Math.hypot(D.x - H.x, D.y - H.y) > 7 ||
						Math.hypot(D.x - rim.x, D.y - rim.y) >= toRim
					) {
						continue;
					}
					n += 1;
					if (D.anim === "walk" || D.anim === "jog") {
						upright += 1;
					}
				}
			}
		}
		assert.isAbove(n, 500, "moments up on the ball");
		assert.isBelow(upright / n, 0.05, `walking or jogging: ${upright} of ${n}`);
	}, 480_000);
});
