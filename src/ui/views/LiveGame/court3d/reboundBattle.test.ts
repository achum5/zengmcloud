import { assert, describe, test } from "vitest";
import { evalPlayer } from "./evaluate.ts";
import { COURT_H, rimX } from "./geometry.ts";
import { compile } from "./testGame.ts";

describe("3D rebound battles", () => {
	test(
		"a box-out is a battle for position, not two men standing still",
		{ timeout: 120_000 },
		() => {
			let pairs = 0;
			let moved = 0;
			let samples = 0;
			let inFront = 0;
			let touching = 0;
			for (const seed of ["r1", "r2", "r3", "r4", "r5"]) {
				const { tl } = compile(seed, 160);
				const tracks = [...tl.tracks.values()];
				for (const tr of tracks) {
					for (const a of tr.acts) {
						if (a.anim !== "boxOut" || a.t1 - a.t0 < 700) {
							continue;
						}
						// The man he has on his back: the one fighting him for it.
						const D0 = evalPlayer(tl, tr.pid, a.t0 + 100);
						const rival = tracks
							.filter(
								(o) =>
									o.team !== tr.team &&
									o.acts.some(
										(b) => b.anim === "fight" && b.t0 < a.t1 && b.t1 > a.t0,
									),
							)
							.map((o) => {
								const P = evalPlayer(tl, o.pid, a.t0 + 100);
								return { o, d: Math.hypot(P.x - D0.x, P.y - D0.y) };
							})
							.sort((x, y) => x.d - y.d)[0];
						if (!rival || rival.d > 3.5) {
							continue;
						}
						pairs += 1;
						const rim = {
							x: rimX(tl.poss.findLast(([t0]) => t0 <= a.t0)![1]),
							y: COURT_H / 2,
						};
						let path = 0;
						let prev: { x: number; y: number } | undefined;
						for (let t = a.t0 + 100; t < a.t1 - 50; t += 50) {
							const D = evalPlayer(tl, tr.pid, t);
							const O = evalPlayer(tl, rival.o.pid, t);
							if (prev) {
								path += Math.hypot(O.x - prev.x, O.y - prev.y);
							}
							prev = O;
							samples += 1;
							const dD = Math.hypot(D.x - rim.x, D.y - rim.y);
							const dO = Math.hypot(O.x - rim.x, O.y - rim.y);
							inFront += dD < dO ? 1 : 0;
							const gap = Math.hypot(D.x - O.x, D.y - O.y);
							touching += gap >= 1.2 && gap <= 3.6 ? 1 : 0;
						}
						moved += path > 0.8 ? 1 : 0;
					}
				}
			}
			assert.isAtLeast(pairs, 30);
			assert.isAtLeast(moved / pairs, 0.6, `${moved} of ${pairs} moved`);
			assert.isAtLeast(inFront / samples, 0.9);
			assert.isAtLeast(touching / samples, 0.85);
		},
	);
});
