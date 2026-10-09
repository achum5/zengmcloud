import { assert, describe, test } from "vitest";
import { compile } from "./testGame.ts";

const SHOTS = new Set([
	"shoot",
	"fade",
	"hook",
	"layup",
	"dunk",
	"dunk1",
	"tomahawk",
	"block",
]);

describe("3D post play", () => {
	test(
		"a low-post shot is a post move off a back-down, not a short jumper",
		{ timeout: 120_000 },
		() => {
			const moves = new Map<string, number>();
			let shots = 0;
			let backedDown = 0;
			for (const seed of ["p1", "p2"]) {
				const { tl, events } = compile(seed, 160);
				for (const b of tl.beats) {
					const e = events[b.i]!;
					if (e.type !== "fgaLowPost") {
						continue;
					}
					const tr = tl.tracks.get(e.pid as number)!;
					const a = tr.acts.find(
						(x) => SHOTS.has(x.anim) && Math.abs(x.t0 - b.actionStart) < 400,
					);
					shots += 1;
					moves.set(a?.anim ?? "none", (moves.get(a?.anim ?? "none") ?? 0) + 1);
					if (
						tr.moves.some(
							(m) =>
								m.anim === "post" && m.t1 > b.preStart && m.t0 < b.actionStart,
						)
					) {
						backedDown += 1;
					}
				}
			}
			assert.isAbove(shots, 30);
			assert.isAtMost((moves.get("shoot") ?? 0) / shots, 0.1);
			for (const kind of ["hook", "fade", "layup"]) {
				assert.isAbove(moves.get(kind) ?? 0, 0, kind);
			}
			assert.isAbove(backedDown / shots, 0.5);
		},
	);
});
