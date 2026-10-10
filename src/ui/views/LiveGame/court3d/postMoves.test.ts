import { assert, describe, test } from "vitest";
import { compile } from "./testGame.ts";

const SHOTS = new Set([
	"shoot",
	"fade",
	"hook",
	"layup",
	"powerLayup",
	"floater",
	"dunk",
	"dunk1",
	"tomahawk",
	"block",
]);

describe("3D post play", () => {
	test(
		"a low-post shot is a floater off a drive, a hook off a roll or a cut, or a move off a back-down - not a short jumper",
		{ timeout: 120_000 },
		() => {
			const moves = new Map<string, number>();
			let shots = 0;
			let backedDown = 0;
			let floated = 0;
			let floatedBackedDown = 0;
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
					const backed = tr.moves.some(
						(m) =>
							m.anim === "post" && m.t1 > b.preStart && m.t0 < b.actionStart,
					);
					if (a?.anim === "floater") {
						floated += 1;
						floatedBackedDown += backed ? 1 : 0;
					} else if (backed) {
						backedDown += 1;
					}
				}
			}
			assert.isAbove(shots, 30);
			assert.isAtMost((moves.get("shoot") ?? 0) / shots, 0.1);
			for (const kind of ["hook", "fade", "floater"]) {
				assert.isAbove(moves.get(kind) ?? 0, 0, kind);
			}
			// A floater is off the drive, never a back-down; post-ups back down.
			assert.isAbove(floated, 0);
			assert.equal(floatedBackedDown, 0);
			assert.isAbove(backedDown, 0);
		},
	);
});
