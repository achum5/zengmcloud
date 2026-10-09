import { assert, describe, test } from "vitest";
import { LAYUPS } from "./poses.ts";
import { compile } from "./testGame.ts";

describe("3D finishes at the rim", () => {
	test("layups come in every kind - never one canned move", () => {
		const count = new Map<string, number>();
		let all = 0;
		for (const seed of ["f1", "f2", "f3"]) {
			const { tl } = compile(seed, 160);
			for (const tr of tl.tracks.values()) {
				for (const a of tr.acts) {
					if (LAYUPS.has(a.anim)) {
						count.set(a.anim, (count.get(a.anim) ?? 0) + 1);
						all += 1;
					}
				}
			}
		}
		assert.isAbove(all, 60);
		for (const anim of LAYUPS) {
			assert.isAtLeast((count.get(anim) ?? 0) / all, 0.05, anim);
		}
	}, 120_000);
});
