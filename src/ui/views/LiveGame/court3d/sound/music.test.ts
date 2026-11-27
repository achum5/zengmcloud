import { assert, describe, test } from "vitest";
import { compile } from "../testGame.ts";
import { HOME, crowdOf, possAt } from "./crowd.ts";
import { MUSIC_KINDS, musicAt } from "./music.ts";

describe("game sound: the building's music", () => {
	const { tl } = compile("sound", 140);
	const samples: { t: number; m: ReturnType<typeof musicAt> }[] = [];
	for (let t = 0; t < tl.end; t += 250) {
		samples.push({ t, m: musicAt(tl, t) });
	}

	test("never out of bounds, one kind at a time", () => {
		for (const { m } of samples) {
			let on = 0;
			for (const k of MUSIC_KINDS) {
				assert.isAtLeast(m[k], 0);
				assert.isAtMost(m[k], 1);
				if (m[k] > 0.05) {
					on++;
				}
			}
			assert.isAtMost(on, 1);
		}
	});

	test("in play only while the road team has the ball", () => {
		let road = 0;
		for (const { t, m } of samples) {
			if (m.musicDefense > 0) {
				assert.notStrictEqual(possAt(tl, t), HOME);
				road++;
			}
		}
		assert.isAbove(road, 20);
	});

	test("up through a timeout", () => {
		const long = crowdOf(tl).breaks.filter(([a, b]) => b - a > 8000);
		assert.isAbove(long.length, 0);
		for (const [a, b] of long) {
			assert.isAbove(musicAt(tl, (a + b) / 2).musicBreak, 0.5);
		}
	});
});
