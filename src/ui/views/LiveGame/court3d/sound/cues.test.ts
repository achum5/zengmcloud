import { assert, describe, test } from "vitest";
import { compile } from "../testGame.ts";
import { cuesOf, firstCueAt, squeaksIn } from "./cues.ts";
import { crowdOf, levelAt } from "./crowd.ts";

describe("game sound: what is heard, and when", () => {
	const { tl } = compile("sound", 140);
	const cues = cuesOf(tl);

	test("in order, all inside the game", () => {
		for (let k = 1; k < cues.length; k++) {
			assert.isAtLeast(cues[k]!.t, cues[k - 1]!.t);
		}
		for (const c of cues) {
			assert.isAtLeast(c.t, 0);
			assert.isAtMost(c.t, tl.end + 1);
			assert.isAtLeast(c.hard, 0);
			assert.isAtMost(c.hard, 1);
		}
	});

	test("a dribble's bounces come at its beat, not on top of each other", () => {
		const dribbles = cues.filter((c) => c.kind === "dribble");
		assert.isAbove(dribbles.length, 100);
		let close = 0;
		for (let k = 1; k < dribbles.length; k++) {
			if (dribbles[k]!.t - dribbles[k - 1]!.t < 150) {
				close++;
			}
		}
		assert.isBelow(close / dribbles.length, 0.02);
		assert.isTrue(dribbles.every((c) => c.fine));
	});

	test("every basket a swish or a slam, every period over a horn", () => {
		const makes = tl.fx.filter((f) => f.kind === "swish").length;
		const nets = cues.filter((c) => c.kind === "swish" || c.kind === "dunk");
		assert.isAtLeast(nets.length, Math.round(makes * 0.9));
		const horns = cues.filter((c) => c.kind === "horn");
		assert.isAtLeast(horns.length, 1);
		assert.isAtMost(horns.length, 8);
		// (One horn a period: never two on top of each other.)
		for (let k = 1; k < horns.length; k++) {
			assert.isAbove(horns[k]!.t - horns[k - 1]!.t, 5000);
		}
		// (No horn where the official's whistle is.)
		for (const h of horns) {
			assert.isFalse(
				cues.some((c) => c.kind === "whistle" && Math.abs(c.t - h.t) < 1),
			);
		}
	});

	test("a pass caught is heard where it is caught", () => {
		const passes = tl.ball.filter(
			(s) => s.kind === "fly" && "pid" in s.from && "pid" in s.to,
		).length;
		const caught = cues.filter((c) => c.kind === "catch");
		assert.isAbove(caught.length, passes * 0.5);
		assert.isTrue(caught.every((c) => c.fine && c.x !== undefined));
	});

	test("sneakers squeak about as often as on a real floor, however it is asked", () => {
		const all = squeaksIn(tl, 0, tl.end);
		const perMin = all.length / (tl.end / 60000);
		assert.isAbove(perMin, 15);
		assert.isBelow(perMin, 70);
		assert.isTrue(all.every((c) => c.fine && c.hard > 0 && c.hard <= 1));
		// (A frame at a time, as the device asks, the same as all at once.)
		const parts: number[] = [];
		for (let t = 0; t < tl.end; t += 137) {
			parts.push(...squeaksIn(tl, t, t + 137).map((c) => c.t));
		}
		assert.deepEqual(
			parts,
			all.map((c) => c.t),
		);
	});

	test("finding a moment's place in the list", () => {
		const t = cues[Math.floor(cues.length / 2)]!.t;
		const k = firstCueAt(cues, t);
		assert.strictEqual(cues[k]!.t, t);
		assert.isTrue(k === 0 || cues[k - 1]!.t < t);
	});

	test("the crowd: up for the home side's baskets, never out of bounds", () => {
		const c = crowdOf(tl);
		assert.isAbove(c.reactions.length, 20);
		const night = { playoffs: false, full: 1 };
		for (let t = 0; t < tl.end; t += 997) {
			const v = levelAt(tl, t, night);
			assert.isAtLeast(v, 0);
			assert.isAtMost(v, 1);
		}
		const cheer = c.reactions.find((r) => r.home && r.kind === "cheer");
		assert.isDefined(cheer);
		const before = levelAt(tl, cheer!.t - 50, night);
		const after = levelAt(tl, cheer!.t + 800, night);
		assert.isAbove(after, before);
	});
});
