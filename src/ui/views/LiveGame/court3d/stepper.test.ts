import { assert, describe, test } from "vitest";
import { evalPlayer, fastAt, floorSpotOf } from "./evaluate.ts";
import { stepTimeline } from "./stepper.ts";
import { compile } from "./testGame.ts";

describe("3D: the ten stepped", () => {
	// (One game, laid out twice: as the director has it, and stepped.)
	const laid = compile("a", 60).tl;
	const stepped = stepTimeline(compile("a", 60).tl);

	test("the play is where and when it was: every act where the director has it", () => {
		let acts = 0;
		const errs: number[] = [];
		for (const tr of laid.tracks.values()) {
			if (tr.pid < 0) {
				continue;
			}
			const st = stepped.tracks.get(tr.pid)!;
			for (const a of tr.acts) {
				const t = (a.t0 + a.t1) / 2;
				const A = floorSpotOf(tr, t);
				const B = floorSpotOf(st, t);
				if (!A || !B) {
					continue;
				}
				acts += 1;
				errs.push(Math.hypot(A.x - B.x, A.y - B.y));
			}
		}
		assert.isAbove(acts, 500);
		errs.sort((x, y) => x - y);
		// (To a hair: the runs it is laid down as bend a little less than the
		// steps do - see KNOT_PINNED.)
		assert.isBelow(errs[Math.floor(errs.length * 0.95)]!, 0.05);
		assert.isBelow(errs.at(-1)!, 0.1);
		assert.deepEqual(
			stepped.beats.map((b) => [b.i, b.actionStart]),
			laid.beats.map((b) => [b.i, b.actionStart]),
		);
	});

	test("stepped, a man is never far off where the play wants him", () => {
		// (Off it while he takes a beat to react, and catches up: a step or
		// two at most.)
		const off: number[] = [];
		for (const tr of laid.tracks.values()) {
			if (tr.pid < 0) {
				continue;
			}
			const st = stepped.tracks.get(tr.pid)!;
			for (let t = 0; t < laid.end; t += 250) {
				const A = floorSpotOf(tr, t);
				const B = floorSpotOf(st, t);
				if (A && B) {
					off.push(Math.hypot(A.x - B.x, A.y - B.y));
				}
			}
		}
		off.sort((a, b) => a - b);
		assert.isAbove(off.length, 1000);
		assert.isBelow(off[Math.floor(off.length * 0.5)]!, 0.4);
		assert.isBelow(off[Math.floor(off.length * 0.99)]!, 3);
	});

	test("stepped, nobody gets going, turns or pulls up harder than a man can", () => {
		let hard = 0;
		let n = 0;
		for (const tr of stepped.tracks.values()) {
			if (tr.pid < 0) {
				continue;
			}
			for (let t = 0; t < stepped.end; t += 100) {
				if (fastAt(stepped, t) > 1.01) {
					continue;
				}
				const a = evalPlayer(stepped, tr.pid, t - 100);
				const b = evalPlayer(stepped, tr.pid, t);
				const c = evalPlayer(stepped, tr.pid, t + 100);
				if (
					!a.shown ||
					!b.shown ||
					!c.shown ||
					tr.acts.some((x) => x.t0 < t + 150 && x.t1 > t - 150)
				) {
					continue;
				}
				n += 1;
				// (Feet a second, a second.)
				const ax = (c.x - 2 * b.x + a.x) / 0.01;
				const ay = (c.y - 2 * b.y + a.y) / 0.01;
				if (Math.hypot(ax, ay) > 40) {
					hard += 1;
				}
			}
		}
		assert.isAbove(n, 1000);
		assert.isBelow(hard / n, 0.01);
	});
});
