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

	test("the man with the ball works his man where he stands - and his man goes with him", () => {
		let probes = 0;
		let followed = 0;
		for (const tr of laid.tracks.values()) {
			if (tr.pid < 0) {
				continue;
			}
			const st = stepped.tracks.get(tr.pid)!;
			for (let t = 0; t < laid.end; t += 200) {
				const A = floorSpotOf(tr, t);
				const B = floorSpotOf(st, t);
				const A0 = floorSpotOf(tr, t - 300);
				const A1 = floorSpotOf(tr, t + 300);
				const ball = laid.ball.findLast((s) => s.t0 <= t);
				if (
					!A ||
					!B ||
					!A0 ||
					!A1 ||
					ball?.kind !== "hold" ||
					ball.pid !== tr.pid ||
					// (Where the play has him standing.)
					Math.hypot(A1.x - A0.x, A1.y - A0.y) > 0.3 ||
					Math.hypot(B.x - A.x, B.y - A.y) < 0.8
				) {
					continue;
				}
				// Off where the play had him, working his man: the nearest of
				// theirs off where the play had him the same way, a beat on.
				probes += 1;
				const dx = B.x - A.x;
				const dy = B.y - A.y;
				let near: number | undefined;
				let nd = 7;
				for (const o of laid.tracks.values()) {
					if (o.pid < 0 || o.team === tr.team) {
						continue;
					}
					const Q = floorSpotOf(o, t);
					if (Q && Math.hypot(Q.x - A.x, Q.y - A.y) < nd) {
						nd = Math.hypot(Q.x - A.x, Q.y - A.y);
						near = o.pid;
					}
				}
				if (near === undefined) {
					continue;
				}
				const C = floorSpotOf(laid.tracks.get(near)!, t + 300);
				const D = floorSpotOf(stepped.tracks.get(near)!, t + 300);
				if (
					C &&
					D &&
					((D.x - C.x) * dx + (D.y - C.y) * dy) / Math.hypot(dx, dy) > 0.3
				) {
					followed += 1;
				}
			}
		}
		assert.isAbove(probes, 30);
		assert.isAbove(followed / probes, 0.5);
	});
});
