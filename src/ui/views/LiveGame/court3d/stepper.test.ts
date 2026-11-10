import { assert, describe, test } from "vitest";
import { evalPlayer, fastAt, floorSpotOf } from "./evaluate.ts";
import { rimX } from "./geometry.ts";
import { compileCourt } from "./director.ts";
import { follow } from "./stepper.ts";
import { fakeGame, gidOf } from "./testGame.ts";

describe("3D: a man stepped", () => {
	test("a cut is run to the corner, planted and gone - not rounded off", () => {
		// (15 feet a second up the floor, then across it.)
		const want = (t: number) =>
			t < 2000
				? { x: 10 + (15 * t) / 1000, y: 10 }
				: { x: 40, y: 10 + (15 * (t - 2000)) / 1000 };
		const none = {
			hit: () => false,
			next: () => Infinity,
			last: () => -Infinity,
		};
		const s = follow(want, 0, 4000, none);
		let near = Infinity;
		let least = Infinity;
		for (const q of s) {
			if (q.t > 1500 && q.t < 2500) {
				near = Math.min(near, Math.hypot(q.x - 40, q.y - 10));
				least = Math.min(least, Math.hypot(q.vx, q.vy));
			}
		}
		assert.isBelow(near, 1);
		assert.isBelow(least, 9);
		const end = s.at(-1)!;
		assert.isBelow(Math.abs(end.x - 40), 0.5);
		assert.isAbove(end.vy, 12);
	});
});

describe("3D: the ten stepped", () => {
	// (One game, laid out twice: as the director has it, and stepped.)
	const game = fakeGame("a", 60);
	const laid = compileCourt({ ...game, gid: gidOf("a") });
	const stepped = compileCourt({ ...game, gid: gidOf("a"), step: true });

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
		// steps do - see KNOT_PINNED. Up against a man, though - a contest,
		// a box-out, a screen, a foul - he is kept off that man where that
		// man really is, after: a foot or so off, now and then.)
		assert.isBelow(errs[Math.floor(errs.length * 0.95)]!, 0.05);
		assert.isBelow(errs[Math.floor(errs.length * 0.99)]!, 1);
		assert.deepEqual(
			stepped.beats.map((b) => [b.i, b.actionStart]),
			laid.beats.map((b) => [b.i, b.actionStart]),
		);
	});

	test("stepped, a man is never far off where the play wants him", () => {
		// (Off it while he takes a beat to react, and catches up: a step or
		// two at most - and the moves of his own it has him make on top of
		// it, round the arc, a crossover, his man's with him: a few feet.)
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
		assert.isBelow(off[Math.floor(off.length * 0.99)]!, 4);
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

	test("on defense a man goes a beat after his man - not with him, nor ahead of him", () => {
		// (How well his going matches his man's, a given time after.)
		// (How his going changes, a tenth of a second either side.)
		const vel = (pid: number, t: number) => {
			const a = evalPlayer(stepped, pid, t - 150);
			const b = evalPlayer(stepped, pid, t - 50);
			const c = evalPlayer(stepped, pid, t + 50);
			const d = evalPlayer(stepped, pid, t + 150);
			return {
				x: (d.x - c.x - (b.x - a.x)) * 10,
				y: (d.y - c.y - (b.y - a.y)) * 10,
			};
		};
		const LAGS = [-200, -100, 0, 50, 100, 150, 200, 250, 300, 400, 500];
		const sums = LAGS.map(() => 0);
		let n = 0;
		for (let t = 1000; t < stepped.end - 1000; t += 100) {
			if (fastAt(stepped, t) > 1.01) {
				continue;
			}
			const ball = stepped.ball.findLast((s) => s.t0 <= t);
			const holder =
				ball?.kind === "hold" ? stepped.tracks.get(ball.pid) : undefined;
			if (!holder) {
				continue;
			}
			for (const o of stepped.tracks.values()) {
				if (o.pid < 0 || o.team !== holder.team) {
					continue;
				}
				const P = evalPlayer(stepped, o.pid, t);
				const vm = vel(o.pid, t);
				if (
					!P.shown ||
					Math.hypot(vm.x, vm.y) < 0.5 ||
					Math.hypot(P.x - rimX(o.team), P.y - 25) > 30
				) {
					continue;
				}
				let near: number | undefined;
				let nd = 6;
				for (const d of stepped.tracks.values()) {
					if (d.pid < 0 || d.team === o.team) {
						continue;
					}
					const Q = evalPlayer(stepped, d.pid, t);
					if (Q.shown && Math.hypot(Q.x - P.x, Q.y - P.y) < nd) {
						nd = Math.hypot(Q.x - P.x, Q.y - P.y);
						near = d.pid;
					}
				}
				if (near === undefined) {
					continue;
				}
				n += 1;
				LAGS.forEach((L, k) => {
					const vd = vel(near!, t + L);
					sums[k]! += vm.x * vd.x + vm.y * vd.y;
				});
			}
		}
		assert.isAbove(n, 200);
		const best = LAGS[sums.indexOf(Math.max(...sums))]!;
		assert.isAtLeast(best, 100);
		assert.isAtMost(best, 300);
	});

	test("off the ball, from one spot behind the line to another, a man goes round it - not across inside it", () => {
		const beyond = (team: number, p: { x: number; y: number }) => {
			const depth = team === 0 ? p.x : 94 - p.x;
			return depth < 14
				? Math.abs(p.y - 25) - 22
				: Math.hypot(depth - 5.25, p.y - 25) - 23.75;
		};
		let runs = 0;
		let round = 0;
		for (const tr of laid.tracks.values()) {
			if (tr.pid < 0) {
				continue;
			}
			for (const m of tr.moves) {
				const mid = (m.t0 + m.t1) / 2;
				const A = floorSpotOf(tr, mid);
				const B = floorSpotOf(stepped.tracks.get(tr.pid)!, mid);
				const off = laid.poss.findLast((p) => p[0] <= mid)?.[1];
				const ball = laid.ball.findLast((s) => s.t0 <= mid);
				if (
					!A ||
					!B ||
					off !== tr.team ||
					(ball?.kind === "hold" && ball.pid === tr.pid) ||
					Math.hypot(m.to.x - m.from.x, m.to.y - m.from.y) < 6 ||
					beyond(tr.team, m.from) < 0 ||
					beyond(tr.team, m.to) < 0 ||
					beyond(tr.team, A) > -0.5 ||
					beyond(tr.team, A) < -3 ||
					tr.acts.some((x) => x.t0 < mid + 600 && x.t1 > mid - 600)
				) {
					continue;
				}
				runs += 1;
				// (On the line, more or less: see KNOT_TOL.)
				if (beyond(tr.team, B) > -0.3) {
					round += 1;
				}
			}
		}
		assert.isAbove(runs, 10);
		assert.isAbove(round / runs, 0.7);
	});

	test("the help goes with the ball too: a man off the ball shifts the way the man with it works his man", () => {
		let n = 0;
		let went = 0;
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
					Math.hypot(A1.x - A0.x, A1.y - A0.y) > 0.3 ||
					Math.hypot(B.x - A.x, B.y - A.y) < 0.8
				) {
					continue;
				}
				const dx = (B.x - A.x) / Math.hypot(B.x - A.x, B.y - A.y);
				const dy = (B.y - A.y) / Math.hypot(B.x - A.x, B.y - A.y);
				// (Theirs 10 to 20 feet off him - the help, not his man.)
				for (const o of laid.tracks.values()) {
					const Q = o.pid >= 0 && o.team !== tr.team && floorSpotOf(o, t);
					if (!Q) {
						continue;
					}
					const g = Math.hypot(Q.x - A.x, Q.y - A.y);
					if (g < 10 || g > 20) {
						continue;
					}
					const C = floorSpotOf(o, t + 300);
					const D = floorSpotOf(stepped.tracks.get(o.pid)!, t + 300);
					if (!C || !D) {
						continue;
					}
					n += 1;
					if ((D.x - C.x) * dx + (D.y - C.y) * dy > 0.05) {
						went += 1;
					}
				}
			}
		}
		assert.isAbove(n, 20);
		assert.isAbove(went / n, 0.5);
	});

	test("going from one side of his line to the other, the man with the ball takes it across with him - and has it back for what comes next", () => {
		// The crossovers the stepping puts in: on the move, not the
		// director's.
		const had = new Set(laid.ball.map((s) => `${s.kind} ${s.t0}`));
		const crosses = stepped.ball.filter(
			(s) =>
				s.kind === "hold" &&
				s.style === "cross" &&
				!s.still &&
				!had.has(`${s.kind} ${s.t0}`),
		);
		assert.isAbove(crosses.length, 5);
		// Each into the hand on the side he heads off to: off where the play
		// has him, more to that side of his way a beat on. (In at his man:
		// way out, bringing it up, he switches hands now and then whichever
		// way he goes - some of those too.)
		let n = 0;
		let goes = 0;
		let up = 0;
		for (const c of crosses) {
			if (c.kind !== "hold") {
				continue;
			}
			const lt = laid.tracks.get(c.pid)!;
			const st = stepped.tracks.get(c.pid)!;
			const at = floorSpotOf(lt, c.t0);
			if (at && Math.hypot(at.x - rimX(lt.team), at.y - 25) >= 40) {
				up += 1;
				continue;
			}
			const A0 = floorSpotOf(lt, c.t0 - 150);
			const A1 = floorSpotOf(lt, c.t0 + 450);
			const B0 = floorSpotOf(st, c.t0 - 150);
			const B1 = floorSpotOf(st, c.t0 + 450);
			if (!A0 || !A1 || !B0 || !B1) {
				continue;
			}
			const L = Math.hypot(A1.x - A0.x, A1.y - A0.y);
			if (L < 0.5) {
				continue;
			}
			// (His left, going the way he goes.)
			const lx = -(A1.y - A0.y) / L;
			const ly = (A1.x - A0.x) / L;
			const off0 = (B0.x - A0.x) * lx + (B0.y - A0.y) * ly;
			const off1 = (B1.x - A1.x) * lx + (B1.y - A1.y) * ly;
			n += 1;
			// (Out of his right hand into his left: off to his left.)
			if ((c.hand === "R") === off1 > off0) {
				goes += 1;
			}
		}
		assert.isAbove(n, 5);
		assert.isAbove(goes / n, 0.8, `${goes}/${n}`);
		assert.isAbove(up, 0);
		// And whatever he does next with it - a pass, a shot, picked up - it
		// is in the hand it was always going to be in.
		const handBefore = (ball: typeof laid.ball, t: number) => {
			const s = ball.findLast((x) => x.t0 < t);
			return s?.kind === "hold" && s.style === "dribble"
				? (s.hand ?? "R")
				: undefined;
		};
		let ends = 0;
		for (const s of laid.ball) {
			if (s.kind === "hold" && s.style !== "hold") {
				continue;
			}
			const was = handBefore(laid.ball, s.t0);
			if (was === undefined) {
				continue;
			}
			ends += 1;
			assert.strictEqual(handBefore(stepped.ball, s.t0), was, `${s.t0}`);
		}
		assert.isAbove(ends, 50);
	});
});
