import { assert, describe, test } from "vitest";
import { compileCourt, isLineItem } from "./director.ts";
import {
	FAST,
	evalBall,
	evalPlayer,
	fastAt,
	poseOf,
	tensionAt,
} from "./evaluate.ts";
import { crowdAt } from "./scene.ts";
import { COURT_H, COURT_W, FT_LINE_DEPTH, rimX } from "./geometry.ts";
import { LAYUPS } from "./poses.ts";
import { kindOf, playShot } from "./physics.ts";
import { compile, fakeGame } from "./testGame.ts";
import { finishOf } from "../../../util/liveGameWording.basketball.ts";
import {
	bodyFor,
	SHOOTING,
	cutBetween,
	sampleTimes,
} from "./directorTesting.ts";

describe("3D director", () => {
	test("every play-by-play line gets one beat, in order, tiling the timeline", () => {
		for (const seed of ["a", "b", "c"]) {
			const { events, tl } = compile(seed);
			const lines = events
				.map((e, i) => ({ e, i }))
				.filter(({ e }) => isLineItem(e))
				.map(({ i }) => i);
			assert.deepStrictEqual(
				tl.beats.map((b) => b.i),
				lines,
			);
			let prevEnd = 0;
			for (const b of tl.beats) {
				assert.strictEqual(
					b.preStart,
					prevEnd,
					`beat ${b.i} ${b.type} starts where the last ended`,
				);
				assert.isAtLeast(b.actionStart, b.preStart);
				assert.isAbove(b.end, b.actionStart);
				prevEnd = b.end;
			}
			assert.strictEqual(tl.end, prevEnd);
		}
	}, 240_000);

	test("in the air, the ball falls at gravity's pace", () => {
		const { tl } = compile("gravity", 120);
		for (const seg of tl.ball) {
			if (seg.kind !== "fly" || seg.t1 - seg.t0 < 300) {
				continue;
			}
			// The second difference of height over equal steps is -g * dt^2.
			const dt = 0.04;
			const m = (seg.t0 + seg.t1) / 2;
			const z = (t: number) => evalBall(tl, t, bodyFor).z;
			const accel =
				(z(m + dt * 1000) - 2 * z(m) + z(m - dt * 1000)) / (dt * dt);
			assert.closeTo(accel, -32.2, 1.5);
		}
	}, 240_000);

	// Picked up off the dribble, switched hand to hand, scooped off the
	// floor, thrown off a dribble: from one move to the next the ball goes
	// from wherever the last one left it - never faster than a thrown ball.
	test("the ball travels - it never jumps between frames", () => {
		for (const seed of ["ball", "a"]) {
			const { tl } = compile(seed, 140);
			let prev: { x: number; y: number; z: number } | undefined;
			let last = -Infinity;
			for (const t of sampleTimes(tl, 20)) {
				const b = evalBall(tl, t, bodyFor);
				assert.isTrue(
					Number.isFinite(b.x) && Number.isFinite(b.y) && Number.isFinite(b.z),
				);
				const cut = cutBetween(tl, last, t);
				last = t;
				if (prev && !cut) {
					const d = Math.hypot(b.x - prev.x, b.y - prev.y, b.z - prev.z);
					assert.isBelow(
						d,
						2,
						`${seed}: ball jumped ${d.toFixed(2)}ft at ${t}`,
					);
				}
				prev = b;
			}
		}
	}, 480_000);

	// Kicked out of the lane, the shooter is open because his man left him
	// to help on the drive - a stunt at the ball, or all the way over - and
	// by the time the shot goes up somebody has closed back out to him: up
	// on him now and then, a step late with a hand up mostly, or way late
	// and still on his way. The passer does not stay in there watching it:
	// he gets back out (a big stays in, for the rebound).
	test("a kick out of the lane: the help leaves the shooter and closes back out", () => {
		const helps: number[] = [];
		let closed = 0;
		let onHim = 0;
		let passers = 0;
		let backOut = 0;
		// (Three games: in two, up on him is 3-4 of ~24 - right on the line,
		// either side of it as the games come out.)
		for (const seed of ["a", "kick", "b"]) {
			const { tl, players } = compile(seed, 140);
			const big = new Set(
				players
					.filter((p) => ["PF", "FC", "C"].includes(p.pos ?? ""))
					.map((p) => p.pid),
			);
			const teamOf = (pid: number) => tl.tracks.get(pid)!.team;
			const at = (pid: number, t: number) => evalPlayer(tl, pid, t);
			// How far off him the nearest man guarding him is.
			const room = (pid: number, t: number) => {
				const p = at(pid, t);
				let best = Infinity;
				for (const tr of tl.tracks.values()) {
					const q = at(tr.pid, t);
					if (tr.team !== teamOf(pid) && q.shown) {
						best = Math.min(best, Math.hypot(q.x - p.x, q.y - p.y));
					}
				}
				return best;
			};
			tl.ball.forEach((pass, i) => {
				if (
					pass.kind !== "fly" ||
					!("pid" in pass.from) ||
					!("pid" in pass.to)
				) {
					return;
				}
				const passer = pass.from.pid;
				const shooter = pass.to.pid;
				const team = teamOf(shooter);
				const shot = tl.ball.slice(i + 1).find((s) => s.kind !== "hold");
				if (
					teamOf(passer) !== team ||
					shot?.kind !== "fly" ||
					!("pid" in shot.from) ||
					shot.from.pid !== shooter ||
					("pid" in shot.to && teamOf(shot.to.pid) === team) ||
					shot.t0 - pass.t1 > 1500
				) {
					return;
				}
				// Out of the lane, to a man who catches it and lets it go - a
				// shot, not a bounce pass on.
				const P = at(passer, pass.t0);
				const s0 = at(shooter, pass.t1);
				const s1 = at(shooter, shot.t0);
				if (
					Math.hypot(P.x - rimX(team), P.y - COURT_H / 2) > 17 ||
					Math.hypot(s1.x - s0.x, s1.y - s0.y) > 3 ||
					!SHOOTING.has(at(shooter, shot.t0 - 1).anim)
				) {
					return;
				}
				const off = room(shooter, pass.t0);
				helps.push(off);
				const late = room(shooter, shot.t0);
				// (Or way late, but on his way: well in off him already, or
				// flying at him as it goes up - feet closer in a moment.)
				if (
					late <= 6.5 ||
					(late <= 9 && late <= off - 3) ||
					(late <= 12 && room(shooter, shot.t0 + 400) <= late - 2)
				) {
					closed += 1;
				}
				if (late <= 4) {
					onHim += 1;
				}
				if (!big.has(passer)) {
					passers += 1;
					const Q = at(passer, pass.t0 + 1500);
					if (Math.hypot(Q.x - P.x, Q.y - P.y) >= 4) {
						backOut += 1;
					}
				}
			});
		}
		assert.isAtLeast(helps.length, 20);
		const left = helps.filter((d) => d >= 6);
		assert.isAtLeast(left.length / helps.length, 0.6);
		assert.isAtLeast(Math.max(...left) - Math.min(...left), 4);
		assert.isAtLeast(closed / helps.length, 0.85);
		// (Up on him now and then: one kick-out in six or seven - a three,
		// mostly, and the league's tight ones are about that.)
		assert.isAtLeast(onHim / helps.length, 0.14);
		assert.isAtLeast(backOut / passers, 0.6);
	}, 240_000);

	// With the ball played in close in the front court, nobody off it is
	// left stood out at half court watching - the man who pitched it ahead,
	// the one the break left back: he trails up into the play. Nor parked a
	// long step behind the arc, out of it: he comes up to it.
	test("nobody stood out at half court while the ball is played in close", () => {
		let samples = 0;
		// (Stood 36 ft+ from the rim - half court - and 31 ft+.)
		const lines = [36, 31].map((at) => ({ at, stood: [] as string[] }));
		for (const seed of ["a", "b", "c", "d", "e", "f"]) {
			const { tl } = compile(seed, 140);
			const fast = (t: number) => tl.fast.some(([a, b]) => t >= a && t < b);
			const runs = lines.map(
				() => new Map<number, { t: number; x: number; y: number }>(),
			);
			const end = (k: number, pid: number, t: number) => {
				const r = runs[k]!.get(pid);
				if (r && t - r.t >= 2000) {
					lines[k]!.stood.push(`${seed} ${pid} ${r.t} ${t - r.t}`);
				}
				runs[k]!.delete(pid);
			};
			for (let t = 0; t < tl.end; t += 200) {
				const h = fast(t) ? undefined : evalBall(tl, t, bodyFor).holder;
				const team = h === undefined ? undefined : tl.tracks.get(h)!.team;
				const rim =
					team === undefined ? undefined : { x: rimX(team), y: COURT_H / 2 };
				const H = h === undefined ? undefined : evalPlayer(tl, h, t);
				if (!H || !rim || Math.hypot(H.x - rim.x, H.y - rim.y) > 30) {
					runs.forEach((m, k) => {
						for (const pid of m.keys()) {
							end(k, pid, t);
						}
					});
					continue;
				}
				for (const tr of tl.tracks.values()) {
					if (tr.team !== team || tr.pid === h) {
						continue;
					}
					const q = evalPlayer(tl, tr.pid, t);
					if (!q.shown) {
						continue;
					}
					samples += 1;
					const out = Math.hypot(q.x - rim.x, q.y - rim.y);
					lines.forEach((line, k) => {
						const r = runs[k]!.get(tr.pid);
						const deep = out >= line.at;
						if (deep && r && Math.hypot(q.x - r.x, q.y - r.y) <= 3) {
							return;
						}
						end(k, tr.pid, t);
						if (deep) {
							runs[k]!.set(tr.pid, { t, x: q.x, y: q.y });
						}
					});
				}
			}
		}
		assert.isAbove(samples, 10_000);
		// (A man stood out there 2 s+: 22 of them in these six games before;
		// a break's trailers waiting on the wings to get down the floor
		// before they came on, 3 more.)
		assert.isAtMost(lines[0]!.stood.length, 3, lines[0]!.stood.join("; "));
		// (31 ft out: 10 of them before - a 3-on-2's trailers parked at the
		// hashes, men trailing up to 30-32 ft.)
		assert.isAtMost(lines[1]!.stood.length, 4, lines[1]!.stood.join("; "));
	}, 960_000);

	// After a basket he lets it out - three fingers up, a fist, a flex, a
	// finger at the man who found him - on his way back down the floor: not
	// stood there doing it, nor pulled up to do it when he was already on
	// his way. (Play stopped after it - a timeout, a change, the end of the
	// period, his free throw to come - all of him, where he stands.)
	test("a scorer lets it out on his way back, never stood still for it", () => {
		const STOOD = new Set([
			"thanks",
			"threeUp",
			"goggles",
			"shimmy",
			"roar",
			"chestPound",
		]);
		const CHEERS = new Set(["three", "fist", "flex", "pound"]);
		let cheers = 0;
		let stood = 0;
		const still: string[] = [];
		for (const seed of ["a", "b", "c"]) {
			const { tl } = compile(seed, 140);
			// (The beat after the basket: play on, or stopped.)
			const live = (t: number) => {
				const k = tl.beats.findIndex((b) => b.actionStart <= t && b.end > t);
				const next = k >= 0 ? tl.beats[k + 1] : undefined;
				return (
					next !== undefined &&
					!["timeout", "sub", "endOfPeriod", "gameOver"].includes(next.type) &&
					!/AndOne$/.test(tl.beats[k]!.type)
				);
			};
			for (const tr of tl.tracks.values()) {
				stood += tr.acts.filter(
					(a) => STOOD.has(a.anim) && live(a.t0 - 400),
				).length;
				for (const g of tr.arms) {
					if (!CHEERS.has(g.kind) || fastAt(tl, g.t0) > 1.01) {
						continue;
					}
					cheers += 1;
					const a = evalPlayer(tl, tr.pid, g.t0);
					const b = evalPlayer(tl, tr.pid, g.t1);
					if (Math.hypot(b.x - a.x, b.y - a.y) < 3) {
						still.push(`${seed}: ${tr.pid} ${g.kind} at ${Math.round(g.t0)}`);
					}
				}
			}
		}
		assert.strictEqual(stood, 0);
		assert.isAbove(cheers, 10);
		assert.isAtMost(still.length, cheers * 0.15, still.join("; "));
	}, 480_000);

	// Turning right round, he turns one way and keeps turning - he never
	// flicks between facings from one frame to the next.
	test("players turn smoothly - no snap in which way anybody faces", () => {
		const { tl } = compile("turns", 140);
		const pids = [...tl.tracks.keys()];
		let prev = new Map<number, ReturnType<typeof evalPlayer>>();
		let last = -Infinity;
		for (const t of sampleTimes(tl, 20)) {
			const cut = cutBetween(tl, last, t);
			last = t;
			const cur = new Map<number, ReturnType<typeof evalPlayer>>();
			for (const pid of pids) {
				const st = evalPlayer(tl, pid, t);
				cur.set(pid, st);
				const p = prev.get(pid);
				if (p && p.shown && st.shown && !cut) {
					let d = st.yaw - p.yaw;
					d -= Math.round(d / (Math.PI * 2)) * Math.PI * 2;
					// (A spin move goes right round, quick - but round, all the
					// same, not in a snap.)
					const spin = st.anim === "spinMove" || p.anim === "spinMove";
					assert.isBelow(
						Math.abs(d),
						spin ? 0.5 : 0.25,
						`pid ${pid} turned ${d.toFixed(2)} in a frame at ${t}`,
					);
				}
			}
			prev = cur;
		}
	}, 960_000);

	// Watched at the usual speed: the ball down through the net and both
	// teams heading back up the floor at real speed - a good two and a half
	// seconds of it - and only then does the picture build up speed, over
	// the best part of half a second, rather than lurching away.
	test("after a basket the picture lets it sink in, then builds up speed", () => {
		const { tl } = compile("cuts", 160);
		let makes = 0;
		for (const b of tl.beats) {
			if (!/^(fg|tp)/.test(b.type) || b.type.startsWith("fga")) {
				continue;
			}
			const swish = tl.fx.find(
				(f) => f.kind === "swish" && Math.abs(f.t - b.actionStart) < 200,
			);
			const next = tl.fast.find(([, z]) => z > b.actionStart);
			if (!swish || !next || next[1] - next[0] < 5000) {
				continue;
			}
			makes += 1;
			// The viewer's clock (ms), the picture's running at the multiplier.
			let t = swish.t;
			let seen = 0;
			let off: number | undefined;
			let full: number | undefined;
			while (full === undefined && t < next[1]) {
				const rate = fastAt(tl, t);
				if (off === undefined && rate >= 1.5) {
					off = seen;
				}
				if (rate >= FAST * 0.9) {
					full = seen;
				}
				t += 10 * rate;
				seen += 10;
			}
			assert.isAtLeast(off!, 2500, `${b.type} at ${b.actionStart}`);
			assert.isAtLeast(full! - off!, 400, `${b.type} at ${b.actionStart}`);
		}
		assert.isAbove(makes, 10);
	}, 240_000);

	// A quick trip off a defensive board or a steal is a fast break - run
	// out at full speed, never fast-forwarded - and a long one walks it up
	// (run through fast) and runs something first: a screen or a pass more
	// before the action that gets the shot.
	test("quick trips run out on the break; long ones run something first", () => {
		let quick = 0;
		let broke = 0;
		const actions: Record<"short" | "long", number[]> = { short: [], long: [] };
		for (const seed of [
			"break",
			"break2",
			"break3",
			"break4",
			"break5",
			"break6",
			"break7",
			"break8",
		]) {
			const { events, tl } = compile(seed, 160);
			tl.beats.forEach((b, j) => {
				const e = events[b.i]!;
				if (!/^fga/.test(e.type) || j === 0) {
					return;
				}
				const prev = tl.beats[j - 1]!;
				const p = events[prev.i]!;
				if (typeof e.clock !== "number" || typeof p.clock !== "number") {
					return;
				}
				const gap = p.clock - e.clock;
				// A break is played at real speed from the push up the floor
				// on: whatever is hurried through (the rebound, the outlet) is
				// over well before the shot - not just the last few seconds of
				// a set.
				const live =
					b.actionStart -
					Math.max(
						prev.actionStart,
						...tl.fast
							.filter(([x, y]) => y > prev.actionStart && x < b.actionStart)
							.map(([, y]) => y),
					);
				if ((p.type === "drb" || p.type === "stl") && gap < 6) {
					quick += 1;
					broke += live >= 4500 ? 1 : 0;
				}
				if (p.type !== "drb" && !/^(fg|tp)/.test(p.type)) {
					return;
				}
				// Screens set and passes thrown on the way to the shot.
				let n = tl.ball.filter(
					(s) =>
						s.kind === "fly" &&
						s.t0 > prev.actionStart &&
						s.t0 < b.actionStart &&
						"pid" in s.from &&
						"pid" in s.to &&
						s.t1 - s.t0 > 200,
				).length;
				for (const tr of tl.tracks.values()) {
					n += tr.acts.filter(
						(a) =>
							a.anim === "screen" &&
							a.t0 > prev.actionStart &&
							a.t0 < b.actionStart,
					).length;
				}
				if (gap < 9) {
					actions.short.push(n);
				} else if (gap >= 16) {
					actions.long.push(n);
				}
			});
		}
		const mean = (xs: number[]) => xs.reduce((a, b) => a + b, 0) / xs.length;
		assert.isAbove(quick, 10);
		assert.isAbove(broke / quick, 0.75);
		assert.isAbove(actions.long.length, 20);
		assert.isAbove(mean(actions.long), mean(actions.short) + 2);
	}, 480_000);

	test("on the break the ball goes one way - nobody takes it back up the floor to bring it in again", () => {
		let breaks = 0;
		const back: string[] = [];
		for (const seed of [
			"break",
			"break2",
			"break3",
			"break4",
			"break5",
			"break6",
			"break7",
			"break8",
		]) {
			const { events, tl } = compile(seed, 160);
			tl.beats.forEach((b, j) => {
				const e = events[b.i]!;
				if (!/^fga/.test(e.type) || j === 0) {
					return;
				}
				// Quick off a board, a steal or a basket.
				const prev = tl.beats[j - 1]!;
				const p = events[prev.i]!;
				if (
					!/^(drb|stl|fg[A-Z]|tp)/.test(p.type) ||
					typeof e.clock !== "number" ||
					typeof p.clock !== "number" ||
					p.clock - e.clock >= 9
				) {
					return;
				}
				const shot = tl.ball.find(
					(s) =>
						s.kind === "fly" &&
						s.t0 >= b.actionStart - 300 &&
						"pid" in s.from &&
						!("pid" in s.to),
				);
				if (shot?.kind !== "fly" || "pid" in shot.to) {
					return;
				}
				const rim = {
					x: shot.to.x < COURT_W / 2 ? rimX(0) : rimX(1),
					y: COURT_H / 2,
				};
				breaks += 1;
				for (const tr of tl.tracks.values()) {
					for (const m of tr.moves) {
						// Out on the floor in the frontcourt with it, back the way
						// he came. (Not a step or two back out of the lane, facing
						// the rim, to throw it - nor a drive out of the corner,
						// up the floor but in at the rim.)
						const out0 = Math.abs(m.from.x - rim.x);
						const r0 = Math.hypot(m.from.x - rim.x, m.from.y - rim.y);
						if (
							m.anim === "dribble" &&
							m.t0 >= prev.actionStart &&
							m.t0 < b.actionStart &&
							out0 < COURT_W / 2 &&
							r0 >= 19 &&
							Math.abs(m.to.x - rim.x) - out0 > 8 &&
							Math.hypot(m.to.x - rim.x, m.to.y - rim.y) > r0
						) {
							back.push(`${seed} ${Math.round(m.t0)} ${tr.pid}`);
						}
					}
				}
			});
		}
		assert.isAbove(breaks, 10);
		assert.deepEqual(back, []);
	}, 480_000);

	test("whoever holds the ball is on the floor", () => {
		const { tl } = compile("holder", 140);
		for (const t of sampleTimes(tl, 50)) {
			const b = evalBall(tl, t, bodyFor);
			if (b.holder !== undefined) {
				assert.isTrue(
					evalPlayer(tl, b.holder, t).shown,
					`holder ${b.holder} hidden at ${t}`,
				);
			}
		}
	}, 240_000);

	test("a make swishes and a miss rattles, right when its line shows", () => {
		const { tl } = compile("fx", 140);
		for (const b of tl.beats) {
			const near = (kind: string) =>
				tl.fx.some(
					(f) =>
						f.kind === kind &&
						f.t >= b.actionStart - 10 &&
						f.t <= b.actionStart + 200,
				);
			if (/^(fg|tp)/.test(b.type) && !b.type.startsWith("fga")) {
				assert.isTrue(near("swish"), `${b.type} at ${b.actionStart}`);
			}
			if (b.type.startsWith("miss") && b.type !== "missFt") {
				// (Not an air ball: nothing there to rattle.)
				const path = tl.ball.find(
					(s) =>
						s.kind === "path" &&
						s.t0 < b.actionStart &&
						s.t1 >= b.actionStart - 5,
				);
				if (path?.kind === "path") {
					const p0 = { x: path.pts[0]!, y: path.pts[1]!, z: path.pts[2]! };
					const side =
						Math.abs(p0.x - rimX(0)) < Math.abs(p0.x - rimX(1)) ? 0 : 1;
					if (kindOf(playShot(side, p0, path.v0)) === "air") {
						continue;
					}
				}
				assert.isTrue(near("clank"), `${b.type} at ${b.actionStart}`);
			}
		}
	}, 240_000);

	test("up to the rim, off it and into the hands of the man who gets it, the ball never swerves", () => {
		const { tl } = compile("a", 140);
		const speed = (t: number) => {
			const a = evalBall(tl, t, bodyFor);
			const b = evalBall(tl, t + 5, bodyFor);
			return {
				x: (b.x - a.x) / 0.005,
				y: (b.y - a.y) / 0.005,
				z: (b.z - a.z) / 0.005,
			};
		};
		const swerve = (t: number) => {
			const a = speed(t - 6);
			const b = speed(t + 1);
			return Math.hypot(b.x - a.x, b.y - a.y, b.z - a.z);
		};
		const into: number[] = [];
		const off: number[] = [];
		tl.ball.forEach((p, k) => {
			if (p.kind !== "path") {
				return;
			}
			into.push(swerve(p.t0));
			const next = tl.ball[k + 1];
			// (Through the net and down on the floor, it bounces: that it
			// comes up off it into his hands is no swerve.)
			const low = (p.pts.at(-1) ?? 0) < 1;
			if (next?.kind === "fly" && "pid" in next.to && !low) {
				off.push(swerve(p.t1));
			}
		});
		const ok = (xs: number[]) => xs.filter((x) => x < 3).length / xs.length;
		assert.isAbove(into.length, 60);
		assert.isAbove(off.length, 20);
		assert.isAbove(ok(into), 0.9);
		assert.isAbove(ok(off), 0.9);
	}, 240_000);

	test("a left-hander's follow-through is held up with his left", () => {
		const SHOTS = new Set(["shoot", "fade", "hook", "floater", "setShot"]);
		const held = { L: 0, R: 0 };
		for (const seed of ["a", "b"]) {
			const { tl } = compile(seed, 150);
			for (const tr of tl.tracks.values()) {
				for (const g of tr.arms) {
					if (g.kind !== "follow") {
						continue;
					}
					const shot = tr.acts.findLast(
						(a) => SHOTS.has(a.anim) && a.t0 < g.t0,
					);
					const st = evalPlayer(tl, tr.pid, g.t0 + (g.t1 - g.t0) * 0.25);
					if (!shot || !st.arm) {
						continue;
					}
					const hand = shot.mirror ? "L" : "R";
					assert.strictEqual(st.arm.hand, hand, `${seed} at ${g.t0}`);
					held[hand] += 1;
				}
				// And at the line: the arm he shot it with.
				for (const a of tr.acts) {
					if (a.anim !== "follow") {
						continue;
					}
					const q = poseOf(evalPlayer(tl, tr.pid, a.t0 + (a.t1 - a.t0) * 0.4));
					const [up, down] = a.mirror ? [q.shF, q.shN] : [q.shN, q.shF];
					assert.isAbove(up, down + 40, `${seed} at ${a.t0}`);
					if (a.mirror) {
						held.L += 1;
					}
				}
			}
		}
		assert.isAbove(held.L, 2);
		assert.isAbove(held.R, 20);
	}, 480_000);

	// A man contesting a shot in close is beside him - an arm's length off
	// where the shooter comes down, his own side of the way in, and round
	// him, a step back, chasing the drive from behind; the man going up for
	// the rebound off it is not up on top of the shooter coming down under
	// it; and anybody else of theirs in his way in - beaten on the drive,
	// walled up in front of it, in the scrum off an offensive board - gives
	// a step to the side as he comes by. (Over eight games: at most one shot
	// in two hundred with a body still in his way.)
	test("a man contesting a shot in close is beside him - not stood in his way, the shooter going up through him", () => {
		const CLOSE = new Set([
			"layup",
			"fingerRoll",
			"powerLayup",
			"scoop",
			"reverseLayup",
			"floater",
		]);
		let shots = 0;
		const into: string[] = [];
		for (const seed of ["a", "b", "c", "d", "e", "f", "g", "h"]) {
			const { tl } = compile(seed, 150);
			const tracks = [...tl.tracks.values()];
			for (const tr of tracks) {
				for (const a of tr.acts) {
					if (!CLOSE.has(a.anim)) {
						continue;
					}
					shots += 1;
					let hit: string | undefined;
					for (let t = a.t0; t <= a.t1 && !hit; t += 40) {
						const S = evalPlayer(tl, tr.pid, t);
						for (const o of tracks) {
							if (o.team === tr.team) {
								continue;
							}
							const Q = evalPlayer(tl, o.pid, t);
							if (Math.hypot(Q.x - S.x, Q.y - S.y) < 1.2) {
								hit = `${seed}: ${tr.pid} and ${o.pid} at ${t}`;
							}
						}
					}
					if (hit) {
						into.push(hit);
					}
				}
			}
		}
		assert.isAbove(shots, 250);
		assert.isAtMost(into.length / shots, 1 / 200, into.join("; "));
	}, 960_000);

	test("a finish at the rim looks the way the play-by-play words it", () => {
		for (const gender of ["male", "female"] as const) {
			const { events, players } = fakeGame(`words-${gender}`, 220);
			const gid = 4242;
			const tl = compileCourt({ events, players, gid, gender });
			const beatOf = new Map(tl.beats.map((b) => [b.i, b]));
			let checked = 0;
			let dunks = 0;
			events.forEach((e, i) => {
				if (e.type !== "fgAtRim" && e.type !== "fgAtRimAndOne") {
					return;
				}
				// Staged from its attempt line on.
				let a = i - 1;
				while (a >= 0 && events[a]!.type !== "fgaAtRim") {
					a -= 1;
				}
				const from = beatOf.get(a)!.preStart;
				const to = beatOf.get(i)!.end;
				const acts = tl.tracks
					.get(e.pid)!
					.acts.filter((x) => x.t0 >= from && x.t0 <= to);
				const dunked = acts.some((x) =>
					["dunk", "dunk1", "tomahawk", "windmill"].includes(x.anim),
				);
				const finish = finishOf(e, gid, gender);
				assert.strictEqual(
					dunked,
					finish === "dunk" || finish === "poster",
					`line ${i}`,
				);
				assert.strictEqual(
					acts.some((x) => LAYUPS.has(x.anim)),
					finish === "layup",
					`line ${i}`,
				);
				checked += 1;
				dunks += dunked ? 1 : 0;
			});
			assert.isAbove(checked, 5);
			if (gender === "female") {
				assert.isBelow(dunks, 2);
			} else {
				assert.isAbove(dunks, 0);
			}
		}
	}, 240_000);

	// A man fed at the rim from out on the floor is hit on his way in - on a
	// cut, a roll, a run down the floor - not stood under it waiting on the
	// ball while the man with it gets out to the arc to throw it in to him.
	// (That dump-in, a man parked at the rim and scoring every time, was
	// the one possession a viewer kept noticing.)
	test("a pass in to a man at the rim finds him on his way there - not parked under it", () => {
		const FINISH = new Set<string>([
			"dunk",
			"dunk1",
			"tomahawk",
			"windmill",
			...LAYUPS,
		]);
		const rims = [0, 1].map((side) => ({
			x: rimX(side as 0 | 1),
			y: COURT_H / 2,
		}));
		let fed = 0;
		let parked = 0;
		for (const seed of ["a", "b", "c", "d"]) {
			const { tl } = compile(seed, 150);
			tl.ball.forEach((s, i) => {
				if (s.kind !== "fly" || !("pid" in s.to)) {
					return;
				}
				// (A bounce pass comes up off the floor: from whoever threw it
				// down there.)
				const thrown =
					"pid" in s.from
						? s
						: tl.ball.find((q) => q.kind === "fly" && q.t1 === s.t0);
				if (
					thrown?.kind !== "fly" ||
					!("pid" in thrown.from) ||
					thrown.from.pid === s.to.pid
				) {
					return;
				}
				const catcher = s.to.pid;
				const C = evalPlayer(tl, catcher, s.t1);
				const F = evalPlayer(tl, thrown.from.pid, thrown.t0);
				const rim = rims.reduce((a, b) =>
					Math.hypot(C.x - a.x, C.y - a.y) < Math.hypot(C.x - b.x, C.y - b.y)
						? a
						: b,
				);
				if (
					Math.hypot(C.x - rim.x, C.y - rim.y) > 8 ||
					Math.hypot(F.x - rim.x, F.y - rim.y) < 21
				) {
					return;
				}
				// He goes up with it at the rim, there and then.
				const shot = tl.ball
					.slice(i + 1, i + 6)
					.find(
						(q) =>
							q.kind === "fly" &&
							"pid" in q.from &&
							q.from.pid === catcher &&
							!("pid" in q.to) &&
							q.t0 - s.t1 < 2500,
					);
				if (
					!shot ||
					!tl.tracks
						.get(catcher)!
						.acts.some(
							(a) =>
								FINISH.has(a.anim) &&
								a.t0 <= shot.t0 + 50 &&
								a.t1 >= shot.t0 - 50,
						)
				) {
					return;
				}
				fed += 1;
				// How long he had been stood on the spot he caught it on.
				let stood = 0;
				for (let w = 50; w <= 3000; w += 50) {
					const Q = evalPlayer(tl, catcher, s.t1 - w);
					if (Math.hypot(Q.x - C.x, Q.y - C.y) > 1.5) {
						break;
					}
					stood = w;
				}
				if (stood >= 600) {
					parked += 1;
				}
			});
		}
		assert.isAbove(fed, 8);
		assert.isAtMost(parked, fed * 0.15, `${parked} of ${fed} parked`);
	}, 960_000);

	// A handoff he goes on from - a drive, a run with it - is taken on the
	// move: in his hands as he comes by, and him on his way with it round
	// the man who gave it, not stood there with it first (and then a
	// crossover in place, the handoff done for nothing).
	test("a handoff is taken on the move", () => {
		const ons: number[] = [];
		for (const seed of ["a", "b", "c", "d"]) {
			const { tl } = compile(seed, 150);
			tl.ball.forEach((s, i) => {
				if (
					s.kind !== "fly" ||
					!("pid" in s.from) ||
					!("pid" in s.to) ||
					s.from.pid === s.to.pid ||
					s.t1 - s.t0 > 200
				) {
					return;
				}
				const giver = s.from.pid;
				const recv = s.to.pid;
				const G = evalPlayer(tl, giver, s.t0);
				const C = evalPlayer(tl, recv, s.t1);
				if (
					Math.hypot(G.x - C.x, G.y - C.y) > 3.5 ||
					!tl.tracks
						.get(giver)!
						.acts.some(
							(a) =>
								a.anim === "pass" && a.t0 <= s.t0 + 20 && a.t1 >= s.t0 - 20,
						)
				) {
					return;
				}
				// Gone 3 ft on with it - before he lets it go - and how soon.
				const gone = tl.ball
					.slice(i + 1)
					.find((z) => !(z.kind === "hold" && z.pid === recv));
				const until = Math.min(gone?.t0 ?? Infinity, s.t1 + 2500);
				for (let w = 0; s.t1 + w <= until; w += 25) {
					const Q = evalPlayer(tl, recv, s.t1 + w);
					if (Math.hypot(Q.x - C.x, Q.y - C.y) >= 3) {
						ons.push(w);
						return;
					}
				}
			});
		}
		ons.sort((a, b) => a - b);
		assert.isAbove(ons.length, 15);
		assert.isAtMost(ons[Math.floor(ons.length / 2)]!, 700, `${ons}`);
	}, 960_000);

	// From the camera (across the floor from the far sideline), a defender
	// right in front of a shooter would hide his whole shot: he meets it from
	// the side instead.
	test("a contested jumper stays in sight", () => {
		for (const seed of ["a", "b", "c"]) {
			const { tl } = compile(seed);
			const tracks = [...tl.tracks.values()];
			let checked = 0;
			for (const tr of tracks) {
				for (const a of tr.acts) {
					if (a.anim !== "contest") {
						continue;
					}
					const mid = (a.t0 + a.t1) / 2;
					const shooter = tracks.find(
						(o) =>
							o.team !== tr.team &&
							o.acts.some(
								(x) =>
									(x.anim === "shoot" || x.anim === "fade") &&
									x.t0 <= mid &&
									x.t1 >= mid,
							),
					);
					if (!shooter) {
						continue;
					}
					const d = evalPlayer(tl, tr.pid, mid);
					const sh = evalPlayer(tl, shooter.pid, mid);
					if (d.y > sh.y) {
						assert.isAtLeast(Math.abs(d.x - sh.x), 1.4, `${seed} ${mid}`);
					}
					checked += 1;
				}
			}
			assert.isAbove(checked, 3);
		}
	}, 240_000);

	// With the ball in play, at real speed, everybody is on the floor: a
	// corner man spacing or drifting keeps his feet in - not stood behind
	// the baseline or on the sideline - and a defender after a man along
	// the baseline pulls up at it, not carried on out of bounds.
	test("in play, nobody stands out of bounds", () => {
		const out: string[] = [];
		let checked = 0;
		for (const seed of ["a", "b", "c"]) {
			const { tl } = compile(seed);
			const since = new Map<number, number>();
			for (const t of sampleTimes(tl, 100)) {
				const b = evalBall(tl, t, bodyFor);
				const H =
					b.holder === undefined ? undefined : evalPlayer(tl, b.holder, t);
				const live =
					fastAt(tl, t) <= 1.001 &&
					H !== undefined &&
					H.x > 0 &&
					H.x < COURT_W &&
					H.y > 0 &&
					H.y < COURT_H;
				for (const tr of tl.tracks.values()) {
					const p = live ? evalPlayer(tl, tr.pid, t) : undefined;
					// (Past a line - but not one on his way on or off from the
					// bench, out by the table.)
					const by = p ? Math.max(-p.x, p.x - COURT_W, -p.y, p.y - COURT_H) : 0;
					if (p?.shown && by > 0.3 && by < 2.5) {
						const t0 = since.get(tr.pid) ?? t;
						since.set(tr.pid, t0);
						if (t - t0 === 500) {
							out.push(
								`${seed} pid ${tr.pid} at ${t0}: ${p.x.toFixed(1)},${p.y.toFixed(1)} ${p.anim}`,
							);
						}
					} else {
						since.delete(tr.pid);
					}
					checked += p?.shown ? 1 : 0;
				}
			}
		}
		assert.isAbove(checked, 50_000);
		assert.isAtMost(out.length, 1, out.join("; "));
	}, 480_000);

	test("a free throw has the shooter's routine", () => {
		for (const seed of ["a", "b"]) {
			const { events, tl } = compile(seed);
			let checked = 0;
			for (const b of tl.beats) {
				const e = events[b.i]!;
				if (e.type !== "ft" && e.type !== "missFt") {
					continue;
				}
				const pid = e.pid as number;
				const acts = tl.tracks
					.get(pid)!
					.acts.filter((a) => a.t0 >= b.preStart && a.t0 <= b.actionStart);
				const shot = acts.find((a) => a.anim === "setShot");
				const follow = acts.find((a) => a.anim === "follow");
				assert.isDefined(shot, `${seed} ${b.i}`);
				assert.isDefined(follow, `${seed} ${b.i}`);
				assert.isAtLeast(follow!.t1, b.actionStart, `${seed} ${b.i}`);
				// The ball: from the official's hands, a dribble, then up.
				const segs = tl.ball.filter(
					(g) => g.t0 >= b.preStart && g.t0 < shot!.t0,
				);
				assert.isTrue(
					segs.some(
						(g) => g.kind === "hold" && g.pid === pid && g.style === "dribble",
					),
					`${seed} ${b.i}`,
				);
				const bounce = segs.find(
					(g) =>
						g.kind === "fly" &&
						!("pid" in g.from) &&
						!("pid" in g.to) &&
						g.from.z > 3,
				);
				assert.isDefined(bounce, `${seed} ${b.i}`);
				// Bounced in from under the basket, to a shooter behind the line.
				const team = tl.tracks.get(pid)!.team;
				const depth = (x: number) => (team === 0 ? x : COURT_W - x);
				if (bounce?.kind === "fly" && !("pid" in bounce.from)) {
					assert.isBelow(depth(bounce.from.x), 8, `${seed} ${b.i}`);
				}
				const st = evalPlayer(tl, pid, shot!.t0);
				assert.isAbove(depth(st.x), FT_LINE_DEPTH + 0.8, `${seed} ${b.i}`);
				checked += 1;
			}
			assert.isAbove(checked, 4);
		}
	}, 240_000);

	// At the tip-off both teams stand ready, nobody on offense or defense
	// yet, till the ball is tipped - nobody gives away who wins it.
	test("the tip-off gives nothing away before the ball is tipped", () => {
		// (A game where the winners would otherwise set off early.)
		const { tl } = compile("arena-lab", 70);
		const toss = tl.fx.find((f) => f.kind === "toss")!.t;
		const [, tip] = tl.jumps![0]!;
		assert.isAbove(tip, toss);
		for (const tr of tl.tracks.values()) {
			const at = evalPlayer(tl, tr.pid, toss);
			if (!at.shown) {
				continue;
			}
			for (let t = toss; t < tip; t += 40) {
				const st = evalPlayer(tl, tr.pid, t);
				// Nobody on defense yet, and nobody off before the tip.
				assert.notInclude(
					[
						"stance",
						"guard",
						"slide",
						"shuffle",
						"walk",
						"jog",
						"run",
						"sprint",
					],
					st.anim,
					`${tr.pid} at ${t}`,
				);
				assert.isBelow(
					Math.hypot(st.x - at.x, st.y - at.y),
					0.05,
					`${tr.pid} at ${t}`,
				);
			}
		}
	});

	// Running, sliding, dribbling, a man still talks with a hand: the
	// screener's man points out the screen, a switch or a man getting back
	// calls out who he has, an open man puts a hand up for it - and a shooter
	// holds his follow-through up till his shot gets there. One arm - never
	// the one on the ball - and the rest of him goes on as it was.
	test("players talk with a hand on the move", () => {
		const { tl } = compile("a", 140);
		const kinds = {
			point: 0,
			hand: 0,
			wave: 0,
			slap: 0,
			follow: 0,
			bar: 0,
			three: 0,
			fist: 0,
			flex: 0,
			pound: 0,
			head: 0,
		};
		let onTheMove = 0;
		for (const tr of tl.tracks.values()) {
			tr.arms.forEach((g, i) => {
				kinds[g.kind] += 1;
				assert.isAbove(g.t1, g.t0);
				// One thing at a time.
				if (i > 0) {
					assert.isAtLeast(g.t0, tr.arms[i - 1]!.t1);
				}
				const st = evalPlayer(tl, tr.pid, (g.t0 + g.t1) / 2);
				if (!st.arm) {
					return;
				}
				const q = poseOf(st);
				const bare = poseOf({ ...st, arm: undefined });
				if (st.arm.both) {
					// (Both hands up on his head: both arms, his legs as they were.)
					for (const k of ["hipN", "kneeN", "hipF", "kneeF", "lean"] as const) {
						assert.strictEqual(q[k], bare[k], k);
					}
					assert.isAbove(q.onHeadN, bare.onHeadN);
					assert.isAbove(q.onHeadF, bare.onHeadF);
					assert.isNotTrue(st.holding);
					assert.isUndefined(st.dribbleHand);
					if (st.moving) {
						onTheMove += 1;
					}
					return;
				}
				const still =
					st.arm.hand === "R"
						? (["shF", "elF", "abF", "wrF"] as const)
						: (["shN", "elN", "abN", "wrN"] as const);
				for (const k of [
					"hipN",
					"kneeN",
					"hipF",
					"kneeF",
					"lean",
					...still,
				] as const) {
					assert.strictEqual(q[k], bare[k], k);
				}
				const moved = st.arm.hand === "R" ? q.shN - bare.shN : q.shF - bare.shF;
				assert.notStrictEqual(moved, 0);
				assert.notStrictEqual(st.arm.hand, st.dribbleHand);
				assert.isNotTrue(st.holding);
				if (st.moving) {
					onTheMove += 1;
				}
			});
		}
		assert.isAtLeast(kinds.point, 40);
		assert.isAtLeast(kinds.hand, 40);
		assert.isAtLeast(kinds.wave, 2);
		// Two to a substitution: going on and coming off, they slap hands.
		const subs = tl.beats.filter((b) => b.type === "sub").length;
		assert.isAtLeast(kinds.slap, subs);
		assert.isAtLeast(kinds.follow, 30);
		assert.isAtLeast(onTheMove, 60);
	}, 240_000);

	// A close game late in the fourth (or in overtime) has the building on
	// its feet; the first three periods never do.
	test("a tight finish has the crowd on its feet", () => {
		let tense = 0;
		for (const seed of ["e", "f", "j", "n"]) {
			const { events, tl } = compile(seed);
			const fourth = tl.beats.find((b) => {
				const e = events[b.i]!;
				return e.type === "period" && e.period === 4;
			})!;
			for (const [t, level] of tl.tension) {
				if (level > 0) {
					assert.isAtLeast(t, fourth.preStart, seed);
				}
			}
			const close = tl.tension.find(([, level]) => level >= 1);
			if (close) {
				tense += 1;
				const t = close[0] + 50;
				assert.strictEqual(tensionAt(tl, t), 1);
				assert.isAtLeast(crowdAt(tl, t, 0).up, 0.9);
			}
		}
		assert.isAbove(tense, 1);
	}, 480_000);
});
