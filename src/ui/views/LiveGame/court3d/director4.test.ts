import { assert, describe, test } from "vitest";
import {
	BALL_R,
	FLOW_MS,
	evalBall,
	evalPlayer,
	fastAt,
	offenseAt,
	poseOf,
} from "./evaluate.ts";
import { attackDir, COURT_H, COURT_W, RIM_R, RIM_Z, rimX } from "./geometry.ts";
import { runMs } from "./motion.ts";
import { compile } from "./testGame.ts";
import { bodyFor, cutBetween, sampleTimes } from "./directorTesting.ts";

describe("3D director", () => {
	// Shots are thrown, not floated: under gravity a three from the arc
	// climbs to about fifteen feet, the way real ones do.
	test("a three arcs like a real one", () => {
		const { tl } = compile("arcs", 200);
		let threes = 0;
		for (const seg of tl.ball) {
			if (seg.kind !== "fly" || !("pid" in seg.from) || "pid" in seg.to) {
				continue;
			}
			const a = evalBall(tl, seg.t0, bodyFor);
			const b = evalBall(tl, seg.t1, bodyFor);
			const across = Math.hypot(b.x - a.x, b.y - a.y);
			if (across < 22 || b.z < 9.5) {
				continue;
			}
			threes += 1;
			let apex = 0;
			for (let t = seg.t0; t <= seg.t1; t += 10) {
				apex = Math.max(apex, evalBall(tl, t, bodyFor).z);
			}
			assert.isAbove(apex, 13, "a three gets up");
			assert.isBelow(apex, 19, "a three is not a moonball");
		}
		assert.isAbove(threes, 0);
	}, 240_000);

	// Off the ball nobody stands rooted to his spot: a man out on the
	// perimeter drifts along the arc - staying behind the line - and his man,
	// standing off him, slides with him.
	test("off the ball, shooters drift along the arc and their men go too", () => {
		const { tl } = compile("drift", 140);
		const tracks = [...tl.tracks.values()];
		let drifts = 0;
		let followed = 0;
		for (const tr of tracks) {
			for (const m of tr.moves) {
				if (m.anim !== "drift") {
					continue;
				}
				drifts += 1;
				// Out on the perimeter he stays behind the line; a big in close
				// never shifts into the lane.
				const depthOf = (p: { x: number }) =>
					tr.team === 0 ? p.x : COURT_W - p.x;
				const outside = (p: { x: number; y: number }) => {
					const depth = depthOf(p);
					return depth < 14
						? Math.abs(p.y - 25) - 22
						: Math.hypot(depth - 5.25, p.y - 25) - 23.75;
				};
				if (outside(m.from) > 0) {
					assert.isAbove(
						outside(m.to),
						0.5,
						`drift inside the line at ${m.t0}`,
					);
				} else if (depthOf(m.to) < 19 && Math.abs(m.to.y - 25) < 8) {
					// (Only back to where he has to be - to set a screen, to
					// catch it - having stepped out a moment.)
					const before = tr.moves[tr.moves.indexOf(m) - 1];
					assert.strictEqual(before?.anim, "drift", `into the lane at ${m.t0}`);
					assert.deepEqual(before?.from, m.to);
				}
				// The man guarding him - the nearest of the other team - goes the
				// same way he does.
				const me = evalPlayer(tl, tr.pid, m.t0);
				let guard: number | undefined;
				let best = 12;
				for (const o of tracks) {
					const st = evalPlayer(tl, o.pid, m.t0);
					const d = Math.hypot(st.x - me.x, st.y - me.y);
					if (o.team !== tr.team && st.shown && d < best) {
						best = d;
						guard = o.pid;
					}
				}
				if (guard !== undefined) {
					const a = evalPlayer(tl, guard, m.t0);
					const b = evalPlayer(tl, guard, m.t1 + 400);
					const L = Math.hypot(m.to.x - m.from.x, m.to.y - m.from.y);
					const along =
						((b.x - a.x) * (m.to.x - m.from.x) +
							(b.y - a.y) * (m.to.y - m.from.y)) /
						L;
					if (along > L * 0.3) {
						followed += 1;
					}
				}
			}
		}
		assert.isAbove(drifts, 40);
		// (A man already on the move with the play goes his own way.)
		assert.isAbove(followed, drifts * 0.25);
	}, 240_000);

	// Dribbling where he stands, waiting on the set, a man works his man -
	// a steady dribble, moves where he stands, the ball walked a few steps -
	// and does not lunge at him and back out, over and over.
	test("a man dribbling in place does not keep lunging at his man and backing out", () => {
		let lunges = 0;
		let close = 0;
		for (const seed of ["a", "b"]) {
			const { tl } = compile(seed, 140);
			for (const tr of tl.tracks.values()) {
				let last = -Infinity;
				const ms = tr.moves;
				for (let i = 0; i + 1 < ms.length; i++) {
					const a = ms[i]!;
					const b = ms[i + 1]!;
					if (a.anim !== "dribble" || fastAt(tl, a.t0) > 1.001) {
						continue;
					}
					const L = Math.hypot(a.to.x - a.from.x, a.to.y - a.from.y);
					const home = Math.hypot(b.to.x - a.from.x, b.to.y - a.from.y);
					if (L < 2 || L > 6 || b.t0 - a.t1 > 700 || home > 0.5) {
						continue;
					}
					lunges += 1;
					if (a.t0 - last < 5000) {
						close += 1;
					}
					last = a.t0;
				}
			}
		}
		assert.isBelow(lunges, 30);
		assert.isAtMost(close, 2);
	}, 480_000);

	test("nobody slides off across the floor still set in a screen or a celebration", () => {
		const inPlace = new Set([
			"screen",
			"postUp",
			"protest",
			"hips",
			"point",
			"flex",
			"celebrate",
			"highFive",
			"lowFive",
		]);
		let planted = 0;
		for (const seed of ["a", "b"]) {
			const { tl } = compile(seed, 140);
			for (const tr of tl.tracks.values()) {
				for (const a of tr.acts) {
					if (!inPlace.has(a.anim)) {
						continue;
					}
					planted += 1;
					for (const m of tr.moves) {
						const d = Math.hypot(m.to.x - m.from.x, m.to.y - m.from.y);
						if (d > 1.5 && m.t1 > a.t0 + 1 && m.t0 < a.t1 - 1) {
							assert.fail(
								`${seed}: ${tr.pid} ${a.anim} ${a.t0}-${a.t1} runs ${m.t0}-${m.t1}`,
							);
						}
					}
				}
			}
		}
		assert.isAbove(planted, 50);
	}, 240_000);

	// Nobody just walks off after a play: the scorer points out the man who
	// found him, a call against a man gets words with the official, and at
	// the buzzer the winners celebrate while the losers stand there, hands
	// on their hips.
	test("a basket, a call, the buzzer: the players react", () => {
		const { tl } = compile("a", 140);
		const count = (anim: string) =>
			[...tl.tracks.values()].reduce(
				(n, tr) => n + tr.acts.filter((a) => a.anim === anim).length,
				0,
			);
		// (A finger at the man who found him - or, a basket of his own, at
		// the bench - on his way back down the floor.)
		const points = [...tl.tracks.values()].reduce(
			(n, tr) =>
				n +
				tr.arms.filter(
					(g) =>
						g.kind === "point" && (g.easy === true || typeof g.at !== "number"),
				).length,
			0,
		);
		assert.isAtLeast(count("point") + points, 15);
		assert.isAtLeast(count("protest"), 4);
		const over = tl.beats.at(-1)!;
		const at = (anim: string) =>
			[...tl.tracks.values()].filter((tr) =>
				tr.acts.some((a) => a.anim === anim && a.t0 >= over.preStart),
			);
		const winners = at("celebrate");
		const losers = at("hips");
		assert.strictEqual(winners.length, 5);
		assert.isAtLeast(losers.length, 1);
		assert.isTrue(losers.every((tr) => tr.team !== winners[0]!.team));
	}, 240_000);

	// A change with his side to defend: the man going off points out the man
	// he had for the man coming on (and the man coming on, now and then, at
	// him too) - never with his side to attack.
	test("a man going off on defense points out his man for the man coming on", () => {
		let defending = 0;
		let told = 0;
		let got = 0;
		const wrong: string[] = [];
		for (const seed of ["a", "b"]) {
			const { tl, events } = compile(seed, 140);
			for (const b of tl.beats) {
				if (b.type !== "sub" || fastAt(tl, b.actionStart) > 1.001) {
					continue;
				}
				const e = events[b.i]!;
				// (Who has it when play goes on: the side of the next man to
				// shoot it or lose it - or not of the next to take it off them,
				// or to foul.)
				let attack: number | undefined;
				for (const x of events.slice(b.i + 1)) {
					const tr =
						typeof x.pid === "number" ? tl.tracks.get(x.pid) : undefined;
					if (!tr) {
						if (
							/^(period|overtime|endOfPeriod|gameOver|jumpBall)$/.test(x.type)
						) {
							break;
						}
						continue;
					}
					if (/^(fga|ft$|missFt$|tov$)/.test(x.type)) {
						attack = tr.team;
						break;
					}
					if (/^(stl$|pf)/.test(x.type)) {
						attack = 1 - tr.team;
						break;
					}
				}
				const on = (e.pids ?? []) as number[];
				((e.pidsOff ?? []) as number[]).forEach((off, j) => {
					const tr = tl.tracks.get(off)!;
					const at = (pid: number | undefined) =>
						(pid === undefined ? undefined : tl.tracks.get(pid))?.arms.filter(
							(g) =>
								g.kind === "point" &&
								typeof g.at === "number" &&
								tl.tracks.get(g.at)?.team !== tr.team &&
								g.t0 >= b.actionStart &&
								g.t0 < b.actionStart + 6000,
						) ?? [];
					const pts = at(off);
					if (attack === undefined) {
						return;
					}
					if (tr.team === attack) {
						if (pts.length > 0) {
							wrong.push(`${seed}: ${off} at ${Math.round(b.actionStart)}`);
						}
						return;
					}
					defending += 1;
					told += pts.length > 0 ? 1 : 0;
					got += at(on[j]).length > 0 ? 1 : 0;
				});
			}
		}
		assert.isAbove(defending, 15);
		assert.isAtLeast(told, defending * 0.35);
		assert.isAbove(got, 3);
		assert.deepEqual(wrong, []);
	}, 480_000);

	// Fouled on his shot, he now and then claps it (and, at the rim, has a
	// word about it); the man who fouled him, his hands on his hips or his
	// head - stood there for it, never on the move.
	test("a shooting foul: the man fouled claps it now and then, stood there for it", () => {
		const REACT = new Set(["clap", "hips", "headHands"]);
		let fouls = 0;
		let reacted = 0;
		for (const seed of ["a", "b", "c", "d"]) {
			const { tl, events } = compile(seed, 140);
			tl.beats.forEach((b, k) => {
				if (b.type !== "pfFG" && b.type !== "pfTP") {
					return;
				}
				const fga = tl.beats.slice(0, k).findLast((x) => /^fga/.test(x.type));
				const shooter = fga && events[fga.i]?.pid;
				const fouler = events[b.i]?.pid;
				if (typeof shooter !== "number" || typeof fouler !== "number") {
					return;
				}
				fouls += 1;
				for (const pid of [shooter, fouler]) {
					const tr = tl.tracks.get(pid)!;
					for (const a of tr.acts) {
						if (
							!REACT.has(a.anim) ||
							a.t0 < b.actionStart ||
							a.t0 > b.actionStart + 2500
						) {
							continue;
						}
						reacted += 1;
						assert.isFalse(
							tr.moves.some((m) => m.t1 > a.t0 && m.t0 < a.t1),
							`${seed} ${pid} ${a.anim} at ${Math.round(a.t0)}`,
						);
						const mid = evalPlayer(tl, pid, (a.t0 + a.t1) / 2);
						assert.strictEqual(mid.anim, a.anim, `${seed} ${pid}`);
					}
				}
			});
		}
		assert.isAbove(fouls, 10);
		assert.isAtLeast(reacted, 3);
	}, 480_000);

	// The ball taken off him, the man who lost it now and then has an arm
	// up at the official, or his hands on his head on his way back.
	test("a steal: the man who lost it now and then wants the call, or holds his head", () => {
		let steals = 0;
		const said: string[] = [];
		for (const seed of ["a", "b"]) {
			const { tl, events } = compile(seed, 140);
			for (const b of tl.beats) {
				const victim = events[b.i]?.pidTov;
				if (b.type !== "stl" || typeof victim !== "number") {
					continue;
				}
				steals += 1;
				const g = tl.tracks
					.get(victim)!
					.arms.find(
						(x) =>
							(x.kind === "hand" || x.kind === "head") &&
							x.t0 >= b.actionStart &&
							x.t0 < b.actionStart + 1000,
					);
				if (g) {
					said.push(g.kind);
				}
			}
		}
		assert.isAbove(steals, 10);
		assert.isAtLeast(said.length, 3);
		assert.includeMembers(said, ["hand", "head"]);
	}, 480_000);

	// Out of bounds, whose ball: the men nearest it now and then point the
	// way they say it goes - their own way.
	test("the ball out of bounds: the men nearest point their own way, now and then", () => {
		let outs = 0;
		let claims = 0;
		for (const seed of ["a", "b"]) {
			const { tl } = compile(seed, 140);
			for (const f of tl.fx) {
				if (f.kind !== "whistle" || f.call !== "out") {
					continue;
				}
				outs += 1;
				for (const tr of tl.tracks.values()) {
					for (const g of tr.arms) {
						if (
							g.kind !== "point" ||
							typeof g.at !== "object" ||
							g.t0 < f.t ||
							g.t0 > f.t + 1500
						) {
							continue;
						}
						claims += 1;
						const P = evalPlayer(tl, tr.pid, g.t0);
						assert.strictEqual(
							Math.sign(g.at.x - P.x),
							attackDir(tr.team),
							`${seed} ${tr.pid} at ${Math.round(g.t0)}`,
						);
					}
				}
			}
		}
		assert.isAbove(outs, 8);
		assert.isAtLeast(claims, outs * 0.3);
	}, 480_000);

	// Missed in close - at the rim, a hook, a floater - and not the man who
	// gets it, the shooter isn't left stood where he landed: he follows it in,
	// fights a man of theirs for it, or backs out.
	test("after his miss in close the shooter plays it, not stood where he landed", () => {
		let misses = 0;
		let stood = 0;
		for (const seed of ["a", "b", "c"]) {
			const { tl, events } = compile(seed, 140);
			tl.beats.forEach((b, k) => {
				if (
					!/^miss(AtRim|LowPost)$/.test(b.type) ||
					fastAt(tl, b.actionStart) > 1.001
				) {
					return;
				}
				const fga = tl.beats.slice(0, k).findLast((x) => /^fga/.test(x.type));
				const shooter = fga && events[fga.i]?.pid;
				const next = tl.beats[k + 1];
				if (
					typeof shooter !== "number" ||
					(next && events[next.i]?.pid === shooter)
				) {
					return;
				}
				const tr = tl.tracks.get(shooter)!;
				const shot = tl.ball.findLast(
					(z) =>
						z.kind === "fly" &&
						"pid" in z.from &&
						z.from.pid === shooter &&
						!("pid" in z.to) &&
						z.t0 <= b.actionStart,
				);
				if (shot?.kind !== "fly") {
					return;
				}
				const up = tr.acts.findLast(
					(a) => a.t0 <= shot.t0 + 50 && a.t1 > shot.t0,
				);
				const t0 = (up?.t1 ?? shot.t0 + 400) + 150;
				misses += 1;
				const A = evalPlayer(tl, shooter, t0);
				const B = evalPlayer(tl, shooter, t0 + 900);
				const busy = tr.acts.some((a) => a.t1 > t0 && a.t0 < t0 + 900);
				if (Math.hypot(B.x - A.x, B.y - A.y) < 1 && !busy) {
					stood += 1;
				}
			});
		}
		assert.isAbove(misses, 10);
		assert.isAtMost(stood, misses * 0.15);
	}, 480_000);

	// From one move into the next - pulling up from a run, down into his
	// stance, up for a catch - his body eases over rather than snapping
	// there between one frame and the next.
	test("one move eases into the next", () => {
		const { tl } = compile("ease", 140);
		const pids = [...tl.tracks.keys()];
		const joints = [
			"hipN",
			"kneeN",
			"hipF",
			"kneeF",
			"shN",
			"elN",
			"shF",
			"elF",
			"lean",
		] as const;
		let prev = new Map<number, ReturnType<typeof evalPlayer>>();
		let last = -Infinity;
		let changes = 0;
		let snaps = 0;
		for (const t of sampleTimes(tl, 20)) {
			const cut = cutBetween(tl, last, t);
			last = t;
			const cur = new Map<number, ReturnType<typeof evalPlayer>>();
			for (const pid of pids) {
				const st = evalPlayer(tl, pid, t);
				cur.set(pid, st);
				const p = prev.get(pid);
				if (!p || !p.shown || !st.shown || cut || p.anim === st.anim) {
					continue;
				}
				changes += 1;
				const a = poseOf(p);
				const b = poseOf(st);
				if (Math.max(...joints.map((j) => Math.abs(a[j] - b[j]))) > 40) {
					snaps += 1;
				}
			}
			prev = cur;
		}
		assert.isAbove(changes, 1000);
		assert.isBelow(snaps, changes * 0.05);
	}, 720_000);

	// The picture never cuts in play: the ball taken out after a basket and
	// brought up, the walk to an inbound or to the line, are all played out -
	// fast - rather than skipped. Never fast through a shot, and always five
	// a side out there.
	test("the picture never cuts: it runs through the dead time fast", () => {
		const { events, tl } = compile("cuts", 160);
		assert.strictEqual(tl.cuts.length, 0);
		assert.isAbove(tl.fast.length, 40);
		const pids = [...tl.tracks.keys()];
		let last = -Infinity;
		for (const [a, b] of tl.fast) {
			assert.isAbove(b, a);
			assert.isAtLeast(a, last, "in order, apart");
			last = b;
			for (const bt of tl.beats) {
				assert.isFalse(
					/^(fg|miss|blk|tp)/.test(bt.type) && a < bt.end && b > bt.actionStart,
					`fast through ${bt.type} at ${bt.actionStart}`,
				);
			}
			for (const t of [a, b]) {
				const on = [0, 0];
				for (const pid of pids) {
					const st = evalPlayer(tl, pid, t);
					if (st.shown) {
						on[st.team]! += 1;
					}
				}
				// (And whoever was subbed out, walking off to his bench.)
				for (const n of on) {
					assert.isAtLeast(n, 5, `at ${t}`);
					assert.isAtMost(n, 7, `at ${t}`);
				}
			}
		}
		assert.isAbove(events.length, 0);
	}, 240_000);

	test("a blocked shot is swatted off his hand - down to the floor, or to whoever gets it", () => {
		const { tl } = compile("a", 140);
		let blocks = 0;
		for (const f of tl.fx) {
			if (f.kind !== "block") {
				continue;
			}
			const swat = tl.ball.find(
				(s) => s.kind === "fly" && Math.abs(s.t0 - f.t) < 1 && "pid" in s.from,
			);
			assert.isDefined(swat, `at ${f.t}`);
			if (swat?.kind !== "fly") {
				continue;
			}
			// Off the hand that got it.
			const got = tl.ball.find(
				(s) => s.kind === "fly" && Math.abs(s.t1 - f.t) < 1 && "pid" in s.to,
			);
			assert.deepEqual(
				got?.kind === "fly" ? got.to : undefined,
				swat.from,
				`at ${f.t}`,
			);
			assert.isTrue("pid" in swat.to || swat.to.z < 0.5, `at ${f.t}`);
			blocks += 1;
		}
		assert.isAbove(blocks, 2);
	}, 240_000);

	// The man guarding the shooter - the nearest of theirs to him, on
	// average, over the few seconds before it - stays with him: not off
	// away from him, three feet and more, while he hardly moves (the ball
	// fed in to him in the post, the drive pulled up), only to come back
	// at him for the shot. Help off him and back is part of the game, but
	// not that often. (Six games: one shot in eleven, where it was more
	// than one in eleven before.)
	test("the man guarding the shooter is not off away from him and back before the shot", () => {
		let shots = 0;
		const back: string[] = [];
		for (const seed of ["a", "b", "c", "d", "e", "f"]) {
			const { tl } = compile(seed, 140);
			for (const s of tl.ball) {
				if (s.kind !== "fly" || !("pid" in s.from) || "pid" in s.to) {
					continue;
				}
				const S = s.from.pid;
				const rel = s.t0;
				const team = tl.tracks.get(S)?.team;
				if (fastAt(tl, rel) > 1 || team === undefined) {
					continue;
				}
				let D: number | undefined;
				let best = 6;
				for (const o of tl.tracks.values()) {
					if (o.team === team || o.pid < 0) {
						continue;
					}
					let sum = 0;
					let k = 0;
					for (let t = rel - 3500; t <= rel; t += 250) {
						const a = evalPlayer(tl, S, t);
						const d = evalPlayer(tl, o.pid, t);
						if (!d.shown) {
							sum = Infinity;
							break;
						}
						sum += Math.hypot(a.x - d.x, a.y - d.y);
						k += 1;
					}
					if (sum / k < best) {
						best = sum / k;
						D = o.pid;
					}
				}
				if (D === undefined) {
					continue;
				}
				shots += 1;
				const apart: number[] = [];
				const man: { x: number; y: number }[] = [];
				for (let t = rel - 3500; t <= rel; t += 100) {
					const a = evalPlayer(tl, S, t);
					const d = evalPlayer(tl, D, t);
					apart.push(Math.hypot(a.x - d.x, a.y - d.y));
					man.push({ x: a.x, y: a.y });
				}
				let off: string | undefined;
				for (let i = 0; i < apart.length && !off; i++) {
					for (let j = i + 3; j < apart.length && j <= i + 20 && !off; j++) {
						if (
							apart[j]! - apart[i]! < 3 ||
							Math.hypot(man[j]!.x - man[i]!.x, man[j]!.y - man[i]!.y) > 2.5
						) {
							continue;
						}
						for (let k = j + 1; k < apart.length && k <= j + 20; k++) {
							if (apart[j]! - apart[k]! >= 3) {
								off = `${seed}: ${D} off ${S} at ${Math.round(rel - 3500 + j * 100)}`;
								break;
							}
						}
					}
				}
				if (off) {
					back.push(off);
				}
			}
		}
		assert.isAbove(shots, 400);
		assert.isAtMost(back.length / shots, 1 / 11, back.join("; "));
	}, 600_000);

	// Out on the floor with it, a man does not get by the man guarding him
	// - the nearest of theirs to him - for nothing: level with him or past
	// him on the way to the rim for a beat and more, no screen in it, only
	// for him to be back in front of him again, the man with the ball still
	// out there (picked up coming at him, he ran on past him; or expected to
	// pull up at the end of a run he went straight on from). (Four games:
	// eight or nine a game at most, where it was twelve and more before.)
	test("the man with the ball does not get by his man for nothing", () => {
		let times = 0;
		const by: string[] = [];
		for (const seed of ["a", "b", "c", "d"]) {
			const { tl } = compile(seed, 140);
			const tracks = [...tl.tracks.values()];
			let run: { t0: number; h: number; d: number } | undefined;
			for (let t = 0; t < tl.end; t += 50) {
				const h =
					fastAt(tl, t) > 1.001 ? undefined : evalBall(tl, t, bodyFor).holder;
				const H = h === undefined ? undefined : evalPlayer(tl, h, t);
				const team = h === undefined ? undefined : tl.tracks.get(h)?.team;
				if (
					!H ||
					h === undefined ||
					team === undefined ||
					!H.shown ||
					!/^dribble/.test(H.anim)
				) {
					run = undefined;
					continue;
				}
				const rim = { x: rimX(team), y: COURT_H / 2 };
				const dr = Math.hypot(rim.x - H.x, rim.y - H.y);
				if (dr < 15 || (H.x - COURT_W / 2) * (rim.x - COURT_W / 2) < 0) {
					run = undefined;
					continue;
				}
				let d: number | undefined;
				let near = 9;
				for (const o of tracks) {
					const q = evalPlayer(tl, o.pid, t);
					if (o.team !== team && o.pid >= 0 && q.shown) {
						const g = Math.hypot(q.x - H.x, q.y - H.y);
						if (g < near) {
							near = g;
							d = o.pid;
						}
					}
				}
				if (d === undefined) {
					run = undefined;
					continue;
				}
				const G = evalPlayer(tl, d, t);
				if (
					(G.x - H.x) * (rim.x - H.x) + (G.y - H.y) * (rim.y - H.y) >=
					0.5 * dr
				) {
					run = undefined;
					continue;
				}
				if (!run || run.h !== h || run.d !== d) {
					run = { t0: t, h, d };
				}
				if (t - run.t0 !== 400) {
					continue;
				}
				// (Coming off a screen is another thing.)
				const t0 = run.t0;
				const screened = tracks.some(
					(o) =>
						o.team === team &&
						o.pid !== h &&
						o.acts.some(
							(a) => a.anim === "screen" && a.t1 > t0 - 2000 && a.t0 < t,
						) &&
						(() => {
							const q = evalPlayer(tl, o.pid, t0);
							return Math.hypot(q.x - H.x, q.y - H.y) < 10;
						})(),
				);
				if (screened) {
					continue;
				}
				times += 1;
				for (let u = t; u < t + 2500; u += 100) {
					if (evalBall(tl, u, bodyFor).holder !== h) {
						break;
					}
					const H2 = evalPlayer(tl, h, u);
					const G2 = evalPlayer(tl, d, u);
					const d2 = Math.hypot(rim.x - H2.x, rim.y - H2.y);
					if (d2 < 12) {
						break;
					}
					if (
						((G2.x - H2.x) * (rim.x - H2.x) + (G2.y - H2.y) * (rim.y - H2.y)) /
							d2 >
						2
					) {
						by.push(`${seed}: ${h} by ${d} at ${t0}`);
						break;
					}
				}
			}
		}
		assert.isAbove(times, 40);
		assert.isAtMost(by.length, 34, by.join("; "));
	}, 900_000);

	test("a man backing down in the post backs into his man, not nobody", () => {
		let n = 0;
		const off: string[] = [];
		for (const seed of ["a", "b", "c", "d"]) {
			const { tl } = compile(seed, 140);
			const tracks = [...tl.tracks.values()];
			for (const tr of tracks) {
				const rim = { x: rimX(tr.team), y: COURT_H / 2 };
				for (const m of tr.moves) {
					if (
						m.anim !== "post" ||
						m.t1 - m.t0 < 200 ||
						fastAt(tl, m.t0) > 1.001 ||
						evalBall(tl, (m.t0 + m.t1) / 2, bodyFor).holder !== tr.pid
					) {
						continue;
					}
					n += 1;
					// A man of theirs up against him on the rim side of him.
					let k = 0;
					let on = 0;
					for (let t = m.t0; t <= m.t1; t += 100) {
						k += 1;
						const P = evalPlayer(tl, tr.pid, t);
						on += tracks.some((o) => {
							if (o.team === tr.team || o.pid < 0) {
								return false;
							}
							const q = evalPlayer(tl, o.pid, t);
							return (
								q.shown &&
								Math.hypot(q.x - P.x, q.y - P.y) < 3 &&
								(q.x - P.x) * (rim.x - P.x) + (q.y - P.y) * (rim.y - P.y) > 0
							);
						})
							? 1
							: 0;
					}
					if (on < 0.7 * k) {
						off.push(`${seed}: ${tr.pid} at ${Math.round(m.t0)} (${on}/${k})`);
					}
				}
			}
		}
		assert.isAbove(n, 15);
		assert.deepEqual(off, []);
	}, 900_000);

	test("a man right on a jump shooter does not just stand there watching it go", () => {
		let n = 0;
		const idle: string[] = [];
		for (const seed of ["a", "b"]) {
			const { tl } = compile(seed, 140);
			const tracks = [...tl.tracks.values()];
			for (const s of tl.ball) {
				if (
					s.kind !== "fly" ||
					!("pid" in s.from) ||
					"pid" in s.to ||
					fastAt(tl, s.t0) > 1.001
				) {
					continue;
				}
				const rel = s.t0;
				const shooter = tl.tracks.get(s.from.pid)!;
				if (
					!shooter.acts.some(
						(a) =>
							(a.anim === "shoot" || a.anim === "setShot") &&
							a.t0 <= rel &&
							a.t1 >= rel,
					)
				) {
					continue;
				}
				const S = evalPlayer(tl, shooter.pid, rel);
				for (const o of tracks) {
					const q = evalPlayer(tl, o.pid, rel);
					if (
						o.team === shooter.team ||
						o.pid < 0 ||
						!q.shown ||
						Math.hypot(q.x - S.x, q.y - S.y) > 3.5
					) {
						continue;
					}
					n += 1;
					if (!o.acts.some((a) => a.t1 > rel - 400 && a.t0 < rel + 300)) {
						idle.push(
							`${seed}: ${o.pid} by ${shooter.pid} at ${Math.round(rel)}`,
						);
					}
				}
			}
		}
		assert.isAbove(n, 20);
		assert.deepEqual(idle, []);
	}, 900_000);

	test("an and-one's foul is on the shooter as he lets it go - and nobody fakes, boxes out or fights for position with nobody there", () => {
		const CONTACT = new Set(["hack", "contest", "block", "reach", "bump"]);
		let fouls = 0;
		let fakes = 0;
		const off: string[] = [];
		for (const seed of ["a", "b", "c", "f"]) {
			const { tl, events } = compile(seed, 140);
			const tracks = [...tl.tracks.values()];
			for (const b of tl.beats) {
				const e = events[b.i];
				if (
					!e ||
					typeof e.pidFoul !== "number" ||
					typeof e.pid !== "number" ||
					!/^(fg|tp)/.test(e.type)
				) {
					continue;
				}
				const shooter = e.pid;
				const rel = tl.ball.findLast(
					(s) =>
						s.kind === "fly" &&
						"pid" in s.from &&
						s.from.pid === shooter &&
						!("pid" in s.to) &&
						s.t0 <= b.end,
				);
				if (!rel) {
					continue;
				}
				fouls += 1;
				const S = evalPlayer(tl, shooter, rel.t0);
				const F = evalPlayer(tl, e.pidFoul, rel.t0);
				const near = Math.hypot(S.x - F.x, S.y - F.y);
				const on = tl.tracks
					.get(e.pidFoul)!
					.acts.some(
						(a) =>
							CONTACT.has(a.anim) && a.t1 > rel.t0 - 600 && a.t0 < rel.t0 + 400,
					);
				if (near > 5.5 || !on) {
					off.push(
						`${seed}: ${e.type} by ${e.pidFoul} ${near.toFixed(1)} ft off${on ? "" : ", no hand on him"}`,
					);
				}
			}
			for (const tr of tracks) {
				for (const a of tr.acts) {
					if (
						(a.anim !== "jab" && a.anim !== "shotFake") ||
						fastAt(tl, a.t0) > 1.001 ||
						!evalPlayer(tl, tr.pid, a.t0).shown
					) {
						continue;
					}
					fakes += 1;
					const P = evalPlayer(tl, tr.pid, a.t0);
					const there = tracks.some((o) => {
						const q = evalPlayer(tl, o.pid, a.t0);
						return (
							o.team !== tr.team &&
							o.pid >= 0 &&
							q.shown &&
							Math.hypot(q.x - P.x, q.y - P.y) < 12
						);
					});
					if (!there) {
						off.push(`${seed}: ${a.anim} by ${tr.pid} at ${Math.round(a.t0)}`);
					}
				}
				for (const a of tr.acts) {
					if (
						(a.anim !== "boxOut" && a.anim !== "fight") ||
						fastAt(tl, a.t0) > 1.001 ||
						!evalPlayer(tl, tr.pid, a.t0).shown
					) {
						continue;
					}
					let near = Infinity;
					for (let t = a.t0; t <= a.t1; t += 100) {
						const P = evalPlayer(tl, tr.pid, t);
						for (const o of tracks) {
							const q = evalPlayer(tl, o.pid, t);
							if (o.team !== tr.team && o.pid >= 0 && q.shown) {
								near = Math.min(near, Math.hypot(q.x - P.x, q.y - P.y));
							}
						}
					}
					if (near > 7) {
						off.push(
							`${seed}: ${a.anim} by ${tr.pid} at ${Math.round(a.t0)}, ${near.toFixed(1)} ft from anybody`,
						);
					}
				}
			}
		}
		assert.isAbove(fouls, 8);
		assert.isAbove(fakes, 8);
		assert.deepEqual(off, []);
	}, 900_000);

	// A jumper goes up: each man coming in to the glass has somebody put a
	// body on him - his own man, or the man nearest - once that man is done
	// with what he was doing (a contest, a run he was on), not left to walk
	// in because that took him past the moment the rest went in. (Eight
	// games: four can run a long way either side of it.)
	test("a jumper up, the men going to the glass are boxed out", () => {
		let shots = 0;
		let boxes = 0;
		for (const seed of ["a", "b", "c", "d", "e", "f", "g", "h"]) {
			const { events, tl } = compile(seed, 150);
			const beatOf = new Map(tl.beats.map((b) => [b.i, b]));
			events.forEach((e, i) => {
				const b = beatOf.get(i);
				if (!b || !/^(miss|fg)(Tp|MidRange)$/.test(e.type)) {
					return;
				}
				const fly = tl.ball.findLast(
					(s) =>
						s.kind === "fly" &&
						s.t0 <= b.actionStart + 50 &&
						!("pid" in s.to) &&
						s.to.z > 6,
				);
				if (!fly) {
					return;
				}
				shots += 1;
				for (const tr of tl.tracks.values()) {
					boxes += tr.acts.filter(
						(a) => a.anim === "boxOut" && a.t1 > fly.t0 && a.t0 < fly.t0 + 1500,
					).length;
				}
			});
		}
		assert.isAbove(shots, 100);
		assert.isAtLeast(boxes / shots, 1.3, `${boxes} over ${shots}`);
	}, 960_000);

	test("off the rim, the man who gets it mostly goes up and takes it in the air", () => {
		let air = 0;
		let all = 0;
		// (Eight games: in four, the share swings by several points either
		// way with any change to where men happen to be when it comes off.)
		for (const seed of ["a", "b", "c", "d", "e", "f", "g", "h"]) {
			const { events, tl } = compile(seed, 140);
			const beatOf = new Map(tl.beats.map((b) => [b.i, b]));
			events.forEach((e, i) => {
				if (e.type !== "drb" && e.type !== "orb") {
					return;
				}
				const b = beatOf.get(i);
				const k = tl.ball.findIndex(
					(s) =>
						s.kind === "path" &&
						b &&
						s.t1 <= b.actionStart &&
						s.t1 > b.actionStart - 2600,
				);
				if (!b || k < 0) {
					return;
				}
				const next = tl.ball[k + 1];
				all += 1;
				// (Not snatched up off the floor after a hop.)
				if (
					next?.kind === "fly" &&
					"pid" in next.to &&
					next.to.pid === e.pid &&
					!("z" in next.from && next.from.z < 1)
				) {
					air += 1;
					// Up for it, at the top of his jump.
					const st = evalPlayer(tl, e.pid as number, next.t1);
					assert.isAbove(st.z, 0.2, `${seed} line ${i}`);
				}
			});
		}
		assert.isAbove(all, 160);
		assert.isAbove(air / all, 0.5);
	}, 480_000);

	test("an assisted basket's last pass comes from the man credited with it", () => {
		const { events, tl } = compile("assists", 160);
		const beatOf = new Map(tl.beats.map((b) => [b.i, b]));
		let checked = 0;
		events.forEach((e, i) => {
			if (typeof e.pidAst !== "number" || !/^(fg|tp)/.test(e.type)) {
				return;
			}
			let a = i - 1;
			while (a >= 0 && !events[a]!.type.startsWith("fga")) {
				a -= 1;
			}
			const attempt = beatOf.get(a);
			if (!attempt || events[a]!.pid !== e.pid) {
				return;
			}
			// The last ball into his hands before he gets it off.
			const passes = tl.ball.filter(
				(s) =>
					s.kind === "fly" &&
					"pid" in s.to &&
					s.to.pid === e.pid &&
					s.t1 <= attempt.actionStart + 1500 &&
					s.t0 >= attempt.preStart,
			);
			let last = passes.at(-1);
			assert.isDefined(last, `line ${i}`);
			// A bounce pass comes up off the floor: who threw it down there?
			while (last && last.kind === "fly" && !("pid" in last.from)) {
				const t0: number = last.t0;
				last = tl.ball.find((s) => s.kind === "fly" && s.t1 === t0);
			}
			assert.deepInclude(
				last?.kind === "fly" ? last.from : {},
				{ pid: e.pidAst },
				`line ${i}`,
			);
			checked += 1;
		});
		assert.isAbove(checked, 10);
	}, 240_000);

	// A finish at the rim goes up from in front of the glass: the ball
	// leaves his hand in front of the backboard - not round behind it, where
	// it could only get to the rim through the board - and never rises up
	// through the ring from under it.
	test("a layup leaves his hand in front of the glass", () => {
		// (The glass is this far behind the middle of the rim, feet.)
		const GLASS = 1.25;
		const DUNKS = new Set(["dunk", "dunk1", "tomahawk", "windmill"]);
		let checked = 0;
		for (const seed of ["a", "b", "c", "d"]) {
			const { tl } = compile(seed, 150);
			for (const s of tl.ball) {
				// (Up at the rim - not the game ball thrown up at the horn by
				// whoever has it, wherever he is.)
				if (
					s.kind !== "fly" ||
					!("pid" in s.from) ||
					"pid" in s.to ||
					s.to.z < 8.5 ||
					s.to.z > RIM_Z + 4
				) {
					continue;
				}
				const rx = s.to.x < COURT_W / 2 ? rimX(0) : rimX(1);
				if (Math.hypot(s.to.x - rx, s.to.y - COURT_H / 2) > 3) {
					continue;
				}
				const shooter = s.from.pid;
				const anim = tl.tracks
					.get(shooter)!
					.acts.find((a) => a.t0 <= s.t0 && s.t0 < a.t1 + 50)?.anim;
				if (anim === undefined || DUNKS.has(anim)) {
					continue;
				}
				const b0 = evalBall(tl, s.t0 + 1, bodyFor);
				if (Math.hypot(b0.x - rx, b0.y - COURT_H / 2) > 5) {
					continue;
				}
				checked += 1;
				const front = rx < COURT_W / 2 ? 1 : -1;
				assert.isAtLeast(
					(b0.x - rx) * front,
					BALL_R - GLASS,
					`${seed}: ${anim} by ${shooter} at ${Math.round(s.t0)}`,
				);
				let prev = b0;
				for (let t = s.t0 + 8; t <= s.t1; t += 8) {
					const b = evalBall(tl, t, bodyFor);
					assert.isFalse(
						Math.hypot(b.x - rx, b.y - COURT_H / 2) < RIM_R &&
							prev.z < RIM_Z &&
							b.z >= RIM_Z,
						`${seed}: up through the ring at ${Math.round(t)}`,
					);
					prev = b;
				}
			}
		}
		assert.isAbove(checked, 60);
	}, 960_000);

	// A shot from out on the floor goes up with room round him: off a ball
	// screen he turns the corner, the man who set it rolling away - not up
	// with him still stood at his shoulder.
	test("a jumper goes up with no teammate at the shooter's shoulder", () => {
		let shots = 0;
		const close: string[] = [];
		for (const seed of ["a", "b", "c", "d"]) {
			const { events, tl } = compile(seed, 150);
			for (const b of tl.beats) {
				const e = events[b.i];
				if (!e || !/^fga/.test(e.type) || typeof e.pid !== "number") {
					continue;
				}
				const pid = e.pid;
				const shot = tl.ball.find(
					(s) =>
						s.kind === "fly" &&
						"pid" in s.from &&
						s.from.pid === pid &&
						!("pid" in s.to) &&
						s.to.z > 6 &&
						s.t0 >= b.preStart - 100 &&
						s.t0 <= b.end + 200,
				);
				if (
					shot?.kind !== "fly" ||
					"pid" in shot.to ||
					fastAt(tl, shot.t0 - 1000) > 1
				) {
					continue;
				}
				const team = tl.tracks.get(pid)!.team;
				const S = evalPlayer(tl, pid, shot.t0);
				const rx = shot.to.x < COURT_W / 2 ? rimX(0) : rimX(1);
				if (Math.hypot(S.x - rx, S.y - COURT_H / 2) < 6) {
					continue;
				}
				shots += 1;
				const near = Math.min(
					...[...tl.tracks.values()]
						.filter((tr) => tr.team === team && tr.pid !== pid)
						.map((tr) => evalPlayer(tl, tr.pid, shot.t0))
						.filter((m) => m.shown)
						.map((m) => Math.hypot(m.x - S.x, m.y - S.y)),
				);
				if (near < 4) {
					close.push(`${seed}: ${pid} at ${Math.round(shot.t0)}`);
				}
			}
		}
		assert.isAbove(shots, 200);
		assert.isAtMost(close.length, shots * 0.075, close.join("; "));
	}, 960_000);

	// The man with the ball doesn't stand about with it back in his own half
	// - waiting on a break that never gets going, or for a whistle: it is
	// brought up the floor. (Seconds as they go by on screen.)
	test("nobody stands with the ball in the backcourt", () => {
		for (const seed of ["a", "b", "c", "d"]) {
			const { tl } = compile(seed, 140);
			let worst = 0;
			tl.beats.forEach((b, k) => {
				let run = 0;
				for (let t = tl.beats[k - 1]?.end ?? 0; t < b.actionStart; t += 100) {
					const h = evalBall(tl, t, bodyFor).holder;
					if (h === undefined) {
						run = 0;
						continue;
					}
					const p0 = evalPlayer(tl, h, t);
					const p1 = evalPlayer(tl, h, t + 100);
					const back = (p0.x - COURT_W / 2) * attackDir(offenseAt(tl, t)) < 0;
					const still = Math.hypot(p1.x - p0.x, p1.y - p0.y) < 0.35;
					run = back && still ? run + 100 / fastAt(tl, t) : 0;
					worst = Math.max(worst, run);
				}
			});
			assert.isBelow(worst, 2500, seed);
		}
	}, 960_000);

	// A whistle on a foul that stops play, the man with the ball out on the
	// floor - the set still coming to him - isn't stood there with it
	// waiting on it: he has it on the floor, probing, as it goes. (In close
	// - posting up, the split action going on round him, or caught under
	// the rim - holding it is what he is doing.)
	test("no man with the ball stood waiting on a foul's whistle", () => {
		let fouls = 0;
		const stood: string[] = [];
		for (const seed of ["a", "b", "c", "d"]) {
			const { tl } = compile(seed, 140);
			for (const b of tl.beats) {
				if (b.type !== "pfNonShooting" && b.type !== "pfBonus") {
					continue;
				}
				const t = b.actionStart;
				const at = evalBall(tl, t, bodyFor);
				const h = at.holder;
				if (h === undefined) {
					continue;
				}
				const team = tl.tracks.get(h)!.team;
				const H = evalPlayer(tl, h, t);
				if (
					(H.x - COURT_W / 2) * attackDir(team) < 8 ||
					Math.hypot(H.x - rimX(team), H.y - COURT_H / 2) < 14
				) {
					continue;
				}
				fouls += 1;
				// How long he has had it, within 3 ft of where he is now.
				let s = t;
				while (s > t - 6000) {
					const q = evalPlayer(tl, h, s - 100);
					if (
						evalBall(tl, s - 100, bodyFor).holder !== h ||
						Math.hypot(q.x - H.x, q.y - H.y) > 3
					) {
						break;
					}
					s -= 100;
				}
				if (t - s >= 2000) {
					stood.push(`${seed} ${Math.round(t)} ${h} ${t - s}`);
				}
			}
		}
		assert.isAtLeast(fouls, 20);
		assert.isAtMost(stood.length, 1, stood.join("; "));
	}, 480_000);

	// A shot fake with a man up on him: he comes up out of his stance for
	// it - off his feet if he bites, a hand up if he does not.
	test("a defender up on a shot fake goes up for it", () => {
		const { tl } = compile("a", 140);
		let close = 0;
		let rose = 0;
		for (const tr of tl.tracks.values()) {
			for (const a of tr.acts) {
				if (a.anim !== "shotFake") {
					continue;
				}
				const me = evalPlayer(tl, tr.pid, a.t0);
				let guard: number | undefined;
				let best = 7;
				for (const o of tl.tracks.values()) {
					const st = evalPlayer(tl, o.pid, a.t0);
					const d = Math.hypot(st.x - me.x, st.y - me.y);
					if (o.team !== tr.team && st.shown && d < best) {
						best = d;
						guard = o.pid;
					}
				}
				if (guard === undefined) {
					continue;
				}
				close += 1;
				const o = tl.tracks.get(guard)!;
				const soon = (t: number) => t >= a.t0 && t <= a.t0 + 300;
				if (
					o.acts.some((x) => x.anim === "contest" && soon(x.t0)) ||
					o.arms.some((x) => x.kind === "hand" && soon(x.t0))
				) {
					rose += 1;
				}
			}
		}
		assert.isAbove(close, 15);
		assert.isAbove(rose / close, 0.8);
	}, 240_000);

	test("flat out, a player sprints - bounding off the floor stride to stride; at an easy pace, he jogs", () => {
		const { tl } = compile("a");
		let sprints = 0;
		let jogs = 0;
		const gaitOf = (v: number) => (v >= 19 ? "sprint" : v < 11 ? "jog" : "run");
		for (const [pid, tr] of tl.tracks) {
			for (const [k, m] of tr.moves.entries()) {
				const d = Math.hypot(m.to.x - m.from.x, m.to.y - m.from.y);
				const secs = (m.t1 - m.t0) / 1000;
				if (m.anim !== "run" || d < 20) {
					continue;
				}
				// How fast he goes at his quickest along it: getting going and
				// pulling up take their time, so that is faster than the average.
				const at = (t: number) => {
					const a = evalPlayer(tl, pid, t - 10);
					const b = evalPlayer(tl, pid, t + 10);
					return Math.hypot(b.x - a.x, b.y - a.y) / 0.02;
				};
				const quickest = (t0: number, t1: number) => {
					let v = 0;
					for (
						let t = t0 + (t1 - t0) * 0.35;
						t < t1 - (t1 - t0) * 0.35;
						t += 20
					) {
						v = Math.max(v, at(t));
					}
					return v;
				};
				const fastest = quickest(m.t0, m.t1);
				// (A walk - some run is that slow - is a walk. And right on the
				// line between two gaits, either will do.)
				if (fastest < 6.25) {
					continue;
				}
				// (So too if, running on into it from the run before or on out
				// of it into the next, he takes in half the pause between - and
				// through the middle of that he goes at the other gait's pace: a
				// step aside eased off right there, say.)
				const prev = tr.moves[k - 1];
				const next = tr.moves[k + 1];
				const gap0 = prev ? m.t0 - prev.t1 : Infinity;
				const gap1 = next ? next.t0 - m.t1 : Infinity;
				const joined = quickest(
					gap0 <= FLOW_MS ? m.t0 - gap0 / 2 : m.t0,
					gap1 <= FLOW_MS ? m.t1 + gap1 / 2 : m.t1,
				);
				const sure =
					Math.min(Math.abs(fastest - 19), Math.abs(fastest - 11)) > 0.75 &&
					gaitOf(joined) === gaitOf(fastest);
				let top = 0;
				let low = Infinity;
				let frames = 0;
				for (let t = m.t0 + secs * 250; t < m.t1 - secs * 250; t += 20) {
					const st = evalPlayer(tl, pid, t);
					if (st.anim !== "sprint" && st.anim !== "run" && st.anim !== "jog") {
						continue;
					}
					frames += 1;
					if (sure) {
						assert.strictEqual(st.anim, gaitOf(fastest));
					}
					top = Math.max(top, st.z);
					low = Math.min(low, st.z);
				}
				if (fastest >= 19) {
					sprints += 1;
					assert.isAbove(top, 0.15);
				} else {
					jogs += 1;
				}
				// (Seen running for less than a step - the rest of it taken up
				// doing something else, a reach - he can be in the air all of
				// that.)
				if (frames >= 20) {
					assert.isBelow(low, 0.02);
				}
			}
		}
		assert.isAbove(sprints, 5);
		assert.isAbove(jogs, 5);
	}, 240_000);

	// A run with time either side of it is taken at a player's pace: not
	// flat out from a standstill and pulled up dead, a blink each way.
	test("a short run with nothing either side of it is no dash", () => {
		for (const seed of ["a", "b"]) {
			const { tl } = compile(seed);
			let alone = 0;
			let dashes = 0;
			for (const tr of tl.tracks.values()) {
				tr.moves.forEach((m, i) => {
					const prev = tr.moves[i - 1];
					const next = tr.moves[i + 1];
					const L = Math.hypot(m.to.x - m.from.x, m.to.y - m.from.y);
					if (
						L < 1 ||
						(prev && m.t0 - prev.t1 < 320) ||
						(next && next.t0 - m.t1 < 320)
					) {
						return;
					}
					alone += 1;
					// (Twice as hard as he usually goes about it, and more.)
					if (m.t1 - m.t0 < runMs(L, 26, 0, 2)) {
						dashes += 1;
					}
				});
			}
			assert.isAbove(alone, 500);
			// (The rest the moment he has to be there by, and what he does
			// there, leave no more time for.)
			assert.isBelow(dashes / alone, 0.025, seed);
		}
	}, 480_000);
});
