import { assert, describe, test } from "vitest";
import {
	compileCourt,
	isLineItem,
	linesBetween,
	snapForCursor,
	targetForCursor,
	type CourtTimeline,
} from "./director.ts";
import {
	FAST,
	bodyPoint,
	evalBall,
	evalPlayer,
	fastAt,
	handWorld,
	heldBall,
	offenseAt,
	poseOf,
	tensionAt,
	withBody,
} from "./evaluate.ts";
import { buildClocks, gameClockAt } from "./clock.ts";
import { crowdAt } from "./scene.ts";
import {
	attackDir,
	COURT_H,
	COURT_W,
	FT_LINE_DEPTH,
	RIM_Z,
	rimX,
} from "./geometry.ts";
import { bodyOf, posed, skeleton } from "./poses.ts";
import { compile, fakeGame, gidOf } from "./testGame.ts";
import { finishOf } from "../../../util/liveGameWording.basketball.ts";

const body = bodyOf();
const bodyFor = () => body;
// What a man is doing as he lets a shot go.
const SHOOTING = new Set<string>([
	"shoot",
	"fade",
	"hook",
	"layup",
	"dunk",
	"dunk1",
	"tomahawk",
	"block",
]);

// Whether the picture cut between two moments (anyone may be anywhere after).
const cutBetween = (tl: CourtTimeline, a: number, b: number) =>
	tl.cuts.some((c) => c > a && c <= b);

const sampleTimes = (tl: CourtTimeline, step: number) => {
	const out: number[] = [];
	for (let t = 0; t <= tl.end; t += step) {
		out.push(t);
	}
	return out;
};

describe("2.5D director", () => {
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
	}, 60_000);

	test("the same game stages the same way every time (every device agrees)", () => {
		const a = compile("same").tl;
		const b = compile("same").tl;
		assert.strictEqual(JSON.stringify(a.beats), JSON.stringify(b.beats));
		assert.strictEqual(JSON.stringify(a.ball), JSON.stringify(b.ball));
		assert.strictEqual(
			JSON.stringify([...a.tracks.values()]),
			JSON.stringify([...b.tracks.values()]),
		);
	}, 60_000);

	// A saved replay is the play-by-play read back out of the database and
	// staged again, on whatever device, after whatever else was watched.
	test("a saved replay stages exactly like the game did, every viewing", () => {
		const live = compile("replay");
		compile("another game in between");
		// (IndexedDB stores a structured clone.)
		const saved = structuredClone(live.events);
		const again = compileCourt({
			events: saved,
			players: live.players,
			gid: gidOf("replay"),
		});
		const dump = (tl: CourtTimeline) =>
			JSON.stringify({ ...tl, tracks: [...tl.tracks.entries()] });
		assert.strictEqual(dump(again), dump(live.tl));
		for (const t of sampleTimes(live.tl, 250)) {
			for (const p of live.players) {
				assert.deepStrictEqual(
					evalPlayer(again, p.pid, t),
					evalPlayer(live.tl, p.pid, t),
				);
			}
			assert.deepStrictEqual(
				evalBall(again, t, bodyFor),
				evalBall(live.tl, t, bodyFor),
			);
		}
	}, 60_000);

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
	}, 60_000);

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
	}, 60_000);

	test("a player's moves never overlap and stay near the floor", () => {
		const { tl } = compile("moves", 160);
		for (const tr of tl.tracks.values()) {
			for (let k = 0; k < tr.moves.length; k++) {
				const m = tr.moves[k]!;
				assert.isAbove(m.t1, m.t0);
				if (k > 0) {
					assert.isAtLeast(
						m.t0,
						tr.moves[k - 1]!.t1 - 1,
						`pid ${tr.pid} move ${k}`,
					);
				}
				// On the floor, or on his way to or from the bench.
				for (const p of [m.from, m.to]) {
					assert.isTrue(
						p.x > -4 && p.x < 98 && p.y > -8.5 && p.y < 54,
						`pid ${tr.pid} off the floor: ${p.x},${p.y}`,
					);
				}
			}
		}
	}, 60_000);

	test("bodies glide - nobody on the floor teleports between frames", () => {
		const { tl } = compile("glide", 140);
		const pids = [...tl.tracks.keys()];
		let prev = new Map<number, ReturnType<typeof evalPlayer>>();
		let last = -Infinity;
		for (const t of sampleTimes(tl, 20)) {
			const cur = new Map<number, ReturnType<typeof evalPlayer>>();
			const cut = cutBetween(tl, last, t);
			last = t;
			for (const pid of pids) {
				const st = evalPlayer(tl, pid, t);
				cur.set(pid, st);
				const p = prev.get(pid);
				if (p && p.shown && st.shown && !cut) {
					const d = Math.hypot(st.x - p.x, st.y - p.y);
					assert.isBelow(d, 2, `pid ${pid} jumped ${d.toFixed(2)}ft at t=${t}`);
				}
			}
			prev = cur;
		}
	}, 60_000);

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
	}, 120_000);

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
	}, 60_000);

	// A defender goes with his man: when a man on offense runs, the man
	// guarding him (the nearest of the defense) is on the move too - not
	// standing there until the next step of the set sends him.
	test("defenders move with their men", () => {
		for (const seed of ["a", "b"]) {
			const { tl } = compile(seed, 140);
			const tracks = [...tl.tracks.values()];
			let running = 0;
			let left = 0;
			for (let t = 0; t < tl.end; t += 100) {
				const seg = tl.ball[tl.ball.findLastIndex((x) => x.t0 <= t)];
				if (seg?.kind !== "hold") {
					continue;
				}
				const off = offenseAt(tl, t);
				const b = evalBall(tl, t, bodyFor);
				if (off === 1 ? b.x < 52 : b.x > 42) {
					continue;
				}
				const now = tracks
					.map((tr) => ({ tr, st: evalPlayer(tl, tr.pid, t) }))
					.filter((x) => x.st.shown);
				for (const o of now) {
					if (o.tr.team !== off) {
						continue;
					}
					const o2 = evalPlayer(tl, o.tr.pid, t + 100);
					if (Math.hypot(o2.x - o.st.x, o2.y - o.st.y) < 0.6) {
						continue;
					}
					let near: (typeof now)[number] | undefined;
					let best = 12;
					for (const d of now) {
						const dd = Math.hypot(d.st.x - o.st.x, d.st.y - o.st.y);
						if (d.tr.team !== off && dd < best) {
							best = dd;
							near = d;
						}
					}
					if (near) {
						running += 1;
						const d2 = evalPlayer(tl, near.tr.pid, t + 100);
						if (Math.hypot(d2.x - near.st.x, d2.y - near.st.y) < 0.15) {
							left += 1;
						}
					}
				}
			}
			assert.isAbove(running, 500);
			assert.isBelow(left / running, 0.2, seed);
		}
	}, 60_000);

	// Kicked out of the lane, the shooter is open because his man left him
	// to help on the drive - a stunt at the ball, or all the way over - and
	// by the time the shot goes up somebody has closed back out to him. The
	// passer does not stay in there watching it: he gets back out (a big
	// stays in, for the rebound).
	test("a kick out of the lane: the help leaves the shooter and closes back out", () => {
		const helps: number[] = [];
		let closed = 0;
		let passers = 0;
		let backOut = 0;
		for (const seed of ["a", "kick"]) {
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
				helps.push(room(shooter, pass.t0));
				if (room(shooter, shot.t0) <= 4) {
					closed += 1;
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
		assert.isAtLeast(backOut / passers, 0.6);
	}, 60_000);

	// A man set in a screen or sealing in the post is a body: whoever comes
	// at him runs into him - held up a moment, fighting round him - rather
	// than straight through him.
	test("nobody runs straight through a man set in a screen", () => {
		let screens = 0;
		let contact = 0;
		for (const seed of ["a", "b"]) {
			const { tl } = compile(seed, 140);
			const tracks = [...tl.tracks.values()];
			for (const tr of tracks) {
				for (const a of tr.acts) {
					if (a.anim !== "screen" && a.anim !== "postUp") {
						continue;
					}
					screens += 1;
					let closest = Infinity;
					// (From once he has stepped in to set it.)
					for (let t = a.t0 + 200; t <= a.t1; t += 50) {
						const S = evalPlayer(tl, tr.pid, t);
						for (const o of tracks) {
							const p = evalPlayer(tl, o.pid, t);
							if (o.team !== tr.team && p.shown) {
								closest = Math.min(closest, Math.hypot(p.x - S.x, p.y - S.y));
							}
						}
					}
					assert.isAtLeast(
						closest,
						1.3,
						`${seed}: through ${tr.pid} at ${a.t0}`,
					);
					if (closest < 2.4) {
						contact += 1;
					}
				}
			}
		}
		assert.isAbove(screens, 40);
		// And he is into somebody, often.
		assert.isAbove(contact / screens, 0.25);
	}, 60_000);

	// The ball brought over half court and the trip a few seconds old, the
	// defense is back with it: nobody stays at the other end guarding
	// nobody, or runs back there after a man left behind - and nobody on the
	// offense stands around back there either (the trailer comes up).
	test("once the ball is over half court, nobody is left at the other end", () => {
		for (const seed of ["a", "b"]) {
			const { tl } = compile(seed, 140);
			const tracks = [...tl.tracks.values()];
			const subs = tl.beats.filter((b) => b.type === "sub");
			const mid = COURT_W / 2;
			let k = 0;
			let n = 0;
			let back = 0;
			let runD = 0;
			let runO = 0;
			let longD = 0;
			let longO = 0;
			for (let t = 0; t < tl.end; t += 100) {
				while (k + 1 < tl.ball.length && tl.ball[k + 1]!.t0 <= t) {
					k++;
				}
				const s = tl.ball[k]!;
				const h = s.kind === "hold" ? tl.tracks.get(s.pid) : undefined;
				const side = h ? Math.sign(rimX(h.team) - mid) : 0;
				const H = s.kind === "hold" ? evalPlayer(tl, s.pid, t) : undefined;
				const since = tl.poss.findLast(([t0]) => t0 <= t)?.[0] ?? 0;
				if (
					!h ||
					!H ||
					offenseAt(tl, t) !== h.team ||
					(H.x - mid) * side < 4 ||
					t - since < 3000 ||
					subs.some((b) => t >= b.preStart - 500 && t <= b.end + 500)
				) {
					runD = 0;
					runO = 0;
					continue;
				}
				n++;
				let d = 0;
				let o = 0;
				for (const tr of tracks) {
					const st = evalPlayer(tl, tr.pid, t);
					if (
						tr === h ||
						!st.shown ||
						(st.x - mid) * side > -2 ||
						tr.shown.some(([ts]) => Math.abs(ts - t) < 4000)
					) {
						continue;
					}
					if (tr.team !== h.team) {
						d++;
					} else if (!st.moving) {
						o++;
					}
				}
				back += d > 0 ? 1 : 0;
				runD = d > 0 ? runD + 100 : 0;
				runO = o > 0 ? runO + 100 : 0;
				longD = Math.max(longD, runD);
				longO = Math.max(longO, runO);
			}
			assert.isAbove(n, 5000, seed);
			// (Getting back on a break, a step behind the ball.)
			assert.isBelow(back / n, 0.03, seed);
			assert.isAtMost(longD, 2000, seed);
			assert.isAtMost(longO, 1600, seed);
		}
	}, 60_000);

	// Two men are never on top of each other in play: teammates (two
	// defenders helping off the same way, two men sent to the same spot)
	// keep a step apart, and nobody stands inside his opponent - but for a
	// screen, a post-up, a box-out, a rebound fight, a block at the rim.
	test("nobody stands on top of anybody", () => {
		const TOGETHER = new Set([
			"screen",
			"postUp",
			"fight",
			"boxOut",
			"bump",
			"highFive",
			"reach",
			"poke",
			"block",
			"dunk",
			"dunk1",
			"tomahawk",
			"fall",
			"hurt",
			"rebound",
			"board",
			"snatch",
			"pickup",
			"contest",
			"shoot",
			"setShot",
			"fade",
			"hook",
			"layup",
			"catch",
		]);
		for (const seed of ["a", "b"]) {
			const { tl } = compile(seed, 140);
			const tracks = [...tl.tracks.values()];
			const doing = (pid: number, t: number) =>
				tl.tracks
					.get(pid)!
					.acts.some((a) => a.t0 <= t && a.t1 > t && TOGETHER.has(a.anim));
			let live = 0;
			let close = 0;
			let prev = new Map<number, { x: number; y: number }>();
			for (let t = 0; t < tl.end; t += 100) {
				if (fastAt(tl, t) > 1.001) {
					prev = new Map();
					continue;
				}
				live += 1;
				const here = tracks
					.map((tr) => ({ tr, s: evalPlayer(tl, tr.pid, t) }))
					.filter((x) => x.s.shown);
				const now = new Map(here.map((x) => [x.tr.pid, x.s]));
				for (let i = 0; i < here.length; i++) {
					for (let j = i + 1; j < here.length; j++) {
						const A = here[i]!;
						const B = here[j]!;
						const d = Math.hypot(A.s.x - B.s.x, A.s.y - B.s.y);
						const mates = A.tr.team === B.tr.team;
						if (d >= (mates ? 2 : 1)) {
							continue;
						}
						// (Standing - not just going by.)
						const still = [A, B].every((X) => {
							const p = prev.get(X.tr.pid);
							return p && Math.hypot(X.s.x - p.x, X.s.y - p.y) < 0.4;
						});
						if (still && !doing(A.tr.pid, t) && !doing(B.tr.pid, t)) {
							close += 1;
						}
					}
				}
				prev = now;
			}
			assert.isAbove(live, 10_000, seed);
			assert.isBelow(close / live, 0.015, seed);
		}
	}, 120_000);

	// Left out of the play while the ball is worked somewhere else, a man
	// does not stand there like a statue for seconds on end: he drifts and
	// comes back, lifts out of the corner and sinks into it again, steps out
	// of the lane - or trails up from the other end.
	test("off the ball, nobody stands frozen in place for long", () => {
		const { tl } = compile("a", 140);
		const STEP = 100;
		const live: boolean[] = [];
		const holder: (number | undefined)[] = [];
		let k = 0;
		for (let t = 0; t < tl.end; t += STEP) {
			while (k + 1 < tl.ball.length && tl.ball[k + 1]!.t0 <= t) {
				k++;
			}
			const s = tl.ball[k]!;
			live.push(
				s.kind === "hold" ||
					(s.kind === "fly" && "pid" in s.from && "pid" in s.to),
			);
			holder.push(s.kind === "hold" ? s.pid : undefined);
		}
		const fast = (t: number) => tl.fast.some(([a, b]) => t >= a && t < b);
		const frozen: number[] = [];
		for (const tr of tl.tracks.values()) {
			let run = 0;
			for (let i = 0, t = 0; t < tl.end; i++, t += STEP) {
				const st = evalPlayer(tl, tr.pid, t);
				const off =
					live[i] &&
					st.shown &&
					holder[i] !== tr.pid &&
					offenseAt(tl, t) === tr.team &&
					!fast(t);
				if (off && !st.moving) {
					run += STEP;
				} else {
					if (run >= 2500) {
						frozen.push(run);
					}
					run = 0;
				}
			}
		}
		assert.isBelow(frozen.length, 60);
		assert.isBelow(frozen.filter((x) => x >= 4000).length, 12);
	}, 120_000);

	// A screen, a post-up, a celebration, words with the official: each is
	// done where he stands, and the pose ends as he sets off again rather
	// than him sliding away across the floor in it.
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
	}, 60_000);

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
		assert.isAtLeast(count("point"), 10);
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
	}, 60_000);

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
	}, 120_000);

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
					assert.isBelow(
						Math.abs(d),
						0.25,
						`pid ${pid} turned ${d.toFixed(2)} in a frame at ${t}`,
					);
				}
			}
			prev = cur;
		}
	}, 120_000);

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
	}, 60_000);

	// What just happened gets a moment at real speed before the picture
	// hurries on, and longer after a basket - the ball down through the net,
	// the scorer turning back up the floor. (Barely one between free throws,
	// and none after a substitution.)
	test("after a play the picture holds a moment before it hurries on", () => {
		const { tl } = compile("cuts", 160);
		const FT = /^(ft|missFt)$/;
		let afterScore = 0;
		for (const [a] of tl.fast) {
			const before = tl.beats.filter((b) => b.actionStart <= a);
			for (const [j, b] of before.entries()) {
				const next = tl.beats
					.slice(tl.beats.indexOf(b) + 1)
					.find((x) => x.type !== "sub");
				if (b.type === "sub") {
					continue;
				}
				const hold =
					FT.test(b.type) && next && FT.test(next.type)
						? 200
						: (/^(fg|tp)/.test(b.type) && !b.type.startsWith("fga")) ||
							  b.type === "ft"
							? 1500
							: 800;
				assert.isAtLeast(a - b.end, hold - 1e-6, `${b.type} ${j}`);
			}
			const last = before.at(-1);
			if (last && /^(fg|tp)/.test(last.type)) {
				afterScore += 1;
			}
		}
		assert.isAbove(afterScore, 10);
	}, 60_000);

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
	}, 60_000);

	// The clock on screen never stops while the ball is live - the shot in
	// the air, the rebound - and stays stopped through a whistle.
	test("the game clock runs on through live play and stops for a whistle", () => {
		const { events, tl } = compile("clock", 160);
		const clocks = buildClocks(tl, events);
		let live = 0;
		let whistles = 0;
		tl.beats.forEach((b, j) => {
			const e = events[b.i]!;
			const next = tl.beats[j + 1];
			const n = next ? events[next.i] : undefined;
			if (
				typeof e.clock !== "number" ||
				typeof n?.clock !== "number" ||
				b.end - b.actionStart < 400
			) {
				return;
			}
			const at = (t: number) => gameClockAt(clocks, t)!;
			const mid = (b.actionStart + b.end) / 2;
			if (/^fga/.test(e.type) && n.clock < e.clock - 0.3) {
				// In the air: already running down to the result's reading.
				assert.isBelow(at(mid), e.clock, `${e.type} at ${b.actionStart}`);
				assert.isAtLeast(at(mid), n.clock);
				live += 1;
			} else if (/^(pf|ft|missFt)/.test(e.type) && n.clock === e.clock) {
				assert.strictEqual(at(mid), e.clock, `${e.type} at ${b.actionStart}`);
				whistles += 1;
			}
		});
		assert.isAbove(live, 20);
		assert.isAbove(whistles, 10);
	}, 60_000);

	// A quick trip off a defensive board or a steal is a fast break - run
	// out at full speed, never fast-forwarded - and a long one walks it up
	// (run through fast) and runs something first: a screen or a pass more
	// before the action that gets the shot.
	test("quick trips run out on the break; long ones run something first", () => {
		let quick = 0;
		let broke = 0;
		const actions: Record<"short" | "long", number[]> = { short: [], long: [] };
		for (const seed of ["break", "break2", "break3", "break4"]) {
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
	}, 120_000);

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
	}, 60_000);

	// A basket's line comes with the points, rebounds and assists it adds up -
	// events, not lines - so the page taking all of them at once is one line
	// on, not a jump ahead to cut past.
	test("a line and the stats that come with it are one line on", () => {
		const { tl } = compile("a", 140);
		for (let k = 0; k + 1 < tl.beats.length; k++) {
			const a = tl.beats[k]!;
			const b = tl.beats[k + 1]!;
			// However many events after it the page took along with it.
			for (let to = a.i + 1; to <= b.i; to++) {
				assert.strictEqual(linesBetween(tl, a.i, to), 1);
			}
		}
		assert.strictEqual(linesBetween(tl, tl.beats[0]!.i, tl.beats[6]!.i), 6);
	}, 60_000);

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
				assert.isTrue(near("clank"), `${b.type} at ${b.actionStart}`);
			}
		}
	}, 60_000);

	test("a shot is played out at the rim the way its line says it went - free throws too", () => {
		const { events, tl } = compile("a", 140);
		const beatOf = new Map(tl.beats.map((b) => [b.i, b]));
		let made = 0;
		let missed = 0;
		let fts = 0;
		events.forEach((e, i) => {
			const make =
				(/^(fg|tp)/.test(e.type) && !e.type.startsWith("fga")) ||
				e.type === "ft";
			const miss = e.type.startsWith("miss");
			const b = beatOf.get(i);
			if (!b || (!make && !miss)) {
				return;
			}
			// What the ball does at the rim, there when the line shows.
			const path = tl.ball.find(
				(s) =>
					s.kind === "path" &&
					s.t0 <= b.actionStart &&
					s.t1 >= b.actionStart - 1,
			);
			if (path?.kind !== "path") {
				return;
			}
			const rx = rimX(path.pts[0]! < COURT_W / 2 ? 0 : 1);
			let through = false;
			for (let k = 0; k < path.pts.length; k += 3) {
				const rho = Math.hypot(path.pts[k]! - rx, path.pts[k + 1]! - 25);
				if (path.pts[k + 2]! < RIM_Z - 0.5 && rho < 0.63) {
					through = true;
				}
			}
			assert.strictEqual(through, make, `line ${i}`);
			made += make ? 1 : 0;
			missed += miss ? 1 : 0;
			fts += e.type === "ft" || e.type === "missFt" ? 1 : 0;
		});
		assert.isAbove(made, 15);
		assert.isAbove(missed, 15);
		assert.isAbove(fts, 15);
	}, 60_000);

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
			if (next?.kind === "fly" && "pid" in next.to) {
				off.push(swerve(p.t1));
			}
		});
		const ok = (xs: number[]) => xs.filter((x) => x < 3).length / xs.length;
		assert.isAbove(into.length, 60);
		assert.isAbove(off.length, 20);
		assert.isAbove(ok(into), 0.9);
		assert.isAbove(ok(off), 0.9);
	}, 60_000);

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
	}, 60_000);

	test("off the rim, the man who gets it mostly goes up and takes it in the air", () => {
		const { events, tl } = compile("a", 140);
		const beatOf = new Map(tl.beats.map((b) => [b.i, b]));
		let air = 0;
		let all = 0;
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
			if (next?.kind === "fly" && "pid" in next.to && next.to.pid === e.pid) {
				air += 1;
				// Up for it, at the top of his jump.
				const st = evalPlayer(tl, e.pid as number, next.t1);
				assert.isAbove(st.z, 0.2, `line ${i}`);
			}
		});
		assert.isAbove(all, 20);
		assert.isAbove(air / all, 0.6);
	}, 60_000);

	test("the playback target only moves forward as lines are shown", () => {
		const { events, tl } = compile("cursor", 60);
		let prev = -1;
		for (let c = 0; c <= events.length; c++) {
			const t = targetForCursor(tl, c);
			assert.isAtLeast(t, prev);
			assert.isAtMost(snapForCursor(tl, c), t);
			prev = t;
		}
		assert.strictEqual(targetForCursor(tl, events.length), tl.end);
	}, 60_000);
	test("a shot goes up from the zone the sim says", () => {
		const { events, tl } = compile("zones", 160);
		const beatOf = new Map(tl.beats.map((b) => [b.i, b]));
		const band: Record<string, [number, number]> = {
			fgaAtRim: [0, 6],
			fgaLowPost: [3, 13],
			fgaMidRange: [7, 24],
			fgaTp: [21.9, 40],
		};
		let checked = 0;
		events.forEach((e, i) => {
			const range = band[e.type];
			if (!range) {
				return;
			}
			const b = beatOf.get(i)!;
			const st = evalPlayer(tl, e.pid, b.actionStart);
			// Display team 0 is raw team 1, and attacks the left rim.
			const rimX = e.t === 1 ? 5.25 : 94 - 5.25;
			const d = Math.hypot(st.x - rimX, st.y - 25);
			assert.isTrue(
				d >= range[0] && d <= range[1],
				`${e.type} by ${e.pid} from ${d.toFixed(1)}ft (line ${i})`,
			);
			checked += 1;
		});
		assert.isAbove(checked, 50);
	}, 60_000);

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
	}, 60_000);

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
					["dunk", "dunk1", "tomahawk"].includes(x.anim),
				);
				const finish = finishOf(e, gid, gender);
				assert.strictEqual(
					dunked,
					finish === "dunk" || finish === "poster",
					`line ${i}`,
				);
				assert.strictEqual(
					acts.some((x) => x.anim === "layup"),
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
	}, 60_000);

	// "Dunks on him": the man the words name is the one who meets him at the
	// rim - nearest him of anybody on the other side, right there, up with
	// him - not someone else who happened to be in there.
	test("a poster dunk is over the man the play-by-play says", () => {
		const gid = 4242;
		let posters = 0;
		for (const seed of ["posters", "posters2"]) {
			const { events, players } = fakeGame(seed, 400);
			const tl = compileCourt({ events, players, gid });
			const beatOf = new Map(tl.beats.map((b) => [b.i, b]));
			events.forEach((e, i) => {
				if (
					(e.type !== "fgAtRim" && e.type !== "fgAtRimAndOne") ||
					typeof e.pidDefense !== "number" ||
					finishOf(e, gid, "male") !== "poster"
				) {
					return;
				}
				let a = i - 1;
				while (a >= 0 && events[a]!.type !== "fgaAtRim") {
					a -= 1;
				}
				// At the top of his slam.
				const top = beatOf.get(a)!.actionStart + 1300 * 0.42;
				const dunker = evalPlayer(tl, e.pid, top);
				const near = [...tl.tracks.values()]
					.filter((tr) => tr.team !== dunker.team)
					.map((tr) => evalPlayer(tl, tr.pid, top))
					.filter((st) => st.shown)
					.sort(
						(x, y) =>
							Math.hypot(x.x - dunker.x, x.y - dunker.y) -
							Math.hypot(y.x - dunker.x, y.y - dunker.y),
					);
				const victim = near[0]!;
				assert.strictEqual(victim.pid, e.pidDefense, `line ${i}`);
				assert.isBelow(
					Math.hypot(victim.x - dunker.x, victim.y - dunker.y),
					2.5,
					`line ${i}`,
				);
				assert.isAbove(victim.z, 0.5, `line ${i}`);
				posters += 1;
			});
		}
		assert.isAbove(posters, 3);
	}, 60_000);

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
	}, 60_000);

	// Every free throw: the official bounces him the ball, he dribbles, shoots
	// and holds his follow-through until the ball gets to the rim.
	// A three is a three: both feet behind the line, toes and all, as he goes
	// up - at the top, on the wings and in the corners, where the line runs
	// 22 feet out along the sideline.
	test("a three goes up with his feet behind the line", () => {
		const past = (team: number, x: number, y: number) => {
			const depth = team === 0 ? x : COURT_W - x;
			return depth < 14
				? Math.abs(y - 25) - 22
				: Math.hypot(depth - 5.25, y - 25) - 23.75;
		};
		let threes = 0;
		for (const seed of ["a", "b", "c"]) {
			const { events, tl } = compile(seed);
			for (const b of tl.beats) {
				const e = events[b.i]!;
				if (e.type !== "fgaTp") {
					continue;
				}
				const pid = e.pid as number;
				const tr = tl.tracks.get(pid)!;
				const act = tr.acts.find(
					(a) =>
						(a.anim === "shoot" || a.anim === "fade") &&
						a.t0 >= b.preStart &&
						a.t0 <= b.end + 2000,
				);
				if (!act) {
					continue;
				}
				threes += 1;
				for (const u of [0, 0.15, 0.3]) {
					const t = act.t0 + (act.t1 - act.t0) * u;
					const st = evalPlayer(tl, pid, t);
					const sk = skeleton(
						body,
						posed(st.anim, st.phase, st.dribble, st.dribbleHand),
					);
					for (const leg of [sk.legR, sk.legL]) {
						const a = leg.end;
						const tip = leg.tip!;
						// The front of his shoe, a little past his toes.
						for (const k of [-0.4, 1.3]) {
							const p = bodyPoint(st, {
								f: a.f + (tip.f - a.f) * k,
								s: a.s + (tip.s - a.s) * k,
								u: 0,
							});
							assert.isAbove(
								past(tr.team, p.x, p.y),
								0.05,
								`${seed} ${b.i} at ${t}`,
							);
						}
					}
				}
			}
		}
		assert.isAbove(threes, 30);
	}, 60_000);

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
	}, 60_000);

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

	// The man with the ball doesn't stand about with it back in his own half
	// - waiting on a break that never gets going, or for a whistle: it is
	// brought up the floor. (Seconds as they go by on screen.)
	test("nobody stands with the ball in the backcourt", () => {
		for (const seed of ["a", "b"]) {
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
	}, 120_000);

	// The camera goes off the floor only while nothing is happening on it: the
	// game opens on the building and cuts in for the tip, and a timeout or the
	// break between periods looks round it - but no shot, pass or tip-off is
	// ever played out while it does.
	test("a look round the building only ever covers dead time", () => {
		for (const seed of ["a", "b", "c"]) {
			const { events, tl } = compile(seed);
			assert.isAbove(tl.shots.length, 1);
			const first = tl.beats[0]!;
			assert.strictEqual(events[first.i]!.type, "jumpBall");
			assert.strictEqual(tl.shots[0]!.t0, first.preStart);
			let prev = -Infinity;
			for (const s of tl.shots) {
				assert.isAbove(s.t1, s.t0);
				assert.isAtLeast(s.t0, prev);
				prev = s.t1;
				for (const seg of tl.ball) {
					if (seg.kind === "fly" && seg.t0 < s.t1 && seg.t1 > s.t0) {
						assert.fail(`the ball flies during an arena shot at ${s.t0}`);
					}
				}
				for (const b of tl.beats) {
					if (b.actionStart > s.t0 && b.actionStart < s.t1) {
						const type = events[b.i]!.type;
						assert.include(
							[
								"timeout",
								"endOfPeriod",
								"period",
								"overtime",
								"sub",
								"gameOver",
							],
							type,
							`${type} at ${b.actionStart} in an arena shot`,
						);
					}
				}
			}
		}
	}, 60_000);

	// A pass coming his way: his hands come up for it before it gets there.
	test("a receiver shows his hands for a pass", () => {
		for (const seed of ["a", "b"]) {
			const { tl } = compile(seed);
			let passes = 0;
			let shown = 0;
			for (const seg of tl.ball) {
				if (
					seg.kind !== "fly" ||
					!("pid" in seg.from) ||
					!("pid" in seg.to) ||
					seg.t1 - seg.t0 < 300
				) {
					continue;
				}
				const to = evalPlayer(tl, seg.to.pid, seg.t1 - 120);
				if (to.team !== evalPlayer(tl, seg.from.pid, seg.t0).team) {
					continue;
				}
				passes += 1;
				if ((to.target ?? 0) > 0.9) {
					shown += 1;
				}
				// Not once he has it.
				assert.isUndefined(evalPlayer(tl, seg.to.pid, seg.t1 + 400).target);
			}
			assert.isAbove(passes, 20);
			assert.isAbove(shown / passes, 0.9, seed);
		}
	}, 60_000);

	// Running, sliding, dribbling, a man still talks with a hand: the
	// screener's man points out the screen, a switch or a man getting back
	// calls out who he has, an open man puts a hand up for it - and a shooter
	// holds his follow-through up till his shot gets there. One arm - never
	// the one on the ball - and the rest of him goes on as it was.
	test("players talk with a hand on the move", () => {
		const { tl } = compile("a", 140);
		const kinds = { point: 0, hand: 0, wave: 0, slap: 0, follow: 0 };
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
	}, 60_000);

	// One pass from the ball and up on his man, a defender has an arm out in
	// the passing lane - and it comes and goes, never jumping from one arm
	// to the other.
	test("one pass away, a defender gets an arm in the lane", () => {
		const { tl } = compile("a", 140);
		let cand = 0;
		let denied = 0;
		let flips = 0;
		const hand = new Map<number, string>();
		const said = (pid: number, t: number) =>
			tl.tracks.get(pid)!.arms.some((g) => g.t0 <= t + 1 && g.t1 >= t - 101);
		for (let t = 0; t < tl.end / 2; t += 100) {
			const b = evalBall(tl, t, bodyFor);
			if (b.holder === undefined) {
				hand.clear();
				continue;
			}
			const h = evalPlayer(tl, b.holder, t);
			const theirs = [...tl.tracks.values()]
				.filter((o) => o.team === h.team && o.pid !== b.holder)
				.map((o) => evalPlayer(tl, o.pid, t))
				.filter((o) => o.shown);
			for (const tr of tl.tracks.values()) {
				if (tr.team === h.team) {
					continue;
				}
				const st = evalPlayer(tl, tr.pid, t);
				if (!st.shown) {
					continue;
				}
				const was = hand.get(tr.pid);
				if (st.arm && st.arm.w > 0.3 && !said(tr.pid, t)) {
					if (was !== undefined && was !== st.arm.hand) {
						flips += 1;
					}
					hand.set(tr.pid, st.arm.hand);
				} else {
					hand.delete(tr.pid);
				}
				if (st.anim !== "stance") {
					continue;
				}
				const man = theirs
					.map((o) => ({ o, d: Math.hypot(o.x - st.x, o.y - st.y) }))
					.sort((x, y) => x.d - y.d)[0];
				const far = man && Math.hypot(man.o.x - h.x, man.o.y - h.y);
				if (!man || man.d > 5 || far! < 12 || far! > 21) {
					continue;
				}
				cand += 1;
				if (st.arm && st.arm.w > 0.3) {
					denied += 1;
				}
			}
		}
		assert.isAbove(cand, 300);
		assert.isAbove(denied / cand, 0.45);
		assert.isBelow(flips, 15);
	}, 120_000);

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
	}, 60_000);

	test("flat out, a player sprints - bounding off the floor stride to stride; at an easy pace, he jogs", () => {
		const { tl } = compile("a");
		let sprints = 0;
		let jogs = 0;
		for (const [pid, tr] of tl.tracks) {
			for (const m of tr.moves) {
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
				let fastest = 0;
				for (let t = m.t0 + secs * 350; t < m.t1 - secs * 350; t += 20) {
					fastest = Math.max(fastest, at(t));
				}
				// (A walk - some run is that slow - is a walk. And right on the
				// line between two gaits, either will do.)
				if (fastest < 6.25) {
					continue;
				}
				const sure =
					Math.min(Math.abs(fastest - 19), Math.abs(fastest - 11)) > 0.75;
				let top = 0;
				let low = Infinity;
				for (let t = m.t0 + secs * 250; t < m.t1 - secs * 250; t += 20) {
					const st = evalPlayer(tl, pid, t);
					if (st.anim !== "sprint" && st.anim !== "run" && st.anim !== "jog") {
						continue;
					}
					if (sure) {
						assert.strictEqual(
							st.anim,
							fastest >= 19 ? "sprint" : fastest < 11 ? "jog" : "run",
						);
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
				assert.isBelow(low, 0.02);
			}
		}
		assert.isAbove(sprints, 5);
		assert.isAbove(jogs, 5);
	}, 60_000);

	// A dunk is a dunk: the ball goes up over the rim in his hand and is
	// thrown down through it, and a make, he hangs there by both hands - a
	// guard, a wing or a center alike.
	test("a dunker gets it over the rim and hangs on it", () => {
		let dunks = 0;
		for (const seed of ["a", "b", "c"]) {
			const { tl } = compile(seed);
			for (const [pid, tr] of tl.tracks) {
				for (const a of tr.acts) {
					if (!a.rim || a.rim.grip.length === 0) {
						continue;
					}
					dunks += 1;
					const rim = a.rim.at;
					const at = (u: number) => a.t0 + (a.t1 - a.t0) * u;
					for (const hgt of [72, 79, 85]) {
						const b = bodyOf(hgt, 220);
						// Up over the rim with it as he throws it down.
						let top = { x: 0, y: 0, z: 0 };
						for (let u = 0.4; u <= 0.47; u += 0.01) {
							const ball = heldBall(evalPlayer(tl, pid, at(u)), b);
							if (ball.z > top.z) {
								top = ball;
							}
						}
						assert.isAbove(top.z, RIM_Z + 0.6, `${seed} ${pid} ${hgt}`);
						assert.isBelow(
							Math.hypot(top.x - rim.x, top.y - rim.y),
							2.4,
							`${seed} ${pid} ${hgt}`,
						);
						// Then both hands on the front of the rim.
						const st = withBody(evalPlayer(tl, pid, at(0.6)), b);
						for (const which of ["near", "far"] as const) {
							const h = handWorld(st, b, which);
							assert.isBelow(
								Math.hypot(h.x - rim.x, h.y - rim.y, h.z - rim.z),
								1.1,
								`${seed} ${pid} ${hgt} ${which}`,
							);
						}
					}
				}
			}
		}
		assert.isAbove(dunks, 2);
	}, 60_000);

	// A close game late in the fourth (or in overtime) has the building on
	// its feet; the first three periods never do.
	test("a tight finish has the crowd on its feet", () => {
		let tense = 0;
		for (const seed of ["e", "f", "g"]) {
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
	}, 60_000);

	test("a rebounder chins it before he goes anywhere with it", () => {
		for (const seed of ["a", "b"]) {
			const { tl } = compile(seed);
			let boards = 0;
			for (const [pid, tr] of tl.tracks) {
				for (const a of tr.acts) {
					if (a.anim !== "board") {
						continue;
					}
					boards += 1;
					// Down with it in both hands, not yet dribbling or passing.
					for (const t of [a.t1 - 300, a.t1 - 20]) {
						const st = evalPlayer(tl, pid, t);
						assert.strictEqual(st.anim, "board", `${seed} ${t}`);
						assert.isTrue(st.holding, `${seed} ${pid} at ${t}`);
						assert.strictEqual(st.z, 0, `${seed} ${t}`);
					}
				}
			}
			assert.isAbove(boards, 10);
		}
	}, 60_000);
});
