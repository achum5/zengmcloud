import { assert, describe, test } from "vitest";
import {
	compileCourt,
	linesBetween,
	snapForCursor,
	targetForCursor,
	type CourtPlayer,
	type CourtTimeline,
	type RawEvent,
} from "./director.ts";
import {
	FLOW_MS,
	evalBall,
	evalPlayer,
	fastAt,
	floorSpotOf,
	offenseAt,
} from "./evaluate.ts";
import {
	buildClocks,
	gameClockAt,
	inboundCaught,
	LEAD_MOST,
	runsOn,
} from "./clock.ts";
import { attackDir, COURT_H, COURT_W, rimX } from "./geometry.ts";
import { stageGame } from "./stepper.ts";
import { compile, fakeGame, gidOf, stepped } from "./testGame.ts";
import { finishOf } from "../../../util/liveGameWording.basketball.ts";
import { bodyFor, cutBetween, sampleTimes } from "./directorTesting.ts";

describe("3D director", () => {
	// A saved replay is the play-by-play read back out of the database and
	// staged again, on whatever device, after whatever else was watched.
	test("a saved replay stages exactly like the game did, every viewing", () => {
		const live = compile("replay");
		compile("another game in between");
		// (IndexedDB stores a structured clone.)
		const saved = structuredClone(live.events);
		const again = stageGame({
			events: saved,
			players: live.players,
			gid: gidOf("replay"),
			step: stepped,
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
	}, 480_000);

	// Off an offensive board the rest spread out for the kick-out - but the
	// man who gets the next shot in there (the dump-off, a post-up) stays
	// in for it: not out to the arc with them and straight back in again.
	test("off an offensive board, the man with the next shot in close stays in close", () => {
		let near = 0;
		const out: string[] = [];
		for (const gid of [1, 2, 3]) {
			const players: CourtPlayer[] = [];
			const POS = ["PG", "SG", "SF", "PF", "C"];
			const events: RawEvent[] = [{ type: "init", boxScore: {} }];
			for (const raw of [0, 1] as const) {
				for (let j = 0; j < 5; j++) {
					const pid = raw * 10 + j + 1;
					players.push({ pid, team: raw === 0 ? 1 : 0, pos: POS[j] });
					events.push({ type: "stat", t: raw, pid, s: "gs", amt: 1 });
				}
			}
			events.push({ type: "jumpBall", t: 0, pid: 5, pid2: 15, clock: 720 });
			// Every one of them off the board, every other one with the shot
			// in: a miss, the board, and the shot in close 2.5 s on; then the
			// other end.
			let c = 718;
			let k = 0;
			for (let r = 1; r <= 5; r++) {
				for (let y = 1; y <= 5; y++) {
					if (y === r) {
						continue;
					}
					const s = [1, 2, 3, 4, 5].find((p) => p !== r && p !== y)!;
					const zone = k % 2 === 0 ? "AtRim" : "LowPost";
					const b = 11 + (k % 5);
					events.push(
						{
							type: "fgaMidRange",
							t: 0,
							pid: s,
							clock: c - 8,
							desperation: false,
						},
						{ type: "missMidRange", t: 0, pid: s, clock: c - 9 },
						{ type: "orb", t: 0, pid: r, clock: c - 9.5 },
						{
							type: `fga${zone}`,
							t: 0,
							pid: y,
							clock: c - 12,
							desperation: false,
						},
						{ type: `fg${zone}`, t: 0, pid: y, pidAst: r, clock: c - 12.5 },
						{ type: "stat", t: 0, pid: y, s: "pts", amt: 2 },
						{
							type: "fgaMidRange",
							t: 1,
							pid: b,
							clock: c - 24,
							desperation: false,
						},
						{ type: "fgMidRange", t: 1, pid: b, clock: c - 25 },
						{ type: "stat", t: 1, pid: b, s: "pts", amt: 2 },
					);
					c -= 26;
					k += 1;
				}
			}
			events.push({ type: "gameOver" });
			const tl = compileCourt({ events, players, gid });
			tl.beats.forEach((bt, j) => {
				if (bt.type !== "orb") {
					return;
				}
				const shot = tl.beats
					.slice(j + 1)
					.find((x) => x.type.startsWith("fga"))!;
				const y = events[shot.i]!.pid as number;
				const rim = { x: rimX(tl.tracks.get(y)!.team), y: COURT_H / 2 };
				const off = (t: number) => {
					const p = evalPlayer(tl, y, t);
					return Math.hypot(p.x - rim.x, p.y - rim.y);
				};
				// In close as it comes off: no farther out than 19 ft (the arc
				// is 22-24) till he shoots.
				if (off(bt.actionStart) >= 17) {
					return;
				}
				near += 1;
				for (let t = bt.actionStart; t < shot.actionStart; t += 100) {
					if (off(t) > 19) {
						out.push(`${gid} ${Math.round(bt.actionStart)} ${y}`);
						break;
					}
				}
			});
		}
		assert.isAtLeast(near, 15);
		assert.deepEqual(out, []);
	}, 480_000);

	// The ball brought over half court and the trip a few seconds old, the
	// defense is back with it: nobody stays at the other end guarding
	// nobody, or runs back there after a man left behind - and nobody on the
	// offense stands around back there either (the trailer comes up).
	test("once the ball is over half court, nobody is left at the other end", () => {
		for (const seed of ["a", "b"]) {
			const { tl } = compile(seed, 140);
			const tracks = [...tl.tracks.values()];
			// (Dead balls: a substitution, the walk to the line - and a timeout
			// or the break between periods, the ball taken to the huddle, till
			// it is inbounded from the sideline after it.)
			const dead: [number, number][] = tl.beats
				.filter(
					(b) => b.type === "sub" || b.type === "ft" || b.type === "missFt",
				)
				.map((b) => [b.preStart, b.end]);
			for (const b of tl.beats) {
				if (
					b.type !== "timeout" &&
					b.type !== "endOfPeriod" &&
					b.type !== "period" &&
					b.type !== "overtime"
				) {
					continue;
				}
				const inbound = tl.ball.find((s) => {
					if (
						s.kind !== "fly" ||
						s.t0 < b.preStart ||
						!("pid" in s.from) ||
						!("pid" in s.to)
					) {
						return false;
					}
					const P = evalPlayer(tl, s.from.pid, s.t0);
					return P.y < 0 || P.y > COURT_H || P.x < 0 || P.x > COURT_W;
				});
				dead.push([b.preStart, inbound?.kind === "fly" ? inbound.t1 : b.end]);
			}
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
					dead.some(([a, z]) => t >= a - 500 && t <= z + 500)
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
			// (Getting back on a break, a step behind the ball - all the way
			// from the far baseline, flat out, off a block down there.)
			assert.isBelow(back / n, 0.03, seed);
			assert.isAtMost(longD, 2800, seed);
			// (A trip fouled or lost seconds in can catch the trailer still on
			// his way up.)
			assert.isAtMost(longO, 1800, seed);
		}
	}, 480_000);

	// A spot the play has back in the backcourt, with the ball over half
	// court already, is half court for him (nobody goes back over it) - but a
	// man over it already trails from where he is: not off along the line to
	// the sideline for it.
	test("over half court already, nobody runs along the line for a backcourt spot", () => {
		const along: string[] = [];
		for (const seed of ["a", "b", "c", "d"]) {
			const { tl } = compile(seed, 140);
			// (Over once the game is: walking over to shake hands is no run for
			// a spot.)
			const over =
				tl.beats.findLast((b) => b.type === "gameOver")?.actionStart ??
				Infinity;
			for (const [pid, tr] of tl.tracks) {
				for (const m of tr.moves) {
					const x0 = m.from.x - COURT_W / 2;
					const x1 = m.to.x - COURT_W / 2;
					if (
						m.t0 < over &&
						Math.abs(Math.abs(x1) - 3.5) < 0.05 &&
						Math.abs(x0) >= 3 &&
						Math.sign(x0) === Math.sign(x1) &&
						Math.abs(m.to.y - m.from.y) >= 6 &&
						Math.abs(x1 - x0) <= 3
					) {
						along.push(`${seed} ${Math.round(m.t0)} ${pid}`);
					}
				}
			}
		}
		assert.deepEqual(along, []);
	}, 480_000);

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
	}, 480_000);

	// And he gets going round and slows coming onto it, like a man - not
	// turning flat out from a standstill in a frame and stopped dead in the
	// next, like a turret.
	test("turning eases in and out - never flat out from one frame to the next", () => {
		const { tl } = compile("turns", 40);
		const pids = [...tl.tracks.keys()];
		const STEP = 16;
		// Turning speed (radians a second) gained or lost in a frame.
		const JERK = 4;
		let prev = new Map<number, { yaw: number; rate?: number }>();
		let frames = 0;
		let jerks = 0;
		let quick = 0;
		for (let t = 0; t < Math.min(tl.end, 240_000); t += STEP) {
			if (fastAt(tl, t) > 1 || cutBetween(tl, t - STEP, t)) {
				prev = new Map();
				continue;
			}
			const cur = new Map<number, { yaw: number; rate?: number }>();
			for (const pid of pids) {
				const st = evalPlayer(tl, pid, t);
				if (!st.shown) {
					continue;
				}
				const p = prev.get(pid);
				let rate: number | undefined;
				if (p) {
					let d = st.yaw - p.yaw;
					d -= Math.round(d / (Math.PI * 2)) * Math.PI * 2;
					rate = (d * 1000) / STEP;
					if (Math.abs(rate) > 6) {
						quick += 1;
					}
					if (p.rate !== undefined && st.anim !== "spinMove") {
						frames += 1;
						if (Math.abs(rate - p.rate) > JERK) {
							jerks += 1;
						}
					}
				}
				cur.set(
					pid,
					rate === undefined ? { yaw: st.yaw } : { yaw: st.yaw, rate },
				);
			}
			prev = cur;
		}
		assert.isAbove(frames, 50_000);
		// (Plenty of quick turns - just none begun or ended in a frame.)
		assert.isAbove(quick, 1000, `${quick}`);
		assert.isBelow(jerks, frames * 0.001, `${jerks} of ${frames}`);
	}, 960_000);

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
				// In the air: already running down to the result's reading (or
				// a little past it, the picture taking longer - see LEAD_MOST).
				const air = next!.actionStart - 1;
				assert.isBelow(at(air), e.clock, `${e.type} at ${b.actionStart}`);
				assert.isAtLeast(at(air), n.clock - LEAD_MOST);
				live += 1;
			} else if (/^(pf|ft|missFt)/.test(e.type) && n.clock === e.clock) {
				// (Stopped at it - or as far ahead of it as it had got - and on
				// through the dead time to the next free throw.)
				const stopped = at(b.actionStart);
				assert.strictEqual(at(mid), stopped, `${e.type} at ${b.actionStart}`);
				assert.isAtMost(stopped, e.clock);
				assert.isAtLeast(stopped, e.clock - LEAD_MOST);
				if (/^(ft|missFt)$/.test(n.type)) {
					assert.strictEqual(
						at(next!.actionStart),
						stopped,
						`${e.type} to ${n.type} at ${b.actionStart}`,
					);
				}
				whistles += 1;
			}
		});
		assert.isAbove(live, 20);
		assert.isAbove(whistles, 10);
	}, 240_000);

	// THE HEAVE AT THE BUZZER. Up in the time the sim gives it, from when
	// the clock starts on it - running already off a board, from the inbound
	// after a whistle (off a basket, no longer than it takes to get it out
	// of the net and in) - from way out, the ball out of his hands before the
	// horn, and the clock out as it flies: not crawling to 0.0 the whole way.
	test("a heave at the buzzer goes up as time runs out", () => {
		const players: CourtPlayer[] = [];
		const POS = ["PG", "SG", "SF", "PF", "C"];
		const events: RawEvent[] = [{ type: "init", boxScore: {} }];
		for (const raw of [0, 1] as const) {
			for (let j = 0; j < 5; j++) {
				const pid = raw * 10 + j + 1;
				players.push({ pid, team: raw === 0 ? 1 : 0, pos: POS[j] });
				events.push({ type: "stat", t: raw, pid, s: "gs", amt: 1 });
			}
		}
		events.push({ type: "jumpBall", t: 0, pid: 5, pid2: 15, clock: 720 });
		// How the other end ends, c on the clock, before the heave.
		const ends: ((c: number) => RawEvent[])[] = [
			// A miss and their board,
			(c) => [
				{ type: "fgaMidRange", t: 0, pid: 2, clock: c, desperation: false },
				{ type: "missMidRange", t: 0, pid: 2, clock: c - 0.8 },
				{ type: "drb", t: 1, pid: 15, clock: c - 1.2 },
			],
			// a miss out of bounds off the shooting team,
			(c) => [
				{ type: "fgaTp", t: 0, pid: 3, clock: c, desperation: false },
				{ type: "missTp", t: 0, pid: 3, clock: c - 1 },
				{ type: "outOfBounds", t: 0, on: "offense", clock: c - 1.3 },
			],
			// a basket.
			(c) => [
				{ type: "fgaAtRim", t: 0, pid: 4, clock: c, desperation: false },
				{ type: "fgAtRim", t: 0, pid: 4, clock: c - 0.5 },
				{ type: "stat", t: 0, pid: 4, s: "pts", amt: 2 },
			],
		];
		const lefts = [3.6, 2.4, 4.4, 1.8, 3.1, 2.7];
		let period = 1;
		for (let k = 0; k < lefts.length * ends.length; k++) {
			// (Up and down the floor till then.)
			for (let c = 130; c > 60; c -= 22) {
				events.push(
					{ type: "fgaMidRange", t: 0, pid: 1, clock: c, desperation: false },
					{ type: "missMidRange", t: 0, pid: 1, clock: c - 1 },
					{ type: "drb", t: 1, pid: 14, clock: c - 1.5 },
					{ type: "fgaTp", t: 1, pid: 12, clock: c - 12, desperation: false },
					{ type: "tp", t: 1, pid: 12, clock: c - 13 },
					{ type: "stat", t: 1, pid: 12, s: "pts", amt: 3 },
				);
			}
			const left = lefts[k % lefts.length]!;
			const shooter = 11 + (k % 5);
			events.push(
				...ends[k % ends.length]!(left + 1.3),
				{
					type: "fgaTp",
					t: 1,
					pid: shooter,
					clock: 0.1,
					desperation: true,
				},
				{ type: "missTp", t: 1, pid: shooter, clock: 0 },
				{ type: "endOfPeriod", t: 1, reason: "noShot", clock: 0 },
			);
			period += 1;
			events.push({ type: "period", period, clock: 720 });
		}
		events.push({ type: "gameOver" });
		const tl = compileCourt({ events, players, gid: 7 });
		const clocks = buildClocks(tl, events);
		let heaves = 0;
		let far = 0;
		tl.beats.forEach((b, j) => {
			const e = events[b.i]!;
			if (e.desperation !== true) {
				return;
			}
			heaves += 1;
			const prior = tl.beats
				.slice(0, j)
				.findLast((x) => typeof events[x.i]!.clock === "number")!;
			const p = events[prior.i]!;
			const gap = (p.clock as number) - (e.clock as number);
			const from = runsOn(p)
				? prior.actionStart
				: inboundCaught(tl.ball, b.preStart, b.actionStart)!;
			const took = b.actionStart - from;
			const what = `${p.type} ${gap.toFixed(1)}s at ${Math.round(b.actionStart)}`;
			if (/^fg(?!a)/.test(p.type)) {
				assert.isAtMost(took, Math.max(3600, gap * 1000 + 300), what);
			} else {
				assert.isAtMost(took, gap * 1000 + 300, what);
				assert.isAtLeast(took, gap * 1000 - 600, what);
			}
			const shooter = e.pid as number;
			const P = evalPlayer(tl, shooter, b.actionStart);
			const rim = { x: rimX(tl.tracks.get(shooter)!.team), y: COURT_H / 2 };
			const act = tl.tracks
				.get(shooter)!
				.acts.find(
					(a) => a.t0 >= b.actionStart - 100 && a.t0 <= b.actionStart + 100,
				);
			if (Math.hypot(P.x - rim.x, P.y - rim.y) > 40) {
				assert.strictEqual(act?.anim, "heave", what);
				far += 1;
			}
			const release = tl.ball.find(
				(s) =>
					s.t0 >= b.actionStart &&
					s.kind === "fly" &&
					"pid" in s.from &&
					s.from.pid === shooter,
			)!.t0;
			assert.isAbove(gameClockAt(clocks, release - 20)!, 0, what);
			assert.strictEqual(gameClockAt(clocks, release + 300), 0, what);
		});
		assert.strictEqual(heaves, lefts.length * ends.length);
		assert.isAtLeast(far, 6);
	}, 480_000);

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
	}, 240_000);

	test("a jump shot that is blocked is got as it leaves his hand - not hung up in the air till the blocker gets there", () => {
		let blocks = 0;
		for (const seed of ["a", "b", "c"]) {
			const { tl } = compile(seed, 150);
			for (const s of tl.ball) {
				if (s.kind !== "fly" || !("pid" in s.from) || !("pid" in s.to)) {
					continue;
				}
				const by = tl.tracks.get(s.to.pid);
				const shooter = tl.tracks.get(s.from.pid);
				if (
					!by ||
					!shooter ||
					by.team === shooter.team ||
					!by.acts.some(
						(a) => a.anim === "block" && a.t0 <= s.t1 && a.t1 >= s.t1,
					)
				) {
					continue;
				}
				assert.isAtMost(s.t1 - s.t0, 150, `${seed} at ${s.t0}`);
				blocks += 1;
			}
		}
		assert.isAbove(blocks, 8);
	}, 480_000);

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
	}, 240_000);

	// A pull-up three off a ball screen: he comes off it - a dribble or two
	// past the big's shoulder - and up behind it, not stood where he was all
	// along shooting over it.
	// Caught for a three, he is behind the line already - got out there as
	// it came, met it there - and goes straight up: not caught a step inside
	// and backpedalled out with it before the shot (about every other
	// catch-and-shoot before). And a man taking it a long way back out
	// turns and dribbles it out, never backpedals the whole way with it.
	test("a three is caught behind the line, and nobody backpedals a long way with the ball", () => {
		let shots = 0;
		const backed: string[] = [];
		const long: string[] = [];
		for (const seed of ["a", "b", "c"]) {
			const { tl } = compile(seed, 150);
			for (const tr of tl.tracks.values()) {
				for (const a of tr.acts) {
					if (a.anim !== "shoot") {
						continue;
					}
					const c = tr.acts.find(
						(x) =>
							x.anim === "catch" && x.t1 <= a.t0 + 20 && x.t1 > a.t0 - 1500,
					);
					if (!c) {
						continue;
					}
					shots += 1;
					if (
						tr.moves.some(
							(m) =>
								m.anim === "back" &&
								m.t0 >= c.t0 - 50 &&
								m.t1 <= a.t0 + 50 &&
								!tr.acts.some(
									(x) =>
										x.anim === "stepBack" && x.t0 <= m.t0 + 50 && x.t1 >= m.t0,
								),
						)
					) {
						backed.push(`${seed} ${tr.pid} at ${Math.round(a.t0)}`);
					}
				}
				for (const m of tr.moves) {
					if (
						(m.anim === "dribble" || m.anim === "dribbleWalk") &&
						m.face !== undefined &&
						-(m.to.x - m.from.x) * m.face > 6.5 &&
						-(m.to.x - m.from.x) * m.face >
							Math.hypot(m.to.x - m.from.x, m.to.y - m.from.y) * 0.4
					) {
						long.push(`${seed} ${tr.pid} at ${Math.round(m.t0)}`);
					}
				}
			}
		}
		assert.isAbove(shots, 60);
		assert.isAtMost(backed.length, shots * 0.08, backed.join(", "));
		assert.strictEqual(long.length, 0, long.join(", "));
	}, 960_000);

	// Going up in close, whoever of theirs is right there goes up with him,
	// arms high - not stood there in his slide while the shooter goes up
	// through him. (About half of them did before.)
	test("a man right there as a shot goes up in close goes up with it", () => {
		const CLOSE = new Set([
			"layup",
			"fingerRoll",
			"powerLayup",
			"scoop",
			"reverseLayup",
			"floater",
		]);
		let near = 0;
		const idle: string[] = [];
		for (const seed of ["a", "b", "c"]) {
			const { tl } = compile(seed, 150);
			for (const tr of tl.tracks.values()) {
				for (const a of tr.acts) {
					if (!CLOSE.has(a.anim)) {
						continue;
					}
					const fly = tl.ball.find(
						(s) =>
							s.kind === "fly" &&
							"pid" in s.from &&
							s.from.pid === tr.pid &&
							s.t0 >= a.t0 &&
							s.t0 <= a.t1,
					);
					if (!fly) {
						continue;
					}
					const S = evalPlayer(tl, tr.pid, fly.t0);
					for (const o of tl.tracks.values()) {
						const q = evalPlayer(tl, o.pid, fly.t0);
						if (
							o.team === tr.team ||
							!q.shown ||
							Math.hypot(q.x - S.x, q.y - S.y) > 4.5
						) {
							continue;
						}
						near += 1;
						if (
							!o.acts.some((x) => x.t1 > fly.t0 - 300 && x.t0 < fly.t0 + 500)
						) {
							idle.push(`${seed} ${o.pid} at ${Math.round(fly.t0)}`);
						}
					}
				}
			}
		}
		assert.isAbove(near, 60);
		assert.isAtMost(idle.length, near * 0.1, idle.join(", "));
	}, 960_000);

	test("a pull-up three off a ball screen comes off the screen first", () => {
		let n = 0;
		const stood: string[] = [];
		// (Over eight games: off a ball screen, for three, only now and then.)
		for (const seed of ["a", "b", "c", "d", "e", "f", "g", "h"]) {
			const { tl } = compile(seed, 150);
			const tracks = [...tl.tracks.values()];
			for (const s of tl.ball) {
				if (s.kind !== "fly" || !("pid" in s.from) || "pid" in s.to) {
					continue;
				}
				const rx = s.to.x < COURT_W / 2 ? rimX(0) : rimX(1);
				const shooter = s.from.pid;
				const tr = tl.tracks.get(shooter)!;
				const P = evalPlayer(tl, shooter, s.t0);
				if (
					Math.hypot(P.x - rx, P.y - COURT_H / 2) < 23.9 ||
					!tr.acts.some(
						(a) => a.anim === "shoot" && a.t0 <= s.t0 && a.t1 > s.t0,
					)
				) {
					continue;
				}
				// Off the dribble: his own ball the last two seconds.
				const caught = tl.ball.some(
					(q) =>
						q.kind === "fly" &&
						"pid" in q.to &&
						q.to.pid === shooter &&
						q.t1 > s.t0 - 2000 &&
						q.t1 < s.t0,
				);
				// A screen for him: a teammate set beside him just before.
				const screen = tracks.some(
					(o) =>
						o.team === tr.team &&
						o !== tr &&
						o.acts.some((a) => {
							if (a.anim !== "screen" || a.t1 < s.t0 - 2500 || a.t0 > s.t0) {
								return false;
							}
							const at = Math.max(a.t0, s.t0 - 2500);
							const Q = evalPlayer(tl, o.pid, at);
							const H = evalPlayer(tl, shooter, at);
							return Math.hypot(Q.x - H.x, Q.y - H.y) < 6;
						}),
				);
				if (caught || !screen) {
					continue;
				}
				n += 1;
				// (Over his last strides and the gather into the shot.)
				const B = evalPlayer(tl, shooter, s.t0 - 2200);
				if (Math.hypot(P.x - B.x, P.y - B.y) < 3) {
					stood.push(`${seed}: ${shooter} at ${Math.round(s.t0)}`);
				}
			}
		}
		assert.isAbove(n, 10);
		assert.isAtMost(stood.length, n * 0.25, stood.join("; "));
	}, 960_000);

	// Off the ball on the offense, a man going on the same way after a moment
	// - nothing done there, no ball - runs on into it rather than pulling up
	// dead and setting off again, unless he is already going no faster than
	// an easy run.
	test("off the ball, a man on his way doesn't stop dead between two runs the same way", () => {
		const RUNS = new Set(["run", "sprint", "jog"]);
		let pairs = 0;
		const stops: string[] = [];
		for (const seed of ["a", "b", "c", "d"]) {
			const { tl } = compile(seed, 150);
			const holds = new Map<number, [number, number][]>();
			tl.ball.forEach((b, i) => {
				if (b.kind === "hold") {
					const list = holds.get(b.pid) ?? [];
					list.push([b.t0, tl.ball[i + 1]?.t0 ?? Infinity]);
					holds.set(b.pid, list);
				}
			});
			for (const tr of tl.tracks.values()) {
				const ms = tr.moves;
				for (let i = 0; i + 1 < ms.length; i++) {
					const a = ms[i]!;
					const b = ms[i + 1]!;
					const gap = b.t0 - a.t1;
					const la = Math.hypot(a.to.x - a.from.x, a.to.y - a.from.y);
					const lb = Math.hypot(b.to.x - b.from.x, b.to.y - b.from.y);
					if (
						!RUNS.has(a.anim) ||
						!RUNS.has(b.anim) ||
						gap < 0 ||
						gap > 1500 ||
						la < 6 ||
						lb < 4 ||
						Math.hypot(a.to.x - b.from.x, a.to.y - b.from.y) > 0.25 ||
						offenseAt(tl, a.t1) !== tr.team ||
						((a.to.x - a.from.x) * (b.to.x - b.from.x) +
							(a.to.y - a.from.y) * (b.to.y - b.from.y)) /
							(la * lb) <
							0.5 ||
						tr.acts.some((x) => x.t1 > a.t1 - 50 && x.t0 < b.t0 + 50) ||
						(holds.get(tr.pid) ?? []).some(([h0, h1]) => h1 > a.t0 && h0 < b.t0)
					) {
						continue;
					}
					pairs += 1;
					// (Faster than an easy run on average into the stop.)
					if (gap > FLOW_MS && la / ((a.t1 - a.t0) / 1000) > 8.2) {
						stops.push(
							`${seed}: ${tr.pid} at ${Math.round(a.t1)} ${Math.round(gap)}ms`,
						);
					}
				}
			}
		}
		assert.isAbove(pairs, 100);
		assert.isAtMost(stops.length, pairs * 0.02, stops.join("; "));
	}, 960_000);

	// A jumper off a handoff goes up off the hip of the man who gave it, a
	// dribble or two on the way he was going: not stood with it at his side
	// for a second or two while he turns to screen, nor up off the catch with
	// the two of them shoulder to shoulder.
	test("a pull-up off a handoff comes off the man who gave it", () => {
		const shots: string[] = [];
		const slow: string[] = [];
		const close: string[] = [];
		// (Over six games: a handoff into a jumper only now and then.)
		for (const seed of ["a", "b", "c", "d", "e", "f"]) {
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
				// Next the ball is let go of: a jumper of his.
				const up = tl.ball
					.slice(i + 1)
					.find((z) => !(z.kind === "hold" && z.pid === recv));
				if (
					up?.kind !== "fly" ||
					"pid" in up.to ||
					up.t0 - s.t1 > 3500 ||
					!tl.tracks
						.get(recv)!
						.acts.some(
							(a) => a.anim === "shoot" && a.t0 <= up.t0 && a.t1 > up.t0,
						)
				) {
					return;
				}
				const at = `${seed}: ${recv} at ${Math.round(up.t0)}`;
				shots.push(at);
				// (Stood there with it: the longest he is still, off the catch
				// till he starts up into his jumper - a dribble on from it, a
				// screen to come off, a drive and up, all fine.)
				let stood = 0;
				let still = 0;
				const rise =
					tl.tracks
						.get(recv)!
						.acts.find(
							(a) => a.anim === "shoot" && a.t0 <= up.t0 && a.t1 > up.t0,
						)?.t0 ?? up.t0;
				let last = evalPlayer(tl, recv, s.t1);
				for (let t = s.t1 + 100; t <= rise; t += 100) {
					const P = evalPlayer(tl, recv, t);
					still =
						Math.hypot(P.x - last.x, P.y - last.y) < 0.1 ? still + 100 : 0;
					stood = Math.max(stood, still);
					last = P;
				}
				if (stood > 1500) {
					slow.push(`${at} (stood ${stood} ms)`);
				}
				const A = evalPlayer(tl, giver, up.t0);
				const B = evalPlayer(tl, recv, up.t0);
				if (Math.hypot(A.x - B.x, A.y - B.y) < 2.5) {
					close.push(at);
				}
			});
		}
		assert.isAtLeast(shots.length, 8);
		assert.isAtMost(slow.length, 1, slow.join("; "));
		assert.isAtMost(close.length, 1, close.join("; "));
	}, 960_000);

	// A drive into the lane goes in on its own: nobody of his running in at
	// his shoulder the whole way - the man who set him the screen or gave him
	// the handoff rolls in to the far side of the rim, not down the lane with
	// him, the two of them and both their men one pack.
	test("a drive into the lane has no teammate running in at his shoulder", () => {
		let drives = 0;
		const along: string[] = [];
		for (const seed of ["a", "b", "c", "d"]) {
			const { tl } = compile(seed, 150);
			const tracks = [...tl.tracks.values()];
			for (const tr of tracks) {
				const mates = tracks.filter((o) => o !== tr && o.team === tr.team);
				for (const m of tr.moves) {
					const rim = Math.min(
						...[rimX(0), rimX(1)].map((x) =>
							Math.hypot(m.to.x - x, m.to.y - COURT_H / 2),
						),
					);
					if (
						m.anim !== "dribble" ||
						Math.hypot(m.to.x - m.from.x, m.to.y - m.from.y) < 6 ||
						rim > 12
					) {
						continue;
					}
					drives += 1;
					// Over the back half of it: a teammate within 4.5 ft of him,
					// both going the same way.
					const n = new Map<number, number>();
					for (let i = 0; i < 6; i++) {
						const t = m.t0 + (m.t1 - m.t0) * (0.5 + i / 12);
						const A = evalPlayer(tl, tr.pid, t);
						const A0 = evalPlayer(tl, tr.pid, t - 100);
						const ax = A.x - A0.x;
						const ay = A.y - A0.y;
						for (const o of mates) {
							const B = evalPlayer(tl, o.pid, t);
							const B0 = evalPlayer(tl, o.pid, t - 100);
							const bx = B.x - B0.x;
							const by = B.y - B0.y;
							const va = Math.hypot(ax, ay);
							const vb = Math.hypot(bx, by);
							if (
								Math.hypot(A.x - B.x, A.y - B.y) < 4.5 &&
								va > 0.4 &&
								vb > 0.4 &&
								(ax * bx + ay * by) / (va * vb) > 0.6
							) {
								n.set(o.pid, (n.get(o.pid) ?? 0) + 1);
							}
						}
					}
					const most = Math.max(0, ...n.values());
					if (most >= 3) {
						along.push(`${seed}: ${tr.pid} at ${Math.round(m.t0)}`);
					}
				}
			}
		}
		assert.isAbove(drives, 120);
		assert.isAtMost(along.length, drives * 0.035, along.join("; "));
	}, 960_000);

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
	}, 240_000);

	// The ball left bouncing at a basket or a whistle goes off the floor
	// away from whoever is stood there - the man dunked on has stepped back
	// out from under the rim, a man having words holds his ground - not on
	// through him while he stands there.
	test("a ball bouncing loose at a stop goes round a man stood there, not through him", () => {
		const AT_IT = new Set([
			"poke",
			"strip",
			"dunk",
			"dunk1",
			"tomahawk",
			"windmill",
			"pickup",
			"snatch",
		]);
		const through: string[] = [];
		let loose = 0;
		for (const seed of ["a", "b", "c"]) {
			const { tl } = compile(seed);
			const tracks = [...tl.tracks.values()];
			const since = new Map<number, number>();
			let k = 0;
			for (let t = 0; t < tl.end; t += 50) {
				while (k + 1 < tl.ball.length && tl.ball[k + 1]!.t0 <= t) {
					k++;
				}
				const seg = tl.ball[k];
				const B =
					fastAt(tl, t) <= 1.001 &&
					(seg?.kind === "bounce" || seg?.kind === "rest")
						? evalBall(tl, t, bodyFor)
						: undefined;
				const high = B !== undefined && B.z >= 1 && B.z <= 6.3;
				loose += high ? 1 : 0;
				for (const tr of tracks) {
					const p = high ? floorSpotOf(tr, t) : undefined;
					if (
						p &&
						Math.hypot(p.x - B!.x, p.y - B!.y) < 0.75 &&
						// (Stood there - not the man at it himself: knocking it
						// loose, coming down from his own dunk under it, picking
						// it up.)
						!AT_IT.has(evalPlayer(tl, tr.pid, t).anim)
					) {
						const t0 = since.get(tr.pid) ?? t;
						since.set(tr.pid, t0);
						if (t - t0 === 150) {
							through.push(`${seed} pid ${tr.pid} at ${t0}`);
						}
					} else {
						since.delete(tr.pid);
					}
				}
			}
		}
		assert.isAbove(loose, 1000);
		assert.isAtMost(through.length, 1, through.join("; "));
	}, 480_000);

	// Once it is over half court, the ball never goes back over: no set has
	// the man with it dribble it back into the backcourt to start from there,
	// no break has him bring it back to the spot it brings it up to.
	test("over half court with it, nobody takes it back over", () => {
		for (const seed of ["a", "b", "c", "d"]) {
			const { tl } = compile(seed, 150);
			let k = 0;
			for (const b of tl.beats) {
				let side: number | undefined;
				let over = false;
				for (let t = b.preStart; t < b.actionStart; t += 100) {
					while (k + 1 < tl.ball.length && tl.ball[k + 1]!.t0 <= t) {
						k += 1;
					}
					const seg = tl.ball[k]!;
					if (seg.kind !== "hold") {
						if (seg.kind !== "fly") {
							over = false;
						}
						continue;
					}
					const s = offenseAt(tl, t);
					if (s !== side) {
						side = s;
						over = false;
					}
					const p = evalPlayer(tl, seg.pid, t);
					const at = (p.x - COURT_W / 2) * attackDir(s);
					const dribbling = tl.tracks
						.get(seg.pid)!
						.moves.some((m) => m.t0 <= t && t < m.t1 && m.anim === "dribble");
					const caught = seg.t0 > t - 100 && tl.ball[k - 1]?.kind === "fly";
					if (at > 2 && (dribbling || caught)) {
						over = true;
					}
					assert.isFalse(
						over && at < -2 && dribbling,
						`${seed}: ${seg.pid} back over half court with it at ${Math.round(t)}`,
					);
				}
			}
		}
	}, 960_000);

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
	}, 240_000);

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
	}, 240_000);

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
		for (let t = 0; t < tl.end; t += 100) {
			const b = evalBall(tl, t, bodyFor);
			// (Run through fast, no arm says anything - see ARM_FAST.)
			if (b.holder === undefined || fastAt(tl, t) > 1) {
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
		assert.isAbove(cand, 200);
		assert.isAbove(denied / cand, 0.4);
		assert.isBelow(flips, 15);
	}, 480_000);

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
						assert.include(["board", "boardOne"], st.anim, `${seed} ${t}`);
						assert.isTrue(st.holding, `${seed} ${pid} at ${t}`);
						assert.strictEqual(st.z, 0, `${seed} ${t}`);
					}
				}
			}
			assert.isAbove(boards, 10);
		}
	}, 240_000);
});
