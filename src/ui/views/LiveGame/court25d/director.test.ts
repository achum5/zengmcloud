import { assert, describe, test } from "vitest";
import {
	compileCourt,
	isLineItem,
	snapForCursor,
	targetForCursor,
	type CourtTimeline,
} from "./director.ts";
import {
	bodyPoint,
	evalBall,
	evalPlayer,
	handWorld,
	heldBall,
	offenseAt,
	poseOf,
	tensionAt,
	withBody,
} from "./evaluate.ts";
import { crowdAt } from "./scene.ts";
import { COURT_H, COURT_W, FT_LINE_DEPTH, RIM_Z, rimX } from "./geometry.ts";
import { bodyOf, posed, skeleton } from "./poses.ts";
import { compile, fakeGame, gidOf } from "./testGame.ts";
import { finishOf } from "../../../util/liveGameWording.basketball.ts";

const body = bodyOf();
const bodyFor = () => body;

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
	});

	test("the same game stages the same way every time (every device agrees)", () => {
		const a = compile("same").tl;
		const b = compile("same").tl;
		assert.strictEqual(JSON.stringify(a.beats), JSON.stringify(b.beats));
		assert.strictEqual(JSON.stringify(a.ball), JSON.stringify(b.ball));
		assert.strictEqual(
			JSON.stringify([...a.tracks.values()]),
			JSON.stringify([...b.tracks.values()]),
		);
	});

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
	});

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
	});

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
	});

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
				for (const p of [m.from, m.to]) {
					const team = tr.team;
					const depth = team === 0 ? p.x : COURT_W - p.x;
					const out =
						depth < 14
							? Math.abs(p.y - 25) - 22
							: Math.hypot(depth - 5.25, p.y - 25) - 23.75;
					assert.isAbove(out, 0.5, `drift inside the line at ${m.t0}`);
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
	});

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
				// Out of the lane, to a man who catches it and lets it go.
				const P = at(passer, pass.t0);
				const s0 = at(shooter, pass.t1);
				const s1 = at(shooter, shot.t0);
				if (
					Math.hypot(P.x - rimX(team), P.y - COURT_H / 2) > 17 ||
					Math.hypot(s1.x - s0.x, s1.y - s0.y) > 3
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
	});

	// What just happened gets a moment at real speed before the picture
	// hurries on: a second, and longer after a basket - the ball down
	// through the net, the scorer turning back up the floor.
	test("after a play the picture holds a moment before it hurries on", () => {
		const { tl } = compile("cuts", 160);
		let afterScore = 0;
		for (const [a] of tl.fast) {
			const before = tl.beats.filter((b) => b.actionStart <= a);
			for (const b of before) {
				const score = /^(fg|tp)/.test(b.type) || b.type === "ft";
				assert.isAtLeast(a - b.end, (score ? 1600 : 1000) - 1e-6, `${b.type}`);
			}
			const last = before.at(-1);
			if (last && /^(fg|tp)/.test(last.type)) {
				afterScore += 1;
			}
		}
		assert.isAbove(afterScore, 10);
	});

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
	});

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
	});

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
	});
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
	});

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
	});

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
	});

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
	});

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
	});

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
				const shot = acts.find((a) => a.anim === "shoot");
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
	});

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
	});

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
	});

	// Running, sliding, dribbling, a man still talks with a hand: the
	// screener's man points out the screen, a switch or a man getting back
	// calls out who he has, an open man puts a hand up for it. One arm - never
	// the one on the ball - and the rest of him goes on as it was.
	test("players talk with a hand on the move", () => {
		const { tl } = compile("a", 140);
		const kinds = { point: 0, hand: 0, wave: 0 };
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
		assert.isAtLeast(onTheMove, 60);
	}, 60_000);

	test("flat out, a player sprints - bounding off the floor stride to stride", () => {
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
				let top = 0;
				let low = Infinity;
				for (let t = m.t0 + secs * 250; t < m.t1 - secs * 250; t += 20) {
					const st = evalPlayer(tl, pid, t);
					if (st.anim !== "sprint" && st.anim !== "run") {
						continue;
					}
					assert.strictEqual(st.anim, d / secs >= 19 ? "sprint" : "run");
					top = Math.max(top, st.z);
					low = Math.min(low, st.z);
				}
				if (d / secs >= 19) {
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
	});

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
	});

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
	});

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
	});
});
