import { assert, describe, test } from "vitest";
import { type CourtTimeline } from "./director.ts";
import {
	bodyPoint,
	evalBall,
	evalPlayer,
	fastAt,
	handWorld,
	heldBall,
	offenseAt,
	withBody,
} from "./evaluate.ts";
import { COURT_H, COURT_W, RIM_Z, rimX } from "./geometry.ts";
import { bodyOf, posed, skeleton, LAYUPS } from "./poses.ts";
import { compile } from "./testGame.ts";
import {
	body,
	bodyFor,
	SHOOTING,
	cutBetween,
	sampleTimes,
	OPEN_STOOD_MOST,
} from "./directorTesting.ts";

describe("3D director", () => {
	test("the same game stages the same way every time (every device agrees)", () => {
		const a = compile("same").tl;
		const b = compile("same").tl;
		assert.strictEqual(JSON.stringify(a.beats), JSON.stringify(b.beats));
		assert.strictEqual(JSON.stringify(a.ball), JSON.stringify(b.ball));
		assert.strictEqual(
			JSON.stringify([...a.tracks.values()]),
			JSON.stringify([...b.tracks.values()]),
		);
	}, 240_000);

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
	}, 240_000);

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
		// (Every 20 ms of a whole game: three minutes and more of work.)
	}, 1200_000);

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
	}, 600_000);

	// A man set in a screen or sealing in the post is a body: whoever comes
	// at him runs into him - held up a moment, fighting round him - rather
	// than straight through him.
	test("nobody runs straight through a man set in a screen", () => {
		let screens = 0;
		let contact = 0;
		let close = 0;
		for (const seed of ["a", "b", "c"]) {
			const { tl } = compile(seed, 140);
			const tracks = [...tl.tracks.values()];
			for (const tr of tracks) {
				for (const a of tr.acts) {
					if (a.anim !== "screen" && a.anim !== "postUp") {
						continue;
					}
					screens += 1;
					let closest = Infinity;
					// (From once he has stepped in to set it - as it shows: not
					// run through at many times real speed, the picture hurrying
					// on to the next play, when the men are let be.)
					for (let t = a.t0 + 200; t <= a.t1; t += 50) {
						if (fastAt(tl, t) > 2) {
							continue;
						}
						const S = evalPlayer(tl, tr.pid, t);
						for (const o of tracks) {
							const p = evalPlayer(tl, o.pid, t);
							if (o.team !== tr.team && p.shown) {
								closest = Math.min(closest, Math.hypot(p.x - S.x, p.y - S.y));
							}
						}
					}
					// Never through him; at worst, now and then (about one screen
					// in a hundred), a shoulder brushing his.
					assert.isAtLeast(
						closest,
						0.85,
						`${seed}: through ${tr.pid} at ${a.t0}`,
					);
					if (closest < 1.3) {
						close += 1;
					}
					if (closest < 2.4) {
						contact += 1;
					}
				}
			}
		}
		assert.isAbove(screens, 60);
		assert.isAtMost(close / screens, 0.02);
		// And he is into somebody, often.
		assert.isAbove(contact / screens, 0.25);
	}, 480_000);

	// Coming back on defense, they get back the way a defense does: into the
	// paint and round it, somebody at the rim, picking up their men as they
	// come in - not five in a line across the floor out past the arc, level
	// with the ball, nobody back. (As the ball is brought up to the top of
	// the key, mostly somebody back within 15 feet of the rim.)
	// Off a defensive board or a steal, the five who lost it pick up whoever
	// is coming up the floor their side of it - a break is matched up on the
	// fly - not each off across the floor to find the man he had, through
	// his teammates in the paint.
	test("off a turnover or a board, the defense matches up on the fly - nobody crosses the floor for his own man", () => {
		let changes = 0;
		let crosses = 0;
		// (Eight games: in four, the rate swings a good way either side with
		// any change to where men happen to be.)
		for (const seed of ["a", "b", "c", "d", "e", "f", "g", "h"]) {
			const { events, tl } = compile(seed, 150);
			const pids = [...tl.tracks.keys()];
			for (const b of tl.beats) {
				if (b.type !== "drb" && b.type !== "stl") {
					continue;
				}
				const off = tl.tracks.get(events[b.i]?.pid as number)?.team;
				if (off === undefined) {
					continue;
				}
				changes += 1;
				let prev: Map<number, { x: number; y: number }> | undefined;
				for (let t = b.end; t < b.end + 7000; t += 100) {
					const D =
						fastAt(tl, t) > 1
							? []
							: pids
									.map((pid) => evalPlayer(tl, pid, t))
									.filter(
										(q) =>
											q.team !== off &&
											q.shown &&
											q.x > -1 &&
											q.x < COURT_W + 1,
									);
					if (D.length !== 5) {
						prev = undefined;
						continue;
					}
					if (prev) {
						for (let i = 0; i < 5; i++) {
							for (let j = i + 1; j < 5; j++) {
								const a = D[i]!;
								const c = D[j]!;
								const a0 = prev.get(a.pid);
								const c0 = prev.get(c.pid);
								if (
									a0 &&
									c0 &&
									Math.sign(a.y - c.y) !== Math.sign(a0.y - c0.y) &&
									Math.abs(a.x - c.x) < 6 &&
									Math.hypot(a.x - a0.x, a.y - a0.y) > 0.5 &&
									Math.hypot(c.x - c0.x, c.y - c0.y) > 0.5
								) {
									crosses += 1;
								}
							}
						}
					}
					prev = new Map(D.map((q) => [q.pid, { x: q.x, y: q.y }]));
				}
			}
		}
		assert.isAbove(changes, 300);
		assert.isAtMost(crosses / changes, 0.37, `${crosses} in ${changes}`);
	}, 960_000);

	test("getting back, somebody is back at the rim - not five in a line out at the top", () => {
		let n = 0;
		let back = 0;
		for (const seed of ["a", "b", "c"]) {
			const { events, tl } = compile(seed, 140);
			for (const b of tl.beats) {
				const e = events[b.i];
				if (!e || !/^fga/.test(e.type)) {
					continue;
				}
				// (The rim the shot is at.)
				const shot = tl.ball.find(
					(s) =>
						s.kind === "fly" &&
						"pid" in s.from &&
						s.from.pid === e.pid &&
						!("pid" in s.to) &&
						s.to.z > 6 &&
						s.t0 >= b.preStart - 100 &&
						s.t0 <= b.end + 200,
				);
				if (shot?.kind !== "fly" || "pid" in shot.to) {
					continue;
				}
				const rx = shot.to.x < COURT_W / 2 ? rimX(0) : rimX(1);
				const team = tl.tracks.get(e.pid)!.team;
				for (let t = b.preStart; t < b.actionStart - 500; t += 200) {
					const h = evalBall(tl, t, bodyFor).holder;
					if (h === undefined || tl.tracks.get(h)!.team !== team) {
						continue;
					}
					const H = evalPlayer(tl, h, t);
					const out = Math.abs(H.x - rx);
					if (out < 28 || out > 47) {
						continue;
					}
					n += 1;
					const near = [...tl.tracks.values()].some((tr) => {
						if (tr.team === team) {
							return false;
						}
						const st = evalPlayer(tl, tr.pid, t);
						return st.shown && Math.hypot(st.x - rx, st.y - COURT_H / 2) < 15;
					});
					back += near ? 1 : 0;
				}
			}
		}
		assert.isAbove(n, 150);
		assert.isAbove(back / n, 0.7);
	}, 480_000);

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
			"lowFive",
			"reach",
			"hack",
			"poke",
			"block",
			"dunk",
			"dunk1",
			"dunkPump",
			"dunkPump1",
			"tomahawk",
			"windmill",
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
			"fingerRoll",
			"layupClutch",
			"powerLayup",
			"scoop",
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
	}, 480_000);

	// A screen is set for somebody: the man it is for comes by the
	// screener's shoulder - not past where it was going to be before the
	// screener got there, and not off somewhere it isn't, the screen set for
	// nobody. (Not one turned into off a handoff: the man he gave it to was
	// right there first.)
	test("the man a screen is for comes off it", () => {
		let screens = 0;
		let unused = 0;
		let late = 0;
		for (const seed of ["a", "b", "c", "d"]) {
			const { tl } = compile(seed, 150);
			const set: { team: number; t0: number; used: boolean }[] = [];
			for (const tr of tl.tracks.values()) {
				for (const a of tr.acts) {
					if (
						a.anim !== "screen" ||
						fastAt(tl, a.t0) > 1 ||
						fastAt(tl, a.t1) > 1 ||
						tl.ball.some(
							(b) =>
								b.kind === "hold" &&
								b.pid === tr.pid &&
								b.t0 > a.t0 - 2500 &&
								b.t0 < a.t0,
						)
					) {
						continue;
					}
					screens += 1;
					let best = Infinity;
					let at = 0;
					for (let t = a.t0 - 1500; t <= a.t1 + 400; t += 50) {
						const S = evalPlayer(tl, tr.pid, t);
						for (const q of tl.tracks.values()) {
							if (q.pid === tr.pid || q.team !== tr.team) {
								continue;
							}
							const Q = evalPlayer(tl, q.pid, t);
							const d = Math.hypot(Q.x - S.x, Q.y - S.y);
							if (d < best) {
								best = d;
								at = t;
							}
						}
					}
					set.push({ team: tr.team, t0: a.t0, used: best <= 5 });
					if (best <= 5 && at < a.t0 - 100) {
						late += 1;
					}
				}
			}
			// (Not one of two ways he has to go, the other taken - the far side
			// of a Floppy, read and turned down: one set with it was used.)
			for (const x of set) {
				if (
					!x.used &&
					!set.some(
						(o) => o.used && o.team === x.team && Math.abs(o.t0 - x.t0) < 800,
					)
				) {
					unused += 1;
				}
			}
		}
		assert.isAbove(screens, 60);
		assert.isAtMost(unused, screens * 0.05, `${unused} of ${screens} unused`);
		assert.isAtMost(late, screens * 0.15, `${late} of ${screens} late`);
	}, 960_000);

	// A screen, a post-up, a celebration, words with the official: each is
	// done where he stands, and the pose ends as he sets off again rather
	// than him sliding away across the floor in it.
	// Having words with the official, hands on his hips, a flex for the
	// crowd: he does it where he stands - nobody else stepping round him
	// slides him over in the middle of it. (Off his own dunk, he is still
	// coming down from it as he starts.)
	test("a man reacting where he stands is not slid across the floor doing it", () => {
		const REACT = new Set([
			"protest",
			"hips",
			"point",
			"flex",
			"roar",
			"chestPound",
			"celebrate",
			"clap",
		]);
		let n = 0;
		const slid: string[] = [];
		for (const seed of ["a", "b", "c", "d"]) {
			const { tl } = compile(seed, 150);
			for (const tr of tl.tracks.values()) {
				for (const a of tr.acts) {
					if (
						!REACT.has(a.anim) ||
						fastAt(tl, a.t0) > 1.01 ||
						tr.moves.some((m) => m.t1 > a.t0 && m.t0 < a.t1) ||
						tr.acts.some(
							(b) => SHOOTING.has(b.anim) && b.t1 > a.t0 - 700 && b.t0 < a.t0,
						)
					) {
						continue;
					}
					const p0 = evalPlayer(tl, tr.pid, a.t0);
					if (!p0.shown) {
						continue;
					}
					n += 1;
					let far = 0;
					for (let t = a.t0; t <= a.t1; t += 50) {
						const p = evalPlayer(tl, tr.pid, t);
						far = Math.max(far, Math.hypot(p.x - p0.x, p.y - p0.y));
					}
					if (far > 1.2) {
						slid.push(
							`${seed}: ${tr.pid} ${a.anim} at ${a.t0} ${far.toFixed(1)}ft`,
						);
					}
				}
			}
		}
		assert.isAbove(n, 150);
		assert.deepEqual(slid, []);
	}, 960_000);

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
				// A timeout, the end of a period (or the next one starting, when
				// the last ended on a shot at the horn): the whistle is the play
				// - the walk off to the huddles after it is hurried through.
				if (/^(timeout|endOfPeriod|period|overtime)$/.test(b.type)) {
					assert.isAtLeast(a - b.actionStart, 800 - 1e-6, `${b.type} ${j}`);
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
	}, 240_000);

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
	}, 240_000);

	// The man the words have at the shot - blocking it, fouling him - stays
	// on his own man till it comes, and gets there from him: not gone to the
	// spot steps ahead of it and stood there seconds, nobody near him, his
	// man alone in the corner. (Four games: four of them stood like that
	// before.)
	test("the man who meets the shot is on his man till it comes - not stood waiting at the spot", () => {
		let shots = 0;
		const parked: string[] = [];
		for (const seed of ["a", "b", "c", "d"]) {
			const { tl } = compile(seed, 150);
			const teamOf = (pid: number) => tl.tracks.get(pid)?.team;
			const flies = tl.ball.filter(
				(s) => s.kind === "fly" && "pid" in s.from,
			) as Extract<CourtTimeline["ball"][number], { kind: "fly" }>[];
			for (const tr of tl.tracks.values()) {
				for (const a of tr.acts) {
					if (a.anim !== "block" && a.anim !== "reach" && a.anim !== "hack") {
						continue;
					}
					const shot = flies.find(
						(s) =>
							"pid" in s.from &&
							teamOf(s.from.pid) !== tr.team &&
							s.t0 >= a.t0 - 250 &&
							s.t0 <= a.t0 + 600 &&
							!("pid" in s.to && teamOf(s.to.pid) === teamOf(s.from.pid)),
					);
					if (!shot) {
						continue;
					}
					shots += 1;
					// The longest he stood, the eight seconds before it.
					let most = 0;
					let from = 0;
					let free = a.t0 - 8000;
					for (const m of tr.moves) {
						if (m.t1 <= a.t0 - 8000 || m.t0 >= a.t0) {
							continue;
						}
						if (m.t0 - free > most) {
							most = m.t0 - free;
							from = free;
						}
						free = Math.max(free, m.t1);
					}
					if (a.t0 - free > most) {
						most = a.t0 - free;
						from = free;
					}
					const t = from + most / 2;
					if (most < 1500 || fastAt(tl, t) > 1) {
						continue;
					}
					const me = evalPlayer(tl, tr.pid, t);
					let near = Infinity;
					for (const o of tl.tracks.values()) {
						const q = evalPlayer(tl, o.pid, t);
						if (o.team !== tr.team && q.shown) {
							near = Math.min(near, Math.hypot(q.x - me.x, q.y - me.y));
						}
					}
					if (near > 8) {
						parked.push(`${seed} ${tr.pid} at ${Math.round(from)}`);
					}
				}
			}
		}
		assert.isAbove(shots, 40);
		assert.isAtMost(parked.length, 1, parked.join(", "));
	}, 480_000);

	// In the lane off the drive or the roll, short of the rim, a guard
	// mostly floats it up over the big, and a big mostly hooks it in over
	// him.
	test("a lane finish: a guard's mostly a floater, a big's mostly a hook", () => {
		const GUARD = new Set(["PG", "SG", "G"]);
		const BIG = new Set(["PF", "FC", "C"]);
		const n = { guard: { floater: 0, hook: 0 }, big: { floater: 0, hook: 0 } };
		for (const seed of ["a", "b", "c", "d"]) {
			const { tl, players } = compile(seed, 150);
			for (const tr of tl.tracks.values()) {
				const pos = players.find((p) => p.pid === tr.pid)?.pos ?? "";
				const k = GUARD.has(pos) ? "guard" : BIG.has(pos) ? "big" : undefined;
				for (const a of tr.acts) {
					if (
						!k ||
						(a.anim !== "floater" && a.anim !== "hook") ||
						// (Not a hook off a post-up.)
						tr.acts.some(
							(b) => b.anim === "postUp" && b.t1 > a.t0 - 4000 && b.t0 < a.t0,
						)
					) {
						continue;
					}
					n[k][a.anim] += 1;
				}
			}
		}
		const share = (x: { floater: number; hook: number }, of: number) =>
			of / Math.max(1, x.floater + x.hook);
		assert.isAbove(n.guard.floater + n.guard.hook, 20);
		assert.isAbove(n.big.floater + n.big.hook, 8);
		assert.isAbove(share(n.guard, n.guard.floater), 0.6);
		assert.isAbove(share(n.big, n.big.hook), 0.6);
	}, 480_000);

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
			// At the rim, where he is as it leaves his hands (driving in, he
			// gathers it a few feet further out than that).
			const shot = tl.ball.find(
				(s) =>
					e.type === "fgaAtRim" &&
					s.kind === "fly" &&
					"pid" in s.from &&
					s.from.pid === e.pid &&
					s.t0 >= b.actionStart - 1,
			);
			const st = evalPlayer(tl, e.pid, shot?.t0 ?? b.actionStart);
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
	}, 240_000);

	// Nor stood out at the free-throw line first, waiting on it: his run
	// there - down the floor on the break, off his spot on a cut - gets him
	// there as he sets off on the cut in.
	test("a man fed at the rim runs in to it in stride - not stood at the free-throw line waiting first", () => {
		const FINISH = new Set<string>([
			"dunk",
			"dunk1",
			"dunkPump",
			"dunkPump1",
			"tomahawk",
			"windmill",
			...LAYUPS,
		]);
		const rims = [0, 1].map((side) => ({
			x: rimX(side as 0 | 1),
			y: COURT_H / 2,
		}));
		let fed = 0;
		const waited: string[] = [];
		for (const seed of ["a", "b", "c", "d", "e", "f"]) {
			const { tl } = compile(seed, 150);
			tl.ball.forEach((s, i) => {
				if (
					s.kind !== "fly" ||
					!("pid" in s.to) ||
					!("pid" in s.from) ||
					s.from.pid === s.to.pid
				) {
					return;
				}
				const catcher = s.to.pid;
				const C = evalPlayer(tl, catcher, s.t1);
				const rim = rims.reduce((a, b) =>
					Math.hypot(C.x - a.x, C.y - a.y) < Math.hypot(C.x - b.x, C.y - b.y)
						? a
						: b,
				);
				if (Math.hypot(C.x - rim.x, C.y - rim.y) > 9) {
					return;
				}
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
				// The longest he stood still out there, 10 to 20 feet from the
				// rim, in the six seconds before he caught it - doing nothing
				// else (not set in a screen, say).
				const acts = tl.tracks.get(catcher)!.acts;
				let still = 0;
				let most = 0;
				let prev = evalPlayer(tl, catcher, s.t1 - 6000);
				for (let w = 5900; w >= 0; w -= 100) {
					const t = s.t1 - w;
					const Q = evalPlayer(tl, catcher, t);
					const d = Math.hypot(Q.x - rim.x, Q.y - rim.y);
					still =
						Math.hypot(Q.x - prev.x, Q.y - prev.y) < 0.15 &&
						d >= 10 &&
						d <= 20 &&
						!acts.some((a) => a.t0 <= t && a.t1 > t)
							? still + 100
							: 0;
					most = Math.max(most, still);
					prev = Q;
				}
				if (most >= 700) {
					waited.push(`${seed}: ${catcher} at ${Math.round(s.t1)} ${most}ms`);
				}
			});
		}
		assert.isAbove(fed, 25);
		assert.isAtMost(waited.length, fed * 0.4, waited.join("; "));
	}, 960_000);

	// The shot up, a man getting back backs out with his eyes on it - never
	// backs out of a corner up the floor facing the stands, the rim behind
	// his shoulder.
	test("getting back after a shot, nobody backpedals with the rim behind him", () => {
		let backs = 0;
		const blind: string[] = [];
		for (const seed of ["a", "b", "c"]) {
			const { tl } = compile(seed, 150);
			for (const s of tl.ball) {
				if (s.kind !== "fly" || !("pid" in s.from) || "pid" in s.to) {
					continue;
				}
				const shooter = tl.tracks.get(s.from.pid);
				if (!shooter) {
					continue;
				}
				const rim = { x: s.to.x < 47 ? 5.25 : 88.75, y: 25 };
				for (const tr of tl.tracks.values()) {
					if (tr.team !== shooter.team || tr.pid === shooter.pid) {
						continue;
					}
					for (const m of tr.moves) {
						if (m.anim !== "back" || m.t0 < s.t0 || m.t0 > s.t0 + 1500) {
							continue;
						}
						const gx = m.to.x - m.from.x;
						const gy = m.to.y - m.from.y;
						const g = Math.hypot(gx, gy);
						const rx = rim.x - m.from.x;
						const ry = rim.y - m.from.y;
						const r = Math.hypot(rx, ry);
						if (g < 2 || r < 1 || r > 35) {
							continue;
						}
						backs += 1;
						// Backpedalling, he faces back the way he came: the rim more
						// than square to that is behind him.
						if ((-gx * rx - gy * ry) / (g * r) < 0) {
							blind.push(`${seed} ${tr.pid} at ${Math.round(m.t0)}`);
						}
					}
				}
			}
		}
		assert.isAbove(backs, 20);
		assert.isAtMost(blind.length, backs * 0.05, blind.join(", "));
	}, 960_000);

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
	}, 240_000);

	// A quick trip off a defensive board or a steal: the ball keeps coming up
	// the floor - not brought up and stood there bouncing it, 2.5 s and more,
	// waiting on the others to get down with him, or on the whistle for a
	// foul away from the ball.
	test("off a board or a steal, the man with the ball keeps it coming", () => {
		let trips = 0;
		let stood = 0;
		for (const seed of ["a", "b", "c", "d"]) {
			const { tl } = compile(seed, 150);
			tl.beats.forEach((b, k) => {
				const prev = tl.beats[k - 1];
				if (
					!prev ||
					!/^(fga|pfNonShooting$)/.test(b.type) ||
					(prev.type !== "drb" && prev.type !== "stl") ||
					b.actionStart - prev.end > 12_000
				) {
					return;
				}
				trips += 1;
				let run = 0;
				let worst = 0;
				for (let t = prev.end; t < b.actionStart - 300; t += 100) {
					const h = evalBall(tl, t, bodyFor).holder;
					if (h === undefined) {
						run = 0;
						continue;
					}
					const p0 = evalPlayer(tl, h, t);
					const p1 = evalPlayer(tl, h, t + 100);
					const still = Math.hypot(p1.x - p0.x, p1.y - p0.y) < 0.35;
					run = still ? run + 100 / fastAt(tl, t) : 0;
					worst = Math.max(worst, run);
				}
				if (worst >= 2500) {
					stood += 1;
				}
			});
		}
		assert.isAbove(trips, 25);
		assert.isAtMost(stood, trips / 30, `${stood} of ${trips}`);
	}, 960_000);

	// Passing it off his dribble, a man takes it as it comes up into his
	// hands and it goes straight on out of them - not picked up at the top
	// of a bounce gone by and stood holding it till the pass goes. (Before:
	// four in ten stood with it over 150 ms first.)
	test("a man passing off his dribble does not stand holding it first", () => {
		const THROWS = new Set<string>([
			"pass",
			"passOverhead",
			"passBounce",
			"passPush",
			"passBouncePush",
			"passBehind",
			"passNoLook",
		]);
		let passes = 0;
		let stood = 0;
		for (const seed of ["a", "b"]) {
			const { tl } = compile(seed);
			tl.ball.forEach((seg, i) => {
				const held = tl.ball[i - 1];
				const was = tl.ball[i - 2];
				if (
					seg.kind !== "fly" ||
					!("pid" in seg.from) ||
					held?.kind !== "hold" ||
					held.style !== "hold" ||
					held.pid !== seg.from.pid ||
					was?.kind !== "hold" ||
					was.pid !== held.pid ||
					was.style === "hold"
				) {
					return;
				}
				const tr = tl.tracks.get(held.pid)!;
				const act = tr.acts.findLast(
					(a) => THROWS.has(a.anim) && a.t0 <= seg.t0 && a.t1 >= seg.t0,
				);
				// (Not one held in his stance a while first, on purpose.)
				if (
					!act ||
					tr.acts.some((a) => a !== act && a.t1 > held.t0 && a.t0 < act.t0)
				) {
					return;
				}
				passes += 1;
				if (act.t0 - held.t0 > 150) {
					stood += 1;
				}
			});
		}
		assert.isAbove(passes, 100);
		assert.isBelow(stood / passes, 0.2);
	}, 480_000);

	// Passed out to a man left open, his defender reads it as it goes and is
	// on his way out to him in the air - not stood watching it till it gets
	// there and only then off after him.
	test("a pass out to a man left open: his man is on his way while it is in the air", () => {
		let passes = 0;
		let stood = 0;
		for (const seed of ["a", "b", "c"]) {
			const { tl } = compile(seed);
			for (const seg of tl.ball) {
				if (
					seg.kind !== "fly" ||
					!("pid" in seg.from) ||
					!("pid" in seg.to) ||
					seg.from.pid === seg.to.pid ||
					seg.t1 - seg.t0 < 550 ||
					fastAt(tl, seg.t0) > 1
				) {
					continue;
				}
				const R0 = evalPlayer(tl, seg.to.pid, seg.t0);
				const R1 = evalPlayer(tl, seg.to.pid, seg.t1);
				if (evalPlayer(tl, seg.from.pid, seg.t0).team !== R0.team) {
					continue;
				}
				// (His man: the nearest of them to him as it gets there.)
				let man: number | undefined;
				let near = Infinity;
				for (const tr of tl.tracks.values()) {
					const q = evalPlayer(tl, tr.pid, seg.t1);
					const d = Math.hypot(q.x - R1.x, q.y - R1.y);
					if (tr.team !== R0.team && q.shown && d < near) {
						near = d;
						man = tr.pid;
					}
				}
				if (man === undefined) {
					continue;
				}
				const D0 = evalPlayer(tl, man, seg.t0);
				const D1 = evalPlayer(tl, man, seg.t1);
				if (Math.hypot(D0.x - R0.x, D0.y - R0.y) < 6) {
					continue;
				}
				passes += 1;
				const pace =
					Math.hypot(D1.x - D0.x, D1.y - D0.y) / ((seg.t1 - seg.t0) / 1000);
				if (pace < 4) {
					stood += 1;
				}
			}
		}
		assert.isAbove(passes, 30);
		assert.isBelow(stood / passes, OPEN_STOOD_MOST);
	}, 480_000);

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
	}, 240_000);

	test("nobody on the floor goes faster than a man can run - not even back from meeting a dunk", () => {
		// (Seed 55: the man dunked on, there under the rim for the block,
		// was off to his man 18 ft away in a fifth of a second.)
		for (const [seed, n] of [
			["55", 150],
			["a", 120],
			["b", 120],
		] as const) {
			const { tl } = compile(seed, n);
			// (Through a cut or a stretch run through fast, anything goes.)
			const skipped = (a: number, b: number) =>
				tl.fast.some(([x, y]) => a < y && b > x) ||
				tl.cuts.some((c) => c >= a && c <= b);
			for (const [pid, tr] of tl.tracks) {
				for (const m of tr.moves) {
					const L = Math.hypot(m.to.x - m.from.x, m.to.y - m.from.y);
					if (L < 1 || skipped(m.t0, m.t1)) {
						continue;
					}
					assert.isAtMost(
						L / ((m.t1 - m.t0) / 1000),
						34,
						`${seed}: ${pid} at ${Math.round(m.t0)}`,
					);
				}
			}
		}
	}, 480_000);
});
