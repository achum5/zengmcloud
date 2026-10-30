import { assert, describe, test } from "vitest";
import { compileCourt } from "./director.ts";
import { evalBall, evalPlayer } from "./evaluate.ts";
import { bodyOf, JUMPER, PUSH_JUMPER } from "./poses.ts";
import type { CourtRatings } from "./ratings.ts";
import { fakeGame, gidOf } from "./testGame.ts";

const hash01 = (a: number, b: number) => {
	const x = Math.sin(a * 12.9898 + b * 78.233) * 43758.5453;
	return x - Math.floor(x);
};

// Players play like their ratings: the same game, everybody rated the same
// way, and what they do at the rim shows it.
const play = (seed: string, ratings: CourtRatings) => {
	const { events, players } = fakeGame(seed, 150);
	return compileCourt({
		events,
		players: players.map((p) => ({ ...p, ratings })),
		gid: gidOf(seed),
	});
};
const count = (seeds: string[], ratings: CourtRatings) => {
	const n: Record<string, number> = {};
	for (const seed of seeds) {
		for (const tr of play(seed, ratings).tracks.values()) {
			for (const a of tr.acts) {
				n[a.anim] = (n[a.anim] ?? 0) + 1;
			}
		}
	}
	return n;
};
const all = (v: number): CourtRatings => ({
	hgt: 50,
	stre: v,
	spd: v,
	jmp: v,
	ins: v,
	dnk: v,
	drb: v,
	tp: v,
	fg: v,
	diq: v,
	oiq: v,
	pss: v,
	reb: v,
});

describe("3D: players play like their ratings", () => {
	test("a high flier puts on a show at the rim; a plodder just puts it in", () => {
		const seeds = ["r1", "r2", "r3"];
		const hi = count(seeds, all(85));
		const lo = count(seeds, all(30));
		const show = (n: Record<string, number>) => {
			const dunks =
				(n.dunk ?? 0) + (n.dunk1 ?? 0) + (n.tomahawk ?? 0) + (n.windmill ?? 0);
			return { dunks, share: ((n.tomahawk ?? 0) + (n.windmill ?? 0)) / dunks };
		};
		const h = show(hi);
		const l = show(lo);
		assert.isAbove(h.dunks, 10);
		assert.isAbove(l.dunks, 10);
		assert.isAbove(h.share, l.share + 0.2, JSON.stringify({ h, l }));
		assert.strictEqual(lo.windmill ?? 0, 0);
	});

	test("a crafty finisher rolls it in; a strong one goes up strong", () => {
		const seeds = ["r1", "r2", "r3"];
		const crafty = count(seeds, { ...all(50), ins: 85, drb: 85, stre: 30 });
		const strong = count(seeds, { ...all(50), ins: 30, drb: 30, stre: 85 });
		const roll = (n: Record<string, number>) =>
			(n.fingerRoll ?? 0) /
			Math.max(1, (n.fingerRoll ?? 0) + (n.powerLayup ?? 0));
		assert.isAbove(roll(crafty), roll(strong) + 0.3);
	});

	test("a leaper gets up higher for it; a man who can't jump barely leaves the floor", () => {
		const peaks = (ratings: CourtRatings) => {
			const out: number[] = [];
			for (const seed of ["r1", "r2"]) {
				for (const tr of play(seed, ratings).tracks.values()) {
					for (const a of tr.acts) {
						if (
							(a.anim === "board" ||
								a.anim === "rebound" ||
								a.anim === "block" ||
								a.anim === "contest") &&
							a.jump
						) {
							out.push(a.jump[2]);
						}
					}
				}
			}
			return out.reduce((x, y) => x + y, 0) / Math.max(1, out.length);
		};
		// (Some go up only as high as the ball is - by more than enough.)
		assert.isAbove(peaks(all(85)), peaks(all(30)) * 1.2);
	});

	test("a quick man runs the floor faster than a slow one", () => {
		const pace = (ratings: CourtRatings) => {
			let feet = 0;
			let ms = 0;
			for (const seed of ["r1", "r2"]) {
				for (const tr of play(seed, ratings).tracks.values()) {
					for (const m of tr.moves) {
						const d = Math.hypot(m.to.x - m.from.x, m.to.y - m.from.y);
						if (m.anim === "run" && d > 25) {
							feet += d;
							ms += m.t1 - m.t0;
						}
					}
				}
			}
			return feet / (ms / 1000);
		};
		assert.isAbove(
			pace({ ...all(50), spd: 85 }),
			pace({ ...all(50), spd: 30 }) * 1.08,
		);
	});

	// However each man is rated - every one his own way, as in a real game -
	// the rules every game keeps still hold: nobody faster than a man can run,
	// and the ball never jumps from one place to another.
	test("rated every which way, the game still holds together", () => {
		const body = bodyOf();
		for (const seed of ["r1", "r2"]) {
			const { events, players } = fakeGame(seed, 150);
			const keys = [
				"stre",
				"spd",
				"jmp",
				"ins",
				"dnk",
				"drb",
				"tp",
				"fg",
				"diq",
				"oiq",
				"pss",
				"reb",
			] as const;
			const tl = compileCourt({
				events,
				players: players.map((p) => ({
					...p,
					ratings: Object.fromEntries(
						keys.map((k, i) => [
							k,
							Math.round(25 + 65 * hash01(p.pid, 811 + i)),
						]),
					) as CourtRatings,
				})),
				gid: gidOf(seed),
			});
			const skipped = (a: number, b: number) =>
				tl.fast.some(([x, y]) => a < y && b > x) ||
				tl.cuts.some((c) => c >= a && c <= b);
			for (const tr of tl.tracks.values()) {
				for (const m of tr.moves) {
					const L = Math.hypot(m.to.x - m.from.x, m.to.y - m.from.y);
					if (L >= 1 && !skipped(m.t0, m.t1)) {
						assert.isAtMost(
							L / ((m.t1 - m.t0) / 1000),
							34,
							`${seed} ${tr.pid} at ${Math.round(m.t0)}`,
						);
					}
				}
			}
			let prev: { x: number; y: number; z: number } | undefined;
			let last = -Infinity;
			for (let t = 0; t <= tl.end; t += 20) {
				const b = evalBall(tl, t, () => body);
				const cut = tl.cuts.some((c) => c > last && c <= t);
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
	});

	test("a pure shooter gets it off quicker than a poor one", () => {
		const strokes = (ratings: CourtRatings, anim: string) => {
			const out: number[] = [];
			for (const seed of ["r1", "r2"]) {
				for (const tr of play(seed, ratings).tracks.values()) {
					for (const a of tr.acts) {
						if (a.anim === anim) {
							out.push(a.t1 - a.t0);
						}
					}
				}
			}
			return out.reduce((x, y) => x + y, 0) / Math.max(1, out.length);
		};
		const pure = { ...all(50), tp: 85, fg: 85, ft: 90 };
		const poor = { ...all(50), tp: 30, fg: 30, ft: 40 };
		assert.isBelow(strokes(pure, "shoot"), strokes(poor, "shoot") * 0.85);
		assert.isBelow(strokes(pure, "setShot"), strokes(poor, "setShot") * 0.85);
	});

	test("a great handler works his man with moves; a poor one keeps it simple", () => {
		const moves = (ratings: CourtRatings) => {
			let cross = 0;
			let fancy = 0;
			for (const seed of ["r1", "r2"]) {
				for (const seg of play(seed, ratings).ball) {
					if (seg.kind === "hold" && seg.style === "cross") {
						cross += 1;
						if (seg.move === "legs" || seg.move === "back") {
							fancy += 1;
						}
					}
				}
			}
			return { cross, fancy: fancy / Math.max(1, cross) };
		};
		const great = moves({ ...all(50), drb: 90 });
		const poor = moves({ ...all(50), drb: 30 });
		assert.isAbove(
			great.cross,
			poor.cross * 1.3,
			JSON.stringify({ great, poor }),
		);
		// (The same move again and again is one stretch of it: plain crosses
		// run together, so their share is counted high.)
		assert.isAbove(
			great.fancy,
			poor.fancy + 0.1,
			JSON.stringify({ great, poor }),
		);
	});

	test("sharp defenders are closer to the shot than poor ones", () => {
		const body = bodyOf();
		// (Over six games: a few tenths of a foot - 0.33-0.39 ft, over the
		// six - which a game or two alone may not show; one alone ranges from
		// none to two-thirds of a foot.)
		const space = (diq: number) => {
			let sum = 0;
			let n = 0;
			for (const seed of ["r1", "r2", "r3", "r4", "r5", "r6"]) {
				const { events, players } = fakeGame(seed, 150);
				const tl = compileCourt({
					events,
					// (Each side's defense rated so when it defends; the offense
					// the same either way.)
					players: players.map((p) => ({ ...p, ratings: { ...all(50), diq } })),
					gid: gidOf(seed),
				});
				for (const s of tl.ball) {
					if (
						s.kind !== "fly" ||
						!("pid" in s.from) ||
						"pid" in s.to ||
						s.to.z < 6
					) {
						continue;
					}
					const shooter = s.from.pid;
					const team = tl.tracks.get(shooter)!.team;
					const S = evalPlayer(tl, shooter, s.t0);
					let near = Infinity;
					for (const tr of tl.tracks.values()) {
						if (tr.team === team) {
							continue;
						}
						const q = evalPlayer(tl, tr.pid, s.t0);
						if (q.shown) {
							near = Math.min(near, Math.hypot(q.x - S.x, q.y - S.y));
						}
					}
					if (near < 20) {
						sum += near;
						n += 1;
					}
				}
			}
			void body;
			return sum / Math.max(1, n);
		};
		assert.isBelow(space(90), space(30) - 0.15);
	});

	test("a good passer puts zip on it; a poor one floats it", () => {
		const flight = (pss: number) => {
			let ms = 0;
			let n = 0;
			for (const seed of ["r1", "r2"]) {
				for (const seg of play(seed, { ...all(50), pss }).ball) {
					if (seg.kind === "fly" && "pid" in seg.from && "pid" in seg.to) {
						ms += seg.t1 - seg.t0;
						n += 1;
					}
				}
			}
			return ms / Math.max(1, n);
		};
		assert.isBelow(flight(90), flight(30) * 0.9);
	});

	test("shooters get their own shots off their own ways: step-backs and side-steps", () => {
		let back = 0;
		let side = 0;
		const byMan = new Map<number, { back: number; side: number }>();
		for (const seed of ["r1", "r2", "r3", "r4"]) {
			const tl = play(seed, all(70));
			for (const tr of tl.tracks.values()) {
				const rimX = tr.team === 0 ? 5.25 : 88.75;
				for (const a of tr.acts) {
					if (a.anim !== "stepBack") {
						continue;
					}
					// The hop: the run he is on through it.
					const m = tr.moves.find(
						(x) => x.t0 >= a.t0 - 60 && x.t0 <= a.t0 + 60,
					);
					if (!m) {
						continue;
					}
					const hx = m.to.x - m.from.x;
					const hy = m.to.y - m.from.y;
					const L = Math.hypot(hx, hy);
					const rx = rimX - m.to.x;
					const ry = 25 - m.to.y;
					const R = Math.hypot(rx, ry);
					if (L < 1 || R < 1) {
						continue;
					}
					const along = Math.abs((hx * rx + hy * ry) / (L * R));
					const k = byMan.get(tr.pid) ?? { back: 0, side: 0 };
					if (along < 0.5) {
						side += 1;
						k.side += 1;
					} else {
						back += 1;
						k.back += 1;
					}
					byMan.set(tr.pid, k);
				}
			}
		}
		// (A side-step is the rarer of the two.)
		assert.isAbove(side, 2, `side ${side} back ${back}`);
		assert.isAbove(back, 5, `side ${side} back ${back}`);
	});
	test("a hard closeout gets shot-faked, and gone by on one dribble - not run through", () => {
		let n = 0;
		let closest = Infinity;
		for (const seed of ["r1", "r2", "r3"]) {
			const tl = play(seed, all(75));
			for (const tr of tl.tracks.values()) {
				for (const f of tr.acts) {
					if (f.anim !== "shotFake") {
						continue;
					}
					const shot = tr.acts.find(
						(a) => a.anim === "shoot" && a.t0 > f.t1 && a.t0 < f.t1 + 1200,
					);
					const by = tr.moves.find(
						(m) => m.anim === "dribble" && m.t0 >= f.t0 && m.t0 < f.t1 + 200,
					);
					// His man up at it, on his way out to him.
					const up = [...tl.tracks.values()].find(
						(o) =>
							o.team !== tr.team &&
							o.acts.some(
								(a) =>
									a.anim === "contest" &&
									a.jump !== undefined &&
									a.t0 > f.t0 &&
									a.t0 < f.t0 + 500,
							),
					);
					if (!shot || !by || !up) {
						continue;
					}
					n += 1;
					for (let t = f.t0; t < shot.t0; t += 25) {
						const a = evalPlayer(tl, tr.pid, t);
						const b = evalPlayer(tl, up.pid, t);
						closest = Math.min(closest, Math.hypot(a.x - b.x, a.y - b.y));
					}
				}
			}
		}
		assert.isAbove(n, 2, `${n}`);
		assert.isAbove(closest, 1.9, `${closest}`);
	});
	test("a poor shooter pushes his jumper, a good one doesn't - and it leaves his hand", () => {
		const body = bodyOf();
		let pushed = 0;
		for (const [v, poor] of [
			[40, true],
			[75, false],
		] as const) {
			const tl = play("r1", all(v));
			for (const tr of tl.tracks.values()) {
				for (const a of tr.acts) {
					if (a.anim !== "shoot") {
						continue;
					}
					assert.strictEqual(a.push === true, poor, `${v} ${Math.round(a.t0)}`);
					if (!poor) {
						continue;
					}
					pushed += 1;
					const go = a.t0 + (a.t1 - a.t0) * PUSH_JUMPER.release;
					const b0 = evalBall(tl, go - 8, () => body);
					const b1 = evalBall(tl, go + 8, () => body);
					assert.isBelow(
						Math.hypot(b1.x - b0.x, b1.y - b0.y, b1.z - b0.z),
						0.8,
						`${Math.round(go)}`,
					);
				}
			}
		}
		assert.isAbove(pushed, 5);
	});
	test("good rebounders crash the glass and put a body on a man; poor ones watch it", () => {
		const glass = (v: number) => {
			const n = { boxOut: 0, fight: 0 };
			for (const tr of play("r1", all(v)).tracks.values()) {
				for (const a of tr.acts) {
					if (a.anim === "boxOut" || a.anim === "fight") {
						n[a.anim] += 1;
					}
				}
			}
			return n;
		};
		const poor = glass(30);
		const good = glass(85);
		assert.isAbove(
			good.boxOut,
			poor.boxOut * 1.6,
			JSON.stringify({ poor, good }),
		);
		assert.isAbove(
			good.fight,
			poor.fight * 1.4,
			JSON.stringify({ poor, good }),
		);
	});
	test("worn down deep into a long stretch, a man short on endurance is bent over getting his breath at the line", () => {
		// (The game's minutes drawn out, so its free throws come late in
		// each period.)
		const crouched = (endu: number) => {
			const { events, players } = fakeGame("r1", 150);
			const tl = compileCourt({
				events: events.map((e) =>
					typeof e.clock === "number"
						? { ...e, clock: Math.max(0, 720 - (720 - e.clock) * 3) }
						: e,
				),
				players: players.map((p) => ({ ...p, ratings: { ...all(60), endu } })),
				gid: gidOf("r1"),
			});
			let n = 0;
			for (const tr of tl.tracks.values()) {
				n += tr.acts.filter((a) => a.anim === "crouch").length;
			}
			return n;
		};
		const short = crouched(30);
		const long = crouched(85);
		assert.isAbove(short, long + 10, `${short} ${long}`);
	});
	test("a smart man off the ball keeps moving to where he's open; a poor one stands and watches more", () => {
		// How much of the time the ball is in a man's hands his teammates
		// are on the move.
		const moving = (v: number) => {
			let on = 0;
			let n = 0;
			for (const seed of ["r1", "r2", "r3"]) {
				const tl = play(seed, all(v));
				tl.ball.forEach((s, k) => {
					if (s.kind !== "hold") {
						return;
					}
					const team = tl.tracks.get(s.pid)?.team;
					const end = tl.ball[k + 1]?.t0 ?? s.t0;
					for (let t = s.t0; t < end; t += 100) {
						if (tl.fast.some(([a, b]) => t >= a && t < b)) {
							continue;
						}
						for (const tr of tl.tracks.values()) {
							if (tr.pid === s.pid || tr.team !== team) {
								continue;
							}
							n += 1;
							if (tr.moves.some((m) => m.t0 <= t && m.t1 > t)) {
								on += 1;
							}
						}
					}
				});
			}
			return on / n;
		};
		const poor = moving(30);
		const smart = moving(85);
		assert.isAbove(smart, poor + 0.015, `${poor} ${smart}`);
	});
	test("a man with a game inside now and then backs his man down from the elbow and turns over him", () => {
		let n = 0;
		for (const seed of ["r1", "r2", "r3"]) {
			const tl = play(seed, all(75));
			for (const tr of tl.tracks.values()) {
				const rimX = tr.team === 0 ? 88.75 : 5.25;
				for (const a of tr.acts) {
					if (a.anim !== "fade") {
						continue;
					}
					const backed = tr.moves.find(
						(m) => m.anim === "post" && m.t1 <= a.t0 + 50 && m.t1 > a.t0 - 500,
					);
					if (backed && Math.hypot(backed.to.x - rimX, backed.to.y - 25) > 9) {
						n += 1;
					}
				}
			}
		}
		assert.isAbove(n, 2, `${n}`);
	});
	test("a handler now and then gets his pull-up off a hesitation", () => {
		let n = 0;
		for (const seed of ["r1", "r2", "r3"]) {
			for (const tr of play(seed, all(75)).tracks.values()) {
				for (const a of tr.acts) {
					if (
						a.anim === "dribbleHesi" &&
						tr.acts.some(
							(x) =>
								(x.anim === "shoot" || x.anim === "fade") &&
								x.t0 > a.t1 &&
								x.t0 < a.t1 + 1500,
						)
					) {
						n += 1;
					}
				}
			}
		}
		assert.isAbove(n, 2, `${n}`);
	});
	test("shooters come down their own way: some where they went up, some ahead of it, some back", () => {
		const tl = play("r1", all(75));
		let moved = 0;
		let n = 0;
		for (const tr of tl.tracks.values()) {
			for (const a of tr.acts) {
				if (a.anim !== "shoot") {
					continue;
				}
				const up = evalPlayer(tl, tr.pid, a.t0 + (a.t1 - a.t0) * 0.3);
				const down = evalPlayer(tl, tr.pid, a.t1 - 20);
				n += 1;
				if (Math.hypot(down.x - up.x, down.y - up.y) > 0.35) {
					moved += 1;
				}
			}
		}
		assert.isAbove(moved / n, 0.2, `${moved}/${n}`);
		assert.isBelow(moved / n, 0.85, `${moved}/${n}`);
	});
	test("each shooter his own release - high, quick or the usual - the same every time", () => {
		const tl = play("r1", all(75));
		const kinds = new Set<string>();
		for (const tr of tl.tracks.values()) {
			// (Leaning away from a hand up at it, he goes up his own way of
			// that instead: see LEAN_ODDS.)
			const his = new Set(
				tr.acts
					.filter((a) => a.anim === "shoot" && a.lean === undefined)
					.map((a) => a.arms ?? "-"),
			);
			assert.isAtMost(his.size, 1, `${tr.pid}`);
			for (const k of his) {
				kinds.add(k);
			}
		}
		assert.strictEqual(kinds.size, 3, [...kinds].join(","));
	});
	test("a good defender at the rim goes up with the man going up; a poor one is sometimes caught flat-footed", () => {
		// (The man right there going straight up with him: see wallUp.)
		const ups = (v: number) => {
			const peaks: number[] = [];
			for (const seed of ["r1", "r2"]) {
				for (const tr of play(seed, all(v)).tracks.values()) {
					for (const a of tr.acts) {
						if (
							a.anim === "contest" &&
							a.jump?.[0] === 0.15 &&
							a.jump[1] === 0.85 &&
							Math.abs(a.t1 - a.t0 - 680) < 1
						) {
							peaks.push(a.jump[2]);
						}
					}
				}
			}
			return {
				mean: peaks.reduce((x, y) => x + y, 0) / peaks.length,
				flat: peaks.filter((p) => p < 0.3).length,
				n: peaks.length,
			};
		};
		const poor = ups(30);
		const good = ups(85);
		assert.isAbove(good.mean, poor.mean * 1.4, JSON.stringify({ poor, good }));
		assert.isAbove(poor.flat, good.flat, JSON.stringify({ poor, good }));
	});
	test("a fine handler keeps his dribble low; a poor one bounces it up round his waist", () => {
		const body = bodyOf();
		const high = (v: number) => {
			const { events, players } = fakeGame("r1", 40);
			const tl = compileCourt({
				events,
				players: players.map((p) => ({ ...p, ratings: all(v) })),
				gid: gidOf("r1"),
			});
			const zs: number[] = [];
			tl.ball.forEach((s, k) => {
				if (s.kind !== "hold" || s.style !== "dribble") {
					return;
				}
				const end = tl.ball[k + 1]?.t0 ?? s.t0;
				for (let t = s.t0 + 300; t < end - 100; t += 40) {
					zs.push(evalBall(tl, t, () => body).z);
				}
			});
			zs.sort((a, b) => a - b);
			return zs[Math.floor(zs.length * 0.95)]!;
		};
		const poor = high(30);
		const fine = high(85);
		assert.isBelow(fine, poor - 0.2, `${poor} ${fine}`);
	});
	test("a poor passer stares down where it is going; a fine one sees it late", () => {
		// How far round to the man he is passing to his head already is,
		// a good while before it goes (degrees, on average).
		const staring = (v: number) => {
			let sum = 0;
			let n = 0;
			for (const seed of ["r1", "r2"]) {
				const tl = play(seed, all(v));
				tl.ball.forEach((s, k) => {
					const before = tl.ball[k - 1];
					if (
						s.kind !== "fly" ||
						!("pid" in s.from) ||
						!("pid" in s.to) ||
						before?.kind !== "hold" ||
						before.pid !== s.from.pid ||
						s.t0 - before.t0 < 900 ||
						tl.fast.some(([a, b]) => s.t0 - 1000 < b && s.t0 > a)
					) {
						return;
					}
					n += 1;
					sum += Math.abs(evalPlayer(tl, s.from.pid, s.t0 - 600).look ?? 0);
				});
			}
			return sum / n;
		};
		const poor = staring(30);
		const fine = staring(85);
		assert.isAbove(poor, fine + 10, `${poor} ${fine}`);
	});
	test("now and then a shooter leans away from a hand right up at it - and it still leaves his hand", () => {
		const body = bodyOf();
		let n = 0;
		for (const seed of ["r1", "r2", "r3"]) {
			const tl = play(seed, all(75));
			for (const tr of tl.tracks.values()) {
				for (const a of tr.acts) {
					if (a.anim !== "shoot" || a.lean === undefined) {
						continue;
					}
					n += 1;
					const go = a.t0 + (a.t1 - a.t0) * JUMPER.release;
					const b0 = evalBall(tl, go - 8, () => body);
					const b1 = evalBall(tl, go + 8, () => body);
					assert.isBelow(
						Math.hypot(b1.x - b0.x, b1.y - b0.y, b1.z - b0.z),
						0.8,
						`${Math.round(go)}`,
					);
				}
			}
		}
		assert.isAbove(n, 2, `${n}`);
	});
	// Elite players do what nobody else does: a great shooter now and then
	// pulls up from way out, a few steps back of the line; nobody else ever
	// does but at the buzzer.
	test("a great shooter now and then pulls up from way out; most never do", () => {
		const deep = (ratings: CourtRatings) => {
			let n = 0;
			for (const seed of ["r1", "r2", "r3", "r4", "r5"]) {
				const tl = play(seed, ratings);
				for (const s of tl.ball) {
					if (s.kind !== "fly" || !("pid" in s.from) || "pid" in s.to) {
						continue;
					}
					const pid = s.from.pid;
					if (
						!tl.tracks
							.get(pid)
							?.acts.some(
								(a) => a.anim === "shoot" && a.t0 <= s.t0 && a.t1 > s.t0,
							)
					) {
						continue;
					}
					const P = evalPlayer(tl, pid, s.t0);
					const d = Math.min(
						Math.hypot(P.x - 5.25, P.y - 25),
						Math.hypot(P.x - 88.75, P.y - 25),
					);
					if (d >= 28.5 && d <= 34) {
						n += 1;
					}
				}
			}
			return n;
		};
		const great = deep({ ...all(60), tp: 85 });
		const most = deep({ ...all(60), tp: 50 });
		assert.isAbove(great, 3, `${great} ${most}`);
		assert.isBelow(most * 3, great, `${great} ${most}`);
	});
	// And now and then a great shooter doesn't wait to see his three go in:
	// he is on his way back up the floor before it is through.
	test("a great shooter now and then heads back before his three is through", () => {
		const away = (ratings: CourtRatings) => {
			let n = 0;
			for (const seed of ["r1", "r2", "r3"]) {
				const tl = play(seed, ratings);
				for (const f of tl.fx) {
					if (f.kind !== "roar" || f.what !== "three") {
						continue;
					}
					const shot = tl.ball.findLast(
						(s) =>
							s.kind === "fly" &&
							"pid" in s.from &&
							!("pid" in s.to) &&
							s.t0 < f.t,
					);
					if (
						shot?.kind !== "fly" ||
						!("pid" in shot.from) ||
						"pid" in shot.to
					) {
						continue;
					}
					const pid = shot.from.pid;
					const rx = shot.to.x < 47 ? 5.25 : 88.75;
					const A = evalPlayer(tl, pid, shot.t0);
					const B = evalPlayer(tl, pid, f.t - 150);
					if (Math.abs(B.x - rx) - Math.abs(A.x - rx) > 3) {
						n += 1;
					}
				}
			}
			return n;
		};
		const great = away({ ...all(60), tp: 85 });
		const most = away({ ...all(60), tp: 60 });
		assert.isAbove(great, 2, `${great} ${most}`);
		assert.isBelow(most * 3, great, `${great} ${most}`);
	});
	// And a great handler crosses his man up so badly his feet go out from
	// under him - a slow man on him most of all; hardly anybody else does.
	test("a great handler crosses up a slow man; hardly anybody else does", () => {
		// (Stumbling on defense - not the man driving, knocked off balance
		// by a man a step late to help.)
		const stumbles = (ratings: CourtRatings) => {
			let n = 0;
			for (const seed of ["r1", "r2", "r3"]) {
				const tl = play(seed, ratings);
				for (const tr of tl.tracks.values()) {
					for (const a of tr.acts) {
						const b = evalBall(tl, a.t0 + 60, () => bodyOf());
						if (
							a.anim === "stumble" &&
							b.holder !== undefined &&
							tl.tracks.get(b.holder)?.team !== tr.team
						) {
							n += 1;
						}
					}
				}
			}
			return n;
		};
		const great = stumbles({ ...all(55), drb: 85, spd: 35, diq: 35 });
		const most = stumbles(all(55));
		assert.isAbove(great, 3, `${great} ${most}`);
		assert.isBelow(most * 3, great, `${great} ${most}`);
	});
});
