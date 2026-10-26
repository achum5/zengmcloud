import { assert, describe, test } from "vitest";
import { atTable, crewAt, crewFor, type CrewMember } from "./crew.ts";
import { bodyPoint, cameraCuts, fastAt, poseOf, withBody } from "./evaluate.ts";
import { benchX, COURT_H, COURT_W, HUDDLE_Y, type Pt3 } from "./geometry.ts";
import { compileCourt, type CourtPlayer, type RawEvent } from "./director.ts";
import { bodyOf, skeleton } from "./poses.ts";
import { compile, gidOf } from "./testGame.ts";

const AWAY = {
	region: "Golden State",
	name: "Warriors",
	colors: ["#1d428a", "#ffc72c", "#ffffff"] as [string, string, string],
};
const HOME = {
	region: "Chicago",
	name: "Bulls",
	colors: ["#ce1141", "#000000", "#ffffff"] as [string, string, string],
};

const SIGNALS = new Set(["signalUp", "signalSide", "threeUp", "travel"]);
const isRef = (pid: number) => pid >= -3 && pid <= -1;

// The end of regulation, the sim's way, and overtime started with a tip -
// the man who has the ball in the huddle subbed out.
const overtime = () => {
	const players: CourtPlayer[] = [];
	const POS = ["PG", "SG", "SF", "PF", "C", "F"];
	const events: RawEvent[] = [{ type: "init", boxScore: {} }];
	for (const raw of [0, 1] as const) {
		for (let j = 0; j < 6; j++) {
			const pid = raw * 10 + j + 1;
			players.push({ pid, team: raw === 0 ? 1 : 0, pos: POS[j] });
			if (j < 5) {
				events.push({ type: "stat", t: raw, pid, s: "gs", amt: 1 });
			}
		}
	}
	events.push(
		{ type: "jumpBall", t: 0, pid: 5, pid2: 15, clock: 12 },
		{ type: "fgaMidRange", t: 1, pid: 12, clock: 6, desperation: false },
		{ type: "missMidRange", t: 1, pid: 12, clock: 5.6 },
		{ type: "drb", t: 0, pid: 4, clock: 5 },
		{ type: "fgaTp", t: 0, pid: 2, clock: 1.4, desperation: false },
		{ type: "missTp", t: 0, pid: 2, clock: 0.6 },
		{ type: "drb", t: 1, pid: 14, clock: 0.1 },
		{ type: "endOfPeriod", t: 1, reason: "noShot", clock: 0 },
		{ type: "overtime", period: 5, clock: 300 },
		{ type: "sub", t: 1, pids: [16], pidsOff: [14], clock: 300 },
		{ type: "jumpBall", t: 1, pid: 15, pid2: 5, clock: 300 },
		{ type: "fgaMidRange", t: 1, pid: 13, clock: 290, desperation: false },
		{ type: "fgMidRange", t: 1, pid: 13, clock: 289.6 },
		{ type: "stat", t: 1, pid: 13, s: "pts", amt: 2 },
		{ type: "endOfPeriod", t: 0, reason: "noShot", clock: 0 },
		{ type: "gameOver" },
	);
	return { events, tl: compileCourt({ events, players, gid: 5 }) };
};

// A game and everyone working it.
const working = (seed: string) => {
	const { events, tl } = compile(seed);
	return { events, tl, crew: crewFor(gidOf(seed), AWAY, HOME) };
};

describe("3D crew", () => {
	test("the same people work a game every time it is shown", () => {
		const a = crewFor(77, AWAY, HOME);
		assert.deepEqual(crewFor(77, AWAY, HOME), a);
		const count = (crew: CrewMember[], role: CrewMember["role"]) =>
			crew.filter((m) => m.role === role).length;
		assert.strictEqual(count(a, "ref"), 3);
		assert.strictEqual(count(a, "coach"), 2);
		// No caps, Santa hats or headbands on officials and coaches.
		for (const seed of [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) {
			for (const m of crewFor(seed, AWAY, HOME)) {
				assert.strictEqual(m.face.accessories.id, "none", m.role);
			}
		}
		assert.isAbove(count(a, "photo"), 3);
		// A team's coach is its own, whoever and wherever it plays.
		const b = crewFor(912, HOME, AWAY);
		const coach = (crew: CrewMember[], team: 0 | 1) =>
			crew.find((m) => m.role === "coach" && m.team === team)!;
		assert.deepEqual(coach(b, 1).face, coach(a, 0).face);
		assert.deepEqual(coach(b, 0).face, coach(a, 1).face);
		const { tl, crew } = working("crew");
		for (const t of [500, 40_000, 120_000]) {
			assert.deepEqual(crewAt(tl, t, crew), crewAt(tl, t, crew));
		}
	}, 60_000);

	// Officials run with the play, so in the picture they never pop from
	// one place to another - except where the picture cuts.
	test("the officials keep to the floor and run - never jump, but at a cut", () => {
		for (const seed of ["a", "b"]) {
			const { tl, crew } = working(seed);
			const cuts = cameraCuts(tl);
			let prev: Map<number, { x: number; y: number }> | undefined;
			let prevT = 0;
			for (let t = 0; t <= tl.end; t += 50) {
				const refs = crewAt(tl, t, crew).states.filter((s) => isRef(s.pid));
				assert.strictEqual(refs.length, 3);
				for (const r of refs) {
					assert.isTrue(
						r.x >= -3 && r.x <= COURT_W + 3 && r.y >= -1 && r.y <= COURT_H + 2,
						`${seed} ${t} ${r.pid} at ${r.x}, ${r.y}`,
					);
				}
				if (prev && !cuts.some((c) => c > prevT && c <= t)) {
					for (const r of refs) {
						const p = prev.get(r.pid)!;
						// Thirty feet a second is a sprint.
						assert.isBelow(
							Math.hypot(r.x - p.x, r.y - p.y),
							1.5,
							`${seed} ${t} ${r.pid}`,
						);
					}
				}
				prev = new Map(refs.map((r) => [r.pid, { x: r.x, y: r.y }]));
				prevT = t;
			}
		}
	}, 60_000);

	// A jump ahead (the next key play, joining late) is worked out from ten
	// seconds short of where it lands, not every step in between - and lands
	// the officials where playing it all through would have. Gone back and
	// played on into it, they never pop.
	test("a jump ahead finds the officials where playing through would have", () => {
		const { tl, crew } = working("crew");
		const { tl: through } = working("crew");
		const refsAt = (on: typeof tl, t: number) =>
			crewAt(on, t, crew).states.filter((s) => isRef(s.pid));
		const marks = [0.3, 0.55, 0.8].map((f) => Math.round(tl.end * f));
		for (let t = 0; t <= marks.at(-1)!; t += 250) {
			refsAt(through, t);
		}
		for (const t of marks) {
			const want = refsAt(through, t);
			refsAt(tl, t).forEach((r, i) => {
				const p = want[i]!;
				assert.isBelow(Math.hypot(r.x - p.x, r.y - p.y), 0.5, `${t} ${r.pid}`);
			});
		}
		const cuts = cameraCuts(tl);
		let prev = refsAt(tl, marks[1]! - 30_000);
		for (let t = marks[1]! - 29_950; t <= marks[1]! + 5000; t += 50) {
			const refs = refsAt(tl, t);
			if (!cuts.some((c) => c > t - 50 && c <= t)) {
				refs.forEach((r, i) => {
					const p = prev[i]!;
					assert.isBelow(
						Math.hypot(r.x - p.x, r.y - p.y),
						1.5,
						`${t} ${r.pid}`,
					);
				});
			}
			prev = refs;
		}
	}, 60_000);

	test("every whistle is signaled, and a made three gets both arms up", () => {
		for (const seed of ["a", "b", "c"]) {
			const { tl, crew } = working(seed);
			let calls = 0;
			let threes = 0;
			tl.fx.forEach((f, i) => {
				// Another whistle right behind it takes over.
				const next = tl.fx
					.slice(i + 1)
					.find((g) => g.kind === "whistle" || g.kind === "roar");
				if (next && next.t < f.t + 700) {
					return;
				}
				const refs = crewAt(tl, f.t + 350, crew).states.filter((s) =>
					isRef(s.pid),
				);
				if (f.kind === "whistle" && f.call) {
					assert.isTrue(
						refs.some((r) => SIGNALS.has(r.anim)),
						`${seed}: ${f.call} at ${f.t}`,
					);
					calls += 1;
				} else if (f.kind === "roar" && f.what === "three") {
					assert.isTrue(
						refs.some((r) => r.anim === "threeUp"),
						`${seed}: three at ${f.t}`,
					);
					threes += 1;
				}
			});
			assert.isAbove(calls, 10);
			assert.isAbove(threes, 0);
		}
	}, 60_000);

	test("at the line, the official under the basket bounces the shooter the ball", () => {
		const { tl, crew } = working("a");
		let checked = 0;
		for (const g of tl.ball) {
			// A bounce pass from his hands (not a loose ball coming down off
			// the rim).
			if (
				g.kind !== "fly" ||
				"pid" in g.from ||
				"pid" in g.to ||
				g.from.z < 3 ||
				g.from.z > 6 ||
				g.to.z > 1
			) {
				continue;
			}
			// (At the line: not a ball knocked loose in a scramble.)
			const t0 = g.t0;
			const beat = tl.beats.findLast((b) => b.preStart <= t0);
			if (beat?.type !== "ft" && beat?.type !== "missFt") {
				continue;
			}
			const from = g.from;
			const refs = crewAt(tl, g.t0 - 60, crew).states.filter((s) =>
				isRef(s.pid),
			);
			const near = Math.min(
				...refs.map((r) => Math.hypot(r.x - from.x, r.y - from.y)),
			);
			assert.isBelow(near, 1.6, `at ${g.t0}`);
			// Under the basket: nearer the end line than the line.
			assert.isTrue(from.x < 9 || from.x > COURT_W - 9, `at ${g.t0}`);
			checked += 1;
		}
		assert.isAbove(checked, 4);
	}, 60_000);

	test("the opening tip is thrown up by an official at center court", () => {
		const { tl, crew } = working("a");
		const toss = tl.fx.find((f) => f.kind === "toss")!;
		const refs = crewAt(tl, toss.t - 200, crew).states.filter((s) =>
			isRef(s.pid),
		);
		const tosser = refs.find((r) => r.anim === "toss");
		assert.isDefined(tosser);
		assert.isBelow(
			Math.hypot(tosser!.x - COURT_W / 2, tosser!.y - COURT_H / 2),
			2.5,
		);
	}, 60_000);

	test("an overtime tip: the ball is brought out to the official, who throws it up and backs out", () => {
		const { events, tl } = overtime();
		const crew = crewFor(5, AWAY, HOME);
		const beat = tl.beats.findLast((b) => events[b.i]?.type === "jumpBall")!;
		const toss = tl.fx.find((f) => f.kind === "toss" && f.t > beat.preStart)!;
		const center = (p: { x: number; y: number }) =>
			Math.hypot(p.x - COURT_W / 2, p.y - COURT_H / 2);
		// Handed to him by a man who brought it out - not there at the
		// circle, up in the air on its own, from the start.
		const given = tl.ball.find(
			(s) =>
				s.kind === "fly" &&
				s.t0 >= beat.preStart &&
				s.t1 <= toss.t &&
				"pid" in s.from &&
				!("pid" in s.to) &&
				center(s.to) < 0.6,
		);
		assert.isDefined(given);
		assert.isFalse(
			tl.ball.some(
				(s) => s.kind === "rest" && s.t0 >= beat.preStart && s.t0 < toss.t,
			),
		);
		const refs = (t: number) =>
			crewAt(tl, t, crew).states.filter((s) => isRef(s.pid));
		// Holding it out once it is in his hands, not before.
		assert.isFalse(refs(given!.t0 - 50).some((r) => r.anim === "toss"));
		const tosser = refs(toss.t - 100).find((r) => r.anim === "toss");
		assert.isDefined(tosser);
		assert.isBelow(center(tosser!), 2.5);
		// Then out of there on his own side, not through the two of them.
		for (let t = toss.t; t < toss.t + 1500; t += 100) {
			const r = refs(t).find((s) => s.pid === tosser!.pid)!;
			assert.isBelow(r.y, COURT_H / 2 - 1, `${t}`);
		}
	}, 60_000);

	test("coaches keep to their boxes, photographers to the baselines", () => {
		const { tl, crew } = working("b");
		for (let t = 0; t <= tl.end; t += 400) {
			for (const s of crewAt(tl, t, crew).states) {
				if (s.pid === -11 || s.pid === -12) {
					const team = s.pid === -11 ? 0 : 1;
					assert.isBelow(Math.abs(s.x - benchX(team)), 7, `${t}`);
					// (Out on the floor only in the middle of his huddle.)
					const huddle = s.x === benchX(team) && s.y === HUDDLE_Y;
					assert.isTrue(huddle || (s.y > -4 && s.y < 0.5), `${t}`);
				} else if (s.pid <= -21 && !atTable(s.pid)) {
					assert.isTrue(s.x < -2 || s.x > COURT_W + 2, `${t}`);
					// Clear of the basket's stanchion and the lane under it.
					assert.isTrue(s.y < 17 || s.y > 33, `${t}`);
				}
			}
		}
	}, 60_000);

	// An official comes round to the play - the ball, the way he runs, a
	// signal - over a moment, not spun round in a frame; and so does a coach
	// pacing his sideline. (Before: 403 frames a game an official turned
	// more than 20 degrees from the last - every time he went quicker or
	// slower than a jog, about - and 190 a coach, setting off and stopping.)
	// (Nor spun round after a camera cut, as their pace took in where they
	// were before it; nor about-turned one way one frame and the other the
	// next, after a signal.)
	// (Over three games: one alone can have most of a few games' worth, or
	// none.)
	test("the officials and coaches turn, never spun round in a frame", () => {
		const games = ["a", "b", "c"];
		let frames = 0;
		let spun = 0;
		let others = 0;
		for (const seed of games) {
			const { tl, crew } = working(seed);
			const cuts = cameraCuts(tl);
			const was = new Map<number, number>();
			for (let t = 0; t < Math.min(tl.end, 900_000); t += 16) {
				if (fastAt(tl, t) > 1 || cuts.some((c) => c > t - 16 && c <= t)) {
					was.clear();
					continue;
				}
				for (const r of crewAt(tl, t, crew).states) {
					const yaw = was.get(r.pid);
					if (yaw !== undefined) {
						frames += isRef(r.pid) ? 1 : 0;
						let d = r.yaw - yaw;
						d -= Math.round(d / (Math.PI * 2)) * Math.PI * 2;
						if (Math.abs(d) > (20 * Math.PI) / 180) {
							if (isRef(r.pid)) {
								spun += 1;
							} else {
								others += 1;
							}
						}
					}
					was.set(r.pid, r.yaw);
				}
			}
		}
		assert.isAbove(frames, 50_000 * games.length);
		assert.isBelow(spun, 15 * games.length);
		assert.isAtMost(others, 5 * games.length);
	}, 480_000);

	// Stood, walking or running, an official went from one to the next in a
	// frame - his legs snapped from one stride to the other - and the same
	// into a signal and out of it again.
	// And a photographer's camera snapped up to his eye as the play crossed
	// a line, and down again; a coach was clapping or arms out at a call
	// from one frame to the next, and back.
	test("an official eases from standing to walking to running, and into his signals and out; a photographer's camera comes up as the play comes; a coach claps and folds his arms again", () => {
		const { tl, crew } = working("a");
		const body = bodyOf();
		const cuts = cameraCuts(tl);
		const was = new Map<number, Pt3[]>();
		let frames = 0;
		let popped = 0;
		let snapped = 0;
		let coached = 0;
		for (let t = 0; t < Math.min(tl.end, 900_000); t += 16) {
			if (
				fastAt(tl, t) > 1 ||
				fastAt(tl, t - 16) > 1 ||
				cuts.some((c) => c > t - 16 && c <= t)
			) {
				was.clear();
				continue;
			}
			for (const r of crewAt(tl, t, crew).states) {
				const role = crew.find((m) => m.pid === r.pid)?.role;
				const photo = role === "photo";
				if (!isRef(r.pid) && !photo && role !== "coach") {
					continue;
				}
				const me = withBody(r, body);
				const sk = skeleton(body, poseOf(me));
				const ends = [sk.legR.end, sk.legL.end, sk.armR.end, sk.armL.end].map(
					(v) => bodyPoint(me, v),
				);
				const before = was.get(r.pid);
				if (before) {
					frames += isRef(r.pid) ? 1 : 0;
					// (Sixty feet a second, for a hand or a foot, is a blur.)
					if (
						ends.some(
							(e, k) =>
								Math.hypot(
									e.x - before[k]!.x,
									e.y - before[k]!.y,
									e.z - before[k]!.z,
								) >
								60 * 0.016,
						)
					) {
						if (photo) {
							snapped += 1;
						} else if (role === "coach") {
							coached += 1;
						} else {
							popped += 1;
						}
					}
				}
				was.set(r.pid, ends);
			}
		}
		assert.isAbove(frames, 50_000);
		assert.isBelow(popped / frames, 0.0006, `${popped} in ${frames}`);
		assert.strictEqual(snapped, 0);
		assert.isAtMost(coached, 2);
	}, 240_000);
});
