import { assert, describe, test } from "vitest";
import {
	compileCourt,
	type CourtPlayer,
	type RawEvent,
	type Stakes,
} from "./director.ts";
import { bodyPoint, evalPlayer, poseOf } from "./evaluate.ts";
import { attackDir, benchX, COURT_W, HUDDLE_Y } from "./geometry.ts";
import { bodyOf, skeleton } from "./poses.ts";

// The last seconds of a game, the sim's way, won (or not) on the last shot.
const ending = (
	lastShot: RawEvent[],
	stakes?: Stakes,
	horn: RawEvent = { type: "endOfPeriod", t: 0, reason: "noShot", clock: 0 },
) => {
	const players: CourtPlayer[] = [];
	const POS = ["PG", "SG", "SF", "PF", "C"];
	const events: RawEvent[] = [{ type: "init", boxScore: {} }];
	for (const raw of [0, 1] as const) {
		for (let j = 0; j < 5; j++) {
			const pid = raw * 10 + j + 1;
			players.push({ pid, team: raw === 0 ? 1 : 0, pos: POS[j] });
			events.push(
				{ type: "stat", t: raw, pid, s: "pts", amt: 0 },
				{ type: "stat", t: raw, pid, s: "gs", amt: 1 },
			);
		}
	}
	events.push(
		{ type: "jumpBall", t: 0, pid: 5, pid2: 15, clock: 9 },
		{ type: "fgaMidRange", t: 1, pid: 12, clock: 6, desperation: false },
		{ type: "fgMidRange", t: 1, pid: 12, clock: 5.6 },
		{ type: "stat", t: 1, pid: 12, s: "pts", amt: 2 },
		...lastShot,
		horn,
		{ type: "gameOver" },
	);
	return compileCourt({ events, players, gid: 3, stakes });
};

describe("3D end of game", () => {
	test("a game-winner is mobbed, and everybody daps up the other team", () => {
		const tl = ending([
			{ type: "fgaTp", t: 0, pid: 2, clock: 1.4, desperation: false },
			{ type: "tp", t: 0, pid: 2, clock: 0.6 },
			{ type: "stat", t: 0, pid: 2, s: "pts", amt: 3 },
		]);
		const over = tl.beats.at(-1)!;
		// His teammates all over him - from wherever on the floor the horn
		// found them.
		for (const mate of [1, 3, 4, 5]) {
			let near = Infinity;
			for (let t = over.actionStart; t <= over.actionStart + 4000; t += 100) {
				const H = evalPlayer(tl, 2, t);
				const M = evalPlayer(tl, mate, t);
				near = Math.min(near, Math.hypot(M.x - H.x, M.y - H.y));
			}
			assert.isBelow(near, 4, `${mate}`);
		}
		// Everybody daps up at least two of the other team - no line: each
		// with a different man, the two of them face to face, gripping right
		// hands; and pulled into a hug, their heads past each other's.
		const body = bodyOf();
		const DAPS = new Set(["dap", "dapPat", "dapHug"]);
		const hugs = { n: 0 };
		for (const pid of [1, 2, 3, 4, 5]) {
			const daps = tl.tracks
				.get(pid)!
				.acts.filter((a) => DAPS.has(a.anim) && a.t0 > over.actionStart);
			const withWhom = new Set<number>();
			for (const a of daps) {
				const them = [11, 12, 13, 14, 15].find((q) =>
					tl.tracks
						.get(q)!
						.acts.some((b) => b.anim === a.anim && Math.abs(b.t0 - a.t0) < 1),
				);
				assert.isDefined(them, `${pid} ${a.anim} ${a.t0}`);
				withWhom.add(them!);
				const t = (a.t0 + a.t1) / 2;
				const S = evalPlayer(tl, pid, t);
				const O = evalPlayer(tl, them!, t);
				const sk = skeleton(body, poseOf(S));
				const ok = skeleton(body, poseOf(O));
				const h = bodyPoint(S, sk.armR.end);
				const g = bodyPoint(O, ok.armR.end);
				assert.isBelow(Math.hypot(h.x - g.x, h.y - g.y, h.z - g.z), 0.35);
				if (a.anim === "dapHug") {
					hugs.n++;
					const H = bodyPoint(S, sk.head);
					const G = bodyPoint(O, ok.head);
					assert.isAbove(Math.hypot(H.x - G.x, H.y - G.y), 1.1);
				}
			}
			assert.isAtLeast(withWhom.size, 2, `${pid}`);
			assert.strictEqual(withWhom.size, daps.length, `${pid}`);
		}
		assert.isAbove(hugs.n, 0);
		// And then off the floor, each side to its own end of the benches
		// (walking off as the picture holds on the building).
		const last = tl.beats.at(-1)!.end + 20_000;
		for (const pid of [1, 2, 3, 4, 5, 11, 12, 13, 14, 15]) {
			const P = evalPlayer(tl, pid, last);
			assert.isBelow(P.y, 0, `${pid}`);
		}
	});

	// Ahead with the ball at the end: brought up and the last of the clock
	// dribbled out - and at the horn it is over where they are, nobody off to
	// the bench and back.
	test("the clock is dribbled out, and the horn finds them on the floor", () => {
		const tl = ending([], undefined, {
			type: "endOfPeriod",
			t: 0,
			reason: "runOutClock",
			clock: 0,
		});
		const n = tl.beats.length;
		const horn = tl.beats[n - 2]!;
		assert.strictEqual(horn.type, "endOfPeriod");
		const s = tl.ball.findLast((b) => b.t0 <= horn.actionStart - 1000);
		assert.isTrue(
			s?.kind === "hold" && s.style !== "hold" && s.pid <= 5,
			JSON.stringify(s),
		);
		const huddles = [benchX(0), benchX(1)].map((x) => ({ x, y: HUDDLE_Y }));
		for (
			let t = horn.preStart;
			t <= tl.beats[n - 1]!.actionStart + 2000;
			t += 100
		) {
			for (const pid of [1, 2, 3, 4, 5, 11, 12, 13, 14, 15]) {
				const P = evalPlayer(tl, pid, t);
				for (const h of huddles) {
					assert.isAbove(Math.hypot(P.x - h.x, P.y - h.y), 3, `${pid} ${t}`);
				}
			}
		}
	});

	// A quarter run out with no shot: the seconds left are played - the
	// ball got up the floor and worked for a shot that never comes - before
	// the horn and the walk to the benches.
	test("a quarter's last seconds are played out before the horn", () => {
		const players: CourtPlayer[] = [];
		const events: RawEvent[] = [{ type: "init", boxScore: {} }];
		for (const raw of [0, 1] as const) {
			for (let j = 0; j < 5; j++) {
				const pid = raw * 10 + j + 1;
				players.push({ pid, team: raw === 0 ? 1 : 0, pos: "G" });
				events.push({ type: "stat", t: raw, pid, s: "gs", amt: 1 });
			}
		}
		events.push(
			{ type: "jumpBall", t: 0, pid: 5, pid2: 15, clock: 14 },
			{ type: "fgaMidRange", t: 1, pid: 12, clock: 9, desperation: false },
			{ type: "missMidRange", t: 1, pid: 12, clock: 8.6 },
			{ type: "drb", t: 0, pid: 4, clock: 8 },
			{ type: "endOfPeriod", t: 0, reason: "noShot", clock: 0 },
			{ type: "period", period: 2, clock: 720 },
			{ type: "endOfPeriod", t: 1, reason: "noShot", clock: 0 },
			{ type: "gameOver" },
		);
		const tl = compileCourt({ events, players, gid: 4 });
		const horn = tl.beats.find((b) => b.type === "endOfPeriod")!;
		assert.isAbove(horn.actionStart - horn.preStart, 3000);
		const t = horn.actionStart - 400;
		const s = tl.ball.findLast((b) => b.t0 <= t);
		const pid = s?.kind === "hold" ? s.pid : undefined;
		assert.isTrue(pid !== undefined && pid <= 5, JSON.stringify(s));
		const P = evalPlayer(tl, pid!, t);
		assert.isAbove((P.x - COURT_W / 2) * attackDir(1), 0);
	});

	// A quarter that ends on a shot at the horn has no line of its own for
	// the break (the sim goes straight to the next period): it is had all
	// the same, everybody off to his huddle.
	test("a quarter ended by a shot at the horn still breaks at the benches", () => {
		const players: CourtPlayer[] = [];
		const events: RawEvent[] = [{ type: "init", boxScore: {} }];
		for (const raw of [0, 1] as const) {
			for (let j = 0; j < 5; j++) {
				const pid = raw * 10 + j + 1;
				players.push({ pid, team: raw === 0 ? 1 : 0, pos: "G" });
				events.push({ type: "stat", t: raw, pid, s: "gs", amt: 1 });
			}
		}
		events.push(
			{ type: "jumpBall", t: 0, pid: 5, pid2: 15, clock: 14 },
			{ type: "fgaTp", t: 1, pid: 12, clock: 0.4, desperation: true },
			{ type: "missTp", t: 1, pid: 12, clock: 0 },
			{ type: "period", period: 2, clock: 720 },
			{ type: "fgaMidRange", t: 0, pid: 2, clock: 705, desperation: false },
			{ type: "fgMidRange", t: 0, pid: 2, clock: 704.6 },
			{ type: "endOfPeriod", t: 1, reason: "noShot", clock: 0 },
			{ type: "gameOver" },
		);
		const tl = compileCourt({ events, players, gid: 6 });
		const brk = tl.beats.find((b) => b.type === "period")!;
		for (const pid of [1, 2, 3, 4, 5, 11, 12, 13, 14, 15]) {
			const team = players.find((p) => p.pid === pid)!.team;
			let near = Infinity;
			for (let t = brk.preStart; t <= brk.end; t += 100) {
				const P = evalPlayer(tl, pid, t);
				near = Math.min(near, Math.hypot(P.x - benchX(team), P.y - HUDDLE_Y));
			}
			assert.isBelow(near, 5, `${pid}`);
		}
	});

	// The playoffs: a series won is mobbed and its bench storms the floor; the
	// championship, won at home, brings the confetti down.
	test("a series won is celebrated, a title at home with confetti", () => {
		const buzzer: RawEvent[] = [
			{ type: "fgaTp", t: 0, pid: 2, clock: 1.4, desperation: false },
			{ type: "tp", t: 0, pid: 2, clock: 0.6 },
			{ type: "stat", t: 0, pid: 2, s: "pts", amt: 3 },
		];
		const kinds = (stakes?: Stakes) =>
			ending(buzzer, stakes)
				.fx.filter((f) => f.kind === "clinch" || f.kind === "confetti")
				.map((f) => `${f.kind}${f.big ? "!" : ""}:${f.team}`);
		// (Home, side 1, wins it.)
		assert.deepEqual(kinds(), []);
		assert.deepEqual(kinds({ won: [2, 2], toWin: 4, finals: false }), []);
		assert.deepEqual(kinds({ won: [2, 3], toWin: 4, finals: false }), [
			"clinch:1",
		]);
		assert.deepEqual(kinds({ won: [3, 3], toWin: 4, finals: true }), [
			"clinch!:1",
			"confetti:1",
		]);
		// The road team winning it all: no confetti in somebody else's building.
		const away = ending(
			[
				{ type: "fgaTp", t: 1, pid: 12, clock: 1.4, desperation: false },
				{ type: "tp", t: 1, pid: 12, clock: 0.6 },
				{ type: "stat", t: 1, pid: 12, s: "pts", amt: 3 },
			],
			{ won: [3, 3], toWin: 4, finals: true },
		);
		assert.deepEqual(
			away.fx
				.filter((f) => f.kind === "clinch" || f.kind === "confetti")
				.map((f) => `${f.kind}:${f.team}`),
			["clinch:0"],
		);
		// And the title is celebrated longer.
		const plain = ending(buzzer).beats.at(-1)!;
		const title = ending(buzzer, {
			won: [3, 3],
			toWin: 4,
			finals: true,
		}).beats.at(-1)!;
		assert.isAbove(
			title.end - title.actionStart,
			plain.end - plain.actionStart,
		);
	});
});
