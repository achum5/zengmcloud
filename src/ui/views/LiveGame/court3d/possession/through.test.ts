import { assert, describe, test } from "vitest";
import type { Act, BallSeg, Move, Track } from "../director.ts";
import {
	carryThrough,
	inTime,
	spotAt,
	throughBoards,
	throughContests,
	throughPasses,
	throughPullUps,
	throughReaches,
} from "./through.ts";

const track = (moves: Move[], acts: Act[] = []): Track => ({
	pid: 1,
	team: 0,
	start: { ...moves[0]!.from },
	moves,
	acts,
	arms: [],
	faces: [],
	looks: [],
	shown: [],
});
const run = (t0: number, t1: number, x0: number, x1: number): Move => ({
	t0,
	t1,
	from: { x: x0, y: 20 },
	to: { x: x1, y: 20 },
	anim: "run",
});
const stand = (t0: number, t1: number, x: number): Move => ({
	t0,
	t1,
	from: { x, y: 20 },
	to: { x, y: 20 },
	anim: "stand" as Move["anim"],
});
const act = (t0: number, t1: number, anim: Act["anim"], extra = {}): Act => ({
	t0,
	t1,
	anim,
	...extra,
});

describe("on through the catch", () => {
	test("a run stood for the catch goes on through it, stopped on the play's spot before his shot goes up", () => {
		const tr = track(
			[run(0, 1500, 40, 60), stand(1500, 4000, 60)],
			[
				act(1700, 1900, "catch"),
				act(2000, 3000, "shoot", { jump: [0.28, 0.87, 1.5] }),
			],
		);
		assert.isTrue(carryThrough(tr, 1800));
		const [a, b] = tr.moves;
		assert.strictEqual(a!.t1, 1800);
		assert.isAtLeast(a!.v1!, 1.5);
		assert.isAtMost(a!.v1!, 7);
		assert.isBelow(a!.to.x, 60);
		// (Stopped on the spot by the time he goes up, not before he starts.)
		assert.strictEqual(b!.v0, a!.v1);
		assert.strictEqual(b!.v1, 0);
		assert.deepEqual(b!.to, { x: 60, y: 20 });
		assert.isAbove(b!.t1, 2000);
		assert.isAtMost(b!.t1, 2000 + 0.28 * 1000 - 60 + 1e-9);
		// (And stood there from then.)
		assert.strictEqual(tr.moves.at(-1)!.t0, b!.t1);
		assert.deepEqual(tr.moves.at(-1)!.from, { x: 60, y: 20 });
	});

	test("never faster at the catch than he could stop from in the time", () => {
		const tr = track(
			[run(0, 1000, 20, 60), stand(1000, 4000, 60)],
			[
				act(1300, 1500, "catch"),
				act(1600, 2400, "triple"),
				act(1600, 2000, "jab"),
			],
		);
		assert.isTrue(carryThrough(tr, 1400));
		const [a, b] = tr.moves;
		const T = (b!.t1 - b!.t0) / 1000;
		assert.isAtMost(a!.v1! / T, 11 + 1e-9);
	});

	test("a pass straight on is thrown on the way", () => {
		const tr = track(
			[run(0, 1500, 40, 60), stand(1500, 4000, 60)],
			[act(1700, 1900, "catch"), act(1900, 2200, "pass")],
		);
		assert.isTrue(carryThrough(tr, 1800));
		assert.strictEqual(tr.moves[1]!.t1, 1800 + 450);
	});

	test("off on another run straight after it, he goes on into that one", () => {
		const tr = track(
			[run(0, 1500, 40, 60), stand(1500, 1900, 60), run(1900, 4000, 60, 90)],
			[act(1700, 1900, "catch")],
		);
		assert.isTrue(carryThrough(tr, 1800));
		assert.strictEqual(tr.moves.length, 2);
		const [a, b] = tr.moves;
		assert.strictEqual(a!.t1, b!.t0);
		assert.deepEqual(a!.to, b!.from);
		assert.isAbove(a!.v1!, 1.5);
		assert.strictEqual(a!.v1, b!.v0);
	});

	test("turning back the way he came, he plants", () => {
		const tr = track(
			[run(0, 1500, 40, 60), stand(1500, 1900, 60), run(1900, 4000, 60, 30)],
			[act(1700, 1900, "catch")],
		);
		carryThrough(tr, 1800);
		const b = tr.moves.find((m) => m.t0 >= 1800 && m.from.x !== m.to.x)!;
		// (A change of no more than 4 ft/s in the way he is going.)
		assert.isAtMost(2 * (b.v0 ?? 0), 4 + 1e-9);
	});

	test("a pass thrown off the dribble leaves his hands on the move, and is caught on the move", () => {
		const passer = {
			...track(
				[run(0, 1700, 40, 60), stand(1700, 4000, 60)],
				[act(1880, 2180, "pass")],
			),
			pid: 1,
		};
		const catcher = {
			...track(
				[
					{
						...run(1000, 2100, 60, 75),
						from: { x: 60, y: 40 },
						to: { x: 75, y: 40 },
					},
					{
						...stand(2100, 4000, 75),
						from: { x: 75, y: 40 },
						to: { x: 75, y: 40 },
					},
				],
				[act(2310, 2510, "catch")],
			),
			pid: 2,
		};
		const ball: BallSeg[] = [
			{ kind: "hold", t0: 0, pid: 1, style: "dribble" },
			{ kind: "hold", t0: 1850, pid: 1, style: "hold" },
			{ kind: "fly", t0: 2000, t1: 2400, from: { pid: 1 }, to: { pid: 2 } },
			{ kind: "hold", t0: 2400, pid: 2, style: "hold" },
		];
		const tracks = new Map([
			[1, passer],
			[2, catcher],
		]);
		assert.strictEqual(throughPasses(tracks, ball), 2);
		assert.strictEqual(passer.moves[0]!.t1, 2000);
		assert.isAbove(passer.moves[0]!.v1!, 1.5);
		assert.strictEqual(catcher.moves[0]!.t1, 2400);
		assert.isAbove(catcher.moves[0]!.v1!, 1.5);
	});

	test("a pull-up is gathered into off the dribble, stopped on the spot before he goes up", () => {
		const shooter = track(
			[run(0, 1500, 40, 60), stand(1500, 4000, 60)],
			[act(1900, 2900, "shoot", { jump: [0.28, 0.87, 1.5] })],
		);
		const ball: BallSeg[] = [
			{ kind: "hold", t0: 0, pid: 1, style: "dribble" },
			{ kind: "hold", t0: 1700, pid: 1, style: "hold" },
			{
				kind: "fly",
				t0: 2100,
				t1: 3100,
				from: { pid: 1 },
				to: { x: 88, y: 25, z: 10 },
			},
		];
		assert.strictEqual(throughPullUps(new Map([[1, shooter]]), ball), 1);
		const [a, b] = shooter.moves;
		assert.strictEqual(a!.t1, 1700);
		assert.isAtLeast(a!.v1!, 1.5);
		assert.deepEqual(b!.to, { x: 60, y: 20 });
		assert.strictEqual(b!.v1, 0);
		assert.isAtMost(b!.t1, 1900 + 0.28 * 1000 - 60 + 1e-9);
	});

	test("a jumper off the catch is no pull-up", () => {
		const shooter = track(
			[run(0, 1500, 40, 60), stand(1500, 4000, 60)],
			[act(1900, 2900, "shoot", { jump: [0.28, 0.87, 1.5] })],
		);
		const ball: BallSeg[] = [
			{ kind: "fly", t0: 1200, t1: 1700, from: { pid: 2 }, to: { pid: 1 } },
			{ kind: "hold", t0: 1700, pid: 1, style: "hold" },
			{
				kind: "fly",
				t0: 2100,
				t1: 3100,
				from: { pid: 1 },
				to: { x: 88, y: 25, z: 10 },
			},
		];
		assert.strictEqual(throughPullUps(new Map([[1, shooter]]), ball), 0);
		assert.strictEqual(shooter.moves[0]!.t1, 1500);
	});

	test("hopping on into the air the way he was going, the hop's first step is one run with the one in", () => {
		const tr = track(
			[
				run(0, 1500, 40, 60),
				{ ...run(1500, 1700, 60, 61), v1: 4 },
				{ ...run(1700, 2000, 61, 62.2), v0: 4, v1: 4 },
				{ ...stand(2000, 4000, 62.2) },
			],
			[act(1650, 2650, "shoot", { jump: [0.28, 0.87, 1.5] })],
		);
		assert.isTrue(carryThrough(tr, 1400));
		assert.strictEqual(tr.moves.length, 3);
		const [a, b] = tr.moves;
		assert.strictEqual(a!.t1, b!.t0);
		assert.deepEqual(a!.to, b!.from);
		assert.strictEqual(a!.v1, b!.v0);
	});

	test("the stride or two after a run of the set's spacing is his own, as the run was", () => {
		const m = run(0, 1500, 40, 60);
		const tr = track([m, stand(1500, 4000, 60)], [act(1700, 1900, "catch")]);
		const spacing = new Set<Move>([m]);
		assert.isTrue(carryThrough(tr, 1800, spacing));
		assert.strictEqual(tr.moves[0], m);
		assert.isTrue(spacing.has(tr.moves[1]!));
	});

	test("stood a long while for it, or barely moved to it: as it was", () => {
		const long = track(
			[run(0, 500, 40, 60), stand(500, 4000, 60)],
			[act(1700, 1900, "catch")],
		);
		assert.isFalse(carryThrough(long, 1800));
		const short = track(
			[run(1000, 1500, 59, 60), stand(1500, 4000, 60)],
			[act(1700, 1900, "catch")],
		);
		assert.isFalse(carryThrough(short, 1800));
	});
});

describe("closing out through the shot", () => {
	const shooterAt = (x: number, y: number): Track => ({
		...track([
			{
				t0: 0,
				t1: 4000,
				from: { x, y },
				to: { x, y },
				anim: "stand" as Move["anim"],
			},
		]),
		pid: 1,
		team: 0,
	});
	const defender = (moves: Move[], acts: Act[]): Track => ({
		...track(moves, acts),
		pid: 2,
		team: 1,
	});
	const contest = act(1740, 2420, "contest", { jump: [0.095, 0.955, 1.46] });
	const off = 1740 + 0.095 * 680;
	const down = 1740 + 0.955 * 680;
	const shot: BallSeg[] = [
		{ kind: "hold", t0: 0, pid: 1, style: "hold" },
		{
			kind: "fly",
			t0: 2000,
			t1: 3000,
			from: { pid: 1 },
			to: { x: 88.75, y: 25, z: 10 },
		},
	];
	const closeout = (): Move => ({
		t0: 0,
		t1: 1500,
		from: { x: 60, y: 35 },
		to: { x: 60, y: 25 },
		anim: "closeout",
	});
	const nearestTo = (
		tr: Track,
		P: { x: number; y: number },
		t0: number,
		t1: number,
	) => {
		let d = Infinity;
		for (let t = t0; t <= t1; t += 10) {
			const Q = spotAt(tr, t);
			d = Math.min(d, Math.hypot(Q.x - P.x, Q.y - P.y));
		}
		return d;
	};

	test("a closeout stood for the shot is still coming as it goes up: on the play's spot then, into the air at that pace, stopped short of him", () => {
		const d = defender([closeout(), stand(1500, 4000, 60)], [contest]);
		d.moves[1]!.from = { x: 60, y: 25 };
		d.moves[1]!.to = { x: 60, y: 25 };
		const tracks = new Map([
			[1, shooterAt(60, 20)],
			[2, d],
		]);
		assert.strictEqual(throughContests(tracks, shot), 1);
		const [a, air, brake] = d.moves;
		// (On the play's spot as it leaves his hands - the room it gives him.)
		const R = spotAt(d, 2000);
		assert.isBelow(Math.hypot(R.x - 60, R.y - 25), 1e-6);
		assert.strictEqual(a!.t1, off);
		assert.isAtLeast(a!.v1!, 1.5);
		assert.strictEqual(air!.t0, off);
		assert.strictEqual(air!.t1, down);
		assert.strictEqual(air!.v0, a!.v1);
		assert.strictEqual(air!.v1, a!.v1);
		assert.strictEqual(brake!.v1, 0);
		assert.isAtLeast(nearestTo(d, { x: 60, y: 20 }, 2000, 4000), 1.5 - 1e-6);
	});

	test("off to the boards straight after he comes down, he goes on into it", () => {
		const d = defender(
			[
				closeout(),
				{
					...stand(1500, down + 200, 60),
					from: { x: 60, y: 25 },
					to: { x: 60, y: 25 },
				},
				{
					t0: down + 200,
					t1: 3500,
					from: { x: 60, y: 25 },
					to: { x: 75, y: 25 },
					anim: "run",
				},
			],
			[contest],
		);
		const tracks = new Map([
			[1, shooterAt(60, 20)],
			[2, d],
		]);
		assert.strictEqual(throughContests(tracks, shot), 1);
		const n = d.moves.at(-1)!;
		const brake = d.moves.at(-2)!;
		assert.strictEqual(brake.t1, n.t0);
		assert.deepEqual(brake.to, n.from);
		assert.isAbove(n.v0!, 0);
		assert.strictEqual(brake.v1, n.v0);
		assert.isAtLeast(nearestTo(d, { x: 60, y: 20 }, 2000, 3500), 1.5 - 1e-6);
	});

	test("a box-out straight after: stopped by the time it starts", () => {
		const d = defender(
			[
				closeout(),
				{
					...stand(1500, 4000, 60),
					from: { x: 60, y: 25 },
					to: { x: 60, y: 25 },
				},
			],
			[contest, act(down + 150, down + 900, "boxOut")],
		);
		const tracks = new Map([
			[1, shooterAt(60, 20)],
			[2, d],
		]);
		assert.strictEqual(throughContests(tracks, shot), 1);
		const stop = d.moves.find((m) => m.t0 >= down && m.v1 === 0)!;
		assert.isAtMost(stop.t1, down + 150);
	});

	test("stood on his spot a long while for it: stood a step further off, and in through it", () => {
		const d = defender(
			[
				{ ...closeout(), t1: 500 },
				{
					...stand(500, 4000, 60),
					from: { x: 60, y: 25 },
					to: { x: 60, y: 25 },
				},
			],
			[contest],
		);
		const tracks = new Map([
			[1, shooterAt(60, 20)],
			[2, d],
		]);
		assert.strictEqual(throughContests(tracks, shot), 1);
		// (On the play's spot as it leaves his hands, still coming.)
		const R = spotAt(d, 2000);
		assert.isBelow(Math.hypot(R.x - 60, R.y - 25), 1e-6);
		const into = d.moves.find((m) => m.t1 === off)!;
		assert.isAtLeast(into.v1!, 1.5);
		// (Stood further off him before it, not on the spot.)
		const B = spotAt(d, 1000);
		assert.isAtLeast(Math.hypot(B.x - 60, B.y - 20), 5 + 1);
		assert.isAtLeast(nearestTo(d, { x: 60, y: 20 }, 2000, 4000), 1.5 - 1e-6);
	});

	test("still on his way to his spot as he goes up: in through it, not stopped in the air", () => {
		const d = defender(
			[
				{ ...closeout(), t0: 500, t1: off + 300 },
				{
					...stand(off + 300, 4000, 60),
					from: { x: 60, y: 25 },
					to: { x: 60, y: 25 },
				},
			],
			[contest],
		);
		const R0 = spotAt(d, 2000);
		const tracks = new Map([
			[1, shooterAt(60, 20)],
			[2, d],
		]);
		assert.strictEqual(throughContests(tracks, shot), 1);
		// (Where the play's run had him as it leaves the shooter's hands.)
		const R = spotAt(d, 2000);
		assert.isBelow(Math.hypot(R.x - R0.x, R.y - R0.y), 1e-6);
		const into = d.moves.find((m) => m.t1 === off)!;
		assert.isAtLeast(into.v1!, 1.5);
		const air = d.moves.find((m) => m.t0 === off)!;
		assert.strictEqual(air.t1, down);
		assert.strictEqual(air.v0, into.v1);
		assert.strictEqual(air.v1, into.v1);
		assert.isAtLeast(nearestTo(d, { x: 60, y: 20 }, 2000, 4000), 1.5 - 1e-6);
	});

	test("his next run set off while he is still up: off on it once he is down", () => {
		const d = defender(
			[
				closeout(),
				{
					...stand(1500, off + 100, 60),
					from: { x: 60, y: 25 },
					to: { x: 60, y: 25 },
				},
				{
					t0: off + 100,
					t1: 3500,
					from: { x: 60, y: 25 },
					to: { x: 66, y: 25 },
					anim: "run",
				},
				{
					...stand(3500, 4500, 66),
					from: { x: 66, y: 25 },
					to: { x: 66, y: 25 },
				},
			],
			[contest],
		);
		const tracks = new Map([
			[1, shooterAt(60, 20)],
			[2, d],
		]);
		assert.strictEqual(throughContests(tracks, shot), 1);
		const [before, n, after] = d.moves.slice(-3);
		assert.deepEqual(n!.to, { x: 66, y: 25 });
		assert.isAtLeast(n!.t0, down);
		assert.strictEqual(before!.t1, n!.t0);
		assert.deepEqual(before!.to, n!.from);
		// (All of it that much later: he is only stood after it.)
		assert.closeTo(n!.t1 - n!.t0, 3500 - (off + 100), 1e-6);
		assert.strictEqual(after!.t0, n!.t1);
		assert.strictEqual(after!.t1, 4500);
		assert.isAtLeast(nearestTo(d, { x: 60, y: 20 }, 2000, 3500), 1.5 - 1e-6);
	});

	test("drifting in too near his spot to get going that fast: pulls up, then in", () => {
		const d = defender(
			[
				{
					t0: -400,
					t1: 300,
					from: { x: 60, y: 36 },
					to: { x: 60, y: 29 },
					anim: "run",
					v1: 8,
				},
				{ ...closeout(), t0: 300, from: { x: 60, y: 29 }, v0: 8 },
				{
					...stand(1500, 4000, 60),
					from: { x: 60, y: 25 },
					to: { x: 60, y: 25 },
				},
			],
			[contest],
		);
		const tracks = new Map([
			[1, shooterAt(60, 20)],
			[2, d],
		]);
		assert.strictEqual(throughContests(tracks, shot), 1);
		// (Pulled up from the pace he came at...)
		const brake = d.moves[1]!;
		assert.strictEqual(brake.t0, 300);
		assert.strictEqual(brake.v0, 8);
		assert.isNotOk(brake.v1);
		assert.isBelow(brake.to.y, 29);
		// (...and in from there, on the play's spot as it goes, still coming.)
		const into = d.moves.find((m) => m.t1 === off)!;
		assert.isNotOk(into.v0);
		assert.deepEqual(into.from, brake.to);
		assert.isAtLeast(into.v1!, 1.5);
		const R = spotAt(d, 2000);
		assert.isBelow(Math.hypot(R.x - 60, R.y - 25), 1e-6);
	});

	test("boxing out right up to it: as it was", () => {
		const d = defender(
			[
				{ ...closeout(), t1: 500 },
				{
					...stand(500, 4000, 60),
					from: { x: 60, y: 25 },
					to: { x: 60, y: 25 },
				},
			],
			[act(600, 1700, "boxOut"), contest],
		);
		const tracks = new Map([
			[1, shooterAt(60, 20)],
			[2, d],
		]);
		assert.strictEqual(throughContests(tracks, shot), 0);
		assert.strictEqual(d.moves.length, 2);
	});
});

describe("going up for a rebound on the move", () => {
	const shooter: Track = {
		...track([
			{
				t0: 0,
				t1: 4000,
				from: { x: 70, y: 30 },
				to: { x: 70, y: 30 },
				anim: "stand" as Move["anim"],
			},
		]),
		pid: 1,
		team: 0,
	};
	const ball: BallSeg[] = [
		{ kind: "hold", t0: 0, pid: 1, style: "hold" },
		{
			kind: "fly",
			t0: 1000,
			t1: 2000,
			from: { pid: 1 },
			to: { x: 88.75, y: 25, z: 10 },
		},
	];
	// (Up at 2000, the ball in his hands at the top of it, 2400.)
	const board = act(2000, 3100, "board", {
		jump: [250 / 1100, 550 / 1100, 2.4],
	});
	const off = 2000 + 250;
	const top = 2400;
	const rebounder = (moves: Move[], acts: Act[] = [board]): Track => ({
		...track(moves, acts),
		pid: 2,
		team: 1,
	});
	const pace = (tr: Track, t: number) => {
		const A = spotAt(tr, t - 10);
		const B = spotAt(tr, t + 10);
		return Math.hypot(B.x - A.x, B.y - A.y) / 0.02;
	};

	test("a run in stood for it goes on through it: on the play's spot at the top of his jump, still going", () => {
		const r = rebounder([run(1000, 1600, 74, 80), stand(1600, 4000, 80)]);
		const tracks = new Map([
			[1, shooter],
			[2, r],
		]);
		assert.strictEqual(throughBoards(tracks, ball), 1);
		const P = spotAt(r, top);
		assert.isBelow(Math.hypot(P.x - 80, P.y - 20), 1e-6);
		const into = r.moves.find((m) => m.t1 === off)!;
		assert.isAtLeast(into.v1!, 1.5);
		assert.isAbove(pace(r, top), 1.5);
		// (Never sooner off than the play had him.)
		assert.isAtLeast(r.moves[0]!.t0, 1000);
	});

	test("the shuffle onto the spot at the end of his run is taken in with it", () => {
		const r = rebounder([
			run(1000, 1500, 74, 79.6),
			run(1500, 1660, 79.6, 80),
			stand(1660, 4000, 80),
		]);
		const tracks = new Map([
			[1, shooter],
			[2, r],
		]);
		assert.strictEqual(throughBoards(tracks, ball), 1);
		const P = spotAt(r, top);
		assert.isBelow(Math.hypot(P.x - 80, P.y - 20), 1e-6);
		assert.isAbove(pace(r, top), 1.5);
	});

	test("up for it straight off a contest of his: as it was", () => {
		const r = rebounder(
			[run(1000, 1400, 74, 80), stand(1400, 4000, 80)],
			[act(1400, 2000, "contest", { jump: [0.1, 0.9, 1.5] }), board],
		);
		const tracks = new Map([
			[1, shooter],
			[2, r],
		]);
		assert.strictEqual(throughBoards(tracks, ball), 0);
		assert.strictEqual(r.moves.length, 2);
	});

	test("stood right on the spot all along: as it was", () => {
		const r = rebounder([stand(0, 4000, 80)]);
		const tracks = new Map([
			[1, shooter],
			[2, r],
		]);
		assert.strictEqual(throughBoards(tracks, ball), 0);
		assert.strictEqual(r.moves.length, 1);
	});
});

describe("in time for it", () => {
	// (Every move after the one before it, none of them overlapping.)
	const inOrder = (tr: Track) => {
		for (let i = 1; i < tr.moves.length; i++) {
			assert.isAtMost(tr.moves[i - 1]!.t1, tr.moves[i]!.t0 + 1e-6);
		}
	};
	const hack = act(2900, 3570, "hack", { jump: [0.12, 0.8, 0.3] });

	test("sent back for a foul the way he was going, quicker than a body turns: off for it sooner, pulling up first", () => {
		const tr = track(
			[run(0, 2000, 40, 60), run(2000, 3000, 60, 40), stand(3000, 5000, 40)],
			[hack],
		);
		assert.isTrue(inTime(tr, hack));
		inOrder(tr);
		const into = tr.moves.find((m) => m.t1 === 3000)!;
		// (Set off a good while sooner, from short of where the play had him
		// turn - and there on time.)
		assert.isAtMost(into.t0, 1500);
		assert.isBelow(into.from.x, 60);
		assert.strictEqual(into.to.x, 40);
		// (Pulling up first, still the way he was going.)
		const brake = tr.moves[tr.moves.indexOf(into) - 1]!;
		assert.isAtLeast(brake.to.x, brake.from.x);
		assert.strictEqual(brake.v1, 0);
		assert.strictEqual(brake.t1, into.t0);
		// (The rest as it was.)
		assert.strictEqual(tr.moves.at(-1)!.t0, 3000);
		assert.strictEqual(tr.moves[0]!.t0, 0);
	});

	test("a reach he has time for: as it was", () => {
		const reach = act(2900, 3300, "reach");
		const moves = [run(1000, 3000, 40, 50), stand(3000, 5000, 50)];
		const tr = track(
			moves.map((m) => ({ ...m })),
			[reach],
		);
		assert.isFalse(inTime(tr, reach));
		assert.deepEqual(tr.moves, moves);
	});

	test("in a screen till he sets off: as it was", () => {
		const late = act(2200, 2870, "hack", { jump: [0.12, 0.8, 0.3] });
		const moves = [stand(0, 1500, 60), run(1500, 2300, 60, 40)];
		const tr = track(
			moves.map((m) => ({ ...m })),
			[act(0, 1500, "screen"), late],
		);
		assert.isFalse(inTime(tr, late));
		assert.deepEqual(tr.moves, moves);
	});

	test("a loose ball grabbed on the move, not stood for", () => {
		const tr = track(
			[run(0, 1000, 40, 50), stand(1000, 3000, 50)],
			[act(1300, 1850, "snatch")],
		);
		const ball: BallSeg[] = [
			{
				kind: "fly",
				t0: 1000,
				t1: 1500,
				from: { x: 55, y: 20, z: 3 },
				to: { pid: 1 },
			},
			{ kind: "hold", t0: 1500, pid: 1, style: "hold" },
		];
		assert.isAtLeast(throughReaches(new Map([[1, tr]]), ball), 1);
		inOrder(tr);
		const into = tr.moves.find((m) => m.t1 === 1500)!;
		assert.isAtLeast(into.v1!, 1.5);
		const P = spotAt(tr, 1500);
		assert.isBelow(Math.abs(P.x - 50), 2);
		// (Stopped on the play's spot after it.)
		assert.strictEqual(spotAt(tr, 2500).x, 50);
	});
});
