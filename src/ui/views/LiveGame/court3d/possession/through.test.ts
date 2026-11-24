import { assert, describe, test } from "vitest";
import type { Act, BallSeg, Move, Track } from "../director.ts";
import { carryThrough, throughPasses, throughPullUps } from "./through.ts";

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
