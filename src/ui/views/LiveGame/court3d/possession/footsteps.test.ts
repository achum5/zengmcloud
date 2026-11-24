import { assert, describe, test } from "vitest";
import type { Pt } from "../geometry.ts";
import {
	type FootStep,
	footPlanner,
	footsteps,
	type StepIn,
} from "./footsteps.ts";
import { stepsPerSecond } from "./gait.ts";

// A path from how he goes each moment: (t ms) => [vx, vy] (feet a second),
// stepped a millisecond at a time from `start`.
const pathOf = (
	start: Pt,
	until: number,
	vel: (t: number) => [number, number],
): ((t: number) => Pt) => {
	const xs = new Float64Array(until + 1);
	const ys = new Float64Array(until + 1);
	let x = start.x;
	let y = start.y;
	for (let t = 0; t <= until; t++) {
		xs[t] = x;
		ys[t] = y;
		const [vx, vy] = vel(t);
		x += vx / 1000;
		y += vy / 1000;
	}
	return (t) => {
		const i = Math.max(0, Math.min(until, Math.round(t)));
		return { x: xs[i]!, y: ys[i]! };
	};
};

const INCHES = 78;
const inOf = (
	at: (t: number) => Pt,
	to: number,
	yaw: (t: number) => number,
	stance = false,
): StepIn => ({
	from: 0,
	to,
	at,
	yaw,
	stance: () => stance,
	cadence: (mode, v) =>
		stepsPerSecond(
			mode === "walk" ? "walk" : mode === "back" ? "backpedal" : "run",
			v,
			INCHES,
		),
	height: INCHES / 12,
});
const run = (
	at: (t: number) => Pt,
	to: number,
	yaw: (t: number) => number,
	stance = false,
): FootStep[] => footsteps(inOf(at, to, yaw, stance));

const along = (s: FootStep, at: (t: number) => Pt, dir: Pt) => {
	const c = at((s.t0 + Math.min(s.t1, s.t0 + 2000)) / 2);
	return (s.x - c.x) * dir.x + (s.y - c.y) * dir.y;
};

describe("3D possession sim: where his feet come down", () => {
	test("running straight: one foot then the other, on his beat, under him", () => {
		const at = pathOf({ x: 0, y: 25 }, 4000, () => [15, 0]);
		const steps = run(at, 4000, () => 0);
		const going = steps.filter((s) => s.t0 > 1000 && s.t1 < 3500);
		assert.isAtLeast(going.length, 4);
		const rate = stepsPerSecond("run", 15, INCHES);
		for (let i = 1; i < going.length; i++) {
			const a = going[i - 1]!;
			const b = going[i]!;
			assert.notEqual(a.foot, b.foot);
			assert.closeTo(b.t0 - a.t0, 1000 / rate, 30);
			assert.closeTo(b.x - a.x, 15 / rate, 0.5);
			// (About halfway through its time down, under him - out to its own
			// side.)
			assert.closeTo(along(b, at, { x: 1, y: 0 }), 0, 0.3);
			const side = b.y - at((b.t0 + b.t1) / 2).y;
			// (Going +x, his left is -y: see bodyPoint in evaluate.ts.)
			assert.isTrue(b.foot === 0 ? side > 0 : side < 0, "on its own side");
		}
		// (Each down a third of a stride or less: off the floor between.)
		for (const s of going) {
			assert.isBelow(s.t1 - s.t0, (2 * 1000) / rate / 2);
		}
	});

	test("already running as it starts: in stride from the first moment", () => {
		const at = pathOf({ x: 0, y: 25 }, 3000, () => [15, 0]);
		const steps = run(at, 3000, () => 0);
		const rate = stepsPerSecond("run", 15, INCHES);
		// (A foot down as it starts, under him - not both set and skating.)
		const down = steps.filter((s) => s.t0 <= 0 && s.t1 > 0);
		assert.equal(down.length, 1);
		assert.closeTo(along(down[0]!, at, { x: 1, y: 0 }), 0, 0.3);
		// (The next one on his beat.)
		const first = steps.find((s) => s.t0 > 0)!;
		assert.notEqual(first.foot, down[0]!.foot);
		assert.closeTo(first.t0 - down[0]!.t0, 1000 / rate, 30);
		assert.closeTo(along(first, at, { x: 1, y: 0 }), 0, 0.3);
	});

	test("pulling up: braking steps out in front, then set, feet side by side", () => {
		// 16 ft/s, braking at 18 ft/s2 from 1.5 s, then stood.
		const v = (t: number) => Math.max(0, 16 - Math.max(0, t - 1500) * 0.018);
		const at = pathOf({ x: 0, y: 25 }, 4000, (t) => [v(t), 0]);
		const steps = run(at, 4000, () => 0);
		const stopAt = 1500 + 16 / 0.018;
		// (The last of them set beside the one before.)
		const braking = steps
			.filter((s) => s.t0 >= 1500 && s.t0 <= stopAt + 300)
			.slice(0, -1);
		assert.isAtLeast(braking.length, 2);
		assert.isAtMost(braking.length, 4);
		// (Out in front of him as each comes down - well in front, but no
		// further than he reaches.)
		for (const s of braking) {
			const c = at(s.t0);
			assert.isAbove(s.x - c.x, 0.6);
			assert.isBelow(Math.hypot(s.x - c.x, s.y - c.y), 2.4);
		}
		// Set: both down from soon after he stops, side by side under him.
		const end = at(4000);
		const lastR = steps.findLast((s) => s.foot === 0)!;
		const lastL = steps.findLast((s) => s.foot === 1)!;
		assert.equal(lastR.t1, Infinity);
		assert.equal(lastL.t1, Infinity);
		assert.isBelow(Math.max(lastR.t0, lastL.t0), stopAt + 700);
		assert.closeTo((lastR.x + lastL.x) / 2, end.x, 0.6);
	});

	test("a short burst and stop: a foot left behind steps up, not mirrored out in front", () => {
		// Stood, then 6 ft/s for 0.4 s, then stood again.
		const at = pathOf({ x: 20, y: 25 }, 3000, (t) =>
			t > 1000 && t < 1400 ? [6, 0] : [0, 0],
		);
		const steps = run(at, 3000, () => 0);
		const end = at(3000);
		const R = steps.findLast((s) => s.foot === 0)!;
		const L = steps.findLast((s) => s.foot === 1)!;
		// (Set under him, a short stagger - not one 1.5 ft out in front of
		// him and the other as far behind.)
		assert.isBelow(Math.abs(R.x - L.x), 1.3);
		assert.closeTo((R.x + L.x) / 2, end.x, 0.5);
		for (const s of steps) {
			if (s.t0 > 1000) {
				assert.isBelow(Math.abs(s.x - at(s.t0).x), 1.3, `at ${s.t0}`);
			}
		}
	});

	test("cutting hard: the plant goes down wide, outside the turn", () => {
		// 13 ft/s east, then round to the north over a third of a second.
		const turn = (t: number) =>
			t < 1500
				? 0
				: t > 1850
					? Math.PI / 2
					: ((t - 1500) / 350) * (Math.PI / 2);
		const at = pathOf({ x: 0, y: 25 }, 3000, (t) => [
			13 * Math.cos(turn(t)),
			13 * Math.sin(turn(t)),
		]);
		const steps = run(at, 3000, turn);
		// The one down through the middle of the turn.
		const plant = steps.find((s) => s.t0 <= 1675 && s.t1 >= 1675)!;
		assert.isDefined(plant);
		// (Outside the turn: away from where it turns to - south-east of him.)
		const c = at((plant.t0 + plant.t1) / 2);
		const out = { x: Math.SQRT1_2, y: -Math.SQRT1_2 };
		assert.isAbove((plant.x - c.x) * out.x + (plant.y - c.y) * out.y, 0.6);
	});

	test("sliding: out and close, his feet never crossing", () => {
		// Facing up the floor (+y), sliding along it (+x) at 7 ft/s and back.
		const at = pathOf({ x: 40, y: 25 }, 4000, (t) => [t < 2000 ? 7 : -7, 0]);
		const steps = run(at, 4000, () => Math.PI / 2, true);
		for (let t = 200; t <= 4000; t += 25) {
			const R = steps.findLast((s) => s.foot === 0 && s.t0 <= t);
			const L = steps.findLast((s) => s.foot === 1 && s.t0 <= t);
			if (!R || !L) {
				continue;
			}
			// (Facing +y, his left is +x: see bodyPoint in evaluate.ts.)
			assert.isAbove(L.x - R.x, 0.4, `at ${t}`);
		}
	});

	test("planned a little at a time as the game plays: the same steps", () => {
		const at = pathOf({ x: 10, y: 10 }, 8000, (t) => [
			(t % 3000 < 1500 ? 14 : 3) * Math.cos(t / 800),
			(t % 3000 < 1500 ? 14 : 3) * Math.sin(t / 1100),
		]);
		const yaw = (t: number) => t / 1300;
		const all = footsteps(inOf(at, 8000, yaw));
		const plan = footPlanner(inOf(at, 8000, yaw));
		for (let t = 0; t <= 8000; t += 16) {
			plan.through(t);
		}
		plan.through(8000);
		assert.deepEqual(plan.steps, all);
	});

	test("stood in a stance of his move's own: steps into it, one foot at a time", () => {
		// Stood facing +x; from 0.5 s on, a wide stance (a screen, say): his
		// right foot out to +y, his left to -y (see bodyPoint in evaluate.ts).
		const at = pathOf({ x: 40, y: 25 }, 3000, () => [0, 0]);
		const own = (t: number): [Pt, Pt] | undefined =>
			t < 500
				? undefined
				: [
						{ x: 40.2, y: 26.05 },
						{ x: 39.8, y: 23.95 },
					];
		const steps = footsteps({ ...inOf(at, 3000, () => 0), own });
		for (const foot of [0, 1] as const) {
			const s = steps.findLast((s) => s.foot === foot)!;
			const want = own(3000)![foot];
			assert.isBelow(s.t0, 1400);
			assert.isBelow(Math.hypot(s.x - want.x, s.y - want.y), 0.35);
		}
		// (Never both up at once.)
		for (let t = 0; t <= 3000; t += 10) {
			const down = steps.filter((s) => s.t0 <= t && t <= s.t1);
			assert.isAtLeast(down.length, 1, `at ${t}`);
		}
	});

	test("a jab step: the foot goes out where his move puts it, and back", () => {
		const at = pathOf({ x: 40, y: 25 }, 3000, () => [0, 0]);
		// His triple threat - his jab foot (his right) stabbed out 1.3 ft at
		// his man (+x) over 1000 to 1160 ms, held, and back by 1500.
		const R0 = { x: 40.1, y: 25.7 };
		const L0 = { x: 39.8, y: 24.3 };
		const out = (t: number) =>
			t < 1000
				? 0
				: t < 1160
					? (t - 1000) / 160
					: t < 1300
						? 1
						: t < 1500
							? 1 - (t - 1300) / 200
							: 0;
		const own = (t: number): [Pt, Pt] => [
			{ x: R0.x + 1.3 * out(t), y: R0.y },
			L0,
		];
		const steps = footsteps({ ...inOf(at, 3000, () => 0), own });
		const R = steps.filter((s) => s.foot === 0);
		const jab = R.find((s) => s.t0 > 900 && s.t0 < 1400)!;
		assert.isDefined(jab);
		assert.closeTo(jab.x, R0.x + 1.3, 0.35);
		const back = R.find((s) => s.t0 > jab.t0)!;
		assert.isDefined(back);
		assert.isBelow(back.t0, 1900);
		assert.closeTo(back.x, R0.x, 0.35);
		// (His left stays put through it.)
		const L = steps.filter((s) => s.foot === 1 && s.t0 <= 2000 && s.t1 >= 900);
		assert.equal(L.length, 1);
	});

	test("landing from a jump under way: down on both feet, then on in stride", () => {
		const at = pathOf({ x: 10, y: 25 }, 2000, () => [12, 0]);
		const steps = footsteps({ ...inOf(at, 2000, () => 0), set: true });
		const first = steps.filter((s) => s.t0 === 0);
		assert.equal(first.length, 2);
		// (Then stepping on with him.)
		const after = steps.filter((s) => s.t0 > 0 && s.t0 < 1500);
		assert.isAtLeast(after.length, 3);
	});

	test("a foot is down at one spot at a time, and up a moment between", () => {
		const at = pathOf({ x: 10, y: 10 }, 6000, (t) => [
			8 * Math.cos(t / 700),
			8 * Math.sin(t / 900),
		]);
		const steps = run(at, 6000, (t) => t / 1500);
		for (const foot of [0, 1]) {
			const mine = steps.filter((s) => s.foot === foot);
			for (let i = 1; i < mine.length; i++) {
				assert.isAtLeast(mine[i]!.t0 - mine[i - 1]!.t1, 150);
			}
		}
	});
});
