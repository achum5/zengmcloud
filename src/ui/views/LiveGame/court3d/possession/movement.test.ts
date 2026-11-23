import { assert, describe, test } from "vitest";
import { type Body, bodyOf } from "./body.ts";
import { type Goal, type Mover, STEP_S, step, stood } from "./movement.ts";

const GUARD = bodyOf({ spd: 80, hgt: 30 });
const BIG = bodyOf({ spd: 40, hgt: 85 });
const pace = (m: Mover) => Math.hypot(m.vx, m.vy);

const run = (
	m0: Mover,
	goal: (t: number, m: Mover) => Goal,
	b: Body,
	seconds: number,
): (Mover & { t: number })[] => {
	let m = m0;
	const out = [{ t: 0, ...m }];
	for (let k = 1; k * STEP_S <= seconds + 1e-9; k++) {
		m = step(m, goal(k * STEP_S, m), b);
		out.push({ t: k * STEP_S, ...m });
	}
	return out;
};
const goingAt = (v: number): Mover => ({
	x: 0,
	y: 0,
	vx: v,
	vy: 0,
	ax: 0,
	ay: 0,
	yaw: 0,
});
const far = (deg: number) => ({
	x: 200 * Math.cos((deg * Math.PI) / 180),
	y: 200 * Math.sin((deg * Math.PI) / 180),
});

describe("3D possession sim: how a man gets about the floor", () => {
	// Real NBA tracking: at their quickest, guards are at 12 ft/s in 0.8-0.9
	// s from a standstill, bigs in 1.3-1.5 (tools/court3d-tape/nba).
	test("a quick guard gets going in under a second; a slow big takes half as long again", () => {
		const t12 = (b: Body) =>
			run(stood({ x: 0, y: 0 }), () => ({ to: far(0), effort: 1 }), b, 3).find(
				(q) => pace(q) >= 12,
			)!.t;
		const g = t12(GUARD);
		const b = t12(BIG);
		assert.isAtLeast(g, 0.75);
		assert.isAtMost(g, 1.0);
		assert.isAtLeast(b, 1.25);
		assert.isAtMost(b, 1.6);
	});

	test("ratings set where he is in an NBA player's range, never outside it", () => {
		for (const r of [{}, { spd: 0, hgt: 100 }, { spd: 100, hgt: 0 }]) {
			const b = bodyOf(r);
			assert.isAtLeast(b.top, 17.5);
			assert.isAtMost(b.top, 21.7);
			assert.isAtLeast(b.go, 13.9);
			assert.isAtMost(b.go, 21.1);
		}
		// (Worn down: a little less of each.)
		assert.isBelow(bodyOf({ spd: 60 }, 1).top, bodyOf({ spd: 60 }).top);
		assert.isAbove(bodyOf({ spd: 60 }, 1).top, bodyOf({ spd: 60 }).top * 0.9);
	});

	test("no man goes faster, pushes harder or jolts quicker than his body lets him", () => {
		for (const b of [GUARD, BIG]) {
			// (Chasing a spot that jumps about the floor every second.)
			let seed = 7;
			const rnd = () => {
				seed = (seed * 16807) % 2147483647;
				return seed / 2147483647;
			};
			let to = { x: 40, y: 25 };
			let effort = 1;
			const tr = run(
				stood({ x: 47, y: 25 }),
				(t) => {
					if (Math.abs(t - Math.round(t)) < 1e-6) {
						to = { x: rnd() * 94, y: rnd() * 50 };
						effort = rnd();
					}
					return { to, effort };
				},
				b,
				60,
			);
			const most = Math.max(b.go, b.brake, b.side) + 1e-6;
			for (let k = 1; k < tr.length; k++) {
				const a = tr[k]!;
				const p = tr[k - 1]!;
				assert.isAtMost(pace(a), b.top + 1e-6);
				assert.isAtMost(Math.hypot(a.ax, a.ay), most);
				assert.isAtMost(
					Math.hypot(a.ax - p.ax, a.ay - p.ay),
					b.jerk * STEP_S + 1e-6,
				);
			}
		}
	});

	test("he pulls up on his spot and stands there, not past it", () => {
		for (const effort of [1, 0.5, 0.2]) {
			const tr = run(
				goingAt(12),
				() => ({ to: { x: 14, y: 0 }, effort }),
				GUARD,
				5,
			);
			assert.isAtMost(Math.max(...tr.map((q) => q.x)), 14.05);
			const last = tr.at(-1)!;
			assert.strictEqual(pace(last), 0);
			assert.closeTo(last.x, 14, 1e-9);
		}
	});

	// Real men go through a cut of over 90 degrees at about 5 ft/s (median),
	// rounding it - not stopped dead, not at full tilt.
	test("the sharper the cut, the slower he goes through it", () => {
		const slowest = (deg: number) =>
			Math.min(
				...run(
					goingAt(12),
					() => ({ to: far(deg), effort: 1 }),
					GUARD,
					2.5,
				).map(pace),
			);
		const s60 = slowest(60);
		const s90 = slowest(90);
		const s120 = slowest(120);
		const s150 = slowest(150);
		assert.isAbove(s60, 10);
		assert.isAbove(s60, s90);
		assert.isAbove(s90, s120);
		assert.isAbove(s120, s150);
		assert.isAtLeast(s120, 3);
		assert.isAtMost(s120, 7);
		assert.isAbove(s150, 1);
	});

	test("he goes with a moving man, and stays with him", () => {
		const man = (t: number) => ({ x: 10 + 10 * t, y: 20 });
		const tr = run(
			stood({ x: 7, y: 20 }),
			(t) => ({ to: man(t), toV: { x: 10, y: 0 }, effort: 0.9 }),
			GUARD,
			3,
		);
		for (const q of tr.filter((x) => x.t >= 2)) {
			const m = man(q.t);
			assert.isBelow(Math.hypot(q.x - m.x, q.y - m.y), 0.75);
		}
	});

	test("facing: the way he goes, or where he is told, turned no quicker than a man turns", () => {
		const tr = run(
			goingAt(0),
			() => ({ to: { x: 0, y: 0 }, effort: 0, face: Math.PI }),
			GUARD,
			1,
		);
		for (let k = 1; k < tr.length; k++) {
			const d = Math.abs(tr[k]!.yaw - tr[k - 1]!.yaw);
			assert.isAtMost(Math.min(d, 2 * Math.PI - d), GUARD.spin * STEP_S + 1e-9);
		}
		assert.closeTo(Math.abs(tr.at(-1)!.yaw), Math.PI, 1e-9);
		const going = run(
			stood({ x: 0, y: 0 }),
			() => ({ to: far(90), effort: 1 }),
			GUARD,
			1.5,
		);
		assert.closeTo(going.at(-1)!.yaw, Math.PI / 2, 0.05);
	});

	test("the same step from the same place goes the same way, every time", () => {
		const a = run(goingAt(8), () => ({ to: far(130), effort: 0.8 }), BIG, 3);
		const b = run(goingAt(8), () => ({ to: far(130), effort: 0.8 }), BIG, 3);
		assert.deepEqual(a, b);
	});
});
