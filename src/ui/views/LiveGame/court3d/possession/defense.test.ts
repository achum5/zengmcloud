import { assert, describe, test } from "vitest";
import { bodyOf } from "./body.ts";
import {
	commitLead,
	cushionOf,
	decide,
	defenderOf,
	guardSpot,
	moveOn,
	threatOf,
} from "./defense.ts";
import { stood } from "./movement.ts";

const rim = { x: 5.25, y: 25 };
const mid = threatOf(undefined);
const d = defenderOf(7, undefined, bodyOf(undefined));
const dist = (a: { x: number; y: number }, b: { x: number; y: number }) =>
	Math.hypot(a.x - b.x, a.y - b.y);

describe("3D possession sim: a defender's spot", () => {
	test("off the ball he sits where real NBA defenders do: sunk toward the rim and over to the ball, the more the farther the ball", () => {
		// (A man on the wing, 22 ft out.)
		const man = { x: 5.25 + 22 * Math.cos(0.9), y: 25 + 22 * Math.sin(0.9) };
		const near = guardSpot(
			man,
			rim,
			{ x: man.x + 12, y: man.y - 2 },
			false,
			mid,
			d,
		);
		const mid20 = guardSpot(
			man,
			rim,
			{ x: man.x + 15, y: man.y - 13 },
			false,
			mid,
			d,
		);
		const far = guardSpot(
			man,
			rim,
			{ x: man.x + 10, y: man.y - 38 },
			false,
			mid,
			d,
		);
		// (Real medians: 5.5 ft off him within 15 ft of the ball, 7.5 at
		// 15-25, 10.9 past 35.)
		assert.closeTo(dist(near, man), 5.5, 1);
		assert.closeTo(dist(mid20, man), 7.5, 1);
		assert.closeTo(dist(far, man), 10.5, 1.2);
		// (Between him and the rim: nearer it than his man is.)
		for (const p of [near, mid20, far]) {
			assert.isBelow(dist(p, rim), dist(man, rim));
		}
	});

	test("off a man who can't shoot he sinks farther; on a shooter he stays closer", () => {
		const man = { x: 27, y: 25 };
		const ball = { x: 30, y: 2 };
		const poor = guardSpot(
			man,
			rim,
			ball,
			false,
			{ ...mid, three: 0, shot: 0 },
			d,
		);
		const great = guardSpot(
			man,
			rim,
			ball,
			false,
			{ ...mid, three: 1, shot: 1 },
			d,
		);
		assert.isAbove(dist(poor, man), dist(great, man) + 2);
	});

	test("on the ball, between his man and the rim, a step and a half off at the arc", () => {
		const man = { x: 29, y: 25 };
		const p = guardSpot(man, rim, man, true, mid, d);
		assert.closeTo(p.y, 25, 1e-9);
		assert.isBelow(p.x, man.x);
		assert.closeTo(dist(p, man), 5.3, 0.6);
		// (Tighter on a shooter, looser on a man who gets by people.)
		assert.isBelow(
			cushionOf(24, { ...mid, three: 1 }, d),
			cushionOf(24, mid, d) - 0.8,
		);
		assert.isAbove(
			cushionOf(24, { ...mid, drive: 1 }, d),
			cushionOf(24, mid, d) + 0.3,
		);
		// (Up close in the paint, out farther picking him up.)
		assert.isBelow(cushionOf(8, mid, d), 3.5);
		assert.isAbove(cushionOf(45, mid, d), 8);
	});
});

describe("3D possession sim: a defender going about it", () => {
	test("sliding, he goes no faster than his slide - and faces the ball", () => {
		const m = stood({ x: 21, y: 22 });
		const { goal, anim } = decide(
			m,
			{
				man: { x: 26, y: 20, vx: 0, vy: -6 },
				on: true,
				ball: { x: 26, y: 20 },
				rim,
				threat: mid,
			},
			d,
		);
		assert.isDefined(goal.face);
		assert.isAtMost(goal.most ?? Infinity, d.slide);
		assert.include(["slide", "shuffle"], anim);
	});

	test("beaten on the drive, he turns and runs with him", () => {
		const m = { ...stood({ x: 24, y: 25 }), vx: -9, vy: 0 };
		const { goal, anim } = decide(
			m,
			{
				// (His man by him on the way to the rim, going hard.)
				man: { x: 22, y: 27, vx: -14, vy: 0 },
				on: true,
				ball: { x: 22, y: 27 },
				rim,
				threat: mid,
			},
			d,
		);
		assert.isUndefined(goal.most);
		assert.isUndefined(goal.face);
		assert.strictEqual(goal.effort, 1);
		assert.include(["run", "shuffle", "slide"], anim);
	});

	test("never through anybody: he steps aside", () => {
		let m = stood({ x: 20, y: 25.3 });
		const other = { x: 15, y: 25 };
		let nearest = Infinity;
		for (let k = 0; k < 60; k++) {
			const was = m;
			m = moveOn(m, { to: { x: 10, y: 25 }, effort: 1 }, d, [other]);
			nearest = Math.min(nearest, dist(m, other));
			// (Never shoved over faster than a man steps.)
			assert.isAtMost(dist(m, was) / 0.05, d.body.top + 8 + 1e-6);
		}
		assert.isAbove(nearest, 1.5);
	});

	test("a man running through: he steps aside, not backed up the way he goes", () => {
		let m = stood({ x: 20, y: 25 });
		let o = { x: 26, y: 25.2, vx: -15, vy: 0 };
		let nearest = Infinity;
		for (let k = 0; k < 20; k++) {
			m = moveOn(m, { to: { x: 20, y: 25 }, effort: 1 }, d, [o]);
			o = { ...o, x: o.x + o.vx * 0.05 };
			nearest = Math.min(nearest, dist(m, o));
		}
		assert.isAbove(m.x, 18.5);
		assert.isAbove(nearest, 1.2);
	});

	test("going the other way, he sets off sooner for something of his", () => {
		const to = { x: 30, y: 25 };
		const toward = { ...stood({ x: 20, y: 25 }), vx: 8, vy: 0 };
		const still = stood({ x: 20, y: 25 });
		const away = { ...stood({ x: 20, y: 25 }), vx: -8, vy: 0 };
		assert.isBelow(commitLead(toward, to, d), commitLead(still, to, d));
		assert.isBelow(commitLead(still, to, d) + 300, commitLead(away, to, d));
		// (From a stand, 10 ft takes about a second, and a moment to spare.)
		assert.closeTo(commitLead(still, to, d), 1900, 300);
	});

	test("each man his own, the same every time", () => {
		const a = defenderOf(31, { diq: 70, spd: 60 }, bodyOf({ spd: 60 }));
		const b = defenderOf(31, { diq: 70, spd: 60 }, bodyOf({ spd: 60 }));
		assert.deepEqual(a, b);
		// (The sharper his head, the quicker he answers - and the more he
		// reads ahead.)
		const dull = defenderOf(31, { diq: 35 }, bodyOf(undefined));
		assert.isBelow(a.react, dull.react - 80);
		assert.isAbove(a.lead, dull.lead);
	});
});
