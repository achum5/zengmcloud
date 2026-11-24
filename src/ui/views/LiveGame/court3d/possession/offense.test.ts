import { assert, describe, test } from "vitest";
import { bodyOf } from "./body.ts";
import { moveOn, type Seen } from "./defense.ts";
import { type Mover, STEP_S, stood } from "./movement.ts";
import {
	type Attacker,
	attackerOf,
	behindLine,
	decide,
	type Floor,
	mindOf,
} from "./offense.ts";

const rim = { x: 5.25, y: 25 };
const dist = (a: { x: number; y: number }, b: { x: number; y: number }) =>
	Math.hypot(a.x - b.x, a.y - b.y);
const STEP_MS = STEP_S * 1000;
const shooter = (pid: number): Attacker => ({
	...attackerOf(pid, undefined, bodyOf(undefined), false),
	shoots: 0.8,
});
const still = (p: { x: number; y: number }): Seen => ({ ...p, vx: 0, vy: 0 });

// Him stepped on his own for `ms`, the floor as `floorAt` has it each step.
const run = (
	a: Attacker,
	pid: number,
	from: { x: number; y: number },
	ms: number,
	floorAt: (t: number, m: Mover) => Omit<Floor, "t" | "rim" | "team">,
) => {
	const mind = mindOf();
	let m = stood(from);
	const out: (Mover & { t: number })[] = [];
	for (let t = 0; t <= ms; t += STEP_MS) {
		const { goal } = decide(
			m,
			{ t, rim, team: 0, ...floorAt(t, m) },
			a,
			mind,
			pid,
		);
		m = moveOn(m, goal, a, []);
		out.push({ ...m, t });
	}
	return out;
};

describe("3D possession sim: a man off the ball", () => {
	test("works his spot - walking it, not stood on it like a cone - and stays round it, behind the line with the ball near", () => {
		// (On the wing, the ball up top.)
		const set = {
			x: 5.25 + 24.5 * Math.cos(0.8),
			y: 25 + 24.5 * Math.sin(0.8),
		};
		const ball = { x: 32, y: 26 };
		for (const pid of [3, 14, 27, 51]) {
			const a = shooter(pid);
			const steps = run(a, pid, set, 12_000, () => ({
				set,
				ball,
				holder: { ...still(ball), pid: 99 },
				mates: [ball, { x: 6, y: 2 }, { x: 6, y: 48 }, { x: 22, y: 8 }],
			}));
			const stood = steps.filter((q) => Math.hypot(q.vx, q.vy) < 1.5).length;
			assert.isBelow(stood / steps.length, 0.5, `pid ${pid} stood`);
			const far = Math.max(...steps.map((q) => dist(q, set)));
			assert.isBelow(far, 14, `pid ${pid} wandered`);
			// (Behind the line - his middle a step behind it, give or take
			// the last of a stride.)
			const inside = Math.min(...steps.map((q) => behindLine(q, rim)));
			assert.isAbove(inside, -0.3, `pid ${pid} stepped inside`);
		}
	});

	test("never on top of a teammate: with one stood on his spot, he works it well off him", () => {
		const set = { x: 30, y: 25 };
		const mate = { x: 30, y: 26 };
		const steps = run(shooter(5), 5, set, 6000, () => ({
			set,
			ball: { x: 30, y: 4 },
			holder: { ...still({ x: 30, y: 4 }), pid: 99 },
			mates: [mate, { x: 30, y: 4 }],
		}));
		const late = steps.filter((q) => q.t > 3000);
		assert.isAbove(Math.min(...late.map((q) => dist(q, mate))), 7);
	});

	test("out of the lane before the count of three", () => {
		const set = { x: 12, y: 27 };
		const a = attackerOf(9, undefined, bodyOf(undefined), true);
		const steps = run(a, 9, set, 6000, () => ({
			set,
			ball: { x: 30, y: 40 },
			holder: { ...still({ x: 30, y: 40 }), pid: 99 },
			mates: [{ x: 30, y: 40 }],
		}));
		// (In it from the start: out of it, the middle of him, by 3 s.)
		const lane = (q: { x: number; y: number }) =>
			q.x < 19 && Math.abs(q.y - 25) < 8;
		let since = 0;
		let most = 0;
		for (const q of steps) {
			since = lane(q) ? since + STEP_MS : 0;
			most = Math.max(most, since);
		}
		assert.isBelow(most, 3000);
	});

	test("a drive: a man in its way gets out of it; the corner stays on one down the middle and lifts on one down his baseline", () => {
		const drive = (from: { x: number; y: number }) => {
			const u = { x: rim.x - from.x, y: rim.y - from.y };
			const L = Math.hypot(u.x, u.y);
			return (t: number): Seen => {
				const k = Math.min(L - 4, (14 * t) / 1000);
				return {
					x: from.x + (u.x / L) * k,
					y: from.y + (u.y / L) * k,
					vx: k < L - 4 ? (14 * u.x) / L : 0,
					vy: k < L - 4 ? (14 * u.y) / L : 0,
				};
			};
		};
		const floor =
			(set: { x: number; y: number }, D: (t: number) => Seen) =>
			(t: number) => {
				const H = D(t);
				return {
					set,
					ball: H,
					holder: { ...H, pid: 99 },
					drive: t > 300 && t < 1600 ? H : undefined,
					mates: [H],
				};
			};
		// In its way: a big stood at the foot of the lane, the drive coming
		// straight down it.
		const big = attackerOf(11, undefined, bodyOf(undefined), true);
		const inWay = { x: 10, y: 25 };
		const D1 = drive({ x: 30, y: 25 });
		const s1 = run(big, 11, inWay, 1600, floor(inWay, D1));
		const end1 = s1.at(-1)!;
		assert.isAbove(Math.abs(end1.y - 25), 5);
		// The corner, a drive down the middle: he stays.
		const corner = { x: 3, y: 2.6 };
		const s2 = run(shooter(12), 12, corner, 1500, floor(corner, D1));
		assert.isBelow(dist(s2.at(-1)!, corner), 3.5);
		// A drive down his baseline at him: he lifts, up toward the wing.
		const D2 = drive({ x: 24, y: 6 });
		const s3 = run(shooter(13), 13, corner, 1600, floor(corner, D2));
		assert.isAbove(s3.at(-1)!.x - corner.x, 5);
	});

	test("having passed it, he doesn't stand and watch: he cuts or relocates, most of the time", () => {
		let moved = 0;
		let n = 0;
		for (let pid = 1; pid <= 60; pid++) {
			const set = { x: 26, y: 40 };
			const catcher = { x: 30, y: 25 };
			const steps = run(shooter(pid), pid, set, 2000, () => ({
				set,
				ball: catcher,
				holder: { ...still(catcher), pid: 99 },
				mates: [catcher, { x: 6, y: 3 }, { x: 22, y: 8 }],
				passKey: 1000 + pid * 7,
			}));
			n += 1;
			if (dist(steps.at(-1)!, set) > 3) {
				moved += 1;
			}
		}
		// (Real: stays 10% of the time.)
		assert.isAbove(moved / n, 0.75);
	});

	test("with a job of his coming across the floor, he doesn't cut or relocate away from it after his pass", () => {
		const set = { x: 26, y: 40 };
		const job = { x: 26, y: 8 };
		for (let pid = 1; pid <= 30; pid++) {
			const steps = run(shooter(pid), pid, set, 2000, (t) => ({
				set,
				ball: { x: 30, y: 25 },
				holder: { ...still({ x: 30, y: 25 }), pid: 99 },
				mates: [{ x: 30, y: 25 }],
				passKey: 1000 + pid * 7,
				next: { at: job, in: 4500 - t },
			}));
			// (Never down to the rim; no more than a few steps on his spot
			// farther from it than he started.)
			assert.isAbove(
				Math.min(...steps.map((q) => dist(q, rim))),
				15,
				`pid ${pid}`,
			);
			assert.isBelow(
				dist(steps.at(-1)!, job),
				dist(set, job) + 6,
				`pid ${pid}`,
			);
		}
	});

	test("into something of his that pins him (his catch): there on time, however he was working his spot", () => {
		const set = { x: 26, y: 40 };
		const catchAt = { x: 22, y: 44 };
		const due = 2500;
		const a = shooter(21);
		const steps = run(a, 21, set, due, (t) => ({
			set,
			ball: { x: 30, y: 25 },
			holder: { ...still({ x: 30, y: 25 }), pid: 99 },
			mates: [{ x: 30, y: 25 }],
			...(due - t <= 1600
				? {
						handTo: {
							at: catchAt,
							v: { x: 0, y: 0 },
							until: due - t,
							lead: 1600,
							commit: true,
						},
					}
				: {}),
		}));
		assert.isBelow(dist(steps.at(-1)!, catchAt), 0.6);
	});
});
