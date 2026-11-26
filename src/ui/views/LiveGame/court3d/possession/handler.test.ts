import { assert, describe, test } from "vitest";
import { bodyOf } from "./body.ts";
import { moveOn, type Seen } from "./defense.ts";
import { decide, handMindOf, type Handling, LEASH } from "./handler.ts";
import { type Mover, STEP_S, stood } from "./movement.ts";
import { type Attacker, attackerOf } from "./offense.ts";

const rim = { x: 5.25, y: 25 };
const dist = (a: { x: number; y: number }, b: { x: number; y: number }) =>
	Math.hypot(a.x - b.x, a.y - b.y);
const STEP_MS = STEP_S * 1000;
const guard = (pid: number, handle = 0.5, big = false): Attacker => ({
	...attackerOf(pid, undefined, bodyOf(undefined), big),
	handle,
	quick: handle,
});

// Him with the ball, stepped on his own for `ms`, the floor as `floorAt`
// has it each step.
const run = (
	a: Attacker,
	pid: number,
	from: { x: number; y: number },
	ms: number,
	floorAt: (t: number, m: Mover) => Omit<Handling, "t" | "rim">,
	start?: Partial<Mover>,
) => {
	const mind = handMindOf();
	let m: Mover = { ...stood(from), ...start };
	const out: (Mover & { t: number })[] = [];
	for (let t = 0; t <= ms; t += STEP_MS) {
		const f = floorAt(t, m);
		const { goal } = decide(m, { t, rim, ...f }, a, mind, pid);
		m = moveOn(m, goal, a, [], !f.dribbling);
		out.push({ ...m, t });
	}
	return out;
};
// His man between him and the rim, this far off him.
const manOff = (m: Mover, gap: number): Seen => {
	const L = dist(m, rim);
	return {
		x: m.x + ((rim.x - m.x) / L) * gap,
		y: m.y + ((rim.y - m.y) / L) * gap,
		vx: 0,
		vy: 0,
	};
};

describe("3D possession sim: the man with the ball", () => {
	test("on the dribble, nothing of the play's on: he works his man - on the move most of the time, never more than a few steps off the play's spot", () => {
		const set = { x: 30, y: 31 };
		for (const pid of [2, 17, 33, 48]) {
			const steps = run(guard(pid), pid, set, 12_000, (_t, m) => ({
				set,
				dribbling: true,
				man: manOff(m, 4),
				mates: [
					{ x: 8, y: 3 },
					{ x: 8, y: 47 },
					{ x: 22, y: 6 },
					{ x: 14, y: 30 },
				],
			}));
			const still = steps.filter((q) => Math.hypot(q.vx, q.vy) < 1.5).length;
			assert.isBelow(still / steps.length, 0.35, `pid ${pid} stood`);
			const far = Math.max(...steps.map((q) => dist(q, set)));
			assert.isBelow(far, LEASH + 1, `pid ${pid} wandered`);
			// (Not every step the same way: in at his man some of it.)
			const nearest = Math.min(...steps.map((q) => dist(q, rim)));
			assert.isBelow(nearest, dist(set, rim) - 2, `pid ${pid} never attacked`);
		}
	});

	test("with it in his hands, he gathers in a stride or two and his feet stay put - no walking with it", () => {
		const set = { x: 28, y: 25 };
		// (Coming in at a jog as he picks it up.)
		const steps = run(
			guard(7),
			7,
			set,
			2500,
			() => ({ set, dribbling: false, mates: [] }),
			{ vx: -9, vy: 0 },
		);
		const stop = steps.findIndex((q) => Math.hypot(q.vx, q.vy) < 0.3);
		assert.isAbove(stop, 0);
		assert.isBelow(dist(steps[stop]!, set), 4);
		const after = steps.slice(stop);
		assert.isBelow(Math.max(...after.map((q) => dist(q, steps[stop]!))), 0.3);
	});

	test("a pass he caught and holds on to: eased to a stop over most of a second, not pulled up on the spot", () => {
		const set = { x: 28, y: 25 };
		// (Caught at 4.4 ft/s, real men's median.)
		const steps = run(
			guard(7),
			7,
			set,
			2000,
			(t) => ({ set, dribbling: false, mates: [], caught: t }),
			{ vx: -4.4, vy: 0 },
		);
		const at = (ms: number) => {
			const q = steps.find((s) => s.t >= ms)!;
			return Math.hypot(q.vx, q.vy);
		};
		// (Real: 2.7 ft/s 0.4 s on, 1.7 at 0.8 s.)
		assert.isAbove(at(400), 1.8);
		assert.isAbove(at(800), 0.3);
		assert.isBelow(at(1400), 0.3);
	});

	test("straight off a catch, on the dribble, he goes somewhere with it - not stood sizing his man up", () => {
		const set = { x: 28, y: 31 };
		let ft = 0;
		const pids = [2, 5, 11, 17, 23, 31, 40, 48];
		for (const pid of pids) {
			const steps = run(guard(pid), pid, set, 1000, (t, m) => ({
				set,
				dribbling: true,
				man: manOff(m, 5),
				mates: [],
				caught: 200 + t,
			}));
			ft += dist(steps.at(-1)!, set);
		}
		// (Real, a catch put on the floor: 7.1 ft in its first second.)
		assert.isAbove(ft / pids.length, 3);
	});

	test("the play taking him back with nothing of his first: about on its spot by then, wherever he was working it", () => {
		const set = { x: 30, y: 25 };
		const end = 2500;
		const at = { x: 31, y: 28 };
		for (const pid of [3, 9, 21, 30]) {
			// (Going away from it at a dribbling pace to start with.)
			const steps = run(
				guard(pid, 0.9),
				pid,
				set,
				end,
				(t, m) => ({
					set,
					dribbling: true,
					man: manOff(m, 6),
					mates: [],
					home: { at, in: end - t, v: { x: 0, y: 0 }, end: true },
				}),
				{ vx: 0, vy: -5 },
			);
			assert.isBelow(dist(steps.at(-1)!, at), 1, `pid ${pid}`);
		}
	});

	test("crowded, he gives ground; given room, he attacks it", () => {
		const set = { x: 30, y: 25 };
		const mean = (crowd: boolean) => {
			let inAt = 0;
			for (let pid = 1; pid <= 20; pid++) {
				const steps = run(guard(pid, 0.8), pid, set, 3000, (_t, m) => ({
					set,
					dribbling: true,
					man: manOff(m, crowd ? 2.5 : 9),
					mates: [],
				}));
				inAt += dist(set, rim) - Math.min(...steps.map((q) => dist(q, rim)));
			}
			return inAt / 20;
		};
		assert.isAbove(mean(false), mean(true) + 1);
	});

	test("on the play's spot as he picks it up for his shot, wherever he was working it", () => {
		const set = { x: 30, y: 25 };
		const shotAt = { x: 29, y: 27 };
		const up = 4000;
		for (const pid of [3, 9, 21]) {
			const steps = run(guard(pid, 0.9), pid, set, up, (t, m) => ({
				set,
				dribbling: true,
				man: manOff(m, 6),
				mates: [],
				home: { at: shotAt, in: up - t, v: { x: 0, y: 0 } },
			}));
			assert.isBelow(dist(steps.at(-1)!, shotAt), 0.75, `pid ${pid}`);
		}
	});

	test("the play taking him into a pass on the move, he picks it up still going with it", () => {
		const set = { x: 30, y: 25 };
		const up = 4000;
		// (The play has him going 5 ft/s across the floor as he picks it up.)
		const v = { x: 0, y: 5 };
		const at = { x: 29, y: 28 };
		for (const pid of [3, 9, 21]) {
			const steps = run(guard(pid, 0.9), pid, set, up, (t, m) => ({
				set,
				dribbling: true,
				man: manOff(m, 6),
				mates: [],
				home: { at, in: up - t, v },
			}));
			const last = steps.at(-1)!;
			assert.isBelow(dist(last, at), 1.5, `pid ${pid}`);
			assert.isAbove(last.vy, 3, `pid ${pid}`);
		}
	});

	test("a big works it less than a guard", () => {
		const set = { x: 30, y: 25 };
		const ground = (big: boolean) => {
			let ft = 0;
			for (let pid = 1; pid <= 12; pid++) {
				const steps = run(
					guard(pid, big ? 0.15 : 0.85, big),
					pid,
					set,
					6000,
					(_t, m) => ({
						set,
						dribbling: true,
						man: manOff(m, 4),
						mates: [],
					}),
				);
				for (let k = 1; k < steps.length; k++) {
					ft += dist(steps[k]!, steps[k - 1]!);
				}
			}
			return ft;
		};
		assert.isBelow(ground(true), ground(false));
	});
});
