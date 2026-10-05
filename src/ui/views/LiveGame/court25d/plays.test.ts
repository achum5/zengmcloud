import { assert, describe, test } from "vitest";
import { makeCourtRng } from "../courtRng.ts";
import { PLAYBOOK, SPOTS } from "./playbook.ts";
import {
	callShot,
	callTurnover,
	castPlay,
	holderAfter,
	PLAYS,
	walkPlay,
	type Cast,
	type Role,
} from "./plays.ts";

// A lineup in position order: point guard, two wings, a four and a center.
const FIVE: Cast[] = [
	{ pid: 10, rank: 0 },
	{ pid: 20, rank: 2 },
	{ pid: 30, rank: 4 },
	{ pid: 40, rank: 6 },
	{ pid: 50, rank: 8 },
];

describe("2.5D playbook", () => {
	test("every set reads, and only uses spots the court knows", () => {
		assert.strictEqual(PLAYS.length, PLAYBOOK.length);
		assert.isAbove(PLAYS.length, 100);
		assert.strictEqual(new Set(PLAYS.map((p) => p.id)).size, PLAYS.length);
		for (const p of PLAYS) {
			for (const s of p.start) {
				assert.property(SPOTS, s, p.id);
			}
			assert.isAtLeast(p.steps.length, 1, p.id);
			assert.isAtLeast(p.options.length, 1, p.id);
		}
	});

	test("the ball only moves from the man who has it", () => {
		for (const p of PLAYS) {
			let holder: Role = p.ball;
			p.steps.forEach((step, k) => {
				for (const a of step) {
					if (
						a.type === "pass" ||
						a.type === "dribble" ||
						a.type === "handoff"
					) {
						assert.strictEqual(a.who, holder, `${p.id} step ${k}`);
					}
					holder = holderAfter(a, holder);
				}
			});
		}
	});

	test("every option has its shooter on his spot, with the ball or about to get it", () => {
		for (const p of PLAYS) {
			for (const o of p.options) {
				const w = walkPlay(p, o.after + 1, o.branch);
				assert.strictEqual(w.at[o.shooter], o.at, `${p.id} ${o.kind}`);
				if (o.assist === undefined) {
					assert.strictEqual(w.holder, o.shooter, `${p.id} ${o.kind}`);
				} else {
					assert.include([o.assist, o.shooter], w.holder, `${p.id} ${o.kind}`);
				}
			}
		}
	});

	test("the five are cast by what each role asks", () => {
		const pnr = PLAYS.find((p) => p.id === "high-pnr-spread")!;
		const cast = castPlay(pnr, FIVE, new Map())!;
		// The point guard brings it, the center sets the screen and rolls.
		assert.strictEqual(cast.roles[0], 10);
		assert.strictEqual(cast.roles[4], 50);
		// Pinned: the center has to shoot from the 1's spot.
		const odd = castPlay(pnr, FIVE, new Map([[50, 0 as Role]]))!;
		assert.strictEqual(odd.roles[0], 50);
		assert.isAbove(odd.cost, cast.cost);
	});

	test("a shot call puts the real shooter and passer in the roles that shoot and pass", () => {
		const rng = makeCourtRng("plays");
		for (const zone of ["rim", "post", "mid", "three"] as const) {
			for (const shooter of FIVE) {
				for (const passer of [undefined, ...FIVE]) {
					if (passer?.pid === shooter.pid) {
						continue;
					}
					const call = callShot(rng, {
						cats: { half: 1 },
						zone,
						shooter: shooter.pid,
						assist: passer?.pid,
						unassisted: passer === undefined,
						five: FIVE,
					});
					assert.isDefined(call, `${zone} ${shooter.pid} ${passer?.pid}`);
					const o = call!.pick;
					assert.strictEqual(o.zone, zone);
					assert.strictEqual(call!.roles[o.shooter], shooter.pid);
					if (passer === undefined) {
						assert.isUndefined(o.assist);
					} else {
						assert.strictEqual(call!.roles[o.assist!], passer.pid);
					}
					assert.sameMembers(
						call!.roles,
						FIVE.map((c) => c.pid),
					);
				}
			}
		}
	});

	test("a break starts from whoever has the ball", () => {
		const rng = makeCourtRng("break");
		for (const holder of FIVE) {
			const call = callShot(rng, {
				cats: { break: 1 },
				zone: "rim",
				shooter: FIVE[1]!.pid,
				holder: holder.pid,
				five: FIVE,
			});
			if (call) {
				assert.strictEqual(call.roles[call.play.ball], holder.pid);
			}
		}
	});

	test("a turnover call puts the man who loses it where the set goes wrong", () => {
		const rng = makeCourtRng("tov");
		for (const victim of FIVE) {
			const call = callTurnover(rng, {
				cats: { half: 1 },
				victim: victim.pid,
				kinds: { pass: 1, lost: 1 },
				five: FIVE,
			});
			assert.isDefined(call);
			assert.strictEqual(call!.roles[call!.pick.who], victim.pid);
			assert.include(["pass", "lost"], call!.pick.kind);
		}
	});

	test("the same moment calls the same set", () => {
		const call = (seed: string) =>
			callShot(makeCourtRng(seed), {
				cats: { half: 1, early: 0.5 },
				zone: "three",
				shooter: 20,
				assist: 10,
				five: FIVE,
			});
		assert.deepStrictEqual(call("same"), call("same"));
	});
});
