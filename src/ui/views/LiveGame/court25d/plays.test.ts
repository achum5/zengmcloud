import { assert, describe, test } from "vitest";
import { makeCourtRng } from "../courtRng.ts";
import { LEAGUE_PLAY_TYPES } from "./nbaRates.ts";
import { PLAYBOOK, SPOTS } from "./playbook.ts";
import {
	callShot,
	callTurnover,
	castPlay,
	holderAfter,
	PLAYS,
	walkPlay,
	type Cast,
	type PlayZone,
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

	test("half-court shots are scored the ways the league scores them, as often", () => {
		// The shots the sim's half court hands the court, per thousand (over a
		// season of its games): where from, and off a pass, on his own, or a
		// miss that says neither - by a guard, a wing, a big.
		const SHOTS: Record<
			PlayZone,
			Record<"ast" | "own" | "miss", [number, number, number]>
		> = {
			rim: { ast: [10, 38, 33], own: [7, 23, 17], miss: [16, 48, 34] },
			post: { ast: [13, 23, 19], own: [10, 19, 13], miss: [51, 81, 56] },
			mid: { ast: [20, 19, 6], own: [13, 17, 4], miss: [45, 54, 16] },
			three: { ast: [18, 28, 13], own: [11, 21, 9], miss: [62, 93, 42] },
		};
		const by = [[10, 20], [30], [40, 50]];
		const rng = makeCourtRng("league");
		const made: Record<string, number> = {};
		let k = 0;
		for (const [zone, kinds] of Object.entries(SHOTS)) {
			for (const [kind, roles] of Object.entries(kinds)) {
				roles.forEach((n, r) => {
					for (let j = 0; j < n; j++, k++) {
						const shooter = by[r]![j % by[r]!.length]!;
						const call = callShot(rng, {
							// A third of them early in the clock.
							cats: k % 3 === 0 ? { half: 1, early: 3 } : { half: 1 },
							zone: zone as PlayZone,
							shooter,
							...(kind === "ast" ? { assist: shooter === 10 ? 20 : 10 } : {}),
							unassisted: kind === "own",
							five: FIVE,
						});
						assert.isDefined(call, `${zone} ${kind} ${shooter}`);
						const type = call!.pick.playType;
						made[type] = (made[type] ?? 0) + 1;
					}
				});
			}
		}
		// Against the league's mix of the same play types (NBA.com Synergy).
		const types = [
			"Spotup",
			"PRBallHandler",
			"Isolation",
			"Cut",
			"PRRollMan",
			"Handoff",
			"OffScreen",
			"Postup",
		] as const;
		const n = types.reduce((sum, t) => sum + (made[t] ?? 0), 0);
		const league = types.reduce((sum, t) => sum + LEAGUE_PLAY_TYPES[t], 0);
		for (const t of types) {
			assert.closeTo(
				(made[t] ?? 0) / n,
				LEAGUE_PLAY_TYPES[t] / league,
				0.035,
				t,
			);
		}
	}, 60_000);
});
