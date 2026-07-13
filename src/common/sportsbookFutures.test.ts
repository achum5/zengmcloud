import { assert, describe, test } from "vitest";
import { simulateFutures, type FuturesTeam } from "./sportsbookFutures.ts";

// A 2-conference, 8-team league: STL is a 46-3-style juggernaut in the East.
const team = (
	tid: number,
	cid: number,
	did: number,
	won: number,
	gamesRemaining: number,
	rating: number,
): FuturesTeam => ({ tid, cid, did, won, gamesRemaining, rating });

const LEAGUE: FuturesTeam[] = [
	team(0, 0, 0, 46, 33, 20), // juggernaut
	team(1, 0, 0, 41, 33, 14), // strong rival, same conference AND division
	team(2, 0, 1, 33, 33, 4),
	team(3, 0, 1, 20, 33, -8),
	team(4, 1, 2, 32, 33, 3),
	team(5, 1, 2, 30, 33, 1),
	team(6, 1, 3, 25, 33, -4),
	team(7, 1, 3, 15, 33, -12),
];

const run = (seed = 42) =>
	simulateFutures({
		teams: LEAGUE,
		numGamesPlayoffSeries: [7, 7],
		iterations: 3000,
		seed,
	});

describe("simulateFutures", () => {
	test("a juggernaut prices like one (clear title favorite, >50%)", () => {
		const r = run();
		const p = r.titleProb.get(0)!;
		assert.ok(p > 0.5, `juggernaut title prob ${p}`);
		// And clearly ahead of the strong rival.
		assert.ok(p > 2 * r.titleProb.get(1)!);
	});

	test("title probability never exceeds conference probability", () => {
		const r = run();
		for (const t of LEAGUE) {
			const title = r.titleProb.get(t.tid)!;
			const conf = r.confProb.get(t.tid)!;
			assert.ok(
				title <= conf + 1e-9,
				`tid ${t.tid}: title ${title} > conf ${conf}`,
			);
		}
	});

	test("each market's probabilities are coherent (sum to 1 per pool)", () => {
		const r = run();
		const sum = (m: Map<number, number>, tids: number[]) =>
			tids.reduce((s, tid) => s + (m.get(tid) ?? 0), 0);
		assert.ok(Math.abs(sum(r.titleProb, [0, 1, 2, 3, 4, 5, 6, 7]) - 1) < 1e-9);
		assert.ok(Math.abs(sum(r.confProb, [0, 1, 2, 3]) - 1) < 1e-9); // East
		assert.ok(Math.abs(sum(r.confProb, [4, 5, 6, 7]) - 1) < 1e-9); // West
		assert.ok(Math.abs(sum(r.divProb, [0, 1]) - 1) < 1e-9); // shared division
	});

	test("win-total lines sit near the projection and price near fair", () => {
		const r = run();
		const wt = r.winTotals.get(0)!;
		// ~46 + 33*0.94 ≈ 77 wins projected.
		assert.ok(wt.line > 70 && wt.line < 82, `line ${wt.line}`);
		assert.ok(wt.line % 1 !== 0, "line must be a half point");
		assert.ok(wt.pOver > 0.3 && wt.pOver < 0.7, `pOver ${wt.pOver}`);
	});

	test("deterministic for a given seed, different for another", () => {
		const a = run(7);
		const b = run(7);
		const c = run(8);
		assert.strictEqual(a.titleProb.get(0), b.titleProb.get(0));
		assert.notStrictEqual(a.titleProb.get(0), c.titleProb.get(0));
	});

	test("a weak team is a long shot but never a guaranteed zero-cost market", () => {
		const r = run();
		// The worst team can still theoretically run the table; its price just
		// gets floored by the odds clamp downstream. Probability itself may be 0
		// in a finite simulation - that's fine, pricing clamps at +9900.
		assert.ok(r.titleProb.get(7)! < 0.02);
	});
});
