import { assert, describe, test } from "vitest";
import {
	simulateFutures,
	simulatePlayoffBracket,
	type BracketMatchup,
	type FuturesTeam,
} from "./sportsbookFutures.ts";

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

// 3 playoff rounds → 8 playoff teams → every team in this league qualifies,
// so even the 3rd-best team has a bracket path.
const run = (seed = 42) =>
	simulateFutures({
		teams: LEAGUE,
		numGamesPlayoffSeries: [7, 7, 7],
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

	test("a solid 3rd-best team keeps real title equity (never 99-1 territory)", () => {
		const r = run();
		// tid 2 is the clear 3rd-best team. Rating uncertainty must leave it with
		// genuine long-shot equity, not a probability that collapses to ~0.
		const p = r.titleProb.get(2)!;
		assert.ok(p >= 0.002, `3rd-best title prob ${p}`);
		// But still clearly behind the top two.
		assert.ok(p < r.titleProb.get(1)!);
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

describe("simulatePlayoffBracket", () => {
	// Conference semifinals of a 2-round bracket: 4 teams alive; tids 90/91 were
	// eliminated in the (already finished) earlier rounds and appear nowhere.
	const semis = (): BracketMatchup[] => [
		// East: 0 leads 1, three games to none.
		{
			home: { tid: 0, cid: 0, won: 3 },
			away: { tid: 1, cid: 0, won: 0 },
		},
		// West: even series between evenly rated teams.
		{
			home: { tid: 4, cid: 1, won: 1 },
			away: { tid: 5, cid: 1, won: 1 },
		},
	];
	const ratings = new Map<number, number>([
		[0, 5],
		[1, 5],
		[4, 3],
		[5, 3],
	]);
	const run2 = (matchups = semis(), seed = 11) =>
		simulatePlayoffBracket({
			matchups,
			startRound: 1,
			numGamesPlayoffSeries: [7, 7, 7],
			ratings,
			iterations: 3000,
			seed,
		});

	test("an eliminated team is not in the market at all", () => {
		const r = run2();
		assert.strictEqual(r.titleProb.has(90), false);
		assert.strictEqual(r.confProb.has(91), false);
		// Only the 4 alive teams are priced.
		assert.strictEqual(r.titleProb.size, 4);
	});

	test("a 3-0 series lead prices as a massive favorite over an equal team", () => {
		const r = run2();
		// Equal ratings, but tid 0 needs 1 more win before tid 1 gets 4. Rating
		// uncertainty keeps it from being a lock (the opponent might genuinely be
		// better than rated), so ~90%, not ~95%.
		assert.ok(
			r.confProb.get(0)! > 0.85,
			`3-0 leader conf prob ${r.confProb.get(0)}`,
		);
		assert.ok(
			r.confProb.get(0)! > 5 * r.confProb.get(1)!,
			"and dwarfs the trailer",
		);
		assert.ok(r.titleProb.get(0)! > 2 * r.titleProb.get(4)!);
	});

	test("title probabilities sum to 1; finalists (conf) sum to 2", () => {
		const r = run2();
		const sum = (m: Map<number, number>) =>
			[...m.values()].reduce((s, p) => s + p, 0);
		assert.ok(Math.abs(sum(r.titleProb) - 1) < 1e-9);
		assert.ok(Math.abs(sum(r.confProb) - 2) < 1e-9);
	});

	test("an already-decided series always advances its winner", () => {
		const decided: BracketMatchup[] = [
			{
				home: { tid: 0, cid: 0, won: 4 },
				away: { tid: 1, cid: 0, won: 2 },
			},
			{
				home: { tid: 4, cid: 1, won: 2 },
				away: { tid: 5, cid: 1, won: 4 },
			},
		];
		const r = run2(decided);
		assert.strictEqual(r.titleProb.get(1), 0);
		assert.strictEqual(r.titleProb.get(4), 0);
		assert.ok(Math.abs(r.titleProb.get(0)! + r.titleProb.get(5)! - 1) < 1e-9);
	});

	test("a decided FINAL makes the champion a certainty", () => {
		const final: BracketMatchup[] = [
			{
				home: { tid: 0, cid: 0, won: 4 },
				away: { tid: 4, cid: 1, won: 1 },
			},
		];
		const r = simulatePlayoffBracket({
			matchups: final,
			startRound: 2,
			numGamesPlayoffSeries: [7, 7, 7],
			ratings,
			iterations: 500,
			seed: 3,
		});
		assert.strictEqual(r.titleProb.get(0), 1);
		assert.strictEqual(r.titleProb.get(4), 0);
		// Both finalists "won their conference".
		assert.strictEqual(r.confProb.get(0), 1);
		assert.strictEqual(r.confProb.get(4), 1);
	});

	test("a bye advances automatically", () => {
		const withBye: BracketMatchup[] = [
			{ home: { tid: 0, cid: 0, won: 0 } }, // bye
			{
				home: { tid: 4, cid: 1, won: 0 },
				away: { tid: 5, cid: 1, won: 0 },
			},
		];
		const r = simulatePlayoffBracket({
			matchups: withBye,
			startRound: 1,
			numGamesPlayoffSeries: [7, 7, 7],
			ratings,
			iterations: 1000,
			seed: 5,
		});
		// tid 0 always reaches the final.
		assert.strictEqual(r.confProb.get(0), 1);
		assert.ok(r.titleProb.get(0)! > 0.5); // higher rated, plus rested
	});

	test("deterministic per seed", () => {
		const a = run2(semis(), 21);
		const b = run2(semis(), 21);
		assert.strictEqual(a.titleProb.get(4), b.titleProb.get(4));
	});
});
