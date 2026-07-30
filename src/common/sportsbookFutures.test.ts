import { assert, describe, test } from "vitest";
import { softCapMargin } from "./sportsbookOdds.ts";
import {
	bracketMarketsOpen,
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

// The bug this exists for: the Conference Winner market stayed on the board
// through the Finals, by which point both conference champions are known and
// `confProb` (P reaches the final series) is 1.0 for each finalist - so either
// one was a guaranteed payout on a publicly known result.
describe("bracketMarketsOpen", () => {
	const confFinals = (eastWon = [2, 1], westWon = [1, 1]): BracketMatchup[] => [
		{
			home: { tid: 0, cid: 0, won: eastWon[0]! },
			away: { tid: 1, cid: 0, won: eastWon[1]! },
		},
		{
			home: { tid: 4, cid: 1, won: westWon[0]! },
			away: { tid: 5, cid: 1, won: westWon[1]! },
		},
	];
	const finals = (homeWon = 1, awayWon = 1): BracketMatchup[] => [
		{
			home: { tid: 0, cid: 0, won: homeWon },
			away: { tid: 4, cid: 1, won: awayWon },
		},
	];

	test("both conferences are live while their finals are being played", () => {
		const open = bracketMarketsOpen({ matchups: confFinals(), bestOf: 7 });
		assert.deepStrictEqual([...open.conferenceCids].sort(), [0, 1]);
		assert.strictEqual(open.title, true);
	});

	test("no conference market survives into the Finals", () => {
		const open = bracketMarketsOpen({ matchups: finals(), bestOf: 7 });
		assert.strictEqual(open.conferenceCids.size, 0);
		// The championship is still a live question, though.
		assert.strictEqual(open.title, true);
	});

	// Conference finals rarely end on the same day, so the settled one has to
	// come down on its own rather than waiting for the round to turn over.
	test("a conference closes the moment its own series is clinched", () => {
		const open = bracketMarketsOpen({
			matchups: confFinals([4, 1], [2, 2]),
			bestOf: 7,
		});
		assert.deepStrictEqual([...open.conferenceCids], [1]);
	});

	test("either side clinching closes it, not just the home side", () => {
		const open = bracketMarketsOpen({
			matchups: confFinals([1, 4], [2, 2]),
			bestOf: 7,
		});
		assert.deepStrictEqual([...open.conferenceCids], [1]);
	});

	test("the series length decides what counts as clinched", () => {
		// 3 wins takes a best-of-5 but not a best-of-7.
		assert.strictEqual(
			bracketMarketsOpen({ matchups: confFinals([3, 1]), bestOf: 5 })
				.conferenceCids.size,
			1,
		);
		assert.strictEqual(
			bracketMarketsOpen({ matchups: confFinals([3, 1]), bestOf: 7 })
				.conferenceCids.size,
			2,
		);
	});

	test("the title comes down once the last series is decided", () => {
		assert.strictEqual(
			bracketMarketsOpen({ matchups: finals(4, 2), bestOf: 7 }).title,
			false,
		);
		assert.strictEqual(
			bracketMarketsOpen({ matchups: finals(2, 4), bestOf: 7 }).title,
			false,
		);
	});

	// An earlier round with a clinched series is not the end of anything.
	test("a clinched series in an earlier round leaves the title alone", () => {
		assert.strictEqual(
			bracketMarketsOpen({ matchups: confFinals([4, 0], [4, 0]), bestOf: 7 })
				.title,
			true,
		);
	});

	test("a bye advances its team without deciding a conference", () => {
		const open = bracketMarketsOpen({
			matchups: [
				{ home: { tid: 0, cid: 0, won: 0 } },
				{
					home: { tid: 1, cid: 0, won: 0 },
					away: { tid: 2, cid: 0, won: 0 },
				},
			],
			bestOf: 7,
		});
		// Three East teams are still alive behind one bye and one live series.
		assert.deepStrictEqual([...open.conferenceCids], [0]);
	});

	test("an empty bracket claims nothing is open", () => {
		const open = bracketMarketsOpen({ matchups: [], bestOf: 7 });
		assert.strictEqual(open.conferenceCids.size, 0);
		assert.strictEqual(open.title, false);
	});
});

// A talent gap is not a point differential, and a point differential is not a
// win rate - both saturate. A league whose best roster rates 20+ points clear
// of average was projecting a 79-win season a week into the schedule.
describe("win totals stay inside reality", () => {
	const stacked = (gamesPlayed: number): FuturesTeam[] => {
		// The raw margins a stacked league produces, run through the same
		// saturation and evidence shading getLines applies before simulating.
		const RAW = [21, 18, 6, 4, 2, 1, 0, 0, -1, -2, -3, -4, -5, -7, -9, -12];
		const evidence = 0.85 + 0.15 * Math.min(1, gamesPlayed / 25);
		return RAW.map((raw, i) => ({
			tid: i,
			cid: i % 2,
			did: i % 4,
			won: Math.round(gamesPlayed * (0.5 + raw / 60)),
			gamesRemaining: 82 - gamesPlayed,
			rating: softCapMargin(raw) * evidence,
		}));
	};

	const linesFor = (gamesPlayed: number) => {
		const sim = simulateFutures({
			teams: stacked(gamesPlayed),
			numGamesPlayoffSeries: [7, 7, 7, 7],
			iterations: 3000,
			seed: 7,
			ratingUncertainty: 1 + 0.5 * (1 - gamesPlayed / 82),
		});
		return stacked(gamesPlayed).map((t) => sim.winTotals.get(t.tid)!.line);
	};

	test("nobody is projected for a record-shattering season before tip-off", () => {
		const lines = linesFor(0);
		assert.ok(
			Math.max(...lines) <= 63,
			`preseason favorite at ${Math.max(...lines)} wins`,
		);
		assert.ok(
			Math.min(...lines) >= 15,
			`worst team at ${Math.min(...lines)} wins`,
		);
	});

	test("a handful of games doesn't move the number much", () => {
		const before = Math.max(...linesFor(0));
		const after = Math.max(...linesFor(6));
		assert.ok(
			Math.abs(after - before) <= 6,
			`line jumped from ${before} to ${after} on six games`,
		);
	});

	test("the number does eventually follow what a team has actually done", () => {
		// 70 games in at an 85% clip, the season is nearly decided and the line
		// has to reflect it rather than staying shaded toward the field.
		const late = Math.max(...linesFor(70));
		assert.ok(late >= 64, `late-season line only ${late}`);
	});

	test("the whole league lands in a believable band", () => {
		for (const gp of [0, 6, 20, 41]) {
			for (const line of linesFor(gp)) {
				assert.ok(
					line >= 10 && line <= 70,
					`line ${line} at ${gp} games played`,
				);
			}
		}
	});
});

describe("softCapMargin", () => {
	test("ordinary margins pass through almost untouched", () => {
		for (const m of [-5, -2, 0, 2, 5]) {
			assert.ok(
				Math.abs(softCapMargin(m) - m) < 0.7,
				`${m} became ${softCapMargin(m)}`,
			);
		}
	});

	test("an impossible margin is pulled back to a possible one", () => {
		assert.ok(softCapMargin(25) < 11.5, `${softCapMargin(25)}`);
		assert.ok(softCapMargin(-25) > -11.5, `${softCapMargin(-25)}`);
	});

	test("it never reorders two teams", () => {
		let prev = -Infinity;
		for (let m = -30; m <= 30; m += 0.5) {
			const v = softCapMargin(m);
			assert.ok(v > prev, `not monotonic at ${m}`);
			prev = v;
		}
	});
});

// The board as a whole, calibrated against what a real book posts. These bands
// were found by sweeping the model's three knobs (margin saturation, how far a
// rating is shaded toward the field before games are played, and how unsure the
// book is about a rating) across several league shapes at several points in a
// season - so they are the calibration, not a snapshot of whatever the code
// happened to produce. Drifting outside them means the book is giving money
// away in one direction or the other.
describe("the futures board is calibrated", () => {
	const NUM_TEAMS = 30;
	const NUM_GAMES = 82;
	const CAP = 9;
	const PRIOR = 0.85;
	const EV_GAMES = 25;

	// Ratings as raw point margins vs an average team, best first.
	const shape = (kind: "normal" | "superteam" | "parity"): number[] => {
		const out: number[] = [];
		for (let i = 0; i < NUM_TEAMS; i++) {
			const z = (i - (NUM_TEAMS - 1) / 2) / ((NUM_TEAMS - 1) / 2);
			if (kind === "normal") {
				out.push(-z * 8);
			} else if (kind === "parity") {
				out.push(-z * 3);
			} else {
				out.push(i === 0 ? 21 : i === 1 ? 18 : -z * 7);
			}
		}
		return out.sort((a, b) => b - a);
	};

	const priceBoard = (raw: number[], gamesPlayed: number) => {
		const evidence = PRIOR + (1 - PRIOR) * Math.min(1, gamesPlayed / EV_GAMES);
		const teams: FuturesTeam[] = raw.map((r, i) => ({
			tid: i,
			cid: i % 2,
			did: i % 6,
			won: Math.round(
				gamesPlayed * Math.min(0.85, Math.max(0.15, 0.5 + r / 40)),
			),
			gamesRemaining: NUM_GAMES - gamesPlayed,
			rating: softCapMargin(r, CAP) * evidence,
		}));
		const sim = simulateFutures({
			teams,
			numGamesPlayoffSeries: [7, 7, 7, 7],
			iterations: 2500,
			seed: 11,
			ratingUncertainty: 1 + 0.5 * (1 - gamesPlayed / NUM_GAMES),
		});
		const lines = teams.map((t) => sim.winTotals.get(t.tid)!.line);
		return {
			favProb: sim.titleProb.get(0)!,
			lines,
			sum: lines.reduce((a, b) => a + b, 0),
		};
	};

	test("an ordinary league's best team is a real but beatable favorite", () => {
		const p = priceBoard(shape("normal"), 0).favProb;
		assert.ok(p > 0.12 && p < 0.24, `favorite at ${(p * 100).toFixed(1)}%`);
	});

	test("a genuine superteam is priced like one, not like the field", () => {
		const p = priceBoard(shape("superteam"), 0).favProb;
		assert.ok(p > 0.25 && p < 0.5, `superteam at ${(p * 100).toFixed(1)}%`);
	});

	// Never tuned against - a league with no separation must not manufacture a
	// favorite out of noise.
	test("a league with no separation has no strong favorite", () => {
		const { favProb, lines } = priceBoard(shape("parity"), 0);
		assert.ok(
			favProb < 0.16,
			`parity favorite at ${(favProb * 100).toFixed(1)}%`,
		);
		assert.ok(
			Math.max(...lines) - Math.min(...lines) < 22,
			`parity win totals spread ${Math.max(...lines)} to ${Math.min(...lines)}`,
		);
	});

	test("the favorite shortens as the season confirms it, never drifts out", () => {
		for (const kind of ["normal", "superteam"] as const) {
			let prev = 0;
			for (const gp of [0, 20, 41, 70]) {
				const p = priceBoard(shape(kind), gp).favProb;
				assert.ok(
					p >= prev - 0.03,
					`${kind} favorite went backwards at ${gp} games: ${prev} -> ${p}`,
				);
				prev = p;
			}
		}
	});

	test("the win totals add up to roughly the games available", () => {
		const total = (NUM_TEAMS * NUM_GAMES) / 2;
		for (const kind of ["normal", "superteam", "parity"] as const) {
			for (const gp of [0, 20, 41]) {
				const { sum } = priceBoard(shape(kind), gp);
				assert.ok(
					Math.abs(sum - total) < total * 0.03,
					`${kind} at ${gp} games sums to ${Math.round(sum)}, not ~${total}`,
				);
			}
		}
	});
});
