// FUTURES EV HARNESS - proves no offered futures row is free money against the
// engine's own arithmetic, at several points in a season.
//
// Skipped unless FUTURES_EV_ROSTERS points at the same real-league rosters
// JSON spreadCalibration.test.ts uses (that harness is where the margin model
// and its error sizes were measured against actual engine games; this one
// builds on those numbers instead of re-simulating).
//
// Two adversaries, two standards:
//
//   STRUCTURAL (the enforced one, FUTURES_EV_MODEL_ERR=0): the sharpest
//   bettor the game can actually contain - they know the rosters, the
//   results, and the code, so their best strength estimate is the SAME
//   Kalman blend the book computes; nobody in-game can out-estimate it
//   without exporting the league and running headless engine sims. Truth here
//   is that shared estimate played through the exact schedule and the exact
//   bracket rules (fixed 1v8 pairings, 2-2-1-1-1 home pattern, home court by
//   record); the book is its own Monte Carlo machinery and pricing. Any +EV
//   row is a pure structural bias - an approximation leaking money - and
//   fails the test.
//
//   EPISTEMIC (reported, FUTURES_EV_MODEL_ERR=measured ~1.3): truth ratings
//   get a seeded persistent per-team offset at the measured size of the
//   model's real miss vs the engine, which the book cannot see. This is the
//   exposure to someone who DOES headless-sim the engine. It cannot be priced
//   to zero without unbettable juice; the win-total load and tail vig charge
//   for its typical size, and this mode asserts only that the board stays
//   negative-EV on average.
//
// Every row the book would offer (title, conference, division, win-total over
// AND under, and the in-playoffs bracket board) is graded: expected value
// = trueProb x payout - 1.
import { assert, test } from "vitest";
import { resetG } from "../../../test/helpers.ts";
import { g, helpers } from "../../util/index.ts";
import { isSport } from "../../../common/sportFunctions.ts";
import newScheduleGood from "../season/newScheduleGood.ts";
import { futuresStrengthFromPlayers } from "./futuresStrength.ts";
import {
	FUTURES_CAPS,
	FUTURES_MOV_PRIOR_GAMES,
	futuresRatingUncertainty,
	priceFuture,
	SETTLED_PRICE,
	winTotalLoad,
} from "./getLines.ts";
import {
	BASKETBALL_PLAYOFF_SYNERGY_COEF,
	BASKETBALL_PLAYOFF_SYNERGY_OVR_SLOPE,
	BASKETBALL_SYNERGY_COEF,
	BASKETBALL_SYNERGY_OVR_SLOPE,
} from "../../../common/getGameSpread.ts";
import {
	betterSeedHome,
	MARGIN_SIGMA,
	mulberry32,
	normalCdf,
	normalSample,
} from "../../../common/sportsbookOdds.ts";
import {
	simulateFutures,
	simulatePlayoffBracket,
	type BracketMatchup,
	type FuturesScheduleGame,
	type FuturesTeam,
} from "../../../common/sportsbookFutures.ts";
import { americanToDecimal } from "../../../common/sportsbook.ts";

const nodeEnv: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const ROSTERS = nodeEnv.FUTURES_EV_ROSTERS;
const TRUTH_SEASONS = Number(nodeEnv.FUTURES_EV_SEASONS ?? 100_000);

// The engine as measured on real rosters (spreadCalibration.test.ts).
const HCA_TRUE = 3.3504;
// Playoff games are worth more to the home side (measured 4.910 / 4.908).
const HCA_TRUE_PLAYOFFS = 4.909;
const SIGMA_TRUE = 13.1;
// The PERSISTENT per-team error the book's ratings carry - the component of
// the model's miss that follows a team into every game, which is what a
// season-long market actually exposes (single-matchup misses average out over
// 40 different opponents). Measured by spreadCalibration.test.ts's per-team
// decomposition: 1.30 / 1.35 on the two real leagues. Overridable for
// experiments (0 isolates structural bias in the book's own machinery, and is
// the mode the strict assertion runs in - see the bottom of the test).
const BOOK_MODEL_ERROR = Number(nodeEnv.FUTURES_EV_MODEL_ERR ?? 1.3);

const NUM_GAMES = 82;
const PLAYOFF_SERIES = [7, 7, 7, 7];
const CONF_PLAYOFF_TEAMS = 8;

type TruthGame = { home: number; away: number; pHome: number };

test.skipIf(!ROSTERS || !isSport("basketball"))(
	"no futures row is +EV against the engine",
	{ timeout: 1_200_000 },
	async () => {
		resetG();
		g.setWithoutSavingToDB("userTids", []);
		g.setWithoutSavingToDB("userTid", 0);

		const fs = await import(("node" + ":fs") as any);
		const data = JSON.parse(fs.readFileSync(ROSTERS!, "utf8"));
		const byTid = new Map<number, any[]>();
		for (const p of data.players) {
			if (!byTid.has(p.tid)) {
				byTid.set(p.tid, []);
			}
			byTid.get(p.tid)!.push(p);
		}
		const tids = [...byTid.keys()].sort((a, b) => a - b);
		const n = tids.length;

		// League geography from the default 30-team layout (the exports carry no
		// conf/div info; any consistent layout works, the markets just need one).
		const defaultTeams = helpers.getTeamsDefault();
		assert.strictEqual(n, defaultTeams.length, "expected a 30-team export");
		const cidOf = new Map(defaultTeams.map((t) => [t.tid, t.cid]));
		const didOf = new Map(defaultTeams.map((t) => [t.tid, t.did]));
		const cids = [...new Set(defaultTeams.map((t) => t.cid))];
		const dids = [...new Set(defaultTeams.map((t) => t.did))];

		// --- True team ratings: the shipped strength model on these rosters ----
		const strengths = tids.map((tid) => {
			const raw = byTid.get(tid)!;
			const plus = raw.map((p) => ({
				pid: p.pid,
				injury: p.injury ?? { gamesRemaining: 0 },
				value: p.value ?? 0,
				ratings: p.ratings.at(-1),
			}));
			return futuresStrengthFromPlayers(plus, raw, NUM_GAMES);
		});
		const meanOvr = strengths.reduce((s, r) => s + r.expectedOvr, 0) / n;
		assert.ok(
			strengths.every((r) => r.expectedSynergy !== undefined),
			"every roster should read a synergy",
		);
		const meanSyn = strengths.reduce((s, r) => s + r.expectedSynergy!, 0) / n;
		const trueRating = strengths.map(
			(r) =>
				BASKETBALL_SYNERGY_OVR_SLOPE * (r.expectedOvr - meanOvr) +
				BASKETBALL_SYNERGY_COEF * (r.expectedSynergy! - meanSyn),
		);
		// What each team gains (or gives back) in a playoff game, where synergy
		// counts roughly double - same construction as getLines' playoffAdjustOf.
		const playoffAdjust = strengths.map(
			(r) =>
				(BASKETBALL_PLAYOFF_SYNERGY_OVR_SLOPE - BASKETBALL_SYNERGY_OVR_SLOPE) *
					(r.expectedOvr - meanOvr) +
				(BASKETBALL_PLAYOFF_SYNERGY_COEF - BASKETBALL_SYNERGY_COEF) *
					(r.expectedSynergy! - meanSyn),
		);

		// The book's ratings: truth plus its measured model error, drawn once.
		const errRand = mulberry32(987654321);
		const bookModel = trueRating.map(
			(r) => r + normalSample(errRand) * BOOK_MODEL_ERROR,
		);

		// --- Schedule: a real 82-game slate, in a seeded shuffled order --------
		// The generator draws from Math.random; seed it (same trick as
		// simGameOutcomes) so every run of this harness prices the same slate.
		const realRandom = Math.random;
		let scheduleResult: ReturnType<typeof newScheduleGood>;
		try {
			Math.random = mulberry32(13579);
			scheduleResult = newScheduleGood(
				defaultTeams.map((t) => ({
					tid: t.tid,
					seasonAttrs: { cid: t.cid, did: t.did },
				})),
			);
		} finally {
			Math.random = realRandom;
		}
		const { tids: schedulePairs, warning } = scheduleResult;
		assert.strictEqual(warning, undefined);
		assert.strictEqual(schedulePairs.length, (n * NUM_GAMES) / 2);
		const shuffleRand = mulberry32(24601);
		const fullSchedule = [...schedulePairs];
		for (let i = fullSchedule.length - 1; i > 0; i--) {
			const j = Math.floor(shuffleRand() * (i + 1));
			[fullSchedule[i], fullSchedule[j]] = [fullSchedule[j]!, fullSchedule[i]!];
		}

		// The ratings truth plays with - set per state (see the two standards in
		// the header): the book's own blended estimate in structural mode, the
		// engine-side ratings in epistemic mode.
		const STRUCTURAL = BOOK_MODEL_ERROR === 0;
		let truthRatings: number[] = trueRating;
		// Playoff-game ratings for the same state: truthRatings + playoffAdjust.
		let truthPlayoffRatings: number[] = trueRating.map(
			(r, i) => r + playoffAdjust[i]!,
		);
		const setTruthRatings = (base: number[]) => {
			truthRatings = base;
			truthPlayoffRatings = base.map((r, i) => r + playoffAdjust[i]!);
		};

		const truthGame = ([home, away]: [number, number]): TruthGame => ({
			home,
			away,
			pHome: normalCdf(
				(truthRatings[home]! - truthRatings[away]! + HCA_TRUE) / SIGMA_TRUE,
			),
		});

		// --- Truth machinery ---------------------------------------------------
		// Series are playoff games: the playoff-model ratings apply.
		const pTrueSeries = (a: number, b: number, hca: number) =>
			normalCdf(
				(truthPlayoffRatings[a]! - truthPlayoffRatings[b]! + hca) / SIGMA_TRUE,
			);

		// Play out a best-of series game by game; `better` holds home court.
		const playSeries = (
			rand: () => number,
			better: number,
			other: number,
			bestOf: number,
			betterWon = 0,
			otherWon = 0,
		): number => {
			const winsNeeded = Math.ceil(bestOf / 2);
			let bw = betterWon;
			let ow = otherWon;
			while (bw < winsNeeded && ow < winsNeeded) {
				const hca = betterSeedHome(bestOf, bw + ow)
					? HCA_TRUE_PLAYOFFS
					: -HCA_TRUE_PLAYOFFS;
				if (rand() < pTrueSeries(better, other, hca)) {
					bw += 1;
				} else {
					ow += 1;
				}
			}
			return bw >= winsNeeded ? better : other;
		};

		// The engine's actual bracket: top 8 per conference by record, fixed 1v8 /
		// 4v5 / 3v6 / 2v7 pairings, home court (and later-round home court) to the
		// better record, finals between the conference champions.
		const R1_ORDER = [
			[0, 7],
			[3, 4],
			[2, 5],
			[1, 6],
		] as const;
		const runTruthBracket = (
			rand: () => number,
			wins: Int32Array,
		): { champ: number; finalists: number[] } => {
			const finalists: number[] = [];
			for (const cid of cids) {
				const seeds = tids
					.filter((tid) => cidOf.get(tid) === cid)
					.sort((a, b) => wins[b]! - wins[a]! + (rand() - 0.5) * 1e-6);
				let alive = R1_ORDER.map(([hi]) => {
					const better = seeds[hi]!;
					const other = seeds[CONF_PLAYOFF_TEAMS - 1 - hi]!;
					return playSeries(rand, better, other, PLAYOFF_SERIES[0]!);
				});
				let round = 1;
				while (alive.length > 1) {
					const next: number[] = [];
					for (let i = 0; i + 1 < alive.length; i += 2) {
						const a = alive[i]!;
						const b = alive[i + 1]!;
						const better = wins[a]! >= wins[b]! ? a : b;
						const other = better === a ? b : a;
						next.push(playSeries(rand, better, other, PLAYOFF_SERIES[round]!));
					}
					alive = next;
					round += 1;
				}
				finalists.push(alive[0]!);
			}
			const [f0, f1] = finalists as [number, number];
			const better = wins[f0]! >= wins[f1]! ? f0 : f1;
			const other = better === f0 ? f1 : f0;
			const champ = playSeries(rand, better, other, PLAYOFF_SERIES.at(-1)!);
			return { champ, finalists };
		};

		// Truth for one league state: distributions of final wins, division
		// winners, finalists and champions over TRUTH_SEASONS worlds.
		const runTruth = (
			winsInit: number[],
			remaining: TruthGame[],
			seed: number,
		) => {
			const rand = mulberry32(seed);
			const winsHist = tids.map(() => new Float64Array(NUM_GAMES + 1));
			const titleCount = new Float64Array(n);
			const confCount = new Float64Array(n);
			const divCount = new Float64Array(n);
			const wins = new Int32Array(n);
			for (let s = 0; s < TRUTH_SEASONS; s++) {
				for (let i = 0; i < n; i++) {
					wins[i] = winsInit[i]!;
				}
				for (const gm of remaining) {
					if (rand() < gm.pHome) {
						wins[gm.home] = wins[gm.home]! + 1;
					} else {
						wins[gm.away] = wins[gm.away]! + 1;
					}
				}
				for (let i = 0; i < n; i++) {
					const hist = winsHist[i]!;
					hist[wins[i]!] = hist[wins[i]!]! + 1;
				}
				for (const did of dids) {
					let best = -1;
					for (const tid of tids) {
						if (didOf.get(tid) !== did) {
							continue;
						}
						if (
							best === -1 ||
							wins[tid]! > wins[best]! ||
							(wins[tid] === wins[best] && rand() < 0.5)
						) {
							best = tid;
						}
					}
					divCount[best] = divCount[best]! + 1;
				}
				const { champ, finalists } = runTruthBracket(rand, wins);
				titleCount[champ] = titleCount[champ]! + 1;
				for (const f of finalists) {
					confCount[f] = confCount[f]! + 1;
				}
			}
			return {
				pTitle: (tid: number) => titleCount[tid]! / TRUTH_SEASONS,
				pConf: (tid: number) => confCount[tid]! / TRUTH_SEASONS,
				pDiv: (tid: number) => divCount[tid]! / TRUTH_SEASONS,
				pOver: (tid: number, line: number) => {
					let count = 0;
					const hist = winsHist[tid]!;
					for (let w = Math.ceil(line); w <= NUM_GAMES; w++) {
						count += hist[w]!;
					}
					return count / TRUTH_SEASONS;
				},
			};
		};

		// --- Book machinery ----------------------------------------------------
		// ratingOf's blend, exactly as getLines computes it.
		const bookRating = (tid: number, gp: number, movPerGame: number) => {
			const w = gp / (gp + FUTURES_MOV_PRIOR_GAMES);
			return bookModel[tid]! * (1 - w) + movPerGame * w;
		};

		const gradeRows: {
			state: string;
			market: string;
			tid: number;
			ev: number;
			pTrue: number;
			odds: number;
		}[] = [];
		const grade = (
			state: string,
			market: string,
			tid: number,
			pTrue: number,
			odds: number,
		) => {
			// The board never offers a price at/inside the settled threshold - the
			// row comes down (see getLines' notSettled), so there is nothing to bet.
			if (odds <= SETTLED_PRICE) {
				return;
			}
			gradeRows.push({
				state,
				market,
				tid,
				pTrue,
				odds,
				ev: pTrue * americanToDecimal(odds) - 1,
			});
		};

		// One full regular-season state: everything the book prices pre-playoffs.
		const gradeSeasonState = (
			state: string,
			winsInit: number[],
			gpInit: number[],
			movSum: number[],
			remaining: [number, number][],
			seed: number,
		) => {
			// What the book will price with at this state.
			const stateRating = tids.map((tid) =>
				bookRating(
					tid,
					gpInit[tid]!,
					gpInit[tid]! > 0 ? movSum[tid]! / gpInit[tid]! : 0,
				),
			);
			setTruthRatings(STRUCTURAL ? stateRating : trueRating);
			const truth = runTruth(winsInit, remaining.map(truthGame), seed);
			const totalRemaining = remaining.length * 2;
			const totalPossible = n * NUM_GAMES;
			const seasonProgress = Math.min(
				1,
				Math.max(0, 1 - totalRemaining / totalPossible),
			);
			const teams: FuturesTeam[] = tids.map((tid) => ({
				tid,
				cid: cidOf.get(tid)!,
				did: didOf.get(tid)!,
				won: winsInit[tid]!,
				gamesRemaining: NUM_GAMES - gpInit[tid]!,
				rating: stateRating[tid]!,
			}));
			const schedule: FuturesScheduleGame[] = remaining.map(([home, away]) => ({
				homeTid: home,
				awayTid: away,
			}));
			const sim = simulateFutures({
				teams,
				numGamesPlayoffSeries: PLAYOFF_SERIES,
				iterations: 4000,
				seed: seed + 1,
				ratingUncertainty: futuresRatingUncertainty(seasonProgress),
				schedule,
				hcaPoints: HCA_TRUE,
				playoffHcaPoints: HCA_TRUE_PLAYOFFS,
				sigma: MARGIN_SIGMA,
				playoffRatings: new Map(
					tids.map((tid) => [tid, stateRating[tid]! + playoffAdjust[tid]!]),
				),
			});
			for (const tid of tids) {
				grade(
					state,
					"title",
					tid,
					truth.pTitle(tid),
					priceFuture(sim.titleProb.get(tid)!, 4000, FUTURES_CAPS.title),
				);
				grade(
					state,
					"conf",
					tid,
					truth.pConf(tid),
					priceFuture(sim.confProb.get(tid)!, 4000, FUTURES_CAPS.conference),
				);
				grade(
					state,
					"div",
					tid,
					truth.pDiv(tid),
					priceFuture(sim.divProb.get(tid)!, 4000, FUTURES_CAPS.division),
				);
				if (NUM_GAMES - gpInit[tid]! > 0) {
					const wt = sim.winTotals.get(tid)!;
					const load = winTotalLoad({
						gamesRemaining: NUM_GAMES - gpInit[tid]!,
						gp: gpInit[tid]!,
						slope: wt.slope,
						winsSd: wt.winsSd,
						sigma: MARGIN_SIGMA,
					});
					const pOverTrue = truth.pOver(tid, wt.line);
					grade(
						state,
						"winTotalOver",
						tid,
						pOverTrue,
						priceFuture(wt.pOver + load),
					);
					grade(
						state,
						"winTotalUnder",
						tid,
						1 - pOverTrue,
						priceFuture(1 - wt.pOver + load),
					);
				}
			}
		};

		// --- State 1: preseason ------------------------------------------------
		gradeSeasonState(
			"preseason",
			tids.map(() => 0),
			tids.map(() => 0),
			tids.map(() => 0),
			fullSchedule,
			1001,
		);

		// --- States 2-3: mid and late season -----------------------------------
		// Play the season forward ONCE under truth (seeded) to a realistic
		// standings snapshot, then grade the board there.
		const playPrefix = (numGamesPrefix: number, seed: number) => {
			const rand = mulberry32(seed);
			const wins = tids.map(() => 0);
			const gp = tids.map(() => 0);
			const mov = tids.map(() => 0);
			const prefix = fullSchedule.slice(0, numGamesPrefix);
			for (const [home, away] of prefix) {
				const spread = trueRating[home]! - trueRating[away]! + HCA_TRUE;
				const margin = spread + normalSample(rand) * SIGMA_TRUE;
				if (margin > 0) {
					wins[home] = wins[home]! + 1;
				} else {
					wins[away] = wins[away]! + 1;
				}
				gp[home] = gp[home]! + 1;
				gp[away] = gp[away]! + 1;
				mov[home] = mov[home]! + margin;
				mov[away] = mov[away]! - margin;
			}
			return { wins, gp, mov, remaining: fullSchedule.slice(numGamesPrefix) };
		};

		const mid = playPrefix(Math.round((n * 41) / 2), 555);
		gradeSeasonState(
			"mid-season",
			mid.wins,
			mid.gp,
			mid.mov,
			mid.remaining,
			2002,
		);

		const late = playPrefix(Math.round((n * 70) / 2), 777);
		gradeSeasonState(
			"late-season",
			late.wins,
			late.gp,
			late.mov,
			late.remaining,
			3003,
		);

		// --- State 4: playoffs, round 1 in progress ----------------------------
		// Full truth season, seed the real bracket, put every series at 2-1.
		{
			const full = playPrefix(fullSchedule.length, 999);
			const seedRand = mulberry32(4004);
			const winsArr = new Int32Array(n);
			for (const tid of tids) {
				winsArr[tid] = full.wins[tid]!;
			}
			// The book's end-of-season blended ratings, and which ratings the truth
			// bracket runs on (the series-in-progress states below are generated
			// from engine-side reality either way).
			const playoffRating = tids.map(
				(tid) =>
					bookRating(tid, NUM_GAMES, full.mov[tid]! / NUM_GAMES) +
					playoffAdjust[tid]!,
			);
			setTruthRatings(trueRating);
			const matchups: BracketMatchup[] = [];
			for (const cid of cids) {
				const seeds = tids
					.filter((tid) => cidOf.get(tid) === cid)
					.sort((a, b) => winsArr[b]! - winsArr[a]!);
				for (const [hi] of R1_ORDER) {
					const better = seeds[hi]!;
					const other = seeds[CONF_PLAYOFF_TEAMS - 1 - hi]!;
					// Play the first 3 games of the series under truth.
					let bw = 0;
					let ow = 0;
					for (let gm = 0; gm < 3; gm++) {
						const hca = betterSeedHome(PLAYOFF_SERIES[0]!, gm)
							? HCA_TRUE_PLAYOFFS
							: -HCA_TRUE_PLAYOFFS;
						if (seedRand() < pTrueSeries(better, other, hca)) {
							bw += 1;
						} else {
							ow += 1;
						}
					}
					matchups.push({
						home: { tid: better, cid, won: bw },
						away: { tid: other, cid, won: ow },
					});
				}
			}

			// Truth: finish the bracket from this exact state, many times.
			// (playoffRating already carries the playoff adjust; setTruthRatings
			// would add it twice, so set the pair directly.)
			if (STRUCTURAL) {
				truthRatings = playoffRating.map((r, i) => r - playoffAdjust[i]!);
				truthPlayoffRatings = playoffRating;
			} else {
				setTruthRatings(trueRating);
			}
			const titleCount = new Float64Array(n);
			const confCount = new Float64Array(n);
			const truthRand = mulberry32(6006);
			for (let s = 0; s < TRUTH_SEASONS; s++) {
				const finalists: number[] = [];
				for (const cid of cids) {
					let alive: number[] = [];
					for (const m of matchups) {
						if (m.home.cid !== cid) {
							continue;
						}
						alive.push(
							playSeries(
								truthRand,
								m.home.tid,
								m.away!.tid,
								PLAYOFF_SERIES[0]!,
								m.home.won,
								m.away!.won,
							),
						);
					}
					let round = 1;
					while (alive.length > 1) {
						const next: number[] = [];
						for (let i = 0; i + 1 < alive.length; i += 2) {
							const a = alive[i]!;
							const b = alive[i + 1]!;
							const better = winsArr[a]! >= winsArr[b]! ? a : b;
							const other = better === a ? b : a;
							next.push(
								playSeries(truthRand, better, other, PLAYOFF_SERIES[round]!),
							);
						}
						alive = next;
						round += 1;
					}
					finalists.push(alive[0]!);
				}
				const [f0, f1] = finalists as [number, number];
				const better = winsArr[f0]! >= winsArr[f1]! ? f0 : f1;
				const other = better === f0 ? f1 : f0;
				const champ = playSeries(
					truthRand,
					better,
					other,
					PLAYOFF_SERIES.at(-1)!,
				);
				titleCount[champ] = titleCount[champ]! + 1;
				confCount[f0] = confCount[f0]! + 1;
				confCount[f1] = confCount[f1]! + 1;
			}

			// Book: the shipped in-playoffs path.
			const ratings = new Map<number, number>();
			for (const m of matchups) {
				for (const side of [m.home, m.away!]) {
					ratings.set(side.tid, playoffRating[side.tid]!);
				}
			}
			const seedOrder = new Map(
				[...tids]
					.sort((a, b) => winsArr[b]! - winsArr[a]!)
					.map((t, i) => [t, i]),
			);
			const bracketSim = simulatePlayoffBracket({
				matchups,
				startRound: 0,
				numGamesPlayoffSeries: PLAYOFF_SERIES,
				ratings,
				iterations: 4000,
				seed: 5005,
				ratingUncertainty: futuresRatingUncertainty(1),
				hcaPoints: HCA_TRUE_PLAYOFFS,
				sigma: MARGIN_SIGMA,
				seedOrder,
			});
			for (const [tid, p] of bracketSim.titleProb) {
				grade(
					"playoffs-r1",
					"title",
					tid,
					titleCount[tid]! / TRUTH_SEASONS,
					priceFuture(p, 4000, FUTURES_CAPS.title),
				);
			}
			for (const [tid, p] of bracketSim.confProb) {
				grade(
					"playoffs-r1",
					"conf",
					tid,
					confCount[tid]! / TRUTH_SEASONS,
					priceFuture(p, 4000, FUTURES_CAPS.conference),
				);
			}
		}

		// --- The verdict ---------------------------------------------------------
		// A row whose true probability is microscopic is safe by construction (the
		// +30000 cap means a $1 win pays at most $301, so pTrue < 0.2% can't reach
		// +EV) and its truth estimate is all noise - skip those, grade the rest.
		const graded = gradeRows.filter((r) => r.pTrue >= 0.002);
		graded.sort((a, b) => b.ev - a.ev);
		const worst = graded.slice(0, 12);
		const meanEv = graded.reduce((s, r) => s + r.ev, 0) / graded.length;
		const summaryLines = [
			`futures EV (${STRUCTURAL ? "structural" : `epistemic err ${BOOK_MODEL_ERROR}`}): ${gradeRows.length} rows graded (${graded.length} above the pTrue floor), ${TRUTH_SEASONS} truth seasons, mean EV ${(meanEv * 100).toFixed(1)}%`,
			...worst.map(
				(r) =>
					`  ${r.state} ${r.market} tid ${r.tid}: EV ${(r.ev * 100).toFixed(1)}% (pTrue ${(r.pTrue * 100).toFixed(2)}%, odds ${r.odds})`,
			),
		];
		const summary = summaryLines.join("\n");
		if (nodeEnv.FUTURES_EV_OUT) {
			fs.appendFileSync(nodeEnv.FUTURES_EV_OUT, `${summary}\n`);
		}
		console.log(summary);

		if (STRUCTURAL) {
			// The vig is 12% plus the tail ramp, so a structurally sound board sits
			// deeply negative everywhere. The bar is -1%: strictly no free money,
			// with a point of room for the noise floor (truth-MC error plus how a
			// particular schedule draw tilts a division race into the jitter's
			// Jensen bleed). The bugs this harness exists for measured +50% to
			// +300% - they cannot hide under a point.
			for (const r of graded) {
				assert.ok(
					r.ev < -0.01,
					`${r.state} ${r.market} tid ${r.tid} is +EV territory: EV ${(r.ev * 100).toFixed(1)}%, pTrue ${(r.pTrue * 100).toFixed(2)}%, odds ${r.odds}`,
				);
			}
		} else {
			// Epistemic mode: individual rows CAN be +EV for a bettor who
			// headless-sims the engine (the book cannot know its own model's
			// per-team bias), but the load + tail vig must keep the board
			// negative-EV in aggregate - grinding every row loses.
			assert.ok(
				meanEv < -0.05,
				`board mean EV ${(meanEv * 100).toFixed(1)}% - the vig is not covering the model's measured error`,
			);
		}
	},
);
