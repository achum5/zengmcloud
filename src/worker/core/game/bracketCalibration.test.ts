// DOES THE FUTURES BOARD MATCH THE ENGINE'S PLAYOFFS?
//
// The EV harness (sportsbook/futuresCalibration.test.ts) proves the board is
// internally consistent: nothing on it beats the book's OWN ratings. It cannot
// prove the ratings are right, because it grades them against themselves. This
// one closes that loop from the outside - it prices a bracket the way getLines
// does, then plays that same bracket out with the real GameSim under playoff
// parameters, and compares.
//
//   BRACKET_CAL_ROSTERS=<exported rosters json> BRACKET_CAL_RUNS=400 \
//     BRACKET_CAL_INJURIES=1 BRACKET_CAL_OUT=/tmp/out.txt SPORT=basketball \
//     npx vitest --run src/worker/core/game/bracketCalibration.test.ts
//
// BRACKET_CAL_INJURIES=1 plays the postseason with injuries live and carried
// across rounds. Use it: without it the engine is far more deterministic than
// the game a person actually plays (one league's favourite won 73% of titles
// with nobody ever getting hurt, 62% with injuries on), and the board is
// pricing the game with the injuries.
//
// BRACKET_CAL_SWEEP=1,2,3 prices the same bracket at each uncertainty and
// prints only that, for reading off which value matches the engine.
//
// WHAT IT ANSWERED. Two real leagues, 300-400 engine playoffs each:
//
//   - Where the favourite is genuinely clear, the board was already right.
//     League one's top team led by 3 rating points and the board gave it
//     59.5% against the engine's 61.7%.
//   - Where contenders are BUNCHED, the board was badly overconfident in its
//     own ordering. League two's top four sat inside 2.5 points; the board
//     said 33.0/31.6/19.0/9.2 and the engine played out 22.5/17.3/20.5/28.5 -
//     it offered 9% on a team that wins better than one postseason in four.
//   - The cause is measurable and was not priced: the playoff margin model's
//     PERSISTENT per-team error is 2.24 points against the regular season's
//     1.30 (spreadCalibration in PLAYOFFS mode), because the postseason counts
//     synergy roughly double and synergy is the softest thing the model reads.
//     The bracket was jittering ratings by ~1 point, the regular-season
//     figure. See FUTURES_PLAYOFF_DELTA_ERROR.
//   - Uncertainty is the honest fix but not a cure: swept from 1 to 8 points,
//     the joint calibration error over both leagues bottoms out broadly
//     between 2 and 4 (68.2 at 1.0, 64.0 at 2.0, 61.4 at 3.0, 64.3 at 4.0), so
//     the measured 2.24 sits inside the flat of the curve. It cannot rescue an
//     individual mis-ranked team - no symmetric jitter can - it just stops the
//     board claiming to know an order it does not know.
import { test } from "vitest";
import { resetG } from "../../../test/helpers.ts";
import { g, helpers } from "../../util/index.ts";
import GameSim from "../GameSim.ts";
import { processTeam } from "./loadTeams.ts";
import {
	DEFAULT_PLAY_THROUGH_INJURIES,
	PHASE,
} from "../../../common/constants.ts";
import { isSport } from "../../../common/sportFunctions.ts";
import { pregameLineupSynergy } from "../GameSim.basketball/synergy.ts";
import {
	BASKETBALL_PLAYOFF_HCA_FACTOR,
	BASKETBALL_PLAYOFF_SYNERGY_COEF,
	BASKETBALL_PLAYOFF_SYNERGY_OVR_SLOPE,
	BASKETBALL_SYNERGY_COEF,
	BASKETBALL_SYNERGY_OVR_SLOPE,
	homeCourtAdvantagePoints,
} from "../../../common/getGameSpread.ts";
import { betterSeedHome } from "../../../common/sportsbookOdds.ts";
import { simulatePlayoffBracket } from "../../../common/sportsbookFutures.ts";
import {
	futuresPlayoffUncertainty,
	futuresRatingUncertainty,
} from "../sportsbook/getLines.ts";

const nodeEnv: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const ROSTERS = nodeEnv.BRACKET_CAL_ROSTERS;
const RUNS = Number(nodeEnv.BRACKET_CAL_RUNS ?? 400);
// Play the bracket with injuries live, carried across the games of a series
// and the rounds of a run - the engine's real postseason, and the biggest
// thing a static board cannot see.
const INJURIES = nodeEnv.BRACKET_CAL_INJURIES === "1";

const mulberry32 = (a: number) => () => {
	a |= 0;
	a = (a + 0x6d_2b_79_f5) | 0;
	let t = Math.imul(a ^ (a >>> 15), 1 | a);
	t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
	return ((t ^ (t >>> 14)) >>> 0) / 4_294_967_296;
};

test.skipIf(!ROSTERS || !isSport("basketball"))(
	"futures title odds vs the engine's own playoffs",
	{ timeout: 3_600_000 },
	async () => {
		resetG();
		g.setWithoutSavingToDB("userTids", []);
		g.setWithoutSavingToDB("userTid", 0);
		g.setWithoutSavingToDB("phase", PHASE.PLAYOFFS);

		const fs = await import(("node" + ":fs") as any);
		const data = JSON.parse(fs.readFileSync(ROSTERS!, "utf8"));
		const byTid = new Map<number, any[]>();
		for (const p of data.players) {
			p.stats = [];
			p.injuries = [];
			if (!byTid.has(p.tid)) {
				byTid.set(p.tid, []);
			}
			byTid.get(p.tid)!.push(p);
		}
		const tids = [...byTid.keys()].sort((a, b) => a - b);
		const sideByTid = new Map<number, any>();
		for (const tid of tids) {
			const t = {
				tid,
				playThroughInjuries:
					data.teams?.find((x: any) => x.tid === tid)?.playThroughInjuries ??
					DEFAULT_PLAY_THROUGH_INJURIES,
				depth: undefined,
			};
			const teamSeason = { won: 0, lost: 0, tied: 0, otl: 0, cid: 0, did: 0 };
			sideByTid.set(
				tid,
				await processTeam(t as any, teamSeason as any, byTid.get(tid)!),
			);
		}

		// The board's own ratings: exactly getLines' strength path, with no games
		// played so the MOV blend contributes nothing (w = 0).
		const meta = tids.map((tid) => ({
			tid,
			ovr: sideByTid.get(tid)!.ovr as number,
			syn: pregameLineupSynergy(sideByTid.get(tid)!.player) ?? 0,
		}));
		const meanOvr = meta.reduce((s, m) => s + m.ovr, 0) / meta.length;
		const meanSyn = meta.reduce((s, m) => s + m.syn, 0) / meta.length;
		const regRating = (m: (typeof meta)[number]) =>
			BASKETBALL_SYNERGY_OVR_SLOPE * (m.ovr - meanOvr) +
			BASKETBALL_SYNERGY_COEF * (m.syn - meanSyn);
		const playoffRating = (m: (typeof meta)[number]) =>
			BASKETBALL_PLAYOFF_SYNERGY_OVR_SLOPE * (m.ovr - meanOvr) +
			BASKETBALL_PLAYOFF_SYNERGY_COEF * (m.syn - meanSyn);

		// Top 16 by playoff rating, seeded, split into two conferences by
		// alternating seeds so both are competitive.
		const seeded = [...meta].sort(
			(a, b) => playoffRating(b) - playoffRating(a),
		);
		const field = seeded.slice(0, 16);
		const east = field.filter((_, i) => i % 2 === 0);
		const west = field.filter((_, i) => i % 2 === 1);
		const seedOrder = new Map(field.map((m, i) => [m.tid, i]));

		const matchups: any[] = [];
		for (const conf of [east, west]) {
			for (const [h, a] of [
				[0, 7],
				[3, 4],
				[2, 5],
				[1, 6],
			] as const) {
				matchups.push({
					home: { tid: conf[h]!.tid, won: 0 },
					away: { tid: conf[a]!.tid, won: 0 },
				});
			}
		}

		const hca = homeCourtAdvantagePoints(1) * BASKETBALL_PLAYOFF_HCA_FACTOR;

		// SWEEP MODE: price the same bracket at a range of rating uncertainties
		// and print each, so the value that matches the engine can be read off
		// rather than argued about.
		const sweep = nodeEnv.BRACKET_CAL_SWEEP;
		if (sweep) {
			const sweepLines: string[] = [];
			for (const u of sweep.split(",").map(Number)) {
				const r = simulatePlayoffBracket({
					matchups,
					startRound: 0,
					numGamesPlayoffSeries: [7, 7, 7, 7],
					ratings: new Map(field.map((m) => [m.tid, playoffRating(m)])),
					iterations: 40_000,
					hcaPoints: hca,
					seedOrder,
					ratingUncertainty: u,
				});
				sweepLines.push(
					`unc=${u.toFixed(2)}  ` +
						field
							.slice(0, 6)
							.map((m) => `${((r.titleProb.get(m.tid) ?? 0) * 100).toFixed(1)}`)
							.join("  "),
				);
			}
			const fs2: any = await import(("node" + ":fs") as any);
			fs2.writeFileSync(nodeEnv.BRACKET_CAL_OUT!, sweepLines.join("\n") + "\n");
			return;
		}

		// WHAT THE BOARD SAYS.
		const priced = simulatePlayoffBracket({
			matchups,
			startRound: 0,
			numGamesPlayoffSeries: [7, 7, 7, 7],
			ratings: new Map(field.map((m) => [m.tid, playoffRating(m)])),
			iterations: 40_000,
			hcaPoints: hca,
			seedOrder,
			// Exactly what the live board uses at the end of a season.
			ratingUncertainty:
				nodeEnv.BRACKET_CAL_OLD_UNCERTAINTY === "1"
					? futuresRatingUncertainty(1)
					: futuresPlayoffUncertainty(futuresRatingUncertainty(1)),
		});

		// WHAT THE ENGINE DOES. The same bracket, played out with real games.
		const rand = mulberry32(20_260_827);
		const realRandom = Math.random;
		Math.random = rand;
		let gid = 1;
		const titles = new Map<number, number>();
		try {
			// Per-run roster state, so an injury in round one is still there in
			// round three. `injured` is what processTeam already resolved for the
			// sim; knocking a man out for the rest of the run is the closest this
			// gets to the real thing without re-running processTeam per game.
			let runSides = new Map<number, any>();
			const freshRun = () => {
				runSides = new Map(
					[...sideByTid].map(([tid, side]) => [tid, helpers.deepCopy(side)]),
				);
			};
			const sideFor = (tid: number) =>
				INJURIES ? runSides.get(tid) : sideByTid.get(tid);

			const playSeries = (aTid: number, bTid: number): number => {
				// Home court to the better seed, exactly the games betterSeedHome
				// gives it - the same rule the pricer uses.
				const [better, other] =
					seedOrder.get(aTid)! < seedOrder.get(bTid)!
						? [aTid, bTid]
						: [bTid, aTid];
				let wBetter = 0;
				let wOther = 0;
				for (let game = 0; wBetter < 4 && wOther < 4; game++) {
					const betterHome = betterSeedHome(7, game);
					const homeTid = betterHome ? better : other;
					const awayTid = betterHome ? other : better;
					const result: any = new GameSim({
						gid: gid++,
						day: -1,
						teams: helpers.deepCopy([
							sideFor(homeTid),
							sideFor(awayTid),
						]) as any,
						doPlayByPlay: false,
						homeCourtFactor: 1,
						neutralSite: false,
						allStarGame: false,
						baseInjuryRate: INJURIES ? g.get("injuryRate") : 0,
					} as any).run();
					if (INJURIES) {
						// Anyone hurt tonight is out for the rest of this run.
						for (const [side, tid] of [
							[result.team[0], homeTid],
							[result.team[1], awayTid],
						] as const) {
							for (const sp of side.player) {
								if (sp.newInjury) {
									const roster = runSides.get(tid)!;
									const hurt = roster.player.find((p2: any) => p2.id === sp.id);
									if (hurt) {
										hurt.injured = true;
									}
								}
							}
						}
					}
					const homeWon = result.team[0].stat.pts > result.team[1].stat.pts;
					const winner = homeWon ? homeTid : awayTid;
					if (winner === better) {
						wBetter++;
					} else {
						wOther++;
					}
				}
				return wBetter === 4 ? better : other;
			};

			for (let run = 0; run < RUNS; run++) {
				if (INJURIES) {
					freshRun();
				}
				const confWinners: number[] = [];
				for (const conf of [east, west]) {
					let alive = [
						[conf[0]!.tid, conf[7]!.tid],
						[conf[3]!.tid, conf[4]!.tid],
						[conf[2]!.tid, conf[5]!.tid],
						[conf[1]!.tid, conf[6]!.tid],
					].map(([a, b]) => playSeries(a!, b!));
					while (alive.length > 1) {
						const next: number[] = [];
						for (let i = 0; i < alive.length; i += 2) {
							next.push(playSeries(alive[i]!, alive[i + 1]!));
						}
						alive = next;
					}
					confWinners.push(alive[0]!);
				}
				const champ = playSeries(confWinners[0]!, confWinners[1]!);
				titles.set(champ, (titles.get(champ) ?? 0) + 1);
			}
		} finally {
			Math.random = realRandom;
		}

		const lines: string[] = [
			`bracket calibration: ${RUNS} engine playoffs vs the priced board` +
				(INJURIES ? " (injuries live)" : " (no injuries)"),
			`seed  tid   regRating  poRating   board%   engine%`,
		];
		for (const [i, m] of field.entries()) {
			const board = (priced.titleProb.get(m.tid) ?? 0) * 100;
			const engine = ((titles.get(m.tid) ?? 0) / RUNS) * 100;
			lines.push(
				`${String(i + 1).padStart(4)}  ${String(m.tid).padStart(3)}  ` +
					`${regRating(m).toFixed(2).padStart(9)}  ${playoffRating(m).toFixed(2).padStart(8)}  ` +
					`${board.toFixed(1).padStart(6)}  ${engine.toFixed(1).padStart(8)}`,
			);
		}
		const out = lines.join("\n");
		const path = nodeEnv.BRACKET_CAL_OUT;
		if (path) {
			fs.writeFileSync(path, out + "\n");
		}
		console.log(out);
	},
);
