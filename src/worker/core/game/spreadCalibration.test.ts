// SPREAD CALIBRATION HARNESS - how getGameSpread's basketball coefficients were
// measured, kept runnable so they can be re-fit when the engine changes.
//
// Skipped unless SPREAD_CALIBRATION_ROSTERS points at a rosters JSON, because a
// run is ~100,000 full game sims (about six minutes). The JSON is an export of
// a real league's players and teams:
//
//   { players: Player[] (tid >= 0, ratings trimmed to the last row),
//     teams: { tid, playThroughInjuries }[] }
//
// pulled straight out of a league's IndexedDB. Real rosters are NOT optional
// rigor: on synthetic createRandomPlayers leagues the engine's home-court
// advantage measures ~1.8 points, on real leagues ~3.3 - the same code, the
// same settings, different roster shapes. Coefficients fitted on synthetic
// rosters do not describe the game users play.
//
// What the last run (two leagues, 104,400 sims) found:
//   ovr-only refit:   margin = 0.33 * dOvr + 3.2   (shipped 0.3 / 3.3504 - fine)
//   with synergy:     margin = 0.17 * dOvr + 8.6 * dSyn + 3.2
//   model error vs the engine: 2.8 points -> 2.0, on a 1.3 noise floor,
//   and each league's coefficients priced the other league within 0.02 of its
//   own best fit.
//   within-matchup margin sigma: 13.09 / 13.10 (MARGIN_SIGMA 13 - validated),
//   flat across spread sizes (13.0-13.8 from pick'em to 20+ blowouts).
//   total: mean ~209, sigma 17.7-17.8 = 8.5% of the mean (overProb's 0.085).
import { assert, test } from "vitest";
import { resetG } from "../../../test/helpers.ts";
import { g, helpers } from "../../util/index.ts";
import GameSim from "../GameSim.ts";
import { processTeam } from "./loadTeams.ts";
import { DEFAULT_PLAY_THROUGH_INJURIES } from "../../../common/constants.ts";
import { isSport } from "../../../common/sportFunctions.ts";
import { pregameLineupSynergy } from "../GameSim.basketball/synergy.ts";
import { getGameSpread } from "../../../common/getGameSpread.ts";

// The worker's process.env type is a closed set; the harness env vars reach it
// the same way decadesSim's do.
const nodeEnv: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const ROSTERS = nodeEnv.SPREAD_CALIBRATION_ROSTERS;
const K = Number(nodeEnv.SPREAD_CALIBRATION_SIMS ?? 60);

test.skipIf(!ROSTERS || !isSport("basketball"))(
	"spread coefficients vs the engine",
	{ timeout: 3_600_000 },
	async () => {
		resetG();
		g.setWithoutSavingToDB("userTids", []);
		g.setWithoutSavingToDB("userTid", 0);

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
		const sides: any[] = [];
		for (const tid of tids) {
			const t = {
				tid,
				playThroughInjuries:
					data.teams?.find((x: any) => x.tid === tid)?.playThroughInjuries ??
					DEFAULT_PLAY_THROUGH_INJURIES,
				depth: undefined,
			};
			const teamSeason = { won: 0, lost: 0, tied: 0, otl: 0, cid: 0, did: 0 };
			sides.push(
				await processTeam(t as any, teamSeason as any, byTid.get(tid)!),
			);
		}

		// The exact pregame inputs the live formula uses.
		const meta = sides.map((t) => ({
			ovr: t.ovr,
			syn: pregameLineupSynergy(t.player),
		}));

		let gid = 1;
		const rows: { i: number; j: number; pred: number; mean: number }[] = [];
		// Within-matchup noise, pooled across every pairing: the sigma that
		// converts a spread into a win probability (MARGIN_SIGMA), and the same
		// for game totals (overProb's sigma). Binned by predicted spread size to
		// check the noise doesn't grow or shrink in blowouts.
		let marginSS = 0; // sum of squared margin deviations from the pairing mean
		let marginN = 0; // degrees of freedom accumulated (K - 1 per pairing)
		let totalSS = 0;
		let totalSum = 0;
		let totalCount = 0;
		const sigmaBins = [
			{ label: "|spread| 0-5", max: 5, ss: 0, n: 0 },
			{ label: "|spread| 5-10", max: 10, ss: 0, n: 0 },
			{ label: "|spread| 10-20", max: 20, ss: 0, n: 0 },
			{ label: "|spread| 20+", max: Infinity, ss: 0, n: 0 },
		];
		for (let i = 0; i < sides.length; i++) {
			for (let j = 0; j < sides.length; j++) {
				if (i === j) {
					continue;
				}
				let sum = 0;
				let sumSq = 0;
				let tSum = 0;
				let tSumSq = 0;
				for (let k = 0; k < K; k++) {
					const result: any = new GameSim({
						gid: gid++,
						day: 1,
						teams: helpers.deepCopy([sides[i], sides[j]]) as any,
						doPlayByPlay: false,
						homeCourtFactor: 1,
						neutralSite: false,
						allStarGame: false,
						baseInjuryRate: 0,
					} as any).run();
					const margin = result.team[0].stat.pts - result.team[1].stat.pts;
					const total = result.team[0].stat.pts + result.team[1].stat.pts;
					sum += margin;
					sumSq += margin * margin;
					tSum += total;
					tSumSq += total * total;
				}
				const s0 = meta[i]!.syn;
				const s1 = meta[j]!.syn;
				const pred = getGameSpread({
					ovr0: meta[i]!.ovr,
					ovr1: meta[j]!.ovr,
					homeCourtAdvantage: 1,
					neutralSite: false,
					numPeriods: g.get("numPeriods"),
					quarterLength: g.get("quarterLength"),
					synergyDiff:
						s0 !== undefined && s1 !== undefined ? s0 - s1 : undefined,
				})!;
				rows.push({ i, j, mean: sum / K, pred });

				const ss = sumSq - (sum * sum) / K;
				marginSS += ss;
				marginN += K - 1;
				const bin = sigmaBins.find((b) => Math.abs(pred) < b.max)!;
				bin.ss += ss;
				bin.n += K - 1;
				totalSS += tSumSq - (tSum * tSum) / K;
				totalSum += tSum;
				totalCount += K;
			}
		}

		const mae =
			rows.reduce((s, r) => s + Math.abs(r.pred - r.mean), 0) / rows.length;
		const bias = rows.reduce((s, r) => s + (r.pred - r.mean), 0) / rows.length;
		const noiseFloor = (13 / Math.sqrt(K)) * Math.sqrt(2 / Math.PI);

		const marginSigma = Math.sqrt(marginSS / Math.max(1, marginN));
		const meanTotal = totalSum / Math.max(1, totalCount);
		const totalSigma = Math.sqrt(
			totalSS / Math.max(1, totalCount - rows.length),
		);
		const binSummary = sigmaBins
			.filter((b) => b.n > 0)
			.map((b) => `${b.label}: ${Math.sqrt(b.ss / b.n).toFixed(2)}`)
			.join(", ");

		// Decompose the model's miss into a PERSISTENT per-team part (a_i, the
		// bias that follows a team into every game - what a season-long futures
		// market exposes) and matchup-specific noise that averages out. With both
		// orderings of every pairing simmed, a_i is identified as
		// (sum of residuals with i at home - sum with i away) / (2 (n-1)).
		const nTeams = sides.length;
		const residSumOut = new Array(nTeams).fill(0);
		const residSumIn = new Array(nTeams).fill(0);
		for (const r of rows) {
			const resid = r.pred - r.mean;
			residSumOut[r.i] += resid;
			residSumIn[r.j] += resid;
		}
		const teamBias = residSumOut.map(
			(out, i) => (out - residSumIn[i]!) / (2 * (nTeams - 1)),
		);
		const biasMean = teamBias.reduce((a, b) => a + b, 0) / nTeams;
		const rawBiasVar =
			teamBias.reduce((s, b) => s + (b - biasMean) ** 2, 0) / nTeams;
		// Each a_i estimate carries sim-mean noise; subtract it so the reading is
		// the real spread of team biases, not the measurement's.
		const estNoiseVar =
			(2 * (nTeams - 1) * (marginSigma * marginSigma)) /
			K /
			(2 * (nTeams - 1)) ** 2;
		const persistentSd = Math.sqrt(Math.max(0, rawBiasVar - estNoiseVar));

		const summary =
			`spread calibration: ${rows.length} pairings x ${K} sims - MAE ${mae.toFixed(3)} (noise floor ~${noiseFloor.toFixed(2)}), bias ${bias.toFixed(3)}\n` +
			`  margin sigma ${marginSigma.toFixed(3)} (${binSummary})\n` +
			`  total: mean ${meanTotal.toFixed(1)}, sigma ${totalSigma.toFixed(3)} (${((100 * totalSigma) / meanTotal).toFixed(2)}% of mean)\n` +
			`  persistent per-team model error: ${persistentSd.toFixed(3)} (raw ${Math.sqrt(rawBiasVar).toFixed(3)}, est noise ${Math.sqrt(estNoiseVar).toFixed(3)})`;
		// console.log is swallowed inside vitest, so the reading also lands in a
		// file when one is asked for.
		if (nodeEnv.SPREAD_CALIBRATION_OUT) {
			fs.appendFileSync(nodeEnv.SPREAD_CALIBRATION_OUT, `${summary}\n`);
		}
		console.log(summary);
		// The shipped ovr-only model measured ~2.8 on this style of run; the
		// synergy model ~2.0. Failing at 2.6 means the engine has drifted from the
		// coefficients enough to matter - time to re-fit, not to loosen this.
		assert.ok(
			mae < 2.6,
			`MAE ${mae.toFixed(3)} - coefficients need re-fitting`,
		);
		assert.ok(Math.abs(bias) < 0.75, `bias ${bias.toFixed(3)}`);
	},
);
