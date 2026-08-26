// SPREAD FEATURE SEARCH - what else about a roster does the engine reward?
//
// spreadCalibration.test.ts measures how well the SHIPPED formula tracks the
// engine. This one asks the next question: the shipped model misses by ~2.0
// points MAE against a 1.34 noise floor, and 1.3 of that is a PERSISTENT
// per-team bias, so some teams are systematically mispriced. That can only be
// a roster property the model cannot see - overall and lineup synergy are the
// only two things it reads.
//
// Rather than guess which property, this dumps a dataset: every pairing's
// engine margin plus a battery of candidate features per team. The fitting and
// the cross-league validation happen offline against the dump, so one six-
// minute run answers as many model questions as you care to ask.
//
// WHAT IT ANSWERED, so nobody has to run it again to find out:
//
//   - The shipped coefficients are already the best a linear model in these
//     features can do. Refitting ovr + synergy from scratch lands on the same
//     numbers (MAE 2.026 / 2.039 shipped, 2.029 / 2.042 refit).
//   - No composite helps. Rotation-weighted defence, rebounding, shooting,
//     usage, fouling, pace, star power and bench depth were all offered;
//     greedy selection bought 0.078 points of MAE with coefficients that
//     swung twofold between the two leagues, and fitting all of them at once
//     took cross-league error from 2.04 to 3.24. Thirty teams per league is
//     not enough to fit them, and there is nothing there to fit.
//   - Neither do matchup terms. Offence-against-defence interactions (three
//     point shooting vs perimeter defence, rim finishing vs blocking, and so
//     on) are all within noise of nothing, alone or together.
//   - The residual decomposes cleanly: per-team 1.38, sim noise 1.66, which
//     accounts for the whole 2.50 observed. It is a fixed per-team offset and
//     it is invisible to rosters - the best correlate of it among every
//     feature here is |r| = 0.25, and only in one league.
//
// Which is what sent the correction to results instead - see
// worker/util/getTeamSpreadBias.ts.
//
// Env-gated exactly like spreadCalibration: SPREAD_FIT_ROSTERS points at a
// real league's exported rosters, SPREAD_FIT_OUT at where to write the JSON.
import { test } from "vitest";
import { resetG } from "../../../test/helpers.ts";
import { g, helpers } from "../../util/index.ts";
import GameSim from "../GameSim.ts";
import { processTeam } from "./loadTeams.ts";
import { DEFAULT_PLAY_THROUGH_INJURIES } from "../../../common/constants.ts";
import { COMPOSITE_WEIGHTS } from "../../../common/constants.basketball.ts";
import { isSport } from "../../../common/sportFunctions.ts";
import { pregameLineupSynergy } from "../GameSim.basketball/synergy.ts";

const nodeEnv: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const ROSTERS = nodeEnv.SPREAD_FIT_ROSTERS;
const K = Number(nodeEnv.SPREAD_FIT_SIMS ?? 60);

// The engine's own team rating is the mean over the five men ON THE COURT
// (GameSim.basketball, updateTeamCompositeRatings), so a team's composite is
// best summarised by its starters with the bench discounted - the same
// 70/30 split pregameLineupSynergy uses, for the same reason.
const STARTER_WEIGHT = 0.7;

const unitMean = (players: any[], key: string): number => {
	if (players.length === 0) {
		return 0;
	}
	let sum = 0;
	for (const p of players) {
		sum += p.compositeRating[key] ?? 0;
	}
	return sum / players.length;
};

const compositeFeature = (available: any[], key: string): number => {
	const first = available.slice(0, 5);
	const second = available.slice(5, 10);
	if (second.length === 0) {
		return unitMean(first, key);
	}
	return (
		STARTER_WEIGHT * unitMean(first, key) +
		(1 - STARTER_WEIGHT) * unitMean(second, key)
	);
};

test.skipIf(!ROSTERS || !isSport("basketball"))(
	"dump spread features vs engine margins",
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

		// Feature vector per team. `t.player` is already in rotation order (the
		// sim sorts by rosterOrder), so "available" is that order minus the men
		// who cannot play - exactly the group the engine will put on the floor.
		const compositeKeys = Object.keys(COMPOSITE_WEIGHTS).sort();
		const featureNames = [
			"ovr",
			"syn",
			"top1",
			"top5",
			"bench5",
			...compositeKeys.map((key) => `c_${key}`),
		];
		const features = sides.map((t) => {
			const available = t.player.filter((p: any) => !p.injured);
			const values = [...available]
				.map((p: any) => p.valueNoPot ?? 0)
				.sort((a: number, b: number) => b - a);
			const mean = (xs: number[]) =>
				xs.length === 0 ? 0 : xs.reduce((a, b) => a + b, 0) / xs.length;
			return [
				t.ovr,
				pregameLineupSynergy(t.player) ?? 0,
				values[0] ?? 0,
				mean(values.slice(0, 5)),
				mean(values.slice(5, 10)),
				...compositeKeys.map((key) => compositeFeature(available, key)),
			];
		});

		// Every ordered pairing, so both home and away versions of a matchup are
		// present and the fit's intercept is home-court advantage by symmetry.
		let gid = 1;
		const rows: { i: number; j: number; mean: number; sd: number }[] = [];
		for (let i = 0; i < sides.length; i++) {
			for (let j = 0; j < sides.length; j++) {
				if (i === j) {
					continue;
				}
				let sum = 0;
				let sumSq = 0;
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
					sum += margin;
					sumSq += margin * margin;
				}
				rows.push({
					i,
					j,
					mean: sum / K,
					sd: Math.sqrt(Math.max(0, sumSq / K - (sum / K) ** 2)),
				});
			}
		}

		const out = {
			k: K,
			numPeriods: g.get("numPeriods"),
			quarterLength: g.get("quarterLength"),
			featureNames,
			features,
			rows,
		};
		const path = nodeEnv.SPREAD_FIT_OUT;
		if (path) {
			fs.writeFileSync(path, JSON.stringify(out));
		}
		console.log(
			`spread feature dump: ${rows.length} pairings x ${K} sims, ${featureNames.length} features`,
		);
	},
);
