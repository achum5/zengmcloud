import type { Game } from "../../common/types.ts";
import { idb } from "../db/index.ts";
import { g } from "./index.ts";
import { isSport } from "../../common/sportFunctions.ts";
import { MARGIN_SIGMA } from "../../common/sportsbookOdds.ts";

// THE PART OF A TEAM THE FORMULA CANNOT SEE.
//
// getGameSpread prices a game from two things: the gap in team overall and the
// gap in lineup synergy. Measured against the engine on two real leagues'
// rosters - every pairing, sixty sims each - that model lands 2.03 points from
// the engine's true expected margin, against a 1.33-point measurement floor.
// So about 1.5 points of real error, and the whole of it is a FIXED PER-TEAM
// offset: the pairing residual decomposes into 1.38 per team (1.95 once two
// teams meet) plus 1.66 of sim noise, which is the entire 2.50 observed. There
// is essentially no matchup-specific component - interaction terms between one
// team's offence and the other's defence were tried and bought nothing.
//
// Nothing about the roster predicts that offset. Every rotation-weighted
// composite the engine uses (defence, rebounding, shooting, usage, fouling,
// pace...), star power and bench depth were each regressed against it, and
// none correlates above |r| = 0.25 in both leagues - fitting them anyway drove
// cross-league error from 2.04 to 3.24, which is memorising thirty teams
// rather than learning the engine. It is the dynamic half of the sim -
// rotations, fatigue, foul trouble - and no pregame closed form over these
// inputs expresses it.
//
// But it is perfectly visible in RESULTS. A team the model underrates beats
// its number game after game, so the offset can simply be measured: average
// how far each team's actual margin ran past the model's, over the games it
// has played. Out of sample - estimating from one half of a league's pairings
// with a single noisy game each, then pricing the other half - that takes the
// error from 2.04 to 1.93 at twenty games and 1.86 at forty. Which is more
// than double what the best roster-feature model managed, and unlike that one
// it holds up on a league it was not fitted to.
//
// This is also what the futures board already does (see getLines' ratingOf,
// which blends the model rating with real point differential). Game lines were
// the odd ones out, priced purely off the formula while the futures built on
// them knew better.

// How many games of evidence it takes to trust the measurement half way.
//
// Not a feel: a team's offset has spread BIAS_SD and one game's margin carries
// MARGIN_SIGMA of noise, so the mean of n games deserves weight
// n / (n + (sigma/biasSd)^2). At the measured 13.1 and 1.38 that constant is
// about 90, so twenty games move the number a fifth of the way and a full
// season a little under half. Being slow here is deliberate - an over-eager
// correction just feeds this season's coin flips back into next week's line.
export const SPREAD_BIAS_SD = 1.38;

export const spreadBiasPriorGames = (): number =>
	(MARGIN_SIGMA / SPREAD_BIAS_SD) ** 2;

// The correction is capped so a freak run can never turn into an absurd line.
// Three standard deviations of the measured spread of true offsets - a team
// genuinely out there exists, a team four points past it does not.
export const MAX_SPREAD_BIAS = 3 * SPREAD_BIAS_SD;

export type SpreadBiasEntry = { bias: number; games: number };

// Per-team correction in points, to ADD to the model's home-perspective spread
// for that team (and subtract when it is the away side).
export const getTeamSpreadBias = async (
	season: number,
	// Callers holding the season's games already (getLines builds ATS records
	// from the same sweep) pass them rather than re-reading every box score.
	preloadedGames?: Game[],
): Promise<Map<number, SpreadBiasEntry>> => {
	const out = new Map<number, SpreadBiasEntry>();
	if (!isSport("basketball")) {
		// The coefficients, the 1.38 and the whole measurement behind this are
		// basketball's. Other sports keep the plain formula.
		return out;
	}

	// The games cache holds exactly this season's games (see Cache.ts), which is
	// exactly the window this measures - and reading it costs no transaction,
	// which matters because the sim refreshes this every simulated day. A past
	// season isn't in the cache and isn't worth a DB sweep: the correction only
	// ever prices games that haven't been played yet.
	const games =
		preloadedGames ??
		(season === g.get("season")
			? ((await idb.cache.games.getAll()) as Game[])
			: []);

	const sum = new Map<number, number>();
	const count = new Map<number, number>();
	const add = (tid: number, residual: number) => {
		sum.set(tid, (sum.get(tid) ?? 0) + residual);
		count.set(tid, (count.get(tid) ?? 0) + 1);
	};

	for (const game of games) {
		const home = game.teams[0];
		const away = game.teams[1];
		if (home.tid < 0 || away.tid < 0) {
			continue; // All-Star / other special games
		}
		// The MODEL's number, never the quoted one: measuring against a line that
		// already carries the correction would drive the correction to zero and
		// hide the very thing it exists to find. Games from before the split kept
		// only the one number, and for those the two were the same thing.
		const model = game.spreadModel ?? game.spread;
		if (model === undefined) {
			continue; // legacy game with no stored spread - nothing to measure
		}

		// How far the home side ran past what was expected of it. The away side's
		// is the same number the other way up.
		const residual = home.pts - away.pts - model;
		add(home.tid, residual);
		add(away.tid, -residual);
	}

	const prior = spreadBiasPriorGames();
	for (const [tid, total] of sum) {
		const games2 = count.get(tid) ?? 0;
		const shrunk = total / (games2 + prior);
		out.set(tid, {
			bias: Math.max(-MAX_SPREAD_BIAS, Math.min(MAX_SPREAD_BIAS, shrunk)),
			games: games2,
		});
	}
	return out;
};

// The points to add to a home-perspective spread for this matchup. Pure so the
// pricer, the schedule page and the sim all apply it identically.
export const spreadBiasAdjustment = (
	biases: Map<number, SpreadBiasEntry> | undefined,
	homeTid: number,
	awayTid: number,
): number => {
	if (!biases) {
		return 0;
	}
	return (biases.get(homeTid)?.bias ?? 0) - (biases.get(awayTid)?.bias ?? 0);
};
