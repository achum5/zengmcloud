import getAwardCandidates from "./getAwardCandidates.ts";
import { getPlayers } from "./awards.ts";
import { g, helpers } from "../../util/index.ts";
import { idb } from "../../db/index.ts";
import { PHASE } from "../../../common/constants.ts";
import {
	awardWinProbs,
	cumulativeAwardStats,
	projectedGamesPlayed,
} from "../../../common/awardOdds.ts";
import {
	longshotVig,
	probToAmerican,
	SPORTSBOOK_FUTURES_VIG,
	SPORTSBOOK_MAX_AMERICAN,
} from "../../../common/sportsbook.ts";

// Canonical live award-race odds: the single source of truth for both the Award
// Races view and the Sportsbook's award futures, so the two can never disagree.
//
// See common/awardOdds.ts for why this projects the season forward instead of
// pricing the standings so far. The short version: the award formulas add up
// cumulative production, so at game 10 they are ten games of noise, and a player
// who missed four of them is marked down as though he will miss 40% of the year.

// A copy of a candidate with his cumulative production scaled to the season he
// is on pace to finish. Rates (per game, per 100, PER) are already the right
// scale and are left alone; only the accumulating stats move.
const projectPlayer = (
	p: any,
	{
		teamGpByTid,
		numGames,
		cumulative,
	}: {
		teamGpByTid: Map<number, number>;
		numGames: number;
		cumulative: ReadonlySet<string>;
	},
) => {
	const currentStats = p.currentStats ?? {};
	const gp = currentStats.gp ?? 0;
	const teamGp = teamGpByTid.get(p.tid) ?? gp;

	const projectedGP = projectedGamesPlayed({
		gp,
		teamGp,
		numGames,
		injuryGamesRemaining: p.injury?.gamesRemaining ?? 0,
	});

	// Nothing to project from yet - scaling 0 games by anything is still 0.
	const scale = gp > 0 ? projectedGP / gp : 1;

	const projectedStats: Record<string, any> = { ...currentStats };
	if (scale !== 1) {
		for (const key of Object.keys(projectedStats)) {
			if (cumulative.has(key) && typeof projectedStats[key] === "number") {
				projectedStats[key] *= scale;
			}
		}
	}
	// Some formula terms scale themselves by games played, so the projected
	// player has to report the games he'll finish with.
	projectedStats.gp = projectedGP;

	return {
		...p,
		currentStats: projectedStats,
		// Same for the team: judge a candidate on his team's projected full season
		// rather than on how many games it happens to have played tonight.
		teamInfo: {
			...p.teamInfo,
			gp: numGames,
		},
	};
};

const getAwardRaceOdds = async (season: number) => {
	const live = season === g.get("season") && g.get("phase") <= PHASE.PLAYOFFS;

	let races;
	let fractionComplete = 1;
	const talentByPid = new Map<number, number>();

	if (live) {
		const numGames = g.get("numGames");
		const teamSeasons = await idb.getCopies.teamSeasons(
			{ season },
			"noCopyCache",
		);
		const teamGpByTid = new Map<number, number>();
		let maxTeamGp = 0;
		for (const teamSeason of teamSeasons) {
			const gp = helpers.getTeamSeasonGp(teamSeason);
			teamGpByTid.set(teamSeason.tid, gp);
			if (gp > maxTeamGp) {
				maxTeamGp = gp;
			}
		}
		fractionComplete = numGames > 0 ? Math.min(1, maxTeamGp / numGames) : 1;

		const players = await getPlayers(season);
		for (const p of players) {
			talentByPid.set(p.pid, p.ratings?.ovr ?? 0);
		}

		const cumulative = cumulativeAwardStats();
		const projected = players.map((p) =>
			projectPlayer(p, { teamGpByTid, numGames, cumulative }),
		);

		// The field itself comes from the projection too. Ranking candidates by
		// partial totals didn't just misprice a player who missed games - it could
		// leave him out of the top ten entirely.
		const projectedRaces = await getAwardCandidates(season, projected);

		// Swap the real players back in for display, so the table shows the stats
		// he has actually put up rather than the projected ones.
		const actualByPid = new Map(players.map((p: any) => [p.pid, p]));
		races = projectedRaces.map((row) => ({
			...row,
			players: row.players.map((p: any) => ({
				...(actualByPid.get(p.pid) ?? p),
				awardScore: p.awardScore,
			})),
		}));
	} else {
		races = await getAwardCandidates(season);
	}

	return races.map((row) => {
		const probs = awardWinProbs(
			row.players.map((p: any) => ({
				score: typeof p.awardScore === "number" ? p.awardScore : 0,
				talent: talentByPid.get(p.pid) ?? p.ratings?.ovr ?? 0,
			})),
			{
				fractionComplete,
				seed: `${season}|${row.name}`,
			},
		);

		const players = row.players.map((p: any, i: number) => ({
			...p,
			// Award bets carry the heavier futures hold and the same cap as every
			// other bet. The Award Races page and the Sportsbook both read this, so
			// they show identical (taxed) prices.
			odds: probToAmerican(probs[i] ?? 0, {
				// Longshots carry the heavier hold, so backing the whole back half of
				// a race is no longer close to free money.
				vig: longshotVig(probs[i] ?? 0, SPORTSBOOK_FUTURES_VIG),
				maxAmerican: SPORTSBOOK_MAX_AMERICAN,
			}),
		}));
		return { ...row, players };
	});
};

export default getAwardRaceOdds;
