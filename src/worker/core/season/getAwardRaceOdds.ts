import {
	getAwardCandidates,
	type AwardCandidateOptions,
} from "../awards/getAwardCandidates.ts";
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
//
// Awards are user-defined now (core/awards), so a race is whatever individual
// award the league has, scored by its own formula; the score every candidate
// carries is `currentStats.score`.

type RaceOutput = Awaited<ReturnType<typeof getAwardCandidates>>;
export type AwardRace = RaceOutput["awardCandidates"][number][number];
export type AwardRacePlayer = AwardRace["players"][number];

// A race is a union (individual award, named team), so anything added to its
// players has to be added to each member - a plain Omit would collapse the
// union to its common keys and lose actAs and opoyFormula.
type WithPlayers<R, Extra> = R extends { players: (infer P)[] }
	? Omit<R, "players"> & { players: (P & Extra)[] }
	: never;
export type ScoredRace = WithPlayers<AwardRace, { awardScore: number }>;
export type PricedRace = WithPlayers<
	AwardRace,
	{ awardScore: number; odds: number }
>;

const individualRaces = (out: RaceOutput): AwardRace[] =>
	out.awardCandidates.flat().filter((race) => race.numTeams === undefined);

// Two races are the same award when they share a shortName and a group.
export const raceKey = (race: AwardRace): string =>
	`${race.shortName}|${
		race.group === undefined
			? ""
			: race.group.type === "conf"
				? `conf${race.group.cid}`
				: race.group.type === "div"
					? `div${race.group.did}`
					: `series${race.group.tids.join("-")}`
	}`;

export const playerName = (p: { firstName: string; lastName: string }) =>
	`${p.firstName} ${p.lastName}`.trim();

export const playerTalent = (p: AwardRacePlayer): number =>
	p.ratings.at(-1)?.ovr ?? 0;

// Scale every candidate's cumulative production to the season he is on pace
// to finish, in place, before the formulas run. Rates (per game, per 100, PER)
// are already the right scale and are left alone; only the accumulating stats
// move. The team's games and the season fraction the formulas may read move
// with it, so a candidate is judged on a full season rather than on how many
// games his club happens to have played tonight.
export const projectPlayers = ({
	numGames,
}: {
	numGames: number;
}): NonNullable<AwardCandidateOptions["transformPlayers"]> => {
	const cumulative = cumulativeAwardStats();
	return (players, teamInfos) => {
		for (const p of players) {
			const regular = p.currentStats.regularSeason;
			const tid = regular?.tid;
			const teamGp =
				(tid !== undefined ? teamInfos[tid]?.gp : undefined) ??
				regular?.gp ??
				0;
			for (const currentStats of Object.values(p.currentStats) as Record<
				string,
				any
			>[]) {
				if (!currentStats) {
					continue;
				}
				const gp = currentStats.gp ?? 0;
				const projectedGP = projectedGamesPlayed({
					gp,
					teamGp,
					numGames,
					injuryGamesRemaining: p.injury?.gamesRemaining ?? 0,
				});
				// Nothing to project from yet - scaling 0 games by anything is 0.
				const scale = gp > 0 ? projectedGP / gp : 1;
				if (scale !== 1) {
					for (const key of Object.keys(currentStats)) {
						if (cumulative.has(key) && typeof currentStats[key] === "number") {
							currentStats[key] *= scale;
						}
					}
				}
				// Some formula terms scale themselves by games played, so the
				// projected player has to report the games he'll finish with.
				currentStats.gp = projectedGP;
				currentStats.seasonFraction = 1;
				currentStats.teamGp = numGames;
			}
		}
	};
};

const getAwardRaceOdds = async (season: number) => {
	const live = season === g.get("season") && g.get("phase") <= PHASE.PLAYOFFS;

	let races: ScoredRace[];
	let fractionComplete = 1;

	const withScores = (race: AwardRace): ScoredRace => ({
		...race,
		players: race.players.map((p) => ({
			...p,
			awardScore:
				typeof p.currentStats.score === "number" ? p.currentStats.score : 0,
		})),
	});

	if (live) {
		const numGames = g.get("numGames");
		const teamSeasons = await idb.getCopies.teamSeasons(
			{ season },
			"noCopyCache",
		);
		let maxTeamGp = 0;
		for (const teamSeason of teamSeasons) {
			const gp = helpers.getTeamSeasonGp(teamSeason);
			if (gp > maxTeamGp) {
				maxTeamGp = gp;
			}
		}
		fractionComplete = numGames > 0 ? Math.min(1, maxTeamGp / numGames) : 1;

		// The field itself comes from the projection too. Ranking candidates by
		// partial totals didn't just misprice a player who missed games - it
		// could leave him out of the top ten entirely.
		const projected = individualRaces(
			await getAwardCandidates(season, undefined, {
				transformPlayers: projectPlayers({ numGames }),
			}),
		);
		// Swap the real players back in for display, so the table shows the
		// stats he has actually put up rather than the projected ones.
		const actual = new Map(
			individualRaces(await getAwardCandidates(season)).map((race) => [
				raceKey(race),
				new Map(race.players.map((p) => [p.pid, p])),
			]),
		);
		races = projected.map((race): ScoredRace => {
			const actualByPid = actual.get(raceKey(race));
			return {
				...race,
				players: race.players.map((p) => ({
					...(actualByPid?.get(p.pid) ?? p),
					awardScore:
						typeof p.currentStats.score === "number" ? p.currentStats.score : 0,
				})),
			};
		});
	} else {
		races = individualRaces(await getAwardCandidates(season)).map(withScores);
	}

	return races.map((race): PricedRace => {
		const probs = awardWinProbs(
			race.players.map((p) => ({
				score: p.awardScore,
				talent: playerTalent(p),
			})),
			{
				fractionComplete,
				seed: `${season}|${raceKey(race)}`,
			},
		);

		const players = race.players.map((p, i) => ({
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
		return { ...race, players };
	});
};

export default getAwardRaceOdds;
