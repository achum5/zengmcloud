import { idb } from "../db/index.ts";
import { g, helpers } from "../util/index.ts";
import type { UpdateEvents, ViewInput } from "../../common/types.ts";
import { team } from "../core/index.ts";
import { orderBy } from "../../common/utils.ts";
import { PHASE } from "../../common/constants.ts";
import { loadAbbrevs } from "./gameLog.ts";
import getPlayoffsByConf from "../core/season/getPlayoffsByConf.ts";
import { coarsenPlayerForDisplay } from "../../common/coarsenRating.ts";

const updateSeasonPreview = async (
	{ season }: ViewInput<"seasonPreview">,
	updateEvents: UpdateEvents,
	state: any,
) => {
	if (updateEvents.includes("firstRun") || state.season !== season) {
		const NUM_PLAYERS_TO_SHOW = 10;
		const NUM_TEAMS_TO_SHOW = 5;

		const playersRaw = await idb.getCopies.players(
			{
				activeSeason: season,
			},
			"noCopyCache",
		);

		const prevTeamTidsByPid = new Map<number, number>();

		for (const p of playersRaw) {
			const prevTid = p.stats.findLast((row) => row.season === season - 1)?.tid;
			if (prevTid === undefined) {
				continue;
			}

			let currentTid;
			if (
				g.get("season") === season &&
				(g.get("phase") === PHASE.PRESEASON ||
					!p.stats.some((row) => row.season === season))
			) {
				currentTid = p.tid;
			} else {
				currentTid = p.stats.find((row) => row.season === season)?.tid;
			}

			if (currentTid === undefined || currentTid < 0) {
				continue;
			}

			if (currentTid !== prevTid) {
				prevTeamTidsByPid.set(p.pid, prevTid);
			}
		}

		const RATINGS = ["ovr", "pot", "dovr", "dpot", "pos", "skills", "ovrs"];

		const players = await idb.getCopies.playersPlus(playersRaw, {
			attrs: [
				"pid",
				"tid",
				"abbrev",
				"firstName",
				"lastName",
				"age",
				"watch",
				"value",
				"draft",
				"injury",
			],
			ratings: RATINGS,
			season,
			fuzz: true,
			showNoStats: true,
			// Every list on this page is chosen BY rating, and the team ratings are
			// built from these too. Ranking on the display-rounded 0-10 values would
			// make "Top Players" an arbitrary ten of everyone in the same decade, and
			// leave "Improving Players" blind to anyone who didn't happen to cross a
			// boundary. Coarsened for display on the way out instead.
			coarsenRatings: false,
		});

		const playersTopAll = orderBy(players, (p) => p.ratings.ovr, "desc");

		const playersTop = playersTopAll.slice(0, NUM_PLAYERS_TO_SHOW);
		const playersImproving = orderBy(
			players.filter((p) => p.ratings.dovr > 0),
			(p) => p.ratings.ovr + 2 * p.ratings.dovr,
			"desc",
		).slice(0, NUM_PLAYERS_TO_SHOW);
		const playersDeclining = orderBy(
			players.filter((p) => p.ratings.dovr < 0),
			(p) => p.ratings.ovr - 3 * p.ratings.dovr,
			"desc",
		).slice(0, NUM_PLAYERS_TO_SHOW);
		const playersTopRookies = orderBy(
			players.filter((p) => p.draft.year === season - 1),
			(p) => p.ratings.ovr,
			"desc",
		).slice(0, NUM_PLAYERS_TO_SHOW);

		const playersNewTeam = [];
		if (prevTeamTidsByPid.size > 0) {
			const prevAbbrevs = await loadAbbrevs(season - 1);
			for (const p of playersTopAll) {
				const prevTid = prevTeamTidsByPid.get(p.pid);
				if (prevTid !== undefined) {
					playersNewTeam.push({
						...p,
						prevTid,
						prevAbbrev: prevAbbrevs[prevTid] ?? helpers.getAbbrev(prevTid),
					});
					if (playersNewTeam.length === NUM_PLAYERS_TO_SHOW) {
						break;
					}
				}
			}
		}

		const teamSeasonsCurrent = await idb.getCopies.teamSeasons(
			{
				season,
			},
			"noCopyCache",
		);
		const teamSeasonsPrev = await idb.getCopies.teamSeasons(
			{
				season: season - 1,
			},
			"noCopyCache",
		);

		const playersByTid = Map.groupBy(players, (t) => t.tid);

		// These are used when displaying last year's playoff results, so they are for last season
		const numPlayoffRounds = g.get("numGamesPlayoffSeries", season - 1).length;
		const playoffsByConf = await getPlayoffsByConf(season - 1);

		const teamSeasons = teamSeasonsCurrent.map((teamSeason) => {
			const teamPlayers = playersByTid.get(teamSeason.tid) ?? [];

			let ovrStart = teamSeason.ovrStart;

			// Hasn't played first game yet, or old season where ovrStart didn't exist
			ovrStart ??= team.ovr(teamPlayers);

			const teamSeasonPrev = teamSeasonsPrev.find(
				(ts) => ts.tid === teamSeason.tid,
			);
			const ovrPrev = teamSeasonPrev?.ovrEnd ?? ovrStart;
			const dovr = ovrStart - ovrPrev;

			const teamInfoCache = g.get("teamInfoCache")[teamSeason.tid]!;

			const lastSeason = teamSeasonPrev
				? {
						won: teamSeasonPrev.won,
						lost: teamSeasonPrev.lost,
						tied: teamSeasonPrev.tied,
						otl: teamSeasonPrev.otl,
						roundsWonText: helpers.roundsWonText({
							playoffRoundsWon: teamSeasonPrev.playoffRoundsWon,
							numPlayoffRounds,
							playoffsByConf,
						}),
					}
				: undefined;

			return {
				tid: teamSeason.tid,
				abbrev: teamSeason.abbrev ?? teamInfoCache.abbrev,
				region: teamSeason.region ?? teamInfoCache.region,
				name: teamSeason.name ?? teamInfoCache.name,
				ovr: ovrStart,
				dovr,
				players: orderBy(teamPlayers, (p) => p.ratings.ovr, "desc").slice(0, 2),
				lastSeason,
			};
		});

		// Ranking the league's teams by strength is exactly what a league that
		// hides team ratings is hiding - the names alone, in order, give the whole
		// thing away. So these boards don't exist there.
		const hideTeamStrength =
			g.get("hideTeamRatings") || g.get("challengeNoRatings");

		const teamsTop = hideTeamStrength
			? []
			: orderBy(teamSeasons, "ovr", "desc").slice(0, NUM_TEAMS_TO_SHOW);
		const teamsImproving = hideTeamStrength
			? []
			: orderBy(
					teamSeasons.filter((t) => t.dovr > 0),
					"dovr",
					"desc",
				).slice(0, NUM_TEAMS_TO_SHOW);
		const teamsDeclining = hideTeamStrength
			? []
			: orderBy(
					teamSeasons.filter((t) => t.dovr < 0),
					"dovr",
					"asc",
				).slice(0, NUM_TEAMS_TO_SHOW);

		// Everything above ranked on the true ratings; from here on they're only
		// looked at, so round them the way the league displays them.
		const coarse = g.get("hideRatingsOnesDigit");
		const forDisplay = <T extends { ratings: any }>(list: T[]): T[] =>
			coarse ? list.map((p) => coarsenPlayerForDisplay(p, RATINGS)) : list;
		const teamsForDisplay = <T extends { players: any[] }>(list: T[]): T[] =>
			coarse
				? list.map((t) => ({ ...t, players: forDisplay(t.players) }))
				: list;

		return {
			playersDeclining: forDisplay(playersDeclining),
			playersImproving: forDisplay(playersImproving),
			playersNewTeam: forDisplay(playersNewTeam),
			playersTop: forDisplay(playersTop),
			playersTopRookies: forDisplay(playersTopRookies),
			season,
			teamsDeclining: teamsForDisplay(teamsDeclining),
			teamsImproving: teamsForDisplay(teamsImproving),
			teamsTop: teamsForDisplay(teamsTop),
		};
	}
};

export default updateSeasonPreview;
