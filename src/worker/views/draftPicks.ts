import { idb } from "../db/index.ts";
import { g, helpers } from "../util/index.ts";
import type { DraftPick, UpdateEvents, ViewInput } from "../../common/types.ts";
import { groupByUnique } from "../../common/utils.ts";
import { addPowerRankingsStuffToTeams } from "./powerRankings.ts";
import { getEstPicks } from "../core/team/ValueChangeCalculator.ts";
import { PLAYER } from "../../common/constants.ts";
import { getTeamOvrOverride } from "../util/delayedTeamOvrs.ts";
import { hideTeamOvr } from "../../common/teamRatings.ts";
import {
	performanceScore,
	projectPicks,
	type PerformanceScore,
} from "./draftPickProjection.ts";

const adjustProjectedPick = ({
	projectedPick,
	numSeasons,
	numTeams,
}: {
	projectedPick: number;
	numSeasons: number;
	numTeams: number;
}) => {
	if (numSeasons <= 0) {
		return projectedPick;
	}

	const averagePick = numTeams / 2;

	const NUM_SEASONS_CUTOFF = 8;

	// Many years in the future, we know basically nothing
	if (numSeasons >= NUM_SEASONS_CUTOFF) {
		return Math.round(averagePick);
	}

	// Before then, average projectedPick and averagePick with increasing weight
	const fractionProjectedPick = 1 - numSeasons / NUM_SEASONS_CUTOFF;
	return Math.round(
		fractionProjectedPick * projectedPick +
			(1 - fractionProjectedPick) * averagePick,
	);
};

export const processDraftPicks = async (draftPicksRaw: DraftPick[]) => {
	const draftPicks = [];

	const teamsRaw = await idb.getCopies.teamsPlus(
		{
			attrs: ["tid", "abbrev", "playThroughInjuries"],
			seasonAttrs: ["lastTen", "won", "lost", "tied", "otl"],
			stats: ["gp", "mov"],
			season: g.get("season"),
			showNoStats: true,
		},
		"noCopyCache",
	);

	const teamsWithRankings = await addPowerRankingsStuffToTeams(
		teamsRaw,
		g.get("season"),
		"regularSeason",
	);

	const teams = groupByUnique(teamsWithRankings, "tid");

	// The team-ratings settings, which this page was ignoring: it gated the Ovr
	// column on challengeNoRatings alone, so a league running "No Visible Team
	// Ratings" kept every original team's overall on screen - the same half of
	// the rule that Frivolities > Team Seasons once got wrong (see
	// common/teamRatings.ts). No page season: this is "the team's rating", not
	// one season's, so the delay counts back from today.
	const { display: teamOvr, ovrs: delayedOvrs } = await getTeamOvrOverride();

	// Whether the power ranking column is safe to show, which before any games
	// have been played it is not: with no results to combine, a power ranking is
	// the team ratings sorted, and this page was printing it in a league that
	// hides them. Same rule as the Power Rankings page, which closes outright in
	// that state - see powerRankingIsJustTeamOvr.
	const noGamesYet = teamsWithRankings.every((t) => t.stats.gp === 0);

	// With team ratings hidden, the projection cannot come from the ratings:
	// before a game is played, a pick projected off the rating rank IS the
	// rating rank. It comes from performance instead - see
	// draftPickProjection.ts - and getEstPicks blends that ordering with the
	// season's record the same way it blends the rating rank.
	const ratingsHidden = hideTeamOvr({
		challengeNoRatings: g.get("challengeNoRatings"),
		hideTeamRatings: g.get("hideTeamRatings"),
	});
	let performance: PerformanceScore[] | undefined;
	if (ratingsHidden) {
		const lastSeason = await idb.getCopies.teamSeasons(
			{ season: g.get("season") - 1 },
			"noCopyCache",
		);
		const byTid = new Map(lastSeason.map((ts) => [ts.tid, ts]));
		performance = teamsWithRankings.map((t) => {
			const ts = byTid.get(t.tid);
			return performanceScore({
				tid: t.tid,
				lastSeason: ts
					? { won: ts.won, lost: ts.lost, tied: ts.tied, otl: ts.otl }
					: undefined,
				ovrNow: t.powerRankings.ovr,
				ovrThen: ts?.ovrEnd ?? ts?.ovrStart,
				avgAge: t.powerRankings.avgAge,
			});
		});
	}

	let estPicksCache: Record<number, number> | undefined;
	// Performance mode projects each season on its own (this season's record
	// counts for less each year out, and the roster's age counts for more).
	const performancePicks = new Map<number, Record<number, number>>();
	const records = new Map(
		teamsWithRankings.map((t) => [
			t.tid,
			{
				won: t.seasonAttrs.won,
				lost: t.seasonAttrs.lost,
				tied: t.seasonAttrs.tied,
				otl: t.seasonAttrs.otl,
			},
		]),
	);

	for (const dp of draftPicksRaw) {
		const t = teams[dp.originalTid];

		let projectedPick;
		if (dp.pick === 0 && typeof dp.season === "number") {
			const numSeasons = dp.season - g.get("season");
			let basePick: number;
			if (performance) {
				let picks = performancePicks.get(numSeasons);
				if (!picks) {
					picks = projectPicks(
						performance,
						records,
						g.get("numGames"),
						numSeasons,
					);
					performancePicks.set(numSeasons, picks);
				}
				basePick = picks[dp.originalTid] ?? teamsWithRankings.length / 2;
			} else {
				if (!estPicksCache) {
					const teamOvrsSorted = teamsWithRankings
						.map((t) => {
							return {
								ovr: t.powerRankings.ovr,
								tid: t.tid,
							};
						})
						.sort((a, b) => b.ovr - a.ovr);
					const { estPicks } = await getEstPicks(teamOvrsSorted);
					estPicksCache = estPicks;
				}
				basePick = estPicksCache[dp.originalTid]!;
			}

			projectedPick = adjustProjectedPick({
				projectedPick: basePick,
				numSeasons,
				numTeams: teamsWithRankings.length,
			});
		}

		// Extra filter at the end is for TypeScript
		const events = (
			await idb.getCopies.events({
				dpid: dp.dpid,
				filter: (event) => event.type === "trade",
			})
		).filter((row) => row.type === "trade");

		let trades;
		if (events.length > 0) {
			trades = events.map((event) => {
				let tid: number = PLAYER.DOES_NOT_EXIST;

				// Which team traded the pick?
				if (event.teams) {
					for (const i of [0, 1] as const) {
						if (
							event.teams[i].assets.some(
								(asset) => (asset as any).dpid === dp.dpid,
							)
						) {
							tid = event.tids[i];
							break;
						}
					}
				}

				return {
					abbrev: helpers.getAbbrev(tid),
					eid: event.eid,
				};
			});
		}

		draftPicks.push({
			...dp,
			abbrev: teams[dp.tid]?.abbrev ?? "???",
			originalAbbrev: t?.abbrev ?? "???",
			avgAge: t?.powerRankings.avgAge,
			ovr: t?.powerRankings.ovr,
			// What that team rated N seasons ago, for a league that shows the
			// delayed number instead of hiding it outright.
			ovrDelayed: t === undefined ? undefined : delayedOvrs.get(t.tid),
			powerRanking: t?.powerRankings.rank ?? Infinity,
			record: {
				won: t?.seasonAttrs.won ?? 0,
				lost: t?.seasonAttrs.lost ?? 0,
				tied: t?.seasonAttrs.tied ?? 0,
				otl: t?.seasonAttrs.otl ?? 0,
			},
			projectedPick,
			trades,
		});
	}

	return { draftPicks, noGamesYet, teamOvr };
};

const updateDraftPicks = async (
	{ abbrev, tid }: ViewInput<"draftPicks">,
	updateEvents: UpdateEvents,
	state: any,
) => {
	if (
		updateEvents.includes("firstRun") ||
		updateEvents.includes("gameSim") ||
		updateEvents.includes("playerMovement") ||
		updateEvents.includes("newPhase") ||
		// Which columns exist depends on the team-ratings settings, so a change to
		// them has to redraw the table rather than leave a stale one up.
		updateEvents.includes("gameAttributes") ||
		abbrev !== state.abbrev
	) {
		const draftPicksRaw = (await idb.cache.draftPicks.getAll()).filter(
			(dp) => dp.tid === tid || dp.originalTid === tid,
		);

		const {
			draftPicks: draftPicksProcessed,
			noGamesYet,
			teamOvr,
		} = await processDraftPicks(draftPicksRaw);

		// Do this after processDraftPicks so processDraftPicks can use the same caches for both
		const draftPicks = [];
		const draftPicksOutgoing = [];
		for (const dp of draftPicksProcessed) {
			if (dp.tid === tid) {
				draftPicks.push(dp);
			} else if (dp.originalTid === tid) {
				draftPicksOutgoing.push(dp);
			}
		}

		return {
			abbrev,
			draftPicks,
			draftPicksOutgoing,
			noGamesYet,
			teamOvr,
			tid,
		};
	}
};

export default updateDraftPicks;
