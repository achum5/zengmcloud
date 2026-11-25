import { PHASE, PLAYER } from "../../common/constants.ts";
import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";
import type { PlayerAward } from "../../common/types.ts";
import { defineView } from "../util/defineView.ts";
import addFirstNameShort from "../util/addFirstNameShort.ts";
import { groupByUnique, last, maxBy } from "../../common/utils.ts";
import { bySport } from "../../common/sportFunctions.ts";
import type { RouteParams } from "../../ui/router/types.ts";
import { helpers } from "../util/index.ts";
import { getSeasonState, validateSeason } from "../util/processInputs.ts";
import { hasCurrentSeasonDraftHappened } from "../../common/seasons.ts";
import { player, team } from "../core/index.ts";
import { local } from "../util/index.ts";
import { redraftValue } from "../util/redraftValue.ts";

const processInputs = (params: RouteParams<"draftHistory">) => {
	let season: number;

	const draftAlreadyHappened = hasCurrentSeasonDraftHappened(getSeasonState());
	const currentSeason = g.get("season");

	if (
		(params.season === undefined || params.season === String(currentSeason)) &&
		!draftAlreadyHappened
	) {
		// View last season by default
		season = currentSeason - 1;
	} else {
		season = validateSeason(params.season);
	}

	if (
		season > currentSeason ||
		(season === currentSeason && !draftAlreadyHappened)
	) {
		// Future draft class
		return {
			redirectUrl: helpers.leagueUrl(["draft_scouting"]),
		};
	}

	return {
		season,
	};
};

export const getDraftTeamsByTid = async (season: number) => {
	const teamsByTid = groupByUnique(
		(
			await team.getFormattedAll({
				attrs: ["tid"],
				seasonAttrs: ["abbrev", "imgURL", "imgURLSmall"],
				season,
			})
		).map((t) => {
			return {
				...t.seasonAttrs,
				tid: t.tid,
			};
		}),
		"tid",
	);

	// Make sure we have all teams, even if missing teamSeason
	for (const [tid, t] of g.get("teamInfoCache").entries()) {
		if (!teamsByTid[tid]) {
			teamsByTid[tid] = {
				tid,
				abbrev: t.abbrev,
				imgURL: t.imgURL,
				imgURLSmall: t.imgURLSmall,
			};
		}
	}

	return teamsByTid;
};

// Also used for draftTeamHistory, which shows the same table for a different set of players
export const DRAFT_HISTORY_STATS = bySport({
	baseball: ["gp", "keyStats", "war"],
	basketball: ["gp", "min", "pts", "trb", "ast", "per", "ws"],
	football: ["gp", "keyStats", "av"],
	hockey: ["gp", "keyStats", "ops", "dps", "ps"],
} as const);

export default defineView({
	id: "draftHistory",
	processInputs,
	load: async ({ inputs }) => {
		// Update every time because anything could change this (unless all players from class are retired)

		const stats = DRAFT_HISTORY_STATS;

		const summaryStat = bySport({
			baseball: "war",
			basketball: "ws",
			football: "av",
			hockey: "ps",
		} as const);

		const playersRaw = await idb.merged.players.getAll(
			g.get("season") === inputs.season
				? // This is guaranteed to work (ignoring God Mode) because no player this season has had a chance to die or retire
					{ tid: [PLAYER.FREE_AGENT, Infinity] }
				: { draftYear: inputs.season },
			{ filter: (p) => p.draft.year === inputs.season },
		);
		const playersAll = await player.formatAll(playersRaw, {
			attrs: [
				"tid",
				"abbrev",
				"draft",
				"pid",
				"firstName",
				"lastName",
				"age",
				"ageAtDeath",
				"hof",
				"watch",
				"awards",
				"born",
				// A player still on a photo rather than a face is the one worth
				// converting, so the table needs to know which rows those are - same
				// reason draftScouting fetches it.
				"imgURL",
				// For the writeup arrow next to each pick: the draft-year section of
				// his note is the piece about being taken here.
				"note",
			],
			ratings: ["ovr", "pot", "skills", "pos", "season"],
			stats,
			showNoStats: true,
			fuzz: true,
			// The "At Draft" columns ARE the scouting report from his draft class, so
			// under the prospects exemption they stay exact.
			prospectSeasonsExact: true,
		});
		const players = playersAll
			.filter((p) => {
				return p.draft.round >= 1 || (p.careerStats.gp ?? 0) > 0;
			})
			.map((p) => {
				const currentPr = last(p.ratings);
				// Peak means peak as a pro. A draft-year row is a projection from
				// before he played a game, and under the prospects exemption it is
				// also on a different scale from the rest (exact, where the others are
				// coarsened to a tens digit), so a straight max would always pick it.
				const proRatings = p.ratings.filter(
					(r: any) => r.season > p.draft.year,
				);
				const peakPr = maxBy(
					proRatings.length > 0 ? proRatings : p.ratings,
					"ovr",
				)!;
				return {
					// Attributes
					pid: p.pid,
					firstName: p.firstName,
					lastName: p.lastName,
					draft: p.draft,
					currentAge: p.age,
					ageAtDeath: p.ageAtDeath,
					currentAbbrev: p.abbrev,
					currentTid: p.tid,
					hof: p.hof,
					imgURL: p.imgURL,
					watch: p.watch,
					awards: p.awards,
					note: p.note,
					awardCounts: {
						allStar: p.awards.filter(
							(award: PlayerAward) => award.type === "All-Star",
						).length,
						mvp: p.awards.filter(
							(award: PlayerAward) =>
								award.type === undefined &&
								award.numTeams === undefined &&
								award.actAs === "mvp" &&
								award.rank === 1,
						).length,
						roy: p.awards.filter(
							(award: PlayerAward) =>
								award.type === undefined &&
								award.numTeams === undefined &&
								award.actAs === "roy" &&
								award.rank === 1,
						).length,
						champ: p.awards.filter(
							(award: PlayerAward) => award.type === "Won Championship",
						).length,
						hof: p.awards.filter(
							(award: PlayerAward) =>
								award.type === "Inducted into the Hall of Fame",
						).length,
					},

					// Ratings
					currentOvr: p.tid !== PLAYER.RETIRED ? currentPr.ovr : null,
					currentPot: p.tid !== PLAYER.RETIRED ? currentPr.pot : null,
					currentSkills: p.tid !== PLAYER.RETIRED ? currentPr.skills : [],
					pos: currentPr.pos,

					peakAge: peakPr.season - p.born.year,
					peakOvr: peakPr.ovr,
					peakPot: peakPr.pot,
					peakSkills: peakPr.skills,

					// Stats
					careerStats: p.careerStats,
				};
			});

		const teamsByTid = await getDraftTeamsByTid(inputs.season);

		// THE REDRAFT: the class again in order of career value, once it has
		// two seasons behind it - and only when its careers are on file from
		// the start, which a class from before a random league began isn't.
		// In basketball that value counts what's still to come and the best
		// seasons twice (redraftValue.ts); elsewhere it's the career total.
		const seasonsPlayed =
			g.get("season") -
			inputs.season -
			(g.get("phase") > PHASE.PLAYOFFS ? 0 : 1);
		const onFile =
			inputs.season >= g.get("startingSeason") - 1 ||
			playersRaw.some((p) =>
				p.stats.some((row) => row.season === inputs.season + 1),
			);
		// (Ties go to the earlier pick, and anyone undrafted after them all.)
		const pickOrder = (draft: { round: number; pick: number }) =>
			draft.round >= 1 ? draft.round * 1000 + draft.pick : Infinity;
		const redraftByPid = new Map<number, number>();
		if (seasonsPlayed >= 2 && onFile) {
			let value = (p: (typeof players)[number]) =>
				(p.careerStats[summaryStat] as number | undefined) ?? -Infinity;
			if (__SPORT === "basketball") {
				const season = g.get("season");
				const phase = g.get("phase");
				let leftThisSeason = phase <= PHASE.PRESEASON ? 1 : 0;
				if (phase === PHASE.REGULAR_SEASON) {
					const teamSeasons = await idb.cache.teamSeasons.indexGetAll(
						"teamSeasonsBySeasonTid",
						[[season], [season, "Z"]],
					);
					const played =
						teamSeasons.reduce(
							(sum, ts) => sum + ts.won + ts.lost + ts.tied + ts.otl,
							0,
						) / Math.max(1, teamSeasons.length);
					leftThisSeason = Math.max(0, 1 - played / g.get("numGames"));
				}
				const ctx = {
					season,
					leftThisSeason,
					numGames: g.get("numGames"),
					ovrMean: local.playerOvrMean,
					ovrStd: local.playerOvrStd,
				};
				// What the table shows (a tens digit when the ones are hidden).
				const shown = (rating: number) =>
					g.get("hideRatingsOnesDigit") && rating <= 10
						? rating * 10 + 4.5
						: rating;
				const rawByPid = new Map(playersRaw.map((p) => [p.pid, p]));
				const values = new Map<number, number>();
				for (const p of players) {
					const raw = rawByPid.get(p.pid);
					if (!raw) {
						continue;
					}
					const ws = new Map<number, number>();
					for (const row of raw.stats) {
						if (!row.playoffs) {
							ws.set(
								row.season,
								(ws.get(row.season) ?? 0) + (row.ows ?? 0) + (row.dws ?? 0),
							);
						}
					}
					values.set(
						p.pid,
						redraftValue(
							{
								bornYear: raw.born.year,
								ws,
								current:
									p.currentOvr !== null && !g.get("challengeNoRatings")
										? {
												ovr: shown(p.currentOvr),
												peakOvr: shown(p.peakOvr),
											}
										: undefined,
							},
							ctx,
						),
					);
				}
				value = (p) => values.get(p.pid) ?? -Infinity;
			}
			players
				.filter((p) => (p.careerStats.gp ?? 0) > 0)
				.sort(
					(a, b) =>
						value(b) - value(a) || pickOrder(a.draft) - pickOrder(b.draft),
				)
				.forEach((p, i) => {
					redraftByPid.set(p.pid, i + 1);
				});
		}

		return {
			players: addFirstNameShort(
				players.map((p) => ({
					...p,
					redraft: redraftByPid.get(p.pid),
				})),
			),
			season: inputs.season,
			stats,
			summaryStat,
			teamsByTid,
		};
	},
});
