import { idb } from "../db/index.ts";
import { g, helpers } from "../util/index.ts";
import type { ByConf } from "../../common/types.ts";
import { defineView } from "../util/defineView.ts";
import {
	ORDER_TEAMS_SEASON_ATTRS,
	ORDER_TEAMS_STATS,
	getTiebreakers,
	orderTeams,
} from "../util/orderTeams.ts";
import { season, team } from "../core/index.ts";
import {
	formatAtsRecord,
	getTeamAtsRecords,
} from "../util/getTeamAtsRecords.ts";
import {
	formatOver500Record,
	getTeamOver500Records,
} from "../util/getTeamOver500Records.ts";
import { getStrengthOfSchedule } from "../util/getStrengthOfSchedule.ts";
import type { RouteParams } from "../../ui/router/types.ts";
import { bySport } from "../../common/sportFunctions.ts";
import { validateSeason } from "../util/processInputs.ts";
import {
	getLiveGate,
	recordDeltas,
	streakAndLastTen,
	teamResultsNewestFirst,
} from "../util/liveGameGate.ts";

const processInputs = (params: RouteParams<"standings">) => {
	let type: "conf" | "div" | "league" =
		g.get("numGamesPlayoffSeries").length === 0
			? "league"
			: bySport({
					baseball: "div",
					basketball: "conf",
					football: "div",
					hockey: "div",
				});
	if (
		params.type === "conf" ||
		params.type === "div" ||
		params.type === "league"
	) {
		type = params.type;
	}

	return {
		season: validateSeason(params.season),
		type,
	};
};

export const getMaxPlayoffSeed = async (
	playoffSeason: number,
	playoffsByConf: ByConf,
) => {
	const { numPlayoffTeams, numPlayInTeams } =
		await season.getNumPlayoffTeams(playoffSeason);
	const numTotalPlayoffs = numPlayoffTeams + numPlayInTeams;

	const maxPlayoffSeed =
		playoffsByConf !== false
			? numTotalPlayoffs / playoffsByConf
			: numTotalPlayoffs;
	const maxPlayoffSeedNoPlayIn =
		maxPlayoffSeed -
		2 *
			(playoffsByConf !== false
				? numPlayInTeams / playoffsByConf
				: numPlayInTeams);

	return {
		maxPlayoffSeed,
		maxPlayoffSeedNoPlayIn,
	};
};

export default defineView({
	id: "standings",
	processInputs,
	load: async ({ inputs, updateEvents, prevInputs }) => {
		if (
			updateEvents.has("firstRun") ||
			(inputs.season === g.get("season") && updateEvents.has("gameSim")) ||
			(inputs.season === g.get("season") && updateEvents.has("liveGames")) ||
			inputs.season !== prevInputs?.season ||
			inputs.type !== prevInputs?.type
		) {
			// Games still being played live aren't in the standings yet (see
			// liveGameGate.ts): taken back out of the records shown here.
			const { hidden, games: liveRows } =
				inputs.season === g.get("season")
					? await getLiveGate()
					: { hidden: new Set<number>(), games: new Map() };

			const confs = g.get("confs", inputs.season);
			const divs = g.get("divs", inputs.season);

			const playoffsByConf = await season.getPlayoffsByConf(inputs.season);
			const numPlayoffByes = season.getNumPlayoffByes({
				numPlayoffByes: g.get("numPlayoffByes", inputs.season),
				byConf: playoffsByConf,
			});

			const { maxPlayoffSeed, maxPlayoffSeedNoPlayIn } =
				await getMaxPlayoffSeed(inputs.season, playoffsByConf);

			const pointsFormula = g.get("pointsFormula", inputs.season);
			const usePts = pointsFormula !== "";

			// One load of the season's games, shared by both columns built from them -
			// a season of box scores is not something to read twice per render.
			const games = (
				await idb.merged.games.getAll({ season: inputs.season })
			).filter((game) => !hidden.has(game.gid));
			const atsRecords = await getTeamAtsRecords(inputs.season, games);
			const over500Records = await getTeamOver500Records(inputs.season, games);
			const sosByTid = await getStrengthOfSchedule(inputs.season);

			const otlSetting = !!g.get("otl", inputs.season);
			const deltas =
				liveRows.size === 0
					? new Map()
					: recordDeltas(liveRows.values(), {
							otlSetting,
							divByTid: new Map(
								(await idb.cache.teams.getAll()).map((t) => [t.tid, t.did]),
							),
							confByTid: new Map(
								(await idb.cache.teams.getAll()).map((t) => [t.tid, t.cid]),
							),
						});
			const liveAdjust = <
				T extends Record<string, any> & {
					won: number;
					lost: number;
					tied: number;
					otl: number;
				},
			>(
				tid: number,
				seasonAttrs: T,
			): T => {
				const d = deltas.get(tid);
				if (!d) {
					return seasonAttrs;
				}
				const out: Record<string, any> = { ...seasonAttrs };
				for (const [key, value] of Object.entries(d)) {
					if (typeof out[key] === "number" && key !== "pts") {
						out[key] = Math.max(0, out[key] - (value as number));
					}
				}
				out.winp = helpers.calcWinp(out as any);
				out.pts = team.evaluatePointsFormula(out as any, {
					season: inputs.season,
				});
				if ("ptsPct" in out) {
					out.ptsPct = team.ptsPct({ ...(out as any), season: inputs.season });
				}
				const results = teamResultsNewestFirst(
					games,
					tid,
					hidden,
					otlSetting,
				);
				const { streak, lastTen } = streakAndLastTen(results);
				out.streak = streak;
				out.lastTen = lastTen;
				return out as T;
			};
			const liveAdjustStats = <T extends Record<string, any>>(
				tid: number,
				stats: T,
			): T => {
				const d = deltas.get(tid);
				if (!d || typeof stats.gp !== "number") {
					return stats;
				}
				// Per game averages: back to the totals, the game out, averaged
				// again.
				const gp = Math.max(0, stats.gp - d.gp);
				return {
					...stats,
					gp,
					pts: gp > 0 ? (stats.pts * stats.gp - d.pts) / gp : 0,
					oppPts: gp > 0 ? (stats.oppPts * stats.gp - d.oppPts) / gp : 0,
				};
			};

			const teams = (
				await team.getFormattedAll({
					attrs: ["tid"],
					seasonAttrs: [
						...ORDER_TEAMS_SEASON_ATTRS,
						"ptsPct",
						"wonHome",
						"lostHome",
						"tiedHome",
						"otlHome",
						"wonAway",
						"lostAway",
						"tiedAway",
						"otlAway",
						"lastTen",
						"streak",
						"abbrev",
						"region",
						"name",
						"clinchedPlayoffs",
						"imgURL",
						"imgURLSmall",
					],
					stats: ORDER_TEAMS_STATS,
					season: inputs.season,
					showNoStats: true,
				})
			).map((t) => ({
				...t,
				seasonAttrs: liveAdjust(t.tid, t.seasonAttrs),
				stats: liveAdjustStats(t.tid, t.stats),
				ats: formatAtsRecord(atsRecords.get(t.tid)),
				over500: formatOver500Record(over500Records.get(t.tid)),
				sos: sosByTid.get(t.tid),
				gb: {
					league: 0,
					conf: 0,
					div: 0,
				},
				rank: {
					playoffs: 0,
					league: 0,
					conf: 0,
					div: 0,
				},
			}));

			const orderTeamsOptions = {
				addTiebreakersField: true,
				season: inputs.season,
			};

			const rankingGroups = {
				league: [await orderTeams(teams, teams, orderTeamsOptions)],
				conf: await Promise.all(
					confs.map((conf) =>
						orderTeams(
							teams.filter((t) => t.seasonAttrs.cid === conf.cid),
							teams,
							orderTeamsOptions,
						),
					),
				),
				div: await Promise.all(
					divs.map((div) =>
						orderTeams(
							teams.filter((t) => t.seasonAttrs.did === div.did),
							teams,
							orderTeamsOptions,
						),
					),
				),
			};

			for (const [type, groups] of helpers.entries(rankingGroups)) {
				for (const group of groups) {
					for (const [i, t] of group.entries()) {
						if (!usePts) {
							t.gb[type] =
								i === 0 ? 0 : helpers.gb(group[0]!.seasonAttrs, t.seasonAttrs);
						}
						t.rank[type] = i + 1;
					}
				}
			}

			for (const t of teams) {
				t.rank.playoffs = playoffsByConf ? t.rank.conf : t.rank.league;
				if (t.rank.playoffs > maxPlayoffSeed) {
					t.rank.playoffs = -1;
				}
			}

			// Don't show tiebreakers when everyone is tied 0-0
			let showTiebreakers = false;
			for (const t of teams) {
				if (
					t.seasonAttrs.won > 0 ||
					t.seasonAttrs.lost > 0 ||
					t.seasonAttrs.tied > 0 ||
					t.seasonAttrs.otl > 0
				) {
					showTiebreakers = true;
					break;
				}
			}

			let ties = false;
			let otl = false;
			for (const t of teams) {
				if (t.seasonAttrs.tied > 0) {
					ties = true;
				}
				if (t.seasonAttrs.otl > 0) {
					otl = true;
				}
				if (ties && otl) {
					break;
				}
			}

			const playIn =
				inputs.season === g.get("season")
					? g.get("playIn")
					: rankingGroups.league[0]!.some(
							(t) => t.seasonAttrs.clinchedPlayoffs === "w",
						);

			return {
				confs,
				divs,
				maxPlayoffSeed,
				maxPlayoffSeedNoPlayIn,
				numPlayoffByes,
				playIn,
				playoffsByConf,
				pointsFormula,
				rankingGroups,
				season: inputs.season,
				showTiebreakers,
				ties: season.hasTies(inputs.season) || ties,
				otl: g.get("otl", inputs.season) || otl,
				tiebreakers: getTiebreakers(inputs.season),
				type: inputs.type,
				usePts,
			};
		}
	},
});
