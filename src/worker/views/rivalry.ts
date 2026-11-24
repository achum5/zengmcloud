import { idb } from "../db/index.ts";
import { g, helpers } from "../util/index.ts";
import { defineView } from "../util/defineView.ts";
import { validateTeam } from "../util/processInputs.ts";
import type { RouteParams } from "../../ui/router/types.ts";
import { getLiveGate, unplaySeries } from "../util/liveGameGate.ts";
import getWinner from "../../common/getWinner.ts";
import type { Game } from "../../common/types.ts";
import { branding, findSeries, roundLabel } from "./playoffSeries.ts";

// TWO TEAMS, ALL TIME (#/rivalry/<team>/<opp>): the regular season record
// between them, season by season, every time they met in the playoffs, and
// their latest games.
//
// The records come from the head-to-head table, which every season keeps;
// the games themselves only as far back as the box scores are kept. A game
// still being played live hasn't happened yet here (liveGameGate.ts).

const processInputs = (params: RouteParams<"rivalry">) => {
	const tid = validateTeam(params.team);
	const opp = validateTeam(params.opp);
	return { tid, opp };
};

// How far back to look for their latest games, in seasons.
const RECENT_SEASONS = 4;
const RECENT_GAMES = 10;

export type RivalrySeason = {
	season: number;
	won: number;
	lost: number;
	tied: number;
	pts: number;
	oppPts: number;
	// The playoff series that season, if they met in one.
	playoffs?: { label: string; winnerTid: number | undefined };
};

export type RivalryMeeting = {
	season: number;
	round: number;
	finals: boolean;
	label: string;
	winnerTid: number | undefined;
	// Games won in it, the first team's then the other's.
	won: number;
	lost: number;
	abbrevs: Record<number, string>;
	imgURLs: Record<number, string | undefined>;
};

export default defineView({
	id: "rivalry",
	processInputs,
	load: async ({ inputs, updateEvents, prevInputs }) => {
		const { tid, opp } = inputs;
		if (
			!updateEvents.has("firstRun") &&
			tid === prevInputs?.tid &&
			opp === prevInputs?.opp &&
			!updateEvents.has("gameSim") &&
			!updateEvents.has("playoffs") &&
			!updateEvents.has("liveGames") &&
			!updateEvents.has("newPhase")
		) {
			return;
		}
		if (tid === opp) {
			return {
				errorMessage: "Pick two different teams.",
			};
		}

		const currentSeason = g.get("season");
		const gate = await getLiveGate();
		const between = (game: Pick<Game, "teams">) => {
			const tids = [game.teams[0].tid, game.teams[1].tid];
			return tids.includes(tid) && tids.includes(opp);
		};

		const teamInfo = async (t: number) => {
			const teamSeason = await idb.merged.teamSeasons.getByTidSeason(
				t,
				currentSeason,
			);
			const info = g.get("teamInfoCache")[t];
			return {
				tid: t,
				abbrev: teamSeason?.abbrev ?? info?.abbrev ?? "",
				region: teamSeason?.region ?? info?.region ?? "",
				name: teamSeason?.name ?? info?.name ?? "",
				imgURL: teamSeason?.imgURL ?? info?.imgURL,
				imgURLSmall: teamSeason?.imgURLSmall ?? info?.imgURLSmall,
				colors: teamSeason?.colors ?? [],
			};
		};
		const teams = [await teamInfo(tid), await teamInfo(opp)] as const;

		// Every time they met in the playoffs.
		const meetings: RivalryMeeting[] = [];
		for (const row of await idb.merged.playoffSeries.getAll()) {
			const series =
				row.season === currentSeason
					? unplaySeries(row.series, gate.games.values())
					: row.series;
			const hit = findSeries(series, tid, opp);
			if (!hit) {
				continue;
			}
			const h = hit.matchup.home;
			const a = hit.matchup.away!;
			if (h.won + a.won === 0 && row.season === currentSeason) {
				// (Not under way yet: the series page has it.)
				continue;
			}
			const toWin = helpers.numGamesToWinRound(hit.round, row.season);
			const winner = h.won >= toWin ? h : a.won >= toWin ? a : undefined;
			const mine = h.tid === tid ? h : a;
			const theirs = mine === h ? a : h;
			const [bh, ba] = [
				await branding(h.tid, row.season),
				await branding(a.tid, row.season),
			];
			meetings.push({
				season: row.season,
				round: hit.round,
				finals: hit.round === series.length - 1,
				label: await roundLabel(
					row.season,
					hit.round,
					series.length,
					h.cid === a.cid ? h.cid : undefined,
				),
				winnerTid: winner?.tid,
				won: mine.won,
				lost: theirs.won,
				abbrevs: { [h.tid]: bh.abbrev, [a.tid]: ba.abbrev },
				imgURLs: {
					[h.tid]: bh.imgURLSmall ?? bh.imgURL,
					[a.tid]: ba.imgURLSmall ?? ba.imgURL,
				},
			});
		}
		meetings.sort((x, y) => y.season - x.season);

		// The regular season, season by season, from the first team's side.
		const lo = Math.min(tid, opp);
		const hi = Math.max(tid, opp);
		const seasons: RivalrySeason[] = [];
		for (const headToHead of await idb.merged.headToHeads.getAll()) {
			const info = headToHead.regularSeason[lo]?.[hi];
			if (!info) {
				continue;
			}
			// (The head-to-head table is from the lower tid's side.)
			const loRow = {
				won: info.won + info.otw,
				lost: info.lost + info.otl,
				tied: info.tied,
				pts: info.pts,
				oppPts: info.oppPts,
			};
			const row =
				lo === tid
					? loRow
					: {
							won: loRow.lost,
							lost: loRow.won,
							tied: loRow.tied,
							pts: loRow.oppPts,
							oppPts: loRow.pts,
						};
			if (headToHead.season === currentSeason) {
				for (const game of gate.games.values()) {
					if (game.playoffs || !between(game)) {
						continue;
					}
					const i = game.teams[0].tid === tid ? 0 : 1;
					const winner = getWinner(game.teams);
					if (winner === -1) {
						row.tied -= 1;
					} else if (winner === i) {
						row.won -= 1;
					} else {
						row.lost -= 1;
					}
					row.pts -= game.teams[i].pts;
					row.oppPts -= game.teams[i === 0 ? 1 : 0].pts;
				}
			}
			if (row.won + row.lost + row.tied === 0) {
				continue;
			}
			const meeting = meetings.find((m) => m.season === headToHead.season);
			seasons.push({
				season: headToHead.season,
				...row,
				playoffs: meeting
					? { label: meeting.label, winnerTid: meeting.winnerTid }
					: undefined,
			});
		}
		// (A season they met only in the playoffs - a play-in, an expansion
		// year - still gets its line.)
		for (const meeting of meetings) {
			if (!seasons.some((s) => s.season === meeting.season)) {
				seasons.push({
					season: meeting.season,
					won: 0,
					lost: 0,
					tied: 0,
					pts: 0,
					oppPts: 0,
					playoffs: { label: meeting.label, winnerTid: meeting.winnerTid },
				});
			}
		}
		seasons.sort((x, y) => y.season - x.season);

		// Their latest games, as far back as box scores go.
		const recent: Game[] = [];
		for (
			let season = currentSeason;
			season > currentSeason - RECENT_SEASONS && recent.length < RECENT_GAMES;
			season--
		) {
			const games = await idb.merged.games.getAll(
				{ season },
				{ filter: (game) => between(game) && !gate.hidden.has(game.gid) },
			);
			games.sort((x, y) => (y.day ?? 0) - (x.day ?? 0) || y.gid - x.gid);
			recent.push(...games);
		}

		return {
			teams,
			seasons,
			meetings,
			recent: recent.slice(0, RECENT_GAMES).map((game) => ({
				gid: game.gid,
				season: game.season,
				day: game.day,
				date: game.date,
				playoffs: !!game.playoffs,
				overtimes: game.overtimes,
				numPeriods: game.numPeriods,
				teams: [
					{ tid: game.teams[0].tid, pts: game.teams[0].pts },
					{ tid: game.teams[1].tid, pts: game.teams[1].pts },
				] as [{ tid: number; pts: number }, { tid: number; pts: number }],
			})),
			userTids: g.get("userTids"),
		};
	},
});
