import { PHASE } from "../../common/constants.ts";
import type { NationalSlate, NationalTag } from "../../common/types.ts";
import { slateOf } from "../../common/nationalBroadcasts.ts";
import { isAllStarGame, isTradeDeadline } from "../../common/specialGames.ts";
import type { RouteParams } from "../../ui/router/types.ts";
import { season as seasonCore } from "../core/index.ts";
import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";
import { defineView } from "../util/defineView.ts";
import { validateSeason, validateTeam } from "../util/processInputs.ts";
import { getLiveGate } from "../util/liveGameGate.ts";
import { getPlannedPlayoffGames } from "../util/plannedPlayoffGames.ts";
import { nationalTags } from "./nationalBroadcast.ts";

// THE CALENDAR PAGE: a season's nights, month by month, for a team or the
// whole league. The same month calendar sits on Daily Schedule, Schedule and
// the dashboard, from getCalendarData.

const processInputs = (params: RouteParams<"calendar">) => ({
	season: validateSeason(params.season),
	tid:
		params.team === undefined
			? g.get("userTid")
			: validateTeam(params.team, ["all"]),
});

export type CalendarGame = {
	gid: number;
	date: string;
	// For the Daily Schedule link.
	day: number | undefined;
	homeTid: number;
	awayTid: number;
	playoffs: boolean;
	// A play-in game.
	playIn?: boolean;
	// A playoff game on the plan, not yet on the schedule (no gid of its own,
	// a negative one), and whether it may not be needed.
	planned?: boolean;
	ifNecessary?: boolean;
	// The final, once it's played.
	homePts?: number;
	awayPts?: number;
	overtimes?: number;
	// The network, for a team's national game still to come.
	national?: NationalTag;
};

export type CalendarKeyDate = {
	date: string;
	day: number | undefined;
	// The NBA's special nights too: opening night, Christmas, MLK Day and
	// Rivals Week (see slateOf).
	type: "allStar" | "tradeDeadline" | NationalSlate;
	// The All-Star Game's box score, once it's played.
	gid?: number;
};

export type CalendarData = {
	games: CalendarGame[];
	keyDates: CalendarKeyDate[];
	// The next night to be played, in the current season.
	today: string | undefined;
	// Games on classic numbered days, before the league went on the
	// calendar.
	undated: boolean;
};

// A season's dated games and key dates: every game, or one team's.
export const getCalendarData = async (
	season: number,
	tid: number | "all",
): Promise<CalendarData> => {
	const games: CalendarGame[] = [];
	const keyDates: CalendarKeyDate[] = [];
	let undated = false;
	const forTeam = (homeTid: number, awayTid: number) =>
		tid === "all" || homeTid === tid || awayTid === tid;

	const playoffSeries = await idb.merged.playoffSeries.get(season);
	const playInGids = new Set(
		(playoffSeries?.playIns ?? []).flatMap((playIn) =>
			playIn.flatMap((matchup) => matchup.gids ?? []),
		),
	);

	// A game still being played live hasn't happened yet, and nothing it gave
	// rise to has (see liveGameGate.ts).
	const gate = season === g.get("season") ? await getLiveGate() : undefined;

	// Every regular season night, with its day.
	const regularNights = new Map<string, number | undefined>();

	const played = await idb.merged.games.getAll({ season });
	for (const game of played) {
		if (game.date === undefined) {
			undated = true;
			continue;
		}
		const [home, away] = game.teams;
		if (!game.playoffs && !isAllStarGame(home.tid, away.tid)) {
			regularNights.set(game.date, game.day);
		}
		if (isAllStarGame(home.tid, away.tid)) {
			keyDates.push({
				date: game.date,
				day: game.day,
				type: "allStar",
				gid: game.gid,
			});
			continue;
		}
		if (!forTeam(home.tid, away.tid)) {
			continue;
		}
		games.push({
			gid: game.gid,
			date: game.date,
			day: game.day,
			homeTid: home.tid,
			awayTid: away.tid,
			playoffs: !!game.playoffs,
			...(playInGids.has(game.gid) ? { playIn: true } : {}),
			...(gate?.hidden.has(game.gid)
				? {}
				: {
						homePts: home.pts,
						awayPts: away.pts,
						overtimes: game.overtimes,
					}),
		});
	}

	let today: string | undefined;
	if (season === g.get("season")) {
		today = await seasonCore.getCalendarDate();
		if (gate?.dayCutoff !== undefined) {
			for (const game of gate.games.values()) {
				if (game.day === gate.dayCutoff && game.date !== undefined) {
					today = game.date;
				}
			}
		}
		const playoffs = g.get("phase") === PHASE.PLAYOFFS;
		const schedule = await seasonCore.getSchedule();
		// (One tag object a network, so its logo crosses to the page once.)
		const national = new Map<number, NationalTag>();
		if (tid !== "all") {
			const byNetwork = new Map<string, NationalTag>();
			for (const [gid, tag] of await nationalTags(schedule)) {
				if (!byNetwork.has(tag.network)) {
					byNetwork.set(tag.network, tag);
				}
				national.set(gid, byNetwork.get(tag.network)!);
			}
		}
		for (const game of schedule) {
			if (game.date === undefined) {
				undated = true;
				continue;
			}
			if (
				!playoffs &&
				!isTradeDeadline(game.homeTid, game.awayTid) &&
				!isAllStarGame(game.homeTid, game.awayTid)
			) {
				regularNights.set(game.date, game.day);
			}
			if (gate?.dayCutoff !== undefined && game.day > gate.dayCutoff) {
				continue;
			}
			if (isTradeDeadline(game.homeTid, game.awayTid)) {
				keyDates.push({
					date: game.date,
					day: game.day,
					type: "tradeDeadline",
				});
			} else if (isAllStarGame(game.homeTid, game.awayTid)) {
				keyDates.push({
					date: game.date,
					day: game.day,
					type: "allStar",
				});
			} else if (forTeam(game.homeTid, game.awayTid)) {
				games.push({
					gid: game.gid,
					date: game.date,
					day: game.day,
					homeTid: game.homeTid,
					awayTid: game.awayTid,
					playoffs,
					...(playoffs && playoffSeries?.currentRound === -1
						? { playIn: true }
						: {}),
					...(national.has(game.gid)
						? { national: national.get(game.gid) }
						: {}),
				});
			}
		}

		// The rest of the round's games, on the nights they're planned for
		// (see playoffCalendar.ts).
		if (playoffs) {
			const listed = new Set(
				games
					.filter((game) => game.playoffs)
					.map((game) => `${game.date}:${game.homeTid}:${game.awayTid}`),
			);
			let gid = -1;
			for (const game of await getPlannedPlayoffGames()) {
				if (
					forTeam(game.homeTid, game.awayTid) &&
					!listed.has(`${game.date}:${game.homeTid}:${game.awayTid}`)
				) {
					games.push({
						gid: gid--,
						date: game.date,
						day: undefined,
						homeTid: game.homeTid,
						awayTid: game.awayTid,
						playoffs: true,
						planned: true,
						...(game.playIn ? { playIn: true } : {}),
						...(game.ifNecessary ? { ifNecessary: true } : {}),
					});
				}
			}
		}
	}

	// The special nights, from opening night on.
	const nights = [...regularNights.keys()].sort();
	for (const date of nights) {
		const slate = slateOf(date, nights[0]);
		if (slate !== undefined) {
			keyDates.push({ date, day: regularNights.get(date), type: slate });
		}
	}

	games.sort((a, b) => (a.date < b.date ? -1 : a.date > b.date ? 1 : 0));

	return { games, keyDates, today, undated };
};

// For pages that show the calendar only when the season has dates.
export const getCalendarIfDated = async (
	season: number,
	tid: number | "all",
) => {
	const data = await getCalendarData(season, tid);
	return data.games.length > 0 || data.keyDates.length > 0 ? data : undefined;
};

export default defineView({
	id: "calendar",
	processInputs,
	load: async ({ inputs, updateEvents, prevInputs }) => {
		if (
			!updateEvents.has("firstRun") &&
			!updateEvents.has("gameAttributes") &&
			!updateEvents.has("newPhase") &&
			!(inputs.season === g.get("season") && updateEvents.has("gameSim")) &&
			!(inputs.season === g.get("season") && updateEvents.has("liveGames")) &&
			inputs.season === prevInputs?.season &&
			inputs.tid === prevInputs.tid
		) {
			return;
		}

		return {
			...(await getCalendarData(inputs.season, inputs.tid)),
			nbaCalendar: g.get("nbaCalendar"),
			season: inputs.season,
			tid: inputs.tid,
		};
	},
});
