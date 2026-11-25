import { season } from "../core/index.ts";
import { getGlobalSettings } from "../util/getGlobalSettings.ts";
import { DEFAULT_OWN_GAME_SIM_CUTOFF_SECONDS } from "../../common/ownGameSim.ts";
import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";
import { defineView, keepType } from "../util/defineView.ts";
import { getTopPlayers, getUpcoming } from "./schedule.ts";
import {
	computeStandingsAsOf,
	getAutoRecapsForDay,
	type RecapDayStandings,
	type RecapGame,
} from "../util/getDayGamesForRecap.ts";
import {
	buildHub,
	toHubRecord,
	type HubComingGame,
} from "./dailyScheduleHub.ts";
import { PHASE } from "../../common/constants.ts";
import { makeResponsiveDropdownOption } from "../../common/makeResponsiveDropdownOption.tsx";
import { env } from "../util/env.ts";
import type { RouteParams } from "../../ui/router/types.ts";
import { validateSeason } from "../util/processInputs.ts";
import {
	isAllStarGame,
	isNoGameDay,
	isOffDay,
	isTradeDeadline,
} from "../../common/specialGames.ts";
import { formatGameDate } from "../../common/calendarDates.ts";
import { getCalendarIfDated } from "./calendar.ts";
import { getLiveGate, liveScheduleRow } from "../util/liveGameGate.ts";
import { nationalTagsPlayed } from "./nationalBroadcast.ts";
import type { Game, NationalTag } from "../../common/types.ts";

const processInputs = (params: RouteParams<"dailySchedule">) => {
	let cid;
	if (params.cid !== undefined && params.cid !== "all") {
		cid = Number.parseInt(params.cid);
		if (Number.isNaN(cid)) {
			cid = undefined;
		}
	}

	if (params.season === "today") {
		return {
			cid,
			day: undefined,
			season: g.get("season"),
			today: true,
		};
	}

	// The most recently played day (current day - 1), resolved by the view.
	if (params.season === "yesterday") {
		return {
			cid,
			day: undefined,
			season: g.get("season"),
			yesterday: true,
		};
	}

	const season = validateSeason(params.season);

	let day = params.day === undefined ? undefined : Number.parseInt(params.day);
	if (Number.isNaN(day)) {
		day = 1;
	}

	return {
		cid,
		day,
		season,
	};
};

// The day actually shown, which can be different than inputs.day, such as when there is no day in the URL
const keepPrevOutput = {
	day: keepType<number>(),
};

export default defineView({
	id: "dailySchedule",
	processInputs,
	keepPrevOutput,
	load: async ({ inputs, updateEvents, prevInputs, prevOutput }) => {
		const currentSeason = g.get("season");

		if (
			updateEvents.has("firstRun") ||
			(inputs.season === currentSeason && updateEvents.has("gameSim")) ||
			updateEvents.has("newPhase") ||
			// A game note was added/edited (e.g. an AI recap filed from this page), so
			// the note under each game card needs to refresh.
			updateEvents.has("notes") ||
			// A game started or finished being played live (see liveGameGate.ts).
			(inputs.season === currentSeason && updateEvents.has("liveGames")) ||
			// A background sim refined the point spreads, so the numbers next to
			// each game changed.
			inputs.season !== prevInputs?.season ||
			inputs.day !== prevOutput.day ||
			inputs.cid !== prevInputs?.cid
		) {
			const process = async (inputsDayOverride?: number) => {
				let isToday = false;

				let day: number;
				if (inputs.today) {
					day = -1;
				} else {
					// What day is it? Get it from URL by default, but that could be undefined
					day = inputsDayOverride ?? inputs.day ?? -1;
					if (day === -1) {
						if (updateEvents.has("firstRun")) {
							// If this is a new load of the view, initialize to the current day (current season) or day 1 (past season)
							day = -1;
						} else if (prevInputs?.day !== undefined) {
							// If this is a refresh and we're moving from day in URL to no day in URL, go to current day (current season) or day 1 (past season)
							day = -1;
						} else if (prevOutput.day !== undefined) {
							// If this is a refresh and we already had a day loaded even with no day in the URL, keep that day the same
							day = prevOutput.day;
						}
					}
				}

				// Games still being played live somewhere read as in progress, and
				// in the playoffs what their tip-off worked out (the next day's
				// games) is not there yet - see liveGameGate.ts.
				const gate =
					inputs.season === currentSeason ? await getLiveGate() : undefined;
				const liveRows = gate ? [...gate.games.values()] : [];

				let schedule;
				if (inputs.season === currentSeason) {
					schedule = await season.getSchedule();
					const dayCutoff = gate?.dayCutoff;
					if (dayCutoff !== undefined) {
						schedule = schedule.filter(
							(game) => game.day === undefined || game.day <= dayCutoff,
						);
					}

					// Today is the day of a game still being played, while nothing
					// past it has been.
					let liveDay: number | undefined;
					for (const game of liveRows) {
						if (
							game.day !== undefined &&
							(liveDay === undefined || game.day < liveDay)
						) {
							liveDay = game.day;
						}
					}
					if (
						liveDay !== undefined &&
						schedule[0]?.day !== undefined &&
						schedule[0].day <= liveDay
					) {
						liveDay = undefined;
					}
					if (liveDay !== undefined) {
						let playedPast = false;
						for (const game of await idb.cache.games.getAll()) {
							if (
								game.season === inputs.season &&
								game.day !== undefined &&
								game.day > liveDay
							) {
								playedPast = true;
								break;
							}
						}
						if (playedPast) {
							liveDay = undefined;
						}
					}

					if (day === -1) {
						if (liveDay !== undefined) {
							day = liveDay;
						} else if (schedule[0]?.day !== undefined) {
							day = schedule[0].day;
						}
					}
					if (day === -1) {
						day = 1;
					}

					if (inputs.yesterday) {
						// The last day played (a day with no games before today has
						// nothing to show - season/offDays.ts).
						const today = schedule[0]?.day;
						let lastPlayedDay: number | undefined;
						await idb.merged.games.forEach(
							{ season: inputs.season },
							(game) => {
								if (
									game.day !== undefined &&
									(today === undefined || game.day < today) &&
									(lastPlayedDay === undefined || game.day > lastPlayedDay)
								) {
									lastPlayedDay = game.day;
								}
							},
						);
						if (lastPlayedDay !== undefined) {
							day = lastPlayedDay;
						} else if (today !== undefined) {
							day = Math.max(1, today - 1);
						}
					}

					const scheduleDay = schedule.filter((game) => game.day === day);
					isToday =
						(!!scheduleDay[0] && schedule[0]!.gid === scheduleDay[0].gid) ||
						(liveDay !== undefined && day === liveDay);
				} else {
					if (day === -1) {
						day = 1;
					}
				}

				// Allowing the games to be filtered by conference is really only particularly useful in large leagues, and on mobile it makes the title bar take up an extra row. So only show it for large league or non-mobile.
				const showCids = g.get("numActiveTeams") >= 60 || !env.mobile;

				const confs = g.get("confs", inputs.season);
				let cid: number | undefined;
				if (showCids && inputs.cid !== undefined) {
					cid = confs.find((conf) => conf.cid === inputs.cid)?.cid;
				}

				let cidFilter:
					| ((teams: [{ tid: number }, { tid: number }]) => boolean)
					| undefined;
				if (cid !== undefined) {
					const tids = new Set(
						(await idb.cache.teams.getAll())
							.filter((t) => t.cid === cid)
							.map((t) => t.tid),
					);

					cidFilter = (teams) => {
						const homeTid = teams[0].tid;
						const awayTid = teams[1].tid;
						return (
							isAllStarGame(homeTid, awayTid) ||
							isTradeDeadline(homeTid, awayTid) ||
							tids.has(awayTid) ||
							tids.has(homeTid)
						);
					};
				}

				// Only keep the whole game for games on this day. For other games, only day and playoffs are needed, for the list of days.
				const games = await idb.merged.games.getAll(
					{ season: inputs.season },
					{
						map: (game) => ({
							day: game.day,
							date: game.date,
							// For the day recap's anchor game, below
							gid: game.gid,
							dayNote: game.dayNote,
							playoffs: game.playoffs,
							// For the hub's "Coming up": who plays the next night.
							tids: [game.teams[0].tid, game.teams[1].tid] as const,
							completedGame:
								game.day === day && (!cidFilter || cidFilter(game.teams))
									? game
									: undefined,
						}),
					},
				);

				const daysAndPlayoffs = new Map<number, boolean>();
				// How many games each night has, for the week strip, and who
				// plays in them, for the hub's "Coming up".
				const gamesByDay = new Map<
					number,
					{ gid: number; homeTid: number; awayTid: number }[]
				>();
				const addToDay = (
					day: number,
					game: { gid: number; homeTid: number; awayTid: number },
				) => {
					const list = gamesByDay.get(day);
					if (list) {
						list.push(game);
					} else {
						gamesByDay.set(day, [game]);
					}
				};
				// On a calendar, the night of each day. A trade deadline day shares
				// its night with that night's games, so it's named instead.
				const dateByDay = new Map<number, string>();
				const deadlineDays = new Set<number>();
				for (const game of games) {
					if (game.day !== undefined) {
						addToDay(game.day, {
							gid: game.gid,
							homeTid: game.tids[0],
							awayTid: game.tids[1],
						});
						daysAndPlayoffs.set(game.day, !!game.playoffs);
						if (game.date !== undefined) {
							dateByDay.set(game.day, game.date);
						}
					}
				}

				if (schedule) {
					const isPlayoffs = g.get("phase") === PHASE.PLAYOFFS;

					for (const game of schedule) {
						if (game.day !== undefined) {
							if (!isNoGameDay(game.homeTid, game.awayTid)) {
								addToDay(game.day, {
									gid: game.gid,
									homeTid: game.homeTid,
									awayTid: game.awayTid,
								});
							}
							daysAndPlayoffs.set(game.day, isPlayoffs);
							if (game.date !== undefined) {
								dateByDay.set(game.day, game.date);
							}
							if (isTradeDeadline(game.homeTid, game.awayTid)) {
								deadlineDays.add(game.day);
							}
						}
					}
				}

				const completed: (Game & { national?: NationalTag })[] = games
					.map((game) => game.completedGame)
					.filter((game) => game !== undefined)
					.filter((game) => !gate?.hidden.has(game.gid));

				let upcoming: (Awaited<ReturnType<typeof getUpcoming>>[number] & {
					live?: true;
				})[] = [];
				if (inputs.season === currentSeason) {
					// If it's the current season, get any upcoming games
					upcoming =
						gate?.dayCutoff !== undefined && day > gate.dayCutoff
							? []
							: await getUpcoming({
									cid,
									day,
									withTotals: true,
								});
					upcoming = [
						...liveRows
							.filter(
								(game) =>
									game.day === day && (!cidFilter || cidFilter(game.teams)),
							)
							.map(liveScheduleRow),
						...upcoming,
					];
				}

				const cids = showCids
					? [
							{
								key: "all",
								label: makeResponsiveDropdownOption(
									"All confs",
									"All conferences",
								),
							},
							...confs.map((conf) => {
								// Shorten "Eastern Conference" to "Eastern Conf" on mobile
								const label = conf.name.endsWith("onference")
									? makeResponsiveDropdownOption(
											conf.name.slice(0, -"erence".length),
											conf.name,
										)
									: conf.name;

								return {
									key: conf.cid,
									label,
								};
							}),
						]
					: [];

				const days = Array.from(daysAndPlayoffs.entries())
					.map(([day, playoffs]) => ({ day, playoffs }))
					.sort((a, b) => a.day - b.day)
					.map(({ day, playoffs }) => {
						const date = dateByDay.get(day);
						const name =
							date === undefined
								? `${day}`
								: deadlineDays.has(day)
									? "Trade deadline"
									: formatGameDate(date);
						return {
							key: day,
							label: playoffs ? `${name} (playoffs)` : name,
						};
					});

				if (inputs.season !== currentSeason) {
					// Add team branding info, in case that was different in past season. Otherwise, ScoreBox uses teamInfoCache for latest values
					for (const [i, game] of completed.entries()) {
						completed[i] = { ...game };

						for (const t of game.teams) {
							const teamSeason = await idb.merged.teamSeasons.getByTidSeason(
								t.tid,
								game.season,
							);

							if (teamSeason) {
								t.branding = {
									region: teamSeason.region,
									name: teamSeason.name,
									abbrev: teamSeason.abbrev,
									imgURL: teamSeason.imgURL,
									imgURLSmall: teamSeason.imgURLSmall,
								};
							}
						}
					}
				}

				// Put the user's own team's game(s) at the top of each list, so you never
				// have to hunt for yours. Stable: everything else keeps its order.
				const userTids = new Set(g.get("userTids"));
				const isMine = (game: { teams: { tid: number }[] }) =>
					game.teams.some((t) => userTids.has(t.tid));
				const userFirst = (a: any, b: any) =>
					(isMine(b) ? 1 : 0) - (isMine(a) ? 1 : 0);
				completed.sort(userFirst);
				upcoming.sort(userFirst);

				// The national games' networks, as on each game's own page.
				if (__SPORT === "basketball" && completed.length > 0) {
					const national = await nationalTagsPlayed(
						completed.map((game) => game.gid),
						inputs.season,
					);
					for (const [i, game] of completed.entries()) {
						const tag = national.get(game.gid);
						if (tag) {
							completed[i] = { ...game, national: tag };
						}
					}
				}

				// Every completed game gets an automatic, procedural recap (headline +
				// a couple of fact-anchored paragraphs) shown under its card, and the
				// whole day gets an auto day recap. Both are generated fresh here
				// (deterministic, never stored) and only fill in where there's no real
				// note - a filed AI/manual recap always wins. The "Copy AI Prompt" flow
				// reads the database note/dayNote, which these never touch, so it stays
				// available as the on-demand upgrade.
				// (Nothing about the day while one of its games is still going.)
				const dayStillPlaying = liveRows.some((game) => game.day === day);
				let autoDayRecap = "";
				let recapGames: RecapGame[] = [];
				let standings: RecapDayStandings | undefined;
				if (completed.length > 0 && !dayStillPlaying) {
					const recaps = await getAutoRecapsForDay({
						season: inputs.season,
						day,
					});
					const { notes, dayRecap } = recaps;
					autoDayRecap = dayRecap;
					recapGames = recaps.games;
					standings = recaps.standings;
					for (const [i, game] of completed.entries()) {
						if (!game.note && notes[game.gid]) {
							completed[i] = { ...game, note: notes[game.gid] };
						}
					}
				}

				// The day's "Day in the League" recap is stored on its anchor game (the
				// lowest-gid game of the day - see Game.dayNote / setNote). Resolve the
				// anchor from ALL of the day's games, NOT the possibly conference-filtered
				// `completed`, so it matches the write side (which sees every game) no
				// matter which conference is being viewed.
				const dayGames = games.filter((game) => game.day === day);
				const anchorGame =
					dayGames.length > 0
						? dayGames.reduce((a, b) => (a.gid <= b.gid ? a : b))
						: undefined;
				// A filed "Day in the League" recap wins; otherwise fall back to the
				// auto day recap (never persisted).
				const dayNote = dayStillPlaying
					? undefined
					: (anchorGame?.dayNote ?? (autoDayRecap || undefined));

				// A night still to be played: the standings going into it (the
				// regular season's only - the playoffs have their bracket).
				const dayIsPlayoffs = daysAndPlayoffs.get(day) ?? false;
				if (
					__SPORT === "basketball" &&
					!standings &&
					!dayIsPlayoffs &&
					upcoming.length > 0
				) {
					// (Not counting a game still being played live.)
					standings = await computeStandingsAsOf(
						inputs.season,
						day - 1,
						(await idb.merged.games.getAll({ season: inputs.season })).filter(
							(game) => !gate?.hidden.has(game.gid),
						),
					);
				}

				// Every night of the season, for the week strip, and the user's
				// team's game in it.
				const userTid = g.get("userTid");
				const nights = Array.from(daysAndPlayoffs.entries())
					.map(([nightDay, playoffs]) => {
						const mine = gamesByDay
							.get(nightDay)
							?.find(
								(game) => game.homeTid === userTid || game.awayTid === userTid,
							);
						return {
							day: nightDay,
							date: dateByDay.get(nightDay),
							numGames: gamesByDay.get(nightDay)?.length ?? 0,
							playoffs,
							deadline: deadlineDays.has(nightDay),
							...(mine
								? {
										mine: {
											home: mine.homeTid === userTid,
											opp:
												mine.homeTid === userTid ? mine.awayTid : mine.homeTid,
										},
									}
								: {}),
						};
					})
					.sort((a, b) => a.day - b.day);

				// The next night with games, for "Coming up", with each team's
				// record and the line when it's still to be played.
				const nextNight = nights.find(
					(night) => night.day > day && night.numGames > 0,
				);
				let nextGames: HubComingGame[] = nextNight
					? (gamesByDay.get(nextNight.day) ?? [])
					: [];
				if (
					__SPORT === "basketball" &&
					nextNight &&
					nextGames.length > 0 &&
					inputs.season === currentSeason
				) {
					const lines = new Map(
						(await getUpcoming({ day: nextNight.day })).map((game) => [
							game.gid,
							game,
						]),
					);
					nextGames = nextGames.map((game) => {
						const line = lines.get(game.gid);
						if (!line) {
							return game;
						}
						const [home, away] = line.teams;
						return {
							...game,
							spread: line.spread,
							records:
								"won" in home && "won" in away
									? [toHubRecord(home), toHubRecord(away)]
									: undefined,
						};
					});
				}

				return {
					cid,
					cids,
					completed,
					hub: {
						...buildHub({ games: recapGames, standings }),
						next: nextNight
							? {
									day: nextNight.day,
									date: nextNight.date,
									games: nextGames,
								}
							: undefined,
					},
					nights,
					// A day with no games (season/offDays.ts), still to come.
					offDay:
						schedule?.some(
							(game) =>
								game.day === day && isOffDay(game.homeTid, game.awayTid),
						) ?? false,
					// The night the league is on, for the strip's "today".
					todayDay:
						inputs.season === currentSeason ? schedule?.[0]?.day : undefined,
					date: dateByDay.get(day),
					day,
					dayNote,
					days,
					isToday,
					upcoming,
				};
			};

			let info = await process();

			if (
				info.completed.length === 0 &&
				info.upcoming.length === 0 &&
				!info.offDay &&
				info.days.length > 0
			) {
				const dayAbove = info.days.find(({ key }) => key > info.day);

				const newDay = dayAbove ? dayAbove.key : info.days.at(-1)!.key;

				// No games at requested day, so just use the last day we actually have games for
				info = await process(newDay);
			}

			const topPlayers = await getTopPlayers(undefined, 1, info.day);

			return {
				...info,
				calendar: await getCalendarIfDated(inputs.season, "all"),
				// So the UI can grey the sim buttons with the SAME rule the worker
				// enforces, instead of offering a button that is then refused.
				ownGameSimCutoffSeconds:
					(await getGlobalSettings()).ownGameSimCutoffSeconds ??
					DEFAULT_OWN_GAME_SIM_CUTOFF_SECONDS,
				elam: g.get("elam"),
				elamASG: g.get("elamASG"),
				season: inputs.season,
				ties: season.hasTies("current"),
				topPlayers,
			};
		}
	},
});
