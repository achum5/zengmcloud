import { PHASE } from "../../common/constants.ts";
import type { Conditions, PlayoffSeries } from "../../common/types.ts";
import { season, game, phase, freeAgents } from "../core/index.ts";
import { idb } from "../db/index.ts";
import {
	g,
	updateStatus,
	local,
	helpers,
	updatePlayMenu,
	sim,
} from "../util/index.ts";
import { runDraft } from "./actions.ts";
import { bySport } from "../../common/sportFunctions.ts";
import { cleanupAutoPlay } from "../core/league/autoPlay.ts";
import { realisticFreeAgencyOn } from "../core/realisticFrontOffice/isOn.ts";
import { capDraftReadyForOnePick } from "../core/sync/draftReady.ts";

// On the NBA calendar, a sim day is a night of games: the play-in's nights
// left, from its plan (see playoffCalendar.ts).
const playInNightsLeft = (playoffSeries: PlayoffSeries) => {
	const playIns = playoffSeries.playIns;
	if (!playIns || !playIns.every((playIn) => playIn[0].dates)) {
		return;
	}
	const nights = new Set<string>();
	let lastGamesLeft = false;
	for (const playIn of playIns) {
		for (const matchup of playIn.slice(0, 2)) {
			if (matchup.home.won + matchup.away.won === 0) {
				nights.add(matchup.dates![0]!);
			}
		}
		const final = playIn[2];
		if (!final || final.home.won + final.away.won === 0) {
			lastGamesLeft = true;
		}
	}
	return nights.size + (lastGamesLeft ? 1 : 0);
};

// The round's nights left, from its plan, and the All-Star Game's night when
// it's before the Finals.
const roundNightsLeft = (playoffSeries: PlayoffSeries) => {
	const roundSeries = playoffSeries.series[playoffSeries.currentRound];
	if (!roundSeries?.some((series) => series.dates)) {
		return;
	}
	const numGamesToWin = helpers.numGamesToWinRound(playoffSeries.currentRound);
	const nights = new Set<string>();
	let notStarted = true;
	for (const { away, home, dates } of roundSeries) {
		if (!away) {
			continue;
		}
		if (home.won + away.won > 0) {
			notStarted = false;
		}
		if (Math.max(home.won, away.won) < numGamesToWin) {
			for (const night of dates?.slice(home.won + away.won) ?? []) {
				nights.add(night);
			}
		}
	}
	const allStar =
		notStarted &&
		g.get("allStarGame") === -1 &&
		roundSeries.length === 1 &&
		playoffSeries.currentRound ===
			g.get("numGamesPlayoffSeries", "current").length - 1;
	return nights.size + (allStar ? 1 : 0);
};

const getNumDaysThisRound = (playoffSeries: PlayoffSeries) => {
	let numDaysThisRound = 0;

	if (playoffSeries.currentRound === -1) {
		return playInNightsLeft(playoffSeries) ?? 1;
	}

	const nightsLeft = roundNightsLeft(playoffSeries);
	if (nightsLeft !== undefined) {
		return nightsLeft;
	}

	if (playoffSeries.series.length > 0) {
		const roundSeries = playoffSeries.series[playoffSeries.currentRound];
		if (roundSeries) {
			for (const series of roundSeries) {
				const num = series.away
					? g.get("numGamesPlayoffSeries", "current")[
							playoffSeries.currentRound
						]! -
						series.home.won -
						series.away.won
					: 0;

				if (num > numDaysThisRound) {
					numDaysThisRound = num;
				}
			}
		}
	}

	return numDaysThisRound;
};

const getNumDaysPlayIn = async () => {
	const playoffSeries = await idb.cache.playoffSeries.get(g.get("season"));
	if (!playoffSeries) {
		throw new Error("playoffSeries not found");
	}

	if (playoffSeries.currentRound > -1 || !playoffSeries.playIns) {
		return 0;
	}

	const nightsLeft = playInNightsLeft(playoffSeries);
	if (nightsLeft !== undefined) {
		return nightsLeft;
	}

	let numDays = 0;
	if (
		playoffSeries.playIns.some((playIn) => playIn[0].home.pts === undefined)
	) {
		numDays = 2;
	} else if (
		playoffSeries.playIns.some(
			(playIn) => playIn.length > 2 && playIn[2]?.home.pts === undefined,
		)
	) {
		numDays = 1;
	}

	return numDays;
};

const getNumDaysPlayoffs = async () => {
	const playoffSeries = await idb.cache.playoffSeries.get(g.get("season"));
	if (!playoffSeries) {
		throw new Error("playoffSeries not found");
	}

	// Max 7 days per round that hasn't started yet. On the calendar, where
	// the series of a round are spread over nights, enough nights for a round
	// as long as any it can have (the sim stops when the playoffs end).
	const calendar =
		playInNightsLeft(playoffSeries) !== undefined ||
		roundNightsLeft(playoffSeries) !== undefined;
	let numDaysFutureRounds = 0;
	let numSeries = playoffSeries.series[0]?.length ?? 1;

	for (let i = 0; i < g.get("numGamesPlayoffSeries", "current").length; i++) {
		if (i > playoffSeries.currentRound) {
			const numGames = g.get("numGamesPlayoffSeries", "current")[i]!;
			numDaysFutureRounds += calendar
				? numGames * Math.min(numSeries, 4) + 3
				: numGames;
		}
		numSeries = Math.max(1, Math.ceil(numSeries / 2));
	}

	const numDaysPlayIn = await getNumDaysPlayIn();

	let numDaysAllStar = 0;
	if (g.get("allStarGame") === -1) {
		// All-Star Game will happen right before finals. Would be nice to precisely figure out if it's happened yet or not, but whatever, just add 1
		numDaysAllStar += 1;
	}

	return (
		numDaysAllStar +
		numDaysPlayIn +
		numDaysFutureRounds +
		getNumDaysThisRound(playoffSeries)
	);
};

// `amount` may also be an explicit number of league days, which is how the Auto
// Play scheduler runs a rule set to "sim N days" - the presets are just the
// three the Play Menu offers, and there is nothing special about them.
const playAmount = async (
	amount: "day" | "week" | "month" | "untilPreseason" | number,
	conditions: Conditions,
) => {
	if (sim.isActive()) {
		return;
	}

	let numDays;

	if (typeof amount === "number") {
		numDays = Math.max(1, Math.round(amount));
	} else if (amount === "day") {
		numDays = 1;
	} else if (amount === "week") {
		numDays =
			__SPORT !== "football" || g.get("phase") === PHASE.FREE_AGENCY ? 7 : 1;
	} else if (amount === "month") {
		numDays = bySport({
			football: 4,
			default: 30,
		});
	} else if (amount === "untilPreseason") {
		numDays = g.get("daysLeft");
	} else {
		throw new Error(`Invalid amount: ${amount}`);
	}

	if (g.get("phase") <= PHASE.PLAYOFFS) {
		const numDaysRemaining =
			g.get("phase") === PHASE.PLAYOFFS
				? await getNumDaysPlayoffs()
				: await season.getDaysLeftSchedule();

		if (numDaysRemaining < numDays) {
			numDays = numDaysRemaining;
		}

		await game.play(numDays, conditions);
	} else if (g.get("phase") === PHASE.FREE_AGENCY) {
		if (numDays > g.get("daysLeft")) {
			numDays = g.get("daysLeft");
		}

		await freeAgents.play(numDays, conditions);
	} else if (g.get("phase") === PHASE.DRAFT_LOTTERY) {
		if (
			g.get("repeatSeason")?.type === "playersAndRosters" ||
			g.get("forceHistoricalRosters")
		) {
			await phase.newPhase(PHASE.PRESEASON, conditions);
		}
	}
};

// The sim loop stops after its current day, and then it updates the status and play menu itself. Until then, the play menu needs to stop offering anything.
const playStop = async () => {
	// If no sim is running, there's nothing to update. This matters because a persistent notification (like the changelog shown at startup) can request a stop before any league is loaded, when g isn't set and updating the play menu would throw.
	const stopRequested = sim.requestStop();
	if (stopRequested) {
		await updatePlayMenu();
	}
};

const playMenu = {
	stop: async () => {
		await playStop();
	},
	day: async (param: unknown, conditions: Conditions) => {
		await playAmount("day", conditions);
	},
	// An arbitrary number of league days, for Auto Play's custom-length rules.
	days: async (param: unknown, conditions: Conditions) => {
		const numDays = typeof param === "number" ? param : 1;
		await playAmount(numDays, conditions);
	},
	week: async (param: unknown, conditions: Conditions) => {
		await playAmount("week", conditions);
	},
	month: async (param: unknown, conditions: Conditions) => {
		await playAmount("month", conditions);
	},
	untilAllStarGame: async (param: unknown, conditions: Conditions) => {
		if (g.get("phase") < PHASE.PLAYOFFS) {
			const numDays = await season.getDaysLeftSchedule("allStarGame");
			game.play(numDays, conditions);
		}
	},
	untilTradeDeadline: async (param: unknown, conditions: Conditions) => {
		if (g.get("phase") < PHASE.PLAYOFFS) {
			const numDays = await season.getDaysLeftSchedule("tradeDeadline");
			game.play(numDays, conditions);
		}
	},
	untilPlayoffs: async (param: unknown, conditions: Conditions) => {
		if (g.get("phase") < PHASE.PLAYOFFS) {
			// College: room for the conference tournaments, which are scheduled
			// a round at a time once the regular season ends.
			const numDays =
				(await season.getDaysLeftSchedule()) + (g.get("college") ? 10 : 0);
			game.play(numDays, conditions);
		}
	},
	untilEndOfRound: async (param: unknown, conditions: Conditions) => {
		if (g.get("phase") === PHASE.PLAYOFFS) {
			const playoffSeries = await idb.cache.playoffSeries.get(g.get("season"));
			if (!playoffSeries) {
				throw new Error("playoffSeries not found");
			}
			local.playingUntilEndOfRound = true;
			game.play(getNumDaysThisRound(playoffSeries), conditions);
		}
	},
	untilEndOfPlayIn: async (param: unknown, conditions: Conditions) => {
		if (g.get("phase") === PHASE.PLAYOFFS) {
			const numDays = await getNumDaysPlayIn();

			// local.playingUntilEndOfPlayIn is not needed because we always know how many games to play
			game.play(numDays, conditions);
		}
	},
	throughPlayoffs: async (param: unknown, conditions: Conditions) => {
		if (g.get("phase") === PHASE.PLAYOFFS) {
			const numDays = await getNumDaysPlayoffs();
			game.play(numDays, conditions);
		}
	},
	untilDraft: async (param: unknown, conditions: Conditions) => {
		if (g.get("phase") === PHASE.DRAFT_LOTTERY) {
			await phase.newPhase(PHASE.DRAFT, conditions);
		}
	},
	// `param`: the pick the Draft page showed on the clock, when it sent one -
	// so a click on a page a moment behind the room makes that pick or none,
	// never the one after it.
	onePick: async (param: unknown, conditions: Conditions) => {
		const dpid = typeof param === "number" ? param : undefined;
		// One pick, then the room waits - not on to wherever this team had
		// readied up through (see capDraftReadyForOnePick).
		try {
			await capDraftReadyForOnePick(dpid);
		} catch (error) {
			console.error(error);
		}
		await runDraft({ type: "onePick", dpid }, conditions);
	},
	untilYourNextPick: async (param: unknown, conditions: Conditions) => {
		await runDraft({ type: "untilYourNextPick" }, conditions);
	},
	untilEnd: async (param: unknown, conditions: Conditions) => {
		await runDraft({ type: "untilEnd" }, conditions);
	},
	untilResignPlayers: async (param: unknown, conditions: Conditions) => {
		if (
			g.get("draftType") === "freeAgents" &&
			g.get("phase") === PHASE.DRAFT_LOTTERY
		) {
			await phase.newPhase(PHASE.DRAFT, conditions);
			await phase.newPhase(PHASE.AFTER_DRAFT, conditions);
		}
		if (
			g.get("phase") === PHASE.AFTER_DRAFT ||
			(g.get("phase") === PHASE.DRAFT_LOTTERY &&
				g.get("repeatSeason")?.type === "players")
		) {
			await phase.newPhase(PHASE.RESIGN_PLAYERS, conditions);
		}
	},
	// The UI confirms with getUntilFreeAgencyWarning before calling this
	untilFreeAgency: async (param: unknown, conditions: Conditions) => {
		if (g.get("phase") === PHASE.RESIGN_PLAYERS) {
			await phase.newPhase(PHASE.FREE_AGENCY, conditions);
			await updateStatus(helpers.daysLeft(true));
		}
	},
	untilPreseason: async (param: unknown, conditions: Conditions) => {
		await playAmount("untilPreseason", conditions);
	},
	untilRegularSeason: async (param: unknown, conditions: Conditions) => {
		if (g.get("phase") === PHASE.PRESEASON) {
			await phase.newPhase(PHASE.REGULAR_SEASON, conditions);
		}
	},
	stopAuto: async () => {
		cleanupAutoPlay();
		updatePlayMenu();
		await playStop();
	},
};

// Text for the UI to confirm with before untilFreeAgency, or undefined if there's nothing to warn about
export const getUntilFreeAgencyWarning = async () => {
	if (g.get("phase") !== PHASE.RESIGN_PLAYERS) {
		return;
	}

	const negotiations = await idb.cache.negotiations.getAll();
	const numRemaining = negotiations.length;
	if (numRemaining === 0) {
		return;
	}

	if (realisticFreeAgencyOn()) {
		// Realistic front offices: standing re-signing offers are decided as
		// free agency opens.
		const t = await idb.cache.teams.get(g.get("userTid"));
		const offered = negotiations.filter((n) =>
			t?.offerBoard?.some((offer) => offer.pid === n.pid),
		).length;
		return `Proceed to free agency? ${offered} of your ${numRemaining} unsigned players will decide on your offers now, and the rest will become free agents.`;
	}

	return `Are you sure you want to proceed to free agency while ${numRemaining} of your players remain unsigned? If you do not re-sign them before free agency begins, they will be free to sign with any team${
		g.get("salaryCapType") === "soft"
			? ""
			: ", and you won't be able to go over the salary cap to sign them"
	}.`;
};

export default playMenu;
