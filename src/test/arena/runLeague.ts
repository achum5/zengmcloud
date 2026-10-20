import {
	draft,
	freeAgents,
	game,
	league,
	phase,
	realRosters,
	season,
	team,
} from "../../worker/core/index.ts";
import { idb } from "../../worker/db/index.ts";
import { g, helpers } from "../../worker/util/index.ts";
import { PHASE, PLAYER } from "../../common/constants.ts";
import { getDefaultSettings } from "../../worker/views/newLeague.ts";
import { last } from "../../common/utils.ts";
import { defaultGameAttributes } from "../../common/defaultGameAttributes.ts";
import type { Conditions } from "../../common/types.ts";
import { ARENA_SETTINGS, ARENA_START_SEASON } from "./settings.ts";

const conditions = {} as Conditions;

// What the end-of-run tables cannot reconstruct: things that only exist at a
// moment in the season (payrolls, cap space, who was a free agent).
export type SeasonSnapshot = {
	season: number;
	// End of the regular season.
	payrolls: number[];
	deadMoney: number[];
	// Trades made with at least a week still to go to the deadline, so the rest
	// went in the final week.
	tradesBeforeDeadlineWeek: number;
	tradesAtDeadline: number;
	// Start of free agency.
	capSpace: number[];
	// Every player whose contract ran out this season, as the re-signing phase
	// opened, and where he was once free agency had run its course.
	expiring: {
		pid: number;
		tid: number;
		value: number;
		tidAfter?: number;
	}[];
};

export const createArenaLeague = async (lid: number) => {
	const getLeagueOptions = {
		type: "real" as const,
		season: ARENA_START_SEASON,
		phase: PHASE.PRESEASON,
		randomDebuts: false,
		randomDebutsKeepCurrent: false,
		realDraftRatings: "rookie" as const,
		realStats: "none" as const,
		includePlayers: true,
	};
	const realLeague = await realRosters.getLeague(getLeagueOptions);

	// Passed in at creation, like the new league screen does, so real contracts
	// are scaled to this cap rather than left sized for the default one.
	const defaults = getDefaultSettings();
	const settings: Record<string, unknown> = { ...defaults };
	for (const [key, value] of Object.entries(ARENA_SETTINGS)) {
		if (Object.hasOwn(defaults, key)) {
			settings[key] = value;
		}
	}

	await league.createStream(
		realLeague as any,
		{
			confs: last(defaultGameAttributes.confs).value,
			divs: last(defaultGameAttributes.divs).value,
			fromFile: {
				gameAttributes: realLeague.gameAttributes,
				hasRookieContracts: true,
				maxGid: undefined,
				startingSeason: realLeague.startingSeason,
				teams: realLeague.teams,
				version: undefined,
			},
			getLeagueOptions,
			keptKeys: new Set(["startingSeason", "version"]) as any,
			lid,
			name: "Arena",
			setLeagueCreationStatus: () => {},
			settings,
			shuffleRosters: false,
			startingSeasonFromInput: undefined,
			teamsFromInput: helpers.addPopRank(helpers.getTeamsDefault()),
			tid: 0,
		} as any,
	);
	await league.loadGameAttributes();
	await league.setGameAttributes({
		...ARENA_SETTINGS,
		// Every team is run by the CPU.
		spectator: true,
		// Faces are drawn for the UI only and are slow to generate.
		realisticFaces: false,
	} as any);
};

const countTrades = async () =>
	(
		await idb.getCopies.events(
			{ season: g.get("season") } as any,
			"noCopyCache",
		)
	).filter((e) => e.type === "trade").length;

const payrollsAndDeadMoney = async () => {
	const payrolls: number[] = [];
	const deadMoney: number[] = [];
	for (let tid = 0; tid < g.get("numTeams"); tid++) {
		payrolls.push(await team.getPayroll(tid));
		const released = await idb.cache.releasedPlayers.indexGetAll(
			"releasedPlayersByTid",
			tid,
		);
		deadMoney.push(released.reduce((sum, rp) => sum + rp.contract.amount, 0));
	}
	return { payrolls, deadMoney };
};

// Auto play, one phase at a time and awaited, which is what league/autoPlay.ts
// does - but driven from here so the harness can look at the league between
// phases. It never sets local.autoPlayUntil, so finalize never re-enters.
export const advanceSeasons = async (numSeasons: number) => {
	const snapshots: SeasonSnapshot[] = [];
	const targetSeason = g.get("season") + numSeasons;
	let snapshot: SeasonSnapshot | undefined;

	let guard = 0;
	while (
		!(g.get("season") === targetSeason && g.get("phase") === PHASE.PRESEASON)
	) {
		guard += 1;
		if (guard > numSeasons * 50) {
			throw new Error(
				`Arena stuck at season ${g.get("season")} phase ${g.get("phase")}`,
			);
		}

		const currentPhase = g.get("phase");
		if (currentPhase === PHASE.PRESEASON) {
			snapshot = {
				season: g.get("season"),
				payrolls: [],
				deadMoney: [],
				tradesBeforeDeadlineWeek: 0,
				tradesAtDeadline: 0,
				capSpace: [],
				expiring: [],
			};
			snapshots.push(snapshot);
			await phase.newPhase(PHASE.REGULAR_SEASON, conditions);
		} else if (currentPhase === PHASE.REGULAR_SEASON) {
			const daysToDeadline = await season.getDaysLeftSchedule("tradeDeadline");
			if (daysToDeadline > 7) {
				await game.play(daysToDeadline - 7, conditions);
				snapshot!.tradesBeforeDeadlineWeek = await countTrades();
			}
			await game.play(await season.getDaysLeftSchedule(), conditions);
			snapshot!.tradesAtDeadline = await countTrades();
		} else if (currentPhase === PHASE.AFTER_TRADE_DEADLINE) {
			await game.play(await season.getDaysLeftSchedule(), conditions);
		} else if (currentPhase === PHASE.PLAYOFFS) {
			Object.assign(snapshot!, await payrollsAndDeadMoney());
			await game.play(100, conditions);
		} else if (currentPhase === PHASE.DRAFT_LOTTERY) {
			await phase.newPhase(PHASE.DRAFT, conditions);
		} else if (currentPhase === PHASE.DRAFT) {
			await draft.runPicks({ type: "untilEnd" }, conditions);
			if (g.get("phase") === PHASE.DRAFT) {
				await phase.newPhase(PHASE.AFTER_DRAFT, conditions);
			}
		} else if (currentPhase === PHASE.AFTER_DRAFT) {
			const players = await idb.cache.players.indexGetAll("playersByTid", [
				0,
				Infinity,
			]);
			snapshot!.expiring = players
				.filter((p) => p.contract.exp === g.get("season"))
				.map((p) => ({ pid: p.pid, tid: p.tid, value: p.value }));
			await phase.newPhase(PHASE.RESIGN_PLAYERS, conditions);
		} else if (currentPhase === PHASE.RESIGN_PLAYERS) {
			await phase.newPhase(PHASE.FREE_AGENCY, conditions);
			for (let tid = 0; tid < g.get("numTeams"); tid++) {
				snapshot!.capSpace.push(
					g.get("salaryCap") - (await team.getPayroll(tid)),
				);
			}
		} else if (currentPhase === PHASE.FREE_AGENCY) {
			await freeAgents.play(g.get("daysLeft"), conditions);
			if (g.get("phase") === PHASE.FREE_AGENCY) {
				await phase.newPhase(PHASE.PRESEASON, conditions);
			}
			for (const row of snapshot!.expiring) {
				const p = await idb.cache.players.get(row.pid);
				row.tidAfter = p ? p.tid : PLAYER.RETIRED;
			}
		} else {
			throw new Error(`Unexpected phase ${currentPhase}`);
		}
	}

	return snapshots;
};
