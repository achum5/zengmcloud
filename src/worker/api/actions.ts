import { PHASE } from "../../common/constants.ts";
import { draft, game, season, trade } from "../core/index.ts";
import { idb } from "../db/index.ts";
import {
	g,
	helpers,
	toUI,
	toUIWithoutWaiting,
	updateStatus,
} from "../util/index.ts";
import type { Conditions, TradeTeams } from "../../common/types.ts";

type TradeForOptions = {
	dpid?: number;
	pid?: number;
	otherDpids?: number[];
	otherPids?: number[];
	tid?: number;
	userDpids?: number[];
	userPids?: number[];
};

const tradeFor = async (arg: TradeForOptions, conditions: Conditions) => {
	let teams: TradeTeams | undefined;

	if (arg.pid !== undefined) {
		const p = await idb.cache.players.get(arg.pid);

		if (!p || p.tid < 0) {
			return;
		}

		// Start new trade for a single player, like a Trade For button
		teams = [
			{
				tid: g.get("userTid"),
				pids: [],
				pidsExcluded: [],
				dpids: [],
				dpidsExcluded: [],
			},
			{
				tid: p.tid,
				pids: [arg.pid],
				pidsExcluded: [],
				dpids: [],
				dpidsExcluded: [],
			},
		];
	} else if (arg.dpid !== undefined) {
		const dp = await idb.cache.draftPicks.get(arg.dpid);

		if (!dp) {
			return;
		}

		// Start new trade for a single player, like a Trade For button
		teams = [
			{
				tid: g.get("userTid"),
				pids: [],
				pidsExcluded: [],
				dpids: [],
				dpidsExcluded: [],
			},
			{
				tid: dp.tid,
				pids: [],
				pidsExcluded: [],
				dpids: [arg.dpid],
				dpidsExcluded: [],
			},
		];
	} else if (
		arg.userPids &&
		arg.userDpids &&
		arg.otherPids &&
		arg.otherDpids &&
		arg.tid !== undefined
	) {
		// Start a new trade with everything specified, from the trading block
		teams = [
			{
				tid: g.get("userTid"),
				pids: arg.userPids,
				pidsExcluded: [],
				dpids: arg.userDpids,
				dpidsExcluded: [],
			},
			{
				tid: arg.tid,
				pids: arg.otherPids,
				pidsExcluded: [],
				dpids: arg.otherDpids,
				dpidsExcluded: [],
			},
		];
	} else if (arg.tid !== undefined) {
		// Start trade with team, like from League Finances
		teams = [
			{
				tid: g.get("userTid"),
				pids: [],
				pidsExcluded: [],
				dpids: [],
				dpidsExcluded: [],
			},
			{
				tid: arg.tid,
				pids: [],
				pidsExcluded: [],
				dpids: [],
				dpidsExcluded: [],
			},
		];
	}

	// Start a new trade based on a list of pids and dpids, like from the trading block
	if (teams) {
		await trade.create(teams);
		toUIWithoutWaiting(
			"realtimeUpdate",
			[[], helpers.leagueUrl(["trade"])],
			conditions,
		);
	}
};

export const runDraft = async (
	action: Parameters<typeof draft.runPicks>[0],
	conditions: Conditions,
) => {
	if (
		g.get("phase") === PHASE.DRAFT ||
		g.get("phase") === PHASE.FANTASY_DRAFT ||
		g.get("phase") === PHASE.EXPANSION_DRAFT
	) {
		await updateStatus("Draft in progress...");
		await draft.runPicks(action, conditions);
	}
};

const untilPick = async (dpid: number, conditions: Conditions) => {
	await runDraft({ type: "untilPick", dpid }, conditions);
};

const addToTradingBlock = (
	// Require at least one of pids or dpids
	param:
		| {
				pids: number[];
				dpids?: number[];
		  }
		| {
				pids?: number[];
				dpids: number[];
		  },
	conditions: Conditions,
) => {
	toUIWithoutWaiting(
		"realtimeUpdate",
		[[], helpers.leagueUrl(["trading_block"]), param],
		conditions,
	);
};

const liveGame = async (gid: number, conditions: Conditions) => {
	await toUI(
		"realtimeUpdate",
		[
			[],
			helpers.leagueUrl(["live_game"]),
			{
				fromAction: true,
			},
		],
		conditions,
	);
	// Awaited, unlike upstream. In a sync room the api layer stamps the room's
	// position when this call RESOLVES - fire-and-forget meant the stamp was
	// written before the game had simulated, so a live sim of the season's final
	// game left the room stamped one day (and one phase) in the past. Every
	// caught-up device then read as "ahead of the room" and ground through
	// full-log replays forever. The page navigation above has already happened,
	// so awaiting costs the UI nothing.
	const delivered = await game.play(1, conditions, gid, true);

	// THE SIM DID NOT HAPPEN, AND THE PAGE IS ALREADY THERE.
	//
	// Navigating first is what makes the button feel instant, but it means every
	// way game.play can decline - the lock held by an auto-play day, the game
	// already played, an illegal roster, the trade deadline, the room's fence -
	// strands the user on a live game page that says "Loading..." and never
	// stops. Each of those explains itself in a toast; this is what stops the
	// screen lying about it. Back to the day's schedule, where the game they
	// wanted is listed with whatever actually became of it.
	if (!delivered) {
		toUI(
			"realtimeUpdate",
			[[], helpers.leagueUrl(["daily_schedule"])],
			conditions,
		);
	}
};

const simGame = async (gid: number, conditions: Conditions) => {
	await game.play(1, conditions, gid);
};

// TODAY'S GAMES WITHOUT A HUMAN TEAM IN THEM, simmed so the user games are
// left to play: every team in userTids sits out - all the teams picked in
// multi team mode, and in a cloud room every league mate's. Each one just as
// "Sim game" sims it (game.play for its gid alone: its own claim on the room's
// fence, published as a single game), one after another. Stops at the first
// one that doesn't go - play has already said why.
const simCpuGames = async (_param: unknown, conditions: Conditions) => {
	const userTids = new Set(g.get("userTids"));
	const isCpuGame = (game: { homeTid: number; awayTid: number }) =>
		game.homeTid >= 0 &&
		game.awayTid >= 0 &&
		!userTids.has(game.homeTid) &&
		!userTids.has(game.awayTid);
	const gids = (await season.getSchedule(true))
		.filter(isCpuGame)
		.map((game) => game.gid);
	for (const gid of gids) {
		const stillToPlay = async () =>
			(await season.getSchedule(true)).some((game) => game.gid === gid);
		if (!(await stillToPlay())) {
			continue;
		}
		await game.play(1, conditions, gid);
		if (await stillToPlay()) {
			break;
		}
	}
};

const simToGame = async (gid: number, conditions: Conditions) => {
	const numDays = await season.getDaysLeftSchedule(gid);
	await updateStatus("Playing..."); // For quick UI updating, before game.play
	await game.play(numDays, conditions);
};

export default {
	addToTradingBlock,
	liveGame,
	simCpuGames,
	simGame,
	simToGame,
	tradeFor,
	untilPick,
};
