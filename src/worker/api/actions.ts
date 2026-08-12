import { PHASE } from "../../common/constants.ts";
import { draft, game, season, trade } from "../core/index.ts";
import { idb } from "../db/index.ts";
import { g, helpers, toUI, updateStatus } from "../util/index.ts";
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
		toUI("realtimeUpdate", [[], helpers.leagueUrl(["trade"])], conditions);
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
		const draftPicks = await draft.getOrder();

		if (draftPicks.length === 0) {
			await updateStatus("Idle");
		}
	}
};

const untilPick = async (dpid: number, conditions: Conditions) => {
	await runDraft({ type: "untilPick", dpid }, conditions);
};

const addToTradingBlock = async (
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
	toUI(
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
	const delivered = await game.play(1, conditions, true, gid, true);

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
	await game.play(1, conditions, true, gid);
};

const simToGame = async (gid: number, conditions: Conditions) => {
	const numDays = await season.getDaysLeftSchedule(gid);
	await updateStatus("Playing..."); // For quick UI updating, before game.play
	await game.play(numDays, conditions);
};

export default {
	addToTradingBlock,
	liveGame,
	simGame,
	simToGame,
	tradeFor,
	untilPick,
};
