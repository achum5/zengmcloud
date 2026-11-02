import { PHASE } from "../../../common/constants.ts";
import { idb } from "../../db/index.ts";
import { g, local } from "../../util/index.ts";
import { player } from "../index.ts";
import { getRosterOrderByPid } from "./rosterAutoSort.basketball.ts";

// A team's roster in the order it will be in when its next game tips off.
//
// The pregame spread reads a lineup off the roster order (see
// pregameLineupSynergyFromPlayers), and the sim re-sorts rosters right before
// a day's games: every CPU team in the regular season, and a user's team that
// keeps its roster sorted, any day (see checkRosterSizes and play.ts's
// cbRunDay). Player values move after every game, so the order saved last
// night is not always the order the game is played in - and a spread quoted
// off it was off the one saved on the game by half a point now and then. Same
// sort, same values, same five.
//
// Basketball only: it is the only sport whose spread reads a lineup.
export const rosterAtTipoff = async <
	T extends { pid: number; rosterOrder: number },
>(
	tid: number,
	players: T[],
): Promise<T[]> => {
	if (__SPORT !== "basketball" || players.length === 0) {
		return players;
	}
	const t = await idb.cache.teams.get(tid);
	const userTeamAndActive =
		g.get("userTids").includes(tid) &&
		!local.autoPlayUntil &&
		!g.get("spectator");
	const resorted = userTeamAndActive
		? !!t?.keepRosterSorted
		: g.get("phase") !== PHASE.PLAYOFFS;
	if (!resorted) {
		return players;
	}
	const rows = await player.formatAll(players, {
		attrs: ["pid", "valueNoPot", "valueNoPotFuzz"],
		ratings: ["pos"],
		season: g.get("season"),
		showNoStats: true,
		showRookies: true,
	});
	const order = getRosterOrderByPid(rows, tid, true);
	return players.map((p) => {
		const rosterOrder = order.get(p.pid);
		return rosterOrder === undefined || rosterOrder === p.rosterOrder
			? p
			: { ...p, rosterOrder };
	});
};
