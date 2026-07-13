import { g } from "../util/index.ts";
import { idb } from "../db/index.ts";
import type { UpdateEvents } from "../../common/types.ts";
import { getLines } from "../core/sportsbook/getLines.ts";
import { settleBets } from "../core/sportsbook/bets.ts";
import { getSyncEngine } from "../core/sync/engineHolder.ts";
import { SPORTSBOOK_PRESEASON_GRANT } from "../../common/sportsbook.ts";

const updateSportsbook = async (
	inputs: unknown,
	updateEvents: UpdateEvents,
) => {
	if (
		updateEvents.includes("firstRun") ||
		updateEvents.includes("gameSim") ||
		updateEvents.includes("newPhase") ||
		updateEvents.includes("playerMovement") ||
		updateEvents.includes("g.userTids") ||
		// Bets placed/settled bump this so the wallet + open bets refresh.
		updateEvents.includes("watchList")
	) {
		// Safety-net settlement: if this device is the one that may write (single
		// player, or the sim authority in a room), catch up any bet whose outcome is
		// already known but that a missed hook didn't settle. Idempotent + no-op
		// when there's nothing to settle.
		const engine = getSyncEngine();
		if (engine === undefined || engine.isAuthority()) {
			try {
				await settleBets();
			} catch (error) {
				console.error("Sportsbook view settlement failed", error);
			}
		}

		const board = await getLines();

		const userTid = g.get("userTid");
		const teams = await idb.cache.teams.getAll();

		const walletFor = (tid: number) => {
			const t = teams.find((team) => team.tid === tid);
			return t?.sportsbook;
		};

		// The device's own team is the one that bets. A team with no wallet yet
		// (e.g. a league imported mid-season, before its first preseason grant) is
		// shown holding the standard grant; it's persisted the first time a bet is
		// placed.
		const sb = walletFor(userTid);
		const wallet = {
			tid: userTid,
			balance: sb?.balance ?? SPORTSBOOK_PRESEASON_GRANT,
			bets: sb?.bets ?? [],
			history: (sb?.history ?? []).slice(0, 40),
		};

		// The human-managed teams' balances, just for fun (a little leaderboard).
		const userTids = new Set(g.get("userTids"));
		const balances = teams
			.filter((t) => userTids.has(t.tid))
			.map((t) => ({
				tid: t.tid,
				balance: t.sportsbook?.balance ?? SPORTSBOOK_PRESEASON_GRANT,
			}))
			.sort((a, b) => b.balance - a.balance);

		return {
			board,
			wallet,
			balances,
			userTid,
			phase: g.get("phase"),
			season: g.get("season"),
		};
	}
};

export default updateSportsbook;
