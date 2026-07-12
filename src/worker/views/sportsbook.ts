import { g } from "../util/index.ts";
import { idb } from "../db/index.ts";
import type { UpdateEvents } from "../../common/types.ts";
import { getLines } from "../core/sportsbook/getLines.ts";
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
		const board = await getLines();

		const userTids = g.get("userTids");
		const teams = await idb.cache.teams.getAll();

		// One wallet per user-managed team. A team with no wallet yet (e.g. a
		// league imported mid-season, before its first preseason grant) is shown
		// as already holding the standard grant, so the book is usable right away;
		// it's actually persisted the first time a bet is placed.
		const wallets = userTids.map((tid) => {
			const t = teams.find((team) => team.tid === tid);
			const sb = t?.sportsbook;
			return {
				tid,
				balance: sb?.balance ?? SPORTSBOOK_PRESEASON_GRANT,
				initialized: sb !== undefined,
				bets: sb?.bets ?? [],
				history: (sb?.history ?? []).slice(0, 40),
			};
		});

		return {
			board,
			wallets,
			userTid: g.get("userTid"),
			phase: g.get("phase"),
			season: g.get("season"),
		};
	}
};

export default updateSportsbook;
