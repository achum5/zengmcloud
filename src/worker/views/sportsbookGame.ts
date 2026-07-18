import { g } from "../util/index.ts";
import { idb } from "../db/index.ts";
import type { UpdateEvents } from "../../common/types.ts";
import { getGameProps } from "../core/sportsbook/getGameProps.ts";
import { SPORTSBOOK_PRESEASON_GRANT } from "../../common/sportsbook.ts";

// The "click into a game" prop board - one specific game's full player/team/
// game props, computed on demand (see getGameProps.ts for why this is kept
// separate from the main sportsbook board).
const updateSportsbookGame = async (
	inputs: { gid: number },
	updateEvents: UpdateEvents,
) => {
	if (
		updateEvents.includes("firstRun") ||
		updateEvents.includes("gameSim") ||
		updateEvents.includes("newPhase") ||
		updateEvents.includes("playerMovement") ||
		updateEvents.includes("gameAttributes") ||
		updateEvents.includes("watchList")
	) {
		let board: Awaited<ReturnType<typeof getGameProps>>;
		try {
			board = await getGameProps(inputs.gid);
		} catch (error) {
			console.error("Sportsbook game props unavailable", error);
			board = undefined;
		}

		const userTid = g.get("userTid");
		const t = await idb.cache.teams.get(userTid);
		const sb = t?.sportsbook;
		const wallet = {
			tid: userTid,
			balance: sb?.balance ?? SPORTSBOOK_PRESEASON_GRANT,
		};

		return {
			gid: inputs.gid,
			board,
			wallet,
			season: g.get("season"),
		};
	}
};

export default updateSportsbookGame;
