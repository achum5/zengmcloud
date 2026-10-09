import { g } from "../util/index.ts";
import { idb } from "../db/index.ts";
import { defineView, type ViewArgs } from "../util/defineView.ts";
import type { RouteParams } from "../../ui/router/types.ts";
import { getGameProps } from "../core/sportsbook/getGameProps.ts";
import { SPORTSBOOK_PRESEASON_GRANT } from "../../common/sportsbook.ts";

// The "click into a game" prop board - one specific game's full player/team/
// game props, computed on demand (see getGameProps.ts for why this is kept
// separate from the main sportsbook board).
const processInputs = (params: RouteParams<"sportsbookGame">) => ({
	gid: params.gid !== undefined ? Number.parseInt(params.gid) : -1,
});

const updateSportsbookGame = async ({
	inputs,
	updateEvents,
	prevInputs,
}: ViewArgs<typeof processInputs>) => {
	if (
		inputs.gid !== prevInputs?.gid ||
		updateEvents.has("firstRun") ||
		updateEvents.has("gameSim") ||
		updateEvents.has("newPhase") ||
		updateEvents.has("playerMovement") ||
		updateEvents.has("gameAttributes") ||
		updateEvents.has("watchList")
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

export default defineView({
	id: "sportsbookGame",
	processInputs,
	load: updateSportsbookGame,
});
