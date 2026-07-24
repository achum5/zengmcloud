import { helpers } from "../util/index.ts";
import type { ViewInput } from "../../common/types.ts";

// The simmed scrimmage result rides in on the routing context (see
// simIntrasquadGame -> realtimeUpdate). With no game to show - e.g. someone
// navigated here directly - bounce back to the league dashboard.
const updateIntrasquadGame = async ({
	liveSim,
	abbrev,
}: ViewInput<"intrasquadGame">) => {
	if (!liveSim) {
		return {
			redirectUrl: helpers.leagueUrl([]),
		};
	}

	return {
		liveSim,
		abbrev,
	};
};

export default updateIntrasquadGame;
