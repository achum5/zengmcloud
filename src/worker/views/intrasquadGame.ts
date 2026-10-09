import { helpers } from "../util/index.ts";
import { defineView, type ViewInput } from "../util/defineView.ts";
import type { RouteParams } from "../../ui/router/types.ts";
import type { boxScoreToLiveSim } from "./liveGame.ts";

const processInputs = (params: RouteParams<"intrasquadGame">, ctxBBGM: any) => {
	return {
		liveSim: ctxBBGM.liveSim as
			| Awaited<ReturnType<typeof boxScoreToLiveSim>>
			| undefined,
		abbrev: ctxBBGM.abbrev as string | undefined,
	};
};

// The simmed scrimmage result rides in on the routing context (see
// simIntrasquadGame -> realtimeUpdate). With no game to show - e.g. someone
// navigated here directly - bounce back to the league dashboard.
const updateIntrasquadGame = async ({
	liveSim,
	abbrev,
}: ViewInput<typeof processInputs>) => {
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

export default defineView({
	id: "intrasquadGame",
	processInputs,
	load: ({ inputs }) => updateIntrasquadGame(inputs),
});
