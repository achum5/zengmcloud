import { idb } from "../db/index.ts";
import { g, helpers } from "../util/index.ts";
import { DEFAULT_TEAM_COLORS } from "../../common/constants.ts";
import type { UpdateEvents, ViewInput } from "../../common/types.ts";

// The team's current roster, plus the color info needed to label the two
// squads, so the front end can split them into a "Primary" and a "Secondary"
// squad and run an intrasquad scrimmage (an exhibition game between two halves
// of one team). Mirrors how the Roster page loads the active roster.
const updateIntrasquad = async (
	{ tid, abbrev }: ViewInput<"intrasquad">,
	updateEvents: UpdateEvents,
	state: any,
) => {
	if (
		updateEvents.includes("firstRun") ||
		updateEvents.includes("playerMovement") ||
		updateEvents.includes("gameSim") ||
		tid !== state.tid
	) {
		const season = g.get("season");

		const t = await idb.getCopy.teamsPlus(
			{
				season,
				tid,
				attrs: ["tid", "abbrev", "region", "name", "colors"],
			},
			"noCopyCache",
		);

		if (!t) {
			// Bad tid - bounce back to the league dashboard.
			return {
				redirectUrl: helpers.leagueUrl([]),
			};
		}

		const playersAll = await idb.cache.players.indexGetAll("playersByTid", tid);
		const players = await idb.getCopies.playersPlus(playersAll, {
			attrs: ["pid", "firstName", "lastName", "age", "watch", "injury"],
			ratings: ["ovr", "pos", "skills"],
			stats: ["jerseyNumber"],
			season,
			tid,
			showNoStats: true,
			showRookies: true,
			fuzz: true,
		});

		if (players.length > 0) {
			players.sort((a, b) => b.ratings.ovr - a.ratings.ovr);
		}

		return {
			abbrev,
			tid,
			season,
			region: t.region,
			name: t.name,
			colors: t.colors ?? DEFAULT_TEAM_COLORS,
			numPlayersOnCourt: g.get("numPlayersOnCourt"),
			players,
		};
	}
};

export default updateIntrasquad;
