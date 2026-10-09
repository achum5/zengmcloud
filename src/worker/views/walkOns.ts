import { collegeClassLabel, collegeFinalSeason } from "../../common/college.ts";
import { PHASE, PLAYER } from "../../common/constants.ts";
import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";
import { defineView, type ViewArgs } from "../util/defineView.ts";

// Unsigned players anyone can add to a roster with room: no recruiting, no
// NIL to speak of.
const updateWalkOns = async ({ updateEvents }: ViewArgs) => {
	if (
		!updateEvents.has("firstRun") &&
		!updateEvents.has("playerMovement") &&
		!updateEvents.has("newPhase")
	) {
		return;
	}

	if (!g.get("college")) {
		return { college: false as const };
	}

	const userTid = g.get("userTid");
	const season = g.get("season");
	// In the offseason, everyone is joining next season's team.
	const classSeason = g.get("phase") > PHASE.PLAYOFFS ? season + 1 : season;
	const freeAgents = (
		await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT)
	).filter((p) => {
		const final = collegeFinalSeason(p);
		return !p.recruiting && (final === undefined || final >= classSeason);
	});
	const players = await idb.getCopies.playersPlus(freeAgents, {
		attrs: ["pid", "firstName", "lastName", "age", "hgt", "injury"],
		ratings: ["ovr", "pot", "pos", "skills"],
		season,
		showNoStats: true,
		showRookies: true,
		fuzz: true,
	});
	const byPid = new Map(freeAgents.map((p) => [p.pid, p]));
	const rosterSize = (
		await idb.cache.players.indexGetAll("playersByTid", userTid)
	).length;

	return {
		college: true as const,
		season,
		rosterSize,
		maxRosterSize: g.get("maxRosterSize"),
		players: players.map((p) => ({
			...p,
			classLabel: collegeClassLabel(byPid.get(p.pid)!, classSeason),
		})),
	};
};

export default defineView({
	id: "walkOns",
	load: updateWalkOns,
});
