// The one exception to sim authority: your OWN team's single game.
//
// Everything else that advances the shared timeline is refused on a device that
// is not in charge of simming (see isSimAuthorityLockedCall and the guard in
// worker/index.ts). A single game is safe to carve out because it is a disjoint
// slice of the schedule day, and simDayClaimPolicy.ts already fences slices
// atomically - two devices can never both sim the same gid, whatever the UI
// allows. The policy in common/ownGameSim.ts decides; this module gathers the
// facts it needs.
//
// "Sim game" and "Watch game" are the same call with one extra flag - the
// play-by-play logger is passive and never touches the RNG - so both are
// treated identically here. Any two independent sims of a gid produce DIFFERENT
// games, so the hazard was never live-sim-specific.

import {
	decideOwnGameSim,
	DEFAULT_OWN_GAME_SIM_CUTOFF_SECONDS,
	type OwnGameSimDecision,
} from "../../../common/ownGameSim.ts";
import { idb } from "../../db/index.ts";
import { g, local } from "../../util/index.ts";
import { getGlobalSettings } from "../../util/getGlobalSettings.ts";
import { getSyncEngine } from "./engineHolder.ts";
import { getRoomAutoPlayNextRunAt } from "./connect.ts";
import { isWatchingLiveBroadcast } from "./liveWatchGate.ts";

const OWN_GAME_ACTIONS = new Set(["simGame", "liveGame"]);

// Is this the kind of call the exception could ever apply to? Cheap and
// synchronous, so the expensive lookup below only runs for the two actions.
export const isOwnGameSimCall = (type: string, name: string): boolean =>
	type === "actions" && OWN_GAME_ACTIONS.has(name);

// Does this gid belong to the team the device currently controls?
//
// g.userTid, NOT userTids: in multi-team mode the exception follows the team
// that is actually selected, so controlling several teams does not turn into
// permission to sim several games out from under the simmer.
const gidIsOwnGame = async (gid: number): Promise<boolean> => {
	const userTid = g.get("userTid");
	const schedule = await idb.cache.schedule.getAll();
	const game = schedule.find((row) => row.gid === gid);
	if (game) {
		return game.homeTid === userTid || game.awayTid === userTid;
	}
	// Not on the schedule any more - already played, or a stale click. Either
	// way there is nothing here to grant.
	return false;
};

export const decideOwnGameSimCall = async (
	param: unknown,
): Promise<OwnGameSimDecision> => {
	const gid = typeof param === "number" ? param : undefined;
	if (gid === undefined) {
		return { allow: false, reason: "You can only sim your own team's game." };
	}

	const engine = getSyncEngine();
	const settings = await getGlobalSettings();
	const nextRunAt = getRoomAutoPlayNextRunAt();

	return decideOwnGameSim({
		isOwnGame: await gidIsOwnGame(gid),
		isAuthority: engine?.isAuthority() ?? false,
		connectedAndReady: engine !== undefined,
		// A live sim already playing here, or a league-mate's broadcast in
		// progress: starting another is exactly what the fence would refuse.
		simInFlight: local.liveSimGid !== undefined || isWatchingLiveBroadcast(),
		msUntilAutoSim:
			nextRunAt === undefined ? undefined : nextRunAt - Date.now(),
		cutoffSeconds:
			settings.ownGameSimCutoffSeconds ?? DEFAULT_OWN_GAME_SIM_CUTOFF_SECONDS,
	});
};
