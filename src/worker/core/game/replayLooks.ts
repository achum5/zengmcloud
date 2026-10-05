import { idb } from "../../db/index.ts";
import type { ReplayLooks } from "../../../common/types.ts";

// How the players and teams in a game look right now, as the game is played -
// saved with its replay so it can be shown that way for good (see
// ReplayLooks). Cosmetic: anything that can't be read is just left out.
export const takeReplayLooks = async (
	tids: number[],
	pids: number[],
): Promise<ReplayLooks> => {
	const looks: ReplayLooks = { players: {}, teams: {} };
	for (const pid of pids) {
		const p = await idb.cache.players.get(pid);
		if (!p) {
			continue;
		}
		looks.players[pid] = {
			face: p.face,
			imgURL: p.imgURL || undefined,
			hgt: p.hgt,
			weight: p.weight,
		};
	}
	for (const tid of tids) {
		// An All-Star side has no team of its own.
		if (tid < 0) {
			continue;
		}
		const t = await idb.cache.teams.get(tid);
		if (!t) {
			continue;
		}
		looks.teams[tid] = {
			region: t.region,
			name: t.name,
			abbrev: t.abbrev,
			imgURL: t.imgURL,
			colors: t.colors,
			jersey: t.jersey,
			court: t.court,
		};
	}
	return looks;
};
