import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import type { ArenaLooks, ReplayLooks } from "../../../common/types.ts";

// The home team's building for a game in `season` (see ArenaLooks): its
// capacity that season, the titles it won before it, and the numbers it had
// retired by then. Cosmetic, like the rest: undefined when there's no real
// home team (an All-Star side), or nothing can be read.
export const takeArenaLooks = async (
	tid: number,
	season: number,
): Promise<ArenaLooks | undefined> => {
	if (tid < 0) {
		return undefined;
	}
	try {
		const looks: ArenaLooks = { titles: [], retired: [] };
		const teamSeasons = await idb.getCopies.teamSeasons({ tid }, "noCopyCache");
		for (const ts of teamSeasons) {
			if (ts.season === season) {
				looks.capacity = ts.stadiumCapacity;
			} else if (
				ts.season < season &&
				ts.playoffRoundsWon >= 0 &&
				ts.playoffRoundsWon === g.get("numGamesPlayoffSeries", ts.season).length
			) {
				looks.titles.push(ts.season);
			}
		}
		looks.titles.sort((a, b) => a - b);
		const t = await idb.cache.teams.get(tid);
		for (const row of t?.retiredJerseyNumbers ?? []) {
			if (row.seasonRetired > season) {
				continue;
			}
			let name: string | undefined;
			if (row.pid !== undefined) {
				const p = await idb.getCopy.players({ pid: row.pid }, "noCopyCache");
				name = p?.lastName || undefined;
			}
			// A number retired for someone who never played here.
			if (name === undefined && row.text && row.text.length <= 14) {
				name = row.text;
			}
			looks.retired.push({ number: row.number, name });
		}
		return looks;
	} catch {
		return undefined;
	}
};

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
	// The first team is at home.
	if (tids[0] !== undefined) {
		looks.arena = await takeArenaLooks(tids[0], g.get("season"));
	}
	return looks;
};
