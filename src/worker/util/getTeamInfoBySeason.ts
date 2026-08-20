import { DEFAULT_JERSEY, DEFAULT_TEAM_COLORS } from "../../common/constants.ts";
import { idb } from "../db/index.ts";

export const getTeamInfoBySeason = async (tid: number, season: number) => {
	// Belt and braces: the types say number, but callers feed this from loosely
	// typed playersPlus rows, and an undefined here becomes an invalid
	// IndexedDB key - a DataError that takes down the whole view instead of one
	// lookup. No team info is the honest answer for a key that cannot exist.
	if (!Number.isFinite(tid) || !Number.isFinite(season)) {
		return;
	}
	if (tid === -1 || tid === -2) {
		return {
			abbrev: "ASG",
			colors: DEFAULT_TEAM_COLORS,
			jersey: DEFAULT_JERSEY,
			name: tid === -1 ? "1" : "2",
			region: "All-Stars",
		};
	}

	const teamSeasonsIndex = idb.league
		.transaction("teamSeasons")
		.store.index("tid, season");

	let ts:
		| {
				abbrev: string;
				colors: [string, string, string];
				imgURL?: string;
				imgURLSmall?: string;
				jersey?: string;
				name: string;
				region: string;
		  }
		| undefined = await teamSeasonsIndex.get([tid, season]);
	if (!ts) {
		// No team season entry for the requested season... is there an older one, somehow? If so, use the latest one before the requested season. If not, use the first we find (it is the oldest existing one, so assume that applies).
		for await (const cursor of teamSeasonsIndex.iterate(
			IDBKeyRange.bound([tid, -Infinity], [tid, Infinity]),
		)) {
			if (cursor.value.season > season && ts) {
				break;
			}
			ts = cursor.value;
		}
	}
	if (!ts) {
		ts = await idb.cache.teams.get(tid);
	}

	if (ts) {
		return {
			abbrev: ts.abbrev,
			colors: ts.colors ?? DEFAULT_TEAM_COLORS,
			// The logo AS IT WAS that season, same as the colors - a franchise that
			// has since rebranded had a different mark then.
			imgURL: ts.imgURL,
			imgURLSmall: ts.imgURLSmall,
			jersey: ts.jersey,
			name: ts.name,
			region: ts.region,
		};
	}

	// Could be an invalid tid, like PLAYER.TOT or PLAYER.DOES_NOT_EXIST
};
