import { idb } from "../db/index.ts";

// The ratings the 3D game plays each man by - how fast he is, how high he
// gets up, how he handles it, shoots it, defends (see CourtRatings in
// ui/views/LiveGame/court3d/director.ts): his, as of the game's season.
export const COURT_RATING_KEYS = [
	"ovr",
	"hgt",
	"stre",
	"spd",
	"jmp",
	"endu",
	"ins",
	"dnk",
	"ft",
	"fg",
	"tp",
	"oiq",
	"diq",
	"drb",
	"pss",
	"reb",
] as const;

// Football's (see field3d/ratings.ts): how fast he is, how strong, how hard
// to bring down, how sure his hands - by what he does.
export const FIELD_RATING_KEYS = [
	"ovr",
	"hgt",
	"stre",
	"spd",
	"endu",
	"elu",
	"bsc",
	"rtr",
	"hnd",
	"thv",
	"thp",
	"tha",
	"rbk",
	"pbk",
	"pcv",
	"tck",
	"prs",
	"rns",
	"kpw",
	"kac",
	"ppw",
	"pac",
] as const;

export const courtRatingsFor = async (
	pids: Iterable<number>,
	season: number,
	keys: readonly string[] = COURT_RATING_KEYS,
): Promise<Record<number, Record<string, number>>> => {
	const want = [...new Set(pids)].filter((pid) => pid >= 0);
	const out: Record<number, Record<string, number>> = {};
	if (want.length === 0) {
		return out;
	}
	for (const p of await idb.merged.players.getAll({ pids: want })) {
		// (A game from before his first season here: as he first was.)
		const r = p.ratings.findLast((x) => x.season <= season) ?? p.ratings[0];
		if (!r) {
			continue;
		}
		const row: Record<string, number> = {};
		for (const k of keys) {
			const v = (r as unknown as Record<string, unknown>)[k];
			if (typeof v === "number" && Number.isFinite(v)) {
				row[k] = v;
			}
		}
		out[p.pid] = row;
	}
	return out;
};
