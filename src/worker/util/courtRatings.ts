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

export const courtRatingsFor = async (
	pids: Iterable<number>,
	season: number,
): Promise<Record<number, Record<string, number>>> => {
	const want = [...new Set(pids)].filter((pid) => pid >= 0);
	const out: Record<number, Record<string, number>> = {};
	if (want.length === 0) {
		return out;
	}
	for (const p of await idb.merged.players.getAll({ pids: want })) {
		const r = p.ratings.findLast((x) => x.season <= season) ?? p.ratings.at(-1);
		if (!r) {
			continue;
		}
		const row: Record<string, number> = {};
		for (const k of COURT_RATING_KEYS) {
			const v = (r as unknown as Record<string, unknown>)[k];
			if (typeof v === "number" && Number.isFinite(v)) {
				row[k] = v;
			}
		}
		out[p.pid] = row;
	}
	return out;
};
