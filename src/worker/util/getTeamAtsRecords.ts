import type { Game } from "../../common/types.ts";
import { idb } from "../db/index.ts";
import { g } from "./index.ts";
import { getGameSpread } from "../../common/getGameSpread.ts";

export type AtsRecord = { won: number; lost: number; pushed: number };

// A team's against-the-spread record for a season, reconstructed from stored
// games. The pregame team overalls are saved on every completed game
// (game.teams[t].ovr), and getGameSpread re-derives the exact spread the
// sportsbook and ScoreBox showed, so the ATS result is deterministic - no
// betting line has to be stored at sim time, and this works retroactively on
// leagues that were already mid-season. Legacy games missing OVRs are skipped
// (the same limitation ScoreBox's spread display already tolerates). Playoff
// and All-Star games are left out so the ATS record spans exactly the games
// counted in the regular-season W-L shown next to it.
export const getTeamAtsRecords = async (
	season: number,
	// Callers that already hold the season's games (the standings page, which
	// also builds the >.500 column from them) pass them in rather than making
	// this pull every box score a second time.
	preloadedGames?: Game[],
): Promise<Map<number, AtsRecord>> => {
	const homeCourtAdvantage = g.get("homeCourtAdvantage");
	const numPeriodsDefault = g.get("numPeriods");
	const quarterLength = g.get("quarterLength");

	const games =
		preloadedGames ?? (await idb.getCopies.games({ season }, "noCopyCache"));

	const records = new Map<number, AtsRecord>();
	const bump = (tid: number, key: keyof AtsRecord) => {
		let rec = records.get(tid);
		if (!rec) {
			rec = { won: 0, lost: 0, pushed: 0 };
			records.set(tid, rec);
		}
		rec[key] += 1;
	};

	for (const game of games) {
		if (game.playoffs) {
			continue; // regular season only, to match the displayed W-L
		}
		const home = game.teams[0];
		const away = game.teams[1];
		if (home.tid < 0 || away.tid < 0) {
			continue; // All-Star / other special games
		}

		const spread = getGameSpread({
			ovr0: home.ovr,
			ovr1: away.ovr,
			homeCourtAdvantage,
			// A regular-season neutral-site game (rare) drops home court, matching
			// how the line was priced. Finals/playoff neutrality never applies here.
			neutralSite: !!game.neutralSite,
			numPeriods: game.numPeriods ?? numPeriodsDefault,
			quarterLength,
		});
		if (spread === undefined) {
			continue; // legacy game with no stored OVRs - can't grade it
		}

		// `spread` is the predicted HOME margin (> 0 home favored). The home team
		// covers when it beats that margin, the away team when home falls short of
		// it, and it's a push for both on the exact number.
		const diff = home.pts - away.pts - spread;
		if (diff > 0) {
			bump(home.tid, "won");
			bump(away.tid, "lost");
		} else if (diff < 0) {
			bump(home.tid, "lost");
			bump(away.tid, "won");
		} else {
			bump(home.tid, "pushed");
			bump(away.tid, "pushed");
		}
	}

	return records;
};

// "34-27", or "34-27-2" when there are pushes (shown only when nonzero). Empty
// string when the team has no gradable games yet, so callers can omit the tag.
export const formatAtsRecord = (rec: AtsRecord | undefined): string => {
	if (!rec || rec.won + rec.lost + rec.pushed === 0) {
		return "";
	}
	return rec.pushed > 0
		? `${rec.won}-${rec.lost}-${rec.pushed}`
		: `${rec.won}-${rec.lost}`;
};
