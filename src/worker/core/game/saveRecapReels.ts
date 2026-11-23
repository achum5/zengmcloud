import { PHASE } from "../../../common/constants.ts";
import { getBestPlayerBoxScore } from "../../../common/getBestPlayerBoxScore.ts";
import { makeRecapReel } from "../../../common/recapReel.ts";
import { isAllStarGame } from "../../../common/specialGames.ts";
import type { GameResults } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";

// THE NIGHTLY RECAP'S PLAYS: every game of a night is cut down to its recap
// plays (common/recapReel.ts) right after it is simmed, while its play-by-play
// is still at hand, and only the last few nights' are kept. Presentation
// only: generating play-by-play rolls no dice, so the games come out the same.

// Saving them at all. They travel to every device in a shared league, and a
// device on a build from before their store cannot take them (it stops
// syncing till it updates), so the store goes out a step ahead of them.
export const RECAP_REELS_ON = false;

// How many nights back (counting tonight) recaps can be watched.
export const RECAP_NIGHTS = 7;

// The games that get recap plays, and so are simmed with play-by-play: every
// game of the regular season and playoffs but the All-Star Game.
export const wantsRecapReel = (homeTid: number, awayTid: number) =>
	__SPORT === "basketball" &&
	(g.get("phase") === PHASE.REGULAR_SEASON ||
		g.get("phase") === PHASE.PLAYOFFS) &&
	!isAllStarGame(homeTid, awayTid);

// Each team's best player in the game, as the box score picks them.
const starsOf = (result: GameResults) => {
	const stars: number[] = [];
	for (const t of result.team) {
		const best = getBestPlayerBoxScore(
			t.player.map((p: any) => ({ ...p.stat, pid: p.id })),
		);
		if (best) {
			stars.push((best.p as any).pid);
		}
	}
	return stars;
};

export const saveRecapReels = async (results: GameResults[]) => {
	const season = g.get("season");
	let lastDay: number | undefined;
	for (const result of results) {
		if (
			result.playByPlay === undefined ||
			typeof result.day !== "number" ||
			!wantsRecapReel(result.team[0].id, result.team[1].id)
		) {
			continue;
		}
		const reel = makeRecapReel(result.playByPlay, {
			numPeriods: g.get("numPeriods"),
			spread: result.pregameSpread,
			stars: starsOf(result),
		});
		await idb.cache.recapReels.put({
			gid: result.gid,
			season,
			day: result.day,
			reel,
		});
		lastDay = Math.max(lastDay ?? -Infinity, result.day);
	}

	if (lastDay !== undefined) {
		await pruneRecapReels(season, lastDay);
	}
};

// Every night's but this season's last RECAP_NIGHTS (a schedule day is a
// night with games).
const pruneRecapReels = async (season: number, day: number) => {
	for (const row of await idb.cache.recapReels.getAll()) {
		if (row.season !== season || row.day <= day - RECAP_NIGHTS) {
			await idb.cache.recapReels.delete(row.gid);
		}
	}
};
