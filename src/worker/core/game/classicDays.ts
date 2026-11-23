import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";

// THE CLASSIC DAY COUNT of this season, for a calendar season to tick at the
// classic pace (calendarTicks.ts). Stored with the league when the season's
// schedule is made (saveClassicDays.ts). Kept apart from the saving, which
// needs the schedule maker, so views can read it without importing that.

// This season's, if stored.
export const storedClassicDays = () => {
	const stored = g.get("calendarClassicDays");
	return stored?.season === g.get("season") ? stored.days : undefined;
};

// Each team's regular-season games played this season.
const playedByTid = async () => {
	const played = new Map<number, number>();
	for (const game of await idb.cache.games.getAll()) {
		if (
			game.season !== g.get("season") ||
			game.playoffs ||
			game.teams[0].tid < 0 ||
			game.teams[1].tid < 0
		) {
			continue;
		}
		for (const t of game.teams) {
			played.set(t.tid, (played.get(t.tid) ?? 0) + 1);
		}
	}
	return played;
};

// For injury projections (injuryTicksUntil): the classic pace, if this
// season has one stored.
export const getInjuryPace = async () => {
	const classicDays = storedClassicDays();
	if (classicDays === undefined) {
		return;
	}
	return { classicDays, played: await playedByTid() };
};
