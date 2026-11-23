import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import setGameAttributes from "../league/setGameAttributes.ts";
import newSchedule from "../season/newSchedule.ts";
import { countClassicDays } from "./calendarTicks.ts";
import { storedClassicDays } from "./classicDays.ts";

// Storing this season's classic day count (classicDays.ts): when the season's
// schedule is made, or, for a season dated before there was one, the first
// time a night asks.

// How many classic days BBGM's own schedule for these teams takes.
export const classicDaysFor = (
	teams: {
		tid: number;
		seasonAttrs: { cid: number; did: number };
	}[],
) => countClassicDays(newSchedule(teams));

export const saveClassicDays = async (days: number) => {
	await setGameAttributes({
		calendarClassicDays: { season: g.get("season"), days },
	});
};

// This season's, worked out and stored if it's on a calendar and has none.
export const ensureClassicDays = async () => {
	const stored = storedClassicDays();
	if (stored !== undefined) {
		return stored;
	}
	const dated = (await idb.cache.schedule.getAll()).some(
		(game) => game.date !== undefined,
	);
	if (!dated) {
		return;
	}
	const season = g.get("season");
	const teams = [];
	for (const t of await idb.cache.teams.getAll()) {
		const teamSeason = await idb.cache.teamSeasons.indexGet(
			"teamSeasonsByTidSeason",
			[t.tid, season],
		);
		if (!t.disabled && teamSeason) {
			teams.push({
				tid: t.tid,
				seasonAttrs: { cid: teamSeason.cid, did: teamSeason.did },
			});
		}
	}
	const days = classicDaysFor(teams);
	await saveClassicDays(days);
	return days;
};
