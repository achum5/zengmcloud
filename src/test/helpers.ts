import { Cache, idb } from "../worker/db/index.ts";
import { STORES, type Store } from "../worker/db/Cache.ts";
import { g, helpers, local } from "../worker/util/index.ts";
import { forgetTiers } from "../worker/core/trade/tradePosture.ts";
import {
	defaultGameAttributes,
	footballOverrides,
} from "../common/defaultGameAttributes.ts";

export const mockIDBLeague = (): any => {
	const store = {
		index() {
			return {
				getAll() {
					return [];
				},
			};
		},
	};

	const league = {
		getAll() {
			return [];
		},
		transaction() {
			return {
				store,
				objectStore() {
					return store;
				},
			};
		},
	};

	return league;
};

/**
 * Finds the number of times an element appears in an array.
 *
 * @memberOf test.core
 * @param {Array} array The array to search over.
 * @param {*} x Element to search for
 * @return {number} The number of times x was found in array.
 */
export function numInArrayEqualTo<T>(array: T[], x: T): number {
	let n = 0;
	let idx = array.indexOf(x);

	while (idx !== -1) {
		n += 1;
		idx = array.indexOf(x, idx + 1);
	}

	return n;
}

export const resetCache = async (
	data: Partial<Record<Store, Readonly<any[]>>> = {},
	{ stubFlush = true }: { stubFlush?: boolean } = {},
) => {
	idb.cache = new Cache(); // We want these to do nothing while testing, usually

	idb.cache.fill = async () => {};

	if (stubFlush) {
		idb.cache.flush = async () => {};
	}

	for (const store of STORES) {
		// This stuff is all needed because a real Cache.fill is not called.
		idb.cache._data[store] = {};
		idb.cache._deletes[store] = new Set();
		idb.cache._dirtyRecords[store] = new Set();
		idb.cache._maxIds[store] = -1;

		idb.cache._markDirtyIndexes(store);
	}

	idb.cache._status = "full";

	if (!data) {
		return;
	}

	if (data.players) {
		await idb.cache.players.addAll(data.players);
	}

	if (data.teams) {
		await idb.cache.teams.addAll(data.teams);
	}

	if (data.teamSeasons) {
		await idb.cache.teamSeasons.addAll(data.teamSeasons);
	}

	if (data.teamStats) {
		await idb.cache.teamStats.addAll(data.teamStats);
	}

	if (data.trade) {
		await idb.cache.trade.addAll(data.trade);
	}

	if (data.draftPicks) {
		for (const obj of data.draftPicks) {
			await idb.cache.draftPicks.add(obj);
		}
	}

	if (data.releasedPlayers) {
		for (const obj of data.releasedPlayers) {
			await idb.cache.releasedPlayers.add(obj);
		}
	}

	if (data.scheduledEvents) {
		for (const obj of data.scheduledEvents) {
			await idb.cache.scheduledEvents.add(obj);
		}
	}

	if (data.events) {
		for (const obj of data.events) {
			await idb.cache.events.add(obj);
		}
	}
};

export const resetG = () => {
	// The trade market remembers what it last read each team as, keyed by
	// tid, and player valuation reads a league-wide rating mean it computed
	// lazily, so one test file's league would otherwise leak into the next.
	forgetTiers();
	local.playerOvrMeanStdStale = true;
	const season = 2016;
	const teams = helpers.getTeamsDefault();
	Object.assign(g, defaultGameAttributes);

	if (__SPORT === "football") {
		Object.assign(g, footballOverrides);
	}

	Object.assign(g, {
		userTid: 0,
		userTids: [0],
		season,
		startingSeason: season,
		teamInfoCache: teams.map((t) => ({
			abbrev: t.abbrev,
			disabled: false,
			imgURL: t.imgURL,
			imgURLSmall: t.imgURLSmall,
			name: t.name,
			region: t.region,
		})),
		gracePeriodEnd: season + 2,
		numTeams: teams.length,
		numActiveTeams: teams.length,
	});
};

// A small test league that also has playoffs it could actually hold. The
// default bracket (16 teams plus a play-in) is invalid in a league of 8 or 12,
// and the draft pick model plays the bracket out to price a pick, so it
// refuses one that can't exist. Half the league makes it, rounded down to a
// whole bracket, with no byes and no play-in.
export const setLeagueSize = (numTeams: number) => {
	g.setWithoutSavingToDB("numTeams", numTeams);
	g.setWithoutSavingToDB("numActiveTeams", numTeams);
	let rounds = 1;
	while (2 ** (rounds + 1) <= numTeams / 2) {
		rounds += 1;
	}
	g.setWithoutSavingToDB("numGamesPlayoffSeries", Array(rounds).fill(7));
	g.setWithoutSavingToDB("numPlayoffByes", 0);
	g.setWithoutSavingToDB("playIn", false);
};
