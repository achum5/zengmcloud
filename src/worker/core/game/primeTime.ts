import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { player, team } from "../index.ts";

// PRIME TIME: a football game under the lights when two of the league's
// better teams meet - by their ratings as the season began (as the 3D game
// shows it, the rest in the afternoon). Cosmetic, so false whenever it
// cannot be told.

// The share of the league counted as one of its better teams.
const GOOD = 0.4;

export const isPrimeTime = (
	ovrs: Map<number, number>,
	tids: [number, number],
): boolean => {
	if (ovrs.size === 0 || tids.some((tid) => !ovrs.has(tid))) {
		return false;
	}
	const best = [...ovrs.entries()]
		.sort((a, b) => b[1] - a[1])
		.slice(0, Math.ceil(ovrs.size * GOOD))
		.map(([tid]) => tid);
	return tids.every((tid) => best.includes(tid));
};

// Each team's rating in `season`: as it began, or - nobody having played
// yet this season - as it is now.
const ovrsIn = async (season: number): Promise<Map<number, number>> => {
	const ovrs = new Map<number, number>();
	const teamSeasons = await idb.merged.teamSeasons.getAll({ season });
	const now = season === g.get("season");
	const teams = now ? await idb.cache.teams.getAll() : [];
	for (const ts of teamSeasons) {
		if (now && teams.find((t) => t.tid === ts.tid)?.disabled) {
			continue;
		}
		let ovr = ts.ovrStart;
		if (ovr === undefined && now) {
			const players = await player.formatAll(
				await idb.cache.players.indexGetAll("playersByTid", ts.tid),
				{
					attrs: ["injury", "pid", "value"],
					ratings: ["ovr", "pot", "ovrs", "pos"],
					season,
					tid: ts.tid,
					showNoStats: true,
					showRookies: true,
					fuzz: true,
					coarsenRatings: false,
				},
			);
			ovr = team.ovr(players);
		}
		if (ovr !== undefined) {
			ovrs.set(ts.tid, ovr);
		}
	}
	return ovrs;
};

export const primeTimeFor = async (
	season: number,
	tids: [number, number],
): Promise<boolean> => {
	if (tids.some((tid) => tid < 0)) {
		return false;
	}
	try {
		return isPrimeTime(await ovrsIn(season), tids);
	} catch {
		return false;
	}
};
