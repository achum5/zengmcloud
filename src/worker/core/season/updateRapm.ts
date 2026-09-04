// THE SEASON'S RAPM, COMPUTED ONCE, WHEN THE REGULAR SEASON ENDS.
//
// Every other advanced stat in the game is cheap enough to recompute after
// every day of games. This one is a regression over every lineup the league
// put on the floor all year, so it runs exactly once, at the moment there is a
// full season to run it on and before the games it reads start aging out.
//
// It is a regular-season stat. The playoffs are a few hundred possessions
// against a handful of opponents, which is not enough to separate a player
// from his teammates no matter how the arithmetic is arranged, and pretending
// otherwise would put a number on the page that means nothing.

import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { decodeShifts } from "../../util/gameShifts.ts";
import { computeRapm, type RapmStint } from "../../util/rapm.ts";
import type { Game } from "../../../common/types.ts";

// A player is a different regressor for each team he played for, which is both
// the honest thing to do - the five he shared the floor with in November are
// not the five he shared it with in March - and what lines each estimate up
// with the stats row it gets written to.
const key = (pid: number, tid: number) => `${pid}|${tid}`;

export const stintsFromGames = (games: readonly Game[]): RapmStint[] => {
	const stints: RapmStint[] = [];

	for (const game of games) {
		if (game.playoffs || !game.shifts) {
			continue;
		}

		const tids = [game.teams[0].tid, game.teams[1].tid] as const;

		for (const shift of decodeShifts(game)) {
			const lineups = [
				shift.lineups[0].map((pid) => key(pid, tids[0])),
				shift.lineups[1].map((pid) => key(pid, tids[1])),
			] as const;

			for (const o of [0, 1] as const) {
				if (shift.poss[o] > 0) {
					stints.push({
						off: lineups[o],
						def: lineups[o === 0 ? 1 : 0],
						poss: shift.poss[o],
						pts: shift.pts[o],
					});
				}
			}
		}
	}

	return stints;
};

export const updateRapm = async (season: number = g.get("season")) => {
	// The current season's games are all in the cache already, so this costs
	// nothing but the arithmetic.
	const games = (await idb.cache.games.getAll()).filter(
		(game) => game.season === season,
	);

	const stints = stintsFromGames(games);
	if (stints.length === 0) {
		return;
	}

	const fit = computeRapm(stints);
	if (!fit) {
		return;
	}

	for (const p of await idb.cache.players.getAll()) {
		let changed = false;

		for (const ps of p.stats) {
			if (ps.season !== season || ps.playoffs) {
				continue;
			}

			const rating = fit.ratings.get(key(p.pid, ps.tid));
			if (!rating) {
				continue;
			}

			ps.orapm = rating.off;
			ps.drapm = rating.def;
			ps.rapm = rating.off + rating.def;
			changed = true;
		}

		if (changed) {
			await idb.cache.players.put(p);
		}
	}
};
