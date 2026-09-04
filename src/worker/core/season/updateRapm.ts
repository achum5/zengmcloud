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
//
// And it is shrunk toward last season's rating rather than toward average,
// because one season is not many possessions either. Measured over five simmed
// seasons, that lifts how well the ratings recover a player's actual ability
// from 0.51 to 0.64 - which is exactly what pooling five seasons of lineups
// into one regression achieves, at none of the cost. What a player was worth
// last year is the most informative thing there is about what he is worth this
// year, and using it is the difference between a plain one-season regression
// and how impact is actually estimated. It stays a prior and never a floor: a
// season that disagrees strongly enough moves the rating as far as the
// evidence warrants, and a rookie, with nothing behind him, is shrunk toward
// average exactly as before.

import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { decodeShifts } from "../../util/gameShifts.ts";
import {
	computeRapm,
	type RapmRating,
	type RapmStint,
} from "../../util/rapm.ts";
import type { Game, Player } from "../../../common/types.ts";

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

// What each of this season's players was rated last season, keyed the way this
// season's regression keys him. A player traded midseason a year ago has two
// ratings behind him and one man's worth of prior, so they are averaged by the
// minutes they were earned over.
const lastSeasonRatings = (
	players: readonly Player[],
	season: number,
): Map<string, { off: number; def: number }> => {
	const prior = new Map<string, { off: number; def: number }>();

	for (const p of players) {
		let off = 0;
		let def = 0;
		let min = 0;
		for (const ps of p.stats) {
			if (ps.season !== season - 1 || ps.playoffs || ps.orapm === undefined) {
				continue;
			}
			// A stats row with no minutes cannot be weighted by them, and has
			// nothing behind its rating either.
			const weight = ps.min > 0 ? ps.min : 0;
			off += weight * ps.orapm;
			def += weight * (ps.drapm ?? 0);
			min += weight;
		}

		if (min === 0) {
			continue;
		}

		// Applied to every team he plays for THIS season, because the prior is
		// about the player and the regression is keyed by where he played.
		for (const ps of p.stats) {
			if (ps.season === season && !ps.playoffs) {
				prior.set(key(p.pid, ps.tid), { off: off / min, def: def / min });
			}
		}
	}

	return prior;
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

	const players = await idb.cache.players.getAll();

	const fit = computeRapm(stints, {
		prior: lastSeasonRatings(players, season),
	});
	if (!fit) {
		return;
	}

	// Where each rating stands in the league it was earned in. Worked out here
	// because here is the only place the whole distribution is in hand: a page
	// showing a career would otherwise have to reread every season of it to say
	// what a number was worth at the time.
	const ranks = percentiles([...fit.ratings.values()]);

	for (const p of players) {
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
			ps.rapmPoss = rating.poss;
			ps.orapmPct = ranks.off(rating.off);
			ps.drapmPct = ranks.def(rating.def);
			ps.rapmPct = ranks.total(rating.off + rating.def);
			changed = true;
		}

		if (changed) {
			await idb.cache.players.put(p);
		}
	}
};

// How many of the league a rating beats, per side. Sorted once and answered by
// binary search, because every rated player asks three times.
export const percentiles = (ratings: readonly RapmRating[]) => {
	const rank = (values: number[]) => {
		values.sort((a, b) => a - b);
		return (value: number) => {
			if (values.length === 0) {
				return 0;
			}
			let low = 0;
			let high = values.length;
			while (low < high) {
				const mid = (low + high) >> 1;
				if (values[mid]! < value) {
					low = mid + 1;
				} else {
					high = mid;
				}
			}
			// Nobody beats everybody: the best man in the league beats every
			// OTHER man, which is 99 out of a hundred.
			return Math.min(99, Math.round((100 * low) / values.length));
		};
	};

	return {
		off: rank(ratings.map((r) => r.off)),
		def: rank(ratings.map((r) => r.def)),
		total: rank(ratings.map((r) => r.off + r.def)),
	};
};
