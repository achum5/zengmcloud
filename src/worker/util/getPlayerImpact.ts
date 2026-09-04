// WHAT THE GAME DID WHILE HE WAS OUT THERE, AND WHO HE WAS OUT THERE WITH.
//
// The lineups the sim records for RAPM (see gameShifts.ts) answer a question
// no box score can: how the team actually scored and defended with a given
// five on the floor. RAPM squeezes that into one number per player. This is
// the same evidence left as it is - exact arithmetic on possessions and
// points, no model, nothing fitted - so a reader can see the thing the rating
// was estimated from.
//
// Two players who share almost every minute is exactly the situation RAPM
// exists to untangle and this cannot: a bench big whose only minutes come
// beside the starting point guard will show the point guard's number. The
// possessions column is there so that is visible rather than hidden, and
// pairings too thin to mean anything are dropped outright.

import { idb } from "../db/index.ts";
import { decodeShifts } from "./gameShifts.ts";
import type { Game } from "../../common/types.ts";

export type ImpactPartner = {
	pid: number;
	// Possessions the two were on the floor together.
	poss: number;
	// Net points per 100 possessions while both played.
	together: number;
	// The same for his possessions WITHOUT this teammate. Undefined when there
	// were too few of them to say anything.
	apart: number | undefined;
};

export type PlayerImpact = {
	season: number;
	tid: number;
	// His own possessions and net rating on the floor.
	poss: number;
	net: number;
	partners: ImpactPartner[];
};

// Below this a number is noise dressed up as a measurement.
const MIN_POSS = 200;

type Totals = { poss: number; margin: number };

const add = (totals: Map<number, Totals>, pid: number, row: Totals) => {
	const existing = totals.get(pid);
	if (existing) {
		existing.poss += row.poss;
		existing.margin += row.margin;
	} else {
		totals.set(pid, { ...row });
	}
};

const per100 = ({ poss, margin }: Totals) => (100 * margin) / poss;

export const playerImpactFromGames = (
	games: readonly Game[],
	pid: number,
	tid: number,
): PlayerImpact | undefined => {
	const on: Totals = { poss: 0, margin: 0 };
	const withPartner = new Map<number, Totals>();

	for (const game of games) {
		if (game.playoffs || !game.shifts) {
			continue;
		}

		const t =
			game.teams[0].tid === tid ? 0 : game.teams[1].tid === tid ? 1 : -1;
		if (t < 0) {
			continue;
		}

		for (const shift of decodeShifts(game)) {
			const lineup = shift.lineups[t]!;
			if (!lineup.includes(pid)) {
				continue;
			}

			// One possession apiece for each side, so a stint counts once from
			// the perspective of the ten men who played it.
			const row = {
				poss: shift.poss[0] + shift.poss[1],
				margin:
					t === 0 ? shift.pts[0] - shift.pts[1] : shift.pts[1] - shift.pts[0],
			};
			if (row.poss === 0) {
				continue;
			}

			on.poss += row.poss;
			on.margin += row.margin;
			for (const other of lineup) {
				if (other !== pid) {
					add(withPartner, other, row);
				}
			}
		}
	}

	if (on.poss < MIN_POSS) {
		return undefined;
	}

	const partners: ImpactPartner[] = [];
	for (const [partnerPid, together] of withPartner) {
		if (together.poss < MIN_POSS) {
			continue;
		}

		const rest: Totals = {
			poss: on.poss - together.poss,
			margin: on.margin - together.margin,
		};

		partners.push({
			pid: partnerPid,
			poss: together.poss,
			together: per100(together),
			apart: rest.poss >= MIN_POSS ? per100(rest) : undefined,
		});
	}

	partners.sort((a, b) => b.poss - a.poss);

	return {
		season: games[0]?.season ?? 0,
		tid,
		poss: on.poss,
		net: per100(on),
		partners,
	};
};

// This season only. Its games are the ones already sitting in the cache, so
// this costs a walk over one team's schedule; every other season would mean
// reading the whole league's games off disk to render a page.
export const getPlayerImpact = async (
	pid: number,
	tid: number,
	season: number,
): Promise<PlayerImpact | undefined> => {
	const games = (await idb.cache.games.getAll()).filter(
		(game) =>
			game.season === season &&
			!game.playoffs &&
			(game.teams[0].tid === tid || game.teams[1].tid === tid),
	);

	if (games.length === 0) {
		return undefined;
	}

	return playerImpactFromGames(games, pid, tid);
};
