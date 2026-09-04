// LINEUP SCORING, PACKED SMALL ENOUGH TO KEEP.
//
// A season's worth of ten-man matchups is the raw material for RAPM (see
// rapm.ts), and it has to survive from the night a game is played until the
// regular season ends. That means it lives on the game row, next to the box
// score, which also means it inherits the box score's lifetime: leagues drop
// old games after a couple of seasons, and these go with them.
//
// So it is stored as a flat run of integers rather than a list of objects,
// because the field names would cost more than the numbers do. Each matchup
// takes `2 * numPlayersOnCourt + 4` of them: the two lineups, then possessions
// and points for each side.

import type { Game } from "../../common/types.ts";
import type { GameShift } from "../core/GameSim.basketball/shiftLog.ts";

export const encodeShifts = (
	shifts: readonly GameShift[],
	numPlayersOnCourt: number,
): number[] => {
	const out: number[] = [];
	for (const shift of shifts) {
		// A lineup that is short a man (an ejection with nobody left to sub in)
		// would break the fixed stride, and it is not worth a variable one.
		if (
			shift.lineups[0].length !== numPlayersOnCourt ||
			shift.lineups[1].length !== numPlayersOnCourt
		) {
			continue;
		}
		out.push(
			...shift.lineups[0],
			...shift.lineups[1],
			shift.poss[0],
			shift.poss[1],
			shift.pts[0],
			shift.pts[1],
		);
	}
	return out;
};

export type DecodedShift = {
	lineups: [number[], number[]];
	poss: [number, number];
	pts: [number, number];
};

export const decodeShifts = (
	game: Pick<Game, "shifts" | "numPlayersOnCourt">,
): DecodedShift[] => {
	const { shifts, numPlayersOnCourt } = game;
	if (!shifts || !numPlayersOnCourt || numPlayersOnCourt < 1) {
		return [];
	}

	const stride = 2 * numPlayersOnCourt + 4;
	const out: DecodedShift[] = [];
	// A truncated tail means the row was damaged somewhere; take what parses.
	for (let i = 0; i + stride <= shifts.length; i += stride) {
		out.push({
			lineups: [
				shifts.slice(i, i + numPlayersOnCourt),
				shifts.slice(i + numPlayersOnCourt, i + 2 * numPlayersOnCourt),
			],
			poss: [
				shifts[i + 2 * numPlayersOnCourt]!,
				shifts[i + 2 * numPlayersOnCourt + 1]!,
			],
			pts: [
				shifts[i + 2 * numPlayersOnCourt + 2]!,
				shifts[i + 2 * numPlayersOnCourt + 3]!,
			],
		});
	}
	return out;
};
