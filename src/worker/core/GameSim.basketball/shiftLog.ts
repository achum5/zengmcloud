// WHO WAS ON THE FLOOR, AND WHAT HAPPENED WHILE THEY WERE.
//
// A box score says what a player did. It cannot say what the game did while he
// played, and that is the only evidence there is about the half of basketball
// that never touches the ball. This records it: every distinct ten-man matchup
// a game produces, with the possessions and points each side got out of it.
//
// It is deliberately tiny. Matchups repeat constantly - the same starters face
// each other all night - so they are folded together as they recur, which
// leaves a game with a few dozen rows instead of a few hundred, and leaves the
// season's regression with less to chew through.

import type { TeamNum } from "../../../common/types.ts";

export type GameShift = {
	// On the floor, team 0 then team 1, each sorted so the same five always
	// looks the same.
	lineups: [number[], number[]];
	// Possessions each team had while this ten were out there.
	poss: [number, number];
	// Points each team scored in them.
	pts: [number, number];
};

const signature = (lineups: [number[], number[]]) =>
	`${lineups[0].join(",")}|${lineups[1].join(",")}`;

export class ShiftLog {
	private byLineup = new Map<string, GameShift>();
	private current: GameShift | undefined;

	// The floor changed. Everything from here belongs to the new ten.
	setLineups(zero: readonly number[], one: readonly number[]) {
		const lineups: [number[], number[]] = [
			[...zero].sort((a, b) => a - b),
			[...one].sort((a, b) => a - b),
		];

		const key = signature(lineups);
		let shift = this.byLineup.get(key);
		if (!shift) {
			shift = { lineups, poss: [0, 0], pts: [0, 0] };
			this.byLineup.set(key, shift);
		}
		this.current = shift;
	}

	addPossession(t: TeamNum) {
		if (this.current) {
			this.current.poss[t] += 1;
		}
	}

	addPoints(t: TeamNum, amount: number) {
		if (this.current) {
			this.current.pts[t] += amount;
		}
	}

	// Matchups that produced something. A lineup that came and went inside one
	// dead ball has nothing to say.
	getShifts(): GameShift[] {
		return [...this.byLineup.values()].filter(
			(shift) => shift.poss[0] > 0 || shift.poss[1] > 0,
		);
	}
}
