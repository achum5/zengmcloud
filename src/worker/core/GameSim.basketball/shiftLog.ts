// WHO WAS ON THE FLOOR, AND WHAT HAPPENED WHILE THEY WERE.
//
// A box score says what a player did. It cannot say what the game did while he
// played, and that is the only evidence there is about the half of basketball
// that never touches the ball. This records it: every distinct ten-man matchup
// a game produces, with the possessions, points and time each side got out of
// it.
//
// It is deliberately tiny. Matchups repeat constantly - the same starters face
// each other all night - so they are folded together as they recur, which
// leaves a game with a few dozen rows instead of a few hundred.

import type { TeamNum } from "../../../common/types.ts";

export type GameShift = {
	// On the floor, team 0 then team 1, each sorted so the same five always
	// looks the same.
	lineups: [number[], number[]];
	// Possessions each team had while this ten were out there.
	poss: [number, number];
	// Points each team scored in them.
	pts: [number, number];
	// Seconds of game they played together.
	sec?: number;
};

// The box score stats a player's time on the floor is judged by, in the order
// they are kept: his team's, then the other team's, while he was out there.
// See gameShifts.ts for how they are stored.
export const ON_FLOOR_STATS = [
	"fg",
	"fga",
	"tp",
	"tpa",
	"fta",
	"orb",
	"drb",
	"ast",
	"blk",
	"stl",
	"tov",
] as const;
export type OnFloorStat = (typeof ON_FLOOR_STATS)[number];

const STAT_INDEX: Record<string, number | undefined> = Object.fromEntries(
	ON_FLOOR_STATS.map((s, i) => [s, i]),
);

const signature = (lineups: [number[], number[]]) =>
	`${lineups[0].join(",")}|${lineups[1].join(",")}`;

export class ShiftLog {
	private byLineup = new Map<string, GameShift>();
	private current: GameShift | undefined;
	// Per player, what the box score did while he was on the floor: his
	// team's stats, then the other team's (ON_FLOOR_STATS, twice).
	private onFloor = new Map<number, number[]>();

	// The floor changed. Everything from here belongs to the new ten.
	setLineups(zero: readonly number[], one: readonly number[]) {
		const lineups: [number[], number[]] = [
			[...zero].sort((a, b) => a - b),
			[...one].sort((a, b) => a - b),
		];

		const key = signature(lineups);
		let shift = this.byLineup.get(key);
		if (!shift) {
			shift = { lineups, poss: [0, 0], pts: [0, 0], sec: 0 };
			this.byLineup.set(key, shift);
		}
		this.current = shift;
	}

	addPossession(t: TeamNum) {
		if (this.current) {
			this.current.poss[t] += 1;
		}
	}

	addTime(sec: number) {
		if (this.current) {
			this.current.sec = (this.current.sec ?? 0) + sec;
		}
	}

	addPoints(t: TeamNum, amount: number) {
		if (this.current) {
			this.current.pts[t] += amount;
		}
	}

	// A box score stat for team t, to everybody on the floor for it.
	addStat(t: TeamNum, stat: string, amount: number) {
		const i = STAT_INDEX[stat];
		if (i === undefined || !this.current) {
			return;
		}
		for (const side of [0, 1] as const) {
			const at = (side === t ? 0 : ON_FLOOR_STATS.length) + i;
			for (const pid of this.current.lineups[side]) {
				let box = this.onFloor.get(pid);
				if (!box) {
					box = new Array(2 * ON_FLOOR_STATS.length).fill(0);
					this.onFloor.set(pid, box);
				}
				box[at]! += amount;
			}
		}
	}

	// (Plain pairs, not the map: results get copied around.)
	getOnFloor(): [number, number[]][] {
		return [...this.onFloor];
	}

	// Matchups that produced something. A lineup that came and went inside one
	// dead ball has nothing to say.
	getShifts(): GameShift[] {
		return [...this.byLineup.values()].filter(
			(shift) => shift.poss[0] > 0 || shift.poss[1] > 0,
		);
	}
}
