import { DEFAULT_PLAY_THROUGH_INJURIES } from "../../common/constants.ts";
import type { Player } from "../../common/types.ts";
import GameSim from "../../worker/core/GameSim.ts";
import { processTeam } from "../../worker/core/game/loadTeams.ts";

// Playing single games outside a league's schedule, for the labs: any roster
// against any other, with the game's own sim and no injuries.

export type SimTeam = Awaited<ReturnType<typeof processTeam>>;

// The sim plays a roster in its rosterOrder.
export const setOrder = (roster: Player[]) => {
	for (const [i, p] of roster.entries()) {
		p.rosterOrder = i;
	}
};

export const toSimTeam = async (tid: number, roster: Player[]) =>
	processTeam(
		{
			tid,
			playThroughInjuries: DEFAULT_PLAY_THROUGH_INJURIES,
			depth: undefined,
		} as any,
		{ won: 0, lost: 0, tied: 0, otl: 0, cid: 0, did: 0 },
		roster,
	);

let gid = 0;
export const play = (t0: SimTeam, t1: SimTeam) =>
	new GameSim({
		gid: gid++,
		day: 1,
		teams: [t0, t1] as any,
		doPlayByPlay: false,
		homeCourtFactor: 1,
		neutralSite: false,
		allStarGame: false,
		baseInjuryRate: 0,
	} as any).run() as any;
