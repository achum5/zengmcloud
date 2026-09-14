import { assert, test } from "vitest";
import { resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import { player } from "../index.ts";
import GameSim from "./index.ts";
import { processTeam } from "../game/loadTeams.ts";
import { DEFAULT_PLAY_THROUGH_INJURIES } from "../../../common/constants.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { synergyForLineup } from "./synergy.ts";
import type { Player } from "../../../common/types.ts";

// The sim memoizes each player's fractional skills (see synergySkillsCache),
// on the understanding that composite ratings do not change during a game.
// They change once: homeCourtAdvantage() rescales every composite in the
// constructor, AFTER the opening lineup has already filled the memo. Left
// alone, every synergy recomputation for the rest of the game reads the
// pre-home-court skills - a small, silent drift from what the sim computed
// before the memo existed. So the memo has to be dropped when the ratings
// are rescaled; this test holds it to that.
test("lineup synergy tracks the home-court-scaled ratings", async () => {
	resetG();
	g.setWithoutSavingToDB("userTids", []);
	g.setWithoutSavingToDB("userTid", 0);

	const sides = [];
	for (const tid of [0, 1]) {
		const players: Player[] = [];
		for (let i = 0; i < 13; i++) {
			const p = player.generate(tid, 22 + (i % 12), 2016, true, DEFAULT_LEVEL);
			p.pid = tid * 100 + i;
			p.stats = [];
			p.injuries = [];
			players.push(p as Player);
		}
		sides.push(
			await processTeam(
				{
					tid,
					playThroughInjuries: DEFAULT_PLAY_THROUGH_INJURIES,
					depth: undefined,
				} as any,
				{ won: 0, lost: 0, tied: 0, otl: 0, cid: 0, did: 0 } as any,
				players,
			),
		);
	}

	const sim = new GameSim({
		gid: 1,
		day: 1,
		teams: sides as any,
		doPlayByPlay: false,
		homeCourtFactor: 1.5,
		neutralSite: false,
		allStarGame: false,
		baseInjuryRate: 0,
	} as any);

	// The constructor has filled the memo (opening lineup) and then rescaled
	// every rating for home court. The next recomputation - what every
	// substitution does - must match what the current ratings say.
	sim.updateSynergy();
	for (const t of [0, 1] as const) {
		const fresh = synergyForLineup(
			sim.playersOnCourt[t].slice(0, sim.numPlayersOnCourt),
		);
		assert.deepStrictEqual(sim.team[t].synergy, fresh);
	}
});
