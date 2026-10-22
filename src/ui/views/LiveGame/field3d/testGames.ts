import { DEFAULT_LEVEL } from "../../../../common/budgetLevels.ts";
import { makeSeededRandom } from "../../../../common/random.ts";
import { range } from "../../../../common/utils.ts";
import { resetCache, resetG } from "../../../../test/helpers.ts";
import GameSim from "../../../../worker/core/GameSim.football/index.ts";
import loadTeams from "../../../../worker/core/game/loadTeams.ts";
import { player, team } from "../../../../worker/core/index.ts";
import { g, helpers } from "../../../../worker/util/index.ts";
import type { RawEvent } from "./director.ts";
import { GROUPS, type RosterMan } from "./personnel.ts";

// FOR THE TESTS: real football games, played by the sim between two made-up
// teams - whatever it says happened, the field has to stage.

export type SimGame = { events: RawEvent[]; roster: RosterMan[] };

export const simGames = async (gids: number[]): Promise<SimGame[]> => {
	// The same games every run, so whatever goes wrong goes wrong again.
	const random = Math.random;
	Math.random = makeSeededRandom(gids[0] ?? 1);
	try {
		return await play(gids);
	} finally {
		Math.random = random;
	}
};

const play = async (gids: number[]): Promise<SimGame[]> => {
	resetG();
	g.setWithoutSavingToDB("season", 2013);
	const teamsDefault = helpers.getTeamsDefault().slice(0, 2);
	await resetCache({
		// (Each his own name: a two-point try names its men, nothing more.)
		players: [0, 1].flatMap((tid) =>
			range(53).map((k) => ({
				...player.generate(tid, 25, 2010, true, DEFAULT_LEVEL),
				firstName: `Player${tid}`,
				lastName: String(k),
			})),
		),
		teams: teamsDefault.map(team.generate),
		teamSeasons: teamsDefault.map((t) => team.genSeasonRow(t)),
		teamStats: teamsDefault.map((t) => team.genStatsRow(t.tid)),
	});
	const games: SimGame[] = [];
	for (const gid of gids) {
		const teams = await loadTeams([0, 1], {});
		for (const t of [teams[0], teams[1]]) {
			// Each group's chart, best first - the way a team's own is set.
			const score = (p: any, pos: string) =>
				(p.ovrs[pos] ?? 0) + (p.pos === pos ? 15 : 0);
			const depth: any = {};
			for (const pos of GROUPS) {
				depth[pos] = [...t.player].sort(
					(a: any, b: any) => score(b, pos) - score(a, pos),
				);
			}
			t.depth = depth;
		}
		const sim = new GameSim({
			gid,
			teams: [teams[0], teams[1]],
			baseInjuryRate: g.get("injuryRate"),
			doPlayByPlay: true,
			homeCourtFactor: 1,
			allStarGame: false,
			neutralSite: false,
		});
		const events = sim.run().playByPlay as RawEvent[];
		// (Raw team 0 is the home team: display side 1.)
		const raw = events[0]!.boxScore;
		const roster: RosterMan[] = [];
		for (const rt of [0, 1] as const) {
			for (const p of raw[rt].player) {
				roster.push({
					pid: p.id,
					team: rt === 0 ? 1 : 0,
					pos: p.pos,
					name: p.name,
				});
			}
		}
		games.push({ events, roster });
	}
	return games;
};
