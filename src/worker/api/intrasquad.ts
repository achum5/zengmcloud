import {
	DEFAULT_PLAY_THROUGH_INJURIES,
	DEFAULT_STADIUM_CAPACITY,
	DEFAULT_TEAM_COLORS,
	PHASE,
} from "../../common/constants.ts";
import type { Conditions, Player, Team } from "../../common/types.ts";
import { GameSim, team } from "../core/index.ts";
import { processTeam } from "../core/game/loadTeams.ts";
import { gameSimToBoxScore } from "../core/game/writeGameStats.ts";
import { idb } from "../db/index.ts";
import { g, toUI } from "../util/index.ts";
import { boxScoreToLiveSim } from "../views/liveGame.ts";
import type { TeamSeasonOverride } from "../views/gameLog.ts";
import { isSport } from "../../common/sportFunctions.ts";
import { randInt } from "../../common/random.ts";

// An intrasquad scrimmage: one team split into two squads (Primary vs
// Secondary) plays a one-off game, exactly like an exhibition game but sourced
// from a single roster and run from INSIDE the open league. The whole sim is
// display-only (never written to the league DB), and any in-memory game state
// it has to touch is snapshotted and restored so the league is left untouched.
export const simIntrasquadGame = async (
	{
		tid,
		squads,
	}: {
		tid: number;
		// [primaryPids, secondaryPids], each already in the user's chosen order
		// (index 0 = the squad's top player, which becomes a starter).
		squads: [number[], number[]];
	},
	conditions: Conditions,
) => {
	const t = await idb.cache.teams.get(tid);
	if (!t) {
		throw new Error("Invalid team.");
	}

	// Fresh copies so the sim's mutations never touch the league's cached
	// players. Deliberately NOT "noCopyCache": that flag is the caller promising
	// not to mutate, and it hands back the live cache rows - so `collect` below,
	// which rewrites rosterOrder to match the squads the user dragged together,
	// was reordering the actual roster and publishing it to the room. A
	// scrimmage is a friendly; none of it is meant to reach league state.
	const allPlayers = await idb.getCopies.players({ tid });
	const byPid = new Map(allPlayers.map((p) => [p.pid, p]));

	const collect = (pids: number[]): Player[] => {
		const out: Player[] = [];
		for (const [i, pid] of pids.entries()) {
			const p = byPid.get(pid);
			if (p) {
				// Honor the drag order: the sim starts players by rosterOrder, so make
				// it match the order the user arranged this squad in.
				p.rosterOrder = i;
				out.push(p);
			}
		}
		return out;
	};

	const squadPlayers: [Player[], Player[]] = [
		collect(squads[0]),
		collect(squads[1]),
	];
	if (squadPlayers[0].length < 5 || squadPlayers[1].length < 5) {
		throw new Error("Each squad needs at least 5 players to run a scrimmage.");
	}

	// Both squads wear the team's colors; the secondary squad swaps primary and
	// secondary so the two are visually distinct on the court (like light/dark
	// jerseys in a real scrimmage).
	const colors = t.colors ?? DEFAULT_TEAM_COLORS;
	const squadInfo = [
		{ name: "Primary", abbrev: "PRI", colors },
		{
			name: "Secondary",
			abbrev: "SEC",
			colors: [colors[1], colors[0], colors[2]] as [string, string, string],
		},
	];

	// This runs inside the open league (unlike the non-league exhibition), so
	// snapshot every in-memory game attribute the sim touches and restore it
	// afterward - nothing here is meant to persist or leak into league state.
	const prevPhase = g.get("phase");
	const prevUserTids = g.get("userTids");
	const prevUserTid = g.get("userTid");

	try {
		// Regular-season phase skips the playoff-series DB access in
		// gameSimToBoxScore; both squads are "user" teams for a friendly game.
		g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
		g.setWithoutSavingToDB("userTids", [0, 1]);
		g.setWithoutSavingToDB("userTid", 0);

		const dh = false;

		const teamsProcessed = (await Promise.all(
			squadPlayers.map(async (players, squadTid) => {
				let depth: Team["depth"];
				if (!isSport("basketball")) {
					depth = await team.genDepth(players);
				}

				return processTeam(
					{
						tid: squadTid,
						playThroughInjuries: DEFAULT_PLAY_THROUGH_INJURIES,
						depth,
					},
					{ won: 0, lost: 0, tied: 0, otl: 0, cid: 0, did: 0 },
					players,
					true,
				);
			}),
		)) as [any, any];

		for (const tp of teamsProcessed) {
			if (tp.depth !== undefined) {
				tp.depth = team.getDepthPlayers(tp.depth, tp.player, dh);
			}
		}

		// A fresh random gid keeps processLiveGameEvents from reusing another
		// game's player cache.
		const gid = randInt(0, 1000000000);

		const result = new GameSim({
			gid,
			day: -1,
			teams: teamsProcessed,
			doPlayByPlay: true,
			homeCourtFactor: 1,
			// No real home team in a scrimmage.
			neutralSite: true,
			allStarGame: false,
			baseInjuryRate: g.get("injuryRate"),
			dh,
		}).run();

		const { gameStats: boxScore } = await gameSimToBoxScore(
			result,
			DEFAULT_STADIUM_CAPACITY,
		);

		// Both squads are this team, so both play on this team's floor. Stated
		// here because the squads are numbered 0 and 1 for the sim, and those
		// are real tids belonging to other teams - without this the scrimmage
		// borrowed their courts (see boxScoreToLiveSim).
		const teamSeasonOverrides = [0, 1].map((i) => ({
			region: t.region,
			name: squadInfo[i]!.name,
			abbrev: squadInfo[i]!.abbrev,
			imgURL: t.imgURL,
			imgURLSmall: t.imgURLSmall,
			colors: squadInfo[i]!.colors,
			court: t.court,
		})) as [TeamSeasonOverride, TeamSeasonOverride];

		const liveSim = await boxScoreToLiveSim({
			allStars: undefined,
			confetti: false,
			boxScore,
			playByPlay: result.playByPlay as any,
			teamSeasonOverrides,
		});

		// The single flag that keeps the whole thing out of the league (no record
		// changes, no box-score save, unrecoverable-warning on navigate).
		liveSim.initialBoxScore.exhibition = true;

		await toUI(
			"realtimeUpdate",
			[
				[],
				`/l/${g.get("lid")}/intrasquad_game`,
				{
					liveSim,
					abbrev: t.abbrev,
				},
			],
			conditions,
		);
	} finally {
		g.setWithoutSavingToDB("phase", prevPhase);
		g.setWithoutSavingToDB("userTids", prevUserTids);
		g.setWithoutSavingToDB("userTid", prevUserTid);
	}
};
