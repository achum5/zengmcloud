import { player, team } from "../core/index.ts";
import { getFollowedBroadcastPayload } from "../core/sync/connect.ts";
import { idb } from "../db/index.ts";
import { g, helpers } from "../util/index.ts";
import {
	makeAbbrevsUnique,
	setTeamInfo,
	type TeamSeasonOverride,
} from "./gameLog.ts";
import { takeArenaLooks } from "../core/game/replayLooks.ts";
import type {
	AllStars,
	ArenaLooks,
	CourtStyle,
	JerseySkinIds,
	Game,
} from "../../common/types.ts";
import { defineView } from "../util/defineView.ts";
import { STARTING_NUM_TIMEOUTS } from "../../common/constants.ts";
import { formatClock } from "../../common/formatClock.ts";
import { getPeriodName } from "../../common/getPeriodName.ts";
import { bySport } from "../../common/sportFunctions.ts";
import type { RouteParams } from "../../ui/router/types.ts";

const processInputs = (params: RouteParams<"liveGame">, ctxBBGM: any) => {
	const obj: {
		fromAction: boolean;
		gid?: number;
		playByPlay?: unknown[];
		// A multiplayer follower watching someone else's live sim: the game record
		// travels in the payload (rather than being read from idb) so the view
		// doesn't depend on the separate changeset sync having landed it yet.
		boxScore?: any;
		mpFollower?: boolean;
		// A rewatch of a saved live game (not a fresh sim). Shows a "Replay" label
		// and doesn't set the live-sim-in-progress lock.
		replay?: boolean;
	} = {
		fromAction: !!ctxBBGM.fromAction,
	};

	if (ctxBBGM.playByPlay !== undefined) {
		obj.gid = ctxBBGM.gidOneGame;
		obj.playByPlay = ctxBBGM.playByPlay;
		obj.boxScore = ctxBBGM.boxScore;
		obj.mpFollower = !!ctxBBGM.mpFollower;
		obj.replay = !!ctxBBGM.replay;
	}

	return obj;
};

// IS THIS THE CHAMPIONSHIP? Drives the trophy at center court in the live-game
// graphic, and the confetti when the series ends.
//
// BOTH ARE FACTS ABOUT THE GAME BEING WATCHED, and the game record carries
// them: writeGameStats stamps `finals` on a final-round game and stores each
// side's series record as it stood after it. This used to be re-derived from
// the league as it is RIGHT NOW - the current phase, the current season's
// playoffSeries, whether the current round happens to be the last one - which
// gives the same answer only while the game is being played for the first
// time. Watch that same game back in replay and every one of those reads
// describes some other moment, so the trophy went missing from every finals
// rewatch. Reported from a live league.
//
// Pure, and takes only the game, which is the whole point.
export const championshipStakes = (game: {
	finals?: boolean;
	numGamesToWinSeries?: number;
	teams: [{ playoffs?: { won: number } }, { playoffs?: { won: number } }];
}): { finals: boolean; confetti: boolean } => {
	const finals = game.finals === true;
	return {
		finals,
		// The series ended here: one side reached the wins it needed, counting
		// this game. Only ever celebrated for the final round - winning a
		// semi-final is not a championship.
		confetti:
			finals &&
			game.numGamesToWinSeries !== undefined &&
			Math.max(
				game.teams[0].playoffs?.won ?? 0,
				game.teams[1].playoffs?.won ?? 0,
			) >= game.numGamesToWinSeries,
	};
};

// WHICH COURT A LIVE-SIM SIDE IS DRAWN ON.
//
// Normally the team's own, found by tid. But a SYNTHETIC GAME'S tid IS AN
// ARRAY INDEX, NOT A TEAM: an intrasquad scrimmage numbers its two squads 0
// and 1, and an exhibition numbers its two sides the same way, so the lookup
// handed those games whatever courts the league's first two teams happen to
// own. The field report is a Cleveland scrimmage wearing Cleveland's logo and
// colors - which came from the override - on Boston's parquet, green key and
// TD Garden rails, which came from tid 1. Half one court, half another.
//
// An override is a synthetic game saying who its teams really are, so it
// settles the court too, INCLUDING saying there is not one: an exhibition
// between two historical teams belongs on neutral hardwood, not on whatever
// floor tid 0 owns today.
export const liveSimCourt = ({
	override,
	teamCourt,
}: {
	override: TeamSeasonOverride | undefined;
	// The court belonging to the league team with this side's tid. Only read
	// when there is no override, because only then does the tid mean anything.
	teamCourt: CourtStyle | undefined;
}): CourtStyle | undefined => (override ? override.court : teamCourt);

export const boxScoreToLiveSim = async ({
	allStars,
	boxScore,
	confetti,
	playByPlay,
	teamSeasonOverrides,
}: {
	allStars: AllStars | undefined;
	boxScore: Game;
	confetti: boolean;
	playByPlay: unknown[];
	teamSeasonOverrides?: [TeamSeasonOverride, TeamSeasonOverride];
}) => {
	const otl = g.get("otl", "current");

	// Stats to set to 0
	const resetStatsPlayer = [...player.stats.raw];
	if (player.stats.byPos) {
		resetStatsPlayer.push(...player.stats.byPos);
	}
	const resetStatsTeam = [...team.stats.raw];
	if (team.stats.byPos) {
		resetStatsTeam.push(...team.stats.byPos);
	}

	const initialBoxScore: any = boxScore;

	if (__SPORT === "basketball") {
		resetStatsTeam.push("ba");

		initialBoxScore.elam = allStars ? g.get("elamASG") : g.get("elam");
		initialBoxScore.elamOvertime = g.get("elamOvertime");
	}

	initialBoxScore.overtime = "";
	initialBoxScore.quarter = "";

	// Initialize quarterShort so there is something to display immediately
	initialBoxScore.quarterShort = bySport({
		baseball: "1",
		default:
			initialBoxScore.numPeriods === 0
				? "OT"
				: `${getPeriodName(initialBoxScore.numPeriods, true)}1`,
	});

	// Basketball clock is in seconds
	const clock =
		__SPORT === "basketball"
			? g.get("quarterLength") * 60
			: g.get("quarterLength");
	initialBoxScore.time = formatClock(clock);
	initialBoxScore.gameOver = false;
	delete initialBoxScore.shootout;

	for (const i of [0, 1] as const) {
		const t = initialBoxScore.teams[i];

		// Fix records, taking out result of this game
		// Keep in sync with LiveGame.tsx
		if (initialBoxScore.playoffs) {
			if (t.playoffs) {
				if (initialBoxScore.won.tid === t.tid) {
					t.playoffs.won -= 1;
				} else if (initialBoxScore.lost.tid === t.tid) {
					t.playoffs.lost -= 1;
				}
			}
		} else {
			if (
				initialBoxScore.won.pts === initialBoxScore.lost.pts &&
				initialBoxScore.won.sPts === initialBoxScore.lost.sPts
			) {
				// Tied!
				if (t.tied !== undefined) {
					t.tied -= 1;
				}
			} else if (initialBoxScore.won.tid === t.tid) {
				t.won -= 1;
			} else if (initialBoxScore.lost.tid === t.tid) {
				if (initialBoxScore.overtimes > 0 && otl) {
					t.otl -= 1;
				} else {
					t.lost -= 1;
				}
			}
		}

		await setTeamInfo(
			t,
			i,
			allStars,
			initialBoxScore,
			teamSeasonOverrides?.[i],
		);
		// Attach the team's custom court style (basketball) so the live-game court
		// graphic can draw the home team's court.
		//
		// A SYNTHETIC GAME'S tid IS AN ARRAY INDEX, NOT A TEAM. An intrasquad
		// scrimmage numbers its two squads 0 and 1, and an exhibition numbers
		// its two sides the same way, so looking the court up by tid handed
		// those games whatever courts the league's first two teams happen to
		// own. The field report: a Cleveland scrimmage drawn with Cleveland's
		// logo and colors - which come from the override - on Boston's parquet,
		// green key and TD Garden rails, which came from tid 1. Half one court,
		// half another.
		//
		// An override is a synthetic game saying who its teams really are, so
		// it settles the court too, INCLUDING saying there is not one: an
		// exhibition between two historical teams belongs on neutral hardwood,
		// not on whatever floor tid 0 owns today.
		if (__SPORT === "basketball") {
			const override = teamSeasonOverrides?.[i];
			let teamCourt: CourtStyle | undefined;
			let worn: { jersey?: string; jerseySkins?: JerseySkinIds } | undefined;
			// Only worth asking when the tid is a real team AND nothing has
			// already said otherwise - the lookup is the whole fault above.
			if (!override && t.tid >= 0) {
				try {
					const team = await idb.cache.teams.get(t.tid);
					teamCourt = team?.court;
					worn = team;
				} catch {
					// Court styling is cosmetic; fall back to defaults.
				}
			}
			t.court = liveSimCourt({ override, teamCourt });
			// What the team wears in the 2.5D game: the jersey it has made its
			// own, if it has, and its uniforms drawn from pictures.
			if (worn?.jersey !== undefined) {
				t.jersey = worn.jersey;
			}
			if (worn?.jerseySkins !== undefined) {
				t.jerseySkins = worn.jerseySkins;
			}
		}
		t.ptsQtrs = [];

		for (const stat of resetStatsTeam) {
			if (Object.hasOwn(t, stat)) {
				t[stat] = 0;
			}
		}

		// Special reset of shootout stats to undefined, since that is used in the UI to identify if we're in a shootout yet
		delete t.sPts;
		delete t.sAtt;

		for (let j = 0; j < t.players.length; j++) {
			const p = t.players[j];

			// Fix for players who were hurt this game - don't show right away! And handle players playing through an injury who were injured again.
			if (p.injury.newThisGame) {
				p.injury = p.injuryAtStart
					? {
							...p.injuryAtStart,
							playingThrough: true,
						}
					: {
							type: "Healthy",
							gamesRemaining: 0,
						};
			}

			for (const stat of resetStatsPlayer) {
				if (Object.hasOwn(p, stat)) {
					p[stat] = 0;
				}
			}
		}
	}
	makeAbbrevsUnique(initialBoxScore.teams);

	// For FBGM, build up scoringSummary from events, to handle deleting a score due to penalty
	if (
		bySport({
			baseball: true,
			basketball: false,
			football: true,
			hockey: true,
		})
	) {
		initialBoxScore.scoringSummary = [];
	}

	if (STARTING_NUM_TIMEOUTS !== undefined) {
		initialBoxScore.teams[0].timeouts = STARTING_NUM_TIMEOUTS;
		initialBoxScore.teams[1].timeouts = STARTING_NUM_TIMEOUTS;
	}

	return {
		confetti,
		events: playByPlay,
		initialBoxScore,
		otl,
		quarterLength: g.get("quarterLength"),
	};
};

export default defineView({
	id: "liveGame",
	processInputs,
	load: async ({ inputs, updateEvents }) => {
		const redirectToMenu = {
			redirectUrl: helpers.leagueUrl(["daily_schedule"]),
		};

		// A follower already parked on this page can miss the navigation that
		// carries a new broadcast's payload (same-URL refreshes can be dropped by
		// the view queue), leaving it replaying the PREVIOUS game's props. The sync
		// layer caches the followed broadcast's payload, so a plain load of this
		// page (or an explicit "mpLiveBroadcast" refresh from the recovery effect)
		// can serve the current broadcast without the navigation.
		let { gid, playByPlay } = inputs;
		let inputBoxScore = inputs.boxScore;
		if (
			(playByPlay === undefined || playByPlay.length === 0) &&
			(updateEvents.has("firstRun") || updateEvents.has("mpLiveBroadcast"))
		) {
			const payload = getFollowedBroadcastPayload();
			if (payload) {
				gid = payload.gid;
				playByPlay = payload.playByPlay;
				// boxScoreToLiveSim mutates the box score in place, so hand it a copy -
				// a later load of this page needs the cached one pristine.
				inputBoxScore = helpers.deepCopy(payload.boxScore);
			}
		}

		if (
			updateEvents.has("firstRun") &&
			!inputs.fromAction &&
			(playByPlay === undefined || playByPlay.length === 0)
		) {
			return redirectToMenu;
		}

		if (
			gid !== undefined &&
			playByPlay !== undefined &&
			playByPlay.length > 0
		) {
			// A multiplayer follower gets the game record in the broadcast payload, so
			// it doesn't have to wait for the separate changeset sync to land the game
			// row before it can render the live sim. Everyone else reads it from idb.
			const boxScore = inputBoxScore ?? (await idb.getCopy.games({ gid }));

			if (!boxScore) {
				throw new Error("Invalid gid");
			}

			const allStarGame =
				boxScore.teams[0].tid === -1 || boxScore.teams[1].tid === -1;
			let allStars;

			if (allStarGame) {
				allStars = await idb.cache.allStars.get(g.get("season"));

				if (!allStars) {
					return redirectToMenu;
				}
			}

			const { finals, confetti } = championshipStakes(boxScore);

			const out = await boxScoreToLiveSim({
				allStars,
				boxScore,
				confetti,
				playByPlay,
			});
			(out.initialBoxScore as any).finals = finals;

			// A rewatch of a saved game: flag it and build a small "2026 Playoffs" /
			// "2026 Regular Season" label for the header.
			let arenaThen: ArenaLooks | undefined;
			if (inputs.replay) {
				(out.initialBoxScore as any).replay = true;
				const label = boxScore.playoffs
					? `${boxScore.season} Playoffs`
					: `${boxScore.season} Regular Season`;
				(out.initialBoxScore as any).replayLabel = label;
				// How everyone looked that night, for the 2.5D court.
				try {
					const row =
						(await idb.cache.liveGamePlayByPlay.get(gid)) ??
						(await (idb.league as any).get("liveGamePlayByPlay", gid));
					if (row?.looks) {
						(out.initialBoxScore as any).replayLooks = row.looks;
						arenaThen = row.looks.arena;
					}
				} catch {
					// Cosmetic - today's looks it is.
				}
			}
			// The home team's building, for the 2.5D court: as it was that night
			// on a replay that kept it, otherwise as of the game's season.
			(out.initialBoxScore as any).arena =
				arenaThen ??
				(await takeArenaLooks(boxScore.teams[0].tid, boxScore.season));

			return out;
		}
	},
});
