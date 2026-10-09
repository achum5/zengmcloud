import { PHASE } from "../../common/constants.ts";
import { g } from "../util/index.ts";
import { defineView } from "../util/defineView.ts";
import { getPlayers } from "./playerRatings.ts";
import addFirstNameShort from "../util/addFirstNameShort.ts";
import { idb } from "../db/index.ts";
import { getActualPlayThroughInjuries } from "../core/game/loadTeams.ts";
import { actualPhase } from "../util/actualPhase.ts";
import { bySport } from "../../common/sportFunctions.ts";
import { exemptFromCoarseRatings } from "../../common/coarsenRating.ts";
import coarsenInjuryDrops from "../core/player/coarsenInjuryDrops.ts";
import { REMAINING_PLAYOFF_TEAMS_PHASES } from "../../common/constants.ts";
import type { RouteParams } from "../../ui/router/types.ts";
import { validateAbbrev } from "../util/processInputs.ts";
import { validateSeason } from "../util/processInputs.ts";

const processInputs = (params: RouteParams<"injuries">) => {
	let season: number | "current";

	if (params.season && params.season !== "current") {
		season = validateSeason(params.season);
	} else {
		season = "current";
	}

	let abbrev;
	let tid: number | undefined;

	const [validatedTid, validatedAbbrev] = validateAbbrev(params.abbrev, true);

	if (params.abbrev !== undefined && validatedAbbrev !== "???") {
		abbrev = validatedAbbrev;
		tid = validatedTid;
	} else if (params.abbrev === "watch") {
		abbrev = "watch";
	} else if (
		params.abbrev === "playoffs" &&
		REMAINING_PLAYOFF_TEAMS_PHASES.has(actualPhase())
	) {
		abbrev = "playoffs";
	} else {
		abbrev = "all";
	}

	return {
		abbrev,
		season,
		tid,
	};
};

export default defineView({
	id: "injuries",
	processInputs,
	load: async ({ inputs, updateEvents, prevInputs }) => {
		if (
			updateEvents.has("firstRun") ||
			((inputs.season === g.get("season") || inputs.season === "current") &&
				(updateEvents.has("gameSim") || updateEvents.has("playerMovement"))) ||
			(updateEvents.has("newPhase") && g.get("phase") === PHASE.PRESEASON) ||
			inputs.season !== prevInputs?.season ||
			inputs.abbrev !== prevInputs?.abbrev
		) {
			const stats = bySport({
				baseball: ["gp", "keyStats"],
				basketball: ["gp", "pts", "trb", "ast"],
				football: ["gp", "keyStats"],
				hockey: ["gp", "keyStats"],
			} as const);

			const players = await getPlayers(
				inputs.season === "current" ? g.get("season") : inputs.season,
				inputs.abbrev,
				["injury", "injuries"],
				["ovr", "pot"],
				[...stats, "jerseyNumber"],
				inputs.tid,
			);

			// With the ones digit hidden, a drop shows how far the tens digit fell,
			// measured against the full ratings history the rows above don't carry.
			const exceptProspects = g.get("hideRatingsOnesDigitExceptProspects");
			const rawPlayers = g.get("hideRatingsOnesDigit")
				? new Map(
						(
							await idb.getCopies.players(
								{
									pids: players
										.filter((p) => p.injuries.length > 0)
										.map((p) => p.pid),
								},
								"noCopyCache",
							)
						).map((p) => [p.pid, p]),
					)
				: undefined;
			const drops = (p: (typeof players)[number], index: number) => {
				const raw = rawPlayers?.get(p.pid);
				if (raw && !exemptFromCoarseRatings(raw.tid, exceptProspects)) {
					return coarsenInjuryDrops(raw, index, { fuzz: true });
				}
				const injury = p.injuries[index];
				return { ovrDrop: injury?.ovrDrop, potDrop: injury?.potDrop };
			};

			const injuries = [];
			for (const p of players) {
				if (inputs.season === "current") {
					if (p.injury.gamesRemaining > 0) {
						injuries.push({
							...p,
							type: p.injury.type,
							games: p.injury.gamesRemaining,
							...drops(p, p.injuries.length - 1),
							playingThrough: false,
						});
					}
				} else {
					for (const [i, injury] of p.injuries.entries()) {
						if (injury.season === inputs.season) {
							injuries.push({
								...p,
								type: injury.type,
								games: injury.games,
								...drops(p, i),
								playingThrough: false,
							});
						}
					}
				}
			}

			if (inputs.season === "current") {
				const teams = await idb.cache.teams.getAll();
				const playingThrough: Record<number, number> = {};
				const index = actualPhase() === PHASE.PLAYOFFS ? 1 : 0;
				for (const t of teams) {
					if (t.disabled) {
						continue;
					}

					playingThrough[t.tid] = getActualPlayThroughInjuries(t)[index];
				}

				for (const injury of injuries) {
					const cutoff = playingThrough[injury.tid];
					if (cutoff !== undefined && injury.games <= cutoff) {
						injury.playingThrough = true;
					}
				}
			}

			return {
				abbrev: inputs.abbrev,
				injuries: addFirstNameShort(injuries),
				season: inputs.season,
				stats,
			};
		}
	},
});
