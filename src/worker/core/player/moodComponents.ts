import { finances } from "../index.ts";
import { PHASE, PLAYER } from "../../../common/constants.ts";
import { facilitiesEffectMood } from "../../../common/budgetLevels.ts";
import type { MoodComponents, Player } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g, helpers, local } from "../../util/index.ts";
import { getNegotiationPids } from "../../views/negotiationList.ts";
import { getNumPlayersTradedAwayNormalized } from "./getNumPlayersTradedAwayNormalized.ts";
import { defaultGameAttributes } from "../../../common/defaultGameAttributes.ts";
import { signingDifficulty } from "../../util/difficulty.ts";
import { realisticFrontOfficesOn } from "../realisticFrontOffice/isOn.ts";
import { getContractExp } from "../../../common/contracts.ts";
import { getContractSummaryOrThrow } from "../contract/index.ts";

// A player whose rookie contract just expired, while his team can re-sign him. That's during the re-signing phase, or before then for a player still on his team, which includes when AI teams re-sign players in newPhaseResignPlayers before the phase is updated.
const isResigningAfterRookieContract = (p: Player) => {
	const season = g.get("season");
	const phase = g.get("phase");
	if (
		phase <= PHASE.PLAYOFFS ||
		phase > PHASE.RESIGN_PLAYERS ||
		(phase !== PHASE.RESIGN_PLAYERS && p.tid < 0) ||
		p.draft.year >= season
	) {
		return false;
	}

	const lastContract = p.contracts.at(-1);
	return (
		lastContract?.type === "rookie" &&
		!lastContract.endedEarly &&
		getContractExp(lastContract) === season
	);
};

const getMinFractionDiff = async (pid: number, tid: number) => {
	if (__SPORT !== "basketball") {
		return 0;
	}

	if (!local.minFractionDiffs) {
		const season = g.get("season");
		const playersAll = await idb.cache.players.getAll();

		const players = [];
		for (const p of playersAll) {
			let stats;
			for (let i = p.stats.length - 1; i >= 0; i--) {
				const row = p.stats[i]!;
				if (row.season === season && !row.playoffs) {
					stats = row;
				} else if (row.season < season) {
					break;
				}
			}

			if (stats) {
				if (stats.minAvailable !== undefined && stats.minAvailable > 500) {
					players.push({
						pid: p.pid,
						tid: stats.tid,
						value: p.valueNoPot,

						// Fraction of available minutes that this player played
						fraction: (stats.min ?? 0) / stats.minAvailable,
					});
				}
			}
		}

		players.sort((a, b) => b.value - a.value);

		local.minFractionDiffs = {};

		if (players.length < 100) {
			return 0;
		}

		// Logistic regression would be better than binning to find expected value, but no good library
		const BIN_SIZE = 20;
		const numBins = Math.ceil(players.length / BIN_SIZE);
		for (let i = 0; i < numBins; i++) {
			const binPlayers = players.slice(i * BIN_SIZE, (i + 1) * BIN_SIZE);
			let average = 0;
			for (const p of binPlayers) {
				average += p.fraction;
			}
			average /= binPlayers.length;
			for (const p of binPlayers) {
				local.minFractionDiffs[p.pid] = {
					tid: p.tid,
					diff: p.fraction - average,
				};
			}
		}
	}

	const p = local.minFractionDiffs[pid];
	if (!p || p.tid !== tid) {
		return 0;
	}

	return p.diff;
};

// Each component, worked out on its own, as it counts in his mood toward this
// team: the difficulty's modulation, the component's bounds, then his traits.
// Exported so a component can be worked out for another time too - the
// team's record and hype the season his contract runs out (the smart CPU's
// re-signing forecast).
export const finishedComponent = (
	key: Exclude<keyof MoodComponents, "custom">,
	raw: number,
	p: Pick<Player, "moodTraits">,
	tid: number,
) => {
	let value = raw;

	// Apply difficulty modulation (signing-specific override, else overall difficulty)
	const difficulty = g.get("spectator")
		? defaultGameAttributes.difficulty
		: signingDifficulty();
	if (realisticFrontOfficesOn()) {
		// Every team on the same terms, human or CPU, and difficulty does nothing.
	} else if (g.get("userTids").includes(tid) && !g.get("spectator")) {
		// Higher difficulty should result in lower mood, but we don't want to swap signs because that'd make for weird output (like complaining about team success when you won the title... but it's okay to just have it at 0 and say nothing)
		if (difficulty > 0) {
			if (value > 0) {
				value /= 1 + difficulty;
			} else {
				value *= 1 + difficulty;
			}
		} else if (difficulty < 0) {
			if (value > 0) {
				value *= 1 - difficulty;
			} else {
				value /= 1 - difficulty;
			}
		}
	} else {
		// At default difficulty, make players more likely to refuse. Decrease this, and players will be more likely to enter free agency
		const amount = 0.5 - helpers.bound(difficulty / 2, -0.25, 0.25);
		if (amount > 0) {
			if (value > 0) {
				value /= 1 + amount;
			} else {
				value *= 1 + amount;
			}
		} else {
			if (value > 0) {
				value *= 1 - amount;
			} else {
				value /= 1 - amount;
			}
		}
	}

	// Bound all components - they don't all have the same bounds!
	const bound = COMPONENT_BOUNDS[key];
	if (bound) {
		value = helpers.bound(value, bound[0], bound[1]);
	}

	// Apply traits modulation
	if (g.get("playerMoodTraits")) {
		for (const trait of MOOD_TRAIT_ORDER) {
			const factor = MOOD_TRAIT_FACTORS[trait]![key];
			if (factor !== undefined && p.moodTraits.includes(trait as any)) {
				value *= factor;
			}
		}
	}

	return value;
};

// Made once, not at every component: the smart CPU works out moods hundreds
// of thousands of times a market moment.
type FinishedKey = Exclude<keyof MoodComponents, "custom">;
const COMPONENT_BOUNDS: Partial<Record<FinishedKey, [number, number]>> = {
	marketSize: [-2, 2],
	facilities: [-2, 2],
	teamPerformance: [-Infinity, 2],
	hype: [-2, 2],
	loyalty: [0, Infinity],
	trades: [-Infinity, 0],
	playingTime: [-Infinity, 2],
	rookieContract: [0, Infinity],
};
const MOOD_TRAIT_FACTORS: Record<
	string,
	Partial<Record<FinishedKey, number>>
> = {
	F: { marketSize: 2.5, hype: 2.5, playingTime: 2.5 },
	L: { marketSize: 0.5, loyalty: 2.5, trades: 2.5 },
	$: { facilities: 1.5, marketSize: 0.5, teamPerformance: 0.5 },
	W: { marketSize: 0.5, playingTime: 0.5, teamPerformance: 2.5 },
};
const MOOD_TRAIT_ORDER = ["F", "L", "$", "W"] as const;

// TEAM PERFORMANCE from a share of games won, before finishedComponent: -2 at
// 25% and 2 at 75% (60% and a title), losing counting double.
export const teamPerformanceRaw = (winp: number) => {
	let value: number;
	if (__SPORT === "hockey") {
		// 40% to 60% -> -2 to 2
		value = -2 + ((winp - 0.4) * 4) / 0.2;
	} else {
		// 25% to 75% -> -2 to 2
		value = -2 + ((winp - 0.25) * 4) / 0.5;
	}

	// Negative matters more
	if (__SPORT === "basketball" && value < 0) {
		value *= 2;
	}

	// Set upper bound, in case went over due to playoff bonus
	return helpers.bound(value, -Infinity, 2);
};

// HYPE from the team's hype (0 to 1), before finishedComponent.
export const hypeRaw = (hype: number) => -2 + 4 * hype;

// LOYALTY from seasons with the team, before finishedComponent.
export const loyaltyRaw = (seasons: number, ownTeam: boolean) =>
	seasons / 8 + (ownTeam ? (__SPORT === "football" ? 5 : 2) : 0);

// Make components -2 to 2, then scale with traits to -5 to 5
const moodComponents = async (
	p: Player,
	tid: number,
): Promise<MoodComponents> => {
	const season = g.get("season");
	const phase = g.get("phase");

	const teamSeasons = await idb.cache.teamSeasons.indexGetAll(
		"teamSeasonsByTidSeason",
		[
			[tid, season - 2],
			[tid, season],
		],
	);
	const currentTeamSeason = teamSeasons.findLast((ts) => ts.season === season);

	// Only this team's rank: not a copy of every team for each mood.
	const teams = await idb.cache.teams.getAll();
	const ti = teams.findIndex((t) => t.tid === tid);
	if (ti < 0) {
		throw new Error(`tid ${tid} not found`);
	}
	const t = teams[ti]!;
	const popRank = helpers.getPopRanks(teams)[ti]!;

	const components: MoodComponents = {
		marketSize: 0,
		facilities: 0,
		teamPerformance: 0,
		hype: 0,
		loyalty: 0,
		trades: 0,
		playingTime: 0,
		rookieContract: 0,
		difficulty: 0,
		relatives: 0,
	};

	if (p.customMoodItems) {
		for (const row of p.customMoodItems) {
			if (row.tid === undefined || row.tid === tid) {
				if (!components.custom) {
					components.custom = [];
				}

				components.custom.push({
					amount: row.amount,
					text: row.text,
				});
			}
		}
	}

	{
		// MARKET SIZE: -2 to 2, based on population rank
		const marketSize0to1 = (teams.length - popRank) / (teams.length - 1);
		components.marketSize = -2 + marketSize0to1 * 4;
	}

	{
		// FACILITIES: -2 to 2, based on facilities level
		const facilitiesLevel = await finances.getLevelLastThree("facilities", {
			t,
			teamSeasons,
		});
		components.facilities = facilitiesEffectMood(facilitiesLevel);
	}

	{
		// TEAM PERFORMANCE: -2 means no playoffs and 25% winning percentage. +2 means championship and 60% winning percentage, or 75% winning percentage
		if (currentTeamSeason) {
			const projectedRecord = {
				won: currentTeamSeason.won,
				lost: currentTeamSeason.lost,
				tied: currentTeamSeason.tied,
				otl: currentTeamSeason.otl,
			};

			// If a custom league file starts after the regular season, don't assume all teams have 0 winning percentage
			const leagueFileFromAfterSeason =
				g.get("phase") >= PHASE.PLAYOFFS &&
				projectedRecord.won === 0 &&
				projectedRecord.lost === 0 &&
				projectedRecord.tied === 0 &&
				projectedRecord.otl === 0;
			if (!leagueFileFromAfterSeason) {
				let wonTitle = false;

				// If season ongoing, project record and playoff success based on last year
				if (phase < PHASE.PLAYOFFS) {
					const previousSeason = teamSeasons.find(
						(ts) => ts.season === season - 1,
					);
					const previousRecord = {
						won: previousSeason ? previousSeason.won : 0,
						lost: previousSeason ? previousSeason.lost : 0,
						tied: previousSeason ? previousSeason.tied : 1,
						otl: previousSeason ? previousSeason.otl : 0,
					};

					const fractionComplete =
						(projectedRecord.won +
							projectedRecord.lost +
							projectedRecord.tied +
							projectedRecord.otl) /
						g.get("numGames");

					const currentGames =
						projectedRecord.won +
						projectedRecord.lost +
						projectedRecord.tied +
						projectedRecord.otl;
					const previousGames =
						previousRecord.won +
						previousRecord.lost +
						previousRecord.tied +
						previousRecord.otl;

					const remainingGames = helpers.bound(
						g.get("numGames") - currentGames,
						0,
						Infinity,
					);

					for (const key of ["won", "lost", "tied", "otl"] as const) {
						const currentFraction =
							currentGames > 0 ? projectedRecord[key] / currentGames : 0;
						const previousFraction =
							previousGames > 0 ? previousRecord[key] / previousGames : 0;

						projectedRecord[key] +=
							(currentFraction * fractionComplete +
								previousFraction * (1 - fractionComplete)) *
							remainingGames;
					}

					if (previousSeason) {
						wonTitle =
							previousSeason.playoffRoundsWon >=
							g.get("numGamesPlayoffSeries", season - 1).length;
					}
				} else {
					wonTitle =
						currentTeamSeason.playoffRoundsWon >=
						g.get("numGamesPlayoffSeries", "current").length;
				}

				let winp = helpers.calcWinp(projectedRecord);
				if (wonTitle) {
					// If won title, equivalent to extra 15%, so 60% winp and title maxes it out
					winp += 0.15;
				}

				components.teamPerformance = teamPerformanceRaw(winp);
			}
		}
	}

	{
		// HYPE
		if (currentTeamSeason) {
			components.hype = hypeRaw(currentTeamSeason.hype);
		}
	}

	{
		// LOYALTY
		const numSeasonsWithTeam = p.stats.filter((row) => row.tid === tid).length;

		let wantsToReSign = p.tid === tid;
		if (
			!wantsToReSign &&
			p.tid === PLAYER.FREE_AGENT &&
			phase === PHASE.RESIGN_PLAYERS
		) {
			// Is this a free agent that a user team can re-sign? If so, apply bonus only for that team.
			const negotiationPids = await getNegotiationPids(tid);
			wantsToReSign = negotiationPids.has(p.pid);
		}

		components.loyalty = loyaltyRaw(numSeasonsWithTeam, wantsToReSign);
	}

	{
		// TRADES
		let numPlayersTradedAwayNormalized;
		if (p.tid === PLAYER.FREE_AGENT) {
			numPlayersTradedAwayNormalized = p.numPlayersTradedAwayNormalized?.[tid];
		}
		if (numPlayersTradedAwayNormalized === undefined) {
			numPlayersTradedAwayNormalized = getNumPlayersTradedAwayNormalized(
				teamSeasons,
				season,
			);
		}

		components.trades = helpers.bound(
			-(numPlayersTradedAwayNormalized - 5) / 4,
			-Infinity,
			0,
		);
	}

	{
		// PLAYING TIME
		const diff = await getMinFractionDiff(p.pid, tid);
		components.playingTime = diff * 10;
	}

	{
		// ROOKIE CONTRACT
		if (
			isResigningAfterRookieContract(p) ||
			getContractSummaryOrThrow(p).rookie ||
			p.tid === PLAYER.UNDRAFTED
		) {
			components.rookieContract = 8;
		}
	}

	{
		// Relatives
		if (p.relatives.length > 0) {
			const relativePids = new Set(p.relatives.map((relative) => relative.pid));
			const players = await idb.cache.players.indexGetAll("playersByTid", tid);
			const teamPids = new Set(players.map((p) => p.pid));
			const relativesOnTeam = teamPids.intersection(relativePids);
			components.relatives = 2 * relativesOnTeam.size;
		}
	}

	for (const key of helpers.keys(components)) {
		if (key !== "custom") {
			components[key] = finishedComponent(key, components[key], p, tid);
		}
	}

	return components;
};

export default moodComponents;
