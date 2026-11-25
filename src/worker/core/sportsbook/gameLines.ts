import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import teamOvr from "../team/ovr.ts";
import { getActualPlayThroughInjuries } from "../game/loadTeams.ts";
import {
	BASKETBALL_PLAYOFF_TOTAL_FACTOR,
	gameLengthFactor,
	getGameSpread,
	roundHalf,
} from "../../../common/getGameSpread.ts";
import { pregameLineupSynergyFromPlayers } from "../GameSim.basketball/synergy.ts";
import type { CalendarPlayThrough } from "../game/calendarPlayThrough.ts";
import { PHASE } from "../../../common/constants.ts";
import {
	probToAmerican,
	SPORTSBOOK_MAX_AMERICAN,
} from "../../../common/sportsbook.ts";
import {
	BASKETBALL_MARGIN_KEY_SIGMA,
	coverProbs,
	expectedTotalFromRates,
	leagueAvgGameTotal,
	MARGIN_SIGMA,
	marginToWinProb,
	normalCdf,
	overProb,
	probIgnoringPush,
	regressedScoring,
	toHalfPointLine,
	totalsPrior,
	type TotalsPrior,
} from "../../../common/sportsbookOdds.ts";
import {
	getTeamSpreadBias,
	spreadBiasAdjustment,
	type SpreadBiasEntry,
} from "../../util/getTeamSpreadBias.ts";
import { player, team } from "../index.ts";
import { rosterAtTipoff } from "../team/rosterAtTipoff.ts";

// The moneyline / spread / total for a single upcoming game.
//
// This lives in one place because it is computed twice: once for every game on
// the main board (getLines.ts) and once for the game whose prop page is open
// (getGameProps.ts). A bet placed from the prop page is validated against the
// MAIN board, so the two have to agree exactly - and when they were two copies
// of the same formula they didn't. The prop page priced off a different team
// overall (unfuzzed, not healed forward to the game's day), ignored neutral-site
// games entirely, and left the longshot cap off. Any of those is enough to make
// an honest bet bounce as "that line has moved".
//
// The SPREAD is the closed-form model plus one thing the model cannot know,
// and the MONEYLINE falls out of it so the two can never disagree. The line
// posted is that margin to the nearest half point - the same number ScoreBox,
// the hub and the game's page show, and the one stored on the game when it is
// played - and both sides are priced off the shape of the engine's final
// margins (marginDistribution), so a line near the 7-8 point pile-up carries
// the juice it deserves instead of a flat -110.
//
// That one thing is the team itself. The model reads a roster's overall and
// its lineup synergy; measured against the engine those two leave about 1.5
// points of error, and ALL of it is a fixed per-team offset that no roster
// property predicts (see getTeamSpreadBias.ts, which has the numbers). It is
// visible in results, though, so the line carries a correction measured from
// how far each side has run past its own number this season. Out of sample
// that is worth ~0.2 points by mid-season, which is the largest honest
// improvement available to a formula here - bigger than anything more roster
// features could buy, and it holds up on a league it was not fitted to.
//
// It used to be corrected by playing the matchup fifty times in the background
// and blending the average margin in. That is gone. Measured against the engine
// on a realistic talent grid - team overalls 31 to 70, four hundred runs per
// matchup - the closed form is off by about three quarters of a point, and the
// best refit of it that exists shaves that to half a point. On a number
// displayed to the nearest half point, that is nothing. The premise the sim was
// built on - that two teams the same overall apart can be genuinely different
// distances apart - did not survive being measured: adding a level term, an
// interaction term or a quadratic term to the fit each moved the residual by
// under a hundredth of a point. The relationship really is the straight line.
//
// What it cost was real: fifty full game sims per matchup, a cache keyed on
// every player's injury countdown (so every day tick invalidated all of it),
// numbers that appeared only on the two pages that warmed them and then moved
// under you a second later, and two devices in a room showing different lines
// until both had done the same work.
//
// The TOTAL stays on season scoring, and always did. Measured against the
// engine it was already accurate to about a point and a half, and a sample
// small enough to be free carries four points of noise.

export type GameLine = {
	// Expected home margin, correction included. Positive means the home team
	// is favored - this is the number that gets quoted.
	margin: number;
	// The same before the per-team form correction, which is what the
	// correction is measured against (see getTeamSpreadBias.ts).
	modelMargin: number;
	neutralSite: boolean;
	moneyline: { home: number; away: number };
	spread: { line: number; home: number; away: number };
	total: { line: number; over: number; under: number };
	// The game total the line is set from (points, both teams).
	expectedTotal: number;
};

type PricerTeam = {
	tid: number;
	playThroughInjuries: [number, number];
	playThroughInjuryDays?: [number, number];
	stats?: unknown;
};

type Matchup = {
	gid?: number;
	day: number;
	homeTid: number;
	awayTid: number;
	finals?: boolean;
};

// Per-game lines: base vig, capped at the same longshot price as everything
// else on the board.
const priceOdds = (prob: number) =>
	probToAmerican(prob, { maxAmerican: SPORTSBOOK_MAX_AMERICAN });

// Win/push/lose for the side that wins when (its margin + line) > 0, given its
// expected margin. Basketball reads the engine's real margin shape; the other
// sports a normal curve, a push being a final within half a point of the line.
export const sideProbs = (
	margin: number,
	line: number,
	sigma: number,
): { win: number; push: number; lose: number } => {
	if (__SPORT === "basketball") {
		return coverProbs(margin, line, sigma);
	}
	const hi = normalCdf((line + margin - 0.5) / sigma);
	const lo = normalCdf((line + margin + 0.5) / sigma);
	if (line % 1 !== 0) {
		const win = normalCdf((line + margin) / sigma);
		return { win, push: 0, lose: 1 - win };
	}
	return { win: hi, push: lo - hi, lose: 1 - lo };
};

// Last season's league-average game total, the guess a new season's totals
// start from (see totalsPrior) - scaled if the game got longer or shorter
// since. The sport default when there is no last season to read.
export const getTotalsPrior = async (season: number): Promise<TotalsPrior> => {
	const lengthNow = gameLengthFactor(
		g.get("numPeriods"),
		g.get("quarterLength"),
	);
	let lastSeasonAvg: number | undefined;
	if (season > g.get("startingSeason")) {
		try {
			const teams = await team.getFormattedAll({
				attrs: ["tid"],
				stats: ["pts", "gp"],
				season: season - 1,
			});
			let points = 0;
			let teamGames = 0;
			for (const t of teams) {
				const s = t.stats as { gp?: number; pts?: number } | undefined;
				if (s?.gp && s.gp > 0 && typeof s.pts === "number") {
					points += s.pts * s.gp;
					teamGames += s.gp;
				}
			}
			if (teamGames > 0) {
				const lengthThen = gameLengthFactor(
					g.get("numPeriods", season - 1),
					g.get("quarterLength", season - 1),
				);
				lastSeasonAvg = ((2 * points) / teamGames) * (lengthNow / lengthThen);
			}
		} catch {}
	}
	return totalsPrior(lastSeasonAvg, lengthNow);
};

export const buildGameLinePricer = async ({
	activeTeams,
	season,
	todayDay,
	ticksUntil,
	calendar,
	spreadBiases,
}: {
	activeTeams: PricerTeam[];
	season: number;
	// The day the schedule currently sits on, so a game further out can be priced
	// off rosters healed forward to it.
	todayDay: number;
	// Or, on a calendar, how many injury ticks each team has before the game
	// (see injuryTicksUntil).
	ticksUntil?: (tid: number, gid: number | undefined, day: number) => number;
	// And on the calendar, the user's hurt players by the days they'll have
	// left that night (see calendarPlayThrough.ts).
	calendar?: CalendarPlayThrough;
	// Per-team form correction (see getTeamSpreadBias.ts). Passed in by callers
	// that already swept the season's games for something else; fetched here
	// otherwise, so every caller prices the same line either way.
	spreadBiases?: Map<number, SpreadBiasEntry>;
}) => {
	const homeCourtAdvantage = g.get("homeCourtAdvantage");
	const numPeriods = g.get("numPeriods");
	const quarterLength = g.get("quarterLength");
	const neutralSiteSetting = g.get("neutralSite");
	const phase = g.get("phase");

	// The spread is already length-scaled (getGameSpread); the noise it fights
	// through scales only with the square root of the minutes, so a short-game
	// league's favorites are better bets than the raw margin suggests.
	const sigma =
		(__SPORT === "basketball" ? BASKETBALL_MARGIN_KEY_SIGMA : MARGIN_SIGMA) *
		Math.sqrt(gameLengthFactor(numPeriods, quarterLength));

	const teamByTid = new Map(activeTeams.map((t) => [t.tid, t]));

	const biases = spreadBiases ?? (await getTeamSpreadBias(season));

	// Price each game off the SAME team overall the Schedule/ScoreBox page shows
	// next to it, so the sportsbook's spread and moneyline never diverge from the
	// line displayed there. That means fuzzed ratings (fuzz: true), injuries
	// healed forward to the game's day, the team's playThroughInjuries setting,
	// and the playoff flag - not the flat, fuzz-free team ovr used for futures.
	const playersRaw = await idb.cache.players.indexGetAll("playersByTid", [
		0, // Active players have tid >= 0
		Infinity,
	]);
	const players = await player.formatAll(playersRaw, {
		attrs: ["injury", "pid", "value", "tid"],
		ratings: ["ovr", "pos", "ovrs"],
		season,
		fuzz: true,
		// These ovrs are arithmetic, never display. In a league that hides the
		// ones digit the default would hand back 0-10 here, team.ovr would build a
		// team overall out of them, and every spread on the board would collapse
		// to roughly the home-court constant - the favorite decided by who's at
		// home rather than by who's better.
		coarsenRatings: false,
	});
	const playersByTid = Map.groupBy(players, (p) => p.tid);
	// The raw rows as well: the trimmed player.formatAll rows above are all team.ovr
	// needs, but the synergy composites read the full ratings row.
	// (In the order each game will be played in - see rosterAtTipoff - so the
	// lineup the spread reads is the one the sim will read.)
	const playersRawByTid = new Map<number, typeof playersRaw>();
	for (const [tid, raw] of Map.groupBy(playersRaw, (p) => p.tid)) {
		playersRawByTid.set(tid, await rosterAtTipoff(tid, raw));
	}

	const playoffSeries = await idb.cache.playoffSeries.get(season);
	const roundSeries = playoffSeries
		? playoffSeries.currentRound === -1 && playoffSeries.playIns
			? playoffSeries.playIns.flat()
			: playoffSeries.series[playoffSeries.currentRound]
		: undefined;

	const ticksBefore = (t: PricerTeam, matchup: Matchup) =>
		ticksUntil
			? ticksUntil(t.tid, matchup.gid, matchup.day)
			: matchup.day - todayDay;

	// How a team's hurt players stand for a game (see injuryAtGame).
	const injuredFor = (t: PricerTeam, matchup: Matchup) => {
		const byDays = calendar?.byDays(t, !!roundSeries);
		return {
			numDaysInFuture: ticksBefore(t, matchup),
			playThroughInjuries: getActualPlayThroughInjuries(t),
			...(byDays
				? { byDays, calendarDaysAhead: calendar!.daysUntil(matchup.gid) }
				: {}),
		};
	};

	const gameOvr = (t: PricerTeam, matchup: Matchup): number | undefined =>
		teamOvr(playersByTid.get(t.tid) ?? [], {
			accountForInjuredPlayers: injuredFor(t, matchup),
			playoffs: !!roundSeries,
		});

	// Same availability window as gameOvr, so both spread inputs describe one
	// roster. The board and ScoreBox share this whole pricer, so both quote the
	// same synergy-aware line.
	const gameSynergy = (t: PricerTeam, matchup: Matchup): number | undefined =>
		pregameLineupSynergyFromPlayers(playersRawByTid.get(t.tid) ?? [], {
			...injuredFor(t, matchup),
			playoffs: !!roundSeries,
		});

	// League-average per-game total, for game totals when a team has no data.
	// t.stats can be missing entirely in a league with no games played (e.g.
	// started directly in the playoffs), so never assume it exists.
	const statsOf = (t: PricerTeam | undefined) => {
		const s = t?.stats as
			| { gp?: number; pts?: number; oppPts?: number }
			| undefined;
		return {
			gp: s?.gp ?? 0,
			pts: s?.pts ?? 0,
			oppPts: s?.oppPts ?? 0,
		};
	};
	const leagueAvgTotal = leagueAvgGameTotal(
		activeTeams.map(statsOf),
		await getTotalsPrior(season),
	);

	// team.getFormattedAll returns PER-GAME stats by default, so pts/oppPts are
	// already the genuine per-game averages - regressed toward the league mean
	// early in the season (see regressedScoring).
	const scoringFor = (t: PricerTeam) => {
		const s = statsOf(t);
		return regressedScoring(s.pts, s.gp, leagueAvgTotal);
	};
	const scoringAgainst = (t: PricerTeam) => {
		const s = statsOf(t);
		return regressedScoring(s.oppPts, s.gp, leagueAvgTotal);
	};

	// Neutral site drops home-court advantage, matching ScoreBox: an upcoming
	// finals/playoff game the settings mark neutral.
	const isNeutralSite = (matchup: Matchup) =>
		(neutralSiteSetting === "finals" && !!matchup.finals) ||
		(neutralSiteSetting === "playoffs" && phase === PHASE.PLAYOFFS);

	return {
		leagueAvgTotal,
		scoringFor,
		scoringAgainst,
		priceGame: (matchup: Matchup): GameLine | undefined => {
			const home = teamByTid.get(matchup.homeTid);
			const away = teamByTid.get(matchup.awayTid);
			if (!home || !away) {
				return undefined;
			}

			const neutralSite = isNeutralSite(matchup);
			const playoffs = !!roundSeries;
			const synergy0 = gameSynergy(home, matchup);
			const synergy1 = gameSynergy(away, matchup);
			const margin = getGameSpread({
				ovr0: gameOvr(home, matchup),
				ovr1: gameOvr(away, matchup),
				homeCourtAdvantage,
				neutralSite,
				numPeriods,
				quarterLength,
				synergyDiff:
					synergy0 !== undefined && synergy1 !== undefined
						? synergy0 - synergy1
						: undefined,
				playoffs,
			});
			if (margin === undefined) {
				return undefined;
			}

			// What the model says, plus what this season has shown about the two
			// teams. The model number is kept alongside so the sim can store it
			// and the correction can go on being measured from it.
			const modelMargin = margin;
			const margin2 =
				modelMargin +
				spreadBiasAdjustment(biases, matchup.homeTid, matchup.awayTid);

			const pHome =
				__SPORT === "basketball"
					? probIgnoringPush(sideProbs(margin2, 0, sigma))
					: marginToWinProb(margin2, sigma);
			// The scoring rates feeding the total are regular-season numbers, and
			// playoff games score less (measured - see
			// BASKETBALL_PLAYOFF_TOTAL_FACTOR).
			const playoffTotalFactor =
				playoffs && __SPORT === "basketball"
					? BASKETBALL_PLAYOFF_TOTAL_FACTOR
					: 1;
			// (The same formula the live game's header quotes the line by - see
			// totalAsOf in views/liveGameInfo.ts.)
			const expectedTotal = expectedTotalFromRates({
				home: statsOf(home),
				away: statsOf(away),
				leagueAvgTotal,
				factor: playoffTotalFactor,
			});
			const totalLine = toHalfPointLine(expectedTotal);
			const pOver = overProb(expectedTotal, totalLine);
			// Home spread: home favored by `margin2`, so the line is -margin2,
			// to the nearest half point like every other place it is shown.
			// (Rounded as the home margin, exactly as the stored spread is - a
			// quarter point rounds the other way negated. + 0 turns -0 into 0.)
			const spreadLine = -roundHalf(margin2) + 0;
			const pHomeCovers = probIgnoringPush(
				sideProbs(margin2, spreadLine, sigma),
			);

			return {
				margin: margin2,
				modelMargin,
				neutralSite,
				moneyline: {
					home: priceOdds(pHome),
					away: priceOdds(1 - pHome),
				},
				spread: {
					line: spreadLine,
					home: priceOdds(pHomeCovers),
					away: priceOdds(1 - pHomeCovers),
				},
				total: {
					line: totalLine,
					over: priceOdds(pOver),
					under: priceOdds(1 - pOver),
				},
				expectedTotal,
			};
		},
	};
};
