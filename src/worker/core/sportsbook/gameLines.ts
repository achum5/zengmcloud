import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import teamOvr from "../team/ovr.ts";
import { getActualPlayThroughInjuries } from "../game/loadTeams.ts";
import {
	BASKETBALL_PLAYOFF_TOTAL_FACTOR,
	gameLengthFactor,
	getGameSpread,
} from "../../../common/getGameSpread.ts";
import { pregameLineupSynergyFromPlayers } from "../GameSim.basketball/synergy.ts";
import { PHASE } from "../../../common/constants.ts";
import { bySport, isSport } from "../../../common/sportFunctions.ts";
import {
	probToAmerican,
	SPORTSBOOK_MAX_AMERICAN,
} from "../../../common/sportsbook.ts";
import {
	expectedGameTotal,
	MARGIN_SIGMA,
	marginToWinProb,
	overProb,
	toHalfPointLine,
} from "../../../common/sportsbookOdds.ts";
import {
	getTeamSpreadBias,
	spreadBiasAdjustment,
	type SpreadBiasEntry,
} from "../../util/getTeamSpreadBias.ts";

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
// and the MONEYLINE falls out of it so the two can never disagree.
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
};

type PricerTeam = {
	tid: number;
	playThroughInjuries: [number, number];
	stats?: unknown;
};

type Matchup = {
	day: number;
	homeTid: number;
	awayTid: number;
	finals?: boolean;
};

// Per-game lines: base vig, capped at the same longshot price as everything
// else on the board.
const priceOdds = (prob: number) =>
	probToAmerican(prob, { maxAmerican: SPORTSBOOK_MAX_AMERICAN });

export const buildGameLinePricer = async ({
	activeTeams,
	season,
	todayDay,
	spreadBiases,
}: {
	activeTeams: PricerTeam[];
	season: number;
	// The day the schedule currently sits on, so a game further out can be priced
	// off rosters healed forward to it.
	todayDay: number;
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
		MARGIN_SIGMA * Math.sqrt(gameLengthFactor(numPeriods, quarterLength));

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
	const players = await idb.getCopies.playersPlus(playersRaw, {
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
	// The raw rows as well: the trimmed playersPlus rows above are all team.ovr
	// needs, but the synergy composites read the full ratings row.
	const playersRawByTid = Map.groupBy(playersRaw, (p) => p.tid);

	const playoffSeries = await idb.cache.playoffSeries.get(season);
	const roundSeries = playoffSeries
		? playoffSeries.currentRound === -1 && playoffSeries.playIns
			? playoffSeries.playIns.flat()
			: playoffSeries.series[playoffSeries.currentRound]
		: undefined;

	const gameOvr = (t: PricerTeam, day: number): number | undefined =>
		teamOvr(playersByTid.get(t.tid) ?? [], {
			accountForInjuredPlayers: {
				numDaysInFuture: day - todayDay,
				playThroughInjuries: getActualPlayThroughInjuries(t),
			},
			playoffs: !!roundSeries,
		});

	// Same availability window as gameOvr, so both spread inputs describe one
	// roster. The board and ScoreBox share this whole pricer, so both quote the
	// same synergy-aware line.
	const gameSynergy = (t: PricerTeam, day: number): number | undefined =>
		pregameLineupSynergyFromPlayers(playersRawByTid.get(t.tid) ?? [], {
			numDaysInFuture: day - todayDay,
			playThroughInjuries: getActualPlayThroughInjuries(t),
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
	let totalPtsPerGame = 0;
	let teamsWithGames = 0;
	for (const t of activeTeams) {
		const s = statsOf(t);
		if (s.gp > 0) {
			totalPtsPerGame += s.pts;
			teamsWithGames += 1;
		}
	}
	const leagueAvgTotal =
		teamsWithGames > 0
			? (2 * totalPtsPerGame) / teamsWithGames
			: bySport({ basketball: 220, football: 45, baseball: 9, hockey: 6 });

	// teamsPlus returns PER-GAME stats by default, so pts/oppPts are already the
	// genuine per-game averages. Early in the season, regress toward the league
	// mean (fully trusted after ~8 games) so a hot opening week doesn't swing
	// totals wildly.
	const halfLeague = leagueAvgTotal / 2;
	const regress = (perGame: number, gp: number) => {
		const w = Math.min(1, gp / 8);
		return halfLeague + (perGame - halfLeague) * w;
	};
	const scoringFor = (t: PricerTeam) => {
		const s = statsOf(t);
		return s.gp > 0 ? regress(s.pts, s.gp) : undefined;
	};
	const scoringAgainst = (t: PricerTeam) => {
		const s = statsOf(t);
		return s.gp > 0 ? regress(s.oppPts, s.gp) : undefined;
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
			const synergy0 = gameSynergy(home, matchup.day);
			const synergy1 = gameSynergy(away, matchup.day);
			const margin = getGameSpread({
				ovr0: gameOvr(home, matchup.day),
				ovr1: gameOvr(away, matchup.day),
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

			const pHome = marginToWinProb(margin2, sigma);
			// The scoring rates feeding the total are regular-season numbers, and
			// the engine scores ~6.6% fewer points under playoff parameters
			// (measured - see BASKETBALL_PLAYOFF_TOTAL_FACTOR). Without this every
			// playoff total sat ~14 points high and the under was nearly free.
			const playoffTotalFactor =
				playoffs && isSport("basketball") ? BASKETBALL_PLAYOFF_TOTAL_FACTOR : 1;
			const expectedTotal =
				playoffTotalFactor *
				expectedGameTotal({
					homeFor: scoringFor(home),
					homeAgainst: scoringAgainst(home),
					awayFor: scoringFor(away),
					awayAgainst: scoringAgainst(away),
					leagueAvgTotal,
				});
			const totalLine = toHalfPointLine(expectedTotal);
			const pOver = overProb(expectedTotal, totalLine);
			// Home spread: home favored by `margin2`, so the line is -margin2.
			const spreadLine =
				toHalfPointLine(Math.abs(margin2)) * (margin2 >= 0 ? -1 : 1);

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
					home: priceOdds(0.5),
					away: priceOdds(0.5),
				},
				total: {
					line: totalLine,
					over: priceOdds(pOver),
					under: priceOdds(1 - pOver),
				},
			};
		},
	};
};
