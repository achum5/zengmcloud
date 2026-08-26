import { g } from "../../util/index.ts";
import { idb } from "../../db/index.ts";
import teamOvr from "../team/ovr.ts";
import getSchedule from "../season/getSchedule.ts";
import { buildGameLinePricer } from "./gameLines.ts";
import getAwardRaceOdds from "../season/getAwardRaceOdds.ts";
import { getPlayers, getTopPlayers } from "../season/awards.ts";
import {
	dpoyScore,
	mvpScore,
	royFilter,
	royScore,
} from "../season/doAwards.basketball.ts";
import {
	getTeamAtsRecords,
	formatAtsRecord,
} from "../../util/getTeamAtsRecords.ts";
import { PHASE, PLAYER, RATINGS } from "../../../common/constants.ts";
import { isSport } from "../../../common/sportFunctions.ts";
import {
	BASKETBALL_SYNERGY_COEF,
	BASKETBALL_SYNERGY_OVR_SLOPE,
	gameLengthFactor,
	homeCourtAdvantagePoints,
} from "../../../common/getGameSpread.ts";
import {
	futuresTailVig,
	probToAmerican,
	SPORTSBOOK_FUTURES_VIG,
	SPORTSBOOK_MAX_AMERICAN,
} from "../../../common/sportsbook.ts";
import {
	MARGIN_SIGMA,
	mcShade,
	softCapMargin,
	strengthProbs,
	tierMembershipProbs,
} from "../../../common/sportsbookOdds.ts";
import { getFuturesStrengths } from "./futuresStrength.ts";
import {
	bracketMarketsOpen,
	simulateFutures,
	simulatePlayoffBracket,
	type BracketMatchup,
} from "../../../common/sportsbookFutures.ts";

// The 3 tiers an All-League/All-Defensive Team splits into, 5 players each -
// matches worker/core/season/doAwards.basketball.ts's makeTeams().
const TEAM_AWARD_TIER_SIZES = [5, 5, 5];
const TEAM_AWARD_TIER_TITLES = ["First Team", "Second Team", "Third Team"];

// Order a board the way a book prints one: shortest price at the top. American
// odds already sort that way numerically - the bigger the favorite the more
// negative, the longer the shot the more positive - so ascending is descending
// by chance of winning.
export const sortedByPrice = <T extends { americanOdds: number }>(
	rows: T[],
): T[] => [...rows].sort((a, b) => a.americanOdds - b.americanOdds);

type TierCandidate = {
	pid: number;
	name: string;
	tid: number;
	abbrev: string;
	awardScore?: number;
};

// Turn a ranked field of candidates into a per-tier board (odds for "makes
// tier N"), via the shared tier-membership Monte Carlo. Capped to the
// top-priced 10 per tier so the board stays readable - a candidate with
// effectively 0 chance at a given tier just isn't shown for it.
const buildTierBoard = (
	field: TierCandidate[],
	tierSizes: number[],
	seed: number,
	noiseFactor?: number,
) => {
	const titles = TEAM_AWARD_TIER_TITLES;
	if (field.length === 0) {
		return tierSizes.map((_, i) => ({
			tier: i + 1,
			title: titles[i] ?? `Tier ${i + 1}`,
			candidates: [] as {
				pid: number;
				name: string;
				tid: number;
				abbrev: string;
				americanOdds: number;
			}[],
		}));
	}
	const TIER_ITERATIONS = 3000;
	const scores = field.map((p) => p.awardScore ?? 0);
	const probs = tierMembershipProbs(scores, tierSizes, {
		iterations: TIER_ITERATIONS,
		seed,
		noiseFactor,
	});
	return tierSizes.map((_, tierIdx) => ({
		tier: tierIdx + 1,
		title: titles[tierIdx] ?? `Tier ${tierIdx + 1}`,
		candidates: notSettled(
			sortedByPrice(
				field.map((p, i) => ({
					pid: p.pid,
					name: p.name,
					tid: p.tid,
					abbrev: p.abbrev,
					americanOdds: priceFuture(probs[i]![tierIdx]!, TIER_ITERATIONS),
				})),
			),
		).slice(0, TEAM_AWARD_BOARD_SIZE),
	}));
};

// How many candidates to list per award-team tier. Generous so there are real
// longshots to bet, not just the handful of locks - the UI keeps the list
// scrollable so a long field doesn't take over the page.
const TEAM_AWARD_BOARD_SIZE = 30;

// Early in a season a handful of games barely hint at who'll make an award
// team, so the tier boards start noisier (more uncertain) and sharpen as games
// are played. These are the EARLY-season values; they ramp to their base by
// season's end, so late-season odds are unchanged.
const TIER_NOISE_EARLY = 1.2; // more tier-board Monte-Carlo noise early
const TIER_NOISE_LATE = 0.6; // matches tierMembershipProbs' default noiseFactor

// The same idea for the futures Monte Carlo, in points of margin rather than a
// noise multiplier: how far off a team's rating could be from its true
// strength. This is the model's HONEST uncertainty, not a flattener: it IS
// futuresRatingError (persistent model error 1.3 preseason, easing toward 1
// as results blend in). Jitter above the real uncertainty quietly taxes every
// favorite (Jensen: a strong team's market probability is concave in its
// rating), and the EV harness shows the vig only barely covers even the
// honest amount.
const FUTURES_UNCERTAINTY_START = 1.3;
const FUTURES_UNCERTAINTY_END = 1;

// The Bayesian weight schedule for blending the model rating with a team's
// actual point differential: weight = gp / (gp + this). 100 is not a mood,
// it's the two error sizes: per-game margin noise is sigma ~13.1 (measured),
// the model's PERSISTENT per-team error vs the engine ~1.3 (measured, see
// FUTURES_MODEL_ERROR), and the sample mean of n games deserves weight
// n / (n + sigma^2/err^2) = n / (n + 101). Any heavier and the blend imports
// more sample noise than bias it removes; any lighter and the book never
// learns what the model can't see. The model never fully lets go, which is
// what keeps a mid-season trade priced in immediately instead of dragged down
// by forty games of the old roster's results. Exported for the EV harness.
export const FUTURES_MOV_PRIOR_GAMES = 100;

// Exported for the EV harness, so it jitters exactly as much as the board.
export const futuresRatingUncertainty = (seasonProgress: number) =>
	FUTURES_UNCERTAINTY_END +
	(FUTURES_UNCERTAINTY_START - FUTURES_UNCERTAINTY_END) * (1 - seasonProgress);

// The strength model's PERSISTENT per-team error vs the engine, in points -
// the component of its miss that follows a team into every game, which is
// what a season-long market exposes (single-matchup misses average out over
// forty different opponents). Measured by spreadCalibration.test.ts's
// per-team decomposition on two real leagues' rosters: 1.30 and 1.35.
export const FUTURES_MODEL_ERROR = 1.3;

// How far off the book's own rating could be at this point in a season: the
// model's persistent error, shrunk by the blend, plus the sample noise the
// blended point differential carries. Both components in points of margin.
export const futuresRatingError = (gp: number, sigma: number) => {
	const w = gp / (gp + FUTURES_MOV_PRIOR_GAMES);
	const movNoise = gp > 0 ? sigma / Math.sqrt(gp) : 0;
	return Math.hypot((1 - w) * FUTURES_MODEL_ERROR, w * movNoise);
};

// The extra juice a win-total side carries because the book cannot know its
// own line is centered. A bettor with a better model always bets the side the
// book's rating error favors, and the value of that pick is exactly
// E[Phi(|D| / winsSd)] - 1/2 = arctan(sd(D) / winsSd) / pi, where D is the
// book's miss on the line in wins (games x dP/dRating x rating error). Added
// to BOTH sides' probabilities before the vig - the market equivalent of a
// book juicing a total it is unsure of to -125/-125 instead of -110/-110.
export const winTotalLoad = ({
	gamesRemaining,
	gp,
	slope,
	winsSd,
	sigma,
}: {
	gamesRemaining: number;
	gp: number;
	slope: number;
	winsSd: number;
	sigma: number;
}) =>
	Math.atan(
		(gamesRemaining * slope * futuresRatingError(gp, sigma)) /
			Math.max(0.5, winsSd),
	) / Math.PI;

// LEGACY (non-basketball) evidence shade: how far a team's projected strength
// is shaded toward the field before it has played, and how many games it takes
// to earn the full number back. The basketball path deliberately has no such
// shade - the engine plays preseason rosters at face value, so shading the
// mean 15% just made every preseason favorite +EV.
const FUTURES_PRIOR_WEIGHT = 0.85;
const FUTURES_EVIDENCE_GAMES = 25;

// Cap how many upcoming games get a line at once, so the board stays readable.
const MAX_GAME_LINES = 24;

// How many worlds the futures/bracket Monte Carlo simulates per board.
const FUTURES_ITERATIONS = 4000;

// No futures price shorter than this is ever offered - the outcome is treated
// as settled and its row comes off the board, like a real book pulling a
// market once it's decided. The favorite side of the odds clamp tops out at
// -9900 (implied 99%), so a TRULY clinched outcome left up would be a
// guaranteed ~1% payout - literal free money, riskless. Exported for the EV
// harness.
export const SETTLED_PRICE = -9000;
const notSettled = <T extends { americanOdds: number }>(rows: T[]): T[] =>
	rows.filter((r) => r.americanOdds > SETTLED_PRICE);

// The longest price each futures market offers. The smaller the field, the
// shorter the cap: a division has five outcomes and a conference fifteen, so
// a 300-1 price there isn't a longshot, it's the book claiming three digits of
// precision its model doesn't have - the far tail is exactly where the
// model's measured per-team error can hide a multiple of the true chance.
// Real books cap these markets the same way and for the same reason.
// Exported for the EV harness.
export const FUTURES_CAPS = {
	title: SPORTSBOOK_MAX_AMERICAN,
	conference: 12000,
	division: 6000,
};

// Futures + award bets: the heavier futures hold plus a tail ramp, capped per
// market (default +30000). Used for every non-game market (title/conference/
// division/win totals, award boards, All-Star). `iterations` is the Monte
// Carlo size the probability came from, for the tail shade (see mcShade);
// pass Infinity for a closed-form number. Exported for the EV harness
// (futuresCalibration.test.ts), so it grades the exact prices the board
// shows.
export const priceFuture = (
	prob: number,
	iterations = FUTURES_ITERATIONS,
	maxAmerican = SPORTSBOOK_MAX_AMERICAN,
) => {
	const shaded = mcShade(prob, iterations);
	return probToAmerican(shaded, {
		vig: futuresTailVig(shaded, SPORTSBOOK_FUTURES_VIG),
		maxAmerican,
	});
};

// Power for the preseason overall-rating award projections (MVP/DPOY/ROY). Higher
// than the in-season AWARD_POWER so the top-rated players are clear, heavy
// favorites before any games separate the field.
const PRESEASON_AWARD_POWER = 1.8;

// A player's overall rating, from either an array of season ratings (getPlayers)
// or a single season's ratings object (playersPlus with a fixed season).
const ovrOf = (p: any): number => {
	const r = Array.isArray(p.ratings) ? p.ratings.at(-1) : p.ratings;
	return typeof r?.ovr === "number" ? r.ovr : 0;
};

// A single rating (e.g. "diq") off the same ratings object, for the preseason
// DPOY defensive tilt.
const ratingVal = (p: any, key: string): number => {
	const r = Array.isArray(p.ratings) ? p.ratings.at(-1) : p.ratings;
	return typeof r?.[key] === "number" ? r[key] : 0;
};

// Before (and early in) a season the award formulas read essentially no stats,
// so every candidate scores ~0 and the odds collapse to noise - which is why a
// fresh season showed random role players at flat prices. Blend the real award
// formula with a projection off player OVERALL, weighted by how much of the
// season is still ahead (earlyWeight = 1 at tip-off → 0 at season's end). So the
// board opens as an overall-based projection - the best players are the
// favorites - and hands off to actual production as games are played. A throwing
// formula (missing stats) just contributes 0, leaving the overall projection.
const projectedScore =
	(statScore: (p: any) => number, earlyWeight: number, ovrCoef: number) =>
	(p: any): number => {
		let s = 0;
		try {
			const v = statScore(p);
			if (typeof v === "number" && Number.isFinite(v)) {
				s = v;
			}
		} catch {
			s = 0;
		}
		return s + earlyWeight * ovrCoef * ovrOf(p);
	};

// Team ovr (0-100) for every active team, the strength that drives every
// futures market. Mirrors how the Power Rankings page rates teams.
export const getTeamOvrs = async (
	teams: { tid: number }[],
	season: number,
): Promise<Map<number, number>> => {
	const ratings = ["ovr", "pos", "ovrs"];
	if (isSport("basketball")) {
		ratings.push(...RATINGS);
	}

	const ovrByTid = new Map<number, number>();
	for (const t of teams) {
		const rawPlayers = await idb.cache.players.indexGetAll(
			"playersByTid",
			t.tid,
		);
		const teamPlayers = await idb.getCopies.playersPlus(rawPlayers, {
			attrs: ["tid", "injury", "value"],
			ratings,
			stats: ["season", "tid"],
			season,
			showNoStats: true,
			showRookies: true,
			fuzz: false,
			// Feeds team.ovr, so it needs the real ratings - the display rounding
			// would put the whole league in a handful of ties.
			coarsenRatings: false,
			tid: t.tid,
		});
		ovrByTid.set(t.tid, teamOvr(teamPlayers as any, {}));
	}
	return ovrByTid;
};

// The whole live odds board, computed from current league state.
export const getLines = async () => {
	const season = g.get("season");
	const numGames = g.get("numGames");
	const confs = g.get("confs");
	const divs = g.get("divs");

	const teams = await idb.getCopies.teamsPlus(
		{
			attrs: [
				"tid",
				"cid",
				"did",
				"abbrev",
				"region",
				"name",
				"disabled",
				"playThroughInjuries",
			],
			seasonAttrs: ["won", "lost", "tied", "otl"],
			stats: ["pts", "oppPts", "gp"],
			season,
			// Without this, teams with no stats rows are dropped entirely - in a
			// league with no games played yet (e.g. started in the playoffs) that's
			// EVERY team, which blanked the whole board.
			showNoStats: true,
		},
		"noCopyCache",
	);
	const activeTeams = teams.filter((t) => !t.disabled);
	const teamByTid = new Map(activeTeams.map((t) => [t.tid, t]));

	// Each team's against-the-spread record, shown next to its W-L on every game.
	const atsRecords = await getTeamAtsRecords(season);

	const phase = g.get("phase");
	// Read once up here: the injury-availability horizon below needs to know how
	// deep in the playoffs we are, and the in-playoffs bracket pricing reuses it.
	const playoffSeries =
		phase >= PHASE.PLAYOFFS
			? await idb.cache.playoffSeries.get(season)
			: undefined;

	const gamesPlayedOf = (t: (typeof activeTeams)[number]) =>
		t.seasonAttrs.won +
		t.seasonAttrs.lost +
		(t.seasonAttrs.tied ?? 0) +
		(t.seasonAttrs.otl ?? 0);

	// Basketball futures run on the engine-measured strength model (overall +
	// lineup synergy at the spread coefficients, injury-availability weighted -
	// see futuresStrength.ts). Other sports keep the legacy Power-Rankings
	// heuristic, which needs the flat team ovr.
	let strengthByTid:
		| Awaited<ReturnType<typeof getFuturesStrengths>>
		| undefined;
	let ovrByTid: Map<number, number> | undefined;
	if (isSport("basketball")) {
		// Injury horizon: the games a team still has in front of it - its
		// remaining regular season, or during the playoffs a rough remaining
		// bracket length (~6 games a round).
		const playoffHorizon =
			6 *
			Math.max(
				1,
				g.get("numGamesPlayoffSeries").length -
					Math.max(0, playoffSeries?.currentRound ?? 0),
			);
		strengthByTid = await getFuturesStrengths(
			activeTeams.map((t) => ({
				tid: t.tid,
				horizonGames:
					phase >= PHASE.PLAYOFFS
						? playoffHorizon
						: Math.max(1, numGames - gamesPlayedOf(t)),
			})),
			season,
		);
	} else {
		ovrByTid = await getTeamOvrs(activeTeams, season);
	}

	// A team's per-game scoring, for the futures strength model below. t.stats can
	// be missing entirely in a league with no games played (e.g. started directly
	// in the playoffs), so never assume it exists.
	const statsOf = (t: (typeof activeTeams)[number] | undefined) => {
		const s = t?.stats as
			| { gp?: number; pts?: number; oppPts?: number }
			| undefined;
		return {
			gp: s?.gp ?? 0,
			pts: s?.pts ?? 0,
			oppPts: s?.oppPts ?? 0,
		};
	};
	// --- Game lines -------------------------------------------------------
	const schedule = await getSchedule();

	// Shared with the per-game prop board, so a spread/moneyline/total quoted on
	// a game's page is the same number this board carries - see gameLines.ts.
	const pricer = await buildGameLinePricer({
		activeTeams,
		season,
		todayDay: schedule[0]?.day ?? 0,
	});

	const games = [];
	for (const matchup of schedule) {
		if (matchup.homeTid < 0 || matchup.awayTid < 0) {
			continue; // All-Star / special games
		}
		const home = teamByTid.get(matchup.homeTid);
		const away = teamByTid.get(matchup.awayTid);
		if (!home || !away) {
			continue;
		}

		const line = pricer.priceGame(matchup);
		if (!line) {
			continue;
		}

		games.push({
			gid: matchup.gid,
			home: {
				tid: home.tid,
				abbrev: home.abbrev,
				region: home.region,
				name: home.name,
				won: home.seasonAttrs.won,
				lost: home.seasonAttrs.lost,
				ats: formatAtsRecord(atsRecords.get(home.tid)),
			},
			away: {
				tid: away.tid,
				abbrev: away.abbrev,
				region: away.region,
				name: away.name,
				won: away.seasonAttrs.won,
				lost: away.seasonAttrs.lost,
				ats: formatAtsRecord(atsRecords.get(away.tid)),
			},
			moneyline: line.moneyline,
			spread: line.spread,
			total: line.total,
		});
		if (games.length >= MAX_GAME_LINES) {
			break;
		}
	}

	// Any game still priced off the formula gets simulated in the background, and
	// the board re-renders when they land. Deliberately not awaited: this is the
	// work the peek-only pricing path exists to keep off the page load.
	// --- Futures: Monte Carlo of the season + playoffs ---------------------
	// One simulation drives EVERY futures market (division, conference, title,
	// win totals), so they can never contradict each other, and a dominant team
	// prices like one because it actually plays through the bracket. See
	// common/sportsbookFutures.ts.

	// Margins scale with game length; per-game noise only with its square root.
	const lengthFactor = gameLengthFactor(
		g.get("numPeriods"),
		g.get("quarterLength"),
	);
	const futuresSigma = MARGIN_SIGMA * Math.sqrt(lengthFactor);
	const hcaPoints =
		homeCourtAdvantagePoints(g.get("homeCourtAdvantage")) * lengthFactor;

	// The model half of a team's rating: its expected margin vs an average team,
	// before any games inform it.
	let modelMarginOf: (tid: number) => number;
	if (strengthByTid) {
		// Basketball: the engine-measured spread model, applied vs the league
		// means. Same coefficients as every game line, so a team's futures price
		// and its nightly spreads can never tell two different stories.
		const rows = [...strengthByTid.values()];
		const meanExpOvr =
			rows.reduce((s, r) => s + r.expectedOvr, 0) / Math.max(1, rows.length);
		const synergyOk =
			rows.length > 0 && rows.every((r) => r.expectedSynergy !== undefined);
		const meanExpSynergy = synergyOk
			? rows.reduce((s, r) => s + r.expectedSynergy!, 0) / rows.length
			: 0;
		modelMarginOf = (tid) => {
			const r = strengthByTid!.get(tid);
			if (!r) {
				return 0;
			}
			// Same fallback slope as getGameSpread's ovr-only branch when a roster
			// is too small to read a lineup synergy.
			const margin = synergyOk
				? BASKETBALL_SYNERGY_OVR_SLOPE * (r.expectedOvr - meanExpOvr) +
					BASKETBALL_SYNERGY_COEF * (r.expectedSynergy! - meanExpSynergy)
				: (15 / 50) * (r.expectedOvr - meanExpOvr);
			return margin * lengthFactor;
		};
	} else {
		// Legacy path: ovr gap x 0.6, the Power Rankings scaling.
		const meanOvr =
			activeTeams.reduce((s, t) => s + (ovrByTid!.get(t.tid) ?? 50), 0) /
			Math.max(1, activeTeams.length);
		modelMarginOf = (tid) => ((ovrByTid!.get(tid) ?? 50) - meanOvr) * 0.6;
	}

	// A team's strength as a point margin vs an average team, blending its
	// MODEL rating with its actual season PERFORMANCE (real point differential).
	const ratingOf = (tid: number) => {
		const s = statsOf(teamByTid.get(tid));
		const actualMOV = s.gp > 0 ? s.pts - s.oppPts : 0; // per-game differential
		if (strengthByTid) {
			// Bayesian blend at the measured error sizes - see
			// FUTURES_MOV_PRIOR_GAMES. No cap and no preseason shade: the engine's
			// margins are linear in the rating gap (measured past +35) and it plays
			// preseason rosters at face value, so both "corrections" were just
			// mispricings.
			const w = s.gp / (s.gp + FUTURES_MOV_PRIOR_GAMES);
			return modelMarginOf(tid) * (1 - w) + actualMOV * w;
		}

		// Legacy (non-basketball) path, unchanged.
		// Trust what the team has actually done more and more as the sample grows:
		// by ~30 games the real point differential carries 3/4 of the weight.
		const perfWeight = s.gp > 0 ? 0.75 * Math.min(1, s.gp / 30) : 0;
		const blended =
			modelMarginOf(tid) * (1 - perfWeight) + actualMOV * perfWeight;

		// A rating gap is an estimate, and before the games are played it is only
		// an estimate. A book shades its number toward the field until the
		// evidence arrives, which is why nobody posts 79.5 in October. Full
		// strength by ~25 games; before that the number is pulled in.
		const evidence =
			FUTURES_PRIOR_WEIGHT +
			(1 - FUTURES_PRIOR_WEIGHT) * Math.min(1, s.gp / FUTURES_EVIDENCE_GAMES);

		// ...and however big the gap, a sustained point differential has a ceiling.
		return softCapMargin(blended) * evidence;
	};

	const futuresTeams = activeTeams.map((t) => ({
		tid: t.tid,
		cid: t.cid,
		did: t.did,
		won: t.seasonAttrs.won,
		gamesRemaining: Math.max(0, numGames - gamesPlayedOf(t)),
		rating: ratingOf(t.tid),
	}));

	// Deterministic seed from league state: lines are stable between sims and
	// the server re-derives the same board when validating a bet.
	const totalWon = futuresTeams.reduce((s, t) => s + t.won, 0);
	const totalRemaining = futuresTeams.reduce((s, t) => s + t.gamesRemaining, 0);
	const seed =
		(season * 9301 + totalRemaining * 49297 + totalWon * 233) % 2147483647;

	// How far through the regular season we are (0 at tip-off → 1 at the end),
	// from the ACTUAL season length (numGames), not a default. Award and
	// award-team odds use this to start uncertain and sharpen as games are
	// played; futures already model games-remaining directly via simulateFutures.
	// A season-progress fraction is derived deterministically, so the server
	// re-derives identical odds when settling a bet.
	const totalPossibleGames = futuresTeams.length * numGames;
	const seasonProgress =
		totalPossibleGames > 0
			? Math.min(1, Math.max(0, 1 - totalRemaining / totalPossibleGames))
			: 1;
	const tierNoiseFactor =
		TIER_NOISE_EARLY - (TIER_NOISE_EARLY - TIER_NOISE_LATE) * seasonProgress;

	// How unsure the book is about each team's true strength, in points of
	// margin, scaled by how much season is still to come.
	const futuresUncertainty = futuresRatingUncertainty(seasonProgress);

	// The playoff neutral-site settings change who gets a home edge in simulated
	// series, so the book prices the bracket the engine will actually play.
	const neutralSiteSetting = g.get("neutralSite");
	const playoffsNeutral = neutralSiteSetting === "playoffs";
	const finalsNeutral = playoffsNeutral || neutralSiteSetting === "finals";

	// The actual remaining slate, so win totals price each team's real schedule
	// (opponents + home/away split). During the playoffs the schedule holds
	// playoff games, not regular-season slate - and gamesRemaining is 0 anyway.
	const futuresSchedule =
		phase < PHASE.PLAYOFFS
			? schedule
					.filter(
						(m) =>
							m.homeTid >= 0 &&
							m.awayTid >= 0 &&
							teamByTid.has(m.homeTid) &&
							teamByTid.has(m.awayTid),
					)
					.map((m) => ({ homeTid: m.homeTid, awayTid: m.awayTid }))
			: undefined;

	const sim = simulateFutures({
		teams: futuresTeams,
		numGamesPlayoffSeries: g.get("numGamesPlayoffSeries"),
		iterations: FUTURES_ITERATIONS,
		seed,
		ratingUncertainty: futuresUncertainty,
		schedule: futuresSchedule,
		hcaPoints,
		sigma: futuresSigma,
		playoffsNeutral,
		finalsNeutral,
	});

	// Regular-season markets (division winners, win totals) are DECIDED once the
	// playoffs start - a real book closes them, so we don't offer them at all.
	const regularSeasonOver = phase >= PHASE.PLAYOFFS;
	// Likewise, the champion/conference winner are decided the moment the
	// playoffs actually finish. The season NUMBER doesn't roll over until next
	// preseason, so without this a champion already crowned weeks ago would
	// stay bettable (on a publicly-known outcome) all the way through the
	// draft lottery, draft, and free agency.
	const playoffsOver = phase > PHASE.PLAYOFFS;

	// During the playoffs, price the title/conference markets from the ACTUAL
	// bracket - who's alive and each series' current score - instead of the
	// hypothetical seeded-by-record bracket above, which knows nothing about the
	// real playoffs (it kept eliminated teams as favorites and gave a team up
	// 3-0 no credit). Teams no longer in the bracket are delisted, like a real
	// book. Falls back to the hypothetical sim if the bracket can't be read.
	let bracketSim: ReturnType<typeof simulatePlayoffBracket> | undefined;
	// Which of those markets the bracket still leaves undecided. Undefined until
	// the playoffs start, when everything is open.
	let marketsOpen: ReturnType<typeof bracketMarketsOpen> | undefined;
	if (regularSeasonOver && !playoffsOver) {
		try {
			// currentRound is -1 while the play-in runs; round 0's matchups exist
			// then with provisional play-in slots, an acceptable approximation for
			// the day or two it lasts.
			const rnd = Math.max(0, playoffSeries?.currentRound ?? 0);
			const roundMatchups = playoffSeries?.series[rnd] ?? [];
			const matchups: BracketMatchup[] = roundMatchups.map((m) => ({
				home: { tid: m.home.tid, cid: m.home.cid, won: m.home.won ?? 0 },
				away: m.away
					? { tid: m.away.tid, cid: m.away.cid, won: m.away.won ?? 0 }
					: undefined,
			}));
			if (matchups.length > 0) {
				marketsOpen = bracketMarketsOpen({
					matchups,
					bestOf: g.get("numGamesPlayoffSeries")[rnd] ?? 7,
				});
				// Fold the bracket state into the seed so lines are deterministic per
				// state but move as series progress (the regular-season seed inputs
				// are frozen once the playoffs start).
				let bracketSeed = seed + rnd * 7919;
				for (const [i, m] of matchups.entries()) {
					bracketSeed +=
						(i + 1) *
						(m.home.won * 13 +
							(m.away?.won ?? 0) * 29 +
							m.home.tid * 3 +
							(m.away?.tid ?? 0));
				}
				const ratings = new Map<number, number>();
				for (const m of matchups) {
					ratings.set(m.home.tid, ratingOf(m.home.tid));
					if (m.away) {
						ratings.set(m.away.tid, ratingOf(m.away.tid));
					}
				}
				// Regular-season finish order, so simulated later rounds put home
				// court where the engine will actually put it.
				const seedOrder = new Map(
					[...activeTeams]
						.sort((a, b) => b.seasonAttrs.won - a.seasonAttrs.won)
						.map((t, i) => [t.tid, i]),
				);
				bracketSim = simulatePlayoffBracket({
					matchups,
					startRound: rnd,
					numGamesPlayoffSeries: g.get("numGamesPlayoffSeries"),
					ratings,
					iterations: FUTURES_ITERATIONS,
					seed: bracketSeed % 2147483647,
					// A whole season has priced these teams; the book's remaining
					// uncertainty is the end-of-ramp value, not the old 3.5-point
					// default that flattened genuine favorites into free money.
					ratingUncertainty: futuresUncertainty,
					hcaPoints: playoffsNeutral ? 0 : hcaPoints,
					sigma: futuresSigma,
					finalsNeutral,
					seedOrder,
				});
			}
		} catch (error) {
			console.error("Sportsbook playoff bracket odds unavailable", error);
		}
	}

	const teamRow = (
		t: (typeof activeTeams)[number],
		prob: number,
		maxAmerican: number,
	) => ({
		tid: t.tid,
		abbrev: t.abbrev,
		region: t.region,
		name: t.name,
		americanOdds: priceFuture(prob, FUTURES_ITERATIONS, maxAmerican),
	});

	const championship =
		playoffsOver || marketsOpen?.title === false
			? []
			: activeTeams
					.filter((t) => !bracketSim || bracketSim.titleProb.has(t.tid))
					.map((t) =>
						teamRow(
							t,
							(bracketSim ? bracketSim.titleProb : sim.titleProb).get(t.tid) ??
								0,
							FUTURES_CAPS.title,
						),
					)
					.filter((r) => r.americanOdds > SETTLED_PRICE)
					.sort((a, b) => a.americanOdds - b.americanOdds);

	// A conference comes off the board as soon as its winner is knowable: once
	// the final series is set, both conference champions are already decided, and
	// leaving the market up made either finalist a guaranteed payout.
	const conferences = playoffsOver
		? []
		: confs
				.filter(
					(conf) =>
						marketsOpen === undefined ||
						marketsOpen.conferenceCids.has(conf.cid),
				)
				.map((conf) => ({
					cid: conf.cid,
					name: conf.name,
					teams: activeTeams
						.filter(
							(t) =>
								t.cid === conf.cid &&
								(!bracketSim || bracketSim.confProb.has(t.tid)),
						)
						.map((t) =>
							teamRow(
								t,
								(bracketSim ? bracketSim.confProb : sim.confProb).get(t.tid) ??
									0,
								FUTURES_CAPS.conference,
							),
						)
						.filter((r) => r.americanOdds > SETTLED_PRICE)
						.sort((a, b) => a.americanOdds - b.americanOdds),
				}));

	const divisions = regularSeasonOver
		? []
		: divs.map((div) => ({
				did: div.did,
				name: div.name,
				teams: activeTeams
					.filter((t) => t.did === div.did)
					.map((t) =>
						teamRow(t, sim.divProb.get(t.tid) ?? 0, FUTURES_CAPS.division),
					)
					.filter((r) => r.americanOdds > SETTLED_PRICE)
					.sort((a, b) => a.americanOdds - b.americanOdds),
			}));

	// Win totals straight from the same simulated distributions. Only offered
	// while a team still has regular-season games to play.
	const winTotals = regularSeasonOver
		? []
		: activeTeams
				.filter((t) => {
					const ft = futuresTeams.find((f) => f.tid === t.tid);
					return (ft?.gamesRemaining ?? 0) > 0;
				})
				.map((t) => {
					const wt = sim.winTotals.get(t.tid)!;
					const ft = futuresTeams.find((f) => f.tid === t.tid)!;
					// Charge both sides for the book's uncertainty in its own line -
					// see winTotalLoad. Only meaningful where the model's error is
					// measured (basketball); the legacy path keeps its shade instead.
					const load = strengthByTid
						? winTotalLoad({
								gamesRemaining: ft.gamesRemaining,
								gp: gamesPlayedOf(t),
								slope: wt.slope,
								winsSd: wt.winsSd,
								sigma: futuresSigma,
							})
						: 0;
					return {
						tid: t.tid,
						abbrev: t.abbrev,
						region: t.region,
						name: t.name,
						line: wt.line,
						over: priceFuture(wt.pOver + load),
						under: priceFuture(1 - wt.pOver + load),
					};
				})
				.sort((a, b) => b.line - a.line);

	// --- Awards (by current award-race position) --------------------------
	// Award futures (MVP, DPOY, ROY, SMOY, MIP, and the All-League/All-Defensive/
	// All-Rookie team boards) close the moment the regular season ends. The
	// awards are earned over the regular season, so once the playoffs begin the
	// race is effectively settled - a real book stops taking bets rather than let
	// them ride through the playoffs and offseason on a near-known outcome. Same
	// principle as win totals/division odds closing at `regularSeasonOver`.
	const awardsClosed = regularSeasonOver;

	type AwardCandidateRow = {
		pid: number;
		name: string;
		tid: number;
		abbrev: string;
		americanOdds: number;
	};
	let awards: {
		award: "mvp" | "dpoy" | "roy" | "smoy" | "mip";
		name: string;
		candidates: AwardCandidateRow[];
	}[] = [];
	let allLeague: ReturnType<typeof buildTierBoard> = [];
	let allDefensive: ReturnType<typeof buildTierBoard> = [];
	let allRookie: AwardCandidateRow[] = [];

	// A shared preseason projection pool (basketball): every active + free-agent
	// player with overall + a defensive rating (and ewa/ws for the All-Star
	// board). Fetched at most once, lazily, since only the boards that need a
	// preseason projection touch it.
	let projectionPool: any[] | undefined;
	const getProjectionPool = async (): Promise<any[]> => {
		if (projectionPool === undefined) {
			const rawPlayers = await idb.cache.players.indexGetAll("playersByTid", [
				PLAYER.FREE_AGENT,
				Infinity,
			]);
			projectionPool = await idb.getCopies.playersPlus(rawPlayers, {
				attrs: ["pid", "name", "tid", "abbrev", "injury", "draft"],
				ratings: ["ovr", "diq"],
				stats: ["ewa", "ws"],
				season,
				mergeStats: "totOnly",
				showNoStats: true,
				// Ranked, not shown. Coarsened to the tens digit a whole decade of
				// players ties and the preseason award boards become arbitrary.
				coarsenRatings: false,
			});
		}
		return projectionPool;
	};

	// Turn an overall-projected field into a single-winner award board: the best
	// player is a heavy favorite (PRESEASON_AWARD_POWER), priced with the futures
	// hold + cap. Used only before games separate the field.
	const projectedAwardBoard = (
		field: {
			pid: number;
			name: string;
			tid: number;
			abbrev: string;
			score: number;
		}[],
	): AwardCandidateRow[] => {
		const top = [...field].sort((a, b) => b.score - a.score).slice(0, 20);
		if (top.length === 0) {
			return [];
		}
		const probs = strengthProbs(
			top.map((p) => p.score),
			PRESEASON_AWARD_POWER,
		);
		return notSettled(
			sortedByPrice(
				top.map((p, i) => ({
					pid: p.pid,
					name: p.name,
					tid: p.tid,
					abbrev: p.abbrev,
					// Closed-form probability - no Monte Carlo tail to shade.
					americanOdds: priceFuture(probs[i] ?? 0, Infinity),
				})),
			),
		);
	};

	if (!awardsClosed) {
		// Single-winner award races (MVP, DPOY, ROY, SMOY, MIP) are priced off the
		// EXACT same model the Award Races page shows - getAwardRaceOdds, the shared
		// source of truth - so the Sportsbook and that page never disagree. (This
		// replaced an older Sportsbook-only overall-blend model that ran badly
		// inflated favorites vs. the award page.)
		const awardKeyByName: Record<
			string,
			"mvp" | "dpoy" | "roy" | "smoy" | "mip"
		> = {
			"Most Valuable Player": "mvp",
			"Defensive Player of the Year": "dpoy",
			"Rookie of the Year": "roy",
			"Sixth Man of the Year": "smoy",
			"Most Improved Player": "mip",
		};
		try {
			const raceOdds = await getAwardRaceOdds(season);
			awards = raceOdds
				.map((race) => {
					const key = awardKeyByName[race.name];
					if (!key) {
						return undefined;
					}
					return {
						award: key,
						name: race.name,
						// A price board reads down from the favorite. The award race
						// hands its candidates over ranked by award score, which is close
						// to but not the same as ranked by price - the win-probability
						// model also weighs talent, and it settles the field by
						// simulation, so two candidates a hair apart can come back a few
						// hundredths of a percent the other way. Sort on the price that's
						// actually shown, and take the twenty shortest.
						candidates: notSettled(
							sortedByPrice(
								race.players.map((p: any) => ({
									pid: p.pid,
									name: p.name,
									tid: p.tid,
									abbrev: p.abbrev,
									americanOdds: p.odds,
								})),
							),
						).slice(0, 20),
					};
				})
				.filter((x) => x !== undefined);
		} catch (error) {
			console.error("Sportsbook award odds unavailable", error);
		}

		// Before any games are played the award-race formulas read no stats, so the
		// races above come back empty ("No candidates"). Offer MVP / DPOY / ROY
		// anyway, projected from overall rating so the top-rated players are heavy
		// favorites - the whole point of a preseason board. SMOY and MIP depend on
		// bench role / year-over-year improvement, which don't exist yet, so they
		// stay closed until games decide them. Once the race formulas have data
		// (games played) these fallbacks don't run and the boards match the Award
		// Races page exactly.
		if (isSport("basketball")) {
			const emptyAward = (key: "mvp" | "dpoy" | "roy") =>
				awards.find((a) => a.award === key)?.candidates.length === 0 ||
				!awards.some((a) => a.award === key);
			if (emptyAward("mvp") || emptyAward("dpoy") || emptyAward("roy")) {
				try {
					const pool = await getProjectionPool();
					const healthy = pool.filter(
						(p: any) => (p.injury?.gamesRemaining ?? 0) === 0,
					);
					const ids = (p: any) => ({
						pid: p.pid,
						name: p.name,
						tid: p.tid,
						abbrev: p.abbrev,
					});
					const boards: Record<"mvp" | "dpoy" | "roy", AwardCandidateRow[]> = {
						mvp: projectedAwardBoard(
							healthy.map((p: any) => ({ ...ids(p), score: ovrOf(p) })),
						),
						// Tilt DPOY toward defensive rating so it isn't an MVP clone.
						dpoy: projectedAwardBoard(
							healthy.map((p: any) => ({
								...ids(p),
								score: ovrOf(p) + 0.5 * ratingVal(p, "diq"),
							})),
						),
						// Rookies only (drafted the prior offseason - see royFilter).
						roy: projectedAwardBoard(
							healthy
								.filter((p: any) => p.draft?.year === season - 1)
								.map((p: any) => ({ ...ids(p), score: ovrOf(p) })),
						),
					};
					const names: Record<"mvp" | "dpoy" | "roy", string> = {
						mvp: "Most Valuable Player",
						dpoy: "Defensive Player of the Year",
						roy: "Rookie of the Year",
					};
					for (const key of ["mvp", "dpoy", "roy"] as const) {
						const existing = awards.find((a) => a.award === key);
						if (existing) {
							if (existing.candidates.length === 0) {
								existing.candidates = boards[key];
							}
						} else {
							awards.push({
								award: key,
								name: names[key],
								candidates: boards[key],
							});
						}
					}
				} catch (error) {
					console.error(
						"Sportsbook preseason award projection unavailable",
						error,
					);
				}
			}
		}

		// The All-League / All-Defensive / All-Rookie TEAM boards have no Award
		// Races equivalent to match, so they keep their own tier-membership model.
		// It blends an overall projection early (earlyWeight) so a fresh season
		// shows the best players as favorites instead of flat noise, then hands off
		// to the real award formula as games are played.
		if (isSport("basketball")) {
			const earlyWeight = 1 - seasonProgress;
			const AWARD_OVR_COEF = 1 / 40;
			try {
				const players = await getPlayers(season);

				const mvpField = getTopPlayers(
					{
						amount: 40,
						score: projectedScore(mvpScore, earlyWeight, AWARD_OVR_COEF),
					},
					players,
				) as unknown as TierCandidate[];
				allLeague = buildTierBoard(
					mvpField,
					TEAM_AWARD_TIER_SIZES,
					seed + 101,
					tierNoiseFactor,
				);

				const dpoyField = getTopPlayers(
					{
						amount: 40,
						score: projectedScore(dpoyScore, earlyWeight, AWARD_OVR_COEF),
					},
					players,
				) as unknown as TierCandidate[];
				allDefensive = buildTierBoard(
					dpoyField,
					TEAM_AWARD_TIER_SIZES,
					seed + 202,
					tierNoiseFactor,
				);

				const royField = getTopPlayers(
					{
						amount: 20,
						filter: royFilter,
						score: projectedScore(royScore, earlyWeight, AWARD_OVR_COEF),
					},
					players,
				) as unknown as TierCandidate[];
				allRookie =
					buildTierBoard(royField, [5], seed + 303, tierNoiseFactor)[0]
						?.candidates ?? [];
			} catch (error) {
				console.error("Sportsbook award-team odds unavailable", error);
			}
		}
	}

	// --- All-Star Team futures ----------------------------------------------
	// Decided earlier in the season than the year-end awards (whenever the
	// roster is actually selected - see worker/core/allStar/create.ts), so
	// gated on its OWN existence check, independent of the awards board above.
	let allStar: AwardCandidateRow[] = [];
	const allStarsDecided =
		(await idb.getCopy.allStars({ season }, "noCopyCache")) !== undefined;
	if (!allStarsDecided && isSport("basketball")) {
		try {
			// Same pool as the real selection (worker/core/allStar/create.ts) -
			// includes free agents.
			const pool = await getProjectionPool();
			const allStarEarly = 1 - seasonProgress;
			const rosterSize = g.get("allStarNum") * 2;

			// The overall projection, on the same scale as the ewa/ws production it
			// hands off to: a star projects like a star, and everyone below
			// replacement level (~45 ovr) projects at 0, so the field spreads out
			// instead of bunching. Blended with real ewa/ws, which take over as games
			// are played.
			const ovrProjection = (p: any) => Math.max(0, ovrOf(p) - 45) * 1.3;
			const scored = pool
				.filter((p: any) => (p.injury?.gamesRemaining ?? 0) === 0)
				.map((p: any) => ({
					pid: p.pid,
					name: p.name,
					tid: p.tid,
					abbrev: p.abbrev,
					awardScore:
						2.5 * (p.stats?.ewa ?? 0) +
						(p.stats?.ws ?? 0) +
						allStarEarly * ovrProjection(p),
				}))
				// Restrict to the genuine contenders (the roster plus a buffer of
				// bubble players). Feeding the whole ~400-player league to the
				// tier-membership Monte Carlo inflated the score spread so much that
				// the noise washed out the separation - leaving stars barely favored
				// and role players near even. With a contender-sized field, the elite
				// sit far above the cutoff and price like the locks they are.
				.sort((a, b) => b.awardScore - a.awardScore)
				.slice(0, rosterSize + 24);

			// Low noise so the clear top players are heavy favorites (not coin flips)
			// while the bubble spots stay competitive.
			allStar =
				buildTierBoard(scored, [rosterSize], seed + 404, 0.4)[0]?.candidates ??
				[];
		} catch (error) {
			console.error("Sportsbook All-Star odds unavailable", error);
		}
	}

	return {
		games,
		championship,
		conferences,
		divisions,
		winTotals,
		awards,
		allLeague,
		allDefensive,
		allRookie,
		allStar,
		// How many players make the All-Star team (both rosters), so the UI can
		// block parlaying more "makes the All-Star team" legs than fit.
		allStarRosterSize: g.get("allStarNum") * 2,
	};
};
