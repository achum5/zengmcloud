import { g } from "../../util/index.ts";
import { idb } from "../../db/index.ts";
import teamOvr from "../team/ovr.ts";
import getSchedule from "../season/getSchedule.ts";
import { getPlayoffsByConf } from "../season/getPlayoffsByConf.ts";
import { buildGameLinePricer } from "./gameLines.ts";
import { injuryTicksUntil } from "../game/calendarTicks.ts";
import { getInjuryClock } from "../game/classicDays.ts";
import { calendarPlayThrough } from "../game/calendarPlayThrough.ts";
import getAwardRaceOdds, {
	legacyAwardKey,
	playerName,
	playerTalent,
	projectPlayers,
} from "../season/getAwardRaceOdds.ts";
import { getAwardCandidates } from "../awards/getAwardCandidates.ts";
import {
	getTeamAtsRecords,
	formatAtsRecord,
} from "../../util/getTeamAtsRecords.ts";
import {
	PHASE,
	PLAYER,
	RATINGS,
	TEAM_AWARD_INFO,
} from "../../../common/constants.ts";
import {
	BASKETBALL_PLAYOFF_HCA_FACTOR,
	gameLengthFactor,
	homeCourtAdvantagePoints,
} from "../../../common/getGameSpread.ts";
import {
	futuresTailVig,
	probToAmerican,
	probToAmericanOnOdds,
	SPORTSBOOK_FUTURES_VIG,
	SPORTSBOOK_MAX_AMERICAN,
} from "../../../common/sportsbook.ts";
import {
	BASKETBALL_MARGIN_SHRINK,
	capTopProb,
	MARGIN_SIGMA,
	mcShade,
	mulberry32,
	normalSample,
	softCapMargin,
	strengthProbs,
	tierMembershipProbs,
} from "../../../common/sportsbookOdds.ts";
import { isAllStarGame } from "../../../common/specialGames.ts";
import { getActualPlayThroughInjuries } from "../game/loadTeams.ts";
import {
	getTeamSpreadBias,
	SPREAD_BIAS_SD,
	spreadBiasPriorGames,
} from "../../util/getTeamSpreadBias.ts";
import {
	bracketMarketsOpen,
	simulateFutures,
	simulatePlayoffBracket,
	type BracketMatchup,
	type PlayInGame,
	type PlayInState,
} from "../../../common/sportsbookFutures.ts";
import { player, team } from "../index.ts";
import { getLinesModelRatings } from "./futuresRatings.ts";
import { offBoardWhileLive } from "./liveHold.ts";
import { getLiveGate } from "../../util/liveGameGate.ts";

const TEAM_AWARD_TIER_TITLES = ["First Team", "Second Team", "Third Team"];

// How many teams the league's own award settings name for a team award - "ALL"
// All-League, "DEF" All-Defensive, "ALR" All-Rookie, the abbrevs bets settle
// by (see legacyAwardPids) - or undefined when there is no single league-wide
// one to price (none in the settings, or picked by conference or division).
// The board used to assume three All-Defensive teams of five where the
// league names two (every Third Team bet a loser) and one All-Rookie team
// where it names two (ten men make it, priced as if five did).
const teamAwardNumTeams = (shortName: string): number | undefined => {
	const award = (
		g.get("awards") as {
			shortName: string;
			numTeams?: number;
			group?: unknown;
		}[]
	).find((a) => a.shortName === shortName);
	if (!award || award.numTeams === undefined || award.group !== undefined) {
		return undefined;
	}
	return award.numTeams;
};
const tierSizesFor = (shortName: string): number[] =>
	Array.from({ length: teamAwardNumTeams(shortName) ?? 0 }, () =>
		TEAM_AWARD_INFO.byPos ? 5 : TEAM_AWARD_INFO.numPlayersPerTeam,
	);
// Players on the All-Rookie team(s), the parlay cap and the board size.
export const allRookieSpots = (): number =>
	tierSizesFor("ALR").reduce((a, b) => a + b, 0);

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

// THE SAME NUMBER FOR A PLAYOFF GAME, AND IT IS NOT THE SAME NUMBER.
//
// The engine plays the postseason with synergy counting roughly double, so the
// playoff margin model leans on the least certain thing the book knows about a
// roster - how well it fits together - about twice as hard as the regular-season
// one does. Measured by spreadCalibration.test.ts in PLAYOFFS mode on a real
// league's rosters: residual sd 3.54 against a 1.69 sim-noise floor, so 3.11
// per pairing and 2.20 per team, against 1.3 in the regular season.
//
// What the bracket needs is the EXTRA miss the playoff translation adds on top
// of whatever the season already taught the book - the two errors share their
// inputs, so it is the part that is not already in FUTURES_MODEL_ERROR:
// sqrt(2.20^2 - 1.3^2).
export const FUTURES_PLAYOFF_MODEL_ERROR = 2.2;

export const FUTURES_PLAYOFF_DELTA_ERROR = Math.sqrt(
	FUTURES_PLAYOFF_MODEL_ERROR ** 2 - FUTURES_MODEL_ERROR ** 2,
);

// How unsure the book is about a team's PLAYOFF strength: its regular-season
// uncertainty, plus the independent miss above.
//
// THIS IS WHAT KEEPS A BRACKET HONEST. Priced at the regular-season number the
// board reads its own ordering as gospel, and that shows up exactly where a
// bracket is decided - between contenders whose ratings sit within a couple of
// points of each other. Measured against the engine's own playoffs
// (game/bracketCalibration.test.ts), pricing a four-team pack inside 2.5 points
// at the regular-season uncertainty gave the nominal favourite 33% where the
// engine gave it 22%, and 9% to a team the engine won 28% with. A -1200 title
// favourite is the same defect at the other end of the field.
export const futuresPlayoffUncertainty = (regularUncertainty: number) =>
	Math.hypot(regularUncertainty, FUTURES_PLAYOFF_DELTA_ERROR);

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

// How far off the book's own rating could be, in points, when it prices an
// in-progress bracket. The heuristic ratings above are a coarse read of a
// roster, and this is the figure they were always priced with; the shared
// default in sportsbookFutures moved on and this did not follow it.
// (Basketball's is measured - see linesFuturesUncertainty.)
const LEGACY_BRACKET_UNCERTAINTY = 3.5;

// No futures price shorter than this is ever offered - the outcome is treated
// as settled and its row comes off the board, like a real book pulling a
// market once it's decided. The favorite side of the odds clamp tops out at
// -9900 (implied 99%), so a TRULY clinched outcome left up would be a
// guaranteed ~1% payout - literal free money, riskless. Exported for the EV
// harness.
export const SETTLED_PRICE = -9000;
const notSettled = <T extends { americanOdds: number }>(rows: T[]): T[] =>
	rows.filter((r) => r.americanOdds > SETTLED_PRICE);
// A SINGLE-winner market (a title, a conference, a division, an award) comes
// down whole once its winner is as good as known - as a real book pulls it -
// rather than losing just the favourite and leaving the rest of the field up
// at the longest prices it offers, for an outcome already decided against
// them. (A tier board, five places to a tier, takes a decided man off and
// keeps going: see notSettled.)
const openUnlessDecided = <T extends { americanOdds: number }>(
	rows: T[],
): T[] => (rows.some((r) => r.americanOdds <= SETTLED_PRICE) ? [] : rows);

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
	const shaded = Math.min(1, mcShade(prob, iterations));
	return probToAmericanOnOdds(shaded, {
		vig: futuresTailVig(shaded, SPORTSBOOK_FUTURES_VIG),
		maxAmerican,
	});
};

// How sharply each preseason award board (projected, before a game is
// played) leans to the top of its field - fitted on the winners of 50 audited
// seasons and 48 first seasons of new leagues. MVP goes to the best player
// about as often as 1.8 says. ROY goes to the best rookie far more often (76%
// of seasons, where 1.8 priced him at 49%, so backing him returned +35%).
// DPOY far less, being won on blocks and steals the ratings only half see:
// with last season's race to go on as well (see below), 1.3; on ratings
// alone, 0.8. And nothing is a lock before the season: no favourite past
// 90%, or an injury-free year is taken for granted.
const PRESEASON_AWARD_POWER = {
	mvp: 1.8,
	dpoy: 1.3,
	dpoyRatingsOnly: 0.8,
	roy: 3.2,
};
const PRESEASON_AWARD_MAX_PROB = 0.9;

// A player's overall rating, from either an array of season ratings (getPlayers)
// or a single season's ratings object (player.formatAll with a fixed season).
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

// Team overall for the futures board (sports other than basketball - see
// linesFuturesUncertainty for basketball's), injuries counted: averaged over the days
// of the regular season still to play - each man in it for the days he is fit
// for (or playing through, at that discount) - and as the team will be when
// the playoffs start. Real ratings (no fuzz): the book knows its business.
// `daysLeft` is regular-season schedule days; injuries count down a day at a
// time. During the playoffs it is the playoff days ahead instead, and both
// numbers are the team over those.
const FUTURES_OVR_SAMPLES = 24;
export const getFuturesTeamOvrs = async (
	teams: { tid: number; playThroughInjuries: [number, number] }[],
	season: number,
	daysLeft: number,
	inPlayoffs: boolean,
): Promise<Map<number, { season: number; playoffs: number }>> => {
	const ratings = [
		"ovr",
		"pos",
		"ovrs",
		...(__SPORT === "basketball" ? RATINGS : []),
	] as const;
	const out = new Map<number, { season: number; playoffs: number }>();
	const days = Math.max(1, Math.round(daysLeft));
	const step = Math.max(1, Math.ceil(days / FUTURES_OVR_SAMPLES));
	for (const t of teams) {
		const rawPlayers = await idb.cache.players.indexGetAll(
			"playersByTid",
			t.tid,
		);
		const teamPlayers = (await player.formatAll(rawPlayers, {
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
		})) as any[];
		const pti = getActualPlayThroughInjuries(t);
		// (The regular-season formula either way, with the playoff
		// play-through setting there.)
		const at = (numDaysInFuture: number, playoffs: boolean) =>
			teamOvr(teamPlayers, {
				accountForInjuredPlayers: {
					numDaysInFuture,
					playThroughInjuries: playoffs ? [pti[1], pti[1]] : pti,
				},
			});
		let sum = 0;
		let n = 0;
		for (let d = 0; d < days; d += step) {
			sum += at(d, inPlayoffs);
			n += 1;
		}
		const mean = sum / Math.max(1, n);
		out.set(t.tid, {
			season: mean,
			playoffs: inPlayoffs ? mean : at(days, true),
		});
	}
	return out;
};

// THE BASKETBALL FUTURES ARE PRICED ON THE GAME LINES' OWN SCALE
// (futuresRatings.ts): each team's strength is exactly what tonight's spread
// reads - overall and lineup synergy at the measured coefficients, injuries
// for the days they last, plus the per-team form correction - instead of the
// overall-only rating above blended with raw point differential. Graded over
// twenty audited seasons (linesAudit.test.ts), snapshot by snapshot from
// preseason to the finals, it called the champion and the conference winners
// clearly better (log loss 1.49 against 1.63, and 1.35 against 1.46) and set
// win totals closer (3.8 wins off at the end against 4.1).
//
// How unsure it is of a team is then measured, not tuned: the part of a team
// the formula cannot see is the per-team offset getTeamSpreadBias measures
// (spread SPREAD_BIAS_SD across teams), and after gp games the book's
// estimate of it is off by SPREAD_BIAS_SD * sqrt(K / (K + gp)) - K being that
// correction's own prior weight. In a playoff game the translation adds its
// own independent miss (FUTURES_PLAYOFF_MODEL_ERROR is the whole of it).
const linesFuturesUncertainty = (meanGp: number) =>
	SPREAD_BIAS_SD *
	Math.sqrt(spreadBiasPriorGames() / (spreadBiasPriorGames() + meanGp));
const LINES_FUTURES_PLAYOFF_EXTRA = Math.sqrt(
	Math.max(0, FUTURES_PLAYOFF_MODEL_ERROR ** 2 - SPREAD_BIAS_SD ** 2),
);

// THE ALL-STAR BOARD. The roster is picked on All-Star night: the top of the
// league by 2.5 * EWA + WS at that moment (allStar/create.ts), and a man hurt
// that night is still named - he is replaced, and his bet still wins. So each
// player is projected to that night: what he has, plus a per-game rate times
// the games he will play before it. The rate is a Bayesian blend of his own
// and what his overall says (0.073 a game per point over 55, worth 5.4
// games), and the remaining games carry their noise (0.6 per root game, and
// the rate's own doubt on top). A Monte Carlo then picks the roster the way
// the game does, top healthy players to the roster size with anyone hurt on
// the night riding along (9% of the field, as measured).
//
// Fitted on 27 audited seasons and graded snapshot by snapshot: priced and
// made-it rates agree in every band from preseason to the eve of the game.
// The board it replaced left every injured player off (named anyway when good
// enough, so their bets were never on offer), read raw season totals plus an
// overall term that faded over the whole season rather than the first half,
// and was so wide early that backing the field returned +51% preseason.
const ALL_STAR_RATE_PER_OVR = 0.073;
const ALL_STAR_RATE_FLOOR_OVR = 55;
const ALL_STAR_PRIOR_GAMES = 5.4;
const ALL_STAR_GAME_NOISE = 0.6;
const ALL_STAR_RATE_NOISE = 0.245;
const ALL_STAR_HURT_ON_NIGHT = 0.09;
const ALL_STAR_ITERATIONS = 3000;
const allStarBoard = ({
	pool,
	schedule,
	numGames,
	allStarNum,
	byConf,
	seed,
}: {
	pool: any[];
	schedule: { day: number; homeTid: number; awayTid: number }[];
	numGames: number;
	allStarNum: number;
	// tid -> cid when each conference picks its own allStarNum.
	byConf: Map<number, number> | undefined;
	seed: number;
}) => {
	// Each team's games before All-Star night, from the schedule; before there
	// is one, where the night falls in a season (the engine places it after
	// allStarGame of the season's games: ~0.63 of a team's games at 0.7, as
	// measured).
	const night = schedule.find((m) => isAllStarGame(m.homeTid, m.awayTid));
	const gamesBefore = new Map<number, number>();
	if (night) {
		for (const m of schedule) {
			if (m.day < night.day && m.homeTid >= 0 && m.awayTid >= 0) {
				gamesBefore.set(m.homeTid, (gamesBefore.get(m.homeTid) ?? 0) + 1);
				gamesBefore.set(m.awayTid, (gamesBefore.get(m.awayTid) ?? 0) + 1);
			}
		}
	}
	const fallbackLeft = Math.round(
		0.9 * Math.max(0, g.get("allStarGame") ?? 0.7) * numGames,
	);
	const field = pool
		.map((p: any) => {
			const score = 2.5 * (p.stats?.ewa ?? 0) + (p.stats?.ws ?? 0);
			const gp = p.stats?.gp ?? 0;
			const left =
				p.tid < 0
					? 0
					: night
						? (gamesBefore.get(p.tid) ?? 0)
						: Math.max(0, fallbackLeft - gp);
			const injured = p.injury?.gamesRemaining ?? 0;
			const games = Math.max(0, left - injured);
			const prior =
				ALL_STAR_RATE_PER_OVR * Math.max(0, ovrOf(p) - ALL_STAR_RATE_FLOOR_OVR);
			const shrink = ALL_STAR_PRIOR_GAMES / (gp + ALL_STAR_PRIOR_GAMES);
			const rate =
				(score + ALL_STAR_PRIOR_GAMES * prior) / (gp + ALL_STAR_PRIOR_GAMES);
			return {
				pid: p.pid as number,
				name: p.name as string,
				tid: p.tid as number,
				abbrev: p.abbrev as string,
				projection: score + rate * games,
				sd: Math.sqrt(
					ALL_STAR_GAME_NOISE ** 2 * games +
						(ALL_STAR_RATE_NOISE * games) ** 2 * shrink,
				),
				// Out past the night itself: named, but not one of the healthy
				// roster spots.
				outOnNight: injured > left,
				group: byConf ? (byConf.get(p.tid) ?? -1) : 0,
			};
		})
		.filter((p) => !byConf || p.group >= 0)
		.sort((a, b) => b.projection - a.projection)
		.slice(0, 100);
	const groups = [...new Set(field.map((p) => p.group))];
	const spots = byConf ? allStarNum : 2 * allStarNum;
	const made = field.map(() => 0);
	const rand = mulberry32(seed);
	const values = field.map(() => 0);
	const healthy = field.map(() => true);
	for (let iter = 0; iter < ALL_STAR_ITERATIONS; iter++) {
		for (const [i, p] of field.entries()) {
			values[i] = p.projection + normalSample(rand) * p.sd;
			healthy[i] = !p.outOnNight && rand() >= ALL_STAR_HURT_ON_NIGHT;
		}
		for (const group of groups) {
			const healthyValues = field
				.map((p, i) =>
					p.group === group && healthy[i] ? values[i]! : -Infinity,
				)
				.sort((x, y) => y - x);
			const cutoff = healthyValues[spots - 1] ?? -Infinity;
			for (const [i, p] of field.entries()) {
				if (p.group === group && values[i]! >= cutoff) {
					made[i]! += 1;
				}
			}
		}
	}
	return notSettled(
		sortedByPrice(
			field.map((p, i) => ({
				pid: p.pid,
				name: p.name,
				tid: p.tid,
				abbrev: p.abbrev,
				americanOdds: priceFuture(
					made[i]! / ALL_STAR_ITERATIONS,
					ALL_STAR_ITERATIONS,
				),
			})),
		),
	).slice(0, 40);
};

// The whole live odds board, computed from current league state.
export const getLines = async () => {
	const season = g.get("season");
	const numGames = g.get("numGames");
	const confs = g.get("confs");
	const divs = g.get("divs");

	const teams = await team.getFormattedAll({
		attrs: [
			"tid",
			"cid",
			"did",
			"abbrev",
			"region",
			"name",
			"disabled",
			"playThroughInjuries",
			"playThroughInjuryDays",
		],
		seasonAttrs: ["won", "lost", "tied", "otl"],
		stats: ["pts", "oppPts", "gp"],
		season,
		// Without this, teams with no stats rows are dropped entirely - in a
		// league with no games played yet (e.g. started in the playoffs) that's
		// EVERY team, which blanked the whole board.
		showNoStats: true,
	});
	const activeTeams = teams.filter((t) => !t.disabled);
	const teamByTid = new Map(activeTeams.map((t) => [t.tid, t]));

	// One sweep of the season's games feeds both: the against-the-spread record
	// shown next to each team's W-L, and the per-team form correction the lines
	// are priced with (getTeamSpreadBias.ts).
	const seasonGames = await idb.merged.games.getAll({ season });
	const atsRecords = await getTeamAtsRecords(season, seasonGames);
	const spreadBiases = await getTeamSpreadBias(season, seasonGames);

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
	// A game still being played out live: what it decided is in the league
	// already, so the markets it moves are off the board till it is over on
	// screen (see liveHold.ts).
	const gate = await getLiveGate();

	// Shared with the per-game prop board, so a spread/moneyline/total quoted on
	// a game's page is the same number this board carries - see gameLines.ts.
	const injuryClock = await getInjuryClock();
	const pricer = await buildGameLinePricer({
		activeTeams,
		season,
		todayDay: schedule[0]?.day ?? 0,
		ticksUntil: injuryTicksUntil(schedule, injuryClock),
		calendar: calendarPlayThrough(schedule, injuryClock),
		spreadBiases,
	});

	const games = [];
	for (const matchup of schedule) {
		if (matchup.homeTid < 0 || matchup.awayTid < 0) {
			continue; // All-Star / special games
		}
		if (offBoardWhileLive(gate, matchup)) {
			continue;
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
			day: matchup.day,
			date: matchup.date,
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
	// (Basketball: a game's result comes in at 0.96 of the lines model's margin
	// - BASKETBALL_MARGIN_SHRINK, as its moneyline is priced - so every margin
	// the futures play out is read on that scale too. Across 31,600 audited
	// games results came in at 0.96-0.97 of the model's margin from the second
	// week to the playoffs (0.89 in the first), and without the shrink the win
	// totals ran too far from the middle: on opening night actual wins came in
	// at 0.90 of a line's distance from it.)
	const futuresSigma =
		(MARGIN_SIGMA * Math.sqrt(lengthFactor)) /
		(__SPORT === "basketball" ? BASKETBALL_MARGIN_SHRINK : 1);
	// Each team's overall for the futures, injuries counted (getFuturesTeamOvrs):
	// over the regular-season days still to play - its schedule, or a season's
	// worth of games before there is one - and, once the playoffs are on, over
	// the playoff days still ahead, about six a round.
	// (On a calendar, injuries count down by the team's games instead: about
	// as many as each team has left.)
	const teamGameRows = schedule.filter(
		(game) => game.homeTid >= 0 && game.awayTid >= 0,
	);
	const scheduleDays =
		schedule.length === 0
			? 0
			: teamGameRows.some((game) => game.date !== undefined)
				? Math.ceil((2 * teamGameRows.length) / Math.max(1, activeTeams.length))
				: schedule.at(-1)!.day - schedule[0]!.day + 1;
	const roundsLeft = Math.max(
		1,
		g.get("numGamesPlayoffSeries").length -
			Math.max(0, playoffSeries?.currentRound ?? 0),
	);
	const inPlayoffs = phase === PHASE.PLAYOFFS;
	const futuresDaysLeft = inPlayoffs
		? 6 * roundsLeft
		: phase < PHASE.REGULAR_SEASON || scheduleDays === 0
			? numGames
			: scheduleDays;
	// (Basketball replaces these below with the game lines' own ratings.)
	const futuresOvrs =
		__SPORT === "basketball"
			? new Map<number, { season: number; playoffs: number }>()
			: await getFuturesTeamOvrs(
					activeTeams,
					season,
					futuresDaysLeft,
					inPlayoffs,
				);
	const meanOf = (key: "season" | "playoffs") =>
		activeTeams.reduce((s, t) => s + (futuresOvrs.get(t.tid)?.[key] ?? 50), 0) /
		Math.max(1, activeTeams.length);
	const meanOvr = meanOf("season");
	const meanPlayoffOvr = meanOf("playoffs");

	// A team's strength as a point margin vs an average team: its model rating
	// (its overall, as above) blended with what it has actually done (its real
	// point differential) - for the regular season, and for the playoffs.
	const ratingFrom = (tid: number, ovr: number, mean: number) => {
		const s = statsOf(teamByTid.get(tid));
		const actualMOV = s.gp > 0 ? s.pts - s.oppPts : 0; // per-game differential
		// Other sports: the Power Rankings heuristic, as it always was (only the
		// injuries are new). Trust what the team has actually done more and more
		// as the sample grows: by ~30 games the real point differential carries
		// 3/4 of the weight.
		const perfWeight = s.gp > 0 ? 0.75 * Math.min(1, s.gp / 30) : 0;
		const blended =
			(ovr - mean) * 0.6 * (1 - perfWeight) + actualMOV * perfWeight;

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
	let ratingOf = (tid: number) =>
		ratingFrom(tid, futuresOvrs.get(tid)?.season ?? meanOvr, meanOvr);
	// In PLAYOFF games: the team as it will be then.
	let playoffRatingOf = (tid: number) =>
		ratingFrom(
			tid,
			futuresOvrs.get(tid)?.playoffs ?? meanPlayoffOvr,
			meanPlayoffOvr,
		);
	if (__SPORT === "basketball") {
		// See linesFuturesUncertainty: basketball reads the game lines' model.
		const lm = await getLinesModelRatings({
			teams: activeTeams,
			season,
			daysLeft: futuresDaysLeft,
			inPlayoffs,
			spreadBiases,
			lengthFactor,
		});
		ratingOf = (tid) => lm.get(tid)?.season ?? 0;
		playoffRatingOf = (tid) => lm.get(tid)?.playoffs ?? 0;
	}

	// Home court in a playoff series, as the engine plays it (basketball's is
	// measured bigger than the regular season's) - none on a neutral floor.
	const neutralSiteSetting = g.get("neutralSite");
	const playoffsNeutral = neutralSiteSetting === "playoffs";
	const finalsNeutral = neutralSiteSetting === "finals";
	const playoffHca =
		__SPORT === "basketball"
			? homeCourtAdvantagePoints(g.get("homeCourtAdvantage")) *
				BASKETBALL_PLAYOFF_HCA_FACTOR *
				lengthFactor
			: 0;

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
	const meanGp =
		activeTeams.reduce((sum, t) => sum + gamesPlayedOf(t), 0) /
		Math.max(1, activeTeams.length);
	const futuresUncertainty =
		__SPORT === "basketball"
			? linesFuturesUncertainty(meanGp)
			: futuresRatingUncertainty(seasonProgress);

	// Only where the playoffs are played by conference does "wins the
	// conference" name a series (otherwise both finalists can come from one).
	const byConf = (await getPlayoffsByConf(season)) !== false;
	const sim = simulateFutures({
		teams: futuresTeams,
		playIn: g.get("playIn"),
		byConf,
		numGamesPlayoffSeries: g.get("numGamesPlayoffSeries"),
		iterations: FUTURES_ITERATIONS,
		seed,
		ratingUncertainty: futuresUncertainty,
		playoffRatingExtraUncertainty:
			__SPORT === "basketball" ? LINES_FUTURES_PLAYOFF_EXTRA : 0,
		sigma: futuresSigma,
		...(__SPORT === "basketball"
			? {
					// Each team's own slate - who it still plays, and where - not
					// a league-average one: a team in a strong division has the
					// harder road, and the win totals know it.
					schedule: schedule.filter((m) => m.homeTid >= 0 && m.awayTid >= 0),
					hcaPoints:
						homeCourtAdvantagePoints(g.get("homeCourtAdvantage")) *
						lengthFactor,
					playoffRatings: new Map(
						activeTeams.map((t) => [t.tid, playoffRatingOf(t.tid)]),
					),
					playoffHcaPoints: playoffHca,
					playoffsNeutral,
					finalsNeutral,
				}
			: {}),
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
			// currentRound is -1 while the play-in runs: round 0's matchups exist
			// then with the play-in's slots held provisionally, and the play-in
			// itself is simulated into them (see PlayInState).
			const rnd = Math.max(0, playoffSeries?.currentRound ?? 0);
			const roundMatchups = playoffSeries?.series[rnd] ?? [];
			const slot = (t: (typeof roundMatchups)[number]["home"]) => ({
				tid: t.tid,
				cid: t.cid,
				won: t.won ?? 0,
				...(t.pendingPlayIn ? { pending: true } : {}),
			});
			const matchups: BracketMatchup[] = roundMatchups.map((m) => ({
				home: slot(m.home),
				away: m.away ? slot(m.away) : undefined,
			}));
			const playIns: PlayInState[] =
				playoffSeries?.currentRound === -1
					? (playoffSeries.playIns ?? []).map((playIn) => {
							const game = (m: (typeof playIn)[number]): PlayInGame => ({
								home: { tid: m.home.tid, cid: m.home.cid },
								away: { tid: m.away.tid, cid: m.away.cid },
								...(m.home.won + m.away.won > 0
									? { winner: m.home.won > 0 ? m.home.tid : m.away.tid }
									: {}),
							});
							return [
								game(playIn[0]),
								game(playIn[1]),
								playIn[2] ? game(playIn[2]) : undefined,
							];
						})
					: [];
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
				for (const [i, game] of playIns.flat().entries()) {
					if (game?.winner !== undefined) {
						bracketSeed += (i + 1) * 101 * (game.winner + 1);
					}
				}
				const ratings = new Map<number, number>();
				for (const m of matchups) {
					ratings.set(m.home.tid, playoffRatingOf(m.home.tid));
					if (m.away) {
						ratings.set(m.away.tid, playoffRatingOf(m.away.tid));
					}
				}
				for (const playIn of playIns) {
					for (const game of playIn) {
						if (game) {
							ratings.set(game.home.tid, playoffRatingOf(game.home.tid));
							ratings.set(game.away.tid, playoffRatingOf(game.away.tid));
						}
					}
				}
				// Home court in a round still to come goes to the better regular
				// season, as the engine schedules it.
				const seedOrder = new Map(
					[...activeTeams]
						.sort(
							(a, b) =>
								b.seasonAttrs.won / Math.max(1, gamesPlayedOf(b)) -
								a.seasonAttrs.won / Math.max(1, gamesPlayedOf(a)),
						)
						.map((t, i) => [t.tid, i]),
				);
				bracketSim = simulatePlayoffBracket({
					matchups,
					startRound: rnd,
					numGamesPlayoffSeries: g.get("numGamesPlayoffSeries"),
					ratings,
					iterations: FUTURES_ITERATIONS,
					seed: bracketSeed % 2147483647,
					// The season's remaining doubt about each team and the
					// playoff translation's own miss, together.
					ratingUncertainty:
						__SPORT === "basketball"
							? Math.hypot(
									linesFuturesUncertainty(meanGp),
									LINES_FUTURES_PLAYOFF_EXTRA,
								)
							: LEGACY_BRACKET_UNCERTAINTY,
					sigma: futuresSigma,
					playIns,
					...(__SPORT === "basketball"
						? {
								hcaPoints: playoffsNeutral ? 0 : playoffHca,
								finalsNeutral,
								seedOrder,
							}
						: {}),
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
			: openUnlessDecided(
					activeTeams
						.filter((t) => !bracketSim || bracketSim.titleProb.has(t.tid))
						.map((t) =>
							teamRow(
								t,
								(bracketSim ? bracketSim.titleProb : sim.titleProb).get(
									t.tid,
								) ?? 0,
								FUTURES_CAPS.title,
							),
						)
						.sort((a, b) => a.americanOdds - b.americanOdds),
				);

	// A conference comes off the board as soon as its winner is knowable: once
	// the final series is set, both conference champions are already decided, and
	// leaving the market up made either finalist a guaranteed payout.
	// Only where the playoffs are played by conference: otherwise "wins the
	// conference" names no series at all - both finalists can come from one.
	const conferences =
		playoffsOver || !byConf
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
						teams: openUnlessDecided(
							activeTeams
								.filter(
									(t) =>
										t.cid === conf.cid &&
										(!bracketSim || bracketSim.confProb.has(t.tid)),
								)
								.map((t) =>
									teamRow(
										t,
										(bracketSim ? bracketSim.confProb : sim.confProb).get(
											t.tid,
										) ?? 0,
										FUTURES_CAPS.conference,
									),
								)
								.sort((a, b) => a.americanOdds - b.americanOdds),
						),
					}))
					.filter((conf) => conf.teams.length > 0);

	const divisions = regularSeasonOver
		? []
		: divs
				.map((div) => ({
					did: div.did,
					name: div.name,
					teams: openUnlessDecided(
						activeTeams
							.filter((t) => t.did === div.did)
							.map((t) =>
								teamRow(t, sim.divProb.get(t.tid) ?? 0, FUTURES_CAPS.division),
							)
							.sort((a, b) => a.americanOdds - b.americanOdds),
					),
				}))
				.filter((div) => div.teams.length > 0);

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
					// A two-way market on a line set at the coin flip, held like one:
					// the game lines' juice, -110 a side. It used to carry the
					// futures hold and the Monte Carlo shade on top - -135 both ways,
					// a 15% hold no book puts on a win total.
					return {
						tid: t.tid,
						abbrev: t.abbrev,
						region: t.region,
						name: t.name,
						line: wt.line,
						over: probToAmerican(wt.pOver),
						under: probToAmerican(1 - wt.pOver),
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
			projectionPool = await player.formatAll(rawPlayers, {
				attrs: ["pid", "name", "tid", "abbrev", "injury", "draft"],
				ratings: ["ovr", "diq"],
				stats: ["ewa", "ws", "gp"],
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
		power: number,
	): AwardCandidateRow[] => {
		const top = [...field].sort((a, b) => b.score - a.score).slice(0, 20);
		if (top.length === 0) {
			return [];
		}
		const probs = capTopProb(
			strengthProbs(
				top.map((p) => p.score),
				power,
			),
			PRESEASON_AWARD_MAX_PROB,
		);
		return openUnlessDecided(
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
		// Races whose winner is as good as known: pulled, and NOT refilled by the
		// preseason projection below (which used to put a board back up without
		// the winner on it - every bet on it a loser).
		const decided = new Set<string>();
		try {
			const raceOdds = await getAwardRaceOdds(season);
			awards = raceOdds
				.map((race) => {
					const key = legacyAwardKey(race);
					if (!key) {
						return undefined;
					}
					if (race.players.some((p) => p.odds <= SETTLED_PRICE)) {
						decided.add(key);
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
						candidates: openUnlessDecided(
							sortedByPrice(
								race.players.map((p) => ({
									pid: p.pid,
									name: playerName(p),
									tid: p.currentStats.tid,
									abbrev: p.currentStats.abbrev,
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
		if (__SPORT === "basketball") {
			const emptyAward = (key: "mvp" | "dpoy" | "roy") =>
				!decided.has(key) &&
				(awards.find((a) => a.award === key)?.candidates.length === 0 ||
					!awards.some((a) => a.award === key));
			if (emptyAward("mvp") || emptyAward("dpoy") || emptyAward("roy")) {
				try {
					const pool = await getProjectionPool();
					const ids = (p: any) => ({
						pid: p.pid,
						name: p.name,
						tid: p.tid,
						abbrev: p.abbrev,
					});
					// A man hurt for part of the year is still a candidate, marked
					// down for the share of it he misses (a fifth of the season costs
					// about one field-spread of overall, five times less likely; half of
					// it, out of the running). Leaving him off entirely made his win a
					// result nobody could have bet.
					const missShare = (p: any) =>
						Math.min(
							1,
							(p.injury?.gamesRemaining ?? 0) / Math.max(1, numGames),
						);
					const field = pool
						.filter((p: any) => missShare(p) < 0.6)
						.map((p: any) => ({
							...p,
							ratings: { ...p.ratings, ovr: ovrOf(p) - 25 * missShare(p) },
						}));
					// DPOY is won with blocks, steals and defensive win shares, which
					// ratings read poorly: on overall plus defensive IQ alone the
					// preseason board missed so often a bettor backing all of it made
					// money. Last season's DPOY race is the better guide, as it is to
					// a real book - so a returning candidate is read on that and his
					// ratings together, everyone else on ratings a step behind.
					const lastDpoy = new Map<number, number>();
					if (emptyAward("dpoy") && season > g.get("startingSeason")) {
						try {
							const lastRaces = (
								await getAwardCandidates(season - 1, undefined, {
									numPlayersPerIndividualAward: 40,
								})
							).awardCandidates.flat();
							const race = lastRaces.find(
								(r) => r.numTeams === undefined && legacyAwardKey(r) === "dpoy",
							);
							for (const p of race?.players ?? []) {
								const score = p.currentStats.score;
								if (typeof score === "number" && Number.isFinite(score)) {
									lastDpoy.set(p.pid, score);
								}
							}
						} catch {}
					}
					const dpoyScores = (() => {
						const rated = field.map((p: any) => ({
							p,
							rating: ovrOf(p) + 0.5 * ratingVal(p, "diq"),
						}));
						const z = (values: number[]) => {
							const mean = values.reduce((a, b) => a + b, 0) / values.length;
							const sd =
								Math.sqrt(
									values.reduce((a, b) => a + (b - mean) ** 2, 0) /
										values.length,
								) || 1;
							return (v: number) => (v - mean) / sd;
						};
						const top = [...rated]
							.sort((a, b) => b.rating - a.rating)
							.slice(0, 40)
							.map((x) => x.rating);
						const zRating = z(top.length > 0 ? top : [0]);
						const last = [...lastDpoy.values()];
						const zLast = z(last.length > 0 ? last : [0]);
						const floor =
							last.length > 0 ? Math.min(...last.map(zLast)) - 0.5 : 0;
						return rated.map(({ p, rating }) => {
							const prev = lastDpoy.get(p.pid);
							return {
								...ids(p),
								score:
									last.length === 0
										? rating
										: 0.6 * (prev === undefined ? floor : zLast(prev)) +
											0.4 * zRating(rating) -
											5 * missShare(p),
							};
						});
					})();
					const boards: Record<"mvp" | "dpoy" | "roy", AwardCandidateRow[]> = {
						mvp: projectedAwardBoard(
							field.map((p: any) => ({ ...ids(p), score: ovrOf(p) })),
							PRESEASON_AWARD_POWER.mvp,
						),
						dpoy: projectedAwardBoard(
							dpoyScores,
							lastDpoy.size > 0
								? PRESEASON_AWARD_POWER.dpoy
								: PRESEASON_AWARD_POWER.dpoyRatingsOnly,
						),
						// Rookies only (drafted the prior offseason - see royFilter).
						roy: projectedAwardBoard(
							field
								.filter((p: any) => p.draft?.year === season - 1)
								.map((p: any) => ({ ...ids(p), score: ovrOf(p) })),
							PRESEASON_AWARD_POWER.roy,
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
		if (__SPORT === "basketball") {
			const earlyWeight = 1 - seasonProgress;
			const AWARD_OVR_COEF = 1 / 40;
			try {
				// The league's own MVP, DPOY and ROY formulas (core/awards) score a
				// deep field, projected to a full season the same way the race
				// odds are; the early-season ovr blend sits on top.
				const races = (
					await getAwardCandidates(season, undefined, {
						numPlayersPerIndividualAward: 40,
						transformPlayers: projectPlayers({
							numGames: g.get("numGames"),
						}),
					})
				).awardCandidates
					.flat()
					.filter(
						(race) => race.numTeams === undefined && race.group === undefined,
					);
				const field = (
					key: "mvp" | "dpoy" | "roy",
					amount: number,
				): TierCandidate[] =>
					(races.find((race) => legacyAwardKey(race) === key)?.players ?? [])
						.slice(0, amount)
						.map((p) => ({
							pid: p.pid,
							name: playerName(p),
							tid: p.currentStats.tid,
							abbrev: p.currentStats.abbrev,
							awardScore: projectedScore(
								() => p.currentStats.score ?? 0,
								earlyWeight,
								AWARD_OVR_COEF,
							)({ ratings: { ovr: playerTalent(p) } }),
						}));

				const mvpField = field("mvp", 40);
				allLeague = buildTierBoard(
					mvpField,
					tierSizesFor("ALL"),
					seed + 101,
					tierNoiseFactor,
				);

				const dpoyField = field("dpoy", 40);
				allDefensive = buildTierBoard(
					dpoyField,
					tierSizesFor("DEF"),
					seed + 202,
					tierNoiseFactor,
				);

				const rookieSpots = allRookieSpots();
				allRookie =
					rookieSpots === 0
						? []
						: (buildTierBoard(
								field("roy", rookieSpots + 15),
								[rookieSpots],
								seed + 303,
								tierNoiseFactor,
							)[0]?.candidates ?? []);
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
	const allStarsDecided = (await idb.merged.allStars.get(season)) !== undefined;
	// (Never offered in a league with no All-Star game: no roster is picked.)
	if (
		!allStarsDecided &&
		__SPORT === "basketball" &&
		g.get("allStarGame") !== null
	) {
		try {
			// Same pool as the real selection (worker/core/allStar/create.ts) -
			// includes free agents.
			const pool = await getProjectionPool();
			allStar = allStarBoard({
				pool,
				schedule,
				numGames,
				allStarNum: g.get("allStarNum"),
				byConf:
					g.get("allStarType") === "byConf" && confs.length === 2
						? new Map(activeTeams.map((t) => [t.tid, t.cid]))
						: undefined,
				seed: seed + 404,
			});
		} catch (error) {
			console.error("Sportsbook All-Star odds unavailable", error);
		}
	}

	// Every futures and award price moves with a game's result.
	const liveHold = gate.hidden.size > 0;
	return {
		games,
		championship: liveHold ? [] : championship,
		conferences: liveHold ? [] : conferences,
		divisions: liveHold ? [] : divisions,
		winTotals: liveHold ? [] : winTotals,
		awards: liveHold ? [] : awards,
		allLeague: liveHold ? [] : allLeague,
		allDefensive: liveHold ? [] : allDefensive,
		allRookie: liveHold ? [] : allRookie,
		allStar: liveHold ? [] : allStar,
		liveHold,
		// How many players make the All-Star team (both rosters), so the UI can
		// block parlaying more "makes the All-Star team" legs than fit.
		allStarRosterSize: g.get("allStarNum") * 2,
		// The same for the All-Rookie team(s).
		allRookieSpots: allRookieSpots(),
	};
};
