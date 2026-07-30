import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import teamOvr from "../team/ovr.ts";
import { getActualPlayThroughInjuries } from "../game/loadTeams.ts";
import { getGameSpread } from "../../../common/getGameSpread.ts";
import { PHASE } from "../../../common/constants.ts";
import { bySport } from "../../../common/sportFunctions.ts";
import {
	probToAmerican,
	SPORTSBOOK_MAX_AMERICAN,
} from "../../../common/sportsbook.ts";
import {
	expectedGameTotal,
	marginToWinProb,
	overProb,
	toHalfPointLine,
} from "../../../common/sportsbookOdds.ts";
import {
	blendMargin,
	peekSimMargin,
	rosterFingerprint,
	settingsFingerprint,
	simMarginKey,
	type SimMarginJob,
} from "./simSpreads.ts";

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
// The SPREAD is the closed-form model corrected by the engine itself: where a
// simulated margin for this exact matchup is already cached, it is blended in
// (see simSpreads.ts for why, and for why nothing sims on this path). Where it
// isn't, the game is queued for a background run and priced off the formula in
// the meantime, so the board renders at its old speed either way. The MONEYLINE
// falls out of whatever spread that produces, so the two can never disagree.
//
// The TOTAL stays on season scoring. Measured against the engine it was already
// accurate to about a point and a half, and a sample small enough to be free
// carries four points of noise - simming it would make it worse, not better.

export type GameLine = {
	// Expected home margin. Positive means the home team is favored.
	margin: number;
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
}: {
	activeTeams: PricerTeam[];
	season: number;
	// The day the schedule currently sits on, so a game further out can be priced
	// off rosters healed forward to it.
	todayDay: number;
}) => {
	const homeCourtAdvantage = g.get("homeCourtAdvantage");
	const numPeriods = g.get("numPeriods");
	const quarterLength = g.get("quarterLength");
	const neutralSiteSetting = g.get("neutralSite");
	const phase = g.get("phase");

	const teamByTid = new Map(activeTeams.map((t) => [t.tid, t]));

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
		// ptModifier and rosterOrder are not used by the formula - they're part of
		// the simulated spread's cache key, because the engine reads them.
		attrs: ["injury", "pid", "value", "tid", "ptModifier", "rosterOrder"],
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

	// Keys for the simulated-spread cache, built from the player list already
	// loaded above so asking for one costs nothing. Both the main board and a
	// game's prop page reach the spread through this same function, so they can
	// never key it differently and quote two different lines.
	const settingsKey = settingsFingerprint();
	const rosterKeyCache = new Map<number, string>();
	const rosterKey = (t: PricerTeam) => {
		let key = rosterKeyCache.get(t.tid);
		if (key === undefined) {
			key = rosterFingerprint(
				playersByTid.get(t.tid) ?? [],
				getActualPlayThroughInjuries(t),
			);
			rosterKeyCache.set(t.tid, key);
		}
		return key;
	};

	// Games priced off the formula because nothing was cached for them yet. The
	// caller drains this in the background - see warmSimMargins.
	const pending = new Map<string, SimMarginJob>();

	// Neutral site drops home-court advantage, matching ScoreBox: an upcoming
	// finals/playoff game the settings mark neutral.
	const isNeutralSite = (matchup: Matchup) =>
		(neutralSiteSetting === "finals" && !!matchup.finals) ||
		(neutralSiteSetting === "playoffs" && phase === PHASE.PLAYOFFS);

	return {
		leagueAvgTotal,
		scoringFor,
		scoringAgainst,
		// Every game priced off the formula so far because no simulated margin was
		// cached for it. Hand to warmSimMargins WITHOUT awaiting.
		pendingSims: () => [...pending.values()],
		priceGame: (matchup: Matchup): GameLine | undefined => {
			const home = teamByTid.get(matchup.homeTid);
			const away = teamByTid.get(matchup.awayTid);
			if (!home || !away) {
				return undefined;
			}

			const neutralSite = isNeutralSite(matchup);
			const formulaMargin = getGameSpread({
				ovr0: gameOvr(home, matchup.day),
				ovr1: gameOvr(away, matchup.day),
				homeCourtAdvantage,
				neutralSite,
				numPeriods,
				quarterLength,
			});
			if (formulaMargin === undefined) {
				return undefined;
			}

			// Correct the formula with the engine, where the engine has already
			// spoken for this exact matchup. Otherwise queue it and stand on the
			// formula - pricing never waits on a sim.
			const key = simMarginKey({
				settings: settingsKey,
				homeRoster: rosterKey(home),
				awayRoster: rosterKey(away),
				neutralSite,
				daysInFuture: Math.max(0, matchup.day - todayDay),
			});
			const sim = peekSimMargin(key);
			let margin = formulaMargin;
			if (sim) {
				margin = blendMargin(formulaMargin, sim);
			} else {
				pending.set(key, {
					key,
					homeTid: home.tid,
					awayTid: away.tid,
					neutralSite,
					daysInFuture: Math.max(0, matchup.day - todayDay),
				});
			}

			const pHome = marginToWinProb(margin);
			const expectedTotal = expectedGameTotal({
				homeFor: scoringFor(home),
				homeAgainst: scoringAgainst(home),
				awayFor: scoringFor(away),
				awayAgainst: scoringAgainst(away),
				leagueAvgTotal,
			});
			const totalLine = toHalfPointLine(expectedTotal);
			const pOver = overProb(expectedTotal, totalLine);
			// Home spread: home favored by `margin`, so the line is -margin.
			const spreadLine =
				toHalfPointLine(Math.abs(margin)) * (margin >= 0 ? -1 : 1);

			return {
				margin,
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
