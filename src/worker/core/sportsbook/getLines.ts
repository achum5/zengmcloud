import { g } from "../../util/index.ts";
import { idb } from "../../db/index.ts";
import teamOvr from "../team/ovr.ts";
import getSchedule from "../season/getSchedule.ts";
import getAwardRaceOdds from "../season/getAwardRaceOdds.ts";
import { getPlayers, getTopPlayers } from "../season/awards.ts";
import {
	dpoyScore,
	mvpScore,
	royFilter,
	royScore,
} from "../season/doAwards.basketball.ts";
import { getGameSpread } from "../../../common/getGameSpread.ts";
import { getActualPlayThroughInjuries } from "../game/loadTeams.ts";
import { PHASE, PLAYER, RATINGS } from "../../../common/constants.ts";
import { isSport, bySport } from "../../../common/sportFunctions.ts";
import { probToAmerican } from "../../../common/sportsbook.ts";
import {
	expectedGameTotal,
	marginToWinProb,
	overProb,
	tierMembershipProbs,
	toHalfPointLine,
} from "../../../common/sportsbookOdds.ts";
import {
	simulateFutures,
	simulatePlayoffBracket,
	type BracketMatchup,
} from "../../../common/sportsbookFutures.ts";

// The 3 tiers an All-League/All-Defensive Team splits into, 5 players each -
// matches worker/core/season/doAwards.basketball.ts's makeTeams().
const TEAM_AWARD_TIER_SIZES = [5, 5, 5];
const TEAM_AWARD_TIER_TITLES = ["First Team", "Second Team", "Third Team"];

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
	const scores = field.map((p) => p.awardScore ?? 0);
	const probs = tierMembershipProbs(scores, tierSizes, {
		iterations: 3000,
		seed,
		noiseFactor,
	});
	return tierSizes.map((_, tierIdx) => ({
		tier: tierIdx + 1,
		title: titles[tierIdx] ?? `Tier ${tierIdx + 1}`,
		candidates: field
			.map((p, i) => ({
				pid: p.pid,
				name: p.name,
				tid: p.tid,
				abbrev: p.abbrev,
				americanOdds: priceOdds(probs[i]![tierIdx]!),
			}))
			.sort((a, b) => a.americanOdds - b.americanOdds)
			.slice(0, TEAM_AWARD_BOARD_SIZE),
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

// Cap how many upcoming games get a line at once, so the board stays readable.
const MAX_GAME_LINES = 24;

const priceOdds = (prob: number) => probToAmerican(prob);

// A player's overall rating, from either an array of season ratings (getPlayers)
// or a single season's ratings object (playersPlus with a fixed season).
const ovrOf = (p: any): number => {
	const r = Array.isArray(p.ratings) ? p.ratings.at(-1) : p.ratings;
	return typeof r?.ovr === "number" ? r.ovr : 0;
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
	const homeCourtAdvantage = g.get("homeCourtAdvantage");
	const numPeriods = g.get("numPeriods");
	const quarterLength = g.get("quarterLength");
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

	const ovrByTid = await getTeamOvrs(activeTeams, season);

	// League-average per-game total, for game totals when a team has no data.
	// t.stats can be missing entirely in a league with no games played (e.g.
	// started directly in the playoffs), so never assume it exists.
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
	const scoringFor = (t: (typeof activeTeams)[number]) => {
		const s = statsOf(t);
		return s.gp > 0 ? regress(s.pts, s.gp) : undefined;
	};
	const scoringAgainst = (t: (typeof activeTeams)[number]) => {
		const s = statsOf(t);
		return s.gp > 0 ? regress(s.oppPts, s.gp) : undefined;
	};

	// --- Game lines -------------------------------------------------------
	const schedule = await getSchedule();

	// Price each game off the SAME team overall the Schedule/ScoreBox page shows
	// next to it, so the sportsbook's spread and moneyline never diverge from the
	// line displayed there. That means fuzzed ratings (fuzz: true), injuries
	// healed forward to the game's day, the team's playThroughInjuries setting,
	// and the playoff flag - not the flat, fuzz-free team ovr used for futures.
	const todayDay = schedule[0]?.day ?? 0;
	const linePlayersRaw = await idb.cache.players.indexGetAll("playersByTid", [
		0, // Active players have tid >= 0
		Infinity,
	]);
	const linePlayers = await idb.getCopies.playersPlus(linePlayersRaw, {
		attrs: ["injury", "pid", "value", "tid"],
		ratings: ["ovr", "pos", "ovrs"],
		season,
		fuzz: true,
	});
	const linePlayersByTid = Map.groupBy(linePlayers, (p) => p.tid);

	const linePlayoffSeries = await idb.cache.playoffSeries.get(season);
	const lineRoundSeries = linePlayoffSeries
		? linePlayoffSeries.currentRound === -1 && linePlayoffSeries.playIns
			? linePlayoffSeries.playIns.flat()
			: linePlayoffSeries.series[linePlayoffSeries.currentRound]
		: undefined;

	const neutralSiteSetting = g.get("neutralSite");
	const phase = g.get("phase");

	// Team overall on a given day, mirroring worker/views/schedule.ts's getTeam.
	const gameOvr = (
		t: (typeof activeTeams)[number],
		day: number,
	): number | undefined =>
		teamOvr(linePlayersByTid.get(t.tid) ?? [], {
			accountForInjuredPlayers: {
				numDaysInFuture: day - todayDay,
				playThroughInjuries: getActualPlayThroughInjuries(t),
			},
			playoffs: !!lineRoundSeries,
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

		// Neutral site drops home-court advantage, matching ScoreBox: an upcoming
		// finals/playoff game the settings mark neutral.
		const neutralSiteResolved =
			(neutralSiteSetting === "finals" && !!matchup.finals) ||
			(neutralSiteSetting === "playoffs" && phase === PHASE.PLAYOFFS);

		const margin = getGameSpread({
			ovr0: gameOvr(home, matchup.day),
			ovr1: gameOvr(away, matchup.day),
			homeCourtAdvantage,
			neutralSite: neutralSiteResolved,
			numPeriods,
			quarterLength,
		});
		if (margin === undefined) {
			continue;
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

		games.push({
			gid: matchup.gid,
			home: {
				tid: home.tid,
				abbrev: home.abbrev,
				region: home.region,
				name: home.name,
				won: home.seasonAttrs.won,
				lost: home.seasonAttrs.lost,
			},
			away: {
				tid: away.tid,
				abbrev: away.abbrev,
				region: away.region,
				name: away.name,
				won: away.seasonAttrs.won,
				lost: away.seasonAttrs.lost,
			},
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
		});
		if (games.length >= MAX_GAME_LINES) {
			break;
		}
	}

	// --- Futures: Monte Carlo of the season + playoffs ---------------------
	// One simulation drives EVERY futures market (division, conference, title,
	// win totals), so they can never contradict each other, and a dominant team
	// prices like one because it actually plays through the bracket. See
	// common/sportsbookFutures.ts.
	const meanOvr =
		activeTeams.reduce((s, t) => s + (ovrByTid.get(t.tid) ?? 50), 0) /
		Math.max(1, activeTeams.length);
	// A team's strength as a point margin vs an average team, blending its RATING
	// (ovr gap × 0.6, the Power Rankings scaling) with its actual season
	// PERFORMANCE (real point differential). The performance share grows with
	// games played, so a 46-3 team is priced off what it has actually done.
	const ratingOf = (tid: number) => {
		const estMOV = ((ovrByTid.get(tid) ?? 50) - meanOvr) * 0.6;
		const s = statsOf(teamByTid.get(tid));
		if (s.gp <= 0) {
			return estMOV;
		}
		const actualMOV = s.pts - s.oppPts; // per-game differential
		// Trust what the team has actually done more and more as the sample grows:
		// by ~30 games the real point differential carries 3/4 of the weight.
		const perfWeight = 0.75 * Math.min(1, s.gp / 30);
		return estMOV * (1 - perfWeight) + actualMOV * perfWeight;
	};

	const futuresTeams = activeTeams.map((t) => {
		const gp =
			t.seasonAttrs.won +
			t.seasonAttrs.lost +
			(t.seasonAttrs.tied ?? 0) +
			(t.seasonAttrs.otl ?? 0);
		return {
			tid: t.tid,
			cid: t.cid,
			did: t.did,
			won: t.seasonAttrs.won,
			gamesRemaining: Math.max(0, numGames - gp),
			rating: ratingOf(t.tid),
		};
	});

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

	const sim = simulateFutures({
		teams: futuresTeams,
		numGamesPlayoffSeries: g.get("numGamesPlayoffSeries"),
		iterations: 4000,
		seed,
	});

	// Regular-season markets (division winners, win totals) are DECIDED once the
	// playoffs start - a real book closes them, so we don't offer them at all.
	const regularSeasonOver = g.get("phase") >= PHASE.PLAYOFFS;
	// Likewise, the champion/conference winner are decided the moment the
	// playoffs actually finish. The season NUMBER doesn't roll over until next
	// preseason, so without this a champion already crowned weeks ago would
	// stay bettable (on a publicly-known outcome) all the way through the
	// draft lottery, draft, and free agency.
	const playoffsOver = g.get("phase") > PHASE.PLAYOFFS;

	// During the playoffs, price the title/conference markets from the ACTUAL
	// bracket - who's alive and each series' current score - instead of the
	// hypothetical seeded-by-record bracket above, which knows nothing about the
	// real playoffs (it kept eliminated teams as favorites and gave a team up
	// 3-0 no credit). Teams no longer in the bracket are delisted, like a real
	// book. Falls back to the hypothetical sim if the bracket can't be read.
	let bracketSim: ReturnType<typeof simulatePlayoffBracket> | undefined;
	if (regularSeasonOver && !playoffsOver) {
		try {
			const playoffSeries = await idb.cache.playoffSeries.get(season);
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
				bracketSim = simulatePlayoffBracket({
					matchups,
					startRound: rnd,
					numGamesPlayoffSeries: g.get("numGamesPlayoffSeries"),
					ratings,
					iterations: 4000,
					seed: bracketSeed % 2147483647,
				});
			}
		} catch (error) {
			console.error("Sportsbook playoff bracket odds unavailable", error);
		}
	}

	const teamRow = (t: (typeof activeTeams)[number], prob: number) => ({
		tid: t.tid,
		abbrev: t.abbrev,
		region: t.region,
		name: t.name,
		americanOdds: priceOdds(prob),
	});

	const championship = playoffsOver
		? []
		: activeTeams
				.filter((t) => !bracketSim || bracketSim.titleProb.has(t.tid))
				.map((t) =>
					teamRow(
						t,
						(bracketSim ? bracketSim.titleProb : sim.titleProb).get(t.tid) ?? 0,
					),
				)
				.sort((a, b) => a.americanOdds - b.americanOdds);

	const conferences = playoffsOver
		? []
		: confs.map((conf) => ({
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
							(bracketSim ? bracketSim.confProb : sim.confProb).get(t.tid) ?? 0,
						),
					)
					.sort((a, b) => a.americanOdds - b.americanOdds),
			}));

	const divisions = regularSeasonOver
		? []
		: divs.map((div) => ({
				did: div.did,
				name: div.name,
				teams: activeTeams
					.filter((t) => t.did === div.did)
					.map((t) => teamRow(t, sim.divProb.get(t.tid) ?? 0))
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
					return {
						tid: t.tid,
						abbrev: t.abbrev,
						region: t.region,
						name: t.name,
						line: wt.line,
						over: priceOdds(wt.pOver),
						under: priceOdds(1 - wt.pOver),
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
						candidates: race.players.slice(0, 20).map((p: any) => ({
							pid: p.pid,
							name: p.name,
							tid: p.tid,
							abbrev: p.abbrev,
							americanOdds: p.odds,
						})),
					};
				})
				.filter((x) => x !== undefined);
		} catch (error) {
			console.error("Sportsbook award odds unavailable", error);
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
			// Matches the real selection pool in worker/core/allStar/create.ts
			// (includes free agents - a good unsigned player is eligible too).
			const rawPlayers = await idb.cache.players.indexGetAll("playersByTid", [
				PLAYER.FREE_AGENT,
				Infinity,
			]);
			const asPlayers = await idb.getCopies.playersPlus(rawPlayers, {
				attrs: ["pid", "name", "tid", "abbrev", "injury"],
				ratings: ["ovr"],
				stats: ["ewa", "ws"],
				season,
				mergeStats: "totOnly",
				showNoStats: true,
			});
			// Mirrors worker/core/allStar/create.ts's basketball score formula, so
			// the odds are priced the way the real selection decides it - blended
			// with an overall projection early (the ewa/ws formula is ~0 before
			// games, so without this the board would be flat noise). ewa+ws runs
			// bigger than the year-end formulas, so a larger overall coefficient
			// keeps the projection meaningful for the first stretch of games. A
			// player with a game-ending injury is dropped from the field entirely
			// (create.ts lists them marked injured, but they don't count toward a
			// roster spot, and they'd be extreme long shots regardless).
			const allStarEarly = 1 - seasonProgress;
			const healthyField = asPlayers
				.filter((p: any) => (p.injury?.gamesRemaining ?? 0) === 0)
				.map((p: any) => ({
					pid: p.pid,
					name: p.name,
					tid: p.tid,
					abbrev: p.abbrev,
					awardScore:
						2.5 * (p.stats?.ewa ?? 0) +
						(p.stats?.ws ?? 0) +
						allStarEarly * (ovrOf(p) / 8),
				}));
			const rosterSize = g.get("allStarNum") * 2;
			allStar =
				buildTierBoard(
					healthyField,
					[rosterSize],
					seed + 404,
					tierNoiseFactor,
				)[0]?.candidates ?? [];
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
