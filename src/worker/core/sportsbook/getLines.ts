import { g } from "../../util/index.ts";
import { idb } from "../../db/index.ts";
import teamOvr from "../team/ovr.ts";
import getSchedule from "../season/getSchedule.ts";
import getAwardCandidates from "../season/getAwardCandidates.ts";
import { getPlayers, getTopPlayers } from "../season/awards.ts";
import {
	dpoyScore,
	mvpScore,
	royFilter,
	royScore,
} from "../season/doAwards.basketball.ts";
import { getGameSpread } from "../../../common/getGameSpread.ts";
import { PHASE, PLAYER, RATINGS } from "../../../common/constants.ts";
import { isSport, bySport } from "../../../common/sportFunctions.ts";
import { probToAmerican } from "../../../common/sportsbook.ts";
import {
	expectedGameTotal,
	marginToWinProb,
	overProb,
	strengthProbs,
	tierMembershipProbs,
	toHalfPointLine,
} from "../../../common/sportsbookOdds.ts";
import { simulateFutures } from "../../../common/sportsbookFutures.ts";

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

// How sharply award odds follow the formula's score gaps.
const AWARD_POWER = 0.9;

// Cap how many upcoming games get a line at once, so the board stays readable.
const MAX_GAME_LINES = 24;

const priceOdds = (prob: number) => probToAmerican(prob);

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
			attrs: ["tid", "cid", "did", "abbrev", "region", "name", "disabled"],
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

		const margin = getGameSpread({
			ovr0: ovrByTid.get(home.tid),
			ovr1: ovrByTid.get(away.tid),
			homeCourtAdvantage,
			neutralSite: false,
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
		const spreadLine = toHalfPointLine(Math.abs(margin)) * (margin >= 0 ? -1 : 1);

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

	const sim = simulateFutures({
		teams: futuresTeams,
		numGamesPlayoffSeries: g.get("numGamesPlayoffSeries"),
		iterations: 4000,
		seed,
	});

	const teamRow = (t: (typeof activeTeams)[number], prob: number) => ({
		tid: t.tid,
		abbrev: t.abbrev,
		region: t.region,
		name: t.name,
		americanOdds: priceOdds(prob),
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

	const championship = playoffsOver
		? []
		: activeTeams
				.map((t) => teamRow(t, sim.titleProb.get(t.tid) ?? 0))
				.sort((a, b) => a.americanOdds - b.americanOdds);

	const conferences = playoffsOver
		? []
		: confs.map((conf) => ({
				cid: conf.cid,
				name: conf.name,
				teams: activeTeams
					.filter((t) => t.cid === conf.cid)
					.map((t) => teamRow(t, sim.confProb.get(t.tid) ?? 0))
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
	// The instant this season's awards are actually decided (idb.cache.awards
	// exists for it), the true winners/teams are public knowledge - the season
	// NUMBER doesn't roll over until next preseason, so without this check the
	// whole awards section (and the team-award/All-Star sections below) would
	// stay bettable on an already-known result all the way through the draft
	// lottery, draft, and free agency. Same principle as win totals/division
	// odds closing once the regular season ends.
	const awardsDecided =
		(await idb.getCopy.awards({ season }, "noCopyCache")) !== undefined;

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

	if (!awardsDecided) {
		// The award scorers read season stats and can throw in a league with none
		// (e.g. one started directly in the playoffs). A failed awards section
		// must never take down the whole board - it just isn't offered.
		let awardCandidatesRaw: Awaited<ReturnType<typeof getAwardCandidates>> =
			[];
		try {
			awardCandidatesRaw = await getAwardCandidates(season);
		} catch (error) {
			console.error("Sportsbook award odds unavailable", error);
		}
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
		awards = awardCandidatesRaw
			.map((race) => {
				const key = awardKeyByName[race.name];
				if (!key) {
					return undefined;
				}
				// Price strictly off the award formula's own scores (a tempered
				// softmax over the exact scores BBGM uses to pick the winner), so a
				// runaway favorite is short and a tight race is bunched. Softmax
				// over the whole field (then show the top 8) so these match the
				// Award Races page odds.
				const probs = strengthProbs(
					race.players.map((p: any) =>
						typeof p.awardScore === "number" ? p.awardScore : 0,
					),
					AWARD_POWER,
				);
				const players = race.players.slice(0, 20);
				return {
					award: key,
					name: race.name,
					candidates: players.map((p: any, i: number) => ({
						pid: p.pid,
						name: p.name,
						tid: p.tid,
						abbrev: p.abbrev,
						americanOdds: priceOdds(probs[i]!),
					})),
				};
			})
			.filter((x) => x !== undefined);

		// --- Team-award futures: All-League / All-Defensive / All-Rookie ----
		// All-Defensive is basketball-only (see common/types.basketball.ts), and
		// the All-Star score formula below is only verified for basketball, so
		// this whole section is basketball-only rather than guess a formula for
		// other sports. Reuses the EXACT same ranking formulas
		// (mvpScore/dpoyScore/royScore) the game itself uses to pick these teams
		// (see worker/core/season/doAwards.basketball.ts's makeTeams/allRookie),
		// so the odds are priced consistently with how the outcome will actually
		// be decided - not a separately-invented model that could disagree.
		if (isSport("basketball")) {
			try {
				const players = await getPlayers(season);

				const mvpField = getTopPlayers(
					{ amount: 40, score: mvpScore },
					players,
				) as unknown as TierCandidate[];
				allLeague = buildTierBoard(
					mvpField,
					TEAM_AWARD_TIER_SIZES,
					seed + 101,
				);

				const dpoyField = getTopPlayers(
					{ amount: 40, score: dpoyScore },
					players,
				) as unknown as TierCandidate[];
				allDefensive = buildTierBoard(
					dpoyField,
					TEAM_AWARD_TIER_SIZES,
					seed + 202,
				);

				const royField = getTopPlayers(
					{ amount: 20, filter: royFilter, score: royScore },
					players,
				) as unknown as TierCandidate[];
				allRookie =
					buildTierBoard(royField, [5], seed + 303)[0]?.candidates ?? [];
			} catch (error) {
				console.error("Sportsbook team-award odds unavailable", error);
			}
		}
	}

	// --- All-Star Team futures ----------------------------------------------
	// Decided earlier in the season than the year-end awards (whenever the
	// roster is actually selected - see worker/core/allStar/create.ts), so
	// gated on its OWN existence check, independent of awardsDecided above.
	let allStar: AwardCandidateRow[] = [];
	const allStarsDecided =
		(await idb.getCopy.allStars({ season }, "noCopyCache")) !== undefined;
	if (!allStarsDecided && isSport("basketball")) {
		try {
			// Matches the real selection pool in worker/core/allStar/create.ts
			// (includes free agents - a good unsigned player is eligible too).
			const rawPlayers = await idb.cache.players.indexGetAll(
				"playersByTid",
				[PLAYER.FREE_AGENT, Infinity],
			);
			const asPlayers = await idb.getCopies.playersPlus(rawPlayers, {
				attrs: ["pid", "name", "tid", "abbrev", "injury"],
				stats: ["ewa", "ws"],
				season,
				mergeStats: "totOnly",
				showNoStats: true,
			});
			// Must mirror worker/core/allStar/create.ts's basketball score
			// formula exactly, so the odds are priced the same way the real
			// selection will decide it. A player with a game-ending injury is
			// dropped from the field entirely (create.ts still lists them, marked
			// injured, but they don't count toward a roster spot - approximating
			// that here isn't worth the complexity, and they'd be extreme
			// long shots regardless).
			const healthyField = asPlayers
				.filter((p: any) => (p.injury?.gamesRemaining ?? 0) === 0)
				.map((p: any) => ({
					pid: p.pid,
					name: p.name,
					tid: p.tid,
					abbrev: p.abbrev,
					awardScore: 2.5 * (p.stats?.ewa ?? 0) + (p.stats?.ws ?? 0),
				}));
			const rosterSize = g.get("allStarNum") * 2;
			allStar =
				buildTierBoard(healthyField, [rosterSize], seed + 404)[0]
					?.candidates ?? [];
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
