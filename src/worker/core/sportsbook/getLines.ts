import { g } from "../../util/index.ts";
import { idb } from "../../db/index.ts";
import teamOvr from "../team/ovr.ts";
import getSchedule from "../season/getSchedule.ts";
import getAwardCandidates from "../season/getAwardCandidates.ts";
import { getGameSpread } from "../../../common/getGameSpread.ts";
import { RATINGS } from "../../../common/constants.ts";
import { isSport, bySport } from "../../../common/sportFunctions.ts";
import { probToAmerican } from "../../../common/sportsbook.ts";
import {
	expectedGameTotal,
	marginToWinProb,
	overProb,
	seriesWinProb,
	strengthProbs,
	toHalfPointLine,
	winTotalOverProb,
} from "../../../common/sportsbookOdds.ts";

// How sharply the division market concentrates on the projected-best record.
const POWER_DIV = 1.4;
// How sharply award odds follow the formula's score gaps.
const AWARD_POWER = 0.9;

// Cap how many upcoming games get a line at once, so the board stays readable.
const MAX_GAME_LINES = 24;

const priceOdds = (prob: number) => probToAmerican(prob);

// Team ovr (0-100) for every active team, the strength that drives every
// futures market. Mirrors how the Power Rankings page rates teams.
const getTeamOvrs = async (
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
		},
		"noCopyCache",
	);
	const activeTeams = teams.filter((t) => !t.disabled);
	const teamByTid = new Map(activeTeams.map((t) => [t.tid, t]));

	const ovrByTid = await getTeamOvrs(activeTeams, season);

	// League-average per-game total, for game totals when a team has no data.
	let totalPtsPerGame = 0;
	let teamsWithGames = 0;
	for (const t of activeTeams) {
		if (t.stats.gp > 0) {
			totalPtsPerGame += t.stats.pts;
			teamsWithGames += 1;
		}
	}
	const leagueAvgTotal =
		teamsWithGames > 0
			? (2 * totalPtsPerGame) / teamsWithGames
			: bySport({ basketball: 220, football: 45, baseball: 9, hockey: 6 });

	// teamsPlus returns PER-GAME stats by default, so pts/oppPts are already the
	// per-game averages - don't divide by gp again.
	const scoringFor = (t: (typeof activeTeams)[number]) =>
		t.stats.gp > 0 ? t.stats.pts : undefined;
	const scoringAgainst = (t: (typeof activeTeams)[number]) =>
		t.stats.gp > 0 ? t.stats.oppPts : undefined;

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
			home: { tid: home.tid, abbrev: home.abbrev, region: home.region, name: home.name },
			away: { tid: away.tid, abbrev: away.abbrev, region: away.region, name: away.name },
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

	// --- Futures ----------------------------------------------------------
	// A single consistent model so the markets can't contradict each other (a
	// team can't be likelier to win the title than its own conference):
	//   1. Each team's strength → a rating in "points better than average".
	//   2. Conference winner: probability of winning the (rounds-1) series it
	//      takes to reach the finals, normalized within each conference.
	//   3. Champion = P(win conference) × P(win the finals vs the other
	//      conference's expected representative). Since the finals factor is ≤ 1,
	//      champion odds are always longer than conference odds. ✓
	const meanOvr =
		activeTeams.reduce((s, t) => s + (ovrByTid.get(t.tid) ?? 50), 0) /
		Math.max(1, activeTeams.length);
	// A team's strength as a point margin vs an average team, blending its RATING
	// (ovr gap × 0.6, the Power Rankings scaling) with its actual season
	// PERFORMANCE (real point differential). Rating is weighted heavily; the
	// performance share grows with games played and tops out at ~0.45, so two
	// equally-rated teams with different records aren't priced identically.
	const ratingOf = (tid: number) => {
		const estMOV = ((ovrByTid.get(tid) ?? 50) - meanOvr) * 0.6;
		const t = teamByTid.get(tid);
		const gp = t?.stats.gp ?? 0;
		if (!t || gp <= 0) {
			return estMOV;
		}
		const actualMOV = t.stats.pts - t.stats.oppPts; // per-game differential
		const perfWeight = 0.45 * Math.min(1, gp / 15);
		return estMOV * (1 - perfWeight) + actualMOV * perfWeight;
	};

	const rounds = g.get("numGamesPlayoffSeries").length;
	const roundsToConf = Math.max(1, rounds - 1);

	// Series-win probability vs an average team, then vs a specific rating.
	const sSeriesVsAvg = (tid: number) => seriesWinProb(marginToWinProb(ratingOf(tid)));
	const sSeriesVs = (tid: number, oppRating: number) =>
		seriesWinProb(marginToWinProb(ratingOf(tid) - oppRating));

	// Conference winner probabilities (each conference sums to 1).
	const confWinProb = new Map<number, number>();
	for (const conf of confs) {
		const list = activeTeams.filter((t) => t.cid === conf.cid);
		const raw = list.map((t) => sSeriesVsAvg(t.tid) ** roundsToConf);
		const sum = raw.reduce((a, b) => a + b, 0) || 1;
		list.forEach((t, i) => confWinProb.set(t.tid, raw[i]! / sum));
	}

	// Each conference's expected finalist rating (for the finals matchup).
	const confRepRating = new Map<number, number>();
	for (const conf of confs) {
		const list = activeTeams.filter((t) => t.cid === conf.cid);
		confRepRating.set(
			conf.cid,
			list.reduce((s, t) => s + (confWinProb.get(t.tid) ?? 0) * ratingOf(t.tid), 0),
		);
	}
	const meanRepRating =
		confs.length > 0
			? [...confRepRating.values()].reduce((a, b) => a + b, 0) / confs.length
			: 0;

	// Champion probability = conference win × finals win vs the OTHER conference's
	// representative (or the average representative when not a 2-conference league).
	const champWinProb = new Map<number, number>();
	for (const t of activeTeams) {
		const pConf = confWinProb.get(t.tid) ?? 0;
		let oppRating: number;
		if (confs.length === 2) {
			const otherCid = confs.find((c) => c.cid !== t.cid)?.cid;
			oppRating = otherCid !== undefined ? (confRepRating.get(otherCid) ?? 0) : meanRepRating;
		} else {
			oppRating = meanRepRating;
		}
		champWinProb.set(t.tid, pConf * sSeriesVs(t.tid, oppRating));
	}

	const teamRow = (t: (typeof activeTeams)[number], prob: number) => ({
		tid: t.tid,
		abbrev: t.abbrev,
		region: t.region,
		name: t.name,
		americanOdds: priceOdds(prob),
	});

	const championship = activeTeams
		.map((t) => teamRow(t, champWinProb.get(t.tid) ?? 0))
		.sort((a, b) => a.americanOdds - b.americanOdds);

	const conferences = confs.map((conf) => ({
		cid: conf.cid,
		name: conf.name,
		teams: activeTeams
			.filter((t) => t.cid === conf.cid)
			.map((t) => teamRow(t, confWinProb.get(t.tid) ?? 0))
			.sort((a, b) => a.americanOdds - b.americanOdds),
	}));

	// --- Win totals (over/under projected final wins) ---------------------
	const projectedWinsByTid = new Map<number, number>();
	const winTotals = activeTeams
		.map((t) => {
			const gp =
				t.seasonAttrs.won +
				t.seasonAttrs.lost +
				(t.seasonAttrs.tied ?? 0) +
				(t.seasonAttrs.otl ?? 0);
			const gamesRemaining = Math.max(0, numGames - gp);
			const winProb = marginToWinProb(ratingOf(t.tid));
			const projectedWins = t.seasonAttrs.won + gamesRemaining * winProb;
			projectedWinsByTid.set(t.tid, projectedWins);
			const line = toHalfPointLine(projectedWins);
			const pOver = winTotalOverProb({
				projectedWins,
				line,
				gamesTotal: numGames,
				winProb,
			});
			return {
				tid: t.tid,
				abbrev: t.abbrev,
				region: t.region,
				name: t.name,
				line,
				over: priceOdds(pOver),
				under: priceOdds(1 - pOver),
			};
		})
		.sort((a, b) => b.line - a.line);

	// Division winner: about the best regular-season record, so price it off
	// projected wins (softmax within each division), independent of the playoffs.
	const divisions = divs.map((div) => {
		const list = activeTeams.filter((t) => t.did === div.did);
		const probs = strengthProbs(
			list.map((t) => projectedWinsByTid.get(t.tid) ?? 0),
			POWER_DIV,
		);
		return {
			did: div.did,
			name: div.name,
			teams: list
				.map((t, i) => teamRow(t, probs[i] ?? 0))
				.sort((a, b) => a.americanOdds - b.americanOdds),
		};
	});

	// --- Awards (by current award-race position) --------------------------
	const awardCandidatesRaw = await getAwardCandidates(season);
	const awardKeyByName: Record<string, "mvp" | "dpoy" | "roy" | "smoy" | "mip"> =
		{
			"Most Valuable Player": "mvp",
			"Defensive Player of the Year": "dpoy",
			"Rookie of the Year": "roy",
			"Sixth Man of the Year": "smoy",
			"Most Improved Player": "mip",
		};
	const awards = awardCandidatesRaw
		.map((race) => {
			const key = awardKeyByName[race.name];
			if (!key) {
				return undefined;
			}
			// Price strictly off the award formula's own scores (a tempered softmax
			// over the exact scores BBGM uses to pick the winner), so a runaway
			// favorite is short and a tight race is bunched. Softmax over the whole
			// field (then show the top 8) so these match the Award Races page odds.
			const probs = strengthProbs(
				race.players.map((p: any) =>
					typeof p.awardScore === "number" ? p.awardScore : 0,
				),
				AWARD_POWER,
			);
			const players = race.players.slice(0, 8);
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

	return {
		games,
		championship,
		conferences,
		divisions,
		winTotals,
		awards,
	};
};
