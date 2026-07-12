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
	awardProbsFromScores,
	expectedGameTotal,
	marginToWinProb,
	overProb,
	strengthProbs,
	toHalfPointLine,
	winTotalOverProb,
} from "../../../common/sportsbookOdds.ts";

// How sharply futures concentrate on the strongest teams (higher = the favorite
// gets shorter odds). Championship needs winning multiple rounds, so it's the
// most top-heavy; division (mostly about record) the least.
const POWER_CHAMPION = 1.7;
const POWER_CONF = 1.4;
const POWER_DIV = 1.1;

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

	const scoringFor = (t: (typeof activeTeams)[number]) =>
		t.stats.gp > 0 ? t.stats.pts / t.stats.gp : undefined;
	const scoringAgainst = (t: (typeof activeTeams)[number]) =>
		t.stats.gp > 0 ? t.stats.oppPts / t.stats.gp : undefined;

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

	// --- Futures: strength → probabilities → prices -----------------------
	const ovrList = activeTeams.map((t) => ovrByTid.get(t.tid) ?? 50);

	const strengthMarket = (
		list: typeof activeTeams,
		power: number,
	): { tid: number; abbrev: string; region: string; name: string; americanOdds: number }[] => {
		const probs = strengthProbs(
			list.map((t) => ovrByTid.get(t.tid) ?? 50),
			power,
		);
		return list
			.map((t, i) => ({
				tid: t.tid,
				abbrev: t.abbrev,
				region: t.region,
				name: t.name,
				americanOdds: priceOdds(probs[i]!),
			}))
			.sort((a, b) => a.americanOdds - b.americanOdds);
	};

	void ovrList;
	const championship = strengthMarket(activeTeams, POWER_CHAMPION);

	const conferences = confs.map((conf) => ({
		cid: conf.cid,
		name: conf.name,
		teams: strengthMarket(
			activeTeams.filter((t) => t.cid === conf.cid),
			POWER_CONF,
		),
	}));

	const divisions = divs.map((div) => ({
		did: div.did,
		name: div.name,
		teams: strengthMarket(
			activeTeams.filter((t) => t.did === div.did),
			POWER_DIV,
		),
	}));

	// --- Win totals (over/under projected final wins) ---------------------
	const winTotals = activeTeams
		.map((t) => {
			const gp = t.seasonAttrs.won + t.seasonAttrs.lost + (t.seasonAttrs.tied ?? 0) + (t.seasonAttrs.otl ?? 0);
			const gamesRemaining = Math.max(0, numGames - gp);
			const estimatedMov = (ovrByTid.get(t.tid) ?? 50) * 0.6 - 30;
			const winProb = marginToWinProb(estimatedMov);
			const projectedWins = t.seasonAttrs.won + gamesRemaining * winProb;
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
			const players = race.players.slice(0, 8);
			// Rank model: descending synthetic scores (we only have the order).
			const probs = awardProbsFromScores(
				players.map((_: any, i: number) => players.length - i),
			);
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
