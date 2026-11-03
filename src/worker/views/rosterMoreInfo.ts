import { PHASE } from "../../common/constants.ts";
import { idb } from "../db/index.ts";
import { headToHead, team } from "../core/index.ts";
import { g, helpers } from "../util/index.ts";

// THE TEAM PAGE'S "MORE TEAM INFO", after Basketball Reference's: a season's
// scoring, ratings and attendance, each with where it ranks in the league,
// the preseason odds, and how the playoffs went. Everything here is already
// in the league - this only gathers it for one team and ranks it.

type Ranked = {
	value: number;
	rank: number;
};

type PlayoffResult = {
	result: "won" | "lost" | "leads" | "trails" | "tied";
	round: string;
	won: number;
	lost: number;
	opp: {
		tid: number;
		abbrev: string;
		region: string;
		name: string;
	};
};

export type MoreTeamInfo = {
	numTeams: number;
	gp: number;
	pts?: Ranked;
	oppPts?: Ranked;
	srs?: Ranked;
	pace?: Ranked;
	ortg?: Ranked;
	drtg?: Ranked;
	nrtg?: Ranked;
	expected?: Ranked & { won: number; lost: number };
	att?: Ranked;
	preseasonOdds?: {
		title?: number;
		winTotal?: number;
		// Once the regular season is over
		result?: "over" | "under" | "push";
	};
	playoffs: PlayoffResult[];
};

// Ties share a rank, the way a standings page shows them.
const rankAmong = (
	values: number[],
	value: number,
	higherIsBetter: boolean,
): number =>
	1 +
	values.filter((other) => (higherIsBetter ? other > value : other < value))
		.length;

// SRS, the Simple Rating System: margin of victory, adjusted for how good the
// teams a team actually played were. Every team's rating is its own margin
// plus the average rating of its opponents, game by game, which is solved by
// iterating until it settles, then centred on zero. Each pass moves only half
// way to the new estimate: a straight update can swing back and forth for
// ever on a thin early-season schedule instead of settling.
const getSrs = async (season: number, mov: Map<number, number>) => {
	const opponents = new Map<number, Map<number, number>>();
	await headToHead.iterate(
		{ tid: "all", type: "regularSeason", season },
		(info) => {
			const games = info.won + info.lost + info.tied + info.otl;
			if (games > 0 && mov.has(info.tid) && mov.has(info.tid2)) {
				let row = opponents.get(info.tid);
				if (!row) {
					row = new Map();
					opponents.set(info.tid, row);
				}
				row.set(info.tid2, games);
			}
		},
	);
	if (opponents.size === 0) {
		return;
	}

	let srs = new Map(mov);
	for (let i = 0; i < 2000; i++) {
		const next = new Map<number, number>();
		for (const [tid, margin] of mov) {
			let sum = 0;
			let games = 0;
			for (const [tid2, count] of opponents.get(tid) ?? []) {
				sum += count * (srs.get(tid2) ?? 0);
				games += count;
			}
			const estimate = margin + (games > 0 ? sum / games : 0);
			next.set(tid, (srs.get(tid)! + estimate) / 2);
		}
		const mean =
			[...next.values()].reduce((total, value) => total + value, 0) / next.size;
		let change = 0;
		for (const [tid, value] of next) {
			next.set(tid, value - mean);
			change = Math.max(change, Math.abs(value - mean - srs.get(tid)!));
		}
		srs = next;
		if (change < 1e-9) {
			break;
		}
	}
	return srs;
};

const getPlayoffs = async (
	season: number,
	tid: number,
	teamNames: Map<number, PlayoffResult["opp"]>,
) => {
	const playoffSeries = await idb.merged.playoffSeries.get(season);
	if (!playoffSeries) {
		return [];
	}
	const numRounds = g.get("numGamesPlayoffSeries", season).length;
	const byConf = playoffSeries.byConf ?? false;

	const results: PlayoffResult[] = [];
	for (const [round, matchups] of playoffSeries.series.entries()) {
		for (const { home, away } of matchups) {
			if (!away) {
				continue;
			}
			const [us, them] =
				home.tid === tid ? [home, away] : away.tid === tid ? [away, home] : [];
			if (!us || !them) {
				continue;
			}
			const toWin = helpers.numGamesToWinRound(round, season);
			const result =
				us.won >= toWin
					? "won"
					: them.won >= toWin
						? "lost"
						: us.won > them.won
							? "leads"
							: us.won < them.won
								? "trails"
								: "tied";
			results.push({
				result,
				round: helpers.playoffRoundName(round, numRounds, byConf),
				won: us.won,
				lost: them.won,
				opp: teamNames.get(them.tid) ?? {
					tid: them.tid,
					abbrev: them.abbrev ?? "",
					region: them.region ?? "",
					name: "",
				},
			});
		}
	}
	return results;
};

export const getMoreTeamInfo = async (
	season: number,
	tid: number,
): Promise<MoreTeamInfo | undefined> => {
	if (__SPORT !== "basketball") {
		return;
	}

	const teams = await team.getFormattedAll({
		attrs: ["tid"],
		seasonAttrs: ["abbrev", "region", "name", "att"],
		stats: [
			"gp",
			"pts",
			"oppPts",
			"mov",
			"pace",
			"ortg",
			"drtg",
			"nrtg",
			"pw",
			"pl",
		],
		season,
		regularSeason: true,
	});
	const teamNames = new Map(
		teams.map((t) => [
			t.tid,
			{
				tid: t.tid,
				abbrev: t.seasonAttrs.abbrev,
				region: t.seasonAttrs.region,
				name: t.seasonAttrs.name,
			},
		]),
	);
	// Read directly rather than through the stats above, which have no row for
	// a team until it has played - and the preseason odds are wanted most on
	// the day the season starts.
	const teamSeason = await idb.merged.teamSeasons.getByTidSeason(tid, season);
	if (!teamSeason) {
		return;
	}

	const played = teams.filter((t) => t.stats.gp > 0);
	const us = played.find((t) => t.tid === tid);
	const gp = us?.stats.gp ?? 0;
	const ranked = (
		pick: (t: (typeof played)[number]) => number,
		higherIsBetter: boolean,
	): Ranked | undefined => {
		if (!us) {
			return;
		}
		const value = pick(us);
		return {
			value,
			rank: rankAmong(played.map(pick), value, higherIsBetter),
		};
	};

	const info: MoreTeamInfo = {
		numTeams: played.length,
		gp,
		pts: ranked((t) => t.stats.pts, true),
		oppPts: ranked((t) => t.stats.oppPts, false),
		pace: ranked((t) => t.stats.pace, true),
		ortg: ranked((t) => t.stats.ortg, true),
		drtg: ranked((t) => t.stats.drtg, false),
		nrtg: ranked((t) => t.stats.nrtg, true),
		playoffs: await getPlayoffs(season, tid, teamNames),
	};

	if (us) {
		const expected = ranked((t) => t.stats.pw, true)!;
		info.expected = {
			...expected,
			won: Math.round(us.stats.pw),
			lost: Math.round(us.stats.pl),
		};

		const srs = await getSrs(
			season,
			new Map(played.map((t) => [t.tid, t.stats.mov])),
		);
		const ours = srs?.get(tid);
		if (srs && ours !== undefined) {
			info.srs = {
				value: ours,
				rank: rankAmong([...srs.values()], ours, true),
			};
		}

		// Per home game, the way the rest of the game shows it. A season total
		// would count playoff crowds too, pushing playoff teams up the ranking
		if (us.seasonAttrs.att > 0) {
			info.att = ranked((t) => t.seasonAttrs.att, true);
		}
	}

	const odds = teamSeason.preseasonOdds;
	if (odds) {
		const regularSeasonOver =
			season < g.get("season") || g.get("phase") >= PHASE.PLAYOFFS;
		const won = teamSeason.won;
		info.preseasonOdds = {
			...odds,
			result:
				regularSeasonOver && odds.winTotal !== undefined
					? won > odds.winTotal
						? "over"
						: won < odds.winTotal
							? "under"
							: "push"
					: undefined,
		};
	}

	return info;
};
