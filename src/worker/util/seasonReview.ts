import { PLAYER } from "../../common/constants.ts";
import type { Player } from "../../common/types.ts";
import { idb } from "../db/index.ts";
import { player } from "../core/index.ts";
import { g } from "./index.ts";
import {
	GamesPlayedCache,
	getCategoriesAndStats,
	iterateAllPlayersWithStats,
	playerMeetsCategoryRequirements,
} from "../views/leaders.ts";
import { roundLabel } from "../views/playoffSeries.ts";

// THE SEASON IN REVIEW: a finished season told a slide at a time - the
// champions, the awards, the league leaders, the user's own season and the
// players who retired. Everything on it is the league's own record.

export type ReviewTeam = {
	tid: number;
	abbrev: string;
	region: string;
	name: string;
	imgURL: string | undefined;
	colors: [string, string, string];
	won: number;
	lost: number;
	tied: number;
	otl: number;
};

export type ReviewStat = { label: string; value: string };

export type ReviewPlayer = {
	pid: number;
	name: string;
	tid: number;
	abbrev: string;
	// The team's colors that season, for the slide behind him.
	colors: [string, string, string] | undefined;
	stats: ReviewStat[];
};

export type SeasonReview = {
	season: number;
	champion?: ReviewTeam & {
		// The finals, from the champion's side.
		finals?: { opp: ReviewTeam; won: number; lost: number };
	};
	awards: {
		name: string;
		// The MVP and the playoffs' own MVP get a slide each.
		big: boolean;
		note: string;
		player: ReviewPlayer;
	}[];
	leaders: { label: string; value: string; player: ReviewPlayer }[];
	user?: ReviewTeam & {
		result: string;
		best?: ReviewPlayer & { ws: number };
	};
	retired: (ReviewPlayer & { from: number; to: number; ws: number })[];
};

const ws = (rows: Player["stats"], tid?: number) =>
	rows.reduce(
		(sum, row) =>
			!row.playoffs && (tid === undefined || row.tid === tid)
				? sum + (row.ows ?? 0) + (row.dws ?? 0)
				: sum,
		0,
	);

export const getSeasonReview = async (
	season: number,
): Promise<SeasonReview | undefined> => {
	if (__SPORT !== "basketball") {
		return;
	}
	const awards = await idb.merged.awards.get(season);
	if (!awards) {
		return;
	}

	const teamSeasons = await idb.merged.teamSeasons.getAll({ season });
	const reviewTeam = (tid: number): ReviewTeam | undefined => {
		const ts = teamSeasons.find((row) => row.tid === tid);
		if (!ts) {
			return;
		}
		return {
			tid,
			abbrev: ts.abbrev ?? g.get("teamInfoCache")[tid]?.abbrev ?? "",
			region: ts.region ?? g.get("teamInfoCache")[tid]?.region ?? "",
			name: ts.name ?? g.get("teamInfoCache")[tid]?.name ?? "",
			imgURL: ts.imgURL ?? g.get("teamInfoCache")[tid]?.imgURL,
			colors: ts.colors,
			won: ts.won,
			lost: ts.lost,
			tied: ts.tied,
			otl: ts.otl,
		};
	};
	const abbrevOf = (tid: number) => reviewTeam(tid)?.abbrev ?? "";

	const numRounds = g.get("numGamesPlayoffSeries", season).length;
	const playoffSeries = await idb.merged.playoffSeries.get(season);

	// The champions, and who they beat for it.
	let champion: SeasonReview["champion"];
	const champTs = teamSeasons.find((ts) => ts.playoffRoundsWon === numRounds);
	if (champTs) {
		const team = reviewTeam(champTs.tid)!;
		const matchup = playoffSeries?.series.at(-1)?.[0];
		let finals;
		if (matchup?.away) {
			const mine = matchup.home.tid === team.tid ? matchup.home : matchup.away;
			const theirs = mine === matchup.home ? matchup.away : matchup.home;
			const opp = reviewTeam(theirs.tid);
			if (opp) {
				finals = { opp, won: mine.won, lost: theirs.won };
			}
		}
		champion = { ...team, finals };
	}

	const line = async (
		pid: number,
		tid: number,
		seasonType: "regularSeason" | "playoffs",
	): Promise<ReviewPlayer | undefined> => {
		const p = await idb.merged.players.get(pid);
		if (!p) {
			return;
		}
		const p2 = await player.format(p, {
			attrs: ["name"],
			stats: ["gp", "pts", "trb", "ast"],
			season,
			seasonType,
			mergeStats: "totOnly",
			showNoStats: true,
		});
		if (!p2) {
			return;
		}
		const one = (value: number | undefined) =>
			value === undefined ? "-" : value.toFixed(1);
		return {
			pid,
			name: p2.name,
			tid,
			abbrev: abbrevOf(tid),
			colors: reviewTeam(tid)?.colors,
			stats: [
				{ label: "PTS", value: one(p2.stats.pts) },
				{ label: "REB", value: one(p2.stats.trb) },
				{ label: "AST", value: one(p2.stats.ast) },
			],
		};
	};

	// The individual awards, in the league's own order. Of the series awards,
	// only the finals: its MVP gets a slide, with his line from the series.
	const awardRows: SeasonReview["awards"] = [];
	for (const award of awards.awards) {
		const series = typeof award.statRange === "number";
		if (
			award.numTeams !== undefined ||
			(award.group !== undefined && !series) ||
			(series && award.statRange !== -1)
		) {
			continue;
		}
		const winner = award.winner[0];
		if (!winner || winner.pid === undefined) {
			continue;
		}
		const playoffs = award.statRange !== undefined;
		const p = await line(
			winner.pid,
			winner.tid,
			playoffs ? "playoffs" : "regularSeason",
		);
		if (!p) {
			continue;
		}
		const overrides = series ? winner.statOverrides : undefined;
		if (overrides) {
			p.stats = (["pts", "trb", "ast"] as const).map((stat, i) => ({
				label: p.stats[i]!.label,
				value:
					typeof overrides[stat] === "number"
						? overrides[stat].toFixed(1)
						: p.stats[i]!.value,
			}));
		}
		awardRows.push({
			name: award.name,
			big: playoffs || award.shortName === "MVP",
			note: series
				? "Per game in the finals"
				: playoffs
					? "Per game in the playoffs"
					: "Per game",
			player: p,
		});
	}

	// The league leaders, by the same requirements as the Leaders page.
	const { categories } = getCategoriesAndStats();
	const wanted = [
		{ stat: "pts", label: "Points" },
		{ stat: "trb", label: "Rebounds" },
		{ stat: "ast", label: "Assists" },
	] as const;
	const cats = wanted.flatMap((w) => {
		const cat = categories.find((c) => c.stat === w.stat);
		return cat ? [{ ...w, cat }] : [];
	});
	const gamesPlayedCache = new GamesPlayedCache();
	await gamesPlayedCache.loadSeasons([season], false);
	const formatOptions = {
		attrs: ["pid", "name"],
		// (Covers the requirements too: games and the stat's own total.)
		stats: ["abbrev", "tid", "gp", "min", "pts", "trb", "ast"],
		season,
		seasonType: "regularSeason",
		mergeStats: "totOnly",
		statType: "perGame",
	} as const;
	const abbrevs = await player.loadAbbrevs(formatOptions);
	const format = player.createFormatter(formatOptions, abbrevs);

	const best = cats.map(() => ({ value: -Infinity, pid: -1, tid: -1 }));
	const userTid = g.get("userTid");
	const userBest = { ws: -Infinity, pid: -1 };
	await iterateAllPlayersWithStats(season, (pRaw) => {
		const teamWS = ws(
			pRaw.stats.filter((row) => row.season === season),
			userTid,
		);
		if (
			pRaw.stats.some((row) => row.season === season && row.tid === userTid) &&
			teamWS > userBest.ws
		) {
			userBest.ws = teamWS;
			userBest.pid = pRaw.pid;
		}

		const p = format(pRaw);
		if (!p) {
			return;
		}
		const stats = p.stats as Record<string, any> | undefined;
		if (!stats) {
			return;
		}
		for (const [i, { cat, stat }] of cats.entries()) {
			const value = stats[stat];
			if (typeof value !== "number" || value <= best[i]!.value) {
				continue;
			}
			if (
				playerMeetsCategoryRequirements({
					career: false,
					cat,
					gamesPlayedCache,
					p,
					playerStats: stats,
					seasonType: "regularSeason",
					season,
					statType: "perGame",
				})
			) {
				best[i] = { value, pid: pRaw.pid, tid: stats.tid };
			}
		}
	});

	const leaders: SeasonReview["leaders"] = [];
	for (const [i, { label }] of cats.entries()) {
		const b = best[i]!;
		if (b.pid < 0) {
			continue;
		}
		const p = await line(b.pid, b.tid, "regularSeason");
		if (p) {
			leaders.push({ label, value: b.value.toFixed(1), player: p });
		}
	}

	// The user's season.
	let user: SeasonReview["user"];
	const userTeam = reviewTeam(userTid);
	const userTs = teamSeasons.find((ts) => ts.tid === userTid);
	if (userTeam && userTs) {
		let result = "Missed the playoffs";
		if (userTs.playoffRoundsWon >= numRounds) {
			result = "Won the title";
		} else if (userTs.playoffRoundsWon >= 0 && playoffSeries) {
			const round = userTs.playoffRoundsWon;
			const matchup = playoffSeries.series[round]?.find(
				(m) => m.home.tid === userTid || m.away?.tid === userTid,
			);
			if (matchup?.away) {
				const mine = matchup.home.tid === userTid ? matchup.home : matchup.away;
				const theirs = mine === matchup.home ? matchup.away : matchup.home;
				const label = await roundLabel(
					season,
					round,
					numRounds,
					matchup.home.cid === matchup.away.cid ? matchup.home.cid : undefined,
				);
				result = `Lost to ${abbrevOf(theirs.tid)} in the ${label}, ${theirs.won}-${mine.won}`;
			} else {
				result = "Made the playoffs";
			}
		} else if (userTs.playoffRoundsWon >= 0) {
			result = "Made the playoffs";
		}
		let bestPlayer;
		if (userBest.pid >= 0) {
			const p = await line(userBest.pid, userTid, "regularSeason");
			if (p) {
				bestPlayer = { ...p, ws: userBest.ws };
			}
		}
		user = { ...userTeam, result, best: bestPlayer };
	}

	// Who retired, the biggest careers first.
	const retiredRaw = await idb.merged.players.getAll({ retiredYear: season });
	const retired: SeasonReview["retired"] = [];
	for (const p of retiredRaw
		.filter((p) => p.tid === PLAYER.RETIRED)
		.map((p) => ({ p, ws: ws(p.stats) }))
		.filter(({ ws }) => ws >= 20)
		.sort((a, b) => b.ws - a.ws)
		.slice(0, 3)) {
		const seasons = p.p.stats.filter((row) => !row.playoffs);
		const lastRow = seasons.at(-1);
		const info = await line(p.p.pid, lastRow?.tid ?? -1, "regularSeason");
		if (info) {
			retired.push({
				...info,
				stats: [
					{
						label: "Seasons",
						value: String(new Set(seasons.map((row) => row.season)).size),
					},
					{ label: "WS", value: p.ws.toFixed(1) },
				],
				from: seasons[0]?.season ?? season,
				to: lastRow?.season ?? season,
				ws: p.ws,
			});
		}
	}

	return {
		season,
		champion,
		awards: awardRows,
		leaders,
		user,
		retired,
	};
};
