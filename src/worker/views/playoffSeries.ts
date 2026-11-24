import { idb } from "../db/index.ts";
import { g, helpers } from "../util/index.ts";
import { defineView } from "../util/defineView.ts";
import { validateSeason, validateTeam } from "../util/processInputs.ts";
import type { RouteParams } from "../../ui/router/types.ts";
import { getLiveGate, unplaySeries } from "../util/liveGameGate.ts";
import { season as seasonCore } from "../core/index.ts";
import { betterSeedHome } from "../../common/sportsbookOdds.ts";
import { getAutoRecapsForGids } from "../util/getDayGamesForRecap.ts";
import getWinner from "../../common/getWinner.ts";
import type { Game, PlayoffSeries } from "../../common/types.ts";

// ONE PLAYOFF SERIES (#/playoff_series/<season>/<team>/<opp>): the two teams,
// where it stands, every game with its recap, the games still to come, who
// led it, how the two teams played, the regular season between them that
// year and every other time they met in the playoffs.
//
// Two teams meet at most once in a season's playoffs, so the pair names the
// series, and a box score knows both without knowing its round.
//
// A game being played live hasn't happened yet here, the same as on the
// bracket (liveGameGate.ts).

const processInputs = (params: RouteParams<"playoffSeries">) => {
	const season = validateSeason(params.season);
	const tid = validateTeam(params.team);
	const opp = validateTeam(params.opp);
	return { season, tid, opp };
};

type Matchup = PlayoffSeries["series"][number][number];

export const findSeries = (
	series: PlayoffSeries["series"],
	a: number,
	b: number,
) => {
	for (const [round, matchups] of series.entries()) {
		for (const matchup of matchups) {
			const tids = [matchup.home.tid, matchup.away?.tid];
			if (
				matchup.away &&
				!matchup.away.pendingPlayIn &&
				tids.includes(a) &&
				tids.includes(b)
			) {
				return { round, matchup };
			}
		}
	}
	return undefined;
};

// "Eastern Conference Finals", "First Round", "Finals".
export const roundLabel = async (
	season: number,
	round: number,
	numRounds: number,
	cid: number | undefined,
) => {
	const playoffsByConf = await seasonCore.getPlayoffsByConf(season);
	let name: string = helpers.playoffRoundName(round, numRounds, playoffsByConf);
	if (name === "1st round") {
		name = "first round";
	}
	const confRounds =
		playoffsByConf === false
			? 0
			: numRounds - Math.log2(Math.max(1, playoffsByConf));
	if (cid !== undefined && round < confRounds) {
		const conf = g.get("confs", season).find((c) => c.cid === cid);
		if (conf) {
			name = name.includes("conference")
				? name.replace("conference", conf.name)
				: `${conf.name} ${name}`;
		}
	}
	return name.replace(/\b[a-z]/g, (c) => c.toUpperCase());
};

const SUMS = [
	"min",
	"pts",
	"fg",
	"fga",
	"tp",
	"tpa",
	"ft",
	"fta",
	"orb",
	"drb",
	"ast",
	"tov",
	"stl",
	"blk",
	"pf",
	"pm",
] as const;
type Sums = Record<(typeof SUMS)[number], number>;
const zeroSums = () => Object.fromEntries(SUMS.map((key) => [key, 0])) as Sums;

export type SeriesPlayer = Sums & {
	pid: number;
	tid: number;
	name: string;
	pos: string;
	gp: number;
	gs: number;
};

export type SeriesTeamTotals = Sums & { tid: number; gp: number };

export const branding = async (tid: number, season: number) => {
	const teamSeason = await idb.merged.teamSeasons.getByTidSeason(tid, season);
	const info = g.get("teamInfoCache")[tid];
	return {
		abbrev: teamSeason?.abbrev ?? info?.abbrev ?? "",
		region: teamSeason?.region ?? info?.region ?? "",
		name: teamSeason?.name ?? info?.name ?? "",
		imgURL: teamSeason?.imgURL ?? info?.imgURL,
		imgURLSmall: teamSeason?.imgURLSmall ?? info?.imgURLSmall,
	};
};

export default defineView({
	id: "playoffSeries",
	processInputs,
	load: async ({ inputs, updateEvents, prevInputs }) => {
		const { season, tid, opp } = inputs;
		const currentSeason = season === g.get("season");
		if (
			!updateEvents.has("firstRun") &&
			season === prevInputs?.season &&
			tid === prevInputs?.tid &&
			opp === prevInputs?.opp &&
			!(
				currentSeason &&
				(updateEvents.has("gameSim") ||
					updateEvents.has("playoffs") ||
					updateEvents.has("liveGames"))
			)
		) {
			return;
		}

		const playoffSeries = await idb.merged.playoffSeries.get(season);
		if (!playoffSeries) {
			return { errorMessage: "No playoffs that season." };
		}

		let series = playoffSeries.series;
		const gate = currentSeason ? await getLiveGate() : undefined;
		if (gate && gate.games.size > 0) {
			series = unplaySeries(series, gate.games.values());
		}
		const found = findSeries(series, tid, opp);
		if (!found) {
			return { errorMessage: "These two teams didn't meet in the playoffs." };
		}
		const { round } = found;
		const matchup: Matchup = found.matchup;
		const away = matchup.away!;
		const home = matchup.home;

		const numRounds = series.length;
		const numGames = g.get("numGamesPlayoffSeries", season)[round] ?? 7;
		const numGamesToWin = helpers.numGamesToWinRound(round, season);
		const augmented = (
			await helpers.augmentSeries([[matchup]], season)
		)[0]![0]!;

		const teamOf = async (t: typeof home, aug: (typeof augmented)["home"]) => {
			const teamSeason = await idb.merged.teamSeasons.getByTidSeason(
				t.tid,
				season,
			);
			return {
				tid: t.tid,
				cid: t.cid,
				seed: t.seed,
				won: t.won,
				abbrev: aug.abbrev,
				region: aug.region,
				name: teamSeason?.name ?? g.get("teamInfoCache")[t.tid]?.name ?? "",
				imgURL: aug.imgURL,
				imgURLSmall: aug.imgURLSmall,
				colors: aug.colors,
				record: aug.regularSeason,
			};
		};
		const teams = [
			await teamOf(home, augmented.home),
			await teamOf(away, augmented.away!),
		] as const;

		const label = await roundLabel(
			season,
			round,
			numRounds,
			home.cid === away.cid ? home.cid : undefined,
		);

		// The games played, oldest first.
		const gids = (matchup.gids ?? []).filter((gid) => !gate?.hidden.has(gid));
		const rows = (await idb.merged.games.getAll({ gids })).filter(
			(game) => game.won && game.lost,
		);
		rows.sort((a, b) => (a.day ?? 0) - (b.day ?? 0) || a.gid - b.gid);

		const autoNotes = await getAutoRecapsForGids({
			season,
			gids: rows.filter((game) => !game.note).map((game) => game.gid),
		});
		const playedGames = [];
		for (const [i, row] of rows.entries()) {
			const game = structuredClone(row);
			if (!currentSeason) {
				for (const t of game.teams) {
					t.branding = await branding(t.tid, season);
				}
			}
			playedGames.push({
				...game,
				gameNum: i + 1,
				note: game.note ?? autoNotes[game.gid],
			});
		}
		// Box scores for this many of the series' games are gone (BBGM keeps
		// only the last few seasons'), so the game by game can't be told.
		const missingGames = (matchup.gids?.length ?? 0) - gids.length;
		const boxScoresDeleted =
			rows.length < gids.length || (missingGames > 0 && !currentSeason);

		// The games still to come, while it's on.
		const leaderWins = Math.max(home.won, away.won);
		const upcoming: {
			gameNum: number;
			date: string | undefined;
			hostTid: number;
			ifNecessary: boolean;
		}[] = [];
		if (currentSeason && leaderWins < numGamesToWin) {
			const played = home.won + away.won;
			for (let k = played; k < numGames; k++) {
				upcoming.push({
					gameNum: k + 1,
					date: matchup.dates?.[k],
					hostTid: betterSeedHome(numGames, k) ? home.tid : away.tid,
					ifNecessary: leaderWins + (k - played) >= numGamesToWin,
				});
			}
		}

		// Everybody's lines, added up over the games played.
		const players = new Map<number, SeriesPlayer>();
		const totals = new Map<number, SeriesTeamTotals>();
		for (const game of rows) {
			for (const t of game.teams) {
				const total = totals.get(t.tid) ?? {
					tid: t.tid,
					gp: 0,
					...zeroSums(),
				};
				totals.set(t.tid, total);
				total.gp += 1;
				for (const key of SUMS) {
					if (key !== "pm" && typeof t[key] === "number") {
						total[key] += t[key];
					}
				}
				for (const p of t.players) {
					if (!(p.min > 0)) {
						continue;
					}
					const player = players.get(p.pid) ?? {
						pid: p.pid,
						tid: t.tid,
						name: p.name,
						pos: p.pos,
						gp: 0,
						gs: 0,
						...zeroSums(),
					};
					players.set(p.pid, player);
					player.tid = t.tid;
					player.gp += 1;
					player.gs += p.gs ?? 0;
					for (const key of SUMS) {
						if (typeof p[key] === "number") {
							player[key] += p[key];
						}
					}
				}
			}
		}

		// The regular season between them that year.
		const meetings = (
			await idb.merged.games.getAll(
				{ season },
				{
					map: (game) =>
						!game.playoffs &&
						game.won &&
						game.lost &&
						!gate?.hidden.has(game.gid) &&
						[tid, opp].includes(game.teams[0].tid) &&
						[tid, opp].includes(game.teams[1].tid)
							? {
									gid: game.gid,
									day: game.day,
									date: game.date,
									overtimes: game.overtimes,
									numPeriods: game.numPeriods,
									teams: [
										{ tid: game.teams[0].tid, pts: game.teams[0].pts },
										{ tid: game.teams[1].tid, pts: game.teams[1].pts },
									] as const,
									winner: getWinner(game.teams),
								}
							: undefined,
				},
			)
		)
			.filter((game) => game !== undefined)
			.sort((a, b) => (a.day ?? 0) - (b.day ?? 0) || a.gid - b.gid);
		// The record, from the head-to-head table, which outlives the box
		// scores. From home's side.
		let seasonRecord: { won: number; lost: number; tied: number } | undefined;
		const headToHead = await idb.merged.headToHeads.get(season);
		if (headToHead) {
			const lo = Math.min(home.tid, away.tid);
			const hi = Math.max(home.tid, away.tid);
			const info = headToHead.regularSeason[lo]?.[hi];
			if (info) {
				const won = info.won + info.otw;
				const lost = info.lost + info.otl;
				seasonRecord =
					lo === home.tid
						? { won, lost, tied: info.tied }
						: { won: lost, lost: won, tied: info.tied };
			}
		}

		// Every other time these two met in the playoffs.
		const pastMeetings: {
			season: number;
			label: string;
			winnerTid: number | undefined;
			won: number;
			lost: number;
			abbrevs: Record<number, string>;
		}[] = [];
		for (const other of await idb.merged.playoffSeries.getAll()) {
			if (other.season === season) {
				continue;
			}
			const hit = findSeries(other.series, tid, opp);
			if (!hit) {
				continue;
			}
			const h = hit.matchup.home;
			const a = hit.matchup.away!;
			const toWin = helpers.numGamesToWinRound(hit.round, other.season);
			const winner = h.won >= toWin ? h : a.won >= toWin ? a : undefined;
			const loser = winner === h ? a : h;
			pastMeetings.push({
				season: other.season,
				label: await roundLabel(
					other.season,
					hit.round,
					other.series.length,
					h.cid === a.cid ? h.cid : undefined,
				),
				winnerTid: winner?.tid,
				won: winner ? winner.won : Math.max(h.won, a.won),
				lost: winner ? loser.won : Math.min(h.won, a.won),
				abbrevs: {
					[h.tid]: (await branding(h.tid, other.season)).abbrev,
					[a.tid]: (await branding(a.tid, other.season)).abbrev,
				},
			});
		}
		pastMeetings.sort((a, b) => b.season - a.season);

		return {
			season,
			round,
			label,
			numGames,
			numGamesToWin,
			teams,
			playedGames: playedGames as (Game & {
				gameNum: number;
				note: string | undefined;
			})[],
			boxScoresDeleted,
			upcoming,
			players: [...players.values()],
			totals: teams.map((t) => totals.get(t.tid)),
			meetings,
			seasonRecord,
			pastMeetings,
			userTids: g.get("userTids"),
		};
	},
});
