import { idb } from "../db/index.ts";
import { g } from "./index.ts";
import { getGameSpread, roundHalf } from "../../common/getGameSpread.ts";
import { getTeamInfoBySeason } from "./getTeamInfoBySeason.ts";
import { getGlobalSettings } from "./getGlobalSettings.ts";
import {
	beginRecapBatch,
	endRecapBatch,
	getAutoDayRecap,
	getAutoRecap,
} from "./getAutoRecap.ts";
import {
	DEFAULT_RECAP_MAX_GAMES,
	DEFAULT_RECAP_MAX_DAYS,
} from "../../common/constants.ts";

// Per-game averages we compute for a player (this season, career, playoffs).
export type RecapAverages = {
	gp: number;
	min: number;
	pts: number;
	reb: number;
	ast: number;
	stl: number;
	blk: number;
	tov: number;
	fgp: number; // FG%
	tpp: number; // 3P%
	ftp: number; // FT%
};

export type RecapCareerSeason = RecapAverages & {
	season: number;
	age?: number;
	teams?: string[]; // team abbrev(s) that season (more than one if traded)
};

// Career entry before team abbrevs are resolved (worker-internal).
type CareerRaw = RecapAverages & {
	season: number;
	age?: number;
	tids: number[];
};

// One player's box-score line, trimmed to the stats worth narrating, plus the
// broader context a real writer would have: season/playoff averages and, for key
// players, a season-by-season career line.
export type RecapPlayer = {
	name: string;
	pid: number;
	min: number;
	pts: number;
	reb: number;
	ast: number;
	stl: number;
	blk: number;
	tov: number;
	fg: number;
	fga: number;
	tp: number;
	tpa: number;
	ft: number;
	fta: number;
	pf: number;
	pm?: number;
	// Averages ENTERING this game - the game itself (and anything after) is
	// excluded, so "he came in averaging X" is always true of these numbers.
	seasonAvg?: RecapAverages;
	playoffAvg?: RecapAverages;
	// Past seasons only (the current season is the seasonAvg above).
	career?: RecapCareerSeason[];
	// Set when a player who PLAYED was hurt this game or played through an injury.
	injury?: {
		type: string;
		gamesRemaining: number;
		newThisGame?: boolean;
		playingThrough?: boolean;
	};
};

// A player who did NOT play because of injury (the game's inactives).
export type RecapInjuryOut = {
	name: string;
	type: string;
	gamesRemaining: number;
};

export type RecapLast10Game = {
	opp: string; // opponent abbrev
	home: boolean;
	won: boolean;
	pts: number;
	oppPts: number;
};

export type RecapTeam = {
	tid: number;
	region: string;
	name: string;
	abbrev: string;
	pts: number;
	players: RecapPlayer[];
	// Season record and quarter-by-quarter scoring for THIS game.
	record?: { won: number; lost: number };
	ptsQtrs?: number[];
	last10?: RecapLast10Game[];
	// Win/loss streak AS OF this game (includes this game's result).
	streak?: { won: boolean; count: number };
	// Players held out of this game due to injury.
	injuries?: RecapInjuryOut[];
	seed?: number;
};

// A playoff series' state for context (bracket entry the game belongs to).
export type RecapSeries = {
	round: number; // 1-indexed
	numRounds: number;
	bestOf?: number;
	homeAbbrev: string;
	awayAbbrev: string;
	homeSeed?: number;
	awaySeed?: number;
	// Series wins each team had ENTERING this game (before it was played), so the
	// recap can set the stakes and then narrate this game's result changing them.
	homeWon: number;
	awayWon: number;
};

// A play-in tournament game's context. These are single elimination, NOT a
// series, and carry different stakes than a normal playoff game:
//   - "seed7v8": winner claims the higher (prizeSeed) playoff seed, loser lives
//     on in the final play-in game
//   - "seed9v10": winner advances to the final play-in game, loser is eliminated
//   - "final": winner claims the last playoff seed (prizeSeed), loser eliminated
export type RecapPlayIn = {
	kind: "seed7v8" | "seed9v10" | "final";
	homeAbbrev: string;
	awayAbbrev: string;
	homeSeed?: number;
	awaySeed?: number;
	// The playoff seed the winner clinches (undefined for the 9v10 game, whose
	// winner only advances to the final).
	prizeSeed?: number;
};

// Everything an AI needs to write a recap of one completed game.
export type RecapGame = {
	gid: number;
	day: number;
	overtimes: number;
	winnerTid: number;
	playoffs: boolean;
	teams: [RecapTeam, RecapTeam];
	series?: RecapSeries;
	// Set instead of `series` when this is a play-in tournament game.
	playIn?: RecapPlayIn;
	// The pregame betting line: which team was favored and by how many points
	// (points 0 = pick'em). Undefined when it can't be computed (legacy games with
	// no stored OVRs). Lets the recap frame upsets/blowouts against expectations.
	spread?: { favTid: number; points: number };
	// Narrative highlights ZenGM already generated (game-winners, milestones, ...).
	clutchPlays: string[];
	// Set ONLY for the All-Star Game. The game itself is still the normal box
	// score in `teams`; this carries the weekend extras (MVP + the dunk and
	// three-point contests) so the recap can cover the whole All-Star Weekend.
	allStar?: {
		mvp?: string;
		dunk?: { winner?: string; players: string[] };
		three?: { winner?: string; players: string[] };
		// The two squads' real names ("Team LeBron", "Eastern Conference"). The
		// game record's own team rows are the sentinel All-Star tids, which
		// getTeamInfoBySeason answers with region "All-Stars" and name "1"/"2" -
		// so a recap built from those read "1 beat 2 in the All-Star Game".
		teamNames?: [string, string];
	};
};

const STAT_KEYS = [
	"min",
	"fg",
	"fga",
	"tp",
	"tpa",
	"ft",
	"fta",
	"orb",
	"drb",
	"ast",
	"stl",
	"blk",
	"tov",
	"pf",
	"pts",
] as const;

const r1 = (x: number): number => Math.round(x * 10) / 10;
const pct = (made: number, att: number): number =>
	att > 0 ? Math.round((made / att) * 1000) / 10 : 0;

// The minimal shape of a completed game these record helpers read.
type RecapGameRow = {
	gid: number;
	day?: number;
	playoffs?: boolean;
	teams: { tid: number }[];
	won?: { tid: number };
	lost?: { tid: number };
};

// The series wins each team had ENTERING a given game (before it was played).
// Counts completed playoff games between the two teams with a SMALLER gid; the
// current game and any later ones are excluded. This reconstructs the pre-game
// series score from the games themselves, because the live playoffSeries.won
// totals reflect the state AFTER every game already played (including this one).
// Prefers the series' own gids list when present; otherwise falls back to
// head-to-head playoff games (two teams meet in exactly one series per
// postseason, so this is unambiguous).
export const seriesWinsBefore = (
	currentGid: number,
	homeTid: number,
	awayTid: number,
	gids: number[] | undefined,
	games: RecapGameRow[],
): { homeWon: number; awayWon: number } => {
	const winnerByGid = new Map<number, number>();
	for (const g of games) {
		if (g.won && g.lost) {
			winnerByGid.set(g.gid, g.won.tid);
		}
	}

	let homeWon = 0;
	let awayWon = 0;
	const tally = (gid: number) => {
		if (gid >= currentGid) {
			return; // only games strictly before this one
		}
		const winner = winnerByGid.get(gid);
		if (winner === homeTid) {
			homeWon += 1;
		} else if (winner === awayTid) {
			awayWon += 1;
		}
	};

	if (Array.isArray(gids) && gids.length > 0) {
		for (const gid of gids) {
			tally(gid);
		}
	} else {
		for (const g of games) {
			if (!g.playoffs || !g.won) {
				continue;
			}
			const t0 = g.teams[0]?.tid;
			const t1 = g.teams[1]?.tid;
			const samePair =
				(t0 === homeTid && t1 === awayTid) ||
				(t0 === awayTid && t1 === homeTid);
			if (samePair) {
				tally(g.gid);
			}
		}
	}

	return { homeWon, awayWon };
};

// A team's regular-season record AS OF a given day (through and including that
// day's games), reconstructed from the games rather than the live teamSeasons
// row - which holds the CURRENT record and would be wrong when recapping a past
// day. During the playoffs (upToDay past every regular-season day) this is the
// full regular-season record. Playoff games are excluded so this stays the
// regular-season record.
export const regularSeasonRecordAsOf = (
	tid: number,
	upToDay: number,
	games: RecapGameRow[],
): { won: number; lost: number } => {
	let won = 0;
	let lost = 0;
	for (const g of games) {
		if (g.playoffs || !g.won || !g.lost) {
			continue;
		}
		if ((g.day ?? 0) > upToDay) {
			continue;
		}
		if (g.teams[0]?.tid !== tid && g.teams[1]?.tid !== tid) {
			continue;
		}
		if (g.won.tid === tid) {
			won += 1;
		} else {
			lost += 1;
		}
	}
	return { won, lost };
};

type Totals = Record<(typeof STAT_KEYS)[number], number> & { gp: number };

const aggregate = (rows: any[]): Totals => {
	const tot: Totals = {
		gp: 0,
		min: 0,
		fg: 0,
		fga: 0,
		tp: 0,
		tpa: 0,
		ft: 0,
		fta: 0,
		orb: 0,
		drb: 0,
		ast: 0,
		stl: 0,
		blk: 0,
		tov: 0,
		pf: 0,
		pts: 0,
	};
	for (const row of rows) {
		tot.gp += row.gp ?? 0;
		for (const key of STAT_KEYS) {
			tot[key] += row[key] ?? 0;
		}
	}
	return tot;
};

const toAverages = (tot: Totals): RecapAverages => {
	const gp = tot.gp || 1;
	return {
		gp: tot.gp,
		min: r1(tot.min / gp),
		pts: r1(tot.pts / gp),
		reb: r1((tot.orb + tot.drb) / gp),
		ast: r1(tot.ast / gp),
		stl: r1(tot.stl / gp),
		blk: r1(tot.blk / gp),
		tov: r1(tot.tov / gp),
		fgp: pct(tot.fg, tot.fga),
		tpp: pct(tot.tp, tot.tpa),
		ftp: pct(tot.ft, tot.fta),
	};
};

// Career context for a player: PAST seasons only, season by season, with age
// and team[s]. The current season is deliberately excluded - its live totals
// include the game being recapped (and any games after it), and a "this
// season" number that already contains the game misreads as what the player
// came in averaging. The current season is covered by the entering-this-game
// averages instead. Only requested for key players, to keep the prompt from
// ballooning. Teams are returned as raw tids; the caller resolves to abbrevs.
const playerCareer = (player: any, season: number): CareerRaw[] | undefined => {
	const stats: any[] = Array.isArray(player?.stats) ? player.stats : [];
	const regular = stats.filter((s) => !s.playoffs && s.season !== season);

	const bornYear =
		typeof player?.born?.year === "number" ? player.born.year : undefined;
	const bySeason = new Map<number, any[]>();
	for (const row of regular) {
		const arr = bySeason.get(row.season) ?? [];
		arr.push(row);
		bySeason.set(row.season, arr);
	}
	const careerRaw = [...bySeason.entries()]
		.map(([s, rows]) => ({
			season: s,
			tot: aggregate(rows),
			tids: [
				...new Set(
					rows.map((r) => r.tid).filter((t) => typeof t === "number" && t >= 0),
				),
			] as number[],
		}))
		.filter(({ tot }) => tot.gp > 0)
		.sort((a, b) => a.season - b.season)
		.map(({ season: s, tot, tids }) => ({
			season: s,
			age: bornYear !== undefined ? s - bornYear : undefined,
			tids,
			...toAverages(tot),
		}));
	return careerRaw.length > 0 ? careerRaw : undefined;
};

// One player's box line in one completed game, tagged so averages can be
// computed AS OF a given game.
export type PlayerGameLine = {
	day: number;
	gid: number;
	playoffs: boolean;
	row: Record<string, number>;
};

// A player's averages ENTERING a given game (strictly earlier games only).
// Live season totals can't be used for this: they include the game being
// recapped (and, when recapping a past day, every game after it too), and a
// reader - human or AI - reads "season average" as the pregame number. E.g. a
// 16 ppg scorer who erupts for 46 lifts his post-game average to 26; feeding
// 26 produces "he came in averaging 26 and nearly doubled it".
export const enteringAverages = (
	lines: PlayerGameLine[],
	beforeGid: number,
	beforeDay: number,
	playoffs: boolean,
): RecapAverages | undefined => {
	const rows = lines
		.filter(
			(l) =>
				l.playoffs === playoffs &&
				(l.day < beforeDay || (l.day === beforeDay && l.gid < beforeGid)),
		)
		.map((l) => l.row);
	if (rows.length === 0) {
		return undefined;
	}
	const tot = aggregate(rows);
	return tot.gp > 0 ? toAverages(tot) : undefined;
};

// The game/day budgets for one recap run come from Global Settings
// (recapMaxGames / recapMaxDays), falling back to these defaults:
//
// - Games: each recap is several paragraphs of AI output, so an unbounded
//   sweep of a whole unrecapped season would blow past any model's reply
//   length; whatever doesn't fit stays note-less and the next Copy sweeps it up.
// - Days: one recap run also backfills WHOLE-DAY recaps for up to this many
//   days it covers (oldest first, FIFO) - so the day recap is never tied to the
//   day the user happens to be viewing; it fills in whichever days are missing one.

// A compact one-day results slate, for a day that needs a recap but whose games
// aren't in the detailed blocks (they were already game-recapped). Enough for the
// AI to write that day's overview: final scores, winner, and each team's leading
// scorer.
export type RecapDaySlate = {
	day: number;
	games: {
		away: string; // "Region Name"
		home: string;
		awayPts: number;
		homePts: number;
		winner: string; // "Region Name" of the winner
		topAway?: { name: string; pts: number };
		topHome?: { name: string; pts: number };
	}[];
};

// A MONONYM LEAVES A TRAILING SPACE. Stored game records build the name as
// `${firstName} ${lastName}`, and a player with no surname - Nene, Pele,
// Ronaldinho - comes out as "Nene " with the space still on it. Downstream that
// became "Nene 's 25 points and 11 rebounds", every time he had a good night.
//
// Fixed at the source too (core/game/writeGameStats.ts), but games already
// played carry the old string forever, so the recap layer cleans what it reads
// rather than trusting it.
export const cleanName = (name: unknown): string =>
	String(name ?? "Unknown")
		.replaceAll(/\s+/g, " ")
		.trim() || "Unknown";

// The league standings AS OF a given league day, split by conference, so a day
// recap can talk about the playoff picture accurately for that day (not the
// current, later state). One per day recap needed.
export type RecapDayStandings = {
	day: number;
	confs: {
		name: string;
		teams: {
			rank: number;
			abbrev: string;
			region: string;
			name: string;
			won: number;
			lost: number;
			gb: number; // games back of the conference leader
		}[];
	}[];
};

// Which completed games one recap run covers: every completed game this season
// still missing a recap note - so days that were simmed past get their recaps
// generated in the same run instead of paging through them day by day. Games
// that already have a note are skipped, so a recap is never overwritten. Strict
// FIFO: chronological order, and when the cap bites, the OLDEST games keep
// their slots - a deep backlog is cleared oldest-first across successive runs,
// never leapfrogged by newer days.
//
// The sweep never crosses the regular-season/playoff boundary: a run stays
// within the phase of the day the user clicked Copy on. So clicking Copy on the
// first playoff day starts the playoffs fresh instead of dragging in a backlog
// of un-recapped regular-season games (and vice versa).
export const selectRecapGames = <
	T extends { gid: number; day?: number; playoffs?: unknown; note?: unknown },
>(
	completed: T[],
	day: number,
	maxGames: number,
): T[] => {
	// Phase of the clicked day: the games on that exact day settle it (a day is
	// entirely regular season or entirely playoffs). If none are on that day,
	// fall back to whether any playoff game has been played by then.
	const gamesOnDay = completed.filter((game) => (game.day ?? 0) === day);
	const isPlayoffs =
		gamesOnDay.length > 0
			? gamesOnDay.some((game) => !!game.playoffs)
			: completed.some((game) => !!game.playoffs && (game.day ?? 0) <= day);

	return completed
		.filter((game) => !game.note && !!game.playoffs === isPlayoffs)
		.sort((a, b) => (a.day ?? 0) - (b.day ?? 0) || a.gid - b.gid)
		.slice(0, maxGames);
};

// Completed games for a recap run (see selectRecapGames), each with team
// names, every box-score line, season/career/playoff averages, records,
// quarter scoring, a last-10 game log, and (in the playoffs) the
// series/bracket state - the raw material a "Copy AI Prompt" button bakes
// into a recap prompt.
export const getDayGamesForRecap = async ({
	season,
	day,
}: {
	season: number;
	day: number;
}): Promise<{
	games: RecapGame[];
	dayRecapDays: number[];
	daySlates: RecapDaySlate[];
	standingsByDay: RecapDayStandings[];
}> => {
	const globalSettings = await getGlobalSettings();
	const maxRecapGames = globalSettings.recapMaxGames ?? DEFAULT_RECAP_MAX_GAMES;
	const maxRecapDays = globalSettings.recapMaxDays ?? DEFAULT_RECAP_MAX_DAYS;

	const allGames = await idb.getCopies.games({ season }, "noCopyCache");

	// The All-Star Game is a normal game record with sentinel team ids (-1 home,
	// -2 away). Its MVP and the weekend contests live on the separate allStars
	// object, which we fold into that game's recap below.
	const allStars = await idb.getCopy.allStars({ season });
	const isAllStarGame = (game: { teams: { tid: number }[] }): boolean =>
		game.teams[0]?.tid === -1 && game.teams[1]?.tid === -2;

	const allStarPayload = (): RecapGame["allStar"] => {
		if (!allStars) {
			return {};
		}
		const contest = (
			c: { players: { name: string }[]; winner?: number } | undefined,
		): { winner?: string; players: string[] } | undefined => {
			if (!c) {
				return undefined;
			}
			return {
				winner:
					typeof c.winner === "number" ? c.players[c.winner]?.name : undefined,
				players: c.players.map((p) => p.name),
			};
		};
		return {
			mvp: allStars.mvp?.name,
			dunk: contest(allStars.dunk),
			three: contest(allStars.three),
			teamNames: allStars.teamNames,
		};
	};

	const games = selectRecapGames(
		allGames.filter((game) => game.won && game.lost),
		day,
		maxRecapGames,
	);

	// Every player's box line from every completed game this season, so each
	// recapped game can report what a player was averaging ENTERING it (see
	// enteringAverages).
	const linesByPid = new Map<number, PlayerGameLine[]>();
	for (const game of allGames) {
		if (!game.won || !game.lost) {
			continue;
		}
		for (const t of game.teams) {
			for (const p of (t as any).players ?? []) {
				if ((p?.min ?? 0) > 0 && typeof p?.pid === "number") {
					const arr = linesByPid.get(p.pid) ?? [];
					arr.push({
						day: game.day ?? 0,
						gid: game.gid,
						playoffs: !!game.playoffs,
						row: { ...p, gp: 1 },
					});
					linesByPid.set(p.pid, arr);
				}
			}
		}
	}

	// Team info memo so we resolve each team's name/abbrev at most once.
	const teamInfoCache = new Map<
		number,
		{ region: string; name: string; abbrev: string } | undefined
	>();
	const teamInfo = async (tid: number) => {
		if (!teamInfoCache.has(tid)) {
			teamInfoCache.set(tid, await getTeamInfoBySeason(tid, season));
		}
		return teamInfoCache.get(tid);
	};
	const abbrevOf = async (tid: number) =>
		(await teamInfo(tid))?.abbrev ?? "???";

	// A team's abbrev in a SPECIFIC (historical) season, for career lines.
	const abbrevSeasonCache = new Map<string, string>();
	const abbrevBySeasonTid = async (tid: number, s: number): Promise<string> => {
		const key = `${tid}:${s}`;
		if (!abbrevSeasonCache.has(key)) {
			const info = await getTeamInfoBySeason(tid, s);
			abbrevSeasonCache.set(key, info?.abbrev ?? "???");
		}
		return abbrevSeasonCache.get(key)!;
	};

	// A team's last 10 completed games up to and including this day.
	const last10For = async (
		tid: number,
		upToDay: number,
	): Promise<RecapLast10Game[]> => {
		const teamGames = allGames
			.filter(
				(game) =>
					game.won &&
					game.lost &&
					(game.day ?? 0) <= upToDay &&
					(game.teams[0].tid === tid || game.teams[1].tid === tid),
			)
			.sort((a, b) => (b.day ?? 0) - (a.day ?? 0) || b.gid - a.gid)
			.slice(0, 10);

		const out: RecapLast10Game[] = [];
		for (const game of teamGames) {
			const home = game.teams[0].tid === tid;
			const mine = home ? game.teams[0] : game.teams[1];
			const opp = home ? game.teams[1] : game.teams[0];
			out.push({
				opp: await abbrevOf(opp.tid),
				home,
				won: game.won.tid === tid,
				pts: mine.pts,
				oppPts: opp.pts,
			});
		}
		return out;
	};

	// A team's win/loss streak as of a given day (counts back from its most
	// recent completed game, which is the game being recapped).
	const streakFor = (
		tid: number,
		upToDay: number,
	): { won: boolean; count: number } | undefined => {
		const teamGames = allGames
			.filter(
				(game) =>
					game.won &&
					game.lost &&
					(game.day ?? 0) <= upToDay &&
					(game.teams[0].tid === tid || game.teams[1].tid === tid),
			)
			.sort((a, b) => (b.day ?? 0) - (a.day ?? 0) || b.gid - a.gid);
		if (teamGames.length === 0) {
			return undefined;
		}
		const won = teamGames[0]!.won.tid === tid;
		let count = 0;
		for (const game of teamGames) {
			if ((game.won.tid === tid) === won) {
				count += 1;
			} else {
				break;
			}
		}
		return { won, count };
	};

	// Playoff series lookup for the current postseason. numGamesPlayoffSeries is
	// the best-of length PER ROUND (customizable, so not always 7); index it by
	// the 0-based round to tell the recap how long each series actually is.
	const playoffSeries = await idb.cache.playoffSeries.get(season);
	const numGamesPlayoffSeries = g.get("numGamesPlayoffSeries", season);
	const seriesForGame = async (game: any): Promise<RecapSeries | undefined> => {
		if (!playoffSeries || !Array.isArray(playoffSeries.series)) {
			return undefined;
		}
		const [tidA, tidB] = [game.teams[0].tid, game.teams[1].tid];
		for (let round = playoffSeries.series.length - 1; round >= 0; round--) {
			const matchups = playoffSeries.series[round];
			if (!matchups) {
				continue;
			}
			for (const matchup of matchups) {
				const home = matchup?.home;
				const away = matchup?.away;
				if (!home || !away) {
					continue;
				}
				const byGid = matchup.gids?.includes(game.gid);
				const byTid =
					(home.tid === tidA && away.tid === tidB) ||
					(home.tid === tidB && away.tid === tidA);
				if (byGid || byTid) {
					// Series record ENTERING this game, reconstructed from the games
					// themselves - not home.won/away.won, which are the current totals.
					const { homeWon, awayWon } = seriesWinsBefore(
						game.gid,
						home.tid,
						away.tid,
						matchup.gids,
						allGames,
					);
					return {
						round: round + 1,
						numRounds: playoffSeries.series.length,
						bestOf: numGamesPlayoffSeries?.[round],
						homeAbbrev: home.abbrev ?? (await abbrevOf(home.tid)),
						awayAbbrev: away.abbrev ?? (await abbrevOf(away.tid)),
						homeSeed: home.seed,
						awaySeed: away.seed,
						homeWon,
						awayWon,
					};
				}
			}
		}
		return undefined;
	};

	// Play-in tournament lookup. During the play-in, matchups live in
	// playoffSeries.playIns (not .series), so a play-in game is otherwise
	// indistinguishable from a normal playoff game - this classifies it.
	const playInForGame = async (game: any): Promise<RecapPlayIn | undefined> => {
		const playIns = playoffSeries?.playIns;
		if (!Array.isArray(playIns)) {
			return undefined;
		}
		const [tidA, tidB] = [game.teams[0].tid, game.teams[1].tid];
		for (const playIn of playIns) {
			for (let j = 0; j < playIn.length; j++) {
				const matchup: any = playIn[j];
				const home = matchup?.home;
				const away = matchup?.away;
				if (!home || !away) {
					continue;
				}
				const byGid = matchup.gids?.includes(game.gid);
				const byTid =
					(home.tid === tidA && away.tid === tidB) ||
					(home.tid === tidB && away.tid === tidA);
				if (byGid || byTid) {
					const kind = j === 0 ? "seed7v8" : j === 1 ? "seed9v10" : "final";
					let prizeSeed: number | undefined;
					if (kind === "seed7v8") {
						// Winner takes the better (numerically lower) of the two seeds.
						prizeSeed = Math.min(home.seed, away.seed);
					} else if (kind === "final") {
						// Winner takes the last playoff spot (the 8-seed slot from the
						// group's 7-vs-8 game).
						prizeSeed = playIn[0]?.away?.seed;
					}
					return {
						kind,
						homeAbbrev: home.abbrev ?? (await abbrevOf(home.tid)),
						awayAbbrev: away.abbrev ?? (await abbrevOf(away.tid)),
						homeSeed: home.seed,
						awaySeed: away.seed,
						prizeSeed,
					};
				}
			}
		}
		return undefined;
	};

	const seedOf = (tid: number): number | undefined => {
		if (!playoffSeries || !Array.isArray(playoffSeries.series)) {
			return undefined;
		}
		for (const matchups of playoffSeries.series) {
			for (const matchup of matchups ?? []) {
				if (matchup?.home?.tid === tid) {
					return matchup.home.seed;
				}
				if (matchup?.away?.tid === tid) {
					return matchup.away?.seed;
				}
			}
		}
		return undefined;
	};

	const result: RecapGame[] = [];
	for (const game of games) {
		const playoffs = !!game.playoffs;
		const allStar = isAllStarGame(game);

		const teams = [] as unknown as [RecapTeam, RecapTeam];
		for (const t of game.teams) {
			const info = await teamInfo(t.tid);

			const allPlayers = Array.isArray(t.players) ? t.players : [];

			// Players who missed the game due to injury (the inactives).
			const injuries: RecapInjuryOut[] = allPlayers
				.filter(
					(p: any) =>
						(p?.min ?? 0) === 0 && p?.injury && p.injury.gamesRemaining > 0,
				)
				.map((p: any) => ({
					name: cleanName(p?.name),
					type: String(p.injury.type ?? "injury"),
					gamesRemaining: p.injury.gamesRemaining ?? 0,
				}));

			// Rank players by scoring so we only pull full career context for the
			// team's top handful (keeps the prompt rich but not enormous).
			const played = allPlayers.filter((p: any) => (p?.min ?? 0) > 0);
			const topByPts = new Set(
				[...played]
					.sort((a: any, b: any) => (b?.pts ?? 0) - (a?.pts ?? 0))
					.slice(0, 6)
					.map((p: any) => p.pid),
			);

			const players: RecapPlayer[] = [];
			for (const p of played) {
				const base: RecapPlayer = {
					name: cleanName(p?.name),
					pid: p?.pid,
					min: Math.round(p?.min ?? 0),
					pts: p?.pts ?? 0,
					reb: (p?.orb ?? 0) + (p?.drb ?? 0),
					ast: p?.ast ?? 0,
					stl: p?.stl ?? 0,
					blk: p?.blk ?? 0,
					tov: p?.tov ?? 0,
					fg: p?.fg ?? 0,
					fga: p?.fga ?? 0,
					tp: p?.tp ?? 0,
					tpa: p?.tpa ?? 0,
					ft: p?.ft ?? 0,
					fta: p?.fta ?? 0,
					pf: p?.pf ?? 0,
					pm: typeof p?.pm === "number" ? p.pm : undefined,
					injury:
						p?.injury && (p.injury.newThisGame || p.injury.playingThrough)
							? {
									type: String(p.injury.type ?? "injury"),
									gamesRemaining: p.injury.gamesRemaining ?? 0,
									newThisGame: !!p.injury.newThisGame,
									playingThrough: !!p.injury.playingThrough,
								}
							: undefined,
				};

				if (typeof p?.pid === "number") {
					const lines = linesByPid.get(p.pid) ?? [];
					base.seasonAvg = enteringAverages(
						lines,
						game.gid,
						game.day ?? day,
						false,
					);
					if (playoffs) {
						base.playoffAvg = enteringAverages(
							lines,
							game.gid,
							game.day ?? day,
							true,
						);
					}
				}

				const full =
					typeof p?.pid === "number" && topByPts.has(p.pid)
						? await idb.cache.players.get(p.pid)
						: undefined;
				if (full) {
					const careerRaw = playerCareer(full, season);
					if (careerRaw) {
						const career: RecapCareerSeason[] = [];
						for (const c of careerRaw) {
							const teams: string[] = [];
							for (const tid of c.tids) {
								teams.push(await abbrevBySeasonTid(tid, c.season));
							}
							const { tids, ...rest } = c;
							void tids;
							career.push({ ...rest, teams });
						}
						base.career = career;
					}
				}
				players.push(base);
			}

			teams.push({
				tid: t.tid,
				region: info?.region ?? "",
				name: info?.name ?? "Team",
				abbrev: info?.abbrev ?? "???",
				pts: t.pts,
				players,
				// Records, streaks, last-10, and seeds are meaningless for the two
				// one-off All-Star squads, so leave them off that game's blocks.
				record: allStar
					? undefined
					: regularSeasonRecordAsOf(t.tid, game.day ?? day, allGames),
				ptsQtrs: Array.isArray(t.ptsQtrs) ? t.ptsQtrs : undefined,
				last10: allStar ? undefined : await last10For(t.tid, game.day ?? day),
				streak: allStar ? undefined : streakFor(t.tid, game.day ?? day),
				injuries: injuries.length > 0 ? injuries : undefined,
				seed: playoffs ? seedOf(t.tid) : undefined,
			});
		}

		// A play-in game is a playoff game, but its matchup lives in playIns, not
		// series - classify it first so we tag it as play-in rather than looking
		// (and failing) to find it as a normal series game.
		const playIn = playoffs ? await playInForGame(game) : undefined;

		// The pregame spread (same number ScoreBox showed), so the recap knows who
		// was favored and can frame an upset/blowout against expectations. Games
		// store it at sim time now - the synergy input can't be rebuilt from a box
		// score - so only legacy games re-derive the ovr-only line.
		let spread: RecapGame["spread"];
		const rawSpread =
			game.spread ??
			getGameSpread({
				ovr0: game.teams[0].ovr,
				ovr1: game.teams[1].ovr,
				homeCourtAdvantage: g.get("homeCourtAdvantage"),
				neutralSite: !!game.neutralSite,
				numPeriods: game.numPeriods ?? g.get("numPeriods"),
				quarterLength: g.get("quarterLength"),
				playoffs,
			});
		if (rawSpread !== undefined) {
			// > 0 → home (teams[0]) favored; < 0 → away favored; 0 → pick'em.
			// Rounded because this number gets SPOKEN - "13-point underdogs" -
			// and a quoted line is a half point. play.ts rounds what it stores
			// now, but games simmed before it did carry a raw float, and a recap
			// reading one of those would otherwise read out all seventeen
			// decimals of it.
			const points = roundHalf(rawSpread);
			spread =
				points >= 0
					? { favTid: game.teams[0].tid, points }
					: { favTid: game.teams[1].tid, points: -points };
		}

		result.push({
			gid: game.gid,
			day: game.day ?? day,
			overtimes: game.overtimes ?? 0,
			winnerTid: game.won.tid,
			playoffs,
			teams,
			series: playoffs && !playIn ? await seriesForGame(game) : undefined,
			playIn,
			// No betting line for an exhibition; the All-Star payload replaces it.
			spread: allStar ? undefined : spread,
			clutchPlays: Array.isArray(game.clutchPlays) ? game.clutchPlays : [],
			allStar: allStar ? allStarPayload() : undefined,
		});
	}

	// Days (with completed games) still missing a WHOLE-DAY recap - the day's
	// anchor game (lowest gid of the day) has no dayNote - oldest first (FIFO),
	// capped. Computed over ALL completed days in the season, NOT just the ones
	// with note-less games, so a day whose games are already all game-recapped
	// (e.g. an early day recapped before its day recap existed) still gets its day
	// recap backfilled.
	const completedByDay = new Map<number, typeof allGames>();
	for (const game of allGames) {
		if (!game.won || !game.lost) {
			continue;
		}
		const d = game.day ?? 0;
		const arr = completedByDay.get(d) ?? [];
		arr.push(game);
		completedByDay.set(d, arr);
	}
	// The days whose full game data is in the detailed blocks this run.
	const detailedDays = new Set(result.map((game) => game.day));
	// Request a day recap for a missing-dayNote day only when we actually have its
	// material: it's detailed above (write from full data), OR all its games are
	// already game-recapped (a backfill day - write from the compact slate below).
	// A day that's note-less but not yet detailed is DEFERRED - it'll be detailed
	// and recapped on a later run, so we don't write it a thin recap now.
	const dayRecapDays = [...completedByDay.keys()]
		.sort((a, b) => a - b)
		.filter((d) => {
			const dayGames = completedByDay.get(d)!;
			const anchor = dayGames.reduce((a, b) => (a.gid <= b.gid ? a : b));
			if (anchor.dayNote !== undefined) {
				return false;
			}
			const fullyRecapped = dayGames.every(
				(game) => (game as any).note !== undefined,
			);
			return detailedDays.has(d) || fullyRecapped;
		})
		.slice(0, maxRecapDays);

	// For any of those days whose games are NOT detailed above (the fully-recapped
	// backfill days), hand the AI a compact results slate so it still has material.
	const teamName = async (tid: number): Promise<string> => {
		const info = await teamInfo(tid);
		return info ? `${info.region} ${info.name}` : "???";
	};
	const topScorer = (team: any): { name: string; pts: number } | undefined => {
		let best: { name: string; pts: number } | undefined;
		for (const p of (team?.players ?? []) as any[]) {
			if (
				p &&
				typeof p.pts === "number" &&
				p.name &&
				(!best || p.pts > best.pts)
			) {
				best = { name: cleanName(p.name), pts: p.pts };
			}
		}
		return best;
	};
	const daySlates: RecapDaySlate[] = [];
	for (const d of dayRecapDays) {
		if (detailedDays.has(d)) {
			continue;
		}
		const dayGames = [...completedByDay.get(d)!].sort((a, b) => a.gid - b.gid);
		const slateGames: RecapDaySlate["games"] = [];
		for (const game of dayGames) {
			const home = game.teams[0];
			const away = game.teams[1];
			const homeName = await teamName(home.tid);
			const awayName = await teamName(away.tid);
			slateGames.push({
				away: awayName,
				home: homeName,
				awayPts: away.pts,
				homePts: home.pts,
				winner: game.won.tid === home.tid ? homeName : awayName,
				topAway: topScorer(away),
				topHome: topScorer(home),
			});
		}
		daySlates.push({ day: d, games: slateGames });
	}

	// The full standings, split by conference, AS OF each day a recap is needed -
	// so a day recap can talk about the playoff picture as it stood that day.
	const allTeams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
	const confs = g.get("confs", season);
	const standingsByDay: RecapDayStandings[] = dayRecapDays.map((d) => ({
		day: d,
		confs: confs.map((conf) => {
			const rows = allTeams
				.filter((t) => t.cid === conf.cid)
				.map((t) => {
					const rec = regularSeasonRecordAsOf(t.tid, d, allGames);
					return {
						abbrev: t.abbrev,
						region: t.region,
						name: t.name,
						won: rec.won,
						lost: rec.lost,
					};
				})
				.sort((a, b) => {
					const wpA = a.won + a.lost > 0 ? a.won / (a.won + a.lost) : 0;
					const wpB = b.won + b.lost > 0 ? b.won / (b.won + b.lost) : 0;
					return wpB - wpA || b.won - a.won || a.abbrev.localeCompare(b.abbrev);
				});
			const leader = rows[0];
			return {
				name: conf.name,
				teams: rows.map((t, i) => ({
					rank: i + 1,
					abbrev: t.abbrev,
					region: t.region,
					name: t.name,
					won: t.won,
					lost: t.lost,
					gb: leader ? (leader.won - t.won + (t.lost - leader.lost)) / 2 : 0,
				})),
			};
		}),
	}));

	return { games: result, dayRecapDays, daySlates, standingsByDay };
};

// Shared machinery for the always-on auto recaps. These build a RecapGame from a
// completed game (box score + records, streaks, entering averages, injuries, and,
// in the postseason, series/play-in state) and run it through getAutoRecap /
// getAutoDayRecap. They render automatically under each game card and as the day
// recap; the "Copy AI Prompt" flow above stays the on-demand upgrade (it reads the
// real game.note/dayNote, which auto recaps never touch, so a filed AI/human note
// always wins). Generated fresh on each view (deterministic, so stable) and never
// written to the database. Career lines - which only the AI prompt uses - are
// skipped to avoid the per-player lookups they'd cost.
const createAutoRecapContext = async (season: number) => {
	const allGames = await idb.getCopies.games({ season }, "noCopyCache");

	const allStars = await idb.getCopy.allStars({ season });
	const isAllStarGame = (game: { teams: { tid: number }[] }): boolean =>
		game.teams[0]?.tid === -1 && game.teams[1]?.tid === -2;
	const allStarPayload = (): RecapGame["allStar"] => {
		if (!allStars) {
			return {};
		}
		const contest = (
			c: { players: { name: string }[]; winner?: number } | undefined,
		): { winner?: string; players: string[] } | undefined => {
			if (!c) {
				return undefined;
			}
			return {
				winner:
					typeof c.winner === "number" ? c.players[c.winner]?.name : undefined,
				players: c.players.map((p) => p.name),
			};
		};
		return {
			mvp: allStars.mvp?.name,
			dunk: contest(allStars.dunk),
			three: contest(allStars.three),
			teamNames: allStars.teamNames,
		};
	};

	// Every player's box line from every completed game this season, for the
	// entering-this-game averages (see enteringAverages).
	const linesByPid = new Map<number, PlayerGameLine[]>();
	for (const game of allGames) {
		if (!game.won || !game.lost) {
			continue;
		}
		for (const t of game.teams) {
			for (const p of (t as any).players ?? []) {
				if ((p?.min ?? 0) > 0 && typeof p?.pid === "number") {
					const arr = linesByPid.get(p.pid) ?? [];
					arr.push({
						day: game.day ?? 0,
						gid: game.gid,
						playoffs: !!game.playoffs,
						row: { ...p, gp: 1 },
					});
					linesByPid.set(p.pid, arr);
				}
			}
		}
	}

	const teamInfoCache = new Map<
		number,
		{ region: string; name: string; abbrev: string } | undefined
	>();
	const teamInfo = async (tid: number) => {
		if (!teamInfoCache.has(tid)) {
			teamInfoCache.set(tid, await getTeamInfoBySeason(tid, season));
		}
		return teamInfoCache.get(tid);
	};
	const abbrevOf = async (tid: number) =>
		(await teamInfo(tid))?.abbrev ?? "???";

	const last10For = async (
		tid: number,
		upToDay: number,
	): Promise<RecapLast10Game[]> => {
		const teamGames = allGames
			.filter(
				(game) =>
					game.won &&
					game.lost &&
					(game.day ?? 0) <= upToDay &&
					(game.teams[0].tid === tid || game.teams[1].tid === tid),
			)
			.sort((a, b) => (b.day ?? 0) - (a.day ?? 0) || b.gid - a.gid)
			.slice(0, 10);
		const out: RecapLast10Game[] = [];
		for (const game of teamGames) {
			const home = game.teams[0].tid === tid;
			const mine = home ? game.teams[0] : game.teams[1];
			const opp = home ? game.teams[1] : game.teams[0];
			out.push({
				opp: await abbrevOf(opp.tid),
				home,
				won: game.won!.tid === tid,
				pts: mine.pts,
				oppPts: opp.pts,
			});
		}
		return out;
	};

	const streakFor = (
		tid: number,
		upToDay: number,
	): { won: boolean; count: number } | undefined => {
		const teamGames = allGames
			.filter(
				(game) =>
					game.won &&
					game.lost &&
					(game.day ?? 0) <= upToDay &&
					(game.teams[0].tid === tid || game.teams[1].tid === tid),
			)
			.sort((a, b) => (b.day ?? 0) - (a.day ?? 0) || b.gid - a.gid);
		if (teamGames.length === 0) {
			return undefined;
		}
		const won = teamGames[0]!.won!.tid === tid;
		let count = 0;
		for (const game of teamGames) {
			if ((game.won!.tid === tid) === won) {
				count += 1;
			} else {
				break;
			}
		}
		return { won, count };
	};

	const playoffSeries = await idb.cache.playoffSeries.get(season);
	const numGamesPlayoffSeries = g.get("numGamesPlayoffSeries", season);
	const seriesForGame = async (game: any): Promise<RecapSeries | undefined> => {
		if (!playoffSeries || !Array.isArray(playoffSeries.series)) {
			return undefined;
		}
		const [tidA, tidB] = [game.teams[0].tid, game.teams[1].tid];
		for (let round = playoffSeries.series.length - 1; round >= 0; round--) {
			const matchups = playoffSeries.series[round];
			if (!matchups) {
				continue;
			}
			for (const matchup of matchups) {
				const home = matchup?.home;
				const away = matchup?.away;
				if (!home || !away) {
					continue;
				}
				const byGid = matchup.gids?.includes(game.gid);
				const byTid =
					(home.tid === tidA && away.tid === tidB) ||
					(home.tid === tidB && away.tid === tidA);
				if (byGid || byTid) {
					const { homeWon, awayWon } = seriesWinsBefore(
						game.gid,
						home.tid,
						away.tid,
						matchup.gids,
						allGames,
					);
					return {
						round: round + 1,
						numRounds: playoffSeries.series.length,
						bestOf: numGamesPlayoffSeries?.[round],
						homeAbbrev: home.abbrev ?? (await abbrevOf(home.tid)),
						awayAbbrev: away.abbrev ?? (await abbrevOf(away.tid)),
						homeSeed: home.seed,
						awaySeed: away.seed,
						homeWon,
						awayWon,
					};
				}
			}
		}
		return undefined;
	};
	const playInForGame = async (game: any): Promise<RecapPlayIn | undefined> => {
		const playIns = playoffSeries?.playIns;
		if (!Array.isArray(playIns)) {
			return undefined;
		}
		const [tidA, tidB] = [game.teams[0].tid, game.teams[1].tid];
		for (const playIn of playIns) {
			for (let j = 0; j < playIn.length; j++) {
				const matchup: any = playIn[j];
				const home = matchup?.home;
				const away = matchup?.away;
				if (!home || !away) {
					continue;
				}
				const byGid = matchup.gids?.includes(game.gid);
				const byTid =
					(home.tid === tidA && away.tid === tidB) ||
					(home.tid === tidB && away.tid === tidA);
				if (byGid || byTid) {
					const kind = j === 0 ? "seed7v8" : j === 1 ? "seed9v10" : "final";
					let prizeSeed: number | undefined;
					if (kind === "seed7v8") {
						prizeSeed = Math.min(home.seed, away.seed);
					} else if (kind === "final") {
						prizeSeed = playIn[0]?.away?.seed;
					}
					return {
						kind,
						homeAbbrev: home.abbrev ?? (await abbrevOf(home.tid)),
						awayAbbrev: away.abbrev ?? (await abbrevOf(away.tid)),
						homeSeed: home.seed,
						awaySeed: away.seed,
						prizeSeed,
					};
				}
			}
		}
		return undefined;
	};
	const seedOf = (tid: number): number | undefined => {
		if (!playoffSeries || !Array.isArray(playoffSeries.series)) {
			return undefined;
		}
		for (const matchups of playoffSeries.series) {
			for (const matchup of matchups ?? []) {
				if (matchup?.home?.tid === tid) {
					return matchup.home.seed;
				}
				if (matchup?.away?.tid === tid) {
					return matchup.away?.seed;
				}
			}
		}
		return undefined;
	};

	const buildRecapGame = async (
		game: any,
		effectiveDay: number,
	): Promise<RecapGame> => {
		const playoffs = !!game.playoffs;
		const allStar = isAllStarGame(game);

		const teams = [] as unknown as [RecapTeam, RecapTeam];
		for (const t of game.teams) {
			const info = await teamInfo(t.tid);
			const allPlayers = Array.isArray(t.players) ? t.players : [];

			const injuries: RecapInjuryOut[] = allPlayers
				.filter(
					(p: any) =>
						(p?.min ?? 0) === 0 && p?.injury && p.injury.gamesRemaining > 0,
				)
				.map((p: any) => ({
					name: String(p?.name ?? "Unknown"),
					type: String(p.injury.type ?? "injury"),
					gamesRemaining: p.injury.gamesRemaining ?? 0,
				}));

			const played = allPlayers.filter((p: any) => (p?.min ?? 0) > 0);
			const players: RecapPlayer[] = [];
			for (const p of played) {
				const base: RecapPlayer = {
					name: String(p?.name ?? "Unknown"),
					pid: p?.pid,
					min: Math.round(p?.min ?? 0),
					pts: p?.pts ?? 0,
					reb: (p?.orb ?? 0) + (p?.drb ?? 0),
					ast: p?.ast ?? 0,
					stl: p?.stl ?? 0,
					blk: p?.blk ?? 0,
					tov: p?.tov ?? 0,
					fg: p?.fg ?? 0,
					fga: p?.fga ?? 0,
					tp: p?.tp ?? 0,
					tpa: p?.tpa ?? 0,
					ft: p?.ft ?? 0,
					fta: p?.fta ?? 0,
					pf: p?.pf ?? 0,
					pm: typeof p?.pm === "number" ? p.pm : undefined,
					injury:
						p?.injury && (p.injury.newThisGame || p.injury.playingThrough)
							? {
									type: String(p.injury.type ?? "injury"),
									gamesRemaining: p.injury.gamesRemaining ?? 0,
									newThisGame: !!p.injury.newThisGame,
									playingThrough: !!p.injury.playingThrough,
								}
							: undefined,
				};
				if (typeof p?.pid === "number") {
					const lines = linesByPid.get(p.pid) ?? [];
					base.seasonAvg = enteringAverages(
						lines,
						game.gid,
						game.day ?? effectiveDay,
						false,
					);
					if (playoffs) {
						base.playoffAvg = enteringAverages(
							lines,
							game.gid,
							game.day ?? effectiveDay,
							true,
						);
					}
				}
				players.push(base);
			}

			teams.push({
				tid: t.tid,
				region: info?.region ?? "",
				name: info?.name ?? "Team",
				abbrev: info?.abbrev ?? "???",
				pts: t.pts,
				players,
				record: allStar
					? undefined
					: regularSeasonRecordAsOf(t.tid, game.day ?? effectiveDay, allGames),
				ptsQtrs: Array.isArray(t.ptsQtrs) ? t.ptsQtrs : undefined,
				last10: allStar
					? undefined
					: await last10For(t.tid, game.day ?? effectiveDay),
				streak: allStar
					? undefined
					: streakFor(t.tid, game.day ?? effectiveDay),
				injuries: injuries.length > 0 ? injuries : undefined,
				seed: playoffs ? seedOf(t.tid) : undefined,
			});
		}

		const playIn = playoffs ? await playInForGame(game) : undefined;

		let spread: RecapGame["spread"];
		// Stored at sim time when available (synergy-aware); re-derived ovr-only
		// for legacy games.
		const rawSpread =
			game.spread ??
			getGameSpread({
				ovr0: game.teams[0].ovr,
				ovr1: game.teams[1].ovr,
				homeCourtAdvantage: g.get("homeCourtAdvantage"),
				neutralSite: !!game.neutralSite,
				numPeriods: game.numPeriods ?? g.get("numPeriods"),
				quarterLength: g.get("quarterLength"),
				playoffs,
			});
		if (rawSpread !== undefined) {
			// A half point, because it gets spoken - see the other site above.
			const points = roundHalf(rawSpread);
			spread =
				points >= 0
					? { favTid: game.teams[0].tid, points }
					: { favTid: game.teams[1].tid, points: -points };
		}

		return {
			gid: game.gid,
			day: game.day ?? effectiveDay,
			overtimes: game.overtimes ?? 0,
			winnerTid: game.won.tid,
			playoffs,
			teams,
			series: playoffs && !playIn ? await seriesForGame(game) : undefined,
			playIn,
			spread: allStar ? undefined : spread,
			clutchPlays: Array.isArray(game.clutchPlays) ? game.clutchPlays : [],
			allStar: allStar ? allStarPayload() : undefined,
		};
	};

	return { allGames, buildRecapGame };
};

// The full standings, split by conference, AS OF a given day - the league context
// a day recap needs. Mirrors the standings block in getDayGamesForRecap.
const computeStandingsAsOf = async (
	season: number,
	day: number,
	allGames: Awaited<ReturnType<typeof idb.getCopies.games>>,
): Promise<RecapDayStandings> => {
	const allTeams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
	const confs = g.get("confs", season);
	return {
		day,
		confs: confs.map((conf) => {
			const rows = allTeams
				.filter((t) => t.cid === conf.cid)
				.map((t) => {
					const rec = regularSeasonRecordAsOf(t.tid, day, allGames);
					return {
						abbrev: t.abbrev,
						region: t.region,
						name: t.name,
						won: rec.won,
						lost: rec.lost,
					};
				})
				.sort((a, b) => {
					const wpA = a.won + a.lost > 0 ? a.won / (a.won + a.lost) : 0;
					const wpB = b.won + b.lost > 0 ? b.won / (b.won + b.lost) : 0;
					return wpB - wpA || b.won - a.won || a.abbrev.localeCompare(b.abbrev);
				});
			const leader = rows[0];
			return {
				name: conf.name,
				teams: rows.map((t, i) => ({
					rank: i + 1,
					abbrev: t.abbrev,
					region: t.region,
					name: t.name,
					won: t.won,
					lost: t.lost,
					gb: leader ? (leader.won - t.won + (t.lost - leader.lost)) / 2 : 0,
				})),
			};
		}),
	};
};

// Every completed game on a league day, each with its auto recap (keyed by gid),
// plus an auto recap for the whole day. Used by the Daily Schedule.
// Every completed game of one day, built into the shape a recap is written
// from. Exported because the recaps are only as good as this is: a corpus run
// holds each number in the finished prose against the box score it came out of
// (see recapCorpus.test.ts), and that needs both halves.
export const recapGamesForDay = async ({
	season,
	day,
}: {
	season: number;
	day: number;
}): Promise<RecapGame[]> => {
	const ctx = await createAutoRecapContext(season);
	const dayGames = ctx.allGames
		.filter((game) => (game.day ?? 0) === day && game.won && game.lost)
		.sort((a, b) => a.gid - b.gid);
	const games: RecapGame[] = [];
	for (const game of dayGames) {
		games.push(await ctx.buildRecapGame(game, day));
	}
	return games;
};

export const getAutoRecapsForDay = async ({
	season,
	day,
}: {
	season: number;
	day: number;
}): Promise<{ notes: Record<number, string>; dayRecap: string }> => {
	const games = await recapGamesForDay({ season, day });
	if (games.length === 0) {
		return { notes: {}, dayRecap: "" };
	}

	// One night, one pool of phrasing. Each game seeds its own rng, so without
	// this the fourteen recaps pick independently and land on the same verbs over
	// and over - five "got past"es and four "routed"s in a single night.
	const notes: Record<number, string> = {};
	beginRecapBatch();
	try {
		for (const recapGame of games) {
			notes[recapGame.gid] = getAutoRecap(recapGame);
		}
	} finally {
		endRecapBatch();
	}

	const playoffs = games.some((game) => game.playoffs);
	const standings = playoffs
		? undefined
		: await computeStandingsAsOf(
				season,
				day,
				await idb.getCopies.games({ season }, "noCopyCache"),
			);
	const dayRecap = getAutoDayRecap({ season, day, playoffs, games, standings });

	return { notes, dayRecap };
};

// The auto recap for a single completed game, for the box score page.
export const getAutoRecapForGid = async ({
	season,
	gid,
}: {
	season: number;
	gid: number;
}): Promise<string | undefined> => {
	const ctx = await createAutoRecapContext(season);
	const game = ctx.allGames.find((g2) => g2.gid === gid && g2.won && g2.lost);
	if (!game) {
		return undefined;
	}
	return getAutoRecap(await ctx.buildRecapGame(game, game.day ?? 0));
};
