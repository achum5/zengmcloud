import { idb } from "../db/index.ts";
import { g } from "./index.ts";
import { getGameSpread } from "../../common/getGameSpread.ts";
import { getTeamInfoBySeason } from "./getTeamInfoBySeason.ts";

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

// One recap run's game budget. Each recap is several paragraphs of AI output,
// so an unbounded sweep of a whole unrecapped season would blow past any
// model's reply length; whatever doesn't fit stays note-less and the next
// Copy sweeps it up.
const MAX_RECAP_GAMES = 20;

// One recap run also backfills WHOLE-DAY recaps for up to this many days it
// covers (oldest first, FIFO) - so the day recap is never tied to the day the
// user happens to be viewing; it fills in whichever recent days are missing one.
const MAX_RECAP_DAYS = 10;

// Which completed games one recap run covers: every completed game this season
// still missing a recap note - so days that were simmed past get their recaps
// generated in the same run instead of paging through them day by day. Games
// that already have a note are skipped, so a recap is never overwritten. Strict
// FIFO: chronological order, and when the cap bites, the OLDEST games keep
// their slots - a deep backlog is cleared oldest-first across successive runs,
// never leapfrogged by newer days.
export const selectRecapGames = <
	T extends { gid: number; day?: number; note?: unknown },
>(
	completed: T[],
	day: number,
	maxGames: number,
): T[] => {
	return completed
		.filter((game) => !game.note)
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
}): Promise<{ games: RecapGame[]; dayRecapDays: number[] }> => {
	const allGames = await idb.getCopies.games({ season }, "noCopyCache");

	const games = selectRecapGames(
		allGames.filter((game) => game.won && game.lost),
		day,
		MAX_RECAP_GAMES,
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

	// Playoff series lookup for the current postseason.
	const playoffSeries = await idb.cache.playoffSeries.get(season);
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
					name: String(p?.name ?? "Unknown"),
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
				record: regularSeasonRecordAsOf(t.tid, game.day ?? day, allGames),
				ptsQtrs: Array.isArray(t.ptsQtrs) ? t.ptsQtrs : undefined,
				last10: await last10For(t.tid, game.day ?? day),
				streak: streakFor(t.tid, game.day ?? day),
				injuries: injuries.length > 0 ? injuries : undefined,
				seed: playoffs ? seedOf(t.tid) : undefined,
			});
		}

		// A play-in game is a playoff game, but its matchup lives in playIns, not
		// series - classify it first so we tag it as play-in rather than looking
		// (and failing) to find it as a normal series game.
		const playIn = playoffs ? await playInForGame(game) : undefined;

		// The pregame spread (same calc ScoreBox shows), so the recap knows who was
		// favored and can frame an upset/blowout against expectations.
		let spread: RecapGame["spread"];
		const rawSpread = getGameSpread({
			ovr0: game.teams[0].ovr,
			ovr1: game.teams[1].ovr,
			homeCourtAdvantage: g.get("homeCourtAdvantage"),
			neutralSite: !!game.neutralSite,
			numPeriods: game.numPeriods ?? g.get("numPeriods"),
			quarterLength: g.get("quarterLength"),
		});
		if (rawSpread !== undefined) {
			// > 0 → home (teams[0]) favored; < 0 → away favored; 0 → pick'em.
			spread =
				rawSpread >= 0
					? { favTid: game.teams[0].tid, points: rawSpread }
					: { favTid: game.teams[1].tid, points: -rawSpread };
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
			spread,
			clutchPlays: Array.isArray(game.clutchPlays) ? game.clutchPlays : [],
		});
	}

	// Which of the days covered by this batch still need a WHOLE-DAY recap (the
	// day's anchor game - lowest gid of the day - has no dayNote yet), oldest
	// first and capped. This is what lets a paste backfill day recaps for every
	// missed day it covers instead of only the day being viewed.
	const daysInBatch = [...new Set(result.map((game) => game.day))].sort(
		(a, b) => a - b,
	);
	const dayRecapDays = daysInBatch
		.filter((d) => {
			const dayGames = allGames.filter(
				(game) => game.won && game.lost && (game.day ?? 0) === d,
			);
			if (dayGames.length === 0) {
				return false;
			}
			const anchor = dayGames.reduce((a, b) => (a.gid <= b.gid ? a : b));
			return anchor.dayNote === undefined;
		})
		.slice(0, MAX_RECAP_DAYS);

	return { games: result, dayRecapDays };
};
