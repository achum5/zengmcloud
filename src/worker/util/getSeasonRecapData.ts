import { idb } from "../db/index.ts";
import { g, helpers } from "./index.ts";
import { PHASE } from "../../common/constants.ts";
import { formatEventText } from "./formatEventText.ts";
import { getHistoryTeam } from "../views/teamHistory.ts";
import { getPlayoffsByConfBySeason } from "../views/frivolitiesTeamSeasons.ts";

// One player's season line for a team-season recap. Regular-season per-game
// averages, plus the postseason line if they played in it.
export type RecapSeasonPlayer = {
	name: string;
	pid: number;
	pos?: string;
	age?: number;
	ovr?: number;
	pot?: number;
	gp: number;
	min: number;
	pts: number;
	trb: number;
	ast: number;
	stl: number;
	blk: number;
	tov: number;
	fgp: number;
	tpp: number;
	ftp: number;
	per?: number;
	playoff?: {
		gp: number;
		pts: number;
		trb: number;
		ast: number;
	};
	// What this player was paid this season, in thousands of dollars. Money is
	// half of why moves happen, so the recap can see who was expensive.
	salary?: number;
	// This season's individual awards ("MVP", "First Team All-League", ...).
	awards?: string[];
	// This player's transactions in the recap window (the prior offseason that
	// built the team + this season): drafted, signed, re-signed, traded, released.
	transactions?: string[];
	// Major injuries in this player's CAREER (50+ games missed), each with the
	// season it happened - context for durability/injury storylines.
	majorInjuries?: { type: string; games: number; season: number }[];
};

// A single prior season in a franchise's history, for context.
export type RecapFranchiseSeason = {
	season: number;
	won: number;
	lost: number;
	result: string; // e.g. "Won finals", "Lost in first round", "Missed playoffs"
};

export type RecapSeasonTeam = {
	tid: number;
	region: string;
	name: string;
	abbrev: string;
	won: number;
	lost: number;
	otl?: number;
	tied?: number;
	ptsPerGame?: number;
	oppPtsPerGame?: number;
	seed?: number;
	madePlayoffs: boolean;
	playoffResult: string; // roundsWonText for THIS season
	// This season's playoff series, round by round: opponent + games won/lost, so
	// the recap states the ACTUAL series length instead of guessing it.
	playoffSeriesResults: {
		round: number;
		opp: string;
		won: number;
		lost: number;
		win: boolean;
	}[];
	players: RecapSeasonPlayer[];
	franchise: {
		championships: number;
		lastChampionship?: number;
		playoffAppearances: number;
		finalsAppearances: number;
		totalWon: number;
		totalLost: number;
		recent: RecapFranchiseSeason[];
	};
	// End-of-season payroll this year and last, in thousands of dollars. A team
	// that shed or took on salary shows up here without having to be inferred
	// from the wording of individual moves.
	payroll?: number;
	priorPayroll?: number;
	// Who the team lost and gained relative to last season's roster - the shape
	// of the turnover, independent of how each individual move was worded.
	departed: string[];
	arrived: string[];
	// Moves that BUILT this season's roster: the prior BBGM year's offseason
	// (draft, re-signings, free agency after that year's playoffs). Framed as this
	// season's offseason. Plain text, OLDEST FIRST, each tagged with the phase it
	// happened in - the recap is told to read them as a sequence, so a move and
	// whatever it paid for stay adjacent.
	offseasonMoves: string[];
	// Trades/cuts/signings made DURING this season, same ordering and tagging.
	inSeasonMoves: string[];
	// How many older moves the per-team cap dropped, so the recap knows the list
	// isn't the whole offseason rather than assuming it is.
	offseasonMovesOmitted?: number;
	inSeasonMovesOmitted?: number;
};

export type RecapSeasonData = {
	season: number;
	// League money settings, in thousands of dollars. Only included when the
	// recap is for the current season - these aren't tracked per season, so
	// quoting them against an old season would be a guess.
	salaryCap?: number;
	luxuryTax?: number;
	minPayroll?: number;
	champ?: { tid: number; region: string; name: string; abbrev: string };
	runnerUp?: { tid: number; region: string; name: string; abbrev: string };
	// League individual-award winners this season (name + team abbrev).
	awards: { label: string; player: string; abbrev?: string }[];
	teams: RecapSeasonTeam[];
	// How many of those teams already have their season note written. When it
	// reaches teams.length the pass is done and comes off the page.
	alreadyWrittenTotal: number;
};

// Offseason phases: everything after the playoffs conclude, in a given BBGM
// year. The free agency / draft / re-signing that happens here shapes the NEXT
// season's rosters (the season doesn't flip until preseason).
const isOffseasonPhase = (phase: number | undefined): boolean =>
	phase !== undefined && phase >= PHASE.DRAFT_LOTTERY;

const TRANSACTION_EVENT_TYPES = new Set([
	"reSigned",
	"release",
	"trade",
	"freeAgent",
	"draft",
	// Deliberately not "refuseToSign": its text is written second-person, to the
	// user ("refuses to sign with you"), and only fires for the human's own team.
]);

// Where in the calendar a move happened. Tagged onto every move so the order
// within an offseason (draft -> re-signings -> free agency) and within a season
// (before/after the deadline) is explicit instead of implied.
const PHASE_LABEL: Record<number, string> = {
	[PHASE.EXPANSION_DRAFT]: "Expansion draft",
	[PHASE.FANTASY_DRAFT]: "Fantasy draft",
	[PHASE.PRESEASON]: "Preseason",
	[PHASE.REGULAR_SEASON]: "Regular season",
	[PHASE.AFTER_TRADE_DEADLINE]: "After trade deadline",
	[PHASE.PLAYOFFS]: "Playoffs",
	[PHASE.DRAFT_LOTTERY]: "Draft lottery",
	[PHASE.DRAFT]: "Draft",
	[PHASE.AFTER_DRAFT]: "After draft",
	[PHASE.RESIGN_PLAYERS]: "Re-signing period",
	[PHASE.FREE_AGENCY]: "Free agency",
};

export const labelMove = (phase: number | undefined, text: string): string => {
	const label = phase === undefined ? undefined : PHASE_LABEL[phase];
	return label ? `[${label}] ${text}` : text;
};

// Per-team cap on how many moves go in the prompt, and per-player cap for the
// moves listed under a player. Both keep the newest, which are the ones closest
// to the season being written about.
const MAX_TEAM_MOVES = 40;
const MAX_PLAYER_MOVES = 12;

// Trim a move list to the cap without disturbing the order it happened in,
// keeping the moves nearest the season and reporting how many fell off.
export const capMoves = (
	lines: string[],
	max: number,
): { lines: string[]; omitted: number } => ({
	lines: lines.slice(-max),
	omitted: Math.max(0, lines.length - max),
});

export type RecapTeamMoves = {
	offseason: string[];
	inSeason: string[];
	offseasonOmitted: number;
	inSeasonOmitted: number;
};

const stripTags = (s: string): string =>
	s
		.replace(/<[^>]*>/g, "")
		.replace(/\s+/g, " ")
		.trim();

// Per-team offseason + in-season transaction lists. The offseason that built
// season S is BBGM's year-(S-1) offseason (phase >= draft lottery), presented as
// season S's offseason. In-season moves are trades/cuts logged during season S.
const gatherMoves = async (
	season: number,
): Promise<{
	byTid: Map<number, RecapTeamMoves>;
	// Every transaction that involves a given player (draft, signing, trade, cut),
	// in chronological order, for that player's block in the recap.
	byPid: Map<number, string[]>;
}> => {
	const byTid = new Map<number, RecapTeamMoves>();
	const byPid = new Map<number, string[]>();
	const push = (tid: number, key: "offseason" | "inSeason", line: string) => {
		let entry = byTid.get(tid);
		if (!entry) {
			entry = {
				offseason: [],
				inSeason: [],
				offseasonOmitted: 0,
				inSeasonOmitted: 0,
			};
			byTid.set(tid, entry);
		}
		entry[key].push(line);
	};
	const pushPid = (pid: number, line: string) => {
		const arr = byPid.get(pid) ?? [];
		arr.push(line);
		byPid.set(pid, arr);
	};

	const classify = async (
		bucket: "offseason" | "inSeason",
		eventSeason: number,
	) => {
		const events = await idb.getCopies.events(
			{ season: eventSeason },
			"noCopyCache",
		);
		for (const event of events) {
			if (!TRANSACTION_EVENT_TYPES.has(event.type)) {
				continue;
			}
			// Offseason bucket wants only offseason-phase events of year S-1;
			// in-season bucket wants only in-season-phase events of year S.
			const offseason = isOffseasonPhase((event as any).phase);
			if (bucket === "offseason" && !offseason) {
				continue;
			}
			if (bucket === "inSeason" && offseason) {
				continue;
			}
			const tids = Array.isArray(event.tids) ? event.tids : [];
			if (tids.length === 0) {
				continue;
			}
			// formatEventText can throw on an old/odd event; skip it rather than
			// failing the whole recap.
			let text = "";
			try {
				text = stripTags(await formatEventText(event));
			} catch (error) {
				console.error("Skipping an event in the season recap", error);
				continue;
			}
			if (!text) {
				continue;
			}
			text = labelMove((event as any).phase, text);
			for (const tid of tids) {
				if (typeof tid === "number" && tid >= 0) {
					push(tid, bucket, text);
				}
			}
			// Same move, attributed to each player it involves.
			const pids = Array.isArray((event as any).pids)
				? (event as any).pids
				: [];
			for (const pid of pids) {
				if (typeof pid === "number" && pid >= 0) {
					pushPid(pid, text);
				}
			}
		}
	};

	// Prior year's offseason built this season; this year's in-season moves.
	await classify("offseason", season - 1);
	await classify("inSeason", season);

	// Leave them in the order they happened. The recap is told to read the moves
	// as a sequence - a dump and the signing it funded, a trade and the minutes
	// that opened up - so reversing them would hide exactly the thing that makes
	// an offseason readable. Cap per bucket, keeping the moves nearest the
	// season, and record how many were dropped so the prompt can say so.
	for (const entry of byTid.values()) {
		const offseason = capMoves(entry.offseason, MAX_TEAM_MOVES);
		const inSeason = capMoves(entry.inSeason, MAX_TEAM_MOVES);
		entry.offseason = offseason.lines;
		entry.offseasonOmitted = offseason.omitted;
		entry.inSeason = inSeason.lines;
		entry.inSeasonOmitted = inSeason.omitted;
	}

	// Per player, same: chronological (offseason build → in-season), newest kept.
	for (const [pid, lines] of byPid) {
		byPid.set(pid, capMoves(lines, MAX_PLAYER_MOVES).lines);
	}

	return { byTid, byPid };
};

// A player's individual awards for a given season, as short labels.
const awardsForSeason = (player: any, season: number): string[] => {
	const awards: string[] = Array.isArray(player?.awards) ? player.awards : [];
	return awards
		.filter((a: any) => a && a.season === season && a.type)
		.map((a: any) => String(a.type));
};

// Everything an AI needs to write a season-in-review for every team in the
// league: each team's record and playoff result, its key players' season (and
// postseason) lines, its franchise history, and the transactions that built and
// shaped the team this season - with the offseason correctly attributed across
// BBGM's preseason year-flip.
export const getSeasonRecapData = async (
	season: number,
): Promise<RecapSeasonData> => {
	let numPlayoffRounds = 4;
	try {
		numPlayoffRounds = g.get("numGamesPlayoffSeries", season).length;
	} catch {
		// Fall back to a sane default if this season's setting isn't available.
	}

	// Per-team season attrs + team points for/against.
	const teamsPlus = await idb.getCopies.teamsPlus(
		{
			attrs: ["tid"],
			seasonAttrs: [
				"abbrev",
				"region",
				"name",
				"won",
				"lost",
				"tied",
				"otl",
				"playoffRoundsWon",
				"cid",
				"did",
			],
			stats: ["pts", "oppPts", "gp"],
			season,
			// Do NOT add dummy seasons - that would include teams that weren't active
			// this season (not yet expanded, contracted, disabled), each showing up
			// with an empty 0-0 record. Only teams with a real teamSeason for this
			// year are returned.
		},
		"noCopyCache",
	);

	// Franchise history uses the playoffs-by-conf map for the roundsWonText helper.
	// Each team's own teamSeasons history is fetched per-tid below (getCopies
	// requires a tid or season - it can't fetch the whole store at once).
	const playoffsByConfBySeason = await getPlayoffsByConfBySeason();

	// Playoff seeds for this season (first-round matchups carry the seeds). Use
	// getCopy so it works for PAST seasons too (the cache only holds the current
	// one), which also feeds the per-team series results below.
	const playoffSeries = await idb.getCopy.playoffSeries({ season });
	const seedByTid = new Map<number, number>();
	const firstRound = playoffSeries?.series?.[0];
	if (Array.isArray(firstRound)) {
		for (const matchup of firstRound) {
			if (matchup?.home?.tid !== undefined && matchup.home.seed !== undefined) {
				seedByTid.set(matchup.home.tid, matchup.home.seed);
			}
			if (
				matchup?.away?.tid !== undefined &&
				matchup.away?.seed !== undefined
			) {
				seedByTid.set(matchup.away.tid, matchup.away.seed);
			}
		}
	}

	// tid → this season's abbrev, for labeling playoff opponents.
	const abbrevByTid = new Map<number, string>();
	for (const t of teamsPlus) {
		if (t.seasonAttrs?.abbrev) {
			abbrevByTid.set(t.tid, t.seasonAttrs.abbrev);
		}
	}

	// A team's playoff series this season, in order. Walks every round and records
	// the series (opponent + games won/lost) wherever this team appears - handling
	// byes (skipped early rounds) and early exits (only appears until eliminated).
	const seriesResultsForTid = (
		tid: number,
	): RecapSeasonTeam["playoffSeriesResults"] => {
		const out: RecapSeasonTeam["playoffSeriesResults"] = [];
		const rounds = playoffSeries?.series;
		if (!Array.isArray(rounds)) {
			return out;
		}
		for (let r = 0; r < rounds.length; r++) {
			const matchups = rounds[r];
			if (!Array.isArray(matchups)) {
				continue;
			}
			let me: any;
			let opp: any;
			for (const matchup of matchups) {
				if (matchup?.home?.tid === tid) {
					me = matchup.home;
					opp = matchup.away;
					break;
				}
				if (matchup?.away?.tid === tid) {
					me = matchup.away;
					opp = matchup.home;
					break;
				}
			}
			if (!me || !opp) {
				// Not in this round (bye / already eliminated), or a bye matchup.
				continue;
			}
			const meWon = me.won ?? 0;
			const oppWon = opp.won ?? 0;
			out.push({
				round: r + 1,
				opp: opp.abbrev ?? abbrevByTid.get(opp.tid) ?? "???",
				won: meWon,
				lost: oppWon,
				win: meWon > oppWon,
			});
		}
		return out;
	};

	const { byTid: movesByTid, byPid: movesByPid } = await gatherMoves(season);

	// League award winners this season.
	const awardsRow = await idb.getCopy.awards({ season }, "noCopyCache");
	const awards: RecapSeasonData["awards"] = [];
	const pushAward = (label: string, a: any) => {
		if (a && (a.name || a.pid !== undefined)) {
			awards.push({
				label,
				player: a.name ?? `pid ${a.pid}`,
				abbrev: a.abbrev,
			});
		}
	};
	if (awardsRow) {
		pushAward("MVP", awardsRow.mvp);
		pushAward("Finals MVP", awardsRow.finalsMvp);
		pushAward("Defensive Player of the Year", awardsRow.dpoy);
		pushAward("Rookie of the Year", awardsRow.roy);
		pushAward("Sixth Man", awardsRow.smoy);
		pushAward("Most Improved", awardsRow.mip);
	}

	const teams: RecapSeasonTeam[] = [];
	let champ: RecapSeasonData["champ"];
	let runnerUp: RecapSeasonData["runnerUp"];

	for (const t of teamsPlus) {
		try {
			const sa = t.seasonAttrs;
			// Guard: skip any team that wasn't actually active this season (no real
			// season row, so no name/record to write about).
			if (!sa || (sa.region === undefined && sa.name === undefined)) {
				continue;
			}
			const tid = t.tid;
			const teamInfo = {
				tid,
				region: sa.region,
				name: sa.name,
				abbrev: sa.abbrev,
			};

			if (sa.playoffRoundsWon === numPlayoffRounds) {
				champ = teamInfo;
			} else if (sa.playoffRoundsWon === numPlayoffRounds - 1) {
				runnerUp = teamInfo;
			}

			// Roster: players who logged regular-season minutes for this team this year.
			const playersRaw = await idb.getCopies.players(
				{ statsTid: tid },
				"noCopyCache",
			);
			const playersPlus = await idb.getCopies.playersPlus(playersRaw, {
				attrs: ["pid", "firstName", "lastName", "born", "awards", "injuries"],
				ratings: ["pos", "ovr", "pot"],
				stats: [
					"gp",
					"min",
					"pts",
					"trb",
					"ast",
					"stl",
					"blk",
					"tov",
					"fgp",
					"tpp",
					"ftp",
					"per",
				],
				season,
				tid,
				regularSeason: true,
				fuzz: false,
				mergeStats: "totOnly",
			});

			const playoffPlus = await idb.getCopies.playersPlus(playersRaw, {
				attrs: ["pid"],
				stats: ["gp", "pts", "trb", "ast"],
				season,
				tid,
				playoffs: true,
				regularSeason: false,
				fuzz: false,
				mergeStats: "totOnly",
			});
			const playoffByPid = new Map<number, any>();
			for (const p of playoffPlus) {
				if (p.stats && p.stats.gp > 0) {
					playoffByPid.set(p.pid, p.stats);
				}
			}

			// The raw rows carry salaries and retirement info that playersPlus
			// doesn't surface, and we already have them - no extra reads.
			const rawByPid = new Map<number, any>();
			for (const p of playersRaw) {
				rawByPid.set(p.pid, p);
			}
			const salaryFor = (pid: number): number | undefined => {
				const salaries = rawByPid.get(pid)?.salaries;
				if (!Array.isArray(salaries)) {
					return undefined;
				}
				// A player can pick up more than one row for a season (signed, then
				// re-signed); the last one is what they finished the year on.
				let amount: number | undefined;
				for (const row of salaries) {
					if (row?.season === season && typeof row.amount === "number") {
						amount = row.amount;
					}
				}
				return amount;
			};

			const players: RecapSeasonPlayer[] = [];
			for (const p of playersPlus) {
				const st = p.stats;
				if (!st || st.gp === 0) {
					continue;
				}
				const bornYear = p.born?.year;
				const playoff = playoffByPid.get(p.pid);
				players.push({
					name: `${p.firstName} ${p.lastName}`.trim(),
					pid: p.pid,
					pos: p.ratings?.pos,
					age: typeof bornYear === "number" ? season - bornYear : undefined,
					ovr: p.ratings?.ovr,
					pot: p.ratings?.pot,
					gp: st.gp,
					min: Math.round((st.min ?? 0) * 10) / 10,
					pts: Math.round((st.pts ?? 0) * 10) / 10,
					trb: Math.round((st.trb ?? 0) * 10) / 10,
					ast: Math.round((st.ast ?? 0) * 10) / 10,
					stl: Math.round((st.stl ?? 0) * 10) / 10,
					blk: Math.round((st.blk ?? 0) * 10) / 10,
					tov: Math.round((st.tov ?? 0) * 10) / 10,
					fgp: Math.round((st.fgp ?? 0) * 10) / 10,
					tpp: Math.round((st.tpp ?? 0) * 10) / 10,
					ftp: Math.round((st.ftp ?? 0) * 10) / 10,
					per: st.per !== undefined ? Math.round(st.per * 10) / 10 : undefined,
					playoff: playoff
						? {
								gp: playoff.gp,
								pts: Math.round((playoff.pts ?? 0) * 10) / 10,
								trb: Math.round((playoff.trb ?? 0) * 10) / 10,
								ast: Math.round((playoff.ast ?? 0) * 10) / 10,
							}
						: undefined,
					salary: salaryFor(p.pid),
					awards: awardsForSeason(p, season).slice(0, 4),
					transactions: movesByPid.get(p.pid),
					majorInjuries: (Array.isArray(p.injuries) ? p.injuries : [])
						.filter((inj: any) => inj && (inj.games ?? 0) >= 50)
						.map((inj: any) => ({
							type: String(inj.type ?? "injury"),
							games: inj.games,
							season: inj.season,
						})),
				});
			}
			// Best players first (by minutes, a decent proxy for role), capped.
			players.sort((a, b) => b.min * b.gp - a.min * a.gp);
			const topPlayers = players.slice(0, 10);

			// Who left and who showed up since last season. playersRaw is indexed on
			// statsTid, so last season's roster is already in hand. Roster presence
			// (a stats row for the team that year), not games played - a player who
			// missed the whole season with an injury didn't depart.
			const minutesThisSeason = new Map<number, number>();
			const minutesLastSeason = new Map<number, number>();
			for (const p of playersRaw) {
				for (const row of Array.isArray(p.stats) ? p.stats : []) {
					if (row?.tid !== tid || row.playoffs) {
						continue;
					}
					const target =
						row.season === season
							? minutesThisSeason
							: row.season === season - 1
								? minutesLastSeason
								: undefined;
					if (target) {
						target.set(p.pid, (target.get(p.pid) ?? 0) + (row.min ?? 0));
					}
				}
			}
			const nameOf = (pid: number): string => {
				const p = rawByPid.get(pid);
				return p ? `${p.firstName} ${p.lastName}`.trim() : `pid ${pid}`;
			};
			const mostUsedFirst =
				(minutes: Map<number, number>) => (a: number, b: number) =>
					(minutes.get(b) ?? 0) - (minutes.get(a) ?? 0);
			const departed = [...minutesLastSeason.keys()]
				.filter((pid) => !minutesThisSeason.has(pid))
				.sort(mostUsedFirst(minutesLastSeason))
				.slice(0, 12)
				.map((pid) => {
					// Active players carry retiredYear: Infinity, so this only fires on
					// someone who actually hung it up.
					const retiredYear = rawByPid.get(pid)?.retiredYear;
					const retired =
						typeof retiredYear === "number" && retiredYear <= season;
					return `${nameOf(pid)}${retired ? " (retired)" : ""}`;
				});
			const arrived = [...minutesThisSeason.keys()]
				.filter((pid) => !minutesLastSeason.has(pid))
				.sort(mostUsedFirst(minutesThisSeason))
				.slice(0, 12)
				.map(nameOf);

			// Franchise history (this team's seasons up to and including this one).
			const teamSeasons = (
				await idb.getCopies.teamSeasons({ tid }, "noCopyCache")
			)
				.filter((ts) => ts.season <= season)
				.sort((a, b) => a.season - b.season);
			const fh = getHistoryTeam(teamSeasons, playoffsByConfBySeason);
			const recent: RecapFranchiseSeason[] = fh.history
				.filter((h) => h.season < season)
				.slice(0, 6)
				.map((h) => ({
					season: h.season,
					won: h.won,
					lost: h.lost,
					result: h.roundsWonText,
				}));

			// payrollEndOfSeason is -1 until the season is assessed, so only quote it
			// once it's real. Last season's alongside it, so shedding or taking on
			// salary is visible instead of having to be read out of move wording.
			const payrollOf = (s: number): number | undefined => {
				const amount = teamSeasons.find(
					(ts) => ts.season === s,
				)?.payrollEndOfSeason;
				return typeof amount === "number" && amount > 0 ? amount : undefined;
			};

			const teamMoves: RecapTeamMoves = movesByTid.get(tid) ?? {
				offseason: [],
				inSeason: [],
				offseasonOmitted: 0,
				inSeasonOmitted: 0,
			};

			teams.push({
				tid,
				region: sa.region,
				name: sa.name,
				abbrev: sa.abbrev,
				won: sa.won,
				lost: sa.lost,
				otl: sa.otl || undefined,
				tied: sa.tied || undefined,
				ptsPerGame:
					t.stats && t.stats.gp > 0
						? Math.round((t.stats.pts ?? 0) * 10) / 10
						: undefined,
				oppPtsPerGame:
					t.stats && t.stats.gp > 0
						? Math.round((t.stats.oppPts ?? 0) * 10) / 10
						: undefined,
				seed: seedByTid.get(tid),
				madePlayoffs: sa.playoffRoundsWon >= 0,
				playoffResult: helpers.roundsWonText({
					playoffRoundsWon: sa.playoffRoundsWon,
					numPlayoffRounds,
					playoffsByConf: playoffsByConfBySeason.get(season),
					showMissedPlayoffs: true,
				}),
				playoffSeriesResults: seriesResultsForTid(tid),
				players: topPlayers,
				franchise: {
					championships: fh.championships,
					lastChampionship: fh.lastChampionship,
					playoffAppearances: fh.playoffAppearances,
					finalsAppearances: fh.finalsAppearances,
					totalWon: fh.totalWon,
					totalLost: fh.totalLost,
					recent,
				},
				payroll: payrollOf(season),
				priorPayroll: payrollOf(season - 1),
				departed,
				arrived,
				offseasonMoves: teamMoves.offseason,
				inSeasonMoves: teamMoves.inSeason,
				offseasonMovesOmitted: teamMoves.offseasonOmitted || undefined,
				inSeasonMovesOmitted: teamMoves.inSeasonOmitted || undefined,
			});
		} catch (error) {
			// One team's data going wrong shouldn't sink the whole league recap.
			console.error(`Skipping team ${t.tid} in the season recap`, error);
		}
	}

	// Standings order: best record first.
	teams.sort(
		(a, b) =>
			b.won - a.won || a.lost - b.lost || a.region.localeCompare(b.region),
	);

	// A team recap is filed as that team's teamSeason note, so counting the
	// non-empty ones is how far through the season the pass is.
	const written = new Set<number>();
	for (const ts of await idb.getCopies.teamSeasons({ season }, "noCopyCache")) {
		if (typeof ts.note === "string" && ts.note !== "") {
			written.add(ts.tid);
		}
	}
	const alreadyWrittenTotal = teams.filter((t) => written.has(t.tid)).length;

	// The cap settings aren't tracked per season, so they're only trustworthy for
	// the season currently being played. Quoting today's cap against an old
	// season would be a guess dressed up as data.
	const money =
		season === g.get("season")
			? {
					salaryCap: g.get("salaryCap"),
					luxuryTax: g.get("luxuryPayroll"),
					minPayroll: g.get("minPayroll"),
				}
			: {};

	return {
		season,
		...money,
		champ,
		runnerUp,
		awards,
		teams,
		alreadyWrittenTotal,
	};
};
