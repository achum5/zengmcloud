import { idb } from "../db/index.ts";
import { g, helpers } from "./index.ts";
import { processPlayersHallOfFame } from "./processPlayersHallOfFame.ts";
import { getPlayoffsByConfBySeason } from "../views/frivolitiesTeamSeasons.ts";

// A full stat line (box score + advanced), per game where applicable. Only keys
// with real values are present, so the UI can print whatever exists. Always has
// gp. Kept as a flexible record because we bake the complete stat set - box and
// advanced - into every season and career line.
export type RetiredStatLine = { gp: number } & Record<string, number>;

// One regular season the player played: the full stat line plus the team(s) he
// suited up for that year and how each of those teams' seasons ended.
export type RetiredSeasonLine = {
	season: number;
	age?: number;
	stats: RetiredStatLine;
	teams: { abbrev: string; result: string }[];
};

export type RetiredPlayer = {
	pid: number;
	name: string;
	pos?: string;
	hof: boolean;
	jerseyNumber?: string;
	ageAtRetirement?: number;
	country?: string;
	college?: string;
	heightIn?: number;
	weightLbs?: number;
	draft?: {
		undrafted: boolean;
		round: number;
		pick: number;
		year: number;
	};
	firstSeason?: number;
	lastSeason?: number;
	seasonsPlayed: number;
	totalGP: number;
	neverPlayed: boolean;
	peakOvr?: number;
	career?: RetiredStatLine;
	playoffs?: RetiredStatLine;
	// Every team the player suited up for, with the span and games played.
	teams: { abbrev: string; from: number; to: number; gp: number }[];
	// Regular-season line for each season the player actually played.
	bySeason: RetiredSeasonLine[];
	// Award tally: each distinct award type, how many, and in which seasons.
	awards: { type: string; count: number; seasons: number[] }[];
	rings: number;
};

export type RetiredPlayersData = {
	season: number;
	players: RetiredPlayer[];
};

// Box-score + advanced stats to bake into every line, in display order.
const STAT_KEYS = [
	// Box (per game)
	"min",
	"pts",
	"orb",
	"drb",
	"trb",
	"ast",
	"stl",
	"blk",
	"tov",
	"pf",
	"fg",
	"fga",
	"fgp",
	"tp",
	"tpa",
	"tpp",
	"ft",
	"fta",
	"ftp",
	// Advanced
	"per",
	"tsp",
	"usgp",
	"ortg",
	"drtg",
	"ows",
	"dws",
	"ws",
	"ws48",
	"obpm",
	"dbpm",
	"bpm",
	"vorp",
	"ewa",
	"pm",
] as const;

const r = (x: number): number => Math.round(x * 1000) / 1000;

const statLine = (stats: any): RetiredStatLine | undefined => {
	if (!stats || !stats.gp) {
		return undefined;
	}
	const out: RetiredStatLine = { gp: stats.gp };
	for (const key of STAT_KEYS) {
		const value = stats[key];
		if (typeof value === "number" && !Number.isNaN(value)) {
			out[key] = r(value);
		}
	}
	return out;
};

// Every player who retired in a given season, each with the full career context
// a writer would need for a retirement piece: career and playoff lines, a
// season-by-season log (full box + advanced stats, with each season's team
// result), every team, the biography (draft slot, age, college/country,
// height), award tally, and rings. Depth scales naturally with the player - a
// Hall of Famer carries decades of data, an undrafted washout carries almost
// none - so the prompt lets the writeup length follow the data.
export const getRetiredPlayersForRecap = async (
	season: number,
): Promise<RetiredPlayersData> => {
	const retiredRaw = await idb.getCopies.players(
		{ retiredYear: season },
		"noCopyCache",
	);

	const processed = processPlayersHallOfFame(
		await idb.getCopies.playersPlus(retiredRaw, {
			attrs: [
				"pid",
				"name",
				"born",
				"college",
				"draft",
				"hof",
				"awards",
				"weight",
				"hgt",
				"jerseyNumber",
			],
			ratings: ["pos", "ovr", "pot"],
			stats: ["season", "tid", "abbrev", "age", ...STAT_KEYS],
			playoffs: true,
			combined: false,
			showNoStats: true,
			showRookies: true,
			fuzz: false,
			// Keep BOTH per-team rows and the season TOT row for traded seasons, so a
			// well-traveled player's team list is complete AND per-season lines
			// aren't double-counted.
			mergeStats: "totAndTeams",
		}),
	);

	const playoffsByConfBySeason = await getPlayoffsByConfBySeason();

	// A team's playoff result for a given season ("won finals", "missed
	// playoffs", ...). teamSeasons are fetched once per team and memoized.
	const teamResultsCache = new Map<number, Map<number, number>>();
	const teamResult = async (tid: number, s: number): Promise<string> => {
		let bySeason = teamResultsCache.get(tid);
		if (!bySeason) {
			bySeason = new Map();
			const tss = await idb.getCopies.teamSeasons({ tid }, "noCopyCache");
			for (const ts of tss) {
				bySeason.set(ts.season, ts.playoffRoundsWon);
			}
			teamResultsCache.set(tid, bySeason);
		}
		const playoffRoundsWon = bySeason.get(s);
		if (playoffRoundsWon === undefined) {
			return "";
		}
		return helpers.roundsWonText({
			playoffRoundsWon,
			numPlayoffRounds: g.get("numGamesPlayoffSeries", s).length,
			playoffsByConf: playoffsByConfBySeason.get(s),
			showMissedPlayoffs: true,
		});
	};

	const players: RetiredPlayer[] = [];
	for (const p of processed) {
		const allStats: any[] = Array.isArray(p.stats) ? p.stats : [];
		const regular = allStats.filter((s) => !s.playoffs && s.gp > 0);

		// Group regular-season rows by season. A season the player was traded in
		// has a TOT row (whole-season totals) plus one row per team.
		const bySeasonMap = new Map<number, { tot?: any; teamRows: any[] }>();
		for (const s of regular) {
			const entry = bySeasonMap.get(s.season) ?? { teamRows: [] };
			if (s.abbrev === "TOT") {
				entry.tot = s;
			} else {
				entry.teamRows.push(s);
			}
			bySeasonMap.set(s.season, entry);
		}

		// One line per season (TOT stats if traded), oldest first, each carrying the
		// full stat set and the result of every team the player played for.
		const bySeason: RetiredSeasonLine[] = [];
		for (const [s, entry] of [...bySeasonMap.entries()].sort(
			(a, b) => a[0] - b[0],
		)) {
			const statsRow = entry.tot ?? entry.teamRows[0];
			const stats = statLine(statsRow);
			if (!stats) {
				continue;
			}
			const teams: { abbrev: string; result: string }[] = [];
			for (const tr of entry.teamRows) {
				teams.push({
					abbrev: tr.abbrev,
					result: await teamResult(tr.tid, s),
				});
			}
			bySeason.push({ season: s, age: statsRow?.age, stats, teams });
		}

		// Teams: span + games for each distinct franchise (per-team rows, not TOT).
		const teamMap = new Map<string, { from: number; to: number; gp: number }>();
		for (const s of regular) {
			if (!s.abbrev || s.abbrev === "TOT") {
				continue;
			}
			const entry = teamMap.get(s.abbrev);
			if (entry) {
				entry.from = Math.min(entry.from, s.season);
				entry.to = Math.max(entry.to, s.season);
				entry.gp += s.gp;
			} else {
				teamMap.set(s.abbrev, { from: s.season, to: s.season, gp: s.gp });
			}
		}
		const teams = [...teamMap.entries()]
			.map(([abbrev, v]) => ({ abbrev, ...v }))
			.sort((a, b) => a.from - b.from);

		// Award tally.
		const awardMap = new Map<string, number[]>();
		let rings = 0;
		for (const a of Array.isArray(p.awards) ? p.awards : []) {
			if (!a || typeof a.type !== "string") {
				continue;
			}
			const arr = awardMap.get(a.type) ?? [];
			arr.push(a.season);
			awardMap.set(a.type, arr);
			if (a.type === "Won Championship") {
				rings += 1;
			}
		}
		const awards = [...awardMap.entries()]
			.map(([type, seasons]) => ({
				type,
				count: seasons.length,
				seasons: seasons.sort((x, y) => x - y),
			}))
			.sort((a, b) => b.count - a.count);

		const bornYear = p.born?.year;
		const draft = p.draft;
		const totalGP = p.careerStats?.gp ?? 0;

		players.push({
			pid: p.pid,
			name: p.name,
			pos: p.bestPos,
			hof: !!p.hof,
			jerseyNumber: p.jerseyNumber,
			ageAtRetirement:
				typeof bornYear === "number" ? season - bornYear : undefined,
			country: p.born?.loc || undefined,
			college: p.college || undefined,
			heightIn: p.hgt,
			weightLbs: p.weight,
			draft: draft
				? {
						undrafted: !draft.round || draft.round === 0,
						round: draft.round,
						pick: draft.pick,
						year: draft.year,
					}
				: undefined,
			firstSeason: bySeason[0]?.season,
			lastSeason: bySeason.at(-1)?.season,
			seasonsPlayed: bySeason.length,
			totalGP,
			neverPlayed: totalGP === 0,
			peakOvr: p.peakOvr || undefined,
			career: statLine(p.careerStats),
			playoffs: statLine(p.careerStatsPlayoffs),
			teams,
			bySeason,
			awards,
			rings,
		});
	}

	// Most accomplished first: HoF, then rings, then career scoring.
	players.sort(
		(a, b) =>
			Number(b.hof) - Number(a.hof) ||
			b.rings - a.rings ||
			(b.career?.pts ?? 0) * (b.career?.gp ?? 0) -
				(a.career?.pts ?? 0) * (a.career?.gp ?? 0),
	);

	return { season, players };
};
