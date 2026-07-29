import { idb } from "../db/index.ts";
import { g, helpers } from "./index.ts";
import { getPlayoffsByConfBySeason } from "../views/frivolitiesTeamSeasons.ts";
import getAwardCandidates from "../core/season/getAwardCandidates.ts";
import { PHASE, PHASE_TEXT, RATINGS } from "../../common/constants.ts";
import { getGlobalSettings } from "./getGlobalSettings.ts";
import { hasSeasonNote } from "../../common/seasonNote.ts";

// Data for the league-wide "Player Recaps" workflow: a short written recap of
// one season for EVERY player in the league, filed into each player's own note.
//
// The point is to humanize the league - a player whose note reads like a career
// rather than a stat line. So each player carries their whole history, not just
// the season being written: every season's stats AND full ratings, every
// transaction, awards, feats and injuries. The AI needs the arc to say anything
// true about the year.
//
// Delivered in BATCHES because that is far more than fits in one prompt. The
// batch size is a global setting (recapMaxPlayers).
//
// EVERYTHING IS TRUNCATED AT THE SEASON BEING WRITTEN. Backfilling an old year
// with the full record in hand produced recaps full of hindsight - "he'd hang on
// one more year in Vancouver", "by the time he collected a ring in 2002" - which
// reads as prophecy rather than a season recap. Rather than instructing the AI
// not to look ahead (which it will do anyway, because the data is right there),
// the future is simply absent: no later stats, ratings, awards, transactions,
// feats or injuries, and no present-day facts either - the team, contract,
// injury, retirement and Hall of Fame status a player has TODAY are all things
// that had not happened yet.

export type RecapPlayerSeasonStats = {
	season: number;
	age: number;
	abbrev: string;
	// How that team did that year ("45-37, lost in the first round"), so reading
	// a career top to bottom shows what he was playing FOR each season.
	teamResult?: string;
	playoffs: boolean;
	gp: number;
	gs?: number;
	min: number;
	pts: number;
	trb: number;
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
	per?: number;
	ewa?: number;
};

export type RecapPlayerSeasonRatings = {
	season: number;
	age: number;
	pos: string;
	ovr: number;
	pot: number;
	// Every sub-rating for the season, keyed by the sport's rating names.
	ratings: Record<string, number>;
};

export type RecapPlayer = {
	pid: number;
	name: string;
	pos: string;
	age: number;
	born: { year: number; loc: string };
	hgt: number;
	weight: number;
	draft: {
		year: number;
		round: number;
		pick: number;
		originalTid: number;
		abbrev?: string;
	};
	// Team(s) they actually played for THAT season, from that season's stat rows
	// - not p.tid, which is where they are today.
	teamAbbrevs: string[];
	// Only set when they had already retired / been inducted by then.
	retiredYear: number | undefined;
	hof: boolean;
	// Present-day facts, included ONLY when writing the current season.
	contract?: { amount: number; exp: number };
	injury?: { type: string; gamesRemaining: number };
	// Whole career, oldest first.
	stats: RecapPlayerSeasonStats[];
	ratings: RecapPlayerSeasonRatings[];
	// His best single game in each category this season - the concrete detail
	// that makes a recap read like writing instead of a summary.
	seasonHighs?: Record<string, number>;
	// Where he finished in each award race this season, when he was in it at all.
	awardFinishes: { name: string; rank: number }[];
	awards: { season: number; type: string }[];
	transactions: string[];
	injuries: { season: number; type: string; games: number }[];
	feats: { season: number; text: string }[];
	// Only for the season's draft class: where they went and what they joined.
	draftInfo?: RecapDraftInfo;
	// Only for NEXT season's draft class: the scouting profile to write from.
	prospect?: RecapProspect;
	// This is the season they retired after, so the batch asks for a career
	// retrospective alongside the season recap. The whole career is already in
	// the block above; this is the summary a retrospective actually leans on.
	retiring?: RecapRetirement;
	// Whether this player's note already has a section for the season being
	// written, so the UI can report how much is already done.
	alreadyWritten: boolean;
};

// Who a player actually played WITH. Without this the AI can't say a word about
// a player's role - whether he was the first option, who he was deferring to,
// who went down and left him carrying it - which is most of what a season recap
// is about. Sent once per team for the whole batch rather than per player.
export type RecapTeamPlayer = {
	name: string;
	pos: string;
	age: number;
	gp: number;
	min: number;
	pts: number;
	trb: number;
	ast: number;
};

export type RecapLeagueTeam = {
	abbrev: string;
	// The full name that season, so the AI writes "the Toronto Raptors" rather
	// than guessing a city from an abbreviation - and so the name matches when
	// the note is turned into links.
	region?: string;
	name?: string;
	won: number;
	lost: number;
	result: string;
	conf?: string;
	// Top of the rotation by minutes, so a player's block has a supporting cast.
	roster: RecapTeamPlayer[];
};

// A teammate on the roster a rookie landed on, so the AI can talk about fit
// rather than guessing at it.
export type RecapRosterSpot = {
	name: string;
	pos: string;
	age: number;
	ovr: number;
	pot: number;
};

// What a career retrospective leans on: the totals, the span, where he spent
// it. Everything here is derivable from the season rows, but a retrospective
// that has to add up eighteen seasons itself gets them wrong.
export type RecapRetirement = {
	ageAtRetirement: number;
	seasonsPlayed: number;
	firstSeason?: number;
	lastSeason?: number;
	totalGP: number;
	peakOvr: number;
	// Per game across the whole career, regular season and playoffs.
	career?: Record<string, number>;
	playoffs?: Record<string, number>;
	// Every team he suited up for, with the span and games.
	teams: { abbrev: string; from: number; to: number; gp: number }[];
	rings: number;
};

// A player in NEXT season's draft class, written up as a scouting report rather
// than a season recap - he hasn't played a game in this league and has no stats
// to recap. The ratings are the whole basis for the report, which is why the
// full set is here.
export type RecapProspect = {
	draftYear: number;
	// From his own ratings row - a prospect has none for the season being
	// written, so the usual position lookup comes back empty.
	pos: string;
	college: string;
	ovr: number;
	pot: number;
	ratings: Record<string, number>;
};

export type RecapDraftInfo = {
	round: number;
	pick: number;
	overall?: number;
	abbrev: string;
	// The drafting team's season, and the roster the rookie is joining.
	teamResult?: string;
	roster: RecapRosterSpot[];
};

// Players go out in batches, so on its own a block gives the AI no way to know
// whether 24 a night led the league or was thirtieth. Without that it either
// avoids the strongest sentence available or guesses at it. Sent once for the
// batch.
export type RecapStatLeaders = {
	stat: string;
	label: string;
	// Per-game average across everyone who qualified, so the era reads right: 24
	// a night means something different in a league averaging 9.
	leagueAvg: number;
	players: { name: string; abbrev: string; value: number }[];
};

// BBGM doesn't keep ballots - the award formulas ARE the vote - so the order is
// reconstructed by running the same ranking the game used to pick the winner.
export type RecapAwardRace = {
	name: string;
	players: { name: string; abbrev: string }[];
};

export type RecapPlayerBatch = {
	season: number;
	// Which pass this batch belongs to, so the prompt builder knows whether it's
	// writing season recaps or scouting reports.
	filter: RecapFilter;
	// Standings + playoff results for the season being written.
	leagueTeams: RecapLeagueTeam[];
	champion?: string;
	leaders: RecapStatLeaders[];
	awardRaces: RecapAwardRace[];
	batchIndex: number;
	batchCount: number;
	batchSize: number;
	totalPlayers: number;
	// How many of the WHOLE season's players already have this year written.
	alreadyWrittenTotal: number;
	players: RecapPlayer[];
};

const num = (x: unknown): number => (typeof x === "number" ? x : 0);

// Max stats are stored as [value, gid].
const maxValue = (x: unknown): number | undefined =>
	Array.isArray(x) && typeof x[0] === "number" ? x[0] : undefined;

const LEADER_STATS = [
	{ stat: "pts", label: "points" },
	{ stat: "trb", label: "rebounds" },
	{ stat: "ast", label: "assists" },
	{ stat: "stl", label: "steals" },
	{ stat: "blk", label: "blocks" },
	{ stat: "tp", label: "three-pointers" },
	{ stat: "min", label: "minutes" },
] as const;

const LEADER_BOARD_SIZE = 5;

// The best single game a player had in each category this season. Kept to the
// categories anyone writes about - "dropped 51 in March" is the kind of detail
// that makes a recap read like writing, and it is nowhere in a season average.
const HIGH_STATS = ["pts", "trb", "ast", "stl", "blk", "tp"] as const;

const seasonHighsFor = (p: any, season: number) => {
	const out: Record<string, number> = {};
	for (const row of p.stats ?? []) {
		if (row.season !== season || row.playoffs) {
			continue;
		}
		for (const stat of HIGH_STATS) {
			const value = maxValue(row[`${stat}Max`]);
			// A player traded mid-season has a row per team, so take the better.
			if (value !== undefined && value > (out[stat] ?? -1)) {
				out[stat] = value;
			}
		}
	}
	return Object.keys(out).length > 0 ? out : undefined;
};

// A player has to have played a real share of the year to lead it. Taken from
// the longest season anyone actually played rather than from the numGames
// setting, so it works for a season in progress and for any league length.
const QUALIFY_SHARE = 0.4;

// Every player's combined regular-season line for one season. Combined, because
// a player traded in February has a row per team and neither one is his year.
const seasonTotalsByPid = (
	playersAll: any[],
	season: number,
	abbrevFor: (row: any) => string,
) => {
	const out = new Map<
		number,
		{ name: string; abbrev: string; gp: number; totals: Record<string, number> }
	>();
	for (const p of playersAll) {
		let entry:
			| {
					name: string;
					abbrev: string;
					gp: number;
					totals: Record<string, number>;
			  }
			| undefined;
		for (const row of p.stats ?? []) {
			if (row.season !== season || row.playoffs || num(row.gp) <= 0) {
				continue;
			}
			if (!entry) {
				entry = {
					name: `${p.firstName} ${p.lastName}`,
					abbrev: abbrevFor(row),
					gp: 0,
					totals: {},
				};
			}
			entry.gp += num(row.gp);
			for (const { stat } of LEADER_STATS) {
				const value =
					stat === "trb"
						? num(row.trb) || num(row.orb) + num(row.drb)
						: num(row[stat]);
				entry.totals[stat] = (entry.totals[stat] ?? 0) + value;
			}
			// Whoever he finished the year with is the team to name him under.
			entry.abbrev = abbrevFor(row);
		}
		if (entry) {
			out.set(p.pid, entry);
		}
	}
	return out;
};

const buildLeaders = (
	totalsByPid: Map<
		number,
		{ name: string; abbrev: string; gp: number; totals: Record<string, number> }
	>,
): RecapStatLeaders[] => {
	const rows = [...totalsByPid.values()];
	if (rows.length === 0) {
		return [];
	}
	const maxGp = rows.reduce((max, row) => Math.max(max, row.gp), 0);
	const minGp = Math.max(1, Math.round(maxGp * QUALIFY_SHARE));
	const qualified = rows.filter((row) => row.gp >= minGp);
	if (qualified.length === 0) {
		return [];
	}

	return LEADER_STATS.map(({ stat, label }) => {
		const perGame = qualified.map((row) => ({
			name: row.name,
			abbrev: row.abbrev,
			value: Math.round(((row.totals[stat] ?? 0) / row.gp) * 10) / 10,
		}));
		const leagueAvg =
			Math.round(
				(perGame.reduce((sum, row) => sum + row.value, 0) / perGame.length) *
					10,
			) / 10;
		return {
			stat,
			label,
			leagueAvg,
			players: perGame
				.sort((a, b) => b.value - a.value)
				.slice(0, LEADER_BOARD_SIZE),
		};
	});
};

// A player belongs to a season if the league had ratings for them that year -
// which covers everyone who was in the league at all, including players who
// never got off the bench and unsigned free agents. (Stats alone would miss
// them, and the ask was explicitly every player in the league.)
const ratingsForSeason = (p: any, season: number) =>
	p.ratings.find((r: any) => r.season === season);

// The class that will be drafted at the end of NEXT season. Writing 2002, that
// is everyone with draft year 2003 - players nobody in the league has seen play
// a game yet, which is exactly what a scouting report is for.
const isNextDraftClass = (p: any, season: number) =>
	p.draft?.year === season + 1;

// The scouting profile to write from: what he looks like NOW, in the season the
// report is being written, which is the year before his draft. A prospect
// generated for next year's class often has no row until his draft season, so
// that is the fallback - but never anything later, or a report backfilled years
// afterward would describe the player he turned into instead of the one being
// scouted.
const prospectRatings = (p: any, season: number) => {
	const rows = p.ratings ?? [];
	return (
		rows.find((r: any) => r.season === season) ??
		rows.filter((r: any) => r.season <= season + 1).at(-1) ??
		rows[0]
	);
};

// Award finishes, reconstructed by running the same ranking the game itself uses
// to hand out the awards. BBGM stores winners, not ballots, so this is the only
// way to know a player was fourth in MVP - and being fourth in MVP is a season's
// whole story for the player it happened to.
//
// Cached per season because the batches for one season all want the same answer
// and it walks every player in the league to get it.
const AWARD_RACE_DEPTH = 10;
let awardRaceCache:
	| { key: string; races: RecapAwardRace[]; ranks: Map<number, string[]> }
	| undefined;

const getAwardRaces = async (
	season: number,
	abbrevByTid: Map<number, string>,
) => {
	const key = `${g.get("lid")}|${season}|${g.get("phase")}`;
	if (awardRaceCache?.key === key) {
		return awardRaceCache;
	}

	const races: RecapAwardRace[] = [];
	const ranks = new Map<number, string[]>();
	try {
		const candidates = await getAwardCandidates(season);
		for (const race of candidates) {
			const players = race.players.slice(0, AWARD_RACE_DEPTH);
			if (players.length === 0) {
				continue;
			}
			races.push({
				name: race.name,
				players: players.map((p: any) => ({
					name: p.name,
					abbrev: p.abbrev ?? abbrevByTid.get(p.tid) ?? "",
				})),
			});
			for (const [i, p] of players.entries()) {
				const list = ranks.get((p as any).pid) ?? [];
				list.push(`${race.name}|${i + 1}`);
				ranks.set((p as any).pid, list);
			}
		}
	} catch {
		// A season with no stats has no races. Never fail the prompt over it.
	}

	awardRaceCache = { key, races, ranks };
	return awardRaceCache;
};

// From the draft lottery onward, the phase is the offseason that FOLLOWS the
// season it's dated to, so a move made then is for the NEXT year: a player who
// signs in 2002 free agency plays his first game for that team in 2003. Dated
// as bare "2002 free agency" there is no way to tell that from a move made
// during the 2002 season, which leaves the AI unable to explain how a player
// got to the team he's playing for - or dating the move a year early.
const takesEffectNextSeason = (phase: number) => phase >= PHASE.DRAFT_LOTTERY;

const prospectFor = (p: any, season: number): RecapProspect | undefined => {
	if (!isNextDraftClass(p, season)) {
		return undefined;
	}
	const r = prospectRatings(p, season);
	if (!r) {
		return undefined;
	}
	const ratings: Record<string, number> = {};
	for (const key of RATINGS) {
		if (typeof r[key] === "number") {
			ratings[key] = r[key];
		}
	}
	return {
		draftYear: num(p.draft?.year),
		pos: r.pos ?? "",
		college: p.college ?? "",
		ovr: num(r.ovr),
		pot: num(r.pot),
		ratings,
	};
};

export const describeTransaction = (
	t: any,
	abbrevByTid: Map<number, string>,
): string => {
	const team = abbrevByTid.get(t.tid) ?? `team ${t.tid}`;
	const phaseText = PHASE_TEXT[t.phase as keyof typeof PHASE_TEXT] ?? "";
	const when = `${t.season} ${phaseText}${
		takesEffectNextSeason(t.phase) ? ` (for ${t.season + 1})` : ""
	}`.trim();
	switch (t.type) {
		case "draft":
			return `${when}: drafted by ${team}${
				t.pickNum !== undefined ? ` (pick ${t.pickNum})` : ""
			}`;
		case "freeAgent":
			return `${when}: signed with ${team}`;
		case "trade":
			return `${when}: traded to ${team}${
				t.fromTid !== undefined
					? ` from ${abbrevByTid.get(t.fromTid) ?? `team ${t.fromTid}`}`
					: ""
			}`;
		case "godMode":
			return `${when}: moved to ${team} (God Mode)`;
		case "import":
			return `${when}: imported to ${team}`;
		case "sisyphus":
			return `${when}: assigned to ${team}`;
		default:
			return `${when}: ${String(t.type)} (${team})`;
	}
};

// Two separate passes, never one merged batch.
//
// "players" is everyone who was actually in the league that season. "prospects"
// is next year's draft class, who were never in it - no stats, no team, no
// season to recap. Mixing them meant a scouting report and a one-sentence recap
// for a deep-bench player came out of the same prompt under the same length
// rules, and every prospect carried the league standings block it has no use
// for. They are different jobs and they get different prompts.
export type RecapFilter = "players" | "prospects";

export const getPlayerRecapData = async ({
	season,
	batchIndex = 0,
	filter = "players",
}: {
	season: number;
	batchIndex?: number;
	filter?: RecapFilter;
}): Promise<RecapPlayerBatch | undefined> => {
	const globalSettings = await getGlobalSettings();
	const batchSize = Math.max(1, globalSettings.recapMaxPlayers ?? 40);

	const teams = await idb.getCopies.teamsPlus(
		{
			attrs: ["tid"],
			seasonAttrs: ["abbrev"],
			season,
			addDummySeason: true,
		},
		"noCopyCache",
	);
	const abbrevByTid = new Map<number, string>();
	for (const t of teams) {
		abbrevByTid.set(t.tid, (t.seasonAttrs as any)?.abbrev ?? `T${t.tid}`);
	}

	// activeAndRetired is "all except draft prospects", so on its own it leaves
	// out the very players the scouting reports are about: a class that hasn't
	// been drafted yet is still sitting at PLAYER.UNDRAFTED and is invisible to
	// it. Next season's class is fetched separately and merged in - and by draft
	// year rather than by tid, so it works the same whether the draft has
	// happened yet or the season is being backfilled years later.
	const [activeAndRetired, prospects] = await Promise.all([
		idb.getCopies.players({ activeAndRetired: true }, "noCopyCache"),
		idb.getCopies.players({ draftYear: season + 1 }, "noCopyCache"),
	]);
	const byPid = new Map(activeAndRetired.map((p) => [p.pid, p]));
	for (const p of prospects) {
		if (!byPid.has(p.pid)) {
			byPid.set(p.pid, p);
		}
	}
	const playersAll = [...byPid.values()];

	// The pass's players, in a STABLE order so batch N means the same thing
	// between the Copy and the Paste (and across reloads).
	const inSeason = playersAll
		.filter((p: any) => {
			// Next year's draft class is its own pass. Checked first and both ways,
			// so a player can never land in both batches or fall between them.
			const prospect = isNextDraftClass(p, season);
			if (filter === "prospects") {
				return prospect;
			}
			if (prospect) {
				return false;
			}
			// Anyone who retired after this season is included even if the ratings
			// check misses them, since their retirement writeup is written from this
			// batch and there is no second pass that would catch them.
			return (
				ratingsForSeason(p, season) !== undefined || p.retiredYear === season
			);
		})
		.sort((a: any, b: any) => (a.pid ?? 0) - (b.pid ?? 0));

	const totalPlayers = inSeason.length;
	if (totalPlayers === 0) {
		return undefined;
	}

	const batchCount = Math.ceil(totalPlayers / batchSize);
	const clampedIndex = Math.min(Math.max(0, batchIndex), batchCount - 1);
	const slice = inSeason.slice(
		clampedIndex * batchSize,
		(clampedIndex + 1) * batchSize,
	);

	const alreadyWrittenTotal = inSeason.filter((p: any) =>
		hasSeasonNote(p.note, season),
	).length;

	// --- Team context -------------------------------------------------------
	// What each team was doing in each season the batch touches, so a career
	// reads with the stakes attached ("45-37, lost in the first round") rather
	// than as a column of numbers. Fetched by SEASON (all teams at once) rather
	// than per team-season, and only back as far as the batch's oldest career.
	let earliestSeason = season;
	for (const p of slice) {
		for (const row of (p as any).stats ?? []) {
			if (row.season < earliestSeason && row.gp > 0) {
				earliestSeason = row.season;
			}
		}
	}

	const playoffsByConfBySeason = await getPlayoffsByConfBySeason();
	const confNameByCid = new Map<number, string>();
	try {
		for (const conf of g.get("confs", season)) {
			confNameByCid.set(conf.cid, conf.name);
		}
	} catch {
		// Conference names are decoration; the standings still work without them.
	}
	const teamResultByKey = new Map<string, string>();
	const teamAbbrevByKey = new Map<string, string>();
	const leagueTeams: RecapLeagueTeam[] = [];
	let champion: string | undefined;

	for (let yr = earliestSeason; yr <= season; yr += 1) {
		let rows: any[];
		try {
			rows = await idb.getCopies.teamSeasons({ season: yr }, "noCopyCache");
		} catch {
			continue;
		}

		let numPlayoffRounds = 4;
		try {
			numPlayoffRounds = g.get("numGamesPlayoffSeries", yr).length;
		} catch {
			// Fall back if that season's setting isn't available.
		}

		for (const ts of rows) {
			const result = helpers.roundsWonText({
				playoffRoundsWon: ts.playoffRoundsWon,
				numPlayoffRounds,
				playoffsByConf: playoffsByConfBySeason.get(yr),
				showMissedPlayoffs: true,
			});
			const key = `${yr}|${ts.tid}`;
			teamResultByKey.set(key, `${ts.won}-${ts.lost}, ${result}`);
			teamAbbrevByKey.set(key, ts.abbrev ?? `T${ts.tid}`);

			if (yr === season) {
				leagueTeams.push({
					abbrev: ts.abbrev ?? `T${ts.tid}`,
					region: ts.region,
					name: ts.name,
					won: ts.won,
					lost: ts.lost,
					result,
					conf: confNameByCid.get(ts.cid),
					roster: [],
				});
				if (ts.playoffRoundsWon >= numPlayoffRounds) {
					champion = ts.abbrev ?? `T${ts.tid}`;
				}
			}
		}
	}
	leagueTeams.sort((a, b) => b.won - a.won || a.lost - b.lost);

	// Rosters for the season being written, so a rookie's block can show the
	// team he actually landed on. Built from the players already in memory.
	const rosterByTid = new Map<number, RecapRosterSpot[]>();
	for (const p of playersAll as any[]) {
		const r = ratingsForSeason(p, season);
		if (!r) {
			continue;
		}
		const tids = new Set(
			(p.stats ?? [])
				.filter((row: any) => row.season === season && row.gp > 0)
				.map((row: any) => row.tid),
		);
		for (const tid of tids) {
			const list = rosterByTid.get(tid as number) ?? [];
			list.push({
				name: `${p.firstName} ${p.lastName}`,
				pos: r.pos ?? "",
				age: season - (p.born?.year ?? season),
				ovr: num(r.ovr),
				pot: num(r.pot),
			});
			rosterByTid.set(tid as number, list);
		}
	}
	for (const list of rosterByTid.values()) {
		list.sort((a, b) => b.ovr - a.ovr);
	}

	// --- League context, sent once for the whole batch -----------------------
	const abbrevForStatRow = (row: any) =>
		teamAbbrevByKey.get(`${row.season}|${row.tid}`) ??
		abbrevByTid.get(row.tid) ??
		`T${row.tid}`;

	const leaders = buildLeaders(
		seasonTotalsByPid(playersAll as any[], season, abbrevForStatRow),
	);

	const { races: awardRaces, ranks: awardRanks } = await getAwardRaces(
		season,
		abbrevByTid,
	);

	// Who each team actually played, by minutes. This is what lets a recap say
	// anything about a player's ROLE rather than just his numbers.
	const TEAMMATES_SHOWN = 8;
	const seasonRosterByAbbrev = new Map<string, RecapTeamPlayer[]>();
	for (const p of playersAll as any[]) {
		const r = ratingsForSeason(p, season);
		for (const row of p.stats ?? []) {
			if (row.season !== season || row.playoffs || num(row.gp) <= 0) {
				continue;
			}
			const abbrev = abbrevForStatRow(row);
			const gp = num(row.gp);
			const perGame = (v: number) => Math.round((v / gp) * 10) / 10;
			const list = seasonRosterByAbbrev.get(abbrev) ?? [];
			list.push({
				name: `${p.firstName} ${p.lastName}`,
				pos: r?.pos ?? "",
				age: season - (p.born?.year ?? season),
				gp,
				min: perGame(num(row.min)),
				pts: perGame(num(row.pts)),
				trb: perGame(num(row.trb) || num(row.orb) + num(row.drb)),
				ast: perGame(num(row.ast)),
			});
			seasonRosterByAbbrev.set(abbrev, list);
		}
	}
	for (const team of leagueTeams) {
		team.roster = (seasonRosterByAbbrev.get(team.abbrev) ?? [])
			.sort((a, b) => b.min - a.min)
			.slice(0, TEAMMATES_SHOWN);
	}

	// Statistical feats, indexed by pid. One fetch for the whole store rather
	// than per player.
	const featsByPid = new Map<number, { season: number; text: string }[]>();
	try {
		const allFeats = await idb.getCopies.playerFeats(undefined, "noCopyCache");
		for (const feat of allFeats as any[]) {
			const list = featsByPid.get(feat.pid) ?? [];
			list.push({
				season: feat.season,
				// The feat's own summary line if there is one, else a stat digest.
				text:
					typeof feat.name === "string" && typeof feat.stats === "object"
						? `${feat.stats.pts ?? 0} pts, ${feat.stats.trb ?? 0} reb, ${feat.stats.ast ?? 0} ast${feat.won ? " (win)" : " (loss)"}`
						: "notable game",
			});
			featsByPid.set(feat.pid, list);
		}
	} catch {
		// Feats are a bonus; never fail the whole prompt over them.
	}

	// Anything that happened after the season being written must not reach the
	// prompt - see the note at the top of this file.
	const upTo = <T extends { season: number }>(rows: T[] | undefined) =>
		(rows ?? []).filter((row) => row.season <= season);

	const currentSeason = g.get("season");
	const isCurrentSeason = season === currentSeason;

	// Career per-game totals for a retiring player. The AI has every season row,
	// but a retrospective that has to add up eighteen of them itself gets them
	// wrong, so the sums are done here.
	const PER_GAME = [
		"min",
		"pts",
		"trb",
		"ast",
		"stl",
		"blk",
		"tov",
		"fg",
		"fga",
		"tp",
		"tpa",
		"ft",
		"fta",
	] as const;

	const careerLine = (rows: RecapPlayerSeasonStats[]) => {
		const gp = rows.reduce((sum, row) => sum + row.gp, 0);
		if (gp === 0) {
			return undefined;
		}
		const out: Record<string, number> = { gp };
		for (const key of PER_GAME) {
			const total = rows.reduce((sum, row) => sum + row[key], 0);
			out[key] = Math.round((total / gp) * 10) / 10;
		}
		out.fgp = out.fga! > 0 ? Math.round((out.fg! / out.fga!) * 1000) / 10 : 0;
		out.tpp = out.tpa! > 0 ? Math.round((out.tp! / out.tpa!) * 1000) / 10 : 0;
		out.ftp = out.fta! > 0 ? Math.round((out.ft! / out.fta!) * 1000) / 10 : 0;
		return out;
	};

	const players: RecapPlayer[] = slice.map((p: any) => {
		const seasonRatings = ratingsForSeason(p, season);
		const bornYear = p.born?.year ?? season;

		const statRows: RecapPlayerSeasonStats[] = upTo(p.stats)
			.filter((s: any) => s.gp > 0)
			.map((s: any) => ({
				season: s.season,
				age: s.season - bornYear,
				abbrev:
					teamAbbrevByKey.get(`${s.season}|${s.tid}`) ??
					abbrevByTid.get(s.tid) ??
					`T${s.tid}`,
				teamResult: s.playoffs
					? undefined
					: teamResultByKey.get(`${s.season}|${s.tid}`),
				playoffs: !!s.playoffs,
				gp: num(s.gp),
				gs: s.gs,
				min: num(s.min),
				pts: num(s.pts),
				trb: num(s.trb) || num(s.orb) + num(s.drb),
				ast: num(s.ast),
				stl: num(s.stl),
				blk: num(s.blk),
				tov: num(s.tov),
				fg: num(s.fg),
				fga: num(s.fga),
				tp: num(s.tp),
				tpa: num(s.tpa),
				ft: num(s.ft),
				fta: num(s.fta),
				per: s.per,
				ewa: s.ewa,
			}));

		const ratingRows: RecapPlayerSeasonRatings[] = upTo(p.ratings).map(
			(r: any) => {
				const ratings: Record<string, number> = {};
				for (const key of RATINGS) {
					if (typeof r[key] === "number") {
						ratings[key] = r[key];
					}
				}
				return {
					season: r.season,
					age: r.season - bornYear,
					pos: r.pos ?? "",
					ovr: num(r.ovr),
					pot: num(r.pot),
					ratings,
				};
			},
		);

		const awards = upTo(p.awards).map((a: any) => ({
			season: a.season,
			type: a.type,
		}));

		// Where they actually were that season, from that season's stat rows. A
		// player with no games has no known team - p.tid is today's answer, not
		// that year's.
		const teamAbbrevs = [
			...new Set(
				statRows
					.filter((row) => row.season === season)
					.map((row) => row.abbrev),
			),
		];

		const retiredYear =
			typeof p.retiredYear === "number" && p.retiredYear <= season
				? p.retiredYear
				: undefined;

		let retiring: RecapRetirement | undefined;
		if (p.retiredYear === season) {
			const reg = statRows.filter((row) => !row.playoffs);
			const post = statRows.filter((row) => row.playoffs);

			const byTeam = new Map<
				string,
				{ abbrev: string; from: number; to: number; gp: number }
			>();
			for (const row of reg) {
				const existing = byTeam.get(row.abbrev);
				if (existing) {
					existing.from = Math.min(existing.from, row.season);
					existing.to = Math.max(existing.to, row.season);
					existing.gp += row.gp;
				} else {
					byTeam.set(row.abbrev, {
						abbrev: row.abbrev,
						from: row.season,
						to: row.season,
						gp: row.gp,
					});
				}
			}

			retiring = {
				ageAtRetirement: season - bornYear,
				seasonsPlayed: new Set(reg.map((row) => row.season)).size,
				firstSeason: reg[0]?.season,
				lastSeason: reg.at(-1)?.season,
				totalGP: reg.reduce((sum, row) => sum + row.gp, 0),
				peakOvr: ratingRows.reduce((max, r) => Math.max(max, r.ovr), 0),
				career: careerLine(reg),
				playoffs: careerLine(post),
				teams: [...byTeam.values()].sort((a, b) => a.from - b.from),
				rings: awards.filter((a: any) => String(a.type) === "Won Championship")
					.length,
			};
		}

		return {
			pid: p.pid,
			name: `${p.firstName} ${p.lastName}`,
			pos: seasonRatings?.pos ?? "",
			age: season - bornYear,
			born: { year: bornYear, loc: p.born?.loc ?? "" },
			hgt: num(p.hgt),
			weight: num(p.weight),
			draft: {
				year: num(p.draft?.year),
				round: num(p.draft?.round),
				pick: num(p.draft?.pick),
				originalTid: num(p.draft?.originalTid),
				abbrev: abbrevByTid.get(p.draft?.tid),
			},
			teamAbbrevs,
			retiredYear,
			// Induction is an event with a season, so it truncates like the rest -
			// p.hof would announce a Hall of Fame career decades early.
			hof: awards.some((a: any) => String(a.type).includes("Hall of Fame")),
			contract:
				isCurrentSeason && p.contract
					? { amount: p.contract.amount, exp: p.contract.exp }
					: undefined,
			injury:
				isCurrentSeason &&
				p.injury &&
				p.injury.type &&
				p.injury.type !== "Healthy"
					? {
							type: p.injury.type,
							gamesRemaining: num(p.injury.gamesRemaining),
						}
					: undefined,
			stats: statRows,
			ratings: ratingRows,
			seasonHighs: seasonHighsFor(p, season),
			awardFinishes: (awardRanks.get(p.pid) ?? []).map((entry) => {
				const [name, rank] = entry.split("|");
				return { name: name!, rank: Number(rank) };
			}),
			awards,
			transactions: upTo(p.transactions).map((t: any) =>
				describeTransaction(t, abbrevByTid),
			),
			injuries: upTo(p.injuries).map((i: any) => ({
				season: i.season,
				type: i.type,
				games: num(i.games),
			})),
			feats: (featsByPid.get(p.pid) ?? []).filter((f) => f.season <= season),
			draftInfo:
				p.draft?.year === season && p.draft?.round > 0
					? {
							round: num(p.draft.round),
							pick: num(p.draft.pick),
							overall:
								num(p.draft.round) > 0
									? (num(p.draft.round) - 1) * g.get("numActiveTeams") +
										num(p.draft.pick)
									: undefined,
							abbrev: abbrevByTid.get(p.draft.tid) ?? "?",
							teamResult: teamResultByKey.get(`${season}|${p.draft.tid}`),
							// Who he is joining. Capped: the top of the roster is what
							// shapes a rookie's role; the 14th man does not.
							roster: (rosterByTid.get(p.draft.tid) ?? [])
								.filter((spot) => spot.name !== `${p.firstName} ${p.lastName}`)
								.slice(0, 10),
						}
					: undefined,
			prospect: prospectFor(p, season),
			retiring,
			alreadyWritten: hasSeasonNote(p.note, season),
		};
	});

	return {
		season,
		filter,
		leagueTeams,
		champion,
		leaders,
		awardRaces,
		batchIndex: clampedIndex,
		batchCount,
		batchSize,
		totalPlayers,
		alreadyWrittenTotal,
		players,
	};
};
