import { idb } from "../db/index.ts";
import { g, helpers } from "./index.ts";
import { getPlayoffsByConfBySeason } from "../views/frivolitiesTeamSeasons.ts";
import { PHASE_TEXT, RATINGS } from "../../common/constants.ts";
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
	awards: { season: number; type: string }[];
	transactions: string[];
	injuries: { season: number; type: string; games: number }[];
	feats: { season: number; text: string }[];
	// Only for the season's draft class: where they went and what they joined.
	draftInfo?: RecapDraftInfo;
	// This is the season they retired after, so the batch asks for a career
	// retrospective alongside the season recap. The whole career is already in
	// the block above; this is the summary a retrospective actually leans on.
	retiring?: RecapRetirement;
	// Whether this player's note already has a section for the season being
	// written, so the UI can report how much is already done.
	alreadyWritten: boolean;
};

// The league picture for the season being written: every team's record and how
// their year ended. Sent once per prompt rather than per player.
export type RecapLeagueTeam = {
	abbrev: string;
	won: number;
	lost: number;
	result: string;
	conf?: string;
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

export type RecapDraftInfo = {
	round: number;
	pick: number;
	overall?: number;
	abbrev: string;
	// The drafting team's season, and the roster the rookie is joining.
	teamResult?: string;
	roster: RecapRosterSpot[];
};

export type RecapPlayerBatch = {
	season: number;
	// Standings + playoff results for the season being written.
	leagueTeams: RecapLeagueTeam[];
	champion?: string;
	batchIndex: number;
	batchCount: number;
	batchSize: number;
	totalPlayers: number;
	// How many of the WHOLE season's players already have this year written.
	alreadyWrittenTotal: number;
	players: RecapPlayer[];
};

const num = (x: unknown): number => (typeof x === "number" ? x : 0);

// A player belongs to a season if the league had ratings for them that year -
// which covers everyone who was in the league at all, including players who
// never got off the bench and unsigned free agents. (Stats alone would miss
// them, and the ask was explicitly every player in the league.)
const ratingsForSeason = (p: any, season: number) =>
	p.ratings.find((r: any) => r.season === season);

const describeTransaction = (
	t: any,
	abbrevByTid: Map<number, string>,
): string => {
	const team = abbrevByTid.get(t.tid) ?? `team ${t.tid}`;
	const when =
		`${t.season} ${PHASE_TEXT[t.phase as keyof typeof PHASE_TEXT] ?? ""}`.trim();
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

export const getPlayerRecapData = async ({
	season,
	batchIndex = 0,
}: {
	season: number;
	batchIndex?: number;
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

	const playersAll = await idb.getCopies.players(
		{ activeAndRetired: true },
		"noCopyCache",
	);

	// Everyone who was in the league that season, in a STABLE order so batch N
	// means the same thing between the Copy and the Paste (and across reloads).
	// Anyone who retired after this season is included even if the ratings check
	// misses them, since their retirement writeup is written from this batch and
	// there is no second pass that would catch them.
	const inSeason = playersAll
		.filter(
			(p: any) =>
				ratingsForSeason(p, season) !== undefined || p.retiredYear === season,
		)
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
					won: ts.won,
					lost: ts.lost,
					result,
					conf: confNameByCid.get(ts.cid),
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
			retiring,
			alreadyWritten: hasSeasonNote(p.note, season),
		};
	});

	return {
		season,
		leagueTeams,
		champion,
		batchIndex: clampedIndex,
		batchCount,
		batchSize,
		totalPlayers,
		alreadyWrittenTotal,
		players,
	};
};
