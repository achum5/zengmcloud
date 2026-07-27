import { idb } from "../db/index.ts";
import { g } from "./index.ts";
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
	// Whether this player's note already has a section for the season being
	// written, so the UI can report how much is already done.
	alreadyWritten: boolean;
};

export type RecapPlayerBatch = {
	season: number;
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
	const inSeason = playersAll
		.filter((p: any) => ratingsForSeason(p, season) !== undefined)
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

	const players: RecapPlayer[] = slice.map((p: any) => {
		const seasonRatings = ratingsForSeason(p, season);
		const bornYear = p.born?.year ?? season;

		const statRows: RecapPlayerSeasonStats[] = upTo(p.stats)
			.filter((s: any) => s.gp > 0)
			.map((s: any) => ({
				season: s.season,
				age: s.season - bornYear,
				abbrev: abbrevByTid.get(s.tid) ?? `T${s.tid}`,
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
			alreadyWritten: hasSeasonNote(p.note, season),
		};
	});

	return {
		season,
		batchIndex: clampedIndex,
		batchCount,
		batchSize,
		totalPlayers,
		alreadyWrittenTotal,
		players,
	};
};
