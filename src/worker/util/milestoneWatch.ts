// WHO IS ABOUT TO PASS WHOM. Every player the league has had, his regular
// season totals for his career and season by season, and from them: the
// active players climbing the league's all-time lists, the ones a game or a
// few from a round number, and the ones on pace for one of the best seasons
// the league has seen. Behind the Milestone Watch page and the line of it on
// a player's page.
//
// Retired players' totals don't change, so they are read once (a long-lived
// league has thousands) and kept until the number of them does; everyone
// else's are worked out fresh from the cache each time. A game still being
// played live somewhere is taken back out, as the rest of the app hides it
// (liveGameGate.ts).

import { PHASE, PLAYER } from "../../common/constants.ts";
import type { Game, Player } from "../../common/types.ts";
import { idb } from "../db/index.ts";
import g from "./g.ts";
import { getLiveGate } from "./liveGameGate.ts";
import { careerOnFile } from "./recapCareer.ts";

export const WATCH_STATS = ["pts", "trb", "ast", "stl", "blk", "tp"] as const;
export type WatchStat = (typeof WATCH_STATS)[number];

type Totals = Record<WatchStat, number> & { gp: number };

type CareerRow = {
	pid: number;
	name: string;
	tid: number;
	active: boolean;
	// Whether his whole career is in the league's files (recapCareer.ts):
	// round numbers are only told of a career that is.
	onFile: boolean;
	career: Totals;
	seasons: Map<number, Totals>;
};

const zero = (): Totals => ({
	pts: 0,
	trb: 0,
	ast: 0,
	stl: 0,
	blk: 0,
	tp: 0,
	gp: 0,
});

const add = (t: Totals, row: Record<string, any>, sign = 1, gp?: number) => {
	t.pts += sign * (row.pts ?? 0);
	t.trb += sign * ((row.orb ?? 0) + (row.drb ?? 0));
	t.ast += sign * (row.ast ?? 0);
	t.stl += sign * (row.stl ?? 0);
	t.blk += sign * (row.blk ?? 0);
	t.tp += sign * (row.tp ?? 0);
	t.gp += sign * (gp ?? row.gp ?? 0);
};

const careerRow = (p: Player, season: number): CareerRow => {
	const career = zero();
	const seasons = new Map<number, Totals>();
	for (const row of p.stats) {
		if (!row || row.playoffs) {
			continue;
		}
		add(career, row);
		let t = seasons.get(row.season);
		if (!t) {
			t = zero();
			seasons.set(row.season, t);
		}
		add(t, row);
	}
	return {
		pid: p.pid,
		name: `${p.firstName} ${p.lastName}`.trim(),
		tid: p.tid,
		active: p.tid !== PLAYER.RETIRED,
		onFile: careerOnFile(p, season, g.get("startingSeason")),
		career,
		seasons,
	};
};

let retiredCache: { key: string; rows: CareerRow[] } | undefined;

const retiredRows = async () => {
	const inDb = await idb.league
		.transaction("players")
		.store.index("tid")
		.count(PLAYER.RETIRED);
	const inCache = (
		await idb.cache.players.indexGetAll("playersByTid", PLAYER.RETIRED)
	).length;
	const key = `${g.get("lid")}:${inDb}:${inCache}`;
	if (retiredCache?.key !== key) {
		const season = g.get("season");
		const rows: CareerRow[] = [];
		await idb.merged.players.forEach({ activeAndRetired: true }, (p) => {
			if (p.tid === PLAYER.RETIRED && p.stats.length > 0) {
				rows.push(careerRow(p, season));
			}
		});
		retiredCache = { key, rows };
	}
	return retiredCache.rows;
};

// Everyone, with this season's games still being played live taken out.
const loadRows = async () => {
	const season = g.get("season");
	const active = (
		await idb.cache.players.indexGetAll("playersByTid", [
			PLAYER.FREE_AGENT,
			Infinity,
		])
	)
		.filter((p) => p.stats.length > 0)
		.map((p) => careerRow(p, season));

	const gate = await getLiveGate();
	if (gate.games.size > 0) {
		const byPid = new Map(active.map((row) => [row.pid, row]));
		for (const game of gate.games.values() as Iterable<Game>) {
			if (game.playoffs || game.season !== season) {
				continue;
			}
			for (const t of game.teams) {
				for (const line of t.players ?? []) {
					const row = byPid.get(line.pid);
					if (!row) {
						continue;
					}
					const played = (line.min ?? 0) > 0 ? 1 : 0;
					add(row.career, line, -1, played);
					const t2 = row.seasons.get(season);
					if (t2) {
						add(t2, line, -1, played);
					}
				}
			}
		}
	}

	return [...(await retiredRows()), ...active];
};

// ------------------------------------------------------------------ THE LISTS

export type WatchPlayer = {
	pid: number;
	name: string;
	tid: number;
};

export type ChaseRow = WatchPlayer & {
	total: number;
	rank: number;
	// The man one place up, and how many it takes to pass him; for the leader,
	// the man behind and the lead.
	next?: WatchPlayer & { total: number; active: boolean };
	behind?: WatchPlayer & { total: number; active: boolean };
};

export type RoundRow = WatchPlayer & {
	stat: WatchStat;
	total: number;
	mark: number;
	// At this season's rate (his career's, early on), about how many games.
	games: number | undefined;
};

export type PaceRow = WatchPlayer & {
	stat: WatchStat;
	total: number;
	projected: number;
	// Where the projection would put the season among every season before.
	place: number;
	record: { pid: number; name: string; season: number; total: number };
};

// The steps a career mark comes in, and the first one worth telling.
const ROUND: Record<WatchStat, { step: number; min: number }> = {
	pts: { step: 5000, min: 10000 },
	trb: { step: 2500, min: 5000 },
	ast: { step: 2500, min: 5000 },
	stl: { step: 500, min: 1000 },
	blk: { step: 500, min: 1000 },
	tp: { step: 500, min: 1000 },
};

// How far off a round number can be, in games at his rate, to be watched.
const ROUND_GAMES = 20;

// How deep in the all-time list a man is worth watching.
const CHASE_DEPTH = 50;

const ranked = (rows: CareerRow[], stat: WatchStat) =>
	rows
		.filter((row) => row.career[stat] > 0)
		.sort((a, b) => b.career[stat] - a.career[stat]);

const player = (row: CareerRow): WatchPlayer => ({
	pid: row.pid,
	name: row.name,
	tid: row.tid,
});

const neighbor = (row: CareerRow | undefined, stat: WatchStat) =>
	row
		? { ...player(row), total: row.career[stat], active: row.active }
		: undefined;

const chaseRow = (list: CareerRow[], i: number, stat: WatchStat): ChaseRow => {
	const row = list[i]!;
	// (Tied, he shares the place of the first man on that total.)
	let rank = i + 1;
	while (rank > 1 && list[rank - 2]!.career[stat] === row.career[stat]) {
		rank -= 1;
	}
	return {
		...player(row),
		total: row.career[stat],
		rank,
		next: rank > 1 ? neighbor(list[rank - 2], stat) : undefined,
		behind: rank === 1 ? neighbor(list[i + 1], stat) : undefined,
	};
};

// A man's rate this season, or his career's before he has ten games in.
const perGame = (row: CareerRow, stat: WatchStat, season: number) => {
	const now = row.seasons.get(season);
	if (now && now.gp >= 10) {
		return now[stat] / now.gp;
	}
	return row.career.gp > 0 ? row.career[stat] / row.career.gp : 0;
};

const roundRow = (
	row: CareerRow,
	stat: WatchStat,
	season: number,
): RoundRow | undefined => {
	if (!row.active || !row.onFile) {
		return undefined;
	}
	const total = row.career[stat];
	const { step, min } = ROUND[stat];
	const mark = Math.max(min, (Math.floor(total / step) + 1) * step);
	const rate = perGame(row, stat, season);
	if (rate <= 0) {
		return undefined;
	}
	const games = Math.ceil((mark - total) / rate);
	if (games > ROUND_GAMES) {
		return undefined;
	}
	return { ...player(row), stat, total, mark, games };
};

// Team games played this season, by tid.
const teamGamesPlayed = async (season: number) => {
	const out = new Map<number, number>();
	for (const ts of await idb.cache.teamSeasons.indexGetAll(
		"teamSeasonsBySeasonTid",
		[[season], [season, "Z"]],
	)) {
		out.set(ts.tid, ts.won + ts.lost + (ts.tied ?? 0) + (ts.otl ?? 0));
	}
	return out;
};

// The best seasons the league has had, before this one, for a stat.
const pastSeasons = (rows: CareerRow[], stat: WatchStat, season: number) => {
	const out: { row: CareerRow; season: number; total: number }[] = [];
	for (const row of rows) {
		for (const [s, t] of row.seasons) {
			if (s < season && t[stat] > 0) {
				out.push({ row, season: s, total: t[stat] });
			}
		}
	}
	return out.sort((a, b) => b.total - a.total);
};

// How far into the season the pace is worth reading, in team games.
const PACE_FROM = 15;
// How high a projected season has to place to be watched.
const PACE_PLACE = 10;

const paceRows = async (rows: CareerRow[], season: number) => {
	if (g.get("phase") !== PHASE.REGULAR_SEASON) {
		return [];
	}
	const numGames = g.get("numGames");
	const played = await teamGamesPlayed(season);
	const out: PaceRow[] = [];
	for (const stat of WATCH_STATS) {
		const past = pastSeasons(rows, stat, season).slice(0, PACE_PLACE);
		const record = past[0];
		if (!record) {
			continue;
		}
		for (const row of rows) {
			const now = row.seasons.get(season);
			const teamGp = played.get(row.tid);
			if (
				!row.active ||
				!now ||
				now.gp < 10 ||
				teamGp === undefined ||
				teamGp < PACE_FROM
			) {
				continue;
			}
			const projected = Math.round(
				now[stat] + (now[stat] / now.gp) * Math.max(0, numGames - teamGp),
			);
			const place = past.filter((s) => s.total >= projected).length + 1;
			if (place > PACE_PLACE) {
				continue;
			}
			out.push({
				...player(row),
				stat,
				total: now[stat],
				projected,
				place,
				record: {
					pid: record.row.pid,
					name: record.row.name,
					season: record.season,
					total: record.total,
				},
			});
		}
	}
	return out.sort((a, b) => a.place - b.place || b.projected - a.projected);
};

// The all-time lists and the record book mean little until the league has
// some history: five seasons of it, its own or brought in with it.
const HISTORY = 5;
const hasHistory = (rows: CareerRow[], season: number) => {
	const seasons = new Set<number>();
	for (const row of rows) {
		for (const s of row.seasons.keys()) {
			if (s < season) {
				seasons.add(s);
				if (seasons.size >= HISTORY) {
					return true;
				}
			}
		}
	}
	return false;
};

export const getMilestoneWatch = async () => {
	const season = g.get("season");
	const rows = await loadRows();
	const history = hasHistory(rows, season);

	const chase = (history ? WATCH_STATS : []).map((stat) => {
		const list = ranked(rows, stat);
		const out: ChaseRow[] = [];
		for (let i = 0; i < Math.min(list.length, CHASE_DEPTH); i++) {
			if (list[i]!.active) {
				out.push(chaseRow(list, i, stat));
			}
		}
		return { stat, rows: out.slice(0, 5) };
	});

	const round: RoundRow[] = [];
	for (const row of rows) {
		for (const stat of WATCH_STATS) {
			const r = roundRow(row, stat, season);
			if (r) {
				round.push(r);
			}
		}
	}
	round.sort(
		(a, b) => (a.games ?? Infinity) - (b.games ?? Infinity) || b.mark - a.mark,
	);

	return {
		season,
		chase,
		round: round.slice(0, 12),
		pace: history ? (await paceRows(rows, season)).slice(0, 8) : [],
	};
};

// ------------------------------------------------------ EVERYONE'S ALL-TIME

export type AllTimeRank = {
	stat: WatchStat;
	total: number;
	// Undefined with none of it.
	rank: number | undefined;
	// How many players are on the list.
	of: number;
	next?: ChaseRow["next"];
	behind?: ChaseRow["behind"];
};

// A player's place on each all-time list, for his page: every player with a
// regular season game or stat to his name, playing or retired. Still playing,
// also the round number he's closest to and a season on a record pace.
export const getPlayerAllTime = async (pid: number) => {
	const season = g.get("season");
	const rows = await loadRows();
	const me = rows.find((row) => row.pid === pid);
	if (
		!me ||
		(me.career.gp === 0 && WATCH_STATS.every((stat) => me.career[stat] === 0))
	) {
		return undefined;
	}

	const ranks: AllTimeRank[] = WATCH_STATS.map((stat) => {
		const list = ranked(rows, stat);
		const i = list.findIndex((row) => row.pid === pid);
		if (i < 0) {
			return { stat, total: 0, rank: undefined, of: list.length };
		}
		const { next, behind, rank } = chaseRow(list, i, stat);
		return {
			stat,
			total: me.career[stat],
			rank,
			of: list.length,
			next,
			behind,
		};
	});

	let round: RoundRow | undefined;
	for (const stat of WATCH_STATS) {
		const r = roundRow(me, stat, season);
		if (r && (!round || (r.games ?? 0) < (round.games ?? 0))) {
			round = r;
		}
	}

	const pace =
		me.active && hasHistory(rows, season)
			? (await paceRows(rows, season)).find((row) => row.pid === pid)
			: undefined;

	return { active: me.active, gp: me.career.gp, ranks, round, pace };
};
