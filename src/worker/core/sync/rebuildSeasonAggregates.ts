// RECORDS AND TEAM TOTALS, REBUILT FROM THE GAMES.
//
// A league-mate's record went from 39-22 to 38-23 in one game: a loss was
// added and a win was taken away. The device that simmed the loss held a
// season row that did not yet include the win - it had the game, but its own
// copy of the team's row was a game behind - and it published that row WHOLE.
// Rows here replicate whole, last writer wins, so the correct 39-22 was
// overwritten by a stale 38-23 and every device agreed on the wrong number
// from then on. The friend counted 81 games on the schedule; there were 82,
// and one win was gone.
//
// The guard on incoming rows cannot see this: it compares games played, and
// 39-22 and 38-23 are both 61. It is not made stricter here on purpose. A
// declined row triggers a full ordered re-read of the log, and the stale row
// is the newest write in that log, so the re-read lands it anyway - all cost,
// no cure. Accepting the row and correcting it is cheaper and actually works.
//
// What every device DOES agree on is the games. A game is written once, under
// a league-unique gid, and never rewritten, so the games store is complete on
// every device that is caught up, and every counter the sim increments as it
// plays is a pure function of it. So this rebuilds them from it: the record
// fields of a team's season row and the additive totals of its stats row, for
// the current season, exactly as writeTeamStats would have produced them had
// every device seen every game in order. It runs after any remote changeset
// that brought games or season rows, writes only rows whose values actually
// differ - on a healthy league it writes nothing - and the writes are captured
// so the correction publishes back to the room and the stale author converges
// too. The rebuild is a pure function of the games, so it settles instead of
// echoing.
//
// One thing it never does is COUNT DOWN. A rebuild that finds fewer games
// than a row already counted has, far more likely than a phantom game, a
// games store with a hole in it; writing and publishing that row would spread
// the hole to every device. Such rows are held and reported instead.
//
// It rebuilds ONLY what is derivable. Hype, attendance, revenues and expenses
// are also incremented per game but depend on state at the time (ticket
// prices, budgets) that a stored game does not carry; those stay as they are.
// Player season rows are derivable too but carry more shape (rows per team
// per player, `minAvailable` accrued while healthy but not playing), so they
// are AUDITED here - counted against the box scores and reported - and not
// yet written.

import { bySport, isSport } from "../../../common/sportFunctions.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import teamStatsKeys from "../team/stats.ts";

// ---------------------------------------------------------------- RECORDS

export type RecordFields = {
	won: number;
	lost: number;
	tied: number;
	otl: number;
	wonHome: number;
	lostHome: number;
	tiedHome: number;
	otlHome: number;
	wonAway: number;
	lostAway: number;
	tiedAway: number;
	otlAway: number;
	wonDiv: number;
	lostDiv: number;
	tiedDiv: number;
	otlDiv: number;
	wonConf: number;
	lostConf: number;
	tiedConf: number;
	otlConf: number;
	lastTen: (-1 | 0 | 1 | "OTL")[];
	streak: number;
	// Home games INCLUDING playoffs - used for attendance averages.
	gpHome: number;
};

export const RECORD_KEYS = [
	"won",
	"lost",
	"tied",
	"otl",
	"wonHome",
	"lostHome",
	"tiedHome",
	"otlHome",
	"wonAway",
	"lostAway",
	"tiedAway",
	"otlAway",
	"wonDiv",
	"lostDiv",
	"tiedDiv",
	"otlDiv",
	"wonConf",
	"lostConf",
	"tiedConf",
	"otlConf",
	"lastTen",
	"streak",
	"gpHome",
] as const;

// The slice of a stored game this needs. Kept minimal so tests can build one
// by hand and so nothing here depends on box-score shape it does not read.
export type GameForRecords = {
	gid: number;
	day?: number;
	season: number;
	playoffs?: boolean;
	overtimes: number;
	won: { tid: number; pts: number };
	lost: { tid: number; pts: number };
	teams: [{ tid: number }, { tid: number }];
};

const emptyRecord = (): RecordFields => ({
	won: 0,
	lost: 0,
	tied: 0,
	otl: 0,
	wonHome: 0,
	lostHome: 0,
	tiedHome: 0,
	otlHome: 0,
	wonAway: 0,
	lostAway: 0,
	tiedAway: 0,
	otlAway: 0,
	wonDiv: 0,
	lostDiv: 0,
	tiedDiv: 0,
	otlDiv: 0,
	wonConf: 0,
	lostConf: 0,
	tiedConf: 0,
	otlConf: 0,
	lastTen: [],
	streak: 0,
	gpHome: 0,
});

const isAllStarGame = (game: GameForRecords) =>
	game.teams[0].tid === -1 && game.teams[1].tid === -2;

// The games in the order they were played. Day first, then gid, which is the
// order the sim wrote them in - and the order streak and last-ten depend on.
export const inPlayedOrder = <T extends { gid: number; day?: number }>(
	games: readonly T[],
): T[] =>
	[...games].sort((a, b) => (a.day ?? 0) - (b.day ?? 0) || a.gid - b.gid);

// Regular-season games a row has counted.
export const regularSeasonPlayed = (row: {
	won?: number;
	lost?: number;
	tied?: number;
	otl?: number;
}) => (row.won ?? 0) + (row.lost ?? 0) + (row.tied ?? 0) + (row.otl ?? 0);

// One team's record for one season, replayed from its games. Mirrors the
// bookkeeping in writeTeamStats line for line: regular-season games only for
// the record itself, every home game for gpHome, division and conference by
// the two teams' ids at the time, overtime losses when the league counts them.
export const rebuildRecord = ({
	tid,
	season,
	games,
	divisionOf,
	conferenceOf,
	otl,
}: {
	tid: number;
	season: number;
	games: readonly GameForRecords[];
	divisionOf: (tid: number) => number | undefined;
	conferenceOf: (tid: number) => number | undefined;
	// Whether an overtime loss is recorded as OTL rather than a loss.
	otl: boolean;
}): RecordFields => {
	const record = emptyRecord();
	const mine = inPlayedOrder(
		games.filter(
			(game) =>
				game.season === season &&
				!isAllStarGame(game) &&
				(game.teams[0].tid === tid || game.teams[1].tid === tid),
		),
	);

	for (const game of mine) {
		const home = game.teams[0].tid === tid;
		if (home) {
			record.gpHome += 1;
		}
		if (game.playoffs) {
			continue;
		}

		const opponent = home ? game.teams[1].tid : game.teams[0].tid;
		const sameDiv =
			divisionOf(tid) !== undefined && divisionOf(tid) === divisionOf(opponent);
		const sameConf =
			conferenceOf(tid) !== undefined &&
			conferenceOf(tid) === conferenceOf(opponent);
		const tie = game.won.pts === game.lost.pts;
		const won = !tie && game.won.tid === tid;
		const side = home ? "Home" : "Away";

		if (record.lastTen.length === 10) {
			record.lastTen.pop();
		}

		if (won) {
			record.won += 1;
			if (sameDiv) {
				record.wonDiv += 1;
			}
			if (sameConf) {
				record.wonConf += 1;
			}
			record[`won${side}`] += 1;
			record.lastTen.unshift(1);
			record.streak = record.streak >= 0 ? record.streak + 1 : 1;
		} else if (!tie) {
			const kind = game.overtimes > 0 && otl ? "otl" : "lost";
			record[kind] += 1;
			if (sameDiv) {
				record[`${kind}Div`] += 1;
			}
			if (sameConf) {
				record[`${kind}Conf`] += 1;
			}
			record[`${kind}${side}`] += 1;
			record.lastTen.unshift(kind === "lost" ? 0 : "OTL");
			record.streak = record.streak <= 0 ? record.streak - 1 : -1;
		} else {
			record.tied += 1;
			if (sameDiv) {
				record.tiedDiv += 1;
			}
			if (sameConf) {
				record.tiedConf += 1;
			}
			record[`tied${side}`] += 1;
			record.lastTen.unshift(-1);
			record.streak = 0;
		}
	}

	return record;
};

export const recordDiffers = (
	row: Record<string, unknown>,
	rebuilt: RecordFields,
): boolean =>
	RECORD_KEYS.some((key) => {
		const have = row[key];
		const want = rebuilt[key];
		if (key === "lastTen") {
			return JSON.stringify(have ?? []) !== JSON.stringify(want);
		}
		return (have ?? 0) !== want;
	});

// ---------------------------------------------------------------- TEAM STATS

type GameTeamStats = Record<string, unknown> & { tid: number };

export type GameForStats = GameForRecords & {
	teams: [GameTeamStats, GameTeamStats];
};

// Keys the sim never adds into the team stats row from a game.
const SKIP_KEYS = new Set<string>(
	bySport({
		basketball: ["ptsQtrs", "gp", "ba"],
		default: ["ptsQtrs", "gp"],
	}),
);

const IDENTITY_KEYS = new Set(["rid", "tid", "season", "playoffs"]);

const isStatValue = (value: unknown) =>
	typeof value === "number" || Array.isArray(value);

// "oppFg" -> "fg"; undefined for a key that is not an opponent key.
const oppBase = (key: string) =>
	key.startsWith("opp") && key.length > 3
		? key[3]!.toLowerCase() + key.slice(4)
		: undefined;

// The keys of a stats row that the sim adds into per game, which is what a
// replay can rebuild. Identity keys and gp are handled apart; derived keys
// (per, ortg, ...) are computed at read time and never touched by the sim;
// keys the sim skips (ptsQtrs, ba) and opponent keys with no per-game source
// (there is no oppMin) are left exactly as stored.
export const rebuildableKeys = (row: Record<string, unknown>): string[] => {
	const derived = new Set<string>(teamStatsKeys.derived);
	return Object.keys(row).filter((key) => {
		if (
			IDENTITY_KEYS.has(key) ||
			derived.has(key) ||
			key === "gp" ||
			SKIP_KEYS.has(key) ||
			!isStatValue(row[key])
		) {
			return false;
		}
		const base = oppBase(key);
		return base === undefined || (base !== "min" && !SKIP_KEYS.has(base));
	});
};

// One team's stats row for one (season, playoffs), replayed from its games.
// Only keys the row already carries are rebuilt, and only the ones the sim
// adds per game: an "opp" key is the opponent's base key, a football `Lng` is
// a maximum, a by-position key is summed element-wise, everything else is a
// sum. Hockey's shutouts are counted from the score, as the sim counts them.
export const rebuildTeamStatsRow = ({
	row,
	tid,
	season,
	playoffs,
	games,
}: {
	row: Record<string, unknown>;
	tid: number;
	season: number;
	playoffs: boolean;
	games: readonly GameForStats[];
}): Record<string, unknown> => {
	const byPos = new Set<string>(teamStatsKeys.byPos ?? []);
	const mine = inPlayedOrder(
		games.filter(
			(game) =>
				game.season === season &&
				(game.playoffs === true) === playoffs &&
				!isAllStarGame(game) &&
				(game.teams[0].tid === tid || game.teams[1].tid === tid),
		),
	);

	const out: Record<string, unknown> = { ...row };
	const keys = rebuildableKeys(row);
	for (const key of keys) {
		out[key] = byPos.has(key) ? [] : 0;
	}
	out.gp = 0;

	const add = (key: string, value: unknown) => {
		if (byPos.has(key)) {
			if (!Array.isArray(value)) {
				return;
			}
			const arr = out[key] as unknown[];
			for (const [i, v] of value.entries()) {
				if (typeof v === "number") {
					arr[i] = ((arr[i] as number | undefined) ?? 0) + v;
				}
			}
			return;
		}
		if (typeof value !== "number") {
			return;
		}
		if (isSport("football") && key.endsWith("Lng")) {
			out[key] = Math.max(out[key] as number, value);
		} else {
			out[key] = (out[key] as number) + value;
		}
	};

	for (const game of mine) {
		const us = game.teams[0].tid === tid ? game.teams[0] : game.teams[1];
		const them = game.teams[0].tid === tid ? game.teams[1] : game.teams[0];
		for (const key of keys) {
			const base = oppBase(key);
			add(key, base === undefined ? us[key] : them[base]);
		}
		if (isSport("hockey")) {
			if (keys.includes("so") && them.pts === 0) {
				out.so = (out.so as number) + 1;
			}
			if (keys.includes("oppSo") && us.pts === 0) {
				out.oppSo = (out.oppSo as number) + 1;
			}
		}
		out.gp = (out.gp as number) + 1;
	}
	return out;
};

export const statsRowDiffers = (
	row: Record<string, unknown>,
	rebuilt: Record<string, unknown>,
): boolean =>
	Object.keys(rebuilt).some(
		(key) =>
			!IDENTITY_KEYS.has(key) &&
			JSON.stringify(row[key] ?? null) !== JSON.stringify(rebuilt[key] ?? null),
	);

// ---------------------------------------------------------------- THE RUNNER

type RecordChange = { tid: number; before: string; after: string };

export type RebuildReport = {
	season: number;
	teamsChecked: number;
	recordsFixed: RecordChange[];
	// Rows the games could not justify (they count more games than exist
	// here). Left alone - see the header.
	recordsHeld: RecordChange[];
	statsRowsFixed: number;
	statsRowsHeld: number;
	// Player rows whose games played, minutes or points disagree with the box
	// scores. Reported, not written - see the header.
	playerRowsSuspect: number;
};

const fmt = (row: Record<string, unknown>) =>
	`${row.won ?? 0}-${row.lost ?? 0}${(row.tied as number) ? `-${row.tied}` : ""}${(row.otl as number) ? `-${row.otl}` : ""}`;

export const rebuildSeasonAggregates = async ({
	tids,
	auditPlayers = true,
}: {
	// Limit to these teams; undefined rebuilds every team in the season.
	tids?: readonly number[];
	auditPlayers?: boolean;
} = {}): Promise<RebuildReport> => {
	const season = g.get("season");
	const otl = g.get("otl") === true;
	// The cache holds the current season's games, which is the season a live
	// row can be wrong about.
	const games = (await idb.cache.games.getAll()).filter(
		(game) => game.season === season,
	) as unknown as GameForStats[];

	const seasonRows = (await idb.cache.teamSeasons.indexGetAll(
		"teamSeasonsBySeasonTid",
		[[season], [season, "Z"]],
	)) as Record<string, any>[];
	const divisionOf = (tid: number) =>
		seasonRows.find((row) => row.tid === tid)?.did;
	const conferenceOf = (tid: number) =>
		seasonRows.find((row) => row.tid === tid)?.cid;

	const wanted = tids === undefined ? undefined : new Set(tids);
	const report: RebuildReport = {
		season,
		teamsChecked: 0,
		recordsFixed: [],
		recordsHeld: [],
		statsRowsFixed: 0,
		statsRowsHeld: 0,
		playerRowsSuspect: 0,
	};

	for (const row of seasonRows) {
		if (wanted && !wanted.has(row.tid)) {
			continue;
		}
		report.teamsChecked += 1;
		const rebuilt = rebuildRecord({
			tid: row.tid,
			season,
			games,
			divisionOf,
			conferenceOf,
			otl,
		});
		if (recordDiffers(row, rebuilt)) {
			const change = { tid: row.tid, before: fmt(row), after: fmt(rebuilt) };
			if (regularSeasonPlayed(rebuilt) < regularSeasonPlayed(row)) {
				report.recordsHeld.push(change);
			} else {
				await changeTracker.runCaptured(async () => {
					Object.assign(row, rebuilt);
					await idb.cache.teamSeasons.put(row as any);
				});
				report.recordsFixed.push(change);
			}
		}

		for (const playoffs of [false, true]) {
			const statsRow = (await idb.cache.teamStats.indexGet(
				"teamStatsByPlayoffsTid",
				[playoffs, row.tid],
			)) as Record<string, unknown> | undefined;
			if (!statsRow || statsRow.season !== season) {
				continue;
			}
			const rebuiltStats = rebuildTeamStatsRow({
				row: statsRow,
				tid: row.tid,
				season,
				playoffs,
				games,
			});
			if (statsRowDiffers(statsRow, rebuiltStats)) {
				if ((rebuiltStats.gp as number) < ((statsRow.gp as number) ?? 0)) {
					report.statsRowsHeld += 1;
				} else {
					await changeTracker.runCaptured(async () => {
						await idb.cache.teamStats.put(rebuiltStats as any);
					});
					report.statsRowsFixed += 1;
				}
			}
		}
	}

	if (auditPlayers) {
		report.playerRowsSuspect = await auditPlayerRows({ season, games, wanted });
	}

	return report;
};

// Games played, minutes and points per (player, team, playoffs) from the box
// scores, against the player's own season rows. A disagreement means the same
// lost update hit a player row; it is counted so the field can say how often,
// which is what decides whether the rebuild is extended to them.
const auditPlayerRows = async ({
	season,
	games,
	wanted,
}: {
	season: number;
	games: readonly GameForStats[];
	wanted: Set<number> | undefined;
}): Promise<number> => {
	type Sum = { gp: number; min: number; pts: number };
	type Slot = { pid: number; tid: number; playoffs: boolean; sum: Sum };
	const fromBox = new Map<string, Slot>();
	for (const game of games) {
		if (game.season !== season || isAllStarGame(game)) {
			continue;
		}
		for (const team of game.teams) {
			if (wanted && !wanted.has(team.tid)) {
				continue;
			}
			const players = Array.isArray(team.players) ? team.players : [];
			for (const p of players as any[]) {
				if (!p || typeof p.pid !== "number" || !(p.gp > 0)) {
					continue;
				}
				const playoffs = game.playoffs === true;
				const key = `${p.pid}|${team.tid}|${playoffs}`;
				const slot = fromBox.get(key) ?? {
					pid: p.pid,
					tid: team.tid,
					playoffs,
					sum: { gp: 0, min: 0, pts: 0 },
				};
				slot.sum.gp += 1;
				slot.sum.min += p.min ?? 0;
				slot.sum.pts += p.pts ?? 0;
				fromBox.set(key, slot);
			}
		}
	}

	let suspect = 0;
	const players = new Map<number, any>();
	for (const slot of fromBox.values()) {
		if (!players.has(slot.pid)) {
			players.set(slot.pid, await idb.cache.players.get(slot.pid));
		}
		const p = players.get(slot.pid);
		if (!p) {
			continue;
		}
		const have: Sum = { gp: 0, min: 0, pts: 0 };
		for (const row of p.stats as any[]) {
			if (
				row.season === season &&
				row.tid === slot.tid &&
				row.playoffs === slot.playoffs
			) {
				have.gp += row.gp ?? 0;
				have.min += row.min ?? 0;
				have.pts += row.pts ?? 0;
			}
		}
		if (
			have.gp !== slot.sum.gp ||
			Math.abs(have.min - slot.sum.min) > 0.5 ||
			have.pts !== slot.sum.pts
		) {
			suspect += 1;
		}
	}
	return suspect;
};

const listChanges = (changes: RecordChange[]) =>
	changes
		.map((fix) => `tid ${fix.tid} ${fix.before} -> ${fix.after}`)
		.join(", ");

export const describeRebuild = (report: RebuildReport): string | undefined => {
	const parts: string[] = [];
	if (report.recordsFixed.length > 0) {
		parts.push(`fixed records ${listChanges(report.recordsFixed)}`);
	}
	if (report.recordsHeld.length > 0) {
		parts.push(
			`held records that count more games than this device has: ${listChanges(report.recordsHeld)}`,
		);
	}
	if (report.statsRowsFixed > 0) {
		parts.push(`fixed ${report.statsRowsFixed} team stat row(s)`);
	}
	if (report.statsRowsHeld > 0) {
		parts.push(`held ${report.statsRowsHeld} team stat row(s)`);
	}
	if (report.playerRowsSuspect > 0) {
		parts.push(
			`${report.playerRowsSuspect} player row(s) disagree with the box scores (not written)`,
		);
	}
	if (parts.length === 0) {
		return undefined;
	}
	return `[sync] Rebuilt from games (${report.season}, ${report.teamsChecked} teams): ${parts.join("; ")}`;
};
