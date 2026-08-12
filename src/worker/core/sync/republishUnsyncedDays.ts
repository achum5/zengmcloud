import { idb } from "../../db/index.ts";
import type { Store } from "../../db/Cache.ts";
import type { Changeset, SyncChange } from "./changeset.ts";
import { getLeaguePosition, type LeaguePosition } from "./leaguePosition.ts";

// PUSHING OUT A DAY THAT WAS SIMMED HERE AND NEVER REACHED THE ROOM.
//
// How a league gets into this state, once: the device in charge of simming
// starts an advance, another device publishes an ordinary edit while the sim is
// running, and the sim loses the compare-and-swap when it finally goes to
// publish. A timeline advance that loses that race is DISCARDED rather than
// retried, because a sim computed against a world the room has moved past
// cannot simply be replayed on top of a different one.
//
// Discarding is supposed to be followed by snapping this device's database back
// to a room checkpoint, which is what removes the records the chain will never
// carry. If the room has no checkpoint - and a room can go hundreds of versions
// without one, since publishing is blocked whenever a history or integrity check
// finds anything - there is nothing to snap back to. The advance is dropped, the
// records stay, and the device goes on believing it is caught up while holding
// games nobody else has. That is the state this repairs.
//
// It does NOT re-simulate anything. The games already exist here, exactly as
// they were played; this rebuilds the changeset that should have been published
// at the time and sends it as an ordinary new version.

// WHAT A PLAYED DAY TOUCHES.
//
// Deliberately a little wider than the strict minimum. The failure mode of
// missing a store is silent and permanent - every device ends up disagreeing
// about something nobody thinks to check - whereas the cost of including one
// that did not change is a few extra whole-record puts that apply to identical
// values. Where the choice is "possibly redundant" against "possibly divergent",
// this takes redundant every time.
//
// `events` is deliberately absent: an event's `eid` is an autoincrement that
// differs on every device, so publishing rows keyed by it would collide with
// unrelated events elsewhere. The news is cosmetic; the standings are not.
const PK_FIELD: Partial<Record<Store, string>> = {
	games: "gid",
	schedule: "gid",
	players: "pid",
	teams: "tid",
	teamSeasons: "rid",
	teamStats: "rid",
	playoffSeries: "season",
	draftPicks: "dpid",
};

export type UnsyncedDaysReport =
	| { kind: "none"; reason: string }
	| {
			kind: "found";
			season: number;
			roomDay: number;
			localDay: number;
			// The days this device has played that the room has not.
			days: number[];
			games: number;
			// How many records the repair would publish, so the size of what is
			// about to happen is visible before it happens rather than after.
			records: number;
	  };

// Where the ROOM thinks the league is, as stamped on the authority document by
// the last advance that actually landed. Deliberately not this device's own
// bookkeeping: the whole point is that this device's bookkeeping is the thing
// that is wrong.
const roomPosition = (authority: unknown): LeaguePosition | undefined => {
	const position = (authority as { position?: unknown } | undefined)?.position;
	if (!position || typeof position !== "object") {
		return undefined;
	}
	const { season, phase, day } = position as Record<string, unknown>;
	if (
		typeof season !== "number" ||
		typeof phase !== "number" ||
		typeof day !== "number"
	) {
		return undefined;
	}
	return { season, phase, day };
};

const put = (store: Store, row: any): SyncChange | undefined => {
	const field = PK_FIELD[store];
	const id = field === undefined ? undefined : row?.[field];
	if (id === undefined) {
		return undefined;
	}
	return { store, id, type: "put", value: row };
};

// Everything the changeset is built from, gathered in one place so the building
// itself is a pure function of it - which is the only way to test that a day's
// worth of records comes out right without standing up a whole league.
export type LeagueRows = {
	games: any[];
	schedule: any[];
	teamSeasons: any[];
	teamStats: any[];
	players: any[];
	gameAttributes: any[];
	playoffSeries?: any;
};

const readLeagueRows = async (season: number): Promise<LeagueRows> => ({
	games: (await idb.cache.games.getAll()) as any[],
	schedule: (await idb.cache.schedule.getAll()) as any[],
	teamSeasons: (await idb.cache.teamSeasons.getAll()) as any[],
	teamStats: (await idb.cache.teamStats.getAll()) as any[],
	players: (await idb.cache.players.getAll()) as any[],
	gameAttributes: (await idb.cache.gameAttributes.getAll()) as any[],
	playoffSeries: await idb.cache.playoffSeries.get(season),
});

// WHICH DAYS TO SEND.
//
// "after" is the automatic repair: everything this device has played past
// where the room says it is. "only" is the manual one, for a room whose
// position was never stamped and so cannot be compared against - the user
// names the day.
export type DaySelection =
	| { kind: "after"; day: number }
	| { kind: "only"; days: number[] };

const selects = (selection: DaySelection, day: number): boolean =>
	selection.kind === "after"
		? day > selection.day
		: selection.days.includes(day);

// The games played here that the room has not seen, and everything their being
// played implies.
export const buildChanges = (
	rows: LeagueRows,
	season: number,
	selection: DaySelection,
): { changes: SyncChange[]; days: number[]; games: number } => {
	const changes: SyncChange[] = [];
	const days = new Set<number>();

	const missing = rows.games.filter(
		(game) =>
			game.season === season &&
			typeof game.day === "number" &&
			selects(selection, game.day),
	);
	for (const game of missing) {
		days.add(game.day);
		const change = put("games", game);
		if (change) {
			changes.push(change);
		}
	}

	// The room still holds a schedule row for every one of those games, and a
	// put-only changeset cannot remove it. Left behind, every other device shows
	// a game as still to be played that has already been played here.
	const gids = new Set(missing.map((game) => game.gid));
	for (const row of rows.schedule) {
		if (gids.has(row.gid)) {
			changes.push({ store: "schedule", id: row.gid, type: "delete" });
		}
	}

	// Standings, team stats and the season's playoff bracket are aggregates: a
	// day of games rewrites them wholesale, so the whole current season goes.
	for (const row of rows.teamSeasons) {
		if (row.season === season) {
			const change = put("teamSeasons", row);
			if (change) {
				changes.push(change);
			}
		}
	}
	for (const row of rows.teamStats) {
		if (row.season === season) {
			const change = put("teamStats", row);
			if (change) {
				changes.push(change);
			}
		}
	}
	const series = rows.playoffSeries;
	if (series) {
		const change = put("playoffSeries", series);
		if (change) {
			changes.push(change);
		}
	}

	// Every rostered player: stats, injuries, mood and ratings all move on a
	// game day, and picking out "only the ones who played" would miss the ones
	// whose injury ticked down on the bench.
	for (const player of rows.players) {
		if (typeof player.tid === "number" && player.tid >= 0) {
			const change = put("players", player);
			if (change) {
				changes.push(change);
			}
		}
	}

	// Small, and it carries the phase and the day counter - the numbers that
	// tell every other device the league has moved.
	for (const row of rows.gameAttributes) {
		if (row?.key !== undefined) {
			changes.push({
				store: "gameAttributes",
				id: row.key,
				type: "put",
				value: row,
			});
		}
	}

	return {
		changes,
		days: [...days].sort((a, b) => a - b),
		games: missing.length,
	};
};

// Whether there is anything to push, and if so how far apart the two are. Pure,
// so every refusal is a test rather than a hope.
export const planUnsyncedPush = ({
	room,
	local,
	isAuthority,
}: {
	room: LeaguePosition | undefined;
	local: LeaguePosition;
	isAuthority: boolean;
}):
	| { ok: true; season: number; roomDay: number }
	| { ok: false; reason: string } => {
	if (!isAuthority) {
		return {
			ok: false,
			reason:
				"Only the device in charge of simming can push a missing day, because it is the only one that can have simmed it.",
		};
	}
	if (!room) {
		return {
			ok: false,
			reason:
				"The room has not recorded how far along it is yet, so there is nothing to compare this device against.",
		};
	}
	if (local.season !== room.season || local.phase !== room.phase) {
		return {
			ok: false,
			reason: `This device is on ${local.season} phase ${local.phase} and the room is on ${room.season} phase ${room.phase}. That is a bigger gap than a missing day, and pushing games across it would make things worse rather than better.`,
		};
	}
	if (local.day <= room.day) {
		return {
			ok: false,
			reason: "The room already has every day this device has played.",
		};
	}
	return { ok: true, season: local.season, roomDay: room.day };
};

// What the repair WOULD do, without doing any of it.
export const describeUnsyncedDays = async (
	authority: unknown,
	isAuthority: boolean,
): Promise<UnsyncedDaysReport> => {
	if (!isAuthority) {
		return {
			kind: "none",
			reason:
				"Only the device in charge of simming can push a missing day, because it is the only one that can have simmed it.",
		};
	}

	const local = await getLeaguePosition();
	const plan = planUnsyncedPush({
		room: roomPosition(authority),
		local,
		isAuthority,
	});
	if (!plan.ok) {
		return { kind: "none", reason: plan.reason };
	}

	const rows = await readLeagueRows(plan.season);
	const { changes, days, games } = buildChanges(rows, plan.season, {
		kind: "after",
		day: plan.roomDay,
	});
	if (games === 0) {
		return {
			kind: "none",
			reason: "No played games here are missing from the room.",
		};
	}

	return {
		kind: "found",
		season: plan.season,
		roomDay: plan.roomDay,
		localDay: local.day,
		days,
		games,
		records: changes.length,
	};
};

// Build the changeset. The caller hands it to the sync engine exactly as an
// ordinary action's changeset would be handed over, so it goes through the same
// durable outbox, the same retry, and the same chunked publish.
export const buildUnsyncedDaysChangeset = async (
	authority: unknown,
	isAuthority: boolean,
): Promise<{ changeset: Changeset; report: UnsyncedDaysReport }> => {
	const report = await describeUnsyncedDays(authority, isAuthority);
	if (report.kind !== "found") {
		return { changeset: { changes: [] }, report };
	}
	const rows = await readLeagueRows(report.season);
	const { changes } = buildChanges(rows, report.season, {
		kind: "after",
		day: report.roomDay,
	});
	return { changeset: { changes }, report };
};

// ------------------------------------------------------- NAMING THE DAY
//
// The automatic repair compares this device against the position the room
// stamped on its authority document - and a room that has never stamped one
// (an older room, or one whose last advance failed before the stamp) gives it
// nothing to compare against. That is not a reason to be stuck: the person at
// the keyboard knows perfectly well which day did not go out. So they name it,
// and it goes out exactly as it would have when it simmed.
//
// No comparison, therefore no refusals about how far apart the two are. The
// only conditions are the two that are about safety rather than bookkeeping:
// this must be the device in charge of simming, and the day must actually have
// games on it here.

export type DayPushReport =
	| { kind: "none"; reason: string }
	| {
			kind: "found";
			season: number;
			day: number;
			games: number;
			records: number;
			// The scoreboard for the day, so what is about to be sent can be read
			// back before sending it. Naming the wrong day should be obvious
			// BEFORE it is published, not after.
			lines: string[];
	  };

// "PHO 103 @ DAL 110". teams[0] is the home side in ZenGM, and the visitor is
// named first the way every scoreboard names it.
const scoreLine = (game: any, abbrev: (tid: number) => string): string => {
	const [home, away] = game.teams ?? [];
	if (!home || !away) {
		return `Game ${game.gid}`;
	}
	return `${abbrev(away.tid)} ${away.pts ?? "?"} @ ${abbrev(home.tid)} ${home.pts ?? "?"}`;
};

export const describeDayPush = async (
	{ season, day }: { season: number; day: number },
	isAuthority: boolean,
): Promise<DayPushReport> => {
	if (!isAuthority) {
		return {
			kind: "none",
			reason:
				"Only the device in charge of simming can push a day, because it is the only one that can have simmed it.",
		};
	}
	if (!Number.isInteger(season) || !Number.isInteger(day)) {
		return { kind: "none", reason: "Give a season and a day." };
	}

	const rows = await readLeagueRows(season);
	const selection: DaySelection = { kind: "only", days: [day] };
	const { changes, games } = buildChanges(rows, season, selection);
	if (games === 0) {
		// Say which days DO exist rather than leaving them to guess. Getting the
		// day wrong is the easiest mistake to make here and the least useful
		// thing to be told nothing about.
		const played = [
			...new Set(
				rows.games
					.filter((row) => row.season === season && row.day !== undefined)
					.map((row) => row.day as number),
			),
		].sort((a, b) => a - b);
		return {
			kind: "none",
			reason:
				played.length === 0
					? `No games have been played here in ${season}.`
					: `No games were played here on day ${day} of ${season}. Days with games: ${played.join(", ")}.`,
		};
	}

	const abbrevByTid = new Map<number, string>(
		((await idb.cache.teams.getAll()) as any[]).map((team) => [
			team.tid,
			team.abbrev,
		]),
	);
	const abbrev = (tid: number) => abbrevByTid.get(tid) ?? `#${tid}`;

	return {
		kind: "found",
		season,
		day,
		games,
		records: changes.length,
		lines: rows.games
			.filter((game) => game.season === season && game.day === day)
			.map((game) => scoreLine(game, abbrev)),
	};
};

export const buildDayPushChangeset = async (
	target: { season: number; day: number },
	isAuthority: boolean,
): Promise<{ changeset: Changeset; report: DayPushReport }> => {
	const report = await describeDayPush(target, isAuthority);
	if (report.kind !== "found") {
		return { changeset: { changes: [] }, report };
	}
	const rows = await readLeagueRows(report.season);
	const { changes } = buildChanges(rows, report.season, {
		kind: "only",
		days: [report.day],
	});
	return { changeset: { changes }, report };
};
