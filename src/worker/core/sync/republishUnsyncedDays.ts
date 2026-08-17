import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
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
	teamSeasons: any[];
	teamStats: any[];
	players: any[];
	gameAttributes: any[];
	playoffSeries?: any;
};

const readLeagueRows = async (season: number): Promise<LeagueRows> => ({
	games: (await idb.cache.games.getAll()) as any[],
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
	//
	// ONE DELETE PER PLAYED GAME, asked of the games themselves rather than of
	// the local schedule. That distinction is the whole bug this had the first
	// time: simming a day DELETES its schedule rows, so by the time anyone comes
	// to repair the day this device no longer holds a single one of them, the
	// filter matched nothing, and the changeset went out with the games and no
	// deletes at all. The room got the scores; every other device went on
	// listing the same games under "Upcoming Games".
	//
	// A game that has been played has no business on anyone's schedule, so the
	// delete is unconditional - and deleting a row the receiver does not have is
	// a no-op, which makes asking first worth nothing.
	for (const game of missing) {
		changes.push({ store: "schedule", id: game.gid, type: "delete" });
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

	// Players. Two very different situations share this code, and they get
	// different widths:
	//
	// The AUTOMATIC repair ("after") has PROVEN the room is behind this device -
	// planUnsyncedPush compared positions - so this device's current rows are
	// the room's rows plus the missing days, and sending every rostered player
	// is merely redundant where nothing changed. Wide is safe there, and it
	// catches the ones whose injury ticked down on the bench.
	//
	// The MANUAL push ("only") can offer no such proof: it exists precisely for
	// a room whose position was never stamped. If this device's copy of the
	// league has drifted from the room's in ANY way - and a device that needs
	// this repair is a device whose bookkeeping already went wrong once -
	// pushing the whole player table broadcasts the drift to every device as
	// one giant, authoritative overwrite. That is how a league woke up one
	// morning with every player's ratings quietly shifted and its recent
	// injuries erased: the pushed day was real, but it carried a fork of the
	// entire league along with it. So the manual push carries only the players
	// who actually appear in the pushed games' box scores - the rows that day
	// demonstrably changed - and nothing else.
	const namedDayOnly = selection.kind === "only";
	let pushPids: Set<number> | undefined;
	if (namedDayOnly) {
		pushPids = new Set();
		for (const game of missing) {
			for (const t of game.teams ?? []) {
				for (const p of t.players ?? []) {
					if (typeof p.pid === "number") {
						pushPids.add(p.pid);
					}
				}
			}
		}
	}
	for (const player of rows.players) {
		if (typeof player.tid === "number" && player.tid >= 0) {
			if (pushPids && !pushPids.has(player.pid)) {
				continue;
			}
			const change = put("players", player);
			if (change) {
				changes.push(change);
			}
		}
	}

	// Game attributes carry the phase - the number that tells every other
	// device the league has moved. Only for the proven-behind automatic repair:
	// a manual push happens INSIDE the room's current phase (the day being
	// named is a day of it), and this store also holds every league setting,
	// which a device pushing a lost day has no business rewriting for the room.
	if (!namedDayOnly) {
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
// But "the room never stamped a position" is the ONLY licence to skip the
// comparison. When the room HAS stamped one, it is checked here exactly as the
// automatic repair checks it - naming a day the room already played past means
// the room's copy of that day (and of every row this push carries with it) is
// the living one, and publishing this device's would overwrite the room's
// present with this device's past. The other two conditions are about safety
// rather than bookkeeping: this must be the device in charge of simming, and
// the day must actually have games on it here.

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

// Pure, so every refusal is a test. Returns undefined when the push may
// proceed.
export const dayPushRefusal = (
	room: LeaguePosition | undefined,
	target: { season: number; day: number },
	local: { season: number; phase: number },
): string | undefined => {
	if (!room) {
		// Nothing stamped, nothing to compare - the case this repair exists for.
		return undefined;
	}
	if (room.season !== target.season || room.phase !== local.phase) {
		return `The room is on season ${room.season}, phase ${room.phase} - not where this day belongs. Pushing across that gap would overwrite the room's present with this device's past.`;
	}
	if (target.day <= room.day) {
		return `The room has already played through day ${room.day}, so it did not miss day ${target.day} - it has its own copy, and pushing this device's would overwrite every record it carries. If this device's results really are the ones the league should keep, that needs more than a day push.`;
	}
	return undefined;
};

export const describeDayPush = async (
	{ season, day }: { season: number; day: number },
	isAuthority: boolean,
	authority?: unknown,
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

	const refusal = dayPushRefusal(
		roomPosition(authority),
		{ season, day },
		{ season: g.get("season"), phase: g.get("phase") },
	);
	if (refusal) {
		return { kind: "none", reason: refusal };
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
	authority?: unknown,
): Promise<{ changeset: Changeset; report: DayPushReport }> => {
	const report = await describeDayPush(target, isAuthority, authority);
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
