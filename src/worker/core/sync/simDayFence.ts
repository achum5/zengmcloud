// The play.ts-facing side of the schedule-day sim fence (see
// simDayClaimPolicy.ts for the rule and FirebaseTransport.claimSimDay for the
// transaction). Outside a sync room - or on a transport without the fence -
// every claim is granted locally, so single-player behavior is untouched.

import { g, logEvent } from "../../util/index.ts";
import { getSyncEngine } from "./engineHolder.ts";
import { syncDebugLog } from "./debugLog.ts";
import { isSingleGameSimLabel } from "./actionLabels.ts";
import { decideSimDayClaim } from "./simDayClaimPolicy.ts";
import type { ChangesetEntry, SyncTransport } from "./types.ts";

// Generous relative to a day sim (seconds), but short enough that a crashed
// simmer doesn't wedge the room for long. Matches the draft-advance lease.
const SIM_DAY_LEASE_MS = 90_000;

// See claimSimDayFence: a hung claim must fail, not freeze the sim.
const CLAIM_TIMEOUT_MS = 10_000;
const withClaimTimeout = <T>(promise: Promise<T>): Promise<T> =>
	new Promise((resolve, reject) => {
		const id = setTimeout(
			() => reject(new Error(`Timed out after ${CLAIM_TIMEOUT_MS}ms`)),
			CLAIM_TIMEOUT_MS,
		);
		promise.then(
			(value) => {
				clearTimeout(id);
				resolve(value);
			},
			(error) => {
				clearTimeout(id);
				reject(error);
			},
		);
	});

let currentTransport: SyncTransport | undefined;

// The most recent day this device claimed, so the end-of-sim publish can close
// its crash-recovery window. Module-level because a multi-day sim recurses
// through fresh play() closures; safe because only one sim chain runs at a
// time (the gameSim lock). Intermediate days of a multi-day sim don't need
// completing - each newer-day claim supersedes them via the high-water mark.
let lastClaimedDay: number | undefined;
// ...and the games that claim covered, because completion is per gid: marking
// anything broader permanently fences games this sim never touched (see
// simDayClaimPolicy.ts).
let lastClaimedGids: number[] | undefined;

// A SINGLE game whose result is durably queued but has not yet been confirmed
// in the room - its completion waits for the upload to land.
//
// A day sim in that state is left to its lease on purpose: completing a slice
// whose results might never arrive could fence the day forever. But the two
// cases are not alike. A single game's result cannot be discarded by the sync
// engine (it rebases like an edit - see isTimelineAdvanceLabel), so a queued
// one WILL publish, the only question being when: a phone that was put away a
// beat after "Sim my game" holds it until the app comes back. Leaving that to a
// 90-second lease meant the room's next scheduled sim treated the game as a
// crashed slice and played it again, and the phone's result then landed on top
// of that. So the completion is held here and fired by the drain that finally
// confirms the upload (completeDeferredSimDayFence, from onUploadComplete).
let deferred: { day: number; gids: number[] } | undefined;

export const setupSimDayFence = (transport: SyncTransport) => {
	currentTransport = transport;
	lastClaimedDay = undefined;
	lastClaimedGids = undefined;
	deferred = undefined;
};

export const teardownSimDayFence = () => {
	currentTransport = undefined;
	lastClaimedDay = undefined;
	lastClaimedGids = undefined;
	deferred = undefined;
};

const stageKey = () => `sim:${g.get("season")}`;

// Claim the right to sim these games. Returns true when the sim may proceed:
// always outside a sync room, and for exactly one device per (season, day,
// games) inside one. A false return means another device already simmed (or is
// simming) some of these games - the caller must skip the day and catch up
// instead of simming, or its results would double-apply on the whole room.
export const claimSimDayFence = async (
	day: number,
	gids: number[],
): Promise<boolean> => {
	const engine = getSyncEngine();
	const transport = currentTransport;
	if (!engine || !transport?.claimSimDay) {
		return true;
	}

	// TIMED: the claim is a cloud transaction, and an unbounded hang here is a
	// sim that freezes silently mid-click. A timeout counts as "not granted" -
	// the safe direction (skip and catch up; if the claim actually landed
	// server-side, its lease expires on its own).
	let granted = false;
	try {
		granted = await withClaimTimeout(
			transport.claimSimDay(stageKey(), day, gids, SIM_DAY_LEASE_MS),
		);
	} catch (error) {
		syncDebugLog("simDayFence:claim-failed", { day, error: String(error) });
		return false;
	}
	if (granted) {
		lastClaimedDay = day;
		lastClaimedGids = gids;
	} else {
		syncDebugLog("simDayFence:rejected", { day, gids });
		// The natural cause of a rejection is that this device is behind the
		// room's real history; pull it in rather than leaving stale state up.
		void engine.catchUp().catch(() => undefined);
	}
	return granted;
};

const complete = (day: number, gids: number[], how: string) => {
	const transport = currentTransport;
	if (!transport?.completeSimDay) {
		return;
	}
	syncDebugLog("simDayFence:completed", { day, gids, how });
	void transport.completeSimDay(stageKey(), day, gids).catch(() => undefined);
};

// Close the last claimed slice's crash-recovery window.
//
// `synced` means the results are confirmed in the room: complete now. Not
// synced means they are durably queued but not yet confirmed. A whole day in
// that state is left to its lease - completing it could wedge the room on a
// crash (day fenced forever, results never published), and a day advance can
// still be discarded by the engine if it lost a race. A single game is
// different, for the reasons on `deferred`: it cannot be discarded, so its
// completion is only postponed, to the drain that lands it. Best-effort
// throughout: a failure just leaves the lease to expire.
export const completeClaimedSimDayFence = ({
	synced,
	singleGame,
}: {
	synced: boolean;
	singleGame: boolean;
}) => {
	const day = lastClaimedDay;
	const gids = lastClaimedGids;
	lastClaimedDay = undefined;
	lastClaimedGids = undefined;
	if (day === undefined) {
		return;
	}
	if (synced) {
		complete(day, gids ?? [], "synced");
		return;
	}
	if (singleGame) {
		deferred = { day, gids: gids ?? [] };
		syncDebugLog("simDayFence:completion-deferred", { day, gids });
	}
};

// A full drain just confirmed everything that was queued - the deferred single
// game included, if there was one. Idempotent; a completion for a day the fence
// has moved past is a no-op server-side.
export const completeDeferredSimDayFence = () => {
	const pending = deferred;
	deferred = undefined;
	if (!pending) {
		return;
	}
	complete(pending.day, pending.gids, "deferred-upload-landed");
};

// The games a queued single-game entry carries, read off the changeset itself
// so it needs no bookkeeping that a restart would lose: the game row a sim
// writes carries its gid and day.
export const fencedGamesIn = (
	entry: Pick<ChangesetEntry, "action" | "changeset">,
): { day: number; gids: number[] } | undefined => {
	if (!isSingleGameSimLabel(entry.action)) {
		return undefined;
	}
	const gids: number[] = [];
	let day: number | undefined;
	for (const change of entry.changeset.changes) {
		if (change.store !== "games" || change.type !== "put") {
			continue;
		}
		const value = change.value as { gid?: unknown; day?: unknown };
		if (typeof value?.gid === "number" && typeof value.day === "number") {
			gids.push(value.gid);
			day ??= value.day;
		}
	}
	if (day === undefined || gids.length === 0) {
		return undefined;
	}
	return { day, gids };
};

export type QueuedResultVerdict = "publish" | "drop" | "wait";

// IS A RESULT THAT WAITED IN THE OUTBOX STILL THE ROOM'S TO RECEIVE?
//
// Asked by the sync engine before it publishes an entry that did not go up on
// its first attempt. Only a single game's result can be superseded this way: a
// device sims its own game, is put away before the upload lands, and while it
// is away the lease on its fence slice lapses and the room's scheduled sim
// plays the game as crash recovery. When the device comes back, its result is
// a second sim of a game the room already has, and publishing it would put
// two games' worth of aggregates on top of each other - the very thing the
// fence exists to prevent. So the fence is asked again, with the same pure
// policy the claim transaction uses:
//
//   - granted:              our slice is still ours (or recoverable) - publish.
//   - lease held elsewhere: someone is mid-sim on these games - wait.
//   - completed elsewhere, or the day is history - drop, and re-sync to what
//                           the room has.
//
// Never asked of a first attempt (the claim was granted moments ago and the
// policy would read our own live lease as "held"), which is what `deferred`
// records; and never of anything but a single-game entry, which is the only
// kind whose completion is ever deferred.
export const revalidateQueuedSingleGame = async (
	entry: Pick<ChangesetEntry, "action" | "changeset">,
): Promise<QueuedResultVerdict> => {
	const transport = currentTransport;
	if (!transport?.readSimDayClaim || !transport.claimSimDay || !deferred) {
		return "publish";
	}
	const games = fencedGamesIn(entry);
	if (!games || games.day !== deferred.day) {
		return "publish";
	}

	let existing;
	try {
		existing = await withClaimTimeout(transport.readSimDayClaim());
	} catch (error) {
		syncDebugLog("simDayFence:revalidate-read-failed", {
			error: String(error),
		});
		// Cannot tell; the entry stays queued for the next kick.
		return "wait";
	}

	// Our own live claim, nobody else's since: the policy would call it held,
	// but it is held by us. Publish.
	if (
		existing &&
		existing.holderId === transport.clientId &&
		existing.stageKey === stageKey() &&
		existing.day === games.day &&
		games.gids.every((gid) => existing!.gids.includes(gid)) &&
		!games.gids.some((gid) => existing!.completedGids?.includes(gid))
	) {
		return "publish";
	}

	const decision = decideSimDayClaim(existing, {
		stageKey: stageKey(),
		day: games.day,
		gids: games.gids,
		now: Date.now(),
		leaseMs: SIM_DAY_LEASE_MS,
	});
	syncDebugLog("simDayFence:revalidate", {
		day: games.day,
		gids: games.gids,
		grant: decision.grant,
		reason: decision.grant ? undefined : decision.reason,
	});
	if (decision.grant) {
		// Take it back for real (the lease may have lapsed), so the room sees a
		// live holder again while the upload goes up.
		try {
			const granted = await withClaimTimeout(
				transport.claimSimDay(
					stageKey(),
					games.day,
					games.gids,
					SIM_DAY_LEASE_MS,
				),
			);
			return granted ? "publish" : "wait";
		} catch {
			return "wait";
		}
	}
	if (decision.reason === "lease-held") {
		return "wait";
	}
	// day-already-run or games-already-simmed: the room has these games from
	// someone else. This result is stale.
	deferred = undefined;
	logEvent({
		type: "error",
		text: "The game you simmed earlier didn't reach the cloud before the league simmed that day from another device, so the league's result stands and yours was set aside. This device is re-syncing.",
		saveToDb: false,
		persistent: true,
	});
	syncDebugLog("simDayFence:stale-result-dropped", {
		day: games.day,
		gids: games.gids,
		reason: decision.reason,
	});
	return "drop";
};
