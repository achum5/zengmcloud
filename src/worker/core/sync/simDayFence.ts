// The play.ts-facing side of the schedule-day sim fence (see
// simDayClaimPolicy.ts for the rule and FirebaseTransport.claimSimDay for the
// transaction). Outside a sync room - or on a transport without the fence -
// every claim is granted locally, so single-player behavior is untouched.

import { g } from "../../util/index.ts";
import { getSyncEngine } from "./engineHolder.ts";
import { syncDebugLog } from "./debugLog.ts";
import type { SyncTransport } from "./types.ts";

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

export const setupSimDayFence = (transport: SyncTransport) => {
	currentTransport = transport;
	lastClaimedDay = undefined;
};

export const teardownSimDayFence = () => {
	currentTransport = undefined;
	lastClaimedDay = undefined;
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
	} else {
		syncDebugLog("simDayFence:rejected", { day, gids });
		// The natural cause of a rejection is that this device is behind the
		// room's real history; pull it in rather than leaving stale state up.
		void engine.catchUp().catch(() => undefined);
	}
	return granted;
};

// Close the last claimed slice's crash-recovery window. Call only once its
// results are durably queued for upload - completing earlier could wedge the
// room on a crash (day fenced forever, results never published). Best-effort:
// a failure just leaves the lease to expire.
export const completeClaimedSimDayFence = () => {
	const transport = currentTransport;
	const day = lastClaimedDay;
	lastClaimedDay = undefined;
	if (day === undefined || !transport?.completeSimDay) {
		return;
	}
	void transport.completeSimDay(stageKey(), day).catch(() => undefined);
};
