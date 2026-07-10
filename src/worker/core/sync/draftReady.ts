// Draft ready-up: during the draft, CPU picks only advance once EVERY
// user-controlled team has readied up. Each device publishes its own ready
// state ("ready through overall pick N") to a shared control doc; readiness is
// counted PER TEAM (any of a user's devices covers their team), so someone
// with the app open on two devices can't deadlock or double-count the room.
//
// When all teams are ready and the pick on the clock belongs to a CPU team,
// every connected device races for an atomic claim (a Firestore transaction);
// exactly one wins and sims exactly one pick. The winner re-verifies a live
// connection and re-fetches the log head before claiming, so a device that
// missed another device's advance can't re-sim the same pick, and the claim
// carries a lease so a winner that crashes mid-advance unblocks the room. The
// pick itself runs inside a capture window and publishes through the normal
// afterAction pipeline - identical to any other sim.
//
// A pick on the clock belonging to a HUMAN team never auto-advances: that
// user drafting IS their ready (runPicks pauses on user picks regardless, as
// a second line of defense).

import { PHASE } from "../../../common/constants.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { g, local, toUI } from "../../util/index.ts";
import getOrder from "../draft/getOrder.ts";
import runPicks from "../draft/runPicks.ts";
import { getSyncEngine } from "./engineHolder.ts";
import { runAfterActionHook } from "./afterActionHook.ts";
import { syncDebugLog } from "./debugLog.ts";
import type { DraftReadyEntry, SyncTransport } from "./types.ts";
import type { MpDraftReady } from "../../../common/types.ts";

const EVALUATE_INTERVAL_MS = 2000;

// How long an advance claim blocks other devices from claiming the same pick.
// Generous vs a one-pick sim (a second or two) so it only matters as crash
// recovery for a claimant that died mid-advance.
const CLAIM_LEASE_MS = 90_000;

// How many upcoming picks the UI's "ready through…" list offers.
const MAX_UPCOMING = 24;

// The overall pick number (1-based across the whole draft) used as the
// "ready through" comparator.
export const overallPickNumber = (
	dp: { round: number; pick: number },
	numActiveTeams: number,
): number => (dp.round - 1) * numActiveTeams + dp.pick;

// Which user teams are covered by a ready entry valid for this draft and the
// current next pick. Exported for tests.
export const readyTeamTids = (
	ready: Record<string, DraftReadyEntry | null | undefined> | undefined,
	userTids: number[],
	draftKey: string,
	nextOverall: number,
): number[] => {
	const covered = new Set<number>();
	for (const entry of Object.values(ready ?? {})) {
		if (
			entry &&
			entry.draftKey === draftKey &&
			typeof entry.untilPick === "number" &&
			entry.untilPick >= nextOverall &&
			typeof entry.tid === "number" &&
			userTids.includes(entry.tid)
		) {
			covered.add(entry.tid);
		}
	}
	return [...covered].sort((a, b) => a - b);
};

let currentTransport: SyncTransport | undefined;
let unsubscribe: (() => void) | undefined;
let evaluateTimer: ReturnType<typeof setInterval> | undefined;
let latestReady: Record<string, DraftReadyEntry | null> | undefined;
let advancing = false;
let lastPushed: string | undefined;

const pushToUI = (state: MpDraftReady | undefined) => {
	const key = JSON.stringify(state ?? null);
	if (key === lastPushed) {
		return;
	}
	lastPushed = key;
	void toUI("updateLocal", [{ mpDraftReady: state }]);
};

const draftKeyNow = (): string => `${g.get("season")}-${g.get("phase")}`;

// One evaluation pass: refresh the UI state and, if everything lines up,
// attempt to claim + run the next CPU pick.
const evaluate = async () => {
	const engine = getSyncEngine();
	const transport = currentTransport;
	if (!engine || !transport) {
		pushToUI(undefined);
		return;
	}

	// Only the regular draft, with a league loaded.
	let phase: number;
	try {
		phase = g.get("phase");
	} catch {
		return;
	}
	if (phase !== PHASE.DRAFT || !local.leagueLoaded) {
		pushToUI(undefined);
		return;
	}

	let order;
	try {
		order = await getOrder();
	} catch {
		return;
	}
	if (order.length === 0) {
		pushToUI(undefined);
		return;
	}

	const numActiveTeams = g.get("numActiveTeams");
	const userTids = g.get("userTids");
	const userTid = g.get("userTid");
	const draftKey = draftKeyNow();

	const next = order[0]!;
	const nextOverall = overallPickNumber(next, numActiveTeams);
	const onClockUser = userTids.includes(next.tid);

	const readyTids = readyTeamTids(latestReady, userTids, draftKey, nextOverall);

	// This device's own ready-through pick, if valid for this draft.
	const mine = latestReady?.[engine.clientId];
	const myUntilPick =
		mine && mine.draftKey === draftKey && mine.untilPick >= nextOverall
			? mine.untilPick
			: undefined;

	// Upcoming picks for the "ready through…" picker, plus useful waypoints.
	const upcoming = order.slice(0, MAX_UPCOMING).map((dp) => ({
		number: overallPickNumber(dp, numActiveTeams),
		label: `R${dp.round}P${dp.pick}`,
		mine: dp.tid === userTid,
	}));
	const myNext = order.find((dp) => dp.tid === userTid);
	const lastInRound = [...order].filter((dp) => dp.round === next.round).at(-1);

	pushToUI({
		readyTeams: readyTids.length,
		totalTeams: userTids.length,
		ready: myUntilPick !== undefined,
		myUntilPick,
		nextPick: { number: nextOverall, label: `R${next.round}P${next.pick}` },
		onClockUser,
		myPickNumber: myNext
			? overallPickNumber(myNext, numActiveTeams)
			: undefined,
		endOfRoundPick: lastInRound
			? overallPickNumber(lastInRound, numActiveTeams)
			: undefined,
		endOfDraftPick: overallPickNumber(order.at(-1)!, numActiveTeams),
		upcoming,
	});

	// ---- Advance? ----
	if (
		advancing ||
		onClockUser ||
		userTids.length === 0 ||
		readyTids.length < userTids.length ||
		!engine.isCaughtUp() ||
		!transport.claimDraftAdvance
	) {
		return;
	}

	advancing = true;
	try {
		// The advance forks the room if it runs on a stale or half-connected
		// device, so the same preflight as the worker guard: proven-live
		// connection, then a fresh read of the log head.
		await engine.ensureReady();
		if (!(await engine.verifyConnection(true))) {
			return;
		}
		await engine.catchUp();

		// Re-derive after the catch-up: someone else may have advanced already.
		const order2 = await getOrder();
		const next2 = order2[0];
		if (
			g.get("phase") !== PHASE.DRAFT ||
			!next2 ||
			overallPickNumber(next2, numActiveTeams) !== nextOverall ||
			g.get("userTids").includes(next2.tid) ||
			!engine.isCaughtUp() ||
			readyTeamTids(latestReady, g.get("userTids"), draftKey, nextOverall)
				.length < g.get("userTids").length
		) {
			return;
		}

		// Exactly one device wins this.
		const claimed = await transport.claimDraftAdvance(
			draftKey,
			nextOverall,
			CLAIM_LEASE_MS,
		);
		if (!claimed) {
			return;
		}

		syncDebugLog("draftReady:advance", {
			pick: nextOverall,
			label: `R${next2.round}P${next2.pick}`,
		});

		// Run one pick inside a capture window and publish through the normal
		// pipeline (the label is a draft action, so any device may author it).
		changeTracker.beginSim();
		try {
			await runPicks({ type: "onePick" });
		} finally {
			changeTracker.endSim();
		}
		await runAfterActionHook("actions", "onePick");
	} catch (error) {
		console.error("[sync] draft ready advance failed", error);
	} finally {
		advancing = false;
		// Re-evaluate soon - with everyone still ready, the next CPU pick chains.
		setTimeout(() => {
			void evaluate();
		}, 500);
	}
};

// Publish this device's ready state: ready through overall pick `untilPick`,
// or null to clear. Called from the UI via the worker API.
export const setDraftReady = async (untilPick: number | null) => {
	const engine = getSyncEngine();
	const transport = currentTransport;
	if (!engine || !transport?.publishDraftReady) {
		throw new Error("Connect to a shared league first.");
	}
	if (untilPick === null) {
		await transport.publishDraftReady(null);
	} else {
		await transport.publishDraftReady({
			untilPick,
			draftKey: draftKeyNow(),
			tid: g.get("userTid"),
			name: engine.localName,
		});
	}
	void evaluate();
};

export const setupDraftReady = (transport: SyncTransport) => {
	teardownDraftReady();
	currentTransport = transport;
	latestReady = undefined;
	unsubscribe = transport.subscribeDraftReady?.((ready) => {
		latestReady = ready;
		void evaluate();
	});
	evaluateTimer = setInterval(() => {
		void evaluate();
	}, EVALUATE_INTERVAL_MS);
};

export const teardownDraftReady = () => {
	unsubscribe?.();
	unsubscribe = undefined;
	if (evaluateTimer !== undefined) {
		clearInterval(evaluateTimer);
		evaluateTimer = undefined;
	}
	currentTransport = undefined;
	latestReady = undefined;
	pushToUI(undefined);
};
