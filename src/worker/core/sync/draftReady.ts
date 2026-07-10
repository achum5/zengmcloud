// Phase ready-up: in a synced league, certain shared advances only happen once
// EVERY user-controlled team has readied up. Each device publishes its own
// ready state ("ready through step N of the current stage") to a shared
// control doc; readiness is counted PER TEAM (any of a user's devices covers
// their team), so someone with the app open on two devices can't deadlock or
// double-count the room.
//
// Gated stages, each with its own notion of a "step":
//   - DRAFT_LOTTERY: one step - advance past the lottery (the lottery itself
//     still runs, inside the phase change if it wasn't revealed manually).
//   - DRAFT: each pick is a step (overall pick number). A pick on the clock
//     belonging to a HUMAN team never auto-advances - that user drafting IS
//     their ready (runPicks pauses on user picks regardless, as a second line
//     of defense).
//   - RESIGN_PLAYERS: one step - start free agency.
//   - FREE_AGENCY: each day is a step, so you can ready through "N days left"
//     or the end of free agency (which rolls into the preseason on its own).
//
// When all teams are ready for the next step, every connected device races for
// an atomic claim (a Firestore transaction); exactly one wins and runs exactly
// one step. The winner re-verifies a live connection and re-fetches the log
// head before claiming, so a device that missed another device's advance can't
// re-run the same step, and the claim carries a lease so a winner that crashes
// mid-advance unblocks the room. Every advance runs inside a capture window
// and publishes through the normal afterAction pipeline - identical to the
// simmer doing it by hand.

import { PHASE } from "../../../common/constants.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { idb } from "../../db/index.ts";
import { g, local, toUI } from "../../util/index.ts";
import getOrder from "../draft/getOrder.ts";
import runPicks from "../draft/runPicks.ts";
import newPhase from "../phase/newPhase.ts";
import freeAgentsPlay from "../freeAgents/play.ts";
import { getSyncEngine } from "./engineHolder.ts";
import { runAfterActionHook } from "./afterActionHook.ts";
import { syncDebugLog } from "./debugLog.ts";
import type { DraftReadyEntry, SyncTransport } from "./types.ts";
import type { MpPhaseReady, Phase } from "../../../common/types.ts";

const EVALUATE_INTERVAL_MS = 2000;

// How long an advance claim blocks other devices from claiming the same step.
// Generous vs a single step (a second or two) so it only matters as crash
// recovery for a claimant that died mid-advance.
const CLAIM_LEASE_MS = 90_000;

// Free-agency steps are derived from daysLeft (which counts DOWN) so they
// increase as days sim; the base just keeps them positive.
const FA_STEP_BASE = 1000;

// The overall pick number (1-based across the whole draft) used as the
// "ready through" comparator during the draft.
export const overallPickNumber = (
	dp: { round: number; pick: number },
	numActiveTeams: number,
): number => (dp.round - 1) * numActiveTeams + dp.pick;

// Which user teams are covered by a ready entry valid for this stage and the
// next step. Exported for tests.
export const readyTeamTids = (
	ready: Record<string, DraftReadyEntry | null | undefined> | undefined,
	userTids: number[],
	stageKey: string,
	nextStep: number,
): number[] => {
	const covered = new Set<number>();
	for (const entry of Object.values(ready ?? {})) {
		if (
			entry &&
			entry.draftKey === stageKey &&
			typeof entry.untilPick === "number" &&
			entry.untilPick >= nextStep &&
			typeof entry.tid === "number" &&
			userTids.includes(entry.tid)
		) {
			covered.add(entry.tid);
		}
	}
	return [...covered].sort((a, b) => a - b);
};

// Everything the evaluator needs to know about the current gated stage:
// UI state pieces plus how to run one step.
type StageInfo = {
	nextStep: number;
	nextLabel: string;
	onClockUser: boolean;
	waypoints: { step: number; label: string }[];
	options: { step: number; label: string; mine?: boolean }[];
	advance: () => Promise<void>;
};

// The phase advance the draft lottery leads to - mirrors autoPlay's branching
// for repeat-season leagues.
const afterLotteryPhase = (): Phase => {
	const type = g.get("repeatSeason")?.type;
	if (type === "playersAndRosters" || g.get("forceHistoricalRosters")) {
		return PHASE.PRESEASON;
	}
	if (type === "players") {
		return PHASE.RESIGN_PLAYERS;
	}
	return PHASE.DRAFT;
};

// Run a phase change as a ready-up advance: captured + published like any
// other bulk change.
const advancePhase = async (phase: Phase) => {
	changeTracker.beginSim();
	try {
		await newPhase(phase, {});
	} finally {
		changeTracker.endSim();
	}
	await runAfterActionHook("playMenu", "day");
};

const getStageInfo = async (): Promise<StageInfo | undefined> => {
	const phase = g.get("phase");

	if (phase === PHASE.DRAFT_LOTTERY) {
		return {
			nextStep: 1,
			nextLabel: "Advance past the lottery",
			onClockUser: false,
			waypoints: [],
			options: [],
			advance: () => advancePhase(afterLotteryPhase()),
		};
	}

	if (phase === PHASE.DRAFT) {
		const order = await getOrder();
		if (order.length === 0) {
			return undefined;
		}
		const numActiveTeams = g.get("numActiveTeams");
		const userTids = g.get("userTids");
		const userTid = g.get("userTid");
		const next = order[0]!;

		// Team abbrevs so each pick in the list shows who owns it.
		let abbrevByTid = new Map<number, string>();
		try {
			const teams = await idb.cache.teams.getAll();
			abbrevByTid = new Map(teams.map((t) => [t.tid, t.abbrev]));
		} catch {
			// Labels just omit the team.
		}

		// EVERY remaining pick, so "ready through R1P16" works no matter how far
		// out it is. The UI scrolls the list.
		const options = order.map((dp) => {
			const abbrev = abbrevByTid.get(dp.tid);
			return {
				step: overallPickNumber(dp, numActiveTeams),
				label: `R${dp.round}P${dp.pick}${abbrev ? ` · ${abbrev}` : ""}`,
			};
		});
		const myNext = order.find((dp) => dp.tid === userTid);
		const lastInRound = [...order]
			.filter((dp) => dp.round === next.round)
			.at(-1);
		const endOfDraftStep = overallPickNumber(order.at(-1)!, numActiveTeams);
		const endOfRoundStep = lastInRound
			? overallPickNumber(lastInRound, numActiveTeams)
			: undefined;

		// One waypoint per distinct step: in the final round "through this round"
		// IS "through end of draft", so only the latter shows (a duplicate step
		// also duplicated React keys and rendered the same label twice).
		const waypoints: { step: number; label: string }[] = [];
		if (myNext) {
			waypoints.push({
				step: overallPickNumber(myNext, numActiveTeams),
				label: "Until my pick",
			});
		}
		if (endOfRoundStep !== undefined && endOfRoundStep !== endOfDraftStep) {
			waypoints.push({ step: endOfRoundStep, label: "Through this round" });
		}
		waypoints.push({ step: endOfDraftStep, label: "Through end of draft" });
		const seenSteps = new Set<number>();
		const uniqueWaypoints = waypoints.filter((w) => {
			if (seenSteps.has(w.step)) {
				return false;
			}
			seenSteps.add(w.step);
			return true;
		});

		return {
			nextStep: overallPickNumber(next, numActiveTeams),
			nextLabel: `R${next.round}P${next.pick}`,
			onClockUser: userTids.includes(next.tid),
			waypoints: uniqueWaypoints,
			options,
			advance: async () => {
				changeTracker.beginSim();
				try {
					await runPicks({ type: "onePick" });
				} finally {
					changeTracker.endSim();
				}
				await runAfterActionHook("actions", "onePick");
			},
		};
	}

	if (phase === PHASE.RESIGN_PLAYERS) {
		return {
			nextStep: 1,
			nextLabel: "Start free agency",
			onClockUser: false,
			waypoints: [],
			options: [],
			advance: () => advancePhase(PHASE.FREE_AGENCY),
		};
	}

	if (phase === PHASE.FREE_AGENCY) {
		const daysLeft = g.get("daysLeft");
		if (typeof daysLeft !== "number" || daysLeft <= 0) {
			return undefined;
		}

		// Step for simming the NEXT day. "Ready through X days left" = the step
		// of the day whose sim lands on X.
		const nextStep = FA_STEP_BASE - daysLeft + 1;
		const options: { step: number; label: string }[] = [];
		for (let target = daysLeft - 1; target >= 0; target--) {
			options.push({
				step: FA_STEP_BASE - target,
				label:
					target === 0
						? "End of free agency"
						: `${target} ${target === 1 ? "day" : "days"} left`,
			});
		}

		return {
			nextStep,
			// Label with the days left AFTER this day sims, so it reads continuously
			// with the "ready through…" list below it (28 left, 27 left, …).
			nextLabel:
				daysLeft - 1 === 0 ? "Final day" : `Next day (${daysLeft - 1} left)`,
			onClockUser: false,
			waypoints: [],
			options,
			// freeAgents.play brackets + publishes itself (same as the play menu).
			advance: () => freeAgentsPlay(1, {}),
		};
	}

	return undefined;
};

let currentTransport: SyncTransport | undefined;
let unsubscribe: (() => void) | undefined;
let evaluateTimer: ReturnType<typeof setInterval> | undefined;
let latestReady: Record<string, DraftReadyEntry | null> | undefined;
let advancing = false;
let lastPushed: string | undefined;

// The (stage, step, holdout) we last nudged, so the sole-holdout notification
// fires ONCE per stuck-state instead of every 2s evaluate tick.
let lastHoldoutNotifKey: string | undefined;

// Decide whether THIS device should push a "you're the last one not readied
// up" notification, and to which team. Returns the sole holdout's tid to
// notify, or undefined when: there's no lone holdout, a human is on the clock
// (that's a "make your pick" moment, not a "ready up" one), it's not a genuine
// multi-team room, or this device isn't the designated single publisher.
//
// Single-publisher rule: the notification is sent only by the READY device
// with the smallest client id. Every device runs this each tick, so without a
// deterministic sole sender the room would fire one copy per ready device. The
// holdout's own devices are never in the ready set, so they're never the
// publisher (and the Cloud Function skips the author regardless). Pure and
// exported for tests; the caller owns dedup + the actual push.
export const lastHoldoutToNotify = ({
	latestReady: ready,
	userTids,
	readyTids,
	stageKey,
	nextStep,
	onClockUser,
	clientId,
}: {
	latestReady: Record<string, DraftReadyEntry | null> | undefined;
	userTids: number[];
	readyTids: number[];
	stageKey: string;
	nextStep: number;
	onClockUser: boolean;
	clientId: string;
}): number | undefined => {
	if (onClockUser || userTids.length < 2) {
		return undefined;
	}
	const holdouts = userTids.filter((tid) => !readyTids.includes(tid));
	if (holdouts.length !== 1) {
		return undefined;
	}
	const holdoutTid = holdouts[0]!;

	let minReadyUid: string | undefined;
	for (const [uid, entry] of Object.entries(ready ?? {})) {
		if (
			entry &&
			entry.draftKey === stageKey &&
			typeof entry.untilPick === "number" &&
			entry.untilPick >= nextStep &&
			typeof entry.tid === "number" &&
			userTids.includes(entry.tid) &&
			entry.tid !== holdoutTid &&
			(minReadyUid === undefined || uid < minReadyUid)
		) {
			minReadyUid = uid;
		}
	}
	if (minReadyUid === undefined || minReadyUid !== clientId) {
		return undefined;
	}
	return holdoutTid;
};

// The page a holdout notification deep-links to for the current gated phase.
const holdoutNotifPath = (phase: number): string => {
	switch (phase) {
		case PHASE.DRAFT_LOTTERY:
			return "draft_lottery";
		case PHASE.DRAFT:
			return "draft";
		case PHASE.RESIGN_PLAYERS:
			return "negotiation";
		case PHASE.FREE_AGENCY:
			return "free_agents";
		default:
			return "";
	}
};

// Push the sole-holdout nudge if this device is the designated publisher and
// we haven't already nudged this exact stuck-state. Fire-and-forget on the
// separate notifications channel - fully decoupled from the changeset/outbox
// pipeline, so it can never affect sync.
const maybeNotifyLastHoldout = async (
	engine: NonNullable<ReturnType<typeof getSyncEngine>>,
	stage: StageInfo,
	stageKey: string,
	userTids: number[],
	readyTids: number[],
) => {
	// Don't nudge off a stale view of the world while still catching up.
	if (!engine.isCaughtUp()) {
		return;
	}
	const holdoutTid = lastHoldoutToNotify({
		latestReady,
		userTids,
		readyTids,
		stageKey,
		nextStep: stage.nextStep,
		onClockUser: stage.onClockUser,
		clientId: engine.clientId,
	});
	if (holdoutTid === undefined) {
		return;
	}

	// A holdout blocks the step from advancing, so (stage, step, holdout) is a
	// stable key while the room is stuck - fire once, not every tick.
	const key = `${stageKey}:${stage.nextStep}:${holdoutTid}`;
	if (key === lastHoldoutNotifKey) {
		return;
	}
	lastHoldoutNotifKey = key;

	let teamName = "your team";
	try {
		const team = await idb.cache.teams.get(holdoutTid);
		if (team) {
			teamName = `${team.region} ${team.name}`;
		}
	} catch {
		// The name is a nicety; a generic body still delivers the nudge.
	}

	try {
		await engine.publishNotification({
			title: "Everyone's ready but you",
			body: `The league is waiting on the ${teamName} to ready up.`,
			targetTids: [holdoutTid],
			path: holdoutNotifPath(g.get("phase")),
		});
		syncDebugLog("phaseReady:holdout-notified", {
			stageKey,
			step: stage.nextStep,
			holdoutTid,
		});
	} catch (error) {
		// A failed push is harmless - clear the key so a later tick can retry.
		lastHoldoutNotifKey = undefined;
		syncDebugLog("phaseReady:holdout-notify-failed", { error });
	}
};

const pushToUI = (state: MpPhaseReady | undefined) => {
	const key = JSON.stringify(state ?? null);
	if (key === lastPushed) {
		return;
	}
	lastPushed = key;
	void toUI("updateLocal", [{ mpPhaseReady: state }]);
};

const stageKeyNow = (): string => `${g.get("season")}-${g.get("phase")}`;

// One evaluation pass: refresh the UI state and, if everything lines up,
// attempt to claim + run the next step.
const evaluate = async () => {
	const engine = getSyncEngine();
	const transport = currentTransport;
	if (!engine || !transport) {
		pushToUI(undefined);
		return;
	}

	let stage: StageInfo | undefined;
	try {
		if (!local.leagueLoaded) {
			pushToUI(undefined);
			return;
		}
		stage = await getStageInfo();
	} catch {
		return;
	}
	if (!stage) {
		pushToUI(undefined);
		return;
	}

	const userTids = g.get("userTids");
	const stageKey = stageKeyNow();
	const phase = g.get("phase");

	const readyTids = readyTeamTids(
		latestReady,
		userTids,
		stageKey,
		stage.nextStep,
	);

	// Readiness is counted PER TEAM, so the button must reflect the TEAM's
	// state, not just this device's own entry - with the same team open on two
	// devices, readying up on one shows ready on both. The team's ready-through
	// step is the furthest any of its devices committed to.
	const myTid = g.get("userTid");
	let myUntilStep: number | undefined;
	for (const entry of Object.values(latestReady ?? {})) {
		if (
			entry &&
			entry.draftKey === stageKey &&
			entry.tid === myTid &&
			typeof entry.untilPick === "number" &&
			entry.untilPick >= stage.nextStep &&
			(myUntilStep === undefined || entry.untilPick > myUntilStep)
		) {
			myUntilStep = entry.untilPick;
		}
	}

	pushToUI({
		phase,
		readyTeams: readyTids.length,
		totalTeams: userTids.length,
		ready: myUntilStep !== undefined,
		myUntilStep,
		nextStep: { number: stage.nextStep, label: stage.nextLabel },
		onClockUser: stage.onClockUser,
		waypoints: stage.waypoints.filter((w) => w.step > stage.nextStep),
		options: stage.options.filter((o) => o.step > stage.nextStep),
	});

	// Nudge the SOLE remaining holdout (before the advance early-returns below, so
	// a queued-upload backpressure return can't skip it). Safe + spam-proof: see
	// maybeNotifyLastHoldout.
	void maybeNotifyLastHoldout(engine, stage, stageKey, userTids, readyTids);

	// ---- Advance? ----
	if (
		advancing ||
		stage.onClockUser ||
		userTids.length === 0 ||
		readyTids.length < userTids.length ||
		!engine.isCaughtUp() ||
		!transport.claimDraftAdvance
	) {
		return;
	}

	// Backpressure: never advance while this device still has uploads queued.
	// Without this, a failing/slow connection let chained advances keep simming
	// steps while entries piled up in the outbox by the hundreds - each step's
	// data was safe (durable outbox), but the backlog grew unboundedly and the
	// room fell far behind. Advancing only from a drained outbox paces the chain
	// to what the connection can actually deliver.
	try {
		if ((await engine.pendingUploadCount()) > 0) {
			void engine.drainOutbox();
			return;
		}
	} catch {
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
		const stage2 = await getStageInfo();
		if (
			!stage2 ||
			g.get("phase") !== phase ||
			stage2.nextStep !== stage.nextStep ||
			stage2.onClockUser ||
			!engine.isCaughtUp() ||
			readyTeamTids(latestReady, g.get("userTids"), stageKey, stage.nextStep)
				.length < g.get("userTids").length
		) {
			return;
		}

		// Exactly one device wins this.
		const claimed = await transport.claimDraftAdvance(
			stageKey,
			stage.nextStep,
			CLAIM_LEASE_MS,
		);
		if (!claimed) {
			return;
		}

		syncDebugLog("phaseReady:advance", {
			stageKey,
			step: stage.nextStep,
			label: stage2.nextLabel,
		});

		await stage2.advance();
	} catch (error) {
		console.error("[sync] ready-up advance failed", error);
	} finally {
		advancing = false;
		// Re-evaluate soon - with everyone still ready, the next step chains.
		setTimeout(() => {
			void evaluate();
		}, 500);
	}
};

// Publish this device's ready state: ready through step `untilStep` of the
// current stage, or null to clear. Called from the UI via the worker API.
export const setDraftReady = async (untilStep: number | null) => {
	const engine = getSyncEngine();
	const transport = currentTransport;
	if (!engine || !transport?.publishDraftReady) {
		throw new Error("Connect to a shared league first.");
	}

	// Readiness is per TEAM, so this action supersedes anything the same team
	// published from another device: the team's current coverage is the max of
	// its devices' entries, and the write below clears those other entries so
	// the team's LATEST action wins (otherwise "Not ready" on this device
	// couldn't revoke a ready published from the phone).
	const tid = g.get("userTid");
	const stageKey = stageKeyNow();
	let teamUntilStep: number | undefined;
	const sameTeamUids: string[] = [];
	for (const [uid, entry] of Object.entries(latestReady ?? {})) {
		if (!entry || entry.draftKey !== stageKey || entry.tid !== tid) {
			continue;
		}
		if (uid !== engine.clientId) {
			sameTeamUids.push(uid);
		}
		if (
			typeof entry.untilPick === "number" &&
			(teamUntilStep === undefined || entry.untilPick > teamUntilStep)
		) {
			teamUntilStep = entry.untilPick;
		}
	}

	// Readying UP is always allowed (even mid-catch-up - it only consents to
	// future steps). But REVOKING or reducing readiness while this device is
	// behind or the room is mid-advance is acting on a stale world: it can halt
	// a chain of steps the rest of the room already agreed to, right while the
	// authority is running them. Make the device see the current state first.
	const reducing =
		untilStep === null ||
		(teamUntilStep !== undefined && untilStep < teamUntilStep);
	if (reducing && (!engine.isCaughtUp() || engine.isRoomBusy())) {
		throw new Error(
			"Still catching up on league changes — try again in a moment.",
		);
	}

	if (untilStep === null) {
		await transport.publishDraftReady(null, sameTeamUids);
	} else {
		await transport.publishDraftReady(
			{
				untilPick: untilStep,
				draftKey: stageKey,
				tid,
				name: engine.localName,
			},
			sameTeamUids,
		);
	}
	void evaluate();
};

export const setupDraftReady = (transport: SyncTransport) => {
	teardownDraftReady();
	currentTransport = transport;
	latestReady = undefined;
	lastHoldoutNotifKey = undefined;
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
	lastHoldoutNotifKey = undefined;
	pushToUI(undefined);
};
