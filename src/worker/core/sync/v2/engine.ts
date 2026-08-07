import { g, local, lock, logEvent } from "../../../util/index.ts";
import { idb } from "../../../db/index.ts";
import { outbox } from "../outbox.ts";
import {
	compressSerialized,
	decompressSerialized,
	deserializeChangeset,
	serializeChangeset,
} from "../serialize.ts";
import {
	AUTO_PUBLISH_CHECKPOINTS,
	buildRoomSnapshotPayload,
	validateRoomSnapshotPayload,
} from "../roomSnapshot.ts";
import { repairLeagueHistory } from "../historyRepair.ts";
import { checkLeagueIntegrity } from "../leagueIntegrity.ts";
import { checkApplyGuard } from "../applyGuard.ts";
import {
	claimRecoveryAttempt,
	clearRecoveryAttempt,
} from "../recoveryBreadcrumb.ts";
import { payloadLeagueId, readLocalLeagueId } from "../leagueIdentity.ts";
import { syncDebugLog } from "../debugLog.ts";
import {
	refreshAfterApply,
	summarizeChangesetForRefresh,
	type Changeset,
} from "../changeset.ts";
import type { SyncNotification } from "../notifications.ts";
import type {
	Authority,
	ChangesetEntry,
	SyncMember,
	SyncTransport,
	V2StateDoc,
} from "../types.ts";
import {
	applyCheckpointV2,
	applyVersionedChangeset,
	readAppliedVersion,
} from "./applyVersion.ts";
import { catchUpPlan } from "./protocol.ts";
import { isTimelineAdvanceLabel } from "../actionLabels.ts";

// ---------------------------------------------------------------------------
// ENGINE V2: the version chain, running.
//
// The v1 engine spends thousands of lines deciding which of two disagreeing
// databases is right. This engine cannot have that problem, because it never
// constructs the disagreement: it applies exactly the next version or nothing
// (applyVersion.ts), and EVERY published change - from any device - becomes
// the next version by compare-and-set on the room's single version pointer.
// The CAS is the entire concurrency story: two devices racing for N+1 means
// one wins and one re-reads; the pointer cannot fork. No device depends on
// any OTHER device being online to get its change into the cloud. Recovery
// from ANY distance is one code path: catchUpPlan says checkpoint-then-deltas
// or deltas, both bounded, both ordered, both atomic per step.
//
// Staleness has two different answers, on purpose (see actionLabels.ts):
// an ordinary edit whose base moved catches up and republishes - it is a
// whole-record statement of user intent. A TIMELINE ADVANCE whose base moved
// is discarded loudly and the device snaps back to the chain via checkpoint,
// because a sim day derived from a world the room has moved past is exactly
// the artifact that forked v1 leagues when it was merged late.
//
// It intentionally presents the same surface connect.ts and the coordination
// layer already consume (mapped exhaustively before this was written), so
// ready-up gates, the FA board, live watching, notifications and the rest run
// unchanged on top of it. Members that expose v1-only machinery (watermarks,
// batch rebuilding, log archaeology) are honest constants here - the
// machinery does not exist to report on.
// ---------------------------------------------------------------------------

// Publish a fresh checkpoint once the chain has grown this far past the last
// one.
//
// This was 25, on the reasoning that "checkpoints are cheap to take (same
// builder as v1, gzipped)". The premise was exactly backwards: it is the same
// builder as v1, and that builder is the most expensive thing in the entire
// sync layer - it reads EVERY store of the league into memory, JSON-stringifies
// the whole graph, and gzips the result. v1 pays that every 1200 log entries;
// at 25 versions v2 was paying it roughly fifty times more often, where a
// version is a single user action. On a phone with a deep league that is
// repeated hundred-megabyte allocation, and iOS kills the tab for it - which
// is precisely what "v2 crashes whenever I open a page" was.
//
// 300 still checkpoints far more often than v1 while making the rebuild a
// rare event. Nothing about recovery weakens: deltas older than the PREVIOUS
// checkpoint are the only ones pruned, so the chain always spans at least a
// full interval and a behind device still catches up on deltas alone.
const CHECKPOINT_EVERY_VERSIONS = 300;

// And never even consider it more than this often. v1 has always had this
// throttle (SNAPSHOT_CHECK_MIN_MS); v2 shipped without one, so a burst of
// versions during an active session could trigger the full rebuild the moment
// the interval elapsed, mid-play. Checking is cheap, but the thing it gates is
// not, and there is no urgency to a checkpoint.
const CHECKPOINT_CHECK_MIN_MS = 5 * 60 * 1000;

// How long before the SAME checkpoint may be attempted again after a failed
// restore. Downloading and parsing an entire league is not something to retry
// on a five-second timer: when the failure is deterministic (a payload this
// build refuses), the retries never succeed and each one allocates the whole
// league, which is fatal on a phone long before it is useful anywhere.
const CHECKPOINT_RESTORE_RETRY_MS = 2 * 60 * 1000;

// Firestore's setDoc never REJECTS while offline - it buffers the write and
// resolves whenever the connection returns, which can be never for a
// backgrounded phone. Un-timed awaits on transport writes therefore hang the
// drain, and the drain is awaited by the user's action: an offline sim would
// freeze the Play button instead of fast-failing to "queued". Every publish
// attempt is timed; a timeout leaves the entry safely in the outbox for the
// next drain kick (immediately on the next action, every 5s from the health
// tick while anything is queued, every 30s from the catch-up timer).
const PUBLISH_ATTEMPT_TIMEOUT_MS = 20_000;

// Catch-up reads are timed for the same reason publishes are: a read that
// hangs (network died mid-request) would wedge the single-flight catch-up
// chain, and every later catch-up waits behind it forever. Short enough that
// a wedged channel fails a couple of reads within seconds - which is what
// arms the network cycle below - instead of sitting on one hung read while
// the user watches nothing happen.
const CATCHUP_READ_TIMEOUT_MS = 8_000;

// Payloads at or under this many serialized characters ride inside the
// pointer document itself (Firestore's doc limit is 1 MB; this leaves lots of
// headroom). Covers roster moves, trades, signings - everything interactive.
const INLINE_DELTA_LIMIT = 150_000;

// The head probe runs every 5s health tick; its read must resolve or fail
// well within one tick so probes never pile up behind a hung one.
const HEAD_PROBE_TIMEOUT_MS = 4_000;

// How long a successful readiness probe counts for. Long enough that a burst
// of edits does not pay for a round trip each, short enough that a connection
// which died quietly stops being vouched for within a few interactions.
const READY_TTL_MS = 60_000;

// How long to wait before the next catch-up pass, indexed by how many passes
// have failed in a row. The first retry is immediate - a single blip should
// not cost the user a wait - and it climbs from there. Any delta that actually
// applies resets the streak, so a long healthy walk never backs off.
const CATCHUP_BACKOFF_MS = [0, 1_000, 3_000, 8_000, 15_000, 30_000];

// After tearing the Firestore connection down and back up, give it a moment to
// re-establish before reading through it. Reading immediately is how a cycle
// turned into "client is offline" on the very next request.
const NETWORK_CYCLE_SETTLE_MS = 2_000;

const withTimeout = <T>(promise: Promise<T>, ms: number): Promise<T> =>
	new Promise((resolve, reject) => {
		const id = setTimeout(
			() => reject(new Error(`Timed out after ${ms}ms`)),
			ms,
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

const makeId = (): string =>
	typeof crypto !== "undefined" && crypto.randomUUID
		? crypto.randomUUID()
		: `${Date.now()}-${Math.floor(Math.random() * 1e9)}`;

export class SyncEngineV2 {
	readonly transport: SyncTransport;
	localName = "You";

	private code: string | undefined;
	private stopped = false;

	private authority: Authority | undefined;
	private authorityUnsubscribe: (() => void) | undefined;
	private stateUnsubscribe: (() => void) | undefined;

	// Mirrors. The durable truth for `applied` is the marker in the league DB;
	// this mirror only spares reads and is re-read before every decision that
	// matters.
	private roomVersion = 0;
	private roomState: V2StateDoc | undefined;
	private appliedMirror = 0;

	private catchUpChain: Promise<boolean> = Promise.resolve(true);
	private catchingUp = false;
	private drainChain: Promise<boolean> = Promise.resolve(true);
	private draining = false;

	// Readiness is a LEASE, not a latch. It used to be a one-way boolean: the
	// first successful ping set it true and nothing ever set it back, so
	// ensureReady short-circuited forever and "Cloud sync is not ready" could
	// never fire again however dead the connection got - on the very guard that
	// gates every sim. Now it expires, and a dead listener revokes it outright.
	private readyUntil = 0;
	private readyProbe: Promise<void> | undefined;
	private lastReportedReady = false;

	// Set when a stale timeline advance was discarded: the local database
	// contains a mutation the chain will never carry, so the next catch-up must
	// rebuild from the checkpoint instead of trusting the delta walk alone.
	private mustRecoverFromCheckpoint = false;

	// Wedge detection (see probeHead / doCatchUp): a single-flight guard for
	// the probe, per-path consecutive-failure counts, and a throttle on the
	// network-cycle remedy. The counts are SEPARATE on purpose: a wedge can
	// hang the delta reads while the tiny pointer read still succeeds, so a
	// probe success must not absolve a string of catch-up failures.
	private probing = false;
	private probeFailureStreak = 0;
	private catchupFailureStreak = 0;
	private lastNetworkCycleAt = 0;
	private lastCatchupFailureAt = 0;
	// The catch-up step already reported to the user via a persistent notice,
	// so repeated failures at the same step nag exactly once.
	private notifiedFailingStep: string | undefined;
	private publishingCheckpoint = false;
	private checkpointVettedForSession = false;
	// Last time the checkpoint decision was even considered (see
	// CHECKPOINT_CHECK_MIN_MS). Starts at 0 so the first look happens promptly
	// - a room with no checkpoint at all has no recovery until one exists.
	private lastCheckpointCheckAt = 0;
	// The last checkpoint restore this device attempted, so a restore that
	// fails for a reason that will not change (a payload it refuses) cannot be
	// retried in a tight loop - see CHECKPOINT_RESTORE_RETRY_MS.
	private lastCheckpointAttempt: { key: string; at: number } | undefined;
	// Bumped every time a checkpoint actually lands. Comparing the version
	// numbers cannot answer "did the restore happen": a device asking for a
	// forced restore is typically ALREADY at the checkpoint's version - that is
	// the whole complaint - so the numbers look identical either way.
	private checkpointRestores = 0;

	// Whether the UI is currently showing the catch-up indicator for this
	// engine, so it is only ever cleared when it was shown (no flicker on the
	// ordinary one-version live applies).
	private catchUpPillShown = false;

	// Whether each live listener is currently believed to be alive. A Firestore
	// listener is TERMINAL once its error callback fires, so "subscribed" and
	// "working" are different questions and readiness depends on the second.
	private stateListenerHealthy = false;
	private authorityListenerHealthy = false;
	private authorityRestartTimer: ReturnType<typeof setTimeout> | undefined;

	// When the state listener last actually delivered. Distinct from the room's
	// own `at` stamp, which is when the version was WRITTEN: an idle room and a
	// dead listener produce the same stale `at`, and telling those two apart is
	// the entire point of the field on the debug capture.
	private lastStateDeliveryAt = 0;

	// The last few versions this device applied or published, for the sync
	// page's activity list and the debug capture. v2 has no readable log to
	// page back through the way v1 does - deltas are pruned behind the
	// checkpoint - so the honest answer is what this session has seen, kept in
	// memory and costing nothing.
	private recentVersions: ChangesetEntry[] = [];
	private static RECENT_VERSIONS_LIMIT = 60;

	private memoryQueue: Omit<ChangesetEntry, "seq">[] = [];
	private memoryNotifications = new Map<string, SyncNotification[]>();

	private onAuthorityChange: ((a: Authority | undefined) => void) | undefined;
	private onReadyChange: ((ready: boolean) => void) | undefined;
	private onPendingChange: ((count: number) => void) | undefined;
	private onUploadComplete: (() => void) | undefined;
	// Fired while a drain is actually pushing entries to the cloud, so the
	// header can show it happening. Nothing else in the app can tell the user
	// "your sim is going up right now, keep the app open".
	private onUploadingChange:
		| ((progress: { done: number; total: number } | undefined) => void)
		| undefined;
	private onCatchUpProgress:
		| ((progress: { done: number; total: number } | undefined) => void)
		| undefined;
	private publishTimeoutMs: number;

	constructor(
		transport: SyncTransport,
		options: {
			// Accepted and ignored: connect decides whether to claim authority and
			// calls claimAuthority() itself. Keeping it as engine state was how
			// "am I the simmer?" got two answers that could disagree.
			isHost?: boolean;
			code?: string;
			onAuthorityChange?: (a: Authority | undefined) => void;
			onReadyChange?: (ready: boolean) => void;
			onPendingChange?: (count: number) => void;
			onUploadComplete?: () => void;
			onUploadingChange?: (
				progress: { done: number; total: number } | undefined,
			) => void;
			// Shown while this device is visibly behind the room and working on
			// it (a big walk, or fetches that are failing and retrying) - never
			// for the ordinary one-version live apply.
			onCatchUpProgress?: (
				progress: { done: number; total: number } | undefined,
			) => void;
			// Test hook: shrink the publish-attempt timeout.
			publishTimeoutMs?: number;
		} = {},
	) {
		this.transport = transport;
		this.code = options.code;
		this.onAuthorityChange = options.onAuthorityChange;
		this.onReadyChange = options.onReadyChange;
		this.onPendingChange = options.onPendingChange;
		this.onUploadComplete = options.onUploadComplete;
		this.onUploadingChange = options.onUploadingChange;
		this.onCatchUpProgress = options.onCatchUpProgress;
		this.publishTimeoutMs =
			options.publishTimeoutMs ?? PUBLISH_ATTEMPT_TIMEOUT_MS;
	}

	get clientId(): string {
		return this.transport.clientId;
	}

	// ---- Lifecycle ---------------------------------------------------------

	start() {
		this.subscribeAuthority();
		this.subscribeState();
	}

	// The authority listener, with the same self-healing the state listener has
	// had all along. It shipped without an error handler, and a Firestore
	// listener is terminal once it errors - so one network blip left the device
	// permanently showing whoever was simming at that moment, with isRoomBusy()
	// frozen at whatever it last saw. Nothing here is allowed to be permanent.
	private subscribeAuthority() {
		this.authorityUnsubscribe?.();
		this.authorityUnsubscribe = this.transport.subscribeAuthority?.(
			(authority) => {
				this.authorityListenerHealthy = true;
				this.authority = authority;
				this.onAuthorityChange?.(authority);
			},
			(error) => {
				if (this.stopped) {
					return;
				}
				// Report "nobody is simming" rather than a name we can no longer
				// vouch for: a stale holder is what makes another device believe
				// the room is busy and refuse to sim.
				this.authorityListenerHealthy = false;
				this.authority = undefined;
				this.markNotReady();
				this.authorityUnsubscribe?.();
				this.authorityUnsubscribe = undefined;
				this.onAuthorityChange?.(undefined);
				syncDebugLog("v2:authority-listener-died", { error });
				if (this.authorityRestartTimer !== undefined) {
					clearTimeout(this.authorityRestartTimer);
				}
				this.authorityRestartTimer = setTimeout(() => {
					this.authorityRestartTimer = undefined;
					if (!this.stopped) {
						syncDebugLog("v2:authority-listener-restarted", {});
						this.subscribeAuthority();
					}
				}, 5000);
				const nodeTimer = this.authorityRestartTimer as unknown as {
					unref?: () => void;
				};
				if (typeof nodeTimer?.unref === "function") {
					nodeTimer.unref();
				}
			},
		);
		if (this.authorityUnsubscribe !== undefined) {
			this.authorityListenerHealthy = true;
		} else {
			// No authority support on this transport (tests, stubs). Absent is not
			// broken - don't hold readiness hostage to a listener that was never
			// going to exist.
			this.authorityListenerHealthy = true;
		}
	}

	// The live pointer listener, with self-healing: a listener that errors is
	// re-established after a short pause instead of dying silently. (A silently
	// dead listener demotes the device to timer-paced catch-up - the "every
	// change takes 30 seconds" field bug.) probeHead() is the belt to this
	// suspender: it detects a listener that stopped delivering WITHOUT erroring.
	private subscribeState() {
		this.stateUnsubscribe?.();
		this.stateUnsubscribe = this.transport.subscribeRoomV2State?.(
			(state) => {
				this.stateListenerHealthy = true;
				this.lastStateDeliveryAt = Date.now();
				this.roomState = state;
				if (state.version > this.roomVersion) {
					this.roomVersion = state.version;
				}
				if (state.version > this.appliedMirror) {
					// Pass the freshly-delivered state as a hint: for a small edit
					// the payload rides inside it, so the apply needs ZERO further
					// reads - it lands even when the read path is wedged.
					void this.catchUp(state);
				}
			},
			() => {
				if (this.stopped) {
					return;
				}
				this.stateListenerHealthy = false;
				this.markNotReady();
				const timer = setTimeout(() => {
					if (!this.stopped) {
						syncDebugLog("v2:state-listener-restarted", {});
						this.subscribeState();
					}
				}, 5000);
				const nodeTimer = timer as unknown as { unref?: () => void };
				if (typeof nodeTimer?.unref === "function") {
					nodeTimer.unref();
				}
			},
		);
		// A transport with no state listener at all (tests, stubs) is not a broken
		// one - the head probe is the fallback and readiness still holds.
		this.stateListenerHealthy = true;
	}

	// Ask the SERVER where the room's head is, and catch up if it is past us.
	// Runs from the 5s health tick. This is what bounds staleness when the live
	// listener is dead without knowing it (iOS killing the stream in the
	// background, a quietly-terminated onSnapshot): the pointer is one tiny
	// document, so polling it is nearly free, and it turns "stale until the 30s
	// timer" into "stale for at most one tick".
	//
	// The probe is deliberately strict: server answer or failure, never the
	// cache (a cached answer is the listener's own stale view - it would
	// "confirm" exactly the staleness the probe exists to catch). Two failures
	// in a row mean the SDK's backend channel is wedged - listeners quiet,
	// server reads hanging, Safari's specialty - and the cure is cycling the
	// connection and rebuilding the listener on the fresh channel.
	async probeHead(): Promise<void> {
		// Skipping during a catch-up walk is free: the walk reads this same
		// state doc every round (with its own timeout + failure streak), so a
		// probe on top would be a second copy of the read it exists to bound.
		if (
			this.stopped ||
			!this.transport.fetchRoomV2State ||
			this.probing ||
			this.catchingUp
		) {
			return;
		}
		this.probing = true;
		try {
			const state = await withTimeout(
				this.transport.fetchRoomV2State(),
				HEAD_PROBE_TIMEOUT_MS,
			);
			this.probeFailureStreak = 0;
			if (!state) {
				return;
			}
			if (state.version > this.roomVersion) {
				// The listener should have delivered this version. If it has existed
				// for a while (age, not just a race with a fresh commit), the
				// listener has stopped delivering - rebuild it.
				if (Date.now() - state.at > 10_000) {
					syncDebugLog("v2:listener-missed", {
						listenerVersion: this.roomVersion,
						serverVersion: state.version,
					});
					this.subscribeState();
				}
				this.roomVersion = state.version;
			}
			this.roomState = state;
			if (state.version > this.appliedMirror) {
				void this.catchUp(state);
			}
		} catch {
			this.probeFailureStreak += 1;
			await this.maybeCycleNetwork("probe");
		} finally {
			this.probing = false;
		}
	}

	// The wedged-channel remedy, fired when either failure streak says the
	// connection is lying about being alive: cycle it, put a fresh listener on
	// the rebuilt channel, and catch up.
	private async maybeCycleNetwork(source: "probe" | "catchup") {
		if (this.stopped || !this.transport.cycleNetwork) {
			return;
		}
		// Never yank the connection out from under a catch-up that is running.
		// disableNetwork fails every read in flight, so a probe-triggered cycle
		// landing mid-walk turned a working pass into "client is offline" and
		// cost the device its place. The catchup source is exempt: it calls this
		// from its own catch block, where the pass is already over.
		if (source === "probe" && this.catchingUp) {
			return;
		}
		if (
			Math.max(this.probeFailureStreak, this.catchupFailureStreak) < 2 ||
			Date.now() - this.lastNetworkCycleAt <= 30_000
		) {
			return;
		}
		this.probeFailureStreak = 0;
		this.catchupFailureStreak = 0;
		this.lastNetworkCycleAt = Date.now();
		syncDebugLog("v2:network-cycled", { source });
		try {
			await this.transport.cycleNetwork();
		} catch {
			// Even a failed cycle is followed by a fresh listener + catch-up.
		}
		this.subscribeState();
		// Let the rebuilt connection settle before reading through it - but do
		// NOT block on the wait. This runs inside the failing pass's catch, and
		// awaiting here would add the settle to the latency of every failure,
		// including the ones the pre-action guard is sitting on.
		const settle = setTimeout(() => {
			if (!this.stopped) {
				void this.catchUp();
			}
		}, NETWORK_CYCLE_SETTLE_MS);
		const nodeTimeout = settle as unknown as { unref?: () => void };
		if (typeof nodeTimeout?.unref === "function") {
			nodeTimeout.unref();
		}
	}

	// Tell the UI this device is visibly behind and working on it. Never fires
	// for the ordinary one-version live apply - only for a real walk (a big
	// gap) or when fetches are failing and retrying, which is exactly when a
	// user staring at a quiet screen starts doubting the app.
	private reportCatchUpProgress(done: number, total: number) {
		this.catchUpPillShown = true;
		try {
			this.onCatchUpProgress?.({ done, total });
		} catch {
			// Display only.
		}
	}

	private clearCatchUpProgress() {
		if (!this.catchUpPillShown) {
			return;
		}
		this.catchUpPillShown = false;
		try {
			this.onCatchUpProgress?.(undefined);
		} catch {
			// Display only.
		}
	}

	// Consecutive health ticks spent with the head known to be past what's
	// applied, plus the last figures this path pushed (so a stuck state logs
	// once, not once per tick).
	private stuckBehindTicks = 0;
	private lastStuckReport = "";

	// Called from the 5s health tick. The pill's other triggers - a big walk,
	// a failing fetch - both miss the case from the field: the device KNOWS a
	// version is coming (the listener delivered the pointer), but the apply is
	// silently slow (an IndexedDB stall mid-transaction, a wedge no fetch
	// timeout covers), the gap is small, and nothing fails. The screen showed
	// nothing for 30 seconds and then a signing appeared out of nowhere. Two
	// ticks of known-behind is the line between "ordinary live apply" (sub-
	// second to a few seconds, stays quiet) and "the user deserves to know a
	// change is in flight".
	reportIfStuckBehind(): void {
		if (this.stopped) {
			return;
		}
		if (this.roomVersion <= this.appliedMirror) {
			this.stuckBehindTicks = 0;
			this.lastStuckReport = "";
			// A finished walk clears its own pill; this only mops up one raised
			// here after the gap closed with no walk in flight to clear it.
			if (this.catchUpPillShown && !this.catchingUp) {
				this.clearCatchUpProgress();
			}
			return;
		}
		this.stuckBehindTicks += 1;
		if (this.stuckBehindTicks < 2) {
			return;
		}
		const report = `${this.appliedMirror}/${this.roomVersion}`;
		if (report !== this.lastStuckReport) {
			this.lastStuckReport = report;
			this.reportCatchUpProgress(this.appliedMirror, this.roomVersion);
		}
		// Belt to the listener handler's suspender: if nothing is working on
		// the gap (the handler's kick was swallowed by a race, or the walk
		// died without rethrowing), start one. Single-flight + backoff make a
		// redundant kick free.
		if (!this.catchingUp) {
			void this.catchUp(this.roomState);
		}
	}

	stop() {
		this.stopped = true;
		this.clearCatchUpProgress();
		if (this.authorityRestartTimer !== undefined) {
			clearTimeout(this.authorityRestartTimer);
			this.authorityRestartTimer = undefined;
		}
		this.authorityUnsubscribe?.();
		this.authorityUnsubscribe = undefined;
		this.stateUnsubscribe?.();
		this.stateUnsubscribe = undefined;
	}

	// ---- Authority / coordination surface ----------------------------------

	isAuthority(): boolean {
		return this.authority?.holderId === this.transport.clientId;
	}

	getAuthority(): Authority | undefined {
		return this.authority;
	}

	// "Host" means the device currently in charge of simming, read live from the
	// shared authority doc - NOT the isHost flag this session was constructed
	// with.
	//
	// The flag only ever answers "should I claim authority on connect", and it
	// is set once and never cleared. Its one consumer is the notification
	// builder, which stays silent about a sim unless the device that ran it is
	// the simmer (otherwise a room gets the same result announced twice). Read
	// off the flag, that silence landed on the WRONG device: join a room as a
	// follower, press "Sim here" (which claims authority in memory but does not
	// rewrite the persisted session), then reopen the app - the authority doc
	// still holds this device's uid, so it is genuinely the simmer and the Play
	// menu works, but the flag reconstructed as false. Every sim it ran went out
	// to everyone's league with no notification to anyone.
	getIsHost(): boolean {
		return this.isAuthority();
	}

	async claimAuthority() {
		await this.transport.claimAuthority?.(
			this.transport.clientId,
			this.localName,
		);
	}

	markRoomBusy(position?: unknown): void {
		if (!this.isAuthority()) {
			return;
		}
		void this.transport.publishBusy?.(
			Date.now() + 3 * 60 * 1000,
			position as any,
		);
	}

	clearRoomBusy(position?: unknown): void {
		if (!this.isAuthority()) {
			return;
		}
		void this.transport.publishBusy?.(0, position as any);
	}

	isRoomBusy(): boolean {
		if (this.isAuthority()) {
			return false;
		}
		const busyUntil = this.authority?.busyUntil ?? 0;
		return busyUntil > Date.now();
	}

	async registerMember(member: SyncMember) {
		this.localName = member.name || this.localName;
		await this.transport.registerMember?.(this.transport.clientId, member);
	}

	async publishNotification(notification: SyncNotification) {
		await this.transport.publishNotification?.({
			...notification,
			authorId: this.transport.clientId,
			authorName: this.localName,
		});
	}

	// ---- Health / readiness surface -----------------------------------------

	isCaughtUp(): boolean {
		return this.appliedMirror >= this.roomVersion;
	}

	isReady(): boolean {
		return (
			this.stateListenerHealthy &&
			this.authorityListenerHealthy &&
			Date.now() < this.readyUntil
		);
	}

	// Report a CHANGE in readiness. Edge-triggered off a remembered value rather
	// than off isReady() read twice around the mutation: the callers that matter
	// most (a listener error handler) have already flipped the health flag by
	// the time they get here, so "was it ready a moment ago" cannot be
	// reconstructed from the current state - and reconstructing it wrong means
	// the UI never hears that the connection died.
	private pushReady() {
		const ready = this.isReady();
		if (ready !== this.lastReportedReady) {
			this.lastReportedReady = ready;
			this.onReadyChange?.(ready);
		}
	}

	private markNotReady() {
		this.readyUntil = 0;
		this.pushReady();
	}

	async ensureReady(force = false): Promise<void> {
		// A dead listener is recoverable, so try to recover it here rather than
		// failing until the restart timer gets around to it - but do not claim
		// readiness on a connection whose live half is down.
		if (!this.stateListenerHealthy) {
			this.subscribeState();
		}
		if (!this.authorityListenerHealthy) {
			this.subscribeAuthority();
		}
		if (!this.stateListenerHealthy || !this.authorityListenerHealthy) {
			this.markNotReady();
			throw new Error("Cloud sync listeners are not ready.");
		}
		if (!force && Date.now() < this.readyUntil) {
			return;
		}
		// Single-flight: the pre-action guard can fire several of these at once
		// (a burst of edits), and they would otherwise each pay for their own
		// round trip.
		if (this.readyProbe) {
			return this.readyProbe;
		}
		// TIMED, without exception: a raw Firestore write buffers forever on a
		// wedged/offline channel, and this is awaited by the pre-action guard
		// for every sim click - an unbounded hang here was a Play button that
		// did NOTHING, with no toast and no log, while ordinary edits (which
		// skip the forced ping once ready) kept working. A timeout throws, the
		// guard catches it, and the user sees "Cloud sync is not ready".
		this.readyProbe = (async () => {
			try {
				await withTimeout(
					this.transport.ping?.() ?? Promise.resolve(),
					CATCHUP_READ_TIMEOUT_MS,
				);
				this.readyUntil = Date.now() + READY_TTL_MS;
				this.pushReady();
			} catch (error) {
				this.markNotReady();
				throw error;
			}
		})().finally(() => {
			this.readyProbe = undefined;
		});
		return this.readyProbe;
	}

	// MUST return a boolean: the worker's pre-action guard does
	// `const live = await engine.verifyConnection(...)` and refuses the action
	// when falsy. (Returning void here would silently block every cloud-tracked
	// action in a v2 room.)
	async verifyConnection(force = false): Promise<boolean> {
		if (this.transport.verifyConnection) {
			return (await this.transport.verifyConnection(force)) ?? true;
		}
		try {
			await this.transport.ping?.();
			return true;
		} catch {
			return false;
		}
	}

	contactAge(): number | undefined {
		return (this.transport as any).contactAge?.();
	}

	isBusyApplying(): boolean {
		return this.catchingUp;
	}

	async waitUntilIdle(timeoutMs: number): Promise<boolean> {
		const deadline = Date.now() + timeoutMs;
		while (this.catchingUp || this.draining) {
			if (Date.now() > deadline) {
				return false;
			}
			await new Promise((resolve) => setTimeout(resolve, 100));
		}
		return true;
	}

	// ---- v1-machinery surface, answered honestly ----------------------------
	// These exist so connect.ts and views consuming the engine work unchanged.
	// The machinery they report on has no v2 counterpart.

	getPersistedSeq(): number {
		return this.appliedMirror;
	}

	adoptSnapshotWatermark(_seq: number): void {}

	isResyncing(): boolean {
		return false;
	}

	markResyncNeeded(): void {
		// A v2 device that suspects itself just catches up; there is no marker
		// protocol because there is nothing else catch-up could do.
		void this.catchUp();
	}

	hasRebuildingBatches(): boolean {
		return false;
	}

	hasChangesSubscription(): boolean {
		return this.stateUnsubscribe !== undefined;
	}

	startChangesSubscription(): void {}

	// When the live listener last DELIVERED, not when the room last wrote. The
	// two look identical on a quiet room and completely different on a dead
	// listener, which is the only distinction this field exists to draw.
	getLastChangesDeliveryAt(): number {
		return this.lastStateDeliveryAt;
	}

	// Same shape as v1's, so the debug page and drive-catch-up logging consume
	// either engine. The v1-only fields report the honest constants.
	getCatchUpDiagnostics() {
		return {
			caughtUp: this.isCaughtUp(),
			persistedSeq: this.appliedMirror,
			maxSeq: this.roomVersion,
			behind: Math.max(0, this.roomVersion - this.appliedMirror),
			pendingBatches: 0,
			rebuilding: 0,
			pendingBatchDetail: [] as unknown[],
			applyFailed: false,
			failedApplies: 0,
			catchingUp: this.catchingUp,
			progressDone: this.appliedMirror,
			progressTotal: this.roomVersion,
			liveSubscription: this.hasChangesSubscription(),
		};
	}

	// What this device has seen the chain do, newest last, capped. Not a read of
	// the room's log: v2 prunes deltas behind the checkpoint, so there is no
	// durable log to page back through - the honest answer is this session's
	// versions, which is also the answer that matters when the question is "is
	// anything reaching this device". Costs nothing and reads nothing.
	async fetchRecentLog(limit?: number): Promise<ChangesetEntry[]> {
		const n = limit ?? SyncEngineV2.RECENT_VERSIONS_LIMIT;
		return this.recentVersions.slice(-n);
	}

	private noteVersion(entry: {
		version: number;
		authorId: string;
		action: string;
		at: number;
		records: number;
		attrs?: string[];
	}) {
		this.recentVersions.push({
			id: `v${entry.version}`,
			authorId: entry.authorId,
			seq: entry.version,
			action: entry.action,
			changeset: { changes: [] },
			records: entry.records,
			attrs: entry.attrs ?? [],
		});
		if (this.recentVersions.length > SyncEngineV2.RECENT_VERSIONS_LIMIT) {
			this.recentVersions.splice(
				0,
				this.recentVersions.length - SyncEngineV2.RECENT_VERSIONS_LIMIT,
			);
		}
	}

	// The v2 line for the sync-page log capture: chain state at a glance.
	getV2SnapshotLine(): string {
		return `protocol=v2 appliedVersion=${this.appliedMirror} roomVersion=${this.roomVersion} checkpointVersion=${this.roomState?.checkpointVersion ?? "none"} roomAction=${this.roomState?.action ?? "—"}`;
	}

	// A "full resync" in v2 is just the ordinary catch-up: there is exactly one
	// recovery path, and this is it.
	async resyncAll(_options?: { windowEntries?: number }): Promise<{
		total: number;
		applied: number;
		incomplete: number;
		failed: boolean;
	}> {
		const before = this.appliedMirror;
		const ok = await this.catchUp();
		const applied = Math.max(0, this.appliedMirror - before);
		return {
			total: applied,
			applied,
			incomplete: ok ? 0 : 1,
			failed: !ok,
		};
	}

	// The Force Resync button, v2 edition.
	//
	// resyncAll above is NOT that button, and using it for one was the bug: an
	// ordinary catch-up finds nothing to do the instant applied === roomVersion,
	// which is precisely the state someone pressing this is in. Their counter
	// agrees with the room; it is the DATABASE that looks wrong. So the button
	// does not ask catch-up to go find work - it declares the local database
	// untrustworthy and snaps it back to a state the chain vouches for, then
	// walks the tail.
	//
	// Same machinery a discarded stale advance already uses. The only difference
	// is who decided, and that matters in one place: the automatic backoffs
	// exist to stop a failing restore from looping, and a person who just
	// clicked is not a loop. Clear them and go.
	async forceCheckpointRestore(): Promise<{
		total: number;
		applied: number;
		incomplete: number;
		failed: boolean;
	}> {
		if (!this.transport.fetchRoomV2State || !this.transport.fetchV2Checkpoint) {
			throw new Error("This room does not support checkpoint recovery.");
		}
		// Server-fresh, because the whole premise is that this device's own view
		// of things is suspect.
		const state = await withTimeout(
			this.transport.fetchRoomV2State(),
			CATCHUP_READ_TIMEOUT_MS,
		);
		if (!state) {
			throw new Error("Couldn't read the room. Check your connection.");
		}
		if (
			state.checkpointVersion === undefined ||
			state.checkpointChunkCount === undefined
		) {
			// A brand-new room has no recovery point yet. Say so, rather than
			// running a no-op and reporting success - that is the failure this
			// whole method exists to end. (chunkCount matters as much as the
			// version: without it the restore path declines and falls through to
			// an ordinary catch-up, which is the no-op again.)
			throw new Error(
				"The room hasn't published a checkpoint yet, so there's nothing to restore from. Whoever is in charge of simming just needs the app open for a few minutes, then try again.",
			);
		}

		const restoresBefore = this.checkpointRestores;
		this.mustRecoverFromCheckpoint = true;
		this.lastCheckpointAttempt = undefined;
		this.catchupFailureStreak = 0;
		this.lastCatchupFailureAt = 0;
		syncDebugLog("v2:force-checkpoint-restore", {
			applied: this.appliedMirror,
			checkpointVersion: state.checkpointVersion,
			roomVersion: state.version,
		});

		const ok = await this.catchUp(state);
		// Never leave the flag armed on the way out: every later catch-up would
		// drag the whole league back down again.
		this.mustRecoverFromCheckpoint = false;

		// Whether the checkpoint itself landed, which is the part the user asked
		// for. The tail after it is ordinary catch-up work that the health tick
		// finishes on its own, so a walk that stalled there is "not finished
		// yet", not "your league is still wrong".
		if (this.checkpointRestores === restoresBefore) {
			throw new Error(
				"Couldn't restore from the room's checkpoint. It may be damaged - whoever is in charge of simming just needs the app open for a few minutes so a fresh one publishes, then try again.",
			);
		}
		return {
			total: 1,
			applied: 1,
			incomplete: ok ? 0 : 1,
			failed: false,
		};
	}

	// ---- Catch-up: the ONE recovery path ------------------------------------

	catchUp(hintState?: V2StateDoc): Promise<boolean> {
		// Back off after a failed pass, ALWAYS.
		//
		// This used to back off only when the room version had not moved since
		// the last failure, on the theory that a moving room means something has
		// changed and is worth another go. That is backwards. A device that is
		// behind and failing is by definition watching the room move away from
		// it, so the one case the guard excluded was the only case that mattered.
		// With another device simming, roomVersion changed on every pass and the
		// backoff never engaged at all: a phone stuck on delta@158 started a new
		// read the instant the previous one timed out, every eight seconds,
		// indefinitely.
		//
		// That is not free. Firestore's getDoc cannot be cancelled, so every
		// abandoned attempt leaves a read pending on the client forever. Pile up
		// enough and the SDK stops trying and answers "client is offline" to
		// everything - a red dot that no amount of further retrying recovers,
		// because the retrying is what caused it.
		if (this.backingOff()) {
			return Promise.resolve(false);
		}
		this.catchUpChain = this.catchUpChain
			.catch(() => false)
			// Checked AGAIN here, at execution time, and this is not redundant.
			// Passes are serialized on this chain, so a call that arrives while
			// one is in flight is queued - and it was admitted by the check above
			// against a `lastCatchupFailureAt` from before the running pass had
			// failed. It then ran the instant that pass failed, skipping the wait
			// entirely. The field capture showed it plainly: three attempts on
			// delta@174 spaced 8.019s and 8.022s apart, which is the read timeout
			// exactly, i.e. each retry starting ~19ms after the previous one gave
			// up, where the ladder should have spaced them 0s, 3s, 8s.
			.then(() => (this.backingOff() ? false : this.doCatchUp(hintState)));
		return this.catchUpChain;
	}

	private backingOff(): boolean {
		const wait =
			CATCHUP_BACKOFF_MS[
				Math.min(this.catchupFailureStreak, CATCHUP_BACKOFF_MS.length - 1)
			]!;
		return wait > 0 && Date.now() - this.lastCatchupFailureAt < wait;
	}

	private async doCatchUp(hintState?: V2StateDoc): Promise<boolean> {
		if (this.stopped || !this.transport.fetchRoomV2State) {
			return false;
		}
		// Same wall as the apply layer, hit BEFORE any network work: remote
		// state must never be applied over a league this session doesn't own.
		if (!checkApplyGuard()) {
			return false;
		}
		this.catchingUp = true;
		// Which read the pass is on, so a failure names its culprit instead of
		// logging an anonymous {} - "delta@61 timed out" is diagnosable from a
		// field capture, "error: {}" is not.
		let step = "state";
		try {
			// A bounded number of rounds: each round re-reads the pointer, so a
			// room advancing while we walk simply extends the walk. Two rounds
			// with no progress means something upstream is missing - report
			// false, never loop.
			for (let round = 0; round < 50; round++) {
				// stop() can land mid-walk (teardown, league switch). Every write
				// after it would target whatever league loads NEXT - abort here,
				// not just at entry.
				if (this.stopped) {
					return false;
				}
				step = "state";
				// Round 0 can run on the state the live listener just delivered -
				// fresher than any cache and immune to a wedged read path. Later
				// rounds re-read (the room may have moved while we walked).
				const state =
					round === 0 && hintState && hintState.version >= this.roomVersion
						? hintState
						: await withTimeout(
								this.transport.fetchRoomV2State(),
								CATCHUP_READ_TIMEOUT_MS,
							);
				if (!state) {
					return false;
				}
				this.roomState = state;
				this.roomVersion = Math.max(this.roomVersion, state.version);

				const applied = await readAppliedVersion();
				this.appliedMirror = applied;

				// A real walk (several versions, or a checkpoint restore) is worth
				// showing in the header; the ordinary one-version live apply is not.
				if (state.version - applied > 3) {
					this.reportCatchUpProgress(applied, state.version);
				}

				// A discarded stale advance leaves local records the chain never
				// carried. The delta walk can't remove those (deltas only add), so
				// recovery is a forced checkpoint restore: snap the whole database
				// back to a state the chain vouches for, then walk the tail.
				if (this.mustRecoverFromCheckpoint) {
					if (
						state.checkpointVersion === undefined ||
						state.checkpointChunkCount === undefined ||
						!this.transport.fetchV2Checkpoint
					) {
						// No checkpoint to snap back to (a brand-new room before its
						// first checkpoint). The overlap from applying the chain's
						// versions is the best available convergence; say so loudly.
						syncDebugLog("v2:recovery-no-checkpoint", {
							applied,
							roomVersion: state.version,
						});
						this.mustRecoverFromCheckpoint = false;
					} else {
						step = `checkpoint@${state.checkpointVersion}`;
						const serialized = await withTimeout(
							this.transport.fetchV2Checkpoint(
								state.checkpointVersion,
								state.checkpointChunkCount,
								state.checkpointGeneration,
							),
							CATCHUP_READ_TIMEOUT_MS,
						);
						if (serialized === undefined) {
							return false;
						}
						const payload = deserializeChangeset(
							await decompressSerialized(serialized),
						);
						await applyCheckpointV2(payload, state.checkpointVersion);
						this.appliedMirror = state.checkpointVersion;
						this.checkpointRestores += 1;
						this.mustRecoverFromCheckpoint = false;
						syncDebugLog("v2:recovered-from-checkpoint", {
							checkpointVersion: state.checkpointVersion,
						});
						await this.afterRemoteApply("checkpoint");
						continue;
					}
				}

				const plan = catchUpPlan(applied, state);

				if (plan.type === "caught-up") {
					this.catchupFailureStreak = 0;
					this.notifiedFailingStep = undefined;
					this.clearCatchUpProgress();
					return true;
				}

				if (plan.type === "checkpoint-then-deltas") {
					if (
						state.checkpointChunkCount === undefined ||
						!this.transport.fetchV2Checkpoint
					) {
						return false;
					}
					// A restore is the single most expensive thing a device can do:
					// download, decompress and parse the whole league. Retrying that
					// on every health tick is how a REFUSED restore (a checkpoint
					// this device will never accept) turned into a full-league parse
					// every five seconds until the phone ran out of memory. One
					// attempt per checkpoint per backoff window; a genuinely
					// transient failure still gets retried, just not instantly.
					const attemptKey = `${plan.checkpointVersion}:${state.checkpointGeneration ?? ""}`;
					if (
						this.lastCheckpointAttempt?.key === attemptKey &&
						Date.now() - this.lastCheckpointAttempt.at <
							CHECKPOINT_RESTORE_RETRY_MS
					) {
						syncDebugLog("v2:checkpoint-restore-backoff", {
							version: plan.checkpointVersion,
						});
						return false;
					}
					this.lastCheckpointAttempt = { key: attemptKey, at: Date.now() };
					step = `checkpoint@${plan.checkpointVersion}`;
					const serialized = await withTimeout(
						this.transport.fetchV2Checkpoint(
							plan.checkpointVersion,
							state.checkpointChunkCount,
							state.checkpointGeneration,
						),
						CATCHUP_READ_TIMEOUT_MS,
					);
					if (serialized === undefined) {
						return false;
					}
					const payload = deserializeChangeset(
						await decompressSerialized(serialized),
					);
					// Validated inside (and refused with the local league untouched
					// if the payload is not a playable league).
					await applyCheckpointV2(payload, plan.checkpointVersion);
					this.appliedMirror = plan.checkpointVersion;
					this.checkpointRestores += 1;
					await this.afterRemoteApply("checkpoint");
					continue;
				}

				// Deltas, in order. Any miss aborts the pass; the next pass retries.
				let progressed = false;
				for (const version of plan.versions) {
					step = `delta@${version}`;
					// The head version's payload rides inline on the pointer doc when
					// small (most interactive edits). Using it needs no read at all -
					// which is why a roster move now lands the moment the pointer
					// push arrives, even when chunk reads are hanging.
					const delta =
						version === state.version && state.inlineDelta !== undefined
							? {
									serialized: state.inlineDelta,
									authorId: state.authorId,
									action: state.action ?? "?",
									at: state.at,
								}
							: this.transport.fetchV2Delta
								? await withTimeout(
										this.transport.fetchV2Delta(version),
										CATCHUP_READ_TIMEOUT_MS,
									)
								: undefined;
					if (!delta) {
						syncDebugLog("v2:delta-missing", { version });
						return false;
					}
					const changeset = deserializeChangeset(
						await decompressSerialized(delta.serialized),
					) as Changeset;
					const outcome = await applyVersionedChangeset({
						version,
						authorId: delta.authorId,
						action: delta.action,
						changeset,
						at: delta.at,
					});
					if (outcome === "gap") {
						// The marker moved backwards relative to the plan - only possible
						// if a checkpoint restore is needed after all. Re-plan.
						break;
					}
					this.noteVersion({
						version,
						authorId: delta.authorId,
						action: delta.action,
						at: delta.at,
						records: changeset.changes.length,
						attrs: changeset.changes
							.filter((c) => c.store === "gameAttributes")
							.map((c) => String(c.id)),
					});
					this.appliedMirror = Math.max(this.appliedMirror, version);
					progressed = true;
					// A delta that applied is proof the connection works, so the
					// failure streak starts over. Without this the streak only ever
					// reset on reaching the head, which meant a device walking a big
					// gap over a patchy link accumulated failures across passes that
					// were each mostly SUCCESSFUL - fifty versions applied and one
					// read lost still counted as a pass that failed. The streak then
					// only went up, which is how the retry cadence and the network
					// cycling both ended up permanently in their most aggressive
					// state on the device that could least afford it.
					this.catchupFailureStreak = 0;
					if (this.catchUpPillShown) {
						this.reportCatchUpProgress(this.appliedMirror, this.roomVersion);
					}
					await this.afterRemoteApply(changeset);
				}
				if (!progressed) {
					return false;
				}
				// The walk consumed everything the pointer said exists. Declaring
				// success here (instead of one more confirming pointer read) is what
				// lets a hint-fed pass finish with ZERO reads; if the room moved
				// mid-walk, the listener/probe delivers the newer pointer and the
				// next pass handles it.
				if (this.appliedMirror >= this.roomVersion) {
					this.catchupFailureStreak = 0;
					this.notifiedFailingStep = undefined;
					this.clearCatchUpProgress();
					return true;
				}
			}
			return false;
		} catch (error) {
			syncDebugLog("v2:catchup-failed", { error: String(error), step });
			// A TIMED-OUT read is the wedge signature (the pointer push worked,
			// the read hangs), so it counts double: the cycle fires after ONE
			// timeout instead of two - halving the stall the user sits through.
			this.catchupFailureStreak += String(error).includes("Timed out") ? 2 : 1;
			this.lastCatchupFailureAt = Date.now();
			// Repeated identical failures deserve one honest line to the user -
			// the retrying is automatic, but silence while stuck reads as broken.
			if (
				this.catchupFailureStreak === 3 &&
				this.notifiedFailingStep !== step
			) {
				this.notifiedFailingStep = step;
				try {
					logEvent({
						type: "error",
						text: "Having trouble loading the league's shared data. Retrying automatically.",
						saveToDb: false,
						persistent: true,
					});
				} catch {
					// UI notice only.
				}
			}
			// The user is behind and fetches are failing - the one moment they
			// most need the header to say "working on it" instead of nothing.
			if (this.roomVersion > this.appliedMirror) {
				this.reportCatchUpProgress(this.appliedMirror, this.roomVersion);
			}
			await this.maybeCycleNetwork("catchup");
			return false;
		} finally {
			this.catchingUp = false;
		}
	}

	// After remote state landed: run the SAME receiver-side refresh the v1
	// apply path runs - g reload, season cache refill, phase text, Play menu,
	// status line, phase redirect, score ticker, realtimeUpdate. Skipping any
	// of it leaves a follower's screen frozen on the old world even though its
	// database moved (the original v2 field bug: the phase flipped in data but
	// the device sat on the Draft Lottery page until a manual refresh).
	// Failure is cosmetic, never a reason to fail the apply that committed.
	private async afterRemoteApply(changeset: Changeset | "checkpoint") {
		try {
			if (changeset === "checkpoint") {
				// A full-state restore touched everything; refresh everything. No
				// redirect - yanking someone to a phase landing page mid-recovery
				// is a surprise, not a continuation.
				await refreshAfterApply({
					touchedSeason: true,
					touchedGameAttributes: true,
					touchedGames: true,
					touchedPhase: true,
					touchedStatus: true,
					touchedStores: new Set([
						"draftLotteryResults",
						"teams",
						"allStars",
						"scheduledEvents",
					]),
					refreshUI: true,
					sweepGames: false,
					redirect: false,
				});
			} else {
				await refreshAfterApply({
					...summarizeChangesetForRefresh(changeset),
					refreshUI: true,
					// v2 applies are atomic - phantoms and stranded days cannot exist.
					sweepGames: false,
					redirect: true,
				});
			}
		} catch {
			// The next navigation shows the applied state regardless.
		}
	}

	// ---- Publishing: the ONE write path -------------------------------------

	// Same contract afterAction already speaks: durably queue, then drain.
	// Notifications ride with the entry and fire only on a confirmed commit.
	async onLocalChangeset(
		changeset: Changeset,
		action: string,
		notifications?: SyncNotification[],
	): Promise<"confirmed" | "queued"> {
		if (changeset.changes.length === 0) {
			return "confirmed";
		}
		const entry: Omit<ChangesetEntry, "seq"> = {
			id: makeId(),
			authorId: this.transport.clientId,
			action,
			changeset,
		};
		if (this.code !== undefined) {
			await outbox.addAll(this.code, [entry]);
			if (notifications && notifications.length > 0) {
				await outbox.addNotifications(this.code, entry.id, notifications);
			}
		} else {
			this.memoryQueue.push(entry);
			if (notifications && notifications.length > 0) {
				this.memoryNotifications.set(entry.id, notifications);
			}
		}
		void this.reportPending();
		const drained = await this.drainOutbox();
		return drained ? "confirmed" : "queued";
	}

	drainOutbox(): Promise<boolean> {
		this.drainChain = this.drainChain
			.catch(() => false)
			.then(() => this.doDrain());
		return this.drainChain;
	}

	private async pendingEntries(): Promise<Omit<ChangesetEntry, "seq">[]> {
		if (this.code === undefined) {
			return [...this.memoryQueue];
		}
		return outbox.pending(this.code);
	}

	async pendingUploadCount(): Promise<number> {
		if (this.code === undefined) {
			return this.memoryQueue.length;
		}
		return outbox.count(this.code);
	}

	private async reportPending() {
		try {
			this.onPendingChange?.(await this.pendingUploadCount());
		} catch {
			// Display only.
		}
	}

	private async removePending(id: string) {
		if (this.code === undefined) {
			this.memoryQueue = this.memoryQueue.filter((e) => e.id !== id);
		} else {
			await outbox.remove(id);
		}
		void this.reportPending();
	}

	private async takeNotifications(
		id: string,
	): Promise<SyncNotification[] | undefined> {
		if (this.code === undefined) {
			const found = this.memoryNotifications.get(id);
			this.memoryNotifications.delete(id);
			return found;
		}
		return (await outbox.takeNotifications(id)) as
			| SyncNotification[]
			| undefined;
	}

	private async doDrain(): Promise<boolean> {
		if (this.stopped) {
			return false;
		}
		// Never touch the chain while the loaded league is not this session's
		// league (a switch mid-session, a zombie engine): publishing would read
		// the WRONG league's marker and could mint versions from it.
		if (!checkApplyGuard()) {
			return false;
		}
		this.draining = true;
		try {
			const pending = await this.pendingEntries();
			if (pending.length === 0) {
				return true;
			}

			// Show the upload while it is happening. A sim is the one action where
			// the user genuinely needs to know not to close the app yet, and a
			// silent header during a slow push reads as "nothing is happening".
			let done = 0;
			this.onUploadingChange?.({ done, total: pending.length });
			for (const entry of pending) {
				// Every device publishes the same way: as the next version. No
				// change ever waits on another device being online.
				const ok = await this.publishAsVersion(entry);
				if (!ok) {
					return false;
				}
				done += 1;
				this.onUploadingChange?.({ done, total: pending.length });
			}
			this.onUploadComplete?.();
			return true;
		} catch (error) {
			syncDebugLog("v2:drain-failed", { error });
			return false;
		} finally {
			this.draining = false;
			// Cleared on every exit - a drain that fails must not leave the header
			// claiming an upload is still in flight.
			this.onUploadingChange?.(undefined);
		}
	}

	// The ONE publish path, for every device: mint the next version. The
	// compare-and-set on the room's version pointer is the entire concurrency
	// story - whoever wins the CAS authored that version, the loser re-reads.
	// No change ever depends on another device being online.
	//
	// Staleness has two answers, on purpose (see actionLabels.ts):
	// - An ordinary edit whose base moved catches up and republishes on top of
	//   the new head - it is a whole-record statement of user intent, and it
	//   survives a race. Bounded retries; on exhaustion the entry stays
	//   durably queued for the next drain kick.
	// - A TIMELINE ADVANCE whose base moved is DISCARDED, loudly, and the
	//   device snaps back to the chain via checkpoint recovery. A sim day
	//   derived from a world the room has moved past is exactly the artifact
	//   that forked v1 leagues when it was merged late.
	private async publishAsVersion(
		entry: Omit<ChangesetEntry, "seq">,
	): Promise<boolean> {
		if (
			!this.transport.publishV2Delta ||
			!this.transport.commitV2Version ||
			!this.transport.fetchRoomV2State
		) {
			return false;
		}
		const isAdvance = isTimelineAdvanceLabel(entry.action);

		for (let attempt = 0; attempt < 3; attempt++) {
			// Never author on top of state we don't have: catch up first,
			// publish second. The head is the local mirror (kept fresh by the
			// listener, the head probe, and our own commits) - NOT a fresh
			// server read, which can lag this device's own just-committed
			// version by a beat and made a burst of edits fight itself with
			// CAS conflicts on every entry. The mirror being stale-low is
			// harmless: publishV2Delta's transaction refuses a taken slot and
			// the commit CAS refuses a moved head, so the worst case is one
			// retry - which DOES re-read the server, as does a device that has
			// never seen the state doc at all.
			if (attempt > 0 || this.roomState === undefined) {
				const state = await withTimeout(
					this.transport.fetchRoomV2State(),
					this.publishTimeoutMs,
				);
				if (!state) {
					return false;
				}
				this.roomState = state;
				this.roomVersion = Math.max(this.roomVersion, state.version);
			}
			const applied = await readAppliedVersion();
			if (applied < this.roomVersion) {
				if (isAdvance) {
					return this.discardStaleAdvance(entry, applied, this.roomVersion);
				}
				const caught = await this.doCatchUpInline();
				if (!caught) {
					return false;
				}
			}

			const target = Math.max(this.roomVersion, applied) + 1;
			const serialized = await compressSerialized(
				serializeChangeset(entry.changeset),
			);
			// ONE stamp for the payload and the commit: the commit transaction
			// verifies the chunks it points at carry this exact (authorId, at),
			// so it can never bless a payload another racer overwrote.
			const at = Date.now();
			const commit = {
				version: target,
				authorId: this.transport.clientId,
				byName: this.localName,
				at,
				action: entry.action,
				// Small payloads ride the pointer doc itself, so receivers
				// apply them straight off the listener push with no reads.
				inlineDelta:
					serialized.length <= INLINE_DELTA_LIMIT ? serialized : undefined,
			};

			// The fast path, which is nearly every change a person makes: a
			// payload small enough to ride the pointer is also small enough to be
			// written in the same transaction as the pointer, so publishing costs
			// ONE round trip instead of two. Nothing else about the protocol
			// changes - same slot, same CAS, same meaning on failure.
			let won: boolean;
			if (
				commit.inlineDelta !== undefined &&
				this.transport.publishAndCommitV2Version
			) {
				won = await withTimeout(
					this.transport.publishAndCommitV2Version(
						commit,
						serialized,
						target - 1,
					),
					this.publishTimeoutMs,
				);
			} else {
				try {
					await withTimeout(
						this.transport.publishV2Delta(
							{
								version: target,
								authorId: this.transport.clientId,
								action: entry.action,
								at,
							},
							serialized,
						),
						this.publishTimeoutMs,
					);
				} catch (error) {
					if ((error as Error)?.message === "v2-slot-taken") {
						// Someone committed this version while we prepared - same
						// meaning as losing the CAS, caught one step earlier.
						syncDebugLog("v2:slot-taken", {
							action: entry.action,
							target,
							attempt,
						});
						if (isAdvance) {
							return this.discardStaleAdvance(entry, applied, target);
						}
						continue;
					}
					throw error;
				}

				won = await withTimeout(
					this.transport.commitV2Version(commit, target - 1),
					this.publishTimeoutMs,
				);
			}

			if (!won) {
				// Someone committed target concurrently - a genuine same-second
				// race, since the pre-action guard verified we started current.
				syncDebugLog("v2:cas-lost", {
					action: entry.action,
					target,
					attempt,
				});
				if (isAdvance) {
					return this.discardStaleAdvance(entry, applied, target);
				}
				// Their version applies first, then this edit goes on top.
				continue;
			}

			// The local database ALREADY contains this mutation (it was made
			// here); the marker advances to match in its own transaction. A
			// crash before this lands is healed by the next catch-up
			// re-applying version `target` over identical records - idempotent
			// by construction.
			await this.writeMarker(target);
			this.noteVersion({
				version: target,
				authorId: this.transport.clientId,
				action: entry.action,
				at,
				records: entry.changeset.changes.length,
				attrs: entry.changeset.changes
					.filter((c) => c.store === "gameAttributes")
					.map((c) => String(c.id)),
			});
			this.appliedMirror = Math.max(this.appliedMirror, target);
			this.roomVersion = Math.max(this.roomVersion, target);

			await this.removePending(entry.id);
			const notifications = await this.takeNotifications(entry.id);
			for (const notification of notifications ?? []) {
				void this.publishNotification(notification).catch(() => {});
			}
			syncDebugLog("v2:published", { version: target, action: entry.action });
			return true;
		}

		// Retries exhausted (a CAS storm - takes 3 concurrent commits inside a
		// few seconds). The entry stays durably queued; the health tick keeps
		// draining, and the next pass starts from a fresh read of the head.
		syncDebugLog("v2:publish-retries-exhausted", { action: entry.action });
		return false;
	}

	// A timeline advance authored on a world the room has moved past: remove
	// it from the queue, drop its notifications, tell the user plainly, and
	// snap this device back to the chain's truth. The local database contains
	// records the chain will never carry, so the recovery is a forced
	// checkpoint restore - the same "reject the local change" rule that keeps
	// every device an exact copy of the cloud.
	private async discardStaleAdvance(
		entry: Omit<ChangesetEntry, "seq">,
		applied: number,
		roomVersion: number,
	): Promise<boolean> {
		syncDebugLog("v2:stale-advance-discarded", {
			action: entry.action,
			applied,
			roomVersion,
		});
		console.error(
			`[sync] Discarded "${entry.action}": the league advanced on another device before this device's advance reached the cloud. Restoring this device to the room's state.`,
		);
		try {
			logEvent({
				type: "error",
				text: "Your last advance didn't reach the cloud before the league moved on from another device, so it was undone here. This device is re-syncing to the room's state.",
				saveToDb: false,
				persistent: true,
			});
		} catch {
			// UI notice only.
		}
		await this.removePending(entry.id);
		await this.takeNotifications(entry.id);
		this.mustRecoverFromCheckpoint = true;
		void this.catchUp();
		return false;
	}

	// Not a single-flight re-entry (doDrain holds the drain lock; catchUp's
	// chain would deadlock against callers awaiting the drain).
	private async doCatchUpInline(): Promise<boolean> {
		const wasCatchingUp = this.catchingUp;
		try {
			return await this.doCatchUp();
		} finally {
			this.catchingUp = wasCatchingUp;
		}
	}

	// ---- Checkpoints ---------------------------------------------------------

	// Called from the health tick, authority only. Publishes when the chain has
	// outgrown the last checkpoint (or has none), through the same integrity
	// gates as v1 - a damaged league cannot become the room's recovery point.
	//
	// SINGLE-FLIGHT, and this is not optional: a checkpoint build+upload takes
	// longer than one health tick, and two overlapping publishes of the same
	// version used to interleave their chunk writes into the same documents -
	// producing a checkpoint that no device could ever read (the field failure
	// that stranded a fresh joiner at version 0 retrying forever).
	async maybePublishCheckpoint({
		// Tests drive the cadence logic directly; production never passes this.
		enabled = AUTO_PUBLISH_CHECKPOINTS,
	}: { enabled?: boolean } = {}): Promise<void> {
		// See AUTO_PUBLISH_CHECKPOINTS: v2 uses the same whole-league builder, so
		// it has exactly the same way of killing a phone that holds authority.
		if (!enabled) {
			return;
		}
		if (
			this.stopped ||
			this.publishingCheckpoint ||
			!this.isAuthority() ||
			!this.transport.publishV2Checkpoint ||
			!this.transport.commitV2Checkpoint ||
			this.catchingUp ||
			this.draining ||
			lock.get("gameSim") ||
			lock.get("newPhase") ||
			local.autoPlayUntil
		) {
			return;
		}
		// The one wall that matters most here: NEVER publish the loaded league
		// into a room it doesn't belong to. This path reads the whole current
		// database; without the guard, a league switch mid-session (or an
		// engine that outlived one) would checkpoint league A's data into
		// league B's room - full cross-league contamination in one write.
		if (!checkApplyGuard()) {
			return;
		}
		// Cheap to ask, ruinous to do - and nothing about a checkpoint is
		// urgent. Rate-limit the whole consideration, like v1 does, so an
		// active session can never be interrupted by the rebuild the moment
		// the version interval elapses.
		const now = Date.now();
		if (now - this.lastCheckpointCheckAt < CHECKPOINT_CHECK_MIN_MS) {
			return;
		}
		this.lastCheckpointCheckAt = now;
		this.publishingCheckpoint = true;
		try {
			// The head comes from the engine's mirror: the same health tick that
			// calls this just ran probeHead (a server read of this exact doc), so
			// a second fetch bought nothing. A stale mirror can only DELAY a
			// checkpoint by a tick; commitV2Checkpoint stamps `applied`, which is
			// this device's own database truth, so it can never mislabel one.
			const state = this.roomState;
			if (!state) {
				return;
			}
			const applied = await readAppliedVersion();
			if (applied < state.version) {
				return;
			}

			const last = state.checkpointVersion ?? 0;
			let mustReplace = false;
			if (
				state.checkpointVersion !== undefined &&
				state.checkpointChunkCount !== undefined &&
				!this.checkpointVettedForSession &&
				this.transport.fetchV2Checkpoint
			) {
				// Once per session, prove the room's recovery point is actually
				// usable: fetch it and parse it like a joiner would. An unreadable
				// checkpoint strands every future joiner, and only a caught-up
				// device can replace it - so the moment one notices, it must.
				this.checkpointVettedForSession = true;
				// Also a whole-league parse, so it is also timed - once per
				// session rather than per interval, but on a phone it is the same
				// order of cost as the publish it guards.
				const vetStartedAt = Date.now();
				try {
					const serialized = await this.transport.fetchV2Checkpoint(
						state.checkpointVersion,
						state.checkpointChunkCount,
						state.checkpointGeneration,
					);
					if (serialized === undefined) {
						mustReplace = true;
					} else {
						const payload = deserializeChangeset(
							await decompressSerialized(serialized),
						);
						if (validateRoomSnapshotPayload(payload).length > 0) {
							mustReplace = true;
						} else {
							// Only a DIFFERENT identity means another league's state is
							// sitting in this room, which every restorer refuses and which
							// therefore strands fresh joiners. A checkpoint with no
							// identity just predates the protection; restorers accept it,
							// so rebuilding the whole league over it would be an expensive
							// no-op.
							const localLeagueId = await readLocalLeagueId();
							const checkpointLeagueId = payloadLeagueId(
								(payload as any)?.stores,
							);
							if (
								localLeagueId !== undefined &&
								checkpointLeagueId !== undefined &&
								checkpointLeagueId !== localLeagueId
							) {
								mustReplace = true;
							}
						}
					}
				} catch {
					mustReplace = true;
				}
				syncDebugLog("v2:checkpoint-vetted", {
					version: state.checkpointVersion,
					ms: Date.now() - vetStartedAt,
					mustReplace,
				});
				if (mustReplace) {
					syncDebugLog("v2:checkpoint-unreadable-replacing", {
						version: state.checkpointVersion,
					});
				}
			}

			if (
				!mustReplace &&
				state.checkpointVersion !== undefined &&
				state.version - last < CHECKPOINT_EVERY_VERSIONS
			) {
				return;
			}

			const { problems } = await repairLeagueHistory("v2-pre-checkpoint");
			if (problems.length > 0) {
				syncDebugLog("v2:checkpoint-blocked-history", { problems });
				return;
			}
			const integrityProblems = await checkLeagueIntegrity();
			if (integrityProblems.length > 0) {
				syncDebugLog("v2:checkpoint-blocked-integrity", {
					problems: integrityProblems,
				});
				return;
			}

			// The expensive part: the entire league into memory, stringified and
			// gzipped. Timed and sized so the capture shows what this actually
			// costs on the device that runs it - the number that would have made
			// the 25-version cadence obviously wrong.
			//
			// And on a device where it does not fit, it is not slow, it is fatal:
			// iOS kills the worker mid-build, the app reloads, the authority finds
			// the room still has no checkpoint, and builds again. Crashing with
			// nobody touching the phone. The cadence constants above cannot help,
			// because a crash resets them - only a note that survives the process
			// can. One attempt; a device that cannot do this stops volunteering and
			// the room waits for one that can.
			const publishOp = "checkpoint-publish";
			const publishLid = g.get("lid");
			if (!(await claimRecoveryAttempt(publishLid, publishOp))) {
				console.error(
					"[sync] Not building a room checkpoint on this device: the last attempt didn't finish (it likely ran out of memory). Another device in the room will publish one.",
				);
				return;
			}
			let payload;
			let serialized;
			const buildStartedAt = Date.now();
			try {
				payload = await buildRoomSnapshotPayload();
				serialized = await compressSerialized(serializeChangeset(payload));
			} finally {
				await clearRecoveryAttempt(publishLid, publishOp);
			}
			const buildMs = Date.now() - buildStartedAt;
			// A fresh generation per publish: chunks land at generation-unique doc
			// ids and the pointer flips to them only at commit, so a reader can
			// NEVER see half of one publish spliced with half of another.
			const generation = makeId().slice(0, 8);
			const chunkCount = await this.transport.publishV2Checkpoint(
				applied,
				serialized,
				generation,
			);
			await this.transport.commitV2Checkpoint(applied, chunkCount, generation);
			syncDebugLog("v2:checkpoint-published", {
				version: applied,
				chunkCount,
				generation,
				bytes: serialized.length,
				buildMs,
			});

			if (last > 0 && !mustReplace) {
				try {
					await this.transport.deleteV2DeltasBefore?.(last);
				} catch {
					// Housekeeping.
				}
			}
		} catch (error) {
			syncDebugLog("v2:checkpoint-failed", { error: String(error) });
		} finally {
			this.publishingCheckpoint = false;
		}
	}

	// ---- Internals -----------------------------------------------------------

	private async writeMarker(version: number) {
		const transaction = (idb.league as any).transaction(
			"gameAttributes",
			"readwrite",
		);
		transaction.objectStore("gameAttributes").put({
			key: "syncV2AppliedVersion",
			value: version,
		});
		await transaction.done;
		void g;
	}
}
