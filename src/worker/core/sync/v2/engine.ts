import { g, local, lock, logEvent } from "../../../util/index.ts";
import { idb } from "../../../db/index.ts";
import { outbox } from "../outbox.ts";
import {
	compressSerialized,
	decompressSerialized,
	deserializeChangeset,
	serializeChangeset,
} from "../serialize.ts";
import { buildRoomSnapshotPayload } from "../roomSnapshot.ts";
import { repairLeagueHistory } from "../historyRepair.ts";
import { checkLeagueIntegrity } from "../leagueIntegrity.ts";
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
// one. Small on purpose: the checkpoint is the only recovery, and v2
// checkpoints are cheap to take (same builder as v1, gzipped).
const CHECKPOINT_EVERY_VERSIONS = 25;

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

// The head probe runs every 5s health tick; its read must resolve or fail
// well within one tick so probes never pile up behind a hung one.
const HEAD_PROBE_TIMEOUT_MS = 4_000;

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
	private isHostFlag: boolean;
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
	private ready = false;

	// Set when a stale timeline advance was discarded: the local database
	// contains a mutation the chain will never carry, so the next catch-up must
	// rebuild from the checkpoint instead of trusting the delta walk alone.
	private mustRecoverFromCheckpoint = false;

	// Head-probe bookkeeping (see probeHead): single-flight guard, consecutive
	// failure count, and a throttle on the network-cycle remedy.
	private probing = false;
	private headFailureStreak = 0;
	private lastNetworkCycleAt = 0;

	private memoryQueue: Omit<ChangesetEntry, "seq">[] = [];
	private memoryNotifications = new Map<string, SyncNotification[]>();

	private onAuthorityChange: ((a: Authority | undefined) => void) | undefined;
	private onReadyChange: ((ready: boolean) => void) | undefined;
	private onPendingChange: ((count: number) => void) | undefined;
	private onUploadComplete: (() => void) | undefined;
	private publishTimeoutMs: number;

	constructor(
		transport: SyncTransport,
		options: {
			isHost?: boolean;
			code?: string;
			onAuthorityChange?: (a: Authority | undefined) => void;
			onReadyChange?: (ready: boolean) => void;
			onPendingChange?: (count: number) => void;
			onUploadComplete?: () => void;
			// Test hook: shrink the publish-attempt timeout.
			publishTimeoutMs?: number;
		} = {},
	) {
		this.transport = transport;
		this.isHostFlag = options.isHost ?? false;
		this.code = options.code;
		this.onAuthorityChange = options.onAuthorityChange;
		this.onReadyChange = options.onReadyChange;
		this.onPendingChange = options.onPendingChange;
		this.onUploadComplete = options.onUploadComplete;
		this.publishTimeoutMs =
			options.publishTimeoutMs ?? PUBLISH_ATTEMPT_TIMEOUT_MS;
	}

	get clientId(): string {
		return this.transport.clientId;
	}

	// ---- Lifecycle ---------------------------------------------------------

	start() {
		this.authorityUnsubscribe = this.transport.subscribeAuthority?.(
			(authority) => {
				this.authority = authority;
				this.onAuthorityChange?.(authority);
			},
		);
		this.subscribeState();
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
				this.roomState = state;
				if (state.version > this.roomVersion) {
					this.roomVersion = state.version;
				}
				if (state.version > this.appliedMirror) {
					void this.catchUp();
				}
			},
			() => {
				if (this.stopped) {
					return;
				}
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
		if (this.stopped || !this.transport.fetchRoomV2State || this.probing) {
			return;
		}
		this.probing = true;
		try {
			const state = await withTimeout(
				this.transport.fetchRoomV2State(),
				HEAD_PROBE_TIMEOUT_MS,
			);
			this.headFailureStreak = 0;
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
				void this.catchUp();
			}
		} catch {
			this.headFailureStreak += 1;
			if (
				this.headFailureStreak >= 2 &&
				this.transport.cycleNetwork &&
				Date.now() - this.lastNetworkCycleAt > 30_000
			) {
				this.headFailureStreak = 0;
				this.lastNetworkCycleAt = Date.now();
				syncDebugLog("v2:network-cycled", {});
				try {
					await this.transport.cycleNetwork();
				} catch {
					// Even a failed cycle is followed by a fresh listener + catch-up.
				}
				this.subscribeState();
				void this.catchUp();
			}
		} finally {
			this.probing = false;
		}
	}

	stop() {
		this.stopped = true;
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

	getIsHost(): boolean {
		return this.isHostFlag;
	}

	async claimAuthority() {
		await this.transport.claimAuthority?.(
			this.transport.clientId,
			this.localName,
		);
		this.isHostFlag = true;
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
		return this.ready;
	}

	async ensureReady(force = false): Promise<void> {
		if (this.ready && !force) {
			return;
		}
		await this.transport.ping?.();
		this.ready = true;
		this.onReadyChange?.(true);
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

	getLastChangesDeliveryAt(): number {
		return this.roomState?.at ?? 0;
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

	async fetchRecentLog(_limit?: number): Promise<ChangesetEntry[]> {
		return [];
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

	// ---- Catch-up: the ONE recovery path ------------------------------------

	catchUp(): Promise<boolean> {
		this.catchUpChain = this.catchUpChain
			.catch(() => false)
			.then(() => this.doCatchUp());
		return this.catchUpChain;
	}

	private async doCatchUp(): Promise<boolean> {
		if (this.stopped || !this.transport.fetchRoomV2State) {
			return false;
		}
		this.catchingUp = true;
		try {
			// A bounded number of rounds: each round re-reads the pointer, so a
			// room advancing while we walk simply extends the walk. Two rounds
			// with no progress means something upstream is missing - report
			// false, never loop.
			for (let round = 0; round < 50; round++) {
				const state = await withTimeout(
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
						const serialized = await withTimeout(
							this.transport.fetchV2Checkpoint(
								state.checkpointVersion,
								state.checkpointChunkCount,
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
					return true;
				}

				if (plan.type === "checkpoint-then-deltas") {
					if (
						state.checkpointChunkCount === undefined ||
						!this.transport.fetchV2Checkpoint
					) {
						return false;
					}
					const serialized = await withTimeout(
						this.transport.fetchV2Checkpoint(
							plan.checkpointVersion,
							state.checkpointChunkCount,
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
					await this.afterRemoteApply("checkpoint");
					continue;
				}

				// Deltas, in order. Any miss aborts the pass; the next pass retries.
				let progressed = false;
				for (const version of plan.versions) {
					const delta = this.transport.fetchV2Delta
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
					this.appliedMirror = Math.max(this.appliedMirror, version);
					progressed = true;
					await this.afterRemoteApply(changeset);
				}
				if (!progressed) {
					return false;
				}
			}
			return false;
		} catch (error) {
			syncDebugLog("v2:catchup-failed", { error });
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
		this.draining = true;
		try {
			const pending = await this.pendingEntries();
			if (pending.length === 0) {
				return true;
			}

			for (const entry of pending) {
				// Every device publishes the same way: as the next version. No
				// change ever waits on another device being online.
				const ok = await this.publishAsVersion(entry);
				if (!ok) {
					return false;
				}
			}
			this.onUploadComplete?.();
			return true;
		} catch (error) {
			syncDebugLog("v2:drain-failed", { error });
			return false;
		} finally {
			this.draining = false;
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
			// publish second. The head is the MAX of the server read and the
			// local mirror: a server/cache read can lag this device's own
			// just-committed version by a beat, and trusting it alone made a
			// burst of edits fight itself with CAS conflicts on every entry
			// (each one targeting the version its predecessor just took).
			const state = await withTimeout(
				this.transport.fetchRoomV2State(),
				this.publishTimeoutMs,
			);
			if (!state) {
				return false;
			}
			this.roomVersion = Math.max(this.roomVersion, state.version);
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

			const won = await withTimeout(
				this.transport.commitV2Version(
					{
						version: target,
						authorId: this.transport.clientId,
						byName: this.localName,
						at,
						action: entry.action,
					},
					target - 1,
				),
				this.publishTimeoutMs,
			);

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
	async maybePublishCheckpoint(): Promise<void> {
		if (
			this.stopped ||
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
		try {
			const state = await this.transport.fetchRoomV2State?.();
			if (!state) {
				return;
			}
			const last = state.checkpointVersion ?? 0;
			if (
				state.checkpointVersion !== undefined &&
				state.version - last < CHECKPOINT_EVERY_VERSIONS
			) {
				return;
			}
			const applied = await readAppliedVersion();
			if (applied < state.version) {
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

			const payload = await buildRoomSnapshotPayload();
			const serialized = await compressSerialized(serializeChangeset(payload));
			const chunkCount = await this.transport.publishV2Checkpoint(
				applied,
				serialized,
			);
			await this.transport.commitV2Checkpoint(applied, chunkCount);
			syncDebugLog("v2:checkpoint-published", {
				version: applied,
				chunkCount,
			});

			if (last > 0) {
				try {
					await this.transport.deleteV2DeltasBefore?.(last);
				} catch {
					// Housekeeping.
				}
			}
		} catch (error) {
			syncDebugLog("v2:checkpoint-failed", { error });
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
