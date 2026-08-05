import { g, local, lock, toUI } from "../../../util/index.ts";
import { league } from "../../index.ts";
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
import type { Changeset } from "../changeset.ts";
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
	applyRequestRecordsLocally,
	applyVersionedChangeset,
	readAppliedVersion,
} from "./applyVersion.ts";
import { catchUpPlan } from "./protocol.ts";

// ---------------------------------------------------------------------------
// ENGINE V2: the version chain, running.
//
// The v1 engine spends thousands of lines deciding which of two disagreeing
// databases is right. This engine cannot have that problem, because it never
// constructs the disagreement: it applies exactly the next version or nothing
// (applyVersion.ts), only the authority mints versions and only by
// compare-and-set (the pointer cannot fork), and a follower's edits travel as
// requests the authority folds into the chain. Recovery from ANY distance is
// one code path: catchUpPlan says checkpoint-then-deltas or deltas, both
// bounded, both ordered, both atomic per step.
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
	private requestTimer: ReturnType<typeof setInterval> | undefined;
	private ready = false;

	private memoryQueue: Omit<ChangesetEntry, "seq">[] = [];
	private memoryNotifications = new Map<string, SyncNotification[]>();

	private onAuthorityChange: ((a: Authority | undefined) => void) | undefined;
	private onReadyChange: ((ready: boolean) => void) | undefined;
	private onPendingChange: ((count: number) => void) | undefined;
	private onUploadComplete: (() => void) | undefined;

	constructor(
		transport: SyncTransport,
		options: {
			isHost?: boolean;
			code?: string;
			onAuthorityChange?: (a: Authority | undefined) => void;
			onReadyChange?: (ready: boolean) => void;
			onPendingChange?: (count: number) => void;
			onUploadComplete?: () => void;
		} = {},
	) {
		this.transport = transport;
		this.isHostFlag = options.isHost ?? false;
		this.code = options.code;
		this.onAuthorityChange = options.onAuthorityChange;
		this.onReadyChange = options.onReadyChange;
		this.onPendingChange = options.onPendingChange;
		this.onUploadComplete = options.onUploadComplete;
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
		this.stateUnsubscribe = this.transport.subscribeRoomV2State?.((state) => {
			this.roomState = state;
			if (state.version > this.roomVersion) {
				this.roomVersion = state.version;
			}
			if (state.version > this.appliedMirror) {
				void this.catchUp();
			}
		});
		// The authority folds follower requests into the chain. Light poll: one
		// small query, and only for the authority.
		this.requestTimer = setInterval(() => {
			if (this.isAuthority()) {
				void this.drainRequests();
			}
		}, 10_000);
		const nodeTimer = this.requestTimer as unknown as { unref?: () => void };
		if (typeof nodeTimer?.unref === "function") {
			nodeTimer.unref();
		}
	}

	stop() {
		this.stopped = true;
		this.authorityUnsubscribe?.();
		this.authorityUnsubscribe = undefined;
		this.stateUnsubscribe?.();
		this.stateUnsubscribe = undefined;
		if (this.requestTimer !== undefined) {
			clearInterval(this.requestTimer);
			this.requestTimer = undefined;
		}
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

	async verifyConnection(force = false): Promise<void> {
		if (this.transport.verifyConnection) {
			await this.transport.verifyConnection(force);
		} else {
			await this.transport.ping?.();
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
				const state = await this.transport.fetchRoomV2State();
				if (!state) {
					return false;
				}
				this.roomState = state;
				this.roomVersion = Math.max(this.roomVersion, state.version);

				const applied = await readAppliedVersion();
				this.appliedMirror = applied;
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
					const serialized = await this.transport.fetchV2Checkpoint(
						plan.checkpointVersion,
						state.checkpointChunkCount,
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
					await this.afterRemoteApply(true);
					continue;
				}

				// Deltas, in order. Any miss aborts the pass; the next pass retries.
				let progressed = false;
				for (const version of plan.versions) {
					const delta = await this.transport.fetchV2Delta?.(version);
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
					await this.afterRemoteApply(
						changeset.changes.some((c) => c.store === "gameAttributes"),
					);
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

	// After remote state landed: refresh g if attributes moved, tell the UI.
	// Both are mirrors of the database - failure here is cosmetic, never a
	// reason to fail the apply that already committed.
	private async afterRemoteApply(touchedGameAttributes: boolean) {
		try {
			if (touchedGameAttributes) {
				await league.loadGameAttributes();
			}
			await toUI("realtimeUpdate", [
				["gameAttributes", "gameSim", "newPhase", "playerMovement"],
			]);
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
				const ok = this.isAuthority()
					? await this.publishAsVersion(entry)
					: await this.publishAsRequest(entry);
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

	// The authority's path: mint the next version. The compare-and-set is the
	// entire concurrency story - losing it means this device is no longer the
	// chain's writer, and its unpublished local mutation is DISCARDED, loudly,
	// in favor of catching up. One writer means one writer.
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

		// Never author on top of state we don't have: catch up first, publish
		// second. (For a healthy authority this is a no-op read.)
		const state = await this.transport.fetchRoomV2State();
		if (!state) {
			return false;
		}
		const applied = await readAppliedVersion();
		if (applied < state.version) {
			const caught = await this.doCatchUpInline();
			if (!caught) {
				return false;
			}
		}

		const target = (await this.transport.fetchRoomV2State())!.version + 1;
		const serialized = await compressSerialized(
			serializeChangeset(entry.changeset),
		);
		await this.transport.publishV2Delta(
			{
				version: target,
				authorId: this.transport.clientId,
				action: entry.action,
				at: Date.now(),
			},
			serialized,
		);

		const won = await this.transport.commitV2Version(
			{
				version: target,
				authorId: this.transport.clientId,
				byName: this.localName,
				at: Date.now(),
				action: entry.action,
			},
			target - 1,
		);

		if (!won) {
			// Someone else advanced the chain while this device thought it was
			// the writer. The one-writer rule has exactly one honest outcome:
			// this entry is dropped and the device re-syncs to the chain's
			// truth. Silent-merging it is how v1 leagues forked.
			syncDebugLog("v2:cas-lost", { action: entry.action, target });
			console.error(
				`[sync] Discarded a local change from "${entry.action}": another device is in charge of the league's timeline now. Re-syncing to the room's state.`,
			);
			await this.removePending(entry.id);
			await this.takeNotifications(entry.id);
			void this.catchUp();
			return false;
		}

		// The local database ALREADY contains this mutation (it was made here);
		// the marker advances to match in its own transaction. A crash before
		// this lands is healed by the next catch-up re-applying version
		// `target` over identical records - idempotent by construction.
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

	// A follower's path: hand the edit to the authority. Local state already
	// reflects the edit optimistically; the authority's next version carries
	// the same whole records back, converging by construction.
	private async publishAsRequest(
		entry: Omit<ChangesetEntry, "seq">,
	): Promise<boolean> {
		if (!this.transport.publishV2Request) {
			return false;
		}
		const serialized = await compressSerialized(
			serializeChangeset(entry.changeset),
		);
		await this.transport.publishV2Request({
			id: entry.id,
			authorId: this.transport.clientId,
			byName: this.localName,
			action: entry.action,
			data: serialized,
			at: Date.now(),
		});
		await this.removePending(entry.id);
		const notifications = await this.takeNotifications(entry.id);
		for (const notification of notifications ?? []) {
			void this.publishNotification(notification).catch(() => {});
		}
		syncDebugLog("v2:request-published", { action: entry.action });
		return true;
	}

	// Authority: fold waiting follower requests into the chain, oldest first.
	async drainRequests(): Promise<void> {
		if (
			this.stopped ||
			!this.isAuthority() ||
			!this.transport.fetchV2Requests
		) {
			return;
		}
		try {
			const requests = await this.transport.fetchV2Requests();
			for (const request of requests) {
				if (request.authorId === this.transport.clientId) {
					await this.transport.deleteV2Request?.(request.id);
					continue;
				}
				const changeset = deserializeChangeset(
					await decompressSerialized(request.data),
				) as Changeset;
				const ok = await this.publishAsVersion({
					id: makeId(),
					authorId: this.transport.clientId,
					action: request.action,
					changeset,
				});
				if (!ok) {
					return;
				}
				// The published version's records now need to exist HERE too - the
				// request was another device's mutation. Applying our own freshly
				// minted version does exactly that, atomically.
				await this.catchUpSelfAfterRequest(changeset, request.action);
				await this.transport.deleteV2Request?.(request.id);
			}
		} catch (error) {
			syncDebugLog("v2:drain-requests-failed", { error });
		}
	}

	private async catchUpSelfAfterRequest(changeset: Changeset, action: string) {
		// publishAsVersion advanced the marker already (it assumes local data
		// contains the mutation, which holds for OWN actions). For a folded
		// request the records still have to land locally - same records, same
		// idempotent whole-record puts, wrapped in one transaction.
		const version = await readAppliedVersion();
		try {
			await applyRequestRecordsLocally(changeset);
			await this.afterRemoteApply(
				changeset.changes.some((c) => c.store === "gameAttributes"),
			);
			syncDebugLog("v2:request-folded", { action, version });
		} catch (error) {
			syncDebugLog("v2:request-fold-failed", { action, error });
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
			const serialized = await compressSerialized(
				serializeChangeset(payload),
			);
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
