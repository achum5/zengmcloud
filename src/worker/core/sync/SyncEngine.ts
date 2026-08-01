import {
	applyChangeset,
	type Changeset,
	type SyncChange,
} from "./changeset.ts";
import {
	compressSerialized,
	decompressSerialized,
	deserializeChangeset,
	serializeChangeset,
} from "./serialize.ts";
import type { SyncNotification } from "./notifications.ts";
import type {
	Authority,
	ChangesetEntry,
	SyncMember,
	SyncTransport,
} from "./types.ts";
import { outbox } from "./outbox.ts";
import { shouldTraceSyncLabel, syncDebugLog } from "./debugLog.ts";
import type { LeaguePosition } from "./leaguePosition.ts";

// Changesets larger than this are "bulk" (e.g. a simulation, which mutates
// hundreds of records). They're only published by the host, and are split into
// chunks so each fits in one Firestore doc.
const MAX_SYNC_CHANGES = 200;

// Max characters of serialized changeset per Firestore doc. Deliberately
// conservative: Firestore's ~1 MB limit counts UTF-8 BYTES, and a JS string
// character can encode up to 3 bytes (accented/non-Latin names), so capping at
// 300k chars keeps even a worst-case all-multibyte part safely under the doc
// limit. Bulk changesets are split at the STRING level into parts of this
// size, so a single record of any size is always shippable - the old
// per-record chunking gave an oversized record its own over-limit chunk, which
// Firestore rejected forever and which then blocked the FIFO upload queue
// permanently.
const MAX_PART_CHARS = 300_000;

// Split a serialized changeset into parts of at most MAX_PART_CHARS, never
// splitting a surrogate pair.
const splitSerialized = (serialized: string): string[] => {
	const parts: string[] = [];
	let i = 0;
	while (i < serialized.length) {
		let end = Math.min(i + MAX_PART_CHARS, serialized.length);
		const last = serialized.charCodeAt(end - 1);
		if (end < serialized.length && last >= 0xd800 && last <= 0xdbff) {
			end += 1;
		}
		parts.push(serialized.slice(i, end));
		i = end;
	}
	return parts;
};

// Backlog drain paging. Entries can be up to ~700 KB (a bulk-sim chunk), so a
// page of this many is a few MB per fetch - enough to make real progress, small
// enough not to time out / exhaust memory on a phone. MAX_PAGES bounds one
// catchUp() call so it can't spin forever while the head keeps moving; the timer
// resumes on the next tick from the banked watermark.
const CATCH_UP_PAGE_SIZE = 25;
const CATCH_UP_MAX_PAGES = 40;
// Fallback page size when a full page fails to fetch. Entries can be ~700 KB
// bulk-sim chunks, so a full page of them is ~15 MB in one query - which can
// time out on a weak phone connection, and a drain that just retries the same
// heavy page every tick is wedged in all but name. A few entries at a time
// always gets through; the drain is slower but it MOVES.
const CATCH_UP_SMALL_PAGE = 3;

// Only surface the "catching up …%" indicator once the backlog is at least this
// many entries behind - a handful of missed changes catches up near-instantly
// and shouldn't flash a progress bar.
const CATCH_UP_PROGRESS_MIN = 30;

// Uploads have been time-boxed since day one; the DOWNLOAD side never was. A
// fetch that neither resolves nor rejects - a phone that slept mid-request, a
// Firestore query wedged behind a dead socket - leaves catchUp()'s `finally`
// unreached forever, which latches the reentrancy guard. Every later pass then
// returns at the guard, so the catch-up bar sits at 0%, the live subscription
// never starts, and nothing recovers it. Generous, because a full page of
// bulk-sim chunks is genuinely big on a weak connection.
// Now a backstop rather than the primary defence: the transport's catch-up
// reads go straight to the server and reject when they can't, so a wedged
// connection fails in seconds. These only catch a request that stalls
// mid-flight. Kept generous enough that a genuinely large page (a full page of
// bulk-sim chunks can be megabytes) isn't cut off on a slow connection - and
// when it is, the small-page retry below picks it up.
const CATCH_UP_FETCH_TIMEOUT = 45_000;
const CATCH_UP_SMALL_FETCH_TIMEOUT = 15_000;

// Belt and braces for the same failure: if a pass somehow hangs anywhere else,
// a later call assumes it is lost and proceeds. Comfortably longer than a pass
// that is merely slow (page cap x the fetch timeout is the theoretical worst
// case, but a real pass that far behind still logs progress every page).
const CATCH_UP_STALL_TIMEOUT = 3 * CATCH_UP_FETCH_TIMEOUT;
const CATCH_UP_FULL_LOG_TIMEOUT = 120_000;

// How long a "room is advancing" lease lasts. Generous enough to cover a single
// day's sim + upload even on a slow phone; it's re-stamped when a bulk upload
// starts and cleared as soon as the advance is published, so this only actually
// matters as a crash-recovery ceiling (a simmer that dies mid-sim unblocks the
// room after this).
const ROOM_BUSY_LEASE_MS = 45_000;

const READY_TTL = 10_000;
const READY_TIMEOUT = 5_000;

// One outbox-drain publish attempt may not hang longer than this. Firestore's
// setDoc NEVER rejects while offline - it buffers the write and resolves only on
// server ack - so without a timeout an offline drain would hang forever, wedging
// every queued change behind it. On timeout the entry stays queued and the drain
// retries later; if the buffered write eventually lands anyway, the re-publish
// overwrites the same doc id, so nothing duplicates.
const PUBLISH_ATTEMPT_TIMEOUT = 45_000;

// Outbox-drain retry backoff after a failed publish: quick first retry, then
// doubling to a cap, reset by any success. The periodic catch-up timer is a
// second, independent kick, so a queued change never waits on just one timer.
const DRAIN_RETRY_MIN_MS = 2_000;
const DRAIN_RETRY_MAX_MS = 60_000;

// Backoff for re-creating a dead Firestore listener. onSnapshot listeners are
// TERMINAL on error - after the error callback fires they never fire again and
// must be re-created. Without this, one transient stream error (token refresh
// hiccup, backgrounded tab, dropped socket) permanently killed sync until a page
// refresh: ensureReady() threw forever, so nothing ever uploaded again.
const LISTENER_RESTART_MIN_MS = 1_000;
const LISTENER_RESTART_MAX_MS = 30_000;

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

const makeId = (): string => {
	if (typeof crypto !== "undefined" && crypto.randomUUID) {
		return crypto.randomUUID();
	}
	// Fallback for environments without crypto.randomUUID.
	return `${Date.now()}-${Math.floor(Math.random() * 1e9)}`;
};

// Draft actions are turn-based - the game only enables the pick for whoever is
// on the clock - so their changesets may broadcast from ANY device, not just
// the sim authority. (Everything else that mutates in bulk is a sim and stays
// sim authority-only.) Matches an action label like "main.draftUser" or
// "playMenu.untilYourNextPick".
const isDraftAction = (action: string): boolean => {
	const name = action.includes(".")
		? action.slice(action.indexOf(".") + 1)
		: action;
	return (
		name === "draftUser" ||
		name === "onePick" ||
		name === "untilYourNextPick" ||
		name === "untilEnd"
	);
};

// Connects the local change-capture layer to a transport (Firebase or a fake).
// - Local actions → onLocalChangeset() → publish to the shared log.
// - Remote entries → handleEntry() → applyChangeset() into the local cache.
// Small changes go as one entry; bulk changes (sims) are host-only and chunked.
// It ignores its own entries and dedups by entry id, so echoes/replays are safe.
export class SyncEngine {
	private transport: SyncTransport;

	// Who currently holds sim authority (may advance the league). Kept in sync with
	// the shared control doc via subscribeAuthority. Undefined until someone
	// claims it. This device is the authority when authority.holderId === our id.
	private authority: Authority | undefined;

	// If the user chose "sim here" when connecting, claim it on start.
	private claimOnStart: boolean;

	private onAuthorityChange:
		| ((authority: Authority | undefined) => void)
		| undefined;

	private onReadyChange: ((ready: boolean) => void) | undefined;

	private authorityUnsubscribe: (() => void) | undefined;

	// This device's display name, used as the author of push notifications
	// ("Alex completed a trade") and as the sim authority's name. Set when push is
	// enabled.
	localName = "A league-mate";

	private onWatermark: ((seq: number) => void) | undefined;

	// Fired when a bulk batch is abandoned (its chunks aren't in the log and the
	// watermark is about to bank past it) - so the owner can persist a durable
	// "needs resync" marker that survives a reload and self-heals on next connect.
	private onResyncNeeded: (() => void) | undefined;

	private unsubscribe: (() => void) | undefined;

	// Entry ids we've produced or applied - prevents re-applying (and thus
	// re-broadcasting) the same change.
	private seen = new Set<string>();

	// Buffers incoming bulk chunks until a whole batch has arrived. A chunk is
	// either a legacy record-level slice (SyncChange[]) or a new-format string
	// part of the serialized whole changeset. entryIds remembers which log
	// entries fed the batch, so a failed apply can remove them all from `seen`
	// and the next catch-up genuinely re-processes them (instead of dedup
	// silently skipping the data forever).
	private pendingBatches = new Map<
		string,
		{
			count: number;
			chunks: Map<number, SyncChange[] | string>;
			entryIds: string[];
			action: string;
			authorId: string;
			// Highest seq among the chunks received so far - compared against the
			// author's overall progress to prove a batch can never complete.
			maxChunkSeq: number;
			// The EARLIEST chunk seen for this batch - where it actually sits in the
			// log. maxChunkSeq can't answer that once an author re-uploads a missing
			// chunk, because that chunk carries a fresh timestamp. See the
			// out-of-order guard in completeBatch.
			minChunkSeq: number;
		}
	>();

	// Highest entry seq seen per author, across ALL their entries. An author's
	// outbox publishes in strict FIFO order, so once we've seen an entry from
	// them BEYOND a stuck batch's chunks, the batch's missing chunks can never
	// arrive - they would have had to publish first. That's the evidence that
	// lets sweepStaleBatches abandon a dead batch instead of pinning the
	// watermark forever.
	private lastSeqByAuthor = new Map<string, number>();

	// Self-heal for a bulk batch that never completes: batchId → how many chunks
	// it had when a catch-up pass last reached the head of the log (meaning a
	// FULL walk of the backlog didn't complete it), and how many times each has
	// been dropped and rebuilt from a clean re-fetch. See sweepStaleBatches.
	private staleBatchHave = new Map<string, number>();
	private batchResetCounts = new Map<string, number>();

	// Batches that were just reset and are awaiting their re-fetch. A reset
	// un-sees the batch's entries but also empties pendingBatches for it - so
	// without this, an advanceWatermark from ANY source (a live entry landing,
	// the next page) could bank the watermark PAST the un-seen entries before
	// the re-fetch runs, silently skipping the batch forever. Treated exactly
	// like a pending batch by advanceWatermark; cleared when the batch re-forms.
	private rebuildingBatches = new Set<string>();

	// Batches this device gave up on (their chunks were provably not in the log
	// at the time - e.g. the simmer was killed mid-upload). If a chunk for one of
	// these ever arrives LATER (the author came back and drained its outbox), the
	// batch is resurrected and rescued by batchId, so the skipped changeset - a
	// whole simmed day! - still lands instead of being lost until a manual full
	// resync. Bounded so it can't grow forever.
	private abandonedBatches = new Set<string>();
	private static ABANDONED_MEMORY_LIMIT = 50;

	// Batch rescue (fetch every chunk directly by batchId) currently in flight,
	// so concurrent sweeps/chunks don't double-fetch the same batch.
	private rescueInFlight = new Set<string>();

	// Dedup key for the watermark-pinned debug log, so the live-subscription
	// path logs when the blocked state CHANGES rather than on every snapshot.
	private lastPinLogKey: string | undefined;

	// Highest entry seq (server-timestamp) we've seen, and the highest we've
	// persisted as the catch-up watermark.
	private maxSeq: number;

	private persistedSeq: number;

	// Entry ids whose changesets failed to apply. While non-empty, we STOP
	// advancing the persisted watermark, so catch-up re-fetches (and re-applies)
	// from before the failure instead of skipping it forever. Failed ids are also
	// removed from `seen`, so the retry genuinely re-processes them - and a clean
	// re-apply removes them here, unwedging everything automatically. (The old
	// boolean version of this could never self-clear: the failed entry stayed in
	// `seen`, so every "retry" was dedup-skipped and only a manual full resync
	// recovered.)
	private failedApplies = new Set<string>();

	// True only while resyncAll is replaying the whole log, so the out-of-order
	// guard doesn't decline its own replay and recurse. Assigned before it was
	// ever declared, which typechecked as an error and left the flag untyped.
	private resyncing = false;

	// Live entries that arrived while a resync was replaying the log. Applied in
	// seq order once it finishes - they are newer than everything in the replay.
	private deferredDuringResync: ChangesetEntry[] = [];

	private get applyFailed(): boolean {
		return this.failedApplies.size > 0;
	}

	// Guards catchUp() against reentrancy, so the connect-time drain and the
	// periodic timer can't fetch the same pages at once.
	private catchingUp = false;
	// When the guard went up, so a pass that hangs forever can be recognised as
	// lost rather than blocking every future pass. A hung promise never reaches
	// the `finally` that would clear the flag, so nothing else can.
	private catchingUpSince = 0;

	// The room code, used to scope the durable outbox (undefined = don't use the
	// outbox, e.g. the in-memory test transport).
	private code: string | undefined;

	// Reports live upload progress while (re)publishing, so the UI can show a
	// "keep the app open" indicator with a real count. undefined = idle.
	private onUploadProgress:
		| ((progress: { done: number; total: number } | undefined) => void)
		| undefined;

	// Fires once a local change is CONFIRMED uploaded, so the UI can flash a brief
	// "synced ✓" - on any device, for any change (a trade/signing, not just a sim).
	private onUploadComplete: (() => void) | undefined;

	// Reports backlog-drain progress while catching up after an absence (entries
	// applied / total). undefined = caught up / trivial gap.
	private onCatchUpProgress:
		| ((progress: { done: number; total: number } | undefined) => void)
		| undefined;

	// Live drain progress: total entries to apply this drain (undefined = not
	// showing progress), and how many we've applied so far. Persist across the
	// multiple catchUp() calls a big drain spans. The baseline is the seq the
	// total was counted FROM (the fetch frontier at count time): only entries
	// beyond it count toward `done`, so re-fetching an already-seen pinned tail
	// doesn't inflate the bar.
	private catchUpTotal: number | undefined;
	private catchUpDone = 0;
	private catchUpBaseline = 0;

	private readyUntil = 0;

	private readyProbe: Promise<void> | undefined;

	private listenerHealthy = true;

	private authorityListenerHealthy = true;

	// True once stop() has run - blocks listener restarts and drain retries from
	// resurrecting a torn-down engine.
	private stopped = false;

	// Fallback upload queue for engines without a room code (the in-memory test
	// transport): same FIFO drain semantics, just not durable.
	private memoryQueue: Omit<ChangesetEntry, "seq">[] = [];

	// Single-flight drain: all drainOutbox() calls chain onto this so two drains
	// can never interleave (which would reorder publishes).
	private drainChain: Promise<boolean> = Promise.resolve(true);

	private drainBackoffMs = 0;

	private drainRetryTimer: ReturnType<typeof setTimeout> | undefined;

	// Notifies the owner (connect.ts) whenever the number of queued-but-unconfirmed
	// uploads changes, so the UI can show "N queued" instead of leaving the user
	// in the dark about deltas that haven't reached the cloud yet.
	private onPendingChange: ((count: number) => void) | undefined;

	private listenerRestartBackoffMs = 0;

	private listenerRestartTimer: ReturnType<typeof setTimeout> | undefined;

	constructor(
		transport: SyncTransport,
		options: {
			isHost?: boolean;
			initialWatermark?: number;
			onWatermark?: (seq: number) => void;
			onAuthorityChange?: (authority: Authority | undefined) => void;
			code?: string;
			onUploadProgress?: (
				progress: { done: number; total: number } | undefined,
			) => void;
			onUploadComplete?: () => void;
			onCatchUpProgress?: (
				progress: { done: number; total: number } | undefined,
			) => void;
			onReadyChange?: (ready: boolean) => void;
			onPendingChange?: (count: number) => void;
			onResyncNeeded?: () => void;
		} = {},
	) {
		this.transport = transport;
		this.claimOnStart = options.isHost ?? false;
		this.onWatermark = options.onWatermark;
		this.onResyncNeeded = options.onResyncNeeded;
		this.onAuthorityChange = options.onAuthorityChange;
		this.maxSeq = options.initialWatermark ?? 0;
		this.persistedSeq = options.initialWatermark ?? 0;
		this.code = options.code;
		this.onUploadProgress = options.onUploadProgress;
		this.onUploadComplete = options.onUploadComplete;
		this.onCatchUpProgress = options.onCatchUpProgress;
		this.onReadyChange = options.onReadyChange;
		this.onPendingChange = options.onPendingChange;
	}

	get clientId(): string {
		return this.transport.clientId;
	}

	// Is THIS device currently in charge of simming (i.e. may it advance the league)?
	isAuthority(): boolean {
		return (
			this.authority !== undefined &&
			this.authority.holderId === this.transport.clientId
		);
	}

	getAuthority(): Authority | undefined {
		return this.authority;
	}

	// Mark the room "actively advancing" for a lease window, so followers hold off
	// on conflict-prone edits while a sim/phase/draft is running or still
	// uploading (before it shows up in the change log). Only the sim authority may
	// write this. Fire-and-forget - it must never add latency to a sim.
	markRoomBusy(position?: LeaguePosition): void {
		if (!this.isAuthority()) {
			return;
		}
		void this.transport.publishBusy?.(
			Date.now() + ROOM_BUSY_LEASE_MS,
			position,
		);
	}

	// Release the "actively advancing" lease once the advance has been published
	// (its seq is now visible to followers, so the caught-up check takes over).
	// `position` is where the league now sits, stamped alongside the lease
	// release. That is the moment the advance has been published, so it is the
	// honest answer to "how far is this room" - and the one thing a follower can
	// check that doesn't come from its own change log. See leaguePosition.ts.
	clearRoomBusy(position?: LeaguePosition): void {
		if (!this.isAuthority()) {
			return;
		}
		void this.transport.publishBusy?.(0, position);
	}

	// Is SOMEONE ELSE mid-advance right now? True only for followers - the
	// sim authority is the one doing the advancing, so it never blocks itself.
	isRoomBusy(): boolean {
		if (this.isAuthority()) {
			return false;
		}
		const busyUntil = this.authority?.busyUntil;
		return busyUntil !== undefined && Date.now() < busyUntil;
	}

	// Has this device applied everything it has seen from the log? False while a
	// bulk sim is mid-transfer, an apply failed, or newer entries haven't been
	// applied yet - i.e. while acting now would be acting on a stale world.
	isCaughtUp(): boolean {
		return (
			this.pendingBatches.size === 0 &&
			!this.applyFailed &&
			this.persistedSeq >= this.maxSeq
		);
	}

	private markNotReady() {
		const wasReady = this.isReady();
		this.readyUntil = 0;
		if (wasReady) {
			this.onReadyChange?.(false);
		}
	}

	isReady(): boolean {
		return (
			this.listenerHealthy &&
			this.authorityListenerHealthy &&
			Date.now() < this.readyUntil
		);
	}

	private async withReadyTimeout(promise: Promise<void>) {
		let timeoutID: ReturnType<typeof setTimeout> | undefined;
		try {
			await Promise.race([
				promise,
				new Promise<void>((_resolve, reject) => {
					timeoutID = setTimeout(() => {
						reject(new Error("Cloud sync readiness check timed out."));
					}, READY_TIMEOUT);
				}),
			]);
		} finally {
			if (timeoutID !== undefined) {
				clearTimeout(timeoutID);
			}
		}
	}

	async ensureReady(force = false): Promise<void> {
		if (!this.listenerHealthy || !this.authorityListenerHealthy) {
			// A dead listener is recoverable - re-create it right now rather than
			// failing until the (backoff) restart timer gets around to it. This used
			// to throw unconditionally with no recovery path, which permanently
			// wedged the session after one listener error until a page refresh.
			this.restartUnhealthyListeners();
			if (!this.listenerHealthy || !this.authorityListenerHealthy) {
				this.markNotReady();
				throw new Error("Cloud sync listeners are not ready.");
			}
		}
		if (!force && Date.now() < this.readyUntil) {
			return;
		}
		if (this.readyProbe) {
			return this.readyProbe;
		}

		this.readyProbe = (async () => {
			try {
				if (this.transport.ping) {
					await this.withReadyTimeout(this.transport.ping());
				}
				const wasReady = this.isReady();
				this.readyUntil = Date.now() + READY_TTL;
				if (!wasReady && this.isReady()) {
					this.onReadyChange?.(true);
				}
			} catch (error) {
				this.markNotReady();
				throw error;
			}
		})().finally(() => {
			this.readyProbe = undefined;
		});

		return this.readyProbe;
	}

	// Back-compat alias: "host" now means "current sim authority". Used by the
	// notification builder to decide who narrates a sim.
	getIsHost(): boolean {
		return this.isAuthority();
	}

	// Claim sim authority for this device. Optimistically flips local state so
	// advancing unlocks immediately; the shared-doc subscription then confirms
	// (and would correct us if someone claimed at the same instant).
	async claimAuthority(): Promise<void> {
		const holder: Authority = {
			holderId: this.transport.clientId,
			holderName: this.localName,
		};
		await this.ensureReady();
		try {
			await this.transport.claimAuthority?.(holder.holderId, holder.holderName);
		} catch (error) {
			this.markNotReady();
			throw error;
		}
		this.authority = holder;
		this.onAuthorityChange?.(holder);
	}

	// Register this device for push in the room (records its FCM token). No-op if
	// the transport doesn't support push (e.g. the in-memory test transport).
	async registerMember(member: SyncMember) {
		this.localName = member.name || this.localName;
		await this.transport.registerMember?.(this.transport.clientId, member);
	}

	// Enqueue a push notification for the other devices in the room.
	async publishNotification(notification: SyncNotification) {
		await this.transport.publishNotification?.({
			...notification,
			authorId: this.transport.clientId,
			authorName: this.localName,
		});
	}

	start() {
		// Watch who is in charge of simming, so every device agrees on who may advance.
		// This is tiny (one doc) and needed immediately, so it always starts now -
		// unlike the changes subscription, which waits until the backlog is drained
		// (see startChangesSubscription) so its initial snapshot isn't the whole log.
		this.startAuthoritySubscription();

		// If the user chose to sim here on connect, claim it now.
		if (this.claimOnStart) {
			void this.claimAuthority().catch(() => undefined);
		}
	}

	private startAuthoritySubscription() {
		if (this.authorityUnsubscribe !== undefined || this.stopped) {
			return;
		}
		this.authorityUnsubscribe = this.transport.subscribeAuthority?.(
			(authority) => {
				this.authority = authority;
				this.authorityListenerHealthy = true;
				this.listenerRestartBackoffMs = 0;
				this.onAuthorityChange?.(authority);
			},
			(error) => {
				// A Firestore listener is TERMINAL once its error callback fires - it
				// never fires again and must be re-created. Tear it down and schedule
				// a restart; nothing here is allowed to be permanent.
				const wasReady = this.isReady();
				this.authorityListenerHealthy = false;
				this.authority = undefined;
				this.readyUntil = 0;
				this.authorityUnsubscribe?.();
				this.authorityUnsubscribe = undefined;
				this.onAuthorityChange?.(undefined);
				if (wasReady) {
					this.onReadyChange?.(false);
				}
				syncDebugLog("engine:authority-listener-died", { error });
				this.scheduleListenerRestart();
			},
		);
		if (this.authorityUnsubscribe !== undefined) {
			this.authorityListenerHealthy = true;
		}
	}

	// Start the live changes subscription. Deferred until the caller has drained
	// the backlog via catchUp() and moved the transport watermark to the head, so
	// the real-time listener's initial snapshot is just the live tail rather than
	// a re-load of everything we just caught up on.
	startChangesSubscription() {
		if (this.unsubscribe || this.stopped) {
			return;
		}
		this.unsubscribe = this.transport.subscribe({
			onEntry: (entry) => {
				// Deliveries from the LIVE changes listener specifically (unlike the
				// transport's global lastContactAt, which ANY listener refreshes - the
				// authority doc heartbeats during a sim, so it proves nothing about
				// whether GAME DATA is still arriving). The catch-up poll gates on this.
				this.lastChangesDeliveryAt = Date.now();
				return this.handleEntry(entry);
			},
			onError: (error) => {
				// Terminal listener death (see startAuthoritySubscription). Re-create
				// it from the durable watermark; until then the periodic catch-up
				// keeps changes flowing, and queued uploads keep draining - a dead
				// download listener must never block uploads.
				const wasReady = this.isReady();
				this.listenerHealthy = false;
				this.readyUntil = 0;
				this.unsubscribe?.();
				this.unsubscribe = undefined;
				if (wasReady) {
					this.onReadyChange?.(false);
				}
				syncDebugLog("engine:changes-listener-died", { error });
				this.scheduleListenerRestart();
			},
			onBatchProcessed: () => this.advanceWatermark(),
		});
		this.listenerHealthy = true;
	}

	// Re-create whichever listeners have died. Safe to call anytime: healthy
	// listeners are left alone (their unsubscribe handles are still set).
	private restartUnhealthyListeners() {
		if (this.stopped) {
			return;
		}
		if (this.authorityUnsubscribe === undefined) {
			this.startAuthoritySubscription();
		}
		if (this.unsubscribe === undefined && !this.listenerHealthy) {
			// Restart the live tail from what we've durably applied, so the new
			// listener's initial snapshot is bounded and nothing is skipped.
			this.transport.updateSince?.(this.persistedSeq);
			this.startChangesSubscription();
		}
	}

	private scheduleListenerRestart() {
		if (this.stopped || this.listenerRestartTimer !== undefined) {
			return;
		}
		this.listenerRestartBackoffMs = Math.min(
			Math.max(this.listenerRestartBackoffMs * 2, LISTENER_RESTART_MIN_MS),
			LISTENER_RESTART_MAX_MS,
		);
		this.listenerRestartTimer = setTimeout(() => {
			this.listenerRestartTimer = undefined;
			syncDebugLog("engine:listener-restart-attempt", {
				backoffMs: this.listenerRestartBackoffMs,
			});
			this.restartUnhealthyListeners();
		}, this.listenerRestartBackoffMs);
	}

	// Has the live changes subscription been started yet?
	hasChangesSubscription(): boolean {
		return this.unsubscribe !== undefined;
	}

	// Persist the watermark only when there are no half-received bulk batches, so
	// a reconnect never skips past chunks it hasn't fully applied. (Whole-record
	// applies are idempotent, so re-fetching a bit on reconnect is harmless.)
	private advanceWatermark() {
		// Don't skip past a half-received bulk batch (including one that was just
		// reset and is being re-fetched), or past a changeset that failed to
		// apply - either would silently and permanently drop data.
		if (
			this.pendingBatches.size > 0 ||
			this.rebuildingBatches.size > 0 ||
			this.applyFailed
		) {
			if (this.maxSeq > this.persistedSeq) {
				const pinKey = `${this.pendingBatches.size}|${this.failedApplies.size}`;
				if (pinKey !== this.lastPinLogKey) {
					this.lastPinLogKey = pinKey;
					syncDebugLog("engine:watermark-pinned", {
						persistedSeq: this.persistedSeq,
						maxSeq: this.maxSeq,
						pendingBatches: this.describePendingBatches(),
						failedApplies: this.failedApplies.size,
					});
				}
			}
			return;
		}
		this.lastPinLogKey = undefined;
		if (this.maxSeq > this.persistedSeq) {
			this.persistedSeq = this.maxSeq;
			this.onWatermark?.(this.maxSeq);
		}
	}

	// Snapshot of every half-received bulk batch, for the catch-up diagnostics:
	// which chunk indexes are still missing tells us whether the rest of a batch
	// simply hasn't arrived yet or genuinely isn't in the log.
	private describePendingBatches() {
		return [...this.pendingBatches.entries()].map(([batchId, batch]) => {
			const missing: number[] = [];
			for (let i = 0; i < batch.count && missing.length < 20; i++) {
				if (!batch.chunks.has(i)) {
					missing.push(i);
				}
			}
			return {
				batchId,
				action: batch.action,
				have: batch.chunks.size,
				need: batch.count,
				missing,
			};
		});
	}

	stop() {
		this.stopped = true;
		if (this.drainRetryTimer !== undefined) {
			clearTimeout(this.drainRetryTimer);
			this.drainRetryTimer = undefined;
		}
		if (this.listenerRestartTimer !== undefined) {
			clearTimeout(this.listenerRestartTimer);
			this.listenerRestartTimer = undefined;
		}
		this.unsubscribe?.();
		this.unsubscribe = undefined;
		this.authorityUnsubscribe?.();
		this.authorityUnsubscribe = undefined;
		this.listenerHealthy = false;
		this.authorityListenerHealthy = false;
		this.markNotReady();
	}

	// Hand a locally-produced changeset to the sync layer. This is the guarantee
	// the whole multiplayer model rests on: once a local mutation exists, its
	// delta MUST reach the shared log eventually, no matter what the connection
	// does. So the handoff is durable-FIRST:
	//
	//   1. Build the entry (or chunked entries) immediately.
	//   2. Persist ALL of them to the IndexedDB outbox BEFORE any network attempt
	//      or readiness check. Once this step completes, the delta survives
	//      anything - a dead connection, a killed tab, a page refresh.
	//   3. Drain the outbox (strict FIFO, retried forever with backoff). The
	//      drain is the ONLY path to the cloud, so ordering can never invert.
	//
	// Previously the readiness check ran FIRST, and a failure handed the changes
	// back to the in-memory change tracker for retry - so a wedged connection
	// followed by a page refresh silently lost the delta and forked the room
	// forever. Now there is no network-dependent step before durability.
	//
	// Returns "confirmed" when everything queued (including this change) reached
	// the cloud during this call, or "queued" when it is safely persisted and
	// will upload automatically. Throws ONLY if the delta could not be made
	// durable (step 2 failed) - the caller must then retain the changes itself.
	async onLocalChangeset(
		changeset: Changeset,
		action: string,
	): Promise<"confirmed" | "queued"> {
		if (changeset.changes.length === 0) {
			return "confirmed";
		}
		const trace = shouldTraceSyncLabel(action);
		if (trace) {
			syncDebugLog("engine:onLocalChangeset-start", {
				action,
				records: changeset.changes.length,
				isAuthority: this.isAuthority(),
				authorityHolderId: this.authority?.holderId,
				clientId: this.transport.clientId,
				isCaughtUp: this.isCaughtUp(),
				isReady: this.isReady(),
			});
		}

		// Send as a single doc only if it fits in one Firestore doc by BOTH record
		// count AND serialized size. A changeset can be <= MAX_SYNC_CHANGES records
		// yet still blow past Firestore's ~1 MB/doc limit - e.g. advancing to free
		// agency turns over many large player records at once. When it doesn't
		// fit, split the SERIALIZED changeset into string parts - which handles a
		// single record of any size.
		const serialized = serializeChangeset(changeset);
		const fitsInOneDoc =
			changeset.changes.length <= MAX_SYNC_CHANGES &&
			serialized.length <= MAX_PART_CHARS;

		let entries: Omit<ChangesetEntry, "seq">[];
		if (fitsInOneDoc) {
			entries = [
				{
					id: makeId(),
					authorId: this.transport.clientId,
					action,
					changeset,
				},
			];
		} else {
			// Bulk change (e.g. a sim, a phase advance, or a big draft advance).
			// The worker action guard is responsible for preventing a follower from
			// starting these. Once a local bulk mutation exists it gets queued and
			// published regardless of what the authority listener currently claims -
			// a transient listener blip must never turn a sim into a local-only fork.
			if (!this.isAuthority() && !isDraftAction(action)) {
				console.warn(
					`[sync] Publishing bulk change from "${action}" (${changeset.changes.length} records) even though local sim authority is not currently confirmed.`,
				);
				syncDebugLog("engine:bulk-authority-not-confirmed", {
					action,
					records: changeset.changes.length,
					authorityHolderId: this.authority?.holderId,
					clientId: this.transport.clientId,
				});
			}

			// Re-stamp the busy lease now that the (possibly long) upload is
			// starting, so it can't expire mid-transfer and let a follower slip an
			// edit into the tail of the upload window.
			this.markRoomBusy();

			entries = await this.buildPartEntries(serialized, changeset, action);
		}

		// Durability point. After this the delta can no longer be lost, only
		// delayed. All entries are queued in ONE transaction (all-or-nothing): a
		// partial enqueue of a chunked batch would publish an incompletable batch
		// that pins every follower's watermark forever. `seen` keeps our own echo
		// from being re-applied.
		await this.enqueueAll(entries);
		for (const entry of entries) {
			this.seen.add(entry.id);
		}
		void this.reportPending();
		if (trace) {
			syncDebugLog("engine:enqueued", {
				action,
				entries: entries.length,
				records: changeset.changes.length,
				durable: this.code !== undefined,
			});
		}

		const drainedAll = await this.drainOutbox();
		return drainedAll ? "confirmed" : "queued";
	}

	// Split an already-serialized changeset into part entries (one Firestore doc
	// each) sharing a fresh batchId. `records`/`attrs` ride along for the
	// activity page, since a lone part isn't independently parseable.
	private async buildPartEntries(
		serialized: string,
		changeset: Changeset,
		action: string,
	): Promise<Omit<ChangesetEntry, "seq">[]> {
		// Compress before splitting: a sim day's ~6 MB of JSON becomes a few
		// hundred KB, so this cuts BOTH the bytes uploaded and the number of chunk
		// docs a behind device has to page through (the thing that actually makes
		// catch-up slow). Falls back to the plain string if unsupported/failed.
		const payload = await compressSerialized(serialized);
		if (payload !== serialized) {
			syncDebugLog("engine:payload-compressed", {
				action,
				rawChars: serialized.length,
				sentChars: payload.length,
				ratio: Math.round((serialized.length / payload.length) * 10) / 10,
			});
		}
		const parts = splitSerialized(payload);
		const batchId = makeId();
		const attrs = changeset.changes
			.filter((c) => c.store === "gameAttributes")
			.map((c) => String(c.id));
		return parts.map((part, i) => ({
			id: makeId(),
			authorId: this.transport.clientId,
			action,
			batchId,
			chunkIndex: i,
			chunkCount: parts.length,
			changeset: { changes: [] },
			payloadPart: part,
			records: changeset.changes.length,
			attrs,
		}));
	}

	// Add a batch of entries to the upload queue atomically (all-or-nothing, so
	// a failure partway can never strand a partial chunk batch): the durable
	// outbox when we have a room code, or the in-memory fallback (the test
	// transport) otherwise.
	private async enqueueAll(entries: Omit<ChangesetEntry, "seq">[]) {
		if (this.code !== undefined) {
			await outbox.addAll(this.code, entries);
		} else {
			this.memoryQueue.push(...entries);
		}
	}

	private async pendingEntries(): Promise<Omit<ChangesetEntry, "seq">[]> {
		if (this.code === undefined) {
			return [...this.memoryQueue];
		}
		return outbox.pending(this.code);
	}

	private async removePending(id: string) {
		if (this.code === undefined) {
			this.memoryQueue = this.memoryQueue.filter((e) => e.id !== id);
			return;
		}
		await outbox.remove(id);
	}

	// How many queued-but-unconfirmed uploads exist right now.
	async pendingUploadCount(): Promise<number> {
		if (this.code === undefined) {
			return this.memoryQueue.length;
		}
		return outbox.count(this.code);
	}

	private async reportPending() {
		if (!this.onPendingChange) {
			return;
		}
		try {
			this.onPendingChange(await this.pendingUploadCount());
		} catch {
			// Counting is display-only; never let it interfere with the drain.
		}
	}

	// Push everything in the outbox to the cloud, strictly oldest-first,
	// single-flight (concurrent calls chain, they never interleave - interleaving
	// could publish an older record value AFTER a newer one and roll the room
	// back under last-write-wins). Returns true when the queue fully drained
	// (every queued delta confirmed in the shared log); false when a publish
	// failed or timed out - everything unconfirmed is still safely queued, a
	// retry is scheduled with backoff, and the periodic catch-up timer provides a
	// second independent kick.
	//
	// Coalesced: while one run is already queued and not yet started, further
	// calls just share it - each run reads the whole outbox anyway, so chaining
	// a run per caller only produced back-to-back retry churn against a failing
	// entry (every timer/caller stacked another full attempt).
	private drainQueued = false;

	drainOutbox(): Promise<boolean> {
		if (this.drainQueued) {
			return this.drainChain;
		}
		this.drainQueued = true;
		const run = this.drainChain.then(() => {
			this.drainQueued = false;
			return this.doDrain();
		});
		this.drainChain = run.catch(() => false);
		return run;
	}

	// IndexedDB ops inside the drain are timed: a hung outbox read/delete (seen
	// under mobile storage pressure) must fail-and-retry, not silently freeze
	// the whole upload queue forever (the drain is single-flight, so one hung
	// run blocks every future run behind it).
	private static OUTBOX_OP_TIMEOUT = 15_000;

	private async doDrain(): Promise<boolean> {
		if (this.stopped) {
			return false;
		}
		let pending: Omit<ChangesetEntry, "seq">[];
		try {
			pending = await withTimeout(
				this.pendingEntries(),
				SyncEngine.OUTBOX_OP_TIMEOUT,
			);
		} catch (error) {
			console.error(
				"[sync] outbox drain: could not read pending uploads",
				error,
			);
			this.scheduleDrainRetry();
			return false;
		}
		if (pending.length === 0) {
			return true;
		}

		syncDebugLog("engine:drain-start", { pending: pending.length });
		let done = 0;
		let total = pending.length;
		this.onUploadProgress?.({ done, total });
		try {
			while (pending.length > 0) {
				const entry = pending.shift()!;
				// A stranded entry from a previous session isn't in `seen` yet; add it
				// so our own echo isn't re-applied.
				this.seen.add(entry.id);

				// Self-heal a legacy-format entry too big for one Firestore doc. The
				// old per-record chunking let an oversized record produce an
				// unshippable chunk; Firestore rejected it forever and, being at the
				// head of this FIFO queue, it blocked every upload behind it. Replace
				// it with string parts (and, if it was one chunk of a batch, an empty
				// stand-in so receivers can complete the batch's other chunks).
				if (entry.payloadPart === undefined) {
					const legacySerialized = serializeChangeset(entry.changeset);
					if (legacySerialized.length > MAX_PART_CHARS) {
						const parts = await this.migrateOversizedEntry(
							entry,
							legacySerialized,
						);
						if (!parts) {
							this.markNotReady();
							this.scheduleDrainRetry();
							return false;
						}
						// The parts take the entry's place at the head of the queue, so
						// nothing behind it can overtake its content.
						pending.unshift(...parts);
						total = done + pending.length;
						this.onUploadProgress?.({ done, total });
						continue;
					}
				}

				try {
					// Firestore's setDoc never rejects while offline (it buffers), so
					// every attempt is timed. On timeout the entry stays queued; if the
					// buffered write lands later anyway, re-publishing overwrites the
					// same doc id - no duplicate.
					await withTimeout(
						this.transport.publish(entry),
						PUBLISH_ATTEMPT_TIMEOUT,
					);
				} catch (error) {
					this.markNotReady();
					this.scheduleDrainRetry();
					syncDebugLog("engine:drain-publish-failed", {
						entryId: entry.id,
						action: entry.action,
						stillQueued: pending.length + 1,
						error,
					});
					// Also to the plain console, so a stuck queue is diagnosable even
					// with debug logging off.
					console.error(
						`[sync] upload failed (${pending.length + 1} still queued)`,
						error,
					);
					return false;
				}
				await withTimeout(
					this.removePending(entry.id),
					SyncEngine.OUTBOX_OP_TIMEOUT,
				).catch(() => {
					// The publish landed; a failed/hung removal just means this entry
					// re-publishes later (same doc id - idempotent overwrite).
				});
				done += 1;
				this.onUploadProgress?.({ done, total });
				if (shouldTraceSyncLabel(entry.action)) {
					syncDebugLog("engine:drain-publish-confirmed", {
						entryId: entry.id,
						action: entry.action,
						batchId: entry.batchId,
						chunkIndex: entry.chunkIndex,
					});
				}
				// Entries enqueued while we were draining go out in this same pass,
				// still in enqueue order.
				if (pending.length === 0) {
					pending = await withTimeout(
						this.pendingEntries(),
						SyncEngine.OUTBOX_OP_TIMEOUT,
					).catch(() => [] as Omit<ChangesetEntry, "seq">[]);
					total = done + pending.length;
				}
			}
			this.drainBackoffMs = 0;
			this.onUploadComplete?.();
			syncDebugLog("engine:drain-complete", { published: done });
			return true;
		} finally {
			this.onUploadProgress?.(undefined);
			void this.reportPending();
		}
	}

	// Replace a legacy-format entry too big for one Firestore doc. If it was a
	// chunk of a batch (its siblings may already be in the log, with receivers
	// waiting on this index), first publish an EMPTY chunk under the same id and
	// chunk coordinates so the batch completes on receivers; then re-ship this
	// entry's actual content as a fresh string-part batch. Whole-record applies
	// are idempotent, so the two batches together land exactly the original
	// data. Returns the part entries to publish next, or undefined if the
	// stand-in publish failed (treated as a normal transient drain failure).
	private async migrateOversizedEntry(
		entry: Omit<ChangesetEntry, "seq">,
		serialized: string,
	): Promise<Omit<ChangesetEntry, "seq">[] | undefined> {
		syncDebugLog("engine:migrating-oversized-entry", {
			entryId: entry.id,
			action: entry.action,
			chars: serialized.length,
			isChunk: entry.batchId !== undefined,
		});
		if (entry.batchId !== undefined) {
			const replacement = { ...entry, changeset: { changes: [] } };
			try {
				await withTimeout(
					this.transport.publish(replacement),
					PUBLISH_ATTEMPT_TIMEOUT,
				);
			} catch (error) {
				syncDebugLog("engine:migrate-stand-in-failed", {
					entryId: entry.id,
					error,
				});
				return undefined;
			}
		}
		const parts = await this.buildPartEntries(
			serialized,
			entry.changeset,
			entry.action,
		);
		await this.enqueueAll(parts);
		for (const part of parts) {
			this.seen.add(part.id);
		}
		await this.removePending(entry.id);
		void this.reportPending();
		return parts;
	}

	private scheduleDrainRetry() {
		if (this.stopped || this.drainRetryTimer !== undefined) {
			return;
		}
		this.drainBackoffMs = Math.min(
			Math.max(this.drainBackoffMs * 2, DRAIN_RETRY_MIN_MS),
			DRAIN_RETRY_MAX_MS,
		);
		this.drainRetryTimer = setTimeout(() => {
			this.drainRetryTimer = undefined;
			void this.drainOutbox();
		}, this.drainBackoffMs);
	}

	// Back-compat name: the connect-time "finish what a previous session left
	// stranded" call. The drain already handles that case (stranded entries are
	// simply the oldest queued rows), so this just kicks it.
	async flushOutbox() {
		await this.drainOutbox();
	}

	// Apply an entry from the shared log. Returns whether it was applied (false
	// if it was our own, a duplicate, or a not-yet-complete bulk chunk). Never
	// throws.
	async handleEntry(entry: ChangesetEntry): Promise<boolean> {
		// A resync is replaying the WHOLE log in order. The live listener is still
		// running underneath it, and anything it delivers now is NEWER than
		// everything in that replay - applying it mid-replay would let an old
		// entry land on top of it. Hold it and deliver it after, which is where it
		// belongs. (The replay itself doesn't come through here.)
		if (this.resyncing) {
			this.deferredDuringResync.push(entry);
			return false;
		}

		// Track ordering position for the watermark even for entries we skip
		// (our own / already-seen) - they're still "caught up" past.
		if (entry.seq > this.maxSeq) {
			this.maxSeq = entry.seq;
		}
		// Track each author's overall progress (even for skipped entries), as the
		// evidence sweepStaleBatches uses to abandon a batch that can never
		// complete.
		if (entry.seq > (this.lastSeqByAuthor.get(entry.authorId) ?? 0)) {
			this.lastSeqByAuthor.set(entry.authorId, entry.seq);
		}

		if (entry.authorId === this.transport.clientId) {
			return false;
		}
		if (this.seen.has(entry.id)) {
			return false;
		}
		this.seen.add(entry.id);

		if (
			entry.batchId !== undefined &&
			entry.chunkIndex !== undefined &&
			entry.chunkCount !== undefined
		) {
			return this.handleChunk(entry);
		}

		return this.apply(entry.changeset, [entry.id]);
	}

	private async handleChunk(entry: ChangesetEntry): Promise<boolean> {
		const batchId = entry.batchId!;

		// A chunk for a batch this device previously ABANDONED: the author came
		// back and is uploading the missing pieces. The batch's earlier chunks
		// are below our watermark now, so ordered fetches can't rebuild it -
		// resurrect it via the by-batchId rescue instead. (The rescue ingests
		// this chunk too, straight from the log.)
		if (this.abandonedBatches.has(batchId)) {
			syncDebugLog("engine:batch-resurrected", {
				batchId,
				chunkIndex: entry.chunkIndex,
			});
			if (await this.rescueBatch(batchId)) {
				this.abandonedBatches.delete(batchId);
				return true;
			}
			// The rescue couldn't run (no by-id fetch, or one already in flight) or
			// came up short. Do NOT return here: this chunk is already marked `seen`,
			// so returning would drop it with nothing buffered to pin the watermark -
			// a resurrected batch silently going missing a second time, which is
			// exactly the shape of failure this whole path exists to undo. Fall
			// through and buffer it like any other chunk, and keep the abandoned
			// marker so the next chunk retries the rescue.
		}

		let batch = this.pendingBatches.get(batchId);
		if (!batch) {
			batch = {
				count: entry.chunkCount!,
				chunks: new Map(),
				entryIds: [],
				action: entry.action,
				authorId: entry.authorId,
				maxChunkSeq: 0,
				minChunkSeq: Number.POSITIVE_INFINITY,
			};
			this.pendingBatches.set(batchId, batch);
			// If this batch was just reset, its re-fetch has begun - pendingBatches
			// pins the watermark from here on.
			this.rebuildingBatches.delete(batchId);
			syncDebugLog("engine:batch-start", {
				batchId,
				action: entry.action,
				need: batch.count,
				seq: entry.seq,
			});
		}
		if (entry.seq > batch.maxChunkSeq) {
			batch.maxChunkSeq = entry.seq;
		}
		if (entry.seq < batch.minChunkSeq) {
			batch.minChunkSeq = entry.seq;
		}
		batch.chunks.set(
			entry.chunkIndex!,
			entry.payloadPart ?? entry.changeset.changes,
		);
		if (!batch.entryIds.includes(entry.id)) {
			batch.entryIds.push(entry.id);
		}

		if (batch.chunks.size < batch.count) {
			// Still waiting for the rest of the batch.
			return false;
		}

		return this.completeBatch(batchId, batch);
	}

	// A bulk batch has every chunk: reassemble and apply it. Shared by the
	// normal in-order path (handleChunk) and the by-batchId rescue path.
	private async completeBatch(
		batchId: string,
		batch: NonNullable<ReturnType<SyncEngine["pendingBatches"]["get"]>>,
	): Promise<boolean> {
		this.pendingBatches.delete(batchId);
		this.staleBatchHave.delete(batchId);
		this.batchResetCounts.delete(batchId);
		syncDebugLog("engine:batch-complete", {
			batchId,
			action: batch.action,
			chunks: batch.count,
		});

		// Reassemble. New-format batches are string parts of ONE serialized
		// changeset (joined then parsed - so a single record of any size works);
		// legacy batches are per-chunk change arrays (concatenated). A parse
		// failure routes through the same failed-apply path as an apply error, so
		// the watermark stays pinned and the batch retries on a later catch-up.
		let changes: SyncChange[];
		try {
			if (batch.chunks.size > 0 && typeof batch.chunks.get(0) === "string") {
				let joined = "";
				for (let i = 0; i < batch.count; i++) {
					joined += (batch.chunks.get(i) as string | undefined) ?? "";
				}
				// Compressed (GZ1:) or plain JSON - the payload says which, so a room
				// whose devices are on different versions keeps working either way.
				const serialized = await decompressSerialized(joined);
				changes = (deserializeChangeset(serialized) as Changeset).changes;
			} else {
				changes = [];
				for (let i = 0; i < batch.count; i++) {
					const chunk = batch.chunks.get(i);
					if (chunk && typeof chunk !== "string") {
						changes.push(...chunk);
					}
				}
			}
		} catch (error) {
			console.error("Failed to reassemble synced batch", error);
			for (const id of batch.entryIds) {
				this.failedApplies.add(id);
				this.seen.delete(id);
			}
			return false;
		}

		// OUT-OF-ORDER GUARD. A batch whose newest chunk predates our durable
		// watermark is history: we have already applied entries that came AFTER
		// it. Applying it in place would be a last-write-wins violation - it
		// stomps whatever those newer entries set.
		//
		// This is not hypothetical. It is how a device sitting in RESIGN_PLAYERS
		// snapped back to AFTER_DRAFT the moment it took an action: a bulk batch
		// from around the draft had been abandoned with chunks missing, the room
		// moved on a phase, the author later uploaded the rest, and the rescue
		// path (which bypasses the watermark BY DESIGN, so it can reach chunks an
		// ordered fetch no longer can) replayed that old changeset - gameAttributes
		// and all - straight over the newer phase.
		//
		// Judged on the batch's EARLIEST chunk, which is where it actually sits in
		// the log. Using the newest defeated the whole guard against the one case
		// it was written for: the missing chunk the author finally uploads carries
		// a CURRENT timestamp, so the batch's newest chunk sits above the watermark
		// and a draft-era changeset reads as brand new. That is why this kept
		// coming back every offseason - the offseason is several big chunked
		// advances in a row, so it has the most chances to drop one.
		//
		// A batch still legitimately in flight can never look old this way: while
		// it is pending, advanceWatermark pins the watermark at or below it, so
		// minChunkSeq < persistedSeq only holds for a batch the watermark has
		// already moved past - which is to say, an abandoned one.
		//
		// The batch is NOT dropped. Every one of its chunks is in the durable log,
		// so a full ordered resync replays them in sequence along with everything
		// after them - the missed data still lands, it just can no longer land on
		// top of newer state. Done inline (not scheduled) so the caller's "did it
		// apply" answer stays truthful.
		//
		// The resync itself is exempt: it replays the WHOLE log, where every old
		// batch legitimately sits below the watermark. Without that exemption it
		// would decline its own replay and recurse.
		if (
			!this.resyncing &&
			Number.isFinite(batch.minChunkSeq) &&
			batch.minChunkSeq < this.persistedSeq
		) {
			syncDebugLog("engine:batch-out-of-order", {
				batchId,
				action: batch.action,
				minChunkSeq: batch.minChunkSeq,
				maxChunkSeq: batch.maxChunkSeq,
				persistedSeq: this.persistedSeq,
			});
			// Durable marker first, so a reload mid-resync still heals on connect.
			this.onResyncNeeded?.();
			try {
				const result = await this.resyncAll();
				syncDebugLog("engine:batch-out-of-order-resynced", result);
				if (!result.failed && result.incomplete === 0) {
					return true;
				}
			} catch (error) {
				syncDebugLog("engine:batch-out-of-order-resync-failed", { error });
			}
			// completeBatch already removed this batch from pendingBatches, so
			// without this nothing pins the watermark and it advances straight past
			// a changeset that never applied. Mark the carrying entries failed (and
			// un-seen) so the next catch-up genuinely re-fetches and retries them.
			for (const id of batch.entryIds) {
				this.failedApplies.add(id);
				this.seen.delete(id);
			}
			return false;
		}

		// A failed apply un-sees the WHOLE batch, so the retry can rebuild it from
		// a clean re-fetch.
		return this.apply({ changes }, batch.entryIds);
	}

	// Batch rescue: fetch EVERY chunk of a bulk batch directly by batchId -
	// bypassing the watermark and seq ordering entirely - and complete it if the
	// durable log has all the pieces. This is what recovers an interrupted
	// upload no matter WHEN the author finishes it: a seq-ordered fetch can
	// never re-reach chunks below this device's watermark, but a by-id fetch
	// always can. Returns true only if the batch completed AND applied.
	private async rescueBatch(batchId: string): Promise<boolean> {
		if (!this.transport.fetchBatchEntries || this.rescueInFlight.has(batchId)) {
			return false;
		}
		this.rescueInFlight.add(batchId);
		try {
			let entries;
			try {
				entries = await this.transport.fetchBatchEntries(batchId);
			} catch (error) {
				// Network hiccup - no evidence either way. The next sweep retries.
				syncDebugLog("engine:batch-rescue-fetch-failed", { batchId, error });
				return false;
			}
			const chunks = entries.filter(
				(entry) =>
					entry.batchId === batchId &&
					entry.chunkIndex !== undefined &&
					entry.chunkCount !== undefined &&
					entry.authorId !== this.transport.clientId,
			);
			if (chunks.length === 0) {
				return false;
			}

			let batch = this.pendingBatches.get(batchId);
			if (!batch) {
				const first = chunks[0]!;
				batch = {
					count: first.chunkCount!,
					chunks: new Map(),
					entryIds: [],
					action: first.action,
					authorId: first.authorId,
					maxChunkSeq: 0,
					minChunkSeq: Number.POSITIVE_INFINITY,
				};
				this.pendingBatches.set(batchId, batch);
				this.rebuildingBatches.delete(batchId);
			}
			for (const entry of chunks) {
				if (!batch.chunks.has(entry.chunkIndex!)) {
					batch.chunks.set(
						entry.chunkIndex!,
						entry.payloadPart ?? entry.changeset.changes,
					);
					if (!batch.entryIds.includes(entry.id)) {
						batch.entryIds.push(entry.id);
					}
					// Mark seen so a later ordered fetch of the same chunk is a no-op.
					this.seen.add(entry.id);
					if (entry.seq > batch.maxChunkSeq) {
						batch.maxChunkSeq = entry.seq;
					}
					if (entry.seq < batch.minChunkSeq) {
						batch.minChunkSeq = entry.seq;
					}
				}
			}

			if (batch.chunks.size < batch.count) {
				// The log genuinely lacks some chunks right now (e.g. they are still
				// in the author's outbox). This is the ONLY sound evidence for
				// judging a batch dead - and even then, abandonment is remembered so
				// a later upload resurrects it (see handleChunk).
				syncDebugLog("engine:batch-rescue-short", {
					batchId,
					have: batch.chunks.size,
					need: batch.count,
				});
				return false;
			}

			syncDebugLog("engine:batch-rescued", {
				batchId,
				action: batch.action,
				chunks: batch.count,
			});
			const ok = await this.completeBatch(batchId, batch);
			if (ok) {
				this.abandonedBatches.delete(batchId);
				this.advanceWatermark();
			}
			return ok;
		} finally {
			this.rescueInFlight.delete(batchId);
		}
	}

	// Apply a changeset, attributing the outcome to the log entries that carried
	// it. On failure those entries are marked failed (pinning the watermark below
	// them) and removed from `seen` (so the next catch-up pass re-fetches and
	// genuinely re-applies them - dedup would otherwise skip the "retry" forever).
	// On success any earlier failure marks for them are cleared, which unpins the
	// watermark automatically. We deliberately do NOT kick off a full-log resync
	// on failure - re-applying the entire history on every hiccup is brutally
	// slow on a phone; the pinned watermark + periodic catchUp() converge, and
	// the manual "Force full resync" remains the big hammer.
	private async apply(
		changeset: Changeset,
		entryIds?: string[],
	): Promise<boolean> {
		try {
			await applyChangeset(changeset);
		} catch (error) {
			console.error("Failed to apply remote changeset", error);
			if (entryIds) {
				for (const id of entryIds) {
					this.failedApplies.add(id);
					this.seen.delete(id);
				}
				syncDebugLog("engine:apply-failed", {
					entries: entryIds.length,
					entryIds: entryIds.slice(0, 5),
					records: changeset.changes.length,
					error,
				});
			}
			return false;
		}
		if (entryIds) {
			for (const id of entryIds) {
				this.failedApplies.delete(id);
			}
		}
		return true;
	}

	// Drain everything after our watermark, PAGE BY PAGE, and bank the durable
	// watermark after each page. This is the workhorse that lets a device which has
	// been away for weeks actually catch up: pulling the whole backlog in one query
	// times out / runs out of memory on a phone and makes zero progress, so it
	// never converges. Paging fixes both - bounded memory per fetch, and durable
	// progress banked per page so an interruption resumes instead of restarting.
	//
	// A `fetchCursor` walks forward through the backlog independent of the durable
	// watermark: whole-record bulk sims are chunked across many entries, so the
	// durable watermark can't advance until a batch is fully assembled (which may
	// straddle several pages), but we must keep fetching past it to gather the rest
	// of the batch. The watermark still only advances over fully-applied,
	// gap-free history (advanceWatermark guards that), so resume-after-interrupt
	// stays correct.
	//
	// Reentrancy-guarded so the connect-time drain and the periodic timer don't
	// double-fetch. Returns true when it has drained all the way to the head this
	// pass (so the caller may safely start the live subscription from there);
	// false if a fetch failed, an apply failed, or it stopped early with more to
	// go - in which case the next tick resumes from the banked watermark.
	async catchUp(): Promise<boolean> {
		if (!this.transport.fetchEntriesSince || this.stopped) {
			return false;
		}
		if (this.catchingUp) {
			const stalledFor = Date.now() - this.catchingUpSince;
			if (stalledFor < CATCH_UP_STALL_TIMEOUT) {
				return false;
			}
			// The previous pass is never coming back. Take the guard from it and
			// drop its progress total, so the bar doesn't keep showing a count that
			// nothing is working through - this pass recounts from scratch.
			syncDebugLog("engine:catchup-stalled", {
				stalledFor,
				progressDone: this.catchUpDone,
				progressTotal: this.catchUpTotal,
				persistedSeq: this.persistedSeq,
				maxSeq: this.maxSeq,
			});
			this.finishCatchUp();
		}
		this.catchingUp = true;
		this.catchingUpSince = Date.now();
		// Per-pass diagnostics: enough to see from a pasted console log exactly why
		// a device that keeps "catching up" isn't converging (fetches failing,
		// applies failing, a bulk batch missing chunks, or the watermark pinned).
		const startSeq = this.persistedSeq;
		let pages = 0;
		let fetched = 0;
		let applied = 0;
		let outcome = "page-cap";
		try {
			// On a fresh drain (not one already in progress), measure how far behind
			// we are so the UI can show a real total + ETA. Cheap server-side count.
			// Count from the FETCH FRONTIER (the highest seq we've already pulled),
			// not the durable watermark: an incomplete bulk batch pins the watermark
			// behind entries that were long since fetched and applied, and counting
			// from the pinned spot made every 15s tick re-show a full "catching up"
			// bar for a tail that contains zero new work. Only genuinely unfetched
			// entries should surface the bar.
			if (this.catchUpTotal === undefined && this.transport.countEntriesSince) {
				const frontier = Math.max(this.persistedSeq, this.maxSeq);
				try {
					// Time-boxed like the fetches below: a hung count would latch the
					// reentrancy guard just as effectively, and losing the progress
					// bar is a far smaller cost than losing catch-up entirely.
					const remaining = await withTimeout(
						this.transport.countEntriesSince(frontier),
						CATCH_UP_SMALL_FETCH_TIMEOUT,
					);
					// A `remaining` that stays high while `behind` (maxSeq - persistedSeq)
					// is ~0 means the count query and the watermark disagree - a classic
					// cause of a stuck "catching up" bar (the total is set but the drain
					// immediately hits head and never counts down). Logged so that shows up.
					syncDebugLog("engine:catchup-count", {
						remaining,
						frontier,
						persistedSeq: this.persistedSeq,
						maxSeq: this.maxSeq,
						behind: Math.max(0, this.maxSeq - this.persistedSeq),
						willShowBar: remaining >= CATCH_UP_PROGRESS_MIN,
					});
					if (remaining >= CATCH_UP_PROGRESS_MIN) {
						this.catchUpTotal = remaining;
						this.catchUpDone = 0;
						this.catchUpBaseline = frontier;
						this.reportCatchUp();
					}
				} catch (error) {
					// A failed count just means no progress bar; the drain still runs.
					syncDebugLog("engine:catchup-count-failed", { error });
				}
			}

			let fetchCursor = this.persistedSeq;
			// A bounded number of pages per call, so a single catchUp() can't spin
			// forever if the head keeps moving; the next tick picks up where we left.
			for (let page = 0; page < CATCH_UP_MAX_PAGES; page++) {
				// The engine was torn down mid-drain (a reconnect replaced it). Stop
				// touching the cache immediately - a zombie drain from a replaced
				// engine otherwise keeps paging concurrently with the new engine's
				// drain, and the two interleaved passes churn each other's state.
				if (this.stopped) {
					outcome = "stopped";
					return false;
				}
				let entries: ChangesetEntry[];
				// Requested page size for THIS fetch - the head check below must compare
				// against what was actually asked for, or a successful small-page retry
				// would read as a short page and falsely declare the head reached.
				let requested = CATCH_UP_PAGE_SIZE;
				// Logged BEFORE the await. Without this a capture taken while a fetch
				// is outstanding is indistinguishable from one where the fetch was
				// never issued - both show a count and then silence - and that
				// ambiguity cost two rounds of diagnosis.
				const fetchStarted = Date.now();
				syncDebugLog("engine:catchup-fetch-start", {
					page,
					fetchCursor,
					requested,
				});
				try {
					entries = await withTimeout(
						this.transport.fetchEntriesSince(fetchCursor, requested),
						CATCH_UP_FETCH_TIMEOUT,
					);
					syncDebugLog("engine:catchup-fetch-done", {
						page,
						ms: Date.now() - fetchStarted,
						entries: entries.length,
					});
				} catch (error) {
					// A full page of bulk-sim chunks can be ~15 MB and time out on a weak
					// connection - the exact moment a behind device is trying to recover.
					// Retry this page tiny before failing the pass; single chunks always
					// get through.
					syncDebugLog("engine:catchup-fetch-retry-small", {
						page,
						fetchCursor,
						ms: Date.now() - fetchStarted,
						error,
					});
					requested = CATCH_UP_SMALL_PAGE;
					try {
						entries = await withTimeout(
							this.transport.fetchEntriesSince(fetchCursor, requested),
							CATCH_UP_SMALL_FETCH_TIMEOUT,
						);
					} catch (error2) {
						outcome = "fetch-failed";
						syncDebugLog("engine:catchup-fetch-failed", {
							page,
							fetchCursor,
							error: error2,
						});
						return false;
					}
				}
				// A reconnect may have stopped this engine while the fetch was in
				// flight (the top-of-loop check only covers stops between pages). Bail
				// before applying entries or re-pushing progress, so a zombie pass from
				// a replaced engine can't touch the new session's cache or re-show a
				// catch-up bar that teardown already cleared.
				if (this.stopped) {
					outcome = "stopped";
					return false;
				}
				pages += 1;
				if (entries.length === 0) {
					// Nothing after the cursor - we're at the head.
					outcome = "head";
					await this.sweepStaleBatches();
					this.finishCatchUp();
					return true;
				}

				fetched += entries.length;
				for (const entry of entries.sort((a, b) => a.seq - b.seq)) {
					if (await this.handleEntry(entry)) {
						applied += 1;
					}
					// Only entries past the counted baseline advance the bar - a
					// re-fetched pinned tail (already seen, applies nothing) shouldn't
					// eat into a total that never included it.
					if (entry.seq > this.catchUpBaseline) {
						this.catchUpDone += 1;
					}
					if (entry.seq > fetchCursor) {
						fetchCursor = entry.seq;
					}
				}
				this.advanceWatermark();
				this.reportCatchUp();
				syncDebugLog("engine:catchup-page", {
					page,
					entries: entries.length,
					firstSeq: entries[0]!.seq,
					lastSeq: entries[entries.length - 1]!.seq,
					appliedSoFar: applied,
					persistedSeq: this.persistedSeq,
					pendingBatches: this.pendingBatches.size,
					failedApplies: this.failedApplies.size,
				});

				// A changeset failed to apply: the watermark is now pinned here and
				// paging further just piles unusable entries into memory. Stop and let
				// a resync / retry recover, rather than draining the whole log for
				// nothing.
				if (this.applyFailed) {
					outcome = "apply-failed";
					return false;
				}

				// Short page → we've reached the head. (Compared against the size THIS
				// fetch requested - a small-page retry returning its full ask is not
				// the head.)
				if (entries.length < requested) {
					outcome = "head";
					await this.sweepStaleBatches();
					this.finishCatchUp();
					return true;
				}
			}
			// Hit the per-call page cap with full pages still coming: more to drain.
			//
			// Sweep BEFORE returning, bounded to how far this pass actually walked.
			// This is the call sweepStaleBatches documents as essential, and without
			// it a device far enough behind can never recover: the head is more than
			// one page budget away, so the head-only sweeps above are unreachable;
			// an incomplete bulk batch pins the watermark; and every pass therefore
			// restarts from the same pinned seq, re-fetching the identical window,
			// applying nothing (all entries already `seen`), and hitting this cap
			// again - forever. Sweeping here is what lets the batch be rescued by
			// batchId (or, if its chunks truly aren't in the log, abandoned) so the
			// watermark can move and catch-up can finish.
			await this.sweepStaleBatches(fetchCursor);
			return false;
		} finally {
			this.catchingUp = false;
			// One summary per pass that did work or hit a blocker; the every-15s
			// idle no-op (0 entries fetched, nothing pinned) stays silent.
			if (
				fetched > 0 ||
				outcome !== "head" ||
				this.pendingBatches.size > 0 ||
				this.applyFailed
			) {
				syncDebugLog("engine:catchup-pass", {
					outcome,
					pages,
					fetched,
					applied,
					startSeq,
					persistedSeq: this.persistedSeq,
					maxSeq: this.maxSeq,
					watermarkAdvanced: this.persistedSeq > startSeq,
					pendingBatches: this.describePendingBatches(),
					failedApplies: [...this.failedApplies].slice(0, 10),
					progressDone: this.catchUpDone,
					progressTotal: this.catchUpTotal,
					liveSubscription: this.hasChangesSubscription(),
				});
			}
		}
	}

	// Cap on how many times a stale batch AT THE HEAD of the log (its author may
	// still be uploading) may be dropped and rebuilt before we leave it pinned
	// and just keep logging. A batch the log has provably moved past doesn't get
	// this patience - one failed rebuild is enough to abandon it (see
	// sweepStaleBatches), since every extra cycle re-fetches the whole pinned
	// tail and flashes the catching-up indicator for nothing.
	private static BATCH_RESET_LIMIT = 5;

	// Called when a catch-up pass reaches the head of the log, OR hits its page
	// cap (with throughSeq = how far it walked). Any bulk batch whose full seq
	// range was walked and is STILL missing chunks is suspect: those chunks
	// either weren't in the log or were somehow dropped on the way in. A batch
	// that makes NO progress between two such passes gets reset - its entries
	// are un-seen and the buffered chunks dropped - so the next pass re-fetches
	// and rebuilds it from scratch. That's the same safe retry path a failed
	// apply uses; if the chunks exist server-side the rebuild completes and the
	// watermark unpins automatically. (A batch still receiving chunks - the
	// simmer is mid-upload - keeps growing between passes and is left alone.)
	//
	// The page-cap invocation is what makes abandonment REACHABLE on a device
	// far behind: sweeping only at the head, with every pass restarting from the
	// PINNED watermark under a bounded page budget, meant a long-enough tail
	// (an active room kept simming past the pin) made the head unreachable and
	// a dead batch unabandonable - pinned forever, re-downloading the same tail
	// every tick. throughSeq preserves the evidence requirement: only batches
	// whose entire chunk range the pass provably re-fetched are judged.
	private async sweepStaleBatches(throughSeq = Infinity) {
		const nowStale = new Map<string, number>();
		for (const [batchId, batch] of this.pendingBatches) {
			if (batch.maxChunkSeq > throughSeq) {
				// This pass didn't walk the batch's full range - no evidence about
				// its missing chunks either way. Preserve its stale-tracking state.
				const prior = this.staleBatchHave.get(batchId);
				if (prior !== undefined) {
					nowStale.set(batchId, prior);
				}
				continue;
			}
			// Is this batch PROVABLY dead - can its missing chunks ever still arrive?
			// They can't when EITHER:
			//   - its AUTHOR has published entries beyond it (FIFO outbox: anything
			//     still queued there would have to publish before those later
			//     entries, so the missing chunks can never arrive), OR
			//   - the LOG itself continues past the batch (maxSeq is beyond the
			//     batch's last chunk, and the sweep only judges batches whose full
			//     range this pass walked - so every entry around it was seen and
			//     the chunks simply are not there). This case is the one that
			//     was wedging devices forever: a chunk lost during upload, with the
			//     room's later activity coming from OTHER authors, left
			//     authorProgress behind the batch even though the log had moved on
			//     by tens of thousands of entries.
			const authorProgress = this.lastSeqByAuthor.get(batch.authorId) ?? 0;
			const logMovedPast = this.maxSeq > batch.maxChunkSeq;
			const provablyDead = authorProgress > batch.maxChunkSeq || logMovedPast;
			const lastHave = this.staleBatchHave.get(batchId);
			if (
				!provablyDead &&
				(lastHave === undefined || batch.chunks.size > lastHave)
			) {
				// First sighting, or it grew since last pass - it may still be
				// arriving, so check again next pass. Only batches that are NOT
				// provably dead get this patience: a dead batch's chunks can never
				// arrive (see above), and each extra confirmation pass costs a full
				// cycle - on a phone that suspends the app after ~a minute of
				// inactivity, these in-memory counters reset on every suspend, so a
				// long dance never finished at all. Dead batches go straight to a
				// reset on first sighting and are abandoned right after ONE clean
				// rebuild still comes up short.
				nowStale.set(batchId, batch.chunks.size);
				continue;
			}

			// Before ANY reset/abandon judgment, try to complete the batch by
			// fetching its chunks directly by batchId. This finds chunks a
			// seq-ordered re-fetch can never reach (below the watermark - e.g.
			// after a restart mid-batch) and settles the question with evidence:
			// if the rescue comes up short, the chunks truly are not in the log
			// right now. A completed rescue applies in-order (the watermark is
			// still pinned at this batch) and ends the story here.
			if (await this.rescueBatch(batchId)) {
				continue;
			}
			if (!this.pendingBatches.has(batchId)) {
				// The rescue path removed it (e.g. reassembly failed into the
				// failed-apply retry path) - nothing left to judge this pass.
				continue;
			}

			const resets = this.batchResetCounts.get(batchId) ?? 0;
			const detail = {
				batchId,
				action: batch.action,
				have: batch.chunks.size,
				need: batch.count,
				resets,
			};
			if (provablyDead && resets >= 1) {
				// One clean rebuild already re-fetched the whole tail and the chunks
				// still weren't there, and the log has provably moved past the batch -
				// so they are not in the durable log and never will be. Abandon it
				// (entries stay `seen`) so the watermark can move again, NOW: every
				// extra rebuild cycle re-fetched the entire pinned tail and flashed
				// the catching-up indicator for a batch that was already proven dead.
				this.pendingBatches.delete(batchId);
				this.batchResetCounts.delete(batchId);
				this.rebuildingBatches.delete(batchId);
				// Remember it: if the author later drains its outbox and the missing
				// chunks land in the log, handleChunk resurrects the batch and the
				// rescue completes it - the skipped changeset still applies instead
				// of needing a manual Force full resync.
				this.abandonedBatches.add(batchId);
				if (this.abandonedBatches.size > SyncEngine.ABANDONED_MEMORY_LIMIT) {
					const oldest = this.abandonedBatches.values().next().value;
					if (oldest !== undefined) {
						this.abandonedBatches.delete(oldest);
					}
				}
				// `abandonedBatches` is in-memory, so a reload loses the in-session
				// resurrection path and the device would stay silently behind (this is
				// how a ready-up phase advance stranded two follower devices on the old
				// phase). Persist a durable marker so the next connect self-heals with a
				// full-log resync instead of waiting for a manual Force full resync.
				this.onResyncNeeded?.();
				syncDebugLog("engine:batch-abandoned", {
					...detail,
					authorProgress,
					maxChunkSeq: batch.maxChunkSeq,
					maxSeq: this.maxSeq,
					reason:
						authorProgress > batch.maxChunkSeq ? "author-past" : "log-past",
				});
				console.error(
					"[sync] Skipped a bulk change whose chunks are not in the log yet; it will auto-recover if they ever upload. If anything looks stale, use Force full resync.",
					detail,
				);
				continue;
			}
			if (!provablyDead && resets >= SyncEngine.BATCH_RESET_LIMIT) {
				// The incomplete batch IS the head of the log (nothing published after
				// it yet) - its author may still be uploading the rest. Keep waiting.
				nowStale.set(batchId, batch.chunks.size);
				syncDebugLog("engine:batch-permanently-incomplete", detail);
				console.error(
					"[sync] A bulk change at the head of the log never fully arrived; waiting for its author to finish uploading.",
					detail,
				);
				continue;
			}
			this.batchResetCounts.set(batchId, resets + 1);
			for (const id of batch.entryIds) {
				this.seen.delete(id);
			}
			this.pendingBatches.delete(batchId);
			// Keep the watermark pinned until the re-fetch re-forms this batch.
			this.rebuildingBatches.add(batchId);
			// Track its size so a batch that stays stuck (never grows) counts toward
			// the reset limit on EVERY pass - converging in ~LIMIT passes instead of
			// alternating reset / first-sighting and taking twice as long.
			nowStale.set(batchId, batch.chunks.size);
			syncDebugLog("engine:batch-reset", detail);
		}
		this.staleBatchHave = nowStale;
		// An abandoned batch may have just released the watermark - bank it now
		// rather than waiting for the next entry to arrive.
		this.advanceWatermark();
	}

	// Emit current drain progress (no-op unless a progress total is set - i.e. the
	// gap was big enough to bother showing). A stopped (torn-down) engine never
	// reports: it must not re-show a catch-up bar the new session's teardown
	// already cleared.
	private reportCatchUp() {
		if (this.stopped || this.catchUpTotal === undefined) {
			return;
		}
		this.onCatchUpProgress?.({
			done: Math.min(this.catchUpDone, this.catchUpTotal),
			total: this.catchUpTotal,
		});
	}

	// Clear the drain progress once we've reached the head.
	private finishCatchUp() {
		if (this.catchUpTotal !== undefined) {
			this.catchUpTotal = undefined;
			this.catchUpDone = 0;
			this.catchUpBaseline = 0;
			this.onCatchUpProgress?.(undefined);
		}
	}

	// Is the cloud connection actually live right now? The sim/advance/transaction
	// guard calls this so we never mutate the shared league while only *looking*
	// connected. `force` demands a real round-trip (no recent-contact shortcut),
	// used before a sim so a silently-dead socket can't slip a sim through and then
	// fail to upload. A transport without the probe (the test fake) is treated as live.
	async verifyConnection(force = false): Promise<boolean> {
		return (await this.transport.verifyConnection?.(force)) ?? true;
	}

	// When the live changes subscription last delivered an entry (0 = never).
	// See startChangesSubscription for why this is tracked separately from the
	// transport's global contact time.
	private lastChangesDeliveryAt = 0;

	getLastChangesDeliveryAt(): number {
		return this.lastChangesDeliveryAt;
	}

	// Ms since last confirmed live contact with the cloud (undefined if untracked).
	// Drives the header status dot - a soft signal kept fresh by the subscription/
	// catch-up while healthy, going stale when the connection quietly dies.
	contactAge(): number | undefined {
		const at = this.transport.getLastContactAt?.();
		return at === undefined ? undefined : Date.now() - at;
	}

	// The watermark we've durably caught up through (server-timestamp millis).
	getPersistedSeq(): number {
		return this.persistedSeq;
	}

	// A snapshot of everything that decides whether we're "caught up", so a
	// device stuck on the catching-up indicator can be diagnosed from a pasted
	// console log: how far the watermark trails the head, whether a bulk batch is
	// waiting on missing chunks, whether an apply is pinned, and the live progress
	// counters that drive the UI indicator.
	// A sweep just RESET a batch (tore it down for a clean re-fetch) and its
	// rebuild pass hasn't re-formed it yet. The drain loop chains immediately
	// while this is true - rebuild-and-abandon must complete in one continuous
	// sequence, not across poll ticks a phone screen-lock can interrupt.
	hasRebuildingBatches(): boolean {
		return this.rebuildingBatches.size > 0;
	}

	getCatchUpDiagnostics() {
		return {
			caughtUp: this.isCaughtUp(),
			persistedSeq: this.persistedSeq,
			maxSeq: this.maxSeq,
			behind: Math.max(0, this.maxSeq - this.persistedSeq),
			pendingBatches: this.pendingBatches.size,
			rebuilding: this.rebuildingBatches.size,
			pendingBatchDetail: this.describePendingBatches(),
			applyFailed: this.applyFailed,
			failedApplies: this.failedApplies.size,
			catchingUp: this.catchingUp,
			progressDone: this.catchUpDone,
			progressTotal: this.catchUpTotal,
			liveSubscription: this.hasChangesSubscription(),
		};
	}

	// Is a full-log replay running right now? During one, declining a stale write
	// is the guard doing its job on old history, not evidence of a new problem -
	// see the regression guard in changeset.ts.
	isResyncing(): boolean {
		return this.resyncing;
	}

	// Flag this device as needing a full-log resync on the next connect. The
	// engine calls onResyncNeeded itself when it knowingly skips a batch; this is
	// for the cases it CAN'T notice - a caller that found evidence in the data
	// that shared state went missing (see findStrandedScheduleRows).
	markResyncNeeded() {
		this.onResyncNeeded?.();
	}

	// Read the entire shared change log once (not a live subscription), for a full
	// resync. Empty if the transport can't. Can be large - prefer fetchRecentLog
	// for anything that only needs recent activity.
	async fetchLog(): Promise<ChangesetEntry[]> {
		const fetch = this.transport.fetchAllEntries?.();
		// Time-boxed for the same reason the paged fetch is: this is the whole
		// log, it runs on the recovery path for a device that's silently behind,
		// and a request that never settles would wedge that recovery with no way
		// back. Longer than a page fetch, because it IS the whole log.
		const entries = fetch
			? await withTimeout(fetch, CATCH_UP_FULL_LOG_TIMEOUT)
			: [];
		return entries.sort((a, b) => a.seq - b.seq);
	}

	// The most recent `n` entries, oldest-first, for the activity panel - so it
	// renders a bounded list instead of pulling the whole log. Falls back to the
	// full log for a transport without the paged read (the in-memory test fake).
	async fetchRecentLog(n: number): Promise<ChangesetEntry[]> {
		const entries = this.transport.fetchRecentEntries
			? await this.transport.fetchRecentEntries(n)
			: ((await this.transport.fetchAllEntries?.()) ?? []);
		return entries.sort((a, b) => a.seq - b.seq);
	}

	// Force a full catch-up: re-read the WHOLE log and re-apply every entry from
	// the beginning, in order. Safe to run anytime because every change is a
	// whole-record write (idempotent) - applying old-then-new in timestamp order
	// always lands on the current shared state. This is the manual recovery for a
	// device that silently diverged (e.g. an apply that failed and got skipped).
	// Own entries are re-applied too; they just rewrite our own latest values.
	async resyncAll(): Promise<{
		total: number;
		applied: number;
		incomplete: number;
		failed: boolean;
	}> {
		const entries = await this.fetchLog();

		// Start clean so nothing is deduped away and no half-batch lingers.
		this.pendingBatches.clear();
		this.failedApplies.clear();
		this.resyncing = true;
		this.deferredDuringResync.length = 0;
		try {
			return await this.resyncAllInner(entries);
		} finally {
			this.resyncing = false;
			const deferred = this.deferredDuringResync.splice(0);
			deferred.sort((a, b) => a.seq - b.seq);
			for (const entry of deferred) {
				try {
					await this.handleEntry(entry);
				} catch (error) {
					syncDebugLog("engine:deferred-entry-failed", { error });
				}
			}
		}
	}

	private async resyncAllInner(
		entries: Awaited<ReturnType<SyncEngine["fetchLog"]>>,
	): Promise<{
		total: number;
		applied: number;
		incomplete: number;
		failed: boolean;
	}> {
		let applied = 0;
		let newMaxSeq = this.maxSeq;

		// Replay by ORDERING UNIT, not by entry.
		//
		// A bulk changeset's place in the log is where it STARTED. Its chunks are a
		// transport detail, and a chunk the author re-uploaded days later carries a
		// timestamp from the RE-UPLOAD, not from when the change was made. Walking
		// entries in plain seq order therefore lands a whole draft-era changeset
		// after a phase advance that genuinely came later - the phase goes
		// backwards, and it goes backwards even though the replay was "in order".
		//
		// So group each batch's chunks into one unit keyed by its EARLIEST chunk,
		// and sort units by that. Single entries are units of one. Within a batch
		// the chunks go in index order so it completes on its last one.
		type Unit = { key: number; entries: typeof entries };
		const units: Unit[] = [];
		const batchUnits = new Map<string, Unit>();
		for (const entry of entries) {
			if (entry.seq > newMaxSeq) {
				newMaxSeq = entry.seq;
			}
			const isChunk =
				entry.batchId !== undefined &&
				entry.chunkIndex !== undefined &&
				entry.chunkCount !== undefined;
			if (!isChunk) {
				units.push({ key: entry.seq, entries: [entry] });
				continue;
			}
			let unit = batchUnits.get(entry.batchId!);
			if (!unit) {
				unit = { key: entry.seq, entries: [] };
				batchUnits.set(entry.batchId!, unit);
				units.push(unit);
			}
			unit.key = Math.min(unit.key, entry.seq);
			unit.entries.push(entry);
		}
		for (const unit of batchUnits.values()) {
			unit.entries.sort((a, b) => a.chunkIndex! - b.chunkIndex!);
		}
		units.sort((a, b) => a.key - b.key);

		for (const unit of units) {
			for (const entry of unit.entries) {
				this.seen.add(entry.id);

				const ok =
					entry.batchId !== undefined &&
					entry.chunkIndex !== undefined &&
					entry.chunkCount !== undefined
						? await this.handleChunk(entry)
						: await this.apply(entry.changeset, [entry.id]);
				if (ok) {
					applied++;
				}
			}
		}

		// Batches still missing a chunk after reading the WHOLE log: those chunks
		// genuinely aren't in the cloud (a publish that never finished), so this
		// device can't catch up from the log at all.
		const incomplete = this.pendingBatches.size;
		this.pendingBatches.clear();
		this.maxSeq = Math.max(this.maxSeq, newMaxSeq);

		// Only bank the watermark if EVERYTHING re-applied cleanly. If any entry
		// failed to apply OR a batch is missing a chunk, leave the watermark where
		// it was - never skip past unapplied data and silently diverge. A fully
		// clean pass also resets the auto-resync budget.
		if (!this.applyFailed && incomplete === 0) {
			if (newMaxSeq > this.persistedSeq) {
				this.persistedSeq = newMaxSeq;
				this.onWatermark?.(this.persistedSeq);
			}
		}

		return {
			total: entries.length,
			applied,
			incomplete,
			failed: this.applyFailed,
		};
	}
}
