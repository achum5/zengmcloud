/**
 * Cloud Sync Service
 *
 * Handles all Firestore operations for cloud league synchronization.
 * This runs entirely in the UI thread since Firebase doesn't work in SharedWorker.
 */

import {
	doc,
	setDoc,
	getDoc,
	getDocFromServer,
	getDocs,
	getDocsFromServer,
	deleteDoc,
	collection,
	query,
	where,
	writeBatch,
	onSnapshot,
	limit,
	startAfter,
	orderBy,
	type Unsubscribe,
	type QueryDocumentSnapshot,
	type DocumentData,
	type QuerySnapshot,
} from "firebase/firestore";
import { getFirebaseDb, getFirebaseAuth, getCurrentUserId, getUserDisplayName, getUserEmail, waitForAuth, getDeviceId } from "./firebase.ts";
import type { Store, CloudLeague, CloudMember, CloudSyncStatus } from "../../common/cloudTypes.ts";
import { toWorker } from "./index.ts";
import { localActions } from "./local.ts";

// All stores that need to be synced
const ALL_STORES: Store[] = [
	"allStars", "awards", "draftLotteryResults", "draftPicks", "events",
	"gameAttributes", "games", "headToHeads", "messages", "negotiations",
	"playerFeats", "players", "playoffSeries", "releasedPlayers", "savedTrades",
	"savedTradingBlock", "schedule", "scheduledEvents", "seasonLeaders",
	"teamSeasons", "teamStats", "teams", "trade",
];

// Primary keys for each store
const STORE_PRIMARY_KEYS: Record<Store, string> = {
	allStars: "season",
	awards: "season",
	draftLotteryResults: "season",
	draftPicks: "dpid",
	events: "eid",
	gameAttributes: "key",
	games: "gid",
	headToHeads: "season",
	messages: "mid",
	negotiations: "pid",
	playerFeats: "fid",
	players: "pid",
	playoffSeries: "season",
	releasedPlayers: "rid",
	savedTrades: "hash",
	savedTradingBlock: "rid",
	schedule: "gid",
	scheduledEvents: "id",
	seasonLeaders: "season",
	teamSeasons: "rid",
	teamStats: "rid",
	teams: "tid",
	trade: "rid",
};

// Current state
let currentCloudId: string | null = null;
let syncStatus: CloudSyncStatus = "disconnected";
let listeners: Map<string, Unsubscribe> = new Map();
let statusCallback: ((status: CloudSyncStatus) => void) | null = null;

// Pending updates state - for notification-based sync
let pendingUpdateCallback: ((info: PendingUpdateInfo | null) => void) | null = null;
let lastKnownUpdateTime: number = 0;

export type PendingUpdateInfo = {
	updatedAt: number;
	updatedBy: string; // displayName of who made the change
	message?: string;
};

// Set callback for pending update notifications
export const onPendingUpdate = (callback: (info: PendingUpdateInfo | null) => void) => {
	pendingUpdateCallback = callback;
};

// Notify UI of pending update
const notifyPendingUpdate = (info: PendingUpdateInfo | null) => {
	if (pendingUpdateCallback) {
		pendingUpdateCallback(info);
	}
};

// ====== Device Version Tracking (for incremental sync) ======
// Each device tracks the version of each store it last synced, so we can
// determine which stores need to be downloaded on refresh.
// Using version numbers instead of timestamps avoids clock skew issues.

const getDeviceStoreVersions = (cloudId: string): Record<string, number> => {
	const key = `cloudStoreVersions_${cloudId}`;
	const stored = localStorage.getItem(key);
	if (!stored) return {};
	try {
		return JSON.parse(stored);
	} catch {
		return {};
	}
};

const setDeviceStoreVersions = (cloudId: string, versions: Record<string, number>): void => {
	const key = `cloudStoreVersions_${cloudId}`;
	localStorage.setItem(key, JSON.stringify(versions));
};

const updateDeviceStoreVersions = (cloudId: string, updates: Record<string, number>): void => {
	const current = getDeviceStoreVersions(cloudId);
	setDeviceStoreVersions(cloudId, { ...current, ...updates });
};

// Legacy: Clean up old timestamp-based tracking
const clearLegacyDeviceSyncTime = (cloudId: string): void => {
	const key = `cloudLastSync_${cloudId}`;
	localStorage.removeItem(key);
};

/**
 * Migrate a league from timestamp-based storeUpdates to version-based storeVersions.
 * Called when entering a league to ensure it uses the new version system.
 * Returns the new storeVersions if migration was performed, or null if no migration needed.
 */
const migrateToVersionNumbers = async (cloudId: string, league: CloudLeague): Promise<Record<string, number> | null> => {
	// Already has storeVersions - no migration needed
	if (league.storeVersions && Object.keys(league.storeVersions).length > 0) {
		return null;
	}

	// Check if it has legacy storeUpdates (timestamps)
	const hasLegacyTimestamps = league.storeUpdates && Object.keys(league.storeUpdates).length > 0;

	console.log("[CloudSync] Checking migration:", {
		cloudId,
		hasStoreVersions: !!(league.storeVersions && Object.keys(league.storeVersions).length > 0),
		hasLegacyTimestamps,
	});

	// Migrate: set all stores to version 1
	const db = getFirebaseDb();
	const storeVersions: Record<string, number> = {};
	for (const store of ALL_STORES) {
		storeVersions[store] = 1;
	}

	console.log("[CloudSync] Migrating league to version numbers:", cloudId);
	await setDoc(doc(db, "leagues", cloudId), {
		storeVersions,
	}, { merge: true });

	// Also set device versions to 1 (assuming we're in sync)
	setDeviceStoreVersions(cloudId, storeVersions);
	clearLegacyDeviceSyncTime(cloudId);

	// Return the new versions so caller can use them instead of stale in-memory data
	return storeVersions;
};

// Remove undefined values (Firestore doesn't accept them)
const removeUndefined = (obj: any): any => {
	if (obj === null || obj === undefined) return null;
	if (Array.isArray(obj)) return obj.map(removeUndefined);
	if (typeof obj === "object") {
		const cleaned: Record<string, any> = {};
		for (const [key, value] of Object.entries(obj)) {
			if (value !== undefined) {
				cleaned[key] = removeUndefined(value);
			}
		}
		return cleaned;
	}
	return obj;
};

// Set status callback
export const onSyncStatusChange = (callback: (status: CloudSyncStatus) => void) => {
	statusCallback = callback;
};

// Update sync status
const setSyncStatus = (status: CloudSyncStatus) => {
	syncStatus = status;
	// Update global state so NavBar can show status
	localActions.update({ cloudSyncStatus: status });
	if (statusCallback) {
		statusCallback(status);
	}
};

// Get current sync status
export const getSyncStatus = (): CloudSyncStatus => syncStatus;

// Get current cloud ID
export const getCurrentCloudId = (): string | null => currentCloudId;

/**
 * Create a new cloud league
 */
export const createCloudLeague = async (
	name: string,
	sport: "basketball" | "football" | "baseball" | "hockey",
	userTeamId: number,
): Promise<string> => {
	console.log("[createCloudLeague] Starting...");
	const db = getFirebaseDb();
	console.log("[createCloudLeague] Got db");
	const userId = getCurrentUserId();
	console.log("[createCloudLeague] userId:", userId);
	if (!userId) throw new Error("Not signed in");

	const cloudId = `league-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
	console.log("[createCloudLeague] cloudId:", cloudId);

	const email = getUserEmail();
	const member: CloudMember = {
		userId: userId,
		displayName: getUserDisplayName() || "Unknown",
		teamId: userTeamId,
		role: "commissioner",
		joinedAt: Date.now(),
	};
	// Only add email if it exists (Firestore doesn't accept undefined)
	if (email) {
		member.email = email;
	}

	const leagueData = {
		cloudId,
		name,
		sport,
		ownerId: userId,
		members: [member],
		createdAt: Date.now(),
		updatedAt: Date.now(),
		season: 0,
		phase: 0,
		schemaVersion: 1,
	};

	console.log("[createCloudLeague] Calling setDoc...");
	await setDoc(doc(db, "leagues", cloudId), removeUndefined(leagueData));
	console.log("[createCloudLeague] setDoc complete!");

	return cloudId;
};

/**
 * Upload league data to Firestore
 */
export const uploadLeagueData = async (
	cloudId: string,
	onProgress?: (message: string, percent: number) => void,
): Promise<void> => {
	console.log("[uploadLeagueData] Starting for cloudId:", cloudId);
	const db = getFirebaseDb();
	setSyncStatus("syncing");

	try {
		// Get all data from worker
		console.log("[uploadLeagueData] Calling toWorker getLeagueDataForCloud...");
		onProgress?.("Collecting league data...", 0);
		const allData = await toWorker("main", "getLeagueDataForCloud", undefined) as Record<Store, any[]>;
		console.log("[uploadLeagueData] Got data from worker");

		// Count total records
		let totalRecords = 0;
		for (const store of ALL_STORES) {
			totalRecords += (allData[store] || []).length;
		}

		let uploadedRecords = 0;
		// Use smaller batch size to avoid payload limit (10MB max per request)
		const BATCH_SIZE = 50;

		for (const store of ALL_STORES) {
			const records = allData[store] || [];
			if (records.length === 0) continue;

			const pk = STORE_PRIMARY_KEYS[store];
			const collectionPath = `leagues/${cloudId}/stores/${store}/data`;

			// Upload in batches
			for (let i = 0; i < records.length; i += BATCH_SIZE) {
				const batch = writeBatch(db);
				const batchRecords = records.slice(i, i + BATCH_SIZE);

				for (const record of batchRecords) {
					const docId = String(record[pk]);
					const docRef = doc(db, collectionPath, docId);
					// Serialize as JSON to avoid Firestore limitations (nested arrays, undefined, etc.)
					batch.set(docRef, { _json: JSON.stringify(record) });
				}

				await batch.commit();
				uploadedRecords += batchRecords.length;

				const percent = Math.round((uploadedRecords / totalRecords) * 100);
				onProgress?.(`Uploading ${store}...`, percent);
			}
		}

		// Extract season and phase from gameAttributes
		const gameAttributesArray = allData.gameAttributes || [];
		const gameAttributesObj: Record<string, any> = {};
		for (const ga of gameAttributesArray) {
			if (ga.key) {
				gameAttributesObj[ga.key] = ga.value;
			}
		}

		// Build storeVersions object with version 1 for all stores (initial upload)
		// Using version numbers instead of timestamps avoids clock skew issues
		const now = Date.now();
		const storeVersions: Record<string, number> = {};
		for (const store of ALL_STORES) {
			storeVersions[store] = 1;
		}

		// Update league metadata with actual season/phase and store versions
		await setDoc(doc(db, "leagues", cloudId), {
			updatedAt: now,
			season: gameAttributesObj.season || 0,
			phase: gameAttributesObj.phase || 0,
			storeVersions,
		}, { merge: true });

		// Save device's store versions (we just uploaded everything at version 1)
		setDeviceStoreVersions(cloudId, storeVersions);
		// Clean up any legacy timestamp tracking
		clearLegacyDeviceSyncTime(cloudId);

		setSyncStatus("synced");
		onProgress?.("Upload complete!", 100);
	} catch (error) {
		setSyncStatus("error");
		throw error;
	}
};

/**
 * Download league data from Firestore
 */
export const downloadLeagueData = async (
	cloudId: string,
	onProgress?: (message: string, percent: number) => void,
): Promise<Record<Store, any[]>> => {
	const db = getFirebaseDb();
	setSyncStatus("syncing");

	try {
		const data: Record<Store, any[]> = {} as any;
		let storeIndex = 0;

		for (const store of ALL_STORES) {
			onProgress?.(`Downloading ${store}...`, Math.round((storeIndex / ALL_STORES.length) * 100));

			const collectionPath = `leagues/${cloudId}/stores/${store}/data`;
			const snapshot = await getDocs(collection(db, collectionPath));

			data[store] = [];
			snapshot.forEach((docSnap) => {
				const docData = docSnap.data();
				// Parse JSON back to original format
				if (docData._json) {
					data[store].push(JSON.parse(docData._json));
				} else {
					data[store].push(docData);
				}
			});

			storeIndex++;
		}

		setSyncStatus("synced");
		onProgress?.("Download complete!", 100);
		return data;
	} catch (error) {
		setSyncStatus("error");
		throw error;
	}
};

/**
 * STREAMING download - Downloads league data from Firestore in small batches
 * to avoid memory exhaustion on mobile devices.
 *
 * Instead of loading all data into memory at once, this function:
 * 1. Creates the league database first
 * 2. Streams each store from Firestore using pagination (500 docs at a time)
 * 3. Immediately writes each batch to IndexedDB via worker
 * 4. Finalizes the league after all data is downloaded
 *
 * This approach keeps memory usage low and works well on mobile devices.
 */
export const streamDownloadLeagueData = async (
	cloudId: string,
	leagueName: string,
	memberTeamId: number | undefined,
	onProgress?: (message: string, percent: number) => void,
): Promise<number> => {
	const db = getFirebaseDb();
	setSyncStatus("syncing");

	const BATCH_SIZE = 500; // Documents per Firestore query
	const totalStores = ALL_STORES.length;

	try {
		onProgress?.("Initializing league...", 1);

		// Initialize the league in worker (creates empty database)
		const lid = await toWorker("main", "initCloudLeagueDownload", {
			cloudId,
			name: leagueName,
		});

		try {
			// Stream each store
			let storeIndex = 0;
			for (const store of ALL_STORES) {
				const storePercent = Math.round((storeIndex / totalStores) * 90) + 5;
				onProgress?.(`Downloading ${store}...`, storePercent);

				const collectionPath = `leagues/${cloudId}/stores/${store}/data`;
				const collectionRef = collection(db, collectionPath);

				// Use pagination to fetch in batches
				let lastDoc: QueryDocumentSnapshot<DocumentData> | null = null;
				let hasMore = true;
				let batchCount = 0;

				while (hasMore) {
					// Build query with pagination
					const baseQuery = query(collectionRef, orderBy("__name__"), limit(BATCH_SIZE));
					const paginatedQuery = lastDoc
						? query(collectionRef, orderBy("__name__"), startAfter(lastDoc), limit(BATCH_SIZE))
						: baseQuery;

					const snapshot: QuerySnapshot<DocumentData> = await getDocs(paginatedQuery);

					if (snapshot.empty) {
						hasMore = false;
						break;
					}

					// Parse the batch
					const records: any[] = [];
					snapshot.forEach((docSnap: QueryDocumentSnapshot) => {
						const docData = docSnap.data();
						// Parse JSON back to original format
						if (docData._json) {
							records.push(JSON.parse(docData._json));
						} else {
							records.push(docData);
						}
					});

					// Send batch to worker to write to IndexedDB
					await toWorker("main", "writeCloudStoreBatch", {
						store,
						records,
					});

					batchCount++;

					// Update progress within the store
					const batchPercent = storePercent + Math.min(batchCount, 5);
					onProgress?.(`Downloading ${store}... (batch ${batchCount})`, batchPercent);

					// Check if there are more documents
					if (snapshot.docs.length < BATCH_SIZE) {
						hasMore = false;
					} else {
						lastDoc = snapshot.docs[snapshot.docs.length - 1] ?? null;
					}
				}

				storeIndex++;
			}

			// Finalize the league (updates metadata from actual data)
			onProgress?.("Finalizing league...", 97);
			await toWorker("main", "finalizeCloudLeagueDownload", {
				memberTeamId,
			});

			// Get league metadata to save store versions
			const league = await getCloudLeague(cloudId);
			if (league?.storeVersions) {
				// Save the cloud's current store versions
				setDeviceStoreVersions(cloudId, league.storeVersions);
			} else {
				// Legacy league without versions - set all to 1 as baseline
				const defaultVersions: Record<string, number> = {};
				for (const store of ALL_STORES) {
					defaultVersions[store] = 1;
				}
				setDeviceStoreVersions(cloudId, defaultVersions);
			}
			clearLegacyDeviceSyncTime(cloudId);

			setSyncStatus("synced");
			onProgress?.("Download complete!", 100);

			return lid;
		} catch (error) {
			// Clean up partial download
			await toWorker("main", "cancelCloudLeagueDownload", undefined);
			throw error;
		}
	} catch (error) {
		setSyncStatus("error");
		throw error;
	}
};

/**
 * Get list of cloud leagues user has access to
 */
export const getCloudLeagues = async (): Promise<CloudLeague[]> => {
	const db = getFirebaseDb();
	const userId = getCurrentUserId();
	if (!userId) return [];

	try {
		// Query leagues where user is the owner
		const ownerQuery = query(
			collection(db, "leagues"),
			where("ownerId", "==", userId)
		);
		const ownerSnapshot = await getDocs(ownerQuery);

		const leagues: CloudLeague[] = [];
		ownerSnapshot.forEach((docSnap) => {
			leagues.push(docSnap.data() as CloudLeague);
		});

		// Sort by updatedAt descending
		return leagues.sort((a, b) => b.updatedAt - a.updatedAt);
	} catch (error) {
		console.error("Failed to get cloud leagues:", error);
		return [];
	}
};

/**
 * Get cloud league metadata
 */
export const getCloudLeague = async (cloudId: string): Promise<CloudLeague | null> => {
	const db = getFirebaseDb();

	try {
		const docSnap = await getDoc(doc(db, "leagues", cloudId));
		if (docSnap.exists()) {
			return docSnap.data() as CloudLeague;
		}
		return null;
	} catch (error) {
		console.error("Failed to get cloud league:", error);
		return null;
	}
};

/**
 * Delete a cloud league
 */
export const deleteCloudLeague = async (cloudId: string): Promise<void> => {
	const db = getFirebaseDb();
	const userId = getCurrentUserId();
	if (!userId) throw new Error("Not signed in");

	// Verify ownership
	const league = await getCloudLeague(cloudId);
	if (!league) throw new Error("League not found");
	if (league.ownerId !== userId) throw new Error("You don't own this league");

	// Delete the league document (subcollections will be orphaned but that's ok for now)
	await deleteDoc(doc(db, "leagues", cloudId));
};

/**
 * Start real-time sync for a cloud league.
 *
 * LIGHTWEIGHT APPROACH: Instead of 22 aggressive listeners that cause lag,
 * we only listen to the league metadata document for updates.
 * When another user makes changes, we show a notification and let the
 * user choose when to refresh - just like BBGM's local file behavior.
 */
export const startRealtimeSync = async (cloudId: string): Promise<void> => {
	console.log("[CloudSync] startRealtimeSync called with cloudId:", cloudId);

	// Wait for Firebase Auth to be ready before proceeding
	// This ensures we have the correct userId for ownership checks
	console.log("[CloudSync] Waiting for Firebase Auth...");
	await waitForAuth();

	const db = getFirebaseDb();
	const userId = getCurrentUserId();
	console.log("[CloudSync] Current userId:", userId);

	// Stop any existing sync
	stopRealtimeSync();

	currentCloudId = cloudId;
	setSyncStatus("connecting");

	// Load current user's membership info for permission checks
	await loadCurrentCloudMember();
	console.log("[CloudSync] Loaded cloud member:", currentCloudMember);

	// Get current update time as baseline
	// CRITICAL: Use getDocFromServer to bypass Firestore cache!
	// We need accurate storeVersions for the comparison below.
	// If we use cached data, we might miss updates made while this device was offline.
	let league: CloudLeague | null = null;
	try {
		const leagueDocSnap = await getDocFromServer(doc(db, "leagues", cloudId));
		if (leagueDocSnap.exists()) {
			league = leagueDocSnap.data() as CloudLeague;
		}
	} catch (error) {
		console.warn("[CloudSync] Failed to fetch from server, falling back to cache:", error);
		league = await getCloudLeague(cloudId);
	}
	console.log("[CloudSync] Fetched league from Firestore:", league ? {
		cloudId: league.cloudId,
		updatedAt: league.updatedAt,
		lastUpdatedBy: league.lastUpdatedBy,
		lastUpdatedByUserId: league.lastUpdatedByUserId,
		memberCount: league.members?.length,
		storeVersionsCount: league.storeVersions ? Object.keys(league.storeVersions).length : 0,
		sampleStoreVersions: league.storeVersions ? Object.fromEntries(Object.entries(league.storeVersions).slice(0, 5)) : null,
	} : null);

	if (league) {
		lastKnownUpdateTime = league.updatedAt || 0;

		// Migrate legacy leagues from timestamps to version numbers
		// Returns the new storeVersions if migration was performed
		const migratedVersions = await migrateToVersionNumbers(cloudId, league);
		if (migratedVersions) {
			console.log("[CloudSync] League migrated to version numbers");
		}

		// NOTE: We intentionally do NOT initialize device store versions here.
		// If the device has no stored versions, we leave them empty/zero.
		// This ensures that when a refresh is triggered, the comparison will see
		// cloud versions > device versions (0), causing a full download.
		//
		// Previously, we assumed local IndexedDB data was current if the league
		// existed locally. But this assumption breaks when:
		// 1. Another device made changes while this device was offline
		// 2. The user opens the league without refreshing
		// In these cases, the local data is stale but versions would match cloud,
		// causing refresh to skip downloading any data.
		//
		// Device versions are properly set AFTER data is actually downloaded:
		// - After streamDownloadLeagueData() for new downloads
		// - After refreshFromCloud() for refreshes
		const existingVersions = getDeviceStoreVersions(cloudId);
		// Use migrated versions if migration just happened (avoids stale in-memory data)
		const cloudVersions = migratedVersions || league.storeVersions || {};
		const needsInitialSync = Object.keys(existingVersions).length === 0;

		console.log("[CloudSync] Version comparison on startup:", {
			deviceVersionsCount: Object.keys(existingVersions).length,
			cloudVersionsCount: Object.keys(cloudVersions).length,
			needsInitialSync,
			sampleDeviceVersions: Object.fromEntries(Object.entries(existingVersions).slice(0, 5)),
			sampleCloudVersions: Object.fromEntries(Object.entries(cloudVersions).slice(0, 5)),
		});

		// DEBUG: Log ALL versions for key stores to help debug sync issues
		const keyStores = ["draftPicks", "players", "teams", "gameAttributes", "events"];
		console.log("[CloudSync] DEBUG key store versions:", Object.fromEntries(
			keyStores.map(store => [store, {
				device: existingVersions[store] || 0,
				cloud: cloudVersions[store] || 0,
				match: (existingVersions[store] || 0) === (cloudVersions[store] || 0),
			}])
		));

		// Check if device is behind the cloud (changes made while offline)
		let needsRefreshDueToVersions = false;
		if (!needsInitialSync && Object.keys(cloudVersions).length > 0) {
			// Compare versions to see if any cloud stores are ahead of device
			for (const store of ALL_STORES) {
				const deviceVersion = existingVersions[store] || 0;
				const cloudVersion = cloudVersions[store] || 0;
				if (cloudVersion > deviceVersion) {
					console.log(`[CloudSync] Store ${store} is outdated: device=${deviceVersion}, cloud=${cloudVersion}`);
					needsRefreshDueToVersions = true;
					break;
				}
			}
		} else if (!needsInitialSync && Object.keys(cloudVersions).length === 0) {
			// Edge case: Device has versions but cloud doesn't
			// This shouldn't happen after migration, but if it does, log a warning
			console.warn("[CloudSync] WARNING: Device has versions but cloud has none - this indicates a sync issue");
		}

		if (needsInitialSync) {
			console.log("[CloudSync] Device has no stored versions - will require full refresh to sync");
		}

		if (needsInitialSync || needsRefreshDueToVersions) {
			// Show a notification so the user knows to refresh
			// This handles the case where changes were made while this device was offline
			console.log("[CloudSync] Device is behind cloud - showing refresh notification");
			// Capture values for the closure
			const notificationInfo = {
				updatedAt: league.updatedAt || Date.now(),
				updatedBy: league.lastUpdatedBy || "Cloud",
				message: needsInitialSync ? "Sync required to get latest data" : "Updates available from cloud",
			};
			setTimeout(() => {
				notifyPendingUpdate(notificationInfo);
			}, 1000); // Small delay to let UI fully initialize
		} else {
			console.log("[CloudSync] Device versions match cloud - no refresh needed on startup");
		}
		// Clean up legacy timestamp tracking
		clearLegacyDeviceSyncTime(cloudId);
	} else {
		console.warn("[CloudSync] League not found in Firestore! cloudId:", cloudId);
	}
	console.log("[CloudSync] Setting baseline lastKnownUpdateTime:", lastKnownUpdateTime);

	try {
		// LIGHTWEIGHT: Only listen to league metadata document, not all 22 stores!
		// This single listener detects when ANY changes are made to the league.
		const leagueDocRef = doc(db, "leagues", cloudId);

		const unsubscribe = onSnapshot(leagueDocRef, (docSnapshot) => {
			if (!docSnapshot.exists()) {
				console.log("[CloudSync] Listener: Document does not exist");
				return;
			}

			const data = docSnapshot.data() as CloudLeague & { lastUpdatedByDeviceId?: string };
			const newUpdateTime = data.updatedAt || 0;
			const currentDeviceId = getDeviceId();

			console.log("[CloudSync] Listener triggered:", {
				newUpdateTime,
				lastKnownUpdateTime,
				lastUpdatedBy: data.lastUpdatedBy,
				lastUpdatedByDeviceId: data.lastUpdatedByDeviceId,
				currentDeviceId,
			});

			// If update time changed and it's newer than what we know
			if (newUpdateTime > lastKnownUpdateTime) {
				const updater = data.lastUpdatedBy || "Someone";

				// Don't notify about changes from THIS DEVICE (same browser/device)
				// This allows same user on different devices to see updates
				if (data.lastUpdatedByDeviceId !== currentDeviceId) {
					console.log("[CloudSync] Notifying pending update from:", updater);
					notifyPendingUpdate({
						updatedAt: newUpdateTime,
						updatedBy: updater,
						message: data.lastUpdateMessage,
					});
				} else {
					// Our own change from this device - just update the baseline
					console.log("[CloudSync] Own device change, updating baseline");
					lastKnownUpdateTime = newUpdateTime;
				}
			} else {
				console.log("[CloudSync] No new updates (newUpdateTime <= lastKnownUpdateTime)");
			}
		}, (error) => {
			console.error("League metadata listener error:", error);
			setSyncStatus("error");
		});

		listeners.set("__metadata__", unsubscribe);
		console.log("[CloudSync] Listener successfully attached to:", `leagues/${cloudId}`);
		setSyncStatus("synced");
	} catch (error) {
		setSyncStatus("error");
		throw error;
	}
};

/**
 * Stop real-time sync
 */
export const stopRealtimeSync = () => {
	for (const unsubscribe of listeners.values()) {
		unsubscribe();
	}
	listeners.clear();
	currentCloudId = null;
	currentCloudMember = null;
	lastKnownUpdateTime = 0;
	notifyPendingUpdate(null);
	setSyncStatus("disconnected");
};

// Callback for refresh progress updates
let refreshProgressCallback: ((message: string, percent: number) => void) | null = null;

export const onRefreshProgress = (callback: ((message: string, percent: number) => void) | null) => {
	refreshProgressCallback = callback;
};

/**
 * Refresh league data from cloud.
 * Called when user clicks "Update Available" notification.
 *
 * INCREMENTAL SYNC: Only downloads stores that changed since the device's
 * last sync, making updates much faster (5 stores instead of 22 for a sim).
 *
 * @param forceFullRefresh - If true, ignores version comparison and downloads all stores
 */
export const refreshFromCloud = async (forceFullRefresh: boolean = false): Promise<void> => {
	console.log("[CloudSync] ========== REFRESH FROM CLOUD STARTING ==========");
	console.log("[CloudSync] forceFullRefresh =", forceFullRefresh);

	if (!currentCloudId) {
		throw new Error("Not connected to a cloud league");
	}

	const cloudId = currentCloudId;
	const db = getFirebaseDb();

	console.log("[CloudSync] REFRESH: cloudId =", cloudId);
	console.log("[CloudSync] REFRESH: deviceId =", getDeviceId());

	refreshProgressCallback?.("Connecting to cloud...", 0);

	// CRITICAL: Force server read to bypass Firestore cache!
	// The real-time listener may have triggered the notification, but getDoc() could
	// still return stale cached data. We MUST get fresh data from the server.
	console.log("[CloudSync] Fetching fresh league metadata from server (bypassing cache)...");
	const leagueDocSnap = await getDocFromServer(doc(db, "leagues", cloudId));
	if (!leagueDocSnap.exists()) {
		throw new Error("League not found");
	}
	const league = leagueDocSnap.data() as CloudLeague;

	console.log("[CloudSync] REFRESH: League from Firestore:", {
		cloudId: league.cloudId,
		updatedAt: league.updatedAt,
		lastUpdatedBy: league.lastUpdatedBy,
		lastUpdatedByDeviceId: league.lastUpdatedByDeviceId,
	});

	const userId = getCurrentUserId();
	const member = userId ? league.members.find(m => m.userId === userId) : undefined;
	const memberTeamId = member?.teamId;

	// Update our baseline time
	lastKnownUpdateTime = league.updatedAt || Date.now();

	// Clear pending notification
	notifyPendingUpdate(null);

	setSyncStatus("syncing");

	// Determine which stores need to be refreshed (version-based incremental sync)
	const deviceVersions = getDeviceStoreVersions(cloudId);
	const cloudVersions = league.storeVersions || {};

	console.log("[CloudSync] Version-based incremental sync check:", {
		deviceVersionsCount: Object.keys(deviceVersions).length,
		cloudVersionsCount: Object.keys(cloudVersions).length,
		sampleDeviceVersions: Object.fromEntries(Object.entries(deviceVersions).slice(0, 5)),
		sampleCloudVersions: Object.fromEntries(Object.entries(cloudVersions).slice(0, 5)),
	});

	let storesToRefresh: Store[];
	let isFullRefresh = false;

	if (forceFullRefresh) {
		// User requested full refresh - bypass version comparison
		console.log("[CloudSync] FORCED FULL REFRESH - ignoring version comparison");
		storesToRefresh = [...ALL_STORES];
		isFullRefresh = true;
	} else if (Object.keys(deviceVersions).length === 0 || Object.keys(cloudVersions).length === 0) {
		// First sync or no version tracking data - do full refresh
		console.log("[CloudSync] Full refresh reason:", {
			deviceVersionsEmpty: Object.keys(deviceVersions).length === 0,
			cloudVersionsEmpty: Object.keys(cloudVersions).length === 0,
		});
		storesToRefresh = [...ALL_STORES];
		isFullRefresh = true;
	} else {
		// Incremental sync - only refresh stores where cloud version > device version
		console.log("[CloudSync] Store versions comparison:", {
			stores: Object.fromEntries(
				ALL_STORES.map(store => [store, {
					device: deviceVersions[store] || 0,
					cloud: cloudVersions[store] || 0,
					needsRefresh: (cloudVersions[store] || 0) > (deviceVersions[store] || 0),
				}])
			),
		});

		storesToRefresh = ALL_STORES.filter(store => {
			const deviceVersion = deviceVersions[store] || 0;
			const cloudVersion = cloudVersions[store] || 0;
			return cloudVersion > deviceVersion;
		});
		console.log(`[CloudSync] Incremental refresh: ${storesToRefresh.length}/${ALL_STORES.length} stores have newer versions`);
		console.log("[CloudSync] Stores to refresh:", storesToRefresh);
	}

	if (storesToRefresh.length === 0 && !forceFullRefresh) {
		console.log("[CloudSync] No stores need refreshing - already up to date");
		refreshProgressCallback?.("Already up to date!", 100);
		setSyncStatus("synced");
		// Sync versions with cloud (in case there are any new stores)
		if (Object.keys(cloudVersions).length > 0) {
			setDeviceStoreVersions(cloudId, cloudVersions);
		}
		// Small delay then reload to refresh UI
		setTimeout(() => window.location.reload(), 500);
		return;
	}

	refreshProgressCallback?.(`Refreshing ${storesToRefresh.length} store(s)...`, 2);

	try {
		// For full refresh, clear all data first
		// For incremental, we'll just overwrite the changed stores
		if (isFullRefresh) {
			console.log("[CloudSync] Initializing full refresh (clearing all data)...");
			await toWorker("main", "initCloudLeagueRefresh", undefined);
		} else {
			console.log("[CloudSync] Incremental refresh (clearing only changed stores)...");
			await toWorker("main", "initIncrementalRefresh", { stores: storesToRefresh });
		}

		const BATCH_SIZE = 500;
		const ATOMIC_THRESHOLD = 5000; // Use atomic replacement for stores under this size
		const totalStores = storesToRefresh.length;
		let storeIndex = 0;

		// Download and replace each store
		// Small stores: atomic replacement (safe to interrupt)
		// Large stores: batched writes (mobile-friendly, but less safe)
		for (const store of storesToRefresh) {
			const storePercent = Math.round((storeIndex / totalStores) * 90) + 5;
			console.log(`[CloudSync] Refreshing store: ${store} (${storeIndex + 1}/${totalStores})`);
			refreshProgressCallback?.(`Downloading ${store}...`, storePercent);

			const collectionPath = `leagues/${cloudId}/stores/${store}/data`;
			const collectionRef = collection(db, collectionPath);

			// Download data for this store
			const allRecords: any[] = [];
			let lastDoc: QueryDocumentSnapshot<DocumentData> | null = null;
			let hasMore = true;
			let batchCount = 0;
			let useBatchedWrites = false; // Switch to batched mode if store is large

			while (hasMore) {
				const baseQuery = query(collectionRef, orderBy("__name__"), limit(BATCH_SIZE));
				const paginatedQuery = lastDoc
					? query(collectionRef, orderBy("__name__"), startAfter(lastDoc), limit(BATCH_SIZE))
					: baseQuery;

				// CRITICAL: Force server read to bypass Firestore cache!
				const snapshot: QuerySnapshot<DocumentData> = await getDocsFromServer(paginatedQuery);

				if (snapshot.empty) {
					hasMore = false;
					break;
				}

				batchCount++;

				// Parse the batch
				const batchRecords: any[] = [];
				snapshot.forEach((docSnap: QueryDocumentSnapshot) => {
					const docData = docSnap.data();
					if (docData._json) {
						batchRecords.push(JSON.parse(docData._json));
					} else {
						batchRecords.push(docData);
					}
				});

				// Check if we should switch to batched mode (for mobile memory)
				if (!useBatchedWrites && allRecords.length + batchRecords.length > ATOMIC_THRESHOLD) {
					console.log(`[CloudSync] ${store}: switching to batched writes (${allRecords.length + batchRecords.length} records exceeds threshold)`);
					useBatchedWrites = true;

					// Write accumulated records so far
					if (allRecords.length > 0) {
						await toWorker("main", "writeCloudRefreshBatch", { store, records: allRecords });
						allRecords.length = 0; // Clear the array
					}
				}

				if (useBatchedWrites) {
					// Write this batch immediately (mobile-friendly)
					await toWorker("main", "writeCloudRefreshBatch", { store, records: batchRecords });
					console.log(`[CloudSync] ${store}: wrote batch ${batchCount}, ${batchRecords.length} records`);
				} else {
					// Accumulate for atomic replacement
					allRecords.push(...batchRecords);
					console.log(`[CloudSync] ${store}: downloaded batch ${batchCount}, total ${allRecords.length} records`);
				}

				if (snapshot.docs.length < BATCH_SIZE) {
					hasMore = false;
				} else {
					lastDoc = snapshot.docs[snapshot.docs.length - 1] ?? null;
				}
			}

			// Write remaining records
			if (!useBatchedWrites && allRecords.length > 0) {
				// Small store: atomic replacement (clear + write in single transaction)
				console.log(`[CloudSync] ${store}: atomic replacement with ${allRecords.length} records`);
				// DEBUG: Log sample downloaded data
				const pk = STORE_PRIMARY_KEYS[store];
				const sampleIds = allRecords.slice(0, 5).map(r => r[pk]);
				console.log(`[CloudSync] DEBUG ${store} sample IDs downloaded:`, sampleIds);
				// For draftPicks, log more detail
				if (store === "draftPicks") {
					console.log(`[CloudSync] DEBUG draftPicks downloaded sample:`, allRecords.slice(0, 3).map(r => ({
						dpid: r.dpid,
						pick: r.pick,
						round: r.round,
						season: r.season,
						tid: r.tid,
						originalTid: r.originalTid,
						pid: r.pid,
					})));
				}
				// For players, log relevant fields
				if (store === "players") {
					const recentDrafted = allRecords.filter(p => p.draft && p.draft.year >= 2025).slice(0, 3);
					console.log(`[CloudSync] DEBUG players with recent draft:`, recentDrafted.map(r => ({
						pid: r.pid,
						firstName: r.firstName,
						lastName: r.lastName,
						tid: r.tid,
						draft: r.draft,
					})));
				}
				refreshProgressCallback?.(`Saving ${store}...`, storePercent + 2);
				await toWorker("main", "atomicStoreReplace", { store, records: allRecords });
			} else if (!useBatchedWrites && allRecords.length === 0) {
				// Empty store in cloud - clear local store to match
				console.log(`[CloudSync] ${store}: store is empty in cloud, clearing local store`);
				await toWorker("main", "atomicStoreReplace", { store, records: [] });
			} else if (useBatchedWrites) {
				// Large store: already written in batches
				console.log(`[CloudSync] ${store}: batched writes complete`);
			}

			storeIndex++;
		}

		// Finalize the refresh
		console.log("[CloudSync] Finalizing refresh...");
		refreshProgressCallback?.("Finalizing...", 97);
		await toWorker("main", "finalizeCloudRefresh", { memberTeamId });

		// Update device's store versions to match cloud
		if (Object.keys(cloudVersions).length > 0) {
			// Update device versions to match cloud for refreshed stores
			const updatedVersions = { ...deviceVersions };
			for (const store of storesToRefresh) {
				updatedVersions[store] = cloudVersions[store] || 1;
			}
			setDeviceStoreVersions(cloudId, updatedVersions);
			console.log("[CloudSync] Updated device store versions:", updatedVersions);
		}

		// If this was a full refresh and the cloud didn't have storeVersions data,
		// populate them now so future refreshes can be incremental.
		if (isFullRefresh && Object.keys(cloudVersions).length === 0) {
			console.log("[CloudSync] Populating storeVersions in Firestore for future incremental syncs...");
			const allStoreVersions: Record<string, number> = {};
			for (const store of ALL_STORES) {
				allStoreVersions[store] = 1;
			}
			await setDoc(doc(db, "leagues", cloudId), {
				storeVersions: allStoreVersions,
			}, { merge: true });
			// Save these versions locally too
			setDeviceStoreVersions(cloudId, allStoreVersions);
		}

		// Clean up legacy timestamp tracking
		clearLegacyDeviceSyncTime(cloudId);

		console.log(`[CloudSync] Refresh complete! Updated ${storesToRefresh.length} stores`);
		refreshProgressCallback?.("Done! Reloading...", 100);
		setSyncStatus("synced");

		// Reload the page to show the fresh data
		window.location.reload();
	} catch (error) {
		console.error("[CloudSync] Refresh error:", error);
		refreshProgressCallback?.(null as any, 0);
		setSyncStatus("error");
		throw error;
	}
};

/**
 * Mark that we just made changes (updates the league metadata)
 * Call this after syncing local changes to cloud.
 */
export const markLeagueUpdated = async (message?: string): Promise<void> => {
	if (!currentCloudId) return;

	const db = getFirebaseDb();
	const userId = getCurrentUserId();
	const displayName = getUserDisplayName() || "Unknown";
	const deviceId = getDeviceId();

	const now = Date.now();
	lastKnownUpdateTime = now;

	await setDoc(doc(db, "leagues", currentCloudId), {
		updatedAt: now,
		lastUpdatedBy: displayName,
		lastUpdatedByUserId: userId,
		lastUpdatedByDeviceId: deviceId,
		lastUpdateMessage: message,
	}, { merge: true });
};

/**
 * Sync local changes to Firestore (data only, no metadata update)
 */
const syncLocalChangesData = async (
	store: Store,
	records: any[],
	deletedIds: (string | number)[],
): Promise<void> => {
	if (!currentCloudId) return;

	const db = getFirebaseDb();
	const pk = STORE_PRIMARY_KEYS[store];
	const collectionPath = `leagues/${currentCloudId}/stores/${store}/data`;

	console.log(`[CloudSync] syncLocalChangesData: uploading to ${collectionPath}`);

	const BATCH_SIZE = 400;

	// Process updates
	for (let i = 0; i < records.length; i += BATCH_SIZE) {
		const batch = writeBatch(db);
		const batchRecords = records.slice(i, i + BATCH_SIZE);

		for (const record of batchRecords) {
			const docId = String(record[pk]);
			const docRef = doc(db, collectionPath, docId);
			// Serialize as JSON to avoid Firestore limitations
			batch.set(docRef, { _json: JSON.stringify(record) });
		}

		await batch.commit();
	}

	// Process deletes
	for (let i = 0; i < deletedIds.length; i += BATCH_SIZE) {
		const batch = writeBatch(db);
		const batchIds = deletedIds.slice(i, i + BATCH_SIZE);

		for (const id of batchIds) {
			const docRef = doc(db, collectionPath, String(id));
			batch.delete(docRef);
		}

		await batch.commit();
	}
};

/**
 * Sync local changes to Firestore (single store, with metadata update)
 * This is kept for backwards compatibility but syncLocalChangesMultiple is preferred.
 */
export const syncLocalChanges = async (
	store: Store,
	records: any[],
	deletedIds: (string | number)[],
): Promise<void> => {
	await syncLocalChangesData(store, records, deletedIds);
	await updateSyncMetadata([store]);
};

/**
 * Sync multiple stores to Firestore, then update metadata atomically.
 * This prevents race conditions where Device B refreshes between store syncs.
 */
export const syncLocalChangesMultiple = async (
	changes: Array<{
		store: Store;
		records: any[];
		deletedIds: (string | number)[];
	}>,
): Promise<void> => {
	if (!currentCloudId) {
		console.log("[CloudSync] syncLocalChangesMultiple: skipped - no currentCloudId");
		return;
	}

	console.log("[CloudSync] ========== UPLOADING CHANGES TO CLOUD ==========");
	console.log("[CloudSync] UPLOAD: cloudId =", currentCloudId);
	console.log("[CloudSync] UPLOAD: deviceId =", getDeviceId());
	console.log(`[CloudSync] UPLOAD: processing ${changes.length} store changes`);

	const syncedStores: Store[] = [];

	// First, sync all data for all stores
	for (const { store, records, deletedIds } of changes) {
		if (records.length > 0 || deletedIds.length > 0) {
			console.log(`[CloudSync] Syncing data for ${store}: ${records.length} records, ${deletedIds.length} deletes`);
			// DEBUG: Log sample record data to verify what's being uploaded
			if (records.length > 0) {
				const pk = STORE_PRIMARY_KEYS[store];
				const sampleIds = records.slice(0, 3).map(r => r[pk]);
				console.log(`[CloudSync] DEBUG ${store} sample IDs being uploaded:`, sampleIds);
				// For draftPicks specifically, log more detail
				if (store === "draftPicks" && records.length > 0) {
					console.log(`[CloudSync] DEBUG draftPicks sample data:`, records.slice(0, 2).map(r => ({
						dpid: r.dpid,
						pick: r.pick,
						round: r.round,
						season: r.season,
						tid: r.tid,
						originalTid: r.originalTid,
						pid: r.pid, // Player ID if pick was made
					})));
				}
				// For players, log relevant fields
				if (store === "players" && records.length > 0) {
					console.log(`[CloudSync] DEBUG players sample:`, records.slice(0, 2).map(r => ({
						pid: r.pid,
						firstName: r.firstName,
						lastName: r.lastName,
						tid: r.tid,
						draft: r.draft ? { year: r.draft.year, pick: r.draft.pick, round: r.draft.round } : null,
					})));
				}
			}
			await syncLocalChangesData(store, records, deletedIds);
			syncedStores.push(store);
		}
	}

	// Then update metadata for ALL stores at once (atomic operation)
	if (syncedStores.length > 0) {
		console.log(`[CloudSync] All data synced, now updating metadata for ${syncedStores.length} stores:`, syncedStores);
		await updateSyncMetadata(syncedStores);
	} else {
		console.log("[CloudSync] syncLocalChangesMultiple: no stores had changes to sync");
	}
};

/**
 * Update league metadata after syncing stores.
 * Increments version numbers for all updated stores atomically.
 */
const updateSyncMetadata = async (stores: Store[]): Promise<void> => {
	if (!currentCloudId || stores.length === 0) return;

	const db = getFirebaseDb();
	const userId = getCurrentUserId();
	const displayName = getUserDisplayName() || "Unknown";
	const deviceId = getDeviceId();
	const now = Date.now();
	lastKnownUpdateTime = now; // Update our baseline so we don't notify ourselves

	// CRITICAL: Use getDocFromServer to bypass Firestore cache!
	// Using cached data can cause version numbers to not increment properly
	// if multiple syncs happen in quick succession.
	console.log("[CloudSync] updateSyncMetadata: fetching current versions from server for stores:", stores);
	let currentVersions: Record<string, number> = {};
	try {
		const leagueDocSnap = await getDocFromServer(doc(db, "leagues", currentCloudId));
		if (leagueDocSnap.exists()) {
			const league = leagueDocSnap.data() as CloudLeague;
			currentVersions = league?.storeVersions || {};
		}
	} catch (error) {
		console.warn("[CloudSync] Failed to fetch from server, falling back to cache:", error);
		const league = await getCloudLeague(currentCloudId);
		currentVersions = league?.storeVersions || {};
	}
	console.log("[CloudSync] updateSyncMetadata: current versions from cloud:", currentVersions);

	// Build the update object with incremented versions
	const updateData: Record<string, any> = {
		updatedAt: now,
		lastUpdatedBy: displayName,
		lastUpdatedByUserId: userId,
		lastUpdatedByDeviceId: deviceId,
		lastUpdateMessage: `Updated ${stores.join(", ")}`,
	};

	// Increment version for each updated store
	const newVersions: Record<string, number> = {};
	for (const store of stores) {
		const currentVersion = currentVersions[store] || 0;
		const newVersion = currentVersion + 1;
		updateData[`storeVersions.${store}`] = newVersion;
		newVersions[store] = newVersion;
		console.log(`[CloudSync] Incrementing ${store}: ${currentVersion} -> ${newVersion}`);
	}

	console.log("[CloudSync] Writing metadata update to Firestore...");
	await setDoc(doc(db, "leagues", currentCloudId), updateData, { merge: true });
	console.log("[CloudSync] Metadata update complete");

	// Update local device versions to match
	updateDeviceStoreVersions(currentCloudId, newVersions);
	console.log("[CloudSync] Updated device store versions:", newVersions);
};

/**
 * Add a member to a cloud league
 */
export const addLeagueMember = async (
	cloudId: string,
	userId: string,
	displayName: string,
	email: string | undefined,
	teamId: number,
): Promise<void> => {
	const db = getFirebaseDb();
	const currentUserId = getCurrentUserId();
	if (!currentUserId) throw new Error("Not signed in");

	// Verify ownership
	const league = await getCloudLeague(cloudId);
	if (!league) throw new Error("League not found");
	if (league.ownerId !== currentUserId) throw new Error("Only the owner can add members");

	const member: CloudMember = {
		userId: userId,
		displayName,
		teamId,
		role: "member",
		joinedAt: Date.now(),
	};
	// Only add email if it exists (Firestore doesn't accept undefined)
	if (email) {
		member.email = email;
	}

	// Add to members array
	await setDoc(doc(db, "leagues", cloudId), {
		members: [...league.members, member],
		updatedAt: Date.now(),
	}, { merge: true });
};

/**
 * Join a cloud league as a member (for non-owners)
 * The user provides the cloudId (shared by the commissioner)
 */
export const joinCloudLeague = async (
	cloudId: string,
	teamId: number,
): Promise<CloudLeague> => {
	const db = getFirebaseDb();
	const auth = getFirebaseAuth();
	const user = auth.currentUser;
	if (!user) throw new Error("Not signed in");

	// Get the league
	const league = await getCloudLeague(cloudId);
	if (!league) throw new Error("League not found. Check the league ID and try again.");

	// Check if already a member
	const existingMember = league.members.find(m => m.userId === user.uid);
	if (existingMember) {
		throw new Error("You are already a member of this league");
	}

	// Check if team is already taken
	const teamTaken = league.members.find(m => m.teamId === teamId);
	if (teamTaken) {
		throw new Error(`Team ${teamId} is already claimed by ${teamTaken.displayName}`);
	}

	// Add self as member
	const member: CloudMember = {
		userId: user.uid,
		displayName: user.displayName || user.email || "Unknown",
		teamId,
		role: "member",
		joinedAt: Date.now(),
	};
	if (user.email) {
		member.email = user.email;
	}

	// Update league with new member
	await setDoc(doc(db, "leagues", cloudId), {
		members: [...league.members, member],
		updatedAt: Date.now(),
	}, { merge: true });

	return { ...league, members: [...league.members, member] };
};

/**
 * Get leagues the current user is a member of (but not owner)
 */
export const getJoinedLeagues = async (): Promise<CloudLeague[]> => {
	const db = getFirebaseDb();
	const auth = getFirebaseAuth();
	const user = auth.currentUser;
	if (!user) return [];

	// Note: Firestore array-contains needs exact object match, so we can't query by userId alone
	// For now, fetch all leagues and filter client-side
	// TODO: Use a separate "memberships" subcollection for better scalability

	const allLeaguesSnapshot = await getDocs(collection(db, "leagues"));
	const leagues: CloudLeague[] = [];

	allLeaguesSnapshot.forEach((docSnap) => {
		const data = docSnap.data() as CloudLeague;
		// Check if user is a member (but not owner - those are in getCloudLeagues)
		if (data.ownerId !== user.uid) {
			const isMember = data.members?.some(m => m.userId === user.uid);
			if (isMember) {
				leagues.push({
					...data,
					cloudId: docSnap.id,
				});
			}
		}
	});

	return leagues;
};

/**
 * Get list of teams and their current owners for a league
 */
export const getLeagueMembers = async (cloudId: string): Promise<CloudMember[]> => {
	const league = await getCloudLeague(cloudId);
	return league?.members || [];
};

/**
 * Get teams from a cloud league for team selection UI
 */
export type CloudTeam = {
	tid: number;
	region: string;
	name: string;
	abbrev: string;
	claimedBy?: string; // displayName of user who claimed this team
};

export const getCloudLeagueTeams = async (cloudId: string): Promise<CloudTeam[]> => {
	const db = getFirebaseDb();

	// Get the league to check members
	const league = await getCloudLeague(cloudId);
	if (!league) throw new Error("League not found");

	// Get teams from Firestore
	const teamsPath = `leagues/${cloudId}/stores/teams/data`;
	const teamsSnapshot = await getDocs(collection(db, teamsPath));

	const teams: CloudTeam[] = [];
	teamsSnapshot.forEach((docSnap) => {
		const data = docSnap.data();
		// Parse JSON if stored that way
		const teamData = data._json ? JSON.parse(data._json) : data;

		// Check if this team is claimed
		const claimer = league.members.find(m => m.teamId === teamData.tid);

		teams.push({
			tid: teamData.tid,
			region: teamData.region || "???",
			name: teamData.name || "???",
			abbrev: teamData.abbrev || "???",
			claimedBy: claimer?.displayName,
		});
	});

	// Sort by tid
	teams.sort((a, b) => a.tid - b.tid);

	return teams;
};

// ==== Cloud Permission System ====

// Current user's cloud membership (cached for the active league)
let currentCloudMember: CloudMember | null = null;

/**
 * Get the current user's membership info for the active cloud league
 */
export const getCurrentCloudMember = (): CloudMember | null => currentCloudMember;

/**
 * Load/refresh the current user's cloud membership
 */
export const loadCurrentCloudMember = async (): Promise<CloudMember | null> => {
	if (!currentCloudId) {
		currentCloudMember = null;
		return null;
	}

	const userId = getCurrentUserId();
	if (!userId) {
		currentCloudMember = null;
		return null;
	}

	const league = await getCloudLeague(currentCloudId);
	if (!league) {
		currentCloudMember = null;
		return null;
	}

	currentCloudMember = league.members.find(m => m.userId === userId) || null;
	return currentCloudMember;
};

/**
 * Check if the current user is the commissioner of the active cloud league
 */
export const isCloudCommissioner = (): boolean => {
	return currentCloudMember?.role === "commissioner";
};

/**
 * Get the team ID the current user controls in the cloud league
 */
export const getCloudUserTeamId = (): number | null => {
	return currentCloudMember?.teamId ?? null;
};

/**
 * Check if the current user can control a specific team
 */
export const canControlTeam = (teamId: number): boolean => {
	// If not in a cloud league, allow all teams
	if (!currentCloudId || !currentCloudMember) {
		return true;
	}

	// User can only control their assigned team
	return currentCloudMember.teamId === teamId;
};

/**
 * Check if the current user can simulate games (commissioner only)
 */
export const canSimulateGames = (): boolean => {
	// If not in a cloud league, allow simulation
	if (!currentCloudId || !currentCloudMember) {
		return true;
	}

	// Only commissioner can simulate
	return currentCloudMember.role === "commissioner";
};

/**
 * Remove a member from a cloud league (commissioner only)
 */
export const removeLeagueMember = async (
	cloudId: string,
	memberUserId: string,
): Promise<void> => {
	const db = getFirebaseDb();
	const currentUserId = getCurrentUserId();
	if (!currentUserId) throw new Error("Not signed in");

	// Get the league
	const league = await getCloudLeague(cloudId);
	if (!league) throw new Error("League not found");

	// Verify ownership
	if (league.ownerId !== currentUserId) {
		throw new Error("Only the commissioner can remove members");
	}

	// Can't remove yourself (the commissioner)
	if (memberUserId === currentUserId) {
		throw new Error("You cannot remove yourself from the league");
	}

	// Find and remove the member
	const updatedMembers = league.members.filter(m => m.userId !== memberUserId);

	if (updatedMembers.length === league.members.length) {
		throw new Error("Member not found in league");
	}

	// Update the league
	await setDoc(doc(db, "leagues", cloudId), {
		members: updatedMembers,
		updatedAt: Date.now(),
	}, { merge: true });
};

/**
 * Update a member's team assignment (commissioner only)
 */
export const updateMemberTeam = async (
	cloudId: string,
	memberUserId: string,
	newTeamId: number,
): Promise<void> => {
	const db = getFirebaseDb();
	const currentUserId = getCurrentUserId();
	if (!currentUserId) throw new Error("Not signed in");

	// Get the league
	const league = await getCloudLeague(cloudId);
	if (!league) throw new Error("League not found");

	// Verify ownership
	if (league.ownerId !== currentUserId) {
		throw new Error("Only the commissioner can reassign teams");
	}

	// Check if the new team is already taken by someone else
	const teamOwner = league.members.find(m => m.teamId === newTeamId && m.userId !== memberUserId);
	if (teamOwner) {
		throw new Error(`Team is already assigned to ${teamOwner.displayName}`);
	}

	// Find and update the member
	const memberIndex = league.members.findIndex(m => m.userId === memberUserId);
	if (memberIndex === -1) {
		throw new Error("Member not found in league");
	}

	const updatedMembers = league.members.map((m, i) =>
		i === memberIndex ? { ...m, teamId: newTeamId } : m
	);

	// Update the league
	await setDoc(doc(db, "leagues", cloudId), {
		members: updatedMembers,
		updatedAt: Date.now(),
	}, { merge: true });
};

// ==== Debug Utilities ====
// These are exposed on window for debugging via browser console

/**
 * Force a full refresh from cloud, ignoring version comparison.
 * Call from browser console: window.cloudSyncDebug.forceFullRefresh()
 */
export const forceFullRefresh = async (): Promise<void> => {
	console.log("[CloudSync] DEBUG: Force full refresh triggered from console");
	await refreshFromCloud(true);
};

/**
 * Clear local device versions to force a full refresh on next sync.
 * Call from browser console: window.cloudSyncDebug.clearLocalVersions()
 */
export const clearLocalVersions = (): void => {
	if (!currentCloudId) {
		console.error("[CloudSync] No cloud league connected");
		return;
	}
	const key = `cloudStoreVersions_${currentCloudId}`;
	localStorage.removeItem(key);
	console.log(`[CloudSync] DEBUG: Cleared local versions for ${currentCloudId}. Reload page to trigger refresh notification.`);
};

/**
 * Get current sync debug info
 * Call from browser console: window.cloudSyncDebug.getInfo()
 */
export const getDebugInfo = (): object => {
	const deviceVersions = currentCloudId ? getDeviceStoreVersions(currentCloudId) : {};
	return {
		currentCloudId,
		syncStatus,
		lastKnownUpdateTime,
		deviceVersions,
		currentCloudMember,
	};
};

// Expose debug utilities on window for console access
if (typeof window !== "undefined") {
	(window as any).cloudSyncDebug = {
		forceFullRefresh,
		clearLocalVersions,
		getInfo: getDebugInfo,
		refreshFromCloud,
	};
}
