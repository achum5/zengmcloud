import { toWorker } from "./toWorker.ts";
import { local } from "./local.ts";
import { safeLocalStorage } from "./safeLocalStorage.ts";
import { runReconnect, type ReconnectDeps } from "./reconnectLoop.ts";
import { isValidFirebaseConfig } from "../../common/syncInvite.ts";
import type { FirebaseConfig } from "../../common/firebaseConfig.ts";

// The intent to be connected to a shared-league room is persisted per-league in
// localStorage, so a full page refresh (which tears down the worker and its
// in-memory sync engine) can transparently reconnect - including the host role.

export type StoredSync = {
	code: string;
	isHost: boolean;
	// One-shot marker set when the user chose this room by name (e.g. at league
	// creation) but the connect happens later, after navigation. It makes that
	// first connect count as an EXPLICIT join (allowed to bind the league file to
	// the room); cleared once the connect succeeds.
	pendingJoin?: boolean;
	// A bring-your-own-Firestore project for this room, if the user joined via an
	// invite. Absent for ordinary rooms, which use the built-in project. Persisted
	// so an auto-reconnect after a refresh restores the same project.
	firebaseConfig?: FirebaseConfig;
};

const key = (lid: number) => `syncSession-${lid}`;

export const getStoredSync = (lid: number): StoredSync | undefined => {
	const raw = safeLocalStorage.getItem(key(lid));
	if (!raw) {
		return undefined;
	}
	try {
		const parsed = JSON.parse(raw);
		if (typeof parsed?.code === "string" && parsed.code.trim() !== "") {
			return {
				code: parsed.code,
				isHost: !!parsed.isHost,
				pendingJoin: !!parsed.pendingJoin,
				// Only accept a well-formed config; anything else falls back to the
				// default project rather than pointing sync at a broken one.
				...(isValidFirebaseConfig(parsed.firebaseConfig)
					? { firebaseConfig: parsed.firebaseConfig }
					: {}),
			};
		}
	} catch {}
	return undefined;
};

export const setStoredSync = (lid: number, session: StoredSync) => {
	safeLocalStorage.setItem(
		key(lid),
		JSON.stringify({
			code: session.code.trim(),
			isHost: session.isHost,
			...(session.pendingJoin ? { pendingJoin: true } : {}),
			...(session.firebaseConfig
				? { firebaseConfig: session.firebaseConfig }
				: {}),
		}),
	);
};

export const clearStoredSync = (lid: number) => {
	safeLocalStorage.removeItem(key(lid));
};

// A connect the worker never answers must not hold the loop for good either.
const CONNECT_ANSWER_MS = 180_000;

// The league a reconnect loop is running for right now (not "ever tried": a
// loop that ended clears it, so coming back to a league always tries again).
let reconnectingLid: number | undefined;
// Cuts the loop's current wait short.
let wakeRetry: (() => void) | undefined;

const wait = (ms: number) =>
	new Promise<void>((resolve) => {
		const done = () => {
			clearTimeout(timer);
			if (wakeRetry === done) {
				wakeRetry = undefined;
			}
			resolve();
		};
		const timer = setTimeout(done, ms);
		wakeRetry = done;
	});

const withAnswerDeadline = <T>(promise: Promise<T>) =>
	new Promise<T | undefined>((resolve, reject) => {
		const timer = setTimeout(() => {
			resolve(undefined);
		}, CONNECT_ANSWER_MS);
		promise.then(
			(value) => {
				clearTimeout(timer);
				resolve(value);
			},
			(error) => {
				clearTimeout(timer);
				reject(error);
			},
		);
	});

const liveDeps: ReconnectDeps = {
	markSyncRequired: async (session) =>
		await toWorker("main", "markSyncRequired", session),
	getSyncStatus: async () => await toWorker("main", "getSyncStatus", undefined),
	refreshSyncUIState: async () =>
		await toWorker("main", "refreshSyncUIState", undefined),
	connect: async (session) =>
		await withAnswerDeadline(
			(async () =>
				await toWorker("main", "connectSharedLeague", {
					code: session.code,
					isHost: session.isHost,
					// A join the user chose at league creation runs its actual connect
					// here, post-navigation - it still counts as explicit, once.
					explicit: !!session.pendingJoin,
					// Only the FIRST connect can create the room, so the protocol
					// choice rides along with that one-shot join and is dropped after.
					firebaseConfig: session.firebaseConfig,
				}))(),
		),
	currentLid: () => {
		const lid = local.getState().lid;
		return typeof lid === "number" ? lid : undefined;
	},
	stored: getStoredSync,
	setStored: setStoredSync,
	clearStored: clearStoredSync,
	wait,
};

export const autoReconnectSync = async (lid: number) => {
	if (reconnectingLid === lid) {
		// Already on it: if it is waiting to go round again, go now.
		wakeRetry?.();
		return;
	}
	if (!getStoredSync(lid)) {
		return;
	}
	reconnectingLid = lid;
	try {
		await runReconnect(lid, liveDeps);
	} finally {
		if (reconnectingLid === lid) {
			reconnectingLid = undefined;
		}
	}
};

// The app back in front, or back online: a reconnect waiting out its backoff
// goes now, and a league in a room with no reconnect running gets one (when
// it is connected that is one status read and done).
export const nudgeAutoReconnect = () => {
	const lid = local.getState().lid;
	if (typeof lid === "number") {
		void autoReconnectSync(lid);
	}
};
