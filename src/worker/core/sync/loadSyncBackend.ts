import type { SyncRoom } from "./adminRooms.ts";

// The Firebase SDK is 428 KB of a 2,517 KB worker bundle - firestore, auth and
// the webchannel wrapper - and a league that is never shared downloads and
// parses every byte of it for nothing. This one dynamic import is what stops
// that: the SDK and everything that touches it live in their own chunk, fetched
// the first time a device connects to a room and never otherwise.
//
// Deliberately one import, not several: the whole firebase side is one chunk, so
// a connect pays a single round trip rather than a waterfall. The module
// registry memoizes it, so later calls are free.
export const loadSyncBackend = () => import("./firebaseLazy.ts");

// Room administration. Thin wrappers rather than re-exports, so a caller can
// reach these without the eager bundle naming the Firebase side at all.
export const listSyncRooms = async (): Promise<SyncRoom[]> =>
	(await loadSyncBackend()).listSyncRooms();

export const deleteSyncRoom = async (code: string) =>
	(await loadSyncBackend()).deleteSyncRoom(code);

export const deleteAllSyncRooms = async () =>
	(await loadSyncBackend()).deleteAllSyncRooms();

export const pruneSyncRoomChanges = async (
	code: string,
	olderThanDays: number,
) => (await loadSyncBackend()).pruneSyncRoomChanges(code, olderThanDays);

export const pruneAllSyncRoomChanges = async (olderThanDays: number) =>
	(await loadSyncBackend()).pruneAllSyncRoomChanges(olderThanDays);
