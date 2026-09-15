// THE FIREBASE SIDE OF SYNC, and the only module the dynamic import in
// loadSyncBackend.ts names.
//
// Everything re-exported here transitively imports the Firebase SDK, so the
// bundler puts this whole graph in its own chunk. Nothing in the eagerly-loaded
// worker may import this file (or anything it re-exports) directly - a single
// static import would pull the SDK back into the main bundle and silently undo
// the split. The test that guards it lives in loadSyncBackend.test.ts.
export { FirebaseTransport } from "./FirebaseTransport.ts";
export { ensureAnonymousAuth } from "./auth.ts";
export {
	listSyncRooms,
	deleteSyncRoom,
	deleteAllSyncRooms,
	pruneSyncRoomChanges,
	pruneAllSyncRoomChanges,
} from "./adminRooms.ts";
export type { SyncRoom } from "./adminRooms.ts";
