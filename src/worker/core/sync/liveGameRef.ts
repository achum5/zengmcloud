// What a live game doc may say about where its payload is, before anything
// builds doc ids from it or loops over it. Every live game doc is written by
// some device in the room, and deletes under control/ are open to every
// member (see firestore.rules), so nothing read from one is trusted to name a
// doc other than that game's own, or to send a read or delete loop on for
// ever.

// The most payload chunks a game can have (one is ~700 KB; a game is one or
// two).
export const MAX_LIVE_GAME_CHUNKS = 50;

export const validLiveGameRef = (gid: unknown, chunkCount: unknown): boolean =>
	typeof gid === "number" &&
	Number.isSafeInteger(gid) &&
	typeof chunkCount === "number" &&
	Number.isSafeInteger(chunkCount) &&
	chunkCount >= 1 &&
	chunkCount <= MAX_LIVE_GAME_CHUNKS;
