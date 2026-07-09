import { idb } from "../../db/index.ts";
import { PLAYER } from "../../../common/constants.ts";
import type { Player } from "../../../common/types.ts";

const hasRelativeAndMutate = (p: Player, pids: number[]) => {
	if (!p.relatives) {
		return false;
	}

	const has = p.relatives.some((relative) => pids.includes(relative.pid));
	if (has) {
		p.relatives = p.relatives.filter(
			(relative) => !pids.includes(relative.pid),
		);
	}
	return has;
};

const remove = async (pids: number[]) => {
	if (pids.length === 0) {
		return;
	}

	for (const pid of pids) {
		await idb.cache.players.delete(pid);
	}

	// Also remove any relatives
	const players = await idb.cache.players.getAll();
	for (const p of players) {
		if (pids.includes(p.pid)) {
			continue;
		}

		if (hasRelativeAndMutate(p, pids)) {
			await idb.cache.players.put(p);
		}
	}
	// Retired players mostly live only on disk, not in the cache. Collect the
	// ones to fix with a read-only cursor, then write them back THROUGH the
	// cache - a raw cursor.update would bypass the sync change tracker, so the
	// relatives trim would apply on this device only and never reach the room.
	const retiredToFix: Player[] = [];
	for await (const cursor of idb.league
		.transaction("players")
		.store.index("tid")
		.iterate(PLAYER.RETIRED)) {
		const p = cursor.value;
		if (pids.includes(p.pid)) {
			continue;
		}

		// Skip anything the cache holds - the cache copy may be newer than disk
		// and was already handled by the loop above.
		if (
			hasRelativeAndMutate(p, pids) &&
			(await idb.cache.players.get(p.pid)) === undefined
		) {
			retiredToFix.push(p);
		}
	}
	for (const p of retiredToFix) {
		await idb.cache.players.put(p);
	}
};

export default remove;
