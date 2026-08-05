import { idb } from "../../db/index.ts";

// THE permanent identity of a league lineage, minted the first time a league
// connects to a room and never changed afterwards. It lives in gameAttributes
// - deliberately NOT device-local - so it travels inside every checkpoint,
// snapshot and export of the league. That is the whole point: data carries its
// own provenance, so a room poisoned with some other league's state can never
// overwrite a league again - the payload's identity gives it away no matter
// how the wrong data got into the room (a zombie engine, a second tab, an old
// build without publish guards, a reused room code).
//
// Copies and clones of a league share the identity. That is correct: syncing a
// copy into its own lineage's room is a divergence problem the version chain
// handles, not cross-league contamination.

export const SYNC_LEAGUE_ID_KEY = "syncLeagueId";

export const generateLeagueId = (): string =>
	typeof crypto !== "undefined" && crypto.randomUUID
		? crypto.randomUUID()
		: `${Date.now()}-${Math.floor(Math.random() * 1e12)}`;

export const readLocalLeagueId = async (): Promise<string | undefined> => {
	const league = idb.league as any;
	// gameAttributes is a few dozen rows, so the scan fallback (for stores
	// without a keyed get - the in-memory test DBs) costs nothing.
	const row =
		typeof league.get === "function"
			? await league.get("gameAttributes", SYNC_LEAGUE_ID_KEY)
			: (await league.getAll("gameAttributes")).find(
					(r: any) => r?.key === SYNC_LEAGUE_ID_KEY,
				);
	return typeof row?.value === "string" && row.value !== ""
		? row.value
		: undefined;
};

export const writeLocalLeagueId = async (id: string): Promise<void> => {
	await (idb.league as any).put("gameAttributes", {
		key: SYNC_LEAGUE_ID_KEY,
		value: id,
	});
};

export type LeagueIdentityOutcome =
	| { action: "minted"; id: string }
	| { action: "adopted"; id: string }
	| { action: "matched"; id: string }
	| { action: "rebound"; id: string; previous: string }
	| { action: "refused"; local: string; room: string };

// THE RULE, in one place because getting it wrong breaks the system in one of
// two opposite ways - locking legitimate players out of their own league, or
// letting a league be overwritten by a room that isn't its own.
//
// The discriminator is intent, expressed as explicit-vs-automatic:
//
//   - An EXPLICIT join (the user typed the room code and pressed Connect) is a
//     statement that this league belongs in this room. A league-mate whose
//     copy minted its own identity, or who is joining a re-created room, must
//     always be able to do this - otherwise the protection permanently locks
//     real players out with no recovery. Their league re-binds to the room.
//   - An AUTOMATIC reconnect (a stale session pointer, a zombie engine, a
//     second tab) carries no such intent. BOTH cross-league contamination
//     incidents were exactly this shape. A mismatch here is refused.
export const resolveLeagueIdentity = async ({
	localId,
	explicit,
	fetchRoomLeagueId,
	claimRoomLeagueId,
}: {
	localId: string | undefined;
	explicit: boolean;
	fetchRoomLeagueId: () => Promise<string | undefined>;
	claimRoomLeagueId: (leagueId: string) => Promise<string>;
}): Promise<LeagueIdentityOutcome> => {
	if (localId === undefined) {
		// No identity yet: adopt the room's, or mint one and claim the room.
		const roomLeagueId = await fetchRoomLeagueId();
		if (roomLeagueId !== undefined) {
			return { action: "adopted", id: roomLeagueId };
		}
		// A lost first-claim race can only be a league-mate claiming the same
		// lineage, so taking theirs is right.
		const bound = await claimRoomLeagueId(generateLeagueId());
		return { action: "minted", id: bound };
	}

	const bound = await claimRoomLeagueId(localId);
	if (bound === localId) {
		return { action: "matched", id: bound };
	}
	if (explicit) {
		return { action: "rebound", id: bound, previous: localId };
	}
	return { action: "refused", local: localId, room: bound };
};

// The identity carried inside a snapshot/checkpoint payload's gameAttributes
// rows, if the publisher's league had one.
export const payloadLeagueId = (
	stores: Record<string, unknown[]> | undefined,
): string | undefined => {
	const rows = stores?.gameAttributes;
	if (!Array.isArray(rows)) {
		return undefined;
	}
	for (const row of rows) {
		if ((row as any)?.key === SYNC_LEAGUE_ID_KEY) {
			const value = (row as any).value;
			return typeof value === "string" && value !== "" ? value : undefined;
		}
	}
	return undefined;
};
