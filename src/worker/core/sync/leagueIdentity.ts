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
