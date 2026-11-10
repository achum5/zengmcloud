import { PHASE } from "../../../common/constants.ts";
import { gameAttributesArrayToObject } from "../../../common/gameAttributesArrayToObject.ts";
import { unwrapGameAttribute } from "../../../common/unwrapGameAttribute.ts";
import type { FlatContractInfo, Phase } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import {
	addLegacyEvent,
	addLegacyReleasedPlayer,
	addReleasesFromCurrentRecord,
	applyLegacyReleasesToCurrentRecord,
	createLegacyContractInfoByPid,
	getFractionCurrentSeasonPaid,
	isLegacyPlayerRecord,
	type LeagueStateForUpgrade,
	removeStrayLegacyFields,
	upgradeLegacyPlayer,
} from "../../db/migrations/81/upgradeLegacyPlayers.ts";
import { g } from "../../util/index.ts";
import { getContractSummary } from "../contract/index.ts";
import type { SyncChange } from "./changeset.ts";

// ---------------------------------------------------------------------------
// OLD-SHAPE RECORDS FROM THE ROOM.
//
// League database version 81 replaced p.contract and p.salaries with
// p.contracts and p.contractDemand, and folded the releasedPlayers store into
// the players' own contracts (endedEarly + p.owedByTids). Upgrading converts
// what is on this device - but sync keeps delivering records written by other
// code: changes a league mate published before updating (which a device
// catching up replays in order), changes from a league mate still on an older
// build, and checkpoints taken before the upgrade. Nothing in the room says
// which build wrote a record, so the record's own shape decides.
//
// Everything here converts on the way IN, before a write reaches the
// database, the same way the database upgrade would have if the record had
// been on this device then. A releasedPlayers row no longer has anywhere to
// go: its release is recorded on the player's contract instead, and its
// deletes (the old build clearing contracts that were paid off) are dropped,
// since current code clears those itself (clearPaidReleasedContracts).
//
// The other direction can't be fixed from here: a device still on an older
// build that receives a player in the new shape can't read it. Once that
// device updates, its own upgrade leaves those records alone (see migrate81).
// ---------------------------------------------------------------------------

const LEGACY_RELEASED_PLAYERS = "releasedPlayers";

// amount/exp of a player record of either shape: what he's paid now, or what
// he's asking for. Undefined for a retired player, or a record with neither.
export const contractSummaryOfRecord = (
	p: any,
): FlatContractInfo | undefined => {
	if (!p || typeof p !== "object") {
		return undefined;
	}
	if (Array.isArray(p.contracts)) {
		return getContractSummary(p);
	}
	const contract = p.contract;
	if (
		contract &&
		typeof contract.amount === "number" &&
		typeof contract.exp === "number"
	) {
		const summary: FlatContractInfo = {
			amount: contract.amount,
			exp: contract.exp,
		};
		if (contract.rookie) {
			summary.rookie = true;
		}
		return summary;
	}
	return undefined;
};

const hasLegacyFields = (p: any) =>
	!!p && typeof p === "object" && ("contract" in p || "salaries" in p);

export const hasLegacyContractRecords = (changes: SyncChange[]) => {
	return changes.some(
		(change) =>
			(change.store as string) === LEGACY_RELEASED_PLAYERS ||
			(change.store === "players" &&
				change.type === "put" &&
				hasLegacyFields(change.value)),
	);
};

// Where the league is, for the conversion: this changeset's own season and
// phase if it moves them (a phase change carries the new values), otherwise
// this device's.
const getLeagueState = async (
	changes: SyncChange[],
): Promise<LeagueStateForUpgrade> => {
	let season: number = g.get("season");
	let phase: Phase = g.get("phase");
	for (const change of changes) {
		if (change.store === "gameAttributes" && change.type === "put") {
			const row = change.value as { key?: string; value?: unknown };
			if (row?.key === "season" && typeof row.value === "number") {
				season = row.value;
			} else if (row?.key === "phase" && typeof row.value === "number") {
				phase = row.value as Phase;
			}
		}
	}

	let fractionCurrentSeasonPaid = 0;
	if (phase >= PHASE.REGULAR_SEASON && phase <= PHASE.PLAYOFFS) {
		try {
			const teamSeasons = await idb.cache.teamSeasons.indexGetAll(
				"teamSeasonsBySeasonTid",
				[[season], [season, Infinity]],
			);
			fractionCurrentSeasonPaid = getFractionCurrentSeasonPaid(
				teamSeasons,
				g.get("numGames"),
			);
		} catch {
			// Only affects which seasons are recorded as paid
		}
	}

	return {
		fractionCurrentSeasonPaid,
		phase,
		season,
	};
};

// This device's current copy of a player. The cache holds every player who
// isn't retired, and retired players who are still owed money.
const getCurrentRecord = async (pid: number) => {
	try {
		const p = await idb.cache.players.get(pid);
		if (p) {
			return p;
		}
	} catch {
		// Cache not ready, fall back to the database
	}
	try {
		return await (idb.league as any).get("players", pid);
	} catch {
		return undefined;
	}
};

// Convert the old-shape player records and releasedPlayers rows in a list of
// synced changes to what this build stores. Returns the same array when there
// is nothing to convert, so the common case costs one scan.
export const upgradeLegacyChanges = async (
	changes: SyncChange[],
): Promise<SyncChange[]> => {
	if (!hasLegacyContractRecords(changes)) {
		return changes;
	}

	const leagueState = await getLeagueState(changes);

	// Releases and signings in this same change, which describe the contracts
	// on these records
	const info = createLegacyContractInfoByPid();
	for (const change of changes) {
		if (change.type !== "put") {
			continue;
		}
		if ((change.store as string) === LEGACY_RELEASED_PLAYERS) {
			addLegacyReleasedPlayer(info, change.value);
		} else if (change.store === "events") {
			addLegacyEvent(info, change.value);
		}
	}

	const playerPutPids = new Set<number>();
	for (const change of changes) {
		if (change.store === "players" && change.type === "put") {
			const pid = (change.value as any)?.pid;
			if (typeof pid === "number") {
				playerPutPids.add(pid);
			}
		}
	}

	const out: SyncChange[] = [];
	for (const change of changes) {
		if ((change.store as string) === LEGACY_RELEASED_PLAYERS) {
			continue;
		}

		if (
			change.store === "players" &&
			change.type === "put" &&
			hasLegacyFields(change.value)
		) {
			const p = structuredClone(change.value);
			if (isLegacyPlayerRecord(p)) {
				addReleasesFromCurrentRecord(info, await getCurrentRecord(p.pid));
				upgradeLegacyPlayer(p, leagueState, info);
			} else {
				// Already the current shape, with old fields an older build added
				// back
				removeStrayLegacyFields(p);
			}
			out.push({ ...change, value: p });
			continue;
		}

		out.push(change);
	}

	// A release whose player record isn't in this change: record it on this
	// device's copy of him
	for (const [pid, releasedPlayers] of info.releasedPlayersByPid) {
		if (playerPutPids.has(pid)) {
			continue;
		}
		const current = await getCurrentRecord(pid);
		if (!current || !Array.isArray(current.contracts)) {
			continue;
		}
		const p = structuredClone(current);
		if (
			applyLegacyReleasesToCurrentRecord(
				p,
				leagueState,
				releasedPlayers,
				info.releaseEventsByPid.get(pid) ?? [],
			)
		) {
			out.push({ store: "players", id: pid, type: "put", value: p });
		}
	}

	return out;
};

// A checkpoint's stores, from a device that hadn't upgraded yet. Converted in
// place the same way the database upgrade converts a league, with the
// checkpoint's own releasedPlayers rows and events.
export const upgradeLegacySnapshotStores = (
	stores: Record<string, unknown[]>,
) => {
	const players = stores.players;
	const releasedPlayers = stores[LEGACY_RELEASED_PLAYERS];
	delete stores[LEGACY_RELEASED_PLAYERS];
	if (!Array.isArray(players)) {
		return;
	}
	if (!players.some(isLegacyPlayerRecord)) {
		for (const p of players) {
			removeStrayLegacyFields(p);
		}
		return;
	}

	const gameAttributes = gameAttributesArrayToObject(
		((stores.gameAttributes ?? []) as any[]).filter(
			(row) => row && typeof row.key === "string",
		),
	);
	const season = unwrapGameAttribute(gameAttributes, "season");
	const phase = unwrapGameAttribute(gameAttributes, "phase");
	if (typeof season !== "number" || typeof phase !== "number") {
		throw new Error(
			"Can't convert the checkpoint's contracts: it has no season or phase",
		);
	}
	const numGames = unwrapGameAttribute(gameAttributes, "numGames");
	const teamSeasons = ((stores.teamSeasons ?? []) as any[]).filter(
		(ts) => ts?.season === season,
	);
	const leagueState: LeagueStateForUpgrade = {
		fractionCurrentSeasonPaid: getFractionCurrentSeasonPaid(
			teamSeasons,
			typeof numGames === "number" ? numGames : 0,
		),
		phase,
		season,
	};

	const info = createLegacyContractInfoByPid();
	for (const rp of releasedPlayers ?? []) {
		addLegacyReleasedPlayer(info, rp);
	}
	for (const event of stores.events ?? []) {
		addLegacyEvent(info, event);
	}

	for (const p of players as any[]) {
		if (isLegacyPlayerRecord(p)) {
			upgradeLegacyPlayer(p, leagueState, info);
		} else if (p && !Array.isArray(p.contracts)) {
			p.contracts = [];
		} else {
			removeStrayLegacyFields(p);
		}
	}
};
