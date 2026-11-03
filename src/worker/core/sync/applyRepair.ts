// A hand-built repair for a shared league, applied as ONE ordinary change.
//
// Some damage the sync engine cannot undo by itself: a version that was valid
// by every rule the chain enforces, built on a world that was wrong. The
// 2022 preseason in one league is the case behind this - a second, stale copy
// of the game on a league-mate's phone re-ran the last day of free agency and
// the season rollover, and its version rewrote every player in the room. The
// history needed to put it right was all still in the room; what was missing
// was a way to send the corrected records back up.
//
// The file is worked out from the room's history, off the device, and names
// the exact version it was made against. It is refused on any other: a league
// that has moved on since would have newer edits that the file knows nothing
// about, and whole-record puts would quietly take them back.

import { idb } from "../../db/index.ts";
import { toUI } from "../../util/index.ts";
import { recomputeLocalUITeamOvrs } from "../../util/recomputeLocalUITeamOvrs.ts";
import type { Changeset, SyncChange } from "./changeset.ts";
import { deserializeChangeset } from "./serialize.ts";
import { getSyncEngine } from "./engineHolder.ts";
import { syncDebugLog } from "./debugLog.ts";
import { SyncEngineV2 } from "./v2/engine.ts";
import {
	readAppliedVersion,
	reapplyOwnChangesLocally,
} from "./v2/applyVersion.ts";

export const REPAIR_FILE_KIND = "zengm-sync-repair";

// Only the stores a repair has any business rewriting. Everything else - the
// schedule, games, draft picks, device-local stores - is refused outright.
const REPAIRABLE_STORES = new Set([
	"players",
	"teams",
	"teamSeasons",
	"teamStats",
	"events",
	"faDayResults",
]);

export type RepairFile = {
	kind: typeof REPAIR_FILE_KIND;
	room: string;
	expectedVersion: number;
	label?: string;
	changes: SyncChange[];
};

// Pure, so every refusal is a test.
export const checkRepairFile = (
	file: unknown,
	here: {
		room: string | undefined;
		applied: number;
		caughtUp: boolean;
		inCharge: boolean;
	},
): { ok: true; file: RepairFile } | { ok: false; reason: string } => {
	const f = file as Partial<RepairFile> | undefined;
	if (!f || f.kind !== REPAIR_FILE_KIND || !Array.isArray(f.changes)) {
		return { ok: false, reason: "That isn't a repair file." };
	}
	if (here.room === undefined || f.room !== here.room) {
		return {
			ok: false,
			reason: `This repair is for the ${f.room ?? "?"} room.`,
		};
	}
	if (!here.inCharge) {
		return {
			ok: false,
			reason: "Only the device in charge of simming can apply a repair.",
		};
	}
	if (!here.caughtUp || here.applied !== f.expectedVersion) {
		return {
			ok: false,
			reason: `The league is at version ${here.applied}, and this repair was made for version ${f.expectedVersion}. Ask for a new one.`,
		};
	}
	for (const change of f.changes) {
		if (!REPAIRABLE_STORES.has(String(change?.store))) {
			return {
				ok: false,
				reason: `A repair can't change ${String(change?.store)}.`,
			};
		}
		if (change.type !== "put" && change.type !== "delete") {
			return { ok: false, reason: "That isn't a repair file." };
		}
		if (
			change.type === "put" &&
			(change.value === undefined || change.value === null)
		) {
			return { ok: false, reason: "That isn't a repair file." };
		}
	}
	return { ok: true, file: f as RepairFile };
};

export const applySyncRepair = async (
	text: string,
): Promise<{ records: number; outcome: "confirmed" | "queued" }> => {
	const engine = getSyncEngine();
	if (!engine || !(engine instanceof SyncEngineV2)) {
		throw new Error("Not connected to a shared league.");
	}

	let parsed: unknown;
	try {
		parsed = deserializeChangeset(text);
	} catch {
		throw new Error("That isn't a repair file.");
	}

	// Nothing of anyone's still on its way up, so the version this device has
	// is the room's.
	await engine.waitUntilIdle(30_000);

	// The room this league file is bound to (connect.ts writes it on joining).
	let room: string | undefined;
	try {
		const row = await (idb.league as any).get("gameAttributes", "syncV2Room");
		room = typeof row?.value === "string" ? row.value : undefined;
	} catch {
		room = undefined;
	}
	const checked = checkRepairFile(parsed, {
		room,
		applied: await readAppliedVersion(),
		caughtUp: engine.isCaughtUp(),
		inCharge: engine.isAuthority(),
	});
	if (!checked.ok) {
		throw new Error(checked.reason);
	}
	const { file } = checked;

	// Here first, exactly as every other device will apply it - disk and
	// memory, not captured as a change of its own - then up as one version.
	// Not a timeline advance: if somebody's edit beats it to the room, it goes
	// on top, like any other edit.
	await reapplyOwnChangesLocally(file.changes);
	const changeset: Changeset = { changes: file.changes };
	syncDebugLog("v2:repair-applying", {
		expectedVersion: file.expectedVersion,
		records: file.changes.length,
		label: file.label,
	});
	const outcome = await engine.onLocalChangeset(
		changeset,
		"main.applySyncRepair",
		[],
	);
	syncDebugLog("v2:repair-applied", { outcome, records: file.changes.length });

	await toUI("realtimeUpdate", [
		["playerMovement", "team", "teamFinances", "gameAttributes"],
	]);

	// Team ratings in the header, from the players as they now are.
	try {
		await recomputeLocalUITeamOvrs();
	} catch {
		// Cosmetic; the next view refresh does it.
	}

	return { records: file.changes.length, outcome };
};
