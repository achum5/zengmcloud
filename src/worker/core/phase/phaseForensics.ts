// A durable record of every phase change THIS DEVICE executed locally.
//
// Why it exists: a league-mate's phone showed "2007 free agency" while the
// room sat at the start of the 2007 draft. By the time a sync capture was
// taken, the device was already mid-heal (an ordered replay walking room
// history back over the divergence) and the in-memory debug log had long
// scrolled past whatever ran the phase forward. The room was never
// contaminated - the version chain refuses a stale advance - but the ORIGIN
// was unrecoverable, because every trace of it lived in memory.
//
// So the trace is durable now. newPhase() is the one choke point every local
// phase change goes through - a Play-menu click, a chained multi-phase
// advance like untilFreeAgency, a phase flip riding on a sim's completion -
// while REMOTE phase changes (applied from a league-mate's changeset) write
// game attributes directly and never call it. An entry here therefore means
// exactly "this device ran this transition itself", with the API action it
// ran under and what the device believed about its sync state at that moment.
// That is precisely the set of facts the field incident needed and did not
// have.

import type { Phase } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { getCurrentAction } from "../../util/actionContext.ts";
import { getSyncEngine } from "../sync/engineHolder.ts";

export type PhaseForensicsEntry = {
	at: number;
	lid: number | undefined;
	season: number;
	from: number;
	to: number;
	// The API action this transition ran under ("playMenu.untilFreeAgency"),
	// or undefined for something outside the dispatcher (startup, a hook).
	source: string | undefined;
	// What the device believed at that moment. engine=false with sync=false on
	// a league that IS in a room is the smoking gun for "the guard was absent".
	engine: boolean;
	authority: boolean;
};

const KEY = "phaseForensics";

// Enough history to cover an incident plus the healing that follows it,
// small enough that the meta row stays trivial.
export const MAX_PHASE_FORENSICS = 40;

// Pure so the ring behavior is testable without IDB.
export const appendPhaseForensics = (
	prior: PhaseForensicsEntry[] | undefined,
	entry: PhaseForensicsEntry,
	max: number = MAX_PHASE_FORENSICS,
): PhaseForensicsEntry[] => {
	const next = [...(Array.isArray(prior) ? prior : []), entry];
	return next.length > max ? next.slice(next.length - max) : next;
};

export const formatPhaseForensics = (
	entries: PhaseForensicsEntry[] | undefined,
): string => {
	if (!Array.isArray(entries) || entries.length === 0) {
		return "phase changes (this device, durable): none recorded";
	}
	const lines = entries.map(
		(e) =>
			`  ${new Date(e.at).toISOString()} lid=${e.lid ?? "?"} season=${
				e.season
			} phase ${e.from}->${e.to} source=${e.source ?? "?"} engine=${
				e.engine
			} authority=${e.authority}`,
	);
	return [
		`phase changes (this device, durable, last ${entries.length}):`,
		...lines,
	].join("\n");
};

export const recordPhaseForensics = async (to: Phase): Promise<void> => {
	// Forensics must never be the reason a phase change fails.
	try {
		const engine = getSyncEngine();
		const entry: PhaseForensicsEntry = {
			at: Date.now(),
			lid: (() => {
				try {
					const lid = g.get("lid");
					return typeof lid === "number" ? lid : undefined;
				} catch {
					return undefined;
				}
			})(),
			season: g.get("season"),
			from: g.get("phase"),
			to,
			source: getCurrentAction(),
			engine: engine !== undefined,
			authority: engine?.isAuthority() ?? false,
		};
		const prior = (await idb.meta.get("attributes", KEY)) as
			| PhaseForensicsEntry[]
			| undefined;
		await idb.meta.put("attributes", appendPhaseForensics(prior, entry), KEY);
	} catch {
		// Best effort only.
	}
};

export const getPhaseForensics = async (): Promise<
	PhaseForensicsEntry[] | undefined
> => {
	try {
		return (await idb.meta.get("attributes", KEY)) as
			| PhaseForensicsEntry[]
			| undefined;
	} catch {
		return undefined;
	}
};
