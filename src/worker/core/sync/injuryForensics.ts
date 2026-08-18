// A durable record of every injury change on THIS DEVICE, with its source.
//
// Why it exists: twice now the league has produced a player whose injury
// countdown lost days it should not have - a 2 -> 0 jump the first time, a
// 4 -> 0 jump with the player suiting up the second - and both times the
// evidence was gone by the time anyone looked. The in-memory sync log had
// scrolled past it, the box-score stamps only show the aftermath, and every
// candidate mechanism (a doubled day countdown, a stale row published by a
// diverged device, a repair push, a checkpoint restore) writes the exact same
// field. Guessing between them from a screenshot has failed twice.
//
// So, same treatment the phantom phase advance got (phaseForensics.ts): every
// write to a player's injury is recorded durably, per device, with what wrote
// it. The next time a countdown skips, the log on the device that simmed the
// day names the write that made him healthy - a day tick (with the day
// number), a remote changeset (with its version, author action and whether it
// arrived live or in a replay), a checkpoint restore, or a God Mode edit.
// Cross-checking two devices' logs then shows where they diverged.
//
// Entries live in the meta database - per device, never synced, surviving
// reloads - capped as a ring. Sources are recorded compactly: a day tick is
// ONE entry for the whole league ("d41 p392:4>3 p551:1>0!"), not one per
// player, so the ring holds weeks of history.

import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { getSyncEngine } from "./engineHolder.ts";

export type InjuryForensicsEntry = {
	at: number;
	season: number | undefined;
	phase: number | undefined;
	source:
		| "day-tick"
		| "day-tick-skipped"
		| "new-injury"
		| "apply"
		| "checkpoint"
		| "edit";
	detail: string;
	// A live remote apply that wiped a multi-game injury to healthy - the
	// signature of the field incidents. Formatted with a marker so it jumps
	// out of a pasted report.
	suspicious?: boolean;
};

const KEY = "injuryForensics";

// Weeks of ordinary play (one day-tick entry per day, a few injuries and
// heals), small enough that the meta row stays trivial.
export const MAX_INJURY_FORENSICS = 250;

export const appendInjuryForensics = (
	prior: InjuryForensicsEntry[] | undefined,
	entry: InjuryForensicsEntry,
	max: number = MAX_INJURY_FORENSICS,
): InjuryForensicsEntry[] => {
	const next = [...(Array.isArray(prior) ? prior : []), entry];
	return next.length > max ? next.slice(next.length - max) : next;
};

type InjurySnapshot = { type?: string; gamesRemaining?: number } | undefined;

const days = (injury: InjurySnapshot): number =>
	typeof injury?.gamesRemaining === "number" ? injury.gamesRemaining : 0;

export const injuriesDiffer = (
	before: InjurySnapshot,
	after: InjurySnapshot,
): boolean =>
	days(before) !== days(after) || (before?.type ?? "") !== (after?.type ?? "");

// The signature of both field incidents: a player several games from healthy,
// made healthy (or nearly) in one write that is NOT the ordered replay of
// several days. One day of ordinary catch-up moves the counter by one; a
// live apply that moves it by more skipped days that never happened here.
export const suspiciousInjuryApply = (
	before: InjurySnapshot,
	after: InjurySnapshot,
	resync: boolean,
): boolean => !resync && days(before) - days(after) > 1;

export const formatInjuryForensics = (
	entries: InjuryForensicsEntry[] | undefined,
): string => {
	if (!Array.isArray(entries) || entries.length === 0) {
		return "injury changes (this device, durable): none recorded";
	}
	const lines = entries.map(
		(e) =>
			`  ${new Date(e.at).toISOString()} s${e.season ?? "?"}ph${e.phase ?? "?"} ${
				e.suspicious ? "SUSPICIOUS " : ""
			}${e.source}: ${e.detail}`,
	);
	return [
		`injury changes (this device, durable, last ${entries.length}):`,
		...lines,
	].join("\n");
};

export const recordInjuryForensics = async (
	entry: Omit<InjuryForensicsEntry, "at" | "season" | "phase">,
): Promise<void> => {
	// Forensics must never be the reason a sim or an apply fails.
	try {
		const full: InjuryForensicsEntry = {
			at: Date.now(),
			season: (() => {
				try {
					return g.get("season");
				} catch {
					return undefined;
				}
			})(),
			phase: (() => {
				try {
					return g.get("phase");
				} catch {
					return undefined;
				}
			})(),
			...entry,
		};
		const prior = (await idb.meta.get("attributes", KEY)) as
			| InjuryForensicsEntry[]
			| undefined;
		await idb.meta.put("attributes", appendInjuryForensics(prior, full), KEY);
	} catch {
		// Best effort only.
	}
};

export const getInjuryForensics = async (): Promise<
	InjuryForensicsEntry[] | undefined
> => {
	try {
		return (await idb.meta.get("attributes", KEY)) as
			| InjuryForensicsEntry[]
			| undefined;
	} catch {
		return undefined;
	}
};

// The remote-apply probe, shared by the v1 and v2 apply paths: called with an
// incoming player row BEFORE it overwrites the local one. Reads the local row
// from the cache; if the injury field differs, records who changed it to what
// under which version. Quiet for the overwhelmingly common case (stats-only
// row churn with the injury untouched).
export const noteInjuryApply = async (
	incoming: any,
	context: { action?: string; version?: number },
): Promise<void> => {
	try {
		const pid = incoming?.pid;
		if (typeof pid !== "number") {
			return;
		}
		const local = await idb.cache.players.get(pid);
		if (!local) {
			return;
		}
		const before = local.injury as InjurySnapshot;
		const after = incoming.injury as InjurySnapshot;
		if (!injuriesDiffer(before, after)) {
			return;
		}

		const engine = getSyncEngine();
		const resync = engine?.isResyncing() ?? false;
		const suspicious = suspiciousInjuryApply(before, after, resync);

		// During a full ordered replay, hundreds of legitimate injury
		// transitions stream past. Only the wipes are worth the ring space.
		if (resync && !suspicious && days(before) - days(after) <= 1) {
			return;
		}

		const name = `${local.firstName ?? ""} ${local.lastName ?? ""}`.trim();
		const detail = `p${pid} ${name} ${days(before)}(${before?.type ?? "Healthy"}) > ${days(
			after,
		)}(${after?.type ?? "Healthy"}) via=${context.action ?? "?"} v=${
			context.version ?? "?"
		}${resync ? " resync" : ""}`;

		if (suspicious) {
			console.error(`[injury-forensics] SUSPICIOUS apply: ${detail}`);
		}

		await recordInjuryForensics({
			source: "apply",
			detail,
			...(suspicious ? { suspicious: true } : {}),
		});
	} catch {
		// Best effort only.
	}
};
