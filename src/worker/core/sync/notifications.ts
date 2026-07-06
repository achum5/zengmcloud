import { PHASE, PHASE_TEXT } from "../../../common/constants.ts";
import { g } from "../../util/index.ts";
import type { Changeset } from "./changeset.ts";

// A push notification to fan out to the OTHER devices in the league room. The
// acting device (the one whose app is open, that just made the change) writes
// this to Firestore; a Cloud Function delivers it to everyone else's phones.
export type SyncNotification = {
	title: string;
	body: string;
	// Which teams this is relevant to (their managing devices get pinged). null
	// means everyone in the room. v1 uses null for everything - see the Cloud
	// Function, which already supports per-team targeting for when we want it.
	targetTids: number[] | null;
};

// Phases that need a human to act (draft, re-signing, etc.) - the inverse of the
// phases the auto-play scheduler can advance on its own. Reaching one of these
// is the "it's your turn" signal.
const HUMAN_PHASES = new Set<number>([
	PHASE.DRAFT_LOTTERY,
	PHASE.DRAFT,
	PHASE.AFTER_DRAFT,
	PHASE.RESIGN_PLAYERS,
	PHASE.EXPANSION_DRAFT,
	PHASE.FANTASY_DRAFT,
]);

const phaseText = (phase: number): string =>
	(PHASE_TEXT as Record<string, string>)[String(phase)] ?? "a new phase";

// If this changeset advanced the game phase, return the new phase number.
const newPhaseFromChangeset = (changeset: Changeset): number | undefined => {
	for (const change of changeset.changes) {
		if (
			change.store === "gameAttributes" &&
			change.id === "phase" &&
			change.type === "put"
		) {
			const value = (change.value as { value?: unknown })?.value;
			if (typeof value === "number") {
				return value;
			}
		}
	}
	return undefined;
};

// Distinct real teams (tid >= 0) that received a player in this changeset. Two
// or more usually means a trade; one means a signing/claim.
const receivingTeams = (changeset: Changeset): number[] => {
	const tids = new Set<number>();
	for (const change of changeset.changes) {
		if (change.store === "players" && change.type === "put") {
			const tid = (change.value as { tid?: unknown })?.tid;
			if (typeof tid === "number" && tid >= 0) {
				tids.add(tid);
			}
		}
	}
	return [...tids];
};

// True if the changeset touches roster state at all (signings, cuts, trades,
// draft picks). Deliberately broad - for a friend group, an occasional extra
// ping is better than a missed move.
const isRosterChange = (changeset: Changeset): boolean =>
	changeset.changes.some(
		(change) =>
			change.store === "players" ||
			change.store === "releasedPlayers" ||
			change.store === "draftPicks",
	);

// Turn a locally-produced changeset into a push notification for everyone else,
// or undefined if it isn't worth a ping. Called on the device that made the
// change (its app is open), so `g` already reflects the post-action state.
//
// Only the host announces sims (everyone else shouldn't be simming); a sim that
// lands on a human-decision phase is announced as "your turn" rather than a
// generic "sim complete".
export const buildNotification = (
	label: string,
	changeset: Changeset,
	{ isHost, authorName }: { isHost: boolean; authorName: string },
): SyncNotification | undefined => {
	const isSim = label.startsWith("playMenu.");
	const newPhase = newPhaseFromChangeset(changeset);
	const enteredHumanPhase = newPhase !== undefined && HUMAN_PHASES.has(newPhase);

	if (isSim) {
		// Non-host devices shouldn't be simming; if they somehow do, stay quiet so
		// the room doesn't get duplicate sim announcements.
		if (!isHost) {
			return undefined;
		}
		if (enteredHumanPhase) {
			return {
				title: "Your league needs you",
				body: `The host reached ${phaseText(newPhase!)} — your input is needed.`,
				targetTids: null,
			};
		}
		return {
			title: "Sim complete",
			body: `The host advanced the league (${phaseText(g.get("phase"))}).`,
			targetTids: null,
		};
	}

	// A manual phase advance (not via a sim) that reaches a human-decision phase.
	if (enteredHumanPhase) {
		return {
			title: "Your league needs you",
			body: `New phase: ${phaseText(newPhase!)}.`,
			targetTids: null,
		};
	}

	const teams = receivingTeams(changeset);
	if (teams.length >= 2) {
		return {
			title: "Trade completed",
			body: `${authorName} completed a trade.`,
			targetTids: null,
		};
	}

	if (isRosterChange(changeset)) {
		return {
			title: "Roster move",
			body: `${authorName} made a roster move.`,
			targetTids: null,
		};
	}

	return undefined;
};
