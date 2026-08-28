// The decision rule for claimDraftAdvance, extracted pure so it's unit-testable
// (the Firestore transaction in FirebaseTransport just applies it).
//
// The claim doc is a per-room mutex AND a monotonic fence. The original
// lease-only design guarded exactly one (stage, step) pair for 90s - which
// stopped two caught-up devices from simming the SAME step, but let a STALE
// device claim any OLD step minutes later without even colliding. That's how a
// device that rejoined ~20 free-agency days behind re-claimed long-finished
// day-steps (everyone's ready-through entries still covered them) and re-simmed
// them, publishing regressed daysLeft/roster state as brand-new history and
// dragging the whole room back to a state days older than where the real sim
// had gotten to.
//
// The fence: within a stage (draftKey), the highest step ever claimed is a
// high-water mark. A step BELOW it is history and can never be claimed again,
// no matter how stale the asker. The newest step can be re-claimed only for
// recovery: its lease expired without completion (the holder crashed
// mid-advance), or its completion has stood for the reclaim grace while the
// room still derives the step (the completion was false - the "advance" never
// moved the world).

export type AdvanceClaimDoc = {
	holderId: string;
	draftKey: string;
	pick: number;
	at: number;
	// Highest step ever claimed within draftKey. Old docs (pre-fence) lack it;
	// their `pick` serves as the mark.
	maxPick?: number;
	// Set by completeDraftAdvance once the claimed step's advance finished, which
	// closes the crash-recovery re-claim window for it.
	completed?: boolean;
};

export type AdvanceClaimDecision =
	| { grant: true; maxPick: number }
	| { grant: false; reason: string };

// How long a COMPLETED newest step stays sealed before it can be re-claimed.
//
// Why a completed step is re-claimable at all: an advance can resolve without
// doing anything - the winner's sim lock was held, its day-claim was refused,
// its stop-crossing permission was consumed by a concurrent single-game sim -
// and marking such a step completed seals it: every device shows the whole
// room ready and nothing ever advances again. A live league wedged exactly
// this way at a day-15 stop. draftReady now verifies the world moved before
// completing, but a seal written by an older client (or any future bug of the
// same shape) must not be a permanent dead end.
//
// Why re-claiming is safe: a device only asks to claim a step it still
// DERIVES from its own world, and the preflight in draftReady catches up to
// the live log head seconds before asking. A truly-run step is published to
// the log BEFORE its completion mark is written, so by the time completed can
// be read, a caught-up device derives the NEXT step and never asks for this
// one again. An ask for a completed step long after the claim is therefore
// evidence the completion was false. The grace covers the near window - an
// ask whose catch-up raced the publish, plus device clock skew.
export const COMPLETED_RECLAIM_GRACE_MS = 5 * 60_000;

export const decideAdvanceClaim = (
	existing: AdvanceClaimDoc | undefined,
	ask: { draftKey: string; pick: number; now: number; leaseMs: number },
): AdvanceClaimDecision => {
	// No claim yet, or a claim from a different stage: the fence resets with the
	// stage (stage keys carry season+phase, so steps only compare within one).
	if (!existing || existing.draftKey !== ask.draftKey) {
		return { grant: true, maxPick: ask.pick };
	}

	const maxPick =
		typeof existing.maxPick === "number" ? existing.maxPick : existing.pick;

	// A step below the high-water mark is already-run history. Only a stale
	// device would ask for it; granting would re-sim finished steps and publish
	// regressed state on top of the room's real history.
	if (ask.pick < maxPick) {
		return { grant: false, reason: "step-already-run" };
	}

	if (ask.pick === maxPick) {
		// The newest claimed step. Completed → it ran to the end; re-running it
		// would duplicate it (a caught-up device would be asking for the NEXT
		// step, so this asker is stale by one) - EXCEPT when the room is still
		// deriving this step long after the claim, which means the completion
		// was false and honoring it forever wedges the league. See
		// COMPLETED_RECLAIM_GRACE_MS.
		if (existing.completed) {
			if (ask.now - existing.at >= COMPLETED_RECLAIM_GRACE_MS) {
				return { grant: true, maxPick };
			}
			return { grant: false, reason: "step-completed" };
		}
		// Still leased → its holder is presumably mid-advance.
		if (ask.now - existing.at < ask.leaseMs) {
			return { grant: false, reason: "lease-held" };
		}
		// Lease lapsed without completion: the holder crashed mid-advance.
		// Re-claimable so the room isn't wedged forever.
		return { grant: true, maxPick };
	}

	// A genuinely newer step. The previous step's advance published state that
	// made this step derivable, so progress must not be blocked on its
	// completion mark (which is best-effort).
	return { grant: true, maxPick: ask.pick };
};
