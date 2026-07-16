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
// crash recovery: its lease expired AND its holder never marked it completed.

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
		// step, so this asker is stale by one).
		if (existing.completed) {
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
