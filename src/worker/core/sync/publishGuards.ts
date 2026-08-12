// TWO GUARDS AROUND THE MOMENT A CHANGE IS PUBLISHED, both written after a
// simmed day was lost.
//
// What happened: the device in charge of simming ran a day. While it was
// running - about ten seconds - another device published a trading card and
// took the next version. The sim's advance lost the compare-and-swap, and a
// timeline advance that loses that race is DISCARDED, because a sim computed
// against a world the room has moved past cannot simply be replayed on top of
// a different one. Discarding is supposed to be followed by snapping this
// device's database back to a room checkpoint - the step that removes the
// records the chain will never carry. The room had no checkpoint. So the
// advance was dropped, the games stayed on the simmer's disk, and the device
// went on believing it was caught up while holding a day nobody else had.
//
// Three separate things had to be true for that to happen, and each of these
// guards removes one:
//
//  1. A follower published in the middle of a sim. It should have waited.
//  2. What it published could not possibly have invalidated the sim - it was a
//     trading card - and the day was thrown away anyway.
//  3. The discard assumed a checkpoint existed to snap back to. None did.
//
// Both decisions are pure so each rule is a test rather than a hope.

// ---------------------------------------------------------------- GUARD ONE
//
// A follower holds its publish while the room is marked busy.
//
// `busyUntil` is stamped on the authority document for exactly the length of an
// advance: set when a sim-authority call starts, cleared when its changeset
// lands. Publishing inside that window is how a follower steals the version the
// sim is about to claim. There is nothing to gain by racing it - the change is
// already made locally and sits in the durable outbox, the drain is kicked
// every few seconds, and it goes up the moment the sim's version lands.
//
// This replaces an older, weaker rule that REFUSED a hand-picked list of
// "dangerous" edits during a sim (SIM_CONFLICT_GATED in worker/index.ts). That
// list is an allowlist of things to worry about, so anything missing from it -
// trading cards, as it turned out - sails straight through. Holding every
// publish is the same rule with the polarity the other way round: nothing has
// to be anticipated, and the cost of being wrong is a few seconds' delay
// instead of a lost day.
export const holdPublishForSim = ({
	isAuthority,
	roomBusy,
}: {
	isAuthority: boolean;
	roomBusy: boolean;
}): boolean => !isAuthority && roomBusy;

// ---------------------------------------------------------------- GUARD TWO
//
// What an advance does when it loses the race anyway.
//
// Stores a played day neither reads nor writes. A version that touches only
// these cannot have invalidated a sim, so an advance that lost the CAS to one
// is still perfectly good and belongs on top of it.
//
// An ALLOWLIST, and that direction is the point: a store nobody has thought
// about is treated as conflicting, so the failure mode of forgetting one is a
// discard that was already the old behaviour - never a day published over a
// world that moved underneath it.
export const SIM_INERT_STORES: ReadonlySet<string> = new Set([
	// Cards people make and swap. Pure collectibles; nothing reads them.
	"tradingCards",
	// Uploaded artwork the cards point at.
	"images",
]);

export type StaleAdvancePlan = {
	plan: "rebase" | "discard";
	reason: string;
};

export const resolveStaleAdvancePlan = ({
	applied,
	roomVersion,
	hasCheckpoint,
	interveningStores,
}: {
	applied: number;
	roomVersion: number;
	// Whether the room has a checkpoint to snap this device back to.
	hasCheckpoint: boolean;
	// Every store touched by the versions this device has not applied, or
	// undefined when they could not be determined (more than one version behind,
	// or a payload too big to ride the pointer document - which a sim day always
	// is).
	interveningStores: readonly string[] | undefined;
}): StaleAdvancePlan => {
	if (roomVersion <= applied) {
		return {
			plan: "rebase",
			reason: "Nothing actually intervened.",
		};
	}

	// GUARD THREE, and the one that turns a lost race into a lost day. Discarding
	// only makes sense as the first half of "discard, then restore" - the restore
	// is what removes the records the chain will never carry. With no checkpoint
	// there is no restore, so a discard leaves this device holding games no other
	// device will ever see, quietly, with every status indicator green. Keeping
	// the advance and republishing it is not perfect, but every device ends up
	// agreeing, which is the property that actually matters.
	if (!hasCheckpoint) {
		return {
			plan: "rebase",
			reason:
				"The room has no checkpoint to restore from, so discarding would strand these records here instead of removing them.",
		};
	}

	if (
		interveningStores !== undefined &&
		interveningStores.length > 0 &&
		interveningStores.every((store) => SIM_INERT_STORES.has(store))
	) {
		return {
			plan: "rebase",
			reason: `The version that won touched only ${interveningStores.join(", ")}, which a played day neither reads nor writes.`,
		};
	}

	return {
		plan: "discard",
		reason:
			"The room moved on in a way that could have changed what this advance was computed from.",
	};
};
