// The decision rule for claimSimDay, extracted pure so it's unit-testable
// (the Firestore transaction in FirebaseTransport just applies it).
//
// Why this exists: sim authority is advisory (a shared doc, applied
// optimistically), so during an authority handoff or a catch-up race two
// devices can BOTH believe they may sim. When that happens, both sim the same
// schedule day: their game records collide by gid (last write wins, one sim's
// games survive), but every read-modify-write aggregate - team records,
// headToHeads, player season stats - is applied twice and diverges from the
// game log permanently. This doc is the server-side fence that makes that
// impossible: within a season, exactly one device can claim a given
// (day, games) slice of the schedule, ever.
//
// Shape of the fence: `maxDay` is a monotonic high-water mark - a day below it
// is history and can never be claimed again, no matter how stale the asker.
// Within the newest day, the gids simmed so far are tracked so a day can be
// consumed in disjoint slices (live-sim one game, then sim the rest of the
// day) while a slice that overlaps an already-claimed gid is refused. The
// newest claim is re-claimable only for crash recovery: its lease expired and
// its holder never marked it completed.
//
// COMPLETION IS PER GID, because claims are. It used to be one boolean over
// the whole day, and the mismatch wedged rooms: a slice that died between
// claiming and durably queuing its results left its gids in the doc, and the
// next slice on the same day to finish stamped `completed` over the union -
// permanently fencing games that were never simmed, with the crash-recovery
// lease shadowed. A playoff room saw exactly that: every game 4 published
// except one, and that one refused forever with "games-already-simmed".
// `completedGids` records only the gids whose slices actually reported their
// results durably queued; everything else stays lease-recoverable.

export type SimDayClaimDoc = {
	holderId: string;
	// Stage key carries the season, so days only compare within one season
	// (schedule days restart at 1 each season).
	stageKey: string;
	// The newest day claimed within stageKey.
	day: number;
	// gids claimed within `day` so far (a day can be simmed in disjoint slices).
	gids: number[];
	// Timestamp of the latest claim, for the crash-recovery lease.
	at: number;
	// Highest day ever claimed within stageKey. Defensive default: docs written
	// before this field existed fall back to `day`.
	maxDay?: number;
	// Legacy day-level completion mark, kept so devices running older code
	// still read a completion signal. New code writes and trusts
	// completedGids; when that field is present this boolean is ignored.
	completed?: boolean;
	// The gids whose slices reported their results durably queued. Only these
	// are fenced permanently; a claimed gid missing from here is a slice that
	// died mid-sim and stays recoverable through the lease.
	completedGids?: number[];
};

// How long a LEGACY day-level completion keeps fencing a game, measured from
// the day's newest claim. A legacy mark cannot tell a simmed game from one
// whose claim died before queuing anything, so it gets a grace window instead
// of forever: an asker only reaches this check after the rejection-triggered
// catch-up (and the pre-sim guard) has pulled in everything the room ever
// published, so a game still sitting in its schedule this long after the
// day's last sim activity has no results coming. Generous next to the 90s
// crash lease because the one risk left - a device that queued results
// durably, vanished before flushing, and comes back later - deserves real
// time to flush.
export const SIM_DAY_LEGACY_COMPLETED_GRACE_MS = 10 * 60_000;

export type SimDayClaimDecision =
	| {
			grant: true;
			day: number;
			maxDay: number;
			gids: number[];
			// Carried through so a grant never erases completion state; undefined
			// keeps the doc's legacy shape (or a fresh day's absence of it).
			completedGids?: number[];
	  }
	| { grant: false; reason: string };

export const decideSimDayClaim = (
	existing: SimDayClaimDoc | undefined,
	ask: {
		stageKey: string;
		day: number;
		gids: number[];
		now: number;
		leaseMs: number;
	},
): SimDayClaimDecision => {
	// No claim yet, or a claim from a different season: the fence resets.
	if (!existing || existing.stageKey !== ask.stageKey) {
		return { grant: true, day: ask.day, maxDay: ask.day, gids: ask.gids };
	}

	const maxDay =
		typeof existing.maxDay === "number" ? existing.maxDay : existing.day;

	// A day below the high-water mark is already-run history. Only a stale
	// device would ask for it; granting would re-sim finished games and publish
	// their aggregates on top of the room's real history a second time.
	if (ask.day < maxDay) {
		return { grant: false, reason: "day-already-run" };
	}

	// A genuinely newer day. The previous day's sim published the state that
	// made this day reachable, so progress must not be blocked on its
	// completion mark (which is best-effort).
	if (ask.day > maxDay) {
		return { grant: true, day: ask.day, maxDay: ask.day, gids: ask.gids };
	}

	// Same day as the newest claim. Disjoint slices are normal (a live-simmed
	// game followed by the rest of the day); overlapping ones are the exact
	// double-sim this fence exists to stop.
	const claimed = new Set(existing.gids);
	const overlap = ask.gids.some((gid) => claimed.has(gid));

	if (!overlap) {
		return {
			grant: true,
			day: ask.day,
			maxDay,
			gids: [...existing.gids, ...ask.gids],
			completedGids: existing.completedGids,
		};
	}

	// Which of the doc's gids have durably-queued results. New docs say
	// exactly (completedGids); legacy docs only have the day-level boolean,
	// which covered every claimed gid - simmed or not.
	const sliceAccurate = existing.completedGids !== undefined;
	const completedSet = new Set(
		existing.completedGids ?? (existing.completed ? existing.gids : []),
	);

	if (ask.gids.some((gid) => completedSet.has(gid))) {
		if (sliceAccurate) {
			// These exact games' results are durably queued somewhere. Re-simming
			// them can only fork the room's aggregates.
			return { grant: false, reason: "games-already-simmed" };
		}
		// A legacy mark. It may be fencing a game whose claim died before
		// queuing anything - the wedge described up top - so it holds for a
		// grace window rather than forever. Recovery converts the doc to the
		// slice-accurate shape (completedGids: []), which both ends the
		// ambiguity and puts the recovered sim under a fresh lease.
		if (ask.now - existing.at < SIM_DAY_LEGACY_COMPLETED_GRACE_MS) {
			return { grant: false, reason: "games-already-simmed" };
		}
		const merged = [
			...existing.gids,
			...ask.gids.filter((gid) => !claimed.has(gid)),
		];
		return {
			grant: true,
			day: ask.day,
			maxDay,
			gids: merged,
			completedGids: [],
		};
	}

	if (ask.now - existing.at < ask.leaseMs) {
		return { grant: false, reason: "lease-held" };
	}

	// Lease lapsed without those gids completing: their holder crashed mid-sim.
	// Re-claimable so the room isn't wedged forever; the union keeps earlier
	// slices fenced, and completion state carries through untouched - this is
	// the branch that makes a dead slice recoverable no matter how many other
	// slices of the day finished after it.
	const merged = [
		...existing.gids,
		...ask.gids.filter((gid) => !claimed.has(gid)),
	];
	return {
		grant: true,
		day: ask.day,
		maxDay,
		gids: merged,
		completedGids: existing.completedGids,
	};
};
