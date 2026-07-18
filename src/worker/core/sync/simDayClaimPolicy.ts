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
	// Set by completeSimDay once the claimed slice's sim finished, which closes
	// the crash-recovery re-claim window for it.
	completed?: boolean;
};

export type SimDayClaimDecision =
	| { grant: true; day: number; maxDay: number; gids: number[] }
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
		};
	}

	if (existing.completed) {
		return { grant: false, reason: "games-already-simmed" };
	}

	if (ask.now - existing.at < ask.leaseMs) {
		return { grant: false, reason: "lease-held" };
	}

	// Lease lapsed without completion: the holder crashed mid-sim. Re-claimable
	// so the room isn't wedged forever; the union keeps earlier slices fenced.
	const merged = [
		...existing.gids,
		...ask.gids.filter((gid) => !claimed.has(gid)),
	];
	return { grant: true, day: ask.day, maxDay, gids: merged };
};
